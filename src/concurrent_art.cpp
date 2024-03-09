//
// Created by skyitachi on 24-1-27.
//
#include "concurrent_art.h"

#include <fmt/core.h>

#include <thread>

#include "leaf.h"
#include "node16.h"
#include "node256.h"
#include "node4.h"
#include "node48.h"
#include "prefix.h"

namespace part {

bool ConcurrentART::Get(const part::ARTKey& key, std::vector<idx_t>& result_ids) {
  //  fmt::println("root readers: {}", root->Readers());
  while (lookup(root.get(), key, 0, result_ids)) {
    result_ids.clear();
    std::this_thread::yield();
  }
  return !result_ids.empty();
}

bool ConcurrentART::lookup(ConcurrentNode* next_node, const ARTKey& key, idx_t depth, std::vector<idx_t>& result_ids) {
  next_node->RLock();
  if (!next_node->IsSet()) {
    next_node->RUnlock();
    return false;
  }
  if (next_node->IsDeleted()) {
    // NOTE: important to unlock correctly
    next_node->RUnlock();
    return true;
  }
  bool retry = false;
  while (next_node->IsSet()) {
    if (next_node->GetType() == NType::PREFIX) {
      CPrefix::Traverse(*this, next_node, key, depth, retry);
      if (retry) {
        next_node->RUnlock();
        return retry;
      }
      assert(next_node->RLocked());
      if (next_node->GetType() == NType::PREFIX) {
        // NOTE: need unlock asap
        next_node->RUnlock();
        return false;
      }
    }

    assert(next_node->RLocked());

    // inlined leaf can return directly
    if (next_node->GetType() == NType::LEAF_INLINED) {
      result_ids.emplace_back(next_node->GetDocId());
      next_node->RUnlock();
      return false;
    }

    if (next_node->GetType() == NType::LEAF) {
      auto& cleaf = CLeaf::Get(*this, *next_node);
      // NOTE: GetDocIds already released lock
      cleaf.GetDocIds(*this, *next_node, result_ids, std::numeric_limits<int64_t>::max(), retry);
      return retry;
    }

    assert(depth < key.len);
    auto child = next_node->GetChild(*this, key[depth]);
    if (!child) {
      // cannot found node
      next_node->RUnlock();
      return false;
    }
    child.value()->RLock();
    // child.value is not same as next_node children
    if (child.value()->IsDeleted()) {
      // NOTE: need retry
      child.value()->RUnlock();
      next_node->RUnlock();
      return true;
    }
    next_node->RUnlock();
    next_node = child.value();
    depth++;
  }
  return false;
}

void ConcurrentART::Put(const ARTKey& key, idx_t doc_id) {
  bool retry = false;
  do {
    root->RLock();
    retry = insert(*root, key, 0, doc_id);
    if (retry) {
      std::this_thread::yield();
    }
  } while (retry);
}

// NOTE: never hold locks after the insert
bool ConcurrentART::insert(ConcurrentNode& node, const ARTKey& key, idx_t depth, const idx_t& doc_id) {
  assert(node.RLocked());
  if (node.IsDeleted()) {
    node.RUnlock();
    return true;
  }
  if (!node.IsSet()) {
    assert(depth <= key.len);
    auto ref = std::ref(node);
    ref.get().Upgrade();
    CPrefix::New(*this, ref, key, depth, key.len - depth);
    P_ASSERT(ref.get().Locked());
    CLeaf::New(ref, doc_id);
    ref.get().Unlock();
    return false;
  }
  auto node_type = node.GetType();

  if (node_type == NType::LEAF || node_type == NType::LEAF_INLINED) {
    // insert into leaf
    return insertToLeaf(&node, doc_id);
  }

  if (node_type != NType::PREFIX) {
    assert(depth < key.len);
    // lock in advance to prevent double check
    node.Upgrade();
    auto child = node.GetChild(*this, key[depth]);
    if (child) {
      node.Unlock();
      child.value()->RLock();
      return insert(*child.value(), key, depth + 1, doc_id);
    }
    ConcurrentNode* new_node = AllocateNode();
    ConcurrentNode* next_node = new_node;
    if (depth + 1 < key.len) {
      new_node->Lock();
      CPrefix::NewPrefixNew(*this, new_node, key, depth + 1, key.len - depth - 1);
      auto& new_prefix = CPrefix::Get(*this, *new_node);
      next_node = new_prefix.ptr;
      new_node->Unlock();
    }
    assert(next_node);
    next_node->Lock();
    CLeaf::New(*next_node, doc_id);
    next_node->Unlock();

    ConcurrentNode::InsertChild(*this, &node, key[depth], new_node);
    // NOTE: important release lock carefully
    node.Unlock();
    return false;
  }

  auto next_node = &node;
  bool retry = false;
  auto mismatch_position = CPrefix::Traverse(*this, next_node, key, depth, retry);
  if (retry) {
    // need retry
    next_node->RUnlock();
    return true;
  }

  assert(next_node->RLocked());
  if (next_node->GetType() != NType::PREFIX) {
    return insert(*next_node, key, depth, doc_id);
  }

  ConcurrentNode* remaining_prefix_node = nullptr;
  auto prefix_byte = CPrefix::GetByte(*this, *next_node, mismatch_position);

  // NOTE: next_node may next_node same as remaining_prefix_node
  // NOTE: next_node may not be initialized
  next_node->Upgrade();
  retry = CPrefix::Split(*this, next_node, remaining_prefix_node, mismatch_position);
  if (retry) {
    return retry;
  }
  assert(remaining_prefix_node != nullptr);

  ConcurrentNode* new_node4;

  assert(next_node->Locked() || next_node->RLocked());

  if (next_node->IsDeleted()) {
    // next_node points to node
    assert(next_node->Locked());
    next_node->Reset();
    CNode4::New(*this, *next_node);
    new_node4 = next_node;
  } else {
    // update prefix new ptr
    assert(next_node->Locked());
    CNode4::New(*this, *next_node);
    new_node4 = next_node;
  }

  assert(!new_node4->IsDeleted() && new_node4->Locked());
  assert(!remaining_prefix_node->IsDeleted());

  CNode4::InsertChild(*this, new_node4, prefix_byte, remaining_prefix_node);

  auto next_prefix_node = AllocateNode();
  auto ref_next_prefix = std::ref(*next_prefix_node);
  // NOTE: new node no need lock
  ref_next_prefix.get().Lock();

  if (depth + 1 < key.len) {
    // NOTE: need create PrefixNode
    CPrefix::New(*this, ref_next_prefix, key, depth + 1, key.len - depth - 1);
  }

  assert(ref_next_prefix.get().Locked());

  CLeaf::New(ref_next_prefix, doc_id);
  ref_next_prefix.get().Unlock();

  CNode4::InsertChild(*this, new_node4, key[depth], next_prefix_node);
  new_node4->Unlock();

  return false;
}

ConcurrentART::ConcurrentART(const FixedSizeAllocatorListPtr allocators_ptr)
    : allocators(allocators_ptr), owns_data(false) {
  if (!allocators) {
    owns_data = true;
    allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
    allocators->emplace_back(sizeof(CPrefix), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(CLeaf), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(CNode4), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(CNode16), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(CNode48), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(CNode256), Allocator::DefaultAllocator());
  }
  root = std::make_unique<ConcurrentNode>();
}

ConcurrentART::ConcurrentART(const std::string& index_path, const FixedSizeAllocatorListPtr allocators_ptr)
    : ConcurrentART(allocators_ptr) {
  index_path_ = index_path;

  index_fd_ = ::open(index_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (index_fd_ == -1) {
    throw std::invalid_argument(fmt::format("cann open {} index file, error: {}", index_path, strerror(errno)));
  }

  metadata_fd_ = ::open(index_path.c_str(), O_RDWR, 0644);
  try {
    auto pointer = ReadMetadata();
    root = std::make_unique<ConcurrentNode>(pointer.block_id, pointer.offset);
    root->Lock();
    root->SetSerialized();
    root->Deserialize(*this);
  } catch (std::exception& e) {
    root = std::make_unique<ConcurrentNode>();
  }
}

BlockPointer ConcurrentART::ReadMetadata() const {
  if (metadata_fd_ == -1) {
    throw std::invalid_argument(fmt::format("no meta file"));
  }
  BlockPointer pointer(0, 0);
  BlockDeserializer reader(metadata_fd_, pointer);
  BlockPointer root_pointer;
  reader.ReadData(reinterpret_cast<data_ptr_t>(&root_pointer.block_id), sizeof(block_id_t));
  reader.ReadData(reinterpret_cast<data_ptr_t>(&root_pointer.offset), sizeof(uint32_t));
  return root_pointer;
}

ConcurrentART::~ConcurrentART() {
  root->Reset();
  for (auto* node_ptr : node_allocators_) {
    delete node_ptr;
  }
}

bool ConcurrentART::insertToLeaf(ConcurrentNode* leaf, idx_t doc_id) {
  assert(leaf->RLocked());
  bool retry = false;
  // make sure leaf unlocked after insert
  leaf->Upgrade();
  CLeaf::Insert(*this, leaf, doc_id, retry);
  assert(!leaf->Locked());
  return retry;
}

ConcurrentNode* ConcurrentART::AllocateNode() {
  auto new_node = new ConcurrentNode();
  new_node->ResetAll();
  node_allocators_.push_back(new_node);
  return new_node;
}

void ConcurrentART::Serialize() {
  if (root->IsSet()) {
    SequentialSerializer data_writer(index_path_, META_OFFSET);
    auto pointer = root->Serialize(*this, data_writer);
    data_writer.Flush();
    SequentialSerializer meta_writer(index_path_);
    UpdateMetadata(pointer, meta_writer);
    meta_writer.Flush();
  }
}

void ConcurrentART::Deserialize() { root->Deserialize(*this); }

void ConcurrentART::UpdateMetadata(BlockPointer pointer, Serializer& writer) {
  writer.Write<block_id_t>(pointer.block_id);
  writer.Write<uint32_t>(pointer.offset);
}

// NOTE: no need to retry ???
void ConcurrentART::Merge(ART& other) {
  root->RLock();
  root->Merge(*this, other, *other.root);
}
}  // namespace part
