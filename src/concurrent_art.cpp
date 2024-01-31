//
// Created by skyitachi on 24-1-27.
//
#include "concurrent_art.h"

#include <fmt/core.h>

#include <thread>

#include "leaf.h"
#include "prefix.h"

namespace part {

bool ConcurrentART::Get(const part::ARTKey& key, std::vector<idx_t>& result_ids) {
  while (lookup(*root, key, 0, result_ids)) {
    result_ids.clear();
    std::this_thread::yield();
  }
  return !result_ids.empty();
}

bool ConcurrentART::lookup(ConcurrentNode& node, const ARTKey& key, idx_t depth, std::vector<idx_t>& result_ids) {
  auto next_node = std::ref(node);
  next_node.get().RLock();
  if (next_node.get().IsDeleted()) {
    // NOTE: important to unlock correctly
    next_node.get().RUnlock();
    return true;
  }
  bool retry = false;
  while (next_node.get().IsSet()) {
    if (next_node.get().GetType() == NType::PREFIX) {
      CPrefix::Traverse(*this, next_node, key, depth, retry);
      if (retry) {
        next_node.get().RUnlock();
        return retry;
      }
      assert(next_node.get().RLocked());
      if (next_node.get().GetType() == NType::PREFIX) {
        // NOTE: need unlock asap
        next_node.get().RUnlock();
        return false;
      }
    }

    // inlined leaf can return directly
    if (next_node.get().GetType() == NType::LEAF_INLINED) {
      result_ids.emplace_back(next_node.get().GetDocId());
      next_node.get().RUnlock();
      return false;
    }

    //
    if (next_node.get().GetType() == NType::LEAF) {
      auto& cleaf = CLeaf::Get(*this, next_node.get());
      // NOTE: GetDocIds already released lock
      cleaf.GetDocIds(*this, next_node.get(), result_ids, std::numeric_limits<int64_t>::max(), retry);
      return retry;
    }
  }
  return false;
}

void ConcurrentART::Put(const ARTKey& key, idx_t doc_id) {
  while (insert(*root, key, 0, doc_id)) {
    std::this_thread::yield();
  }
}

bool ConcurrentART::insert(ConcurrentNode& node, const ARTKey& key, idx_t depth, const idx_t& doc_id) {
  node.RLock();
  if (node.IsDeleted()) {
    node.RUnlock();
    return true;
  }
  if (!node.IsSet()) {
    assert(depth <= key.len);
    auto ref = std::ref(node);
    fmt::println("before lock upgrade");
    ref.get().Upgrade();
    fmt::println("lock upgraded");
    CPrefix::New(*this, ref, key, depth, key.len - depth);
    P_ASSERT(ref.get().Locked());
    CLeaf::New(ref, doc_id);
    ref.get().Unlock();
    return false;
  }
  // TODO:
  return false;
}

ConcurrentART::ConcurrentART(const FixedSizeAllocatorListPtr allocators_ptr)
    : allocators(allocators_ptr), owns_data(false) {
  if (!allocators) {
    owns_data = true;
    allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
    allocators->emplace_back(sizeof(CPrefix), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(CLeaf), Allocator::DefaultAllocator());
  }
  root = std::make_unique<ConcurrentNode>();
}

ConcurrentART::ConcurrentART(const std::string& index_path,
                             const ConcurrentART::FixedSizeAllocatorListPtr allocators_ptr) {
  index_path_ = index_path;

  index_fd_ = ::open(index_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (index_fd_ == -1) {
    throw std::invalid_argument(fmt::format("cann open {} index file, error: {}", index_path, strerror(errno)));
  }

  metadata_fd_ = ::open(index_path.c_str(), O_RDWR, 0644);
  try {
    auto pointer = ReadMetadata();
    root = std::make_unique<ConcurrentNode>(pointer.block_id, pointer.offset);
    root->SetSerialized();
    //    root->Deserialize(*this);
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

ConcurrentART::~ConcurrentART() { root->Reset(); }

}  // namespace part
