//
// Created by skyitachi on 24-1-25.
//
#include "concurrent_node.h"

#include <fmt/core.h>

#include <thread>

#include "concurrent_art.h"
#include "leaf.h"
#include "node16.h"
#include "node256.h"
#include "node4.h"
#include "node48.h"
#include "prefix.h"

namespace part {
constexpr uint32_t RETRY_THRESHOLD = 100;
constexpr uint64_t HAS_WRITER = ~0L;

static std::string ToStr(uint8_t byte) {
  if (isalpha(byte)) {
    return std::string(1, byte);
  }
  return std::to_string((uint32_t)byte);
}

void ConcurrentNode::RLock() {
  int retry = 0;
  auto start = std::chrono::high_resolution_clock::now();
  while (true) {
    uint64_t prev = lock_.load();
    if (prev != HAS_WRITER) {
      uint64_t next = prev + 1;
      if (lock_.compare_exchange_weak(prev, next)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
      auto end = std::chrono::high_resolution_clock::now();

      // 计算耗时
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      if (duration > 100) {
        fmt::println("debug point RLock");
      }
    }
  }
}

void ConcurrentNode::RUnlock() {
  int retry = 0;
  //  fmt::println("debug RUnlock {}", static_cast<void *>(this));
  auto start = std::chrono::high_resolution_clock::now();
  while (true) {
    uint64_t prev = lock_;
    if (prev != HAS_WRITER && prev > 0) {
      uint64_t next = prev - 1;
      if (lock_.compare_exchange_weak(prev, next)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
      auto end = std::chrono::high_resolution_clock::now();

      // 计算耗时
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      if (duration > 100) {
        fmt::println("debug point RUnlock");
      }
    }
  }
}

void ConcurrentNode::Lock() {
  int retry = 0;
  auto start = std::chrono::high_resolution_clock::now();

  while (true) {
    uint64_t prev = lock_;
    if (prev == 0) {
      if (lock_.compare_exchange_weak(prev, HAS_WRITER)) {
        return;
      }
    }

    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();

      auto end = std::chrono::high_resolution_clock::now();

      // 计算耗时
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      if (duration > 100) {
        fmt::println("debug point Lock");
      }
    }
  }
}

void ConcurrentNode::Unlock() {
  int retry = 0;
  //  fmt::println("debug Unlock {}", static_cast<void *>(this));
  auto start = std::chrono::high_resolution_clock::now();
  while (true) {
    uint64_t prev = lock_;
    if (prev == HAS_WRITER) {
      if (lock_.compare_exchange_weak(prev, 0)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();

      auto end = std::chrono::high_resolution_clock::now();

      // 计算耗时
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      if (duration > 100) {
        fmt::println("debug point Unlock");
      }
    }
  }
}

void ConcurrentNode::Downgrade() {
  // one reader
  lock_.store(1);
}

void ConcurrentNode::Upgrade() {
  int retry = 0;
  auto start = std::chrono::high_resolution_clock::now();
  while (true) {
    // NOTE: only one reader can upgrade to writer
    uint64_t prev = 1;
    if (lock_.compare_exchange_weak(prev, HAS_WRITER)) {
      return;
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
      auto end = std::chrono::high_resolution_clock::now();

      // 计算耗时
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
      if (duration > 100) {
        fmt::println("debug point Upgrade");
      }
    }
  }
}

// NOTE: not exactly right
bool ConcurrentNode::RLocked() const {
  auto cur = lock_.load();
  return cur > 0 && cur != HAS_WRITER;
}

bool ConcurrentNode::Locked() const {
  auto cur = lock_.load();
  return cur == HAS_WRITER;
}

FixedSizeAllocator& ConcurrentNode::GetAllocator(const ConcurrentART& art, NType type) {
  return (*art.allocators)[(uint8_t)type - 1];
}

void ConcurrentNode::Free(ConcurrentART& art, ConcurrentNode* node) {
  assert(node->Locked());
  if (!node->IsSet()) {
    return;
  }
  if (!node->IsSerialized()) {
    auto type = node->GetType();
    switch (type) {
      case NType::LEAF_INLINED: {
        node->Reset();
        node->SetDeleted();
        return;
      }
      case NType::LEAF:
        return CLeaf::Free(art, node);
      case NType::PREFIX:
        return CPrefix::Free(art, node);
      case NType::NODE_4:
        return CNode4::Free(art, node);
      case NType::NODE_16:
        return CNode16::Free(art, node);
      case NType::NODE_48:
        return CNode48::Free(art, node);
      case NType::NODE_256:
        return CNode256::Free(art, node);
      default:
        throw std::invalid_argument("[CNode.Free] unsupported node type");
    }
  }
}
std::optional<ConcurrentNode*> ConcurrentNode::GetChild(ConcurrentART& art, uint8_t byte) const {
  assert(RLocked() || Locked());
  assert(IsSet() && !IsSerialized());

  std::optional<ConcurrentNode*> child;
  switch (GetType()) {
    case NType::NODE_4:
      child = CNode4::Get(art, this).GetChild(byte);
      break;
    case NType::NODE_16:
      child = CNode16::Get(art, this).GetChild(byte);
      break;
    case NType::NODE_48:
      child = CNode48::Get(art, this).GetChild(byte);
      break;
    case NType::NODE_256:
      child = CNode256::Get(art, this).GetChild(byte);
      break;
    default:
      throw std::invalid_argument("GetChild not support other types");
  }
  if (child && reinterpret_cast<unsigned long>(child.value()) == 0xbebebebebebebebe) {
    fmt::println("debug null pointer");
    ::fflush(stdout);
  }
  if (child && child.value()->IsSerialized()) {
    // TODO: Deserialize
  }
  return child;
}

void ConcurrentNode::ToGraph(ConcurrentART& art, std::ofstream& out, idx_t& id, std::string parent_id) {
  RLock();
  if (!IsSet()) {
    return;
  }
  RUnlock();
  switch (GetType()) {
    case NType::LEAF: {
      id++;
      auto total_count = CLeaf::TotalCount(art, this);
      std::string leaf_prefix("LEAF_");
      out << leaf_prefix << id;
      out << "[shape=plain color=green ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      out << "<TR><TD COLSPAN=\"" << (uint32_t)total_count << "\">leaf_" << id << "</TD></TR><TR>\n";

      ConcurrentNode* next_node = this;
      next_node->RLock();
      while (next_node->IsSet()) {
        auto& leaf = CLeaf::Get(art, *next_node);
        for (int i = 0; i < leaf.count; i++) {
          out << "<TD>" << leaf.row_ids[i] << "</TD>\n";
        }
        next_node->RUnlock();
        next_node = leaf.ptr;
        next_node->RLock();
      }
      out << "</TR></TABLE>>];\n";
      if (!parent_id.empty()) {
        out << parent_id << "->" << leaf_prefix << id << ";\n";
      }
      next_node->RUnlock();
      break;
    }
    case NType::LEAF_INLINED: {
      id++;
      std::string leaf_prefix("LEAF_INLINED_");
      out << leaf_prefix << id;
      out << "[shape=plain color=green ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      out << "<TR><TD COLSPAN=\"" << 1 << "\">"
          << "leaf_" << id << "</TD></TR><TR>\n";
      for (int i = 0; i < 1; i++) {
        out << "<TD>" << GetDocId() << "</TD>\n";
      }
      out << "</TR></TABLE>>];\n";
      if (!parent_id.empty()) {
        out << parent_id << "->" << leaf_prefix << id << ";\n";
      }
      break;
    }
    case NType::PREFIX: {
      id++;
      std::string prefix_prefix("PREFIX_");
      std::string current_id_str = fmt::format("{}{}", prefix_prefix, id);
      RLock();
      auto& prefix = CPrefix::Get(art, *this);
      out << prefix_prefix << id;
      out << "[shape=plain color=pink ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";

      out << "<TR><TD COLSPAN=\"" << (uint32_t)prefix.data[PREFIX_SIZE] << "\"> prefix_" << id << "</TD></TR>\n";
      out << "<TR>\n";
      for (int i = 0; i < prefix.data[PREFIX_SIZE]; i++) {
        out << "<TD>" << ToStr(prefix.data[i]) << "</TD>\n";
      }
      out << "</TR>";
      out << "</TABLE>>];\n";

      // Print table end
      if (!parent_id.empty()) {
        out << parent_id << "->" << current_id_str << ";\n";
      }

      RUnlock();
      if (prefix.ptr->IsSet()) {
        prefix.ptr->ToGraph(art, out, id, current_id_str);
      }
      break;
    }
    case NType::NODE_4: {
      id++;
      RLock();
      std::string node_prefix("NODE4_");
      std::string current_id_str = fmt::format("{}{}", node_prefix, id);
      auto& node4 = CNode4::Get(art, this);
      out << node_prefix << id;
      out << "[shape=plain color=yellow ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";

      out << "<TR><TD COLSPAN=\"" << (uint32_t)node4.count << "\"> node4_" << id << "</TD></TR>\n";
      out << "<TR>\n";
      for (int i = 0; i < node4.count; i++) {
        out << "<TD>" << ToStr(node4.key[i]) << "</TD>\n";
      }
      out << "</TR>";
      out << "</TABLE>>];\n";

      // Print table end
      if (!parent_id.empty()) {
        out << parent_id << "->" << current_id_str << ";\n";
      }

      RUnlock();
      for (int i = 0; i < node4.count; i++) {
        node4.children[i]->ToGraph(art, out, id, current_id_str);
      }
      break;
    }
    case NType::NODE_16: {
      id++;
      std::string node_prefix("NODE16_");
      std::string current_id_str = fmt::format("{}{}", node_prefix, id);
      RLock();
      auto& node16 = CNode16::Get(art, this);
      out << node_prefix << id;
      out << "[shape=plain color=yellow ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";

      out << "<TR><TD COLSPAN=\"" << (uint32_t)node16.count << "\"> node16_" << id << "</TD></TR>\n";
      out << "<TR>\n";
      RUnlock();
      for (int i = 0; i < node16.count; i++) {
        out << "<TD>" << ToStr(node16.key[i]) << "</TD>\n";
      }
      out << "</TR>";
      out << "</TABLE>>];\n";

      // Print table end
      if (!parent_id.empty()) {
        out << parent_id << "->" << current_id_str << ";\n";
      }

      for (int i = 0; i < node16.count; i++) {
        node16.children[i]->ToGraph(art, out, id, current_id_str);
      }
      break;
    }
    case NType::NODE_48: {
      id++;
      std::string node_prefix("NODE48_");
      std::string current_id_str = fmt::format("{}{}", node_prefix, id);
      RLock();
      auto& node48 = CNode48::Get(art, this);
      out << node_prefix << id;
      out << "[shape=plain color=yellow ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";

      out << "<TR><TD COLSPAN=\"" << (uint32_t)node48.count << "\"> node48_" << id << "</TD></TR>\n";
      out << "<TR>\n";
      RUnlock();
      for (int i = 0; i < Node::NODE_256_CAPACITY; i++) {
        if (node48.child_index[i] != Node::EMPTY_MARKER) {
          out << "<TD>" << ToStr(i) << "</TD>\n";
        }
      }
      out << "</TR>";
      out << "</TABLE>>];\n";

      // Print table end
      if (!parent_id.empty()) {
        out << parent_id << "->" << current_id_str << ";\n";
      }

      for (int i = 0; i < 256; i++) {
        if (node48.child_index[i] != Node::EMPTY_MARKER) {
          node48.children[node48.child_index[i]]->ToGraph(art, out, id, current_id_str);
        }
      }
      break;
    }
    case NType::NODE_256: {
      id++;
      std::string node_prefix("NODE256_");
      std::string current_id_str = fmt::format("{}{}", node_prefix, id);
      RLock();
      auto& node256 = CNode256::Get(art, this);
      out << node_prefix << id;
      out << "[shape=plain color=yellow ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";

      out << "<TR><TD COLSPAN=\"" << (uint32_t)node256.count << "\"> node256_" << id << "</TD></TR>\n";
      out << "<TR>\n";
      RUnlock();
      for (int i = 0; i < Node::NODE_256_CAPACITY; i++) {
        if (node256.children[i]) {
          out << "<TD>" << ToStr(i) << "</TD>\n";
        }
      }
      out << "</TR>";
      out << "</TABLE>>];\n";

      // Print table end
      if (!parent_id.empty()) {
        out << parent_id << "->" << current_id_str << ";\n";
      }

      for (int i = 0; i < 256; i++) {
        if (node256.children[i]) {
          node256.children[i]->ToGraph(art, out, id, current_id_str);
        }
      }
      break;
    }
    default:
      throw std::invalid_argument("ToGraph does not support such node types");
  }
}

void ConcurrentNode::InsertChild(ConcurrentART& art, ConcurrentNode* node, uint8_t byte, ConcurrentNode* child) {
  assert(node->Locked());
  switch (node->GetType()) {
    case NType::NODE_4:
      CNode4::InsertChild(art, node, byte, child);
      break;
    case NType::NODE_16:
      CNode16::InsertChild(art, node, byte, child);
      break;
    case NType::NODE_48:
      CNode48::InsertChild(art, node, byte, child);
      break;
    case NType::NODE_256:
      CNode256::InsertChild(art, node, byte, child);
      break;
    default:
      throw std::invalid_argument(fmt::format("Invalid node type for InsertChild type: {}", (uint8_t)node->GetType()));
  }
}

int64_t ConcurrentNode::Readers() { return lock_.load(); }

BlockPointer ConcurrentNode::Serialize(ConcurrentART& art, Serializer& serializer) {
  assert(RLocked());
  if (!IsSet()) {
    RUnlock();
    return BlockPointer();
  }
  if (IsSerialized()) {
    Upgrade();
    Deserialize(art);
    Downgrade();
  }
  switch (GetType()) {
    case NType::PREFIX:
      return CPrefix::Serialize(art, this, serializer);
    case NType::NODE_4:
      return CNode4::Serialize(art, this, serializer);
    case NType::NODE_16:
      return CNode16::Serialize(art, this, serializer);
    case NType::NODE_48:
      return CNode48::Serialize(art, this, serializer);
    case NType::NODE_256:
      return CNode256::Serialize(art, this, serializer);
    case NType::LEAF:
    case NType::LEAF_INLINED:
      return CLeaf::Serialize(art, this, serializer);
    default:
      throw std::invalid_argument("invalid type for serialize");
  }
}

void ConcurrentNode::Deserialize(ConcurrentART& art) {
  assert(Locked());
  BlockPointer pointer(GetBufferId(), GetOffset());
  BlockDeserializer reader(art.GetIndexFileFd(), pointer);

  // NOTE: pointer has alead parsed
  this->DeserializeInternal(art, reader);
}

// NOTE: no release lock
bool ConcurrentNode::Deserialize(ConcurrentART& art, Deserializer& reader) {
  P_ASSERT(Locked());
  auto block_id = reader.Read<block_id_t>();
  auto offset = reader.Read<uint32_t>();

  Reset();

  if (block_id == INVALID_BLOCK) {
    Unlock();
    return false;
  }

  SetSerialized();
  SetPtr(block_id, offset);

  // important
  Unlock();
  //  // NOTE: eager deserialize
  //  this->Deserialize(art);
  return true;
}

void ConcurrentNode::DeserializeInternal(ConcurrentART& art, Deserializer& reader) {
  P_ASSERT(Locked());
  P_ASSERT(IsSet() && IsSerialized());

  auto type = reader.Read<uint8_t>();

  // NOTE: important
  Reset();

  SetType(type);

  auto decoded_type = GetType();

  if (decoded_type == NType::PREFIX) {
    return CPrefix::Deserialize(art, this, reader);
  }

  if (decoded_type == NType::LEAF_INLINED) {
    SetDocID(reader.Read<idx_t>());
    this->Unlock();
    return;
  }

  if (decoded_type == NType::LEAF) {
    return CLeaf::Deserialize(art, this, reader);
  }

  this->Update(ConcurrentNode::GetAllocator(art, decoded_type).ConcNew());
  this->SetType(type);

  switch (decoded_type) {
    case NType::NODE_4:
      return CNode4::Deserialize(art, this, reader);
    case NType::NODE_16:
      return CNode16::Deserialize(art, this, reader);
    case NType::NODE_48:
      return CNode48::Deserialize(art, this, reader);
    case NType::NODE_256:
      return CNode256::Deserialize(art, this, reader);
    default:
      throw std::invalid_argument("other type deserializer not supported");
  }
}

void ConcurrentNode::Merge(ConcurrentART& cart, ART& art, Node& other) {
  assert(RLocked());
  if (!other.IsSet()) {
    RUnlock();
    return;
  }
  if (!IsSet()) {
    Upgrade();
    MergeUpdate(cart, art, other);
    assert(!Locked());
    return;
  }
  ResolvePrefixes(cart, art, other);
}

// NOTE: only used in merge function
void ConcurrentNode::MergeUpdate(ConcurrentART& cart, ART& art, Node& other) {
  // Origin node may not be cleared
  //  P_ASSERT(Locked() && !IsSet());
  P_ASSERT(Locked());
  switch (other.GetType()) {
    case NType::PREFIX:
      return CPrefix::MergeUpdate(cart, art, this, other);
    case NType::LEAF:
      return CLeaf::MergeUpdate(cart, art, this, other);
    case NType::LEAF_INLINED:
      return CLeaf::MergeUpdate(cart, art, this, other);
    case NType::NODE_4:
      return CNode4::MergeUpdate(cart, art, this, other);
    case NType::NODE_16:
      return CNode16::MergeUpdate(cart, art, this, other);
    case NType::NODE_48:
      return CNode48::MergeUpdate(cart, art, this, other);
    case NType::NODE_256:
      return CNode256::MergeUpdate(cart, art, this, other);
    default:
      throw std::invalid_argument("unsupported node type for merge");
  }
}

// NOTE: return value meaning
bool ConcurrentNode::ResolvePrefixes(ConcurrentART& cart, ART& art, Node& other) {
  P_ASSERT(RLocked());
  if (IsSerialized()) {
    Upgrade();
    this->Deserialize(cart);
    Downgrade();
  }
  if (GetType() != NType::PREFIX && other.GetType() != NType::PREFIX) {
    return MergeInternal(cart, art, other);
  }

  auto l_node = this;
  auto r_node = std::ref(other);

  // case1: both are prefix nodes, l_node won't be updated
  if (l_node->GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
    CPrefix::MergeTwoPrefix(cart, art, l_node, r_node);
    return true;
  }

  // case 2: none prefix type node merge with prefix type node
  if (l_node->GetType() != NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
    // NOTE: leaf node merge prefix node case will never appear
    // none prefix node merge prefix_node
    return l_node->MergePrefix(cart, art, r_node);
  }

  // case 3: prefixes diff at a specific byte
  if (l_node->GetType() == NType::PREFIX && r_node.get().GetType() != NType::PREFIX) {
    MergeNonePrefixByPrefix(cart, art, l_node, other);
    return true;
  }

  return true;
}

bool ConcurrentNode::MergeInternal(ConcurrentART& cart, ART& art, Node& other) {
  assert(RLocked());
  if (IsSerialized()) {
    Upgrade();
    this->Deserialize(cart);
    Downgrade();
  }
  assert(IsSet() && other.IsSet());
  assert(GetType() != NType::PREFIX && other.GetType() != NType::PREFIX);

  // NOTE: cannot swap

  auto cnode = this;
  auto& node = other;

  auto is_leaf = cnode->GetType() == NType::LEAF || cnode->GetType() == NType::LEAF_INLINED;
  auto right_leaf = node.GetType() == NType::LEAF || node.GetType() == NType::LEAF_INLINED;
  if (is_leaf && right_leaf) {
    Upgrade();
    CLeaf::Merge(cart, art, cnode, other);
    assert(!cnode->Locked());
    return true;
  }

  uint8_t byte = 0;
  auto r_child = node.GetNextChild(art, byte);

  while (r_child) {
    P_ASSERT(cnode->RLocked());
    auto l_child = cnode->GetChild(cart, byte);
    if (!l_child) {
      auto new_child = cart.AllocateNode();
      cnode->Upgrade();
      InsertChild(cart, cnode, byte, new_child);
      // NOTE: important order matters
      new_child->Lock();
      // must use downgrade
      new_child->MergeUpdate(cart, art, *r_child.value());
      // NOTE: downgrade after the new_child merged
      cnode->Downgrade();
    } else {
      l_child.value()->RLock();
      l_child.value()->ResolvePrefixes(cart, art, *r_child.value());
    }
    if (byte == std::numeric_limits<uint8_t>::max()) {
      break;
    }
    byte++;
    r_child = node.GetNextChild(art, byte);
  }
  assert(cnode->RLocked());
  cnode->RUnlock();
  return true;
}

// NOTE: return value meaning
bool ConcurrentNode::MergePrefix(ConcurrentART& cart, ART& art, Node& other) {
  assert(RLocked());
  if (IsSerialized()) {
    Upgrade();
    this->Deserialize(cart);
    Downgrade();
  }
  // NOTE: can not be leaf type
  assert(GetType() != NType::PREFIX || GetType() != NType::LEAF_INLINED || GetType() != NType::LEAF);
  assert(other.GetType() == NType::PREFIX);

  auto l_node = this;
  idx_t pos = 0;
  auto ref_node = std::ref(other);
  switch (l_node->GetType()) {
    case NType::NODE_4:
      return CNode4::TraversePrefix(cart, art, l_node, ref_node, pos);
    case NType::NODE_16:
      return CNode16::TraversePrefix(cart, art, l_node, ref_node, pos);
    case NType::NODE_48:
      return CNode48::TraversePrefix(cart, art, l_node, ref_node, pos);
    case NType::NODE_256:
      return CNode256::TraversePrefix(cart, art, l_node, ref_node, pos);
    default:
      throw std::logic_error("MergePrefix not support the node type");
  }
}

// NOTE: return value indicated whether other was merged into cart
bool ConcurrentNode::TraversePrefix(ConcurrentART& cart, ART& art, ConcurrentNode* node, reference<Node>& prefix,
                                    idx_t& pos) {
  P_ASSERT(node->RLocked());
  if (node->IsSerialized()) {
    node->Upgrade();
    node->Deserialize(cart);
    node->Downgrade();
  }
  P_ASSERT(node->GetType() != NType::LEAF && node->GetType() != NType::LEAF_INLINED);
  switch (node->GetType()) {
    case NType::PREFIX:
      return CPrefix::TraversePrefix(cart, art, node, prefix, 0, pos);
    case NType::NODE_4:
      return CNode4::TraversePrefix(cart, art, node, prefix, pos);
    case NType::NODE_16:
      return CNode16::TraversePrefix(cart, art, node, prefix, pos);
    case NType::NODE_48:
      return CNode48::TraversePrefix(cart, art, node, prefix, pos);
    case NType::NODE_256:
      return CNode256::TraversePrefix(cart, art, node, prefix, pos);
    default:
      throw std::logic_error("TraversePrefix does not support this type");
  }
}

void ConcurrentNode::MergePrefixesDiffer(ConcurrentART& cart, ART& art, ConcurrentNode* l_node, reference<Node>& r_node,
                                         idx_t& left_pos, idx_t& right_pos) {
  assert(l_node->RLocked());
  if (l_node->IsSerialized()) {
    l_node->Upgrade();
    l_node->Deserialize(cart);
    l_node->Downgrade();
  }

  auto r_byte = Prefix::GetByte(art, r_node, right_pos);
  Prefix::Reduce(art, r_node, right_pos);

  auto l_byte = CPrefix::GetByte(cart, *l_node, left_pos);
  ConcurrentNode* l_child = nullptr;

  l_node->Upgrade();
  // NOTE: split will update pointer, which is not needed in this case
  bool retry = CPrefix::Split(cart, l_node, l_child, left_pos);
  assert(!retry);

  assert(l_node->Locked());
  CNode4::New(cart, *l_node);
  assert(l_node->GetType() == NType::NODE_4);
  CNode4::InsertChild(cart, l_node, l_byte, l_child);
  assert(l_node->Locked());

  auto new_child = cart.AllocateNode();
  new_child->Lock();
  new_child->MergeUpdate(cart, art, r_node);
  assert(!new_child->Locked());

  CNode4::InsertChild(cart, l_node, r_byte, new_child);
  l_node->Unlock();
}

// NOTE: this method is too tricky
// NOTE: this is ticky
void ConcurrentNode::MergeNonePrefixByPrefix(ConcurrentART& cart, ART& art, ConcurrentNode* l_node, Node& other,
                                             idx_t l_pos) {
  assert(l_node->RLocked());
  if (l_node->IsSerialized()) {
    l_node->Upgrade();
    l_node->Deserialize(cart);
    l_node->Downgrade();
  }
  assert(l_node->GetType() == NType::PREFIX && other.GetType() != NType::PREFIX);

  auto& cprefix = CPrefix::Get(cart, *l_node);

  ConcurrentNode* r_node = cart.AllocateNode();
  r_node->Lock();
  r_node->MergeUpdate(cart, art, other);
  assert(!r_node->Locked());

  // NOTE: too much memory consumed
  Node new_r;
  ConvertToNode(cart, art, l_node, new_r, l_pos);
  assert(new_r.GetType() == NType::PREFIX);

  cprefix.data[Node::PREFIX_SIZE] = l_pos;
  r_node->RLock();
  l_node->Upgrade();

  // NOTE: cprefix.ptr maybe can be released
  cprefix.ptr = r_node;

  l_node->Unlock();
  l_node = cprefix.ptr;

  assert(l_node->GetType() != NType::PREFIX && new_r.GetType() == NType::PREFIX);

  l_node->MergePrefix(cart, art, new_r);
}

// NOTE: no need to consider locks
void ConcurrentNode::ConvertToNode(ConcurrentART& cart, ART& art, ConcurrentNode* src, Node& dst, idx_t pos) {
  switch (src->GetType()) {
    case NType::LEAF_INLINED:
    case NType::LEAF:
      return CLeaf::ConvertToNode(cart, art, src, dst);
    case NType::PREFIX:
      return CPrefix::ConvertToNode(cart, art, src, dst, pos);
    case NType::NODE_4:
      return CNode4::ConvertToNode(cart, art, src, dst);
    case NType::NODE_16:
      return CNode16::ConvertToNode(cart, art, src, dst);
    case NType::NODE_48:
      return CNode48::ConvertToNode(cart, art, src, dst);
    case NType::NODE_256:
      return CNode256::ConvertToNode(cart, art, src, dst);
    default:
      throw std::logic_error("ConvertToNode does not support this node type");
  }
}

void ConcurrentNode::InsertForMerge(ConcurrentART& cart, ART& art, ConcurrentNode* node, Prefix& other, idx_t pos) {
  assert(node->Locked());
  assert(node->GetType() != NType::LEAF && node->GetType() != NType::LEAF_INLINED && node->GetType() != NType::PREFIX);

  ConcurrentNode* child = cart.AllocateNode();
  child->Lock();
  InsertChild(cart, node, other.data[pos], child);
  if (pos + 1 < other.data[Node::PREFIX_SIZE]) {
    // copy prefix
    Node new_node;
    auto& new_prefix = Prefix::New(art, new_node);
    for (idx_t i = pos + 1; i < other.data[Node::PREFIX_SIZE]; i++) {
      new_prefix.data[i - pos - 1] = other.data[i];
    }
    new_prefix.data[Node::PREFIX_SIZE] = other.data[Node::PREFIX_SIZE] - pos - 1;
    CPrefix::MergeUpdate(cart, art, child, new_node);
    return;
  }
  child->MergeUpdate(cart, art, other.ptr);
}

}  // namespace part