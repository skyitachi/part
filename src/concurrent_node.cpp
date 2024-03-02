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
  if (child && child.value()->IsSerialized()) {
    // TODO: Deserialize
  }
  return child;
}

void ConcurrentNode::ToGraph(ConcurrentART& art, std::ofstream& out, idx_t& id, std::string parent_id) {
  switch (GetType()) {
    case NType::LEAF: {
      id++;
      RLock();
      auto& leaf = CLeaf::Get(art, *this);
      std::string leaf_prefix("LEAF_");
      out << leaf_prefix << id;
      out << "[shape=plain color=green ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
      out << "<TR><TD COLSPAN=\"" << (uint32_t)leaf.count << "\">leaf_" << id << "</TD></TR><TR>\n";
      for (int i = 0; i < leaf.count; i++) {
        out << "<TD>" << leaf.row_ids[i] << "</TD>\n";
      }
      out << "</TR></TABLE>>];\n";
      if (!parent_id.empty()) {
        out << parent_id << "->" << leaf_prefix << id << ";\n";
      }
      RUnlock();
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
        out << "<TD>" << (uint32_t)prefix.data[i] << "</TD>\n";
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
        out << "<TD>" << (uint32_t)node4.key[i] << "</TD>\n";
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
        out << "<TD>" << (uint32_t)node16.key[i] << "</TD>\n";
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
          out << "<TD>" << i << "</TD>\n";
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
          out << "<TD>" << i << "</TD>\n";
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
  if (!IsSet()) {
    return BlockPointer();
  }
  if (IsSerialized()) {
    Deserialize(art);
  }
  switch (GetType()) {
    case NType::PREFIX:
      return CPrefix::Serialize(art, this, serializer);
    case NType::NODE_4:
      return CNode4::Serialize(art, this, serializer);
    case NType::LEAF:
      return CLeaf::Serialize(art, this, serializer);
    case NType::LEAF_INLINED:
      return CLeaf::Serialize(art, this, serializer);
    default:
      throw std::invalid_argument("invalid type for serialize");
  }
}

void ConcurrentNode::Deserialize(ConcurrentART& art) {
  assert(IsSet() && IsSerialized());

  BlockPointer pointer(GetBufferId(), GetOffset());
  BlockDeserializer reader(art.GetIndexFileFd(), pointer);
  // NOTE: important
  Reset();
  auto type = reader.Read<uint8_t>();

  SetType(type);

  auto decoded_type = GetType();

  if (decoded_type == NType::PREFIX) {
    return CPrefix::Deserialize(art, this, reader);
  }

  if (decoded_type == NType::LEAF_INLINED) {
    return SetDocID(reader.Read<idx_t>());
  }

  if (decoded_type == NType::LEAF) {
    return CLeaf::Deserialize(art, this, reader);
  }

  this->Update(ConcurrentNode::GetAllocator(art, decoded_type).ConcNew());
  SetType(uint8_t(decoded_type));

  switch (decoded_type) {
    case NType::NODE_4:
      return CNode4::Deserialize(art, this, reader);
    default:
      throw std::invalid_argument("other type deserializer not supported");
  }
}

void ConcurrentNode::Merge(ConcurrentART& cart, ART& art, Node& other) {
  RLock();
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
  assert(Locked() && !IsSet());
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

bool ConcurrentNode::ResolvePrefixes(ConcurrentART& cart, ART& art, Node& other) {
  assert(RLocked());
  if (GetType() != NType::PREFIX && other.GetType() != NType::PREFIX) {
    return MergeInternal(cart, art, other);
  }

  auto l_node = this;
  auto r_node = std::ref(other);

  idx_t mismatch_position = INVALID_INDEX;

  // case1: both are prefix nodes, l_node won't be updated
  if (l_node->GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
    if (!CPrefix::Traverse(cart, art, l_node, r_node, mismatch_position)) {
      return false;
    }

    if (mismatch_position == INVALID_INDEX) {
      return true;
    }
  } else {
    // NOTE: no need swap
    mismatch_position = 0;
  }

  assert(mismatch_position != INVALID_INDEX);

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

  // case 4: prefixes differ at a specific byte
  MergePrefixesDiffer(cart, art, l_node, r_node, mismatch_position);

  return false;
}

bool ConcurrentNode::MergeInternal(ConcurrentART& cart, ART& art, Node& other) {
  assert(RLocked());
  assert(IsSet() && other.IsSet());
  assert(GetType() != NType::PREFIX && other.GetType() != NType::PREFIX);

  // NOTE: cannot swap

  auto cnode = this;
  auto& node = other;

  auto is_leaf = cnode->GetType() == NType::LEAF || cnode->GetType() == NType::LEAF_INLINED;
  auto right_leaf = node.GetType() == NType::LEAF || node.GetType() == NType::LEAF_INLINED;
  if (is_leaf && right_leaf) {
    Upgrade();
    cnode->MergeUpdate(cart, art, other);
    return true;
  }

  uint8_t byte = 0;
  auto r_child = node.GetNextChild(art, byte);

  while (r_child) {
    assert(cnode->RLocked());
    auto l_child = cnode->GetChild(cart, byte);
    if (!l_child) {
      auto new_child = cart.AllocateNode();
      cnode->Upgrade();
      InsertChild(cart, cnode, byte, new_child);
      // NOTE: important order matters
      new_child->Lock();
      // must use downgrade
      cnode->Downgrade();
      new_child->MergeUpdate(cart, art, *r_child.value());
    } else {
      l_child.value()->RLock();
      cnode->RUnlock();
      if (!l_child.value()->ResolvePrefixes(cart, art, *r_child.value())) {
        return false;
      }
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

// TODO: none leaf node merge prefix node
bool ConcurrentNode::MergePrefix(ConcurrentART& cart, ART& art, Node& other) {
  assert(RLocked());
  // NOTE: can not be leaf type
  assert(GetType() != NType::PREFIX || GetType() != NType::LEAF_INLINED || GetType() != NType::LEAF);

  auto l_node = this;
  idx_t pos = 0;
  auto& prefix = Prefix::Get(art, other);
  switch (l_node->GetType()) {
    case NType::NODE_4:
      return CNode4::TraversePrefix(cart, art, l_node, prefix, pos);
    case NType::NODE_16:

    default:
      throw std::logic_error("MergePrefix not support the node type");
  }

  return false;
}

// NOTE: return value indicated whether other was merged inot cart
bool ConcurrentNode::TraversePrefix(ConcurrentART& cart, ART& art, ConcurrentNode*& node, Prefix& prefix, idx_t& pos) {
  assert(node->RLocked());
  assert(node->GetType() != NType::LEAF && node->GetType() != NType::LEAF_INLINED);
  switch (node->GetType()) {
    case NType::PREFIX:
      return CPrefix::TraversePrefix(cart, art, node, prefix, 0, pos);
    case NType::NODE_4:
      return CNode4::TraversePrefix(cart, art, node, prefix, pos);
    default:
      throw std::logic_error("TraversePrefix does not suppor this type");
  }
}

void ConcurrentNode::MergePrefixesDiffer(ConcurrentART& cart, ART& art, ConcurrentNode* l_node, reference<Node>& r_node,
                                         idx_t& mismatched_position) {
  assert(l_node->RLocked());
  auto r_byte = Prefix::GetByte(art, r_node, mismatched_position);
  Prefix::Reduce(art, r_node, mismatched_position);

  auto l_byte = CPrefix::GetByte(cart, *l_node, mismatched_position);
  ConcurrentNode* l_child = nullptr;

  l_node->Upgrade();
  CPrefix::Split(cart, l_node, l_child, mismatched_position);
  assert(l_node->Locked());

  l_node->Update(GetAllocator(cart, NType::NODE_4).ConcNew());
  CNode4::InsertChild(cart, l_node, l_byte, l_child);
  assert(l_node->Locked());

  auto new_child = cart.AllocateNode();
  new_child->Lock();
  new_child->MergeUpdate(cart, art, r_node);
  assert(!new_child->Locked());

  CNode4::InsertChild(cart, l_node, r_byte, new_child);
  l_node->Unlock();
}

void ConcurrentNode::MergeNonePrefixByPrefix(ConcurrentART& cart, ART& art, ConcurrentNode* l_node, Node& other) {
  assert(l_node->Locked());
  assert(l_node->GetType() == NType::PREFIX && other.GetType() != NType::PREFIX);

  ConcurrentNode* r_node = cart.AllocateNode();
  r_node->MergeUpdate(cart, art, other);
  // swap l_node and r_node
  ConcurrentNode temp = *l_node;
  l_node->Update(r_node);
  r_node->Update(&temp);

  assert(l_node->GetType() != NType::PREFIX && r_node->GetType() == NType::PREFIX);

  Node new_r;
  ConvertToNode(cart, art, r_node, new_r);

  l_node->MergePrefix(cart, art, new_r);
}

// NOTE: no need to consider locks
void ConcurrentNode::ConvertToNode(ConcurrentART& cart, ART& art, ConcurrentNode* src, Node& dst) {
  switch (src->GetType()) {
    case NType::LEAF_INLINED:
    case NType::LEAF:
      return CLeaf::ConvertToNode(cart, art, src, dst);
    case NType::PREFIX:
      return CPrefix::ConvertToNode(cart, art, src, dst);
    case NType::NODE_4:
      return CNode4::ConvertToNode(cart, art, src, dst);
    // TODO: implement new node type
    default:
      throw std::logic_error("ConvertToNode does not support this node type");
  }
}

}  // namespace part