//
// Created by skyitachi on 24-1-25.
//
#include "concurrent_node.h"

#include <fmt/core.h>

#include <thread>

#include "concurrent_art.h"
#include "leaf.h"
#include "node16.h"
#include "node4.h"
#include "prefix.h"
#include "node256.h"
#include "node48.h"

namespace part {
constexpr uint32_t RETRY_THRESHOLD = 100;
constexpr uint64_t HAS_WRITER = ~0L;

void ConcurrentNode::RLock() {
  int retry = 0;
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
    }
  }
}

void ConcurrentNode::RUnlock() {
  int retry = 0;
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
    }
  }
}

void ConcurrentNode::Lock() {
  int retry = 0;
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
    }
  }
}

void ConcurrentNode::Unlock() {
  int retry = 0;
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
    }
  }
}

void ConcurrentNode::Downgrade() {
  // one reader
  lock_.store(1);
}

// TODO: may need test
void ConcurrentNode::Upgrade() {
  int retry = 0;
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
std::optional<ConcurrentNode*> ConcurrentNode::GetChild(ConcurrentART& art, const uint8_t byte) const {
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
    default:
      throw std::invalid_argument("ToGraph does not support such node types");
  }
}

void ConcurrentNode::InsertChild(ConcurrentART& art, ConcurrentNode* node, const uint8_t byte, ConcurrentNode* child) {
  if (!node->Locked()) {
    fmt::println("debug point");
  }
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

}  // namespace part