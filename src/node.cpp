//
// Created by Shiping Yao on 2023/12/5.
//
#include "node.h"

#include <node256.h>
#include <node48.h>

#include "art.h"
#include "fixed_size_allocator.h"
#include "leaf.h"
#include "node16.h"
#include "node4.h"
#include "prefix.h"

namespace part {

Node::Node(Deserializer &reader) {
  auto block_id = reader.Read<block_id_t>();
  auto offset = reader.Read<uint32_t>();

  Reset();

  if (block_id == INVALID_BLOCK) {
    return;
  }

  SetSerialized();
  SetPtr(block_id, offset);
}

Node::Node(ART &art, Deserializer &reader) : Node(reader) {
  if (IsSet() && IsSerialized()) {
    Deserialize(art);
  }
}

FixedSizeAllocator &Node::GetAllocator(const ART &art, NType type) { return (*art.allocators)[(uint8_t)type - 1]; }

std::optional<Node *> Node::GetChild(ART &art, const uint8_t byte) const {
  assert(IsSet() && !IsSerialized());

  std::optional<Node *> child;
  switch (GetType()) {
    case NType::NODE_4:
      child = Node4::Get(art, *this).GetChild(byte);
      break;
    case NType::NODE_16:
      child = Node16::Get(art, *this).GetChild(byte);
      break;
    case NType::NODE_48:
      child = Node48::Get(art, *this).GetChild(byte);
      break;
    case NType::NODE_256:
      child = Node256::Get(art, *this).GetChild(byte);
      break;
    default:
      throw std::invalid_argument("Invalid node type for GetChild");
  }
  if (child && child.value()->IsSerialized()) {
    child.value()->Deserialize(art);
  }
  return child;
}

void Node::InsertChild(ART &art, Node &node, uint8_t byte, const Node child) {
  // assert node RLOCKED
  switch (node.GetType()) {
    case NType::NODE_4:
      Node4::InsertChild(art, node, byte, child);
      break;
    case NType::NODE_16:
      Node16::InsertChild(art, node, byte, child);
      break;
    case NType::NODE_48:
      Node48::InsertChild(art, node, byte, child);
      break;
    case NType::NODE_256:
      Node256::InsertChild(art, node, byte, child);
      break;
    default:
      throw std::invalid_argument(fmt::format("Invalid node type for InsertChild type: {}", (uint8_t)node.GetType()));
  }
}

void Node::Free(ART &art, Node &node) {
  if (!node.IsSet()) {
    return;
  }

  if (!node.IsSerialized()) {
    auto type = node.GetType();
    switch (type) {
      case NType::LEAF:
        return Leaf::Free(art, node);
      case NType::PREFIX:
        return Prefix::Free(art, node);
      case NType::LEAF_INLINED:
        return node.Reset();
      case NType::NODE_4:
        Node4::Free(art, node);
        break;
      case NType::NODE_16:
        Node16::Free(art, node);
        break;
      case NType::NODE_48:
        Node48::Free(art, node);
        break;
      case NType::NODE_256:
        Node256::Free(art, node);
        break;
    }

    Node::GetAllocator(art, type).Free(node);
  }

  node.Reset();
}

BlockPointer Node::Serialize(ART &art, Serializer &serializer) {
  // NOTE: important
  if (!IsSet()) {
    return BlockPointer();
  }
  if (IsSerialized()) {
    Deserialize(art);
  }

  switch (GetType()) {
    case NType::PREFIX:
      return Prefix::Serialize(art, *this, serializer);
    case NType::LEAF:
      return Leaf::Serialize(art, *this, serializer);
    case NType::NODE_4:
      return Node4::Serialize(art, *this, serializer);
    case NType::NODE_16:
      return Node16::Serialize(art, *this, serializer);
    case NType::NODE_48:
      return Node48::Serialize(art, *this, serializer);
    case NType::NODE_256:
      return Node256::Serialize(art, *this, serializer);
    case NType::LEAF_INLINED:
      return Leaf::Serialize(art, *this, serializer);
    default:
      throw std::invalid_argument("invalid type for serialize");
  }
}

void Node::Deserialize(ART &art) {
  assert(IsSet() && IsSerialized());

  BlockPointer pointer(GetBufferId(), GetOffset());
  BlockDeserializer reader(art.GetIndexFileFd(), pointer);
  // NOTE: important
  Reset();
  auto type = reader.Read<uint8_t>();

  SetType(type);

  auto decoded_type = GetType();

  if (decoded_type == NType::PREFIX) {
    return Prefix::Deserialize(art, *this, reader);
  }

  if (decoded_type == NType::LEAF_INLINED) {
    return SetDocID(reader.Read<idx_t>());
  }

  if (decoded_type == NType::LEAF) {
    return Leaf::Deserialize(art, *this, reader);
  }

  *this = Node::GetAllocator(art, decoded_type).New();
  SetType(uint8_t(decoded_type));

  switch (decoded_type) {
    case NType::NODE_4:
      return Node4::Deserialize(art, *this, reader);
    case NType::NODE_16:
      return Node16::Deserialize(art, *this, reader);
    case NType::NODE_48:
      return Node48::Deserialize(art, *this, reader);
    case NType::NODE_256:
      return Node256::Deserialize(art, *this, reader);
    default:
      throw std::invalid_argument("other type deserializer not supported");
  }
}

void Node::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte) {
  switch (node.GetType()) {
    case NType::NODE_4:
      return Node4::DeleteChild(art, node, prefix, byte);
    case NType::NODE_16:
      return Node16::DeleteChild(art, node, byte);
    case NType::NODE_48:
      return Node48::DeleteChild(art, node, byte);
    case NType::NODE_256:
      return Node256::DeleteChild(art, node, byte);
    default:
      throw std::invalid_argument("Invalid node type for DeleteChild.");
  }
}

void Node::ReplaceChild(const ART &art, const uint8_t byte, const Node child) {
  switch (GetType()) {
    case NType::NODE_4:
      return Node4::Get(art, *this).ReplaceChild(byte, child);
    case NType::NODE_16:
      return Node16::Get(art, *this).ReplaceChild(byte, child);
    case NType::NODE_48:
      return Node48::Get(art, *this).ReplaceChild(byte, child);
    case NType::NODE_256:
      return Node256::Get(art, *this).ReplaceChild(byte, child);
    default:
      throw std::invalid_argument("Invalid node type for ReplaceChild.");
  }
}

void Node::Merge(ART &art, Node &other) {
  assert(IsSet());
  if (!IsSet()) {
    *this = other;
    other = Node();
    return;
  }
  ResolvePrefixes(art, other);
}

bool Node::ResolvePrefixes(ART &art, Node &other) {
  assert(IsSet() && other.IsSet());

  // case 1: both nodes have no prefix
  if (GetType() != NType::PREFIX && other.GetType() != NType::PREFIX) {
    return MergeInternal(art, other);
  }

  auto l_node = std::ref(*this);
  auto r_node = std::ref(other);

  idx_t mismatch_position = INVALID_INDEX;

  // traverse prefixes
  if (l_node.get().GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
    if (!Prefix::Traverse(art, l_node, r_node, mismatch_position)) {
      return false;
    }

    if (mismatch_position == INVALID_INDEX) {
      return true;
    }
  } else {
    if (l_node.get().GetType() == NType::PREFIX) {
      std::swap(*this, other);
    }
    mismatch_position = 0;
  }

  assert(mismatch_position != INVALID_INDEX);

  // case 2: none prefix type merge with prefix type node
  if (l_node.get().GetType() != NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
    return MergePrefixContainsOtherPrefix(art, l_node, r_node, mismatch_position);
  }

  // case 3:prefixes differ at a specific byte
  MergePrefixesDiffer(art, l_node, r_node, mismatch_position);

  return true;
}

bool Node::MergeInternal(ART &art, Node &other) {
  assert(IsSet() && other.IsSet());
  assert(GetType() != NType::PREFIX && other.GetType() != NType::PREFIX);

  // merge smaller nodes into bigger node
  if (GetType() < other.GetType()) {
    std::swap(*this, other);
  }

  Node empty_node;
  auto &l_node = *this;
  auto &r_node = other;

  // l->GetType() >= r->GetType()
  if (l_node.GetType() == NType::LEAF || r_node.GetType() == NType::LEAF_INLINED) {
    assert(r_node.GetType() == NType::LEAF || l_node.GetType() == NType::LEAF_INLINED);

    Leaf::Merge(art, l_node, r_node);
    return true;
  }

  uint8_t byte = 0;
  auto r_child = r_node.GetNextChild(art, byte);

  while (r_child) {
    auto l_child = l_node.GetChild(art, byte);
    if (!l_child) {
      InsertChild(art, l_node, byte, *r_child.value());
      r_node.ReplaceChild(art, byte, empty_node);
    } else {
      // recursive
      if (!l_child.value()->ResolvePrefixes(art, *r_child.value())) {
        return false;
      }
    }
    if (byte == std::numeric_limits<uint8_t>::max()) {
      break;
    }
    byte++;
    r_child = r_node.GetNextChild(art, byte);
  }

  // NOTE: memory management
  Free(art, r_node);
  return true;
}

std::optional<Node *> Node::GetNextChild(ART &art, uint8_t &byte) const {
  assert(IsSet());
  switch (GetType()) {
    case NType::NODE_4:
      return Node4::Get(art, *this).GetNextChild(byte);
    case NType::NODE_16:
      return Node16::Get(art, *this).GetNextChild(byte);
    case NType::NODE_48:
      return Node48::Get(art, *this).GetNextChild(byte);
    case NType::NODE_256:
      return Node256::Get(art, *this).GetNextChild(byte);
    default:
      throw std::invalid_argument("Invalid node type for GetNextChild");
  }
}

void Node::MergePrefixesDiffer(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatched_position) {
  Node l_child;
  auto l_byte = Prefix::GetByte(art, l_node, mismatched_position);
  Prefix::Split(art, l_node, l_child, mismatched_position);

  Node4::New(art, l_node);
  Node4::InsertChild(art, l_node, l_byte, l_child);
  auto r_byte = Prefix::GetByte(art, r_node, mismatched_position);
  // reduce
  Prefix::Reduce(art, r_node, mismatched_position);
  // NOTE: child will copy r_node
  Node4::InsertChild(art, l_node, r_byte, r_node);

  // NOTE: InsertChild already copy child, so need to reset here
  r_node.get().Reset();
}

bool Node::MergePrefixContainsOtherPrefix(ART &art, reference<Node> &l_node, reference<Node> &r_node,
                                          idx_t &mismatch_position) {
  // r_node's prefix contains l_node's prefix
  assert(l_node.get().GetType() != NType::LEAF && l_node.get().GetType() != NType::LEAF_INLINED);

  auto mismatch_byte = Prefix::GetByte(art, r_node, mismatch_position);
  auto child_node = l_node.get().GetChild(art, mismatch_byte);

  Prefix::Reduce(art, r_node, mismatch_position);

  if (!child_node) {
    Node::InsertChild(art, l_node, mismatch_byte, r_node);
    r_node.get().Reset();
    return true;
  }

  return child_node.value()->ResolvePrefixes(art, r_node);
}

void Node::ToGraph(ART &art, std::ofstream &out, idx_t &id, std::string parent_id) {
  switch (GetType()) {
    case NType::LEAF: {
      id++;
      auto &leaf = Leaf::Get(art, *this);
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
      auto &prefix = Prefix::Get(art, *this);
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

      if (prefix.ptr.IsSet()) {
        prefix.ptr.ToGraph(art, out, id, current_id_str);
      }
      break;
    }
    case NType::NODE_4: {
      id++;
      std::string node_prefix("NODE4_");
      std::string current_id_str = fmt::format("{}{}", node_prefix, id);
      auto &node4 = Node4::Get(art, *this);
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

      for (int i = 0; i < node4.count; i++) {
        node4.children[i].ToGraph(art, out, id, current_id_str);
      }
      break;
    }
    case NType::NODE_16: {
      id++;
      std::string node_prefix("NODE16_");
      std::string current_id_str = fmt::format("{}{}", node_prefix, id);
      auto &node16 = Node16::Get(art, *this);
      out << node_prefix << id;
      out << "[shape=plain color=yellow ";
      out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";

      out << "<TR><TD COLSPAN=\"" << (uint32_t)node16.count << "\"> node16_" << id << "</TD></TR>\n";
      out << "<TR>\n";
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
        node16.children[i].ToGraph(art, out, id, current_id_str);
      }
      break;
    }
  }
}

}  // namespace part
