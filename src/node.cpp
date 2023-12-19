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
  auto previous_block_pointer = reader.GetBlockPointer();
  auto block_id = reader.Read<block_id_t>();
  auto offset = reader.Read<uint32_t>();

  Reset();

  if (block_id == INVALID_BLOCK) {
    return;
  }

  SetSerialized();
  SetPtr(block_id, offset);
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

}  // namespace part
