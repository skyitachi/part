//
// Created by Shiping Yao on 2023/12/5.
//
#include "node.h"
#include "fixed_size_allocator.h"
#include "art.h"
#include "node4.h"
#include "prefix.h"
#include "node16.h"
#include <node48.h>
#include <node256.h>
#include "leaf.h"

namespace part {

FixedSizeAllocator &Node::GetAllocator(const ART &art, NType type) {
  return (*art.allocators)[(uint8_t)type - 1];
}

std::optional<Node*> Node::GetChild(ART &art, const uint8_t byte) const {
  assert(IsSet() && !IsSerialized());

  std::optional<Node*> child;
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
    // TODO: deserialized
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
    default:
      throw std::invalid_argument("Invalid node type for InsertChild");
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

}
