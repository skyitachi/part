//
// Created by Shiping Yao on 2023/12/5.
//
#include "node.h"
#include "fixed_size_allocator.h"
#include "art.h"
#include "node4.h"

namespace part {

FixedSizeAllocator &Node::GetAllocator(const ART &art, NType type) {
  return (*art.allocators)[(uint8_t)type - 1];
}

std::optional<Node*> Node::GetChild(ART &art, const uint8_t byte) const {
  assert(IsSet() && !IsSerialized());

  std::optional<Node*> child;
  switch (GetType()) {
    case NType::NODE_4:
      return Node4::Get(art, *this).GetChild(byte);
    case NType::NODE_16:
      break;
    case NType::NODE_48:
      break;
    case NType::NODE_256:
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
    case NType::NODE_48:
    case NType::NODE_256:
    default:
      throw std::invalid_argument("Invalid node type for InsertChild");
  }

}

}
