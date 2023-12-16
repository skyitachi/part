//
// Created by skyitachi on 23-12-7.
//
#include "node16.h"
#include "node4.h"

namespace part {

Node16 &Node16::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::NODE_16).New();
  node.SetType((uint8_t)NType::NODE_16);

  auto &n16 = Node16::Get(art, node);
  n16.count = 0;
  return n16;
}

Node16 &part::Node16::GrowNode4(ART &art, Node &node16, Node &node4) {
  auto &n4 = Node4::Get(art, node4);
  auto &n16 = New(art, node16);

  n16.count = n4.count;
  for (idx_t i = 0; i < n4.count; i++) {
    n16.key[i] = n4.key[i];
    n16.children[i] = n4.children[i];
  }

  n4.count = 0;
  Node::Free(art, node4);
  return n16;
}

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte,
                         const Node child) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n16 = Node16::Get(art, node);

  for (idx_t i = 0; i < n16.count; i++) {
    assert(n16.key[i] != byte);
  }

  if (n16.count < Node::NODE_16_CAPACITY) {
    idx_t child_pos = 0;

    while (child_pos < n16.count && n16.key[child_pos] < byte) {
      child_pos++;
    }

    for (idx_t i = n16.count; i > child_pos; i--) {
      n16.key[i] = n16.key[i - 1];
      n16.children[i] = n16.children[i - 1];
    }

    n16.key[child_pos] = byte;
    n16.children[child_pos] = child;
    n16.count++;
  } else {
    // TODO: node48
  }
}

std::optional<Node *> Node16::GetChild(const uint8_t byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      assert(children[i].IsSet());
      return &children[i];
    }
  }

  return std::nullopt;
}

void Node16::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n16 = Node16::Get(art, node);

  for (idx_t i = 0; i < n16.count; i++) {
    Node::Free(art, n16.children[i]);
  }
}

BlockPointer Node16::Serialize(ART &art, Node &node, Serializer &serializer) {
  return BlockPointer();
}

void Node16::Deserialize(ART &art, Node &node, Deserializer &deserializer) {}

} // namespace part
