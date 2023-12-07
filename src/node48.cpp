//
// Created by skyitachi on 23-12-7.
//
#include <node48.h>
#include <node16.h>

namespace part {

Node48 &Node48::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::NODE_48).New();
  node.SetType((uint8_t)NType::NODE_48);
  auto &n48 = Node48::Get(art, node);

  n48.count = 0;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = Node::EMPTY_MARKER;
  }

  for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i].Reset();
  }

  return n48;
}

void Node48::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n48 = Node48::Get(art, node);

  if (!n48.count) {
    return;
  }

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    if (n48.child_index[i] != Node::EMPTY_MARKER) {
      Node::Free(art, n48.children[n48.child_index[i]]);
    }
  }

}

Node48 &Node48::GrowNode16(ART &art, Node &node48, Node &node16) {
  auto &n16 = Node16::Get(art, node16);
  auto &n48 = Node48::New(art, node48);

  n48.count = n16.count;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = Node::EMPTY_MARKER;
  }

  for (idx_t i = 0; i < n16.count; i++) {
    n48.child_index[n16.key[i]] = i;
    n48.children[i] = n16.children[i];
  }

  for (idx_t i = n16.count; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i].Reset();
  }

  n16.count = 0;
  Node::Free(art, node16);

  return n48;
}


}