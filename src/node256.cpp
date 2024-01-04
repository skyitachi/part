//
// Created by skyitachi on 23-12-7.
//
#include <node256.h>
#include <node48.h>

namespace part {

Node256 &Node256::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::NODE_256).New();
  node.SetType((uint8_t)NType::NODE_256);

  auto &n256 = Node256::Get(art, node);
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n256.children[i].Reset();
  }

  return n256;
}

void Node256::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n256 = Node256::Get(art, node);

  if (!n256.count) {
    return;
  }

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    if (n256.children[i].IsSet()) {
      Node::Free(art, n256.children[i]);
    }
  }
}

Node256 &Node256::GrowNode48(ART &art, Node &node256, Node &node48) {
  auto &n48 = Node48::Get(art, node48);
  auto &n256 = Node256::New(art, node256);

  n256.count = n48.count;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    if (n48.child_index[i] != Node::EMPTY_MARKER) {
      n256.children[i] = n48.children[n48.child_index[i]];
    } else {
      n256.children[i].Reset();
    }
  }

  n48.count = 0;
  Node::Free(art, node48);
  return n256;
}

void Node256::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n256 = Node256::Get(art, node);

  assert(!n256.children[byte].IsSet());

  n256.count++;
  assert(n256.count <= Node::NODE_256_CAPACITY);
  n256.children[byte] = child;
}

std::optional<Node *> Node256::GetChild(const uint8_t byte) {
  if (children[byte].IsSet()) {
    return &children[byte];
  }

  return std::nullopt;
}

BlockPointer Node256::Serialize(ART &art, Node &node, Serializer &writer) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n256 = Node256::Get(art, node);

  std::vector<BlockPointer> child_block_pointers;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    child_block_pointers.emplace_back(n256.children[i].Serialize(art, writer));
  }

  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::NODE_256);
  writer.Write(n256.count);

  for (auto &child_block_pointer : child_block_pointers) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }

  return block_pointer;
}

void Node256::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto &n256 = Node256::Get(art, node);
  n256.count = reader.Read<uint16_t>();

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n256.children[i] = Node(reader);
  }
}
void Node256::DeleteChild(ART &art, Node &node, const uint8_t byte) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n256 = Node256::Get(art, node);

  Node::Free(art, n256.children[byte]);
  n256.children[byte].Reset();
  n256.count--;

  if (n256.count < Node::NODE_256_SHRINK_THRESHOLD) {
    auto &node256 = node;
    Node48::ShrinkNode256(art, node, node256);
  }

}
}  // namespace part