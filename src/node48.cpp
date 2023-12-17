//
// Created by skyitachi on 23-12-7.
//
#include <node16.h>
#include <node256.h>
#include <node48.h>

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

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte,
                         const Node child) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n48 = Node48::Get(art, node);

  assert(n48.child_index[byte] == Node::EMPTY_MARKER);

  if (n48.count < Node::NODE_48_CAPACITY) {
    idx_t child_pos = n48.count;
    if (n48.children[child_pos].IsSet()) {
      child_pos = 0;
      while (n48.children[child_pos].IsSet()) {
        child_pos++;
      }
      n48.children[child_pos] = child;
      n48.child_index[byte] = child_pos;
      n48.count++;
    }
  } else {
    auto node48 = node;
    Node256::GrowNode48(art, node, node48);
    Node256::InsertChild(art, node, byte, child);
  }
}

std::optional<Node *> Node48::GetChild(const uint8_t byte) {
  if (child_index[byte] != Node::EMPTY_MARKER) {
    assert(children[child_index[byte]].IsSet());
    return &children[child_index[byte]];
  }
  return std::nullopt;
}

BlockPointer Node48::Serialize(ART &art, Node &node, Serializer &writer) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n48 = Node48::Get(art, node);

  std::vector<BlockPointer> child_pointer_blocks;
  for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
    child_pointer_blocks.emplace_back(n48.children[i].Serialize(art, writer));
  }
  auto block_pointer = writer.GetBlockPointer();

  writer.Write(NType::NODE_48);
  writer.Write<uint8_t>(n48.count);

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    writer.Write(n48.child_index[i]);
  }

  for (auto &child_block_pointer : child_pointer_blocks) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }

  return block_pointer;
}

void Node48::Deserialize(ART &art, Node &node, Deserializer &reader) {

  auto &n48 = Node48::Get(art, node);
  n48.count = reader.Read<uint8_t>();

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = reader.Read<uint8_t>();
  }

  for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i] = Node(reader);
  }
}

} // namespace part