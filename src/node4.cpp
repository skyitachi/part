//
// Created by Shiping Yao on 2023/12/6.
//
#include <node16.h>
#include <node4.h>

namespace part {

Node4 &Node4::New(ART &art, Node &node) {

  node = Node::GetAllocator(art, NType::NODE_4).New();
  node.SetType((uint8_t)NType::NODE_4);
  auto &n4 = Node4::Get(art, node);

  n4.count = 0;
  return n4;
}

std::optional<Node *> Node4::GetChild(const uint8_t byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      assert(children[i].IsSet());
      return &children[i];
    }
  }
  return std::nullopt;
}

void Node4::InsertChild(ART &art, Node &node, uint8_t byte, const Node child) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n4 = Node4::Get(art, node);

  for (idx_t i = 0; i < n4.count; i++) {
    assert(n4.key[i] != byte);
  }

  if (n4.count < Node::NODE_4_CAPACITY) {
    idx_t child_pos = 0;
    while (child_pos < n4.count && n4.key[child_pos] < byte) {
      child_pos++;
    }

    for (idx_t i = n4.count; i > child_pos; i--) {
      n4.key[i] = n4.key[i - 1];
      n4.children[i] = n4.children[i - 1];
    }

    n4.key[child_pos] = byte;
    n4.children[child_pos] = child;
    n4.count++;
  } else {
    auto node4 = node;
    Node16::GrowNode4(art, node, node4);
    Node16::InsertChild(art, node, byte, child);
  }
}

void Node4::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n4 = Node4::Get(art, node);

  for (idx_t i = 0; i < n4.count; i++) {
    Node::Free(art, n4.children[i]);
  }
}

BlockPointer Node4::Serialize(ART &art, Node &node, Serializer &writer) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n4 = Node4::Get(art, node);
  std::vector<BlockPointer> child_block_pointers;

  for (idx_t i = 0; i < n4.count; i++) {
    child_block_pointers.emplace_back(n4.children[i].Serialize(art, writer));
  }
  for (idx_t i = n4.count; i < Node::NODE_4_CAPACITY; i++) {
    child_block_pointers.emplace_back((block_id_t)INVALID_INDEX, 0);
  }
  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::NODE_4);
  writer.Write<uint8_t>(n4.count);

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    writer.Write(n4.key[i]);
  }

  for (auto &child_block_pointer : child_block_pointers) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }
  return block_pointer;
}

void Node4::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto ref_node = std::ref(node);
  auto &n4 = Node4::Get(art, ref_node);
  auto count = reader.Read<uint8_t>();

  n4.count = count;

  for (idx_t i = 0; i < count; i++) {
    n4.key[i] = reader.Read<uint8_t>();
  }

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    n4.children[i] = Node(reader);
  }
}

} // namespace part