//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE48_H
#define PART_NODE48_H

#include "fixed_size_allocator.h"
#include "node.h"

namespace part {

class Node48 {
 public:
  uint8_t count;

  uint8_t child_index[Node::NODE_256_CAPACITY];

  Node children[Node::NODE_48_CAPACITY];

  static Node48 &New(ART &art, Node &node);

  static void Free(ART &art, Node &node);

  static inline Node48 &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::NODE_48).Get<Node48>(ptr);
  }

  static Node48 &GrowNode16(ART &art, Node &node48, Node &node16);

  static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);

  static void DeleteChild(ART &art, Node &node, const uint8_t byte);

  static Node48 &ShrinkNode256(ART &art, Node &node48, Node &node256);

  void ReplaceChild(const uint8_t byte, const Node child);

  std::optional<Node *> GetChild(const uint8_t byte);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);
};
}  // namespace part
#endif  // PART_NODE48_H
