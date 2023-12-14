//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE256_H
#define PART_NODE256_H
#include "node.h"
#include "fixed_size_allocator.h"

namespace part {

class Node256 {

public:
  uint16_t count;

  Node children[Node::NODE_256_CAPACITY];

  static Node256 &New(ART &art, Node &node);

  static void Free(ART &art, Node &node);

  static inline Node256 &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::NODE_256).Get<Node256>(ptr);
  }

  static Node256 &GrowNode48(ART &art, Node &node256, Node &node48);

  static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);

  std::optional<Node *> GetChild(const uint8_t byte);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);
};
}
#endif // PART_NODE256_H
