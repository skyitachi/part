//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE4_H
#define PART_NODE4_H

#include "fixed_size_allocator.h"
#include "node.h"

namespace part {

class Node4 {
 public:
  //! Number of non-null children
  uint8_t count;
  //! Array containing all partial key bytes
  uint8_t key[Node::NODE_4_CAPACITY];
  //! ART node pointers to the child nodes
  Node children[Node::NODE_4_CAPACITY];

  static Node4 &New(ART &art, Node &node);

  static void Free(ART &art, Node &node);

  //! Get a reference to the node
  static inline Node4 &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::NODE_4).Get<Node4>(ptr);
  }

  std::optional<Node *> GetChild(const uint8_t byte);

  std::optional<Node *> GetNextChild(uint8_t &byte);

  static void InsertChild(ART &art, Node &node, uint8_t byte, const Node child);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

  static Node4 &ShrinkNode16(ART &art, Node &node4, Node &node16);

  void ReplaceChild(const uint8_t byte, const Node child);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);
};
}  // namespace part

#endif  // PART_NODE4_H
