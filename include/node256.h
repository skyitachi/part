//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE256_H
#define PART_NODE256_H
#include "concurrent_node.h"
#include "fixed_size_allocator.h"
#include "node.h"

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

  static void DeleteChild(ART &art, Node &node, const uint8_t byte);

  inline void ReplaceChild(const uint8_t byte, const Node child) { children[byte] = child; }

  std::optional<Node *> GetChild(const uint8_t byte);

  std::optional<Node *> GetNextChild(uint8_t &byte);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);
};

class CNode256 {
 public:
  uint16_t count;

  ConcurrentNode *children[Node::NODE_256_CAPACITY];

  static CNode256 &New(ConcurrentART &art, ConcurrentNode &node);

  static void Free(ConcurrentART &art, ConcurrentNode *node);

  static inline CNode256 &Get(const ConcurrentART &art, const ConcurrentNode *ptr) {
    assert(ptr->Locked() || ptr->RLocked());
    assert(!ptr->IsSerialized());
    return *ConcurrentNode::GetAllocator(art, NType::NODE_256).Get<CNode256>(*ptr);
  }

  static CNode256 &GrowNode48(ConcurrentART &art, ConcurrentNode *node48);

  static void InsertChild(ConcurrentART &art, ConcurrentNode *node, const uint8_t byte, ConcurrentNode *child);

  std::optional<ConcurrentNode *> GetChild(uint8_t byte);

  static void MergeUpdate(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other);

  static bool TraversePrefix(ConcurrentART &cart, ART &art, ConcurrentNode *node, reference<Node> &other, idx_t &pos);

  static void ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst);

  static BlockPointer Serialize(ConcurrentART &art, ConcurrentNode *node, Serializer &serializer);

  static void Deserialize(ConcurrentART &art, ConcurrentNode *node, Deserializer &deserializer);
};
}  // namespace part
#endif  // PART_NODE256_H
