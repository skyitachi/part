//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE16_H
#define PART_NODE16_H

#include "concurrent_node.h"
#include "fixed_size_allocator.h"
#include "node.h"

namespace part {

class Node16 {
 public:
  Node16(const Node16 &) = delete;
  Node16 &operator=(const Node16 &) = delete;

  uint8_t count;

  uint8_t key[Node::NODE_16_CAPACITY];

  Node children[Node::NODE_16_CAPACITY];

  static inline Node16 &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::NODE_16).Get<Node16>(ptr);
  }

  static Node16 &New(ART &art, Node &node);

  static void Free(ART &art, Node &node);

  static Node16 &GrowNode4(ART &art, Node &node16, Node &node4);

  static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);

  static void DeleteChild(ART &art, Node &node, const uint8_t byte);

  static Node16 &ShrinkNode48(ART &art, Node &node16, Node &node48);

  void ReplaceChild(const uint8_t byte, const Node child);

  std::optional<Node *> GetChild(const uint8_t byte);

  std::optional<Node *> GetNextChild(uint8_t &byte);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);
};

class CNode16 {
 public:
  CNode16(const CNode16 &) = delete;
  CNode16 &operator=(const CNode16 &) = delete;

  uint8_t count;

  uint8_t key[Node::NODE_16_CAPACITY];

  ConcurrentNode *children[Node::NODE_16_CAPACITY];

  static inline CNode16 &Get(const ConcurrentART &art, const ConcurrentNode *ptr) {
    assert(ptr->RLocked() || ptr->Locked());
    assert(!ptr->IsSerialized());
    return *ConcurrentNode::GetAllocator(art, NType::NODE_16).Get<CNode16>(*ptr);
  }

  static CNode16 &New(ConcurrentART &art, ConcurrentNode &node);

  static void Free(ConcurrentART &art, ConcurrentNode *node);

  static CNode16 &GrowNode4(ConcurrentART &art, ConcurrentNode *node4);

  static void InsertChild(ConcurrentART &art, ConcurrentNode *node, const uint8_t byte, ConcurrentNode *child);

  std::optional<ConcurrentNode *> GetChild(const uint8_t byte);
};

}  // namespace part

#endif  // PART_NODE16_H
