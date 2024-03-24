//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE48_H
#define PART_NODE48_H

#include "concurrent_art.h"
#include "concurrent_node.h"
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

  std::optional<Node *> GetNextChild(uint8_t &byte);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);
};

class CNode48 {
 public:
  uint8_t count;

  uint8_t child_index[Node::NODE_256_CAPACITY];

  //  ConcurrentNode *children[Node::NODE_48_CAPACITY];

  union {
    uint64_t node;
    ConcurrentNode *ptr;
  } children[Node::NODE_48_CAPACITY];

  static CNode48 &New(ConcurrentART &art, ConcurrentNode &node);

  static void Free(ConcurrentART &art, ConcurrentNode *node);

  static void ShallowFree(ConcurrentART &art, ConcurrentNode *node);

  static inline CNode48 &Get(const ConcurrentART &art, const ConcurrentNode *ptr) {
    assert(ptr->Locked() || ptr->RLocked());
    assert(!ptr->IsSerialized());
    return *ConcurrentNode::GetAllocator(art, NType::NODE_48).Get<CNode48>(*ptr);
  }

  static CNode48 &GrowNode16(ConcurrentART &art, ConcurrentNode *node16);

  static void InsertChild(ConcurrentART &art, ConcurrentNode *node, uint8_t byte, ConcurrentNode *child);

  std::optional<ConcurrentNode *> GetChild(uint8_t byte);

  static void MergeUpdate(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other);

  static bool TraversePrefix(ConcurrentART &cart, ART &art, ConcurrentNode *node, reference<Node> &other, idx_t &pos);

  static void ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst);

  static BlockPointer Serialize(ConcurrentART &art, ConcurrentNode *node, Serializer &serializer);

  static void Deserialize(ConcurrentART &art, ConcurrentNode *node, Deserializer &deserializer);
};
}  // namespace part
#endif  // PART_NODE48_H
