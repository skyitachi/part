//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE16_H
#define PART_NODE16_H

#include "node.h"
#include "fixed_size_allocator.h"

namespace part {

class Node16 {
public:
  Node16(const Node16&) = delete;
  Node16 &operator=(const Node16&) = delete;

  uint8_t count;

  uint8_t key[Node::NODE_16_CAPACITY];

  Node children[Node::NODE_16_CAPACITY];

  static inline Node16 &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::NODE_16).Get<Node16>(ptr);
  }

  static Node16& New(ART &art, Node &node);

  static Node16 &GrowNode4(ART &art, Node &node16, Node &node4);

  static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);



};

}

#endif // PART_NODE16_H
