//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_PREFIX_H
#define PART_PREFIX_H
#include <cassert>

#include "art.h"
#include "node.h"
#include "fixed_size_allocator.h"

namespace part {
class ARTKey;

class Prefix {
public:
  uint8_t data[Node::PREFIX_SIZE + 1];

  Node ptr;

  static inline Prefix &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::PREFIX).Get<Prefix>(ptr);
  }

  static idx_t Traverse(ART& art, std::reference_wrapper<Node>& prefix_node, const ARTKey& key, idx_t &depth);

public:
  static Prefix &New(ART &art, Node &node);
  static void New(ART &art, std::reference_wrapper<Node> &node,
                  const ARTKey &key, const uint32_t depth, uint32_t count);
};
} // namespace part
#endif // PART_PREFIX_H
