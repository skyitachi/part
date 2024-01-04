//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_PREFIX_H
#define PART_PREFIX_H
#include <cassert>

#include "art.h"
#include "fixed_size_allocator.h"
#include "node.h"

namespace part {
class ARTKey;

class Prefix {
 public:
  uint8_t data[Node::PREFIX_SIZE + 1];

  Node ptr;

  static void Free(ART &art, Node &node);

  static inline Prefix &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::PREFIX).Get<Prefix>(ptr);
  }

  static inline uint8_t GetByte(const ART &art, const Node &prefix_node, const idx_t position) {
    auto prefix = Prefix::Get(art, prefix_node);
    assert(position < Node::PREFIX_SIZE);
    assert(position < prefix.data[Node::PREFIX_SIZE]);
    return prefix.data[position];
  }

  static idx_t Traverse(ART &art, std::reference_wrapper<Node> &prefix_node, const ARTKey &key, idx_t &depth);

  static void Split(ART &art, std::reference_wrapper<Node> &prefix_node, Node &child_node, idx_t position);

  static void Concatenate(ART &art, Node &prefix_node, const uint8_t byte, Node &child_prefix_node);

  Prefix &Append(ART &art, const uint8_t byte);

  void Append(ART &art, Node other_prefix);

  // serialization
  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);

  static Prefix &New(ART &art, Node &node);

  static void New(ART &art, std::reference_wrapper<Node> &node, const ARTKey &key, const uint32_t depth,
                  uint32_t count);

  static Prefix &New(ART &art, Node &node, uint8_t byte, Node next);

  static idx_t TotalCount(ART &art, std::reference_wrapper<Node> &node);
};
}  // namespace part
#endif  // PART_PREFIX_H
