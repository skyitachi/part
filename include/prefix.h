//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_PREFIX_H
#define PART_PREFIX_H
#include <cassert>
#include <memory>

#include "art.h"
#include "concurrent_art.h"
#include "concurrent_node.h"
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

  static bool Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position);

  static void Split(ART &art, std::reference_wrapper<Node> &prefix_node, Node &child_node, idx_t position);

  static void Reduce(ART &art, Node &prefix_node, const idx_t n);

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

class CPrefix {
 public:
  uint8_t data[Node::PREFIX_SIZE + 1];

  //  ConcurrentNode ptr or maybe called next more clearly
  ConcurrentNode *ptr;

  static void Free(ConcurrentART &art, ConcurrentNode &node);

  static void New(ConcurrentART &art, reference<ConcurrentNode> &node, const ARTKey &key, const uint32_t depth,
                  uint32_t count);

  // NOTE: no locks
  static CPrefix &NewPrefixNew(ConcurrentART &art, ConcurrentNode *node);

  static CPrefix &New(ConcurrentART &art, ConcurrentNode &node);

  static inline CPrefix &Get(const ConcurrentART &art, const ConcurrentNode &ptr) {
    P_ASSERT(!ptr.IsSerialized());
    P_ASSERT(ptr.Locked() || ptr.RLocked());
    return *ConcurrentNode::GetAllocator(art, NType::PREFIX).Get<CPrefix>(ptr);
  }

  static inline uint8_t GetByte(const ConcurrentART &art, const ConcurrentNode &prefix_node, const idx_t position) {
    auto prefix = CPrefix::Get(art, prefix_node);
    assert(position < Node::PREFIX_SIZE);
    assert(position < prefix.data[Node::PREFIX_SIZE]);
    return prefix.data[position];
  }

  static idx_t Traverse(ConcurrentART &cart, reference<ConcurrentNode> &prefix_node, const ARTKey &key, idx_t &depth,
                        bool &retry);

  static bool Split(ConcurrentART &art, reference<ConcurrentNode> &prefix_node, ConcurrentNode *&child_node,
                    idx_t position);

  // NOTE: new prefix append data no need to sync with lock
  CPrefix &NewPrefixAppend(ConcurrentART &art, const uint8_t byte);
};
}  // namespace part
#endif  // PART_PREFIX_H
