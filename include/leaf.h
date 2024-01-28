//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_LEAF_H
#define PART_LEAF_H
#include <vector>

#include "fixed_size_allocator.h"
#include "node.h"
#include "types.h"
#include "concurrent_node.h"
#include "concurrent_art.h"

namespace part {

class Leaf {
 public:
  static void New(Node &node, const idx_t value);
  static void Free(ART &art, Node &node);

  static idx_t TotalCount(ART &art, Node &node);
  static bool GetDocIds(ART &art, Node &node, std::vector<idx_t> &result_ids, idx_t max_count);

  static inline Leaf &Get(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return *Node::GetAllocator(art, NType::LEAF).Get<Leaf>(ptr);
  }

  static void Insert(ART &art, Node &node, const idx_t row_id);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);

  static bool Remove(ART &art, std::reference_wrapper<Node> &node, const idx_t row_id);

  static void Merge(ART &art, Node &l_node, Node &r_node);

 public:
  //! The number of row IDs in this leaf
  uint8_t count;
  //! Up to LEAF_SIZE row IDs
  // NOTE: no need to store row_ids in the leaf node for unique indexing
  idx_t row_ids[Node::LEAF_SIZE];
  //! A pointer to the next LEAF node
  Node ptr;

 private:
  static void MoveInlinedToLeaf(ART &art, Node &node);
  Leaf &Append(ART &art, const idx_t row_id);
};

class CLeaf {
 public:
  uint8_t count;
  idx_t row_ids[Node::LEAF_SIZE];
  ConcurrentNode ptr;

  static CLeaf &Get(ConcurrentART &art, const ConcurrentNode &ptr);
  static bool GetDocIds(ConcurrentART &art,
                        ConcurrentNode &node,
                        std::vector<idx_t> &result_ids,
                        idx_t max_count,
                        bool &retry);

};
}  // namespace part

#endif  // PART_LEAF_H
