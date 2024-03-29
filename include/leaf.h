//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_LEAF_H
#define PART_LEAF_H
#include <vector>

#include "concurrent_art.h"
#include "concurrent_node.h"
#include "fixed_size_allocator.h"
#include "node.h"
#include "types.h"

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

  static inline Leaf *GetPtr(const ART &art, const Node ptr) {
    assert(!ptr.IsSerialized());
    return Node::GetAllocator(art, NType::LEAF).Get<Leaf>(ptr);
  }

  static void Insert(ART &art, Node &node, idx_t row_id);

  static BlockPointer Serialize(ART &art, Node &node, Serializer &serializer);

  static void Deserialize(ART &art, Node &node, Deserializer &deserializer);

  static bool Remove(ART &art, std::reference_wrapper<Node> &node, idx_t row_id);

  static void Merge(ART &art, Node &l_node, Node &r_node);

 public:
  //! The number of row IDs in this leaf
  uint8_t count;
  //! Up to LEAF_SIZE row IDs
  // NOTE: no need to store row_ids in the leaf node for unique indexing
  idx_t row_ids[Node::LEAF_SIZE];
  //! A pointer to the next LEAF node
  Node ptr;

  // private:
  static void MoveInlinedToLeaf(ART &art, Node &node);
  Leaf &Append(ART &art, idx_t row_id);
};

class CLeaf {
 public:
  uint8_t count;
  idx_t row_ids[Node::LEAF_SIZE];
  ConcurrentNode *ptr;

  static CLeaf &Get(ConcurrentART &art, const ConcurrentNode &ptr);
  static CLeaf *GetPtr(ConcurrentART &art, const ConcurrentNode &ptr);

  static data_ptr_t GetPointer(ConcurrentART &art, ConcurrentNode *ptr);
  static bool GetDocIds(ConcurrentART &art, ConcurrentNode &node, std::vector<idx_t> &result_ids, idx_t max_count,
                        bool &retry);

  static void New(ConcurrentNode &node, idx_t doc_id);

  static void Free(ConcurrentART &art, ConcurrentNode *node);

  static void Insert(ConcurrentART &art, ConcurrentNode *&node, const idx_t row_id, bool &retry);

  static void MoveInlinedToLeaf(ConcurrentART &art, ConcurrentNode &node);

  CLeaf &Append(ConcurrentART &art, ConcurrentNode *&node, idx_t doc_id);

  static void Append(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other);

  static idx_t TotalCount(ConcurrentART &art, ConcurrentNode *node);

  static BlockPointer Serialize(ConcurrentART &art, ConcurrentNode *node, Serializer &serializer);

  static void Deserialize(ConcurrentART &art, ConcurrentNode *node, Deserializer &deserializer);

  static void MergeUpdate(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other);

  static void ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst);

  static void Merge(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &other);
};
}  // namespace part

#endif  // PART_LEAF_H
