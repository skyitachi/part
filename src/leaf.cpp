//
// Created by Shiping Yao on 2023/12/4.
//
#include "leaf.h"

namespace part {

void Leaf::New(Node &node, const idx_t doc_id) {
  node.Reset();
  node.SetType((uint8_t)NType::LEAF_INLINED);
  node.SetDocID(doc_id);
}

idx_t Leaf::TotalCount(ART &art, Node &node) {

  // NOTE: first leaf in the leaf chain is already deserialized
  assert(node.IsSet() && !node.IsSerialized());

  if (node.GetType() == NType::LEAF_INLINED) {
    return 1;
  }

  idx_t count = 0;
  std::reference_wrapper<Node> node_ref(node);
  while (node_ref.get().IsSet()) {
    auto &leaf = Leaf::Get(art, node_ref);
    count += leaf.count;

    // TODO: serialized
    //    if (leaf.ptr.IsSerialized()) {
    //      leaf.ptr.Deserialize(art);
    //    }
    node_ref = leaf.ptr;
  }
  return count;
}

bool Leaf::GetDocIds(ART &art, Node &node, std::vector<idx_t> &result_ids,
                     idx_t max_count) {
  assert(node.IsSet());

  if (result_ids.size() + Leaf::TotalCount(art, node) > max_count) {
    return false;
  }

  // NOTE: Leaf::TotalCount fully deserializes the leaf
  assert(!node.IsSerialized());

  if (node.GetType() == NType::LEAF_INLINED) {
    // push back the inlined row ID of this leaf
    result_ids.push_back(node.GetDocId());

  } else {
    // push back all the row IDs of this leaf
    std::reference_wrapper<Node> last_leaf_ref(node);
    while (last_leaf_ref.get().IsSet()) {
      auto &leaf = Leaf::Get(art, last_leaf_ref);
      for (idx_t i = 0; i < leaf.count; i++) {
        result_ids.push_back(leaf.row_ids[i]);
      }

      assert(!leaf.ptr.IsSerialized());
      last_leaf_ref = leaf.ptr;
    }
  }
  return true;
}

void Leaf::Insert(ART &art, Node &node, const idx_t row_id) {

  assert(node.IsSet() && !node.IsSerialized());

  if (node.GetType() == NType::LEAF_INLINED) {
    Leaf::MoveInlinedToLeaf(art, node);
    Leaf::Insert(art, node, row_id);
    return;
  }

  std::reference_wrapper<Leaf> leaf = Leaf::Get(art, node);
  while (leaf.get().ptr.IsSet()) {
    if (leaf.get().ptr.IsSerialized()) {
      // TODO: deserialized
    }
    leaf = Leaf::Get(art, leaf.get().ptr);
  }
  leaf.get().Append(art, row_id);
}

void Leaf::MoveInlinedToLeaf(ART &art, Node &node) {

  assert(node.GetType() == NType::LEAF_INLINED);
  auto doc_id = node.GetDocId();
  node = Node::GetAllocator(art, NType::LEAF).New();
  node.SetType((uint8_t)NType::LEAF);

  auto &leaf = Leaf::Get(art, node);
  leaf.count = 1;
  leaf.row_ids[0] = doc_id;
  leaf.ptr.Reset();
}

Leaf &Leaf::Append(ART &art, const idx_t row_id) {
  auto leaf = std::ref(*this);

  if (leaf.get().count == Node::LEAF_SIZE) {
    leaf.get().ptr = Node::GetAllocator(art, NType::LEAF).New();
    leaf.get().ptr.SetType((uint8_t)NType::LEAF);

    leaf = Leaf::Get(art, leaf.get().ptr);
    leaf.get().count = 0;
    leaf.get().ptr.Reset();
  }
  leaf.get().row_ids[leaf.get().count] = row_id;
  leaf.get().count++;
  return leaf.get();
}

void Leaf::Free(ART &art, Node &node) {

  Node current_node = node;
  Node next_node;

  while (current_node.IsSet() && !current_node.IsSerialized()) {
    next_node = Leaf::Get(art, current_node).ptr;
    Node::GetAllocator(art, NType::LEAF).Free(current_node);
    current_node = next_node;
  }

  node.Reset();
}

} // namespace part