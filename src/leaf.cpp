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
  auto node_ref = std::ref(node);
  while (node_ref.get().IsSet()) {
    auto &leaf = Leaf::Get(art, node_ref);
    count += leaf.count;

    if (leaf.ptr.IsSerialized()) {
      leaf.ptr.Deserialize(art);
    }
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
        leaf.get().ptr.Deserialize(art);
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

BlockPointer Leaf::Serialize(ART &art, Node &node, Serializer &writer) {
  if (node.GetType() == NType::LEAF_INLINED) {
    auto block_pointer = writer.GetBlockPointer();
    writer.Write(NType::LEAF_INLINED);
    writer.Write(node.GetDocId());
    return block_pointer;
  }

  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::LEAF);
  idx_t total_count = Leaf::TotalCount(art, node);
  writer.Write<idx_t>(total_count);

  // iterate all leaves and write their row IDs
  auto ref_node = std::ref(node);
  while (ref_node.get().IsSet()) {
    assert(!ref_node.get().IsSerialized());
    auto &leaf = Leaf::Get(art, ref_node);

    // write row IDs
    for (idx_t i = 0; i < leaf.count; i++) {
      writer.Write(leaf.row_ids[i]);
    }
    ref_node = leaf.ptr;
  }
  return block_pointer;
}

void Leaf::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto total_count = reader.Read<idx_t>();
  auto ref_node = std::ref(node);

  while (total_count > 0) {
    ref_node.get() = Node::GetAllocator(art, NType::LEAF).New();
    ref_node.get().SetType((uint8_t)NType::LEAF);

    auto &leaf = Leaf::Get(art, ref_node);
    leaf.count = std::min((idx_t)Node::LEAF_SIZE, total_count);

    for (idx_t i = 0; i < leaf.count; i++) {
      leaf.row_ids[i] = reader.Read<idx_t>();
    }

    total_count -= leaf.count;
    ref_node = leaf.ptr;
    leaf.ptr.Reset();
  }
}

} // namespace part