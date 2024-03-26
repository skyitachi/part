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

bool Leaf::GetDocIds(ART &art, Node &node, std::vector<idx_t> &result_ids, idx_t max_count) {
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

  // assert
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

// TODO: bug?
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
    //    fmt::println("[Leaf.INLINE] block_id: {}, offset: {}",
    //    block_pointer.block_id, block_pointer.offset);
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

// TODO: a lot of improvements todo
bool Leaf::Remove(ART &art, std::reference_wrapper<Node> &node, const idx_t row_id) {
  assert(node.get().IsSet() && !node.get().IsSerialized());

  if (node.get().GetType() == NType::LEAF_INLINED) {
    return node.get().GetDocId() == row_id;
  }

  auto leaf = std::ref(Leaf::Get(art, node));
  auto prev_leaf = std::ref(leaf);

  while (leaf.get().ptr.IsSet()) {
    prev_leaf = leaf;
    if (leaf.get().ptr.IsSerialized()) {
      leaf.get().ptr.Deserialize(art);
    }
    leaf = Leaf::Get(art, leaf.get().ptr);
  }

  auto last_idx = leaf.get().count;
  auto last_row_id = leaf.get().row_ids[last_idx - 1];

  if (leaf.get().count == 1) {
    Node::Free(art, prev_leaf.get().ptr);
    if (last_row_id == row_id) {
      return false;
    }
  } else {
    leaf.get().count--;
  }
  while (node.get().IsSet()) {
    assert(!node.get().IsSerialized());
    for (idx_t i = 0; i < leaf.get().count; i++) {
      if (leaf.get().row_ids[i] == row_id) {
        leaf.get().row_ids[i] = last_row_id;
        return false;
      }
    }
    node = leaf.get().ptr;
  }
  return false;
}

// NOTE: 这个效率实在是太低了啊
void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {
  assert(l_node.IsSet() && r_node.IsSet());

  if (r_node.GetType() == NType::LEAF_INLINED) {
    Insert(art, l_node, r_node.GetDocId());
    r_node.Reset();
    return;
  }

  if (l_node.GetType() == NType::LEAF_INLINED) {
    auto doc_id = l_node.GetDocId();
    l_node = r_node;
    Insert(art, l_node, doc_id);
    r_node.Reset();
  }

  assert(l_node.GetType() != NType::LEAF_INLINED);
  assert(r_node.GetType() != NType::LEAF_INLINED);

  auto l_node_ref = std::ref(l_node);
  auto &l_leaf = Leaf::Get(art, l_node_ref);

  while (l_leaf.count == Node::LEAF_SIZE) {
    l_node_ref = l_leaf.ptr;

    if (!l_leaf.ptr.IsSet()) {
      break;
    }

    l_leaf = Leaf::Get(art, l_node_ref);
  }

  auto last_leaf_node = l_node_ref.get();
  l_node_ref.get() = r_node;
  r_node.Reset();

  if (last_leaf_node.IsSet()) {
    l_leaf = Leaf::Get(art, l_node_ref);
    while (l_leaf.ptr.IsSet()) {
      l_leaf = Leaf::Get(art, l_leaf.ptr);
    }
    auto &last_leaf = Leaf::Get(art, last_leaf_node);
    for (idx_t i = 0; i < last_leaf.count; i++) {
      l_leaf = l_leaf.Append(art, last_leaf.row_ids[i]);
    }
    Node::GetAllocator(art, NType::LEAF).Free(last_leaf_node);
  }
}

// node is not updated, so need unlock in this method internally
bool CLeaf::GetDocIds(ConcurrentART &art, ConcurrentNode &node, std::vector<idx_t> &result_ids, idx_t max_count,
                      bool &retry) {
  assert(node.RLocked());
  assert(node.IsSet());

  // NOTE: Leaf::TotalCount fully deserializes the leaf
  assert(!node.IsSerialized());

  if (node.GetType() == NType::LEAF_INLINED) {
    result_ids.push_back(node.GetDocId());
    // important
    node.RUnlock();
    return true;
  }
  auto last_leaf_ref = std::ref(node);
  while (last_leaf_ref.get().IsSet()) {
    if (last_leaf_ref.get().IsDeleted()) {
      retry = true;
      last_leaf_ref.get().RUnlock();
      return false;
    }
    auto &leaf = CLeaf::Get(art, last_leaf_ref);
    for (idx_t i = 0; i < leaf.count; i++) {
      result_ids.push_back(leaf.row_ids[i]);
      if (result_ids.size() >= max_count) {
        last_leaf_ref.get().RUnlock();
        return true;
      }
    }

    // NOTE: release lock asap
    last_leaf_ref.get().RUnlock();
    assert(leaf.ptr);
    leaf.ptr->RLock();
    assert(!leaf.ptr->IsSerialized());
    last_leaf_ref = *leaf.ptr;
  }
  last_leaf_ref.get().RUnlock();
  return true;
}

// NOTE: CLeaf class Layout cannot be changed
CLeaf &CLeaf::Get(ConcurrentART &art, const ConcurrentNode &ptr) {
  assert(ptr.RLocked() || ptr.Locked());
  assert(!ptr.IsSerialized());
  return *ConcurrentNode::GetAllocator(art, NType::LEAF).Get<CLeaf>(ptr);
}

CLeaf *CLeaf::GetPtr(ConcurrentART &art, const ConcurrentNode &ptr) {
  assert(ptr.RLocked() || ptr.Locked());
  assert(!ptr.IsSerialized());
  return ConcurrentNode::GetAllocator(art, NType::LEAF).Get<CLeaf>(ptr);
}

data_ptr_t CLeaf::GetPointer(ConcurrentART &art, ConcurrentNode *ptr) {
  auto &allocator = ConcurrentNode::GetAllocator(art, NType::LEAF);
  return allocator.GetPointer(ptr);
}

// NOTE: default is inline
void CLeaf::New(ConcurrentNode &node, idx_t doc_id) {
  assert(node.Locked());
  node.Reset();
  node.SetType((uint8_t)NType::LEAF_INLINED);
  node.SetDocID(doc_id);
  //  node.ptr = std::make_unique<ConcurrentNode>();
}

// NOTE: acquire no locks after Insert
void CLeaf::Insert(ConcurrentART &art, ConcurrentNode *&node, const idx_t row_id, bool &retry) {
  assert((node->Locked()) && !node->IsDeleted());
  assert(node->IsSet() && !node->IsSerialized());
  if (node->GetType() == NType::LEAF_INLINED) {
    CLeaf::MoveInlinedToLeaf(art, *node);
    CLeaf::Insert(art, node, row_id, retry);
    return;
  }

  // node points to ref
  auto ref = std::ref(CLeaf::Get(art, *node));
  if (ref.get().next == 0) {
    ref.get().Append(art, node, row_id);
    return;
  }

  auto next_node = ref.get().ptr;
  next_node->RLock();

  bool need_upgrade = false;
  bool holds_write_lock = true;
  while (next_node->IsSet()) {
    assert(next_node->RLocked());
    if (next_node->IsDeleted()) {
      node->RUnlock();
      next_node->RUnlock();
      retry = true;
      return;
    }
    if (next_node->IsSerialized()) {
      // TODO: Deserialize no need serialize here (new approach)
    }
    if (holds_write_lock) {
      node->Unlock();
      holds_write_lock = false;
    } else {
      node->RUnlock();
    }
    // NOTE: need keep ref.get().ptr and node all lock
    // NOTE: insert params need to check
    node = next_node;
    ref = std::ref(CLeaf::Get(art, *node));
    if (ref.get().next == 0) {
      ref.get().ptr = art.AllocateNode();
      ref.get().ptr->ResetAll();
    }
    next_node = ref.get().ptr;
    next_node->RLock();
    need_upgrade = true;
  }
  assert(next_node->RLocked());
  next_node->RUnlock();

  if (need_upgrade) {
    node->Upgrade();
  }
  assert(node->Locked());
  ref.get().Append(art, node, row_id);
}

void CLeaf::MoveInlinedToLeaf(ConcurrentART &art, ConcurrentNode &node) {
  assert(node.GetType() == NType::LEAF_INLINED && node.Locked() && !node.IsDeleted());
  auto doc_id = node.GetDocId();
  node.Update(ConcurrentNode::GetAllocator(art, NType::LEAF).ConcNew());
  node.SetType((uint8_t)NType::LEAF);

  auto &cleaf = CLeaf::Get(art, node);
  cleaf.count = 1;
  cleaf.row_ids[0] = doc_id;
  // NOTE: important, initialize the ptr here
  // TODO: should marked as null pointer
  cleaf.next = 0;
//  cleaf.ptr = art.AllocateNode();
//  cleaf.ptr->ResetAll();
}

CLeaf &CLeaf::Append(ConcurrentART &art, ConcurrentNode *&node, idx_t doc_id) {
  // NOTE: node points to leaf
  auto leaf = std::ref(*this);
  assert(node->Locked());
//  assert(leaf.get().ptr);

  if (leaf.get().count == Node::LEAF_SIZE) {
    if (leaf.get().next == 0) {
      // NOTE: allocate here (lazy)
      leaf.get().ptr = art.AllocateNode();
      leaf.get().ptr->ResetAll();
    }
    leaf.get().ptr->Update(ConcurrentNode::GetAllocator(art, NType::LEAF).ConcNew());
    leaf.get().ptr->SetType((uint8_t)NType::LEAF);
    leaf.get().ptr->Lock();
    node->Unlock();
    // ConcurrentNode is non-trivial, when use this get method will lose lock state
    // it's a design problem
    node = leaf.get().ptr;
    leaf = CLeaf::Get(art, *node);
    // NOTE: important initialize
    leaf.get().next = 0;
    leaf.get().count = 0;
  }

  assert(node->Locked());
  leaf.get().row_ids[leaf.get().count] = doc_id;
  leaf.get().count++;

  node->Unlock();
  return leaf.get();
}

void CLeaf::Free(ConcurrentART &art, ConcurrentNode *node) {
  ConcurrentNode *current_node = node;
  ConcurrentNode *next_node;

  while (current_node->IsSet() && !current_node->IsSerialized()) {
    assert(current_node->Locked());
    next_node = CLeaf::Get(art, *current_node).ptr;
    next_node->Lock();
    // add deleted flag and reset all lock states
    ConcurrentNode::GetAllocator(art, NType::LEAF).Free(*current_node);
    // ResetAll and SetDeleted order matters
    current_node->Reset();
    current_node->SetDeleted();
    current_node->Unlock();

    current_node = next_node;
  }
}

BlockPointer CLeaf::Serialize(ConcurrentART &art, ConcurrentNode *node, Serializer &writer) {
  assert(node->RLocked());
  if (node->GetType() == NType::LEAF_INLINED) {
    auto block_pointer = writer.GetBlockPointer();
    writer.Write(NType::LEAF_INLINED);
    writer.Write(node->GetDocId());
    //    fmt::println("[Leaf.INLINE] block_id: {}, offset: {}",
    //    block_pointer.block_id, block_pointer.offset);
    node->RUnlock();
    return block_pointer;
  }

  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::LEAF);
  idx_t total_count = CLeaf::TotalCount(art, node);
  writer.Write<idx_t>(total_count);

  // iterate all leaves and write their row IDs
  auto &ref_node = node;
  while (ref_node->IsSet()) {
    assert(!ref_node->IsSerialized());
    auto &leaf = CLeaf::Get(art, *ref_node);

    // write row IDs
    for (idx_t i = 0; i < leaf.count; i++) {
      writer.Write(leaf.row_ids[i]);
    }
    leaf.ptr->RLock();
    ref_node->RUnlock();
    ref_node = leaf.ptr;
  }
  ref_node->RUnlock();
  return block_pointer;
}

idx_t CLeaf::TotalCount(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->IsSet() && !node->IsSerialized());

  if (node->GetType() == NType::LEAF_INLINED) {
    return 1;
  }

  idx_t count = 0;
  auto &node_ref = node;
  while (node_ref->IsSet()) {
    node_ref->RLock();
    auto &leaf = CLeaf::Get(art, *node_ref);
    node_ref->RUnlock();
    count += leaf.count;
    if (leaf.ptr->IsSerialized()) {
      leaf.ptr->Deserialize(art);
    }
    node_ref = leaf.ptr;
  }
  return count;
}

void CLeaf::FastDeserialize(ConcurrentART &art, ConcurrentNode *node) {
  P_ASSERT(node->Locked());

  if (node->GetType() == NType::LEAF_INLINED) {
    // set serialized flag to false
    node->SetData(Node::UnSetSerialized(node->GetData()));
    node->Unlock();
    fmt::println("leaf inline deserialized");
    return;
  }

  auto &cleaf = CLeaf::Get(art, *node);

  fmt::println("CLeaf count: {}", cleaf.count);

  auto next = cleaf.next;
  if (Node::IsSerialized(cleaf.next)) {
    cleaf.ptr = art.AllocateNode();
    cleaf.ptr->Lock();
    node->Unlock();
    cleaf.ptr->SetData(Node::UnSetSerialized(next));
    cleaf.ptr->FastDeserialize(art);
    return;
  }
  node->Unlock();
}

void CLeaf::Deserialize(ConcurrentART &art, ConcurrentNode *node, Deserializer &reader) {
  assert(node->Locked());
  auto total_count = reader.Read<idx_t>();
  auto &ref_node = node;

  while (total_count > 0) {
    if (!ref_node) {
      ref_node = art.AllocateNode();
      ref_node->Lock();
    }
    assert(ref_node->Locked());
    ref_node->Update(ConcurrentNode::GetAllocator(art, NType::LEAF).ConcNew());
    ref_node->SetType((uint8_t)NType::LEAF);

    auto &leaf = CLeaf::Get(art, *ref_node);
    leaf.count = std::min((idx_t)Node::LEAF_SIZE, total_count);

    for (idx_t i = 0; i < leaf.count; i++) {
      leaf.row_ids[i] = reader.Read<idx_t>();
    }

    total_count -= leaf.count;
    leaf.ptr->Lock();
    ref_node->Unlock();
    ref_node = leaf.ptr;
    if (ref_node) {
      ref_node->Reset();
    }
  }

  assert(ref_node->Locked());
  ref_node->Unlock();
}

void CLeaf::MergeUpdate(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other) {
  assert(node->Locked());
  assert(other.GetType() == NType::LEAF || other.GetType() == NType::LEAF_INLINED);

  node->Reset();
  node->SetType((uint8_t)other.GetType());
  if (other.GetType() == NType::LEAF_INLINED) {
    node->SetDocID(other.GetDocId());
    node->Unlock();
    return;
  }

  auto current_node = node;
  auto ref_node = std::ref(other);

  while (ref_node.get().IsSet()) {
    assert(current_node->Locked());
    current_node->Update(ConcurrentNode::GetAllocator(cart, NType::LEAF).ConcNew());
    current_node->SetType((uint8_t)NType::LEAF);
    auto &cleaf = CLeaf::Get(cart, *current_node);
    auto &leaf = Leaf::Get(art, ref_node.get());
    cleaf.count = leaf.count;
    for (idx_t i = 0; i < leaf.count; i++) {
      cleaf.row_ids[i] = leaf.row_ids[i];
    }
    cleaf.ptr = cart.AllocateNode();
    cleaf.ptr->ResetAll();
    ref_node = leaf.ptr;

    cleaf.ptr->Lock();
    current_node->Unlock();
    current_node = cleaf.ptr;
  }
  // NOTE: important
  current_node->Unlock();
}

void CLeaf::ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst) {
  assert(src->GetType() == NType::LEAF || src->GetType() == NType::LEAF_INLINED);
  src->RLock();
  if (src->GetType() == NType::LEAF_INLINED) {
    Leaf::New(dst, src->GetDocId());
    src->RUnlock();
    return;
  }
  ConcurrentNode *current_node = src;
  auto ref_node = std::ref(dst);
  while (current_node->IsSet()) {
    auto &cleaf = CLeaf::Get(cart, *current_node);
    ref_node.get() = Node::GetAllocator(art, NType::LEAF).New();
    ref_node.get().SetType((uint8_t)NType::LEAF);

    auto &leaf = Leaf::Get(art, ref_node.get());
    leaf.count = cleaf.count;
    for (idx_t i = 0; i < cleaf.count; i++) {
      leaf.row_ids[i] = cleaf.row_ids[i];
    }
    cleaf.ptr->RLock();
    current_node->RUnlock();
    current_node = cleaf.ptr;
    ref_node = leaf.ptr;
  }
  current_node->RUnlock();
}

void CLeaf::Merge(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &other) {
  assert(src->Locked());

  if (src->GetType() == NType::LEAF_INLINED) {
    MoveInlinedToLeaf(cart, *src);
  }
  auto &cleaf = CLeaf::Get(cart, *src);
  if (other.GetType() == NType::LEAF_INLINED) {
    cleaf.Append(cart, src, other.GetDocId());
    return;
  }
  auto ref_node = std::ref(other);

  CLeaf::Append(cart, art, src, other);
  assert(!src->Locked());
}

void CLeaf::Append(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other) {
  assert(node->Locked());

  auto ref_node = std::ref(other);
  auto cleaf = CLeaf::GetPtr(cart, *node);

  while (ref_node.get().IsSet()) {
    auto &leaf = Leaf::Get(art, ref_node.get());
    for (idx_t i = 0; i < leaf.count; i++) {
      while (node->IsSet()) {
        cleaf = CLeaf::GetPtr(cart, *node);
        if (cleaf->count < Node::LEAF_SIZE) {
          break;
        }
        cleaf->ptr->Lock();
        node->Unlock();
        node = cleaf->ptr;
        if (!node->IsSet()) {
          node->Update(ConcurrentNode::GetAllocator(cart, NType::LEAF).ConcNew());
          node->SetType((uint8_t)NType::LEAF);
          cleaf = CLeaf::GetPtr(cart, *node);
          cleaf->count = 0;
          cleaf->ptr = cart.AllocateNode();
          break;
        }
      }
      cleaf->row_ids[cleaf->count] = leaf.row_ids[i];
      cleaf->count++;
    }
    ref_node = leaf.ptr;
  }
  node->Unlock();
}

}  // namespace part