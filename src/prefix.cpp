//
// Created by Shiping Yao on 2023/12/4.
//

#include "prefix.h"

#include "art_key.h"
#include "node.h"

namespace part {

void Prefix::New(ART &art, std::reference_wrapper<Node> &node, const ARTKey &key, const uint32_t depth,
                 uint32_t count) {
  if (count == 0) {
    return;
  }
  idx_t copy_count = 0;

  while (count > 0) {
    // assert node.get().IsRLocked and node.get().Upgrade to Lock
    node.get() = Node::GetAllocator(art, NType::PREFIX).New();
    node.get().SetType((uint8_t)NType::PREFIX);
    auto &prefix = Prefix::Get(art, node);

    auto this_count = std::min((uint32_t)Node::PREFIX_SIZE, count);
    prefix.data[Node::PREFIX_SIZE] = (uint8_t)this_count;
    std::memcpy(prefix.data, key.data + depth + copy_count, this_count);

    node = prefix.ptr;
    copy_count += this_count;
    count -= this_count;
  }
}

Prefix &Prefix::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::PREFIX).New();
  node.SetType((uint8_t)NType::PREFIX);
  auto &prefix = Prefix::Get(art, node);
  prefix.data[Node::PREFIX_SIZE] = 0;
  return prefix;
}

idx_t Prefix::Traverse(ART &art, std::reference_wrapper<Node> &prefix_node, const ARTKey &key, idx_t &depth) {
  assert(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());
  assert(prefix_node.get().GetType() == NType::PREFIX);

  while (prefix_node.get().GetType() == NType::PREFIX) {
    auto &prefix = Prefix::Get(art, prefix_node);
    for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
      if (prefix.data[i] != key[depth]) {
        return i;
      }
      depth++;
    }
    prefix_node = prefix.ptr;
    P_ASSERT(prefix_node.get().IsSet());
    if (prefix_node.get().IsSerialized()) {
      prefix_node.get().Deserialize(art);
    }
  }

  return INVALID_INDEX;
}

bool Prefix::Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position) {
  auto &l_prefix = Prefix::Get(art, l_node);
  auto &r_prefix = Prefix::Get(art, r_node);

  idx_t max_count = std::min(l_prefix.data[Node::PREFIX_SIZE], r_prefix.data[Node::PREFIX_SIZE]);
  for (idx_t i = 0; i < max_count; i++) {
    if (l_prefix.data[i] != r_prefix.data[i]) {
      mismatch_position = i;
      break;
    }
  }

  if (mismatch_position == INVALID_INDEX) {
    if (l_prefix.data[Node::PREFIX_SIZE] == r_prefix.data[Node::PREFIX_SIZE]) {
      return l_prefix.ptr.ResolvePrefixes(art, r_prefix.ptr);
    }

    mismatch_position = max_count;

    if (r_prefix.ptr.GetType() != NType::PREFIX && r_prefix.data[Node::PREFIX_SIZE] == max_count) {
      std::swap(l_node.get(), r_node.get());
      l_node = r_prefix.ptr;
    } else {
      l_node = l_prefix.ptr;
    }
  }

  return true;
}

void Prefix::Split(ART &art, std::reference_wrapper<Node> &prefix_node, Node &child_node, idx_t position) {
  assert(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());

  auto &prefix = Prefix::Get(art, prefix_node);

  if (position + 1 == Node::PREFIX_SIZE) {
    prefix.data[Node::PREFIX_SIZE]--;
    prefix_node = prefix.ptr;
    child_node = prefix.ptr;
    return;
  }

  if (position + 1 < prefix.data[Node::PREFIX_SIZE]) {
    auto child_prefix = std::ref(Prefix::New(art, child_node));
    for (idx_t i = position + 1; i < prefix.data[Node::PREFIX_SIZE]; i++) {
      child_prefix = child_prefix.get().Append(art, prefix.data[i]);
    }

    assert(prefix.ptr.IsSet());
    if (prefix.ptr.IsSerialized()) {
      prefix.ptr.Deserialize(art);
    }

    if (prefix.ptr.GetType() == NType::PREFIX) {
      child_prefix.get().Append(art, prefix.ptr);
    } else {
      child_prefix.get().ptr = prefix.ptr;
    }
  }

  if (position + 1 == prefix.data[Node::PREFIX_SIZE]) {
    child_node = prefix.ptr;
  }

  prefix.data[Node::PREFIX_SIZE] = position;

  if (position == 0) {
    prefix.ptr.Reset();
    Node::Free(art, prefix_node.get());
    return;
  }

  prefix_node = prefix.ptr;
}

Prefix &Prefix::Append(ART &art, const uint8_t byte) {
  auto prefix = std::ref(*this);

  if (prefix.get().data[Node::PREFIX_SIZE] == Node::PREFIX_SIZE) {
    prefix = Prefix::New(art, prefix.get().ptr);
  }
  prefix.get().data[prefix.get().data[Node::PREFIX_SIZE]] = byte;
  prefix.get().data[Node::PREFIX_SIZE]++;
  return prefix.get();
}

void Prefix::Append(ART &art, Node other_prefix) {
  assert(other_prefix.IsSet() && !other_prefix.IsSerialized());

  auto prefix = std::ref(*this);

  while (other_prefix.GetType() == NType::PREFIX) {
    auto &other = Prefix::Get(art, other_prefix);
    for (idx_t i = 0; i < other.data[Node::PREFIX_SIZE]; i++) {
      prefix = prefix.get().Append(art, other.data[i]);
    }

    assert(other.ptr.IsSet());
    if (other.ptr.IsSerialized()) {
      other.ptr.Deserialize(art);
    }

    prefix.get().ptr = other.ptr;
    Node::GetAllocator(art, NType::PREFIX).Free(other_prefix);

    other_prefix = prefix.get().ptr;
  }

  assert(prefix.get().ptr.GetType() != NType::PREFIX);
}

void Prefix::Free(ART &art, Node &node) {
  Node current_node = node;
  Node next_node;
  while (current_node.IsSet() && current_node.GetType() == NType::PREFIX) {
    next_node = Prefix::Get(art, current_node).ptr;
    Node::GetAllocator(art, NType::PREFIX).Free(current_node);
    current_node = next_node;
  }

  Node::Free(art, current_node);
  node.Reset();
}

idx_t Prefix::TotalCount(ART &art, std::reference_wrapper<Node> &node) {
  assert(node.get().IsSet() && !node.get().IsSerialized());

  idx_t count = 0;
  while (node.get().GetType() == NType::PREFIX) {
    auto &prefix = Prefix::Get(art, node);
    count += prefix.data[Node::PREFIX_SIZE];

    if (prefix.ptr.IsSerialized()) {
      prefix.ptr.Deserialize(art);
    }
    node = prefix.ptr;
  }
  return count;
}

BlockPointer Prefix::Serialize(ART &art, Node &node, Serializer &serializer) {
  auto first_non_prefix = std::ref(node);
  idx_t total_count = Prefix::TotalCount(art, first_non_prefix);

  auto child_block_pointer = first_non_prefix.get().Serialize(art, serializer);

  auto block_pointer = serializer.GetBlockPointer();
  serializer.Write(NType::PREFIX);
  serializer.Write<idx_t>(total_count);

  auto current_node = std::ref(node);
  while (current_node.get().GetType() == NType::PREFIX) {
    auto &prefix = Prefix::Get(art, current_node);
    for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
      serializer.Write(prefix.data[i]);
    }
    current_node = prefix.ptr;
  }
  serializer.Write(child_block_pointer.block_id);
  serializer.Write(child_block_pointer.offset);

  return block_pointer;
}

void Prefix::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto total_count = reader.Read<idx_t>();
  auto current_node = std::ref(node);

  while (total_count) {
    current_node.get() = Node::GetAllocator(art, NType::PREFIX).New();
    current_node.get().SetType((uint8_t)NType::PREFIX);

    auto &prefix = Prefix::Get(art, current_node);
    prefix.data[Node::PREFIX_SIZE] = std::min((idx_t)Node::PREFIX_SIZE, total_count);

    for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
      prefix.data[i] = reader.Read<uint8_t>();
    }

    total_count -= prefix.data[Node::PREFIX_SIZE];

    current_node = prefix.ptr;
    prefix.ptr.Reset();
  }

  current_node.get() = Node(art, reader);
}

void Prefix::Concatenate(ART &art, Node &prefix_node, const uint8_t byte, Node &child_prefix_node) {
  assert(prefix_node.IsSet() && !prefix_node.IsSerialized());
  assert(child_prefix_node.IsSet());

  if (child_prefix_node.IsSerialized()) {
    child_prefix_node.Deserialize(art);
  }

  if (prefix_node.GetType() == NType::PREFIX) {
    auto prefix = std::ref(Prefix::Get(art, prefix_node));

    while (prefix.get().ptr.GetType() == NType::PREFIX) {
      prefix = std::ref(Prefix::Get(art, prefix.get().ptr));
      assert(prefix.get().ptr.IsSet() && !prefix.get().ptr.IsSerialized());
    }

    prefix = prefix.get().Append(art, byte);

    if (child_prefix_node.GetType() == NType::PREFIX) {
      prefix.get().Append(art, child_prefix_node);
    } else {
      prefix.get().ptr = child_prefix_node;
    }
    return;
  }

  // indicates that prefix_node can be reallocated
  if (prefix_node.GetType() != NType::PREFIX && child_prefix_node.GetType() == NType::PREFIX) {
    auto child_prefix = child_prefix_node;
    auto &prefix = Prefix::New(art, prefix_node, byte, Node());
    prefix.Append(art, child_prefix);
    return;
  }

  Prefix::New(art, prefix_node, byte, child_prefix_node);
}

Prefix &Prefix::New(ART &art, Node &node, uint8_t byte, Node next) {
  node = Node::GetAllocator(art, NType::PREFIX).New();
  node.SetType((uint8_t)NType::PREFIX);

  auto &prefix = Prefix::Get(art, node);
  prefix.data[Node::PREFIX_SIZE] = 1;
  prefix.data[0] = byte;
  prefix.ptr = next;
  return prefix;
}

void Prefix::Reduce(ART &art, Node &prefix_node, const idx_t n) {
  assert(prefix_node.IsSet() && n < Node::PREFIX_SIZE);

  auto &prefix = Prefix::Get(art, prefix_node);

  // remove prefix node necessary
  if (n == (idx_t)(prefix.data[Node::PREFIX_SIZE] - 1)) {
    auto next_ptr = prefix.ptr;
    assert(next_ptr.IsSet());
    prefix.ptr.Reset();
    Node::Free(art, prefix_node);
    prefix_node = next_ptr;
    return;
  }

  // why not prefix.data[Node::PREFIX_SIZE]
  for (idx_t i = 0; i < Node::PREFIX_SIZE - n - 1; i++) {
    prefix.data[i] = prefix.data[n + i + 1];
  }
  assert(n < (idx_t)(prefix.data[Node::PREFIX_SIZE] - 1));
  prefix.data[Node::PREFIX_SIZE] -= n + 1;

  // if prefix.ptr is not prefix, nothing happens
  prefix.Append(art, prefix.ptr);
}

// NOTE: keep checks IsDeleted
idx_t CPrefix::Traverse(ConcurrentART &cart, reference<ConcurrentNode> &prefix_node, const ARTKey &key, idx_t &depth,
                        bool &retry) {
  assert(prefix_node.get().RLocked() && !prefix_node.get().IsDeleted());
  assert(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());
  assert(prefix_node.get().GetType() == NType::PREFIX);

  // assert prefix_node is RLocked
  while (prefix_node.get().GetType() == NType::PREFIX) {
    assert(prefix_node.get().RLocked() && !prefix_node.get().IsDeleted());

    auto &cprefix = CPrefix::Get(cart, prefix_node);
    for (idx_t i = 0; i < cprefix.data[Node::PREFIX_SIZE]; i++) {
      if (cprefix.data[i] != key[depth]) {
        // NOTE: keep this node RLocked
        return i;
      }
      depth++;
    }

    // NOTE: RUnlock asap
    prefix_node.get().RUnlock();
    // important
    assert(cprefix.ptr);
    cprefix.ptr->RLock();
    if (cprefix.ptr->IsDeleted()) {
      // NOTE: this node deleted, need a retry
      retry = true;
      return INVALID_INDEX;
    }
    prefix_node = std::ref(*cprefix.ptr);
    P_ASSERT(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());
    // NOTE: node ptr is changed
  }
  return INVALID_INDEX;
}

// NOTE: 只要保证write 和 read 不冲突就行
void CPrefix::New(ConcurrentART &art, reference<ConcurrentNode> &node, const ARTKey &key, const uint32_t depth,
                  uint32_t count) {
  if (count == 0) {
    return;
  }
  assert(node.get().Locked());

  idx_t copy_count = 0;

  while (count > 0) {
    // only update data pointer
    node.get().Update(ConcurrentNode::GetAllocator(art, NType::PREFIX).ConcNew());
    node.get().SetType((uint8_t)NType::PREFIX);
    auto &cprefix = CPrefix::Get(art, node);

    auto this_count = std::min((uint32_t)Node::PREFIX_SIZE, count);
    cprefix.data[Node::PREFIX_SIZE] = (uint8_t)this_count;
    std::memcpy(cprefix.data, key.data + depth + copy_count, this_count);

    cprefix.ptr = art.AllocateNode();

    node.get().Unlock();
    // NOTE: is this necessary, important
    cprefix.ptr->ResetAll();
    assert(!cprefix.ptr->IsSet());
    node = std::ref(*cprefix.ptr);
    // NOTE: keep node.Locked invariants
    node.get().Lock();
    copy_count += this_count;
    count -= this_count;
  }
}

void CPrefix::Free(ConcurrentART &art, ConcurrentNode *node) {
  ConcurrentNode *current_node = node;
  ConcurrentNode *next_node;
  while (current_node->IsSet() && current_node->GetType() == NType::PREFIX) {
    assert(current_node->Locked());
    next_node = CPrefix::Get(art, *current_node).ptr;
    next_node->Lock();
    ConcurrentNode::GetAllocator(art, NType::PREFIX).Free(*current_node);
    current_node->Reset();
    current_node->SetDeleted();
    current_node->Unlock();

    current_node = next_node;
  }
  // NOTE: maybe no need use ResetAll
  assert(current_node->Locked());
  ConcurrentNode::Free(art, current_node);
  current_node->Reset();
  current_node->SetDeleted();
  current_node->Unlock();
}

// NOTE: child_node is new node, no need add lock to these node
bool CPrefix::Split(ConcurrentART &art, reference<ConcurrentNode> &prefix_node, ConcurrentNode *&child_node,
                    idx_t position) {
  assert(prefix_node.get().RLocked());
  assert(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());

  auto &cprefix = CPrefix::Get(art, prefix_node);
  if (position + 1 == Node::PREFIX_SIZE) {
    prefix_node.get().Upgrade();
    cprefix.data[Node::PREFIX_SIZE]--;
    prefix_node.get().Unlock();
    assert(cprefix.ptr);
    cprefix.ptr->RLock();
    prefix_node = *cprefix.ptr;
    child_node = cprefix.ptr;
    return false;
  }

  if (position + 1 < cprefix.data[Node::PREFIX_SIZE]) {
    // NOTE: child_prefix is new node
    child_node = art.AllocateNode();
    auto child_prefix = std::ref(CPrefix::NewPrefixNew(art, child_node));
    for (idx_t i = position + 1; i < cprefix.data[Node::PREFIX_SIZE]; i++) {
      // child prefix is new node
      child_prefix = child_prefix.get().NewPrefixAppend(art, cprefix.data[i]);
    }

    cprefix.ptr->RLock();
    if (cprefix.ptr->IsDeleted()) {
      cprefix.ptr->RUnlock();
      return true;
    }
    assert(cprefix.ptr && cprefix.ptr->IsSet());
    if (cprefix.ptr->IsSerialized()) {
      // TODO: deseralize
    }

    //
    if (cprefix.ptr->GetType() == NType::PREFIX) {
      bool retry = false;
      child_prefix.get().NewPrefixAppend(art, cprefix.ptr, retry);
      if (retry) {
        cprefix.ptr->RLock();
        return true;
      }
    } else {
      child_prefix.get().ptr = cprefix.ptr;
      cprefix.ptr->RUnlock();
    }
  }

  assert(prefix_node.get().RLocked());
  if (position + 1 == cprefix.data[Node::PREFIX_SIZE]) {
    child_node = cprefix.ptr;
  }

  prefix_node.get().Upgrade();
  cprefix.data[Node::PREFIX_SIZE] = position;

  if (position == 0) {
    // NOTE: all locks released
    // NOTE: it's important, it will protect cprefix.ptr been freed
    // cprefix.ptr->Reset();
    // keeps lock
    CPrefix::FreeSelf(art, &prefix_node.get());
    return false;
  }

  if (child_node == cprefix.ptr) {
    // allocate new pointer
    cprefix.ptr = art.AllocateNode();
  }
  prefix_node.get().Unlock();

  cprefix.ptr->RLock();
  prefix_node = *cprefix.ptr;
  return false;
}

CPrefix &CPrefix::New(ConcurrentART &art, ConcurrentNode &node) {
  node = ConcurrentNode::GetAllocator(art, NType::PREFIX).ConcNew();
  node.SetType((uint8_t)NType::PREFIX);
  auto &prefix = CPrefix::Get(art, node);
  prefix.data[Node::PREFIX_SIZE] = 0;
  // NOTE: important
  prefix.ptr = art.AllocateNode();
  return prefix;
}

CPrefix &CPrefix::NewPrefixAppend(ConcurrentART &art, const uint8_t byte) {
  auto prefix = std::ref(*this);

  if (prefix.get().data[Node::PREFIX_SIZE] == Node::PREFIX_SIZE) {
    assert(prefix.get().ptr);
    prefix = CPrefix::NewPrefixNew(art, prefix.get().ptr);
  }
  prefix.get().data[prefix.get().data[Node::PREFIX_SIZE]] = byte;
  prefix.get().data[Node::PREFIX_SIZE]++;
  return prefix.get();
}

CPrefix &CPrefix::NewPrefixNew(ConcurrentART &art, ConcurrentNode *node) {
  node->Update(ConcurrentNode::GetAllocator(art, NType::PREFIX).ConcNew());
  node->SetType((uint8_t)NType::PREFIX);
  auto &cprefix = CPrefix::Get(art, *node);
  cprefix.data[Node::PREFIX_SIZE] = 0;
  // NOTE: important init
  cprefix.ptr = art.AllocateNode();
  return cprefix;
}

CPrefix &CPrefix::NewPrefixNew(ConcurrentART &art, ConcurrentNode *node,
                               const ARTKey &key,
                               const uint32_t depth,
                               uint32_t count) {

  node->Update(ConcurrentNode::GetAllocator(art, NType::PREFIX).ConcNew());
  node->SetType((uint8_t)NType::PREFIX);
  auto &cprefix = CPrefix::Get(art, *node);
  cprefix.data[Node::PREFIX_SIZE] = 0;
  // NOTE: important init
  cprefix.ptr = art.AllocateNode();
  for (uint32_t i = 0; i < count; i++) {
    // NOTE: important to Append prefix data
    cprefix = cprefix.NewPrefixAppend(art, key.data[depth + i]);
  }

  return cprefix;
}

// NOTE: current prefix node is new node, so no need to add lock
// Append work as path compression
void CPrefix::NewPrefixAppend(ConcurrentART &art, ConcurrentNode *other_prefix, bool &retry) {
  assert(other_prefix->RLocked());
  assert(other_prefix->IsSet() && !other_prefix->IsSerialized());

  auto current_prefix = std::ref(*this);
  while (other_prefix->GetType() == NType::PREFIX) {
    auto &other = CPrefix::Get(art, *other_prefix);
    for (idx_t i = 0; i < other.data[Node::PREFIX_SIZE]; i++) {
      current_prefix = current_prefix.get().NewPrefixAppend(art, other.data[i]);
    }
    other.ptr->RLock();
    if (other.ptr->IsDeleted()) {
      retry = true;
      other.ptr->RUnlock();
      other_prefix->RUnlock();
      return;
    }
    assert(other.ptr->IsSet());
    if (other.ptr->IsSerialized()) {
      // TODO: Deserialize
    }
    current_prefix.get().ptr = other.ptr;
    other_prefix->Upgrade();
    ConcurrentNode::GetAllocator(art, NType::PREFIX).ConcFree(other_prefix);
    other_prefix->SetDeleted();
    other_prefix->Unlock();

    other_prefix = current_prefix.get().ptr;
  }

  other_prefix->RUnlock();
  assert(current_prefix.get().ptr->GetType() != NType::PREFIX);
}

void CPrefix::FreeSelf(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  ConcurrentNode::GetAllocator(art, NType::PREFIX).Free(*node);
  node->Reset();
  node->SetDeleted();
}

}  // namespace part
