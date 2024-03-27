//
// Created by Shiping Yao on 2023/12/6.
//
#include <node16.h>
#include <node4.h>

#include "prefix.h"

namespace part {

Node4 &Node4::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::NODE_4).New();
  node.SetType((uint8_t)NType::NODE_4);
  auto &n4 = Node4::Get(art, node);

  n4.count = 0;
  return n4;
}

std::optional<Node *> Node4::GetChild(const uint8_t byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      assert(children[i].IsSet());
      return &children[i];
    }
  }
  return std::nullopt;
}

void Node4::InsertChild(ART &art, Node &node, uint8_t byte, const Node child) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n4 = Node4::Get(art, node);

  for (idx_t i = 0; i < n4.count; i++) {
    assert(n4.key[i] != byte);
  }

  if (n4.count < Node::NODE_4_CAPACITY) {
    idx_t child_pos = 0;
    while (child_pos < n4.count && n4.key[child_pos] < byte) {
      child_pos++;
    }
    // TODO: upgrade NODE to WLOCK
    for (idx_t i = n4.count; i > child_pos; i--) {
      n4.key[i] = n4.key[i - 1];
      n4.children[i] = n4.children[i - 1];
    }

    n4.key[child_pos] = byte;
    n4.children[child_pos] = child;
    n4.count++;
    // downgrade to RLock
  } else {
    auto node4 = node;
    // 要WLOCK parent  的node， 然后replace 原来的指针
    Node16::GrowNode4(art, node, node4);
    // assert new node WLocked
    Node16::InsertChild(art, node, byte, child);
  }
}

void Node4::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n4 = Node4::Get(art, node);

  for (idx_t i = 0; i < n4.count; i++) {
    Node::Free(art, n4.children[i]);
  }
}

void Node4::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n4 = Node4::Get(art, node);

  idx_t child_pos = 0;
  for (; child_pos < n4.count; child_pos++) {
    if (n4.key[child_pos] == byte) {
      break;
    }
  }
  assert(child_pos < n4.count);
  assert(n4.count > 1);

  Node::Free(art, n4.children[child_pos]);
  n4.count--;

  for (idx_t i = child_pos; i < n4.count; i++) {
    n4.key[i] = n4.key[i + 1];
    n4.children[i] = n4.children[i + 1];
  }

  if (n4.count == 1) {
    // compress to prefix
    auto old_n4_node = node;

    auto child = *n4.GetChild(n4.key[0]).value();
    // indicates prefix node can be overwritten
    Prefix::Concatenate(art, prefix, n4.key[0], child);

    n4.count--;
    Node::Free(art, old_n4_node);
  }
}

BlockPointer Node4::Serialize(ART &art, Node &node, Serializer &writer) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n4 = Node4::Get(art, node);
  std::vector<BlockPointer> child_block_pointers;

  for (idx_t i = 0; i < n4.count; i++) {
    child_block_pointers.emplace_back(n4.children[i].Serialize(art, writer));
  }
  for (idx_t i = n4.count; i < Node::NODE_4_CAPACITY; i++) {
    child_block_pointers.emplace_back((block_id_t)INVALID_BLOCK, 0);
  }
  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::NODE_4);
  writer.Write<uint8_t>(n4.count);

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    writer.Write(n4.key[i]);
  }

  for (auto &child_block_pointer : child_block_pointers) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }

  return block_pointer;
}

void Node4::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto &n4 = Node4::Get(art, node);

  auto block_pointer = reader.GetBlockPointer();
  auto count = reader.Read<uint8_t>();

  n4.count = count;

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    n4.key[i] = reader.Read<uint8_t>();
  }

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    n4.children[i] = Node(art, reader);
  }
}

void Node4::ReplaceChild(const uint8_t byte, const Node child) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      children[i] = child;
      return;
    }
  }
}

Node4 &Node4::ShrinkNode16(ART &art, Node &node4, Node &node16) {
  auto &n4 = Node4::New(art, node4);
  auto &n16 = Node16::Get(art, node16);

  assert(n16.count <= Node::NODE_4_CAPACITY);
  n4.count = n16.count;

  for (idx_t i = 0; i < n16.count; i++) {
    n4.key[i] = n16.key[i];
    n4.children[i] = n16.children[i];
  }

  n16.count = 0;
  Node::Free(art, node16);
  return n4;
}

std::optional<Node *> Node4::GetNextChild(uint8_t &byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] >= byte) {
      byte = key[i];
      assert(children[i].IsSet());
      return &children[i];
    }
  }
  return std::nullopt;
}

CNode4 &CNode4::New(ConcurrentART &art, ConcurrentNode &node) {
  assert(node.Locked());
  node.Update(ConcurrentNode::GetAllocator(art, NType::NODE_4).ConcNew());
  node.SetType((uint8_t)NType::NODE_4);
  auto &n4 = CNode4::Get(art, &node);
  n4.count = 0;
  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    n4.children[i].node = 0;
  }
  return n4;
}

// TODO: 这里也要重新设计
void CNode4::Free(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  assert(node->IsSet() && !node->IsSerialized());

  auto &n4 = CNode4::Get(art, node);
  for (idx_t i = 0; i < n4.count; i++) {
    assert(n4.children[i].ptr);
    n4.children[i].ptr->Lock();
    ConcurrentNode::Free(art, n4.children[i].ptr);
    n4.children[i].ptr->Unlock();
  }
}

void CNode4::ShallowFree(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  ConcurrentNode::GetAllocator(art, NType::NODE_4).Free(*node);
}

void CNode4::InsertChild(ConcurrentART &art, ConcurrentNode *node, uint8_t byte, ConcurrentNode *child) {
  assert(node->Locked());
  assert(node->IsSet() && !node->IsSerialized());
  assert(node->GetType() == NType::NODE_4);

  auto &n4 = CNode4::Get(art, node);
  for (idx_t i = 0; i < n4.count; i++) {
    assert(n4.key[i] != byte);
  }

  if (n4.count < Node::NODE_4_CAPACITY) {
    idx_t child_pos = 0;
    while (child_pos < n4.count && n4.key[child_pos] < byte) {
      child_pos++;
    }
    for (idx_t i = n4.count; i > child_pos; i--) {
      n4.key[i] = n4.key[i - 1];
      n4.children[i] = n4.children[i - 1];
    }

    n4.key[child_pos] = byte;
    n4.children[child_pos].ptr = child;
    n4.count++;
  } else {
    // NOTE: update node pointer, node already points to CNode16
    CNode16::GrowNode4(art, node);
    assert(node->GetType() == NType::NODE_16);
    assert(node->IsSet());
    CNode16::InsertChild(art, node, byte, child);
  }
}

std::optional<ConcurrentNode *> CNode4::GetChild(const uint8_t byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      // NOTE: no need this assertion, only needs check children[i]->IsDeleted()
      // assert(children[i]->IsSet());
      return children[i].ptr;
    }
  }
  return std::nullopt;
}

BlockPointer CNode4::Serialize(ConcurrentART &art, ConcurrentNode *node, Serializer &writer) {
  assert(node->RLocked());
  assert(node->IsSet() && !node->IsSerialized());
  auto &n4 = CNode4::Get(art, node);
  std::vector<BlockPointer> child_block_pointers;

  for (idx_t i = 0; i < n4.count; i++) {
    n4.children[i].ptr->RLock();
    child_block_pointers.emplace_back(n4.children[i].ptr->Serialize(art, writer));
  }
  for (idx_t i = n4.count; i < Node::NODE_4_CAPACITY; i++) {
    child_block_pointers.emplace_back((block_id_t)INVALID_BLOCK, 0);
  }
  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::NODE_4);
  writer.Write<uint8_t>(n4.count);

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    writer.Write(n4.key[i]);
  }

  for (auto &child_block_pointer : child_block_pointers) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }

  node->RUnlock();
  return block_pointer;
}

void CNode4::Deserialize(ConcurrentART &art, ConcurrentNode *node, Deserializer &reader) {
  assert(node->Locked());
  auto &n4 = CNode4::Get(art, node);

  auto count = reader.Read<uint8_t>();

  n4.count = count;

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    n4.key[i] = reader.Read<uint8_t>();
  }

  for (idx_t i = 0; i < Node::NODE_4_CAPACITY; i++) {
    n4.children[i].ptr = art.AllocateNode();
    n4.children[i].ptr->Lock();
    bool valid = n4.children[i].ptr->Deserialize(art, reader);
    if (!valid) {
      n4.children[i].ptr = nullptr;
    }
  }
  node->Unlock();
}

// NOTE: only node is not set
void CNode4::MergeUpdate(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other) {
  P_ASSERT(node->Locked());
  P_ASSERT(!node->IsSet());
  P_ASSERT(other.GetType() == NType::NODE_4);

  node->Update(ConcurrentNode::GetAllocator(cart, NType::NODE_4).ConcNew());
  node->SetType((uint8_t)NType::NODE_4);

  auto &n4 = Node4::Get(art, other);
  auto &cn4 = CNode4::Get(cart, node);
  cn4.count = n4.count;

  for (idx_t i = 0; i < n4.count; i++) {
    cn4.key[i] = n4.key[i];
    cn4.children[i].ptr = cart.AllocateNode();
    // NOTE: important
    cn4.children[i].ptr->ResetAll();
    cn4.children[i].ptr->Lock();
    cn4.children[i].ptr->MergeUpdate(cart, art, n4.children[i]);
    assert(!cn4.children[i].ptr->Locked());
  }
  node->Unlock();
}

bool CNode4::TraversePrefix(ConcurrentART &cart, ART &art, ConcurrentNode *&node, reference<Node> &other, idx_t &pos) {
  auto &prefix = Prefix::Get(art, other);
  assert(node->RLocked());
  assert(pos < prefix.data[Node::PREFIX_SIZE]);

  auto &cn4 = CNode4::Get(cart, node);
  for (idx_t i = 0; i < cn4.count; i++) {
    if (cn4.key[i] == prefix.data[pos]) {
      cn4.children[i].ptr->RLock();
      node->RUnlock();
      node = cn4.children[i].ptr;
      pos += 1;
      if (pos < prefix.data[Node::PREFIX_SIZE]) {
        return ConcurrentNode::TraversePrefix(cart, art, node, other, pos);
      } else {
        // merge prefix.ptr
        if (!prefix.ptr.IsSet()) {
          fmt::println("node ptr is not set, debug");
          ::fflush(stdout);
        }
        node->Merge(cart, art, prefix.ptr);
        return true;
      }
    }
  }
  node->Upgrade();
  ConcurrentNode::InsertForMerge(cart, art, node, prefix, pos);
  node->Unlock();
  return true;
}

void CNode4::ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst) {
  assert(src->GetType() == NType::NODE_4);
  src->RLock();
  auto &cn4 = CNode4::Get(cart, src);
  auto &n4 = Node4::New(art, dst);
  n4.count = cn4.count;
  for (idx_t i = 0; i < cn4.count; i++) {
    n4.key[i] = cn4.key[i];
    ConcurrentNode::ConvertToNode(cart, art, cn4.children[i].ptr, n4.children[i]);
  }
  src->RUnlock();
}

void CNode4::FastDeserialize(ConcurrentART &art, ConcurrentNode *node) {
  P_ASSERT(node->Locked());
  P_ASSERT(node->GetType() == NType::NODE_4);
  auto &cn4 = CNode4::Get(art, node);
  fmt::println("[CNode4] FastDeserialize count {}", cn4.count);
  for (idx_t i = 0; i < cn4.count; i++) {
    P_ASSERT(cn4.children[i].node != 0);
    auto child_node = Node::UnSetSerialized(cn4.children[i].node);
    cn4.children[i].ptr = art.AllocateNode();
    cn4.children[i].ptr->Lock();
    cn4.children[i].ptr->SetData(child_node);
    cn4.children[i].ptr->FastDeserialize(art);
  }
  node->Unlock();
}

}  // namespace part