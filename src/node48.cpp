//
// Created by skyitachi on 23-12-7.
//
#include <node16.h>
#include <node256.h>
#include <node48.h>

#include "prefix.h"

namespace part {

Node48 &Node48::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::NODE_48).New();
  node.SetType((uint8_t)NType::NODE_48);
  auto &n48 = Node48::Get(art, node);

  n48.count = 0;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = Node::EMPTY_MARKER;
  }

  for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i].Reset();
  }

  return n48;
}

void Node48::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n48 = Node48::Get(art, node);

  if (!n48.count) {
    return;
  }

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    if (n48.child_index[i] != Node::EMPTY_MARKER) {
      Node::Free(art, n48.children[n48.child_index[i]]);
    }
  }
}

Node48 &Node48::GrowNode16(ART &art, Node &node48, Node &node16) {
  auto &n16 = Node16::Get(art, node16);
  auto &n48 = Node48::New(art, node48);

  n48.count = n16.count;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = Node::EMPTY_MARKER;
  }

  for (idx_t i = 0; i < n16.count; i++) {
    n48.child_index[n16.key[i]] = i;
    n48.children[i] = n16.children[i];
  }

  for (idx_t i = n16.count; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i].Reset();
  }

  n16.count = 0;
  Node::Free(art, node16);

  return n48;
}

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n48 = Node48::Get(art, node);

  assert(n48.child_index[byte] == Node::EMPTY_MARKER);

  if (n48.count < Node::NODE_48_CAPACITY) {
    idx_t child_pos = n48.count;
    if (n48.children[child_pos].IsSet()) {
      child_pos = 0;
      while (n48.children[child_pos].IsSet()) {
        child_pos++;
      }
    }
    n48.children[child_pos] = child;
    n48.child_index[byte] = child_pos;
    n48.count++;
  } else {
    auto node48 = node;
    Node256::GrowNode48(art, node, node48);
    Node256::InsertChild(art, node, byte, child);
  }
}

std::optional<Node *> Node48::GetChild(const uint8_t byte) {
  if (child_index[byte] != Node::EMPTY_MARKER) {
    assert(children[child_index[byte]].IsSet());
    return &children[child_index[byte]];
  }
  return std::nullopt;
}

BlockPointer Node48::Serialize(ART &art, Node &node, Serializer &writer) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n48 = Node48::Get(art, node);

  std::vector<BlockPointer> child_pointer_blocks;
  for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
    child_pointer_blocks.emplace_back(n48.children[i].Serialize(art, writer));
  }
  auto block_pointer = writer.GetBlockPointer();

  writer.Write(NType::NODE_48);
  writer.Write<uint8_t>(n48.count);

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    writer.Write(n48.child_index[i]);
  }

  for (auto &child_block_pointer : child_pointer_blocks) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }

  return block_pointer;
}

void Node48::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto &n48 = Node48::Get(art, node);
  n48.count = reader.Read<uint8_t>();

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = reader.Read<uint8_t>();
  }

  for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i] = Node(art, reader);
  }
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n48 = Node48::Get(art, node);

  Node::Free(art, n48.children[n48.child_index[byte]]);
  n48.child_index[byte] = Node::EMPTY_MARKER;
  n48.count--;

  if (n48.count < Node::NODE_48_SHRINK_THRESHOLD) {
    auto node48 = node;
    Node16::ShrinkNode48(art, node, node48);
  }
}

void Node48::ReplaceChild(const uint8_t byte, const Node child) {
  assert(child_index[byte] != Node::EMPTY_MARKER);
  children[child_index[byte]] = child;
}

Node48 &Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {
  auto &n48 = Node48::New(art, node48);
  auto &n256 = Node256::Get(art, node256);

  n48.count = 0;

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    assert(n48.count < Node::NODE_48_CAPACITY);
    if (n256.children[i].IsSet()) {
      n48.child_index[i] = n48.count;
      n48.children[n48.count] = n256.children[i];
      n48.count++;
    } else {
      n48.child_index[i] = Node::EMPTY_MARKER;
    }
  }

  for (idx_t i = n48.count; i < Node::NODE_48_CAPACITY; i++) {
    // important for search
    n48.children[i].Reset();
  }

  n256.count = 0;
  Node::Free(art, node256);
  return n48;
}

std::optional<Node *> Node48::GetNextChild(uint8_t &byte) {
  for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
    if (child_index[i] != Node::EMPTY_MARKER) {
      byte = i;
      assert(children[child_index[i]].IsSet());
      return &children[child_index[i]];
    }
  }
  return std::nullopt;
}

CNode48 &CNode48::New(ConcurrentART &art, ConcurrentNode &node) {
  assert(node.Locked());
  node.Update(ConcurrentNode::GetAllocator(art, NType::NODE_48).ConcNew());
  node.SetType((uint8_t)NType::NODE_48);

  auto &n48 = CNode48::Get(art, &node);
  n48.count = 0;
  return n48;
}

void CNode48::Free(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  assert(node->IsSet() && !node->IsSerialized());

  auto &n48 = CNode48::Get(art, node);
  for (idx_t i = 0; i < n48.count; i++) {
    assert(n48.children[i]);
    n48.children[i]->Lock();
    ConcurrentNode::Free(art, n48.children[i]);
    n48.children[i]->Unlock();
  }
}

void CNode48::ShallowFree(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  ConcurrentNode::GetAllocator(art, NType::NODE_48).Free(*node);
}

CNode48 &CNode48::GrowNode16(ConcurrentART &art, ConcurrentNode *node16) {
  assert(node16->Locked());
  auto &n16 = CNode16::Get(art, node16);
  // NOTE: it will delete automatically, no need delete manually
  auto node48 = art.AllocateNode();
  // NOTE: unnecessary lock
  node48->Lock();
  auto &n48 = CNode48::New(art, *node48);
  node48->Unlock();
  n48.count = n16.count;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    n48.child_index[i] = Node::EMPTY_MARKER;
  }
  for (idx_t i = 0; i < n16.count; i++) {
    n48.child_index[n16.key[i]] = i;
    n48.children[i] = n16.children[i];
  }

  // NOTE: should initialize as empty pointer
  for (idx_t i = n16.count; i < Node::NODE_48_CAPACITY; i++) {
    n48.children[i] = nullptr;
  }

  n16.count = 0;
  n16.ShallowFree(art, node16);
  // reset node4 points to n16
  node16->Update(node48);
  node16->SetType((uint8_t)NType::NODE_48);
  assert(node16->Locked());
  auto &new48 = CNode48::Get(art, node16);
  return new48;
}

void CNode48::InsertChild(ConcurrentART &art, ConcurrentNode *node, const uint8_t byte, ConcurrentNode *child) {
  assert(node->Locked());
  assert(node->IsSet() && !node->IsSerialized());

  auto &n48 = CNode48::Get(art, node);

  assert(n48.child_index[byte] == Node::EMPTY_MARKER);

  if (n48.count < Node::NODE_48_CAPACITY) {
    idx_t child_pos = n48.count;
    // NOTE: judge pointer whether nullptr
    if (n48.children[child_pos]) {
      child_pos = 0;
      while (n48.children[child_pos]) {
        child_pos++;
      }
    }
    n48.children[child_pos] = child;
    n48.child_index[byte] = child_pos;
    n48.count++;
  } else {
    CNode256::GrowNode48(art, node);
    CNode256::InsertChild(art, node, byte, child);
  }
}

std::optional<ConcurrentNode *> CNode48::GetChild(const uint8_t byte) {
  if (child_index[byte] != Node::EMPTY_MARKER) {
    // assert(children[child_index[byte]]->IsSet());
    return children[child_index[byte]];
  }
  return std::nullopt;
}

void CNode48::MergeUpdate(ConcurrentART &cart, ART &art, ConcurrentNode *node, Node &other) {
  assert(node->Locked());
  assert(!node->IsSet());
  assert(other.GetType() == NType::NODE_48);

  node->Update(ConcurrentNode::GetAllocator(cart, NType::NODE_48).ConcNew());
  node->SetType((uint8_t)NType::NODE_48);

  auto &n48 = Node48::Get(art, other);
  auto &cn48 = CNode48::Get(cart, node);

  cn48.count = n48.count;
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    cn48.child_index[i] = n48.child_index[i];
    if (cn48.child_index[i] != Node::EMPTY_MARKER) {
      auto new_child = cart.AllocateNode();
      new_child->Lock();
      new_child->MergeUpdate(cart, art, n48.children[n48.child_index[i]]);
      assert(!new_child->Locked());
      cn48.children[cn48.child_index[i]] = new_child;
    }
  }
  node->Unlock();
}

bool CNode48::TraversePrefix(ConcurrentART &cart, ART &art, ConcurrentNode *&node, reference<Node> &other, idx_t &pos) {
  auto &prefix = Prefix::Get(art, other);
  assert(node->RLocked());
  assert(pos < prefix.data[Node::PREFIX_SIZE]);

  auto &cn48 = CNode48::Get(cart, node);
  if (cn48.child_index[prefix.data[pos]] != Node::EMPTY_MARKER) {
    node->RUnlock();
    node = cn48.children[cn48.child_index[prefix.data[pos]]];
    pos += 1;
    if (pos < prefix.data[Node::PREFIX_SIZE]) {
      return ConcurrentNode::TraversePrefix(cart, art, node, other, pos);
    } else {
      node->Merge(cart, art, prefix.ptr);
      return true;
    }
  }
  node->Upgrade();
  ConcurrentNode::InsertForMerge(cart, art, node, prefix, pos);
  node->Unlock();
  return false;
}

void CNode48::ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst) {
  assert(src->GetType() == NType::NODE_48);
  src->RLock();

  auto &cn48 = CNode48::Get(cart, src);
  dst = Node::GetAllocator(art, NType::NODE_48).New();
  auto &n48 = Node48::Get(art, dst);
  n48.count = cn48.count;
  std::memcpy(n48.child_index, cn48.child_index, sizeof(cn48.child_index));
  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    if (cn48.child_index[i] != Node::EMPTY_MARKER) {
      ConvertToNode(cart, art, cn48.children[cn48.child_index[i]], n48.children[n48.child_index[i]]);
    }
  }
  src->RUnlock();
}

}  // namespace part