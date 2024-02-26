//
// Created by skyitachi on 23-12-7.
//
#include "node16.h"

#include "concurrent_art.h"
#include "node4.h"
#include "node48.h"

namespace part {

Node16 &Node16::New(ART &art, Node &node) {
  node = Node::GetAllocator(art, NType::NODE_16).New();
  node.SetType((uint8_t)NType::NODE_16);

  auto &n16 = Node16::Get(art, node);
  n16.count = 0;
  return n16;
}

Node16 &part::Node16::GrowNode4(ART &art, Node &node16, Node &node4) {
  auto &n4 = Node4::Get(art, node4);
  auto &n16 = Node16::New(art, node16);

  n16.count = n4.count;
  for (idx_t i = 0; i < n4.count; i++) {
    n16.key[i] = n4.key[i];
    n16.children[i] = n4.children[i];
  }

  n4.count = 0;
  Node::Free(art, node4);
  return n16;
}

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n16 = Node16::Get(art, node);

  for (idx_t i = 0; i < n16.count; i++) {
    assert(n16.key[i] != byte);
  }

  if (n16.count < Node::NODE_16_CAPACITY) {
    idx_t child_pos = 0;

    while (child_pos < n16.count && n16.key[child_pos] < byte) {
      child_pos++;
    }

    for (idx_t i = n16.count; i > child_pos; i--) {
      n16.key[i] = n16.key[i - 1];
      n16.children[i] = n16.children[i - 1];
    }

    n16.key[child_pos] = byte;
    n16.children[child_pos] = child;
    n16.count++;
  } else {
    auto node16 = node;
    Node48::GrowNode16(art, node, node16);
    Node48::InsertChild(art, node, byte, child);
  }
}

std::optional<Node *> Node16::GetChild(const uint8_t byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      assert(children[i].IsSet());
      return &children[i];
    }
  }

  return std::nullopt;
}

void Node16::Free(ART &art, Node &node) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n16 = Node16::Get(art, node);

  for (idx_t i = 0; i < n16.count; i++) {
    Node::Free(art, n16.children[i]);
  }
}

BlockPointer Node16::Serialize(ART &art, Node &node, Serializer &writer) {
  assert(node.IsSet() && !node.IsSerialized());
  auto &n16 = Node16::Get(art, node);

  std::vector<BlockPointer> child_block_pointers;
  for (idx_t i = 0; i < n16.count; i++) {
    child_block_pointers.emplace_back(n16.children[i].Serialize(art, writer));
  }

  for (idx_t i = n16.count; i < Node::NODE_16_CAPACITY; i++) {
    child_block_pointers.emplace_back((block_id_t)INVALID_BLOCK, 0);
  }

  auto block_pointer = writer.GetBlockPointer();
  writer.Write(NType::NODE_16);
  writer.Write<uint8_t>(n16.count);

  for (idx_t i = 0; i < Node::NODE_16_CAPACITY; i++) {
    writer.Write(n16.key[i]);
  }

  for (auto &child_block_pointer : child_block_pointers) {
    writer.Write(child_block_pointer.block_id);
    writer.Write(child_block_pointer.offset);
  }

  return block_pointer;
}

void Node16::Deserialize(ART &art, Node &node, Deserializer &reader) {
  auto ref_node = std::ref(node);
  auto &n16 = Node16::Get(art, ref_node);

  auto count = reader.Read<uint8_t>();
  n16.count = count;

  for (idx_t i = 0; i < Node::NODE_16_CAPACITY; i++) {
    n16.key[i] = reader.Read<uint8_t>();
  }

  for (idx_t i = 0; i < Node::NODE_16_CAPACITY; i++) {
    n16.children[i] = Node(art, reader);
  }
}

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {
  assert(node.IsSet() && !node.IsSerialized());

  auto &n16 = Node16::Get(art, node);

  if (n16.count == 0) {
    return;
  }

  idx_t child_pos = 0;
  for (; child_pos < n16.count; child_pos++) {
    if (n16.key[child_pos] == byte) {
      break;
    }
  }

  assert(child_pos < n16.count);

  Node::Free(art, n16.children[child_pos]);
  n16.count--;
  for (idx_t i = child_pos; i < n16.count; i++) {
    n16.key[i] = n16.key[i + 1];
    n16.children[i] = n16.children[i + 1];
  }

  if (n16.count < Node::NODE_4_CAPACITY) {
    auto node16 = node;
    Node4::ShrinkNode16(art, node, node16);
  }
}

void Node16::ReplaceChild(const uint8_t byte, const Node child) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      children[i] = child;
      return;
    }
  }
}

Node16 &Node16::ShrinkNode48(ART &art, Node &node16, Node &node48) {
  // TODO: order matters
  auto &n48 = Node48::Get(art, node48);
  auto &n16 = Node16::New(art, node16);

  n16.count = 0;

  for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
    assert(n16.count <= Node::NODE_16_CAPACITY);
    if (n48.child_index[i] != Node::EMPTY_MARKER) {
      n16.key[n16.count] = i;
      n16.children[n16.count] = n48.children[n48.child_index[i]];
      n16.count++;
    }
  }

  n48.count = 0;
  Node::Free(art, node48);
  return n16;
}

std::optional<Node *> Node16::GetNextChild(uint8_t &byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] >= byte) {
      byte = key[i];
      assert(children[i].IsSet());
      return &children[i];
    }
  }
  return std::nullopt;
}

CNode16 &CNode16::New(ConcurrentART &art, ConcurrentNode &node) {
  assert(node.Locked());
  node.Update(ConcurrentNode::GetAllocator(art, NType::NODE_16).ConcNew());
  node.SetType((uint8_t)NType::NODE_16);

  auto &n16 = CNode16::Get(art, &node);
  n16.count = 0;
  return n16;
}

void CNode16::Free(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  assert(node->IsSet() && !node->IsSerialized());

  auto &n16 = CNode16::Get(art, node);
  for (idx_t i = 0; i < n16.count; i++) {
    assert(n16.children[i]);
    n16.children[i]->Lock();
    ConcurrentNode::Free(art, n16.children[i]);
    n16.children[i]->Unlock();
  }
}

void CNode16::ShallowFree(ConcurrentART &art, ConcurrentNode *node) {
  assert(node->Locked());
  ConcurrentNode::GetAllocator(art, NType::NODE_16).Free(*node);
}

// NOTE: node16 should be allocated
CNode16 &CNode16::GrowNode4(ConcurrentART &art, ConcurrentNode *node4) {
  assert(node4->Locked());
  auto &n4 = CNode4::Get(art, node4);
  // NOTE: it will delete automatically, no need delete manually
  auto node16 = art.AllocateNode();
  // NOTE: unnecessary lock
  node16->Lock();
  auto &n16 = CNode16::New(art, *node16);
  node16->Unlock();
  n16.count = n4.count;
  for (idx_t i = 0; i < n4.count; i++) {
    n16.key[i] = n4.key[i];
    n16.children[i] = n4.children[i];
  }

  for (idx_t i = n4.count; i < Node::NODE_16_CAPACITY; i++) {
    n16.children[i] = nullptr;
  }

  n4.count = 0;
  n4.ShallowFree(art, node4);
  //  ConcurrentNode::Free(art, node4);
  // reset node4 points to n16
  node4->Update(node16);
  node4->SetType((uint8_t)NType::NODE_16);
  assert(node4->Locked());
  auto &new16 = CNode16::Get(art, node4);
  return new16;
}

void CNode16::InsertChild(ConcurrentART &art, ConcurrentNode *node, const uint8_t byte, ConcurrentNode *child) {
  assert(node->Locked());
  assert(node->IsSet() && !node->IsSerialized());

  auto &n16 = CNode16::Get(art, node);
  for (idx_t i = 0; i < n16.count; i++) {
    assert(n16.key[i] != byte);
  }
  if (n16.count < Node::NODE_16_CAPACITY) {
    idx_t child_pos = 0;

    while (child_pos < n16.count && n16.key[child_pos] < byte) {
      child_pos++;
    }

    for (idx_t i = n16.count; i > child_pos; i--) {
      n16.key[i] = n16.key[i - 1];
      n16.children[i] = n16.children[i - 1];
    }

    n16.key[child_pos] = byte;
    n16.children[child_pos] = child;
    n16.count++;
  } else {
    // TODO: Grow to 48
    CNode48::GrowNode16(art, node);
    CNode48::InsertChild(art, node, byte, child);
  }
}

std::optional<ConcurrentNode *> CNode16::GetChild(const uint8_t byte) {
  for (idx_t i = 0; i < count; i++) {
    if (key[i] == byte) {
      //      assert(children[i]->IsSet());
      return children[i];
    }
  }

  return std::nullopt;
}

}  // namespace part
