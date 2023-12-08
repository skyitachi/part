//
// Created by Shiping Yao on 2023/12/4.
//

#include "prefix.h"
#include "art_key.h"

namespace part {

void Prefix::New(ART &art, std::reference_wrapper<Node> &node,
                 const ARTKey &key, const uint32_t depth, uint32_t count) {
  if (count == 0) {
    return;
  }
  idx_t copy_count = 0;

  while (count > 0) {
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

idx_t Prefix::Traverse(ART &art, std::reference_wrapper<Node> &prefix_node,
                       const ARTKey &key, idx_t &depth) {
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
    assert(prefix_node.get().IsSet());
    // TODO: serialized
  }

  return INVALID_INDEX;
}

void Prefix::Split(ART &art, std::reference_wrapper<Node> &prefix_node,
                   Node &child_node, idx_t position) {
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
    // TODO: serialize

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
    // TODO: serialized

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
}

} // namespace part
