//
// Created by Shiping Yao on 2023/12/4.
//

#include <iostream>
#include "art.h"
#include "art_key.h"
#include "node.h"
#include "prefix.h"
#include "leaf.h"
#include "fixed_size_allocator.h"
#include "node4.h"
#include "node16.h"
#include <node48.h>
#include <node256.h>

namespace part {

ART::ART(const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr)
    : allocators(allocators_ptr), owns_data(false) {
  if (!allocators) {
    owns_data = true;
    allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
    allocators->emplace_back(sizeof(Prefix), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(Leaf), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(Node4), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(Node16), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(Node48), Allocator::DefaultAllocator());
    allocators->emplace_back(sizeof(Node256), Allocator::DefaultAllocator());
  }

  root = std::make_unique<Node>();
}

ART::~ART() { root->Reset(); }

void ART::Put(const ARTKey &key, idx_t doc_id) {
  insert(*root, key, 0, doc_id);
}

bool ART::Get(const ARTKey &key, std::vector<idx_t>& result_ids) {
  auto leaf = lookup(*root, key, 0);
  if (!leaf) {
    return false;
  }

  return Leaf::GetDocIds(*this, *leaf.value(), result_ids, 1);
}

std::optional<Node*> ART::lookup(Node node, const ARTKey &key, idx_t depth) {
  auto next_node = std::ref(node);
  while(next_node.get().IsSet()) {
    if (next_node.get().GetType() == NType::PREFIX) {
      Prefix::Traverse(*this, next_node, key, depth);
      if (next_node.get().GetType() == NType::PREFIX) {
        return std::nullopt;
      }
    }

    if (next_node.get().GetType() == NType::LEAF || next_node.get().GetType() == NType::LEAF_INLINED) {
      return &next_node.get();
    }

    assert(depth < key.len);
    auto child = next_node.get().GetChild(*this, key[depth]);
    if (!child) {
      return std::nullopt;
    }
    next_node = *child.value();
    depth++;
  }
  return std::nullopt;
}

void ART::insert(Node &node, const ARTKey &key, idx_t depth, const idx_t &doc_id) {
  if (!node.IsSet()) {
    assert(depth <= key.len);
    std::reference_wrapper<Node> ref_node(node);
    Prefix::New(*this, ref_node, key, depth, key.len - depth);
    Leaf::New(ref_node, doc_id);
    return;
  }

  auto node_type = node.GetType();

  if (node_type == NType::LEAF || node_type == NType::LEAF_INLINED) {
    InsertToLeaf(node, doc_id);
    return;
  }

  if (node_type != NType::PREFIX) {
    assert(depth < key.len);

    auto child = node.GetChild(*this, key[depth]);
    if (child) {
      insert(*child.value(), key, depth + 1, doc_id);
      // need to replace child for node
      return;
    }

    Node leaf_node;

    auto ref_node = std::ref(leaf_node);
    if (depth + 1 < key.len) {
      Prefix::New(*this, ref_node, key, depth + 1, key.len - depth - 1);
    }

    Leaf::New(ref_node, doc_id);
    Node::InsertChild(*this, node, key[depth], leaf_node);
    return;
  }

  // insert to prefix

  auto next_node = std::ref(node);
  auto mismatch_position = Prefix::Traverse(*this, next_node, key, depth);

  if (next_node.get().GetType() != NType::PREFIX) {
    return insert(next_node, key, depth, doc_id);
  }

  Node remaining_prefix;
  auto prefix_byte = Prefix::GetByte(*this, next_node, mismatch_position);
  // next_node changes reference, that means root node will change
  Prefix::Split(*this, next_node, remaining_prefix, mismatch_position);
  // root node change to Node4
  Node4::New(*this, next_node);

  Node4::InsertChild(*this, next_node, prefix_byte, remaining_prefix);

  // insert new leaf
  Node leaf_node;
  auto ref_node = std::ref(leaf_node);

  if (depth + 1 < key.len) {
    Prefix::New(*this, ref_node, key, depth + 1, key.len - depth - 1);
  }

  Leaf::New(ref_node, doc_id);
  Node4::InsertChild(*this, next_node, key[depth], leaf_node);
}

bool ART::InsertToLeaf(Node &leaf, const idx_t row_id) {
  // NOTE: no constraints check

  Leaf::Insert(*this, leaf, row_id);
  return true;
}

idx_t ART::GetMemoryUsage() {
  if (owns_data) {
    idx_t total = 0;
    for(auto &allocator: *allocators) {
      total += allocator.GetMemoryUsage();
    }
    return total;
  }
  return 0;
}
} // namespace part
