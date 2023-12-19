//
// Created by Shiping Yao on 2023/12/4.
//

#include "art.h"

#include <node256.h>
#include <node48.h>

#include <iostream>

#include "art_key.h"
#include "fixed_size_allocator.h"
#include "leaf.h"
#include "node.h"
#include "node16.h"
#include "node4.h"
#include "prefix.h"

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

ART::ART(const std::string &metadata_path, const std::string &index_path,
         const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr)
    : ART(allocators_ptr) {
  index_path_ = index_path;
  meta_path_ = metadata_path;

  index_fd_ = ::open(index_path.c_str(), O_CREAT | O_RDWR, 0644);
  if (index_fd_ == -1) {
    throw std::invalid_argument(fmt::format("cann open {} index file, error: {}", index_path, strerror(errno)));
  }

  metadata_fd_ = ::open(metadata_path.c_str(), O_RDWR, 0644);
  if (metadata_fd_ == -1) {
    root = std::make_unique<Node>();
  } else {
    auto pointer = ReadMetadata();
    root = std::make_unique<Node>(pointer.block_id, pointer.offset);
    root->SetSerialized();
    root->Deserialize(*this);
  }
}

ART::~ART() { root->Reset(); }

void ART::Put(const ARTKey &key, idx_t doc_id) { insert(*root, key, 0, doc_id); }

bool ART::Get(const ARTKey &key, std::vector<idx_t> &result_ids) {
  auto leaf = lookup(*root, key, 0);
  if (!leaf) {
    return false;
  }

  return Leaf::GetDocIds(*this, *leaf.value(), result_ids, std::numeric_limits<int64_t>::max());
}

std::optional<Node *> ART::lookup(Node node, const ARTKey &key, idx_t depth) {
  auto next_node = std::ref(node);
  while (next_node.get().IsSet()) {
    // TODO: invalid GetType call
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
    for (auto &allocator : *allocators) {
      total += allocator.GetMemoryUsage();
    }
    return total;
  }
  return 0;
}

BlockPointer ART::Serialize(Serializer &writer) {
  if (root->IsSet()) {
    auto block_pointer = root->Serialize(*this, writer);
    writer.Flush();
    return block_pointer;
  }
  return BlockPointer();
}

void ART::Serialize() {
  SequentialSerializer data_writer(index_path_);
  BlockPointer pointer = BlockPointer();
  if (root->IsSet()) {
    pointer = root->Serialize(*this, data_writer);
    data_writer.Flush();
    SequentialSerializer meta_writer(meta_path_);
    UpdateMetadata(pointer, meta_writer);
    meta_writer.Flush();
  }
}

void ART::UpdateMetadata(BlockPointer pointer, Serializer &writer) {
  writer.Write<block_id_t>(pointer.block_id);
  writer.Write<uint32_t>(pointer.offset);
}

BlockPointer ART::ReadMetadata() {
  if (metadata_fd_ == -1) {
    throw std::invalid_argument(fmt::format("no meta file"));
  }
  BlockPointer pointer;
  ::read(metadata_fd_, &pointer.block_id, sizeof(block_id_t));
  ::read(metadata_fd_, &pointer.offset, sizeof(uint32_t));
  return pointer;
}

void ART::Deserialize() { root->Deserialize(*this); }

static idx_t sumNoneLeafCount(ART &art, Node &node, bool count_leaf = false) {
  if (!node.IsSet()) {
    return 0;
  }
  if (node.IsSerialized()) {
    node.Deserialize(art);
  }
  auto type = node.GetType();

  idx_t current = count_leaf ? 0 : 1;

  switch (type) {
    case NType::PREFIX: {
      auto &prefix = Prefix::Get(art, node);
      auto sum = current + sumNoneLeafCount(art, prefix.ptr, count_leaf);
      return sum;
    }
    case NType::LEAF: {
      if (!count_leaf) {
        return 0;
      }
      auto &leaf = Leaf::Get(art, node);
      idx_t sum = 1;
      while (leaf.ptr.IsSet()) {
        leaf = Leaf::Get(art, leaf.ptr);
        sum += 1;
      }
      return sum;
    }
    case NType::LEAF_INLINED:
      if (count_leaf) {
        return 1;
      }
      return 0;
    case NType::NODE_4: {
      auto &n4 = Node4::Get(art, node);
      idx_t sum = current;
      for (idx_t i = 0; i < n4.count; i++) {
        sum += sumNoneLeafCount(art, n4.children[i], count_leaf);
      }
      return sum;
    }
    case NType::NODE_16: {
      auto &n16 = Node16::Get(art, node);
      idx_t sum = current;
      for (idx_t i = 0; i < n16.count; i++) {
        sum += sumNoneLeafCount(art, n16.children[i], count_leaf);
      }
      return sum;
    }
    case NType::NODE_48: {
      auto &n48 = Node48::Get(art, node);
      idx_t sum = current;
      for (idx_t i = 0; i < n48.count; i++) {
        sum += sumNoneLeafCount(art, n48.children[i], count_leaf);
      }
      return sum;
    }
    case NType::NODE_256: {
      auto &n256 = Node256::Get(art, node);
      idx_t sum = current;
      for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
        if (n256.children[i].IsSet()) {
          sum += sumNoneLeafCount(art, n256.children[i], count_leaf);
        }
      }
      return sum;
    }
  }
  return 0;
}

idx_t ART::NoneLeafCount() {
  return sumNoneLeafCount(*this, *root, false);
}

idx_t ART::LeafCount() {
  return sumNoneLeafCount(*this, *root, true);
}

}  // namespace part
