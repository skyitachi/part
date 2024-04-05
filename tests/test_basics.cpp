//
// Created by Shiping Yao on 2023/12/8.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <memory>
#include <type_traits>

#include "art.h"
#include "leaf.h"
#include "node.h"
#include "node16.h"
#include "node256.h"
#include "node4.h"
#include "node48.h"
#include "prefix.h"
#include "util.h"

using namespace part;

TEST(ArtTest, basic) {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  assert(arena_allocator.IsEmpty());
}

TEST(ARTSerializeTest, basic) {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ART art;

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  art.Put(k1, 123);

  art.Put(k1, 124);

  art.Put(k1, 123);

  std::vector<idx_t> results;
  bool success = art.Get(k1, results);
  EXPECT_TRUE(success);
  EXPECT_EQ(3, results.size());
  EXPECT_EQ(123, results[0]);
  EXPECT_EQ(124, results[1]);
  EXPECT_EQ(123, results[2]);

  SequentialSerializer writer("art.index", META_OFFSET);

  auto block_pointer = art.Serialize(writer);

  fmt::println("block_id: {}, offset_: {}", block_pointer.block_id, block_pointer.offset);

  SequentialSerializer meta_writer("art.index");

  art.UpdateMetadata(block_pointer, meta_writer);

  meta_writer.Flush();
}

TEST(ARTDeserializeTest, Basic) {
  ART art("art.index");
  EXPECT_FALSE(art.root->IsSerialized());

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  std::vector<idx_t> results;
  bool success = art.Get(k1, results);

  EXPECT_TRUE(success);
  EXPECT_EQ(3, results.size());
  EXPECT_EQ(123, results[0]);
  EXPECT_EQ(124, results[1]);
  EXPECT_EQ(123, results[2]);
}

TEST(ARTTest, NodeCount) {
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  art.Put(k1, 1);

  EXPECT_EQ(art.NoneLeafCount(), 1);
  EXPECT_EQ(art.LeafCount(), 1);

  // same prefix
  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  art.Put(k2, 2);

  EXPECT_EQ(art.NoneLeafCount(), 1);
  EXPECT_EQ(art.LeafCount(), 1);

  ARTKey k3 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 11);
  art.Put(k3, 2);

  EXPECT_EQ(art.NoneLeafCount(), 2);
  EXPECT_EQ(art.LeafCount(), 2);
}

TEST(ARTNodeCountTest, BigARTNodeCount) {
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  ART art;
  Random random;

  auto kv_pairs = random.GenKvPairs(100000, arena_allocator);
  for (const auto& [k, v] : kv_pairs) {
    art.Put(k, v);
  }

  fmt::println("non_leaf_count: {}, leaf count: {}", art.NoneLeafCount(), art.LeafCount());
}

TEST(ARTNodeCountTest, SequenceBigARTNodeCount) {
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);

  ART art;
  Random random;

  auto kv_pairs = random.GenOrderedKvPairs(100000, arena_allocator);
  for (const auto& [k, v] : kv_pairs) {
    art.Put(k, v);
  }

  fmt::println("non_leaf_count: {}, leaf count: {}", art.NoneLeafCount(), art.LeafCount());
}

TEST(ARTTest, ARTKeyMemoryLeak) {
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  Random random;
  auto kv_pairs = random.GenKvPairs(10, arena_allocator);
  for (auto& pair : kv_pairs) {
    fmt::println("key ken: {}", pair.first.len);
  }
}

TEST(ARTTest, DeleteTest) {
  ART art;

  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  Random random;

  auto kv_pairs = random.GenKvPairs(10000, arena_allocator);

  for (auto& pair : kv_pairs) {
    art.Put(pair.first, pair.second);
  }

  for (auto& pair : kv_pairs) {
    std::vector<idx_t> results;
    EXPECT_TRUE(art.Get(pair.first, results));
    EXPECT_EQ(results[0], pair.second);
  }

  for (auto& pair : kv_pairs) {
    art.Delete(pair.first, pair.second);
    std::vector<idx_t> results;
    //    bool success = art.Get(pair.first, results);
    EXPECT_FALSE(art.Get(pair.first, results));
  }

  //  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1);
  //  art.Put(k1, 1);
  //
  //  art.Delete(k1, 1);
  //
  //  std::vector<idx_t> results;
  //  art.Get(k1, results);
  //  EXPECT_EQ(results.size(), 0);
  //
  //  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 2);
  //  art.Put(k2, 2);
  //
  //  EXPECT_EQ(results[0], 1);
}

TEST(ARTTest, MergeTest) {
  // important
  auto allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
  allocators->emplace_back(sizeof(Prefix), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Leaf), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node4), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node16), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node48), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node256), Allocator::DefaultAllocator());

  ART left(allocators);
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  auto k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  auto k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 11);
  left.Put(k1, 1000);

  std::vector<idx_t> results;
  EXPECT_TRUE(left.Get(k1, results));
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1000, results[0]);
  results.clear();

  ART right(allocators);
  right.Put(k2, 1001);

  EXPECT_TRUE(right.Get(k2, results));
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1001, results[0]);
  results.clear();

  left.Merge(right);

  EXPECT_TRUE(left.Get(k2, results));
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1001, results[0]);
}

TEST(ARTTest, MergeMediumTest) {
  auto allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
  allocators->emplace_back(sizeof(Prefix), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Leaf), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node4), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node16), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node48), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node256), Allocator::DefaultAllocator());

  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  Random random;

  auto kv_pairs = random.GenKvPairs(10000, arena_allocator);

  ART left(allocators);
  for (idx_t i = 0; i < kv_pairs.size() / 2; i++) {
    left.Put(kv_pairs[i].first, kv_pairs[i].second);
  }
  //  left.Draw("left.dot");

  for (idx_t i = 0; i < kv_pairs.size() / 2; i++) {
    std::vector<idx_t> results;
    auto success = left.Get(kv_pairs[i].first, results);
    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv_pairs[i].second, results[0]);
  }

  ART right(allocators);
  for (idx_t i = kv_pairs.size() / 2; i < kv_pairs.size(); i++) {
    right.Put(kv_pairs[i].first, kv_pairs[i].second);
  }
  //  right.Draw("right.dot");

  for (idx_t i = kv_pairs.size() / 2; i < kv_pairs.size(); i++) {
    std::vector<idx_t> results;
    auto success = right.Get(kv_pairs[i].first, results);
    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv_pairs[i].second, results[0]);
  }

  left.Merge(right);

  //  left.Draw("merge.dot");
  for (auto& pair : kv_pairs) {
    std::vector<idx_t> results;
    auto success = left.Get(pair.first, results);
    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(pair.second, results[0]);
  }
}

TEST(ARTTest, DrawTest) {
  ART art;
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  auto k1 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 1);
  art.Put(k1, 1);

  auto k2 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 2);
  art.Put(k2, 2);

  auto k3 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 3);
  art.Put(k3, 3);
  auto k4 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 4);
  art.Put(k4, 4);

  art.Draw("art.dot");
}

TEST(ARTTest, MergeFixture) {
  auto allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
  allocators->emplace_back(sizeof(Prefix), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Leaf), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node4), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node16), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node48), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node256), Allocator::DefaultAllocator());

  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  uint8_t data[10][8] = {
      {183, 40, 50, 212, 24, 77, 123, 190},   {190, 71, 43, 17, 99, 135, 96, 191},
      {195, 234, 91, 93, 96, 212, 213, 162},  {203, 69, 104, 75, 223, 166, 177, 80},
      {237, 52, 189, 151, 236, 111, 170, 65}, {143, 83, 232, 152, 224, 234, 235, 150},
      {159, 110, 8, 28, 255, 206, 172, 222},  {172, 4, 180, 145, 143, 21, 41, 4},
      {190, 98, 215, 234, 15, 132, 127, 87},  {246, 59, 249, 227, 205, 232, 110, 91},
  };

  std::vector<std::pair<ARTKey, long>> kv_pairs;
  for (int i = 0; i < 10; i++) {
    ARTKey key(data[i], 8);
    kv_pairs.emplace_back(key, i);
  }

  ART left(allocators);
  for (idx_t i = 0; i < kv_pairs.size() / 2; i++) {
    left.Put(kv_pairs[i].first, kv_pairs[i].second);
  }
  left.Draw("left_fixture.dot");

  for (idx_t i = 0; i < kv_pairs.size() / 2; i++) {
    std::vector<idx_t> results;
    auto success = left.Get(kv_pairs[i].first, results);
    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv_pairs[i].second, results[0]);
  }

  ART right(allocators);
  for (idx_t i = kv_pairs.size() / 2; i < kv_pairs.size(); i++) {
    right.Put(kv_pairs[i].first, kv_pairs[i].second);
  }
  right.Draw("right_fixture.dot");

  for (idx_t i = kv_pairs.size() / 2; i < kv_pairs.size(); i++) {
    std::vector<idx_t> results;
    auto success = right.Get(kv_pairs[i].first, results);
    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv_pairs[i].second, results[0]);
  }

  // serialized tree cannot merge
  left.Merge(right);

  left.Draw("merge_fixture.dot");
  for (auto& pair : kv_pairs) {
    std::vector<idx_t> results;
    auto success = left.Get(pair.first, results);
    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(pair.second, results[0]);
  }
}

TEST(ARTTest, SerializedMergeTest) {
  ART left("left.idx");
  Allocator& allocator = Allocator::DefaultAllocator();

  ArenaAllocator arena_allocator(allocator, 16384);
  Random random;

  auto allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
  allocators->emplace_back(sizeof(Prefix), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Leaf), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node4), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node16), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node48), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node256), Allocator::DefaultAllocator());

  auto kv_pairs = random.GenKvPairs(10000, arena_allocator);

  for (idx_t i = 0; i < kv_pairs.size() / 2; i++) {
    left.Put(kv_pairs[i].first, kv_pairs[i].second);
  }
  left.Serialize();

  ART right("right.idx");
  for (idx_t i = kv_pairs.size() / 2; i < kv_pairs.size(); i++) {
    right.Put(kv_pairs[i].first, kv_pairs[i].second);
  }
  right.Serialize();

  // deserialize
  {
    ART left1("left.idx", allocators);
    ART right1("right.idx", allocators);

    left1.Merge(right1);
    for (auto& pair : kv_pairs) {
      std::vector<idx_t> results;
      auto success = left1.Get(pair.first, results);
      EXPECT_TRUE(success);
      EXPECT_EQ(1, results.size());
      EXPECT_EQ(pair.second, results[0]);
    }
  }
}

TEST(ARTTest, TypeTraitsTest) {
  auto ret = std::is_trivially_copyable<Node>::value;
  fmt::println("copyable: {}", ret);
  ret = std::is_copy_constructible<Node>::value;
  fmt::println("construct: {}", ret);
  ret = std::is_move_constructible<Node>::value;
  fmt::println("move construct: {}", ret);
  ret = std::is_copy_assignable<Node>::value;
  fmt::println("copy construct: {}", ret);
  ret = std::is_move_assignable<Node>::value;

  fmt::println("move assign: {}", ret);
}

struct IndexPointer {
  uint64_t data;
  std::atomic<int64_t> lock = {0};
};

struct CNode {
  char data[10];
  std::unique_ptr<IndexPointer> ptr;
};

struct CNode2 {
  char data[10];
  IndexPointer* ptr;
};

TEST(ARTTest, PointerTest) {
  fmt::println("sizeof(uintptr_t) = {}, sizeof(IndexPointer) = {} ", sizeof(uintptr_t), sizeof(IndexPointer));
  int64_t a = 1;
  //  uintptr_t ptr = &a;
  fmt::println("sizeof(CNode) = {}", sizeof(CNode));
  fmt::println("sizeof(CNode2) = {}", sizeof(CNode2));

  auto iptr = std::make_unique<IndexPointer>();
  iptr->data = 10;

  iptr->lock.store(10);
  auto v = iptr->lock.load();

  fmt::println("value = {}", v);

  char* addr = (char*)malloc(sizeof(CNode));
  auto cnode = new (addr) CNode();

  cnode->ptr = std::make_unique<IndexPointer>();
  cnode->ptr->data = 100;
  cnode->ptr->lock.store(10);

  fmt::println("lock value: {}", cnode->ptr->lock.load());
  // NOTE: manual manage unique_ptr memory, placement new memory cannot use smart pointer directly
  cnode->ptr.reset(nullptr);

  free(addr);

  fmt::println("CNode is trivial: {}", std::is_trivial<CNode>::value);
  fmt::println("CNode2 is trivial: {}", std::is_trivial<CNode2>::value);

  CNode2* cnode2 = (CNode2*)malloc(sizeof(CNode2));
  cnode2->ptr = new IndexPointer();

  delete cnode2->ptr;
  free(cnode2);
}

TEST(ARTTest, LongPrefixTest) {
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  std::vector<ARTKey> keys;

  keys.push_back(ARTKey::CreateARTKey<std::string_view>(arena_allocator, "123456789123456"));
  keys.push_back(ARTKey::CreateARTKey<std::string_view>(arena_allocator, "123456789123457"));

  art.Put(keys[0], 1);
  art.Put(keys[1], 2);

  std::vector<idx_t> result_ids;
  art.Get(keys[0], result_ids);
  fmt::println("results: {}", result_ids.size());
  art.Draw("long_prefix.dot");
}

TEST(ARTTest, SwapTest) {
  int a = 10;
  int b = 20;
  int* pa = &a;
  int* pb = &b;

  fmt::println("pa = {}, pb = {}", static_cast<void*>(pa), static_cast<void*>(pb));
  std::swap(pa, pb);
  fmt::println("after std::swap, pa = {}, pb = {}", static_cast<void*>(pa), static_cast<void*>(pb));
}

TEST(ARTTEST, Reference) {
  int a = 10;
  int& b = a;
  b = 100;
  fmt::println("a: {}, b: {}", a, b);

  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ConcurrentNode* cnode1 = art.AllocateNode();
  cnode1->Lock();
  CLeaf::New(*cnode1, 1);
  CLeaf::MoveInlinedToLeaf(art, *cnode1);
  // important must be auto &cleaf
  auto& cleaf = CLeaf::Get(art, *cnode1);
  auto node = cnode1;
  fmt::println("before append, count: {}, node: {}, cnode1: {}, cnode1 data: {}", cleaf.count, static_cast<void*>(node),
               static_cast<void*>(cnode1), cnode1->GetData());
  cleaf = cleaf.Append(art, node, 2);
  node->Lock();
  fmt::println("append 2: cleaf.count {}", cleaf.count);
  cleaf = cleaf.Append(art, node, 3);
  fmt::println("append 3: cleaf.count {}", cleaf.count);
  node->Lock();
  cleaf = cleaf.Append(art, node, 4);
  fmt::println("append 4: cleaf.count {}", cleaf.count);
  node->Lock();
  {
    auto& new_leaf = CLeaf::Get(art, *cnode1);
    fmt::println("append 4, count: {}, node: {}, cnode1: {}, cnode1 data: {}, cnode_pointer: {}", new_leaf.count,
                 static_cast<void*>(node), static_cast<void*>(cnode1), cnode1->GetData(),
                 static_cast<void*>(CLeaf::GetPointer(art, cnode1)));
  }
  //  auto new_leaf = cleaf.Append(art, node, 5);
  //  fmt::println("append 5: cleaf.count {}, new_leaf: {}", cleaf.count, new_leaf.count);
  cleaf.Append(art, node, 5);
  node->RLock();
  auto& new_leaf = CLeaf::Get(art, *node);
  node->RUnlock();

  fmt::println("append 5: cleaf.count {}", new_leaf.count);

  cnode1->Lock();
  cleaf = CLeaf::Get(art, *cnode1);
  fmt::println("after append, count: {}, node: {}, cnode1: {}, cnode_pointer: {}", cleaf.count,
               static_cast<void*>(node), static_cast<void*>(cnode1),
               static_cast<void*>(CLeaf::GetPointer(art, cnode1)));

  cnode1->Unlock();
  node->Lock();
  auto cleaf2 = CLeaf::Get(art, *node);
  node->Unlock();

  fmt::println("new leaf count: {}, node: {}, cnode1: {}", cleaf2.count, static_cast<void*>(node),
               static_cast<void*>(cnode1));

  fmt::println("node data: {}, cnode1 data: {}", node->GetData(), cnode1->GetData());
  fmt::println("node buffer_id: {}, offset: {}", node->GetBufferId(), node->GetOffset());
  fmt::println("cnode1 buffer_id: {}, offset: {}", cnode1->GetBufferId(), cnode1->GetOffset());
}

TEST(ARTTEST, ReferenceReproduce) {
  ART art;
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  Node node1;
  Leaf::New(node1, 1);
  Leaf::MoveInlinedToLeaf(art, node1);

  auto l_node_ref = std::ref(node1);
  reference<Leaf> l_leaf = Leaf::Get(art, node1);

  for (idx_t i = 0; i < 5; i++) {
    l_leaf = l_leaf.get().Append(art, i);
  }

  auto l_origin_leaf = Leaf::Get(art, node1);

  fmt::println("origin leaf: {}", l_origin_leaf.count);

  fmt::println("leaf: {}", l_leaf.get().count);
}

TEST(ConcurrentARTTEST, ReferenceReproduce) {
  ConcurrentART art;
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ConcurrentNode* node1 = art.AllocateNode();
  ConcurrentNode* next_node = node1;
  node1->Lock();
  CLeaf::New(*node1, 1);
  CLeaf::MoveInlinedToLeaf(art, *node1);

  reference<CLeaf> l_leaf = CLeaf::Get(art, *node1);

  node1->Unlock();
  for (idx_t i = 0; i < 5; i++) {
    next_node->Lock();
    l_leaf = l_leaf.get().Append(art, next_node, i);
  }

  node1->Lock();
  auto l_origin_leaf = CLeaf::Get(art, *node1);
  node1->Unlock();

  fmt::println("origin leaf: {}", l_origin_leaf.count);

  fmt::println("leaf: {}", l_leaf.get().count);
}

TEST(ConcurrentARTTEST, ReferenceReproduce2) {
  ConcurrentART art;
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ConcurrentNode* node1 = art.AllocateNode();
  fmt::println("node1 = {}", static_cast<void*>(node1));
  ConcurrentNode* next_node = node1;
  node1->Lock();
  CLeaf::New(*node1, 1);
  CLeaf::MoveInlinedToLeaf(art, *node1);

  auto& l_leaf = CLeaf::Get(art, *node1);

  fmt::println("is_reference:{} first ptr: {}", std::is_reference<decltype(l_leaf)>::value,
               static_cast<void*>(l_leaf.GetPointer(art, node1)));

  node1->Unlock();
  for (idx_t i = 0; i < 5; i++) {
    next_node->Lock();
    l_leaf = l_leaf.Append(art, next_node, i);
    fmt::println("is_reference:{}, after put {},  next_node:{}, ptr: {}", std::is_reference<decltype(l_leaf)>::value, i,
                 static_cast<void*>(next_node), static_cast<void*>(l_leaf.GetPointer(art, next_node)));
  }

  node1->Lock();
  fmt::println("after append node1 = {}", static_cast<void*>(node1));
  fmt::println("after append next_node = {}", static_cast<void*>(next_node));
  auto l_origin_leaf = CLeaf::Get(art, *node1);
  node1->Unlock();

  fmt::println("is_reference: {}, origin leaf: {}", std::is_reference<decltype(l_origin_leaf)>::value,
               l_origin_leaf.count);

  fmt::println("leaf: {}", l_leaf.count);

  next_node->Lock();
  auto new_leaf = CLeaf::Get(art, *next_node);
  fmt::println("new leaf: {}", new_leaf.count);
  next_node->Unlock();
}

TEST(ARTTest, StringKeyTest) {
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

//  auto k1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "xengine");
//
//  art.Put(k1, 1);
//  art.Put(k1, -1);

  auto k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1000);
  auto k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 999);

  // NOTE: 这个doc_id是有问题的
  art.Put(k2, -1);

  art.Draw("999.dot");

  art.Put(k2, 1);

}

TEST(ARTTest, LazyDeleteTest) {
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  auto k1 = ARTKey::CreateARTKey<int64_t >(arena_allocator, 1);

  art.Put(k1, 1);

  std::vector<idx_t> result_ids;
  art.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  art.LazyDelete(k1, 1);

  result_ids.clear();
  art.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 0);

  ASSERT_ANY_THROW(art.Put(k1, -1));
}
