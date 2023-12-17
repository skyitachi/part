//
// Created by Shiping Yao on 2023/12/8.
//
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <art.h>
#include <allocator.h>
#include <arena_allocator.h>
#include <art_key.h>
#include <node.h>
#include <fmt/core.h>

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

    SequentialSerializer writer("art.index");

    auto block_pointer = art.Serialize(writer);

    fmt::println("block_id: {}, offset_: {}", block_pointer.block_id, block_pointer.offset);

    SequentialSerializer meta_writer("art.meta");

    art.UpdateMetadata(block_pointer, meta_writer);
}

TEST(ARTDeserializeTest, Basic) {

    ART art("art.meta", "art.index");
    art.Deserialize();
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

TEST(ARTSerializeAndDeserialize, Basic) {
    
}
