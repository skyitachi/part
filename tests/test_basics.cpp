//
// Created by Shiping Yao on 2023/12/8.
//
#include <gtest/gtest.h>

#include <art.h>
#include <allocator.h>
#include <arena_allocator.h>
#include <art_key.h>

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

    SequentialSerializer writer("art.index");

    auto block_pointer = art.Serialize(writer);

    fmt::println("block_id: {}, offset_: {}", block_pointer.block_id, block_pointer.offset);

}
