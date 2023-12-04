//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_ARENA_ALLOCATOR_H
#define PART_ARENA_ALLOCATOR_H

#include "allocator.h"
#include "helper.h"

namespace part {
  struct ArenaChunk {
    ArenaChunk(Allocator &allocator, idx_t size);
    ~ArenaChunk();

    AllocatedData data;
    idx_t current_position;
    idx_t maximum_size;
    std::unique_ptr<ArenaChunk> next;
    ArenaChunk *prev;
  };


  class ArenaAllocator {
    static constexpr const idx_t ARENA_ALLOCATOR_INITIAL_CAPACITY = 2048;

  public:
    ArenaAllocator(Allocator &allocator, idx_t initial_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY);
    ~ArenaAllocator();

    data_ptr_t Allocate(idx_t size);
    data_ptr_t Reallocate(data_ptr_t pointer, idx_t old_size, idx_t size);

    data_ptr_t AllocateAligned(idx_t size);
    data_ptr_t ReallocateAligned(data_ptr_t pointer, idx_t old_size, idx_t size);

    //! Resets the current head and destroys all previous arena chunks
    void Reset();
    void Destroy();
    void Move(ArenaAllocator &allocator);

    ArenaChunk *GetHead();
    ArenaChunk *GetTail();

    bool IsEmpty() const;
    idx_t SizeInBytes() const;

    //! Returns an "Allocator" wrapper for this arena allocator
    Allocator &GetAllocator() {
      return arena_allocator;
    }

  private:
    //! Internal allocator that is used by the arena allocator
    Allocator &allocator;
    idx_t current_capacity;
    std::unique_ptr<ArenaChunk> head;
    ArenaChunk *tail;
    //! An allocator wrapper using this arena allocator
    Allocator arena_allocator;
  };
}
#endif //PART_ARENA_ALLOCATOR_H
