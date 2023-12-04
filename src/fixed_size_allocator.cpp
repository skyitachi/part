//
// Created by Shiping Yao on 2023/12/4.
//
#include "fixed_size_allocator.h"

namespace part {

constexpr idx_t FixedSizeAllocator::BASE[];
constexpr uint8_t FixedSizeAllocator::SHIFT[];

FixedSizeAllocator::FixedSizeAllocator(const idx_t allocation_size, Allocator &allocator)
  : allocation_size(allocation_size), total_allocations(0), allocator(allocator){

  idx_t bits_per_value = sizeof(validity_t) * 8;
  idx_t curr_alloc_size = 0;

  bitmask_count = 0;
  allocations_per_buffer = 0;
  while(curr_alloc_size < BUFFER_ALLOC_SIZE) {
    if (!bitmask_count || (bitmask_count * bits_per_value) % allocations_per_buffer == 0) {
      bitmask_count++;
      curr_alloc_size += sizeof(validity_t);
    }

    auto remaining_alloc_size = BUFFER_ALLOC_SIZE - curr_alloc_size;
    auto remaining_allocations = std::min(remaining_alloc_size / allocation_size, bits_per_value);

    if (remaining_allocations == 0) {
      break;
    }
    allocations_per_buffer += remaining_allocations;
    curr_alloc_size += remaining_allocations * allocation_size;
  }
  allocation_offset = bitmask_count * sizeof(validity_t);
}

FixedSizeAllocator::~FixedSizeAllocator() {
  for(auto &buffer: buffers) {
    allocator.FreeData(buffer.ptr, BUFFER_ALLOC_SIZE);
  }
}

Node FixedSizeAllocator::New() {
  if (buffers_with_free_space.empty()) {
    idx_t buffer_id = buffers.size();
    auto buffer = allocator.AllocateData(BUFFER_ALLOC_SIZE);
    buffers.emplace_back(buffer, 0);
    buffers_with_free_space.insert(buffer_id);

    // TODO mask
  }
  assert(!buffers_with_free_space.empty());
  auto buffer_id = (uint32_t)*buffers_with_free_space.begin();

  buffers[buffer_id].allocation_count++;
  total_allocations++;
  if (buffers[buffer_id].allocation_count == allocations_per_buffer) {
    buffers_with_free_space.erase(buffer_id);
  }


}


}
