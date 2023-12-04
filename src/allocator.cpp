//
// Created by Shiping Yao on 2023/12/3.
//
#include <cassert>
#include <exception>
#include <fmt/core.h>

#include "allocator.h"

namespace part {

  struct AllocatorDebugInfo {
#ifdef DEBUG
    AllocatorDebugInfo();
    ~AllocatorDebugInfo();

    void AllocateData(data_ptr_t pointer, idx_t size);
    void FreeData(data_ptr_t pointer, idx_t size);
    void ReallocateData(data_ptr_t pointer, data_ptr_t new_pointer, idx_t old_size, idx_t new_size);

  private:
    //! The number of bytes that are outstanding (i.e. that have been allocated - but not freed)
    //! Used for debug purposes
    atomic<idx_t> allocation_count;
#ifdef PART_DEBUG_ALLOCATION
    mutex pointer_lock;
    //! Set of active outstanding pointers together with stack traces
    unordered_map<data_ptr_t, pair<idx_t, string>> pointers;
#endif
#endif
  };


PrivateAllocatorData::PrivateAllocatorData() {
}

PrivateAllocatorData::~PrivateAllocatorData() {

}

Allocator::Allocator()
  : Allocator(Allocator::DefaultAllocate, Allocator::DefaultFree, Allocator::DefaultReallocate, nullptr) {
}

Allocator::Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
                     reallocate_function_ptr_t reallocate_function_p, std::unique_ptr<PrivateAllocatorData> private_data_p)
    : allocate_function(allocate_function_p), free_function(free_function_p),
      reallocate_function(reallocate_function_p), private_data(std::move(private_data_p)) {
  assert(allocate_function);
  assert(free_function);
  assert(reallocate_function);
#ifdef DEBUG
  if (!private_data) {
  private_data = make_uniq<PrivateAllocatorData>();
}
private_data->debug_info = make_uniq<AllocatorDebugInfo>();
#endif
}

Allocator::~Allocator() {
}

data_ptr_t Allocator::AllocateData(idx_t size) {
  assert(size > 0);
  if (size >= MAXIMUM_ALLOC_SIZE) {
//    assert(false);
    throw std::invalid_argument(fmt::format("Requested allocation size of {} is out of range - maximum allocation size is {}",
                            size, MAXIMUM_ALLOC_SIZE));
  }
  auto result = allocate_function(private_data.get(), size);
#ifdef DEBUG
  D_ASSERT(private_data);
private_data->debug_info->AllocateData(result, size);
#endif
  if (!result) {
    throw std::invalid_argument(fmt::format("Failed to allocate block of {} bytes", size));
  }
  return result;
}

void Allocator::FreeData(data_ptr_t ptr, idx_t size) {
  if (!ptr) {
    return;
  }
  assert(size > 0);
#ifdef DEBUG
  assert(private_data);
	private_data->debug_info->FreeData(pointer, size);
#endif
  free_function(private_data.get(), ptr, size);
}

data_ptr_t Allocator::ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t size) {
  if (!pointer) {
    return nullptr;
  }
  if (size >= MAXIMUM_ALLOC_SIZE) {
    assert(false);
    throw std::invalid_argument(
        fmt::format("Requested re-allocation size of {} is out of range - maximum allocation size is {}", size,
        MAXIMUM_ALLOC_SIZE));
  }
  auto new_pointer = reallocate_function(private_data.get(), pointer, old_size, size);
#ifdef DEBUG
  assert(private_data);
private_data->debug_info->ReallocateData(pointer, new_pointer, old_size, size);
#endif
  if (!new_pointer) {
    throw std::invalid_argument(fmt::format("Failed to re-allocate block of {} bytes", size));
  }
  return new_pointer;
}

std::shared_ptr<Allocator> &Allocator::DefaultAllocatorReference() {
  static std::shared_ptr<Allocator> DEFAULT_ALLOCATOR = std::make_shared<Allocator>();
  return DEFAULT_ALLOCATOR;
}

Allocator& Allocator::DefaultAllocator() {
  return *DefaultAllocatorReference();
}


#ifdef DEBUG
  AllocatorDebugInfo::AllocatorDebugInfo() {
    allocation_count = 0;
  }
  AllocatorDebugInfo::~AllocatorDebugInfo() {
#ifdef PART_DEBUG_ALLOCATION
    if (allocation_count != 0) {
      printf("Outstanding allocations found for Allocator\n");
      for (auto &entry : pointers) {
        printf("Allocation of size %llu at address %p\n", entry.second.first, (void *)entry.first);
        printf("Stack trace:\n%s\n", entry.second.second.c_str());
        printf("\n");
      }
    }
#endif
    //! Verify that there is no outstanding memory still associated with the batched allocator
    //! Only works for access to the batched allocator through the batched allocator interface
    //! If this assertion triggers, enable DUCKDB_DEBUG_ALLOCATION for more information about the allocations
    D_ASSERT(allocation_count == 0);
  }

  void AllocatorDebugInfo::AllocateData(data_ptr_t pointer, idx_t size) {
    allocation_count += size;
#ifdef PART_DEBUG_ALLOCATION
    lock_guard<mutex> l(pointer_lock);
    pointers[pointer] = make_pair(size, Exception::GetStackTrace());
#endif
  }

  void AllocatorDebugInfo::FreeData(data_ptr_t pointer, idx_t size) {
    D_ASSERT(allocation_count >= size);
    allocation_count -= size;
#ifdef PART_DEBUG_ALLOCATION
    lock_guard<mutex> l(pointer_lock);
    // verify that the pointer exists
    D_ASSERT(pointers.find(pointer) != pointers.end());
    // verify that the stored size matches the passed in size
    D_ASSERT(pointers[pointer].first == size);
    // erase the pointer
    pointers.erase(pointer);
#endif
  }

  void AllocatorDebugInfo::FreeData(data_ptr_t pointer, idx_t size) {
    D_ASSERT(allocation_count >= size);
    allocation_count -= size;
#ifdef PART_DEBUG_ALLOCATION
    lock_guard<mutex> l(pointer_lock);
    // verify that the pointer exists
    D_ASSERT(pointers.find(pointer) != pointers.end());
    // verify that the stored size matches the passed in size
    D_ASSERT(pointers[pointer].first == size);
    // erase the pointer
    pointers.erase(pointer);
#endif
  }

  void AllocatorDebugInfo::ReallocateData(data_ptr_t pointer, data_ptr_t new_pointer, idx_t old_size, idx_t new_size) {
    FreeData(pointer, old_size);
    AllocateData(new_pointer, new_size);
  }

#endif

AllocatedData::AllocatedData(): allocator(nullptr), pointer(nullptr), allocated_size(0) {
}

AllocatedData::AllocatedData(Allocator &allocator, data_ptr_t pointer, idx_t allocated_size)
  : allocator(&allocator), pointer(pointer), allocated_size(allocated_size) {
}

AllocatedData::~AllocatedData() {
  Reset();
}

AllocatedData::AllocatedData(AllocatedData &&other) noexcept
  : allocator(other.allocator), pointer(nullptr), allocated_size(0) {
  std::swap(pointer, other.pointer);
  std::swap(allocated_size, other.allocated_size);
}

AllocatedData &AllocatedData::operator=(AllocatedData &&other) noexcept {
  std::swap(allocator, other.allocator);
  std::swap(pointer, other.pointer);
  std::swap(allocated_size, other.allocated_size);
  return *this;
}

void AllocatedData::Reset() {
  if (!pointer) {
    return;
  }
  allocator->FreeData(pointer, allocated_size);
  allocated_size = 0;
  pointer = nullptr;
}

}

