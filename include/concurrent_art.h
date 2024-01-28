//
// Created by skyitachi on 24-1-27.
//

#ifndef PART_CONCURRENT_ART_H
#define PART_CONCURRENT_ART_H
#include <fstream>
#include <memory>
#include <vector>

#include "arena_allocator.h"
#include "art_key.h"
#include "block.h"
#include "concurrent_node.h"
#include "fixed_size_allocator.h"

namespace part {

class ConcurrentART {
  using FixedSizeAllocatorListPtr = std::shared_ptr<std::vector<FixedSizeAllocator>>;

 public:
  explicit ConcurrentART(const FixedSizeAllocatorListPtr &allocators_ptr = nullptr);

  explicit ConcurrentART(const std::string &index_path, const FixedSizeAllocatorListPtr &allocators_ptr = nullptr);

  ~ConcurrentART();

  std::unique_ptr<ConcurrentNode> root;

  FixedSizeAllocatorListPtr allocators;
  bool owns_data;

  bool Get(const ARTKey &key, std::vector<idx_t> &result_ids);

 private:
  bool lookup(ConcurrentNode &node, const ARTKey &key, idx_t depth, std::vector<idx_t> &result_ids);
};
}  // namespace part
#endif  // PART_CONCURRENT_ART_H
