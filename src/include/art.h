//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_ART_H
#define PART_ART_H
#include <memory>
#include <vector>
#include <optional>

#include "types.h"

namespace part {
class Node;
class FixedSizeAllocator;
class ARTKey;

class ART {
public:

  explicit ART(const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr = nullptr);

  ~ART();
  std::unique_ptr<Node> root;
  std::shared_ptr<std::vector<FixedSizeAllocator>> allocators;
  bool owns_data;

  // only support int64_t value
  void Put(const ARTKey& key, int64_t value);

  std::optional<int64_t> Get(const ARTKey& key);

private:
  void insert(Node& node, const ARTKey &key, idx_t depth, const int64_t& value);
};

} // namespace part

#endif // PART_ART_H
