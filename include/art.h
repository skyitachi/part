//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_ART_H
#define PART_ART_H
#include <memory>
#include <optional>
#include <vector>

#include "types.h"
#include "block.h"
#include "serializer.h"

namespace part {
class Node;
class FixedSizeAllocator;
class ARTKey;

class ART {
public:
  explicit ART(const std::shared_ptr<std::vector<FixedSizeAllocator>>
                   &allocators_ptr = nullptr);

  explicit ART(const std::string &metadata_path,
               const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr = nullptr);

  ~ART();
  std::unique_ptr<Node> root;
  std::shared_ptr<std::vector<FixedSizeAllocator>> allocators;
  bool owns_data;

  // only support int64_t value
  void Put(const ARTKey &key, idx_t doc_id);

  bool Get(const ARTKey &key, std::vector<idx_t> &result_ids);

  idx_t GetMemoryUsage();

  BlockPointer Serialize(Serializer &writer);

  void UpdateMetadata(BlockPointer pointer);

private:
  void insert(Node &node, const ARTKey &key, idx_t depth, const idx_t &value);
  std::optional<Node *> lookup(Node node, const ARTKey &key, idx_t depth);
  //! Insert a row ID into a leaf
  bool InsertToLeaf(Node &leaf, const idx_t row_id);

  int metadata_fd_;
};

} // namespace part

#endif // PART_ART_H
