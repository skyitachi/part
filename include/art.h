//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_ART_H
#define PART_ART_H
#include <memory>
#include <optional>
#include <vector>

#include "arena_allocator.h"
#include "art_key.h"
#include "block.h"
#include "node.h"
#include "serializer.h"
#include "types.h"

namespace part {
class Node;
class FixedSizeAllocator;
class ARTKey;

class ART {
 public:
  explicit ART(const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr = nullptr);

  explicit ART(const std::string &index_path,
               const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr = nullptr);

  ~ART();
  std::unique_ptr<Node> root;
  std::shared_ptr<std::vector<FixedSizeAllocator>> allocators;
  bool owns_data;

  // only support int64_t value
  void Put(const ARTKey &key, idx_t doc_id);

  bool Get(const ARTKey &key, std::vector<idx_t> &result_ids);

  idx_t GetMemoryUsage();

  idx_t LeafCount();

  idx_t NoneLeafCount();

  BlockPointer Serialize(Serializer &writer);

  void UpdateMetadata(BlockPointer pointer, Serializer &meta_writer);

  BlockPointer ReadMetadata() const;

  void Serialize();

  void Deserialize();

  int GetIndexFileFd() { return index_fd_; }

 private:
  void insert(Node &node, const ARTKey &key, idx_t depth, const idx_t &value);
  std::optional<Node *> lookup(Node node, const ARTKey &key, idx_t depth);
  //! Insert a row ID into a leaf
  bool InsertToLeaf(Node &leaf, const idx_t row_id);

  int metadata_fd_;
  int index_fd_;
  std::string index_path_;
};

}  // namespace part

#endif  // PART_ART_H
