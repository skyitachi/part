//
// Created by skyitachi on 24-1-27.
//

#ifndef PART_CONCURRENT_ART_H
#define PART_CONCURRENT_ART_H
#include <fstream>
#include <list>
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
  explicit ConcurrentART(const FixedSizeAllocatorListPtr allocators_ptr = nullptr);

  explicit ConcurrentART(const std::string &index_path, const FixedSizeAllocatorListPtr allocators_ptr = nullptr);

  ~ConcurrentART();

  std::unique_ptr<ConcurrentNode> root;

  FixedSizeAllocatorListPtr allocators;

  bool owns_data;

  bool Get(const ARTKey &key, std::vector<idx_t> &result_ids);

  void Put(const ARTKey &key, idx_t doc_id);

  BlockPointer ReadMetadata() const;

  ConcurrentNode *AllocateNode();

  void Draw(const std::string &outf) {
    std::ofstream out(outf);
    out << "digraph G {" << std::endl;
    idx_t id = 0;
    root->ToGraph(*this, out, id);
    out << "}" << std::endl;
    out.close();
  }

  void Serialize();

  void Deserialize();

  BlockPointer Serialize(Serializer &writer);

  void Merge(ART &other);

 private:
  bool lookup(ConcurrentNode *node, const ARTKey &key, idx_t depth, std::vector<idx_t> &result_ids);
  // if need retry
  bool insert(ConcurrentNode &node, const ARTKey &key, idx_t depth, const idx_t &doc_id);

  bool insertToLeaf(ConcurrentNode *leaf, idx_t doc_id);

  int metadata_fd_ = -1;
  int index_fd_ = -1;
  std::string index_path_;

  std::list<ConcurrentNode *> node_allocators_;
};
}  // namespace part
#endif  // PART_CONCURRENT_ART_H
