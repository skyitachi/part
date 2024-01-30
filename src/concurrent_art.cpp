//
// Created by skyitachi on 24-1-27.
//
#include "concurrent_art.h"

#include <thread>

#include "leaf.h"
#include "prefix.h"

namespace part {

bool ConcurrentART::Get(const part::ARTKey& key, std::vector<idx_t>& result_ids) {
  while (lookup(*root, key, 0, result_ids)) {
    result_ids.clear();
    std::this_thread::yield();
  }
  return !result_ids.empty();
}

bool ConcurrentART::lookup(ConcurrentNode& node, const ARTKey& key, idx_t depth, std::vector<idx_t>& result_ids) {
  auto next_node = std::ref(node);
  next_node.get().RLock();
  if (next_node.get().IsDeleted()) {
    // NOTE: important to unlock correctly
    next_node.get().RUnlock();
    return true;
  }
  bool retry = false;
  while (next_node.get().IsSet()) {
    if (next_node.get().GetType() == NType::PREFIX) {
      CPrefix::Traverse(*this, next_node, key, depth, retry);
      if (retry) {
        next_node.get().RUnlock();
        return retry;
      }
      assert(next_node.get().RLocked());
      if (next_node.get().GetType() == NType::PREFIX) {
        // NOTE: need unlock asap
        next_node.get().RUnlock();
        return false;
      }
    }

    // inlined leaf can return directly
    if (next_node.get().GetType() == NType::LEAF_INLINED) {
      result_ids.emplace_back(next_node.get().GetDocId());
      next_node.get().RUnlock();
      return false;
    }

    //
    if (next_node.get().GetType() == NType::LEAF) {
      auto& cleaf = CLeaf::Get(*this, next_node.get());
      // NOTE: GetDocIds already released lock
      cleaf.GetDocIds(*this, next_node.get(), result_ids, std::numeric_limits<int64_t>::max(), retry);
      return retry;
    }
  }
  return false;
}

void ConcurrentART::Put(const ARTKey& key, idx_t doc_id) {
  while (insert(*root, key, 0, doc_id)) {
    std::this_thread::yield();
  }
}

bool ConcurrentART::insert(ConcurrentNode& node, const ARTKey& key, idx_t depth, const idx_t& doc_id) {
  node.RLock();
  if (node.IsDeleted()) {
    return true;
  }
  if (!node.IsSet()) {
    assert(depth <= key.len);
    auto ref = std::ref(node);
    ref.get().Upgrade();
    CPrefix::New(*this, ref, key, depth, key.len - depth);
    assert(ref.get().Locked());
    CLeaf::New(ref, doc_id);
    ref.get().Unlock();
    return false;
  }
  // TODO:
  return false;
}

}  // namespace part
