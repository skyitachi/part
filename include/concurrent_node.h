//
// Created by skyitachi on 24-1-25.
//

#ifndef PART_CONCURRENT_NODE_H
#define PART_CONCURRENT_NODE_H
#include <fmt/core.h>

#include <atomic>

#include "node.h"

namespace part {
class ConcurrentART;

// NOTE: 1. node cannot be serialized when accessed, different from Node (currently for simplicity)
class ConcurrentNode : public Node {
 public:
  ConcurrentNode(const uint32_t buffer_id, const uint32_t offset) {
    SetPtr(buffer_id, offset);
    // important
    lock_ = 0;
  }

  ConcurrentNode(Deserializer &reader) : Node(reader) {}

  ConcurrentNode(ART &art, Deserializer &reader) : Node(art, reader) {}

  ConcurrentNode(){};

  ConcurrentNode(const ConcurrentNode &other) {
    SetPtr(other.GetBufferId(), other.GetOffset());
    lock_ = 0;
  }

  ConcurrentNode &operator=(const ConcurrentNode &other) {
    SetPtr(other.GetBufferId(), other.GetOffset());
    // NOTE: is this ok???
    lock_ = other.lock_.load();
    return *this;
  }

  // keep lock_ states
  void Update(const ConcurrentNode &&other) {
    Reset();
    SetPtr(other.GetBufferId(), other.GetOffset());
  }

  static FixedSizeAllocator &GetAllocator(const ConcurrentART &art, NType type);

  void Lock();
  void Unlock();
  void RLock();
  void RUnlock();
  void Downgrade();
  void Upgrade();

  bool RLocked() const;
  bool Locked() const;

  inline void ResetAll() {
    Reset();
    lock_ = 0;
  }

 private:
  // NOTE: 如何传递锁状态是个问题
  std::atomic<uint64_t> lock_ = {0};
};

}  // namespace part
#endif  // PART_CONCURRENT_NODE_H
