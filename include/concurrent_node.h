//
// Created by skyitachi on 24-1-25.
//

#ifndef PART_CONCURRENT_NODE_H
#define PART_CONCURRENT_NODE_H
#include <atomic>

#include "node.h"

namespace part {

class ConcurrentNode: public Node {
 public:
  ConcurrentNode(const uint32_t buffer_id, const uint32_t offset) { SetPtr(buffer_id, offset); }

  ConcurrentNode(Deserializer &reader): Node(reader) {}

  ConcurrentNode(ART &art, Deserializer &reader): Node(art, reader) {}

  ConcurrentNode() {};

  void Lock();
  void Unlock();
  void RLock();
  void RUnlock();
  void Downgrade();
  void Upgrade();

 private:
  std::atomic<uint64_t> lock_;

};

}
#endif  // PART_CONCURRENT_NODE_H
