//
// Created by skyitachi on 24-1-25.
//
#include "concurrent_node.h"

#include <thread>

namespace part {
constexpr uint32_t RETRY_THRESHOLD = 100;
constexpr uint64_t HAS_WRITER = ~0L;

void ConcurrentNode::RLock() {
  int retry = 0;
  while (true) {
    uint64_t prev = lock_.load();
    if (prev != HAS_WRITER) {
      uint64_t next = prev + 1;
      if (lock_.compare_exchange_weak(prev, next)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
    }
  }
}

void ConcurrentNode::RUnlock() {
  int retry = 0;
  while (true) {
    uint64_t prev = lock_;
    if (prev != HAS_WRITER && prev > 0) {
      uint64_t next = prev - 1;
      if (lock_.compare_exchange_weak(prev, next)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
    }
  }
}

void ConcurrentNode::Lock() {
  int retry = 0;
  while (true) {
    uint64_t prev = lock_;
    if (prev == 0) {
      if (lock_.compare_exchange_weak(prev, HAS_WRITER)) {
        return;
      }
    }

    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
    }
  }
}

void ConcurrentNode::Unlock() {
  int retry = 0;
  while (true) {
    uint64_t prev = lock_;
    if (prev == HAS_WRITER) {
      if (lock_.compare_exchange_weak(prev, 0)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
    }
  }
}

// TODO: need test
void ConcurrentNode::Downgrade() {
  // one reader
  lock_.store(1);
}

// TODO: need test
void ConcurrentNode::Upgrade() {
  int retry = 0;
  while (true) {
    uint64_t prev = lock_;
    // only one reader left
    if (prev == 1) {
      if (lock_.compare_exchange_weak(prev, HAS_WRITER)) {
        return;
      }
    }
    retry++;
    if (retry > RETRY_THRESHOLD) {
      retry = 0;
      std::this_thread::yield();
    }
  }
}

}  // namespace part