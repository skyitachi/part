//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_ART_KEY_H
#define PART_ART_KEY_H
#include "arena_allocator.h"
#include "types.h"

namespace part {

class ArenaAllocator;

class ARTKey {
public:
  ARTKey();
  ARTKey(const data_ptr_t &data, const uint32_t &len);
  ARTKey(ArenaAllocator &allocator, const uint32_t &len);

  uint32_t len;
  data_ptr_t data;

public:
  template <class T>
  static inline ARTKey CreateARTKey(ArenaAllocator &allocator, T element) {
    auto data = ARTKey::CreateData<T>(allocator, element);
    return ARTKey(data, sizeof(element));
  }

  data_t &operator[](size_t i) { return data[i]; }

  const data_t &operator[](size_t i) const { return data[i]; }

  bool operator>(const ARTKey &k) const;
  bool operator>=(const ARTKey &k) const;
  bool operator==(const ARTKey &k) const;

  inline bool Empty() const { return len == 0; }

  inline bool ByteMatches(const ARTKey &other, const uint32_t &depth) const {
    return data[depth] == other[depth];
  }

private:
  template <class T>
  static inline data_ptr_t CreateData(ArenaAllocator *allocator, T value) {
    auto data = allocator->Allocate(sizeof(value));
    return data;
  }
};

} // namespace part
#endif // PART_ART_KEY_H
