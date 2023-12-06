//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_RADIX_H
#define PART_RADIX_H
#include <algorithm>
#include <numeric>
#include <string>

#include "types.h"

namespace part {

class Radix {
public:
  template <class T>
  static inline void EncodeData(data_ptr_t data_ptr, T value) {
    // not implement
  }

  static inline void EncodeStringDataPrefix(data_ptr_t dataptr,
                                            const std::string &value,
                                            idx_t prefix_len) {
    auto len = static_cast<idx_t>(value.size());
    std::memcpy(dataptr, value.data(), std::min(len, prefix_len));
    if (len < prefix_len) {
      std::memset(dataptr + len, '\0', prefix_len - len);
    }
  }

  static inline uint8_t FlipSign(uint8_t key_byte) { return key_byte ^ 128; }
};

template <> inline void Radix::EncodeData(data_ptr_t dataptr, int64_t value) {
  Store<uint64_t>(BSwap<uint64_t>(value), dataptr);
  dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t data_ptr, std::string value) {
  EncodeStringDataPrefix(data_ptr, value, value.size());
}
} // namespace part
#endif // PART_RADIX_H
