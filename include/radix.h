//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_RADIX_H
#define PART_RADIX_H
#include <algorithm>
#include <cfloat>
#include <cmath>
#include <numeric>
#include <string>

#include "types.h"

namespace part {

class Radix {
 public:
  template <class T>
  static inline void EncodeData(data_ptr_t data_ptr, T value) {
    throw std::logic_error("Cannot create data from this type");
  }

  static inline void EncodeStringDataPrefix(data_ptr_t dataptr, const std::string &value, idx_t prefix_len) {
    auto len = static_cast<idx_t>(value.size());
    std::memcpy(dataptr, value.data(), std::min(len, prefix_len));
    if (len < prefix_len) {
      std::memset(dataptr + len, '\0', prefix_len - len);
    }
  }

  static inline uint8_t FlipSign(uint8_t key_byte) { return key_byte ^ 128; }

  static inline uint32_t EncodeFloat(float x) {
    uint64_t buff;

    //! zero
    if (x == 0) {
      buff = 0;
      buff |= (1u << 31);
      return buff;
    }
    // nan
    if (std::isnan(x)) {
      return UINT_MAX;
    }
    //! infinity
    if (x > FLT_MAX) {
      return UINT_MAX - 1;
    }
    //! -infinity
    if (x < -FLT_MAX) {
      return 0;
    }
    buff = Load<uint32_t>(const_data_ptr_cast(&x));
    if ((buff & (1u << 31)) == 0) {  //! +0 and positive numbers
      buff |= (1u << 31);
    } else {         //! negative numbers
      buff = ~buff;  //! complement 1
    }

    return buff;
  }

  static inline uint64_t EncodeDouble(double x) {
    uint64_t buff;
    //! zero
    if (x == 0) {
      buff = 0;
      buff += (1ull << 63);
      return buff;
    }
    // nan
    if (std::isnan(x)) {
      return ULLONG_MAX;
    }
    //! infinity
    if (x > DBL_MAX) {
      return ULLONG_MAX - 1;
    }
    //! -infinity
    if (x < -DBL_MAX) {
      return 0;
    }
    buff = Load<uint64_t>(const_data_ptr_cast(&x));
    if (buff < (1ull << 63)) {  //! +0 and positive numbers
      buff += (1ull << 63);
    } else {         //! negative numbers
      buff = ~buff;  //! complement 1
    }
    return buff;
  }
};

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, bool value) {
  Store<uint8_t>(value ? 1 : 0, dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int8_t value) {
  Store<uint8_t>(value, dataptr);
  dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int16_t value) {
  Store<uint16_t>(BSwap<uint16_t>(value), dataptr);
  dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, int64_t value) {
  Store<uint64_t>(BSwap<uint64_t>(value), dataptr);
  dataptr[0] = FlipSign(dataptr[0]);
}

template <>
inline void Radix::EncodeData(part::data_ptr_t data_ptr, int32_t value) {
  Store<uint32_t>(BSwap<uint32_t>(value), data_ptr);
  data_ptr[0] = FlipSign(data_ptr[0]);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint8_t value) {
  Store<uint8_t>(value, dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint16_t value) {
  Store<uint16_t>(BSwap<uint16_t>(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint32_t value) {
  Store<uint32_t>(BSwap<uint32_t>(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, uint64_t value) {
  Store<uint64_t>(BSwap<uint64_t>(value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t data_ptr, std::string value) {
  EncodeStringDataPrefix(data_ptr, value, value.size());
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, float value) {
  uint32_t converted_value = EncodeFloat(value);
  Store<uint32_t>(BSwap<uint32_t>(converted_value), dataptr);
}

template <>
inline void Radix::EncodeData(data_ptr_t dataptr, double value) {
  uint64_t converted_value = EncodeDouble(value);
  Store<uint64_t>(BSwap<uint64_t>(converted_value), dataptr);
}

}  // namespace part
#endif  // PART_RADIX_H
