//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_TYPES_H
#define PART_TYPES_H

#include <cinttypes>
#include <memory>
#include <cstring>

namespace part {

using idx_t = uint64_t;
using data_t = uint8_t;
using data_ptr_t = data_t *;
using const_data_ptr_t = const data_t *;
using validity_t = uint64_t;

using block_id_t = int64_t;

#define INVALID_BLOCK (-1)

// maximum block id, 2^62
#define MAXIMUM_BLOCK 4611686018427388000LL

static constexpr const idx_t INVALID_INDEX = idx_t(-1);

template <class SRC> data_ptr_t data_ptr_cast(SRC *src) {
  return reinterpret_cast<data_ptr_t>(src);
}

#ifndef STANDARD_VECTOR_SIZE
#define STANDARD_VECTOR_SIZE 2048
#endif

#define BSWAP16(x)                                                             \
  ((uint16_t)((((uint16_t)(x)&0xff00) >> 8) | (((uint16_t)(x)&0x00ff) << 8)))

#define BSWAP32(x)                                                             \
  ((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) |                             \
              (((uint32_t)(x)&0x00ff0000) >> 8) |                              \
              (((uint32_t)(x)&0x0000ff00) << 8) |                              \
              (((uint32_t)(x)&0x000000ff) << 24)))

#define BSWAP64(x)                                                             \
  ((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) |                  \
              (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |                  \
              (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) |                  \
              (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |                   \
              (((uint64_t)(x)&0x00000000ff000000ull) << 8) |                   \
              (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |                  \
              (((uint64_t)(x)&0x000000000000ff00ull) << 40) |                  \
              (((uint64_t)(x)&0x00000000000000ffull) << 56)))

template <class T> static inline T BSwap(const T &x) {
  static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                    sizeof(T) == 8,
                "Size of type must be 1, 2, 4, or 8 for BSwap");
  if (sizeof(T) == 1) {
    return x;
  } else if (sizeof(T) == 2) {
    return BSWAP16(x);
  } else if (sizeof(T) == 4) {
    return BSWAP32(x);
  } else {
    return BSWAP64(x);
  }
}

template <typename T> void Store(const T &val, data_ptr_t ptr) {
  std::memcpy(ptr, (void *)&val, sizeof(val));
}

} // namespace part
#endif // PART_TYPES_H
