//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_HELPER_H
#define PART_HELPER_H

namespace part {
template <class T, T val = 8>
static inline T AlignValue(T n) {
  return ((n + (val - 1)) / val) * val;
}
}  // namespace part
#endif  // PART_HELPER_H
