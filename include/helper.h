//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_HELPER_H
#define PART_HELPER_H
#include <string>

namespace part {
template <class T, T val = 8>
static inline T AlignValue(T n) {
  return ((n + (val - 1)) / val) * val;
}

std::string GetStackTrace(int max_depth = 120);

void PartAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);

#define P_ASSERT(condition) part::PartAssertInternal(bool(condition), #condition, __FILE__, __LINE__)

}  // namespace part
#endif  // PART_HELPER_H
