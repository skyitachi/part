//
// Created by Shiping Yao on 2023/12/1.
//

#include <art_key.h>
#include <fmt/core.h>

namespace part {

ARTKey::ARTKey() : len(0) {}
ARTKey::ARTKey(const data_ptr_t &data, const uint32_t &len) : len(len), data(data) {}

ARTKey::ARTKey(ArenaAllocator &allocator, const uint32_t &len) : len(len) { data = allocator.Allocate(len); }

bool ARTKey::operator>(const ARTKey &k) const {
  for (uint32_t i = 0; i < std::min(len, k.len); i++) {
    printf("data pointer: %p, other data pointer: %p\n", data, k.data);
    if (data[i] > k.data[i]) {
      return true;
    } else if (data[i] < k.data[i]) {
      return false;
    }
  }
  return len > k.len;
}

bool ARTKey::operator>=(const part::ARTKey &k) const {
  for (uint32_t i = 0; i < std::min(len, k.len); i++) {
    if (data[i] > k.data[i]) {
      return true;
    } else if (data[i] < k.data[i]) {
      return false;
    }
  }
  return len >= k.len;
}

bool ARTKey::operator==(const ARTKey &k) const {
  if (len != k.len) {
    return false;
  }
  return std::memcmp(data, k.data, len) == 0;
}

bool ARTKey::operator<(const ARTKey &k) const { return k >= *this; }

// NOTE: 终于知道为什么要加0了
template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, std::string_view value) {
  uint32_t len = value.size() + 1;
  auto data = allocator.Allocate(len);
  std::memcpy(data, value.data(), len - 1);
  data[len - 1] = '\0';
  return ARTKey(data, len);
}

}  // namespace part
