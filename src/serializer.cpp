//
// Created by skyitachi on 23-12-15.
//
#include "serializer.h"

namespace part {

void SequentialSerializer::WriteData(const_data_ptr_t buffer, idx_t write_size) {
  idx_t copy = write_size;
  idx_t data_offset = 0;
  //  fmt::println("[Debug.WriteData] {}, {}, write_size: {}", block_id_,
  //  offset_, copy);
  while (offset_ + copy > capacity_) {
    size_t to_write = capacity_ - offset_;
    copy -= to_write;
    auto written = ::write(fd_, buffer + data_offset, to_write);
    if (written != to_write) {
      throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
    }
    offset_ = 0;
    block_id_++;
    data_offset += to_write;
  }
  if (copy > 0) {
    auto written = ::write(fd_, buffer + data_offset, copy);
    if (written != copy) {
      throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
    }
    offset_ += copy;
  }
}

BlockPointer SequentialSerializer::GetBlockPointer() { return BlockPointer(block_id_, offset_); }

void SequentialDeserializer::ReadData(data_ptr_t buffer, idx_t read_size) {
  auto offset = block_id_ * BLOCK_SIZE + offset_;
  ssize_t r = pread(fd_, buffer, read_size, offset);
  if (r != read_size) {
    fmt::println("[Debug] ReadData block_id: {}, offset: {}", block_id_, offset_);
    throw std::invalid_argument(
        fmt::format("cannot read enough data: {}, expected: {}, read: {}", strerror(errno), read_size, r));
  }
  block_id_ += read_size / BLOCK_SIZE;
  auto remain = read_size % BLOCK_SIZE;
  if (offset_ + remain > BLOCK_SIZE) {
    offset_ = offset_ + remain - BLOCK_SIZE;
    block_id_ += 1;
  } else {
    offset_ += remain;
  }
}

BlockPointer SequentialDeserializer::GetBlockPointer() { return {static_cast<block_id_t>(block_id_), offset_}; }

}  // namespace part
