//
// Created by skyitachi on 23-12-15.
//
#include "serializer.h"

namespace part {

void SequentialSerializer::WriteData(const_data_ptr_t buffer, idx_t write_size) {
  idx_t copy = write_size;
  idx_t data_offset = 0;
  while (offset_ + copy > capacity_) {
    size_t to_write = capacity_ - offset_;
    copy -= to_write;
    auto written = write(buffer + data_offset, to_write);
    if (written != to_write) {
      throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
    }
    offset_ = 0;
    block_id_++;
    data_offset += to_write;
  }
  if (copy > 0) {
    auto written = write(buffer + data_offset, copy);
    if (written != copy) {
      throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
    }
    offset_ += copy;
  }
}

idx_t SequentialSerializer::write(const_data_ptr_t data, idx_t size) {
  idx_t offset = 0;
  while (buf_offset_ + size >= capacity_) {
    if (buf_offset_ > 0) {
      idx_t single = capacity_ - buf_offset_;
      if (single > 0) {
        std::memcpy(buffer_ + buf_offset_, data + offset, single);
      }
      auto written = ::write(fd_, buffer_, capacity_);
      if (written != capacity_) {
        throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
      }
      buf_offset_ = 0;
      size -= single;
      offset += single;
    } else {
      auto written = ::write(fd_, data + offset, capacity_);
      if (written != capacity_) {
        throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
      }
      size -= capacity_;
      offset += capacity_;
    }
  }
  if (size > 0) {
    std::memcpy(buffer_ + buf_offset_, data + offset, size);
    buf_offset_ += size;
    offset += size;
  }
  return offset;
}

void SequentialSerializer::Flush() {
  if (buf_offset_ > 0) {
    auto written = ::write(fd_, buffer_, buf_offset_);
    if (written != buf_offset_) {
      throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
    }
    buf_offset_ = 0;
  }
}

BlockPointer SequentialSerializer::GetBlockPointer() { return BlockPointer(block_id_, offset_); }

void BlockDeserializer::ReadData(data_ptr_t buffer, idx_t read_size) {
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

BlockPointer BlockDeserializer::GetBlockPointer() { return {static_cast<block_id_t>(block_id_), offset_}; }

}  // namespace part
