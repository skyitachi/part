//
// Created by skyitachi on 23-12-15.
//
#include "serializer.h"

namespace part {

void SequentialSerializer::WriteData(const_data_ptr_t buffer, idx_t write_size) {
    idx_t copy = write_size;
    while (offset_ + copy > capacity_) {
        size_t to_write = capacity_ - offset_;
        copy -= to_write;
        auto written = ::write(fd_, buffer, to_write);
        if (written != to_write) {
            throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
        }
        offset_ = 0;
        block_id_++;
    }
    if (copy > 0) {
        auto written = ::write(fd_, buffer, copy);
        if (written != copy) {
            throw std::invalid_argument(fmt::format("cannot write enough data to disk"));
        }
        offset_ += copy;
    }
}

BlockPointer SequentialSerializer::GetBlockPointer() {
    return BlockPointer(block_id_, offset_);
}

}
