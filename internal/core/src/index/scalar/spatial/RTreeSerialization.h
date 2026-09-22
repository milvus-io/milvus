// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <ios>
#include <istream>
#include <new>
#include <streambuf>
#include <string>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

#include <boost/archive/archive_exception.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/serialization/split_free.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>

#include "common/EasyAssert.h"

// Boost-serialization of the R-tree structure. Was `RTreeIndexSerialization.h`.
//
// Two changes from the original, both deletions:
//
// 1. IT NOW LIVES IN `milvus::index`. The original declared `class
//    RTreeSerializer` in the GLOBAL namespace — a header in `index/` putting a
//    symbol at global scope.
//
// 2. FOUR OF THE SIX METHODS ARE GONE. `saveText` / `loadText` /
//    `serializeToString` / `deserializeFromString` had zero callers anywhere in
//    the repository; only `saveBinary` (RTreeIndexWrapper.cpp:188) and
//    `loadBinary` (:241) were ever used.
//
// Both operations either complete or throw a typed Milvus error. The baseline
// bool-return helpers swallowed archive failures, which could publish an empty
// loaded tree or a partial build as success.

namespace milvus::index {

namespace rtree_serialization_detail {

// Boost's binary primitive reads directly from streambuf::sgetn. A short read
// therefore does not necessarily update std::istream::bad(), so retain the
// underlying read(2) result explicitly to distinguish truncation from an OS
// read failure without materializing the archive.
class FileReadBuffer final : public std::streambuf {
 public:
    explicit FileReadBuffer(const std::string& filename)
        : fd_(::open(filename.c_str(), O_RDONLY | O_CLOEXEC)) {
        if (fd_ < 0) {
            ThrowInfo(FileOpenFailed,
                      "failed to open R-Tree archive {} for reading: {}",
                      filename,
                      std::strerror(errno));
        }
        setg(buffer_.data(), buffer_.data(), buffer_.data());
    }

    FileReadBuffer(const FileReadBuffer&) = delete;
    FileReadBuffer&
    operator=(const FileReadBuffer&) = delete;

    ~FileReadBuffer() override {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    int
    ReadError() const {
        return read_errno_;
    }

 protected:
    int_type
    underflow() override {
        if (gptr() < egptr()) {
            return traits_type::to_int_type(*gptr());
        }
        for (;;) {
            const auto count = ::read(fd_, buffer_.data(), buffer_.size());
            if (count > 0) {
                setg(buffer_.data(),
                     buffer_.data(),
                     buffer_.data() + static_cast<size_t>(count));
                return traits_type::to_int_type(*gptr());
            }
            if (count == 0) {
                return traits_type::eof();
            }
            if (errno == EINTR) {
                continue;
            }
            read_errno_ = errno;
            return traits_type::eof();
        }
    }

    std::streamsize
    xsgetn(char* destination, std::streamsize requested) override {
        std::streamsize copied = 0;
        while (copied < requested) {
            if (gptr() == egptr() &&
                traits_type::eq_int_type(underflow(), traits_type::eof())) {
                break;
            }
            const auto available =
                static_cast<std::streamsize>(egptr() - gptr());
            const auto count = std::min(available, requested - copied);
            std::memcpy(
                destination + copied, gptr(), static_cast<size_t>(count));
            gbump(static_cast<int>(count));
            copied += count;
        }
        return copied;
    }

 private:
    static constexpr size_t kBufferSize = 64 * 1024;
    std::array<char, kBufferSize> buffer_{};
    int fd_{-1};
    int read_errno_{0};
};

}  // namespace rtree_serialization_detail

class RTreeSerializer {
 public:
    template <typename RTreeType>
    static void
    saveBinary(const RTreeType& tree, const std::string& filename) {
        errno = 0;
        std::ofstream output(filename, std::ios::binary | std::ios::trunc);
        if (!output.is_open()) {
            ThrowInfo(FileOpenFailed,
                      "failed to open R-Tree archive {} for writing: {}",
                      filename,
                      std::strerror(errno));
        }
        try {
            boost::archive::binary_oarchive archive(output);
            archive << tree;
        } catch (const SegcoreError&) {
            throw;
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const boost::archive::archive_exception& error) {
            ThrowInfo(FileWriteFailed,
                      "failed to serialize R-Tree archive {}: {}",
                      filename,
                      error.what());
        } catch (const std::ios_base::failure& error) {
            ThrowInfo(FileWriteFailed,
                      "failed to write R-Tree archive {}: {}",
                      filename,
                      error.what());
        }
        output.flush();
        if (!output.good()) {
            ThrowInfo(
                FileWriteFailed, "failed to flush R-Tree archive {}", filename);
        }
        output.close();
        if (output.fail()) {
            ThrowInfo(
                FileWriteFailed, "failed to close R-Tree archive {}", filename);
        }
    }

    template <typename RTreeType>
    static void
    loadBinary(RTreeType& tree, const std::string& filename) {
        rtree_serialization_detail::FileReadBuffer input_buffer(filename);
        std::istream input(&input_buffer);
        RTreeType loaded;
        try {
            boost::archive::binary_iarchive archive(input);
            archive >> loaded;
        } catch (const SegcoreError&) {
            throw;
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const boost::archive::archive_exception& error) {
            if (input_buffer.ReadError() != 0) {
                ThrowInfo(FileReadFailed,
                          "failed while reading R-Tree archive {}: {}",
                          filename,
                          std::strerror(input_buffer.ReadError()));
            }
            ThrowInfo(DataFormatBroken,
                      "invalid R-Tree archive {}: {}",
                      filename,
                      error.what());
        } catch (const std::ios_base::failure& error) {
            ThrowInfo(FileReadFailed,
                      "failed while reading R-Tree archive {}: {}",
                      filename,
                      error.what());
        }
        if (input_buffer.ReadError() != 0) {
            ThrowInfo(FileReadFailed,
                      "failed while reading R-Tree archive {}: {}",
                      filename,
                      std::strerror(input_buffer.ReadError()));
        }
        tree = std::move(loaded);
    }
};

}  // namespace milvus::index
