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

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>

namespace milvus::segcore::test {

struct ReadCounts {
    std::atomic<int> opens{0};
    std::atomic<int> handles{0};
    std::atomic<int> footer_reads{0};
    std::atomic<int64_t> footer_bytes{0};
    std::atomic<int> data_reads{0};
    std::atomic<bool> fail_data_reads{false};
};

class CountedInput : public arrow::io::RandomAccessFile {
 public:
    CountedInput(std::shared_ptr<arrow::io::RandomAccessFile> input,
                 std::shared_ptr<ReadCounts> counts)
        : input_(std::move(input)), counts_(std::move(counts)) {
        ++counts_->handles;
        // Instrumentation-only reads bypass the counters. Count the reads
        // issued by storage/Arrow separately from this footer-boundary probe.
        auto size = input_->GetSize();
        if (size.ok() && size.ValueOrDie() >= 8) {
            auto tail = input_->ReadAt(size.ValueOrDie() - 8, 8);
            if (tail.ok() && tail.ValueOrDie()->size() == 8) {
                const auto* d = tail.ValueOrDie()->data();
                uint32_t footer_size = static_cast<uint32_t>(d[0]) |
                                       static_cast<uint32_t>(d[1]) << 8 |
                                       static_cast<uint32_t>(d[2]) << 16 |
                                       static_cast<uint32_t>(d[3]) << 24;
                footer_start_ = size.ValueOrDie() - 8 - footer_size;
            }
        }
    }
    ~CountedInput() override {
        (void)Close();
    }
    arrow::Status
    Close() override {
        if (!input_->closed()) {
            --counts_->handles;
        }
        return input_->Close();
    }
    bool
    closed() const override {
        return input_->closed();
    }
    arrow::Result<int64_t>
    Tell() const override {
        return input_->Tell();
    }
    arrow::Status
    Seek(int64_t pos) override {
        return input_->Seek(pos);
    }
    arrow::Result<int64_t>
    GetSize() override {
        return input_->GetSize();
    }
    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto pos, Tell());
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_->Read(nbytes, out);
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto pos, Tell());
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_->Read(nbytes);
    }
    arrow::Result<int64_t>
    ReadAt(int64_t pos, int64_t nbytes, void* out) override {
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_->ReadAt(pos, nbytes, out);
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t pos, int64_t nbytes) override {
        ARROW_RETURN_NOT_OK(Record(pos, nbytes));
        return input_->ReadAt(pos, nbytes);
    }

 private:
    arrow::Status
    Record(int64_t pos, int64_t nbytes) {
        if (pos + nbytes > footer_start_) {
            ++counts_->footer_reads;
            counts_->footer_bytes += nbytes;
        } else {
            ++counts_->data_reads;
            if (counts_->fail_data_reads) {
                return arrow::Status::IOError("injected data read failure");
            }
        }
        return arrow::Status::OK();
    }
    std::shared_ptr<arrow::io::RandomAccessFile> input_;
    std::shared_ptr<ReadCounts> counts_;
    int64_t footer_start_ = 0;
};

class CountedFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    explicit CountedFileSystem(std::shared_ptr<arrow::fs::FileSystem> base)
        : arrow::fs::SubTreeFileSystem("", std::move(base)) {
    }

    std::shared_ptr<ReadCounts>
    CountsFor(const std::string& path) {
        std::lock_guard lock(mutex_);
        auto& counts = counts_[path];
        if (!counts) {
            counts = std::make_shared<ReadCounts>();
        }
        return counts;
    }
    // Set only while no readers are being created. Allows deterministic open
    // failures and cancellation during metadata preparation.
    std::function<arrow::Status(const std::string&)> before_open;

    using arrow::fs::SubTreeFileSystem::OpenInputFile;
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        if (before_open) {
            ARROW_RETURN_NOT_OK(before_open(path));
        }
        ARROW_ASSIGN_OR_RAISE(
            auto file, arrow::fs::SubTreeFileSystem::OpenInputFile(path));
        auto counts = CountsFor(path);
        ++counts->opens;
        return std::make_shared<CountedInput>(std::move(file),
                                              std::move(counts));
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const arrow::fs::FileInfo& info) override {
        return OpenInputFile(info.path());
    }

 private:
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<ReadCounts>> counts_;
};

}  // namespace milvus::segcore::test
