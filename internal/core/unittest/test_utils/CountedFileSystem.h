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
#include <memory>
#include <string>

#include <arrow/buffer.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>

// Counts opens and read ranges for every handle served by a wrapped
// filesystem. A read whose range intersects the parquet footer region counts
// as a footer read; everything else counts as a data read. The footer region
// is recovered from the trailing 8 bytes of the file, so no configuration is
// needed and each file is judged independently.
namespace milvus::segcore {

struct ReadCounts {
    std::atomic<int> opens{0};
    std::atomic<int> handles{0};
    std::atomic<int> footer_reads{0};
    std::atomic<int> data_reads{0};
};

// Wraps an underlying RandomAccessFile and classifies every read as a footer
// read or a data read based on the file's trailing parquet footer layout.
class CountedInput : public arrow::io::RandomAccessFile {
 public:
    CountedInput(std::shared_ptr<arrow::io::RandomAccessFile> input,
                 std::shared_ptr<ReadCounts> counts)
        : input_(std::move(input)), counts_(std::move(counts)) {
        ++counts_->handles;
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
        (void)CountedInput::Close();
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
    Seek(int64_t position) override {
        return input_->Seek(position);
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
        } else {
            ++counts_->data_reads;
        }
        return arrow::Status::OK();
    }
    std::shared_ptr<arrow::io::RandomAccessFile> input_;
    std::shared_ptr<ReadCounts> counts_;
    int64_t footer_start_ = 0;
};

// Serves a wrapped filesystem but hands out a fresh counted handle for every
// OpenInputFile call, so footer/data reads can be attributed to each reader.
class CountedFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    CountedFileSystem(std::shared_ptr<arrow::fs::FileSystem> base,
                      std::shared_ptr<ReadCounts> counts)
        : arrow::fs::SubTreeFileSystem("", std::move(base)),
          counts(std::move(counts)) {
    }
    using arrow::fs::SubTreeFileSystem::OpenInputFile;
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ARROW_ASSIGN_OR_RAISE(
            auto file, arrow::fs::SubTreeFileSystem::OpenInputFile(path));
        ++counts->opens;
        return std::make_shared<CountedInput>(std::move(file), counts);
    }
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const arrow::fs::FileInfo& info) override {
        return OpenInputFile(info.path());
    }
    std::shared_ptr<ReadCounts> counts;

 private:
};

}  // namespace milvus::segcore
