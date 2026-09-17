// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <unistd.h>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "storage/FileWriter.h"

namespace milvus::storage {

// Owns a staging writer and removes its file unless the load commits it.
// All slice writes must drain before Finish(), Commit(), or destruction.
class StagingIndexFile final {
 public:
    static std::shared_ptr<StagingIndexFile>
    Create(std::string path,
           size_t file_size,
           io::Priority priority = io::Priority::MIDDLE) {
        AssertInfo(
            file_size <= static_cast<size_t>(std::numeric_limits<off_t>::max()),
            "Staging file '{}' size {} exceeds off_t range",
            path,
            file_size);
        auto target = std::shared_ptr<StagingIndexFile>(
            new StagingIndexFile(std::move(path)));
        target->writer_ = std::make_unique<PositionedFileWriter>(
            target->path_, file_size, priority);
        return target;
    }

    StagingIndexFile(const StagingIndexFile&) = delete;
    StagingIndexFile&
    operator=(const StagingIndexFile&) = delete;

    ~StagingIndexFile() {
        const bool owns_file = writer_ != nullptr || finished_;
        writer_.reset();
        if (owns_file && !committed_) {
            ::unlink(path_.c_str());
        }
    }

    void
    WriteAt(size_t offset, const void* data, size_t bytes) {
        AssertInfo(
            writer_ != nullptr, "Staging file '{}' is already finished", path_);
        writer_->WriteAt(offset, data, bytes);
    }

    void
    Finish() {
        if (finished_) {
            return;
        }
        writer_->Finish();
        writer_.reset();
        finished_ = true;
    }

    void
    Commit() {
        AssertInfo(finished_,
                   "Staging file '{}' must be finished before commit",
                   path_);
        committed_ = true;
    }

    bool
    Committed() const noexcept {
        return committed_;
    }

 private:
    explicit StagingIndexFile(std::string path) : path_(std::move(path)) {
    }
    std::string path_;
    std::unique_ptr<PositionedFileWriter> writer_;
    bool finished_{false};
    bool committed_{false};
};

}  // namespace milvus::storage
