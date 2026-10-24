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

#include "storage/IndexEntryTarget.h"

#include <limits>
#include <unistd.h>

namespace milvus::storage {

IndexFileTarget::IndexFileTarget(
    std::string path,
    size_t file_size,
    bool retain_on_success,
    std::optional<FileWriter::WriteMode> write_mode)
    : path(std::move(path)),
      file_size(file_size),
      retain_on_success(retain_on_success),
      write_mode(write_mode) {
}

IndexFileTarget::~IndexFileTarget() {
    Cleanup();
}

void
IndexFileTarget::Prepare(io::Priority priority) {
    if (Prepared()) {
        return;
    }
    AssertInfo(
        file_size <= static_cast<size_t>(std::numeric_limits<off_t>::max()),
        "Staging file '{}' size {} exceeds off_t range",
        path,
        file_size);
    writer_ = std::make_unique<PositionedFileWriter>(
        path, file_size, priority, write_mode);
}

void
IndexFileTarget::WriteAt(size_t offset, const void* data, size_t bytes) {
    AssertInfo(writer_ != nullptr, "Staging file '{}' is not open", path);
    writer_->WriteAt(offset, data, bytes);
}

void
IndexFileTarget::Finish() {
    if (finished_) {
        return;
    }
    AssertInfo(writer_ != nullptr, "Staging file '{}' is not open", path);
    writer_->Finish();
    writer_.reset();
    finished_ = true;
}

void
IndexFileTarget::Commit() {
    AssertInfo(
        finished_, "Staging file '{}' must be finished before commit", path);
    committed_ = retain_on_success;
}

void
IndexFileTarget::Cleanup() noexcept {
    if (committed_) {
        return;
    }
    const bool owns_file = Prepared();
    writer_.reset();
    finished_ = false;
    if (owns_file) {
        ::unlink(path.c_str());
    }
}

}  // namespace milvus::storage
