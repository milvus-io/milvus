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

#include "GrowingLOBManager.h"

#include <filesystem>

#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::segcore {

GrowingLOBManager::GrowingLOBManager(int64_t segment_id,
                                     const std::string& local_data_path)
    : segment_id_(segment_id) {
    // create directory structure: {local_data_path}/{segment_id}/lob/
    segment_dir_ = fmt::format("{}/{}", local_data_path, segment_id_);
    lob_dir_ = fmt::format("{}/lob", segment_dir_);

    std::filesystem::create_directories(lob_dir_);

    LOG_INFO("GrowingLOBManager created for segment {}: lob_dir={}",
             segment_id_,
             lob_dir_);
}

GrowingLOBManager::~GrowingLOBManager() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [field_id, state] : field_states_) {
            if (state && state->write_stream.is_open()) {
                state->write_stream.close();
            }
        }
    }

    auto stats = GetTotalStats();
    LOG_INFO(
        "GrowingLOBManager destroyed for segment {}: total_count={}, "
        "total_bytes={}MB, num_fields={}",
        segment_id_,
        stats.total_count,
        stats.total_bytes / (1024 * 1024),
        field_states_.size());

    try {
        if (std::filesystem::exists(segment_dir_)) {
            std::filesystem::remove_all(segment_dir_);
            LOG_DEBUG("Cleaned up segment directory: {}", segment_dir_);
        }
    } catch (const std::exception& e) {
        LOG_WARN("Failed to clean up segment directory {}: {}",
                 segment_dir_,
                 e.what());
    }
}

// get or create field state, caller must hold mutex_
FieldLOBState&
GrowingLOBManager::GetOrCreateFieldState(FieldId field_id) {
    auto it = field_states_.find(field_id.get());
    if (it != field_states_.end()) {
        return *it->second;
    }

    auto state = std::make_unique<FieldLOBState>();
    state->file_path = fmt::format("{}/{}", lob_dir_, field_id.get());

    state->write_stream.open(state->file_path,
                             std::ios::binary | std::ios::app | std::ios::out);
    AssertInfo(state->write_stream.is_open(),
               fmt::format("Failed to open LOB file: {}", state->file_path));

    state->write_stream.seekp(0, std::ios::end);
    state->current_offset = state->write_stream.tellp();

    LOG_DEBUG("Created LOB file for segment={}, field={}, path={}, offset={}",
              segment_id_,
              field_id.get(),
              state->file_path,
              state->current_offset.load());

    auto& ref = *state;
    field_states_[field_id.get()] = std::move(state);
    return ref;
}

// get field state, caller must hold mutex_
const FieldLOBState*
GrowingLOBManager::GetFieldState(FieldId field_id) const {
    auto it = field_states_.find(field_id.get());
    if (it != field_states_.end()) {
        return it->second.get();
    }
    return nullptr;
}

LOBReference
GrowingLOBManager::WriteLOB(FieldId field_id, std::string_view text_data) {
    uint32_t data_size = static_cast<uint32_t>(text_data.size());

    std::lock_guard<std::mutex> lock(mutex_);

    auto& state = GetOrCreateFieldState(field_id);

    uint64_t offset = state.current_offset.load();

    state.write_stream.write(text_data.data(), data_size);
    AssertInfo(
        state.write_stream.good(),
        fmt::format("Failed to write LOB data to file: {}", state.file_path));

    state.write_stream.flush();

    state.current_offset += data_size;

    state.total_bytes.fetch_add(data_size, std::memory_order_relaxed);
    state.total_count.fetch_add(1, std::memory_order_relaxed);

    LOG_DEBUG("WriteLOB: segment={}, field={}, offset={}, size={}KB",
              segment_id_,
              field_id.get(),
              offset,
              data_size / 1024);

    return LOBReference::ForGrowing(offset, data_size);
}

std::string
GrowingLOBManager::ReadLOB(FieldId field_id, const LOBReference& ref) const {
    if (!ref.IsLOBRef()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "ReadLOB called with non-LOB reference");
    }

    auto offset = ref.GetOffset();
    auto size = ref.GetSize();

    std::string file_path;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto* state = GetFieldState(field_id);
        AssertInfo(
            state != nullptr,
            fmt::format("Field {} not found in LOB manager", field_id.get()));
        file_path = state->file_path;
    }

    // open file for reading
    std::ifstream read_stream(file_path, std::ios::binary | std::ios::in);
    AssertInfo(
        read_stream.is_open(),
        fmt::format("Failed to open LOB file for reading: {}", file_path));

    // seek to offset
    read_stream.seekg(offset);
    AssertInfo(
        read_stream.good(),
        fmt::format(
            "Failed to seek to offset {} in LOB file: {}", offset, file_path));

    // read data
    std::string result;
    result.resize(size);
    read_stream.read(result.data(), size);
    AssertInfo(read_stream.good() || read_stream.eof(),
               fmt::format("Failed to read {} bytes from offset {} in LOB "
                           "file: {}",
                           size,
                           offset,
                           file_path));

    LOG_DEBUG("ReadLOB: segment={}, field={}, offset={}, size={}KB",
              segment_id_,
              field_id.get(),
              offset,
              size / 1024);

    return result;
}

LOBStats
GrowingLOBManager::GetTotalStats() const {
    LOBStats stats;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [field_id, state] : field_states_) {
        if (state) {
            stats.total_count +=
                state->total_count.load(std::memory_order_relaxed);
            stats.total_bytes +=
                state->total_bytes.load(std::memory_order_relaxed);
        }
    }
    return stats;
}

LOBStats
GrowingLOBManager::GetFieldStats(FieldId field_id) const {
    LOBStats stats;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto* state = GetFieldState(field_id);
    if (state) {
        stats.total_count = state->total_count.load(std::memory_order_relaxed);
        stats.total_bytes = state->total_bytes.load(std::memory_order_relaxed);
    }
    return stats;
}

std::string
GrowingLOBManager::GetLOBFilePath(FieldId field_id) const {
    return fmt::format("{}/{}", lob_dir_, field_id.get());
}

}  // namespace milvus::segcore
