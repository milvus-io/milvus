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

#include "folly/coro/BlockingWait.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LegacyIndexLoader.h"

#include <algorithm>
#include <any>
#include <charconv>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "folly/OperationCancelled.h"
#include "folly/coro/AsyncScope.h"
#include "milvus-storage/common/extend_status.h"
#include "monitor/Monitor.h"
#include "storage/DataCodec.h"
#include "storage/EntryStreamUtils.h"
#include "storage/Event.h"
#include "storage/LocalFileIOPool.h"
#include "storage/RemoteInputStream.h"
#include "storage/Util.h"

namespace milvus::storage {
namespace {

// A bounded raw range can coexist with a DIRECT I/O copy, including tail
// padding.
size_t
RawUnitScratchBytes(size_t bytes) {
    const auto aligned_bytes = (bytes + kStreamSliceAlignment - 1) /
                               kStreamSliceAlignment * kStreamSliceAlignment;
    return bytes + aligned_bytes;
}

// Preserves ChunkManager path semantics. Construction (Size) and reads run
// on LocalFileIOPool; native Arrow inputs use asynchronous range I/O.
class LegacyChunkInput final : public milvus::InputStream {
 public:
    LegacyChunkInput(ChunkManagerPtr manager, std::string path)
        : manager_(std::move(manager)),
          path_(std::move(path)),
          size_(manager_->Size(path_)) {
    }
    size_t
    Size() const override {
        return size_;
    }
    size_t
    Tell() const override {
        return position_;
    }
    bool
    Eof() const override {
        return position_ == size_;
    }
    bool
    Seek(int64_t offset) override {
        if (offset < 0 || static_cast<uint64_t>(offset) > size_) {
            return false;
        }
        position_ = offset;
        return true;
    }
    size_t
    ReadAt(void* data, size_t offset, size_t bytes) override {
        AssertInfo(offset <= size_ && bytes <= size_ - offset,
                   "Legacy index read exceeds object size");
        return manager_->Read(path_, offset, data, bytes);
    }
    size_t
    Read(void* data, size_t bytes) override {
        const auto read =
            ReadAt(data, position_, std::min(bytes, size_ - position_));
        position_ += read;
        return read;
    }
    size_t
    Read(int, size_t) override {
        ThrowInfo(Unsupported,
                  "LegacyChunkInput requires positioned memory reads");
    }

 private:
    ChunkManagerPtr manager_;
    std::string path_;
    size_t size_;
    size_t position_{0};
};

void
CheckFormat(bool valid, const char* message) {
    if (!valid) {
        ThrowInfo(DataFormatBroken, "Invalid legacy index: {}", message);
    }
}

// Metadata keeps the wire buffer, parsed descriptor and JSON scratch together.
// Reserve conservatively for the small descriptor while it is parsed.
size_t
MetadataScratchBytes(size_t bytes) {
    return SaturatingAdd(SaturatingMultiply(bytes, size_t{32}), size_t{4096});
}

folly::coro::Task<LoadAdmissionLease>
Admit(size_t bytes,
      proto::common::LoadPriority priority,
      folly::CancellationToken token) {
    try {
        co_return co_await LoadAdmissionController::GetInstance().AcquireAsync(
            {bytes, 1},
            priority == proto::common::LoadPriority::LOW
                ? LoadAdmissionPriority::Low
                : LoadAdmissionPriority::High,
            token);
    } catch (const folly::OperationCancelled&) {
        ThrowInfo(FollyCancel, "Legacy index admission cancelled");
    }
}

folly::coro::Task<void>
ReadRange(milvus::InputStream& input,
          size_t offset,
          uint8_t* data,
          size_t bytes,
          proto::common::LoadPriority priority,
          folly::CancellationToken token,
          bool use_async = true) {
    ThrowIfCancelled(token, "LegacyIndexLoader::Read");
    if (!use_async) {
        const auto read = input.ReadAt(data, offset, bytes);
        CheckFormat(read == bytes, "short object read");
    } else if (auto* remote = dynamic_cast<RemoteInputStream*>(&input)) {
        co_await ReadInputStreamExactlyAsync(
            *remote, offset, data, bytes, token);
    } else if (bytes != 0) {
        co_await RunLocalFileIOAsync(
            [&] {
                ThrowIfCancelled(token, "LegacyIndexLoader::Read");
                const auto read = input.ReadAt(data, offset, bytes);
                CheckFormat(read == bytes, "short object read");
            },
            priority);
    }
    ThrowIfCancelled(token, "LegacyIndexLoader::Read");
}

// Fill the same destination variants used by packed readers. The read/decode
// lease remains held until an awaited local write completes.
folly::coro::Task<void>
WriteTargetAsync(const EntryTarget& target,
                 size_t offset,
                 std::span<const uint8_t> bytes,
                 proto::common::LoadPriority priority,
                 folly::CancellationToken token) {
    const auto* disk = std::get_if<FileEntryTarget>(&target);
    ThrowIfCancelled(token, "LegacyIndexLoader::WriteTarget");
    const auto size = EntryTargetSize(target);
    AssertInfo(offset <= size && bytes.size() <= size - offset,
               "Legacy payload exceeds entry target");
    if (bytes.empty()) {
        co_return;
    }
    if (disk) {
        co_await RunLocalFileIOAsync(
            [&] {
                ThrowIfCancelled(token, "LegacyIndexLoader::WriteTarget");
                const auto start = std::chrono::steady_clock::now();
                disk->staging->WriteAt(
                    disk->offset + offset, bytes.data(), bytes.size());
                monitor::internal_storage_write_disk_duration.Observe(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count());
            },
            priority);
    } else {
        std::memcpy(std::get<MemoryEntryTarget>(target).data + offset,
                    bytes.data(),
                    bytes.size());
    }
}

// The caller owns admission until the read, decode and target write finish.
folly::coro::Task<void>
ReadLegacyUnit(milvus::InputStream& input,
               const LegacyIndexFileInfo& info,
               size_t offset,
               size_t bytes,
               const EntryTarget& target,
               proto::common::LoadPriority priority,
               folly::CancellationToken token,
               size_t base_offset = 0) {
    ThrowIfCancelled(token, "LegacyIndexLoader::ReadUnit");
    const auto start_read = std::chrono::steady_clock::now();
    // Observe each read/decode unit before consumption; concurrent disk writes
    // overlap downloads and cannot be subtracted from the load's wall time.
    const auto observe_read = [&] {
        monitor::internal_storage_download_duration.Observe(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_read)
                .count());
    };
    if (info.raw_payload) {
        auto buffer = std::make_unique_for_overwrite<uint8_t[]>(bytes);
        co_await ReadRange(input,
                           info.payload_offset + offset,
                           buffer.get(),
                           bytes,
                           priority,
                           token);
        observe_read();
        co_await WriteTargetAsync(target,
                                  base_offset + offset,
                                  {buffer.get(), bytes},
                                  priority,
                                  token);
    } else {
        auto buffer = std::shared_ptr<uint8_t[]>(new uint8_t[info.file_bytes]);
        co_await ReadRange(
            input, 0, buffer.get(), info.file_bytes, priority, token);
        auto codec = DeserializeFileData(
            buffer, info.file_bytes, true, std::nullopt, info.payload_bytes);
        ThrowIfCancelled(token, "LegacyIndexLoader::Decode");
        observe_read();
        co_await WriteTargetAsync(target,
                                  base_offset,
                                  {codec->PayloadData(), info.payload_bytes},
                                  priority,
                                  token);
    }
    ThrowIfCancelled(token, "LegacyIndexLoader::Consume");
}

// Reuse the existing envelope parser after validating its variable-size fields.
LegacyIndexFileInfo
ParseDescriptor(const std::shared_ptr<uint8_t[]>& data,
                size_t bytes,
                size_t file_bytes) {
    auto reader = std::make_shared<BinlogReader>(data, bytes);
    ReadMediumType(reader);
    DescriptorEvent descriptor(reader);
    const auto type =
        static_cast<DataType>(descriptor.event_data.fix_part.data_type);
    CheckFormat(type == DataType::NONE || type == DataType::INT8 ||
                    type == DataType::STRING,
                "unsupported index payload type");
    const auto& extras = descriptor.event_data.extras;
    CheckFormat(
        extras.contains(ORIGIN_SIZE_KEY) && extras.contains(INDEX_BUILD_ID_KEY),
        "missing payload size or build id");
    const auto& origin =
        std::any_cast<const std::string&>(extras.at(ORIGIN_SIZE_KEY));
    size_t payload_bytes = 0;
    const auto result = std::from_chars(
        origin.data(), origin.data() + origin.size(), payload_bytes);
    CheckFormat(
        result.ec == std::errc{} && result.ptr == origin.data() + origin.size(),
        "invalid decoded payload size");
    CheckFormat(payload_bytes <=
                    static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                "decoded payload size exceeds int64");
    // The existing decoder uses stol for this field. Validate its input here
    // so malformed persisted metadata cannot escape as an untyped exception.
    const auto& build_id_text =
        std::any_cast<const std::string&>(extras.at(INDEX_BUILD_ID_KEY));
    int64_t build_id = 0;
    const auto build_id_result =
        std::from_chars(build_id_text.data(),
                        build_id_text.data() + build_id_text.size(),
                        build_id);
    CheckFormat(
        build_id_result.ec == std::errc{} &&
            build_id_result.ptr == build_id_text.data() + build_id_text.size(),
        "invalid index build id");
    const bool encrypted = !descriptor.GetEdekFromExtra().empty();
    if (encrypted) {
        CheckFormat(descriptor.GetEZFromExtra() != -1,
                    "missing encryption zone");
    }
    LegacyIndexFileInfo info;
    info.file_bytes = file_bytes;
    info.payload_bytes = payload_bytes;
    info.payload_offset = bytes;
    info.raw_payload = type == DataType::NONE && !encrypted;
    info.max_transient_bytes = MetadataScratchBytes(bytes);
    if (!info.raw_payload) {
        // Wire/ciphertext/plaintext may overlap. Parquet INT8 also uses INT32
        // page/dictionary/conversion buffers before filling byte FieldData;
        // old STRING decoding copies the string into byte FieldData.
        const size_t decoded_buffers = type == DataType::INT8     ? 12
                                       : type == DataType::STRING ? 4
                                                                  : 0;
        info.max_transient_bytes = SaturatingAdd(
            info.max_transient_bytes,
            SaturatingAdd(SaturatingMultiply(file_bytes, size_t{4}),
                          SaturatingMultiply(payload_bytes, decoded_buffers)));
    }
    return info;
}

}  // namespace

size_t
LegacyIndexFileInfo::TotalTransientBytes(bool use_async_load) const {
    if (!raw_payload) {
        return max_transient_bytes;
    }
    if (!use_async_load) {
        // GetObjectData reads one complete object into a single buffer.
        return file_bytes;
    }
    // Full raw slices are aligned; only the last slice needs padding. Count
    // every read buffer and its possible DIRECT I/O copy, without a local cap.
    const auto padding =
        (kStreamSliceAlignment - payload_bytes % kStreamSliceAlignment) %
        kStreamSliceAlignment;
    return std::max(
        max_transient_bytes,
        SaturatingAdd(SaturatingMultiply(payload_bytes, size_t{2}), padding));
}

size_t
LegacyIndexLoadTransientBytes(size_t total_bytes,
                              size_t max_file_bytes,
                              bool use_async_load) {
    if (use_async_load) {
        return total_bytes;
    }
    // Match the old loaders' batch count. indexSliceSize is a startup setting;
    // unlike async admission limits, it does not change on cached reloads.
    const auto files_per_batch = DEFAULT_FIELD_MAX_MEMORY_LIMIT /
                                 std::max<size_t>(1, FILE_SLICE_SIZE.load());
    const auto batch_bytes =
        files_per_batch == 0
            ? total_bytes
            : std::min(total_bytes,
                       SaturatingMultiply(max_file_bytes, files_per_batch));
    return std::max<size_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT, batch_bytes);
}

folly::coro::Task<std::shared_ptr<milvus::InputStream>>
OpenLegacyIndexInputAsync(const ChunkManagerPtr& chunk_manager,
                          const milvus_storage::ArrowFileSystemPtr& fs,
                          const std::string& remote_file,
                          proto::common::LoadPriority priority) {
    const auto token = co_await folly::coro::co_current_cancellation_token;
    ThrowIfCancelled(token, "LegacyIndexLoader::Open");
    // Legacy local uploads use ChunkManager paths directly, whereas Arrow's
    // local filesystem is rooted and would prepend its root a second time.
    if (!fs || milvus_storage::IsLocalFileSystem(fs)) {
        AssertInfo(chunk_manager != nullptr,
                   "Legacy index requires a file source");
        std::shared_ptr<milvus::InputStream> input;
        co_await RunLocalFileIOAsync(
            [&] {
                input = std::make_shared<LegacyChunkInput>(chunk_manager,
                                                           remote_file);
            },
            priority);
        ThrowIfCancelled(token, "LegacyIndexLoader::Open");
        co_return input;
    }
    auto input = co_await RemoteInputStream::OpenAsync(fs, remote_file);
    ThrowIfCancelled(token, "LegacyIndexLoader::Open");
    co_return input;
}

// Share envelope validation while sync callers read ranges inline. Neither
// path materializes payloads merely to determine their decoded size.
static folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileImpl(milvus::InputStream& input,
                           proto::common::LoadPriority priority,
                           folly::CancellationToken token,
                           bool use_async) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "LegacyIndexLoader::Inspect");
    size_t file_bytes;
    if (!use_async || dynamic_cast<RemoteInputStream*>(&input)) {
        file_bytes = input.Size();
    } else {
        co_await RunLocalFileIOAsync([&] { file_bytes = input.Size(); },
                                     priority);
    }
    ThrowIfCancelled(token, "LegacyIndexLoader::InspectSize");
    CheckFormat(
        file_bytes <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "file size exceeds int64");
    EventHeader header;
    const size_t header_bytes = GetEventHeaderSize(header);
    const size_t prefix_bytes = sizeof(MAGIC_NUM) + header_bytes;
    CheckFormat(file_bytes >= prefix_bytes, "truncated descriptor header");
    size_t descriptor_bytes;
    {
        auto lease =
            co_await Admit(MetadataScratchBytes(prefix_bytes), priority, token);
        auto prefix = std::shared_ptr<uint8_t[]>(new uint8_t[prefix_bytes]);
        co_await ReadRange(
            input, 0, prefix.get(), prefix_bytes, priority, token, use_async);
        int32_t magic;
        std::memcpy(&magic, prefix.get(), sizeof(magic));
        CheckFormat(magic == MAGIC_NUM, "invalid magic number");
        auto reader = std::make_shared<BinlogReader>(prefix, prefix_bytes);
        ReadMediumType(reader);
        header = EventHeader(reader);
        const size_t fixed_bytes =
            header_bytes + GetEventFixPartSize(EventType::DescriptorEvent) +
            static_cast<size_t>(EventType::EventTypeEnd) + sizeof(int32_t);
        CheckFormat(
            header.event_type_ == EventType::DescriptorEvent &&
                header.event_length_ >= 0 &&
                static_cast<size_t>(header.event_length_) >= fixed_bytes &&
                header.event_length_ <= file_bytes - sizeof(MAGIC_NUM),
            "invalid descriptor length or type");
        descriptor_bytes = sizeof(MAGIC_NUM) + header.event_length_;
        CheckFormat(header.next_position_ == descriptor_bytes,
                    "invalid descriptor next position");
    }
    LegacyIndexFileInfo info;
    {
        auto lease = co_await Admit(
            MetadataScratchBytes(descriptor_bytes), priority, token);
        auto descriptor =
            std::shared_ptr<uint8_t[]>(new uint8_t[descriptor_bytes]);
        co_await ReadRange(input,
                           0,
                           descriptor.get(),
                           descriptor_bytes,
                           priority,
                           token,
                           use_async);
        const size_t extra_offset =
            sizeof(MAGIC_NUM) + header_bytes +
            GetEventFixPartSize(EventType::DescriptorEvent) +
            static_cast<size_t>(EventType::EventTypeEnd);
        int32_t extra_bytes;
        std::memcpy(
            &extra_bytes, descriptor.get() + extra_offset, sizeof(extra_bytes));
        CheckFormat(extra_bytes >= 0 && static_cast<size_t>(extra_bytes) ==
                                            descriptor_bytes - extra_offset -
                                                sizeof(extra_bytes),
                    "invalid descriptor extra length");
        try {
            info = ParseDescriptor(descriptor, descriptor_bytes, file_bytes);
        } catch (const nlohmann::json::exception& e) {
            ThrowInfo(DataFormatBroken,
                      "Invalid legacy descriptor JSON: {}",
                      e.what());
        }
    }
    if (info.raw_payload) {
        const size_t event_prefix = header_bytes + 2 * sizeof(Timestamp);
        CheckFormat(event_prefix <= file_bytes - descriptor_bytes,
                    "truncated index event");
        auto lease =
            co_await Admit(MetadataScratchBytes(event_prefix), priority, token);
        auto prefix = std::shared_ptr<uint8_t[]>(new uint8_t[event_prefix]);
        co_await ReadRange(input,
                           descriptor_bytes,
                           prefix.get(),
                           event_prefix,
                           priority,
                           token,
                           use_async);
        auto reader = std::make_shared<BinlogReader>(prefix, event_prefix);
        const auto event = EventHeader(reader);
        CheckFormat(
            event.event_type_ == EventType::IndexFileEvent &&
                event.event_length_ >= 0 &&
                static_cast<size_t>(event.event_length_) >= event_prefix &&
                static_cast<size_t>(event.event_length_) ==
                    file_bytes - descriptor_bytes,
            "invalid raw index event length or type");
        info.payload_offset += event_prefix;
        CheckFormat(info.payload_bytes == file_bytes - info.payload_offset,
                    "raw payload length mismatch");
        info.max_transient_bytes =
            std::max(info.max_transient_bytes,
                     RawUnitScratchBytes(std::min(info.payload_bytes,
                                                  DefaultStreamSliceSize())));
    }
    co_return info;
}

LegacyIndexFileInfo
InspectLegacyIndexFile(const ChunkManagerPtr& chunk_manager,
                       const std::string& path,
                       proto::common::LoadPriority priority,
                       folly::CancellationToken token) {
    ThrowIfCancelled(token, "LegacyIndexLoader::Inspect");
    LegacyChunkInput input(chunk_manager, path);
    return folly::coro::blockingWait(
        InspectLegacyIndexFileImpl(input, priority, std::move(token), false));
}

folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileAsync(milvus::InputStream& input,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token) {
    return InspectLegacyIndexFileImpl(input, priority, std::move(token), true);
}

folly::coro::Task<void>
StreamLegacyIndexFileAsync(milvus::InputStream& input,
                           const LegacyIndexFileInfo& info,
                           const EntryTarget& target,
                           proto::common::LoadPriority priority,
                           folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "LegacyIndexLoader::Stream");
    if (info.raw_payload) {
        CheckFormat(
            info.payload_offset <= info.file_bytes &&
                info.payload_bytes == info.file_bytes - info.payload_offset,
            "invalid raw payload range");
        size_t offset = 0;
        do {
            const auto bytes =
                std::min(DefaultStreamSliceSize(), info.payload_bytes - offset);
            auto lease =
                co_await Admit(RawUnitScratchBytes(bytes), priority, token);
            co_await ReadLegacyUnit(
                input, info, offset, bytes, target, priority, token);
            offset += bytes;
        } while (offset < info.payload_bytes);
    } else {
        auto lease = co_await Admit(info.max_transient_bytes, priority, token);
        co_await ReadLegacyUnit(
            input, info, 0, info.payload_bytes, target, priority, token);
    }
}

folly::coro::Task<void>
StreamLegacyIndexFilesAsync(std::span<const LegacyIndexFile> files,
                            const ChunkManagerPtr& chunk_manager,
                            const milvus_storage::ArrowFileSystemPtr& fs,
                            const EntryTarget& target,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "LegacyIndexLoader::StreamFiles");
    if (files.empty()) {
        co_return;
    }
    auto executor = ResolveAsyncLoadExecutor({}, priority);
    if (files.size() == 1 &&
        (!files.front().info.raw_payload ||
         files.front().info.payload_bytes <= DefaultStreamSliceSize())) {
        // Small unsliced entries and metadata need no fan-out machinery.
        auto input = co_await OpenLegacyIndexInputAsync(
            chunk_manager, fs, files.front().path, priority);
        co_await folly::coro::co_withExecutor(
            executor.copy(),
            StreamLegacyIndexFileAsync(
                *input, files.front().info, target, priority, token));
        co_return;
    }
    folly::CancellationSource failed;
    const auto effective_token =
        folly::cancellation_token_merge(token, failed.getToken());
    std::mutex failure_mutex;
    std::exception_ptr first_error;
    auto record_failure = [&](std::exception_ptr error) {
        {
            std::lock_guard lock(failure_mutex);
            if (!first_error) {
                first_error = std::move(error);
            }
        }
        failed.requestCancellation();
    };

    folly::coro::AsyncScope scope;
    struct Unit {
        std::shared_ptr<milvus::InputStream> input;
        LegacyIndexFileInfo info;
        size_t offset;
        size_t bytes;
        size_t base_offset;
    };
    auto run_unit = [&](Unit unit,
                        LoadAdmissionLease lease) -> folly::coro::Task<void> {
        try {
            co_await ReadLegacyUnit(*unit.input,
                                    unit.info,
                                    unit.offset,
                                    unit.bytes,
                                    target,
                                    priority,
                                    effective_token,
                                    unit.base_offset);
        } catch (...) {
            // Publish failure before leases are returned and wake admissions.
            record_failure(std::current_exception());
        }
        // Read/decode buffers and borrowed local writes have already drained.
        lease.Release();
    };

    size_t base_offset = 0;
    try {
        for (const auto& file : files) {
            ThrowIfCancelled(effective_token, "LegacyIndexLoader::Open");
            const auto& info = file.info;
            CheckFormat(info.payload_bytes <=
                            std::numeric_limits<size_t>::max() - base_offset,
                        "concatenated payload size overflow");
            if (info.raw_payload) {
                CheckFormat(info.payload_offset <= info.file_bytes &&
                                info.payload_bytes ==
                                    info.file_bytes - info.payload_offset,
                            "invalid raw payload range");
            }
            auto input = co_await OpenLegacyIndexInputAsync(
                chunk_manager, fs, file.path, priority);
            size_t offset = 0;
            do {
                const auto bytes = info.raw_payload
                                       ? std::min(DefaultStreamSliceSize(),
                                                  info.payload_bytes - offset)
                                       : info.payload_bytes;
                const auto charge = info.raw_payload
                                        ? RawUnitScratchBytes(bytes)
                                        : info.max_transient_bytes;
                // Admit before dispatch; completed slices immediately make
                // room for new work regardless of destination order.
                auto lease = co_await Admit(charge, priority, effective_token);
                ThrowIfCancelled(effective_token,
                                 "LegacyIndexLoader::Dispatch");
                scope.add(folly::coro::co_withCancellation(
                    folly::CancellationToken{},
                    folly::coro::co_withExecutor(
                        executor.copy(),
                        run_unit(Unit{input, info, offset, bytes, base_offset},
                                 std::move(lease)))));
                offset += bytes;
            } while (offset < info.payload_bytes);
            base_offset += info.payload_bytes;
        }
    } catch (...) {
        record_failure(std::current_exception());
    }
    co_await folly::coro::co_withCancellation(folly::CancellationToken{},
                                              scope.joinAsync());
    if (first_error) {
        std::rethrow_exception(first_error);
    }
    ThrowIfCancelled(token, "LegacyIndexLoader::StreamFilesComplete");
}

folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileAsync(
    const std::string& path,
    const ChunkManagerPtr& chunk_manager,
    const milvus_storage::ArrowFileSystemPtr& fs,
    const std::shared_ptr<const LegacyIndexFileInfos>& infos,
    proto::common::LoadPriority priority,
    folly::CancellationToken token) {
    ThrowIfCancelled(token, "LegacyIndexLoader::Inspect");
    if (infos) {
        if (auto it = infos->find(path); it != infos->end()) {
            co_return it->second;
        }
    }
    auto input =
        co_await OpenLegacyIndexInputAsync(chunk_manager, fs, path, priority);
    co_return co_await InspectLegacyIndexFileAsync(*input, priority, token);
}

folly::coro::Task<std::shared_ptr<const LegacyIndexFileInfos>>
InspectLegacyIndexFilesAsync(std::span<const std::string> files,
                             const ChunkManagerPtr& chunk_manager,
                             const milvus_storage::ArrowFileSystemPtr& fs,
                             proto::common::LoadPriority priority,
                             folly::CancellationToken token) {
    auto infos = std::make_shared<LegacyIndexFileInfos>();
    infos->reserve(files.size());
    for (const auto& path : files) {
        ThrowIfCancelled(token, "LegacyIndexLoader::InspectFiles");
        if (!infos->contains(path)) {
            auto input = co_await OpenLegacyIndexInputAsync(
                chunk_manager, fs, path, priority);
            infos->emplace(
                path,
                co_await InspectLegacyIndexFileAsync(*input, priority, token));
        }
    }
    co_return infos;
}

folly::coro::Task<void>
ReadLegacyIndexFilesAsync(std::span<const LegacyIndexFile> files,
                          const ChunkManagerPtr& chunk_manager,
                          const milvus_storage::ArrowFileSystemPtr& fs,
                          const EntryTarget& target,
                          proto::common::LoadPriority priority,
                          folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    const auto* disk = std::get_if<FileEntryTarget>(&target);
    std::exception_ptr failure;
    try {
        if (disk) {
            co_await RunLocalFileIOAsync(
                [&] {
                    ThrowIfCancelled(token, "LegacyIndexLoader::PrepareTarget");
                    disk->staging->Prepare(
                        io::GetPriorityFromLoadPriority(priority));
                },
                priority);
        }
        co_await StreamLegacyIndexFilesAsync(
            files, chunk_manager, fs, target, priority, token);
        if (disk) {
            co_await RunLocalFileIOAsync(
                [&] {
                    const auto start = std::chrono::steady_clock::now();
                    ThrowIfCancelled(token, "LegacyIndexLoader::FinishTarget");
                    disk->staging->Finish();
                    monitor::internal_storage_write_disk_duration.Observe(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count());
                },
                priority);
        }
        ThrowIfCancelled(token, "LegacyIndexLoader::ReadFilesComplete");
    } catch (...) {
        failure = std::current_exception();
    }
    if (failure) {
        if (disk) {
            co_await RunLocalFileIOAsync([&] { disk->staging->Cleanup(); },
                                         priority);
        }
        std::rethrow_exception(failure);
    }
}

}  // namespace milvus::storage
