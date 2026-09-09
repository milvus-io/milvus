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

#include "storage/LegacyIndexLoader.h"

#include <algorithm>
#include <any>
#include <charconv>
#include <cstring>
#include <limits>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "milvus-storage/common/extend_status.h"
#include "folly/OperationCancelled.h"
#include "storage/AsyncFileReader.h"
#include "storage/DataCodec.h"
#include "storage/EntryStreamUtils.h"
#include "storage/Event.h"
#include "storage/RemoteInputStream.h"
#include "storage/Util.h"

namespace milvus::storage {
namespace {

// Preserves legacy ChunkManager-only contexts. These synchronous reads run on
// the selected async executor; native Arrow inputs use asynchronous range I/O.
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
          folly::CancellationToken token) {
    ThrowIfCancelled(token, "LegacyIndexLoader::Read");
    if (auto* remote = dynamic_cast<RemoteInputStream*>(&input)) {
        co_await ReadFileRangeAsync(
            *remote->GetFile(), offset, data, bytes, token);
    } else if (bytes != 0) {
        const auto read = input.ReadAt(data, offset, bytes);
        CheckFormat(read == bytes, "short object read");
    }
    ThrowIfCancelled(token, "LegacyIndexLoader::Read");
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

std::shared_ptr<milvus::InputStream>
OpenLegacyIndexInput(const ChunkManagerPtr& chunk_manager,
                     const milvus_storage::ArrowFileSystemPtr& fs,
                     const std::string& remote_file) {
    // Legacy local uploads use ChunkManager paths directly, whereas Arrow's
    // local filesystem is rooted and would prepend its root a second time.
    if (!fs || milvus_storage::IsLocalFileSystem(fs)) {
        AssertInfo(chunk_manager != nullptr,
                   "Legacy index requires a file source");
        return std::make_shared<LegacyChunkInput>(chunk_manager, remote_file);
    }
    auto opened = fs->OpenInputFile(remote_file);
    if (!opened.ok()) {
        throw milvus_storage::ToSegcoreError(opened.status());
    }
    return std::make_shared<RemoteInputStream>(std::move(*opened));
}

folly::coro::Task<LegacyIndexFileInfo>
InspectLegacyIndexFileAsync(milvus::InputStream& input,
                            proto::common::LoadPriority priority,
                            folly::CancellationToken token) {
    token = folly::cancellation_token_merge(
        token, co_await folly::coro::co_current_cancellation_token);
    ThrowIfCancelled(token, "LegacyIndexLoader::Inspect");
    const size_t file_bytes = input.Size();
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
        co_await ReadRange(input, 0, prefix.get(), prefix_bytes, token);
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
        co_await ReadRange(input, 0, descriptor.get(), descriptor_bytes, token);
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
        co_await ReadRange(
            input, descriptor_bytes, prefix.get(), event_prefix, token);
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
                     SaturatingMultiply(
                         std::min(info.payload_bytes, DefaultStreamSliceSize()),
                         size_t{2}));
    }
    co_return info;
}

folly::coro::Task<void>
StreamLegacyIndexFileAsync(milvus::InputStream& input,
                           const LegacyIndexFileInfo& info,
                           const LegacyIndexConsumer& consume,
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
            auto lease = co_await Admit(
                SaturatingMultiply(bytes, size_t{2}), priority, token);
            auto buffer = std::make_unique_for_overwrite<uint8_t[]>(bytes);
            co_await ReadRange(input,
                               info.payload_offset + offset,
                               buffer.get(),
                               bytes,
                               token);
            co_await consume(offset, {buffer.get(), bytes});
            ThrowIfCancelled(token, "LegacyIndexLoader::Consume");
            offset += bytes;
        } while (offset < info.payload_bytes);
    } else {
        auto lease = co_await Admit(info.max_transient_bytes, priority, token);
        auto buffer = std::shared_ptr<uint8_t[]>(new uint8_t[info.file_bytes]);
        co_await ReadRange(input, 0, buffer.get(), info.file_bytes, token);
        auto codec = DeserializeFileData(
            buffer, info.file_bytes, true, std::nullopt, info.payload_bytes);
        ThrowIfCancelled(token, "LegacyIndexLoader::Decode");
        co_await consume(0, {codec->PayloadData(), info.payload_bytes});
        ThrowIfCancelled(token, "LegacyIndexLoader::Consume");
    }
}

}  // namespace milvus::storage
