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

#include <arrow/io/memory.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/FieldData.h"
#include "common/Json.h"
#include "common/LoadInfo.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"

namespace milvus::test::expr_index {

inline uint64_t
NextFixtureId() {
    static std::atomic<uint64_t> next{0};
    return next.fetch_add(1, std::memory_order_relaxed);
}

// Keep this owner before the segment member so the segment releases its raw
// column before the binlogs are removed. Only this fixture's objects are erased.
class RawFieldFiles {
 public:
    RawFieldFiles()
        : cm_(storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager()),
          id_(NextFixtureId()) {
    }

    RawFieldFiles(const RawFieldFiles&) = delete;
    RawFieldFiles&
    operator=(const RawFieldFiles&) = delete;

    ~RawFieldFiles() {
        for (const auto& file : files_) {
            cm_->Remove(file);
        }
    }

    LoadFieldDataInfo
    Prepare(FieldId field_id, const std::vector<FieldDataPtr>& chunks) {
        FieldBinlogInfo field{};
        field.field_id = field_id.get();
        field.row_count = 0;
        field.entries_nums.reserve(chunks.size());
        field.memory_sizes.reserve(chunks.size());
        field.insert_files.reserve(chunks.size());
        const auto batch_id = NextFixtureId();
        for (size_t i = 0; i < chunks.size(); ++i) {
            const auto& chunk = chunks[i];
            const auto file =
                cm_->GetRootPath() + "/expr_index_integration/" +
                std::to_string(id_) + "/" + std::to_string(batch_id) + "/" +
                std::to_string(field_id.get()) + "/" + std::to_string(i);
            auto payload = std::make_shared<storage::PayloadReader>(chunk);
            storage::InsertData insert(payload);
            insert.SetFieldDataMeta(
                {1, 1, static_cast<int64_t>(id_), field_id.get()});
            auto bytes = insert.serialize_to_remote_file();
            cm_->Write(file, bytes.data(), bytes.size());
            files_.push_back(file);
            field.row_count += chunk->get_num_rows();
            field.entries_nums.push_back(chunk->get_num_rows());
            field.memory_sizes.push_back(chunk->Size());
            field.insert_files.push_back(file);
        }
        LoadFieldDataInfo info;
        info.field_infos.emplace(field_id.get(), std::move(field));
        return info;
    }

 private:
    storage::ChunkManagerPtr cm_;
    uint64_t id_;
    std::vector<std::string> files_;
};

inline std::shared_ptr<FieldData<std::string>>
StringField(const std::vector<std::string>& rows,
            bool nullable = false,
            const uint8_t* validity = nullptr) {
    auto field =
        std::make_shared<FieldData<std::string>>(DataType::VARCHAR, nullable);
    FieldDataBase& output = *field;
    if (nullable) {
        AssertInfo(validity != nullptr,
                   "nullable string fixture needs validity");
        output.FillFieldData(rows.data(), validity, rows.size(), 0);
    } else {
        output.FillFieldData(rows.data(), rows.size());
    }
    return field;
}

inline std::shared_ptr<FieldData<Json>>
JsonField(const std::vector<std::string>& rows,
          bool nullable = false,
          const uint8_t* validity = nullptr) {
    std::vector<Json> jsons;
    jsons.reserve(rows.size());
    for (const auto& row : rows) {
        jsons.emplace_back(simdjson::padded_string(row));
    }
    auto field = std::make_shared<FieldData<Json>>(DataType::JSON, nullable);
    FieldDataBase& output = *field;
    if (nullable) {
        std::vector<uint8_t> all_valid((rows.size() + 7) / 8, 0xFF);
        output.FillFieldData(jsons.data(),
                             validity == nullptr ? all_valid.data() : validity,
                             jsons.size(),
                             0);
    } else {
        output.FillFieldData(jsons.data(), jsons.size());
    }
    return field;
}

struct OpenedIndex {
    index::IIndexReaderBasePtr reader;
    std::string family;
    Config params;
    index::ReaderCaps caps;
};

// The consumer fixture enters the same adapter/materializer/builder and packed
// loader interfaces as production. Its cache translator only owns the finished
// production reader; it does not substitute query behavior.
inline OpenedIndex
BuildIndex(FieldId field_id,
           DataType field_type,
           const std::string& index_type,
           const std::vector<FieldDataPtr>& chunks,
           Config params = Config::object(),
           DataType element_type = DataType::NONE,
           bool nested = false,
           bool mmap = false) {
    int64_t rows = 0;
    for (const auto& chunk : chunks) {
        rows += chunk->get_num_rows();
    }
    AssertInfo(!chunks.empty(), "consumer index fixture needs input batches");
    params[index::FIELD_ID] = field_id.get();
    params[index::SCALAR_INDEX_ENGINE_VERSION] = 3;
    params["nullable"] = chunks.front()->IsNullable();
    params["num_rows"] = rows;
    auto adapted = index::AdaptIndexType({.index_type = index_type,
                                          .field_type = field_type,
                                          .element_type = element_type,
                                          .params = std::move(params),
                                          .is_nested = nested});
    auto materializer = indexbuilder::MakeScalarBuildInputMaterializer(
        field_type, adapted.value_type, rows, adapted.family, adapted.params);
    for (const auto& chunk : chunks) {
        materializer->Add(chunk);
    }
    auto artifact = std::move(*materializer).Build();
    auto buffer = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto output = std::make_shared<storage::RemoteOutputStream>(buffer);
    storage::IndexEntryDirectStreamWriter writer(output, 4096);
    artifact->Serialize(writer);
    writer.Finish();
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            buffer->Finish().ValueOrDie()));
    auto source = storage::IndexEntryReader::Open(input, input->Size());
    const auto family = index::ResolvePackedLoadFamily(
        adapted.family, source->IndexMeta(), adapted.params);
    auto load_params = std::move(adapted.params);
    if (family != index::families::kJsonFlat) {
        load_params = index::AnnotateJsonProjectionCompleteness(
            std::move(load_params), source->Directory(), source->IndexMeta());
    }
    const auto loader = index::LoaderRegistry::Instance().Lookup(family);
    AssertInfo(static_cast<bool>(loader), "missing consumer fixture loader");
    storage::LoadOptions options;
    options.enable_mmap = mmap;
    options.mmap_dir_path = "/tmp";
    options.params = load_params;
    const auto caps = loader.derive_caps(load_params);
    auto reader = loader.Load(
        {index::OpenedIndexSource{index::PackedIndexSource{
             std::shared_ptr<storage::IndexEntryReader>(std::move(source))}},
         options});
    AssertInfo(reader != nullptr, "consumer fixture loader returned no reader");
    return {std::move(reader), family, std::move(load_params), caps};
}

class ReaderTranslator final
    : public cachinglayer::Translator<index::IIndexReaderBase> {
 public:
    ReaderTranslator(index::IIndexReaderBasePtr reader, OpContext** observed)
        : reader_(std::move(reader)),
          usage_(reader_->CellByteSize()),
          key_("expr-reader-" + std::to_string(NextFixtureId())),
          observed_(observed),
          meta_(usage_.file_bytes > 0 ? cachinglayer::StorageType::DISK
                                      : cachinglayer::StorageType::MEMORY,
                cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
                cachinglayer::CellDataType::SCALAR_INDEX,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                false) {
    }

    size_t
    num_cells() const override {
        return 1;
    }
    cachinglayer::cid_t
    cell_id_of(cachinglayer::uid_t) const override {
        return 0;
    }
    std::pair<cachinglayer::ResourceUsage, cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(cachinglayer::cid_t) const override {
        return {usage_, {0, 0}};
    }
    int64_t
    cells_storage_bytes(
        const std::vector<cachinglayer::cid_t>&) const override {
        return usage_.file_bytes;
    }
    const std::string&
    key() const override {
        return key_;
    }
    cachinglayer::Meta*
    meta() override {
        return &meta_;
    }
    std::vector<std::pair<cachinglayer::cid_t, index::IIndexReaderBasePtr>>
    get_cells(OpContext* context,
              const std::vector<cachinglayer::cid_t>& cells) override {
        AssertInfo(cells.size() == 1 && cells[0] == 0 && reader_ != nullptr,
                   "consumer reader fixture assumes one non-evictable cell");
        if (observed_ != nullptr) {
            *observed_ = context;
        }
        std::vector<std::pair<cachinglayer::cid_t, index::IIndexReaderBasePtr>>
            result;
        result.emplace_back(0, std::move(reader_));
        return result;
    }

 private:
    index::IIndexReaderBasePtr reader_;
    cachinglayer::ResourceUsage usage_;
    std::string key_;
    OpContext** observed_;
    cachinglayer::Meta meta_;
};

inline void
InstallIndex(segcore::SegmentSealed& segment,
             FieldId field_id,
             DataType field_type,
             OpenedIndex opened,
             DataType element_type = DataType::NONE,
             OpContext** observed = nullptr) {
    segcore::LoadIndexInfo info{};
    info.field_id = field_id.get();
    info.field_type = field_type;
    info.element_type = element_type;
    info.index_id = NextFixtureId();
    info.index_engine_version = 3;
    info.index_family = opened.family;
    info.index_value_type = opened.reader->ValueType();
    info.index_caps = opened.caps;
    info.num_rows = opened.reader->Count();
    for (const auto& [key, value] : opened.params.items()) {
        info.index_params.emplace(
            key, value.is_string() ? value.get<std::string>() : value.dump());
    }
    std::unique_ptr<cachinglayer::Translator<index::IIndexReaderBase>>
        translator = std::make_unique<ReaderTranslator>(
            std::move(opened.reader), observed);
    info.cache_index = cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(translator));
    // Consumer tests retain a raw column for refinement/fallback. The fixture
    // supplies the load estimate explicitly, as the production boundary does.
    info.load_resource_request = LoadResourceRequest{};
    segment.LoadIndex(info);
}

}  // namespace milvus::test::expr_index
