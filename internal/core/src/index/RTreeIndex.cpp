// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <boost/filesystem.hpp>
#include "common/Slice.h"  // for INDEX_FILE_SLICE_META and Disassemble
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "pb/schema.pb.h"
#include "index/Utils.h"
#include "index/RTreeIndex.h"

namespace milvus::index {

constexpr const char* TMP_RTREE_INDEX_PREFIX = "/tmp/milvus/rtree-index/";

// helper to check suffix
static inline bool
ends_with(const std::string& value, const std::string& suffix) {
    return value.size() >= suffix.size() &&
           value.compare(value.size() - suffix.size(), suffix.size(), suffix) ==
               0;
}

template <typename T>
void
RTreeIndex<T>::InitForBuildIndex() {
    auto field =
        std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
    auto prefix = disk_file_manager_->GetIndexIdentifier();
    path_ = std::string(TMP_RTREE_INDEX_PREFIX) + prefix;
    boost::filesystem::create_directories(path_);

    std::string index_file_path = path_ + "/index_file";  // base path (no ext)

    if (boost::filesystem::exists(index_file_path + ".bgi")) {
        ThrowInfo(
            IndexBuildError, "build rtree index temp dir:{} not empty", path_);
    }
    wrapper_ = std::make_shared<RTreeIndexWrapper>(index_file_path, true);
}

template <typename T>
RTreeIndex<T>::RTreeIndex(const storage::FileManagerContext& ctx)
    : ScalarIndex<T>(RTREE_INDEX_TYPE),
      schema_(ctx.fieldDataMeta.field_schema) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);

    if (ctx.for_loading_index) {
        return;
    }
}

template <typename T>
RTreeIndex<T>::~RTreeIndex() {
    // Free wrapper explicitly to ensure files not being used
    wrapper_.reset();

    // Remove temporary directory if it exists
    if (!path_.empty()) {
        auto local_cm = storage::LocalChunkManagerSingleton::GetInstance()
                            .GetChunkManager();
        if (local_cm) {
            LOG_INFO("rtree index remove path:{}", path_);
            local_cm->RemoveDir(path_);
        }
    }
}

static std::string
GetFileName(const std::string& path) {
    auto pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}

// Loading existing R-Tree index
// The config must contain "index_files" -> vector<string>
// Remote index objects will be downloaded to local disk via DiskFileManager,
// then RTreeIndexWrapper will load them.
template <typename T>
void
RTreeIndex<T>::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    LOG_DEBUG("Load RTreeIndex with config {}", config.dump());

    auto index_files_opt =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files_opt.has_value(),
               "index file paths are empty when loading R-Tree index");

    auto files = index_files_opt.value();

    // 1. Extract and load null_offset file(s) if present
    {
        auto find_file = [&](const std::string& target) -> auto {
            return std::find_if(
                files.begin(), files.end(), [&](const std::string& filename) {
                    return GetFileName(filename) == target;
                });
        };

        auto fill_null_offsets = [&](const uint8_t* data, int64_t size) {
            folly::SharedMutexWritePriority::WriteHolder lock(mutex_);
            null_offset_.resize((size_t)size / sizeof(size_t));
            memcpy(null_offset_.data(), data, (size_t)size);
        };

        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);

        std::vector<std::string> null_offset_files;
        if (auto it = find_file(INDEX_FILE_SLICE_META); it != files.end()) {
            // sliced case: collect all parts with prefix index_null_offset
            null_offset_files.push_back(*it);
            for (auto& f : files) {
                auto filename = GetFileName(f);
                static const std::string kName = "index_null_offset";
                if (filename.size() >= kName.size() &&
                    filename.substr(0, kName.size()) == kName) {
                    null_offset_files.push_back(f);
                }
            }
            if (!null_offset_files.empty()) {
                auto index_datas = mem_file_manager_->LoadIndexToMemory(
                    null_offset_files, load_priority);
                auto compacted = CompactIndexDatas(index_datas);
                auto codecs = std::move(compacted.at("index_null_offset"));
                for (auto&& codec : codecs.codecs_) {
                    fill_null_offsets(codec->PayloadData(),
                                      codec->PayloadSize());
                }
            }
        } else if (auto it = find_file("index_null_offset");
                   it != files.end()) {
            null_offset_files.push_back(*it);
            files.erase(it);
            auto index_datas = mem_file_manager_->LoadIndexToMemory(
                {*null_offset_files.begin()}, load_priority);
            auto null_data = std::move(index_datas.at("index_null_offset"));
            fill_null_offsets(null_data->PayloadData(),
                              null_data->PayloadSize());
        }

        // remove loaded null_offset files from files list
        if (!null_offset_files.empty()) {
            files.erase(std::remove_if(
                            files.begin(),
                            files.end(),
                            [&](const std::string& f) {
                                return std::find(null_offset_files.begin(),
                                                 null_offset_files.end(),
                                                 f) != null_offset_files.end();
                            }),
                        files.end());
        }
    }

    // 2. Ensure each file has full remote path. If only filename provided, prepend remote prefix.
    for (auto& f : files) {
        boost::filesystem::path p(f);
        if (!p.has_parent_path()) {
            auto remote_prefix = disk_file_manager_->GetRemoteIndexPrefix();
            f = remote_prefix + "/" + f;
        }
    }

    // 3. Cache remote index files to local disk.
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    disk_file_manager_->CacheIndexToDisk(files, load_priority);

    // 4. Determine local base path (without extension) for RTreeIndexWrapper.
    auto local_paths = disk_file_manager_->GetLocalFilePaths();
    AssertInfo(!local_paths.empty(),
               "RTreeIndex local files are empty after caching to disk");

    // Pick a .dat or .idx file explicitly; avoid meta or others.
    std::string base_path;
    for (const auto& p : local_paths) {
        if (ends_with(p, ".bgi")) {
            base_path = p.substr(0, p.size() - 4);
            break;
        }
    }
    // Fallback: if not found, try meta json
    if (base_path.empty()) {
        for (const auto& p : local_paths) {
            if (ends_with(p, ".meta.json")) {
                base_path =
                    p.substr(0, p.size() - std::string(".meta.json").size());
                break;
            }
        }
    }
    // Final fallback: use the first path as-is
    if (base_path.empty()) {
        base_path = local_paths.front();
    }
    path_ = base_path;

    // 5. Instantiate wrapper and load.
    wrapper_ =
        std::make_shared<RTreeIndexWrapper>(path_, /*is_build_mode=*/false);
    wrapper_->load();

    total_num_rows_ =
        wrapper_->count() + static_cast<int64_t>(null_offset_.size());
    is_built_ = true;

    LOG_INFO(
        "Loaded R-Tree index from {} with {} rows", path_, total_num_rows_);
}

template <typename T>
void
RTreeIndex<T>::Build(const Config& config) {
    InitForBuildIndex();

    // load raw WKB data into memory
    auto field_datas = mem_file_manager_->CacheRawDataToMemory(config);
    BuildWithFieldData(field_datas);
    // after build, mark built
    total_num_rows_ =
        wrapper_->count() + static_cast<int64_t>(null_offset_.size());
    is_built_ = true;
}

template <typename T>
void
RTreeIndex<T>::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    // Default to bulk load for build performance
    // If needed, we can wire a config switch later to disable it.
    bool use_bulk_load = true;
    if (use_bulk_load) {
        // Single pass: collect null offsets locally and compute total rows
        int64_t total_rows = 0;
        if (schema_.nullable()) {
            std::vector<size_t> local_nulls;
            int64_t global_offset = 0;
            for (const auto& fd : field_datas) {
                const auto n = fd->get_num_rows();
                for (int64_t i = 0; i < n; ++i) {
                    if (!fd->is_valid(i)) {
                        local_nulls.push_back(
                            static_cast<size_t>(global_offset));
                    }
                    ++global_offset;
                }
                total_rows += n;
            }
            if (!local_nulls.empty()) {
                folly::SharedMutexWritePriority::WriteHolder lock(mutex_);
                null_offset_.reserve(null_offset_.size() + local_nulls.size());
                null_offset_.insert(
                    null_offset_.end(), local_nulls.begin(), local_nulls.end());
            }
        } else {
            for (const auto& fd : field_datas) {
                total_rows += fd->get_num_rows();
            }
        }
        // bulk load non-null geometries
        wrapper_->bulk_load_from_field_data(field_datas, schema_.nullable());
        total_num_rows_ = total_rows;
        is_built_ = true;
        return;
    }
}

template <typename T>
void
RTreeIndex<T>::finish() {
    if (wrapper_) {
        LOG_INFO("rtree index finish");
        wrapper_->finish();
    }
}

template <typename T>
IndexStatsPtr
RTreeIndex<T>::Upload(const Config& config) {
    // 1. Ensure all buffered data flushed to disk
    finish();

    // 2. Walk temp dir and register files to DiskFileManager
    boost::filesystem::path dir(path_);
    boost::filesystem::directory_iterator end_iter;

    for (boost::filesystem::directory_iterator it(dir); it != end_iter; ++it) {
        if (boost::filesystem::is_directory(*it)) {
            LOG_WARN("{} is a directory, skip", it->path().string());
            continue;
        }

        AssertInfo(disk_file_manager_->AddFile(it->path().string()),
                   "failed to add index file: {}",
                   it->path().string());
    }

    // 3. Collect remote paths to size mapping
    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    // 4. Serialize and register in-memory null_offset if any
    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);
    auto remote_mem_path_to_size =
        mem_file_manager_->GetRemotePathsToFileSize();

    // 5. Assemble IndexStats result
    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size() +
                        remote_mem_path_to_size.size());
    for (auto& kv : remote_paths_to_size) {
        index_files.emplace_back(kv.first, kv.second);
    }
    for (auto& kv : remote_mem_path_to_size) {
        index_files.emplace_back(kv.first, kv.second);
    }

    int64_t mem_size = mem_file_manager_->GetAddedTotalMemSize();
    int64_t file_size = disk_file_manager_->GetAddedTotalFileSize();

    return IndexStats::New(mem_size + file_size, std::move(index_files));
}

template <typename T>
BinarySet
RTreeIndex<T>::Serialize(const Config& config) {
    folly::SharedMutexWritePriority::ReadHolder lock(mutex_);
    auto bytes = null_offset_.size() * sizeof(size_t);
    BinarySet res_set;
    if (bytes > 0) {
        std::shared_ptr<uint8_t[]> buf(new uint8_t[bytes]);
        std::memcpy(buf.get(), null_offset_.data(), bytes);
        res_set.Append("index_null_offset", buf, bytes);
    }
    milvus::Disassemble(res_set);
    return res_set;
}

template <typename T>
void
RTreeIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    ThrowInfo(ErrorCode::NotImplemented,
              "Load(BinarySet) is not yet supported for RTreeIndex");
}

template <typename T>
void
RTreeIndex<T>::Build(size_t n, const T* values, const bool* valid_data) {
    // Generic Build by value array is not required for RTree at the moment.
    ThrowInfo(ErrorCode::NotImplemented,
              "Build(size_t, values, valid) not supported for RTreeIndex");
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::In(size_t n, const T* values) {
    ThrowInfo(ErrorCode::NotImplemented, "In() not supported for RTreeIndex");
    return {};
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::IsNull() {
    int64_t count = Count();
    TargetBitmap bitset(count);
    folly::SharedMutexWritePriority::ReadHolder lock(mutex_);
    auto end = std::lower_bound(
        null_offset_.begin(), null_offset_.end(), static_cast<size_t>(count));
    for (auto it = null_offset_.begin(); it != end; ++it) {
        bitset.set(*it);
    }
    return bitset;
}

template <typename T>
TargetBitmap
RTreeIndex<T>::IsNotNull() {
    int64_t count = Count();
    TargetBitmap bitset(count, true);
    folly::SharedMutexWritePriority::ReadHolder lock(mutex_);
    auto end = std::lower_bound(
        null_offset_.begin(), null_offset_.end(), static_cast<size_t>(count));
    for (auto it = null_offset_.begin(); it != end; ++it) {
        bitset.reset(*it);
    }
    return bitset;
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::InApplyFilter(size_t n,
                             const T* values,
                             const std::function<bool(size_t)>& filter) {
    ThrowInfo(ErrorCode::NotImplemented,
              "InApplyFilter() not supported for RTreeIndex");
    return {};
}

template <typename T>
void
RTreeIndex<T>::InApplyCallback(size_t n,
                               const T* values,
                               const std::function<void(size_t)>& callback) {
    ThrowInfo(ErrorCode::NotImplemented,
              "InApplyCallback() not supported for RTreeIndex");
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::NotIn(size_t n, const T* values) {
    ThrowInfo(ErrorCode::NotImplemented,
              "NotIn() not supported for RTreeIndex");
    return {};
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::Range(T value, OpType op) {
    ThrowInfo(ErrorCode::NotImplemented,
              "Range(value, op) not supported for RTreeIndex");
    return {};
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::Range(T lower_bound_value,
                     bool lb_inclusive,
                     T upper_bound_value,
                     bool ub_inclusive) {
    ThrowInfo(ErrorCode::NotImplemented,
              "Range(lower, upper) not supported for RTreeIndex");
    return {};
}

template <typename T>
void
RTreeIndex<T>::QueryCandidates(proto::plan::GISFunctionFilterExpr_GISOp op,
                               const Geometry query_geometry,
                               std::vector<int64_t>& candidate_offsets) {
    AssertInfo(wrapper_ != nullptr, "R-Tree index wrapper is null");

    // Create GEOS context and ensure it's properly released
    GEOSContextHandle_t ctx = GEOS_init_r();

    wrapper_->query_candidates(
        op, query_geometry.GetGeometry(), ctx, candidate_offsets);
    GEOS_finish_r(ctx);
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::Query(const DatasetPtr& dataset) {
    AssertInfo(schema_.data_type() == proto::schema::DataType::Geometry,
               "RTreeIndex can only be queried on geometry field");
    auto op =
        dataset->Get<proto::plan::GISFunctionFilterExpr_GISOp>(OPERATOR_TYPE);
    // Query geometry WKB passed via MATCH_VALUE as std::string
    auto geometry = dataset->Get<Geometry>(MATCH_VALUE);

    // 1) Coarse candidates by R-Tree on MBR
    std::vector<int64_t> candidate_offsets;
    QueryCandidates(op, geometry, candidate_offsets);

    // 2) Build initial bitmap from candidates
    TargetBitmap res(this->Count());
    for (auto off : candidate_offsets) {
        if (off >= 0 && off < res.size()) {
            res.set(off);
        }
    }

    return res;
}

// ------------------------------------------------------------------
// BuildWithRawDataForUT – real implementation for unit-test scenarios
// ------------------------------------------------------------------

template <typename T>
void
RTreeIndex<T>::BuildWithRawDataForUT(size_t n,
                                     const void* values,
                                     const Config& config) {
    // In UT we directly receive an array of std::string (WKB) with length n.
    const std::string* wkb_array = reinterpret_cast<const std::string*>(values);

    // Guard: n should represent number of strings not raw bytes
    AssertInfo(n > 0, "BuildWithRawDataForUT expects element count > 0");
    LOG_WARN("BuildWithRawDataForUT:{}", n);
    this->InitForBuildIndex();

    int64_t offset = 0;
    for (size_t i = 0; i < n; ++i) {
        const auto& wkb = wkb_array[i];
        const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(wkb.data());
        this->wrapper_->add_geometry(data_ptr, wkb.size(), offset++);
    }
    this->finish();
    LOG_WARN("BuildWithRawDataForUT finish");
    this->total_num_rows_ = offset;
    LOG_WARN("BuildWithRawDataForUT total_num_rows_:{}", this->total_num_rows_);
    this->is_built_ = true;
}

template <typename T>
void
RTreeIndex<T>::BuildWithStrings(const std::vector<std::string>& geometries) {
    AssertInfo(!geometries.empty(),
               "BuildWithStrings expects non-empty geometries");
    LOG_INFO("BuildWithStrings: building RTree index for {} geometries",
             geometries.size());

    this->InitForBuildIndex();

    int64_t offset = 0;
    for (const auto& wkb : geometries) {
        if (!wkb.empty()) {
            const uint8_t* data_ptr =
                reinterpret_cast<const uint8_t*>(wkb.data());
            this->wrapper_->add_geometry(data_ptr, wkb.size(), offset);
        } else {
            // Handle null geometry
            this->null_offset_.push_back(offset);
        }
        offset++;
    }

    this->finish();
    this->total_num_rows_ = offset;
    this->is_built_ = true;

    LOG_INFO("BuildWithStrings: completed building RTree index, total_rows: {}",
             this->total_num_rows_);
}

template <typename T>
void
RTreeIndex<T>::AddGeometry(const std::string& wkb_data, int64_t row_offset) {
    if (!wrapper_) {
        // Initialize if not already done
        this->InitForBuildIndex();
    }

    if (!wkb_data.empty()) {
        const uint8_t* data_ptr =
            reinterpret_cast<const uint8_t*>(wkb_data.data());
        wrapper_->add_geometry(data_ptr, wkb_data.size(), row_offset);

        // Update total row count
        if (row_offset >= total_num_rows_) {
            total_num_rows_ = row_offset + 1;
        }

        LOG_DEBUG("Added geometry at row offset {}", row_offset);
    } else {
        // Handle null geometry
        folly::SharedMutexWritePriority::WriteHolder lock(mutex_);
        null_offset_.push_back(static_cast<size_t>(row_offset));

        // Update total row count
        if (row_offset >= total_num_rows_) {
            total_num_rows_ = row_offset + 1;
        }

        LOG_DEBUG("Added null geometry at row offset {}", row_offset);
    }
}

// Explicit template instantiation for std::string as we only support string field for now.
template class RTreeIndex<std::string>;

}  // namespace milvus::index