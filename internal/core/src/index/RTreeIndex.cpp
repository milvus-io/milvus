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

#include <string.h>
#include "common/FastMem.h"
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <exception>
#include <list>
#include <map>
#include <mutex>
#include <string_view>
#include <shared_mutex>
#include <utility>

#include "boost/filesystem/directory.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/iterator/iterator_facade.hpp"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Geometry.h"
#include "common/Slice.h"  // for INDEX_FILE_SLICE_META and Disassemble
#include "common/Tracer.h"
#include "folly/SharedMutex.h"
#include "geos_c.h"
#include "glog/logging.h"
#include "index/RTreeIndex.h"
#include "index/Utils.h"
#include "knowhere/dataset.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/DataCodec.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/FileWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/Types.h"

namespace milvus::index {

static constexpr size_t kMetaJsonSuffixLen = sizeof(".meta.json") - 1;

static std::string
GetRTreeTempPrefix() {
    auto& lcm = milvus::storage::LocalChunkManagerSingleton::GetInstance();
    return lcm.GetChunkManager()->GetRootPath() + "/rtree-index/";
}

// helper to check suffix
static inline bool
ends_with(const std::string& value, const std::string& suffix) {
    return value.size() >= suffix.size() &&
           value.compare(value.size() - suffix.size(), suffix.size(), suffix) ==
               0;
}

template <typename T>
void
RTreeIndex<T>::InitForBuildIndex(bool is_growing) {
    if (is_growing) {
        // Concurrency model: production currently serializes writes to a
        // growing segment (one flowgraph consumer per vchannel drives
        // SegmentGrowingImpl::Insert, and recovery's LoadGrowing completes
        // before consumption starts). The locking here is defense-in-depth
        // honoring the segcore-level IndexingRecord::AppendingIndex contract
        // ("concurrent, reentrant"), not a claim that production exercises
        // multi-writer inserts today. Writers may still race concurrent
        // READERS (search on the growing index), so the lazy first-time init
        // stays idempotent under the write lock: the wrapper is created
        // exactly once and readers never observe a torn shared_ptr.
        std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
        if (wrapper_) {
            return;
        }
        path_ = "";
        std::string index_file_path;  // empty: in-memory growing wrapper
        wrapper_ = std::make_shared<RTreeIndexWrapper>(index_file_path, true);
        return;
    }

    // Non-growing (build/load) path runs single-threaded.
    auto prefix = disk_file_manager_->GetIndexIdentifier();
    path_ = GetRTreeTempPrefix() + prefix;
    boost::filesystem::create_directories(path_);
    std::string index_file_path = path_ + "/index_file";  // base path (no ext)
    if (boost::filesystem::exists(index_file_path + ".bgi")) {
        ThrowInfo(
            IndexBuildError, "build rtree index temp dir:{} not empty", path_);
    }

    auto wrapper = std::make_shared<RTreeIndexWrapper>(index_file_path, true);
    std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
    wrapper_ = std::move(wrapper);
}

template <typename T>
RTreeIndex<T>::RTreeIndex(const storage::FileManagerContext& ctx)
    : ScalarIndex<T>(RTREE_INDEX_TYPE),
      schema_(ctx.fieldDataMeta.field_schema) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);
    this->file_manager_ = mem_file_manager_;

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

    auto files = std::move(*index_files_opt);

    // 1. Extract and load null_offset file(s) if present
    {
        auto find_file = [&](const std::string& target) -> auto{
            return std::find_if(
                files.begin(), files.end(), [&](const std::string& filename) {
                    return GetFileName(filename) == target;
                });
        };

        auto fill_null_offsets = [&](const uint8_t* data, int64_t size) {
            // null_offset is a size_t[] payload; its byte length must be a
            // multiple of sizeof(size_t). Otherwise resize() truncates the
            // destination while FastMemcpy still copies `size` bytes, writing
            // past the buffer. Reject a malformed/truncated sidecar instead.
            AssertInfo((size_t)size % sizeof(size_t) == 0,
                       "corrupt R-Tree null_offset payload: byte size {} is "
                       "not a multiple of {}",
                       size,
                       sizeof(size_t));
            std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
            null_offset_.resize((size_t)size / sizeof(size_t));
            milvus::fastmem::FastMemcpy(
                null_offset_.data(), data, (size_t)size);
        };

        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);

        std::vector<std::string> null_offset_files;
        bool loaded_null_offsets = false;
        if (auto it = find_file(INDEX_FILE_SLICE_META); it != files.end()) {
            // sliced case: collect all parts with prefix index_null_offset
            null_offset_files.push_back(*it);
            for (auto& f : files) {
                auto filename = GetFileName(f);
                static constexpr std::string_view kName = "index_null_offset_";
                if (filename.size() > kName.size() &&
                    filename.compare(
                        0, kName.size(), kName.data(), kName.size()) == 0) {
                    null_offset_files.push_back(f);
                }
            }
            if (null_offset_files.size() > 1) {
                auto index_datas = mem_file_manager_->LoadIndexToMemory(
                    null_offset_files, load_priority);
                auto slice_meta =
                    std::move(index_datas.at(INDEX_FILE_SLICE_META));
                auto codecs = CompactIndexDatasByKey(
                    "index_null_offset", std::move(slice_meta), index_datas);
                AssertInfo(codecs.codecs_.size() > 0,
                           "null offset file is empty");
                auto codec = AssembleIndexDataCodec(codecs);
                fill_null_offsets(codec->PayloadData(), codec->PayloadSize());
                loaded_null_offsets = true;
            } else {
                null_offset_files.clear();
            }
        }
        if (!loaded_null_offsets) {
            if (auto it = find_file("index_null_offset"); it != files.end()) {
                null_offset_files.push_back(*it);
                files.erase(it);
                auto index_datas = mem_file_manager_->LoadIndexToMemory(
                    {*null_offset_files.begin()}, load_priority);
                auto null_data = std::move(index_datas.at("index_null_offset"));
                fill_null_offsets(null_data->PayloadData(),
                                  null_data->PayloadSize());
            }
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
                base_path = p.substr(0, p.size() - kMetaJsonSuffixLen);
                break;
            }
        }
    }
    // Final fallback: use the first path as-is
    if (base_path.empty()) {
        base_path = local_paths.front();
    }
    path_ = base_path;

    // 5. Instantiate wrapper and load it fully BEFORE publishing, then
    // publish every member under the same lock the readers take (Count /
    // QueryCandidates / ComputeByteSize snapshot wrapper_ under mutex_):
    // the no-torn-read invariant only holds if every writer participates,
    // exactly as the growing publish in InitForBuildIndex does.
    auto wrapper =
        std::make_shared<RTreeIndexWrapper>(path_, /*is_build_mode=*/false);
    wrapper->load();

    {
        std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
        wrapper_ = wrapper;
        total_num_rows_ =
            wrapper->count() + static_cast<int64_t>(null_offset_.size());
        is_built_ = true;
    }
    ComputeByteSize();

    LOG_INFO(
        "Loaded R-Tree index from {} with {} rows", path_, total_num_rows_);
}

template <typename T>
void
RTreeIndex<T>::Build(const Config& config) {
    InitForBuildIndex(false);

    // load raw WKB data into memory
    auto field_datas = mem_file_manager_->CacheRawDataToMemory(config);
    BuildWithFieldData(field_datas);
    // after build, mark built
    total_num_rows_ =
        wrapper_->count() + static_cast<int64_t>(null_offset_.size());
    is_built_ = true;
    ComputeByteSize();
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
                std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
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
        ComputeByteSize();
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
    std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
    auto bytes = null_offset_.size() * sizeof(size_t);
    BinarySet res_set;
    if (bytes > 0) {
        std::shared_ptr<uint8_t[]> buf(new uint8_t[bytes]);
        milvus::fastmem::FastMemcpy(buf.get(), null_offset_.data(), bytes);
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
    std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
    // null_offset_ is not guaranteed to be sorted by construction: AddGeometry
    // appends offsets in arrival order, and while production currently
    // serializes inserts per growing segment (so the order is monotonic
    // today), nothing in this class enforces it. A std::lower_bound shortcut
    // would silently rely on that external property and could leave in-range
    // offsets past the bound (or, worse, iterate offsets >= count).
    // Bounds-check each element instead so an out-of-range offset can never
    // write past the bitset.
    for (auto off : null_offset_) {
        if (off < static_cast<size_t>(count)) {
            bitset.set(off);
        }
    }
    return bitset;
}

template <typename T>
TargetBitmap
RTreeIndex<T>::IsNotNull() {
    int64_t count = Count();
    TargetBitmap bitset(count, true);
    std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
    // See IsNull(): null_offset_ is not sorted by construction, so
    // bounds-check every offset rather than relying on a sorted-range shortcut.
    for (auto off : null_offset_) {
        if (off < static_cast<size_t>(count)) {
            bitset.reset(off);
        }
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
RTreeIndex<T>::Range(const T& value, OpType op) {
    ThrowInfo(ErrorCode::NotImplemented,
              "Range(value, op) not supported for RTreeIndex");
    return {};
}

template <typename T>
const TargetBitmap
RTreeIndex<T>::Range(const T& lower_bound_value,
                     bool lb_inclusive,
                     const T& upper_bound_value,
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
    // Snapshot wrapper_ under the shared lock: on a growing segment it is
    // published lazily by InitForBuildIndex() under the write lock, so an
    // unsynchronized read here could observe a torn shared_ptr.
    std::shared_ptr<RTreeIndexWrapper> wrapper;
    {
        std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
        wrapper = wrapper_;
    }
    AssertInfo(wrapper != nullptr, "R-Tree index wrapper is null");

    // Scoped GEOS context: InitGEOSContext throws a retriable
    // MemAllocateFailed on OOM instead of handing a null context to
    // query_candidates, and the RAII guard releases the context even when
    // query_candidates throws (e.g. candidate_offsets growth hitting
    // bad_alloc, or an AssertInfo on the query geometry) -- a trailing
    // GEOS_finish_r would leak once per failed query.
    ScopedGeosResources geos("query_candidates");

    wrapper->query_candidates(
        op, query_geometry.GetGeometry(), geos.ctx, candidate_offsets);
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
    // Count() reports the row space (max(total_num_rows_, indexed)), so a
    // candidate below the row count can no longer be clipped away. The bound
    // stays as a guard for the benign race only: on a growing index a writer
    // may publish a higher offset between the Count() snapshot above and
    // QueryCandidates() returning. Such a row is not yet visible to this
    // query's timestamp, so dropping it is correct -- not an invariant
    // violation, which is why this stays silent rather than asserting.
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
    this->InitForBuildIndex(false);

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

    this->InitForBuildIndex(false);

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
RTreeIndex<T>::AddGeometry(const std::string& wkb_data,
                           int64_t row_offset,
                           bool is_valid) {
    // Snapshot wrapper_ under the lock; lazily (and idempotently) initialize it
    // if this is the first insert. Production serializes inserts per growing
    // segment (see InitForBuildIndex), but AddGeometry races concurrent
    // READERS and the AppendingIndex contract permits concurrent writers, so
    // wrapper_ must never be read or published unlocked.
    std::shared_ptr<RTreeIndexWrapper> wrapper;
    {
        std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
        wrapper = wrapper_;
    }
    if (!wrapper) {
        this->InitForBuildIndex(true);  // idempotent under the write lock
        std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
        wrapper = wrapper_;
    }

    // Nullness is decided by is_valid alone, never by the payload: a valid row
    // with an empty/unparseable WKB gets a placeholder MBR from add_geometry,
    // keeping growing consistent with the sealed bulk_load classification.
    if (is_valid) {
        const uint8_t* data_ptr =
            reinterpret_cast<const uint8_t*>(wkb_data.data());
        wrapper->add_geometry(data_ptr, wkb_data.size(), row_offset);

        // Update total row count under the same lock that guards readers
        // (Count/NumRows), so the non-null path is symmetric with the null
        // path below and never races a concurrent search on the growing index.
        {
            std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
            if (row_offset >= total_num_rows_) {
                total_num_rows_ = row_offset + 1;
            }
        }

        LOG_DEBUG("Added geometry at row offset {}", row_offset);
    } else {
        // Handle null geometry. Idempotent per offset: a batch retried after
        // a mid-batch retriable failure (bad_alloc -> MemAllocateFailed in
        // the segcore caller) re-drives rows that already committed, and a
        // duplicated null offset would inflate Count() past the segment row
        // space. The dedup set may throw first (nothing mutated yet); the
        // vector push_back rolls the marker back with a noexcept erase.
        std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
        auto [marker, inserted] =
            null_offset_dedup_.insert(static_cast<size_t>(row_offset));
        if (inserted) {
            try {
                null_offset_.push_back(static_cast<size_t>(row_offset));
            } catch (...) {
                null_offset_dedup_.erase(marker);
                throw;
            }
        }

        // Update total row count
        if (row_offset >= total_num_rows_) {
            total_num_rows_ = row_offset + 1;
        }

        LOG_DEBUG("Added null geometry at row offset {}", row_offset);
    }
}

template <typename T>
void
RTreeIndex<T>::WriteEntries(storage::IndexEntryWriter* writer) {
    finish();

    std::vector<boost::filesystem::path> files;
    if (!path_.empty()) {
        boost::filesystem::path dir(path_);
        for (boost::filesystem::directory_iterator iter(dir);
             iter != boost::filesystem::directory_iterator();
             ++iter) {
            if (!boost::filesystem::is_directory(*iter)) {
                files.push_back(iter->path());
            }
        }
        std::sort(files.begin(), files.end());
    }

    std::vector<std::string> file_names;
    for (const auto& f : files) {
        file_names.push_back(f.filename().string());
    }

    std::shared_lock<folly::SharedMutexWritePriority> lock(mutex_);
    bool has_null = !null_offset_.empty();
    lock.unlock();

    writer->PutMeta("file_names", file_names);
    writer->PutMeta("has_null", has_null);

    for (const auto& file_path : files) {
        auto file = file_path.string();
        auto fd = open(file.c_str(), O_RDONLY | O_CLOEXEC);
        AssertInfo(fd != -1, "open file failed: {}", file);
        auto file_size = boost::filesystem::file_size(file);
        auto file_name = file_path.filename().string();
        writer->WriteEntry(file_name, fd, file_size);
        close(fd);
    }

    if (has_null) {
        std::shared_lock<folly::SharedMutexWritePriority> lock2(mutex_);
        writer->WriteEntry("index_null_offset",
                           null_offset_.data(),
                           null_offset_.size() * sizeof(size_t));
    }

    LOG_INFO("write rtree index entries: {} files, {} null offsets",
             files.size(),
             null_offset_.size());
}

template <typename T>
void
RTreeIndex<T>::LoadEntries(storage::IndexEntryReader& reader,
                           const Config& config) {
    auto file_names = reader.GetMeta<std::vector<std::string>>("file_names");
    bool has_null = reader.GetMeta<bool>("has_null");

    path_ = disk_file_manager_->GetLocalIndexObjectPrefix();
    boost::filesystem::create_directories(path_);

    std::vector<std::pair<std::string, std::string>> pairs;
    for (const auto& fn : file_names) {
        pairs.emplace_back(fn, path_ + "/" + fn);
    }
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    reader.ReadEntriesStreamToFiles(
        pairs, storage::io::GetPriorityFromLoadPriority(load_priority));

    if (has_null) {
        auto null_entry = reader.ReadEntry("index_null_offset");
        // See fill_null_offsets in Load(): the payload must be size_t-aligned,
        // otherwise resize() under-allocates and FastMemcpy overflows.
        AssertInfo(null_entry.data.size() % sizeof(size_t) == 0,
                   "corrupt R-Tree null_offset entry: byte size {} is not a "
                   "multiple of {}",
                   null_entry.data.size(),
                   sizeof(size_t));
        std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
        null_offset_.resize(null_entry.data.size() / sizeof(size_t));
        milvus::fastmem::FastMemcpy(null_offset_.data(),
                                    null_entry.data.data(),
                                    null_entry.data.size());
    }

    // Determine base path (without extension) from local file names,
    // same logic as V2 Load.
    std::string base_path;
    for (const auto& fn : file_names) {
        auto local = path_ + "/" + fn;
        if (ends_with(local, ".bgi")) {
            base_path = local.substr(0, local.size() - 4);
            break;
        }
    }
    if (base_path.empty()) {
        for (const auto& fn : file_names) {
            auto local = path_ + "/" + fn;
            if (ends_with(local, ".meta.json")) {
                base_path = local.substr(0, local.size() - kMetaJsonSuffixLen);
                break;
            }
        }
    }
    AssertInfo(!base_path.empty(),
               "RTreeIndex LoadEntries: cannot determine base path from files");
    path_ = base_path;

    // Load the wrapper fully BEFORE publishing, then publish under the
    // readers' lock -- same discipline as Load() above.
    auto wrapper =
        std::make_shared<RTreeIndexWrapper>(path_, /*is_build_mode=*/false);
    wrapper->load();

    {
        std::unique_lock<folly::SharedMutexWritePriority> lock(mutex_);
        wrapper_ = wrapper;
        total_num_rows_ =
            wrapper->count() + static_cast<int64_t>(null_offset_.size());
        is_built_ = true;
    }
    ComputeByteSize();
    LOG_INFO(
        "LoadEntries RTreeIndex done, file_count: {}, has_null: {}, "
        "base_path: {}",
        file_names.size(),
        has_null,
        path_);
}

// Explicit template instantiation for std::string as we only support string field for now.
template class RTreeIndex<std::string>;

}  // namespace milvus::index
