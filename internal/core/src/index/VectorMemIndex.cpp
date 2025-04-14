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

#include "index/VectorMemIndex.h"

#include <unistd.h>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/Common.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "fmt/format.h"

#include "index/Index.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "common/EasyAssert.h"
#include "config/ConfigKnowhere.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/comp/time_recorder.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/File.h"
#include "common/Slice.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "log/Log.h"
#include "storage/DataCodec.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"
#include "monitor/prometheus_client.h"

namespace milvus::index {

template <typename T>
VectorMemIndex<T>::VectorMemIndex(
    const IndexType& index_type,
    const MetricType& metric_type,
    const IndexVersion& version,
    bool use_knowhere_build_pool,
    const storage::FileManagerContext& file_manager_context)
    : VectorIndex(index_type, metric_type),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
    CheckMetricTypeSupport<T>(metric_type);
    AssertInfo(!is_unsupported(index_type, metric_type),
               index_type + " doesn't support metric: " + metric_type);
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
    CheckCompatible(version);
    auto get_index_obj =
        knowhere::IndexFactory::Instance().Create<T>(GetIndexType(), version);
    if (get_index_obj.has_value()) {
        index_ = get_index_obj.value();
    } else {
        auto err = get_index_obj.error();
        if (err == knowhere::Status::invalid_index_error) {
            PanicInfo(ErrorCode::Unsupported, get_index_obj.what());
        }
        PanicInfo(ErrorCode::KnowhereError, get_index_obj.what());
    }
}

template <typename T>
VectorMemIndex<T>::VectorMemIndex(const IndexType& index_type,
                                  const MetricType& metric_type,
                                  const IndexVersion& version,
                                  bool use_knowhere_build_pool,
                                  const knowhere::ViewDataOp view_data)
    : VectorIndex(index_type, metric_type),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
    CheckMetricTypeSupport<T>(metric_type);
    AssertInfo(!is_unsupported(index_type, metric_type),
               index_type + " doesn't support metric: " + metric_type);

    auto view_data_pack = knowhere::Pack(view_data);
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<T>(
        GetIndexType(), version, view_data_pack);
    if (get_index_obj.has_value()) {
        index_ = get_index_obj.value();
    } else {
        auto err = get_index_obj.error();
        if (err == knowhere::Status::invalid_index_error) {
            PanicInfo(ErrorCode::Unsupported, get_index_obj.what());
        }
        PanicInfo(ErrorCode::KnowhereError, get_index_obj.what());
    }
}

template <typename T>
knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorMemIndex<T>::VectorIterators(const milvus::DatasetPtr dataset,
                                   const knowhere::Json& conf,
                                   const milvus::BitsetView& bitset) const {
    return this->index_.AnnIterator(dataset, conf, bitset);
}

template <typename T>
IndexStatsPtr
VectorMemIndex<T>::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    return IndexStats::NewFromSizeMap(file_manager_->GetAddedTotalMemSize(),
                                      remote_paths_to_size);
}

template <typename T>
BinarySet
VectorMemIndex<T>::Serialize(const Config& config) {
    knowhere::BinarySet ret;
    auto stat = index_.Serialize(ret);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to serialize index: {}",
                  KnowhereStatusString(stat));
    Disassemble(ret);

    return ret;
}

template <typename T>
void
VectorMemIndex<T>::LoadWithoutAssemble(const BinarySet& binary_set,
                                       const Config& config) {
    auto stat = index_.Deserialize(binary_set, config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to Deserialize index: {}",
                  KnowhereStatusString(stat));
    SetDim(index_.Dim());
}

template <typename T>
void
VectorMemIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
void
VectorMemIndex<T>::Load(milvus::tracer::TraceContext ctx,
                        const Config& config) {
    if (config.contains(MMAP_FILE_PATH)) {
        return LoadFromFile(config);
    }

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");

    std::unordered_set<std::string> pending_index_files(index_files->begin(),
                                                        index_files->end());

    LOG_INFO("load index files: {}", index_files.value().size());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::map<std::string, FieldDataPtr> index_datas{};

    // try to read slice meta first
    std::string slice_meta_filepath;
    for (auto& file : pending_index_files) {
        auto file_name = file.substr(file.find_last_of('/') + 1);
        if (file_name == INDEX_FILE_SLICE_META) {
            slice_meta_filepath = file;
            pending_index_files.erase(file);
            break;
        }
    }

    // start read file span with active scope
    {
        auto read_file_span =
            milvus::tracer::StartSpan("SegCoreReadIndexFile", &ctx);
        auto read_scope =
            milvus::tracer::GetTracer()->WithActiveSpan(read_file_span);
        LOG_INFO("load with slice meta: {}", !slice_meta_filepath.empty());

        if (!slice_meta_filepath
                 .empty()) {  // load with the slice meta info, then we can load batch by batch
            std::string index_file_prefix = slice_meta_filepath.substr(
                0, slice_meta_filepath.find_last_of('/') + 1);

            auto result = file_manager_->LoadIndexToMemory(
                {slice_meta_filepath}, config[milvus::THREAD_POOL_PRIORITY]);
            auto raw_slice_meta = result[INDEX_FILE_SLICE_META];
            Config meta_data = Config::parse(
                std::string(static_cast<const char*>(raw_slice_meta->Data()),
                            raw_slice_meta->Size()));

            for (auto& item : meta_data[META]) {
                std::string prefix = item[NAME];
                int slice_num = item[SLICE_NUM];
                auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
                auto new_field_data = milvus::storage::CreateFieldData(
                    DataType::INT8, false, 1, total_len);

                std::vector<std::string> batch;
                batch.reserve(slice_num);
                for (auto i = 0; i < slice_num; ++i) {
                    std::string file_name = GenSlicedFileName(prefix, i);
                    batch.push_back(index_file_prefix + file_name);
                }

                auto batch_data = file_manager_->LoadIndexToMemory(
                    batch, config[milvus::THREAD_POOL_PRIORITY]);
                for (const auto& file_path : batch) {
                    const std::string file_name =
                        file_path.substr(file_path.find_last_of('/') + 1);
                    AssertInfo(batch_data.find(file_name) != batch_data.end(),
                               "lost index slice data: {}",
                               file_name);
                    auto data = batch_data[file_name];
                    new_field_data->FillFieldData(data->Data(), data->Size());
                }
                for (auto& file : batch) {
                    pending_index_files.erase(file);
                }

                AssertInfo(
                    new_field_data->IsFull(),
                    "index len is inconsistent after disassemble and assemble");
                index_datas[prefix] = new_field_data;
            }
        }

        if (!pending_index_files.empty()) {
            auto result = file_manager_->LoadIndexToMemory(
                std::vector<std::string>(pending_index_files.begin(),
                                         pending_index_files.end()),
                config[milvus::THREAD_POOL_PRIORITY]);
            for (auto&& index_data : result) {
                index_datas.insert(std::move(index_data));
            }
        }

        read_file_span->End();
    }

    LOG_INFO("construct binary set...");
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        LOG_INFO("add index data to binary set: {}", key);
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        binary_set.Append(key, buf, size);
    }

    // start engine load index span
    auto span_load_engine =
        milvus::tracer::StartSpan("SegCoreEngineLoadIndex", &ctx);
    auto engine_scope =
        milvus::tracer::GetTracer()->WithActiveSpan(span_load_engine);
    LOG_INFO("load index into Knowhere...");
    LoadWithoutAssemble(binary_set, config);
    span_load_engine->End();
    LOG_INFO("load vector index done");
}

template <typename T>
void
VectorMemIndex<T>::BuildWithDataset(const DatasetPtr& dataset,
                                    const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    SetDim(dataset->GetDim());

    knowhere::TimeRecorder rc("BuildWithoutIds", 1);
    auto stat = index_.Build(dataset, index_config, use_knowhere_build_pool_);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to build index, " + KnowhereStatusString(stat));
    rc.ElapseFromBegin("Done");
    SetDim(index_.Dim());
}

template <typename T>
void
VectorMemIndex<T>::Build(const Config& config) {
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when building in memory index");
    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());

    auto opt_fields = GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    std::unordered_map<int64_t, std::vector<std::vector<uint32_t>>> scalar_info;
    auto is_partition_key_isolation =
        GetValueFromConfig<bool>(config, "partition_key_isolation");
    if (opt_fields.has_value() &&
        index_.IsAdditionalScalarSupported(
            is_partition_key_isolation.value_or(false))) {
        scalar_info = file_manager_->CacheOptFieldToMemory(opt_fields.value());
    }

    Config build_config;
    build_config.update(config);
    build_config.erase("insert_files");
    build_config.erase(VEC_OPT_FIELDS);
    if (!IndexIsSparse(GetIndexType())) {
        int64_t total_size = 0;
        int64_t total_num_rows = 0;
        int64_t dim = 0;
        for (auto data : field_datas) {
            total_size += data->Size();
            total_num_rows += data->get_num_rows();
            AssertInfo(dim == 0 || dim == data->get_dim(),
                       "inconsistent dim value between field datas!");
            dim = data->get_dim();
        }

        auto buf = std::shared_ptr<uint8_t[]>(new uint8_t[total_size]);
        int64_t offset = 0;
        // TODO: avoid copying
        for (auto data : field_datas) {
            std::memcpy(buf.get() + offset, data->Data(), data->Size());
            offset += data->Size();
            data.reset();
        }
        field_datas.clear();

        auto dataset = GenDataset(total_num_rows, dim, buf.get());
        if (!scalar_info.empty()) {
            dataset->Set(knowhere::meta::SCALAR_INFO, std::move(scalar_info));
        }
        BuildWithDataset(dataset, build_config);
    } else {
        // sparse
        int64_t total_rows = 0;
        int64_t dim = 0;
        for (auto field_data : field_datas) {
            total_rows += field_data->Length();
            dim = std::max(
                dim,
                std::dynamic_pointer_cast<FieldData<SparseFloatVector>>(
                    field_data)
                    ->Dim());
        }
        std::vector<knowhere::sparse::SparseRow<float>> vec(total_rows);
        int64_t offset = 0;
        for (auto field_data : field_datas) {
            auto ptr = static_cast<const knowhere::sparse::SparseRow<float>*>(
                field_data->Data());
            AssertInfo(ptr, "failed to cast field data to sparse rows");
            for (size_t i = 0; i < field_data->Length(); ++i) {
                // this does a deep copy of field_data's data.
                // TODO: avoid copying by enforcing field data to give up
                // ownership.
                AssertInfo(dim >= ptr[i].dim(), "bad dim");
                vec[offset + i] = ptr[i];
            }
            offset += field_data->Length();
        }
        auto dataset = GenDataset(total_rows, dim, vec.data());
        dataset->SetIsSparse(true);
        if (!scalar_info.empty()) {
            dataset->Set(knowhere::meta::SCALAR_INFO, std::move(scalar_info));
        }
        BuildWithDataset(dataset, build_config);
    }
}

template <typename T>
void
VectorMemIndex<T>::AddWithDataset(const DatasetPtr& dataset,
                                  const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    knowhere::TimeRecorder rc("AddWithDataset", 1);
    auto stat = index_.Add(dataset, index_config, use_knowhere_build_pool_);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to append index, " + KnowhereStatusString(stat));
    rc.ElapseFromBegin("Done");
}

template <typename T>
void
VectorMemIndex<T>::Query(const DatasetPtr dataset,
                         const SearchInfo& search_info,
                         const BitsetView& bitset,
                         SearchResult& search_result) const {
    //    AssertInfo(GetMetricType() == search_info.metric_type_,
    //               "Metric type of field index isn't the same with search info");

    auto num_queries = dataset->GetRows();
    knowhere::Json search_conf = PrepareSearchParams(search_info);
    auto topk = search_info.topk_;
    // TODO :: check dim of search data
    auto final = [&] {
        auto index_type = GetIndexType();
        if (CheckAndUpdateKnowhereRangeSearchParam(
                search_info, topk, GetMetricType(), search_conf)) {
            milvus::tracer::AddEvent("start_knowhere_index_range_search");
            auto res = index_.RangeSearch(dataset, search_conf, bitset);
            milvus::tracer::AddEvent("finish_knowhere_index_range_search");
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          "failed to range search: {}: {}",
                          KnowhereStatusString(res.error()),
                          res.what());
            }
            auto result = ReGenRangeSearchResult(
                res.value(), topk, num_queries, GetMetricType());
            milvus::tracer::AddEvent("finish_ReGenRangeSearchResult");
            return result;
        } else {
            milvus::tracer::AddEvent("start_knowhere_index_search");
            auto res = index_.Search(dataset, search_conf, bitset);
            milvus::tracer::AddEvent("finish_knowhere_index_search");
            if (!res.has_value()) {
                PanicInfo(
                    ErrorCode::UnexpectedError,
                    // escape json brace in case of using message as format
                    "failed to search: config={} {}: {}",
                    milvus::EscapeBraces(search_conf.dump()),
                    KnowhereStatusString(res.error()),
                    res.what());
            }
            return res.value();
        }
    }();

    auto ids = final->GetIds();
    float* distances = const_cast<float*>(final->GetDistance());
    final->SetIsOwner(true);
    auto round_decimal = search_info.round_decimal_;
    auto total_num = num_queries * topk;

    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = std::round(distances[i] * multiplier) / multiplier;
        }
    }
    search_result.seg_offsets_.resize(total_num);
    search_result.distances_.resize(total_num);
    search_result.total_nq_ = num_queries;
    search_result.unity_topK_ = topk;
    std::copy_n(ids, total_num, search_result.seg_offsets_.data());
    std::copy_n(distances, total_num, search_result.distances_.data());
}

template <typename T>
const bool
VectorMemIndex<T>::HasRawData() const {
    return index_.HasRawData(GetMetricType());
}

template <typename T>
std::vector<uint8_t>
VectorMemIndex<T>::GetVector(const DatasetPtr dataset) const {
    auto index_type = GetIndexType();
    if (IndexIsSparse(index_type)) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, index is sparse");
    }

    // if dataset is empty, return empty vector
    if (dataset->GetRows() == 0) {
        return {};
    }

    auto res = index_.GetVectorByIds(dataset);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, " + KnowhereStatusString(res.error()));
    }
    auto tensor = res.value()->GetTensor();
    auto row_num = res.value()->GetRows();
    auto dim = res.value()->GetDim();
    int64_t data_size = milvus::GetVecRowSize<T>(dim) * row_num;
    std::vector<uint8_t> raw_data;
    raw_data.resize(data_size);
    memcpy(raw_data.data(), tensor, data_size);
    return raw_data;
}

template <typename T>
std::unique_ptr<const knowhere::sparse::SparseRow<float>[]>
VectorMemIndex<T>::GetSparseVector(const DatasetPtr dataset) const {
    auto res = index_.GetVectorByIds(dataset);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, " + KnowhereStatusString(res.error()));
    }
    // release and transfer ownership to the result unique ptr.
    res.value()->SetIsOwner(false);
    return std::unique_ptr<const knowhere::sparse::SparseRow<float>[]>(
        static_cast<const knowhere::sparse::SparseRow<float>*>(
            res.value()->GetTensor()));
}

template <typename T>
void VectorMemIndex<T>::LoadFromFile(const Config& config) {
    auto filepath = GetValueFromConfig<std::string>(config, MMAP_FILE_PATH);
    AssertInfo(filepath.has_value(), "mmap filepath is empty when load index");

    std::filesystem::create_directories(
        std::filesystem::path(filepath.value()).parent_path());

    auto file = File::Open(filepath.value(), O_CREAT | O_TRUNC | O_RDWR);

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");

    std::unordered_set<std::string> pending_index_files(index_files->begin(),
                                                        index_files->end());

    LOG_INFO("load index files: {}", index_files.value().size());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    // try to read slice meta first
    std::string slice_meta_filepath;
    for (auto& file : pending_index_files) {
        auto file_name = file.substr(file.find_last_of('/') + 1);
        if (file_name == INDEX_FILE_SLICE_META) {
            slice_meta_filepath = file;
            pending_index_files.erase(file);
            break;
        }
    }

    LOG_INFO("load with slice meta: {}", !slice_meta_filepath.empty());
    std::chrono::duration<double> load_duration_sum;
    std::chrono::duration<double> write_disk_duration_sum;
    if (!slice_meta_filepath
             .empty()) {  // load with the slice meta info, then we can load batch by batch
        std::string index_file_prefix = slice_meta_filepath.substr(
            0, slice_meta_filepath.find_last_of('/') + 1);
        std::vector<std::string> batch{};
        batch.reserve(parallel_degree);

        auto result = file_manager_->LoadIndexToMemory(
            {slice_meta_filepath}, config[milvus::THREAD_POOL_PRIORITY]);
        auto raw_slice_meta = result[INDEX_FILE_SLICE_META];
        Config meta_data = Config::parse(
            std::string(static_cast<const char*>(raw_slice_meta->Data()),
                        raw_slice_meta->Size()));

        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);
            auto HandleBatch = [&](int index) {
                auto start_load2_mem = std::chrono::system_clock::now();
                auto batch_data = file_manager_->LoadIndexToMemory(
                    batch, config[milvus::THREAD_POOL_PRIORITY]);
                load_duration_sum +=
                    (std::chrono::system_clock::now() - start_load2_mem);
                for (int j = index - batch.size() + 1; j <= index; j++) {
                    std::string file_name = GenSlicedFileName(prefix, j);
                    AssertInfo(batch_data.find(file_name) != batch_data.end(),
                               "lost index slice data");
                    auto data = batch_data[file_name];
                    auto start_write_file = std::chrono::system_clock::now();
                    auto written = file.Write(data->Data(), data->Size());
                    write_disk_duration_sum +=
                        (std::chrono::system_clock::now() - start_write_file);
                    AssertInfo(
                        written == data->Size(),
                        fmt::format("failed to write index data to disk {}: {}",
                                    filepath->data(),
                                    strerror(errno)));
                }
                for (auto& file : batch) {
                    pending_index_files.erase(file);
                }
                batch.clear();
            };

            for (auto i = 0; i < slice_num; ++i) {
                std::string file_name = GenSlicedFileName(prefix, i);
                batch.push_back(index_file_prefix + file_name);
                if (batch.size() >= parallel_degree) {
                    HandleBatch(i);
                }
            }
            if (batch.size() > 0) {
                HandleBatch(slice_num - 1);
            }
        }
    } else {
        //1. load files into memory
        auto start_load_files2_mem = std::chrono::system_clock::now();
        auto result = file_manager_->LoadIndexToMemory(
            std::vector<std::string>(pending_index_files.begin(),
                                     pending_index_files.end()),
            config[milvus::THREAD_POOL_PRIORITY]);
        load_duration_sum +=
            (std::chrono::system_clock::now() - start_load_files2_mem);
        //2. write data into files
        auto start_write_file = std::chrono::system_clock::now();
        for (auto& [_, index_data] : result) {
            file.Write(index_data->Data(), index_data->Size());
        }
        write_disk_duration_sum +=
            (std::chrono::system_clock::now() - start_write_file);
    }
    milvus::monitor::internal_storage_download_duration.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(load_duration_sum)
            .count());
    milvus::monitor::internal_storage_write_disk_duration.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            write_disk_duration_sum)
            .count());
    file.Close();

    LOG_INFO("load index into Knowhere...");
    auto conf = config;
    conf.erase(MMAP_FILE_PATH);
    conf[ENABLE_MMAP] = true;
    auto start_deserialize = std::chrono::system_clock::now();
    auto stat = index_.DeserializeFromFile(filepath.value(), conf);
    auto deserialize_duration =
        std::chrono::system_clock::now() - start_deserialize;
    if (stat != knowhere::Status::success) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to Deserialize index: {}",
                  KnowhereStatusString(stat));
    }
    milvus::monitor::internal_storage_deserialize_duration.Observe(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            deserialize_duration)
            .count());

    auto dim = index_.Dim();
    this->SetDim(index_.Dim());

    auto ok = unlink(filepath->data());
    AssertInfo(ok == 0,
               "failed to unlink mmap index file {}: {}",
               filepath.value(),
               strerror(errno));
    LOG_INFO(
        "load vector index done, mmap_file_path:{}, download_duration:{}, "
        "write_files_duration:{}, deserialize_duration:{}",
        filepath.value(),
        std::chrono::duration_cast<std::chrono::milliseconds>(load_duration_sum)
            .count(),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            write_disk_duration_sum)
            .count(),
        std::chrono::duration_cast<std::chrono::milliseconds>(
            deserialize_duration)
            .count());
}

template class VectorMemIndex<float>;
template class VectorMemIndex<bin1>;
template class VectorMemIndex<float16>;
template class VectorMemIndex<bfloat16>;

}  // namespace milvus::index
