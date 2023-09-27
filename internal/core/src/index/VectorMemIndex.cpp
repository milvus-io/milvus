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
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "fmt/format.h"

#include "index/Index.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "common/EasyAssert.h"
#include "config/ConfigKnowhere.h"
#include "knowhere/factory.h"
#include "knowhere/comp/time_recorder.h"
#include "common/BitsetView.h"
#include "common/Slice.h"
#include "common/Consts.h"
#include "common/RangeSearchHelper.h"
#include "common/Utils.h"
#include "log/Log.h"
#include "storage/FieldData.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"
#include "common/File.h"
#include "common/Tracer.h"

namespace milvus::index {

VectorMemIndex::VectorMemIndex(
    const IndexType& index_type,
    const MetricType& metric_type,
    const IndexVersion& version,
    const storage::FileManagerContext& file_manager_context)
    : VectorIndex(index_type, metric_type) {
    AssertInfo(!is_unsupported(index_type, metric_type),
               index_type + " doesn't support metric: " + metric_type);
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
    CheckCompatible(version);
    index_ = knowhere::IndexFactory::Instance().Create(GetIndexType(), version);
}

BinarySet
VectorMemIndex::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

BinarySet
VectorMemIndex::Serialize(const Config& config) {
    knowhere::BinarySet ret;
    auto stat = index_.Serialize(ret);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to serialize index, " + KnowhereStatusString(stat));
    Disassemble(ret);

    return ret;
}

void
VectorMemIndex::LoadWithoutAssemble(const BinarySet& binary_set,
                                    const Config& config) {
    auto stat = index_.Deserialize(binary_set, config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to Deserialize index, " + KnowhereStatusString(stat));
    SetDim(index_.Dim());
}

void
VectorMemIndex::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    LoadWithoutAssemble(binary_set, config);
}

void
VectorMemIndex::Load(const Config& config) {
    if (config.contains(kMmapFilepath)) {
        return LoadFromFile(config);
    }

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load index");

    std::unordered_set<std::string> pending_index_files(index_files->begin(),
                                                        index_files->end());

    LOG_SEGCORE_INFO_ << "load index files: " << index_files.value().size();

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    std::map<std::string, storage::FieldDataPtr> index_datas{};

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

    LOG_SEGCORE_INFO_ << "load with slice meta: "
                      << !slice_meta_filepath.empty();

    if (!slice_meta_filepath
             .empty()) {  // load with the slice meta info, then we can load batch by batch
        std::string index_file_prefix = slice_meta_filepath.substr(
            0, slice_meta_filepath.find_last_of('/') + 1);
        std::vector<std::string> batch{};
        batch.reserve(parallel_degree);

        auto result = file_manager_->LoadIndexToMemory({slice_meta_filepath});
        auto raw_slice_meta = result[INDEX_FILE_SLICE_META];
        Config meta_data = Config::parse(
            std::string(static_cast<const char*>(raw_slice_meta->Data()),
                        raw_slice_meta->Size()));

        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);

            auto new_field_data =
                milvus::storage::CreateFieldData(DataType::INT8, 1, total_len);
            auto HandleBatch = [&](int index) {
                auto batch_data = file_manager_->LoadIndexToMemory(batch);
                for (int j = index - batch.size() + 1; j <= index; j++) {
                    std::string file_name = GenSlicedFileName(prefix, j);
                    AssertInfo(batch_data.find(file_name) != batch_data.end(),
                               "lost index slice data");
                    auto data = batch_data[file_name];
                    new_field_data->FillFieldData(data->Data(), data->Size());
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

            AssertInfo(
                new_field_data->IsFull(),
                "index len is inconsistent after disassemble and assemble");
            index_datas[prefix] = new_field_data;
        }
    }

    if (!pending_index_files.empty()) {
        auto result = file_manager_->LoadIndexToMemory(std::vector<std::string>(
            pending_index_files.begin(), pending_index_files.end()));
        for (auto&& index_data : result) {
            index_datas.insert(std::move(index_data));
        }
    }

    LOG_SEGCORE_INFO_ << "construct binary set...";
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        LOG_SEGCORE_INFO_ << "add index data to binary set: " << key;
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        binary_set.Append(key, buf, size);
    }

    LOG_SEGCORE_INFO_ << "load index into Knowhere...";
    LoadWithoutAssemble(binary_set, config);
    LOG_SEGCORE_INFO_ << "load vector index done";
}

void
VectorMemIndex::BuildWithDataset(const DatasetPtr& dataset,
                                 const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    SetDim(dataset->GetDim());

    knowhere::TimeRecorder rc("BuildWithoutIds", 1);
    auto stat = index_.Build(*dataset, index_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to build index, " + KnowhereStatusString(stat));
    rc.ElapseFromBegin("Done");
    SetDim(index_.Dim());
}

void
VectorMemIndex::Build(const Config& config) {
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build disk ann index");
    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());

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
    for (auto data : field_datas) {
        std::memcpy(buf.get() + offset, data->Data(), data->Size());
        offset += data->Size();
        data.reset();
    }
    field_datas.clear();

    Config build_config;
    build_config.update(config);
    build_config.erase("insert_files");

    auto dataset = GenDataset(total_num_rows, dim, buf.get());
    BuildWithDataset(dataset, build_config);
}

void
VectorMemIndex::AddWithDataset(const DatasetPtr& dataset,
                               const Config& config) {
    knowhere::Json index_config;
    index_config.update(config);

    knowhere::TimeRecorder rc("AddWithDataset", 1);
    auto stat = index_.Add(*dataset, index_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to append index, " + KnowhereStatusString(stat));
    rc.ElapseFromBegin("Done");
}

std::unique_ptr<SearchResult>
VectorMemIndex::Query(const DatasetPtr dataset,
                      const SearchInfo& search_info,
                      const BitsetView& bitset) {
    //    AssertInfo(GetMetricType() == search_info.metric_type_,
    //               "Metric type of field index isn't the same with search info");

    auto num_queries = dataset->GetRows();
    knowhere::Json search_conf = search_info.search_params_;
    auto topk = search_info.topk_;
    // TODO :: check dim of search data
    auto final = [&] {
        search_conf[knowhere::meta::TOPK] = topk;
        search_conf[knowhere::meta::METRIC_TYPE] = GetMetricType();
        auto index_type = GetIndexType();
        if (CheckKeyInConfig(search_conf, RADIUS)) {
            if (CheckKeyInConfig(search_conf, RANGE_FILTER)) {
                CheckRangeSearchParam(search_conf[RADIUS],
                                      search_conf[RANGE_FILTER],
                                      GetMetricType());
            }
            milvus::tracer::AddEvent("start_knowhere_index_range_search");
            auto res = index_.RangeSearch(*dataset, search_conf, bitset);
            milvus::tracer::AddEvent("finish_knowhere_index_range_search");
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to range search: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            auto result = ReGenRangeSearchResult(
                res.value(), topk, num_queries, GetMetricType());
            milvus::tracer::AddEvent("finish_ReGenRangeSearchResult");
            return result;
        } else {
            milvus::tracer::AddEvent("start_knowhere_index_search");
            auto res = index_.Search(*dataset, search_conf, bitset);
            milvus::tracer::AddEvent("finish_knowhere_index_search");
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to search: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
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
    auto result = std::make_unique<SearchResult>();
    result->seg_offsets_.resize(total_num);
    result->distances_.resize(total_num);
    result->total_nq_ = num_queries;
    result->unity_topK_ = topk;

    std::copy_n(ids, total_num, result->seg_offsets_.data());
    std::copy_n(distances, total_num, result->distances_.data());

    return result;
}

const bool
VectorMemIndex::HasRawData() const {
    return index_.HasRawData(GetMetricType());
}

std::vector<uint8_t>
VectorMemIndex::GetVector(const DatasetPtr dataset) const {
    auto res = index_.GetVectorByIds(*dataset);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, " + KnowhereStatusString(res.error()));
    }
    auto index_type = GetIndexType();
    auto tensor = res.value()->GetTensor();
    auto row_num = res.value()->GetRows();
    auto dim = res.value()->GetDim();
    int64_t data_size;
    if (is_in_bin_list(index_type)) {
        data_size = dim / 8 * row_num;
    } else {
        data_size = dim * row_num * sizeof(float);
    }
    std::vector<uint8_t> raw_data;
    raw_data.resize(data_size);
    memcpy(raw_data.data(), tensor, data_size);
    return raw_data;
}
void
VectorMemIndex::LoadFromFile(const Config& config) {
    auto filepath = GetValueFromConfig<std::string>(config, kMmapFilepath);
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

    LOG_SEGCORE_INFO_ << "load index files: " << index_files.value().size();

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

    LOG_SEGCORE_INFO_ << "load with slice meta: "
                      << !slice_meta_filepath.empty();

    if (!slice_meta_filepath
             .empty()) {  // load with the slice meta info, then we can load batch by batch
        std::string index_file_prefix = slice_meta_filepath.substr(
            0, slice_meta_filepath.find_last_of('/') + 1);
        std::vector<std::string> batch{};
        batch.reserve(parallel_degree);

        auto result = file_manager_->LoadIndexToMemory({slice_meta_filepath});
        auto raw_slice_meta = result[INDEX_FILE_SLICE_META];
        Config meta_data = Config::parse(
            std::string(static_cast<const char*>(raw_slice_meta->Data()),
                        raw_slice_meta->Size()));

        for (auto& item : meta_data[META]) {
            std::string prefix = item[NAME];
            int slice_num = item[SLICE_NUM];
            auto total_len = static_cast<size_t>(item[TOTAL_LEN]);

            auto HandleBatch = [&](int index) {
                auto batch_data = file_manager_->LoadIndexToMemory(batch);
                for (int j = index - batch.size() + 1; j <= index; j++) {
                    std::string file_name = GenSlicedFileName(prefix, j);
                    AssertInfo(batch_data.find(file_name) != batch_data.end(),
                               "lost index slice data");
                    auto data = batch_data[file_name];
                    auto written = file.Write(data->Data(), data->Size());
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
        auto result = file_manager_->LoadIndexToMemory(std::vector<std::string>(
            pending_index_files.begin(), pending_index_files.end()));
        for (auto& [_, index_data] : result) {
            file.Write(index_data->Data(), index_data->Size());
        }
    }
    file.Close();

    LOG_SEGCORE_INFO_ << "load index into Knowhere...";
    auto conf = config;
    conf.erase(kMmapFilepath);
    conf[kEnableMmap] = true;
    auto stat = index_.DeserializeFromFile(filepath.value(), conf);
    if (stat != knowhere::Status::success) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to Deserialize index: {}",
                              KnowhereStatusString(stat)));
    }

    auto dim = index_.Dim();
    this->SetDim(index_.Dim());

    auto ok = unlink(filepath->data());
    AssertInfo(ok == 0,
               fmt::format("failed to unlink mmap index file {}: {}",
                           filepath.value(),
                           strerror(errno)));
    LOG_SEGCORE_INFO_ << "load vector index done";
}

}  // namespace milvus::index
