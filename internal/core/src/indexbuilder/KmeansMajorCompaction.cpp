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

#include "index/VectorDiskIndex.h"

#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "config/ConfigKnowhere.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/kmeans.h"
#include "indexbuilder/KmeansMajorCompaction.h"
#include "indexbuilder/MajorCompaction.h"
#include "segcore/SegcoreConfig.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/Util.h"
#include "common/Consts.h"
#include "common/RangeSearchHelper.h"
#include "indexbuilder/types.h"
#include <random>

namespace milvus::indexbuilder {

template <typename T>
KmeansMajorCompaction<T>::KmeansMajorCompaction(
    Config& config, const storage::FileManagerContext& file_manager_context)
    : config_(config) {
    file_manager_ =
        std::make_shared<storage::DiskFileManagerImpl>(file_manager_context);
    AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_compaction_raw_data_path_prefix =
        file_manager_->GetCompactionRawDataObjectPrefix();

    if (local_chunk_manager->Exist(local_compaction_raw_data_path_prefix)) {
        LOG_INFO("path {} already exists, delete it",
                 local_compaction_raw_data_path_prefix);
        local_chunk_manager->RemoveDir(local_compaction_raw_data_path_prefix);
    }
    local_chunk_manager->CreateDir(local_compaction_raw_data_path_prefix);
}

template <typename T>
std::unique_ptr<T[]>
KmeansMajorCompaction<T>::Sample(const std::vector<std::string>& file_paths,
                                 const std::vector<uint64_t>& file_sizes,
                                 uint64_t train_size,
                                 uint64_t total_size) {
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    // train data fits in memory, read by sequence and generate centroids and id_mapping in one pass
    if (train_size >= total_size) {
        auto buf = std::unique_ptr<T[]>(new T[total_size / sizeof(T)]);
        int64_t offset = 0;
        for (int i = 0; i < file_paths.size(); i++) {
            local_chunk_manager->Read(
                file_paths[i],
                0,
                reinterpret_cast<char*>(buf.get()) + offset,
                file_sizes[i]);
            offset += file_sizes[i];
        }
        return buf;
    }
    // random sampling to get train data
    std::vector<int32_t> idx(file_paths.size());
    std::iota(idx.begin(), idx.end(), 0);
    std::shuffle(idx.begin(), idx.end(), std::mt19937());
    int selected_size = 0;
    auto buf = std::unique_ptr<T[]>(new T[train_size / sizeof(T)]);
    int64_t offset = 0;

    for (int i = 0; i < file_paths.size(); i++) {
        if (selected_size < train_size &&
            selected_size + file_sizes[idx[i]] >= train_size) {
            auto cur_size = train_size - selected_size;
            local_chunk_manager->Read(
                file_paths[idx[i]],
                0,
                reinterpret_cast<char*>(buf.get()) + offset,
                cur_size);
            break;
        } else {
            selected_size += file_sizes[idx[i]];
            local_chunk_manager->Read(
                file_paths[idx[i]],
                0,
                reinterpret_cast<char*>(buf.get()) + offset,
                file_sizes[idx[i]]);
            offset += file_sizes[idx[i]];
        }
    }
    return buf;
}

template <typename T>
BinarySet
KmeansMajorCompaction<T>::Upload() {
    BinarySet ret;

    std::unordered_map<std::string, int64_t> remote_paths_to_size;
    file_manager_->AddCompactionResultFiles(result_files_,
                                            remote_paths_to_size);
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

void
WritePBFile(google::protobuf::Message& message, std::string& file_path) {
    std::ofstream outfile;
    outfile.open(file_path.data(), std::ios_base::out | std::ios_base::binary);
    if (outfile.fail()) {
        std::stringstream err_msg;
        err_msg << "Error: open local file '" << file_path << " failed, "
                << strerror(errno);
        throw SegcoreError(FileOpenFailed, err_msg.str());
    }

    outfile.seekp(0, std::ios::beg);
    if (!message.SerializeToOstream(&outfile)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << file_path << " failed, "
                << strerror(errno);
        throw SegcoreError(FileWriteFailed, err_msg.str());
    }
}

template <typename T>
void
KmeansMajorCompaction<T>::Train() {
    if constexpr (!std::is_same_v<T, float>) {
        PanicInfo(
            ErrorCode::UnexpectedError,
            fmt::format("kmeans major compaction only supports float32 now"));
    }
    auto insert_files = milvus::index::GetValueFromConfig<
        std::map<int64_t, std::vector<std::string>>>(config_, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when major compaction");
    auto segment_size =
        milvus::index::GetValueFromConfig<int64_t>(config_, "segment_size");
    AssertInfo(segment_size.has_value(),
               "segment size is empty when major compaction");
    auto train_size =
        milvus::index::GetValueFromConfig<uint64_t>(config_, "train_size");
    AssertInfo(train_size.has_value(),
               "train size is empty when major compaction");

    std::vector<std::string> data_files;
    std::vector<uint64_t> offsets;
    uint32_t dim = 0;
    auto data_size = file_manager_->CacheCompactionRawDataToDisk(
        insert_files.value(), data_files, offsets, dim);
    AssertInfo(data_files.size() == offsets.size(),
               fmt::format("file path num {} and file size {} not equal",
                           data_files.size(),
                           offsets.size()));
    auto data_num = data_size / sizeof(T) / dim;

    auto train_num = train_size.value() * 1024 * 1024 * 1024 / sizeof(T) / dim;

    // make train num equal to data num
    if (train_num >= data_num) {
        train_num = data_num;
    }
    auto train_size_new = train_num * dim * sizeof(T);
    auto buf = Sample(data_files, offsets, train_size_new, data_size);
    auto dataset = GenDataset(train_num, dim, buf.release());

    // get num of clusters by whole data size / segment size
    int num_clusters =
        DIV_ROUND_UP(data_size, (segment_size.value() * 1024 * 1024));
    auto res =
        knowhere::kmeans::ClusteringMajorCompaction<T>(*dataset, num_clusters);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to kmeans train: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    dataset.reset();  // release train data
    auto centroids = reinterpret_cast<const T*>(res.value()->GetTensor());
    auto centroid_id_mapping =
        reinterpret_cast<const uint32_t*>(res.value()->GetCentroidIdMapping());

    auto total_num = res.value()->GetRows();

    // write centroids and centroid_id_mapping to file
    milvus::proto::segcore::ClusteringCentroidsStats stats;

    for (int i = 0; i < num_clusters; i++) {
        milvus::proto::schema::VectorField* vector_field =
            stats.add_centroids();
        vector_field->set_dim(dim);
        milvus::proto::schema::FloatArray* float_array =
            vector_field->mutable_float_vector();
        for (int j = 0; j < dim; j++) {
            float_array->add_data(float(centroids[i * dim + j]));
        }
    }
    auto output_path = file_manager_->GetCompactionResultObjectPrefix();

    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    if (local_chunk_manager->Exist(output_path)) {
        LOG_INFO("path {} already exists, delete it", output_path);
        local_chunk_manager->RemoveDir(output_path);
    }
    local_chunk_manager->CreateDir(output_path);
    std::string centroid_stats_path = output_path + "centroids";
    result_files_.emplace_back(centroid_stats_path);
    WritePBFile(stats, centroid_stats_path);

    auto compute_num_in_centroid = [&](const uint32_t* centroid_id_mapping,
                                       uint64_t start,
                                       uint64_t end) -> std::vector<int64_t> {
        std::vector<int64_t> num_vectors(num_clusters, 0);
        for (uint64_t i = start; i < end; ++i) {
            num_vectors[centroid_id_mapping[i]]++;
        }
        return num_vectors;
    };

    if (train_num >= data_num) {  // do not compute id_mapping again
        uint64_t i = 0;
        uint64_t cur_offset = 0;
        for (auto it = insert_files.value().begin();
             it != insert_files.value().end();
             it++) {
            milvus::proto::segcore::ClusteringCentroidIdMappingStats stats;
            // write centroid_id_mapping by file sizes
            uint64_t num_offset = offsets[i] / sizeof(T) / dim;
            for (uint64_t j = 0; j < num_offset; j++) {
                stats.add_centroid_id_mapping(
                    centroid_id_mapping[cur_offset + j]);
            }
            cur_offset += num_offset;
            auto num_vectors =
                compute_num_in_centroid(centroid_id_mapping, 0, total_num);
            for (uint64_t j = 0; j < num_clusters; j++) {
                stats.add_num_in_centroid(num_vectors[j]);
            }
            std::string id_mapping_path =
                output_path + std::to_string(it->first);
            result_files_.emplace_back(id_mapping_path);
            WritePBFile(stats, id_mapping_path);
            i++;
        }
    } else {
        uint64_t i = 0;
        uint64_t start = 0;
        uint64_t gather_size = 0;
        // choose half of train size as a group to compute centroids
        uint64_t group_size = train_size_new / 2;
        std::vector<int64_t> gather_segment_id;
        for (auto it = insert_files.value().begin();
             it != insert_files.value().end();
             it++) {
            gather_segment_id.emplace_back(it->first);
            gather_size += offsets[i];

            if (gather_size > group_size) {
                auto buf = std::unique_ptr<T[]>(new T[gather_size / sizeof(T)]);
                uint64_t cur_offset = 0;
                for (uint64_t j = start; j <= i; ++j) {
                    local_chunk_manager->Read(
                        data_files[j],
                        0,
                        reinterpret_cast<char*>(buf.get()) + cur_offset,
                        offsets[j]);
                    cur_offset += offsets[j];
                }
                auto dataset = GenDataset(
                    gather_size / sizeof(T) / dim, dim, buf.release());
                auto res = knowhere::kmeans::ClusteringDataAssign<T>(
                    *dataset, centroids, num_clusters);
                if (!res.has_value()) {
                    PanicInfo(ErrorCode::UnexpectedError,
                              fmt::format("failed to kmeans train: {}: {}",
                                          KnowhereStatusString(res.error()),
                                          res.what()));
                }
                dataset.reset();
                auto centroid_id_mapping = reinterpret_cast<const uint32_t*>(
                    res.value()->GetCentroidIdMapping());

                cur_offset = 0;

                for (uint64_t j = start; j <= i; ++j) {
                    uint64_t num_offset = offsets[j] / sizeof(T) / dim;

                    milvus::proto::segcore::ClusteringCentroidIdMappingStats
                        stats;
                    for (uint64_t k = 0; k < num_offset; k++) {
                        stats.add_centroid_id_mapping(
                            centroid_id_mapping[cur_offset + k]);
                    }
                    auto num_vectors =
                        compute_num_in_centroid(centroid_id_mapping,
                                                cur_offset,
                                                cur_offset + num_offset);
                    cur_offset += num_offset;
                    for (uint64_t k = 0; k < num_clusters; k++) {
                        stats.add_num_in_centroid(num_vectors[k]);
                    }
                    std::string id_mapping_path =
                        output_path + std::to_string(gather_segment_id[j]);
                    result_files_.emplace_back(id_mapping_path);
                    WritePBFile(stats, id_mapping_path);
                }
                start = i + 1;
                gather_size = 0;
            }
            gather_size += offsets[i];
            i++;
        }
    }

    // remove raw data since it is not used anymore
    // keep the result file and leave the ownership to golang side
    auto local_compaction_raw_data_path_prefix =
        file_manager_->GetCompactionRawDataObjectPrefix();

    if (local_chunk_manager->Exist(local_compaction_raw_data_path_prefix)) {
        LOG_INFO("delete major compaction raw data dir: {}",
                 local_compaction_raw_data_path_prefix);
        local_chunk_manager->RemoveDir(local_compaction_raw_data_path_prefix);
    }
}

template class KmeansMajorCompaction<float>;

}  // namespace milvus::indexbuilder
