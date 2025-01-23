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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/filesystem/path.hpp"
#include "storage/MemFileManagerImpl.h"
#include "pb/clustering.pb.h"
#include "knowhere/cluster/cluster_factory.h"

namespace milvus::clustering {

// after clustering result uploaded, return result meta for golang usage
struct ClusteringResultMeta {
    std::string centroid_path;   // centroid result path
    int64_t centroid_file_size;  // centroid result size
    std::unordered_map<std::string, int64_t>
        id_mappings;  // id mapping result path/size for each segment
};

class KmeansClustering {
 public:
    explicit KmeansClustering(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    // every time is a brand new kmeans training
    template <typename T>
    void
    Run(const milvus::proto::clustering::AnalyzeInfo& config);

    // should never be called before run
    ClusteringResultMeta
    GetClusteringResultMeta() {
        if (!is_runned_) {
            throw SegcoreError(
                ErrorCode::UnexpectedError,
                "clustering result is not ready before kmeans run");
        }
        return cluster_result_;
    }

    inline std::string
    GetRemoteCentroidsObjectPrefix() const {
        auto index_meta_ = file_manager_->GetIndexMeta();
        auto field_meta_ = file_manager_->GetFieldDataMeta();
        boost::filesystem::path prefix =
            file_manager_->GetChunkManager()->GetRootPath();
        boost::filesystem::path path = std::string(ANALYZE_ROOT_PATH);
        boost::filesystem::path path1 =
            std::to_string(index_meta_.build_id) + "/" +
            std::to_string(index_meta_.index_version) + "/" +
            std::to_string(field_meta_.collection_id) + "/" +
            std::to_string(field_meta_.partition_id) + "/" +
            std::to_string(field_meta_.field_id);
        return (prefix / path / path1).string();
    }

    inline std::string
    GetRemoteCentroidIdMappingObjectPrefix(int64_t segment_id) const {
        auto index_meta_ = file_manager_->GetIndexMeta();
        auto field_meta_ = file_manager_->GetFieldDataMeta();
        boost::filesystem::path prefix =
            file_manager_->GetChunkManager()->GetRootPath();
        boost::filesystem::path path = std::string(ANALYZE_ROOT_PATH);
        boost::filesystem::path path1 =
            std::to_string(index_meta_.build_id) + "/" +
            std::to_string(index_meta_.index_version) + "/" +
            std::to_string(field_meta_.collection_id) + "/" +
            std::to_string(field_meta_.partition_id) + "/" +
            std::to_string(field_meta_.field_id) + "/" +
            std::to_string(segment_id);
        return (prefix / path / path1).string();
    }

    ~KmeansClustering() = default;

 private:
    template <typename T>
    void
    StreamingAssignandUpload(
        knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
        const milvus::proto::clustering::AnalyzeInfo& config,
        const milvus::proto::clustering::ClusteringCentroidsStats&
            centroid_stats,
        const std::vector<
            milvus::proto::clustering::ClusteringCentroidIdMappingStats>&
            id_mapping_stats,
        const std::vector<int64_t>& segment_ids,
        const std::map<int64_t, std::vector<std::string>>& insert_files,
        const std::map<int64_t, int64_t>& num_rows,
        const int64_t dim,
        const int64_t trained_segments_num,
        const int64_t num_clusters);

    template <typename T>
    void
    FetchDataFiles(uint8_t* buf,
                   const int64_t expected_train_size,
                   const int64_t expected_remote_file_size,
                   const std::vector<std::string>& files,
                   const int64_t dim,
                   int64_t& offset);

    // given all possible segments, sample data to buffer
    template <typename T>
    void
    SampleTrainData(
        const std::vector<int64_t>& segment_ids,
        const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
        const std::map<int64_t, int64_t>& segment_num_rows,
        const int64_t expected_train_size,
        const int64_t dim,
        const bool random_sample,
        uint8_t* buf);

    // transform centroids result to PB format for future usage of golang side
    template <typename T>
    milvus::proto::clustering::ClusteringCentroidsStats
    CentroidsToPB(const T* centroids,
                  const int64_t num_clusters,
                  const int64_t dim);

    // transform flattened id mapping result to several PB files by each segment for future usage of golang side
    std::vector<milvus::proto::clustering::ClusteringCentroidIdMappingStats>
    CentroidIdMappingToPB(const uint32_t* centroid_id_mapping,
                          const std::vector<int64_t>& segment_ids,
                          const int64_t trained_segments_num,
                          const std::map<int64_t, int64_t>& num_row_map,
                          const int64_t num_clusters);

    template <typename T>
    bool
    IsDataSkew(const milvus::proto::clustering::AnalyzeInfo& config,
               const int64_t dim,
               std::vector<int64_t>& num_in_each_centroid);

    std::unique_ptr<storage::MemFileManagerImpl> file_manager_;
    ClusteringResultMeta cluster_result_;
    bool is_runned_ = false;
    std::string msg_header_;
};

using KmeansClusteringPtr = std::unique_ptr<KmeansClustering>;
}  // namespace milvus::clustering
