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
#include <unordered_map>
#include <vector>

#include "storage/MemFileManagerImpl.h"
#include "storage/space.h"
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
    Run(const Config& config);

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

    // ut
    inline std::string
    GetRemoteCentroidsObjectPrefix() const {
        auto index_meta_ = file_manager_->GetIndexMeta();
        auto field_meta_ = file_manager_->GetFieldDataMeta();
        return file_manager_->GetChunkManager()->GetRootPath() + "/" +
               std::string(ANALYZE_ROOT_PATH) + "/" +
               std::to_string(index_meta_.build_id) + "/" +
               std::to_string(index_meta_.index_version) + "/" +
               std::to_string(field_meta_.collection_id) + "/" +
               std::to_string(field_meta_.partition_id) + "/" +
               std::to_string(field_meta_.field_id);
    }

    inline std::string
    GetRemoteCentroidIdMappingObjectPrefix(int64_t segment_id) const {
        auto index_meta_ = file_manager_->GetIndexMeta();
        auto field_meta_ = file_manager_->GetFieldDataMeta();
        return file_manager_->GetChunkManager()->GetRootPath() + "/" +
               std::string(ANALYZE_ROOT_PATH) + "/" +
               std::to_string(index_meta_.build_id) + "/" +
               std::to_string(index_meta_.index_version) + "/" +
               std::to_string(field_meta_.collection_id) + "/" +
               std::to_string(field_meta_.partition_id) + "/" +
               std::to_string(field_meta_.field_id) + "/" +
               std::to_string(segment_id);
    }

    ~KmeansClustering() = default;

 private:
    template <typename T>
    void
    StreamingAssignandUpload(
        knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
        const milvus::proto::segcore::ClusteringCentroidsStats& centroid_stats,
        const std::vector<
            milvus::proto::segcore::ClusteringCentroidIdMappingStats>&
            id_mapping_stats,
        const std::vector<int64_t>& segment_ids,
        const std::map<int64_t, std::vector<std::string>>& insert_files,
        const std::map<int64_t, int64_t>& num_rows,
        const int64_t dim,
        const int64_t trained_segments_num,
        const int64_t num_clusters);

    // bool to indicate if the whole segment data are fetched
    template <typename T>
    bool
    FetchSegmentData(uint8_t* buf,
                     const int64_t expected_train_size,
                     const std::vector<std::string>& files,
                     const int64_t num_rows,
                     const int64_t dim,
                     int64_t& offset);

    // given all possible segments, sample data to buffer
    template <typename T>
    int64_t
    SampleTrainData(
        const std::vector<int64_t>& segment_ids,
        const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
        const std::map<int64_t, int64_t>& segment_num_rows,
        const int64_t expected_train_size,
        const int64_t dim,
        uint8_t* buf);

    // transform centroids result to PB format for future usage of golang side
    template <typename T>
    milvus::proto::segcore::ClusteringCentroidsStats
    CentroidsToPB(const T* centroids,
                  const int64_t num_clusters,
                  const int64_t dim);

    // transform flattened id mapping result to several PB files by each segment for future usage of golang side
    std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
    CentroidIdMappingToPB(const uint32_t* centroid_id_mapping,
                          const std::vector<int64_t>& segment_ids,
                          const int64_t trained_segments_num,
                          const std::map<int64_t, int64_t>& num_row_map,
                          const int64_t num_clusters);

    std::unique_ptr<storage::MemFileManagerImpl> file_manager_;
    ClusteringResultMeta cluster_result_;
    bool is_runned_ = false;
};

using KmeansClusteringPtr = std::unique_ptr<KmeansClustering>;
}  // namespace milvus::clustering
