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
#include "clustering/Clustering.h"
#include "knowhere/cluster/cluster_factory.h"

namespace milvus::clustering {

template <typename T>
class KmeansClustering : public Clustering {
 public:
    explicit KmeansClustering(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    void
    Run(const Config& config) override;

    ClusteringResultMeta
    GetClusteringResultMeta() override {
        return cluster_result_;
    }

 private:
    void
    StreamingAssignandUpload(
        const T* centroids,
        const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
        const std::map<int64_t, int64_t>& segment_num_rows);

    // bool to indicate if the whole segment data are fetched
    bool
    FetchSegmentData(uint8_t* buf,
                     int64_t expected_train_size,
                     const std::vector<std::string>& files,
                     const int64_t num_rows,
                     int64_t& offset);

    // given all possible segments, sample data to buffer
    std::unique_ptr<uint8_t[]>
    SampleTrainData(
        const std::vector<int64_t>& segment_ids,
        const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
        const std::map<int64_t, int64_t>& segment_num_rows,
        int64_t expected_train_num);

    // transform centroids result to PB format for future usage of golang side
    milvus::proto::segcore::ClusteringCentroidsStats
    CentroidsToPB(const T* centroids);

    // transform flattened id mapping result to several PB files by each segment for future usage of golang side
    std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
    CentroidIdMappingToPB(const uint32_t* centroid_id_mapping,
                          const std::vector<int64_t>& segment_ids,
                          const std::map<int64_t, int64_t>& num_row_map);

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

    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
    uint32_t num_clusters_;
    int64_t dim_;

    // segment used for kmeans train and group together to call knowhere and get results
    // segment_id -> kmeans result offset, to know the id mapping result of cur segment
    std::unordered_map<int64_t, int64_t> trained_segmentid_to_offset_;
    // segment_ids, is by trained sequence
    std::vector<int64_t> trained_segment_ids_;
    milvus::proto::segcore::ClusteringCentroidsStats centroid_stats_;
    std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
        id_mapping_stats_;
    ClusteringResultMeta cluster_result_;

    knowhere::Cluster<knowhere::ClusterNode> cluster_node_;
};

template <typename T>
using KmeansClusteringPtr = std::unique_ptr<KmeansClustering<T>>;
}  // namespace milvus::clustering
