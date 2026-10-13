// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "clustering/snapshot_index_c.h"
#include "clustering/CentroidDataset.h"
#include "common/CGoCatch.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/FieldMeta.h"
#include "common/Utils.h"
#include "common/protobuf_utils.h"
#include "index/IndexTypeAdapter.h"
#include "index/ParamUtils.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "index/Utils.h"
#include "pb/index_cgo_msg.pb.h"
#include "segcore/Types.h"
#include "storage/Util.h"
#include "storage/PluginLoader.h"
#include "storage/LocalChunkManager.h"
#include "storage/artifact/FileSink.h"

// The same conversion used by segment index builds.
milvus::storage::StorageConfig
get_storage_config(const milvus::proto::indexcgo::StorageConfig& config);

CStatus
BuildSnapshotIndex(ProtoLayoutInterface result,
                   const uint8_t* build_blob,
                   uint64_t build_size,
                   const uint8_t* centroid_blob,
                   uint64_t centroid_size,
                   const char* prefix) {
    using namespace milvus;
    try {
        proto::indexcgo::BuildIndexInfo build;
        proto::clustering::ClusteringCentroidsStats centroids;
        AssertInfo(build.ParseFromArray(build_blob, build_size) &&
                       centroids.ParseFromArray(centroid_blob, centroid_size),
                   "invalid snapshot index protobuf");
        AssertInfo(prefix && *prefix && build.segmentid() == 0 &&
                       build.buildid() == 0 && build.index_version() > 0 &&
                       build.current_index_version() > 0,
                   "invalid snapshot index ownership");
        auto type = static_cast<DataType>(build.field_schema().data_type());
        AssertInfo(type == DataType::VECTOR_FLOAT,
                   "unsupported snapshot index field type");
        auto dataset = clustering::BuildCentroidDataset(centroids);
        AssertInfo(dataset->GetDim() == build.dim() &&
                       dataset->GetRows() == build.num_rows(),
                   "snapshot index shape differs from cluster table");
        auto params = RepeatedKeyValToMap(build.index_params());
        auto config = index::ParseConfigFromIndexParams(params);
        config[knowhere::meta::DIM] = build.dim();
        config[index::INDEX_ENGINE_VERSION] =
            std::to_string(build.current_index_version());
        auto adapted = index::AdaptIndexType({
            .index_type = params.at("index_type"),
            .field_type = type,
            .index_engine_version = build.current_index_version(),
            .params = std::move(config),
        });
        auto storage_config = get_storage_config(build.storage_config());
        auto cm = storage::CreateChunkManager(storage_config);
        auto fs = storage::InitArrowFileSystem(storage_config);
        storage::FieldDataMeta field{build.collectionid(),
                                     build.partitionid(),
                                     0,
                                     build.field_schema().fieldid(),
                                     build.field_schema()};
        storage::IndexMeta meta{
            0, build.field_schema().fieldid(), 0, build.index_version()};
        storage::FileManagerContext files(field, meta, cm, fs);
        files.index_remote_prefix = prefix;
        files.index_local_prefix =
            "snapshot_" + std::to_string(build.collectionid()) + "_" +
            std::to_string(build.partitionid()) + "_" +
            std::to_string(build.index_version()) + "_" +
            std::to_string(build.field_schema().fieldid());
        if (build.has_storage_plugin_context()) {
            const auto& plugin = build.storage_plugin_context();
            files.set_plugin_context(
                storage::PluginLoader::GetInstance()
                    .registerCipherPluginContext(plugin.encryption_zone_id(),
                                                 plugin.collection_id(),
                                                 plugin.encryption_key()));
        }
        storage::ArtifactPtr artifact;
        if (adapted.family == index::families::kVectorDisk) {
            const auto local =
                storage::LocalChunkManagerSingleton::GetInstance()
                    .GetChunkManager();
            adapted.params["local_dir"] = local->GetRootPath();
            indexbuilder::VectorDiskBuildMaterializer materializer(
                local->GetRootPath(),
                type,
                adapted.value_type,
                build.dim(),
                false,
                build.num_rows(),
                adapted.family,
                adapted.params);
            AssertInfo(materializer.InputSpec().side_inputs.empty(),
                       "snapshot index cannot require segment side fields");
            auto batch = storage::CreateFieldData(
                type, DataType::NONE, false, build.dim(), build.num_rows());
            batch->FillFieldData(dataset->GetTensor(), build.num_rows());
            materializer.Add(batch);
            materializer.FinishPrimary();
            artifact = std::move(materializer).Build();
        } else {
            using Input = index::VectorBuildInput<float>;
            auto builder = index::BuilderRegistry<Input>::Instance().Create(
                adapted.family, adapted.params);
            AssertInfo(builder && builder->InputSpec().side_inputs.empty(),
                       "unsupported snapshot index builder or side fields");
            Input input{
                .physical_values = {static_cast<const float*>(
                                        dataset->GetTensor()),
                                    static_cast<size_t>(build.num_rows()) *
                                        build.dim()},
                .logical_rows = build.num_rows(),
                .physical_rows = build.num_rows(),
                .dim = build.dim(),
            };
            artifact = std::move(*builder).Build(input);
        }
        storage::V1DiskSink sink(files);
        artifact->Serialize(sink);
        auto stats = sink.Finish();
        sink.ReleaseLocalStaging();
        AssertInfo(!stats.Files().empty() && stats.MemSize() > 0,
                   "snapshot index upload did not produce complete artifacts");
        proto::clustering::ClusteringIndexInfo output;
        *output.mutable_index_params() = build.index_params();
        for (const auto& file : stats.Files())
            output.add_index_file_paths(file.file_name);
        output.set_index_engine_version(build.current_index_version());
        output.set_index_size(stats.MemSize());
        AssertInfo(reinterpret_cast<milvus::ProtoLayout*>(result)
                       ->SerializeAndHoldProto(output),
                   "cannot serialize snapshot index receipt");
        return SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}

CStatus
SearchSnapshotIndex(CLoadIndexInfo handle,
                    const uint8_t* queries,
                    uint64_t query_bytes,
                    int64_t nq,
                    int64_t topk,
                    const char* search_params,
                    int64_t* labels) {
    using namespace milvus;
    try {
        auto info = static_cast<segcore::LoadIndexInfo*>(handle);
        AssertInfo(info && info->partition_stats_version > 0 &&
                       info->cache_index && queries && labels && nq > 0 &&
                       topk > 0 && topk <= info->num_rows && info->dim > 0,
                   "invalid snapshot index query");
        AssertInfo(info->field_type == DataType::VECTOR_FLOAT,
                   "unsupported snapshot query type");
        AssertInfo(
            static_cast<uint64_t>(nq) <= std::numeric_limits<uint64_t>::max() /
                                             info->dim / sizeof(float) &&
                query_bytes ==
                    static_cast<uint64_t>(nq) * info->dim * sizeof(float),
            "snapshot query shape differs");
        auto accessor =
            SemiInlineGet(info->cache_index->PinCells(nullptr, {0}));
        const auto* base = accessor->get_cell_of(0);
        const auto* reader = dynamic_cast<const index::IVectorReader*>(base);
        AssertInfo(reader && base->Count() == info->num_rows,
                   "snapshot index count differs from metadata");
        index::VectorSearchParams search;
        search.topk_ = topk;
        search.metric_type_ = info->index_params.at("metric_type");
        search.search_params_ = knowhere::Json::parse(search_params);
        SearchResult result;
        auto dataset = knowhere::GenDataSet(nq, info->dim, queries);
        reader->Search(dataset, search, BitsetView{}, nullptr, result);
        AssertInfo(result.seg_offsets_.size() == static_cast<size_t>(nq) * topk,
                   "snapshot index returned an invalid result shape");
        std::copy(
            result.seg_offsets_.begin(), result.seg_offsets_.end(), labels);
        return SuccessCStatus();
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
