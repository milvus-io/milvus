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

#include <map>
#include <string>
#include <utility>

#include "common/IndexMeta.h"
#include "pb/segcore.pb.h"

namespace milvus::segcore {

// BuildSegmentIndexMeta derives a segment's index configuration from its own load
// info, replacing the collection-wide copy segments used to be handed at creation.
//
// That copy was a per-node mutable global refreshed out of band, and every consumer
// of it turned out to have a local source: search resolves the metric from the
// segment's loaded index, and interim-index construction needs only the per-field
// index params, which travel with the segment in SegmentLoadInfo.index_infos.
//
// max_index_row_cnt comes from the segment's own load info. It scales the
// interim-index build threshold (VecIndexConfig::GetBuildThreshold), and
// IndexingRecord additionally refuses to build anything when it is not positive,
// so it cannot simply be dropped: leaving it at 0 silently disables the growing
// interim index entirely. QueryNode computes it from the schema and DataCoord's
// segment size budget -- the same expression the collection-wide index meta used
// -- and stamps it onto SegmentLoadInfo.
inline IndexMetaPtr
BuildSegmentIndexMeta(const milvus::proto::segcore::SegmentLoadInfo* load_info) {
    std::map<FieldId, FieldIndexMeta> field_metas;
    if (load_info != nullptr) {
        for (const auto& index_info : load_info->index_infos()) {
            std::map<std::string, std::string> index_params;
            for (const auto& kv : index_info.index_params()) {
                index_params.emplace(kv.key(), kv.value());
            }
            if (index_params.empty()) {
                continue;
            }
            auto field_id = FieldId(index_info.fieldid());
            field_metas.emplace(field_id,
                                FieldIndexMeta(field_id,
                                               std::move(index_params),
                                               /*type_params=*/{}));
        }
    }
    int64_t max_index_row_cnt =
        load_info != nullptr ? load_info->max_index_row_count() : 0;
    return std::make_shared<CollectionIndexMeta>(max_index_row_cnt,
                                                 std::move(field_metas));
}

}  // namespace milvus::segcore
