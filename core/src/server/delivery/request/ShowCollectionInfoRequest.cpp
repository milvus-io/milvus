// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "server/delivery/request/ShowCollectionInfoRequest.h"
#include "server/DBWrapper.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"
#include "utils/ValidationUtil.h"

#include <memory>
#include <vector>

namespace milvus {
namespace server {

void
ConstructPartitionStat(const engine::PartitionStat& partition_stat, PartitionStat& req_partition_stat) {
    int64_t row_count = 0;
    req_partition_stat.tag_ = partition_stat.tag_;
    for (auto& seg : partition_stat.segments_stat_) {
        SegmentStat seg_stat;
        seg_stat.name_ = seg.name_;
        seg_stat.row_num_ = seg.row_count_;
        seg_stat.index_name_ = seg.index_name_;
        seg_stat.data_size_ = seg.data_size_;
        req_partition_stat.segments_stat_.emplace_back(seg_stat);
        row_count += seg.row_count_;
    }
    req_partition_stat.total_row_num_ = row_count;
}

ShowCollectionInfoRequest::ShowCollectionInfoRequest(const std::shared_ptr<milvus::server::Context>& context,
                                                     const std::string& collection_name,
                                                     CollectionInfo& collection_info)
    : BaseRequest(context, BaseRequest::kShowCollectionInfo),
      collection_name_(collection_name),
      collection_info_(collection_info) {
}

BaseRequestPtr
ShowCollectionInfoRequest::Create(const std::shared_ptr<milvus::server::Context>& context,
                                  const std::string& collection_name, CollectionInfo& collection_info) {
    return std::shared_ptr<BaseRequest>(new ShowCollectionInfoRequest(context, collection_name, collection_info));
}

Status
ShowCollectionInfoRequest::OnExecute() {
    std::string hdr = "ShowCollectionInfoRequest(collection=" + collection_name_ + ")";
    TimeRecorderAuto rc(hdr);

    // step 1: check collection name
    auto status = ValidationUtil::ValidateCollectionName(collection_name_);
    if (!status.ok()) {
        return status;
    }

    // step 2: check collection existence
    // only process root collection, ignore partition collection
    engine::meta::CollectionSchema collection_schema;
    collection_schema.collection_id_ = collection_name_;
    status = DBWrapper::DB()->DescribeCollection(collection_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
        } else {
            return status;
        }
    } else {
        if (!collection_schema.owner_collection_.empty()) {
            return Status(SERVER_INVALID_COLLECTION_NAME, CollectionNotExistMsg(collection_name_));
        }
    }

    // step 3: get partitions
    engine::CollectionInfo collection_info;
    status = DBWrapper::DB()->GetCollectionInfo(collection_name_, collection_info);
    if (!status.ok()) {
        return status;
    }

    // step 4: construct partitions info
    int64_t total_row_count = 0;
    collection_info_.partitions_stat_.reserve(collection_info.partitions_stat_.size());
    for (auto& partition : collection_info.partitions_stat_) {
        PartitionStat partition_stat;
        ConstructPartitionStat(partition, partition_stat);
        total_row_count += partition_stat.total_row_num_;
        collection_info_.partitions_stat_.emplace_back(partition_stat);
    }

    collection_info_.total_row_num_ = total_row_count;

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
