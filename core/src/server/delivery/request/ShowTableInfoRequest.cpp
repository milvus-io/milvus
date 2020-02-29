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

#include "server/delivery/request/ShowTableInfoRequest.h"
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

ShowTableInfoRequest::ShowTableInfoRequest(const std::shared_ptr<Context>& context, const std::string& table_name,
                                           TableInfo& table_info)
    : BaseRequest(context, INFO_REQUEST_GROUP), table_name_(table_name), table_info_(table_info) {
}

BaseRequestPtr
ShowTableInfoRequest::Create(const std::shared_ptr<Context>& context, const std::string& table_name,
                             TableInfo& table_info) {
    return std::shared_ptr<BaseRequest>(new ShowTableInfoRequest(context, table_name, table_info));
}

Status
ShowTableInfoRequest::OnExecute() {
    std::string hdr = "ShowTableInfoRequest(table=" + table_name_ + ")";
    TimeRecorderAuto rc(hdr);

    // step 1: check table name
    auto status = ValidationUtil::ValidateTableName(table_name_);
    if (!status.ok()) {
        return status;
    }

    // step 2: check table existence
    // only process root table, ignore partition table
    engine::meta::TableSchema table_schema;
    table_schema.table_id_ = table_name_;
    status = DBWrapper::DB()->DescribeTable(table_schema);
    if (!status.ok()) {
        if (status.code() == DB_NOT_FOUND) {
            return Status(SERVER_TABLE_NOT_EXIST, TableNotExistMsg(table_name_));
        } else {
            return status;
        }
    } else {
        if (!table_schema.owner_table_.empty()) {
            return Status(SERVER_INVALID_TABLE_NAME, TableNotExistMsg(table_name_));
        }
    }

    // step 3: get partitions
    engine::TableInfo table_info;
    status = DBWrapper::DB()->GetTableInfo(table_name_, table_info);
    if (!status.ok()) {
        return status;
    }

    // step 4: construct partitions info
    int64_t total_row_count = 0;
    table_info_.partitions_stat_.reserve(table_info.partitions_stat_.size());
    for (auto& partition : table_info.partitions_stat_) {
        PartitionStat partition_stat;
        ConstructPartitionStat(partition, partition_stat);
        total_row_count += partition_stat.total_row_num_;
        table_info_.partitions_stat_.emplace_back(partition_stat);
    }

    table_info_.total_row_num_ = total_row_count;

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
