// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "db/impl/Session.h"

#include <algorithm>
#include <string>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include "db/impl/MetaFields.h"
#include "db/snapshot/Resources.h"
#include "utils/Exception.h"

namespace milvus::engine::snapshot {

#define NULLPTR_CHECK(ptr)                                                  \
    if (ptr == nullptr) {                                                   \
        return Status(SERVER_UNSUPPORTED_ERROR, "Convert pointer failed."); \
    }

///////////////////////////////////////////////////////////////////////
namespace {
class MockDB {
 private:
    using TableRaw = std::map<std::string, std::string>;

 public:
    static MockDB&
    GetInstance() {
        static MockDB db = MockDB();
        return db;
    }

    Status
    Query(const std::string& table, int64_t id, TableRaw& raw) {
        std::lock_guard lock(mutex_);
        return QueryNoLock(table, id, raw);
    }

    Status
    Insert(const std::string& table, const TableRaw& raw, int64_t& result_id) {
        std::unique_lock<std::mutex> lock(mutex_);
        return InsertNoLock(table, raw, result_id);
    }

    Status
    Update(const std::string& table, const TableRaw& raw, int64_t& result_id, TableRaw& pre_raw) {
        std::unique_lock<std::mutex> lock(mutex_);
        return UpdateNoLock(table, raw, result_id, pre_raw);
    }

    Status
    Delete(const std::string& table, const TableRaw& raw, int64_t& result_id, TableRaw& pre_raw) {
        std::unique_lock<std::mutex> lock(mutex_);
        return DeleteNoLock(table, raw, result_id);
    }

 private:
    Status
    QueryNoLock(const std::string& table, int64_t id, TableRaw& raw) {
        const std::string id_str = std::to_string(id);
        auto table_resources_ = resources_[table];
        for (auto& res: table_resources_) {
            if (res[F_ID] == id_str) {
                raw = res;
                return Status::OK();
            }
        }

        std::string err_msg = "Cannot find table " + table + " id " + id_str;
        return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
    }

    Status
    InsertNoLock(const std::string& table, const TableRaw& raw, int64_t& result_id) {
        TableRaw new_raw = TableRaw(raw);

        auto max_id = max_ip_map_[table];
        max_ip_map_[table] = max_id + 1;
        new_raw[F_ID] = std::to_string(max_id + 1);

        auto& collection_resources_ = resources_[table];
        collection_resources_.push_back(new_raw);
        result_id = max_id + 1;

        return Status::OK();
    }

    Status
    UpdateNoLock(const std::string& table, const TableRaw& raw, int64_t& result_id, TableRaw& pre_raw) {
        const std::string id_str = raw.at(F_ID);
        size_t id = std::stol(id_str);
        if (id <= 0) {
            return Status(SERVER_UNSUPPORTED_ERROR, "raw id should be larger than 0");
        }

        auto& collection_resources_ = resources_[table];
        for (auto& res: collection_resources_) {
            if (res[F_ID] == id_str) {
                pre_raw = res;
                for (auto& kv: raw) {
                    res[kv.first] = kv.second;
                }
                result_id = id;
                return Status::OK();
            }
        }

        std::string err_msg = "Can not find target resource " + table + ": id " + id_str;
        return Status(SERVER_UNSUPPORTED_ERROR, err_msg);
    }

    Status
    DeleteNoLock(const std::string& table, const TableRaw& raw, int64_t& result_id) {
        const std::string id_str = raw.at(F_ID);
        size_t id = std::stol(id_str);
        if (id <= 0) {
            return Status(SERVER_UNSUPPORTED_ERROR, "raw id should be larger than 0");
        }

        auto& collection_resources_ = resources_[table];
        for (size_t i = 0; i < collection_resources_.size(); i++) {
            auto& res = collection_resources_.at(i);
            if (res[F_ID] == id_str) {
                collection_resources_.erase(collection_resources_.begin() + i, collection_resources_.begin() + i + 1);
                return Status::OK();
            }
        }
        return Status::OK();
    }

 private:
    MockDB() {
        max_ip_map_.clear();
        resources_.clear();

        max_ip_map_["Collection"] = 0;
        resources_["Collection"] = std::vector<TableRaw>();

        max_ip_map_["SchemaCommit"] = 0;
        resources_["SchemaCommit"] = std::vector<TableRaw>();

        max_ip_map_["Field"] = 0;
        resources_["Field"] = std::vector<TableRaw>();

        max_ip_map_["FieldCommit"] = 0;
        resources_["FieldCommit"] = std::vector<TableRaw>();

        max_ip_map_["FieldElement"] = 0;
        resources_["FieldElement"] = std::vector<TableRaw>();

        max_ip_map_["CollectionCommit"] = 0;
        resources_["CollectionCommit"] = std::vector<TableRaw>();

        max_ip_map_["Partition"] = 0;
        resources_["Partition"] = std::vector<TableRaw>();

        max_ip_map_["PartitionCommit"] = 0;
        resources_["PartitionCommit"] = std::vector<TableRaw>();

        max_ip_map_["Segment"] = 0;
        resources_["Segment"] = std::vector<TableRaw>();

        max_ip_map_["SegmentCommit"] = 0;
        resources_["SegmentCommit"] = std::vector<TableRaw>();

        max_ip_map_["SegmentFile"] = 0;
        resources_["SegmentFile"] = std::vector<TableRaw>();
    }

 private:
    std::mutex mutex_;
    std::unordered_map<std::string, int64_t> max_ip_map_;
    std::unordered_map<std::string, std::vector<TableRaw>> resources_;
};
}

}  // namespace milvus::engine::snapshot
