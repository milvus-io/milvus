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
            if (res["id"] == id_str) {
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
        new_raw["id"] = std::to_string(max_id + 1);

        auto& collection_resources_ = resources_[table];
        collection_resources_.push_back(new_raw);
        result_id = max_id + 1;

        return Status::OK();
    }

    Status
    UpdateNoLock(const std::string& table, const TableRaw& raw, int64_t& result_id, TableRaw& pre_raw) {
        const std::string id_str = raw.at("id");
        size_t id = std::stol(id_str);
        if (id <= 0) {
            return Status(SERVER_UNSUPPORTED_ERROR, "raw id should be larger than 0");
        }

        auto& collection_resources_ = resources_[table];
        for (auto& res: collection_resources_) {
            if (res["id"] == id_str) {
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
        const std::string id_str = raw.at("id");
        size_t id = std::stol(id_str);
        if (id <= 0) {
            return Status(SERVER_UNSUPPORTED_ERROR, "raw id should be larger than 0");
        }

        auto& collection_resources_ = resources_[table];
        for (size_t i = 0; i < collection_resources_.size(); i++) {
            auto& res = collection_resources_.at(i);
            if (res["id"] == id_str) {
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


Status
Session::Commit(std::vector<int64_t>& result_ids) {
    return db_engine_->ExecuteTransaction(sql_context_, result_ids);
}


///////////////////////////////////////// Mock ///////////////////////////////////////////////

//void
//MockSession::Transaction() {
//    if (transcation_enable_) {
//        throw std::domain_error("Now is transaction ... ");
//    }
//    add_resources_.clear();
//    update_resources_.clear();
//    delete_resources_.clear();
//    transcation_enable_ = true;
//}

template<typename T>
Status
MockSession::Select(const std::string& table, int64_t id, typename T::Ptr& resource) {
    auto& db = MockDB::GetInstance();
    std::map<std::string, std::string> raw;
    auto status = db.Query(table, id, raw);
    if (!status.ok()) {
        return status;
    }

    auto mf_p = std::dynamic_pointer_cast<MappingsField>(resource);
    if (mf_p != nullptr) {
        std::string mapping = raw[F_MAPPINGS];
        auto sub_str = mapping.substr(1, mapping.length() - 2);
        auto mapping_json = nlohmann::json::parse(sub_str);
        std::set<int64_t> mappings;
        for (auto& ele : mapping_json) {
            mappings.insert(ele.get<int64_t>());
        }
        mf_p->GetMappings() = mappings;
    }

    auto sf_p = std::dynamic_pointer_cast<StateField>(resource);
    if (sf_p != nullptr) {
        auto status_str = raw[F_STATUS];
        auto status_int = std::stol(status_str);
        switch (static_cast<State>(status_int)) {
            case PENDING: {
                sf_p->ResetStatus();
                break;
            }
            case ACTIVE: {
                sf_p->ResetStatus();
                sf_p->Activate();
                break;
            }
            case DEACTIVE: {
                sf_p->ResetStatus();
                sf_p->Deactivate();
                break;
            }
            default: {
                return Status(SERVER_UNSUPPORTED_ERROR, "Invalid state value");
            }
        }
    }

    auto lsn_f = std::dynamic_pointer_cast<LsnField>(resource);
    if (lsn_f != nullptr) {
        auto lsn = std::stoul(raw[F_LSN]);
        lsn_f->SetLsn(lsn);
    }

    auto created_on_f = std::dynamic_pointer_cast<CreatedOnField>(resource);
    if (created_on_f != nullptr) {
        auto created_on = std::stol(raw[F_CREATED_ON]);
        created_on_f->SetCreatedTime(created_on);
    }

    auto update_on_p = std::dynamic_pointer_cast<UpdatedOnField>(resource);
    if (update_on_p != nullptr) {
        auto update_on = std::stol(raw[F_UPDATED_ON]);
        update_on_p->SetUpdatedTime(update_on);
    }

    auto id_p = std::dynamic_pointer_cast<IdField>(resource);
    if (id_p != nullptr) {
        auto t_id = std::stol(raw[F_ID]);
        id_p->SetID(t_id);
    }

    auto cid_p = std::dynamic_pointer_cast<CollectionIdField>(resource);
    if (cid_p != nullptr) {
        auto cid = std::stol(raw[F_COLLECTON_ID]);
        cid_p->SetCollectionId(cid);
    }

    auto sid_p = std::dynamic_pointer_cast<SchemaIdField>(resource);
    if (sid_p != nullptr) {
        auto sid = std::stol(raw[F_SCHEMA_ID]);
        sid_p->SetSchemaId(sid);
    }

    auto num_p = std::dynamic_pointer_cast<NumField>(resource);
    if (num_p != nullptr) {
        auto num = std::stol(raw[F_NUM]);
        num_p->SetNum(num);
    }

    auto ftype_p = std::dynamic_pointer_cast<FtypeField>(resource);
    if (ftype_p != nullptr) {
        auto ftype = std::stol(raw[F_FTYPE]);
        ftype_p->SetFtype(ftype);
    }

    auto fid_p = std::dynamic_pointer_cast<FieldIdField>(resource);
    if (fid_p != nullptr) {
        auto fid = std::stol(raw[F_FIELD_ID]);
        fid_p->SetFieldId(fid);
    }

    auto feid_p = std::dynamic_pointer_cast<FieldElementIdField>(resource);
    if (feid_p != nullptr) {
        auto feid = std::stol(raw[F_FIELD_ELEMENT_ID]);
        feid_p->SetFieldElementId(id);
    }

    auto pid_p = std::dynamic_pointer_cast<PartitionIdField>(resource);
    if (pid_p != nullptr) {
        auto p_id = std::stol(raw[F_PARTITION_ID]);
        pid_p->SetPartitionId(p_id);
    }

    auto sgid_p = std::dynamic_pointer_cast<SegmentIdField>(resource);
    if (sgid_p != nullptr) {
        auto sg_id = std::stol(raw[F_SEGMENT_ID]);
        sgid_p->SetSegmentId(sg_id);
    }

    auto name_p = std::dynamic_pointer_cast<NameField>(resource);
    if (name_p != nullptr) {
        auto name_str = raw[F_NAME];
        auto name = name_str.substr(1, name_str.length() - 2);
        name_p->SetName(name);
    }

    return Status::OK();
}

Status
MockSession::Apply(ResourceContextPtr resp) {
//    if (!is_transaction && transcation_enable_) {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Transaction ...");
//    }
//
//    if (is_transaction && !transcation_enable_) {
//        return Status(SERVER_UNSUPPORTED_ERROR, "Must Transaction");
//    }

    auto status = Status::OK();
    int64_t result_id;

    auto& db = MockDB::GetInstance();
    std::map<std::string, std::string> pre_raw;
    std::map<std::string, std::string> attr_map;
    if (resp->Op() == oAdd) {
        ResourceContextAddAttrMap(resp, attr_map);
        db.Insert(resp->Table(), attr_map, result_id);
    } else if (resp->Op() == oUpdate) {
        ResourceContextUpdateAttrMap(resp, attr_map);
        db.Update(resp->Table(), attr_map, result_id, pre_raw);
    } else if (resp->Op() == oDelete) {
        ResourceContextDeleteAttrMap(resp, attr_map);
        db.Delete(resp->Table(), attr_map, result_id, pre_raw);
    }

    result_ids_.push_back(result_id);

    switch (resp->Op()) {
        case oAdd: {
            add_resources_[resp->Table()].emplace_back(result_id);
            break;
        }
        case oUpdate: {
            update_resources_[resp->Table()].push_back(pre_raw);
            break;
        }
        case oDelete: {
            delete_resources_[resp->Table()].push_back(pre_raw);
        }
        default: {
            throw std::domain_error("Unknown operator ... ");
        }
    }

    return Status::OK();
}

Status
MockSession::Commit(std::vector<int64_t>& result_ids) {
    add_resources_.clear();
    update_resources_.clear();
    delete_resources_.clear();

    result_ids.clear();
    std::copy(result_ids_.begin(), result_ids_.end(), std::back_inserter(result_ids));

    return Status::OK();
}

//void
//MockSession::RollBack() {
//    auto& db = MockDB::GetInstance();
//    std::map<std::string, std::string> pre_raw;
//    int64_t result_id;
//    for (auto & kv : add_resources_) {
//        for (auto & id : kv.second) {
//            std::map<std::string, std::string> raw = {std::make_pair("id", std::to_string(id))};
//            db.Delete(kv.first, raw, result_id, pre_raw);
//        }
//    }
//
//    for (auto& kv : update_resources_) {
//        for (auto& raw : kv.second) {
//            db.Update(kv.first, raw, result_id, pre_raw);
//        }
//    }
//
//    for (auto & kv : delete_resources_) {
//        for (auto& raw : kv.second) {
//            db.Insert(kv.first, raw, result_id);
//        }
//    }
//}

}  // namespace milvus::engine::snapshot
