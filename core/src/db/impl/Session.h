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

#pragma once

#include <any>

#include "db/impl/DBEngine.h"
#include "db/impl/ResourceAttrs.hpp"
#include "db/snapshot/BaseResource.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Utils.h"
#include "utils/Exception.h"
#include "utils/Json.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

///////////////////////////////////////////////////////////////////////////////////////
class Session {
 public:
    explicit
    Session(DBEnginePtr engine): db_engine_(engine), pos_(-1) {
    }

    ~Session() = default;

 public:
    template <typename ResourceT, typename U>
    Status
    Select(const std::string& field, const U& value, std::vector<typename ResourceT::Ptr>& resources);

    template<typename ResourceT>
    Status
    Apply(ResourceContextPtr<ResourceT> resp);

    Status
    ResultPos() {
        if (apply_context_.empty()) {
            return Status(SERVER_UNEXPECTED_ERROR, "Session is empty");
        }
        pos_ = apply_context_.size() - 1;

        return Status::OK();
    }

    Status
    Commit(std::vector<int64_t>& result_ids) {
        return db_engine_->ExecuteTransaction(apply_context_, result_ids);
    }

    Status
    Commit(int64_t& result_id) {
        if (apply_context_.empty()) {
            return Status::OK();
        }

        if (pos_ < 0) {
            throw Exception(1, "Result pos is small than 0");
//            return Status(SERVER_UNEXPECTED_ERROR, "Result pos is small than 0");
        }
        std::vector<int64_t> result_ids;
        auto status = db_engine_->ExecuteTransaction(apply_context_, result_ids);
        if (!status.ok()) {
            return status;
        }

        result_id = result_ids.at(pos_);
        return Status::OK();
    }

 private:
    std::vector<DBApplyContext> apply_context_;
    int64_t pos_;
    DBEnginePtr db_engine_;
};

template <typename T, typename U>
Status
Session::Select(const std::string& field, const U& value, std::vector<typename T::Ptr>& resources) {
//    std::string field_value = "\'" + name + "\'";
    DBQueryContext context;
    context.table_ = T::Name;

    if (!field.empty()) {
        std::string field_value;
        ResourceFieldToSqlStr(value, field_value);
        context.filter_attrs_ = {{field, field_value}};
    }

    AttrsMapList attrs;
    auto status = db_engine_->Query(context, attrs);
    if (!status.ok()) {
        return status;
    }

    if (attrs.empty()) {
        return Status::OK();
    }

    for (auto raw : attrs) {
        auto resource = CreateResPtr<T>();

        auto mf_p = std::dynamic_pointer_cast<MappingsField>(resource);
        if (mf_p != nullptr) {
            std::string mapping = raw[F_MAPPINGS];
            auto mapping_json = nlohmann::json::parse(mapping);
            std::set<int64_t> mappings;
            for (auto& ele : mapping_json) {
                mappings.insert(ele.get<int64_t>());
            }
            mf_p->GetMappings() = mappings;
        }

        auto sf_p = std::dynamic_pointer_cast<StateField>(resource);
        if (sf_p != nullptr) {
            auto status_str = raw[F_STATE];
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
            feid_p->SetFieldElementId(feid);
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
            auto name = raw[F_NAME];
            name_p->SetName(name);
        }

        auto pf_p = std::dynamic_pointer_cast<ParamsField>(resource);
        if (pf_p != nullptr) {
            auto params = nlohmann::json::parse(raw[F_PARAMS]);
            pf_p->SetParams(params);
        }

        auto size_p = std::dynamic_pointer_cast<SizeField>(resource);
        if (size_p != nullptr) {
            auto size = std::stol(raw[F_SIZE]);
            size_p->SetSize(size);
        }

        auto rc_p = std::dynamic_pointer_cast<RowCountField>(resource);
        if (rc_p != nullptr) {
            auto rc = std::stol(raw[F_ROW_COUNT]);
            rc_p->SetRowCount(rc);
        }

        resources.push_back(std::move(resource));
    }

    return Status::OK();
}

template <typename ResourceT>
Status
Session::Apply(ResourceContextPtr<ResourceT> resp) {
    // TODO: may here not need to store resp
    auto status = Status::OK();
    std::string sql;

    DBApplyContext context;
    context.op_ = resp->Op();
    if (context.op_ == oAdd) {
        status = ResourceContextAddAttrMap<ResourceT>(resp, context.attrs_);
    } else if (context.op_ == oUpdate) {
        status = ResourceContextUpdateAttrMap<ResourceT>(resp, context.attrs_);
        context.id_ = resp->Resource()->GetID();
    } else if (context.op_ == oDelete) {
        context.id_ = resp->ID();
    } else {
        return Status(SERVER_UNEXPECTED_ERROR, "Unknown resource context operation");
    }

    if (!status.ok()) {
        return status;
    }

    context.table_ = resp->Table();
    apply_context_.push_back(context);

    return Status::OK();
}

using SessionPtr = std::shared_ptr<Session>;

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
