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
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/meta/MetaResourceAttrs.h"
#include "db/meta/backend/MetaEngine.h"
#include "db/snapshot/BaseResource.h"
#include "db/snapshot/ResourceContext.h"
#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Utils.h"
#include "utils/Exception.h"
#include "utils/Json.h"
#include "utils/Status.h"

namespace milvus::engine::meta {
using snapshot::FETYPE_TYPE;
using snapshot::FTYPE_TYPE;

class MetaSession {
 public:
    explicit MetaSession(MetaEnginePtr engine) : db_engine_(engine), pos_(-1) {
    }

    ~MetaSession() = default;

 public:
    template <typename ResourceT, typename U>
    Status
    Select(const std::string& field, const std::vector<U>& value, const std::vector<std::string>& target_attrs,
           std::vector<typename ResourceT::Ptr>& resources);

    template <typename ResourceT>
    Status
    Apply(snapshot::ResourceContextPtr<ResourceT> resp);

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
    std::vector<MetaApplyContext> apply_context_;
    int64_t pos_;
    MetaEnginePtr db_engine_;
};

template <typename T, typename U>
Status
MetaSession::Select(const std::string& field, const std::vector<U>& values,
                    const std::vector<std::string>& target_attrs, std::vector<typename T::Ptr>& resources) {
    MetaQueryContext context;
    context.table_ = T::Name;

    if (!field.empty()) {
        std::vector<std::string> field_values;
        for (auto& v : values) {
            std::string field_value;
            ResourceFieldToSqlStr(v, field_value);
            field_values.push_back(field_value);
        }
        context.filter_attrs_ = {{field, field_values}};
    }

    if (!target_attrs.empty()) {
        context.all_required_ = false;
        context.query_fields_ = target_attrs;
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
        auto resource = snapshot::CreateResPtr<T>();
        std::unordered_map<std::string, std::string>::iterator iter;

        if (auto mf_p = std::dynamic_pointer_cast<snapshot::FlushableMappingsField>(resource)) {
            iter = raw.find(F_MAPPINGS);
            if (iter != raw.end()) {
                auto mapping_json = nlohmann::json::parse(iter->second);
                std::set<int64_t> mappings;
                for (auto& ele : mapping_json) {
                    mappings.insert(ele.get<int64_t>());
                }
                mf_p->GetFlushIds() = mappings;
            }
        } else if (auto mf_p = std::dynamic_pointer_cast<snapshot::MappingsField>(resource)) {
            iter = raw.find(F_MAPPINGS);
            if (iter != raw.end()) {
                auto mapping_json = nlohmann::json::parse(iter->second);
                std::set<int64_t> mappings;
                for (auto& ele : mapping_json) {
                    mappings.insert(ele.get<int64_t>());
                }
                mf_p->GetMappings() = mappings;
            }
        }

        auto sf_p = std::dynamic_pointer_cast<snapshot::StateField>(resource);
        if (sf_p != nullptr) {
            iter = raw.find(F_STATE);
            if (iter != raw.end()) {
                auto status_int = std::stol(iter->second);
                sf_p->ResetStatus();
                switch (static_cast<snapshot::State>(status_int)) {
                    case snapshot::PENDING: {
                        break;
                    }
                    case snapshot::ACTIVE: {
                        sf_p->Activate();
                        break;
                    }
                    case snapshot::DEACTIVE: {
                        sf_p->Deactivate();
                        break;
                    }
                    default: { return Status(SERVER_UNSUPPORTED_ERROR, "Invalid state value"); }
                }
            }
        }

        auto lsn_f = std::dynamic_pointer_cast<snapshot::LsnField>(resource);
        if (lsn_f != nullptr) {
            iter = raw.find(F_LSN);
            if (iter != raw.end()) {
                auto lsn = std::stoul(iter->second);
                lsn_f->SetLsn(lsn);
            }
        }

        auto created_on_f = std::dynamic_pointer_cast<snapshot::CreatedOnField>(resource);
        if (created_on_f != nullptr) {
            iter = raw.find(F_CREATED_ON);
            if (iter != raw.end()) {
                auto created_on = std::stol(iter->second);
                created_on_f->SetCreatedTime(created_on);
            }
        }

        auto update_on_p = std::dynamic_pointer_cast<snapshot::UpdatedOnField>(resource);
        if (update_on_p != nullptr) {
            iter = raw.find(F_UPDATED_ON);
            if (iter != raw.end()) {
                auto update_on = std::stol(iter->second);
                update_on_p->SetUpdatedTime(update_on);
            }
        }

        auto id_p = std::dynamic_pointer_cast<snapshot::IdField>(resource);
        if (id_p != nullptr) {
            iter = raw.find(F_ID);
            if (iter != raw.end()) {
                auto t_id = std::stol(iter->second);
                id_p->SetID(t_id);
            }
        }

        auto cid_p = std::dynamic_pointer_cast<snapshot::CollectionIdField>(resource);
        if (cid_p != nullptr) {
            iter = raw.find(F_COLLECTON_ID);
            if (iter != raw.end()) {
                auto cid = std::stol(iter->second);
                cid_p->SetCollectionId(cid);
            }
        }

        auto sid_p = std::dynamic_pointer_cast<snapshot::SchemaIdField>(resource);
        if (sid_p != nullptr) {
            iter = raw.find(F_SCHEMA_ID);
            if (iter != raw.end()) {
                auto sid = std::stol(iter->second);
                sid_p->SetSchemaId(sid);
            }
        }

        auto num_p = std::dynamic_pointer_cast<snapshot::NumField>(resource);
        if (num_p != nullptr) {
            iter = raw.find(F_NUM);
            if (iter != raw.end()) {
                auto num = std::stol(iter->second);
                num_p->SetNum(num);
            }
        }

        auto ftype_p = std::dynamic_pointer_cast<snapshot::FtypeField>(resource);
        if (ftype_p != nullptr) {
            iter = raw.find(F_FTYPE);
            if (iter != raw.end()) {
                auto ftype = (FTYPE_TYPE)std::stol(iter->second);
                ftype_p->SetFtype(ftype);
            }
        }

        auto fetype_p = std::dynamic_pointer_cast<snapshot::FEtypeField>(resource);
        if (fetype_p != nullptr) {
            iter = raw.find(F_FETYPE);
            if (iter != raw.end()) {
                auto fetype = (FETYPE_TYPE)std::stol(iter->second);
                fetype_p->SetFEtype(fetype);
            }
        }

        auto fid_p = std::dynamic_pointer_cast<snapshot::FieldIdField>(resource);
        if (fid_p != nullptr) {
            iter = raw.find(F_FIELD_ID);
            if (iter != raw.end()) {
                auto fid = std::stol(iter->second);
                fid_p->SetFieldId(fid);
            }
        }

        auto feid_p = std::dynamic_pointer_cast<snapshot::FieldElementIdField>(resource);
        if (feid_p != nullptr) {
            iter = raw.find(F_FIELD_ELEMENT_ID);
            if (iter != raw.end()) {
                auto feid = std::stol(iter->second);
                feid_p->SetFieldElementId(feid);
            }
        }

        auto pid_p = std::dynamic_pointer_cast<snapshot::PartitionIdField>(resource);
        if (pid_p != nullptr) {
            iter = raw.find(F_PARTITION_ID);
            if (iter != raw.end()) {
                auto p_id = std::stol(iter->second);
                pid_p->SetPartitionId(p_id);
            }
        }

        auto sgid_p = std::dynamic_pointer_cast<snapshot::SegmentIdField>(resource);
        if (sgid_p != nullptr) {
            iter = raw.find(F_SEGMENT_ID);
            if (iter != raw.end()) {
                auto sg_id = std::stol(iter->second);
                sgid_p->SetSegmentId(sg_id);
            }
        }

        auto name_p = std::dynamic_pointer_cast<snapshot::NameField>(resource);
        if (name_p != nullptr) {
            iter = raw.find(F_NAME);
            if (iter != raw.end()) {
                name_p->SetName(iter->second);
            }
        }

        auto pf_p = std::dynamic_pointer_cast<snapshot::ParamsField>(resource);
        if (pf_p != nullptr) {
            iter = raw.find(F_PARAMS);
            if (iter != raw.end()) {
                auto params = nlohmann::json::parse(iter->second);
                pf_p->SetParams(params);
            }
        }

        auto size_p = std::dynamic_pointer_cast<snapshot::SizeField>(resource);
        if (size_p != nullptr) {
            iter = raw.find(F_SIZE);
            if (iter != raw.end()) {
                uint64_t size = std::stoul(iter->second);
                size_p->SetSize(size);
            }
        }

        auto rc_p = std::dynamic_pointer_cast<snapshot::RowCountField>(resource);
        if (rc_p != nullptr) {
            iter = raw.find(F_ROW_COUNT);
            if (iter != raw.end()) {
                uint64_t rc = std::stoul(iter->second);
                rc_p->SetRowCount(rc);
            }
        }

        auto tn_p = std::dynamic_pointer_cast<snapshot::TypeNameField>(resource);
        if (tn_p != nullptr) {
            iter = raw.find(F_TYPE_NAME);
            if (iter != raw.end()) {
                tn_p->SetTypeName(iter->second);
            }
        }

        resources.push_back(std::move(resource));
    }

    return Status::OK();
}

template <typename ResourceT>
Status
MetaSession::Apply(snapshot::ResourceContextPtr<ResourceT> resp) {
    // TODO: may here not need to store resp
    auto status = Status::OK();
    std::string sql;

    MetaApplyContext context;
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

using SessionPtr = std::shared_ptr<MetaSession>;

}  // namespace milvus::engine::meta
