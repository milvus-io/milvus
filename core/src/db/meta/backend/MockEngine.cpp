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

#include "db/meta/backend/MockEngine.h"

#include <utility>

#include "db/meta/MetaNames.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

void
MockEngine::Init() {
    max_ip_map_.clear();
    resources_.clear();
}

Status
MockEngine::QueryNoLock(const MetaQueryContext& context, AttrsMapList& attrs) {
    if (resources_.find(context.table_) == resources_.end()) {
        return Status(0, "Empty");
    }

    auto select_target_attrs = [](const TableRaw& raw, AttrsMapList& des,
                                  const std::vector<std::string>& target_attrs) {
        if (target_attrs.empty()) {
            return;
        }

        auto m = std::unordered_map<std::string, std::string>();
        for (auto& attr : target_attrs) {
            auto iter = raw.find(attr);
            if (iter != raw.end()) {
                m.insert(std::make_pair(iter->first, iter->second));
            }
        }
        if (!m.empty()) {
            des.push_back(m);
        }
    };

    auto term = [](const std::string& attr, const std::vector<std::string>& attrs) -> bool {
        for (auto& t : attrs) {
            if (attr == t) {
                return true;
            }
        }

        return false;
    };

    auto& candidate_raws = resources_[context.table_];

    if (!context.filter_attrs_.empty()) {
        bool selected = true;
        for (auto& raw : candidate_raws) {
            for (auto& filter_attr : context.filter_attrs_) {
                auto iter = raw.find(filter_attr.first);
                if (iter == raw.end()) {
                    selected = false;
                    break;
                }

                if (!term(iter->second, filter_attr.second)) {
                    selected = false;
                    break;
                }
            }
            if (selected) {
                if (context.all_required_) {
                    attrs.push_back(raw);
                } else {
                    select_target_attrs(raw, attrs, context.query_fields_);
                }
            }
            selected = true;
        }
    } else {
        if (context.all_required_) {
            attrs = candidate_raws;
        } else {
            for (auto& attr : candidate_raws) {
                select_target_attrs(attr, attrs, context.query_fields_);
            }
        }
    }

    for (auto& result_raw : attrs) {
        for (auto& kv : result_raw) {
            if (*kv.second.begin() == '\'' && *kv.second.rbegin() == '\'') {
                std::string v = kv.second;
                StringHelpFunctions::TrimStringQuote(v, "\'");
                kv.second = v;
            }
        }
    }

    return Status::OK();
}

Status
MockEngine::AddNoLock(const MetaApplyContext& add_context, int64_t& result_id, TableRaw& pre_raw) {
    if (max_ip_map_.find(add_context.table_) == max_ip_map_.end() ||
        resources_.find(add_context.table_) == resources_.end()) {
        max_ip_map_[add_context.table_] = 0;
        resources_[add_context.table_] = std::vector<TableRaw>();
    }

    auto max_id = max_ip_map_[add_context.table_];
    max_ip_map_[add_context.table_] = max_id + 1;

    TableRaw new_raw;
    for (auto& attr : add_context.attrs_) {
        new_raw.insert(attr);
    }

    new_raw[F_ID] = std::to_string(max_id + 1);
    resources_[add_context.table_].push_back(new_raw);
    pre_raw = new_raw;
    result_id = max_id + 1;

    return Status::OK();
}

Status
MockEngine::UpdateNoLock(const MetaApplyContext& update_context, int64_t& result_id, TableRaw& pre_raw) {
    const std::string id_str = std::to_string(update_context.id_);

    auto& target_collection = resources_[update_context.table_];
    for (auto& attrs : target_collection) {
        if (attrs[F_ID] == id_str) {
            pre_raw = attrs;
            for (auto& kv : update_context.attrs_) {
                attrs[kv.first] = kv.second;
            }
            result_id = update_context.id_;
            return Status::OK();
        }
    }

    std::string err = "Cannot found resource in " + update_context.table_ + " where id = " + id_str;
    return Status(SERVER_UNEXPECTED_ERROR, err);
}

Status
MockEngine::DeleteNoLock(const MetaApplyContext& delete_context, int64_t& result_id, TableRaw& pre_raw) {
    const std::string id_str = std::to_string(delete_context.id_);
    auto& target_collection = resources_[delete_context.table_];

    for (auto iter = target_collection.begin(); iter != target_collection.end(); iter++) {
        if ((*iter)[F_ID] == id_str) {
            pre_raw = *iter;
            result_id = std::stol(iter->at(F_ID));
            target_collection.erase(iter);
            return Status::OK();
        }
    }

    std::string err = "Cannot found resource in " + delete_context.table_ + " where id = " + id_str;
    return Status(SERVER_UNEXPECTED_ERROR, err);
}

Status
MockEngine::Query(const MetaQueryContext& context, AttrsMapList& attrs) {
    std::lock_guard<std::mutex> lock(mutex_);
    return QueryNoLock(context, attrs);
}

Status
MockEngine::ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto status = Status::OK();
    std::vector<std::pair<MetaContextOp, TableEntity>> pair_entities;
    TableRaw raw;
    for (auto& context : sql_contexts) {
        int64_t id;
        if (context.op_ == oAdd) {
            status = AddNoLock(context, id, raw);
        } else if (context.op_ == oUpdate) {
            status = UpdateNoLock(context, id, raw);
        } else if (context.op_ == oDelete) {
            status = DeleteNoLock(context, id, raw);
        } else {
            status = Status(SERVER_UNEXPECTED_ERROR, "Unknown resource context");
        }

        if (!status.ok()) {
            break;
        }
        result_ids.push_back(id);
        pair_entities.emplace_back(context.op_, TableEntity(context.table_, raw));
    }

    if (!status.ok()) {
        RollBackNoLock(pair_entities);
    }

    return status;
}

Status
MockEngine::RollBackNoLock(const std::vector<std::pair<MetaContextOp, TableEntity>>& pre_entities) {
    for (auto& o_e : pre_entities) {
        auto table = o_e.second.first;
        if (o_e.first == oAdd) {
            auto id = std::stol(o_e.second.second.at(F_ID));
            max_ip_map_[table] = id - 1;
            auto& table_res = resources_[table];
            for (size_t i = 0; i < table_res.size(); i++) {
                auto store_id = std::stol(table_res[i].at(F_ID));
                if (store_id == id) {
                    table_res.erase(table_res.begin() + i, table_res.begin() + i + 1);
                    break;
                }
            }
        } else if (o_e.first == oUpdate) {
            auto id = std::stol(o_e.second.second.at(F_ID));
            auto& table_res = resources_[table];
            for (size_t j = 0; j < table_res.size(); j++) {
                auto store_id = std::stol(table_res[j].at(F_ID));
                if (store_id == id) {
                    table_res.erase(table_res.begin() + j, table_res.begin() + j + 1);
                    table_res.push_back(o_e.second.second);
                    break;
                }
            }
        } else if (o_e.first == oDelete) {
            resources_[o_e.second.first].push_back(o_e.second.second);
        } else {
            continue;
        }
    }

    return Status::OK();
}

Status
MockEngine::TruncateAll() {
    max_ip_map_.clear();
    resources_.clear();
    return Status::OK();
}

}  // namespace milvus::engine::meta
