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

#include "db/meta/backend/MetaHelper.h"

#include <vector>

#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

Status
MetaHelper::MetaQueryContextToSql(const MetaQueryContext& context, std::string& sql) {
    sql.clear();
    if (context.all_required_) {
        sql = "SELECT * FROM ";
    } else {
        std::string query_fields;
        StringHelpFunctions::MergeStringWithDelimeter(context.query_fields_, ",", query_fields);
        sql = "SELECT " + query_fields + " FROM ";
    }
    sql += context.table_;

    std::vector<std::string> filter_conditions;
    for (auto& attr : context.filter_attrs_) {
        std::string filter_str;
        if (attr.second.size() < 1) {
            return Status(SERVER_UNEXPECTED_ERROR, "Invalid filter attrs. ");
        } else if (attr.second.size() == 1) {
            filter_conditions.emplace_back(attr.first + "=" + attr.second[0]);
        } else {
            std::string in_condition;
            StringHelpFunctions::MergeStringWithDelimeter(attr.second, ",", in_condition);
            in_condition = attr.first + " IN (" + in_condition + ")";
            filter_conditions.emplace_back(in_condition);
        }

        StringHelpFunctions::MergeStringWithDelimeter(filter_conditions, " AND ", filter_str);
        sql += " WHERE " + filter_str;
    }

    sql += ";";

    return Status::OK();
}

Status
MetaHelper::MetaFilterContextToSql(const MetaFilterContext &context, std::string &sql) {
    sql.clear();
    sql = "SELECT * FROM " + context.table_ + " WHERE " + context.combination_->Dump() + ";";

    return Status::OK();
}

Status
MetaHelper::MetaApplyContextToSql(const MetaApplyContext& context, std::string& sql) {
    if (!context.sql_.empty()) {
        sql = context.sql_;
        return Status::OK();
    }

    switch (context.op_) {
        case oAdd: {
            std::string field_names, values;
            std::vector<std::string> field_list, value_list;
            for (auto& kv : context.attrs_) {
                field_list.push_back(kv.first);
                value_list.push_back(kv.second);
            }
            StringHelpFunctions::MergeStringWithDelimeter(field_list, ",", field_names);
            StringHelpFunctions::MergeStringWithDelimeter(value_list, ",", values);
            sql = "INSERT INTO " + context.table_ + "(" + field_names + ") " + "VALUES(" + values + ")";
            break;
        }
        case oUpdate: {
            std::string field_pairs;
            std::vector<std::string> updated_attrs;
            for (auto& attr_kv : context.attrs_) {
                updated_attrs.emplace_back(attr_kv.first + "=" + attr_kv.second);
            }

            StringHelpFunctions::MergeStringWithDelimeter(updated_attrs, ",", field_pairs);
            sql = "UPDATE " + context.table_ + " SET " + field_pairs + " WHERE id = " + std::to_string(context.id_);
            break;
        }
        case oDelete: {
            sql = "DELETE FROM " + context.table_ + " WHERE id = " + std::to_string(context.id_);
            break;
        }
        default:
            return Status(SERVER_UNEXPECTED_ERROR, "Unknown context operation");
    }

    return Status::OK();
}

}  // namespace milvus::engine::meta
