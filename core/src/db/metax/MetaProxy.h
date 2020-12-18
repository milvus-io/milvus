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

#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "db/Types.h"
#include "db/metax/MetaDef.h"
#include "db/metax/MetaQuery.h"
#include "db/metax/MetaQueryHelper.h"
#include "db/metax/MetaQueryTraits.h"
#include "db/metax/MetaResField.h"
#include "db/metax/MetaTraits.h"
#include "db/metax/backend/MetaMysqlWare.h"
#include "db/metax/backend/MetaSqlDef.h"
#include "db/snapshot/ResourceTypes.h"
#include "utils/Status.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::metax {

class MetaProxy {
 public:
    explicit MetaProxy(EngineType type) : type_(type) {
        switch (type) {
            case EngineType::mysql_: {
                ware_ = std::make_shared<MetaMysqlWare>();
                break;
            }
            case EngineType::sqlite_: {
                break;
            }
            case EngineType::mock_: {
                break;
            }
            default: { break; }
        }
    }

    Status
    Insert(const MetaResFieldTuple& fields, snapshot::ID_TYPE& result_id) {
        return ware_->Insert(fields, result_id);
    }

    template <typename... Args>
    Status
    Select(Args... args) {
        static_assert(sizeof...(args) >= 3, "Select clause must at least 3");
        using args_tuple_type = std::tuple<Args...>;
        using columns_type = std::tuple_element_t<0, args_tuple_type>;
//        static_assert(is_columns<columns_type::template_type>::value, "");
        // TODO(yhz): Hard Code for template check
        args_tuple_type args_tuple = std::make_tuple(std::forward<Args>(args)...);
        decltype(auto) columns = std::get<0>(args_tuple);
        static_assert(is_columns_v<remove_cr_t<decltype(columns)>>, "It must be columns type");
        decltype(auto) tables = std::get<1>(args_tuple);
        static_assert(is_table_relation_v<remove_cr_t<decltype(tables)>>, "It must be tables type");
        decltype(auto) conditions = std::get<2>(args_tuple);
        static_assert(is_cond_clause_v<remove_cr_t<decltype(conditions)>>, "It must be condition");

        std::vector<std::string> selected_columns;
        TraverseColumns(columns, selected_columns, true);

//        std::vector<std::string> selected_tables;
        std::string selected_table_relation;
        TraverseTables(tables, selected_table_relation);

        std::string selected_condtition;
        TraverseConditions(conditions, selected_condtition);

        std::stringstream ss;
        ss << "SELECT ";

        std::string column_out;
        StringHelpFunctions::MergeStringWithDelimeter(selected_columns, ",", column_out);
        ss << column_out;
        ss << selected_table_relation;
        ss << selected_condtition;
        std::cout << " " << ss.str() << std::endl;

//        std::vector<std::string> selected_tables;
//        TraverseTables(tables, selected_tables);

        return Status::OK();
    }

 private:
    EngineType type_;
    MetaEngineWarePtr ware_;
};

using MetaProxyPtr = std::shared_ptr<MetaProxy>;

}  // namespace milvus::engine::metax
