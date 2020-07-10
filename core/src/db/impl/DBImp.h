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

#include <type_traits>

#include "db/impl/MySqlEngine.h"
#include "db/impl/Session.h"
#include "db/snapshot/Resources.h"
#include "utils/Exception.h"

namespace milvus::engine {

using namespace snapshot;

class DBImp {
 public:
    static
    DBImp&
    GetInstance() {
        static DBImp db;
        return db;
    }

 public:
    DBImp() {
        DBMetaOptions options;
        options.backend_uri_ = "mysql://root:12345678@127.0.0.1:3307/milvus";
        engine_ = std::make_shared<MySqlEngine>(options);
    }


    SessionPtr
    CreateSession() {
//        return std::make_shared<MockSession>(engine_);
        return std::make_shared<Session>(engine_);
    }

    template <typename T, typename std::enable_if<std::is_base_of<BaseResource, T>::value, T>::type* = nullptr>
    Status
    Select(int64_t id, typename T::Ptr& resource) {
        // TODO move select logic to here
        auto session = CreateSession();
        return session->Select<T>(T::Name, id, resource);
    }

    Status
    SelectResourceIDs(const std::string& table, std::vector<int64_t>& ids, const std::string& filter_field, const std::string& filter_value) {
        std::string sql = "SELECT id FROM " + table;
        //+ " WHERE " + filter_field + " = " + filter_value + ";";
        if (!filter_field.empty()) {
            sql += " WHERE " + filter_field + " = " + filter_value;
        }
        sql += ";";

        AttrsMapList attrs;
        auto status = engine_->Query(sql, attrs);
        if (!status.ok()) {
            throw Exception(status.code(), status.message());
        }

        for (auto& raw: attrs) {
            ids.push_back(std::stol(raw[F_ID]));
        }

        return Status::OK();
    }

    template <typename ResourceT>
    Status
    Apply(ResourceContextPtr<ResourceT> resp, int64_t& result_id) {
        auto session = CreateSession();
        session->Apply<ResourceT>(resp);

        std::vector<int64_t> result_ids;
        auto status = session->Commit(result_ids);

        if (!status.ok()) {
            throw Exception(status.code(), status.message());
        }

        if (result_ids.size() != 1) {
            throw Exception(1, "Result id is not equal one ... ");
        }

        result_id = result_ids.at(0);
        return Status::OK();
    }

 private:
    DBEnginePtr engine_;
};

}  // namespace milvus::engine
