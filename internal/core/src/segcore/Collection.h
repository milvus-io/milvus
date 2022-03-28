// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <string>

#include "common/Schema.h"

namespace milvus::segcore {

class Collection {
 public:
    explicit Collection(const std::string& collection_proto);

    void
    parse();

 public:
    SchemaPtr&
    get_schema() {
        return schema_;
    }

    const std::string&
    get_collection_name() {
        return collection_name_;
    }

 private:
    std::string collection_name_;
    std::string schema_proto_;
    SchemaPtr schema_;
};

using CollectionPtr = std::unique_ptr<Collection>;

}  // namespace milvus::segcore
