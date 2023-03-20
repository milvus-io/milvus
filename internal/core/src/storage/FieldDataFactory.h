// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>

#include "storage/FieldData.h"

namespace milvus::storage {

class FieldDataFactory {
 private:
    FieldDataFactory() = default;
    FieldDataFactory(const FieldDataFactory&) = delete;
    FieldDataFactory
    operator=(const FieldDataFactory&) = delete;

 public:
    static FieldDataFactory&
    GetInstance() {
        static FieldDataFactory inst;
        return inst;
    }

    std::string
    GetName() const {
        return "FieldDataFactory";
    }

    FieldDataPtr
    CreateFieldData(const DataType& type, const int64_t dim = 1);
};

}  // namespace milvus::storage
