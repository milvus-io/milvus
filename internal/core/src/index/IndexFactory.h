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

#include <utils/Types.h>
#include "index/Index.h"
#include "common/type_c.h"
#include "index/ScalarIndex.h"
#include "index/StringIndex.h"
#include <string>

namespace milvus::scalar {

class IndexFactory {
 public:
    IndexFactory() = default;
    IndexFactory(const IndexFactory&) = delete;
    IndexFactory
    operator=(const IndexFactory&) = delete;

 public:
    static IndexFactory&
    GetInstance() {
        // thread-safe enough after c++ 11
        static IndexFactory instance;
        return instance;
    }

    IndexBasePtr
    CreateIndex(CDataType dtype, const std::string& index_type);

    template <typename T>
    ScalarIndexPtr<T>
    CreateIndex(const std::string& index_type);
};

}  // namespace milvus::scalar

#include "index/IndexFactory-inl.h"
