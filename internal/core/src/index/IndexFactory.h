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

#include <memory>
#include <string>
#include <mutex>
#include <shared_mutex>

#include "common/JsonCastType.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "index/VectorIndex.h"
#include "index/IndexInfo.h"
#include "storage/Types.h"
#include "storage/FileManager.h"
#include "index/StringIndexMarisa.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/BoolIndex.h"
#include "segcore/load_index_c.h"

namespace milvus::index {

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

    LoadResourceRequest
    IndexLoadResource(DataType field_type,
                      IndexVersion index_version,
                      float index_size,
                      const std::map<std::string, std::string>& index_params,
                      bool mmap_enable);

    LoadResourceRequest
    VecIndexLoadResource(DataType field_type,
                         IndexVersion index_version,
                         float index_size,
                         const std::map<std::string, std::string>& index_params,
                         bool mmap_enable);

    LoadResourceRequest
    ScalarIndexLoadResource(
        DataType field_type,
        IndexVersion index_version,
        float index_size,
        const std::map<std::string, std::string>& index_params,
        bool mmap_enable);

    IndexBasePtr
    CreateIndex(const CreateIndexInfo& create_index_info,
                const storage::FileManagerContext& file_manager_context,
                bool use_build_pool = true);

    IndexBasePtr
    CreateVectorIndex(const CreateIndexInfo& create_index_info,
                      const storage::FileManagerContext& file_manager_context,
                      bool use_knowhere_build_pool_ = true);

    // For base types like int, float, double, string, etc
    IndexBasePtr
    CreatePrimitiveScalarIndex(
        DataType data_type,
        const CreateIndexInfo& create_index_info,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    // For types like array, struct, union, etc
    IndexBasePtr
    CreateCompositeScalarIndex(
        const CreateIndexInfo& create_index_info,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    // For types like Json, XML, etc
    IndexBasePtr
    CreateComplexScalarIndex(
        IndexType index_type,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    IndexBasePtr
    CreateJsonIndex(IndexType index_type,
                    JsonCastType cast_dtype,
                    const std::string& nested_path,
                    const storage::FileManagerContext& file_manager_context =
                        storage::FileManagerContext());

    IndexBasePtr
    CreateScalarIndex(const CreateIndexInfo& create_index_info,
                      const storage::FileManagerContext& file_manager_context =
                          storage::FileManagerContext());

    // IndexBasePtr
    // CreateIndex(DataType dtype, const IndexType& index_type);
 private:
    FRIEND_TEST(StringIndexMarisaTest, Reverse);

    template <typename T>
    ScalarIndexPtr<T>
    CreatePrimitiveScalarIndex(const CreateIndexInfo& create_index_info,
                               const storage::FileManagerContext& file_manager =
                                   storage::FileManagerContext());
};

}  // namespace milvus::index
