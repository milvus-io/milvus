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

#include <string>
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/BoolIndex.h"

namespace milvus::index {

template <typename T>
inline ScalarIndexPtr<T>
IndexFactory::CreateScalarIndex(const IndexType& index_type,
                                storage::FileManagerImplPtr file_manager) {
    return CreateScalarIndexSort<T>(file_manager);
}

// template <>
// inline ScalarIndexPtr<bool>
// IndexFactory::CreateScalarIndex(const IndexType& index_type) {
//    return CreateBoolIndex();
//}

template <>
inline ScalarIndexPtr<std::string>
IndexFactory::CreateScalarIndex(const IndexType& index_type,
                                storage::FileManagerImplPtr file_manager) {
#if defined(__linux__) || defined(__APPLE__)
    return CreateStringIndexMarisa(file_manager);
#else
    throw std::runtime_error("unsupported platform");
#endif
}

}  // namespace milvus::index
