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

#include <string>
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/IndexType.h"
#include "index/BoolIndex.h"

namespace milvus::scalar {

template <typename T>
inline ScalarIndexPtr<T>
IndexFactory::CreateIndex(const std::string& index_type) {
    return CreateScalarIndexSort<T>();
}

template <>
inline ScalarIndexPtr<bool>
IndexFactory::CreateIndex(const std::string& index_type) {
    return CreateBoolIndex();
}

template <>
inline ScalarIndexPtr<std::string>
IndexFactory::CreateIndex(const std::string& index_type) {
#if defined(__linux__) || defined(__APPLE__)
    return CreateStringIndexMarisa();
#else
    throw std::runtime_error("unsupported platform");
#endif
}

}  // namespace milvus::scalar
