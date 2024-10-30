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

#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/variant.hpp>
#include "common/Vector.h"

namespace milvus {
namespace exec {

class EvalCtx;
class Expr;
class PhyCallExpr;

namespace expression {

struct FilterFunctionRegisterKey {
    std::string func_name;
    std::vector<DataType> func_param_type_list;

    std::string
    ToString() const;

    bool
    operator==(const FilterFunctionRegisterKey& other) const {
        return func_name == other.func_name &&
               func_param_type_list == other.func_param_type_list;
    }

    struct Hash {
        size_t
        operator()(const FilterFunctionRegisterKey& s) const {
            size_t h1 = std::hash<std::string_view>{}(s.func_name);
            size_t h2 = boost::hash_range(s.func_param_type_list.begin(),
                                          s.func_param_type_list.end());
            return h1 ^ h2;
        }
    };
};

using FilterFunctionParameter = std::shared_ptr<Expr>;
using FilterFunctionReturn = VectorPtr;
using FilterFunctionPtr = void (*)(const RowVector& args,
                                   FilterFunctionReturn& result);

class FunctionFactory {
 public:
    static FunctionFactory&
    Instance();

    void
    Initialize();

    void
    RegisterFilterFunction(std::string func_name,
                           std::vector<DataType> func_param_type_list,
                           FilterFunctionPtr func);

    const FilterFunctionPtr
    GetFilterFunction(const FilterFunctionRegisterKey& func_sig) const;

    size_t
    GetFilterFunctionNum() const {
        return filter_function_map_.size();
    }

    std::vector<FilterFunctionRegisterKey>
    ListAllFilterFunctions() const {
        std::vector<FilterFunctionRegisterKey> result;
        for (const auto& [key, value] : filter_function_map_) {
            result.push_back(key);
        }
        return result;
    }

 private:
    void
    RegisterAllFunctions();

    std::unordered_map<FilterFunctionRegisterKey,
                       FilterFunctionPtr,
                       FilterFunctionRegisterKey::Hash>
        filter_function_map_;
    std::once_flag init_flag_;
};

}  // namespace expression
}  // namespace exec
}  // namespace milvus
