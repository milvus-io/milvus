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

#include "exec/expression/function/FunctionFactory.h"
#include <mutex>
#include "exec/expression/function/impl/StringFunctions.h"
#include "log/Log.h"

namespace milvus {
namespace exec {
namespace expression {

std::string
FilterFunctionRegisterKey::ToString() const {
    std::ostringstream oss;
    oss << func_name << "(";
    for (size_t i = 0; i < func_param_type_list.size(); ++i) {
        oss << GetDataTypeName(func_param_type_list[i]);
        if (i < func_param_type_list.size() - 1) {
            oss << ", ";
        }
    }

    oss << ")";
    return oss.str();
}

FunctionFactory&
FunctionFactory::Instance() {
    static FunctionFactory factory;
    return factory;
}

void
FunctionFactory::Initialize() {
    std::call_once(init_flag_, &FunctionFactory::RegisterAllFunctions, this);
}

void
FunctionFactory::RegisterAllFunctions() {
    RegisterFilterFunction(
        "empty", {DataType::VARCHAR}, function::EmptyVarchar);
    RegisterFilterFunction("starts_with",
                           {DataType::VARCHAR, DataType::VARCHAR},
                           function::StartsWithVarchar);
    LOG_INFO("{} functions registered", GetFilterFunctionNum());
}

void
FunctionFactory::RegisterFilterFunction(
    std::string func_name,
    std::vector<DataType> func_param_type_list,
    FilterFunctionPtr func) {
    filter_function_map_[FilterFunctionRegisterKey{
        func_name, func_param_type_list}] = func;
}

const FilterFunctionPtr
FunctionFactory::GetFilterFunction(
    const FilterFunctionRegisterKey& func_sig) const {
    auto iter = filter_function_map_.find(func_sig);
    if (iter != filter_function_map_.end()) {
        return iter->second;
    }
    return nullptr;
}

}  // namespace expression
}  // namespace exec
}  // namespace milvus
