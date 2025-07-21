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

#include <functional>
#include <optional>
#include <string>
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonCastType.h"

namespace milvus {

class JsonCastFunction {
 public:
    enum class Type {
        kUnknown,
        kString2Double,
    };

    template <typename T, typename F>
    std::optional<T>
    cast(const F& t) const {
        ThrowInfo(Unsupported, "Not implemented");
    }

    template <typename T>
    bool
    match() const {
        switch (cast_function_type_) {
            case Type::kString2Double:
                return std::is_same_v<T, double>;
        }
        return false;
    }

    static JsonCastFunction
    FromString(const std::string& str);

    template <typename T>
    static std::optional<T>
    CastJsonValue(const JsonCastFunction& cast_function,
                  const Json& json,
                  const std::string& pointer);

 private:
    JsonCastFunction(Type type) : cast_function_type_(type) {
    }
    Type cast_function_type_;

    static const std::unordered_map<std::string, JsonCastFunction>
        predefined_cast_functions_;
};
template <>
std::optional<double>
JsonCastFunction::cast<double, std::string>(const std::string& t) const;

template <>
std::optional<double>
JsonCastFunction::cast<double, int64_t>(const int64_t& t) const;

template <>
std::optional<double>
JsonCastFunction::cast<double, double>(const double& t) const;

template <>
std::optional<double>
JsonCastFunction::cast<double, bool>(const bool& t) const;

}  // namespace milvus
