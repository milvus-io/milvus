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
        PanicInfo(Unsupported, "Not implemented");
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
