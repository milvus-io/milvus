#include "common/JsonCastFunction.h"
#include <string>
#include "common/EasyAssert.h"

namespace milvus {

const std::unordered_map<std::string, JsonCastFunction>
    JsonCastFunction::predefined_cast_functions_ = {
        {"STRING_TO_DOUBLE", JsonCastFunction(Type::kString2Double)},
};

JsonCastFunction
JsonCastFunction::FromString(const std::string& str) {
    auto it = predefined_cast_functions_.find(str);
    if (it != predefined_cast_functions_.end()) {
        return it->second;
    }
    return JsonCastFunction(Type::kUnknown);
}

template <>
std::optional<double>
JsonCastFunction::cast<double, std::string>(const std::string& t) const {
    try {
        return std::stod(t);
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

template <>
std::optional<double>
JsonCastFunction::cast<double, int64_t>(const int64_t& t) const {
    return static_cast<double>(t);
}

template <>
std::optional<double>
JsonCastFunction::cast<double, double>(const double& t) const {
    return t;
}

template <>
std::optional<double>
JsonCastFunction::cast<double, bool>(const bool& t) const {
    return std::nullopt;
}

template <typename T>
std::optional<T>
JsonCastFunction::CastJsonValue(const JsonCastFunction& cast_function,
                                const Json& json,
                                const std::string& pointer) {
    AssertInfo(cast_function.match<T>(), "Type mismatch");

    auto json_type = json.type(pointer);
    std::optional<T> res;

    switch (json_type.value()) {
        case simdjson::ondemand::json_type::string: {
            auto json_value = json.at<std::string_view>(pointer);
            res = cast_function.cast<T, std::string>(
                std::string(json_value.value()));
            break;
        }

        case simdjson::ondemand::json_type::number: {
            if (json.get_number_type(pointer) ==
                simdjson::ondemand::number_type::floating_point_number) {
                auto json_value = json.at<double>(pointer);
                res = cast_function.cast<T, double>(json_value.value());
            } else {
                auto json_value = json.at<int64_t>(pointer);
                res = cast_function.cast<T, int64_t>(json_value.value());
            }
            break;
        }

        case simdjson::ondemand::json_type::boolean: {
            auto json_value = json.at<bool>(pointer);
            res = cast_function.cast<T, bool>(json_value.value());
            break;
        }

        default:
            break;
    }

    return res;
}

template std::optional<bool>
JsonCastFunction::CastJsonValue<bool>(const JsonCastFunction& cast_function,
                                      const Json& json,
                                      const std::string& pointer);

template std::optional<int64_t>
JsonCastFunction::CastJsonValue<int64_t>(const JsonCastFunction& cast_function,
                                         const Json& json,
                                         const std::string& pointer);

template std::optional<double>
JsonCastFunction::CastJsonValue<double>(const JsonCastFunction& cast_function,
                                        const Json& json,
                                        const std::string& pointer);

template std::optional<std::string>
JsonCastFunction::CastJsonValue<std::string>(
    const JsonCastFunction& cast_function,
    const Json& json,
    const std::string& pointer);

}  // namespace milvus
