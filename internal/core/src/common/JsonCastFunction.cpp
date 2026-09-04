#include "common/JsonCastFunction.h"

#include <simdjson.h>
#include <cstdint>
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
    // Parse the string as a complete JSON number document. Besides avoiding
    // platform-dependent std::stod behavior, this accepts representable
    // subnormals and underflow-to-zero while rejecting overflow and trailing
    // non-whitespace content.
    simdjson::padded_string number_token(t);
    thread_local simdjson::ondemand::parser parser;
    auto document = parser.iterate(number_token);
    if (document.error() != simdjson::SUCCESS) {
        return std::nullopt;
    }

    // get_number() preserves simdjson's integer-domain validation. Calling
    // document.get_double() directly would approximate integers larger than
    // uint64_t even though the regular raw-JSON path rejects them.
    auto number = document.get_number();
    if (number.error() != simdjson::SUCCESS) {
        return std::nullopt;
    }
    return number.value().as_double();
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
    if (json_type.error() != simdjson::SUCCESS) {
        return std::nullopt;
    }

    std::optional<T> res;

    switch (json_type.value()) {
        case simdjson::ondemand::json_type::string: {
            auto json_value = json.at<std::string_view>(pointer);
            if (json_value.error() != simdjson::SUCCESS) {
                return std::nullopt;
            }
            res = cast_function.cast<T, std::string>(
                std::string(json_value.value()));
            break;
        }

        case simdjson::ondemand::json_type::number: {
            // STRING_TO_DOUBLE accepts numeric JSON values as identity casts.
            // Parse into simdjson's tagged number first so integers outside the
            // uint64_t domain remain invalid instead of being approximated by
            // get_double().
            auto json_value = json.at_numeric(pointer);
            if (json_value.error() != simdjson::SUCCESS) {
                return std::nullopt;
            }
            res = cast_function.cast<T, double>(json_value.value().as_double());
            break;
        }

        case simdjson::ondemand::json_type::boolean: {
            auto json_value = json.at<bool>(pointer);
            if (json_value.error() != simdjson::SUCCESS) {
                return std::nullopt;
            }
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
