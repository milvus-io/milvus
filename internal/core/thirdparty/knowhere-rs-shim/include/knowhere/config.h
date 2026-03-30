#pragma once

#include <cctype>
#include <cstdint>
#include <limits>
#include <list>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "knowhere/comp/index_param.h"
#include "knowhere/expected.h"
#include "knowhere/sparse_utils.h"

namespace knowhere {

using Json = nlohmann::json;
using Config = Json;

using CFG_INT = std::optional<int32_t>;
using CFG_STRING = std::optional<std::string>;
using CFG_FLOAT = std::optional<float>;
using CFG_LIST = std::optional<std::list<int>>;
using CFG_BOOL = std::optional<bool>;

enum PARAM_TYPE {
    TRAIN = 1 << 0,
    SEARCH = 1 << 1,
    RANGE_SEARCH = 1 << 2,
    FEDER = 1 << 3,
    DESERIALIZE = 1 << 4,
    DESERIALIZE_FROM_FILE = 1 << 5,
};

class BaseConfig {
 public:
    virtual ~BaseConfig() = default;

    virtual Status
    CheckAndAdjustForBuild(std::string* = nullptr) {
        return Status::success;
    }

    virtual Status
    CheckAndAdjustForSearch(std::string* = nullptr) {
        return Status::success;
    }

    virtual Status
    CheckAndAdjustForRangeSearch(std::string* = nullptr) {
        return Status::success;
    }
};

struct Resource {
    uint64_t memoryCost = 0;
    uint64_t diskCost = 0;
};

namespace feature {
enum Type {
    MMAP = 0,
    LAZY_LOAD = 1,
};
}  // namespace feature

inline bool
IndexTypeHasRawData(const std::string& name) {
    return name == IndexEnum::INDEX_HNSW ||
           name == IndexEnum::INDEX_FAISS_IDMAP ||
           name == IndexEnum::INDEX_FAISS_IVFFLAT ||
           name == IndexEnum::INDEX_FAISS_IVFFLAT_CC ||
           name == IndexEnum::INDEX_FAISS_IVFPQ ||
           name == IndexEnum::INDEX_FAISS_IVFSQ8 ||
           name == IndexEnum::INDEX_FAISS_IVFSQ ||
           name == IndexEnum::INDEX_FAISS_SCANN_DVR ||
           name == IndexEnum::INDEX_DISKANN ||
           name == IndexEnum::INDEX_FAISS_BIN_IDMAP ||
           name == IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
}

inline std::string
RenderSearchJsonValue(const Json& value) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    return value.dump();
}

inline Status
InvalidSearchParam(std::string& message, std::string text) {
    message = std::move(text);
    return Status::invalid_args;
}

inline std::optional<int64_t>
GetConfigIntegral(const Config& config, const char* key) {
    if (!config.contains(key)) {
        return std::nullopt;
    }

    const auto& value = config.at(key);
    if (value.is_number_integer()) {
        return value.get<int64_t>();
    }
    if (value.is_number_unsigned()) {
        const auto raw = value.get<uint64_t>();
        if (raw > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return std::nullopt;
        }
        return static_cast<int64_t>(raw);
    }
    if (value.is_string()) {
        const auto raw = value.get<std::string>();
        if (raw.empty()) {
            return std::nullopt;
        }
        size_t pos = 0;
        try {
            const auto parsed = std::stoll(raw, &pos);
            if (pos == raw.size()) {
                return parsed;
            }
        } catch (...) {
        }
    }
    return std::nullopt;
}

inline const Json*
FindSearchParamValue(const Config& config, const char* key) {
    if (config.contains(key)) {
        return &config.at(key);
    }
    if (config.contains("params")) {
        const auto& params = config.at("params");
        if (params.is_object() && params.contains(key)) {
            return &params.at(key);
        }
    }
    return nullptr;
}

inline Status
ValidateSearchIntegerParam(const Config& config,
                           const char* key,
                           int64_t min,
                           int64_t max,
                           std::string& message) {
    const auto* value_ptr = FindSearchParamValue(config, key);
    if (value_ptr == nullptr) {
        return Status::success;
    }

    const auto& value = *value_ptr;
    int64_t parsed = 0;

    if (value.is_number_integer()) {
        parsed = value.get<int64_t>();
    } else if (value.is_number_unsigned()) {
        const auto raw = value.get<uint64_t>();
        if (raw > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return InvalidSearchParam(
                message,
                "Out of range in json: param '" + std::string(key) + "' (" +
                    std::to_string(raw) + ") should be in range [" +
                    std::to_string(min) + ", " + std::to_string(max) + "]");
        }
        parsed = static_cast<int64_t>(raw);
    } else if (value.is_string()) {
        const auto raw = value.get<std::string>();
        size_t pos = 0;
        try {
            parsed = std::stoll(raw, &pos);
        } catch (...) {
            pos = 0;
        }
        if (raw.empty() || pos != raw.size()) {
            return InvalidSearchParam(
                message,
                "Type conflict in json: param '" + std::string(key) + "' (" +
                    raw + ") should be integer");
        }
    } else if (value.is_number_float() || value.is_boolean() ||
               value.is_null()) {
        return InvalidSearchParam(
            message,
            "Type conflict in json: param '" + std::string(key) + "' (" +
                RenderSearchJsonValue(value) + ") should be integer");
    } else {
        return InvalidSearchParam(
            message,
            "param '" + std::string(key) + "' (" + RenderSearchJsonValue(value) +
                    ") should be integer");
    }

    if (parsed < min || parsed > max) {
        return InvalidSearchParam(
            message,
            "Out of range in json: param '" + std::string(key) + "' (" +
                std::to_string(parsed) + ") should be in range [" +
                std::to_string(min) + ", " + std::to_string(max) + "]");
    }

    return Status::success;
}

inline Status
ValidateSearchRefineKParam(const Config& config, std::string& message) {
    const auto* value_ptr = FindSearchParamValue(config, indexparam::REFINE_K);
    if (value_ptr == nullptr) {
        return Status::success;
    }

    const auto& value = *value_ptr;
    double refine_k = 0.0;

    if (value.is_number()) {
        refine_k = value.get<double>();
    } else if (value.is_string()) {
        const auto raw = value.get<std::string>();
        if (raw.empty()) {
            return InvalidSearchParam(
                message,
                "invalid float value, key: 'refine_k', value: '': invalid parameter");
        }

        size_t pos = 0;
        try {
            refine_k = std::stod(raw, &pos);
        } catch (...) {
            pos = 0;
        }
        if (pos != raw.size()) {
            return InvalidSearchParam(
                message,
                "invalid float value, key: 'refine_k', value: '" + raw +
                    "': invalid parameter");
        }
    } else {
        return InvalidSearchParam(
            message,
            "Type conflict in json: param 'refine_k' (" +
                RenderSearchJsonValue(value) + ") should be a number");
    }

    if (refine_k < 1.0 ||
        refine_k > static_cast<double>(std::numeric_limits<float>::max())) {
        return InvalidSearchParam(
            message,
            "Out of range in json: param 'refine_k' (" +
                RenderSearchJsonValue(value) + ") should be in range [1.000000, " +
                std::to_string(std::numeric_limits<float>::max()) + "]");
    }

    return Status::success;
}

inline Status
ValidateHnswSearchConfig(const Config& config, std::string& message) {
    message.clear();
    const auto* value_ptr = FindSearchParamValue(config, indexparam::EF);
    if (value_ptr != nullptr) {
        const auto& value = *value_ptr;
        int64_t ef = 0;

        if (value.is_number_integer()) {
            ef = value.get<int64_t>();
        } else if (value.is_number_unsigned()) {
            const auto raw = value.get<uint64_t>();
            if (raw >
                static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
                return InvalidSearchParam(
                    message,
                    "param 'ef' (" + std::to_string(raw) +
                        ") should be in range [1, 2147483647]");
            }
            ef = static_cast<int64_t>(raw);
        } else if (value.is_string()) {
            const auto raw = value.get<std::string>();
            size_t pos = 0;
            try {
                ef = std::stoll(raw, &pos);
            } catch (...) {
                pos = 0;
            }
            if (raw.empty() || pos != raw.size()) {
                return InvalidSearchParam(
                    message,
                    "Type conflict in json: param 'ef' (" + raw +
                        ") should be integer");
            }
        } else if (value.is_number_float() || value.is_boolean() ||
                   value.is_null()) {
            return InvalidSearchParam(
                message,
                "Type conflict in json: param 'ef' (" +
                    RenderSearchJsonValue(value) + ") should be integer");
        } else {
            return InvalidSearchParam(
                message,
                "param 'ef' (" + RenderSearchJsonValue(value) +
                    ") should be integer");
        }

        if (ef < 1 || ef > std::numeric_limits<int32_t>::max()) {
            return InvalidSearchParam(
                message,
                "param 'ef' (" + std::to_string(ef) +
                    ") should be in range [1, 2147483647]");
        }

        if (const auto topk = GetConfigIntegral(config, meta::TOPK);
            topk.has_value() && *topk > 0 && ef < *topk) {
            return InvalidSearchParam(
                message,
                "ef(" + std::to_string(ef) + ") should be larger than k(" +
                    std::to_string(*topk) + ")");
        }
    }

    return ValidateSearchRefineKParam(config, message);
}

inline Status
ValidateIvfRabitqSearchConfig(const Config& config, std::string& message) {
    message.clear();
    if (const auto status =
            ValidateSearchIntegerParam(config, "nprobe", 1, 65536, message);
        status != Status::success) {
        return status;
    }

    if (const auto status = ValidateSearchIntegerParam(
            config, "rbq_bits_query", 0, 8, message);
        status != Status::success) {
        return status;
    }

    return ValidateSearchRefineKParam(config, message);
}

inline Status
ValidateDiskannSearchConfig(const Config& config, std::string& message) {
    message.clear();
    const auto* value_ptr = FindSearchParamValue(config, "search_list_size");
    if (value_ptr == nullptr) {
        return Status::success;
    }

    const auto& value = *value_ptr;
    int64_t search_list_size = 0;

    if (value.is_number_integer()) {
        search_list_size = value.get<int64_t>();
    } else if (value.is_number_unsigned()) {
        const auto raw = value.get<uint64_t>();
        if (raw > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
            return InvalidSearchParam(
                message,
                "param 'search_list_size' (" + std::to_string(raw) +
                    ") should be in range [1, 2147483647]");
        }
        search_list_size = static_cast<int64_t>(raw);
    } else if (value.is_string()) {
        const auto raw = value.get<std::string>();
        size_t pos = 0;
        try {
            search_list_size = std::stoll(raw, &pos);
        } catch (...) {
            pos = 0;
        }
        if (raw.empty() || pos != raw.size()) {
            return InvalidSearchParam(
                message,
                "Type conflict in json: param 'search_list_size' (" + raw +
                    ") should be integer");
        }
    } else if (value.is_number_float() || value.is_boolean() ||
               value.is_null()) {
        return InvalidSearchParam(
            message,
            "Type conflict in json: param 'search_list_size' (" +
                RenderSearchJsonValue(value) + ") should be integer");
    } else {
        return InvalidSearchParam(
            message,
            "param 'search_list_size' (" + RenderSearchJsonValue(value) +
                ") should be integer");
    }

    if (search_list_size < 1 ||
        search_list_size > std::numeric_limits<int32_t>::max()) {
        return InvalidSearchParam(
            message,
            "param 'search_list_size' (" + std::to_string(search_list_size) +
                ") should be in range [1, 2147483647]");
    }

    if (const auto topk = GetConfigIntegral(config, meta::TOPK);
        topk.has_value() && *topk > 0 && search_list_size <= *topk) {
        return InvalidSearchParam(
            message,
            "search_list_size(" + std::to_string(search_list_size) +
                ") should be larger than k(" + std::to_string(*topk) + ")");
    }

    return Status::success;
}

inline std::string
ToLowerAscii(std::string raw) {
    for (char& ch : raw) {
        ch = static_cast<char>(
            std::tolower(static_cast<unsigned char>(ch)));
    }
    return raw;
}

inline std::string
RenderPythonStyleValue(const Json& value) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    if (value.is_boolean()) {
        return value.get<bool>() ? "True" : "False";
    }
    if (value.is_array()) {
        std::string rendered = "[";
        bool first = true;
        for (const auto& item : value) {
            if (!first) {
                rendered += ", ";
            }
            first = false;
            if (item.is_string()) {
                rendered += "'";
                rendered += item.get<std::string>();
                rendered += "'";
            } else {
                rendered += RenderPythonStyleValue(item);
            }
        }
        rendered += "]";
        return rendered;
    }
    return value.dump();
}

template <typename T>
struct IndexStaticFaced {
    static std::optional<int64_t>
    GetIntegral(const Config& config, const char* key) {
        return GetConfigIntegral(config, key);
    }

    static Status
    Invalid(std::string& message, std::string text) {
        message = std::move(text);
        return Status::invalid_args;
    }

    static std::string
    RenderJsonValue(const Json& value) {
        if (value.is_boolean()) {
            return value.get<bool>() ? "True" : "False";
        }
        if (value.is_string()) {
            return value.get<std::string>();
        }
        return value.dump();
    }

    static Status
    ValidateIntegerParam(const Config& config,
                         const char* key,
                         int64_t min,
                         int64_t max,
                         std::string& message) {
        if (!config.contains(key)) {
            return Status::success;
        }

        const auto& value = config.at(key);
        if (value.is_null()) {
            return Status::success;
        }
        if (value.is_number_float()) {
            return Invalid(message, "wrong data type in json");
        }
        if (value.is_boolean() || value.is_array() || value.is_object()) {
            return Invalid(message,
                           "invalid integer value, key: '" + std::string(key) +
                               "', value: '" + RenderJsonValue(value) +
                               "': invalid parameter");
        }
        if (value.is_string()) {
            const auto raw = value.get<std::string>();
            if (!raw.empty()) {
                size_t pos = 0;
                try {
                    static_cast<void>(std::stoll(raw, &pos));
                    if (pos != raw.size()) {
                        return Invalid(message, "wrong data type in json");
                    }
                } catch (...) {
                }
            }
        }

        const auto parsed = GetIntegral(config, key);
        if (!parsed.has_value()) {
            return Invalid(message,
                           "invalid integer value, key: '" + std::string(key) +
                               "', value: '" + RenderJsonValue(value) +
                               "': invalid parameter");
        }
        if (*parsed < min || *parsed > max) {
            return Invalid(message,
                           "param '" + std::string(key) + "' (" +
                               std::to_string(*parsed) +
                               ") should be in range [" + std::to_string(min) +
                               ", " + std::to_string(max) + "]");
        }
        return Status::success;
    }

    static Status
    ValidateBooleanLikeParam(const Config& config,
                             const char* key,
                             bool python_style_error,
                             std::string& message) {
        if (!config.contains(key)) {
            return Status::success;
        }

        const auto& value = config.at(key);
        if (value.is_null() || value.is_boolean()) {
            return Status::success;
        }

        if (value.is_string()) {
            const auto raw = ToLowerAscii(value.get<std::string>());
            if (raw == "true" || raw == "false") {
                return Status::success;
            }
        }

        if (python_style_error) {
            return Invalid(message,
                           "Type conflict in json: param '" +
                               std::string(key) + "' (\"" +
                               RenderJsonValue(value) +
                               "\") should be a boolean");
        }
        return Invalid(message, "should be a boolean: invalid parameter");
    }

    static Status
    ValidateEnumParam(const Config& config,
                      const char* key,
                      std::initializer_list<const char*> allowed_values,
                      std::string invalid_message,
                      std::string& message) {
        if (!config.contains(key)) {
            return Status::success;
        }

        const auto& value = config.at(key);
        if (value.is_null()) {
            return Status::success;
        }

        if (value.is_string()) {
            const auto lowered = ToLowerAscii(value.get<std::string>());
            for (const auto* candidate : allowed_values) {
                if (lowered == candidate) {
                    return Status::success;
                }
            }
        }

        return Invalid(message, std::move(invalid_message));
    }

    static std::unique_ptr<BaseConfig>
    CreateConfig(const std::string&, int64_t) {
        return std::make_unique<BaseConfig>();
    }

    static Status
    ConfigCheck(const std::string& name,
                int64_t,
                const Config& config,
                std::string& message) {
        message.clear();

        if (name == IndexEnum::INDEX_FAISS_SCANN_DVR) {
            if (const auto nlist = GetIntegral(config, indexparam::NLIST);
                nlist.has_value() && (*nlist < 1 || *nlist > 65536)) {
                return Invalid(
                    message,
                    "Out of range in json: param 'nlist' (" +
                        std::to_string(*nlist) +
                        ") should be in range [1, 65536]");
            }

            const auto dim = GetIntegral(config, meta::DIM);
            const auto sub_dim =
                GetIntegral(config, indexparam::SUB_DIM).value_or(2);
            if (dim.has_value() && *dim > 0 && sub_dim > 0 &&
                (*dim % sub_dim) != 0) {
                return Invalid(
                    message,
                    "The dimension of a vector (dim) should be a multiple of "
                    "sub_dim. Dimension:" +
                        std::to_string(*dim) + ", sub_dim:" +
                        std::to_string(sub_dim) + ": invalid parameter");
            }
        }

        const bool validates_nlist =
            name == IndexEnum::INDEX_FAISS_IVFFLAT ||
            name == IndexEnum::INDEX_FAISS_IVFFLAT_CC ||
            name == IndexEnum::INDEX_FAISS_IVFPQ ||
            name == IndexEnum::INDEX_FAISS_IVFSQ8 ||
            name == IndexEnum::INDEX_FAISS_IVFSQ ||
            name == IndexEnum::INDEX_FAISS_SCANN_DVR;
        if (validates_nlist) {
            if (const auto nlist = GetIntegral(config, indexparam::NLIST);
                nlist.has_value() && (*nlist < 1 || *nlist > 65536)) {
                return Invalid(message, "Out of range in json: param 'nlist'");
            }
        }

        if (name == IndexEnum::INDEX_FAISS_IVF_RABITQ) {
            if (const auto status = ValidateIntegerParam(
                    config, indexparam::NLIST, 1, 65536, message);
                status != Status::success) {
                return status;
            }

            if (const auto status = ValidateBooleanLikeParam(
                    config, indexparam::REFINE, true, message);
                status != Status::success) {
                return status;
            }

            if (config.contains(indexparam::REFINE_TYPE)) {
                const auto value =
                    RenderPythonStyleValue(config.at(indexparam::REFINE_TYPE));
                if (const auto status = ValidateEnumParam(
                        config,
                        indexparam::REFINE_TYPE,
                        {"sq6", "sq8", "fp16", "bf16", "fp32", "flat"},
                        "invalid refine type : " + value +
                            ", optional types are [sq6, sq8, fp16, bf16, fp32, flat]",
                        message);
                    status != Status::success) {
                    return status;
                }
            }
        }

        if (name == IndexEnum::INDEX_FAISS_IVFPQ) {
            if (const auto nbits = GetIntegral(config, indexparam::NBITS);
                nbits.has_value() && (*nbits < 1 || *nbits > 16)) {
                return Invalid(message, "Out of range in json: param 'nbits'");
            }

            const auto m = GetIntegral(config, indexparam::IVFM);
            const auto dim = GetIntegral(config, meta::DIM);
            if (m.has_value() && dim.has_value() && *m > 0 && *dim > 0 &&
                (*dim % *m) != 0) {
                return Invalid(
                    message,
                    "The dimension of a vector (dim) should be a multiple of the number of subquantizers (m)");
            }
        }

        if (name == IndexEnum::INDEX_HNSW ||
            name == IndexEnum::INDEX_HNSW_SQ) {
            if (const auto status =
                    ValidateIntegerParam(config, indexparam::M, 2, 2048, message);
                status != Status::success) {
                return status;
            }

            if (const auto status = ValidateIntegerParam(
                    config,
                    indexparam::EFCONSTRUCTION,
                    1,
                    std::numeric_limits<int32_t>::max(),
                    message);
                status != Status::success) {
                return status;
            }
        }

        if (name == IndexEnum::INDEX_HNSW_SQ) {
            if (const auto status = ValidateEnumParam(
                    config,
                    indexparam::SQ_TYPE,
                    {"sq6", "sq8", "bf16", "fp16"},
                    "invalid scalar quantizer type: invalid parameter",
                    message);
                status != Status::success) {
                return status;
            }

            if (const auto status = ValidateBooleanLikeParam(
                    config, indexparam::REFINE, false, message);
                status != Status::success) {
                return status;
            }

            if (config.contains(indexparam::REFINE_TYPE)) {
                const auto value =
                    RenderPythonStyleValue(config.at(indexparam::REFINE_TYPE));
                if (const auto status = ValidateEnumParam(
                        config,
                        indexparam::REFINE_TYPE,
                        {"sq4u", "sq6", "sq8", "fp16", "bf16", "fp32", "flat"},
                        "invalid refine type : " + value +
                            ", optional types are [sq4u, sq6, sq8, fp16, bf16, fp32, flat]: invalid parameter",
                        message);
                    status != Status::success) {
                    return status;
                }
            }
        }

        if (name == IndexEnum::INDEX_DISKANN) {
            if (const auto status =
                    ValidateDiskannSearchConfig(config, message);
                status != Status::success) {
                return status;
            }
        }

        if (name == IndexEnum::INDEX_FAISS_IVF_RABITQ) {
            if (const auto status =
                    ValidateIvfRabitqSearchConfig(config, message);
                status != Status::success) {
                return status;
            }
        }

        return Status::success;
    }

    static expected<Resource>
    EstimateLoadResource(const std::string&,
                         int64_t,
                         uint64_t index_size_in_bytes,
                         int64_t,
                         int64_t,
                         const Config&) {
        Resource resource{};
        resource.memoryCost = index_size_in_bytes;
        resource.diskCost = 0;
        return resource;
    }

    static bool
    HasRawData(const std::string& name, int64_t, const Config&) {
        return IndexTypeHasRawData(name);
    }
};

inline bool
UseDiskLoad(const std::string&, int64_t) {
    return false;
}

}  // namespace knowhere
