#pragma once

#include <cstdint>
#include <list>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "knowhere/expected.h"

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

template <typename T>
struct IndexStaticFaced {
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
    HasRawData(const std::string&, int64_t, const Config&) {
        return false;
    }
};

inline bool
UseDiskLoad(const std::string&, int64_t) {
    return false;
}

namespace sparse {
template <typename T>
class SparseRow {
 public:
    SparseRow() = default;
};
}  // namespace sparse

}  // namespace knowhere
