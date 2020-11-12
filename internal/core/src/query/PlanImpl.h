#pragma once

#include "Plan.h"
#include "utils/Json.h"
#include "PlanNode.h"
#include "utils/EasyAssert.h"
#include "pb/service_msg.pb.h"
#include <vector>
#include <boost/align/aligned_allocator.hpp>

namespace milvus::query {
using Json = nlohmann::json;

// class definitions
struct Plan {
    std::unique_ptr<VectorPlanNode> plan_node_;
    // TODO: add move extra info
};

template <typename T>
using aligned_vector = std::vector<T, boost::alignment::aligned_allocator<T, 512>>;

struct Placeholder {
    // milvus::proto::service::PlaceholderGroup group_;
    std::string tag_;
    int64_t num_of_queries_;
    int64_t line_sizeof_;
    aligned_vector<char> blob_;

    template <typename T>
    const T*
    get_blob() const {
        return reinterpret_cast<const T*>(blob_.data());
    }

    template <typename T>
    T*
    get_blob() {
        return reinterpret_cast<T*>(blob_.data());
    }
};

struct PlaceholderGroup : std::vector<Placeholder> {
    using std::vector<Placeholder>::vector;
};
}  // namespace milvus::query