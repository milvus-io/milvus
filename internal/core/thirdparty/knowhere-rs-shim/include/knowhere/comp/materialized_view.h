#pragma once

#include <cstdint>
#include <string_view>
#include <unordered_map>

#include <nlohmann/json.hpp>

namespace knowhere {

struct MaterializedViewSearchInfo {
    std::unordered_map<int64_t, uint64_t> field_id_to_touched_categories_cnt;
    bool is_pure_and = true;
    bool has_not = false;
};

inline void
to_json(nlohmann::json& j, const MaterializedViewSearchInfo& info) {
    constexpr std::string_view kFieldIdToTouchedCategoriesCntKey =
        "field_id_to_touched_categories_cnt";
    constexpr std::string_view kIsPureAndKey = "is_pure_and";
    constexpr std::string_view kHasNotKey = "has_not";

    j = nlohmann::json{
        {kFieldIdToTouchedCategoriesCntKey,
         info.field_id_to_touched_categories_cnt},
        {kIsPureAndKey, info.is_pure_and},
        {kHasNotKey, info.has_not},
    };
}

inline void
from_json(const nlohmann::json& j, MaterializedViewSearchInfo& info) {
    constexpr std::string_view kFieldIdToTouchedCategoriesCntKey =
        "field_id_to_touched_categories_cnt";
    constexpr std::string_view kIsPureAndKey = "is_pure_and";
    constexpr std::string_view kHasNotKey = "has_not";

    if (j.is_null()) {
        return;
    }

    if (j.contains(kFieldIdToTouchedCategoriesCntKey)) {
        j.at(kFieldIdToTouchedCategoriesCntKey)
            .get_to(info.field_id_to_touched_categories_cnt);
    }
    if (j.contains(kIsPureAndKey)) {
        j.at(kIsPureAndKey).get_to(info.is_pure_and);
    }
    if (j.contains(kHasNotKey)) {
        j.at(kHasNotKey).get_to(info.has_not);
    }
}

}  // namespace knowhere
