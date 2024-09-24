#include <segcore/aggregation/Aggregation.h>
#include <segcore/aggregation/MaxAggregation.h>

namespace milvus::segcore::aggregation {

std::shared_ptr<Aggregation> Aggregation::GetAggregation(const std::string& name) {
    auto iter = _aggregations.find(name);
    if (iter == _aggregations.end()) {
        return nullptr;
    }
    return iter->second;
}

static std::map<std::string, std::shared_ptr<Aggregation>> init_aggregations() {
    return {
        {"max", std::make_shared<MaxAggregation>()},
    };
}

std::map<std::string, std::shared_ptr<Aggregation>>
Aggregation::_aggregations = init_aggregations();

} // namespace milvus::segcore::aggregation
