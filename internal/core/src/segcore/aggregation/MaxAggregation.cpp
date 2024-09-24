#include <segcore/aggregation/MaxAggregation.h>

namespace milvus::segcore::aggregation {

template <>
void MaxAggregator<int32_t>::SetMaxIr(proto::internal::MaxIR* max_ir, const int32_t& max) {
    max_ir->set_int_(static_cast<int64_t>(max));
}

template <>
void MaxAggregator<int64_t>::SetMaxIr(proto::internal::MaxIR* max_ir, const int64_t& max) {
    max_ir->set_int_(max);
}

template <>
void MaxAggregator<float>::SetMaxIr(proto::internal::MaxIR* max_ir, const float& max) {
    max_ir->set_float_(static_cast<double>(max));
}

template <>
void MaxAggregator<double>::SetMaxIr(proto::internal::MaxIR* max_ir, const double& max) {
    max_ir->set_float_(max);
}

template <>
void MaxAggregator<std::string>::SetMaxIr(proto::internal::MaxIR* max_ir, const std::string& max) {
    max_ir->set_str(max);
}


std::shared_ptr<Aggregator>
MaxAggregation::GetAggregator(
    std::vector<proto::schema::DataType>& input_types) {
    
    if (input_types.size() != 1) {
        throw std::runtime_error("Max aggregation expects single argument");
    }
    switch (input_types[0]) {
    case proto::schema::DataType::Int8:
    case proto::schema::DataType::Int16:
    case proto::schema::DataType::Int32:
        return std::make_shared<MaxAggregator<int32_t>>();
    case proto::schema::DataType::Int64:
        return std::make_shared<MaxAggregator<int64_t>>();
    case proto::schema::DataType::Float:
        return std::make_shared<MaxAggregator<float>>();
    case proto::schema::DataType::Double:
        return std::make_shared<MaxAggregator<double>>();
    case proto::schema::DataType::String:
    case proto::schema::DataType::VarChar:
        return std::make_shared<MaxAggregator<std::string>>();
    }

    // unsupported type
    return nullptr;
}

} // namespace milvus::segcore::aggregation
