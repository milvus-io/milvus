#pragma once

#include <segcore/aggregation/Aggregation.h>

namespace milvus::segcore::aggregation {

template <>
google::protobuf::RepeatedField<int32_t>
AggregationHelpers<int32_t>::GetDataFromScalars(const proto::schema::ScalarField& scalars) {
    return scalars.int_data().data();
}

template <>
google::protobuf::RepeatedField<int64_t>
AggregationHelpers<int64_t>::GetDataFromScalars(const proto::schema::ScalarField& scalars) {
    return scalars.long_data().data();
}

template <>
google::protobuf::RepeatedField<float>
AggregationHelpers<float>::GetDataFromScalars(const proto::schema::ScalarField& scalars) {
    return scalars.float_data().data();
}

template <>
google::protobuf::RepeatedField<double>
AggregationHelpers<double>::GetDataFromScalars(const proto::schema::ScalarField& scalars) {
    return scalars.double_data().data();
}

template <>
google::protobuf::RepeatedPtrField<std::string>
AggregationHelpers<std::string>::GetDataFromScalars(const proto::schema::ScalarField& scalars) {
    return scalars.string_data().data();
}

} // namespace milvus::segcore::aggregation
