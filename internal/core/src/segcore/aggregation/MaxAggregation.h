#pragma once

#include <google/protobuf/text_format.h>
#include <pb/schema.pb.h>
#include <pb/internal.pb.h>
#include <segcore/aggregation/Aggregation.h>

namespace milvus::segcore::aggregation {

template <typename T>
class MaxAggregator : public Aggregator {
  public:
    void
    Aggregate(
        google::protobuf::RepeatedPtrField<proto::schema::FieldData> fields,
        proto::internal::AggrData* aggr_output);

  private:
    void SetMaxIr(proto::internal::MaxIR* max_ir, const T& max);
};

template <typename T>
void MaxAggregator<T>::Aggregate(
    google::protobuf::RepeatedPtrField<proto::schema::FieldData> fields,
    proto::internal::AggrData* aggr_output) {

    const auto& scalars = fields[0].scalars();
    // const auto& data = scalars.long_data().data();
    const auto& data = AggregationHelpers<T>::GetDataFromScalars(scalars);

    if (data.size() < 1) {
        LOG_WARN("no data to aggregate");
        return;
    }
    T max = data[0];
    for (int i = 1; i < data.size(); i++) {
        max = std::max(max, data[i]);
    }

    // const auto& max_ir = aggr_output->mutable_max_ir();
    // max_ir->set_int_(max);
    SetMaxIr(aggr_output->mutable_max_ir(), max);
}

class MaxAggregation : public Aggregation {
  public:
    std::shared_ptr<Aggregator>
    GetAggregator(
        std::vector<proto::schema::DataType>& input_types);
};

} // namespace milvus::segcore::aggregation