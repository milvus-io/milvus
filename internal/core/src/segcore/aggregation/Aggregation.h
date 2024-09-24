
#pragma once

#include <google/protobuf/text_format.h>
#include "pb/schema.pb.h"
#include "pb/internal.pb.h"
#include "log/Log.h"

namespace milvus::segcore::aggregation {

class Aggregator {
  public:
    virtual void
    Aggregate(
        google::protobuf::RepeatedPtrField<proto::schema::FieldData> fields,
        proto::internal::AggrData* aggr_output) = 0;
};


class Aggregation {
  public:
    virtual std::shared_ptr<Aggregator>
    GetAggregator(
        std::vector<proto::schema::DataType>& input_types) = 0;

    static std::shared_ptr<Aggregation>
    GetAggregation(
        const std::string& name);

    // static RegisterAggregation

    static int count_aggr() {
        return _aggregations.size();
    }
    
  private:
    static std::map<std::string, std::shared_ptr<Aggregation>> _aggregations;
    // static Aggregations _aggregations;
};


template <typename T>
struct DataReturn {
    typedef google::protobuf::RepeatedField<T> type;
};

template<>
struct DataReturn<std::string> {
    typedef google::protobuf::RepeatedPtrField<std::string> type;
};

template <typename T>
class AggregationHelpers {
  public:

    // DataReturn<T>::type should have operator[] that returns T
    static typename DataReturn<T>::type
    GetDataFromScalars(const proto::schema::ScalarField& scalars);
};

} // namespace milvus::segcore::aggregation
