#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "knowhere/binaryset.h"
#include "knowhere/bitsetview.h"
#include "knowhere/config.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/object.h"

namespace knowhere {

class IndexNode : public Object {
 public:
    class Iterator {
     public:
        virtual ~Iterator() = default;
        virtual bool HasNext() = 0;
        virtual std::pair<int64_t, float> Next() = 0;
    };

    using IteratorPtr = std::shared_ptr<Iterator>;

    virtual Status
    Build(const DataSet& dataset, const Config& config) {
        RETURN_IF_ERROR(Train(dataset, config));
        return Add(dataset, config);
    }

    virtual Status
    Train(const DataSet&, const Config&) = 0;

    virtual Status
    Add(const DataSet&, const Config&) = 0;

    virtual expected<DataSetPtr>
    Search(const DataSet&, const Config&, const BitsetView&) const = 0;

    virtual expected<DataSetPtr>
    RangeSearch(const DataSet&, const Config&, const BitsetView&) const = 0;

    virtual expected<DataSetPtr>
    GetVectorByIds(const DataSet&) const = 0;

    virtual expected<std::vector<IteratorPtr>>
    AnnIterator(const DataSet&, const Config&, const BitsetView&) const {
        return Status::not_implemented;
    }

    virtual bool
    HasRawData(const std::string& metric_type) const = 0;

    virtual expected<DataSetPtr>
    GetIndexMeta(const Config&) const = 0;

    virtual Status
    Serialize(BinarySet& binary_set) const = 0;

    virtual Status
    Deserialize(const BinarySet& binary_set, const Config& config) = 0;

    virtual Status
    DeserializeFromFile(const std::string& filename, const Config& config) = 0;

    virtual std::unique_ptr<BaseConfig>
    CreateConfig() const = 0;

    virtual int64_t
    Dim() const = 0;

    virtual int64_t
    Size() const = 0;

    virtual int64_t
    Count() const = 0;

    virtual std::string
    Type() const = 0;

    virtual ~IndexNode() = default;
};

}  // namespace knowhere
