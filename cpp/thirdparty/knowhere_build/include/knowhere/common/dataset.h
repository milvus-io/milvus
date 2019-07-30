#pragma once

#include <vector>
#include <memory>
#include "knowhere/common/array.h"
#include "knowhere/common/buffer.h"
#include "knowhere/common/tensor.h"
#include "knowhere/common/schema.h"
#include "knowhere/common/config.h"
#include "knowhere/adapter/arrow.h"


namespace zilliz {
namespace knowhere {

class Dataset;

using DatasetPtr = std::shared_ptr<Dataset>;

class Dataset {
 public:
    Dataset() = default;

    Dataset(std::vector<ArrayPtr> &&array, SchemaPtr array_schema,
            std::vector<TensorPtr> &&tensor, SchemaPtr tensor_schema)
        : array_(std::move(array)),
          array_schema_(std::move(array_schema)),
          tensor_(std::move(tensor)),
          tensor_schema_(std::move(tensor_schema)) {}

    Dataset(std::vector<ArrayPtr> array, SchemaPtr array_schema)
        : array_(std::move(array)), array_schema_(std::move(array_schema)) {}

    Dataset(std::vector<TensorPtr> tensor, SchemaPtr tensor_schema)
        : tensor_(std::move(tensor)), tensor_schema_(std::move(tensor_schema)) {}

    Dataset(const Dataset &) = delete;
    Dataset &operator=(const Dataset &) = delete;

    DatasetPtr
    Clone() {
        auto dataset = std::make_shared<Dataset>();

        std::vector<ArrayPtr> clone_array;
        for (auto &array : array_) {
            clone_array.emplace_back(CopyArray(array));
        }
        dataset->set_array(clone_array);

        std::vector<TensorPtr> clone_tensor;
        for (auto &tensor : tensor_) {
            auto buffer = tensor->data();
            std::shared_ptr<Buffer> copy_buffer;
            // TODO: checkout copy success;
            buffer->Copy(0, buffer->size(), &copy_buffer);
            auto copy = std::make_shared<Tensor>(tensor->type(), copy_buffer, tensor->shape());
            clone_tensor.emplace_back(copy);
        }
        dataset->set_tensor(clone_tensor);

        if (array_schema_)
            dataset->set_array_schema(CopySchema(array_schema_));
        if (tensor_schema_)
            dataset->set_tensor_schema(CopySchema(tensor_schema_));

        return dataset;
    }

 public:
    const std::vector<ArrayPtr> &
    array() const { return array_; }

    void
    set_array(std::vector<ArrayPtr> array) {
        array_ = std::move(array);
    }

    const std::vector<TensorPtr> &
    tensor() const { return tensor_; }

    void
    set_tensor(std::vector<TensorPtr> tensor) {
        tensor_ = std::move(tensor);
    }

    SchemaConstPtr
    array_schema() const { return array_schema_; }

    void
    set_array_schema(SchemaPtr array_schema) {
        array_schema_ = std::move(array_schema);
    }

    SchemaConstPtr
    tensor_schema() const { return tensor_schema_; }

    void
    set_tensor_schema(SchemaPtr tensor_schema) {
        tensor_schema_ = std::move(tensor_schema);
    }

    //const Config &
    //meta() const { return meta_; }

    //void
    //set_meta(Config meta) {
    //    meta_ = std::move(meta);
    //}

 private:
    SchemaPtr array_schema_;
    SchemaPtr tensor_schema_;
    std::vector<ArrayPtr> array_;
    std::vector<TensorPtr> tensor_;
    //Config meta_;
};

using DatasetPtr = std::shared_ptr<Dataset>;


} // namespace knowhere
} // namespace zilliz
