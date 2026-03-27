#pragma once

#include <any>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <variant>

#include "knowhere/comp/index_param.h"

namespace knowhere {

class DataSet {
 public:
    using Var = std::variant<const float*,
                             const size_t*,
                             const int64_t*,
                             const void*,
                             int64_t,
                             std::string,
                             std::any>;

    DataSet() = default;

    ~DataSet() {
        if (!is_owner_) {
            return;
        }
        for (auto& [_, value] : data_) {
            if (auto ptr = std::get_if<const float*>(&value); ptr != nullptr) {
                delete[] *ptr;
            } else if (auto ptr = std::get_if<const size_t*>(&value);
                       ptr != nullptr) {
                delete[] *ptr;
            } else if (auto ptr = std::get_if<const int64_t*>(&value);
                       ptr != nullptr) {
                delete[] *ptr;
            } else if (auto ptr = std::get_if<const void*>(&value);
                       ptr != nullptr) {
                delete[] reinterpret_cast<const char*>(*ptr);
            }
        }
    }

    void
    SetDistance(const float* distance) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::DISTANCE] = distance;
    }

    void
    SetLims(const size_t* lims) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::LIMS] = lims;
    }

    void
    SetIds(const int64_t* ids) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::IDS] = ids;
    }

    void
    SetTensor(const void* tensor) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::TENSOR] = tensor;
    }

    void
    SetRows(int64_t rows) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::ROWS] = rows;
    }

    void
    SetDim(int64_t dim) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::DIM] = dim;
    }

    void
    SetTensorBeginId(int64_t offset) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[meta::INPUT_BEG_ID] = offset;
    }

    void
    SetIsOwner(bool is_owner) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        is_owner_ = is_owner;
    }

    void
    SetIsSparse(bool is_sparse) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        is_sparse_ = is_sparse;
    }

    const float*
    GetDistance() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::DISTANCE);
        return it == data_.end() ? nullptr : *std::get_if<const float*>(&it->second);
    }

    const size_t*
    GetLims() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::LIMS);
        return it == data_.end() ? nullptr : *std::get_if<const size_t*>(&it->second);
    }

    const int64_t*
    GetIds() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::IDS);
        return it == data_.end() ? nullptr : *std::get_if<const int64_t*>(&it->second);
    }

    const void*
    GetTensor() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::TENSOR);
        return it == data_.end() ? nullptr : *std::get_if<const void*>(&it->second);
    }

    int64_t
    GetRows() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::ROWS);
        return it == data_.end() ? 0 : *std::get_if<int64_t>(&it->second);
    }

    int64_t
    GetDim() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::DIM);
        return it == data_.end() ? 0 : *std::get_if<int64_t>(&it->second);
    }

    int64_t
    GetTensorBeginId() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(meta::INPUT_BEG_ID);
        return it == data_.end() ? 0 : *std::get_if<int64_t>(&it->second);
    }

    bool
    IsSparse() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return is_sparse_;
    }

    template <typename T>
    void
    Set(const std::string& key, T&& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_[key] = std::any(std::forward<T>(value));
    }

    template <typename T>
    T
    Get(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = data_.find(key);
        if (it == data_.end()) {
            return T{};
        }
        return std::any_cast<T>(std::get<std::any>(it->second));
    }

 private:
    mutable std::shared_mutex mutex_;
    std::map<std::string, Var> data_;
    bool is_owner_ = true;
    bool is_sparse_ = false;
};

using DataSetPtr = std::shared_ptr<DataSet>;

inline DataSetPtr
GenDataSet(int64_t rows, int64_t dim, const void* tensor) {
    auto dataset = std::make_shared<DataSet>();
    dataset->SetRows(rows);
    dataset->SetDim(dim);
    dataset->SetTensor(tensor);
    dataset->SetIsOwner(false);
    return dataset;
}

}  // namespace knowhere
