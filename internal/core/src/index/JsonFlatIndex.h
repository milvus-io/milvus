// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include <algorithm>
#include <memory>
#include "common/EasyAssert.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/Index.h"
#include "index/InvertedIndexTantivy.h"
#include "index/InvertedIndexUtil.h"
#include "index/ScalarIndex.h"
#include "log/Log.h"
namespace milvus::index {

// JsonFlatIndexQueryExecutor is used to execute queries on a specified json path, and can be constructed by JsonFlatIndex
template <typename T>
class JsonFlatIndexQueryExecutor : public InvertedIndexTantivy<T> {
 public:
    JsonFlatIndexQueryExecutor(std::string& json_path,
                               std::shared_ptr<TantivyIndexWrapper> wrapper) {
        json_path_ = json_path;
        this->wrapper_ = wrapper;
    }

    ~JsonFlatIndexQueryExecutor() {
        this->wrapper_ = nullptr;
    }

    const TargetBitmap
    In(size_t n, const T* values) override {
        TargetBitmap bitset(this->Count());
        for (size_t i = 0; i < n; ++i) {
            this->wrapper_->json_term_query(json_path_, values[i], &bitset);
        }
        return bitset;
    }

    const TargetBitmap
    IsNull() override {
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_exist_query(json_path_, &bitset);
        bitset.flip();
        return bitset;
    }

    const TargetBitmap
    IsNotNull() override {
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_exist_query(json_path_, &bitset);
        return bitset;
    }

    const TargetBitmap
    InApplyFilter(
        size_t n,
        const T* values,
        const std::function<bool(size_t /* offset */)>& filter) override {
        TargetBitmap bitset(this->Count());
        for (size_t i = 0; i < n; ++i) {
            this->wrapper_->json_term_query(json_path_, values[i], &bitset);
            apply_hits_with_filter(bitset, filter);
        }
        return bitset;
    }

    virtual void
    InApplyCallback(
        size_t n,
        const T* values,
        const std::function<void(size_t /* offset */)>& callback) override {
        TargetBitmap bitset(this->Count());
        for (size_t i = 0; i < n; ++i) {
            this->wrapper_->json_term_query(json_path_, values[i], &bitset);
            apply_hits_with_callback(bitset, callback);
        }
    }

    const TargetBitmap
    NotIn(size_t n, const T* values) override {
        TargetBitmap bitset(this->Count());
        for (size_t i = 0; i < n; ++i) {
            this->wrapper_->json_term_query(json_path_, values[i], &bitset);
        }

        bitset.flip();

        // TODO: optimize this
        auto null_bitset = IsNotNull();
        bitset &= null_bitset;

        return bitset;
    }

    const TargetBitmap
    Range(T value, OpType op) override {
        LOG_INFO("[executor] JsonFlatIndexQueryExecutor Range");
        TargetBitmap bitset(this->Count());
        switch (op) {
            case OpType::LessThan: {
                this->wrapper_->json_range_query(
                    json_path_, T(), value, true, false, false, false, &bitset);
            } break;
            case OpType::LessEqual: {
                this->wrapper_->json_range_query(
                    json_path_, T(), value, true, false, true, false, &bitset);
            } break;
            case OpType::GreaterThan: {
                this->wrapper_->json_range_query(
                    json_path_, value, T(), false, true, false, false, &bitset);
            } break;
            case OpType::GreaterEqual: {
                this->wrapper_->json_range_query(
                    json_path_, value, T(), false, true, true, false, &bitset);
            } break;
            default:
                PanicInfo(OpTypeInvalid,
                          fmt::format("Invalid OperatorType: {}", op));
        }
        return bitset;
    }

    const TargetBitmap
    Query(const DatasetPtr& dataset) override {
        return InvertedIndexTantivy<T>::Query(dataset);
    }

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override {
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_range_query(json_path_,
                                         lower_bound_value,
                                         upper_bound_value,
                                         false,
                                         false,
                                         lb_inclusive,
                                         ub_inclusive,
                                         &bitset);
        return bitset;
    }

    const TargetBitmap
    PrefixMatch(const std::string_view prefix) override {
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_prefix_query(
            json_path_, std::string(prefix), &bitset);
        return bitset;
    }

    const TargetBitmap
    RegexQuery(const std::string& pattern) override {
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_regex_query(json_path_, pattern, &bitset);
        return bitset;
    }

 private:
    std::string json_path_;
};

// JsonFlatIndex is not bound to any specific type,
// we need to reuse InvertedIndexTantivy's Build and Load implementation, so we specify the template parameter as std::string
// JsonFlatIndex should not be used to execute queries, use JsonFlatIndexQueryExecutor instead
class JsonFlatIndex : public InvertedIndexTantivy<std::string> {
 public:
    JsonFlatIndex() : InvertedIndexTantivy<std::string>() {
    }

    explicit JsonFlatIndex(const storage::FileManagerContext& ctx,
                           const std::string& nested_path)
        : InvertedIndexTantivy<std::string>(
              TANTIVY_INDEX_LATEST_VERSION, ctx, false, false),
          nested_path_(nested_path) {
    }

    void
    build_index_for_json(const std::vector<std::shared_ptr<FieldDataBase>>&
                             field_datas) override;

    template <typename T>
    std::shared_ptr<JsonFlatIndexQueryExecutor<T>>
    create_executor(std::string json_path) const {
        // json path should be in the format of /a/b/c, we need to convert it to tantivy path like a.b.c
        std::replace(json_path.begin(), json_path.end(), '/', '.');
        if (!json_path.empty()) {
            json_path = json_path.substr(1);
        }

        LOG_INFO("Create JsonFlatIndexQueryExecutor with json_path: {}",
                 json_path);

        return std::make_shared<JsonFlatIndexQueryExecutor<T>>(json_path,
                                                               this->wrapper_);
    }

    JsonCastType
    GetCastType() const override {
        return JsonCastType::FromString("JSON");
    }

    std::string
    GetNestedPath() const {
        return nested_path_;
    }

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader(SetBitsetFn set_bitset) {
        this->wrapper_->create_reader(set_bitset);
    }

 private:
    std::string nested_path_;
};
}  // namespace milvus::index