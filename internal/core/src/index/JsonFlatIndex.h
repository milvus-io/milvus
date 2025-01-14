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

    const TargetBitmap
    In(size_t n, const T* values) override {
        LOG_INFO("[executor] JsonFlatIndexQueryExecutor In");
        TargetBitmap bitset(this->Count());
        for (size_t i = 0; i < n; ++i) {
            auto array = this->wrapper_->json_term_query(json_path_, values[i]);
            apply_hits(bitset, array, true);
        }
        return bitset;
    }

    const TargetBitmap
    IsNull() override {
        TargetBitmap bitset(this->Count(), true);
        auto array = this->wrapper_->json_exist_query(json_path_);
        apply_hits(bitset, array, false);
        return bitset;
    }

    const TargetBitmap
    IsNotNull() override {
        TargetBitmap bitset(this->Count());
        auto array = this->wrapper_->json_exist_query(json_path_);
        apply_hits(bitset, array, true);
        return bitset;
    }

    const TargetBitmap
    InApplyFilter(
        size_t n,
        const T* values,
        const std::function<bool(size_t /* offset */)>& filter) override {
        TargetBitmap bitset(this->Count());
        for (size_t i = 0; i < n; ++i) {
            auto array = this->wrapper_->json_term_query(json_path_, values[i]);
            apply_hits_with_filter(bitset, array, filter);
        }
        return bitset;
    }

    virtual void
    InApplyCallback(
        size_t n,
        const T* values,
        const std::function<void(size_t /* offset */)>& callback) override {
        PanicInfo(ErrorCode::Unsupported, "InApplyCallback is not implemented");
    }

    const TargetBitmap
    NotIn(size_t n, const T* values) override {
        TargetBitmap bitset(this->Count(), true);
        for (size_t i = 0; i < n; ++i) {
            auto array = this->wrapper_->json_term_query(json_path_, values[i]);
            apply_hits(bitset, array, false);
        }

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
                auto array = this->wrapper_->json_range_query(
                    json_path_, T(), value, true, false, false, false);
                apply_hits(bitset, array, true);
            } break;
            case OpType::LessEqual: {
                auto array = this->wrapper_->json_range_query(
                    json_path_, T(), value, true, false, true, false);
                apply_hits(bitset, array, true);
            } break;
            case OpType::GreaterThan: {
                auto array = this->wrapper_->json_range_query(
                    json_path_, value, T(), false, true, false, false);
                apply_hits(bitset, array, true);
            } break;
            case OpType::GreaterEqual: {
                auto array = this->wrapper_->json_range_query(
                    json_path_, value, T(), false, true, true, false);
                apply_hits(bitset, array, true);
            } break;
            default:
                PanicInfo(OpTypeInvalid,
                          fmt::format("Invalid OperatorType: {}", op));
        }
        return bitset;
    }

    const TargetBitmap
    Query(const DatasetPtr& dataset) override {
        InvertedIndexTantivy<T>::Query(dataset);
    }

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override {
        TargetBitmap bitset(this->Count());
        auto array = this->wrapper_->json_range_query(json_path_,
                                                      lower_bound_value,
                                                      upper_bound_value,
                                                      false,
                                                      false,
                                                      lb_inclusive,
                                                      ub_inclusive);
        apply_hits(bitset, array, true);
        return bitset;
    }

    const TargetBitmap
    PrefixMatch(const std::string& prefix) {
        LOG_INFO("[executor] JsonFlatIndexQueryExecutor PrefixMatch {}",
                 prefix);
        TargetBitmap bitset(this->Count());
        auto array = this->wrapper_->json_prefix_query(json_path_, prefix);
        apply_hits(bitset, array, true);
        return bitset;
    }

    const TargetBitmap
    PatternMatch(const std::string& pattern) override {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        return RegexQuery(regex_pattern);
    }

    const TargetBitmap
    RegexQuery(const std::string& pattern) override {
        TargetBitmap bitset(this->Count());
        auto array = this->wrapper_->json_regex_query(json_path_, pattern);
        apply_hits(bitset, array, true);
        return bitset;
    }

 private:
    std::string json_path_;
};

template <>
inline const TargetBitmap
JsonFlatIndexQueryExecutor<std::string>::Query(const DatasetPtr& dataset) {
    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    if (op == OpType::PrefixMatch) {
        auto prefix = dataset->Get<std::string>(PREFIX_VALUE);
        return PrefixMatch(prefix);
    }
    return ScalarIndex<std::string>::Query(dataset);
}

// JsonFlatIndex is not bound to any specific type,
// we need to reuse InvertedIndexTantivy's Build and Load implementation, so we specify the template parameter as std::string
// JsonFlatIndex should not be used to execute queries, use JsonFlatIndexQueryExecutor instead
class JsonFlatIndex : public InvertedIndexTantivy<std::string> {
 public:
    JsonFlatIndex() : InvertedIndexTantivy<std::string>() {
    }

    explicit JsonFlatIndex(const storage::FileManagerContext& ctx)
        : InvertedIndexTantivy<std::string>(ctx) {
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
};
}  // namespace milvus::index