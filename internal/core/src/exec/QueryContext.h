// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <folly/Executor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/Optional.h>
#include <folly/CancellationToken.h>

#include "common/Common.h"
#include "common/Types.h"
#include "common/Exception.h"
#include "common/OpContext.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {

enum class ContextScope { GLOBAL = 0, SESSION = 1, QUERY = 2, Executor = 3 };

class BaseConfig {
 public:
    virtual folly::Optional<std::string>
    Get(const std::string& key) const = 0;

    template <typename T>
    folly::Optional<T>
    Get(const std::string& key) const {
        auto val = Get(key);
        if (val.hasValue()) {
            return folly::to<T>(val.value());
        } else {
            return folly::none;
        }
    }

    template <typename T>
    T
    Get(const std::string& key, const T& default_value) const {
        auto val = Get(key);
        if (val.hasValue()) {
            return folly::to<T>(val.value());
        } else {
            return default_value;
        }
    }

    virtual bool
    IsValueExists(const std::string& key) const = 0;

    virtual const std::unordered_map<std::string, std::string>&
    values() const {
        ThrowInfo(NotImplemented, "method values() is not supported");
    }

    virtual ~BaseConfig() = default;
};

class MemConfig : public BaseConfig {
 public:
    explicit MemConfig(
        const std::unordered_map<std::string, std::string>& values)
        : values_(values) {
    }

    explicit MemConfig() : values_{} {
    }

    explicit MemConfig(std::unordered_map<std::string, std::string>&& values)
        : values_(std::move(values)) {
    }

    folly::Optional<std::string>
    Get(const std::string& key) const override {
        folly::Optional<std::string> val;
        auto it = values_.find(key);
        if (it != values_.end()) {
            val = it->second;
        }
        return val;
    }

    bool
    IsValueExists(const std::string& key) const override {
        return values_.find(key) != values_.end();
    }

    const std::unordered_map<std::string, std::string>&
    values() const override {
        return values_;
    }

 private:
    std::unordered_map<std::string, std::string> values_;
};

class QueryConfig : public MemConfig {
 public:
    // Whether to use the simplified expression evaluation path. False by default.
    static constexpr const char* kExprEvalSimplified =
        "expression.eval_simplified";

    static constexpr const char* kExprEvalBatchSize =
        "expression.eval_batch_size";

    explicit QueryConfig(
        const std::unordered_map<std::string, std::string>& values)
        : MemConfig(values) {
    }

    QueryConfig() = default;

    bool
    get_expr_eval_simplified() const {
        return BaseConfig::Get<bool>(kExprEvalSimplified, false);
    }

    int64_t
    get_expr_batch_size() const {
        return BaseConfig::Get<int64_t>(kExprEvalBatchSize,
                                        EXEC_EVAL_EXPR_BATCH_SIZE.load());
    }
};

class Context {
 public:
    explicit Context(ContextScope scope,
                     const std::shared_ptr<const Context> parent = nullptr)
        : scope_(scope), parent_(parent) {
    }

    ContextScope
    scope() const {
        return scope_;
    }

    std::shared_ptr<const Context>
    parent() const {
        return parent_;
    }
    // // TODO: support dynamic update
    // void
    // set_config(const std::shared_ptr<const Config>& config) {
    //     std::atomic_exchange(&config_, config);
    // }

    // std::shared_ptr<const config>
    // get_config() {
    //     return config_;
    // }

 private:
    ContextScope scope_;
    std::shared_ptr<const Context> parent_;
    //std::shared_ptr<const Config> config_;
};

class QueryContext : public Context {
 public:
    QueryContext(const std::string& query_id,
                 const milvus::segcore::SegmentInternalInterface* segment,
                 int64_t active_count,
                 milvus::Timestamp timestamp,
                 milvus::Timestamp collection_ttl = 0,
                 int32_t consistency_level = 0,
                 const query::PlanOptions& plan_options = query::PlanOptions(),
                 std::shared_ptr<QueryConfig> query_config =
                     std::make_shared<QueryConfig>(),
                 folly::Executor* executor = nullptr,
                 std::unordered_map<std::string, std::shared_ptr<Config>>
                     connector_configs = {})
        : Context(ContextScope::QUERY),
          query_id_(query_id),
          segment_(segment),
          active_count_(active_count),
          query_timestamp_(timestamp),
          collection_ttl_timestamp_(collection_ttl),
          query_config_(query_config),
          executor_(executor),
          consistency_level_(consistency_level),
          plan_options_(plan_options) {
    }

    folly::Executor*
    executor() const {
        return executor_;
    }

    const std::unordered_map<std::string, std::shared_ptr<Config>>&
    connector_configs() const {
        return connector_configs_;
    }

    std::shared_ptr<QueryConfig>
    query_config() const {
        return query_config_;
    }

    std::string
    query_id() const {
        return query_id_;
    }

    const milvus::segcore::SegmentInternalInterface*
    get_segment() {
        return segment_;
    }

    milvus::Timestamp
    get_query_timestamp() {
        return query_timestamp_;
    }

    milvus::Timestamp
    get_collection_ttl() {
        return collection_ttl_timestamp_;
    }

    int64_t
    get_active_count() {
        return active_count_;
    }

    milvus::SearchInfo
    get_search_info() {
        return search_info_;
    }

    knowhere::MetricType
    get_metric_type() {
        return search_info_.metric_type_;
    }

    const query::PlaceholderGroup*
    get_placeholder_group() {
        return placeholder_group_;
    }

    void
    set_search_info(const milvus::SearchInfo& search_info) {
        search_info_ = search_info;
    }

    void
    set_placeholder_group(const query::PlaceholderGroup* placeholder_group) {
        placeholder_group_ = placeholder_group;
    }

    void
    set_search_result(milvus::SearchResult&& result) {
        search_result_ = std::move(result);
    }

    milvus::SearchResult&&
    get_search_result() {
        return std::move(search_result_);
    }

    void
    set_retrieve_result(milvus::RetrieveResult&& result) {
        retrieve_result_ = std::move(result);
    }

    milvus::RetrieveResult&&
    get_retrieve_result() {
        return std::move(retrieve_result_);
    }

    void
    set_op_context(milvus::OpContext* op_context) {
        op_context_ = op_context;
    }

    milvus::OpContext*
    get_op_context() {
        return op_context_;
    }

    int32_t
    get_consistency_level() {
        return consistency_level_;
    }

    const query::PlanOptions&
    get_plan_options() const {
        return plan_options_;
    }

    void
    set_skip_filter(bool v) {
        skip_filter_ = v;
    }

    bool
    get_skip_filter() const {
        return skip_filter_;
    }

 private:
    folly::Executor* executor_;
    //folly::Executor::KeepAlive<> executor_keepalive_;
    std::unordered_map<std::string, std::shared_ptr<Config>> connector_configs_;
    std::shared_ptr<QueryConfig> query_config_;
    std::string query_id_;

    // current segment that query execute in
    const milvus::segcore::SegmentInternalInterface* segment_;
    // num rows for current query
    int64_t active_count_;
    // timestamp this query generate
    milvus::Timestamp query_timestamp_;
    milvus::Timestamp collection_ttl_timestamp_;
    // used for vector search
    milvus::SearchInfo search_info_;
    const query::PlaceholderGroup* placeholder_group_;

    // used for store segment search/retrieve result
    milvus::SearchResult search_result_;
    milvus::RetrieveResult retrieve_result_;

    // used for save op context
    milvus::OpContext* op_context_{nullptr};

    int32_t consistency_level_ = 0;

    query::PlanOptions plan_options_;

    // Set by MvccNode when no filtering is needed (sealed, no filter,
    // no deletes, no TTL). VectorSearchNode checks this to pass empty
    // BitsetView to Knowhere (IDSelectorAll fast path).
    bool skip_filter_{false};
};

// Represent the state of one thread of query execution.
// TODO: add more class member such as memory pool
class ExecContext : public Context {
 public:
    explicit ExecContext(QueryContext* query_context)
        : Context(ContextScope::Executor), query_context_(query_context) {
    }

    QueryContext*
    get_query_context() const {
        return query_context_;
    }

    std::shared_ptr<QueryConfig>
    get_query_config() const {
        return query_context_->query_config();
    }

 private:
    QueryContext* query_context_;
};

/// @brief Helper function to check cancellation token and throw if cancelled.
/// This function safely checks the cancellation token from QueryContext and throws
/// folly::FutureCancellation if the operation has been cancelled.
/// @param query_context Pointer to QueryContext (can be nullptr)
inline void
checkCancellation(QueryContext* query_context) {
    if (query_context == nullptr) {
        return;
    }
    auto* op_context = query_context->get_op_context();
    if (op_context != nullptr &&
        op_context->cancellation_token.isCancellationRequested()) {
        throw folly::FutureCancellation();
    }
}

}  // namespace milvus::exec
