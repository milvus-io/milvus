#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/config.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/index/index_node.h"
#include "knowhere/sparse_utils.h"

namespace milvus {
class OpContext;
}  // namespace milvus

namespace knowhere {

namespace detail {

inline std::string
NormalizeMetric(std::string metric_name) {
    std::transform(metric_name.begin(),
                   metric_name.end(),
                   metric_name.begin(),
                   [](unsigned char ch) { return std::toupper(ch); });
    return metric_name;
}

inline bool
IsFloatBruteForceMetric(const std::string& metric_name) {
    return metric_name == metric::L2 || metric_name == metric::IP ||
           metric_name == metric::COSINE;
}

inline float
L2Distance(const float* lhs, const float* rhs, int64_t dim) {
    float sum = 0.0f;
    for (int64_t i = 0; i < dim; ++i) {
        const float diff = lhs[i] - rhs[i];
        sum += diff * diff;
    }
    return sum;
}

inline float
InnerProduct(const float* lhs, const float* rhs, int64_t dim) {
    float sum = 0.0f;
    for (int64_t i = 0; i < dim; ++i) {
        sum += lhs[i] * rhs[i];
    }
    return sum;
}

inline float
CosineSimilarity(const float* lhs, const float* rhs, int64_t dim) {
    const float dot = InnerProduct(lhs, rhs, dim);
    const float lhs_norm = std::sqrt(InnerProduct(lhs, lhs, dim));
    const float rhs_norm = std::sqrt(InnerProduct(rhs, rhs, dim));
    if (lhs_norm == 0.0f || rhs_norm == 0.0f) {
        return 0.0f;
    }
    return dot / (lhs_norm * rhs_norm);
}

inline Status
SearchFloatWithBuf(const DataSetPtr& base_dataset,
                   const DataSetPtr& query_dataset,
                   int64_t* ids,
                   float* distances,
                   const Json& config,
                   const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr || ids == nullptr ||
        distances == nullptr) {
        return Status::invalid_args;
    }

    const auto topk = config.value(meta::TOPK, int64_t{0});
    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    if (topk <= 0 || dim <= 0 || query_dim != dim || nb < 0 || nq < 0) {
        return Status::invalid_args;
    }

    const auto* base_tensor =
        reinterpret_cast<const float*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const float*>(query_dataset->GetTensor());
    if (base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto metric_name = NormalizeMetric(
        config.value(meta::METRIC_TYPE, std::string(metric::L2)));
    if (!IsFloatBruteForceMetric(metric_name)) {
        return Status::invalid_metric_type;
    }

    const bool larger_is_better =
        metric_name == metric::IP || metric_name == metric::COSINE;
    const auto default_distance = larger_is_better
                                      ? -std::numeric_limits<float>::max()
                                      : std::numeric_limits<float>::max();
    const auto tensor_begin_id = base_dataset->GetTensorBeginId();

    using SearchHit = std::pair<float, int64_t>;
    std::vector<SearchHit> hits;
    hits.reserve(static_cast<size_t>(nb));

    auto compare_hits = [larger_is_better](const SearchHit& lhs,
                                           const SearchHit& rhs) {
        if (lhs.first == rhs.first) {
            return lhs.second < rhs.second;
        }
        return larger_is_better ? lhs.first > rhs.first : lhs.first < rhs.first;
    };

    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        hits.clear();
        const auto* query = query_tensor + query_idx * dim;
        for (int64_t base_idx = 0; base_idx < nb; ++base_idx) {
            if (!bitset.empty() && base_idx < static_cast<int64_t>(bitset.size()) &&
                bitset.test(base_idx)) {
                continue;
            }

            const auto* base = base_tensor + base_idx * dim;
            float score = 0.0f;
            if (metric_name == metric::L2) {
                score = L2Distance(base, query, dim);
            } else if (metric_name == metric::IP) {
                score = InnerProduct(base, query, dim);
            } else {
                score = CosineSimilarity(base, query, dim);
            }
            hits.emplace_back(score, tensor_begin_id + base_idx);
        }

        const auto result_count =
            std::min<int64_t>(topk, static_cast<int64_t>(hits.size()));
        if (result_count > 0) {
            std::partial_sort(hits.begin(),
                              hits.begin() + result_count,
                              hits.end(),
                              compare_hits);
        }

        for (int64_t rank = 0; rank < topk; ++rank) {
            const auto output_offset = query_idx * topk + rank;
            if (rank < result_count) {
                ids[output_offset] = hits[rank].second;
                distances[output_offset] = hits[rank].first;
            } else {
                ids[output_offset] = -1;
                distances[output_offset] = default_distance;
            }
        }
    }

    return Status::success;
}

}  // namespace detail

class BruteForce {
 public:
    template <typename DataType>
    static expected<DataSetPtr>
    Search(const DataSetPtr base_dataset,
           const DataSetPtr query_dataset,
           const Json& config,
           const BitsetView& bitset,
           milvus::OpContext* = nullptr) {
        if constexpr (std::is_same_v<DataType, float>) {
            if (base_dataset == nullptr || query_dataset == nullptr) {
                return Status::invalid_args;
            }
            const auto nq = query_dataset->GetRows();
            const auto topk = config.value(meta::TOPK, int64_t{0});
            if (nq < 0 || topk <= 0) {
                return Status::invalid_args;
            }

            auto result = std::make_shared<DataSet>();
            auto* ids = new int64_t[nq * topk];
            auto* distances = new float[nq * topk];
            auto status = detail::SearchFloatWithBuf(
                base_dataset, query_dataset, ids, distances, config, bitset);
            if (status != Status::success) {
                delete[] ids;
                delete[] distances;
                return status;
            }

            result->SetRows(nq);
            result->SetDim(topk);
            result->SetIds(ids);
            result->SetDistance(distances);
            result->SetIsOwner(true);
            return result;
        }
        return Status::not_implemented;
    }

    template <typename DataType>
    static Status
    SearchWithBuf(const DataSetPtr base_dataset,
                  const DataSetPtr query_dataset,
                  int64_t* ids,
                  float* distances,
                  const Json& config,
                  const BitsetView& bitset,
                  milvus::OpContext* = nullptr) {
        if constexpr (std::is_same_v<DataType, float>) {
            return detail::SearchFloatWithBuf(base_dataset,
                                              query_dataset,
                                              ids,
                                              distances,
                                              config,
                                              bitset);
        }
        return Status::not_implemented;
    }

    template <typename DataType>
    static expected<DataSetPtr>
    RangeSearch(const DataSetPtr,
                const DataSetPtr,
                const Json&,
                const BitsetView&,
                milvus::OpContext* = nullptr) {
        return Status::not_implemented;
    }

    static expected<DataSetPtr>
    SearchSparse(const DataSetPtr,
                 const DataSetPtr,
                 const Json&,
                 const BitsetView&,
                 milvus::OpContext* = nullptr) {
        return Status::not_implemented;
    }

    static Status
    SearchSparseWithBuf(const DataSetPtr,
                        const DataSetPtr,
                        sparse::label_t*,
                        float*,
                        const Json&,
                        const BitsetView&,
                        milvus::OpContext* = nullptr) {
        return Status::not_implemented;
    }

    template <typename DataType>
    static expected<std::vector<IndexNode::IteratorPtr>>
    AnnIterator(const DataSetPtr,
                const DataSetPtr,
                const Json&,
                const BitsetView&,
                bool = true,
                milvus::OpContext* = nullptr) {
        return Status::not_implemented;
    }
};

}  // namespace knowhere
