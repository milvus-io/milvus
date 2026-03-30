#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/config.h"
#include "knowhere/dataset.h"
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

inline bool
IsSparseBruteForceMetric(const std::string& metric_name) {
    return metric_name == metric::IP || metric_name == metric::BM25;
}

inline bool
IsBinaryBruteForceMetric(const std::string& metric_name) {
    return metric_name == metric::HAMMING || metric_name == metric::JACCARD;
}

inline bool
IsRangeHit(const std::string& metric_name,
           float score,
           float radius,
           bool has_range_filter,
           float range_filter) {
    if (metric_name == metric::IP || metric_name == metric::COSINE ||
        metric_name == metric::BM25) {
        if (score <= radius) {
            return false;
        }
        return !has_range_filter || score < range_filter;
    }

    if (score >= radius) {
        return false;
    }
    return !has_range_filter || score > range_filter;
}

inline uint8_t
TailMask(int64_t dim_bits) {
    const auto tail = dim_bits % 8;
    if (tail == 0) {
        return 0xFF;
    }
    return static_cast<uint8_t>((1u << tail) - 1u);
}

inline float
HammingDistance(const uint8_t* lhs, const uint8_t* rhs, int64_t dim_bits) {
    const auto bytes = static_cast<size_t>((dim_bits + 7) / 8);
    const auto tail_mask = TailMask(dim_bits);
    float distance = 0.0f;
    for (size_t i = 0; i < bytes; ++i) {
        uint8_t diff = static_cast<uint8_t>(lhs[i] ^ rhs[i]);
        if (i + 1 == bytes) {
            diff &= tail_mask;
        }
        distance += static_cast<float>(__builtin_popcount(diff));
    }
    return distance;
}

inline float
JaccardDistance(const uint8_t* lhs, const uint8_t* rhs, int64_t dim_bits) {
    const auto bytes = static_cast<size_t>((dim_bits + 7) / 8);
    const auto tail_mask = TailMask(dim_bits);
    int intersection = 0;
    int union_count = 0;
    for (size_t i = 0; i < bytes; ++i) {
        uint8_t lhs_byte = lhs[i];
        uint8_t rhs_byte = rhs[i];
        if (i + 1 == bytes) {
            lhs_byte &= tail_mask;
            rhs_byte &= tail_mask;
        }
        intersection += __builtin_popcount(
            static_cast<unsigned int>(lhs_byte & rhs_byte));
        union_count += __builtin_popcount(
            static_cast<unsigned int>(lhs_byte | rhs_byte));
    }
    if (union_count == 0) {
        return 0.0f;
    }
    return 1.0f - static_cast<float>(intersection) /
                      static_cast<float>(union_count);
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

inline float
FloatFromBFloat16Bits(uint16_t bits) {
    const uint32_t wide = static_cast<uint32_t>(bits) << 16;
    float value = 0.0f;
    std::memcpy(&value, &wide, sizeof(value));
    return value;
}

inline float
FloatFromFloat16Bits(uint16_t bits) {
    const uint32_t sign = static_cast<uint32_t>(bits & 0x8000u) << 16;
    const uint32_t exponent = static_cast<uint32_t>((bits >> 10) & 0x1Fu);
    uint32_t mantissa = static_cast<uint32_t>(bits & 0x03FFu);
    uint32_t wide = 0;

    if (exponent == 0) {
        if (mantissa == 0) {
            wide = sign;
        } else {
            uint32_t normalized_exponent = 127 - 15 + 1;
            while ((mantissa & 0x0400u) == 0) {
                mantissa <<= 1;
                --normalized_exponent;
            }
            mantissa &= 0x03FFu;
            wide = sign | (normalized_exponent << 23) | (mantissa << 13);
        }
    } else if (exponent == 0x1Fu) {
        wide = sign | 0x7F800000u | (mantissa << 13);
    } else {
        wide = sign | ((exponent + (127 - 15)) << 23) | (mantissa << 13);
    }

    float value = 0.0f;
    std::memcpy(&value, &wide, sizeof(value));
    return value;
}

template <typename HalfType>
inline float
HalfValueToFloat(HalfType value) {
    if constexpr (std::is_same_v<HalfType, fp16>) {
        return FloatFromFloat16Bits(value.bits);
    } else {
        return FloatFromBFloat16Bits(value.bits);
    }
}

template <typename HalfType>
inline std::vector<float>
ConvertHalfTensorToFloat32(const HalfType* tensor, int64_t rows, int64_t dim) {
    const auto count = static_cast<size_t>(rows * dim);
    std::vector<float> converted(count, 0.0f);
    for (size_t i = 0; i < count; ++i) {
        converted[i] = HalfValueToFloat(tensor[i]);
    }
    return converted;
}

inline std::vector<float>
ConvertInt8TensorToFloat32(const int8* tensor, int64_t rows, int64_t dim) {
    const auto count = static_cast<size_t>(rows * dim);
    std::vector<float> converted(count, 0.0f);
    for (size_t i = 0; i < count; ++i) {
        converted[i] = static_cast<float>(tensor[i]);
    }
    return converted;
}

inline bool
IsMaskedByBitset(const BitsetView& bitset,
                 int64_t tensor_begin_id,
                 int64_t local_index) {
    if (bitset.empty()) {
        return false;
    }
    const auto global_index = tensor_begin_id + local_index;
    return global_index >= 0 &&
           global_index < static_cast<int64_t>(bitset.size()) &&
           bitset.test(global_index);
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
    const auto* base_ids = base_dataset->GetIds();

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
            if (IsMaskedByBitset(bitset, tensor_begin_id, base_idx)) {
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
            hits.emplace_back(
                score, base_ids != nullptr ? base_ids[base_idx] : tensor_begin_id + base_idx);
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

inline Status
SearchBinaryWithBuf(const DataSetPtr& base_dataset,
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
        reinterpret_cast<const uint8_t*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const uint8_t*>(query_dataset->GetTensor());
    if (base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto metric_name = NormalizeMetric(
        config.value(meta::METRIC_TYPE, std::string(metric::JACCARD)));
    if (!IsBinaryBruteForceMetric(metric_name)) {
        return Status::invalid_metric_type;
    }

    const auto bytes_per_vector = static_cast<int64_t>((dim + 7) / 8);
    const auto default_distance = std::numeric_limits<float>::max();
    const auto tensor_begin_id = base_dataset->GetTensorBeginId();
    const auto* base_ids = base_dataset->GetIds();

    using SearchHit = std::pair<float, int64_t>;
    std::vector<SearchHit> hits;
    hits.reserve(static_cast<size_t>(nb));

    auto compare_hits = [](const SearchHit& lhs, const SearchHit& rhs) {
        if (lhs.first == rhs.first) {
            return lhs.second < rhs.second;
        }
        return lhs.first < rhs.first;
    };

    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        hits.clear();
        const auto* query =
            query_tensor + static_cast<size_t>(query_idx * bytes_per_vector);
        for (int64_t base_idx = 0; base_idx < nb; ++base_idx) {
            if (IsMaskedByBitset(bitset, tensor_begin_id, base_idx)) {
                continue;
            }

            const auto* base =
                base_tensor + static_cast<size_t>(base_idx * bytes_per_vector);
            float score = 0.0f;
            if (metric_name == metric::HAMMING) {
                score = HammingDistance(base, query, dim);
            } else {
                score = JaccardDistance(base, query, dim);
            }
            hits.emplace_back(
                score, base_ids != nullptr ? base_ids[base_idx] : tensor_begin_id + base_idx);
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

template <typename HalfType>
inline Status
SearchHalfWithBuf(const DataSetPtr& base_dataset,
                  const DataSetPtr& query_dataset,
                  int64_t* ids,
                  float* distances,
                  const Json& config,
                  const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    const auto* base_tensor =
        reinterpret_cast<const HalfType*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const HalfType*>(query_dataset->GetTensor());
    if (dim <= 0 || query_dim != dim || nb < 0 || nq < 0 ||
        base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto converted_base = ConvertHalfTensorToFloat32(base_tensor, nb, dim);
    const auto converted_query =
        ConvertHalfTensorToFloat32(query_tensor, nq, query_dim);
    auto float_base = GenDataSet(nb, dim, converted_base.data());
    float_base->SetTensorBeginId(base_dataset->GetTensorBeginId());
    float_base->SetIsOwner(false);
    if (base_dataset->GetIds() != nullptr) {
        float_base->SetIds(base_dataset->GetIds());
    }
    auto float_query = GenDataSet(nq, dim, converted_query.data());
    float_query->SetTensorBeginId(query_dataset->GetTensorBeginId());
    float_query->SetIsOwner(false);
    if (query_dataset->GetIds() != nullptr) {
        float_query->SetIds(query_dataset->GetIds());
    }

    return SearchFloatWithBuf(
        float_base, float_query, ids, distances, config, bitset);
}

inline Status
SearchInt8WithBuf(const DataSetPtr& base_dataset,
                  const DataSetPtr& query_dataset,
                  int64_t* ids,
                  float* distances,
                  const Json& config,
                  const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    const auto* base_tensor =
        reinterpret_cast<const int8*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const int8*>(query_dataset->GetTensor());
    if (dim <= 0 || query_dim != dim || nb < 0 || nq < 0 ||
        base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto converted_base = ConvertInt8TensorToFloat32(base_tensor, nb, dim);
    const auto converted_query =
        ConvertInt8TensorToFloat32(query_tensor, nq, query_dim);
    auto float_base = GenDataSet(nb, dim, converted_base.data());
    float_base->SetTensorBeginId(base_dataset->GetTensorBeginId());
    float_base->SetIsOwner(false);
    if (base_dataset->GetIds() != nullptr) {
        float_base->SetIds(base_dataset->GetIds());
    }
    auto float_query = GenDataSet(nq, dim, converted_query.data());
    float_query->SetTensorBeginId(query_dataset->GetTensorBeginId());
    float_query->SetIsOwner(false);
    if (query_dataset->GetIds() != nullptr) {
        float_query->SetIds(query_dataset->GetIds());
    }

    return SearchFloatWithBuf(
        float_base, float_query, ids, distances, config, bitset);
}

inline expected<DataSetPtr>
RangeSearchFloat(const DataSetPtr& base_dataset,
                 const DataSetPtr& query_dataset,
                 const Json& config,
                 const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    const auto* base_tensor =
        reinterpret_cast<const float*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const float*>(query_dataset->GetTensor());
    if (dim <= 0 || query_dim != dim || nb < 0 || nq < 0 ||
        base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto metric_name = NormalizeMetric(
        config.value(meta::METRIC_TYPE, std::string(metric::L2)));
    if (!IsFloatBruteForceMetric(metric_name)) {
        return Status::invalid_metric_type;
    }

    if (!config.contains(meta::RADIUS)) {
        return Status::invalid_args;
    }
    const auto radius = config.value(meta::RADIUS, 0.0f);
    const bool has_range_filter = config.contains(meta::RANGE_FILTER);
    const auto range_filter = config.value(meta::RANGE_FILTER, 0.0f);
    const auto tensor_begin_id = base_dataset->GetTensorBeginId();
    const auto* base_ids = base_dataset->GetIds();

    std::vector<size_t> lims(static_cast<size_t>(nq) + 1, 0);
    std::vector<int64_t> result_ids;
    std::vector<float> result_distances;

    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        lims[static_cast<size_t>(query_idx)] = result_ids.size();
        const auto* query = query_tensor + query_idx * dim;
        for (int64_t base_idx = 0; base_idx < nb; ++base_idx) {
            if (IsMaskedByBitset(bitset, tensor_begin_id, base_idx)) {
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

            if (!IsRangeHit(metric_name,
                            score,
                            radius,
                            has_range_filter,
                            range_filter)) {
                continue;
            }

            result_ids.emplace_back(base_ids != nullptr ? base_ids[base_idx]
                                                        : tensor_begin_id + base_idx);
            result_distances.emplace_back(score);
        }
        lims[static_cast<size_t>(query_idx + 1)] = result_ids.size();
    }

    auto result = std::make_shared<DataSet>();
    auto* raw_lims = new size_t[lims.size()];
    auto* raw_ids = new int64_t[result_ids.size()];
    auto* raw_distances = new float[result_distances.size()];
    std::copy(lims.begin(), lims.end(), raw_lims);
    std::copy(result_ids.begin(), result_ids.end(), raw_ids);
    std::copy(result_distances.begin(), result_distances.end(), raw_distances);
    result->SetRows(nq);
    result->SetDim(0);
    result->SetLims(raw_lims);
    result->SetIds(raw_ids);
    result->SetDistance(raw_distances);
    result->SetIsOwner(true);
    return result;
}

inline expected<DataSetPtr>
RangeSearchBinary(const DataSetPtr& base_dataset,
                  const DataSetPtr& query_dataset,
                  const Json& config,
                  const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    const auto* base_tensor =
        reinterpret_cast<const uint8_t*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const uint8_t*>(query_dataset->GetTensor());
    if (dim <= 0 || query_dim != dim || nb < 0 || nq < 0 ||
        base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto metric_name = NormalizeMetric(
        config.value(meta::METRIC_TYPE, std::string(metric::JACCARD)));
    if (!IsBinaryBruteForceMetric(metric_name)) {
        return Status::invalid_metric_type;
    }

    if (!config.contains(meta::RADIUS)) {
        return Status::invalid_args;
    }
    const auto radius = config.value(meta::RADIUS, 0.0f);
    const bool has_range_filter = config.contains(meta::RANGE_FILTER);
    const auto range_filter = config.value(meta::RANGE_FILTER, 0.0f);
    const auto bytes_per_vector = static_cast<int64_t>((dim + 7) / 8);
    const auto tensor_begin_id = base_dataset->GetTensorBeginId();
    const auto* base_ids = base_dataset->GetIds();

    std::vector<size_t> lims(static_cast<size_t>(nq) + 1, 0);
    std::vector<int64_t> result_ids;
    std::vector<float> result_distances;

    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        lims[static_cast<size_t>(query_idx)] = result_ids.size();
        const auto* query =
            query_tensor + static_cast<size_t>(query_idx * bytes_per_vector);
        for (int64_t base_idx = 0; base_idx < nb; ++base_idx) {
            if (IsMaskedByBitset(bitset, tensor_begin_id, base_idx)) {
                continue;
            }

            const auto* base =
                base_tensor + static_cast<size_t>(base_idx * bytes_per_vector);
            float score = 0.0f;
            if (metric_name == metric::HAMMING) {
                score = HammingDistance(base, query, dim);
            } else {
                score = JaccardDistance(base, query, dim);
            }

            if (!IsRangeHit(metric_name,
                            score,
                            radius,
                            has_range_filter,
                            range_filter)) {
                continue;
            }

            result_ids.emplace_back(base_ids != nullptr ? base_ids[base_idx]
                                                        : tensor_begin_id + base_idx);
            result_distances.emplace_back(score);
        }
        lims[static_cast<size_t>(query_idx + 1)] = result_ids.size();
    }

    auto result = std::make_shared<DataSet>();
    auto* raw_lims = new size_t[lims.size()];
    auto* raw_ids = new int64_t[result_ids.size()];
    auto* raw_distances = new float[result_distances.size()];
    std::copy(lims.begin(), lims.end(), raw_lims);
    std::copy(result_ids.begin(), result_ids.end(), raw_ids);
    std::copy(result_distances.begin(), result_distances.end(), raw_distances);
    result->SetRows(nq);
    result->SetDim(0);
    result->SetLims(raw_lims);
    result->SetIds(raw_ids);
    result->SetDistance(raw_distances);
    result->SetIsOwner(true);
    return result;
}

template <typename HalfType>
inline expected<DataSetPtr>
RangeSearchHalf(const DataSetPtr& base_dataset,
                const DataSetPtr& query_dataset,
                const Json& config,
                const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    const auto* base_tensor =
        reinterpret_cast<const HalfType*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const HalfType*>(query_dataset->GetTensor());
    if (dim <= 0 || query_dim != dim || nb < 0 || nq < 0 ||
        base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto converted_base = ConvertHalfTensorToFloat32(base_tensor, nb, dim);
    const auto converted_query =
        ConvertHalfTensorToFloat32(query_tensor, nq, query_dim);
    auto float_base = GenDataSet(nb, dim, converted_base.data());
    float_base->SetTensorBeginId(base_dataset->GetTensorBeginId());
    float_base->SetIsOwner(false);
    if (base_dataset->GetIds() != nullptr) {
        float_base->SetIds(base_dataset->GetIds());
    }
    auto float_query = GenDataSet(nq, dim, converted_query.data());
    float_query->SetTensorBeginId(query_dataset->GetTensorBeginId());
    float_query->SetIsOwner(false);
    if (query_dataset->GetIds() != nullptr) {
        float_query->SetIds(query_dataset->GetIds());
    }

    return RangeSearchFloat(float_base, float_query, config, bitset);
}

inline expected<DataSetPtr>
RangeSearchInt8(const DataSetPtr& base_dataset,
                const DataSetPtr& query_dataset,
                const Json& config,
                const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto dim = base_dataset->GetDim();
    const auto query_dim = query_dataset->GetDim();
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    const auto* base_tensor =
        reinterpret_cast<const int8*>(base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const int8*>(query_dataset->GetTensor());
    if (dim <= 0 || query_dim != dim || nb < 0 || nq < 0 ||
        base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto converted_base = ConvertInt8TensorToFloat32(base_tensor, nb, dim);
    const auto converted_query =
        ConvertInt8TensorToFloat32(query_tensor, nq, query_dim);
    auto float_base = GenDataSet(nb, dim, converted_base.data());
    float_base->SetTensorBeginId(base_dataset->GetTensorBeginId());
    float_base->SetIsOwner(false);
    if (base_dataset->GetIds() != nullptr) {
        float_base->SetIds(base_dataset->GetIds());
    }
    auto float_query = GenDataSet(nq, dim, converted_query.data());
    float_query->SetTensorBeginId(query_dataset->GetTensorBeginId());
    float_query->SetIsOwner(false);
    if (query_dataset->GetIds() != nullptr) {
        float_query->SetIds(query_dataset->GetIds());
    }

    return RangeSearchFloat(float_base, float_query, config, bitset);
}

inline expected<DataSetPtr>
SearchSparse(const DataSetPtr& base_dataset,
             const DataSetPtr& query_dataset,
             const Json& config,
             const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto topk = config.value(meta::TOPK, int64_t{0});
    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    if (topk <= 0 || nb < 0 || nq < 0 || !base_dataset->IsSparse() ||
        !query_dataset->IsSparse()) {
        return Status::invalid_args;
    }

    const auto* base_tensor =
        reinterpret_cast<const sparse::SparseRow<sparse_u32_f32::ValueType>*>(
            base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const sparse::SparseRow<sparse_u32_f32::ValueType>*>(
            query_dataset->GetTensor());
    if (base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto metric_name = NormalizeMetric(
        config.value(meta::METRIC_TYPE, std::string(metric::IP)));
    if (!IsSparseBruteForceMetric(metric_name)) {
        return Status::invalid_metric_type;
    }

    const auto tensor_begin_id = base_dataset->GetTensorBeginId();
    const auto* base_ids = base_dataset->GetIds();
    using SearchHit = std::pair<float, int64_t>;
    std::vector<SearchHit> hits;
    hits.reserve(static_cast<size_t>(nb));

    auto compare_hits = [](const SearchHit& lhs, const SearchHit& rhs) {
        if (lhs.first == rhs.first) {
            return lhs.second < rhs.second;
        }
        return lhs.first > rhs.first;
    };

    auto result = std::make_shared<DataSet>();
    auto* ids = new sparse::label_t[static_cast<size_t>(nq * topk)];
    auto* distances = new float[static_cast<size_t>(nq * topk)];
    constexpr auto default_distance = -std::numeric_limits<float>::max();

    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        hits.clear();
        const auto& query = query_tensor[query_idx];
        for (int64_t base_idx = 0; base_idx < nb; ++base_idx) {
            if (IsMaskedByBitset(bitset, tensor_begin_id, base_idx)) {
                continue;
            }

            const auto score = base_tensor[base_idx].dot(query);
            if (score <= 0.0f) {
                continue;
            }

            hits.emplace_back(
                score, base_ids != nullptr ? base_ids[base_idx] : tensor_begin_id + base_idx);
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

    result->SetRows(nq);
    result->SetDim(topk);
    result->SetIds(ids);
    result->SetDistance(distances);
    result->SetIsOwner(true);
    return result;
}

inline expected<DataSetPtr>
RangeSearchSparse(const DataSetPtr& base_dataset,
                  const DataSetPtr& query_dataset,
                  const Json& config,
                  const BitsetView& bitset) {
    if (base_dataset == nullptr || query_dataset == nullptr) {
        return Status::invalid_args;
    }

    const auto nb = base_dataset->GetRows();
    const auto nq = query_dataset->GetRows();
    if (nb < 0 || nq < 0 || !base_dataset->IsSparse() || !query_dataset->IsSparse()) {
        return Status::invalid_args;
    }

    const auto* base_tensor =
        reinterpret_cast<const sparse::SparseRow<sparse_u32_f32::ValueType>*>(
            base_dataset->GetTensor());
    const auto* query_tensor =
        reinterpret_cast<const sparse::SparseRow<sparse_u32_f32::ValueType>*>(
            query_dataset->GetTensor());
    if (base_tensor == nullptr || query_tensor == nullptr) {
        return Status::invalid_args;
    }

    const auto metric_name = NormalizeMetric(
        config.value(meta::METRIC_TYPE, std::string(metric::IP)));
    if (!IsSparseBruteForceMetric(metric_name)) {
        return Status::invalid_metric_type;
    }

    if (!config.contains(meta::RADIUS)) {
        return Status::invalid_args;
    }

    const auto radius = config.value(meta::RADIUS, 0.0f);
    const bool has_range_filter = config.contains(meta::RANGE_FILTER);
    const auto range_filter = config.value(meta::RANGE_FILTER, 0.0f);
    const auto tensor_begin_id = base_dataset->GetTensorBeginId();
    const auto* base_ids = base_dataset->GetIds();

    std::vector<size_t> lims(static_cast<size_t>(nq) + 1, 0);
    std::vector<int64_t> result_ids;
    std::vector<float> result_distances;

    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        lims[static_cast<size_t>(query_idx)] = result_ids.size();
        const auto& query = query_tensor[query_idx];
        for (int64_t base_idx = 0; base_idx < nb; ++base_idx) {
            if (IsMaskedByBitset(bitset, tensor_begin_id, base_idx)) {
                continue;
            }

            const auto score = base_tensor[base_idx].dot(query);
            if (score <= 0.0f ||
                !IsRangeHit(metric_name, score, radius, has_range_filter, range_filter)) {
                continue;
            }

            result_ids.emplace_back(base_ids != nullptr ? base_ids[base_idx]
                                                        : tensor_begin_id + base_idx);
            result_distances.emplace_back(score);
        }
        lims[static_cast<size_t>(query_idx + 1)] = result_ids.size();
    }

    auto result = std::make_shared<DataSet>();
    auto* raw_lims = new size_t[lims.size()];
    auto* raw_ids = new int64_t[result_ids.size()];
    auto* raw_distances = new float[result_distances.size()];
    std::copy(lims.begin(), lims.end(), raw_lims);
    std::copy(result_ids.begin(), result_ids.end(), raw_ids);
    std::copy(result_distances.begin(), result_distances.end(), raw_distances);
    result->SetRows(nq);
    result->SetDim(0);
    result->SetLims(raw_lims);
    result->SetIds(raw_ids);
    result->SetDistance(raw_distances);
    result->SetIsOwner(true);
    return result;
}

class BufferedIterator : public IndexNode::Iterator {
 public:
    explicit BufferedIterator(std::vector<std::pair<int64_t, float>> hits)
        : hits_(std::move(hits)) {
    }

    bool
    HasNext() override {
        return cursor_ < hits_.size();
    }

    std::pair<int64_t, float>
    Next() override {
        if (!HasNext()) {
            return {-1, 0.0f};
        }
        return hits_[cursor_++];
    }

 private:
    std::vector<std::pair<int64_t, float>> hits_;
    size_t cursor_ = 0;
};

inline expected<std::vector<IndexNode::IteratorPtr>>
BuildIteratorsFromSearchResult(const DataSetPtr& result) {
    if (result == nullptr || result->GetIds() == nullptr ||
        result->GetDistance() == nullptr) {
        return Status::invalid_args;
    }

    const auto nq = result->GetRows();
    const auto topk = result->GetDim();
    if (nq < 0 || topk < 0) {
        return Status::invalid_args;
    }

    std::vector<IndexNode::IteratorPtr> iterators;
    iterators.reserve(static_cast<size_t>(nq));
    const auto* ids = result->GetIds();
    const auto* distances = result->GetDistance();
    for (int64_t query_idx = 0; query_idx < nq; ++query_idx) {
        std::vector<std::pair<int64_t, float>> hits;
        hits.reserve(static_cast<size_t>(topk));
        for (int64_t rank = 0; rank < topk; ++rank) {
            const auto offset = query_idx * topk + rank;
            if (ids[offset] < 0) {
                break;
            }
            hits.emplace_back(ids[offset], distances[offset]);
        }
        iterators.emplace_back(
            std::make_shared<BufferedIterator>(std::move(hits)));
    }
    return iterators;
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
        if constexpr (std::is_same_v<DataType, fp16> ||
                      std::is_same_v<DataType, bf16>) {
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
            auto status = detail::SearchHalfWithBuf<DataType>(
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
        if constexpr (std::is_same_v<DataType, int8>) {
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
            auto status = detail::SearchInt8WithBuf(
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
        if constexpr (std::is_same_v<DataType, bin1>) {
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
            auto status = detail::SearchBinaryWithBuf(
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
        if constexpr (std::is_same_v<DataType,
                                     sparse::SparseRow<sparse_u32_f32::ValueType>>) {
            return detail::SearchSparse(base_dataset, query_dataset, config, bitset);
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
        if constexpr (std::is_same_v<DataType, fp16> ||
                      std::is_same_v<DataType, bf16>) {
            return detail::SearchHalfWithBuf<DataType>(base_dataset,
                                                       query_dataset,
                                                       ids,
                                                       distances,
                                                       config,
                                                       bitset);
        }
        if constexpr (std::is_same_v<DataType, int8>) {
            return detail::SearchInt8WithBuf(base_dataset,
                                             query_dataset,
                                             ids,
                                             distances,
                                             config,
                                             bitset);
        }
        if constexpr (std::is_same_v<DataType, float>) {
            return detail::SearchFloatWithBuf(base_dataset,
                                              query_dataset,
                                              ids,
                                              distances,
                                              config,
                                              bitset);
        }
        if constexpr (std::is_same_v<DataType, bin1>) {
            return detail::SearchBinaryWithBuf(base_dataset,
                                               query_dataset,
                                               ids,
                                               distances,
                                               config,
                                               bitset);
        }
        if constexpr (std::is_same_v<DataType,
                                     sparse::SparseRow<sparse_u32_f32::ValueType>>) {
            auto result = detail::SearchSparse(base_dataset, query_dataset, config, bitset);
            if (!result.has_value()) {
                return result.error();
            }
            if (result.value() == nullptr || result.value()->GetIds() == nullptr ||
                result.value()->GetDistance() == nullptr) {
                return Status::invalid_args;
            }
            const auto count = static_cast<size_t>(
                result.value()->GetRows() * result.value()->GetDim());
            std::copy_n(result.value()->GetIds(), count, ids);
            std::copy_n(result.value()->GetDistance(), count, distances);
            return Status::success;
        }
        return Status::not_implemented;
    }

    template <typename DataType>
    static expected<DataSetPtr>
    RangeSearch(const DataSetPtr base_dataset,
                const DataSetPtr query_dataset,
                const Json& config,
                const BitsetView& bitset,
                milvus::OpContext* = nullptr) {
        if constexpr (std::is_same_v<DataType, float>) {
            return detail::RangeSearchFloat(
                base_dataset, query_dataset, config, bitset);
        }
        if constexpr (std::is_same_v<DataType, fp16> ||
                      std::is_same_v<DataType, bf16>) {
            return detail::RangeSearchHalf<DataType>(
                base_dataset, query_dataset, config, bitset);
        }
        if constexpr (std::is_same_v<DataType, int8>) {
            return detail::RangeSearchInt8(
                base_dataset, query_dataset, config, bitset);
        }
        if constexpr (std::is_same_v<DataType, bin1>) {
            return detail::RangeSearchBinary(
                base_dataset, query_dataset, config, bitset);
        }
        if constexpr (std::is_same_v<DataType,
                                     sparse::SparseRow<sparse_u32_f32::ValueType>>) {
            return detail::RangeSearchSparse(
                base_dataset, query_dataset, config, bitset);
        }
        return Status::not_implemented;
    }

    static expected<DataSetPtr>
    SearchSparse(const DataSetPtr base_dataset,
                 const DataSetPtr query_dataset,
                 const Json& config,
                 const BitsetView& bitset,
                 milvus::OpContext* = nullptr) {
        return detail::SearchSparse(base_dataset, query_dataset, config, bitset);
    }

    static Status
    SearchSparseWithBuf(const DataSetPtr base_dataset,
                        const DataSetPtr query_dataset,
                        sparse::label_t* ids,
                        float* distances,
                        const Json& config,
                        const BitsetView& bitset,
                        milvus::OpContext* = nullptr) {
        auto result = detail::SearchSparse(base_dataset, query_dataset, config, bitset);
        if (!result.has_value()) {
            return result.error();
        }
        if (result.value() == nullptr || result.value()->GetIds() == nullptr ||
            result.value()->GetDistance() == nullptr) {
            return Status::invalid_args;
        }
        const auto count =
            static_cast<size_t>(result.value()->GetRows() * result.value()->GetDim());
        std::copy_n(result.value()->GetIds(), count, ids);
        std::copy_n(result.value()->GetDistance(), count, distances);
        return Status::success;
    }

    template <typename DataType>
    static expected<std::vector<IndexNode::IteratorPtr>>
    AnnIterator(const DataSetPtr base_dataset,
                const DataSetPtr query_dataset,
                const Json& config,
                const BitsetView& bitset,
                bool = true,
                milvus::OpContext* = nullptr) {
        if constexpr (std::is_same_v<DataType, float> ||
                      std::is_same_v<DataType, int8> ||
                      std::is_same_v<DataType, fp16> ||
                      std::is_same_v<DataType, bf16> ||
                      std::is_same_v<DataType, bin1> ||
                      std::is_same_v<DataType,
                                     sparse::SparseRow<sparse_u32_f32::ValueType>>) {
            if (base_dataset == nullptr || query_dataset == nullptr) {
                return Status::invalid_args;
            }

            Json iterator_config = config;
            iterator_config[meta::TOPK] = base_dataset->GetRows();
            auto result =
                Search<DataType>(base_dataset, query_dataset, iterator_config, bitset);
            if (!result.has_value()) {
                return result.error();
            }
            return detail::BuildIteratorsFromSearchResult(result.value());
        }
        return Status::not_implemented;
    }
};

}  // namespace knowhere
