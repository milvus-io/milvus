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

#include <faiss/utils/BinaryDistance.h>

#include <limits.h>
#include <omp.h>
#include <typeinfo>

#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/utils.h>
#include <faiss/FaissHook.h>
#include <faiss/utils/hamming.h>
#include <faiss/utils/jaccard-inl.h>
#include <faiss/utils/substructure-inl.h>
#include <faiss/utils/superstructure-inl.h>

#include <faiss/utils/distances.h>

namespace faiss {

#define fast_loop_imp(fun_u64, fun_u8) \
    auto a = reinterpret_cast<const uint64_t*>(data1); \
    auto b = reinterpret_cast<const uint64_t*>(data2); \
    int div = code_size / 8; \
    int mod = code_size % 8; \
    int i = 0, len = div; \
    switch(len & 7) { \
        default: \
            while (len > 7) { \
                len -= 8; \
                fun_u64; i++; \
                case 7: fun_u64; i++; \
                case 6: fun_u64; i++; \
                case 5: fun_u64; i++; \
                case 4: fun_u64; i++; \
                case 3: fun_u64; i++; \
                case 2: fun_u64; i++; \
                case 1: fun_u64; i++; \
            } \
    } \
    if (mod) { \
        auto a = data1 + 8 * div; \
        auto b = data2 + 8 * div; \
        switch (mod) { \
            case 7: fun_u8(6); \
            case 6: fun_u8(5); \
            case 5: fun_u8(4); \
            case 4: fun_u8(3); \
            case 3: fun_u8(2); \
            case 2: fun_u8(1); \
            case 1: fun_u8(0); \
            default: break; \
        } \
    }

int popcnt(const uint8_t* data, const size_t code_size) {
    auto data1 = data, data2 = data; // for the macro fast_loop_imp
#define fun_u64 accu += popcount64(a[i])
#define fun_u8(i) accu += lookup8bit[a[i]]
    int accu = 0;
    fast_loop_imp(fun_u64, fun_u8);
    return accu;
#undef fun_u64
#undef fun_u8
}

int xor_popcnt(const uint8_t* data1, const uint8_t*data2, const size_t code_size) {
#define fun_u64 accu += popcount64(a[i] ^ b[i]);
#define fun_u8(i) accu += lookup8bit[a[i] ^ b[i]];
    int accu = 0;
    fast_loop_imp(fun_u64, fun_u8);
    return accu;
#undef fun_u64
#undef fun_u8
}

int or_popcnt(const uint8_t* data1, const uint8_t*data2, const size_t code_size) {
#define fun_u64 accu += popcount64(a[i] | b[i])
#define fun_u8(i) accu += lookup8bit[a[i] | b[i]]
    int accu = 0;
    fast_loop_imp(fun_u64, fun_u8);
    return accu;
#undef fun_u64
#undef fun_u8
}

int and_popcnt(const uint8_t* data1, const uint8_t*data2, const size_t code_size) {
#define fun_u64 accu += popcount64(a[i] & b[i])
#define fun_u8(i) accu += lookup8bit[a[i] & b[i]]
    int accu = 0;
    fast_loop_imp(fun_u64, fun_u8);
    return accu;
#undef fun_u64
#undef fun_u8
}

bool is_subset(const uint8_t* data1, const uint8_t* data2, const size_t code_size) {
#define fun_u64 if((a[i] & b[i]) != a[i]) return false
#define fun_u8(i) if((a[i] & b[i]) != a[i]) return false
    fast_loop_imp(fun_u64, fun_u8);
    return true;
#undef fun_u64
#undef fun_u8
}

float bvec_jaccard (const uint8_t* data1, const uint8_t* data2, const size_t code_size) {
#define fun_u64 accu_num += popcount64(a[i] & b[i]); accu_den += popcount64(a[i] | b[i])
#define fun_u8(i) accu_num += lookup8bit[a[i] & b[i]]; accu_den += lookup8bit[a[i] | b[i]]
    int accu_num = 0;
    int accu_den = 0;
    fast_loop_imp(fun_u64, fun_u8);
    return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
#undef fun_u64
#undef fun_u8
}

template <class T>
static
void binary_distance_knn_mc(
        int bytes_per_code,
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n1,
        size_t n2,
        size_t k,
        float *distances,
        int64_t *labels,
        const BitsetView bitset)
{
    int thread_max_num = omp_get_max_threads();
    size_t l3_size = get_L3_Size();

    /*
     * Later we may propose a more reasonable strategy.
     */
    if (n1 < n2) {
        size_t group_num = n1 * thread_max_num;
        size_t *match_num = new size_t[group_num];
        int64_t *match_data = new int64_t[group_num * k];
        for (size_t i = 0; i < group_num; i++) {
            match_num[i] = 0;
        }

        T *hc = new T[n1];
        for (size_t i = 0; i < n1; i++) {
            hc[i].set(bs1 + i * bytes_per_code, bytes_per_code);
        }

#pragma omp parallel for
        for (size_t j = 0; j < n2; j++) {
            if(!bitset || !bitset.test(j)) {
                int thread_no = omp_get_thread_num();

                const uint8_t * bs2_ = bs2 + j * bytes_per_code;
                for (size_t i = 0; i < n1; i++) {
                    if (hc[i].compute(bs2_)) {
                        size_t match_index = thread_no * n1 + i;
                        size_t &index = match_num[match_index];
                        if (index < k) {
                            match_data[match_index * k + index] = j;
                            index++;
                        }
                    }
                }
            }
        }
        for (size_t i = 0; i < n1; i++) {
            size_t n_i = 0;
            float *distances_i = distances + i * k;
            int64_t *labels_i = labels + i * k;

            for (size_t t = 0; t < thread_max_num && n_i < k; t++) {
                size_t match_index = t * n1 + i;
                size_t copy_num = std::min(k - n_i, match_num[match_index]);
                memcpy(labels_i + n_i, match_data + match_index * k, copy_num * sizeof(int64_t));
                memset(distances_i + n_i, 0, copy_num * sizeof(float));
                n_i += copy_num;
            }
            for (; n_i < k; n_i++) {
                distances_i[n_i] = 1.0 / 0.0;
                labels_i[n_i] = -1;
            }
        }

        delete[] hc;
        delete[] match_num;
        delete[] match_data;

    } else {
        const size_t block_size = l3_size / bytes_per_code;

        size_t *num = new size_t[n1];
        for (size_t i = 0; i < n1; i++) {
            num[i] = 0;
        }

        for (size_t j0 = 0; j0 < n2; j0 += block_size) {
            const size_t j1 = std::min(j0 + block_size, n2);
#pragma omp parallel for
            for (size_t i = 0; i < n1; i++) {
                size_t num_i = num[i];
                if (num_i == k) continue;
                float * dis = distances + i * k;
                int64_t * lab = labels + i * k;

                T hc (bs1 + i * bytes_per_code, bytes_per_code);
                const uint8_t * bs2_ = bs2 + j0 * bytes_per_code;
                for (size_t j = j0; j < j1; j++, bs2_ += bytes_per_code) {
                    if(!bitset || !bitset.test(j)){
                        if (hc.compute (bs2_)) {
                            dis[num_i] = 0;
                            lab[num_i] = j;
                            if (++num_i == k) break;
                        }
                    }
                }
                num[i] = num_i;
            }
        }

        for (size_t i = 0; i < n1; i++) {
            float * dis = distances + i * k;
            int64_t * lab = labels + i * k;
            for (size_t num_i = num[i]; num_i < k; num_i++) {
                dis[num_i] = 1.0 / 0.0;
                lab[num_i] = -1;
            }
        }

        delete[] num;
    }
}

void binary_distance_knn_mc (
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        size_t k,
        size_t ncodes,
        float *distances,
        int64_t *labels,
        const BitsetView bitset) {

    switch (metric_type) {
    case METRIC_Substructure:
        switch (ncodes) {
#define binary_distance_knn_mc_Substructure(ncodes) \
        case ncodes: \
            binary_distance_knn_mc<faiss::SubstructureComputer ## ncodes> \
                (ncodes, a, b, na, nb, k, distances, labels, bitset); \
        break;
        binary_distance_knn_mc_Substructure(8);
        binary_distance_knn_mc_Substructure(16);
        binary_distance_knn_mc_Substructure(32);
        binary_distance_knn_mc_Substructure(64);
        binary_distance_knn_mc_Substructure(128);
        binary_distance_knn_mc_Substructure(256);
        binary_distance_knn_mc_Substructure(512);
#undef binary_distance_knn_mc_Substructure
        default:
            binary_distance_knn_mc<faiss::SubstructureComputerDefault>
                    (ncodes, a, b, na, nb, k, distances, labels, bitset);
            break;
        }
        break;

    case METRIC_Superstructure:
        switch (ncodes) {
#define binary_distance_knn_mc_Superstructure(ncodes) \
        case ncodes: \
            binary_distance_knn_mc<faiss::SuperstructureComputer ## ncodes> \
                (ncodes, a, b, na, nb, k, distances, labels, bitset); \
        break;
        binary_distance_knn_mc_Superstructure(8);
        binary_distance_knn_mc_Superstructure(16);
        binary_distance_knn_mc_Superstructure(32);
        binary_distance_knn_mc_Superstructure(64);
        binary_distance_knn_mc_Superstructure(128);
        binary_distance_knn_mc_Superstructure(256);
        binary_distance_knn_mc_Superstructure(512);
#undef binary_distance_knn_mc_Superstructure
        default:
            binary_distance_knn_mc<faiss::SuperstructureComputerDefault>
                    (ncodes, a, b, na, nb, k, distances, labels, bitset);
            break;
        }
        break;

    default:
        break;
    }
}


template <class C, class MetricComputer>
void binary_distance_knn_hc (
        int bytes_per_code,
        HeapArray<C> * ha,
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n2,
        const BitsetView bitset = nullptr)
{
    typedef typename C::T T;
    size_t k = ha->k;

    size_t l3_size = get_L3_Size();
    size_t thread_max_num = omp_get_max_threads();

    /*
     * Here is an empirical formula, and later we may propose a more reasonable strategy.
     */
    if ((bytes_per_code + k * (sizeof(T) + sizeof(int64_t))) * ha->nh * thread_max_num <= l3_size &&
            (ha->nh < (n2 >> 11) + thread_max_num / 3)) {
        // init heap
        size_t thread_heap_size = ha->nh * k;
        size_t all_heap_size = thread_heap_size * thread_max_num;
        T *value = new T[all_heap_size];
        int64_t *labels = new int64_t[all_heap_size];
        T init_value = (typeid(T) == typeid(float)) ? (1.0 / 0.0) : 0x7fffffff;
        for (int i = 0; i < all_heap_size; i++) {
            value[i] = init_value;
            labels[i] = -1;
        }

        MetricComputer *hc = new MetricComputer[ha->nh];
        for (size_t i = 0; i < ha->nh; i++) {
            hc[i].set(bs1 + i * bytes_per_code, bytes_per_code);
        }

#pragma omp parallel for
        for (size_t j = 0; j < n2; j++) {
            if(!bitset || !bitset.test(j)) {
                int thread_no = omp_get_thread_num();

                const uint8_t * bs2_ = bs2 + j * bytes_per_code;
                for (size_t i = 0; i < ha->nh; i++) {
                    T dis = hc[i].compute (bs2_);
                    T *val_ = value + thread_no * thread_heap_size + i * k;
                    int64_t *ids_ = labels + thread_no * thread_heap_size + i * k;
                    if (C::cmp(val_[0], dis)) {
                        faiss::heap_swap_top<C>(k, val_, ids_, dis, j);
                    }
                }
            }
        }

        for (size_t t = 1; t < thread_max_num; t++) {
            // merge heap
            for (size_t i = 0; i < ha->nh; i++) {
                T * __restrict value_x = value + i * k;
                int64_t * __restrict labels_x = labels + i * k;
                T *value_x_t = value_x + t * thread_heap_size;
                int64_t *labels_x_t = labels_x + t * thread_heap_size;
                for (size_t j = 0; j < k; j++) {
                    if (C::cmp(value_x[0], value_x_t[j])) {
                        faiss::heap_swap_top<C>(k, value_x, labels_x, value_x_t[j], labels_x_t[j]);
                    }
                }
            }
        }

        // copy result
        memcpy(ha->val, value, thread_heap_size * sizeof(T));
        memcpy(ha->ids, labels, thread_heap_size * sizeof(int64_t));

        delete[] hc;
        delete[] value;
        delete[] labels;

    } else {
        const size_t block_size = l3_size / bytes_per_code;

        ha->heapify ();

        for (size_t j0 = 0; j0 < n2; j0 += block_size) {
            const size_t j1 = std::min(j0 + block_size, n2);
#pragma omp parallel for
            for (size_t i = 0; i < ha->nh; i++) {
                MetricComputer hc (bs1 + i * bytes_per_code, bytes_per_code);

                const uint8_t *bs2_ = bs2 + j0 * bytes_per_code;
                T dis;
                T *__restrict bh_val_ = ha->val + i * k;
                int64_t *__restrict bh_ids_ = ha->ids + i * k;
                for (size_t j = j0; j < j1; j++, bs2_ += bytes_per_code) {
                    if (!bitset || !bitset.test(j)) {
                        dis = hc.compute (bs2_);
                        if (C::cmp(bh_val_[0], dis)) {
                            faiss::heap_swap_top<C>(k, bh_val_, bh_ids_, dis, j);
                        }
                    }
                }

            }
        }
    }
    ha->reorder ();
}

template <class C>
void binary_distance_knn_hc (
        MetricType metric_type,
        HeapArray<C> * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        const BitsetView bitset)
{
    switch (metric_type) {
    case METRIC_Jaccard: {
        if (cpu_support_avx2() && ncodes > 64) {
            binary_distance_knn_hc<C, faiss::JaccardComputerAVX2>
                    (ncodes, ha, a, b, nb, bitset);
        } else {
            switch (ncodes) {
#define binary_distance_knn_hc_jaccard(ncodes) \
            case ncodes: \
                binary_distance_knn_hc<C, faiss::JaccardComputer ## ncodes> \
                    (ncodes, ha, a, b, nb, bitset); \
                break;
            binary_distance_knn_hc_jaccard(8);
            binary_distance_knn_hc_jaccard(16);
            binary_distance_knn_hc_jaccard(32);
            binary_distance_knn_hc_jaccard(64);
            binary_distance_knn_hc_jaccard(128);
            binary_distance_knn_hc_jaccard(256);
            binary_distance_knn_hc_jaccard(512);
#undef binary_distance_knn_hc_jaccard
            default:
                binary_distance_knn_hc<C, faiss::JaccardComputerDefault>
                        (ncodes, ha, a, b, nb, bitset);
                break;
            }
        }
        break;
    }

    case METRIC_Hamming: {
        if (cpu_support_avx2() && ncodes > 64) {
            binary_distance_knn_hc<C, faiss::HammingComputerAVX2>
                    (ncodes, ha, a, b, nb, bitset);
        } else {
            switch (ncodes) {
#define binary_distance_knn_hc_hamming(ncodes) \
            case ncodes: \
                binary_distance_knn_hc<C, faiss::HammingComputer ## ncodes> \
                    (ncodes, ha, a, b, nb, bitset); \
                break;
            binary_distance_knn_hc_hamming(4);
            binary_distance_knn_hc_hamming(8);
            binary_distance_knn_hc_hamming(16);
            binary_distance_knn_hc_hamming(20);
            binary_distance_knn_hc_hamming(32);
            binary_distance_knn_hc_hamming(64);
#undef binary_distance_knn_hc_hamming
            default:
                binary_distance_knn_hc<C, faiss::HammingComputerDefault>
                        (ncodes, ha, a, b, nb, bitset);
                break;
            }
        }
        break;
    }

    default:
        break;
    }
}

template
void binary_distance_knn_hc<CMax<int, int64_t>>(
        MetricType metric_type,
        int_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        const BitsetView bitset);

template
void binary_distance_knn_hc<CMax<float, int64_t>>(
        MetricType metric_type,
        float_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        const BitsetView bitset);


template <class C, typename T, class MetricComputer>
void binary_range_search (
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    size_t ncodes,
    T radius,
    std::vector<faiss::RangeSearchPartialResult*>& result,
    size_t buffer_size,
    const BitsetView bitset)
{

#pragma omp parallel
    {
        RangeSearchResult *tmp_res = new RangeSearchResult(na);
        tmp_res->buffer_size = buffer_size;
        auto pres = new RangeSearchPartialResult(tmp_res);

        MetricComputer mc(a, ncodes);
        RangeQueryResult& qres = pres->new_result(0);

#pragma omp for
        for (size_t j = 0; j < nb; j++) {
            if(!bitset || !bitset.test(j)) {
                T dist = mc.compute(b + j * ncodes);
                if (C::cmp(radius, dist)) {
                    qres.add(dist, j);
                }
            }
        }
#pragma omp critical
        result.push_back(pres);
    }
}

template <class C, typename T>
void binary_range_search(
    MetricType metric_type,
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    T radius,
    size_t ncodes,
    std::vector<faiss::RangeSearchPartialResult*>& result,
    size_t buffer_size,
    const BitsetView bitset) {

    switch (metric_type) {
    case METRIC_Tanimoto:
        radius = Tanimoto_2_Jaccard(radius);
    case METRIC_Jaccard: {
        if (cpu_support_avx2() && ncodes > 64) {
            binary_range_search<C, T, faiss::JaccardComputerAVX2>
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
        } else {
            switch (ncodes) {
#define binary_range_search_jaccard(ncodes) \
            case ncodes: \
                binary_range_search<C, T, faiss::JaccardComputer ## ncodes> \
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset); \
                break;
            binary_range_search_jaccard(8);
            binary_range_search_jaccard(16);
            binary_range_search_jaccard(32);
            binary_range_search_jaccard(64);
            binary_range_search_jaccard(128);
            binary_range_search_jaccard(256);
            binary_range_search_jaccard(512);
#undef binary_range_search_jaccard
            default:
                binary_range_search<C, T, faiss::JaccardComputerDefault>
                        (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
                break;
            }
        }
        if (METRIC_Tanimoto == metric_type) {
            for (auto &prspr: result) {
                auto len = prspr->buffers.size() * prspr->buffer_size - prspr->buffer_size + prspr->wp;
                for (auto &buf: prspr->buffers) {
                    for (auto i = 0; i < prspr->buffer_size && i < len; ++ i)
                        buf.dis[i] = Jaccard_2_Tanimoto(buf.dis[i]);
                    len -= prspr->buffer_size;
                }
            }
        }
        break;
    }

    case METRIC_Hamming: {
        if (cpu_support_avx2() && ncodes > 64) {
            binary_range_search<C, T, faiss::HammingComputerAVX2>
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
        } else {
            switch (ncodes) {
#define binary_range_search_hamming(ncodes) \
            case ncodes: \
                binary_range_search<C, T, faiss::HammingComputer ## ncodes> \
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset); \
                break;
            binary_range_search_hamming(4);
            binary_range_search_hamming(8);
            binary_range_search_hamming(16);
            binary_range_search_hamming(20);
            binary_range_search_hamming(32);
            binary_range_search_hamming(64);
#undef binary_range_search_hamming
            default:
                binary_range_search<C, T, faiss::HammingComputerDefault>
                        (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
                break;
            }
        }
        break;
    }

    case METRIC_Superstructure: {
        switch (ncodes) {
#define binary_range_search_superstructure(ncodes) \
        case ncodes: \
            binary_range_search<C, T, faiss::SuperstructureComputer ## ncodes> \
                (a, b, na, nb, ncodes, radius, result, buffer_size, bitset); \
            break;
        binary_range_search_superstructure(8);
        binary_range_search_superstructure(16);
        binary_range_search_superstructure(32);
        binary_range_search_superstructure(64);
        binary_range_search_superstructure(128);
        binary_range_search_superstructure(256);
        binary_range_search_superstructure(512);
#undef binary_range_search_superstructure
        default:
            binary_range_search<C, T, faiss::SuperstructureComputerDefault>
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
            break;
        }
        break;
    }

    case METRIC_Substructure: {
        switch (ncodes) {
#define binary_range_search_substructure(ncodes) \
        case ncodes: \
            binary_range_search<C, T, faiss::SubstructureComputer ## ncodes> \
                (a, b, na, nb, ncodes, radius, result, buffer_size, bitset); \
            break;
        binary_range_search_substructure(8);
        binary_range_search_substructure(16);
        binary_range_search_substructure(32);
        binary_range_search_substructure(64);
        binary_range_search_substructure(128);
        binary_range_search_substructure(256);
        binary_range_search_substructure(512);
#undef binary_range_search_substructure
        default:
            binary_range_search<C, T, faiss::SubstructureComputerDefault>
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
            break;
        }
        break;
    }

    default:
        break;
    }
}

template
void binary_range_search<CMax<int, int64_t>, int>(
    MetricType metric_type,
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    int radius,
    size_t ncodes,
    std::vector<faiss::RangeSearchPartialResult*>& result,
    size_t buffer_size,
    const BitsetView bitset);

template
void binary_range_search<CMax<float, int64_t>, float>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        float radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView bitset);

template
void binary_range_search<CMin<bool, int64_t>, bool>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        bool radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView bitset);

} // namespace faiss
