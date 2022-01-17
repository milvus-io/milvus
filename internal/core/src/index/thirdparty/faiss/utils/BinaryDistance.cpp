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
#ifdef __aarch64__
uint8_t lookup8bit[256] = {
    /* 0 */ 0, /* 1 */ 1, /* 2 */ 1, /* 3 */ 2,
    /* 4 */ 1, /* 5 */ 2, /* 6 */ 2, /* 7 */ 3,
    /* 8 */ 1, /* 9 */ 2, /* a */ 2, /* b */ 3,
    /* c */ 2, /* d */ 3, /* e */ 3, /* f */ 4,
    /* 10 */ 1, /* 11 */ 2, /* 12 */ 2, /* 13 */ 3,
    /* 14 */ 2, /* 15 */ 3, /* 16 */ 3, /* 17 */ 4,
    /* 18 */ 2, /* 19 */ 3, /* 1a */ 3, /* 1b */ 4,
    /* 1c */ 3, /* 1d */ 4, /* 1e */ 4, /* 1f */ 5,
    /* 20 */ 1, /* 21 */ 2, /* 22 */ 2, /* 23 */ 3,
    /* 24 */ 2, /* 25 */ 3, /* 26 */ 3, /* 27 */ 4,
    /* 28 */ 2, /* 29 */ 3, /* 2a */ 3, /* 2b */ 4,
    /* 2c */ 3, /* 2d */ 4, /* 2e */ 4, /* 2f */ 5,
    /* 30 */ 2, /* 31 */ 3, /* 32 */ 3, /* 33 */ 4,
    /* 34 */ 3, /* 35 */ 4, /* 36 */ 4, /* 37 */ 5,
    /* 38 */ 3, /* 39 */ 4, /* 3a */ 4, /* 3b */ 5,
    /* 3c */ 4, /* 3d */ 5, /* 3e */ 5, /* 3f */ 6,
    /* 40 */ 1, /* 41 */ 2, /* 42 */ 2, /* 43 */ 3,
    /* 44 */ 2, /* 45 */ 3, /* 46 */ 3, /* 47 */ 4,
    /* 48 */ 2, /* 49 */ 3, /* 4a */ 3, /* 4b */ 4,
    /* 4c */ 3, /* 4d */ 4, /* 4e */ 4, /* 4f */ 5,
    /* 50 */ 2, /* 51 */ 3, /* 52 */ 3, /* 53 */ 4,
    /* 54 */ 3, /* 55 */ 4, /* 56 */ 4, /* 57 */ 5,
    /* 58 */ 3, /* 59 */ 4, /* 5a */ 4, /* 5b */ 5,
    /* 5c */ 4, /* 5d */ 5, /* 5e */ 5, /* 5f */ 6,
    /* 60 */ 2, /* 61 */ 3, /* 62 */ 3, /* 63 */ 4,
    /* 64 */ 3, /* 65 */ 4, /* 66 */ 4, /* 67 */ 5,
    /* 68 */ 3, /* 69 */ 4, /* 6a */ 4, /* 6b */ 5,
    /* 6c */ 4, /* 6d */ 5, /* 6e */ 5, /* 6f */ 6,
    /* 70 */ 3, /* 71 */ 4, /* 72 */ 4, /* 73 */ 5,
    /* 74 */ 4, /* 75 */ 5, /* 76 */ 5, /* 77 */ 6,
    /* 78 */ 4, /* 79 */ 5, /* 7a */ 5, /* 7b */ 6,
    /* 7c */ 5, /* 7d */ 6, /* 7e */ 6, /* 7f */ 7,
    /* 80 */ 1, /* 81 */ 2, /* 82 */ 2, /* 83 */ 3,
    /* 84 */ 2, /* 85 */ 3, /* 86 */ 3, /* 87 */ 4,
    /* 88 */ 2, /* 89 */ 3, /* 8a */ 3, /* 8b */ 4,
    /* 8c */ 3, /* 8d */ 4, /* 8e */ 4, /* 8f */ 5,
    /* 90 */ 2, /* 91 */ 3, /* 92 */ 3, /* 93 */ 4,
    /* 94 */ 3, /* 95 */ 4, /* 96 */ 4, /* 97 */ 5,
    /* 98 */ 3, /* 99 */ 4, /* 9a */ 4, /* 9b */ 5,
    /* 9c */ 4, /* 9d */ 5, /* 9e */ 5, /* 9f */ 6,
    /* a0 */ 2, /* a1 */ 3, /* a2 */ 3, /* a3 */ 4,
    /* a4 */ 3, /* a5 */ 4, /* a6 */ 4, /* a7 */ 5,
    /* a8 */ 3, /* a9 */ 4, /* aa */ 4, /* ab */ 5,
    /* ac */ 4, /* ad */ 5, /* ae */ 5, /* af */ 6,
    /* b0 */ 3, /* b1 */ 4, /* b2 */ 4, /* b3 */ 5,
    /* b4 */ 4, /* b5 */ 5, /* b6 */ 5, /* b7 */ 6,
    /* b8 */ 4, /* b9 */ 5, /* ba */ 5, /* bb */ 6,
    /* bc */ 5, /* bd */ 6, /* be */ 6, /* bf */ 7,
    /* c0 */ 2, /* c1 */ 3, /* c2 */ 3, /* c3 */ 4,
    /* c4 */ 3, /* c5 */ 4, /* c6 */ 4, /* c7 */ 5,
    /* c8 */ 3, /* c9 */ 4, /* ca */ 4, /* cb */ 5,
    /* cc */ 4, /* cd */ 5, /* ce */ 5, /* cf */ 6,
    /* d0 */ 3, /* d1 */ 4, /* d2 */ 4, /* d3 */ 5,
    /* d4 */ 4, /* d5 */ 5, /* d6 */ 5, /* d7 */ 6,
    /* d8 */ 4, /* d9 */ 5, /* da */ 5, /* db */ 6,
    /* dc */ 5, /* dd */ 6, /* de */ 6, /* df */ 7,
    /* e0 */ 3, /* e1 */ 4, /* e2 */ 4, /* e3 */ 5,
    /* e4 */ 4, /* e5 */ 5, /* e6 */ 5, /* e7 */ 6,
    /* e8 */ 4, /* e9 */ 5, /* ea */ 5, /* eb */ 6,
    /* ec */ 5, /* ed */ 6, /* ee */ 6, /* ef */ 7,
    /* f0 */ 4, /* f1 */ 5, /* f2 */ 5, /* f3 */ 6,
    /* f4 */ 5, /* f5 */ 6, /* f6 */ 6, /* f7 */ 7,
    /* f8 */ 5, /* f9 */ 6, /* fa */ 6, /* fb */ 7,
    /* fc */ 6, /* fd */ 7, /* fe */ 7, /* ff */ 8
};
#endif


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
#ifdef __AVX__
            binary_distance_knn_hc<C, faiss::JaccardComputerAVX2>
                    (ncodes, ha, a, b, nb, bitset);
#endif
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
#ifdef __AVX__
            binary_distance_knn_hc<C, faiss::JaccardComputerAVX2>
            binary_distance_knn_hc<C, faiss::HammingComputerAVX2>
                    (ncodes, ha, a, b, nb, bitset);
#endif
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

#ifdef __AVX__
            binary_distance_knn_hc<C, faiss::JaccardComputerAVX2>
            binary_range_search<C, T, faiss::JaccardComputerAVX2>
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
#endif
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
#ifdef __AVX__
            binary_range_search<C, T, faiss::HammingComputerAVX2>
                    (a, b, na, nb, ncodes, radius, result, buffer_size, bitset);
        #endif
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
