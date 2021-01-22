#include <faiss/utils/BinaryDistance.h>

#include <vector>
#include <memory>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <limits.h>
#include <omp.h>
#include <typeinfo>


#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/utils.h>
#include <faiss/FaissHook.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/distances_avx.h>
#include <faiss/utils/distances_avx512.h>
#include <faiss/utils/hamming.h>
#include <faiss/utils/substructure-inl.h>
#include <faiss/utils/superstructure-inl.h>

namespace faiss {

int
popcnt(const uint8_t* data, const size_t n) {
    const uint64_t *b = (uint64_t *)data;
    int accu = 0;
    int i = 0;
    for (; i < n/8; i++)
        accu += popcount64 (b[i]);
    switch( n % 8 )
    {
        case 7: accu += popcount64 (data[i*8 + 6]);
        case 6: accu += popcount64 (data[i*8 + 5]);
        case 5: accu += popcount64 (data[i*8 + 4]);
        case 4: accu += popcount64 (data[i*8 + 3]);
        case 3: accu += popcount64 (data[i*8 + 2]);
        case 2: accu += popcount64 (data[i*8 + 1]);
        case 1: accu += popcount64 (data[i*8 + 0]);
        default: break;
    }
    return accu;
}

int
XOR_popcnt(const uint8_t* data1, const uint8_t*data2, const size_t n) {
    const uint64_t *a = (uint64_t *)data1;
    const uint64_t *b = (uint64_t *)data2;
    int accu = 0;
    int i = 0;
    for (; i < n/8; i++)
        accu += popcount64 (a[i] ^ b[i]);
    switch( n % 8 )
    {
        case 7: accu += popcount64 (data1[i*8 + 6] ^ data2[i*8 + 6]);
        case 6: accu += popcount64 (data1[i*8 + 5] ^ data2[i*8 + 5]);
        case 5: accu += popcount64 (data1[i*8 + 4] ^ data2[i*8 + 4]);
        case 4: accu += popcount64 (data1[i*8 + 3] ^ data2[i*8 + 3]);
        case 3: accu += popcount64 (data1[i*8 + 2] ^ data2[i*8 + 2]);
        case 2: accu += popcount64 (data1[i*8 + 1] ^ data2[i*8 + 1]);
        case 1: accu += popcount64 (data1[i*8 + 0] ^ data2[i*8 + 0]);
        default: break;
    }
    return accu;
}

int
OR_popcnt(const uint8_t* data1, const uint8_t*data2, const size_t n) {
    const uint64_t *a = (uint64_t *)data1;
    const uint64_t *b = (uint64_t *)data2;
    int accu = 0;
    int i = 0;
    for (; i < n/8; i++)
        accu += popcount64 (a[i] | b[i]);
    switch( n % 8 )
    {
        case 7: accu += popcount64 (data1[i*8 + 6] | data2[i*8 + 6]);
        case 6: accu += popcount64 (data1[i*8 + 5] | data2[i*8 + 5]);
        case 5: accu += popcount64 (data1[i*8 + 4] | data2[i*8 + 4]);
        case 4: accu += popcount64 (data1[i*8 + 3] | data2[i*8 + 3]);
        case 3: accu += popcount64 (data1[i*8 + 2] | data2[i*8 + 2]);
        case 2: accu += popcount64 (data1[i*8 + 1] | data2[i*8 + 1]);
        case 1: accu += popcount64 (data1[i*8 + 0] | data2[i*8 + 0]);
        default: break;
    }
    return accu;
}

int
AND_popcnt(const uint8_t* data1, const uint8_t*data2, const size_t n) {
    const uint64_t *a = (uint64_t *)data1;
    const uint64_t *b = (uint64_t *)data2;
    int accu = 0;
    int i = 0;
    for (; i < n/8; i++)
        accu += popcount64 (a[i] & b[i]);
    switch( n % 8 )
    {
        case 7: accu += popcount64 (data1[i*8 + 6] & data2[i*8 + 6]);
        case 6: accu += popcount64 (data1[i*8 + 5] & data2[i*8 + 5]);
        case 5: accu += popcount64 (data1[i*8 + 4] & data2[i*8 + 4]);
        case 4: accu += popcount64 (data1[i*8 + 3] & data2[i*8 + 3]);
        case 3: accu += popcount64 (data1[i*8 + 2] & data2[i*8 + 2]);
        case 2: accu += popcount64 (data1[i*8 + 1] & data2[i*8 + 1]);
        case 1: accu += popcount64 (data1[i*8 + 0] & data2[i*8 + 0]);
        default: break;
    }
    return accu;
}

float fvec_jaccard (
        const uint8_t* data1,
        const uint8_t* data2,
        const size_t n) {
    int accu_num = AND_popcnt(data1, data2, n);
    int accu_den = OR_popcnt(data1, data2, n);
    return (accu_den == 0) ? 1.0 : (1.0 - (float)(accu_num) / (float)(accu_den));
}


static const size_t size_1M = 1 * 1024 * 1024;
static const size_t batch_size = 65536;


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
        const BitsetView& bitset)
{
    if ((bytes_per_code + sizeof(size_t) + k * sizeof(int64_t)) * n1 < size_1M) {
        int thread_max_num = omp_get_max_threads();

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
        size_t *num = new size_t[n1];
        for (size_t i = 0; i < n1; i++) {
            num[i] = 0;
        }

        const size_t block_size = batch_size;
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
        const BitsetView& bitset) {

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


template <class T2, class Computer>
void binary_distance_knn_hc (
        Computer computer,
        int bytes_per_code,
        T2 * ha,
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n2,
        const BitsetView& bitset =nullptr)
{
    typedef typename T2::T T1;
    size_t k = ha->k;

    if ((bytes_per_code + k * (sizeof(T1) + sizeof(int64_t))) * ha->nh < size_1M) {
        int thread_max_num = omp_get_max_threads();
        // init heap
        size_t thread_heap_size = ha->nh * k;
        size_t all_heap_size = thread_heap_size * thread_max_num;
        T1 *value = new T1[all_heap_size];
        int64_t *labels = new int64_t[all_heap_size];
        T1 init_value = typeid(T1) == typeid(float)? 1.0 / 0.0:0x7fffffff;
        for (int i = 0; i < all_heap_size; i++) {
            value[i] = init_value;
            labels[i] = -1;
        }

#pragma omp parallel for
        for (size_t j = 0; j < n2; j++) {
            if(!bitset || !bitset.test(j)) {
                int thread_no = omp_get_thread_num();

                const uint8_t * bs2_ = bs2 + j * bytes_per_code;
                for (size_t i = 0; i < ha->nh; i++) {
                    T1 dis = computer(bs1 + i * bytes_per_code, bs2_, bytes_per_code);
                    T1 * val_ = value + thread_no * thread_heap_size + i * k;
                    int64_t * ids_ = labels + thread_no * thread_heap_size + i * k;
                    if (  dis < val_[0]) {
                        faiss::maxheap_swap_top<T1> (k, val_, ids_, dis, j);
                    }
                }
            }
        }

        for (size_t t = 1; t < thread_max_num; t++) {
            // merge heap
            for (size_t i = 0; i < ha->nh; i++) {
                T1 * __restrict value_x = value + i * k;
                int64_t * __restrict labels_x = labels + i * k;
                T1 *value_x_t = value_x + t * thread_heap_size;
                int64_t *labels_x_t = labels_x + t * thread_heap_size;
                for (size_t j = 0; j < k; j++) {
                    if (value_x_t[j] < value_x[0]) {
                        faiss::maxheap_swap_top<T1> (k, value_x, labels_x, value_x_t[j], labels_x_t[j]);
                    }
                }
            }
        }

        // copy result
        memcpy(ha->val, value, thread_heap_size * sizeof(T1));
        memcpy(ha->ids, labels, thread_heap_size * sizeof(int64_t));

        delete[] value;
        delete[] labels;

    } else {
        ha->heapify ();

        const size_t block_size = batch_size;
        for (size_t j0 = 0; j0 < n2; j0 += block_size) {
            const size_t j1 = std::min(j0 + block_size, n2);
#pragma omp parallel for
            for (size_t i = 0; i < ha->nh; i++) {
                const uint8_t *bs2_ = bs2 + j0 * bytes_per_code;
                T1 dis;
                T1 *__restrict bh_val_ = ha->val + i * k;
                int64_t *__restrict bh_ids_ = ha->ids + i * k;
                size_t j;
                for (j = j0; j < j1; j++, bs2_ += bytes_per_code) {
                    if (!bitset || !bitset.test(j)) {
                        dis = computer(bs1 + i * bytes_per_code, bs2_, bytes_per_code);
                        if (dis < bh_val_[0]) {
                            faiss::maxheap_swap_top<T1>(k, bh_val_, bh_ids_, dis, j);
                        }
                    }
                }

            }
        }
    }
    ha->reorder ();
}

template <class T1>
void binary_distance_knn_hc (
        MetricType metric_type,
        T1 * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        const BitsetView& bitset)
{
    size_t dim = ncodes * 8;
    switch (metric_type) {
        case METRIC_Jaccard:
        case METRIC_Tanimoto: {
            float (*jaccard)(const uint8_t *a, const uint8_t *b, size_t n) = nullptr;
            if (support_avx512() && dim > 1024) {
                jaccard = jaccard__AVX512;
            } else if (support_avx2() && dim > 512) {
                jaccard = jaccard__AVX2;
            } else {
                jaccard = fvec_jaccard;
            }
            binary_distance_knn_hc(jaccard, ncodes, ha, a, b, nb, bitset);
            break;
        }
        case METRIC_Hamming: {
            int (*hamming)(const uint8_t * a, const uint8_t * b, size_t n) = nullptr;
            if (support_avx512() && dim > 1024) {
                hamming = XOR_popcnt_AVX512VBMI_lookup;
            } else if (support_avx2() && dim > 512) {
                hamming = XOR_popcnt_AVX2_lookup;
            } else {
                hamming = XOR_popcnt;
            }
            binary_distance_knn_hc(hamming, ncodes, ha, a, b, nb, bitset);
            break;
        }
        default:
            break;
    }
}

template
void binary_distance_knn_hc<int_maxheap_array_t>(
        MetricType metric_type,
        int_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        const BitsetView& bitset);

template
void binary_distance_knn_hc<float_maxheap_array_t>(
        MetricType metric_type,
        float_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        const BitsetView& bitset);


} // namespace faiss
