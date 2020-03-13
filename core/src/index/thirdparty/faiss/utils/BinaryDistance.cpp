#include <faiss/utils/BinaryDistance.h>

#include <vector>
#include <memory>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <limits.h>
#include <omp.h>

#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/utils.h>

namespace faiss {

    size_t batch_size = 65536;

    template <class T>
    static
    void binary_distence_knn_hc(
            int bytes_per_code,
            float_maxheap_array_t * ha,
            const uint8_t * bs1,
            const uint8_t * bs2,
            size_t n2,
            bool order = true,
            bool init_heap = true,
            ConcurrentBitsetPtr bitset = nullptr)
    {
        size_t k = ha->k;
        if (init_heap) ha->heapify ();

        int thread_max_num = omp_get_max_threads();
        if (ha->nh < thread_max_num) {
            // omp for n2
            int all_hash_size = thread_max_num * k;
            float *value = new float[all_hash_size];
            int64_t *labels = new int64_t[all_hash_size];

            for (int i = 0; i < ha->nh; i++) {
                T hc (bs1 + i * bytes_per_code, bytes_per_code);
                // init hash
                for (int i = 0; i < all_hash_size; i++) {
                    value[i] = 1.0 / 0.0;
                }
#pragma omp parallel for
                for (size_t j = 0; j < n2; j++) {
                    if(!bitset || !bitset->test(j)) {
                        const uint8_t * bs2_ = bs2 + j * bytes_per_code;
                        tadis_t dis = hc.compute (bs2_);

                        int thread_no = omp_get_thread_num();
                        float * __restrict val_ = value + thread_no * k;
                        int64_t * __restrict ids_ = labels + thread_no * k;
                        if (dis < val_[0]) {
                            faiss::maxheap_pop<tadis_t> (k, val_, ids_);
                            faiss::maxheap_push<tadis_t> (k, val_, ids_, dis, j);
                        }
                    }
                }
                // merge hash
                tadis_t * __restrict bh_val_ = ha->val + i * k;
                int64_t * __restrict bh_ids_ = ha->ids + i * k;
                for (int i = 0; i < all_hash_size; i++) {
                    if (value[i] < bh_val_[0]) {
                        faiss::maxheap_pop<tadis_t> (k, bh_val_, bh_ids_);
                        faiss::maxheap_push<tadis_t> (k, bh_val_, bh_ids_, value[i], labels[i]);
                    }
                }
            }
            delete[] value;
            delete[] labels;

        } else {
            const size_t block_size = batch_size;
            for (size_t j0 = 0; j0 < n2; j0 += block_size) {
                const size_t j1 = std::min(j0 + block_size, n2);
#pragma omp parallel for
                for (size_t i = 0; i < ha->nh; i++) {
                    T hc (bs1 + i * bytes_per_code, bytes_per_code);

                    const uint8_t * bs2_ = bs2 + j0 * bytes_per_code;
                    tadis_t dis;
                    tadis_t * __restrict bh_val_ = ha->val + i * k;
                    int64_t * __restrict bh_ids_ = ha->ids + i * k;
                    size_t j;
                    for (j = j0; j < j1; j++, bs2_+= bytes_per_code) {
                        if(!bitset || !bitset->test(j)){
                            dis = hc.compute (bs2_);
                            if (dis < bh_val_[0]) {
                                faiss::maxheap_pop<tadis_t> (k, bh_val_, bh_ids_);
                                faiss::maxheap_push<tadis_t> (k, bh_val_, bh_ids_, dis, j);
                            }
                        }
                    }

                }
            }
        }

        if (order) ha->reorder ();
    }

    void binary_distence_knn_hc (
            MetricType metric_type,
            float_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            int order,
            ConcurrentBitsetPtr bitset)
    {
        switch (metric_type) {
        case METRIC_Jaccard:
        case METRIC_Tanimoto:
            switch (ncodes) {
#define binary_distence_knn_hc_jaccard(ncodes) \
            case ncodes: \
                binary_distence_knn_hc<faiss::JaccardComputer ## ncodes> \
                    (ncodes, ha, a, b, nb, order, true, bitset); \
            break;
            binary_distence_knn_hc_jaccard(8);
            binary_distence_knn_hc_jaccard(16);
            binary_distence_knn_hc_jaccard(32);
            binary_distence_knn_hc_jaccard(64);
            binary_distence_knn_hc_jaccard(128);
            binary_distence_knn_hc_jaccard(256);
            binary_distence_knn_hc_jaccard(512);
#undef binary_distence_knn_hc_jaccard
            default:
                binary_distence_knn_hc<faiss::JaccardComputerDefault>
                        (ncodes, ha, a, b, nb, order, true, bitset);
                break;
            }
            break;

        case METRIC_Substructure:
            switch (ncodes) {
#define binary_distence_knn_hc_Substructure(ncodes) \
            case ncodes: \
                binary_distence_knn_hc<faiss::SubstructureComputer ## ncodes> \
                    (ncodes, ha, a, b, nb, order, true, bitset); \
            break;
            binary_distence_knn_hc_Substructure(8);
            binary_distence_knn_hc_Substructure(16);
            binary_distence_knn_hc_Substructure(32);
            binary_distence_knn_hc_Substructure(64);
            binary_distence_knn_hc_Substructure(128);
            binary_distence_knn_hc_Substructure(256);
            binary_distence_knn_hc_Substructure(512);
#undef binary_distence_knn_hc_Substructure
            default:
                binary_distence_knn_hc<faiss::SubstructureComputerDefault>
                        (ncodes, ha, a, b, nb, order, true, bitset);
                break;
            }
            break;

        case METRIC_Superstructure:
            switch (ncodes) {
#define binary_distence_knn_hc_Superstructure(ncodes) \
            case ncodes: \
                binary_distence_knn_hc<faiss::SuperstructureComputer ## ncodes> \
                    (ncodes, ha, a, b, nb, order, true, bitset); \
            break;
            binary_distence_knn_hc_Superstructure(8);
            binary_distence_knn_hc_Superstructure(16);
            binary_distence_knn_hc_Superstructure(32);
            binary_distence_knn_hc_Superstructure(64);
            binary_distence_knn_hc_Superstructure(128);
            binary_distence_knn_hc_Superstructure(256);
            binary_distence_knn_hc_Superstructure(512);
#undef binary_distence_knn_hc_Superstructure
            default:
                binary_distence_knn_hc<faiss::SuperstructureComputerDefault>
                        (ncodes, ha, a, b, nb, order, true, bitset);
                break;
            }
            break;

        default:
            break;
        }
    }

}
