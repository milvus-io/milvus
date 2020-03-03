#include <faiss/utils/jaccard.h>

#include <vector>
#include <memory>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <limits.h>

#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/utils.h>

namespace faiss {

    size_t jaccard_batch_size = 65536;

    template <class JaccardComputer>
    static
    void jaccard_knn_hc(
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

        const size_t block_size = jaccard_batch_size;
        for (size_t j0 = 0; j0 < n2; j0 += block_size) {
            const size_t j1 = std::min(j0 + block_size, n2);
#pragma omp parallel for
            for (size_t i = 0; i < ha->nh; i++) {
                JaccardComputer hc (bs1 + i * bytes_per_code, bytes_per_code);

                const uint8_t * bs2_ = bs2 + j0 * bytes_per_code;
                tadis_t dis;
                tadis_t * __restrict bh_val_ = ha->val + i * k;
                int64_t * __restrict bh_ids_ = ha->ids + i * k;
                size_t j;
                for (j = j0; j < j1; j++, bs2_+= bytes_per_code) {
                    if(!bitset || !bitset->test(j)){
                        dis = hc.jaccard (bs2_);
                        if (dis < bh_val_[0]) {
                            faiss::maxheap_pop<tadis_t> (k, bh_val_, bh_ids_);
                            faiss::maxheap_push<tadis_t> (k, bh_val_, bh_ids_, dis, j);
                        }
                    }
                }

            }
        }
        if (order) ha->reorder ();
    }

    void jaccard_knn_hc (
            float_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            int order,
            ConcurrentBitsetPtr bitset)
    {
        switch (ncodes) {
            case 16:
                jaccard_knn_hc<faiss::JaccardComputer16>
                        (16, ha, a, b, nb, order, true, bitset);
                break;
            case 32:
                jaccard_knn_hc<faiss::JaccardComputer32>
                        (32, ha, a, b, nb, order, true, bitset);
                break;
            case 64:
                jaccard_knn_hc<faiss::JaccardComputer64>
                        (64, ha, a, b, nb, order, true, bitset);
                break;
            case 128:
                jaccard_knn_hc<faiss::JaccardComputer128>
                        (128, ha, a, b, nb, order, true, bitset);
                break;
            default:
                jaccard_knn_hc<faiss::JaccardComputerDefault>
                        (ncodes, ha, a, b, nb, order, true, bitset);
        }
    }

}
