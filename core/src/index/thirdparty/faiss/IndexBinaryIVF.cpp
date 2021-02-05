/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved
// -*- c++ -*-

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexBinaryIVF.h>

#include <cmath>
#include <cstdio>
#include <omp.h>

#include <memory>

#include <faiss/utils/BinaryDistance.h>
#include <faiss/utils/hamming.h>
#include <faiss/utils/jaccard-inl.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexLSH.h>


namespace faiss {

IndexBinaryIVF::IndexBinaryIVF(IndexBinary *quantizer, size_t d, size_t nlist)
    : IndexBinary(d),
      invlists(new ArrayInvertedLists(nlist, code_size)),
      own_invlists(true),
      nprobe(1),
      max_codes(0),
      quantizer(quantizer),
      nlist(nlist),
      own_fields(false),
      clustering_index(nullptr)
{
  FAISS_THROW_IF_NOT (d == quantizer->d);
  is_trained = quantizer->is_trained && (quantizer->ntotal == nlist);

  cp.niter = 10;
  if(STATISTICS_LEVEL >= 3) {
      nprobe_statistics.resize(nlist, 0);
  }
}

IndexBinaryIVF::IndexBinaryIVF(IndexBinary *quantizer, size_t d, size_t nlist, MetricType metric)
    : IndexBinary(d, metric),
      invlists(new ArrayInvertedLists(nlist, code_size)),
      own_invlists(true),
      nprobe(1),
      max_codes(0),
      quantizer(quantizer),
      nlist(nlist),
      own_fields(false),
      clustering_index(nullptr)
{
  FAISS_THROW_IF_NOT (d == quantizer->d);
  is_trained = quantizer->is_trained && (quantizer->ntotal == nlist);

  cp.niter = 10;
  if(STATISTICS_LEVEL >= 3) {
      nprobe_statistics.resize(nlist, 0);
  }
}

IndexBinaryIVF::IndexBinaryIVF()
    : invlists(nullptr),
      own_invlists(false),
      nprobe(1),
      max_codes(0),
      quantizer(nullptr),
      nlist(0),
      own_fields(false),
      clustering_index(nullptr)
{}

void IndexBinaryIVF::add(idx_t n, const uint8_t *x) {
  add_with_ids(n, x, nullptr);
}

void IndexBinaryIVF::add_with_ids(idx_t n, const uint8_t *x, const idx_t *xids) {
  add_core(n, x, xids, nullptr);
}

void IndexBinaryIVF::add_core(idx_t n, const uint8_t *x, const idx_t *xids,
                              const idx_t *precomputed_idx) {
  FAISS_THROW_IF_NOT(is_trained);
  assert(invlists);
  direct_map.check_can_add (xids);

  const idx_t * idx;

  std::unique_ptr<idx_t[]> scoped_idx;

  if (precomputed_idx) {
    idx = precomputed_idx;
  } else {
    scoped_idx.reset(new idx_t[n]);
    quantizer->assign(n, x, scoped_idx.get());
    idx = scoped_idx.get();
  }

  long n_add = 0;
  for (size_t i = 0; i < n; i++) {
    idx_t id = xids ? xids[i] : ntotal + i;
    idx_t list_no = idx[i];

    if (list_no < 0) {
        direct_map.add_single_id (id, -1, 0);
    } else {
        const uint8_t *xi = x + i * code_size;
        size_t offset = invlists->add_entry(list_no, id, xi);

        direct_map.add_single_id (id, list_no, offset);
    }

    n_add++;
  }
  if (verbose) {
    printf("IndexBinaryIVF::add_with_ids: added %ld / %ld vectors\n",
           n_add, n);
  }
  ntotal += n_add;
}

void IndexBinaryIVF::make_direct_map (bool b)
{
    if (b) {
        direct_map.set_type (DirectMap::Array, invlists, ntotal);
    } else {
        direct_map.set_type (DirectMap::NoMap, invlists, ntotal);
    }
}

void IndexBinaryIVF::set_direct_map_type (DirectMap::Type type)
{
    direct_map.set_type (type, invlists, ntotal);
}


void IndexBinaryIVF::search(idx_t n, const uint8_t *x, idx_t k,
                            int32_t *distances, idx_t *labels,
                            const BitsetView bitset) const {
  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<int32_t[]> coarse_dis(new int32_t[n * nprobe]);

  double t0 = getmillisecs();
  quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());

  index_ivf_stats.quantization_time += getmillisecs() - t0;
  if (STATISTICS_LEVEL >= 3) {
      int64_t size = n * nprobe;
      for (int64_t i = 0; i < size; i++) {
          nprobe_statistics[idx[i]]++;
      }
  }

  t0 = getmillisecs();
  invlists->prefetch_lists(idx.get(), n * nprobe);

  search_preassigned(n, x, k, idx.get(), coarse_dis.get(),
                     distances, labels, false, nullptr, bitset);

  index_ivf_stats.search_time += getmillisecs() - t0;
}

#if 0
void IndexBinaryIVF::get_vector_by_id(idx_t n, const idx_t *xid, uint8_t *x, const BitsetView bitset) {
    make_direct_map(true);

    /* only get vector by 1 id */
    FAISS_ASSERT(n == 1);
    if (!bitset || !bitset.test(xid[0])) {
        reconstruct(xid[0], x + 0 * d);
    } else {
        memset(x, UINT8_MAX, d * sizeof(uint8_t));
    }
}

void IndexBinaryIVF::search_by_id (idx_t n, const idx_t *xid, idx_t k, int32_t *distances, idx_t *labels,
                                   const BitsetView bitset) {
    make_direct_map(true);

    auto x = new uint8_t[n * d];
    for (idx_t i = 0; i < n; ++i) {
        reconstruct(xid[i], x + i * d);
    }

    search(n, x, k, distances, labels, bitset);
    delete []x;
}
#endif

void IndexBinaryIVF::reconstruct(idx_t key, uint8_t *recons) const {
    idx_t lo = direct_map.get (key);
    reconstruct_from_offset (lo_listno(lo), lo_offset(lo), recons);
}

void IndexBinaryIVF::reconstruct_n(idx_t i0, idx_t ni, uint8_t *recons) const {
  FAISS_THROW_IF_NOT(ni == 0 || (i0 >= 0 && i0 + ni <= ntotal));

  for (idx_t list_no = 0; list_no < nlist; list_no++) {
    size_t list_size = invlists->list_size(list_no);
    const Index::idx_t *idlist = invlists->get_ids(list_no);

    for (idx_t offset = 0; offset < list_size; offset++) {
      idx_t id = idlist[offset];
      if (!(id >= i0 && id < i0 + ni)) {
        continue;
      }

      uint8_t *reconstructed = recons + (id - i0) * d;
      reconstruct_from_offset(list_no, offset, reconstructed);
    }
  }
}

void IndexBinaryIVF::search_and_reconstruct(idx_t n, const uint8_t *x, idx_t k,
                                            int32_t *distances, idx_t *labels,
                                            uint8_t *recons) const {
  std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
  std::unique_ptr<int32_t[]> coarse_dis(new int32_t[n * nprobe]);

  quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());

  invlists->prefetch_lists(idx.get(), n * nprobe);

  // search_preassigned() with `store_pairs` enabled to obtain the list_no
  // and offset into `codes` for reconstruction
  search_preassigned(n, x, k, idx.get(), coarse_dis.get(),
                     distances, labels, /* store_pairs */true);
  for (idx_t i = 0; i < n; ++i) {
    for (idx_t j = 0; j < k; ++j) {
      idx_t ij = i * k + j;
      idx_t key = labels[ij];
      uint8_t *reconstructed = recons + ij * d;
      if (key < 0) {
        // Fill with NaNs
        memset(reconstructed, -1, sizeof(*reconstructed) * d);
      } else {
        int list_no = key >> 32;
        int offset = key & 0xffffffff;

        // Update label to the actual id
        labels[ij] = invlists->get_single_id(list_no, offset);

        reconstruct_from_offset(list_no, offset, reconstructed);
      }
    }
  }
}

void IndexBinaryIVF::reconstruct_from_offset(idx_t list_no, idx_t offset,
                                             uint8_t *recons) const {
  memcpy(recons, invlists->get_single_code(list_no, offset), code_size);
}

void IndexBinaryIVF::reset() {
  direct_map.clear();
  invlists->reset();
  ntotal = 0;
}

size_t IndexBinaryIVF::remove_ids(const IDSelector& sel) {
    size_t nremove = direct_map.remove_ids (sel, invlists);
    ntotal -= nremove;
    return nremove;
}

void IndexBinaryIVF::train(idx_t n, const uint8_t *x) {
  if (verbose) {
    printf("Training quantizer\n");
  }

  if (quantizer->is_trained && (quantizer->ntotal == nlist)) {
    if (verbose) {
      printf("IVF quantizer does not need training.\n");
    }
  } else {
    if (verbose) {
      printf("Training quantizer on %ld vectors in %dD\n", n, d);
    }

    Clustering clus(d, nlist, cp);
    quantizer->reset();

    IndexFlat index_tmp;

    if (metric_type == METRIC_Jaccard || metric_type == METRIC_Tanimoto) {
        index_tmp = IndexFlat(d, METRIC_Jaccard);
    } else if (metric_type == METRIC_Substructure || metric_type == METRIC_Superstructure) {
        // unsupported
        FAISS_THROW_MSG("IVF not to support Substructure and Superstructure.");
    } else {
        index_tmp = IndexFlat(d, METRIC_L2);
    }

    if (clustering_index && verbose) {
      printf("using clustering_index of dimension %d to do the clustering\n",
             clustering_index->d);
    }

    // LSH codec that is able to convert the binary vectors to floats.
    IndexLSH codec(d, d, false, false);

    clus.train_encoded (n, x, &codec, clustering_index ? *clustering_index : index_tmp);

    // convert clusters to binary
    std::unique_ptr<uint8_t[]> x_b(new uint8_t[clus.k * code_size]);
    real_to_binary(d * clus.k, clus.centroids.data(), x_b.get());

    quantizer->add(clus.k, x_b.get());
    quantizer->is_trained = true;
  }

  is_trained = true;
}

void IndexBinaryIVF::merge_from(IndexBinaryIVF &other, idx_t add_id) {
  // minimal sanity checks
  FAISS_THROW_IF_NOT(other.d == d);
  FAISS_THROW_IF_NOT(other.nlist == nlist);
  FAISS_THROW_IF_NOT(other.code_size == code_size);
  FAISS_THROW_IF_NOT_MSG(direct_map.no() && other.direct_map.no(),
                         "direct map copy not implemented");
  FAISS_THROW_IF_NOT_MSG(typeid (*this) == typeid (other),
                         "can only merge indexes of the same type");

  invlists->merge_from (other.invlists, add_id);

  ntotal += other.ntotal;
  other.ntotal = 0;
}

void IndexBinaryIVF::replace_invlists(InvertedLists *il, bool own) {
  FAISS_THROW_IF_NOT(il->nlist == nlist &&
                     il->code_size == code_size);
  if (own_invlists) {
    delete invlists;
  }
  invlists = il;
  own_invlists = own;
}


namespace {

using idx_t = Index::idx_t;


template<class HammingComputer>
struct IVFBinaryScannerL2: BinaryInvertedListScanner {

    HammingComputer hc;
    size_t code_size;
    bool store_pairs;

    IVFBinaryScannerL2 (size_t code_size, bool store_pairs):
        code_size (code_size), store_pairs(store_pairs)
    {}

    void set_query (const uint8_t *query_vector) override {
        hc.set (query_vector, code_size);
    }

    idx_t list_no;
    void set_list (idx_t list_no, uint8_t /* coarse_dis */) override {
        this->list_no = list_no;
    }

    uint32_t distance_to_code (const uint8_t *code) const override {
        return hc.compute (code);
    }

    size_t scan_codes (size_t n,
                       const uint8_t *codes,
                       const idx_t *ids,
                       int32_t *simi, idx_t *idxi,
                       size_t k,
                       const BitsetView bitset) const override
    {
        using C = CMax<int32_t, idx_t>;

        size_t nup = 0;
        for (size_t j = 0; j < n; j++) {
            if (!bitset || !bitset.test(ids[j])) {
                uint32_t dis = hc.compute (codes);
                if (dis < simi[0]) {
                    idx_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                    heap_swap_top<C> (k, simi, idxi, dis, id);
                    nup++;
                }
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t n,
                           const uint8_t *codes,
                           const idx_t *ids,
                           int radius,
                           RangeQueryResult &result) const
    {
        size_t nup = 0;
        for (size_t j = 0; j < n; j++) {
            uint32_t dis = hc.compute (codes);
            if (dis < radius) {
                int64_t id = store_pairs ? lo_build (list_no, j) : ids[j];
                result.add (dis, id);
            }
            codes += code_size;
        }
    }
};

template<class DistanceComputer, bool store_pairs>
struct IVFBinaryScannerJaccard: BinaryInvertedListScanner {
    DistanceComputer hc;
    size_t code_size;

    IVFBinaryScannerJaccard (size_t code_size): code_size (code_size)
    {}

    void set_query (const uint8_t *query_vector) override {
        hc.set (query_vector, code_size);
    }

    idx_t list_no;
    void set_list (idx_t list_no, uint8_t /* coarse_dis */) override {
        this->list_no = list_no;
    }

    uint32_t distance_to_code (const uint8_t *code) const override {
        return 0;
    }

    size_t scan_codes (size_t n,
                       const uint8_t *codes,
                       const idx_t *ids,
                       int32_t *simi, idx_t *idxi,
                       size_t k,
                       const BitsetView bitset = nullptr) const override
    {
        using C = CMax<float, idx_t>;
        float* psimi = (float*)simi;
        size_t nup = 0;
        for (size_t j = 0; j < n; j++) {
            if(!bitset || !bitset.test(ids[j])){
                float dis = hc.compute (codes);

                if (dis < psimi[0]) {
                    idx_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                    heap_swap_top<C> (k, psimi, idxi, dis, id);
                    nup++;
                }
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t n,
                           const uint8_t *codes,
                           const idx_t *ids,
                           int radius,
                           RangeQueryResult &result) const override {
        // not yet
    }
};

template <bool store_pairs>
BinaryInvertedListScanner *select_IVFBinaryScannerL2 (size_t code_size) {
#define HC(name) return new IVFBinaryScannerL2<name> (code_size, store_pairs)
    switch (code_size) {
        case 4: HC(HammingComputer4);
        case 8: HC(HammingComputer8);
        case 16: HC(HammingComputer16);
        case 20: HC(HammingComputer20);
        case 32: HC(HammingComputer32);
        case 64: HC(HammingComputer64);
        default: HC(HammingComputerDefault);
    }
#undef HC
}

template <bool store_pairs>
BinaryInvertedListScanner *select_IVFBinaryScannerJaccard (size_t code_size) {
    switch (code_size) {
#define HANDLE_CS(cs)                                                  \
    case cs:                                                            \
        return new IVFBinaryScannerJaccard<JaccardComputer ## cs, store_pairs> (cs);
     HANDLE_CS(16)
     HANDLE_CS(32)
     HANDLE_CS(64)
     HANDLE_CS(128)
     HANDLE_CS(256)
     HANDLE_CS(512)
#undef HANDLE_CS
    default:
        return new IVFBinaryScannerJaccard<JaccardComputerDefault,
            store_pairs>(code_size);
    }
}

void search_knn_hamming_heap(const IndexBinaryIVF& ivf,
                             size_t n,
                             const uint8_t *x,
                             idx_t k,
                             const idx_t *keys,
                             const int32_t * coarse_dis,
                             int32_t *distances, idx_t *labels,
                             bool store_pairs,
                             const IVFSearchParameters *params,
                             IndexIVFStats &index_ivf_stats,
                             const BitsetView bitset = nullptr)
{
    long nprobe = params ? params->nprobe : ivf.nprobe;
    long max_codes = params ? params->max_codes : ivf.max_codes;
    MetricType metric_type = ivf.metric_type;

    // almost verbatim copy from IndexIVF::search_preassigned

    size_t nlistv = 0, ndis = 0, nheap = 0;
    using HeapForIP = CMin<int32_t, idx_t>;
    using HeapForL2 = CMax<int32_t, idx_t>;

#pragma omp parallel if(n > 1) reduction(+: nlistv, ndis, nheap)
    {
        std::unique_ptr<BinaryInvertedListScanner> scanner
            (ivf.get_InvertedListScanner (store_pairs));

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            const uint8_t *xi = x + i * ivf.code_size;
            scanner->set_query(xi);

            const idx_t * keysi = keys + i * nprobe;
            int32_t * simi = distances + k * i;
            idx_t * idxi = labels + k * i;

            if (metric_type == METRIC_INNER_PRODUCT) {
                heap_heapify<HeapForIP> (k, simi, idxi);
            } else {
                heap_heapify<HeapForL2> (k, simi, idxi);
            }

            size_t nscan = 0;

            for (size_t ik = 0; ik < nprobe; ik++) {
                idx_t key = keysi[ik];  /* select the list  */
                if (key < 0) {
                    // not enough centroids for multiprobe
                    continue;
                }
                FAISS_THROW_IF_NOT_FMT
                    (key < (idx_t) ivf.nlist,
                     "Invalid key=%ld  at ik=%ld nlist=%ld\n",
                     key, ik, ivf.nlist);

                scanner->set_list (key, coarse_dis[i * nprobe + ik]);

                nlistv++;

                size_t list_size = ivf.invlists->list_size(key);
                InvertedLists::ScopedCodes scodes (ivf.invlists, key);
                std::unique_ptr<InvertedLists::ScopedIds> sids;
                const Index::idx_t * ids = nullptr;

                if (!store_pairs) {
                    sids.reset (new InvertedLists::ScopedIds (ivf.invlists, key));
                    ids = sids->get();
                }

                nheap += scanner->scan_codes (list_size, scodes.get(),
                                              ids, simi, idxi, k, bitset);

                nscan += list_size;
                if (max_codes && nscan >= max_codes)
                    break;
            }

            ndis += nscan;
            if (metric_type == METRIC_INNER_PRODUCT) {
                heap_reorder<HeapForIP> (k, simi, idxi);
            } else {
                heap_reorder<HeapForL2> (k, simi, idxi);
            }

        } // parallel for
    } // parallel

    if(STATISTICS_LEVEL >= 1) {
        index_ivf_stats.nq += n;
        index_ivf_stats.nlist += nlistv;
        index_ivf_stats.ndis += ndis;
        index_ivf_stats.nheap_updates += nheap;
    }

}

void search_knn_binary_dis_heap(const IndexBinaryIVF& ivf,
                                size_t n,
                                const uint8_t *x,
                                idx_t k,
                                const idx_t *keys,
                                const float * coarse_dis,
                                float *distances,
                                idx_t *labels,
                                bool store_pairs,
                                const IVFSearchParameters *params,
                                IndexIVFStats &index_ivf_stats,
                                const BitsetView bitset = nullptr)
{
    long nprobe = params ? params->nprobe : ivf.nprobe;
    long max_codes = params ? params->max_codes : ivf.max_codes;
    MetricType metric_type = ivf.metric_type;

    // almost verbatim copy from IndexIVF::search_preassigned

    size_t nlistv = 0, ndis = 0, nheap = 0;
    using HeapForJaccard = CMax<float, idx_t>;

#pragma omp parallel if(n > 1) reduction(+: nlistv, ndis, nheap)
    {
        std::unique_ptr<BinaryInvertedListScanner> scanner
            (ivf.get_InvertedListScanner(store_pairs));

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            const uint8_t *xi = x + i * ivf.code_size;
            scanner->set_query(xi);

            const idx_t * keysi = keys + i * nprobe;
            float * simi = distances + k * i;
            idx_t * idxi = labels + k * i;

            heap_heapify<HeapForJaccard> (k, simi, idxi);

            size_t nscan = 0;

            for (size_t ik = 0; ik < nprobe; ik++) {
                idx_t key = keysi[ik];  /* select the list  */
                if (key < 0) {
                    // not enough centroids for multiprobe
                    continue;
                }
                FAISS_THROW_IF_NOT_FMT
                (key < (idx_t) ivf.nlist,
                 "Invalid key=%ld  at ik=%ld nlist=%ld\n",
                 key, ik, ivf.nlist);

                scanner->set_list (key, (int32_t)coarse_dis[i * nprobe + ik]);

                nlistv++;

                size_t list_size = ivf.invlists->list_size(key);
                InvertedLists::ScopedCodes scodes (ivf.invlists, key);
                std::unique_ptr<InvertedLists::ScopedIds> sids;
                const Index::idx_t * ids = nullptr;

                if (!store_pairs) {
                    sids.reset (new InvertedLists::ScopedIds (ivf.invlists, key));
                    ids = sids->get();
                }

                nheap += scanner->scan_codes (list_size, scodes.get(),
                                              ids, (int32_t*)simi, idxi, k, bitset);

                nscan += list_size;
                if (max_codes && nscan >= max_codes)
                    break;
            }

            ndis += nscan;
            heap_reorder<HeapForJaccard> (k, simi, idxi);

        } // parallel for
    } // parallel

    if(STATISTICS_LEVEL >= 1) {
        index_ivf_stats.nq += n;
        index_ivf_stats.nlist += nlistv;
        index_ivf_stats.ndis += ndis;
        index_ivf_stats.nheap_updates += nheap;
    }

}

template<class HammingComputer, bool store_pairs>
void search_knn_hamming_count(const IndexBinaryIVF& ivf,
                              size_t nx,
                              const uint8_t *x,
                              const idx_t *keys,
                              int k,
                              int32_t *distances,
                              idx_t *labels,
                              const IVFSearchParameters *params,
                              IndexIVFStats &index_ivf_stats,
                              const BitsetView bitset = nullptr) {
  const int nBuckets = ivf.d + 1;
  std::vector<int> all_counters(nx * nBuckets, 0);
  std::unique_ptr<idx_t[]> all_ids_per_dis(new idx_t[nx * nBuckets * k]);

  long nprobe = params ? params->nprobe : ivf.nprobe;
  long max_codes = params ? params->max_codes : ivf.max_codes;

  std::vector<HCounterState<HammingComputer>> cs;
  for (size_t i = 0; i < nx; ++i) {
    cs.push_back(HCounterState<HammingComputer>(
                   all_counters.data() + i * nBuckets,
                   all_ids_per_dis.get() + i * nBuckets * k,
                   x + i * ivf.code_size,
                   ivf.d,
                   k
                 ));
  }

  size_t nlistv = 0, ndis = 0;

#pragma omp parallel for reduction(+: nlistv, ndis)
  for (size_t i = 0; i < nx; i++) {
    const idx_t * keysi = keys + i * nprobe;
    HCounterState<HammingComputer>& csi = cs[i];

    size_t nscan = 0;

    for (size_t ik = 0; ik < nprobe; ik++) {
      idx_t key = keysi[ik];  /* select the list  */
      if (key < 0) {
        // not enough centroids for multiprobe
        continue;
      }
      FAISS_THROW_IF_NOT_FMT (
        key < (idx_t) ivf.nlist,
        "Invalid key=%ld  at ik=%ld nlist=%ld\n",
        key, ik, ivf.nlist);

      nlistv++;
      size_t list_size = ivf.invlists->list_size(key);
      InvertedLists::ScopedCodes scodes (ivf.invlists, key);
      const uint8_t *list_vecs = scodes.get();
      const Index::idx_t *ids = store_pairs
        ? nullptr
        : ivf.invlists->get_ids(key);

      for (size_t j = 0; j < list_size; j++) {
        if (!bitset || !bitset.test(ids[j])) {
          const uint8_t *yj = list_vecs + ivf.code_size * j;
          idx_t id = store_pairs ? (key << 32 | j) : ids[j];
          csi.update_counter(yj, id);
        }
      }
      if (ids)
          ivf.invlists->release_ids (key, ids);

      nscan += list_size;
      if (max_codes && nscan >= max_codes)
        break;
    }
    ndis += nscan;

    int nres = 0;
    for (int b = 0; b < nBuckets && nres < k; b++) {
      for (int l = 0; l < csi.counters[b] && nres < k; l++) {
        labels[i * k + nres] = csi.ids_per_dis[b * k + l];
        distances[i * k + nres] = b;
        nres++;
      }
    }
    while (nres < k) {
      labels[i * k + nres] = -1;
      distances[i * k + nres] = std::numeric_limits<int32_t>::max();
      ++nres;
    }
  }

  if(STATISTICS_LEVEL >= 1) {
      index_ivf_stats.nq += nx;
      index_ivf_stats.nlist += nlistv;
      index_ivf_stats.ndis += ndis;
  }
}



template<bool store_pairs>
void search_knn_hamming_count_1 (
                        const IndexBinaryIVF& ivf,
                        size_t nx,
                        const uint8_t *x,
                        const idx_t *keys,
                        int k,
                        int32_t *distances,
                        idx_t *labels,
                        const IVFSearchParameters *params,
                        IndexIVFStats &index_ivf_stats,
                        const BitsetView bitset = nullptr) {
    switch (ivf.code_size) {
#define HANDLE_CS(cs)                                                                  \
    case cs:                                                                           \
       search_knn_hamming_count<HammingComputer ## cs, store_pairs>(                   \
           ivf, nx, x, keys, k, distances, labels, params, index_ivf_stats, bitset); \
      break;
      HANDLE_CS(4);
      HANDLE_CS(8);
      HANDLE_CS(16);
      HANDLE_CS(20);
      HANDLE_CS(32);
      HANDLE_CS(64);
#undef HANDLE_CS
    default:
        search_knn_hamming_count<HammingComputerDefault, store_pairs>
            (ivf, nx, x, keys, k, distances, labels, params, index_ivf_stats, bitset);
        break;
    }
}

}  // namespace

BinaryInvertedListScanner *IndexBinaryIVF::get_InvertedListScanner
      (bool store_pairs) const
{
    switch (metric_type) {
        case METRIC_Jaccard:
        case METRIC_Tanimoto:
            if (store_pairs) {
                return select_IVFBinaryScannerJaccard<true> (code_size);
            } else {
                return select_IVFBinaryScannerJaccard<false> (code_size);
            }
        case METRIC_Substructure:
        case METRIC_Superstructure:
            // unsupported
            return nullptr;
        default:
            if (store_pairs) {
                return select_IVFBinaryScannerL2<true>(code_size);
            } else {
                return select_IVFBinaryScannerL2<false>(code_size);
            }
    }
}

void IndexBinaryIVF::search_preassigned(idx_t n, const uint8_t *x, idx_t k,
                                        const idx_t *idx,
                                        const int32_t * coarse_dis,
                                        int32_t *distances, idx_t *labels,
                                        bool store_pairs,
                                        const IVFSearchParameters *params,
                                        const BitsetView bitset
                                        ) const {
    if (metric_type == METRIC_Jaccard || metric_type == METRIC_Tanimoto) {
        if (use_heap) {
            float *D = new float[k * n];
            float *c_dis = new float [n * nprobe];
            memcpy(c_dis, coarse_dis, sizeof(float) * n * nprobe);
            search_knn_binary_dis_heap(*this, n, x, k, idx, c_dis ,
                                       D, labels, store_pairs,
                                       params, index_ivf_stats, bitset);
            if (metric_type == METRIC_Tanimoto) {
                for (int i = 0; i < k * n; i++) {
                    D[i] = Jaccard_2_Tanimoto(D[i]);
                }
            }
            memcpy(distances, D, sizeof(float) * n * k);
            delete [] D;
            delete [] c_dis;
        } else {
            //not implemented
        }
    } else if (metric_type == METRIC_Substructure || metric_type == METRIC_Superstructure) {
        // unsupported
    } else {
        if (use_heap) {
            search_knn_hamming_heap (*this, n, x, k, idx, coarse_dis,
                                     distances, labels, store_pairs,
                                     params, index_ivf_stats, bitset);
        } else {
            if (store_pairs) {
                search_knn_hamming_count_1<true>
                        (*this, n, x, idx, k, distances, labels, params, index_ivf_stats, bitset);
            } else {
                search_knn_hamming_count_1<false>
                        (*this, n, x, idx, k, distances, labels, params, index_ivf_stats, bitset);
            }
        }
    }
}

void IndexBinaryIVF::range_search(
        idx_t n, const uint8_t *x, int radius,
        RangeSearchResult *res,
        const BitsetView bitset) const
{
    std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
    std::unique_ptr<int32_t[]> coarse_dis(new int32_t[n * nprobe]);

    double t0 = getmillisecs();

    quantizer->search(n, x, nprobe, coarse_dis.get(), idx.get());
    index_ivf_stats.quantization_time += getmillisecs() - t0;

    t0 = getmillisecs();
    invlists->prefetch_lists(idx.get(), n * nprobe);

    bool store_pairs = false;
    size_t nlistv = 0, ndis = 0;

    std::vector<RangeSearchPartialResult *> all_pres (omp_get_max_threads());

#pragma omp parallel reduction(+: nlistv, ndis)
    {
        RangeSearchPartialResult pres(res);
        std::unique_ptr<BinaryInvertedListScanner> scanner
            (get_InvertedListScanner(store_pairs));
        FAISS_THROW_IF_NOT (scanner.get ());

        all_pres[omp_get_thread_num()] = &pres;

        auto scan_list_func = [&](size_t i, size_t ik, RangeQueryResult &qres)
        {

            idx_t key = idx[i * nprobe + ik];  /* select the list  */
            if (key < 0) return;
            FAISS_THROW_IF_NOT_FMT (
                    key < (idx_t) nlist,
                    "Invalid key=%ld  at ik=%ld nlist=%ld\n",
                    key, ik, nlist);
            const size_t list_size = invlists->list_size(key);

            if (list_size == 0) return;

            InvertedLists::ScopedCodes scodes (invlists, key);
            InvertedLists::ScopedIds ids (invlists, key);

            scanner->set_list (key, coarse_dis[i * nprobe + ik]);
            nlistv++;
            ndis += list_size;
            scanner->scan_codes_range (list_size, scodes.get(),
                                       ids.get(), radius, qres);
        };

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            scanner->set_query (x + i * code_size);

            RangeQueryResult & qres = pres.new_result (i);

            for (size_t ik = 0; ik < nprobe; ik++) {
                scan_list_func (i, ik, qres);
            }

        }

        pres.finalize();

    }

    if(STATISTICS_LEVEL >= 1) {
        index_ivf_stats.nq += n;
        index_ivf_stats.nlist += nlistv;
        index_ivf_stats.ndis += ndis;
        index_ivf_stats.search_time += getmillisecs() - t0;
    }

}




IndexBinaryIVF::~IndexBinaryIVF() {
  if (own_invlists) {
    delete invlists;
  }

  if (own_fields) {
    delete quantizer;
  }
}


}  // namespace faiss
