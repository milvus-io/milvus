/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexIVF.h"

#include <cstdio>

#include "utils.h"
#include "hamming.h"

#include "FaissAssert.h"
#include "IndexFlat.h"
#include "AuxIndexStructures.h"

namespace faiss {

using ScopedIds = InvertedLists::ScopedIds;
using ScopedCodes = InvertedLists::ScopedCodes;

/*****************************************
 * Level1Quantizer implementation
 ******************************************/


Level1Quantizer::Level1Quantizer (Index * quantizer, size_t nlist):
    quantizer (quantizer),
    nlist (nlist),
    quantizer_trains_alone (0),
    own_fields (false),
    clustering_index (nullptr)
{
    // here we set a low # iterations because this is typically used
    // for large clusterings (nb this is not used for the MultiIndex,
    // for which quantizer_trains_alone = true)
    cp.niter = 10;
}

Level1Quantizer::Level1Quantizer ():
    quantizer (nullptr),
    nlist (0),
    quantizer_trains_alone (0), own_fields (false),
    clustering_index (nullptr)
{}

Level1Quantizer::~Level1Quantizer ()
{
    if (own_fields) delete quantizer;
}

void Level1Quantizer::train_q1 (size_t n, const float *x, bool verbose, MetricType metric_type)
{
    size_t d = quantizer->d;
    if (quantizer->is_trained && (quantizer->ntotal == nlist)) {
        if (verbose)
            printf ("IVF quantizer does not need training.\n");
    } else if (quantizer_trains_alone == 1) {
        if (verbose)
            printf ("IVF quantizer trains alone...\n");
        quantizer->train (n, x);
        quantizer->verbose = verbose;
        FAISS_THROW_IF_NOT_MSG (quantizer->ntotal == nlist,
                          "nlist not consistent with quantizer size");
    } else if (quantizer_trains_alone == 0) {
        if (verbose)
            printf ("Training level-1 quantizer on %ld vectors in %ldD\n",
                    n, d);

        Clustering clus (d, nlist, cp);
        quantizer->reset();
        if (clustering_index) {
            clus.train (n, x, *clustering_index);
            quantizer->add (nlist, clus.centroids.data());
        } else {
            clus.train (n, x, *quantizer);
        }
        quantizer->is_trained = true;
    } else if (quantizer_trains_alone == 2) {
        if (verbose)
            printf (
                "Training L2 quantizer on %ld vectors in %ldD%s\n",
                n, d,
                clustering_index ? "(user provided index)" : "");
        FAISS_THROW_IF_NOT (metric_type == METRIC_L2);
        Clustering clus (d, nlist, cp);
        if (!clustering_index) {
            IndexFlatL2 assigner (d);
            clus.train(n, x, assigner);
        } else {
            clus.train(n, x, *clustering_index);
        }
        if (verbose)
            printf ("Adding centroids to quantizer\n");
        quantizer->add (nlist, clus.centroids.data());
    }
}



/*****************************************
 * IndexIVF implementation
 ******************************************/


IndexIVF::IndexIVF (Index * quantizer, size_t d,
                    size_t nlist, size_t code_size,
                    MetricType metric):
    Index (d, metric),
    Level1Quantizer (quantizer, nlist),
    invlists (new ArrayInvertedLists (nlist, code_size)),
    own_invlists (true),
    code_size (code_size),
    nprobe (1),
    max_codes (0),
    maintain_direct_map (false)
{
    FAISS_THROW_IF_NOT (d == quantizer->d);
    is_trained = quantizer->is_trained && (quantizer->ntotal == nlist);
    // Spherical by default if the metric is inner_product
    if (metric_type == METRIC_INNER_PRODUCT) {
        cp.spherical = true;
    }

}

IndexIVF::IndexIVF ():
    invlists (nullptr), own_invlists (false),
    code_size (0),
    nprobe (1), max_codes (0),
    maintain_direct_map (false)
{}

void IndexIVF::add (idx_t n, const float * x)
{
    add_with_ids (n, x, nullptr);
}

void IndexIVF::make_direct_map (bool new_maintain_direct_map)
{
    // nothing to do
    if (new_maintain_direct_map == maintain_direct_map)
        return;

    if (new_maintain_direct_map) {
        direct_map.resize (ntotal, -1);
        for (size_t key = 0; key < nlist; key++) {
            size_t list_size = invlists->list_size (key);
            ScopedIds idlist (invlists, key);

            for (long ofs = 0; ofs < list_size; ofs++) {
                FAISS_THROW_IF_NOT_MSG (
                       0 <= idlist [ofs] && idlist[ofs] < ntotal,
                       "direct map supported only for seuquential ids");
                direct_map [idlist [ofs]] = key << 32 | ofs;
            }
        }
    } else {
        direct_map.clear ();
    }
    maintain_direct_map = new_maintain_direct_map;
}


void IndexIVF::search (idx_t n, const float *x, idx_t k,
                         float *distances, idx_t *labels) const
{
    long * idx = new long [n * nprobe];
    ScopeDeleter<long> del (idx);
    float * coarse_dis = new float [n * nprobe];
    ScopeDeleter<float> del2 (coarse_dis);

    double t0 = getmillisecs();
    quantizer->search (n, x, nprobe, coarse_dis, idx);
    indexIVF_stats.quantization_time += getmillisecs() - t0;

    t0 = getmillisecs();
    invlists->prefetch_lists (idx, n * nprobe);

    search_preassigned (n, x, k, idx, coarse_dis,
                        distances, labels, false);
    indexIVF_stats.search_time += getmillisecs() - t0;
}



void IndexIVF::search_preassigned (idx_t n, const float *x, idx_t k,
                                   const idx_t *keys,
                                   const float *coarse_dis ,
                                   float *distances, idx_t *labels,
                                   bool store_pairs,
                                   const IVFSearchParameters *params) const
{
    long nprobe = params ? params->nprobe : this->nprobe;
    long max_codes = params ? params->max_codes : this->max_codes;

    size_t nlistv = 0, ndis = 0, nheap = 0;

    using HeapForIP = CMin<float, idx_t>;
    using HeapForL2 = CMax<float, idx_t>;

    idx_t check_period = InterruptCallback::get_period_hint
        (nprobe * ntotal * d / nlist);

    for (idx_t i0 = 0; i0 < n; i0 += check_period) {
        idx_t i1 = std::min(i0 + check_period, n);

#pragma omp parallel reduction(+: nlistv, ndis, nheap)
        {
            InvertedListScanner *scanner = get_InvertedListScanner(store_pairs);
            ScopeDeleter1<InvertedListScanner> del(scanner);
#pragma omp for
            for (size_t i = i0; i < i1; i++) {
                // loop over queries
                const float * xi = x + i * d;
                scanner->set_query (xi);
                const long * keysi = keys + i * nprobe;
                float * simi = distances + i * k;
                long * idxi = labels + i * k;

                if (metric_type == METRIC_INNER_PRODUCT) {
                    heap_heapify<HeapForIP> (k, simi, idxi);
                } else {
                    heap_heapify<HeapForL2> (k, simi, idxi);
                }

                long nscan = 0;

                // loop over probes
                for (size_t ik = 0; ik < nprobe; ik++) {
                    long key = keysi[ik];  /* select the list  */
                    if (key < 0) {
                        // not enough centroids for multiprobe
                        continue;
                    }
                    FAISS_THROW_IF_NOT_FMT (key < (long) nlist,
                        "Invalid key=%ld  at ik=%ld nlist=%ld\n",
                        key, ik, nlist);


                    size_t list_size = invlists->list_size(key);

                    // don't waste time on empty lists
                    if (list_size == 0) {
                        continue;
                    }

                    scanner->set_list (key, coarse_dis[i * nprobe + ik]);

                    nlistv++;

                    InvertedLists::ScopedCodes scodes (invlists, key);
                    const Index::idx_t * ids = store_pairs ? nullptr :
                        invlists->get_ids (key);

                    nheap += scanner->scan_codes (list_size, scodes.get(),
                                                  ids, simi, idxi, k);

                    if (ids) {
                        invlists->release_ids (ids);
                    }

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
        InterruptCallback::check ();
    } // loop over blocks

    indexIVF_stats.nq += n;
    indexIVF_stats.nlist += nlistv;
    indexIVF_stats.ndis += ndis;
    indexIVF_stats.nheap_updates += nheap;

}




void IndexIVF::range_search (idx_t nx, const float *x, float radius,
                             RangeSearchResult *result) const
{

    long * keys = new long [nx * nprobe];
    ScopeDeleter<long> del (keys);
    float * coarse_dis = new float [nx * nprobe];
    ScopeDeleter<float> del2 (coarse_dis);

    double t0 = getmillisecs();
    quantizer->search (nx, x, nprobe, coarse_dis, keys);
    indexIVF_stats.quantization_time += getmillisecs() - t0;

    t0 = getmillisecs();
    invlists->prefetch_lists (keys, nx * nprobe);

    size_t nlistv = 0, ndis = 0;
    bool store_pairs = false;

#pragma omp parallel reduction(+: nlistv, ndis)
    {
        RangeSearchPartialResult pres(result);
        InvertedListScanner *scanner = get_InvertedListScanner(store_pairs);
        ScopeDeleter1<InvertedListScanner> del3(scanner);

#pragma omp for
        for (size_t i = 0; i < nx; i++) {
            const float * xi = x + i * d;
            scanner->set_query (xi);
            const long * keysi = keys + i * nprobe;

            RangeQueryResult & qres = pres.new_result (i);

            for (size_t ik = 0; ik < nprobe; ik++) {
                long key = keysi[ik];  /* select the list  */
                if (key < 0) continue;
                FAISS_THROW_IF_NOT_FMT (key < (long) nlist,
                      "Invalid key=%ld  at ik=%ld nlist=%ld\n",
                      key, ik, nlist);
                const size_t list_size = invlists->list_size(key);

                if (list_size == 0) continue;

                InvertedLists::ScopedCodes scodes (invlists, key);
                InvertedLists::ScopedIds ids (invlists, key);

                scanner->set_list (key, coarse_dis[i * nprobe + ik]);
                nlistv++;
                ndis += list_size;
                scanner->scan_codes_range (list_size, scodes.get(),
                                           ids.get(), radius, qres);

            }
        }

        pres.finalize ();
    }
    indexIVF_stats.search_time += getmillisecs() - t0;
    indexIVF_stats.nq += nx;
    indexIVF_stats.nlist += nlistv;
    indexIVF_stats.ndis += ndis;
}


InvertedListScanner *IndexIVF::get_InvertedListScanner (
    bool /*store_pairs*/) const
{
    return nullptr;
}

void IndexIVF::reconstruct (idx_t key, float* recons) const
{
    FAISS_THROW_IF_NOT_MSG (direct_map.size() == ntotal,
                            "direct map is not initialized");
    FAISS_THROW_IF_NOT_MSG (key >= 0 && key < direct_map.size(),
                            "invalid key");
    long list_no = direct_map[key] >> 32;
    long offset = direct_map[key] & 0xffffffff;
    reconstruct_from_offset (list_no, offset, recons);
}


void IndexIVF::reconstruct_n (idx_t i0, idx_t ni, float* recons) const
{
    FAISS_THROW_IF_NOT (ni == 0 || (i0 >= 0 && i0 + ni <= ntotal));

    for (long list_no = 0; list_no < nlist; list_no++) {
        size_t list_size = invlists->list_size (list_no);
        ScopedIds idlist (invlists, list_no);

        for (long offset = 0; offset < list_size; offset++) {
            long id = idlist[offset];
            if (!(id >= i0 && id < i0 + ni)) {
                continue;
            }

            float* reconstructed = recons + (id - i0) * d;
            reconstruct_from_offset (list_no, offset, reconstructed);
        }
    }
}


void IndexIVF::search_and_reconstruct (idx_t n, const float *x, idx_t k,
                                       float *distances, idx_t *labels,
                                       float *recons) const
{
    long * idx = new long [n * nprobe];
    ScopeDeleter<long> del (idx);
    float * coarse_dis = new float [n * nprobe];
    ScopeDeleter<float> del2 (coarse_dis);

    quantizer->search (n, x, nprobe, coarse_dis, idx);

    invlists->prefetch_lists (idx, n * nprobe);

    // search_preassigned() with `store_pairs` enabled to obtain the list_no
    // and offset into `codes` for reconstruction
    search_preassigned (n, x, k, idx, coarse_dis,
                        distances, labels, true /* store_pairs */);
    for (idx_t i = 0; i < n; ++i) {
        for (idx_t j = 0; j < k; ++j) {
            idx_t ij = i * k + j;
            idx_t key = labels[ij];
            float* reconstructed = recons + ij * d;
            if (key < 0) {
                // Fill with NaNs
                memset(reconstructed, -1, sizeof(*reconstructed) * d);
            } else {
                int list_no = key >> 32;
                int offset = key & 0xffffffff;

                // Update label to the actual id
                labels[ij] = invlists->get_single_id (list_no, offset);

                reconstruct_from_offset (list_no, offset, reconstructed);
            }
        }
    }
}

void IndexIVF::reconstruct_from_offset(
    long /*list_no*/,
    long /*offset*/,
    float* /*recons*/) const {
  FAISS_THROW_MSG ("reconstruct_from_offset not implemented");
}

void IndexIVF::reset ()
{
    direct_map.clear ();
    invlists->reset ();
    ntotal = 0;
}


long IndexIVF::remove_ids (const IDSelector & sel)
{
    FAISS_THROW_IF_NOT_MSG (!maintain_direct_map,
                    "direct map remove not implemented");

    std::vector<long> toremove(nlist);

#pragma omp parallel for
    for (long i = 0; i < nlist; i++) {
        long l0 = invlists->list_size (i), l = l0, j = 0;
        ScopedIds idsi (invlists, i);
        while (j < l) {
            if (sel.is_member (idsi[j])) {
                l--;
                invlists->update_entry (
                     i, j,
                     invlists->get_single_id (i, l),
                     ScopedCodes (invlists, i, l).get());
            } else {
                j++;
            }
        }
        toremove[i] = l0 - l;
    }
    // this will not run well in parallel on ondisk because of possible shrinks
    long nremove = 0;
    for (long i = 0; i < nlist; i++) {
        if (toremove[i] > 0) {
            nremove += toremove[i];
            invlists->resize(
                i, invlists->list_size(i) - toremove[i]);
        }
    }
    ntotal -= nremove;
    return nremove;
}




void IndexIVF::train (idx_t n, const float *x)
{
    if (verbose)
        printf ("Training level-1 quantizer\n");

    train_q1 (n, x, verbose, metric_type);

    if (verbose)
        printf ("Training IVF residual\n");

    train_residual (n, x);
    is_trained = true;

}

void IndexIVF::train_residual(idx_t /*n*/, const float* /*x*/) {
  if (verbose)
    printf("IndexIVF: no residual training\n");
  // does nothing by default
}


void IndexIVF::check_compatible_for_merge (const IndexIVF &other) const
{
    // minimal sanity checks
    FAISS_THROW_IF_NOT (other.d == d);
    FAISS_THROW_IF_NOT (other.nlist == nlist);
    FAISS_THROW_IF_NOT (other.code_size == code_size);
    FAISS_THROW_IF_NOT_MSG (typeid (*this) == typeid (other),
                  "can only merge indexes of the same type");
}


void IndexIVF::merge_from (IndexIVF &other, idx_t add_id)
{
    check_compatible_for_merge (other);
    FAISS_THROW_IF_NOT_MSG ((!maintain_direct_map &&
                             !other.maintain_direct_map),
                  "direct map copy not implemented");

    invlists->merge_from (other.invlists, add_id);

    ntotal += other.ntotal;
    other.ntotal = 0;
}


void IndexIVF::replace_invlists (InvertedLists *il, bool own)
{
    //FAISS_THROW_IF_NOT (ntotal == 0);
    FAISS_THROW_IF_NOT (il->nlist == nlist &&
                        il->code_size == code_size);
    if (own_invlists) {
        delete invlists;
    }
    invlists = il;
    own_invlists = own;
}


void IndexIVF::copy_subset_to (IndexIVF & other, int subset_type,
                                 long a1, long a2) const
{

    FAISS_THROW_IF_NOT (nlist == other.nlist);
    FAISS_THROW_IF_NOT (code_size == other.code_size);
    FAISS_THROW_IF_NOT (!other.maintain_direct_map);
    FAISS_THROW_IF_NOT_FMT (
          subset_type == 0 || subset_type == 1 || subset_type == 2,
          "subset type %d not implemented", subset_type);

    size_t accu_n = 0;
    size_t accu_a1 = 0;
    size_t accu_a2 = 0;

    InvertedLists *oivf = other.invlists;

    for (long list_no = 0; list_no < nlist; list_no++) {
        size_t n = invlists->list_size (list_no);
        ScopedIds ids_in (invlists, list_no);

        if (subset_type == 0) {
            for (long i = 0; i < n; i++) {
                idx_t id = ids_in[i];
                if (a1 <= id && id < a2) {
                    oivf->add_entry (list_no,
                                     invlists->get_single_id (list_no, i),
                                     ScopedCodes (invlists, list_no, i).get());
                    other.ntotal++;
                }
            }
        } else if (subset_type == 1) {
            for (long i = 0; i < n; i++) {
                idx_t id = ids_in[i];
                if (id % a1 == a2) {
                    oivf->add_entry (list_no,
                                     invlists->get_single_id (list_no, i),
                                     ScopedCodes (invlists, list_no, i).get());
                    other.ntotal++;
                }
            }
        } else if (subset_type == 2) {
            // see what is allocated to a1 and to a2
            size_t next_accu_n = accu_n + n;
            size_t next_accu_a1 = next_accu_n * a1 / ntotal;
            size_t i1 = next_accu_a1 - accu_a1;
            size_t next_accu_a2 = next_accu_n * a2 / ntotal;
            size_t i2 = next_accu_a2 - accu_a2;

            for (long i = i1; i < i2; i++) {
                oivf->add_entry (list_no,
                                 invlists->get_single_id (list_no, i),
                                 ScopedCodes (invlists, list_no, i).get());
            }

            other.ntotal += i2 - i1;
            accu_a1 = next_accu_a1;
            accu_a2 = next_accu_a2;
        }
        accu_n += n;
    }
    FAISS_ASSERT(accu_n == ntotal);

}


IndexIVF::~IndexIVF()
{
    if (own_invlists) {
        delete invlists;
    }
}


void IndexIVFStats::reset()
{
    memset ((void*)this, 0, sizeof (*this));
}


IndexIVFStats indexIVF_stats;

void InvertedListScanner::scan_codes_range (size_t ,
                       const uint8_t *,
                       const idx_t *,
                       float ,
                       RangeQueryResult &) const
{
    FAISS_THROW_MSG ("scan_codes_range not implemented");
}



} // namespace faiss
