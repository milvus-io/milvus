/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexIVF.h>


#include <omp.h>

#include <cstdio>
#include <memory>

#include <faiss/utils/utils.h>
#include <faiss/utils/hamming.h>

#include <faiss/impl/FaissAssert.h>
#include <faiss/IndexFlat.h>
#include <faiss/impl/AuxIndexStructures.h>

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

size_t Level1Quantizer::coarse_code_size () const
{
    size_t nl = nlist - 1;
    size_t nbyte = 0;
    while (nl > 0) {
        nbyte ++;
        nl >>= 8;
    }
    return nbyte;
}

void Level1Quantizer::encode_listno (Index::idx_t list_no, uint8_t *code) const
{
    // little endian
    size_t nl = nlist - 1;
    while (nl > 0) {
        *code++ = list_no & 0xff;
        list_no >>= 8;
        nl >>= 8;
    }
}

Index::idx_t Level1Quantizer::decode_listno (const uint8_t *code) const
{
    size_t nl = nlist - 1;
    int64_t list_no = 0;
    int nbit = 0;
    while (nl > 0) {
        list_no |= int64_t(*code++) << nbit;
        nbit += 8;
        nl >>= 8;
    }
    FAISS_THROW_IF_NOT (list_no >= 0 && list_no < nlist);
    return list_no;
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
    parallel_mode (0)
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
    nprobe (1), max_codes (0), parallel_mode (0)
{}

void IndexIVF::add (idx_t n, const float * x)
{
    add_with_ids (n, x, nullptr);
}


void IndexIVF::add_with_ids (idx_t n, const float * x, const idx_t *xids)
{
    // do some blocking to avoid excessive allocs
    idx_t bs = 65536;
    if (n > bs) {
        for (idx_t i0 = 0; i0 < n; i0 += bs) {
            idx_t i1 = std::min (n, i0 + bs);
            if (verbose) {
                printf("   IndexIVF::add_with_ids %ld:%ld\n", i0, i1);
            }
            add_with_ids (i1 - i0, x + i0 * d,
                          xids ? xids + i0 : nullptr);
        }
        return;
    }

    FAISS_THROW_IF_NOT (is_trained);
    direct_map.check_can_add (xids);

    std::unique_ptr<idx_t []> idx(new idx_t[n]);
    quantizer->assign (n, x, idx.get());
    size_t nadd = 0, nminus1 = 0;

    for (size_t i = 0; i < n; i++) {
        if (idx[i] < 0) nminus1++;
    }

    std::unique_ptr<uint8_t []> flat_codes(new uint8_t [n * code_size]);
    encode_vectors (n, x, idx.get(), flat_codes.get());

    DirectMapAdd dm_adder(direct_map, n, xids);

#pragma omp parallel reduction(+: nadd)
    {
        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();

        // each thread takes care of a subset of lists
        for (size_t i = 0; i < n; i++) {
            idx_t list_no = idx [i];
            if (list_no >= 0 && list_no % nt == rank) {
                idx_t id = xids ? xids[i] : ntotal + i;
                size_t ofs = invlists->add_entry (
                     list_no, id,
                     flat_codes.get() + i * code_size
                );

                dm_adder.add (i, list_no, ofs);

                nadd++;
            } else if (rank == 0 && list_no == -1) {
                dm_adder.add (i, -1, 0);
            }
        }
    }


    if (verbose) {
        printf("    added %ld / %ld vectors (%ld -1s)\n", nadd, n, nminus1);
    }

    ntotal += n;
}

void IndexIVF::make_direct_map (bool b)
{
    if (b) {
        direct_map.set_type (DirectMap::Array, invlists, ntotal);
    } else {
        direct_map.set_type (DirectMap::NoMap, invlists, ntotal);
    }
}

void IndexIVF::set_direct_map_type (DirectMap::Type type)
{
    direct_map.set_type (type, invlists, ntotal);
}


void IndexIVF::search (idx_t n, const float *x, idx_t k,
                         float *distances, idx_t *labels) const
{
    std::unique_ptr<idx_t[]> idx(new idx_t[n * nprobe]);
    std::unique_ptr<float[]> coarse_dis(new float[n * nprobe]);

    double t0 = getmillisecs();
    quantizer->search (n, x, nprobe, coarse_dis.get(), idx.get());
    indexIVF_stats.quantization_time += getmillisecs() - t0;

    t0 = getmillisecs();
    invlists->prefetch_lists (idx.get(), n * nprobe);

    search_preassigned (n, x, k, idx.get(), coarse_dis.get(),
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

    bool interrupt = false;

    int pmode = this->parallel_mode & ~PARALLEL_MODE_NO_HEAP_INIT;
    bool do_heap_init = !(this->parallel_mode & PARALLEL_MODE_NO_HEAP_INIT);

    // don't start parallel section if single query
    bool do_parallel =
        pmode == 0 ? n > 1 :
        pmode == 1 ? nprobe > 1 :
        nprobe * n > 1;

#pragma omp parallel if(do_parallel) reduction(+: nlistv, ndis, nheap)
    {
        InvertedListScanner *scanner = get_InvertedListScanner(store_pairs);
        ScopeDeleter1<InvertedListScanner> del(scanner);

        /*****************************************************
         * Depending on parallel_mode, there are two possible ways
         * to organize the search. Here we define local functions
         * that are in common between the two
         ******************************************************/

        // intialize + reorder a result heap

        auto init_result = [&](float *simi, idx_t *idxi) {
            if (!do_heap_init) return;
            if (metric_type == METRIC_INNER_PRODUCT) {
                heap_heapify<HeapForIP> (k, simi, idxi);
            } else {
                heap_heapify<HeapForL2> (k, simi, idxi);
            }
        };

        auto reorder_result = [&] (float *simi, idx_t *idxi) {
            if (!do_heap_init) return;
            if (metric_type == METRIC_INNER_PRODUCT) {
                heap_reorder<HeapForIP> (k, simi, idxi);
            } else {
                heap_reorder<HeapForL2> (k, simi, idxi);
            }
        };

        // single list scan using the current scanner (with query
        // set porperly) and storing results in simi and idxi
        auto scan_one_list = [&] (idx_t key, float coarse_dis_i,
                                  float *simi, idx_t *idxi) {

            if (key < 0) {
                // not enough centroids for multiprobe
                return (size_t)0;
            }
            FAISS_THROW_IF_NOT_FMT (key < (idx_t) nlist,
                                    "Invalid key=%ld nlist=%ld\n",
                                    key, nlist);

            size_t list_size = invlists->list_size(key);

            // don't waste time on empty lists
            if (list_size == 0) {
                return (size_t)0;
            }

            scanner->set_list (key, coarse_dis_i);

            nlistv++;

            InvertedLists::ScopedCodes scodes (invlists, key);

            std::unique_ptr<InvertedLists::ScopedIds> sids;
            const Index::idx_t * ids = nullptr;

            if (!store_pairs)  {
                sids.reset (new InvertedLists::ScopedIds (invlists, key));
                ids = sids->get();
            }

            nheap += scanner->scan_codes (list_size, scodes.get(),
                                          ids, simi, idxi, k);

            return list_size;
        };

        /****************************************************
         * Actual loops, depending on parallel_mode
         ****************************************************/

        if (pmode == 0) {

#pragma omp for
            for (size_t i = 0; i < n; i++) {

                if (interrupt) {
                    continue;
                }

                // loop over queries
                scanner->set_query (x + i * d);
                float * simi = distances + i * k;
                idx_t * idxi = labels + i * k;

                init_result (simi, idxi);

                long nscan = 0;

                // loop over probes
                for (size_t ik = 0; ik < nprobe; ik++) {

                    nscan += scan_one_list (
                         keys [i * nprobe + ik],
                         coarse_dis[i * nprobe + ik],
                         simi, idxi
                    );

                    if (max_codes && nscan >= max_codes) {
                        break;
                    }
                }

                ndis += nscan;
                reorder_result (simi, idxi);

                if (InterruptCallback::is_interrupted ()) {
                    interrupt = true;
                }

            } // parallel for
        } else if (pmode == 1) {
            std::vector <idx_t> local_idx (k);
            std::vector <float> local_dis (k);

            for (size_t i = 0; i < n; i++) {
                scanner->set_query (x + i * d);
                init_result (local_dis.data(), local_idx.data());

#pragma omp for schedule(dynamic)
                for (size_t ik = 0; ik < nprobe; ik++) {
                    ndis += scan_one_list
                        (keys [i * nprobe + ik],
                         coarse_dis[i * nprobe + ik],
                         local_dis.data(), local_idx.data());

                    // can't do the test on max_codes
                }
                // merge thread-local results

                float * simi = distances + i * k;
                idx_t * idxi = labels + i * k;
#pragma omp single
                init_result (simi, idxi);

#pragma omp barrier
#pragma omp critical
                {
                    if (metric_type == METRIC_INNER_PRODUCT) {
                        heap_addn<HeapForIP>
                            (k, simi, idxi,
                             local_dis.data(), local_idx.data(), k);
                    } else {
                        heap_addn<HeapForL2>
                            (k, simi, idxi,
                             local_dis.data(), local_idx.data(), k);
                    }
                }
#pragma omp barrier
#pragma omp single
                reorder_result (simi, idxi);
            }
        } else {
            FAISS_THROW_FMT ("parallel_mode %d not supported\n",
                             pmode);
        }
    } // parallel section

    if (interrupt) {
        FAISS_THROW_MSG ("computation interrupted");
    }

    indexIVF_stats.nq += n;
    indexIVF_stats.nlist += nlistv;
    indexIVF_stats.ndis += ndis;
    indexIVF_stats.nheap_updates += nheap;

}




void IndexIVF::range_search (idx_t nx, const float *x, float radius,
                             RangeSearchResult *result) const
{
    std::unique_ptr<idx_t[]> keys (new idx_t[nx * nprobe]);
    std::unique_ptr<float []> coarse_dis (new float[nx * nprobe]);

    double t0 = getmillisecs();
    quantizer->search (nx, x, nprobe, coarse_dis.get (), keys.get ());
    indexIVF_stats.quantization_time += getmillisecs() - t0;

    t0 = getmillisecs();
    invlists->prefetch_lists (keys.get(), nx * nprobe);

    range_search_preassigned (nx, x, radius, keys.get (), coarse_dis.get (),
                              result);

    indexIVF_stats.search_time += getmillisecs() - t0;
}

void IndexIVF::range_search_preassigned (
         idx_t nx, const float *x, float radius,
         const idx_t *keys, const float *coarse_dis,
         RangeSearchResult *result) const
{

    size_t nlistv = 0, ndis = 0;
    bool store_pairs = false;

    std::vector<RangeSearchPartialResult *> all_pres (omp_get_max_threads());

#pragma omp parallel reduction(+: nlistv, ndis)
    {
        RangeSearchPartialResult pres(result);
        std::unique_ptr<InvertedListScanner> scanner
            (get_InvertedListScanner(store_pairs));
        FAISS_THROW_IF_NOT (scanner.get ());
        all_pres[omp_get_thread_num()] = &pres;

        // prepare the list scanning function

        auto scan_list_func = [&](size_t i, size_t ik, RangeQueryResult &qres) {

            idx_t key = keys[i * nprobe + ik];  /* select the list  */
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

        if (parallel_mode == 0) {

#pragma omp for
            for (size_t i = 0; i < nx; i++) {
                scanner->set_query (x + i * d);

                RangeQueryResult & qres = pres.new_result (i);

                for (size_t ik = 0; ik < nprobe; ik++) {
                    scan_list_func (i, ik, qres);
                }

            }

        } else if (parallel_mode == 1) {

            for (size_t i = 0; i < nx; i++) {
                scanner->set_query (x + i * d);

                RangeQueryResult & qres = pres.new_result (i);

#pragma omp for schedule(dynamic)
                for (size_t ik = 0; ik < nprobe; ik++) {
                    scan_list_func (i, ik, qres);
                }
            }
        } else if (parallel_mode == 2) {
            std::vector<RangeQueryResult *> all_qres (nx);
            RangeQueryResult *qres = nullptr;

#pragma omp for schedule(dynamic)
            for (size_t iik = 0; iik < nx * nprobe; iik++) {
                size_t i = iik / nprobe;
                size_t ik = iik % nprobe;
                if (qres == nullptr || qres->qno != i) {
                    FAISS_ASSERT (!qres || i > qres->qno);
                    qres = &pres.new_result (i);
                    scanner->set_query (x + i * d);
                }
                scan_list_func (i, ik, *qres);
            }
        } else {
            FAISS_THROW_FMT ("parallel_mode %d not supported\n", parallel_mode);
        }
        if (parallel_mode == 0) {
            pres.finalize ();
        } else {
#pragma omp barrier
#pragma omp single
            RangeSearchPartialResult::merge (all_pres, false);
#pragma omp barrier

        }
    }
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
    idx_t lo = direct_map.get (key);
    reconstruct_from_offset (lo_listno(lo), lo_offset(lo), recons);
}


void IndexIVF::reconstruct_n (idx_t i0, idx_t ni, float* recons) const
{
    FAISS_THROW_IF_NOT (ni == 0 || (i0 >= 0 && i0 + ni <= ntotal));

    for (idx_t list_no = 0; list_no < nlist; list_no++) {
        size_t list_size = invlists->list_size (list_no);
        ScopedIds idlist (invlists, list_no);

        for (idx_t offset = 0; offset < list_size; offset++) {
            idx_t id = idlist[offset];
            if (!(id >= i0 && id < i0 + ni)) {
                continue;
            }

            float* reconstructed = recons + (id - i0) * d;
            reconstruct_from_offset (list_no, offset, reconstructed);
        }
    }
}


/* standalone codec interface */
size_t IndexIVF::sa_code_size () const
{
    size_t coarse_size = coarse_code_size();
    return code_size + coarse_size;
}

void IndexIVF::sa_encode (idx_t n, const float *x,
                                 uint8_t *bytes) const
{
    FAISS_THROW_IF_NOT (is_trained);
    std::unique_ptr<int64_t []> idx (new int64_t [n]);
    quantizer->assign (n, x, idx.get());
    encode_vectors (n, x, idx.get(), bytes, true);
}


void IndexIVF::search_and_reconstruct (idx_t n, const float *x, idx_t k,
                                       float *distances, idx_t *labels,
                                       float *recons) const
{
    idx_t * idx = new idx_t [n * nprobe];
    ScopeDeleter<idx_t> del (idx);
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
                int list_no = lo_listno (key);
                int offset = lo_offset (key);

                // Update label to the actual id
                labels[ij] = invlists->get_single_id (list_no, offset);

                reconstruct_from_offset (list_no, offset, reconstructed);
            }
        }
    }
}

void IndexIVF::reconstruct_from_offset(
    int64_t /*list_no*/,
    int64_t /*offset*/,
    float* /*recons*/) const {
  FAISS_THROW_MSG ("reconstruct_from_offset not implemented");
}

void IndexIVF::reset ()
{
    direct_map.clear ();
    invlists->reset ();
    ntotal = 0;
}


size_t IndexIVF::remove_ids (const IDSelector & sel)
{
    size_t nremove = direct_map.remove_ids (sel, invlists);
    ntotal -= nremove;
    return nremove;
}


void IndexIVF::update_vectors (int n, const idx_t *new_ids, const float *x)
{

    if (direct_map.type == DirectMap::Hashtable) {
        // just remove then add
        IDSelectorArray sel(n, new_ids);
        size_t nremove = remove_ids (sel);
        FAISS_THROW_IF_NOT_MSG (nremove == n,
                                "did not find all entries to remove");
        add_with_ids (n, x, new_ids);
        return;
    }

    FAISS_THROW_IF_NOT (direct_map.type == DirectMap::Array);
    // here it is more tricky because we don't want to introduce holes
    // in continuous range of ids

    FAISS_THROW_IF_NOT (is_trained);
    std::vector<idx_t> assign (n);
    quantizer->assign (n, x, assign.data());

    std::vector<uint8_t> flat_codes (n * code_size);
    encode_vectors (n, x, assign.data(), flat_codes.data());

    direct_map.update_codes (invlists, n, new_ids, assign.data(), flat_codes.data());

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
    FAISS_THROW_IF_NOT_MSG (this->direct_map.no() && other.direct_map.no(),
                            "merge direct_map not implemented");
}


void IndexIVF::merge_from (IndexIVF &other, idx_t add_id)
{
    check_compatible_for_merge (other);

    invlists->merge_from (other.invlists, add_id);

    ntotal += other.ntotal;
    other.ntotal = 0;
}


void IndexIVF::replace_invlists (InvertedLists *il, bool own)
{
    if (own_invlists) {
        delete invlists;
    }
    // FAISS_THROW_IF_NOT (ntotal == 0);
    if (il) {
        FAISS_THROW_IF_NOT (il->nlist == nlist &&
                            il->code_size == code_size);
    }
    invlists = il;
    own_invlists = own;
}


void IndexIVF::copy_subset_to (IndexIVF & other, int subset_type,
                                 idx_t a1, idx_t a2) const
{

    FAISS_THROW_IF_NOT (nlist == other.nlist);
    FAISS_THROW_IF_NOT (code_size == other.code_size);
    FAISS_THROW_IF_NOT (other.direct_map.no());
    FAISS_THROW_IF_NOT_FMT (
          subset_type == 0 || subset_type == 1 || subset_type == 2,
          "subset type %d not implemented", subset_type);

    size_t accu_n = 0;
    size_t accu_a1 = 0;
    size_t accu_a2 = 0;

    InvertedLists *oivf = other.invlists;

    for (idx_t list_no = 0; list_no < nlist; list_no++) {
        size_t n = invlists->list_size (list_no);
        ScopedIds ids_in (invlists, list_no);

        if (subset_type == 0) {
            for (idx_t i = 0; i < n; i++) {
                idx_t id = ids_in[i];
                if (a1 <= id && id < a2) {
                    oivf->add_entry (list_no,
                                     invlists->get_single_id (list_no, i),
                                     ScopedCodes (invlists, list_no, i).get());
                    other.ntotal++;
                }
            }
        } else if (subset_type == 1) {
            for (idx_t i = 0; i < n; i++) {
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

            for (idx_t i = i1; i < i2; i++) {
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
