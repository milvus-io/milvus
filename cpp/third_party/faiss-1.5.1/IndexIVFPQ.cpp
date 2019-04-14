/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexIVFPQ.h"

#include <cmath>
#include <cstdio>
#include <cassert>

#include <algorithm>

#include "Heap.h"
#include "utils.h"

#include "Clustering.h"
#include "IndexFlat.h"

#include "hamming.h"

#include "FaissAssert.h"

#include "AuxIndexStructures.h"

namespace faiss {


/*****************************************
 * IndexIVFPQ implementation
 ******************************************/

IndexIVFPQ::IndexIVFPQ (Index * quantizer, size_t d, size_t nlist,
                        size_t M, size_t nbits_per_idx):
    IndexIVF (quantizer, d, nlist, 0, METRIC_L2),
    pq (d, M, nbits_per_idx)
{
    FAISS_THROW_IF_NOT (nbits_per_idx <= 8);
    code_size = pq.code_size;
    invlists->code_size = code_size;
    is_trained = false;
    by_residual = true;
    use_precomputed_table = 0;
    scan_table_threshold = 0;

    polysemous_training = nullptr;
    do_polysemous_training = false;
    polysemous_ht = 0;

}


/****************************************************************
 * training                                                     */

void IndexIVFPQ::train_residual (idx_t n, const float *x)
{
    train_residual_o (n, x, nullptr);
}


void IndexIVFPQ::train_residual_o (idx_t n, const float *x, float *residuals_2)
{
    const float * x_in = x;

    x = fvecs_maybe_subsample (
         d, (size_t*)&n, pq.cp.max_points_per_centroid * pq.ksub,
         x, verbose, pq.cp.seed);

    ScopeDeleter<float> del_x (x_in == x ? nullptr : x);

    const float *trainset;
    ScopeDeleter<float> del_residuals;
    if (by_residual) {
        if(verbose) printf("computing residuals\n");
        idx_t * assign = new idx_t [n]; // assignement to coarse centroids
        ScopeDeleter<idx_t> del (assign);
        quantizer->assign (n, x, assign);
        float *residuals = new float [n * d];
        del_residuals.set (residuals);
        for (idx_t i = 0; i < n; i++)
           quantizer->compute_residual (x + i * d, residuals+i*d, assign[i]);

        trainset = residuals;
    } else {
        trainset = x;
    }
    if (verbose)
        printf ("training %zdx%zd product quantizer on %ld vectors in %dD\n",
                pq.M, pq.ksub, n, d);
    pq.verbose = verbose;
    pq.train (n, trainset);

    if (do_polysemous_training) {
        if (verbose)
            printf("doing polysemous training for PQ\n");
        PolysemousTraining default_pt;
        PolysemousTraining *pt = polysemous_training;
        if (!pt) pt = &default_pt;
        pt->optimize_pq_for_hamming (pq, n, trainset);
    }

    // prepare second-level residuals for refine PQ
    if (residuals_2) {
        uint8_t *train_codes = new uint8_t [pq.code_size * n];
        ScopeDeleter<uint8_t> del (train_codes);
        pq.compute_codes (trainset, train_codes, n);

        for (idx_t i = 0; i < n; i++) {
            const float *xx = trainset + i * d;
            float * res = residuals_2 + i * d;
            pq.decode (train_codes + i * pq.code_size, res);
            for (int j = 0; j < d; j++)
                res[j] = xx[j] - res[j];
        }

    }

    if (by_residual) {
        precompute_table ();
    }

}






/****************************************************************
 * IVFPQ as codec                                               */


/* produce a binary signature based on the residual vector */
void IndexIVFPQ::encode (long key, const float * x, uint8_t * code) const
{
    if (by_residual) {
        float residual_vec[d];
        quantizer->compute_residual (x, residual_vec, key);
        pq.compute_code (residual_vec, code);
    }
    else pq.compute_code (x, code);
}

void IndexIVFPQ::encode_multiple (size_t n, long *keys,
                                  const float * x, uint8_t * xcodes,
                                  bool compute_keys) const
{
    if (compute_keys)
        quantizer->assign (n, x, keys);

    encode_vectors (n, x, keys, xcodes);
}

void IndexIVFPQ::decode_multiple (size_t n, const long *keys,
                                  const uint8_t * xcodes, float * x) const
{
    pq.decode (xcodes, x, n);
    if (by_residual) {
        std::vector<float> centroid (d);
        for (size_t i = 0; i < n; i++) {
            quantizer->reconstruct (keys[i], centroid.data());
            float *xi = x + i * d;
            for (size_t j = 0; j < d; j++) {
                xi [j] += centroid [j];
            }
        }
    }
}




/****************************************************************
 * add                                                          */


void IndexIVFPQ::add_with_ids (idx_t n, const float * x, const long *xids)
{
    add_core_o (n, x, xids, nullptr);
}


static float * compute_residuals (
        const Index *quantizer,
        Index::idx_t n, const float* x,
        const Index::idx_t *list_nos)
{
    size_t d = quantizer->d;
    float *residuals = new float [n * d];
    // TODO: parallelize?
    for (size_t i = 0; i < n; i++) {
        if (list_nos[i] < 0)
            memset (residuals + i * d, 0, sizeof(*residuals) * d);
        else
            quantizer->compute_residual (
                 x + i * d, residuals + i * d, list_nos[i]);
    }
    return residuals;
}

void IndexIVFPQ::encode_vectors(idx_t n, const float* x,
                                const idx_t *list_nos,
                                uint8_t * codes) const
{
    if (by_residual) {
        float *to_encode = compute_residuals (quantizer, n, x, list_nos);
        ScopeDeleter<float> del (to_encode);
        pq.compute_codes (to_encode, codes, n);
    } else {
        pq.compute_codes (x, codes, n);
    }
}


void IndexIVFPQ::add_core_o (idx_t n, const float * x, const long *xids,
                             float *residuals_2, const long *precomputed_idx)
{

    idx_t bs = 32768;
    if (n > bs) {
        for (idx_t i0 = 0; i0 < n; i0 += bs) {
            idx_t i1 = std::min(i0 + bs, n);
            if (verbose) {
                printf("IndexIVFPQ::add_core_o: adding %ld:%ld / %ld\n",
                       i0, i1, n);
            }
            add_core_o (i1 - i0, x + i0 * d,
                        xids ? xids + i0 : nullptr,
                        residuals_2 ? residuals_2 + i0 * d : nullptr,
                        precomputed_idx ? precomputed_idx + i0 : nullptr);
        }
        return;
    }

    FAISS_THROW_IF_NOT (is_trained);
    double t0 = getmillisecs ();
    const long * idx;
    ScopeDeleter<long> del_idx;

    if (precomputed_idx) {
        idx = precomputed_idx;
    } else {
        long * idx0 = new long [n];
        del_idx.set (idx0);
        quantizer->assign (n, x, idx0);
        idx = idx0;
    }

    double t1 = getmillisecs ();
    uint8_t * xcodes = new uint8_t [n * code_size];
    ScopeDeleter<uint8_t> del_xcodes (xcodes);

    const float *to_encode = nullptr;
    ScopeDeleter<float> del_to_encode;

    if (by_residual) {
        to_encode = compute_residuals (quantizer, n, x, idx);
        del_to_encode.set (to_encode);
    } else {
        to_encode = x;
    }
    pq.compute_codes (to_encode, xcodes, n);

    double t2 = getmillisecs ();
    // TODO: parallelize?
    size_t n_ignore = 0;
    for (size_t i = 0; i < n; i++) {
        idx_t key = idx[i];
        if (key < 0) {
            n_ignore ++;
            if (residuals_2)
                memset (residuals_2, 0, sizeof(*residuals_2) * d);
            continue;
        }
        idx_t id = xids ? xids[i] : ntotal + i;

        uint8_t *code = xcodes + i * code_size;
        size_t offset = invlists->add_entry (key, id, code);

        if (residuals_2) {
            float *res2 = residuals_2 + i * d;
            const float *xi = to_encode + i * d;
            pq.decode (code, res2);
            for (int j = 0; j < d; j++)
                res2[j] = xi[j] - res2[j];
        }

        if (maintain_direct_map)
            direct_map.push_back (key << 32 | offset);
    }


    double t3 = getmillisecs ();
    if(verbose) {
        char comment[100] = {0};
        if (n_ignore > 0)
            snprintf (comment, 100, "(%ld vectors ignored)", n_ignore);
        printf(" add_core times: %.3f %.3f %.3f %s\n",
               t1 - t0, t2 - t1, t3 - t2, comment);
    }
    ntotal += n;
}


void IndexIVFPQ::reconstruct_from_offset (long list_no, long offset,
                                          float* recons) const
{
    const uint8_t* code = invlists->get_single_code (list_no, offset);

    if (by_residual) {
        std::vector<float> centroid(d);
        quantizer->reconstruct (list_no, centroid.data());

        pq.decode (code, recons);
        for (int i = 0; i < d; ++i) {
            recons[i] += centroid[i];
        }
    } else {
        pq.decode (code, recons);
    }
}



/// 2G by default, accommodates tables up to PQ32 w/ 65536 centroids
size_t IndexIVFPQ::precomputed_table_max_bytes = ((size_t)1) << 31;

/** Precomputed tables for residuals
 *
 * During IVFPQ search with by_residual, we compute
 *
 *     d = || x - y_C - y_R ||^2
 *
 * where x is the query vector, y_C the coarse centroid, y_R the
 * refined PQ centroid. The expression can be decomposed as:
 *
 *    d = || x - y_C ||^2 + || y_R ||^2 + 2 * (y_C|y_R) - 2 * (x|y_R)
 *        ---------------   ---------------------------       -------
 *             term 1                 term 2                   term 3
 *
 * When using multiprobe, we use the following decomposition:
 * - term 1 is the distance to the coarse centroid, that is computed
 *   during the 1st stage search.
 * - term 2 can be precomputed, as it does not involve x. However,
 *   because of the PQ, it needs nlist * M * ksub storage. This is why
 *   use_precomputed_table is off by default
 * - term 3 is the classical non-residual distance table.
 *
 * Since y_R defined by a product quantizer, it is split across
 * subvectors and stored separately for each subvector. If the coarse
 * quantizer is a MultiIndexQuantizer then the table can be stored
 * more compactly.
 *
 * At search time, the tables for term 2 and term 3 are added up. This
 * is faster when the length of the lists is > ksub * M.
 */

void IndexIVFPQ::precompute_table ()
{
    if (use_precomputed_table == -1)
        return;

    if (use_precomputed_table == 0) { // then choose the type of table
        if (quantizer->metric_type == METRIC_INNER_PRODUCT) {
            if (verbose) {
                printf("IndexIVFPQ::precompute_table: precomputed "
                        "tables not needed for inner product quantizers\n");
            }
            return;
        }
        const MultiIndexQuantizer *miq =
            dynamic_cast<const MultiIndexQuantizer *> (quantizer);
        if (miq && pq.M % miq->pq.M == 0)
            use_precomputed_table = 2;
        else {
            size_t table_size = pq.M * pq.ksub * nlist * sizeof(float);
            if (table_size > precomputed_table_max_bytes) {
                if (verbose) {
                    printf(
                       "IndexIVFPQ::precompute_table: not precomputing table, "
                       "it would be too big: %ld bytes (max %ld)\n",
                       table_size, precomputed_table_max_bytes);
                    use_precomputed_table = 0;
                }
                return;
            }
            use_precomputed_table = 1;
        }
    } // otherwise assume user has set appropriate flag on input

    if (verbose) {
        printf ("precomputing IVFPQ tables type %d\n",
                use_precomputed_table);
    }

    // squared norms of the PQ centroids
    std::vector<float> r_norms (pq.M * pq.ksub, NAN);
    for (int m = 0; m < pq.M; m++)
        for (int j = 0; j < pq.ksub; j++)
            r_norms [m * pq.ksub + j] =
                fvec_norm_L2sqr (pq.get_centroids (m, j), pq.dsub);

    if (use_precomputed_table == 1) {

        precomputed_table.resize (nlist * pq.M * pq.ksub);
        std::vector<float> centroid (d);

        for (size_t i = 0; i < nlist; i++) {
            quantizer->reconstruct (i, centroid.data());

            float *tab = &precomputed_table[i * pq.M * pq.ksub];
            pq.compute_inner_prod_table (centroid.data(), tab);
            fvec_madd (pq.M * pq.ksub, r_norms.data(), 2.0, tab, tab);
        }
    } else if (use_precomputed_table == 2) {
        const MultiIndexQuantizer *miq =
           dynamic_cast<const MultiIndexQuantizer *> (quantizer);
        FAISS_THROW_IF_NOT (miq);
        const ProductQuantizer &cpq = miq->pq;
        FAISS_THROW_IF_NOT (pq.M % cpq.M == 0);

        precomputed_table.resize(cpq.ksub * pq.M * pq.ksub);

        // reorder PQ centroid table
        std::vector<float> centroids (d * cpq.ksub, NAN);

        for (int m = 0; m < cpq.M; m++) {
            for (size_t i = 0; i < cpq.ksub; i++) {
                memcpy (centroids.data() + i * d + m * cpq.dsub,
                        cpq.get_centroids (m, i),
                        sizeof (*centroids.data()) * cpq.dsub);
            }
        }

        pq.compute_inner_prod_tables (cpq.ksub, centroids.data (),
                                      precomputed_table.data ());

        for (size_t i = 0; i < cpq.ksub; i++) {
            float *tab = &precomputed_table[i * pq.M * pq.ksub];
            fvec_madd (pq.M * pq.ksub, r_norms.data(), 2.0, tab, tab);
        }

    }
}

namespace {

using idx_t = Index::idx_t;

static uint64_t get_cycles () {
#ifdef  __x86_64__
    uint32_t high, low;
    asm volatile("rdtsc \n\t"
                 : "=a" (low),
                   "=d" (high));
    return ((uint64_t)high << 32) | (low);
#else
    return 0;
#endif
}

#define TIC t0 = get_cycles()
#define TOC get_cycles () - t0



/** QueryTables manages the various ways of searching an
 * IndexIVFPQ. The code contains a lot of branches, depending on:
 * - metric_type: are we computing L2 or Inner product similarity?
 * - by_residual: do we encode raw vectors or residuals?
 * - use_precomputed_table: are x_R|x_C tables precomputed?
 * - polysemous_ht: are we filtering with polysemous codes?
 */
struct QueryTables {

    /*****************************************************
     * General data from the IVFPQ
     *****************************************************/

    const IndexIVFPQ & ivfpq;
    const IVFSearchParameters *params;

    // copied from IndexIVFPQ for easier access
    int d;
    const ProductQuantizer & pq;
    MetricType metric_type;
    bool by_residual;
    int use_precomputed_table;
    int polysemous_ht;

    // pre-allocated data buffers
    float * sim_table, * sim_table_2;
    float * residual_vec, *decoded_vec;

    // single data buffer
    std::vector<float> mem;

    // for table pointers
    std::vector<const float *> sim_table_ptrs;

    explicit QueryTables (const IndexIVFPQ & ivfpq,
                          const IVFSearchParameters *params):
        ivfpq(ivfpq),
        d(ivfpq.d),
        pq (ivfpq.pq),
        metric_type (ivfpq.metric_type),
        by_residual (ivfpq.by_residual),
        use_precomputed_table (ivfpq.use_precomputed_table)
    {
        mem.resize (pq.ksub * pq.M * 2 + d * 2);
        sim_table = mem.data ();
        sim_table_2 = sim_table + pq.ksub * pq.M;
        residual_vec = sim_table_2 + pq.ksub * pq.M;
        decoded_vec = residual_vec + d;

        // for polysemous
        polysemous_ht = ivfpq.polysemous_ht;
        if (auto ivfpq_params =
            dynamic_cast<const IVFPQSearchParameters *>(params)) {
            polysemous_ht = ivfpq_params->polysemous_ht;
        }
        if (polysemous_ht != 0)  {
            q_code.resize (pq.code_size);
        }
        init_list_cycles = 0;
        sim_table_ptrs.resize (pq.M);
    }

    /*****************************************************
     * What we do when query is known
     *****************************************************/

    // field specific to query
    const float * qi;

    // query-specific intialization
    void init_query (const float * qi) {
        this->qi = qi;
        if (metric_type == METRIC_INNER_PRODUCT)
            init_query_IP ();
        else
            init_query_L2 ();
        if (!by_residual && polysemous_ht != 0)
            pq.compute_code (qi, q_code.data());
    }

    void init_query_IP () {
        // precompute some tables specific to the query qi
        pq.compute_inner_prod_table (qi, sim_table);
    }

    void init_query_L2 () {
        if (!by_residual) {
            pq.compute_distance_table (qi, sim_table);
        } else if (use_precomputed_table) {
            pq.compute_inner_prod_table (qi, sim_table_2);
        }
    }

    /*****************************************************
     * When inverted list is known: prepare computations
     *****************************************************/

    // fields specific to list
    Index::idx_t key;
    float coarse_dis;
    std::vector<uint8_t> q_code;

    uint64_t init_list_cycles;

    /// once we know the query and the centroid, we can prepare the
    /// sim_table that will be used for accumulation
    /// and dis0, the initial value
    float precompute_list_tables () {
        float dis0 = 0;
        uint64_t t0; TIC;
        if (by_residual) {
            if (metric_type == METRIC_INNER_PRODUCT)
                dis0 = precompute_list_tables_IP ();
            else
                dis0 = precompute_list_tables_L2 ();
        }
        init_list_cycles += TOC;
        return dis0;
     }

    float precompute_list_table_pointers () {
        float dis0 = 0;
        uint64_t t0; TIC;
        if (by_residual) {
            if (metric_type == METRIC_INNER_PRODUCT)
              FAISS_THROW_MSG ("not implemented");
            else
              dis0 = precompute_list_table_pointers_L2 ();
        }
        init_list_cycles += TOC;
        return dis0;
     }

    /*****************************************************
     * compute tables for inner prod
     *****************************************************/

    float precompute_list_tables_IP ()
    {
        // prepare the sim_table that will be used for accumulation
        // and dis0, the initial value
        ivfpq.quantizer->reconstruct (key, decoded_vec);
        // decoded_vec = centroid
        float dis0 = fvec_inner_product (qi, decoded_vec, d);

        if (polysemous_ht) {
            for (int i = 0; i < d; i++) {
                residual_vec [i] = qi[i] - decoded_vec[i];
            }
            pq.compute_code (residual_vec, q_code.data());
        }
        return dis0;
    }


    /*****************************************************
     * compute tables for L2 distance
     *****************************************************/

    float precompute_list_tables_L2 ()
    {
        float dis0 = 0;

        if (use_precomputed_table == 0 || use_precomputed_table == -1) {
            ivfpq.quantizer->compute_residual (qi, residual_vec, key);
            pq.compute_distance_table (residual_vec, sim_table);

            if (polysemous_ht != 0) {
                pq.compute_code (residual_vec, q_code.data());
            }

        } else if (use_precomputed_table == 1) {
            dis0 = coarse_dis;

            fvec_madd (pq.M * pq.ksub,
                       &ivfpq.precomputed_table [key * pq.ksub * pq.M],
                       -2.0, sim_table_2,
                       sim_table);


            if (polysemous_ht != 0) {
                ivfpq.quantizer->compute_residual (qi, residual_vec, key);
                pq.compute_code (residual_vec, q_code.data());
            }

        } else if (use_precomputed_table == 2) {
            dis0 = coarse_dis;

            const MultiIndexQuantizer *miq =
                dynamic_cast<const MultiIndexQuantizer *> (ivfpq.quantizer);
            FAISS_THROW_IF_NOT (miq);
            const ProductQuantizer &cpq = miq->pq;
            int Mf = pq.M / cpq.M;

            const float *qtab = sim_table_2; // query-specific table
            float *ltab = sim_table; // (output) list-specific table

            long k = key;
            for (int cm = 0; cm < cpq.M; cm++) {
                // compute PQ index
                int ki = k & ((uint64_t(1) << cpq.nbits) - 1);
                k >>= cpq.nbits;

                // get corresponding table
                const float *pc = &ivfpq.precomputed_table
                    [(ki * pq.M + cm * Mf) * pq.ksub];

                if (polysemous_ht == 0) {

                    // sum up with query-specific table
                    fvec_madd (Mf * pq.ksub,
                               pc,
                               -2.0, qtab,
                               ltab);
                    ltab += Mf * pq.ksub;
                    qtab += Mf * pq.ksub;
                } else {
                    for (int m = cm * Mf; m < (cm + 1) * Mf; m++) {
                        q_code[m] = fvec_madd_and_argmin
                            (pq.ksub, pc, -2, qtab, ltab);
                        pc += pq.ksub;
                        ltab += pq.ksub;
                        qtab += pq.ksub;
                    }
                }

            }
        }

        return dis0;
    }

    float precompute_list_table_pointers_L2 ()
    {
        float dis0 = 0;

        if (use_precomputed_table == 1) {
            dis0 = coarse_dis;

            const float * s = &ivfpq.precomputed_table [key * pq.ksub * pq.M];
            for (int m = 0; m < pq.M; m++) {
                sim_table_ptrs [m] = s;
                s += pq.ksub;
            }
        } else if (use_precomputed_table == 2) {
            dis0 = coarse_dis;

            const MultiIndexQuantizer *miq =
                dynamic_cast<const MultiIndexQuantizer *> (ivfpq.quantizer);
            FAISS_THROW_IF_NOT (miq);
            const ProductQuantizer &cpq = miq->pq;
            int Mf = pq.M / cpq.M;

            long k = key;
            int m0 = 0;
            for (int cm = 0; cm < cpq.M; cm++) {
                int ki = k & ((uint64_t(1) << cpq.nbits) - 1);
                k >>= cpq.nbits;

                const float *pc = &ivfpq.precomputed_table
                    [(ki * pq.M + cm * Mf) * pq.ksub];

                for (int m = m0; m < m0 + Mf; m++) {
                    sim_table_ptrs [m] = pc;
                    pc += pq.ksub;
                }
                m0 += Mf;
            }
        } else {
          FAISS_THROW_MSG ("need precomputed tables");
        }

        if (polysemous_ht) {
            FAISS_THROW_MSG ("not implemented");
            // Not clear that it makes sense to implemente this,
            // because it costs M * ksub, which is what we wanted to
            // avoid with the tables pointers.
        }

        return dis0;
    }


};


/*****************************************************
 * Scaning the codes.
 * The scanning functions call their favorite precompute_*
 * function to precompute the tables they need.
 *****************************************************/
template <typename IDType, bool store_pairs, class C, MetricType METRIC_TYPE>
struct IVFPQScannerT: QueryTables {

    const uint8_t * list_codes;
    const IDType * list_ids;
    size_t list_size;

    explicit IVFPQScannerT (const IndexIVFPQ & ivfpq,
                            const IVFSearchParameters *params):
        QueryTables (ivfpq, params)
    {
        FAISS_THROW_IF_NOT (pq.byte_per_idx == 1);
        assert(METRIC_TYPE == metric_type);
    }

    float dis0;

    void init_list (idx_t list_no, float coarse_dis,
                      int mode) {
        this->key = list_no;
        this->coarse_dis = coarse_dis;

        if (mode == 2) {
            dis0 = precompute_list_tables ();
        } else if (mode == 1) {
            dis0 = precompute_list_table_pointers ();
        }
    }

    /*****************************************************
     * Scaning the codes: simple PQ scan.
     *****************************************************/

    /// version of the scan where we use precomputed tables
    size_t scan_list_with_table (
             size_t ncode, const uint8_t *codes, const idx_t *ids,
             size_t k, float * heap_sim, long * heap_ids) const
    {
        int nup = 0;
        for (size_t j = 0; j < ncode; j++) {

            float dis = dis0;
            const float *tab = sim_table;

            for (size_t m = 0; m < pq.M; m++) {
                dis += tab[*codes++];
                tab += pq.ksub;
            }

            if (C::cmp (heap_sim[0], dis)) {
                heap_pop<C> (k, heap_sim, heap_ids);
                long id = store_pairs ? (key << 32 | j) : ids[j];
                heap_push<C> (k, heap_sim, heap_ids, dis, id);
                nup++;
            }
        }
        return nup;
    }


    /// tables are not precomputed, but pointers are provided to the
    /// relevant X_c|x_r tables
    size_t scan_list_with_pointer (
             size_t ncode, const uint8_t *codes, const idx_t *ids,
             size_t k, float * heap_sim, long * heap_ids) const
    {
        size_t nup = 0;
        for (size_t j = 0; j < ncode; j++) {

            float dis = dis0;
            const float *tab = sim_table_2;

            for (size_t m = 0; m < pq.M; m++) {
                int ci = *codes++;
                dis += sim_table_ptrs [m][ci] - 2 * tab [ci];
                tab += pq.ksub;
            }

            if (C::cmp (heap_sim[0], dis)) {
                heap_pop<C> (k, heap_sim, heap_ids);
                long id = store_pairs ? (key << 32 | j) : ids[j];
                heap_push<C> (k, heap_sim, heap_ids, dis, id);
                nup++;
            }
        }
        return nup;
    }


    /// nothing is precomputed: access residuals on-the-fly
    size_t scan_on_the_fly_dist (
             size_t ncode, const uint8_t *codes, const idx_t *ids,
             size_t k, float * heap_sim, long * heap_ids) const
    {
        const float *dvec;
        float dis0 = 0;
        size_t nup = 0;
        if (by_residual) {
            if (METRIC_TYPE == METRIC_INNER_PRODUCT) {
                ivfpq.quantizer->reconstruct (key, residual_vec);
                dis0 = fvec_inner_product (residual_vec, qi, d);
            } else {
                ivfpq.quantizer->compute_residual (qi, residual_vec, key);
            }
            dvec = residual_vec;
        } else {
            dvec = qi;
            dis0 = 0;
        }

        for (size_t j = 0; j < ncode; j++) {

            pq.decode (codes, decoded_vec);
            codes += pq.code_size;

            float dis;
            if (METRIC_TYPE == METRIC_INNER_PRODUCT) {
                dis = dis0 + fvec_inner_product (decoded_vec, qi, d);
            } else {
                dis = fvec_L2sqr (decoded_vec, dvec, d);
            }

            if (C::cmp (heap_sim[0], dis)) {
                heap_pop<C> (k, heap_sim, heap_ids);
                long id = store_pairs ? (key << 32 | j) : ids[j];
                heap_push<C> (k, heap_sim, heap_ids, dis, id);
                nup++;
            }
        }
        return nup;
    }

    /*****************************************************
     * Scanning codes with polysemous filtering
     *****************************************************/

    template <class HammingComputer>
    size_t scan_list_polysemous_hc (
             size_t ncode, const uint8_t *codes, const idx_t *ids,
             size_t k, float * heap_sim, long * heap_ids) const
    {
        int ht = ivfpq.polysemous_ht;
        size_t n_hamming_pass = 0, nup = 0;

        int code_size = pq.code_size;

        HammingComputer hc (q_code.data(), code_size);

        for (size_t j = 0; j < ncode; j++) {
            const uint8_t *b_code = codes;
            int hd = hc.hamming (b_code);
            if (hd < ht) {
                n_hamming_pass ++;

                float dis = dis0;
                const float *tab = sim_table;

                for (size_t m = 0; m < pq.M; m++) {
                    dis += tab[*b_code++];
                    tab += pq.ksub;
                }

                if (C::cmp (heap_sim[0], dis)) {
                    heap_pop<C> (k, heap_sim, heap_ids);
                    long id = store_pairs ? (key << 32 | j) : ids[j];
                    heap_push<C> (k, heap_sim, heap_ids, dis, id);
                    nup++;
                }
            }
            codes += code_size;
        }
#pragma omp critical
        {
            indexIVFPQ_stats.n_hamming_pass += n_hamming_pass;
        }
        return nup;
    }

    size_t scan_list_polysemous (
             size_t ncode, const uint8_t *codes, const idx_t *ids,
             size_t k, float * heap_sim, long * heap_ids) const
    {
        switch (pq.code_size) {
#define HANDLE_CODE_SIZE(cs)                                            \
        case cs:                                                        \
            return scan_list_polysemous_hc <HammingComputer ## cs>      \
                (ncode, codes, ids, k, heap_sim, heap_ids);             \
            break
        HANDLE_CODE_SIZE(4);
        HANDLE_CODE_SIZE(8);
        HANDLE_CODE_SIZE(16);
        HANDLE_CODE_SIZE(20);
        HANDLE_CODE_SIZE(32);
        HANDLE_CODE_SIZE(64);
#undef HANDLE_CODE_SIZE
        default:
            if (pq.code_size % 8 == 0)
                return scan_list_polysemous_hc <HammingComputerM8>
                    (ncode, codes, ids, k, heap_sim, heap_ids);
            else
                return scan_list_polysemous_hc <HammingComputerM4>
                    (ncode, codes, ids, k, heap_sim, heap_ids);
            break;
        }
    }

};


/* We put as many parameters as possible in template. Hopefully the
 * gain in runtime is worth the code bloat. C is the comparator < or
 * >, it is directly related to METRIC_TYPE. precompute_mode is how
 * much we precompute (2 = precompute distance tables, 1 = precompute
 * pointers to distances, 0 = compute distances one by one). Currently
 * only 2 is supported. */
template<MetricType METRIC_TYPE, bool store_pairs, class C,
         int precompute_mode>
struct IVFPQScanner:
    IVFPQScannerT<Index::idx_t, store_pairs, C, METRIC_TYPE>,
    InvertedListScanner
{

    IVFPQScanner(const IndexIVFPQ & ivfpq):
        IVFPQScannerT<Index::idx_t, store_pairs, C, METRIC_TYPE>(ivfpq, nullptr)
    {
    }

    void set_query (const float *query) override {
        this->init_query (query);
    }

    void set_list (idx_t list_no, float coarse_dis) override {
        this->init_list (list_no, coarse_dis, precompute_mode);
    }

    float distance_to_code (const uint8_t *code) const override {
        assert(precompute_mode == 2);
        float dis = this->dis0;
        const float *tab = this->sim_table;

        for (size_t m = 0; m < this->pq.M; m++) {
            dis += tab[*code++];
            tab += this->pq.ksub;
        }
        return dis;
    }

    size_t scan_codes (size_t ncode,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *heap_sim, idx_t *heap_ids,
                       size_t k) const override
    {
        if (this->polysemous_ht > 0) {
            assert(precompute_mode == 2);
            this->scan_list_polysemous
                (ncode, codes, ids, k, heap_sim, heap_ids);
        } else if (precompute_mode == 2) {
            this->scan_list_with_table
                (ncode, codes, ids, k, heap_sim, heap_ids);
        } else if (precompute_mode == 1) {
            this->scan_list_with_pointer
                (ncode, codes, ids, k, heap_sim, heap_ids);
        } else if (precompute_mode == 0) {
            this->scan_on_the_fly_dist
                (ncode, codes, ids, k, heap_sim, heap_ids);
        } else {
            FAISS_THROW_MSG("bad precomp mode");
        }
        return 0;
    }

};




} // anonymous namespace

InvertedListScanner *
IndexIVFPQ::get_InvertedListScanner (bool store_pairs) const
{
    if (metric_type == METRIC_INNER_PRODUCT) {
        if (store_pairs) {
            return new IVFPQScanner<
                METRIC_INNER_PRODUCT, true, CMin<float, long>, 2> (*this);
        } else {
            return new IVFPQScanner<
                METRIC_INNER_PRODUCT, false, CMin<float, long>, 2>(*this);
        }
    } else if (metric_type == METRIC_L2) {
        if (store_pairs) {
            return new IVFPQScanner<
                METRIC_L2, true, CMax<float, long>, 2> (*this);
        } else {
            return new IVFPQScanner<
                METRIC_L2, false, CMax<float, long>, 2>(*this);
        }
    }
    return nullptr;

}



IndexIVFPQStats indexIVFPQ_stats;

void IndexIVFPQStats::reset () {
    memset (this, 0, sizeof (*this));
}



IndexIVFPQ::IndexIVFPQ ()
{
    // initialize some runtime values
    use_precomputed_table = 0;
    scan_table_threshold = 0;
    do_polysemous_training = false;
    polysemous_ht = 0;
    polysemous_training = nullptr;
}


struct CodeCmp {
    const uint8_t *tab;
    size_t code_size;
    bool operator () (int a, int b) const {
        return cmp (a, b) > 0;
    }
    int cmp (int a, int b) const {
        return memcmp (tab + a * code_size, tab + b * code_size,
                       code_size);
    }
};


size_t IndexIVFPQ::find_duplicates (idx_t *dup_ids, size_t *lims) const
{
    size_t ngroup = 0;
    lims[0] = 0;
    for (size_t list_no = 0; list_no < nlist; list_no++) {
        size_t n = invlists->list_size (list_no);
        std::vector<int> ord (n);
        for (int i = 0; i < n; i++) ord[i] = i;
        InvertedLists::ScopedCodes codes (invlists, list_no);
        CodeCmp cs = { codes.get(), code_size };
        std::sort (ord.begin(), ord.end(), cs);

        InvertedLists::ScopedIds list_ids (invlists, list_no);
        int prev = -1;  // all elements from prev to i-1 are equal
        for (int i = 0; i < n; i++) {
            if (prev >= 0 && cs.cmp (ord [prev], ord [i]) == 0) {
                // same as previous => remember
                if (prev + 1 == i) { // start new group
                    ngroup++;
                    lims[ngroup] = lims[ngroup - 1];
                    dup_ids [lims [ngroup]++] = list_ids [ord [prev]];
                }
                dup_ids [lims [ngroup]++] = list_ids [ord [i]];
            } else { // not same as previous.
                prev = i;
            }
        }
    }
    return ngroup;
}




/*****************************************
 * IndexIVFPQR implementation
 ******************************************/

IndexIVFPQR::IndexIVFPQR (
            Index * quantizer, size_t d, size_t nlist,
            size_t M, size_t nbits_per_idx,
            size_t M_refine, size_t nbits_per_idx_refine):
    IndexIVFPQ (quantizer, d, nlist, M, nbits_per_idx),
    refine_pq (d, M_refine, nbits_per_idx_refine),
    k_factor (4)
{
    by_residual = true;
}

IndexIVFPQR::IndexIVFPQR ():
    k_factor (1)
{
    by_residual = true;
}



void IndexIVFPQR::reset()
{
    IndexIVFPQ::reset();
    refine_codes.clear();
}




void IndexIVFPQR::train_residual (idx_t n, const float *x)
{

    float * residual_2 = new float [n * d];
    ScopeDeleter <float> del(residual_2);

    train_residual_o (n, x, residual_2);

    if (verbose)
        printf ("training %zdx%zd 2nd level PQ quantizer on %ld %dD-vectors\n",
                refine_pq.M, refine_pq.ksub, n, d);

    refine_pq.cp.max_points_per_centroid = 1000;
    refine_pq.cp.verbose = verbose;

    refine_pq.train (n, residual_2);

}


void IndexIVFPQR::add_with_ids (idx_t n, const float *x, const long *xids) {
    add_core (n, x, xids, nullptr);
}

void IndexIVFPQR::add_core (idx_t n, const float *x, const long *xids,
                                const long *precomputed_idx) {

    float * residual_2 = new float [n * d];
    ScopeDeleter <float> del(residual_2);

    idx_t n0 = ntotal;

    add_core_o (n, x, xids, residual_2, precomputed_idx);

    refine_codes.resize (ntotal * refine_pq.code_size);

    refine_pq.compute_codes (
        residual_2, &refine_codes[n0 * refine_pq.code_size], n);


}


void IndexIVFPQR::search_preassigned (idx_t n, const float *x, idx_t k,
                                      const idx_t *idx,
                                      const float *L1_dis,
                                      float *distances, idx_t *labels,
                                      bool store_pairs,
                                      const IVFSearchParameters *params
                                      ) const
{
    uint64_t t0;
    TIC;
    size_t k_coarse = long(k * k_factor);
    idx_t *coarse_labels = new idx_t [k_coarse * n];
    ScopeDeleter<idx_t> del1 (coarse_labels);
    { // query with quantizer levels 1 and 2.
        float *coarse_distances = new float [k_coarse * n];
        ScopeDeleter<float> del(coarse_distances);

        IndexIVFPQ::search_preassigned (
                   n, x, k_coarse,
                   idx, L1_dis, coarse_distances, coarse_labels,
                   true, params);
    }


    indexIVFPQ_stats.search_cycles += TOC;

    TIC;

    // 3rd level refinement
    size_t n_refine = 0;
#pragma omp parallel reduction(+ : n_refine)
    {
        // tmp buffers
        float *residual_1 = new float [2 * d];
        ScopeDeleter<float> del (residual_1);
        float *residual_2 = residual_1 + d;
#pragma omp for
        for (idx_t i = 0; i < n; i++) {
            const float *xq = x + i * d;
            const long * shortlist = coarse_labels + k_coarse * i;
            float * heap_sim = distances + k * i;
            long * heap_ids = labels + k * i;
            maxheap_heapify (k, heap_sim, heap_ids);

            for (int j = 0; j < k_coarse; j++) {
                long sl = shortlist[j];

                if (sl == -1) continue;

                int list_no = sl >> 32;
                int ofs = sl & 0xffffffff;

                assert (list_no >= 0 && list_no < nlist);
                assert (ofs >= 0 && ofs < invlists->list_size (list_no));

                // 1st level residual
                quantizer->compute_residual (xq, residual_1, list_no);

                // 2nd level residual
                const uint8_t * l2code =
                    invlists->get_single_code (list_no, ofs);

                pq.decode (l2code, residual_2);
                for (int l = 0; l < d; l++)
                    residual_2[l] = residual_1[l] - residual_2[l];

                // 3rd level residual's approximation
                idx_t id = invlists->get_single_id (list_no, ofs);
                assert (0 <= id && id < ntotal);
                refine_pq.decode (&refine_codes [id * refine_pq.code_size],
                                  residual_1);

                float dis = fvec_L2sqr (residual_1, residual_2, d);

                if (dis < heap_sim[0]) {
                    maxheap_pop (k, heap_sim, heap_ids);
                    long id_or_pair = store_pairs ? sl : id;
                    maxheap_push (k, heap_sim, heap_ids, dis, id_or_pair);
                }
                n_refine ++;
            }
            maxheap_reorder (k, heap_sim, heap_ids);
        }
    }
    indexIVFPQ_stats.nrefine += n_refine;
    indexIVFPQ_stats.refine_cycles += TOC;
}

void IndexIVFPQR::reconstruct_from_offset (long list_no, long offset,
                                           float* recons) const
{
    IndexIVFPQ::reconstruct_from_offset (list_no, offset, recons);

    idx_t id = invlists->get_single_id (list_no, offset);
    assert (0 <= id && id < ntotal);

    std::vector<float> r3(d);
    refine_pq.decode (&refine_codes [id * refine_pq.code_size], r3.data());
    for (int i = 0; i < d; ++i) {
      recons[i] += r3[i];
    }
}

void IndexIVFPQR::merge_from (IndexIVF &other_in, idx_t add_id)
{
    IndexIVFPQR *other = dynamic_cast<IndexIVFPQR *> (&other_in);
    FAISS_THROW_IF_NOT(other);

    IndexIVF::merge_from (other_in, add_id);

    refine_codes.insert (refine_codes.end(),
                         other->refine_codes.begin(),
                         other->refine_codes.end());
    other->refine_codes.clear();
}

long IndexIVFPQR::remove_ids(const IDSelector& /*sel*/) {
  FAISS_THROW_MSG("not implemented");
  return 0;
}

/*************************************
 * Index2Layer implementation
 *************************************/


Index2Layer::Index2Layer (Index * quantizer, size_t nlist,
                          int M,
                          MetricType metric):
    Index (quantizer->d, metric),
    q1 (quantizer, nlist),
    pq (quantizer->d, M, 8)
{
    is_trained = false;
    for (int nbyte = 0; nbyte < 7; nbyte++) {
        if ((1L << (8 * nbyte)) >= nlist) {
            code_size_1 = nbyte;
            break;
        }
    }
    code_size_2 = pq.code_size;
    code_size = code_size_1 + code_size_2;
}

Index2Layer::Index2Layer ()
{
    code_size = code_size_1 = code_size_2 = 0;
}

Index2Layer::~Index2Layer ()
{}

void Index2Layer::train(idx_t n, const float* x)
{
    if (verbose) {
        printf ("training level-1 quantizer %ld vectors in %dD\n",
                n, d);
    }

    q1.train_q1 (n, x, verbose, metric_type);

    if (verbose) {
        printf("computing residuals\n");
    }

    const float * x_in = x;

    x = fvecs_maybe_subsample (
         d, (size_t*)&n, pq.cp.max_points_per_centroid * pq.ksub,
         x, verbose, pq.cp.seed);

    ScopeDeleter<float> del_x (x_in == x ? nullptr : x);

    std::vector<idx_t> assign(n); // assignement to coarse centroids
    q1.quantizer->assign (n, x, assign.data());
    std::vector<float> residuals(n * d);
    for (idx_t i = 0; i < n; i++) {
        q1.quantizer->compute_residual (
           x + i * d, residuals.data() + i * d, assign[i]);
    }

    if (verbose)
        printf ("training %zdx%zd product quantizer on %ld vectors in %dD\n",
                pq.M, pq.ksub, n, d);
    pq.verbose = verbose;
    pq.train (n, residuals.data());

    is_trained = true;
}

void Index2Layer::add(idx_t n, const float* x)
{
    idx_t bs = 32768;
    if (n > bs) {
        for (idx_t i0 = 0; i0 < n; i0 += bs) {
            idx_t i1 = std::min(i0 + bs, n);
            if (verbose) {
                printf("Index2Layer::add: adding %ld:%ld / %ld\n",
                       i0, i1, n);
            }
            add (i1 - i0, x + i0 * d);
        }
        return;
    }

    std::vector<idx_t> codes1 (n);
    q1.quantizer->assign (n, x, codes1.data());
    std::vector<float> residuals(n * d);
    for (idx_t i = 0; i < n; i++) {
        q1.quantizer->compute_residual (
            x + i * d, residuals.data() + i * d, codes1[i]);
    }
    std::vector<uint8_t> codes2 (n * code_size_2);

    pq.compute_codes (residuals.data(), codes2.data(), n);

    codes.resize ((ntotal + n) * code_size);
    uint8_t *wp = &codes[ntotal * code_size];

    {
        int i = 0x11223344;
        const char *ip = (char*)&i;
        FAISS_THROW_IF_NOT_MSG (ip[0] == 0x44,
                                "works only on a little-endian CPU");
    }

    // copy to output table
    for (idx_t i = 0; i < n; i++) {
        memcpy (wp, &codes1[i], code_size_1);
        wp += code_size_1;
        memcpy (wp, &codes2[i * code_size_2], code_size_2);
        wp += code_size_2;
    }

    ntotal += n;

}

void Index2Layer::search(
    idx_t /*n*/,
    const float* /*x*/,
    idx_t /*k*/,
    float* /*distances*/,
    idx_t* /*labels*/) const {
  FAISS_THROW_MSG("not implemented");
}


void Index2Layer::reconstruct_n(idx_t i0, idx_t ni, float* recons) const
{
    float recons1[d];
    FAISS_THROW_IF_NOT (i0 >= 0 && i0 + ni <= ntotal);
    const uint8_t *rp = &codes[i0 * code_size];

    for (idx_t i = 0; i < ni; i++) {
        idx_t key = 0;
        memcpy (&key, rp, code_size_1);
        q1.quantizer->reconstruct (key, recons1);
        rp += code_size_1;
        pq.decode (rp, recons);
        for (idx_t j = 0; j < d; j++) {
            recons[j] += recons1[j];
        }
        rp += code_size_2;
        recons += d;
    }
}

void Index2Layer::transfer_to_IVFPQ (IndexIVFPQ & other) const
{
    FAISS_THROW_IF_NOT (other.nlist == q1.nlist);
    FAISS_THROW_IF_NOT (other.code_size == code_size_2);
    FAISS_THROW_IF_NOT (other.ntotal == 0);

    const uint8_t *rp = codes.data();

    for (idx_t i = 0; i < ntotal; i++) {
        idx_t key = 0;
        memcpy (&key, rp, code_size_1);
        rp += code_size_1;
        other.invlists->add_entry (key, i, rp);
        rp += code_size_2;
    }

    other.ntotal = ntotal;

}



void Index2Layer::reconstruct(idx_t key, float* recons) const
{
    reconstruct_n (key, 1, recons);
}

void Index2Layer::reset()
{
    ntotal = 0;
    codes.clear ();
}


} // namespace faiss
