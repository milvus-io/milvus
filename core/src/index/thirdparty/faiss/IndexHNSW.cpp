/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexHNSW.h>


#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio>
#include <cmath>
#include <omp.h>

#include <unordered_set>
#include <queue>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>

#ifdef __SSE__
#endif

#include <faiss/utils/distances.h>
#include <faiss/utils/random.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/Index2Layer.h>
#include <faiss/impl/AuxIndexStructures.h>


extern "C" {

/* declare BLAS functions, see http://www.netlib.org/clapack/cblas/ */

int sgemm_ (const char *transa, const char *transb, FINTEGER *m, FINTEGER *
            n, FINTEGER *k, const float *alpha, const float *a,
            FINTEGER *lda, const float *b, FINTEGER *
            ldb, float *beta, float *c, FINTEGER *ldc);

}

namespace faiss {

using idx_t = Index::idx_t;
using MinimaxHeap = HNSW::MinimaxHeap;
using storage_idx_t = HNSW::storage_idx_t;
using NodeDistFarther = HNSW::NodeDistFarther;

HNSWStats hnsw_stats;

/**************************************************************
 * add / search blocks of descriptors
 **************************************************************/

namespace {


/* Wrap the distance computer into one that negates the
   distances. This makes supporting INNER_PRODUCE search easier */

struct NegativeDistanceComputer: DistanceComputer {

    /// owned by this
    DistanceComputer *basedis;

    explicit NegativeDistanceComputer(DistanceComputer *basedis):
        basedis(basedis)
    {}

    void set_query(const float *x) override {
        basedis->set_query(x);
    }

     /// compute distance of vector i to current query
    float operator () (idx_t i) override {
        return -(*basedis)(i);
    }

     /// compute distance between two stored vectors
    float symmetric_dis (idx_t i, idx_t j) override {
        return -basedis->symmetric_dis(i, j);
    }

    virtual ~NegativeDistanceComputer ()
    {
        delete basedis;
    }

};

DistanceComputer *storage_distance_computer(const Index *storage)
{
    if (storage->metric_type == METRIC_INNER_PRODUCT) {
        return new NegativeDistanceComputer(storage->get_distance_computer());
    } else {
        return storage->get_distance_computer();
    }
}



void hnsw_add_vertices(IndexHNSW &index_hnsw,
                       size_t n0,
                       size_t n, const float *x,
                       bool verbose,
                       bool preset_levels = false) {
    size_t d = index_hnsw.d;
    HNSW & hnsw = index_hnsw.hnsw;
    size_t ntotal = n0 + n;
    double t0 = getmillisecs();
    if (verbose) {
        printf("hnsw_add_vertices: adding %ld elements on top of %ld "
               "(preset_levels=%d)\n",
               n, n0, int(preset_levels));
    }

    if (n == 0) {
        return;
    }

    int max_level = hnsw.prepare_level_tab(n, preset_levels);

    if (verbose) {
        printf("  max_level = %d\n", max_level);
    }

    std::vector<omp_lock_t> locks(ntotal);
    for(int i = 0; i < ntotal; i++)
        omp_init_lock(&locks[i]);

    // add vectors from highest to lowest level
    std::vector<int> hist;
    std::vector<int> order(n);

    { // make buckets with vectors of the same level

        // build histogram
        for (int i = 0; i < n; i++) {
            storage_idx_t pt_id = i + n0;
            int pt_level = hnsw.levels[pt_id] - 1;
            while (pt_level >= hist.size())
                hist.push_back(0);
            hist[pt_level] ++;
        }

        // accumulate
        std::vector<int> offsets(hist.size() + 1, 0);
        for (int i = 0; i < hist.size() - 1; i++) {
            offsets[i + 1] = offsets[i] + hist[i];
        }

        // bucket sort
        for (int i = 0; i < n; i++) {
            storage_idx_t pt_id = i + n0;
            int pt_level = hnsw.levels[pt_id] - 1;
            order[offsets[pt_level]++] = pt_id;
        }
    }

    idx_t check_period = InterruptCallback::get_period_hint
        (max_level * index_hnsw.d * hnsw.efConstruction);

    { // perform add
        RandomGenerator rng2(789);

        int i1 = n;

        for (int pt_level = hist.size() - 1; pt_level >= 0; pt_level--) {
            int i0 = i1 - hist[pt_level];

            if (verbose) {
                printf("Adding %d elements at level %d\n",
                       i1 - i0, pt_level);
            }

            // random permutation to get rid of dataset order bias
            for (int j = i0; j < i1; j++)
                std::swap(order[j], order[j + rng2.rand_int(i1 - j)]);

            bool interrupt = false;

#pragma omp parallel if(i1 > i0 + 100)
            {
                VisitedTable vt (ntotal);

                DistanceComputer *dis =
                    storage_distance_computer (index_hnsw.storage);
                ScopeDeleter1<DistanceComputer> del(dis);
                int prev_display = verbose && omp_get_thread_num() == 0 ? 0 : -1;
                size_t counter = 0;

#pragma omp  for schedule(dynamic)
                for (int i = i0; i < i1; i++) {
                    storage_idx_t pt_id = order[i];
                    dis->set_query (x + (pt_id - n0) * d);

                    // cannot break
                    if (interrupt) {
                        continue;
                    }

                    hnsw.add_with_locks(*dis, pt_level, pt_id, locks, vt);

                    if (prev_display >= 0 && i - i0 > prev_display + 10000) {
                        prev_display = i - i0;
                        printf("  %d / %d\r", i - i0, i1 - i0);
                        fflush(stdout);
                    }

                    if (counter % check_period == 0) {
                        if (InterruptCallback::is_interrupted ()) {
                            interrupt = true;
                        }
                    }
                    counter++;
                }

            }
            if (interrupt) {
                FAISS_THROW_MSG ("computation interrupted");
            }
            i1 = i0;
        }
        FAISS_ASSERT(i1 == 0);
    }
    if (verbose) {
        printf("Done in %.3f ms\n", getmillisecs() - t0);
    }

    for(int i = 0; i < ntotal; i++) {
        omp_destroy_lock(&locks[i]);
    }
}


}  // namespace




/**************************************************************
 * IndexHNSW implementation
 **************************************************************/

IndexHNSW::IndexHNSW(int d, int M, MetricType metric):
    Index(d, metric),
    hnsw(M),
    own_fields(false),
    storage(nullptr),
    reconstruct_from_neighbors(nullptr)
{}

IndexHNSW::IndexHNSW(Index *storage, int M):
    Index(storage->d, storage->metric_type),
    hnsw(M),
    own_fields(false),
    storage(storage),
    reconstruct_from_neighbors(nullptr)
{}

IndexHNSW::~IndexHNSW() {
    if (own_fields) {
        delete storage;
    }
}

void IndexHNSW::train(idx_t n, const float* x)
{
    FAISS_THROW_IF_NOT_MSG(storage,
       "Please use IndexHSNWFlat (or variants) instead of IndexHNSW directly");
    // hnsw structure does not require training
    storage->train (n, x);
    is_trained = true;
}

void IndexHNSW::search (idx_t n, const float *x, idx_t k,
                        float *distances, idx_t *labels) const

{
    FAISS_THROW_IF_NOT_MSG(storage,
       "Please use IndexHSNWFlat (or variants) instead of IndexHNSW directly");
    size_t nreorder = 0;

    idx_t check_period = InterruptCallback::get_period_hint (
          hnsw.max_level * d * hnsw.efSearch);

    for (idx_t i0 = 0; i0 < n; i0 += check_period) {
        idx_t i1 = std::min(i0 + check_period, n);

#pragma omp parallel reduction(+ : nreorder)
        {
            VisitedTable vt (ntotal);

            DistanceComputer *dis = storage_distance_computer(storage);
            ScopeDeleter1<DistanceComputer> del(dis);

#pragma omp for
            for(idx_t i = i0; i < i1; i++) {
                idx_t * idxi = labels + i * k;
                float * simi = distances + i * k;
                dis->set_query(x + i * d);

                maxheap_heapify (k, simi, idxi);
                hnsw.search(*dis, k, idxi, simi, vt);

                maxheap_reorder (k, simi, idxi);

                if (reconstruct_from_neighbors &&
                    reconstruct_from_neighbors->k_reorder != 0) {
                    int k_reorder = reconstruct_from_neighbors->k_reorder;
                    if (k_reorder == -1 || k_reorder > k) k_reorder = k;

                    nreorder += reconstruct_from_neighbors->compute_distances(
                             k_reorder, idxi, x + i * d, simi);

                    // sort top k_reorder
                    maxheap_heapify (k_reorder, simi, idxi, simi, idxi, k_reorder);
                    maxheap_reorder (k_reorder, simi, idxi);
                }

            }

        }
        InterruptCallback::check ();
    }

    if (metric_type == METRIC_INNER_PRODUCT) {
        // we need to revert the negated distances
        for (size_t i = 0; i < k * n; i++) {
            distances[i] = -distances[i];
        }
    }

    hnsw_stats.nreorder += nreorder;
}


void IndexHNSW::add(idx_t n, const float *x)
{
    FAISS_THROW_IF_NOT_MSG(storage,
       "Please use IndexHSNWFlat (or variants) instead of IndexHNSW directly");
    FAISS_THROW_IF_NOT(is_trained);
    int n0 = ntotal;
    storage->add(n, x);
    ntotal = storage->ntotal;

    hnsw_add_vertices (*this, n0, n, x, verbose,
                       hnsw.levels.size() == ntotal);
}

void IndexHNSW::reset()
{
    hnsw.reset();
    storage->reset();
    ntotal = 0;
}

void IndexHNSW::reconstruct (idx_t key, float* recons) const
{
    storage->reconstruct(key, recons);
}

void IndexHNSW::shrink_level_0_neighbors(int new_size)
{
#pragma omp parallel
    {
        DistanceComputer *dis = storage_distance_computer(storage);
        ScopeDeleter1<DistanceComputer> del(dis);

#pragma omp for
        for (idx_t i = 0; i < ntotal; i++) {

            size_t begin, end;
            hnsw.neighbor_range(i, 0, &begin, &end);

            std::priority_queue<NodeDistFarther> initial_list;

            for (size_t j = begin; j < end; j++) {
                int v1 = hnsw.neighbors[j];
                if (v1 < 0) break;
                initial_list.emplace(dis->symmetric_dis(i, v1), v1);

                // initial_list.emplace(qdis(v1), v1);
            }

            std::vector<NodeDistFarther> shrunk_list;
            HNSW::shrink_neighbor_list(*dis, initial_list,
                                       shrunk_list, new_size);

            for (size_t j = begin; j < end; j++) {
                if (j - begin < shrunk_list.size())
                    hnsw.neighbors[j] = shrunk_list[j - begin].id;
                else
                    hnsw.neighbors[j] = -1;
            }
        }
    }

}

void IndexHNSW::search_level_0(
    idx_t n, const float *x, idx_t k,
    const storage_idx_t *nearest, const float *nearest_d,
    float *distances, idx_t *labels, int nprobe,
    int search_type) const
{

    storage_idx_t ntotal = hnsw.levels.size();
#pragma omp parallel
    {
        DistanceComputer *qdis = storage_distance_computer(storage);
        ScopeDeleter1<DistanceComputer> del(qdis);

        VisitedTable vt (ntotal);

#pragma omp for
        for(idx_t i = 0; i < n; i++) {
            idx_t * idxi = labels + i * k;
            float * simi = distances + i * k;

            qdis->set_query(x + i * d);
            maxheap_heapify (k, simi, idxi);

            if (search_type == 1) {

                int nres = 0;

                for(int j = 0; j < nprobe; j++) {
                    storage_idx_t cj = nearest[i * nprobe + j];

                    if (cj < 0) break;

                    if (vt.get(cj)) continue;

                    int candidates_size = std::max(hnsw.efSearch, int(k));
                    MinimaxHeap candidates(candidates_size);

                    candidates.push(cj, nearest_d[i * nprobe + j]);

                    nres = hnsw.search_from_candidates(
                      *qdis, k, idxi, simi,
                      candidates, vt, 0, nres
                    );
                }
            } else if (search_type == 2) {

                int candidates_size = std::max(hnsw.efSearch, int(k));
                candidates_size = std::max(candidates_size, nprobe);

                MinimaxHeap candidates(candidates_size);
                for(int j = 0; j < nprobe; j++) {
                    storage_idx_t cj = nearest[i * nprobe + j];

                    if (cj < 0) break;
                    candidates.push(cj, nearest_d[i * nprobe + j]);
                }
                hnsw.search_from_candidates(
                  *qdis, k, idxi, simi,
                  candidates, vt, 0
                );

            }
            vt.advance();

            maxheap_reorder (k, simi, idxi);

        }
    }


}

void IndexHNSW::init_level_0_from_knngraph(
       int k, const float *D, const idx_t *I)
{
    int dest_size = hnsw.nb_neighbors (0);

#pragma omp parallel for
    for (idx_t i = 0; i < ntotal; i++) {
        DistanceComputer *qdis = storage_distance_computer(storage);
        float vec[d];
        storage->reconstruct(i, vec);
        qdis->set_query(vec);

        std::priority_queue<NodeDistFarther> initial_list;

        for (size_t j = 0; j < k; j++) {
            int v1 = I[i * k + j];
            if (v1 == i) continue;
            if (v1 < 0) break;
            initial_list.emplace(D[i * k + j], v1);
        }

        std::vector<NodeDistFarther> shrunk_list;
        HNSW::shrink_neighbor_list(*qdis, initial_list, shrunk_list, dest_size);

        size_t begin, end;
        hnsw.neighbor_range(i, 0, &begin, &end);

        for (size_t j = begin; j < end; j++) {
            if (j - begin < shrunk_list.size())
                hnsw.neighbors[j] = shrunk_list[j - begin].id;
            else
                hnsw.neighbors[j] = -1;
        }
    }
}



void IndexHNSW::init_level_0_from_entry_points(
          int n, const storage_idx_t *points,
          const storage_idx_t *nearests)
{

    std::vector<omp_lock_t> locks(ntotal);
    for(int i = 0; i < ntotal; i++)
        omp_init_lock(&locks[i]);

#pragma omp parallel
    {
        VisitedTable vt (ntotal);

        DistanceComputer *dis = storage_distance_computer(storage);
        ScopeDeleter1<DistanceComputer> del(dis);
        float vec[storage->d];

#pragma omp  for schedule(dynamic)
        for (int i = 0; i < n; i++) {
            storage_idx_t pt_id = points[i];
            storage_idx_t nearest = nearests[i];
            storage->reconstruct (pt_id, vec);
            dis->set_query (vec);

            hnsw.add_links_starting_from(*dis, pt_id,
                                         nearest, (*dis)(nearest),
                                         0, locks.data(), vt);

            if (verbose && i % 10000 == 0) {
                printf("  %d / %d\r", i, n);
                fflush(stdout);
            }
        }
    }
    if (verbose) {
        printf("\n");
    }

    for(int i = 0; i < ntotal; i++)
        omp_destroy_lock(&locks[i]);
}

void IndexHNSW::reorder_links()
{
    int M = hnsw.nb_neighbors(0);

#pragma omp parallel
    {
        std::vector<float> distances (M);
        std::vector<size_t> order (M);
        std::vector<storage_idx_t> tmp (M);
        DistanceComputer *dis = storage_distance_computer(storage);
        ScopeDeleter1<DistanceComputer> del(dis);

#pragma omp for
        for(storage_idx_t i = 0; i < ntotal; i++) {

            size_t begin, end;
            hnsw.neighbor_range(i, 0, &begin, &end);

            for (size_t j = begin; j < end; j++) {
                storage_idx_t nj = hnsw.neighbors[j];
                if (nj < 0) {
                    end = j;
                    break;
                }
                distances[j - begin] = dis->symmetric_dis(i, nj);
                tmp [j - begin] = nj;
            }

            fvec_argsort (end - begin, distances.data(), order.data());
            for (size_t j = begin; j < end; j++) {
                hnsw.neighbors[j] = tmp[order[j - begin]];
            }
        }

    }
}


void IndexHNSW::link_singletons()
{
    printf("search for singletons\n");

    std::vector<bool> seen(ntotal);

    for (size_t i = 0; i < ntotal; i++) {
        size_t begin, end;
        hnsw.neighbor_range(i, 0, &begin, &end);
        for (size_t j = begin; j < end; j++) {
            storage_idx_t ni = hnsw.neighbors[j];
            if (ni >= 0) seen[ni] = true;
        }
    }

    int n_sing = 0, n_sing_l1 = 0;
    std::vector<storage_idx_t> singletons;
    for (storage_idx_t i = 0; i < ntotal; i++) {
        if (!seen[i]) {
            singletons.push_back(i);
            n_sing++;
            if (hnsw.levels[i] > 1)
                n_sing_l1++;
        }
    }

    printf("  Found %d / %ld singletons (%d appear in a level above)\n",
           n_sing, ntotal, n_sing_l1);

    std::vector<float>recons(singletons.size() * d);
    for (int i = 0; i < singletons.size(); i++) {

        FAISS_ASSERT(!"not implemented");

    }


}


/**************************************************************
 * ReconstructFromNeighbors implementation
 **************************************************************/


ReconstructFromNeighbors::ReconstructFromNeighbors(
             const IndexHNSW & index, size_t k, size_t nsq):
    index(index), k(k), nsq(nsq) {
    M = index.hnsw.nb_neighbors(0);
    FAISS_ASSERT(k <= 256);
    code_size = k == 1 ? 0 : nsq;
    ntotal = 0;
    d = index.d;
    FAISS_ASSERT(d % nsq == 0);
    dsub = d / nsq;
    k_reorder = -1;
}

void ReconstructFromNeighbors::reconstruct(storage_idx_t i, float *x, float *tmp) const
{


    const HNSW & hnsw = index.hnsw;
    size_t begin, end;
    hnsw.neighbor_range(i, 0, &begin, &end);

    if (k == 1 || nsq == 1) {
        const float * beta;
        if (k == 1) {
            beta = codebook.data();
        } else {
            int idx = codes[i];
            beta = codebook.data() + idx * (M + 1);
        }

        float w0 = beta[0]; // weight of image itself
        index.storage->reconstruct(i, tmp);

        for (int l = 0; l < d; l++)
            x[l] = w0 * tmp[l];

        for (size_t j = begin; j < end; j++) {

            storage_idx_t ji = hnsw.neighbors[j];
            if (ji < 0) ji = i;
            float w = beta[j - begin + 1];
            index.storage->reconstruct(ji, tmp);
            for (int l = 0; l < d; l++)
                x[l] += w * tmp[l];
        }
    } else if (nsq == 2) {
        int idx0 = codes[2 * i];
        int idx1 = codes[2 * i + 1];

        const float *beta0 = codebook.data() +  idx0 * (M + 1);
        const float *beta1 = codebook.data() + (idx1 + k) * (M + 1);

        index.storage->reconstruct(i, tmp);

        float w0;

        w0 = beta0[0];
        for (int l = 0; l < dsub; l++)
            x[l] = w0 * tmp[l];

        w0 = beta1[0];
        for (int l = dsub; l < d; l++)
            x[l] = w0 * tmp[l];

        for (size_t j = begin; j < end; j++) {
            storage_idx_t ji = hnsw.neighbors[j];
            if (ji < 0) ji = i;
            index.storage->reconstruct(ji, tmp);
            float w;
            w = beta0[j - begin + 1];
            for (int l = 0; l < dsub; l++)
                x[l] += w * tmp[l];

            w = beta1[j - begin + 1];
            for (int l = dsub; l < d; l++)
                x[l] += w * tmp[l];
        }
    } else {
        const float *betas[nsq];
        {
            const float *b = codebook.data();
            const uint8_t *c = &codes[i * code_size];
            for (int sq = 0; sq < nsq; sq++) {
                betas[sq] = b + (*c++) * (M + 1);
                b += (M + 1) * k;
            }
        }

        index.storage->reconstruct(i, tmp);
        {
            int d0 = 0;
            for (int sq = 0; sq < nsq; sq++) {
                float w = *(betas[sq]++);
                int d1 = d0 + dsub;
                for (int l = d0; l < d1; l++) {
                    x[l] = w * tmp[l];
                }
                d0 = d1;
            }
        }

        for (size_t j = begin; j < end; j++) {
            storage_idx_t ji = hnsw.neighbors[j];
            if (ji < 0) ji = i;

            index.storage->reconstruct(ji, tmp);
            int d0 = 0;
            for (int sq = 0; sq < nsq; sq++) {
                float w = *(betas[sq]++);
                int d1 = d0 + dsub;
                for (int l = d0; l < d1; l++) {
                    x[l] += w * tmp[l];
                }
                d0 = d1;
            }
        }
    }
}

void ReconstructFromNeighbors::reconstruct_n(storage_idx_t n0,
                                             storage_idx_t ni,
                                             float *x) const
{
#pragma omp parallel
    {
        std::vector<float> tmp(index.d);
#pragma omp for
        for (storage_idx_t i = 0; i < ni; i++) {
            reconstruct(n0 + i, x + i * index.d, tmp.data());
        }
    }
}

size_t ReconstructFromNeighbors::compute_distances(
    size_t n, const idx_t *shortlist,
    const float *query, float *distances) const
{
    std::vector<float> tmp(2 * index.d);
    size_t ncomp = 0;
    for (int i = 0; i < n; i++) {
        if (shortlist[i] < 0) break;
        reconstruct(shortlist[i], tmp.data(), tmp.data() + index.d);
        distances[i] = fvec_L2sqr(query, tmp.data(), index.d);
        ncomp++;
    }
    return ncomp;
}

void ReconstructFromNeighbors::get_neighbor_table(storage_idx_t i, float *tmp1) const
{
    const HNSW & hnsw = index.hnsw;
    size_t begin, end;
    hnsw.neighbor_range(i, 0, &begin, &end);
    size_t d = index.d;

    index.storage->reconstruct(i, tmp1);

    for (size_t j = begin; j < end; j++) {
        storage_idx_t ji = hnsw.neighbors[j];
        if (ji < 0) ji = i;
        index.storage->reconstruct(ji, tmp1 + (j - begin + 1) * d);
    }

}


/// called by add_codes
void ReconstructFromNeighbors::estimate_code(
       const float *x, storage_idx_t i, uint8_t *code) const
{

    // fill in tmp table with the neighbor values
    float *tmp1 = new float[d * (M + 1) + (d * k)];
    float *tmp2 = tmp1 + d * (M + 1);
    ScopeDeleter<float> del(tmp1);

    // collect coordinates of base
    get_neighbor_table (i, tmp1);

    for (size_t sq = 0; sq < nsq; sq++) {
        int d0 = sq * dsub;

        {
            FINTEGER ki = k, di = d, m1 = M + 1;
            FINTEGER dsubi = dsub;
            float zero = 0, one = 1;

            sgemm_ ("N", "N", &dsubi, &ki, &m1, &one,
                    tmp1 + d0, &di,
                    codebook.data() + sq * (m1 * k), &m1,
                    &zero, tmp2, &dsubi);
        }

        float min = HUGE_VAL;
        int argmin = -1;
        for (size_t j = 0; j < k; j++) {
            float dis = fvec_L2sqr(x + d0, tmp2 + j * dsub, dsub);
            if (dis < min) {
                min = dis;
                argmin = j;
            }
        }
        code[sq] = argmin;
    }

}

void ReconstructFromNeighbors::add_codes(size_t n, const float *x)
{
    if (k == 1) { // nothing to encode
        ntotal += n;
        return;
    }
    codes.resize(codes.size() + code_size * n);
#pragma omp parallel for
    for (int i = 0; i < n; i++) {
        estimate_code(x + i * index.d, ntotal + i,
                      codes.data() + (ntotal + i) * code_size);
    }
    ntotal += n;
    FAISS_ASSERT (codes.size() == ntotal * code_size);
}


/**************************************************************
 * IndexHNSWFlat implementation
 **************************************************************/


IndexHNSWFlat::IndexHNSWFlat()
{
    is_trained = true;
}

IndexHNSWFlat::IndexHNSWFlat(int d, int M, MetricType metric):
    IndexHNSW(new IndexFlat(d, metric), M)
{
    own_fields = true;
    is_trained = true;
}


/**************************************************************
 * IndexHNSWPQ implementation
 **************************************************************/


IndexHNSWPQ::IndexHNSWPQ() {}

IndexHNSWPQ::IndexHNSWPQ(int d, int pq_m, int M):
    IndexHNSW(new IndexPQ(d, pq_m, 8), M)
{
    own_fields = true;
    is_trained = false;
}

void IndexHNSWPQ::train(idx_t n, const float* x)
{
    IndexHNSW::train (n, x);
    (dynamic_cast<IndexPQ*> (storage))->pq.compute_sdc_table();
}


/**************************************************************
 * IndexHNSWSQ implementation
 **************************************************************/


IndexHNSWSQ::IndexHNSWSQ(int d, ScalarQuantizer::QuantizerType qtype, int M,
                         MetricType metric):
    IndexHNSW (new IndexScalarQuantizer (d, qtype, metric), M)
{
    is_trained = false;
    own_fields = true;
}

IndexHNSWSQ::IndexHNSWSQ() {}


/**************************************************************
 * IndexHNSW2Level implementation
 **************************************************************/


IndexHNSW2Level::IndexHNSW2Level(Index *quantizer, size_t nlist, int m_pq, int M):
    IndexHNSW (new Index2Layer (quantizer, nlist, m_pq), M)
{
    own_fields = true;
    is_trained = false;
}

IndexHNSW2Level::IndexHNSW2Level() {}


namespace {


// same as search_from_candidates but uses v
// visno -> is in result list
// visno + 1 -> in result list + in candidates
int search_from_candidates_2(const HNSW & hnsw,
                             DistanceComputer & qdis, int k,
                             idx_t *I, float * D,
                             MinimaxHeap &candidates,
                             VisitedTable &vt,
                             int level, int nres_in = 0)
{
    int nres = nres_in;
    int ndis = 0;
    for (int i = 0; i < candidates.size(); i++) {
        idx_t v1 = candidates.ids[i];
        FAISS_ASSERT(v1 >= 0);
        vt.visited[v1] = vt.visno + 1;
    }

    int nstep = 0;

    while (candidates.size() > 0) {
        float d0 = 0;
        int v0 = candidates.pop_min(&d0);

        size_t begin, end;
        hnsw.neighbor_range(v0, level, &begin, &end);

        for (size_t j = begin; j < end; j++) {
            int v1 = hnsw.neighbors[j];
            if (v1 < 0) break;
            if (vt.visited[v1] == vt.visno + 1) {
                // nothing to do
            } else {
                ndis++;
                float d = qdis(v1);
                candidates.push(v1, d);

                // never seen before --> add to heap
                if (vt.visited[v1] < vt.visno) {
                    if (nres < k) {
                        faiss::maxheap_push (++nres, D, I, d, v1);
                    } else if (d < D[0]) {
                        faiss::maxheap_pop (nres--, D, I);
                        faiss::maxheap_push (++nres, D, I, d, v1);
                    }
                }
                vt.visited[v1] = vt.visno + 1;
            }
        }

        nstep++;
        if (nstep > hnsw.efSearch) {
            break;
        }
    }

    if (level == 0) {
#pragma omp critical
        {
            hnsw_stats.n1 ++;
            if (candidates.size() == 0)
                hnsw_stats.n2 ++;
        }
    }


    return nres;
}


}  // namespace

void IndexHNSW2Level::search (idx_t n, const float *x, idx_t k,
                              float *distances, idx_t *labels) const
{
    if (dynamic_cast<const Index2Layer*>(storage)) {
        IndexHNSW::search (n, x, k, distances, labels);

    } else { // "mixed" search

        const IndexIVFPQ *index_ivfpq =
            dynamic_cast<const IndexIVFPQ*>(storage);

        int nprobe = index_ivfpq->nprobe;

        std::unique_ptr<idx_t[]> coarse_assign(new idx_t[n * nprobe]);
        std::unique_ptr<float[]> coarse_dis(new float[n * nprobe]);

        index_ivfpq->quantizer->search (n, x, nprobe, coarse_dis.get(),
                                        coarse_assign.get());

        index_ivfpq->search_preassigned (n, x, k, coarse_assign.get(),
                                         coarse_dis.get(), distances, labels,
                                         false);

#pragma omp parallel
        {
            VisitedTable vt (ntotal);
            DistanceComputer *dis = storage_distance_computer(storage);
            ScopeDeleter1<DistanceComputer> del(dis);

            int candidates_size = hnsw.upper_beam;
            MinimaxHeap candidates(candidates_size);

#pragma omp for
            for(idx_t i = 0; i < n; i++) {
                idx_t * idxi = labels + i * k;
                float * simi = distances + i * k;
                dis->set_query(x + i * d);

                // mark all inverted list elements as visited

                for (int j = 0; j < nprobe; j++) {
                    idx_t key = coarse_assign[j + i * nprobe];
                    if (key < 0) break;
                    size_t list_length = index_ivfpq->get_list_size (key);
                    const idx_t * ids = index_ivfpq->invlists->get_ids (key);

                    for (int jj = 0; jj < list_length; jj++) {
                        vt.set (ids[jj]);
                    }
                }

                candidates.clear();
                // copy the upper_beam elements to candidates list

                int search_policy = 2;

                if (search_policy == 1) {

                    for (int j = 0 ; j < hnsw.upper_beam && j < k; j++) {
                        if (idxi[j] < 0) break;
                        candidates.push (idxi[j], simi[j]);
                        // search_from_candidates adds them back
                        idxi[j] = -1;
                        simi[j] = HUGE_VAL;
                    }

                    // reorder from sorted to heap
                    maxheap_heapify (k, simi, idxi, simi, idxi, k);

                    hnsw.search_from_candidates(
                      *dis, k, idxi, simi,
                      candidates, vt, 0, k
                    );

                    vt.advance();

                } else if (search_policy == 2) {

                    for (int j = 0 ; j < hnsw.upper_beam && j < k; j++) {
                        if (idxi[j] < 0) break;
                        candidates.push (idxi[j], simi[j]);
                    }

                    // reorder from sorted to heap
                    maxheap_heapify (k, simi, idxi, simi, idxi, k);

                    search_from_candidates_2 (
                        hnsw, *dis, k, idxi, simi,
                        candidates, vt, 0, k);
                    vt.advance ();
                    vt.advance ();

                }

                maxheap_reorder (k, simi, idxi);
            }
        }
    }


}


void IndexHNSW2Level::flip_to_ivf ()
{
    Index2Layer *storage2l =
        dynamic_cast<Index2Layer*>(storage);

    FAISS_THROW_IF_NOT (storage2l);

    IndexIVFPQ * index_ivfpq =
        new IndexIVFPQ (storage2l->q1.quantizer,
                        d, storage2l->q1.nlist,
                        storage2l->pq.M, 8);
    index_ivfpq->pq = storage2l->pq;
    index_ivfpq->is_trained = storage2l->is_trained;
    index_ivfpq->precompute_table();
    index_ivfpq->own_fields = storage2l->q1.own_fields;
    storage2l->transfer_to_IVFPQ(*index_ivfpq);
    index_ivfpq->make_direct_map (true);

    storage = index_ivfpq;
    delete storage2l;

}


}  // namespace faiss
