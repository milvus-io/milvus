/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexShards.h"

#include <cstdio>
#include <functional>

#include "FaissAssert.h"
#include "Heap.h"
#include "WorkerThread.h"

namespace faiss {

// subroutines
namespace {

typedef Index::idx_t idx_t;


// add translation to all valid labels
void translate_labels (long n, idx_t *labels, long translation)
{
    if (translation == 0) return;
    for (long i = 0; i < n; i++) {
        if(labels[i] < 0) continue;
        labels[i] += translation;
    }
}


/** merge result tables from several shards.
 * @param all_distances  size nshard * n * k
 * @param all_labels     idem
 * @param translartions  label translations to apply, size nshard
 */

template <class IndexClass, class C>
void merge_tables (long n, long k, long nshard,
                   typename IndexClass::distance_t *distances,
                   idx_t *labels,
                   const typename IndexClass::distance_t *all_distances,
                   idx_t *all_labels,
                   const long *translations)
{
    if(k == 0) {
        return;
    }
    using distance_t = typename IndexClass::distance_t;

    long stride = n * k;
#pragma omp parallel
    {
        std::vector<int> buf (2 * nshard);
        int * pointer = buf.data();
        int * shard_ids = pointer + nshard;
        std::vector<distance_t> buf2 (nshard);
        distance_t * heap_vals = buf2.data();
#pragma omp for
        for (long i = 0; i < n; i++) {
            // the heap maps values to the shard where they are
            // produced.
            const distance_t *D_in = all_distances + i * k;
            const idx_t *I_in = all_labels + i * k;
            int heap_size = 0;

            for (long s = 0; s < nshard; s++) {
                pointer[s] = 0;
                if (I_in[stride * s] >= 0)
                    heap_push<C> (++heap_size, heap_vals, shard_ids,
                                 D_in[stride * s], s);
            }

            distance_t *D = distances + i * k;
            idx_t *I = labels + i * k;

            for (int j = 0; j < k; j++) {
                if (heap_size == 0) {
                    I[j] = -1;
                    D[j] = C::neutral();
                } else {
                    // pop best element
                    int s = shard_ids[0];
                    int & p = pointer[s];
                    D[j] = heap_vals[0];
                    I[j] = I_in[stride * s + p] + translations[s];

                    heap_pop<C> (heap_size--, heap_vals, shard_ids);
                    p++;
                    if (p < k && I_in[stride * s + p] >= 0)
                        heap_push<C> (++heap_size, heap_vals, shard_ids,
                                     D_in[stride * s + p], s);
                }
            }
        }
    }
}

template<class IndexClass>
void runOnIndexes(bool threaded,
                  std::function<void(int no, IndexClass*)> f,
                  std::vector<IndexClass *> indexes)
{
    FAISS_THROW_IF_NOT_MSG(!indexes.empty(), "no shards in index");

    if (!threaded) {
        for (int no = 0; no < indexes.size(); no++) {
            IndexClass *index = indexes[no];
            f(no, index);
        }
    } else {
        std::vector<std::unique_ptr<WorkerThread> > threads;
        std::vector<std::future<bool>> v;

        for (int no = 0; no < indexes.size(); no++) {
            IndexClass *index = indexes[no];
            threads.emplace_back(new WorkerThread());
            WorkerThread *wt = threads.back().get();
            v.emplace_back(wt->add([no, index, f](){ f(no, index); }));
        }

        // Blocking wait for completion
        for (auto& func : v) {
            func.get();
        }
    }

};

} // anonymous namespace


template<class IndexClass>
IndexShardsTemplate<IndexClass>::IndexShardsTemplate (idx_t d, bool threaded, bool successive_ids):
    IndexClass (d), own_fields (false),
    threaded (threaded), successive_ids (successive_ids)
{


}


template<class IndexClass>
void IndexShardsTemplate<IndexClass>::add_shard (IndexClass *idx)
{
    shard_indexes.push_back (idx);
    sync_with_shard_indexes ();
}

template<class IndexClass>
void IndexShardsTemplate<IndexClass>::sync_with_shard_indexes ()
{
    if (shard_indexes.empty()) return;
    IndexClass * index0 = shard_indexes[0];
    this->d = index0->d;
    this->metric_type = index0->metric_type;
    this->is_trained = index0->is_trained;
    this->ntotal = index0->ntotal;
    for (int i = 1; i < shard_indexes.size(); i++) {
        IndexClass * index = shard_indexes[i];
        FAISS_THROW_IF_NOT (this->metric_type == index->metric_type);
        FAISS_THROW_IF_NOT (this->d == index->d);
        this->ntotal += index->ntotal;
    }
}






template<class IndexClass>
void IndexShardsTemplate<IndexClass>::train (idx_t n, const component_t *x)
{
    auto train_func = [n, x](int no, IndexClass *index)
    {
        if (index->verbose)
            printf ("begin train shard %d on %ld points\n", no, n);
        index->train(n, x);
        if (index->verbose)
            printf ("end train shard %d\n", no);
    };

    runOnIndexes<IndexClass> (threaded, train_func, shard_indexes);
    sync_with_shard_indexes ();
}

template<class IndexClass>
void IndexShardsTemplate<IndexClass>::add (idx_t n, const component_t *x)
{
    add_with_ids (n, x, nullptr);
}

template<class IndexClass>
void IndexShardsTemplate<IndexClass>::add_with_ids (idx_t n, const component_t * x, const idx_t *xids)
{

    FAISS_THROW_IF_NOT_MSG(!(successive_ids && xids),
                   "It makes no sense to pass in ids and "
                   "request them to be shifted");

    if (successive_ids) {
        FAISS_THROW_IF_NOT_MSG(!xids,
                       "It makes no sense to pass in ids and "
                       "request them to be shifted");
        FAISS_THROW_IF_NOT_MSG(this->ntotal == 0,
                       "when adding to IndexShards with sucessive_ids, "
                       "only add() in a single pass is supported");
    }

    long nshard = shard_indexes.size();
    const idx_t *ids = xids;
    ScopeDeleter<idx_t> del;
    if (!ids && !successive_ids) {
        idx_t *aids = new idx_t[n];
        for (idx_t i = 0; i < n; i++)
            aids[i] = this->ntotal + i;
        ids = aids;
        del.set (ids);
    }

    size_t components_per_vec =
        sizeof(component_t) == 1 ? (this->d + 7) / 8 : this->d;

    auto add_func = [n, ids, x, nshard, components_per_vec]
        (int no, IndexClass *index) {

        idx_t i0 = no * n / nshard;
        idx_t i1 = (no + 1) * n / nshard;

        auto x0 = x + i0 * components_per_vec;

        if (index->verbose) {
            printf ("begin add shard %d on %ld points\n", no, n);
        }
        if (ids) {
            index->add_with_ids (i1 - i0, x0, ids + i0);
        } else {
            index->add (i1 - i0, x0);
        }
        if (index->verbose) {
            printf ("end add shard %d on %ld points\n", no, i1 - i0);
        }
    };

    runOnIndexes<IndexClass> (threaded, add_func, shard_indexes);

    this->ntotal += n;
}

template<class IndexClass>
void IndexShardsTemplate<IndexClass>::reset ()
{
    for (int i = 0; i < shard_indexes.size(); i++) {
        shard_indexes[i]->reset ();
    }
    sync_with_shard_indexes ();
}

template<class IndexClass>
void IndexShardsTemplate<IndexClass>::search (
           idx_t n, const component_t *x, idx_t k,
           distance_t *distances, idx_t *labels) const
{
    long nshard = shard_indexes.size();
    distance_t *all_distances = new distance_t [nshard * k * n];
    idx_t *all_labels = new idx_t [nshard * k * n];
    ScopeDeleter<distance_t> del (all_distances);
    ScopeDeleter<idx_t> del2 (all_labels);

    auto query_func = [n, k, x, all_distances, all_labels]
        (int no, IndexClass *index) {

        if (index->verbose) {
            printf ("begin query shard %d on %ld points\n", no, n);
        }
        index->search (n, x, k,
                       all_distances + no * k * n,
                       all_labels + no * k * n);
        if (index->verbose) {
            printf ("end query shard %d\n", no);
        }
    };

    runOnIndexes<IndexClass> (threaded, query_func, shard_indexes);

    std::vector<long> translations (nshard, 0);
    if (successive_ids) {
        translations[0] = 0;
        for (int s = 0; s + 1 < nshard; s++)
            translations [s + 1] = translations [s] +
                shard_indexes [s]->ntotal;
    }

    if (this->metric_type == METRIC_L2) {
        merge_tables<IndexClass, CMin<distance_t, int> > (
             n, k, nshard, distances, labels,
             all_distances, all_labels, translations.data ());
    } else {
        merge_tables<IndexClass, CMax<distance_t, int> > (
             n, k, nshard, distances, labels,
             all_distances, all_labels, translations.data ());
    }

}


template<class IndexClass>
IndexShardsTemplate<IndexClass>::~IndexShardsTemplate ()
{
    if (own_fields) {
        for (int s = 0; s < shard_indexes.size(); s++)
            delete shard_indexes [s];
    }
}

// explicit instanciations
template struct IndexShardsTemplate<Index>;
template struct IndexShardsTemplate<IndexBinary>;



} // namespace faiss
