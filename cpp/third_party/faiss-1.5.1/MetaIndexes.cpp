/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "MetaIndexes.h"

#include <cstdio>

#include "FaissAssert.h"
#include "Heap.h"
#include "AuxIndexStructures.h"
#include "WorkerThread.h"


namespace faiss {

namespace {

typedef Index::idx_t idx_t;

} // namespace

/*****************************************************
 * IndexIDMap implementation
 *******************************************************/

IndexIDMap::IndexIDMap (Index *index):
    index (index),
    own_fields (false)
{
    FAISS_THROW_IF_NOT_MSG (index->ntotal == 0, "index must be empty on input");
    is_trained = index->is_trained;
    metric_type = index->metric_type;
    verbose = index->verbose;
    d = index->d;
}

void IndexIDMap::add (idx_t, const float *)
{
    FAISS_THROW_MSG ("add does not make sense with IndexIDMap, "
                      "use add_with_ids");
}


void IndexIDMap::train (idx_t n, const float *x)
{
    index->train (n, x);
    is_trained = index->is_trained;
}

void IndexIDMap::reset ()
{
    index->reset ();
    id_map.clear();
    ntotal = 0;
}


void IndexIDMap::add_with_ids (idx_t n, const float * x, const long *xids)
{
    index->add (n, x);
    for (idx_t i = 0; i < n; i++)
        id_map.push_back (xids[i]);
    ntotal = index->ntotal;
}


void IndexIDMap::search (idx_t n, const float *x, idx_t k,
                              float *distances, idx_t *labels) const
{
    index->search (n, x, k, distances, labels);
    idx_t *li = labels;
#pragma omp parallel for
    for (idx_t i = 0; i < n * k; i++) {
        li[i] = li[i] < 0 ? li[i] : id_map[li[i]];
    }
}


void IndexIDMap::range_search (idx_t n, const float *x, float radius,
                   RangeSearchResult *result) const
{
  index->range_search(n, x, radius, result);
#pragma omp parallel for
  for (idx_t i = 0; i < result->lims[result->nq]; i++) {
      result->labels[i] = result->labels[i] < 0 ?
        result->labels[i] : id_map[result->labels[i]];
  }
}

namespace {

struct IDTranslatedSelector: IDSelector {
    const std::vector <long> & id_map;
    const IDSelector & sel;
    IDTranslatedSelector (const std::vector <long> & id_map,
                          const IDSelector & sel):
        id_map (id_map), sel (sel)
    {}
    bool is_member(idx_t id) const override {
      return sel.is_member(id_map[id]);
    }
};

}

long IndexIDMap::remove_ids (const IDSelector & sel)
{
    // remove in sub-index first
    IDTranslatedSelector sel2 (id_map, sel);
    long nremove = index->remove_ids (sel2);

    long j = 0;
    for (idx_t i = 0; i < ntotal; i++) {
        if (sel.is_member (id_map[i])) {
            // remove
        } else {
            id_map[j] = id_map[i];
            j++;
        }
    }
    FAISS_ASSERT (j == index->ntotal);
    ntotal = j;
    id_map.resize(ntotal);
    return nremove;
}




IndexIDMap::~IndexIDMap ()
{
    if (own_fields) delete index;
}

/*****************************************************
 * IndexIDMap2 implementation
 *******************************************************/

IndexIDMap2::IndexIDMap2 (Index *index): IndexIDMap (index)
{}

void IndexIDMap2::add_with_ids(idx_t n, const float* x, const long* xids)
{
    size_t prev_ntotal = ntotal;
    IndexIDMap::add_with_ids (n, x, xids);
    for (size_t i = prev_ntotal; i < ntotal; i++) {
        rev_map [id_map [i]] = i;
    }
}

void IndexIDMap2::construct_rev_map ()
{
    rev_map.clear ();
    for (size_t i = 0; i < ntotal; i++) {
        rev_map [id_map [i]] = i;
    }
}


long IndexIDMap2::remove_ids(const IDSelector& sel)
{
    // This is quite inefficient
    long nremove = IndexIDMap::remove_ids (sel);
    construct_rev_map ();
    return nremove;
}

void IndexIDMap2::reconstruct (idx_t key, float * recons) const
{
    try {
        index->reconstruct (rev_map.at (key), recons);
    } catch (const std::out_of_range& e) {
        FAISS_THROW_FMT ("key %ld not found", key);
    }
}

/*****************************************************
 * IndexSplitVectors implementation
 *******************************************************/


IndexSplitVectors::IndexSplitVectors (idx_t d, bool threaded):
    Index (d), own_fields (false),
    threaded (threaded), sum_d (0)
{

}

void IndexSplitVectors::add_sub_index (Index *index)
{
    sub_indexes.push_back (index);
    sync_with_sub_indexes ();
}

void IndexSplitVectors::sync_with_sub_indexes ()
{
    if (sub_indexes.empty()) return;
    Index * index0 = sub_indexes[0];
    sum_d = index0->d;
    metric_type = index0->metric_type;
    is_trained = index0->is_trained;
    ntotal = index0->ntotal;
    for (int i = 1; i < sub_indexes.size(); i++) {
        Index * index = sub_indexes[i];
        FAISS_THROW_IF_NOT (metric_type == index->metric_type);
        FAISS_THROW_IF_NOT (ntotal == index->ntotal);
        sum_d += index->d;
    }

}

void IndexSplitVectors::add(idx_t /*n*/, const float* /*x*/) {
  FAISS_THROW_MSG("not implemented");
}



void IndexSplitVectors::search (
           idx_t n, const float *x, idx_t k,
           float *distances, idx_t *labels) const
{
    FAISS_THROW_IF_NOT_MSG (k == 1,
                      "search implemented only for k=1");
    FAISS_THROW_IF_NOT_MSG (sum_d == d,
                      "not enough indexes compared to # dimensions");

    long nshard = sub_indexes.size();
    float *all_distances = new float [nshard * k * n];
    idx_t *all_labels = new idx_t [nshard * k * n];
    ScopeDeleter<float> del (all_distances);
    ScopeDeleter<idx_t> del2 (all_labels);

    auto query_func = [n, x, k, distances, labels, all_distances, all_labels, this]
        (int no) {
        const IndexSplitVectors *index = this;
        float *distances1 = no == 0 ? distances : all_distances + no * k * n;
        idx_t *labels1 = no == 0 ? labels : all_labels + no * k * n;
        if (index->verbose)
            printf ("begin query shard %d on %ld points\n", no, n);
        const Index * sub_index = index->sub_indexes[no];
        long sub_d = sub_index->d, d = index->d;
        idx_t ofs = 0;
        for (int i = 0; i < no; i++) ofs += index->sub_indexes[i]->d;
        float *sub_x = new float [sub_d * n];
        ScopeDeleter<float> del1 (sub_x);
        for (idx_t i = 0; i < n; i++)
            memcpy (sub_x + i * sub_d, x + ofs + i * d, sub_d * sizeof (sub_x));
        sub_index->search (n, sub_x, k, distances1, labels1);
        if (index->verbose)
            printf ("end query shard %d\n", no);
    };

    if (!threaded) {
        for (int i = 0; i < nshard; i++) {
            query_func(i);
        }
    } else {
        std::vector<std::unique_ptr<WorkerThread> > threads;
        std::vector<std::future<bool>> v;

        for (int i = 0; i < nshard; i++) {
            threads.emplace_back(new WorkerThread());
            WorkerThread *wt = threads.back().get();
            v.emplace_back(wt->add([i, query_func](){query_func(i); }));
        }

        // Blocking wait for completion
        for (auto& func : v) {
            func.get();
        }
    }

    long factor = 1;
    for (int i = 0; i < nshard; i++) {
        if (i > 0) { // results of 0 are already in the table
            const float *distances_i = all_distances + i * k * n;
            const idx_t *labels_i = all_labels + i * k * n;
            for (long j = 0; j < n; j++) {
                if (labels[j] >= 0 && labels_i[j] >= 0) {
                    labels[j] += labels_i[j] * factor;
                    distances[j] += distances_i[j];
                } else {
                    labels[j] = -1;
                    distances[j] = 0.0 / 0.0;
                }
            }
        }
        factor *= sub_indexes[i]->ntotal;
    }

}

void IndexSplitVectors::train(idx_t /*n*/, const float* /*x*/) {
  FAISS_THROW_MSG("not implemented");
}

void IndexSplitVectors::reset ()
{
    FAISS_THROW_MSG ("not implemented");
}


IndexSplitVectors::~IndexSplitVectors ()
{
    if (own_fields) {
        for (int s = 0; s < sub_indexes.size(); s++)
            delete sub_indexes [s];
    }
}


} // namespace faiss
