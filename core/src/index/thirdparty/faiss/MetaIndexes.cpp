/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/MetaIndexes.h>

#include <cstdio>
#include <stdint.h>

#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/utils/WorkerThread.h>


namespace faiss {

namespace {


} // namespace

/*****************************************************
 * IndexIDMap implementation
 *******************************************************/

template <typename IndexT>
IndexIDMapTemplate<IndexT>::IndexIDMapTemplate (IndexT *index):
    index (index),
    own_fields (false)
{
    FAISS_THROW_IF_NOT_MSG (index->ntotal == 0, "index must be empty on input");
    this->is_trained = index->is_trained;
    this->metric_type = index->metric_type;
    this->verbose = index->verbose;
    this->d = index->d;
}

template <typename IndexT>
void IndexIDMapTemplate<IndexT>::add
    (idx_t, const typename IndexT::component_t *)
{
    FAISS_THROW_MSG ("add does not make sense with IndexIDMap, "
                      "use add_with_ids");
}


template <typename IndexT>
void IndexIDMapTemplate<IndexT>::train
    (idx_t n, const typename IndexT::component_t *x)
{
    index->train (n, x);
    this->is_trained = index->is_trained;
}

template <typename IndexT>
void IndexIDMapTemplate<IndexT>::reset ()
{
    index->reset ();
    id_map.clear();
    this->ntotal = 0;
}


template <typename IndexT>
void IndexIDMapTemplate<IndexT>::add_with_ids
    (idx_t n, const typename IndexT::component_t * x,
     const typename IndexT::idx_t *xids)
{
    index->add (n, x);
    for (idx_t i = 0; i < n; i++)
        id_map.push_back (xids[i]);
    this->ntotal = index->ntotal;
}


template <typename IndexT>
void IndexIDMapTemplate<IndexT>::search
    (idx_t n, const typename IndexT::component_t *x, idx_t k,
     typename IndexT::distance_t *distances, typename IndexT::idx_t *labels) const
{
    index->search (n, x, k, distances, labels);
    idx_t *li = labels;
#pragma omp parallel for
    for (idx_t i = 0; i < n * k; i++) {
        li[i] = li[i] < 0 ? li[i] : id_map[li[i]];
    }
}


template <typename IndexT>
void IndexIDMapTemplate<IndexT>::range_search
    (typename IndexT::idx_t n, const typename IndexT::component_t *x,
     typename IndexT::distance_t radius, RangeSearchResult *result) const
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
    const std::vector <int64_t> & id_map;
    const IDSelector & sel;
    IDTranslatedSelector (const std::vector <int64_t> & id_map,
                          const IDSelector & sel):
        id_map (id_map), sel (sel)
    {}
    bool is_member(idx_t id) const override {
      return sel.is_member(id_map[id]);
    }
};

}

template <typename IndexT>
size_t IndexIDMapTemplate<IndexT>::remove_ids (const IDSelector & sel)
{
    // remove in sub-index first
    IDTranslatedSelector sel2 (id_map, sel);
    size_t nremove = index->remove_ids (sel2);

    int64_t j = 0;
    for (idx_t i = 0; i < this->ntotal; i++) {
        if (sel.is_member (id_map[i])) {
            // remove
        } else {
            id_map[j] = id_map[i];
            j++;
        }
    }
    FAISS_ASSERT (j == index->ntotal);
    this->ntotal = j;
    id_map.resize(this->ntotal);
    return nremove;
}

template <typename IndexT>
IndexIDMapTemplate<IndexT>::~IndexIDMapTemplate ()
{
    if (own_fields) delete index;
}



/*****************************************************
 * IndexIDMap2 implementation
 *******************************************************/

template <typename IndexT>
IndexIDMap2Template<IndexT>::IndexIDMap2Template (IndexT *index):
    IndexIDMapTemplate<IndexT> (index)
{}

template <typename IndexT>
void IndexIDMap2Template<IndexT>::add_with_ids
    (idx_t n, const typename IndexT::component_t* x,
     const typename IndexT::idx_t* xids)
{
    size_t prev_ntotal = this->ntotal;
    IndexIDMapTemplate<IndexT>::add_with_ids (n, x, xids);
    for (size_t i = prev_ntotal; i < this->ntotal; i++) {
        rev_map [this->id_map [i]] = i;
    }
}

template <typename IndexT>
void IndexIDMap2Template<IndexT>::construct_rev_map ()
{
    rev_map.clear ();
    for (size_t i = 0; i < this->ntotal; i++) {
        rev_map [this->id_map [i]] = i;
    }
}


template <typename IndexT>
size_t IndexIDMap2Template<IndexT>::remove_ids(const IDSelector& sel)
{
    // This is quite inefficient
    size_t nremove = IndexIDMapTemplate<IndexT>::remove_ids (sel);
    construct_rev_map ();
    return nremove;
}

template <typename IndexT>
void IndexIDMap2Template<IndexT>::reconstruct
    (idx_t key, typename IndexT::component_t * recons) const
{
    try {
        this->index->reconstruct (rev_map.at (key), recons);
    } catch (const std::out_of_range& e) {
        FAISS_THROW_FMT ("key %ld not found", key);
    }
}


// explicit template instantiations

template struct IndexIDMapTemplate<Index>;
template struct IndexIDMapTemplate<IndexBinary>;
template struct IndexIDMap2Template<Index>;
template struct IndexIDMap2Template<IndexBinary>;


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

    int64_t nshard = sub_indexes.size();
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
        int64_t sub_d = sub_index->d, d = index->d;
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

    int64_t factor = 1;
    for (int i = 0; i < nshard; i++) {
        if (i > 0) { // results of 0 are already in the table
            const float *distances_i = all_distances + i * k * n;
            const idx_t *labels_i = all_labels + i * k * n;
            for (int64_t j = 0; j < n; j++) {
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
