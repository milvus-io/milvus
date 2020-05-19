/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexPreTransform.h>

#include <cstdio>
#include <cmath>
#include <cstring>
#include <memory>

#include <faiss/impl/FaissAssert.h>

namespace faiss {

/*********************************************
 * IndexPreTransform
 *********************************************/

IndexPreTransform::IndexPreTransform ():
    index(nullptr), own_fields (false)
{
}


IndexPreTransform::IndexPreTransform (
        Index * index):
    Index (index->d, index->metric_type),
    index (index), own_fields (false)
{
    is_trained = index->is_trained;
    ntotal = index->ntotal;
}


IndexPreTransform::IndexPreTransform (
        VectorTransform * ltrans,
        Index * index):
    Index (index->d, index->metric_type),
    index (index), own_fields (false)
{
    is_trained = index->is_trained;
    ntotal = index->ntotal;
    prepend_transform (ltrans);
}

void IndexPreTransform::prepend_transform (VectorTransform *ltrans)
{
    FAISS_THROW_IF_NOT (ltrans->d_out == d);
    is_trained = is_trained && ltrans->is_trained;
    chain.insert (chain.begin(), ltrans);
    d = ltrans->d_in;
}


IndexPreTransform::~IndexPreTransform ()
{
    if (own_fields) {
        for (int i = 0; i < chain.size(); i++)
            delete chain[i];
        delete index;
    }
}




void IndexPreTransform::train (idx_t n, const float *x)
{
    int last_untrained = 0;
    if (!index->is_trained) {
        last_untrained = chain.size();
    } else {
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (!chain[i]->is_trained) {
                last_untrained = i;
                break;
            }
        }
    }
    const float *prev_x = x;
    ScopeDeleter<float> del;

    if (verbose) {
        printf("IndexPreTransform::train: training chain 0 to %d\n",
               last_untrained);
    }

    for (int i = 0; i <= last_untrained; i++) {

        if (i < chain.size()) {
            VectorTransform *ltrans = chain [i];
            if (!ltrans->is_trained) {
                if (verbose) {
                    printf("   Training chain component %d/%zd\n",
                           i, chain.size());
                    if (OPQMatrix *opqm = dynamic_cast<OPQMatrix*>(ltrans)) {
                        opqm->verbose = true;
                    }
                }
                ltrans->train (n, prev_x);
            }
        } else {
            if (verbose) {
                printf("   Training sub-index\n");
            }
            index->train (n, prev_x);
        }
        if (i == last_untrained) break;
        if (verbose) {
            printf("   Applying transform %d/%zd\n",
                   i, chain.size());
        }

        float * xt = chain[i]->apply (n, prev_x);

        if (prev_x != x) delete [] prev_x;
        prev_x = xt;
        del.set(xt);
    }

    is_trained = true;
}


const float *IndexPreTransform::apply_chain (idx_t n, const float *x) const
{
    const float *prev_x = x;
    ScopeDeleter<float> del;

    for (int i = 0; i < chain.size(); i++) {
        float * xt = chain[i]->apply (n, prev_x);
        ScopeDeleter<float> del2 (xt);
        del2.swap (del);
        prev_x = xt;
    }
    del.release ();
    return prev_x;
}

void IndexPreTransform::reverse_chain (idx_t n, const float* xt, float* x) const
{
    const float* next_x = xt;
    ScopeDeleter<float> del;

    for (int i = chain.size() - 1; i >= 0; i--) {
        float* prev_x = (i == 0) ? x : new float [n * chain[i]->d_in];
        ScopeDeleter<float> del2 ((prev_x == x) ? nullptr : prev_x);
        chain [i]->reverse_transform (n, next_x, prev_x);
        del2.swap (del);
        next_x = prev_x;
    }
}

void IndexPreTransform::add (idx_t n, const float *x)
{
    FAISS_THROW_IF_NOT (is_trained);
    const float *xt = apply_chain (n, x);
    ScopeDeleter<float> del(xt == x ? nullptr : xt);
    index->add (n, xt);
    ntotal = index->ntotal;
}

void IndexPreTransform::add_with_ids (idx_t n, const float * x,
                                      const idx_t *xids)
{
    FAISS_THROW_IF_NOT (is_trained);
    const float *xt = apply_chain (n, x);
    ScopeDeleter<float> del(xt == x ? nullptr : xt);
    index->add_with_ids (n, xt, xids);
    ntotal = index->ntotal;
}




void IndexPreTransform::search (idx_t n, const float *x, idx_t k,
                               float *distances, idx_t *labels) const
{
    FAISS_THROW_IF_NOT (is_trained);
    const float *xt = apply_chain (n, x);
    ScopeDeleter<float> del(xt == x ? nullptr : xt);
    index->search (n, xt, k, distances, labels);
}

void IndexPreTransform::range_search (idx_t n, const float* x, float radius,
                                      RangeSearchResult* result) const
{
    FAISS_THROW_IF_NOT (is_trained);
    const float *xt = apply_chain (n, x);
    ScopeDeleter<float> del(xt == x ? nullptr : xt);
    index->range_search (n, xt, radius, result);
}



void IndexPreTransform::reset () {
    index->reset();
    ntotal = 0;
}

size_t IndexPreTransform::remove_ids (const IDSelector & sel) {
    size_t nremove = index->remove_ids (sel);
    ntotal = index->ntotal;
    return nremove;
}


void IndexPreTransform::reconstruct (idx_t key, float * recons) const
{
    float *x = chain.empty() ? recons : new float [index->d];
    ScopeDeleter<float> del (recons == x ? nullptr : x);
    // Initial reconstruction
    index->reconstruct (key, x);

    // Revert transformations from last to first
    reverse_chain (1, x, recons);
}


void IndexPreTransform::reconstruct_n (idx_t i0, idx_t ni, float *recons) const
{
    float *x = chain.empty() ? recons : new float [ni * index->d];
    ScopeDeleter<float> del (recons == x ? nullptr : x);
    // Initial reconstruction
    index->reconstruct_n (i0, ni, x);

    // Revert transformations from last to first
    reverse_chain (ni, x, recons);
}


void IndexPreTransform::search_and_reconstruct (
      idx_t n, const float *x, idx_t k,
      float *distances, idx_t *labels, float* recons) const
{
    FAISS_THROW_IF_NOT (is_trained);

    const float* xt = apply_chain (n, x);
    ScopeDeleter<float> del ((xt == x) ? nullptr : xt);

    float* recons_temp = chain.empty() ? recons : new float [n * k * index->d];
    ScopeDeleter<float> del2 ((recons_temp == recons) ? nullptr : recons_temp);
    index->search_and_reconstruct (n, xt, k, distances, labels, recons_temp);

    // Revert transformations from last to first
    reverse_chain (n * k, recons_temp, recons);
}

size_t IndexPreTransform::sa_code_size () const
{
    return index->sa_code_size ();
}

void IndexPreTransform::sa_encode (idx_t n, const float *x,
                                         uint8_t *bytes) const
{
    if (chain.empty()) {
        index->sa_encode (n, x, bytes);
    } else {
        const float *xt = apply_chain (n, x);
        ScopeDeleter<float> del(xt == x ? nullptr : xt);
        index->sa_encode (n, xt, bytes);
    }
}

void IndexPreTransform::sa_decode (idx_t n, const uint8_t *bytes,
                                           float *x) const
{
    if (chain.empty()) {
        index->sa_decode (n, bytes, x);
    } else {
        std::unique_ptr<float []> x1 (new float [index->d * n]);
        index->sa_decode (n, bytes, x1.get());
        // Revert transformations from last to first
        reverse_chain (n, x1.get(), x);
    }
}



} // namespace faiss
