/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexIVFFlat.h"

#include <cstdio>

#include "utils.h"

#include "FaissAssert.h"
#include "IndexFlat.h"
#include "AuxIndexStructures.h"


namespace faiss {


/*****************************************
 * IndexIVFFlat implementation
 ******************************************/

IndexIVFFlat::IndexIVFFlat (Index * quantizer,
                            size_t d, size_t nlist, MetricType metric):
    IndexIVF (quantizer, d, nlist, sizeof(float) * d, metric)
{
    code_size = sizeof(float) * d;
}


void IndexIVFFlat::add_with_ids (idx_t n, const float * x, const long *xids)
{
    add_core (n, x, xids, nullptr);
}

void IndexIVFFlat::add_core (idx_t n, const float * x, const long *xids,
                             const long *precomputed_idx)

{
    FAISS_THROW_IF_NOT (is_trained);
    assert (invlists);
    FAISS_THROW_IF_NOT_MSG (!(maintain_direct_map && xids),
                            "cannot have direct map and add with ids");
    const long * idx;
    ScopeDeleter<long> del;

    if (precomputed_idx) {
        idx = precomputed_idx;
    } else {
        long * idx0 = new long [n];
        del.set (idx0);
        quantizer->assign (n, x, idx0);
        idx = idx0;
    }
    long n_add = 0;
    for (size_t i = 0; i < n; i++) {
        long id = xids ? xids[i] : ntotal + i;
        long list_no = idx [i];

        if (list_no < 0)
            continue;
        const float *xi = x + i * d;
        size_t offset = invlists->add_entry (
              list_no, id, (const uint8_t*) xi);

        if (maintain_direct_map)
            direct_map.push_back (list_no << 32 | offset);
        n_add++;
    }
    if (verbose) {
        printf("IndexIVFFlat::add_core: added %ld / %ld vectors\n",
               n_add, n);
    }
    ntotal += n;
}

void IndexIVFFlat::encode_vectors(idx_t n, const float* x,
                                  const idx_t * /* list_nos */,
                                  uint8_t * codes) const
{
    memcpy (codes, x, code_size * n);
}



namespace {


template<MetricType metric, bool store_pairs, class C>
struct IVFFlatScanner: InvertedListScanner {

    size_t d;
    IVFFlatScanner(size_t d): d(d) {}

    const float *xi;
    void set_query (const float *query) override {
        this->xi = query;
    }

    idx_t list_no;
    void set_list (idx_t list_no, float /* coarse_dis */) override {
        this->list_no = list_no;
    }

    float distance_to_code (const uint8_t *code) const override {
        const float *yj = (float*)code;
        float dis = metric == METRIC_INNER_PRODUCT ?
            fvec_inner_product (xi, yj, d) : fvec_L2sqr (xi, yj, d);
        return dis;
    }

    size_t scan_codes (size_t list_size,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *simi, idx_t *idxi,
                       size_t k) const override
    {
        const float *list_vecs = (const float*)codes;
        size_t nup = 0;
        for (size_t j = 0; j < list_size; j++) {
            const float * yj = list_vecs + d * j;
            float dis = metric == METRIC_INNER_PRODUCT ?
                fvec_inner_product (xi, yj, d) : fvec_L2sqr (xi, yj, d);
            if (C::cmp (simi[0], dis)) {
                heap_pop<C> (k, simi, idxi);
                long id = store_pairs ? (list_no << 32 | j) : ids[j];
                heap_push<C> (k, simi, idxi, dis, id);
                nup++;
            }
        }
        return nup;
    }

    void scan_codes_range (size_t list_size,
                           const uint8_t *codes,
                           const idx_t *ids,
                           float radius,
                           RangeQueryResult & res) const override
    {
        const float *list_vecs = (const float*)codes;
        for (size_t j = 0; j < list_size; j++) {
            const float * yj = list_vecs + d * j;
            float dis = metric == METRIC_INNER_PRODUCT ?
                fvec_inner_product (xi, yj, d) : fvec_L2sqr (xi, yj, d);
            if (C::cmp (radius, dis)) {
                long id = store_pairs ? (list_no << 32 | j) : ids[j];
                res.add (dis, id);
            }
        }
    }


};


} // anonymous namespace



InvertedListScanner* IndexIVFFlat::get_InvertedListScanner
     (bool store_pairs) const
{
    if (metric_type == METRIC_INNER_PRODUCT) {
        if (store_pairs) {
            return new IVFFlatScanner<
                METRIC_INNER_PRODUCT, true, CMin<float, long> > (d);
        } else {
            return new IVFFlatScanner<
                METRIC_INNER_PRODUCT, false, CMin<float, long> >(d);
        }
    } else if (metric_type == METRIC_L2) {
        if (store_pairs) {
            return new IVFFlatScanner<
                METRIC_L2, true, CMax<float, long> > (d);
        } else {
            return new IVFFlatScanner<
                METRIC_L2, false, CMax<float, long> >(d);
        }
    }
    return nullptr;
}



void IndexIVFFlat::update_vectors (int n, idx_t *new_ids, const float *x)
{

    FAISS_THROW_IF_NOT (maintain_direct_map);
    FAISS_THROW_IF_NOT (is_trained);
    std::vector<idx_t> assign (n);
    quantizer->assign (n, x, assign.data());

    for (size_t i = 0; i < n; i++) {
        idx_t id = new_ids[i];
        FAISS_THROW_IF_NOT_MSG (0 <= id && id < ntotal,
                                "id to update out of range");
        { // remove old one
            long dm = direct_map[id];
            long ofs = dm & 0xffffffff;
            long il = dm >> 32;
            size_t l = invlists->list_size (il);
            if (ofs != l - 1) { // move l - 1 to ofs
                long id2 = invlists->get_single_id (il, l - 1);
                direct_map[id2] = (il << 32) | ofs;
                invlists->update_entry (il, ofs, id2,
                                        invlists->get_single_code (il, l - 1));
            }
            invlists->resize (il, l - 1);
        }
        { // insert new one
            long il = assign[i];
            size_t l = invlists->list_size (il);
            long dm = (il << 32) | l;
            direct_map[id] = dm;
            invlists->add_entry (il, id, (const uint8_t*)(x + i * d));
        }
    }

}

void IndexIVFFlat::reconstruct_from_offset (long list_no, long offset,
                                            float* recons) const
{
    memcpy (recons, invlists->get_single_code (list_no, offset), code_size);
}

/*****************************************
 * IndexIVFFlatDedup implementation
 ******************************************/

IndexIVFFlatDedup::IndexIVFFlatDedup (
            Index * quantizer, size_t d, size_t nlist_,
            MetricType metric_type):
    IndexIVFFlat (quantizer, d, nlist_, metric_type)
{}


void IndexIVFFlatDedup::train(idx_t n, const float* x)
{
    std::unordered_map<uint64_t, idx_t> map;
    float * x2 = new float [n * d];
    ScopeDeleter<float> del (x2);

    long n2 = 0;
    for (long i = 0; i < n; i++) {
        uint64_t hash = hash_bytes((uint8_t *)(x + i * d), code_size);
        if (map.count(hash) &&
            !memcmp (x2 + map[hash] * d, x + i * d, code_size)) {
            // is duplicate, skip
        } else {
            map [hash] = n2;
            memcpy (x2 + n2 * d, x + i * d, code_size);
            n2 ++;
        }
    }
    if (verbose) {
        printf ("IndexIVFFlatDedup::train: train on %ld points after dedup "
                "(was %ld points)\n", n2, n);
    }
    IndexIVFFlat::train (n2, x2);
}



void IndexIVFFlatDedup::add_with_ids(
           idx_t na, const float* x, const long* xids)
{

    FAISS_THROW_IF_NOT (is_trained);
    assert (invlists);
    FAISS_THROW_IF_NOT_MSG (
           !maintain_direct_map,
           "IVFFlatDedup not implemented with direct_map");
    long * idx = new long [na];
    ScopeDeleter<long> del (idx);
    quantizer->assign (na, x, idx);

    long n_add = 0, n_dup = 0;
    // TODO make a omp loop with this
    for (size_t i = 0; i < na; i++) {
        idx_t id = xids ? xids[i] : ntotal + i;
        long list_no = idx [i];

        if (list_no < 0) {
            continue;
        }
        const float *xi = x + i * d;

        // search if there is already an entry with that id
        InvertedLists::ScopedCodes codes (invlists, list_no);

        long n = invlists->list_size (list_no);
        long offset = -1;
        for (long o = 0; o < n; o++) {
            if (!memcmp (codes.get() + o * code_size,
                         xi, code_size)) {
                offset = o;
                break;
            }
        }

        if (offset == -1) { // not found
            invlists->add_entry (list_no, id, (const uint8_t*) xi);
        } else {
            // mark equivalence
            idx_t id2 = invlists->get_single_id (list_no, offset);
            std::pair<idx_t, idx_t> pair (id2, id);
            instances.insert (pair);
            n_dup ++;
        }
        n_add++;
    }
    if (verbose) {
        printf("IndexIVFFlat::add_with_ids: added %ld / %ld vectors"
               " (out of which %ld are duplicates)\n",
               n_add, na, n_dup);
    }
    ntotal += n_add;
}

void IndexIVFFlatDedup::search_preassigned (
           idx_t n, const float *x, idx_t k,
           const idx_t *assign,
           const float *centroid_dis,
           float *distances, idx_t *labels,
           bool store_pairs,
           const IVFSearchParameters *params) const
{
    FAISS_THROW_IF_NOT_MSG (
           !store_pairs, "store_pairs not supported in IVFDedup");

    IndexIVFFlat::search_preassigned (n, x, k, assign, centroid_dis,
                                      distances, labels, false,
                                      params);

    std::vector <idx_t> labels2 (k);
    std::vector <float> dis2 (k);

    for (long i = 0; i < n; i++) {
        idx_t *labels1 = labels + i * k;
        float *dis1 = distances + i * k;
        long j = 0;
        for (; j < k; j++) {
            if (instances.find (labels1[j]) != instances.end ()) {
                // a duplicate: special handling
                break;
            }
        }
        if (j < k) {
            // there are duplicates, special handling
            long j0 = j;
            long rp = j;
            while (j < k) {
                auto range = instances.equal_range (labels1[rp]);
                float dis = dis1[rp];
                labels2[j] = labels1[rp];
                dis2[j] = dis;
                j ++;
                for (auto it = range.first; j < k && it != range.second; ++it) {
                    labels2[j] = it->second;
                    dis2[j] = dis;
                    j++;
                }
                rp++;
            }
            memcpy (labels1 + j0, labels2.data() + j0,
                    sizeof(labels1[0]) * (k - j0));
            memcpy (dis1 + j0, dis2.data() + j0,
                    sizeof(dis2[0]) * (k - j0));
        }
    }

}


long IndexIVFFlatDedup::remove_ids(const IDSelector& sel)
{
    std::unordered_map<idx_t, idx_t> replace;
    std::vector<std::pair<idx_t, idx_t> > toadd;
    for (auto it = instances.begin(); it != instances.end(); ) {
        if (sel.is_member(it->first)) {
            // then we erase this entry
            if (!sel.is_member(it->second)) {
                // if the second is not erased
                if (replace.count(it->first) == 0) {
                    replace[it->first] = it->second;
                } else { // remember we should add an element
                    std::pair<idx_t, idx_t> new_entry (
                          replace[it->first], it->second);
                    toadd.push_back(new_entry);
                }
            }
            it = instances.erase(it);
        } else {
            if (sel.is_member(it->second)) {
                it = instances.erase(it);
            } else {
                ++it;
            }
        }
    }

    instances.insert (toadd.begin(), toadd.end());

    // mostly copied from IndexIVF.cpp

    FAISS_THROW_IF_NOT_MSG (!maintain_direct_map,
                    "direct map remove not implemented");

    std::vector<long> toremove(nlist);

#pragma omp parallel for
    for (long i = 0; i < nlist; i++) {
        long l0 = invlists->list_size (i), l = l0, j = 0;
        InvertedLists::ScopedIds idsi (invlists, i);
        while (j < l) {
            if (sel.is_member (idsi[j])) {
                if (replace.count(idsi[j]) == 0) {
                    l--;
                    invlists->update_entry (
                        i, j,
                        invlists->get_single_id (i, l),
                        InvertedLists::ScopedCodes (invlists, i, l).get());
                } else {
                    invlists->update_entry (
                        i, j,
                        replace[idsi[j]],
                        InvertedLists::ScopedCodes (invlists, i, j).get());
                    j++;
                }
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


void IndexIVFFlatDedup::range_search(
        idx_t ,
        const float* ,
        float ,
        RangeSearchResult* ) const
{
    FAISS_THROW_MSG ("not implemented");
}

void IndexIVFFlatDedup::update_vectors (int , idx_t *, const float *)
{
    FAISS_THROW_MSG ("not implemented");
}


void IndexIVFFlatDedup::reconstruct_from_offset (
         long , long ,
         float* ) const
{
    FAISS_THROW_MSG ("not implemented");
}




} // namespace faiss
