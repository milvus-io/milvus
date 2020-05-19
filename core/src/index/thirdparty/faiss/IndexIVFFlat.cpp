/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexIVFFlat.h>

#include <cstdio>

#include <faiss/IndexFlat.h>

#include <faiss/utils/distances.h>
#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>


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


void IndexIVFFlat::add_with_ids (idx_t n, const float * x, const idx_t *xids)
{
    add_core (n, x, xids, nullptr);
}

void IndexIVFFlat::add_core (idx_t n, const float * x, const int64_t *xids,
                             const int64_t *precomputed_idx)

{
    FAISS_THROW_IF_NOT (is_trained);
    assert (invlists);
    direct_map.check_can_add (xids);
    const int64_t * idx;
    ScopeDeleter<int64_t> del;

    if (precomputed_idx) {
        idx = precomputed_idx;
    } else {
        int64_t * idx0 = new int64_t [n];
        del.set (idx0);
        quantizer->assign (n, x, idx0);
        idx = idx0;
    }
    int64_t n_add = 0;
    for (size_t i = 0; i < n; i++) {
        idx_t id = xids ? xids[i] : ntotal + i;
        idx_t list_no = idx [i];
        size_t offset;

        if (list_no >= 0) {
            const float *xi = x + i * d;
            offset = invlists->add_entry (
                     list_no, id, (const uint8_t*) xi);
            n_add++;
        } else {
            offset = 0;
        }
        direct_map.add_single_id (id, list_no, offset);
    }

    if (verbose) {
        printf("IndexIVFFlat::add_core: added %ld / %ld vectors\n",
               n_add, n);
    }
    ntotal += n;
}

void IndexIVFFlat::encode_vectors(idx_t n, const float* x,
                                  const idx_t * list_nos,
                                  uint8_t * codes,
                                  bool include_listnos) const
{
    if (!include_listnos) {
        memcpy (codes, x, code_size * n);
    } else {
        size_t coarse_size = coarse_code_size ();
        for (size_t i = 0; i < n; i++) {
            int64_t list_no = list_nos [i];
            uint8_t *code = codes + i * (code_size + coarse_size);
            const float *xi = x + i * d;
            if (list_no >= 0) {
                encode_listno (list_no, code);
                memcpy (code + coarse_size, xi, code_size);
            } else {
                memset (code, 0, code_size + coarse_size);
            }

        }
    }
}

void IndexIVFFlat::sa_decode (idx_t n, const uint8_t *bytes,
                                      float *x) const
{
    size_t coarse_size = coarse_code_size ();
    for (size_t i = 0; i < n; i++) {
        const uint8_t *code = bytes + i * (code_size + coarse_size);
        float *xi = x + i * d;
        memcpy (xi, code + coarse_size, code_size);
    }
}


namespace {


template<MetricType metric, class C>
struct IVFFlatScanner: InvertedListScanner {
    size_t d;
    bool store_pairs;

    IVFFlatScanner(size_t d, bool store_pairs):
        d(d), store_pairs(store_pairs) {}

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
                int64_t id = store_pairs ? lo_build (list_no, j) : ids[j];
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
                int64_t id = store_pairs ? lo_build (list_no, j) : ids[j];
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
        return new IVFFlatScanner<
            METRIC_INNER_PRODUCT, CMin<float, int64_t> > (d, store_pairs);
    } else if (metric_type == METRIC_L2) {
        return new IVFFlatScanner<
            METRIC_L2, CMax<float, int64_t> >(d, store_pairs);
    } else {
        FAISS_THROW_MSG("metric type not supported");
    }
    return nullptr;
}




void IndexIVFFlat::reconstruct_from_offset (int64_t list_no, int64_t offset,
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

    int64_t n2 = 0;
    for (int64_t i = 0; i < n; i++) {
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
           idx_t na, const float* x, const idx_t* xids)
{

    FAISS_THROW_IF_NOT (is_trained);
    assert (invlists);
    FAISS_THROW_IF_NOT_MSG (direct_map.no(),
           "IVFFlatDedup not implemented with direct_map");
    int64_t * idx = new int64_t [na];
    ScopeDeleter<int64_t> del (idx);
    quantizer->assign (na, x, idx);

    int64_t n_add = 0, n_dup = 0;
    // TODO make a omp loop with this
    for (size_t i = 0; i < na; i++) {
        idx_t id = xids ? xids[i] : ntotal + i;
        int64_t list_no = idx [i];

        if (list_no < 0) {
            continue;
        }
        const float *xi = x + i * d;

        // search if there is already an entry with that id
        InvertedLists::ScopedCodes codes (invlists, list_no);

        int64_t n = invlists->list_size (list_no);
        int64_t offset = -1;
        for (int64_t o = 0; o < n; o++) {
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

    for (int64_t i = 0; i < n; i++) {
        idx_t *labels1 = labels + i * k;
        float *dis1 = distances + i * k;
        int64_t j = 0;
        for (; j < k; j++) {
            if (instances.find (labels1[j]) != instances.end ()) {
                // a duplicate: special handling
                break;
            }
        }
        if (j < k) {
            // there are duplicates, special handling
            int64_t j0 = j;
            int64_t rp = j;
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


size_t IndexIVFFlatDedup::remove_ids(const IDSelector& sel)
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

    FAISS_THROW_IF_NOT_MSG (direct_map.no(),
                    "direct map remove not implemented");

    std::vector<int64_t> toremove(nlist);

#pragma omp parallel for
    for (int64_t i = 0; i < nlist; i++) {
        int64_t l0 = invlists->list_size (i), l = l0, j = 0;
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
    int64_t nremove = 0;
    for (int64_t i = 0; i < nlist; i++) {
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

void IndexIVFFlatDedup::update_vectors (int , const idx_t *, const float *)
{
    FAISS_THROW_MSG ("not implemented");
}


void IndexIVFFlatDedup::reconstruct_from_offset (
         int64_t , int64_t , float* ) const
{
    FAISS_THROW_MSG ("not implemented");
}




} // namespace faiss
