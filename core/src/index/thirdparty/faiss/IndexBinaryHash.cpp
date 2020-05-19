/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved
// -*- c++ -*-

#include <faiss/IndexBinaryHash.h>

#include <cstdio>
#include <memory>

#include <faiss/utils/hamming.h>
#include <faiss/utils/utils.h>

#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/FaissAssert.h>


namespace faiss {

void IndexBinaryHash::InvertedList::add (
        idx_t id, size_t code_size, const uint8_t *code)
{
    ids.push_back(id);
    vecs.insert(vecs.end(), code, code + code_size);
}

IndexBinaryHash::IndexBinaryHash(int d, int b):
    IndexBinary(d), b(b), nflip(0)
{
    is_trained = true;
}

IndexBinaryHash::IndexBinaryHash(): b(0), nflip(0)
{
    is_trained = true;
}

void IndexBinaryHash::reset()
{
    invlists.clear();
    ntotal = 0;
}


void IndexBinaryHash::add(idx_t n, const uint8_t *x)
{
    add_with_ids(n, x, nullptr);
}

void IndexBinaryHash::add_with_ids(idx_t n, const uint8_t *x, const idx_t *xids)
{
    uint64_t mask = ((uint64_t)1 << b) - 1;
    // simplistic add function. Cannot really be parallelized.

    for (idx_t i = 0; i < n; i++) {
        idx_t id = xids ? xids[i] : ntotal + i;
        const uint8_t * xi = x + i * code_size;
        idx_t hash = *((uint64_t*)xi) & mask;
        invlists[hash].add(id, code_size, xi);
    }
    ntotal += n;
}

namespace {


/** Enumerate all bit vectors of size nbit with up to maxflip 1s
 * test in P127257851 P127258235
 */
struct FlipEnumerator {
    int nbit, nflip, maxflip;
    uint64_t mask, x;

    FlipEnumerator (int nbit, int maxflip): nbit(nbit), maxflip(maxflip) {
        nflip = 0;
        mask = 0;
        x = 0;
    }

    bool next() {
        if (x == mask) {
            if (nflip == maxflip) {
                return false;
            }
            // increase Hamming radius
            nflip++;
            mask = (((uint64_t)1 << nflip) - 1);
            x = mask << (nbit - nflip);
            return true;
        }

        int i = __builtin_ctzll(x);

        if (i > 0) {
            x ^= (uint64_t)3 << (i - 1);
        } else {
            // nb of LSB 1s
            int n1 = __builtin_ctzll(~x);
            // clear them
            x &= ((uint64_t)(-1) << n1);
            int n2 = __builtin_ctzll(x);
            x ^= (((uint64_t)1 << (n1 + 2)) - 1) << (n2 - n1 - 1);
        }
        return true;
    }

};

using idx_t = Index::idx_t;


struct RangeSearchResults {
    int radius;
    RangeQueryResult &qres;

    inline void add (float dis, idx_t id) {
        if (dis < radius) {
            qres.add (dis, id);
        }
    }

};

struct KnnSearchResults {
    // heap params
    idx_t k;
    int32_t * heap_sim;
    idx_t * heap_ids;

    using C = CMax<int, idx_t>;

    inline void add (float dis, idx_t id) {
        if (dis < heap_sim[0]) {
            heap_pop<C> (k, heap_sim, heap_ids);
            heap_push<C> (k, heap_sim, heap_ids, dis, id);
        }
    }

};

template<class HammingComputer, class SearchResults>
void
search_single_query_template(const IndexBinaryHash & index, const uint8_t *q,
                    SearchResults &res,
                    size_t &n0, size_t &nlist, size_t &ndis)
{
    size_t code_size = index.code_size;
    uint64_t mask = ((uint64_t)1 << index.b) - 1;
    uint64_t qhash = *((uint64_t*)q) & mask;
    HammingComputer hc (q, code_size);
    FlipEnumerator fe(index.b, index.nflip);

    // loop over neighbors that are at most at nflip bits
    do {
        uint64_t hash = qhash ^ fe.x;
        auto it = index.invlists.find (hash);

        if (it == index.invlists.end()) {
            continue;
        }

        const IndexBinaryHash::InvertedList &il = it->second;

        size_t nv = il.ids.size();

        if (nv == 0) {
            n0++;
        } else {
            const uint8_t *codes = il.vecs.data();
            for (size_t i = 0; i < nv; i++) {
                int dis = hc.hamming (codes);
                res.add(dis, il.ids[i]);
                codes += code_size;
            }
            ndis += nv;
            nlist++;
        }
    } while(fe.next());
}

template<class SearchResults>
void
search_single_query(const IndexBinaryHash & index, const uint8_t *q,
                    SearchResults &res,
                    size_t &n0, size_t &nlist, size_t &ndis)
{
#define HC(name) search_single_query_template<name>(index, q, res, n0, nlist, ndis);
    switch(index.code_size) {
    case 4: HC(HammingComputer4); break;
    case 8: HC(HammingComputer8); break;
    case 16: HC(HammingComputer16); break;
    case 20: HC(HammingComputer20); break;
    case 32: HC(HammingComputer32); break;
    default:
        if (index.code_size % 8 == 0) {
            HC(HammingComputerM8);
        } else {
            HC(HammingComputerDefault);
        }
    }
#undef HC
}


} // anonymous namespace



void IndexBinaryHash::range_search(idx_t n, const uint8_t *x, int radius,
                                   RangeSearchResult *result) const
{

    size_t nlist = 0, ndis = 0, n0 = 0;

#pragma omp parallel if(n > 100) reduction(+: ndis, n0, nlist)
    {
        RangeSearchPartialResult pres (result);

#pragma omp for
        for (size_t i = 0; i < n; i++) { // loop queries
            RangeQueryResult & qres = pres.new_result (i);
            RangeSearchResults res = {radius, qres};
            const uint8_t *q = x + i * code_size;

            search_single_query (*this, q, res, n0, nlist, ndis);

        }
        pres.finalize ();
    }
    indexBinaryHash_stats.nq += n;
    indexBinaryHash_stats.n0 += n0;
    indexBinaryHash_stats.nlist += nlist;
    indexBinaryHash_stats.ndis += ndis;
}

void IndexBinaryHash::search(idx_t n, const uint8_t *x, idx_t k,
                             int32_t *distances, idx_t *labels) const
{

    using HeapForL2 = CMax<int32_t, idx_t>;
    size_t nlist = 0, ndis = 0, n0 = 0;

#pragma omp parallel for if(n > 100) reduction(+: nlist, ndis, n0)
    for (size_t i = 0; i < n; i++) {
        int32_t * simi = distances + k * i;
        idx_t * idxi = labels + k * i;

        heap_heapify<HeapForL2> (k, simi, idxi);
        KnnSearchResults res = {k, simi, idxi};
        const uint8_t *q = x + i * code_size;

        search_single_query (*this, q, res, n0, nlist, ndis);

    }
    indexBinaryHash_stats.nq += n;
    indexBinaryHash_stats.n0 += n0;
    indexBinaryHash_stats.nlist += nlist;
    indexBinaryHash_stats.ndis += ndis;
}

size_t IndexBinaryHash::hashtable_size() const
{
    return invlists.size();
}


void IndexBinaryHash::display() const
{
    for (auto it = invlists.begin(); it != invlists.end(); ++it) {
        printf("%ld: [", it->first);
        const std::vector<idx_t> & v = it->second.ids;
        for (auto x: v) {
            printf("%ld ", 0 + x);
        }
        printf("]\n");

    }
}


void IndexBinaryHashStats::reset()
{
    memset ((void*)this, 0, sizeof (*this));
}

IndexBinaryHashStats indexBinaryHash_stats;

/*******************************************************
 * IndexBinaryMultiHash implementation
 ******************************************************/


IndexBinaryMultiHash::IndexBinaryMultiHash(int d, int nhash, int b):
    IndexBinary(d),
    storage(new IndexBinaryFlat(d)), own_fields(true),
    maps(nhash), nhash(nhash), b(b), nflip(0)
{
    FAISS_THROW_IF_NOT(nhash * b <= d);
}

IndexBinaryMultiHash::IndexBinaryMultiHash():
    storage(nullptr), own_fields(true),
    nhash(0), b(0), nflip(0)
{}

IndexBinaryMultiHash::~IndexBinaryMultiHash()
{
    if (own_fields) {
        delete storage;
    }
}


void IndexBinaryMultiHash::reset()
{
    storage->reset();
    ntotal = 0;
    for(auto map: maps) {
        map.clear();
    }
}

void IndexBinaryMultiHash::add(idx_t n, const uint8_t *x)
{
    storage->add(n, x);
    // populate maps
    uint64_t mask = ((uint64_t)1 << b) - 1;

    for(idx_t i = 0; i < n; i++) {
        const uint8_t *xi = x + i * code_size;
        int ho = 0;
        for(int h = 0; h < nhash; h++) {
            uint64_t hash = *(uint64_t*)(xi + (ho >> 3)) >> (ho & 7);
            hash &= mask;
            maps[h][hash].push_back(i + ntotal);
            ho += b;
        }
    }
    ntotal += n;
}


namespace {

template <class HammingComputer, class SearchResults>
static
void verify_shortlist(
        const IndexBinaryFlat & index,
        const uint8_t * q,
        const std::unordered_set<Index::idx_t> & shortlist,
        SearchResults &res)
{
    size_t code_size = index.code_size;
    size_t nlist = 0, ndis = 0, n0 = 0;

    HammingComputer hc (q, code_size);
    const uint8_t *codes = index.xb.data();

    for (auto i: shortlist) {
        int dis = hc.hamming (codes + i * code_size);
        res.add(dis, i);
    }
}

template<class SearchResults>
void
search_1_query_multihash(const IndexBinaryMultiHash & index, const uint8_t *xi,
                         SearchResults &res,
                         size_t &n0, size_t &nlist, size_t &ndis)
{

    std::unordered_set<idx_t> shortlist;
    int b = index.b;
    uint64_t mask = ((uint64_t)1 << b) - 1;

    int ho = 0;
    for(int h = 0; h < index.nhash; h++) {
        uint64_t qhash = *(uint64_t*)(xi + (ho >> 3)) >> (ho & 7);
        qhash &= mask;
        const IndexBinaryMultiHash::Map & map = index.maps[h];

        FlipEnumerator fe(index.b, index.nflip);
        // loop over neighbors that are at most at nflip bits
        do {
            uint64_t hash = qhash ^ fe.x;
            auto it = map.find (hash);

            if (it != map.end()) {
                const std::vector<idx_t> & v = it->second;
                for (auto i: v) {
                    shortlist.insert(i);
                }
                nlist++;
            } else {
                n0++;
            }
        } while(fe.next());

        ho += b;
    }
    ndis += shortlist.size();

    // verify shortlist

#define HC(name) verify_shortlist<name> (*index.storage, xi, shortlist, res)
    switch(index.code_size) {
    case 4: HC(HammingComputer4); break;
    case 8: HC(HammingComputer8); break;
    case 16: HC(HammingComputer16); break;
    case 20: HC(HammingComputer20); break;
    case 32: HC(HammingComputer32); break;
    default:
        if (index.code_size % 8 == 0) {
            HC(HammingComputerM8);
        } else {
            HC(HammingComputerDefault);
        }
    }
#undef HC
}

} // anonymous namespace

void IndexBinaryMultiHash::range_search(idx_t n, const uint8_t *x, int radius,
                                   RangeSearchResult *result) const
{

    size_t nlist = 0, ndis = 0, n0 = 0;

#pragma omp parallel if(n > 100) reduction(+: ndis, n0, nlist)
    {
        RangeSearchPartialResult pres (result);

#pragma omp for
        for (size_t i = 0; i < n; i++) { // loop queries
            RangeQueryResult & qres = pres.new_result (i);
            RangeSearchResults res = {radius, qres};
            const uint8_t *q = x + i * code_size;

            search_1_query_multihash (*this, q, res, n0, nlist, ndis);

        }
        pres.finalize ();
    }
    indexBinaryHash_stats.nq += n;
    indexBinaryHash_stats.n0 += n0;
    indexBinaryHash_stats.nlist += nlist;
    indexBinaryHash_stats.ndis += ndis;
}

void IndexBinaryMultiHash::search(idx_t n, const uint8_t *x, idx_t k,
                             int32_t *distances, idx_t *labels) const
{

    using HeapForL2 = CMax<int32_t, idx_t>;
    size_t nlist = 0, ndis = 0, n0 = 0;

#pragma omp parallel for if(n > 100) reduction(+: nlist, ndis, n0)
    for (size_t i = 0; i < n; i++) {
        int32_t * simi = distances + k * i;
        idx_t * idxi = labels + k * i;

        heap_heapify<HeapForL2> (k, simi, idxi);
        KnnSearchResults res = {k, simi, idxi};
        const uint8_t *q = x + i * code_size;

        search_1_query_multihash (*this, q, res, n0, nlist, ndis);

    }
    indexBinaryHash_stats.nq += n;
    indexBinaryHash_stats.n0 += n0;
    indexBinaryHash_stats.nlist += nlist;
    indexBinaryHash_stats.ndis += ndis;
}

size_t IndexBinaryMultiHash::hashtable_size() const
{
    size_t tot = 0;
    for (auto map: maps) {
        tot += map.size();
    }

    return tot;
}


}
