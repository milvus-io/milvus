/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


/*
 * Structures that collect search results from distance computations
 */

#pragma once


#include <faiss/utils/Heap.h>
#include <faiss/utils/partitioning.h>
#include <faiss/impl/AuxIndexStructures.h>


namespace faiss {



/*****************************************************************
 * Heap based result handler
 *****************************************************************/


template<class C>
struct HeapResultHandler {

    using T = typename C::T;
    using TI = typename C::TI;

    int nq;
    T *heap_dis_tab;
    TI *heap_ids_tab;

    int64_t k;  // number of results to keep

    HeapResultHandler(
        size_t nq,
        T * heap_dis_tab, TI * heap_ids_tab,
        size_t k):
        nq(nq),
        heap_dis_tab(heap_dis_tab), heap_ids_tab(heap_ids_tab), k(k)
    {

    }

    /******************************************************
     * API for 1 result at a time (each SingleResultHandler is
     * called from 1 thread)
     */

    struct SingleResultHandler {
        HeapResultHandler & hr;
        size_t k;

        T *heap_dis;
        TI *heap_ids;
        T thresh;

        SingleResultHandler(HeapResultHandler &hr): hr(hr), k(hr.k) {}

        /// begin results for query # i
        void begin(size_t i) {
            heap_dis = hr.heap_dis_tab + i * k;
            heap_ids = hr.heap_ids_tab + i * k;
            heap_heapify<C> (k, heap_dis, heap_ids);
            thresh = heap_dis[0];
        }

        /// add one result for query i
        void add_result(T dis, TI idx) {
            if (C::cmp(heap_dis[0], dis)) {
                heap_pop<C>(k, heap_dis, heap_ids);
                heap_push<C>(k, heap_dis, heap_ids, dis, idx);
                thresh = heap_dis[0];
            }
        }

        /// series of results for query i is done
        void end() {
            heap_reorder<C> (k, heap_dis, heap_ids);
        }
    };


    /******************************************************
     * API for multiple results (called from 1 thread)
     */

    size_t i0, i1;

    /// begin
    void begin_multiple(size_t i0, size_t i1) {
        this->i0 = i0;
        this->i1 = i1;
        for(size_t i = i0; i < i1; i++) {
            heap_heapify<C> (k, heap_dis_tab + i * k, heap_ids_tab + i * k);
        }
    }

    /// add results for query i0..i1 and j0..j1
    void add_results(size_t j0, size_t j1, const T *dis_tab) {
        // maybe parallel for
        for (size_t i = i0; i < i1; i++) {
            T * heap_dis = heap_dis_tab + i * k;
            TI * heap_ids = heap_ids_tab + i * k;
            T thresh = heap_dis[0];
            for (size_t j = j0; j < j1; j++) {
                T dis = *dis_tab++;
                if (C::cmp(thresh, dis)) {
                    heap_pop<C>(k, heap_dis, heap_ids);
                    heap_push<C>(k, heap_dis, heap_ids, dis, j);
                    thresh = heap_dis[0];
                }
            }
        }
    }

    /// series of results for queries i0..i1 is done
    void end_multiple() {
        // maybe parallel for
        for(size_t i = i0; i < i1; i++) {
            heap_reorder<C> (k, heap_dis_tab + i * k, heap_ids_tab + i * k);
        }
    }

};

/*****************************************************************
 * Reservoir result handler
 *
 * A reservoir is a result array of size capacity > n (number of requested
 * results) all results below a threshold are stored in an arbitrary order. When
 * the capacity is reached, a new threshold is chosen by partitionning the
 * distance array.
 *****************************************************************/



/// Reservoir for a single query
template<class C>
struct ReservoirTopN {
    using T = typename C::T;
    using TI = typename C::TI;

    T *vals;
    TI *ids;

    size_t i; // number of stored elements
    size_t n; // number of requested elements
    size_t capacity;  // size of storage

    T threshold; // current threshold

    ReservoirTopN() {}

    ReservoirTopN(
        size_t n, size_t capacity,
        T *vals, TI *ids
    ):
        vals(vals), ids(ids),
        i(0), n(n), capacity(capacity) {
        assert(n < capacity);
        threshold = C::neutral();
    }

    void add(T val, TI id) {
        if (C::cmp(threshold, val)) {
            if (i == capacity) {
                shrink_fuzzy();
            }
            vals[i] = val;
            ids[i] = id;
            i++;
        }
    }

    // reduce storage from capacity to anything
    // between n and (capacity + n) / 2
    void shrink_fuzzy() {
        assert(i == capacity);

        threshold = partition_fuzzy<C>(
            vals, ids, capacity, n, (capacity + n) / 2,
            &i);
    }

    void to_result(T *heap_dis, TI *heap_ids) const {

        for (int j = 0; j < std::min(i, n); j++) {
            heap_push<C>(
                j + 1, heap_dis, heap_ids,
                vals[j], ids[j]
            );
        }

        if (i < n) {
            heap_reorder<C> (i, heap_dis, heap_ids);
            // add empty results
            heap_heapify<C> (n - i, heap_dis + i, heap_ids + i);
        } else {
            // add remaining elements
            heap_addn<C> (n, heap_dis, heap_ids, vals + n, ids + n, i - n);
            heap_reorder<C> (n, heap_dis, heap_ids);
        }

    }

};



template<class C>
struct ReservoirResultHandler {

    using T = typename C::T;
    using TI = typename C::TI;

    int nq;
    T *heap_dis_tab;
    TI *heap_ids_tab;

    int64_t k;  // number of results to keep
    size_t capacity; // capacity of the reservoirs

    ReservoirResultHandler(
        size_t nq,
        T * heap_dis_tab, TI * heap_ids_tab,
        size_t k):
        nq(nq),
        heap_dis_tab(heap_dis_tab), heap_ids_tab(heap_ids_tab), k(k)
    {
        // double then round up to multiple of 16 (for SIMD alignment)
        capacity = (2 * k + 15) & ~15;
    }

    /******************************************************
     * API for 1 result at a time (each SingleResultHandler is
     * called from 1 thread)
     */

    struct SingleResultHandler {
        ReservoirResultHandler & hr;

        std::vector<T> reservoir_dis;
        std::vector<TI> reservoir_ids;
        ReservoirTopN<C> res1;

        SingleResultHandler(ReservoirResultHandler &hr):
            hr(hr), reservoir_dis(hr.capacity), reservoir_ids(hr.capacity)
        {
        }

        size_t i;

        /// begin results for query # i
        void begin(size_t i) {
            res1 = ReservoirTopN<C>(
                hr.k, hr.capacity, reservoir_dis.data(), reservoir_ids.data());
            this->i = i;
        }

        /// add one result for query i
        void add_result(T dis, TI idx) {
            res1.add(dis, idx);
        }

        /// series of results for query i is done
        void end() {
            T * heap_dis = hr.heap_dis_tab + i * hr.k;
            TI * heap_ids = hr.heap_ids_tab + i * hr.k;
            res1.to_result(heap_dis, heap_ids);
        }
    };

    /******************************************************
     * API for multiple results (called from 1 thread)
     */

    size_t i0, i1;

    std::vector<T> reservoir_dis;
    std::vector<TI> reservoir_ids;
    std::vector<ReservoirTopN<C>> reservoirs;

    /// begin
    void begin_multiple(size_t i0, size_t i1) {
        this->i0 = i0;
        this->i1 = i1;
        reservoir_dis.resize((i1 - i0) * capacity);
        reservoir_ids.resize((i1 - i0) * capacity);
        reservoirs.clear();
        for (size_t i = i0; i < i1; i++) {
            reservoirs.emplace_back(
                k, capacity,
                reservoir_dis.data() + (i - i0) * capacity,
                reservoir_ids.data() + (i - i0) * capacity
            );
        }
    }

    /// add results for query i0..i1 and j0..j1
    void add_results(size_t j0, size_t j1, const T *dis_tab) {
        // maybe parallel for
        for (size_t i = i0; i < i1; i++) {
            ReservoirTopN<C> & reservoir = reservoirs[i - i0];
            for (size_t j = j0; j < j1; j++) {
                T dis = *dis_tab++;
                reservoir.add(dis, j);
            }
        }
    }

    /// series of results for queries i0..i1 is done
    void end_multiple() {
        // maybe parallel for
        for(size_t i = i0; i < i1; i++) {
            reservoirs[i - i0].to_result(
                heap_dis_tab + i * k, heap_ids_tab + i * k);
        }
    }

};


/*****************************************************************
 * Result handler for range searches
 *****************************************************************/



template<class C>
struct RangeSearchResultHandler {
    using T = typename C::T;
    using TI = typename C::TI;

    RangeSearchResult *res;
    float radius;

    RangeSearchResultHandler(RangeSearchResult *res, float radius):
        res(res), radius(radius)
    {}

    /******************************************************
     * API for 1 result at a time (each SingleResultHandler is
     * called from 1 thread)
     ******************************************************/

    struct SingleResultHandler {
        // almost the same interface as RangeSearchResultHandler
        RangeSearchPartialResult pres;
        float radius;
        RangeQueryResult *qr = nullptr;

        SingleResultHandler(RangeSearchResultHandler &rh):
            pres(rh.res), radius(rh.radius)
        {}

        /// begin results for query # i
        void begin(size_t i) {
            qr = &pres.new_result(i);
        }

        /// add one result for query i
        void add_result(T dis, TI idx) {

            if (C::cmp(radius, dis)) {
                qr->add(dis, idx);
            }
        }

        /// series of results for query i is done
        void end() {
        }

        ~SingleResultHandler() {
            pres.finalize();
        }
    };

    /******************************************************
     * API for multiple results (called from 1 thread)
     ******************************************************/

    size_t i0, i1;

    std::vector <RangeSearchPartialResult *> partial_results;
    std::vector <size_t> j0s;
    int pr = 0;

    /// begin
    void begin_multiple(size_t i0, size_t i1) {
        this->i0 = i0;
        this->i1 = i1;
    }

    /// add results for query i0..i1 and j0..j1

    void add_results(size_t j0, size_t j1, const T *dis_tab) {
        RangeSearchPartialResult *pres;
        // there is one RangeSearchPartialResult structure per j0
        // (= block of columns of the large distance matrix)
        // it is a bit tricky to find the poper PartialResult structure
        // because the inner loop is on db not on queries.

        if (pr < j0s.size() && j0 == j0s[pr]) {
            pres = partial_results[pr];
            pr++;
        } else if (j0 == 0 && j0s.size() > 0) {
            pr = 0;
            pres = partial_results[pr];
            pr++;
        } else { // did not find this j0
            pres = new RangeSearchPartialResult (res);
            partial_results.push_back(pres);
            j0s.push_back(j0);
            pr = partial_results.size();
        }

        for (size_t i = i0; i < i1; i++) {
            const float *ip_line = dis_tab + (i - i0) * (j1 - j0);
            RangeQueryResult & qres = pres->new_result (i);

            for (size_t j = j0; j < j1; j++) {
                float dis = *ip_line++;
                if (C::cmp(radius, dis)) {
                    qres.add (dis, j);
                }
            }
        }
    }

    void end_multiple() {

    }

    ~RangeSearchResultHandler() {
        if (partial_results.size() > 0) {
            RangeSearchPartialResult::merge (partial_results);
        }
    }

};




}  // namespace faiss

