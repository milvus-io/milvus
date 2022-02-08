/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include <faiss/IndexIVF.h>
#include <faiss/impl/ScalarQuantizerOp.h>

namespace faiss {

/**
 * The uniform quantizer has a range [vmin, vmax]. The range can be
 * the same for all dimensions (uniform) or specific per dimension
 * (default).
 */

struct ScalarQuantizer {

    QuantizerType qtype;

    /** The uniform encoder can estimate the range of representable
     * values of the unform encoder using different statistics. Here
     * rs = rangestat_arg */

    RangeStat rangestat;
    float rangestat_arg;

    /// dimension of input vectors
    size_t d;

    /// bytes per vector
    size_t code_size;

    /// trained values (including the range)
    std::vector<float> trained;

    ScalarQuantizer (size_t d, QuantizerType qtype);
    ScalarQuantizer ();

    void train (size_t n, const float *x);

    /// Used by an IVF index to train based on the residuals
    void train_residual (size_t n,
                         const float *x,
                         Index *quantizer,
                         bool by_residual,
                         bool verbose);

    /// same as compute_code for several vectors
    void compute_codes (const float * x,
                        uint8_t * codes,
                        size_t n) const ;

    /// decode a vector from a given code (or n vectors if third argument)
    void decode (const uint8_t *code, float *x, size_t n) const;


    /*****************************************************
     * Objects that provide methods for encoding/decoding, distance
     * computation and inverted list scanning
     *****************************************************/

    Quantizer * select_quantizer() const;

    SQDistanceComputer *get_distance_computer (MetricType metric = METRIC_L2)
        const;

    InvertedListScanner *select_InvertedListScanner
        (MetricType mt, const Index *quantizer, bool store_pairs,
         bool by_residual=false) const;

    size_t cal_size() { return sizeof(*this) + trained.size() * sizeof(float); }
};

template<class DCClass>
struct IVFSQScannerIP: InvertedListScanner {
    DCClass dc;
    bool store_pairs, by_residual;

    size_t code_size;

    idx_t list_no;  /// current list (set to 0 for Flat index
    float accu0;    /// added to all distances

    IVFSQScannerIP(int d, const std::vector<float> & trained,
                   size_t code_size, bool store_pairs,
                   bool by_residual):
        dc(d, trained), store_pairs(store_pairs),
        by_residual(by_residual),
        code_size(code_size), list_no(0), accu0(0)
    {}


    void set_query (const float *query) override {
        dc.set_query (query);
    }

    void set_list (idx_t list_no, float coarse_dis) override {
        this->list_no = list_no;
        accu0 = by_residual ? coarse_dis : 0;
    }

    float distance_to_code (const uint8_t *code) const final {
        return accu0 + dc.query_to_code (code);
    }

    size_t scan_codes (size_t list_size,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *simi, idx_t *idxi,
                       size_t k,
                       const BitsetView bitset) const override
    {
        size_t nup = 0;

        for (size_t j = 0; j < list_size; j++) {
            if(!bitset || !bitset.test(ids[j])){
                float accu = accu0 + dc.query_to_code (codes);

                if (accu > simi [0]) {
                    int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                    minheap_swap_top (k, simi, idxi, accu, id);
                    nup++;
                }
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t list_size,
                           const uint8_t *codes,
                           const idx_t *ids,
                           float radius,
                           RangeQueryResult & res,
                           const BitsetView bitset = nullptr) const override
    {
        for (size_t j = 0; j < list_size; j++) {
            float accu = accu0 + dc.query_to_code (codes);
            if (accu > radius) {
                int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                res.add (accu, id);
            }
            codes += code_size;
        }
    }
};


template<class DCClass>
struct IVFSQScannerL2: InvertedListScanner {
    DCClass dc;

    bool store_pairs, by_residual;
    size_t code_size;
    const Index *quantizer;
    idx_t list_no;    /// current inverted list
    const float *x;   /// current query

    std::vector<float> tmp;

    IVFSQScannerL2(int d, const std::vector<float> & trained,
                   size_t code_size, const Index *quantizer,
                   bool store_pairs, bool by_residual):
        dc(d, trained), store_pairs(store_pairs), by_residual(by_residual),
        code_size(code_size), quantizer(quantizer),
        list_no (0), x (nullptr), tmp (d)
    {
    }


    void set_query (const float *query) override {
        x = query;
        if (!quantizer) {
            dc.set_query (query);
        }
    }


    void set_list (idx_t list_no, float /*coarse_dis*/) override {
        if (by_residual) {
            this->list_no = list_no;
            // shift of x_in wrt centroid
            quantizer->Index::compute_residual (x, tmp.data(), list_no);
            dc.set_query (tmp.data ());
        } else {
            dc.set_query (x);
        }
    }

    float distance_to_code (const uint8_t *code) const final {
        return dc.query_to_code (code);
    }

    size_t scan_codes (size_t list_size,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *simi, idx_t *idxi,
                       size_t k,
                       const BitsetView bitset) const override
    {
        size_t nup = 0;
        for (size_t j = 0; j < list_size; j++) {
            if(!bitset || !bitset.test(ids[j])){
                float dis = dc.query_to_code (codes);

                if (dis < simi [0]) {
                    int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                    maxheap_swap_top (k, simi, idxi, dis, id);
                    nup++;
                }
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t list_size,
                           const uint8_t *codes,
                           const idx_t *ids,
                           float radius,
                           RangeQueryResult & res,
                           const BitsetView bitset = nullptr) const override
    {
        for (size_t j = 0; j < list_size; j++) {
            float dis = dc.query_to_code (codes);
            if (dis < radius) {
                int64_t id = store_pairs ? (list_no << 32 | j) : ids[j];
                res.add (dis, id);
            }
            codes += code_size;
        }
    }
};


} // namespace faiss
