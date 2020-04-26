/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/ScalarQuantizer.h>

#include <cstdio>
#include <algorithm>
#include <omp.h>

#include <faiss/FaissHook.h>
#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/ScalarQuantizerCodec.h>

namespace faiss {

/*******************************************************************
 * ScalarQuantizer implementation
 *
 * The main source of complexity is to support combinations of 4
 * variants without incurring runtime tests or virtual function calls:
 *
 * - 4 / 8 bits per code component
 * - uniform / non-uniform
 * - IP / L2 distance search
 * - scalar / AVX distance computation
 *
 * The appropriate Quantizer object is returned via select_quantizer
 * that hides the template mess.
 ********************************************************************/



/*******************************************************************
 * ScalarQuantizer implementation
 ********************************************************************/

ScalarQuantizer::ScalarQuantizer
          (size_t d, QuantizerType qtype):
              qtype (qtype), rangestat(RangeStat::RS_minmax), rangestat_arg(0), d (d)
{
    switch (qtype) {
    case QuantizerType::QT_8bit:
    case QuantizerType::QT_8bit_uniform:
    case QuantizerType::QT_8bit_direct:
        code_size = d;
        break;
    case QuantizerType::QT_4bit:
    case QuantizerType::QT_4bit_uniform:
        code_size = (d + 1) / 2;
        break;
    case QuantizerType::QT_6bit:
        code_size = (d * 6 + 7) / 8;
        break;
    case QuantizerType::QT_fp16:
        code_size = d * 2;
        break;
    }
}

ScalarQuantizer::ScalarQuantizer ():
    qtype(QuantizerType::QT_8bit),
    rangestat(RangeStat::RS_minmax), rangestat_arg(0), d (0), code_size(0)
{}

void ScalarQuantizer::train (size_t n, const float *x)
{
    int bit_per_dim =
        qtype == QuantizerType::QT_4bit_uniform ? 4 :
        qtype == QuantizerType::QT_4bit ? 4 :
        qtype == QuantizerType::QT_6bit ? 6 :
        qtype == QuantizerType::QT_8bit_uniform ? 8 :
        qtype == QuantizerType::QT_8bit ? 8 : -1;

    switch (qtype) {
    case QuantizerType::QT_4bit_uniform:
    case QuantizerType::QT_8bit_uniform:
        train_Uniform (rangestat, rangestat_arg,
                       n * d, 1 << bit_per_dim, x, trained);
        break;
    case QuantizerType::QT_4bit:
    case QuantizerType::QT_8bit:
    case QuantizerType::QT_6bit:
        train_NonUniform (rangestat, rangestat_arg,
                          n, d, 1 << bit_per_dim, x, trained);
        break;
    case QuantizerType::QT_fp16:
    case QuantizerType::QT_8bit_direct:
        // no training necessary
        break;
    }
}

void ScalarQuantizer::train_residual(size_t n,
                                     const float *x,
                                     Index *quantizer,
                                     bool by_residual,
                                     bool verbose)
{
    const float * x_in = x;

    // 100k points more than enough
    x = fvecs_maybe_subsample (
         d, (size_t*)&n, 100000,
         x, verbose, 1234);

    ScopeDeleter<float> del_x (x_in == x ? nullptr : x);

    if (by_residual) {
        std::vector<Index::idx_t> idx(n);
        quantizer->assign (n, x, idx.data());

        std::vector<float> residuals(n * d);
        quantizer->compute_residual_n (n, x, residuals.data(), idx.data());

        train (n, residuals.data());
    } else {
        train (n, x);
    }
}


Quantizer *ScalarQuantizer::select_quantizer () const
{
    /* use hook to decide use AVX512 or not */
    sq_sel_quantizer(qtype, d, trained);
}


void ScalarQuantizer::compute_codes (const float * x,
                                     uint8_t * codes,
                                     size_t n) const
{
    std::unique_ptr<Quantizer> squant(select_quantizer ());

    memset (codes, 0, code_size * n);
#pragma omp parallel for
    for (size_t i = 0; i < n; i++)
        squant->encode_vector (x + i * d, codes + i * code_size);
}

void ScalarQuantizer::decode (const uint8_t *codes, float *x, size_t n) const
{
    std::unique_ptr<Quantizer> squant(select_quantizer ());

#pragma omp parallel for
    for (size_t i = 0; i < n; i++)
        squant->decode_vector (codes + i * code_size, x + i * d);
}


SQDistanceComputer *
ScalarQuantizer::get_distance_computer (MetricType metric) const
{
    FAISS_THROW_IF_NOT(metric == METRIC_L2 || metric == METRIC_INNER_PRODUCT);
    /* use hook to decide use AVX512 or not */
    if (metric == METRIC_L2) {
        return sq_get_distance_computer_L2(qtype, d, trained);
    } else {
        return sq_get_distance_computer_IP(qtype, d, trained);
    }
}


/*******************************************************************
 * IndexScalarQuantizer/IndexIVFScalarQuantizer scanner object
 *
 * It is an InvertedListScanner, but is designed to work with
 * IndexScalarQuantizer as well.
 ********************************************************************/

namespace {

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
                       ConcurrentBitsetPtr bitset) const override
    {
        size_t nup = 0;

        for (size_t j = 0; j < list_size; j++) {
            if(!bitset || !bitset->test(ids[j])){
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
                           ConcurrentBitsetPtr bitset = nullptr) const override
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
                       ConcurrentBitsetPtr bitset) const override
    {
        size_t nup = 0;
        for (size_t j = 0; j < list_size; j++) {
            if(!bitset || !bitset->test(ids[j])){
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
                           ConcurrentBitsetPtr bitset = nullptr) const override
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

template<class DCClass>
InvertedListScanner* sel2_InvertedListScanner
      (const ScalarQuantizer *sq,
       const Index *quantizer, bool store_pairs, bool r)
{
    if (DCClass::Sim::metric_type == METRIC_L2) {
        return new IVFSQScannerL2<DCClass>(sq->d, sq->trained, sq->code_size,
                                           quantizer, store_pairs, r);
    } else if (DCClass::Sim::metric_type == METRIC_INNER_PRODUCT) {
        return new IVFSQScannerIP<DCClass>(sq->d, sq->trained, sq->code_size,
                                           store_pairs, r);
    } else {
        FAISS_THROW_MSG("unsupported metric type");
    }
}

template<class Similarity, class Codec, bool uniform>
InvertedListScanner* sel12_InvertedListScanner
        (const ScalarQuantizer *sq,
         const Index *quantizer, bool store_pairs, bool r)
{
    constexpr int SIMDWIDTH = Similarity::simdwidth;
    using QuantizerClass = QuantizerTemplate<Codec, uniform, SIMDWIDTH>;
    using DCClass = DCTemplate<QuantizerClass, Similarity, SIMDWIDTH>;
    return sel2_InvertedListScanner<DCClass> (sq, quantizer, store_pairs, r);
}


template<class Similarity>
InvertedListScanner* sel1_InvertedListScanner
        (const ScalarQuantizer *sq, const Index *quantizer,
         bool store_pairs, bool r)
{
    constexpr int SIMDWIDTH = Similarity::simdwidth;
    switch(sq->qtype) {
    case QuantizerType::QT_8bit_uniform:
        return sel12_InvertedListScanner
            <Similarity, Codec8bit, true>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_4bit_uniform:
        return sel12_InvertedListScanner
            <Similarity, Codec4bit, true>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_8bit:
        return sel12_InvertedListScanner
            <Similarity, Codec8bit, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_4bit:
        return sel12_InvertedListScanner
            <Similarity, Codec4bit, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_6bit:
        return sel12_InvertedListScanner
            <Similarity, Codec6bit, false>(sq, quantizer, store_pairs, r);
    case QuantizerType::QT_fp16:
        return sel2_InvertedListScanner
            <DCTemplate<QuantizerFP16<SIMDWIDTH>, Similarity, SIMDWIDTH> >
            (sq, quantizer, store_pairs, r);
    case QuantizerType::QT_8bit_direct:
        if (sq->d % 16 == 0) {
            return sel2_InvertedListScanner
                <DistanceComputerByte<Similarity, SIMDWIDTH> >
                (sq, quantizer, store_pairs, r);
        } else {
            return sel2_InvertedListScanner
                <DCTemplate<Quantizer8bitDirect<SIMDWIDTH>,
                            Similarity, SIMDWIDTH> >
                (sq, quantizer, store_pairs, r);
        }
    }

    FAISS_THROW_MSG ("unknown qtype");
    return nullptr;
}

template<int SIMDWIDTH>
InvertedListScanner* sel0_InvertedListScanner
        (MetricType mt, const ScalarQuantizer *sq,
         const Index *quantizer, bool store_pairs, bool by_residual)
{
    if (mt == METRIC_L2) {
        return sel1_InvertedListScanner<SimilarityL2<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    } else if (mt == METRIC_INNER_PRODUCT) {
        return sel1_InvertedListScanner<SimilarityIP<SIMDWIDTH> >
            (sq, quantizer, store_pairs, by_residual);
    } else {
        FAISS_THROW_MSG("unsupported metric type");
    }
}


} // anonymous namespace


InvertedListScanner* ScalarQuantizer::select_InvertedListScanner
        (MetricType mt, const Index *quantizer,
         bool store_pairs, bool by_residual) const
{
    if (d % 16 == 0 && support_avx512()) {
        return sel0_InvertedListScanner<16>
                (mt, this, quantizer, store_pairs, by_residual);
    } if (d % 8 == 0) {
        return sel0_InvertedListScanner<8>
            (mt, this, quantizer, store_pairs, by_residual);
    } else {
        return sel0_InvertedListScanner<1>
            (mt, this, quantizer, store_pairs, by_residual);
    }
}



} // namespace faiss
