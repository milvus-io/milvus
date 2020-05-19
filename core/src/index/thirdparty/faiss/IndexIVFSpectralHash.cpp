/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-


#include <faiss/IndexIVFSpectralHash.h>

#include <memory>
#include <algorithm>
#include <stdint.h>

#include <faiss/utils/hamming.h>
#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/VectorTransform.h>

namespace faiss {


IndexIVFSpectralHash::IndexIVFSpectralHash (
        Index * quantizer, size_t d, size_t nlist,
        int nbit, float period):
    IndexIVF (quantizer, d, nlist, (nbit + 7) / 8, METRIC_L2),
    nbit (nbit), period (period), threshold_type (Thresh_global)
{
    FAISS_THROW_IF_NOT (code_size % 4 == 0);
    RandomRotationMatrix *rr = new RandomRotationMatrix (d, nbit);
    rr->init (1234);
    vt = rr;
    own_fields = true;
    is_trained = false;
}

IndexIVFSpectralHash::IndexIVFSpectralHash():
    IndexIVF(), vt(nullptr), own_fields(false),
    nbit(0), period(0), threshold_type(Thresh_global)
{}

IndexIVFSpectralHash::~IndexIVFSpectralHash ()
{
    if (own_fields) {
        delete vt;
    }
}

namespace {


float median (size_t n, float *x) {
    std::sort(x, x + n);
    if (n % 2 == 1) {
        return x [n / 2];
    } else {
        return (x [n / 2 - 1] + x [n / 2]) / 2;
    }
}

}


void IndexIVFSpectralHash::train_residual (idx_t n, const float *x)
{
    if (!vt->is_trained) {
        vt->train (n, x);
    }

    if (threshold_type == Thresh_global) {
        // nothing to do
        return;
    } else if (threshold_type == Thresh_centroid ||
        threshold_type == Thresh_centroid_half) {
        // convert all centroids with vt
        std::vector<float> centroids (nlist * d);
        quantizer->reconstruct_n (0, nlist, centroids.data());
        trained.resize(nlist * nbit);
        vt->apply_noalloc (nlist, centroids.data(), trained.data());
        if (threshold_type == Thresh_centroid_half) {
            for (size_t i = 0; i < nlist * nbit; i++) {
                trained[i] -= 0.25 * period;
            }
        }
        return;
    }
    // otherwise train medians

    // assign
    std::unique_ptr<idx_t []> idx (new idx_t [n]);
    quantizer->assign (n, x, idx.get());

    std::vector<size_t> sizes(nlist + 1);
    for (size_t i = 0; i < n; i++) {
        FAISS_THROW_IF_NOT (idx[i] >= 0);
        sizes[idx[i]]++;
    }

    size_t ofs = 0;
    for (int j = 0; j < nlist; j++) {
        size_t o0 = ofs;
        ofs += sizes[j];
        sizes[j] = o0;
    }

    // transform
    std::unique_ptr<float []> xt (vt->apply (n, x));

    // transpose + reorder
    std::unique_ptr<float []> xo (new float[n * nbit]);

    for (size_t i = 0; i < n; i++) {
        size_t idest = sizes[idx[i]]++;
        for (size_t j = 0; j < nbit; j++) {
            xo[idest + n * j] = xt[i * nbit + j];
        }
    }

    trained.resize (n * nbit);
    // compute medians
#pragma omp for
    for (int i = 0; i < nlist; i++) {
        size_t i0 = i == 0 ? 0 : sizes[i - 1];
        size_t i1 = sizes[i];
        for (int j = 0; j < nbit; j++) {
            float *xoi = xo.get() + i0 + n * j;
            if (i0 == i1) { // nothing to train
                trained[i * nbit + j] = 0.0;
            } else if (i1 == i0 + 1) {
                trained[i * nbit + j] = xoi[0];
            } else {
                trained[i * nbit + j] = median(i1 - i0, xoi);
            }
        }
    }
}


namespace {

void binarize_with_freq(size_t nbit, float freq,
                        const float *x, const float *c,
                        uint8_t *codes)
{
    memset (codes, 0, (nbit + 7) / 8);
    for (size_t i = 0; i < nbit; i++) {
        float xf = (x[i] - c[i]);
        int xi = int(floor(xf * freq));
        int bit = xi & 1;
        codes[i >> 3] |= bit << (i & 7);
    }
}


};



void IndexIVFSpectralHash::encode_vectors(idx_t n, const float* x_in,
                                          const idx_t *list_nos,
                                          uint8_t * codes,
                                          bool include_listnos) const
{
    FAISS_THROW_IF_NOT (is_trained);
    float freq = 2.0 / period;

    FAISS_THROW_IF_NOT_MSG (!include_listnos, "listnos encoding not supported");

    // transform with vt
    std::unique_ptr<float []> x (vt->apply (n, x_in));

#pragma omp parallel
    {
        std::vector<float> zero (nbit);

        // each thread takes care of a subset of lists
#pragma omp for
        for (size_t i = 0; i < n; i++) {
            int64_t list_no = list_nos [i];

            if (list_no >= 0) {
                const float *c;
                if (threshold_type == Thresh_global) {
                    c = zero.data();
                } else {
                    c = trained.data() + list_no * nbit;
                }
                binarize_with_freq (nbit, freq,
                                    x.get() + i * nbit, c,
                                    codes + i * code_size) ;
            }
        }
    }
}

namespace {


template<class HammingComputer>
struct IVFScanner: InvertedListScanner {

    // copied from index structure
    const IndexIVFSpectralHash *index;
    size_t code_size;
    size_t nbit;
    bool store_pairs;

    float period, freq;
    std::vector<float> q;
    std::vector<float> zero;
    std::vector<uint8_t> qcode;
    HammingComputer hc;

    using idx_t = Index::idx_t;

    IVFScanner (const IndexIVFSpectralHash * index,
                bool store_pairs):
        index (index),
        code_size(index->code_size),
        nbit(index->nbit),
        store_pairs(store_pairs),
        period(index->period), freq(2.0 / index->period),
        q(nbit), zero(nbit), qcode(code_size),
        hc(qcode.data(), code_size)
    {
    }


    void set_query (const float *query) override {
        FAISS_THROW_IF_NOT(query);
        FAISS_THROW_IF_NOT(q.size() == nbit);
        index->vt->apply_noalloc (1, query, q.data());

        if (index->threshold_type ==
            IndexIVFSpectralHash::Thresh_global) {
            binarize_with_freq
                (nbit, freq, q.data(), zero.data(), qcode.data());
            hc.set (qcode.data(), code_size);
        }
    }

    idx_t list_no;

    void set_list (idx_t list_no, float /*coarse_dis*/) override {
        this->list_no = list_no;
        if (index->threshold_type != IndexIVFSpectralHash::Thresh_global) {
            const float *c = index->trained.data() + list_no * nbit;
            binarize_with_freq (nbit, freq, q.data(), c, qcode.data());
            hc.set (qcode.data(), code_size);
        }
    }

    float distance_to_code (const uint8_t *code) const final {
        return hc.hamming (code);
    }

    size_t scan_codes (size_t list_size,
                       const uint8_t *codes,
                       const idx_t *ids,
                       float *simi, idx_t *idxi,
                       size_t k) const override
    {
        size_t nup = 0;
        for (size_t j = 0; j < list_size; j++) {

            float dis = hc.hamming (codes);

            if (dis < simi [0]) {
                maxheap_pop (k, simi, idxi);
                int64_t id = store_pairs ? lo_build (list_no, j) : ids[j];
                maxheap_push (k, simi, idxi, dis, id);
                nup++;
            }
            codes += code_size;
        }
        return nup;
    }

    void scan_codes_range (size_t list_size,
                           const uint8_t *codes,
                           const idx_t *ids,
                           float radius,
                           RangeQueryResult & res) const override
    {
        for (size_t j = 0; j < list_size; j++) {
            float dis = hc.hamming (codes);
            if (dis < radius) {
                int64_t id = store_pairs ? lo_build (list_no, j) : ids[j];
                res.add (dis, id);
            }
            codes += code_size;
        }
    }


};

} // anonymous namespace

InvertedListScanner* IndexIVFSpectralHash::get_InvertedListScanner
    (bool store_pairs) const
{
    switch (code_size) {
#define HANDLE_CODE_SIZE(cs) \
    case cs: \
        return new IVFScanner<HammingComputer ## cs> (this, store_pairs)
        HANDLE_CODE_SIZE(4);
        HANDLE_CODE_SIZE(8);
        HANDLE_CODE_SIZE(16);
        HANDLE_CODE_SIZE(20);
        HANDLE_CODE_SIZE(32);
        HANDLE_CODE_SIZE(64);
#undef HANDLE_CODE_SIZE
        default:
            if (code_size % 8 == 0) {
                return new IVFScanner<HammingComputerM8>(this, store_pairs);
            } else if (code_size % 4 == 0) {
                return new IVFScanner<HammingComputerM4>(this, store_pairs);
            } else {
                FAISS_THROW_MSG("not supported");
            }
    }

}



}  // namespace faiss
