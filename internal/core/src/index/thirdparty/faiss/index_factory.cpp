/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/*
 * implementation of Hyper-parameter auto-tuning
 */

#include <faiss/AutoTune.h>

#include <cmath>

#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/random.h>

#include <faiss/IndexFlat.h>
#include <faiss/VectorTransform.h>
#include <faiss/IndexPreTransform.h>
#include <faiss/IndexLSH.h>
#include <faiss/IndexPQ.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexIVFPQR.h>
#include <faiss/Index2Layer.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/impl/ScalarQuantizerOp.h>
#include <faiss/IndexScalarQuantizer.h>
#include <faiss/IndexHNSW.h>
#include <faiss/IndexLattice.h>

#include <faiss/IndexBinaryFlat.h>
#include <faiss/IndexBinaryHNSW.h>
#include <faiss/IndexBinaryIVF.h>
#include <faiss/IndexSQHybrid.h>

namespace faiss {


/***************************************************************
 * index_factory
 ***************************************************************/

namespace {

struct VTChain {
    std::vector<VectorTransform *> chain;
    ~VTChain () {
        for (int i = 0; i < chain.size(); i++) {
            delete chain[i];
        }
    }
};


/// what kind of training does this coarse quantizer require?
char get_trains_alone(const Index *coarse_quantizer) {
    return
        dynamic_cast<const MultiIndexQuantizer*>(coarse_quantizer) ? 1 :
        dynamic_cast<const IndexHNSWFlat*>(coarse_quantizer) ? 2 :
        0;
}


}

Index *index_factory (int d, const char *description_in, MetricType metric)
{
    FAISS_THROW_IF_NOT(metric == METRIC_L2 ||
                       metric == METRIC_INNER_PRODUCT);
    VTChain vts;
    Index *coarse_quantizer = nullptr;
    Index *index = nullptr;
    bool add_idmap = false;
    bool make_IndexRefineFlat = false;

    ScopeDeleter1<Index> del_coarse_quantizer, del_index;

    char description[strlen(description_in) + 1];
    char *ptr;
    memcpy (description, description_in, strlen(description_in) + 1);

    int64_t ncentroids = -1;
    bool use_2layer = false;

    for (char *tok = strtok_r (description, " ,", &ptr);
         tok;
         tok = strtok_r (nullptr, " ,", &ptr)) {
        int d_out, opq_M, nbit, M, M2, pq_m, ncent, r2;
        std::string stok(tok);
        nbit = 8;

        // to avoid mem leaks with exceptions:
        // do all tests before any instanciation

        VectorTransform *vt_1 = nullptr;
        Index *coarse_quantizer_1 = nullptr;
        Index *index_1 = nullptr;

        // VectorTransforms
        if (sscanf (tok, "PCA%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out);
            d = d_out;
        } else if (sscanf (tok, "PCAR%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out, 0, true);
            d = d_out;
        } else if (sscanf (tok, "RR%d", &d_out) == 1) {
            vt_1 = new RandomRotationMatrix (d, d_out);
            d = d_out;
        } else if (sscanf (tok, "PCAW%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out, -0.5, false);
            d = d_out;
        } else if (sscanf (tok, "PCAWR%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out, -0.5, true);
            d = d_out;
        } else if (sscanf (tok, "OPQ%d_%d", &opq_M, &d_out) == 2) {
            vt_1 = new OPQMatrix (d, opq_M, d_out);
            d = d_out;
        } else if (sscanf (tok, "OPQ%d", &opq_M) == 1) {
            vt_1 = new OPQMatrix (d, opq_M);
        } else if (sscanf (tok, "ITQ%d", &d_out) == 1) {
            vt_1 = new ITQTransform (d, d_out, true);
            d = d_out;
        } else if (stok == "ITQ") {
            vt_1 = new ITQTransform (d, d, false);
        } else if (sscanf (tok, "Pad%d", &d_out) == 1) {
            if (d_out > d) {
                vt_1 = new RemapDimensionsTransform (d, d_out, false);
                d = d_out;
            }
        } else if (stok == "L2norm") {
            vt_1 = new NormalizationTransform (d, 2.0);

        // coarse quantizers
        } else if (!coarse_quantizer &&
                   sscanf (tok, "IVF%ld_HNSW%d", &ncentroids, &M) == 2) {
            FAISS_THROW_IF_NOT (metric == METRIC_L2);
            coarse_quantizer_1 = new IndexHNSWFlat (d, M);

        } else if (!coarse_quantizer &&
                   sscanf (tok, "IVF%ld", &ncentroids) == 1) {
            if (metric == METRIC_L2) {
                coarse_quantizer_1 = new IndexFlatL2 (d);
            } else {
                coarse_quantizer_1 = new IndexFlatIP (d);
            }
        } else if (!coarse_quantizer && sscanf (tok, "IMI2x%d", &nbit) == 1) {
            FAISS_THROW_IF_NOT_MSG (metric == METRIC_L2,
                             "MultiIndex not implemented for inner prod search");
            coarse_quantizer_1 = new MultiIndexQuantizer (d, 2, nbit);
            ncentroids = 1 << (2 * nbit);

        } else if (!coarse_quantizer &&
                   sscanf (tok, "Residual%dx%d", &M, &nbit) == 2) {
            FAISS_THROW_IF_NOT_MSG (metric == METRIC_L2,
                       "MultiIndex not implemented for inner prod search");
            coarse_quantizer_1 = new MultiIndexQuantizer (d, M, nbit);
            ncentroids = int64_t(1) << (M * nbit);
            use_2layer = true;

        } else if (!coarse_quantizer &&
                   sscanf (tok, "Residual%ld", &ncentroids) == 1) {
            coarse_quantizer_1 = new IndexFlatL2 (d);
            use_2layer = true;

        } else if (stok == "IDMap") {
            add_idmap = true;

        // IVFs
        } else if (!index && (stok == "Flat" || stok == "FlatDedup")) {
            if (coarse_quantizer) {
                // if there was an IVF in front, then it is an IVFFlat
                IndexIVF *index_ivf = stok == "Flat" ?
                    new IndexIVFFlat (
                          coarse_quantizer, d, ncentroids, metric) :
                    new IndexIVFFlatDedup (
                          coarse_quantizer, d, ncentroids, metric);
                index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
                index_ivf->cp.spherical = metric == METRIC_INNER_PRODUCT;
                del_coarse_quantizer.release ();
                index_ivf->own_fields = true;
                index_1 = index_ivf;
            } else {
                FAISS_THROW_IF_NOT_MSG (stok != "FlatDedup",
                                        "dedup supported only for IVFFlat");
                index_1 = new IndexFlat (d, metric);
            }
        } else if (!index && (stok == "SQ8" || stok == "SQ4" || stok == "SQ6" ||
                              stok == "SQfp16")) {
            QuantizerType qt =
                stok == "SQ8" ? QuantizerType::QT_8bit :
                stok == "SQ6" ? QuantizerType::QT_6bit :
                stok == "SQ4" ? QuantizerType::QT_4bit :
                stok == "SQfp16" ? QuantizerType::QT_fp16 :
                QuantizerType::QT_4bit;
            if (coarse_quantizer) {
                FAISS_THROW_IF_NOT (!use_2layer);
                IndexIVFScalarQuantizer *index_ivf =
                    new IndexIVFScalarQuantizer (
                      coarse_quantizer, d, ncentroids, qt, metric);
                index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
                del_coarse_quantizer.release ();
                index_ivf->own_fields = true;
                index_1 = index_ivf;
            } else {
                index_1 = new IndexScalarQuantizer (d, qt, metric);
            }
        } else if (!index && (stok == "SQ8Hybrid" || stok == "SQ4Hybrid" || stok == "SQ6Hybrid" ||
                              stok == "SQfp16Hybrid")) {
            QuantizerType qt =
                    stok == "SQ8Hybrid" ? QuantizerType::QT_8bit :
                    stok == "SQ6Hybrid" ? QuantizerType::QT_6bit :
                    stok == "SQ4Hybrid" ? QuantizerType::QT_4bit :
                    stok == "SQfp16Hybrid" ? QuantizerType::QT_fp16 :
                    QuantizerType::QT_4bit;
            FAISS_THROW_IF_NOT_MSG(coarse_quantizer,
                                   "SQ Hybrid only with an IVF");
            FAISS_THROW_IF_NOT (!use_2layer);
            IndexIVFSQHybrid *index_ivf =
                    new IndexIVFSQHybrid (
                            coarse_quantizer, d, ncentroids, qt, metric);
            index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
            del_coarse_quantizer.release ();
            index_ivf->own_fields = true;
            index_1 = index_ivf;
        } else if (!index && sscanf (tok, "PQ%d+%d", &M, &M2) == 2) {
            FAISS_THROW_IF_NOT_MSG(coarse_quantizer,
                             "PQ with + works only with an IVF");
            FAISS_THROW_IF_NOT_MSG(metric == METRIC_L2,
                             "IVFPQR not implemented for inner product search");
            IndexIVFPQR *index_ivf = new IndexIVFPQR (
                  coarse_quantizer, d, ncentroids, M, 8, M2, 8);
            index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
            del_coarse_quantizer.release ();
            index_ivf->own_fields = true;
            index_1 = index_ivf;
        } else if (!index && (sscanf (tok, "PQ%dx%d", &M, &nbit) == 2 ||
                              sscanf (tok, "PQ%d", &M) == 1 ||
                              sscanf (tok, "PQ%dnp", &M) == 1)) {
            bool do_polysemous_training = stok.find("np") == std::string::npos;
            if (coarse_quantizer) {
                if (!use_2layer) {
                    IndexIVFPQ *index_ivf = new IndexIVFPQ (
                        coarse_quantizer, d, ncentroids, M, nbit);
                    index_ivf->quantizer_trains_alone =
                        get_trains_alone (coarse_quantizer);
                    index_ivf->metric_type = metric;
                    index_ivf->cp.spherical = metric == METRIC_INNER_PRODUCT;
                    del_coarse_quantizer.release ();
                    index_ivf->own_fields = true;
                    index_ivf->do_polysemous_training = do_polysemous_training;
                    index_1 = index_ivf;
                } else {
                    Index2Layer *index_2l = new Index2Layer
                        (coarse_quantizer, ncentroids, M, nbit);
                    index_2l->q1.quantizer_trains_alone =
                        get_trains_alone (coarse_quantizer);
                    index_2l->q1.own_fields = true;
                    index_1 = index_2l;
                }
            } else {
                IndexPQ *index_pq = new IndexPQ (d, M, nbit, metric);
                index_pq->do_polysemous_training = do_polysemous_training;
                index_1 = index_pq;
            }
        } else if (!index &&
                   sscanf (tok, "HNSW%d_%d+PQ%d", &M, &ncent, &pq_m) == 3) {
            Index * quant = new IndexFlatL2 (d);
            IndexHNSW2Level * hidx2l = new IndexHNSW2Level (quant, ncent, pq_m, M);
            Index2Layer * idx2l = dynamic_cast<Index2Layer*>(hidx2l->storage);
            idx2l->q1.own_fields = true;
            index_1 = hidx2l;
        } else if (!index &&
                   sscanf (tok, "HNSW%d_2x%d+PQ%d", &M, &nbit, &pq_m) == 3) {
            Index * quant = new MultiIndexQuantizer (d, 2, nbit);
            IndexHNSW2Level * hidx2l =
                new IndexHNSW2Level (quant, 1 << (2 * nbit), pq_m, M);
            Index2Layer * idx2l = dynamic_cast<Index2Layer*>(hidx2l->storage);
            idx2l->q1.own_fields = true;
            idx2l->q1.quantizer_trains_alone = 1;
            index_1 = hidx2l;
        } else if (!index &&
                   sscanf (tok, "HNSW%d_PQ%d", &M, &pq_m) == 2) {
            index_1 = new IndexHNSWPQ (d, pq_m, M);
        } else if (!index &&
                   sscanf (tok, "HNSW%d", &M) == 1) {
            index_1 = new IndexHNSWFlat (d, M);
        } else if (!index &&
                   sscanf (tok, "HNSW%d_SQ%d", &M, &pq_m) == 2 &&
                   pq_m == 8) {
            index_1 = new IndexHNSWSQ (d, QuantizerType::QT_8bit, M);
        } else if (!index && (stok == "LSH" || stok == "LSHr" ||
                              stok == "LSHrt" || stok == "LSHt")) {
            bool rotate_data = strstr(tok, "r") != nullptr;
            bool train_thresholds = strstr(tok, "t") != nullptr;
            index_1 = new IndexLSH (d, d, rotate_data, train_thresholds);
        } else if (!index &&
                   sscanf (tok, "ZnLattice%dx%d_%d", &M, &r2, &nbit) == 3) {
            FAISS_THROW_IF_NOT(!coarse_quantizer);
            index_1 = new IndexLattice(d, M, nbit, r2);
        } else if (stok == "RFlat") {
            make_IndexRefineFlat = true;
        } else {
            FAISS_THROW_FMT( "could not parse token \"%s\" in %s\n",
                             tok, description_in);
        }

        if (index_1 && add_idmap) {
            IndexIDMap *idmap = new IndexIDMap(index_1);
            del_index.set (idmap);
            idmap->own_fields = true;
            index_1 = idmap;
            add_idmap = false;
        }

        if (vt_1)  {
            vts.chain.push_back (vt_1);
        }

        if (coarse_quantizer_1) {
            coarse_quantizer = coarse_quantizer_1;
            del_coarse_quantizer.set (coarse_quantizer);
        }

        if (index_1) {
            index = index_1;
            del_index.set (index);
        }
    }

    FAISS_THROW_IF_NOT_FMT(index, "description %s did not generate an index",
                    description_in);

    // nothing can go wrong now
    del_index.release ();
    del_coarse_quantizer.release ();

    if (add_idmap) {
        fprintf(stderr, "index_factory: WARNING: "
                "IDMap option not used\n");
    }

    if (vts.chain.size() > 0) {
        IndexPreTransform *index_pt = new IndexPreTransform (index);
        index_pt->own_fields = true;
        // add from back
        while (vts.chain.size() > 0) {
            index_pt->prepend_transform (vts.chain.back ());
            vts.chain.pop_back ();
        }
        index = index_pt;
    }

    if (make_IndexRefineFlat) {
        IndexRefineFlat *index_rf = new IndexRefineFlat (index);
        index_rf->own_fields = true;
        index = index_rf;
    }

    return index;
}

IndexBinary *index_binary_factory(int d, const char *description, MetricType metric = METRIC_L2)
{
    IndexBinary *index = nullptr;

    int ncentroids = -1;
    int M;

    ScopeDeleter1<IndexBinary> del_index;
    if (sscanf(description, "BIVF%d_HNSW%d", &ncentroids, &M) == 2) {
        IndexBinaryIVF *index_ivf = new IndexBinaryIVF(
                new IndexBinaryHNSW(d, M), d, ncentroids
        );
        index_ivf->own_fields = true;
        index = index_ivf;

    } else if (sscanf(description, "BIVF%d", &ncentroids) == 1) {
        IndexBinaryIVF *index_ivf = new IndexBinaryIVF(
                new IndexBinaryFlat(d), d, ncentroids
        );
        index_ivf->own_fields = true;
        index = index_ivf;

    } else if (sscanf(description, "BHNSW%d", &M) == 1) {
        IndexBinaryHNSW *index_hnsw = new IndexBinaryHNSW(d, M);
        index = index_hnsw;

    } else if (std::string(description) == "BFlat") {
        IndexBinary* index_x = new IndexBinaryFlat(d, metric);

        {
            IndexBinaryIDMap *idmap = new IndexBinaryIDMap(index_x);
            del_index.set (idmap);
            idmap->own_fields = true;
            index_x = idmap;
        }

        if (index_x) {
            index = index_x;
            del_index.set(index);
        }

    } else {
        FAISS_THROW_IF_NOT_FMT(index, "description %s did not generate an index",
                                   description);
    }

    del_index.release();

    return index;
}



} // namespace faiss
