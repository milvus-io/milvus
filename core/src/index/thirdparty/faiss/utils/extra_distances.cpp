/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/utils/distances.h>

#include <cmath>
#include <omp.h>


#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>

namespace faiss {

/***************************************************************************
 * Distance functions (other than L2 and IP)
 ***************************************************************************/

struct VectorDistanceL2 {
    size_t d;

    float operator () (const float *x, const float *y) const {
        return fvec_L2sqr (x, y, d);
    }
};

struct VectorDistanceL1 {
    size_t d;

    float operator () (const float *x, const float *y) const {
        return fvec_L1 (x, y, d);
    }
};

struct VectorDistanceLinf {
    size_t d;

    float operator () (const float *x, const float *y) const {
        return fvec_Linf (x, y, d);
        /*
        float vmax = 0;
        for (size_t i = 0; i < d; i++) {
            float diff = fabs (x[i] - y[i]);
            if (diff > vmax) vmax = diff;
        }
        return vmax;*/
    }
};

struct VectorDistanceLp {
    size_t d;
    const float p;

    float operator () (const float *x, const float *y) const {
        float accu = 0;
        for (size_t i = 0; i < d; i++) {
            float diff = fabs (x[i] - y[i]);
            accu += powf (diff, p);
        }
        return accu;
    }
};

struct VectorDistanceCanberra {
    size_t d;

    float operator () (const float *x, const float *y) const {
        float accu = 0;
        for (size_t i = 0; i < d; i++) {
            float xi = x[i], yi = y[i];
            accu += fabs (xi - yi) / (fabs(xi) + fabs(yi));
        }
        return accu;
    }
};

struct VectorDistanceBrayCurtis {
    size_t d;

    float operator () (const float *x, const float *y) const {
        float accu_num = 0, accu_den = 0;
        for (size_t i = 0; i < d; i++) {
            float xi = x[i], yi = y[i];
            accu_num += fabs (xi - yi);
            accu_den += fabs (xi + yi);
        }
        return accu_num / accu_den;
    }
};

struct VectorDistanceJensenShannon {
    size_t d;

    float operator () (const float *x, const float *y) const {
        float accu = 0;

        for (size_t i = 0; i < d; i++) {
            float xi = x[i], yi = y[i];
            float mi = 0.5 * (xi + yi);
            float kl1 = - xi * log(mi / xi);
            float kl2 = - yi * log(mi / yi);
            accu += kl1 + kl2;
        }
        return 0.5 * accu;
    }
};










namespace {

template<class VD>
void pairwise_extra_distances_template (
                     VD vd,
                     int64_t nq, const float *xq,
                     int64_t nb, const float *xb,
                     float *dis,
                     int64_t ldq, int64_t ldb, int64_t ldd)
{

#pragma omp parallel for if(nq > 10)
    for (int64_t i = 0; i < nq; i++) {
        const float *xqi = xq + i * ldq;
        const float *xbj = xb;
        float *disi = dis + ldd * i;

        for (int64_t j = 0; j < nb; j++) {
            disi[j] = vd (xqi, xbj);
            xbj += ldb;
        }
    }
}


template<class VD>
void knn_extra_metrics_template (
        VD vd,
        const float * x,
        const float * y,
        size_t nx, size_t ny,
        float_maxheap_array_t * res)
{
    size_t k = res->k;
    size_t d = vd.d;
    size_t check_period = InterruptCallback::get_period_hint (ny * d);
    check_period *= omp_get_max_threads();

    for (size_t i0 = 0; i0 < nx; i0 += check_period) {
        size_t i1 = std::min(i0 + check_period, nx);

#pragma omp parallel for
        for (size_t i = i0; i < i1; i++) {
            const float * x_i = x + i * d;
            const float * y_j = y;
            size_t j;
            float * simi = res->get_val(i);
            int64_t * idxi = res->get_ids (i);

            maxheap_heapify (k, simi, idxi);
            for (j = 0; j < ny; j++) {
                float disij = vd (x_i, y_j);

                if (disij < simi[0]) {
                    maxheap_pop (k, simi, idxi);
                    maxheap_push (k, simi, idxi, disij, j);
                }
                y_j += d;
            }
            maxheap_reorder (k, simi, idxi);
        }
        InterruptCallback::check ();
    }

}


template<class VD>
struct ExtraDistanceComputer : DistanceComputer {
    VD vd;
    Index::idx_t nb;
    const float *q;
    const float *b;

    float operator () (idx_t i) override {
        return vd (q, b + i * vd.d);
    }

    float symmetric_dis(idx_t i, idx_t j) override {
        return vd (b + j * vd.d, b + i * vd.d);
    }

    ExtraDistanceComputer(const VD & vd, const float *xb,
                          size_t nb, const float *q = nullptr)
        : vd(vd), nb(nb), q(q), b(xb) {}

    void set_query(const float *x) override {
        q = x;
    }
};
















} // anonymous namespace

void pairwise_extra_distances (
                     int64_t d,
                     int64_t nq, const float *xq,
                     int64_t nb, const float *xb,
                     MetricType mt, float metric_arg,
                     float *dis,
                     int64_t ldq, int64_t ldb, int64_t ldd)
{
    if (nq == 0 || nb == 0) return;
    if (ldq == -1) ldq = d;
    if (ldb == -1) ldb = d;
    if (ldd == -1) ldd = nb;

    switch(mt) {
#define HANDLE_VAR(kw)                                          \
     case METRIC_ ## kw: {                                      \
        VectorDistance ## kw vd({(size_t)d});                   \
        pairwise_extra_distances_template (vd, nq, xq, nb, xb,  \
                                           dis, ldq, ldb, ldd); \
        break;                                                  \
    }
        HANDLE_VAR(L2);
        HANDLE_VAR(L1);
        HANDLE_VAR(Linf);
        HANDLE_VAR(Canberra);
        HANDLE_VAR(BrayCurtis);
        HANDLE_VAR(JensenShannon);
#undef HANDLE_VAR
    case METRIC_Lp: {
        VectorDistanceLp vd({(size_t)d, metric_arg});
        pairwise_extra_distances_template (vd, nq, xq, nb, xb,
                                           dis, ldq, ldb, ldd);
        break;
    }
    default:
        FAISS_THROW_MSG ("metric type not implemented");
    }

}

void knn_extra_metrics (
        const float * x,
        const float * y,
        size_t d, size_t nx, size_t ny,
        MetricType mt, float metric_arg,
        float_maxheap_array_t * res)
{

    switch(mt) {
#define HANDLE_VAR(kw)                                          \
     case METRIC_ ## kw: {                                      \
        VectorDistance ## kw vd({(size_t)d});                   \
        knn_extra_metrics_template (vd, x, y, nx, ny, res);     \
        break;                                                  \
    }
        HANDLE_VAR(L2);
        HANDLE_VAR(L1);
        HANDLE_VAR(Linf);
        HANDLE_VAR(Canberra);
        HANDLE_VAR(BrayCurtis);
        HANDLE_VAR(JensenShannon);
#undef HANDLE_VAR
    case METRIC_Lp: {
        VectorDistanceLp vd({(size_t)d, metric_arg});
        knn_extra_metrics_template (vd, x, y, nx, ny, res);
        break;
    }
    default:
        FAISS_THROW_MSG ("metric type not implemented");
    }

}

DistanceComputer *get_extra_distance_computer (
        size_t d,
        MetricType mt, float metric_arg,
        size_t nb, const float *xb)
{

    switch(mt) {
#define HANDLE_VAR(kw)                                                  \
     case METRIC_ ## kw: {                                              \
        VectorDistance ## kw vd({(size_t)d});                           \
        return new ExtraDistanceComputer<VectorDistance ## kw>(vd, xb, nb); \
    }
        HANDLE_VAR(L2);
        HANDLE_VAR(L1);
        HANDLE_VAR(Linf);
        HANDLE_VAR(Canberra);
        HANDLE_VAR(BrayCurtis);
        HANDLE_VAR(JensenShannon);
#undef HANDLE_VAR
    case METRIC_Lp: {
        VectorDistanceLp vd({(size_t)d, metric_arg});
        return new ExtraDistanceComputer<VectorDistanceLp> (vd, xb, nb);
        break;
    }
    default:
        FAISS_THROW_MSG ("metric type not implemented");
    }

}


} // namespace faiss
