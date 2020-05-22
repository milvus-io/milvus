/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/utils/utils.h>

#include <cstdio>
#include <cassert>
#include <cstring>
#include <cmath>

#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <omp.h>

#include <algorithm>
#include <vector>

#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/random.h>



#ifndef FINTEGER
#define FINTEGER long
#endif


extern "C" {

/* declare BLAS functions, see http://www.netlib.org/clapack/cblas/ */

int sgemm_ (const char *transa, const char *transb, FINTEGER *m, FINTEGER *
            n, FINTEGER *k, const float *alpha, const float *a,
            FINTEGER *lda, const float *b, FINTEGER *
            ldb, float *beta, float *c, FINTEGER *ldc);

/* Lapack functions, see http://www.netlib.org/clapack/old/single/sgeqrf.c */

int sgeqrf_ (FINTEGER *m, FINTEGER *n, float *a, FINTEGER *lda,
                 float *tau, float *work, FINTEGER *lwork, FINTEGER *info);

int sorgqr_(FINTEGER *m, FINTEGER *n, FINTEGER *k, float *a,
            FINTEGER *lda, float *tau, float *work,
            FINTEGER *lwork, FINTEGER *info);

int sgemv_(const char *trans, FINTEGER *m, FINTEGER *n, float *alpha,
           const float *a, FINTEGER *lda, const float *x, FINTEGER *incx,
           float *beta, float *y, FINTEGER *incy);

}


/**************************************************
 * Get some stats about the system
 **************************************************/

namespace faiss {

double getmillisecs () {
    struct timeval tv;
    gettimeofday (&tv, nullptr);
    return tv.tv_sec * 1e3 + tv.tv_usec * 1e-3;
}

uint64_t get_cycles () {
#ifdef  __x86_64__
    uint32_t high, low;
    asm volatile("rdtsc \n\t"
                 : "=a" (low),
                   "=d" (high));
    return ((uint64_t)high << 32) | (low);
#else
    return 0;
#endif
}


#ifdef __linux__

size_t get_mem_usage_kb ()
{
    int pid = getpid ();
    char fname[256];
    snprintf (fname, 256, "/proc/%d/status", pid);
    FILE * f = fopen (fname, "r");
    FAISS_THROW_IF_NOT_MSG (f, "cannot open proc status file");
    size_t sz = 0;
    for (;;) {
        char buf [256];
        if (!fgets (buf, 256, f)) break;
        if (sscanf (buf, "VmRSS: %ld kB", &sz) == 1) break;
    }
    fclose (f);
    return sz;
}

#elif __APPLE__

size_t get_mem_usage_kb ()
{
    fprintf(stderr, "WARN: get_mem_usage_kb not implemented on the mac\n");
    return 0;
}

#endif





void reflection (const float * __restrict u,
                 float * __restrict x,
                 size_t n, size_t d, size_t nu)
{
    size_t i, j, l;
    for (i = 0; i < n; i++) {
        const float * up = u;
        for (l = 0; l < nu; l++) {
            float ip1 = 0, ip2 = 0;

            for (j = 0; j < d; j+=2) {
                ip1 += up[j] * x[j];
                ip2 += up[j+1] * x[j+1];
            }
            float ip = 2 * (ip1 + ip2);

            for (j = 0; j < d; j++)
                x[j] -= ip * up[j];
            up += d;
        }
        x += d;
    }
}


/* Reference implementation (slower) */
void reflection_ref (const float * u, float * x, size_t n, size_t d, size_t nu)
{
    size_t i, j, l;
    for (i = 0; i < n; i++) {
        const float * up = u;
        for (l = 0; l < nu; l++) {
            double ip = 0;

            for (j = 0; j < d; j++)
                ip += up[j] * x[j];
            ip *= 2;

            for (j = 0; j < d; j++)
                x[j] -= ip * up[j];

            up += d;
        }
        x += d;
    }
}






/***************************************************************************
 * Some matrix manipulation functions
 ***************************************************************************/


/* This function exists because the Torch counterpart is extremly slow
   (not multi-threaded + unexpected overhead even in single thread).
   It is here to implement the usual property |x-y|^2=|x|^2+|y|^2-2<x|y>  */
void inner_product_to_L2sqr (float * __restrict dis,
                             const float * nr1,
                             const float * nr2,
                             size_t n1, size_t n2)
{

#pragma omp parallel for
    for (size_t j = 0 ; j < n1 ; j++) {
        float * disj = dis + j * n2;
        for (size_t i = 0 ; i < n2 ; i++)
            disj[i] = nr1[j] + nr2[i] - 2 * disj[i];
    }
}


void matrix_qr (int m, int n, float *a)
{
    FAISS_THROW_IF_NOT (m >= n);
    FINTEGER mi = m, ni = n, ki = mi < ni ? mi : ni;
    std::vector<float> tau (ki);
    FINTEGER lwork = -1, info;
    float work_size;

    sgeqrf_ (&mi, &ni, a, &mi, tau.data(),
             &work_size, &lwork, &info);
    lwork = size_t(work_size);
    std::vector<float> work (lwork);

    sgeqrf_ (&mi, &ni, a, &mi,
             tau.data(), work.data(), &lwork, &info);

    sorgqr_ (&mi, &ni, &ki, a, &mi, tau.data(),
             work.data(), &lwork, &info);

}




/***************************************************************************
 * Result list routines
 ***************************************************************************/


void ranklist_handle_ties (int k, int64_t *idx, const float *dis)
{
    float prev_dis = -1e38;
    int prev_i = -1;
    for (int i = 0; i < k; i++) {
        if (dis[i] != prev_dis) {
            if (i > prev_i + 1) {
                // sort between prev_i and i - 1
                std::sort (idx + prev_i, idx + i);
            }
            prev_i = i;
            prev_dis = dis[i];
        }
    }
}

size_t merge_result_table_with (size_t n, size_t k,
                                int64_t *I0, float *D0,
                                const int64_t *I1, const float *D1,
                                bool keep_min,
                                int64_t translation)
{
    size_t n1 = 0;

#pragma omp parallel reduction(+:n1)
    {
        std::vector<int64_t> tmpI (k);
        std::vector<float> tmpD (k);

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            int64_t *lI0 = I0 + i * k;
            float *lD0 = D0 + i * k;
            const int64_t *lI1 = I1 + i * k;
            const float *lD1 = D1 + i * k;
            size_t r0 = 0;
            size_t r1 = 0;

            if (keep_min) {
                for (size_t j = 0; j < k; j++) {

                    if (lI0[r0] >= 0 && lD0[r0] < lD1[r1]) {
                        tmpD[j] = lD0[r0];
                        tmpI[j] = lI0[r0];
                        r0++;
                    } else if (lD1[r1] >= 0) {
                        tmpD[j] = lD1[r1];
                        tmpI[j] = lI1[r1] + translation;
                        r1++;
                    } else { // both are NaNs
                        tmpD[j] = NAN;
                        tmpI[j] = -1;
                    }
                }
            } else {
                for (size_t j = 0; j < k; j++) {
                    if (lI0[r0] >= 0 && lD0[r0] > lD1[r1]) {
                        tmpD[j] = lD0[r0];
                        tmpI[j] = lI0[r0];
                        r0++;
                    } else if (lD1[r1] >= 0) {
                        tmpD[j] = lD1[r1];
                        tmpI[j] = lI1[r1] + translation;
                        r1++;
                    } else { // both are NaNs
                        tmpD[j] = NAN;
                        tmpI[j] = -1;
                    }
                }
            }
            n1 += r1;
            memcpy (lD0, tmpD.data(), sizeof (lD0[0]) * k);
            memcpy (lI0, tmpI.data(), sizeof (lI0[0]) * k);
        }
    }

    return n1;
}



size_t ranklist_intersection_size (size_t k1, const int64_t *v1,
                                   size_t k2, const int64_t *v2_in)
{
    if (k2 > k1) return ranklist_intersection_size (k2, v2_in, k1, v1);
    int64_t *v2 = new int64_t [k2];
    memcpy (v2, v2_in, sizeof (int64_t) * k2);
    std::sort (v2, v2 + k2);
    { // de-dup v2
        int64_t prev = -1;
        size_t wp = 0;
        for (size_t i = 0; i < k2; i++) {
            if (v2 [i] != prev) {
                v2[wp++] = prev = v2 [i];
            }
        }
        k2 = wp;
    }
    const int64_t seen_flag = 1L << 60;
    size_t count = 0;
    for (size_t i = 0; i < k1; i++) {
        int64_t q = v1 [i];
        size_t i0 = 0, i1 = k2;
        while (i0 + 1 < i1) {
            size_t imed = (i1 + i0) / 2;
            int64_t piv = v2 [imed] & ~seen_flag;
            if (piv <= q) i0 = imed;
            else          i1 = imed;
        }
        if (v2 [i0] == q) {
            count++;
            v2 [i0] |= seen_flag;
        }
    }
    delete [] v2;

    return count;
}

double imbalance_factor (int k, const int *hist) {
    double tot = 0, uf = 0;

    for (int i = 0 ; i < k ; i++) {
        tot += hist[i];
        uf += hist[i] * (double) hist[i];
    }
    uf = uf * k / (tot * tot);

    return uf;
}


double imbalance_factor (int n, int k, const int64_t *assign) {
    std::vector<int> hist(k, 0);
    for (int i = 0; i < n; i++) {
        hist[assign[i]]++;
    }

    return imbalance_factor (k, hist.data());
}



int ivec_hist (size_t n, const int * v, int vmax, int *hist) {
    memset (hist, 0, sizeof(hist[0]) * vmax);
    int nout = 0;
    while (n--) {
        if (v[n] < 0 || v[n] >= vmax) nout++;
        else hist[v[n]]++;
    }
    return nout;
}


void bincode_hist(size_t n, size_t nbits, const uint8_t *codes, int *hist)
{
    FAISS_THROW_IF_NOT (nbits % 8 == 0);
    size_t d = nbits / 8;
    std::vector<int> accu(d * 256);
    const uint8_t *c = codes;
    for (size_t i = 0; i < n; i++)
        for(int j = 0; j < d; j++)
            accu[j * 256 + *c++]++;
    memset (hist, 0, sizeof(*hist) * nbits);
    for (int i = 0; i < d; i++) {
        const int *ai = accu.data() + i * 256;
        int * hi = hist + i * 8;
        for (int j = 0; j < 256; j++)
            for (int k = 0; k < 8; k++)
                if ((j >> k) & 1)
                    hi[k] += ai[j];
    }

}



size_t ivec_checksum (size_t n, const int *a)
{
    size_t cs = 112909;
    while (n--) cs = cs * 65713 + a[n] * 1686049;
    return cs;
}


namespace {
    struct ArgsortComparator {
        const float *vals;
        bool operator() (const size_t a, const size_t b) const {
            return vals[a] < vals[b];
        }
    };

    struct SegmentS {
        size_t i0; // begin pointer in the permutation array
        size_t i1; // end
        size_t len() const {
            return i1 - i0;
        }
    };

    // see https://en.wikipedia.org/wiki/Merge_algorithm#Parallel_merge
    // extended to > 1 merge thread

    // merges 2 ranges that should be consecutive on the source into
    // the union of the two on the destination
    template<typename T>
    void parallel_merge (const T *src, T *dst,
                         SegmentS &s1, SegmentS & s2, int nt,
                         const ArgsortComparator & comp) {
        if (s2.len() > s1.len()) { // make sure that s1 larger than s2
            std::swap(s1, s2);
        }

        // compute sub-ranges for each thread
        SegmentS s1s[nt], s2s[nt], sws[nt];
        s2s[0].i0 = s2.i0;
        s2s[nt - 1].i1 = s2.i1;

        // not sure parallel actually helps here
#pragma omp parallel for num_threads(nt)
        for (int t = 0; t < nt; t++) {
            s1s[t].i0 = s1.i0 + s1.len() * t / nt;
            s1s[t].i1 = s1.i0 + s1.len() * (t + 1) / nt;

            if (t + 1 < nt) {
                T pivot = src[s1s[t].i1];
                size_t i0 = s2.i0, i1 = s2.i1;
                while (i0 + 1 < i1) {
                    size_t imed = (i1 + i0) / 2;
                    if (comp (pivot, src[imed])) {i1 = imed; }
                    else                         {i0 = imed; }
                }
                s2s[t].i1 = s2s[t + 1].i0 = i1;
            }
        }
        s1.i0 = std::min(s1.i0, s2.i0);
        s1.i1 = std::max(s1.i1, s2.i1);
        s2 = s1;
        sws[0].i0 = s1.i0;
        for (int t = 0; t < nt; t++) {
            sws[t].i1 = sws[t].i0 + s1s[t].len() + s2s[t].len();
            if (t + 1 < nt) {
                sws[t + 1].i0 = sws[t].i1;
            }
        }
        assert(sws[nt - 1].i1 == s1.i1);

        // do the actual merging
#pragma omp parallel for num_threads(nt)
        for (int t = 0; t < nt; t++) {
            SegmentS sw = sws[t];
            SegmentS s1t = s1s[t];
            SegmentS s2t = s2s[t];
            if (s1t.i0 < s1t.i1 && s2t.i0 < s2t.i1) {
                for (;;) {
                    // assert (sw.len() == s1t.len() + s2t.len());
                    if (comp(src[s1t.i0], src[s2t.i0])) {
                        dst[sw.i0++] = src[s1t.i0++];
                        if (s1t.i0 == s1t.i1) break;
                    } else {
                        dst[sw.i0++] = src[s2t.i0++];
                        if (s2t.i0 == s2t.i1) break;
                    }
                }
            }
            if (s1t.len() > 0) {
                assert(s1t.len() == sw.len());
                memcpy(dst + sw.i0, src + s1t.i0, s1t.len() * sizeof(dst[0]));
            } else if (s2t.len() > 0) {
                assert(s2t.len() == sw.len());
                memcpy(dst + sw.i0, src + s2t.i0, s2t.len() * sizeof(dst[0]));
            }
        }
    }

};

void fvec_argsort (size_t n, const float *vals,
                    size_t *perm)
{
    for (size_t i = 0; i < n; i++) perm[i] = i;
    ArgsortComparator comp = {vals};
    std::sort (perm, perm + n, comp);
}

void fvec_argsort_parallel (size_t n, const float *vals,
                            size_t *perm)
{
    size_t * perm2 = new size_t[n];
    // 2 result tables, during merging, flip between them
    size_t *permB = perm2, *permA = perm;

    int nt = omp_get_max_threads();
    { // prepare correct permutation so that the result ends in perm
      // at final iteration
        int nseg = nt;
        while (nseg > 1) {
            nseg = (nseg + 1) / 2;
            std::swap (permA, permB);
        }
    }

#pragma omp parallel
    for (size_t i = 0; i < n; i++) permA[i] = i;

    ArgsortComparator comp = {vals};

    SegmentS segs[nt];

    // independent sorts
#pragma omp parallel for
    for (int t = 0; t < nt; t++) {
        size_t i0 = t * n / nt;
        size_t i1 = (t + 1) * n / nt;
        SegmentS seg = {i0, i1};
        std::sort (permA + seg.i0, permA + seg.i1, comp);
        segs[t] = seg;
    }
    int prev_nested = omp_get_nested();
    omp_set_nested(1);

    int nseg = nt;
    while (nseg > 1) {
        int nseg1 = (nseg + 1) / 2;
        int sub_nt = nseg % 2 == 0 ? nt : nt - 1;
        int sub_nseg1 = nseg / 2;

#pragma omp parallel for num_threads(nseg1)
        for (int s = 0; s < nseg; s += 2) {
            if (s + 1 == nseg) { // otherwise isolated segment
                memcpy(permB + segs[s].i0, permA + segs[s].i0,
                       segs[s].len() * sizeof(size_t));
            } else {
                int t0 = s * sub_nt / sub_nseg1;
                int t1 = (s + 1) * sub_nt / sub_nseg1;
                printf("merge %d %d, %d threads\n", s, s + 1, t1 - t0);
                parallel_merge(permA, permB, segs[s], segs[s + 1],
                               t1 - t0, comp);
            }
        }
        for (int s = 0; s < nseg; s += 2)
            segs[s / 2] = segs[s];
        nseg = nseg1;
        std::swap (permA, permB);
    }
    assert (permA == perm);
    omp_set_nested(prev_nested);
    delete [] perm2;
}


















const float *fvecs_maybe_subsample (
          size_t d, size_t *n, size_t nmax, const float *x,
          bool verbose, int64_t seed)
{

    if (*n <= nmax) return x; // nothing to do

    size_t n2 = nmax;
    if (verbose) {
        printf ("  Input training set too big (max size is %ld), sampling "
                "%ld / %ld vectors\n", nmax, n2, *n);
    }
    std::vector<int> subset (*n);
    rand_perm (subset.data (), *n, seed);
    float *x_subset = new float[n2 * d];
    for (int64_t i = 0; i < n2; i++)
        memcpy (&x_subset[i * d],
                &x[subset[i] * size_t(d)],
                sizeof (x[0]) * d);
    *n = n2;
    return x_subset;
}


void binary_to_real(size_t d, const uint8_t *x_in, float *x_out) {
    for (size_t i = 0; i < d; ++i) {
        x_out[i] = 2 * ((x_in[i >> 3] >> (i & 7)) & 1) - 1;
    }
}

void real_to_binary(size_t d, const float *x_in, uint8_t *x_out) {
  for (size_t i = 0; i < d / 8; ++i) {
    uint8_t b = 0;
    for (int j = 0; j < 8; ++j) {
      if (x_in[8 * i + j] > 0) {
        b |= (1 << j);
      }
    }
    x_out[i] = b;
  }
}


// from Python's stringobject.c
uint64_t hash_bytes (const uint8_t *bytes, int64_t n) {
    const uint8_t *p = bytes;
    uint64_t x = (uint64_t)(*p) << 7;
    int64_t len = n;
    while (--len >= 0) {
        x = (1000003*x) ^ *p++;
    }
    x ^= n;
    return x;
}


bool check_openmp() {
    omp_set_num_threads(10);

    if (omp_get_max_threads() != 10) {
        return false;
    }

    std::vector<int> nt_per_thread(10);
    size_t sum = 0;
    bool in_parallel = true;
#pragma omp parallel reduction(+: sum)
    {
        if (!omp_in_parallel()) {
            in_parallel = false;
        }

        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();

        nt_per_thread[rank] = nt;
#pragma omp for
        for(int i = 0; i < 1000 * 1000 * 10; i++) {
            sum += i;
        }
    }

    if (!in_parallel) {
        return false;
    }
    if (nt_per_thread[0] != 10) {
        return false;
    }
    if (sum == 0) {
        return false;
    }

    return true;
}

} // namespace faiss
