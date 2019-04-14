/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#undef FINTEGER
#define FINTEGER long


extern "C" {

/* declare BLAS functions, see http://www.netlib.org/clapack/cblas/ */

int sgemm_ (const char *transa, const char *transb, FINTEGER *m, FINTEGER *
            n, FINTEGER *k, const float *alpha, const float *a,
            FINTEGER *lda, const float *b, FINTEGER *
            ldb, float *beta, float *c, FINTEGER *ldc);

/* Lapack functions, see http://www.netlib.org/clapack/old/single/sgeqrf.c */

int sgeqrf_ (FINTEGER *m, FINTEGER *n, float *a, FINTEGER *lda,
                 float *tau, float *work, FINTEGER *lwork, FINTEGER *info);

}

float *new_random_vec(int size)
{
    float *x = new float[size];
    for (int i = 0; i < size; i++)
        x[i] = drand48();
    return x;
}


int main() {

    FINTEGER m = 10, n = 20, k = 30;
    float *a = new_random_vec(m * k), *b = new_random_vec(n * k), *c = new float[n * m];
    float one = 1.0, zero = 0.0;

    printf("BLAS test\n");

    sgemm_("Not transposed", "Not transposed",
           &m, &n, &k, &one, a, &m, b, &k, &zero, c, &m);

    printf("errors=\n");

    for (int i = 0; i < m; i++) {
        for (int j = 0; j < n; j++) {
            float accu = 0;
            for (int l = 0; l < k; l++)
                accu += a[i + l * m] * b[l + j * k];
            printf ("%6.3f ", accu - c[i + j * m]);
        }
        printf("\n");
    }

    long info = 0x64bL << 32;
    long mi = 0x64bL << 32 | m;
    float *tau = new float[m];
    FINTEGER lwork = -1;

    float work1;

    printf("Intentional Lapack error (appears only for 64-bit INTEGER):\n");
    sgeqrf_ (&mi, &n, c, &m, tau, &work1, &lwork, (FINTEGER*)&info);

    // sgeqrf_ (&m, &n, c, &zeroi, tau, &work1, &lwork, (FINTEGER*)&info);
    printf("info=%016lx\n", info);

    if(info >> 32 == 0x64b) {
        printf("Lapack uses 32-bit integers\n");
    } else {
        printf("Lapack uses 64-bit integers\n");
    }


    return 0;
}
