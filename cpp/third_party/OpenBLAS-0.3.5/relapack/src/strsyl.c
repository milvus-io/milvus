#include "relapack.h"

static void RELAPACK_strsyl_rec(const char *, const char *, const int *,
    const int *, const int *, const float *, const int *, const float *,
    const int *, float *, const int *, float *, int *);


/** STRSYL solves the real Sylvester matrix equation.
 *
 * This routine is functionally equivalent to LAPACK's strsyl.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/d4/d7d/strsyl_8f.html
 * */
void RELAPACK_strsyl(
    const char *tranA, const char *tranB, const int *isgn,
    const int *m, const int *n,
    const float *A, const int *ldA, const float *B, const int *ldB,
    float *C, const int *ldC, float *scale,
    int *info
) {

    // Check arguments
    const int notransA = LAPACK(lsame)(tranA, "N");
    const int transA = LAPACK(lsame)(tranA, "T");
    const int ctransA = LAPACK(lsame)(tranA, "C");
    const int notransB = LAPACK(lsame)(tranB, "N");
    const int transB = LAPACK(lsame)(tranB, "T");
    const int ctransB = LAPACK(lsame)(tranB, "C");
    *info = 0;
    if (!transA && !ctransA && !notransA)
        *info = -1;
    else if (!transB && !ctransB && !notransB)
        *info = -2;
    else if (*isgn != 1 && *isgn != -1)
        *info = -3;
    else if (*m < 0)
        *info = -4;
    else if (*n < 0)
        *info = -5;
    else if (*ldA < MAX(1, *m))
        *info = -7;
    else if (*ldB < MAX(1, *n))
        *info = -9;
    else if (*ldC < MAX(1, *m))
        *info = -11;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("STRSYL", &minfo);
        return;
    }

    // Clean char * arguments
    const char cleantranA = notransA ? 'N' : (transA ? 'T' : 'C');
    const char cleantranB = notransB ? 'N' : (transB ? 'T' : 'C');

    // Recursive kernel
    RELAPACK_strsyl_rec(&cleantranA, &cleantranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
}


/** strsyl's recursive compute kernel */
static void RELAPACK_strsyl_rec(
    const char *tranA, const char *tranB, const int *isgn,
    const int *m, const int *n,
    const float *A, const int *ldA, const float *B, const int *ldB,
    float *C, const int *ldC, float *scale,
    int *info
) {

    if (*m <= MAX(CROSSOVER_STRSYL, 1) && *n <= MAX(CROSSOVER_STRSYL, 1)) {
        // Unblocked
        RELAPACK_strsyl_rec2(tranA, tranB, isgn, m, n, A, ldA, B, ldB, C, ldC, scale, info);
        return;
    }

    // Constants
    const float ONE[]  = { 1. };
    const float MONE[] = { -1. };
    const float MSGN[] = { -*isgn };
    const int   iONE[] = { 1 };

    // Outputs
    float scale1[] = { 1. };
    float scale2[] = { 1. };
    int   info1[]  = { 0 };
    int   info2[]  = { 0 };

    if (*m > *n) {
        // Splitting
        int m1 = SREC_SPLIT(*m);
        if (A[m1 + *ldA * (m1 - 1)])
            m1++;
        const int m2 = *m - m1;

        // A_TL A_TR
        // 0    A_BR
        const float *const A_TL = A;
        const float *const A_TR = A + *ldA * m1;
        const float *const A_BR = A + *ldA * m1 + m1;

        // C_T
        // C_B
        float *const C_T = C;
        float *const C_B = C + m1;

        if (*tranA == 'N') {
            // recusion(A_BR, B, C_B)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, &m2, n, A_BR, ldA, B, ldB, C_B, ldC, scale1, info1);
            // C_T = C_T - A_TR * C_B
            BLAS(sgemm)("N", "N", &m1, n, &m2, MONE, A_TR, ldA, C_B, ldC, scale1, C_T, ldC);
            // recusion(A_TL, B, C_T)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, &m1, n, A_TL, ldA, B, ldB, C_T, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(slascl)("G", iONE, iONE, ONE, scale2, &m2, n, C_B, ldC, info);
        } else {
            // recusion(A_TL, B, C_T)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, &m1, n, A_TL, ldA, B, ldB, C_T, ldC, scale1, info1);
            // C_B = C_B - A_TR' * C_T
            BLAS(sgemm)("C", "N", &m2, n, &m1, MONE, A_TR, ldA, C_T, ldC, scale1, C_B, ldC);
            // recusion(A_BR, B, C_B)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, &m2, n, A_BR, ldA, B, ldB, C_B, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(slascl)("G", iONE, iONE, ONE, scale2, &m1, n, C_B, ldC, info);
        }
    } else {
        // Splitting
        int n1 = SREC_SPLIT(*n);
        if (B[n1 + *ldB * (n1 - 1)])
            n1++;
        const int n2 = *n - n1;

        // B_TL B_TR
        // 0    B_BR
        const float *const B_TL = B;
        const float *const B_TR = B + *ldB * n1;
        const float *const B_BR = B + *ldB * n1 + n1;

        // C_L C_R
        float *const C_L = C;
        float *const C_R = C + *ldC * n1;

        if (*tranB == 'N') {
            // recusion(A, B_TL, C_L)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, m, &n1, A, ldA, B_TL, ldB, C_L, ldC, scale1, info1);
            // C_R = C_R -/+ C_L * B_TR
            BLAS(sgemm)("N", "N", m, &n2, &n1, MSGN, C_L, ldC, B_TR, ldB, scale1, C_R, ldC);
            // recusion(A, B_BR, C_R)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, m, &n2, A, ldA, B_BR, ldB, C_R, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(slascl)("G", iONE, iONE, ONE, scale2, m, &n1, C_L, ldC, info);
        } else {
            // recusion(A, B_BR, C_R)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, m, &n2, A, ldA, B_BR, ldB, C_R, ldC, scale1, info1);
            // C_L = C_L -/+ C_R * B_TR'
            BLAS(sgemm)("N", "C", m, &n1, &n2, MSGN, C_R, ldC, B_TR, ldB, scale1, C_L, ldC);
            // recusion(A, B_TL, C_L)
            RELAPACK_strsyl_rec(tranA, tranB, isgn, m, &n1, A, ldA, B_TL, ldB, C_L, ldC, scale2, info2);
            // apply scale
            if (scale2[0] != 1)
                LAPACK(slascl)("G", iONE, iONE, ONE, scale2, m, &n2, C_R, ldC, info);
        }
    }

    *scale = scale1[0] * scale2[0];
    *info  = info1[0] || info2[0];
}
