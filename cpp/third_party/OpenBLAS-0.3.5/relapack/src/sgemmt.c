#include "relapack.h"

static void RELAPACK_sgemmt_rec(const char *, const char *, const char *,
    const int *, const int *, const float *, const float *, const int *,
    const float *, const int *, const float *, float *, const int *);

static void RELAPACK_sgemmt_rec2(const char *, const char *, const char *,
    const int *, const int *, const float *, const float *, const int *,
    const float *, const int *, const float *, float *, const int *);


/** SGEMMT computes a matrix-matrix product with general matrices but updates
 * only the upper or lower triangular part of the result matrix.
 *
 * This routine performs the same operation as the BLAS routine
 * sgemm(transA, transB, n, n, k, alpha, A, ldA, B, ldB, beta, C, ldC)
 * but only updates the triangular part of C specified by uplo:
 * If (*uplo == 'L'), only the lower triangular part of C is updated,
 * otherwise the upper triangular part is updated.
 * */
void RELAPACK_sgemmt(
    const char *uplo, const char *transA, const char *transB,
    const int *n, const int *k,
    const float *alpha, const float *A, const int *ldA,
    const float *B, const int *ldB,
    const float *beta, float *C, const int *ldC
) {

#if HAVE_XGEMMT
    BLAS(sgemmt)(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
    return;
#else

    // Check arguments
    const int lower = LAPACK(lsame)(uplo, "L");
    const int upper = LAPACK(lsame)(uplo, "U");
    const int notransA = LAPACK(lsame)(transA, "N");
    const int tranA = LAPACK(lsame)(transA, "T");
    const int notransB = LAPACK(lsame)(transB, "N");
    const int tranB = LAPACK(lsame)(transB, "T");
    int info = 0;
    if (!lower && !upper)
        info = 1;
    else if (!tranA && !notransA)
        info = 2;
    else if (!tranB && !notransB)
        info = 3;
    else if (*n < 0)
        info = 4;
    else if (*k < 0)
        info = 5;
    else if (*ldA < MAX(1, notransA ? *n : *k))
        info = 8;
    else if (*ldB < MAX(1, notransB ? *k : *n))
        info = 10;
    else if (*ldC < MAX(1, *n))
        info = 13;
    if (info) {
        LAPACK(xerbla)("SGEMMT", &info);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';
    const char cleantransA = notransA ? 'N' : 'T';
    const char cleantransB = notransB ? 'N' : 'T';

    // Recursive kernel
    RELAPACK_sgemmt_rec(&cleanuplo, &cleantransA, &cleantransB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
#endif
}


/** sgemmt's recursive compute kernel */
static void RELAPACK_sgemmt_rec(
    const char *uplo, const char *transA, const char *transB,
    const int *n, const int *k,
    const float *alpha, const float *A, const int *ldA,
    const float *B, const int *ldB,
    const float *beta, float *C, const int *ldC
) {

    if (*n <= MAX(CROSSOVER_SGEMMT, 1)) {
        // Unblocked
        RELAPACK_sgemmt_rec2(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
        return;
    }

    // Splitting
    const int n1 = SREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_T
    // A_B
    const float *const A_T = A;
    const float *const A_B = A + ((*transA == 'N') ? n1 : *ldA * n1);

    // B_L B_R
    const float *const B_L = B;
    const float *const B_R = B + ((*transB == 'N') ? *ldB * n1 : n1);

    // C_TL C_TR
    // C_BL C_BR
    float *const C_TL = C;
    float *const C_TR = C + *ldC * n1;
    float *const C_BL = C             + n1;
    float *const C_BR = C + *ldC * n1 + n1;

    // recursion(C_TL)
    RELAPACK_sgemmt_rec(uplo, transA, transB, &n1, k, alpha, A_T, ldA, B_L, ldB, beta, C_TL, ldC);

    if (*uplo == 'L')
        // C_BL = alpha A_B B_L + beta C_BL
        BLAS(sgemm)(transA, transB, &n2, &n1, k, alpha, A_B, ldA, B_L, ldB, beta, C_BL, ldC);
    else
        // C_TR = alpha A_T B_R + beta C_TR
        BLAS(sgemm)(transA, transB, &n1, &n2, k, alpha, A_T, ldA, B_R, ldB, beta, C_TR, ldC);

    // recursion(C_BR)
    RELAPACK_sgemmt_rec(uplo, transA, transB, &n2, k, alpha, A_B, ldA, B_R, ldB, beta, C_BR, ldC);
}


/** sgemmt's unblocked compute kernel */
static void RELAPACK_sgemmt_rec2(
    const char *uplo, const char *transA, const char *transB,
    const int *n, const int *k,
    const float *alpha, const float *A, const int *ldA,
    const float *B, const int *ldB,
    const float *beta, float *C, const int *ldC
) {

    const int incB = (*transB == 'N') ? 1 : *ldB;
    const int incC = 1;

    int i;
    for (i = 0; i < *n; i++) {
        // A_0
        // A_i
        const float *const A_0 = A;
        const float *const A_i = A + ((*transA == 'N') ? i : *ldA * i);

        // * B_i *
        const float *const B_i = B + ((*transB == 'N') ? *ldB * i : i);

        // * C_0i *
        // * C_ii *
        float *const C_0i = C + *ldC * i;
        float *const C_ii = C + *ldC * i + i;

        if (*uplo == 'L') {
            const int nmi = *n - i;
            if (*transA == 'N')
                BLAS(sgemv)(transA, &nmi, k, alpha, A_i, ldA, B_i, &incB, beta, C_ii, &incC);
            else
                BLAS(sgemv)(transA, k, &nmi, alpha, A_i, ldA, B_i, &incB, beta, C_ii, &incC);
        } else {
            const int ip1 = i + 1;
            if (*transA == 'N')
                BLAS(sgemv)(transA, &ip1, k, alpha, A_0, ldA, B_i, &incB, beta, C_0i, &incC);
            else
                BLAS(sgemv)(transA, k, &ip1, alpha, A_0, ldA, B_i, &incB, beta, C_0i, &incC);
        }
    }
}
