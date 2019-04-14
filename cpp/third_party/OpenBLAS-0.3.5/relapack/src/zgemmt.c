#include "relapack.h"

static void RELAPACK_zgemmt_rec(const char *, const char *, const char *,
    const int *, const int *, const double *, const double *, const int *,
    const double *, const int *, const double *, double *, const int *);

static void RELAPACK_zgemmt_rec2(const char *, const char *, const char *,
    const int *, const int *, const double *, const double *, const int *,
    const double *, const int *, const double *, double *, const int *);


/** ZGEMMT computes a matrix-matrix product with general matrices but updates
 * only the upper or lower triangular part of the result matrix.
 *
 * This routine performs the same operation as the BLAS routine
 * zgemm(transA, transB, n, n, k, alpha, A, ldA, B, ldB, beta, C, ldC)
 * but only updates the triangular part of C specified by uplo:
 * If (*uplo == 'L'), only the lower triangular part of C is updated,
 * otherwise the upper triangular part is updated.
 * */
void RELAPACK_zgemmt(
    const char *uplo, const char *transA, const char *transB,
    const int *n, const int *k,
    const double *alpha, const double *A, const int *ldA,
    const double *B, const int *ldB,
    const double *beta, double *C, const int *ldC
) {

#if HAVE_XGEMMT
    BLAS(zgemmt)(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
    return;
#else

    // Check arguments
    const int lower = LAPACK(lsame)(uplo, "L");
    const int upper = LAPACK(lsame)(uplo, "U");
    const int notransA = LAPACK(lsame)(transA, "N");
    const int tranA = LAPACK(lsame)(transA, "T");
    const int ctransA = LAPACK(lsame)(transA, "C");
    const int notransB = LAPACK(lsame)(transB, "N");
    const int tranB = LAPACK(lsame)(transB, "T");
    const int ctransB = LAPACK(lsame)(transB, "C");
    int info = 0;
    if (!lower && !upper)
        info = 1;
    else if (!tranA && !ctransA && !notransA)
        info = 2;
    else if (!tranB && !ctransB && !notransB)
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
        LAPACK(xerbla)("ZGEMMT", &info);
        return;
    }

    // Clean char * arguments
    const char cleanuplo = lower ? 'L' : 'U';
    const char cleantransA = notransA ? 'N' : (tranA ? 'T' : 'C');
    const char cleantransB = notransB ? 'N' : (tranB ? 'T' : 'C');

    // Recursive kernel
    RELAPACK_zgemmt_rec(&cleanuplo, &cleantransA, &cleantransB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
#endif
}


/** zgemmt's recursive compute kernel */
static void RELAPACK_zgemmt_rec(
    const char *uplo, const char *transA, const char *transB,
    const int *n, const int *k,
    const double *alpha, const double *A, const int *ldA,
    const double *B, const int *ldB,
    const double *beta, double *C, const int *ldC
) {

    if (*n <= MAX(CROSSOVER_ZGEMMT, 1)) {
        // Unblocked
        RELAPACK_zgemmt_rec2(uplo, transA, transB, n, k, alpha, A, ldA, B, ldB, beta, C, ldC);
        return;
    }

    // Splitting
    const int n1 = ZREC_SPLIT(*n);
    const int n2 = *n - n1;

    // A_T
    // A_B
    const double *const A_T = A;
    const double *const A_B = A + 2 * ((*transA == 'N') ? n1 : *ldA * n1);

    // B_L B_R
    const double *const B_L = B;
    const double *const B_R = B + 2 * ((*transB == 'N') ? *ldB * n1 : n1);

    // C_TL C_TR
    // C_BL C_BR
    double *const C_TL = C;
    double *const C_TR = C + 2 * *ldC * n1;
    double *const C_BL = C                 + 2 * n1;
    double *const C_BR = C + 2 * *ldC * n1 + 2 * n1;

    // recursion(C_TL)
    RELAPACK_zgemmt_rec(uplo, transA, transB, &n1, k, alpha, A_T, ldA, B_L, ldB, beta, C_TL, ldC);

    if (*uplo == 'L')
        // C_BL = alpha A_B B_L + beta C_BL
        BLAS(zgemm)(transA, transB, &n2, &n1, k, alpha, A_B, ldA, B_L, ldB, beta, C_BL, ldC);
    else
        // C_TR = alpha A_T B_R + beta C_TR
        BLAS(zgemm)(transA, transB, &n1, &n2, k, alpha, A_T, ldA, B_R, ldB, beta, C_TR, ldC);

    // recursion(C_BR)
    RELAPACK_zgemmt_rec(uplo, transA, transB, &n2, k, alpha, A_B, ldA, B_R, ldB, beta, C_BR, ldC);
}


/** zgemmt's unblocked compute kernel */
static void RELAPACK_zgemmt_rec2(
    const char *uplo, const char *transA, const char *transB,
    const int *n, const int *k,
    const double *alpha, const double *A, const int *ldA,
    const double *B, const int *ldB,
    const double *beta, double *C, const int *ldC
) {

    const int incB = (*transB == 'N') ? 1 : *ldB;
    const int incC = 1;

    int i;
    for (i = 0; i < *n; i++) {
        // A_0
        // A_i
        const double *const A_0 = A;
        const double *const A_i = A + 2 * ((*transA == 'N') ? i : *ldA * i);

        // * B_i *
        const double *const B_i = B + 2 * ((*transB == 'N') ? *ldB * i : i);

        // * C_0i *
        // * C_ii *
        double *const C_0i = C + 2 * *ldC * i;
        double *const C_ii = C + 2 * *ldC * i + 2 * i;

        if (*uplo == 'L') {
            const int nmi = *n - i;
            if (*transA == 'N')
                BLAS(zgemv)(transA, &nmi, k, alpha, A_i, ldA, B_i, &incB, beta, C_ii, &incC);
            else
                BLAS(zgemv)(transA, k, &nmi, alpha, A_i, ldA, B_i, &incB, beta, C_ii, &incC);
        } else {
            const int ip1 = i + 1;
            if (*transA == 'N')
                BLAS(zgemv)(transA, &ip1, k, alpha, A_0, ldA, B_i, &incB, beta, C_0i, &incC);
            else
                BLAS(zgemv)(transA, k, &ip1, alpha, A_0, ldA, B_i, &incB, beta, C_0i, &incC);
        }
    }
}
