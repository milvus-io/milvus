#include "relapack.h"
#include <math.h>

static void RELAPACK_ztgsyl_rec(const char *, const int *, const int *,
    const int *, const double *, const int *, const double *, const int *,
    double *, const int *, const double *, const int *, const double *,
    const int *, double *, const int *, double *, double *, double *, int *);


/** ZTGSYL solves the generalized Sylvester equation.
 *
 * This routine is functionally equivalent to LAPACK's ztgsyl.
 * For details on its interface, see
 * http://www.netlib.org/lapack/explore-html/db/d68/ztgsyl_8f.html
 * */
void RELAPACK_ztgsyl(
    const char *trans, const int *ijob, const int *m, const int *n,
    const double *A, const int *ldA, const double *B, const int *ldB,
    double *C, const int *ldC,
    const double *D, const int *ldD, const double *E, const int *ldE,
    double *F, const int *ldF,
    double *scale, double *dif,
    double *Work, const int *lWork, int *iWork, int *info
) {

    // Parse arguments
    const int notran = LAPACK(lsame)(trans, "N");
    const int tran = LAPACK(lsame)(trans, "C");

    // Compute work buffer size
    int lwmin = 1;
    if (notran && (*ijob == 1 || *ijob == 2))
        lwmin = MAX(1, 2 * *m * *n);
    *info = 0;

    // Check arguments
    if (!tran && !notran)
        *info = -1;
    else if (notran && (*ijob < 0 || *ijob > 4))
        *info = -2;
    else if (*m <= 0)
        *info = -3;
    else if (*n <= 0)
        *info = -4;
    else if (*ldA < MAX(1, *m))
        *info = -6;
    else if (*ldB < MAX(1, *n))
        *info = -8;
    else if (*ldC < MAX(1, *m))
        *info = -10;
    else if (*ldD < MAX(1, *m))
        *info = -12;
    else if (*ldE < MAX(1, *n))
        *info = -14;
    else if (*ldF < MAX(1, *m))
        *info = -16;
    else if (*lWork < lwmin && *lWork != -1)
        *info = -20;
    if (*info) {
        const int minfo = -*info;
        LAPACK(xerbla)("ZTGSYL", &minfo);
        return;
    }

    if (*lWork == -1) {
        // Work size query
        *Work = lwmin;
        return;
    }

    // Clean char * arguments
    const char cleantrans = notran ? 'N' : 'C';

    // Constant
    const double ZERO[] = { 0., 0. };

    int isolve = 1;
    int ifunc  = 0;
    if (notran) {
        if (*ijob >= 3) {
            ifunc = *ijob - 2;
            LAPACK(zlaset)("F", m, n, ZERO, ZERO, C, ldC);
            LAPACK(zlaset)("F", m, n, ZERO, ZERO, F, ldF);
        } else if (*ijob >= 1)
            isolve = 2;
    }

    double scale2;
    int iround;
    for (iround = 1; iround <= isolve; iround++) {
        *scale = 1;
        double dscale = 0;
        double dsum   = 1;
        RELAPACK_ztgsyl_rec(&cleantrans, &ifunc, m, n, A, ldA, B, ldB, C, ldC, D, ldD, E, ldE, F, ldF, scale, &dsum, &dscale, info);
        if (dscale != 0) {
            if (*ijob == 1 || *ijob == 3)
                *dif = sqrt(2 * *m * *n) / (dscale * sqrt(dsum));
            else
                *dif = sqrt(*m * *n) / (dscale * sqrt(dsum));
        }
        if (isolve == 2) {
            if (iround == 1) {
                if (notran)
                    ifunc = *ijob;
                scale2 = *scale;
                LAPACK(zlacpy)("F", m, n, C, ldC, Work, m);
                LAPACK(zlacpy)("F", m, n, F, ldF, Work + 2 * *m * *n, m);
                LAPACK(zlaset)("F", m, n, ZERO, ZERO, C, ldC);
                LAPACK(zlaset)("F", m, n, ZERO, ZERO, F, ldF);
            } else {
                LAPACK(zlacpy)("F", m, n, Work, m, C, ldC);
                LAPACK(zlacpy)("F", m, n, Work + 2 * *m * *n, m, F, ldF);
                *scale = scale2;
            }
        }
    }
}


/** ztgsyl's recursive vompute kernel */
static void RELAPACK_ztgsyl_rec(
    const char *trans, const int *ifunc, const int *m, const int *n,
    const double *A, const int *ldA, const double *B, const int *ldB,
    double *C, const int *ldC,
    const double *D, const int *ldD, const double *E, const int *ldE,
    double *F, const int *ldF,
    double *scale, double *dsum, double *dscale,
    int *info
) {

    if (*m <= MAX(CROSSOVER_ZTGSYL, 1) && *n <= MAX(CROSSOVER_ZTGSYL, 1)) {
        // Unblocked
        LAPACK(ztgsy2)(trans, ifunc, m, n, A, ldA, B, ldB, C, ldC, D, ldD, E, ldE, F, ldF, scale, dsum, dscale, info);
        return;
    }

    // Constants
    const double ONE[]  = { 1., 0. };
    const double MONE[] = { -1., 0. };
    const int    iONE[] = { 1 };

    // Outputs
    double scale1[] = { 1., 0. };
    double scale2[] = { 1., 0. };
    int    info1[]  = { 0 };
    int    info2[]  = { 0 };

    if (*m > *n) {
        // Splitting
        const int m1 = ZREC_SPLIT(*m);
        const int m2 = *m - m1;

        // A_TL A_TR
        // 0    A_BR
        const double *const A_TL = A;
        const double *const A_TR = A + 2 * *ldA * m1;
        const double *const A_BR = A + 2 * *ldA * m1 + 2 * m1;

        // C_T
        // C_B
        double *const C_T = C;
        double *const C_B = C + 2 * m1;

        // D_TL D_TR
        // 0    D_BR
        const double *const D_TL = D;
        const double *const D_TR = D + 2 * *ldD * m1;
        const double *const D_BR = D + 2 * *ldD * m1 + 2 * m1;

        // F_T
        // F_B
        double *const F_T = F;
        double *const F_B = F + 2 * m1;

        if (*trans == 'N') {
            // recursion(A_BR, B, C_B, D_BR, E, F_B)
            RELAPACK_ztgsyl_rec(trans, ifunc, &m2, n, A_BR, ldA, B, ldB, C_B, ldC, D_BR, ldD, E, ldE, F_B, ldF, scale1, dsum, dscale, info1);
            // C_T = C_T - A_TR * C_B
            BLAS(zgemm)("N", "N", &m1, n, &m2, MONE, A_TR, ldA, C_B, ldC, scale1, C_T, ldC);
            // F_T = F_T - D_TR * C_B
            BLAS(zgemm)("N", "N", &m1, n, &m2, MONE, D_TR, ldD, C_B, ldC, scale1, F_T, ldF);
            // recursion(A_TL, B, C_T, D_TL, E, F_T)
            RELAPACK_ztgsyl_rec(trans, ifunc, &m1, n, A_TL, ldA, B, ldB, C_T, ldC, D_TL, ldD, E, ldE, F_T, ldF, scale2, dsum, dscale, info2);
            // apply scale
            if (scale2[0] != 1) {
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, &m2, n, C_B, ldC, info);
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, &m2, n, F_B, ldF, info);
            }
        } else {
            // recursion(A_TL, B, C_T, D_TL, E, F_T)
            RELAPACK_ztgsyl_rec(trans, ifunc, &m1, n, A_TL, ldA, B, ldB, C_T, ldC, D_TL, ldD, E, ldE, F_T, ldF, scale1, dsum, dscale, info1);
            // apply scale
            if (scale1[0] != 1)
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale1, &m2, n, F_B, ldF, info);
            // C_B = C_B - A_TR^H * C_T
            BLAS(zgemm)("C", "N", &m2, n, &m1, MONE, A_TR, ldA, C_T, ldC, scale1, C_B, ldC);
            // C_B = C_B - D_TR^H * F_T
            BLAS(zgemm)("C", "N", &m2, n, &m1, MONE, D_TR, ldD, F_T, ldC, ONE, C_B, ldC);
            // recursion(A_BR, B, C_B, D_BR, E, F_B)
            RELAPACK_ztgsyl_rec(trans, ifunc, &m2, n, A_BR, ldA, B, ldB, C_B, ldC, D_BR, ldD, E, ldE, F_B, ldF, scale2, dsum, dscale, info2);
            // apply scale
            if (scale2[0] != 1) {
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, &m1, n, C_T, ldC, info);
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, &m1, n, F_T, ldF, info);
            }
        }
    } else {
        // Splitting
        const int n1 = ZREC_SPLIT(*n);
        const int n2 = *n - n1;

        // B_TL B_TR
        // 0    B_BR
        const double *const B_TL = B;
        const double *const B_TR = B + 2 * *ldB * n1;
        const double *const B_BR = B + 2 * *ldB * n1 + 2 * n1;

        // C_L C_R
        double *const C_L = C;
        double *const C_R = C + 2 * *ldC * n1;

        // E_TL E_TR
        // 0    E_BR
        const double *const E_TL = E;
        const double *const E_TR = E + 2 * *ldE * n1;
        const double *const E_BR = E + 2 * *ldE * n1 + 2 * n1;

        // F_L F_R
        double *const F_L = F;
        double *const F_R = F + 2 * *ldF * n1;

        if (*trans == 'N') {
            // recursion(A, B_TL, C_L, D, E_TL, F_L)
            RELAPACK_ztgsyl_rec(trans, ifunc, m, &n1, A, ldA, B_TL, ldB, C_L, ldC, D, ldD, E_TL, ldE, F_L, ldF, scale1, dsum, dscale, info1);
            // C_R = C_R + F_L * B_TR
            BLAS(zgemm)("N", "N", m, &n2, &n1, ONE, F_L, ldF, B_TR, ldB, scale1, C_R, ldC);
            // F_R = F_R + F_L * E_TR
            BLAS(zgemm)("N", "N", m, &n2, &n1, ONE, F_L, ldF, E_TR, ldE, scale1, F_R, ldF);
            // recursion(A, B_BR, C_R, D, E_BR, F_R)
            RELAPACK_ztgsyl_rec(trans, ifunc, m, &n2, A, ldA, B_BR, ldB, C_R, ldC, D, ldD, E_BR, ldE, F_R, ldF, scale2, dsum, dscale, info2);
            // apply scale
            if (scale2[0] != 1) {
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, m, &n1, C_L, ldC, info);
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, m, &n1, F_L, ldF, info);
            }
        } else {
            // recursion(A, B_BR, C_R, D, E_BR, F_R)
            RELAPACK_ztgsyl_rec(trans, ifunc, m, &n2, A, ldA, B_BR, ldB, C_R, ldC, D, ldD, E_BR, ldE, F_R, ldF, scale1, dsum, dscale, info1);
            // apply scale
            if (scale1[0] != 1)
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale1, m, &n1, C_L, ldC, info);
            // F_L = F_L + C_R * B_TR
            BLAS(zgemm)("N", "C", m, &n1, &n2, ONE, C_R, ldC, B_TR, ldB, scale1, F_L, ldF);
            // F_L = F_L + F_R * E_TR
            BLAS(zgemm)("N", "C", m, &n1, &n2, ONE, F_R, ldF, E_TR, ldB, ONE, F_L, ldF);
            // recursion(A, B_TL, C_L, D, E_TL, F_L)
            RELAPACK_ztgsyl_rec(trans, ifunc, m, &n1, A, ldA, B_TL, ldB, C_L, ldC, D, ldD, E_TL, ldE, F_L, ldF, scale2, dsum, dscale, info2);
            // apply scale
            if (scale2[0] != 1) {
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, m, &n2, C_R, ldC, info);
                LAPACK(zlascl)("G", iONE, iONE, ONE, scale2, m, &n2, F_R, ldF, info);
            }
        }
    }

    *scale = scale1[0] * scale2[0];
    *info  = info1[0] || info2[0];
}
