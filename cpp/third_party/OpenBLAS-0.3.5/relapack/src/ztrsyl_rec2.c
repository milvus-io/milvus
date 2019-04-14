/*  -- translated by f2c (version 20100827).
   You must link the resulting object file with libf2c:
	on Microsoft Windows system, link with libf2c.lib;
	on Linux or Unix systems, link with .../path/to/libf2c.a -lm
	or, if you install libf2c.a in a standard place, with -lf2c -lm
	-- in that order, at the end of the command line, as in
		cc *.o -lf2c -lm
	Source for libf2c is in /netlib/f2c/libf2c.zip, e.g.,

		http://www.netlib.org/f2c/libf2c.zip
*/

#include "../config.h"
#include "f2c.h"

#if BLAS_COMPLEX_FUNCTIONS_AS_ROUTINES
doublecomplex zdotu_fun(int *n, doublecomplex *x, int *incx, doublecomplex *y, int *incy) {
    extern void zdotu_(doublecomplex *, int *, doublecomplex *, int *, doublecomplex *, int *);
    doublecomplex result;
    zdotu_(&result, n, x, incx, y, incy);
    return result;
}
#define zdotu_ zdotu_fun

doublecomplex zdotc_fun(int *n, doublecomplex *x, int *incx, doublecomplex *y, int *incy) {
    extern void zdotc_(doublecomplex *, int *, doublecomplex *, int *, doublecomplex *, int *);
    doublecomplex result;
    zdotc_(&result, n, x, incx, y, incy);
    return result;
}
#define zdotc_ zdotc_fun
#endif

#if LAPACK_BLAS_COMPLEX_FUNCTIONS_AS_ROUTINES
doublecomplex zladiv_fun(doublecomplex *a, doublecomplex *b) {
    extern void zladiv_(doublecomplex *, doublecomplex *, doublecomplex *);
    doublecomplex result;
    zladiv_(&result, a, b);
    return result;
}
#define zladiv_ zladiv_fun
#endif

/* Table of constant values */

static int c__1 = 1;

/** RELAPACK_ZTRSYL_REC2 solves the complex Sylvester matrix equation (unblocked algorithm)
 *
 * This routine is an exact copy of LAPACK's ztrsyl.
 * It serves as an unblocked kernel in the recursive algorithms.
 * */
/* Subroutine */ void RELAPACK_ztrsyl_rec2(char *trana, char *tranb, int
	*isgn, int *m, int *n, doublecomplex *a, int *lda,
	doublecomplex *b, int *ldb, doublecomplex *c__, int *ldc,
	double *scale, int *info, ftnlen trana_len, ftnlen tranb_len)
{
    /* System generated locals */
    int a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2,
	    i__3, i__4;
    double d__1, d__2;
    doublecomplex z__1, z__2, z__3, z__4;

    /* Builtin functions */
    double d_imag(doublecomplex *);
    void d_cnjg(doublecomplex *, doublecomplex *);

    /* Local variables */
    static int j, k, l;
    static doublecomplex a11;
    static double db;
    static doublecomplex x11;
    static double da11;
    static doublecomplex vec;
    static double dum[1], eps, sgn, smin;
    static doublecomplex suml, sumr;
    extern int lsame_(char *, char *, ftnlen, ftnlen);
    /* Double Complex */ doublecomplex zdotc_(int *,
	    doublecomplex *, int *, doublecomplex *, int *), zdotu_(
	    int *, doublecomplex *, int *,
	    doublecomplex *, int *);
    extern /* Subroutine */ int dlabad_(double *, double *);
    extern double dlamch_(char *, ftnlen);
    static double scaloc;
    extern /* Subroutine */ int xerbla_(char *, int *, ftnlen);
    extern double zlange_(char *, int *, int *, doublecomplex *,
	    int *, double *, ftnlen);
    static double bignum;
    extern /* Subroutine */ int zdscal_(int *, double *,
	    doublecomplex *, int *);
    /* Double Complex */ doublecomplex zladiv_(doublecomplex *,
	     doublecomplex *);
    static int notrna, notrnb;
    static double smlnum;

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1;
    b -= b_offset;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1;
    c__ -= c_offset;

    /* Function Body */
    notrna = lsame_(trana, "N", (ftnlen)1, (ftnlen)1);
    notrnb = lsame_(tranb, "N", (ftnlen)1, (ftnlen)1);
    *info = 0;
    if (! notrna && ! lsame_(trana, "C", (ftnlen)1, (ftnlen)1)) {
	*info = -1;
    } else if (! notrnb && ! lsame_(tranb, "C", (ftnlen)1, (ftnlen)1)) {
	*info = -2;
    } else if (*isgn != 1 && *isgn != -1) {
	*info = -3;
    } else if (*m < 0) {
	*info = -4;
    } else if (*n < 0) {
	*info = -5;
    } else if (*lda < max(1,*m)) {
	*info = -7;
    } else if (*ldb < max(1,*n)) {
	*info = -9;
    } else if (*ldc < max(1,*m)) {
	*info = -11;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZTRSY2", &i__1, (ftnlen)6);
	return;
    }
    *scale = 1.;
    if (*m == 0 || *n == 0) {
	return;
    }
    eps = dlamch_("P", (ftnlen)1);
    smlnum = dlamch_("S", (ftnlen)1);
    bignum = 1. / smlnum;
    dlabad_(&smlnum, &bignum);
    smlnum = smlnum * (double) (*m * *n) / eps;
    bignum = 1. / smlnum;
/* Computing MAX */
    d__1 = smlnum, d__2 = eps * zlange_("M", m, m, &a[a_offset], lda, dum, (
	    ftnlen)1), d__1 = max(d__1,d__2), d__2 = eps * zlange_("M", n, n,
	    &b[b_offset], ldb, dum, (ftnlen)1);
    smin = max(d__1,d__2);
    sgn = (double) (*isgn);
    if (notrna && notrnb) {
	i__1 = *n;
	for (l = 1; l <= i__1; ++l) {
	    for (k = *m; k >= 1; --k) {
		i__2 = *m - k;
/* Computing MIN */
		i__3 = k + 1;
/* Computing MIN */
		i__4 = k + 1;
		z__1 = zdotu_(&i__2, &a[k + min(i__3,*m) * a_dim1], lda, &c__[
			min(i__4,*m) + l * c_dim1], &c__1);
		suml.r = z__1.r, suml.i = z__1.i;
		i__2 = l - 1;
		z__1 = zdotu_(&i__2, &c__[k + c_dim1], ldc, &b[l * b_dim1 + 1]
			, &c__1);
		sumr.r = z__1.r, sumr.i = z__1.i;
		i__2 = k + l * c_dim1;
		z__3.r = sgn * sumr.r, z__3.i = sgn * sumr.i;
		z__2.r = suml.r + z__3.r, z__2.i = suml.i + z__3.i;
		z__1.r = c__[i__2].r - z__2.r, z__1.i = c__[i__2].i - z__2.i;
		vec.r = z__1.r, vec.i = z__1.i;
		scaloc = 1.;
		i__2 = k + k * a_dim1;
		i__3 = l + l * b_dim1;
		z__2.r = sgn * b[i__3].r, z__2.i = sgn * b[i__3].i;
		z__1.r = a[i__2].r + z__2.r, z__1.i = a[i__2].i + z__2.i;
		a11.r = z__1.r, a11.i = z__1.i;
		da11 = (d__1 = a11.r, abs(d__1)) + (d__2 = d_imag(&a11), abs(
			d__2));
		if (da11 <= smin) {
		    a11.r = smin, a11.i = 0.;
		    da11 = smin;
		    *info = 1;
		}
		db = (d__1 = vec.r, abs(d__1)) + (d__2 = d_imag(&vec), abs(
			d__2));
		if (da11 < 1. && db > 1.) {
		    if (db > bignum * da11) {
			scaloc = 1. / db;
		    }
		}
		z__3.r = scaloc, z__3.i = 0.;
		z__2.r = vec.r * z__3.r - vec.i * z__3.i, z__2.i = vec.r *
			z__3.i + vec.i * z__3.r;
		z__1 = zladiv_(&z__2, &a11);
		x11.r = z__1.r, x11.i = z__1.i;
		if (scaloc != 1.) {
		    i__2 = *n;
		    for (j = 1; j <= i__2; ++j) {
			zdscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L10: */
		    }
		    *scale *= scaloc;
		}
		i__2 = k + l * c_dim1;
		c__[i__2].r = x11.r, c__[i__2].i = x11.i;
/* L20: */
	    }
/* L30: */
	}
    } else if (! notrna && notrnb) {
	i__1 = *n;
	for (l = 1; l <= i__1; ++l) {
	    i__2 = *m;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = k - 1;
		z__1 = zdotc_(&i__3, &a[k * a_dim1 + 1], &c__1, &c__[l *
			c_dim1 + 1], &c__1);
		suml.r = z__1.r, suml.i = z__1.i;
		i__3 = l - 1;
		z__1 = zdotu_(&i__3, &c__[k + c_dim1], ldc, &b[l * b_dim1 + 1]
			, &c__1);
		sumr.r = z__1.r, sumr.i = z__1.i;
		i__3 = k + l * c_dim1;
		z__3.r = sgn * sumr.r, z__3.i = sgn * sumr.i;
		z__2.r = suml.r + z__3.r, z__2.i = suml.i + z__3.i;
		z__1.r = c__[i__3].r - z__2.r, z__1.i = c__[i__3].i - z__2.i;
		vec.r = z__1.r, vec.i = z__1.i;
		scaloc = 1.;
		d_cnjg(&z__2, &a[k + k * a_dim1]);
		i__3 = l + l * b_dim1;
		z__3.r = sgn * b[i__3].r, z__3.i = sgn * b[i__3].i;
		z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		a11.r = z__1.r, a11.i = z__1.i;
		da11 = (d__1 = a11.r, abs(d__1)) + (d__2 = d_imag(&a11), abs(
			d__2));
		if (da11 <= smin) {
		    a11.r = smin, a11.i = 0.;
		    da11 = smin;
		    *info = 1;
		}
		db = (d__1 = vec.r, abs(d__1)) + (d__2 = d_imag(&vec), abs(
			d__2));
		if (da11 < 1. && db > 1.) {
		    if (db > bignum * da11) {
			scaloc = 1. / db;
		    }
		}
		z__3.r = scaloc, z__3.i = 0.;
		z__2.r = vec.r * z__3.r - vec.i * z__3.i, z__2.i = vec.r *
			z__3.i + vec.i * z__3.r;
		z__1 = zladiv_(&z__2, &a11);
		x11.r = z__1.r, x11.i = z__1.i;
		if (scaloc != 1.) {
		    i__3 = *n;
		    for (j = 1; j <= i__3; ++j) {
			zdscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L40: */
		    }
		    *scale *= scaloc;
		}
		i__3 = k + l * c_dim1;
		c__[i__3].r = x11.r, c__[i__3].i = x11.i;
/* L50: */
	    }
/* L60: */
	}
    } else if (! notrna && ! notrnb) {
	for (l = *n; l >= 1; --l) {
	    i__1 = *m;
	    for (k = 1; k <= i__1; ++k) {
		i__2 = k - 1;
		z__1 = zdotc_(&i__2, &a[k * a_dim1 + 1], &c__1, &c__[l *
			c_dim1 + 1], &c__1);
		suml.r = z__1.r, suml.i = z__1.i;
		i__2 = *n - l;
/* Computing MIN */
		i__3 = l + 1;
/* Computing MIN */
		i__4 = l + 1;
		z__1 = zdotc_(&i__2, &c__[k + min(i__3,*n) * c_dim1], ldc, &b[
			l + min(i__4,*n) * b_dim1], ldb);
		sumr.r = z__1.r, sumr.i = z__1.i;
		i__2 = k + l * c_dim1;
		d_cnjg(&z__4, &sumr);
		z__3.r = sgn * z__4.r, z__3.i = sgn * z__4.i;
		z__2.r = suml.r + z__3.r, z__2.i = suml.i + z__3.i;
		z__1.r = c__[i__2].r - z__2.r, z__1.i = c__[i__2].i - z__2.i;
		vec.r = z__1.r, vec.i = z__1.i;
		scaloc = 1.;
		i__2 = k + k * a_dim1;
		i__3 = l + l * b_dim1;
		z__3.r = sgn * b[i__3].r, z__3.i = sgn * b[i__3].i;
		z__2.r = a[i__2].r + z__3.r, z__2.i = a[i__2].i + z__3.i;
		d_cnjg(&z__1, &z__2);
		a11.r = z__1.r, a11.i = z__1.i;
		da11 = (d__1 = a11.r, abs(d__1)) + (d__2 = d_imag(&a11), abs(
			d__2));
		if (da11 <= smin) {
		    a11.r = smin, a11.i = 0.;
		    da11 = smin;
		    *info = 1;
		}
		db = (d__1 = vec.r, abs(d__1)) + (d__2 = d_imag(&vec), abs(
			d__2));
		if (da11 < 1. && db > 1.) {
		    if (db > bignum * da11) {
			scaloc = 1. / db;
		    }
		}
		z__3.r = scaloc, z__3.i = 0.;
		z__2.r = vec.r * z__3.r - vec.i * z__3.i, z__2.i = vec.r *
			z__3.i + vec.i * z__3.r;
		z__1 = zladiv_(&z__2, &a11);
		x11.r = z__1.r, x11.i = z__1.i;
		if (scaloc != 1.) {
		    i__2 = *n;
		    for (j = 1; j <= i__2; ++j) {
			zdscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L70: */
		    }
		    *scale *= scaloc;
		}
		i__2 = k + l * c_dim1;
		c__[i__2].r = x11.r, c__[i__2].i = x11.i;
/* L80: */
	    }
/* L90: */
	}
    } else if (notrna && ! notrnb) {
	for (l = *n; l >= 1; --l) {
	    for (k = *m; k >= 1; --k) {
		i__1 = *m - k;
/* Computing MIN */
		i__2 = k + 1;
/* Computing MIN */
		i__3 = k + 1;
		z__1 = zdotu_(&i__1, &a[k + min(i__2,*m) * a_dim1], lda, &c__[
			min(i__3,*m) + l * c_dim1], &c__1);
		suml.r = z__1.r, suml.i = z__1.i;
		i__1 = *n - l;
/* Computing MIN */
		i__2 = l + 1;
/* Computing MIN */
		i__3 = l + 1;
		z__1 = zdotc_(&i__1, &c__[k + min(i__2,*n) * c_dim1], ldc, &b[
			l + min(i__3,*n) * b_dim1], ldb);
		sumr.r = z__1.r, sumr.i = z__1.i;
		i__1 = k + l * c_dim1;
		d_cnjg(&z__4, &sumr);
		z__3.r = sgn * z__4.r, z__3.i = sgn * z__4.i;
		z__2.r = suml.r + z__3.r, z__2.i = suml.i + z__3.i;
		z__1.r = c__[i__1].r - z__2.r, z__1.i = c__[i__1].i - z__2.i;
		vec.r = z__1.r, vec.i = z__1.i;
		scaloc = 1.;
		i__1 = k + k * a_dim1;
		d_cnjg(&z__3, &b[l + l * b_dim1]);
		z__2.r = sgn * z__3.r, z__2.i = sgn * z__3.i;
		z__1.r = a[i__1].r + z__2.r, z__1.i = a[i__1].i + z__2.i;
		a11.r = z__1.r, a11.i = z__1.i;
		da11 = (d__1 = a11.r, abs(d__1)) + (d__2 = d_imag(&a11), abs(
			d__2));
		if (da11 <= smin) {
		    a11.r = smin, a11.i = 0.;
		    da11 = smin;
		    *info = 1;
		}
		db = (d__1 = vec.r, abs(d__1)) + (d__2 = d_imag(&vec), abs(
			d__2));
		if (da11 < 1. && db > 1.) {
		    if (db > bignum * da11) {
			scaloc = 1. / db;
		    }
		}
		z__3.r = scaloc, z__3.i = 0.;
		z__2.r = vec.r * z__3.r - vec.i * z__3.i, z__2.i = vec.r *
			z__3.i + vec.i * z__3.r;
		z__1 = zladiv_(&z__2, &a11);
		x11.r = z__1.r, x11.i = z__1.i;
		if (scaloc != 1.) {
		    i__1 = *n;
		    for (j = 1; j <= i__1; ++j) {
			zdscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L100: */
		    }
		    *scale *= scaloc;
		}
		i__1 = k + l * c_dim1;
		c__[i__1].r = x11.r, c__[i__1].i = x11.i;
/* L110: */
	    }
/* L120: */
	}
    }
    return;
}
