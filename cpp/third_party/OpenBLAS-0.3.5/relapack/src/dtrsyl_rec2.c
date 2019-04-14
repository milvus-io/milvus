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

#include "f2c.h"

/* Table of constant values */

static int c__1 = 1;
static int c_false = FALSE_;
static int c__2 = 2;
static double c_b26 = 1.;
static double c_b30 = 0.;
static int c_true = TRUE_;

int RELAPACK_dtrsyl_rec2(char *trana, char *tranb, int *isgn, int
	*m, int *n, double *a, int *lda, double *b, int *
	ldb, double *c__, int *ldc, double *scale, int *info,
	ftnlen trana_len, ftnlen tranb_len)
{
    /* System generated locals */
    int a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2,
	    i__3, i__4;
    double d__1, d__2;

    /* Local variables */
    static int j, k, l;
    static double x[4]	/* was [2][2] */;
    static int k1, k2, l1, l2;
    static double a11, db, da11, vec[4]	/* was [2][2] */, dum[1], eps,
	     sgn;
    extern double ddot_(int *, double *, int *, double *,
	    int *);
    static int ierr;
    static double smin, suml, sumr;
    extern /* Subroutine */ int dscal_(int *, double *, double *,
	    int *);
    extern int lsame_(char *, char *, ftnlen, ftnlen);
    static int knext, lnext;
    static double xnorm;
    extern /* Subroutine */ int dlaln2_(int *, int *, int *,
	    double *, double *, double *, int *, double *,
	     double *, double *, int *, double *, double *
	    , double *, int *, double *, double *, int *),
	     dlasy2_(int *, int *, int *, int *, int *,
	    double *, int *, double *, int *, double *,
	    int *, double *, double *, int *, double *,
	    int *), dlabad_(double *, double *);
    extern double dlamch_(char *, ftnlen), dlange_(char *, int *,
	    int *, double *, int *, double *, ftnlen);
    static double scaloc;
    extern /* Subroutine */ int xerbla_(char *, int *, ftnlen);
    static double bignum;
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
    if (! notrna && ! lsame_(trana, "T", (ftnlen)1, (ftnlen)1) && ! lsame_(
	    trana, "C", (ftnlen)1, (ftnlen)1)) {
	*info = -1;
    } else if (! notrnb && ! lsame_(tranb, "T", (ftnlen)1, (ftnlen)1) && !
	    lsame_(tranb, "C", (ftnlen)1, (ftnlen)1)) {
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
	xerbla_("DTRSYL", &i__1, (ftnlen)6);
	return 0;
    }
    *scale = 1.;
    if (*m == 0 || *n == 0) {
	return 0;
    }
    eps = dlamch_("P", (ftnlen)1);
    smlnum = dlamch_("S", (ftnlen)1);
    bignum = 1. / smlnum;
    dlabad_(&smlnum, &bignum);
    smlnum = smlnum * (double) (*m * *n) / eps;
    bignum = 1. / smlnum;
/* Computing MAX */
    d__1 = smlnum, d__2 = eps * dlange_("M", m, m, &a[a_offset], lda, dum, (
	    ftnlen)1), d__1 = max(d__1,d__2), d__2 = eps * dlange_("M", n, n,
	    &b[b_offset], ldb, dum, (ftnlen)1);
    smin = max(d__1,d__2);
    sgn = (double) (*isgn);
    if (notrna && notrnb) {
	lnext = 1;
	i__1 = *n;
	for (l = 1; l <= i__1; ++l) {
	    if (l < lnext) {
		goto L60;
	    }
	    if (l == *n) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + 1 + l * b_dim1] != 0.) {
		    l1 = l;
		    l2 = l + 1;
		    lnext = l + 2;
		} else {
		    l1 = l;
		    l2 = l;
		    lnext = l + 1;
		}
	    }
	    knext = *m;
	    for (k = *m; k >= 1; --k) {
		if (k > knext) {
		    goto L50;
		}
		if (k == 1) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + (k - 1) * a_dim1] != 0.) {
			k1 = k - 1;
			k2 = k;
			knext = k - 2;
		    } else {
			k1 = k;
			k2 = k;
			knext = k - 1;
		    }
		}
		if (l1 == l2 && k1 == k2) {
		    i__2 = *m - k1;
/* Computing MIN */
		    i__3 = k1 + 1;
/* Computing MIN */
		    i__4 = k1 + 1;
		    suml = ddot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = abs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = abs(vec[0]);
		    if (da11 < 1. && db > 1.) {
			if (db > bignum * da11) {
			    scaloc = 1. / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L10: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		} else if (l1 == l2 && k1 != k2) {
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = ddot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = ddot_(&i__2, &a[k2 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    d__1 = -sgn * b[l1 + l1 * b_dim1];
		    dlaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1
			    * a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &d__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L20: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k2 + l1 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 == k2) {
		    i__2 = *m - k1;
/* Computing MIN */
		    i__3 = k1 + 1;
/* Computing MIN */
		    i__4 = k1 + 1;
		    suml = ddot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__2 = *m - k1;
/* Computing MIN */
		    i__3 = k1 + 1;
/* Computing MIN */
		    i__4 = k1 + 1;
		    suml = ddot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l2 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    d__1 = -sgn * a[k1 + k1 * a_dim1];
		    dlaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1 *
			     b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &d__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L30: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 != k2) {
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = ddot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = ddot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l2 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = ddot_(&i__2, &a[k2 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = ddot_(&i__2, &a[k2 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l2 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = ddot_(&i__2, &c__[k2 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    dlasy2_(&c_false, &c_false, isgn, &c__2, &c__2, &a[k1 +
			    k1 * a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec,
			     &c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L40: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L50:
		;
	    }
L60:
	    ;
	}
    } else if (! notrna && notrnb) {
	lnext = 1;
	i__1 = *n;
	for (l = 1; l <= i__1; ++l) {
	    if (l < lnext) {
		goto L120;
	    }
	    if (l == *n) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + 1 + l * b_dim1] != 0.) {
		    l1 = l;
		    l2 = l + 1;
		    lnext = l + 2;
		} else {
		    l1 = l;
		    l2 = l;
		    lnext = l + 1;
		}
	    }
	    knext = 1;
	    i__2 = *m;
	    for (k = 1; k <= i__2; ++k) {
		if (k < knext) {
		    goto L110;
		}
		if (k == *m) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + 1 + k * a_dim1] != 0.) {
			k1 = k;
			k2 = k + 1;
			knext = k + 2;
		    } else {
			k1 = k;
			k2 = k;
			knext = k + 1;
		    }
		}
		if (l1 == l2 && k1 == k2) {
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = abs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = abs(vec[0]);
		    if (da11 < 1. && db > 1.) {
			if (db > bignum * da11) {
			    scaloc = 1. / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L70: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		} else if (l1 == l2 && k1 != k2) {
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    d__1 = -sgn * b[l1 + l1 * b_dim1];
		    dlaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1 *
			     a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &d__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L80: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k2 + l1 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 == k2) {
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    d__1 = -sgn * a[k1 + k1 * a_dim1];
		    dlaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1 *
			     b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &d__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L90: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 != k2) {
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = ddot_(&i__3, &a[k2 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = ddot_(&i__3, &c__[k2 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    dlasy2_(&c_true, &c_false, isgn, &c__2, &c__2, &a[k1 + k1
			    * a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec, &
			    c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L100: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L110:
		;
	    }
L120:
	    ;
	}
    } else if (! notrna && ! notrnb) {
	lnext = *n;
	for (l = *n; l >= 1; --l) {
	    if (l > lnext) {
		goto L180;
	    }
	    if (l == 1) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + (l - 1) * b_dim1] != 0.) {
		    l1 = l - 1;
		    l2 = l;
		    lnext = l - 2;
		} else {
		    l1 = l;
		    l2 = l;
		    lnext = l - 1;
		}
	    }
	    knext = 1;
	    i__1 = *m;
	    for (k = 1; k <= i__1; ++k) {
		if (k < knext) {
		    goto L170;
		}
		if (k == *m) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + 1 + k * a_dim1] != 0.) {
			k1 = k;
			k2 = k + 1;
			knext = k + 2;
		    } else {
			k1 = k;
			k2 = k;
			knext = k + 1;
		    }
		}
		if (l1 == l2 && k1 == k2) {
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l1;
/* Computing MIN */
		    i__3 = l1 + 1;
/* Computing MIN */
		    i__4 = l1 + 1;
		    sumr = ddot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = abs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = abs(vec[0]);
		    if (da11 < 1. && db > 1.) {
			if (db > bignum * da11) {
			    scaloc = 1. / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L130: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		} else if (l1 == l2 && k1 != k2) {
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k2 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    d__1 = -sgn * b[l1 + l1 * b_dim1];
		    dlaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1 *
			     a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &d__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L140: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k2 + l1 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 == k2) {
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l2 + min(i__4,*n) * b_dim1], ldb);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    d__1 = -sgn * a[k1 + k1 * a_dim1];
		    dlaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1
			    * b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &d__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L150: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 != k2) {
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l2 + min(i__4,*n) * b_dim1], ldb);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k2 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = ddot_(&i__2, &a[k2 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = ddot_(&i__2, &c__[k2 + min(i__3,*n) * c_dim1], ldc,
			     &b[l2 + min(i__4,*n) * b_dim1], ldb);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    dlasy2_(&c_true, &c_true, isgn, &c__2, &c__2, &a[k1 + k1 *
			     a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec, &
			    c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L160: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L170:
		;
	    }
L180:
	    ;
	}
    } else if (notrna && ! notrnb) {
	lnext = *n;
	for (l = *n; l >= 1; --l) {
	    if (l > lnext) {
		goto L240;
	    }
	    if (l == 1) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + (l - 1) * b_dim1] != 0.) {
		    l1 = l - 1;
		    l2 = l;
		    lnext = l - 2;
		} else {
		    l1 = l;
		    l2 = l;
		    lnext = l - 1;
		}
	    }
	    knext = *m;
	    for (k = *m; k >= 1; --k) {
		if (k > knext) {
		    goto L230;
		}
		if (k == 1) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + (k - 1) * a_dim1] != 0.) {
			k1 = k - 1;
			k2 = k;
			knext = k - 2;
		    } else {
			k1 = k;
			k2 = k;
			knext = k - 1;
		    }
		}
		if (l1 == l2 && k1 == k2) {
		    i__1 = *m - k1;
/* Computing MIN */
		    i__2 = k1 + 1;
/* Computing MIN */
		    i__3 = k1 + 1;
		    suml = ddot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l1;
/* Computing MIN */
		    i__2 = l1 + 1;
/* Computing MIN */
		    i__3 = l1 + 1;
		    sumr = ddot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = abs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = abs(vec[0]);
		    if (da11 < 1. && db > 1.) {
			if (db > bignum * da11) {
			    scaloc = 1. / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L190: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		} else if (l1 == l2 && k1 != k2) {
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = ddot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = ddot_(&i__1, &a[k2 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k2 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    d__1 = -sgn * b[l1 + l1 * b_dim1];
		    dlaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1
			    * a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &d__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L200: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k2 + l1 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 == k2) {
		    i__1 = *m - k1;
/* Computing MIN */
		    i__2 = k1 + 1;
/* Computing MIN */
		    i__3 = k1 + 1;
		    suml = ddot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__1 = *m - k1;
/* Computing MIN */
		    i__2 = k1 + 1;
/* Computing MIN */
		    i__3 = k1 + 1;
		    suml = ddot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l2 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l2 + min(i__3,*n) * b_dim1], ldb);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    d__1 = -sgn * a[k1 + k1 * a_dim1];
		    dlaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1
			    * b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &d__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L210: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 != k2) {
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = ddot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = ddot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l2 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l2 + min(i__3,*n) * b_dim1], ldb);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = ddot_(&i__1, &a[k2 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k2 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = ddot_(&i__1, &a[k2 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l2 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = ddot_(&i__1, &c__[k2 + min(i__2,*n) * c_dim1], ldc,
			     &b[l2 + min(i__3,*n) * b_dim1], ldb);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    dlasy2_(&c_false, &c_true, isgn, &c__2, &c__2, &a[k1 + k1
			    * a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec, &
			    c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    dscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L220: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L230:
		;
	    }
L240:
	    ;
	}
    }
    return 0;
}
