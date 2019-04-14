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
static float c_b26 = 1.f;
static float c_b30 = 0.f;
static int c_true = TRUE_;

void RELAPACK_strsyl_rec2(char *trana, char *tranb, int *isgn, int
	*m, int *n, float *a, int *lda, float *b, int *ldb, float *
	c__, int *ldc, float *scale, int *info, ftnlen trana_len,
	ftnlen tranb_len)
{
    /* System generated locals */
    int a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2,
	    i__3, i__4;
    float r__1, r__2;

    /* Local variables */
    static int j, k, l;
    static float x[4]	/* was [2][2] */;
    static int k1, k2, l1, l2;
    static float a11, db, da11, vec[4]	/* was [2][2] */, dum[1], eps, sgn;
    static int ierr;
    static float smin;
    extern float sdot_(int *, float *, int *, float *, int *);
    static float suml, sumr;
    extern int lsame_(char *, char *, ftnlen, ftnlen);
    extern /* Subroutine */ int sscal_(int *, float *, float *, int *);
    static int knext, lnext;
    static float xnorm;
    extern /* Subroutine */ int slaln2_(int *, int *, int *, float
	    *, float *, float *, int *, float *, float *, float *, int *,
	    float *, float *, float *, int *, float *, float *, int *),
	    slasy2_(int *, int *, int *, int *, int *,
	    float *, int *, float *, int *, float *, int *, float *,
	    float *, int *, float *, int *), slabad_(float *, float *);
    static float scaloc;
    extern float slamch_(char *, ftnlen), slange_(char *, int *,
	    int *, float *, int *, float *, ftnlen);
    extern /* Subroutine */ int xerbla_(char *, int *, ftnlen);
    static float bignum;
    static int notrna, notrnb;
    static float smlnum;

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
	xerbla_("STRSYL", &i__1, (ftnlen)6);
	return;
    }
    *scale = 1.f;
    if (*m == 0 || *n == 0) {
	return;
    }
    eps = slamch_("P", (ftnlen)1);
    smlnum = slamch_("S", (ftnlen)1);
    bignum = 1.f / smlnum;
    slabad_(&smlnum, &bignum);
    smlnum = smlnum * (float) (*m * *n) / eps;
    bignum = 1.f / smlnum;
/* Computing MAX */
    r__1 = smlnum, r__2 = eps * slange_("M", m, m, &a[a_offset], lda, dum, (
	    ftnlen)1), r__1 = max(r__1,r__2), r__2 = eps * slange_("M", n, n,
	    &b[b_offset], ldb, dum, (ftnlen)1);
    smin = dmax(r__1,r__2);
    sgn = (float) (*isgn);
    if (notrna && notrnb) {
	lnext = 1;
	i__1 = *n;
	for (l = 1; l <= i__1; ++l) {
	    if (l < lnext) {
		goto L70;
	    }
	    if (l == *n) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + 1 + l * b_dim1] != 0.f) {
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
		    goto L60;
		}
		if (k == 1) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + (k - 1) * a_dim1] != 0.f) {
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
		    suml = sdot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.f;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = dabs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = dabs(vec[0]);
		    if (da11 < 1.f && db > 1.f) {
			if (db > bignum * da11) {
			    scaloc = 1.f / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
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
		    suml = sdot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = sdot_(&i__2, &a[k2 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    r__1 = -sgn * b[l1 + l1 * b_dim1];
		    slaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1
			    * a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &r__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
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
		    suml = sdot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__2 = *m - k1;
/* Computing MIN */
		    i__3 = k1 + 1;
/* Computing MIN */
		    i__4 = k1 + 1;
		    suml = sdot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l2 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    r__1 = -sgn * a[k1 + k1 * a_dim1];
		    slaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1 *
			     b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &r__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L40: */
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
		    suml = sdot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = sdot_(&i__2, &a[k1 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l2 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = sdot_(&i__2, &a[k2 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l1 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = *m - k2;
/* Computing MIN */
		    i__3 = k2 + 1;
/* Computing MIN */
		    i__4 = k2 + 1;
		    suml = sdot_(&i__2, &a[k2 + min(i__3,*m) * a_dim1], lda, &
			    c__[min(i__4,*m) + l2 * c_dim1], &c__1);
		    i__2 = l1 - 1;
		    sumr = sdot_(&i__2, &c__[k2 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    slasy2_(&c_false, &c_false, isgn, &c__2, &c__2, &a[k1 +
			    k1 * a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec,
			     &c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L50: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L60:
		;
	    }
L70:
	    ;
	}
    } else if (! notrna && notrnb) {
	lnext = 1;
	i__1 = *n;
	for (l = 1; l <= i__1; ++l) {
	    if (l < lnext) {
		goto L130;
	    }
	    if (l == *n) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + 1 + l * b_dim1] != 0.f) {
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
		    goto L120;
		}
		if (k == *m) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + 1 + k * a_dim1] != 0.f) {
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
		    suml = sdot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.f;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = dabs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = dabs(vec[0]);
		    if (da11 < 1.f && db > 1.f) {
			if (db > bignum * da11) {
			    scaloc = 1.f / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L80: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		} else if (l1 == l2 && k1 != k2) {
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    r__1 = -sgn * b[l1 + l1 * b_dim1];
		    slaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1 *
			     a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &r__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L90: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k2 + l1 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 == k2) {
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    r__1 = -sgn * a[k1 + k1 * a_dim1];
		    slaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1 *
			     b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &r__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L100: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 != k2) {
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k1 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k2 + c_dim1], ldc, &b[l1 *
			    b_dim1 + 1], &c__1);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__3 = k1 - 1;
		    suml = sdot_(&i__3, &a[k2 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__3 = l1 - 1;
		    sumr = sdot_(&i__3, &c__[k2 + c_dim1], ldc, &b[l2 *
			    b_dim1 + 1], &c__1);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    slasy2_(&c_true, &c_false, isgn, &c__2, &c__2, &a[k1 + k1
			    * a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec, &
			    c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__3 = *n;
			for (j = 1; j <= i__3; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L110: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L120:
		;
	    }
L130:
	    ;
	}
    } else if (! notrna && ! notrnb) {
	lnext = *n;
	for (l = *n; l >= 1; --l) {
	    if (l > lnext) {
		goto L190;
	    }
	    if (l == 1) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + (l - 1) * b_dim1] != 0.f) {
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
		    goto L180;
		}
		if (k == *m) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + 1 + k * a_dim1] != 0.f) {
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
		    suml = sdot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l1;
/* Computing MIN */
		    i__3 = l1 + 1;
/* Computing MIN */
		    i__4 = l1 + 1;
		    sumr = sdot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.f;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = dabs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = dabs(vec[0]);
		    if (da11 < 1.f && db > 1.f) {
			if (db > bignum * da11) {
			    scaloc = 1.f / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L140: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		} else if (l1 == l2 && k1 != k2) {
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k2 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    r__1 = -sgn * b[l1 + l1 * b_dim1];
		    slaln2_(&c_true, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1 *
			     a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &r__1,
			    &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L150: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k2 + l1 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 == k2) {
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l2 + min(i__4,*n) * b_dim1], ldb);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    r__1 = -sgn * a[k1 + k1 * a_dim1];
		    slaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1
			    * b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &r__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L160: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[1];
		} else if (l1 != l2 && k1 != k2) {
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k1 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k1 + min(i__3,*n) * c_dim1], ldc,
			     &b[l2 + min(i__4,*n) * b_dim1], ldb);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k2 * a_dim1 + 1], &c__1, &c__[l1 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k2 + min(i__3,*n) * c_dim1], ldc,
			     &b[l1 + min(i__4,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__2 = k1 - 1;
		    suml = sdot_(&i__2, &a[k2 * a_dim1 + 1], &c__1, &c__[l2 *
			    c_dim1 + 1], &c__1);
		    i__2 = *n - l2;
/* Computing MIN */
		    i__3 = l2 + 1;
/* Computing MIN */
		    i__4 = l2 + 1;
		    sumr = sdot_(&i__2, &c__[k2 + min(i__3,*n) * c_dim1], ldc,
			     &b[l2 + min(i__4,*n) * b_dim1], ldb);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    slasy2_(&c_true, &c_true, isgn, &c__2, &c__2, &a[k1 + k1 *
			     a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec, &
			    c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__2 = *n;
			for (j = 1; j <= i__2; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L170: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L180:
		;
	    }
L190:
	    ;
	}
    } else if (notrna && ! notrnb) {
	lnext = *n;
	for (l = *n; l >= 1; --l) {
	    if (l > lnext) {
		goto L250;
	    }
	    if (l == 1) {
		l1 = l;
		l2 = l;
	    } else {
		if (b[l + (l - 1) * b_dim1] != 0.f) {
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
		    goto L240;
		}
		if (k == 1) {
		    k1 = k;
		    k2 = k;
		} else {
		    if (a[k + (k - 1) * a_dim1] != 0.f) {
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
		    suml = sdot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l1;
/* Computing MIN */
		    i__2 = l1 + 1;
/* Computing MIN */
		    i__3 = l1 + 1;
		    sumr = sdot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    scaloc = 1.f;
		    a11 = a[k1 + k1 * a_dim1] + sgn * b[l1 + l1 * b_dim1];
		    da11 = dabs(a11);
		    if (da11 <= smin) {
			a11 = smin;
			da11 = smin;
			*info = 1;
		    }
		    db = dabs(vec[0]);
		    if (da11 < 1.f && db > 1.f) {
			if (db > bignum * da11) {
			    scaloc = 1.f / db;
			}
		    }
		    x[0] = vec[0] * scaloc / a11;
		    if (scaloc != 1.f) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L200: */
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
		    suml = sdot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = sdot_(&i__1, &a[k2 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k2 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    r__1 = -sgn * b[l1 + l1 * b_dim1];
		    slaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &a[k1 + k1
			    * a_dim1], lda, &c_b26, &c_b26, vec, &c__2, &r__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L210: */
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
		    suml = sdot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = sgn * (c__[k1 + l1 * c_dim1] - (suml + sgn *
			    sumr));
		    i__1 = *m - k1;
/* Computing MIN */
		    i__2 = k1 + 1;
/* Computing MIN */
		    i__3 = k1 + 1;
		    suml = sdot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l2 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l2 + min(i__3,*n) * b_dim1], ldb);
		    vec[1] = sgn * (c__[k1 + l2 * c_dim1] - (suml + sgn *
			    sumr));
		    r__1 = -sgn * a[k1 + k1 * a_dim1];
		    slaln2_(&c_false, &c__2, &c__1, &smin, &c_b26, &b[l1 + l1
			    * b_dim1], ldb, &c_b26, &c_b26, vec, &c__2, &r__1,
			     &c_b30, x, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L220: */
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
		    suml = sdot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[0] = c__[k1 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = sdot_(&i__1, &a[k1 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l2 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k1 + min(i__2,*n) * c_dim1], ldc,
			     &b[l2 + min(i__3,*n) * b_dim1], ldb);
		    vec[2] = c__[k1 + l2 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = sdot_(&i__1, &a[k2 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l1 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k2 + min(i__2,*n) * c_dim1], ldc,
			     &b[l1 + min(i__3,*n) * b_dim1], ldb);
		    vec[1] = c__[k2 + l1 * c_dim1] - (suml + sgn * sumr);
		    i__1 = *m - k2;
/* Computing MIN */
		    i__2 = k2 + 1;
/* Computing MIN */
		    i__3 = k2 + 1;
		    suml = sdot_(&i__1, &a[k2 + min(i__2,*m) * a_dim1], lda, &
			    c__[min(i__3,*m) + l2 * c_dim1], &c__1);
		    i__1 = *n - l2;
/* Computing MIN */
		    i__2 = l2 + 1;
/* Computing MIN */
		    i__3 = l2 + 1;
		    sumr = sdot_(&i__1, &c__[k2 + min(i__2,*n) * c_dim1], ldc,
			     &b[l2 + min(i__3,*n) * b_dim1], ldb);
		    vec[3] = c__[k2 + l2 * c_dim1] - (suml + sgn * sumr);
		    slasy2_(&c_false, &c_true, isgn, &c__2, &c__2, &a[k1 + k1
			    * a_dim1], lda, &b[l1 + l1 * b_dim1], ldb, vec, &
			    c__2, &scaloc, x, &c__2, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 1;
		    }
		    if (scaloc != 1.f) {
			i__1 = *n;
			for (j = 1; j <= i__1; ++j) {
			    sscal_(m, &scaloc, &c__[j * c_dim1 + 1], &c__1);
/* L230: */
			}
			*scale *= scaloc;
		    }
		    c__[k1 + l1 * c_dim1] = x[0];
		    c__[k1 + l2 * c_dim1] = x[2];
		    c__[k2 + l1 * c_dim1] = x[1];
		    c__[k2 + l2 * c_dim1] = x[3];
		}
L240:
		;
	    }
L250:
	    ;
	}
    }
}
