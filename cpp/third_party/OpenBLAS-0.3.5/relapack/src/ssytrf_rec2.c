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
static float c_b8 = -1.f;
static float c_b9 = 1.f;

/** SSYTRF_REC2 computes a partial factorization of a real symmetric matrix using the Bunch-Kaufman diagon al pivoting method.
 *
 * This routine is a minor modification of LAPACK's slasyf.
 * It serves as an unblocked kernel in the recursive algorithms.
 * The blocked BLAS Level 3 updates were removed and moved to the
 * recursive algorithm.
 * */
/* Subroutine */ void RELAPACK_ssytrf_rec2(char *uplo, int *n, int *
	nb, int *kb, float *a, int *lda, int *ipiv, float *w,
	int *ldw, int *info, ftnlen uplo_len)
{
    /* System generated locals */
    int a_dim1, a_offset, w_dim1, w_offset, i__1, i__2;
    float r__1, r__2, r__3;

    /* Builtin functions */
    double sqrt(double);

    /* Local variables */
    static int j, k;
    static float t, r1, d11, d21, d22;
    static int jj, kk, jp, kp, kw, kkw, imax, jmax;
    static float alpha;
    extern logical lsame_(char *, char *, ftnlen, ftnlen);
    extern /* Subroutine */ int sscal_(int *, float *, float *, int *),
	    sgemv_(char *, int *, int *, float *, float *, int *,
	    float *, int *, float *, float *, int *, ftnlen);
    static int kstep;
    extern /* Subroutine */ int scopy_(int *, float *, int *, float *,
	    int *), sswap_(int *, float *, int *, float *, int *
	    );
    static float absakk;
    extern int isamax_(int *, float *, int *);
    static float colmax, rowmax;

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1;
    a -= a_offset;
    --ipiv;
    w_dim1 = *ldw;
    w_offset = 1 + w_dim1;
    w -= w_offset;

    /* Function Body */
    *info = 0;
    alpha = (sqrt(17.f) + 1.f) / 8.f;
    if (lsame_(uplo, "U", (ftnlen)1, (ftnlen)1)) {
	k = *n;
L10:
	kw = *nb + k - *n;
	if ((k <= *n - *nb + 1 && *nb < *n) || k < 1) {
	    goto L30;
	}
	scopy_(&k, &a[k * a_dim1 + 1], &c__1, &w[kw * w_dim1 + 1], &c__1);
	if (k < *n) {
	    i__1 = *n - k;
	    sgemv_("No transpose", &k, &i__1, &c_b8, &a[(k + 1) * a_dim1 + 1],
		     lda, &w[k + (kw + 1) * w_dim1], ldw, &c_b9, &w[kw *
		    w_dim1 + 1], &c__1, (ftnlen)12);
	}
	kstep = 1;
	absakk = (r__1 = w[k + kw * w_dim1], dabs(r__1));
	if (k > 1) {
	    i__1 = k - 1;
	    imax = isamax_(&i__1, &w[kw * w_dim1 + 1], &c__1);
	    colmax = (r__1 = w[imax + kw * w_dim1], dabs(r__1));
	} else {
	    colmax = 0.f;
	}
	if (dmax(absakk,colmax) == 0.f) {
	    if (*info == 0) {
		*info = k;
	    }
	    kp = k;
	} else {
	    if (absakk >= alpha * colmax) {
		kp = k;
	    } else {
		scopy_(&imax, &a[imax * a_dim1 + 1], &c__1, &w[(kw - 1) *
			w_dim1 + 1], &c__1);
		i__1 = k - imax;
		scopy_(&i__1, &a[imax + (imax + 1) * a_dim1], lda, &w[imax +
			1 + (kw - 1) * w_dim1], &c__1);
		if (k < *n) {
		    i__1 = *n - k;
		    sgemv_("No transpose", &k, &i__1, &c_b8, &a[(k + 1) *
			    a_dim1 + 1], lda, &w[imax + (kw + 1) * w_dim1],
			    ldw, &c_b9, &w[(kw - 1) * w_dim1 + 1], &c__1, (
			    ftnlen)12);
		}
		i__1 = k - imax;
		jmax = imax + isamax_(&i__1, &w[imax + 1 + (kw - 1) * w_dim1],
			 &c__1);
		rowmax = (r__1 = w[jmax + (kw - 1) * w_dim1], dabs(r__1));
		if (imax > 1) {
		    i__1 = imax - 1;
		    jmax = isamax_(&i__1, &w[(kw - 1) * w_dim1 + 1], &c__1);
/* Computing MAX */
		    r__2 = rowmax, r__3 = (r__1 = w[jmax + (kw - 1) * w_dim1],
			     dabs(r__1));
		    rowmax = dmax(r__2,r__3);
		}
		if (absakk >= alpha * colmax * (colmax / rowmax)) {
		    kp = k;
		} else if ((r__1 = w[imax + (kw - 1) * w_dim1], dabs(r__1)) >=
			 alpha * rowmax) {
		    kp = imax;
		    scopy_(&k, &w[(kw - 1) * w_dim1 + 1], &c__1, &w[kw *
			    w_dim1 + 1], &c__1);
		} else {
		    kp = imax;
		    kstep = 2;
		}
	    }
	    kk = k - kstep + 1;
	    kkw = *nb + kk - *n;
	    if (kp != kk) {
		a[kp + kp * a_dim1] = a[kk + kk * a_dim1];
		i__1 = kk - 1 - kp;
		scopy_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + (kp +
			1) * a_dim1], lda);
		if (kp > 1) {
		    i__1 = kp - 1;
		    scopy_(&i__1, &a[kk * a_dim1 + 1], &c__1, &a[kp * a_dim1
			    + 1], &c__1);
		}
		if (k < *n) {
		    i__1 = *n - k;
		    sswap_(&i__1, &a[kk + (k + 1) * a_dim1], lda, &a[kp + (k
			    + 1) * a_dim1], lda);
		}
		i__1 = *n - kk + 1;
		sswap_(&i__1, &w[kk + kkw * w_dim1], ldw, &w[kp + kkw *
			w_dim1], ldw);
	    }
	    if (kstep == 1) {
		scopy_(&k, &w[kw * w_dim1 + 1], &c__1, &a[k * a_dim1 + 1], &
			c__1);
		r1 = 1.f / a[k + k * a_dim1];
		i__1 = k - 1;
		sscal_(&i__1, &r1, &a[k * a_dim1 + 1], &c__1);
	    } else {
		if (k > 2) {
		    d21 = w[k - 1 + kw * w_dim1];
		    d11 = w[k + kw * w_dim1] / d21;
		    d22 = w[k - 1 + (kw - 1) * w_dim1] / d21;
		    t = 1.f / (d11 * d22 - 1.f);
		    d21 = t / d21;
		    i__1 = k - 2;
		    for (j = 1; j <= i__1; ++j) {
			a[j + (k - 1) * a_dim1] = d21 * (d11 * w[j + (kw - 1)
				* w_dim1] - w[j + kw * w_dim1]);
			a[j + k * a_dim1] = d21 * (d22 * w[j + kw * w_dim1] -
				w[j + (kw - 1) * w_dim1]);
/* L20: */
		    }
		}
		a[k - 1 + (k - 1) * a_dim1] = w[k - 1 + (kw - 1) * w_dim1];
		a[k - 1 + k * a_dim1] = w[k - 1 + kw * w_dim1];
		a[k + k * a_dim1] = w[k + kw * w_dim1];
	    }
	}
	if (kstep == 1) {
	    ipiv[k] = kp;
	} else {
	    ipiv[k] = -kp;
	    ipiv[k - 1] = -kp;
	}
	k -= kstep;
	goto L10;
L30:
	j = k + 1;
L60:
	jj = j;
	jp = ipiv[j];
	if (jp < 0) {
	    jp = -jp;
	    ++j;
	}
	++j;
	if (jp != jj && j <= *n) {
	    i__1 = *n - j + 1;
	    sswap_(&i__1, &a[jp + j * a_dim1], lda, &a[jj + j * a_dim1], lda);
	}
	if (j < *n) {
	    goto L60;
	}
	*kb = *n - k;
    } else {
	k = 1;
L70:
	if ((k >= *nb && *nb < *n) || k > *n) {
	    goto L90;
	}
	i__1 = *n - k + 1;
	scopy_(&i__1, &a[k + k * a_dim1], &c__1, &w[k + k * w_dim1], &c__1);
	i__1 = *n - k + 1;
	i__2 = k - 1;
	sgemv_("No transpose", &i__1, &i__2, &c_b8, &a[k + a_dim1], lda, &w[k
		+ w_dim1], ldw, &c_b9, &w[k + k * w_dim1], &c__1, (ftnlen)12);
	kstep = 1;
	absakk = (r__1 = w[k + k * w_dim1], dabs(r__1));
	if (k < *n) {
	    i__1 = *n - k;
	    imax = k + isamax_(&i__1, &w[k + 1 + k * w_dim1], &c__1);
	    colmax = (r__1 = w[imax + k * w_dim1], dabs(r__1));
	} else {
	    colmax = 0.f;
	}
	if (dmax(absakk,colmax) == 0.f) {
	    if (*info == 0) {
		*info = k;
	    }
	    kp = k;
	} else {
	    if (absakk >= alpha * colmax) {
		kp = k;
	    } else {
		i__1 = imax - k;
		scopy_(&i__1, &a[imax + k * a_dim1], lda, &w[k + (k + 1) *
			w_dim1], &c__1);
		i__1 = *n - imax + 1;
		scopy_(&i__1, &a[imax + imax * a_dim1], &c__1, &w[imax + (k +
			1) * w_dim1], &c__1);
		i__1 = *n - k + 1;
		i__2 = k - 1;
		sgemv_("No transpose", &i__1, &i__2, &c_b8, &a[k + a_dim1],
			lda, &w[imax + w_dim1], ldw, &c_b9, &w[k + (k + 1) *
			w_dim1], &c__1, (ftnlen)12);
		i__1 = imax - k;
		jmax = k - 1 + isamax_(&i__1, &w[k + (k + 1) * w_dim1], &c__1)
			;
		rowmax = (r__1 = w[jmax + (k + 1) * w_dim1], dabs(r__1));
		if (imax < *n) {
		    i__1 = *n - imax;
		    jmax = imax + isamax_(&i__1, &w[imax + 1 + (k + 1) *
			    w_dim1], &c__1);
/* Computing MAX */
		    r__2 = rowmax, r__3 = (r__1 = w[jmax + (k + 1) * w_dim1],
			    dabs(r__1));
		    rowmax = dmax(r__2,r__3);
		}
		if (absakk >= alpha * colmax * (colmax / rowmax)) {
		    kp = k;
		} else if ((r__1 = w[imax + (k + 1) * w_dim1], dabs(r__1)) >=
			alpha * rowmax) {
		    kp = imax;
		    i__1 = *n - k + 1;
		    scopy_(&i__1, &w[k + (k + 1) * w_dim1], &c__1, &w[k + k *
			    w_dim1], &c__1);
		} else {
		    kp = imax;
		    kstep = 2;
		}
	    }
	    kk = k + kstep - 1;
	    if (kp != kk) {
		a[kp + kp * a_dim1] = a[kk + kk * a_dim1];
		i__1 = kp - kk - 1;
		scopy_(&i__1, &a[kk + 1 + kk * a_dim1], &c__1, &a[kp + (kk +
			1) * a_dim1], lda);
		if (kp < *n) {
		    i__1 = *n - kp;
		    scopy_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + 1
			    + kp * a_dim1], &c__1);
		}
		if (k > 1) {
		    i__1 = k - 1;
		    sswap_(&i__1, &a[kk + a_dim1], lda, &a[kp + a_dim1], lda);
		}
		sswap_(&kk, &w[kk + w_dim1], ldw, &w[kp + w_dim1], ldw);
	    }
	    if (kstep == 1) {
		i__1 = *n - k + 1;
		scopy_(&i__1, &w[k + k * w_dim1], &c__1, &a[k + k * a_dim1], &
			c__1);
		if (k < *n) {
		    r1 = 1.f / a[k + k * a_dim1];
		    i__1 = *n - k;
		    sscal_(&i__1, &r1, &a[k + 1 + k * a_dim1], &c__1);
		}
	    } else {
		if (k < *n - 1) {
		    d21 = w[k + 1 + k * w_dim1];
		    d11 = w[k + 1 + (k + 1) * w_dim1] / d21;
		    d22 = w[k + k * w_dim1] / d21;
		    t = 1.f / (d11 * d22 - 1.f);
		    d21 = t / d21;
		    i__1 = *n;
		    for (j = k + 2; j <= i__1; ++j) {
			a[j + k * a_dim1] = d21 * (d11 * w[j + k * w_dim1] -
				w[j + (k + 1) * w_dim1]);
			a[j + (k + 1) * a_dim1] = d21 * (d22 * w[j + (k + 1) *
				 w_dim1] - w[j + k * w_dim1]);
/* L80: */
		    }
		}
		a[k + k * a_dim1] = w[k + k * w_dim1];
		a[k + 1 + k * a_dim1] = w[k + 1 + k * w_dim1];
		a[k + 1 + (k + 1) * a_dim1] = w[k + 1 + (k + 1) * w_dim1];
	    }
	}
	if (kstep == 1) {
	    ipiv[k] = kp;
	} else {
	    ipiv[k] = -kp;
	    ipiv[k + 1] = -kp;
	}
	k += kstep;
	goto L70;
L90:
	j = k - 1;
L120:
	jj = j;
	jp = ipiv[j];
	if (jp < 0) {
	    jp = -jp;
	    --j;
	}
	--j;
	if (jp != jj && j >= 1) {
	    sswap_(&j, &a[jp + a_dim1], lda, &a[jj + a_dim1], lda);
	}
	if (j > 1) {
	    goto L120;
	}
	*kb = k - 1;
    }
    return;
}
