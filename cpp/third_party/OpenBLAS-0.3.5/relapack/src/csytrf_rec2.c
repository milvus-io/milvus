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

static complex c_b1 = {1.f,0.f};
static int c__1 = 1;

/** CSYTRF_REC2 computes a partial factorization of a complex symmetric matrix using the Bunch-Kaufman diagon al pivoting method.
 *
 * This routine is a minor modification of LAPACK's clasyf.
 * It serves as an unblocked kernel in the recursive algorithms.
 * The blocked BLAS Level 3 updates were removed and moved to the
 * recursive algorithm.
 * */
/* Subroutine */ void RELAPACK_csytrf_rec2(char *uplo, int *n, int *
	nb, int *kb, complex *a, int *lda, int *ipiv, complex *w,
	int *ldw, int *info, ftnlen uplo_len)
{
    /* System generated locals */
    int a_dim1, a_offset, w_dim1, w_offset, i__1, i__2, i__3, i__4;
    float r__1, r__2, r__3, r__4;
    complex q__1, q__2, q__3;

    /* Builtin functions */
    double sqrt(double), r_imag(complex *);
    void c_div(complex *, complex *, complex *);

    /* Local variables */
    static int j, k;
    static complex t, r1, d11, d21, d22;
    static int jj, kk, jp, kp, kw, kkw, imax, jmax;
    static float alpha;
    extern /* Subroutine */ int cscal_(int *, complex *, complex *,
	    int *);
    extern logical lsame_(char *, char *, ftnlen, ftnlen);
    extern /* Subroutine */ int cgemv_(char *, int *, int *, complex *
	    , complex *, int *, complex *, int *, complex *, complex *
	    , int *, ftnlen), ccopy_(int *, complex *, int *,
	    complex *, int *), cswap_(int *, complex *, int *,
	    complex *, int *);
    static int kstep;
    static float absakk;
    extern int icamax_(int *, complex *, int *);
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
	ccopy_(&k, &a[k * a_dim1 + 1], &c__1, &w[kw * w_dim1 + 1], &c__1);
	if (k < *n) {
	    i__1 = *n - k;
	    q__1.r = -1.f, q__1.i = -0.f;
	    cgemv_("No transpose", &k, &i__1, &q__1, &a[(k + 1) * a_dim1 + 1],
		     lda, &w[k + (kw + 1) * w_dim1], ldw, &c_b1, &w[kw *
		    w_dim1 + 1], &c__1, (ftnlen)12);
	}
	kstep = 1;
	i__1 = k + kw * w_dim1;
	absakk = (r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[k + kw *
		w_dim1]), dabs(r__2));
	if (k > 1) {
	    i__1 = k - 1;
	    imax = icamax_(&i__1, &w[kw * w_dim1 + 1], &c__1);
	    i__1 = imax + kw * w_dim1;
	    colmax = (r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[imax
		    + kw * w_dim1]), dabs(r__2));
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
		ccopy_(&imax, &a[imax * a_dim1 + 1], &c__1, &w[(kw - 1) *
			w_dim1 + 1], &c__1);
		i__1 = k - imax;
		ccopy_(&i__1, &a[imax + (imax + 1) * a_dim1], lda, &w[imax +
			1 + (kw - 1) * w_dim1], &c__1);
		if (k < *n) {
		    i__1 = *n - k;
		    q__1.r = -1.f, q__1.i = -0.f;
		    cgemv_("No transpose", &k, &i__1, &q__1, &a[(k + 1) *
			    a_dim1 + 1], lda, &w[imax + (kw + 1) * w_dim1],
			    ldw, &c_b1, &w[(kw - 1) * w_dim1 + 1], &c__1, (
			    ftnlen)12);
		}
		i__1 = k - imax;
		jmax = imax + icamax_(&i__1, &w[imax + 1 + (kw - 1) * w_dim1],
			 &c__1);
		i__1 = jmax + (kw - 1) * w_dim1;
		rowmax = (r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[
			jmax + (kw - 1) * w_dim1]), dabs(r__2));
		if (imax > 1) {
		    i__1 = imax - 1;
		    jmax = icamax_(&i__1, &w[(kw - 1) * w_dim1 + 1], &c__1);
/* Computing MAX */
		    i__1 = jmax + (kw - 1) * w_dim1;
		    r__3 = rowmax, r__4 = (r__1 = w[i__1].r, dabs(r__1)) + (
			    r__2 = r_imag(&w[jmax + (kw - 1) * w_dim1]), dabs(
			    r__2));
		    rowmax = dmax(r__3,r__4);
		}
		if (absakk >= alpha * colmax * (colmax / rowmax)) {
		    kp = k;
		} else /* if(complicated condition) */ {
		    i__1 = imax + (kw - 1) * w_dim1;
		    if ((r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[
			    imax + (kw - 1) * w_dim1]), dabs(r__2)) >= alpha *
			     rowmax) {
			kp = imax;
			ccopy_(&k, &w[(kw - 1) * w_dim1 + 1], &c__1, &w[kw *
				w_dim1 + 1], &c__1);
		    } else {
			kp = imax;
			kstep = 2;
		    }
		}
	    }
	    kk = k - kstep + 1;
	    kkw = *nb + kk - *n;
	    if (kp != kk) {
		i__1 = kp + kp * a_dim1;
		i__2 = kk + kk * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kk - 1 - kp;
		ccopy_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + (kp +
			1) * a_dim1], lda);
		if (kp > 1) {
		    i__1 = kp - 1;
		    ccopy_(&i__1, &a[kk * a_dim1 + 1], &c__1, &a[kp * a_dim1
			    + 1], &c__1);
		}
		if (k < *n) {
		    i__1 = *n - k;
		    cswap_(&i__1, &a[kk + (k + 1) * a_dim1], lda, &a[kp + (k
			    + 1) * a_dim1], lda);
		}
		i__1 = *n - kk + 1;
		cswap_(&i__1, &w[kk + kkw * w_dim1], ldw, &w[kp + kkw *
			w_dim1], ldw);
	    }
	    if (kstep == 1) {
		ccopy_(&k, &w[kw * w_dim1 + 1], &c__1, &a[k * a_dim1 + 1], &
			c__1);
		c_div(&q__1, &c_b1, &a[k + k * a_dim1]);
		r1.r = q__1.r, r1.i = q__1.i;
		i__1 = k - 1;
		cscal_(&i__1, &r1, &a[k * a_dim1 + 1], &c__1);
	    } else {
		if (k > 2) {
		    i__1 = k - 1 + kw * w_dim1;
		    d21.r = w[i__1].r, d21.i = w[i__1].i;
		    c_div(&q__1, &w[k + kw * w_dim1], &d21);
		    d11.r = q__1.r, d11.i = q__1.i;
		    c_div(&q__1, &w[k - 1 + (kw - 1) * w_dim1], &d21);
		    d22.r = q__1.r, d22.i = q__1.i;
		    q__3.r = d11.r * d22.r - d11.i * d22.i, q__3.i = d11.r *
			    d22.i + d11.i * d22.r;
		    q__2.r = q__3.r - 1.f, q__2.i = q__3.i - 0.f;
		    c_div(&q__1, &c_b1, &q__2);
		    t.r = q__1.r, t.i = q__1.i;
		    c_div(&q__1, &t, &d21);
		    d21.r = q__1.r, d21.i = q__1.i;
		    i__1 = k - 2;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = j + (k - 1) * a_dim1;
			i__3 = j + (kw - 1) * w_dim1;
			q__3.r = d11.r * w[i__3].r - d11.i * w[i__3].i,
				q__3.i = d11.r * w[i__3].i + d11.i * w[i__3]
				.r;
			i__4 = j + kw * w_dim1;
			q__2.r = q__3.r - w[i__4].r, q__2.i = q__3.i - w[i__4]
				.i;
			q__1.r = d21.r * q__2.r - d21.i * q__2.i, q__1.i =
				d21.r * q__2.i + d21.i * q__2.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
			i__2 = j + k * a_dim1;
			i__3 = j + kw * w_dim1;
			q__3.r = d22.r * w[i__3].r - d22.i * w[i__3].i,
				q__3.i = d22.r * w[i__3].i + d22.i * w[i__3]
				.r;
			i__4 = j + (kw - 1) * w_dim1;
			q__2.r = q__3.r - w[i__4].r, q__2.i = q__3.i - w[i__4]
				.i;
			q__1.r = d21.r * q__2.r - d21.i * q__2.i, q__1.i =
				d21.r * q__2.i + d21.i * q__2.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/* L20: */
		    }
		}
		i__1 = k - 1 + (k - 1) * a_dim1;
		i__2 = k - 1 + (kw - 1) * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k - 1 + k * a_dim1;
		i__2 = k - 1 + kw * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k + k * a_dim1;
		i__2 = k + kw * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
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
	    cswap_(&i__1, &a[jp + j * a_dim1], lda, &a[jj + j * a_dim1], lda);
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
	ccopy_(&i__1, &a[k + k * a_dim1], &c__1, &w[k + k * w_dim1], &c__1);
	i__1 = *n - k + 1;
	i__2 = k - 1;
	q__1.r = -1.f, q__1.i = -0.f;
	cgemv_("No transpose", &i__1, &i__2, &q__1, &a[k + a_dim1], lda, &w[k
		+ w_dim1], ldw, &c_b1, &w[k + k * w_dim1], &c__1, (ftnlen)12);
	kstep = 1;
	i__1 = k + k * w_dim1;
	absakk = (r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[k + k *
		w_dim1]), dabs(r__2));
	if (k < *n) {
	    i__1 = *n - k;
	    imax = k + icamax_(&i__1, &w[k + 1 + k * w_dim1], &c__1);
	    i__1 = imax + k * w_dim1;
	    colmax = (r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[imax
		    + k * w_dim1]), dabs(r__2));
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
		ccopy_(&i__1, &a[imax + k * a_dim1], lda, &w[k + (k + 1) *
			w_dim1], &c__1);
		i__1 = *n - imax + 1;
		ccopy_(&i__1, &a[imax + imax * a_dim1], &c__1, &w[imax + (k +
			1) * w_dim1], &c__1);
		i__1 = *n - k + 1;
		i__2 = k - 1;
		q__1.r = -1.f, q__1.i = -0.f;
		cgemv_("No transpose", &i__1, &i__2, &q__1, &a[k + a_dim1],
			lda, &w[imax + w_dim1], ldw, &c_b1, &w[k + (k + 1) *
			w_dim1], &c__1, (ftnlen)12);
		i__1 = imax - k;
		jmax = k - 1 + icamax_(&i__1, &w[k + (k + 1) * w_dim1], &c__1)
			;
		i__1 = jmax + (k + 1) * w_dim1;
		rowmax = (r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[
			jmax + (k + 1) * w_dim1]), dabs(r__2));
		if (imax < *n) {
		    i__1 = *n - imax;
		    jmax = imax + icamax_(&i__1, &w[imax + 1 + (k + 1) *
			    w_dim1], &c__1);
/* Computing MAX */
		    i__1 = jmax + (k + 1) * w_dim1;
		    r__3 = rowmax, r__4 = (r__1 = w[i__1].r, dabs(r__1)) + (
			    r__2 = r_imag(&w[jmax + (k + 1) * w_dim1]), dabs(
			    r__2));
		    rowmax = dmax(r__3,r__4);
		}
		if (absakk >= alpha * colmax * (colmax / rowmax)) {
		    kp = k;
		} else /* if(complicated condition) */ {
		    i__1 = imax + (k + 1) * w_dim1;
		    if ((r__1 = w[i__1].r, dabs(r__1)) + (r__2 = r_imag(&w[
			    imax + (k + 1) * w_dim1]), dabs(r__2)) >= alpha *
			    rowmax) {
			kp = imax;
			i__1 = *n - k + 1;
			ccopy_(&i__1, &w[k + (k + 1) * w_dim1], &c__1, &w[k +
				k * w_dim1], &c__1);
		    } else {
			kp = imax;
			kstep = 2;
		    }
		}
	    }
	    kk = k + kstep - 1;
	    if (kp != kk) {
		i__1 = kp + kp * a_dim1;
		i__2 = kk + kk * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp - kk - 1;
		ccopy_(&i__1, &a[kk + 1 + kk * a_dim1], &c__1, &a[kp + (kk +
			1) * a_dim1], lda);
		if (kp < *n) {
		    i__1 = *n - kp;
		    ccopy_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + 1
			    + kp * a_dim1], &c__1);
		}
		if (k > 1) {
		    i__1 = k - 1;
		    cswap_(&i__1, &a[kk + a_dim1], lda, &a[kp + a_dim1], lda);
		}
		cswap_(&kk, &w[kk + w_dim1], ldw, &w[kp + w_dim1], ldw);
	    }
	    if (kstep == 1) {
		i__1 = *n - k + 1;
		ccopy_(&i__1, &w[k + k * w_dim1], &c__1, &a[k + k * a_dim1], &
			c__1);
		if (k < *n) {
		    c_div(&q__1, &c_b1, &a[k + k * a_dim1]);
		    r1.r = q__1.r, r1.i = q__1.i;
		    i__1 = *n - k;
		    cscal_(&i__1, &r1, &a[k + 1 + k * a_dim1], &c__1);
		}
	    } else {
		if (k < *n - 1) {
		    i__1 = k + 1 + k * w_dim1;
		    d21.r = w[i__1].r, d21.i = w[i__1].i;
		    c_div(&q__1, &w[k + 1 + (k + 1) * w_dim1], &d21);
		    d11.r = q__1.r, d11.i = q__1.i;
		    c_div(&q__1, &w[k + k * w_dim1], &d21);
		    d22.r = q__1.r, d22.i = q__1.i;
		    q__3.r = d11.r * d22.r - d11.i * d22.i, q__3.i = d11.r *
			    d22.i + d11.i * d22.r;
		    q__2.r = q__3.r - 1.f, q__2.i = q__3.i - 0.f;
		    c_div(&q__1, &c_b1, &q__2);
		    t.r = q__1.r, t.i = q__1.i;
		    c_div(&q__1, &t, &d21);
		    d21.r = q__1.r, d21.i = q__1.i;
		    i__1 = *n;
		    for (j = k + 2; j <= i__1; ++j) {
			i__2 = j + k * a_dim1;
			i__3 = j + k * w_dim1;
			q__3.r = d11.r * w[i__3].r - d11.i * w[i__3].i,
				q__3.i = d11.r * w[i__3].i + d11.i * w[i__3]
				.r;
			i__4 = j + (k + 1) * w_dim1;
			q__2.r = q__3.r - w[i__4].r, q__2.i = q__3.i - w[i__4]
				.i;
			q__1.r = d21.r * q__2.r - d21.i * q__2.i, q__1.i =
				d21.r * q__2.i + d21.i * q__2.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
			i__2 = j + (k + 1) * a_dim1;
			i__3 = j + (k + 1) * w_dim1;
			q__3.r = d22.r * w[i__3].r - d22.i * w[i__3].i,
				q__3.i = d22.r * w[i__3].i + d22.i * w[i__3]
				.r;
			i__4 = j + k * w_dim1;
			q__2.r = q__3.r - w[i__4].r, q__2.i = q__3.i - w[i__4]
				.i;
			q__1.r = d21.r * q__2.r - d21.i * q__2.i, q__1.i =
				d21.r * q__2.i + d21.i * q__2.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/* L80: */
		    }
		}
		i__1 = k + k * a_dim1;
		i__2 = k + k * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k + 1 + k * a_dim1;
		i__2 = k + 1 + k * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k + 1 + (k + 1) * a_dim1;
		i__2 = k + 1 + (k + 1) * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
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
	    cswap_(&j, &a[jp + a_dim1], lda, &a[jj + a_dim1], lda);
	}
	if (j > 1) {
	    goto L120;
	}
	*kb = k - 1;
    }
    return;
}
