/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#define ASSEMBLER
#include "common.h"

#define N	r32
#define X1	r33
#define INCX	r34
#define Y1	r35
#define INCY	r36

#define PREA	r2
#define PREB	r3

#define I	r14
#define J	r15

#define X2	r16
#define Y2	r17
#define INCXM1	r20
#define INCYM1	r21
#define INCX3M1	r22
#define INCY3M1	r23
#define INCX8	r24
#define INCY8	r25
#define XX	r26
#define YY	r27
#define XA	r28
#define YA	r29
#define PR	r30
#define ARLC	r31

#ifdef DOUBLE
#define PREFETCH_SIZE (6 * 32)
#else
#define PREFETCH_SIZE (8 * 64)
#endif

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	shladd	INCX = INCX, ZBASE_SHIFT, r0
	shladd	INCY = INCY, ZBASE_SHIFT, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	sub	XA = Y1, X1
	(p6) br.ret.sptk.many b0
	}
	;;
	.body
	{ .mmi
	shladd	INCX3M1  = INCX, 1, INCX
	shladd	INCY3M1  = INCY, 1, INCY
	mov	PR = pr
	}
	{ .mmi
	adds	INCXM1  = - SIZE, INCX
	adds	INCYM1  = - SIZE, INCY
	shr.u  XA = XA, BASE_SHIFT
	}
	;;
	{ .mmi
#ifdef DOUBLE
	adds	XA = 4, XA
#else
	adds	XA = -2, XA
#endif
	and	J = 7, N
	mov	pr.rot = 0
	}
	{ .mmi
	adds	INCX3M1 = - SIZE, INCX3M1
	adds	INCY3M1 = - SIZE, INCY3M1
	shr	I = N, 3
	}
	;;
	{ .mmi
#ifdef DOUBLE
	and	XA = 31, XA
#else
	and	XA = 63, XA
#endif
	cmp.eq	p9, p0  =   r0, J
	tbit.z	p0, p7 = X1, BASE_SHIFT
	}
	{ .mmi
	shladd	X2 = INCX,  1, X1
	shladd	Y2 = INCY,  1, Y1
	tbit.z	p0, p12 = N, 2
	}
	;;
	{ .mmi
	cmp.eq	p8 ,p0  =   r0, I
	adds	I = -1, I
#ifdef DOUBLE
	cmp.le	p11, p0 = 15, XA
#else
	cmp.ge	p11, p0 = 31, XA
#endif
	}
	{ .mmb
	shladd	INCX8 = INCX, 3, r0
	shladd	INCY8 = INCY, 3, r0
	(p8) br.cond.dpnt  .L25
	}
	;;
	{ .mmi
	nop.m 0
	nop.m 0
	mov	ar.lc = I
	}
	{ .mbb
	(p7) br.cond.dpnt .L100
	(p11) br.cond.dpnt  .L30
	}
	;;
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	nop.m 0
	mov	ar.ec = 5
	}
	{ .mmi
	adds	PREA  = PREFETCH_SIZE * SIZE + 32, X1
#ifndef DOUBLE
	adds	PREB  = PREFETCH_SIZE * SIZE +  0, Y1
#else
	adds	PREB  = PREFETCH_SIZE * SIZE - 40, Y1
#endif
	nop.i 0
	}
	;;
	.align 32

.L21:
	{ .mmi
	(p21) STFD	[Y1] = f42
	(p21) STFD	[Y2] = f62
	(p21) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f32, f37  = [X1]
	(p16) add	X1 = X1, INCX
	(p21) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f47,  1 * SIZE
	(p21) STFD	[Y2] = f67,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX8
	(p16) LDFPD	f42, f47  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f52
	(p21) STFD	[Y2] = f72
	(p21) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f52, f57  = [X1]
	(p16) add	X1 = X1, INCX
	(p21) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f77,  1 * SIZE
	(p21) STFD	[Y2] = f97,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY8
	(p16) LDFPD	f62, f67  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f82
	(p21) STFD	[Y2] = f102
	(p21) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f72, f77  = [X1]
	(p16) add	X1 = X1, INCX
	(p21) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f87,  1 * SIZE
	(p21) STFD	[Y2] = f107, 1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f82, f87  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f92
	(p21) STFD	[Y2] = f112
	(p21) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f92, f97  = [X1]
	(p16) add	X1 = X1, INCX
	(p21) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f36,  1 * SIZE
	(p20) STFD	[Y2] = f56,  1 * SIZE
	(p16) shladd	X2 = INCX, 3, X2
	}
	{ .mmb
	(p16) LDFPD	f102, f107  = [X1]
	(p16) add	X1 = X1, INCX
	br.ctop.sptk.few .L21
	}
	;;

	{ .mmi
	(p21) STFD	[Y1] = f42
	(p21) STFD	[Y2] = f62
	(p21) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p21) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f47,  1 * SIZE
	(p21) STFD	[Y2] = f67,  1 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f52
	(p21) STFD	[Y2] = f72
	(p21) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p21) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f77,  1 * SIZE
	(p21) STFD	[Y2] = f97,  1 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f82
	(p21) STFD	[Y2] = f102
	(p21) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p21) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f87,  1 * SIZE
	(p21) STFD	[Y2] = f107, 1 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f92
	(p21) STFD	[Y2] = f112
	(p21) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p21) add   Y2 = INCY3M1, Y2
	}
	;;
	.align 32

.L25:
	{ .mmi
	mov	XX = X1
	nop.m 0
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) LDFD	f48 = [X1], 1 * SIZE
	(p12) LDFD	f52 = [X2], 1 * SIZE
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f49 = [X1], INCXM1
	(p12) LDFD	f53 = [X2], INCXM1
	mov	pr = PR, -65474
	}
	{ .mib
	nop.m 0
	tbit.z	p0, p14 = N, 0
	(p9)  br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], 1 * SIZE
	(p12) LDFD	f54 = [X2], 1 * SIZE
	(p12) shladd XX = INCX, 2, XX;;
	}
	;;
	{ .mmi
	(p12) LDFD	f51 = [X1], INCX3M1
	(p12) LDFD	f55 = [X2], INCX3M1
	(p13) shladd XX = INCX, 1, XX;;
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], 1 * SIZE
	(p14) LDFD	f60 = [XX], 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f57 = [X1], INCXM1
	(p14) LDFD	f61 = [XX]
	mov	YY = Y1
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48, 1 * SIZE
	(p12) STFD	[Y2] = f52, 1 * SIZE
	}
	{ .mmi
	(p13) LDFD	f58 = [X1], 1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCYM1, Y1
	}
	{ .mmi
	(p13) LDFD	f59 = [X1]
	(p12) add Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50, 1 * SIZE
	(p12) STFD	[Y2] = f54, 1 * SIZE
	(p12) shladd YY = INCY, 2, YY;;
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
	(p12) STFD	[Y2] = f55
	(p13) shladd YY = INCY, 1, YY
	}
	{ .mmi
	(p12) add Y1 = INCY3M1, Y1
	(p12) add Y2 = INCY3M1, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56, 1 * SIZE
	(p14) STFD	[YY] = f60, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f61
	(p13) add Y1 = INCYM1, Y1
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58, 1 * SIZE
	nop.m 0
	nop.i 0
	}
	;;
	{ .mib
	(p13) STFD	[Y1] = f59
	nop.i 0
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L30:
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	nop.m 0
	mov	ar.ec = 5
	}
	{ .mmi
#ifndef DOUBLE
	adds	PREA  = PREFETCH_SIZE * SIZE + 24, X1
	adds	PREB  = PREFETCH_SIZE * SIZE + 40, Y1
#else
	adds	PREA  = PREFETCH_SIZE * SIZE - 56, X1
	adds	PREB  = PREFETCH_SIZE * SIZE - 24, Y1
#endif
	nop.i 0
	}
	;;
	.align 32

#ifndef DOUBLE
.L31:
	{ .mmi
	(p20) STFD	[Y1] = f91
	(p20) STFD	[Y2] = f111
	(p20) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f32, f37  = [X1]
	(p16) add	X1 = X1, INCX
	(p20) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f35,  1 * SIZE
	(p19) STFD	[Y2] = f55,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX8
	(p16) LDFPD	f42, f47  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f40
	(p19) STFD	[Y2] = f60
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f52, f57  = [X1]
	(p16) add	X1 = X1, INCX
	(p19) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f45,  1 * SIZE
	(p19) STFD	[Y2] = f65,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY8
	(p16) LDFPD	f62, f67  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f50
	(p19) STFD	[Y2] = f70
	(p19) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f72, f77  = [X1]
	(p16) add	X1 = X1, INCX
	(p19) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f75,  1 * SIZE
	(p19) STFD	[Y2] = f95,  1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f82, f87  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f80
	(p19) STFD	[Y2] = f100
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f92, f97  = [X1]
	(p16) add	X1 = X1, INCX
	(p19) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f85,  1 * SIZE
	(p19) STFD	[Y2] = f105, 1 * SIZE
	(p16) shladd	X2 = INCX, 3, X2
	}
	{ .mmb
	(p16) LDFPD	f102, f107  = [X1]
	(p16) add	X1 = X1, INCX
	br.ctop.sptk.few .L31
	}
	;;
	br	.L25
	.align 32

#else
.L31:
	{ .mmi
	(p20) STFD	[Y1] = f41
	(p20) STFD	[Y2] = f61
	(p20) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f32, f37  = [X1]
	(p16) add	X1 = X1, INCX
	(p20) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f46,  1 * SIZE
	(p20) STFD	[Y2] = f66,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX8
	(p16) LDFPD	f42, f47  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f51
	(p20) STFD	[Y2] = f71
	(p20) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f52, f57  = [X1]
	(p16) add	X1 = X1, INCX
	(p20) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f76,  1 * SIZE
	(p20) STFD	[Y2] = f96,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY8
	(p16) LDFPD	f62, f67  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f81
	(p20) STFD	[Y2] = f101
	(p20) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f72, f77  = [X1]
	(p16) add	X1 = X1, INCX
	(p20) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f86,  1 * SIZE
	(p20) STFD	[Y2] = f106, 1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f82, f87  = [X1]
	(p16) add	X1 = X1, INCX
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f91
	(p20) STFD	[Y2] = f111
	(p20) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f92, f97  = [X1]
	(p16) add	X1 = X1, INCX
	(p20) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f35,  1 * SIZE
	(p19) STFD	[Y2] = f55,  1 * SIZE
	(p16) shladd	X2 = INCX, 3, X2
	}
	{ .mmb
	(p16) LDFPD	f102, f107  = [X1]
	(p16) add	X1 = X1, INCX
	br.ctop.sptk.few .L31
	}
	;;
	br	.L25
	.align 32
#endif

.L100:
	{ .mmi
	mov	ar.lc = I
	}
	{ .mbb
	cmp.ne	p6, p0 = 2 * SIZE, INCX
	(p6)  br.cond.dpnt .L200
	(p11) br.cond.dpnt .L130
	}
	;;
	{ .mmi
	adds	PREA  = PREFETCH_SIZE * SIZE + 32, X1
#ifndef DOUBLE
	adds	PREB  = PREFETCH_SIZE * SIZE - 32, Y1
#else
	adds	PREB  = PREFETCH_SIZE * SIZE + 72, Y1
#endif
	mov	ar.ec = 5
	}
	{ .mmi
	LDFD	f32  = [X1], 1 * SIZE
	cmp.eq	p16, p0 = r0, r0
	nop.i 0
	}
	;;
	.align 32

.L121:
	{ .mmi
	(p21) STFD	[Y1] = f47,  1 * SIZE
	(p21) STFD	[Y2] = f67,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX8
	(p16) LDFPD	f37, f42  = [X1], 2 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f52
	(p21) STFD	[Y2] = f72
	(p21) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY8
	(p16) LDFPD	f47, f52  = [X1], 2 * SIZE
	(p21) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f77,  1 * SIZE
	(p21) STFD	[Y2] = f97,  1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f57, f62  = [X1], 2 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f82
	(p21) STFD	[Y2] = f102
	(p21) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f67, f72  = [X1], 2 * SIZE
	(p21) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f87,  1 * SIZE
	(p21) STFD	[Y2] = f107, 1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f77, f82  = [X1], 2 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f92
	(p21) STFD	[Y2] = f113
	(p21) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f87, f92  = [X1], 2 * SIZE
	(p21) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f36,  1 * SIZE
	(p20) STFD	[Y2] = f56,  1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f97, f102  = [X1], 2 * SIZE
	(p16) shladd	X2 = INCX, 3, X2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f41
	(p20) STFD	[Y2] = f61
	(p20) add   Y1 = INCYM1, Y1
	}
	{ .mmb
	(p16) LDFPD	f108, f127  = [X1], 2 * SIZE
	(p20) add   Y2 = INCYM1, Y2
	br.ctop.sptk.few .L121
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f47,  1 * SIZE
	(p21) STFD	[Y2] = f67,  1 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f52
	(p21) STFD	[Y2] = f72
	(p21) add   Y1 = INCY3M1, Y1
	}
	(p21) add   Y2 = INCY3M1, Y2
	;;
	{ .mmi
	(p21) STFD	[Y1] = f77,  1 * SIZE
	(p21) STFD	[Y2] = f97,  1 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f82
	(p21) STFD	[Y2] = f102
	(p21) add   Y1 = INCYM1, Y1
	}
	(p21) add   Y2 = INCYM1, Y2
	;;
	{ .mmi
	(p21) STFD	[Y1] = f87,  1 * SIZE
	(p21) STFD	[Y2] = f107, 1 * SIZE
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f92
	(p21) STFD	[Y2] = f113
	(p21) add   Y1 = INCY3M1, Y1
	}
	(p21) add   Y2 = INCY3M1, Y2

	adds	X1 = -SIZE, X1
	;;
	.align 32

.L125:
	{ .mmi
	mov	XX = X1
	nop.m 0
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) LDFD	f48 = [X1], 1 * SIZE
	(p12) LDFD	f52 = [X2], 1 * SIZE
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f49 = [X1], INCXM1
	(p12) LDFD	f53 = [X2], INCXM1
	mov	pr = PR, -65474
	}
	{ .mib
	nop.m 0
	tbit.z	p0, p14 = N, 0
	(p9)  br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], 1 * SIZE
	(p12) LDFD	f54 = [X2], 1 * SIZE
	(p12) shladd XX = INCX, 2, XX;;
	}
	;;
	{ .mmi
	(p12) LDFD	f51 = [X1], INCX3M1
	(p12) LDFD	f55 = [X2], INCX3M1
	(p13) shladd XX = INCX, 1, XX;;
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], 1 * SIZE
	(p14) LDFD	f60 = [XX], 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f57 = [X1], INCXM1
	(p14) LDFD	f61 = [XX]
	mov	YY = Y1
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48, 1 * SIZE
	(p12) STFD	[Y2] = f52, 1 * SIZE
	}
	{ .mmi
	(p13) LDFD	f58 = [X1], 1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCYM1, Y1
	}
	{ .mmi
	(p13) LDFD	f59 = [X1]
	(p12) add Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50, 1 * SIZE
	(p12) STFD	[Y2] = f54, 1 * SIZE
	(p12) shladd YY = INCY, 2, YY;;
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
	(p12) STFD	[Y2] = f55
	(p13) shladd YY = INCY, 1, YY
	}
	{ .mmi
	(p12) add Y1 = INCY3M1, Y1
	(p12) add Y2 = INCY3M1, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56, 1 * SIZE
	(p14) STFD	[YY] = f60, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f61
	(p13) add Y1 = INCYM1, Y1
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58, 1 * SIZE
	nop.m 0
	nop.i 0
	}
	;;
	{ .mib
	(p13) STFD	[Y1] = f59
	nop.i 0
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L130:
	{ .mmi
	adds	PREA  = PREFETCH_SIZE * SIZE + 32, X1
#ifndef DOUBLE
	adds	PREB  = PREFETCH_SIZE * SIZE + 72, Y1
#else
	adds	PREB  = PREFETCH_SIZE * SIZE + 56, Y1
#endif
	mov	ar.ec = 5
	}
	{ .mmi
	LDFD	f32  = [X1], 1 * SIZE
	cmp.eq	p16, p0 = r0, r0
	nop.i 0
	}
	;;
#ifndef DOUBLE
.L131:
	{ .mmi
	(p19) STFD	[Y1] = f35,  1 * SIZE
	(p19) STFD	[Y2] = f55,  1 * SIZE
	nop.i 0
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX8
	(p16) LDFPD	f37, f42  = [X1], 2 * SIZE
	nop.i 0
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f40
	(p19) STFD	[Y2] = f60
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY8
	(p16) LDFPD	f47, f52  = [X1], 2 * SIZE
	(p19) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f45,  1 * SIZE
	(p19) STFD	[Y2] = f65,  1 * SIZE
	nop.i 0
	}
	{ .mmi
	(p16) LDFPD	f57, f62  = [X1], 2 * SIZE
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f50
	(p19) STFD	[Y2] = f70
	(p19) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f67, f72  = [X1], 2 * SIZE
	(p19) add   Y2 = INCY3M1, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f75,  1 * SIZE
	(p19) STFD	[Y2] = f95,  1 * SIZE
	nop.i 0
	}
	{ .mmi
	(p16) LDFPD	f77, f82  = [X1], 2 * SIZE
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f80
	(p19) STFD	[Y2] = f100
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f87, f92  = [X1], 2 * SIZE
	(p19) add   Y2 = INCYM1, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f85,  1 * SIZE
	(p19) STFD	[Y2] = f105, 1 * SIZE
	nop.i 0
	}
	{ .mmi
	(p16) LDFPD	f97, f102  = [X1], 2 * SIZE
	(p16) shladd	X2 = INCX, 3, X2
	nop.i 0
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f90
	(p19) STFD	[Y2] = f111
	(p19) add   Y1 = INCY3M1, Y1
	}
	{ .mmb
	(p16) LDFPD	f108, f127  = [X1], 2 * SIZE
	(p19) add   Y2 = INCY3M1, Y2
	br.ctop.sptk.few .L131
	}
	;;
	{ .mmi
	adds	X1 = -SIZE, X1
	nop.m 0
	nop.i 0
	}
	;;
	.align 32
#else
.L131:
	{ .mmi
	(p20) STFD	[Y1] = f46,  1 * SIZE
	(p20) STFD	[Y2] = f66,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX8
	(p16) LDFPD	f37, f42  = [X1], 2 * SIZE
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f51
	(p20) STFD	[Y2] = f71
	(p20) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY8
	(p16) LDFPD	f47, f52  = [X1], 2 * SIZE
	(p20) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f76,  1 * SIZE
	(p20) STFD	[Y2] = f96,  1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f57, f62  = [X1], 2 * SIZE
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f81
	(p20) STFD	[Y2] = f101
	(p20) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFPD	f67, f72  = [X1], 2 * SIZE
	(p20) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f86,  1 * SIZE
	(p20) STFD	[Y2] = f106, 1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f77, f82  = [X1], 2 * SIZE
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f91
	(p20) STFD	[Y2] = f112
	(p20) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFPD	f87, f92  = [X1], 2 * SIZE
	(p20) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f35,  1 * SIZE
	(p19) STFD	[Y2] = f55,  1 * SIZE
	}
	{ .mmi
	(p16) LDFPD	f97, f102  = [X1], 2 * SIZE
	(p16) shladd	X2 = INCX, 3, X2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f40
	(p19) STFD	[Y2] = f60
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmb
	(p16) LDFPD	f108, f127  = [X1], 2 * SIZE
	(p19) add   Y2 = INCYM1, Y2
	br.ctop.sptk.few .L131
	}
	;;
	{ .mmi
	adds	X1 = -SIZE, X1
	nop.m 0
	nop.i 0
	}
	;;
	.align 32
#endif

.L135:
	{ .mmi
	mov	XX = X1
	nop.m 0
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) LDFD	f48 = [X1], 1 * SIZE
	(p12) LDFD	f52 = [X2], 1 * SIZE
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f49 = [X1], INCXM1
	(p12) LDFD	f53 = [X2], INCXM1
	mov	pr = PR, -65474
	}
	{ .mib
	nop.m 0
	tbit.z	p0, p14 = N, 0
	(p9)  br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], 1 * SIZE
	(p12) LDFD	f54 = [X2], 1 * SIZE
	(p12) shladd XX = INCX, 2, XX;;
	}
	;;
	{ .mmi
	(p12) LDFD	f51 = [X1], INCX3M1
	(p12) LDFD	f55 = [X2], INCX3M1
	(p13) shladd XX = INCX, 1, XX;;
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], 1 * SIZE
	(p14) LDFD	f60 = [XX], 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f57 = [X1], INCXM1
	(p14) LDFD	f61 = [XX]
	mov	YY = Y1
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48, 1 * SIZE
	(p12) STFD	[Y2] = f52, 1 * SIZE
	}
	{ .mmi
	(p13) LDFD	f58 = [X1], 1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCYM1, Y1
	}
	{ .mmi
	(p13) LDFD	f59 = [X1]
	(p12) add Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50, 1 * SIZE
	(p12) STFD	[Y2] = f54, 1 * SIZE
	(p12) shladd YY = INCY, 2, YY;;
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
	(p12) STFD	[Y2] = f55
	(p13) shladd YY = INCY, 1, YY
	}
	{ .mmi
	(p12) add Y1 = INCY3M1, Y1
	(p12) add Y2 = INCY3M1, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56, 1 * SIZE
	(p14) STFD	[YY] = f60, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f61
	(p13) add Y1 = INCYM1, Y1
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58, 1 * SIZE
	nop.m 0
	nop.i 0
	}
	;;
	{ .mib
	(p13) STFD	[Y1] = f59
	nop.i 0
	br.ret.sptk.many b0
	}
	;;

/* Unaligned Copy INCX =! 1 */
.L200:
	;;
	{ .mmi
	adds	PREA  = PREFETCH_SIZE * SIZE + 32, X1
	adds	PREB  = PREFETCH_SIZE * SIZE + 32, Y1
	mov	ar.ec = 5
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	nop.m 0
	nop.i 0
	}
	;;
	.align 32

.L221:
	{ .mmi
	(p20) STFD	[Y1] = f91
	(p20) STFD	[Y2] = f111
	(p20) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFD	f32  = [X1], 1 * SIZE
	(p16) LDFD	f52  = [X2], 1 * SIZE
	(p20) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f35,  1 * SIZE
	(p19) STFD	[Y2] = f55,  1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f37  = [X1], INCXM1
	(p16) LDFD	f57  = [X2], INCXM1
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f40
	(p19) STFD	[Y2] = f60
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFD	f42  = [X1], 1 * SIZE
	(p16) LDFD	f62  = [X2], 1 * SIZE
	(p19) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f45,  1 * SIZE
	(p19) STFD	[Y2] = f65,  1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f47  = [X1], INCX3M1
	(p16) LDFD	f67  = [X2], INCX3M1
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f50
	(p19) STFD	[Y2] = f70
	(p19) add   Y1 = INCY3M1, Y1
	}
	{ .mmi
	(p16) LDFD	f72  = [X1], 1 * SIZE
	(p16) LDFD	f92  = [X2], 1 * SIZE
	(p19) add   Y2 = INCY3M1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f75,  1 * SIZE
	(p19) STFD	[Y2] = f95,  1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f77  = [X1], INCXM1
	(p16) LDFD	f97  = [X2], INCXM1
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f80
	(p19) STFD	[Y2] = f100
	(p19) add   Y1 = INCYM1, Y1
	}
	{ .mmi
	(p16) LDFD	f82  = [X1], 1 * SIZE
	(p16) LDFD	f102 = [X2], 1 * SIZE
	(p19) add   Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f85,  1 * SIZE
	(p19) STFD	[Y2] = f105, 1 * SIZE
	}
	{ .mmb
	(p16) LDFD	f87  = [X1], INCX3M1
	(p16) LDFD	f107 = [X2], INCX3M1
	br.ctop.sptk.few .L221
	}
	;;
	.align 32

.L225:
	{ .mmi
	mov	XX = X1
	nop.m 0
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) LDFD	f48 = [X1], 1 * SIZE
	(p12) LDFD	f52 = [X2], 1 * SIZE
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f49 = [X1], INCXM1
	(p12) LDFD	f53 = [X2], INCXM1
	mov	pr = PR, -65474
	}
	{ .mib
	nop.m 0
	tbit.z	p0, p14 = N, 0
	(p9)  br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], 1 * SIZE
	(p12) LDFD	f54 = [X2], 1 * SIZE
	(p12) shladd XX = INCX, 2, XX;;
	}
	;;
	{ .mmi
	(p12) LDFD	f51 = [X1], INCX3M1
	(p12) LDFD	f55 = [X2], INCX3M1
	(p13) shladd XX = INCX, 1, XX;;
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], 1 * SIZE
	(p14) LDFD	f60 = [XX], 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f57 = [X1], INCXM1
	(p14) LDFD	f61 = [XX]
	mov	YY = Y1
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48, 1 * SIZE
	(p12) STFD	[Y2] = f52, 1 * SIZE
	}
	{ .mmi
	(p13) LDFD	f58 = [X1], 1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCYM1, Y1
	}
	{ .mmi
	(p13) LDFD	f59 = [X1]
	(p12) add Y2 = INCYM1, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50, 1 * SIZE
	(p12) STFD	[Y2] = f54, 1 * SIZE
	(p12) shladd YY = INCY, 2, YY;;
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
	(p12) STFD	[Y2] = f55
	(p13) shladd YY = INCY, 1, YY
	}
	{ .mmi
	(p12) add Y1 = INCY3M1, Y1
	(p12) add Y2 = INCY3M1, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56, 1 * SIZE
	(p14) STFD	[YY] = f60, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f61
	(p13) add Y1 = INCYM1, Y1
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58, 1 * SIZE
	nop.m 0
	nop.i 0
	}
	;;
	{ .mib
	(p13) STFD	[Y1] = f59
	nop.i 0
	br.ret.sptk.many b0
	}

	EPILOGUE

