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

#ifdef DOUBLE
#define PREFETCH_SIZE (13 * 16)
#else
#define PREFETCH_SIZE ( 9 * 32)
#endif

#if defined(F_INTERFACE) && defined(RETURN_BY_STACK)
#define N	r33
#define X1	r34
#define INCX	r35
#define Y1	r36
#define INCY	r37
#else
#define N	r32
#define X1	r33
#define INCX	r34
#define Y1	r35
#define INCY	r36
#endif

#define PRE1	r2
#define PRE2	r3

#define I	r14
#define J	r15
#define Y2	r16
#define X2	r17
#define INCXM1	r18
#define INCYM1	r19
#define INCX16	r20
#define INCY16	r21
#define INCX3M1	r22
#define INCY3M1	r23
#define XX	r24
#define YY	r25

#define PR	r30
#define ARLC	r31

#define ALPHA	f8

	PROLOGUE
	.prologue
	PROFCODE

	{ .mfi
	mov	f8  = f0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mfi
	mov	f9  = f0
	}
	;;
	.body
#ifdef F_INTERFACE
	LDINT	N    = [N]
	LDINT	INCX = [INCX]
	LDINT	INCY = [INCY]
	;;
#ifndef USE64BITINT
	sxt4	N = N
	sxt4	INCX = INCX
	sxt4	INCY = INCY
	;;
#endif
#endif
	{ .mmi
	shladd	INCX = INCX, ZBASE_SHIFT, r0
	shladd	INCY = INCY, ZBASE_SHIFT, r0
	mov	PR = pr
	}
	{ .mib
	cmp.lt	p0, p7 = r0, N
	mov	r26 = 1
	(p7) br.cond.spnt .L1000
	}
	;;
#ifdef F_INTERFACE
	cmp.le	p0, p6 = r0, INCX
	cmp.le	p0, p7 = r0, INCY
	sub	r26 = r26, N
	;;
	setf.sig f32 = r26
	setf.sig f33 = INCX
	setf.sig f34 = INCY
	;;
	xmpy.l f33 = f32, f33
	xmpy.l f34 = f32, f34
	;;
	getf.sig r26 = f33
	getf.sig r27 = f34
	;;
	(p6) add X1 = X1, r26
	(p7) add Y1 = Y1, r27
	;;
#endif
	{ .mfi
#ifdef DOUBLE
	adds	PRE1 = (PREFETCH_SIZE + 4) * SIZE, X1
#else
	adds	PRE1 = (PREFETCH_SIZE + 8) * SIZE, X1
#endif
	mov	f10 = f0
	mov	pr.rot= 0
	}
	{ .mfi
	and	J =  7, N
	mov	f11 = f0
	shr	I =  N, 3
	}
	;;
	{ .mfi
#ifdef DOUBLE
	adds	PRE2 = (PREFETCH_SIZE +  6) * SIZE, Y1
#else
	adds	PRE2 = (PREFETCH_SIZE + 12) * SIZE, Y1
#endif
	mov	f12 = f0
	mov	ar.ec = 3
	}
	{ .mmf
	shladd	INCX16 = INCX, 3, r0
	shladd	INCY16 = INCY, 3, r0
	mov	f13 = f0
	}
	;;
	{ .mmf
	shladd	INCX3M1 = INCX, 1, INCX
	shladd	INCY3M1 = INCY, 1, INCY
	mov	f14 = f0
	}
	{ .mmf
	adds	INCXM1	= -SIZE, INCX
	adds	INCYM1	= -SIZE, INCY
	mov	f15 = f0
	}
	;;
	{ .mmi
	adds	INCX3M1 = -SIZE, INCX3M1
	adds	INCY3M1 = -SIZE, INCY3M1
	tbit.z	p0, p12 = N, 2
	}
	{ .mmi
	cmp.eq	p8 ,p0  =   r0, I
	cmp.eq	p16, p0 = r0, r0
	adds	I = -1, I
	}
	;;
	{ .mmi
	shladd	X2 = INCX, 1, X1
	shladd	Y2 = INCY, 1, Y1
	mov	ar.lc = I
	}
	{ .mmb
	mov	XX = X1
	mov	YY = Y1
	(p8) br.cond.dpnt  .L55
	}
	;;
	.align 32

.L52:
	{ .mmf
	(p16) lfetch.nt1	[PRE1], INCX16
	(p16) LDFD	f32  = [X1], SIZE
	(p18) FMA	f8  = f34, f82,  f8
	}
	{ .mmf
	(p16) LDFD	f44  = [X2], SIZE
	nop.m 0
	(p18) FMA	f9  = f34, f85,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f80  = [Y1], SIZE
	(p16) LDFD	f92  = [Y2], SIZE
	(p18) FMA	f10 = f37, f82,  f10
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f11 = f37, f85,  f11
	}
	;;
	{ .mmf
	(p16) lfetch.nt1	[PRE2], INCY16
	(p16) LDFD	f35  = [X1], INCXM1
	(p18) FMA	f12 = f40, f88,  f12
	}
	{ .mmf
	(p16) LDFD	f47  = [X2], INCXM1
	nop.m 0
	(p18) FMA	f13 = f40, f91,  f13
	}
	;;
	{ .mmf
	(p16) LDFD	f83  = [Y1], INCYM1
	(p16) LDFD	f95  = [Y2], INCYM1
	(p18) FMA	f14 = f43, f88,  f14
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f15 = f43, f91,  f15
	}
	;;
	{ .mmf
	(p16) LDFD	f38  = [X1], SIZE
	(p16) LDFD	f50  = [X2], SIZE
	(p18) FMA	f8  = f46, f94,  f8
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f9  = f46, f97,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [Y1], SIZE
	(p16) LDFD	f98  = [Y2], SIZE
	(p18) FMA	f10  = f49, f94,  f10
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f11  = f49, f97,  f11
	}
	;;
	{ .mmf
	(p16) LDFD	f41  = [X1], INCX3M1
	(p16) LDFD	f53  = [X2], INCX3M1
	(p18) FMA	f12  = f52, f100, f12
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f13  = f52, f103, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f89  = [Y1], INCY3M1
	(p16) LDFD	f101 = [Y2], INCY3M1
	(p18) FMA	f14  = f55, f100, f14
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f15  = f55, f103, f15
	}
	;;
	{ .mmf
	(p16) LDFD	f56  = [X1], SIZE
	(p16) LDFD	f68  = [X2], SIZE
	(p18) FMA	f8   = f58, f106, f8
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f9   = f58, f109, f9
	}
	;;
	{ .mmf
	(p16) LDFD	f104 = [Y1], SIZE
	(p16) LDFD	f116 = [Y2], SIZE
	(p18) FMA	f10  = f61, f106, f10
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f11  = f61, f109, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f59  = [X1], INCXM1
	(p16) LDFD	f71  = [X2], INCXM1
	(p18) FMA	f12  = f64, f112, f12
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f13  = f64, f115, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f107 = [Y1], INCYM1
	(p16) LDFD	f119 = [Y2], INCYM1
	(p18) FMA	f14  = f67, f112, f14
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f15  = f67, f115, f15
	}
	;;
	{ .mmf
	(p16) LDFD	f62  = [X1], SIZE
	(p16) LDFD	f74  = [X2], SIZE
	(p18) FMA	f8   = f70, f118, f8
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f9   = f70, f121,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f110 = [Y1], SIZE
	(p16) LDFD	f122 = [Y2], SIZE
	(p18) FMA	f10  = f73, f118, f10
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p18) FMA	f11  = f73, f121, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f65  = [X1], INCX3M1
	(p16) LDFD	f77  = [X2], INCX3M1
	(p18) FMA	f12  = f76, f124, f12
	}
	{ .mmf
	(p16) add	XX = INCX16, XX
	(p16) add	YY = INCY16, YY
	(p18) FMA	f13  = f76, f127, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f113 = [Y1], INCY3M1
	(p16) LDFD	f125 = [Y2], INCY3M1
	(p18) FMA	f14 = f79, f124, f14
	}
	{ .mfb
	nop.m 0
	(p18) FMA	f15 = f79, f127, f15
	br.ctop.sptk.few .L52
	}
	;;
	.align 32

.L55:
	(p12) LDFD	f32  = [X1], SIZE
	(p12) LDFD	f40  = [X2], SIZE
	tbit.z	p0, p13 = N, 1
	(p12) LDFD	f34  = [Y1], SIZE
	(p12) LDFD	f42  = [Y2], SIZE
	tbit.z	p0, p14 = N, 0
	;;
	(p12) LDFD	f33  = [X1], INCXM1
	(p12) LDFD	f41  = [X2], INCXM1
	cmp.eq	p9, p0  =   r0, J
	(p12) LDFD	f35  = [Y1], INCYM1
	(p12) LDFD	f43  = [Y2], INCYM1
	(p9) br.cond.dptk .L999
	;;
	(p12) LDFD	f36  = [X1], SIZE
	(p12) LDFD	f44  = [X2], SIZE
	(p12) shladd XX = INCX, 2, XX
	(p12) LDFD	f38  = [Y1], SIZE
	(p12) LDFD	f46  = [Y2], SIZE
	(p12) shladd YY = INCY, 2, YY
	;;
	(p12) LDFD	f37  = [X1], INCX3M1
	(p12) LDFD	f45  = [X2], INCX3M1
	(p13) shladd XX = INCX, 1, XX
	(p12) LDFD	f39  = [Y1], INCY3M1
	(p12) LDFD	f47  = [Y2], INCY3M1
	(p13) shladd YY = INCY, 1, YY
	;;
	(p13) LDFD	f48  = [X1], SIZE
	(p13) LDFD	f50  = [Y1], SIZE
	(p14) LDFD	f56  = [XX], SIZE
	(p14) LDFD	f58  = [YY], SIZE
	;;
	(p13) LDFD	f49  = [X1], INCXM1
	(p13) LDFD	f51  = [Y1], INCYM1
	(p14) LDFD	f57  = [XX]
	(p14) LDFD	f59  = [YY]
	;;
	(p13) LDFD	f52  = [X1], SIZE
	(p13) LDFD	f54  = [Y1], SIZE
	;;
	(p13) LDFD	f53  = [X1]
	(p13) LDFD	f55  = [Y1]
	;;
	(p12) FMA	f8  = f32, f34, f8
	(p12) FMA	f9  = f32, f35, f9
	(p12) FMA	f10 = f33, f34, f10
	(p12) FMA	f11 = f33, f35, f11
	(p12) FMA	f12 = f36, f38, f12
	(p12) FMA	f13 = f36, f39, f13
	(p12) FMA	f14 = f37, f38, f14
	(p12) FMA	f15 = f37, f39, f15
	;;
	(p12) FMA	f8  = f40, f42, f8
	(p12) FMA	f9  = f40, f43, f9
	(p12) FMA	f10 = f41, f42, f10
	(p12) FMA	f11 = f41, f43, f11
	(p12) FMA	f12 = f44, f46, f12
	(p12) FMA	f13 = f44, f47, f13
	(p12) FMA	f14 = f45, f46, f14
	(p12) FMA	f15 = f45, f47, f15
	;;
	(p13) FMA	f8  = f48, f50, f8
	(p13) FMA	f9  = f48, f51, f9
	(p13) FMA	f10 = f49, f50, f10
	(p13) FMA	f11 = f49, f51, f11
	(p13) FMA	f12 = f52, f54, f12
	(p13) FMA	f13 = f52, f55, f13
	(p13) FMA	f14 = f53, f54, f14
	(p13) FMA	f15 = f53, f55, f15
	;;
	(p14) FMA	f8  = f56, f58, f8
	(p14) FMA	f9  = f56, f59, f9
	(p14) FMA	f10 = f57, f58, f10
	(p14) FMA	f11 = f57, f59, f11
	.align 32
	;;
.L999:
	FADD	f8  = f8,  f12
	FADD	f9  = f9,  f13
	FADD	f10 = f10, f14
	FADD	f11 = f11, f15
	mov	ar.lc = ARLC
	;;
#ifndef CONJ
	FSUB	f8 = f8, f11
	FADD	f9 = f9, f10
#else
	FADD	f8 = f8, f11
	FSUB	f9 = f9, f10
#endif
	;;
	.align 32

.L1000:
#if defined(F_INTERFACE) && defined(RETURN_BY_STACK)
	STFD	[r32] = f8, SIZE
	;;
	STFD	[r32] = f9, SIZE
#endif
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	EPILOGUE

