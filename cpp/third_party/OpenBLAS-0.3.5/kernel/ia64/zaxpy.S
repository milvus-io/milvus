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

#ifdef XDOUBLE
#define PREFETCH_SIZE ( 8 * 16)
#elif defined(DOUBLE)
#define PREFETCH_SIZE (16 * 16)
#else
#define PREFETCH_SIZE (32 * 16)
#endif

#ifndef CONJ
#define FMA1	FNMA
#define FMA2	FMA
#else
#define FMA1	FMA
#define FMA2	FNMA
#endif

#define SP	r12

#ifdef XDOUBLE
#define N	r32
#define X1	r14
#define INCX	r15
#define Y1	r16
#define INCY	r17
#else
#define N	r32
#define X1	r37
#define INCX	r38
#define Y1	r39
#define INCY	r36
#endif

#define PREX1	r2
#define PREY1	r3

#define I	r18
#define J	r19
#define Y2	r20
#define X2	r21
#define INCX8	r22
#define INCY8	r23
#define YY1	r24
#define YY2	r25
#define YY3	r26
#define YY4	r27

#define INCX2M1 loc0
#define INCY2M1 loc1
#define INCX4M1 loc2
#define INCY4M1 loc3
#define X3	loc4
#define Y3	loc5
#define X4	loc6
#define Y4	loc7
#define PREX2	loc8
#define PREY2	loc9

#define ARLC	r29
#define PR	r30

#define ALPHA_R	f8
#define ALPHA_I	f9

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	adds	r14 = 16, SP
	adds	r15 = 24, SP
	adds	r16 = 32, SP
	}
	{ .mmb
	adds	r17 = 40, SP
	cmp.gt	p15, p0 = r0, N
	(p15) br.ret.sptk.many b0
	}
	;;
#ifdef XDOUBLE
	{ .mmi
	ld8	X1     = [r14]
	ld8	INCX   = [r15]
	nop	__LINE__
	}
	{ .mmi
	ld8	Y1     = [r16]
	ld8	INCY   = [r17]
	nop	__LINE__
	}
	;;
#else
	{ .mmi
	ld8	INCY   = [r14]
	nop	__LINE__
	nop	__LINE__
	}
	;;
#endif
	{ .mmi
	.save	ar.pfs, r10
	alloc	r10 = ar.pfs, 8, 16, 0, 0
	and	J =  7, N
	shl	INCX = INCX, ZBASE_SHIFT
	}
	{ .mmi
	adds	PREX1 = (PREFETCH_SIZE + 2) * SIZE, X1
	adds	PREY1 = (PREFETCH_SIZE + 2) * SIZE, Y1
	shl	INCY = INCY, ZBASE_SHIFT
	}
	;;
	{ .mmi
	shladd	INCX8  = INCX, 3, r0
	shladd	INCY8  = INCY, 3, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mmi
	adds	INCX2M1  = -SIZE, INCX
	adds	INCY2M1  = -SIZE, INCY
	shr	I =  N, 3
	}
	;;
	{ .mmi
	add	INCX2M1 = INCX2M1, INCX
	add	INCY2M1 = INCY2M1, INCY
	mov	PR = pr
	}
	{ .mmi
	add	X2  = X1, INCX
	add	Y2  = Y1, INCY
	nop	__LINE__
	}
	;;
	{ .mmi
	shladd	INCX4M1	= INCX, 1, INCX2M1
	shladd	INCY4M1	= INCY, 1, INCY2M1
	mov	pr.rot= 0
	}
	{ .mmi
	shladd	X3  = INCX, 1, X1
	shladd	Y3  = INCY, 1, Y1
	}
	;;
	{ .mmi
	shladd	X4  = INCX, 1, X2
	shladd	Y4  = INCY, 1, Y2
	adds	I = -1, I
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	and	r8    =   127, Y1
	and	PREX1 =  -128, PREX1
	}
	;;
	{ .mmi
	mov	YY1 = Y1
	mov	YY2 = Y2
	mov	ar.ec = 3
	}
	{ .mmi
	mov	YY3 = Y3
	mov	YY4 = Y4
	or	PREX1 = PREX1, r8
	}
	;;
	{ .mmi
	shladd	PREX2 = INCX, 2, PREX1
	shladd	PREY2 = INCY, 2, PREY1
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p11 ,p0 = -1, I
	tbit.z	p0, p13 = N, 2
	(p11) br.cond.dpnt  .L25
	}
	;;
	.align 32

.L22:
#ifdef XDOUBLE
	{ .mmf
	(p16) LDFD	f80  = [Y1], 1 * SIZE
	(p16) LDFD	f83  = [Y2], 1 * SIZE
	(p18) FMA1	f82  = ALPHA_I, f40, f82
	}
	{ .mmf
	(p16) LDFD	f92  = [Y3], 1 * SIZE
	(p16) LDFD	f95  = [Y4], 1 * SIZE
	(p18) FMA1	f85  = ALPHA_I, f43, f85
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [Y1], INCY4M1
	(p16) LDFD	f89  = [Y2], INCY4M1
	(p18) FMA1	f94  = ALPHA_I, f52, f94
	}
	{ .mmf
	(p16) LDFD	f98  = [Y3], INCY4M1
	(p16) LDFD	f101 = [Y4], INCY4M1
	(p18) FMA1	f97  = ALPHA_I, f55, f97
	}
	;;
	{ .mmf
	(p16) LDFD	f32  = [X1], 1 * SIZE
	(p16) LDFD	f35  = [X2], 1 * SIZE
	(p18) FMA	f88  = ALPHA_I, f34, f88
	}
	{ .mmf
	(p16) LDFD	f44  = [X3], 1 * SIZE
	(p16) LDFD	f47  = [X4], 1 * SIZE
	(p18) FMA	f91  = ALPHA_I, f37, f91
	}
	;;
	{ .mmf
	(p16) LDFD	f38  = [X1], INCX4M1
	(p16) LDFD	f41  = [X2], INCX4M1
	(p18) FMA	f100 = ALPHA_I, f46, f100
	}
	{ .mmf
	(p16) LDFD	f50  = [X3], INCX4M1
	(p16) LDFD	f53  = [X4], INCX4M1
	(p18) FMA	f103 = ALPHA_I, f49, f103
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f82,   1 * SIZE
	(p18) STFD	[YY2] = f85,   1 * SIZE
	(p18) FMA	f106  = ALPHA_R, f58, f106
	}
	{ .mmf
	(p19) add	YY3 = YY3, INCY4M1
	(p19) add	YY4 = YY4, INCY4M1
	(p18) FMA	f109  = ALPHA_R, f61, f109
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f94,   1 * SIZE
	(p18) STFD	[YY4] = f97,   1 * SIZE
	(p18) FMA	f118  = ALPHA_R, f70, f118
	}
	{ .mmf
	(p16) lfetch.excl.nt1 [PREY1], INCY8
	(p16) lfetch.excl.nt1 [PREY2], INCY8
	(p18) FMA	f121  = ALPHA_R, f73, f121
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f88
	(p18) STFD	[YY2] = f91
	(p18) FMA2	f112  = ALPHA_R, f64, f112
	}
	{ .mmf
	(p18) add	YY1 = YY1, INCY4M1
	(p18) add	YY2 = YY2, INCY4M1
	(p18) FMA2	f115  = ALPHA_R, f67, f115
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f100
	(p18) STFD	[YY4] = f103
	(p18) FMA2	f124  = ALPHA_R, f76, f124
	}
	{ .mmf
	(p18) add	YY3 = YY3, INCY4M1
	(p18) add	YY4 = YY4, INCY4M1
	(p18) FMA2	f127  = ALPHA_R, f79, f127
	}
	;;
	{ .mmf
	(p16) LDFD	f104 = [Y1], 1 * SIZE
	(p16) LDFD	f107 = [Y2], 1 * SIZE
	(p18) FMA1	f106  = ALPHA_I, f64, f106
	}
	{ .mmf
	(p16) LDFD	f116 = [Y3], 1 * SIZE
	(p16) LDFD	f119 = [Y4], 1 * SIZE
	(p18) FMA1	f109  = ALPHA_I, f67, f109
	}
	;;
	{ .mmf
	(p16) LDFD	f110 = [Y1], INCY4M1
	(p16) LDFD	f113 = [Y2], INCY4M1
	(p18) FMA1	f118  = ALPHA_I, f76, f118
	}
	{ .mmf
	(p16) LDFD	f122 = [Y3], INCY4M1
	(p16) LDFD	f125 = [Y4], INCY4M1
	(p18) FMA1	f121  = ALPHA_I, f79, f121
	}
	;;
	{ .mmf
	(p16) LDFD	f56  = [X1], 1 * SIZE
	(p16) LDFD	f59  = [X2], 1 * SIZE
	(p18) FMA	f112  = ALPHA_I, f58, f112
	}
	{ .mmf
	(p16) LDFD	f68  = [X3], 1 * SIZE
	(p16) LDFD	f71  = [X4], 1 * SIZE
	(p18) FMA	f115  = ALPHA_I, f61, f115
	}
	;;
	{ .mmf
	(p16) LDFD	f62  = [X1], INCX4M1
	(p16) LDFD	f65  = [X2], INCX4M1
	(p18) FMA	f124  = ALPHA_I, f70, f124
	}
	{ .mmf
	(p16) LDFD	f74  = [X3], INCX4M1
	(p16) LDFD	f77  = [X4], INCX4M1
	(p18) FMA	f127  = ALPHA_I, f73, f127
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f106,   1 * SIZE
	(p18) STFD	[YY2] = f109,   1 * SIZE
	(p17) FMA	f81  = ALPHA_R, f33, f81
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f84  = ALPHA_R, f36, f84
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f118,   1 * SIZE
	(p18) STFD	[YY4] = f121,   1 * SIZE
	(p17) FMA	f93  = ALPHA_R, f45, f93
	}
	{ .mmf
	(p16) lfetch.nt1 [PREX1], INCX8
	(p16) lfetch.nt1 [PREX2], INCX8
	(p17) FMA	f96  = ALPHA_R, f48, f96
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f112
	(p18) STFD	[YY2] = f115
	(p17) FMA2	f87  = ALPHA_R, f39, f87
	}
	{ .mmf
	(p18) add	YY1 = YY1, INCY4M1
	(p18) add	YY2 = YY2, INCY4M1
	(p17) FMA2	f90  = ALPHA_R, f42, f90
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f124
	(p18) STFD	[YY4] = f127
	(p17) FMA2	f99  = ALPHA_R, f51, f99
	}
	{ .mfb
	nop	__LINE__
	(p17) FMA2	f102 = ALPHA_R, f54, f102
	br.ctop.sptk.few .L22
	}
	;;
	;;
	(p19) add	YY3 = YY3, INCY4M1
	(p19) add	YY4 = YY4, INCY4M1
	;;
#else
	{ .mmf
	(p19) STFD	[YY3] = f125
	(p19) STFD	[YY4] = f32
	(p18) FMA2	f100 = ALPHA_R, f52, f100
	}
	{ .mmf
	(p16) lfetch.excl.nt1 [PREY1], INCY8
	nop   __LINE__
	(p18) FMA2	f103 = ALPHA_R, f55, f103
	}
	;;
	{ .mmf
	(p16) LDFD	f80  = [Y1], 1 * SIZE
	(p16) LDFD	f83  = [Y2], 1 * SIZE
	(p18) FMA1	f82  = ALPHA_I, f40, f82
	}
	{ .mmf
	(p16) LDFD	f92  = [Y3], 1 * SIZE
	(p16) LDFD	f95  = [Y4], 1 * SIZE
	(p18) FMA1	f85  = ALPHA_I, f43, f85
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [Y1], INCY4M1
	(p16) LDFD	f89  = [Y2], INCY4M1
	(p18) FMA1	f94  = ALPHA_I, f52, f94
	}
	{ .mmf
	(p19) add	YY3 = YY3, INCY4M1
	(p19) add	YY4 = YY4, INCY4M1
	(p18) FMA1	f97  = ALPHA_I, f55, f97
	}
	;;
	{ .mmf
	(p16) LDFD	f98  = [Y3], INCY4M1
	(p16) LDFD	f101 = [Y4], INCY4M1
	(p18) FMA	f88  = ALPHA_I, f34, f88
	}
	{ .mmf
	(p19) add	YY1 = YY1, INCY4M1
	(p19) add	YY2 = YY2, INCY4M1
	(p18) FMA	f91  = ALPHA_I, f37, f91
	}
	;;
	{ .mmf
	(p16) LDFD	f32  = [X1], 1 * SIZE
	(p16) LDFD	f35  = [X2], 1 * SIZE
	(p18) FMA	f100 = ALPHA_I, f46, f100
	}
	{ .mmf
	(p16) LDFD	f44  = [X3], 1 * SIZE
	(p16) LDFD	f47  = [X4], 1 * SIZE
	(p18) FMA	f103 = ALPHA_I, f49, f103
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f82,   1 * SIZE
	(p18) STFD	[YY2] = f85,   1 * SIZE
	(p18) FMA	f106  = ALPHA_R, f58, f106
	}
	{ .mmf
	(p16) LDFD	f38  = [X1], INCX4M1
	(p16) LDFD	f41  = [X2], INCX4M1
	(p18) FMA	f109  = ALPHA_R, f61, f109
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f94,   1 * SIZE
	(p18) STFD	[YY4] = f97,   1 * SIZE
	(p18) FMA	f118  = ALPHA_R, f70, f118
	}
	{ .mmf
	(p16) LDFD	f50  = [X3], INCX4M1
	(p16) LDFD	f53  = [X4], INCX4M1
	(p18) FMA	f121  = ALPHA_R, f73, f121
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f88
	(p18) STFD	[YY2] = f91
	(p18) FMA2	f112  = ALPHA_R, f64, f112
	}
	{ .mmf
	(p16) lfetch.nt1 [PREX1], INCX8
	nop   __LINE__
	(p18) FMA2	f115  = ALPHA_R, f67, f115
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f100
	(p18) STFD	[YY4] = f103
	(p18) FMA2	f124  = ALPHA_R, f76, f124
	}
	{ .mmf
	(p16) LDFD	f104 = [Y1], 1 * SIZE
	(p16) LDFD	f107 = [Y2], 1 * SIZE
	(p18) FMA2	f127  = ALPHA_R, f79, f127
	}
	;;
	{ .mmf
	(p16) LDFD	f116 = [Y3], 1 * SIZE
	(p16) LDFD	f119 = [Y4], 1 * SIZE
	(p18) FMA1	f106  = ALPHA_I, f64, f106
	}
	{ .mmf
	(p18) add	YY1 = YY1, INCY4M1
	(p18) add	YY2 = YY2, INCY4M1
	(p18) FMA1	f109  = ALPHA_I, f67, f109
	}
	;;
	{ .mmf
	(p16) LDFD	f110 = [Y1], INCY4M1
	(p16) LDFD	f113 = [Y2], INCY4M1
	(p18) FMA1	f118  = ALPHA_I, f76, f118
	}
	{ .mmf
	(p18) add	YY3 = YY3, INCY4M1
	(p18) add	YY4 = YY4, INCY4M1
	(p18) FMA1	f121  = ALPHA_I, f79, f121
	}
	;;
	{ .mmf
	(p16) LDFD	f122 = [Y3], INCY4M1
	(p16) LDFD	f125 = [Y4], INCY4M1
	(p18) FMA	f112  = ALPHA_I, f58, f112
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p18) FMA	f115  = ALPHA_I, f61, f115
	}
	;;
	{ .mmf
	(p16) LDFD	f56  = [X1], 1 * SIZE
	(p16) LDFD	f59  = [X2], 1 * SIZE
	(p18) FMA	f124  = ALPHA_I, f70, f124
	}
	{ .mmf
	(p16) LDFD	f68  = [X3], 1 * SIZE
	(p16) LDFD	f71  = [X4], 1 * SIZE
	(p18) FMA	f127  = ALPHA_I, f73, f127
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f106,   1 * SIZE
	(p18) STFD	[YY2] = f109,   1 * SIZE
	(p17) FMA	f81  = ALPHA_R, f33, f81
	}
	{ .mmf
	(p16) LDFD	f62  = [X1], INCX4M1
	(p16) LDFD	f65  = [X2], INCX4M1
	(p17) FMA	f84  = ALPHA_R, f36, f84
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f118,   1 * SIZE
	(p18) STFD	[YY4] = f121,   1 * SIZE
	(p17) FMA	f93  = ALPHA_R, f45, f93
	}
	{ .mmf
	(p16) LDFD	f74  = [X3], INCX4M1
	(p16) LDFD	f77  = [X4], INCX4M1
	(p17) FMA	f96  = ALPHA_R, f48, f96
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f112
	(p18) STFD	[YY2] = f115
	(p17) FMA2	f87  = ALPHA_R, f39, f87
	}
	{ .mfb
	nop	__LINE__
	(p17) FMA2	f90  = ALPHA_R, f42, f90
	br.ctop.sptk.few .L22
	}
	;;
	{ .mmi
	(p19) STFD	[YY3] = f125
	(p19) STFD	[YY4] = f32
	(p19) add	YY1 = YY1, INCY4M1
	}
	{ .mmi
	(p19) add	YY2 = YY2, INCY4M1
	(p19) add	YY3 = YY3, INCY4M1
	(p19) add	YY4 = YY4, INCY4M1
	}
	;;
#endif
	.align 32

.L25:
	{ .mmi
	(p13) LDFD	f32 = [X1], 1 * SIZE
	(p13) LDFD	f34 = [X2], 1 * SIZE
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p13) LDFD	f36 = [X3], 1 * SIZE
	(p13) LDFD	f38 = [X4], 1 * SIZE
	cmp.eq	p12, p0 = r0, J
	}
	;;
	{ .mmi
	(p13) LDFD	f80 = [Y1], 1 * SIZE
	(p13) LDFD	f82 = [Y2], 1 * SIZE
	mov	pr = PR, -65474
	}
	{ .mmb
	(p13) LDFD	f84 = [Y3], 1 * SIZE
	(p13) LDFD	f86 = [Y4], 1 * SIZE
	(p12) 	br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p13) LDFD	f33 = [X1], INCX4M1
	(p13) LDFD	f35 = [X2], INCX4M1
	tbit.z	p0, p14 = N, 1
	}
	{ .mmi
	(p13) LDFD	f81 = [Y1], INCY4M1
	(p13) LDFD	f83 = [Y2], INCY4M1
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) LDFD	f37 = [X3], INCX4M1
	(p13) LDFD	f39 = [X4], INCX4M1
	tbit.z	p0, p15 = N, 0
	}
	{ .mmi
	(p13) LDFD	f85 = [Y3], INCY4M1
	(p13) LDFD	f87 = [Y4], INCY4M1
	nop   __LINE__
	}
	;;
	{ .mmf
	(p14) LDFD	f40 = [X1], 1 * SIZE
	(p14) LDFD	f42 = [X2], 1 * SIZE
	}
	;;
	{ .mmf
	(p14) LDFD	f88 = [Y1], 1 * SIZE
	(p14) LDFD	f90 = [Y2], 1 * SIZE
	}
	;;
	{ .mmf
	(p14) LDFD	f41 = [X1], INCX2M1
	(p14) LDFD	f43 = [X2], INCX2M1
	(p13) FMA	f80  = ALPHA_R, f32, f80
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f82  = ALPHA_R, f34, f82
	}
	;;
	{ .mmf
	(p14) LDFD	f89 = [Y1], INCY2M1
	(p14) LDFD	f91 = [Y2], INCY2M1
	(p13) FMA	f84  = ALPHA_R, f36, f84
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f86  = ALPHA_R, f38, f86
	}
	;;
	{ .mmf
	(p15) LDFD	f44 = [X1], 1 * SIZE
	(p15) LDFD	f92 = [Y1], 1 * SIZE
	(p13) FMA2	f81  = ALPHA_R, f33, f81
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA2	f83  = ALPHA_R, f35, f83
	}
	;;
	{ .mmf
	(p15) LDFD	f45 = [X1]
	(p15) LDFD	f93 = [Y1]
	(p13) FMA2	f85  = ALPHA_R, f37, f85
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA2	f87  = ALPHA_R, f39, f87
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA1	f80  = ALPHA_I, f33, f80
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA1	f82  = ALPHA_I, f35, f82
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA1	f84  = ALPHA_I, f37, f84
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA1	f86  = ALPHA_I, f39, f86
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f81  = ALPHA_I, f32, f81
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f83  = ALPHA_I, f34, f83
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f85  = ALPHA_I, f36, f85
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f87  = ALPHA_I, f38, f87
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f80,   1 * SIZE
	(p13) STFD	[YY2] = f82,   1 * SIZE
	(p14) FMA	f88  = ALPHA_R, f40, f88
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p14) FMA	f90  = ALPHA_R, f42, f90
	}
	;;
	{ .mmf
	(p13) STFD	[YY3] = f84,   1 * SIZE
	(p13) STFD	[YY4] = f86,   1 * SIZE
	(p14) FMA2	f89  = ALPHA_R, f41, f89
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p14) FMA2	f91  = ALPHA_R, f43, f91
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f81
	(p13) STFD	[YY2] = f83
	(p15) FMA	f92  = ALPHA_R, f44, f92
	}
	{ .mmf
	(p13) add	YY1 = YY1, INCY4M1
	(p13) add	YY2 = YY2, INCY4M1
	(p15) FMA2	f93  = ALPHA_R, f45, f93
	}
	;;
	{ .mmf
	(p13) STFD	[YY3] = f85
	(p13) STFD	[YY4] = f87
	(p14) FMA1	f88  = ALPHA_I, f41, f88
	}
	{ .mmf
	(p13) add	YY3 = YY3, INCY4M1
	(p13) add	YY4 = YY4, INCY4M1
	(p14) FMA1	f90  = ALPHA_I, f43, f90
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p14) FMA	f89  = ALPHA_I, f40, f89
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p14) FMA	f91  = ALPHA_I, f42, f91
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p15) FMA1	f92  = ALPHA_I, f45, f92
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p15) FMA	f93  = ALPHA_I, f44, f93
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f88,   1 * SIZE
	(p14) STFD	[YY2] = f90,   1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f89
	(p14) STFD	[YY2] = f91
	(p14) add	YY1 = YY1, INCY2M1
	}
	;;
	{ .mmi
	(p15) STFD	[YY1] = f92,   1 * SIZE
	nop   __LINE__
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f93
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE
