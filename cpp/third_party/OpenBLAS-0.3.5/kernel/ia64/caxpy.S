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

#define PREFETCH_SIZE (32 * 16)

#ifndef CONJ
#define FMA1	FNMA
#define FMA2	FMA
#else
#define FMA1	FMA
#define FMA2	FNMA
#endif

#define SP	r12

#define N	r32
#define X1	r37
#define INCX	r38
#define Y1	r39
#define INCY	r36

#define PREX1	r2
#define PREY1	r3

#define I	r33
#define J	r34
#define Y2	r35
#define X2	r14
#define YY1	r15
#define YY2	r16
#define YY3	r17
#define YY4	r18

#define INCXM1  r19
#define INCYM1  r20
#define INCX3M1 r21
#define INCY3M1 r22
#define INCX7M1 r23
#define INCY7M1 r24

#define X3	r8
#define Y3	r9
#define X4	r10
#define Y4	r11
#define INCX8	r25
#define INCY8	r26

#define ARLC	r29
#define PR	r30

#define ALPHA_R	f8
#define ALPHA_I	f9

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	adds	r14 = 16, SP
	and	J =  7, N
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.gt	p15, p0 = r0, N
	shr	I =  N, 3
	(p15) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	ld8	INCY   = [r14]
	nop	__LINE__
	mov	PR = pr
	}
	{ .mmi
	adds	PREX1 = (PREFETCH_SIZE + 0) * SIZE, X1
	adds	PREY1 = (PREFETCH_SIZE + 0) * SIZE, Y1
	shl	INCX = INCX, ZBASE_SHIFT
	}
	;;
	{ .mii
	adds	I = -1, I
	mov	pr.rot= 0
	shl	INCY = INCY, ZBASE_SHIFT
	}
	;;
	{ .mmi
	adds	INCXM1  = -SIZE, INCX
	adds	INCYM1  = -SIZE, INCY
	mov	ar.ec = 3
	}
	{ .mmi
	shladd	X2  = INCX, 1, X1
	shladd	Y2  = INCY, 1, Y1
	cmp.eq	p16, p0 = r0, r0
	}
	;;
	{ .mmi
	shladd	INCX3M1 = INCX, 1, INCXM1
	shladd	INCY3M1 = INCY, 1, INCYM1
	shladd	INCX8 = INCX, 3, r0
	}
	{ .mmi
	shladd	X3  = INCX, 1, X2
	shladd	Y3  = INCY, 1, Y2
	shladd	INCY8 = INCY, 3, r0
	}
	;;
	{ .mmi
	shladd	X4  = INCX, 1, X3
	shladd	Y4  = INCY, 1, Y3
	shladd	INCX7M1 = INCX, 2, INCX3M1
	}
	{ .mmi
	mov	YY1 = Y1
	mov	YY2 = Y2
	shladd	INCY7M1 = INCY, 2, INCY3M1
	}
	;;
	{ .mmi
	mov	YY3 = Y3
	mov	YY4 = Y4
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
	{ .mmf
	(p19) STFD	[YY3] = f14
	(p19) STFD	[YY4] = f15
	(p18) FMA2	f14 = ALPHA_R, f64, f112
	}
	{ .mmf
	(p16) LDFD	f80  = [Y1], 1 * SIZE
	(p16) LDFD	f92  = [Y2], 1 * SIZE
	(p18) FMA2	f15 = ALPHA_R, f76, f124
	}
	;;
	{ .mmf
	(p16) lfetch.excl.nt1 [PREY1], INCY8
	(p16) LDFD	f104 = [Y3], 1 * SIZE
	(p18) FMA1	f6  = ALPHA_I, f40, f6
	}
	{ .mmf
	(p16) LDFD	f116 = [Y4], 1 * SIZE
	nop	__LINE__
	(p18) FMA1	f7  = ALPHA_I, f52, f7
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [Y1], INCYM1
	(p16) LDFD	f98  = [Y2], INCYM1
	(p18) FMA1	f10 = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p18) FMA1	f11 = ALPHA_I, f76, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f110 = [Y3], INCYM1
	(p16) LDFD	f122 = [Y4], INCYM1
	(p18) FMA	f12 = ALPHA_I, f34, f12
	}
	{ .mmf
	(p19) add	YY1 = YY1, INCY7M1
	(p19) add	YY2 = YY2, INCY7M1
	(p18) FMA	f13 = ALPHA_I, f46, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f32  = [X1], 1 * SIZE
	(p16) LDFD	f44  = [X2], 1 * SIZE
	(p18) FMA	f14 = ALPHA_I, f58, f14
	}
	{ .mmf
	(p19) add	YY3 = YY3, INCY7M1
	(p19) add	YY4 = YY4, INCY7M1
	(p18) FMA	f15 = ALPHA_I, f70, f15
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
 	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA_R, f37, f85
	}
	{ .mmf
	(p16) LDFD	f56  = [X3], 1 * SIZE
	(p16) LDFD	f68  = [X4], 1 * SIZE
	(p18) FMA	f7  = ALPHA_R, f49, f97
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f10,  1 * SIZE
	(p18) STFD	[YY4] = f11,  1 * SIZE
	(p18) FMA	f10 = ALPHA_R, f61, f109
	}
	{ .mmf
	(p16) LDFD	f38  = [X1], INCXM1
	(p16) LDFD	f50  = [X2], INCXM1
	(p18) FMA	f11 = ALPHA_R, f73, f121
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12
	(p18) STFD	[YY2] = f13
	(p18) FMA2	f12 = ALPHA_R, f43, f91
	}
	{ .mmf
	(p16) LDFD	f62  = [X3], INCXM1
	(p16) LDFD	f74  = [X4], INCXM1
	(p18) FMA2	f13 = ALPHA_R, f55, f103
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f14
	(p18) STFD	[YY4] = f15
	(p18) FMA2	f14 = ALPHA_R, f67, f115
	}
	{ .mmf
	(p16) LDFD	f83  = [Y1], 1 * SIZE
	(p16) LDFD	f95  = [Y2], 1 * SIZE
	(p18) FMA2	f15 = ALPHA_R, f79, f127
	}
	;;
	{ .mmf
	(p16) LDFD	f107 = [Y3], 1 * SIZE
	(p16) LDFD	f119 = [Y4], 1 * SIZE
	(p18) FMA1	f6  = ALPHA_I, f43, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p18) FMA1	f7  = ALPHA_I, f55, f7
	}
	;;
	{ .mmf
	(p16) LDFD	f89  = [Y1], INCY7M1
	(p16) LDFD	f101 = [Y2], INCY7M1
	(p18) FMA1	f10 = ALPHA_I, f67, f10
	}
	{ .mmf
	(p18) add	YY1 = YY1, INCYM1
	(p18) add	YY2 = YY2, INCYM1
	(p18) FMA1	f11 = ALPHA_I, f79, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f113 = [Y3], INCY7M1
	(p16) LDFD	f125 = [Y4], INCY7M1
	(p18) FMA	f12 = ALPHA_I, f37, f12
	}
	{ .mmf
	(p18) add	YY3 = YY3, INCYM1
	(p18) add	YY4 = YY4, INCYM1
	(p18) FMA	f13 = ALPHA_I, f49, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f35  = [X1], 1 * SIZE
	(p16) LDFD	f47  = [X2], 1 * SIZE
	(p18) FMA	f14 = ALPHA_I, f61, f14
	}
	{ .mmf
	(p16) LDFD	f59  = [X3], 1 * SIZE
	(p16) LDFD	f71  = [X4], 1 * SIZE
	(p18) FMA	f15 = ALPHA_I, f73, f15
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6,  1 * SIZE
	(p18) STFD	[YY2] = f7,  1 * SIZE
	(p17) FMA	f6  = ALPHA_R, f33, f81
	}
	{ .mmf
	(p16) LDFD	f41  = [X1], INCX7M1
	(p16) LDFD	f53  = [X2], INCX7M1
	(p17) FMA	f7  = ALPHA_R, f45, f93
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f10, 1 * SIZE
	(p18) STFD	[YY4] = f11, 1 * SIZE
	(p17) FMA	f10 = ALPHA_R, f57, f105
	}
	{ .mmf
	(p16) LDFD	f65  = [X3], INCX7M1
	(p16) LDFD	f77  = [X4], INCX7M1
	(p17) FMA	f11 = ALPHA_R, f69, f117
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12
	(p18) STFD	[YY2] = f13
	(p17) FMA2	f12 = ALPHA_R, f39, f87
	}
	{ .mfb
	(p16) lfetch.nt1 [PREX1], INCX8
	(p17) FMA2	f13 = ALPHA_R, f51, f99
	br.ctop.sptk.few .L22
	}
	;;
	(p19) add	YY1 = YY1, INCY7M1
	(p19) add	YY2 = YY2, INCY7M1
	;;
	{ .mmf
	(p19) STFD	[YY3] = f14
	(p19) STFD	[YY4] = f15
	}
	{ .mmf
	(p19) add	YY3 = YY3, INCY7M1
	(p19) add	YY4 = YY4, INCY7M1
	}
	;;
	.align 32

.L25:
	{ .mmi
	(p13) LDFD	f32 = [X1], 1 * SIZE
	(p13) LDFD	f36 = [X2], 1 * SIZE
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p13) LDFD	f80 = [Y1], 1 * SIZE
	(p13) LDFD	f84 = [Y2], 1 * SIZE
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p13) LDFD	f33 = [X1], INCXM1
	(p13) LDFD	f37 = [X2], INCXM1
	cmp.eq	p12, p0 = r0, J
	}
	;;
	{ .mmb
	(p13) LDFD	f81 = [Y1], INCYM1
	(p13) LDFD	f85 = [Y2], INCYM1
	(p12) 	br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p13) LDFD	f34 = [X1], 1 * SIZE
	(p13) LDFD	f38 = [X2], 1 * SIZE
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFD	f82 = [Y1], 1 * SIZE
	(p13) LDFD	f86 = [Y2], 1 * SIZE
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmf
	(p13) LDFD	f35 = [X1], INCX3M1
	(p13) LDFD	f39 = [X2], INCX3M1
	(p13) FMA	f80  = ALPHA_R, f32, f80
	}
	;;
	{ .mmf
	(p13) LDFD	f83 = [Y1], INCY3M1
	(p13) LDFD	f87 = [Y2], INCY3M1
	(p13) FMA	f84  = ALPHA_R, f36, f84
	}
	;;
	{ .mmf
	(p14) LDFD	f40 = [X1], 1 * SIZE
	(p14) LDFD	f88 = [Y1], 1 * SIZE
	(p13) FMA2	f81  = ALPHA_R, f33, f81
	}
	;;
	{ .mmf
	(p14) LDFD	f41 = [X1], INCXM1
	(p14) LDFD	f89 = [Y1], INCYM1
	(p13) FMA2	f85  = ALPHA_R, f37, f85
	}
	;;
	{ .mmf
	(p14) LDFD	f42 = [X1], 1 * SIZE
	(p14) LDFD	f90 = [Y1], 1 * SIZE
	(p13) FMA	f82  = ALPHA_R, f34, f82
	}
	;;
	{ .mmf
	(p14) LDFD	f43 = [X1], INCXM1
	(p14) LDFD	f91 = [Y1], INCYM1
	(p13) FMA	f86  = ALPHA_R, f38, f86
	}
	;;
	{ .mmf
	(p15) LDFD	f44 = [X1], 1 * SIZE
	(p15) LDFD	f92 = [Y1], 1 * SIZE
	(p13) FMA2	f83  = ALPHA_R, f35, f83
	}
	;;
	{ .mmf
	(p15) LDFD	f45 = [X1]
	(p15) LDFD	f93 = [Y1]
	(p13) FMA2	f87  = ALPHA_R, f39, f87
	}
	;;
	(p13) FMA1	f80  = ALPHA_I, f33, f80
	(p13) FMA1	f84  = ALPHA_I, f37, f84
	(p13) FMA	f81  = ALPHA_I, f32, f81
	(p13) FMA	f85  = ALPHA_I, f36, f85
	(p13) FMA1	f82  = ALPHA_I, f35, f82
	(p13) FMA1	f86  = ALPHA_I, f39, f86
	(p13) FMA	f83  = ALPHA_I, f34, f83
	(p13) FMA	f87  = ALPHA_I, f38, f87
	;;
	{ .mmf
	(p13) STFD	[YY1] = f80,   1 * SIZE
	(p13) STFD	[YY2] = f84,   1 * SIZE
	(p14) FMA	f88  = ALPHA_R, f40, f88
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f81
	(p13) STFD	[YY2] = f85
	(p14) FMA2	f89  = ALPHA_R, f41, f89
	}
	{ .mmf
	(p13) add	YY1 = YY1, INCYM1
	(p13) add	YY2 = YY2, INCYM1
	(p14) FMA	f90  = ALPHA_R, f42, f90
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f82,   1 * SIZE
	(p13) STFD	[YY2] = f86,   1 * SIZE
	(p14) FMA2	f91  = ALPHA_R, f43, f91
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f83
	(p13) STFD	[YY2] = f87
	(p15) FMA	f92  = ALPHA_R, f44, f92
	}
	{ .mmf
	(p13) add	YY1 = YY1, INCY3M1
	nop   __LINE__
	(p15) FMA2	f93  = ALPHA_R, f45, f93
	}
	;;
	(p14) FMA1	f88  = ALPHA_I, f41, f88
	(p14) FMA	f89  = ALPHA_I, f40, f89
	(p14) FMA1	f90  = ALPHA_I, f43, f90
	(p14) FMA	f91  = ALPHA_I, f42, f91
	;;
	{ .mmf
	(p14) STFD	[YY1] = f88,   1 * SIZE
	(p15) FMA1	f92  = ALPHA_I, f45, f92
	}
	;;
	{ .mmf
	(p14) STFD	[YY1] = f89
	(p14) add	YY1 = YY1, INCYM1
	(p15) FMA	f93  = ALPHA_I, f44, f93
	}
	;;
	(p14) STFD	[YY1] = f90,   1 * SIZE
	;;
	(p14) STFD	[YY1] = f91
	(p14) add	YY1 = YY1, INCYM1
	;;
	(p15) STFD	[YY1] = f92,   1 * SIZE
	;;
	{ .mmb
	(p15) STFD	[YY1] = f93
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE
