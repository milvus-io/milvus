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

#define SP	r12

#define M	r32
#define N	r33
#ifndef XDOUBLE
#define A	r36
#define LDA	r37
#define X1	r38
#define INCX	r39
#define Y1	r34
#define INCY	r35
#else
#define A	r38
#define LDA	r39
#define X1	r34
#define INCX	r35
#define Y1	r36
#define INCY	r37
#endif

#define BUFFER	r11

#define I	r15
#define J	r16
#define AO1	r17
#define AO2	r18
#define AO3	r19
#define AO4	r20
#define AO5	r21
#define AO6	r22
#define AO7	r23
#define AO8	r24
#define X2	r25
#define Y2	r26
#define LDA7M8	r27
#define INCX5	r28
#define INCY5	r29

#define YY1	r8
#define YY2	r9

#define ARLC	r30
#define PR	r31

#ifdef DOUBLE
#define RPREFETCH	(16 * 3 +  8)
#else
#define RPREFETCH	(16 * 3 + 16)
#endif
#define PREFETCH	lfetch.nt1

#define ALPHA	f6

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	mov	ARLC  = ar.lc
	}
	{ .mmi
	adds	r15 = 24, SP
	adds	r14 = 16, SP
	}
	;;
#ifdef XDOUBLE
	ld8	X1     = [r14], 16
	ld8	INCX   = [r15], 16
	;;
#endif
	ld8	Y1     = [r14], 16
	ld8	INCY   = [r15], 16
	;;
	ld8	BUFFER = [r14]
	;;
	mov	PR = pr
	;;
	mov	ALPHA = f8
	.body
	;;
	{ .mmi
	cmp.ge	p7, p0 = r0, M
	cmp.ge	p6, p0 = r0, N
	}
	;;
	{ .mmi
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	shladd	LDA   = LDA, BASE_SHIFT, r0
	}
	;;
	{ .mbb
	(p7) br.cond.dpnt .L999
	(p6) br.cond.dpnt .L999
	}
	.align 16
	;;
	shladd	INCY5 = INCY, 2, INCY
	shladd	INCX5 = INCX, 2, INCX
	cmp.eq	p10, p0 = SIZE, INCX
	;;
	(p10)  mov BUFFER = X1
	(p10) br.cond.dptk .L10
	;;


	mov	pr.rot= 0
	shladd	X2    = INCX, 2, X1
	mov	YY1 = BUFFER
	adds	YY2 = 4 * SIZE, BUFFER
	;;
	shr	I = M, 3
	;;
	{ .mmi
	adds I = -1, I
	cmp.eq	p16, p0 = r0, r0
	mov	ar.ec= 5
	}
	;;
	{ .mmi
	mov	ar.lc = I
	}
	{ .mib
	cmp.gt	p6, p0 = 0, I
	tbit.nz	p13, p0 = M, 2
	(p6) br.cond.dpnt .L05
	}
	;;
	.align 16

.L01:
	(p20) STFD [YY1] = f36,  SIZE
	(p20) STFD [YY2] = f56,  SIZE
	(p16) LDFD f32 = [X1], INCX
	(p16) LDFD f52 = [X2], INCX
	;;
	(p20) STFD [YY1] = f41,  SIZE
	(p20) STFD [YY2] = f61,  SIZE
	(p16) LDFD f37 = [X1], INCX
	(p16) LDFD f57 = [X2], INCX
	;;
	(p20) STFD [YY1] = f46,  SIZE
	(p20) STFD [YY2] = f66,  SIZE
	(p16) LDFD f42 = [X1], INCX
	(p16) LDFD f62 = [X2], INCX
	;;
	(p20) STFD [YY1] = f51,  5 * SIZE
	(p20) STFD [YY2] = f71,  5 * SIZE
	(p16) LDFD f47 = [X1], INCX5
	(p16) LDFD f67 = [X2], INCX5
	br.ctop.sptk.few .L01
	;;
	.align 16

.L05:
	(p13) LDFD f32 = [X1],  INCX
	tbit.nz	p14, p0 = M, 1
	;;
	(p13) LDFD f33 = [X1],  INCX
	tbit.nz	p15, p0 = M, 0
	;;
	(p13) LDFD f34 = [X1],  INCX
	;;
	(p13) LDFD f35 = [X1],  INCX
	;;
	(p14) LDFD f36 = [X1],  INCX
	;;
	(p13) STFD [YY1] = f32, SIZE
	(p14) LDFD f37 = [X1],  INCX
	;;
	(p13) STFD [YY1] = f33, SIZE
	(p15) LDFD f38 = [X1],  INCX
	;;
	(p13) STFD [YY1] = f34, SIZE
	;;
	(p13) STFD [YY1] = f35, SIZE
	;;
	(p14) STFD [YY1] = f36, SIZE
	;;
	(p14) STFD [YY1] = f37, SIZE
	;;
	(p15) STFD [YY1] = f38, SIZE
	;;
	.align 16

.L10:
	mov	YY1  = Y1
	shladd	Y2   = INCY, 2, Y1
	shladd	YY2  = INCY, 2, Y1
	;;
	{ .mmi
	nop	__LINE__
	shr	J   = N, 3
	}
	;;
	{ .mib
	nop	__LINE__
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L20
	}
	;;
	.align 16

.L11:
	mov	AO1 = A
	adds	AO2 = 1 * SIZE, A
	adds	AO3 = 2 * SIZE, A
	adds	AO4 = 3 * SIZE, A
	adds	AO5 = 4 * SIZE, A
	adds	AO6 = 5 * SIZE, A
	adds	AO7 = 6 * SIZE, A
	adds	AO8 = 7 * SIZE, A
	shladd	A   = LDA, 3, A
	;;
	shladd	LDA7M8 = LDA, 3, r0
	;;
	sub	LDA7M8 = LDA, LDA7M8
	;;
	adds	LDA7M8 = 8 * SIZE, LDA7M8
	;;
	mov	f8  = f0
	mov	f9  = f0
	mov	f10 = f0
	mov	f11 = f0
	mov	f12 = f0
	mov	f13 = f0
	mov	f14 = f0
	mov	f15 = f0

	mov	pr.rot= 0
	shr	I = M, 3
	mov	ar.ec = 2
	;;
	mov	X1  = BUFFER
	adds	X2  = 2 * SIZE, BUFFER
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L15
	;;
	.align 16

.L12:
	(p16) LDFD	f32  = [AO1], LDA
	(p16) LDFD	f34  = [AO3], LDA
	(p16) LDFD	f36  = [AO5], LDA
	(p16) LDFD	f38  = [AO7], LDA
	;;
	(p16) LDFD	f33  = [AO2], LDA
	(p16) LDFD	f35  = [AO4], LDA
	(p16) LDFD	f37  = [AO6], LDA
	(p16) LDFD	f39  = [AO8], LDA
	;;
	(p16) LDFD	f40  = [AO1], LDA
	(p16) LDFD	f42  = [AO3], LDA
	(p16) LDFD	f44  = [AO5], LDA
	(p16) LDFD	f46  = [AO7], LDA
	;;
	(p16) LDFD	f41  = [AO2], LDA
	(p16) LDFD	f43  = [AO4], LDA
	(p16) LDFD	f45  = [AO6], LDA
	(p16) LDFD	f47  = [AO8], LDA
	;;
	(p16) LDFD	f48  = [AO1], LDA
	(p16) LDFD	f50  = [AO3], LDA
	(p16) LDFD	f52  = [AO5], LDA
	(p16) LDFD	f54  = [AO7], LDA
	;;
	(p16) LDFD	f49  = [AO2], LDA
	(p16) LDFD	f51  = [AO4], LDA
	(p16) LDFD	f53  = [AO6], LDA
	(p16) LDFD	f55  = [AO8], LDA
	;;
	(p16) LDFD	f56  = [AO1], LDA
	(p16) LDFD	f58  = [AO3], LDA
	(p16) LDFD	f60  = [AO5], LDA
	(p16) LDFD	f62  = [AO7], LDA
	;;
	(p16) LDFD	f57  = [AO2], LDA
	(p16) LDFD	f59  = [AO4], LDA
	(p16) LDFD	f61  = [AO6], LDA
	(p16) LDFD	f63  = [AO8], LDA
	;;
	(p16) LDFD	f64  = [AO1], LDA
	(p16) LDFD	f66  = [AO3], LDA
	(p16) LDFD	f68  = [AO5], LDA
	(p16) LDFD	f70  = [AO7], LDA
	;;
	(p16) LDFD	f65  = [AO2], LDA
	(p16) LDFD	f67  = [AO4], LDA
	(p16) LDFD	f69  = [AO6], LDA
	(p16) LDFD	f71  = [AO8], LDA
	;;
	(p16) LDFD	f72  = [AO1], LDA
	(p16) LDFD	f74  = [AO3], LDA
	(p16) LDFD	f76  = [AO5], LDA
	(p16) LDFD	f78  = [AO7], LDA
	;;
	(p16) LDFD	f73  = [AO2], LDA
	(p16) LDFD	f75  = [AO4], LDA
	(p16) LDFD	f77  = [AO6], LDA
	(p16) LDFD	f79  = [AO8], LDA
	;;
	(p16) LDFD	f80  = [AO1], LDA
	(p16) LDFD	f82  = [AO3], LDA
	(p16) LDFD	f84  = [AO5], LDA
	(p16) LDFD	f86  = [AO7], LDA
	;;
	(p16) LDFD	f81  = [AO2], LDA
	(p16) LDFD	f83  = [AO4], LDA
	(p16) LDFD	f85  = [AO6], LDA
	(p16) LDFD	f87  = [AO8], LDA
	;;
	(p16) LDFD	f88  = [AO1], LDA7M8
	(p16) LDFD	f90  = [AO3], LDA7M8
	(p16) LDFD	f92  = [AO5], LDA7M8
	(p16) LDFD	f94  = [AO7], LDA7M8
	;;
	(p16) LDFD	f89  = [AO2], LDA7M8
	(p16) LDFD	f91  = [AO4], LDA7M8
	(p16) LDFD	f93  = [AO6], LDA7M8
	(p16) LDFD	f95  = [AO8], LDA7M8
	;;
	(p16) LDFD	f96  = [X1], 1 * SIZE
	(p16) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [X1], 3 * SIZE
	(p16) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [X1], 1 * SIZE
	(p16) LDFD	f102 = [X2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [X1], 3 * SIZE
	(p16) LDFD	f103 = [X2], 3 * SIZE
	;;
	(p16) FMA	f8  = f96,  f32, f8
	(p16) FMA	f9  = f96,  f40, f9
	(p16) FMA	f10 = f96,  f48, f10
	(p16) FMA	f11 = f96,  f56, f11
	(p16) FMA	f12 = f96,  f64, f12
	(p16) FMA	f13 = f96,  f72, f13
	(p16) FMA	f14 = f96,  f80, f14
	(p16) FMA	f15 = f96,  f88, f15
	;;
	(p16) FMA	f8  = f97,  f33, f8
	(p16) FMA	f9  = f97,  f41, f9
	(p16) FMA	f10 = f97,  f49, f10
	(p16) FMA	f11 = f97,  f57, f11
	(p16) FMA	f12 = f97,  f65, f12
	(p16) FMA	f13 = f97,  f73, f13
	(p16) FMA	f14 = f97,  f81, f14
	(p16) FMA	f15 = f97,  f89, f15
	;;
	(p16) FMA	f8  = f98,  f34, f8
	(p16) FMA	f9  = f98,  f42, f9
	(p16) FMA	f10 = f98,  f50, f10
	(p16) FMA	f11 = f98,  f58, f11
	(p16) FMA	f12 = f98,  f66, f12
	(p16) FMA	f13 = f98,  f74, f13
	(p16) FMA	f14 = f98,  f82, f14
	(p16) FMA	f15 = f98,  f90, f15
	;;
	(p16) FMA	f8  = f99,  f35, f8
	(p16) FMA	f9  = f99,  f43, f9
	(p16) FMA	f10 = f99,  f51, f10
	(p16) FMA	f11 = f99,  f59, f11
	(p16) FMA	f12 = f99,  f67, f12
	(p16) FMA	f13 = f99,  f75, f13
	(p16) FMA	f14 = f99,  f83, f14
	(p16) FMA	f15 = f99,  f91, f15
	;;
	(p16) FMA	f8  = f100, f36, f8
	(p16) FMA	f9  = f100, f44, f9
	(p16) FMA	f10 = f100, f52, f10
	(p16) FMA	f11 = f100, f60, f11
	(p16) FMA	f12 = f100, f68, f12
	(p16) FMA	f13 = f100, f76, f13
	(p16) FMA	f14 = f100, f84, f14
	(p16) FMA	f15 = f100, f92, f15
	;;
	(p16) FMA	f8  = f101, f37, f8
	(p16) FMA	f9  = f101, f45, f9
	(p16) FMA	f10 = f101, f53, f10
	(p16) FMA	f11 = f101, f61, f11
	(p16) FMA	f12 = f101, f69, f12
	(p16) FMA	f13 = f101, f77, f13
	(p16) FMA	f14 = f101, f85, f14
	(p16) FMA	f15 = f101, f93, f15
	;;
	(p16) FMA	f8  = f102, f38, f8
	(p16) FMA	f9  = f102, f46, f9
	(p16) FMA	f10 = f102, f54, f10
	(p16) FMA	f11 = f102, f62, f11
	(p16) FMA	f12 = f102, f70, f12
	(p16) FMA	f13 = f102, f78, f13
	(p16) FMA	f14 = f102, f86, f14
	(p16) FMA	f15 = f102, f94, f15
	;;
	(p16) FMA	f8  = f103, f39, f8
	(p16) FMA	f9  = f103, f47, f9
	(p16) FMA	f10 = f103, f55, f10
	(p16) FMA	f11 = f103, f63, f11
	(p16) FMA	f12 = f103, f71, f12
	(p16) FMA	f13 = f103, f79, f13
	(p16) FMA	f14 = f103, f87, f14
	(p16) FMA	f15 = f103, f95, f15
	br.ctop.sptk.few .L12
	;;
	.align 16

.L15:
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	;;
	{ .mmi
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	}
	;;
	{ .mmi
	(p13) LDFD	f32  = [AO1], LDA
	(p13) LDFD	f34  = [AO3], LDA
	tbit.nz	p15, p0  = M, 0
	}
	{ .mmi
	(p14) LDFD	f36  = [AO5], LDA
	(p11) adds	AO6  = - 4 * SIZE, AO6
	(p12) adds	AO7  = - 2 * SIZE, AO7
	}
	;;
	(p13) LDFD	f33  = [AO2], LDA
	(p13) LDFD	f35  = [AO4], LDA
	(p14) LDFD	f37  = [AO6], LDA
	(p15) LDFD	f38  = [AO7], LDA
	;;
	(p13) LDFD	f40  = [AO1], LDA
	(p13) LDFD	f42  = [AO3], LDA
	(p14) LDFD	f44  = [AO5], LDA
	(p15) LDFD	f46  = [AO7], LDA
	;;
	(p13) LDFD	f41  = [AO2], LDA
	(p13) LDFD	f43  = [AO4], LDA
	(p14) LDFD	f45  = [AO6], LDA
	;;
	(p13) LDFD	f48  = [AO1], LDA
	(p13) LDFD	f50  = [AO3], LDA
	(p14) LDFD	f52  = [AO5], LDA
	(p15) LDFD	f54  = [AO7], LDA
	;;
	(p13) LDFD	f49  = [AO2], LDA
	(p13) LDFD	f51  = [AO4], LDA
	(p14) LDFD	f53  = [AO6], LDA
	;;
	(p13) LDFD	f56  = [AO1], LDA
	(p13) LDFD	f58  = [AO3], LDA
	(p14) LDFD	f60  = [AO5], LDA
	(p15) LDFD	f62  = [AO7], LDA
	;;
	(p13) LDFD	f57  = [AO2], LDA
	(p13) LDFD	f59  = [AO4], LDA
	(p14) LDFD	f61  = [AO6], LDA
	;;
	(p13) LDFD	f64  = [AO1], LDA
	(p13) LDFD	f66  = [AO3], LDA
	(p14) LDFD	f68  = [AO5], LDA
	(p15) LDFD	f70  = [AO7], LDA
	;;
	(p13) LDFD	f65  = [AO2], LDA
	(p13) LDFD	f67  = [AO4], LDA
	(p14) LDFD	f69  = [AO6], LDA
	;;
	(p13) LDFD	f72  = [AO1], LDA
	(p13) LDFD	f74  = [AO3], LDA
	(p14) LDFD	f76  = [AO5], LDA
	(p15) LDFD	f78  = [AO7], LDA
	;;
	(p13) LDFD	f73  = [AO2], LDA
	(p13) LDFD	f75  = [AO4], LDA
	(p14) LDFD	f77  = [AO6], LDA
	;;
	(p13) LDFD	f80  = [AO1], LDA
	(p13) LDFD	f82  = [AO3], LDA
	(p14) LDFD	f84  = [AO5], LDA
	(p15) LDFD	f86  = [AO7], LDA
	;;
	(p13) LDFD	f81  = [AO2], LDA
	(p13) LDFD	f83  = [AO4], LDA
	(p14) LDFD	f85  = [AO6], LDA
	;;
	(p13) LDFD	f88  = [AO1]
	(p13) LDFD	f90  = [AO3]
	(p14) LDFD	f92  = [AO5]
	(p15) LDFD	f94  = [AO7]
	;;
	(p13) LDFD	f89  = [AO2]
	(p13) LDFD	f91  = [AO4]
	(p14) LDFD	f93  = [AO6]
	;;
	(p13) LDFD	f96  = [X1], 1 * SIZE
	(p13) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [X1], 3 * SIZE
	(p13) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [X1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [X1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [X1], 1 * SIZE
	;;
	(p13) FMA	f8  = f96,  f32, f8
	(p13) FMA	f9  = f96,  f40, f9
	(p13) FMA	f10 = f96,  f48, f10
	(p13) FMA	f11 = f96,  f56, f11
	(p13) FMA	f12 = f96,  f64, f12
	(p13) FMA	f13 = f96,  f72, f13
	(p13) FMA	f14 = f96,  f80, f14
	(p13) FMA	f15 = f96,  f88, f15
	;;
	(p13) FMA	f8  = f97,  f33, f8
	(p13) FMA	f9  = f97,  f41, f9
	(p13) FMA	f10 = f97,  f49, f10
	(p13) FMA	f11 = f97,  f57, f11
	(p13) FMA	f12 = f97,  f65, f12
	(p13) FMA	f13 = f97,  f73, f13
	(p13) FMA	f14 = f97,  f81, f14
	(p13) FMA	f15 = f97,  f89, f15
	;;
	(p13) FMA	f8  = f98,  f34, f8
	(p13) FMA	f9  = f98,  f42, f9
	(p13) FMA	f10 = f98,  f50, f10
	(p13) FMA	f11 = f98,  f58, f11
	(p13) FMA	f12 = f98,  f66, f12
	(p13) FMA	f13 = f98,  f74, f13
	(p13) FMA	f14 = f98,  f82, f14
	(p13) FMA	f15 = f98,  f90, f15
	;;
	(p13) FMA	f8  = f99,  f35, f8
	(p13) FMA	f9  = f99,  f43, f9
	(p13) FMA	f10 = f99,  f51, f10
	(p13) FMA	f11 = f99,  f59, f11
	(p13) FMA	f12 = f99,  f67, f12
	(p13) FMA	f13 = f99,  f75, f13
	(p13) FMA	f14 = f99,  f83, f14
	(p13) FMA	f15 = f99,  f91, f15
	;;
	(p14) FMA	f8  = f100, f36, f8
	(p14) FMA	f9  = f100, f44, f9
	(p14) FMA	f10 = f100, f52, f10
	(p14) FMA	f11 = f100, f60, f11
	(p14) FMA	f12 = f100, f68, f12
	(p14) FMA	f13 = f100, f76, f13
	(p14) FMA	f14 = f100, f84, f14
	(p14) FMA	f15 = f100, f92, f15
	;;
	(p14) FMA	f8  = f101, f37, f8
	(p14) FMA	f9  = f101, f45, f9
	(p14) FMA	f10 = f101, f53, f10
	(p14) FMA	f11 = f101, f61, f11
	(p14) FMA	f12 = f101, f69, f12
	(p14) FMA	f13 = f101, f77, f13
	(p14) FMA	f14 = f101, f85, f14
	(p14) FMA	f15 = f101, f93, f15
	;;
	(p15) FMA	f8  = f102, f38, f8
	(p15) FMA	f9  = f102, f46, f9
	(p15) FMA	f10 = f102, f54, f10
	(p15) FMA	f11 = f102, f62, f11
	(p15) FMA	f12 = f102, f70, f12
	(p15) FMA	f13 = f102, f78, f13
	(p15) FMA	f14 = f102, f86, f14
	(p15) FMA	f15 = f102, f94, f15
	;;
	LDFD	f32 = [Y1], INCY
	;;
	LDFD	f33 = [Y1], INCY
	;;
	LDFD	f34 = [Y1], INCY
	;;
	LDFD	f35 = [Y1], INCY5
	;;
	LDFD	f36 = [Y2], INCY
	;;
	LDFD	f37 = [Y2], INCY
	;;
	LDFD	f38 = [Y2], INCY
	;;
	LDFD	f39 = [Y2], INCY5
	;;
	FMA	f32 = ALPHA, f8,  f32
	FMA	f33 = ALPHA, f9,  f33
	FMA	f34 = ALPHA, f10, f34
	FMA	f35 = ALPHA, f11, f35
	FMA	f36 = ALPHA, f12, f36
	FMA	f37 = ALPHA, f13, f37
	FMA	f38 = ALPHA, f14, f38
	FMA	f39 = ALPHA, f15, f39
	;;
	STFD [YY1] = f32
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f33
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f34
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f35
	add	YY1 = YY1, INCY5
	;;
	STFD [YY2] = f36
	add	YY2 = YY2, INCY
	;;
	STFD [YY2] = f37
	add	YY2 = YY2, INCY
	;;
	STFD [YY2] = f38
	add	YY2 = YY2, INCY
	;;
	STFD [YY2] = f39
	add	YY2 = YY2, INCY5
	;;
	adds	J = -1, J
	;;
	cmp.lt p6, p0 = 0, J
	(p6) br.cond.dptk .L11
	;;
	.align 16

.L20:
	tbit.z	p6, p0  = N, 2
	;;
	(p6) br.cond.dpnt .L30
	;;
	mov	AO1 = A
	adds	AO2 = 1 * SIZE, A
	adds	AO3 = 2 * SIZE, A
	adds	AO4 = 3 * SIZE, A
	adds	AO5 = 4 * SIZE, A
	adds	AO6 = 5 * SIZE, A
	adds	AO7 = 6 * SIZE, A
	adds	AO8 = 7 * SIZE, A
	shladd	A   = LDA, 2, A
	;;
	shladd	LDA7M8 = LDA, 2, r0
	;;
	sub	LDA7M8 = LDA, LDA7M8
	;;
	adds	LDA7M8 = 8 * SIZE, LDA7M8
	;;
	mov	f8  = f0
	mov	f9  = f0
	mov	f10 = f0
	mov	f11 = f0
	mov	f12 = f0
	mov	f13 = f0
	mov	f14 = f0
	mov	f15 = f0

	mov	pr.rot= 0
	shr	I = M, 3
	mov	ar.ec = 2
	;;
	mov	X1  = BUFFER
	adds	X2  = 2 * SIZE, BUFFER
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L25
	;;
	.align 16

.L22:
	(p16) LDFD	f32  = [AO1], LDA
	(p16) LDFD	f34  = [AO3], LDA
	(p16) LDFD	f36  = [AO5], LDA
	(p16) LDFD	f38  = [AO7], LDA
	;;
	(p16) LDFD	f33  = [AO2], LDA
	(p16) LDFD	f35  = [AO4], LDA
	(p16) LDFD	f37  = [AO6], LDA
	(p16) LDFD	f39  = [AO8], LDA
	;;
	(p16) LDFD	f40  = [AO1], LDA
	(p16) LDFD	f42  = [AO3], LDA
	(p16) LDFD	f44  = [AO5], LDA
	(p16) LDFD	f46  = [AO7], LDA
	;;
	(p16) LDFD	f41  = [AO2], LDA
	(p16) LDFD	f43  = [AO4], LDA
	(p16) LDFD	f45  = [AO6], LDA
	(p16) LDFD	f47  = [AO8], LDA
	;;
	(p16) LDFD	f48  = [AO1], LDA
	(p16) LDFD	f50  = [AO3], LDA
	(p16) LDFD	f52  = [AO5], LDA
	(p16) LDFD	f54  = [AO7], LDA
	;;
	(p16) LDFD	f49  = [AO2], LDA
	(p16) LDFD	f51  = [AO4], LDA
	(p16) LDFD	f53  = [AO6], LDA
	(p16) LDFD	f55  = [AO8], LDA
	;;
	(p16) LDFD	f56  = [AO1], LDA7M8
	(p16) LDFD	f58  = [AO3], LDA7M8
	(p16) LDFD	f60  = [AO5], LDA7M8
	(p16) LDFD	f62  = [AO7], LDA7M8
	;;
	(p16) LDFD	f57  = [AO2], LDA7M8
	(p16) LDFD	f59  = [AO4], LDA7M8
	(p16) LDFD	f61  = [AO6], LDA7M8
	(p16) LDFD	f63  = [AO8], LDA7M8
	;;
	(p16) LDFD	f96  = [X1], 1 * SIZE
	(p16) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [X1], 3 * SIZE
	(p16) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [X1], 1 * SIZE
	(p16) LDFD	f102 = [X2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [X1], 3 * SIZE
	(p16) LDFD	f103 = [X2], 3 * SIZE
	;;
	(p16) FMA	f8  = f96,  f32, f8
	(p16) FMA	f9  = f96,  f40, f9
	(p16) FMA	f10 = f96,  f48, f10
	(p16) FMA	f11 = f96,  f56, f11
	;;
	(p16) FMA	f8  = f97,  f33, f8
	(p16) FMA	f9  = f97,  f41, f9
	(p16) FMA	f10 = f97,  f49, f10
	(p16) FMA	f11 = f97,  f57, f11
	;;
	(p16) FMA	f8  = f98,  f34, f8
	(p16) FMA	f9  = f98,  f42, f9
	(p16) FMA	f10 = f98,  f50, f10
	(p16) FMA	f11 = f98,  f58, f11
	;;
	(p16) FMA	f8  = f99,  f35, f8
	(p16) FMA	f9  = f99,  f43, f9
	(p16) FMA	f10 = f99,  f51, f10
	(p16) FMA	f11 = f99,  f59, f11
	;;
	(p16) FMA	f8  = f100, f36, f8
	(p16) FMA	f9  = f100, f44, f9
	(p16) FMA	f10 = f100, f52, f10
	(p16) FMA	f11 = f100, f60, f11

	;;
	(p16) FMA	f8  = f101, f37, f8
	(p16) FMA	f9  = f101, f45, f9
	(p16) FMA	f10 = f101, f53, f10
	(p16) FMA	f11 = f101, f61, f11
	;;
	(p16) FMA	f8  = f102, f38, f8
	(p16) FMA	f9  = f102, f46, f9
	(p16) FMA	f10 = f102, f54, f10
	(p16) FMA	f11 = f102, f62, f11
	;;
	(p16) FMA	f8  = f103, f39, f8
	(p16) FMA	f9  = f103, f47, f9
	(p16) FMA	f10 = f103, f55, f10
	(p16) FMA	f11 = f103, f63, f11
	br.ctop.sptk.few .L22
	;;
	.align 16

.L25:
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	;;
	{ .mmi
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	}
	;;
	{ .mmi
	(p13) LDFD	f32  = [AO1], LDA
	(p13) LDFD	f34  = [AO3], LDA
	tbit.nz	p15, p0  = M, 0
	}
	{ .mmi
	(p14) LDFD	f36  = [AO5], LDA
	(p11) adds	AO6  = - 4 * SIZE, AO6
	(p12) adds	AO7  = - 2 * SIZE, AO7
	}
	;;
	(p13) LDFD	f33  = [AO2], LDA
	(p13) LDFD	f35  = [AO4], LDA
	(p14) LDFD	f37  = [AO6], LDA
	(p15) LDFD	f38  = [AO7], LDA
	;;
	(p13) LDFD	f40  = [AO1], LDA
	(p13) LDFD	f42  = [AO3], LDA
	(p14) LDFD	f44  = [AO5], LDA
	(p15) LDFD	f46  = [AO7], LDA
	;;
	(p13) LDFD	f41  = [AO2], LDA
	(p13) LDFD	f43  = [AO4], LDA
	(p14) LDFD	f45  = [AO6], LDA
	;;
	(p13) LDFD	f48  = [AO1], LDA
	(p13) LDFD	f50  = [AO3], LDA
	(p14) LDFD	f52  = [AO5], LDA
	(p15) LDFD	f54  = [AO7], LDA
	;;
	(p13) LDFD	f49  = [AO2], LDA
	(p13) LDFD	f51  = [AO4], LDA
	(p14) LDFD	f53  = [AO6], LDA
	;;
	(p13) LDFD	f56  = [AO1]
	(p13) LDFD	f58  = [AO3]
	(p14) LDFD	f60  = [AO5]
	(p15) LDFD	f62  = [AO7]
	;;
	(p13) LDFD	f57  = [AO2]
	(p13) LDFD	f59  = [AO4]
	(p14) LDFD	f61  = [AO6]
	;;
	(p13) LDFD	f96  = [X1], 1 * SIZE
	(p13) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [X1], 3 * SIZE
	(p13) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [X1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [X1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [X1], 1 * SIZE
	;;
	(p13) FMA	f8  = f96,  f32, f8
	(p13) FMA	f9  = f96,  f40, f9
	(p13) FMA	f10 = f96,  f48, f10
	(p13) FMA	f11 = f96,  f56, f11
	;;
	(p13) FMA	f8  = f97,  f33, f8
	(p13) FMA	f9  = f97,  f41, f9
	(p13) FMA	f10 = f97,  f49, f10
	(p13) FMA	f11 = f97,  f57, f11
	;;
	(p13) FMA	f8  = f98,  f34, f8
	(p13) FMA	f9  = f98,  f42, f9
	(p13) FMA	f10 = f98,  f50, f10
	(p13) FMA	f11 = f98,  f58, f11
	;;
	(p13) FMA	f8  = f99,  f35, f8
	(p13) FMA	f9  = f99,  f43, f9
	(p13) FMA	f10 = f99,  f51, f10
	(p13) FMA	f11 = f99,  f59, f11
	;;
	(p14) FMA	f8  = f100, f36, f8
	(p14) FMA	f9  = f100, f44, f9
	(p14) FMA	f10 = f100, f52, f10
	(p14) FMA	f11 = f100, f60, f11
	;;
	(p14) FMA	f8  = f101, f37, f8
	(p14) FMA	f9  = f101, f45, f9
	(p14) FMA	f10 = f101, f53, f10
	(p14) FMA	f11 = f101, f61, f11
	;;
	(p15) FMA	f8  = f102, f38, f8
	(p15) FMA	f9  = f102, f46, f9
	(p15) FMA	f10 = f102, f54, f10
	(p15) FMA	f11 = f102, f62, f11
	;;
	LDFD	f32 = [Y1], INCY
	;;
	LDFD	f33 = [Y1], INCY
	;;
	LDFD	f34 = [Y1], INCY
	;;
	LDFD	f35 = [Y1], INCY
	;;
	FMA	f32 = ALPHA, f8,  f32
	FMA	f33 = ALPHA, f9,  f33
	FMA	f34 = ALPHA, f10, f34
	FMA	f35 = ALPHA, f11, f35
	;;
	STFD [YY1] = f32
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f33
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f34
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f35
	add	YY1 = YY1, INCY
	;;
	.align 16

.L30:
	tbit.z	p6, p0  = N, 1
	;;
	(p6) br.cond.dpnt .L40
	;;
	mov	AO1 = A
	adds	AO2 = 1 * SIZE, A
	adds	AO3 = 2 * SIZE, A
	adds	AO4 = 3 * SIZE, A
	adds	AO5 = 4 * SIZE, A
	adds	AO6 = 5 * SIZE, A
	adds	AO7 = 6 * SIZE, A
	adds	AO8 = 7 * SIZE, A
	shladd	A   = LDA, 1, A
	;;
	shladd	LDA7M8 = LDA, 1, r0
	;;
	sub	LDA7M8 = LDA, LDA7M8
	;;
	adds	LDA7M8 = 8 * SIZE, LDA7M8
	;;
	mov	f8  = f0
	mov	f9  = f0
	mov	f10 = f0
	mov	f11 = f0
	mov	f12 = f0
	mov	f13 = f0
	mov	f14 = f0
	mov	f15 = f0

	mov	pr.rot= 0
	shr	I = M, 3
	mov	ar.ec = 2
	;;
	mov	X1  = BUFFER
	adds	X2  = 2 * SIZE, BUFFER
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L35
	;;
	.align 16

.L32:
	(p16) LDFD	f32  = [AO1], LDA
	(p16) LDFD	f34  = [AO3], LDA
	(p16) LDFD	f36  = [AO5], LDA
	(p16) LDFD	f38  = [AO7], LDA
	;;
	(p16) LDFD	f33  = [AO2], LDA
	(p16) LDFD	f35  = [AO4], LDA
	(p16) LDFD	f37  = [AO6], LDA
	(p16) LDFD	f39  = [AO8], LDA
	;;
	(p16) LDFD	f40  = [AO1], LDA7M8
	(p16) LDFD	f42  = [AO3], LDA7M8
	(p16) LDFD	f44  = [AO5], LDA7M8
	(p16) LDFD	f46  = [AO7], LDA7M8
	;;
	(p16) LDFD	f41  = [AO2], LDA7M8
	(p16) LDFD	f43  = [AO4], LDA7M8
	(p16) LDFD	f45  = [AO6], LDA7M8
	(p16) LDFD	f47  = [AO8], LDA7M8
	;;
	(p16) LDFD	f96  = [X1], 1 * SIZE
	(p16) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [X1], 3 * SIZE
	(p16) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [X1], 1 * SIZE
	(p16) LDFD	f102 = [X2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [X1], 3 * SIZE
	(p16) LDFD	f103 = [X2], 3 * SIZE
	;;
	(p16) FMA	f8  = f96,  f32, f8
	(p16) FMA	f9  = f96,  f40, f9
	;;
	(p16) FMA	f8  = f97,  f33, f8
	(p16) FMA	f9  = f97,  f41, f9
	;;
	(p16) FMA	f8  = f98,  f34, f8
	(p16) FMA	f9  = f98,  f42, f9
	;;
	(p16) FMA	f8  = f99,  f35, f8
	(p16) FMA	f9  = f99,  f43, f9
	;;
	(p16) FMA	f8  = f100, f36, f8
	(p16) FMA	f9  = f100, f44, f9
	;;
	(p16) FMA	f8  = f101, f37, f8
	(p16) FMA	f9  = f101, f45, f9
	;;
	(p16) FMA	f8  = f102, f38, f8
	(p16) FMA	f9  = f102, f46, f9
	;;
	(p16) FMA	f8  = f103, f39, f8
	(p16) FMA	f9  = f103, f47, f9
	br.ctop.sptk.few .L32
	;;
	.align 16

.L35:
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	;;
	{ .mmi
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	}
	;;
	{ .mmi
	(p13) LDFD	f32  = [AO1], LDA
	(p13) LDFD	f34  = [AO3], LDA
	tbit.nz	p15, p0  = M, 0
	}
	{ .mmi
	(p14) LDFD	f36  = [AO5], LDA
	(p11) adds	AO6  = - 4 * SIZE, AO6
	(p12) adds	AO7  = - 2 * SIZE, AO7
	}
	;;
	(p13) LDFD	f33  = [AO2], LDA
	(p13) LDFD	f35  = [AO4], LDA
	(p14) LDFD	f37  = [AO6], LDA
	(p15) LDFD	f38  = [AO7], LDA
	;;
	(p13) LDFD	f40  = [AO1]
	(p13) LDFD	f42  = [AO3]
	(p14) LDFD	f44  = [AO5]
	(p15) LDFD	f46  = [AO7]
	;;
	(p13) LDFD	f41  = [AO2]
	(p13) LDFD	f43  = [AO4]
	(p14) LDFD	f45  = [AO6]
	;;
	(p13) LDFD	f96  = [X1], 1 * SIZE
	(p13) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [X1], 3 * SIZE
	(p13) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [X1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [X1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [X1], 1 * SIZE
	;;
	(p13) FMA	f8  = f96,  f32, f8
	(p13) FMA	f9  = f96,  f40, f9
	;;
	(p13) FMA	f8  = f97,  f33, f8
	(p13) FMA	f9  = f97,  f41, f9
	;;
	(p13) FMA	f8  = f98,  f34, f8
	(p13) FMA	f9  = f98,  f42, f9
	;;
	(p13) FMA	f8  = f99,  f35, f8
	(p13) FMA	f9  = f99,  f43, f9
	;;
	(p14) FMA	f8  = f100, f36, f8
	(p14) FMA	f9  = f100, f44, f9
	;;
	(p14) FMA	f8  = f101, f37, f8
	(p14) FMA	f9  = f101, f45, f9
	;;
	(p15) FMA	f8  = f102, f38, f8
	(p15) FMA	f9  = f102, f46, f9
	;;
	LDFD	f32 = [Y1], INCY
	;;
	LDFD	f33 = [Y1], INCY
	;;
	FMA	f32 = ALPHA, f8,  f32
	FMA	f33 = ALPHA, f9,  f33
	;;
	STFD [YY1] = f32
	add	YY1 = YY1, INCY
	;;
	STFD [YY1] = f33
	add	YY1 = YY1, INCY
	;;
	.align 16

.L40:
	tbit.z	p6, p0  = N, 0
	;;
	(p6) br.cond.dpnt .L999
	;;
	mov	AO1 = A
	adds	AO2 = 1 * SIZE, A
	adds	AO3 = 2 * SIZE, A
	adds	AO4 = 3 * SIZE, A
	adds	AO5 = 4 * SIZE, A
	adds	AO6 = 5 * SIZE, A
	adds	AO7 = 6 * SIZE, A
	adds	AO8 = 7 * SIZE, A
	add	A   = LDA, A
	;;
	mov	f8  = f0
	mov	f9  = f0
	mov	f10 = f0
	mov	f11 = f0
	mov	f12 = f0
	mov	f13 = f0
	mov	f14 = f0
	mov	f15 = f0

	mov	pr.rot= 0
	shr	I = M, 3
	mov	ar.ec = 2
	;;
	mov	X1  = BUFFER
	adds	X2  = 2 * SIZE, BUFFER
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L45
	;;
	.align 16

.L42:
	(p16) LDFD	f32  = [AO1], 8 * SIZE
	(p16) LDFD	f34  = [AO3], 8 * SIZE
	(p16) LDFD	f36  = [AO5], 8 * SIZE
	(p16) LDFD	f38  = [AO7], 8 * SIZE
	;;
	(p16) LDFD	f33  = [AO2], 8 * SIZE
	(p16) LDFD	f35  = [AO4], 8 * SIZE
	(p16) LDFD	f37  = [AO6], 8 * SIZE
	(p16) LDFD	f39  = [AO8], 8 * SIZE
	;;
	(p16) LDFD	f96  = [X1], 1 * SIZE
	(p16) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [X1], 3 * SIZE
	(p16) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [X1], 1 * SIZE
	(p16) LDFD	f102 = [X2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [X1], 3 * SIZE
	(p16) LDFD	f103 = [X2], 3 * SIZE
	;;
	(p16) FMA	f8  = f96,  f32, f8
	;;
	(p16) FMA	f8  = f97,  f33, f8
	;;
	(p16) FMA	f8  = f98,  f34, f8
	;;
	(p16) FMA	f8  = f99,  f35, f8
	;;
	(p16) FMA	f8  = f100, f36, f8
	;;
	(p16) FMA	f8  = f101, f37, f8
	;;
	(p16) FMA	f8  = f102, f38, f8
	;;
	(p16) FMA	f8  = f103, f39, f8
	br.ctop.sptk.few .L42
	;;
	.align 16

.L45:
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	;;
	{ .mmi
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	}
	;;
	{ .mmi
	(p13) LDFD	f32  = [AO1]
	(p13) LDFD	f34  = [AO3]
	tbit.nz	p15, p0  = M, 0
	}
	{ .mmi
	(p14) LDFD	f36  = [AO5]
	(p11) adds	AO6  = - 4 * SIZE, AO6
	(p12) adds	AO7  = - 2 * SIZE, AO7
	}
	;;
	(p13) LDFD	f33  = [AO2]
	(p13) LDFD	f35  = [AO4]
	(p14) LDFD	f37  = [AO6]
	(p15) LDFD	f38  = [AO7]
	;;
	(p13) LDFD	f96  = [X1], 1 * SIZE
	(p13) LDFD	f98  = [X2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [X1], 3 * SIZE
	(p13) LDFD	f99  = [X2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [X1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [X1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [X1], 1 * SIZE
	;;
	(p13) FMA	f8  = f96,  f32, f8
	;;
	(p13) FMA	f8  = f97,  f33, f8
	;;
	(p13) FMA	f8  = f98,  f34, f8
	;;
	(p13) FMA	f8  = f99,  f35, f8
	;;
	(p14) FMA	f8  = f100, f36, f8
	;;
	(p14) FMA	f8  = f101, f37, f8
	;;
	(p15) FMA	f8  = f102, f38, f8
	;;
	LDFD	f32 = [Y1], INCY
	;;
	FMA	f32 = ALPHA, f8,  f32
	;;
	STFD [YY1] = f32
	.align 16

.L999:
	mov	 ar.lc = ARLC
	mov pr    = PR, -1
	br.ret.sptk.many b0
	;;
	EPILOGUE
