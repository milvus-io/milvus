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
#define X	r38
#define INCX	r39
#define Y	r34
#define INCY	r35
#else
#define A	r38
#define LDA	r39
#define X	r34
#define INCX	r35
#define Y	r36
#define INCY	r37
#endif

#define BUFFER	r11

#define I	r14
#define J	r15
#define AO1	r16
#define AO2	r17
#define AO3	r18
#define AO4	r19
#define AO5	r20
#define AO6	r21
#define AO7	r22
#define AO8	r23
#define YLD1	r24
#define YLD2	r25
#define YST1	r26
#define YST2	r27
#define II	r28
#define YY	r29

#define ARLC	r30
#define PR	r31

#define LDA7M8	r8
#define PREA	r9
#define PREB	r10

#define ALPHA1	f8
#define ALPHA2	f9
#define ALPHA3	f10
#define ALPHA4	f11
#define ALPHA5	f12
#define ALPHA6	f13
#define ALPHA7	f14
#define ALPHA8	f15

#define RPREFETCHSIZE	( 8 * 1 +  6)
#define WPREFETCHSIZE	( 8 * 1 +  6)

#define RPREFETCH	lfetch.nt1
#define WPREFETCH	lfetch.excl.nt1

#define ALPHA	f6

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	mov	ARLC  = ar.lc
	}
	;;
	mov	PR = pr
	adds	r14 = 16, SP
	adds	r15 = 24, SP
	adds	r16 = 32, SP
	.body
	;;

#ifdef XDOUBLE
	ld8	X      = [r14], 16
	ld8	INCX   = [r15], 16
	;;
#endif
	ld8	Y      = [r14], 16
	ld8	INCY   = [r15], 16
	;;
	ld8	BUFFER = [r14]
	;;

	mov	ALPHA = f8
	cmp.ge	p7, p0 = 0, M
	cmp.ge	p6, p0 = 0, N
	;;
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	LDA  = LDA,  BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	;;
	(p7) br.cond.dpnt .L999
	(p6) br.cond.dpnt .L999
	;;
	sub	I = A, Y
	mov	YY = Y
	;;
	cmp.eq	p10, p0 = SIZE, INCY
	(p10) br.cond.dptk .L10
	;;
	shr	J = M, 3
	mov	YY = BUFFER
	;;
	(p8) adds  YY = SIZE, BUFFER
	;;
	mov	ar.lc = J
	mov	YST1 = YY
	adds	YST2 = 4 * SIZE, YY
	;;
.L02:
	STFD	[YST1] = f0, 1 * SIZE
	STFD	[YST2] = f0, 1 * SIZE
	;;
	STFD	[YST1] = f0, 1 * SIZE
	STFD	[YST2] = f0, 1 * SIZE
	;;
	STFD	[YST1] = f0, 1 * SIZE
	STFD	[YST2] = f0, 1 * SIZE
	;;
	STFD	[YST1] = f0, 5 * SIZE
	STFD	[YST2] = f0, 5 * SIZE
	br.cloop.sptk.few .L02
	;;

.L10:
	shr	J   = N, 3
	;;
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L20
	;;
	.align 16

.L11:
	shladd	LDA7M8 = LDA, 3, r0
	;;
	sub	LDA7M8 = LDA, LDA7M8
	;;
	adds	LDA7M8 = 8 * SIZE, LDA7M8
	;;
	mov	YLD1 = YY
	mov	YST1 = YY
	adds	YLD2 = 1 * SIZE, YY
	adds	YST2 = 1 * SIZE, YY
	;;
	LDFD	ALPHA1 = [X], INCX
	;;
	LDFD	ALPHA2 = [X], INCX
	;;
	LDFD	ALPHA3 = [X], INCX
	;;
	LDFD	ALPHA4 = [X], INCX
	;;
	LDFD	ALPHA5 = [X], INCX
	;;
	LDFD	ALPHA6 = [X], INCX
	;;
	LDFD	ALPHA7 = [X], INCX
	;;
	LDFD	ALPHA8 = [X], INCX
	;;
	FMPY	ALPHA1 = ALPHA, ALPHA1
	FMPY	ALPHA2 = ALPHA, ALPHA2
	FMPY	ALPHA3 = ALPHA, ALPHA3
	FMPY	ALPHA4 = ALPHA, ALPHA4
	FMPY	ALPHA5 = ALPHA, ALPHA5
	FMPY	ALPHA6 = ALPHA, ALPHA6
	;;
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
	shr	I = M, 3
	mov	pr.rot= 0
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	adds J = -1, J
	;;
	adds	PREB = (WPREFETCHSIZE) * SIZE, YY
	;;
	cmp.lt p7, p8 = r0, J
	tbit.nz	p13, p11 = M, 2
	mov	ar.ec= 2
	;;
	FMPY	ALPHA7 = ALPHA, ALPHA7
	;;
	{ .mfi
	and	II = 7, M
	FMPY	ALPHA8 = ALPHA, ALPHA8
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p6, p0 = -1, I
	tbit.nz	p14, p12 = M, 1
	(p6) br.cond.dpnt .L15
	}
	;;
	.align 16

.L12:
	{ .mmf
	(p17) LDFD	f93  = [AO5], LDA7M8
	(p17) LDFD	f94  = [AO6], LDA7M8
	(p17) FMA	f101 = ALPHA1,  f33, f101
	}
	{ .mmf
	(p17) LDFD	f95  = [AO7], LDA7M8
	(p17) LDFD	f96  = [AO8], LDA7M8
	(p17) FMA	f104 = ALPHA1,  f34, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f32  = [AO1]
	(p16) LDFD	f33  = [AO2], LDA
	(p17) FMA	f107 = ALPHA1,  f35, f107
	}
	{ .mmf
	(p16) LDFD	f34  = [AO3], LDA
	(p16) LDFD	f35  = [AO4], LDA
	(p17) FMA	f110 = ALPHA1,  f36, f110
	}
	;;
	{ .mmf
	(p16) LDFD	f100  = [YLD1], 2 * SIZE
	(p16) LDFD	f103  = [YLD2], 2 * SIZE
	(p17) FMA	f113 = ALPHA1,  f37, f113
	}
	{ .mmf
	(p16) adds	PREA = (RPREFETCHSIZE) * SIZE, AO1
	(p16) add	AO1 = AO1, LDA
	(p17) FMA	f116 = ALPHA1,  f38, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f102, 2 * SIZE
	(p18) STFD	[YST2] = f105, 2 * SIZE
	(p17) FMA	f119 = ALPHA1,  f39, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA1,  f40, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f36  = [AO5], LDA
	(p16) LDFD	f37  = [AO6], LDA
	(p17) FMA	f101 = ALPHA2,  f41, f101
	}
	{ .mmf
	(p16) LDFD	f38  = [AO7], LDA
	(p16) LDFD	f39  = [AO8], LDA
	(p17) FMA	f104 = ALPHA2,  f42, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f40  = [AO1], LDA
	(p16) LDFD	f41  = [AO2], LDA
	(p17) FMA	f107 = ALPHA2,  f43, f107
	}
	{ .mmf
	(p16) LDFD	f42  = [AO3], LDA
	(p16) LDFD	f43  = [AO4], LDA
	(p17) FMA	f110 = ALPHA2,  f44, f110
	}
	;;
	{ .mmf
	(p16) LDFD	f106  = [YLD1], 2 * SIZE
	(p16) LDFD	f109  = [YLD2], 2 * SIZE
	(p17) FMA	f113 = ALPHA2,  f45, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA2,  f46, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f108, 2 * SIZE
	(p18) STFD	[YST2] = f111, 2 * SIZE
	(p17) FMA	f119 = ALPHA2,  f47, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA2,  f48, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f44  = [AO5], LDA
	(p16) LDFD	f45  = [AO6], LDA
	(p17) FMA	f101 = ALPHA3,  f49, f101
	}
	{ .mmf
	(p16) LDFD	f46  = [AO7], LDA
	(p16) LDFD	f47  = [AO8], LDA
	(p17) FMA	f104 = ALPHA3,  f50, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f48  = [AO1], LDA
	(p16) LDFD	f49  = [AO2], LDA
	(p17) FMA	f107 = ALPHA3,  f51, f107
	}
	{ .mmf
	(p16) LDFD	f50  = [AO3], LDA
	(p16) LDFD	f51  = [AO4], LDA
	(p17) FMA	f110 = ALPHA3,  f52, f110
	}
	;;
	{ .mmf
	(p16) LDFD	f112 = [YLD1], 2 * SIZE
	(p16) LDFD	f115 = [YLD2], 2 * SIZE
	(p17) FMA	f113 = ALPHA3,  f53, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA3,  f54, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f114, 2 * SIZE
	(p18) STFD	[YST2] = f117, 2 * SIZE
	(p17) FMA	f119 = ALPHA3,  f55, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA3,  f56, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f52  = [AO5], LDA
	(p16) LDFD	f53  = [AO6], LDA
	(p17) FMA	f101 = ALPHA4,  f57, f101
	}
	{ .mmf
	(p16) LDFD	f54  = [AO7], LDA
	(p16) LDFD	f55  = [AO8], LDA
	(p17) FMA	f104 = ALPHA4,  f58, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f56  = [AO1], LDA
	(p16) LDFD	f57  = [AO2], LDA
	(p17) FMA	f107 = ALPHA4,  f59, f107
	}
	{ .mmf
	(p16) LDFD	f58  = [AO3], LDA
	(p16) LDFD	f59  = [AO4], LDA
	(p17) FMA	f110 = ALPHA4,  f60, f110
	}
	;;
	{ .mmf
	(p16) LDFD	f118 = [YLD1], 2 * SIZE
	(p16) LDFD	f121 = [YLD2], 2 * SIZE
	(p17) FMA	f113 = ALPHA4,  f61, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA4,  f62, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f120, 2 * SIZE
	(p18) STFD	[YST2] = f123, 2 * SIZE
	(p17) FMA	f119 = ALPHA4,  f63, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA4,  f64, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f60  = [AO5], LDA
	(p16) LDFD	f61  = [AO6], LDA
	(p17) FMA	f101 = ALPHA5,  f65, f101
	}
	{ .mmf
	(p16) LDFD	f62  = [AO7], LDA
	(p16) LDFD	f63  = [AO8], LDA
	(p17) FMA	f104 = ALPHA5,  f66, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f64  = [AO1], LDA
	(p16) LDFD	f65  = [AO2], LDA
	(p17) FMA	f107 = ALPHA5,  f67, f107
	}
	{ .mmf
	(p16) LDFD	f66  = [AO3], LDA
	(p16) LDFD	f67  = [AO4], LDA
	(p17) FMA	f110 = ALPHA5,  f68, f110
	}
	;;
	{ .mmf
	(p16) WPREFETCH [PREB], 8 * SIZE
	nop   __LINE__
	(p17) FMA	f113 = ALPHA5,  f69, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA5,  f70, f116
	}
	;;
	{ .mmf
	(p16) RPREFETCH [PREA]
	nop   __LINE__
	(p17) FMA	f119 = ALPHA5,  f71, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA5,  f72, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f68  = [AO5], LDA
	(p16) LDFD	f69  = [AO6], LDA
	(p17) FMA	f101 = ALPHA6,  f73, f101
	}
	{ .mmf
	(p16) LDFD	f70  = [AO7], LDA
	(p16) LDFD	f71  = [AO8], LDA
	(p17) FMA	f104 = ALPHA6,  f74, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f72  = [AO1], LDA
	(p16) LDFD	f73  = [AO2], LDA
	(p17) FMA	f107 = ALPHA6,  f75, f107
	}
	{ .mmf
	(p16) LDFD	f74  = [AO3], LDA
	(p16) LDFD	f75  = [AO4], LDA
	(p17) FMA	f110 = ALPHA6,  f76, f110
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f113 = ALPHA6,  f77, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA6,  f78, f116
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f119 = ALPHA6,  f79, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA6,  f80, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f76  = [AO5], LDA
	(p16) LDFD	f77  = [AO6], LDA
	(p17) FMA	f101 = ALPHA7,  f81, f101
	}
	{ .mmf
	(p16) LDFD	f78  = [AO7], LDA
	(p16) LDFD	f79  = [AO8], LDA
	(p17) FMA	f104 = ALPHA7,  f82, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f80  = [AO1], LDA
	(p16) LDFD	f81  = [AO2], LDA
	(p17) FMA	f107 = ALPHA7,  f83, f107
	}
	{ .mmf
	(p16) LDFD	f82  = [AO3], LDA
	(p16) LDFD	f83  = [AO4], LDA
	(p17) FMA	f110 = ALPHA7,  f84, f110
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f113 = ALPHA7,  f85, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA7,  f86, f116
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f119 = ALPHA7,  f87, f119
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f122 = ALPHA7,  f88, f122
	}
	;;
	{ .mmf
	(p16) LDFD	f84  = [AO5], LDA
	(p16) LDFD	f85  = [AO6], LDA
	(p17) FMA	f101 = ALPHA8,  f89, f101
	}
	{ .mmf
	(p16) LDFD	f86  = [AO7], LDA
	(p16) LDFD	f87  = [AO8], LDA
	(p17) FMA	f104 = ALPHA8,  f90, f104
	}
	;;
	{ .mmf
	(p16) LDFD	f88  = [AO1], LDA7M8
	(p16) LDFD	f89  = [AO2], LDA7M8
	(p17) FMA	f107 = ALPHA8,  f91, f107
	}
	{ .mmf
	(p16) LDFD	f90  = [AO3], LDA7M8
	(p16) LDFD	f91  = [AO4], LDA7M8
	(p17) FMA	f110 = ALPHA8,  f92, f110
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f113 = ALPHA8,  f93, f113
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f116 = ALPHA8,  f94, f116
	}
	;;
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p17) FMA	f119 = ALPHA8,  f95, f119
	}
	{ .mfb
	nop   __LINE__
	(p17) FMA	f122 = ALPHA8,  f96, f122
	br.ctop.sptk.few .L12
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f102, 2 * SIZE
	(p18) STFD	[YST2] = f105, 2 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f108, 2 * SIZE
	(p18) STFD	[YST2] = f111, 2 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f114, 2 * SIZE
	(p18) STFD	[YST2] = f117, 2 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f120, 2 * SIZE
	(p18) STFD	[YST2] = f123, 2 * SIZE
	nop   __LINE__
	}
	;;
	.align 16

.L15:
	{ .mmi
	(p7) cmp.eq.unc p9,  p0 = r0, II
	(p8) cmp.eq.unc p10, p0 = r0, II
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	(p9) br.cond.dptk   .L11
	(p10) br.cond.dptk  .L20
	}
	;;
	{ .mmi
	(p13) LDFD	f32  = [AO1], LDA
	(p13) LDFD	f33  = [AO2], LDA
	tbit.nz	p15, p0  = M, 0
	}
	{ .mmi
	(p13) LDFD	f34  = [AO3], LDA
	(p11) adds	AO6  = - 4 * SIZE, AO6
	(p12) adds	AO7  = - 2 * SIZE, AO7
	}
	;;
	(p13) LDFD	f35  = [AO4], LDA
	(p14) LDFD	f36  = [AO5], LDA
	(p14) LDFD	f37  = [AO6], LDA
	(p15) LDFD	f38  = [AO7], LDA
	;;
	(p13) LDFD	f40  = [AO1], LDA
	(p13) LDFD	f41  = [AO2], LDA
	(p13) LDFD	f42  = [AO3], LDA
	(p13) LDFD	f43  = [AO4], LDA
	;;
	(p14) LDFD	f44  = [AO5], LDA
	(p14) LDFD	f45  = [AO6], LDA
	(p15) LDFD	f46  = [AO7], LDA
	;;
	(p13) LDFD	f48  = [AO1], LDA
	(p13) LDFD	f49  = [AO2], LDA
	(p13) LDFD	f50  = [AO3], LDA
	(p13) LDFD	f51  = [AO4], LDA
	;;
	(p14) LDFD	f52  = [AO5], LDA
	(p14) LDFD	f53  = [AO6], LDA
	(p15) LDFD	f54  = [AO7], LDA
	;;
	(p13) LDFD	f56  = [AO1], LDA
	(p13) LDFD	f57  = [AO2], LDA
	(p13) LDFD	f58  = [AO3], LDA
	(p13) LDFD	f59  = [AO4], LDA
	;;
	(p14) LDFD	f60  = [AO5], LDA
	(p14) LDFD	f61  = [AO6], LDA
	(p15) LDFD	f62  = [AO7], LDA
	;;
	(p13) LDFD	f64  = [AO1], LDA
	(p13) LDFD	f65  = [AO2], LDA
	(p13) LDFD	f66  = [AO3], LDA
	(p13) LDFD	f67  = [AO4], LDA
	;;
	(p14) LDFD	f68  = [AO5], LDA
	(p14) LDFD	f69  = [AO6], LDA
	(p15) LDFD	f70  = [AO7], LDA
	;;
	(p13) LDFD	f72  = [AO1], LDA
	(p13) LDFD	f73  = [AO2], LDA
	(p13) LDFD	f74  = [AO3], LDA
	(p13) LDFD	f75  = [AO4], LDA
	;;
	(p14) LDFD	f76  = [AO5], LDA
	(p14) LDFD	f77  = [AO6], LDA
	(p15) LDFD	f78  = [AO7], LDA
	;;
	(p13) LDFD	f80  = [AO1], LDA
	(p13) LDFD	f81  = [AO2], LDA
	(p13) LDFD	f82  = [AO3], LDA
	(p13) LDFD	f83  = [AO4], LDA
	;;
	(p14) LDFD	f84  = [AO5], LDA
	(p14) LDFD	f85  = [AO6], LDA
	(p15) LDFD	f86  = [AO7], LDA
	;;
	(p13) LDFD	f88  = [AO1]
	(p13) LDFD	f89  = [AO2]
	(p13) LDFD	f90  = [AO3]
	(p13) LDFD	f91  = [AO4]
	;;
	(p14) LDFD	f92  = [AO5]
	(p14) LDFD	f93  = [AO6]
	(p15) LDFD	f94  = [AO7]
	;;
	(p13) LDFD	f96  = [YLD1], 2 * SIZE
	(p13) LDFD	f97  = [YLD2], 2 * SIZE
	;;
	(p13) LDFD	f98  = [YLD1], 2 * SIZE
	(p13) LDFD	f99  = [YLD2], 2 * SIZE
	;;
	(p14) LDFD	f100 = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [YLD1], 1 * SIZE
	;;

	(p13) FMA	f96  = ALPHA1,  f32, f96
	(p13) FMA	f97  = ALPHA1,  f33, f97
	(p13) FMA	f98  = ALPHA1,  f34, f98
	(p13) FMA	f99  = ALPHA1,  f35, f99
	(p14) FMA	f100 = ALPHA1,  f36, f100
	(p14) FMA	f101 = ALPHA1,  f37, f101
	(p15) FMA	f102 = ALPHA1,  f38, f102
	;;
	(p13) FMA	f96  = ALPHA2,  f40, f96
	(p13) FMA	f97  = ALPHA2,  f41, f97
	(p13) FMA	f98  = ALPHA2,  f42, f98
	(p13) FMA	f99  = ALPHA2,  f43, f99
	(p14) FMA	f100 = ALPHA2,  f44, f100
	(p14) FMA	f101 = ALPHA2,  f45, f101
	(p15) FMA	f102 = ALPHA2,  f46, f102
	;;
	(p13) FMA	f96  = ALPHA3,  f48, f96
	(p13) FMA	f97  = ALPHA3,  f49, f97
	(p13) FMA	f98  = ALPHA3,  f50, f98
	(p13) FMA	f99  = ALPHA3,  f51, f99
	(p14) FMA	f100 = ALPHA3,  f52, f100
	(p14) FMA	f101 = ALPHA3,  f53, f101
	(p15) FMA	f102 = ALPHA3,  f54, f102
	;;
	(p13) FMA	f96  = ALPHA4,  f56, f96
	(p13) FMA	f97  = ALPHA4,  f57, f97
	(p13) FMA	f98  = ALPHA4,  f58, f98
	(p13) FMA	f99  = ALPHA4,  f59, f99
	(p14) FMA	f100 = ALPHA4,  f60, f100
	(p14) FMA	f101 = ALPHA4,  f61, f101
	(p15) FMA	f102 = ALPHA4,  f62, f102
	;;
	(p13) FMA	f96  = ALPHA5,  f64, f96
	(p13) FMA	f97  = ALPHA5,  f65, f97
	(p13) FMA	f98  = ALPHA5,  f66, f98
	(p13) FMA	f99  = ALPHA5,  f67, f99
	(p14) FMA	f100 = ALPHA5,  f68, f100
	(p14) FMA	f101 = ALPHA5,  f69, f101
	(p15) FMA	f102 = ALPHA5,  f70, f102
	;;
	(p13) FMA	f96  = ALPHA6,  f72, f96
	(p13) FMA	f97  = ALPHA6,  f73, f97
	(p13) FMA	f98  = ALPHA6,  f74, f98
	(p13) FMA	f99  = ALPHA6,  f75, f99
	(p14) FMA	f100 = ALPHA6,  f76, f100
	(p14) FMA	f101 = ALPHA6,  f77, f101
	(p15) FMA	f102 = ALPHA6,  f78, f102
	;;
	(p13) FMA	f96  = ALPHA7,  f80, f96
	(p13) FMA	f97  = ALPHA7,  f81, f97
	(p13) FMA	f98  = ALPHA7,  f82, f98
	(p13) FMA	f99  = ALPHA7,  f83, f99
	(p14) FMA	f100 = ALPHA7,  f84, f100
	(p14) FMA	f101 = ALPHA7,  f85, f101
	(p15) FMA	f102 = ALPHA7,  f86, f102
	;;
	(p13) FMA	f16  = ALPHA8,  f88, f96
	(p13) FMA	f17  = ALPHA8,  f89, f97
	(p13) FMA	f18  = ALPHA8,  f90, f98
	(p13) FMA	f19  = ALPHA8,  f91, f99
	(p14) FMA	f20  = ALPHA8,  f92, f100
	(p14) FMA	f21  = ALPHA8,  f93, f101
	(p15) FMA	f22  = ALPHA8,  f94, f102
	;;
	{ .mmi
	(p13) STFD	[YST1] = f16, 2 * SIZE
	(p13) STFD	[YST2] = f17, 2 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[YST1] = f18, 2 * SIZE
	(p13) STFD	[YST2] = f19
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YST1] = f20, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f21, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mib
	(p15) STFD	[YST1] = f22
	cmp.lt p11, p12 = r0, J
	(p11) br.cond.dptk .L11
	}
	;;
	.align 16

.L20:
	tbit.z	p6, p0 = N, 2
	;;
	(p6) br.cond.dpnt .L30
	;;

	shladd	LDA7M8 = LDA, 2, r0
	;;
	sub	LDA7M8 = LDA, LDA7M8
	;;
	adds	LDA7M8 = 8 * SIZE, LDA7M8
	;;
	mov	YLD1 = YY
	mov	YST1 = YY
	adds	YLD2 = 2 * SIZE, YY
	adds	YST2 = 2 * SIZE, YY
	;;
	LDFD	ALPHA1 = [X], INCX
	;;
	LDFD	ALPHA2 = [X], INCX
	;;
	LDFD	ALPHA3 = [X], INCX
	;;
	LDFD	ALPHA4 = [X], INCX
	;;
	FMPY	ALPHA1 = ALPHA, ALPHA1
	FMPY	ALPHA2 = ALPHA, ALPHA2
	FMPY	ALPHA3 = ALPHA, ALPHA3
	FMPY	ALPHA4 = ALPHA, ALPHA4
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
	shr	I = M, 3
	mov	pr.rot= 0
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	adds J = -1, J
	;;
	cmp.lt p7, p8 = r0, J
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	mov	ar.ec= 1
	;;
	{ .mfi
	and	II = 7, M
	mov	ar.lc = I
	}
	{ .mfb
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L25
	}
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
	(p16) LDFD	f96  = [YLD1], 1 * SIZE
	(p16) LDFD	f98  = [YLD2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [YLD1], 3 * SIZE
	(p16) LDFD	f99  = [YLD2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p16) LDFD	f102 = [YLD2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [YLD1], 3 * SIZE
	(p16) LDFD	f103 = [YLD2], 3 * SIZE
	;;
	(p16) FMA	f96  = ALPHA1,  f32, f96
	(p16) FMA	f98  = ALPHA1,  f34, f98
	(p16) FMA	f97  = ALPHA1,  f33, f97
	(p16) FMA	f99  = ALPHA1,  f35, f99
	(p16) FMA	f100 = ALPHA1,  f36, f100
	(p16) FMA	f102 = ALPHA1,  f38, f102
	(p16) FMA	f101 = ALPHA1,  f37, f101
	(p16) FMA	f103 = ALPHA1,  f39, f103
	;;
	(p16) FMA	f96  = ALPHA2,  f40, f96
	(p16) FMA	f98  = ALPHA2,  f42, f98
	(p16) FMA	f97  = ALPHA2,  f41, f97
	(p16) FMA	f99  = ALPHA2,  f43, f99
	(p16) FMA	f100 = ALPHA2,  f44, f100
	(p16) FMA	f102 = ALPHA2,  f46, f102
	(p16) FMA	f101 = ALPHA2,  f45, f101
	(p16) FMA	f103 = ALPHA2,  f47, f103
	;;
	(p16) FMA	f96  = ALPHA3,  f48, f96
	(p16) FMA	f98  = ALPHA3,  f50, f98
	(p16) FMA	f97  = ALPHA3,  f49, f97
	(p16) FMA	f99  = ALPHA3,  f51, f99
	(p16) FMA	f100 = ALPHA3,  f52, f100
	(p16) FMA	f102 = ALPHA3,  f54, f102
	(p16) FMA	f101 = ALPHA3,  f53, f101
	(p16) FMA	f103 = ALPHA3,  f55, f103
	;;
	(p16) FMA	f16  = ALPHA4,  f56, f96
	(p16) FMA	f18  = ALPHA4,  f58, f98
	(p16) FMA	f17  = ALPHA4,  f57, f97
	(p16) FMA	f19  = ALPHA4,  f59, f99
	(p16) FMA	f20  = ALPHA4,  f60, f100
	(p16) FMA	f22  = ALPHA4,  f62, f102
	(p16) FMA	f21  = ALPHA4,  f61, f101
	(p16) FMA	f23  = ALPHA4,  f63, f103
	;;
	(p16) STFD	[YST1] = f16, 1 * SIZE
	(p16) STFD	[YST2] = f18, 1 * SIZE
	;;
	(p16) STFD	[YST1] = f17, 3 * SIZE
	(p16) STFD	[YST2] = f19, 3 * SIZE
	;;
	(p16) STFD	[YST1] = f20, 1 * SIZE
	(p16) STFD	[YST2] = f22, 1 * SIZE
	;;
	(p16) STFD	[YST1] = f21, 3 * SIZE
	(p16) STFD	[YST2] = f23, 3 * SIZE
	br.ctop.sptk.few .L22
	;;
	.align 16

.L25:
	{ .mmi
	(p8) cmp.eq.unc p10, p0 = r0, II
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	(p10) br.cond.dptk  .L30
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
	(p13) LDFD	f96  = [YLD1], 1 * SIZE
	(p13) LDFD	f98  = [YLD2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [YLD1], 3 * SIZE
	(p13) LDFD	f99  = [YLD2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [YLD1], 1 * SIZE
	;;

	(p13) FMA	f96  = ALPHA1,  f32, f96
	(p13) FMA	f98  = ALPHA1,  f34, f98
	(p13) FMA	f97  = ALPHA1,  f33, f97
	(p13) FMA	f99  = ALPHA1,  f35, f99
	(p14) FMA	f100 = ALPHA1,  f36, f100
	(p15) FMA	f102 = ALPHA1,  f38, f102
	(p14) FMA	f101 = ALPHA1,  f37, f101
	;;
	(p13) FMA	f96  = ALPHA2,  f40, f96
	(p13) FMA	f98  = ALPHA2,  f42, f98
	(p13) FMA	f97  = ALPHA2,  f41, f97
	(p13) FMA	f99  = ALPHA2,  f43, f99
	(p14) FMA	f100 = ALPHA2,  f44, f100
	(p15) FMA	f102 = ALPHA2,  f46, f102
	(p14) FMA	f101 = ALPHA2,  f45, f101
	;;
	(p13) FMA	f96  = ALPHA3,  f48, f96
	(p13) FMA	f98  = ALPHA3,  f50, f98
	(p13) FMA	f97  = ALPHA3,  f49, f97
	(p13) FMA	f99  = ALPHA3,  f51, f99
	(p14) FMA	f100 = ALPHA3,  f52, f100
	(p15) FMA	f102 = ALPHA3,  f54, f102
	(p14) FMA	f101 = ALPHA3,  f53, f101
	;;
	(p13) FMA	f16  = ALPHA4,  f56, f96
	(p13) FMA	f18  = ALPHA4,  f58, f98
	(p13) FMA	f17  = ALPHA4,  f57, f97
	(p13) FMA	f19  = ALPHA4,  f59, f99
	(p14) FMA	f20  = ALPHA4,  f60, f100
	(p15) FMA	f22  = ALPHA4,  f62, f102
	(p14) FMA	f21  = ALPHA4,  f61, f101
	;;
	{ .mmi
	(p13) STFD	[YST1] = f16, 1 * SIZE
	(p13) STFD	[YST2] = f18, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[YST1] = f17, 3 * SIZE
	(p13) STFD	[YST2] = f19
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YST1] = f20, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f21, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mib
	(p15) STFD	[YST1] = f22
	}
	;;
	.align 16

.L30:
	tbit.z	p6, p0 = N, 1
	;;
	(p6) br.cond.dpnt .L40
	;;

	shladd	LDA7M8 = LDA, 1, r0
	;;
	sub	LDA7M8 = LDA, LDA7M8
	;;
	adds	LDA7M8 = 8 * SIZE, LDA7M8
	;;
	mov	YLD1 = YY
	mov	YST1 = YY
	adds	YLD2 = 2 * SIZE, YY
	adds	YST2 = 2 * SIZE, YY
	;;
	LDFD	ALPHA1 = [X], INCX
	;;
	LDFD	ALPHA2 = [X], INCX
	;;
	FMPY	ALPHA1 = ALPHA, ALPHA1
	FMPY	ALPHA2 = ALPHA, ALPHA2
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
	shr	I = M, 3
	mov	pr.rot= 0
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	adds J = -1, J
	;;
	cmp.lt p7, p8 = r0, J
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	mov	ar.ec= 1
	;;
	{ .mfi
	and	II = 7, M
	mov	ar.lc = I
	}
	{ .mfb
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L35
	}
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
	(p16) LDFD	f96  = [YLD1], 1 * SIZE
	(p16) LDFD	f98  = [YLD2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [YLD1], 3 * SIZE
	(p16) LDFD	f99  = [YLD2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p16) LDFD	f102 = [YLD2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [YLD1], 3 * SIZE
	(p16) LDFD	f103 = [YLD2], 3 * SIZE
	;;
	(p16) FMA	f96  = ALPHA1,  f32, f96
	(p16) FMA	f98  = ALPHA1,  f34, f98
	(p16) FMA	f97  = ALPHA1,  f33, f97
	(p16) FMA	f99  = ALPHA1,  f35, f99
	(p16) FMA	f100 = ALPHA1,  f36, f100
	(p16) FMA	f102 = ALPHA1,  f38, f102
	(p16) FMA	f101 = ALPHA1,  f37, f101
	(p16) FMA	f103 = ALPHA1,  f39, f103
	;;
	(p16) FMA	f16  = ALPHA2,  f40, f96
	(p16) FMA	f18  = ALPHA2,  f42, f98
	(p16) FMA	f17  = ALPHA2,  f41, f97
	(p16) FMA	f19  = ALPHA2,  f43, f99
	(p16) FMA	f20  = ALPHA2,  f44, f100
	(p16) FMA	f22  = ALPHA2,  f46, f102
	(p16) FMA	f21  = ALPHA2,  f45, f101
	(p16) FMA	f23  = ALPHA2,  f47, f103
	;;
	(p16) STFD	[YST1] = f16, 1 * SIZE
	(p16) STFD	[YST2] = f18, 1 * SIZE
	;;
	(p16) STFD	[YST1] = f17, 3 * SIZE
	(p16) STFD	[YST2] = f19, 3 * SIZE
	;;
	(p16) STFD	[YST1] = f20, 1 * SIZE
	(p16) STFD	[YST2] = f22, 1 * SIZE
	;;
	(p16) STFD	[YST1] = f21, 3 * SIZE
	(p16) STFD	[YST2] = f23, 3 * SIZE
	br.ctop.sptk.few .L32
	;;
	.align 16

.L35:
	{ .mmi
	(p8) cmp.eq.unc p10, p0 = r0, II
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	(p10) br.cond.dptk  .L40
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
	(p13) LDFD	f41  = [AO2]
	(p13) LDFD	f43  = [AO4]
	(p14) LDFD	f45  = [AO6]
	;;
	(p13) LDFD	f96  = [YLD1], 1 * SIZE
	(p13) LDFD	f98  = [YLD2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [YLD1], 3 * SIZE
	(p13) LDFD	f99  = [YLD2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [YLD1], 1 * SIZE
	;;

	(p13) FMA	f96  = ALPHA1,  f32, f96
	(p13) FMA	f98  = ALPHA1,  f34, f98
	(p13) FMA	f97  = ALPHA1,  f33, f97
	(p13) FMA	f99  = ALPHA1,  f35, f99
	(p14) FMA	f100 = ALPHA1,  f36, f100
	(p15) FMA	f102 = ALPHA1,  f38, f102
	(p14) FMA	f101 = ALPHA1,  f37, f101
	;;
	(p13) FMA	f16  = ALPHA2,  f40, f96
	(p13) FMA	f18  = ALPHA2,  f42, f98
	(p13) FMA	f17  = ALPHA2,  f41, f97
	(p13) FMA	f19  = ALPHA2,  f43, f99
	(p14) FMA	f20  = ALPHA2,  f44, f100
	(p15) FMA	f22  = ALPHA2,  f46, f102
	(p14) FMA	f21  = ALPHA2,  f45, f101
	;;
	{ .mmi
	(p13) STFD	[YST1] = f16, 1 * SIZE
	(p13) STFD	[YST2] = f18, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[YST1] = f17, 3 * SIZE
	(p13) STFD	[YST2] = f19
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YST1] = f20, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f21, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mib
	(p15) STFD	[YST1] = f22
	}
	;;
	.align 16

.L40:
	tbit.z	p6, p0 = N, 0
	;;
	(p6) br.cond.dpnt .L990
	;;
	mov	LDA7M8 = 8 * SIZE
	;;
	mov	YLD1 = YY
	mov	YST1 = YY
	adds	YLD2 = 2 * SIZE, YY
	adds	YST2 = 2 * SIZE, YY
	;;
	LDFD	ALPHA1 = [X], INCX
	;;
	LDFD	ALPHA2 = [X], INCX
	;;
	FMPY	ALPHA1 = ALPHA, ALPHA1
	FMPY	ALPHA2 = ALPHA, ALPHA2
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
	shr	I = M, 3
	mov	pr.rot= 0
	;;
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	adds J = -1, J
	;;
	cmp.lt p7, p8 = r0, J
	tbit.nz	p13, p11 = M, 2
	tbit.nz	p14, p12 = M, 1
	mov	ar.ec= 1
	;;
	{ .mfi
	and	II = 7, M
	mov	ar.lc = I
	}
	{ .mfb
	cmp.eq	p6, p0 = -1, I
	(p6) br.cond.dpnt .L45
	}
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
	(p16) LDFD	f96  = [YLD1], 1 * SIZE
	(p16) LDFD	f98  = [YLD2], 1 * SIZE
	;;
	(p16) LDFD	f97  = [YLD1], 3 * SIZE
	(p16) LDFD	f99  = [YLD2], 3 * SIZE
	;;
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p16) LDFD	f102 = [YLD2], 1 * SIZE
	;;
	(p16) LDFD	f101 = [YLD1], 3 * SIZE
	(p16) LDFD	f103 = [YLD2], 3 * SIZE
	;;
	(p16) FMA	f16  = ALPHA1,  f32, f96
	(p16) FMA	f18  = ALPHA1,  f34, f98
	(p16) FMA	f17  = ALPHA1,  f33, f97
	(p16) FMA	f19  = ALPHA1,  f35, f99
	(p16) FMA	f20  = ALPHA1,  f36, f100
	(p16) FMA	f22  = ALPHA1,  f38, f102
	(p16) FMA	f21  = ALPHA1,  f37, f101
	(p16) FMA	f23 = ALPHA1,  f39, f103
	;;
	(p16) STFD	[YST1] = f16, 1 * SIZE
	(p16) STFD	[YST2] = f18, 1 * SIZE
	;;
	(p16) STFD	[YST1] = f17, 3 * SIZE
	(p16) STFD	[YST2] = f19, 3 * SIZE
	;;
	(p16) STFD	[YST1] = f20, 1 * SIZE
	(p16) STFD	[YST2] = f22, 1 * SIZE
	;;
	(p16) STFD	[YST1] = f21, 3 * SIZE
	(p16) STFD	[YST2] = f23, 3 * SIZE
	br.ctop.sptk.few .L42
	;;
	.align 16

.L45:
	{ .mmi
	(p8) cmp.eq.unc p10, p0 = r0, II
	(p11) adds	AO5  = - 4 * SIZE, AO5
	}
	{ .mbb
	(p11) adds	AO7  = - 4 * SIZE, AO7
	(p10) br.cond.dptk  .L990
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
	(p13) LDFD	f96  = [YLD1], 1 * SIZE
	(p13) LDFD	f98  = [YLD2], 1 * SIZE
	;;
	(p13) LDFD	f97  = [YLD1], 3 * SIZE
	(p13) LDFD	f99  = [YLD2], 3 * SIZE
	;;
	(p14) LDFD	f100 = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f101 = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f102 = [YLD1], 1 * SIZE
	;;

	(p13) FMA	f16  = ALPHA1,  f32, f96
	(p13) FMA	f18  = ALPHA1,  f34, f98
	(p13) FMA	f17  = ALPHA1,  f33, f97
	(p13) FMA	f19  = ALPHA1,  f35, f99
	(p14) FMA	f20  = ALPHA1,  f36, f100
	(p15) FMA	f22  = ALPHA1,  f38, f102
	(p14) FMA	f21 = ALPHA1,  f37, f101
	;;
	{ .mmi
	(p13) STFD	[YST1] = f16, 1 * SIZE
	(p13) STFD	[YST2] = f18, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[YST1] = f17, 3 * SIZE
	(p13) STFD	[YST2] = f19
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YST1] = f20, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f21, 1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mib
	(p15) STFD	[YST1] = f22
	}
	;;
	.align 16


.L990:
	cmp.eq	p10, p0 = SIZE, INCY
	;;
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = Y
	mov	pr.rot= 0
	}
	{ .mib
	mov	YST2 = Y
	shr	J = M, 3
	(p10) br.cond.dptk .L999
	}
	;;
	{ .mmi
	cmp.eq	p6, p0 = r0, J
	adds	J = -1, J
	mov	ar.ec = 4
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	nop	__LINE__
	tbit.nz	p13, p0 = M, 2
	}
	;;
	{ .mib
	nop	__LINE__
	mov	ar.lc = J
	(p6) br.cond.dpnt .L995
	}
	;;
.L992:
	{ .mfi
	(p19)	STFD	[YST2] = f35
	(p18)	FADD	f34 = f34, f66
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f64 = [YLD1], 1 * SIZE
	(p16)	LDFD	f32 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f39
	(p18)	FADD	f38 = f38, f70
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f36 = [YST1], INCY
	(p16)	LDFD	f68 = [YLD1], 1 * SIZE
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f43
	(p18)	FADD	f42 = f42, f74
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f72 = [YLD1], 1 * SIZE
	(p16)	LDFD	f40 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f47
	(p18)	FADD	f46 = f46, f78
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f76 = [YLD1], 1 * SIZE
	(p16)	LDFD	f44 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f51
	(p18)	FADD	f50 = f50, f82
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f80 = [YLD1], 1 * SIZE
	(p16)	LDFD	f48 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f55
	(p18)	FADD	f54 = f54, f86
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f84 = [YLD1], 1 * SIZE
	(p16)	LDFD	f52 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f59
	(p18)	FADD	f58 = f58, f90
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f88 = [YLD1], 1 * SIZE
	(p16)	LDFD	f56 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f63
	(p18)	FADD	f62 = f62, f94
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmb
	(p16)	LDFD	f92 = [YLD1], 1 * SIZE
	(p16)	LDFD	f60 = [YST1], INCY
	br.ctop.sptk.few .L992
	}
	;;

.L995:
	(p13)	LDFD	f32 = [YST1], INCY
	(p13)	LDFD	f40 = [YLD1], 1 * SIZE
	tbit.nz	p14, p0 = M, 1
	;;
	(p13)	LDFD	f33 = [YST1], INCY
	(p13)	LDFD	f41 = [YLD1], 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	;;
	(p13)	LDFD	f34 = [YST1], INCY
	(p13)	LDFD	f42 = [YLD1], 1 * SIZE
	;;
	(p13)	LDFD	f35 = [YST1], INCY
	(p13)	LDFD	f43 = [YLD1], 1 * SIZE
	;;
	(p14)	LDFD	f36 = [YST1], INCY
	(p14)	LDFD	f44 = [YLD1], 1 * SIZE
	;;
	(p14)	LDFD	f37 = [YST1], INCY
	(p14)	LDFD	f45 = [YLD1], 1 * SIZE
	;;
	(p15)	LDFD	f38 = [YST1], INCY
	(p15)	LDFD	f46 = [YLD1], 1 * SIZE
	;;
	(p13)	FADD	f32 = f32, f40
	(p13)	FADD	f33 = f33, f41
	(p13)	FADD	f34 = f34, f42
	(p13)	FADD	f35 = f35, f43
	(p14)	FADD	f36 = f36, f44
	(p14)	FADD	f37 = f37, f45
	(p15)	FADD	f38 = f38, f46
	;;
	(p13)	STFD	[YST2] = f32
	(p13)	add YST2 = YST2, INCY
	;;
	(p13)	STFD	[YST2] = f33
	(p13)	add YST2 = YST2, INCY
	;;
	(p13)	STFD	[YST2] = f34
	(p13)	add YST2 = YST2, INCY
	;;
	(p13)	STFD	[YST2] = f35
	(p13)	add YST2 = YST2, INCY
	;;
	(p14)	STFD	[YST2] = f36
	(p14)	add YST2 = YST2, INCY
	;;
	(p14)	STFD	[YST2] = f37
	(p14)	add YST2 = YST2, INCY
	;;
	(p15)	STFD	[YST2] = f38
	;;

.L999:
	mov	 ar.lc = ARLC
	mov pr    = PR, -1
	br.ret.sptk.many b0
	;;
	EPILOGUE
