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
#define A	r37
#define LDA	r38
#define X	r39
#define INCX	r34
#define Y	r35
#define INCY	r36
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
#define YY	r28
#define XX	r9

#define RPRE1	loc0
#define RPRE2	loc1
#define RPRE3	loc2
#define RPRE4	loc3
#define RPRE5	loc4
#define RPRE6	loc5
#define RPRE7	loc6
#define RPRE8	loc7

#define INCXM1	r2
#define INCX3M1	r3

#define AO9	loc8
#define AO10	loc9
#define AO11	loc10
#define AO12	loc11
#define AO13	loc12
#define AO14	loc13
#define AO15	loc14
#define AO16	loc15

#define PREB	r8

#define ARLC	r29
#define PR	r30
#define ARPFS	r31

#ifdef DOUBLE
#define RPREFETCH	(16 * 2 +  8)
#else
#define RPREFETCH	(16 * 2 + 16)
#endif
#define PREFETCH	lfetch.nt1

#define ALPHA_R	f6
#define ALPHA_I	f7

#if   !defined(CONJ) && !defined(XCONJ)
#define ADD1	     FNMA
#define ADD2	     FMA
#define ADD3	     FNMA
#define ADD4	     FMA
#elif  defined(CONJ) && !defined(XCONJ)
#define ADD1	     FNMA
#define ADD2	     FMA
#define ADD3	     FMA
#define ADD4	     FNMA
#elif !defined(CONJ) &&  defined(XCONJ)
#define ADD1	     FMA
#define ADD2	     FNMA
#define ADD3	     FNMA
#define ADD4	     FMA
#else
#define ADD1	     FMA
#define ADD2	     FNMA
#define ADD3	     FMA
#define ADD4	     FNMA
#endif

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 16, 0, 0
	mov	ARLC  = ar.lc
	}
	;;
	mov	PR = pr
	adds	r14 = 16, SP
	adds	r15 = 24, SP
	adds	r16 = 32, SP
	adds	r17 = 40, SP
	;;
	adds	r8 = -8 * 16, SP
	adds	r9 = -7 * 16, SP
	adds	SP = -8 * 16, SP
	;;
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	;;
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	;;
	stf.spill  [r8] = f20, 32
	stf.spill  [r9] = f21, 32
	;;
	stf.spill  [r8] = f22
	stf.spill  [r9] = f23
	;;
	ld8	INCX   = [r14]
	ld8	Y      = [r15]
	ld8	INCY   = [r16]
	ld8	BUFFER = [r17]
	.body
	;;
	cmp.ge	p7, p0 = 0, M
	cmp.ge	p6, p0 = 0, N
	mov	ALPHA_R = f8
	shladd	INCX = INCX, ZBASE_SHIFT, r0
	shladd	LDA  = LDA,  ZBASE_SHIFT, r0
	mov	ALPHA_I = f9
	;;
	shladd	INCY = INCY, ZBASE_SHIFT, r0
	tbit.nz	p8, p0 = A,   BASE_SHIFT
	(p7) br.cond.dpnt .L999
	;;
	shladd	XX = INCX, 1, X
	adds	INCXM1  = -SIZE, INCX
	(p6) br.cond.dpnt .L999
	;;
	shladd	INCX3M1 = INCX, 1, INCXM1
	cmp.eq	p10, p11 = 2 * SIZE, INCY
	mov	YY = Y
	;;
	(p11) mov YY = BUFFER
	mov	YST1 = BUFFER
	shr	J = M, 2
	;;
	{ .mib
	adds	YST2 = 4 * SIZE, BUFFER
	mov	ar.lc = J
	(p10) br.cond.dptk .L10
	}
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
	{ .mmi
	mov	AO1 = A
	nop	__LINE__
	shr	J   = N, 3
	}
	;;
	{ .mmb
	add	AO2 = LDA, A
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L20
	}
	;;
	.align 16

.L11:
	LDFD	f32 = [X], SIZE
	LDFD	f36 = [XX], SIZE
	mov	pr.rot= 0
	;;
	LDFD	f33 = [X], INCXM1
	LDFD	f37 = [XX], INCXM1
	mov	YLD1 = YY
	;;
	LDFD	f34 = [X], SIZE
	LDFD	f38 = [XX], SIZE
	adds	YLD2 = 4 * SIZE, YY
	;;
	LDFD	f35 = [X], INCX3M1
	LDFD	f39 = [XX], INCX3M1
	mov	YST1 = YY
	;;
	LDFD	f40 = [X], SIZE
	LDFD	f44 = [XX], SIZE
	adds	YST2 = 4 * SIZE, YY
	;;
	LDFD	f41 = [X], INCXM1
	LDFD	f45 = [XX], INCXM1
	shr	I = M, 2
	;;
	LDFD	f42 = [X], SIZE
	LDFD	f46 = [XX], SIZE
	mov	AO1 = A
	;;
	LDFD	f43 = [X], INCX3M1
	LDFD	f47 = [XX], INCX3M1
	add	AO2 = LDA, A
	;;
	shladd	AO3 = LDA, 1, A
	FMPY	f8  = ALPHA_R, f32
	mov	ar.ec= 2
	shladd	AO4 = LDA, 1, AO2
	FMPY	f9  = ALPHA_I, f32
	;;
	shladd	AO5 = LDA, 1, AO3
	FMPY	f10 = ALPHA_R, f34
	shladd	AO6 = LDA, 1, AO4
	FMPY	f11 = ALPHA_I, f34
	;;
	FMPY	f12 = ALPHA_R, f36
	shladd	AO7 = LDA, 1, AO5
	FMPY	f13 = ALPHA_I, f36
	shladd	AO8 = LDA, 1, AO6
	FMPY	f14 = ALPHA_R, f38
	;;
	adds	PREB   = RPREFETCH * SIZE, YLD1
	FMPY	f15 = ALPHA_I, f38
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	FMPY	f16 = ALPHA_R, f40
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	FMPY	f17 = ALPHA_I, f40
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	FMPY	f18 = ALPHA_R, f42
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	FMPY	f19 = ALPHA_I, f42
	adds	RPRE5  = RPREFETCH * SIZE, AO5
	FMPY	f20 = ALPHA_R, f44
	adds	RPRE6  = (RPREFETCH + 8) * SIZE, AO6
	FMPY	f21 = ALPHA_I, f44
	adds	RPRE7  = RPREFETCH * SIZE, AO7
	FMPY	f22 = ALPHA_R, f46
	adds	RPRE8  = (RPREFETCH + 8) * SIZE, AO8
	FMPY	f23 = ALPHA_I, f46
	;;
	ADD1	f8  = ALPHA_I, f33, f8
	tbit.nz	p14, p0 = M, 1
	ADD2	f9  = ALPHA_R, f33, f9
	shladd	A   = LDA, 3, A
	ADD1	f10 = ALPHA_I, f35, f10
	adds	AO9  = 4 * SIZE, AO1
	ADD2	f11 = ALPHA_R, f35, f11
	adds	AO10 = 4 * SIZE, AO2
	ADD1	f12 = ALPHA_I, f37, f12
	adds	AO11 = 4 * SIZE, AO3
	ADD2	f13 = ALPHA_R, f37, f13
	adds	AO12 = 4 * SIZE, AO4
	ADD1	f14 = ALPHA_I, f39, f14
	adds	AO13 = 4 * SIZE, AO5
	ADD2	f15 = ALPHA_R, f39, f15
	adds	AO14 = 4 * SIZE, AO6
	ADD1	f16 = ALPHA_I, f41, f16
	adds	AO15 = 4 * SIZE, AO7
	ADD2	f17 = ALPHA_R, f41, f17
	adds	AO16 = 4 * SIZE, AO8
	ADD1	f18 = ALPHA_I, f43, f18
	cmp.eq	p6, p0 = 0, I
	ADD2	f19 = ALPHA_R, f43, f19
	cmp.eq	p16, p0 = r0, r0
	ADD1	f20 = ALPHA_I, f45, f20
	adds	I = -1, I
	ADD2	f21 = ALPHA_R, f45, f21
	;;
	{ .mfi
	nop	__LINE__
	ADD1	f22 = ALPHA_I, f47, f22
	mov	ar.lc = I
	}
	{ .mfb
	nop	__LINE__
	ADD2	f23 = ALPHA_R, f47, f23
	(p6) br.cond.dpnt .L15
	}
	;;
	.align 16

.L12:
	{ .mfi
	(p17) LDFD	f89  = [AO8], 1 * SIZE
	(p17) FMA	f101 = f8,   f33, f101
	(p16) tbit.nz.unc	p12, p13 = I, 0
	}
	{ .mfi
	(p17) LDFD	f93  = [AO16], 1 * SIZE
	(p17) FMA	f113 = f8,   f37, f113
	}
	;;
	{ .mfi
	(p17) LDFD	f90  = [AO8], 1 * SIZE
	(p17) FMA	f104 = f9,   f33, f104
	(p16) adds	I = -1, I
	}
	{ .mfi
	(p17) LDFD	f94  = [AO16], 1 * SIZE
	(p17) FMA	f116 = f9,   f37, f116
	}
	;;
	{ .mfi
	(p17) LDFD	f91  = [AO8], 1 * SIZE
	(p17) FMA	f107 = f8,   f35, f107
	}
	{ .mfi
	(p17) LDFD	f95  = [AO16], 1 * SIZE
	(p17) FMA	f119 = f8,   f39, f119
	}
	;;
	{ .mfi
	(p17) LDFD	f92  = [AO8], 5 * SIZE
	(p17) FMA	f110 = f9,   f35, f110
	}
	{ .mfi
	(p17) LDFD	f96  = [AO16], 5 * SIZE
	(p17) FMA	f122 = f9,   f39, f122
	}
	;;
	{ .mfi
	(p12) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) ADD3	f101 = f9,   f34, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f9,   f38, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p17) ADD4	f104 = f8,   f34, f104
	}
	{ .mfi
	(p16) LDFD	f112 = [YLD2], 1 * SIZE
	(p17) ADD4	f116 = f8,   f38, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f103 = [YLD1], 1 * SIZE
	(p17) ADD3	f107 = f9,   f36, f107
	}
	{ .mfi
	(p16) LDFD	f115 = [YLD2], 1 * SIZE
	(p17) ADD3	f119 = f9,   f40, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE1], 16 * SIZE
	(p17) ADD4	f110 = f8,   f36, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f8,   f40, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f32  = [AO1], 1 * SIZE
	(p17) FMA	f101 = f10,   f41, f101
	}
	{ .mfi
	(p16) LDFD	f36  = [AO9], 1 * SIZE
	(p17) FMA	f113 = f10,   f45, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f33  = [AO1], 1 * SIZE
	(p17) FMA	f104 = f11,   f41, f104
	}
	{ .mfi
	(p16) LDFD	f37  = [AO9], 1 * SIZE
	(p17) FMA	f116 = f11,   f45, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f34  = [AO1], 1 * SIZE
	(p17) FMA	f107 = f10,   f43, f107
	}
	{ .mfi
	(p16) LDFD	f38  = [AO9], 1 * SIZE
	(p17) FMA	f119 = f10,   f47, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f35  = [AO1], 5 * SIZE
	(p17) FMA	f110 = f11,   f43, f110
	}
	{ .mfi
	(p16) LDFD	f39  = [AO9], 5 * SIZE
	(p17) FMA	f122 = f11,   f47, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f11,   f42, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f11,   f46, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f106 = [YLD1], 1 * SIZE
	(p17) ADD4	f104 = f10,   f42, f104
	}
	{ .mfi
	(p16) LDFD	f118 = [YLD2], 1 * SIZE
	(p17) ADD4	f116 = f10,   f46, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f109 = [YLD1], 5 * SIZE
	(p17) ADD3	f107 = f11,   f44, f107
	}
	{ .mfi
 	(p16) LDFD	f121 = [YLD2], 5 * SIZE
	(p17) ADD3	f119 = f11,   f48, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE2], 16 * SIZE
	(p17) ADD4	f110 = f10,   f44, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f10,   f48, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f40  = [AO2], 1 * SIZE
	(p17) FMA	f101 = f12,   f49, f101
	}
	{ .mfi
	(p16) LDFD	f44  = [AO10], 1 * SIZE
	(p17) FMA	f113 = f12,   f53, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f41  = [AO2], 1 * SIZE
	(p17) FMA	f104 = f13,   f49, f104
	}
	{ .mfi
	(p16) LDFD	f45  = [AO10], 1 * SIZE
	(p17) FMA	f116 = f13,   f53, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f42  = [AO2], 1 * SIZE
	(p17) FMA	f107 = f12,   f51, f107
	}
	{ .mfi
	(p16) LDFD	f46  = [AO10], 1 * SIZE
	(p17) FMA	f119 = f12,   f55, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f43  = [AO2], 5 * SIZE
	(p17) FMA	f110 = f13,   f51, f110
	}
	{ .mfi
	(p16) LDFD	f47  = [AO10], 5 * SIZE
	(p17) FMA	f122 = f13,   f55, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f13,   f50, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f13,   f54, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f12,   f50, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f12,   f54, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f13,   f52, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f13,   f56, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE3], 16 * SIZE
	(p17) ADD4	f110 = f12,   f52, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f12,   f56, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f48  = [AO3], 1 * SIZE
	(p17) FMA	f101 = f14,   f57, f101
	}
	{ .mfi
	(p16) LDFD	f52  = [AO11], 1 * SIZE
	(p17) FMA	f113 = f14,   f61, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f49  = [AO3], 1 * SIZE
	(p17) FMA	f104 = f15,   f57, f104
	}
	{ .mfi
	(p16) LDFD	f53  = [AO11], 1 * SIZE
	(p17) FMA	f116 = f15,   f61, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f50  = [AO3], 1 * SIZE
	(p17) FMA	f107 = f14,   f59, f107
	}
	{ .mfi
	(p16) LDFD	f54  = [AO11], 1 * SIZE
	(p17) FMA	f119 = f14,   f63, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f51  = [AO3], 5 * SIZE
	(p17) FMA	f110 = f15,   f59, f110
	}
	{ .mfi
	(p16) LDFD	f55  = [AO11], 5 * SIZE
	(p17) FMA	f122 = f15,   f63, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f15,   f58, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f15,   f62, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f14,   f58, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f14,   f62, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f15,   f60, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f15,   f64, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE4], 16 * SIZE
	(p17) ADD4	f110 = f14,   f60, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f14,   f64, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f56  = [AO4], 1 * SIZE
	(p17) FMA	f101 = f16,   f65, f101
	}
	{ .mfi
	(p16) LDFD	f60  = [AO12], 1 * SIZE
	(p17) FMA	f113 = f16,   f69, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f57  = [AO4], 1 * SIZE
	(p17) FMA	f104 = f17,   f65, f104
	}
	{ .mfi
	(p16) LDFD	f61  = [AO12], 1 * SIZE
	(p17) FMA	f116 = f17,   f69, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	(p17) FMA	f107 = f16,   f67, f107
	}
	{ .mmf
	(p16) LDFD	f58  = [AO4], 1 * SIZE
	(p16) LDFD	f62  = [AO12], 1 * SIZE
	(p17) FMA	f119 = f16,   f71, f119
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	(p17) FMA	f110 = f17,   f67, f110
	}
	{ .mmf
	(p16) LDFD	f59  = [AO4], 5 * SIZE
	(p16) LDFD	f63  = [AO12], 5 * SIZE
	(p17) FMA	f122 = f17,   f71, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f17,   f66, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f17,   f70, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f16,   f66, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f16,   f70, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f17,   f68, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f17,   f72, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE5], 16 * SIZE
	(p17) ADD4	f110 = f16,   f68, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f16,   f72, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f64  = [AO5], 1 * SIZE
	(p17) FMA	f101 = f18,   f73, f101
	}
	{ .mfi
	(p16) LDFD	f68  = [AO13], 1 * SIZE
	(p17) FMA	f113 = f18,   f77, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f65  = [AO5], 1 * SIZE
	(p17) FMA	f104 = f19,   f73, f104
	}
	{ .mfi
	(p16) LDFD	f69  = [AO13], 1 * SIZE
	(p17) FMA	f116 = f19,   f77, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	(p17) FMA	f107 = f18,   f75, f107
	}
	{ .mmf
	(p16) LDFD	f66  = [AO5], 1 * SIZE
	(p16) LDFD	f70  = [AO13], 1 * SIZE
	(p17) FMA	f119 = f18,   f79, f119
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	(p17) FMA	f110 = f19,   f75, f110
	}
	{ .mmf
	(p16) LDFD	f67  = [AO5], 5 * SIZE
	(p16) LDFD	f71  = [AO13], 5 * SIZE
	(p17) FMA	f122 = f19,   f79, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f19,   f74, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f19,   f78, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f18,   f74, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f18,   f78, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f19,   f76, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f19,   f80, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE6], 16 * SIZE
	(p17) ADD4	f110 = f18,   f76, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f18,   f80, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f72  = [AO6], 1 * SIZE
	(p17) FMA	f101 = f20,   f81, f101
	}
	{ .mfi
	(p16) LDFD	f76  = [AO14], 1 * SIZE
	(p17) FMA	f113 = f20,   f85, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f73  = [AO6], 1 * SIZE
	(p17) FMA	f104 = f21,   f81, f104
	}
	{ .mfi
	(p16) LDFD	f77  = [AO14], 1 * SIZE
	(p17) FMA	f116 = f21,   f85, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f74  = [AO6], 1 * SIZE
	(p17) FMA	f107 = f20,   f83, f107
	}
	{ .mfi
	(p16) LDFD	f78  = [AO14], 1 * SIZE
	(p17) FMA	f119 = f20,   f87, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f75  = [AO6], 5 * SIZE
	(p17) FMA	f110 = f21,   f83, f110
	}
	{ .mfi
	(p16) LDFD	f79  = [AO14], 5 * SIZE
	(p17) FMA	f122 = f21,   f87, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f21,   f82, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f21,   f86, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f20,   f82, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f20,   f86, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f21,   f84, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f21,   f88, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE7], 16 * SIZE
	(p17) ADD4	f110 = f20,   f84, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f20,   f88, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f80  = [AO7], 1 * SIZE
	(p17) FMA	f101 = f22,   f89, f101
	}
	{ .mfi
	(p16) LDFD	f84  = [AO15], 1 * SIZE
	(p17) FMA	f113 = f22,   f93, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f81  = [AO7], 1 * SIZE
	(p17) FMA	f104 = f23,   f89, f104
	}
	{ .mfi
	(p16) LDFD	f85  = [AO15], 1 * SIZE
	(p17) FMA	f116 = f23,   f93, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f82  = [AO7], 1 * SIZE
	(p17) FMA	f107 = f22,   f91, f107
	}
	{ .mfi
	(p16) LDFD	f86  = [AO15], 1 * SIZE
	(p17) FMA	f119 = f22,   f95, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f83  = [AO7], 5 * SIZE
	(p17) FMA	f110 = f23,   f91, f110
	}
	{ .mfi
	(p16) LDFD	f87  = [AO15], 5 * SIZE
	(p17) FMA	f122 = f23,   f95, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f23,   f90, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f23,   f94, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f22,   f90, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f22,   f94, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f23,   f92, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f23,   f96, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE8], 16 * SIZE
	(p17) ADD4	f110 = f22,   f92, f110
	}
	{ .mfb
	(p17) ADD4	f122 = f22,   f96, f122
	br.ctop.sptk.few .L12
	}
	;;
	.align 16

.L15:
	{ .mmi
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	}
	{ .mmi
	(p14) LDFD	f32  = [AO1], 1 * SIZE
	(p14) LDFD	f80  = [YLD1], 1 * SIZE
	cmp.lt p6, p0 = 1, J
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	adds J = -1, J
	}
	{
	(p14) LDFD	f33  = [AO1], 1 * SIZE
	(p14) LDFD	f81  = [YLD1], 1 * SIZE
	and   I = 3, M
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	(p6) cmp.eq.unc p7, p0 = I, r0
	}
	{ .mmi
	(p14) LDFD	f34  = [AO1], 1 * SIZE
	(p14) LDFD	f82  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	}
	{ .mmb
	(p14) LDFD	f35  = [AO1], 1 * SIZE
	(p14) LDFD	f83  = [YLD1], 1 * SIZE
	(p7) br.cond.dptk .L11
	}
	;;
	(p15) LDFD	f36  = [AO1], 1 * SIZE
	(p15) LDFD	f84  = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f37  = [AO1], 1 * SIZE
	(p15) LDFD	f85  = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f38  = [AO2], 1 * SIZE
	(p14) LDFD	f44  = [AO3], 1 * SIZE
	;;
	(p14) LDFD	f39  = [AO2], 1 * SIZE
	(p14) LDFD	f45  = [AO3], 1 * SIZE
	;;
	(p14) LDFD	f40  = [AO2], 1 * SIZE
	(p14) LDFD	f46  = [AO3], 1 * SIZE
	;;
	(p14) LDFD	f41  = [AO2], 1 * SIZE
	(p14) LDFD	f47  = [AO3], 1 * SIZE
	(p14) FMA	f80 = f8,  f32, f80
	;;
	(p15) LDFD	f42  = [AO2], 1 * SIZE
	(p15) LDFD	f48  = [AO3], 1 * SIZE
	(p14) FMA	f81 = f9,  f32, f81
	;;
	(p15) LDFD	f43  = [AO2], 1 * SIZE
	(p15) LDFD	f49  = [AO3], 1 * SIZE
	(p14) FMA	f82 = f8,  f34, f82
	;;
	(p14) LDFD	f50  = [AO4], 1 * SIZE
	(p14) LDFD	f56  = [AO5], 1 * SIZE
	(p14) FMA	f83 = f9,  f34, f83
	;;
	(p14) LDFD	f51  = [AO4], 1 * SIZE
	(p14) LDFD	f57  = [AO5], 1 * SIZE
	(p15) FMA	f84 = f8,  f36, f84
	;;
	(p14) LDFD	f52  = [AO4], 1 * SIZE
	(p14) LDFD	f58  = [AO5], 1 * SIZE
	(p15) FMA	f85 = f9,  f36, f85
	;;
	(p14) LDFD	f53  = [AO4], 1 * SIZE
	(p14) LDFD	f59  = [AO5], 1 * SIZE
	(p14) ADD3	f80 = f9,  f33, f80
	;;
	(p15) LDFD	f54  = [AO4], 1 * SIZE
	(p15) LDFD	f60  = [AO5], 1 * SIZE
	(p14) ADD4	f81 = f8,  f33, f81
	;;
	(p15) LDFD	f55  = [AO4], 1 * SIZE
	(p15) LDFD	f61  = [AO5], 1 * SIZE
	(p14) ADD3	f82 = f9,  f35, f82
	;;
	(p14) LDFD	f62  = [AO6], 1 * SIZE
	(p14) LDFD	f68  = [AO7], 1 * SIZE
	(p14) ADD4	f83 = f8,  f35, f83
	;;
	(p14) LDFD	f63  = [AO6], 1 * SIZE
	(p14) LDFD	f69  = [AO7], 1 * SIZE
	(p15) ADD3	f84 = f9,  f37, f84
	;;
	(p14) LDFD	f64  = [AO6], 1 * SIZE
	(p14) LDFD	f70  = [AO7], 1 * SIZE
	(p15) ADD4	f85 = f8,  f37, f85
	;;
	(p14) LDFD	f65  = [AO6], 1 * SIZE
	(p14) LDFD	f71  = [AO7], 1 * SIZE
	(p14) FMA	f80 = f10, f38, f80
	;;
	(p15) LDFD	f66  = [AO6], 1 * SIZE
	(p15) LDFD	f72  = [AO7], 1 * SIZE
	(p14) FMA	f81 = f11, f38, f81
	;;
	(p15) LDFD	f67  = [AO6], 1 * SIZE
	(p15) LDFD	f73  = [AO7], 1 * SIZE
	(p14) FMA	f82 = f10, f40, f82
	;;
	(p14) LDFD	f74  = [AO8], 1 * SIZE
	(p14) FMA	f83 = f11, f40, f83
	;;
	(p14) LDFD	f75  = [AO8], 1 * SIZE
	(p15) FMA	f84 = f10, f42, f84
	;;
	(p14) LDFD	f76  = [AO8], 1 * SIZE
	(p15) FMA	f85 = f11, f42, f85
	;;
	(p14) LDFD	f77  = [AO8], 1 * SIZE
	(p14) ADD3	f80 = f11, f39, f80
	;;
	(p15) LDFD	f78  = [AO8], 1 * SIZE
	(p14) ADD4	f81 = f10, f39, f81
	;;
	(p15) LDFD	f79  = [AO8], 1 * SIZE
	(p14) ADD3	f82 = f11, f41, f82
	(p14) ADD4	f83 = f10, f41, f83
	(p15) ADD3	f84 = f11, f43, f84
	(p15) ADD4	f85 = f10, f43, f85
	;;
	(p14) FMA	f80 = f12, f44, f80
	(p14) FMA	f81 = f13, f44, f81
	(p14) FMA	f82 = f12, f46, f82
	(p14) FMA	f83 = f13, f46, f83
	(p15) FMA	f84 = f12, f48, f84
	(p15) FMA	f85 = f13, f48, f85
	;;
	(p14) ADD3	f80 = f13, f45, f80
	(p14) ADD4	f81 = f12, f45, f81
	(p14) ADD3	f82 = f13, f47, f82
	(p14) ADD4	f83 = f12, f47, f83
	(p15) ADD3	f84 = f13, f49, f84
	(p15) ADD4	f85 = f12, f49, f85
	;;
	(p14) FMA	f80 = f14, f50, f80
	(p14) FMA	f81 = f15, f50, f81
	(p14) FMA	f82 = f14, f52, f82
	(p14) FMA	f83 = f15, f52, f83
	(p15) FMA	f84 = f14, f54, f84
	(p15) FMA	f85 = f15, f54, f85
	;;
	(p14) ADD3	f80 = f15, f51, f80
	(p14) ADD4	f81 = f14, f51, f81
	(p14) ADD3	f82 = f15, f53, f82
	(p14) ADD4	f83 = f14, f53, f83
	(p15) ADD3	f84 = f15, f55, f84
	(p15) ADD4	f85 = f14, f55, f85
	;;
	(p14) FMA	f80 = f16, f56, f80
	(p14) FMA	f81 = f17, f56, f81
	(p14) FMA	f82 = f16, f58, f82
	(p14) FMA	f83 = f17, f58, f83
	(p15) FMA	f84 = f16, f60, f84
	(p15) FMA	f85 = f17, f60, f85
	;;
	(p14) ADD3	f80 = f17, f57, f80
	(p14) ADD4	f81 = f16, f57, f81
	(p14) ADD3	f82 = f17, f59, f82
	(p14) ADD4	f83 = f16, f59, f83
	(p15) ADD3	f84 = f17, f61, f84
	(p15) ADD4	f85 = f16, f61, f85
	;;
	(p14) FMA	f80 = f18, f62, f80
	(p14) FMA	f81 = f19, f62, f81
	(p14) FMA	f82 = f18, f64, f82
	(p14) FMA	f83 = f19, f64, f83
	(p15) FMA	f84 = f18, f66, f84
	(p15) FMA	f85 = f19, f66, f85
	;;
	(p14) ADD3	f80 = f19, f63, f80
	(p14) ADD4	f81 = f18, f63, f81
	(p14) ADD3	f82 = f19, f65, f82
	(p14) ADD4	f83 = f18, f65, f83
	(p15) ADD3	f84 = f19, f67, f84
	(p15) ADD4	f85 = f18, f67, f85
	;;
	(p14) FMA	f80 = f20, f68, f80
	(p14) FMA	f81 = f21, f68, f81
	(p14) FMA	f82 = f20, f70, f82
	(p14) FMA	f83 = f21, f70, f83
	(p15) FMA	f84 = f20, f72, f84
	(p15) FMA	f85 = f21, f72, f85
	;;
	(p14) ADD3	f80 = f21, f69, f80
	(p14) ADD4	f81 = f20, f69, f81
	(p14) ADD3	f82 = f21, f71, f82
	(p14) ADD4	f83 = f20, f71, f83
	(p15) ADD3	f84 = f21, f73, f84
	(p15) ADD4	f85 = f20, f73, f85
	;;
	(p14) FMA	f80 = f22, f74, f80
	(p14) FMA	f81 = f23, f74, f81
	(p14) FMA	f82 = f22, f76, f82
	(p14) FMA	f83 = f23, f76, f83
	(p15) FMA	f84 = f22, f78, f84
	(p15) FMA	f85 = f23, f78, f85
	;;
	(p14) ADD3	f80 = f23,  f75, f80
	(p14) ADD4	f81 = f22,  f75, f81
	(p14) ADD3	f82 = f23,  f77, f82
	(p14) ADD4	f83 = f22,  f77, f83
	(p15) ADD3	f84 = f23,  f79, f84
	(p15) ADD4	f85 = f22,  f79, f85
	;;
	(p14) STFD	[YST1] = f80, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f81, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f82, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f83, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f84, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f85, 1 * SIZE
	(p6) br.cond.dptk .L11
	;;

.L20:
	{ .mmi
	mov	YLD1 = YY
	adds	YLD2 = 4 * SIZE, YY
	tbit.z	p6, p0 = N, 2
	}
	;;
	{ .mmb
	mov	YST1 = YY
	adds	YST2 = 4 * SIZE, YY
	(p6) br.cond.dpnt .L30
	}
	;;
	LDFD	f32 = [X], SIZE
	LDFD	f36 = [XX], SIZE
	mov	AO1 = A
	;;
	LDFD	f33 = [X], INCXM1
	LDFD	f37 = [XX], INCXM1
	add	AO2 = LDA, A
	;;
	LDFD	f34 = [X], SIZE
	LDFD	f38 = [XX], SIZE
	shladd	AO3 = LDA, 1, A
	;;
	LDFD	f35 = [X], INCX3M1
	LDFD	f39 = [XX], INCX3M1
	shladd	AO4 = LDA, 1, AO2
	;;
	shladd	A   = LDA, 2, A
	FMPY	f8  = ALPHA_R, f32
	adds	AO9  = 4 * SIZE, AO1
	FMPY	f9  = ALPHA_I, f32
	adds	AO10 = 4 * SIZE, AO2
	FMPY	f10 = ALPHA_R, f34
	adds	AO11 = 4 * SIZE, AO3
	FMPY	f11 = ALPHA_I, f34
	adds	AO12 = 4 * SIZE, AO4
	FMPY	f12 = ALPHA_R, f36
	mov	pr.rot= 0
	FMPY	f13 = ALPHA_I, f36
	shr	I = M, 2
	FMPY	f14 = ALPHA_R, f38
	tbit.nz	p14, p0 = M, 1
	FMPY	f15 = ALPHA_I, f38
	;;
	{ .mfi
	cmp.eq	p6, p0 = 0, I
	ADD1	f8  = ALPHA_I, f33, f8
	mov	ar.ec= 2
	}
	ADD2	f9  = ALPHA_R, f33, f9
	adds	I = -1, I
	ADD1	f10 = ALPHA_I, f35, f10
	adds	PREB   = RPREFETCH * SIZE, YLD1
	ADD2	f11 = ALPHA_R, f35, f11
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	ADD1	f12 = ALPHA_I, f37, f12
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	ADD2	f13 = ALPHA_R, f37, f13
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	ADD1	f14 = ALPHA_I, f39, f14
	ADD2	f15 = ALPHA_R, f39, f15
	;;
	{ .mib
	cmp.eq	p16, p0 = r0, r0
	mov	ar.lc = I
	(p6) br.cond.dpnt .L25
	}
	;;
	.align 16

.L22:
	{ .mfi
	(p17) LDFD	f57  = [AO4], 1 * SIZE
	(p17) FMA	f101 = f8,   f33, f101
	(p16) tbit.nz.unc	p12, p13 = I, 0
	}
	{ .mfi
	(p17) LDFD	f61  = [AO12], 1 * SIZE
	(p17) FMA	f113 = f8,   f37, f113
	}
	;;
	{ .mfi
	(p17) LDFD	f58  = [AO4], 1 * SIZE
	(p17) FMA	f104 = f9,   f33, f104
	(p16) adds	I = -1, I
	}
	{ .mfi
	(p17) LDFD	f62  = [AO12], 1 * SIZE
	(p17) FMA	f116 = f9,   f37, f116
	}
	;;
	{ .mfi
	(p17) LDFD	f59  = [AO4], 1 * SIZE
	(p17) FMA	f107 = f8,   f35, f107
	}
	{ .mfi
	(p17) LDFD	f63  = [AO12], 1 * SIZE
	(p17) FMA	f119 = f8,   f39, f119
	}
	;;
	{ .mfi
	(p17) LDFD	f60  = [AO4], 5 * SIZE
	(p17) FMA	f110 = f9,   f35, f110
	}
	{ .mfi
	(p17) LDFD	f64  = [AO12], 5 * SIZE
	(p17) FMA	f122 = f9,   f39, f122
	}
	;;
	{ .mfi
	(p12) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) ADD3	f101 = f9,   f34, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f9,   f38, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p17) ADD4	f104 = f8,   f34, f104
	}
	{ .mfi
	(p16) LDFD	f112 = [YLD2], 1 * SIZE
	(p17) ADD4	f116 = f8,   f38, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f103 = [YLD1], 1 * SIZE
	(p17) ADD3	f107 = f9,   f36, f107
	}
	{ .mfi
	(p16) LDFD	f115 = [YLD2], 1 * SIZE
	(p17) ADD3	f119 = f9,   f40, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE1], 16 * SIZE
	(p17) ADD4	f110 = f8,   f36, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f8,   f40, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f32  = [AO1], 1 * SIZE
	(p17) FMA	f101 = f10,   f41, f101
	}
	{ .mfi
	(p16) LDFD	f36  = [AO9], 1 * SIZE
	(p17) FMA	f113 = f10,   f45, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f33  = [AO1], 1 * SIZE
	(p17) FMA	f104 = f11,   f41, f104
	}
	{ .mfi
	(p16) LDFD	f37  = [AO9], 1 * SIZE
	(p17) FMA	f116 = f11,   f45, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f34  = [AO1], 1 * SIZE
	(p17) FMA	f107 = f10,   f43, f107
	}
	{ .mfi
	(p16) LDFD	f38  = [AO9], 1 * SIZE
	(p17) FMA	f119 = f10,   f47, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f35  = [AO1], 5 * SIZE
	(p17) FMA	f110 = f11,   f43, f110
	}
	{ .mfi
	(p16) LDFD	f39  = [AO9], 5 * SIZE
	(p17) FMA	f122 = f11,   f47, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f106 = [YLD1], 1 * SIZE
	(p17) ADD3	f101 = f11,   f42, f101
	}
	{ .mfi
	(p16) LDFD	f118 = [YLD2], 1 * SIZE
	(p17) ADD3	f113 = f11,   f46, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f109 = [YLD1], 5 * SIZE
	(p17) ADD4	f104 = f10,   f42, f104
	}
	{ .mfi
	(p16) LDFD	f121 = [YLD2], 5 * SIZE
	(p17) ADD4	f116 = f10,   f46, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f11,   f44, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f11,   f48, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE2], 16 * SIZE
	(p17) ADD4	f110 = f10,   f44, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f10,   f48, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f40  = [AO2], 1 * SIZE
	(p17) FMA	f101 = f12,   f49, f101
	}
	{ .mfi
	(p16) LDFD	f44  = [AO10], 1 * SIZE
	(p17) FMA	f113 = f12,   f53, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f41  = [AO2], 1 * SIZE
	(p17) FMA	f104 = f13,   f49, f104
	}
	{ .mfi
	(p16) LDFD	f45  = [AO10], 1 * SIZE
	(p17) FMA	f116 = f13,   f53, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	(p17) FMA	f107 = f12,   f51, f107
	}
	{ .mmf
	(p16) LDFD	f42  = [AO2], 1 * SIZE
	(p16) LDFD	f46  = [AO10], 1 * SIZE
	(p17) FMA	f119 = f12,   f55, f119
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	(p17) FMA	f110 = f13,   f51, f110
	}
	{ .mmf
	(p16) LDFD	f43  = [AO2], 5 * SIZE
	(p16) LDFD	f47  = [AO10], 5 * SIZE
	(p17) FMA	f122 = f13,   f55, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f13,   f50, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f13,   f54, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f12,   f50, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f12,   f54, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f13,   f52, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f13,   f56, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE3], 16 * SIZE
	(p17) ADD4	f110 = f12,   f52, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f12,   f56, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f48  = [AO3], 1 * SIZE
	(p17) FMA	f101 = f14,   f57, f101
	}
	{ .mfi
	(p16) LDFD	f52  = [AO11], 1 * SIZE
	(p17) FMA	f113 = f14,   f61, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f49  = [AO3], 1 * SIZE
	(p17) FMA	f104 = f15,   f57, f104
	}
	{ .mfi
	(p16) LDFD	f53  = [AO11], 1 * SIZE
	(p17) FMA	f116 = f15,   f61, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	(p17) FMA	f107 = f14,   f59, f107
	}
	{ .mmf
	(p16) LDFD	f50  = [AO3], 1 * SIZE
	(p16) LDFD	f54  = [AO11], 1 * SIZE
	(p17) FMA	f119 = f14,   f63, f119
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	(p17) FMA	f110 = f15,   f59, f110
	}
	{ .mmf
	(p16) LDFD	f51  = [AO3], 5 * SIZE
	(p16) LDFD	f55  = [AO11], 5 * SIZE
	(p17) FMA	f122 = f15,   f63, f122
	}
	;;
	{ .mfi
	(p17) ADD3	f101 = f15,   f58, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f15,   f62, f113
	}
	;;
	{ .mfi
	(p17) ADD4	f104 = f14,   f58, f104
	}
	{ .mfi
	(p17) ADD4	f116 = f14,   f62, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f15,   f60, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f15,   f64, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE4], 16 * SIZE
	(p17) ADD4	f110 = f14,   f60, f110
	}
	{ .mfb
	(p17) ADD4	f122 = f14,   f64, f122
	br.ctop.sptk.few .L22
	}
	;;
	.align 16

.L25:
	{ .mmi
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	}
	{ .mmi
	(p14) LDFD	f32  = [AO1], 1 * SIZE
	(p14) LDFD	f80  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f33  = [AO1], 1 * SIZE
	(p14) LDFD	f81  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f34  = [AO1], 1 * SIZE
	(p14) LDFD	f82  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	}
	{ .mmi
	(p14) LDFD	f35  = [AO1], 1 * SIZE
	(p14) LDFD	f83  = [YLD1], 1 * SIZE
	}
	;;
	(p15) LDFD	f36  = [AO1], 1 * SIZE
	(p15) LDFD	f84  = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f37  = [AO1], 1 * SIZE
	(p15) LDFD	f85  = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f38  = [AO2], 1 * SIZE
	(p14) LDFD	f44  = [AO3], 1 * SIZE
	;;

	(p14) LDFD	f39  = [AO2], 1 * SIZE
	(p14) LDFD	f45  = [AO3], 1 * SIZE
	(p14) FMA	f80 = f8,  f32, f80
	;;
	(p14) LDFD	f40  = [AO2], 1 * SIZE
	(p14) LDFD	f46  = [AO3], 1 * SIZE
	(p14) FMA	f81 = f9,  f32, f81
	;;
	(p14) LDFD	f41  = [AO2], 1 * SIZE
	(p14) LDFD	f47  = [AO3], 1 * SIZE
	(p14) FMA	f82 = f8,  f34, f82
	;;
	(p15) LDFD	f42  = [AO2], 1 * SIZE
	(p15) LDFD	f48  = [AO3], 1 * SIZE
	(p14) FMA	f83 = f9,  f34, f83
	;;
	(p15) LDFD	f43  = [AO2], 1 * SIZE
	(p15) LDFD	f49  = [AO3], 1 * SIZE
	(p15) FMA	f84 = f8,  f36, f84
	;;
	(p14) LDFD	f50  = [AO4], 1 * SIZE
	(p15) FMA	f85 = f9,  f36, f85
	;;
	(p14) LDFD	f51  = [AO4], 1 * SIZE
	(p14) ADD3	f80 = f9,  f33, f80
	;;
	(p14) LDFD	f52  = [AO4], 1 * SIZE
	(p14) ADD4	f81 = f8,  f33, f81
	;;
	(p14) LDFD	f53  = [AO4], 1 * SIZE
	(p14) ADD3	f82 = f9,  f35, f82
	;;
	(p15) LDFD	f54  = [AO4], 1 * SIZE
	(p14) ADD4	f83 = f8,  f35, f83
	;;
	(p15) LDFD	f55  = [AO4], 1 * SIZE
	(p15) ADD3	f84 = f9,  f37, f84
	(p15) ADD4	f85 = f8,  f37, f85
	;;
	(p14) FMA	f80 = f10, f38, f80
	(p14) FMA	f81 = f11, f38, f81
	(p14) FMA	f82 = f10, f40, f82
	(p14) FMA	f83 = f11, f40, f83
	(p15) FMA	f84 = f10, f42, f84
	(p15) FMA	f85 = f11, f42, f85
	;;
	(p14) ADD3	f80 = f11, f39, f80
	(p14) ADD4	f81 = f10, f39, f81
	(p14) ADD3	f82 = f11, f41, f82
	(p14) ADD4	f83 = f10, f41, f83
	(p15) ADD3	f84 = f11, f43, f84
	(p15) ADD4	f85 = f10, f43, f85
	;;
	(p14) FMA	f80 = f12, f44, f80
	(p14) FMA	f81 = f13, f44, f81
	(p14) FMA	f82 = f12, f46, f82
	(p14) FMA	f83 = f13, f46, f83
	(p15) FMA	f84 = f12, f48, f84
	(p15) FMA	f85 = f13, f48, f85
	;;
	(p14) ADD3	f80 = f13, f45, f80
	(p14) ADD4	f81 = f12, f45, f81
	(p14) ADD3	f82 = f13, f47, f82
	(p14) ADD4	f83 = f12, f47, f83
	(p15) ADD3	f84 = f13, f49, f84
	(p15) ADD4	f85 = f12, f49, f85
	;;
	(p14) FMA	f80 = f14, f50, f80
	(p14) FMA	f81 = f15, f50, f81
	(p14) FMA	f82 = f14, f52, f82
	(p14) FMA	f83 = f15, f52, f83
	(p15) FMA	f84 = f14, f54, f84
	(p15) FMA	f85 = f15, f54, f85
	;;
	(p14) ADD3	f80 = f15, f51, f80
	(p14) ADD4	f81 = f14, f51, f81
	(p14) ADD3	f82 = f15, f53, f82
	(p14) ADD4	f83 = f14, f53, f83
	(p15) ADD3	f84 = f15, f55, f84
	(p15) ADD4	f85 = f14, f55, f85
	;;
	(p14) STFD	[YST1] = f80, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f81, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f82, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f83, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f84, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f85, 1 * SIZE
	;;

.L30:
	{ .mmi
	mov	YLD1 = YY
	adds	YLD2 = 4 * SIZE, YY
	tbit.z	p6, p0 = N, 1
	}
	;;
	{ .mmb
	mov	YST1 = YY
	adds	YST2 = 4 * SIZE, YY
	(p6) br.cond.dpnt .L40
	}
	;;
	LDFD	f32 = [X], SIZE
	mov	AO1 = A
	mov	pr.rot= 0
	;;
	LDFD	f33 = [X], INCXM1
	add	AO2 = A, LDA
	shr	I = M, 2
	;;
	LDFD	f34 = [X], SIZE
	shladd	A   = LDA, 1, A
	tbit.nz	p14, p0 = M, 1
	;;
	LDFD	f35 = [X], INCXM1
	cmp.eq	p6, p0 = 0, I
	;;
	FMPY	f8  = ALPHA_R, f32
	adds	AO9  = 4 * SIZE, AO1
	FMPY	f9  = ALPHA_I, f32
	adds	AO10 = 4 * SIZE, AO2
	FMPY	f10 = ALPHA_R, f34
	mov	ar.ec= 2
	FMPY	f11 = ALPHA_I, f34
	;;
	adds	PREB   = RPREFETCH * SIZE, YLD1
	adds	I = -1, I
	ADD1	f8  = ALPHA_I, f33, f8
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	ADD2	f9  = ALPHA_R, f33, f9
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	ADD1	f10 = ALPHA_I, f35, f10
	ADD2	f11 = ALPHA_R, f35, f11
	;;
	{ .mib
	cmp.eq	p16, p0 = r0, r0
	mov	ar.lc = I
	(p6) br.cond.dpnt .L35
	}
	;;
	.align 16

.L32:
	{ .mfi
	(p17) LDFD	f41  = [AO2], 1 * SIZE
	(p17) FMA	f101 = f8,   f33, f101
	(p16) tbit.nz.unc	p12, p13 = I, 0
	}
	{ .mfi
	(p17) LDFD	f45  = [AO10], 1 * SIZE
	(p17) FMA	f113 = f8,   f37, f113
	}
	;;
	{ .mfi
	(p17) LDFD	f42  = [AO2], 1 * SIZE
	(p17) FMA	f104 = f9,   f33, f104
	(p16) adds	I = -1, I
	}
	{ .mfi
	(p17) LDFD	f46  = [AO10], 1 * SIZE
	(p17) FMA	f116 = f9,   f37, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	(p17) FMA	f107 = f8,   f35, f107
	}
	{ .mmf
	(p17) LDFD	f43  = [AO2], 1 * SIZE
	(p17) LDFD	f47  = [AO10], 1 * SIZE
	(p17) FMA	f119 = f8,   f39, f119
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	(p17) FMA	f110 = f9,   f35, f110
	}
	{ .mmf
	(p17) LDFD	f44  = [AO2], 5 * SIZE
	(p17) LDFD	f48  = [AO10], 5 * SIZE
	(p17) FMA	f122 = f9,   f39, f122
	}
	;;
	{ .mfi
	(p12) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) ADD3	f101 = f9,   f34, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f9,   f38, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p17) ADD4	f104 = f8,   f34, f104
	}
	{ .mfi
	(p16) LDFD	f112 = [YLD2], 1 * SIZE
	(p17) ADD4	f116 = f8,   f38, f116
	}
	;;
	{ .mfi
	(p16) LDFD	f103 = [YLD1], 1 * SIZE
	(p17) ADD3	f107 = f9,   f36, f107
	}
	{ .mfi
	(p16) LDFD	f115 = [YLD2], 1 * SIZE
	(p17) ADD3	f119 = f9,   f40, f119
	}
	;;
	{ .mfi
	(p12) PREFETCH	[RPRE1], 16 * SIZE
	(p17) ADD4	f110 = f8,   f36, f110
	}
	{ .mfi
	(p17) ADD4	f122 = f8,   f40, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f32  = [AO1], 1 * SIZE
	(p17) FMA	f101 = f10,   f41, f101
	}
	{ .mfi
	(p16) LDFD	f36  = [AO9], 1 * SIZE
	(p17) FMA	f113 = f10,   f45, f113
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	(p17) FMA	f104 = f11,   f41, f104
	}
	{ .mmf
	(p16) LDFD	f33  = [AO1], 1 * SIZE
	(p16) LDFD	f37  = [AO9], 1 * SIZE
	(p17) FMA	f116 = f11,   f45, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	(p17) FMA	f107 = f10,   f43, f107
	}
	{ .mmf
	(p16) LDFD	f34  = [AO1], 1 * SIZE
	(p16) LDFD	f38  = [AO9], 1 * SIZE
	(p17) FMA	f119 = f10,   f47, f119
	}
	;;
	{ .mfi
	(p16) LDFD	f35  = [AO1], 5 * SIZE
	(p17) FMA	f110 = f11,   f43, f110
	}
	{ .mfi
	(p16) LDFD	f39  = [AO9], 5 * SIZE
	(p17) FMA	f122 = f11,   f47, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f106 = [YLD1], 1 * SIZE
	(p17) ADD3	f101 = f11,   f42, f101
	}
	{ .mfi
	(p16) LDFD	f118 = [YLD2], 1 * SIZE
	(p17) ADD3	f113 = f11,   f46, f113
	}
	;;
	{ .mfi
	(p16) LDFD	f109 = [YLD1], 5 * SIZE
	(p17) ADD4	f104 = f10,   f42, f104
	}
	{ .mfi
	(p16) LDFD	f121 = [YLD2], 5 * SIZE
	(p17) ADD4	f116 = f10,   f46, f116
	}
	;;
	{ .mfi
	(p17) ADD3	f107 = f11,   f44, f107
	}
	{ .mfi
	(p17) ADD3	f119 = f11,   f48, f119
	}
	;;
	{ .mfi
	(p13) PREFETCH	[RPRE2], 16 * SIZE
	(p17) ADD4	f110 = f10,   f44, f110
	}
	{ .mfb
	(p17) ADD4	f122 = f10,   f48, f122
	br.ctop.sptk.few .L32
	}
	;;
	.align 16

.L35:
	{ .mmi
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	}
	{ .mmi
	(p14) LDFD	f32  = [AO1], 1 * SIZE
	(p14) LDFD	f80  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f33  = [AO1], 1 * SIZE
	(p14) LDFD	f81  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f34  = [AO1], 1 * SIZE
	(p14) LDFD	f82  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	}
	{ .mmi
	(p14) LDFD	f35  = [AO1], 1 * SIZE
	(p14) LDFD	f83  = [YLD1], 1 * SIZE
	}
	;;
	(p15) LDFD	f36  = [AO1], 1 * SIZE
	(p15) LDFD	f84  = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f37  = [AO1], 1 * SIZE
	(p15) LDFD	f85  = [YLD1], 1 * SIZE
	;;
	(p14) LDFD	f38  = [AO2], 1 * SIZE
	(p14) FMA	f80 = f8,  f32, f80
	;;
	(p14) LDFD	f39  = [AO2], 1 * SIZE
	(p14) FMA	f81 = f9,  f32, f81
	;;
	(p14) LDFD	f40  = [AO2], 1 * SIZE
	(p14) FMA	f82 = f8,  f34, f82
	;;
	(p14) LDFD	f41  = [AO2], 1 * SIZE
	(p14) FMA	f83 = f9,  f34, f83
	;;
	(p15) LDFD	f42  = [AO2], 1 * SIZE
	(p15) FMA	f84 = f8,  f36, f84
	;;
	(p15) LDFD	f43  = [AO2], 1 * SIZE
	(p15) FMA	f85 = f9,  f36, f85
	;;
	(p14) ADD3	f80 = f9,  f33, f80
	(p14) ADD4	f81 = f8,  f33, f81
	(p14) ADD3	f82 = f9,  f35, f82
	(p14) ADD4	f83 = f8,  f35, f83
	(p15) ADD3	f84 = f9,  f37, f84
	(p15) ADD4	f85 = f8,  f37, f85
	;;
	(p14) FMA	f80 = f10, f38, f80
	(p14) FMA	f81 = f11, f38, f81
	(p14) FMA	f82 = f10, f40, f82
	(p14) FMA	f83 = f11, f40, f83
	(p15) FMA	f84 = f10, f42, f84
	(p15) FMA	f85 = f11, f42, f85
	;;
	(p14) ADD3	f80 = f11, f39, f80
	(p14) ADD4	f81 = f10, f39, f81
	(p14) ADD3	f82 = f11, f41, f82
	(p14) ADD4	f83 = f10, f41, f83
	(p15) ADD3	f84 = f11, f43, f84
	(p15) ADD4	f85 = f10, f43, f85
	;;
	(p14) STFD	[YST1] = f80, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f81, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f82, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f83, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f84, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f85, 1 * SIZE
	;;

.L40:
	{ .mmi
	mov	YLD1 = YY
	adds	YLD2 = 4 * SIZE, YY
	tbit.z	p6, p0 = N, 0
	}
	{ .mmb
	mov	YST1 = YY
	adds	YST2 = 4 * SIZE, YY
	(p6) br.cond.dpnt .L990
	}
	;;
	LDFD	f32 = [X], SIZE
	mov	AO1 = A
	adds	AO9 = 4 * SIZE, A
	;;
	LDFD	f33 = [X], INCXM1
	add	A   = A, LDA
	mov	pr.rot= 0
	;;
	{ .mfi
	adds	PREB   = RPREFETCH * SIZE, YLD1
	FMPY	f8  = ALPHA_R, f32
	mov	ar.ec= 2
	}
	{ .mfi
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	FMPY	f9  = ALPHA_I, f32
	shr	I = M, 2
	}
	;;
	{ .mmf
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	ADD1	f8  = ALPHA_I, f33, f8
	}
	{ .mfi
	adds	I = -1, I
	ADD2	f9  = ALPHA_R, f33, f9
	tbit.nz	p14, p0 = M, 1
	}
	;;
	{ .mib
	nop	__LINE__
	mov	ar.lc = I
	(p6) br.cond.dpnt .L45
	}
	;;
	.align 16

.L42:
	{ .mmf
	(p16) LDFD	f100 = [YLD1], 1 * SIZE
	(p16) LDFD	f112 = [YLD2], 1 * SIZE
	(p17) FMA	f101 = f8,   f33, f101
	}
	{ .mmf
	(p16) LDFD	f32  = [AO1], 1 * SIZE
	(p16) LDFD	f44  = [AO9], 1 * SIZE
	(p17) FMA	f113 = f8,   f45, f113
	}
	;;
	{ .mmf
	(p16) LDFD	f103 = [YLD1], 1 * SIZE
	(p16) LDFD	f115 = [YLD2], 1 * SIZE
	(p17) FMA	f104 = f9,   f33, f104
	}
	{ .mmf
	(p16) LDFD	f35  = [AO1], 1 * SIZE
	(p16) LDFD	f47  = [AO9], 1 * SIZE
	(p17) FMA	f116 = f9,   f45, f116
	}
	;;
	{ .mmf
	(p16) LDFD	f106 = [YLD1], 1 * SIZE
	(p16) LDFD	f118 = [YLD2], 1 * SIZE
	(p17) FMA	f107 = f8,   f39, f107
	}
	{ .mmf
	(p16) LDFD	f38  = [AO1], 1 * SIZE
	(p16) LDFD	f50  = [AO9], 1 * SIZE
	(p17) FMA	f119 = f8,   f51, f119
	}
	;;
	{ .mmf
	(p16) LDFD	f109 = [YLD1], 5 * SIZE
	(p16) LDFD	f121 = [YLD2], 5 * SIZE
	(p17) FMA	f110 = f9,   f39, f110
	}
	{ .mmf
	(p16) LDFD	f41  = [AO1], 5 * SIZE
	(p16) LDFD	f53  = [AO9], 5 * SIZE
	(p17) FMA	f122 = f9,   f51, f122
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	(p17) ADD3	f101 = f9,   f36, f101
	}
	{ .mfi
	(p17) ADD3	f113 = f9,   f48, f113
	(p16) tbit.nz.unc	p12, p13 = I, 0
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	(p17) ADD4	f104 = f8,   f36, f104
	}
	{ .mfi
	(p12) PREFETCH	[RPRE1], 16 * SIZE
	(p17) ADD4	f116 = f8,   f48, f116
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	(p17) ADD3	f107 = f9,   f42, f107
	}
	{ .mfi
	(p13) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) ADD3	f119 = f9,   f54, f119
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	(p17) ADD4	f110 = f8,   f42, f110
	}
	{ .mfb
	(p17) ADD4	f122 = f8,   f54, f122
	br.ctop.sptk.few .L42
	}
	;;
	.align 16

.L45:
	{ .mmi
	(p18) STFD	[YST1] = f102, 1 * SIZE
	(p18) STFD	[YST2] = f114, 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	}
	{ .mmi
	(p14) LDFD	f32  = [AO1], 1 * SIZE
	(p14) LDFD	f80  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f105, 1 * SIZE
	(p18) STFD	[YST2] = f117, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f33  = [AO1], 1 * SIZE
	(p14) LDFD	f81  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f108, 1 * SIZE
	(p18) STFD	[YST2] = f120, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f34  = [AO1], 1 * SIZE
	(p14) LDFD	f82  = [YLD1], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f111, 5 * SIZE
	(p18) STFD	[YST2] = f123, 5 * SIZE
	}
	{ .mmi
	(p14) LDFD	f35  = [AO1], 1 * SIZE
	(p14) LDFD	f83  = [YLD1], 1 * SIZE
	}
	;;
	(p15) LDFD	f36  = [AO1], 1 * SIZE
	(p15) LDFD	f84  = [YLD1], 1 * SIZE
	;;
	(p15) LDFD	f37  = [AO1], 1 * SIZE
	(p15) LDFD	f85  = [YLD1], 1 * SIZE
	;;
	(p14) FMA	f80 = f8,  f32, f80
	(p14) FMA	f81 = f9,  f32, f81
	(p14) FMA	f82 = f8,  f34, f82
	(p14) FMA	f83 = f9,  f34, f83
	(p15) FMA	f84 = f8,  f36, f84
	(p15) FMA	f85 = f9,  f36, f85
	;;
	(p14) ADD3	f80 = f9,  f33, f80
	(p14) ADD4	f81 = f8,  f33, f81
	(p14) ADD3	f82 = f9,  f35, f82
	(p14) ADD4	f83 = f8,  f35, f83
	(p15) ADD3	f84 = f9,  f37, f84
	(p15) ADD4	f85 = f8,  f37, f85
	;;
	(p14) STFD	[YST1] = f80, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f81, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f82, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f83, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f84, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f85, 1 * SIZE
	;;

.L990:
	{ .mmi
	mov	YST1 = Y
	mov	YST2 = Y
	mov	pr.rot= 0
	}
	{ .mib
	mov	YLD1 = YY
	shr	J = M, 2
	(p10) br.cond.dptk .L999
	}
	;;
	{ .mmi
	cmp.eq	p6, p0 = r0, J
	adds	INCY = - SIZE, INCY
	mov	ar.ec = 4
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	adds	J = -1, J
	tbit.nz	p13, p0 = M, 1
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
	(p19)	STFD	[YST2] = f35, 1 * SIZE
	(p18)	FADD	f34 = f34, f66
	}
	{ .mmi
	(p16)	LDFD	f64 = [YLD1], 1 * SIZE
	(p16)	LDFD	f32 = [YST1], 1 * SIZE
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f39
	(p18)	FADD	f38 = f38, f70
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f68 = [YLD1], 1 * SIZE
	(p16)	LDFD	f36 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f43, 1 * SIZE
	(p18)	FADD	f42 = f42, f74
	}
	{ .mmi
	(p16)	LDFD	f72 = [YLD1], 1 * SIZE
	(p16)	LDFD	f40 = [YST1], 1 * SIZE
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f47
	(p18)	FADD	f50 = f50, f82
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f76 = [YLD1], 1 * SIZE
	(p16)	LDFD	f44 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f51, 1 * SIZE
	(p18)	FADD	f54 = f54, f86
	}
	{ .mmi
	(p16)	LDFD	f80 = [YLD1], 1 * SIZE
	(p16)	LDFD	f48 = [YST1], 1 * SIZE
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f55
	(p18)	FADD	f58 = f58, f90
	(p19)	add YST2 = YST2, INCY
	}
	{ .mmi
	(p16)	LDFD	f84 = [YLD1], 1 * SIZE
	(p16)	LDFD	f52 = [YST1], INCY
	}
	;;
	{ .mfi
	(p19)	STFD	[YST2] = f59, 1 * SIZE
	(p18)	FADD	f46 = f46, f78
	}
	{ .mmi
	(p16)	LDFD	f88 = [YLD1], 1 * SIZE
	(p16)	LDFD	f56 = [YST1], 1 * SIZE
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
	(p13)	LDFD	f32 = [YST1], 1 * SIZE
	(p13)	LDFD	f40 = [YLD1], 1 * SIZE
	tbit.nz	p14, p0 = M, 0
	;;
	(p13)	LDFD	f33 = [YST1], INCY
	(p13)	LDFD	f41 = [YLD1], 1 * SIZE
	;;
	(p13)	LDFD	f34 = [YST1], 1 * SIZE
	(p13)	LDFD	f42 = [YLD1], 1 * SIZE
	;;
	(p13)	LDFD	f35 = [YST1], INCY
	(p13)	LDFD	f43 = [YLD1], 1 * SIZE
	;;
	(p14)	LDFD	f36 = [YST1], 1 * SIZE
	(p14)	LDFD	f44 = [YLD1], 1 * SIZE
	;;
	(p14)	LDFD	f37 = [YST1], INCY
	(p14)	LDFD	f45 = [YLD1], 1 * SIZE
	;;
	(p13)	FADD	f32 = f32, f40
	(p13)	FADD	f33 = f33, f41
	(p13)	FADD	f34 = f34, f42
	(p13)	FADD	f35 = f35, f43
	(p14)	FADD	f36 = f36, f44
	(p14)	FADD	f37 = f37, f45
	;;
	(p13)	STFD	[YST2] = f32, 1 * SIZE
	;;
	(p13)	STFD	[YST2] = f33
	(p13)	add YST2 = YST2, INCY
	;;
	(p13)	STFD	[YST2] = f34, 1 * SIZE
	;;
	(p13)	STFD	[YST2] = f35
	(p13)	add YST2 = YST2, INCY
	;;
	(p14)	STFD	[YST2] = f36, 1 * SIZE
	;;
	(p14)	STFD	[YST2] = f37
	;;

.L999:
	mov	r8 = r0
	adds	r9 = 1 * 16, SP
	;;
	ldf.fill  f16 = [SP], 32
	ldf.fill  f17 = [r9], 32
	mov	 ar.lc = ARLC
	;;
	ldf.fill  f18 = [SP], 32
	ldf.fill  f19 = [r9], 32
	mov pr    = PR, -1
	;;
	ldf.fill  f20 = [SP], 32
	ldf.fill  f21 = [r9], 32
	mov	ar.pfs = ARPFS
	;;
	ldf.fill  f22 = [SP], 32
	ldf.fill  f23 = [r9]
	br.ret.sptk.many b0
	;;
	EPILOGUE
