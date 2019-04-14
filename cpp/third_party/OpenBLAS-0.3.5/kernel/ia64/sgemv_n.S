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
#define A	r36
#define LDA	r37
#define X	r38
#define INCX	r39
#define Y	r34
#define INCY	r35
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
#define YST1	r25
#define YST2	r27
#define MM	r28
#define YY	r9

#define RPRE1	loc0
#define RPRE2	loc1
#define RPRE3	loc2
#define RPRE4	loc3
#define RPRE5	loc4
#define RPRE6	loc5
#define RPRE7	loc6
#define RPRE8	loc7

#define AO11	loc8
#define AO21	loc9
#define AO31	loc10
#define AO41	loc11
#define AO51	loc12
#define AO61	loc13
#define AO71	loc14
#define AO81	loc15

#define PREB	r8

#define ARLC	r29
#define PR	r30
#define ARPFS	r31

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
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 16, 8, 0
	mov	ARLC  = ar.lc
	}
	;;
	mov	PR = pr
	adds	r14 = 16, SP
	adds	r15 = 24, SP
	adds	r16 = 32, SP
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
	.body
	;;

	ld8	Y      = [r14]
	ld8	INCY   = [r15]
	ld8	BUFFER = [r16]

	mov	ALPHA = f8
	cmp.ge	p7, p0 = 0, M
	cmp.ge	p6, p0 = 0, N
	;;
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	LDA  = LDA,  BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	;;
	tbit.nz	p8, p0 = A,   BASE_SHIFT
	tbit.nz	p9, p0 = LDA, BASE_SHIFT
	mov	MM  = M
	;;
	(p8) adds MM = -1, M
	;;
	(p7) br.cond.dpnt .L999
	(p6) br.cond.dpnt .L999
	;;
	sub	I = A, Y
	cmp.eq	p10, p0 = SIZE, INCY
	mov	YY = Y
	;;
	(p10) tbit.z.unc p10, p0 = I, BASE_SHIFT
	;;
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
	(p9)	br.cond.dptk	.L100

	shr	J   = N, 3
	;;
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L20
	;;
	.align 16

.L11:
	mov	YLD1 = YY
	mov	YST1 = YY
	;;
	LDFD	f8 = [X], INCX
	;;
	LDFD	f9 = [X], INCX
	;;
	LDFD	f10 = [X], INCX
	;;
	LDFD	f11 = [X], INCX
	;;
	LDFD	f12 = [X], INCX
	;;
	LDFD	f13 = [X], INCX
	;;
	LDFD	f14 = [X], INCX
	;;
	LDFD	f15 = [X], INCX
	;;
	FMPY	f8  = ALPHA, f8
	FMPY	f9  = ALPHA, f9
	FMPY	f10 = ALPHA, f10
	FMPY	f11 = ALPHA, f11
	FMPY	f12 = ALPHA, f12
	FMPY	f13 = ALPHA, f13
	FMPY	f14 = ALPHA, f14
	FMPY	f15 = ALPHA, f15
	;;
	mov	AO1 = A
	add	AO2 = LDA, A
	;;
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	;;
	shladd	AO5 = LDA, 1, AO3
	shladd	AO6 = LDA, 1, AO4
	;;
	shladd	AO7 = LDA, 1, AO5
	shladd	AO8 = LDA, 1, AO6
	shladd	A   = LDA, 3, A
	;;
	;;
	adds	PREB   = RPREFETCH * SIZE, YLD1
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	adds	RPRE5  = RPREFETCH * SIZE, AO5
	adds	RPRE6  = (RPREFETCH + 8) * SIZE, AO6
	adds	RPRE7  = RPREFETCH * SIZE, AO7
	adds	RPRE8  = (RPREFETCH + 8) * SIZE, AO8

	(p8) LDFD	f80 = [AO1], 1 * SIZE
	(p8) LDFD	f81 = [AO2], 1 * SIZE
	(p8) LDFD	f82 = [AO3], 1 * SIZE
	(p8) LDFD	f83 = [AO4], 1 * SIZE
	(p8) LDFD	f84 = [AO5], 1 * SIZE
	(p8) LDFD	f85 = [AO6], 1 * SIZE
	(p8) LDFD	f86 = [AO7], 1 * SIZE
	(p8) LDFD	f87 = [AO8], 1 * SIZE
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	;;
	(p8) FMPY	f32 = f8,  f80
	(p8) FMPY	f33 = f9,  f81
	(p8) FMPY	f34 = f10, f82
	(p8) FMA	f35 = f11, f83, f106
	;;
	(p8) FMA	f32 = f12, f84, f32
	(p8) FMA	f33 = f13, f85, f33
	(p8) FMA	f34 = f14, f86, f34
	(p8) FMA	f35 = f15, f87, f35
	;;
	(p8) FADD	f32 = f32, f33
	(p8) FADD	f34 = f34, f35
	;;
	(p8) FADD	f32 = f32, f34
	;;
	(p8) STFD	[YST1] = f32, 1 * SIZE

	shr	I = MM, 3
	mov	pr.rot= 0
	;;
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	tbit.nz	p13, p0 = MM, 2
	;;
	mov	ar.lc = I
	mov	ar.ec= 2
	(p6) br.cond.dpnt .L15
	;;
	.align 16

.L12:
	{ .mfi
	(p17) LDFPD	f95,  f96  = [AO8], 2 * SIZE
	(p17) FMA	f101 = f8,  f33, f101
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mfi
	(p17) FMA	f104 = f8,  f34, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p17) FMA	f107 = f8,  f35, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p17) FMA	f110 = f8,  f36, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f34,  f35  = [AO1], 2 * SIZE
	(p17) FMA	f113 = f8,  f37, f113
	}
	{ .mfi
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p17) FMA	f116 = f8,  f38, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p17) FMA	f119 = f8,  f39, f119
	}
	{ .mfi
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p17) FMA	f122 = f8,  f40, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f38,  f39  = [AO1], 2 * SIZE
	(p17) FMA	f101 = f9,  f41, f101
	}
	{ .mfi
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p17) FMA	f104 = f9,  f42, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f40,  f41  = [AO2], 2 * SIZE
	(p17) FMA	f107 = f9,  f43, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE2], 16 * SIZE
	(p17) FMA	f110 = f9,  f44, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f42,  f43  = [AO2], 2 * SIZE
	(p17) FMA	f113 = f9,  f45, f113
	}
	{ .mfi
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p17) FMA	f116 = f9,  f46, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f44,  f45  = [AO2], 2 * SIZE
	(p17) FMA	f119 = f9,  f47, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	(p17) FMA	f122 = f9,  f48, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f46,  f47  = [AO2], 2 * SIZE
	(p17) FMA	f101 = f10, f49, f101
	}
	{ .mfi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	(p17) FMA	f104 = f10, f50, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f48,  f49  = [AO3], 2 * SIZE
	(p17) FMA	f107 = f10, f51, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE3], 16 * SIZE
	(p17) FMA	f110 = f10, f52, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f50,  f51  = [AO3], 2 * SIZE
	(p17) FMA	f113 = f10, f53, f113
	}
	{ .mfi
	(p17) FMA	f116 = f10, f54, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	(p17) FMA	f119 = f10, f55, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p17) FMA	f122 = f10, f56, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f54,  f55  = [AO3], 2 * SIZE
	(p17) FMA	f101 = f11, f57, f101
	}
	{ .mfi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p17) FMA	f104 = f11, f58, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f56,  f57  = [AO4], 2 * SIZE
	(p17) FMA	f107 = f11, f59, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE4], 16 * SIZE
	(p17) FMA	f110 = f11, f60, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f58,  f59  = [AO4], 2 * SIZE
	(p17) FMA	f113 = f11, f61, f113
	}
	{ .mfi
	(p17) FMA	f116 = f11, f62, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f60,  f61  = [AO4], 2 * SIZE
	(p17) FMA	f119 = f11, f63, f119
	}
	{ .mfi
	(p17) FMA	f122 = f11, f64, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f62,  f63  = [AO4], 2 * SIZE
	(p17) FMA	f101 = f12, f65, f101
	}
	{ .mfi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	(p17) FMA	f104 = f12, f66, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f64,  f65  = [AO5], 2 * SIZE
	(p17) FMA	f107 = f12, f67, f107
	}
	{ .mfi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	(p17) FMA	f110 = f12, f68, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f66,  f67  = [AO5], 2 * SIZE
	(p17) FMA	f113 = f12, f69, f113
	}
	{ .mfi
	(p14) PREFETCH	[RPRE5], 16 * SIZE
	(p17) FMA	f116 = f12, f70, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f68,  f69  = [AO5], 2 * SIZE
	(p17) FMA	f119 = f12, f71, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	(p17) FMA	f122 = f12, f72, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f70,  f71  = [AO5], 2 * SIZE
	(p17) FMA	f101 = f13, f73, f101
	}
	{ .mfi
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p17) FMA	f104 = f13, f74, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f72,  f73  = [AO6], 2 * SIZE
	(p17) FMA	f107 = f13, f75, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE6], 16 * SIZE
	(p17) FMA	f110 = f13, f76, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f74,  f75  = [AO6], 2 * SIZE
	(p17) FMA	f113 = f13, f77, f113
	}
	{ .mfi
	(p17) FMA	f116 = f13, f78, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f76,  f77  = [AO6], 2 * SIZE
	(p17) FMA	f119 = f13, f79, f119
	}
	{ .mfi
	(p17) FMA	f122 = f13, f80, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f78,  f79  = [AO6], 2 * SIZE
	(p17) FMA	f101 = f14, f81, f101
	}
	{ .mfi
	(p17) FMA	f104 = f14, f82, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f80,  f81  = [AO7], 2 * SIZE
	(p17) FMA	f107 = f14, f83, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE7], 16 * SIZE
	(p17) FMA	f110 = f14, f84, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f82,  f83  = [AO7], 2 * SIZE
	(p17) FMA	f113 = f14, f85, f113
	}
	{ .mfi
	(p17) FMA	f116 = f14, f86, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f84,  f85  = [AO7], 2 * SIZE
	(p17) FMA	f119 = f14, f87, f119
	}
	{ .mfi
	(p17) FMA	f122 = f14, f88, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f86,  f87  = [AO7], 2 * SIZE
	(p17) FMA	f16  = f15, f89, f101
	}
	{ .mfi
	(p17) FMA	f17  = f15, f90, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f88,  f89  = [AO8], 2 * SIZE
	(p17) FMA	f18  = f15, f91, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE8], 16 * SIZE
	(p17) FMA	f19  = f15, f92, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f90,  f91  = [AO8], 2 * SIZE
	(p17) FMA	f20  = f15, f93, f113
	}
	{ .mfi
	(p14) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) FMA	f21  = f15, f94, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f92,  f93  = [AO8], 2 * SIZE
	(p17) FMA	f22  = f15, f95, f119
	}
	{ .mfb
	(p16) adds	I = -1, I
	(p17) FMA	f23  = f15, f96, f122
	br.ctop.sptk.few .L12
	}
	;;
	.align 16

.L15:
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	cmp.lt p6, p0 = 1, J
	adds J = -1, J
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LDFPD	f34,  f35  = [AO2], 2 * SIZE
	(p13) LDFPD	f36,  f37  = [AO3], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LDFPD	f50,  f51  = [AO2], 2 * SIZE
	(p13) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p14) LDFPD	f66,  f67  = [AO2], 2 * SIZE
	(p14) LDFPD	f68,  f69  = [AO3], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f81 = [AO2]
	(p15) LDFD	f82 = [AO3]
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f23, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f38,  f39  = [AO4], 2 * SIZE
	(p13) FMA	f100 = f8,  f32, f100
	nop	__LINE__
	}
	{ .mfi
	(p13) LDFPD	f40,  f41  = [AO5], 2 * SIZE
	(p13) FMA	f101 = f8,  f33, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f54,  f55  = [AO4], 2 * SIZE
	(p13) FMA	f102 = f8,  f48, f102
	nop	__LINE__
	}
	{ .mfi
	(p13) LDFPD	f56,  f57  = [AO5], 2 * SIZE
	(p13) FMA	f103 = f8,  f49, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFPD	f70,  f71  = [AO4], 2 * SIZE
	(p14) FMA	f104 = f8,  f64, f104
	nop	__LINE__
	}
	{ .mfi
	(p14) LDFPD	f72,  f73  = [AO5], 2 * SIZE
	(p14) FMA	f105 = f8,  f65, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	(p15) LDFD	f83 = [AO4]
	(p15) FMA	f106 = f8,  f80, f106
	nop	__LINE__
	}
	{ .mfi
	(p15) LDFD	f84 = [AO5]
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f42,  f43  = [AO6], 2 * SIZE
	(p13) FMA	f100 = f9,  f34, f100
	nop	__LINE__
	}
	{ .mfi
	(p13) LDFPD	f44,  f45  = [AO7], 2 * SIZE
	(p13) FMA	f101 = f9,  f35, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f58,  f59  = [AO6], 2 * SIZE
	(p13) FMA	f102 = f9,  f50, f102
	nop	__LINE__
	}
	{ .mfi
	(p13) LDFPD	f60,  f61  = [AO7], 2 * SIZE
	(p13) FMA	f103 = f9,  f51, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFPD	f74,  f75  = [AO6], 2 * SIZE
	(p14) FMA	f104 = f9,  f66, f104
	nop	__LINE__
	}
	{ .mfi
	(p14) LDFPD	f76,  f77  = [AO7], 2 * SIZE
	(p14) FMA	f105 = f9,  f67, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	(p15) LDFD	f85 = [AO6]
	(p15) FMA	f106 = f9,  f81, f106
	nop	__LINE__
	}
	{ .mfi
	(p15) LDFD	f86 = [AO7]
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f46,  f47  = [AO8], 2 * SIZE
	(p13) FMA	f100 = f10, f36, f100
	nop	__LINE__
	}
	{ .mfi
	(p13) FMA	f101 = f10, f37, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f62,  f63  = [AO8], 2 * SIZE
	(p13) FMA	f102 = f10, f52, f102
	nop	__LINE__
	}
	{ .mfi
	(p13) FMA	f103 = f10, f53, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFPD	f78,  f79  = [AO8], 2 * SIZE
	(p14) FMA	f104 = f10, f68, f104
	nop	__LINE__
	}
	{ .mfi
	(p14) FMA	f105 = f10, f69, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	(p15) LDFD	f87 = [AO8]
	(p15) FMA	f106 = f10, f82, f106
	nop	__LINE__
	}
	;;
	(p13) FMA	f100 = f11, f38, f100
	(p13) FMA	f101 = f11, f39, f101
	(p13) FMA	f102 = f11, f54, f102
	(p13) FMA	f103 = f11, f55, f103
	(p14) FMA	f104 = f11, f70, f104
	(p14) FMA	f105 = f11, f71, f105
	(p15) FMA	f106 = f11, f83, f106
	;;
	(p13) FMA	f100 = f12, f40, f100
	(p13) FMA	f101 = f12, f41, f101
	(p13) FMA	f102 = f12, f56, f102
	(p13) FMA	f103 = f12, f57, f103
	(p14) FMA	f104 = f12, f72, f104
	(p14) FMA	f105 = f12, f73, f105
	(p15) FMA	f106 = f12, f84, f106
	;;
	(p13) FMA	f100 = f13, f42, f100
	(p13) FMA	f101 = f13, f43, f101
	(p13) FMA	f102 = f13, f58, f102
	(p13) FMA	f103 = f13, f59, f103
	(p14) FMA	f104 = f13, f74, f104
	(p14) FMA	f105 = f13, f75, f105
	(p15) FMA	f106 = f13, f85, f106
	;;
	(p13) FMA	f100 = f14, f44, f100
	(p13) FMA	f101 = f14, f45, f101
	(p13) FMA	f102 = f14, f60, f102
	(p13) FMA	f103 = f14, f61, f103
	(p14) FMA	f104 = f14, f76, f104
	(p14) FMA	f105 = f14, f77, f105
	(p15) FMA	f106 = f14, f86, f106
	;;
	(p13) FMA	f100 = f15, f46, f100
	(p13) FMA	f101 = f15, f47, f101
	(p13) FMA	f102 = f15, f62, f102
	(p13) FMA	f103 = f15, f63, f103
	(p14) FMA	f104 = f15, f78, f104
	(p14) FMA	f105 = f15, f79, f105
	(p15) FMA	f106 = f15, f87, f106
	;;
	(p13) STFD	[YST1] = f100, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	(p6) br.cond.dptk .L11
	;;
	.align 16


.L20:
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = YY
	tbit.z	p6, p0 = N, 2
	}
	;;
	{ .mib
	mov	AO1 = A
	mov	pr.rot= 0
	(p6) br.cond.dpnt .L30
	}
	;;
	{ .mmi
	LDFD	f8 = [X], INCX
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	add	AO2 = LDA, A
	}
	;;
	{ .mmi
	LDFD	f9 = [X], INCX
	(p8) LDFD	f80 = [AO1], 1 * SIZE
	shladd	AO4 = LDA, 1, AO2
	}
	;;
	{ .mmi
	LDFD	f10 = [X], INCX
	(p8) LDFD	f81 = [AO2], 1 * SIZE
	shladd	AO3 = LDA, 1, A
	}
	;;
	{ .mmi
	LDFD	f11 = [X], INCX
	(p8) LDFD	f82 = [AO3], 1 * SIZE
	}
	;;
	{ .mfi
	(p8) LDFD	f83 = [AO4], 1 * SIZE
	FMPY	f8  = ALPHA, f8
	adds	PREB   = RPREFETCH * SIZE, YLD1
	}
	{ .mfi
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	FMPY	f9  = ALPHA, f9
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	}
	;;
	FMPY	f10 = ALPHA, f10
	shladd	A   = LDA, 2, A
	FMPY	f11 = ALPHA, f11
	;;
	{ .mfi
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	(p8) FMA	f106 = f8,  f80, f106
	mov	ar.ec= 2
	}
	;;
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	(p8) FMA	f106 = f9,  f81, f106
	shr	I = MM, 3
	;;
	{ .mmf
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	(p8) FMA	f106 = f10, f82, f106
	}
	;;
	{ .mfi
	adds	I = -1, I
	(p8) FMA	f106 = f11, f83, f106
	tbit.nz	p13, p0 = MM, 2
	}
	;;
	{ .mib
	(p8) STFD	[YST1] = f106, 1 * SIZE
	mov	ar.lc = I
	(p6) br.cond.dpnt .L25
	}
	;;
	.align 16

.L22:
	{ .mfi
	(p17) LDFPD	f63,  f64  = [AO4], 2 * SIZE
	(p17) FMA	f101 = f8,  f33, f101
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mfi
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p17) FMA	f104 = f8,  f34, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p17) FMA	f107 = f8,  f35, f107
	(p16) adds	I = -1, I
	}
	{ .mfi
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p17) FMA	f110 = f8,  f36, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f34,  f35  = [AO1], 2 * SIZE
	(p17) FMA	f113 = f8,  f37, f113
	}
	{ .mfi
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p17) FMA	f116 = f8,  f38, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p17) FMA	f119 = f8,  f39, f119
	}
	{ .mfi
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p17) FMA	f122 = f8,  f40, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f38,  f39  = [AO1], 2 * SIZE
	(p17) FMA	f101 = f9,  f41, f101
	}
	{ .mfi
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p17) FMA	f104 = f9,  f42, f104
	}
	;;
	{ .mmf
	(p16) LDFPD	f40,  f41  = [AO2], 2 * SIZE
	(p15) PREFETCH	[RPRE2], 16 * SIZE
	(p17) FMA	f107 = f9,  f43, f107
	}
	{ .mfi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	(p17) FMA	f110 = f9,  f44, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f42,  f43  = [AO2], 2 * SIZE
	(p17) FMA	f113 = f9,  f45, f113
	}
	{ .mfi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	(p17) FMA	f116 = f9,  f46, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f44,  f45  = [AO2], 2 * SIZE
	(p17) FMA	f119 = f9,  f47, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p17) FMA	f122 = f9,  f48, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f46,  f47  = [AO2], 2 * SIZE
	(p17) FMA	f101 = f10, f49, f101
	}
	{ .mfi
	(p14) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) FMA	f104 = f10, f50, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f48,  f49  = [AO3], 2 * SIZE
	(p17) FMA	f107 = f10, f51, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE3], 16 * SIZE
	(p17) FMA	f110 = f10, f52, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f50,  f51  = [AO3], 2 * SIZE
	(p17) FMA	f113 = f10, f53, f113
	}
	{ .mfi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p17) FMA	f116 = f10, f54, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	(p17) FMA	f119 = f10, f55, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	(p17) FMA	f122 = f10, f56, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f54,  f55  = [AO3], 2 * SIZE
	(p17) FMA	f16 = f11, f57, f101
	}
	{ .mfi
	(p15) PREFETCH	[RPRE4], 16 * SIZE
	(p17) FMA	f17 = f11, f58, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f56,  f57  = [AO4], 2 * SIZE
	(p17) FMA	f18 = f11, f59, f107
	}
	{ .mfi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	(p17) FMA	f19 = f11, f60, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f58,  f59  = [AO4], 2 * SIZE
	(p17) FMA	f20 = f11, f61, f113
	}
	{ .mfi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	(p17) FMA	f21 = f11, f62, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f60,  f61  = [AO4], 2 * SIZE
	(p17) FMA	f22 = f11, f63, f119
	}
	{ .mfb
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p17) FMA	f23 = f11, f64, f122
	br.ctop.sptk.few .L22
	}
	;;
	.align 16

.L25:
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	}
	;;
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f34,  f35  = [AO2], 2 * SIZE
	(p13) LDFPD	f36,  f37  = [AO3], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f50,  f51  = [AO2], 2 * SIZE
	(p13) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFPD	f66,  f67  = [AO2], 2 * SIZE
	(p14) LDFPD	f68,  f69  = [AO3], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	}
	;;
	{ .mmf
	(p15) LDFD	f81 = [AO2]
	(p15) LDFD	f82 = [AO3]
	(p13) FMA	f100 = f8,  f32, f100
	}
	{ .mfi
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p13) FMA	f101 = f8,  f33, f101
	}
	;;
	;;
	{ .mfi
	(p13) LDFPD	f38,  f39  = [AO4], 2 * SIZE
	(p13) FMA	f102 = f8,  f48, f102
	}
	{ .mfi
	(p13) FMA	f103 = f8,  f49, f103
	}
	;;
	{ .mfi
	(p13) LDFPD	f54,  f55  = [AO4], 2 * SIZE
	(p14) FMA	f104 = f8,  f64, f104
	}
	{ .mfi
	(p14) FMA	f105 = f8,  f65, f105
	}
	;;
	{ .mfi
	(p14) LDFPD	f70,  f71  = [AO4], 2 * SIZE
	(p15) FMA	f106 = f8,  f80, f106
	}
	{ .mfi
	(p13) FMA	f100 = f9,  f34, f100
	}
	;;
	{ .mfi
	(p15) LDFD	f83 = [AO4]
	(p13) FMA	f101 = f9,  f35, f101
	}
	{ .mfi
	(p13) FMA	f102 = f9,  f50, f102
	}
	;;
	(p13) FMA	f103 = f9,  f51, f103
	(p14) FMA	f104 = f9,  f66, f104
	(p14) FMA	f105 = f9,  f67, f105
	(p15) FMA	f106 = f9,  f81, f106
	;;
	(p13) FMA	f100 = f10, f36, f100
	(p13) FMA	f101 = f10, f37, f101
	(p13) FMA	f102 = f10, f52, f102
	(p13) FMA	f103 = f10, f53, f103
	(p14) FMA	f104 = f10, f68, f104
	(p14) FMA	f105 = f10, f69, f105
	(p15) FMA	f106 = f10, f82, f106
	;;
	(p13) FMA	f100 = f11, f38, f100
	(p13) FMA	f101 = f11, f39, f101
	;;
	(p13) FMA	f102 = f11, f54, f102
	(p13) STFD	[YST1] = f100, 1 * SIZE
	(p13) FMA	f103 = f11, f55, f103
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	(p14) FMA	f104 = f11, f70, f104
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	(p14) FMA	f105 = f11, f71, f105
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	(p15) FMA	f106 = f11, f83, f106
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	;;
	.align 16

.L30:
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = YY
	tbit.z	p6, p0 = N, 1
	}
	;;
	{ .mib
	mov	AO1 = A
	mov	pr.rot= 0
	(p6) br.cond.dpnt .L40
	}
	;;
	{ .mmi
	LDFD	f8 = [X], INCX
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	add	AO2 = LDA, A
	}
	;;
	{ .mmi
	LDFD	f9 = [X], INCX
	(p8) LDFD	f80 = [AO1], 1 * SIZE
	shladd	A   = LDA, 1, A
	}
	;;
	adds	PREB   = RPREFETCH * SIZE, YLD1
	FMPY	f8  = ALPHA, f8
	mov	ar.ec= 2
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	FMPY	f9  = ALPHA, f9
	shr	I = MM, 3
	;;
	(p8) LDFD	f81 = [AO2], 1 * SIZE
	cmp.eq	p6, p0 = 0, I
	;;
	(p8) FMA	f106 = f8,  f80, f106
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	tbit.nz	p13, p0 = MM, 2
	;;
	(p8) FMA	f106 = f9,  f81, f106
	cmp.eq	p16, p0 = r0, r0
	adds	I = -1, I
	;;
	{ .mib
	(p8) STFD	[YST1] = f106, 1 * SIZE
	mov	ar.lc = I
	(p6) br.cond.dpnt .L35
	}
	;;
	.align 16

.L32:
	{ .mfi
	(p17) LDFPD	f47,  f48  = [AO2], 2 * SIZE
	(p17) FMA	f101 = f8,  f33, f101
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mmf
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f16, 1 * SIZE
	(p17) FMA	f104 = f8,  f34, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p17) FMA	f107 = f8,  f35, f107
	adds	I = -1, I
	}
	{ .mmf
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p18) STFD	[YST1] = f17, 1 * SIZE
	(p17) FMA	f110 = f8,  f36, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f34,  f35  = [AO1], 2 * SIZE
	(p17) FMA	f113 = f8,  f37, f113
	}
	{ .mmf
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p17) FMA	f116 = f8,  f38, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p17) FMA	f119 = f8,  f39, f119
	}
	{ .mmf
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p17) FMA	f122 = f8,  f40, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f38,  f39  = [AO1], 2 * SIZE
	(p17) FMA	f16 = f9,  f41, f101
	}
	{ .mmf
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f20, 1 * SIZE
	(p17) FMA	f17 = f9,  f42, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f40,  f41  = [AO2], 2 * SIZE
	(p17) FMA	f18 = f9,  f43, f107
	}
	{ .mmf
	(p15) PREFETCH	[RPRE2], 16 * SIZE
	(p18) STFD	[YST1] = f21, 1 * SIZE
	(p17) FMA	f19 = f9,  f44, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f42,  f43  = [AO2], 2 * SIZE
	(p17) FMA	f20 = f9,  f45, f113
	}
	{ .mmf
	(p14) PREFETCH	[PREB],  16 * SIZE
	(p18) STFD	[YST1] = f22, 1 * SIZE
	(p17) FMA	f21 = f9,  f46, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f44,  f45  = [AO2], 2 * SIZE
	(p17) FMA	f22 = f9,  f47, f119
	}
	{ .mfb
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p17) FMA	f23 = f9,  f48, f122
	br.ctop.sptk.few .L32
	}
	;;
	.align 16

.L35:
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	}
	;;
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f34,  f35  = [AO2], 2 * SIZE
	(p18) STFD	[YST1] = f20, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f50,  f51  = [AO2], 2 * SIZE
	(p18) STFD	[YST1] = f21, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFPD	f66,  f67  = [AO2], 2 * SIZE
	(p18) STFD	[YST1] = f22, 1 * SIZE
	}
	;;
	{ .mmi
	(p15) LDFD	f81 = [AO2]
	(p18) STFD	[YST1] = f23, 1 * SIZE
	}
	;;
	(p13) FMA	f100 = f8,  f32, f100
	(p13) FMA	f101 = f8,  f33, f101
	(p13) FMA	f102 = f8,  f48, f102
	(p13) FMA	f103 = f8,  f49, f103
	(p14) FMA	f104 = f8,  f64, f104
	(p14) FMA	f105 = f8,  f65, f105
	(p15) FMA	f106 = f8,  f80, f106
	;;
	(p13) FMA	f100 = f9,  f34, f100
	(p13) FMA	f101 = f9,  f35, f101
	(p13) FMA	f102 = f9,  f50, f102
	(p13) FMA	f103 = f9,  f51, f103
	(p14) FMA	f104 = f9,  f66, f104
	(p14) FMA	f105 = f9,  f67, f105
	(p15) FMA	f106 = f9,  f81, f106
	;;
	(p13) STFD	[YST1] = f100, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	;;
	.align 16

.L40:
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = YY
	tbit.z	p6, p0 = N, 0
	}
	;;
	{ .mib
	mov	AO1 = A
	mov	pr.rot= 0
	(p6) br.cond.dpnt .L990
	}
	;;
	{ .mmi
	LDFD	f8 = [X], INCX
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	}
	;;
	{ .mii
	(p8) LDFD	f80 = [AO1], 1 * SIZE
	adds	PREB   = RPREFETCH * SIZE, YLD1
	}
	;;
	FMPY	f8  = ALPHA, f8
	shr	I = MM, 3
	;;
	(p8) FMA	f106 = f8,  f80, f106
	mov	ar.ec= 3
	;;
	{ .mmi
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	tbit.nz	p14, p15 = r0, 0
	}
	;;
	{ .mmi
	adds	YST2 = 4 * SIZE, YST1
	adds	I = -1, I
	tbit.nz	p13, p0 = MM, 2
	}
	;;
	{ .mmi
	(p8) STFD	[YST1] = f106, 1 * SIZE
	(p8) adds	YST2 = 1 * SIZE, YST2
	}
	{ .mib
	mov	ar.lc = I
	(p6) br.cond.dpnt .L145
	}
	;;
	.align 16

.L42:
	{ .mmf
	(p19) STFD	[YST1] = f16, 1 * SIZE
	(p19) STFD	[YST2] = f20, 1 * SIZE
	(p18) FMA	f16 = f8,  f34, f102
	}
	{ .mmf
	(p16) LDFPD	f32,  f35  = [AO1], 2 * SIZE
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p18) FMA	f20 = f8,  f46, f114
	}
	;;
	{ .mmf
	(p19) STFD	[YST1] = f17, 1 * SIZE
	(p19) STFD	[YST2] = f21, 1 * SIZE
	(p18) FMA	f17 = f8,  f37, f105
	}
	{ .mmf
	(p16) LDFPD	f38,  f41  = [AO1], 2 * SIZE
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p18) FMA	f21 = f8,  f49, f117
	}
	;;
	{ .mmf
	(p19) STFD	[YST1] = f18, 1 * SIZE
	(p19) STFD	[YST2] = f22, 1 * SIZE
	(p18) FMA	f18 = f8,  f40, f108
	}
	{ .mmf
	(p16) LDFPD	f44,  f47  = [AO1], 2 * SIZE
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p18) FMA	f22 = f8,  f52, f120
	}
	;;
	{ .mmf
	(p19) STFD	[YST1] = f19, 5 * SIZE
	(p19) STFD	[YST2] = f23, 5 * SIZE
	(p18) FMA	f19 = f8,  f43, f111
	}
	{ .mmf
	(p16) LDFPD	f50,  f53  = [AO1], 2 * SIZE
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p18) FMA	f23 = f8,  f55, f123
	}
	;;
	{ .mmi
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p14) PREFETCH	[PREB],  16 * SIZE
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mib
	nop	__LINE__
	(p16) adds	I = -1, I
	br.ctop.sptk.few .L42
	}
	;;
	.align 16

.L45:
	{ .mmi
	(p19) STFD	[YST1] = f16, 1 * SIZE
	(p19) STFD	[YST2] = f20, 1 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	}
	;;
	{ .mmi
	(p19) STFD	[YST1] = f17, 1 * SIZE
	(p19) STFD	[YST2] = f21, 1 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	}
	;;
	{ .mmi
	(p19) STFD	[YST1] = f18, 1 * SIZE
	(p19) STFD	[YST2] = f22, 1 * SIZE
	}
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	}
	;;
	{ .mmi
	(p19) STFD	[YST1] = f19, 5 * SIZE
	(p19) STFD	[YST2] = f23, 5 * SIZE
	}
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	}
	;;
	(p13) FMA	f100 = f8,  f32, f100
	(p13) FMA	f101 = f8,  f33, f101
	(p13) FMA	f102 = f8,  f48, f102
	(p13) FMA	f103 = f8,  f49, f103
	;;
	(p13) STFD	[YST1] = f100, 1 * SIZE
	(p14) FMA	f104 = f8,  f64, f104
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	(p14) FMA	f105 = f8,  f65, f105
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	(p15) FMA	f106 = f8,  f80, f106
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	br    .L990
	;;
	.align 16

.L100:
	shr	J   = N, 3
	;;
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L120
	;;
	.align 16

.L111:
	mov	YLD1 = YY
	mov	YST1 = YY
	;;
	LDFD	f8 = [X], INCX
	;;
	LDFD	f9 = [X], INCX
	;;
	LDFD	f10 = [X], INCX
	;;
	LDFD	f11 = [X], INCX
	;;
	LDFD	f12 = [X], INCX
	;;
	LDFD	f13 = [X], INCX
	;;
	LDFD	f14 = [X], INCX
	;;
	LDFD	f15 = [X], INCX
	;;
	FMPY	f8  = ALPHA, f8
	FMPY	f9  = ALPHA, f9
	FMPY	f10 = ALPHA, f10
	FMPY	f11 = ALPHA, f11
	FMPY	f12 = ALPHA, f12
	FMPY	f13 = ALPHA, f13
	FMPY	f14 = ALPHA, f14
	FMPY	f15 = ALPHA, f15
	;;
	mov	AO1 = A
	add	AO2 = LDA, A
	;;
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	;;
	shladd	AO5 = LDA, 1, AO3
	shladd	AO6 = LDA, 1, AO4
	;;
	shladd	AO7 = LDA, 1, AO5
	shladd	AO8 = LDA, 1, AO6
	shladd	A   = LDA, 3, A
	;;
	;;
	adds	PREB   = RPREFETCH * SIZE, YLD1
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	adds	RPRE5  = RPREFETCH * SIZE, AO5
	adds	RPRE6  = (RPREFETCH + 8) * SIZE, AO6
	adds	RPRE7  = RPREFETCH * SIZE, AO7
	adds	RPRE8  = (RPREFETCH + 8) * SIZE, AO8

	(p8) LDFD	f80 = [AO1], 1 * SIZE
	(p8) LDFD	f81 = [AO2], 1 * SIZE
	(p8) LDFD	f82 = [AO3], 1 * SIZE
	(p8) LDFD	f83 = [AO4], 1 * SIZE
	(p8) LDFD	f84 = [AO5], 1 * SIZE
	(p8) LDFD	f85 = [AO6], 1 * SIZE
	(p8) LDFD	f86 = [AO7], 1 * SIZE
	(p8) LDFD	f87 = [AO8], 1 * SIZE
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	;;
	(p8) FMPY	f32 = f8,  f80
	(p8) FMPY	f33 = f9,  f81
	(p8) FMPY	f34 = f10, f82
	(p8) FMA	f35 = f11, f83, f106
	;;
	(p8) FMA	f32 = f12, f84, f32
	(p8) FMA	f33 = f13, f85, f33
	(p8) FMA	f34 = f14, f86, f34
	(p8) FMA	f35 = f15, f87, f35
	;;
	(p8) FADD	f32 = f32, f33
	(p8) FADD	f34 = f34, f35
	;;
	(p8) FADD	f32 = f32, f34
	;;
	(p8) STFD	[YST1] = f32, 1 * SIZE

	shr	I = MM, 3
	mov	pr.rot= 0
	;;
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	;;
	adds	I = -1, I
	tbit.nz	p13, p0 = MM, 2
	;;
	mov	ar.lc = I
	mov	ar.ec= 2
	(p6) br.cond.dpnt .L115
	;;
	.align 16

.L112:
	{ .mfi
	(p17) LDFD	f96  = [AO8], 1 * SIZE
	(p17) FMA	f101 = f8,  f33, f101
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mfi
	(p17) FMA	f104 = f8,  f34, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p17) FMA	f107 = f8,  f35, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p17) FMA	f110 = f8,  f36, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f34,  f35  = [AO1], 2 * SIZE
	(p17) FMA	f113 = f8,  f37, f113
	}
	{ .mfi
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p17) FMA	f116 = f8,  f38, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p17) FMA	f119 = f8,  f39, f119
	}
	{ .mfi
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p17) FMA	f122 = f8,  f40, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f38,  f39  = [AO1], 2 * SIZE
	(p17) FMA	f101 = f9,  f41, f101
	}
	{ .mmf
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p16) LDFD	f40  = [AO2], 1 * SIZE
	(p17) FMA	f104 = f9,  f42, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f41,  f42  = [AO2], 2 * SIZE
	(p17) FMA	f107 = f9,  f43, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE2], 16 * SIZE
	(p17) FMA	f110 = f9,  f44, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f43,  f44  = [AO2], 2 * SIZE
	(p17) FMA	f113 = f9,  f45, f113
	}
	{ .mfi
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p17) FMA	f116 = f9,  f46, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f45,  f46  = [AO2], 2 * SIZE
	(p17) FMA	f119 = f9,  f47, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	(p17) FMA	f122 = f9,  f48, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f47  = [AO2], 1 * SIZE
	(p17) FMA	f101 = f10, f49, f101
	}
	{ .mfi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	(p17) FMA	f104 = f10, f50, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f48,  f49  = [AO3], 2 * SIZE
	(p17) FMA	f107 = f10, f51, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE3], 16 * SIZE
	(p17) FMA	f110 = f10, f52, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f50,  f51  = [AO3], 2 * SIZE
	(p17) FMA	f113 = f10, f53, f113
	}
	{ .mfi
	(p17) FMA	f116 = f10, f54, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	(p17) FMA	f119 = f10, f55, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p17) FMA	f122 = f10, f56, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f54,  f55  = [AO3], 2 * SIZE
	(p17) FMA	f101 = f11, f57, f101
	}
	{ .mmf
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p16) LDFD	f56  = [AO4], 1 * SIZE
	(p17) FMA	f104 = f11, f58, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f57,  f58  = [AO4], 2 * SIZE
	(p17) FMA	f107 = f11, f59, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE4], 16 * SIZE
	(p17) FMA	f110 = f11, f60, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f59,  f60  = [AO4], 2 * SIZE
	(p17) FMA	f113 = f11, f61, f113
	}
	{ .mfi
	(p17) FMA	f116 = f11, f62, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f61,  f62  = [AO4], 2 * SIZE
	(p17) FMA	f119 = f11, f63, f119
	}
	{ .mfi
	(p17) FMA	f122 = f11, f64, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f63  = [AO4], 1 * SIZE
	(p17) FMA	f101 = f12, f65, f101
	}
	{ .mfi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	(p17) FMA	f104 = f12, f66, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f64,  f65  = [AO5], 2 * SIZE
	(p17) FMA	f107 = f12, f67, f107
	}
	{ .mfi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	(p17) FMA	f110 = f12, f68, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f66,  f67  = [AO5], 2 * SIZE
	(p17) FMA	f113 = f12, f69, f113
	}
	{ .mfi
	(p14) PREFETCH	[RPRE5], 16 * SIZE
	(p17) FMA	f116 = f12, f70, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f68,  f69  = [AO5], 2 * SIZE
	(p17) FMA	f119 = f12, f71, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	(p17) FMA	f122 = f12, f72, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f70,  f71  = [AO5], 2 * SIZE
	(p17) FMA	f101 = f13, f73, f101
	}
	{ .mmf
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p16) LDFD	f72 = [AO6], 1 * SIZE
	(p17) FMA	f104 = f13, f74, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f73,  f74  = [AO6], 2 * SIZE
	(p17) FMA	f107 = f13, f75, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE6], 16 * SIZE
	(p17) FMA	f110 = f13, f76, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f75,  f76  = [AO6], 2 * SIZE
	(p17) FMA	f113 = f13, f77, f113
	}
	{ .mfi
	(p17) FMA	f116 = f13, f78, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f77,  f78  = [AO6], 2 * SIZE
	(p17) FMA	f119 = f13, f79, f119
	}
	{ .mfi
	(p17) FMA	f122 = f13, f80, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f79  = [AO6], 1 * SIZE
	(p17) FMA	f101 = f14, f81, f101
	}
	{ .mfi
	(p17) FMA	f104 = f14, f82, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f80,  f81  = [AO7], 2 * SIZE
	(p17) FMA	f107 = f14, f83, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE7], 16 * SIZE
	(p17) FMA	f110 = f14, f84, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f82,  f83  = [AO7], 2 * SIZE
	(p17) FMA	f113 = f14, f85, f113
	}
	{ .mfi
	(p17) FMA	f116 = f14, f86, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f84,  f85  = [AO7], 2 * SIZE
	(p17) FMA	f119 = f14, f87, f119
	}
	{ .mfi
	(p17) FMA	f122 = f14, f88, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f86,  f87  = [AO7], 2 * SIZE
	(p17) FMA	f16  = f15, f89, f101
	}
	{ .mfi
	(p16) LDFD	f88  = [AO8], 1 * SIZE
	(p17) FMA	f17  = f15, f90, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f89,  f90  = [AO8], 2 * SIZE
	(p17) FMA	f18  = f15, f91, f107
	}
	{ .mfi
	(p15) PREFETCH	[RPRE8], 16 * SIZE
	(p17) FMA	f19  = f15, f92, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f91,  f92  = [AO8], 2 * SIZE
	(p17) FMA	f20  = f15, f93, f113
	}
	{ .mfi
	(p14) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) FMA	f21  = f15, f94, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f93,  f94  = [AO8], 2 * SIZE
	(p17) FMA	f22  = f15, f95, f119
	}
	{ .mfb
	(p16) adds	I = -1, I
	(p17) FMA	f23  = f15, f96, f122
	br.ctop.sptk.few .L112
	}
	;;
	.align 16

.L115:
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	cmp.lt p6, p0 = 1, J
	adds J = -1, J
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p13) LDFD	f34  = [AO2], 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LDFPD	f35,  f50  = [AO2], 2 * SIZE
	(p13) LDFPD	f36,  f37  = [AO3], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LDFD	f51  = [AO2], 1 * SIZE
	(p13) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p14) LDFD	f66  = [AO2], 1 * SIZE
	(p14) LDFPD	f68,  f69  = [AO3], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p14) LDFD	f67  = [AO2], 1 * SIZE
	(p15) LDFD	f82 = [AO3]
	nop	__LINE__
	}
	{ .mmi
	(p18) STFD	[YST1] = f23, 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmf
	(p15) LDFD	f81 = [AO2]
	(p13) LDFD	f38  = [AO4], 1 * SIZE
	(p13) FMA	f100 = f8,  f32, f100
	}
	{ .mfi
	(p13) LDFPD	f40,  f41  = [AO5], 2 * SIZE
	(p13) FMA	f101 = f8,  f33, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFPD	f39,  f54  = [AO4], 2 * SIZE
	(p13) FMA	f102 = f8,  f48, f102
	nop	__LINE__
	}
	{ .mfi
	(p13) LDFPD	f56,  f57  = [AO5], 2 * SIZE
	(p13) FMA	f103 = f8,  f49, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFD	f55  = [AO4], 1 * SIZE
	(p14) FMA	f104 = f8,  f64, f104
	nop	__LINE__
	}
	{ .mfi
	(p14) LDFPD	f72,  f73  = [AO5], 2 * SIZE
	(p14) FMA	f105 = f8,  f65, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFD	f70  = [AO4], 1 * SIZE
	(p15) FMA	f106 = f8,  f80, f106
	nop	__LINE__
	}
	{ .mmi
	(p15) LDFD	f84 = [AO5]
	(p13) LDFD	f42  = [AO6], 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmf
	(p13) LDFPD	f43,  f58  = [AO6], 2 * SIZE
	(p14) LDFD	f71  = [AO4], 1 * SIZE
	(p13) FMA	f100 = f9,  f34, f100
	}
	{ .mfi
	(p13) LDFPD	f44,  f45  = [AO7], 2 * SIZE
	(p13) FMA	f101 = f9,  f35, f101
	nop	__LINE__
	}
	;;
	{ .mmf
	(p13) LDFD	f59  = [AO6], 1 * SIZE
	(p15) LDFD	f83 = [AO4]
	(p13) FMA	f102 = f9,  f50, f102
	}
	{ .mfi
	(p13) LDFPD	f60,  f61  = [AO7], 2 * SIZE
	(p13) FMA	f103 = f9,  f51, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFD	f74  = [AO6], 1 * SIZE
	(p14) FMA	f104 = f9,  f66, f104
	nop	__LINE__
	}
	{ .mfi
	(p14) LDFPD	f76,  f77  = [AO7], 2 * SIZE
	(p14) FMA	f105 = f9,  f67, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFD	f75  = [AO6], 1 * SIZE
	(p15) FMA	f106 = f9,  f81, f106
	nop	__LINE__
	}
	{ .mmi
	(p15) LDFD	f86 = [AO7]
	(p13) LDFD	f46  = [AO8], 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmf
	(p13) LDFPD	f47,  f62  = [AO8], 2 * SIZE
	(p15) LDFD	f85 = [AO6]
	(p13) FMA	f100 = f10, f36, f100
	}
	{ .mfi
	(p13) FMA	f101 = f10, f37, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) LDFD	f63  = [AO8], 1 * SIZE
	(p13) FMA	f102 = f10, f52, f102
	nop	__LINE__
	}
	{ .mfi
	(p13) FMA	f103 = f10, f53, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFD	f78  = [AO8], 1 * SIZE
	(p14) FMA	f104 = f10, f68, f104
	nop	__LINE__
	}
	{ .mfi
	(p14) FMA	f105 = f10, f69, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	(p14) LDFD	f79  = [AO8], 1 * SIZE
	(p15) FMA	f106 = f10, f82, f106
	nop	__LINE__
	}
	;;
	(p15) LDFD	f87 = [AO8]
	(p13) FMA	f100 = f11, f38, f100
	(p13) FMA	f101 = f11, f39, f101
	(p13) FMA	f102 = f11, f54, f102
	(p13) FMA	f103 = f11, f55, f103
	(p14) FMA	f104 = f11, f70, f104
	(p14) FMA	f105 = f11, f71, f105
	(p15) FMA	f106 = f11, f83, f106
	;;
	(p13) FMA	f100 = f12, f40, f100
	(p13) FMA	f101 = f12, f41, f101
	(p13) FMA	f102 = f12, f56, f102
	(p13) FMA	f103 = f12, f57, f103
	(p14) FMA	f104 = f12, f72, f104
	(p14) FMA	f105 = f12, f73, f105
	(p15) FMA	f106 = f12, f84, f106
	;;
	(p13) FMA	f100 = f13, f42, f100
	(p13) FMA	f101 = f13, f43, f101
	(p13) FMA	f102 = f13, f58, f102
	(p13) FMA	f103 = f13, f59, f103
	(p14) FMA	f104 = f13, f74, f104
	(p14) FMA	f105 = f13, f75, f105
	(p15) FMA	f106 = f13, f85, f106
	;;
	(p13) FMA	f100 = f14, f44, f100
	(p13) FMA	f101 = f14, f45, f101
	(p13) FMA	f102 = f14, f60, f102
	(p13) FMA	f103 = f14, f61, f103
	(p14) FMA	f104 = f14, f76, f104
	(p14) FMA	f105 = f14, f77, f105
	(p15) FMA	f106 = f14, f86, f106
	;;
	(p13) FMA	f100 = f15, f46, f100
	(p13) FMA	f101 = f15, f47, f101
	(p13) FMA	f102 = f15, f62, f102
	(p13) FMA	f103 = f15, f63, f103
	(p14) FMA	f104 = f15, f78, f104
	(p14) FMA	f105 = f15, f79, f105
	(p15) FMA	f106 = f15, f87, f106
	;;
	(p13) STFD	[YST1] = f100, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	(p6) br.cond.dptk .L111
	;;
	.align 16

.L120:
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = YY
	tbit.z	p6, p0 = N, 2
	}
	;;
	{ .mib
	mov	AO1 = A
	mov	pr.rot= 0
	(p6) br.cond.dpnt .L130
	}
	;;
	{ .mmi
	LDFD	f8 = [X], INCX
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	add	AO2 = LDA, A
	}
	;;
	{ .mmi
	LDFD	f9 = [X], INCX
	(p8) LDFD	f80 = [AO1], 1 * SIZE
	shladd	AO4 = LDA, 1, AO2
	}
	;;
	{ .mmi
	LDFD	f10 = [X], INCX
	(p8) LDFD	f81 = [AO2], 1 * SIZE
	shladd	AO3 = LDA, 1, A
	}
	;;
	{ .mmi
	LDFD	f11 = [X], INCX
	(p8) LDFD	f82 = [AO3], 1 * SIZE
	}
	;;
	{ .mfi
	(p8) LDFD	f83 = [AO4], 1 * SIZE
	FMPY	f8  = ALPHA, f8
	adds	PREB   = RPREFETCH * SIZE, YLD1
	}
	{ .mfi
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	FMPY	f9  = ALPHA, f9
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	}
	;;
	FMPY	f10 = ALPHA, f10
	shladd	A   = LDA, 2, A
	FMPY	f11 = ALPHA, f11
	;;
	{ .mfi
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	(p8) FMA	f106 = f8,  f80, f106
	mov	ar.ec= 2
	}
	;;
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	(p8) FMA	f106 = f9,  f81, f106
	shr	I = MM, 3
	;;
	{ .mmf
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	(p8) FMA	f106 = f10, f82, f106
	}
	;;
	{ .mfi
	adds	I = -1, I
	(p8) FMA	f106 = f11, f83, f106
	tbit.nz	p13, p0 = MM, 2
	}
	;;
	{ .mib
	(p8) STFD	[YST1] = f106, 1 * SIZE
	mov	ar.lc = I
	(p6) br.cond.dpnt .L125
	}
	;;
	.align 16

.L122:
	{ .mfi
	(p17) LDFD	f64  = [AO4], 1 * SIZE
	(p17) FMA	f101 = f8,  f33, f101
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mfi
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p17) FMA	f104 = f8,  f34, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p17) FMA	f107 = f8,  f35, f107
	(p16) adds	I = -1, I
	}
	{ .mfi
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p17) FMA	f110 = f8,  f36, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f34,  f35  = [AO1], 2 * SIZE
	(p17) FMA	f113 = f8,  f37, f113
	}
	{ .mfi
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p17) FMA	f116 = f8,  f38, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p17) FMA	f119 = f8,  f39, f119
	}
	{ .mfi
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p17) FMA	f122 = f8,  f40, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f38,  f39  = [AO1], 2 * SIZE
	(p17) FMA	f101 = f9,  f41, f101
	}
	{ .mmf
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p16) LDFD	f40  = [AO2], 1 * SIZE
	(p17) FMA	f104 = f9,  f42, f104
	}
	;;
	{ .mmf
	(p16) LDFPD	f41,  f42  = [AO2], 2 * SIZE
	(p15) PREFETCH	[RPRE2], 16 * SIZE
	(p17) FMA	f107 = f9,  f43, f107
	}
	{ .mfi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	(p17) FMA	f110 = f9,  f44, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f43,  f44  = [AO2], 2 * SIZE
	(p17) FMA	f113 = f9,  f45, f113
	}
	{ .mfi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	(p17) FMA	f116 = f9,  f46, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f45,  f46  = [AO2], 2 * SIZE
	(p17) FMA	f119 = f9,  f47, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p17) FMA	f122 = f9,  f48, f122
	}
	;;
	{ .mfi
	(p16) LDFD	f47  = [AO2], 1 * SIZE
	(p17) FMA	f101 = f10, f49, f101
	}
	{ .mfi
	(p14) lfetch.excl.nt2	[PREB],  16 * SIZE
	(p17) FMA	f104 = f10, f50, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f48,  f49  = [AO3], 2 * SIZE
	(p17) FMA	f107 = f10, f51, f107
	}
	{ .mfi
	(p14) PREFETCH	[RPRE3], 16 * SIZE
	(p17) FMA	f110 = f10, f52, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f50,  f51  = [AO3], 2 * SIZE
	(p17) FMA	f113 = f10, f53, f113
	}
	{ .mfi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p17) FMA	f116 = f10, f54, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	(p17) FMA	f119 = f10, f55, f119
	}
	{ .mfi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	(p17) FMA	f122 = f10, f56, f122
	}
	;;
	{ .mfi
	(p16) LDFPD	f54,  f55  = [AO3], 2 * SIZE
	(p17) FMA	f16 = f11, f57, f101
	}
	{ .mmf
	(p15) PREFETCH	[RPRE4], 16 * SIZE
	(p16) LDFD	f56  = [AO4], 1 * SIZE
	(p17) FMA	f17 = f11, f58, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f57,  f58  = [AO4], 2 * SIZE
	(p17) FMA	f18 = f11, f59, f107
	}
	{ .mfi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	(p17) FMA	f19 = f11, f60, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f59,  f60  = [AO4], 2 * SIZE
	(p17) FMA	f20 = f11, f61, f113
	}
	{ .mfi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	(p17) FMA	f21 = f11, f62, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f61,  f62  = [AO4], 2 * SIZE
	(p17) FMA	f22 = f11, f63, f119
	}
	{ .mfb
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p17) FMA	f23 = f11, f64, f122
	br.ctop.sptk.few .L122
	}
	;;
	.align 16

.L125:
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p15) LDFD	f80 = [AO1]
	}
	{ .mmi
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	(p13) LDFD	f34  = [AO2], 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f35,  f50  = [AO2], 2 * SIZE
	(p13) LDFPD	f36,  f37  = [AO3], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f20, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f51  = [AO2], 1 * SIZE
	(p13) LDFPD	f52,  f53  = [AO3], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f21, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFD	f66  = [AO2], 1 * SIZE
	(p14) LDFPD	f68,  f69  = [AO3], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f22, 1 * SIZE
	}
	;;
	{ .mmf
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p14) LDFD	f67  = [AO2], 1 * SIZE
	(p13) FMA	f100 = f8,  f32, f100
	}
	{ .mmf
	(p15) LDFD	f82 = [AO3]
	(p13) LDFD	f38  = [AO4], 1 * SIZE
	(p13) FMA	f101 = f8,  f33, f101
	}
	;;
	;;
	{ .mmf
	(p13) LDFPD	f39,  f54  = [AO4], 2 * SIZE
	(p15) LDFD	f81 = [AO2]
	(p13) FMA	f102 = f8,  f48, f102
	}
	{ .mfi
	(p13) FMA	f103 = f8,  f49, f103
	}
	;;
	{ .mfi
	(p13) LDFD	f55  = [AO4], 1 * SIZE
	(p14) FMA	f104 = f8,  f64, f104
	}
	{ .mfi
	(p14) FMA	f105 = f8,  f65, f105
	}
	;;
	{ .mfi
	(p14) LDFD	f70  = [AO4], 1 * SIZE
	(p15) FMA	f106 = f8,  f80, f106
	}
	{ .mfi
	(p13) FMA	f100 = f9,  f34, f100
	}
	;;
	{ .mfi
	(p14) LDFD	f71  = [AO4], 1 * SIZE
	(p13) FMA	f101 = f9,  f35, f101
	}
	{ .mfi
	(p13) FMA	f102 = f9,  f50, f102
	}
	;;
	(p15) LDFD	f83 = [AO4]
	(p13) FMA	f103 = f9,  f51, f103
	(p14) FMA	f104 = f9,  f66, f104
	(p14) FMA	f105 = f9,  f67, f105
	(p15) FMA	f106 = f9,  f81, f106
	;;
	(p13) FMA	f100 = f10, f36, f100
	(p13) FMA	f101 = f10, f37, f101
	(p13) FMA	f102 = f10, f52, f102
	(p13) FMA	f103 = f10, f53, f103
	(p14) FMA	f104 = f10, f68, f104
	(p14) FMA	f105 = f10, f69, f105
	(p15) FMA	f106 = f10, f82, f106
	;;
	(p13) FMA	f100 = f11, f38, f100
	(p13) FMA	f101 = f11, f39, f101
	;;
	(p13) FMA	f102 = f11, f54, f102
	(p13) STFD	[YST1] = f100, 1 * SIZE
	(p13) FMA	f103 = f11, f55, f103
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	(p14) FMA	f104 = f11, f70, f104
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	(p14) FMA	f105 = f11, f71, f105
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	(p15) FMA	f106 = f11, f83, f106
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	;;
	.align 16

.L130:
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = YY
	tbit.z	p6, p0 = N, 1
	}
	;;
	{ .mib
	mov	AO1 = A
	mov	pr.rot= 0
	(p6) br.cond.dpnt .L140
	}
	;;
	{ .mmi
	LDFD	f8 = [X], INCX
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	add	AO2 = LDA, A
	}
	;;
	{ .mmi
	LDFD	f9 = [X], INCX
	(p8) LDFD	f80 = [AO1], 1 * SIZE
	shladd	A   = LDA, 1, A
	}
	;;
	adds	PREB   = RPREFETCH * SIZE, YLD1
	FMPY	f8  = ALPHA, f8
	mov	ar.ec= 2
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	FMPY	f9  = ALPHA, f9
	shr	I = MM, 3
	;;
	(p8) LDFD	f81 = [AO2], 1 * SIZE
	cmp.eq	p6, p0 = 0, I
	;;
	(p8) FMA	f106 = f8,  f80, f106
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	tbit.nz	p13, p0 = MM, 2
	;;
	(p8) FMA	f106 = f9,  f81, f106
	cmp.eq	p16, p0 = r0, r0
	adds	I = -1, I
	;;
	{ .mib
	(p8) STFD	[YST1] = f106, 1 * SIZE
	mov	ar.lc = I
	(p6) br.cond.dpnt .L135
	}
	;;
	.align 16

.L132:
	{ .mfi
	(p17) LDFD	f48  = [AO2], 1 * SIZE
	(p17) FMA	f101 = f8,  f33, f101
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mmf
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f16, 1 * SIZE
	(p17) FMA	f104 = f8,  f34, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p17) FMA	f107 = f8,  f35, f107
	adds	I = -1, I
	}
	{ .mmf
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p18) STFD	[YST1] = f17, 1 * SIZE
	(p17) FMA	f110 = f8,  f36, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f34,  f35  = [AO1], 2 * SIZE
	(p17) FMA	f113 = f8,  f37, f113
	}
	{ .mmf
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f18, 1 * SIZE
	(p17) FMA	f116 = f8,  f38, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p17) FMA	f119 = f8,  f39, f119
	}
	{ .mmf
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p18) STFD	[YST1] = f19, 1 * SIZE
	(p17) FMA	f122 = f8,  f40, f122
	}
	;;
	{ .mmf
	(p16) LDFPD	f38,  f39  = [AO1], 2 * SIZE
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p17) FMA	f16 = f9,  f41, f101
	}
	{ .mmf
	(p18) STFD	[YST1] = f20, 1 * SIZE
	(p16) LDFD	f40  = [AO2], 1 * SIZE
	(p17) FMA	f17 = f9,  f42, f104
	}
	;;
	{ .mfi
	(p16) LDFPD	f41,  f42  = [AO2], 2 * SIZE
	(p17) FMA	f18 = f9,  f43, f107
	}
	{ .mmf
	(p15) PREFETCH	[RPRE2], 16 * SIZE
	(p18) STFD	[YST1] = f21, 1 * SIZE
	(p17) FMA	f19 = f9,  f44, f110
	}
	;;
	{ .mfi
	(p16) LDFPD	f43,  f44  = [AO2], 2 * SIZE
	(p17) FMA	f20 = f9,  f45, f113
	}
	{ .mmf
	(p14) PREFETCH	[PREB],  16 * SIZE
	(p18) STFD	[YST1] = f22, 1 * SIZE
	(p17) FMA	f21 = f9,  f46, f116
	}
	;;
	{ .mfi
	(p16) LDFPD	f45,  f46  = [AO2], 2 * SIZE
	(p17) FMA	f22 = f9,  f47, f119
	}
	{ .mfb
	(p18) STFD	[YST1] = f23, 1 * SIZE
	(p17) FMA	f23 = f9,  f48, f122
	br.ctop.sptk.few .L132
	}
	;;
	.align 16

.L135:
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p18) STFD	[YST1] = f16, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p18) STFD	[YST1] = f17, 1 * SIZE
	}
	;;
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f18, 1 * SIZE
	}
	;;
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	}
	{ .mmi
	(p18) STFD	[YST1] = f19, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f34 = [AO2], 1 * SIZE
	(p18) STFD	[YST1] = f20, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f35  = [AO2], 1 * SIZE
	(p18) STFD	[YST1] = f21, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f50  = [AO2], 1 * SIZE
	(p18) STFD	[YST1] = f22, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD	f51  = [AO2], 1 * SIZE
	(p18) STFD	[YST1] = f23, 1 * SIZE
	}
	;;
	(p14) LDFD	f66  = [AO2], 1 * SIZE
	(p13) FMA	f100 = f8,  f32, f100
	;;
	(p14) LDFD	f67  = [AO2], 1 * SIZE
	(p13) FMA	f101 = f8,  f33, f101
	;;
	(p15) LDFD	f81 = [AO2]
	(p13) FMA	f102 = f8,  f48, f102
	(p13) FMA	f103 = f8,  f49, f103
	(p14) FMA	f104 = f8,  f64, f104
	(p14) FMA	f105 = f8,  f65, f105
	(p15) FMA	f106 = f8,  f80, f106
	;;
	(p13) FMA	f100 = f9,  f34, f100
	(p13) FMA	f101 = f9,  f35, f101
	(p13) FMA	f102 = f9,  f50, f102
	(p13) FMA	f103 = f9,  f51, f103
	(p14) FMA	f104 = f9,  f66, f104
	(p14) FMA	f105 = f9,  f67, f105
	(p15) FMA	f106 = f9,  f81, f106
	;;
	(p13) STFD	[YST1] = f100, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	;;
	.align 16

.L140:
	{ .mmi
	mov	YLD1 = YY
	mov	YST1 = YY
	tbit.z	p6, p0 = N, 0
	}
	;;
	{ .mib
	mov	AO1 = A
	mov	pr.rot= 0
	(p6) br.cond.dpnt .L990
	}
	;;
	{ .mmi
	LDFD	f8 = [X], INCX
	(p8) LDFD	f106 = [YLD1], 1 * SIZE
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	}
	;;
	{ .mmi
	(p8) LDFD	f80 = [AO1], 1 * SIZE
	adds	PREB   = RPREFETCH * SIZE, YLD1
	}
	;;
	FMPY	f8  = ALPHA, f8
	shr	I = MM, 3
	;;
	(p8) FMA	f106 = f8,  f80, f106
	mov	ar.ec= 3
	;;
	{ .mmi
	cmp.eq	p6, p0 = 0, I
	cmp.eq	p16, p0 = r0, r0
	tbit.nz	p14, p15 = r0, 0
	}
	;;
	{ .mmi
	adds	YST2 = 4 * SIZE, YST1
	adds	I = -1, I
	tbit.nz	p13, p0 = MM, 2
	}
	;;
	{ .mmi
	(p8) STFD	[YST1] = f106, 1 * SIZE
	(p8) adds	YST2 = 1 * SIZE, YST2
	}
	{ .mib
	mov	ar.lc = I
	(p6) br.cond.dpnt .L145
	}
	;;
	.align 16

.L142:
	{ .mmf
	(p19) STFD	[YST1] = f16, 1 * SIZE
	(p19) STFD	[YST2] = f20, 1 * SIZE
	(p18) FMA	f16 = f8,  f34, f102
	}
	{ .mmf
	(p16) LDFPD	f32,  f35  = [AO1], 2 * SIZE
	(p16) LDFPD	f100, f103 = [YLD1], 2 * SIZE
	(p18) FMA	f20 = f8,  f46, f114
	}
	;;
	{ .mmf
	(p19) STFD	[YST1] = f17, 1 * SIZE
	(p19) STFD	[YST2] = f21, 1 * SIZE
	(p18) FMA	f17 = f8,  f37, f105
	}
	{ .mmf
	(p16) LDFPD	f38,  f41  = [AO1], 2 * SIZE
	(p16) LDFPD	f106, f109 = [YLD1], 2 * SIZE
	(p18) FMA	f21 = f8,  f49, f117
	}
	;;
	{ .mmf
	(p19) STFD	[YST1] = f18, 1 * SIZE
	(p19) STFD	[YST2] = f22, 1 * SIZE
	(p18) FMA	f18 = f8,  f40, f108
	}
	{ .mmf
	(p16) LDFPD	f44,  f47  = [AO1], 2 * SIZE
	(p16) LDFPD	f112, f115 = [YLD1], 2 * SIZE
	(p18) FMA	f22 = f8,  f52, f120
	}
	;;
	{ .mmf
	(p19) STFD	[YST1] = f19, 5 * SIZE
	(p19) STFD	[YST2] = f23, 5 * SIZE
	(p18) FMA	f19 = f8,  f43, f111
	}
	{ .mmf
	(p16) LDFPD	f50,  f53  = [AO1], 2 * SIZE
	(p16) LDFPD	f118, f121 = [YLD1], 2 * SIZE
	(p18) FMA	f23 = f8,  f55, f123
	}
	;;
	{ .mmi
	(p14) PREFETCH	[RPRE1], 16 * SIZE
	(p14) PREFETCH	[PREB],  16 * SIZE
	(p16) tbit.nz.unc	p14, p15 = I, 0
	}
	{ .mib
	nop	__LINE__
	(p16) adds	I = -1, I
	br.ctop.sptk.few .L142
	}
	;;
	.align 16

.L145:
	{ .mmi
	(p19) STFD	[YST1] = f16, 1 * SIZE
	(p19) STFD	[YST2] = f20, 1 * SIZE
	tbit.nz	p14, p0 = MM, 1
	}
	{ .mmi
	(p13) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p13) LDFPD	f100, f101 = [YLD1], 2 * SIZE
	}
	;;
	{ .mmi
	(p19) STFD	[YST1] = f17, 1 * SIZE
	(p19) STFD	[YST2] = f21, 1 * SIZE
	tbit.nz	p15, p0 = MM, 0
	}
	{ .mmi
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f102, f103 = [YLD1], 2 * SIZE
	}
	;;
	{ .mmi
	(p19) STFD	[YST1] = f18, 1 * SIZE
	(p19) STFD	[YST2] = f22, 1 * SIZE
	}
	{ .mmi
	(p14) LDFPD	f64,  f65  = [AO1], 2 * SIZE
	(p14) LDFPD	f104, f105 = [YLD1], 2 * SIZE
	}
	;;
	{ .mmi
	(p19) STFD	[YST1] = f19, 5 * SIZE
	(p19) STFD	[YST2] = f23, 5 * SIZE
	}
	{ .mmi
	(p15) LDFD	f80 = [AO1]
	(p15) LDFD	f106 = [YLD1], 1 * SIZE
	}
	;;
	(p13) FMA	f100 = f8,  f32, f100
	(p13) FMA	f101 = f8,  f33, f101
	(p13) FMA	f102 = f8,  f48, f102
	(p13) FMA	f103 = f8,  f49, f103
	(p14) FMA	f104 = f8,  f64, f104
	(p14) FMA	f105 = f8,  f65, f105
	(p15) FMA	f106 = f8,  f80, f106
	;;
	(p13) STFD	[YST1] = f100, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f101, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f102, 1 * SIZE
	;;
	(p13) STFD	[YST1] = f103, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f104, 1 * SIZE
	;;
	(p14) STFD	[YST1] = f105, 1 * SIZE
	;;
	(p15) STFD	[YST1] = f106, 1 * SIZE
	;;
	.align 16

.L990:
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
