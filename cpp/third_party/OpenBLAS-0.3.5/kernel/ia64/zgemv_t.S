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

#define I	r15
#define J	r16
#define AO1	r18
#define AO2	r19
#define AO3	r20
#define AO4	r21
#define AO5	r22
#define AO6	r23
#define AO7	r24
#define AO8	r25
#define BO	r26
#define INCYM1	r28

#define RPRE1	loc0
#define RPRE2	loc1
#define RPRE3	loc2
#define RPRE4	loc3
#define RPRE5	loc4
#define RPRE6	loc5
#define RPRE7	loc6
#define RPRE8	loc7

#define AO21	loc8
#define AO41	loc9
#define AO61	loc10
#define AO81	loc11
#define CLD1	loc12
#define CLD2	loc13
#define CST1	loc14
#define CST2	loc15

#define PREB	r8
#define WPRE	r9
#define OFFSET	PREB
#define INCX3M1	WPRE
#define INCY3M1	r10

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
#define ADD1	     FMA
#define ADD2	     FMA
#define ADD3	     FNMA
#define ADD4	     FMA
#elif  defined(CONJ) && !defined(XCONJ)
#define ADD1	     FMA
#define ADD2	     FMA
#define ADD3	     FMA
#define ADD4	     FNMA
#elif !defined(CONJ) &&  defined(XCONJ)
#define ADD1	     FMA
#define ADD2	     FNMA
#define ADD3	     FMA
#define ADD4	     FMA
#else
#define ADD1	     FMA
#define ADD2	     FNMA
#define ADD3	     FNMA
#define ADD4	     FNMA
#endif

	PROLOGUE
	PROFCODE
	.prologue

	{ .mmi
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 16, 0, 0
	adds	r14 = 16, SP
	mov	ARLC  = ar.lc
	}
	{ .mmi
	adds	r8 = -8 * 16, SP
	adds	r9 = -7 * 16, SP
	adds	SP = -8 * 16, SP
	}
	;;
	{ .mmi
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	mov	PR = pr
	}
	;;
	{ .mmi
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	adds	r15 = 152, SP
	}
	;;
	{ .mmi
	stf.spill  [r8] = f20, 32
	stf.spill  [r9] = f21, 32
	adds	r16 = 160, SP
	}
	;;
	{ .mmi
	stf.spill  [r8] = f22
	stf.spill  [r9] = f23
	adds	r17 = 168, SP
	}
	.body
	;;
	{ .mmf
	ld8	INCX   = [r14]
	ld8	Y      = [r15]
	mov	ALPHA_R = f8
	}
	{ .mmf
	ld8	INCY   = [r16]
	ld8	BUFFER = [r17]
	mov	ALPHA_I = f9
	}
	;;
	{ .mmi
	shladd	INCX = INCX, ZBASE_SHIFT, r0
	shladd	LDA  = LDA,  ZBASE_SHIFT, r0
	mov	pr.rot= 0
	}
	{ .mmi
	cmp.ge	p7, p0 = 0, M
	cmp.ge	p6, p0 = 0, N
	shladd	INCY = INCY, ZBASE_SHIFT, r0
	}
	;;
	{ .mmi
	mov	AO1 = BUFFER
	adds	OFFSET = -SIZE, INCX
	shr	I = M, 3
	}
	{ .mib
	adds	INCYM1 = - SIZE, INCY
	shladd	INCX3M1  = INCX, 1, INCX
	(p7) br.cond.dpnt .L999
	}
	;;
	{ .mmi
	shladd	BO  = INCX, 1, X
	adds	AO2 = 4 * SIZE, BUFFER
	mov	ar.ec= 5
	}
	{ .mmb
	shladd	INCY3M1  = INCY, 1, INCYM1
	adds I = -1, I
	(p6) br.cond.dpnt .L999
	}
	;;
	{ .mmi
	adds	INCX3M1 = -SIZE, INCX3M1
	cmp.eq	p16, p0 = r0, r0
	tbit.nz	p13, p0 = M, 2
	}
	{ .mib
	cmp.gt	p6, p0 = 0, I
	mov	ar.lc = I
	(p6) br.cond.dpnt .L05
	}
	;;
	.align 16

.L01:
	(p20) STFD	[AO1] = f36, SIZE
	(p20) STFD	[AO2] = f56, SIZE
	(p16) LDFD	f32 = [X], SIZE
	(p16) LDFD	f52 = [BO], SIZE
	;;
	(p20) STFD	[AO1] = f41, SIZE
	(p20) STFD	[AO2] = f61, SIZE
	(p16) LDFD	f37 = [X], OFFSET
	(p16) LDFD	f57 = [BO], OFFSET
	;;
	(p20) STFD	[AO1] = f46, SIZE
	(p20) STFD	[AO2] = f66, SIZE
	(p16) LDFD	f42 = [X], SIZE
	(p16) LDFD	f62 = [BO], SIZE
	;;
	(p20) STFD	[AO1] = f51, 5 * SIZE
	(p20) STFD	[AO2] = f71, 5 * SIZE
	(p16) LDFD	f47 = [X], INCX3M1
	(p16) LDFD	f67 = [BO], INCX3M1
	;;
	(p20) STFD	[AO1] = f76, SIZE
	(p20) STFD	[AO2] = f96, SIZE
	(p16) LDFD	f72 = [X], SIZE
	(p16) LDFD	f92 = [BO], SIZE
	;;
	(p20) STFD	[AO1] = f81, SIZE
	(p20) STFD	[AO2] = f101, SIZE
	(p16) LDFD	f77 = [X], OFFSET
	(p16) LDFD	f97 = [BO], OFFSET
	;;
	(p20) STFD	[AO1] = f86, SIZE
	(p20) STFD	[AO2] = f106, SIZE
	(p16) LDFD	f82 = [X], SIZE
	(p16) LDFD	f102 = [BO], SIZE
	;;
	(p20) STFD	[AO1] = f91, 5 * SIZE
	(p20) STFD	[AO2] = f111, 5 * SIZE
	(p16) LDFD	f87 = [X], INCX3M1
	(p16) LDFD	f107 = [BO], INCX3M1
	br.ctop.sptk.few .L01
	;;
	.align 16

.L05:
	{ .mmi
	(p13) LDFD f32 = [X],  SIZE
	(p13) LDFD f36 = [BO],  SIZE
	tbit.nz	p14, p0 = M, 1
	}
	;;
	{ .mmi
	(p13) LDFD f33 = [X],  OFFSET
	(p13) LDFD f37 = [BO],  OFFSET
	tbit.nz	p15, p0 = M, 0
	}
	;;
	{ .mmb
	(p13) LDFD f34 = [X],  SIZE
	(p13) LDFD f38 = [BO],  SIZE
	}
	;;
	{ .mmi
	(p13) LDFD f35 = [X],  INCX3M1
	(p13) LDFD f39 = [BO],  INCX3M1
	}
	;;
	{ .mmi
	(p14) LDFD f40 = [X],  SIZE
	}
	;;
	(p14) LDFD f41 = [X],  OFFSET
	(p13) STFD [AO1] = f32, SIZE
	tbit.nz	p8, p0 = A,   BASE_SHIFT
	;;
	(p14) LDFD f42 = [X],  SIZE
	(p13) STFD [AO2] = f36, SIZE
	;;
	(p14) LDFD f43 = [X],  OFFSET
	(p13) STFD [AO1] = f33, SIZE
	;;
	(p15) LDFD f44 = [X],  SIZE
	(p13) STFD [AO2] = f37, SIZE
	;;
	(p15) LDFD f45 = [X],  OFFSET
	(p13) STFD [AO1] = f34, SIZE
	(p13) STFD [AO2] = f38, SIZE
	;;
	(p13) STFD [AO1] = f35, 5 * SIZE
	(p13) STFD [AO2] = f39, 5 * SIZE
	;;
	(p14) STFD [AO1] = f40, SIZE
	;;
	(p14) STFD [AO1] = f41, SIZE
	;;
	(p14) STFD [AO1] = f42, SIZE
	;;
	(p14) STFD [AO1] = f43, SIZE
	;;
	(p15) STFD [AO1] = f44, SIZE
	;;
	(p15) STFD [AO1] = f45, SIZE
	(p8) br.cond.dpnt .L100
	;;
	.align 16

.L10:
	{ .mmi
	mov	CLD1  = Y
	shladd	CLD2  = INCY, 1, Y
	shr	J   = N, 3
	}
	;;
	{ .mmb
	mov	CST1  = Y
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L20
	}
	;;
	.align 16

.L11:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	mov	BO  = BUFFER
	}
	;;
	{ .mmf
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	mov	f12 = f0
	}
	{ .mmf
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	adds	RPRE2  = (RPREFETCH +  2) * SIZE, AO2
	mov	f14 = f0
	}
	;;
	{ .mmf
	shladd	AO5 = LDA, 1, AO3
	shladd	AO6 = LDA, 1, AO4
	mov	f16 = f0
	}
	{ .mmf
	adds	RPRE3  = (RPREFETCH +  4) * SIZE, AO3
	adds	RPRE4  = (RPREFETCH +  6) * SIZE, AO4
	mov	f18 = f0
	}
	;;
	{ .mmf
	shladd	AO7 = LDA, 1, AO5
	shladd	AO8 = LDA, 1, AO6
	mov	f20 = f0
	}
	{ .mmf
	adds	RPRE5  = (RPREFETCH +  8) * SIZE, AO5
	adds	RPRE6  = (RPREFETCH + 10) * SIZE, AO6
	mov	f22 = f0
	}
	;;
	{ .mfi
	shladd	A   = LDA, 3, A
	mov	f9  = f0
	mov	ar.ec= 5
	}
	{ .mmf
	adds	RPRE7  = (RPREFETCH + 12) * SIZE, AO7
	adds	RPRE8  = (RPREFETCH + 14) * SIZE, AO8
	mov	f11 = f0
	}
	;;
	{ .mmf
	adds	WPRE = 16 * SIZE, CLD1
	adds	PREB   = RPREFETCH * SIZE, BO
	mov	f13 = f0
	}
	{ .mmf
	adds	I = -1, M
	cmp.eq	p16, p0 = r0, r0
	mov	f15 = f0
	}
	;;
	{ .mfi
	cmp.eq  p12, p0 = r0, r0
	mov	f17 = f0
	mov	ar.lc = I
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f19 = f0
	}
	;;
	{ .mmf
	lfetch.excl.nt1	[WPRE]
	nop	__LINE__
	mov	f21 = f0
	}
	{ .mmf
	mov	I = 0
	nop	__LINE__
	mov	f23 = f0
	}
	;;
	.align 16

.L16:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32,  f37  = [AO1], 2 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p13, p0 = 1, I
	nop   __LINE__
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p13) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD1	f10 = f116, f46, f10
	}
	{ .mmf
	(p16) cmp.eq.unc p14, p0 = 2, I
	(p16) cmp.eq.unc p15, p0 = 3, I
	(p20) ADD2	f11 = f121, f46, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f42,  f47  = [AO2], 2 * SIZE
	nop   __LINE__
	(p20) ADD1	f12 = f116, f56, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f13 = f121, f56, f13
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE2], 16 * SIZE
	nop   __LINE__
	(p20) ADD1	f14 = f116, f66, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f15 = f121, f66, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f52,  f57  = [AO3], 2 * SIZE
	nop   __LINE__
	(p20) ADD3	f8  = f121, f41, f8
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD4	f9  = f116, f41, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	nop   __LINE__
	(p20) ADD3	f10 = f121, f51, f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD4	f11 = f116, f51, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f62,  f67  = [AO4], 2 * SIZE
	nop   __LINE__
	(p20) ADD3	f12 = f121, f61, f12
	}
	{ .mmf
	(p16) cmp.eq.unc p12, p0 = 4, I
	(p16) cmp.eq.unc p13, p0 = 5, I
	(p20) ADD4	f13 = f116, f61, f13
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	nop   __LINE__
	(p20) ADD3	f14 = f121, f71, f14
	}
	{ .mmf
	(p16) cmp.eq.unc p14, p0 = 6, I
	(p16) cmp.eq.unc p15, p0 = 7, I
	(p20) ADD4	f15 = f116, f71, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f72,  f77  = [AO5], 2 * SIZE
	nop   __LINE__
	(p20) ADD1	f16 = f116, f76, f16
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f17 = f121, f76, f17
	}
	;;
	{ .mmf
	(p12) PREFETCH [RPRE5], 16 * SIZE
	nop   __LINE__
	(p20) ADD1	f18 = f116, f86, f18
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f19 = f121, f86, f19
	}
	;;
	{ .mmf
	(p16) LDFPD	f82,  f87  = [AO6], 2 * SIZE
	nop   __LINE__
	(p20) ADD1	f20 = f116, f96, f20
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f21 = f121, f96, f21
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE6], 16 * SIZE
	nop   __LINE__
	(p20) ADD1	f22 = f116, f106, f22
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f23 = f121, f106, f23
	}
	;;
	{ .mmf
	(p16) LDFPD	f92,  f97  = [AO7], 2 * SIZE
	nop   __LINE__
	(p20) ADD3	f16 = f121, f81, f16
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD4	f17 = f116, f81, f17
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE7], 16 * SIZE
	nop   __LINE__
	(p20) ADD3	f18 = f121, f91, f18
	}
	{ .mmf
	nop   __LINE__
	(p16) adds I = 1, I
	(p20) ADD4	f19 = f116, f91, f19
	}
	;;
	{ .mmf
	(p16) LDFPD	f102, f107 = [AO8], 2 * SIZE
	nop   __LINE__
	(p20) ADD3	f20 = f121, f101, f20
	}
	{ .mmf
	(p15) mov I = 0
	nop   __LINE__
	(p20) ADD4	f21 = f116, f101, f21
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE8], 16 * SIZE
	nop   __LINE__
	(p20) ADD3	f22 = f121, f111, f22
	}
	{ .mfb
	(p16) cmp.eq.unc p12, p0 = 0, I
	(p20) ADD4	f23 = f116, f111, f23
	br.ctop.sptk.few .L16
	}
	;;

.L18:
	LDFD	f32 = [CLD1], SIZE
	LDFD	f36 = [CLD2], SIZE
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	LDFD	f37 = [CLD2], INCYM1
	;;
	LDFD	f34 = [CLD1], SIZE
	LDFD	f38 = [CLD2], SIZE
	;;
	LDFD	f35 = [CLD1], INCY3M1
	LDFD	f39 = [CLD2], INCY3M1
	;;
	LDFD	f40 = [CLD1], SIZE
	LDFD	f44 = [CLD2], SIZE
	;;
	LDFD	f41 = [CLD1], INCYM1
	LDFD	f45 = [CLD2], INCYM1
	;;
	LDFD	f42 = [CLD1], SIZE
	LDFD	f46 = [CLD2], SIZE
	;;
	LDFD	f43 = [CLD1], INCY3M1
	LDFD	f47 = [CLD2], INCY3M1
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f36 = ALPHA_R, f12, f36
	FMA	f33 = ALPHA_I, f8,  f33
	FMA	f37 = ALPHA_I, f12, f37
	FMA	f34 = ALPHA_R, f10, f34
	FMA	f38 = ALPHA_R, f14, f38
	FMA	f35 = ALPHA_I, f10, f35
	FMA	f39 = ALPHA_I, f14, f39
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FNMA	f36 = ALPHA_I, f13, f36
	FMA	f33 = ALPHA_R, f9,  f33
	FMA	f37 = ALPHA_R, f13, f37
	FNMA	f34 = ALPHA_I, f11, f34
	FNMA	f38 = ALPHA_I, f15, f38
	FMA	f35 = ALPHA_R, f11, f35
	FMA	f39 = ALPHA_R, f15, f39
	;;
	FMA	f40 = ALPHA_R, f16, f40
	FMA	f44 = ALPHA_R, f20, f44
	FMA	f41 = ALPHA_I, f16, f41
	FMA	f45 = ALPHA_I, f20, f45
	FMA	f42 = ALPHA_R, f18, f42
	FMA	f46 = ALPHA_R, f22, f46
	FMA	f43 = ALPHA_I, f18, f43
	FMA	f47 = ALPHA_I, f22, f47
	;;
	{ .mmf
	STFD [CST1] = f32, SIZE
	STFD [CST2] = f36, SIZE
	FNMA	f40 = ALPHA_I, f17, f40
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FNMA	f44 = ALPHA_I, f21, f44
	}
	;;
	{ .mmf
	STFD [CST1] = f33
	STFD [CST2] = f37
	FMA	f41 = ALPHA_R, f17, f41
	}
	{ .mmf
	add  CST1 = CST1, INCYM1
	add  CST2 = CST2, INCYM1
	FMA	f45 = ALPHA_R, f21, f45
	}
	;;
	{ .mmf
	STFD [CST1] = f34, SIZE
	STFD [CST2] = f38, SIZE
	FNMA	f42 = ALPHA_I, f19, f42
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FNMA	f46 = ALPHA_I, f23, f46
	}
	;;
	{ .mmf
	STFD [CST1] = f35
	STFD [CST2] = f39
	FMA	f43 = ALPHA_R, f19, f43
	}
	{ .mmf
	add  CST1 = CST1, INCY3M1
	add  CST2 = CST2, INCY3M1
	FMA	f47 = ALPHA_R, f23, f47
	}
	;;
	{ .mmi
	STFD [CST1] = f40, SIZE
	STFD [CST2] = f44, SIZE
	adds J = -1, J
	}
	;;
	{ .mmi
	STFD [CST1] = f41
	STFD [CST2] = f45
	add  CST1 = CST1, INCYM1
	}
	{ .mmi
	nop  __LINE__
	nop  __LINE__
	add  CST2 = CST2, INCYM1
	}
	;;
	{ .mmi
	STFD [CST1] = f42, SIZE
	STFD [CST2] = f46, SIZE
	cmp.lt p6, p0 = 0, J
	}
	;;
	{ .mmi
	STFD [CST1] = f43
	STFD [CST2] = f47
	add  CST1 = CST1, INCY3M1
	}
	{ .mmb
	add  CST2 = CST2, INCY3M1
	(p6) br.cond.dptk .L11
	}
	;;
	.align 16

.L20:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 2
	}
	;;
	{ .mmf
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	mov	f12 = f0
	}
	{ .mfb
	mov	BO  = BUFFER
	mov	f14 = f0
	(p6) br.cond.dpnt .L30
	}
	;;
	{ .mfi
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	mov	f9  = f0
	mov	ar.ec= 5
	}
	{ .mmf
	adds	RPRE2  = (RPREFETCH +  2) * SIZE, AO2
	adds	I = -1, M
	mov	f11 = f0
	}
	;;
	{ .mmf
	adds	RPRE3  = (RPREFETCH +  4) * SIZE, AO3
	adds	RPRE4  = (RPREFETCH +  6) * SIZE, AO4
	mov	f13 = f0
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	shladd	A   = LDA, 2, A
	mov	f15 = f0
	}
	;;
	{ .mmi
	lfetch.excl.nt1	[WPRE]
	adds	PREB   = RPREFETCH * SIZE, BO
	mov	ar.lc = I
	}
	{ .mmi
	adds	WPRE = 16 * SIZE, CLD1
	cmp.eq  p12, p0 = r0, r0
	mov	I = 0
	}
	;;
	.align 16

.L26:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32,  f37  = [AO1], 2 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p13, p0 = 2, I
	nop   __LINE__
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p12) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD1	f10 = f116, f46, f10
	}
	{ .mmf
	(p16) cmp.eq.unc p14, p0 = 4, I
	(p16) cmp.eq.unc p15, p0 = 6, I
	(p20) ADD2	f11 = f121, f46, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f42,  f47  = [AO2], 2 * SIZE
	nop   __LINE__
	(p20) ADD1	f12 = f116, f56, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f13 = f121, f56, f13
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE2], 16 * SIZE
	nop   __LINE__
	(p20) ADD1	f14 = f116, f66, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f15 = f121, f66, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f52,  f57  = [AO3], 2 * SIZE
	nop   __LINE__
	(p20) ADD3	f8  = f121, f41, f8
	}
	{ .mmf
	(p16) adds I = 1, I
	nop   __LINE__
	(p20) ADD4	f9  = f116, f41, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	nop   __LINE__
	(p20) ADD3	f10 = f121, f51, f10
	}
	{ .mmf
	(p16) cmp.eq.unc p15, p0 = 8, I
	nop   __LINE__
	(p20) ADD4	f11 = f116, f51, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f62,  f67  = [AO4], 2 * SIZE
	nop   __LINE__
	(p20) ADD3	f12 = f121, f61, f12
	}
	{ .mmf
	(p15) mov I = 0
	nop   __LINE__
	(p20) ADD4	f13 = f116, f61, f13
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	nop   __LINE__
	(p20) ADD3	f14 = f121, f71, f14
	}
	{ .mfb
	(p16) cmp.eq.unc p12, p0 = 0, I
	(p20) ADD4	f15 = f116, f71, f15
	br.ctop.sptk.few .L26
	}
	;;
.L28:
	LDFD	f32 = [CLD1], SIZE
	LDFD	f36 = [CLD2], SIZE
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	LDFD	f37 = [CLD2], INCYM1
	;;
	LDFD	f34 = [CLD1], SIZE
	LDFD	f38 = [CLD2], SIZE
	;;
	LDFD	f35 = [CLD1], INCY3M1
	LDFD	f39 = [CLD2], INCY3M1
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f36 = ALPHA_R, f12, f36
	FMA	f33 = ALPHA_I, f8,  f33
	FMA	f37 = ALPHA_I, f12, f37
	FMA	f34 = ALPHA_R, f10, f34
	FMA	f38 = ALPHA_R, f14, f38
	FMA	f35 = ALPHA_I, f10, f35
	FMA	f39 = ALPHA_I, f14, f39
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FNMA	f36 = ALPHA_I, f13, f36
	FMA	f33 = ALPHA_R, f9,  f33
	FMA	f37 = ALPHA_R, f13, f37
	FNMA	f34 = ALPHA_I, f11, f34
	FNMA	f38 = ALPHA_I, f15, f38
	FMA	f35 = ALPHA_R, f11, f35
	FMA	f39 = ALPHA_R, f15, f39
	;;
	STFD [CST1] = f32, SIZE
	STFD [CST2] = f36, SIZE
	;;
	STFD [CST1] = f33
	STFD [CST2] = f37
	add  CST1 = CST1, INCYM1
	add  CST2 = CST2, INCYM1
	;;
	STFD [CST1] = f34, SIZE
	STFD [CST2] = f38, SIZE
	;;
	STFD [CST1] = f35
	STFD [CST2] = f39
	add  CST1 = CST1, INCY3M1
	add  CST2 = CST2, INCY3M1
	;;
	.align 16

.L30:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 1
	}
	;;
	{ .mmf
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	adds	RPRE2  = (RPREFETCH +  2) * SIZE, AO2
	mov	f12 = f0
	}
	{ .mfb
	adds	I = -1, M
	mov	f14 = f0
	(p6) br.cond.dpnt .L40
	}
	;;
	{ .mfi
	mov	BO  = BUFFER
	mov	f9  = f0
	mov	ar.ec= 5
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	shladd	A   = LDA, 1, A
	mov	f11 = f0
	}
	;;
	{ .mfi
	adds	WPRE = 16 * SIZE, CLD1
	mov	f13 = f0
	mov	ar.lc = I
	}
	{ .mmf
	adds	PREB   = RPREFETCH * SIZE, BO
	nop	__LINE__
	mov	f15 = f0
	}
	;;
	{ .mmi
	lfetch.excl.nt1	[WPRE]
	cmp.eq  p12, p0 = r0, r0
	mov	I = 0
	}
	;;
	.align 16

.L36:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32,  f37  = [AO1], 2 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p13, p0 = 4, I
	(p16) adds I = 1, I
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p12) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD1	f10 = f116, f46, f10
	}
	{ .mmf
	(p16) cmp.eq.unc p12, p0 = 8, I
	(p20) ADD2	f11 = f121, f46, f11
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFPD	f42,  f47  = [AO2], 2 * SIZE
	(p20) ADD3	f12 = f121, f41, f12
	}
	{ .mmf
	(p12) mov I = 0
	(p20) ADD4	f13 = f116, f41, f13
	}
	;;
	{ .mmf
	(p20) ADD3	f14 = f121, f51, f14
	}
	{ .mfb
	nop	__LINE__
	(p20) ADD4	f15 = f116, f51, f15
	br.ctop.sptk.few .L36
	}
	;;

.L38:
	LDFD	f32 = [CLD1], SIZE
	FADD	f8  = f8,  f12
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	FADD	f10 = f10, f14
	;;
	LDFD	f34 = [CLD1], SIZE
	FADD	f9  = f9,  f13
	;;
	LDFD	f35 = [CLD1], INCYM1
	FADD	f11 = f11, f15
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f33 = ALPHA_I, f8,  f33
	FMA	f34 = ALPHA_R, f10, f34
	FMA	f35 = ALPHA_I, f10, f35
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FMA	f33 = ALPHA_R, f9,  f33
	FNMA	f34 = ALPHA_I, f11, f34
	FMA	f35 = ALPHA_R, f11, f35
	;;
	STFD [CST1] = f32, SIZE
	;;
	STFD [CST1] = f33
	add  CST1 = CST1, INCYM1
	;;
	STFD [CST1] = f34, SIZE
	;;
	STFD [CST1] = f35
	add  CST1 = CST1, INCYM1
	;;
	.align 16


.L40:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	mov	f9  = f0
	tbit.z	p6, p0  = N, 0
	}
	;;
	{ .mfi
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	mov	f10 = f0
	mov	ar.ec= 5
	}
	{ .mfb
	adds	I = -1, M
	mov	f11 = f0
	(p6) br.cond.dpnt .L999
	}
	;;
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	add	A   = LDA, A
	mov	ar.lc = I
	}
	{ .mmi
	adds	WPRE = 16 * SIZE, CLD1
	adds	PREB   = RPREFETCH * SIZE, BO
	mov	BO  = BUFFER
	}
	;;
	{ .mmi
	lfetch.excl.nt1	[WPRE]
	cmp.eq  p12, p0 = r0, r0
	mov	I = 0
	}
	;;
	.align 16

.L46:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32,  f37  = [AO1], 2 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p12, p0 = 7, I
	(p16) adds I = 1, I
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD3	f10  = f121, f41, f10
	}
	{ .mfb
	(p12) mov I = 0
	(p20) ADD4	f11  = f116, f41, f11
	br.ctop.sptk.few .L46
	}
	;;

.L48:
	LDFD	f32 = [CLD1], SIZE
	FADD	f8  = f8,  f10
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	FADD	f9  = f9,  f11
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f33 = ALPHA_I, f8,  f33
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FMA	f33 = ALPHA_R, f9,  f33
	;;
	STFD [CST1] = f32, SIZE
	;;
	STFD [CST1] = f33
	add  CST1 = CST1, INCYM1
	br   .L999
	.align 16
	;;

.L100:
	{ .mmi
	mov	CLD1  = Y
	shladd	CLD2  = INCY, 1, Y
	shr	J   = N, 3
	}
	;;
	{ .mmb
	mov	CST1  = Y
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L120
	}
	;;
	.align 16

.L111:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	mov	BO  = BUFFER
	}
	;;
	{ .mmf
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	mov	f12 = f0
	}
	{ .mmf
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	adds	RPRE2  = (RPREFETCH +  2) * SIZE, AO2
	mov	f14 = f0
	}
	;;
	{ .mmf
	shladd	AO5 = LDA, 1, AO3
	shladd	AO6 = LDA, 1, AO4
	mov	f16 = f0
	}
	{ .mmf
	adds	RPRE3  = (RPREFETCH +  4) * SIZE, AO3
	adds	RPRE4  = (RPREFETCH +  6) * SIZE, AO4
	mov	f18 = f0
	}
	;;
	{ .mmf
	shladd	AO7 = LDA, 1, AO5
	shladd	AO8 = LDA, 1, AO6
	mov	f20 = f0
	}
	{ .mmf
	adds	RPRE5  = (RPREFETCH +  8) * SIZE, AO5
	adds	RPRE6  = (RPREFETCH + 10) * SIZE, AO6
	mov	f22 = f0
	}
	;;
	{ .mfi
	shladd	A   = LDA, 3, A
	mov	f9  = f0
	mov	ar.ec= 5
	}
	{ .mmf
	adds	RPRE7  = (RPREFETCH + 12) * SIZE, AO7
	adds	RPRE8  = (RPREFETCH + 14) * SIZE, AO8
	mov	f11 = f0
	}
	;;
	{ .mmf
	adds	WPRE = 16 * SIZE, CLD1
	adds	PREB   = RPREFETCH * SIZE, BO
	mov	f13 = f0
	}
	{ .mmf
	adds	I = -1, M
	cmp.eq	p16, p0 = r0, r0
	mov	f15 = f0
	}
	;;
	{ .mfi
	cmp.eq  p12, p0 = r0, r0
	mov	f17 = f0
	mov	ar.lc = I
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f19 = f0
	}
	;;
	{ .mmf
	lfetch.excl.nt1	[WPRE]
	nop	__LINE__
	mov	f21 = f0
	}
	{ .mmf
	mov	I = 0
	nop	__LINE__
	mov	f23 = f0
	}
	;;
	.align 16

.L116:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFD	f32 = [AO1], 1 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p13, p0 = 1, I
	(p16) cmp.eq.unc p14, p0 = 2, I
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p13) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD1	f10 = f116, f46, f10
	}
	{ .mmf
	(p16) LDFD	f37 = [AO1], 1 * SIZE
	(p16) cmp.eq.unc p15, p0 = 3, I
	(p20) ADD2	f11 = f121, f46, f11
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFD	f42 = [AO2], 1 * SIZE
	(p20) ADD1	f12 = f116, f56, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f13 = f121, f56, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f47 = [AO2], 1 * SIZE
	nop   __LINE__
	(p20) ADD1	f14 = f116, f66, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f15 = f121, f66, f15
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	(p16) LDFD	f52 = [AO3], 1 * SIZE
	(p20) ADD3	f8  = f121, f41, f8
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD4	f9  = f116, f41, f9
	}
	;;
	{ .mmf
	(p16) LDFD	f57 = [AO3], 1 * SIZE
	nop   __LINE__
	(p20) ADD3	f10 = f121, f51, f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD4	f11 = f116, f51, f11
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	(p16) LDFD	f62 = [AO4], 1 * SIZE
	(p20) ADD3	f12 = f121, f61, f12
	}
	{ .mmf
	(p16) cmp.eq.unc p12, p0 = 4, I
	(p16) cmp.eq.unc p13, p0 = 5, I
	(p20) ADD4	f13 = f116, f61, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f67 = [AO4], 1 * SIZE
	nop   __LINE__
	(p20) ADD3	f14 = f121, f71, f14
	}
	{ .mmf
	(p16) cmp.eq.unc p14, p0 = 6, I
	(p16) cmp.eq.unc p15, p0 = 7, I
	(p20) ADD4	f15 = f116, f71, f15
	}
	;;
	{ .mmf
	(p12) PREFETCH [RPRE5], 16 * SIZE
	(p16) LDFD	f72 = [AO5], 1 * SIZE
	(p20) ADD1	f16 = f116, f76, f16
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f17 = f121, f76, f17
	}
	;;
	{ .mmf
	(p16) LDFD	f77 = [AO5], 1 * SIZE
	nop   __LINE__
	(p20) ADD1	f18 = f116, f86, f18
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f19 = f121, f86, f19
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE6], 16 * SIZE
	(p16) LDFD	f82 = [AO6], 1 * SIZE
	(p20) ADD1	f20 = f116, f96, f20
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f21 = f121, f96, f21
	}
	;;
	{ .mmf
	(p16) LDFD	f87 = [AO6], 1 * SIZE
	nop   __LINE__
	(p20) ADD1	f22 = f116, f106, f22
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f23 = f121, f106, f23
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE7], 16 * SIZE
	(p16) LDFD	f92 = [AO7], 1 * SIZE
	(p20) ADD3	f16 = f121, f81, f16
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD4	f17 = f116, f81, f17
	}
	;;
	{ .mmf
	(p16) LDFD	f97 = [AO7], 1 * SIZE
	nop   __LINE__
	(p20) ADD3	f18 = f121, f91, f18
	}
	{ .mmf
	nop   __LINE__
	(p16) adds I = 1, I
	(p20) ADD4	f19 = f116, f91, f19
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE8], 16 * SIZE
	(p16) LDFD	f102 = [AO8], 1 * SIZE
	(p20) ADD3	f20 = f121, f101, f20
	}
	{ .mmf
	(p15) mov I = 0
	nop   __LINE__
	(p20) ADD4	f21 = f116, f101, f21
	}
	;;
	{ .mmf
	(p16) LDFD	f107 = [AO8], 1 * SIZE
	nop   __LINE__
	(p20) ADD3	f22 = f121, f111, f22
	}
	{ .mfb
	(p16) cmp.eq.unc p12, p0 = 0, I
	(p20) ADD4	f23 = f116, f111, f23
	br.ctop.sptk.few .L116
	}
	;;

.L118:
	LDFD	f32 = [CLD1], SIZE
	LDFD	f36 = [CLD2], SIZE
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	LDFD	f37 = [CLD2], INCYM1
	;;
	LDFD	f34 = [CLD1], SIZE
	LDFD	f38 = [CLD2], SIZE
	;;
	LDFD	f35 = [CLD1], INCY3M1
	LDFD	f39 = [CLD2], INCY3M1
	;;
	LDFD	f40 = [CLD1], SIZE
	LDFD	f44 = [CLD2], SIZE
	;;
	LDFD	f41 = [CLD1], INCYM1
	LDFD	f45 = [CLD2], INCYM1
	;;
	LDFD	f42 = [CLD1], SIZE
	LDFD	f46 = [CLD2], SIZE
	;;
	LDFD	f43 = [CLD1], INCY3M1
	LDFD	f47 = [CLD2], INCY3M1
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f36 = ALPHA_R, f12, f36
	FMA	f33 = ALPHA_I, f8,  f33
	FMA	f37 = ALPHA_I, f12, f37
	FMA	f34 = ALPHA_R, f10, f34
	FMA	f38 = ALPHA_R, f14, f38
	FMA	f35 = ALPHA_I, f10, f35
	FMA	f39 = ALPHA_I, f14, f39
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FNMA	f36 = ALPHA_I, f13, f36
	FMA	f33 = ALPHA_R, f9,  f33
	FMA	f37 = ALPHA_R, f13, f37
	FNMA	f34 = ALPHA_I, f11, f34
	FNMA	f38 = ALPHA_I, f15, f38
	FMA	f35 = ALPHA_R, f11, f35
	FMA	f39 = ALPHA_R, f15, f39
	;;
	FMA	f40 = ALPHA_R, f16, f40
	FMA	f44 = ALPHA_R, f20, f44
	FMA	f41 = ALPHA_I, f16, f41
	FMA	f45 = ALPHA_I, f20, f45
	FMA	f42 = ALPHA_R, f18, f42
	FMA	f46 = ALPHA_R, f22, f46
	FMA	f43 = ALPHA_I, f18, f43
	FMA	f47 = ALPHA_I, f22, f47
	;;
	{ .mmf
	STFD [CST1] = f32, SIZE
	STFD [CST2] = f36, SIZE
	FNMA	f40 = ALPHA_I, f17, f40
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FNMA	f44 = ALPHA_I, f21, f44
	}
	;;
	{ .mmf
	STFD [CST1] = f33
	STFD [CST2] = f37
	FMA	f41 = ALPHA_R, f17, f41
	}
	{ .mmf
	add  CST1 = CST1, INCYM1
	add  CST2 = CST2, INCYM1
	FMA	f45 = ALPHA_R, f21, f45
	}
	;;
	{ .mmf
	STFD [CST1] = f34, SIZE
	STFD [CST2] = f38, SIZE
	FNMA	f42 = ALPHA_I, f19, f42
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FNMA	f46 = ALPHA_I, f23, f46
	}
	;;
	{ .mmf
	STFD [CST1] = f35
	STFD [CST2] = f39
	FMA	f43 = ALPHA_R, f19, f43
	}
	{ .mmf
	add  CST1 = CST1, INCY3M1
	add  CST2 = CST2, INCY3M1
	FMA	f47 = ALPHA_R, f23, f47
	}
	;;
	{ .mmi
	STFD [CST1] = f40, SIZE
	STFD [CST2] = f44, SIZE
	adds J = -1, J
	}
	;;
	{ .mmi
	STFD [CST1] = f41
	STFD [CST2] = f45
	add  CST1 = CST1, INCYM1
	}
	{ .mmi
	nop  __LINE__
	nop  __LINE__
	add  CST2 = CST2, INCYM1
	}
	;;
	{ .mmi
	STFD [CST1] = f42, SIZE
	STFD [CST2] = f46, SIZE
	cmp.lt p6, p0 = 0, J
	}
	;;
	{ .mmi
	STFD [CST1] = f43
	STFD [CST2] = f47
	add  CST1 = CST1, INCY3M1
	}
	{ .mmb
	add  CST2 = CST2, INCY3M1
	(p6) br.cond.dptk .L111
	}
	;;
	.align 16

.L120:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 2
	}
	;;
	{ .mmf
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	mov	f12 = f0
	}
	{ .mfb
	mov	BO  = BUFFER
	mov	f14 = f0
	(p6) br.cond.dpnt .L130
	}
	;;
	{ .mfi
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	mov	f9  = f0
	mov	ar.ec= 5
	}
	{ .mmf
	adds	RPRE2  = (RPREFETCH +  2) * SIZE, AO2
	adds	I = -1, M
	mov	f11 = f0
	}
	;;
	{ .mmf
	adds	RPRE3  = (RPREFETCH +  4) * SIZE, AO3
	adds	RPRE4  = (RPREFETCH +  6) * SIZE, AO4
	mov	f13 = f0
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	shladd	A   = LDA, 2, A
	mov	f15 = f0
	}
	;;
	{ .mmi
	lfetch.excl.nt1	[WPRE]
	adds	PREB   = RPREFETCH * SIZE, BO
	mov	ar.lc = I
	}
	{ .mmi
	adds	WPRE = 16 * SIZE, CLD1
	cmp.eq  p12, p0 = r0, r0
	mov	I = 0
	}
	;;
	.align 16

.L126:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFD	f32 = [AO1], 1 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p13, p0 = 2, I
	(p16) cmp.eq.unc p14, p0 = 4, I
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p12) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD1	f10 = f116, f46, f10
	}
	{ .mmf
	(p16) LDFD	f37 = [AO1], 1 * SIZE
	(p16) cmp.eq.unc p15, p0 = 6, I
	(p20) ADD2	f11 = f121, f46, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f42 = [AO2], 1 * SIZE
	nop   __LINE__
	(p20) ADD1	f12 = f116, f56, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f13 = f121, f56, f13
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFD	f47 = [AO2], 1 * SIZE
	(p20) ADD1	f14 = f116, f66, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p20) ADD2	f15 = f121, f66, f15
	}
	;;
	{ .mmf
	(p16) LDFD	f52 = [AO3], 1 * SIZE
	nop   __LINE__
	(p20) ADD3	f8  = f121, f41, f8
	}
	{ .mmf
	nop   __LINE__
	(p16) adds I = 1, I
	(p20) ADD4	f9  = f116, f41, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	(p16) LDFD	f57 = [AO3], 1 * SIZE
	(p20) ADD3	f10 = f121, f51, f10
	}
	{ .mmf
	nop   __LINE__
	(p16) cmp.eq.unc p15, p0 = 8, I
	(p20) ADD4	f11 = f116, f51, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f62 = [AO4], 1 * SIZE
	nop   __LINE__
	(p20) ADD3	f12 = f121, f61, f12
	}
	{ .mmf
	(p15) mov I = 0
	nop   __LINE__
	(p20) ADD4	f13 = f116, f61, f13
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	(p16) LDFD	f67 = [AO4], 1 * SIZE
	(p20) ADD3	f14 = f121, f71, f14
	}
	{ .mfb
	(p16) cmp.eq.unc p12, p0 = 0, I
	(p20) ADD4	f15 = f116, f71, f15
	br.ctop.sptk.few .L126
	}
	;;
.L128:
	LDFD	f32 = [CLD1], SIZE
	LDFD	f36 = [CLD2], SIZE
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	LDFD	f37 = [CLD2], INCYM1
	;;
	LDFD	f34 = [CLD1], SIZE
	LDFD	f38 = [CLD2], SIZE
	;;
	LDFD	f35 = [CLD1], INCY3M1
	LDFD	f39 = [CLD2], INCY3M1
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f36 = ALPHA_R, f12, f36
	FMA	f33 = ALPHA_I, f8,  f33
	FMA	f37 = ALPHA_I, f12, f37
	FMA	f34 = ALPHA_R, f10, f34
	FMA	f38 = ALPHA_R, f14, f38
	FMA	f35 = ALPHA_I, f10, f35
	FMA	f39 = ALPHA_I, f14, f39
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FNMA	f36 = ALPHA_I, f13, f36
	FMA	f33 = ALPHA_R, f9,  f33
	FMA	f37 = ALPHA_R, f13, f37
	FNMA	f34 = ALPHA_I, f11, f34
	FNMA	f38 = ALPHA_I, f15, f38
	FMA	f35 = ALPHA_R, f11, f35
	FMA	f39 = ALPHA_R, f15, f39
	;;
	STFD [CST1] = f32, SIZE
	STFD [CST2] = f36, SIZE
	;;
	STFD [CST1] = f33
	STFD [CST2] = f37
	add  CST1 = CST1, INCYM1
	add  CST2 = CST2, INCYM1
	;;
	STFD [CST1] = f34, SIZE
	STFD [CST2] = f38, SIZE
	;;
	STFD [CST1] = f35
	STFD [CST2] = f39
	add  CST1 = CST1, INCY3M1
	add  CST2 = CST2, INCY3M1
	;;
	.align 16

.L130:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 1
	}
	;;
	{ .mmf
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	adds	RPRE2  = (RPREFETCH +  2) * SIZE, AO2
	mov	f12 = f0
	}
	{ .mfb
	adds	I = -1, M
	mov	f14 = f0
	(p6) br.cond.dpnt .L140
	}
	;;
	{ .mfi
	mov	BO  = BUFFER
	mov	f9  = f0
	mov	ar.ec= 5
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	shladd	A   = LDA, 1, A
	mov	f11 = f0
	}
	;;
	{ .mfi
	adds	WPRE = 16 * SIZE, CLD1
	mov	f13 = f0
	mov	ar.lc = I
	}
	{ .mmf
	adds	PREB   = RPREFETCH * SIZE, BO
	nop	__LINE__
	mov	f15 = f0
	}
	;;
	{ .mmi
	lfetch.excl.nt1	[WPRE]
	cmp.eq  p12, p0 = r0, r0
	mov	I = 0
	}
	;;
	.align 16

.L136:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFD	f32 = [AO1], 1 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p13, p0 = 4, I
	(p16) adds I = 1, I
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p12) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p20) ADD1	f10 = f116, f46, f10
	}
	{ .mmf
	(p16) LDFD	f37 = [AO1], 1 * SIZE
	(p16) cmp.eq.unc p12, p0 = 8, I
	(p20) ADD2	f11 = f121, f46, f11
	}
	;;
	{ .mmf
	(p13) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFD	f42 = [AO2], 1 * SIZE
	(p20) ADD3	f12 = f121, f41, f12
	}
	{ .mmf
	(p12) mov I = 0
	nop	__LINE__
	(p20) ADD4	f13 = f116, f41, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f47 = [AO2], 1 * SIZE
	nop	__LINE__
	(p20) ADD3	f14 = f121, f51, f14
	}
	{ .mfb
	nop	__LINE__
	(p20) ADD4	f15 = f116, f51, f15
	br.ctop.sptk.few .L136
	}
	;;

.L138:
	LDFD	f32 = [CLD1], SIZE
	FADD	f8  = f8,  f12
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	FADD	f10 = f10, f14
	;;
	LDFD	f34 = [CLD1], SIZE
	FADD	f9  = f9,  f13
	;;
	LDFD	f35 = [CLD1], INCYM1
	FADD	f11 = f11, f15
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f33 = ALPHA_I, f8,  f33
	FMA	f34 = ALPHA_R, f10, f34
	FMA	f35 = ALPHA_I, f10, f35
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FMA	f33 = ALPHA_R, f9,  f33
	FNMA	f34 = ALPHA_I, f11, f34
	FMA	f35 = ALPHA_R, f11, f35
	;;
	STFD [CST1] = f32, SIZE
	;;
	STFD [CST1] = f33
	add  CST1 = CST1, INCYM1
	;;
	STFD [CST1] = f34, SIZE
	;;
	STFD [CST1] = f35
	add  CST1 = CST1, INCYM1
	;;
	.align 16


.L140:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	mov	f9  = f0
	tbit.z	p6, p0  = N, 0
	}
	;;
	{ .mfi
	adds	RPRE1  = (RPREFETCH +  0) * SIZE, AO1
	mov	f10 = f0
	mov	ar.ec= 5
	}
	{ .mfb
	adds	I = -1, M
	mov	f11 = f0
	(p6) br.cond.dpnt .L999
	}
	;;
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	shladd	A   = LDA, 1, A
	mov	ar.lc = I
	}
	{ .mmi
	adds	WPRE = 16 * SIZE, CLD1
	adds	PREB   = RPREFETCH * SIZE, BO
	mov	BO  = BUFFER
	}
	;;
	{ .mmi
	lfetch.excl.nt1	[WPRE]
	cmp.eq  p12, p0 = r0, r0
	mov	I = 0
	}
	;;
	.align 16

.L146:
	{ .mmf
	(p12) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFD	f32  = [AO1], 1 * SIZE
	(p20) ADD1	f8  = f116, f36, f8
	}
	{ .mmf
	(p16) cmp.eq.unc p12, p0 = 7, I
	(p16) adds I = 1, I
	(p20) ADD2	f9  = f121, f36, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f112, f117 = [BO], 2 * SIZE
	(p16) LDFD	f37  = [AO1], 1 * SIZE
	(p20) ADD3	f10  = f121, f41, f10
	}
	{ .mfb
	(p12) mov I = 0
	(p20) ADD4	f11  = f116, f41, f11
	br.ctop.sptk.few .L146
	}
	;;

.L148:
	LDFD	f32 = [CLD1], SIZE
	FADD	f8  = f8,  f10
	shladd	CST2  = INCY, 1, CST1
	;;
	LDFD	f33 = [CLD1], INCYM1
	FADD	f9  = f9,  f11
	;;
	FMA	f32 = ALPHA_R, f8,  f32
	FMA	f33 = ALPHA_I, f8,  f33
	;;
	FNMA	f32 = ALPHA_I, f9,  f32
	FMA	f33 = ALPHA_R, f9,  f33
	;;
	STFD [CST1] = f32, SIZE
	;;
	STFD [CST1] = f33
	add  CST1 = CST1, INCYM1
	;;
	.align 16

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
