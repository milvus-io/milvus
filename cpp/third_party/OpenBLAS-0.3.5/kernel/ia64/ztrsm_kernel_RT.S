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
#define PREFETCHSIZE  (16 *  8)
#else
#define PREFETCHSIZE  (32 *  8)
#endif

#ifndef LN
#define CPREFETCHSIZE  7
#else
#define CPREFETCHSIZE -8
#endif
#define CPREFETCH     lfetch.excl.nt1

#define M	r32
#define N	r33
#define K	r34
#define A	r37
#define B	r38
#define C	r39
#define LDC	r35

#define I	r15
#define J	r16
#define AOFFSET	r17
#define BOFFSET	r18
#define TEMP	r19
#define L	r20

#define C1	r21
#define C2	r22
#define C3	r23
#define C4	r24
#define C5	r25
#define C6	r26
#define C7	r27
#define C8	r28

#define PREA	r8
#define PREB	r9
#define PREC	r10
#define SP	r12
#define ARLC	r29
#define PR	r30
#define ARPFS	r31

#define ALPHA_R	f8
#define ALPHA_I	f9

#define AORIG	loc0
#define KK	loc1
#define KK8	loc2
#define OFFSET	loc3
#define AOFFSET2 loc4
#define BOFFSET2 loc5

#ifndef CONJ
#define FCALC_A	  FSUB
#define FCALC_B	  FADD
#define FMA_A	  FNMA
#define FMA_B	  FMA
#else
#define FCALC_A	  FADD
#define FCALC_B	  FSUB
#define FMA_A	  FMA
#define FMA_B	  FNMA
#endif

#ifndef CONJ
#define FCALC_C	  FMA
#define FCALC_D	  FNMA
#else
#define FCALC_C	  FNMA
#define FCALC_D	  FMA
#endif

#ifndef CONJ
#define FMA_C	FNMA
#define FMA_D	FMA
#define FSUB_A	FSUB
#else
#define FMA_C	FMA
#define FMA_D	FMS
#define FSUB_A	FADD
#endif


	PROLOGUE
	.prologue
	PROFCODE

	{ .mfi
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 8, 0, 0
	mov	f64  = f0
	adds	r14 = 16, SP
	}
	{ .mfi
	nop	__LINE__
	mov	f65  = f0
	adds	r15 = 24, SP
	}
	;;
	{ .mfi
	ld8	LDC    = [r14]
	mov	f81  = f0
	mov	PR = pr
	}
	{ .mfi
	ld8	OFFSET = [r15]
	mov	f96  = f0
	}
	;;
	{ .mfi
	shladd	LDC = LDC, ZBASE_SHIFT, r0
	mov	f97  = f0
	}
	{ .mfi
	nop	__LINE__
	mov	f113 = f0
	}
	;;
#ifdef LN
       { .mmi
	setf.sig f32 = M
	setf.sig f33 = K
	shladd	C = M,  ZBASE_SHIFT, C
	}
	;;
	{.mmf
	nop	__LINE__
	nop	__LINE__
	xmpy.l	f32  = f32, f33
	}
	;;
	{ .mmi
	getf.sig r2 = f32
	;;
	nop	__LINE__
	shladd	A = r2, ZBASE_SHIFT, A
	}
	;;
#endif

#ifdef RN
	sub	KK  = r0, OFFSET
#endif

#ifdef RT
       { .mmi
	setf.sig f32 = N
	setf.sig f33 = K
	nop	 __LINE__
	}
	;;
	{ .mmi
	setf.sig f34 = LDC
	nop	 __LINE__
	nop	 __LINE__
	}
	;;
	{ .mmf
	nop	 __LINE__
	nop	 __LINE__
	xmpy.l	f33  = f32, f33
	}
	{ .mmf
	nop	 __LINE__
	sub	KK = N, OFFSET
	xmpy.l	f34  = f32, f34
	}
	;;
	{ .mmi
	getf.sig r2 = f33
	getf.sig r3 = f34
	}
	;;
	shladd	B = r2, ZBASE_SHIFT, B
	add	C = r3, C
#endif
	;;
	.body
	{ .mfi
	nop	__LINE__
	mov	f80  = f0
	mov	ARLC  = ar.lc
	}
	{ .mfb
	mov	f112 = f0
	}
	;;
	;;
	shr	I  = M, 2
	tbit.z	p6, p0 = N, 0
	(p6)	br.cond.dpnt .L050
	;;

#ifdef RT
       { .mmi
	shl	r2 = K, ZBASE_SHIFT
	}
	;;
	{ .mmi
	sub	B = B, r2
	sub	C = C, LDC
	nop	__LINE__
	}
	;;
#endif
	mov	C1 = C

#ifdef LN
	add	KK = M, OFFSET
#elif defined LT
	mov	KK = OFFSET
#else
	nop	__LINE__
#endif
	;;
#if defined(LN) || defined(RT)
	mov	AORIG = A
#else
	mov	AOFFSET = A
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	{ .mib
	cmp.eq	p6, p7 = 0, I
#ifndef RT
	add	C = LDC, C
#else
	nop	__LINE__
#endif
	(p6)	br.cond.dpnt .L100
	}
	;;
	.align 16

.L092:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 2 + ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f67  = f0
	}
	;;
#else
	{ .mfi
	add	BOFFSET = r3, B
	mov	f66  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	shladd	AOFFSET = r3, 2, AORIG
	}
	;;
#endif
	;;
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	adds	L =  1, L
	;;
	{ .mfi
	(p7) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	adds	L =  -1, L
	}
	{ .mmf
	(p7) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	CPREFETCH [PREC]
	}
	;;
	{ .mfi
	(p7) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	mov	ar.lc = L
	}
	{ .mmi
	adds	C5 = 4 * SIZE, C1
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L098
	;;
	.align 16

.L093:
/*  1 */
	{ .mfi
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f65   = f32, f49, f65	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],   4 * SIZE
	FMA	f80   = f34, f48, f80	// A3 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f81   = f34, f49, f81	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f96   = f36, f48, f96	// A5 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f97   = f36, f49, f97	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f112   = f38, f48, f112	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f113   = f38, f49, f113	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f64   = f33, f49, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f81   = f35, f48, f81	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f80   = f35, f49, f80	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	FMA	f97   = f37, f48, f97	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f96   = f37, f49, f96	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f113   = f39, f48, f113	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f112   = f39, f49, f112	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f65   = f40, f57, f65	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f42, f56, f80	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f81   = f42, f57, f81	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f96   = f44, f56, f96	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f97   = f44, f57, f97	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	(p3) FMA	f112   = f46, f56, f112	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f113   = f46, f57, f113	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f81   = f43, f56, f81	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f80   = f43, f57, f80	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f97   = f45, f56, f97	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f96   = f45, f57, f96	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f113   = f47, f56, f113	// A8 * B1
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f112   = f47, f57, f112	// A8 * B2
	br.cloop.sptk.few .L093
	}
	;;
.L098:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -4, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 2, AORIG
	add	BOFFSET = r2, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	FSUB	f80  = f74, f80
	FSUB_A	f81  = f75, f81
	FSUB	f96  = f88, f96
	FSUB_A	f97  = f89, f97
	FSUB	f112 = f90, f112
	FSUB_A	f113 = f91, f113
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	FSUB	f80  = f74, f80
	FSUB	f81  = f75, f81
	FSUB	f96  = f88, f96
	FSUB	f97  = f89, f97
	FSUB	f112 = f90, f112
	FSUB	f113 = f91, f113
	;;
#endif

#ifdef LN
	adds	AOFFSET = 30 * SIZE, AOFFSET
	;;
	LDFPD	f72, f73 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f74, f75 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f76, f77 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f78, f79 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f88, f89 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f92, f93 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFPD	f104, f105 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f106, f107 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f72, f112
	FMPY	f33  = f73, f112
	;;
	FMA_C	f112 = f73, f113,  f32
	FMA_D	f113 = f72, f113,  f33
	;;
	FNMA	f96  = f74, f112, f96
	FMA_A	f97  = f75, f112, f97
	FNMA	f80  = f76, f112, f80
	FMA_A	f81  = f77, f112, f81
	FNMA	f64  = f78, f112, f64
	FMA_A	f65  = f79, f112, f65
	;;
	FMA_B	f96  = f75, f113, f96
	FNMA	f97  = f74, f113, f97
	FMA_B	f80  = f77, f113, f80
	FNMA	f81  = f76, f113, f81
	FMA_B	f64  = f79, f113, f64
	FNMA	f65  = f78, f113, f65
	;;
	FMPY	f32  = f88, f96
	FMPY	f33  = f89, f96
	;;
	FMA_C	f96  = f89, f97,  f32
	FMA_D	f97  = f88, f97,  f33
	;;
	FNMA	f80  = f90, f96, f80
	FMA_A	f81  = f91, f96, f81
	FNMA	f64  = f92, f96, f64
	FMA_A	f65  = f93, f96, f65
	;;
	FMA_B	f80  = f91, f97, f80
	FNMA	f81  = f90, f97, f81
	FMA_B	f64  = f93, f97, f64
	FNMA	f65  = f92, f97, f65
	;;
	FMPY	f32  = f104, f80
	FMPY	f33  = f105, f80
	;;
	FMA_C	f80  = f105, f81,  f32
	FMA_D	f81  = f104, f81,  f33
	;;
	FNMA	f64  = f106, f80, f64
	FMA_A	f65  = f107, f80, f65
	;;
	FMA_B	f64  = f107, f81, f64
	FNMA	f65  = f106, f81, f65
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f92, f93 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f94, f95 = [AOFFSET]
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f108, f109 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f110, f111 = [AOFFSET]
	adds	AOFFSET = 8 * SIZE, AOFFSET
	;;
	LDFPD	f126, f127 = [AOFFSET]
	adds	AOFFSET = - 30 * SIZE, AOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	;;
	FNMA	f80  = f74, f64, f80
	FMA_A	f81  = f75, f64, f81
	FNMA	f96  = f76, f64, f96
	FMA_A	f97  = f77, f64, f97
	FNMA	f112 = f78, f64, f112
	FMA_A	f113 = f79, f64, f113
	;;
	FMA_B	f80  = f75, f65, f80
	FNMA	f81  = f74, f65, f81
	FMA_B	f96  = f77, f65, f96
	FNMA	f97  = f76, f65, f97
	FMA_B	f112 = f79, f65, f112
	FNMA	f113 = f78, f65, f113
	;;
	FMPY	f32  = f90, f80
	FMPY	f33  = f91, f80
	;;
	FMA_C	f80  = f91, f81,  f32
	FMA_D	f81  = f90, f81,  f33
	;;
	FNMA	f96  = f92, f80, f96
	FMA_A	f97  = f93, f80, f97
	FNMA	f112 = f94, f80, f112
	FMA_A	f113 = f95, f80, f113
	;;
	FMA_B	f96  = f93, f81, f96
	FNMA	f97  = f92, f81, f97
	FMA_B	f112 = f95, f81, f112
	FNMA	f113 = f94, f81, f113
	;;
	FMPY	f32  = f108, f96
	FMPY	f33  = f109, f96
	;;
	FMA_C	f96  = f109, f97,  f32
	FMA_D	f97  = f108, f97,  f33
	;;
	FNMA	f112 = f110, f96, f112
	FMA_A	f113 = f111, f96, f113
	;;
	FMA_B	f112 = f111, f97, f112
	FNMA	f113 = f110, f97, f113
	;;
	FMPY	f32  = f126, f112
	FMPY	f33  = f127, f112
	;;
	FMA_C	f112 = f127, f113,  f32
	FMA_D	f113 = f126, f113,  f33
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f80
	FMPY	f35  = f73, f80
	FMPY	f36  = f72, f96
	FMPY	f37  = f73, f96
	FMPY	f38  = f72, f112
	FMPY	f39  = f73, f112
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f80  = f73, f81,  f34
	FMA_D	f81  = f72, f81,  f35
	FMA_C	f96  = f73, f97,  f36
	FMA_D	f97  = f72, f97,  f37
	FMA_C	f112 = f73, f113, f38
	FMA_D	f113 = f72, f113, f39
	;;
#endif

#ifdef RT
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f80
	FMPY	f35  = f73, f80
	FMPY	f36  = f72, f96
	FMPY	f37  = f73, f96
	FMPY	f38  = f72, f112
	FMPY	f39  = f73, f112
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f80  = f73, f81,  f34
	FMA_D	f81  = f72, f81,  f35
	FMA_C	f96  = f73, f97,  f36
	FMA_D	f97  = f72, f97,  f37
	FMA_C	f112 = f73, f113, f38
	FMA_D	f113 = f72, f113, f39
	;;
#endif

#if defined(LN) || defined(LT)
	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;
	STFD	[BOFFSET]  = f64,  SIZE
	STFD	[BOFFSET2] = f96,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	STFD	[BOFFSET2] = f97,  SIZE
	;;
	STFD	[BOFFSET]  = f80,  SIZE
	STFD	[BOFFSET2] = f112, SIZE
	;;
	STFD	[BOFFSET]  = f81,  5 * SIZE
	STFD	[BOFFSET2] = f113, 5 * SIZE
	;;
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
#else
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	STFD	[AOFFSET2] = f96,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	STFD	[AOFFSET2] = f97,  SIZE
	;;
	STFD	[AOFFSET]  = f80,  SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET]  = f81,  5 * SIZE
	STFD	[AOFFSET2] = f113, 5 * SIZE
	;;
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -8 * SIZE, C1
	adds	C5 = -8 * SIZE, C5
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	STFD	[C5 ] = f96, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	STFD	[C5 ] = f97, SIZE
	;;
	STFD	[C1 ] = f80,  SIZE
	STFD	[C5 ] = f112, SIZE
	;;
	STFD	[C1 ] = f81,  5 * SIZE
	STFD	[C5 ] = f113, 5 * SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -8 * SIZE, C1
	adds	C5 = -8 * SIZE, C5
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 2, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = L, 2, AOFFSET
	add	BOFFSET = L, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  4, KK
#elif defined LN
	adds	KK = -4, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	(p6)	br.cond.dptk .L092
	;;
	.align 16

.L100:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L110
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f67  = f0
	}
	;;
#else
	{ .mfi
	add	BOFFSET = r3, B
	mov	f66  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	shladd	AOFFSET = r3, 1, AORIG
	}
	;;
#endif
	;;
	adds	L =  1, L
	;;
	{ .mii
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  -1, L
	}
	;;
	{ .mmi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	mov	ar.lc = L
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L108
	;;
	.align 16

.L102:
	{ .mfi
	lfetch.nt1	[PREA],  8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	FMA	f80   = f32, f49, f80	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfb
	lfetch.nt1	[PREB],  4 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f81   = f33, f49, f81	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FMA	f96   = f34, f48, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f112   = f34, f49, f112	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f97   = f35, f48, f97	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f113   = f35, f49, f113	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f80   = f40, f57, f80	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f81   = f41, f57, f81	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f96   = f42, f56, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f112   = f42, f57, f112	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f97   = f43, f56, f97	// A4 * B1
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f113   = f43, f57, f113	// A4 * B2
	br.cloop.sptk.few .L102
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_A	  f64  = f64,  f81
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_B	  f65  = f65,  f80
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_A	  f96  = f96,  f113
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_B	  f97  = f97,  f112
	nop	__LINE__
	}
	;;
.L108:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	add	BOFFSET = r2, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET]
	adds	BOFFSET = -2 * SIZE, BOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	FSUB	f96  = f88, f96
	FSUB_A	f97  = f89, f97
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET]
	adds	AOFFSET = -2 * SIZE, AOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	FSUB	f96  = f88, f96
	FSUB	f97  = f89, f97
	;;
#endif

#ifdef LN
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f104, f105 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f106, f107 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f104, f96
	FMPY	f33  = f105, f96
	;;
	FMA_C	f96  = f105, f97,  f32
	FMA_D	f97  = f104, f97,  f33
	;;
	FNMA	f64  = f106, f96, f64
	FMA_A	f65  = f107, f96, f65
	;;
	FMA_B	f64  = f107, f97, f64
	FNMA	f65  = f106, f97, f65
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	;;
	FNMA	f96  = f74, f64, f96
	FMA_A	f97  = f75, f64, f97
	;;
	FMA_B	f96  = f75, f65, f96
	FNMA	f97  = f74, f65, f97
	;;
	FMPY	f32  = f90, f96
	FMPY	f33  = f91, f96
	;;
	FMA_C	f96  = f91, f97,  f32
	FMA_D	f97  = f90, f97,  f33
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f36  = f72, f96
	FMPY	f37  = f73, f96
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f96  = f73, f97,  f36
	FMA_D	f97  = f72, f97,  f37
	;;
#endif

#ifdef RT
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f36  = f72, f96
	FMPY	f37  = f73, f96
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f96  = f73, f97,  f36
	FMA_D	f97  = f72, f97,  f37
	;;
#endif

#if defined(LN) || defined(LT)
	STFD	[BOFFSET]  = f64,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	;;
	STFD	[BOFFSET]  = f96,  SIZE
	;;
	STFD	[BOFFSET]  = f97,  SIZE
	;;
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
#else
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	;;
	STFD	[AOFFSET]  = f96,  SIZE
	;;
	STFD	[AOFFSET]  = f97,  SIZE
	;;
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -4 * SIZE, C1
	adds	C5 = -4 * SIZE, C5
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	;;
	STFD	[C1 ] = f96, SIZE
	;;
	STFD	[C1 ] = f97, SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -4 * SIZE, C1
	adds	C5 = -4 * SIZE, C5
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = L, 1, AOFFSET
	add	BOFFSET = L, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	.align 16

.L110:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L119
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f67  = f0
	}
	;;
#else
	{ .mfi
	add	BOFFSET = r3, B
	mov	f66  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	add	AOFFSET = r3, AORIG
	}
	;;
#endif
	;;
	adds	L =  1, L
	;;
	{ .mii
	nop	__LINE__
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	cmp.eq	p3, p0 = r0, r0
	adds	L =  -1, L
	}
	;;
	{ .mmi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	mov	ar.lc = L
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L118
	;;
	.align 16

.L112:
	{ .mfi
	lfetch.nt1	[PREA],  4 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	lfetch.nt1	[PREB],   4 * SIZE
	FMA	f80   = f32, f49, f80	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mmf
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	(p3) LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f81   = f33, f49, f81	// A2 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f80   = f40, f57, f80	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	adds	L = -1, L
	}
	{ .mfb
	(p3) FMA	f81   = f41, f57, f81	// A2 * B2
	br.cloop.sptk.few .L112
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_A	  f64  = f64,  f81
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_B	  f65  = f65,  f80
	nop	__LINE__
	}
	;;
.L118:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	add	BOFFSET = r2, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	;;
#else
	LDFPD	f72, f73 = [AOFFSET]
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	;;
#endif

#ifdef LN
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	;;
#endif

#ifdef RT
	LDFPD	f72, f73 = [BOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	;;
#endif

#if defined(LN) || defined(LT)
	STFD	[BOFFSET]  = f64,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	;;
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
#else
	STFD	[AOFFSET]  = f64,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	;;
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -2 * SIZE, C1
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	;;
#ifdef LN
	adds	C1 = -2 * SIZE, C1
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	add	AORIG = r2, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	add	AOFFSET = L, AOFFSET
	add	BOFFSET = L, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  1, KK
#elif defined LN
	adds	KK = -1, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	.align 16

.L119:
#ifdef LN
	shladd	KK8 = K, ZBASE_SHIFT, r0
	;;
       add	B = KK8, B
#endif

#if defined(LT) || defined(RN)
	mov	B =  BOFFSET
#endif

#ifdef RN
	adds	KK =  1,  KK
#endif

#ifdef RT
	adds	KK = -1,  KK
#endif
	;;
	{ .mmi
	mov	AOFFSET = A
	nop	__LINE__
	}
	;;
	.align 16

.L050:
	{ .mmi
	shr	I  = M, 2
	}
	{ .mib
	tbit.z	p6, p0 = N, 1
	(p6)	br.cond.dpnt .L010
	}
	;;

#ifdef RT
       { .mmi
	shladd	r3 = LDC, 1, r0
	nop	__LINE__
	shl	r2 = K, 1 + ZBASE_SHIFT
	}
	;;
	{ .mmi
	sub	B = B, r2
	sub	C = C, r3
	nop	__LINE__
	}
	;;
#endif

	mov	C1 = C
	add	C2 = LDC, C
	;;
#ifdef LN
	add	KK = M, OFFSET
#elif defined LT
	mov	KK = OFFSET
#else
	nop	__LINE__
#endif
	;;
#if defined(LN) || defined(RT)
	mov	AORIG = A
#else
	mov	AOFFSET = A
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	{ .mib
	cmp.eq	p6, p7 = 0, I
#ifndef RT
	shladd	C = LDC, 1, C
#else
	nop	__LINE__
#endif
	(p6)	br.cond.dpnt .L060
	}
	;;
	.align 16

.L052:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 2 + ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f67  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 1, B
	mov	f66  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	shladd	AOFFSET = r3, 2, AORIG
	}
	;;
#endif

	{ .mfi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82  = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	(p7) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f98  = f0
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f99  = f0
	adds	L =  1, L
	}
	;;
	{ .mfi
	(p7) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	mov	f114 = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f115 = f0
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	adds C5 = 4 * SIZE, C1
	adds	L =  -1, L
	}
	;;
	{ .mmi
	CPREFETCH [PREC], LDC
	adds C6 = 4 * SIZE, C2
	mov	ar.lc = L
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L058
	;;
	.align 16

.L053:
	{ .mfb
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f65   = f32, f49, f65	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],   8 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f81   = f32, f51, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f96   = f34, f48, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfi
	FMA_B	f97   = f34, f49, f97	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],  2 * SIZE
	FMA	f112   = f34, f50, f112	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f113   = f34, f51, f113	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f64   = f33, f49, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f80   = f33, f51, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f97   = f35, f48, f97	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f96   = f35, f49, f96	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	FMA	f113   = f35, f50, f113	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f112   = f35, f51, f112	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f66   = f36, f48, f66	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f67   = f36, f49, f67	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f82   = f36, f50, f82	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f83   = f36, f51, f83	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f98   = f38, f48, f98	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f99   = f38, f49, f99	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f114   = f38, f50, f114	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f115   = f38, f51, f115	// A7 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f67   = f37, f48, f67	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f66   = f37, f49, f66	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f83   = f37, f50, f83	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f82   = f37, f51, f82	// A6 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f99   = f39, f48, f99	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f98   = f39, f49, f98	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],  2 * SIZE
	FMA	f115   = f39, f50, f115	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f114   = f39, f51, f114	// A8 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f65   = f40, f57, f65	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f81   = f40, f59, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f96   = f42, f56, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f97   = f42, f57, f97	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	(p3) FMA	f112   = f42, f58, f112	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f113   = f42, f59, f113	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f97   = f43, f56, f97	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f96   = f43, f57, f96	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f113   = f43, f58, f113	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f112   = f43, f59, f112	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f66   = f44, f56, f66	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f67   = f44, f57, f67	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f82   = f44, f58, f82	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f83   = f44, f59, f83	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f98   = f46, f56, f98	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f99   = f46, f57, f99	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f114   = f46, f58, f114	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f115   = f46, f59, f115	// A7 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f67   = f45, f56, f67	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f66   = f45, f57, f66	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f83   = f45, f58, f83	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f82   = f45, f59, f82	// A6 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f99   = f47, f56, f99	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f98   = f47, f57, f98	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f115   = f47, f58, f115	// A8 * B3
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f114   = f47, f59, f114	// A8 * B4
	br.cloop.sptk.few .L053
	}
	;;
.L058:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -4, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 2, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f104, f105 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f106, f107 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f120, f121 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f122, f123 = [BOFFSET]
	adds	BOFFSET = -14 * SIZE, BOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	FSUB	f80  = f74, f80
	FSUB_A	f81  = f75, f81
	FSUB	f96  = f88, f96
	FSUB_A	f97  = f89, f97
	FSUB	f112 = f90, f112
	FSUB_A	f113 = f91, f113

	FSUB	f66  = f104, f66
	FSUB_A	f67  = f105, f67
	FSUB	f82  = f106, f82
	FSUB_A	f83  = f107, f83
	FSUB	f98  = f120, f98
	FSUB_A	f99  = f121, f99
	FSUB	f114 = f122, f114
	FSUB_A	f115 = f123, f115
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f92, f93 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f94, f95 = [AOFFSET]
	adds	AOFFSET = -14 * SIZE, AOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	FSUB	f96  = f74, f96
	FSUB	f97  = f75, f97

	FSUB	f66  = f76, f66
	FSUB	f67  = f77, f67
	FSUB	f98  = f78, f98
	FSUB	f99  = f79, f99

	FSUB	f80  = f88, f80
	FSUB	f81  = f89, f81
	FSUB	f112 = f90, f112
	FSUB	f113 = f91, f113

	FSUB	f82  = f92, f82
	FSUB	f83  = f93, f83
	FSUB	f114 = f94, f114
	FSUB	f115 = f95, f115
	;;
#endif

#ifdef LN
	adds	AOFFSET = 30 * SIZE, AOFFSET
	;;
	LDFPD	f72, f73 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f74, f75 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f76, f77 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f78, f79 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f88, f89 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f92, f93 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFPD	f104, f105 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f106, f107 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f72, f98
	FMPY	f33  = f73, f98
	FMPY	f34  = f72, f114
	FMPY	f35  = f73, f114
	;;
	FMA_C	f98  = f73, f99,  f32
	FMA_D	f99  = f72, f99,  f33
	FMA_C	f114 = f73, f115, f34
	FMA_D	f115 = f72, f115, f35
	;;
	FNMA	f66  = f74, f98, f66
	FMA_A	f67  = f75, f98, f67
	FNMA	f82  = f74, f114, f82
	FMA_A	f83  = f75, f114, f83
	;;
	FMA_B	f66  = f75, f99, f66
	FNMA	f67  = f74, f99, f67
	FMA_B	f82  = f75, f115, f82
	FNMA	f83  = f74, f115, f83
	;;
	FNMA	f96  = f76, f98, f96
	FMA_A	f97  = f77, f98, f97
	FNMA	f112 = f76, f114, f112
	FMA_A	f113 = f77, f114, f113
	;;
	FMA_B	f96  = f77, f99, f96
	FNMA	f97  = f76, f99, f97
	FMA_B	f112 = f77, f115, f112
	FNMA	f113 = f76, f115, f113
	;;
	FNMA	f64  = f78, f98, f64
	FMA_A	f65  = f79, f98, f65
	FNMA	f80  = f78, f114, f80
	FMA_A	f81  = f79, f114, f81
	;;
	FMA_B	f64  = f79, f99, f64
	FNMA	f65  = f78, f99, f65
	FMA_B	f80  = f79, f115, f80
	FNMA	f81  = f78, f115, f81
	;;
	FMPY	f32  = f88, f66
	FMPY	f33  = f89, f66
	FMPY	f34  = f88, f82
	FMPY	f35  = f89, f82
	;;
	FMA_C	f66  = f89, f67,  f32
	FMA_D	f67  = f88, f67,  f33
	FMA_C	f82  = f89, f83,  f34
	FMA_D	f83  = f88, f83,  f35
	;;
	FNMA	f96  = f90, f66, f96
	FMA_A	f97  = f91, f66, f97
	FNMA	f112 = f90, f82, f112
	FMA_A	f113 = f91, f82, f113
	;;
	FMA_B	f96  = f91, f67, f96
	FNMA	f97  = f90, f67, f97
	FMA_B	f112 = f91, f83, f112
	FNMA	f113 = f90, f83, f113
	;;
	FNMA	f64  = f92, f66, f64
	FMA_A	f65  = f93, f66, f65
	FNMA	f80  = f92, f82, f80
	FMA_A	f81  = f93, f82, f81
	;;
	FMA_B	f64  = f93, f67, f64
	FNMA	f65  = f92, f67, f65
	FMA_B	f80  = f93, f83, f80
	FNMA	f81  = f92, f83, f81
	;;
	FMPY	f32  = f104, f96
	FMPY	f33  = f105, f96
	FMPY	f34  = f104, f112
	FMPY	f35  = f105, f112
	;;
	FMA_C	f96  = f105, f97,  f32
	FMA_D	f97  = f104, f97,  f33
	FMA_C	f112 = f105, f113, f34
	FMA_D	f113 = f104, f113, f35
	;;
	FNMA	f64  = f106, f96,  f64
	FMA_A	f65  = f107, f96,  f65
	FNMA	f80  = f106, f112, f80
	FMA_A	f81  = f107, f112, f81
	;;
	FMA_B	f64  = f107, f97,  f64
	FNMA	f65  = f106, f97,  f65
	FMA_B	f80  = f107, f113, f80
	FNMA	f81  = f106, f113, f81
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f80
	FMPY	f35  = f121, f80
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	FMA_C	f80  = f121, f81,  f34
	FMA_D	f81  = f120, f81,  f35
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f92, f93 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f94, f95 = [AOFFSET]
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f108, f109 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f110, f111 = [AOFFSET]
	adds	AOFFSET = 8 * SIZE, AOFFSET
	;;
	LDFPD	f126, f127 = [AOFFSET]
	adds	AOFFSET = - 30 * SIZE, AOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f80
	FMPY	f35  = f73, f80
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f80  = f73, f81,  f34
	FMA_D	f81  = f72, f81,  f35
	;;
	FNMA	f96  = f74, f64, f96
	FMA_A	f97  = f75, f64, f97
	FNMA	f112 = f74, f80, f112
	FMA_A	f113 = f75, f80, f113
	;;
	FMA_B	f96  = f75, f65, f96
	FNMA	f97  = f74, f65, f97
	FMA_B	f112 = f75, f81, f112
	FNMA	f113 = f74, f81, f113
	;;
	FNMA	f66  = f76, f64, f66
	FMA_A	f67  = f77, f64, f67
	FNMA	f82  = f76, f80, f82
	FMA_A	f83  = f77, f80, f83
	;;
	FMA_B	f66  = f77, f65, f66
	FNMA	f67  = f76, f65, f67
	FMA_B	f82  = f77, f81, f82
	FNMA	f83  = f76, f81, f83
	;;
	FNMA	f98  = f78, f64, f98
	FMA_A	f99  = f79, f64, f99
	FNMA	f114 = f78, f80, f114
	FMA_A	f115 = f79, f80, f115
	;;
	FMA_B	f98  = f79, f65, f98
	FNMA	f99  = f78, f65, f99
	FMA_B	f114 = f79, f81, f114
	FNMA	f115 = f78, f81, f115
	;;
	FMPY	f32  = f90, f96
	FMPY	f33  = f91, f96
	FMPY	f34  = f90, f112
	FMPY	f35  = f91, f112
	;;
	FMA_C	f96  = f91, f97,  f32
	FMA_D	f97  = f90, f97,  f33
	FMA_C	f112 = f91, f113, f34
	FMA_D	f113 = f90, f113, f35
	;;
	FNMA	f66  = f92, f96, f66
	FMA_A	f67  = f93, f96, f67
	FNMA	f82  = f92, f112, f82
	FMA_A	f83  = f93, f112, f83
	;;
	FMA_B	f66  = f93, f97, f66
	FNMA	f67  = f92, f97, f67
	FMA_B	f82  = f93, f113, f82
	FNMA	f83  = f92, f113, f83
	;;
	FNMA	f98  = f94, f96, f98
	FMA_A	f99  = f95, f96, f99
	FNMA	f114  = f94, f112, f114
	FMA_A	f115  = f95, f112, f115
	;;
	FMA_B	f98  = f95, f97, f98
	FNMA	f99  = f94, f97, f99
	FMA_B	f114  = f95, f113, f114
	FNMA	f115  = f94, f113, f115
	;;
	FMPY	f32  = f108, f66
	FMPY	f33  = f109, f66
	FMPY	f34  = f108, f82
	FMPY	f35  = f109, f82
	;;
	FMA_C	f66  = f109, f67,  f32
	FMA_D	f67  = f108, f67,  f33
	FMA_C	f82  = f109, f83,  f34
	FMA_D	f83  = f108, f83,  f35
	;;
	FNMA	f98  = f110, f66, f98
	FMA_A	f99  = f111, f66, f99
	FNMA	f114  = f110, f82, f114
	FMA_A	f115  = f111, f82, f115
	;;
	FMA_B	f98  = f111, f67, f98
	FNMA	f99  = f110, f67, f99
	FMA_B	f114  = f111, f83, f114
	FNMA	f115  = f110, f83, f115
	;;
	FMPY	f32  = f126, f98
	FMPY	f33  = f127, f98
	FMPY	f34  = f126, f114
	FMPY	f35  = f127, f114
	;;
	FMA_C	f98  = f127, f99,  f32
	FMA_D	f99  = f126, f99,  f33
	FMA_C	f114  = f127, f115,  f34
	FMA_D	f115  = f126, f115,  f35
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f96
	FMPY	f35  = f73, f96
	FMPY	f36  = f72, f66
	FMPY	f37  = f73, f66
	FMPY	f38  = f72, f98
	FMPY	f39  = f73, f98
	;;
	FMA_C	f64  = f73, f65, f32
	FMA_D	f65  = f72, f65, f33
	FMA_C	f96  = f73, f97, f34
	FMA_D	f97  = f72, f97, f35
	FMA_C	f66  = f73, f67, f36
	FMA_D	f67  = f72, f67, f37
	FMA_C	f98  = f73, f99, f38
	FMA_D	f99  = f72, f99, f39
	;;
	FNMA	f80  = f74, f64, f80
	FMA_A	f81  = f75, f64, f81
	FNMA	f112 = f74, f96, f112
	FMA_A	f113 = f75, f96, f113
	FNMA	f82  = f74, f66, f82
	FMA_A	f83  = f75, f66, f83
	FNMA	f114 = f74, f98, f114
	FMA_A	f115 = f75, f98, f115
	;;
	FMA_B	f80  = f75, f65, f80
	FNMA	f81  = f74, f65, f81
	FMA_B	f112 = f75, f97, f112
	FNMA	f113 = f74, f97, f113
	FMA_B	f82  = f75, f67, f82
	FNMA	f83  = f74, f67, f83
	FMA_B	f114 = f75, f99, f114
	FNMA	f115 = f74, f99, f115
	;;
	FMPY	f32  = f90, f80
	FMPY	f33  = f91, f80
	FMPY	f34  = f90, f112
	FMPY	f35  = f91, f112
	FMPY	f36  = f90, f82
	FMPY	f37  = f91, f82
	FMPY	f38  = f90, f114
	FMPY	f39  = f91, f114
	;;
	FMA_C	f80  = f91, f81,  f32
	FMA_D	f81  = f90, f81,  f33
	FMA_C	f112 = f91, f113, f34
	FMA_D	f113 = f90, f113, f35
	FMA_C	f82  = f91, f83,  f36
	FMA_D	f83  = f90, f83,  f37
	FMA_C	f114 = f91, f115, f38
	FMA_D	f115 = f90, f115, f39
	;;
#endif

#ifdef RT
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f104, f105 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f106, f107 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f120, f121 = [BOFFSET]
	;;
	FMPY	f32  = f104, f80
	FMPY	f33  = f105, f80
	FMPY	f34  = f104, f112
	FMPY	f35  = f105, f112
	FMPY	f36  = f104, f82
	FMPY	f37  = f105, f82
	FMPY	f38  = f104, f114
	FMPY	f39  = f105, f114
	;;
	FMA_C	f80  = f105, f81,  f32
	FMA_D	f81  = f104, f81,  f33
	FMA_C	f112 = f105, f113, f34
	FMA_D	f113 = f104, f113, f35
	FMA_C	f82  = f105, f83,  f36
	FMA_D	f83  = f104, f83,  f37
	FMA_C	f114 = f105, f115, f38
	FMA_D	f115 = f104, f115, f39
	;;
	FNMA	f64  = f106, f80,  f64
	FMA_A	f65  = f107, f80,  f65
	FNMA	f96  = f106, f112, f96
	FMA_A	f97  = f107, f112, f97
	FNMA	f66  = f106, f82,  f66
	FMA_A	f67  = f107, f82,  f67
	FNMA	f98  = f106, f114, f98
	FMA_A	f99  = f107, f114, f99
	;;
	FMA_B	f64  = f107, f81,  f64
	FNMA	f65  = f106, f81,  f65
	FMA_B	f96  = f107, f113, f96
	FNMA	f97  = f106, f113, f97
	FMA_B	f66  = f107, f83,  f66
	FNMA	f67  = f106, f83,  f67
	FMA_B	f98  = f107, f115, f98
	FNMA	f99  = f106, f115, f99
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f96
	FMPY	f35  = f121, f96
	FMPY	f36  = f120, f66
	FMPY	f37  = f121, f66
	FMPY	f38  = f120, f98
	FMPY	f39  = f121, f98
	;;
	FMA_C	f64  = f121, f65, f32
	FMA_D	f65  = f120, f65, f33
	FMA_C	f96  = f121, f97, f34
	FMA_D	f97  = f120, f97, f35
	FMA_C	f66  = f121, f67, f36
	FMA_D	f67  = f120, f67, f37
	FMA_C	f98  = f121, f99, f38
	FMA_D	f99  = f120, f99, f39
	;;
#endif

#if defined(LN) || defined(LT)
	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;
	STFD	[BOFFSET]  = f64,  SIZE
	STFD	[BOFFSET2] = f96,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	STFD	[BOFFSET2] = f97,  SIZE
	;;
	STFD	[BOFFSET]  = f80,  SIZE
	STFD	[BOFFSET2] = f112, SIZE
	;;
	STFD	[BOFFSET]  = f81,  5 * SIZE
	STFD	[BOFFSET2] = f113, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f66,  SIZE
	STFD	[BOFFSET2] = f98,  SIZE
	;;
	STFD	[BOFFSET]  = f67,  SIZE
	STFD	[BOFFSET2] = f99,  SIZE
	;;
	STFD	[BOFFSET]  = f82,  SIZE
	STFD	[BOFFSET2] = f114, SIZE
	;;
	STFD	[BOFFSET]  = f83,  5 * SIZE
	STFD	[BOFFSET2] = f115, 5 * SIZE
	;;
	adds	BOFFSET = - 16 * SIZE, BOFFSET
	;;
#else
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	STFD	[AOFFSET2] = f66,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	STFD	[AOFFSET2] = f67,  SIZE
	;;
	STFD	[AOFFSET]  = f96,  SIZE
	STFD	[AOFFSET2] = f98,  SIZE
	;;
	STFD	[AOFFSET]  = f97,  5 * SIZE
	STFD	[AOFFSET2] = f99,  5 * SIZE
	;;
	STFD	[AOFFSET]  = f80,  SIZE
	STFD	[AOFFSET2] = f82,  SIZE
	;;
	STFD	[AOFFSET]  = f81,  SIZE
	STFD	[AOFFSET2] = f83,  SIZE
	;;
	STFD	[AOFFSET]  = f112, SIZE
	STFD	[AOFFSET2] = f114, SIZE
	;;
	STFD	[AOFFSET]  = f113, 5 * SIZE
	STFD	[AOFFSET2] = f115, 5 * SIZE
	;;
	adds	AOFFSET = - 16 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -8 * SIZE, C1
	adds	C2 = -8 * SIZE, C2
	adds	C5 = -8 * SIZE, C5
	adds	C6 = -8 * SIZE, C6
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	STFD	[C5 ] = f66, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	STFD	[C5 ] = f67, SIZE
	;;
	STFD	[C1 ] = f96, SIZE
	STFD	[C5 ] = f98, SIZE
	;;
	STFD	[C1 ] = f97, 5 * SIZE
	STFD	[C5 ] = f99, 5 * SIZE
	;;
	STFD	[C2 ] = f80, SIZE
	STFD	[C6 ] = f82, SIZE
	;;
	STFD	[C2 ] = f81, SIZE
	STFD	[C6 ] = f83, SIZE
	;;
	STFD	[C2 ] = f112, SIZE
	STFD	[C6 ] = f114, SIZE
	;;
	STFD	[C2 ] = f113, 5 * SIZE
	STFD	[C6 ] = f115, 5 * SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -8 * SIZE, C1
	adds	C2 = -8 * SIZE, C2
	adds	C5 = -8 * SIZE, C5
	adds	C6 = -8 * SIZE, C6
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 2, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = L, 2, AOFFSET
	shladd	BOFFSET = L, 1, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  4, KK
#elif defined LN
	adds	KK = -4, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	(p6)	br.cond.dptk .L052
	;;
	.align 16

.L060:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L070
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 1, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	shladd	AOFFSET = r3, 1, AORIG
	}
	;;
#endif
	;;
	adds	L =  1, L
	;;
	{ .mmi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	tbit.z	p12, p0 = L, 0
	}
	{ .mmi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  -1, L
	}
	;;
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	mov	ar.lc = L
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L068
	;;
	.align 16

.L062:
	{ .mfi
	lfetch.nt1	[PREA],  8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f65   = f32, f49, f65	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfb
	lfetch.nt1	[PREB],   8 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f81   = f32, f51, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f96   = f34, f48, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f97   = f34, f49, f97	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f112   = f34, f50, f112	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f113   = f34, f51, f113	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f64   = f33, f49, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f80   = f33, f51, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f97   = f35, f48, f97	// A4 * B1
	}
	{ .mfb
	FMA_A	f96   = f35, f49, f96	// A4 * B2
	nop	__LINE__
	}

	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f113   = f35, f50, f113	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	FMA_A	f112   = f35, f51, f112	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA_B	f65   = f40, f57, f65	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA_B	f81   = f40, f59, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f96   = f42, f56, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f97   = f42, f57, f97	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f112   = f42, f58, f112	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f113   = f42, f59, f113	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f97   = f43, f56, f97	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f96   = f43, f57, f96	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f113   = f43, f58, f113	// A4 * B3
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f112   = f43, f59, f112	// A4 * B4
	br.cloop.sptk.few .L062
	}
	;;
.L068:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	FSUB	f80  = f74, f80
	FSUB_A	f81  = f75, f81
	FSUB	f96  = f88, f96
	FSUB_A	f97  = f89, f97
	FSUB	f112 = f90, f112
	FSUB_A	f113 = f91, f113
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	FSUB	f96  = f74, f96
	FSUB	f97  = f75, f97

	FSUB	f80  = f88, f80
	FSUB	f81  = f89, f81
	FSUB	f112 = f90, f112
	FSUB	f113 = f91, f113
	;;
#endif

#ifdef LN
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f104, f105 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f106, f107 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f104, f96
	FMPY	f33  = f105, f96
	FMPY	f34  = f104, f112
	FMPY	f35  = f105, f112
	;;
	FMA_C	f96  = f105, f97,  f32
	FMA_D	f97  = f104, f97,  f33
	FMA_C	f112 = f105, f113, f34
	FMA_D	f113 = f104, f113, f35
	;;
	FNMA	f64  = f106, f96,  f64
	FMA_A	f65  = f107, f96,  f65
	FNMA	f80  = f106, f112, f80
	FMA_A	f81  = f107, f112, f81
	;;
	FMA_B	f64  = f107, f97,  f64
	FNMA	f65  = f106, f97,  f65
	FMA_B	f80  = f107, f113, f80
	FNMA	f81  = f106, f113, f81
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f80
	FMPY	f35  = f121, f80
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	FMA_C	f80  = f121, f81,  f34
	FMA_D	f81  = f120, f81,  f35
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f80
	FMPY	f35  = f73, f80
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f80  = f73, f81,  f34
	FMA_D	f81  = f72, f81,  f35
	;;
	FNMA	f96  = f74, f64, f96
	FMA_A	f97  = f75, f64, f97
	FNMA	f112 = f74, f80, f112
	FMA_A	f113 = f75, f80, f113
	;;
	FMA_B	f96  = f75, f65, f96
	FNMA	f97  = f74, f65, f97
	FMA_B	f112 = f75, f81, f112
	FNMA	f113 = f74, f81, f113
	;;
	FMPY	f32  = f90, f96
	FMPY	f33  = f91, f96
	FMPY	f34  = f90, f112
	FMPY	f35  = f91, f112
	;;
	FMA_C	f96  = f91, f97,  f32
	FMA_D	f97  = f90, f97,  f33
	FMA_C	f112 = f91, f113, f34
	FMA_D	f113 = f90, f113, f35
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f96
	FMPY	f35  = f73, f96
	;;
	FMA_C	f64  = f73, f65, f32
	FMA_D	f65  = f72, f65, f33
	FMA_C	f96  = f73, f97, f34
	FMA_D	f97  = f72, f97, f35
	;;
	FNMA	f80  = f74, f64, f80
	FMA_A	f81  = f75, f64, f81
	FNMA	f112 = f74, f96, f112
	FMA_A	f113 = f75, f96, f113
	;;
	FMA_B	f80  = f75, f65, f80
	FNMA	f81  = f74, f65, f81
	FMA_B	f112 = f75, f97, f112
	FNMA	f113 = f74, f97, f113

	;;
	FMPY	f32  = f90, f80
	FMPY	f33  = f91, f80
	FMPY	f34  = f90, f112
	FMPY	f35  = f91, f112
	;;
	FMA_C	f80  = f91, f81,  f32
	FMA_D	f81  = f90, f81,  f33
	FMA_C	f112 = f91, f113, f34
	FMA_D	f113 = f90, f113, f35
	;;
#endif

#ifdef RT
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f104, f105 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f106, f107 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f120, f121 = [BOFFSET]
	;;
	FMPY	f32  = f104, f80
	FMPY	f33  = f105, f80
	FMPY	f34  = f104, f112
	FMPY	f35  = f105, f112
	;;
	FMA_C	f80  = f105, f81,  f32
	FMA_D	f81  = f104, f81,  f33
	FMA_C	f112 = f105, f113, f34
	FMA_D	f113 = f104, f113, f35
	;;
	FNMA	f64  = f106, f80,  f64
	FMA_A	f65  = f107, f80,  f65
	FNMA	f96  = f106, f112, f96
	FMA_A	f97  = f107, f112, f97
	;;
	FMA_B	f64  = f107, f81,  f64
	FNMA	f65  = f106, f81,  f65
	FMA_B	f96  = f107, f113, f96
	FNMA	f97  = f106, f113, f97
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f96
	FMPY	f35  = f121, f96
	;;
	FMA_C	f64  = f121, f65, f32
	FMA_D	f65  = f120, f65, f33
	FMA_C	f96  = f121, f97, f34
	FMA_D	f97  = f120, f97, f35
	;;
#endif

#if defined(LN) || defined(LT)
	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;
	STFD	[BOFFSET]  = f64,  SIZE
	STFD	[BOFFSET2] = f96,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	STFD	[BOFFSET2] = f97,  SIZE
	;;
	STFD	[BOFFSET]  = f80,  SIZE
	STFD	[BOFFSET2] = f112, SIZE
	;;
	STFD	[BOFFSET]  = f81,  5 * SIZE
	STFD	[BOFFSET2] = f113, 5 * SIZE
	;;
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
#else
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	STFD	[AOFFSET2] = f80,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	STFD	[AOFFSET2] = f81,  SIZE
	;;
	STFD	[AOFFSET]  = f96,  SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET]  = f97,  5 * SIZE
	STFD	[AOFFSET2] = f113, 5 * SIZE
	;;
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -4 * SIZE, C1
	adds	C2 = -4 * SIZE, C2
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	;;
	STFD	[C1 ] = f96, SIZE
	;;
	STFD	[C1 ] = f97, SIZE
	;;
	STFD	[C2 ] = f80, SIZE
	;;
	STFD	[C2 ] = f81, SIZE
	;;
	STFD	[C2 ] = f112, SIZE
	;;
	STFD	[C2 ] = f113, SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -4 * SIZE, C1
	adds	C2 = -4 * SIZE, C2
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = L, 1, AOFFSET
	shladd	BOFFSET = L, 1, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	.align 16

.L070:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L089
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 1, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	add	AOFFSET = r3, AORIG
	}
	;;
#endif
	;;
	adds	L =  1, L
	;;
	{ .mii
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	adds	L =  -1, L
	}
	;;
	{ .mmi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	cmp.eq	p3, p0 = r0, r0
	mov	ar.lc = L
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L078
	;;
	.align 16

.L072:
	{ .mfb
	lfetch.nt1	[PREA],  4 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f96   = f32, f49, f96	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],   8 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfb
	nop	__LINE__
	FMA	f112   = f32, f51, f112	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	}
	{ .mfi
	nop	__LINE__
	FMA	f97   = f33, f49, f97	// A2 * B2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f113   = f33, f51, f113	// A2 * B4
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p3) FMA	f96   = f40, f57, f96	// A1 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p3) FMA	f112   = f40, f59, f112	// A1 * B4
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f97   = f41, f57, f97	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f113   = f41, f59, f113	// A2 * B4
	br.cloop.sptk.few .L072
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_A	  f64  = f64,  f97
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_A	  f80  = f80,  f113
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_B	  f65  = f65,  f96
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_B	  f81  = f81,  f112
	nop	__LINE__
	}
	;;
.L078:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET]
	adds	BOFFSET = -2 * SIZE, BOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	FSUB	f80  = f74, f80
	FSUB_A	f81  = f75, f81
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET]
	adds	AOFFSET = -2 * SIZE, AOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	FSUB	f80  = f88, f80
	FSUB	f81  = f89, f81
	;;
#endif

#ifdef LN
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f80
	FMPY	f35  = f121, f80
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	FMA_C	f80  = f121, f81,  f34
	FMA_D	f81  = f120, f81,  f35
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET]
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f80
	FMPY	f35  = f73, f80
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f80  = f73, f81,  f34
	FMA_D	f81  = f72, f81,  f35
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65, f32
	FMA_D	f65  = f72, f65, f33
	;;
	FNMA	f80  = f74, f64, f80
	FMA_A	f81  = f75, f64, f81
	;;
	FMA_B	f80  = f75, f65, f80
	FNMA	f81  = f74, f65, f81

	;;
	FMPY	f32  = f90, f80
	FMPY	f33  = f91, f80
	;;
	FMA_C	f80  = f91, f81,  f32
	FMA_D	f81  = f90, f81,  f33
	;;
#endif

#ifdef RT
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f104, f105 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f106, f107 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f120, f121 = [BOFFSET]
	;;
	FMPY	f32  = f104, f80
	FMPY	f33  = f105, f80
	;;
	FMA_C	f80  = f105, f81,  f32
	FMA_D	f81  = f104, f81,  f33
	;;
	FNMA	f64  = f106, f80,  f64
	FMA_A	f65  = f107, f80,  f65
	;;
	FMA_B	f64  = f107, f81,  f64
	FNMA	f65  = f106, f81,  f65
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	;;
	FMA_C	f64  = f121, f65, f32
	FMA_D	f65  = f120, f65, f33
	;;
#endif

#if defined(LN) || defined(LT)
	STFD	[BOFFSET]  = f64,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	;;
	STFD	[BOFFSET]  = f80,  SIZE
	;;
	STFD	[BOFFSET]  = f81,  SIZE
	;;
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
#else
	STFD	[AOFFSET]  = f64,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	;;
	STFD	[AOFFSET]  = f80,  SIZE
	;;
	STFD	[AOFFSET]  = f81,  SIZE
	;;
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -2 * SIZE, C1
	adds	C2 = -2 * SIZE, C2
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	;;
	STFD	[C2 ] = f80, SIZE
	;;
	STFD	[C2 ] = f81, SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -2 * SIZE, C1
	adds	C2 = -2 * SIZE, C2
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	add	AORIG = r2, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	add	AOFFSET = L, AOFFSET
	shladd	BOFFSET = L, 1, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  1, KK
#elif defined LN
	adds	KK = -1, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	.align 16

.L089:
#ifdef LN
	shladd	KK8 = K, ZBASE_SHIFT, r0
	;;
       shladd	B = KK8, 1, B
#endif

#if defined(LT) || defined(RN)
	mov	B =  BOFFSET
#endif

#ifdef RN
	adds	KK =  2,  KK
#endif

#ifdef RT
	adds	KK = -2,  KK
#endif
	;;
	{ .mmi
	mov	AOFFSET = A
	nop	__LINE__
	}
	;;
	.align 16

.L010:
	shr	J = N, 2
	;;
	cmp.ge	p6, p0  = 0, J
	(p6)	br.cond.dpnt .L999
	;;

.L010x:
#ifdef RT
       { .mmi
	shladd	r3 = LDC, 2, r0
	nop	__LINE__
	shl	r2 = K, 2 + ZBASE_SHIFT
	}
	;;
	{ .mmi
	sub	B = B, r2
	sub	C = C, r3
	nop	__LINE__
	}
	;;
#endif
	{ .mmi
	mov	C1 = C			// coffset1 = c + 0 * ldc
	add	C2 = LDC, C		// coffset2 = c + 1 * ldc
	shr	I  = M, 2
	}
	{ .mmi
	adds	J = -1, J
#ifdef LN
	add	KK = M, OFFSET
#elif defined LT
	mov	KK = OFFSET
#else
	nop	__LINE__
#endif
#if defined(LN) || defined(RT)
	mov	AORIG = A
#else
	mov	AOFFSET = A
#endif
	}
	;;
	;;
	{ .mmi
	shladd	C3 = LDC, 1, C		// coffset3 = c + 2 * ldc
	shladd	C4 = LDC, 1, C2		// coffset4 = c + 3 * ldc
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	{ .mib
	cmp.eq	p6, p7 = 0, I
#ifndef RT
	shladd	C = LDC, 2, C		// coffset += 8 * ldc
#else
	nop	__LINE__
#endif
	(p6)	br.cond.dpnt .L020
	}
	;;
	.align 16

.L011:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 2 + ZBASE_SHIFT
	}
	{ .mfi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	mov	f118 = f0
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f67  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
	mov	f66  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	shladd	AOFFSET = r3, 2, AORIG
	}
	;;
#endif
	;;
	{ .mfi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82  = f0
	nop	__LINE__
	}
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83  = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	;;
	{ .mfi
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	mov	f98  = f0
	adds	L =  1, L
	}
	{ .mfi
	(p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	mov	f99  = f0
	adds	C5 = 4 * SIZE, C1
	}
	;;
	{ .mfi
	(p7) LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	mov	f114 = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	(p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f115 = f0
	adds	C6 = 4 * SIZE, C2
	}
	;;
	{ .mfi
	(p7) LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
	mov	f68  = f0
	shr	L = L, 1
	}
	{ .mfi
	setf.d	f86  = r0
	mov	f69  = f0
	adds	C7 = 4 * SIZE, C3
	}
	;;
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f84  = f0
	adds	L =  -1, L
	}
	{ .mfi
	setf.d	f87  = r0
	mov	f85  = f0
	adds	C8 = 4 * SIZE, C4
	}
	;;
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f100 = f0
	mov	ar.lc = L
	}
	{ .mfi
	setf.d	f102 = r0
	mov	f101 = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f116 = f0
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	}
	{ .mfi
	setf.d	f103 = r0
	mov	f117 = f0
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	;;
	{ .mfi
	CPREFETCH [PREC]
	mov	f70  = f0
	cmp.eq  p6, p0 = -1, L
	}
	{ .mfb
	setf.d	f119 = r0
 	mov	f71  = f0
	(p6) br.cond.dpnt   .L018
	}
	;;
	.align 16

.L012:
/*  1 */
	{ .mfi
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p12) cmp.ne p3, p0 =  0, L
	FMA_B	f65   = f32, f49, f65	// A1 * B2
	nop	__LINE__
	}
	;;
/*  2 */
	{ .mfi
	lfetch.nt1	[PREB],  16 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	cmp.ne	p4, p5 =  0, L
	FMA_B	f81   = f32, f51, f81	// A1 * B4
	nop	__LINE__
	}
	;;
/*  3 */
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	FMA_B	f97  = f32, f53, f97	// A1 * B6
	nop	__LINE__
	}
	;;
/*  4 */
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	FMA_B	f113  = f32, f55, f113	// A1 * B8
	nop	__LINE__
	}
	;;
/*  5 */
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	FMA_A	f64   = f33, f49, f64	// A2 * B2
	nop	__LINE__
	}
	;;
/*  6 */
	{ .mfb
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	FMA_A	f80   = f33, f51, f80	// A2 * B4
	nop	__LINE__
	}
	;;
/*  7 */
	{ .mfb
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	FMA	f97   = f33, f52, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	FMA_A	f96  = f33, f53, f96	// A2 * B6
	nop	__LINE__
	}
	;;
/*  8 */
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f113  = f33, f54, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	FMA_A	f112  = f33, f55, f112	// A2 * B8
	nop	__LINE__
	}
	;;
/*  9 */
	{ .mfb
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	FMA_B	f67   = f34, f49, f67	// A3 * B2
	nop	__LINE__
	}
	;;
/* 10 */
	{ .mfb
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	FMA	f82   = f34, f50, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	FMA_B	f83   = f34, f51, f83	// A3 * B4
	nop	__LINE__
	}
	;;
/* 11 */
	{ .mfb
	FMA	f98   = f34, f52, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f99  = f34, f53, f99	// A3 * B6
	nop	__LINE__
	}
	;;
/* 12 */
	{ .mfb
	FMA	f114  = f34, f54, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f115  = f34, f55, f115	// A3 * B8
	nop	__LINE__
	}
	;;
/* 13 */
	{ .mfb
	nop	__LINE__
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f66   = f35, f49, f66	// A4 * B2
	nop	__LINE__
	}
	;;
/* 14 */
	{ .mfb
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f82   = f35, f51, f82	// A4 * B4
	nop	__LINE__
	}
	;;
/* 15 */
	{ .mfb
	FMA	f99   = f35, f52, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f98  = f35, f53, f98	// A4 * B6
	nop	__LINE__
	}
	;;
/* 16 */
	{ .mfb
	FMA	f115  = f35, f54, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f114  = f35, f55, f114	// A4 * B8
	nop	__LINE__
	}
	;;
/* 17 */
	{ .mfb
	nop	__LINE__
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f69   = f36, f49, f69	// A5 * B2
	nop	__LINE__
	}
	;;
/* 18 */
	{ .mfb
	nop	__LINE__
	FMA	f84   = f36, f50, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f85   = f36, f51, f85	// A5 * B4
	nop	__LINE__
	}
	;;
/* 19 */
	{ .mfb
	nop	__LINE__
	FMA	f100  = f36, f52, f100	// A5 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f101  = f36, f53, f101	// A5 * B6
	nop	__LINE__
	}
	;;
/* 20 */
	{ .mfb
	nop	__LINE__
	FMA	f116  = f36, f54, f116	// A5 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f117  = f36, f55, f117	// A5 * B8
	nop	__LINE__
	}
	;;
/* 21 */
	{ .mfb
	nop	__LINE__
	FMA	f69   = f37, f48, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f68   = f37, f49, f68	// A6 * B2
	nop	__LINE__
	}
	;;
/* 22 */
	{ .mfb
	nop	__LINE__
	FMA	f85   = f37, f50, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f84   = f37, f51, f84	// A6 * B4
	nop	__LINE__
	}
	;;
/* 23 */
	{ .mfb
	nop	__LINE__
	FMA	f101  = f37, f52, f101	// A6 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f100  = f37, f53, f100	// A6 * B6
	nop	__LINE__
	}
	;;
/* 24 */
	{ .mfb
	nop	__LINE__
	FMA	f117  = f37, f54, f117	// A6 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f116  = f37, f55, f116	// A6 * B8
	nop	__LINE__
	}
	;;
/* 25 */
	{ .mfb
	nop	__LINE__
	FMA	f70   = f38, f48, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f71   = f38, f49, f71	// A7 * B2
	nop	__LINE__
	}
	;;
/* 26 */
	{ .mfb
	nop	__LINE__
	FMA	f86   = f38, f50, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f87   = f38, f51, f87	// A7 * B4
	nop	__LINE__
	}
	;;
/* 27 */
	{ .mfb
	nop	__LINE__
	FMA	f102  = f38, f52, f102	// A7 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f103  = f38, f53, f103	// A7 * B6
	nop	__LINE__
	}
	;;
/* 28 */
	{ .mfb
	nop	__LINE__
	FMA	f118  = f38, f54, f118	// A7 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f119  = f38, f55, f119	// A7 * B8
	nop	__LINE__
	}
	;;
/* 29 */
	{ .mfb
	nop	__LINE__
	FMA	f71   = f39, f48, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f70   = f39, f49, f70	// A8 * B2
	nop	__LINE__
	}
	;;
/* 30 */
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f87   = f39, f50, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f86   = f39, f51, f86	// A8 * B4
	nop	__LINE__
	}
	;;
/* 31 */
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f103  = f39, f52, f103	// A8 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f102  = f39, f53, f102	// A8 * B6
	nop	__LINE__
	}
	;;
/* 32 */
	{ .mfb
	nop	__LINE__
	FMA	f119  = f39, f54, f119	// A8 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f118  = f39, f55, f118	// A8 * B8
	nop	__LINE__
	}
	;;
/* 33 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f65   = f40, f57, f65	// A1 * B2
	nop	__LINE__
	}
	;;
/* 34 */
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f81   = f40, f59, f81	// A1 * B4
	nop	__LINE__
	}
	;;
/* 35 */
	{ .mfb
	(p4) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f97  = f40, f61, f97	// A1 * B6
	nop	__LINE__
	}
	;;
/* 36 */
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f113  = f40, f63, f113	// A1 * B8
	nop	__LINE__
	}
	;;
/* 37 */
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
/* 38 */
	{ .mfb
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
/* 39 */
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f96  = f41, f61, f96	// A2 * B6
	nop	__LINE__
	}
	;;
/* 40 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f112  = f41, f63, f112	// A2 * B8
	nop	__LINE__
	}
	;;
 /* 41 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f67   = f42, f57, f67	// A3 * B2
	nop	__LINE__
	}
	;;
/* 42 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f83   = f42, f59, f83	// A3 * B4
	nop	__LINE__
	}
	;;
/* 43 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f99  = f42, f61, f99	// A3 * B6
	nop	__LINE__
	}
	;;
/* 44 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f115  = f42, f63, f115	// A3 * B8
	nop	__LINE__
	}
	;;
/* 45 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f66   = f43, f57, f66	// A4 * B2
	nop	__LINE__
	}
	;;
/* 46 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f82   = f43, f59, f82	// A4 * B4
	nop	__LINE__
	}
	;;
/* 47 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f98  = f43, f61, f98	// A4 * B6
	nop	__LINE__
	}
	;;
/* 48 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f114  = f43, f63, f114	// A4 * B8
	nop	__LINE__
	}
	;;
/* 49 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f69   = f44, f57, f69	// A5 * B2
	nop	__LINE__
	}
	;;
/* 50 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f85   = f44, f59, f85	// A5 * B4
	nop	__LINE__
	}
	;;
/* 51 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f100  = f44, f60, f100	// A5 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f101  = f44, f61, f101	// A5 * B6
	nop	__LINE__
	}
	;;
/* 52 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f116  = f44, f62, f116	// A5 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f117  = f44, f63, f117	// A5 * B8
	nop	__LINE__
	}
	;;
/* 53 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f68   = f45, f57, f68	// A6 * B2
	nop	__LINE__
	}
	;;
/* 54 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f84   = f45, f59, f84	// A6 * B4
	nop	__LINE__
	}
	;;
/* 55 */
	{ .mfb
	nop	__LINE__
 	(p3) FMA	f101  = f45, f60, f101	// A6 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f100  = f45, f61, f100	// A6 * B6
	nop	__LINE__
	}
	;;
/* 56 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f117  = f45, f62, f117	// A6 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f116  = f45, f63, f116	// A6 * B8
	nop	__LINE__
	}
	;;
/* 57 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f71   = f46, f57, f71	// A7 * B2
	nop	__LINE__
	}
	;;
/* 58 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f86   = f46, f58, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f87   = f46, f59, f87	// A7 * B4
	nop	__LINE__
	}
	;;
/* 59 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f102  = f46, f60, f102	// A7 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f103  = f46, f61, f103	// A7 * B6
	nop	__LINE__
	}
	;;
/* 60 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f118  = f46, f62, f118	// A7 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f119  = f46, f63, f119	// A7 * B8
	nop	__LINE__
	}
	;;
/* 61 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f70   = f47, f57, f70	// A8 * B2
	nop	__LINE__
	}
	;;
/* 62 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f87   = f47, f58, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f86   = f47, f59, f86	// A8 * B4
	nop	__LINE__
	}
	;;
/* 63 */
	{ .mfb
	nop	__LINE__
	(p3) FMA	f103  = f47, f60, f103	// A8 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f102  = f47, f61, f102	// A8 * B6
	nop	__LINE__
	}
	;;
/* 64 */
	{ .mfi
	nop	__LINE__
	(p3) FMA	f119  = f47, f62, f119	// A8 * B7
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f118  = f47, f63, f118	// A8 * B8
	br.cloop.sptk.few .L012
	}
	;;

.L018:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -4, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 2, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f92, f93 = [BOFFSET], 2 * SIZE
	;;
	{ .mfi
	LDFPD	f94, f95 = [BOFFSET], 2 * SIZE
	FSUB	f64  = f72, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f65  = f73, f65
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f104, f105 = [BOFFSET], 2 * SIZE
	FSUB	f80  = f74, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f81  = f75, f81
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f106, f107 = [BOFFSET], 2 * SIZE
	FSUB	f96  = f76, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f97  = f77, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f108, f109 = [BOFFSET], 2 * SIZE
	FSUB	f112 = f78, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f113 = f79, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f110, f111 = [BOFFSET], 2 * SIZE
	FSUB	f66  = f88, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f67  = f89, f67
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f120, f121 = [BOFFSET], 2 * SIZE
	FSUB	f82  = f90, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f83  = f91, f83
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f122, f123 = [BOFFSET], 2 * SIZE
	FSUB	f98  = f92, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f99  = f93, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f124, f125 = [BOFFSET], 2 * SIZE
	FSUB	f114 = f94, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f115 = f95, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f126, f127 = [BOFFSET]
	FSUB	f68  = f104, f68
	adds	BOFFSET = -30 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f69  = f105, f69
#ifdef LN
	adds	AOFFSET = 30 * SIZE, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	LDFPD	f72, f73 = [AOFFSET]
	FSUB	f84  = f106, f84
#ifdef LN
	adds	AOFFSET = - 2 * SIZE, AOFFSET
#else
	adds	AOFFSET =   2 * SIZE, AOFFSET
#endif
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f85  = f107, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f74, f75 = [AOFFSET]
	FSUB	f100 = f108, f100
#ifdef LN
	adds	AOFFSET = - 2 * SIZE, AOFFSET
#else
	adds	AOFFSET =   2 * SIZE, AOFFSET
#endif
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f101 = f109, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f116 = f110, f116
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f117 = f111, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f70  = f120, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f71  = f121, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f86  = f122, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f87  = f123, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f102 = f124, f102
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f103 = f125, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f118 = f126, f118
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f119 = f127, f119
	nop	__LINE__
	}
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [AOFFSET], 2 * SIZE
	;;
	{ .mfi
	LDFPD	f92, f93 = [AOFFSET], 2 * SIZE
	FSUB	f64  = f72, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f65  = f73, f65
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f94, f95 = [AOFFSET], 2 * SIZE
	FSUB	f66  = f74, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f67  = f75, f67
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f104, f105 = [AOFFSET], 2 * SIZE
	FSUB	f68  = f76, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f69  = f77, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f106, f107 = [AOFFSET], 2 * SIZE
	FSUB	f70  = f78, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f71  = f79, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f108, f109 = [AOFFSET], 2 * SIZE
	FSUB	f80  = f88, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f81  = f89, f81
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f110, f111 = [AOFFSET], 2 * SIZE
	FSUB	f82  = f90, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f83  = f91, f83
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f120, f121 = [AOFFSET], 2 * SIZE
	FSUB	f84  = f92, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f85  = f93, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f122, f123 = [AOFFSET], 2 * SIZE
	FSUB	f86  = f94, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f87  = f95, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f124, f125 = [AOFFSET], 2 * SIZE
	FSUB	f96  = f104, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f97  = f105, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f126, f127 = [AOFFSET]
	FSUB	f98  = f106, f98
	adds	AOFFSET = -30 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FSUB	f99  = f107, f99
#ifdef RT
	adds	BOFFSET = 30 * SIZE, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	LDFPD	f72, f73 = [BOFFSET]
	FSUB	f100 = f108, f100
#ifdef RN
	adds	BOFFSET =   2 * SIZE, BOFFSET
#else
	adds	BOFFSET = - 2 * SIZE, BOFFSET
#endif
	}
	{ .mfi
	nop	__LINE__
	FSUB	f101 = f109, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f74, f75 = [BOFFSET]
	FSUB	f102 = f110, f102
#ifdef RN
	adds	BOFFSET =   2 * SIZE, BOFFSET
#else
	adds	BOFFSET = - 2 * SIZE, BOFFSET
#endif
	}
	{ .mfi
	nop	__LINE__
	FSUB	f103 = f111, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f112 = f120, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f113 = f121, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f114 = f122, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f115 = f123, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f116 = f124, f116
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f117 = f125, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f118 = f126, f118
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f119 = f127, f119
	nop	__LINE__
	}
	;;
#endif

#ifdef LN
	{ .mfi
	LDFPD	f76, f77 = [AOFFSET]
	FMPY	f32  = f72, f70
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f72, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f78, f79 = [AOFFSET]
	FMPY	f33  = f73, f70
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f73, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f88, f89 = [AOFFSET]
	FMPY	f34  = f72, f86
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f72, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f90, f91 = [AOFFSET]
	FMPY	f35  = f73, f86
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f73, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f92, f93 = [AOFFSET]
	FMA_C	f70  = f73, f71,  f32
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f102 = f73, f103, f36
	adds	C1 = -2 * SIZE, C1
	}
	;;
	{ .mfi
	LDFPD	f104, f105 = [AOFFSET]
	FMA_D	f71  = f72, f71,  f33
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f103 = f72, f103, f37
	adds	C2 = -2 * SIZE, C2
	}
	;;
	{ .mfi
	LDFPD	f106, f107 = [AOFFSET]
	FMA_C	f86  = f73, f87,  f34
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f118 = f73, f119, f38
	adds	C3 = -2 * SIZE, C3
	}
	;;
	{ .mfi
	LDFPD	f120, f121 = [AOFFSET]
	FMA_D	f87  = f72, f87,  f35
	adds	BOFFSET2 = 28 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f119 = f72, f119, f39
	adds	BOFFSET  = 24 * SIZE, BOFFSET
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f70,  SIZE
	FNMA	f68  = f74, f70, f68
	adds	C4 = -2 * SIZE, C4
	}
	{ .mfi
	STFD	[BOFFSET2] = f102, SIZE
	FNMA	f100 = f74, f102, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f71,  SIZE
	FMA_A	f69  = f75, f70, f69
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f103, SIZE
	FMA_A	f101 = f75, f102, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f86,  SIZE
	FNMA	f84  = f74, f86, f84
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f118, SIZE
	FNMA	f116 = f74, f118, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f87,  -11 * SIZE
	FMA_A	f85  = f75, f86, f85
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f119, -11 * SIZE
	FMA_A	f117 = f75, f118, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f70, SIZE
	FMA_B	f68  = f75, f71, f68
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f102, SIZE
	FMA_B	f100 = f75, f103, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f71, -3 * SIZE
	FNMA	f69  = f74, f71, f69
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f103, -3 * SIZE
	FNMA	f101 = f74, f103, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f86, SIZE
	FMA_B	f84  = f75, f87, f84
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f118, SIZE
	FMA_B	f116 = f75, f119, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f87, -3 * SIZE
	FNMA	f85  = f74, f87, f85
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f119, -3 * SIZE
	FNMA	f117 = f74, f119, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f66  = f76, f70, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f98  = f76, f102, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f67  = f77, f70, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f99  = f77, f102, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f82  = f76, f86, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f114 = f76, f118, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f83  = f77, f86, f83
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f115 = f77, f118, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f66  = f77, f71, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f98  = f77, f103, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f67  = f76, f71, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f99  = f76, f103, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f82  = f77, f87, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f114 = f77, f119, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f83  = f76, f87, f83
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f115 = f76, f119, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f64  = f78, f70, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f96  = f78, f102, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f65  = f79, f70, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f97  = f79, f102, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f80  = f78, f86, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f112 = f78, f118, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f81  = f79, f86, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f113 = f79, f118, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f64  = f79, f71, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f96  = f79, f103, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f65  = f78, f71, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f97  = f78, f103, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f80  = f79, f87, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f112 = f79, f119, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f81  = f78, f87, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f113 = f78, f119, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f88, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f88, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f89, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f89, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f88, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f88, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f89, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f89, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f68  = f89, f69,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f100 = f89, f101, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f69  = f88, f69,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f101 = f88, f101, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f84  = f89, f85,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f116 = f89, f117, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f85  = f88, f85,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f117 = f88, f117, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f68,  SIZE
	FNMA	f66  = f90, f68, f66
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f100, SIZE
	FNMA	f98  = f90, f100, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f69,  SIZE
	FMA_A	f67  = f91, f68, f67
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f101, SIZE
	FMA_A	f99  = f91, f100, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f84,  SIZE
	FNMA	f82  = f90, f84, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f116, SIZE
	FNMA	f114 = f90, f116, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f85,  -11 * SIZE
	FMA_A	f83  = f91, f84, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f117, -11 * SIZE
	FMA_A	f115 = f91, f116, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f68, SIZE
	FMA_B	f66  = f91, f69, f66
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f100, SIZE
	FMA_B	f98  = f91, f101, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f69, -3 * SIZE
	FNMA	f67  = f90, f69, f67
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f101, -3 * SIZE
	FNMA	f99  = f90, f101, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f84, SIZE
	FMA_B	f82  = f91, f85, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f116, SIZE
	FMA_B	f114 = f91, f117, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f85, -3 * SIZE
	FNMA	f83  = f90, f85, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f117, -3 * SIZE
	FNMA	f115 = f90, f117, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f64  = f92, f68, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f96  = f92, f100, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f65  = f93, f68, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f97  = f93, f100, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f80  = f92, f84, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f112 = f92, f116, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f81  = f93, f84, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f113 = f93, f116, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f64  = f93, f69, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f96  = f93, f101, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f65  = f92, f69, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f97  = f92, f101, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f80  = f93, f85, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f112 = f93, f117, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f81  = f92, f85, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f113 = f92, f117, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f104, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f104, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f105, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f105, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f104, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f104, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f105, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f105, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f66  = f105, f67,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f98  = f105, f99,  f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f67  = f104, f67,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f99  = f104, f99,  f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f82  = f105, f83,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f114 = f105, f115, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f83  = f104, f83,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f115 = f104, f115, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f66,  SIZE
	FNMA	f64  = f106, f66, f64
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f98,  SIZE
	FNMA	f96  = f106, f98, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f67,  SIZE
	FMA_A	f65  = f107, f66, f65
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f99,  SIZE
	FMA_A	f97  = f107, f98, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f82,  SIZE
	FNMA	f80  = f106, f82, f80
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f114, SIZE
	FNMA	f112 = f106, f114, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f83,  -11 * SIZE
	FMA_A	f81  = f107, f82, f81
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f115, -11 * SIZE
	FMA_A	f113 = f107, f114, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f66, SIZE
	FMA_B	f64  = f107, f67, f64
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f98, SIZE
	FMA_B	f96  = f107, f99, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f67, -3 * SIZE
	FNMA	f65  = f106, f67, f65
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f99,  -3 * SIZE
	FNMA	f97  = f106, f99, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f82, SIZE
	FMA_B	f80  = f107, f83, f80
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f114, SIZE
	FMA_B	f112 = f107, f115, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f83, -3 * SIZE
	FNMA	f81  = f106, f83, f81
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f115, -3 * SIZE
	FNMA	f113 = f106, f115, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f120, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f120, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f121, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f121, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f120, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f120, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f121, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f121, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f64  = f121, f65,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f96  = f121, f97,  f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f65  = f120, f65,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f97  = f120, f97,  f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f80  = f121, f81,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f112 = f121, f113, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f81  = f120, f81,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f113 = f120, f113, f39
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f64,  SIZE
	STFD	[BOFFSET2] = f96,  SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f65,  SIZE
	STFD	[BOFFSET2] = f97,  SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f80,  SIZE
	STFD	[BOFFSET2] = f112, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f81,  -3 * SIZE
	STFD	[BOFFSET2] = f113, -3 * SIZE
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64 = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f96, SIZE
	mov	f96 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, -1 * SIZE
	mov	f65 = f0
	adds	KK = -4, KK
	}
	{ .mfi
	STFD	[C3 ] = f97, -1 * SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f80, SIZE
	mov	f80 = f0
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfi
	STFD	[C4 ] = f112, SIZE
	mov	f112 = f0
	sub	L = K, KK
	}
	;;
	{ .mfi
	STFD	[C2 ] = f81, -1 * SIZE
	mov	f81 = f0
	adds	I = -1, I
	}
	{ .mfb
	STFD	[C4 ] = f113, -1 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#endif

#ifdef LT
	{ .mfi
	LDFPD	f76, f77 = [AOFFSET], 2 * SIZE
	FMPY	f32  = f72, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f72, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f78, f79 = [AOFFSET]
	FMPY	f33  = f73, f64
	adds	AOFFSET = 4 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f73, f96
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f90, f91 = [AOFFSET], 2 * SIZE
	FMPY	f34  = f72, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f72, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f92, f93 = [AOFFSET], 2 * SIZE
	FMPY	f35  = f73, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f73, f112
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f94, f95 = [AOFFSET]
	FMA_C	f64  = f73, f65,  f32
	adds	AOFFSET = 6 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f96  = f73, f97,  f36
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f108, f109 = [AOFFSET], 2 * SIZE
	FMA_D	f65  = f72, f65,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f97  = f72, f97,  f37
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f110, f111 = [AOFFSET]
	FMA_C	f80  = f73, f81,  f34
	adds	AOFFSET = 8 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f112 = f73, f113, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f126, f127 = [AOFFSET]
	FMA_D	f81  = f72, f81,  f35
	adds	AOFFSET = - 30 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f113 = f72, f113, f39
	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f64,  SIZE
	FNMA	f66  = f74, f64, f66
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f96,  SIZE
	FNMA	f98  = f74, f96, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f65,  SIZE
	FMA_A	f67  = f75, f64, f67
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f97,  SIZE
	FMA_A	f99  = f75, f96, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f80,  SIZE
	FNMA	f82  = f74, f80, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f112, SIZE
	FNMA	f114 = f74, f112, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f81,  5 * SIZE
	FMA_A	f83  = f75, f80, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f113, 5 * SIZE
	FMA_A	f115 = f75, f112, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	FMA_B	f66  = f75, f65, f66
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f96, SIZE
	FMA_B	f98  = f75, f97, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	FNMA	f67  = f74, f65, f67
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f97, SIZE
	FNMA	f99  = f74, f97, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f80, SIZE
	FMA_B	f82  = f75, f81, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f112, SIZE
	FMA_B	f114 = f75, f113, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f81, SIZE
	FNMA	f83  = f74, f81, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f113, SIZE
	FNMA	f115 = f74, f113, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f68  = f76, f64, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f100 = f76, f96, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f69  = f77, f64, f69
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f101 = f77, f96, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f84  = f76, f80, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f116 = f76, f112, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f85  = f77, f80, f85
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f117 = f77, f112, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f68  = f77, f65, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f100 = f77, f97, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f69  = f76, f65, f69
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f101 = f76, f97, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f84  = f77, f81, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f116 = f77, f113, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f85  = f76, f81, f85
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f117 = f76, f113, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f70  = f78, f64, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f102 = f78, f96, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f71  = f79, f64, f71
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f103 = f79, f96, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f86  = f78, f80, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f118 = f78, f112, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f87  = f79, f80, f87
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f119 = f79, f112, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f70  = f79, f65, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f102 = f79, f97, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f71  = f78, f65, f71
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f103 = f78, f97, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f86  = f79, f81, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f118 = f79, f113, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f87  = f78, f81, f87
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f119 = f78, f113, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f90, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f90, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f91, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f91, f98
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f90, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f90, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f91, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f91, f114
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f66  = f91, f67,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f98  = f91, f99,  f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f67  = f90, f67,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f99  = f90, f99,  f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f82  = f91, f83,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f114 = f91, f115, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f83  = f90, f83,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f115 = f90, f115, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f66,  SIZE
	FNMA	f68  = f92, f66, f68
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f98,  SIZE
	FNMA	f100 = f92, f98, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f67,  SIZE
	FMA_A	f69  = f93, f66, f69
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f99,  SIZE
	FMA_A	f101 = f93, f98, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f82,  SIZE
	FNMA	f84  = f92, f82, f84
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f114, SIZE
	FNMA	f116 = f92, f114, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f83,  5 * SIZE
	FMA_A	f85  = f93, f82, f85
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f115, 5 * SIZE
	FMA_A	f117 = f93, f114, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f66, SIZE
	FMA_B	f68  = f93, f67, f68
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f98, SIZE
	FMA_B	f100 = f93, f99, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f67, SIZE
	FNMA	f69  = f92, f67, f69
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f99, SIZE
	FNMA	f101 = f92, f99, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f82, SIZE
	FMA_B	f84  = f93, f83, f84
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f114, SIZE
	FMA_B	f116 = f93, f115, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f83, SIZE
	FNMA	f85  = f92, f83, f85
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f115, SIZE
	FNMA	f117 = f92, f115, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f70  = f94, f66, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f102 = f94, f98, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f71  = f95, f66, f71
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f103 = f95, f98, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f86  = f94, f82, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f118 = f94, f114, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f87  = f95, f82, f87
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f119 = f95, f114, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f70  = f95, f67, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f102 = f95, f99, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f71  = f94, f67, f71
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f103 = f94, f99, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f86  = f95, f83, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f118 = f95, f115, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f87  = f94, f83, f87
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f119 = f94, f115, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f108, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f108, f100
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f109, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f109, f100
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f108, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f108, f116
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f109, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f109, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f68  = f109, f69,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f100 = f109, f101, f36
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f69  = f108, f69,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f101 = f108, f101, f37
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f84  = f109, f85,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f116 = f109, f117, f38
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f85  = f108, f85,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f117 = f108, f117, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f68,  SIZE
	FNMA	f70  = f110, f68, f70
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f100, SIZE
	FNMA	f102 = f110, f100, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f69,  SIZE
	FMA_A	f71  = f111, f68, f71
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f101, SIZE
	FMA_A	f103 = f111, f100, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f84,  SIZE
	FNMA	f86  = f110, f84, f86
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f116, SIZE
	FNMA	f118 = f110, f116, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f85,  5 * SIZE
	FMA_A	f87  = f111, f84, f87
	nop	__LINE__
	}
	{ .mfi
	STFD	[BOFFSET2] = f117, 5 * SIZE
	FMA_A	f119 = f111, f116, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f68, SIZE
	FMA_B	f70  = f111, f69, f70
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f100, SIZE
	FMA_B	f102 = f111, f101, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f69, SIZE
	FNMA	f71  = f110, f69, f71
	nop	__LINE__
	}
	{ .mfi
	STFD	[C3 ] = f101, SIZE
	FNMA	f103 = f110, f101, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f84, SIZE
	FMA_B	f86  = f111, f85, f86
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f116, SIZE
	FMA_B	f118 = f111, f117, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f85, SIZE
	FNMA	f87  = f110, f85, f87
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4 ] = f117, SIZE
	FNMA	f119 = f110, f117, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f126, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f126, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f127, f70
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f127, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f126, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f126, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f127, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f127, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f70  = f127, f71,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f102 = f127, f103, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f71  = f126, f71,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f103 = f126, f103, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f86  = f127, f87,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f118 = f127, f119, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f87  = f126, f87,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f119 = f126, f119, f39
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f70,  SIZE
	STFD	[BOFFSET2] = f102, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f71,  SIZE
	STFD	[BOFFSET2] = f103, SIZE
	sub	r2 = K, KK
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f86,  SIZE
	STFD	[BOFFSET2] = f118, SIZE
	adds	KK =  4, KK
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f87,  -27 * SIZE
	STFD	[BOFFSET2] = f119
	shladd	r2 = r2, ZBASE_SHIFT, r0
	}
	;;
	{ .mfi
	STFD	[C1 ] = f70, SIZE
	mov	f64 = f0
	shladd	AOFFSET = r2, 2, AOFFSET
	}
	{ .mfi
	STFD	[C3 ] = f102, SIZE
	mov	f65 = f0
	shladd	BOFFSET = r2, 2, BOFFSET
	}
	;;
	{ .mfi
	STFD	[C1 ] = f71, SIZE
	mov	f80 = f0
	mov	L = KK
	}
	{ .mfi
	STFD	[C3 ] = f103, SIZE
	mov	f81 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f86, SIZE
	mov	f96 = f0
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfi
	STFD	[C4 ] = f118, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f87, SIZE
	mov	f112 = f0
	adds	I = -1, I
	}
	{ .mfb
	STFD	[C4 ] = f119, SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#endif

#ifdef RN
	{ .mfi
	LDFPD	f76, f77 = [BOFFSET], 2 * SIZE
	FMPY	f32  = f72, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f72, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f78, f79 = [BOFFSET]
	FMPY	f33  = f73, f64
	adds	BOFFSET = 4 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f73, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f90, f91 = [BOFFSET], 2 * SIZE
	FMPY	f34  = f72, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f72, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f92, f93 = [BOFFSET], 2 * SIZE
	FMPY	f35  = f73, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f73, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f94, f95 = [BOFFSET]
	FMA_C	f64  = f73, f65, f32
	adds	BOFFSET = 6 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f68  = f73, f69, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f108, f109 = [BOFFSET], 2 * SIZE
	FMA_D	f65  = f72, f65, f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f69  = f72, f69, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f110, f111 = [BOFFSET]
	FMA_C	f66  = f73, f67, f34
	adds	BOFFSET = 8 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f70  = f73, f71, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f126, f127 = [BOFFSET]
	FMA_D	f67  = f72, f67, f35
	adds	BOFFSET = - 30 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f71  = f72, f71, f39
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f64,  SIZE
	FNMA	f80  = f74, f64, f80
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f68,  SIZE
	FNMA	f84  = f74, f68, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f65,  SIZE
	FMA_A	f81  = f75, f64, f81
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f69,  SIZE
	FMA_A	f85  = f75, f68, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f66,  SIZE
	FNMA	f82  = f74, f66, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f70,  SIZE
	FNMA	f86  = f74, f70, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f67,  5 * SIZE
	FMA_A	f83  = f75, f66, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f71,  5 * SIZE
	FMA_A	f87  = f75, f70, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	FMA_B	f80  = f75, f65, f80
	nop	__LINE__
	}
	{ .mfi
	STFD	[C5 ] = f68, SIZE
	FMA_B	f84  = f75, f69, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	FNMA	f81  = f74, f65, f81
	nop	__LINE__
	}
	{ .mfi
	STFD	[C5 ] = f69, SIZE
	FNMA	f85  = f74, f69, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f66, SIZE
	FMA_B	f82  = f75, f67, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[C5 ] = f70, SIZE
	FMA_B	f86  = f75, f71, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f67, 5 * SIZE
	FNMA	f83  = f74, f67, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[C5 ] = f71, 5 * SIZE
	FNMA	f87  = f74, f71, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f96  = f76, f64, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f100 = f76, f68, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f97  = f77, f64, f97
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f101 = f77, f68, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f98  = f76, f66, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f102 = f76, f70, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f99  = f77, f66, f99
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f103 = f77, f70, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f96  = f77, f65, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f100 = f77, f69, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f97  = f76, f65, f97
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f101 = f76, f69, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f98  = f77, f67, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f102 = f77, f71, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f99  = f76, f67, f99
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f103 = f76, f71, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f112 = f78, f64, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f116 = f78, f68, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f113 = f79, f64, f113
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f117 = f79, f68, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f114 = f78, f66, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f118 = f78, f70, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f115 = f79, f66, f115
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f119 = f79, f70, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f112 = f79, f65, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f116 = f79, f69, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f113 = f78, f65, f113
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f117 = f78, f69, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f114 = f79, f67, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f118 = f79, f71, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f115 = f78, f67, f115
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f119 = f78, f71, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f90, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f90, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f91, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f91, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f90, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f90, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f91, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f91, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f80  = f91, f81,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f84  = f91, f85,  f36
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f81  = f90, f81,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f85  = f90, f85,  f37
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f82  = f91, f83,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f86  = f91, f87,  f38
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f83  = f90, f83,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f87  = f90, f87,  f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f80,  SIZE
	FNMA	f96  = f92, f80, f96
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f84,  SIZE
	FNMA	f100 = f92, f84, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f81,  SIZE
	FMA_A	f97  = f93, f80, f97
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f85,  SIZE
	FMA_A	f101 = f93, f84, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f82,  SIZE
	FNMA	f98  = f92, f82, f98
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f86,  SIZE
	FNMA	f102 = f92, f86, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f83,  5 * SIZE
	FMA_A	f99  = f93, f82, f99
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f87,  5 * SIZE
	FMA_A	f103 = f93, f86, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f80, SIZE
	FMA_B	f96  = f93, f81, f96
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f84, SIZE
	FMA_B	f100 = f93, f85, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f81, SIZE
	FNMA	f97  = f92, f81, f97
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f85, SIZE
	FNMA	f101 = f92, f85, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f82, SIZE
	FMA_B	f98  = f93, f83, f98
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f86, SIZE
	FMA_B	f102 = f93, f87, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f83, 5 * SIZE
	FNMA	f99  = f92, f83, f99
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f87, 5 * SIZE
	FNMA	f103 = f92, f87, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f112 = f94, f80, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f116 = f94, f84, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f113 = f95, f80, f113
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f117 = f95, f84, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f114 = f94, f82, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f118 = f94, f86, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f115 = f95, f82, f115
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f119 = f95, f86, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f112 = f95, f81, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f116 = f95, f85, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f113 = f94, f81, f113
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f117 = f94, f85, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f114 = f95, f83, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f118 = f95, f87, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f115 = f94, f83, f115
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f119 = f94, f87, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f108, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f108, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f109, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f109, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f108, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f108, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f109, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f109, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f96  = f109, f97,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f100 = f109, f101, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f97  = f108, f97,  f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f101 = f108, f101, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f98  = f109, f99,  f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f102 = f109, f103, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f99  = f108, f99,  f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f103 = f108, f103, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f96,  SIZE
	FNMA	f112 = f110, f96, f112
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f100, SIZE
	FNMA	f116 = f110, f100, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f97,  SIZE
	FMA_A	f113 = f111, f96, f113
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f101, SIZE
	FMA_A	f117 = f111, f100, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f98,  SIZE
	FNMA	f114 = f110, f98, f114
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f102, SIZE
	FNMA	f118 = f110, f102, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f99,  5 * SIZE
	FMA_A	f115 = f111, f98, f115
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f103, 5 * SIZE
	FMA_A	f119 = f111, f102, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f96, SIZE
	FMA_B	f112 = f111, f97, f112
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f100, SIZE
	FMA_B	f116 = f111, f101, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f97, SIZE
	FNMA	f113 = f110, f97, f113
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f101, SIZE
	FNMA	f117 = f110, f101, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f98, SIZE
	FMA_B	f114 = f111, f99, f114
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f102, SIZE
	FMA_B	f118 = f111, f103, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f99, 5 * SIZE
	FNMA	f115 = f110, f99, f115
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f103, 5 * SIZE
	FNMA	f119 = f110, f103, f119
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f126, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f126, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f127, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f127, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f126, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f126, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f127, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f127, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f112 = f127, f113, f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f116 = f127, f117, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f113 = f126, f113, f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f117 = f126, f117, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f114 = f127, f115, f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f118 = f127, f119, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f115 = f126, f115, f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f119 = f126, f119, f39
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f112, SIZE
	STFD	[AOFFSET2] = f116, SIZE
	sub	r2 = K, KK
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f113, SIZE
	STFD	[AOFFSET2] = f117, SIZE
	mov	L = KK
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f114, SIZE
	STFD	[AOFFSET2] = f118, SIZE
	shladd	r2 = r2, ZBASE_SHIFT, r0
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f115, -27 * SIZE
	STFD	[AOFFSET2] = f119
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f112, SIZE
	mov	f64 = f0
	shladd	BOFFSET = r2, 2, BOFFSET
	}
	{ .mfi
	STFD	[C8 ] = f116, SIZE
	mov	f65 = f0
	shladd	AOFFSET = r2, 2, AOFFSET
	}
	;;
	{ .mfi
	STFD	[C4 ] = f113, SIZE
	mov	f80 = f0
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfi
	STFD	[C8 ] = f117, SIZE
	mov	f81 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f114, SIZE
	mov	f96 = f0
	adds	I = -1, I
	}
	{ .mfi
	STFD	[C8 ] = f118, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f115, 5 * SIZE
	mov	f112 = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C8 ] = f119, 5 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L011
	}
#endif

#ifdef RT
	{ .mfi
	LDFPD	f76, f77 = [BOFFSET]
	FMPY	f32  = f72, f112
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f72, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f78, f79 = [BOFFSET]
	FMPY	f33  = f73, f112
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f73, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f88, f89 = [BOFFSET]
	FMPY	f34  = f72, f114
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f72, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f90, f91 = [BOFFSET]
	FMPY	f35  = f73, f114
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f73, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f92, f93 = [BOFFSET]
	FMA_C	f112 = f73, f113, f32
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f116 = f73, f117, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f104, f105 = [BOFFSET]
	FMA_D	f113 = f72, f113, f33
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f117 = f72, f117, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f106, f107 = [BOFFSET]
	FMA_C	f114 = f73, f115, f34
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f118 = f73, f119, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f120, f121 = [BOFFSET]
	FMA_D	f115 = f72, f115, f35
	adds	AOFFSET2 = 28 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f119 = f72, f119, f39
	adds	AOFFSET  = 24 * SIZE, AOFFSET
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f112, SIZE
	FNMA	f96  = f74, f112, f96
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f116, SIZE
	FNMA	f100 = f74, f116, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f113, SIZE
	FMA_A	f97  = f75, f112, f97
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f117, SIZE
	FMA_A	f101 = f75, f116, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f114, SIZE
	FNMA	f98  = f74, f114, f98
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f118, SIZE
	FNMA	f102 = f74, f118, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f115, -11 * SIZE
	FMA_A	f99  = f75, f114, f99
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f119, -11 * SIZE
	FMA_A	f103 = f75, f118, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f112, SIZE
	FMA_B	f96  = f75, f113, f96
	nop	__LINE__
	}
	{ .mfi
	STFD	[C8 ] = f116, SIZE
	FMA_B	f100 = f75, f117, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f113, SIZE
	FNMA	f97  = f74, f113, f97
	nop	__LINE__
	}
	{ .mfi
	STFD	[C8 ] = f117, SIZE
	FNMA	f101 = f74, f117, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f114, SIZE
	FMA_B	f98  = f75, f115, f98
	nop	__LINE__
	}
	{ .mfi
	STFD	[C8 ] = f118, SIZE
	FMA_B	f102 = f75, f119, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f115, 5 * SIZE
	FNMA	f99  = f74, f115, f99
	nop	__LINE__
	}
	{ .mfi
	STFD	[C8 ] = f119, 5 * SIZE
	FNMA	f103 = f74, f119, f103
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f80  = f76, f112, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f84  = f76, f116, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f81  = f77, f112, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f85  = f77, f116, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f82  = f76, f114, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f86  = f76, f118, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f83  = f77, f114, f83
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f87  = f77, f118, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f80  = f77, f113, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f84  = f77, f117, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f81  = f76, f113, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f85  = f76, f117, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f82  = f77, f115, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f86  = f77, f119, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f83  = f76, f115, f83
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f87  = f76, f119, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f64  = f78, f112, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f68  = f78, f116, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f65  = f79, f112, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f69  = f79, f116, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f66  = f78, f114, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f70  = f78, f118, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f67  = f79, f114, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f71  = f79, f118, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f64  = f79, f113, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f68  = f79, f117, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f65  = f78, f113, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f69  = f78, f117, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f66  = f79, f115, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f70  = f79, f119, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f67  = f78, f115, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f71  = f78, f119, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f88, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f88, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f89, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f89, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f88, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f88, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f89, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f89, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f96  = f89, f97, f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f100 = f89, f101, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f97  = f88, f97, f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f101 = f88, f101, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f98  = f89, f99, f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f102 = f89, f103, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f99  = f88, f99, f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f103 = f88, f103, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f96,  SIZE
	FNMA	f80  = f90, f96, f80
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f100, SIZE
	FNMA	f84  = f90, f100, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f97,  SIZE
	FMA_A	f81  = f91, f96, f81
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f101, SIZE
	FMA_A	f85  = f91, f100, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f98,  SIZE
	FNMA	f82  = f90, f98, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f102, SIZE
	FNMA	f86  = f90, f102, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f99,  -11 * SIZE
	FMA_A	f83  = f91, f98, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f103, -11 * SIZE
	FMA_A	f87  = f91, f102, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f96, SIZE
	FMA_B	f80  = f91, f97, f80
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f100, SIZE
	FMA_B	f84  = f91, f101, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f97, SIZE
	FNMA	f81  = f90, f97, f81
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f101, SIZE
	FNMA	f85  = f90, f101, f85
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f98, SIZE
	FMA_B	f82  = f91, f99, f82
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f102, SIZE
	FMA_B	f86  = f91, f103, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f99, 5 * SIZE
	FNMA	f83  = f90, f99, f83
	nop	__LINE__
	}
	{ .mfi
	STFD	[C7 ] = f103, 5 * SIZE
	FNMA	f87  = f90, f103, f87
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f64  = f92, f96, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f68  = f92, f100, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f65  = f93, f96, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f69  = f93, f100, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f66  = f92, f98, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f70  = f92, f102, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_A	f67  = f93, f98, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_A	f71  = f93, f102, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f64  = f93, f97, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f68  = f93, f101, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f65  = f92, f97, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f69  = f92, f101, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_B	f66  = f93, f99, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f70  = f93, f103, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FNMA	f67  = f92, f99, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f71  = f92, f103, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f104, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f104, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f105, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f105, f84
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f104, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f104, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f105, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f105, f86
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f80  = f105, f81, f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f84  = f105, f85, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f81  = f104, f81, f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f85  = f104, f85, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f82  = f105, f83, f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f86  = f105, f87, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f83  = f104, f83, f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f87  = f104, f87, f39
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f80,  SIZE
	FNMA	f64  = f106, f80, f64
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f84,  SIZE
	FNMA	f68  = f106, f84, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f81,  SIZE
	FMA_A	f65  = f107, f80, f65
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f85,  SIZE
	FMA_A	f69  = f107, f84, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f82,  SIZE
	FNMA	f66  = f106, f82, f66
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f86,  SIZE
	FNMA	f70  = f106, f86, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[AOFFSET]  = f83,  -11 * SIZE
	FMA_A	f67  = f107, f82, f67
	nop	__LINE__
	}
	{ .mfi
	STFD	[AOFFSET2] = f87,  -11 * SIZE
	FMA_A	f71  = f107, f86, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f80, SIZE
	FMA_B	f64  = f107, f81, f64
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f84, SIZE
	FMA_B	f68  = f107, f85, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f81, SIZE
	FNMA	f65  = f106, f81, f65
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f85, SIZE
	FNMA	f69  = f106, f85, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f82, SIZE
	FMA_B	f66  = f107, f83, f66
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f86, SIZE
	FMA_B	f70  = f107, f87, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f83, 5 * SIZE
	FNMA	f67  = f106, f83, f67
	nop	__LINE__
	}
	{ .mfi
	STFD	[C6 ] = f87, 5 * SIZE
	FNMA	f71  = f106, f87, f71
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f32  = f120, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f36  = f120, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f33  = f121, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f37  = f121, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f34  = f120, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f38  = f120, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f35  = f121, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f39  = f121, f70
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f64  = f121, f65, f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f68  = f121, f69, f36
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f65  = f120, f65, f33
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f69  = f120, f69, f37
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_C	f66  = f121, f67, f34
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_C	f70  = f121, f71, f38
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA_D	f67  = f120, f67, f35
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_D	f71  = f120, f71, f39
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f64,  SIZE
	STFD	[AOFFSET2] = f68,  SIZE
	shladd	r2 = K, ZBASE_SHIFT, r0
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f65,  SIZE
	STFD	[AOFFSET2] = f69,  SIZE
	shladd	AORIG = r2, 2, AORIG
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f66,  SIZE
	STFD	[AOFFSET2] = f70,  SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[AOFFSET]  = f67,  -3 * SIZE
	STFD	[AOFFSET2] = f71
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64 = f0
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfi
	STFD	[C5 ] = f68, SIZE
	mov	f81 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	mov	f65 = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C5 ] = f69, SIZE
	mov	f96 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f66, SIZE
	mov	f80 = f0
	sub	L = K, KK
	}
	{ .mfi
	STFD	[C5 ] = f70, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f67, 5 * SIZE
	mov	f112 = f0
	adds	I = -1, I
	}
	{ .mfb
	STFD	[C5 ] = f71, 5 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#endif

.L020:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L030
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f67  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
	mov	f66  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	shladd	AOFFSET = r3, 1, AORIG
	}
	;;
#endif
	;;
	adds	L =  1, L
	;;
	{ .mfi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82 = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83 = f0
	shr	L = L, 1
	}
	;;
	{ .mfi
	(p7) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f98 = f0
	adds	L =  -1, L
	}
	{ .mfi
	(p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	mov	f99 = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	(p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f114 = f0
	mov	ar.lc = L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	mov	f115 = f0
	nop	__LINE__
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L028
	;;
	.align 16

.L022:
	{ .mfi
	lfetch.nt1	[PREA],  8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f65   = f32, f49, f65	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],  16 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f81   = f32, f51, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f97  = f32, f53, f97	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f113  = f32, f55, f113	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f64   = f33, f49, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f80   = f33, f51, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	FMA	f97   = f33, f52, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f96  = f33, f53, f96	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f113  = f33, f54, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f112  = f33, f55, f112	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f67   = f34, f49, f67	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f82   = f34, f50, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f83   = f34, f51, f83	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f98   = f34, f52, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f99  = f34, f53, f99	// A3 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f114  = f34, f54, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f115  = f34, f55, f115	// A3 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f67   = f35, f48, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f66   = f35, f49, f66	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f82   = f35, f51, f82	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f99   = f35, f52, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f98  = f35, f53, f98	// A4 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f115  = f35, f54, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f114  = f35, f55, f114	// A4 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f65   = f40, f57, f65	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f81   = f40, f59, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f97  = f40, f61, f97	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f113  = f40, f63, f113	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f96  = f41, f61, f96	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f112  = f41, f63, f112	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f67   = f42, f57, f67	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f83   = f42, f59, f83	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f99  = f42, f61, f99	// A3 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f115  = f42, f63, f115	// A3 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f66   = f43, f57, f66	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f82   = f43, f59, f82	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f98  = f43, f61, f98	// A4 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f114  = f43, f63, f114	// A4 * B8
	br.cloop.sptk.few .L022
	}
	;;
.L028:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f104, f105 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f106, f107 = [BOFFSET], 2 * SIZE
	;;
	{ .mfi
	LDFPD	f120, f121 = [BOFFSET], 2 * SIZE
	FSUB	f64  = f72, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f65  = f73, f65
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f122, f123 = [BOFFSET]
	FSUB	f80  = f74, f80
	adds	BOFFSET = -14 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f81  = f75, f81
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f96  = f88, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f97  = f89, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f112 = f90, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f113 = f91, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f66  = f104, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f67  = f105, f67
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f82  = f106, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f83  = f107, f83
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f98  = f120, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f99  = f121, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f114 = f122, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB_A	f115 = f123, f115
	nop	__LINE__
	}
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f90, f91 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f104, f105 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f106, f107 = [AOFFSET], 2 * SIZE
	;;
	{ .mfi
	LDFPD	f120, f121 = [AOFFSET], 2 * SIZE
	FSUB	f64  = f72, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f65  = f73, f65
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f122, f123 = [AOFFSET]
	FSUB	f66  = f74, f66
	adds	AOFFSET = -14 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FSUB	f67  = f75, f67
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f80  = f88, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f81  = f89, f81
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f82  = f90, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f83  = f91, f83
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f96  = f104, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f97  = f105, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f98  = f106, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f99  = f107, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f112 = f120, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f113 = f121, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f114 = f122, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f115 = f123, f115
	nop	__LINE__
	}
	;;
#endif

#ifdef LN
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f104, f105 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f106, f107 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f104, f66
	FMPY	f33  = f105, f66
	FMPY	f34  = f104, f82
	FMPY	f35  = f105, f82
	FMPY	f36  = f104, f98
	FMPY	f37  = f105, f98
	FMPY	f38  = f104, f114
	FMPY	f39  = f105, f114
	;;
	FMA_C	f66  = f105, f67,  f32
	FMA_D	f67  = f104, f67,  f33
	FMA_C	f82  = f105, f83,  f34
	FMA_D	f83  = f104, f83,  f35
	FMA_C	f98  = f105, f99,  f36
	FMA_D	f99  = f104, f99,  f37
	FMA_C	f114 = f105, f115, f38
	FMA_D	f115 = f104, f115, f39
	;;
	FNMA	f64  = f106, f66, f64
	FMA_A	f65  = f107, f66, f65
	FNMA	f80  = f106, f82, f80
	FMA_A	f81  = f107, f82, f81
	FNMA	f96  = f106, f98, f96
	FMA_A	f97  = f107, f98, f97
	FNMA	f112 = f106, f114, f112
	FMA_A	f113 = f107, f114, f113
	;;
	FMA_B	f64  = f107, f67, f64
	FNMA	f65  = f106, f67, f65
	FMA_B	f80  = f107, f83, f80
	FNMA	f81  = f106, f83, f81
	FMA_B	f96  = f107, f99, f96
	FNMA	f97  = f106, f99, f97
	FMA_B	f112 = f107, f115, f112
	FNMA	f113 = f106, f115, f113
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f80
	FMPY	f35  = f121, f80
	FMPY	f36  = f120, f96
	FMPY	f37  = f121, f96
	FMPY	f38  = f120, f112
	FMPY	f39  = f121, f112
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	FMA_C	f80  = f121, f81,  f34
	FMA_D	f81  = f120, f81,  f35
	FMA_C	f96  = f121, f97,  f36
	FMA_D	f97  = f120, f97,  f37
	FMA_C	f112 = f121, f113, f38
	FMA_D	f113 = f120, f113, f39
	;;
#endif

#ifdef LT
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f90, f91 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f80
	FMPY	f35  = f73, f80
	FMPY	f36  = f72, f96
	FMPY	f37  = f73, f96
	FMPY	f38  = f72, f112
	FMPY	f39  = f73, f112
	;;
	FMA_C	f64  = f73, f65,  f32
	FMA_D	f65  = f72, f65,  f33
	FMA_C	f80  = f73, f81,  f34
	FMA_D	f81  = f72, f81,  f35
	FMA_C	f96  = f73, f97,  f36
	FMA_D	f97  = f72, f97,  f37
	FMA_C	f112 = f73, f113, f38
	FMA_D	f113 = f72, f113, f39
	;;
	FNMA	f66  = f74, f64, f66
	FMA_A	f67  = f75, f64, f67
	FNMA	f82  = f74, f80, f82
	FMA_A	f83  = f75, f80, f83
	FNMA	f98  = f74, f96, f98
	FMA_A	f99  = f75, f96, f99
	FNMA	f114 = f74, f112, f114
	FMA_A	f115 = f75, f112, f115
	;;
	FMA_B	f66  = f75, f65, f66
	FNMA	f67  = f74, f65, f67
	FMA_B	f82  = f75, f81, f82
	FNMA	f83  = f74, f81, f83
	FMA_B	f98  = f75, f97, f98
	FNMA	f99  = f74, f97, f99
	FMA_B	f114 = f75, f113, f114
	FNMA	f115 = f74, f113, f115
	;;
	FMPY	f32  = f90, f66
	FMPY	f33  = f91, f66
	FMPY	f34  = f90, f82
	FMPY	f35  = f91, f82
	FMPY	f36  = f90, f98
	FMPY	f37  = f91, f98
	FMPY	f38  = f90, f114
	FMPY	f39  = f91, f114
	;;
	FMA_C	f66  = f91, f67,  f32
	FMA_D	f67  = f90, f67,  f33
	FMA_C	f82  = f91, f83,  f34
	FMA_D	f83  = f90, f83,  f35
	FMA_C	f98  = f91, f99,  f36
	FMA_D	f99  = f90, f99,  f37
	FMA_C	f114 = f91, f115, f38
	FMA_D	f115 = f90, f115, f39
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f92, f93 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f94, f95 = [BOFFSET]
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f108, f109 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f110, f111 = [BOFFSET]
	adds	BOFFSET = 8 * SIZE, BOFFSET
	;;
	LDFPD	f126, f127 = [BOFFSET]
	adds	BOFFSET = - 30 * SIZE, BOFFSET
	;;

	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	FMPY	f34  = f72, f66
	FMPY	f35  = f73, f66
	;;
	FMA_C	f64  = f73, f65, f32
	FMA_D	f65  = f72, f65, f33
	FMA_C	f66  = f73, f67, f34
	FMA_D	f67  = f72, f67, f35
	;;
	FNMA	f80  = f74, f64, f80
	FMA_A	f81  = f75, f64, f81
	FNMA	f82  = f74, f66, f82
	FMA_A	f83  = f75, f66, f83
	;;
	FMA_B	f80  = f75, f65, f80
	FNMA	f81  = f74, f65, f81
	FMA_B	f82  = f75, f67, f82
	FNMA	f83  = f74, f67, f83
	;;
	FNMA	f96  = f76, f64, f96
	FMA_A	f97  = f77, f64, f97
	FNMA	f98  = f76, f66, f98
	FMA_A	f99  = f77, f66, f99
	;;
	FMA_B	f96  = f77, f65, f96
	FNMA	f97  = f76, f65, f97
	FMA_B	f98  = f77, f67, f98
	FNMA	f99  = f76, f67, f99
	;;
	FNMA	f112 = f78, f64, f112
	FMA_A	f113 = f79, f64, f113
	FNMA	f114 = f78, f66, f114
	FMA_A	f115 = f79, f66, f115
	;;
	FMA_B	f112 = f79, f65, f112
	FNMA	f113 = f78, f65, f113
	FMA_B	f114 = f79, f67, f114
	FNMA	f115 = f78, f67, f115
	;;
	FMPY	f32  = f90, f80
	FMPY	f33  = f91, f80
	FMPY	f34  = f90, f82
	FMPY	f35  = f91, f82
	;;
	FMA_C	f80  = f91, f81,  f32
	FMA_D	f81  = f90, f81,  f33
	FMA_C	f82  = f91, f83,  f34
	FMA_D	f83  = f90, f83,  f35
	;;

	FNMA	f96  = f92, f80, f96
	FMA_A	f97  = f93, f80, f97
	FNMA	f98  = f92, f82, f98
	FMA_A	f99  = f93, f82, f99
	;;
	FMA_B	f96  = f93, f81, f96
	FNMA	f97  = f92, f81, f97
	FMA_B	f98  = f93, f83, f98
	FNMA	f99  = f92, f83, f99
	;;
	FNMA	f112 = f94, f80, f112
	FMA_A	f113 = f95, f80, f113
	FNMA	f114 = f94, f82, f114
	FMA_A	f115 = f95, f82, f115
	;;
	FMA_B	f112 = f95, f81, f112
	FNMA	f113 = f94, f81, f113
	FMA_B	f114 = f95, f83, f114
	FNMA	f115 = f94, f83, f115
	;;
	FMPY	f32  = f108, f96
	FMPY	f33  = f109, f96
	FMPY	f34  = f108, f98
	FMPY	f35  = f109, f98
	;;
	FMA_C	f96  = f109, f97,  f32
	FMA_D	f97  = f108, f97,  f33
	FMA_C	f98  = f109, f99,  f34
	FMA_D	f99  = f108, f99,  f35
	;;
	FNMA	f112 = f110, f96, f112
	FMA_A	f113 = f111, f96, f113
	FNMA	f114 = f110, f98, f114
	FMA_A	f115 = f111, f98, f115
	;;
	FMA_B	f112 = f111, f97, f112
	FNMA	f113 = f110, f97, f113
	FMA_B	f114 = f111, f99, f114
	FNMA	f115 = f110, f99, f115
	;;
	FMPY	f32  = f126, f112
	FMPY	f33  = f127, f112
	FMPY	f34  = f126, f114
	FMPY	f35  = f127, f114
	;;
	FMA_C	f112 = f127, f113, f32
	FMA_D	f113 = f126, f113, f33
	FMA_C	f114 = f127, f115, f34
	FMA_D	f115 = f126, f115, f35
	;;
#endif

#ifdef RT
	adds	BOFFSET = 30 * SIZE, BOFFSET
	;;
	LDFPD	f72, f73 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f74, f75 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f76, f77 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f78, f79 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f88, f89 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f92, f93 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	LDFPD	f104, f105 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f106, f107 = [BOFFSET]
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
	LDFPD	f120, f121 = [BOFFSET]
	;;
	FMPY	f32  = f72, f112
	FMPY	f33  = f73, f112
	FMPY	f34  = f72, f114
	FMPY	f35  = f73, f114
	;;
	FMA_C	f112 = f73, f113, f32
	FMA_D	f113 = f72, f113, f33
	FMA_C	f114 = f73, f115, f34
	FMA_D	f115 = f72, f115, f35
	;;
	FNMA	f96  = f74, f112, f96
	FMA_A	f97  = f75, f112, f97
	FNMA	f98  = f74, f114, f98
	FMA_A	f99  = f75, f114, f99
	;;
	FMA_B	f96  = f75, f113, f96
	FNMA	f97  = f74, f113, f97
	FMA_B	f98  = f75, f115, f98
	FNMA	f99  = f74, f115, f99
	;;
	FNMA	f80  = f76, f112, f80
	FMA_A	f81  = f77, f112, f81
	FNMA	f82  = f76, f114, f82
	FMA_A	f83  = f77, f114, f83
	;;
	FMA_B	f80  = f77, f113, f80
	FNMA	f81  = f76, f113, f81
	FMA_B	f82  = f77, f115, f82
	FNMA	f83  = f76, f115, f83
	;;
	FNMA	f64  = f78, f112, f64
	FMA_A	f65  = f79, f112, f65
	FNMA	f66  = f78, f114, f66
	FMA_A	f67  = f79, f114, f67
	;;
	FMA_B	f64  = f79, f113, f64
	FNMA	f65  = f78, f113, f65
	FMA_B	f66  = f79, f115, f66
	FNMA	f67  = f78, f115, f67
	;;
	FMPY	f32  = f88, f96
	FMPY	f33  = f89, f96
	FMPY	f34  = f88, f98
	FMPY	f35  = f89, f98
	;;
	FMA_C	f96  = f89, f97, f32
	FMA_D	f97  = f88, f97, f33
	FMA_C	f98  = f89, f99, f34
	FMA_D	f99  = f88, f99, f35
	;;
	FNMA	f80  = f90, f96, f80
	FMA_A	f81  = f91, f96, f81
	FNMA	f82  = f90, f98, f82
	FMA_A	f83  = f91, f98, f83
	;;
	FMA_B	f80  = f91, f97, f80
	FNMA	f81  = f90, f97, f81
	FMA_B	f82  = f91, f99, f82
	FNMA	f83  = f90, f99, f83
	;;
	FNMA	f64  = f92, f96, f64
	FMA_A	f65  = f93, f96, f65
	FNMA	f66  = f92, f98, f66
	FMA_A	f67  = f93, f98, f67
	;;
	FMA_B	f64  = f93, f97, f64
	FNMA	f65  = f92, f97, f65
	FMA_B	f66  = f93, f99, f66
	FNMA	f67  = f92, f99, f67
	;;
	FMPY	f32  = f104, f80
	FMPY	f33  = f105, f80
	FMPY	f34  = f104, f82
	FMPY	f35  = f105, f82
	;;
	FMA_C	f80  = f105, f81, f32
	FMA_D	f81  = f104, f81, f33
	FMA_C	f82  = f105, f83, f34
	FMA_D	f83  = f104, f83, f35
	;;
	FNMA	f64  = f106, f80, f64
	FMA_A	f65  = f107, f80, f65
	FNMA	f66  = f106, f82, f66
	FMA_A	f67  = f107, f82, f67
	;;
	FMA_B	f64  = f107, f81, f64
	FNMA	f65  = f106, f81, f65
	FMA_B	f66  = f107, f83, f66
	FNMA	f67  = f106, f83, f67
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f66
	FMPY	f35  = f121, f66
	;;
	FMA_C	f64  = f121, f65, f32
	FMA_D	f65  = f120, f65, f33
	FMA_C	f66  = f121, f67, f34
	FMA_D	f67  = f120, f67, f35
	;;
#endif

#if defined(LN) || defined(LT)
	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;
	STFD	[BOFFSET]  = f64,  SIZE
	STFD	[BOFFSET2] = f96,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	STFD	[BOFFSET2] = f97,  SIZE
	;;
	STFD	[BOFFSET]  = f80,  SIZE
	STFD	[BOFFSET2] = f112, SIZE
	;;
	STFD	[BOFFSET]  = f81,  5 * SIZE
	STFD	[BOFFSET2] = f113, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f66,  SIZE
	STFD	[BOFFSET2] = f98,  SIZE
	;;
	STFD	[BOFFSET]  = f67,  SIZE
	STFD	[BOFFSET2] = f99,  SIZE
	;;
	STFD	[BOFFSET]  = f82,  SIZE
	STFD	[BOFFSET2] = f114, SIZE
	;;
	STFD	[BOFFSET]  = f83,  5 * SIZE
	STFD	[BOFFSET2] = f115, 5 * SIZE
	;;
	adds	BOFFSET = - 16 * SIZE, BOFFSET
	;;
#else
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	STFD	[AOFFSET2] = f80,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	STFD	[AOFFSET2] = f81,  SIZE
	;;
	STFD	[AOFFSET]  = f66,  SIZE
	STFD	[AOFFSET2] = f82,  SIZE
	;;
	STFD	[AOFFSET]  = f67,  5 * SIZE
	STFD	[AOFFSET2] = f83,  5 * SIZE
	;;
	STFD	[AOFFSET]  = f96,  SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET]  = f97,  SIZE
	STFD	[AOFFSET2] = f113, SIZE
	;;
	STFD	[AOFFSET]  = f98,  SIZE
	STFD	[AOFFSET2] = f114, SIZE
	;;
	STFD	[AOFFSET]  = f99,  5 * SIZE
	STFD	[AOFFSET2] = f115, 5 * SIZE
	;;
	adds	AOFFSET = - 16 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -4 * SIZE, C1
	adds	C2 = -4 * SIZE, C2
	adds	C3 = -4 * SIZE, C3
	adds	C4 = -4 * SIZE, C4
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	;;
	STFD	[C1 ] = f66, SIZE
	;;
	STFD	[C1 ] = f67, SIZE
	;;
	STFD	[C2 ] = f80, SIZE
	;;
	STFD	[C2 ] = f81, SIZE
	;;
	STFD	[C2 ] = f82, SIZE
	;;
	STFD	[C2 ] = f83, SIZE
	;;

	STFD	[C3 ] = f96, SIZE
	;;
	STFD	[C3 ] = f97, SIZE
	;;
	STFD	[C3 ] = f98, SIZE
	;;
	STFD	[C3 ] = f99, SIZE
	;;

	STFD	[C4 ] = f112, SIZE
	;;
	STFD	[C4 ] = f113, SIZE
	;;
	STFD	[C4 ] = f114, SIZE
	;;
	STFD	[C4 ] = f115, SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -4 * SIZE, C1
	adds	C2 = -4 * SIZE, C2
	adds	C3 = -4 * SIZE, C3
	adds	C4 = -4 * SIZE, C4
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	shladd	AOFFSET = L, 1, AOFFSET
	shladd	BOFFSET = L, 2, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	.align 16

.L030:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L049
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, ZBASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, ZBASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mfb
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f73  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
	mov	f72  = f0
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f73  = f0
	add	AOFFSET = r3, AORIG
	}
	;;
#endif
	;;
	adds	L =  1, L
	;;

	{ .mmi
	nop	__LINE__
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	tbit.z	p12, p0 = L, 0
	}
	;;
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f88  = f0
	shr	L = L, 1
	}
	{ .mfi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f89  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	(p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	mov	f104  = f0
	adds	L =  -1, L
	}
	{ .mfb
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	mov	f105 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	(p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f120 = f0
	mov	ar.lc = L
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f121 = f0
	nop	__LINE__
	}
	;;
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L038
	;;
	.align 16

.L032:
	{ .mfb
	lfetch.nt1	[PREA],  4 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA_B	f65   = f32, f49, f65	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],  16 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f81   = f32, f51, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f97  = f32, f53, f97	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_B	f113  = f32, f55, f113	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f64   = f33, f49, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f80   = f33, f51, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	FMA	f97   = f33, f52, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f96  = f33, f53, f96	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f113  = f33, f54, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA_A	f112  = f33, f55, f112	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f65   = f40, f57, f65	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f81   = f40, f59, f81	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f97  = f40, f61, f97	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_B	f113  = f40, f63, f113	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f96  = f41, f61, f96	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA_A	f112  = f41, f63, f112	// A2 * B8
	br.cloop.sptk.few .L032
	}
	;;
.L038:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, ZBASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif

#if defined(LN) || defined(LT)
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f104, f105 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f120, f121 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB_A	f65  = f73, f65
	FSUB	f80  = f88, f80
	FSUB_A	f81  = f89, f81
	FSUB	f96  = f104, f96
	FSUB_A	f97  = f105, f97
	FSUB	f112 = f120, f112
	FSUB_A	f113 = f121, f113
	;;
#else
	LDFPD	f72, f73 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f88, f89 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f104, f105 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f120, f121 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f72, f64
	FSUB	f65  = f73, f65
	FSUB	f80  = f88, f80
	FSUB	f81  = f89, f81
	FSUB	f96  = f104, f96
	FSUB	f97  = f105, f97
	FSUB	f112 = f120, f112
	FSUB	f113 = f121, f113
	;;
#endif

#ifdef LN
	LDFPD	f120, f121 = [AOFFSET]
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	FMPY	f34  = f120, f80
	FMPY	f35  = f121, f80
	FMPY	f36  = f120, f96
	FMPY	f37  = f121, f96
	FMPY	f38  = f120, f112
	FMPY	f39  = f121, f112
	;;
	FMA_C	f64  = f121, f65,  f32
	FMA_D	f65  = f120, f65,  f33
	FMA_C	f80  = f121, f81,  f34
	FMA_D	f81  = f120, f81,  f35
	FMA_C	f96  = f121, f97,  f36
	FMA_D	f97  = f120, f97,  f37
	FMA_C	f112 = f121, f113, f38
	FMA_D	f113 = f120, f113, f39
	;;
#endif

#ifdef LT
	LDFPD	f90, f91 = [AOFFSET]
	;;
	FMPY	f32  = f90, f64
	FMPY	f33  = f91, f64
	FMPY	f34  = f90, f80
	FMPY	f35  = f91, f80
	FMPY	f36  = f90, f96
	FMPY	f37  = f91, f96
	FMPY	f38  = f90, f112
	FMPY	f39  = f91, f112
	;;
	FMA_C	f64  = f91, f65,  f32
	FMA_D	f65  = f90, f65,  f33
	FMA_C	f80  = f91, f81,  f34
	FMA_D	f81  = f90, f81,  f35
	FMA_C	f96  = f91, f97,  f36
	FMA_D	f97  = f90, f97,  f37
	FMA_C	f112 = f91, f113, f38
	FMA_D	f113 = f90, f113, f39
	;;
#endif

#ifdef RN
	LDFPD	f72, f73 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f74, f75 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f76, f77 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f78, f79 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f92, f93 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f94, f95 = [BOFFSET]
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f108, f109 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f110, f111 = [BOFFSET]
	adds	BOFFSET = 8 * SIZE, BOFFSET
	;;
	LDFPD	f126, f127 = [BOFFSET]
	adds	BOFFSET = - 30 * SIZE, BOFFSET
	;;

	FMPY	f32  = f72, f64
	FMPY	f33  = f73, f64
	;;
	FMA_C	f64  = f73, f65, f32
	FMA_D	f65  = f72, f65, f33
	;;
	FNMA	f80  = f74, f64, f80
	FMA_A	f81  = f75, f64, f81
	;;
	FMA_B	f80  = f75, f65, f80
	FNMA	f81  = f74, f65, f81

	;;
	FNMA	f96  = f76, f64, f96
	FMA_A	f97  = f77, f64, f97
	;;
	FMA_B	f96  = f77, f65, f96
	FNMA	f97  = f76, f65, f97
	;;
	FNMA	f112 = f78, f64, f112
	FMA_A	f113 = f79, f64, f113
	;;
	FMA_B	f112 = f79, f65, f112
	FNMA	f113 = f78, f65, f113
	;;
	FMPY	f32  = f90, f80
	FMPY	f33  = f91, f80
	;;
	FMA_C	f80  = f91, f81,  f32
	FMA_D	f81  = f90, f81,  f33
	;;

	FNMA	f96  = f92, f80, f96
	FMA_A	f97  = f93, f80, f97
	;;
	FMA_B	f96  = f93, f81, f96
	FNMA	f97  = f92, f81, f97
	;;
	FNMA	f112 = f94, f80, f112
	FMA_A	f113 = f95, f80, f113
	;;
	FMA_B	f112 = f95, f81, f112
	FNMA	f113 = f94, f81, f113
	;;
	FMPY	f32  = f108, f96
	FMPY	f33  = f109, f96
	;;
	FMA_C	f96  = f109, f97,  f32
	FMA_D	f97  = f108, f97,  f33
	;;
	FNMA	f112 = f110, f96, f112
	FMA_A	f113 = f111, f96, f113
	;;
	FMA_B	f112 = f111, f97, f112
	FNMA	f113 = f110, f97, f113
	;;
	FMPY	f32  = f126, f112
	FMPY	f33  = f127, f112
	;;
	FMA_C	f112 = f127, f113, f32
	FMA_D	f113 = f126, f113, f33
	;;
#endif

#ifdef RT
	adds	BOFFSET = 30 * SIZE, BOFFSET
	;;
	LDFPD	f72, f73 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f74, f75 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f76, f77 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f78, f79 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f88, f89 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f90, f91 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f92, f93 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	LDFPD	f104, f105 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f106, f107 = [BOFFSET]
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
	LDFPD	f120, f121 = [BOFFSET]
	;;
	FMPY	f32  = f72, f112
	FMPY	f33  = f73, f112
	;;
	FMA_C	f112 = f73, f113, f32
	FMA_D	f113 = f72, f113, f33
	;;
	FNMA	f96  = f74, f112, f96
	FMA_A	f97  = f75, f112, f97
	;;
	FMA_B	f96  = f75, f113, f96
	FNMA	f97  = f74, f113, f97
	;;
	FNMA	f80  = f76, f112, f80
	FMA_A	f81  = f77, f112, f81
	;;
	FMA_B	f80  = f77, f113, f80
	FNMA	f81  = f76, f113, f81
	;;
	FNMA	f64  = f78, f112, f64
	FMA_A	f65  = f79, f112, f65
	;;
	FMA_B	f64  = f79, f113, f64
	FNMA	f65  = f78, f113, f65
	;;
	FMPY	f32  = f88, f96
	FMPY	f33  = f89, f96
	;;
	FMA_C	f96  = f89, f97, f32
	FMA_D	f97  = f88, f97, f33
	;;
	FNMA	f80  = f90, f96, f80
	FMA_A	f81  = f91, f96, f81
	;;
	FMA_B	f80  = f91, f97, f80
	FNMA	f81  = f90, f97, f81
	;;
	FNMA	f64  = f92, f96, f64
	FMA_A	f65  = f93, f96, f65
	;;
	FMA_B	f64  = f93, f97, f64
	FNMA	f65  = f92, f97, f65
	;;
	FMPY	f32  = f104, f80
	FMPY	f33  = f105, f80
	;;
	FMA_C	f80  = f105, f81, f32
	FMA_D	f81  = f104, f81, f33
	;;
	FNMA	f64  = f106, f80, f64
	FMA_A	f65  = f107, f80, f65
	;;
	FMA_B	f64  = f107, f81, f64
	FNMA	f65  = f106, f81, f65
	;;
	FMPY	f32  = f120, f64
	FMPY	f33  = f121, f64
	;;
	FMA_C	f64  = f121, f65, f32
	FMA_D	f65  = f120, f65, f33
	;;
#endif

#if defined(LN) || defined(LT)
	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;
	STFD	[BOFFSET]  = f64,  SIZE
	STFD	[BOFFSET2] = f96,  SIZE
	;;
	STFD	[BOFFSET]  = f65,  SIZE
	STFD	[BOFFSET2] = f97,  SIZE
	;;
	STFD	[BOFFSET]  = f80,  SIZE
	STFD	[BOFFSET2] = f112, SIZE
	;;
	STFD	[BOFFSET]  = f81,  5 * SIZE
	STFD	[BOFFSET2] = f113, 5 * SIZE
	;;
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
#else
	adds	AOFFSET2 = 4 * SIZE, AOFFSET
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	STFD	[AOFFSET2] = f96,  SIZE
	;;
	STFD	[AOFFSET]  = f65,  SIZE
	STFD	[AOFFSET2] = f97,  SIZE
	;;
	STFD	[AOFFSET]  = f80,  SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET]  = f81,  5 * SIZE
	STFD	[AOFFSET2] = f113, 5 * SIZE
	;;
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
#endif

#ifdef LN
	adds	C1 = -2 * SIZE, C1
	adds	C2 = -2 * SIZE, C2
	adds	C3 = -2 * SIZE, C3
	adds	C4 = -2 * SIZE, C4
#endif
	;;
	STFD	[C1 ] = f64, SIZE
	;;
	STFD	[C1 ] = f65, SIZE
	;;
	STFD	[C2 ] = f80, SIZE
	;;
	STFD	[C2 ] = f81, SIZE
	;;
	STFD	[C3 ] = f96, SIZE
	;;
	STFD	[C3 ] = f97, SIZE
	;;
	STFD	[C4 ] = f112, SIZE
	;;
	STFD	[C4 ] = f113, SIZE
	;;
	mov	f64 = f0
	mov	f65 = f0
	mov	f80 = f0
	mov	f81 = f0
	mov	f96 = f0
	mov	f97 = f0
	mov	f112 = f0
	mov	f113 = f0
	;;
#ifdef LN
	adds	C1 = -2 * SIZE, C1
	adds	C2 = -2 * SIZE, C2
	adds	C3 = -2 * SIZE, C3
	adds	C4 = -2 * SIZE, C4
#endif
	;;
	cmp.ne	p6, p0 = 1, I
	;;
	adds	I = -1, I
	;;
	shladd	r2 = K, ZBASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	add	AORIG = r2, AORIG
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, ZBASE_SHIFT, r0
	;;
	add	AOFFSET = L,    AOFFSET
	shladd	BOFFSET = L, 2, BOFFSET
#endif
	;;
#ifdef LT
	adds	KK =  1, KK
#elif defined LN
	adds	KK = -1, KK
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	.align 16

.L049:
#ifdef LN
	shladd	KK8 = K, ZBASE_SHIFT, r0
	;;
       shladd	B = KK8, 2, B
#endif

#if defined(LT) || defined(RN)
	mov	B =  BOFFSET
#endif

#ifdef RN
	adds	KK =  4,  KK
#endif

#ifdef RT
	adds	KK = -4,  KK
#endif
	;;

	{ .mmb
	mov	AOFFSET = A
	cmp.lt	p6, p0 = 0, J
	(p6)	br.cond.dptk .L010x
	}
	;;
	.align 16

.L999:
	{ .mii
	nop	__LINE__
	mov	 ar.lc = ARLC
	mov pr    = PR, -1
	}
       { .mib
	nop	__LINE__
#ifdef TRMMKERNEL
	mov	ar.pfs = ARPFS
#else
	nop	__LINE__
#endif
	br.ret.sptk.many b0
	}
	EPILOGUE

