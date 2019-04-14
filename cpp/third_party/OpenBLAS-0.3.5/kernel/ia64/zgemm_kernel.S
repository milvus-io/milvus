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

#define CPREFETCHSIZE 7
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

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(CC) || defined(CR) || defined(RC) || defined(RR)
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

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(NC) || defined(TC) || defined(NR) || defined(TR)
#define FCALC_C	  FMA
#define FCALC_D	  FNMA
#else
#define FCALC_C	  FNMA
#define FCALC_D	  FMA
#endif

	PROLOGUE
	.prologue
	PROFCODE

	{ .mfi
#ifdef TRMMKERNEL
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 8, 0, 0
#else
	nop	__LINE__
#endif
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
#ifdef TRMMKERNEL
	ld8	OFFSET = [r15]
#else
	nop	__LINE__
#endif
	mov	f96  = f0
	shr	J = N, 2
	}
	;;
	{ .mfi
	shladd	LDC = LDC, ZBASE_SHIFT, r0
	mov	f97  = f0
	mov	AOFFSET = A
	}
	{ .mfi
	nop	__LINE__
	mov	f113 = f0
#if defined(TRMMKERNEL) && !defined(LEFT)
	sub	KK  = r0, OFFSET
#endif
	}
	;;
	.body
	{ .mfi
	nop	__LINE__
	mov	f80  = f0
	mov	ARLC  = ar.lc
	}
	{ .mfb
	cmp.ge	p6, p0  = 0, J
	mov	f112 = f0
	(p6)	br.cond.dpnt .L050
	}
	;;
	.align 16

.L010:
	{ .mmi
	mov	C1 = C			// coffset1 = c + 0 * ldc
	add	C2 = LDC, C		// coffset2 = c + 1 * ldc
	shr	I  = M, 2
	}
	{ .mmi
	adds	J = -1, J
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	nop	__LINE__
	}
	;;
	{ .mmi
	shladd	C3 = LDC, 1, C		// coffset3 = c + 2 * ldc
	shladd	C4 = LDC, 1, C2		// coffset4 = c + 3 * ldc
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mib
	cmp.eq	p6, p7 = 0, I
	shladd	C = LDC, 2, C		// coffset += 8 * ldc
	(p6)	br.cond.dpnt .L020
	}
	;;
	.align 16

.L011:
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mfb
	adds	BOFFSET = 2 * SIZE, B
	mov	f67  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 2, B
	mov	f66  = f0
	shladd	AOFFSET = KK8, 2, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	nop	__LINE__
	}
	;;
#endif
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 4, KK
#else
	adds	L = 4, KK
#endif
#endif
	}
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83  = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	;;
	{ .mfi
	LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	mov	f98  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	{ .mfi
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	mov	f99  = f0
	adds	C5 = 4 * SIZE, C1
	}
	;;
	{ .mfi
	LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	mov	f114 = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f115 = f0
	adds	C6 = 4 * SIZE, C2
	}
	;;
	{ .mfi
	LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
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
	nop	__LINE__
	}
	{ .mmf
	setf.d	f118 = r0
	setf.d	f119 = r0
 	mov	f71  = f0
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C5], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f112  = f41, f63, f112	// A2 * B8
	nop	__LINE__
	}
	;;
 /* 41 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77 = [C5], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f67   = f42, f57, f67	// A3 * B2
	nop	__LINE__
	}
	;;
/* 42 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f74 = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f78 = [C5], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f83   = f42, f59, f83	// A3 * B4
	nop	__LINE__
	}
	;;
/* 43 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f75 = [C1], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f79 = [C5], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f99  = f42, f61, f99	// A3 * B6
	nop	__LINE__
	}
	;;
/* 44 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f88 = [C2], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92 = [C6], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f115  = f42, f63, f115	// A3 * B8
	nop	__LINE__
	}
	;;
/* 45 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f89 = [C2], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f93 = [C6], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f66   = f43, f57, f66	// A4 * B2
	nop	__LINE__
	}
	;;
/* 46 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f90 = [C2], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f94 = [C6], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f82   = f43, f59, f82	// A4 * B4
	nop	__LINE__
	}
	;;
/* 47 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f91 = [C2], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f95 = [C6], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f98  = f43, f61, f98	// A4 * B6
	nop	__LINE__
	}
	;;
/* 48 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f104 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f108 = [C7], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f114  = f43, f63, f114	// A4 * B8
	nop	__LINE__
	}
	;;
/* 49 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f105 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f109 = [C7], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f69   = f44, f57, f69	// A5 * B2
	nop	__LINE__
	}
	;;
/* 50 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f106 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f110 = [C7], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f85   = f44, f59, f85	// A5 * B4
	nop	__LINE__
	}
	;;
/* 51 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f107 = [C3], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f100  = f44, f60, f100	// A5 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f111 = [C7], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f101  = f44, f61, f101	// A5 * B6
	nop	__LINE__
	}
	;;
/* 52 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f120 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f116  = f44, f62, f116	// A5 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f124 = [C8], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f117  = f44, f63, f117	// A5 * B8
	nop	__LINE__
	}
	;;
/* 53 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f121 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f125 = [C8], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f68   = f45, f57, f68	// A6 * B2
	nop	__LINE__
	}
	;;
/* 54 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f122 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f126 = [C8], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f84   = f45, f59, f84	// A6 * B4
	nop	__LINE__
	}
	;;
/* 55 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f123 = [C4], -3 * SIZE
#else
	nop	__LINE__
#endif
 	(p3) FMA	f101  = f45, f60, f101	// A6 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f127 = [C8], -3 * SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f76 = ALPHA_R, f68, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f77 = ALPHA_R, f69, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f66, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f78 = ALPHA_R, f70, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f67, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f79 = ALPHA_R, f71, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f76 = ALPHA_I, f69, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77 = ALPHA_I, f68, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f67, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f78 = ALPHA_I, f71, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f66, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79 = ALPHA_I, f70, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72, SIZE
	FMA	f88 = ALPHA_R, f80, f88
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f76, SIZE
	FMA	f92 = ALPHA_R, f84, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73, SIZE
	FCALC_C	f89 = ALPHA_R, f81, f89
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f77, SIZE
	FCALC_C	f93 = ALPHA_R, f85, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	FMA	f90 = ALPHA_R, f82, f90
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f78, SIZE
	FMA	f94 = ALPHA_R, f86, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f75, 5 * SIZE
	FCALC_C	f91 = ALPHA_R, f83, f91
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f79, 5 * SIZE
	FCALC_C	f95 = ALPHA_R, f87, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f92 = ALPHA_I, f85, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f93 = ALPHA_I, f84, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f90 = ALPHA_I, f83, f90
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f94 = ALPHA_I, f87, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f91 = ALPHA_I, f82, f91
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f95 = ALPHA_I, f86, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f88, SIZE
	FMA	f104 = ALPHA_R, f96,  f104
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f92, SIZE
	FMA	f108 = ALPHA_R, f100, f108
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f89, SIZE
	FCALC_C	f105 = ALPHA_R, f97,  f105
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f93, SIZE
	FCALC_C	f109 = ALPHA_R, f101, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f90, SIZE
	FMA	f106 = ALPHA_R, f98,  f106
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f94, SIZE
	FMA	f110 = ALPHA_R, f102, f110
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f91, 5 * SIZE
	FCALC_C	f107 = ALPHA_R, f99,  f107
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f95, 5 * SIZE
	FCALC_C	f111 = ALPHA_R, f103, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f104 = ALPHA_I, f97,  f104
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f108 = ALPHA_I, f101, f108
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f105 = ALPHA_I, f96,  f105
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f109 = ALPHA_I, f100, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f106 = ALPHA_I, f99,  f106
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
 	FCALC_D	f110 = ALPHA_I, f103, f110
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f107 = ALPHA_I, f98,  f107
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f111 = ALPHA_I, f102, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f104, SIZE
	FMA	f120 = ALPHA_R, f112, f120
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f108, SIZE
	FMA	f124 = ALPHA_R, f116, f124
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f105, SIZE
	FCALC_C	f121 = ALPHA_R, f113, f121
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f109, SIZE
	FCALC_C	f125 = ALPHA_R, f117, f125
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f106, SIZE
	FMA	f122 = ALPHA_R, f114, f122
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f110, SIZE
	FMA	f126 = ALPHA_R, f118, f126
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f107, 5 * SIZE
	FCALC_C	f123 = ALPHA_R, f115, f123
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f111, 5 * SIZE
	FCALC_C	f127 = ALPHA_R, f119, f127
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f120 = ALPHA_I, f113, f120
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f124 = ALPHA_I, f117, f124
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f121 = ALPHA_I, f112, f121
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f125 = ALPHA_I, f116, f125
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f122 = ALPHA_I, f115, f122
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f126 = ALPHA_I, f119, f126
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f123 = ALPHA_I, f114, f123
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	nop	__LINE__
	FMA	f127 = ALPHA_I, f118, f127
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4] = f120, SIZE
	mov	f64  = f0
	adds	I = -1, I
	}
	{ .mfb
	STFD	[C8] = f124, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4] = f121, SIZE
	mov	f80  = f0
	and	TEMP = 3, M
	}
	{ .mfb
	STFD	[C8] = f125, SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4] = f122, SIZE
	mov	f96  = f0
	cmp.ne	p8, p9 = r0, TEMP
	}
	{ .mfb
	STFD	[C8] = f126, SIZE
	mov	f97  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4] = f123, 5 * SIZE
	mov	f112 = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C8] = f127, 5 * SIZE
	mov	f113  = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMPY	f72  = ALPHA_R, f64
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f76 = ALPHA_R, f68
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f77 = ALPHA_R, f69, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f74 = ALPHA_R, f66
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f78 = ALPHA_R, f70
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f67, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f79 = ALPHA_R, f71, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f76 = ALPHA_I, f69, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77 = ALPHA_I, f68, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f67, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f78 = ALPHA_I, f71, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f66, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79 = ALPHA_I, f70, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72,  SIZE
	FMPY	f88 = ALPHA_R, f80
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f76, SIZE
	FMPY	f92 = ALPHA_R, f84
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73,  SIZE
	FCALC_C	f89 = ALPHA_R, f81, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f77, SIZE
	FCALC_C	f93 = ALPHA_R, f85, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	FMPY	f90 = ALPHA_R, f82
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f78, SIZE
	FMPY	f94 = ALPHA_R, f86
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f75, 5 * SIZE
	FCALC_C	f91 = ALPHA_R, f83, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f79, 5 * SIZE
	FCALC_C	f95 = ALPHA_R, f87, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f92 = ALPHA_I, f85, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f93 = ALPHA_I, f84, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f90 = ALPHA_I, f83, f90
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f94 = ALPHA_I, f87, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f91 = ALPHA_I, f82, f91
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f95 = ALPHA_I, f86, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f88, SIZE
	FMPY	f104 = ALPHA_R, f96
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f92, SIZE
	FMPY	f108 = ALPHA_R, f100
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f89, SIZE
	FCALC_C	f105 = ALPHA_R, f97,  f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f93, SIZE
	FCALC_C	f109 = ALPHA_R, f101, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f90, SIZE
	FMPY	f106 = ALPHA_R, f98
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f94, SIZE
	FMPY	f110 = ALPHA_R, f102
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f91, 5 * SIZE
	FCALC_C	f107 = ALPHA_R, f99,  f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f95, 5 * SIZE
	FCALC_C	f111 = ALPHA_R, f103, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f104 = ALPHA_I, f97,  f104
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f108 = ALPHA_I, f101, f108
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f105 = ALPHA_I, f96,  f105
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f109 = ALPHA_I, f100, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f106 = ALPHA_I, f99,  f106
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
 	FCALC_D	f110 = ALPHA_I, f103, f110
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f107 = ALPHA_I, f98,  f107
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f111 = ALPHA_I, f102, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f104, SIZE
	FMPY	f120 = ALPHA_R, f112
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f108, SIZE
	FMPY	f124 = ALPHA_R, f116
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f105, SIZE
	FCALC_C	f121 = ALPHA_R, f113, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f109, SIZE
	FCALC_C	f125 = ALPHA_R, f117, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f106, SIZE
	FMPY	f122 = ALPHA_R, f114
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f110, SIZE
	FMPY	f126 = ALPHA_R, f118
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f107, 5 * SIZE
	FCALC_C	f123 = ALPHA_R, f115, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C7] = f111, 5 * SIZE
	FCALC_C	f127 = ALPHA_R, f119, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f120 = ALPHA_I, f113, f120
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f124 = ALPHA_I, f117, f124
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f121 = ALPHA_I, f112, f121
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	nop	__LINE__
	FMA	f125 = ALPHA_I, f116, f125
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f122 = ALPHA_I, f115, f122
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FCALC_D	f126 = ALPHA_I, f119, f126
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f123 = ALPHA_I, f114, f123
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfi
	nop	__LINE__
	FMA	f127 = ALPHA_I, f118, f127
	adds	I = -1, I
	}
	;;
	{ .mfi
	STFD	[C4] = f120, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C8] = f124, SIZE
	mov	f65  = f0
	and	TEMP = 3, M
	}
	;;
	{ .mfi
	STFD	[C4] = f121, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C8] = f125, SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C4] = f122, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C8] = f126, SIZE
	mov	f97  = f0
	cmp.ne	p8, p9 = r0, TEMP
	}
	;;
	{ .mfi
	STFD	[C4] = f123, 5 * SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C8] = f127, 5 * SIZE
	mov	f113  = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#endif

.L020:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 2, KK
#else
	adds	L = 4, KK
#endif
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L030
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f66 = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
	mov	f67 = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 2, B
	mov	f66 = f0
	shladd	AOFFSET = KK8, 1, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67 = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82 = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83 = f0
	shr	L = L, 1
	}
	;;
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f98 = f0
	adds	L =  -1, L
	}
	{ .mfi
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	mov	f99 = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f114 = f0
	mov	ar.lc = L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	mov	f115 = f0
	nop	__LINE__
	}
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f88 = [C2], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f97  = f40, f61, f97	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f89 = [C2], SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f74 = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f90 = [C2], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f96  = f41, f61, f96	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f75 = [C1], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f91 = [C2], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f112  = f41, f63, f112	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f104 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f120 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f67   = f42, f57, f67	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f105 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f121 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f83   = f42, f59, f83	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f106 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f122 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f99  = f42, f61, f99	// A3 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f107 = [C3], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f123 = [C4], -3 * SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f88 = ALPHA_R, f80, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f89 = ALPHA_R, f81, f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f66, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f90 = ALPHA_R, f82, f90
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f67, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f91 = ALPHA_R, f83, f91
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81,  f88
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	FMA	f89 = ALPHA_I, f80,  f89
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f67, f74
	nop	__LINE__
	}
	{ .mfb
	FCALC_D	f90 = ALPHA_I, f83,  f90
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f66, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f91 = ALPHA_I, f82,  f91
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72, SIZE
	FMA	f104 = ALPHA_R, f96,  f104
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f88, SIZE
	FMA	f120 = ALPHA_R, f112, f120
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73, SIZE
	FCALC_C	f105 = ALPHA_R, f97,  f105
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f89, SIZE
	FCALC_C	f121 = ALPHA_R, f113, f121
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	FMA	f106 = ALPHA_R, f98,  f106
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f90, SIZE
	FMA	f122 = ALPHA_R, f114, f122
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f75, SIZE
	FCALC_C	f107 = ALPHA_R, f99,  f107
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f91, SIZE
	FCALC_C	f123 = ALPHA_R, f115, f123
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f104 = ALPHA_I, f97,  f104
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f120 = ALPHA_I, f113, f120
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f105 = ALPHA_I, f96,  f105
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f121 = ALPHA_I, f112, f121
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f106 = ALPHA_I, f99,  f106
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f122 = ALPHA_I, f115, f122
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f107 = ALPHA_I, f98,  f107
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f123 = ALPHA_I, f114, f123
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f104, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4] = f120, SIZE
	mov	f65  = f0
	}
	;;
	{ .mfb
	STFD	[C3] = f105, SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4] = f121, SIZE
	mov	f81  = f0
	}
	;;
	{ .mfb
	STFD	[C3] = f106, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4] = f122, SIZE
	mov	f97 = f0
	}
	;;
	{ .mfi
	STFD	[C3] = f107, SIZE
	mov	f112 = f0
	}
	{ .mfb
	STFD	[C4] = f123, SIZE
	mov	f113 = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMPY	f72  = ALPHA_R, f64
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f88 = ALPHA_R, f80
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f89 = ALPHA_R, f81, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f74 = ALPHA_R, f66
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f90 = ALPHA_R, f82
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f67, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f91 = ALPHA_R, f83, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81,  f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	FMA	f89 = ALPHA_I, f80,  f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f67, f74
	nop	__LINE__
	}
	{ .mfb
	FCALC_D	f90 = ALPHA_I, f83,  f90
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f66, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f91 = ALPHA_I, f82,  f91
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72, SIZE
	FMPY	f104 = ALPHA_R, f96
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f88, SIZE
	FMPY	f120 = ALPHA_R, f112
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73, SIZE
	FCALC_C	f105 = ALPHA_R, f97,  f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f89, SIZE
	FCALC_C	f121 = ALPHA_R, f113, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	FMPY	f106 = ALPHA_R, f98
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f90, SIZE
	FMPY	f122 = ALPHA_R, f114
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f75, SIZE
	FCALC_C	f107 = ALPHA_R, f99,  f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f91, SIZE
	FCALC_C	f123 = ALPHA_R, f115, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f104 = ALPHA_I, f97,  f104
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f120 = ALPHA_I, f113, f120
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f105 = ALPHA_I, f96,  f105
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f121 = ALPHA_I, f112, f121
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f106 = ALPHA_I, f99,  f106
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f122 = ALPHA_I, f115, f122
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f107 = ALPHA_I, f98,  f107
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMA	f123 = ALPHA_I, f114, f123
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C3] = f104, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4] = f120, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3] = f105, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4] = f121, SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C3] = f106, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4] = f122, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3] = f107, SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C4] = f123, SIZE
	mov	f113 = f0
	nop	__LINE__
	}
	;;
#endif
	.align 16

.L030:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 1, KK
#else
	adds	L = 4, KK
#endif
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L049
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f72  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
	mov	f73  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 2, B
	mov	f72  = f0
	add	AOFFSET = KK8, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f73  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	{ .mmi
	nop	__LINE__
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	tbit.z	p12, p0 = L, 0
	}
	;;
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
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
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
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
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f120 = f0
	mov	ar.lc = L
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f121 = f0
	nop	__LINE__
	}
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f88 = [C2], SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1], - SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f89 = [C2], - SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f104 = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f120 = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f96  = f41, f61, f96	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f105 = [C3], - SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f121 = [C4], - SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f112  = f41, f63, f112	// A2 * B8
	br.cloop.sptk.few .L032
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f88 = ALPHA_R, f80, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f89 = ALPHA_R, f81, f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f104 = ALPHA_R, f96,  f104
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f120 = ALPHA_R, f112, f120
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f105 = ALPHA_R, f97,  f105
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f121 = ALPHA_R, f113, f121
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81,  f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	FMA	f89 = ALPHA_I, f80,  f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f104 = ALPHA_I, f97,  f104
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f120 = ALPHA_I, f113, f120
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f105 = ALPHA_I, f96,  f105
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f121 = ALPHA_I, f112, f121
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72,  SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f88, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f89, SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f104, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4] = f120, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3] = f105, SIZE
	mov	f112 = f0
	nop	__LINE__
	}
	{ .mfi
	STFD	[C4] = f121, SIZE
	mov	f113 = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f88 = ALPHA_R, f80, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f89 = ALPHA_R, f81, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f104 = ALPHA_R, f96,  f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f120 = ALPHA_R, f112, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f105 = ALPHA_R, f97,  f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f121 = ALPHA_R, f113, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81,  f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	FMA	f89 = ALPHA_I, f80,  f89
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f104 = ALPHA_I, f97,  f104
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f120 = ALPHA_I, f113, f120
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f105 = ALPHA_I, f96,  f105
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMA	f121 = ALPHA_I, f112, f121
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f72,  SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2] = f88, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	AOFFSET = KK8, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C2] = f89, SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C3] = f104, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 1, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4] = f120, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3] = f105, SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4] = f121, SIZE
	mov	f113 = f0
	nop	__LINE__
	}
	;;
#endif
	.align 16

.L049:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
#if defined(TRMMKERNEL) && !defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mmb
	nop	__LINE__
	cmp.lt	p6, p0 = 0, J
	(p6)	br.cond.dptk .L010
	}
	;;
	.align 16

.L050:
	{ .mmi
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	shr	I  = M, 2
	}
	{ .mib
	mov	C1 = C
	tbit.z	p6, p0 = N, 1
	(p6)	br.cond.dpnt .L090
	}
	;;
	{ .mmi
	add	C2 = LDC, C
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	nop	__LINE__
	}
	{ .mib
	cmp.eq	p6, p7 = 0, I
	shladd	C = LDC, 1, C
	(p6)	br.cond.dpnt .L060
	}
	;;
	.align 16

.L052:
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfi
	LDFPD	f48, f49 = [B]
	mov	f66  = f0
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	}
	{ .mfb
	adds	BOFFSET = 2 * SIZE, B
	mov	f67  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 1, B
	mov	f66  = f0
	shladd	AOFFSET = KK8, 2, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	}
	;;
#endif
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82  = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 4, KK
#else
	adds	L = 2, KK
#endif
#endif
	}
	;;
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f98  = f0
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f99  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mfi
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
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
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f80   = f41, f59, f80	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f74 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f97   = f43, f56, f97	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f78 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f96   = f43, f57, f96	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f75 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113   = f43, f58, f113	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f79 = [C5 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f112   = f43, f59, f112	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f88 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f44, f56, f66	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f67   = f44, f57, f67	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f89 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f44, f58, f82	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f93 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f83   = f44, f59, f83	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f90 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f98   = f46, f56, f98	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f94 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f99   = f46, f57, f99	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f91 = [C2 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f114   = f46, f58, f114	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f95 = [C6 ], -3 * SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f76 = ALPHA_R, f66, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f77 = ALPHA_R, f67, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f96, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f78 = ALPHA_R, f98, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f97, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f79 = ALPHA_R, f99, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f76 = ALPHA_I, f67, f76
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77 = ALPHA_I, f66, f77
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f97, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f78 = ALPHA_I, f99, f78
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f96, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79 = ALPHA_I, f98, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72, SIZE
	FMA	f88 = ALPHA_R, f80, f88
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f76, SIZE
	FMA	f92 = ALPHA_R, f82, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73, SIZE
	FCALC_C	f89 = ALPHA_R, f81, f89
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f77, SIZE
	FCALC_C	f93 = ALPHA_R, f83, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	FMA	f90 = ALPHA_R, f112, f90
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f78, SIZE
	FMA	f94 = ALPHA_R, f114, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f75, 5 * SIZE
	FCALC_C	f91 = ALPHA_R, f113, f91
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f79, 5 * SIZE
	FCALC_C	f95 = ALPHA_R, f115, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f92 = ALPHA_I, f83, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f93 = ALPHA_I, f82, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f90 = ALPHA_I, f113, f90
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f94 = ALPHA_I, f115, f94
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f91 = ALPHA_I, f112, f91
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	nop	__LINE__
	FMA	f95 = ALPHA_I, f114, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f88, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f92, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2] = f89, SIZE
	mov	f80  = f0
	adds	I = -1, I
	}
	{ .mfb
	STFD	[C6] = f93, SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f90, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f94, SIZE
	mov	f97  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2] = f91, 5 * SIZE
	mov	f112  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6] = f95, 5 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L052
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f76 = ALPHA_R, f66, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f77 = ALPHA_R, f67, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f96, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f78 = ALPHA_R, f98, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f97, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f79 = ALPHA_R, f99, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f76 = ALPHA_I, f67, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77 = ALPHA_I, f66, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f97, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f78 = ALPHA_I, f99, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f96, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79 = ALPHA_I, f98, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72, SIZE
	FMA	f88 = ALPHA_R, f80, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f76, SIZE
	FMA	f92 = ALPHA_R, f82, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73, SIZE
	FCALC_C	f89 = ALPHA_R, f81, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f77, SIZE
	FCALC_C	f93 = ALPHA_R, f83, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	FMA	f90 = ALPHA_R, f112, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f78, SIZE
	FMA	f94 = ALPHA_R, f114, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f75, 5 * SIZE
	FCALC_C	f91 = ALPHA_R, f113, f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f79, 5 * SIZE
	FCALC_C	f95 = ALPHA_R, f115, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f92 = ALPHA_I, f83, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f93 = ALPHA_I, f82, f93
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f90 = ALPHA_I, f113, f90
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FCALC_D	f94 = ALPHA_I, f115, f94
	cmp.ne	p6, p0 = 1, I
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f91 = ALPHA_I, f112, f91
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMA	f95 = ALPHA_I, f114, f95
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C2] = f88, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C6] = f92, SIZE
	mov	f65  = f0
	adds	I = -1, I
	}
	;;
	{ .mfi
	STFD	[C2] = f89, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C6] = f93, SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C2] = f90, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C6] = f94, SIZE
	mov	f97  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2] = f91, 5 * SIZE
	mov	f112  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C6] = f95, 5 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L052
	}
	;;
#endif
	.align 16

.L060:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 2, KK
#else
	adds	L = 2, KK
#endif
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L070
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	nop	__LINE__
	}
	{ .mmi
	adds	BOFFSET = 2 * SIZE, B
	cmp.eq	p3, p0 = r0, r0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mmi
	shladd	BOFFSET = KK8, 1, B
	shladd	AOFFSET = KK8, 1, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	{ .mmi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	tbit.z	p12, p0 = L, 0
	}
	{ .mmi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f96   = f42, f56, f96	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f88 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f97   = f42, f57, f97	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f112   = f42, f58, f112	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f89 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_B	f113   = f42, f59, f113	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f74 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f90 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f75 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f91 = [C2 ], -3 * SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f88 = ALPHA_R, f80, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f89 = ALPHA_R, f81, f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f96, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f90 = ALPHA_R, f112, f90
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f97, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f91 = ALPHA_R, f113, f91
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f97, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f90 = ALPHA_I, f113, f90
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f96, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f91 = ALPHA_I, f112, f91
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f72,  SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f88, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2] = f89, SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f74, SIZE
	mov	f96  = f0
	adds	L =  1, K
	}
	{ .mfb
	STFD	[C2] = f90, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f75, SIZE
	mov	f112 = f0
	shr	L = L, 1
	}
	{ .mfb
	STFD	[C2] = f91, SIZE
	mov	f113 = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f88 = ALPHA_R, f80, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f89 = ALPHA_R, f81, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f96, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f90 = ALPHA_R, f112, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f97, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f91 = ALPHA_R, f113, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f97, f74
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f90 = ALPHA_I, f113, f90
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f75 = ALPHA_I, f96, f75
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMA	f91 = ALPHA_I, f112, f91
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f72,  SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2] = f88, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C2] = f89, SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f74, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2] = f90, SIZE
	mov	f97 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f75, SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2] = f91, SIZE
	mov	f113 = f0
	nop	__LINE__
	}
	;;
#endif
	.align 16

.L070:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 1, KK
#else
	adds	L = 2, KK
#endif
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L089
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mmi
	shladd	BOFFSET = KK8, 1, B
	add	AOFFSET = KK8, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	nop	__LINE__
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	;;
	{ .mii
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72 = [C1 ], SIZE
	(p5) LDFD	f88 = [C2 ], SIZE
#else
	nop	__LINE__
	nop	__LINE__
#endif
	(p3) FMA	f112   = f40, f59, f112	// A1 * B4
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1 ], - SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f89 = [C2 ], - SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	setf.d	f96 = r0
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	setf.d	f97 = r0
	FMA	f88 = ALPHA_R, f80, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	setf.d	f112 = r0
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	setf.d	f113 = r0
	FCALC_C	f89 = ALPHA_R, f81, f89
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	setf.d	f65 = r0
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	;;
	{ .mfb
	setf.d	f81 = r0
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	setf.d	f64 = r0
	FMA	f89 = ALPHA_I, f80, f89
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1] = f72, SIZE
	STFD	[C2] = f88, SIZE
	mov	f80 = f0
	}
	;;
	{ .mmi
	STFD	[C1] = f73,  SIZE
	STFD	[C2] = f89, SIZE
	mov	B = BOFFSET
	}
	;;
#else
	{ .mfi
	setf.d	f96 = r0
	FMA	f72  = ALPHA_R, f64, f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	setf.d	f97 = r0
	FMA	f88 = ALPHA_R, f80, f0
	nop	__LINE__
	}
	;;
	{ .mfi
	setf.d	f112 = r0
	FCALC_C	f73  = ALPHA_R, f65, f0
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	setf.d	f113 = r0
	FCALC_C	f89 = ALPHA_R, f81, f0
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	setf.d	f65 = r0
	FCALC_D	f88 = ALPHA_I, f81, f88
	nop	__LINE__
	}
	;;
	{ .mfi
	setf.d	f81 = r0
	FMA	f73  = ALPHA_I, f64, f73
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	AOFFSET = KK8, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	setf.d	f64 = r0
	FMA	f89 = ALPHA_I, f80, f89
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f80 = f0
	}
	;;
	{ .mmi
	STFD	[C1] = f72, SIZE
	STFD	[C2] = f88, SIZE
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 1, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
	STFD	[C1] = f73, SIZE
	STFD	[C2] = f89, SIZE
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
#endif
	;;
	.align 16

.L089:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
#if defined(TRMMKERNEL) && !defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	.align 16

.L090:
	{ .mfi
	mov	C1 = C
	mov	f64  = f0
	tbit.z	p6, p0 = N, 0
	}
	{ .mfi
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	mov	f72  = f0
	shr	I  = M, 2
	}
	;;
	{ .mfi
	setf.d	f66  = r0
	mov	f65  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	mov	AOFFSET = A
	mov	f73  = f0
	(p6)	br.cond.dpnt .L999
	}
	;;
	{ .mfi
	setf.d	f74  = r0
	mov	f67  = f0
	nop	__LINE__
	}
	{ .mfb
	cmp.eq	p6, p7 = 0, I
	mov	f75  = f0
	(p6)	br.cond.dpnt .L100
	}
	;;
	.align 16

.L092:
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFPD	f48, f49 = [B]
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 4, KK
#else
	adds	L = 1, KK
#endif
#endif
	}
	;;
#else
	{ .mfi
	add	BOFFSET = KK8, B
	shladd	AOFFSET = KK8, 2, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 4, KK
#else
	adds	L = 1, KK
#endif
#endif
	}
	;;
#endif
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
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
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	CPREFETCH [PREC]
	}
	;;
	{ .mfi
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	mov	ar.lc = L
	}
	{ .mmi
	adds	C5 = 4 * SIZE, C1
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72   = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76  = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f64   = f41, f57, f64	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73   = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f81   = f43, f56, f81	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77  = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f80   = f43, f57, f80	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f74  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f97   = f45, f56, f97	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f78  = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f96   = f45, f57, f96	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f75  = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113   = f47, f56, f113	// A8 * B1
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f79  = [C5 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA_A	f112   = f47, f57, f112	// A8 * B2
	br.cloop.sptk.few .L093
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f76 = ALPHA_R, f96, f76
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f77 = ALPHA_R, f97, f77
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f80, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f78 = ALPHA_R, f112, f78
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f81, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f79 = ALPHA_R, f113, f79
	nop	__LINE__
	}
	;;

	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f76 = ALPHA_I, f97, f76
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77 = ALPHA_I, f96, f77
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f81, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f78 = ALPHA_I, f113, f78
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f80, f75
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79 = ALPHA_I, f112, f79
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f72,  SIZE
	mov	f64  = f0
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	STFD	[C5] = f76, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
	adds	I = -1, I
	}
	{ .mfb
	STFD	[C5] = f77, SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1] = f74, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C5] = f78, SIZE
	mov	f97  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f75, 5 * SIZE
	mov	f112  = f0
	}
	{ .mfb
	STFD	[C5] = f79, 5 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L092
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMA	f6  = ALPHA_R, f64, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f76 = ALPHA_R, f96, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f77 = ALPHA_R, f97, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f80, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f78 = ALPHA_R, f112, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f81, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f79 = ALPHA_R, f113, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f6  = ALPHA_I, f65, f6
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f76 = ALPHA_I, f97, f76
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77 = ALPHA_I, f96, f77
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f81, f74
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	nop	__LINE__
	FCALC_D	f78 = ALPHA_I, f113, f78
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f75 = ALPHA_I, f80, f75
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMA	f79 = ALPHA_I, f112, f79
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f6,  SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C5] = f76, SIZE
	mov	f65  = f0
	cmp.ne	p6, p0 = 1, I
	}
	;;
	{ .mfi
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C5] = f77, SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	BOFFSET = KK8, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f74, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C5] = f78, SIZE
	mov	f97  = f0
	adds	I = -1, I
	}
	;;
	{ .mfi
	STFD	[C1] = f75, 5 * SIZE
	mov	f112  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C5] = f79, 5 * SIZE
	mov	f113 = f0
	(p6)	br.cond.dptk .L092
	}
	;;
#endif
	.align 16

.L100:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 2, KK
#else
	adds	L = 1, KK
#endif
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L110
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mii
	add	BOFFSET = KK8, B
	shladd	AOFFSET = KK8, 1, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	{ .mii
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f74 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f75 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f74 = ALPHA_R, f96, f74
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f75 = ALPHA_R, f97, f75
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	{ .mfb
	setf.d	f112 = r0
	FCALC_D	f74 = ALPHA_I, f97, f74
	nop	__LINE__
	}
	{ .mfb
	setf.d	f113 = r0
	FMA	f75 = ALPHA_I, f96, f75
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1] = f72,  SIZE
	setf.d	f97 = r0
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C1] = f73,  SIZE
	setf.d	f96 = r0
	mov	f80  = f0
	}
	;;
	{ .mfi
	STFD	[C1] = f74, SIZE
	mov	f65  = f0
	adds	L =  1, K
	}
	;;
	{ .mfi
	STFD	[C1] = f75, SIZE
	mov	f81 = f0
	shr	L = L, 1
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	;;
	{ .mfi
	setf.d	f112 = r0
	FMA	f74 = ALPHA_R, f96, f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	setf.d	f113 = r0
	FCALC_C	f75 = ALPHA_R, f97, f0
	nop	__LINE__
	}
	;;
	{ .mfi
	setf.d	f97 = r0
	FCALC_D	f72  = ALPHA_I, f65, f72
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	setf.d	f96 = r0
	FMA	f73  = ALPHA_I, f64, f73
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	nop	__LINE__
	FCALC_D	f74 = ALPHA_I, f97, f74
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	nop	__LINE__
	FMA	f75 = ALPHA_I, f96, f75
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1] = f72,  SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f73,  SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	BOFFSET = KK8, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f74, SIZE
	mov	f65  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1] = f75, SIZE
	mov	f81 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, ZBASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
#endif
	.align 16

.L110:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 1, KK
#else
	adds	L = 1, KK
#endif
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L119
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mii
	add	BOFFSET = KK8, B
	add	AOFFSET = KK8, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	;;
	{ .mii
	nop	__LINE__
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f72  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f73  = [C1 ], -1 * SIZE
#else
	nop	__LINE__
#endif
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
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f73
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1] = f72,  SIZE
	setf.d	f64 = r0
	mov	f80 = f0
	}
	;;
	{ .mmf
	STFD	[C1] = f73,  SIZE
	setf.d	f65 = r0
	mov	f81 = f0
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMA	f72  = ALPHA_R, f64, f0
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FCALC_C	f73  = ALPHA_R, f65, f0
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FCALC_D	f72  = ALPHA_I, f65, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f73  = ALPHA_I, f64, f73
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1] = f72,  SIZE
	setf.d	f64 = r0
	mov	f80 = f0
	}
	;;
	{ .mmf
	STFD	[C1] = f73,  SIZE
	setf.d	f65 = r0
	mov	f81 = f0
	}
	;;
#endif
	.align 16

.L119:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
#if defined(TRMMKERNEL) && !defined(LEFT)
	adds	KK = 1, KK
#else
	nop	__LINE__
#endif
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

