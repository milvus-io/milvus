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

#define PREFETCHSIZE  (8 * 16)

#define CPREFETCHSIZE 7
#define CPREFETCH     lfetch.excl.nt2

#define M	r32
#define N	r33
#define K	r34
#define A	r38
#define B	r39
#define C	r36
#define LDC	r37

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

#define C9	loc0
#define C10	loc1
#define C11	loc2
#define C12	loc3
#define C13	loc4
#define C14	loc5
#define C15	loc6
#define C16	loc7

#define PREA	r8
#define PREB	r9
#define PREC	r10
#define SP	r12
#define ARLC	r29
#define PR	r30
#define ARPFS	r31

#define ALPHA	f8

#define AORIG	loc8
#define KK	loc9
#define KK8	loc10
#define OFFSET	loc11

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	.save	ar.pfs, ARPFS
#ifdef TRMMKERNEL
	alloc	ARPFS = ar.pfs, 8, 16, 0, 0
#else
	alloc	ARPFS = ar.pfs, 8,  8, 0, 0
#endif
	adds	r14 = 16, SP
	mov	ARLC  = ar.lc
	}
	{ .mmi
	adds	r8 = -16 * 16, SP
	adds	r9 = -15 * 16, SP
	adds	SP = -16 * 16, SP
	}
	;;
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	mov	PR = pr
	;;
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	;;
	stf.spill  [r8] = f20, 32
	stf.spill  [r9] = f21, 32
	shr	J = N, 3
	;;
	stf.spill  [r8] = f22, 32
	stf.spill  [r9] = f23, 32
	mov	AOFFSET = A
	;;
	stf.spill  [r8] = f24, 32
	stf.spill  [r9] = f25, 32
	cmp.ge	p6, p0  = 0, J
	;;
	stf.spill  [r8] = f26, 32
	stf.spill  [r9] = f27, 32
	;;
	stf.spill  [r8] = f28, 32
	stf.spill  [r9] = f29, 32
       ;;
	stf.spill  [r8] = f30
	stf.spill  [r9] = f31
	ld8	C      = [r14], 8
	;;
	ld8	LDC      = [r14], 8
	;;
	shladd	LDC = LDC, BASE_SHIFT, r0
	;;
#ifndef TRMMKERNEL
	(p6)	br.cond.dpnt .L050
	.body
	;;
#else
	.body
	;;
	ld8	OFFSET   = [r14], 8
	;;

#if defined(TRMMKERNEL) && !defined(LEFT)
	;;
	sub	KK  = r0, OFFSET
#endif
	(p6)	br.cond.dpnt .L050
	;;
#endif
	.align 32

.L010:
	{ .mfi
	adds	J = -1, J
	mov	f64  = f0
	shr	I  = M, 3
	}
	{ .mfi
	mov	C1 = C			// coffset1 = c + 0 * ldc
	mov	f72  = f0
	}
	;;
	{ .mmf
	cmp.eq	p6, p7 = 0, I
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	mov	f80  = f0
	}
	{ .mmf
	add	C2 = LDC, C		// coffset2 = c + 1 * ldc
	shladd	C3 = LDC, 1, C		// coffset3 = c + 2 * ldc
	mov	f88  = f0
	}
	;;
	{ .mmf
	shladd	C5 = LDC, 2, C		// coffset5 = c + 4 * ldc
	shladd	C = LDC, 3, C		// coffset += 8 * ldc
	mov	f96  = f0
	}
	{ .mmf
	shladd	C4 = LDC, 1, C2		// coffset4 = c + 3 * ldc
	shladd	C6 = LDC, 2, C2		// coffset6 = c + 5 * ldc
	mov	f104 = f0
	}
	;;
	{ .mfi
	shladd	C7 = LDC, 2, C3		// coffset7 = c + 6 * ldc
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}{ .mfb
	sub	C8 = C,  LDC		// coffset8 = c + 7 * ldc
	mov	f120 = f0
	(p6)	br.cond.dpnt .L020
	}
	;;
	.align 16

.L011:
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	mov	BOFFSET = B
	;;

	{ .mfb
	LDFD	f48 = [BOFFSET], SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f49 = [BOFFSET], SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 3, B
	mov	f65  = f0
	shladd	AOFFSET = KK8, 3, AOFFSET
	}
	;;
	LDFD	f48 = [BOFFSET], SIZE
	;;
	{ .mfi
	LDFD	f49 = [BOFFSET],  SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
#endif
	LDFD	f32 = [AOFFSET], SIZE
	LDFD	f50 = [BOFFSET], SIZE
	;;

	{ .mfb
	LDFD	f33 = [AOFFSET], SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	{ .mfb
	LDFD	f51 = [BOFFSET], SIZE
	mov	f89  = f0
	nop	__LINE__
	}
	;;
	LDFD	f52 = [BOFFSET], SIZE
	;;
	{ .mmf
	LDFD	f53 = [BOFFSET], SIZE
	setf.d	f97  = r0
	mov	f105 = f0
	}
	{ .mfb
	setf.d	f113 = r0
	mov	f121 = f0
	nop	__LINE__
	}
	;;
	LDFD	f54 = [BOFFSET], SIZE
	;;
	{ .mmf
	LDFD	f55 = [BOFFSET], SIZE
	setf.d	f66  = r0
	mov	f74  = f0
	}
	{ .mfb
	setf.d	f82  = r0
	mov	f90  = f0
	nop	__LINE__
	}
	;;
	LDFD	f34  = [AOFFSET], SIZE
	;;
	{ .mmf
	LDFD	f35  = [AOFFSET], SIZE
	setf.d	f98  = r0
	mov	f106 = f0
	}
	{ .mfb
	setf.d	f114 = r0
	mov	f122 = f0
	nop	__LINE__
	}
	;;
	LDFD	f36  = [AOFFSET], SIZE
	;;
	{ .mmf
	LDFD	f37  = [AOFFSET], SIZE
	setf.d	f67  = r0
	mov	f75  = f0
	}
	{ .mfi
	setf.d	f83  = r0
	mov	f91  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 8, KK
#else
	adds	L = 8, KK
#endif
#endif
	}
	;;
	LDFD	f38  = [AOFFSET], SIZE
	;;
	{ .mmf
	LDFD	f39  = [AOFFSET], SIZE
	setf.d	f99  = r0
	mov	f107 = f0
	}
	{ .mfi
	setf.d	f115 = r0
	mov	f123 = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f68  = r0
	mov	f76  = f0
	}
	{ .mfi
	setf.d	f84  = r0
	mov	f92  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f100 = r0
	mov	f108 = f0
	}
	{ .mfi
	setf.d	f116 = r0
	mov	f124 = f0
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f69  = r0
	mov	f77  = f0
	}
	{ .mfi
	setf.d	f85  = r0
	mov	f93  = f0
	adds	PREB = (PREFETCHSIZE - 8) * SIZE, BOFFSET
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f101 = r0
	mov	f109 = f0
	}
	{ .mfi
	setf.d	f117 = r0
	mov	f125 = f0
	tbit.z	p12, p0 = L, 0
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f70  = r0
	mov	f78  = f0
	}
	{ .mfi
	setf.d	f86  = r0
	mov	f94  = f0
	shr	L = L, 1
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f102 = r0
	mov	f110 = f0
	}
	{ .mfi
	setf.d	f118 = r0
	mov	f126 = f0
	adds	L =  -1, L
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	setf.d	f71  = r0
	mov	f79  = f0
	}
	{ .mfi
	setf.d	f87  = r0
	mov	f95  = f0
	mov	ar.lc = L
	}
	;;
	{ .mmf
	CPREFETCH [PREC]
	setf.d	f103 = r0
	mov	f111 = f0
	}
	{ .mfi
	setf.d	f119 = r0
	mov	f127 = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	.align 16

.L012:
/*  1 */
	{ .mfi
	lfetch.fault.nt1	[PREA], 8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p12) cmp.ne p3, p0 =  0, L
	FMA	f72   = f32, f49, f72	// A1 * B2
	nop	__LINE__
	}
	;;
/*  2 */
	{ .mfi
	lfetch.nt1	[PREB], 8 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	cmp.ne	p4, p5 =  0, L
	FMA	f88   = f32, f51, f88	// A1 * B4
	nop	__LINE__
	}
	;;
/*  3 */
	{ .mfb
	(p3) LDFD	f40 = [AOFFSET], SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	adds	C9  = 4 * SIZE, C1
	FMA	f104  = f32, f53, f104	// A1 * B6
	nop	__LINE__
	}
	;;
/*  4 */
	{ .mfi
	(p3) LDFD	f56 = [BOFFSET],   SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	adds	C10 = 4 * SIZE, C2
	}
	{ .mfb
	(p3) LDFD	f41 = [AOFFSET], SIZE
	FMA	f120  = f32, f55, f120	// A1 * B8
	nop	__LINE__
	}
	;;
/*  5 */
	{ .mfi
	(p3) LDFD	f57 = [BOFFSET],   SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	adds	C11 = 4 * SIZE, C3
	}
	{ .mfb
	(p3) LDFD	f42 = [AOFFSET], SIZE
	FMA	f73   = f33, f49, f73	// A2 * B2
	nop	__LINE__
	}
	;;
/*  6 */
	{ .mfi
	(p3) LDFD	f58 = [BOFFSET],  SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	adds	C12 = 4 * SIZE, C4
	}
	{ .mfb
	(p3) LDFD	f43 = [AOFFSET], SIZE
	FMA	f89   = f33, f51, f89	// A2 * B4
	nop	__LINE__
	}
	;;
/*  7 */
	{ .mfi
	(p3) LDFD	f59 = [BOFFSET],  SIZE
	FMA	f97   = f33, f52, f97	// A2 * B5
	adds	C13 = 4 * SIZE, C5
	}
	{ .mfb
	(p3) LDFD	f44 = [AOFFSET], SIZE
	FMA	f105  = f33, f53, f105	// A2 * B6
	nop	__LINE__
	}
	;;
/*  8 */
	{ .mfi
	(p3) LDFD	f60 = [BOFFSET], SIZE
	FMA	f113  = f33, f54, f113	// A2 * B7
	adds	C14 = 4 * SIZE, C6
	}
	{ .mfb
	(p3) LDFD	f45 = [AOFFSET], SIZE
	FMA	f121  = f33, f55, f121	// A2 * B8
	nop	__LINE__
	}
	;;
/*  9 */
	{ .mfi
	(p3) LDFD	f61 = [BOFFSET], SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	adds	C15 = 4 * SIZE, C7
	}
	{ .mfb
	(p3) LDFD	f46 = [AOFFSET], SIZE
	FMA	f74   = f34, f49, f74	// A3 * B2
	nop	__LINE__
	}
	;;
/* 10 */
	{ .mfi
	(p3) LDFD	f62 = [BOFFSET], SIZE
	FMA	f82   = f34, f50, f82	// A3 * B3
	adds	C16 = 4 * SIZE, C8
	}
	{ .mfb
	(p3) LDFD	f47 = [AOFFSET], SIZE
	FMA	f90   = f34, f51, f90	// A3 * B4
	nop	__LINE__
	}
	;;
/* 11 */
	{ .mfb
	(p3) LDFD	f63 = [BOFFSET], SIZE
	FMA	f98   = f34, f52, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f106  = f34, f53, f106	// A3 * B6
	nop	__LINE__
	}
	;;
/* 12 */
	{ .mfb
	nop	__LINE__
	FMA	f114  = f34, f54, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f122  = f34, f55, f122	// A3 * B8
	nop	__LINE__
	}
	;;
/* 13 */
	{ .mfb
	nop	__LINE__
	FMA	f67   = f35, f48, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}
	;;
/* 14 */
	{ .mfb
	nop	__LINE__
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f91   = f35, f51, f91	// A4 * B4
	nop	__LINE__
	}
	;;
/* 15 */
	{ .mfb
	nop	__LINE__
	FMA	f99   = f35, f52, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f107  = f35, f53, f107	// A4 * B6
	nop	__LINE__
	}
	;;
/* 16 */
	{ .mfb
	nop	__LINE__
	FMA	f115  = f35, f54, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f123  = f35, f55, f123	// A4 * B8
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
	FMA	f76   = f36, f49, f76	// A5 * B2
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
	FMA	f92   = f36, f51, f92	// A5 * B4
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
	FMA	f108  = f36, f53, f108	// A5 * B6
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
	FMA	f124  = f36, f55, f124	// A5 * B8
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
	FMA	f77   = f37, f49, f77	// A6 * B2
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
	FMA	f93   = f37, f51, f93	// A6 * B4
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
	FMA	f109  = f37, f53, f109	// A6 * B6
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
	FMA	f125  = f37, f55, f125	// A6 * B8
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
	FMA	f78   = f38, f49, f78	// A7 * B2
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
	FMA	f94   = f38, f51, f94	// A7 * B4
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
	FMA	f110  = f38, f53, f110	// A7 * B6
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
	FMA	f126  = f38, f55, f126	// A7 * B8
	nop	__LINE__
	}
	;;
/* 29 */
	{ .mfb
	(p4) LDFD	f32 = [AOFFSET],   SIZE
	FMA	f71   = f39, f48, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79   = f39, f49, f79	// A8 * B2
	nop	__LINE__
	}
	;;
/* 30 */
	{ .mfb
	(p4) LDFD	f33 = [AOFFSET], SIZE
	FMA	f87   = f39, f50, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f48 = [BOFFSET],   SIZE
	FMA	f95   = f39, f51, f95	// A8 * B4
	nop	__LINE__
	}
	;;
/* 31 */
	{ .mfb
	(p4) LDFD	f34 = [AOFFSET], SIZE
	FMA	f103  = f39, f52, f103	// A8 * B5
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f49 = [BOFFSET],   SIZE
	FMA	f111  = f39, f53, f111	// A8 * B6
	nop	__LINE__
	}
	;;
/* 32 */
	{ .mfb
	lfetch.fault.nt1	[PREA], 8 * SIZE
	FMA	f119  = f39, f54, f119	// A8 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f127  = f39, f55, f127	// A8 * B8
	nop	__LINE__
	}
	;;
/* 33 */
	{ .mfb
	lfetch.nt1	[PREB], 8 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
/* 34 */
	{ .mfb
	(p4) LDFD	f35 = [AOFFSET], SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f50 = [BOFFSET],  SIZE
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
/* 35 */
	{ .mfb
	(p4) LDFD	f36 = [AOFFSET], SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f51 = [BOFFSET],  SIZE
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	nop	__LINE__
	}
	;;
/* 36 */
	{ .mfb
	(p4) LDFD	f37 = [AOFFSET], SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f52 = [BOFFSET], SIZE
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	;;
/* 37 */
	{ .mfb
	(p4) LDFD	f38 = [AOFFSET], SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f53 = [BOFFSET], SIZE
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
/* 38 */
	{ .mfb
	(p4) LDFD	f39 = [AOFFSET], SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFD	f54 = [BOFFSET], SIZE
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
/* 39 */
	{ .mfb
	(p4) LDFD	f55 = [BOFFSET], SIZE
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
	nop	__LINE__
	}
	;;
/* 40 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f6  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f7  = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	nop	__LINE__
	}
	;;
 /* 41 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f10 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f11 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
/* 42 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f12 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f13 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
/* 43 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f14 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f15 = [C9 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f106  = f42, f61, f106	// A3 * B6
	nop	__LINE__
	}
	;;
/* 44 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f16 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f17 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f122  = f42, f63, f122	// A3 * B8
	nop	__LINE__
	}
	;;
/* 45 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f18 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f19 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
/* 46 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f20 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f21 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
/* 47 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f22 = [C2 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f23 = [C10], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f107  = f43, f61, f107	// A4 * B6
	nop	__LINE__
	}
	;;
/* 48 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f24 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f25 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f123  = f43, f63, f123	// A4 * B8
	nop	__LINE__
	}
	;;
/* 49 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f26 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f27 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
/* 50 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f28 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f29 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f92   = f44, f59, f92	// A5 * B4
	nop	__LINE__
	}
	;;
/* 51 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f30 = [C3 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f100  = f44, f60, f100	// A5 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f31 = [C11], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f108  = f44, f61, f108	// A5 * B6
	nop	__LINE__
	}
	;;
/* 52 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f32 = [C4 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f116  = f44, f62, f116	// A5 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f33 = [C12], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f124  = f44, f63, f124	// A5 * B8
	nop	__LINE__
	}
	;;
/* 53 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f34 = [C4 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f35 = [C12], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
/* 54 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f36 = [C4 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f37 = [C12], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f93   = f45, f59, f93	// A6 * B4
	nop	__LINE__
	}
	;;
/* 55 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f38 = [C4 ], -3 * SIZE
#else
	nop	__LINE__
#endif
 	(p3) FMA	f101  = f45, f60, f101	// A6 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f39 = [C12], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f109  = f45, f61, f109	// A6 * B6
	nop	__LINE__
	}
	;;
/* 56 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f48 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f117  = f45, f62, f117	// A6 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f49 = [C13], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f125  = f45, f63, f125	// A6 * B8
	nop	__LINE__
	}
	;;
/* 57 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f50 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f51 = [C13], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
/* 58 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f52 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f86   = f46, f58, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f53 = [C13], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f94   = f46, f59, f94	// A7 * B4
	nop  __LINE__
	}
	;;
/* 59 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f54 = [C5 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f102  = f46, f60, f102	// A7 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f55 = [C13], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f110  = f46, f61, f110	// A7 * B6
	nop	__LINE__
	}
	;;
/* 60 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f40 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f118  = f46, f62, f118	// A7 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f41 = [C14], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f126  = f46, f63, f126	// A7 * B8
	nop	__LINE__
	}
	;;
/* 61 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f42 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f43 = [C14], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	nop	__LINE__
	}
	;;
/* 62 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f44 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f87   = f47, f58, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f45 = [C14], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f95   = f47, f59, f95	// A8 * B4
	nop	__LINE__
	}
	;;
/* 63 */
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f59 = [C6 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f103  = f47, f60, f103	// A8 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f60 = [C14], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f111  = f47, f61, f111	// A8 * B6
	nop	__LINE__
	}
	;;
/* 64 */
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f61 = [C7 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f119  = f47, f62, f119	// A8 * B7
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f62 = [C15], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f127  = f47, f63, f127	// A8 * B8
	br.cloop.sptk.few .L012
	}
	;;
.L013:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfi
	(p5) LDFD	f63 = [C7 ], SIZE
	FMA	f64  = ALPHA, f64, f6
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	(p5) LDFD	f6  = [C15], SIZE
	FMA	f68  = ALPHA, f68, f7
	nop	__LINE__
	}
	;;
	{ .mfi
	(p5) LDFD	f7  = [C7 ], SIZE
	FMA	f65  = ALPHA, f65, f10
	adds	I = -1, I
	}
	{ .mfb
	(p5) LDFD	f10 = [C15], SIZE
	FMA	f69  = ALPHA, f69, f11
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f11 = [C7 ], -3 * SIZE
	FMA	f66  = ALPHA, f66, f12
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f12 = [C15], -3 * SIZE
	FMA	f70  = ALPHA, f70, f13
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f13  = [C8 ], SIZE
	FMA	f67  = ALPHA, f67, f14
	nop	__LINE__
	}
	{ .mfb
	LDFD	f14  = [C16], SIZE
	FMA	f71  = ALPHA, f71, f15
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	FMA	f72  = ALPHA, f72, f16
	}
	{ .mmf
	LDFD	f15 = [C8 ], SIZE
	LDFD	f16 = [C16], SIZE
	FMA	f76  = ALPHA, f76, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	FMA	f73  = ALPHA, f73, f18
	}
	{ .mmf
	LDFD	f17 = [C8 ], SIZE
	LDFD	f18 = [C16], SIZE
	FMA	f77  = ALPHA, f77, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	FMA	f74  = ALPHA, f74, f20
	}
	{ .mmf
	LDFD	f19 = [C8 ], -3 * SIZE
	LDFD	f20 = [C16], -3 * SIZE
	FMA	f78  = ALPHA, f78, f21
	}
	;;
	{ .mfb
	STFD	[C1 ] = f67, 5 * SIZE
	FMA	f75  = ALPHA, f75, f22
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f71, 5 * SIZE
	FMA	f79  = ALPHA, f79, f23
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMA	f80  = ALPHA, f80, f24
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f76, SIZE
	FMA	f84  = ALPHA, f84, f25
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	FMA	f81  = ALPHA, f81, f26
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f77, SIZE
	FMA	f85  = ALPHA, f85, f27
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f74, SIZE
	FMA	f82  = ALPHA, f82, f28
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f78, SIZE
	FMA	f86  = ALPHA, f86, f29
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f75, 5 * SIZE
	FMA	f83  = ALPHA, f83, f30
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f79, 5 * SIZE
	FMA	f87  = ALPHA, f87, f31
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMA	f88  = ALPHA, f88, f32
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f84, SIZE
	FMA	f92  = ALPHA, f92, f33
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, SIZE
	FMA	f89  = ALPHA, f89, f34
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f85, SIZE
	FMA	f93  = ALPHA, f93, f35
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f82, SIZE
	FMA	f90  = ALPHA, f90, f36
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f86, SIZE
	FMA	f94  = ALPHA, f94, f37
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f83, 5 * SIZE
	FMA	f91  = ALPHA, f91, f38
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f87, 5 * SIZE
	FMA	f95  = ALPHA, f95, f39
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	FMA	f96  = ALPHA, f96,  f48
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f92, SIZE
	FMA	f100 = ALPHA, f100, f49
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f89, SIZE
	FMA	f97  = ALPHA, f97,  f50
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f93, SIZE
	FMA	f101 = ALPHA, f101, f51
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f90, SIZE
	FMA	f98  = ALPHA, f98,  f52
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f94, SIZE
	FMA	f102 = ALPHA, f102, f53
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f91, 5 * SIZE
	FMA	f99  = ALPHA, f99,  f54
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f95, 5 * SIZE
	FMA	f103 = ALPHA, f103, f55
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f96,  SIZE
	FMA	f104 = ALPHA, f104, f40
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f100, SIZE
	FMA	f108 = ALPHA, f108, f41
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f97,  SIZE
	FMA	f105 = ALPHA, f105, f42
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f101, SIZE
	FMA	f109 = ALPHA, f109, f43
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f98,  SIZE
	FMA	f106 = ALPHA, f106, f44
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f102, SIZE
	FMA	f110 = ALPHA, f110, f45
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f99,  5 * SIZE
	FMA	f107 = ALPHA, f107, f59
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f103, 5 * SIZE
 	FMA	f111 = ALPHA, f111, f60
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f104, SIZE
	FMA	f112 = ALPHA, f112, f61
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f108, SIZE
	FMA	f116 = ALPHA, f116, f62
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f105, SIZE
	FMA	f113 = ALPHA, f113, f63
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f109, SIZE
	FMA	f117 = ALPHA, f117, f6
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f106, SIZE
	FMA	f114 = ALPHA, f114, f7
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f110, SIZE
	FMA	f118 = ALPHA, f118, f10
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f107, 5 * SIZE
	FMA	f115 = ALPHA, f115, f11
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f111, 5 * SIZE
	FMA	f119 = ALPHA, f119, f12
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f112, SIZE
	FMA	f120 = ALPHA, f120, f13
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f116, SIZE
	FMA	f124 = ALPHA, f124, f14
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f113, SIZE
	FMA	f121 = ALPHA, f121, f15
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f117, SIZE
	FMA	f125 = ALPHA, f125, f16
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f114, SIZE
	FMA	f122 = ALPHA, f122, f17
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f118, SIZE
	FMA	f126 = ALPHA, f126, f18
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f115, 5 * SIZE
	FMA	f123 = ALPHA, f123, f19
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f119, 5 * SIZE
	FMA	f127 = ALPHA, f127, f20
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C8 ] = f120, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C16] = f124, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C8 ] = f121, SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C16] = f125, SIZE
	mov	f88  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C8 ] = f122, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C16] = f126, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C8 ] = f123, 5 * SIZE
	mov	f112 = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C16] = f127, 5 * SIZE
	mov	f120 = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	nop	__LINE__
	FMPY	f68  = ALPHA, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
	adds	I = -1, I
	}
	{ .mfb
	nop	__LINE__
	FMPY	f69  = ALPHA, f69
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f66  = ALPHA, f66
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f70  = ALPHA, f70
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f67  = ALPHA, f67
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f71  = ALPHA, f71
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	FMPY	f72  = ALPHA, f72
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMPY	f76  = ALPHA, f76
	}
	;;
	{ .mmf
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	FMPY	f73  = ALPHA, f73
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMPY	f77  = ALPHA, f77
	}
	;;
	{ .mmf
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	FMPY	f74  = ALPHA, f74
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMPY	f78  = ALPHA, f78
	}
	;;
	{ .mfb
	STFD	[C1 ] = f67, 5 * SIZE
	FMPY	f75  = ALPHA, f75
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f71, 5 * SIZE
	FMPY	f79  = ALPHA, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMPY	f80  = ALPHA, f80
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f76, SIZE
	FMPY	f84  = ALPHA, f84
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	FMPY	f81  = ALPHA, f81
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f77, SIZE
	FMPY	f85  = ALPHA, f85
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f74, SIZE
	FMPY	f82  = ALPHA, f82
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f78, SIZE
	FMPY	f86  = ALPHA, f86
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f75, 5 * SIZE
	FMPY	f83  = ALPHA, f83
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f79, 5 * SIZE
	FMPY	f87  = ALPHA, f87
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMPY	f88  = ALPHA, f88
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f84, SIZE
	FMPY	f92  = ALPHA, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, SIZE
	FMPY	f89  = ALPHA, f89
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f85, SIZE
	FMPY	f93  = ALPHA, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f82, SIZE
	FMPY	f90  = ALPHA, f90
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f86, SIZE
	FMPY	f94  = ALPHA, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f83, 5 * SIZE
	FMPY	f91  = ALPHA, f91
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f87, 5 * SIZE
	FMPY	f95  = ALPHA, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	FMPY	f96  = ALPHA, f96
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f92, SIZE
	FMPY	f100 = ALPHA, f100
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f89, SIZE
	FMPY	f97  = ALPHA, f97
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f93, SIZE
	FMPY	f101 = ALPHA, f101
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f90, SIZE
	FMPY	f98  = ALPHA, f98
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f94, SIZE
	FMPY	f102 = ALPHA, f102
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f91, 5 * SIZE
	FMPY	f99  = ALPHA, f99
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f95, 5 * SIZE
	FMPY	f103 = ALPHA, f103
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f96,  SIZE
	FMPY	f104 = ALPHA, f104
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f100, SIZE
	FMPY	f108 = ALPHA, f108
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f97,  SIZE
	FMPY	f105 = ALPHA, f105
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f101, SIZE
	FMPY	f109 = ALPHA, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f98,  SIZE
	FMPY	f106 = ALPHA, f106
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f102, SIZE
	FMPY	f110 = ALPHA, f110
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f99,  5 * SIZE
	FMPY	f107 = ALPHA, f107
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f103, 5 * SIZE
 	FMPY	f111 = ALPHA, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f104, SIZE
	FMPY	f112 = ALPHA, f112
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f108, SIZE
	FMPY	f116 = ALPHA, f116
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f105, SIZE
	FMPY	f113 = ALPHA, f113
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f109, SIZE
	FMPY	f117 = ALPHA, f117
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f106, SIZE
	FMPY	f114 = ALPHA, f114
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f110, SIZE
	FMPY	f118 = ALPHA, f118
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f107, 5 * SIZE
	FMPY	f115 = ALPHA, f115
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f111, 5 * SIZE
	FMPY	f119 = ALPHA, f119
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f112, SIZE
	FMPY	f120 = ALPHA, f120
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f116, SIZE
	FMPY	f124 = ALPHA, f124
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f113, SIZE
	FMPY	f121 = ALPHA, f121
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f117, SIZE
	FMPY	f125 = ALPHA, f125
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C7 ] = f114, SIZE
	FMPY	f122 = ALPHA, f122
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C15] = f118, SIZE
	FMPY	f126 = ALPHA, f126
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C7 ] = f115, 5 * SIZE
	FMPY	f123 = ALPHA, f123
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C15] = f119, 5 * SIZE
	FMPY	f127 = ALPHA, f127
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C8 ] = f120, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C16] = f124, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C8 ] = f121, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C16] = f125, SIZE
	mov	f88  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 3, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C8 ] = f122, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 8, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C16] = f126, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C8 ] = f123, 5 * SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C16] = f127, 5 * SIZE
	mov	f120 = f0
	(p6)	br.cond.dptk .L011
	}
	;;
#endif

.L020:
#if 0
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f89  = f0
	tbit.z	p6, p7 = M, 2
	}
	{ .mfb
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 4, KK
#else
	adds	L = 8, KK
#endif
#endif
	mov	f81  = f0
	(p6)	br.cond.dptk .L030
	}
	;;

#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfi
	LDFPD	f48, f49 = [B]
	mov	f65  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
	mov	f73  = f0
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 3, B
	mov	f65  = f0
	shladd	AOFFSET = KK8, 2, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f73  = f0
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	}
	;;
#endif
	{ .mmf
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	setf.d	f97  = r0
	mov	f105 = f0
	}
	{ .mfi
	setf.d	f113 = r0
	mov	f121 = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mmf
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	setf.d	f66  = r0
	mov	f74  = f0
	}
	{ .mfi
	setf.d	f82  = r0
	mov	f90  = f0
	tbit.z	p12, p0 = L, 0
	}
	;;
	{ .mmf
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	setf.d	f98   = r0
	mov	f106  = f0
	}
	{ .mfi
	setf.d	f114 = r0
	mov	f122 = f0
	shr	L = L, 1
	}
	;;
	{ .mfi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f75  = f0
	adds	L =  -1, L
	}
	{ .mmf
	setf.d	f67  = r0
	setf.d	f83  = r0
	mov	f91  = f0
	}
	;;
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f107 = f0
	mov	ar.lc = L
	}
	{ .mmf
	setf.d	f99  = r0
	setf.d	f115 = r0
	mov	f123 = f0
	}
	;;
	.align 32

.L022:
	{ .mfi
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
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
	FMA	f88   = f32, f51, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	(p5) adds	C9  = 2 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f104  = f32, f53, f104	// A1 * B6
	(p5) adds	C10 = 2 * SIZE, C2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	(p5) adds	C11 = 2 * SIZE, C3
	}
	{ .mfi
	nop	__LINE__
	FMA	f120  = f32, f55, f120	// A1 * B8
	(p5) adds	C12 = 2 * SIZE, C4
	}
	;;
	{ .mfi
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	(p5) adds	C13 = 2 * SIZE, C5
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	(p5) adds	C14 = 2 * SIZE, C6
	}
	;;
	{ .mfi
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	(p5) adds	C15 = 2 * SIZE, C7
	}
	{ .mfi
	nop	__LINE__
	FMA	f89   = f33, f51, f89	// A2 * B4
	(p5) adds	C16 = 2 * SIZE, C8
	}
	;;
	{ .mfb
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	FMA	f97   = f33, f52, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f105  = f33, f53, f105	// A2 * B6
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
	FMA	f121  = f33, f55, f121	// A2 * B8
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
	FMA	f74   = f34, f49, f74	// A3 * B2
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
	FMA	f90   = f34, f51, f90	// A3 * B4
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
	FMA	f106  = f34, f53, f106	// A3 * B6
	nop	__LINE__
	}

	{ .mfb
	nop	__LINE__
	FMA	f114  = f34, f54, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f122  = f34, f55, f122	// A3 * B8
	nop	__LINE__
	}

	{ .mfb
	nop	__LINE__
	FMA	f67   = f35, f48, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f91   = f35, f51, f91	// A4 * B4
	nop	__LINE__
	}

	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f99   = f35, f52, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f107  = f35, f53, f107	// A4 * B6
	nop	__LINE__
	}

	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f115  = f35, f54, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f123  = f35, f55, f123	// A4 * B8
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
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
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
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f70 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f69 = [C1 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f71 = [C9 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f78 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77 = [C2 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f79 = [C10], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f84 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f86 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f85 = [C3 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f87 = [C11], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92 = [C4 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f94 = [C12], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f106  = f42, f61, f106	// A3 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f93 = [C4 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f95 = [C12], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f122  = f42, f63, f122	// A3 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f100 = [C5 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f102 = [C13], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f101 = [C5 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f103 = [C13], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f108 = [C6 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f110 = [C14], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f107  = f43, f61, f107	// A4 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f109 = [C6 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f111 = [C14], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f123  = f43, f63, f123	// A4 * B8
	br.cloop.sptk.few .L022
	}
	;;

.L028:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	LDFD	f116 = [C7 ], SIZE
	FMA	f64  = ALPHA, f64, f68
	nop	__LINE__
	}
	{ .mfb
	LDFD	f118 = [C15], SIZE
	FMA	f66  = ALPHA, f66, f70
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f117 = [C7 ], -1 * SIZE
	FMA	f65  = ALPHA, f65, f69
	nop	__LINE__
	}
	{ .mfb
	LDFD	f119 = [C15], -1 * SIZE
	FMA	f67  = ALPHA, f67, f71
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f124 = [C8], SIZE
	FMA	f72  = ALPHA, f72, f76
	nop	__LINE__
	}
	{ .mfb
	LDFD	f126 = [C16], SIZE
	FMA	f74  = ALPHA, f74, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f125 = [C8], -1 * SIZE
	FMA	f73  = ALPHA, f73, f77
	nop	__LINE__
	}
	{ .mfb
	LDFD	f127 = [C16], -1 * SIZE
	FMA	f75  = ALPHA, f75, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMA	f80  = ALPHA, f80, f84
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f66, SIZE
	FMA	f82  = ALPHA, f82, f86
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, 3 * SIZE
	FMA	f81  = ALPHA, f81, f85
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f67, 3 * SIZE
	FMA	f83  = ALPHA, f83, f87
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMA	f88  = ALPHA, f88, f92
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f74, SIZE
	FMA	f90  = ALPHA, f90, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, 3 * SIZE
	FMA	f89  = ALPHA, f89, f93
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f75, 3 * SIZE
	FMA	f91  = ALPHA, f91, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMA	f96  = ALPHA, f96,  f100
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f82, SIZE
	FMA	f98  = ALPHA, f98,  f102
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, 3 * SIZE
	FMA	f97  = ALPHA, f97,  f101
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f83, 3 * SIZE
	FMA	f99  = ALPHA, f99,  f103
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	FMA	f104 = ALPHA, f104, f108
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f90, SIZE
	FMA	f106 = ALPHA, f106, f110
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f89, 3 * SIZE
	FMA	f105 = ALPHA, f105, f109
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f91, 3 * SIZE
	FMA	f107 = ALPHA, f107, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f96,  SIZE
	FMA	f112 = ALPHA, f112, f116
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f98,  SIZE
	FMA	f114 = ALPHA, f114, f118
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f97,  3 * SIZE
	FMA	f113 = ALPHA, f113, f117
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f99,  3 * SIZE
	FMA	f115 = ALPHA, f115, f119
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f104, SIZE
	FMA	f120 = ALPHA, f120, f124
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f106, SIZE
	FMA	f122 = ALPHA, f122, f126
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C6 ] = f105, 3 * SIZE
	FMA	f121 = ALPHA, f121, f125
	nop	__LINE__
	}
	{ .mfb
	STFD	[C14] = f107, 3 * SIZE
	FMA	f123 = ALPHA, f123, f127
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f112, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f114, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f113, 3 * SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C15] = f115, 3 * SIZE
	mov	f88  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C8 ] = f120, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C16] = f122, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C8 ] = f121, 3 * SIZE
	mov	f112 = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C16] = f123, 3 * SIZE
	mov	f120 = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfb
	FMPY	f64  = ALPHA, f64
	nop	__LINE__
	}
	{ .mfb
	FMPY	f66  = ALPHA, f66
	nop	__LINE__
	}
	;;
	{ .mfb
	FMPY	f65  = ALPHA, f65
	nop	__LINE__
	}
	{ .mfb
	FMPY	f67  = ALPHA, f67
	nop	__LINE__
	}
	;;
	{ .mfb
	FMPY	f72  = ALPHA, f72
	nop	__LINE__
	}
	{ .mfb
	FMPY	f74  = ALPHA, f74
	nop	__LINE__
	}
	;;
	{ .mfb
	FMPY	f73  = ALPHA, f73
	nop	__LINE__
	}
	{ .mfb
	FMPY	f75  = ALPHA, f75
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMPY	f80  = ALPHA, f80
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f66, SIZE
	FMPY	f82  = ALPHA, f82
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, 3 * SIZE
	FMPY	f81  = ALPHA, f81
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f67, 3 * SIZE
	FMPY	f83  = ALPHA, f83
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMPY	f88  = ALPHA, f88
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f74, SIZE
	FMPY	f90  = ALPHA, f90
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, 3 * SIZE
	FMPY	f89  = ALPHA, f89
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f75, 3 * SIZE
	FMPY	f91  = ALPHA, f91
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMPY	f96  = ALPHA, f96
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f82, SIZE
	FMPY	f98  = ALPHA, f98
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, 3 * SIZE
	FMPY	f97  = ALPHA, f97
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f83, 3 * SIZE
	FMPY	f99  = ALPHA, f99
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	FMPY	f104 = ALPHA, f104
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f90, SIZE
	FMPY	f106 = ALPHA, f106
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f89, 3 * SIZE
	FMPY	f105 = ALPHA, f105
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f91, 3 * SIZE
	FMPY	f107 = ALPHA, f107
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f96,  SIZE
	FMPY	f112 = ALPHA, f112
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f98,  SIZE
	FMPY	f114 = ALPHA, f114
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f97,  3 * SIZE
	FMPY	f113 = ALPHA, f113
	nop	__LINE__
	}
	{ .mfb
	STFD	[C13] = f99,  3 * SIZE
	FMPY	f115 = ALPHA, f115
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C6 ] = f104, SIZE
	FMPY	f120 = ALPHA, f120
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C14] = f106, SIZE
	FMPY	f122 = ALPHA, f122
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C6 ] = f105, 3 * SIZE
	FMPY	f121 = ALPHA, f121
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C14] = f107, 3 * SIZE
	FMPY	f123 = ALPHA, f123
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C7 ] = f112, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C15] = f114, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C7 ] = f113, 3 * SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C15] = f115, 3 * SIZE
	mov	f88  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 3, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C8 ] = f120, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C16] = f122, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C8 ] = f121, 3 * SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C16] = f123, 3 * SIZE
	mov	f120 = f0
	nop	__LINE__
	}
	;;
#endif
	.align 32

.L030:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 2, KK
#else
	adds	L = 8, KK
#endif
#endif
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L040
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfi
	LDFPD	f48, f49 = [B]
	mov	f65  = f0
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
#else
	{ .mmf
	shladd	BOFFSET = KK8, 3, B
	shladd	AOFFSET = KK8, 1, AOFFSET
	mov	f65  = f0
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
#endif
	;;
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f81  = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f89  = f0
	shr	L = L, 1
	}
	;;
	{ .mfi
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	mov	f97  = f0
	adds	L =  -1, L
	}
	{ .mfi
	nop	__LINE__
	mov	f105 = f0
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	}
	;;
	{ .mfi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	mov	f113 = f0
	mov	ar.lc = L
	}
	{ .mfi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	mov	f121 = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	.align 32

.L032:
	{ .mfb
	lfetch.nt1	[PREA],  4 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
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
	FMA	f88   = f32, f51, f88	// A1 * B4
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
	FMA	f104  = f32, f53, f104	// A1 * B6
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
	FMA	f120  = f32, f55, f120	// A1 * B8
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
	FMA	f73   = f33, f49, f73	// A2 * B2
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
	FMA	f89   = f33, f51, f89	// A2 * B4
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
	FMA	f105  = f33, f53, f105	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f113  = f33, f54, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f121  = f33, f55, f121	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
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
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
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
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68  = [C1], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76  = [C2], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f69  = [C1], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77  = [C2], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f84  = [C3], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92  = [C4], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f85  = [C3], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f93  = [C4], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	br.cloop.sptk.few .L032
	}
	;;

.L038:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	LDFD	f100 = [C5], SIZE
	FMA	f64  = ALPHA, f64, f68
	nop	__LINE__
	}
	{ .mfb
	LDFD	f108 = [C6], SIZE
	FMA	f65  = ALPHA, f65, f69
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f101 = [C5], -1 * SIZE
	FMA	f72  = ALPHA, f72, f76
	nop	__LINE__
	}
	{ .mfb
	LDFD	f109 = [C6], -1 * SIZE
	FMA	f73  = ALPHA, f73, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f116 = [C7], SIZE
	FMA	f80  = ALPHA, f80, f84
	nop	__LINE__
	}
	{ .mfb
	LDFD	f124 = [C8], SIZE
	FMA	f81  = ALPHA, f81, f85
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f117 = [C7], -1 * SIZE
	FMA	f88  = ALPHA, f88, f92
	nop	__LINE__
	}
	{ .mfb
	LDFD	f125 = [C8], -1 * SIZE
	FMA	f89  = ALPHA, f89, f93
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMA	f96  = ALPHA, f96,  f100
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMA	f104 = ALPHA, f104, f108
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, SIZE
	FMA	f97  = ALPHA, f97,  f101
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	FMA	f105 = ALPHA, f105, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMA	f112 = ALPHA, f112, f116
	nop	__LINE__
	}
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	FMA	f120 = ALPHA, f120, f124
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, SIZE
	FMA	f113 = ALPHA, f113, f117
	nop	__LINE__
	}
	{ .mfb
	STFD	[C4 ] = f89, SIZE
	FMA	f121 = ALPHA, f121, f125
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f96, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6 ] = f104, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C5 ] = f97,  SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C6 ] = f105, SIZE
	mov	f88  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f112, SIZE
	mov	f96  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C8 ] = f120, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C7 ] = f113, SIZE
	mov	f112 = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C8 ] = f121, SIZE
	mov	f120 = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f72  = ALPHA, f72
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f73  = ALPHA, f73
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f80  = ALPHA, f80
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f81  = ALPHA, f81
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f88  = ALPHA, f88
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f89  = ALPHA, f89
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMPY	f96  = ALPHA, f96
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMPY	f104 = ALPHA, f104
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, SIZE
	FMPY	f97  = ALPHA, f97
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	FMPY	f105 = ALPHA, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f80, SIZE
	FMPY	f112 = ALPHA, f112
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	FMPY	f120 = ALPHA, f120
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f81, SIZE
	FMPY	f113 = ALPHA, f113
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4 ] = f89, SIZE
	FMPY	f121 = ALPHA, f121
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C5 ] = f96, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C6 ] = f104, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C5 ] = f97,  SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C6 ] = f105, SIZE
	mov	f88  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 3, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C7 ] = f112, SIZE
	mov	f96  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C8 ] = f120, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C7 ] = f113, SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C8 ] = f121, SIZE
	mov	f120 = f0
	nop	__LINE__
	}
	;;
#endif
	.align 32

.L040:
	{ .mib
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 1, KK
#else
	adds	L = 8, KK
#endif
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L049
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
#else
	{ .mmi
	shladd	BOFFSET = KK8, 3, B
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
#endif
	;;
	{ .mii
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	LDFD	f32 = [AOFFSET], 1 * SIZE
	adds	L =  -1, L
	}
	;;
	{ .mmi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	cmp.eq	p3, p0 = r0, r0
	mov	ar.lc = L
	}
	{ .mmi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	nop	__LINE__
	}
	;;
	.align 32

.L042:
	{ .mfb
	lfetch.nt1	[PREB],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p12) cmp.ne p3, p0 =  0, L
	FMA	f72   = f32, f49, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFD	f40 = [AOFFSET], 1 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f88   = f32, f51, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1]
#else
	nop	__LINE__
#endif
	FMA	f104  = f32, f53, f104	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C2]
#else
	nop	__LINE__
#endif
	FMA	f120  = f32, f55, f120	// A1 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFD	f32 = [AOFFSET],   1 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f84 = [C3]
#else
	nop	__LINE__
#endif
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92 = [C4]
#else
	nop	__LINE__
#endif
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	adds	L = -1, L
	}
	{ .mmb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f100 = [C5]
	(p5) LDFD	f108 = [C6]
#else
	nop	__LINE__
	nop	__LINE__
#endif
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	{ .mmb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f116 = [C7]
	(p5) LDFD	f124 = [C8]
#else
	nop	__LINE__
	nop	__LINE__
#endif
	br.cloop.sptk.few .L042
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f72  = ALPHA, f72, f76
	FMA	f80  = ALPHA, f80, f84
	FMA	f88  = ALPHA, f88, f92

	FMA	f96  = ALPHA, f96,  f100
	FMA	f104 = ALPHA, f104, f108
	FMA	f112 = ALPHA, f112, f116
	FMA	f120 = ALPHA, f120, f124
	;;
	STFD	[C1 ] = f64, SIZE
	mov	f64 = f0
	STFD	[C2 ] = f72, SIZE
	mov	f72 = f0
	;;
	STFD	[C3 ] = f80, SIZE
	mov	f80 = f0
	STFD	[C4 ] = f88, SIZE
	mov	f88 = f0
	;;
	STFD	[C5 ] = f96, SIZE
	mov	f96 = f0
	STFD	[C6 ] = f104, SIZE
	mov	f104 = f0
	;;
	STFD	[C7 ] = f112, SIZE
	mov	f112 = f0
	STFD	[C8 ] = f120, SIZE
	mov	f120 = f0
	;;
#else
	FMPY	f64  = ALPHA, f64
	FMPY	f72  = ALPHA, f72
	FMPY	f80  = ALPHA, f80
	FMPY	f88  = ALPHA, f88

	{ .mfi
	FMPY	f96  = ALPHA, f96
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f104 = ALPHA, f104
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f112 = ALPHA, f112
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f120 = ALPHA, f120
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64 = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C2 ] = f72, SIZE
	mov	f72 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f80, SIZE
	mov	f80 = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	AOFFSET = KK8, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C4 ] = f88, SIZE
	mov	f88 = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 3, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C5 ] = f96, SIZE
	mov	f96 = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 1, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C6 ] = f104, SIZE
	mov	f104 = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C7 ] = f112, SIZE
	mov	f112 = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C8 ] = f120, SIZE
	mov	f120 = f0
	nop	__LINE__
	}
	;;
#endif
	.align 32
#endif

.L049:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
#if defined(TRMMKERNEL) && !defined(LEFT)
	adds	KK = 8, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmb
	nop	__LINE__
	cmp.lt	p6, p0 = 0, J
	(p6)	br.cond.dptk .L010
	}
	;;
	.align 32

.L050:
	{ .mfi
	mov	C1 = C
	mov	f64  = f0
	tbit.z	p6, p0 = N, 2
	}
	{ .mfi
	add	C2 = LDC, C
	mov	f72  = f0
	shr	I  = M, 3
	}
	;;
	{ .mfi
	shladd	C3 = LDC, 1, C
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	mov	AOFFSET = A
	mov	f88  = f0
	(p6)	br.cond.dpnt .L090
	}
	;;
#if 0
	{ .mfi
	cmp.eq	p6, p7 = 0, I
	mov	f65  = f0
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	shladd	C4 = LDC, 1, C2
	mov	f73  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	mov	f81  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	shladd	C = LDC, 2, C
	mov	f89  = f0
	(p6)	br.cond.dpnt .L060
	}
	;;
	.align 32

.L052:
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
	mov	f74  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 2, B
	mov	f66  = f0
	shladd	AOFFSET = KK8, 3, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f74  = f0
	nop	__LINE__
	}
	;;
#endif
	;;
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 8, KK
#else
	adds	L = 4, KK
#endif
#endif
	}
	{ .mfi
	setf.d	f84  = r0
	mov	f90  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f67  = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f75  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mfi
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	mov	f83  = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	setf.d	f91  = r0
	mov	f68  = f0
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	;;
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f76  = f0
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	}
	{ .mfi
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	mov	f92  = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f69  = f0
	shr	L = L, 1
	}
	{ .mmf
	setf.d	f77  = r0
	setf.d	f85  = r0
	mov	f93  = f0
	}
	;;
	{ .mfi
	CPREFETCH [PREC], LDC
	mov	f70  = f0
	adds	L =  -1, L
	}
	{ .mmf
	setf.d	f78  = r0
	setf.d	f86  = r0
	mov	f94  = f0
	}
	;;
	{ .mfi
	CPREFETCH [PREC]
	mov	f71  = f0
	mov	ar.lc = L
	}
	{ .mmf
	setf.d	f79  = r0
	setf.d	f87  = r0
	mov	f95  = f0
	}
	;;
	.align 32

.L053:
	{ .mfb
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
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
	FMA	f88   = f32, f51, f88	// A1 * B4
	adds	C9  = 4 * SIZE, C1
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	adds	C10 = 4 * SIZE, C2
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	adds	C11 = 4 * SIZE, C3
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],  2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	adds	C12 = 4 * SIZE, C4
	}
	{ .mfb
	nop	__LINE__
	FMA	f89   = f33, f51, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f74   = f34, f49, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f82   = f34, f50, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f90   = f34, f51, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f91   = f35, f51, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f76   = f36, f49, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f84   = f36, f50, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f92   = f36, f51, f92	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f69   = f37, f48, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f77   = f37, f49, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f85   = f37, f50, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f93   = f37, f51, f93	// A6 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f70   = f38, f48, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f78   = f38, f49, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f86   = f38, f50, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f94   = f38, f51, f94	// A7 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f71   = f39, f48, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f79   = f39, f49, f79	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],  2 * SIZE
	FMA	f87   = f39, f50, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f95   = f39, f51, f95	// A8 * B4
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
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
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
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f96 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f97 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f98 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f99 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f100 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f101 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f102 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f103 = [C9 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f104 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f105 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f106 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f107 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f92   = f44, f59, f92	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f108 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f109 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f110 = [C2 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f111 = [C10], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f93   = f45, f59, f93	// A6 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f112 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f113 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f114 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f86   = f46, f58, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f115 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f94   = f46, f59, f94	// A7 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f116 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f117 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f118 = [C3 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f87   = f47, f58, f87	// A8 * B3
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f119 = [C11], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f95   = f47, f59, f95	// A8 * B4
	br.cloop.sptk.few .L053
	}
	;;
	.align 32

.L058:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfi
	LDFD	f120 = [C4 ], SIZE
	FMA	f64  = ALPHA, f64, f96
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	LDFD	f121 = [C12], SIZE
	FMA	f68  = ALPHA, f68, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFD	f122 = [C4 ], SIZE
	FMA	f65  = ALPHA, f65, f98
	adds	I = -1, I
	}
	{ .mfb
	LDFD	f123 = [C12], SIZE
	FMA	f69  = ALPHA, f69, f99
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f124 = [C4 ], SIZE
	FMA	f66  = ALPHA, f66, f100
	nop	__LINE__
	}
	{ .mfb
	LDFD	f125 = [C12], SIZE
	FMA	f70  = ALPHA, f70, f101
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFD	f126 = [C4 ], -3 * SIZE
	FMA	f67  = ALPHA, f67, f102
	nop	__LINE__
	}
	{ .mfb
	LDFD	f127 = [C12], -3 * SIZE
	FMA	f71  = ALPHA, f71, f103
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMA	f72  = ALPHA, f72, f104
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f68, SIZE
	FMA	f76  = ALPHA, f76, f105
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, SIZE
	FMA	f73  = ALPHA, f73, f106
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f69, SIZE
	FMA	f77  = ALPHA, f77, f107
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f66, SIZE
	FMA	f74  = ALPHA, f74, f108
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f70, SIZE
	FMA	f78  = ALPHA, f78, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f67, 5 * SIZE
	FMA	f75  = ALPHA, f75, f110
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f71, 5 * SIZE
	FMA	f79  = ALPHA, f79, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMA	f80  = ALPHA, f80, f112
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f76, SIZE
	FMA	f84  = ALPHA, f84, f113
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	FMA	f81  = ALPHA, f81, f114
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f77, SIZE
	FMA	f85  = ALPHA, f85, f115
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f74, SIZE
	FMA	f82  = ALPHA, f82, f116
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f78, SIZE
	FMA	f86  = ALPHA, f86, f117
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f75, 5 * SIZE
	FMA	f83  = ALPHA, f83, f118
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f79, 5 * SIZE
	FMA	f87  = ALPHA, f87, f119
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMA	f88  = ALPHA, f88, f120
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f84, SIZE
	FMA	f92  = ALPHA, f92, f121
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, SIZE
	FMA	f89  = ALPHA, f89, f122
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f85, SIZE
	FMA	f93  = ALPHA, f93, f123
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f82, SIZE
	FMA	f90  = ALPHA, f90, f124
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f86, SIZE
	FMA	f94  = ALPHA, f94, f125
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f83, 5 * SIZE
	FMA	f91  = ALPHA, f91, f126
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f87, 5 * SIZE
	FMA	f95  = ALPHA, f95, f127
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f92, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f89, SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f93, SIZE
	mov	f88  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f90, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f94, SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C4 ] = f91, 5 * SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C12] = f95, 5 * SIZE
	mov	f89  = f0
	(p6)	br.cond.dptk .L052
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	nop	__LINE__
	FMPY	f68  = ALPHA, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
	adds	I = -1, I
	}
	{ .mfb
	nop	__LINE__
	FMPY	f69  = ALPHA, f69
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f66  = ALPHA, f66
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f70  = ALPHA, f70
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f67  = ALPHA, f67
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f71  = ALPHA, f71
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMPY	f72  = ALPHA, f72
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f68, SIZE
	FMPY	f76  = ALPHA, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, SIZE
	FMPY	f73  = ALPHA, f73
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f69, SIZE
	FMPY	f77  = ALPHA, f77
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f66, SIZE
	FMPY	f74  = ALPHA, f74
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f70, SIZE
	FMPY	f78  = ALPHA, f78
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f67, 5 * SIZE
	FMPY	f75  = ALPHA, f75
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f71, 5 * SIZE
	FMPY	f79  = ALPHA, f79
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMPY	f80  = ALPHA, f80
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f76, SIZE
	FMPY	f84  = ALPHA, f84
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	FMPY	f81  = ALPHA, f81
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f77, SIZE
	FMPY	f85  = ALPHA, f85
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f74, SIZE
	FMPY	f82  = ALPHA, f82
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f78, SIZE
	FMPY	f86  = ALPHA, f86
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f75, 5 * SIZE
	FMPY	f83  = ALPHA, f83
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f79, 5 * SIZE
	FMPY	f87  = ALPHA, f87
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	FMPY	f88  = ALPHA, f88
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f84, SIZE
	FMPY	f92  = ALPHA, f92
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, SIZE
	FMPY	f89  = ALPHA, f89
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f85, SIZE
	FMPY	f93  = ALPHA, f93
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f82, SIZE
	FMPY	f90  = ALPHA, f90
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C11] = f86, SIZE
	FMPY	f94  = ALPHA, f94
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f83, 5 * SIZE
	FMPY	f91  = ALPHA, f91
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C11] = f87, 5 * SIZE
	FMPY	f95  = ALPHA, f95
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C4 ] = f88, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C12] = f92, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f89, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C12] = f93, SIZE
	mov	f88  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C4 ] = f90, SIZE
	mov	f65  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 8, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C12] = f94, SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f91, 5 * SIZE
	mov	f81  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C12] = f95, 5 * SIZE
	mov	f89  = f0
	(p6)	br.cond.dptk .L052
	}
	;;
#endif
	.align 32

.L060:
	{ .mfi
	nop	__LINE__
	mov	f66  = f0
	tbit.z	p6, p7  = M, 2
	}
	{ .mfb
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
	mov	f74  = f0
	(p6)	br.cond.dptk .L070
	}
	;;

#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f82  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
	mov	f90  = f0
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
	mov	f82  = f0
	shladd	AOFFSET = KK8, 2, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f90  = f0
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
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f67  = f0
	adds	L =  -1, L
	}
	{ .mfi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	mov	f75  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f83  = f0
	mov	ar.lc = L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	mov	f91  = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	.align 32

.L062:
	{ .mfi
	lfetch.nt1	[PREA],  8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],   8 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	(p5) adds	C9  = 2 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f88   = f32, f51, f88	// A1 * B4
	(p5) adds	C10 = 2 * SIZE, C2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	(p5) adds	C11 = 2 * SIZE, C3
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	(p5) adds	C12 = 2 * SIZE, C4
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f89   = f33, f51, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f74   = f34, f49, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f82   = f34, f50, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f90   = f34, f51, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}

	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f70 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f91   = f35, f51, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f69 = [C1 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f71 = [C9 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f78 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77 = [C2 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f79 = [C10], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f84 = [C3 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f86 = [C11], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f85 = [C3 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f87 = [C11], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92 = [C4 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f94 = [C12], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f93 = [C4 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f95 = [C12], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	br.cloop.sptk.few .L062
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f66  = ALPHA, f66, f70
	FMA	f65  = ALPHA, f65, f69
	FMA	f67  = ALPHA, f67, f71
	FMA	f72  = ALPHA, f72, f76
	FMA	f74  = ALPHA, f74, f78
	FMA	f73  = ALPHA, f73, f77
	FMA	f75  = ALPHA, f75, f79
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMA	f80  = ALPHA, f80, f84
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f66, SIZE
	FMA	f82  = ALPHA, f82, f86
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, 3 * SIZE
	FMA	f81  = ALPHA, f81, f85
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f67, 3 * SIZE
	FMA	f83  = ALPHA, f83, f87
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	FMA	f88  = ALPHA, f88, f92
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f74, SIZE
	FMA	f90  = ALPHA, f90, f94
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, 3 * SIZE
	FMA	f89  = ALPHA, f89, f93
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f75, 3 * SIZE
	FMA	f91  = ALPHA, f91, f95
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f82, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C3 ] = f81, 3 * SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C11] = f83, 3 * SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
	adds	L =  1, K
	}
	{ .mfb
	STFD	[C12] = f90, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f89, 3 * SIZE
	mov	f89  = f0
	shr	L = L, 1
	}
	{ .mfb
	STFD	[C12] = f91, 3 * SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
#else
	FMPY	f64  = ALPHA, f64
	FMPY	f66  = ALPHA, f66
	FMPY	f65  = ALPHA, f65
	FMPY	f67  = ALPHA, f67
	FMPY	f72  = ALPHA, f72
	FMPY	f74  = ALPHA, f74
	FMPY	f73  = ALPHA, f73
	FMPY	f75  = ALPHA, f75
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMPY	f80  = ALPHA, f80
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f66, SIZE
	FMPY	f82  = ALPHA, f82
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, 3 * SIZE
	FMPY	f81  = ALPHA, f81
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f67, 3 * SIZE
	FMPY	f83  = ALPHA, f83
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f72, SIZE
	FMPY	f88  = ALPHA, f88
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C10] = f74, SIZE
	FMPY	f90  = ALPHA, f90
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f73, 3 * SIZE
	FMPY	f89  = ALPHA, f89
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C10] = f75, 3 * SIZE
	FMPY	f91  = ALPHA, f91
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C11] = f82, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f81, 3 * SIZE
	mov	f81  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C11] = f83, 3 * SIZE
	mov	f72  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C12] = f90, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C4 ] = f89, 3 * SIZE
	mov	f89  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C12] = f91, 3 * SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
#endif
	.align 32

.L070:
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
	tbit.z	p6,p7  = M, 1
	(p6)	br.cond.dptk .L080
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
	shladd	BOFFSET = KK8, 2, B
	shladd	AOFFSET = KK8, 1, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	nop	__LINE__
	}
	;;
#endif
	{ .mii
	cmp.eq	p3, p0 = r0, r0
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	adds	L =  -1, L
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	;;
	{ .mmi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	mov	ar.lc = L
	}
	;;
	.align 32

.L072:
	{ .mfb
	lfetch.nt1	[PREA],  4 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
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
	FMA	f88   = f32, f51, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	}
	{ .mmf
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1 ], SIZE
	(p5) LDFD	f76 = [C2 ], SIZE
#else
	nop	__LINE__
	nop	__LINE__
#endif
	FMA	f89   = f33, f51, f89	// A2 * B4
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mmf
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f69 = [C1 ], -1 * SIZE
	(p5) LDFD	f77 = [C2 ], -1 * SIZE
#else
	nop	__LINE__
	nop	__LINE__
#endif
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mmf
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f84 = [C3 ], SIZE
	(p5) LDFD	f92 = [C4 ], SIZE
#else
	nop	__LINE__
	nop	__LINE__
#endif
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f85 = [C3 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
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
	(p5) LDFD	f93 = [C4 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	br.cloop.sptk.few .L072
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f65  = ALPHA, f65, f69
	FMA	f72  = ALPHA, f72, f76
	FMA	f73  = ALPHA, f73, f77

	FMA	f80  = ALPHA, f80, f84
	FMA	f81  = ALPHA, f81, f85
	FMA	f88  = ALPHA, f88, f92
	FMA	f89  = ALPHA, f89, f93
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	STFD	[C2 ] = f73, SIZE
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
	adds	L =  1, K
	}
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C3 ] = f81, SIZE
	STFD	[C4 ] = f89, SIZE
	shr	L = L, 1
	}
	;;
#else
	FMPY	f64  = ALPHA, f64
	FMPY	f65  = ALPHA, f65
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f72  = ALPHA, f72
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f73  = ALPHA, f73
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f80  = ALPHA, f80
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	FMPY	f81  = ALPHA, f81
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f88  = ALPHA, f88
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f89  = ALPHA, f89
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	STFD	[C2 ] = f73, SIZE
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C3 ] = f81, SIZE
	STFD	[C4 ] = f89, SIZE
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
#endif
	.align 32

.L080:
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
	tbit.z	p6,p7  = M, 0
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
	shladd	BOFFSET = KK8, 2, B
	add	AOFFSET = KK8, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	nop	__LINE__
	}
	;;
#endif

	{ .mii
	LDFD	f32 = [AOFFSET], 1 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	adds	L =  -1, L
	}
	;;
	{ .mmi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	cmp.eq	p3, p0 = r0, r0
	mov	ar.lc = L
	}
	;;
	.align 32

.L082:
	{ .mfb
	cmp.ne	p4, p5 =  0, L
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	(p12) cmp.ne p3, p0 =  0, L
	FMA	f72   = f32, f49, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfb
	(p3) LDFD	f40 = [AOFFSET], 1 * SIZE
	FMA	f88   = f32, f51, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1]
#else
	nop	__LINE__
#endif
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mmf
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p4) LDFD	f32 = [AOFFSET],   1 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	}
	{ .mmf
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C2]
	(p5) LDFD	f84 = [C3]
#else
	nop	__LINE__
	nop	__LINE__
#endif
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mib
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	{ .mmb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f92 = [C4]
#else
	nop	__LINE__
#endif
	adds	L = -1, L
	br.cloop.sptk.few .L082
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f72  = ALPHA, f72, f76
	FMA	f80  = ALPHA, f80, f84
	FMA	f88  = ALPHA, f88, f92
	;;
	STFD	[C1 ] = f64, SIZE
	STFD	[C2 ] = f72, SIZE
	STFD	[C3 ] = f80, SIZE
	STFD	[C4 ] = f88, SIZE
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f72  = ALPHA, f72
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f80  = ALPHA, f80
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	FMPY	f88  = ALPHA, f88
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	AOFFSET = KK8, AOFFSET
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
	STFD	[C1 ] = f64, SIZE
	STFD	[C2 ] = f72, SIZE
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 1, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
	STFD	[C3 ] = f80, SIZE
	STFD	[C4 ] = f88, SIZE
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
#endif
	.align 32

.L089:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
#if defined(TRMMKERNEL) && !defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	;;
	.align 16
#endif

.L090:
	{ .mfi
 	mov	C1 = C
	mov	f64  = f0
	tbit.z	p6, p0 = N, 1
	}
	{ .mfi
	add	C2 = LDC, C
	mov	f72  = f0
	shr	I  = M, 3
	}
	;;
	{ .mfi
	setf.d	f66  = r0
	mov	f65  = f0
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	mov	AOFFSET = A
	mov	f73  = f0
	(p6)	br.cond.dpnt .L130
	}
	;;
#if 0
	{ .mfi
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	mov	f67  = f0
	shladd	C = LDC, 1, C
	}
	{ .mfb
	cmp.eq	p6, p7 = 0, I
	mov	f74  = f0
	(p6)	br.cond.dpnt .L100
	}
	;;
	.align 32

.L092:
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f68  = f0
	nop	__LINE__
	}
	{ .mfb
	adds	BOFFSET = 2 * SIZE, B
	mov	f79  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 1, B
	mov	f68  = f0
	shladd	AOFFSET = KK8, 3, AOFFSET
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f79  = f0
	nop	__LINE__
	}
	;;
#endif

	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f75  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 8, KK
#else
	adds	L = 2, KK
#endif
#endif
	}
	;;
	{ .mfi
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	mov	f76  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mfi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	mov	f69  = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f77  = f0
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	adds	L =  -1, L
	}
	{ .mmf
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	CPREFETCH [PREC], LDC
	mov	f70  = f0
	}
	;;
	{ .mfi
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	mov	f78  = f0
	mov	ar.lc = L
	}
	{ .mfi
	CPREFETCH [PREC]
	mov	f71  = f0
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	}
	;;
	.align 32

.L093:
/*  1 */
	{ .mfi
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],   4 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	adds	C9  = 4 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	adds	C11 = 4 * SIZE, C3
	}
	{ .mfi
	nop	__LINE__
	FMA	f74   = f34, f49, f74	// A3 * B2
	adds	C12 = 4 * SIZE, C4
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f96 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f97 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f76   = f36, f49, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f69   = f37, f48, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f98 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f77   = f37, f49, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	FMA	f70   = f38, f48, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f99 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f78   = f38, f49, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f71   = f39, f48, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f100 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f79   = f39, f49, f79	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],  2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f101 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f102 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f103 = [C9 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f104 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f105 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f106 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f107 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f108 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f109 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f110 = [C2 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f111 = [C10], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	br.cloop.sptk.few .L093
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfi
	nop	__LINE__
	FMA	f64  = ALPHA, f64, f96
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	nop	__LINE__
	FMA	f68  = ALPHA, f68, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f65  = ALPHA, f65, f98
	adds	I = -1, I
	}
	{ .mfb
	nop	__LINE__
	FMA	f69  = ALPHA, f69, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMA	f66  = ALPHA, f66, f100
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f70  = ALPHA, f70, f101
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMA	f67  = ALPHA, f67, f102
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f71  = ALPHA, f71, f103
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMA	f72  = ALPHA, f72, f104
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f68, SIZE
	FMA	f76  = ALPHA, f76, f105
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, SIZE
	FMA	f73  = ALPHA, f73, f106
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f69, SIZE
	FMA	f77  = ALPHA, f77, f107
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f66, SIZE
	FMA	f74  = ALPHA, f74, f108
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f70, SIZE
	FMA	f78  = ALPHA, f78, f109
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f67, 5 * SIZE
	FMA	f75  = ALPHA, f75, f110
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f71, 5 * SIZE
	FMA	f79  = ALPHA, f79, f111
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f76, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f77, SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f74, SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f78, SIZE
	mov	f74  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C2 ] = f75, 5 * SIZE
	mov	f67  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C10] = f79, 5 * SIZE
	(p6)	br.cond.dptk .L092
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	nop	__LINE__
	FMPY	f68  = ALPHA, f68
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
	adds	I = -1, I
	}
	{ .mfb
	nop	__LINE__
	FMPY	f69  = ALPHA, f69
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f66  = ALPHA, f66
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f70  = ALPHA, f70
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	FMPY	f67  = ALPHA, f67
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f71  = ALPHA, f71
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f64, SIZE
	FMPY	f72  = ALPHA, f72
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f68, SIZE
	FMPY	f76  = ALPHA, f76
	nop	__LINE__
	}
	;;
	{ .mfb
	STFD	[C1 ] = f65, SIZE
	FMPY	f73  = ALPHA, f73
	nop	__LINE__
	}
	{ .mfb
	STFD	[C9 ] = f69, SIZE
	FMPY	f77  = ALPHA, f77
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f66, SIZE
	FMPY	f74  = ALPHA, f74
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C9 ] = f70, SIZE
	FMPY	f78  = ALPHA, f78
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f67, 5 * SIZE
	FMPY	f75  = ALPHA, f75
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C9 ] = f71, 5 * SIZE
	FMPY	f79  = ALPHA, f79
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C2 ] = f72, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C10] = f76, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f73, SIZE
	mov	f65  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	STFD	[C10] = f77, SIZE
	mov	f73  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C2 ] = f74, SIZE
	mov	f66  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 8, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C10] = f78, SIZE
	mov	f74  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f75, 5 * SIZE
	mov	f67  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mib
	STFD	[C10] = f79, 5 * SIZE
	nop	__LINE__
	(p6)	br.cond.dptk .L092
	}
	;;
#endif
	.align 32

.L100:
	{ .mib
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
	tbit.z	p6, p7 = M, 2
	(p6)	br.cond.dptk .L110
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmf
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	mov	f75  = f0
	}
	{ .mii
	nop	__LINE__
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK8, 1, B
	mov	f75  = f0
	shladd	AOFFSET = KK8, 2, AOFFSET
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  1, L
	}
	;;
#endif
	;;
	{ .mii
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  -1, L
	}
	;;
	{ .mmi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	cmp.eq	p3, p0 = r0, r0
	mov	ar.lc = L
	}
	;;
	.align 32

.L102:
	{ .mfi
	lfetch.nt1	[PREA],  8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	FMA	f72   = f32, f49, f72	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	lfetch.nt1	[PREB],  4 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	adds	C9  = 2 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	adds	C10 = 2 * SIZE, C2
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f74   = f34, f49, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f70 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f75   = f35, f49, f75	// A4 * B2
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
	(p5) LDFD	f69 = [C1 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f71 = [C9 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f76 = [C2 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f78 = [C10], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	br.cloop.sptk.few .L102
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfb
	LDFD	f77 = [C2 ], -1 * SIZE
	FMA	f64  = ALPHA, f64, f68
	nop	__LINE__
	}
	{ .mfb
	LDFD	f79 = [C10], -1 * SIZE
	FMA	f66  = ALPHA, f66, f70
	nop	__LINE__
	}
	;;
	FMA	f65  = ALPHA, f65, f69
	adds	L =  1, K
	FMA	f67  = ALPHA, f67, f71
	;;
	FMA	f72  = ALPHA, f72, f76
	shr	L = L, 1
	FMA	f74  = ALPHA, f74, f78
	FMA	f73  = ALPHA, f73, f77
	FMA	f75  = ALPHA, f75, f79
	;;
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f66, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f65, 3 * SIZE
	STFD	[C9 ] = f67, 3 * SIZE
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f72, SIZE
	STFD	[C10] = f74, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f73, 3 * SIZE
	STFD	[C10] = f75, 3 * SIZE
	mov	f73  = f0
	}
	;;
#else
	{ .mfb
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMPY	f66  = ALPHA, f66
	nop	__LINE__
	}
	;;
	FMPY	f65  = ALPHA, f65
	FMPY	f67  = ALPHA, f67
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f72  = ALPHA, f72
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f74  = ALPHA, f74
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f73  = ALPHA, f73
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f75  = ALPHA, f75
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mmi
	STFD	[C9 ] = f66, SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, 3 * SIZE
	mov	f65  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mmi
	STFD	[C9 ] = f67, 3 * SIZE
	nop	__LINE__
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mmi
	STFD	[C10] = f74, SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C2 ] = f73, 3 * SIZE
	mov	f73  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mib
	STFD	[C10] = f75, 3 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
#endif

	.align 32

.L110:
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
	(p6)	br.cond.dptk .L120
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
	shladd	AOFFSET = KK8, 1, AOFFSET
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  1, L
	}
	;;
#endif
	;;
	{ .mii
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  -1, L
	}
	;;
	{ .mmi
	cmp.eq	p3, p0 = r0, r0
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	mov	ar.lc = L
	}
	;;
	.align 32

.L112:
	{ .mfi
	lfetch.nt1	[PREA],  4 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	lfetch.nt1	[PREB],   4 * SIZE
	FMA	f72   = f32, f49, f72	// A1 * B2
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
	(p5) LDFD	f68 = [C1 ], SIZE
	(p5) LDFD	f76 = [C2 ], SIZE
#else
	nop	__LINE__
	nop	__LINE__
#endif
	FMA	f73   = f33, f49, f73	// A2 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f69 = [C1 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f77 = [C2 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	br.cloop.sptk.few .L112
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f65  = ALPHA, f65, f69
	FMA	f72  = ALPHA, f72, f76
	FMA	f73  = ALPHA, f73, f77
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	mov	f65  = f0
	nop	__LINE__
	}
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f72  = ALPHA, f72
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f73  = ALPHA, f73
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;

	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	mov	f65  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2 ] = f73, SIZE
	mov	f73  = f0
	nop	__LINE__
	}
	;;
#endif
	.align 32

.L120:
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
	(p6)	br.cond.dptk .L129
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
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L =  1, L
	}
	;;
#endif
	{ .mii
	nop	__LINE__
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFD	f32 = [AOFFSET], 1 * SIZE
	nop	__LINE__
	adds	L =  -1, L
	}
	;;
	{ .mmi
	cmp.eq	p3, p0 = r0, r0
	nop	__LINE__
	mov	ar.lc = L
	}
	;;
	.align 32

.L122:
	{ .mfi
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA	f72   = f32, f49, f72	// A1 * B2
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mmi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	(p3) LDFD	f40 = [AOFFSET], 1 * SIZE
	nop  __LINE__
	}
	{ .mmi
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1]
	(p5) LDFD	f76 = [C2]
#else
	nop  __LINE__
	nop  __LINE__
#endif
	nop  __LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	adds	L = -1, L
	}
	{ .mfb
	(p4) LDFD	f32 = [AOFFSET],   1 * SIZE
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	br.cloop.sptk.few .L122
	}
	;;

.L128:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f72  = ALPHA, f72, f76
	;;
	{ .mfi
	STFD	[C1 ] = f64
	mov	f64  = f0
	}
	{ .mfb
	STFD	[C2 ] = f72
	mov	f72  = f0
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f72  = ALPHA, f72
	nop	__LINE__
	}
	;;
	{ .mmi
	nop	__LINE__
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	AOFFSET = KK8, AOFFSET
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	BOFFSET = KK8, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;

#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 1, KK
#else
	nop	__LINE__
#endif
	;;
	{ .mfi
	STFD	[C1 ] = f64
	mov	f64  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C2 ] = f72
	mov	f72  = f0
	}
	;;
#endif
	.align 32

.L129:
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
#endif

.L130:
	{ .mfi
#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	KK  = OFFSET
#else
	nop	__LINE__
#endif
	mov	f64  = f0
	tbit.z	p6, p0 = N, 0
	}
	{ .mib
	mov	AOFFSET = A
	shr	I  = M, 3
	(p6)	br.cond.dpnt .L999
	}
	;;
#if 0
	{ .mfi
	mov	C1 = C
	mov	f65  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	nop	__LINE__
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mfb
	cmp.eq	p7, p0 = 0, I
 	mov	f67  = f0
	(p7)	br.cond.dpnt .L140
	}
	;;
	.align 32

.L132:
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mfb
	LDFD	f48 = [B]
	mov	f68  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 1 * SIZE, B
	mov	f69  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 8, KK
#else
	adds	L = 1, KK
#endif
#endif
	}
	;;
#else
	{ .mfi
	add	BOFFSET = KK8, B
	mov	f68  = f0
	shladd	AOFFSET = KK8, 3, AOFFSET
	}
	;;
	{ .mfi
	LDFD	f48 = [BOFFSET], 1 * SIZE
	mov	f69  = f0
#ifndef TRMMKERNEL
	nop	__LINE__
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	L = K, KK
#elif defined(LEFT)
	adds	L = 8, KK
#else
	adds	L = 1, KK
#endif
#endif
	}
	;;
#endif
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f70  = f0
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
	{ .mii
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mfi
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	mov	f71  = f0
	adds	L =  -1, L
	}
	;;
	{ .mmi
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mmi
	CPREFETCH [PREC]
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	mov	ar.lc = L
	}
	;;
	.align 32

.L133:
	{ .mfi
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	adds	PREB = (PREFETCHSIZE + 0) * SIZE, BOFFSET
	FMA	f65   = f33, f48, f65	// A2 * B1
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	adds	C9  = 4 * SIZE, C1
	}
	{ .mmf
	(p3) LDFD	f56 = [BOFFSET],   1 * SIZE
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f6  = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f7  = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f69   = f37, f48, f69	// A6 * B1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f70   = f38, f48, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f10 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	FMA	f71   = f39, f48, f71	// A8 * B1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f11 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mmf
	(p4) LDFD	f48 = [BOFFSET],  1 * SIZE
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f12 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f13 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	;;
	{ .mfi
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f14 = [C1 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f15 = [C9 ], -3 * SIZE
#else
	nop	__LINE__
#endif
	nop	__LINE__
	br.cloop.sptk.few .L133
	}
	;;

.L138:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	{ .mfi
	FMA	f64  = ALPHA, f64, f6
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	FMA	f68  = ALPHA, f68, f7
	}
	;;
	{ .mfi
	FMA	f65  = ALPHA, f65, f10
	adds	I = -1, I
	}
	{ .mfb
	FMA	f69  = ALPHA, f69, f11
	}
	;;
	{ .mfi
	FMA	f66  = ALPHA, f66, f12
	}
	{ .mfb
	FMA	f70  = ALPHA, f70, f13
	}
	;;
	{ .mfb
	FMA	f67  = ALPHA, f67, f14
	}
	{ .mfb
	FMA	f71  = ALPHA, f71, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	mov	f66  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f67, 5 * SIZE
	nop	__LINE__
	mov	f67  = f0
	}
	{ .mmb
	STFD	[C9 ] = f71, 5 * SIZE
	nop	__LINE__
	(p6)	br.cond.dptk .L132
	}
	;;
#else
	{ .mfi
	FMPY	f64  = ALPHA, f64
	cmp.ne	p6, p0 = 1, I
	}
	{ .mfb
	FMPY	f68  = ALPHA, f68
	}
	;;
	{ .mfi
	FMPY	f65  = ALPHA, f65
	adds	I = -1, I
	}
	{ .mfb
	FMPY	f69  = ALPHA, f69
	}
	;;
	{ .mfi
	FMPY	f66  = ALPHA, f66
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	FMPY	f70  = ALPHA, f70
	}
	;;
	{ .mfi
	FMPY	f67  = ALPHA, f67
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -8, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	FMPY	f71  = ALPHA, f71
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mmi
	STFD	[C9 ] = f68, SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	mov	f65  = f0
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mmi
	STFD	[C9 ] = f69, SIZE
	nop	__LINE__
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	BOFFSET = KK8, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f66, SIZE
	mov	f66  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 8, KK
#else
	nop	__LINE__
#endif
	}
	{ .mmi
	STFD	[C9 ] = f70, SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f67, 5 * SIZE
	mov	f67  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mmb
	STFD	[C9 ] = f71, 5 * SIZE
	nop	__LINE__
	(p6)	br.cond.dptk .L132
	}
	;;
#endif
	.align 32

.L140:
	{ .mib
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
	tbit.z	p6, p7 = M, 2
	(p6)	br.cond.dptk .L150
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFD	f48 = [B]
	adds	BOFFSET = 1 * SIZE, B
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mmi
	add	BOFFSET = KK8, B
	shladd	AOFFSET = KK8, 2, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mmi
	LDFD	f48 = [BOFFSET], 1 * SIZE
	nop	__LINE__
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	{ .mii
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	adds	L =  -1, L
	nop	__LINE__
	}
	;;
	{ .mmi
	adds	PREA = (PREFETCHSIZE + 0) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	mov	ar.lc = L
	}
	;;
	.align 32

.L142:
	{ .mfi
	lfetch.nt1	[PREA],  8 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	cmp.ne	p4, p5 =  0, L
	}
	{ .mfi
	nop	__LINE__
	FMA	f65   = f33, f48, f65	// A2 * B1
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	(p5) adds	C9  = 2 * SIZE, C1
	}
	{ .mmf
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) LDFD	f56 = [BOFFSET],   1 * SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	;;
	{ .mfi
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	(p5) adds	C10 = 2 * SIZE, C2
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f70 = [C9 ], SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mmf
	(p4) LDFD	f48 = [BOFFSET],   1 * SIZE
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f69 = [C1 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	}
	;;
	{ .mfi
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L = -1, L
	}
	{ .mfb
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f71 = [C9 ], -1 * SIZE
#else
	nop	__LINE__
#endif
	nop.f 0
	br.cloop.sptk.few .L142
	}
	;;

.L148:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
	FMA	f66  = ALPHA, f66, f70
	FMA	f65  = ALPHA, f65, f69
	FMA	f67  = ALPHA, f67, f71
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	adds	L =  1, K
	}
	{ .mfb
	STFD	[C9 ] = f66, SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, 3 * SIZE
	mov	f65  = f0
	shr	L = L, 1
	}
	{ .mfb
	STFD	[C9 ] = f67, 3 * SIZE
	mov	f67  = f0
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f66  = ALPHA, f66
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -4, L
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f67  = ALPHA, f67
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 2, AOFFSET
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	BOFFSET = KK8, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 4, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C9 ] = f66, SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, 3 * SIZE
	mov	f65  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	{ .mfb
	STFD	[C9 ] = f67, 3 * SIZE
	mov	f67  = f0
	nop	__LINE__
	}
	;;
#endif
	.align 32

.L150:
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
	(p6)	br.cond.dptk .L160
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFD	f48 = [B]
	adds	BOFFSET = 1 * SIZE, B
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mmi
	add	BOFFSET = KK8, B
	shladd	AOFFSET = KK8, 1, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mmi
	LDFD	f48 = [BOFFSET], 1 * SIZE
	nop	__LINE__
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#endif
	{ .mii
	cmp.eq	p3, p0 = r0, r0
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mii
	 LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	adds	L =  -1, L
	;;
	mov	ar.lc = L
	}
	;;
	.align 32

.L152:
	{ .mfi
	cmp.ne	p4, p5 =  0, L
	FMA	f64   = f32, f48, f64	// A1 * B1
	(p12) cmp.ne p3, p0 =  0, L
	}
	;;
	{ .mmf
	(p3) LDFD	f56 = [BOFFSET],   1 * SIZE
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	}
	;;
	{ .mfi
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	adds	L = -1, L
	}
	;;
	{ .mfb
	(p4) LDFD	f48 = [BOFFSET],   1 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	br.cloop.sptk.few .L152
	}
	;;

.L158:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	LDFD	f68 = [C1 ], SIZE
	;;
	LDFD	f69 = [C1 ], -1 * SIZE
	;;
	FMA	f64  = ALPHA, f64, f68
	FMA	f65  = ALPHA, f65, f69
	;;
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	mov	f65  = f0
	}
	;;
#else
	{ .mfi
	nop	__LINE__
	FMPY	f64  = ALPHA, f64
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	sub	L = K, KK
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FMPY	f65  = ALPHA, f65
	nop	__LINE__
	}
	;;
	{ .mii
	nop	__LINE__
#if defined(TRMMKERNEL) && (defined(LEFT) &&  defined(TRANSA))
	adds	L = -2, L
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && (!defined(LEFT) && !defined(TRANSA))
	adds	L = -1, L
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	KK8 = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	shladd	AOFFSET = KK8, 1, AOFFSET
#else
	nop	__LINE__
#endif
#if defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	add	BOFFSET = KK8, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
#if defined(TRMMKERNEL) && defined(LEFT)
	adds	KK = 2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	STFD	[C1 ] = f65, SIZE
	mov	f65  = f0
#ifdef TRMMKERNEL
	shladd	KK8 = KK, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
#endif
	.align 32

.L160:
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
	(p6)	br.cond.dptk .L169
	}
	;;
#if !defined(TRMMKERNEL) || \
    defined(TRMMKERNEL) && \
    ((defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA)))
	{ .mmi
	LDFD	f48 = [B]
	adds	BOFFSET = 1 * SIZE, B
#ifndef TRMMKERNEL
	adds	L =  1, K
#else
	adds	L =  1, L
#endif
	}
	;;
#else
	{ .mmi
	add	BOFFSET = KK8, B
	add	AOFFSET = KK8, AOFFSET
	nop	__LINE__
	}
	;;
	{ .mmi
	LDFD	f48 = [BOFFSET], 1 * SIZE
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
	LDFD f32 = [AOFFSET], 1 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mii
	adds	L =  -1, L
	cmp.eq	p3, p0 = r0, r0
	;;
	mov	ar.lc = L
	}
	;;
	.align 32

.L162:
	{ .mmf
	cmp.ne	p4, p5 =  0, L
	(p12) cmp.ne p3, p0 =  0, L
	FMA	f64   = f32, f48, f64	// A1 * B1
	}
	;;
	{ .mmi
	(p3) LDFD	f56 = [BOFFSET], 1 * SIZE
	(p3) LDFD	f40 = [AOFFSET], 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p4) LDFD	f32 = [AOFFSET],   1 * SIZE
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	(p5) LDFD	f68 = [C1]
#else
	nop	__LINE__
#endif
	adds	L = -1, L
	}
	{ .mfb
	(p4) LDFD	f48 = [BOFFSET],   1 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	br.cloop.sptk.few .L162
	}
	;;
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	FMA	f64  = ALPHA, f64, f68
#else
	FMPY	f64  = ALPHA, f64
#endif
	;;
	STFD	[C1 ] = f64
	;;
	.align 32

.L169:
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
#endif

.L999:
	mov	r8 = r0
	adds	r9 = 1 * 16, SP
	;;
	ldf.fill  f16 = [SP], 32
	ldf.fill  f17 = [r9], 32
	;;
	ldf.fill  f18 = [SP], 32
	ldf.fill  f19 = [r9], 32
	;;
	ldf.fill  f20 = [SP], 32
	ldf.fill  f21 = [r9], 32
	;;
	ldf.fill  f22 = [SP], 32
	ldf.fill  f23 = [r9], 32
	mov	 ar.lc = ARLC
	;;
       ldf.fill  f24 = [SP], 32
       ldf.fill  f25 = [r9], 32
	mov pr    = PR, -1
       ;;
       ldf.fill  f26 = [SP], 32
       ldf.fill  f27 = [r9], 32
	mov	ar.pfs = ARPFS
       ;;
       ldf.fill  f28 = [SP], 32
       ldf.fill  f29 = [r9], 32
       ;;
       ldf.fill  f30 = [SP], 32
       ldf.fill  f31 = [r9]

	br.ret.sptk.many b0
	EPILOGUE

