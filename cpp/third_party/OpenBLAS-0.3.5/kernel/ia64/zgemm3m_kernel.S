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

#define CPREFETCHSIZE 15
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

#define ALPHA_R	f8
#define ALPHA_I	f9

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 16, 0, 0
	adds	r14 = 16, SP
	mov	ARLC  = ar.lc
	}
	{ .mmi
	adds	r8 = -16 * 16, SP
	adds	r9 = -15 * 16, SP
	adds	SP = -16 * 16, SP
	}
	;;
	{ .mmi
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	mov	PR = pr
	}
	{ .mmi
	ld8	LDC   = [r14], 8
	nop	__LINE__
	nop	__LINE__
	}
	;;
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	shr	J = N, 3
	;;
	stf.spill  [r8] = f20, 32
	stf.spill  [r9] = f21, 32
	shladd	LDC = LDC, ZBASE_SHIFT, r0
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
	(p6)	br.cond.dpnt .L050
	.body
	;;
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
	nop	__LINE__
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
	nop	__LINE__
	}
	{ .mfb
	sub	C8 = C,  LDC		// coffset8 = c + 7 * ldc
	mov	f120 = f0
	(p6)	br.cond.dpnt .L020
	}
	;;
	.align 16

.L011:
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f65  = f0
	nop	__LINE__
	}
	{ .mfb
	adds	BOFFSET = 2 * SIZE, B
	mov	f73  = f0
	nop	__LINE__
	}
	;;
	{ .mfb
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f81  = f0
	nop	__LINE__
	}
	{ .mfb
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f89  = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	setf.d	f97  = r0
	mov	f105 = f0
	}
	{ .mfb
	setf.d	f113 = r0
	mov	f121 = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	setf.d	f66  = r0
	mov	f74  = f0
	}
	{ .mfb
	setf.d	f82  = r0
	mov	f90  = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	setf.d	f98  = r0
	mov	f106 = f0
	}
	{ .mfb
	setf.d	f114 = r0
	mov	f122 = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	setf.d	f67  = r0
	mov	f75  = f0
	}
	{ .mfi
	setf.d	f83  = r0
	mov	f91  = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
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
	adds	L =  1, K
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
	lfetch.nt1	[PREA],  16 * SIZE
	FMA	f64   = f32, f48, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	(p12) cmp.ne p3, p0 =  0, L
	FMA	f72   = f32, f49, f72	// A1 * B2
	nop	__LINE__
	}
	;;
/*  2 */
	{ .mfi
	lfetch.nt1	[PREB],  16 * SIZE
	FMA	f80   = f32, f50, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfi
	cmp.ne	p4, p5 =  0, L
	FMA	f88   = f32, f51, f88	// A1 * B4
	nop	__LINE__
	}
	;;
/*  3 */
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfi
	adds	C9  = 4 * SIZE, C1
	FMA	f104  = f32, f53, f104	// A1 * B6
	nop	__LINE__
	}
	;;
/*  4 */
	{ .mfi
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfi
	adds	C10 = 4 * SIZE, C2
	FMA	f120  = f32, f55, f120	// A1 * B8
	nop	__LINE__
	}
	;;
/*  5 */
	{ .mfi
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfi
	adds	C11 = 4 * SIZE, C3
	FMA	f73   = f33, f49, f73	// A2 * B2
	nop	__LINE__
	}
	;;
/*  6 */
	{ .mfi
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfi
	adds	C12 = 4 * SIZE, C4
	FMA	f89   = f33, f51, f89	// A2 * B4
	nop	__LINE__
	}
	;;
/*  7 */
	{ .mfi
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	FMA	f97   = f33, f52, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfi
	adds	C13 = 4 * SIZE, C5
	FMA	f105  = f33, f53, f105	// A2 * B6
	nop	__LINE__
	}
	;;
/*  8 */
	{ .mfi
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f113  = f33, f54, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfi
	adds	C14 = 4 * SIZE, C6
	FMA	f121  = f33, f55, f121	// A2 * B8
	nop	__LINE__
	}
	;;
/*  9 */
	{ .mfi
	(p3) LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfi
	adds	C15 = 4 * SIZE, C7
	FMA	f74   = f34, f49, f74	// A3 * B2
	nop	__LINE__
	}
	;;
/* 10 */
	{ .mfi
	(p3) LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	FMA	f82   = f34, f50, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfi
	adds	C16 = 4 * SIZE, C8
	FMA	f90   = f34, f51, f90	// A3 * B4
	nop	__LINE__
	}
	;;
/* 11 */
	{ .mfi
	FMA	f98   = f34, f52, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f106  = f34, f53, f106	// A3 * B6
	nop	__LINE__
	}
	;;
/* 12 */
	{ .mfi
	FMA	f114  = f34, f54, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f122  = f34, f55, f122	// A3 * B8
	nop	__LINE__
	}
	;;
/* 13 */
	{ .mfi
	nop	__LINE__
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	{ .mfi
	nop	__LINE__
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}
	;;
/* 14 */
	{ .mfi
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f91   = f35, f51, f91	// A4 * B4
	nop	__LINE__
	}
	;;
/* 15 */
	{ .mfi
	FMA	f99   = f35, f52, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f107  = f35, f53, f107	// A4 * B6
	nop	__LINE__
	}
	;;
/* 16 */
	{ .mfi
	FMA	f115  = f35, f54, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f123  = f35, f55, f123	// A4 * B8
	nop	__LINE__
	}
	;;
/* 17 */
	{ .mfi
	nop	__LINE__
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f76   = f36, f49, f76	// A5 * B2
	nop	__LINE__
	}
	;;
/* 18 */
	{ .mfi
	nop	__LINE__
	FMA	f84   = f36, f50, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f92   = f36, f51, f92	// A5 * B4
	nop	__LINE__
	}
	;;
/* 19 */
	{ .mfi
	nop	__LINE__
	FMA	f100  = f36, f52, f100	// A5 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f108  = f36, f53, f108	// A5 * B6
	nop	__LINE__
	}
	;;
/* 20 */
	{ .mfi
	nop	__LINE__
	FMA	f116  = f36, f54, f116	// A5 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f124  = f36, f55, f124	// A5 * B8
	nop	__LINE__
	}
	;;
/* 21 */
	{ .mfi
	nop	__LINE__
	FMA	f69   = f37, f48, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f77   = f37, f49, f77	// A6 * B2
	nop	__LINE__
	}
	;;
/* 22 */
	{ .mfi
	nop	__LINE__
	FMA	f85   = f37, f50, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f93   = f37, f51, f93	// A6 * B4
	nop	__LINE__
	}
	;;
/* 23 */
	{ .mfi
	nop	__LINE__
	FMA	f101  = f37, f52, f101	// A6 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f109  = f37, f53, f109	// A6 * B6
	nop	__LINE__
	}
	;;
/* 24 */
	{ .mfi
	nop	__LINE__
	FMA	f117  = f37, f54, f117	// A6 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f125  = f37, f55, f125	// A6 * B8
	nop	__LINE__
	}
	;;
/* 25 */
	{ .mfi
	nop	__LINE__
	FMA	f70   = f38, f48, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f78   = f38, f49, f78	// A7 * B2
	nop	__LINE__
	}
	;;
/* 26 */
	{ .mfi
	nop	__LINE__
	FMA	f86   = f38, f50, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f94   = f38, f51, f94	// A7 * B4
	nop	__LINE__
	}
	;;
/* 27 */
	{ .mfi
	nop	__LINE__
	FMA	f102  = f38, f52, f102	// A7 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f110  = f38, f53, f110	// A7 * B6
	nop	__LINE__
	}
	;;
/* 28 */
	{ .mfi
	nop	__LINE__
	FMA	f118  = f38, f54, f118	// A7 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f126  = f38, f55, f126	// A7 * B8
	nop	__LINE__
	}
	;;
/* 29 */
	{ .mfi
	nop	__LINE__
	FMA	f71   = f39, f48, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f79   = f39, f49, f79	// A8 * B2
	nop	__LINE__
	}
	;;
/* 30 */
	{ .mfi
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f87   = f39, f50, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f95   = f39, f51, f95	// A8 * B4
	nop	__LINE__
	}
	;;
/* 31 */
	{ .mfi
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f103  = f39, f52, f103	// A8 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f111  = f39, f53, f111	// A8 * B6
	nop	__LINE__
	}
	;;
/* 32 */
	{ .mfi
	nop	__LINE__
	FMA	f119  = f39, f54, f119	// A8 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMA	f127  = f39, f55, f127	// A8 * B8
	nop	__LINE__
	}
	;;
/* 33 */
	{ .mfi
	nop	__LINE__
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	nop	__LINE__
	}
	;;
/* 34 */
	{ .mfi
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
/* 35 */
	{ .mfi
	(p4) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	nop	__LINE__
	}
	;;
/* 36 */
	{ .mfi
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	;;
/* 37 */
	{ .mfi
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
/* 38 */
	{ .mfi
	(p4) LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
/* 39 */
	{ .mfi
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
	nop	__LINE__
	}
	;;
/* 40 */
	{ .mfi
	(p5) LDFD	f6  = [C1 ], SIZE
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f7  = [C9 ], SIZE
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	nop	__LINE__
	}
	;;
 /* 41 */
	{ .mfi
	(p5) LDFD	f10 = [C1 ], SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f11 = [C9 ], SIZE
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
/* 42 */
	{ .mfi
	(p5) LDFD	f12 = [C1 ], SIZE
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f13 = [C9 ], SIZE
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
/* 43 */
	{ .mfi
	(p5) LDFD	f14 = [C1 ], 5 * SIZE
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f15 = [C9 ], 5 * SIZE
	(p3) FMA	f106  = f42, f61, f106	// A3 * B6
	nop	__LINE__
	}
	;;
/* 44 */
	{ .mfi
	(p5) LDFD	f16 = [C1 ], SIZE
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f17 = [C9 ], SIZE
	(p3) FMA	f122  = f42, f63, f122	// A3 * B8
	nop	__LINE__
	}
	;;
/* 45 */
	{ .mfi
	(p5) LDFD	f18 = [C1 ], SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f19 = [C9 ], SIZE
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
/* 46 */
	{ .mfi
	(p5) LDFD	f20 = [C1 ], SIZE
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f21 = [C9 ], SIZE
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
/* 47 */
	{ .mfi
	(p5) LDFD	f22 = [C1 ], - 11 * SIZE
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f23 = [C9 ], - 11 * SIZE
	(p3) FMA	f107  = f43, f61, f107	// A4 * B6
	nop	__LINE__
	}
	;;
/* 48 */
	{ .mfi
	(p5) LDFD	f24 = [C2 ], SIZE
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f25 = [C10], SIZE
	(p3) FMA	f123  = f43, f63, f123	// A4 * B8
	nop	__LINE__
	}
	;;
/* 49 */
	{ .mfi
	(p5) LDFD	f26 = [C2 ], SIZE
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f27 = [C10], SIZE
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
/* 50 */
	{ .mfi
	(p5) LDFD	f28 = [C2 ], SIZE
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f29 = [C10], SIZE
	(p3) FMA	f92   = f44, f59, f92	// A5 * B4
	nop	__LINE__
	}
	;;
/* 51 */
	{ .mfi
	(p5) LDFD	f30 = [C2 ], 5 * SIZE
	(p3) FMA	f100  = f44, f60, f100	// A5 * B5
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f31 = [C10], 5 * SIZE
	(p3) FMA	f108  = f44, f61, f108	// A5 * B6
	nop	__LINE__
	}
	;;
/* 52 */
	{ .mfi
	(p5) LDFD	f32 = [C2 ], SIZE
	(p3) FMA	f116  = f44, f62, f116	// A5 * B7
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f33 = [C10], SIZE
	(p3) FMA	f124  = f44, f63, f124	// A5 * B8
	nop	__LINE__
	}
	;;
/* 53 */
	{ .mfi
	(p5) LDFD	f34 = [C2 ], SIZE
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f35 = [C10], SIZE
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
/* 54 */
	{ .mfi
	(p5) LDFD	f36 = [C2 ], SIZE
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f37 = [C10], SIZE
	(p3) FMA	f93   = f45, f59, f93	// A6 * B4
	nop	__LINE__
	}
	;;
/* 55 */
	{ .mfi
	(p5) LDFD	f38 = [C2 ], - 11 * SIZE
 	(p3) FMA	f101  = f45, f60, f101	// A6 * B5
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f39 = [C10], - 11 * SIZE
	(p3) FMA	f109  = f45, f61, f109	// A6 * B6
	nop	__LINE__
	}
	;;
/* 56 */
	{ .mfi
	(p5) LDFD	f48 = [C3 ], SIZE
	(p3) FMA	f117  = f45, f62, f117	// A6 * B7
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f49 = [C11], SIZE
	(p3) FMA	f125  = f45, f63, f125	// A6 * B8
	nop	__LINE__
	}
	;;
/* 57 */
	{ .mfi
	(p5) LDFD	f50 = [C3 ], SIZE
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f51 = [C11], SIZE
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
/* 58 */
	{ .mfi
	(p5) LDFD	f52 = [C3 ], SIZE
	(p3) FMA	f86   = f46, f58, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f53 = [C11], SIZE
	(p3) FMA	f94   = f46, f59, f94	// A7 * B4
	nop  __LINE__
	}
	;;
/* 59 */
	{ .mfi
	(p5) LDFD	f54 = [C3 ], 5 * SIZE
	(p3) FMA	f102  = f46, f60, f102	// A7 * B5
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f55 = [C11], 5 * SIZE
	(p3) FMA	f110  = f46, f61, f110	// A7 * B6
	nop	__LINE__
	}
	;;
/* 60 */
	{ .mfi
	(p5) LDFD	f40 = [C3 ], SIZE
	(p3) FMA	f118  = f46, f62, f118	// A7 * B7
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f41 = [C11], SIZE
	(p3) FMA	f126  = f46, f63, f126	// A7 * B8
	nop	__LINE__
	}
	;;
/* 61 */
	{ .mfi
	(p5) LDFD	f42 = [C3 ], SIZE
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f43 = [C11], SIZE
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	nop	__LINE__
	}
	;;
/* 62 */
	{ .mfi
	(p5) LDFD	f44 = [C3 ], SIZE
	(p3) FMA	f87   = f47, f58, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f45 = [C11], SIZE
	(p3) FMA	f95   = f47, f59, f95	// A8 * B4
	nop	__LINE__
	}
	;;
/* 63 */
	{ .mfi
	(p5) LDFD	f46 = [C3 ], - 11 * SIZE
	(p3) FMA	f103  = f47, f60, f103	// A8 * B5
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f56 = [C11], - 11 * SIZE
	(p3) FMA	f111  = f47, f61, f111	// A8 * B6
	nop	__LINE__
	}
	;;
/* 64 */
	{ .mfi
	(p5) LDFD	f57  = [C4 ], SIZE
	(p3) FMA	f119  = f47, f62, f119	// A8 * B7
	adds	L = -1, L
	}
	{ .mfb
	(p5) LDFD	f58  = [C12], SIZE
	(p3) FMA	f127  = f47, f63, f127	// A8 * B8
	br.cloop.sptk.few .L012
	}
	;;
.L013:
	{ .mmf
	(p5) LDFD	f59 = [C4 ], SIZE
	(p5) LDFD	f60 = [C12], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	cmp.ne	p6, p0 = 1, I
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	(p5) LDFD	f61 = [C4 ], SIZE
	(p5) LDFD	f62 = [C12], SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	(p5) LDFD	f63 = [C4 ], 5 * SIZE
	(p5) LDFD	f47 = [C12], 5 * SIZE
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mfi
	(p5) LDFD	f64 = [C4 ], SIZE
	FMA	f14  = ALPHA_I, f65, f14
	nop	__LINE__
	}
	{ .mfi
	(p5) LDFD	f65 = [C12], SIZE
	FMA	f15  = ALPHA_I, f67, f15
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16  = ALPHA_R, f68, f16
	}
	{ .mmf
	(p5) LDFD	f6 = [C4 ], SIZE
	(p5) LDFD	f7 = [C12], SIZE
	FMA	f17  = ALPHA_R, f70, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18  = ALPHA_I, f68, f18
	}
	{ .mmf
	(p5) LDFD	f10 = [C4 ], SIZE
	(p5) LDFD	f11 = [C12], SIZE
	FMA	f19  = ALPHA_I, f70, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20  = ALPHA_R, f69, f20
	}
	{ .mmf
	(p5) LDFD	f12 = [C4 ], - 11 * SIZE
	(p5) LDFD	f13 = [C12], - 11 * SIZE
	FMA	f21  = ALPHA_R, f71, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22  = ALPHA_I, f69, f22
	}
	{ .mmf
	(p5) LDFD	f14 = [C5 ], SIZE
	(p5) LDFD	f15 = [C13], SIZE
	FMA	f23  = ALPHA_I, f71, f23
	}
	;;
	{ .mmf
	STFD	[C1 ] = f16, SIZE
	STFD	[C9 ] = f17, SIZE
	FMA	f24  = ALPHA_R, f72, f24
	}
	{ .mmf
	(p5) LDFD	f16 = [C5 ], SIZE
	(p5) LDFD	f17 = [C13], SIZE
	FMA	f25  = ALPHA_R, f74, f25
	}
	;;
	{ .mmf
	STFD	[C1 ] = f18, SIZE
	STFD	[C9 ] = f19, SIZE
	FMA	f26  = ALPHA_I, f72, f26
	}
	{ .mmf
	(p5) LDFD	f18 = [C5 ], SIZE
	(p5) LDFD	f19 = [C13], SIZE
	FMA	f27  = ALPHA_I, f74, f27
	}
	;;
	{ .mmf
	STFD	[C1 ] = f20, SIZE
	STFD	[C9 ] = f21, SIZE
	FMA	f28  = ALPHA_R, f73, f28
	}
	{ .mmf
	(p5) LDFD	f20 = [C5 ], 5 * SIZE
	(p5) LDFD	f21 = [C13], 5 * SIZE
	FMA	f29  = ALPHA_R, f75, f29
	}
	;;
	{ .mmf
	STFD	[C1 ] = f22, 5 * SIZE
	STFD	[C9 ] = f23, 5 * SIZE
	FMA	f30  = ALPHA_I, f73, f30
	}
	{ .mmf
	(p5) LDFD	f22 = [C5 ], SIZE
	(p5) LDFD	f23 = [C13], SIZE
	FMA	f31  = ALPHA_I, f75, f31
	}
	;;
	{ .mmf
	STFD	[C2 ] = f24, SIZE
	STFD	[C10] = f25, SIZE
	FMA	f32  = ALPHA_R, f76, f32
	}
	{ .mmf
	(p5) LDFD	f24 = [C5 ], SIZE
	(p5) LDFD	f25 = [C13], SIZE
	FMA	f33  = ALPHA_R, f78, f33
	}
	;;
	{ .mmf
	STFD	[C2 ] = f26, SIZE
	STFD	[C10] = f27, SIZE
	FMA	f34  = ALPHA_I, f76, f34
	}
	{ .mmf
	(p5) LDFD	f26 = [C5 ], SIZE
	(p5) LDFD	f27 = [C13], SIZE
	FMA	f35  = ALPHA_I, f78, f35
	}
	;;
	{ .mmf
	STFD	[C2 ] = f28, SIZE
	STFD	[C10] = f29, SIZE
	FMA	f36  = ALPHA_R, f77, f36
	}
	{ .mmf
	(p5) LDFD	f28 = [C5 ], - 11 * SIZE
	(p5) LDFD	f29 = [C13], - 11 * SIZE
	FMA	f37  = ALPHA_R, f79, f37
	}
	;;
	{ .mmf
	STFD	[C2 ] = f30, 5 * SIZE
	STFD	[C10] = f31, 5 * SIZE
	FMA	f38  = ALPHA_I, f77, f38
	}
	{ .mmf
	(p5) LDFD	f30 = [C6 ], SIZE
	(p5) LDFD	f31 = [C14], SIZE
	FMA	f39  = ALPHA_I, f79, f39
	}
	;;
	{ .mmf
	STFD	[C2 ] = f32, SIZE
	STFD	[C10] = f33, SIZE
	FMA	f48  = ALPHA_R, f80, f48
	}
	{ .mmf
	(p5) LDFD	f32 = [C6 ], SIZE
	(p5) LDFD	f33 = [C14], SIZE
	FMA	f49  = ALPHA_R, f82, f49
	}
	;;
	{ .mmf
	STFD	[C2 ] = f34, SIZE
	STFD	[C10] = f35, SIZE
	FMA	f50  = ALPHA_I, f80, f50
	}
	{ .mmf
	(p5) LDFD	f34 = [C6 ], SIZE
	(p5) LDFD	f35 = [C14], SIZE
	FMA	f51  = ALPHA_I, f82, f51
	}
	;;
	{ .mmf
	STFD	[C2 ] = f36, SIZE
	STFD	[C10] = f37, SIZE
	FMA	f52  = ALPHA_R, f81, f52
	}
	{ .mmf
	(p5) LDFD	f36 = [C6 ], 5 * SIZE
	(p5) LDFD	f37 = [C14], 5 * SIZE
	FMA	f53  = ALPHA_R, f83, f53
	}
	;;
	{ .mmf
	STFD	[C2 ] = f38, 5 * SIZE
	STFD	[C10] = f39, 5 * SIZE
	FMA	f54  = ALPHA_I, f81, f54
	}
	{ .mmf
	(p5) LDFD	f38 = [C6 ], SIZE
	(p5) LDFD	f39 = [C14], SIZE
	FMA	f55  = ALPHA_I, f83, f55
	}
	;;
	{ .mmf
	STFD	[C3 ] = f48, SIZE
	STFD	[C11] = f49, SIZE
	FMA	f40  = ALPHA_R, f84, f40
	}
	{ .mmf
	(p5) LDFD	f48 = [C6 ], SIZE
	(p5) LDFD	f49 = [C14], SIZE
	FMA	f41  = ALPHA_R, f86, f41
	}
	;;
	{ .mmf
	STFD	[C3 ] = f50, SIZE
	STFD	[C11] = f51, SIZE
	FMA	f42  = ALPHA_I, f84, f42
	}
	{ .mmf
	(p5) LDFD	f50 = [C6 ], SIZE
	(p5) LDFD	f51 = [C14], SIZE
	FMA	f43  = ALPHA_I, f86, f43
	}
	;;
	{ .mmf
	STFD	[C3 ] = f52, SIZE
	STFD	[C11] = f53, SIZE
	FMA	f44  = ALPHA_R, f85, f44
	}
	{ .mmf
	(p5) LDFD	f52 = [C6 ], - 11 * SIZE
	(p5) LDFD	f53 = [C14], - 11 * SIZE
	FMA	f45  = ALPHA_R, f87, f45
	}
	;;
	{ .mmf
	STFD	[C3 ] = f54, 5 * SIZE
	STFD	[C11] = f55, 5 * SIZE
	FMA	f46  = ALPHA_I, f85, f46
	}
	{ .mmf
	(p5) LDFD	f54 = [C7 ], SIZE
	(p5) LDFD	f55 = [C15], SIZE
	FMA	f56  = ALPHA_I, f87, f56
	}
	;;
	{ .mmf
	STFD	[C3 ] = f40, SIZE
	STFD	[C11] = f41, SIZE
	FMA	f57  = ALPHA_R, f88, f57
	}
	{ .mmf
	(p5) LDFD	f40 = [C7 ], SIZE
	(p5) LDFD	f41 = [C15], SIZE
	FMA	f58  = ALPHA_R, f90, f58
	}
	;;
	{ .mmf
	STFD	[C3 ] = f42, SIZE
	STFD	[C11] = f43, SIZE
	FMA	f59  = ALPHA_I, f88, f59
	}
	{ .mmf
	(p5) LDFD	f42 = [C7 ], SIZE
	(p5) LDFD	f43 = [C15], SIZE
	FMA	f60  = ALPHA_I, f90, f60
	}
	;;
	{ .mmf
	STFD	[C3 ] = f44, SIZE
	STFD	[C11] = f45, SIZE
	FMA	f61  = ALPHA_R, f89, f61
	}
	{ .mmf
	(p5) LDFD	f44 = [C7 ], 5 * SIZE
	(p5) LDFD	f45 = [C15], 5 * SIZE
	FMA	f62  = ALPHA_R, f91, f62
	}
	;;
	{ .mmf
	STFD	[C3 ] = f46, 5 * SIZE
	STFD	[C11] = f56, 5 * SIZE
	FMA	f63  = ALPHA_I, f89, f63
	}
	{ .mmf
	(p5) LDFD	f46 = [C7 ], SIZE
	(p5) LDFD	f56 = [C15], SIZE
	FMA	f47  = ALPHA_I, f91, f47
	}
	;;
	{ .mmf
	STFD	[C4 ] = f57, SIZE
	STFD	[C12] = f58, SIZE
	FMA	f64  = ALPHA_R, f92, f64
	}
	{ .mmf
	(p5) LDFD	f57 = [C7 ], SIZE
	(p5) LDFD	f58 = [C15], SIZE
	FMA	f65  = ALPHA_R, f94, f65
	}
	;;
	{ .mmf
	STFD	[C4 ] = f59, SIZE
	STFD	[C12] = f60, SIZE
	FMA	f6   = ALPHA_I, f92, f6
	}
	{ .mmf
	(p5) LDFD	f59 = [C7 ], SIZE
	(p5) LDFD	f60 = [C15], SIZE
	FMA	f7   = ALPHA_I, f94, f7
	}
	;;
	{ .mmf
	STFD	[C4 ] = f61, SIZE
	STFD	[C12] = f62, SIZE
	FMA	f10  = ALPHA_R, f93, f10
	}
	{ .mmf
	(p5) LDFD	f61 = [C7 ], - 11 * SIZE
	(p5) LDFD	f62 = [C15], - 11 * SIZE
	FMA	f11  = ALPHA_R, f95, f11
	}
	;;
	{ .mmf
	STFD	[C4 ] = f63, 5 * SIZE
	STFD	[C12] = f47, 5 * SIZE
	FMA	f12  = ALPHA_I, f93, f12
	}
	{ .mmf
	(p5) LDFD	f63 = [C8 ], SIZE
	(p5) LDFD	f47 = [C16], SIZE
	FMA	f13  = ALPHA_I, f95, f13
	}
	;;
	{ .mmf
	STFD	[C4 ] = f64, SIZE
	STFD	[C12] = f65, SIZE
	FMA	f14  = ALPHA_R, f96, f14
	}
	{ .mmf
	(p5) LDFD	f64 = [C8 ], SIZE
	(p5) LDFD	f65 = [C16], SIZE
	FMA	f15  = ALPHA_R, f98, f15
	}
	;;
	{ .mmf
	STFD	[C4 ] = f6, SIZE
	STFD	[C12] = f7, SIZE
	FMA	f16  = ALPHA_I, f96, f16
	}
	{ .mmf
	(p5) LDFD	f6  = [C8 ], SIZE
	(p5) LDFD	f7  = [C16], SIZE
	FMA	f17  = ALPHA_I, f98, f17
	}
	;;
	{ .mmf
	STFD	[C4 ] = f10, SIZE
	STFD	[C12] = f11, SIZE
	FMA	f18  = ALPHA_R, f97, f18
	}
	{ .mmf
	(p5) LDFD	f10 = [C8 ], 5 * SIZE
	(p5) LDFD	f11 = [C16], 5 * SIZE
	FMA	f19  = ALPHA_R, f99, f19
	}
	;;
	{ .mmf
	STFD	[C4 ] = f12, 5 * SIZE
	STFD	[C12] = f13, 5 * SIZE
	FMA	f20  = ALPHA_I, f97, f20
	}
	{ .mmf
	(p5) LDFD	f12 = [C8 ], SIZE
	(p5) LDFD	f13 = [C16], SIZE
	FMA	f21  = ALPHA_I, f99, f21
	}
	;;
	{ .mmf
	STFD	[C5 ] = f14, SIZE
	STFD	[C13] = f15, SIZE
	FMA	f22  = ALPHA_R, f100, f22
	}
	{ .mmf
	(p5) LDFD	f14 = [C8 ], SIZE
	(p5) LDFD	f15 = [C16], SIZE
	FMA	f23  = ALPHA_R, f102, f23
	}
	;;
	{ .mmf
	STFD	[C5 ] = f16, SIZE
	STFD	[C13] = f17, SIZE
	FMA	f24  = ALPHA_I, f100, f24
	}
	{ .mmf
	(p5) LDFD	f16 = [C8 ], SIZE
	(p5) LDFD	f17 = [C16], SIZE
	FMA	f25  = ALPHA_I, f102, f25
	}
	;;
	{ .mmf
	STFD	[C5 ] = f18, SIZE
	STFD	[C13] = f19, SIZE
	FMA	f26  = ALPHA_R, f101, f26
	}
	{ .mmf
	(p5) LDFD	f18 = [C8 ], - 11 * SIZE
	(p5) LDFD	f19 = [C16], - 11 * SIZE
	FMA	f27  = ALPHA_R, f103, f27
	}
	;;
	{ .mmf
	STFD	[C5 ] = f20, 5 * SIZE
	STFD	[C13] = f21, 5 * SIZE
	FMA	f28  = ALPHA_I, f101, f28
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f29  = ALPHA_I, f103, f29
	}
	;;
	{ .mmf
	STFD	[C5 ] = f22, SIZE
	STFD	[C13] = f23, SIZE
	FMA	f30  = ALPHA_R, f104, f30
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f31  = ALPHA_R, f106, f31
	}
	;;
	{ .mmf
	STFD	[C5 ] = f24, SIZE
	STFD	[C13] = f25, SIZE
	FMA	f32  = ALPHA_I, f104, f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33  = ALPHA_I, f106, f33
	}
	;;
	{ .mmf
	STFD	[C5 ] = f26, SIZE
	STFD	[C13] = f27, SIZE
	FMA	f34  = ALPHA_R, f105, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35  = ALPHA_R, f107, f35
	}
	;;
	{ .mmf
	STFD	[C5 ] = f28, 5 * SIZE
	STFD	[C13] = f29, 5 * SIZE
	FMA	f36  = ALPHA_I, f105, f36
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f37  = ALPHA_I, f107, f37
	}
	;;
	{ .mmf
	STFD	[C6 ] = f30, SIZE
	STFD	[C14] = f31, SIZE
	FMA	f38  = ALPHA_R, f108, f38
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f39  = ALPHA_R, f110, f39
	}
	;;
	{ .mmf
	STFD	[C6 ] = f32, SIZE
	STFD	[C14] = f33, SIZE
	FMA	f48  = ALPHA_I, f108, f48
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f49  = ALPHA_I, f110, f49
	}
	;;
	{ .mmf
	STFD	[C6 ] = f34, SIZE
	STFD	[C14] = f35, SIZE
	FMA	f50  = ALPHA_R, f109, f50
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f51  = ALPHA_R, f111, f51
	}
	;;
	{ .mmf
	STFD	[C6 ] = f36, 5 * SIZE
	STFD	[C14] = f37, 5 * SIZE
	FMA	f52  = ALPHA_I, f109, f52
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f53  = ALPHA_I, f111, f53
	}
	;;
	{ .mmf
	STFD	[C6 ] = f38, SIZE
	STFD	[C14] = f39, SIZE
	FMA	f54  = ALPHA_R, f112, f54
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f55  = ALPHA_R, f114, f55
	}
	;;
	{ .mmf
	STFD	[C6 ] = f48, SIZE
	STFD	[C14] = f49, SIZE
	FMA	f40  = ALPHA_I, f112, f40
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f41  = ALPHA_I, f114, f41
	}
	;;
	{ .mmf
	STFD	[C6 ] = f50, SIZE
	STFD	[C14] = f51, SIZE
	FMA	f42  = ALPHA_R, f113, f42
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f43  = ALPHA_R, f115, f43
	}
	;;
	{ .mmf
	STFD	[C6 ] = f52, 5 * SIZE
	STFD	[C14] = f53, 5 * SIZE
	FMA	f44  = ALPHA_I, f113, f44
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f45  = ALPHA_I, f115, f45
	}
	;;
	{ .mmf
	STFD	[C7 ] = f54, SIZE
	STFD	[C15] = f55, SIZE
	FMA	f46  = ALPHA_R, f116, f46
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f56  = ALPHA_R, f118, f56
	}
	;;
	{ .mmf
	STFD	[C7 ] = f40, SIZE
	STFD	[C15] = f41, SIZE
	FMA	f57  = ALPHA_I, f116, f57
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f58  = ALPHA_I, f118, f58
	}
	;;
	{ .mmf
	STFD	[C7 ] = f42, SIZE
	STFD	[C15] = f43, SIZE
	FMA	f59  = ALPHA_R, f117, f59
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f60  = ALPHA_R, f119, f60
	}
	;;
	{ .mmf
	STFD	[C7 ] = f44, 5 * SIZE
	STFD	[C15] = f45, 5 * SIZE
	FMA	f61  = ALPHA_I, f117, f61
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f62  = ALPHA_I, f119, f62
	}
	;;
	{ .mmf
	STFD	[C7 ] = f46, SIZE
	STFD	[C15] = f56, SIZE
	FMA	f63  = ALPHA_R, f120, f63
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f47  = ALPHA_R, f122, f47
	}
	;;
	{ .mmf
	STFD	[C7 ] = f57, SIZE
	STFD	[C15] = f58, SIZE
	FMA	f64  = ALPHA_I, f120, f64
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f65  = ALPHA_I, f122, f65
	}
	;;
	{ .mmf
	STFD	[C7 ] = f59, SIZE
	STFD	[C15] = f60, SIZE
	FMA	f6   = ALPHA_R, f121, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f123, f7
	}
	;;
	{ .mmf
	STFD	[C7 ] = f61, 5 * SIZE
	STFD	[C15] = f62, 5 * SIZE
	FMA	f10  = ALPHA_I, f121, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f123, f11
	}
	;;
	{ .mmf
	STFD	[C8 ] = f63, SIZE
	STFD	[C16] = f47, SIZE
	FMA	f12  = ALPHA_R, f124, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f126, f13
	}
	;;
	{ .mmf
	STFD	[C8 ] = f64, SIZE
	STFD	[C16] = f65, SIZE
	FMA	f14  = ALPHA_I, f124, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f126, f15
	}
	;;
	{ .mmf
	STFD	[C8 ] = f6,  SIZE
	STFD	[C16] = f7,  SIZE
	FMA	f16  = ALPHA_R, f125, f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f17  = ALPHA_R, f127, f17
	}
	;;
	{ .mmf
	STFD	[C8 ] = f10, 5 * SIZE
	STFD	[C16] = f11, 5 * SIZE
	FMA	f18  = ALPHA_I, f125, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f19  = ALPHA_I, f127, f19
	}
	;;
	{ .mmf
	STFD	[C8 ] = f12, SIZE
	STFD	[C16] = f13, SIZE
	mov	f64  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C8 ] = f14, SIZE
	STFD	[C16] = f15, SIZE
 	mov	f80  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f88  = f0
	}
	;;
	{ .mmf
	STFD	[C8 ] = f16, SIZE
	STFD	[C16] = f17, SIZE
	mov	f96  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f104 = f0
	}
	;;
	{ .mmf
	STFD	[C8 ] = f18, 5 * SIZE
	STFD	[C16] = f19, 5 * SIZE
	mov	f112 = f0
	}
	{ .mfb
	adds	I = -1, I
	mov	f120 = f0
	(p6)	br.cond.dptk .L011
	}
	;;

.L020:
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	mov	f89  = f0
	tbit.z	p6, p7 = M, 2
	}
	{ .mfb
	nop	__LINE__
	mov	f81  = f0
	(p6)	br.cond.dptk .L030
	}
	;;
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
	{ .mmf
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	setf.d	f97  = r0
	mov	f105 = f0
	}
	{ .mfi
	setf.d	f113 = r0
	mov	f121 = f0
	adds	L =  1, K
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
	(p5) adds	C9  = 4 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f104  = f32, f53, f104	// A1 * B6
	(p5) adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	(p5) adds	C11 = 4 * SIZE, C3
	}
	{ .mfi
	nop	__LINE__
	FMA	f120  = f32, f55, f120	// A1 * B8
	(p5) adds	C12 = 4 * SIZE, C4
	}
	;;
	{ .mfi
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	(p5) adds	C13 = 4 * SIZE, C5
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	(p5) adds	C14 = 4 * SIZE, C6
	}
	;;
	{ .mfi
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f81   = f33, f50, f81	// A2 * B3
	(p5) adds	C15 = 4 * SIZE, C7
	}
	{ .mfi
	nop	__LINE__
	FMA	f89   = f33, f51, f89	// A2 * B4
	(p5) adds	C16 = 4 * SIZE, C8
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
	;;
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
	;;
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
	(p5) LDFD	f6  = [C1 ], SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f7  = [C9 ], SIZE
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f10 = [C1 ], SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f11 = [C9 ], SIZE
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
	(p5) LDFD	f12 = [C1 ], SIZE
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f13 = [C9 ], SIZE
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f14 = [C1 ], - 3 * SIZE
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f15 = [C9 ], - 3 * SIZE
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f16 = [C2 ], SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f17 = [C10], SIZE
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f18 = [C2 ], SIZE
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f19 = [C10], SIZE
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f20 = [C2 ], SIZE
	(p3) FMA	f98   = f42, f60, f98	// A3 * B5
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f21 = [C10], SIZE
	(p3) FMA	f106  = f42, f61, f106	// A3 * B6
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f22 = [C2 ], - 3 * SIZE
	(p3) FMA	f114  = f42, f62, f114	// A3 * B7
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f23 = [C10], - 3 * SIZE
	(p3) FMA	f122  = f42, f63, f122	// A3 * B8
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f24 = [C3 ], SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f25 = [C11], SIZE
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f26 = [C3 ], SIZE
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f27 = [C11], SIZE
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f28 = [C3 ], SIZE
	(p3) FMA	f99   = f43, f60, f99	// A4 * B5
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f29 = [C11], SIZE
	(p3) FMA	f107  = f43, f61, f107	// A4 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
	(p5) LDFD	f30 = [C3 ], - 3 * SIZE
	(p3) FMA	f115  = f43, f62, f115	// A4 * B7
	adds	L = -1, L
	}
	{ .mfb
	(p5) LDFD	f31 = [C11], - 3 * SIZE
	(p3) FMA	f123  = f43, f63, f123	// A4 * B8
	br.cloop.sptk.few .L022
	}
	;;

.L028:
	{ .mmf
	LDFD	f68 = [C4 ], SIZE
	LDFD	f69 = [C12], SIZE
	FMA	f6  = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7  = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	LDFD	f70 = [C4 ], SIZE
	LDFD	f71 = [C12], SIZE
	FMA	f10 = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11 = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	LDFD	f76 = [C4 ], SIZE
	LDFD	f77 = [C12], SIZE
	FMA	f12 = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13 = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	LDFD	f78 = [C4 ], -3 * SIZE
	LDFD	f79 = [C12], -3 * SIZE
	FMA	f14 = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15 = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16 = ALPHA_R, f72, f16
	}
	{ .mmf
	LDFD	f84 = [C5 ], SIZE
	LDFD	f85 = [C13], SIZE
	FMA	f17 = ALPHA_R, f74, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18 = ALPHA_I, f72, f18
	}
	{ .mmf
	LDFD	f86 = [C5 ], SIZE
	LDFD	f87 = [C13], SIZE
	FMA	f19 = ALPHA_I, f74, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20 = ALPHA_R, f73, f20
	}
	{ .mmf
	LDFD	f92 = [C5 ], SIZE
	LDFD	f93 = [C13], SIZE
	FMA	f21 = ALPHA_R, f75, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22 = ALPHA_I, f73, f22
	}
	{ .mmf
	LDFD	f94 = [C5 ], -3 * SIZE
	LDFD	f95 = [C13], -3 * SIZE
	FMA	f23 = ALPHA_I, f75, f23
	}
	;;
	{ .mmf
	STFD	[C2 ] = f16, SIZE
	STFD	[C10] = f17, SIZE
	FMA	f24 = ALPHA_R, f80, f24
	}
	{ .mmf
	LDFD	f100 = [C6 ], SIZE
	LDFD	f101 = [C14], SIZE
	FMA	f25 = ALPHA_R, f82, f25
	}
	;;
	{ .mmf
	STFD	[C2 ] = f18, SIZE
	STFD	[C10] = f19, SIZE
	FMA	f26 = ALPHA_I, f80, f26
	}
	{ .mmf
	LDFD	f102 = [C6 ], SIZE
	LDFD	f103 = [C14], SIZE
	FMA	f27 = ALPHA_I, f82, f27
	}
	;;
	{ .mmf
	STFD	[C2 ] = f20, SIZE
	STFD	[C10] = f21, SIZE
	FMA	f28 = ALPHA_R, f81, f28
	}
	{ .mmf
	LDFD	f108 = [C6 ], SIZE
	LDFD	f109 = [C14], SIZE
	FMA	f29 = ALPHA_R, f83, f29
	}
	;;
	{ .mmf
	STFD	[C2 ] = f22, 5 * SIZE
	STFD	[C10] = f23, 5 * SIZE
	FMA	f30 = ALPHA_I, f81, f30
	}
	{ .mmf
	LDFD	f110 = [C6 ], -3 * SIZE
	LDFD	f111 = [C14], -3 * SIZE
	FMA	f31 = ALPHA_I, f83, f31
	}
	;;
	{ .mmf
	STFD	[C3 ] = f24, SIZE
	STFD	[C11] = f25, SIZE
	FMA	f68 = ALPHA_R, f88, f68
	}
	{ .mmf
	LDFD	f116 = [C7 ], SIZE
	LDFD	f117 = [C15], SIZE
	FMA	f69 = ALPHA_R, f90, f69
	}
	;;
	{ .mmf
	STFD	[C3 ] = f26, SIZE
	STFD	[C11] = f27, SIZE
	FMA	f70 = ALPHA_I, f88, f70
	}
	{ .mmf
	LDFD	f118 = [C7 ], SIZE
	LDFD	f119 = [C15], SIZE
	FMA	f71 = ALPHA_I, f90, f71
	}
	;;
	{ .mmf
	STFD	[C3 ] = f28, SIZE
	STFD	[C11] = f29, SIZE
	FMA	f76 = ALPHA_R, f89, f76
	}
	{ .mmf
	LDFD	f124 = [C7 ], SIZE
	LDFD	f125 = [C15], SIZE
	FMA	f77 = ALPHA_R, f91, f77
	}
	;;
	{ .mmf
	STFD	[C3 ] = f30, 5 * SIZE
	STFD	[C11] = f31, 5 * SIZE
	FMA	f78 = ALPHA_I, f89, f78
	}
	{ .mmf
	LDFD	f126 = [C7 ], -3 * SIZE
	LDFD	f127 = [C15], -3 * SIZE
	FMA	f79 = ALPHA_I, f91, f79
	}
	;;
	{ .mmf
	STFD	[C4 ] = f68, SIZE
	STFD	[C12] = f69, SIZE
	FMA	f84 = ALPHA_R, f96, f84
	}
	{ .mmf
	LDFD	f32 = [C8 ], SIZE
	LDFD	f33 = [C16], SIZE
	FMA	f85 = ALPHA_R, f98, f85
	}
	;;
	{ .mmf
	STFD	[C4 ] = f70, SIZE
	STFD	[C12] = f71, SIZE
	FMA	f86 = ALPHA_I, f96, f86
	}
	{ .mmf
	LDFD	f34 = [C8 ], SIZE
	LDFD	f35 = [C16], SIZE
	FMA	f87 = ALPHA_I, f98, f87
	}
	;;
	{ .mmf
	STFD	[C4 ] = f76, SIZE
	STFD	[C12] = f77, SIZE
	FMA	f92 = ALPHA_R, f97, f92
	}
	{ .mmf
	LDFD	f36 = [C8 ], SIZE
	LDFD	f37 = [C16], SIZE
	FMA	f93 = ALPHA_R, f99, f93
	}
	;;
	{ .mmf
	STFD	[C4 ] = f78, 5 * SIZE
	STFD	[C12] = f79, 5 * SIZE
	FMA	f94 = ALPHA_I, f97, f94
	}
	{ .mmf
	LDFD	f38 = [C8 ], -3 * SIZE
	LDFD	f39 = [C16], -3 * SIZE
	FMA	f95 = ALPHA_I, f99, f95
	}
	;;
	{ .mmf
	STFD	[C5 ] = f84, SIZE
	STFD	[C13] = f85, SIZE
	FMA	f100 = ALPHA_R, f104, f100
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f101 = ALPHA_R, f106, f101
	}
	;;
	{ .mmf
	STFD	[C5 ] = f86, SIZE
	STFD	[C13] = f87, SIZE
	FMA	f102 = ALPHA_I, f104, f102
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f103 = ALPHA_I, f106, f103
	}
	;;
	{ .mmf
	STFD	[C5 ] = f92, SIZE
	STFD	[C13] = f93, SIZE
	FMA	f108 = ALPHA_R, f105, f108
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f109 = ALPHA_R, f107, f109
	}
	;;
	{ .mmf
	STFD	[C5 ] = f94, 5 * SIZE
	STFD	[C13] = f95, 5 * SIZE
	FMA	f110 = ALPHA_I, f105, f110
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f111 = ALPHA_I, f107, f111
	}
	;;
	{ .mmf
	STFD	[C6 ] = f100, SIZE
	STFD	[C14] = f101, SIZE
	FMA	f116 = ALPHA_R, f112, f116
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f117 = ALPHA_R, f114, f117
	}
	;;
	{ .mmf
	STFD	[C6 ] = f102, SIZE
	STFD	[C14] = f103, SIZE
	FMA	f118 = ALPHA_I, f112, f118
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f119 = ALPHA_I, f114, f119
	}
	;;
	{ .mmf
	STFD	[C6 ] = f108, SIZE
	STFD	[C14] = f109, SIZE
	FMA	f124 = ALPHA_R, f113, f124
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f125 = ALPHA_R, f115, f125
	}
	;;
	{ .mmf
	STFD	[C6 ] = f110, 5 * SIZE
	STFD	[C14] = f111, 5 * SIZE
	FMA	f126 = ALPHA_I, f113, f126
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f127 = ALPHA_I, f115, f127
	}
	;;
	{ .mmf
	STFD	[C7 ] = f116, SIZE
	STFD	[C15] = f117, SIZE
	FMA	f32 = ALPHA_R, f120, f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA_R, f122, f33
	}
	;;
	{ .mmf
	STFD	[C7 ] = f118, SIZE
	STFD	[C15] = f119, SIZE
	FMA	f34 = ALPHA_I, f120, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35 = ALPHA_I, f122, f35
	}
	;;
	{ .mmf
	STFD	[C7 ] = f124, SIZE
	STFD	[C15] = f125, SIZE
	FMA	f36 = ALPHA_R, f121, f36
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f37 = ALPHA_R, f123, f37
	}
	;;
	{ .mmf
	STFD	[C7 ] = f126, 5 * SIZE
	STFD	[C15] = f127, 5 * SIZE
	FMA	f38 = ALPHA_I, f121, f38
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f39 = ALPHA_I, f123, f39
	}
	;;
	{ .mmf
	STFD	[C8 ] = f32, SIZE
	STFD	[C16] = f33, SIZE
	mov	f64  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C8 ] = f34, SIZE
	STFD	[C16] = f35, SIZE
	mov	f80  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f88  = f0
	}
	;;
	{ .mmf
	STFD	[C8 ] = f36, SIZE
	STFD	[C16] = f37, SIZE
	mov	f96  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f104 = f0
	}
	;;
	{ .mmf
	STFD	[C8 ] = f38, 5 * SIZE
	STFD	[C16] = f39, 5 * SIZE
	mov	f112 = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f120 = f0
	}
	;;
	.align 32

.L030:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L040
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [B]
	mov	f65  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
	mov	f73  = f0
	adds	L =  1, K
	}
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
	(p5) LDFD	f6   = [C1], SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f12  = [C2], SIZE
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
	(p5) LDFD	f7   = [C1], SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f13  = [C2], SIZE
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f10  = [C1], SIZE
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f14  = [C2], SIZE
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
	nop	__LINE__
	}
	;;
	{ .mfi
	(p5) LDFD	f11  = [C1], -3 * SIZE
	(p3) FMA	f113  = f41, f62, f113	// A2 * B7
	adds	L = -1, L
	}
	{ .mfb
	(p5) LDFD	f15  = [C2], -3 * SIZE
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	br.cloop.sptk.few .L032
	}
	;;

.L038:
	{ .mmf
	LDFD	f16  = [C3], SIZE
	LDFD	f20  = [C4], SIZE
	FMA	f6  = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f12 = ALPHA_R, f72, f12
	}
	;;
	{ .mmf
	LDFD	f17  = [C3], SIZE
	LDFD	f21  = [C4], SIZE
	FMA	f7  = ALPHA_I, f64, f7
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13 = ALPHA_I, f72, f13
	}
	;;
	{ .mmf
	LDFD	f18  = [C3], SIZE
	LDFD	f22  = [C4], SIZE
	FMA	f10 = ALPHA_R, f65, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f14 = ALPHA_R, f73, f14
	}
	;;
	{ .mmf
	LDFD	f19  = [C3], - 3 * SIZE
	LDFD	f23  = [C4], - 3 * SIZE
	FMA	f11 = ALPHA_I, f65, f11
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15 = ALPHA_I, f73, f15
	}
	;;
	{ .mmf
	STFD	[C1] = f6,  SIZE
	STFD	[C2] = f12, SIZE
	FMA	f16 = ALPHA_R, f80, f16
	}
	{ .mmf
	LDFD	f24  = [C5], SIZE
	LDFD	f28  = [C6], SIZE
	FMA	f20 = ALPHA_R, f88, f20
	}
	;;
	{ .mmf
	STFD	[C1] = f7,  SIZE
	STFD	[C2] = f13, SIZE
	FMA	f17 = ALPHA_I, f80, f17
	}
	{ .mmf
	LDFD	f25  = [C5], SIZE
	LDFD	f29  = [C6], SIZE
	FMA	f21 = ALPHA_I, f88, f21
	}
	;;
	{ .mmf
	STFD	[C1] = f10, SIZE
	STFD	[C2] = f14, SIZE
	FMA	f18 = ALPHA_R, f81, f18
	}
	{ .mmf
	LDFD	f26  = [C5], SIZE
	LDFD	f30  = [C6], SIZE
	FMA	f22 = ALPHA_R, f89, f22
	}
	;;
	{ .mmf
	STFD	[C1] = f11, SIZE
	STFD	[C2] = f15, SIZE
	FMA	f19 = ALPHA_I, f81, f19
	}
	{ .mmf
	LDFD	f27  = [C5], - 3 * SIZE
	LDFD	f31  = [C6], - 3 * SIZE
	FMA	f23 = ALPHA_I, f89, f23
	}
	;;
	{ .mmf
	STFD	[C3] = f16, SIZE
	STFD	[C4] = f20, SIZE
	FMA	f24 = ALPHA_R, f96,  f24
	}
	{ .mmf
	LDFD	f32  = [C7], SIZE
	LDFD	f36  = [C8], SIZE
	FMA	f28 = ALPHA_R, f104, f28
	}
	;;
	{ .mmf
	STFD	[C3] = f17, SIZE
	STFD	[C4] = f21, SIZE
	FMA	f25 = ALPHA_I, f96,  f25
	}
	{ .mmf
	LDFD	f33  = [C7], SIZE
	LDFD	f37  = [C8], SIZE
	FMA	f29 = ALPHA_I, f104, f29
	}
	;;
	{ .mmf
	STFD	[C3] = f18, SIZE
	STFD	[C4] = f22, SIZE
	FMA	f26 = ALPHA_R, f97,  f26
	}
	{ .mmf
	LDFD	f34  = [C7], SIZE
	LDFD	f38  = [C8], SIZE
	FMA	f30 = ALPHA_R, f105, f30
	}
	;;
	{ .mmf
	STFD	[C3] = f19, SIZE
	STFD	[C4] = f23, SIZE
	FMA	f27 = ALPHA_I, f97,  f27
	}
	{ .mmf
	LDFD	f35  = [C7], - 3 * SIZE
	LDFD	f39  = [C8], - 3 * SIZE
	FMA	f31 = ALPHA_I, f105, f31
	}
	;;
	{ .mmf
	STFD	[C5] = f24, SIZE
	STFD	[C6] = f28, SIZE
	FMA	f32 = ALPHA_R, f112, f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f36 = ALPHA_R, f120, f36
	}
	;;
	{ .mmf
	STFD	[C5] = f25, SIZE
	STFD	[C6] = f29, SIZE
	FMA	f33 = ALPHA_I, f112, f33
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f37 = ALPHA_I, f120, f37
	}
	;;
	{ .mmf
	STFD	[C5] = f26, SIZE
	STFD	[C6] = f30, SIZE
	FMA	f34 = ALPHA_R, f113, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f38 = ALPHA_R, f121, f38
	}
	;;
	{ .mmf
	STFD	[C5] = f27, SIZE
	STFD	[C6] = f31, SIZE
	FMA	f35 = ALPHA_I, f113,  f35
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f39 = ALPHA_I, f121, f39
	}
	;;
	{ .mmf
	STFD	[C7] = f32, SIZE
	STFD	[C8] = f36, SIZE
	mov	f64  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C7] = f33, SIZE
	STFD	[C8] = f37, SIZE
	mov	f80  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f88  = f0
	}
	;;
	{ .mmf
	STFD	[C7] = f34, SIZE
	STFD	[C8] = f38, SIZE
	mov	f96  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f104 = f0
	}
	;;
	{ .mmf
	STFD	[C7] = f35, SIZE
	STFD	[C8] = f39, SIZE
	mov	f112 = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f120 = f0
	}
	;;
	.align 32

.L040:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L049
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	adds	L =  1, K
	}
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
	{ .mfi
	(p3) LDFPD	f58, f59 = [BOFFSET],  2 * SIZE
	FMA	f96   = f32, f52, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mmf
	(p5) LDFD	f6   = [C1], SIZE
	(p5) LDFD	f10  = [C2], SIZE
	FMA	f104  = f32, f53, f104	// A1 * B6
	}
	;;
	{ .mfi
	(p3) LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mmf
	(p5) LDFD	f7   = [C1], -SIZE
	(p5) LDFD	f11  = [C2], -SIZE
	FMA	f120  = f32, f55, f120	// A1 * B8
	}
	;;
	{ .mmf
	(p3) LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	(p4) LDFD	f32 = [AOFFSET],   1 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	}
	{ .mmf
	(p5) LDFD	f12  = [C3], SIZE
	(p5) LDFD	f14  = [C4], SIZE
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	}
	;;
	{ .mfi
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mmf
	(p5) LDFD	f13  = [C3], -SIZE
	(p5) LDFD	f15  = [C4], -SIZE
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mfi
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	(p3) FMA	f96   = f40, f60, f96	// A1 * B5
	nop	__LINE__
	}
	{ .mmf
	(p5) LDFD	f16  = [C5], SIZE
	(p5) LDFD	f18  = [C6], SIZE
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
	}
	;;
	{ .mfi
	(p4) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	adds	L = -1, L
	}
	{ .mmb
	(p5) LDFD	f17 = [C5], -SIZE
	(p5) LDFD	f19 = [C6], -SIZE
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	{ .mmb
	(p5) LDFD	f20 = [C7], SIZE
	(p5) LDFD	f22 = [C8], SIZE
	br.cloop.sptk.few .L042
	}
	;;
	{ .mmf
	LDFD	f21 = [C7], -SIZE
	LDFD	f23 = [C8], -SIZE
	FMA	f6  = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f10 = ALPHA_R, f72, f10
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7  = ALPHA_I, f64, f7
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11 = ALPHA_I, f72, f11
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f12 = ALPHA_R, f80, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f14 = ALPHA_R, f88, f14
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13 = ALPHA_I, f80, f13
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15 = ALPHA_I, f88, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6,  SIZE
	STFD	[C2 ] = f10, SIZE
	FMA	f16 = ALPHA_R, f96,  f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f18 = ALPHA_R, f104, f18
	}
	;;
	{ .mmf
	STFD	[C1 ] = f7,  SIZE
	STFD	[C2 ] = f11, SIZE
	FMA	f17 = ALPHA_I, f96,  f17
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f19 = ALPHA_I, f104, f19
	}
	;;
	{ .mmf
	STFD	[C3 ] = f12, SIZE
	STFD	[C4 ] = f14, SIZE
	FMA	f20 = ALPHA_R, f112, f20
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f22 = ALPHA_R, f120, f22
	}
	;;
	{ .mmf
	STFD	[C3 ] = f13, SIZE
	STFD	[C4 ] = f15, SIZE
	FMA	f21 = ALPHA_I, f112, f21
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f23 = ALPHA_I, f120, f23
	}
	;;
	{ .mmi
	STFD	[C5 ] = f16, SIZE
	STFD	[C6 ] = f18, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C5 ] = f17, SIZE
	STFD	[C6 ] = f19, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C7 ] = f20, SIZE
	STFD	[C8 ] = f22, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C7 ] = f21, SIZE
	STFD	[C8 ] = f23, SIZE
	nop	__LINE__
	}
	;;
	.align 32

.L049:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
	nop	__LINE__
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
	{ .mfi
	cmp.eq	p6, p7 = 0, I
	mov	f65  = f0
	nop	__LINE__
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
	nop	__LINE__
	}
	{ .mfb
	shladd	C = LDC, 2, C
	mov	f89  = f0
	(p6)	br.cond.dpnt .L060
	}
	;;
	.align 32

.L052:
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
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f82  = f0
	nop	__LINE__
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
	adds	L =  1, K
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
	(p5) LDFD	f6  = [C1 ], SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f7  = [C9 ], SIZE
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f10 = [C1 ], SIZE
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f11 = [C9 ], SIZE
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f12 = [C1 ], SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f13 = [C9 ], SIZE
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f14 = [C1 ], 5 * SIZE
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f15 = [C9 ], 5 * SIZE
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f16 = [C1 ], SIZE
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f17 = [C9], SIZE
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f18 = [C1 ], SIZE
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f19 = [C9], SIZE
	(p3) FMA	f92   = f44, f59, f92	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f20 = [C1 ], SIZE
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f21 = [C9], SIZE
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f22 = [C1 ], -11 * SIZE
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f23 = [C9 ], -11 * SIZE
	(p3) FMA	f93   = f45, f59, f93	// A6 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f24 = [C2 ], SIZE
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f25 = [C10], SIZE
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f26 = [C2 ], SIZE
	(p3) FMA	f86   = f46, f58, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f27 = [C10], SIZE
	(p3) FMA	f94   = f46, f59, f94	// A7 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f28 = [C2 ], SIZE
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f29 = [C10], SIZE
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p5) LDFD	f30 = [C2 ], 5 * SIZE
	(p3) FMA	f87   = f47, f58, f87	// A8 * B3
	adds	L = -1, L
	}
	{ .mfb
	(p5) LDFD	f31 = [C10], 5 * SIZE
	(p3) FMA	f95   = f47, f59, f95	// A8 * B4
	br.cloop.sptk.few .L053
	}
	;;
	.align 32

.L058:
	{ .mmf
	LDFD	f32 = [C2 ], SIZE
	LDFD	f33 = [C10], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	LDFD	f34 = [C2 ], SIZE
	LDFD	f35 = [C10], SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	LDFD	f36 = [C2 ], SIZE
	LDFD	f37 = [C10], SIZE
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	LDFD	f38 = [C2 ], - 11 * SIZE
	LDFD	f39 = [C10], - 11 * SIZE
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16  = ALPHA_R, f68, f16
	}
	{ .mmf
	LDFD	f48 = [C3 ], SIZE
	LDFD	f49 = [C11], SIZE
	FMA	f17  = ALPHA_R, f70, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18  = ALPHA_I, f68, f18
	}
	{ .mmf
	LDFD	f50 = [C3 ], SIZE
	LDFD	f51 = [C11], SIZE
	FMA	f19  = ALPHA_I, f70, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20  = ALPHA_R, f69, f20
	}
	{ .mmf
	LDFD	f52 = [C3 ], SIZE
	LDFD	f53 = [C11], SIZE
	FMA	f21  = ALPHA_R, f71, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22  = ALPHA_I, f69, f22
	}
	{ .mmf
	LDFD	f54 = [C3 ], 5 * SIZE
	LDFD	f55 = [C11], 5 * SIZE
	FMA	f23  = ALPHA_I, f71, f23
	}
	;;
	{ .mmf
	STFD	[C1 ] = f16, SIZE
	STFD	[C9 ] = f17, SIZE
	FMA	f24  = ALPHA_R, f72, f24
	}
	{ .mmf
	LDFD	f40 = [C3 ], SIZE
	LDFD	f41 = [C11], SIZE
	FMA	f25  = ALPHA_R, f74, f25
	}
	;;
	{ .mmf
	STFD	[C1 ] = f18, SIZE
	STFD	[C9 ] = f19, SIZE
	FMA	f26  = ALPHA_I, f72, f26
	}
	{ .mmf
	LDFD	f42 = [C3 ], SIZE
	LDFD	f43 = [C11], SIZE
	FMA	f27  = ALPHA_I, f74, f27
	}
	;;
	{ .mmf
	STFD	[C1 ] = f20, SIZE
	STFD	[C9 ] = f21, SIZE
	FMA	f28  = ALPHA_R, f73, f28
	}
	{ .mmf
	LDFD	f44 = [C3 ], SIZE
	LDFD	f45 = [C11], SIZE
	FMA	f29  = ALPHA_R, f75, f29
	}
	;;
	{ .mmf
	STFD	[C1 ] = f22, 5 * SIZE
	STFD	[C9 ] = f23, 5 * SIZE
	FMA	f30  = ALPHA_I, f73, f30
	}
	{ .mmf
	LDFD	f46 = [C3 ], - 11 * SIZE
	LDFD	f56 = [C11], - 11 * SIZE
	FMA	f31  = ALPHA_I, f75, f31
	}
	;;
	{ .mmf
	STFD	[C2 ] = f24, SIZE
	STFD	[C10] = f25, SIZE
	FMA	f32  = ALPHA_R, f76, f32
	}
	{ .mmf
	LDFD	f57 = [C4 ], SIZE
	LDFD	f58 = [C12], SIZE
	FMA	f33  = ALPHA_R, f78, f33
	}
	;;
	{ .mmf
	STFD	[C2 ] = f26, SIZE
	STFD	[C10] = f27, SIZE
	FMA	f34  = ALPHA_I, f76, f34
	}
	{ .mmf
	LDFD	f59 = [C4 ], SIZE
	LDFD	f60 = [C12], SIZE
	FMA	f35  = ALPHA_I, f78, f35
	}
	;;
	{ .mmf
	STFD	[C2 ] = f28, SIZE
	STFD	[C10] = f29, SIZE
	FMA	f36  = ALPHA_R, f77, f36
	}
	{ .mmf
	LDFD	f61 = [C4 ], SIZE
	LDFD	f62 = [C12], SIZE
	FMA	f37  = ALPHA_R, f79, f37
	}
	;;
	{ .mmf
	STFD	[C2 ] = f30, 5 * SIZE
	STFD	[C10] = f31, 5 * SIZE
	FMA	f38  = ALPHA_I, f77, f38
	}
	{ .mmf
	LDFD	f63 = [C4 ], 5 * SIZE
	LDFD	f47 = [C12], 5 * SIZE
	FMA	f39  = ALPHA_I, f79, f39
	}
	;;
	{ .mmf
	STFD	[C2 ] = f32, SIZE
	STFD	[C10] = f33, SIZE
	FMA	f48  = ALPHA_R, f80, f48
	}
	{ .mmf
	LDFD	f64 = [C4 ], SIZE
	LDFD	f65 = [C12], SIZE
	FMA	f49  = ALPHA_R, f82, f49
	}
	;;
	{ .mmf
	STFD	[C2 ] = f34, SIZE
	STFD	[C10] = f35, SIZE
	FMA	f50  = ALPHA_I, f80, f50
	}
	{ .mmf
	LDFD	f6 = [C4 ], SIZE
	LDFD	f7 = [C12], SIZE
	FMA	f51  = ALPHA_I, f82, f51
	}
	;;
	{ .mmf
	STFD	[C2 ] = f36, SIZE
	STFD	[C10] = f37, SIZE
	FMA	f52  = ALPHA_R, f81, f52
	}
	{ .mmf
	LDFD	f10 = [C4 ], SIZE
	LDFD	f11 = [C12], SIZE
	FMA	f53  = ALPHA_R, f83, f53
	}
	;;
	{ .mmf
	STFD	[C2 ] = f38, 5 * SIZE
	STFD	[C10] = f39, 5 * SIZE
	FMA	f54  = ALPHA_I, f81, f54
	}
	{ .mmf
	LDFD	f12 = [C4 ], - 11 * SIZE
	LDFD	f13 = [C12], - 11 * SIZE
	FMA	f55  = ALPHA_I, f83, f55
	}
	;;
	{ .mmf
	STFD	[C3 ] = f48, SIZE
	STFD	[C11] = f49, SIZE
	FMA	f40  = ALPHA_R, f84, f40
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f41  = ALPHA_R, f86, f41
	}
	;;
	{ .mmf
	STFD	[C3 ] = f50, SIZE
	STFD	[C11] = f51, SIZE
	FMA	f42  = ALPHA_I, f84, f42
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f43  = ALPHA_I, f86, f43
	}
	;;
	{ .mmf
	STFD	[C3 ] = f52, SIZE
	STFD	[C11] = f53, SIZE
	FMA	f44  = ALPHA_R, f85, f44
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f45  = ALPHA_R, f87, f45
	}
	;;
	{ .mmf
	STFD	[C3 ] = f54, 5 * SIZE
	STFD	[C11] = f55, 5 * SIZE
	FMA	f46  = ALPHA_I, f85, f46
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f56  = ALPHA_I, f87, f56
	}
	;;
	{ .mmf
	STFD	[C3 ] = f40, SIZE
	STFD	[C11] = f41, SIZE
	FMA	f57  = ALPHA_R, f88, f57
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f58  = ALPHA_R, f90, f58
	}
	;;
	{ .mmf
	STFD	[C3 ] = f42, SIZE
	STFD	[C11] = f43, SIZE
	FMA	f59  = ALPHA_I, f88, f59
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f60  = ALPHA_I, f90, f60
	}
	;;
	{ .mmf
	STFD	[C3 ] = f44, SIZE
	STFD	[C11] = f45, SIZE
	FMA	f61  = ALPHA_R, f89, f61
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f62  = ALPHA_R, f91, f62
	}
	;;
	{ .mmf
	STFD	[C3 ] = f46, 5 * SIZE
	STFD	[C11] = f56, 5 * SIZE
	FMA	f63  = ALPHA_I, f89, f63
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f47  = ALPHA_I, f91, f47
	}
	;;
	{ .mmf
	STFD	[C4 ] = f57, SIZE
	STFD	[C12] = f58, SIZE
	FMA	f64  = ALPHA_R, f92, f64
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f65  = ALPHA_R, f94, f65
	}
	;;
	{ .mmf
	STFD	[C4 ] = f59, SIZE
	STFD	[C12] = f60, SIZE
	FMA	f6   = ALPHA_I, f92, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_I, f94, f7
	}
	;;
	{ .mmf
	STFD	[C4 ] = f61, SIZE
	STFD	[C12] = f62, SIZE
	FMA	f10  = ALPHA_R, f93, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_R, f95, f11
	}
	;;
	{ .mmf
	STFD	[C4 ] = f63, 5 * SIZE
	STFD	[C12] = f47, 5 * SIZE
	FMA	f12  = ALPHA_I, f93, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_I, f95, f13
	}
	;;
	{ .mmf
	STFD	[C4 ] = f64, SIZE
	STFD	[C12] = f65, SIZE
	mov	f64  = f0
	}
	{ .mmf
	cmp.ne	p6, p0 = 1, I
	nop	__LINE__
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C4 ] = f6, SIZE
	STFD	[C12] = f7, SIZE
 	mov	f80  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f88  = f0
	}
	;;
	{ .mmf
	STFD	[C4 ] = f10, SIZE
	STFD	[C12] = f11, SIZE
	mov	f65 = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f73 = f0
	}
	;;
	{ .mmf
	STFD	[C4 ] = f12, 5 * SIZE
	STFD	[C12] = f13, 5 * SIZE
	mov	f81 = f0
	}
	{ .mfb
	adds	I = -1, I
	mov	f89 = f0
	(p6)	br.cond.dptk .L052
	}
	;;
	.align 32

.L060:
	{ .mfi
	nop	__LINE__
	mov	f66  = f0
	tbit.z	p6, p7  = M, 2
	}
	{ .mfb
	nop	__LINE__
	mov	f74  = f0
	(p6)	br.cond.dptk .L070
	}
	;;
	{ .mfb
	LDFPD	f48, f49 = [B]
	mov	f82  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 2 * SIZE, B
	mov	f90  = f0
	adds	L =  1, K
	}
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
	(p5) adds	C9  = 4 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f88   = f32, f51, f88	// A1 * B4
	(p5) adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mfi
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
	FMA	f65   = f33, f48, f65	// A2 * B1
	(p5) adds	C11 = 4 * SIZE, C3
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	(p5) adds	C12 = 4 * SIZE, C4
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
	(p5) LDFD	f6  = [C1 ], SIZE
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}

	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	FMA	f83   = f35, f50, f83	// A4 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f7  = [C9 ], SIZE
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
	(p5) LDFD	f10 = [C1 ], SIZE
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
	(p5) LDFD	f11 = [C9 ], SIZE
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f12 = [C1 ], SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f13 = [C9], SIZE
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f14 = [C1 ], - 3 * SIZE
	(p3) FMA	f81   = f41, f58, f81	// A2 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f15 = [C9], - 3 * SIZE
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f16  = [C2 ], SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f17  = [C10], SIZE
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f18 = [C2 ], SIZE
	(p3) FMA	f82   = f42, f58, f82	// A3 * B3
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f19 = [C10], SIZE
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f20 = [C2 ], SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f21 = [C10], SIZE
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p5) LDFD	f22 = [C2 ], -3 * SIZE
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	adds	L = -1, L
	}
	{ .mfb
	(p5) LDFD	f23 = [C10], -3 * SIZE
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	br.cloop.sptk.few .L062
	}
	;;
	{ .mmf
	LDFD	f24 = [C3 ], SIZE
	LDFD	f25 = [C11], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	LDFD	f26 = [C3 ], SIZE
	LDFD	f27 = [C11], SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	LDFD	f28 = [C3 ], SIZE
	LDFD	f29 = [C11], SIZE
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	LDFD	f30 = [C3 ], - 3 * SIZE
	LDFD	f31 = [C11], - 3 * SIZE
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16  = ALPHA_R, f72, f16
	}
	{ .mmf
	LDFD	f32 = [C4 ], SIZE
	LDFD	f33 = [C12], SIZE
	FMA	f17  = ALPHA_R, f74, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18  = ALPHA_I, f72, f18
	}
	{ .mmf
	LDFD	f34 = [C4 ], SIZE
	LDFD	f35 = [C12], SIZE
	FMA	f19  = ALPHA_I, f74, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20  = ALPHA_R, f73, f20
	}
	{ .mmf
	LDFD	f36 = [C4 ], SIZE
	LDFD	f37 = [C12], SIZE
	FMA	f21  = ALPHA_R, f75, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22  = ALPHA_I, f73, f22
	}
	{ .mmf
	LDFD	f38 = [C4 ], - 3 * SIZE
	LDFD	f39 = [C12], - 3 * SIZE
	FMA	f23  = ALPHA_I, f75, f23
	}
	;;
	{ .mmf
	STFD	[C2 ] = f16, SIZE
	STFD	[C10] = f17, SIZE
	FMA	f24  = ALPHA_R, f80, f24
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f25  = ALPHA_R, f82, f25
	}
	;;
	{ .mmf
	STFD	[C2 ] = f18, SIZE
	STFD	[C10] = f19, SIZE
	FMA	f26  = ALPHA_I, f80, f26
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f27  = ALPHA_I, f82, f27
	}
	;;
	{ .mmf
	STFD	[C2 ] = f20, SIZE
	STFD	[C10] = f21, SIZE
	FMA	f28  = ALPHA_R, f81, f28
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f29  = ALPHA_R, f83, f29
	}
	;;
	{ .mmf
	STFD	[C2 ] = f22, 5 * SIZE
	STFD	[C10] = f23, 5 * SIZE
	FMA	f30  = ALPHA_I, f81, f30
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f31  = ALPHA_I, f83, f31
	}
	;;
	{ .mmf
	STFD	[C3 ] = f24, SIZE
	STFD	[C11] = f25, SIZE
	FMA	f32  = ALPHA_R, f88, f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33  = ALPHA_R, f90, f33
	}
	;;
	{ .mmf
	STFD	[C3 ] = f26, SIZE
	STFD	[C11] = f27, SIZE
	FMA	f34  = ALPHA_I, f88, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35  = ALPHA_I, f90, f35
	}
	;;
	{ .mmf
	STFD	[C3 ] = f28, SIZE
	STFD	[C11] = f29, SIZE
	FMA	f36  = ALPHA_R, f89, f36
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f37  = ALPHA_R, f91, f37
	}
	;;
	{ .mmf
	STFD	[C3 ] = f30, 5 * SIZE
	STFD	[C11] = f31, 5 * SIZE
	FMA	f38  = ALPHA_I, f89, f38
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f39  = ALPHA_I, f91, f39
	}
	;;
	{ .mmf
	STFD	[C4 ] = f32, SIZE
	STFD	[C12] = f33, SIZE
	mov	f64  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C4 ] = f34, SIZE
	STFD	[C12] = f35, SIZE
	mov	f80  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f88  = f0
	}
	;;
	{ .mmf
	STFD	[C4 ] = f36, SIZE
	STFD	[C12] = f37, SIZE
	mov	f81  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C4 ] = f38, 5 * SIZE
	STFD	[C12] = f39, 5 * SIZE
	mov	f89  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f73  = f0
	}
	;;
	.align 32

.L070:
	{ .mib
	nop	__LINE__
	tbit.z	p6,p7  = M, 1
	(p6)	br.cond.dptk .L080
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	adds	L =  1, K
	}
	;;
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
	(p5) LDFD	f6  = [C1 ], SIZE
	(p5) LDFD	f12 = [C2 ], SIZE
	FMA	f89   = f33, f51, f89	// A2 * B4
	}
	;;
	{ .mfb
	(p3) LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mmf
	(p5) LDFD	f7  = [C1 ], SIZE
	(p5) LDFD	f13 = [C2 ], SIZE
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	nop	__LINE__
	}
	{ .mmf
	(p5) LDFD	f10 = [C1 ], SIZE
	(p5) LDFD	f14 = [C2 ], SIZE
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f11 = [C1 ], - 3 * SIZE
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
	(p5) LDFD	f15 = [C2 ], - 3 * SIZE
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	br.cloop.sptk.few .L072
	}
	;;
	{ .mmf
	LDFD	f16 = [C3], SIZE
	LDFD	f20 = [C4], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f12  = ALPHA_R, f72, f12
	}
	;;
	{ .mmf
	LDFD	f17 = [C3], SIZE
	LDFD	f21 = [C4], SIZE
	FMA	f7   = ALPHA_I, f64, f7
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_I, f72, f13
	}
	;;
	{ .mmf
	LDFD	f18 = [C3], SIZE
	LDFD	f22 = [C4], SIZE
	FMA	f10  = ALPHA_R, f65, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f14  = ALPHA_R, f73, f14
	}
	;;
	{ .mmf
	LDFD	f19 = [C3], - 3 * SIZE
	LDFD	f23 = [C4], - 3 * SIZE
	FMA	f11  = ALPHA_I, f65, f11
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f73, f15
	}
	;;
	{ .mmf
	STFD	[C1] = f6,  SIZE
	STFD	[C2] = f12, SIZE
	FMA	f16  = ALPHA_R, f80, f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f20  = ALPHA_R, f88, f20
	}
	;;
	{ .mmf
	STFD	[C1] = f7,  SIZE
	STFD	[C2] = f13, SIZE
	FMA	f17  = ALPHA_I, f80, f17
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f21  = ALPHA_I, f88, f21
	}
	;;
	{ .mmf
	STFD	[C1] = f10, SIZE
	STFD	[C2] = f14, SIZE
	FMA	f18  = ALPHA_R, f81, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f22  = ALPHA_R, f89, f22
	}
	;;
	{ .mmf
	STFD	[C1] = f11, SIZE
	STFD	[C2] = f15, SIZE
	FMA	f19  = ALPHA_I, f81, f19
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f23  = ALPHA_I, f89, f23
	}
	;;
	{ .mmf
	STFD	[C3] = f16, SIZE
	STFD	[C4] = f20, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C3] = f17, SIZE
	STFD	[C4] = f21, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C3] = f18, SIZE
	STFD	[C4] = f22, SIZE
	mov	f80  = f0
	}
	;;
	{ .mmf
	STFD	[C3] = f19, SIZE
	STFD	[C4] = f23, SIZE
	mov	f88  = f0
	}
	;;
	.align 32

.L080:
	{ .mib
	nop	__LINE__
	tbit.z	p6,p7  = M, 0
	(p6)	br.cond.dptk .L089
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	adds	L =  1, K
	}
	;;
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
	{ .mmf
	(p5) LDFD	f6   = [C1], SIZE
	(p5) LDFD	f10  = [C2], SIZE
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
	}
	;;
	{ .mmf
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p4) LDFD	f32 = [AOFFSET],   1 * SIZE
	(p3) FMA	f80   = f40, f58, f80	// A1 * B3
	}
	{ .mmf
	(p5) LDFD	f7  = [C1], -SIZE
	(p5) LDFD	f11 = [C2], -SIZE
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mib
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	adds	L = -1, L
	br.cloop.sptk.few .L082
	}
	;;
	{ .mmf
	LDFD	f12 = [C3], SIZE
	LDFD	f14 = [C4], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f10  = ALPHA_R, f72, f10
	}
	;;
	{ .mmf
	LDFD	f13 = [C3], -SIZE
	LDFD	f15 = [C4], -SIZE
	FMA	f7   = ALPHA_I, f64, f7
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f72, f11
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f12  = ALPHA_R, f80, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f14  = ALPHA_R, f88, f14
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_I, f80, f13
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f88, f15
	}
	;;
	{ .mmi
	STFD	[C1] = f6,  SIZE
	STFD	[C2] = f10, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C1] = f7,  SIZE
	STFD	[C2] = f11, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C3] = f12, SIZE
	STFD	[C4] = f14, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C3] = f13, SIZE
	STFD	[C4] = f15, SIZE
	nop	__LINE__
	}
	;;
	.align 32

.L089:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
	nop	__LINE__
	}
	;;
	.align 16

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
	nop	__LINE__
	}
	{ .mfb
	mov	AOFFSET = A
	mov	f73  = f0
	(p6)	br.cond.dpnt .L130
	}
	;;
	{ .mfi
	nop	__LINE__
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
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f75  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	mov	f76  = f0
	adds	L =  1, K
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
	(p5) LDFD	f6  = [C1 ], SIZE
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
	(p5) LDFD	f7 = [C9 ], SIZE
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
	(p5) LDFD	f10 = [C1 ], SIZE
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
	(p5) LDFD	f11 = [C9 ], SIZE
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
	(p5) LDFD	f12 = [C1 ], SIZE
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
	(p5) LDFD	f13 = [C9 ], SIZE
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
	(p5) LDFD	f14 = [C1 ], 5 * SIZE
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
	(p5) LDFD	f15 = [C9 ], 5 * SIZE
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
	(p5) LDFD	f16 = [C1 ], SIZE
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f17 = [C9 ], SIZE
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f18 = [C1 ], SIZE
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f19 = [C9 ], SIZE
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	(p5) LDFD	f20 = [C1 ], SIZE
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f21 = [C9 ], SIZE
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p5) LDFD	f22 = [C1 ], -11 * SIZE
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	adds	L = -1, L
	}
	{ .mfb
	(p5) LDFD	f23 = [C9 ], -11 * SIZE
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	br.cloop.sptk.few .L093
	}
	;;
	{ .mmf
	LDFD	f24 = [C2 ], SIZE
	LDFD	f25 = [C10], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	LDFD	f26 = [C2 ], SIZE
	LDFD	f27 = [C10], SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	LDFD	f28 = [C2 ], SIZE
	LDFD	f29 = [C10], SIZE
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	LDFD	f30 = [C2 ], 5 * SIZE
	LDFD	f31 = [C10], 5 * SIZE
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16  = ALPHA_R, f68, f16
	}
	{ .mmf
	LDFD	f32 = [C2 ], SIZE
	LDFD	f33 = [C10], SIZE
	FMA	f17  = ALPHA_R, f70, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18  = ALPHA_I, f68, f18
	}
	{ .mmf
	LDFD	f34 = [C2 ], SIZE
	LDFD	f35 = [C10], SIZE
	FMA	f19  = ALPHA_I, f70, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20  = ALPHA_R, f69, f20
	}
	{ .mmf
	LDFD	f36 = [C2 ], SIZE
	LDFD	f37 = [C10], SIZE
	FMA	f21  = ALPHA_R, f71, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22  = ALPHA_I, f69, f22
	}
	{ .mmf
	LDFD	f38 = [C2 ], - 11 * SIZE
	LDFD	f39 = [C10], - 11 * SIZE
	FMA	f23  = ALPHA_I, f71, f23
	}
	;;
	{ .mmf
	STFD	[C1 ] = f16, SIZE
	STFD	[C9 ] = f17, SIZE
	FMA	f24  = ALPHA_R, f72, f24
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f25  = ALPHA_R, f74, f25
	}
	;;
	{ .mmf
	STFD	[C1 ] = f18, SIZE
	STFD	[C9 ] = f19, SIZE
	FMA	f26  = ALPHA_I, f72, f26
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f27  = ALPHA_I, f74, f27
	}
	;;
	{ .mmf
	STFD	[C1 ] = f20, SIZE
	STFD	[C9 ] = f21, SIZE
	FMA	f28  = ALPHA_R, f73, f28
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f29  = ALPHA_R, f75, f29
	}
	;;
	{ .mmf
	STFD	[C1 ] = f22, 5 * SIZE
	STFD	[C9 ] = f23, 5 * SIZE
	FMA	f30  = ALPHA_I, f73, f30
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f31  = ALPHA_I, f75, f31
	}
	;;
	{ .mmf
	STFD	[C2 ] = f24, SIZE
	STFD	[C10] = f25, SIZE
	FMA	f32  = ALPHA_R, f76, f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33  = ALPHA_R, f78, f33
	}
	;;
	{ .mmf
	STFD	[C2 ] = f26, SIZE
	STFD	[C10] = f27, SIZE
	FMA	f34  = ALPHA_I, f76, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35  = ALPHA_I, f78, f35
	}
	;;
	{ .mmf
	STFD	[C2 ] = f28, SIZE
	STFD	[C10] = f29, SIZE
	FMA	f36  = ALPHA_R, f77, f36
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f37  = ALPHA_R, f79, f37
	}
	;;
	{ .mmf
	STFD	[C2 ] = f30, 5 * SIZE
	STFD	[C10] = f31, 5 * SIZE
	FMA	f38  = ALPHA_I, f77, f38
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f39  = ALPHA_I, f79, f39
	}
	;;
	{ .mmf
	STFD	[C2 ] = f32, SIZE
	STFD	[C10] = f33, SIZE
	mov	f64  = f0
	}
	{ .mmf
	cmp.ne	p6, p0 = 1, I
	nop	__LINE__
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f34, SIZE
	STFD	[C10] = f35, SIZE
	mov	f65  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f73  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f36, SIZE
	STFD	[C10] = f37, SIZE
	mov	f66  = f0
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	mov	f74  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f38, 5 * SIZE
	STFD	[C10] = f39, 5 * SIZE
	mov	f67  = f0
	}
	{ .mfb
	adds	I = -1, I
	mov	f75  = f0
	(p6)	br.cond.dptk .L092
	}
	;;
	.align 32

.L100:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 2
	(p6)	br.cond.dptk .L110
	}
	;;
	{ .mmf
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	mov	f75  = f0
	}
	{ .mii
	nop	__LINE__
	adds	L =  1, K
	}
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
	adds	C9  = 4 * SIZE, C1
	}
	{ .mfi
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mfb
	(p3) LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FMA	f66   = f34, f48, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f6 = [C1 ], SIZE
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
	(p5) LDFD	f7 = [C9 ], SIZE
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
	(p5) LDFD	f10 = [C1 ], SIZE
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
	(p5) LDFD	f11 = [C9 ], SIZE
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
	(p5) LDFD	f12 = [C1], SIZE
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
	(p5) LDFD	f13 = [C9], SIZE
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	br.cloop.sptk.few .L102
	}
	;;
	{ .mmf
	LDFD	f14 = [C1], - 3 * SIZE
	LDFD	f15 = [C9], - 3 * SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	LDFD	f16 = [C2 ], SIZE
	LDFD	f17 = [C10], SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	LDFD	f18 = [C2 ], SIZE
	LDFD	f19 = [C10], SIZE
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	LDFD	f20 = [C2 ], SIZE
	LDFD	f21 = [C10], SIZE
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16  = ALPHA_R, f72, f16
	}
	{ .mmf
	LDFD	f22 = [C2 ], - 3 * SIZE
	LDFD	f23 = [C10], - 3 * SIZE
	FMA	f17  = ALPHA_R, f74, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18  = ALPHA_I, f72, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f19  = ALPHA_I, f74, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20  = ALPHA_R, f73, f20
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f21  = ALPHA_R, f75, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22  = ALPHA_I, f73, f22
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f23  = ALPHA_I, f75, f23
	}
	;;
	{ .mmf
	STFD	[C2 ] = f16, SIZE
	STFD	[C10] = f17, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f18, SIZE
	STFD	[C10] = f19, SIZE
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f20, SIZE
	STFD	[C10] = f21, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C2 ] = f22, 5 * SIZE
	STFD	[C10] = f23, 5 * SIZE
	mov	f73  = f0
	}
	;;
	.align 32

.L110:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L120
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	adds	L =  1, K
	}
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
	(p5) LDFD	f6 = [C1 ], SIZE
	(p5) LDFD	f7 = [C2 ], SIZE
	FMA	f73   = f33, f49, f73	// A2 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f10 = [C1 ], SIZE
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
	(p5) LDFD	f11 = [C2 ],  SIZE
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	br.cloop.sptk.few .L112
	}
	;;
	{ .mmf
	LDFD	f12 = [C1], SIZE
	LDFD	f13 = [C2], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f72, f7
	}
	;;
	{ .mmf
	LDFD	f14 = [C1], - 3 * SIZE
	LDFD	f15 = [C2], - 3 * SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f72, f11
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f73, f13
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f73, f15
	}
	;;
	{ .mmf
	STFD	[C1] = f6, SIZE
	STFD	[C2] = f7, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C1] = f10, SIZE
	STFD	[C2] = f11, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmf
	STFD	[C1] = f12, SIZE
	STFD	[C2] = f13, SIZE
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C1] = f14, SIZE
	STFD	[C2] = f15, SIZE
	mov	f73  = f0
	}
	;;
	.align 32

.L120:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L129
	}
	;;
	{ .mmi
	LDFPD	f48, f49 = [B]
	adds	BOFFSET = 2 * SIZE, B
	adds	L =  1, K
	}
	;;
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
	(p5) LDFD	f6 = [C1], SIZE
	(p5) LDFD	f7 = [C2], SIZE
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
	{ .mmf
	(p5) LDFD	f10  = [C1], -SIZE
	(p5) LDFD	f11  = [C2], -SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f72, f7
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f72, f11
	}
	;;
	{ .mmi
	STFD	[C1 ] = f6, SIZE
	STFD	[C2 ] = f7, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C1 ] = f10, SIZE
	STFD	[C2 ] = f11, SIZE
	nop	__LINE__
	}
	;;
	.align 32

.L129:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
	nop	__LINE__
	}
	;;
	.align 16

.L130:
	{ .mfi
	nop	__LINE__
	mov	f64  = f0
	tbit.z	p6, p0 = N, 0
	}
	{ .mib
	mov	AOFFSET = A
	shr	I  = M, 3
	(p6)	br.cond.dpnt .L999
	}
	;;
	{ .mfi
	mov	C1 = C
	mov	f65  = f0
	nop	__LINE__
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
	{ .mfb
	LDFD	f48 = [B]
	mov	f68  = f0
	nop	__LINE__
	}
	{ .mfi
	adds	BOFFSET = 1 * SIZE, B
	mov	f69  = f0
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f70  = f0
	adds	L =  1, K
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
	(p5) LDFD	f6  = [C1 ], SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f7  = [C9 ], SIZE
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
	(p5) LDFD	f10 = [C1 ], SIZE
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
	(p5) LDFD	f11 = [C9 ], SIZE
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
	(p5) LDFD	f12 = [C1 ], SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	(p5) LDFD	f13 = [C9 ], SIZE
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
	(p5) LDFD	f14 = [C1 ], 5 * SIZE
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
	(p5) LDFD	f15 = [C9 ], 5 * SIZE
	nop	__LINE__
	br.cloop.sptk.few .L133
	}
	;;

.L138:
	{ .mmf
	LDFD	f16 = [C1 ], SIZE
	LDFD	f17 = [C9 ], SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	LDFD	f18 = [C1 ], SIZE
	LDFD	f19 = [C9 ], SIZE
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	LDFD	f20 = [C1 ], SIZE
	LDFD	f21 = [C9 ], SIZE
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	LDFD	f22 = [C1 ], - 11 * SIZE
	LDFD	f23 = [C9 ], - 11 * SIZE
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	FMA	f16  = ALPHA_R, f68, f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f17  = ALPHA_R, f70, f17
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	FMA	f18  = ALPHA_I, f68, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f19  = ALPHA_I, f70, f19
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	FMA	f20  = ALPHA_R, f69, f20
	}
	{ .mmf
	cmp.ne	p6, p0 = 1, I
	adds	I = -1, I
	FMA	f21  = ALPHA_R, f71, f21
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	FMA	f22  = ALPHA_I, f69, f22
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f23  = ALPHA_I, f71, f23
	}
	;;
	{ .mmf
	STFD	[C1 ] = f16, SIZE
	STFD	[C9 ] = f17, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f18, SIZE
	STFD	[C9 ] = f19, SIZE
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f20, SIZE
	STFD	[C9 ] = f21, SIZE
	mov	f66  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f22, 5 * SIZE
	STFD	[C9 ] = f23, 5 * SIZE
	mov	f67  = f0
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	(p6)	br.cond.dptk .L132
	}
	;;
	.align 32

.L140:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 2
	(p6)	br.cond.dptk .L150
	}
	;;
	{ .mmi
	LDFD	f48 = [B]
	adds	BOFFSET = 1 * SIZE, B
	adds	L =  1, K
	}
	;;
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
	(p5) adds	C9  = 4 * SIZE, C1
	}
	{ .mmf
	(p3) LDFD	f56 = [BOFFSET],   1 * SIZE
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	;;
	{ .mfi
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	(p5) adds	C10 = 2 * SIZE, C2
	}
	{ .mmf
	(p5) LDFD	f6  = [C1 ], SIZE
	(p5) LDFD	f7  = [C9 ], SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	}
	;;
	{ .mmf
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p4) LDFD	f48 = [BOFFSET],   1 * SIZE
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	}
	{ .mmf
	(p5) LDFD	f10  = [C1 ], SIZE
	(p5) LDFD	f11  = [C9 ], SIZE
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	}
	;;
	{ .mfi
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L = -1, L
	}
	{ .mmb
	(p5) LDFD	f12  = [C1 ], SIZE
	(p5) LDFD	f13  = [C9 ], SIZE
	br.cloop.sptk.few .L142
	}
	;;

.L148:
	{ .mmf
	LDFD	f14  = [C1 ], - 3 * SIZE
	LDFD	f15  = [C9 ], - 3 * SIZE
	FMA	f6   = ALPHA_R, f64, f6
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f7   = ALPHA_R, f66, f7
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f10  = ALPHA_I, f64, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f11  = ALPHA_I, f66, f11
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f12  = ALPHA_R, f65, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f13  = ALPHA_R, f67, f13
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f14  = ALPHA_I, f65, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f15  = ALPHA_I, f67, f15
	}
	;;
	{ .mmf
	STFD	[C1 ] = f6, SIZE
	STFD	[C9 ] = f7, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f10, SIZE
	STFD	[C9 ] = f11, SIZE
	mov	f65  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f12, SIZE
	STFD	[C9 ] = f13, SIZE
	mov	f66  = f0
	}
	;;
	{ .mmf
	STFD	[C1 ] = f14, 5 * SIZE
	STFD	[C9 ] = f15, 5 * SIZE
	mov	f67  = f0
	}
	;;
	.align 32

.L150:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L160
	}
	;;
	{ .mmi
	LDFD	f48 = [B]
	adds	BOFFSET = 1 * SIZE, B
	adds	L =  1, K
	}
	;;
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
	LDFD	f68 = [C1 ], 1 * SIZE
	;;
	LDFD	f69 = [C1 ], 1 * SIZE
	;;
	LDFD	f70 = [C1 ], 1 * SIZE
	;;
	LDFD	f71 = [C1 ], - 3 * SIZE
	;;
	FMA	f68  = ALPHA_R, f64, f68
	FMA	f69  = ALPHA_I, f64, f69
	FMA	f70  = ALPHA_R, f65, f70
	FMA	f71  = ALPHA_I, f65, f71
	;;
	STFD	[C1 ] = f68, SIZE
	;;
	STFD	[C1 ] = f69, SIZE
	;;
	STFD	[C1 ] = f70, SIZE
	mov	f64  = f0
	;;
	STFD	[C1 ] = f71, SIZE
	mov	f65  = f0
	;;
	.align 32

.L160:
	{ .mib
	nop	__LINE__
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L169
	}
	;;
	{ .mmi
	LDFD	f48 = [B]
	adds	BOFFSET = 1 * SIZE, B
	adds	L =  1, K
	}
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
	(p5) LDFD	f68 = [C1], 1 * SIZE
	adds	L = -1, L
	}
	;;
	{ .mmf
	(p4) LDFD	f48 = [BOFFSET],   1 * SIZE
	(p5) LDFD	f69 = [C1], - 1 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	}
	{ .mib
	nop	__LINE__
	nop	__LINE__
	br.cloop.sptk.few .L162
	}
	;;
	FMA	f68  = ALPHA_R, f64, f68
	FMA	f69  = ALPHA_I, f64, f69
	;;
	STFD	[C1 ] = f68, SIZE
	;;
	STFD	[C1 ] = f69, SIZE
	;;
	.align 32

.L169:
	{ .mmi
	mov	B = BOFFSET
	mov	AOFFSET = A
	nop	__LINE__
	}
	;;
	.align 16

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

