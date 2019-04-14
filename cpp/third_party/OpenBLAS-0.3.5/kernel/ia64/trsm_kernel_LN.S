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

#define CPREFETCHSIZE -7
#define CPREFETCH     lfetch.excl.nt1

#define M	r32
#define N	r33
#define K	r34
#define A	r36
#define B	r37
#define C	r38
#define LDC	r39

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
#define AOFFSET2 loc12
#define BOFFSET2 loc13


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
	adds	r8 = -6 * 16, SP
	adds	r9 = -5 * 16, SP
	adds	SP = -6 * 16, SP
	}
	;;
	{ .mmi
	setf.sig f32 = M
	setf.sig f33 = K
	mov	PR = pr
	}
	;;
	{ .mmi
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	shr	J = N, 3
	}
	;;
	{ .mmi
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	shladd	LDC = LDC, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	stf.spill  [r8] = f20
	stf.spill  [r9] = f21
	mov	AOFFSET = A
	}
	;;
	.body
	{ .mmf
	ld8	OFFSET   = [r14]
	cmp.ge	p6, p0  = 0, J
	xmpy.l	f32  = f32, f33
	}
	;;
	{ .mmi
	getf.sig r2 = f32
	shladd	C = M,  BASE_SHIFT, C
	nop	__LINE__
	}
	;;
	{ .mmb
	shladd	A = r2, BASE_SHIFT, A
	nop	__LINE__
	(p6)	br.cond.dpnt .L050
	}
	;;
	.align 8

.L000:
	{ .mmf
	mov	C1 = C
	add	KK = M, OFFSET
	}
	{ .mmi
	mov	AORIG = A
	add	C2 = LDC, C
	shladd	C3 = LDC, 1, C
	}
	;;
	{ .mmf
	shladd	C5 = LDC, 2, C
	shladd	C = LDC, 3, C
	}
	{ .mmf
	shladd	C4 = LDC, 1, C2
	shladd	C6 = LDC, 2, C2
	}
	;;
	{ .mfi
	shladd	C7 = LDC, 2, C3
	shladd	C8 = LDC, 2, C4
	}
	;;
	;;
	mov	f64  = f0
	mov	f72  = f0
	mov	f80  = f0
	mov	f88  = f0
	mov	f96  = f0
	mov	f104 = f0
	mov	f112 = f0
	mov	f120 = f0

.L040:
	{ .mib
	sub	L = K, KK
	tbit.z	p6, p0 = M, 0
	(p6)	br.cond.dptk .L030
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 0 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfi
	shladd	BOFFSET = r3, 3, B
	sub	AORIG = AORIG, r2
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	add	AOFFSET = r3, AORIG
	}
	;;
	{ .mmi
	adds	L =  1, L
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mii
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	(p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	adds	L =  -1, L
	}
	;;
	{ .mmi
	(p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mib
	(p7) LDFD	f32 = [AOFFSET], 1 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L048
	}
	;;

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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
	nop	__LINE__
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	br.cloop.sptk.few .L042
	}
	;;

.L048:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -8, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	shladd	BOFFSET = r2, 3, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	{ .mfi
	FSUB	f64  = f32, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f72  = f33, f72
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f80  = f34, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f88  = f35, f88
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f96  = f36, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f104 = f37, f104
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f112 = f38, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f120 = f39, f120
	nop	__LINE__
	}
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f80  = f34, f80
	FSUB	f88  = f35, f88
	FSUB	f96  = f36, f96
	FSUB	f104 = f37, f104
	FSUB	f112 = f38, f112
	FSUB	f120 = f39, f120
	;;
#endif

#ifdef LN
	LDFD	f32 = [AOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f96  = f96,  f32
	FMPY	f72  = f72,  f32
	FMPY	f104 = f104, f32
	FMPY	f80  = f80,  f32
 	FMPY	f112 = f112, f32
	FMPY	f88  = f88,  f32
	FMPY	f120 = f120, f32
	;;
	{ .mmi
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f96, SIZE
	adds	C1 = -1 * SIZE, C1
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f104, SIZE
	adds	C2 = -1 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f112, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f88,  - 3 * SIZE
	STFD	[BOFFSET2] = f120, - 3 * SIZE
	}
	;;
	adds	C3 = -1 * SIZE, C3
	adds	C4 = -1 * SIZE, C4
	adds	C5 = -1 * SIZE, C5
	adds	C6 = -1 * SIZE, C6
	adds	C7 = -1 * SIZE, C7
	adds	C8 = -1 * SIZE, C8
	;;
#endif

#ifdef LT
	LDFD	f32 = [AOFFSET]
	;;
	{ .mfi
	FMPY	f64  = f64,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f96  = f96,  f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f72  = f72,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f104 = f104, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f80  = f80,  f32
	}
	{ .mfi
	nop	__LINE__
	FMPY	f112 = f112, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f88  = f88,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f120 = f120, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f64, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f96, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f72, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f104, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f80, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f112, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f88, -3 * SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f120, -3 * SIZE
	}
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f40 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f41, f42 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f43, f44 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f45, f46 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f47, f48 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f49, f50 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f51, f52 = [BOFFSET]
	adds	BOFFSET = 5 * SIZE, BOFFSET
	;;
	LDFD	f53 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [BOFFSET]
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [BOFFSET]
	adds	BOFFSET = 7 * SIZE, BOFFSET
	;;
	LDFD	f16 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f17, f18 = [BOFFSET]
	adds	BOFFSET = 8 * SIZE, BOFFSET
	;;
	LDFPD	f19, f20 = [BOFFSET]
	adds	BOFFSET = 9 * SIZE, BOFFSET
	;;
	LDFD	f21 = [BOFFSET]
	adds	BOFFSET = -63 * SIZE, BOFFSET
	;;

	FMPY	f64  = f64,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	;;
	FNMA	f80  = f64,  f34, f80
	;;
	FNMA	f88  = f64,  f35, f88
	;;
	FNMA	f96  = f64,  f36, f96
	;;
	FNMA	f104 = f64,  f37, f104
	;;
	FNMA	f112 = f64,  f38, f112
	;;
	FNMA	f120 = f64,  f39, f120
	;;
	FMPY	f72  = f72,  f40
	;;
	FNMA	f80  = f72,  f41, f80
	;;
	FNMA	f88  = f72,  f42, f88
	;;
	FNMA	f96  = f72,  f43, f96
	;;
	FNMA	f104 = f72,  f44, f104
	;;
	FNMA	f112 = f72,  f45, f112
	;;
	FNMA	f120 = f72,  f46, f120
	;;
	FMPY	f80  = f80,  f47
	;;
	FNMA	f88  = f80,  f48, f88
	;;
	FNMA	f96  = f80,  f49, f96
	;;
	FNMA	f104 = f80,  f50, f104
	;;
	FNMA	f112 = f80,  f51, f112
	;;
	FNMA	f120 = f80,  f52, f120
	;;
	FMPY	f88  = f88,  f53
	;;
	FNMA	f96  = f88,  f54, f96
	;;
	FNMA	f104 = f88,  f55, f104
	;;
	FNMA	f112 = f88,  f56, f112
	;;
	FNMA	f120 = f88,  f57, f120
	;;
	FMPY	f96  = f96,  f58
	;;
	FNMA	f104 = f96,  f59, f104
	;;
	FNMA	f112 = f96,  f60, f112
	;;
	FNMA	f120 = f96,  f61, f120
	;;
	FMPY	f104 = f104, f16
	;;
	FNMA	f112 = f104, f17, f112
	;;
	FNMA	f120 = f104, f18, f120
	;;
	FMPY	f112 = f112, f19
	;;
	FNMA	f120 = f112, f20, f120
	;;
	FMPY	f120 = f120, f21
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f96, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f104, SIZE
	;;
	STFD	[AOFFSET] = f80, SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET] = f88, -3 * SIZE
	STFD	[AOFFSET2] = f120, - 3 * SIZE
	;;
#endif

#ifdef RT
       	adds	BOFFSET = 62 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f35, f34 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f37, f36 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f39, f38 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f40 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f42, f41 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f44, f43 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f46, f45 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f48, f47 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f50, f49 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f52, f51 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFD	f53 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f55, f54 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f57, f56 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	LDFPD	f59, f58 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f61, f60 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	LDFD	f16 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f18, f17 = [BOFFSET]
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
	LDFPD	f20, f19 = [BOFFSET]
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
	LDFD	f21 = [BOFFSET]
	;;

	FMPY	f120 = f120, f32
	;;
	FNMA	f112 = f120, f33, f112
	;;
	FNMA	f104 = f120, f34, f104
	;;
	FNMA	f96  = f120, f35, f96
	;;
	FNMA	f88  = f120, f36, f88
	;;
	FNMA	f80  = f120, f37, f80
	;;
	FNMA	f72  = f120, f38, f72
	;;
	FNMA	f64  = f120, f39, f64
	;;
	FMPY	f112 = f112, f40
	;;
	FNMA	f104 = f112, f41, f104
	;;
	FNMA	f96  = f112, f42, f96
	;;
	FNMA	f88  = f112, f43, f88
	;;
	FNMA	f80  = f112, f44, f80
	;;
	FNMA	f72  = f112, f45, f72
	;;
	FNMA	f64  = f112, f46, f64
	;;
	FMPY	f104 = f104, f47
	;;
	FNMA	f96  = f104, f48, f96
	;;
	FNMA	f88  = f104, f49, f88
	;;
	FNMA	f80  = f104, f50, f80
	;;
	FNMA	f72  = f104, f51, f72
	;;
	FNMA	f64  = f104, f52, f64
	;;
	FMPY	f96  = f96,  f53
	;;
	FNMA	f88  = f96,  f54, f88
	;;
	FNMA	f80  = f96,  f55, f80
	;;
	FNMA	f72  = f96,  f56, f72
	;;
	FNMA	f64  = f96,  f57, f64
	;;
	FMPY	f88  = f88,  f58
	;;
	FNMA	f80  = f88,  f59, f80
	;;
	FNMA	f72  = f88,  f60, f72
	;;
	FNMA	f64  = f88,  f61, f64
	;;
	FMPY	f80  = f80,  f16
	;;
	FNMA	f72  = f80,  f17, f72
	;;
	FNMA	f64  = f80,  f18, f64
	;;
	FMPY	f72  = f72,  f19
	;;
	FNMA	f64  = f72,  f20, f64
	;;
	FMPY	f64  = f64,  f21
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f96, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f104, SIZE
	;;
	STFD	[AOFFSET]  = f80, SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET]  = f88,  - 3 * SIZE
	STFD	[AOFFSET2] = f120, - 3 * SIZE
	;;

#endif

#ifndef LN
	STFD	[C1 ] = f64, SIZE
#else
	STFD	[C1 ] = f64
#endif
#ifndef LN
	STFD	[C2 ] = f72, SIZE
#else
	STFD	[C2 ] = f72
#endif
#ifndef LN
	STFD	[C3 ] = f80, SIZE
#else
	STFD	[C3 ] = f80
#endif
#ifndef LN
	STFD	[C4 ] = f88, SIZE
#else
	STFD	[C4 ] = f88
#endif
#ifndef LN
	STFD	[C5 ] = f96,  SIZE
#else
	STFD	[C5 ] = f96
#endif
#ifndef LN
	STFD	[C6 ] = f104, SIZE
#else
	STFD	[C6 ] = f104
#endif
#ifndef LN
	STFD	[C7 ] = f112, SIZE
#else
	STFD	[C7 ] = f112
#endif
#ifndef LN
	STFD	[C8 ] = f120, SIZE
#else
	STFD	[C8 ] = f120
#endif
	;;

	mov	f64  = f0
	mov	f72  = f0
	mov	f80  = f0
	mov	f88  = f0
	mov	f96  = f0
	mov	f104 = f0
	mov	f112 = f0
	mov	f120 = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	add	AORIG = r2, AORIG
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	add	AOFFSET = L, AOFFSET
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 3, BOFFSET
#else
	nop	__LINE__
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
	.align 8

.L030:
	{ .mib
	sub	L = K, KK
	tbit.z	p6, p0 = M, 1
	(p6)	br.cond.dptk .L020
	}
	;;
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	setf.d	f73  = r0
	mov	f65  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 3, B
	mov	f65  = f0
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
	shladd	AOFFSET = r3, 1, AORIG
	}
	;;
#endif
	{ .mfi
	setf.d	f105 = r0
	mov	f81  = f0
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	mov	f89  = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f113 = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	setf.d	f97  = r0
	mov	f121 = f0
	shr	L = L, 1
	}
	;;
	{ .mmf
	(p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	adds	L =  -1, L
	}
	;;
	{ .mmf
	(p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mib
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L038
	}
	;;

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
	nop	__LINE__
	(p3) FMA	f112  = f40, f62, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f97   = f41, f60, f97	// A2 * B5
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
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
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
	br.cloop.sptk.few .L032
	}
	;;

.L038:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -8, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	shladd	BOFFSET = r2, 3, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [BOFFSET]
	adds	BOFFSET = -14 * SIZE, BOFFSET
	;;
	{ .mfi
	FSUB	f64  = f32, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f72  = f33, f72
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f80  = f34, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f88  = f35, f88
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f96  = f36, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f104 = f37, f104
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f112 = f38, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f120 = f39, f120
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f65  = f40, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f73  = f41, f73
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f81  = f42, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f89  = f43, f89
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f97  = f44, f97
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f105 = f45, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	FSUB	f113 = f46, f113
	}
	{ .mfi
	nop	__LINE__
	FSUB	f121 = f47, f121
	nop	__LINE__
	}
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [AOFFSET]
	adds	AOFFSET = -14 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65

	FSUB	f72  = f34, f72
	FSUB	f73  = f35, f73

	FSUB	f80  = f36, f80
	FSUB	f81  = f37, f81

	FSUB	f88  = f38, f88
	FSUB	f89  = f39, f89
	;;
	FSUB	f96  = f40, f96
	FSUB	f97  = f41, f97
	;;
	FSUB	f104 = f42, f104
	FSUB	f105 = f43, f105
	;;
	FSUB	f112 = f44, f112
	FSUB	f113 = f45, f113
	;;
	FSUB	f120 = f46, f120
	FSUB	f121 = f47, f121
	;;
#endif

#ifdef LN
	adds	AOFFSET = 2 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET]
	;;
	FMPY	f65  = f65,  f32
	FMPY	f97  = f97,  f32
	FMPY	f73  = f73,  f32
 	FMPY	f105 = f105, f32
	FMPY	f81  = f81,  f32
	FMPY	f113 = f113, f32
	FMPY	f89  = f89,  f32
	FMPY	f121 = f121, f32
	;;
	FNMA	f64  = f65,  f33, f64
	FNMA	f96  = f97,  f33, f96
	FNMA	f72  = f73,  f33, f72
	FNMA	f104 = f105, f33, f104
	FNMA	f80  = f81,  f33, f80
	FNMA	f112 = f113, f33, f112
	FNMA	f88  = f89,  f33, f88
	FNMA	f120 = f121, f33, f120
	;;
	FMPY	f64  = f64,  f34
	FMPY	f96  = f96,  f34
	FMPY	f72  = f72,  f34
	FMPY	f104 = f104, f34
	FMPY	f80  = f80,  f34
 	FMPY	f112 = f112, f34
	FMPY	f88  = f88,  f34
	FMPY	f120 = f120, f34
	;;
	adds	BOFFSET  =  8 * SIZE, BOFFSET
	adds	BOFFSET2 =  8 * SIZE, BOFFSET2
	;;
	{ .mfi
	STFD	[BOFFSET]  = f65, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f97, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f73, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f105, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f81, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f113, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f89,  - 11 * SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f121, - 11 * SIZE
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f96, SIZE
	adds	C1 = -2 * SIZE, C1
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f104, SIZE
	adds	C2 = -2 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f112, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f88,  - 3 * SIZE
	STFD	[BOFFSET2] = f120, - 3 * SIZE
	}
	;;
	adds	C3 = -2 * SIZE, C3
	adds	C4 = -2 * SIZE, C4
	adds	C5 = -2 * SIZE, C5
	adds	C6 = -2 * SIZE, C6
	adds	C7 = -2 * SIZE, C7
	adds	C8 = -2 * SIZE, C8
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET], - 3 * SIZE
	;;
	{ .mfi
	FMPY	f64  = f64,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f96  = f96,  f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f72  = f72,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f104 = f104, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f80  = f80,  f32
	}
	{ .mfi
	nop	__LINE__
	FMPY	f112 = f112, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FMPY	f88  = f88,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f120 = f120, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	FNMA	f65  = f64,  f33, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f97  = f96,  f33, f97
	nop	__LINE__
	}
	;;
	{ .mfi
	FNMA	f73  = f72,  f33, f73
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f105 = f104, f33, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	FNMA	f81  = f80,  f33, f81
	}
	{ .mfi
	nop	__LINE__
	FNMA	f113 = f112, f33, f113
	nop	__LINE__
	}
	;;
	{ .mfi
	FNMA	f89  = f88,  f33, f89
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f121 = f120, f33, f121
	nop	__LINE__
	}
	;;
	FMPY	f65  = f65,  f34
	FMPY	f97  = f97,  f34
	FMPY	f73  = f73,  f34
	FMPY	f105 = f105, f34
	FMPY	f81  = f81,  f34
	FMPY	f113 = f113, f34
	FMPY	f89  = f89,  f34
	FMPY	f121 = f121, f34
	;;
	{ .mfi
	STFD	[BOFFSET]  = f64, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f96, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f72, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f104, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f80, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f112, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f88, 5 * SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f120, 5 * SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f65, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f97, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f73, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f105, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f81, SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f113, SIZE
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f89, -11 * SIZE
	}
	{ .mfi
	STFD	[BOFFSET2] = f121, -11 * SIZE
	}
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f40 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f41, f42 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f43, f44 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f45, f46 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f47, f48 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f49, f50 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f51, f52 = [BOFFSET]
	adds	BOFFSET = 5 * SIZE, BOFFSET
	;;
	LDFD	f53 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [BOFFSET]
	adds	BOFFSET = 6 * SIZE, BOFFSET
	;;
	LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [BOFFSET]
	adds	BOFFSET = 7 * SIZE, BOFFSET
	;;
	LDFD	f16 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f17, f18 = [BOFFSET]
	adds	BOFFSET = 8 * SIZE, BOFFSET
	;;
	LDFPD	f19, f20 = [BOFFSET]
	adds	BOFFSET = 9 * SIZE, BOFFSET
	;;
	LDFD	f21 = [BOFFSET]
	adds	BOFFSET = -63 * SIZE, BOFFSET
	;;

	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f73  = f65,  f33, f73
	;;
	FNMA	f80  = f64,  f34, f80
	FNMA	f81  = f65,  f34, f81
	;;
	FNMA	f88  = f64,  f35, f88
	FNMA	f89  = f65,  f35, f89
	;;
	FNMA	f96  = f64,  f36, f96
	FNMA	f97  = f65,  f36, f97
	;;
	FNMA	f104 = f64,  f37, f104
	FNMA	f105 = f65,  f37, f105
	;;
	FNMA	f112 = f64,  f38, f112
	FNMA	f113 = f65,  f38, f113
	;;
	FNMA	f120 = f64,  f39, f120
	FNMA	f121 = f65,  f39, f121
	;;
	FMPY	f72  = f72,  f40
	FMPY	f73  = f73,  f40
	;;
	FNMA	f80  = f72,  f41, f80
	FNMA	f81  = f73,  f41, f81
	;;
	FNMA	f88  = f72,  f42, f88
	FNMA	f89  = f73,  f42, f89
	;;
	FNMA	f96  = f72,  f43, f96
	FNMA	f97  = f73,  f43, f97
	;;
	FNMA	f104 = f72,  f44, f104
	FNMA	f105 = f73,  f44, f105
	;;
	FNMA	f112 = f72,  f45, f112
	FNMA	f113 = f73,  f45, f113
	;;
	FNMA	f120 = f72,  f46, f120
	FNMA	f121 = f73,  f46, f121
	;;
	FMPY	f80  = f80,  f47
	FMPY	f81  = f81,  f47
	;;
	FNMA	f88  = f80,  f48, f88
	FNMA	f89  = f81,  f48, f89
	;;
	FNMA	f96  = f80,  f49, f96
	FNMA	f97  = f81,  f49, f97
	;;
	FNMA	f104 = f80,  f50, f104
	FNMA	f105 = f81,  f50, f105
	;;
	FNMA	f112 = f80,  f51, f112
	FNMA	f113 = f81,  f51, f113
	;;
	FNMA	f120 = f80,  f52, f120
	FNMA	f121 = f81,  f52, f121
	;;
	FMPY	f88  = f88,  f53
	FMPY	f89  = f89,  f53
	;;
	FNMA	f96  = f88,  f54, f96
	FNMA	f97  = f89,  f54, f97
	;;
	FNMA	f104 = f88,  f55, f104
	FNMA	f105 = f89,  f55, f105
	;;
	FNMA	f112 = f88,  f56, f112
	FNMA	f113 = f89,  f56, f113
	;;
	FNMA	f120 = f88,  f57, f120
	FNMA	f121 = f89,  f57, f121
	;;
	FMPY	f96  = f96,  f58
	FMPY	f97  = f97,  f58
	;;
	FNMA	f104 = f96,  f59, f104
	FNMA	f105 = f97,  f59, f105
	;;
	FNMA	f112 = f96,  f60, f112
	FNMA	f113 = f97,  f60, f113
	;;
	FNMA	f120 = f96,  f61, f120
	FNMA	f121 = f97,  f61, f121
	;;
	FMPY	f104 = f104, f16
	FMPY	f105 = f105, f16
	;;
	FNMA	f112 = f104, f17, f112
	FNMA	f113 = f105, f17, f113
	;;
	FNMA	f120 = f104, f18, f120
	FNMA	f121 = f105, f18, f121
	;;
	FMPY	f112 = f112, f19
	FMPY	f113 = f113, f19
	;;
	FNMA	f120 = f112, f20, f120
	FNMA	f121 = f113, f20, f121
	;;
	FMPY	f120 = f120, f21
	FMPY	f121 = f121, f21
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f80, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2]  = f81, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f88, SIZE
	;;
	STFD	[AOFFSET]  = f73, 5 * SIZE
	STFD	[AOFFSET2] = f89, 5 * SIZE
	;;
	STFD	[AOFFSET] = f96, SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET] = f97, SIZE
	STFD	[AOFFSET2] = f113, SIZE
	;;
	STFD	[AOFFSET] = f104, SIZE
	STFD	[AOFFSET2] = f120, SIZE
	;;
	STFD	[AOFFSET] = f105, -11 * SIZE
	STFD	[AOFFSET2] = f121, - 11 * SIZE
	;;
#endif

#ifdef RT
       	adds	BOFFSET = 62 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f35, f34 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f37, f36 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f39, f38 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f40 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f42, f41 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f44, f43 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f46, f45 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f48, f47 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f50, f49 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f52, f51 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFD	f53 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f55, f54 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f57, f56 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	LDFPD	f59, f58 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f61, f60 = [BOFFSET]
	adds	BOFFSET = - 6 * SIZE, BOFFSET
	;;
	LDFD	f16 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f18, f17 = [BOFFSET]
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
	LDFPD	f20, f19 = [BOFFSET]
	adds	BOFFSET = - 8 * SIZE, BOFFSET
	;;
	LDFD	f21 = [BOFFSET]
	;;

	FMPY	f120 = f120, f32
	FMPY	f121 = f121, f32
	;;
	FNMA	f112 = f120, f33, f112
	FNMA	f113 = f121, f33, f113
	;;
	FNMA	f104 = f120, f34, f104
	FNMA	f105 = f121, f34, f105
	;;
	FNMA	f96  = f120, f35, f96
	FNMA	f97  = f121, f35, f97
	;;
	FNMA	f88  = f120, f36, f88
	FNMA	f89  = f121, f36, f89
	;;
	FNMA	f80  = f120, f37, f80
	FNMA	f81  = f121, f37, f81
	;;
	FNMA	f72  = f120, f38, f72
	FNMA	f73  = f121, f38, f73
	;;
	FNMA	f64  = f120, f39, f64
	FNMA	f65  = f121, f39, f65
	;;
	FMPY	f112 = f112, f40
	FMPY	f113 = f113, f40
	;;
	FNMA	f104 = f112, f41, f104
	FNMA	f105 = f113, f41, f105
	;;
	FNMA	f96  = f112, f42, f96
	FNMA	f97  = f113, f42, f97
	;;
	FNMA	f88  = f112, f43, f88
	FNMA	f89  = f113, f43, f89
	;;
	FNMA	f80  = f112, f44, f80
	FNMA	f81  = f113, f44, f81
	;;
	FNMA	f72  = f112, f45, f72
	FNMA	f73  = f113, f45, f73
	;;
	FNMA	f64  = f112, f46, f64
	FNMA	f65  = f113, f46, f65
	;;
	FMPY	f104 = f104, f47
	FMPY	f105 = f105, f47
	;;
	FNMA	f96  = f104, f48, f96
	FNMA	f97  = f105, f48, f97
	;;
	FNMA	f88  = f104, f49, f88
	FNMA	f89  = f105, f49, f89
	;;
	FNMA	f80  = f104, f50, f80
	FNMA	f81  = f105, f50, f81
	;;
	FNMA	f72  = f104, f51, f72
	FNMA	f73  = f105, f51, f73
	;;
	FNMA	f64  = f104, f52, f64
	FNMA	f65  = f105, f52, f65
	;;
	FMPY	f96  = f96,  f53
	FMPY	f97  = f97,  f53
	;;
	FNMA	f88  = f96,  f54, f88
	FNMA	f89  = f97,  f54, f89
	;;
	FNMA	f80  = f96,  f55, f80
	FNMA	f81  = f97,  f55, f81
	;;
	FNMA	f72  = f96,  f56, f72
	FNMA	f73  = f97,  f56, f73
	;;
	FNMA	f64  = f96,  f57, f64
	FNMA	f65  = f97,  f57, f65
	;;
	FMPY	f88  = f88,  f58
	FMPY	f89  = f89,  f58
	;;
	FNMA	f80  = f88,  f59, f80
	FNMA	f81  = f89,  f59, f81
	;;
	FNMA	f72  = f88,  f60, f72
	FNMA	f73  = f89,  f60, f73
	;;
	FNMA	f64  = f88,  f61, f64
	FNMA	f65  = f89,  f61, f65
	;;
	FMPY	f80  = f80,  f16
	FMPY	f81  = f81,  f16
	;;
	FNMA	f72  = f80,  f17, f72
	FNMA	f73  = f81,  f17, f73
	;;
	FNMA	f64  = f80,  f18, f64
 	FNMA	f65  = f81,  f18, f65
	;;
	FMPY	f72  = f72,  f19
	FMPY	f73  = f73,  f19
	;;
	FNMA	f64  = f72,  f20, f64
	FNMA	f65  = f73,  f20, f65
	;;
	FMPY	f64  = f64,  f21
	FMPY	f65  = f65,  f21
	;;
	adds	AOFFSET  = 8 * SIZE, AOFFSET
	adds	AOFFSET2 = 8 * SIZE, AOFFSET2
	;;
	STFD	[AOFFSET]  = f96, SIZE
	STFD	[AOFFSET2] = f112, SIZE
	;;
	STFD	[AOFFSET]  = f97, SIZE
	STFD	[AOFFSET2] = f113, SIZE
	;;
	STFD	[AOFFSET]  = f104, SIZE
	STFD	[AOFFSET2] = f120, SIZE
	;;
	STFD	[AOFFSET]  = f105, - 11 * SIZE
	STFD	[AOFFSET2] = f121, - 11 * SIZE
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f80, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f81, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f88, SIZE
	;;
	STFD	[AOFFSET]  = f73, - 3 * SIZE
	STFD	[AOFFSET2] = f89, - 3 * SIZE
	;;

#endif
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	;;
#ifndef LN
	STFD	[C1 ] = f65, SIZE
#else
	STFD	[C1 ] = f65, -SIZE
#endif
	;;
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	;;
#ifndef LN
	STFD	[C2 ] = f73, SIZE
#else
	STFD	[C2 ] = f73, -SIZE
#endif
	;;
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
	;;
#ifndef LN
	STFD	[C3 ] = f81, SIZE
#else
	STFD	[C3 ] = f81, - SIZE
#endif
	;;
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
	;;
#ifndef LN
	STFD	[C4 ] = f89, SIZE
#else
	STFD	[C4 ] = f89, -SIZE
#endif
	;;
	STFD	[C5 ] = f96,  SIZE
	mov	f96  = f0
	;;
#ifndef LN
	STFD	[C5 ] = f97,  SIZE
#else
	STFD	[C5 ] = f97,  -SIZE
#endif
	;;
	STFD	[C6 ] = f104, SIZE
	mov	f104 = f0
	;;
#ifndef LN
	STFD	[C6 ] = f105, SIZE
#else
	STFD	[C6 ] = f105, -SIZE
#endif
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#else
	nop	__LINE__
#endif
	;;
	STFD	[C7 ] = f112, SIZE
	mov	f112 = f0
	;;
	{ .mmi
#ifndef LN
	STFD	[C7 ] = f113, SIZE
#else
	STFD	[C7 ] = f113, -SIZE
#endif

#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 3, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
	STFD	[C8 ] = f120, SIZE
	mov	f120 = f0
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C8 ] = f121, SIZE
#else
	STFD	[C8 ] = f121, -SIZE
#endif

#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L020:
	{ .mib
	 sub	L = K, KK
	tbit.z	p6, p0 = M, 2
	(p6)	br.cond.dptk .L010
	 }
	 ;;
	 ;;
	 { .mmi
	 cmp.ne	p7, p0 = r0, L
	 adds	BOFFSET = 0 * SIZE, B
	 shl	r2 = K, 2 + BASE_SHIFT
	 }
	 { .mmi
	 shladd	r3 = KK, BASE_SHIFT, r0
	 nop	__LINE__
	 nop	__LINE__
	 }
	 ;;
 #if defined(LT) || defined(RN)
	 { .mmf
	 (p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	 setf.d	f73  = r0
	 mov	f65  = f0
	 }
	 ;;
 #else
	 { .mfi
	 shladd	BOFFSET = r3, 3, B
	 mov	f65  = f0
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
	 shladd	AOFFSET = r3, 2, AORIG
	 }
	 ;;
 #endif
	 { .mfi
	 setf.d	f105 = r0
	 mov	f81  = f0
	 adds	L =  1, L
	 }
	 { .mfi
	 adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	 mov	f89  = f0
	 cmp.eq	p3, p0 = r0, r0
	 }
	 ;;
	 { .mfi
	 (p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	 mov	f113 = f0
	 tbit.z	p12, p0 = L, 0
	 }
	 { .mfi
	 setf.d	f97  = r0
	 mov	f121 = f0
	 shr	L = L, 1
	 }
	 ;;
	 { .mmf
	 (p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	 setf.d	f66  = r0
	 mov	f67  = f0
	 }
	 { .mfi
	 setf.d	f74  = r0
	 mov	f75  = f0
	 adds	L =  -1, L
	 }
	 ;;
	 { .mmf
	 (p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	 setf.d	f82  = r0
	 mov	f83  = f0
	 }
	 { .mfi
	 setf.d	f90  = r0
	 mov	f91  = f0
	 cmp.eq  p6, p0 = -1, L
	 }
	 ;;
	 { .mmf
	 (p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	 setf.d	f98  = r0
	 mov	f99  = f0
	 }
	 { .mfi
	 setf.d	f106 = r0
	 mov	f107 = f0
	 mov	ar.lc = L
	 }
	 ;;
	 { .mmf
	 (p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	 setf.d	f114 = r0
	 mov	f115 = f0
	 }
	 { .mfb
	 setf.d	f122 = r0
	 mov	f123 = f0
	 (p6) br.cond.dpnt   .L028
	 }
	 ;;

 .L022:
	 { .mfi
	 lfetch.nt1	[PREA],  8 * SIZE
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
	 nop	__LINE__
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
	 nop	__LINE__
	 (p3) FMA	f112  = f40, f62, f112	// A1 * B7
	 nop	__LINE__
	 }
	 { .mfb
	 nop	__LINE__
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
	 nop	__LINE__
	 (p3) FMA	f97   = f41, f60, f97	// A2 * B5
	 nop	__LINE__
	 }
	 { .mfb
	 nop	__LINE__
	 (p3) FMA	f105  = f41, f61, f105	// A2 * B6
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
	 (p3) FMA	f121  = f41, f63, f121	// A2 * B8
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
	 (p3) FMA	f74   = f42, f57, f74	// A3 * B2
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
	 (p3) FMA	f90   = f42, f59, f90	// A3 * B4
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
	 (p3) FMA	f106  = f42, f61, f106	// A3 * B6
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
	 (p3) FMA	f122  = f42, f63, f122	// A3 * B8
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
	 (p3) FMA	f75   = f43, f57, f75	// A4 * B2
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
	 (p3) FMA	f91   = f43, f59, f91	// A4 * B4
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
	 (p3) FMA	f107  = f43, f61, f107	// A4 * B6
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
	 (p3) FMA	f123  = f43, f63, f123	// A4 * B8
	 br.cloop.sptk.few .L022
	 }
	 ;;

 .L028:
 #if defined(LN) || defined(RT)
 #ifdef LN
	 adds	r2 = -4, KK
 #else
	 adds	r2 = -8, KK
 #endif
	 ;;
	 shladd	r2 = r2, BASE_SHIFT, r0
	 ;;
	 shladd	AOFFSET = r2, 2, AORIG
	 shladd	BOFFSET = r2, 3, B
	 ;;
 #endif
	 adds	AOFFSET2 = 4 * SIZE, AOFFSET
	 adds	BOFFSET2 = 4 * SIZE, BOFFSET
	 ;;

 #if defined(LN) || defined(LT)
	 LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f46, f47 = [BOFFSET], 2 * SIZE
	 ;;
	 { .mfi
	 LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	 FSUB	f64  = f32, f64
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f72  = f33, f72
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	 FSUB	f80  = f34, f80
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f88  = f35, f88
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	 FSUB	f96  = f36, f96
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f104 = f37, f104
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	 FSUB	f112 = f38, f112
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f120 = f39, f120
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	 FSUB	f65  = f40, f65
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f73  = f41, f73
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	 FSUB	f81  = f42, f81
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f89  = f43, f89
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	 FSUB	f97  = f44, f97
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f105 = f45, f105
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 LDFPD	f62, f63 = [BOFFSET]
	 FSUB	f113 = f46, f113
	 adds	BOFFSET = -30 * SIZE, BOFFSET
	 }
	 { .mfi
	 nop	__LINE__
	 FSUB	f121 = f47, f121
	 nop	__LINE__
	 }
	 ;;
	 FSUB	f66  = f48, f66
	 FSUB	f74  = f49, f74
	 FSUB	f82  = f50, f82
	 FSUB	f90  = f51, f90
	 FSUB	f98  = f52, f98
	 FSUB	f106 = f53, f106
	 FSUB	f114 = f54, f114
	 FSUB	f122 = f55, f122
	 ;;
	 FSUB	f67  = f56, f67
	 FSUB	f75  = f57, f75
	 FSUB	f83  = f58, f83
	 FSUB	f91  = f59, f91
	 FSUB	f99  = f60, f99
	 FSUB	f107 = f61, f107
	 FSUB	f115 = f62, f115
	 FSUB	f123 = f63, f123
	 ;;
 #else
	 LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f48, f49 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f50, f51 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f52, f53 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f54, f55 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f56, f57 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f58, f59 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f60, f61 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f62, f63 = [AOFFSET]
	 adds	AOFFSET = -30 * SIZE, AOFFSET
	 ;;
	 FSUB	f64  = f32, f64
	 FSUB	f65  = f33, f65
	 FSUB	f66  = f34, f66
	 FSUB	f67  = f35, f67

	 FSUB	f72  = f36, f72
	 FSUB	f73  = f37, f73
	 FSUB	f74  = f38, f74
	 FSUB	f75  = f39, f75

	 FSUB	f80  = f40, f80
	 FSUB	f81  = f41, f81
	 FSUB	f82  = f42, f82
	 FSUB	f83  = f43, f83

	 FSUB	f88  = f44, f88
	 FSUB	f89  = f45, f89
	 FSUB	f90  = f46, f90
	 FSUB	f91  = f47, f91
	 ;;
	 FSUB	f96  = f48, f96
	 FSUB	f97  = f49, f97
	 FSUB	f98  = f50, f98
	 FSUB	f99  = f51, f99
	 ;;
	 FSUB	f104 = f52, f104
	 FSUB	f105 = f53, f105
	 FSUB	f106 = f54, f106
	 FSUB	f107 = f55, f107
	 ;;
	 FSUB	f112 = f56, f112
	 FSUB	f113 = f57, f113
	 FSUB	f114 = f58, f114
	 FSUB	f115 = f59, f115
	 ;;
	 FSUB	f120 = f60, f120
	 FSUB	f121 = f61, f121
	 FSUB	f122 = f62, f122
	 FSUB	f123 = f63, f123
	 ;;
 #endif

 #ifdef LN
	 adds	AOFFSET = 14 * SIZE, AOFFSET
	 ;;
	 LDFPD	f33, f32 = [AOFFSET]
	 adds	AOFFSET = - 2 * SIZE, AOFFSET
	 ;;
	 LDFPD	f35, f34 = [AOFFSET]
	 adds	AOFFSET = - 2 * SIZE, AOFFSET
	 ;;
	 LDFD	f36 = [AOFFSET], - 2 * SIZE
	 ;;
	 LDFPD	f38, f37 = [AOFFSET]
	 adds	AOFFSET = - 4 * SIZE, AOFFSET
	 ;;
	 LDFPD	f40, f39 = [AOFFSET]
	 adds	AOFFSET = - 4 * SIZE, AOFFSET
	 ;;
	 LDFD	f41 = [AOFFSET]
	 ;;
	 FMPY	f67  = f67,  f32
	 FMPY	f99  = f99,  f32
	 FMPY	f75  = f75,  f32
	 FMPY	f107 = f107, f32
	 FMPY	f83  = f83,  f32
	 FMPY	f115 = f115, f32
	 FMPY	f91  = f91,  f32
	 FMPY	f123 = f123, f32
	 ;;
	 FNMA	f66  = f67,  f33, f66
	 FNMA	f98  = f99,  f33, f98
	 FNMA	f74  = f75,  f33, f74
	 FNMA	f106 = f107, f33, f106
	 FNMA	f82  = f83,  f33, f82
	 FNMA	f114 = f115, f33, f114
	 FNMA	f90  = f91,  f33, f90
	 FNMA	f122 = f123, f33, f122
	 ;;
	 FNMA	f65  = f67,  f34, f65
	 FNMA	f97  = f99,  f34, f97
	 FNMA	f73  = f75,  f34, f73
	 FNMA	f105 = f107, f34, f105
	 FNMA	f81  = f83,  f34, f81
	 FNMA	f113 = f115, f34, f113
	 FNMA	f89  = f91,  f34, f89
	 FNMA	f121 = f123, f34, f121
	 ;;
	 FNMA	f64  = f67,  f35, f64
	 FNMA	f96  = f99,  f35, f96
	 FNMA	f72  = f75,  f35, f72
	 FNMA	f104 = f107, f35, f104
	 FNMA	f80  = f83,  f35, f80
	 FNMA	f112 = f115, f35, f112
	 FNMA	f88  = f91,  f35, f88
	 FNMA	f120 = f123, f35, f120
	 ;;
	 FMPY	f66  = f66,  f36
	 FMPY	f98  = f98,  f36
	 FMPY	f74  = f74,  f36
	 FMPY	f106 = f106, f36
	 FMPY	f82  = f82,  f36
	 FMPY	f114 = f114, f36
	 FMPY	f90  = f90,  f36
	 FMPY	f122 = f122, f36
	 ;;
	 FNMA	f65  = f66,  f37, f65
	 FNMA	f97  = f98,  f37, f97
	 FNMA	f73  = f74,  f37, f73
	 FNMA	f105 = f106, f37, f105
	 FNMA	f81  = f82,  f37, f81
	 FNMA	f113 = f114, f37, f113
	 FNMA	f89  = f90,  f37, f89
	 FNMA	f121 = f122, f37, f121
	 ;;
	 FNMA	f64  = f66,  f38, f64
	 FNMA	f96  = f98,  f38, f96
	 FNMA	f72  = f74,  f38, f72
	 FNMA	f104 = f106, f38, f104
	 FNMA	f80  = f82,  f38, f80
	 FNMA	f112 = f114, f38, f112
	 FNMA	f88  = f90,  f38, f88
	 FNMA	f120 = f122, f38, f120
	 ;;
	 adds	BOFFSET  = 24 * SIZE, BOFFSET
	 adds	BOFFSET2 = 24 * SIZE, BOFFSET2
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f67, SIZE
	 FMPY	f65  = f65,  f39
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f99, SIZE
	 FMPY	f97  = f97,  f39
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f75, SIZE
	 FMPY	f73  = f73,  f39
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f107, SIZE
	 FMPY	f105 = f105, f39
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f83, SIZE
	 FMPY	f81  = f81,  f39
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f115, SIZE
	 FMPY	f113 = f113, f39
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f91,  - 11 * SIZE
	 FMPY	f89  = f89,  f39
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f123, - 11 * SIZE
	 FMPY	f121 = f121, f39
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f66, SIZE
	 FNMA	f64  = f65,  f40, f64
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f98, SIZE
	 FNMA	f96  = f97,  f40, f96
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f74, SIZE
	 FNMA	f72  = f73,  f40, f72
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f106, SIZE
	 FNMA	f104 = f105, f40, f104
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f82, SIZE
	 FNMA	f80  = f81,  f40, f80
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f114, SIZE
	 FNMA	f112 = f113, f40, f112
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f90,  -11 * SIZE
	 FNMA	f88  = f89,  f40, f88
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f122, -11 * SIZE
	 FNMA	f120 = f121, f40, f120
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f65, SIZE
	 FMPY	f64  = f64,  f41
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f97, SIZE
	 FMPY	f96  = f96,  f41
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f73, SIZE
	 FMPY	f72  = f72,  f41
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f105, SIZE
	 FMPY	f104 = f104, f41
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f81, SIZE
	 FMPY	f80  = f80,  f41
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f113, SIZE
	 FMPY	f112 = f112, f41
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f89,  - 11 * SIZE
	 FMPY	f88  = f88,  f41
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f121, - 11 * SIZE
	 FMPY	f120 = f120, f41
	 }
	 ;;
	 { .mmi
	 STFD	[BOFFSET]  = f64, SIZE
	 STFD	[BOFFSET2] = f96, SIZE
	 adds	C1 = -4 * SIZE, C1
	 }
	 ;;
	 { .mmi
	 STFD	[BOFFSET]  = f72, SIZE
	 STFD	[BOFFSET2] = f104, SIZE
	 adds	C2 = -4 * SIZE, C2
	 }
	 ;;
	 { .mmi
	 STFD	[BOFFSET]  = f80, SIZE
	 STFD	[BOFFSET2] = f112, SIZE
	 nop	__LINE__
	 }
	 ;;
	 { .mmi
	 STFD	[BOFFSET]  = f88,  - 3 * SIZE
	 STFD	[BOFFSET2] = f120, - 3 * SIZE
	 }
	 ;;
 #endif

 #ifdef LT
	 LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f34, f35 = [AOFFSET]
	 adds	AOFFSET = 3 * SIZE, AOFFSET
	 ;;
	 LDFD	f36 = [AOFFSET], 1 * SIZE
	 ;;
	 LDFPD	f37, f38 = [AOFFSET]
	 adds	AOFFSET = 4 * SIZE, AOFFSET
	 ;;
	 LDFPD	f39, f40 = [AOFFSET]
	 adds	AOFFSET = 5 * SIZE, AOFFSET
	 ;;
	 LDFD	f41 = [AOFFSET], -15 * SIZE
	 ;;
	 { .mfi
	 FMPY	f64  = f64,  f32
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FMPY	f96  = f96,  f32
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FMPY	f72  = f72,  f32
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FMPY	f104 = f104, f32
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FMPY	f80  = f80,  f32
	 }
	 { .mfi
	 nop	__LINE__
	 FMPY	f112 = f112, f32
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FMPY	f88  = f88,  f32
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FMPY	f120 = f120, f32
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f65  = f64,  f33, f65
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f97  = f96,  f33, f97
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f73  = f72,  f33, f73
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f105 = f104, f33, f105
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f81  = f80,  f33, f81
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f113 = f112, f33, f113
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f89  = f88,  f33, f89
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f121 = f120, f33, f121
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f66  = f64,  f34, f66
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f98  = f96,  f34, f98
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f74  = f72,  f34, f74
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f106 = f104, f34, f106
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f82  = f80,  f34, f82
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f114 = f112, f34, f114
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f90  = f88,  f34, f90
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f122 = f120, f34, f122
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f67  = f64,  f35, f67
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f99  = f96,  f35, f99
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f75  = f72,  f35, f75
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f107 = f104, f35, f107
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f83  = f80,  f35, f83
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f115 = f112, f35, f115
	 nop	__LINE__
	 }
	 ;;
	 { .mfi
	 FNMA	f91  = f88,  f35, f91
	 nop	__LINE__
	 }
	 { .mfi
	 nop	__LINE__
	 FNMA	f123 = f120, f35, f123
	 nop	__LINE__
	 }
	 ;;
	 FMPY	f65  = f65,  f36
	 FMPY	f97  = f97,  f36
	 FMPY	f73  = f73,  f36
	 FMPY	f105 = f105, f36
	 FMPY	f81  = f81,  f36
	 FMPY	f113 = f113, f36
	 FMPY	f89  = f89,  f36
	 FMPY	f121 = f121, f36
	 ;;
	 FNMA	f66  = f65,  f37, f66
	 FNMA	f98  = f97,  f37, f98
	 FNMA	f74  = f73,  f37, f74
	 FNMA	f106 = f105, f37, f106
	 FNMA	f82  = f81,  f37, f82
	 FNMA	f114 = f113, f37, f114
	 FNMA	f90  = f89,  f37, f90
	 FNMA	f122 = f121, f37, f122
	 ;;
	 FNMA	f67  = f65,  f38, f67
	 FNMA	f99  = f97,  f38, f99
	 FNMA	f75  = f73,  f38, f75
	 FNMA	f107 = f105, f38, f107
	 FNMA	f83  = f81,  f38, f83
	 FNMA	f115 = f113, f38, f115
	 FNMA	f91  = f89,  f38, f91
	 FNMA	f123 = f121, f38, f123
	 ;;
	 FMPY	f66  = f66,  f39
	 FMPY	f98  = f98,  f39
	 FMPY	f74  = f74,  f39
	 FMPY	f106 = f106, f39
	 FMPY	f82  = f82,  f39
	 FMPY	f114 = f114, f39
	 FMPY	f90  = f90,  f39
	 FMPY	f122 = f122, f39
	 ;;
	 FNMA	f67  = f66,  f40, f67
	 FNMA	f99  = f98,  f40, f99
	 FNMA	f75  = f74,  f40, f75
	 FNMA	f107 = f106, f40, f107
	 FNMA	f83  = f82,  f40, f83
	 FNMA	f115 = f114, f40, f115
	 FNMA	f91  = f90,  f40, f91
	 FNMA	f123 = f122, f40, f123
	 ;;
	 FMPY	f67  = f67,  f41
	 FMPY	f99  = f99,  f41
	 FMPY	f75  = f75,  f41
	 FMPY	f107 = f107, f41
	 FMPY	f83  = f83,  f41
	 FMPY	f115 = f115, f41
	 FMPY	f91  = f91,  f41
	 FMPY	f123 = f123, f41
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f64, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f96, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f72, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f104, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f80, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f112, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f88, 5 * SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f120, 5 * SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f65, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f97, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f73, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f105, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f81, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f113, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f89, 5 * SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f121, 5 * SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f66, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f98, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f74, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f106, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f82, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f114, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f90, 5 * SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f122, 5 * SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f67, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f99, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f75, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f107, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f83, SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f115, SIZE
	 }
	 ;;
	 { .mfi
	 STFD	[BOFFSET]  = f91, -27 * SIZE
	 }
	 { .mfi
	 STFD	[BOFFSET2] = f123, -27 * SIZE
	 }
	 ;;
 #endif

 #ifdef RN
	 LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f38, f39 = [BOFFSET]
	 adds	BOFFSET = 3 * SIZE, BOFFSET
	 ;;
	 LDFD	f40 = [BOFFSET], 1 * SIZE
	 ;;
	 LDFPD	f41, f42 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f43, f44 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f45, f46 = [BOFFSET]
	 adds	BOFFSET = 4 * SIZE, BOFFSET
	 ;;
	 LDFPD	f47, f48 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f49, f50 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f51, f52 = [BOFFSET]
	 adds	BOFFSET = 5 * SIZE, BOFFSET
	 ;;
	 LDFD	f53 = [BOFFSET], 1 * SIZE
	 ;;
	 LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f56, f57 = [BOFFSET]
	 adds	BOFFSET = 6 * SIZE, BOFFSET
	 ;;
	 LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	 ;;
	 LDFPD	f60, f61 = [BOFFSET]
	 adds	BOFFSET = 7 * SIZE, BOFFSET
	 ;;
	 LDFD	f16 = [BOFFSET], 1 * SIZE
	 ;;
	 LDFPD	f17, f18 = [BOFFSET]
	 adds	BOFFSET = 8 * SIZE, BOFFSET
	 ;;
	 LDFPD	f19, f20 = [BOFFSET]
	 adds	BOFFSET = 9 * SIZE, BOFFSET
	 ;;
	 LDFD	f21 = [BOFFSET]
	 adds	BOFFSET = -63 * SIZE, BOFFSET
	 ;;


	 FMPY	f64  = f64,  f32
	 FMPY	f65  = f65,  f32
	 FMPY	f66  = f66,  f32
	 FMPY	f67  = f67,  f32
	 ;;
	 FNMA	f72  = f64,  f33, f72
	 FNMA	f73  = f65,  f33, f73
	 FNMA	f74  = f66,  f33, f74
	 FNMA	f75  = f67,  f33, f75
	 ;;
	 FNMA	f80  = f64,  f34, f80
	 FNMA	f81  = f65,  f34, f81
	 FNMA	f82  = f66,  f34, f82
	 FNMA	f83  = f67,  f34, f83
	 ;;
	 FNMA	f88  = f64,  f35, f88
	 FNMA	f89  = f65,  f35, f89
	 FNMA	f90  = f66,  f35, f90
	 FNMA	f91  = f67,  f35, f91
	 ;;
	 FNMA	f96  = f64,  f36, f96
	 FNMA	f97  = f65,  f36, f97
	 FNMA	f98  = f66,  f36, f98
	 FNMA	f99  = f67,  f36, f99
	 ;;
	 FNMA	f104 = f64,  f37, f104
	 FNMA	f105 = f65,  f37, f105
	 FNMA	f106 = f66,  f37, f106
	 FNMA	f107 = f67,  f37, f107
	 ;;
	 FNMA	f112 = f64,  f38, f112
	 FNMA	f113 = f65,  f38, f113
	 FNMA	f114 = f66,  f38, f114
	 FNMA	f115 = f67,  f38, f115
	 ;;
	 FNMA	f120 = f64,  f39, f120
	 FNMA	f121 = f65,  f39, f121
	 FNMA	f122 = f66,  f39, f122
	 FNMA	f123 = f67,  f39, f123
	 ;;
	 FMPY	f72  = f72,  f40
	 FMPY	f73  = f73,  f40
	 FMPY	f74  = f74,  f40
	 FMPY	f75  = f75,  f40
	 ;;
	 FNMA	f80  = f72,  f41, f80
	 FNMA	f81  = f73,  f41, f81
	 FNMA	f82  = f74,  f41, f82
	 FNMA	f83  = f75,  f41, f83
	 ;;
	 FNMA	f88  = f72,  f42, f88
	 FNMA	f89  = f73,  f42, f89
	 FNMA	f90  = f74,  f42, f90
	 FNMA	f91  = f75,  f42, f91
	 ;;
	 FNMA	f96  = f72,  f43, f96
	 FNMA	f97  = f73,  f43, f97
	 FNMA	f98  = f74,  f43, f98
	 FNMA	f99  = f75,  f43, f99
	 ;;
	 FNMA	f104 = f72,  f44, f104
	 FNMA	f105 = f73,  f44, f105
	 FNMA	f106 = f74,  f44, f106
	 FNMA	f107 = f75,  f44, f107
	 ;;
	 FNMA	f112 = f72,  f45, f112
	 FNMA	f113 = f73,  f45, f113
	 FNMA	f114 = f74,  f45, f114
	 FNMA	f115 = f75,  f45, f115
	 ;;
	 FNMA	f120 = f72,  f46, f120
	 FNMA	f121 = f73,  f46, f121
	 FNMA	f122 = f74,  f46, f122
	 FNMA	f123 = f75,  f46, f123
	 ;;
	 FMPY	f80  = f80,  f47
	 FMPY	f81  = f81,  f47
	 FMPY	f82  = f82,  f47
	 FMPY	f83  = f83,  f47
	 ;;
	 FNMA	f88  = f80,  f48, f88
	 FNMA	f89  = f81,  f48, f89
	 FNMA	f90  = f82,  f48, f90
	 FNMA	f91  = f83,  f48, f91
	 ;;
	 FNMA	f96  = f80,  f49, f96
	 FNMA	f97  = f81,  f49, f97
	 FNMA	f98  = f82,  f49, f98
	 FNMA	f99  = f83,  f49, f99
	 ;;
	 FNMA	f104 = f80,  f50, f104
	 FNMA	f105 = f81,  f50, f105
	 FNMA	f106 = f82,  f50, f106
	 FNMA	f107 = f83,  f50, f107
	 ;;
	 FNMA	f112 = f80,  f51, f112
	 FNMA	f113 = f81,  f51, f113
	 FNMA	f114 = f82,  f51, f114
	 FNMA	f115 = f83,  f51, f115
	 ;;
	 FNMA	f120 = f80,  f52, f120
	 FNMA	f121 = f81,  f52, f121
	 FNMA	f122 = f82,  f52, f122
	 FNMA	f123 = f83,  f52, f123
	 ;;
	 FMPY	f88  = f88,  f53
	 FMPY	f89  = f89,  f53
	 FMPY	f90  = f90,  f53
	 FMPY	f91  = f91,  f53
	 ;;
	 FNMA	f96  = f88,  f54, f96
	 FNMA	f97  = f89,  f54, f97
	 FNMA	f98  = f90,  f54, f98
	 FNMA	f99  = f91,  f54, f99
	 ;;
	 FNMA	f104 = f88,  f55, f104
	 FNMA	f105 = f89,  f55, f105
	 FNMA	f106 = f90,  f55, f106
	 FNMA	f107 = f91,  f55, f107
	 ;;
	 FNMA	f112 = f88,  f56, f112
	 FNMA	f113 = f89,  f56, f113
	 FNMA	f114 = f90,  f56, f114
	 FNMA	f115 = f91,  f56, f115
	 ;;
	 FNMA	f120 = f88,  f57, f120
	 FNMA	f121 = f89,  f57, f121
	 FNMA	f122 = f90,  f57, f122
	 FNMA	f123 = f91,  f57, f123
	 ;;
	 FMPY	f96  = f96,  f58
	 FMPY	f97  = f97,  f58
	 FMPY	f98  = f98,  f58
	 FMPY	f99  = f99,  f58
	 ;;
	 FNMA	f104 = f96,  f59, f104
	 FNMA	f105 = f97,  f59, f105
	 FNMA	f106 = f98,  f59, f106
	 FNMA	f107 = f99,  f59, f107
	 ;;
	 FNMA	f112 = f96,  f60, f112
	 FNMA	f113 = f97,  f60, f113
	 FNMA	f114 = f98,  f60, f114
	 FNMA	f115 = f99,  f60, f115
	 ;;
	 FNMA	f120 = f96,  f61, f120
	 FNMA	f121 = f97,  f61, f121
	 FNMA	f122 = f98,  f61, f122
	 FNMA	f123 = f99,  f61, f123
	 ;;
	 FMPY	f104 = f104, f16
	 FMPY	f105 = f105, f16
	 FMPY	f106 = f106, f16
	 FMPY	f107 = f107, f16
	 ;;
	 FNMA	f112 = f104, f17, f112
	 FNMA	f113 = f105, f17, f113
	 FNMA	f114 = f106, f17, f114
	 FNMA	f115 = f107, f17, f115
	 ;;
	 FNMA	f120 = f104, f18, f120
	 FNMA	f121 = f105, f18, f121
	 FNMA	f122 = f106, f18, f122
	 FNMA	f123 = f107, f18, f123
	 ;;
	 FMPY	f112 = f112, f19
	 FMPY	f113 = f113, f19
	 FMPY	f114 = f114, f19
	 FMPY	f115 = f115, f19
	 ;;
	 FNMA	f120 = f112, f20, f120
	 FNMA	f121 = f113, f20, f121
	 FNMA	f122 = f114, f20, f122
	 FNMA	f123 = f115, f20, f123
	 ;;
	 FMPY	f120 = f120, f21
	 FMPY	f121 = f121, f21
	 FMPY	f122 = f122, f21
	 FMPY	f123 = f123, f21
	 ;;
	 STFD	[AOFFSET]  = f64, SIZE
	 STFD	[AOFFSET2]  = f72, SIZE
	 ;;
	 STFD	[AOFFSET]  = f65, SIZE
	 STFD	[AOFFSET2]  = f73, SIZE
	 ;;
	 STFD	[AOFFSET]  = f66, SIZE
	 STFD	[AOFFSET2]  = f74, SIZE
	 ;;
	 STFD	[AOFFSET]  = f67, 5 * SIZE
	 STFD	[AOFFSET2]  = f75, 5 * SIZE
	 ;;
	 STFD	[AOFFSET]  = f80, SIZE
	 STFD	[AOFFSET2] = f88, SIZE
	 ;;
	 STFD	[AOFFSET]  = f81, SIZE
	 STFD	[AOFFSET2] = f89, SIZE
	 ;;
	 STFD	[AOFFSET]  = f82, SIZE
	 STFD	[AOFFSET2] = f90, SIZE
	 ;;
	 STFD	[AOFFSET]  = f83, 5 * SIZE
	 STFD	[AOFFSET2] = f91, 5 * SIZE
	 ;;
	 STFD	[AOFFSET] = f96, SIZE
	 STFD	[AOFFSET2] = f104, SIZE
	 ;;
	 STFD	[AOFFSET] = f97, SIZE
	 STFD	[AOFFSET2] = f105, SIZE
	 ;;
	 STFD	[AOFFSET] = f98, SIZE
	 STFD	[AOFFSET2] = f106, SIZE
	 ;;
	 STFD	[AOFFSET] = f99, 5 * SIZE
	 STFD	[AOFFSET2] = f107, 5 * SIZE
	 ;;
	 STFD	[AOFFSET] = f112, SIZE
	 STFD	[AOFFSET2] = f120, SIZE
	 ;;
	 STFD	[AOFFSET] = f113, SIZE
	 STFD	[AOFFSET2] = f121, SIZE
	 ;;
	 STFD	[AOFFSET] = f114, SIZE
	 STFD	[AOFFSET2] = f122, SIZE
	 ;;
	 STFD	[AOFFSET] = f115, -27 * SIZE
	 STFD	[AOFFSET2] = f123, - 27 * SIZE
	 ;;
 #endif

 #ifdef RT
	 adds	BOFFSET = 62 * SIZE, BOFFSET
	 ;;
	 LDFPD	f33, f32 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f35, f34 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f37, f36 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f39, f38 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFD	f40 = [BOFFSET], -2 * SIZE
	 ;;
	 LDFPD	f42, f41 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f44, f43 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f46, f45 = [BOFFSET]
	 adds	BOFFSET = - 4 * SIZE, BOFFSET
	 ;;
	 LDFPD	f48, f47 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f50, f49 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f52, f51 = [BOFFSET]
	 adds	BOFFSET = - 4 * SIZE, BOFFSET
	 ;;
	 LDFD	f53 = [BOFFSET], -2 * SIZE
	 ;;
	 LDFPD	f55, f54 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f57, f56 = [BOFFSET]
	 adds	BOFFSET = - 6 * SIZE, BOFFSET
	 ;;
	 LDFPD	f59, f58 = [BOFFSET]
	 adds	BOFFSET = - 2 * SIZE, BOFFSET
	 ;;
	 LDFPD	f61, f60 = [BOFFSET]
	 adds	BOFFSET = - 6 * SIZE, BOFFSET
	 ;;
	 LDFD	f16 = [BOFFSET], -2 * SIZE
	 ;;
	 LDFPD	f18, f17 = [BOFFSET]
	 adds	BOFFSET = - 8 * SIZE, BOFFSET
	 ;;
	 LDFPD	f20, f19 = [BOFFSET]
	 adds	BOFFSET = - 8 * SIZE, BOFFSET
	 ;;
	 LDFD	f21 = [BOFFSET]
	 ;;

	 FMPY	f120 = f120, f32
	 FMPY	f121 = f121, f32
	 FMPY	f122 = f122, f32
	 FMPY	f123 = f123, f32
	 ;;
	 FNMA	f112 = f120, f33, f112
	 FNMA	f113 = f121, f33, f113
	 FNMA	f114 = f122, f33, f114
	 FNMA	f115 = f123, f33, f115
	 ;;
	 FNMA	f104 = f120, f34, f104
	 FNMA	f105 = f121, f34, f105
	 FNMA	f106 = f122, f34, f106
	 FNMA	f107 = f123, f34, f107
	 ;;
	 FNMA	f96  = f120, f35, f96
	 FNMA	f97  = f121, f35, f97
	 FNMA	f98  = f122, f35, f98
	 FNMA	f99  = f123, f35, f99
	 ;;
	 FNMA	f88  = f120, f36, f88
	 FNMA	f89  = f121, f36, f89
	 FNMA	f90  = f122, f36, f90
	 FNMA	f91  = f123, f36, f91
	 ;;
	 FNMA	f80  = f120, f37, f80
	 FNMA	f81  = f121, f37, f81
	 FNMA	f82  = f122, f37, f82
	 FNMA	f83  = f123, f37, f83
	 ;;
	 FNMA	f72  = f120, f38, f72
	 FNMA	f73  = f121, f38, f73
	 FNMA	f74  = f122, f38, f74
	 FNMA	f75  = f123, f38, f75
	 ;;
	 FNMA	f64  = f120, f39, f64
	 FNMA	f65  = f121, f39, f65
	 FNMA	f66  = f122, f39, f66
	 FNMA	f67  = f123, f39, f67
	 ;;
	 FMPY	f112 = f112, f40
	 FMPY	f113 = f113, f40
	 FMPY	f114 = f114, f40
	 FMPY	f115 = f115, f40
	 ;;
	 FNMA	f104 = f112, f41, f104
	 FNMA	f105 = f113, f41, f105
	 FNMA	f106 = f114, f41, f106
	 FNMA	f107 = f115, f41, f107
	 ;;
	 FNMA	f96  = f112, f42, f96
	 FNMA	f97  = f113, f42, f97
	 FNMA	f98  = f114, f42, f98
	 FNMA	f99  = f115, f42, f99
	 ;;
	 FNMA	f88  = f112, f43, f88
	 FNMA	f89  = f113, f43, f89
	 FNMA	f90  = f114, f43, f90
	 FNMA	f91  = f115, f43, f91
	 ;;
	 FNMA	f80  = f112, f44, f80
	 FNMA	f81  = f113, f44, f81
	 FNMA	f82  = f114, f44, f82
	 FNMA	f83  = f115, f44, f83
	 ;;
	 FNMA	f72  = f112, f45, f72
	 FNMA	f73  = f113, f45, f73
	 FNMA	f74  = f114, f45, f74
	 FNMA	f75  = f115, f45, f75
	 ;;
	 FNMA	f64  = f112, f46, f64
	 FNMA	f65  = f113, f46, f65
	 FNMA	f66  = f114, f46, f66
	 FNMA	f67  = f115, f46, f67
	 ;;
	 FMPY	f104 = f104, f47
	 FMPY	f105 = f105, f47
	 FMPY	f106 = f106, f47
	 FMPY	f107 = f107, f47
	 ;;
	 FNMA	f96  = f104, f48, f96
	 FNMA	f97  = f105, f48, f97
	 FNMA	f98  = f106, f48, f98
	 FNMA	f99  = f107, f48, f99
	 ;;
	 FNMA	f88  = f104, f49, f88
	 FNMA	f89  = f105, f49, f89
	 FNMA	f90  = f106, f49, f90
	 FNMA	f91  = f107, f49, f91
	 ;;
	 FNMA	f80  = f104, f50, f80
	 FNMA	f81  = f105, f50, f81
	 FNMA	f82  = f106, f50, f82
	 FNMA	f83  = f107, f50, f83
	 ;;
	 FNMA	f72  = f104, f51, f72
	 FNMA	f73  = f105, f51, f73
	 FNMA	f74  = f106, f51, f74
	 FNMA	f75  = f107, f51, f75
	 ;;
	 FNMA	f64  = f104, f52, f64
	 FNMA	f65  = f105, f52, f65
	 FNMA	f66  = f106, f52, f66
	 FNMA	f67  = f107, f52, f67
	 ;;
	 FMPY	f96  = f96,  f53
	 FMPY	f97  = f97,  f53
	 FMPY	f98  = f98,  f53
	 FMPY	f99  = f99,  f53
	 ;;
	 FNMA	f88  = f96,  f54, f88
	 FNMA	f89  = f97,  f54, f89
	 FNMA	f90  = f98,  f54, f90
	 FNMA	f91  = f99,  f54, f91
	 ;;
	 FNMA	f80  = f96,  f55, f80
	 FNMA	f81  = f97,  f55, f81
	 FNMA	f82  = f98,  f55, f82
	 FNMA	f83  = f99,  f55, f83
	 ;;
	 FNMA	f72  = f96,  f56, f72
	 FNMA	f73  = f97,  f56, f73
	 FNMA	f74  = f98,  f56, f74
	 FNMA	f75  = f99,  f56, f75
	 ;;
	 FNMA	f64  = f96,  f57, f64
	 FNMA	f65  = f97,  f57, f65
	 FNMA	f66  = f98,  f57, f66
	 FNMA	f67  = f99,  f57, f67
	 ;;
	 FMPY	f88  = f88,  f58
	 FMPY	f89  = f89,  f58
	 FMPY	f90  = f90,  f58
	 FMPY	f91  = f91,  f58
	 ;;
	 FNMA	f80  = f88,  f59, f80
	 FNMA	f81  = f89,  f59, f81
	 FNMA	f82  = f90,  f59, f82
	 FNMA	f83  = f91,  f59, f83
	 ;;
	 FNMA	f72  = f88,  f60, f72
	 FNMA	f73  = f89,  f60, f73
	 FNMA	f74  = f90,  f60, f74
	 FNMA	f75  = f91,  f60, f75
	 ;;
	 FNMA	f64  = f88,  f61, f64
	 FNMA	f65  = f89,  f61, f65
	 FNMA	f66  = f90,  f61, f66
	 FNMA	f67  = f91,  f61, f67
	 ;;
	 FMPY	f80  = f80,  f16
	 FMPY	f81  = f81,  f16
	 FMPY	f82  = f82,  f16
	 FMPY	f83  = f83,  f16
	 ;;
	 FNMA	f72  = f80,  f17, f72
	 FNMA	f73  = f81,  f17, f73
	 FNMA	f74  = f82,  f17, f74
	 FNMA	f75  = f83,  f17, f75
	 ;;
	 FNMA	f64  = f80,  f18, f64
	 FNMA	f65  = f81,  f18, f65
	 FNMA	f66  = f82,  f18, f66
	 FNMA	f67  = f83,  f18, f67
	 ;;
	 FMPY	f72  = f72,  f19
	 FMPY	f73  = f73,  f19
	 FMPY	f74  = f74,  f19
	 FMPY	f75  = f75,  f19
	 ;;
	 FNMA	f64  = f72,  f20, f64
	 FNMA	f65  = f73,  f20, f65
	 FNMA	f66  = f74,  f20, f66
	 FNMA	f67  = f75,  f20, f67
	 ;;
	 FMPY	f64  = f64,  f21
	 FMPY	f65  = f65,  f21
	 FMPY	f66  = f66,  f21
	 FMPY	f67  = f67,  f21
	 ;;
	 adds	AOFFSET  = 24 * SIZE, AOFFSET
	 adds	AOFFSET2 = 24 * SIZE, AOFFSET2
	 ;;
	 STFD	[AOFFSET] = f112, SIZE
	 STFD	[AOFFSET2] = f120, SIZE
	 ;;
	 STFD	[AOFFSET] = f113, SIZE
	 STFD	[AOFFSET2] = f121, SIZE
	 ;;
	 STFD	[AOFFSET] = f114, SIZE
	 STFD	[AOFFSET2] = f122, SIZE
	 ;;
	 STFD	[AOFFSET] = f115,  - 11 * SIZE
	 STFD	[AOFFSET2] = f123,  - 11 * SIZE
	 ;;
	 STFD	[AOFFSET] = f96, SIZE
	 STFD	[AOFFSET2] = f104, SIZE
	 ;;
	 STFD	[AOFFSET] = f97, SIZE
	 STFD	[AOFFSET2] = f105, SIZE
	 ;;
	 STFD	[AOFFSET] = f98, SIZE
	 STFD	[AOFFSET2] = f106, SIZE
	 ;;
	 STFD	[AOFFSET] = f99,  - 11 * SIZE
	 STFD	[AOFFSET2] = f107,  - 11 * SIZE
	 ;;
	 STFD	[AOFFSET]  = f80, SIZE
	 STFD	[AOFFSET2] = f88, SIZE
	 ;;
	 STFD	[AOFFSET]  = f81, SIZE
	 STFD	[AOFFSET2] = f89, SIZE
	 ;;
	 STFD	[AOFFSET]  = f82, SIZE
	 STFD	[AOFFSET2] = f90, SIZE
	 ;;
	 STFD	[AOFFSET]  = f83, - 11 * SIZE
	 STFD	[AOFFSET2] = f91,  - 11 * SIZE
	 ;;
	 STFD	[AOFFSET]  = f64, SIZE
	 STFD	[AOFFSET2]  = f72, SIZE
	 ;;
	 STFD	[AOFFSET]  = f65, SIZE
	 STFD	[AOFFSET2]  = f73, SIZE
	 ;;
	 STFD	[AOFFSET]  = f66, SIZE
	 STFD	[AOFFSET2]  = f74, SIZE
	 ;;
	 STFD	[AOFFSET]  = f67, - 3 * SIZE
	 STFD	[AOFFSET2]  = f75, - 3 * SIZE
	 ;;

 #endif
	 { .mmf
	 STFD	[C1 ] = f64, SIZE
	 mov	f64  = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C1 ] = f65, SIZE
	 }
	 ;;
	 { .mmi
	 STFD	[C1 ] = f66, SIZE
 #ifdef LN
	 adds	C3 = -4 * SIZE, C3
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C1 ] = f67, SIZE
 #else
	 STFD	[C1 ] = f67, - 3 * SIZE
 #endif
	 }
	 ;;
	 { .mmf
	 STFD	[C2 ] = f72, SIZE
	 mov	f72  = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C2 ] = f73, SIZE
 #ifdef LN
	 adds	C4 = -4 * SIZE, C4
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
	 STFD	[C2 ] = f74, SIZE
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C2 ] = f75, SIZE
 #else
	 STFD	[C2 ] = f75, - 3 * SIZE
 #endif
 #ifdef LN
	 adds	C5 = -4 * SIZE, C5
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmf
	 STFD	[C3 ] = f80, SIZE
	 mov	f80  = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C3 ] = f81, SIZE
	 }
	 ;;
	 { .mmi
	 STFD	[C3 ] = f82, SIZE
 #ifdef LN
	 adds	C6 = -4 * SIZE, C6
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C3 ] = f83, SIZE
 #else
	 STFD	[C3 ] = f83, - 3 * SIZE
 #endif
	 }
	 ;;
	 { .mmf
	 STFD	[C4 ] = f88, SIZE
	 mov	f88  = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C4 ] = f89, SIZE
 #ifdef LN
	 adds	C8 = -4 * SIZE, C8
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
	 STFD	[C4 ] = f90, SIZE
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C4 ] = f91, SIZE
 #else
	 STFD	[C4 ] = f91, - 3 * SIZE
 #endif
	 nop	__LINE__
	 }
	 ;;
	 { .mmf
	 STFD	[C5 ] = f96,  SIZE
	 mov	f96  = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C5 ] = f97,  SIZE
	 nop	__LINE__
	 }
	 ;;
	 { .mmi
	 STFD	[C5 ] = f98,  SIZE
 #ifdef LN
	 adds	C7 = -4 * SIZE, C7
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C5 ] = f99,  SIZE
 #else
	 STFD	[C5 ] = f99,  - 3 * SIZE
 #endif
	 }
	 ;;
	 { .mmf
	 STFD	[C6 ] = f104, SIZE
	 mov	f104 = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C6 ] = f105, SIZE
	 shladd	r2 = K, BASE_SHIFT, r0
	 }
	 ;;
	 { .mmi
	 STFD	[C6 ] = f106, SIZE
	 sub	L = K, KK
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C6 ] = f107, SIZE
 #else
	 STFD	[C6 ] = f107, - 3 * SIZE
 #endif
 #ifdef RT
	 shladd	AORIG = r2, 2, AORIG
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmf
	 STFD	[C7 ] = f112, SIZE
	 mov	f112 = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C7 ] = f113, SIZE
 #if defined(LT) || defined(RN)
	 shladd	L = L, BASE_SHIFT, r0
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
	 STFD	[C7 ] = f114, SIZE
 #if defined(LT) || defined(RN)
	 shladd	AOFFSET = L, 2, AOFFSET
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
 #ifndef LN
	 STFD	[C7 ] = f115, SIZE
 #else
	 STFD	[C7 ] = f115, - 3 * SIZE
 #endif
 #if defined(LT) || defined(RN)
	 shladd	BOFFSET = L, 3, BOFFSET
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmf
	 STFD	[C8 ] = f120, SIZE
	 mov	f120 = f0
	 }
	 ;;
	 { .mmi
	 STFD	[C8 ] = f121, SIZE
 #ifdef LT
	 adds	KK =  4, KK
 #elif defined LN
	 adds	KK = -4, KK
 #else
	 nop	__LINE__
 #endif
	 }
	 ;;
	 { .mmi
	 STFD	[C8 ] = f122, SIZE
 #if defined(LT) || defined(RN)
	 mov	L = KK
 #else
	 sub	L = K, KK
 #endif
	 }
	 ;;
	 { .mmb
 #ifndef LN
	 STFD	[C8 ] = f123, SIZE
 #else
	 STFD	[C8 ] = f123, - 3 * SIZE
 #endif
	 }
	 ;;
	 .align 8

.L010:
	{ .mib
	cmp.gt	p6, p0 = 8, M
	shr	I  = M, 3
	(p6)	br.cond.dpnt .L049
	}
	;;
	.align 8

.L011:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	shladd	r3 = KK, BASE_SHIFT, r0
	shl	r2 = K, 3 + BASE_SHIFT
	}
	;;
	{ .mmi
	shladd	BOFFSET = r3, 3, B
	sub	AORIG = AORIG, r2
	nop	__LINE__
	}
	;;
	{ .mmf
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	setf.d	f64  = r0
	mov	f72  = f0
	}
	{ .mfi
	setf.d	f80  = r0
	mov	f88  = f0
	shladd	AOFFSET = r3, 3, AORIG
	}
	;;
	{ .mmf
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	setf.d	f96  = r0
	mov	f104 = f0
	}
	{ .mfb
	setf.d	f112 = r0
	mov	f120 = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	setf.d	f65  = r0
	mov	f73  = f0
	}
	{ .mfb
	setf.d	f89  = r0
	mov	f81  = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	(p7) LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
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
	(p7) LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
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
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	setf.d	f98  = r0
	mov	f106 = f0
	}
	{ .mfi
	setf.d	f114 = r0
	mov	f122 = f0
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	;;
	{ .mmf
	(p7) LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	setf.d	f67  = r0
	mov	f75  = f0
	}
	{ .mfi
	setf.d	f83  = r0
	mov	f91  = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mmf
	(p7) LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
	setf.d	f99  = r0
	mov	f107 = f0
	}
	{ .mfi
	setf.d	f115 = r0
	mov	f123 = f0
	adds	L =  1, L
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
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
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
	adds	PREB = (PREFETCHSIZE - 8) * SIZE, BOFFSET
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
	tbit.z	p12, p0 = L, 0
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
	shr	L = L, 1
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
	adds	L =  -1, L
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
	mov	ar.lc = L
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
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mmf
	CPREFETCH [PREC]
	setf.d	f103 = r0
	mov	f111 = f0
	}
	{ .mfb
	setf.d	f119 = r0
	mov	f127 = f0
	(p6) br.cond.dpnt   .L018
	}
	;;
	.align 8

.L012:
/*  1 */
	{ .mfi
	lfetch.fault.nt1	[PREA],  16 * SIZE
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
	{ .mfb
	lfetch.nt1	[PREB],  16 * SIZE
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
	(p3) LDFPD	f56, f57 = [BOFFSET],   2 * SIZE
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
	{ .mfb
	(p3) LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	FMA	f112  = f32, f54, f112	// A1 * B7
	nop	__LINE__
	}
	{ .mfb
	adds	C10 = 4 * SIZE, C2
	FMA	f120  = f32, f55, f120	// A1 * B8
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
	adds	C11 = 4 * SIZE, C3
	FMA	f73   = f33, f49, f73	// A2 * B2
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
	adds	C12 = 4 * SIZE, C4
	FMA	f89   = f33, f51, f89	// A2 * B4
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
	adds	C13 = 4 * SIZE, C5
	FMA	f105  = f33, f53, f105	// A2 * B6
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
	adds	C14 = 4 * SIZE, C6
	FMA	f121  = f33, f55, f121	// A2 * B8
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
	adds	C15 = 4 * SIZE, C7
	FMA	f74   = f34, f49, f74	// A3 * B2
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
	adds	C16 = 4 * SIZE, C8
	FMA	f90   = f34, f51, f90	// A3 * B4
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
	FMA	f106  = f34, f53, f106	// A3 * B6
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
	FMA	f122  = f34, f55, f122	// A3 * B8
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
	FMA	f75   = f35, f49, f75	// A4 * B2
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
	FMA	f91   = f35, f51, f91	// A4 * B4
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
	FMA	f107  = f35, f53, f107	// A4 * B6
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
	nop	__LINE__
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
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	FMA	f87   = f39, f50, f87	// A8 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	FMA	f95   = f39, f51, f95	// A8 * B4
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
	FMA	f111  = f39, f53, f111	// A8 * B6
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
	FMA	f127  = f39, f55, f127	// A8 * B8
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
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
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
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
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
	(p3) FMA	f104  = f40, f61, f104	// A1 * B6
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
	(p3) FMA	f120  = f40, f63, f120	// A1 * B8
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
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
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
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
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
	(p3) FMA	f105  = f41, f61, f105	// A2 * B6
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
	(p3) FMA	f121  = f41, f63, f121	// A2 * B8
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
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
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
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
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
	(p3) FMA	f106  = f42, f61, f106	// A3 * B6
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
	(p3) FMA	f122  = f42, f63, f122	// A3 * B8
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
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
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
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
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
	(p3) FMA	f107  = f43, f61, f107	// A4 * B6
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
	(p3) FMA	f123  = f43, f63, f123	// A4 * B8
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
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
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
	(p3) FMA	f92   = f44, f59, f92	// A5 * B4
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
	(p3) FMA	f108  = f44, f61, f108	// A5 * B6
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
	(p3) FMA	f124  = f44, f63, f124	// A5 * B8
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
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
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
	(p3) FMA	f93   = f45, f59, f93	// A6 * B4
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
	(p3) FMA	f109  = f45, f61, f109	// A6 * B6
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
	(p3) FMA	f125  = f45, f63, f125	// A6 * B8
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
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
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
	(p3) FMA	f94   = f46, f59, f94	// A7 * B4
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
	(p3) FMA	f110  = f46, f61, f110	// A7 * B6
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
	(p3) FMA	f126  = f46, f63, f126	// A7 * B8
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
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
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
	(p3) FMA	f95   = f47, f59, f95	// A8 * B4
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
	(p3) FMA	f111  = f47, f61, f111	// A8 * B6
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
	(p3) FMA	f127  = f47, f63, f127	// A8 * B8
	br.cloop.sptk.few .L012
	}
	;;

.L018:
	adds	r2 = -8, KK
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 3, AORIG
	shladd	BOFFSET = r2, 3, B
	;;

	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	;;

	LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [BOFFSET], 2 * SIZE
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	FSUB	f64  = f32, f64
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f72  = f33, f72
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	FSUB	f80  = f34, f80
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f88  = f35, f88
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	FSUB	f96  = f36, f96
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f104 = f37, f104
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	FSUB	f112 = f38, f112
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f120 = f39, f120
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FSUB	f65  = f40, f65
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f73  = f41, f73
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	FSUB	f81  = f42, f81
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f89  = f43, f89
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FSUB	f97  = f44, f97
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f105 = f45, f105
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f62, f63 = [BOFFSET], 2 * SIZE
	FSUB	f113 = f46, f113
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f121 = f47, f121
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	FSUB	f66  = f48, f66
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f74  = f49, f74
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	FSUB	f82  = f50, f82
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f90  = f51, f90
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	FSUB	f98  = f52, f98
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f106 = f53, f106
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	FSUB	f114 = f54, f114
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f122 = f55, f122
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	FSUB	f67  = f56, f67
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f75  = f57, f75
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	FSUB	f83  = f58, f83
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f91  = f59, f91
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	FSUB	f99  = f60, f99
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f107 = f61, f107
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f46, f47 = [BOFFSET], 2 * SIZE
	FSUB	f115 = f62, f115
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f123 = f63, f123
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	FSUB	f68  = f32, f68
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f76  = f33, f76
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	FSUB	f84  = f34, f84
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f92  = f35, f92
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	FSUB	f100 = f36, f100
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f108 = f37, f108
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	FSUB	f116 = f38, f116
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f124 = f39, f124
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	FSUB	f69  = f40, f69
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f77  = f41, f77
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	FSUB	f85  = f42, f85
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f93  = f43, f93
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	FSUB	f101 = f44, f101
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f109 = f45, f109
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f62, f63 = [BOFFSET]
	FSUB	f117 = f46, f117
	adds	BOFFSET = -62 * SIZE, BOFFSET
	}
	{ .mfi
	nop	__LINE__
	FSUB	f125 = f47, f125
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f70  = f48, f70
#ifdef LN
	adds	AOFFSET = 62 * SIZE, AOFFSET
#else
	nop	__LINE__
#endif
	}
	{ .mfi
	nop	__LINE__
	FSUB	f78  = f49, f78
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f86  = f50, f86
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f94  = f51, f94
	nop	__LINE__
	}
	;;
	{ .mfi
#ifdef LN
	LDFPD	f33, f32 = [AOFFSET]
#else
	LDFPD	f32, f33 = [AOFFSET]
#endif
	FSUB	f102 = f52, f102
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f110 = f53, f110
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f118 = f54, f118
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f126 = f55, f126
#ifdef LN
	adds	AOFFSET = - 2 * SIZE, AOFFSET
#else
	adds	AOFFSET =   2 * SIZE, AOFFSET
#endif
	}
	;;
	{ .mfi
	nop	__LINE__
	FSUB	f71  = f56, f71
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f79  = f57, f79
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f87  = f58, f87
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f95  = f59, f95
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f103 = f60, f103
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f111 = f61, f111
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f119 = f62, f119
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FSUB	f127 = f63, f127
	nop	__LINE__
	}
	;;

	{ .mfi
	LDFPD	f35, f34 = [AOFFSET]
	FMPY	f71  = f71,  f32
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f103 = f103, f32
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	}
	;;
	{ .mfi
	LDFPD	f37, f36 = [AOFFSET]
	FMPY	f79  = f79,  f32
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f111 = f111, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f39, f38 = [AOFFSET]
	FMPY	f87  = f87,  f32
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FMPY	f119 = f119, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFD	f40 = [AOFFSET], -2 * SIZE
	FMPY	f95  = f95,  f32
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FMPY	f127 = f127, f32
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f42, f41 = [AOFFSET]
	FNMA	f70  = f71,  f33, f70
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f102 = f103, f33, f102
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f44, f43 = [AOFFSET]
	FNMA	f78  = f79,  f33, f78
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f110 = f111, f33, f110
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f46, f45 = [AOFFSET]
	FNMA	f86  = f87,  f33, f86
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f118 = f119, f33, f118
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f48, f47 = [AOFFSET]
	FNMA	f94  = f95,  f33, f94
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f126 = f127, f33, f126
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f50, f49 = [AOFFSET]
	FNMA	f69  = f71,  f34, f69
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f101 = f103, f34, f101
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f52, f51 = [AOFFSET]
	FNMA	f77  = f79,  f34, f77
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f109 = f111, f34, f109
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFD	f53 = [AOFFSET], -2 * SIZE
	FNMA	f85  = f87,  f34, f85
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f117 = f119, f34, f117
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f55, f54 = [AOFFSET]
	FNMA	f93  = f95,  f34, f93
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f125 = f127, f34, f125
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f57, f56 = [AOFFSET]
	FNMA	f68  = f71,  f35, f68
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f100 = f103, f35, f100
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f59, f58 = [AOFFSET]
	FNMA	f76  = f79,  f35, f76
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f108 = f111, f35, f108
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f61, f60 = [AOFFSET]
	FNMA	f84  = f87,  f35, f84
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f116 = f119, f35, f116
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFD	f16 = [AOFFSET], -2 * SIZE
	FNMA	f92  = f95,  f35, f92
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	FNMA	f124 = f127, f35, f124
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f18, f17 = [AOFFSET]
	FNMA	f67  = f71,  f36, f67
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f99  = f103, f36, f99
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFPD	f20, f19 = [AOFFSET]
	FNMA	f75  = f79,  f36, f75
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	}
	{ .mfi
	nop	__LINE__
	FNMA	f107 = f111, f36, f107
	nop	__LINE__
	}
	;;
	{ .mfi
	LDFD	f21 = [AOFFSET]
	FNMA	f83  = f87,  f36, f83
	adds	BOFFSET  = 56 * SIZE, BOFFSET
	}
	{ .mfi
	FNMA	f115 = f119, f36, f115
	adds	BOFFSET2 = 56 * SIZE, BOFFSET2
	}
	;;
	FNMA	f91  = f95,  f36, f91
	FNMA	f123 = f127, f36, f123
	;;
	FNMA	f66  = f71,  f37, f66
	FNMA	f98  = f103, f37, f98
	FNMA	f74  = f79,  f37, f74
	FNMA	f106 = f111, f37, f106
	FNMA	f82  = f87,  f37, f82
	FNMA	f114 = f119, f37, f114
	FNMA	f90  = f95,  f37, f90
	FNMA	f122 = f127, f37, f122
	;;
	FNMA	f65  = f71,  f38, f65
	FNMA	f97  = f103, f38, f97
	FNMA	f73  = f79,  f38, f73
	FNMA	f105 = f111, f38, f105
	FNMA	f81  = f87,  f38, f81
	FNMA	f113 = f119, f38, f113
	FNMA	f89  = f95,  f38, f89
	FNMA	f121 = f127, f38, f121
	;;
	FNMA	f64  = f71,  f39, f64
	FNMA	f96  = f103, f39, f96
	FNMA	f72  = f79,  f39, f72
	FNMA	f104 = f111, f39, f104
	FNMA	f80  = f87,  f39, f80
	FNMA	f112 = f119, f39, f112
	FNMA	f88  = f95,  f39, f88
	FNMA	f120 = f127, f39, f120
	;;
	FMPY	f70  = f70,  f40
	FMPY	f102 = f102, f40
	FMPY	f78  = f78,  f40
	FMPY	f110 = f110, f40
	FMPY	f86  = f86,  f40
	FMPY	f118 = f118, f40
	FMPY	f94  = f94,  f40
	FMPY	f126 = f126, f40
	;;
	FNMA	f69  = f70,  f41, f69
	FNMA	f101 = f102, f41, f101
	FNMA	f77  = f78,  f41, f77
	FNMA	f109 = f110, f41, f109
	FNMA	f85  = f86,  f41, f85
	FNMA	f117 = f118, f41, f117
	FNMA	f93  = f94,  f41, f93
	FNMA	f125 = f126, f41, f125
	;;
	FNMA	f68  = f70,  f42, f68
	FNMA	f100 = f102, f42, f100
	FNMA	f76  = f78,  f42, f76
	FNMA	f108 = f110, f42, f108
	FNMA	f84  = f86,  f42, f84
	FNMA	f116 = f118, f42, f116
	FNMA	f92  = f94,  f42, f92
	FNMA	f124 = f126, f42, f124
	;;
	FNMA	f67  = f70,  f43, f67
	FNMA	f99  = f102, f43, f99
	FNMA	f75  = f78,  f43, f75
	FNMA	f107 = f110, f43, f107
	FNMA	f83  = f86,  f43, f83
	FNMA	f115 = f118, f43, f115
	FNMA	f91  = f94,  f43, f91
	FNMA	f123 = f126, f43, f123
	;;
	FNMA	f66  = f70,  f44, f66
	FNMA	f98  = f102, f44, f98
	FNMA	f74  = f78,  f44, f74
	FNMA	f106 = f110, f44, f106
	FNMA	f82  = f86,  f44, f82
	FNMA	f114 = f118, f44, f114
	FNMA	f90  = f94,  f44, f90
	FNMA	f122 = f126, f44, f122
	;;
	FNMA	f65  = f70,  f45, f65
	FNMA	f97  = f102, f45, f97
	FNMA	f73  = f78,  f45, f73
	FNMA	f105 = f110, f45, f105
	FNMA	f81  = f86,  f45, f81
	FNMA	f113 = f118, f45, f113
	FNMA	f89  = f94,  f45, f89
	FNMA	f121 = f126, f45, f121
	;;
	FNMA	f64  = f70,  f46, f64
	FNMA	f96  = f102, f46, f96
	FNMA	f72  = f78,  f46, f72
	FNMA	f104 = f110, f46, f104
	FNMA	f80  = f86,  f46, f80
	FNMA	f112 = f118, f46, f112
	FNMA	f88  = f94,  f46, f88
	FNMA	f120 = f126, f46, f120
	;;
	FMPY	f69  = f69,  f47
	FMPY	f101 = f101, f47
	FMPY	f77  = f77,  f47
	FMPY	f109 = f109, f47
	FMPY	f85  = f85,  f47
	FMPY	f117 = f117, f47
	FMPY	f93  = f93,  f47
	FMPY	f125 = f125, f47
	;;
	FNMA	f68  = f69,  f48, f68
	FNMA	f100 = f101, f48, f100
	FNMA	f76  = f77,  f48, f76
	FNMA	f108 = f109, f48, f108
	FNMA	f84  = f85,  f48, f84
	FNMA	f116 = f117, f48, f116
	FNMA	f92  = f93,  f48, f92
	FNMA	f124 = f125, f48, f124
	;;
	FNMA	f67  = f69,  f49, f67
	FNMA	f99  = f101, f49, f99
	FNMA	f75  = f77,  f49, f75
	FNMA	f107 = f109, f49, f107
	FNMA	f83  = f85,  f49, f83
	FNMA	f115 = f117, f49, f115
	FNMA	f91  = f93,  f49, f91
	FNMA	f123 = f125, f49, f123
	;;
	FNMA	f66  = f69,  f50, f66
	FNMA	f98  = f101, f50, f98
	FNMA	f74  = f77,  f50, f74
	FNMA	f106 = f109, f50, f106
	FNMA	f82  = f85,  f50, f82
	FNMA	f114 = f117, f50, f114
	FNMA	f90  = f93,  f50, f90
	FNMA	f122 = f125, f50, f122
	;;
	FNMA	f65  = f69,  f51, f65
	FNMA	f97  = f101, f51, f97
	FNMA	f73  = f77,  f51, f73
	FNMA	f105 = f109, f51, f105
	FNMA	f81  = f85,  f51, f81
	FNMA	f113 = f117, f51, f113
	FNMA	f89  = f93,  f51, f89
	FNMA	f121 = f125, f51, f121
	;;
	FNMA	f64  = f69,  f52, f64
	FNMA	f96  = f101, f52, f96
	FNMA	f72  = f77,  f52, f72
	FNMA	f104 = f109, f52, f104
	FNMA	f80  = f85,  f52, f80
	FNMA	f112 = f117, f52, f112
	FNMA	f88  = f93,  f52, f88
	FNMA	f120 = f125, f52, f120
	;;
	FMPY	f68  = f68,  f53
	FMPY	f100 = f100, f53
	FMPY	f76  = f76,  f53
	FMPY	f108 = f108, f53
	FMPY	f84  = f84,  f53
	FMPY	f116 = f116, f53
	FMPY	f92  = f92,  f53
	FMPY	f124 = f124, f53
	;;
	FNMA	f67  = f68,  f54, f67
	FNMA	f99  = f100, f54, f99
	FNMA	f75  = f76,  f54, f75
	FNMA	f107 = f108, f54, f107
	FNMA	f83  = f84,  f54, f83
	FNMA	f115 = f116, f54, f115
	FNMA	f91  = f92,  f54, f91
	FNMA	f123 = f124, f54, f123
	;;
	FNMA	f66  = f68,  f55, f66
	FNMA	f98  = f100, f55, f98
	FNMA	f74  = f76,  f55, f74
	FNMA	f106 = f108, f55, f106
	FNMA	f82  = f84,  f55, f82
	FNMA	f114 = f116, f55, f114
	FNMA	f90  = f92,  f55, f90
	FNMA	f122 = f124, f55, f122
	;;
	FNMA	f65  = f68,  f56, f65
	FNMA	f97  = f100, f56, f97
	FNMA	f73  = f76,  f56, f73
	FNMA	f105 = f108, f56, f105
	FNMA	f81  = f84,  f56, f81
	FNMA	f113 = f116, f56, f113
	FNMA	f89  = f92,  f56, f89
	FNMA	f121 = f124, f56, f121
	;;
	FNMA	f64  = f68,  f57, f64
	FNMA	f96  = f100, f57, f96
	FNMA	f72  = f76,  f57, f72
	FNMA	f104 = f108, f57, f104
	FNMA	f80  = f84,  f57, f80
	FNMA	f112 = f116, f57, f112
	FNMA	f88  = f92,  f57, f88
	FNMA	f120 = f124, f57, f120
	;;
	FMPY	f67  = f67,  f58
	FMPY	f99  = f99,  f58
	FMPY	f75  = f75,  f58
	FMPY	f107 = f107, f58
	FMPY	f83  = f83,  f58
	FMPY	f115 = f115, f58
	FMPY	f91  = f91,  f58
	FMPY	f123 = f123, f58
	;;
	FNMA	f66  = f67,  f59, f66
	FNMA	f98  = f99,  f59, f98
	FNMA	f74  = f75,  f59, f74
	FNMA	f106 = f107, f59, f106
	FNMA	f82  = f83,  f59, f82
	FNMA	f114 = f115, f59, f114
	FNMA	f90  = f91,  f59, f90
	FNMA	f122 = f123, f59, f122
	;;
	FNMA	f65  = f67,  f60, f65
	FNMA	f97  = f99,  f60, f97
	FNMA	f73  = f75,  f60, f73
	FNMA	f105 = f107, f60, f105
	FNMA	f81  = f83,  f60, f81
	FNMA	f113 = f115, f60, f113
	FNMA	f89  = f91,  f60, f89
	FNMA	f121 = f123, f60, f121
	;;
	{ .mfi
	STFD	[BOFFSET]  = f71, SIZE
	FNMA	f64  = f67,  f61, f64
	}
	{ .mfi
	STFD	[BOFFSET2] = f103, SIZE
	FNMA	f96  = f99,  f61, f96
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f79, SIZE
	FNMA	f72  = f75,  f61, f72
	}
	{ .mfi
	STFD	[BOFFSET2] = f111, SIZE
	FNMA	f104 = f107, f61, f104
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f87, SIZE
	FNMA	f80  = f83,  f61, f80
	}
	{ .mfi
	STFD	[BOFFSET2] = f119, SIZE
	FNMA	f112 = f115, f61, f112
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f95, - 11 * SIZE
	FNMA	f88  = f91,  f61, f88
	}
	{ .mfi
	STFD	[BOFFSET2] = f127, - 11 * SIZE
	FNMA	f120 = f123, f61, f120
	}
	;;
	{ .mfi
	STFD	[BOFFSET] = f70, SIZE
	FMPY	f66  = f66,  f16
	}
	{ .mfi
	STFD	[BOFFSET2] = f102, SIZE
	FMPY	f98  = f98,  f16
	}
	;;
	{ .mfi
	STFD	[BOFFSET] = f78, SIZE
	FMPY	f74  = f74,  f16
	}
	{ .mfi
	STFD	[BOFFSET2] = f110, SIZE
	FMPY	f106 = f106, f16
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f86, SIZE
	FMPY	f82  = f82,  f16
	}
	{ .mfi
	STFD	[BOFFSET2] = f118, SIZE
	FMPY	f114 = f114, f16
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f94, - 11 * SIZE
	FMPY	f90  = f90,  f16
	}
	{ .mfi
	STFD	[BOFFSET2] = f126, - 11 * SIZE
	FMPY	f122 = f122, f16
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f69, SIZE
	FNMA	f65  = f66,  f17, f65
	}
	{ .mfi
	STFD	[BOFFSET2] = f101, SIZE
	FNMA	f97  = f98,  f17, f97
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f77, SIZE
	FNMA	f73  = f74,  f17, f73
	}
	{ .mfi
	STFD	[BOFFSET2] = f109, SIZE
	FNMA	f105 = f106, f17, f105
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f85, SIZE
	FNMA	f81  = f82,  f17, f81
	}
	{ .mfi
	STFD	[BOFFSET2] = f117, SIZE
	FNMA	f113 = f114, f17, f113
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f93, - 11 * SIZE
	FNMA	f89  = f90,  f17, f89
	}
	{ .mfi
	STFD	[BOFFSET2] = f125, - 11 * SIZE
	FNMA	f121 = f122, f17, f121
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f68, SIZE
	FNMA	f64  = f66,  f18, f64
	}
	{ .mfi
	STFD	[BOFFSET2] = f100, SIZE
	FNMA	f96  = f98,  f18, f96
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f76, SIZE
	FNMA	f72  = f74,  f18, f72
	}
	{ .mfi
	STFD	[BOFFSET2] = f108, SIZE
	FNMA	f104 = f106, f18, f104
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f84, SIZE
	FNMA	f80  = f82,  f18, f80
	}
	{ .mfi
	STFD	[BOFFSET2] = f116, SIZE
	FNMA	f112 = f114, f18, f112
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f92, - 11 * SIZE
	FNMA	f88  = f90,  f18, f88
	}
	{ .mfi
	STFD	[BOFFSET2] = f124, - 11 * SIZE
	FNMA	f120 = f122, f18, f120
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f67, SIZE
	FMPY	f65  = f65,  f19
	}
	{ .mfi
	STFD	[BOFFSET2] = f99, SIZE
	FMPY	f97  = f97,  f19
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f75, SIZE
	FMPY	f73  = f73,  f19
	}
	{ .mfi
	STFD	[BOFFSET2] = f107, SIZE
	FMPY	f105 = f105, f19
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f83, SIZE
	FMPY	f81  = f81,  f19
	}
	{ .mfi
	STFD	[BOFFSET2] = f115, SIZE
	FMPY	f113 = f113, f19
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f91,  - 11 * SIZE
	FMPY	f89  = f89,  f19
	}
	{ .mfi
	STFD	[BOFFSET2] = f123, - 11 * SIZE
	FMPY	f121 = f121, f19
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f66, SIZE
	FNMA	f64  = f65,  f20, f64
	}
	{ .mfi
	STFD	[BOFFSET2] = f98, SIZE
	FNMA	f96  = f97,  f20, f96
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f74, SIZE
	FNMA	f72  = f73,  f20, f72
	}
	{ .mfi
	STFD	[BOFFSET2] = f106, SIZE
	FNMA	f104 = f105, f20, f104
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f82, SIZE
	FNMA	f80  = f81,  f20, f80
	}
	{ .mfi
	STFD	[BOFFSET2] = f114, SIZE
	FNMA	f112 = f113, f20, f112
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f90,  -11 * SIZE
	FNMA	f88  = f89,  f20, f88
	}
	{ .mfi
	STFD	[BOFFSET2] = f122, -11 * SIZE
	FNMA	f120 = f121, f20, f120
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f65, SIZE
	FMPY	f64  = f64,  f21
	}
	{ .mfi
	STFD	[BOFFSET2] = f97, SIZE
	FMPY	f96  = f96,  f21
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f73, SIZE
	FMPY	f72  = f72,  f21
	}
	{ .mfi
	STFD	[BOFFSET2] = f105, SIZE
	FMPY	f104 = f104, f21
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f81, SIZE
	FMPY	f80  = f80,  f21
	}
	{ .mfi
	STFD	[BOFFSET2] = f113, SIZE
 	FMPY	f112 = f112, f21
	}
	;;
	{ .mfi
	STFD	[BOFFSET]  = f89,  - 11 * SIZE
	FMPY	f88  = f88,  f21
	}
	{ .mfi
	STFD	[BOFFSET2] = f121, - 11 * SIZE
	FMPY	f120 = f120, f21
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f96, SIZE
	adds	C1 = -8 * SIZE, C1
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f104, SIZE
	adds	C2 = -8 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f112, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f88,  - 3 * SIZE
	STFD	[BOFFSET2] = f120, - 3 * SIZE
	adds	C9  = 4 * SIZE, C1
	}
	;;

	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	adds	C3 = -8 * SIZE, C3
	}
	;;
	{ .mmi
	STFD	[C1 ] = f67, - 3 * SIZE
	STFD	[C9 ] = f71
	adds	C11 = 4 * SIZE, C3
	}
	;;
	{ .mmf
	STFD	[C2 ] = f72, SIZE
	STFD	[C10] = f76, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmi
	STFD	[C2 ] = f73, SIZE
	STFD	[C10] = f77, SIZE
	adds	C4 = -8 * SIZE, C4
	}
	;;
	{ .mmi
	STFD	[C2 ] = f74, SIZE
	STFD	[C10] = f78, SIZE
	adds	C12 = 4 * SIZE, C4
	}
	;;
	{ .mmi
	STFD	[C2 ] = f75, - 3 * SIZE
	STFD	[C10] = f79
	adds	C5 = -8 * SIZE, C5
	}
	;;
	{ .mmf
	STFD	[C3 ] = f80, SIZE
	STFD	[C11] = f84, SIZE
	mov	f80  = f0
	}
	;;
	{ .mmi
	STFD	[C3 ] = f81, SIZE
	STFD	[C11] = f85, SIZE
	adds	C13 = 4 * SIZE, C5
	}
	;;
	{ .mmi
	STFD	[C3 ] = f82, SIZE
	STFD	[C11] = f86, SIZE
	adds	C6 = -8 * SIZE, C6
	}
	;;
	{ .mmi
	STFD	[C3 ] = f83, - 3 * SIZE
	STFD	[C11] = f87
	adds	C14 = 4 * SIZE, C6
	}
	;;
	{ .mmf
	STFD	[C4 ] = f88, SIZE
	STFD	[C12] = f92, SIZE
	mov	f88  = f0
	}
	;;
	{ .mmi
	STFD	[C4 ] = f89, SIZE
	STFD	[C12] = f93, SIZE
	adds	C8 = -8 * SIZE, C8
	}
	;;
	{ .mmi
	STFD	[C4 ] = f90, SIZE
	STFD	[C12] = f94, SIZE
	adds	C16 = 4 * SIZE, C8
	}
	;;
	{ .mmi
	STFD	[C4 ] = f91, - 3 * SIZE
	STFD	[C12] = f95
	cmp.ne	p6, p0 = 1, I
	}
	;;
	{ .mmf
	STFD	[C5 ] = f96,  SIZE
	STFD	[C13] = f100, SIZE
	mov	f96  = f0
	}
	;;
	{ .mmi
	STFD	[C5 ] = f97,  SIZE
	STFD	[C13] = f101, SIZE
	adds	I = -1, I
	}
	;;
	{ .mmi
	STFD	[C5 ] = f98,  SIZE
	STFD	[C13] = f102, SIZE
	adds	C7 = -8 * SIZE, C7
	}
	;;
	{ .mmi
	STFD	[C5 ] = f99,  - 3 * SIZE
	STFD	[C13] = f103
	adds	C15 = 4 * SIZE, C7
	}
	;;
	{ .mmf
	STFD	[C6 ] = f104, SIZE
	STFD	[C14] = f108, SIZE
	mov	f104 = f0
	}
	;;
	{ .mmi
	STFD	[C6 ] = f105, SIZE
	STFD	[C14] = f109, SIZE
	shladd	r2 = K, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	STFD	[C6 ] = f106, SIZE
	STFD	[C14] = f110, SIZE
	sub	L = K, KK
	}
	;;
	{ .mmi
	STFD	[C6 ] = f107, - 3 * SIZE
	STFD	[C14] = f111
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C7 ] = f112, SIZE
	STFD	[C15] = f116, SIZE
	mov	f112 = f0
	}
	;;
	{ .mmi
	STFD	[C7 ] = f113, SIZE
	STFD	[C15] = f117, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C7 ] = f114, SIZE
	STFD	[C15] = f118, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[C7 ] = f115, - 3 * SIZE
	STFD	[C15] = f119
	nop	__LINE__
	}
	;;
	{ .mmf
	STFD	[C8 ] = f120, SIZE
	STFD	[C16] = f124, SIZE
	mov	f120 = f0
	}
	;;
	{ .mmi
	STFD	[C8 ] = f121, SIZE
	STFD	[C16] = f125, SIZE
	adds	KK = -8, KK
	}
	;;
	{ .mmi
	STFD	[C8 ] = f122, SIZE
	STFD	[C16] = f126, SIZE
	sub	L = K, KK
	}
	;;
	{ .mmb
	STFD	[C8 ] = f123, - 3 * SIZE
	STFD	[C16] = f127
	(p6)	br.cond.dptk .L011
	}
	;;

.L049:
	{ .mmi
	adds	J = -1, J
	mov	AOFFSET = A
	shladd	KK8 = K, BASE_SHIFT, r0
	}
	;;
	{ .mmb
	shladd	B = KK8, 3, B
	cmp.lt	p6, p0 = 0, J
	(p6)	br.cond.dptk .L000
	}
	;;
	.align 8

.L050:
	{ .mib
	setf.d	f64  = r0
	tbit.z	p6, p0 = N, 2
	(p6)	br.cond.dpnt .L090
	}
	;;

#ifdef RT
       { .mmi
	shladd	r3 = LDC, 2, r0
	nop	__LINE__
	shl	r2 = K, 2 + BASE_SHIFT
	}
	;;
	{ .mmi
	sub	B = B, r2
	sub	C = C, r3
	nop	__LINE__
	}
#endif
	;;
	{ .mfi
	mov	C1 = C			// coffset1 = c + 0 * ldc
#ifdef LN
	add	KK = M, OFFSET
#elif defined LT
	mov	KK = OFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
#if defined(LN) || defined(RT)
	mov	AORIG = A
#else
	mov	AOFFSET = A
#endif
	}
	{ .mmf
	add	C2 = LDC, C		// coffset2 = c + 1 * ldc
	shladd	C3 = LDC, 1, C		// coffset3 = c + 2 * ldc
	}
	;;
	{ .mfi
#ifndef RT
	shladd	C = LDC, 2, C		// coffset += 8 * ldc
#else
	nop	__LINE__
#endif
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}{ .mfb
	shladd	C4 = LDC, 1, C2
	}
	;;

	mov	f72  = f0
	mov	f80  = f0
	mov	f88  = f0
	mov	f65  = f0
	mov	f73  = f0
	mov	f81  = f0
	mov	f89  = f0



	tbit.z	p6,p7  = M, 0
	(p6)	br.cond.dptk .L070

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 0 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
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
	{ .mmi
	adds	L =  1, L
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mii
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	adds	L =  -1, L
	}
	;;
	{ .mmi
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mib
	(p7) LDFD	f32 = [AOFFSET], 1 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L088
	}
	;;

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
	nop	__LINE__
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
	nop	__LINE__
	nop	__LINE__
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mib
	(p4) LDFPD	f50, f51 = [BOFFSET],  2 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	{ .mmb
	nop	__LINE__
	adds	L = -1, L
	br.cloop.sptk.few .L082
	}
	;;

.L088:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = -2 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f80  = f34, f80
	FSUB	f88  = f35, f88
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET]
	adds	AOFFSET = -2 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f80  = f34, f80
	FSUB	f88  = f35, f88
	;;
#endif

#ifdef LN
	LDFD	f32 = [AOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	FMPY	f80  = f80,  f32
	FMPY	f88  = f88,  f32
	;;
	{ .mmi
	STFD	[BOFFSET]  = f64, SIZE
	adds	C1 = -1 * SIZE, C1
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f72, SIZE
	adds	C2 = -1 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f80, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f88,  - 3 * SIZE
	}
	;;
	adds	C3 = -1 * SIZE, C3
	adds	C4 = -1 * SIZE, C4
	;;
#endif

#ifdef LT
	LDFD	f32 = [AOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	FMPY	f80  = f80,  f32
	FMPY	f88  = f88,  f32
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	;;
	STFD	[BOFFSET]  = f88, -3 * SIZE
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f39, f40 = [BOFFSET]
	adds	BOFFSET = 5 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET], -15 * SIZE

	FMPY	f64  = f64,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	;;
	FNMA	f80  = f64,  f34, f80
	;;
	FNMA	f88  = f64,  f35, f88
	;;
	FMPY	f72  = f72,  f36
	;;
	FNMA	f80  = f72,  f37, f80
	;;
	FNMA	f88  = f72,  f38, f88
	;;
	FMPY	f80  = f80,  f39
	;;
	FNMA	f88  = f80,  f40, f88
	;;
	FMPY	f88  = f88,  f41
	;;
	STFD	[AOFFSET]   = f64, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	;;
	STFD	[AOFFSET] = f80, SIZE
	;;
	STFD	[AOFFSET] = f88, -3 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 14 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f35, f34 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], - 2 * SIZE
	;;
	LDFPD	f38, f37 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f40, f39 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET]
	;;
	FMPY	f88  = f88,  f32
	;;
	FNMA	f80  = f88,  f33, f80
	;;
	FNMA	f72  = f88,  f34, f72
	;;
	FNMA	f64  = f88,  f35, f64
	;;
	FMPY	f80  = f80,  f36
	;;
	FNMA	f72  = f80,  f37, f72
	;;
	FNMA	f64  = f80,  f38, f64
	;;
	FMPY	f72  = f72,  f39
	;;
	FNMA	f64  = f72,  f40, f64
	;;
	FMPY	f64  = f64,  f41
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f80, SIZE
	;;
	STFD	[AOFFSET]  = f88,  - 3 * SIZE
	;;
#endif

#ifndef LN
	STFD	[C1 ] = f64, SIZE
#else
	STFD	[C1 ] = f64
#endif
#ifndef LN
	STFD	[C2 ] = f72, SIZE
#else
	STFD	[C2 ] = f72
#endif
#ifndef LN
	STFD	[C3 ] = f80, SIZE
#else
	STFD	[C3 ] = f80
#endif
#ifndef LN
	STFD	[C4 ] = f88, SIZE
#else
	STFD	[C4 ] = f88
#endif
	;;

	mov	f64  = f0
	mov	f72  = f0
	mov	f80  = f0
	mov	f88  = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	add	AORIG = r2, AORIG
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	add	AOFFSET = L, AOFFSET
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 2, BOFFSET
#else
	nop	__LINE__
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
	.align 8

.L070:
	tbit.z	p6,p7  = M, 1
	(p6)	br.cond.dptk .L060
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	setf.d	f73  = r0
	mov	f65  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
	mov	f65  = f0
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
	shladd	AOFFSET = r3, 1, AORIG
	}
	;;
#endif
	{ .mfi
	mov	f81  = f0
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	mov	f89  = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mmf
	adds	L =  -1, L
	}
	;;
	{ .mmf
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mib
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L078
	}
	;;
	.align 8

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
	nop	__LINE__
	nop	__LINE__
	FMA	f89   = f33, f51, f89	// A2 * B4
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
	(p3) FMA	f72   = f40, f57, f72	// A1 * B2
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
	(p3) FMA	f88   = f40, f59, f88	// A1 * B4
	}
	;;
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
	(p3) FMA	f65   = f41, f56, f65	// A2 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f89   = f41, f59, f89	// A2 * B4
	br.cloop.sptk.few .L072
	}
	;;
.L078:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f80  = f34, f80
	FSUB	f88  = f35, f88
	FSUB	f65  = f36, f65
	FSUB	f73  = f37, f73
	FSUB	f81  = f38, f81
	FSUB	f89  = f39, f89
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65

	FSUB	f72  = f34, f72
	FSUB	f73  = f35, f73

	FSUB	f80  = f36, f80
	FSUB	f81  = f37, f81

	FSUB	f88  = f38, f88
	FSUB	f89  = f39, f89
	;;
#endif

#ifdef LN
	adds	AOFFSET = 2 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET]
	;;
	FMPY	f65  = f65,  f32
	FMPY	f73  = f73,  f32
	FMPY	f81  = f81,  f32
	FMPY	f89  = f89,  f32
	;;
	FNMA	f64  = f65,  f33, f64
	FNMA	f72  = f73,  f33, f72
	FNMA	f80  = f81,  f33, f80
	FNMA	f88  = f89,  f33, f88
	;;
	FMPY	f64  = f64,  f34
	FMPY	f72  = f72,  f34
	FMPY	f80  = f80,  f34
	FMPY	f88  = f88,  f34
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f65, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f73, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f81, SIZE
	;;
	STFD	[BOFFSET]  = f88, - 3 * SIZE
	STFD	[BOFFSET2] = f89, - 3 * SIZE
	;;
	adds	C1 = -2 * SIZE, C1
	adds	C2 = -2 * SIZE, C2
	adds	C3 = -2 * SIZE, C3
	adds	C4 = -2 * SIZE, C4
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET], - 3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	FMPY	f80  = f80,  f32
	FMPY	f88  = f88,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	FNMA	f73  = f72,  f33, f73
	FNMA	f81  = f80,  f33, f81
	FNMA	f89  = f88,  f33, f89
	;;
	FMPY	f65  = f65,  f34
	FMPY	f73  = f73,  f34
	FMPY	f81  = f81,  f34
	FMPY	f89  = f89,  f34
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f65, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f73, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f81, SIZE
	;;
	STFD	[BOFFSET]  = f88, -3 * SIZE
	STFD	[BOFFSET2] = f89, -3 * SIZE
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f39, f40 = [BOFFSET]
	adds	BOFFSET = 5 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET], -15 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f73  = f65,  f33, f73
	;;
	FNMA	f80  = f64,  f34, f80
	FNMA	f81  = f65,  f34, f81
	;;
	FNMA	f88  = f64,  f35, f88
	FNMA	f89  = f65,  f35, f89
	;;
	FMPY	f72  = f72,  f36
	FMPY	f73  = f73,  f36
	;;
	FNMA	f80  = f72,  f37, f80
	FNMA	f81  = f73,  f37, f81
	;;
	FNMA	f88  = f72,  f38, f88
	FNMA	f89  = f73,  f38, f89
	;;
	FMPY	f80  = f80,  f39
	FMPY	f81  = f81,  f39
	;;
	FNMA	f88  = f80,  f40, f88
	FNMA	f89  = f81,  f40, f89
	;;
	FMPY	f88  = f88,  f41
	FMPY	f89  = f89,  f41
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f80, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2]  = f81, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2]  = f88, SIZE
	;;
	STFD	[AOFFSET]  = f73, -3 * SIZE
	STFD	[AOFFSET2] = f89, -3 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 14 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f35, f34 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], - 2 * SIZE
	;;
	LDFPD	f38, f37 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f40, f39 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET]
	;;
	FMPY	f88  = f88,  f32
	FMPY	f89  = f89,  f32
	;;
	FNMA	f80  = f88,  f33, f80
	FNMA	f81  = f89,  f33, f81
	;;
	FNMA	f72  = f88,  f34, f72
	FNMA	f73  = f89,  f34, f73
	;;
	FNMA	f64  = f88,  f35, f64
	FNMA	f65  = f89,  f35, f65
	;;
	FMPY	f80  = f80,  f36
	FMPY	f81  = f81,  f36
	;;
	FNMA	f72  = f80,  f37, f72
	FNMA	f73  = f81,  f37, f73
	;;
	FNMA	f64  = f80,  f38, f64
 	FNMA	f65  = f81,  f38, f65
	;;
	FMPY	f72  = f72,  f39
	FMPY	f73  = f73,  f39
	;;
	FNMA	f64  = f72,  f40, f64
	FNMA	f65  = f73,  f40, f65
	;;
	FMPY	f64  = f64,  f41
	FMPY	f65  = f65,  f41
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f65, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2]  = f73, SIZE
	;;
	STFD	[AOFFSET]  = f80, SIZE
	STFD	[AOFFSET2]  = f81, SIZE
	;;
	STFD	[AOFFSET]  = f88, -3 * SIZE
	STFD	[AOFFSET2] = f89, -3 * SIZE
	;;
#endif
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	;;
#ifndef LN
	STFD	[C1 ] = f65, SIZE
#else
	STFD	[C1 ] = f65, -SIZE
#endif
	;;
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	;;
#ifndef LN
	STFD	[C2 ] = f73, SIZE
#else
	STFD	[C2 ] = f73, -SIZE
#endif
	;;
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
	;;
#ifndef LN
	STFD	[C3 ] = f81, SIZE
#else
	STFD	[C3 ] = f81, - SIZE
#endif
	;;
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
	;;
#ifndef LN
	STFD	[C4 ] = f89, SIZE
#else
	STFD	[C4 ] = f89, -SIZE
#endif
	;;
	mov	f96  = f0
	;;
	mov	f104 = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#else
	nop	__LINE__
#endif
	;;
	mov	f112 = f0
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
	mov	f120 = f0
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L060:





	tbit.z	p6, p7  = M, 2
	(p6)	br.cond.dptk .L051
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 2 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	shladd	AOFFSET = r3, 2, AORIG
	}
	;;
#endif
	{ .mfi
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	L =  -1, L
	}
	;;
	{ .mfi
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mmf
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	mov	ar.lc = L
	}
	;;
	mov	f64  = f0
	mov	f65  = f0
	mov	f66  = f0
	mov	f67  = f0
	mov	f72  = f0
	mov	f73  = f0
	mov	f74  = f0
	mov	f75  = f0
	mov	f80  = f0
	mov	f81  = f0
	mov	f82  = f0
	mov	f83  = f0
	mov	f88  = f0
	mov	f89  = f0
	mov	f90  = f0
	mov	f91  = f0
	;;
	{ .mmf
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	}
	{ .mfb
	(p6) br.cond.dpnt   .L068
	}
	;;
	.align 8

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
	nop	__LINE__
	FMA	f75   = f35, f49, f75	// A4 * B2
	nop	__LINE__
	}
	{ .mfb
	(p4) LDFPD	f48, f49 = [BOFFSET],   2 * SIZE
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
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
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
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
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f83   = f43, f58, f83	// A4 * B3
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	br.cloop.sptk.few .L062
	}
	;;
	.align 8

.L068:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -4, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 2, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [BOFFSET]
	adds	BOFFSET = -14 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f80  = f34, f80
	FSUB	f88  = f35, f88
	;;
	FSUB	f65  = f36, f65
	FSUB	f73  = f37, f73
	FSUB	f81  = f38, f81
	FSUB	f89  = f39, f89
	;;
	FSUB	f66  = f40, f66
	FSUB	f74  = f41, f74
	FSUB	f82  = f42, f82
	FSUB	f90  = f43, f90
	;;
	FSUB	f67  = f44, f67
	FSUB	f75  = f45, f75
	FSUB	f83  = f46, f83
	FSUB	f91  = f47, f91
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [AOFFSET]
	adds	AOFFSET = -14 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67

	FSUB	f72  = f36, f72
	FSUB	f73  = f37, f73
	FSUB	f74  = f38, f74
	FSUB	f75  = f39, f75

	FSUB	f80  = f40, f80
	FSUB	f81  = f41, f81
	FSUB	f82  = f42, f82
	FSUB	f83  = f43, f83

	FSUB	f88  = f44, f88
	FSUB	f89  = f45, f89
	FSUB	f90  = f46, f90
	FSUB	f91  = f47, f91
	;;
#endif

#ifdef LN
	adds	AOFFSET = 14 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f35, f34 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f36 = [AOFFSET], - 2 * SIZE
	;;
	LDFPD	f38, f37 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f40, f39 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFD	f41 = [AOFFSET]
	;;
	FMPY	f67  = f67,  f32
	FMPY	f75  = f75,  f32
	FMPY	f83  = f83,  f32
	FMPY	f91  = f91,  f32
	;;
	FNMA	f66  = f67,  f33, f66
	FNMA	f74  = f75,  f33, f74
	FNMA	f82  = f83,  f33, f82
	FNMA	f90  = f91,  f33, f90
	;;
	FNMA	f65  = f67,  f34, f65
	FNMA	f73  = f75,  f34, f73
	FNMA	f81  = f83,  f34, f81
	FNMA	f89  = f91,  f34, f89
	;;
	FNMA	f64  = f67,  f35, f64
	FNMA	f72  = f75,  f35, f72
	FNMA	f80  = f83,  f35, f80
	FNMA	f88  = f91,  f35, f88
	;;
	FMPY	f66  = f66,  f36
	FMPY	f74  = f74,  f36
	FMPY	f82  = f82,  f36
	FMPY	f90  = f90,  f36
	;;
	FNMA	f65  = f66,  f37, f65
	FNMA	f73  = f74,  f37, f73
 	FNMA	f81  = f82,  f37, f81
	FNMA	f89  = f90,  f37, f89
	;;
	FNMA	f64  = f66,  f38, f64
	FNMA	f72  = f74,  f38, f72
	FNMA	f80  = f82,  f38, f80
	FNMA	f88  = f90,  f38, f88
	;;
	FMPY	f65  = f65,  f39
	FMPY	f73  = f73,  f39
	FMPY	f81  = f81,  f39
	FMPY	f89  = f89,  f39
	;;
	FNMA	f64  = f65,  f40, f64
	FNMA	f72  = f73,  f40, f72
	FNMA	f80  = f81,  f40, f80
	FNMA	f88  = f89,  f40, f88
	;;
	FMPY	f64  = f64,  f41
	FMPY	f72  = f72,  f41
	FMPY	f80  = f80,  f41
	FMPY	f88  = f88,  f41
	;;
	adds	BOFFSET  =  8 * SIZE, BOFFSET
	adds	BOFFSET2 =  8 * SIZE, BOFFSET2
	;;
	STFD	[BOFFSET]  = f66, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f74, SIZE
	STFD	[BOFFSET2] = f75, SIZE
	;;
	STFD	[BOFFSET]  = f82, SIZE
	STFD	[BOFFSET2] = f83, SIZE
	;;
	STFD	[BOFFSET]  = f90, - 11 * SIZE
	STFD	[BOFFSET2] = f91, - 11 * SIZE
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f65, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f73, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f81, SIZE
	;;
	STFD	[BOFFSET]  = f88, -3 * SIZE
	STFD	[BOFFSET2] = f89, -3 * SIZE
	;;
	adds	C1 = -4 * SIZE, C1
	adds	C2 = -4 * SIZE, C2
	adds	C3 = -4 * SIZE, C3
	adds	C4 = -4 * SIZE, C4
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f36 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f39, f40 = [AOFFSET]
	adds	AOFFSET = 5 * SIZE, AOFFSET
	;;
	LDFD	f41 = [AOFFSET], -15 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	FMPY	f80  = f80,  f32
	FMPY	f88  = f88,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	FNMA	f73  = f72,  f33, f73
	FNMA	f81  = f80,  f33, f81
	FNMA	f89  = f88,  f33, f89
	;;
	FNMA	f66  = f64,  f34, f66
	FNMA	f74  = f72,  f34, f74
	FNMA	f82  = f80,  f34, f82
	FNMA	f90  = f88,  f34, f90
	;;
	FNMA	f67  = f64,  f35, f67
	FNMA	f75  = f72,  f35, f75
	FNMA	f83  = f80,  f35, f83
	FNMA	f91  = f88,  f35, f91
	;;
	FMPY	f65  = f65,  f36
	FMPY	f73  = f73,  f36
	FMPY	f81  = f81,  f36
	FMPY	f89  = f89,  f36
	;;
	FNMA	f66  = f65,  f37, f66
	FNMA	f74  = f73,  f37, f74
	FNMA	f82  = f81,  f37, f82
	FNMA	f90  = f89,  f37, f90
	;;
	FNMA	f67  = f65,  f38, f67
	FNMA	f75  = f73,  f38, f75
	FNMA	f83  = f81,  f38, f83
	FNMA	f91  = f89,  f38, f91
	;;
	FMPY	f66  = f66,  f39
	FMPY	f74  = f74,  f39
	FMPY	f82  = f82,  f39
	FMPY	f90  = f90,  f39
	;;
	FNMA	f67  = f66,  f40, f67
	FNMA	f75  = f74,  f40, f75
	FNMA	f83  = f82,  f40, f83
	FNMA	f91  = f90,  f40, f91
	;;
	FMPY	f67  = f67,  f41
	FMPY	f75  = f75,  f41
	FMPY	f83  = f83,  f41
	FMPY	f91  = f91,  f41
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f65, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f73, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f81, SIZE
	;;
	STFD	[BOFFSET]  = f88, 5 * SIZE
	STFD	[BOFFSET2] = f89, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f74, SIZE
	STFD	[BOFFSET2] = f75, SIZE
	;;
	STFD	[BOFFSET]  = f82, SIZE
	STFD	[BOFFSET2] = f83, SIZE
	;;
	STFD	[BOFFSET]  = f90, -11 * SIZE
	STFD	[BOFFSET2] = f91, -11 * SIZE
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f39, f40 = [BOFFSET]
	adds	BOFFSET = 5 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET], -15 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	FMPY	f66  = f66,  f32
	FMPY	f67  = f67,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f73  = f65,  f33, f73
	FNMA	f74  = f66,  f33, f74
	FNMA	f75  = f67,  f33, f75
	;;
	FNMA	f80  = f64,  f34, f80
	FNMA	f81  = f65,  f34, f81
	FNMA	f82  = f66,  f34, f82
	FNMA	f83  = f67,  f34, f83
	;;
	FNMA	f88  = f64,  f35, f88
	FNMA	f89  = f65,  f35, f89
	FNMA	f90  = f66,  f35, f90
	FNMA	f91  = f67,  f35, f91
	;;
	FMPY	f72  = f72,  f36
	FMPY	f73  = f73,  f36
	FMPY	f74  = f74,  f36
	FMPY	f75  = f75,  f36
	;;
	FNMA	f80  = f72,  f37, f80
	FNMA	f81  = f73,  f37, f81
	FNMA	f82  = f74,  f37, f82
	FNMA	f83  = f75,  f37, f83
	;;
	FNMA	f88  = f72,  f38, f88
	FNMA	f89  = f73,  f38, f89
	FNMA	f90  = f74,  f38, f90
	FNMA	f91  = f75,  f38, f91
	;;
	FMPY	f80  = f80,  f39
	FMPY	f81  = f81,  f39
	FMPY	f82  = f82,  f39
	FMPY	f83  = f83,  f39
	;;
	FNMA	f88  = f80,  f40, f88
	FNMA	f89  = f81,  f40, f89
	FNMA	f90  = f82,  f40, f90
	FNMA	f91  = f83,  f40, f91
	;;
	FMPY	f88  = f88,  f41
	FMPY	f89  = f89,  f41
	FMPY	f90  = f90,  f41
	FMPY	f91  = f91,  f41
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2]  = f73, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2]  = f74, SIZE
	;;
	STFD	[AOFFSET]  = f67, 5 * SIZE
	STFD	[AOFFSET2]  = f75, 5 * SIZE
	;;
	STFD	[AOFFSET]  = f80, SIZE
	STFD	[AOFFSET2] = f88, SIZE
	;;
	STFD	[AOFFSET]  = f81, SIZE
	STFD	[AOFFSET2] = f89, SIZE
	;;
	STFD	[AOFFSET]  = f82, SIZE
	STFD	[AOFFSET2] = f90, SIZE
	;;
	STFD	[AOFFSET]  = f83, -11 * SIZE
	STFD	[AOFFSET2] = f91, -11 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 14 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f35, f34 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], - 2 * SIZE
	;;
	LDFPD	f38, f37 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f40, f39 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET]
	;;
	FMPY	f88  = f88,  f32
	FMPY	f89  = f89,  f32
	FMPY	f90  = f90,  f32
	FMPY	f91  = f91,  f32
	;;
	FNMA	f80  = f88,  f33, f80
	FNMA	f81  = f89,  f33, f81
	FNMA	f82  = f90,  f33, f82
	FNMA	f83  = f91,  f33, f83
	;;
	FNMA	f72  = f88,  f34, f72
	FNMA	f73  = f89,  f34, f73
	FNMA	f74  = f90,  f34, f74
	FNMA	f75  = f91,  f34, f75
	;;
	FNMA	f64  = f88,  f35, f64
	FNMA	f65  = f89,  f35, f65
	FNMA	f66  = f90,  f35, f66
	FNMA	f67  = f91,  f35, f67
	;;
	FMPY	f80  = f80,  f36
	FMPY	f81  = f81,  f36
	FMPY	f82  = f82,  f36
	FMPY	f83  = f83,  f36
	;;
	FNMA	f72  = f80,  f37, f72
	FNMA	f73  = f81,  f37, f73
	FNMA	f74  = f82,  f37, f74
	FNMA	f75  = f83,  f37, f75
	;;
	FNMA	f64  = f80,  f38, f64
 	FNMA	f65  = f81,  f38, f65
	FNMA	f66  = f82,  f38, f66
	FNMA	f67  = f83,  f38, f67
	;;
	FMPY	f72  = f72,  f39
	FMPY	f73  = f73,  f39
	FMPY	f74  = f74,  f39
	FMPY	f75  = f75,  f39
	;;
	FNMA	f64  = f72,  f40, f64
	FNMA	f65  = f73,  f40, f65
	FNMA	f66  = f74,  f40, f66
	FNMA	f67  = f75,  f40, f67
	;;
	FMPY	f64  = f64,  f41
	FMPY	f65  = f65,  f41
	FMPY	f66  = f66,  f41
	FMPY	f67  = f67,  f41
	;;
	adds	AOFFSET  = 8 * SIZE, AOFFSET
	adds	AOFFSET2 = 8 * SIZE, AOFFSET2
	;;
	STFD	[AOFFSET]  = f80, SIZE
	STFD	[AOFFSET2] = f88, SIZE
	;;
	STFD	[AOFFSET]  = f81, SIZE
	STFD	[AOFFSET2] = f89, SIZE
	;;
	STFD	[AOFFSET]  = f82, SIZE
	STFD	[AOFFSET2] = f90, SIZE
	;;
	STFD	[AOFFSET]  = f83, - 11 * SIZE
	STFD	[AOFFSET2] = f91,  - 11 * SIZE
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2]  = f73, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2]  = f74, SIZE
	;;
	STFD	[AOFFSET]  = f67, - 3 * SIZE
	STFD	[AOFFSET2]  = f75, - 3 * SIZE
	;;
#endif
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C1 ] = f67, SIZE
#else
	STFD	[C1 ] = f67, - 3 * SIZE
#endif
	}
	;;
	{ .mmf
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmi
	STFD	[C2 ] = f73, SIZE
	}
	;;
	{ .mmi
	STFD	[C2 ] = f74, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C2 ] = f75, SIZE
#else
	STFD	[C2 ] = f75, - 3 * SIZE
#endif
	}
	;;
	{ .mmf
	STFD	[C3 ] = f80, SIZE
	mov	f80  = f0
	}
	;;
	{ .mmi
	STFD	[C3 ] = f81, SIZE
	}
	;;
	{ .mmi
	STFD	[C3 ] = f82, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C3 ] = f83, SIZE
#else
	STFD	[C3 ] = f83, - 3 * SIZE
#endif
	}
	;;
	{ .mmf
	STFD	[C4 ] = f88, SIZE
	mov	f88  = f0
	}
	;;
	{ .mmi
	STFD	[C4 ] = f89, SIZE
	}
	;;
	{ .mmi
	STFD	[C4 ] = f90, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C4 ] = f91, SIZE
#else
	STFD	[C4 ] = f91, - 3 * SIZE
#endif
	nop	__LINE__
	}
	;;
	mov	f65  = f0
	;;
	mov	f73 = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	{ .mmi
	sub	L = K, KK
	}
	;;
	{ .mmi
#ifdef RT
	shladd	AORIG = r2, 2, AORIG
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
	mov	f81 = f0
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
	mov	f89 = f0
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  4, KK
#elif defined LN
	adds	KK = -4, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L051:
	mov	f72  = f0
	mov	f80  = f0
	mov	f88  = f0
	mov	f65  = f0
	mov	f73  = f0
	mov	f81  = f0
	mov	f89  = f0

	shr	I  = M, 3
	;;
	cmp.eq	p6, p7 = 0, I
	(p6)	br.cond.dpnt .L089
	;;
	.align 16

.L052:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 3 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = r3, 2, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	shladd	AOFFSET = r3, 3, AORIG
	}
	;;
#endif
	{ .mfi
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	f66  = f0
	nop	__LINE__
	}
	{ .mfi
	(p7) LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	mov	f74  = f0
	nop	__LINE__
	}
	;;
	{ .mmf
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	setf.d	f82  = r0
	mov	f90  = f0
	}
	;;
	{ .mmf
	(p7) LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	setf.d	f67  = r0
	mov	f75  = f0
	}
	{ .mfi
	setf.d	f83  = r0
	mov	f91  = f0
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mmf
	(p7) LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
	}
	{ .mfi
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
	adds	L =  1, L
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	}
	{ .mfi
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
	CPREFETCH [PREC]
	}
	;;
	{ .mfi
	setf.d	f70  = r0
	mov	f78  = f0
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	setf.d	f86  = r0
	mov	f94  = f0
	shr	L = L, 1
	}
	;;
	{ .mfi
	setf.d	f71  = r0
	adds	L =  -1, L
	}
	;;
	{ .mfi
	setf.d	f87  = r0
	mov	f79  = f0
	mov	ar.lc = L
	}
	{ .mfb
	cmp.eq  p6, p0 = -1, L
	mov	f95  = f0
	(p6) br.cond.dpnt   .L058
	}
	;;
	.align 8

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
	nop	__LINE__
	(p3) FMA	f66   = f42, f56, f66	// A3 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f74   = f42, f57, f74	// A3 * B2
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
	(p3) FMA	f90   = f42, f59, f90	// A3 * B4
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
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
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
	(p3) FMA	f91   = f43, f59, f91	// A4 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f84   = f44, f58, f84	// A5 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f92   = f44, f59, f92	// A5 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f85   = f45, f58, f85	// A6 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f93   = f45, f59, f93	// A6 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f86   = f46, f58, f86	// A7 * B3
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f94   = f46, f59, f94	// A7 * B4
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f87   = f47, f58, f87	// A8 * B3
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f95   = f47, f59, f95	// A8 * B4
	br.cloop.sptk.few .L053
	}
	;;
	.align 8

.L058:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -8, KK
#else
	adds	r2 = -4, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 3, AORIG
	shladd	BOFFSET = r2, 2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f50, f51 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f52, f53 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f54, f55 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f58, f59 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f62, f63 = [BOFFSET]
	adds	BOFFSET = -30 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f80  = f34, f80
	FSUB	f88  = f35, f88

	FSUB	f65  = f36, f65
	FSUB	f73  = f37, f73
	FSUB	f81  = f38, f81
	FSUB	f89  = f39, f89

	FSUB	f66  = f40, f66
	FSUB	f74  = f41, f74
	FSUB	f82  = f42, f82
	FSUB	f90  = f43, f90

	FSUB	f67  = f44, f67
	FSUB	f75  = f45, f75
	FSUB	f83  = f46, f83
	FSUB	f91  = f47, f91

	FSUB	f68  = f48, f68
	FSUB	f76  = f49, f76
	FSUB	f84  = f50, f84
	FSUB	f92  = f51, f92

	FSUB	f69  = f52, f69
	FSUB	f77  = f53, f77
	FSUB	f85  = f54, f85
	FSUB	f93  = f55, f93

	FSUB	f70  = f56, f70
	FSUB	f78  = f57, f78
	FSUB	f86  = f58, f86
	FSUB	f94  = f59, f94

	FSUB	f71  = f60, f71
	FSUB	f79  = f61, f79
	FSUB	f87  = f62, f87
	FSUB	f95  = f63, f95
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f48, f49 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f50, f51 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f52, f53 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f54, f55 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f58, f59 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f62, f63 = [AOFFSET]
	adds	AOFFSET = -30 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67
	FSUB	f68  = f36, f68
	FSUB	f69  = f37, f69
	FSUB	f70  = f38, f70
	FSUB	f71  = f39, f71
	;;
	FSUB	f72  = f40, f72
	FSUB	f73  = f41, f73
	FSUB	f74  = f42, f74
	FSUB	f75  = f43, f75
	FSUB	f76  = f44, f76
	FSUB	f77  = f45, f77
	FSUB	f78  = f46, f78
	FSUB	f79  = f47, f79
	;;
	FSUB	f80  = f48, f80
	FSUB	f81  = f49, f81
	FSUB	f82  = f50, f82
	FSUB	f83  = f51, f83
	FSUB	f84  = f52, f84
	FSUB	f85  = f53, f85
	FSUB	f86  = f54, f86
	FSUB	f87  = f55, f87

	FSUB	f88  = f56, f88
	FSUB	f89  = f57, f89
	FSUB	f90  = f58, f90
	FSUB	f91  = f59, f91
	FSUB	f92  = f60, f92
	FSUB	f93  = f61, f93
	FSUB	f94  = f62, f94
	FSUB	f95  = f63, f95
	;;
#endif

#ifdef LN
	adds	AOFFSET = 62 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f35, f34 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f37, f36 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f39, f38 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f40 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f42, f41 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f44, f43 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f46, f45 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f48, f47 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f50, f49 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f52, f51 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFD	f53 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f55, f54 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f57, f56 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFPD	f59, f58 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f61, f60 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFD	f16 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f18, f17 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFPD	f20, f19 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFD	f21 = [AOFFSET]
	;;
	FMPY	f71  = f71,  f32
	FMPY	f79  = f79,  f32
	FMPY	f87  = f87,  f32
	FMPY	f95  = f95,  f32
	;;
	FNMA	f70  = f71,  f33, f70
	FNMA	f78  = f79,  f33, f78
	FNMA	f86  = f87,  f33, f86
	FNMA	f94  = f95,  f33, f94
	;;
	FNMA	f69  = f71,  f34, f69
	FNMA	f77  = f79,  f34, f77
	FNMA	f85  = f87,  f34, f85
	FNMA	f93  = f95,  f34, f93
	;;
	FNMA	f68  = f71,  f35, f68
	FNMA	f76  = f79,  f35, f76
	FNMA	f84  = f87,  f35, f84
	FNMA	f92  = f95,  f35, f92
	;;
	FNMA	f67  = f71,  f36, f67
	FNMA	f75  = f79,  f36, f75
	FNMA	f83  = f87,  f36, f83
	FNMA	f91  = f95,  f36, f91
	;;
	FNMA	f66  = f71,  f37, f66
	FNMA	f74  = f79,  f37, f74
	FNMA	f82  = f87,  f37, f82
	FNMA	f90  = f95,  f37, f90
	;;
	FNMA	f65  = f71,  f38, f65
	FNMA	f73  = f79,  f38, f73
	FNMA	f81  = f87,  f38, f81
	FNMA	f89  = f95,  f38, f89
	;;
	FNMA	f64  = f71,  f39, f64
	FNMA	f72  = f79,  f39, f72
	FNMA	f80  = f87,  f39, f80
	FNMA	f88  = f95,  f39, f88
	;;
	FMPY	f70  = f70,  f40
	FMPY	f78  = f78,  f40
	FMPY	f86  = f86,  f40
	FMPY	f94  = f94,  f40
	;;
	FNMA	f69  = f70,  f41, f69
	FNMA	f77  = f78,  f41, f77
	FNMA	f85  = f86,  f41, f85
	FNMA	f93  = f94,  f41, f93
	;;
	FNMA	f68  = f70,  f42, f68
	FNMA	f76  = f78,  f42, f76
	FNMA	f84  = f86,  f42, f84
	FNMA	f92  = f94,  f42, f92
	;;
	FNMA	f67  = f70,  f43, f67
	FNMA	f75  = f78,  f43, f75
	FNMA	f83  = f86,  f43, f83
	FNMA	f91  = f94,  f43, f91
	;;
	FNMA	f66  = f70,  f44, f66
	FNMA	f74  = f78,  f44, f74
	FNMA	f82  = f86,  f44, f82
	FNMA	f90  = f94,  f44, f90
	;;
	FNMA	f65  = f70,  f45, f65
	FNMA	f73  = f78,  f45, f73
	FNMA	f81  = f86,  f45, f81
	FNMA	f89  = f94,  f45, f89
	;;
	FNMA	f64  = f70,  f46, f64
	FNMA	f72  = f78,  f46, f72
	FNMA	f80  = f86,  f46, f80
	FNMA	f88  = f94,  f46, f88
	;;
	FMPY	f69  = f69,  f47
	FMPY	f77  = f77,  f47
	FMPY	f85  = f85,  f47
	FMPY	f93  = f93,  f47
	;;
	FNMA	f68  = f69,  f48, f68
	FNMA	f76  = f77,  f48, f76
	FNMA	f84  = f85,  f48, f84
	FNMA	f92  = f93,  f48, f92
	;;
	FNMA	f67  = f69,  f49, f67
	FNMA	f75  = f77,  f49, f75
	FNMA	f83  = f85,  f49, f83
	FNMA	f91  = f93,  f49, f91
	;;
	FNMA	f66  = f69,  f50, f66
	FNMA	f74  = f77,  f50, f74
	FNMA	f82  = f85,  f50, f82
	FNMA	f90  = f93,  f50, f90
	;;
	FNMA	f65  = f69,  f51, f65
	FNMA	f73  = f77,  f51, f73
	FNMA	f81  = f85,  f51, f81
	FNMA	f89  = f93,  f51, f89
	;;
	FNMA	f64  = f69,  f52, f64
	FNMA	f72  = f77,  f52, f72
	FNMA	f80  = f85,  f52, f80
	FNMA	f88  = f93,  f52, f88
	;;
	FMPY	f68  = f68,  f53
	FMPY	f76  = f76,  f53
	FMPY	f84  = f84,  f53
	FMPY	f92  = f92,  f53
	;;
	FNMA	f67  = f68,  f54, f67
	FNMA	f75  = f76,  f54, f75
	FNMA	f83  = f84,  f54, f83
	FNMA	f91  = f92,  f54, f91
	;;
	FNMA	f66  = f68,  f55, f66
	FNMA	f74  = f76,  f55, f74
	FNMA	f82  = f84,  f55, f82
	FNMA	f90  = f92,  f55, f90
	;;
	FNMA	f65  = f68,  f56, f65
	FNMA	f73  = f76,  f56, f73
	FNMA	f81  = f84,  f56, f81
	FNMA	f89  = f92,  f56, f89
	;;
	FNMA	f64  = f68,  f57, f64
	FNMA	f72  = f76,  f57, f72
	FNMA	f80  = f84,  f57, f80
	FNMA	f88  = f92,  f57, f88
	;;
	FMPY	f67  = f67,  f58
	FMPY	f75  = f75,  f58
	FMPY	f83  = f83,  f58
	FMPY	f91  = f91,  f58
	;;
	FNMA	f66  = f67,  f59, f66
	FNMA	f74  = f75,  f59, f74
	FNMA	f82  = f83,  f59, f82
	FNMA	f90  = f91,  f59, f90
	;;
	FNMA	f65  = f67,  f60, f65
	FNMA	f73  = f75,  f60, f73
	FNMA	f81  = f83,  f60, f81
	FNMA	f89  = f91,  f60, f89
	;;
	FNMA	f64  = f67,  f61, f64
	FNMA	f72  = f75,  f61, f72
	FNMA	f80  = f83,  f61, f80
	FNMA	f88  = f91,  f61, f88
	;;
	FMPY	f66  = f66,  f16
	FMPY	f74  = f74,  f16
	FMPY	f82  = f82,  f16
	FMPY	f90  = f90,  f16
	;;
	FNMA	f65  = f66,  f17, f65
	FNMA	f73  = f74,  f17, f73
	FNMA	f81  = f82,  f17, f81
	FNMA	f89  = f90,  f17, f89
	;;
	FNMA	f64  = f66,  f18, f64
	FNMA	f72  = f74,  f18, f72
	FNMA	f80  = f82,  f18, f80
	FNMA	f88  = f90,  f18, f88
	;;
	FMPY	f65  = f65,  f19
	FMPY	f73  = f73,  f19
	FMPY	f81  = f81,  f19
	FMPY	f89  = f89,  f19
	;;
	FNMA	f64  = f65,  f20, f64
	FNMA	f72  = f73,  f20, f72
	FNMA	f80  = f81,  f20, f80
	FNMA	f88  = f89,  f20, f88
	;;
	FMPY	f64  = f64,  f21
	FMPY	f72  = f72,  f21
	FMPY	f80  = f80,  f21
	FMPY	f88  = f88,  f21
	;;

	adds	BOFFSET  = 24 * SIZE, BOFFSET
	adds	BOFFSET2 = 24 * SIZE, BOFFSET2
	;;
	STFD	[BOFFSET]  = f70, SIZE
	STFD	[BOFFSET2] = f71, SIZE
	;;
	STFD	[BOFFSET]  = f78, SIZE
	STFD	[BOFFSET2] = f79, SIZE
	;;
	STFD	[BOFFSET]  = f86, SIZE
	STFD	[BOFFSET2] = f87, SIZE
	;;
	STFD	[BOFFSET]  = f94, - 11 * SIZE
	STFD	[BOFFSET2] = f95, - 11 * SIZE
	;;
	STFD	[BOFFSET]  = f68, SIZE
	STFD	[BOFFSET2] = f69, SIZE
	;;
	STFD	[BOFFSET]  = f76, SIZE
	STFD	[BOFFSET2] = f77, SIZE
	;;
	STFD	[BOFFSET]  = f84, SIZE
	STFD	[BOFFSET2] = f85, SIZE
	;;
	STFD	[BOFFSET]  = f92, - 11 * SIZE
	STFD	[BOFFSET2] = f93, - 11 * SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f74, SIZE
	STFD	[BOFFSET2] = f75, SIZE
	;;
	STFD	[BOFFSET]  = f82, SIZE
	STFD	[BOFFSET2] = f83, SIZE
	;;
	STFD	[BOFFSET]  = f90, - 11 * SIZE
	STFD	[BOFFSET2] = f91, - 11 * SIZE
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f65, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f73, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f81, SIZE
	;;
	STFD	[BOFFSET]  = f88,  - 3 * SIZE
	STFD	[BOFFSET2] = f89,  - 3 * SIZE
	;;
	adds	C1 = -8 * SIZE, C1
	adds	C2 = -8 * SIZE, C2
	adds	C3 = -8 * SIZE, C3
	adds	C4 = -8 * SIZE, C4
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f40 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f41, f42 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f43, f44 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f45, f46 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f47, f48 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f49, f50 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f51, f52 = [AOFFSET]
	adds	AOFFSET = 5 * SIZE, AOFFSET
	;;
	LDFD	f53 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f54, f55 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [AOFFSET]
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f58, f59 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [AOFFSET]
	adds	AOFFSET = 7 * SIZE, AOFFSET
	;;
	LDFD	f16 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f17, f18 = [AOFFSET]
	adds	AOFFSET = 8 * SIZE, AOFFSET
	;;
	LDFPD	f19, f20 = [AOFFSET]
	adds	AOFFSET = 9 * SIZE, AOFFSET
	;;
	LDFD	f21 = [AOFFSET]
	adds	AOFFSET = -63 * SIZE, AOFFSET
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	FMPY	f80  = f80,  f32
	FMPY	f88  = f88,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	FNMA	f73  = f72,  f33, f73
	FNMA	f81  = f80,  f33, f81
	FNMA	f89  = f88,  f33, f89
	;;
	FNMA	f66  = f64,  f34, f66
	FNMA	f74  = f72,  f34, f74
	FNMA	f82  = f80,  f34, f82
	FNMA	f90  = f88,  f34, f90
	;;
	FNMA	f67  = f64,  f35, f67
	FNMA	f75  = f72,  f35, f75
	FNMA	f83  = f80,  f35, f83
	FNMA	f91  = f88,  f35, f91
	;;
	FNMA	f68  = f64,  f36, f68
	FNMA	f76  = f72,  f36, f76
	FNMA	f84  = f80,  f36, f84
	FNMA	f92  = f88,  f36, f92
	;;
	FNMA	f69  = f64,  f37, f69
	FNMA	f77  = f72,  f37, f77
	FNMA	f85  = f80,  f37, f85
	FNMA	f93  = f88,  f37, f93
	;;
	FNMA	f70  = f64,  f38, f70
	FNMA	f78  = f72,  f38, f78
	FNMA	f86  = f80,  f38, f86
	FNMA	f94  = f88,  f38, f94
	;;
	FNMA	f71  = f64,  f39, f71
	FNMA	f79  = f72,  f39, f79
	FNMA	f87  = f80,  f39, f87
	FNMA	f95  = f88,  f39, f95
	;;
	FMPY	f65  = f65,  f40
	FMPY	f73  = f73,  f40
	FMPY	f81  = f81,  f40
	FMPY	f89  = f89,  f40
	;;
	FNMA	f66  = f65,  f41, f66
	FNMA	f74  = f73,  f41, f74
	FNMA	f82  = f81,  f41, f82
	FNMA	f90  = f89,  f41, f90
	;;
	FNMA	f67  = f65,  f42, f67
	FNMA	f75  = f73,  f42, f75
	FNMA	f83  = f81,  f42, f83
	FNMA	f91  = f89,  f42, f91
	;;
	FNMA	f68  = f65,  f43, f68
	FNMA	f76  = f73,  f43, f76
	FNMA	f84  = f81,  f43, f84
	FNMA	f92  = f89,  f43, f92
	;;
	FNMA	f69  = f65,  f44, f69
	FNMA	f77  = f73,  f44, f77
	FNMA	f85  = f81,  f44, f85
	FNMA	f93  = f89,  f44, f93
	;;
	FNMA	f70  = f65,  f45, f70
	FNMA	f78  = f73,  f45, f78
	FNMA	f86  = f81,  f45, f86
	FNMA	f94  = f89,  f45, f94
	;;
	FNMA	f71  = f65,  f46, f71
	FNMA	f79  = f73,  f46, f79
	FNMA	f87  = f81,  f46, f87
	FNMA	f95  = f89,  f46, f95
	;;
	FMPY	f66  = f66,  f47
	FMPY	f74  = f74,  f47
	FMPY	f82  = f82,  f47
	FMPY	f90  = f90,  f47
	;;
	FNMA	f67  = f66,  f48, f67
	FNMA	f75  = f74,  f48, f75
	FNMA	f83  = f82,  f48, f83
	FNMA	f91  = f90,  f48, f91
	;;
	FNMA	f68  = f66,  f49, f68
	FNMA	f76  = f74,  f49, f76
	FNMA	f84  = f82,  f49, f84
	FNMA	f92  = f90,  f49, f92
	;;
	FNMA	f69  = f66,  f50, f69
	FNMA	f77  = f74,  f50, f77
	FNMA	f85  = f82,  f50, f85
	FNMA	f93  = f90,  f50, f93
	;;
	FNMA	f70  = f66,  f51, f70
	FNMA	f78  = f74,  f51, f78
	FNMA	f86  = f82,  f51, f86
	FNMA	f94  = f90,  f51, f94
	;;
	FNMA	f71  = f66,  f52, f71
	FNMA	f79  = f74,  f52, f79
	FNMA	f87  = f82,  f52, f87
	FNMA	f95  = f90,  f52, f95
	;;
	FMPY	f67  = f67,  f53
	FMPY	f75  = f75,  f53
	FMPY	f83  = f83,  f53
	FMPY	f91  = f91,  f53
	;;
	FNMA	f68  = f67,  f54, f68
	FNMA	f76  = f75,  f54, f76
	FNMA	f84  = f83,  f54, f84
	FNMA	f92  = f91,  f54, f92
	;;
	FNMA	f69  = f67,  f55, f69
	FNMA	f77  = f75,  f55, f77
	FNMA	f85  = f83,  f55, f85
	FNMA	f93  = f91,  f55, f93
	;;
	FNMA	f70  = f67,  f56, f70
	FNMA	f78  = f75,  f56, f78
	FNMA	f86  = f83,  f56, f86
	FNMA	f94  = f91,  f56, f94
	;;
	FNMA	f71  = f67,  f57, f71
	FNMA	f79  = f75,  f57, f79
	FNMA	f87  = f83,  f57, f87
	FNMA	f95  = f91,  f57, f95
	;;
	FMPY	f68  = f68,  f58
	FMPY	f76  = f76,  f58
	FMPY	f84  = f84,  f58
	FMPY	f92  = f92,  f58
	;;
	FNMA	f69  = f68,  f59, f69
	FNMA	f77  = f76,  f59, f77
	FNMA	f85  = f84,  f59, f85
	FNMA	f93  = f92,  f59, f93
	;;
	FNMA	f70  = f68,  f60, f70
	FNMA	f78  = f76,  f60, f78
	FNMA	f86  = f84,  f60, f86
	FNMA	f94  = f92,  f60, f94
	;;
	FNMA	f71  = f68,  f61, f71
	FNMA	f79  = f76,  f61, f79
	FNMA	f87  = f84,  f61, f87
	FNMA	f95  = f92,  f61, f95
	;;
	FMPY	f69  = f69,  f16
	FMPY	f77  = f77,  f16
	FMPY	f85  = f85,  f16
	FMPY	f93  = f93,  f16
	;;
	FNMA	f70  = f69,  f17, f70
	FNMA	f78  = f77,  f17, f78
	FNMA	f86  = f85,  f17, f86
	FNMA	f94  = f93,  f17, f94
	;;
	FNMA	f71  = f69,  f18, f71
	FNMA	f79  = f77,  f18, f79
	FNMA	f87  = f85,  f18, f87
	FNMA	f95  = f93,  f18, f95
	;;
	FMPY	f70  = f70,  f19
	FMPY	f78  = f78,  f19
	FMPY	f86  = f86,  f19
	FMPY	f94  = f94,  f19
	;;
	FNMA	f71  = f70,  f20, f71
	FNMA	f79  = f78,  f20, f79
	FNMA	f87  = f86,  f20, f87
	FNMA	f95  = f94,  f20, f95
	;;
	FMPY	f71  = f71,  f21
	FMPY	f79  = f79,  f21
	FMPY	f87  = f87,  f21
	FMPY	f95  = f95,  f21
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f65, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f73, SIZE
	;;
	STFD	[BOFFSET]  = f80, SIZE
	STFD	[BOFFSET2] = f81, SIZE
	;;
	STFD	[BOFFSET]  = f88, 5 * SIZE
	STFD	[BOFFSET2] = f89, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f74, SIZE
	STFD	[BOFFSET2] = f75, SIZE
	;;
	STFD	[BOFFSET]  = f82, SIZE
	STFD	[BOFFSET2] = f83, SIZE
	;;
	STFD	[BOFFSET]  = f90, 5 * SIZE
	STFD	[BOFFSET2] = f91, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f68, SIZE
	STFD	[BOFFSET2] = f69, SIZE
	;;
	STFD	[BOFFSET]  = f76, SIZE
	STFD	[BOFFSET2] = f77, SIZE
	;;
	STFD	[BOFFSET]  = f84, SIZE
	STFD	[BOFFSET2] = f85, SIZE
	;;
	STFD	[BOFFSET]  = f92, 5 * SIZE
	STFD	[BOFFSET2] = f93, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f70, SIZE
	STFD	[BOFFSET2] = f71, SIZE
	;;
	STFD	[BOFFSET]  = f78, SIZE
	STFD	[BOFFSET2] = f79, SIZE
	;;
	STFD	[BOFFSET]  = f86, SIZE
	STFD	[BOFFSET2] = f87, SIZE
	;;
	STFD	[BOFFSET]  = f94
	STFD	[BOFFSET2] = f95
	adds	C9  = 4 * SIZE, C1
	adds	BOFFSET    = - 27 * SIZE, BOFFSET
	adds	BOFFSET2   = - 27 * SIZE, BOFFSET2
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f36      = [BOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [BOFFSET]
	adds	BOFFSET = 4 * SIZE, BOFFSET
	;;
	LDFPD	f39, f40 = [BOFFSET]
	adds	BOFFSET = 5 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET], -15 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f68  = f68,  f32
	FMPY	f65  = f65,  f32
	FMPY	f69  = f69,  f32
	FMPY	f66  = f66,  f32
	FMPY	f70  = f70,  f32
	FMPY	f67  = f67,  f32
	FMPY	f71  = f71,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f76  = f68,  f33, f76
	FNMA	f73  = f65,  f33, f73
	FNMA	f77  = f69,  f33, f77
	FNMA	f74  = f66,  f33, f74
	FNMA	f78  = f70,  f33, f78
	FNMA	f75  = f67,  f33, f75
	FNMA	f79  = f71,  f33, f79
	;;
	FNMA	f80  = f64,  f34, f80
	FNMA	f84  = f68,  f34, f84
	FNMA	f81  = f65,  f34, f81
	FNMA	f85  = f69,  f34, f85
	FNMA	f82  = f66,  f34, f82
	FNMA	f86  = f70,  f34, f86
	FNMA	f83  = f67,  f34, f83
	FNMA	f87  = f71,  f34, f87
	;;
	FNMA	f88  = f64,  f35, f88
	FNMA	f92  = f68,  f35, f92
	FNMA	f89  = f65,  f35, f89
	FNMA	f93  = f69,  f35, f93
	FNMA	f90  = f66,  f35, f90
	FNMA	f94  = f70,  f35, f94
	FNMA	f91  = f67,  f35, f91
	FNMA	f95  = f71,  f35, f95
	;;
	FMPY	f72  = f72,  f36
	FMPY	f76  = f76,  f36
	FMPY	f73  = f73,  f36
	FMPY	f77  = f77,  f36
	FMPY	f74  = f74,  f36
	FMPY	f78  = f78,  f36
	FMPY	f75  = f75,  f36
	FMPY	f79  = f79,  f36
	;;
	FNMA	f80  = f72,  f37, f80
	FNMA	f84  = f76,  f37, f84
	FNMA	f81  = f73,  f37, f81
	FNMA	f85  = f77,  f37, f85
	FNMA	f82  = f74,  f37, f82
	FNMA	f86  = f78,  f37, f86
	FNMA	f83  = f75,  f37, f83
	FNMA	f87  = f79,  f37, f87
	;;
	FNMA	f88  = f72,  f38, f88
	FNMA	f92  = f76,  f38, f92
	FNMA	f89  = f73,  f38, f89
	FNMA	f93  = f77,  f38, f93
	FNMA	f90  = f74,  f38, f90
	FNMA	f94  = f78,  f38, f94
	FNMA	f91  = f75,  f38, f91
	FNMA	f95  = f79,  f38, f95
	;;
	FMPY	f80  = f80,  f39
	FMPY	f84  = f84,  f39
	FMPY	f81  = f81,  f39
	FMPY	f85  = f85,  f39
	FMPY	f82  = f82,  f39
	FMPY	f86  = f86,  f39
	FMPY	f83  = f83,  f39
	FMPY	f87  = f87,  f39
	;;
	FNMA	f88  = f80,  f40, f88
	FNMA	f92  = f84,  f40, f92
	FNMA	f89  = f81,  f40, f89
	FNMA	f93  = f85,  f40, f93
	FNMA	f90  = f82,  f40, f90
	FNMA	f94  = f86,  f40, f94
	FNMA	f91  = f83,  f40, f91
	FNMA	f95  = f87,  f40, f95
	;;
	FMPY	f88  = f88,  f41
	FMPY	f92  = f92,  f41
	FMPY	f89  = f89,  f41
	FMPY	f93  = f93,  f41
	FMPY	f90  = f90,  f41
	FMPY	f94  = f94,  f41
	FMPY	f91  = f91,  f41
	FMPY	f95  = f95,  f41
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f68, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f69, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2] = f70, SIZE
	;;
	STFD	[AOFFSET]  = f67, 5 * SIZE
	STFD	[AOFFSET2] = f71, 5 * SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f76, SIZE
	;;
	STFD	[AOFFSET]  = f73, SIZE
	STFD	[AOFFSET2] = f77, SIZE
	;;
	STFD	[AOFFSET]  = f74, SIZE
	STFD	[AOFFSET2] = f78, SIZE
	;;
	STFD	[AOFFSET]  = f75, 5 * SIZE
	STFD	[AOFFSET2] = f79, 5 * SIZE
	;;
	STFD	[AOFFSET]  = f80, SIZE
	STFD	[AOFFSET2] = f84, SIZE
	;;
	STFD	[AOFFSET]  = f81, SIZE
	STFD	[AOFFSET2] = f85, SIZE
	;;
	STFD	[AOFFSET]  = f82, SIZE
	STFD	[AOFFSET2] = f86, SIZE
	;;
	STFD	[AOFFSET]  = f83, 5 * SIZE
	STFD	[AOFFSET2] = f87, 5 * SIZE
	;;
	STFD	[AOFFSET] = f88, SIZE
	STFD	[AOFFSET2] = f92, SIZE
	;;
	STFD	[AOFFSET] = f89, SIZE
	STFD	[AOFFSET2] = f93, SIZE
	;;
	STFD	[AOFFSET] = f90, SIZE
	STFD	[AOFFSET2] = f94, SIZE
	;;
	STFD	[AOFFSET] = f91, -27 * SIZE
	STFD	[AOFFSET2] = f95, -27 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 14 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFPD	f35, f34 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f36 = [BOFFSET], -2 * SIZE
	;;
	LDFPD	f38, f37 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFPD	f40, f39 = [BOFFSET]
	adds	BOFFSET = - 4 * SIZE, BOFFSET
	;;
	LDFD	f41 = [BOFFSET]
	;;

	FMPY	f88  = f88,  f32
	FMPY	f92  = f92,  f32
	FMPY	f89  = f89,  f32
	FMPY	f93  = f93,  f32
	FMPY	f90  = f90,  f32
	FMPY	f94  = f94,  f32
	FMPY	f91  = f91,  f32
	FMPY	f95  = f95,  f32
	;;
	FNMA	f80  = f88,  f33, f80
	FNMA	f84  = f92,  f33, f84
	FNMA	f81  = f89,  f33, f81
	FNMA	f85  = f93,  f33, f85
	FNMA	f82  = f90,  f33, f82
	FNMA	f86  = f94,  f33, f86
	FNMA	f83  = f91,  f33, f83
	FNMA	f87  = f95,  f33, f87
	;;
	FNMA	f72  = f88,  f34, f72
	FNMA	f76  = f92,  f34, f76
	FNMA	f73  = f89,  f34, f73
	FNMA	f77  = f93,  f34, f77
	FNMA	f74  = f90,  f34, f74
	FNMA	f78  = f94,  f34, f78
	FNMA	f75  = f91,  f34, f75
	FNMA	f79  = f95,  f34, f79
	;;
	FNMA	f64  = f88,  f35, f64
	FNMA	f68  = f92,  f35, f68
	FNMA	f65  = f89,  f35, f65
	FNMA	f69  = f93,  f35, f69
	FNMA	f66  = f90,  f35, f66
	FNMA	f70  = f94,  f35, f70
	FNMA	f67  = f91,  f35, f67
	FNMA	f71  = f95,  f35, f71
	;;
	FMPY	f80  = f80,  f36
	FMPY	f84  = f84,  f36
	FMPY	f81  = f81,  f36
	FMPY	f85  = f85,  f36
	FMPY	f82  = f82,  f36
	FMPY	f86  = f86,  f36
	FMPY	f83  = f83,  f36
	FMPY	f87  = f87,  f36
	;;
	FNMA	f72  = f80,  f37, f72
	FNMA	f76  = f84,  f37, f76
	FNMA	f73  = f81,  f37, f73
	FNMA	f77  = f85,  f37, f77
	FNMA	f74  = f82,  f37, f74
	FNMA	f78  = f86,  f37, f78
	FNMA	f75  = f83,  f37, f75
	FNMA	f79  = f87,  f37, f79
	;;
	FNMA	f64  = f80,  f38, f64
	FNMA	f68  = f84,  f38, f68
	FNMA	f65  = f81,  f38, f65
	FNMA	f69  = f85,  f38, f69
	FNMA	f66  = f82,  f38, f66
	FNMA	f70  = f86,  f38, f70
	FNMA	f67  = f83,  f38, f67
	FNMA	f71  = f87,  f38, f71
	;;
	FMPY	f72  = f72,  f39
	FMPY	f76  = f76,  f39
	FMPY	f73  = f73,  f39
	FMPY	f77  = f77,  f39
	FMPY	f74  = f74,  f39
	FMPY	f78  = f78,  f39
	FMPY	f75  = f75,  f39
	FMPY	f79  = f79,  f39
	;;
	FNMA	f64  = f72,  f40, f64
	FNMA	f68  = f76,  f40, f68
	FNMA	f65  = f73,  f40, f65
	FNMA	f69  = f77,  f40, f69
	FNMA	f66  = f74,  f40, f66
	FNMA	f70  = f78,  f40, f70
	FNMA	f67  = f75,  f40, f67
	FNMA	f71  = f79,  f40, f71
	;;
	FMPY	f64  = f64,  f41
	FMPY	f68  = f68,  f41
	FMPY	f65  = f65,  f41
	FMPY	f69  = f69,  f41
	FMPY	f66  = f66,  f41
	FMPY	f70  = f70,  f41
	FMPY	f67  = f67,  f41
	FMPY	f71  = f71,  f41
	;;
	adds	AOFFSET  = 24 * SIZE, AOFFSET
	adds	AOFFSET2 = 24 * SIZE, AOFFSET2
	;;
	STFD	[AOFFSET] = f88, SIZE
	STFD	[AOFFSET2] = f92, SIZE
	;;
	STFD	[AOFFSET] = f89, SIZE
	STFD	[AOFFSET2] = f93, SIZE
	;;
	STFD	[AOFFSET] = f90, SIZE
	STFD	[AOFFSET2] = f94, SIZE
	;;
	STFD	[AOFFSET] = f91,  - 11 * SIZE
	STFD	[AOFFSET2] = f95, - 11 * SIZE
	;;
	STFD	[AOFFSET]  = f80, SIZE
	STFD	[AOFFSET2] = f84, SIZE
	;;
	STFD	[AOFFSET]  = f81, SIZE
	STFD	[AOFFSET2] = f85, SIZE
	;;
	STFD	[AOFFSET]  = f82, SIZE
	STFD	[AOFFSET2] = f86, SIZE
	;;
	STFD	[AOFFSET]  = f83, - 11 * SIZE
	STFD	[AOFFSET2] = f87, - 11 * SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f76, SIZE
	;;
	STFD	[AOFFSET]  = f73, SIZE
	STFD	[AOFFSET2] = f77, SIZE
	;;
	STFD	[AOFFSET]  = f74, SIZE
	STFD	[AOFFSET2] = f78, SIZE
	;;
	STFD	[AOFFSET]  = f75, - 11 * SIZE
	STFD	[AOFFSET2] = f79, - 11 * SIZE
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f68, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f69, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2] = f70, SIZE
	;;
	STFD	[AOFFSET]  = f67, - 3 * SIZE
	STFD	[AOFFSET2] = f71, - 3 * SIZE
	;;

#endif
	adds	C9  = 4 * SIZE, C1
	;;

	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C1 ] = f67, 5 * SIZE
#else
	STFD	[C1 ] = f67, - 3 * SIZE
#endif
	STFD	[C9 ] = f71
	adds	C11 = 4 * SIZE, C3
	}
	;;
	{ .mmf
	STFD	[C2 ] = f72, SIZE
	STFD	[C10] = f76, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmi
	STFD	[C2 ] = f73, SIZE
	STFD	[C10] = f77, SIZE
	}
	;;
	{ .mmi
	STFD	[C2 ] = f74, SIZE
	STFD	[C10] = f78, SIZE
	adds	C12 = 4 * SIZE, C4
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C2 ] = f75, 5 * SIZE
#else
	STFD	[C2 ] = f75, - 3 * SIZE
#endif
	STFD	[C10] = f79
	}
	;;
	{ .mmf
	STFD	[C3 ] = f80, SIZE
	STFD	[C11] = f84, SIZE
	}
	;;
	{ .mmi
	STFD	[C3 ] = f81, SIZE
	STFD	[C11] = f85, SIZE
	}
	;;
	{ .mmi
	STFD	[C3 ] = f82, SIZE
	STFD	[C11] = f86, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C3 ] = f83, 5 * SIZE
#else
	STFD	[C3 ] = f83, - 3 * SIZE
#endif
	STFD	[C11] = f87
	}
	;;
	{ .mmf
	STFD	[C4 ] = f88, SIZE
	STFD	[C12] = f92, SIZE
	}
	;;
	{ .mmi
	STFD	[C4 ] = f89, SIZE
	STFD	[C12] = f93, SIZE
	}
	;;
	{ .mmi
	STFD	[C4 ] = f90, SIZE
	STFD	[C12] = f94, SIZE

	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C4 ] = f91, 5 * SIZE
#else
	STFD	[C4 ] = f91, - 3 * SIZE
#endif
	STFD	[C12] = f95
	cmp.ne	p6, p0 = 1, I
	}
	;;
	adds	I = -1, I
	;;
	{ .mmi
	shladd	r2 = K, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	sub	L = K, KK
	}
	;;
	{ .mmi
#ifdef RT
	shladd	AORIG = r2, 3, AORIG
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
       ;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 2, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  8, KK
#elif defined LN
	adds	KK = -8, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	mov	f64  = f0
	mov	f72  = f0
	mov	f80  = f0
	mov	f88  = f0
	mov	f65  = f0
	mov	f73  = f0
	mov	f81  = f0
	mov	f89  = f0

	{ .mmb
	(p6)	br.cond.dptk .L052
	}
	;;
	.align 8

.L089:
#ifdef LN
	shladd	KK8 = K, BASE_SHIFT, r0
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
	mov	AOFFSET = A
	;;
	.align 16

.L090:
	tbit.z	p6, p0 = N, 1
	(p6)	br.cond.dpnt .L130
	;;

#ifdef RT
       { .mmi
	shladd	r3 = LDC, 1, r0
	nop	__LINE__
	shl	r2 = K, 1 + BASE_SHIFT
	}
	;;
	{ .mmi
	sub	B = B, r2
	sub	C = C, r3
	nop	__LINE__
	}
#endif
	;;
	mov	f64  = f0
	mov	f65  = f0
	mov	f66  = f0
	mov	f67  = f0

	mov	f72  = f0
	mov	f73  = f0
	mov	f74  = f0
	mov	f75  = f0
	;;
	{ .mfi
	mov	C1 = C			// coffset1 = c + 0 * ldc
#ifdef LN
	add	KK = M, OFFSET
#elif defined LT
	mov	KK = OFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
#if defined(LN) || defined(RT)
	mov	AORIG = A
#else
	mov	AOFFSET = A
#endif
	}
	{ .mmf
	add	C2 = LDC, C		// coffset2 = c + 1 * ldc
	}
	;;
	{ .mfi
#ifndef RT
	shladd	C = LDC, 1, C		// coffset += 8 * ldc
#else
	nop	__LINE__
#endif
	mov	f81  = f0
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;

	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L110
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 0 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
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
	{ .mmi
	adds	L =  1, L
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mii
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	adds	L =  -1, L
	}
	;;
	{ .mmi
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mib
	(p7) LDFD	f32 = [AOFFSET], 1 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L128
	}
	;;
	.align 8

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
	nop  __LINE__
	nop  __LINE__
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
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET]
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	;;
#else
	LDFPD	f32, f33 = [AOFFSET]
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	;;
#endif

#ifdef LN
	LDFD	f32 = [AOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	;;
	{ .mmi
	STFD	[BOFFSET]  = f64, SIZE
	adds	C1 = -1 * SIZE, C1
	}
	;;
	{ .mmi
	STFD	[BOFFSET]  = f72, -SIZE
	adds	C2 = -1 * SIZE, C2
	}
	;;
#endif

#ifdef LT
	LDFD	f32 = [AOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f72, -SIZE
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f34      = [BOFFSET], -3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	;;
	FMPY	f72  = f72,  f34
	;;
	STFD	[AOFFSET]  = f64,  SIZE
	;;
	STFD	[AOFFSET]  = f72, -SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 2 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f34 = [BOFFSET]
	;;
	FMPY	f72  = f72,  f32
	;;
	FNMA	f64  = f72,  f33, f64
	;;
	FMPY	f64  = f64,  f34
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f72, -SIZE
	;;
#endif

#ifndef LN
	STFD	[C1 ] = f64, SIZE
#else
	STFD	[C1 ] = f64
#endif
#ifndef LN
	STFD	[C2 ] = f72, SIZE
#else
	STFD	[C2 ] = f72
#endif

	mov	f64  = f0
	mov	f72  = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	add	AORIG = r2, AORIG
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	add	AOFFSET = L, AOFFSET
#else
	nop	__LINE__
#endif
	;;
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 1, BOFFSET
#else
	nop	__LINE__
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
	.align 8

.L110:
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L100
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
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
	{ .mfi
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mmf
	adds	L =  -1, L
	}
	;;
	{ .mmf
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mib
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L118
	}
	;;

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
	nop	__LINE__
	nop	__LINE__
	FMA	f73   = f33, f49, f73	// A2 * B2
	}
	;;
	{ .mfb
	(p4) LDFPD	f32, f33 = [AOFFSET],   2 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f73   = f41, f57, f73	// A2 * B2
	br.cloop.sptk.few .L112
	}
	;;
	.align 8

.L118:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = -2 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f65  = f34, f65
	FSUB	f73  = f35, f73
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET]
	adds	AOFFSET = -2 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f72  = f34, f72
	FSUB	f73  = f35, f73
	;;
#endif

#ifdef LN
	adds	AOFFSET = 2 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET]
	;;
	FMPY	f65  = f65,  f32
	FMPY	f73  = f73,  f32
	;;
	FNMA	f64  = f65,  f33, f64
	FNMA	f72  = f73,  f33, f72
	;;
	FMPY	f64  = f64,  f34
	FMPY	f72  = f72,  f34
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	;;
	STFD	[BOFFSET]  = f73, - 3 * SIZE
	;;
	adds	C1 = -2 * SIZE, C1
	adds	C2 = -2 * SIZE, C2
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET], - 3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	FNMA	f73  = f72,  f33, f73
	;;
	FMPY	f65  = f65,  f34
	FMPY	f73  = f73,  f34
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	;;
	STFD	[BOFFSET]  = f73, -3 * SIZE
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f34      = [BOFFSET], -3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f73  = f65,  f33, f73
	;;
	FMPY	f72  = f72,  f34
	FMPY	f73  = f73,  f34
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f73, -3 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 2 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f34 = [BOFFSET]
	;;
	FMPY	f72  = f72,  f32
	FMPY	f73  = f73,  f32
	;;
	FNMA	f64  = f72,  f33, f64
	FNMA	f65  = f73,  f33, f65
	;;
	FMPY	f64  = f64,  f34
	FMPY	f65  = f65,  f34
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f73, -3 * SIZE
	;;
#endif
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	;;
#ifndef LN
	STFD	[C1 ] = f65, SIZE
#else
	STFD	[C1 ] = f65, -SIZE
#endif
	;;
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	;;
#ifndef LN
	STFD	[C2 ] = f73, SIZE
#else
	STFD	[C2 ] = f73, -SIZE
#endif
	;;
	mov	f65  = f0
	mov	f73  = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#else
	nop	__LINE__
#endif
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L100:
	tbit.z	p6, p7 = M, 2
	(p6)	br.cond.dptk .L091
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 2 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	mov	f65  = f0
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
	shladd	AOFFSET = r3, 2, AORIG
	}
	;;
#endif
	{ .mfi
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	L =  -1, L
	}
	;;
	{ .mfi
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mmf
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	mov	ar.lc = L
	}
	;;
	{ .mmf
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	}
	{ .mfb
	(p6) br.cond.dpnt   .L108
	}
	;;

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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f75   = f43, f57, f75	// A4 * B2
	br.cloop.sptk.few .L102
	}
	;;
	.align 8

.L108:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -4, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 2, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	;;
	FSUB	f65  = f34, f65
	FSUB	f73  = f35, f73
	;;
	FSUB	f66  = f36, f66
	FSUB	f74  = f37, f74
	;;
	FSUB	f67  = f38, f67
	FSUB	f75  = f39, f75
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67

	FSUB	f72  = f36, f72
	FSUB	f73  = f37, f73
	FSUB	f74  = f38, f74
	FSUB	f75  = f39, f75
	;;
#endif

#ifdef LN
	adds	AOFFSET = 14 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f35, f34 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f36 = [AOFFSET], - 2 * SIZE
	;;
	LDFPD	f38, f37 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f40, f39 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFD	f41 = [AOFFSET]
	;;
	FMPY	f67  = f67,  f32
	FMPY	f75  = f75,  f32
	;;
	FNMA	f66  = f67,  f33, f66
	FNMA	f74  = f75,  f33, f74
	;;
	FNMA	f65  = f67,  f34, f65
	FNMA	f73  = f75,  f34, f73
	;;
	FNMA	f64  = f67,  f35, f64
	FNMA	f72  = f75,  f35, f72
	;;
	FMPY	f66  = f66,  f36
	FMPY	f74  = f74,  f36
	;;
	FNMA	f65  = f66,  f37, f65
	FNMA	f73  = f74,  f37, f73
	;;
	FNMA	f64  = f66,  f38, f64
	FNMA	f72  = f74,  f38, f72
	;;
	FMPY	f65  = f65,  f39
	FMPY	f73  = f73,  f39
	;;
	FNMA	f64  = f65,  f40, f64
	FNMA	f72  = f73,  f40, f72
	;;
	FMPY	f64  = f64,  f41
	FMPY	f72  = f72,  f41
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f66, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f74, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f73, -3 * SIZE
	STFD	[BOFFSET2] = f75, -3 * SIZE
	;;
	adds	C1 = -4 * SIZE, C1
	adds	C2 = -4 * SIZE, C2
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f36 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f39, f40 = [AOFFSET]
	adds	AOFFSET = 5 * SIZE, AOFFSET
	;;
	LDFD	f41 = [AOFFSET], -15 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	FNMA	f73  = f72,  f33, f73
	;;
	FNMA	f66  = f64,  f34, f66
	FNMA	f74  = f72,  f34, f74
	;;
	FNMA	f67  = f64,  f35, f67
	FNMA	f75  = f72,  f35, f75
	;;
	FMPY	f65  = f65,  f36
	FMPY	f73  = f73,  f36
	;;
	FNMA	f66  = f65,  f37, f66
	FNMA	f74  = f73,  f37, f74
	;;
	FNMA	f67  = f65,  f38, f67
	FNMA	f75  = f73,  f38, f75
	;;
	FMPY	f66  = f66,  f39
	FMPY	f74  = f74,  f39
	;;
	FNMA	f67  = f66,  f40, f67
	FNMA	f75  = f74,  f40, f75
	;;
	FMPY	f67  = f67,  f41
	FMPY	f75  = f75,  f41
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f66, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f74, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f73, -3 * SIZE
	STFD	[BOFFSET2] = f75, -3 * SIZE
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f34      = [BOFFSET], -3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	FMPY	f66  = f66,  f32
	FMPY	f67  = f67,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f73  = f65,  f33, f73
	FNMA	f74  = f66,  f33, f74
	FNMA	f75  = f67,  f33, f75
	;;
	FMPY	f72  = f72,  f34
	FMPY	f73  = f73,  f34
	FMPY	f74  = f74,  f34
	FMPY	f75  = f75,  f34
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2]  = f73, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2]  = f74, SIZE
	;;
	STFD	[AOFFSET]  = f67,  -3 * SIZE
	STFD	[AOFFSET2]  = f75, -3 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 2 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f34 = [BOFFSET]
	;;
	FMPY	f72  = f72,  f32
	FMPY	f73  = f73,  f32
	FMPY	f74  = f74,  f32
	FMPY	f75  = f75,  f32
	;;
	FNMA	f64  = f72,  f33, f64
	FNMA	f65  = f73,  f33, f65
	FNMA	f66  = f74,  f33, f66
	FNMA	f67  = f75,  f33, f67
	;;
	FMPY	f64  = f64,  f34
	FMPY	f65  = f65,  f34
	FMPY	f66  = f66,  f34
	FMPY	f67  = f67,  f34
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2]  = f72, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2]  = f73, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2]  = f74, SIZE
	;;
	STFD	[AOFFSET]  = f67, - 3 * SIZE
	STFD	[AOFFSET2]  = f75, - 3 * SIZE
	;;
#endif
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C1 ] = f67, SIZE
#else
	STFD	[C1 ] = f67, - 3 * SIZE
#endif
	}
	;;
	{ .mmf
	STFD	[C2 ] = f72, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmi
	STFD	[C2 ] = f73, SIZE
	}
	;;
	{ .mmi
	STFD	[C2 ] = f74, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C2 ] = f75, SIZE
#else
	STFD	[C2 ] = f75, - 3 * SIZE
#endif
	}
	;;
	mov	f65 = f0
	mov	f73 = f0
	mov	f66 = f0
	mov	f74 = f0
	mov	f67 = f0
	mov	f75 = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	{ .mmi
	sub	L = K, KK
	}
	;;
	{ .mmi
#ifdef RT
	shladd	AORIG = r2, 2, AORIG
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  4, KK
#elif defined LN
	adds	KK = -4, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L091:
	shr	I  = M, 3
	;;
	cmp.eq	p6, p7 = 0, I
	(p6)	br.cond.dpnt .L129
	;;
	.align 16

.L092:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 3 + BASE_SHIFT
	}
	{ .mmi
	shladd	r3 = KK, BASE_SHIFT, r0
	nop	__LINE__
	nop	__LINE__
	}
	;;
#if defined(LT) || defined(RN)
	{ .mmi
	(p7) LDFPD	f48, f49 = [BOFFSET], 2 * SIZE
	nop	__LINE__
	nop	__LINE__
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
	shladd	AOFFSET = r3, 3, AORIG
	}
	;;
#endif
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	{ .mmf
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	}
	;;
	{ .mmf
	(p7) LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mmf
	(p7) LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	;;
	{ .mmf
	CPREFETCH [PREC], LDC
	}
	{ .mfi
	adds	L =  1, L
	}
	;;
	{ .mmf
	CPREFETCH [PREC]
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	}
	;;
	{ .mfi
	adds	PREB = (PREFETCHSIZE - 8) * SIZE, BOFFSET
	}
	;;
	{ .mfi
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	L =  -1, L
	}
	;;
	{ .mfi
	mov	ar.lc = L
	}
	;;
	mov	f68  = f0
	mov	f69  = f0
	mov	f70  = f0
	mov	f71  = f0
	mov	f76  = f0
	mov	f77  = f0
	mov	f78  = f0
	mov	f79  = f0
	;;
	{ .mfb
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L098
	}
	;;
	.align 8

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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f76   = f44, f57, f76	// A5 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f69   = f45, f56, f69	// A6 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f77   = f45, f57, f77	// A6 * B2
	nop	__LINE__
	}
	;;
	{ .mfb
	nop	__LINE__
	(p3) FMA	f70   = f46, f56, f70	// A7 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f78   = f46, f57, f78	// A7 * B2
	nop	__LINE__
	}
	;;
	{ .mfi
	nop	__LINE__
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	(p3) FMA	f79   = f47, f57, f79	// A8 * B2
	br.cloop.sptk.few .L093
	}
	;;
	.align 8

.L098:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -8, KK
#else
	adds	r2 = -2, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 3, AORIG
	shladd	BOFFSET = r2, 1, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [BOFFSET]
	adds	BOFFSET = -14 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f72  = f33, f72
	FSUB	f65  = f34, f65
	FSUB	f73  = f35, f73

	FSUB	f66  = f36, f66
	FSUB	f74  = f37, f74
	FSUB	f67  = f38, f67
	FSUB	f75  = f39, f75

	FSUB	f68  = f40, f68
	FSUB	f76  = f41, f76
	FSUB	f69  = f42, f69
	FSUB	f77  = f43, f77

	FSUB	f70  = f44, f70
	FSUB	f78  = f45, f78
	FSUB	f71  = f46, f71
	FSUB	f79  = f47, f79
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f40, f41 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f44, f45 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f46, f47 = [AOFFSET]
	adds	AOFFSET = -14 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67
	FSUB	f68  = f36, f68
	FSUB	f69  = f37, f69
	FSUB	f70  = f38, f70
	FSUB	f71  = f39, f71
	;;
	FSUB	f72  = f40, f72
	FSUB	f73  = f41, f73
	FSUB	f74  = f42, f74
	FSUB	f75  = f43, f75
	FSUB	f76  = f44, f76
	FSUB	f77  = f45, f77
	FSUB	f78  = f46, f78
	FSUB	f79  = f47, f79
	;;
#endif

#ifdef LN
	adds	AOFFSET = 62 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f35, f34 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f37, f36 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f39, f38 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f40 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f42, f41 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f44, f43 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f46, f45 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f48, f47 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f50, f49 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f52, f51 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFD	f53 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f55, f54 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f57, f56 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFPD	f59, f58 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f61, f60 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFD	f16 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f18, f17 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFPD	f20, f19 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFD	f21 = [AOFFSET]
	;;
	FMPY	f71  = f71,  f32
	FMPY	f79  = f79,  f32
	;;
	FNMA	f70  = f71,  f33, f70
	FNMA	f78  = f79,  f33, f78
	;;
	FNMA	f69  = f71,  f34, f69
	FNMA	f77  = f79,  f34, f77
	;;
	FNMA	f68  = f71,  f35, f68
	FNMA	f76  = f79,  f35, f76
	;;
	FNMA	f67  = f71,  f36, f67
	FNMA	f75  = f79,  f36, f75
	;;
	FNMA	f66  = f71,  f37, f66
	FNMA	f74  = f79,  f37, f74
	;;
	FNMA	f65  = f71,  f38, f65
	FNMA	f73  = f79,  f38, f73
	;;
	FNMA	f64  = f71,  f39, f64
	FNMA	f72  = f79,  f39, f72
	;;
	FMPY	f70  = f70,  f40
	FMPY	f78  = f78,  f40
	;;
	FNMA	f69  = f70,  f41, f69
	FNMA	f77  = f78,  f41, f77
	;;
	FNMA	f68  = f70,  f42, f68
	FNMA	f76  = f78,  f42, f76
	;;
	FNMA	f67  = f70,  f43, f67
	FNMA	f75  = f78,  f43, f75
	;;
	FNMA	f66  = f70,  f44, f66
	FNMA	f74  = f78,  f44, f74
	;;
	FNMA	f65  = f70,  f45, f65
	FNMA	f73  = f78,  f45, f73
	;;
	FNMA	f64  = f70,  f46, f64
	FNMA	f72  = f78,  f46, f72
	;;
	FMPY	f69  = f69,  f47
	FMPY	f77  = f77,  f47
	;;
	FNMA	f68  = f69,  f48, f68
	FNMA	f76  = f77,  f48, f76
	;;
	FNMA	f67  = f69,  f49, f67
	FNMA	f75  = f77,  f49, f75
	;;
	FNMA	f66  = f69,  f50, f66
	FNMA	f74  = f77,  f50, f74
	;;
	FNMA	f65  = f69,  f51, f65
	FNMA	f73  = f77,  f51, f73
	;;
	FNMA	f64  = f69,  f52, f64
	FNMA	f72  = f77,  f52, f72
	;;
	FMPY	f68  = f68,  f53
	FMPY	f76  = f76,  f53
	;;
	FNMA	f67  = f68,  f54, f67
	FNMA	f75  = f76,  f54, f75
	;;
	FNMA	f66  = f68,  f55, f66
	FNMA	f74  = f76,  f55, f74
	;;
	FNMA	f65  = f68,  f56, f65
	FNMA	f73  = f76,  f56, f73
	;;
	FNMA	f64  = f68,  f57, f64
	FNMA	f72  = f76,  f57, f72
	;;
	FMPY	f67  = f67,  f58
	FMPY	f75  = f75,  f58
	;;
	FNMA	f66  = f67,  f59, f66
	FNMA	f74  = f75,  f59, f74
	;;
	FNMA	f65  = f67,  f60, f65
	FNMA	f73  = f75,  f60, f73
	;;
	FNMA	f64  = f67,  f61, f64
	FNMA	f72  = f75,  f61, f72
	;;
	FMPY	f66  = f66,  f16
	FMPY	f74  = f74,  f16
	;;
	FNMA	f65  = f66,  f17, f65
	FNMA	f73  = f74,  f17, f73
	;;
	FNMA	f64  = f66,  f18, f64
	FNMA	f72  = f74,  f18, f72
	;;
	FMPY	f65  = f65,  f19
	FMPY	f73  = f73,  f19
	;;
	FNMA	f64  = f65,  f20, f64
	FNMA	f72  = f73,  f20, f72
	;;
	FMPY	f64  = f64,  f21
	FMPY	f72  = f72,  f21
	;;

	adds	BOFFSET  =  8 * SIZE, BOFFSET
	adds	BOFFSET2 =  8 * SIZE, BOFFSET2
	;;
	STFD	[BOFFSET]  = f68, SIZE
	STFD	[BOFFSET2] = f70, SIZE
	;;
	STFD	[BOFFSET]  = f76, SIZE
	STFD	[BOFFSET2] = f78, SIZE
	;;
	STFD	[BOFFSET]  = f69, SIZE
	STFD	[BOFFSET2] = f71, SIZE
	;;
	STFD	[BOFFSET]  = f77, - 11 * SIZE
	STFD	[BOFFSET2] = f79, - 11 * SIZE
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f66, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f74, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f73, - 3 * SIZE
	STFD	[BOFFSET2] = f75, - 3 * SIZE
	;;
	adds	C1 = -8 * SIZE, C1
	adds	C2 = -8 * SIZE, C2
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f40 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f41, f42 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f43, f44 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f45, f46 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f47, f48 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f49, f50 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f51, f52 = [AOFFSET]
	adds	AOFFSET = 5 * SIZE, AOFFSET
	;;
	LDFD	f53 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f54, f55 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [AOFFSET]
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f58, f59 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [AOFFSET]
	adds	AOFFSET = 7 * SIZE, AOFFSET
	;;
	LDFD	f16 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f17, f18 = [AOFFSET]
	adds	AOFFSET = 8 * SIZE, AOFFSET
	;;
	LDFPD	f19, f20 = [AOFFSET]
	adds	AOFFSET = 9 * SIZE, AOFFSET
	;;
	LDFD	f21 = [AOFFSET]
	adds	AOFFSET = -63 * SIZE, AOFFSET
	;;
	FMPY	f64  = f64,  f32
	FMPY	f72  = f72,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	FNMA	f73  = f72,  f33, f73
	;;
	FNMA	f66  = f64,  f34, f66
	FNMA	f74  = f72,  f34, f74
	;;
	FNMA	f67  = f64,  f35, f67
	FNMA	f75  = f72,  f35, f75
	;;
	FNMA	f68  = f64,  f36, f68
	FNMA	f76  = f72,  f36, f76
	;;
	FNMA	f69  = f64,  f37, f69
	FNMA	f77  = f72,  f37, f77
	;;
	FNMA	f70  = f64,  f38, f70
	FNMA	f78  = f72,  f38, f78
	;;
	FNMA	f71  = f64,  f39, f71
	FNMA	f79  = f72,  f39, f79
	;;
	FMPY	f65  = f65,  f40
	FMPY	f73  = f73,  f40
	;;
	FNMA	f66  = f65,  f41, f66
	FNMA	f74  = f73,  f41, f74
	;;
	FNMA	f67  = f65,  f42, f67
	FNMA	f75  = f73,  f42, f75
	;;
	FNMA	f68  = f65,  f43, f68
	FNMA	f76  = f73,  f43, f76
	;;
	FNMA	f69  = f65,  f44, f69
	FNMA	f77  = f73,  f44, f77
	;;
	FNMA	f70  = f65,  f45, f70
	FNMA	f78  = f73,  f45, f78
	;;
	FNMA	f71  = f65,  f46, f71
	FNMA	f79  = f73,  f46, f79
	;;
	FMPY	f66  = f66,  f47
	FMPY	f74  = f74,  f47
	;;
	FNMA	f67  = f66,  f48, f67
	FNMA	f75  = f74,  f48, f75
	;;
	FNMA	f68  = f66,  f49, f68
	FNMA	f76  = f74,  f49, f76
	;;
	FNMA	f69  = f66,  f50, f69
	FNMA	f77  = f74,  f50, f77
	;;
	FNMA	f70  = f66,  f51, f70
	FNMA	f78  = f74,  f51, f78
	;;
	FNMA	f71  = f66,  f52, f71
	FNMA	f79  = f74,  f52, f79
	;;
	FMPY	f67  = f67,  f53
	FMPY	f75  = f75,  f53
	;;
	FNMA	f68  = f67,  f54, f68
	FNMA	f76  = f75,  f54, f76
	;;
	FNMA	f69  = f67,  f55, f69
	FNMA	f77  = f75,  f55, f77
	;;
	FNMA	f70  = f67,  f56, f70
	FNMA	f78  = f75,  f56, f78
	;;
	FNMA	f71  = f67,  f57, f71
	FNMA	f79  = f75,  f57, f79
	;;
	FMPY	f68  = f68,  f58
	FMPY	f76  = f76,  f58
	;;
	FNMA	f69  = f68,  f59, f69
	FNMA	f77  = f76,  f59, f77
	;;
	FNMA	f70  = f68,  f60, f70
	FNMA	f78  = f76,  f60, f78
	;;
	FNMA	f71  = f68,  f61, f71
	FNMA	f79  = f76,  f61, f79
	;;
	FMPY	f69  = f69,  f16
	FMPY	f77  = f77,  f16
	;;
	FNMA	f70  = f69,  f17, f70
	FNMA	f78  = f77,  f17, f78
	;;
	FNMA	f71  = f69,  f18, f71
	FNMA	f79  = f77,  f18, f79
	;;
	FMPY	f70  = f70,  f19
	FMPY	f78  = f78,  f19
	;;
	FNMA	f71  = f70,  f20, f71
	FNMA	f79  = f78,  f20, f79
	;;
	FMPY	f71  = f71,  f21
	FMPY	f79  = f79,  f21
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f66, SIZE
	;;
	STFD	[BOFFSET]  = f72, SIZE
	STFD	[BOFFSET2] = f74, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	STFD	[BOFFSET2] = f67, SIZE
	;;
	STFD	[BOFFSET]  = f73, 5 * SIZE
	STFD	[BOFFSET2] = f75, 5 * SIZE
	;;
	STFD	[BOFFSET]  = f68, SIZE
	STFD	[BOFFSET2] = f70, SIZE
	;;
	STFD	[BOFFSET]  = f76, SIZE
	STFD	[BOFFSET2] = f78, SIZE
	;;
	STFD	[BOFFSET]  = f69, SIZE
	STFD	[BOFFSET2] = f71, SIZE
	;;
	STFD	[BOFFSET]  = f77, -11 * SIZE
	STFD	[BOFFSET2] = f79, -11 * SIZE
	;;
	adds	C9  = 4 * SIZE, C1
	;;
#endif

#ifdef RN
	LDFPD	f32, f33 = [BOFFSET]
	adds	BOFFSET = 3 * SIZE, BOFFSET
	;;
	LDFD	f34      = [BOFFSET], -3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	FMPY	f68  = f68,  f32
	FMPY	f65  = f65,  f32
	FMPY	f69  = f69,  f32
	FMPY	f66  = f66,  f32
	FMPY	f70  = f70,  f32
	FMPY	f67  = f67,  f32
	FMPY	f71  = f71,  f32
	;;
	FNMA	f72  = f64,  f33, f72
	FNMA	f76  = f68,  f33, f76
	FNMA	f73  = f65,  f33, f73
	FNMA	f77  = f69,  f33, f77
	FNMA	f74  = f66,  f33, f74
	FNMA	f78  = f70,  f33, f78
	FNMA	f75  = f67,  f33, f75
	FNMA	f79  = f71,  f33, f79
	;;
	FMPY	f72  = f72,  f34
	FMPY	f76  = f76,  f34
	FMPY	f73  = f73,  f34
	FMPY	f77  = f77,  f34
	FMPY	f74  = f74,  f34
	FMPY	f78  = f78,  f34
	FMPY	f75  = f75,  f34
	FMPY	f79  = f79,  f34
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f68, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f69, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2] = f70, SIZE
	;;
	STFD	[AOFFSET]  = f67, 5 * SIZE
	STFD	[AOFFSET2] = f71, 5 * SIZE
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f76, SIZE
	;;
	STFD	[AOFFSET]  = f73, SIZE
	STFD	[AOFFSET2] = f77, SIZE
	;;
	STFD	[AOFFSET]  = f74, SIZE
	STFD	[AOFFSET2] = f78, SIZE
	;;
	STFD	[AOFFSET]  = f75, -11 * SIZE
	STFD	[AOFFSET2] = f79, -11 * SIZE
	;;
#endif

#ifdef RT
	adds	BOFFSET = 2 * SIZE, BOFFSET
	;;
	LDFPD	f33, f32 = [BOFFSET]
	adds	BOFFSET = - 2 * SIZE, BOFFSET
	;;
	LDFD	f34 = [BOFFSET]
	;;

	FMPY	f72  = f72,  f32
	FMPY	f76  = f76,  f32
	FMPY	f73  = f73,  f32
	FMPY	f77  = f77,  f32
	FMPY	f74  = f74,  f32
	FMPY	f78  = f78,  f32
	FMPY	f75  = f75,  f32
	FMPY	f79  = f79,  f32
	;;
	FNMA	f64  = f72,  f33, f64
	FNMA	f68  = f76,  f33, f68
	FNMA	f65  = f73,  f33, f65
	FNMA	f69  = f77,  f33, f69
	FNMA	f66  = f74,  f33, f66
	FNMA	f70  = f78,  f33, f70
	FNMA	f67  = f75,  f33, f67
	FNMA	f71  = f79,  f33, f71
	;;
	FMPY	f64  = f64,  f34
	FMPY	f68  = f68,  f34
	FMPY	f65  = f65,  f34
	FMPY	f69  = f69,  f34
	FMPY	f66  = f66,  f34
	FMPY	f70  = f70,  f34
	FMPY	f67  = f67,  f34
	FMPY	f71  = f71,  f34
	;;
	adds	AOFFSET  =  8 * SIZE, AOFFSET
	adds	AOFFSET2 =  8 * SIZE, AOFFSET2
	;;
	STFD	[AOFFSET]  = f72, SIZE
	STFD	[AOFFSET2] = f76, SIZE
	;;
	STFD	[AOFFSET]  = f73, SIZE
	STFD	[AOFFSET2] = f77, SIZE
	;;
	STFD	[AOFFSET]  = f74, SIZE
	STFD	[AOFFSET2] = f78, SIZE
	;;
	STFD	[AOFFSET]  = f75, - 11 * SIZE
	STFD	[AOFFSET2] = f79, - 11 * SIZE
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f68, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f69, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2] = f70, SIZE
	;;
	STFD	[AOFFSET]  = f67, - 3 * SIZE
	STFD	[AOFFSET2] = f71, - 3 * SIZE
	;;

#endif
	adds	C9  = 4 * SIZE, C1
	;;

	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	adds	C10 = 4 * SIZE, C2
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C1 ] = f67, 5 * SIZE
#else
	STFD	[C1 ] = f67, - 3 * SIZE
#endif
	STFD	[C9 ] = f71
	adds	C11 = 4 * SIZE, C3
	}
	;;
	{ .mmf
	STFD	[C2 ] = f72, SIZE
	STFD	[C10] = f76, SIZE
	mov	f72  = f0
	}
	;;
	{ .mmi
	STFD	[C2 ] = f73, SIZE
	STFD	[C10] = f77, SIZE
	}
	;;
	{ .mmi
	STFD	[C2 ] = f74, SIZE
	STFD	[C10] = f78, SIZE
	adds	C12 = 4 * SIZE, C4
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C2 ] = f75, 5 * SIZE
#else
	STFD	[C2 ] = f75, - 3 * SIZE
#endif
	STFD	[C10] = f79
	}
	;;
	{ .mmf
	cmp.ne	p6, p0 = 1, I
	}
	;;
	adds	I = -1, I
	;;
	{ .mmi
	shladd	r2 = K, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	sub	L = K, KK
	}
	;;
	{ .mmi
#ifdef RT
	shladd	AORIG = r2, 3, AORIG
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
       ;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, 1, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  8, KK
#elif defined LN
	adds	KK = -8, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;

	mov	f64  = f0
	mov	f65  = f0
	mov	f66  = f0
	mov	f67  = f0
	mov	f72  = f0
	mov	f73  = f0
	mov	f74  = f0
	mov	f75  = f0

	(p6)	br.cond.dptk .L092
	;;
	.align 8

.L129:
#ifdef LN
	shladd	KK8 = K, BASE_SHIFT, r0
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
	mov	AOFFSET = A
	;;
	.align 16

.L130:
	tbit.z	p6, p0 = N, 0
	(p6)	br.cond.dpnt .L999
	;;

#ifdef RT
       { .mmi
	nop	__LINE__
	shl	r2 = K, BASE_SHIFT
	}
	;;
	{ .mmi
	sub	B = B, r2
	sub	C = C, LDC
	nop	__LINE__
	}
#endif
	;;
	mov	f64  = f0
	mov	f65  = f0
	mov	f66  = f0
	mov	f67  = f0

	mov	f68  = f0
	mov	f69  = f0
	mov	f70  = f0
	mov	f71  = f0
	;;

	{ .mfi
	mov	C1 = C			// coffset1 = c + 0 * ldc
#ifdef LN
	add	KK = M, OFFSET
#elif defined LT
	mov	KK = OFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmf
#if defined(LN) || defined(RT)
	mov	AORIG = A
#else
	mov	AOFFSET = A
#endif
	}
	;;
	{ .mfi
#ifndef RT
	add	C = C, LDC		// coffset += 8 * ldc
#else
	nop	__LINE__
#endif
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;

.L160:
	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	tbit.z	p6, p7 = M, 0
	(p6)	br.cond.dptk .L150
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 0 + BASE_SHIFT
	}
	;;
	shladd	r3 = KK, BASE_SHIFT, r0
	;;
#if defined(LT) || defined(RN)
	{ .mmi
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	nop	__LINE__
	adds	L =  1, L
	}
	;;
#else
	{ .mmi
	shladd	BOFFSET = KK, BASE_SHIFT, B
	nop	__LINE__
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	adds	L =  1, L
	add	AOFFSET = r3, AORIG
	}
	;;
#endif
	;;
	{ .mii
	tbit.z	p12, p0 = L, 0
	shr	L = L, 1
	}
	;;
	{ .mmi
	cmp.eq  p6, p0 = 0, L
	adds	L =  -1, L
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mib
	(p7) LDFD	f32 = [AOFFSET], 1 * SIZE
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L168
	}
	;;
	.align 8

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
	nop	__LINE__
	adds	L = -1, L
	}
	{ .mfb
	(p4) LDFD	f48 = [BOFFSET],   1 * SIZE
	(p3) FMA	f64   = f40, f56, f64	// A1 * B1
	br.cloop.sptk.few .L162
	}
	;;
	.align 8

.L168:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -1, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	add	AOFFSET = r2, AORIG
	add	BOFFSET = r2, B
	;;
#endif

#if defined(LN) || defined(LT)
	{ .mmi
	LDFD	f32 = [BOFFSET]
	LDFD	f33 = [AOFFSET]
#ifdef LN
	adds	C1 = -1 * SIZE, C1
#else
	nop	__LINE__
#endif
	}
	;;
#else
	{ .mmi
	LDFD	f32 = [AOFFSET]
	LDFD	f33 = [BOFFSET]
	nop	__LINE__
	}
	;;
#endif

	{ .mmf
	sub	L = K, KK
#ifdef RT
	shladd	AORIG = K, BASE_SHIFT, AORIG
#else
	nop	__LINE__
#endif
	FSUB	f64  = f32, f64
	}
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
	FMPY	f64  = f64,  f33
	;;
#if defined(LN) || defined(LT)
	{ .mmf
	STFD	[BOFFSET]  = f64
#ifndef LN
	STFD	[C1 ] = f64, SIZE
#else
	STFD	[C1 ] = f64
#endif
	mov	f64  = f0
	}
	;;
#else
	{ .mmf
	STFD	[AOFFSET]  = f64
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	}
	;;
#endif

#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, BASE_SHIFT, AOFFSET
#else
	nop	__LINE__
#endif
#if defined(LT) || defined(RN)
	shladd	BOFFSET = L, BASE_SHIFT, BOFFSET
#else
	nop	__LINE__
#endif
	;;
	.align 8

.L150:
	tbit.z	p6, p7 = M, 1
	(p6)	br.cond.dptk .L140
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 1 + BASE_SHIFT
	}
	;;
	shladd	r3 = KK, BASE_SHIFT, r0
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK, BASE_SHIFT, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	shladd	AOFFSET = r3, 1, AORIG
	}
	;;
#endif
	{ .mfi
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mmf
	adds	L =  -1, L
	}
	;;
	{ .mmf
	cmp.eq  p6, p0 = -1, L
	}
	;;
	(p7) LDFD	f32 = [AOFFSET], SIZE
	;;
	(p7) LDFD	f33 = [AOFFSET], SIZE
	;;
	;;
	{ .mib
	mov	ar.lc = L
	(p6) br.cond.dpnt   .L158
	}
	;;

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
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -2, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 1, AORIG
	add	BOFFSET = r2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET]
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	;;
#else
	LDFPD	f32, f33 = [AOFFSET]
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	;;
#endif

#ifdef LN
	adds	AOFFSET = 2 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET]
	;;
	FMPY	f65  = f65,  f32
	;;
	FNMA	f64  = f65,  f33, f64
	;;
	FMPY	f64  = f64,  f34
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f65, - SIZE
	;;
	adds	C1 = -2 * SIZE, C1
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f34 = [AOFFSET], - 3 * SIZE
	;;
	FMPY	f64  = f64,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	;;
	FMPY	f65  = f65,  f34
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f65, -SIZE
	;;
#endif

#ifdef RN
	LDFD	f32 = [BOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f65, - SIZE
	;;
#endif

#ifdef RT
	LDFD	f32 = [BOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f65, - SIZE
	;;
#endif
	STFD	[C1 ] = f64, SIZE
	;;
#ifndef LN
	STFD	[C1 ] = f65, SIZE
#else
	STFD	[C1 ] = f65, -SIZE
#endif
	;;
	mov	f64  = f0
	mov	f65  = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	sub	L = K, KK
	;;
#ifdef RT
	shladd	AORIG = r2, 1, AORIG
#else
	nop	__LINE__
#endif
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 1, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	add	BOFFSET = L, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  2, KK
#elif defined LN
	adds	KK = -2, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L140:
	tbit.z	p6, p7 = M, 2
	(p6)	br.cond.dptk .L131
	;;

	{ .mib
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 2 + BASE_SHIFT
	}
	;;
	shladd	r3 = KK, BASE_SHIFT, r0
	;;
#if defined(LT) || defined(RN)
	{ .mmf
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	mov	f65  = f0
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK, BASE_SHIFT, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	shladd	AOFFSET = r3, 2, AORIG
	}
	;;
#endif
	{ .mfi
	adds	L =  1, L
	}
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mfi
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	L =  -1, L
	}
	;;
	{ .mfi
	cmp.eq  p6, p0 = -1, L
	}
	;;
	{ .mmf
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	mov	ar.lc = L
	}
	;;
	{ .mmf
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	}
	{ .mfb
	(p6) br.cond.dpnt   .L148
	}
	;;

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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	}
	;;
	{ .mfi
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	adds	L = -1, L
	}
	{ .mfb
	nop	__LINE__
	nop.f 0
	br.cloop.sptk.few .L142
	}
	;;

.L148:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -4, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 2, AORIG
	add	BOFFSET = r2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET]
	adds	BOFFSET = -2 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET]
	adds	AOFFSET = -2 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67
	;;
#endif

#ifdef LN
	adds	AOFFSET = 14 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f35, f34 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f36 = [AOFFSET], - 2 * SIZE
	;;
	LDFPD	f38, f37 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f40, f39 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFD	f41 = [AOFFSET]
	;;
	FMPY	f67  = f67,  f32
	;;
	FNMA	f66  = f67,  f33, f66
	;;
	FNMA	f65  = f67,  f34, f65
	;;
	FNMA	f64  = f67,  f35, f64
	;;
	FMPY	f66  = f66,  f36
	;;
	FNMA	f65  = f66,  f37, f65
	;;
	FNMA	f64  = f66,  f38, f64
	;;
	FMPY	f65  = f65,  f39
	;;
	FNMA	f64  = f65,  f40, f64
	;;
	FMPY	f64  = f64,  f41
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	;;
	STFD	[BOFFSET]  = f67, -3 * SIZE
	;;
	adds	C1 = -4 * SIZE, C1
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f36 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f37, f38 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f39, f40 = [AOFFSET]
	adds	AOFFSET = 5 * SIZE, AOFFSET
	;;
	LDFD	f41 = [AOFFSET], -15 * SIZE
	;;
	FMPY	f64  = f64,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	;;
	FNMA	f66  = f64,  f34, f66
	;;
	FNMA	f67  = f64,  f35, f67
	;;
	FMPY	f65  = f65,  f36
	;;
	FNMA	f66  = f65,  f37, f66
	;;
	FNMA	f67  = f65,  f38, f67
	;;
	FMPY	f66  = f66,  f39
	;;
	FNMA	f67  = f66,  f40, f67
	;;
	FMPY	f67  = f67,  f41
	;;
	STFD	[BOFFSET]  = f64, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	;;
	STFD	[BOFFSET]  = f67, -3 * SIZE
	;;
#endif

#ifdef RN
	LDFD	f32 = [BOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	FMPY	f66  = f66,  f32
	FMPY	f67  = f67,  f32
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	;;
	STFD	[AOFFSET]  = f67,  -3 * SIZE
	;;
#endif

#ifdef RT
	LDFD	f32 = [BOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f65  = f65,  f32
	FMPY	f66  = f66,  f32
	FMPY	f67  = f67,  f32
	;;
	STFD	[AOFFSET]  = f64, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	;;
	STFD	[AOFFSET]  = f67, - 3 * SIZE
	;;
#endif
	{ .mmf
	STFD	[C1 ] = f64, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C1 ] = f67, SIZE
#else
	STFD	[C1 ] = f67, - 3 * SIZE
#endif
	}
	;;
	{ .mmf
	mov	f72  = f0
	}
	;;
	mov	f65 = f0
	mov	f73 = f0
	mov	f66 = f0
	mov	f74 = f0
	mov	f67 = f0
	mov	f75 = f0
	;;
	shladd	r2 = K, BASE_SHIFT, r0
	;;
	{ .mmi
	sub	L = K, KK
	}
	;;
	{ .mmi
#ifdef RT
	shladd	AORIG = r2, 2, AORIG
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 2, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	add	BOFFSET = L, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  4, KK
#elif defined LN
	adds	KK = -4, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;
	.align 8

.L131:
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	;;
	shr	I  = M, 3
	;;
	cmp.eq	p6, p7 = 0, I
	(p6)	br.cond.dpnt .L169
	;;
	.align 16

.L132:
	{ .mmi
	cmp.ne	p7, p0 = r0, L
	adds	BOFFSET = 0 * SIZE, B
	shl	r2 = K, 3 + BASE_SHIFT
	}
	;;
	shladd	r3 = KK, BASE_SHIFT, r0
	;;
#if defined(LT) || defined(RN)
	{ .mmi
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
#else
	{ .mfi
	shladd	BOFFSET = KK, BASE_SHIFT, B
#ifdef LN
	sub	AORIG = AORIG, r2
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mfi
	(p7) LDFD	f48 = [BOFFSET], 1 * SIZE
	shladd	AOFFSET = r3, 3, AORIG
	}
	;;
#endif
	(p7) LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	{ .mmf
	(p7) LDFPD	f34, f35  = [AOFFSET], 2 * SIZE
	}
	;;
	{ .mmf
	(p7) LDFPD	f36, f37  = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	cmp.eq	p3, p0 = r0, r0
	}
	;;
	{ .mmf
	(p7) LDFPD	f38, f39  = [AOFFSET], 2 * SIZE
	}
	{ .mfi
	adds	PREC = CPREFETCHSIZE * SIZE, C1
	}
	;;
	{ .mmf
	CPREFETCH [PREC]
	}
	{ .mfi
	adds	L =  1, L
	}
	;;
	{ .mfi
	adds	PREA = (PREFETCHSIZE + 8) * SIZE, AOFFSET
	}
	;;
	{ .mfi
	adds	PREB = (PREFETCHSIZE - 8) * SIZE, BOFFSET
	}
	;;
	{ .mfi
	tbit.z	p12, p0 = L, 0
	}
	{ .mfi
	shr	L = L, 1
	}
	;;
	{ .mfi
	adds	L =  -1, L
	}
	;;
	{ .mfi
	mov	ar.lc = L
	}
	;;
	mov	f64  = f0
	mov	f65  = f0
	mov	f66  = f0
	mov	f67  = f0

	mov	f68  = f0
	mov	f69  = f0
	mov	f70  = f0
	mov	f71  = f0
	;;

	{ .mfb
	cmp.eq  p6, p0 = -1, L
	(p6) br.cond.dpnt   .L138
	}
	;;
	.align 16

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
	nop	__LINE__
	FMA	f67   = f35, f48, f67	// A4 * B1
	}
	;;
	{ .mfb
	(p3) LDFPD	f42, f43 = [AOFFSET], 2 * SIZE
	FMA	f68   = f36, f48, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f67   = f43, f56, f67	// A4 * B1
	}
	;;
	{ .mfb
	(p4) LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	(p3) FMA	f68   = f44, f56, f68	// A5 * B1
	nop	__LINE__
	}
	{ .mfb
	nop	__LINE__
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
	nop	__LINE__
	(p3) FMA	f71   = f47, f56, f71	// A8 * B1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p4) LDFPD	f38, f39 = [AOFFSET], 2 * SIZE
	nop	__LINE__
	br.cloop.sptk.few .L133
	}
	;;

.L138:
#if defined(LN) || defined(RT)
#ifdef LN
	adds	r2 = -8, KK
#else
	adds	r2 = -1, KK
#endif
	;;
	shladd	r2 = r2, BASE_SHIFT, r0
	;;
	shladd	AOFFSET = r2, 3, AORIG
	add	BOFFSET = r2, B
	;;
#endif
 	adds	AOFFSET2 = 4 * SIZE, AOFFSET
 	adds	BOFFSET2 = 4 * SIZE, BOFFSET
	;;

#if defined(LN) || defined(LT)
	LDFPD	f32, f33 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [BOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [BOFFSET]
	adds	BOFFSET = -6 * SIZE, BOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67

	FSUB	f68  = f36, f68
	FSUB	f69  = f37, f69
	FSUB	f70  = f38, f70
	FSUB	f71  = f39, f71
	;;
#else
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = -6 * SIZE, AOFFSET
	;;
	FSUB	f64  = f32, f64
	FSUB	f65  = f33, f65
	FSUB	f66  = f34, f66
	FSUB	f67  = f35, f67
	FSUB	f68  = f36, f68
	FSUB	f69  = f37, f69
	FSUB	f70  = f38, f70
	FSUB	f71  = f39, f71
	;;
#endif

#ifdef LN
	adds	AOFFSET = 62 * SIZE, AOFFSET
	;;
	LDFPD	f33, f32 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f35, f34 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f37, f36 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f39, f38 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFD	f40 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f42, f41 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f44, f43 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f46, f45 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFPD	f48, f47 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f50, f49 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f52, f51 = [AOFFSET]
	adds	AOFFSET = - 4 * SIZE, AOFFSET
	;;
	LDFD	f53 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f55, f54 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f57, f56 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFPD	f59, f58 = [AOFFSET]
	adds	AOFFSET = - 2 * SIZE, AOFFSET
	;;
	LDFPD	f61, f60 = [AOFFSET]
	adds	AOFFSET = - 6 * SIZE, AOFFSET
	;;
	LDFD	f16 = [AOFFSET], -2 * SIZE
	;;
	LDFPD	f18, f17 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFPD	f20, f19 = [AOFFSET]
	adds	AOFFSET = - 8 * SIZE, AOFFSET
	;;
	LDFD	f21 = [AOFFSET]
	;;
	FMPY	f71  = f71,  f32
	;;
	FNMA	f70  = f71,  f33, f70
	;;
	FNMA	f69  = f71,  f34, f69
	;;
	FNMA	f68  = f71,  f35, f68
	;;
	FNMA	f67  = f71,  f36, f67
	;;
	FNMA	f66  = f71,  f37, f66
	;;
	FNMA	f65  = f71,  f38, f65
	;;
	FNMA	f64  = f71,  f39, f64
	;;
	FMPY	f70  = f70,  f40
	;;
	FNMA	f69  = f70,  f41, f69
	;;
	FNMA	f68  = f70,  f42, f68
	;;
	FNMA	f67  = f70,  f43, f67
	;;
	FNMA	f66  = f70,  f44, f66
	;;
	FNMA	f65  = f70,  f45, f65
	;;
	FNMA	f64  = f70,  f46, f64
	;;
	FMPY	f69  = f69,  f47
	;;
	FNMA	f68  = f69,  f48, f68
	;;
	FNMA	f67  = f69,  f49, f67
	;;
	FNMA	f66  = f69,  f50, f66
	;;
	FNMA	f65  = f69,  f51, f65
	;;
	FNMA	f64  = f69,  f52, f64
	;;
	FMPY	f68  = f68,  f53
	;;
	FNMA	f67  = f68,  f54, f67
	;;
	FNMA	f66  = f68,  f55, f66
	;;
	FNMA	f65  = f68,  f56, f65
	;;
	FNMA	f64  = f68,  f57, f64
	;;
	FMPY	f67  = f67,  f58
	;;
	FNMA	f66  = f67,  f59, f66
	;;
	FNMA	f65  = f67,  f60, f65
	;;
	FNMA	f64  = f67,  f61, f64
	;;
	FMPY	f66  = f66,  f16
	;;
	FNMA	f65  = f66,  f17, f65
	;;
	FNMA	f64  = f66,  f18, f64
	;;
	FMPY	f65  = f65,  f19
	;;
	FNMA	f64  = f65,  f20, f64
	;;
	FMPY	f64  = f64,  f21
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f68, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	STFD	[BOFFSET2] = f69, SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	STFD	[BOFFSET2] = f70, SIZE
	;;
	STFD	[BOFFSET]  = f67, - 3 * SIZE
	STFD	[BOFFSET2] = f71, - 3 * SIZE
	;;
	adds	C1 = -8 * SIZE, C1
	;;
#endif

#ifdef LT
	LDFPD	f32, f33 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f34, f35 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f36, f37 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f38, f39 = [AOFFSET]
	adds	AOFFSET = 3 * SIZE, AOFFSET
	;;
	LDFD	f40 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f41, f42 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f43, f44 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f45, f46 = [AOFFSET]
	adds	AOFFSET = 4 * SIZE, AOFFSET
	;;
	LDFPD	f47, f48 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f49, f50 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f51, f52 = [AOFFSET]
	adds	AOFFSET = 5 * SIZE, AOFFSET
	;;
	LDFD	f53 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f54, f55 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f56, f57 = [AOFFSET]
	adds	AOFFSET = 6 * SIZE, AOFFSET
	;;
	LDFPD	f58, f59 = [AOFFSET], 2 * SIZE
	;;
	LDFPD	f60, f61 = [AOFFSET]
	adds	AOFFSET = 7 * SIZE, AOFFSET
	;;
	LDFD	f16 = [AOFFSET], 1 * SIZE
	;;
	LDFPD	f17, f18 = [AOFFSET]
	adds	AOFFSET = 8 * SIZE, AOFFSET
	;;
	LDFPD	f19, f20 = [AOFFSET]
	adds	AOFFSET = 9 * SIZE, AOFFSET
	;;
	LDFD	f21 = [AOFFSET]
	adds	AOFFSET = -63 * SIZE, AOFFSET
	;;
	FMPY	f64  = f64,  f32
	;;
	FNMA	f65  = f64,  f33, f65
	;;
	FNMA	f66  = f64,  f34, f66
	;;
	FNMA	f67  = f64,  f35, f67
	;;
	FNMA	f68  = f64,  f36, f68
	;;
	FNMA	f69  = f64,  f37, f69
	;;
	FNMA	f70  = f64,  f38, f70
	;;
	FNMA	f71  = f64,  f39, f71
	;;
	FMPY	f65  = f65,  f40
	;;
	FNMA	f66  = f65,  f41, f66
	;;
	FNMA	f67  = f65,  f42, f67
	;;
	FNMA	f68  = f65,  f43, f68
	;;
	FNMA	f69  = f65,  f44, f69
	;;
	FNMA	f70  = f65,  f45, f70
	;;
	FNMA	f71  = f65,  f46, f71
	;;
	FMPY	f66  = f66,  f47
	;;
	FNMA	f67  = f66,  f48, f67
	;;
	FNMA	f68  = f66,  f49, f68
	;;
	FNMA	f69  = f66,  f50, f69
	;;
	FNMA	f70  = f66,  f51, f70
	;;
	FNMA	f71  = f66,  f52, f71
	;;
	FMPY	f67  = f67,  f53
	;;
	FNMA	f68  = f67,  f54, f68
	;;
	FNMA	f69  = f67,  f55, f69
	;;
	FNMA	f70  = f67,  f56, f70
	;;
	FNMA	f71  = f67,  f57, f71
	;;
	FMPY	f68  = f68,  f58
	;;
	FNMA	f69  = f68,  f59, f69
	;;
	FNMA	f70  = f68,  f60, f70
	;;
	FNMA	f71  = f68,  f61, f71
	;;
	FMPY	f69  = f69,  f16
	;;
	FNMA	f70  = f69,  f17, f70
	;;
	FNMA	f71  = f69,  f18, f71
	;;
	FMPY	f70  = f70,  f19
	;;
	FNMA	f71  = f70,  f20, f71
	;;
	FMPY	f71  = f71,  f21
	;;
	STFD	[BOFFSET]  = f64, SIZE
	STFD	[BOFFSET2] = f68, SIZE
	;;
	STFD	[BOFFSET]  = f65, SIZE
	STFD	[BOFFSET2] = f69, SIZE
	;;
	STFD	[BOFFSET]  = f66, SIZE
	STFD	[BOFFSET2] = f70, SIZE
	;;
	STFD	[BOFFSET]  = f67, -3 * SIZE
	STFD	[BOFFSET2] = f71, -3 * SIZE
	;;
	adds	C9  = 4 * SIZE, C1
	;;
#endif

#ifdef RN
	LDFD	f32 = [BOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f68  = f68,  f32
	FMPY	f65  = f65,  f32
	FMPY	f69  = f69,  f32
	FMPY	f66  = f66,  f32
	FMPY	f70  = f70,  f32
	FMPY	f67  = f67,  f32
	FMPY	f71  = f71,  f32
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f68, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f69, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2] = f70, SIZE
	;;
	STFD	[AOFFSET]  = f67, -3 * SIZE
	STFD	[AOFFSET2] = f71, -3 * SIZE
	;;
#endif

#ifdef RT
	LDFD	f32 = [BOFFSET]
	;;
	FMPY	f64  = f64,  f32
	FMPY	f68  = f68,  f32
	FMPY	f65  = f65,  f32
	FMPY	f69  = f69,  f32
	FMPY	f66  = f66,  f32
	FMPY	f70  = f70,  f32
	FMPY	f67  = f67,  f32
	FMPY	f71  = f71,  f32
	;;
	STFD	[AOFFSET]  = f64, SIZE
	STFD	[AOFFSET2] = f68, SIZE
	;;
	STFD	[AOFFSET]  = f65, SIZE
	STFD	[AOFFSET2] = f69, SIZE
	;;
	STFD	[AOFFSET]  = f66, SIZE
	STFD	[AOFFSET2] = f70, SIZE
	;;
	STFD	[AOFFSET]  = f67, -3 * SIZE
	STFD	[AOFFSET2] = f71, -3 * SIZE
	;;
#endif
	adds	C9  = 4 * SIZE, C1
	;;

	{ .mmf
	STFD	[C1 ] = f64, SIZE
	STFD	[C9 ] = f68, SIZE
	mov	f64  = f0
	}
	;;
	{ .mmi
	STFD	[C1 ] = f65, SIZE
	STFD	[C9 ] = f69, SIZE
	}
	;;
	{ .mmi
	STFD	[C1 ] = f66, SIZE
	STFD	[C9 ] = f70, SIZE
	}
	;;
	{ .mmi
#ifndef LN
	STFD	[C1 ] = f67, 5 * SIZE
#else
	STFD	[C1 ] = f67, - 3 * SIZE
#endif
	STFD	[C9 ] = f71
	}
	;;
	{ .mmf
	cmp.ne	p6, p0 = 1, I
	}
	;;
	adds	I = -1, I
	;;
	{ .mmi
	shladd	r2 = K, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	sub	L = K, KK
	}
	;;
	{ .mmi
#ifdef RT
	shladd	AORIG = r2, 3, AORIG
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	L = L, BASE_SHIFT, r0
#else
	nop	__LINE__
#endif
	}
	;;
       ;;
	{ .mmi
#if defined(LT) || defined(RN)
	shladd	AOFFSET = L, 3, AOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	add	BOFFSET = L, BOFFSET
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#ifdef LT
	adds	KK =  8, KK
#elif defined LN
	adds	KK = -8, KK
#else
	nop	__LINE__
#endif
	}
	;;
	{ .mmi
#if defined(LT) || defined(RN)
	mov	L = KK
#else
	sub	L = K, KK
#endif
	}
	;;

	mov	f64  = f0
	mov	f65  = f0
	mov	f66  = f0
	mov	f67  = f0
	mov	f68  = f0
	mov	f69  = f0
	mov	f70  = f0
	mov	f71  = f0

	(p6)	br.cond.dptk .L132
	.align 8


.L169:
	{ .mii
#ifdef LN
	shladd	B = K, BASE_SHIFT, B
#elif defined(LT) || defined(RN)
	mov	B =  BOFFSET
#else
	nop	__LINE__
#endif

#ifdef RN
	adds	KK =  1,  KK
#elif defined RT
	adds	KK = -1,  KK
#else
	nop	__LINE__
#endif
	mov	AOFFSET = A
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
	mov	 ar.lc = ARLC
	;;
	mov pr    = PR, -1
       ;;
	mov	ar.pfs = ARPFS
       ;;
	br.ret.sptk.many b0
	EPILOGUE
