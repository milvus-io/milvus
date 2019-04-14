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

#ifdef XDOUBLE
#define PREFETCH_SIZE ( 8 * 16 +  4)
#elif defined(DOUBLE)
#define PREFETCH_SIZE (16 * 16 +  8)
#else
#define PREFETCH_SIZE (32 * 16 + 16)
#endif

#if   !defined(USE_MIN) &&  defined(USE_ABS)
#define FMAX	famax
#elif !defined(USE_MIN) && !defined(USE_ABS)
#define FMAX	fmax
#elif  defined(USE_MIN) &&  defined(USE_ABS)
#define FMAX	famin
#else
#define FMAX	fmin
#endif

#define RET	r8

#define N	r32
#define DX	r33
#define INCX	r34

#define PRE1	r2
#define J	r14
#define K	r15
#define X2	r16
#define X3	r17
#define INCX5	r18
#define INCX16	r19

#define DMAX1	f8
#define DMAX2	f9
#define DMAX3	f10
#define DMAX4	f11
#define DMAX5	f12
#define DMAX6	f13
#define DMAX7	f14
#define DMAX8	f15

#define PR	r30
#define ARLC	r31

	PROLOGUE
	.prologue
	PROFCODE
	{ .mfi
	mov	RET = 0
	mov	DMAX1  = f0
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	;;
	.body

#ifdef F_INTERFACE
	{ .mmi
	LDINT	N = [N]
	LDINT	INCX = [INCX]
	nop.i 0
	}
	;;
#ifndef USE64BITINT
	{ .mii
	nop.m 0
	sxt4	N = N
	sxt4	INCX = INCX
	}
	;;
#endif
#endif
	{ .mii
	mov	PR = pr
	cmp.ge	p6, p0 = 0, INCX
	}
	{ .mbb
	cmp.ge	p8, p0 = 0, N
	(p8) br.ret.sptk.many b0
	(p6) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	LDFD	DMAX1 = [DX]
	shladd	INCX = INCX, BASE_SHIFT, r0
	mov	pr.rot= 0
	}
	;;
	{ .mmf
	add	DX = DX, INCX
	adds	K = -1, N
	mov	DMAX2 = DMAX1
	}
	;;
	{ .mfi
	shladd	X2    = INCX, 2, DX
	mov	DMAX5 = DMAX1
	shr	J =  K, 4
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	nop.m 0
	mov	DMAX6 = DMAX1
	}
	;;
	{ .mfi
	shladd	INCX5 = INCX, 2, INCX
	mov	DMAX3 = DMAX1
	mov	ar.ec= 4
	}
	{ .mmf
#ifdef XDOUBLE
	shladd	INCX16= INCX, 3, r0
#else
	shladd	INCX16= INCX, 4, r0
#endif
	adds	J = -1, J
	mov	DMAX7 = DMAX1
	}
	;;
	{ .mfi
	adds	PRE1 = PREFETCH_SIZE * SIZE, DX
	mov	DMAX4 = DMAX1
	mov	ar.lc = J
	}
	{ .mfb
	cmp.eq	p7 ,p0  =  -1, J
	mov	DMAX8 = DMAX1
	(p7) br.cond.dpnt  .L15
	}
	.align 32
	;;
.L10:
	{ .mmf
	(p16) lfetch.nt1  [PRE1], INCX16
	(p16) LDFD	f32 = [DX], INCX
	(p19) FMAX DMAX1 = f35, DMAX1
	}
	{ .mmf
	(p16) LDFD	f48 = [X2], INCX
	nop.m 0
	(p19) FMAX DMAX5 = f51, DMAX5
	}
	;;
	{ .mmf
	(p16) LDFD	f36 = [DX], INCX
	nop.m 0
	(p19) FMAX DMAX2 = f39, DMAX2
	}
	{ .mmf
	(p16) LDFD	f52 = [X2], INCX
	nop.m 0
	(p19) FMAX DMAX6 = f55, DMAX6
	}
	;;
	{ .mmf
	(p16) LDFD	f40 = [DX], INCX
	nop.m 0
	(p19) FMAX DMAX3 = f43, DMAX3
	}
	{ .mmf
	(p16) LDFD	f56 = [X2], INCX
	nop.m 0
	(p19) FMAX DMAX7 = f59, DMAX7
	}
	;;
	{ .mmf
	(p16) LDFD	f44 = [DX], INCX5
	nop.m 0
	(p19) FMAX DMAX4 = f47, DMAX4
	}
	{ .mmf
	(p16) LDFD	f60 = [X2], INCX5
	nop.m 0
	(p19) FMAX DMAX8 = f63, DMAX8
	}
	;;
	{ .mmf
#ifdef XDOUBLE
	(p16) lfetch.nt1  [PRE1], INCX16
#endif
	(p16) LDFD	f64 = [DX], INCX
#ifndef XDOUBLE
	nop.m 0
#endif
	(p19) FMAX DMAX1 = f67, DMAX1
	}
	{ .mmf
	(p16) LDFD	f80 = [X2], INCX
	nop.m 0
	(p19) FMAX DMAX5 = f83, DMAX5
	}
	;;
	{ .mmf
	(p16) LDFD	f68 = [DX], INCX
	nop.m 0
	(p19) FMAX DMAX2 = f71, DMAX2
	}
	{ .mmf
	(p16) LDFD	f84 = [X2], INCX
	nop.m 0
	(p19) FMAX DMAX6 = f87, DMAX6
	}
	;;
	{ .mmf
	(p16) LDFD	f72 = [DX], INCX
	nop.m 0
	(p19) FMAX DMAX3 = f75, DMAX3
	}
	{ .mmf
	(p16) LDFD	f88 = [X2], INCX
	nop.m 0
	(p19) FMAX DMAX7 = f91, DMAX7
	}
	;;
	{ .mmf
	(p16) LDFD	f76 = [DX], INCX5
	nop.m 0
	(p19) FMAX DMAX4 = f79, DMAX4
	}
	{ .mfb
	(p16) LDFD	f92 = [X2], INCX5
	(p19) FMAX DMAX8 = f95, DMAX8
	br.ctop.sptk.few .L10
	}
	.align 32
	;;
.L15:
	and	J =  15, K
	tbit.z	p0, p12 = K, 3
	mov	X3 = DX
	;;
	{ .mmi
	(p12) LDFD	f32 = [DX], INCX
	(p12) LDFD	f36 = [X2], INCX
	tbit.z	p0, p13 = K, 2
	}
	{ .mib
	cmp.eq	p8 ,p0  =   r0, J
	tbit.z	p0, p14 = K, 1
	(p8) br.cond.dpnt  .L99
	}
	;;
	{ .mmi
	(p12) LDFD	f33 = [DX], INCX
	(p12) LDFD	f37 = [X2], INCX
	tbit.z	p0, p15 = K, 0
	}
	;;
	{ .mmi
	(p12) LDFD	f34 = [DX], INCX
	(p12) LDFD	f38 = [X2], INCX
	(p12) shladd X3 = INCX, 3, X3
	}
	;;
	{ .mmi
	(p12) LDFD	f35 = [DX], INCX5
	(p12) LDFD	f39 = [X2], INCX5
	(p13) shladd X3 = INCX, 2, X3
	}
	;;
	{ .mmi
	(p13) LDFD	f40 = [DX], INCX
	(p14) LDFD	f44 = [X3], INCX
	nop.i 0
	}
	;;
	{ .mmi
	(p13) LDFD	f41 = [DX], INCX
	(p14) LDFD	f45 = [X3], INCX
	nop.i 0
	}
	;;
	{ .mmf
	(p13) LDFD	f42 = [DX], INCX
	nop.m 0
	(p12) FMAX DMAX1 = f32, DMAX1
	}
	{ .mmf
	(p15) LDFD	f46 = [X3], INCX
	nop.m 0
	(p12) FMAX DMAX5 = f36, DMAX5
	}
	;;
	{ .mmf
	(p13) LDFD	f43 = [DX], INCX
	nop.m 0
	(p12) FMAX DMAX2 = f33, DMAX2
	}
	(p12) FMAX DMAX6 = f37, DMAX6
	(p12) FMAX DMAX3 = f34, DMAX3
	(p12) FMAX DMAX7 = f38, DMAX7
	(p12) FMAX DMAX4 = f35, DMAX4
	(p12) FMAX DMAX8 = f39, DMAX8
	;;
	(p13) FMAX DMAX1 = f40, DMAX1
	(p14) FMAX DMAX5 = f44, DMAX5
	(p13) FMAX DMAX2 = f41, DMAX2
	(p14) FMAX DMAX6 = f45, DMAX6
	(p13) FMAX DMAX3 = f42, DMAX3
	(p15) FMAX DMAX7 = f46, DMAX7
	(p13) FMAX DMAX4 = f43, DMAX4
	;;
	.align 32

.L99:
	{ .mfi
	nop.m 0
	FMAX	DMAX1 = DMAX5, DMAX1
	mov	ar.lc = ARLC
	}
	{ .mmf
	nop.m 0
	nop.m 0
	FMAX	DMAX2 = DMAX6, DMAX2
	}
	;;
	{ .mfi
	nop.m 0
	FMAX	DMAX3 = DMAX7, DMAX3
	mov	pr = PR, -65474
	}
	{ .mmf
	nop.m 0
	nop.m 0
	FMAX	DMAX4 = DMAX8, DMAX4
	}
	;;
	{ .mmf
 	FMAX	DMAX1 = DMAX2, DMAX1
	}
	{ .mmf
	FMAX	DMAX3 = DMAX4, DMAX3
	}
	;;
#ifndef USE_ABS
	{ .mfb
 	FMAX	DMAX1 = DMAX3, DMAX1
	br.ret.sptk.many b0
	}
#else
	{ .mmf
 	FMAX	DMAX1 = DMAX3, DMAX1
	}
	;;
	{ .mfb
 	fabs	DMAX1 = DMAX1
	br.ret.sptk.many b0
	}
#endif
	;;
	EPILOGUE



