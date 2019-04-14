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

#define IMAX1	r8
#define IMAX2	r26
#define IMAX3	r27
#define IMAX4	r28

#define PRE1	r2

#define N	r14
#define X1	r15
#define INCX	r16

#define I	r17
#define X2	r18
#define INCX5	r19
#define INCX16	r20
#define CURRENT	r21

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

	{ .mmi
	mov	IMAX1 = 0
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	;;
	.body

#ifdef F_INTERFACE
	{ .mmi
	LDINT	N    = [r32]
	LDINT	INCX = [r34]
	mov	X1   = r33
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
#else
	{ .mmi
	mov	N    = r32
	mov	X1   = r33
	mov	INCX = r34
	}
	;;
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
	LDFD	DMAX1 = [X1]
	shladd	INCX = INCX, BASE_SHIFT, r0
	mov	pr.rot= 0
	}
	;;
	mov	IMAX1 = 1
	mov	IMAX2 = 1
	mov	IMAX3 = 1
	mov	IMAX4 = 1
	mov	CURRENT = 1
	adds	N = -1, N
	;;

	{ .mmf
	add	X1 = X1, INCX
	mov	DMAX2 = DMAX1
	}
	;;
	{ .mmf
	shladd	X2    = INCX, 2, X1
	}
	{ .mfi
	cmp.eq	p16, p0 = r0, r0
	shr	I =  N, 4
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
	adds	I = -1, I
	}
	;;
	tbit.z	p0, p7 = N, 3
	;;
	{ .mfi
	adds	PRE1 = PREFETCH_SIZE * SIZE, X1
	mov	DMAX4 = DMAX1
	mov	ar.lc = I
	}
	{ .mfb
	cmp.eq	p6 ,p0  =  -1, I
	(p6) br.cond.dpnt  .L15
	}
	.align 32
	;;
.L10:
	{ .mmf
	(p16) lfetch.nt1  [PRE1], INCX16
	(p16) LDFD	f32 = [X1], INCX
	(p19) fcmp.neq.unc p12, p0 = DMAX1, DMAX5
	}
	{ .mmf
	(p8)  adds	IMAX1 =  1, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX5 = f67, DMAX1
	}
	;;
	{ .mmf
	(p16) LDFD	f36 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p13, p0 = DMAX2, DMAX6
	}
	{ .mmf
	(p9)  adds	IMAX2 =  2, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX6 = f71, DMAX2
	}
	;;
	{ .mmf
	(p16) LDFD	f40 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p14, p0 = DMAX3, DMAX7
	}
	{ .mmf
	(p10) adds	IMAX3 =  3, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX7 = f75, DMAX3
	}
	;;
	{ .mmf
	(p16) LDFD	f44 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p15, p0 = DMAX4, DMAX8
	}
	{ .mmf
	(p11) adds	IMAX4 =  4, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX8 = f79, DMAX4
	}
	;;
	{ .mmf
	(p16) LDFD	f48 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p8,  p0 = DMAX1, DMAX5
	}
	{ .mmf
	(p12) adds	IMAX1 =  5, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX1 = f83, DMAX5
	}
	;;
	{ .mmf
	(p16) LDFD	f52 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p9,  p0 = DMAX2, DMAX6
	}
	{ .mmf
	(p13) adds	IMAX2 =  6, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX2 = f87, DMAX6
	}
	;;
	{ .mmf
	(p16) LDFD	f56 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p10, p0 = DMAX3, DMAX7
	}
	{ .mmf
	(p14) adds	IMAX3 =  7, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX3 = f91, DMAX7
	}
	;;
	{ .mmf
	(p16) LDFD	f60 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p11, p0 = DMAX4, DMAX8
	}
	{ .mmf
	(p15) adds	IMAX4 =  8, CURRENT
	nop   __LINE__
	(p19) FMAX DMAX4 = f95, DMAX8
	}
	;;
	{ .mmf
#ifdef XDOUBLE
	(p16) lfetch.nt1  [PRE1], INCX16
#endif
	(p16) LDFD	f64 = [X1], INCX
#ifndef XDOUBLE
	nop   __LINE__
#endif
	(p19) fcmp.neq.unc p12, p0 = DMAX1, DMAX5
	}
	{ .mmf
	(p8)  adds	IMAX1 =  9, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX5 = f34, DMAX1
	}
	;;
	{ .mmf
	(p16) LDFD	f68 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p13, p0 = DMAX2, DMAX6
	}
	{ .mmf
	(p9)  adds	IMAX2 = 10, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX6 = f38, DMAX2
	}
	;;
	{ .mmf
	(p16) LDFD	f72 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p14, p0 = DMAX3, DMAX7
	}
	{ .mmf
	(p10) adds	IMAX3 = 11, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX7 = f42, DMAX3
	}
	;;
	{ .mmf
	(p16) LDFD	f76 = [X1], INCX
	nop   __LINE__
	(p19) fcmp.neq.unc p15, p0 = DMAX4, DMAX8
	}
	{ .mmf
	(p11) adds	IMAX4 = 12, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX8 = f46, DMAX4
	}
	;;
	{ .mmf
	(p16) LDFD	f80 = [X1], INCX
	nop   __LINE__
	(p18) fcmp.neq.unc p8,  p0 = DMAX1, DMAX5
	}
	{ .mmf
	(p12) adds	IMAX1 = 13, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX1 = f50, DMAX5
	}
	;;
	{ .mmf
	(p16) LDFD	f84 = [X1], INCX
	nop   __LINE__
	(p18) fcmp.neq.unc p9,  p0 = DMAX2, DMAX6
	}
	{ .mmf
	(p13) adds	IMAX2 = 14, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX2 = f54, DMAX6
	}
	;;
	{ .mmf
	(p16) LDFD	f88 = [X1], INCX
	nop   __LINE__
	(p18) fcmp.neq.unc p10, p0 = DMAX3, DMAX7
	}
	{ .mmf
	(p14) adds	IMAX3 = 15, CURRENT
	nop   __LINE__
	(p18) FMAX DMAX3 = f58, DMAX7
	}
	;;
	{ .mmf
	(p16) LDFD	f92 = [X1], INCX
	(p15) adds	IMAX4 = 16, CURRENT
	(p18) fcmp.neq.unc p11, p0 = DMAX4, DMAX8
	}
	{ .mfb
	(p19) adds CURRENT = 16, CURRENT
	(p18) FMAX DMAX4 = f62, DMAX8
	br.ctop.sptk.few .L10
	}
	;;
	.align 32

.L15:
	{ .mmi
	(p7) LDFD	f32 = [X1], INCX
	and	I = 15, N
	cmp.ne p14, p0 = r0, r0
	}
	;;
	{ .mmb
	(p7) LDFD	f33 = [X1], INCX
	cmp.eq	p6, p0 = 0, I
	(p6) br.cond.dptk .L999
	}
	;;
	{ .mmi
	(p7) LDFD	f34 = [X1], INCX
	;;
	(p7) LDFD	f35 = [X1], INCX
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	(p7) LDFD	f36 = [X1], INCX
	;;
	(p7) LDFD	f37 = [X1], INCX
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mfi
	(p7) LDFD	f38 = [X1], INCX
	(p7) FMAX DMAX5 = f32, DMAX1
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmf
	(p7) LDFD	f39 = [X1], INCX
	nop  __LINE__
	(p7) FMAX DMAX6 = f33, DMAX2
	}
	;;
	{ .mmf
	(p13) LDFD	f40 = [X1], INCX
	nop  __LINE__
	(p7) FMAX DMAX7 = f34, DMAX3
	}
	;;
	{ .mmf
	(p13) LDFD	f41 = [X1], INCX
	nop  __LINE__
	(p7) FMAX DMAX8 = f35, DMAX4
	}
	;;
	{ .mmf
	(p13) LDFD	f42 = [X1], INCX
	nop  __LINE__
	(p7) fcmp.neq.unc p8,  p0 = DMAX1, DMAX5
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p7) FMAX DMAX1 = f36, DMAX5
	}
	;;
	{ .mmf
	(p13) LDFD	f43 = [X1], INCX
	nop  __LINE__
	(p7) fcmp.neq.unc p9,  p0 = DMAX2, DMAX6
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p7) FMAX DMAX2 = f37, DMAX6
	}
	;;
	{ .mmf
	(p14) LDFD	f44 = [X1], INCX
	nop  __LINE__
	(p7) fcmp.neq.unc p10, p0 = DMAX3, DMAX7
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p7) FMAX DMAX3 = f38, DMAX7
	}
	;;
	{ .mmf
	(p14) LDFD	f45 = [X1], INCX
	nop  __LINE__
	(p7) fcmp.neq.unc p11, p0 = DMAX4, DMAX8
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p7) FMAX DMAX4 = f39, DMAX8
	}
	;;
	{ .mmf
	(p15) LDFD	f46 = [X1], INCX
	(p8)  adds	IMAX1 =  1, CURRENT
	(p7) fcmp.neq.unc p8,  p0 = DMAX1, DMAX5
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) FMAX DMAX5 = f40, DMAX1
	}
	{ .mmf
	(p9)  adds	IMAX2 =  2, CURRENT
	nop  __LINE__
	(p7) fcmp.neq.unc p9,  p0 = DMAX2, DMAX6
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) FMAX DMAX6 = f41, DMAX2
	}
	{ .mmf
	(p10) adds	IMAX3 =  3, CURRENT
	nop  __LINE__
	(p7) fcmp.neq.unc p10, p0 = DMAX3, DMAX7
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) FMAX DMAX7 = f42, DMAX3
	}
	{ .mmf
	(p11) adds	IMAX4 =  4, CURRENT
	nop  __LINE__
	(p7) fcmp.neq.unc p11, p0 = DMAX4, DMAX8
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) FMAX DMAX8 = f43, DMAX4
	}
	;;
	{ .mmf
	(p8)  adds	IMAX1 =  5, CURRENT
	nop  __LINE__
	(p13) fcmp.neq.unc p8,  p0 = DMAX1, DMAX5
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) mov DMAX1 = DMAX5
	}
	{ .mmf
	(p9)  adds	IMAX2 =  6, CURRENT
	nop  __LINE__
	(p13) fcmp.neq.unc p9,  p0 = DMAX2, DMAX6
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) mov DMAX2 = DMAX6
	}
	{ .mmf
	(p10) adds	IMAX3 =  7, CURRENT
	nop  __LINE__
	(p13) fcmp.neq.unc p10, p0 = DMAX3, DMAX7
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p13) mov DMAX3 = DMAX7
	}
	{ .mmf
	(p11) adds	IMAX4 =  8, CURRENT
	nop  __LINE__
	(p13) fcmp.neq.unc p11, p0 = DMAX4, DMAX8
	}
	{ .mmf
	(p7) adds CURRENT = 8, CURRENT
	nop  __LINE__
	(p13) mov DMAX4 = DMAX8
	}
	;;
	{ .mmf
	(p8)  adds	IMAX1 =  1, CURRENT
	nop  __LINE__
	(p14) FMAX DMAX5 = f44, DMAX1
	}
	{ .mmf
	(p9)  adds	IMAX2 =  2, CURRENT
	(p10) adds	IMAX3 =  3, CURRENT
	(p14) FMAX DMAX6 = f45, DMAX2
	}
	{ .mmf
	(p11) adds	IMAX4 =  4, CURRENT
	(p13) adds CURRENT = 4, CURRENT
	(p15) FMAX DMAX7 = f46, DMAX3
	}
	;;
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p14) fcmp.neq.unc p8,  p0 = DMAX5, DMAX1
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p14) mov DMAX1 = DMAX5
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p14) fcmp.neq.unc p9,  p0 = DMAX6, DMAX2
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p14) mov DMAX2 = DMAX6
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p15) fcmp.neq.unc p10, p0 = DMAX7, DMAX3
	}
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	(p15) mov DMAX3 = DMAX7
	}
	;;
.L999:
	{ .mmf
	(p8) adds	IMAX1 =  1, CURRENT
	nop  __LINE__
	FMAX DMAX5 = DMAX2, DMAX1
	}
	{ .mmf
	(p9) adds	IMAX2 =  2, CURRENT
	(p14) adds CURRENT = 2, CURRENT
	FMAX DMAX6 = DMAX4, DMAX3
	}
	;;
	{ .mmf
	nop  __LINE__
	nop  __LINE__
	fcmp.neq p12, p0 = DMAX5, DMAX1
	}
	{ .mmf
	(p10) adds	IMAX3 =  1, CURRENT
	nop  __LINE__
	fcmp.neq p13, p0 = DMAX6, DMAX3
	}
	;;
	{ .mmf
	(p12) mov	IMAX1 = IMAX2
	(p13) mov	IMAX3 = IMAX4
	FMAX DMAX1 = DMAX6, DMAX5
	}
	;;
	{ .mfi
	nop	__LINE__
	fcmp.neq p12, p0 = DMAX1, DMAX5
	mov	ar.lc = ARLC
	}
	;;
	{ .mib
	(p12) mov	IMAX1 = IMAX3
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

