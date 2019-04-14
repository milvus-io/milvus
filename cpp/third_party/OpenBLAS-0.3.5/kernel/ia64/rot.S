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
#define PREFETCH_SIZE ( 8 *  8 +  4)
#elif defined(DOUBLE)
#define PREFETCH_SIZE (16 *  8 +  8)
#else
#define PREFETCH_SIZE (32 *  8 + 16)
#endif

#define N	r32
#define X1	r33
#define INCX	r34
#define Y1	r35
#define INCY	r36

#define PREX	r2
#define PREY	r3

#define I	r14
#define J	r15
#define Y2	r16
#define X2	r17

#define INCX16	r18
#define INCY16	r19

#define PR	r30
#define ARLC	r31

#define C	f8
#define S	f9

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	adds	r29 = 16, r12
	shladd	INCX = INCX, BASE_SHIFT, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	shr	I =  N, 4
	(p6) br.ret.spnt.many b0
	}
	.body
	;;
	{ .mmi
#ifdef XDOUBLE
	LDFD	S = [r29]
#else
	nop	__LINE__
#endif
	shladd	INCY = INCY, BASE_SHIFT, r0
	mov	PR = pr
	}
	{ .mmi
	mov	X2 = X1
	mov	Y2 = Y1
	mov	pr.rot= 0
	}
	;;
	{ .mmi
#ifndef XDOUBLE
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
#else
	shladd	INCX16 = INCX, 3, r0
	shladd	INCY16 = INCY, 3, r0
#endif
	mov	ar.ec= 3
	}
	{ .mmi
	adds	I = -1, I
	cmp.eq	p16, p0 = r0, r0
	and	J =  15, N
	}
	;;
	{ .mmi
	adds	PREX = PREFETCH_SIZE * SIZE, X1
	adds	PREY = PREFETCH_SIZE * SIZE, Y1
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p6 ,p0  =   -1, I
	tbit.z	p0, p12 = N, 3
	(p6) br.cond.dpnt  .L15
	}
	;;
	.align 32

.L12:
	{ .mmf
	(p18) STFD	[X2] = f6
	(p16) lfetch.excl.nt1 [PREY], INCY16
	(p18) FMA	f12  = C, f40, f12
	}
	{ .mmf
	(p17) LDFD	f120 = [Y1], INCY
	(p18) add	X2 = X2, INCX
	(p18) FMPY	f6   = S, f94
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f7
	(p16) lfetch.excl.nt1 [PREX], INCX16
	(p18) FNMA	f13  = S, f40, f13
	}
	{ .mmf
	(p16) LDFD	f32  = [X1], INCX
	(p18) add	Y2 = Y2, INCY
	(p18) FMPY	f7   = C, f94
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f10
	(p17) LDFD	f123 = [Y1], INCY
	(p18) FMA	f14  = C, f43, f14
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMPY	f10  = S, f97
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f11
	(p16) LDFD	f35  = [X1], INCX
	(p18) FNMA	f15  = S, f43, f15
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FMPY	f11  = C, f97
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f12
	(p17) LDFD	f126 = [Y1], INCY
	(p18) FMPY	f12  = S, f100
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMA	f6   = C, f46, f6
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f13
	(p16) LDFD	f38  = [X1], INCX
	(p18) FMPY	f13  = C, f100
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FNMA	f7   = S, f46, f7
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f14
	(p16) LDFD	f80  = [Y1], INCY
	(p18) FMPY	f14  = S, f103
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMA	f10  = C, f49, f10
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f15
	(p16) LDFD	f41  = [X1], INCX
	(p18) FMPY	f15  = C, f103
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FNMA	f11  = S, f49, f11
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f6
	(p16) LDFD	f83  = [Y1], INCY
	(p18) FMA	f12  = C, f52, f12
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMPY	f6   = S, f106
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f7
	(p16) LDFD	f44  = [X1], INCX
	(p18) FNMA	f13  = S, f52, f13
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FMPY	f7   = C, f106
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f10
	(p16) LDFD	f86  = [Y1], INCY
	(p18) FMA	f14  = C, f55, f14
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMPY	f10  = S, f109
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f11
	(p16) LDFD	f47  = [X1], INCX
	(p18) FNMA	f15  = S, f55, f15
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FMPY	f11  = C, f109
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f12
	(p16) LDFD	f89  = [Y1], INCY
	(p18) FMPY	f12  = S, f112
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMA	f6   = C, f58, f6
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f13
	(p16) LDFD	f50  = [X1], INCX
	(p18) FMPY	f13  = C, f112
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FNMA	f7   = S, f58, f7
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f14
	(p16) LDFD	f92  = [Y1], INCY
	(p18) FMPY	f14  = S, f115
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMA	f10  = C, f61, f10
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f15
	(p16) LDFD	f53  = [X1], INCX
	(p18) FMPY	f15  = C, f115
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FNMA	f11  = S, f61, f11
	}
	;;
#ifndef XDOUBLE
	{ .mmf
	(p18) STFD	[X2] = f6
	(p16) LDFD	f95  = [Y1], INCY
	(p18) FMA	f12  = C, f64, f12
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMPY	f6   = S, f118
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f7
	(p16) LDFD	f56  = [X1], INCX
	(p18) FNMA	f13  = S, f64, f13
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FMPY	f7   = C, f118
	}
	;;
#else
	{ .mmf
	(p18) STFD	[X2] = f6
	(p16) lfetch.excl.nt1 [PREY], INCY16
	(p18) FMA	f12  = C, f64, f12
	}
	{ .mmf
	(p16) LDFD	f95  = [Y1], INCY
	(p18) add	X2 = X2, INCX
	(p18) FMPY	f6   = S, f118
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f7
	(p16) lfetch.excl.nt1 [PREX], INCX16
	(p18) FNMA	f13  = S, f64, f13
	}
	{ .mmf
	(p16) LDFD	f56  = [X1], INCX
	(p18) add	Y2 = Y2, INCY
	(p18) FMPY	f7   = C, f118
	}
	;;
#endif
	{ .mmf
	(p18) STFD	[X2] = f10
	(p16) LDFD	f98  = [Y1], INCY
	(p18) FMA	f14  = C, f67, f14
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMPY	f10  = S, f121
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f11
	(p16) LDFD	f59  = [X1], INCX
	(p18) FNMA	f15  = S, f67, f15
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FMPY	f11  = C, f121
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f12
	(p16) LDFD	f101 = [Y1], INCY
	(p18) FMPY	f12  = S, f124
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMA	f6   = C, f70, f6
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f13
	(p16) LDFD	f62  = [X1], INCX
	(p18) FMPY	f13  = C, f124
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FNMA	f7   = S, f70, f7
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f14
	(p16) LDFD	f104 = [Y1], INCY
	(p18) FMPY	f14  = S, f127
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p18) FMA	f10  = C, f73, f10
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f15
	(p16) LDFD	f65  = [X1], INCX
	(p18) FMPY	f15  = C, f127
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p18) FNMA	f11  = S, f73, f11
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f6
	(p16) LDFD	f107 = [Y1], INCY
	(p18) FMA	f12  = C, f76, f12
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p17) FMPY	f6   = S, f81
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f7
	(p16) LDFD	f68  = [X1], INCX
	(p18) FNMA	f13  = S, f76, f13
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p17) FMPY	f7   = C, f81
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f10
	(p16) LDFD	f110 = [Y1], INCY
	(p18) FMA	f14  = C, f79, f14
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p17) FMPY	f10  = S, f84
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f11
	(p16) LDFD	f71  = [X1], INCX
	(p18) FNMA	f15  = S, f79, f15
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p17) FMPY	f11  = C, f84
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f12
	(p16) LDFD	f113 = [Y1], INCY
	(p17) FMPY	f12  = S, f87
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p17) FMA	f6   = C, f33, f6
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f13
	(p16) LDFD	f74  = [X1], INCX
	(p17) FMPY	f13  = C, f87
	}
	{ .mmf
	(p18) add	Y2 = Y2, INCY
	nop   __LINE__
	(p17) FNMA	f7   = S, f33, f7
	}
	;;
	{ .mmf
	(p18) STFD	[X2] = f14
	(p16) LDFD	f116 = [Y1], INCY
	(p17) FMPY	f14  = S, f90
	}
	{ .mmf
	(p18) add	X2 = X2, INCX
	nop   __LINE__
	(p17) FMA	f10  = C, f36, f10
	}
	;;
	{ .mmf
	(p18) STFD	[Y2] = f15
	(p16) LDFD	f77  = [X1], INCX
	(p17) FMPY	f15  = C, f90
	}
	{ .mfb
	(p18) add	Y2 = Y2, INCY
	(p17) FNMA	f11  = S, f36, f11
	br.ctop.sptk.few .L12
	}
	;;
	.align 32

.L15:
	{ .mmi
	(p12) LDFD	f40  = [Y1], INCY
	(p12) LDFD	f32  = [X1], INCX
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p12) LDFD	f41  = [Y1], INCY
	(p12) LDFD	f33  = [X1], INCX
	mov	pr = PR, -65474
	}
	;;
	{ .mmb
	(p12) LDFD	f42  = [Y1], INCY
	cmp.eq	p7, p0  =   r0, J
	(p7) br.ret.sptk.many b0
	}
	;;
	{ .mmf
	(p12) LDFD	f43  = [Y1], INCY
	nop   __LINE__
	(p12) FMPY	f6   = S, f40
	}
	;;
	{ .mmf
	(p12) LDFD	f34  = [X1], INCX
	nop   __LINE__
	(p12) FMPY	f7   = C, f40
	}
	;;
	{ .mmf
	(p12) LDFD	f44  = [Y1], INCY
	nop   __LINE__
	(p12) FMPY	f10  = S, f41
	}
	;;
	{ .mmf
	(p12) LDFD	f35  = [X1], INCX
	nop   __LINE__
	(p12) FMPY	f11  = C, f41
	}
	;;
	{ .mmf
	(p12) LDFD	f45  = [Y1], INCY
	nop   __LINE__
	(p12) FMPY	f12  = S, f42
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f6   = C, f32, f6
	}
	;;
	{ .mmf
	(p12) LDFD	f36  = [X1], INCX
	nop   __LINE__
	(p12) FMPY	f13  = C, f42
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FNMA	f7   = S, f32, f7
	}
	;;
	{ .mmf
	(p12) LDFD	f46  = [Y1], INCY
	nop   __LINE__
	(p12) FMPY	f14  = S, f43
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f10  = C, f33, f10
	}
	;;
	{ .mmf
	(p12) LDFD	f37  = [X1], INCX
	nop   __LINE__
	(p12) FMPY	f15  = C, f43
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FNMA	f11  = S, f33, f11
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f6
	(p12) LDFD	f47  = [Y1], INCY
	(p12) FMA	f12  = C, f34, f12
	}
	{ .mfi
	(p12) add	X2 = X2, INCX
	(p12) FMPY	f6   = S, f44
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f7
	(p12) LDFD	f38  = [X1], INCX
	(p12) FNMA	f13  = S, f34, f13
	}
	{ .mmf
	(p12) add	Y2 = Y2, INCY
	nop   __LINE__
	(p12) FMPY	f7   = C, f44
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f10
	(p13) LDFD	f52  = [Y1], INCY
	(p12) FMA	f14  = C, f35, f14
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p12) FMPY	f10  = S, f45
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f11
	(p12) LDFD	f39  = [X1], INCX
	(p12) FNMA	f15  = S, f35, f15
	}
	{ .mmf
	(p12) add	Y2 = Y2, INCY
	nop   __LINE__
	(p12) FMPY	f11  = C, f45
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f12
	(p13) LDFD	f53  = [Y1], INCY
	(p12) FMPY	f12  = S, f46
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p12) FMA	f6   = C, f36, f6
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f13
	(p13) LDFD	f48  = [X1], INCX
	(p12) FMPY	f13  = C, f46
	}
	{ .mmf
	(p12) add	Y2 = Y2, INCY
	nop   __LINE__
	(p12) FNMA	f7   = S, f36, f7
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f14
	(p13) LDFD	f54  = [Y1], INCY
	(p12) FMPY	f14  = S, f47
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p12) FMA	f10  = C, f37, f10
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f15
	(p13) LDFD	f49  = [X1], INCX
	(p12) FMPY	f15  = C, f47
	}
	{ .mfi
	(p12) add	Y2 = Y2, INCY
	(p12) FNMA	f11  = S, f37, f11
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f6
	(p13) LDFD	f55  = [Y1], INCY
	(p12) FMA	f12  = C, f38, f12
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p13) FMPY	f6   = S, f52
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f7
	(p13) LDFD	f50  = [X1], INCX
	(p12) FNMA	f13  = S, f38, f13
	}
	{ .mmf
	(p12) add	Y2 = Y2, INCY
	nop   __LINE__
	(p13) FMPY	f7   = C, f52
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f10
	(p14) LDFD	f58  = [Y1], INCY
	(p12) FMA	f14  = C, f39, f14
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p13) FMPY	f10  = S, f53
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f11
	(p13) LDFD	f51  = [X1], INCX
	(p12) FNMA	f15  = S, f39, f15
	}
	{ .mfi
	(p12) add	Y2 = Y2, INCY
	(p13) FMPY	f11  = C, f53
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f12
	(p14) LDFD	f59  = [Y1], INCY
	(p13) FMPY	f12  = S, f54
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p13) FMA	f6   = C, f48, f6
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f13
	(p14) LDFD	f56  = [X1], INCX
	(p13) FMPY	f13  = C, f54
	}
	{ .mmf
	(p12) add	Y2 = Y2, INCY
	nop   __LINE__
	(p13) FNMA	f7   = S, f48, f7
	}
	;;
	{ .mmf
	(p12) STFD	[X2] = f14
	(p15) LDFD	f61  = [Y1], INCY
	(p13) FMPY	f14  = S, f55
	}
	{ .mmf
	(p12) add	X2 = X2, INCX
	nop   __LINE__
	(p13) FMA	f10  = C, f49, f10
	}
	;;
	{ .mmf
	(p12) STFD	[Y2] = f15
	(p14) LDFD	f57  = [X1], INCX
	(p13) FMPY	f15  = C, f55
	}
	{ .mmf
	(p12) add	Y2 = Y2, INCY
	nop   __LINE__
	(p13) FNMA	f11  = S, f49, f11
	}
	;;
	{ .mmf
	(p13) STFD	[X2] = f6
	nop   __LINE__
	(p13) FMA	f12  = C, f50, f12
	}
	{ .mmf
	(p13) add	X2 = X2, INCX
	nop   __LINE__
	(p14) FMPY	f6   = S, f58
	}
	;;
	{ .mmf
	(p13) STFD	[Y2] = f7
	(p15) LDFD	f60  = [X1], INCX
	(p13) FNMA	f13  = S, f50, f13
	}
	{ .mmf
	(p13) add	Y2 = Y2, INCY
	nop   __LINE__
	(p14) FMPY	f7   = C, f58
	}
	;;
	{ .mmf
	(p13) STFD	[X2] = f10
	nop   __LINE__
	(p13) FMA	f14  = C, f51, f14
	}
	{ .mmf
	(p13) add	X2 = X2, INCX
	nop   __LINE__
	(p14) FMPY	f10  = S, f59
	}
	;;
	{ .mmf
	(p13) STFD	[Y2] = f11
	nop   __LINE__
	(p13) FNMA	f15  = S, f51, f15
	}
	{ .mmf
	(p13) add	Y2 = Y2, INCY
	nop   __LINE__
	(p14) FMPY	f11  = C, f59
	}
	;;
	{ .mmf
	(p13) STFD	[X2] = f12
	nop   __LINE__
	(p14) FMA	f6   = C, f56, f6
	}
	{ .mmf
	(p13) add	X2 = X2, INCX
	nop   __LINE__
	(p15) FMPY	f12  = S, f61
	}
	;;
	{ .mmf
	(p13) STFD	[Y2] = f13
	nop   __LINE__
	(p14) FNMA	f7   = S, f56, f7
	}
	{ .mmf
	(p13) add	Y2 = Y2, INCY
	nop   __LINE__
	(p15) FMPY	f13  = C, f61
	}
	;;
	{ .mmf
	(p13) STFD	[X2] = f14
	(p13) add	X2 = X2, INCX
	(p14) FMA	f10  = C, f57, f10
	}
	;;
	{ .mmf
	(p13) STFD	[Y2] = f15
	(p13) add	Y2 = Y2, INCY
	(p14) FNMA	f11  = S, f57, f11
	}
	;;
	{ .mmf
	(p14) STFD	[X2] = f6
	(p14) add	X2 = X2, INCX
	(p15) FMA	f12  = C, f60, f12
	}
	;;
	{ .mmf
	(p14) STFD	[Y2] = f7
	(p14) add	Y2 = Y2, INCY
	(p15) FNMA	f13  = S, f60, f13
	}
	;;
	{ .mmi
	(p14) STFD	[X2] = f10
	(p14) add	X2 = X2, INCX
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[Y2] = f11
	(p14) add	Y2 = Y2, INCY
	nop   __LINE__
	}
	;;
	{ .mmi
	(p15) STFD	[X2] = f12
	(p15) add	X2 = X2, INCX
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[Y2] = f13
	(p15) add	Y2 = Y2, INCY
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

