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

#define PREFETCH_SIZE (4 * 24)

#ifdef F_INTERFACE
#define N	r33
#define X1	r34
#define INCX	r35
#define Y1	r36
#define INCY	r37
#else
#define N	r32
#define X1	r33
#define INCX	r34
#define Y1	r35
#define INCY	r36
#endif

#define PREX1	r2
#define PREY1	r3

#define I	r14
#define J	r15
#define Y2	r16
#define X2	r17

#define INCX4	r24
#define INCY4	r25

#define PR	r30
#define ARLC	r31

	PROLOGUE
	.prologue
	PROFCODE
	{ .mfi
	nop	__LINE__
	mov	f8  = f0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mfi
	mov	r26 = 1
	mov	f9  = f0
	nop	__LINE__
	}
	;;
	.body
#ifdef F_INTERFACE
	LDINT	N    = [N]
	LDINT	INCX = [INCX]
	LDINT	INCY = [INCY]
	;;
#ifndef USE64BITINT
	sxt4	N    = N
	sxt4	INCX = INCX
	sxt4	INCY = INCY
	;;
#endif
	cmp.le	p0, p6 = r0, INCX
	cmp.le	p0, p7 = r0, INCY
	sub	r26 = r26, N
	;;
	setf.sig f32 = r26
	setf.sig f33 = INCX
	setf.sig f34 = INCY
	;;
	xmpy.l f33 = f32, f33
	xmpy.l f34 = f32, f34
	;;
	getf.sig r26 = f33
	getf.sig r27 = f34
	;;
	shl	r26 = r26, ZBASE_SHIFT
	shl	r27 = r27, ZBASE_SHIFT
	;;
	(p6) add X1 = r26, X1
	(p7) add Y1 = r27, Y1
	;;
#endif
	{ .mfi
	adds	PREX1 = (PREFETCH_SIZE + 2) * SIZE, X1
	mov	f10 = f0
	mov	PR = pr
	}
	{ .mfb
	cmp.lt	p0, p6 = r0, N
	mov	f11 = f0
	(p6) br.cond.spnt .L1000
	}
	;;
	{ .mii
	adds	PREY1 = (PREFETCH_SIZE + 2) * SIZE, Y1
	shl	INCX = INCX, ZBASE_SHIFT
	shl	INCY = INCY, ZBASE_SHIFT
	}
	;;
	{ .mfi
	add	X2  = SIZE, X1
	mov	f12 = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	Y2  = SIZE, Y1
	mov	f13 = f0
	shr	I =  N, 3
	}
	;;
	{ .mfi
	adds	I = -1, I
	mov	f14 = f0
	mov	ar.ec= 3
	}
	{ .mmf
	shladd	INCX4 = INCX, 2, r0
	shladd	INCY4 = INCY, 2, r0
	mov	f15 = f0
	}
	;;
	{ .mmi
	and	J =  7, N
	cmp.eq	p16, p0 = r0, r0
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p6 ,p0  =   -1, I
	tbit.nz	p12, p0 = N, 2
	(p6) br.cond.dpnt  .L215
	}
	;;
	.align 32

.L212:
	{ .mmf
	(p16) lfetch.nt1 [PREX1], INCX4
	(p16) LDFD	f80  = [X1], INCX
	(p18) FMA	f8  = f34,  f82,  f8
	}
	{ .mmf
	(p16) LDFD	f83  = [X2], INCX
	nop   __LINE__
	(p18) FMA	f9  = f37,  f82,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f32  = [Y1], INCY
	(p16) LDFD	f35  = [Y2], INCY
	(p18) FMA	f10 = f34,  f85,  f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f11 = f37,  f85,  f11
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [X1], INCX
	(p16) LDFD	f89  = [X2], INCX
	(p18) FMA	f12 = f40,  f88,  f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f13 = f43,  f88,  f13
	}
	;;
	{ .mmf
	(p16) LDFD	f38  = [Y1], INCY
	(p16) LDFD	f41  = [Y2], INCY
	(p18) FMA	f14 = f40,  f91,  f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f15 = f43,  f91,  f15
	}
	;;
	{ .mmf
	(p16) LDFD	f92  = [X1], INCX
	(p16) LDFD	f95  = [X2], INCX
	(p18) FMA	f8  = f46,  f94,  f8
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f9  = f49,  f94,  f9
	}
	;;
	{ .mmf
	(p16) lfetch.nt1 [PREY1], INCY4
	(p16) LDFD	f44  = [Y1], INCY
	(p18) FMA	f10 = f46,  f97,  f10
	}
	{ .mmf
	(p16) LDFD	f47  = [Y2], INCY
	nop   __LINE__
	(p18) FMA	f11 = f49,  f97,  f11
	}
	;;
	{ .mmf
	(p16) LDFD	f98  = [X1], INCX
	(p16) LDFD	f101 = [X2], INCX
	(p18) FMA	f12 = f52,  f100, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f13 = f55,  f100, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f50  = [Y1], INCY
	(p16) LDFD	f53  = [Y2], INCY
	(p18) FMA	f14 = f52,  f103, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f15 = f55,  f103, f15
	}
	;;
	{ .mmf
	(p16) lfetch.nt1 [PREX1], INCX4
	(p16) LDFD	f104 = [X1], INCX
	(p18) FMA	f8  = f58,  f106,  f8
	}
	{ .mmf
	(p16) LDFD	f107 = [X2], INCX
	nop   __LINE__
	(p18) FMA	f9  = f61,  f106,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f56  = [Y1], INCY
	(p16) LDFD	f59  = [Y2], INCY
	(p18) FMA	f10 = f58,  f109,  f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f11 = f61,  f109,  f11
	}
	;;
	{ .mmf
	(p16) LDFD	f110 = [X1], INCX
	(p16) LDFD	f113 = [X2], INCX
	(p18) FMA	f12 = f64,  f112,  f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f13 = f67,  f112,  f13
	}
	;;
	{ .mmf
	(p16) LDFD	f62  = [Y1], INCY
	(p16) LDFD	f65  = [Y2], INCY
	(p18) FMA	f14 = f64,  f115,  f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f15 = f67,  f115,  f15
	}
	;;
	{ .mmf
	(p16) lfetch.nt1 [PREY1], INCY4
	(p16) LDFD	f116 = [X1], INCX
	(p18) FMA	f8  = f70,  f118,  f8
	}
	{ .mmf
	(p16) LDFD	f119 = [X2], INCX
	nop   __LINE__
	(p18) FMA	f9  = f73,  f118,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f68  = [Y1], INCY
	(p16) LDFD	f71  = [Y2], INCY
	(p18) FMA	f10 = f70,  f121,  f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f11 = f73,  f121,  f11
	}
	;;
	{ .mmf
	(p16) LDFD	f122 = [X1], INCX
	(p16) LDFD	f125 = [X2], INCX
	(p18) FMA	f12 = f76,  f124,  f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p18) FMA	f13 = f79,  f124,  f13
	}
	;;
	{ .mmf
	(p16) LDFD	f74  = [Y1], INCY
	(p16) LDFD	f77  = [Y2], INCY
	(p18) FMA	f14 = f76,  f127,  f14
	}
	{ .mfb
	nop   __LINE__
	(p18) FMA	f15 = f79,  f127,  f15
	br.ctop.sptk.few .L212
	}
	;;
	.align 32

.L215:
	{ .mmi
	(p12) LDFD	f48  = [X1], INCX
	(p12) LDFD	f49  = [X2], INCX
	cmp.eq	p7, p0  =   r0, J
	}
	;;
	{ .mmb
	(p12) LDFD	f32  = [Y1], INCY
	(p12) LDFD	f33  = [Y2], INCY
	(p7) br.cond.dptk .L999
	}
	;;
	{ .mmi
	(p12) LDFD	f50  = [X1], INCX
	(p12) LDFD	f51  = [X2], INCX
	tbit.nz	p13, p0 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f34  = [Y1], INCY
	(p12) LDFD	f35  = [Y2], INCY
	nop   __LINE__
	}
	;;
	{ .mmi
	(p12) LDFD	f52  = [X1], INCX
	(p12) LDFD	f53  = [X2], INCX
	tbit.nz	p14, p0 = N, 0
	}
	;;
	{ .mmi
	(p12) LDFD	f36  = [Y1], INCY
	(p12) LDFD	f37  = [Y2], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p12) LDFD	f54  = [X1], INCX
	(p12) LDFD	f55  = [X2], INCX
	(p12) FMA	f8  = f32, f48, f8
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f9  = f33, f48, f9
	}
	;;
	{ .mmf
	(p12) LDFD	f38  = [Y1], INCY
	(p12) LDFD	f39  = [Y2], INCY
	(p12) FMA	f10 = f32, f49, f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f11 = f33, f49, f11
	}
	;;
	{ .mmf
	(p13) LDFD	f56  = [X1], INCX
	(p13) LDFD	f57  = [X2], INCX
	(p12) FMA	f12 = f34, f50, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f13 = f35, f50, f13
	}
	;;
	{ .mmf
	(p13) LDFD	f40  = [Y1], INCY
	(p13) LDFD	f41  = [Y2], INCY
	(p12) FMA	f14 = f34, f51, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f15 = f35, f51, f15
	}
	;;
	{ .mmf
	(p13) LDFD	f58  = [X1], INCX
	(p13) LDFD	f59  = [X2], INCX
	(p12) FMA	f8  = f36, f52, f8
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f9  = f37, f52, f9
	}
	;;
	{ .mmf
	(p13) LDFD	f42  = [Y1], INCY
	(p13) LDFD	f43  = [Y2], INCY
	(p12) FMA	f10 = f36, f53, f10
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f11 = f37, f53, f11
	}
	;;
	{ .mmf
	(p14) LDFD	f60  = [X1]
	(p14) LDFD	f61  = [X2]
	(p12) FMA	f12 = f38, f54, f12
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f13 = f39, f54, f13
	}
	;;
	{ .mmf
	(p14) LDFD	f44  = [Y1]
	(p14) LDFD	f45  = [Y2]
	(p12) FMA	f14 = f38, f55, f14
	}
	{ .mmf
	nop   __LINE__
	nop   __LINE__
	(p12) FMA	f15 = f39, f55, f15
	}
	;;
	(p13) FMA	f8  = f40, f56, f8
	(p13) FMA	f9  = f41, f56, f9
	(p13) FMA	f10 = f40, f57, f10
	(p13) FMA	f11 = f41, f57, f11
	(p13) FMA	f12 = f42, f58, f12
	(p13) FMA	f13 = f43, f58, f13
	(p13) FMA	f14 = f42, f59, f14
	(p13) FMA	f15 = f43, f59, f15
	;;
	(p14) FMA	f8  = f44, f60, f8
	(p14) FMA	f9  = f45, f60, f9
	(p14) FMA	f10 = f44, f61, f10
	(p14) FMA	f11 = f45, f61, f11
	;;
	.align 32

.L999:
	FADD	f8  = f8,  f12
	FADD	f9  = f9,  f13
	FADD	f10 = f10, f14
	FADD	f11 = f11, f15
	mov	ar.lc = ARLC
	;;
#ifndef CONJ
	FSUB	f8 = f8, f11
	FADD	f9 = f9, f10
#else
	FADD	f8 = f8, f11
	FSUB	f9 = f9, f10
#endif
	;;
	.align 32

.L1000:
#ifdef F_INTERFACE
	STFD	[r32] = f8, SIZE
	;;
	STFD	[r32] = f9, SIZE
#endif
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	EPILOGUE

