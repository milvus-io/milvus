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

#define PREFETCH_SIZE (8 * 24)

#define N	r32
#define X1	r33
#define INCX	r34
#define Y1	r35
#define INCY	r36

#define PREX1	r2
#define PREY1	r3

#define I	r14
#define J	r15
#define Y2	r16
#define X2	r17
#define Y3	r18
#define X3	r19
#define Y4	r20
#define X4	r21

#define INCX2	r22
#define INCY2	r23

#define INCX4	r24
#define INCY4	r25
#define INCX16	r26
#define INCY16	r27

#define PREX2	r28
#define PREY2	r29

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
	(p6) shladd X1 = r26, BASE_SHIFT, X1
	(p7) shladd Y1 = r27, BASE_SHIFT, Y1
	;;
#endif
	{ .mmi
	adds	PREX1 = (PREFETCH_SIZE + 2) * SIZE, X1
	adds	PREY1 = (PREFETCH_SIZE + 2) * SIZE, Y1
	mov	PR = pr
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	shl	INCX = INCX, BASE_SHIFT
	(p6) br.ret.sptk.many b0
	}
	;;
	{ .mfi
	add	X2 = INCX, X1
	mov	f10 = f0
	shl	INCY = INCY, BASE_SHIFT
	}
	{ .mmf
	and	r8    =  127, X1
	shladd	X3 = INCX, 1, X1
	mov	f11 = f0
	}
	;;
	{ .mmi
	and	PREY1 = -128, PREY1
	shladd	X4 = INCX, 1, X2
	add	INCX2  = INCX, INCX
	}
	{ .mmi
	shladd	INCX4  = INCX, 2, r0
	add	Y2 = INCY, Y1
	shladd	Y3 = INCY, 1, Y1
	}
	;;
	{ .mmi
	shladd	Y4 = INCY, 1, Y2
	add	INCY2  = INCY, INCY
	nop	__LINE__
	}
	{ .mmi
	shladd	INCY4  = INCY, 2, r0
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
	}
	;;
	{ .mfi
	nop	__LINE__
	mov	f12 = f0
	mov	pr.rot= 0
	}
	{ .mfi
	or	PREY1 = PREY1, r8
	mov	f13 = f0
	shr	I =  N, 4
	}
	;;
	{ .mfi
	adds	I = -1, I
	mov	f14 = f0
	mov	ar.ec= 3
	}
	{ .mmf
	shladd	PREX2 = INCX, 3, PREX1
	shladd	PREY2 = INCY, 3, PREY1
	mov	f15 = f0
	}
	;;
	{ .mmi
	and	J =  15, N
	cmp.eq	p16, p0 = r0, r0
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p6 ,p0  =   -1, I
	tbit.nz	p12, p0 = N, 3
	(p6) br.cond.dpnt  .L215
	}
	;;
	.align 32

.L212:
	{ .mmf
	(p16) lfetch.nt1 [PREX1], INCX16
	(p16) lfetch.nt1 [PREX2], INCX16
	(p18) FMA	f8  = f34, f82,  f8
	}
	{ .mmf
	(p16) LDFD	f80  = [X1], INCX4
	(p16) LDFD	f83  = [X2], INCX4
	(p18) FMA	f9  = f37, f85,  f9
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [X3], INCX4
	(p16) LDFD	f89  = [X4], INCX4
	(p18) FMA	f10 = f40, f88,  f10
	}
	{ .mmf
	(p16) LDFD	f92  = [X1], INCX4
	(p16) LDFD	f95  = [X2], INCX4
	(p18) FMA	f11 = f43, f91,  f11
	}
	;;
	{ .mmf
	(p16) LDFD	f32  = [Y1], INCY4
	(p16) LDFD	f35  = [Y2], INCY4
	(p18) FMA	f12 = f46, f94,  f12
	}
	{ .mmf
	(p16) LDFD	f38  = [Y3], INCY4
	(p16) LDFD	f41  = [Y4], INCY4
	(p18) FMA	f13 = f49, f97,  f13
	}
	;;
	{ .mmf
	(p16) LDFD	f98  = [X3], INCX4
	(p16) LDFD	f101 = [X4], INCX4
	(p18) FMA	f14 = f52, f100, f14
	}
	{ .mmf
	(p16) LDFD	f104 = [X1], INCX4
	(p16) LDFD	f107 = [X2], INCX4
	(p18) FMA	f15 = f55, f103, f15
	}
	;;
	{ .mmf
	(p16) LDFD	f44  = [Y1], INCY4
	(p16) LDFD	f47  = [Y2], INCY4
	(p18) FMA	f8  = f58, f106, f8
	}
	{ .mmf
	(p16) LDFD	f50  = [Y3], INCY4
	(p16) LDFD	f53  = [Y4], INCY4
	(p18) FMA	f9  = f61, f109, f9
	}
	;;
	{ .mmf
	(p16) lfetch.nt1 [PREY1], INCY16
	(p16) lfetch.nt1 [PREY2], INCY16
	(p18) FMA	f10 = f64, f112, f10
	}
	{ .mmf
	(p16) LDFD	f110 = [X3], INCX4
	(p16) LDFD	f113 = [X4], INCX4
	(p18) FMA	f11 = f67, f115, f11
	}
	;;
	{ .mmf
	(p16) LDFD	f56  = [Y1], INCY4
	(p16) LDFD	f59  = [Y2], INCY4
	(p18) FMA	f12 = f70, f118, f12
	}
	{ .mmf
	(p16) LDFD	f62  = [Y3], INCY4
	(p16) LDFD	f65  = [Y4], INCY4
	(p18) FMA	f13 = f73, f121, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f116 = [X1], INCX4
	(p16) LDFD	f119 = [X2], INCX4
	(p18) FMA	f14 = f76, f124, f14
	}
	{ .mmf
	(p16) LDFD	f122 = [X3], INCX4
	(p16) LDFD	f125 = [X4], INCX4
	(p18) FMA	f15 = f79, f127, f15
	}
	;;
	{ .mmi
	(p16) LDFD	f68  = [Y1], INCY4
	(p16) LDFD	f71  = [Y2], INCY4
	nop   __LINE__
	}
	{ .mmb
	(p16) LDFD	f74  = [Y3], INCY4
	(p16) LDFD	f77  = [Y4], INCY4
	br.ctop.sptk.few .L212
	}
	;;
	.align 32

.L215:
	{ .mmi
	(p12) LDFD	f48  = [X1], INCX4
	(p12) LDFD	f49  = [X2], INCX4
	cmp.eq	p7, p0  =   r0, J
	}
	{ .mmb
	(p12) LDFD	f50  = [X3], INCX4
	(p12) LDFD	f51  = [X4], INCX4
	(p7) br.cond.dptk .L999
	}
	;;
	{ .mmi
	(p12) LDFD	f32  = [Y1], INCY4
	(p12) LDFD	f33  = [Y2], INCY4
	tbit.nz	p13, p0 = N, 2
	}
	{ .mmi
	(p12) LDFD	f34  = [Y3], INCY4
	(p12) LDFD	f35  = [Y4], INCY4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p12) LDFD	f52  = [X1], INCX4
	(p12) LDFD	f53  = [X2], INCX4
	tbit.nz	p14, p0 = N, 1
	}
	{ .mmi
	(p12) LDFD	f54  = [X3], INCX4
	(p12) LDFD	f55  = [X4], INCX4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p12) LDFD	f36  = [Y1], INCY4
	(p12) LDFD	f37  = [Y2], INCY4
	tbit.nz	p15, p0 = N, 0
	}
	{ .mmi
	(p12) LDFD	f38  = [Y3], INCY4
	(p12) LDFD	f39  = [Y4], INCY4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) LDFD	f56  = [X1], INCX4
	(p13) LDFD	f57  = [X2], INCX4
	nop   __LINE__
	}
	{ .mmi
	(p13) LDFD	f58  = [X3], INCX4
	(p13) LDFD	f59  = [X4], INCX4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) LDFD	f40  = [Y1], INCY4
	(p13) LDFD	f41  = [Y2], INCY4
	nop   __LINE__
	}
	{ .mmi
	(p13) LDFD	f42  = [Y3], INCY4
	(p13) LDFD	f43  = [Y4], INCY4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) LDFD	f60  = [X1], INCX2
	(p14) LDFD	f61  = [X2], INCX2
	nop   __LINE__
	}
	{ .mmi
	(p14) LDFD	f44  = [Y1], INCY2
	(p14) LDFD	f45  = [Y2], INCY2
	nop   __LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f62  = [X1]
	(p15) LDFD	f46  = [Y1]
	nop   __LINE__
	}
	;;
	(p12) FMA	f8  = f32, f48, f8
	(p12) FMA	f9  = f33, f49, f9
	(p12) FMA	f10 = f34, f50, f10
	(p12) FMA	f11 = f35, f51, f11
	;;
	(p12) FMA	f12 = f36, f52, f12
	(p12) FMA	f13 = f37, f53, f13
	(p12) FMA	f14 = f38, f54, f14
	(p12) FMA	f15 = f39, f55, f15
	;;
	(p13) FMA	f8  = f40, f56, f8
	(p13) FMA	f9  = f41, f57, f9
	(p13) FMA	f10 = f42, f58, f10
	(p13) FMA	f11 = f43, f59, f11
	;;
	(p14) FMA	f8  = f44, f60, f8
	(p14) FMA	f9  = f45, f61, f9
	(p15) FMA	f10 = f46, f62, f10
	;;
	.align 32

.L999:
	FADD	f8  = f8,  f9
	FADD	f10 = f10, f11
	FADD	f12 = f12, f13
	FADD	f14 = f14, f15
	;;
	FADD	f8  = f8,  f10
	FADD	f12 = f12, f14
	mov	ar.lc = ARLC
	;;
	FADD	f8  = f8,  f12
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	EPILOGUE

