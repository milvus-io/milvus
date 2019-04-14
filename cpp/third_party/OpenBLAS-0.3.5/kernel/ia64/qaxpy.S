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

#define N	r32
#define X1	r38
#define INCX	r39
#define Y1	r33
#define INCY	r34

#define PRE1	r2
#define PRE2	r3

#define I	r14
#define J	r15
#define X2	r16
#define Y2	r17
#define X3	r18
#define Y3	r19
#define X4	r20
#define Y4	r21

#define YY1	r22
#define YY2	r23
#define YY3	r24
#define YY4	r25

#define INCX4	r8
#define INCY4	r9
#define INCX2	r10
#define INCY2	r11

#define INCX8	r26
#define INCY8	r27

#define PR	r30
#define ARLC	r31

#define ALPHA	f8
#define SP	r12

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	adds	r8 = 16, SP
	adds	r9 = 24, SP
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mmb
	adds	PRE1 = (PREFETCHSIZE + 2) * SIZE, X1
	cmp.lt	p0, p6 = r0, N
	(p6) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	ld8	Y1   = [r8]
	ld8	INCY = [r9]
	mov	PR = pr
	}
	;;
	.body
	{ .mmi
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	mov	pr.rot = 0
	}
	;;
	{ .mmi
	shladd	INCX4 = INCX, 2, r0
	shladd	INCY4 = INCY, 2, r0
	mov	ar.ec = 3
	}
	{ .mmi
	shladd	INCX8 = INCX, 3, r0
	shladd	INCY8 = INCY, 3, r0
	shr	I =  N, 4
	}
	;;
	{ .mmi
	add	X2  = INCX, X1
	add	Y2  = INCY, Y1
	add	YY2 = INCY, Y1
	}
	;;
	{ .mmi
	shladd	X3  = INCX, 1, X1
	shladd	Y3  = INCY, 1, Y1
	shladd	YY3 = INCY, 1, Y1
	}
	{ .mmi
	shladd	X4  = INCX, 1, X2
	shladd	Y4  = INCY, 1, Y2
	shladd	YY4 = INCY, 1, Y2
	}
	;;
	{ .mmi
	cmp.eq	p7 ,p0  = 0, I
	adds	I = -1, I
	mov	YY1 = Y1
	}
	{ .mmi
	and	r28  = 127, Y1
	and	PRE1 = -128, PRE1
	cmp.eq	p16, p0 = r0, r0
	}
	;;
	{ .mmi
	adds	PRE2 = (PREFETCHSIZE + 2) * SIZE, Y1
	or	PRE1 = PRE1, r28
	mov	ar.lc = I
	}
	{ .mib
	and	J =  15, N
	tbit.z	p0, p12 = N, 3
	(p7) br.cond.dpnt  .L115
	}
	;;
	.align 32

.L112:
	{ .mmf
	(p18) STFD	[YY1] = f6
 	(p18) STFD	[YY2] = f7
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE2], INCY8
	nop   __LINE__
	(p18) FMA	f7  = ALPHA, f61, f109
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f10
	(p18) STFD	[YY4] = f11
	(p18) FMA	f10 = ALPHA, f64, f112
	}
	{ .mmf
	(p16) lfetch.nt1	[PRE1], INCX8
	nop   __LINE__
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	;;
	{ .mmi
	(p16) LDFD	f32  = [X1], INCX4
	(p16) LDFD	f35  = [X2], INCX4
	(p18) add YY1 = INCY4, YY1
	}
	{ .mmi
	(p16) LDFD	f38  = [X3], INCX4
	(p16) LDFD	f41  = [X4], INCX4
	(p18) add YY2 = INCY4, YY2
	}
	;;
	{ .mmi
	(p17) LDFD	f117 = [Y1], INCY4
	(p17) LDFD	f120 = [Y2], INCY4
	(p18) add YY3 = INCY4, YY3
	}
	{ .mmi
	(p17) LDFD	f123 = [Y3], INCY4
	(p17) LDFD	f126 = [Y4], INCY4
	(p18) add YY4 = INCY4, YY4
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12
	(p18) STFD	[YY2] = f13
	(p18) FMA	f12 = ALPHA, f70, f118
	}
	{ .mmf
	(p18) add YY1 = INCY4, YY1
	(p18) add YY2 = INCY4, YY2
	(p18) FMA	f13 = ALPHA, f73, f121
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f14
	(p18) STFD	[YY4] = f15
	(p18) FMA	f14 = ALPHA, f76, f124
	}
	{ .mmf
	(p18) add YY3 = INCY4, YY3
	(p18) add YY4 = INCY4, YY4
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmi
	(p16) LDFD	f44  = [X1], INCX4
	(p16) LDFD	f47  = [X2], INCX4
	nop   __LINE__
	}
	{ .mmi
	(p16) LDFD	f50  = [X3], INCX4
	(p16) LDFD	f53  = [X4], INCX4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p16) LDFD	f80  = [Y1], INCY4
	(p16) LDFD	f83  = [Y2], INCY4
	nop   __LINE__
	}
	{ .mmi
	(p16) LDFD	f86  = [Y3], INCY4
	(p16) LDFD	f89  = [Y4], INCY4
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6
 	(p18) STFD	[YY2] = f7
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE2], INCY8
	nop   __LINE__
	(p17) FMA	f7  = ALPHA, f36, f84
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f10
	(p18) STFD	[YY4] = f11
	(p17) FMA	f10 = ALPHA, f39, f87
	}
	{ .mmf
	(p16) lfetch.nt1	[PRE1], INCX8
	nop   __LINE__
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	;;
	{ .mmi
	(p16) LDFD	f56  = [X1], INCX4
	(p16) LDFD	f59  = [X2], INCX4
	(p18) add YY1 = INCY4, YY1
	}
	{ .mmi
	(p16) LDFD	f62  = [X3], INCX4
	(p16) LDFD	f65  = [X4], INCX4
	(p18) add YY2 = INCY4, YY2
	}
	;;
	{ .mmi
	(p16) LDFD	f92  = [Y1], INCY4
	(p16) LDFD	f95  = [Y2], INCY4
	(p18) add YY3 = INCY4, YY3
	}
	{ .mmi
	(p16) LDFD	f98  = [Y3], INCY4
	(p16) LDFD	f101 = [Y4], INCY4
	(p18) add YY4 = INCY4, YY4
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12
	(p18) STFD	[YY2] = f13
	(p17) FMA	f12 = ALPHA, f45, f93
	}
	{ .mmf
	(p18) add YY1 = INCY4, YY1
	(p18) add YY2 = INCY4, YY2
	(p17) FMA	f13 = ALPHA, f48, f96
	}
	;;
	{ .mmf
	(p18) STFD	[YY3] = f14
	(p18) STFD	[YY4] = f15
	(p17) FMA	f14 = ALPHA, f51, f99
	}
	{ .mmf
	(p18) add YY3 = INCY4, YY3
	(p18) add YY4 = INCY4, YY4
	(p17) FMA	f15 = ALPHA, f54, f102
	}
	;;
	{ .mmi
	(p16) LDFD	f68  = [X1], INCX4
	(p16) LDFD	f71  = [X2], INCX4
	nop   __LINE__
	}
	{ .mmi
	(p16) LDFD	f74  = [X3], INCX4
	(p16) LDFD	f77  = [X4], INCX4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p16) LDFD	f104 = [Y1], INCY4
	(p16) LDFD	f107 = [Y2], INCY4
	nop   __LINE__
	}
	{ .mmb
	(p16) LDFD	f110 = [Y3], INCY4
	(p16) LDFD	f113 = [Y4], INCY4
	br.ctop.sptk.few .L112
	}
	;;
	.align 32

.L115:
	{ .mmi
	(p12) LDFD f32 = [X1], INCX4
	(p12) LDFD f33 = [X2], INCX4
	mov	pr = PR, -65474
	}
	{ .mmi
	(p12) LDFD f34 = [X3], INCX4
	(p12) LDFD f35 = [X4], INCX4
	cmp.eq	p9, p0  =   r0, J
	}
	;;
	{ .mmi
	(p12) LDFD f64 = [Y1], INCY4
	(p12) LDFD f65 = [Y2], INCY4
	mov	ar.lc  = ARLC
	}
	{ .mmb
	(p12) LDFD f66 = [Y3], INCY4
	(p12) LDFD f67 = [Y4], INCY4
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD f36 = [X1], INCX4
	(p12) LDFD f37 = [X2], INCX4
	tbit.z	p0, p13 = N, 2
	}
	{ .mmi
	(p12) LDFD f38 = [X3], INCX4
	(p12) LDFD f39 = [X4], INCX4
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD f68 = [Y1], INCY4
	(p12) LDFD f69 = [Y2], INCY4
	tbit.z	p0, p15 = N, 0
	}
	{ .mmi
	(p12) LDFD f70 = [Y3], INCY4
	(p12) LDFD f71 = [Y4], INCY4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) LDFD f40 = [X1], INCX4
	(p13) LDFD f41 = [X2], INCX4
	shladd	INCX2 = INCX, 1, r0
	}
	{ .mmi
	(p13) LDFD f42 = [X3], INCX4
	(p13) LDFD f43 = [X4], INCX4
	shladd	INCY2 = INCY, 1, r0
	}
	;;
	{ .mmi
	(p13) LDFD f72 = [Y1], INCY4
	(p13) LDFD f73 = [Y2], INCY4
	nop   __LINE__
	}
	{ .mmi
	(p13) LDFD f74 = [Y3], INCY4
	(p13) LDFD f75 = [Y4], INCY4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) LDFD f44 = [X1], INCX2
	(p14) LDFD f45 = [X2], INCX2
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) LDFD f76 = [Y1], INCY2
	(p14) LDFD f77 = [Y2], INCY2
	nop   __LINE__
	}
	;;
	{ .mmi
	(p15) LDFD f46 = [X1]
	(p15) LDFD f78 = [Y1]
	nop   __LINE__
	}
	;;
	(p12) FMA	f32 = ALPHA, f32, f64
	(p12) FMA	f33 = ALPHA, f33, f65
	(p12) FMA	f34 = ALPHA, f34, f66
	(p12) FMA	f35 = ALPHA, f35, f67
	(p12) FMA	f36 = ALPHA, f36, f68
	(p12) FMA	f37 = ALPHA, f37, f69
	(p12) FMA	f38 = ALPHA, f38, f70
	(p12) FMA	f39 = ALPHA, f39, f71
	;;
	{ .mmf
	(p12) STFD	[YY1] = f32
	(p12) STFD	[YY2] = f33
	(p13) FMA	f40 = ALPHA, f40, f72
	}
	{ .mmf
	(p12) add YY1 = INCY4, YY1
	(p12) add YY2 = INCY4, YY2
	(p13) FMA	f41 = ALPHA, f41, f73
	}
	;;
	{ .mmf
	(p12) STFD	[YY3] = f34
	(p12) STFD	[YY4] = f35
	(p13) FMA	f42 = ALPHA, f42, f74
	}
	{ .mmf
	(p12) add YY3 = INCY4, YY3
	(p12) add YY4 = INCY4, YY4
	(p13) FMA	f43 = ALPHA, f43, f75
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f36
	(p12) STFD	[YY2] = f37
	(p14) FMA	f44 = ALPHA, f44, f76
	}
	{ .mmf
	(p12) add YY1 = INCY4, YY1
	(p12) add YY2 = INCY4, YY2
	(p14) FMA	f45 = ALPHA, f45, f77
	}
	;;
	{ .mmf
	(p12) STFD	[YY3] = f38
	(p12) STFD	[YY4] = f39
	(p15) FMA	f46 = ALPHA, f46, f78
	}
	{ .mmi
	(p12) add YY3 = INCY4, YY3
	(p12) add YY4 = INCY4, YY4
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[YY1] = f40
	(p13) STFD	[YY2] = f41
	nop   __LINE__
	}
	{ .mmi
	(p13) add YY1 = INCY4, YY1
	(p13) add YY2 = INCY4, YY2
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[YY3] = f42
	(p13) STFD	[YY4] = f43
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f44
	(p14) STFD	[YY2] = f45
	(p14) add YY1 = INCY2, YY1
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f46
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

