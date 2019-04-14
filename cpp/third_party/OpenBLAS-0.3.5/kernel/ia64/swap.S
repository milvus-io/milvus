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
#define PREFETCH_SIZE ( 8 * 16)
#elif defined(DOUBLE)
#define PREFETCH_SIZE (16 * 16)
#else
#define PREFETCH_SIZE (32 * 16)
#endif

#define SP	r12

#ifndef XDOUBLE
#define N	r32
#define X1	r36
#define INCX	r37
#define Y1	r38
#define INCY	r39
#else
#define N	r32
#define X1	r38
#define INCX	r39
#define Y1	r33
#define INCY	r34
#endif

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

#define YY	r22
#define XX	r23
#define INCX5	r24
#define INCY5	r25
#define INCX16	r26
#define INCY16	r27
#define XYSUB	r28

#define PR	r30
#define ARLC	r31

	PROLOGUE
	.prologue
	PROFCODE

#ifdef XDOUBLE
	adds	r8 = 16, SP
	adds	r9 = 24, SP
	;;
	ld8	Y1   = [r8]
	ld8	INCY = [r9]
	;;
#endif
	{ .mmi
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	tbit.z	p0, p8 = Y1, BASE_SHIFT
	(p6) br.ret.sptk.many b0
	}
	;;
	.body
	{ .mmi
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
	mov	PR = pr
	}
	{ .mmi
	sub	XYSUB = X1, Y1
	mov	X3 = X1
	shr	I =  N, 4
	}
	;;
	{ .mmi
	shladd	INCX5  = INCX, 2, INCX
	shladd	INCY5  = INCY, 2, INCY
	mov	pr.rot= 0
	}
	{ .mmi
	adds	I = -1, I
	and	J = 15, N
	extr	XYSUB =  XYSUB, BASE_SHIFT, 6
	}
	;;
	{ .mmi
	shladd	X2 = INCX, 2, X1
	shladd	Y2 = INCY, 2, Y1
	mov	ar.lc = I
	}
	{ .mmi
	shladd	X4 = INCX, 2, X1
	shladd	Y4 = INCY, 2, Y1
	cmp.eq	p16, p0 = r0, r0
	}
	;;
	{ .mmi
	shladd	PRE2  = XYSUB, BASE_SHIFT, Y1
	cmp.lt	p8 ,p0  =   28, XYSUB
	mov	Y3 = Y1
	}
	;;
	{ .mmi
	adds	PRE1 = (PREFETCH_SIZE +  4) * SIZE, X1
	adds	PRE2 = (PREFETCH_SIZE - 12) * SIZE, PRE2
	mov	ar.ec= 2
	}
	{ .mib
	cmp.eq	p9 ,p0  =   -1, I
	tbit.z	p0, p12 = N, 3
	(p9) br.cond.dpnt  .L15
	}
	;;
	.align 16

.L12:
	{ .mmi
	(p18) STFD	[Y3] = f56
	(p18) STFD	[Y4] = f64
	(p18) add   Y3 = Y3, INCY5
	}
	{ .mmi
	(p16) LDFD	f32 = [X1],  INCX
	(p16) LDFD	f40 = [X2],  INCX
	(p18) add   Y4 = Y4, INCY5
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f65
	(p17) STFD	[X4] = f73
	(p17) add   X3 = X3, INCX
	}
	{ .mmi
	(p16) LDFD	f64 = [Y1],  INCY
	(p16) LDFD	f72 = [Y2],  INCY
	(p17) add   X4 = X4, INCX
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f33
	(p17) STFD	[Y4] = f41
	(p17) add   Y3 = Y3, INCY
	}
	{ .mmi
	(p16) LDFD	f34 = [X1],  INCX
	(p16) LDFD	f42 = [X2],  INCX
	(p17) add   Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f67
	(p17) STFD	[X4] = f75
	(p17) add   X3 = X3, INCX
	}
	{ .mmi
	(p16) LDFD	f66 = [Y1],  INCY
	(p16) LDFD	f74 = [Y2],  INCY
	(p17) add   X4 = X4, INCX
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f35
	(p17) STFD	[Y4] = f43
	(p17) add   Y3 = Y3, INCY
	}
	{ .mmi
	(p16) LDFD	f36 = [X1],  INCX
	(p16) LDFD	f44 = [X2],  INCX
	(p17) add   Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f69
	(p17) STFD	[X4] = f77
	(p17) add   X3 = X3, INCX
	}
	{ .mmi
	(p16) LDFD	f68 = [Y1],  INCY
	(p16) LDFD	f76 = [Y2],  INCY
	(p17) add   X4 = X4, INCX
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f37
	(p17) STFD	[Y4] = f45
	(p17) add   Y3 = Y3, INCY
	}
	{ .mmi
	(p16) LDFD	f38 = [X1],  INCX5
	(p16) LDFD	f46 = [X2],  INCX5
	(p17) add   Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f71
	(p17) STFD	[X4] = f79
	(p17) add   X3 = X3, INCX5
	}
	{ .mmi
	(p16) LDFD	f70 = [Y1],  INCY5
	(p16) LDFD	f78 = [Y2],  INCY5
	(p17) add   X4 = X4, INCX5
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f39
	(p17) STFD	[Y4] = f47
	(p17) add   Y3 = Y3, INCY5
	}
	{ .mmi
	(p16) LDFD	f48 = [X1],  INCX
	(p16) LDFD	f56 = [X2],  INCX
	(p17) add   Y4 = Y4, INCY5
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f81
	(p17) STFD	[X4] = f89
	(p17) add   X3 = X3, INCX
	}
	{ .mmi
	(p16) LDFD	f80 = [Y1],  INCY
	(p16) LDFD	f88 = [Y2],  INCY
	(p17) add   X4 = X4, INCX
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f49
	(p17) STFD	[Y4] = f57
	(p17) add   Y3 = Y3, INCY
	}
	{ .mmi
	(p16) LDFD	f50 = [X1],  INCX
	(p16) LDFD	f58 = [X2],  INCX
	(p17) add   Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f83
	(p17) STFD	[X4] = f91
	(p17) add   X3 = X3, INCX
	}
	{ .mmi
	(p16) LDFD	f82 = [Y1],  INCY
	(p16) LDFD	f90 = [Y2],  INCY
	(p17) add   X4 = X4, INCX
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f51
	(p17) STFD	[Y4] = f59
	(p17) add   Y3 = Y3, INCY
	}
	{ .mmi
	(p16) LDFD	f52 = [X1],  INCX
	(p16) LDFD	f60 = [X2],  INCX
	(p17) add   Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f85
	(p17) STFD	[X4] = f93
	(p17) add   X3 = X3, INCX
	}
	{ .mmi
	(p16) LDFD	f84 = [Y1],  INCY
	(p16) LDFD	f92 = [Y2],  INCY
	(p17) add   X4 = X4, INCX
	}
	;;
	{ .mmi
	(p16) lfetch.nt1	[PRE1]
	(p16) lfetch.nt1	[PRE2]
	(p16) shladd	PRE1 = INCX, 4, PRE1
	}
	{ .mmi
	(p16) LDFD	f54 = [X1],  INCX5
	(p16) LDFD	f62 = [X2],  INCX5
	(p16) shladd	PRE2 = INCX, 4, PRE2
	}
	;;
	{ .mmi
	(p17) STFD	[Y3] = f53
	(p17) STFD	[Y4] = f61
	(p17) add   Y3 = Y3, INCY
	}
	{ .mmi
	(p16) LDFD	f86 = [Y1],  INCY5
	(p16) LDFD	f94 = [Y2],  INCY5
	(p17) add   Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p17) STFD	[X3] = f87
	(p17) STFD	[X4] = f95
	(p17) add   X3 = X3, INCX5
	}
	{ .mib
	nop   __LINE__
	(p17) add   X4 = X4, INCX5
	br.ctop.sptk.few .L12
	}
	;;
.L15:
	{ .mmi
	(p18) STFD	[Y3] = f56
	(p18) STFD	[Y4] = f64
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) LDFD	f32 = [X1],  INCX
	(p12) LDFD	f36 = [X2],  INCX
	cmp.eq	p10,  p0 = r0, J
	}
	;;
	{ .mmi
	(p12) LDFD	f80 = [Y1],  INCY
	(p12) LDFD	f84 = [Y2],  INCY
	(p18) add   Y3 = Y3, INCY5
	}
	{ .mmi
	(p12) LDFD	f33 = [X1],  INCX
	(p12) LDFD	f37 = [X2],  INCX
	(p18) add   Y4 = Y4, INCY5
	}
	;;
	{ .mmi
	(p12) LDFD	f81 = [Y1],  INCY
	(p12) LDFD	f85 = [Y2],  INCY
	mov	pr = PR, -65474
	}
	{ .mmb
	(p12) LDFD	f34 = [X1],  INCX
	(p12) LDFD	f38 = [X2],  INCX
	(p10) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f82 = [Y1],  INCY
	(p12) LDFD	f86 = [Y2],  INCY
	tbit.z	p0, p13 = N, 2
	}
	{ .mmi
	(p12) LDFD	f35 = [X1],  INCX5
	(p12) LDFD	f39 = [X2],  INCX5
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f83 = [Y1],  INCY5
	(p12) LDFD	f87 = [Y2],  INCY5
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmi
	(p13) LDFD	f40 = [X1],  INCX
	(p13) LDFD	f88 = [Y1],  INCY
	}
	;;
	{ .mmi
	(p13) LDFD	f41 = [X1],  INCX
	(p13) LDFD	f89 = [Y1],  INCY
	}
	;;
	{ .mmi
	(p12) STFD	[Y3] = f32
	(p12) STFD	[Y4] = f36
	(p12) add	Y3 = Y3, INCY
	}
	{ .mmi
	(p13) LDFD	f42 = [X1],  INCX
	(p13) LDFD	f90 = [Y1],  INCY
	(p12) add	Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p12) STFD	[X3] = f80
	(p12) STFD	[X4] = f84
	(p12) add	X3 = X3, INCX
	}
	{ .mmi
	(p13) LDFD	f43 = [X1],  INCX
	(p13) LDFD	f91 = [Y1],  INCY
	(p12) add	X4 = X4, INCX
	}
	;;
	{ .mmi
	(p12) STFD	[Y3] = f33
	(p12) STFD	[Y4] = f37
	(p12) add	Y3 = Y3, INCY
	}
	{ .mmi
	(p14) LDFD	f44 = [X1],  INCX
	(p14) LDFD	f92 = [Y1],  INCY
	(p12) add	Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p12) STFD	[X3] = f81
	(p12) STFD	[X4] = f85
	(p12) add	X3 = X3, INCX
	}
	{ .mmi
	(p14) LDFD	f45 = [X1],  INCX
	(p14) LDFD	f93 = [Y1],  INCY
	(p12) add	X4 = X4, INCX
	}
	;;
	{ .mmi
	(p12) STFD	[X3] = f82
	(p12) STFD	[X4] = f86
	(p12) add	X3 = X3, INCX
	}
	{ .mmi
	(p15) LDFD	f46 = [X1],  INCX
	(p15) LDFD	f94 = [Y1],  INCY
	(p12) add	X4 = X4, INCX
	}
	;;
	{ .mmi
	(p12) STFD	[Y3] = f34
	(p12) STFD	[Y4] = f38
	(p12) add	Y3 = Y3, INCY
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add	Y4 = Y4, INCY
	}
	;;
	{ .mmi
	(p12) STFD	[X3] = f83
	(p12) STFD	[X4] = f87
	(p12) add	X3 = X3, INCX5
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add	X4 = X4, INCX5
	}
	;;
	{ .mmi
	(p12) STFD	[Y3] = f35
	(p12) STFD	[Y4] = f39
	(p12) add	Y3 = Y3, INCY5
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add	Y4 = Y4, INCY5
	}
	;;
	{ .mmi
	(p13) STFD	[X3] = f88
	(p13) STFD	[Y3] = f40
	(p13) add	X3 = X3, INCX
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add	Y3 = Y3, INCY
	}
	;;
	{ .mmi
	(p13) STFD	[X3] = f89
	(p13) STFD	[Y3] = f41
	(p13) add	X3 = X3, INCX
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add	Y3 = Y3, INCY
	}
	;;
	{ .mmi
	(p13) STFD	[X3] = f90
	(p13) STFD	[Y3] = f42
	(p13) add	X3 = X3, INCX
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add	Y3 = Y3, INCY
	}
	;;
	{ .mmi
	(p13) STFD	[X3] = f91
	(p13) STFD	[Y3] = f43
	(p13) add	X3 = X3, INCX
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add	Y3 = Y3, INCY
	}
	;;
	{ .mmi
	(p14) STFD	[X3] = f92
	(p14) STFD	[Y3] = f44
	(p14) add	X3 = X3, INCX
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p14) add	Y3 = Y3, INCY
	}
	;;
	{ .mmi
	(p14) STFD	[X3] = f93
	(p14) STFD	[Y3] = f45
	(p14) add	X3 = X3, INCX
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p14) add	Y3 = Y3, INCY
	}
	;;
	{ .mmb
	(p15) STFD	[X3] = f94
	(p15) STFD	[Y3] = f46
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

