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

#ifdef XDOUBLE
#define N	r32
#define X	r14
#define INCX	r15
#define Y	r16
#define INCY	r17
#else
#define N	r32
#define X	r37
#define INCX	r38
#define Y	r39
#define INCY	r36
#endif

#define PRE1	r2
#define PRE2	r3

#define I	r18
#define J	r19
#define YY	r20
#define XX	r21
#define INCXM1	r22
#define INCYM1	r23
#define INCX8	r24
#define INCY8	r25

#define PR	r30
#define ARLC	r31


	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	adds	r14 = 16, SP
	adds	r15 = 24, SP
	adds	r16 = 32, SP
	}
	{ .mmb
	adds	r17 = 40, SP
	cmp.gt	p15, p0 = r0, N
	(p15) br.ret.sptk.many b0
	}
	;;
#ifdef XDOUBLE
	{ .mmi
	ld8	X     = [r14]
	ld8	INCX   = [r15]
	nop	__LINE__
	}
	{ .mmi
	ld8	Y     = [r16]
	ld8	INCY   = [r17]
	nop	__LINE__
	}
	;;
#else
	{ .mmi
	ld8	INCY   = [r14]
	nop	__LINE__
	nop	__LINE__
	}
	;;
#endif
	{ .mii
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	shl	INCX = INCX, ZBASE_SHIFT
	}
	;;
	.body
	{ .mii
	and	J =  7, N
	mov	PR = pr
	shl	INCY = INCY, ZBASE_SHIFT
	}
	;;
	{ .mmi
	mov	XX = X
	mov	YY = Y
	shr	I =  N, 3
	}
	;;
	{ .mmi
	adds	I = -1, I
	cmp.eq	p9, p0  =   r0, J
	mov	pr.rot = 0
	}
	;;
	{ .mmi
	shladd	INCX8 = INCX, 3, r0
	shladd	INCY8 = INCY, 3, r0
	mov	ar.ec= 3
	}
	{ .mmi
	adds	INCXM1 = -SIZE, INCX
	adds	INCYM1 = -SIZE, INCY
	cmp.eq	p16, p0 = r0, r0
	}
	;;
	{ .mmi
	adds	PRE1 = PREFETCH_SIZE * SIZE, X
	adds	PRE2 = PREFETCH_SIZE * SIZE, Y
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p8 ,p0  =   -1, I
	tbit.z	p0, p12 = J, 2
	(p8) br.cond.dpnt  .L55
	}
	;;
	.align 32

.L52:
	{ .mmi
	(p18) STFD	[XX] = f37, 1 * SIZE
	(p18) STFD	[YY] = f34, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f32  = [X], 1 * SIZE
	(p16) LDFD	f35  = [Y], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f43
	(p18) STFD	[YY] = f40
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f38  = [X],  INCXM1
	(p16) LDFD	f41  = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f49, 1 * SIZE
	(p18) STFD	[YY] = f46, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f44  = [X], 1 * SIZE
	(p16) LDFD	f47  = [Y], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f55
	(p18) STFD	[YY] = f52
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f50  = [X],  INCXM1
	(p16) LDFD	f53  = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f61, 1 * SIZE
	(p18) STFD	[YY] = f58, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f56  = [X], 1 * SIZE
	(p16) LDFD	f59  = [Y], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f67
	(p18) STFD	[YY] = f64
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f62  = [X],  INCXM1
	(p16) LDFD	f65  = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f73, 1 * SIZE
	(p18) STFD	[YY] = f70, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f68  = [X], 1 * SIZE
	(p16) LDFD	f71  = [Y], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f79
	(p18) STFD	[YY] = f76
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f74  = [X],  INCXM1
	(p16) LDFD	f77  = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f85, 1 * SIZE
	(p18) STFD	[YY] = f82, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f80  = [X], 1 * SIZE
	(p16) LDFD	f83  = [Y], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f91
	(p18) STFD	[YY] = f88
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f86  = [X],  INCXM1
	(p16) LDFD	f89  = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f97, 1 * SIZE
	(p18) STFD	[YY] = f94, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f92  = [X], 1 * SIZE
	(p16) LDFD	f95  = [Y], 1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f103
	(p18) STFD	[YY] = f100
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f98  = [X],  INCXM1
	(p16) LDFD	f101 = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f109, 1 * SIZE
	(p18) STFD	[YY] = f106, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f104 = [X],  1 * SIZE
	(p16) LDFD	f107 = [Y],  1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f115
	(p18) STFD	[YY] = f112
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f110 = [X],  INCXM1
	(p16) LDFD	f113 = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f121, 1 * SIZE
	(p18) STFD	[YY] = f118, 1 * SIZE
	}
	{ .mmi
	(p16) LDFD	f116 = [X],  1 * SIZE
	(p16) LDFD	f119 = [Y],  1 * SIZE
	}
	;;
	{ .mmi
	(p18) STFD	[XX] = f127
	(p18) STFD	[YY] = f124
	(p18) add   XX = XX, INCXM1
	}
	{ .mmi
	(p16) LDFD	f122 = [X],  INCXM1
	(p16) LDFD	f125 = [Y],  INCYM1
	(p18) add   YY = YY, INCYM1
	}
	{ .mmb
	(p16) lfetch.excl.nt1	[PRE1], INCX8
	(p16) lfetch.excl.nt1	[PRE2], INCY8
	br.ctop.sptk.few .L52
	}
	;;
	.align 32

.L55:
	{ .mmi
	(p12) LDFD	f32 = [X],  1 * SIZE
	(p12) LDFD	f80 = [Y],  1 * SIZE
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p12) LDFD	f33 = [X],  INCXM1
	(p12) LDFD	f81 = [Y],  INCYM1
	mov	pr = PR, -65474
	}
	;;
	{ .mmb
	(p12) LDFD	f34 = [X],  1 * SIZE
	(p12) LDFD	f82 = [Y],  1 * SIZE
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f35 = [X],  INCXM1
	(p12) LDFD	f83 = [Y],  INCYM1
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f36 = [X],  1 * SIZE
	(p12) LDFD	f84 = [Y],  1 * SIZE
	tbit.z	p0, p14 = N, 0
	}
	;;
	{ .mmi
	(p12) LDFD	f37 = [X],  INCXM1
	(p12) LDFD	f85 = [Y],  INCYM1
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f80, 1 * SIZE
	(p12) STFD	[YY] = f32, 1 * SIZE
	}
	{ .mmi
	(p12) LDFD	f38 = [X],  1 * SIZE
	(p12) LDFD	f86 = [Y],  1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f81
	(p12) STFD	[YY] = f33
	(p12) add	XX = XX, INCXM1
	}
	{ .mmi
	(p12) LDFD	f39 = [X],  INCXM1
	(p12) LDFD	f87 = [Y],  INCYM1
	(p12) add	YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f82, 1 * SIZE
	(p12) STFD	[YY] = f34, 1 * SIZE
	}
	{ .mmi
	(p13) LDFD	f40 = [X],  1 * SIZE
	(p13) LDFD	f88 = [Y],  1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f83
	(p12) STFD	[YY] = f35
	(p12) add	XX = XX, INCXM1
	}
	{ .mmi
	(p13) LDFD	f41 = [X],  INCXM1
	(p13) LDFD	f89 = [Y],  INCYM1
	(p12) add	YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f84, 1 * SIZE
	(p12) STFD	[YY] = f36, 1 * SIZE
	}
	{ .mmi
	(p13) LDFD	f42 = [X],  1 * SIZE
	(p13) LDFD	f90 = [Y],  1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f85
	(p12) STFD	[YY] = f37
	(p12) add	XX = XX, INCXM1
	}
	{ .mmi
	(p13) LDFD	f43 = [X],  INCXM1
	(p13) LDFD	f91 = [Y],  INCYM1
	(p12) add	YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f86, 1 * SIZE
	(p12) STFD	[YY] = f38, 1 * SIZE
	}
	{ .mmi
	(p14) LDFD	f44 = [X],  1 * SIZE
	(p14) LDFD	f92 = [Y],  1 * SIZE
	}
	;;
	{ .mmi
	(p12) STFD	[XX] = f87
	(p12) STFD	[YY] = f39
	(p12) add	XX = XX, INCXM1
	}
	{ .mmi
	(p14) LDFD	f45 = [X]
	(p14) LDFD	f93 = [Y]
	(p12) add	YY = YY, INCYM1
	}
	;;
	{ .mmi
	(p13) STFD	[XX] = f88, 1 * SIZE
	(p13) STFD	[YY] = f40, 1 * SIZE
	}
	;;
	(p13) STFD	[XX] = f89
	(p13) add	XX = XX, INCXM1
	(p13) STFD	[YY] = f41
	(p13) add	YY = YY, INCYM1
	;;
	(p13) STFD	[XX] = f90, 1 * SIZE
	(p13) STFD	[YY] = f42, 1 * SIZE
	;;
	(p13) STFD	[XX] = f91
	(p13) add	XX = XX, INCXM1
	(p13) STFD	[YY] = f43
	(p13) add	YY = YY, INCYM1
	;;
	(p14) STFD	[XX] = f92, 1 * SIZE
	(p14) STFD	[YY] = f44, 1 * SIZE
	;;
	(p14) STFD	[XX] = f93
	(p14) STFD	[YY] = f45
	br.ret.sptk.many b0
	;;
	EPILOGUE

