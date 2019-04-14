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

#ifndef COMPLEX
#define COMPADD	0
#define STRIDE INCX
#else
#define COMPADD	1
#define STRIDE SIZE
#endif

#define PRE1	r2

#define I	r17
#define J	r18
#define X2	r19
#define INCX5	r20
#define INCX16	r21

#define N	r32
#define X	r33
#define INCX	r34
#define PR	r30
#define ARLC	r31


	PROLOGUE
	.prologue
	PROFCODE
	{ .mfi
	adds	PRE1 = PREFETCH_SIZE * SIZE, X
	mov	f8  = f0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	;;
	.body
#ifdef F_INTERFACE
	LDINT	N    = [N]
	LDINT	INCX = [INCX]
	;;
#ifndef USE64BITINT
	sxt4	N = N
	sxt4	INCX = INCX
	;;
#endif
#endif
	{ .mmi
	cmp.ge	p6, p0 = r0, N
	cmp.ge	p7, p0 = r0, INCX
	shr	I =  N, (4 - COMPADD)
	}
	{ .mbb
	and	J = ((1 << (4 - COMPADD)) - 1), N
	(p6) 	br.ret.sptk.many b0
	(p7) 	br.ret.sptk.many b0
	}
	;;
	{ .mfi
	mov	f9  = f0
	mov	PR = pr
	}
	{ .mfi
	adds	I = -1, I
	mov	f10 = f0
	shl	INCX = INCX, (BASE_SHIFT + COMPADD)
	}
	;;
	{ .mfi
	shladd	X2     = INCX,  (2 - COMPADD), X
	mov	f11 = f0
	mov	pr.rot = 0
	}
	{ .mfi
	shladd	INCX5  = INCX,  (2 - COMPADD), INCX
	mov	f12 = f0
	tbit.z	p0, p12 = N, (3 - COMPADD)
	}
	;;
	{ .mfi
	shladd	INCX16  = INCX, (4 - COMPADD), r0
	mov	f13 = f0
	mov	ar.ec= 3
	}
	{ .mmf
	cmp.gt	p8 ,p0  =   r0, I
	cmp.eq	p16, p0 = r0, r0
	mov	f14 = f0
	}
	;;
	{ .mmf
#ifdef COMPLEX
       adds	INCX  = - SIZE, INCX
       adds	INCX5 = - SIZE, INCX5
#else
	nop.m	0
	nop.m	0
#endif
	mov	f15 = f0
	}
	{ .mib
	cmp.eq	p9, p0  =   r0, J
	mov	ar.lc = I
	(p8) br.cond.dpnt  .L52
	}
	;;
	.align 32

.L51:
	(p16) LDFD	f32  = [X],  STRIDE
	(p16) lfetch.nt1 [PRE1], INCX16
	(p18) fma.d.s1	f8  = f34, f34, f8

	(p16) LDFD	f35  = [X2], STRIDE
	(p18) fma.d.s1	f9  = f37, f37, f9
	nop.b 0
	;;
	(p16) LDFD	f38  = [X],  INCX
	(p18) fma.d.s1	f10 = f40, f40, f10
	nop.b 0
	(p16) LDFD	f41  = [X2], INCX
	(p18) fma.d.s1	f11 = f43, f43, f11
	nop.b 0
	;;
	(p16) LDFD	f44  = [X],  STRIDE
	(p18) fma.d.s1	f12 = f46, f46, f12
	nop.b 0
	(p16) LDFD	f47  = [X2], STRIDE
	(p18) fma.d.s1	f13 = f49, f49, f13
	nop.b 0
	;;
	(p16) LDFD	f50  = [X],  INCX5
	(p18) fma.d.s1	f14 = f52, f52, f14
	nop.b 0
	(p16) LDFD	f53  = [X2], INCX5
	(p18) fma.d.s1	f15 = f55, f55, f15
	nop.b 0
	;;
	(p16) LDFD	f56  = [X],  STRIDE
	(p18) fma.d.s1	f8  = f58, f58, f8
	nop.b 0
	(p16) LDFD	f59  = [X2], STRIDE
	(p18) fma.d.s1	f9  = f61, f61, f9
	nop.b 0
	;;
	(p16) LDFD	f62  = [X],  INCX
	(p18) fma.d.s1	f10 = f64, f64, f10
	nop.b 0
	(p16) LDFD	f65  = [X2], INCX
	(p18) fma.d.s1	f11 = f67, f67, f11
	nop.b 0
	;;
	(p16) LDFD	f68  = [X],  STRIDE
	(p18) fma.d.s1	f12 = f70, f70, f12
	nop.b 0
	(p16) LDFD	f71  = [X2], STRIDE
	(p18) fma.d.s1	f13 = f73, f73, f13
	nop.b 0
	;;
	(p16) LDFD	f74  = [X],  INCX5
	(p18) fma.d.s1	f14 = f76, f76, f14
	nop.b 0
	(p16) LDFD	f77  = [X2], INCX5
	(p18) fma.d.s1	f15 = f79, f79, f15
	br.ctop.sptk.few .L51
	;;
	.align 32

.L52:
	{ .mmb
	(p12) LDFD	f32  = [X],  STRIDE
	(p12) LDFD	f33  = [X2], STRIDE
	(p9) br.cond.dptk .L998
	}
	;;
	{ .mmi
	(p12) LDFD	f34  = [X],  INCX
	(p12) LDFD	f35  = [X2], INCX
	tbit.z	p0, p13 = N, (2 - COMPADD)
	}
	;;
	{ .mmi
	(p12) LDFD	f36  = [X],  STRIDE
	(p12) LDFD	f37  = [X2], STRIDE
	tbit.z	p0, p14 = N, (1 - COMPADD)
	}
	;;
	{ .mmi
	(p12) LDFD	f38  = [X],  INCX5
	(p12) LDFD	f39  = [X2], INCX5
#ifndef COMPLEX
	tbit.z	p0, p15 = N, 0
#endif
	}
	;;
	(p13) LDFD	f40  = [X], STRIDE
	(p12) fma.d.s1	f8  = f32, f32, f8
	(p12) fma.d.s1	f9  = f33, f33, f9
	;;
	(p13) LDFD	f41  = [X], INCX
	(p12) fma.d.s1	f10 = f34, f34, f10
	(p12) fma.d.s1	f11 = f35, f35, f11
	;;
	(p13) LDFD	f42  = [X], STRIDE
	(p12) fma.d.s1	f12 = f36, f36, f12
	(p12) fma.d.s1	f13 = f37, f37, f13
	;;
	(p13) LDFD	f43  = [X], INCX
	(p12) fma.d.s1	f14 = f38, f38, f14
	(p12) fma.d.s1	f15 = f39, f39, f15
	;;
	(p14) LDFD	f44  = [X], STRIDE
	(p13) fma.d.s1	f8  = f40, f40, f8
	(p13) fma.d.s1	f9  = f41, f41, f9
	;;
	(p14) LDFD	f45  = [X], INCX
	(p13) fma.d.s1	f10 = f42, f42, f10
	(p13) fma.d.s1	f11 = f43, f43, f11
	;;
#ifndef COMPLEX
	(p15) LDFD	f46  = [X]
#endif
	(p14) fma.d.s1	f12 = f44, f44, f12
	(p14) fma.d.s1	f13 = f45, f45, f13
	;;
#ifndef COMPLEX
	(p15) fma.d.s1	f14 = f46, f46, f14
	;;
#endif
	.align 32

.L998:
	{ .mmf
	fadd.d.s1	f8  = f8,  f9
	}
	{ .mmf
	fadd.d.s1	f10 = f10, f11
	}
	{ .mmf
	fadd.d.s1	f12 = f12, f13
	}
	{ .mfi
	fadd.d.s1	f14 = f14, f15
	mov	ar.lc = ARLC
	}
	;;
	{ .mmf
	fadd.d.s1	f8  = f8,  f10
	}
	{ .mfi
	fadd.d.s1	f12 = f12, f14
	mov	pr = PR, -65474
	}
	;;
	{ .mfb
	fadd.d.s1	f8  = f8,  f12
 	br	sqrt
	}
	;;
	EPILOGUE

	.section .data
	.type	sqrt, @function
	.global	sqrt
