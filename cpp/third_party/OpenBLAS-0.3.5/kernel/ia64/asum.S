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
#define INCX16	r21

#define PR	r30
#define ARLC	r31

#define N	r32
#define X	r33
#define INCX	r34


	PROLOGUE
	.prologue
	PROFCODE
	{ .mfi
	adds	PRE1 = PREFETCH_SIZE * SIZE, X
	mov	f8   = f0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	;;
	.body
#ifdef F_INTERFACE
	{ .mmi
	LDINT	N    = [N]
	LDINT	INCX = [INCX]
	nop.i	0
	}
	;;
#ifndef USE64BITINT
	{ .mii
	nop.m	0
	sxt4	N = N
	sxt4	INCX = INCX
	}
	;;
#endif
#endif
	{ .mmi
	cmp.lt	p0, p6 = r0, INCX
	cmp.lt	p0, p7 = r0, N
	shr	I =  N, (4 - COMPADD)
	}
	{ .mbb
	and	J = ((1 << (4 - COMPADD)) - 1), N
	(p6) 	br.ret.sptk.many b0
	(p7) 	br.ret.sptk.many b0
	}
	;;
	{ .mfi
	adds	I = -1, I
	mov	f10 = f0
	mov	PR = pr
	}
	{ .mfi
	cmp.eq	p9, p0  =   r0, J
	mov	f9  = f0
	tbit.z	p0, p12 = N, 3 - COMPADD
	}
	;;
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	cmp.ne	p17, p0 = r0, r0
	mov	ar.ec= 3
	}
	{ .mfi
	cmp.ne	p18, p0 = r0, r0
	mov	f11 = f0
	shl	INCX = INCX, BASE_SHIFT + COMPADD
	}
	;;
	{ .mmi
#ifdef XDOUBLE
	shladd	INCX16  = INCX, (3 - COMPADD), r0
#else
	shladd	INCX16  = INCX, (4 - COMPADD), r0
#endif
	cmp.ne	p19, p0 = r0, r0
	mov	ar.lc = I
	}
	{ .mmb
	cmp.gt	p8 ,p0  =   r0, I
#ifdef COMPLEX
       adds	INCX = - SIZE, INCX
#else
	nop.m	0
#endif
	(p8) br.cond.dpnt  .L55
	}
	;;
	.align 32

.L52:
	{ .mmf
	(p16) lfetch.nt1 [PRE1], INCX16
	(p16) LDFD	f32  = [X], STRIDE
	(p18) fabs	f34  = f34
	}
	{ .mfb
	(p19) FADD	f8  = f8,  f71
	}
	;;
	{ .mmf
	(p16) LDFD	f35  = [X], INCX
	(p18) fabs	f37  = f37
	}
	{ .mfb
	(p19) FADD	f9  = f9,  f74
	}
	;;
	{ .mmf
	(p16) LDFD	f38  = [X], STRIDE
	(p18) fabs	f40  = f40
	}
	{ .mfb
	(p19) FADD	f10 = f10, f77
	}
	;;
	{ .mmf
	(p16) LDFD	f41  = [X], INCX
	(p18) fabs	f43  = f43
	}
	{ .mfb
	(p19) FADD	f11 = f11, f80
	}
	;;
	{ .mmf
	(p16) LDFD	f44  = [X], STRIDE
	(p18) fabs	f46  = f46
	}
	{ .mfb
	(p18) FADD	f8  = f8,  f34
	}
	;;
	{ .mmf
	(p16) LDFD	f47  = [X], INCX
	(p18) fabs	f49  = f49
	}
	{ .mfb
	(p18) FADD	f9  = f9,  f37
	}
	;;
	{ .mmf
	(p16) LDFD	f50  = [X], STRIDE
	(p18) fabs	f52  = f52
	}
	{ .mfb
	(p18) FADD	f10 = f10, f40
	}
	;;
	{ .mmf
	(p16) LDFD	f53  = [X], INCX
	(p18) fabs	f55  = f55
	}
	{ .mfb
	(p18) FADD	f11 = f11, f43
	}
	;;
	{ .mmf
#ifdef XDOUBLE
	(p16) lfetch.nt1 [PRE1], INCX16
#endif
	(p16) LDFD	f56  = [X], STRIDE
	(p18) fabs	f58  = f58
	}
	{ .mfb
	(p18) FADD	f8  = f8,  f46
	}
	;;
	{ .mmf
	(p16) LDFD	f59  = [X], INCX
	(p18) fabs	f61  = f61
	}
	{ .mfb
	(p18) FADD	f9  = f9,  f49
	}
	;;
	{ .mmf
	(p16) LDFD	f62  = [X], STRIDE
	(p18) fabs	f64  = f64
	}
	{ .mfb
	(p18) FADD	f10 = f10, f52
	}
	;;
	{ .mmf
	(p16) LDFD	f65  = [X], INCX
	(p18) fabs	f67  = f67
	}
	{ .mfb
	(p18) FADD	f11 = f11, f55
	}
	;;
	{ .mmf
	(p16) LDFD	f68  = [X], STRIDE
	(p18) fabs	f70  = f70
	}
	{ .mfb
	(p18) FADD	f8  = f8,  f58
	}
	;;
	{ .mmf
	(p16) LDFD	f71  = [X], INCX
	(p18) fabs	f73  = f73
	}
	{ .mfb
	(p18) FADD	f9  = f9,  f61
	}
	;;
	{ .mmf
	(p16) LDFD	f74  = [X], STRIDE
	(p18) fabs	f76  = f76
	}
	{ .mfb
	(p18) FADD	f10 = f10, f64
	}
	;;
	{ .mmf
	(p16) LDFD	f77  = [X], INCX
	(p18) fabs	f79  = f79
	}
	{ .mfb
	(p18) FADD	f11 = f11, f67
	br.ctop.sptk.few .L52
	}
	;;
	FADD	f8  = f8,  f71
	FADD	f9  = f9,  f74
	FADD	f10 = f10, f77
	FADD	f11 = f11, f80
	.align 32
	;;
.L55:
	(p12) LDFD	f32  = [X], STRIDE
	(p9) br.cond.dptk .L998
	;;
	(p12) LDFD	f33  = [X], INCX
	;;
	(p12) LDFD	f34  = [X], STRIDE
	;;
	(p12) LDFD	f35  = [X], INCX
	tbit.z	p0, p13 = N, (2 - COMPADD)
	;;
	(p12) LDFD	f36  = [X], STRIDE
	tbit.z	p0, p14 = N, (1 - COMPADD)
	;;
	(p12) LDFD	f37  = [X], INCX
#ifndef COMPLEX
	tbit.z	p0, p15 = N, 0
#endif
	;;
	(p12) LDFD	f38  = [X], STRIDE
	(p12) fabs	f32  = f32
	;;
	(p12) LDFD	f39  = [X], INCX
	(p12) fabs	f33  = f33
	;;
	(p13) LDFD	f40  = [X], STRIDE
	(p12) fabs	f34  = f34
	;;
	(p13) LDFD	f41  = [X], INCX
	(p12) fabs	f35  = f35
	;;
	(p13) LDFD	f42  = [X], STRIDE
	(p12) fabs	f36  = f36
	(p12) FADD	f8  = f8,  f32
	;;
	(p13) LDFD	f43  = [X], INCX
	(p12) fabs	f37  = f37
	(p12) FADD	f9  = f9,  f33
	;;
	(p14) LDFD	f44  = [X], STRIDE
	(p12) fabs	f38  = f38
	(p12) FADD	f10 = f10, f34
	;;
	(p14) LDFD	f45  = [X], INCX
	(p12) fabs	f39  = f39
	(p12) FADD	f11 = f11, f35
	;;
#ifndef COMPLEX
	(p15) LDFD	f46  = [X]
#endif
	(p13) fabs	f40 = f40
	(p12) FADD	f8  = f8,  f36
	;;
	(p13) fabs	f41 = f41
	(p12) FADD	f9  = f9,  f37
	(p13) fabs	f42 = f42
	(p12) FADD	f10 = f10, f38
	(p13) fabs	f43 = f43
	(p12) FADD	f11 = f11, f39
	;;
	(p14) fabs	f44 = f44
	(p13) FADD	f8  = f8,  f40
	(p14) fabs	f45 = f45
	(p13) FADD	f9  = f9,  f41
#ifndef COMPLEX
	(p15) fabs	f46 = f46
#endif
	(p13) FADD	f10 = f10, f42
	;;
	(p13) FADD	f11 = f11, f43
	(p14) FADD	f8  = f8,  f44
	(p14) FADD	f9  = f9,  f45
#ifndef COMPLEX
	(p15) FADD	f10 = f10, f46
#endif
	;;
	.align 32

.L998:
	{ .mfi
	FADD	f8  = f8,  f9
	mov	ar.lc  = ARLC
	}
	{ .mmf
	FADD	f10 = f10, f11
	}
	;;
	{ .mii
	mov	pr = PR, -65474
	}
	;;
	{ .mfb
	FADD	f8  = f8,  f10
	br.ret.sptk.many b0
	}
	EPILOGUE
