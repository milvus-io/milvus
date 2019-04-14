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

#define PREFETCHSIZE 64 * 8

#define N	r32
#define X	r36
#define INCX	r37
#define Y	r38
#define INCY	r39

#define PRE1	r2
#define PRE2	r3

#define I	r14
#define J	r15
#define Y1	r16
#define Y2	r17
#define X1	r18
#define X2	r19
#define INCX16	r20
#define INCY16	r21
#define YYY	r25
#define YY	r27
#define XA	r28
#define XB	r29
#define PR	r30
#define ARLC	r31

#define ALPHA	f8
#define ALPHA_P	f9

	PROLOGUE
	.prologue
	PROFCODE

	{ .mii
	shladd	INCX = INCX, BASE_SHIFT, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	tbit.nz	p10, p0 = X, BASE_SHIFT
	}
	{ .mfb
	cmp.lt	p0, p6 = r0, N
	fcmp.eq	p7, p0 = ALPHA, f0
	(p6) br.ret.sptk.many b0
	}
	;;
	.body
	{ .mmi
	(p10) LDFD	f32 = [X], INCX
	shladd	INCY = INCY, BASE_SHIFT, r0
	mov	PR = pr
	}
	{ .mib
	(p10) adds N = -1, N
	mov  YYY = Y
	(p7) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p10) LDFD	f33 = [Y], INCY
	cmp.ne	p13, p0 = SIZE, INCX
	shr    XA = X, 2
	}
	{ .mmi
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
	nop.i 0
	}
	;;
	{ .mii
	mov	Y1 = Y
	tbit.nz	p11, p0 = Y, BASE_SHIFT
	shr    XB = Y, 2
	}
	;;
	{ .mmf
	and	XA = 0x3f, XA
	and	XB = 0x3f, XB
	(p10) FMA f32 = ALPHA, f32, f33
	}
	;;
	{ .mmi
	sub	XA = XB, XA
	shladd	Y2 = INCY, 2, Y
	mov	pr.rot = 0x10000
	}
	{ .mbb
	cmp.ne p14, p0 = SIZE, INCY
	(p13) br.cond.dpnt .L100
	(p14) br.cond.dpnt .L100
	}
	;;
	{ .mmi
	cmp.gt	p14, p0 =  r0, XA
	;;
	and	J =  15, N
	shr	I =  N, 4
	}
	{ .mfb
	(p14) adds XA = 64, XA
	fpack	ALPHA_P = f8, f8
	(p11) br.cond.dpnt .L30
	}
	;;
	{ .mmi
	cmp.gt	p14, p0 =  32, XA
	cmp.lt	p15, p0 =  58, XA
	mov	ar.ec = 3
	}
	{ .mmi
	and	J =  31, N
	cmp.eq	p16, p0 = r0, r0
	shr	I =  N, 5
	}
	;;
	{ .mmi
	cmp.eq	p9, p0  =   r0, J
	cmp.eq	p7 ,p0  = 0, I
	adds	I = -1, I
	}
	{ .mbb
	nop.m 0
	(p14) br.cond.dpnt .L20
	(p15) br.cond.dpnt .L20
	}
	;;
	{ .mmi
	(p10) STFD [YYY] = f32
	adds	PRE1 = PREFETCHSIZE * SIZE, X
	mov	ar.lc = I
	}
	{ .mib
	adds	PRE2 = (PREFETCHSIZE - 24) * SIZE, Y
	tbit.z	p0, p11 = N, 4
	(p7) br.cond.dpnt  .L15
	}
	;;
	.align 32

.L12:
/* 0 */
	{ .mmf
	(p18) stf8	[Y1] = f6,   2 * SIZE
	(p16) lfetch.nt1 [PRE1], 32 * SIZE
	(p18) fpma	f12  = ALPHA_P, f46, f94
	}
	{ .mmi
	(p16) ldf8	f32 = [X], 2 * SIZE
	(p16) ldf8	f80 = [Y], 2 * SIZE
	}
	;;
/* 1 */
	{ .mmf
	(p18) stf8	[Y1] = f7,  2 * SIZE
	(p16) lfetch.excl.nt1	[PRE2], 32 * SIZE
	(p18) fpma	f13  = ALPHA_P, f49, f97
	}
	{ .mmi
	(p16) ldf8	f35 = [X], 2 * SIZE
	(p16) ldf8	f83 = [Y], 2 * SIZE
	}
	;;
/* 2 */
	{ .mmf
	(p18) stf8	[Y1] = f10,  2 * SIZE
	(p18) fpma	f14  = ALPHA_P, f52, f100
	}
	{ .mmi
	(p16) ldf8	f38 = [X], 2 * SIZE
	(p16) ldf8	f86 = [Y], 2 * SIZE
	}
	;;
/* 3 */
	{ .mmf
	(p18) stf8	[Y1] = f11, 2 * SIZE
	(p18) fpma	f15  = ALPHA_P, f55, f103
	}
	{ .mmi
	(p16) ldf8	f41 = [X], 2 * SIZE
	(p16) ldf8	f89 = [Y], 2 * SIZE
	}
	;;
/* 4 */
	{ .mmf
	(p18) stf8	[Y1] = f12,  2 * SIZE
	(p18) fpma	f6   = ALPHA_P, f58, f106
	}
	{ .mmi
	(p16) ldf8	f44  = [X], 2 * SIZE
	(p16) ldf8	f92  = [Y], 2 * SIZE
	}
	;;
/* 5 */
	{ .mmf
	(p18) stf8	[Y1] = f13,  2 * SIZE
	(p18) fpma	f7   = ALPHA_P, f61, f109
	}
	{ .mmi
	(p16) ldf8	f47  = [X], 2 * SIZE
	(p16) ldf8	f95  = [Y], 2 * SIZE
	}
	;;
/* 6 */
	{ .mmf
	(p18) stf8	[Y1] = f14,  2 * SIZE
	(p18) fpma	f10  = ALPHA_P, f64, f112
	}
	{ .mmi
	(p16) ldf8	f50  = [X], 2 * SIZE
	(p16) ldf8	f98  = [Y], 2 * SIZE
	}
	;;
/* 7 */
	{ .mmf
	(p18) stf8	[Y1] = f15, 2 * SIZE
	(p18) fpma	f11  = ALPHA_P, f67, f115
	}
	{ .mmi
	(p16) ldf8	f53  = [X], 2 * SIZE
	(p16) ldf8	f101 = [Y], 2 * SIZE
	}
	;;
/* 8 */
	{ .mmf
	(p18) stf8	[Y1] = f6,   2 * SIZE
	(p18) fpma	f12  = ALPHA_P, f70, f118
	}
	{ .mmi
	(p16) ldf8	f56 = [X], 2 * SIZE
	(p16) ldf8	f104 = [Y], 2 * SIZE
	}
	;;
/* 9 */
	{ .mmf
	(p18) stf8	[Y1] = f7,  2 * SIZE
	(p18) fpma	f13  = ALPHA_P, f73, f121
	}
	{ .mmi
	(p16) ldf8	f59 = [X], 2 * SIZE
	(p16) ldf8	f107 = [Y], 2 * SIZE
	}
	;;
/* 10 */
	{ .mmf
	(p18) stf8	[Y1] = f10,  2 * SIZE
	(p18) fpma	f14 = ALPHA_P, f76, f124
	}
	{ .mmi
	(p16) ldf8	f62 = [X], 2 * SIZE
	(p16) ldf8	f110 = [Y], 2 * SIZE
	}
	;;
/* 11 */
	{ .mmf
	(p18) stf8	[Y1] = f11, 2 * SIZE
	(p18) fpma	f15  = ALPHA_P, f79, f127
	}
	{ .mmi
	(p16) ldf8	f65 = [X], 2 * SIZE
	(p16) ldf8	f113 = [Y], 2 * SIZE
	}
	;;
/* 12 */
	{ .mmf
	(p18) stf8	[Y1] = f12,  2 * SIZE
	(p17) fpma	f6   = ALPHA_P, f33, f81
	}
	{ .mmi
	(p16) ldf8	f68  = [X], 2 * SIZE
	(p16) ldf8	f116 = [Y], 2 * SIZE
	}
	;;
/* 13 */
	{ .mmf
	(p18) stf8	[Y1] = f13,  2 * SIZE
	(p17) fpma	f7   = ALPHA_P, f36, f84
	}
	{ .mmi
	(p16) ldf8	f71  = [X], 2 * SIZE
	(p16) ldf8	f119 = [Y], 2 * SIZE
	}
	;;
/* 14 */
	{ .mmf
	(p18) stf8	[Y1] = f14,  2 * SIZE
	(p17) fpma	f10  = ALPHA_P, f39, f87
	}
	{ .mmi
	(p16) ldf8	f74  = [X], 2 * SIZE
	(p16) ldf8	f122 = [Y], 2 * SIZE
	}
	;;
/*15 */
	{ .mmf
	(p18) stf8	[Y1] = f15, 2 * SIZE
	(p17) fpma	f11  = ALPHA_P, f42, f90
	}
	{ .mmb
	(p16) ldf8	f77  = [X], 2 * SIZE
	(p16) ldf8	f125 = [Y], 2 * SIZE
	br.ctop.sptk.few .L12
	}
	;;
	.align 32

.L15:
	{ .mmi
	(p11) ldf8	f32 = [X], 2 * SIZE
	(p11) ldf8	f33 = [Y], 2 * SIZE
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p11) ldf8	f34 = [X], 2 * SIZE
	(p11) ldf8	f35 = [Y], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p11) ldf8	f36 = [X], 2 * SIZE
	(p11) ldf8	f37 = [Y], 2 * SIZE
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p11) ldf8	f38 = [X], 2 * SIZE
	(p11) ldf8	f39 = [Y], 2 * SIZE
	tbit.z	p0, p12 = N, 3
	}
	;;
	{ .mmi
	(p11) ldf8	f40 = [X], 2 * SIZE
	(p11) ldf8	f41 = [Y], 2 * SIZE
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	(p11) ldf8	f42 = [X], 2 * SIZE
	(p11) ldf8	f43 = [Y], 2 * SIZE
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmf
	(p11) ldf8	f44 = [X], 2 * SIZE
	(p11) ldf8	f45 = [Y], 2 * SIZE
	(p11) fpma	f6  = ALPHA_P, f32, f33
	}
	;;
	{ .mmf
	(p11) ldf8	f46 = [X], 2 * SIZE
	(p11) ldf8	f47 = [Y], 2 * SIZE
	(p11) fpma	f7  = ALPHA_P, f34, f35
	}
	;;
	{ .mmf
	(p12) ldf8	f48 = [X], 2 * SIZE
	(p12) ldf8	f49 = [Y], 2 * SIZE
	(p11) fpma	f10 = ALPHA_P, f36, f37
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f6,   2 * SIZE
	nop.m 0
	tbit.z	p0, p15 = N, 0
	}
	{ .mmf
	(p12) ldf8	f50 = [X], 2 * SIZE
	(p12) ldf8	f51 = [Y], 2 * SIZE
	(p11) fpma	f11 = ALPHA_P, f38, f39
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f7,   2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p12) ldf8	f52 = [X], 2 * SIZE
	(p12) ldf8	f53 = [Y], 2 * SIZE
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f10,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p12) ldf8	f54 = [X], 2 * SIZE
	(p12) ldf8	f55 = [Y], 2 * SIZE
	(p11) fpma	f12 = ALPHA_P, f40, f41
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f11,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p13) ldf8	f56 = [X], 2 * SIZE
	(p13) ldf8	f57 = [Y], 2 * SIZE
	(p11) fpma	f13 = ALPHA_P, f42, f43
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f12,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p13) ldf8	f58 = [X], 2 * SIZE
	(p13) ldf8	f59 = [Y], 2 * SIZE
	(p11) fpma	f14 = ALPHA_P, f44, f45
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f13,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p14) ldf8	f60 = [X], 2 * SIZE
	(p14) ldf8	f61 = [Y], 2 * SIZE
	(p11) fpma	f15 = ALPHA_P, f46, f47
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f14,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p15) ldfs	f62  = [X]
	(p15) ldfs	f63  = [Y]
	(p12) fpma	f6  = ALPHA_P, f48, f49
	}
	;;
	(p12) fpma	f7  = ALPHA_P, f50, f51
	(p12) fpma	f10 = ALPHA_P, f52, f53
	;;
	(p11) stf8	[Y1] = f15,  2 * SIZE
	(p12) fpma	f11 = ALPHA_P, f54, f55
	;;
	(p12) stf8	[Y1] = f6,   2 * SIZE
	(p13) fpma	f12 = ALPHA_P, f56, f57
	;;
	(p12) stf8	[Y1] = f7,   2 * SIZE
	(p13) fpma	f13 = ALPHA_P, f58, f59
	;;
	(p12) stf8	[Y1] = f10,  2 * SIZE
	(p14) fpma	f14 = ALPHA_P, f60, f61
	;;
	(p12) stf8	[Y1] = f11,  2 * SIZE
	(p15) FMA	f15 = ALPHA,   f62, f63
	;;
	(p13) stf8	[Y1] = f12,  2 * SIZE
	;;
	(p13) stf8	[Y1] = f13,  2 * SIZE
	;;
	(p14) stf8	[Y1] = f14,  2 * SIZE
	;;
	(p15) stfs	[Y1] = f15
	br.ret.sptk.many b0
	;;
	.align 32

/* X is aligned; case 2 */

.L20:
	{ .mmi
	(p10) STFD [YYY] = f32
	adds	PRE1 = (PREFETCHSIZE - 28) *  SIZE, X
	mov	ar.lc = I
	}
	{ .mib
	adds	PRE2 = (PREFETCHSIZE +  4) * SIZE, Y
	tbit.z	p0, p11 = N, 4
	(p7) br.cond.dpnt  .L25
	}
	;;
	.align 32

.L22:
/* 0 */
	{ .mmf
	(p18) stf8	[Y1] = f6,   2 * SIZE
	(p16) lfetch.nt1	[PRE1], 32 * SIZE
	(p18) fpma	f12  = ALPHA_P, f46, f94
	}
	{ .mmi
	(p17) ldf8	f60  = [X], 2 * SIZE
	(p16) ldf8	f80  = [Y], 2 * SIZE
	}
	;;
/* 1 */
	{ .mmf
	(p18) stf8	[Y1] = f7,  2 * SIZE
	(p16) lfetch.excl.nt1	[PRE2], 32 * SIZE
	(p18) fpma	f13  = ALPHA_P, f49, f97
	}
	{ .mmi
	(p17) ldf8	f63  = [X], 2 * SIZE
	(p16) ldf8	f83  = [Y], 2 * SIZE
	}
	;;
/* 2 */
	{ .mmf
	(p18) stf8	[Y1] = f10,  2 * SIZE
	(p18) fpma	f14  = ALPHA_P, f52, f100
	}
	{ .mmi
	(p17) ldf8	f66  = [X], 2 * SIZE
	(p16) ldf8	f86  = [Y], 2 * SIZE
	}
	;;
/* 3 */
	{ .mmf
	(p18) stf8	[Y1] = f11, 2 * SIZE
	(p18) fpma	f15  = ALPHA_P, f55, f103
	}
	{ .mmi
	(p17) ldf8	f69  = [X], 2 * SIZE
	(p16) ldf8	f89  = [Y], 2 * SIZE
	}
	;;
/* 4 */
	{ .mmf
	(p18) stf8	[Y1] = f12,  2 * SIZE
	(p18) fpma	f6   = ALPHA_P, f58, f106
	}
	{ .mmi
	(p17) ldf8	f72  = [X], 2 * SIZE
	(p16) ldf8	f92  = [Y], 2 * SIZE
	}
	;;
/* 5 */
	{ .mmf
	(p18) stf8	[Y1] = f13,  2 * SIZE
	(p18) fpma	f7   = ALPHA_P, f61, f109
	}
	{ .mmi
	(p17) ldf8	f75  = [X], 2 * SIZE
	(p16) ldf8	f95  = [Y], 2 * SIZE
	}
	;;
/* 6 */
	{ .mmf
	(p18) stf8	[Y1] = f14,  2 * SIZE
	(p18) fpma	f10  = ALPHA_P, f64, f112
	}
	{ .mmi
	(p17) ldf8	f78  = [X], 2 * SIZE
	(p16) ldf8	f98  = [Y], 2 * SIZE
	}
	;;
/* 7 */
	{ .mmf
	(p18) stf8	[Y1] = f15, 2 * SIZE
	(p18) fpma	f11  = ALPHA_P, f67, f115
	}
	{ .mmi
	(p16) ldf8	f32  = [X], 2 * SIZE
	(p16) ldf8	f101 = [Y], 2 * SIZE
	}
	;;
/* 8 */
	{ .mmf
	(p18) stf8	[Y1] = f6,   2 * SIZE
	(p18) fpma	f12  = ALPHA_P, f70, f118
	}
	{ .mmi
	(p16) ldf8	f35 = [X], 2 * SIZE
	(p16) ldf8	f104 = [Y], 2 * SIZE
	}
	;;
/* 9 */
	{ .mmf
	(p18) stf8	[Y1] = f7,  2 * SIZE
	(p18) fpma	f13  = ALPHA_P, f73, f121
	}
	{ .mmi
	(p16) ldf8	f38 = [X], 2 * SIZE
	(p16) ldf8	f107 = [Y], 2 * SIZE
	}
	;;
/* 10 */
	{ .mmf
	(p18) stf8	[Y1] = f10,  2 * SIZE
	(p18) fpma	f14 = ALPHA_P, f76, f124
	}
	{ .mmi
	(p16) ldf8	f41 = [X], 2 * SIZE
	(p16) ldf8	f110 = [Y], 2 * SIZE
	}
	;;
/* 11 */
	{ .mmf
	(p18) stf8	[Y1] = f11, 2 * SIZE
	(p18) fpma	f15  = ALPHA_P, f79, f127
	}
	{ .mmi
	(p16) ldf8	f44  = [X], 2 * SIZE
	(p16) ldf8	f113 = [Y], 2 * SIZE
	}
	;;
/* 12 */
	{ .mmf
	(p18) stf8	[Y1] = f12,  2 * SIZE
	(p17) fpma	f6   = ALPHA_P, f33, f81
	}
	{ .mmi
	(p16) ldf8	f47  = [X], 2 * SIZE
	(p16) ldf8	f116 = [Y], 2 * SIZE
	}
	;;
/* 13 */
	{ .mmf
	(p18) stf8	[Y1] = f13,  2 * SIZE
	(p17) fpma	f7   = ALPHA_P, f36, f84
	}
	{ .mmi
	(p16) ldf8	f50  = [X], 2 * SIZE
	(p16) ldf8	f119 = [Y], 2 * SIZE
	}
	;;
/* 14 */
	{ .mmf
	(p18) stf8	[Y1] = f14,  2 * SIZE
	(p17) fpma	f10  = ALPHA_P, f39, f87
	}
	{ .mmi
	(p16) ldf8	f53  = [X], 2 * SIZE
	(p16) ldf8	f122 = [Y], 2 * SIZE
	}
	;;
/*15 */
	{ .mmf
	(p18) stf8	[Y1] = f15, 2 * SIZE
	(p17) fpma	f11  = ALPHA_P, f42, f90
	}
	{ .mmb
	(p16) ldf8	f56 = [X], 2 * SIZE
	(p16) ldf8	f125 = [Y], 2 * SIZE
	br.ctop.sptk.few .L22
	}
	;;
	.align 32

.L25:
	{ .mmi
	(p11) ldf8	f32 = [X], 2 * SIZE
	(p11) ldf8	f33 = [Y], 2 * SIZE
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p11) ldf8	f34 = [X], 2 * SIZE
	(p11) ldf8	f35 = [Y], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p11) ldf8	f36 = [X], 2 * SIZE
	(p11) ldf8	f37 = [Y], 2 * SIZE
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p11) ldf8	f38 = [X], 2 * SIZE
	(p11) ldf8	f39 = [Y], 2 * SIZE
	tbit.z	p0, p12 = N, 3
	}
	;;
	{ .mmi
	(p11) ldf8	f40 = [X], 2 * SIZE
	(p11) ldf8	f41 = [Y], 2 * SIZE
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	(p11) ldf8	f42 = [X], 2 * SIZE
	(p11) ldf8	f43 = [Y], 2 * SIZE
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmf
	(p11) ldf8	f44 = [X], 2 * SIZE
	(p11) ldf8	f45 = [Y], 2 * SIZE
	(p11) fpma	f6  = ALPHA_P, f32, f33
	}
	;;
	{ .mmf
	(p11) ldf8	f46 = [X], 2 * SIZE
	(p11) ldf8	f47 = [Y], 2 * SIZE
	(p11) fpma	f7  = ALPHA_P, f34, f35
	}
	;;
	{ .mmf
	(p12) ldf8	f48 = [X], 2 * SIZE
	(p12) ldf8	f49 = [Y], 2 * SIZE
	(p11) fpma	f10 = ALPHA_P, f36, f37
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f6,   2 * SIZE
	nop.m 0
	tbit.z	p0, p15 = N, 0
	}
	{ .mmf
	(p12) ldf8	f50 = [X], 2 * SIZE
	(p12) ldf8	f51 = [Y], 2 * SIZE
	(p11) fpma	f11 = ALPHA_P, f38, f39
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f7,   2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p12) ldf8	f52 = [X], 2 * SIZE
	(p12) ldf8	f53 = [Y], 2 * SIZE
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f10,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p12) ldf8	f54 = [X], 2 * SIZE
	(p12) ldf8	f55 = [Y], 2 * SIZE
	(p11) fpma	f12 = ALPHA_P, f40, f41
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f11,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p13) ldf8	f56 = [X], 2 * SIZE
	(p13) ldf8	f57 = [Y], 2 * SIZE
	(p11) fpma	f13 = ALPHA_P, f42, f43
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f12,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p13) ldf8	f58 = [X], 2 * SIZE
	(p13) ldf8	f59 = [Y], 2 * SIZE
	(p11) fpma	f14 = ALPHA_P, f44, f45
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f13,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p14) ldf8	f60 = [X], 2 * SIZE
	(p14) ldf8	f61 = [Y], 2 * SIZE
	(p11) fpma	f15 = ALPHA_P, f46, f47
	}
	;;
	{ .mmi
	(p11) stf8	[Y1] = f14,  2 * SIZE
	nop.m 0
	nop.i 0
	}
	{ .mmf
	(p15) ldfs	f62  = [X]
	(p15) ldfs	f63  = [Y]
	(p12) fpma	f6  = ALPHA_P, f48, f49
	}
	;;
	(p12) fpma	f7  = ALPHA_P, f50, f51
	(p12) fpma	f10 = ALPHA_P, f52, f53
	;;
	(p11) stf8	[Y1] = f15,  2 * SIZE
	(p12) fpma	f11 = ALPHA_P, f54, f55
	;;
	(p12) stf8	[Y1] = f6,   2 * SIZE
	(p13) fpma	f12 = ALPHA_P, f56, f57
	;;
	(p12) stf8	[Y1] = f7,   2 * SIZE
	(p13) fpma	f13 = ALPHA_P, f58, f59
	;;
	(p12) stf8	[Y1] = f10,  2 * SIZE
	(p14) fpma	f14 = ALPHA_P, f60, f61
	;;
	(p12) stf8	[Y1] = f11,  2 * SIZE
	(p15) FMA	f15 = ALPHA,   f62, f63
	;;
	(p13) stf8	[Y1] = f12,  2 * SIZE
	;;
	(p13) stf8	[Y1] = f13,  2 * SIZE
	;;
	(p14) stf8	[Y1] = f14,  2 * SIZE
	;;
	(p15) stfs	[Y1] = f15
	br.ret.sptk.many b0
	;;
	.align 32

.L30:
	{ .mmi
	cmp.eq	p9, p0  =   r0, J
	cmp.eq	p7 ,p0  = 0, I
	mov	ar.ec = 4
	}
	{ .mmi
	cmp.lt	p12, p0 = 33, XA
	adds	I = -1, I
	}
	;;
	{ .mmi
	cmp.gt	p14, p0 =  15, XA
	cmp.lt	p15, p0 =  60, XA
	(p12) cmp.gt.unc p13, p0 = 53, XA
	}
	{ .bbb
	(p13) br.cond.dpnt .L40
	(p14) br.cond.dpnt .L40
	(p15) br.cond.dpnt .L40
	}
	;;
	{ .mmi
	(p10) STFD [YYY] = f32
	adds	PRE1 = (PREFETCHSIZE +  6) * SIZE, X
	mov	ar.lc = I
	}
	{ .mib
	adds	PRE2 = (PREFETCHSIZE +  0) * SIZE, Y
	tbit.z	p0, p12 = N, 3
	(p7) br.cond.dpnt  .L35
	}
	;;
	.align 32

.L32:
	{ .mmf
	(p19) STFD	[Y1] = f6,   1 * SIZE
	(p19) STFD	[Y2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f34, f82
	}
	{ .mmf
	(p16) LDFPD	f32,  f35  = [X], 2 * SIZE
	(p16) LDFD	f80 = [Y], 1 * SIZE
	(p18) FMA	f7  = ALPHA, f46, f94
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f10,  1 * SIZE
	(p19) STFD	[Y2] = f11,  1 * SIZE
	(p18) FMA	f10 = ALPHA, f37, f85
	}
	{ .mmf
	(p16) LDFPD	f38,  f41  = [X], 2 * SIZE
	(p16) LDFPD	f83,  f86  = [Y], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f49, f97
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f12,  1 * SIZE
	(p19) STFD	[Y2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f40, f88
	}
	{ .mmf
	(p16) LDFPD	f44,  f47  = [X], 2 * SIZE
	(p16) LDFPD	f89,  f92  = [Y], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f52, f100
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f14,  5 * SIZE
	(p19) STFD	[Y2] = f15,  5 * SIZE
	(p18) FMA	f14 = ALPHA, f43, f91
	}
	{ .mmf
	(p16) LDFPD	f50,  f53  = [X], 2 * SIZE
	(p16) LDFPD	f95,  f98  = [Y], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f55, f103
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f6,   1 * SIZE
	(p18) STFD	[Y2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p16) LDFPD	f56,  f59  = [X], 2 * SIZE
	(p16) LDFPD	f101, f104 = [Y], 2 * SIZE
	(p18) FMA	f7  = ALPHA, f70, f118
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f10,  1 * SIZE
	(p18) STFD	[Y2] = f11,  1 * SIZE
	(p18) FMA	f10 = ALPHA, f61, f109
	}
	{ .mmf
	(p16) LDFPD	f62,  f65  = [X], 2 * SIZE
	(p16) LDFPD	f107, f110 = [Y], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f73, f121
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f12,  1 * SIZE
	(p18) STFD	[Y2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f64, f112
	}
	{ .mmf
	(p16) LDFPD	f68,  f71  = [X], 2 * SIZE
	(p16) LDFPD	f113, f116 = [Y], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f76, f124
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f14,  5 * SIZE
	(p18) STFD	[Y2] = f15,  5 * SIZE
	(p18) FMA	f14 = ALPHA, f67, f115
	}
	{ .mmf
	(p16) LDFPD	f74,  f77  = [X], 2 * SIZE
	(p16) LDFPD	f119, f122 = [Y], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmi
	(p16) lfetch.nt1	[PRE1], 16 * SIZE
	(p16) lfetch.excl.nt1	[PRE2], 16 * SIZE
	nop.i 0
	}
	{ .mmb
	(p16) LDFD	f125  = [Y], 1 * SIZE
	nop.m 0
	br.ctop.sptk.few .L32
	}
	;;
	.align 32

.L35:
	{ .mmi
	(p12) LDFPD	f32,  f33  = [X], 2 * SIZE
	(p12) LDFD	f34 = [Y], 1 * SIZE;
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p12) LDFPD	f36,  f37  = [X], 2 * SIZE
	(p12) LDFPD	f35,  f38  = [Y], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFPD	f40,  f41  = [X], 2 * SIZE
	(p12) LDFPD	f39,  f42  = [Y], 2 * SIZE
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFPD	f44,  f45  = [X], 2 * SIZE
	(p12) LDFPD	f43,  f46  = [Y], 2 * SIZE
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [X], 2 * SIZE
	(p12) LDFD	f47 = [Y], 1 * SIZE
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFPD	f52,  f53  = [X], 2 * SIZE
	(p13) LDFD	f50 = [Y], 1 * SIZE
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmi
	(p14) LDFPD	f56,  f57  = [X], 2 * SIZE
	(p13) LDFPD	f51,  f54  = [Y], 2 * SIZE
	mov   YY = Y1;
	}
	;;
	(p15) LDFD	f60 = [X]
	(p13) LDFD	f55 = [Y], 1 * SIZE
	;;
	(p14) LDFD	f58 = [Y], 1 * SIZE
	(p12) FMA	f6  = ALPHA, f32, f34
	(p12) FMA	f7  = ALPHA, f40, f42
	;;
	(p14) LDFD	f59 = [Y], 1 * SIZE
	(p12) shladd YY = INCY, 3, YY
	(p12) FMA	f10 = ALPHA, f33, f35
	(p12) FMA	f11 = ALPHA, f41, f43
	;;
	(p15) LDFD	f61  = [Y]
	(p13) shladd YY = INCY, 2, YY
	(p12) FMA	f12 = ALPHA, f36, f38
	(p12) FMA	f13 = ALPHA, f44, f46
	;;
	(p12) STFD	[Y1] = f6,   1 * SIZE
	(p12) FMA	f14 = ALPHA, f37, f39
	(p12) STFD	[Y2] = f7,   1 * SIZE
	(p12) FMA	f15 = ALPHA, f45, f47
	;;
	(p12) STFD	[Y1] = f10,  1 * SIZE
	(p13) FMA	f6  = ALPHA, f48, f50
	(p12) STFD	[Y2] = f11,  1 * SIZE
	(p14) FMA	f7  = ALPHA, f56, f58
	;;
	(p12) STFD	[Y1] = f12,  1 * SIZE
	(p13) FMA	f10 = ALPHA, f49, f51
	(p12) STFD	[Y2] = f13,  1 * SIZE
	(p14) FMA	f11 = ALPHA, f57, f59
	;;
	(p12) STFD	[Y1] = f14,  5 * SIZE
	(p13) FMA	f12 = ALPHA, f52, f54
	(p12) STFD	[Y2] = f15,  5 * SIZE
	(p15) FMA	f13 = ALPHA, f60, f61
	;;
	(p13) STFD	[Y1] = f6,   1 * SIZE
	(p14) STFD	[YY] = f7,   1 * SIZE
	(p13) FMA	f14 = ALPHA, f53, f55
	;;
	(p13) STFD	[Y1] = f10,  1 * SIZE
	(p14) STFD	[YY] = f11,  1 * SIZE
	;;
	(p13) STFD	[Y1] = f12,  1 * SIZE
	(p15) STFD	[YY] = f13
	;;
	(p13) STFD	[Y1] = f14
	br.ret.sptk.many b0
	;;
	.align 32

.L40:
	{ .mmi
	(p10) STFD [YYY] = f32
	adds	PRE1 = (PREFETCHSIZE + 38) * SIZE, X
	mov	ar.lc = I
	}
	{ .mib
	adds	PRE2 = (PREFETCHSIZE + 14) * SIZE, Y
	tbit.z	p0, p12 = N, 3
	(p7) br.cond.dpnt  .L45
	}
	;;
	.align 32

.L42:
	{ .mmf
	(p19) STFD	[Y1] = f6,   1 * SIZE
	(p19) STFD	[Y2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f34, f82
	}
	{ .mmf
	(p16) lfetch.nt1	[PRE1], 16 * SIZE
	(p17) LDFPD	f102, f105 = [Y], 2 * SIZE
	(p18) FMA	f7  = ALPHA, f46, f94
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f10,  1 * SIZE
	(p19) STFD	[Y2] = f11,  1 * SIZE
	(p18) FMA	f10 = ALPHA, f37, f85
	}
	{ .mmf
	(p17) LDFPD	f33,  f36  = [X], 2 * SIZE
	(p17) LDFPD	f108, f111 = [Y], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f49, f97
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f12,  1 * SIZE
	(p19) STFD	[Y2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f40, f88
	}
	{ .mmf
	(p17) LDFPD	f39,  f42  = [X], 2 * SIZE
	(p17) LDFPD	f114, f117 = [Y], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f52, f100
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f14,  5 * SIZE
	(p19) STFD	[Y2] = f15,  5 * SIZE
	(p18) FMA	f14 = ALPHA, f43, f91
	}
	{ .mmf
	(p17) LDFPD	f45,  f48  = [X], 2 * SIZE
	(p17) LDFPD	f120, f123 = [Y], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f55, f103
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f6,   1 * SIZE
	(p18) STFD	[Y2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p17) LDFPD	f51,  f54  = [X], 2 * SIZE
	(p17) LDFD	f126  = [Y], 1 * SIZE
	(p18) FMA	f7  = ALPHA, f70, f118
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f10,  1 * SIZE
	(p18) STFD	[Y2] = f11,  1 * SIZE
	(p18) FMA	f10 = ALPHA, f61, f109
	}
	{ .mmf
	(p17) LDFPD	f57,  f60  = [X], 2 * SIZE
	(p16) LDFD	f80 = [Y], 1 * SIZE
	(p18) FMA	f11 = ALPHA, f73, f121
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f12,  1 * SIZE
	(p18) STFD	[Y2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f64, f112
	}
	{ .mmf
	(p17) LDFPD	f63,  f66  = [X], 2 * SIZE
	(p16) LDFPD	f83,  f86  = [Y], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f76, f124
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f14,  5 * SIZE
	(p18) STFD	[Y2] = f15,  5 * SIZE
	(p18) FMA	f14 = ALPHA, f67, f115
	}
	{ .mmf
	(p17) LDFPD	f69,  f72  = [X], 2 * SIZE
	(p16) LDFPD	f89,  f92  = [Y], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
#if 0
	(p16) lfetch.excl.nt1	[PRE2], 16 * SIZE
#endif
	{ .mmb
	(p17) LDFPD	f75,  f78  = [X], 2 * SIZE
	(p16) LDFPD	f95,  f98  = [Y], 2 * SIZE
	br.ctop.sptk.few .L42
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f6,   1 * SIZE
	(p19) STFD	[Y2] = f7,   1 * SIZE
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f10,  1 * SIZE
	(p19) STFD	[Y2] = f11,  1 * SIZE
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f12,  1 * SIZE
	(p19) STFD	[Y2] = f13,  1 * SIZE
	}
	;;
	{ .mmf
	(p19) STFD	[Y1] = f14,  5 * SIZE
	(p19) STFD	[Y2] = f15,  5 * SIZE
	}
	;;
	.align 32

.L45:
	{ .mmi
	(p12) LDFPD	f32,  f33  = [X], 2 * SIZE
	(p12) LDFD	f34 = [Y], 1 * SIZE;
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p12) LDFPD	f36,  f37  = [X], 2 * SIZE
	(p12) LDFPD	f35,  f38  = [Y], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFPD	f40,  f41  = [X], 2 * SIZE
	(p12) LDFPD	f39,  f42  = [Y], 2 * SIZE
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFPD	f44,  f45  = [X], 2 * SIZE
	(p12) LDFPD	f43,  f46  = [Y], 2 * SIZE
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [X], 2 * SIZE
	(p12) LDFD	f47 = [Y], 1 * SIZE
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFPD	f52,  f53  = [X], 2 * SIZE
	(p13) LDFD	f50 = [Y], 1 * SIZE
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmi
	(p14) LDFPD	f56,  f57  = [X], 2 * SIZE
	(p13) LDFPD	f51,  f54  = [Y], 2 * SIZE
	mov   YY = Y1;
	}
	;;
	(p15) LDFD	f60 = [X]
	(p13) LDFD	f55 = [Y], 1 * SIZE
	;;
	(p14) LDFD	f58 = [Y], 1 * SIZE
	(p12) FMA	f6  = ALPHA, f32, f34
	(p12) FMA	f7  = ALPHA, f40, f42
	;;
	(p14) LDFD	f59 = [Y], 1 * SIZE
	(p12) shladd YY = INCY, 3, YY
	(p12) FMA	f10 = ALPHA, f33, f35
	(p12) FMA	f11 = ALPHA, f41, f43
	;;
	(p15) LDFD	f61  = [Y]
	(p13) shladd YY = INCY, 2, YY
	(p12) FMA	f12 = ALPHA, f36, f38
	(p12) FMA	f13 = ALPHA, f44, f46
	;;
	(p12) STFD	[Y1] = f6,   1 * SIZE
	(p12) FMA	f14 = ALPHA, f37, f39
	(p12) STFD	[Y2] = f7,   1 * SIZE
	(p12) FMA	f15 = ALPHA, f45, f47
	;;
	(p12) STFD	[Y1] = f10,  1 * SIZE
	(p13) FMA	f6  = ALPHA, f48, f50
	(p12) STFD	[Y2] = f11,  1 * SIZE
	(p14) FMA	f7  = ALPHA, f56, f58
	;;
	(p12) STFD	[Y1] = f12,  1 * SIZE
	(p13) FMA	f10 = ALPHA, f49, f51
	(p12) STFD	[Y2] = f13,  1 * SIZE
	(p14) FMA	f11 = ALPHA, f57, f59
	;;
	(p12) STFD	[Y1] = f14,  5 * SIZE
	(p13) FMA	f12 = ALPHA, f52, f54
	(p12) STFD	[Y2] = f15,  5 * SIZE
	(p15) FMA	f13 = ALPHA, f60, f61
	;;
	(p13) STFD	[Y1] = f6,   1 * SIZE
	(p14) STFD	[YY] = f7,   1 * SIZE
	(p13) FMA	f14 = ALPHA, f53, f55
	;;
	(p13) STFD	[Y1] = f10,  1 * SIZE
	(p14) STFD	[YY] = f11,  1 * SIZE
	;;
	(p13) STFD	[Y1] = f12,  1 * SIZE
	(p15) STFD	[YY] = f13
	;;
	(p13) STFD	[Y1] = f14
	br.ret.sptk.many b0
	;;
	.align 32

.L100:
	{ .mii
	and	J =  15, N
	shr	I =  N, 4
	mov	ar.ec = 3
	}
	;;
	{ .mmi
	cmp.eq	p9, p0  =   r0, J
	cmp.eq	p7 ,p0  = 0, I
	adds	I = -1, I
	}
	;;
	{ .mmi
	(p10) STFD [YYY] = f32
	adds	PRE1 = PREFETCHSIZE * SIZE, X
	mov	ar.lc = I
	}
	{ .mib
	adds	PRE2 = PREFETCHSIZE * SIZE, Y
	tbit.z	p0, p12 = N, 3
	(p7) br.cond.dpnt  .L115
	}
	;;
	.align 32

.L112:
	{ .mmi
	(p18) STFD	[Y1] = f6
	(p16) lfetch.nt1	[PRE1], INCX16
	(p18) add Y1 = INCY, Y1
	}
	{.mmf
	(p16) LDFD	f32  = [X], INCX
	(p16) LDFD	f80  = [Y], INCY
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	;;
	{ .mmi
 	(p18) STFD	[Y1] = f7
	(p16) lfetch.excl.nt1	[PRE2], INCY16
	(p18) add Y1 = INCY, Y1
	}
	{ .mmf
	(p16) LDFD	f35  = [X], INCX
	(p16) LDFD	f83  = [Y], INCY
	(p18) FMA	f7  = ALPHA, f61, f109
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f10
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f38  = [X], INCX
	(p16) LDFD	f86  = [Y], INCY
	(p18) FMA	f10 = ALPHA, f64, f112
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f11
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f41  = [X], INCX
	(p16) LDFD	f89  = [Y], INCY
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f12
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f44  = [X], INCX
	(p16) LDFD	f92  = [Y], INCY
	(p18) FMA	f12 = ALPHA, f70, f118
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f13
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f47  = [X], INCX
	(p16) LDFD	f95  = [Y], INCY
	(p18) FMA	f13 = ALPHA, f73, f121
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f14
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f50  = [X], INCX
	(p16) LDFD	f98  = [Y], INCY
	(p18) FMA	f14 = ALPHA, f76, f124
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f15
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f53  = [X], INCX
	(p16) LDFD	f101 = [Y], INCY
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f6
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f56  = [X], INCX
	(p16) LDFD	f104 = [Y], INCY
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	;;
	{ .mmi
 	(p18) STFD	[Y1] = f7
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f59  = [X], INCX
	(p16) LDFD	f107 = [Y], INCY
	(p17) FMA	f7  = ALPHA, f36, f84
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f10
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f62  = [X], INCX
	(p16) LDFD	f110 = [Y], INCY
	(p17) FMA	f10 = ALPHA, f39, f87
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f11
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f65  = [X], INCX
	(p16) LDFD	f113 = [Y], INCY
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f12
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f68  = [X], INCX
	(p16) LDFD	f116 = [Y], INCY
	(p17) FMA	f12 = ALPHA, f45, f93
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f13
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f71  = [X], INCX
	(p16) LDFD	f119 = [Y], INCY
	(p17) FMA	f13 = ALPHA, f48, f96
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f14
	(p18) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p16) LDFD	f74  = [X], INCX
	(p16) LDFD	f122 = [Y], INCY
	(p17) FMA	f14 = ALPHA, f51, f99
	}
	;;
	{ .mmf
	(p18) STFD	[Y1] = f15
	(p18) add Y1 = INCY, Y1
	(p17) FMA	f15 = ALPHA, f54, f102
	}
	{ .mmb
	(p16) LDFD	f77  = [X], INCX
	(p16) LDFD	f125 = [Y], INCY
	br.ctop.sptk.few .L112
	}
	;;
	.align 32

.L115:
	(p12) LDFD f32 = [X], INCX
	(p12) LDFD f34 = [Y], INCY
	mov	pr = PR, -65474
	;;
	(p12) LDFD f33 = [X], INCX
	(p12) LDFD f35 = [Y], INCY
	mov	ar.lc  = ARLC
	;;
	(p12) LDFD f36 = [X], INCX
	(p12) LDFD f38 = [Y], INCY
	(p9) br.ret.sptk.many b0
	;;
	(p12) LDFD f37 = [X], INCX
	(p12) LDFD f39 = [Y], INCY
	tbit.z	p0, p13 = N, 2
	;;
	(p12) LDFD f40 = [X], INCX
	(p12) LDFD f42 = [Y], INCY
	tbit.z	p0, p14 = N, 1
	;;
	(p12) LDFD f41 = [X], INCX
	(p12) LDFD f43 = [Y], INCY
	tbit.z	p0, p15 = N, 0
	;;
	{ .mmf
	(p12) LDFD f44 = [X], INCX
	(p12) LDFD f46 = [Y], INCY
	(p12) FMA	f6  = ALPHA, f32, f34
	}
	;;
	{ .mmf
	(p12) LDFD f45 = [X], INCX
	(p12) LDFD f47 = [Y], INCY
	(p12) FMA	f7  = ALPHA, f33, f35
	}
	;;
	{ .mmf
	(p13) LDFD f48 = [X], INCX
	(p13) LDFD f50 = [Y], INCY
	(p12) FMA	f10 = ALPHA, f36, f38
	}
	;;
	{ .mmf
	(p13) LDFD f49 = [X], INCX
	(p13) LDFD f51 = [Y], INCY
	(p12) FMA	f11 = ALPHA, f37, f39
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f6
	(p12) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p13) LDFD f52 = [X], INCX
	(p13) LDFD f54 = [Y], INCY
	(p12) FMA	f12 = ALPHA, f40, f42
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f7
	(p12) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p13) LDFD f53 = [X], INCX
	(p13) LDFD f55 = [Y], INCY
	(p12) FMA	f13 = ALPHA, f41, f43
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f10
	(p12) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p14) LDFD f56 = [X], INCX
	(p14) LDFD f58 = [Y], INCY
	(p12) FMA	f14 = ALPHA, f44, f46
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f11
	(p12) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p14) LDFD f57 = [X], INCX
	(p14) LDFD f59 = [Y], INCY
	(p12) FMA	f15 = ALPHA, f45, f47
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f12
	(p12) add Y1 = INCY, Y1
	nop.i 0
	}
	{ .mmf
	(p15) LDFD f60 = [X]
	(p15) LDFD f61 = [Y]
	(p13) FMA	f6  = ALPHA, f48, f50
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f13
	(p12) add Y1 = INCY, Y1
	(p13) FMA	f7  = ALPHA, f49, f51
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f14
	(p12) add Y1 = INCY, Y1
	(p13) FMA	f10 = ALPHA, f52, f54
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f15
	(p12) add Y1 = INCY, Y1
	(p13) FMA	f11 = ALPHA, f53, f55
	}
	;;
	{ .mmf
	(p13) STFD	[Y1] = f6
	(p13) add Y1 = INCY, Y1
	(p14) FMA	f12 = ALPHA, f56, f58
	}
	;;
	{ .mmf
	(p13) STFD	[Y1] = f7
	(p13) add Y1 = INCY, Y1
	(p14) FMA	f13 = ALPHA, f57, f59
	}
	;;
	{ .mmf
	(p13) STFD	[Y1] = f10
	(p13) add Y1 = INCY, Y1
	(p15) FMA	f14 = ALPHA, f60, f61
	}
	;;
	(p13) STFD	[Y1] = f11
	(p13) add Y1 = INCY, Y1
	;;
	(p14) STFD	[Y1] = f12
	(p14) add Y1 = INCY, Y1
	;;
	(p14) STFD	[Y1] = f13
	(p14) add Y1 = INCY, Y1
	;;
	(p15) STFD	[Y1] = f14
	br.ret.sptk.many b0
	;;
	EPILOGUE

