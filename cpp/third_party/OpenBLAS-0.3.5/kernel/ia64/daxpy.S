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

#define PREFETCHSIZE  (16 * 16)

#define N	r32
#define X1	r36
#define INCX	r37
#define Y1	r38
#define INCY	r39

#define PREX	r2
#define PREY	r3

#define I	r14
#define J	r15
#define X2	r16
#define Y2	r17
#define YY1	r18
#define YY2	r19
#define INCX16	r20
#define INCY16	r21
#define X3	r26
#define YY	r27
#define PR	r30
#define ARLC	r31

#define ALPHA	f8

	PROLOGUE
	PROFCODE
	.prologue
	{ .mmi
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	tbit.nz	p10, p0 = Y1, BASE_SHIFT
	(p6) br.ret.dpnt.many b0
	}
	;;
	.body
	{ .mmi
	(p10) LDFD	f32 = [X1], INCX
	(p10) LDFD	f33 = [Y1]
	mov	PR = pr
	}
	{ .mmi
	(p10) adds N = -1, N
	mov	YY = Y1
	(p10) add  Y1 = Y1, INCY
	}
	;;
	{ .mmi
	mov	YY1 = Y1
	shladd	YY2 = INCY, 1, Y1
	mov	pr.rot= 0
	}
	{ .mmi
	sub	r8 = X1, Y1
	mov	r9 = 0xf0
	nop	__LINE__
	}
	;;
	{ .mmi
	cmp.ne	p6, p0 = SIZE, INCX
	cmp.ne	p7, p0 = SIZE, INCY
	tbit.nz	p8, p0 = X1, BASE_SHIFT
	}
	{ .mbb
	and	J =  15, N
	(p6) br.cond.dpnt .L100
	(p7) br.cond.dpnt .L100
	}
	;;
	{ .mfi
	cmp.eq	p16, p0 = r0, r0
	(p10) FMA f9 = ALPHA, f32, f33
	shr	I =  N,  4
	}
	{ .mmb
	add	X3 = X1, INCX
	and	r8 = r9, r8
	(p8) br.cond.dpnt.many .L30
	}
	;;
	{ .mmi
	cmp.eq	p11, p0  = r0, J
	adds	I = -1, I
	mov	ar.ec = 3
	}
	{ .mib
	cmp.lt	p9, p0 = 127, r8
	tbit.nz	p12, p0 = N, 3
	(p9) br.cond.dpnt.many	.L20
	}
	;;
	{ .mmi
	(p10) STFD [YY] = f9
	cmp.eq	p7 ,p0  = -1, I
	mov	ar.lc = I
	}
	{ .mib
	adds	PREX = (PREFETCHSIZE + 2) * SIZE, X1
	adds	PREY = (PREFETCHSIZE + 2) * SIZE, Y1
	(p7) br.cond.dpnt  .L15
	}
	;;
	.align 32

.L12:
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p16) lfetch.fault.nt1	[PREX], 16 * SIZE
	(p16) LDFPD	f32,  f35  = [X1], 2 * SIZE
	(p18) FMA	f7  = ALPHA, f64, f112
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p18) FMA	f10 = ALPHA, f61, f109
	}
	{ .mmf
	(p16) LDFPD	f38,  f41  = [X1], 2 * SIZE
	(p16) LDFPD	f80,  f83  = [Y1], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f70, f118
	}
	{ .mmf
	(p16) LDFPD	f44,  f47  = [X1], 2 * SIZE
	(p16) LDFPD	f86,  f89  = [Y1], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f76, f124
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p18) FMA	f14 = ALPHA, f73, f121
	}
	{ .mmf
	(p16) LDFPD	f50,  f53  = [X1], 2 * SIZE
	(p16) LDFPD	f92,  f95  = [Y1], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	{ .mmf
	(p16) LDFPD	f56,  f59  = [X1], 2 * SIZE
	(p16) LDFPD	f98,  f101 = [Y1], 2 * SIZE
	(p17) FMA	f7  = ALPHA, f39, f87
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p17) FMA	f10 = ALPHA, f36, f84
	}
	{ .mmf
	(p16) LDFPD	f62,  f65  = [X1], 2 * SIZE
	(p16) LDFPD	f104, f107 = [Y1], 2 * SIZE
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p17) FMA	f12 = ALPHA, f45, f93
	}
	{ .mmf
	(p16) LDFPD	f68,  f71  = [X1], 2 * SIZE
	(p16) LDFPD	f110, f113 = [Y1], 2 * SIZE
	(p17) FMA	f13 = ALPHA, f51, f99
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p17) FMA	f14 = ALPHA, f48, f96
	}
	{ .mmf
	(p16) LDFPD	f74,  f77  = [X1], 2 * SIZE
	(p16) LDFPD	f116, f119 = [Y1], 2 * SIZE
	(p17) FMA	f15 = ALPHA, f54, f102
	}
	;;
	{ .mmi
	(p16) lfetch.fault.excl.nt1	[PREY], 16 * SIZE
	(p16) LDFPD	f122, f125 = [Y1], 2 * SIZE
	nop	__LINE__
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	br.ctop.sptk.few .L12
	}
	;;
	.align 32

.L15:
	{ .mmi
	(p12) LDFPD	f32,  f33  = [X1], 2 * SIZE
	(p12) LDFPD	f34,  f35  = [Y1], 2 * SIZE
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p12) LDFPD	f36,  f37  = [X1], 2 * SIZE
	(p12) LDFPD	f38,  f39  = [Y1], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFPD	f40,  f41  = [X1], 2 * SIZE
	(p12) LDFPD	f42,  f43  = [Y1], 2 * SIZE
	(p11) br.ret.dpnt.many b0
	}
	;;
	{ .mmi
	(p12) LDFPD	f44,  f45  = [X1], 2 * SIZE
	(p12) LDFPD	f46,  f47  = [Y1], 2 * SIZE
	tbit.nz	p13, p0 = N, 2
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [X1], 2 * SIZE
	(p13) LDFPD	f50,  f51  = [Y1], 2 * SIZE
	tbit.nz	p14, p0 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFPD	f52,  f53  = [X1], 2 * SIZE
	(p13) LDFPD	f54,  f55  = [Y1], 2 * SIZE
	tbit.nz	p15, p0 = N, 0
	}
	;;
	{ .mmi
	(p14) LDFPD	f56,  f57  = [X1], 2 * SIZE
	(p14) LDFPD	f58,  f59  = [Y1], 2 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f60  = [X1]
	(p15) LDFD	f61  = [Y1]
	nop	__LINE__
	}
	;;
	(p12) FMA	f6  = ALPHA, f32, f34
	(p12) FMA	f7  = ALPHA, f36, f38
	(p12) FMA	f10 = ALPHA, f33, f35
	(p12) FMA	f11 = ALPHA, f37, f39
	(p12) FMA	f12 = ALPHA, f40, f42
	(p12) FMA	f13 = ALPHA, f44, f46
	(p12) FMA	f14 = ALPHA, f41, f43
	(p12) FMA	f15 = ALPHA, f45, f47
	;;
	{ .mmf
	(p12) STFD	[YY1] = f6,   1 * SIZE
	(p12) STFD	[YY2] = f7,   1 * SIZE
	(p13) FMA	f6  = ALPHA, f48, f50
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f7  = ALPHA, f52, f54
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f10,  3 * SIZE
	(p12) STFD	[YY2] = f11,  3 * SIZE
	(p13) FMA	f10 = ALPHA, f49, f51
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f11 = ALPHA, f53, f55
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f12,  1 * SIZE
	(p12) STFD	[YY2] = f13,  1 * SIZE
	(p14) FMA	f12 = ALPHA, f56, f58
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f14,  3 * SIZE
	(p12) STFD	[YY2] = f15,  3 * SIZE
	(p14) FMA	f13 = ALPHA, f57, f59
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f6,   1 * SIZE
	(p13) STFD	[YY2] = f7,   1 * SIZE
	(p15) FMA	f14 = ALPHA, f60, f61
	}
	;;
	{ .mmi
	(p13) STFD	[YY1] = f10,  3 * SIZE
	(p13) STFD	[YY2] = f11,  3 * SIZE
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f12,  1 * SIZE
	;;
	(p14) STFD	[YY1] = f13,  1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f14
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L20:
	{ .mmi
	(p10) STFD [YY] = f9
	cmp.eq	p7 ,p0  = -1, I
	mov	ar.lc = I
	}
	{ .mib
	adds	PREX = (PREFETCHSIZE - 4) * SIZE, X1
	adds	PREY = (PREFETCHSIZE + 2) * SIZE, Y1
	(p7) br.cond.dpnt  .L25
	}
	;;
	.align 32

.L22:
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p16) lfetch.fault.nt1	[PREX], 16 * SIZE
	(p17) LDFPD	f57,  f60  = [X1], 2 * SIZE
	(p18) FMA	f7  = ALPHA, f64, f112
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p18) FMA	f10 = ALPHA, f61, f109
	}
	{ .mmf
	(p16) lfetch.fault.excl.nt1	[PREY], 16 * SIZE
	(p16) LDFPD	f80,  f83  = [Y1], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f70, f118
	}
	{ .mmf
	(p17) LDFPD	f63,  f66  = [X1], 2 * SIZE
	(p16) LDFPD	f86,  f89  = [Y1], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f76, f124
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p18) FMA	f14 = ALPHA, f73, f121
	}
	{ .mmf
	(p17) LDFPD	f69,  f72  = [X1], 2 * SIZE
	(p16) LDFPD	f92,  f95  = [Y1], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	{ .mmf
	(p17) LDFPD	f75,  f78  = [X1], 2 * SIZE
	(p16) LDFPD	f98,  f101 = [Y1], 2 * SIZE
	(p17) FMA	f7  = ALPHA, f39, f87
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p17) FMA	f10 = ALPHA, f36, f84
	}
	{ .mmf
	(p16) LDFPD	f32,  f35  = [X1], 2 * SIZE
	(p16) LDFPD	f104, f107 = [Y1], 2 * SIZE
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p17) FMA	f12 = ALPHA, f45, f93
	}
	{ .mmf
	(p16) LDFPD	f38,  f41  = [X1], 2 * SIZE
	(p16) LDFPD	f110, f113 = [Y1], 2 * SIZE
	(p17) FMA	f13 = ALPHA, f51, f99
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p17) FMA	f14 = ALPHA, f48, f96
	}
	{ .mmf
	(p16) LDFPD	f44,  f47  = [X1], 2 * SIZE
	(p16) LDFPD	f116, f119 = [Y1], 2 * SIZE
	(p17) FMA	f15 = ALPHA, f54, f102
	}
	;;
	{ .mmi
	(p16) LDFPD	f50,  f53  = [X1], 2 * SIZE
	(p16) LDFPD	f122, f125 = [Y1], 2 * SIZE
	nop	__LINE__
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	br.ctop.sptk.few .L22
	}
	;;
	.align 32

.L25:
	{ .mmi
	(p12) LDFPD	f32,  f33  = [X1], 2 * SIZE
	(p12) LDFPD	f34,  f35  = [Y1], 2 * SIZE
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p12) LDFPD	f36,  f37  = [X1], 2 * SIZE
	(p12) LDFPD	f38,  f39  = [Y1], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFPD	f40,  f41  = [X1], 2 * SIZE
	(p12) LDFPD	f42,  f43  = [Y1], 2 * SIZE
	(p11) br.ret.dpnt.many b0
	}
	;;
	{ .mmi
	(p12) LDFPD	f44,  f45  = [X1], 2 * SIZE
	(p12) LDFPD	f46,  f47  = [Y1], 2 * SIZE
	tbit.nz	p13, p0 = N, 2
	}
	;;
	{ .mmi
	(p13) LDFPD	f48,  f49  = [X1], 2 * SIZE
	(p13) LDFPD	f50,  f51  = [Y1], 2 * SIZE
	tbit.nz	p14, p0 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFPD	f52,  f53  = [X1], 2 * SIZE
	(p13) LDFPD	f54,  f55  = [Y1], 2 * SIZE
	tbit.nz	p15, p0 = N, 0
	}
	;;
	{ .mmi
	(p14) LDFPD	f56,  f57  = [X1], 2 * SIZE
	(p14) LDFPD	f58,  f59  = [Y1], 2 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f60  = [X1]
	(p15) LDFD	f61  = [Y1]
	nop	__LINE__
	}
	;;
	(p12) FMA	f6  = ALPHA, f32, f34
	(p12) FMA	f7  = ALPHA, f36, f38
	(p12) FMA	f10 = ALPHA, f33, f35
	(p12) FMA	f11 = ALPHA, f37, f39
	(p12) FMA	f12 = ALPHA, f40, f42
	(p12) FMA	f13 = ALPHA, f44, f46
	(p12) FMA	f14 = ALPHA, f41, f43
	(p12) FMA	f15 = ALPHA, f45, f47
	;;
	{ .mmf
	(p12) STFD	[YY1] = f6,   1 * SIZE
	(p12) STFD	[YY2] = f7,   1 * SIZE
	(p13) FMA	f6  = ALPHA, f48, f50
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f7  = ALPHA, f52, f54
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f10,  3 * SIZE
	(p12) STFD	[YY2] = f11,  3 * SIZE
	(p13) FMA	f10 = ALPHA, f49, f51
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f11 = ALPHA, f53, f55
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f12,  1 * SIZE
	(p12) STFD	[YY2] = f13,  1 * SIZE
	(p14) FMA	f12 = ALPHA, f56, f58
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f14,  3 * SIZE
	(p12) STFD	[YY2] = f15,  3 * SIZE
	(p14) FMA	f13 = ALPHA, f57, f59
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f6,   1 * SIZE
	(p13) STFD	[YY2] = f7,   1 * SIZE
	(p15) FMA	f14 = ALPHA, f60, f61
	}
	;;
	{ .mmi
	(p13) STFD	[YY1] = f10,  3 * SIZE
	(p13) STFD	[YY2] = f11,  3 * SIZE
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f12,  1 * SIZE
	;;
	(p14) STFD	[YY1] = f13,  1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f14
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L30:
	{ .mmi
	cmp.eq	p11, p0  = r0, J
	adds	I = -1, I
	mov	ar.ec = 3
	}
	{ .mib
	cmp.lt	p9, p0 = 127, r8
	tbit.nz	p12, p0 = N, 3
	(p9) br.cond.dptk.many	.L40
	}
	;;
	{ .mmi
	(p10) STFD [YY] = f9
	cmp.eq	p7 ,p0  = -1, I
	mov	ar.lc = I
	}
	{ .mib
	adds	PREX = (PREFETCHSIZE + 2) * SIZE, X1
	adds	PREY = (PREFETCHSIZE + 2) * SIZE, Y1
	(p7) br.cond.dpnt  .L35
	}
	;;
	.align 32

.L32:
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p16) lfetch.fault.nt1	[PREX], 16 * SIZE
	(p16) LDFD	f32  = [X1], 1 * SIZE
	(p18) FMA	f7  = ALPHA, f64, f112
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p18) FMA	f10 = ALPHA, f61, f109
	}
	{ .mmf
	(p16) LDFPD	f35,  f38  = [X1], 2 * SIZE
	(p16) LDFPD	f80,  f83  = [Y1], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p18) FMA	f12 = ALPHA, f70, f118
	}
	{ .mmf
	(p16) LDFPD	f41,  f44  = [X1], 2 * SIZE
	(p16) LDFPD	f86,  f89  = [Y1], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f76, f124
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p18) FMA	f14 = ALPHA, f73, f121
	}
	{ .mmf
	(p16) LDFPD	f47,  f50  = [X1], 2 * SIZE
	(p16) LDFPD	f92,  f95  = [Y1], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	{ .mmf
	(p16) LDFPD	f53,  f56  = [X1], 2 * SIZE
	(p16) LDFPD	f98,  f101 = [Y1], 2 * SIZE
	(p17) FMA	f7  = ALPHA, f39, f87
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p17) FMA	f10 = ALPHA, f36, f84
	}
	{ .mmf
	(p16) LDFPD	f59,  f62  = [X1], 2 * SIZE
	(p16) LDFPD	f104, f107 = [Y1], 2 * SIZE
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p17) FMA	f12 = ALPHA, f45, f93
	}
	{ .mmf
	(p16) LDFPD	f65,  f68  = [X1], 2 * SIZE
	(p16) LDFPD	f110, f113 = [Y1], 2 * SIZE
	(p17) FMA	f13 = ALPHA, f51, f99
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p17) FMA	f14 = ALPHA, f48, f96
	}
	{ .mmf
	(p16) LDFPD	f71,  f74  = [X1], 2 * SIZE
	(p16) LDFPD	f116, f119 = [Y1], 2 * SIZE
	(p17) FMA	f15 = ALPHA, f54, f102
	}
	;;
	{ .mmi
	(p16) lfetch.fault.excl.nt1	[PREY], 16 * SIZE
	(p16) LDFPD	f122, f125 = [Y1], 2 * SIZE
	adds	X3 = 1 * SIZE, X1
	}
	{ .mmb
	(p16) LDFD	f77  = [X1], 1 * SIZE
	nop	__LINE__
	br.ctop.sptk.few .L32
	}
	;;
	.align 32

.L35:
	{ .mmi
	(p12) LDFPD	f33,  f36  = [X3]
	(p12) LDFPD	f34,  f35  = [Y1], 2 * SIZE
	mov	pr = PR, -65474
	}
	{ .mmi
	(p12) LDFD	f32  = [X1], 3 * SIZE
	(p12) adds	X3 = 8 * SIZE, X3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p12) LDFPD	f37,  f40  = [X1], 2 * SIZE
	(p12) LDFPD	f38,  f39  = [Y1], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFPD	f41,  f44  = [X1], 2 * SIZE
	(p12) LDFPD	f42,  f43  = [Y1], 2 * SIZE
	(p11) br.ret.dpnt.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f45  = [X1], 1 * SIZE
	(p12) LDFPD	f46,  f47  = [Y1], 2 * SIZE
	tbit.nz	p13, p0 = N, 2
	}
	;;
	{ .mmi
	(p13) LDFPD	f49,  f52  = [X3]
	(p13) LDFPD	f50,  f51  = [Y1], 2 * SIZE
	tbit.nz	p14, p0 = N, 1
	}
	{ .mmi
	(p13) LDFD	f48  = [X1], 3 * SIZE
	(p13) adds	X3 = 4 * SIZE, X3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LDFD	f53  = [X1], 1 * SIZE
	(p13) LDFPD	f54,  f55  = [Y1], 2 * SIZE
	tbit.nz	p15, p0 = N, 0
	}
	;;
	{ .mmi
	(p14) LDFD	f56  = [X1], 2 * SIZE
	(p14) LDFPD	f58,  f59  = [Y1], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p14) LDFD	f57  = [X3]
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f60  = [X1]
	(p15) LDFD	f61  = [Y1]
	nop	__LINE__
	}
	;;
	(p12) FMA	f6  = ALPHA, f32, f34
	(p12) FMA	f7  = ALPHA, f36, f38
	(p12) FMA	f10 = ALPHA, f33, f35
	(p12) FMA	f11 = ALPHA, f37, f39
	(p12) FMA	f12 = ALPHA, f40, f42
	(p12) FMA	f13 = ALPHA, f44, f46
	(p12) FMA	f14 = ALPHA, f41, f43
	(p12) FMA	f15 = ALPHA, f45, f47
	;;
	{ .mmf
	(p12) STFD	[YY1] = f6,   1 * SIZE
	(p12) STFD	[YY2] = f7,   1 * SIZE
	(p13) FMA	f6  = ALPHA, f48, f50
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f7  = ALPHA, f52, f54
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f10,  3 * SIZE
	(p12) STFD	[YY2] = f11,  3 * SIZE
	(p13) FMA	f10 = ALPHA, f49, f51
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f11 = ALPHA, f53, f55
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f12,  1 * SIZE
	(p12) STFD	[YY2] = f13,  1 * SIZE
	(p14) FMA	f12 = ALPHA, f56, f58
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f14,  3 * SIZE
	(p12) STFD	[YY2] = f15,  3 * SIZE
	(p14) FMA	f13 = ALPHA, f57, f59
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f6,   1 * SIZE
	(p13) STFD	[YY2] = f7,   1 * SIZE
	(p15) FMA	f14 = ALPHA, f60, f61
	}
	;;
	{ .mmi
	(p13) STFD	[YY1] = f10,  3 * SIZE
	(p13) STFD	[YY2] = f11,  3 * SIZE
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f12,  1 * SIZE
	;;
	(p14) STFD	[YY1] = f13,  1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f14
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L40:
	{ .mmi
	(p10) STFD [YY] = f9
	cmp.eq	p7 ,p0  = -1, I
	mov	ar.lc = I
	}
	{ .mib
	adds	PREX = (PREFETCHSIZE + 2) * SIZE, X1
	adds	PREY = (PREFETCHSIZE + 8) * SIZE, Y1
	(p7) br.cond.dpnt  .L45
	}
	;;
	.align 32

.L42:
	{ .mmf
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmf
	(p17) LDFPD	f54,  f57  = [X1], 2 * SIZE
	(p16) LDFPD	f80,  f83  = [Y1], 2 * SIZE
	(p18) FMA	f7  = ALPHA, f64, f112
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p18) FMA	f64 = ALPHA, f61, f109
	}
	{ .mmf
	(p17) LDFPD	f60,  f63  = [X1], 2 * SIZE
	(p16) LDFPD	f86,  f89  = [Y1], 2 * SIZE
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p18) FMA	f67 = ALPHA, f70, f118
	}
	{ .mmf
	(p17) LDFPD	f66,  f69  = [X1], 2 * SIZE
	(p16) LDFPD	f92,  f95  = [Y1], 2 * SIZE
	(p18) FMA	f13 = ALPHA, f76, f124
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14,  3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p18) FMA	f9  = ALPHA, f73, f121
	}
	{ .mmf
	(p17) LDFPD	f72,  f75  = [X1], 2 * SIZE
	(p16) LDFPD	f98,  f101 = [Y1], 2 * SIZE
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	;;
	{ .mmi
	(p18) STFD	[YY1] = f6,   1 * SIZE
	(p18) STFD	[YY2] = f7,   1 * SIZE
	(p17) adds	X3 = 2 * SIZE, X1
	}
	{ .mmf
	(p16) LDFPD	f104, f107 = [Y1], 2 * SIZE
	(p17) LDFD	f78  = [X1], 1 * SIZE
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	;;
	{ .mmf
	(p16) LDFPD	f110, f113 = [Y1], 2 * SIZE
	(p16) lfetch.fault.nt1	[PREX], 16 * SIZE
	(p17) FMA	f7  = ALPHA, f39, f87
	}
	{ .mmf
	(p16) LDFD	f32  = [X1], 1 * SIZE
	(p17) FMA	f10 = ALPHA, f36, f84
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f64,  3 * SIZE
	(p18) STFD	[YY2] = f11,  3 * SIZE
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	{ .mmf
	(p16) LDFPD	f35,  f38  = [X1], 2 * SIZE
	(p16) LDFPD	f116, f119 = [Y1], 2 * SIZE
	(p17) FMA	f12 = ALPHA, f45, f93
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f67,  1 * SIZE
	(p18) STFD	[YY2] = f13,  1 * SIZE
	(p17) FMA	f13 = ALPHA, f51, f99
	}
	{ .mmf
	(p16) LDFPD	f41,  f44  = [X1], 2 * SIZE
	(p16) LDFPD	f122, f125 = [Y1], 2 * SIZE
	(p17) FMA	f14 = ALPHA, f48, f96
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f9,   3 * SIZE
	(p18) STFD	[YY2] = f15,  3 * SIZE
	(p17) FMA	f15 = ALPHA, f54, f102
	}
	{ .mmb
	(p16) lfetch.fault.excl.nt1	[PREY], 16 * SIZE
	(p16) LDFPD	f47,  f50  = [X1], 2 * SIZE
	br.ctop.sptk.few .L42
	}
	;;
	.align 32

.L45:
	{ .mmi
	(p12) LDFPD	f33,  f36  = [X3]
	(p12) LDFPD	f34,  f35  = [Y1], 2 * SIZE
	mov	pr = PR, -65474
	}
	{ .mmi
	(p12) LDFD	f32  = [X1], 3 * SIZE
	(p12) adds	X3 = 8 * SIZE, X3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p12) LDFPD	f37,  f40  = [X1], 2 * SIZE
	(p12) LDFPD	f38,  f39  = [Y1], 2 * SIZE
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFPD	f41,  f44  = [X1], 2 * SIZE
	(p12) LDFPD	f42,  f43  = [Y1], 2 * SIZE
	(p11) br.ret.dpnt.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f45  = [X1], 1 * SIZE
	(p12) LDFPD	f46,  f47  = [Y1], 2 * SIZE
	tbit.nz	p13, p0 = N, 2
	}
	;;
	{ .mmi
	(p13) LDFPD	f49,  f52  = [X3]
	(p13) LDFPD	f50,  f51  = [Y1], 2 * SIZE
	tbit.nz	p14, p0 = N, 1
	}
	{ .mmi
	(p13) LDFD	f48  = [X1], 3 * SIZE
	(p13) adds	X3 = 4 * SIZE, X3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LDFD	f53  = [X1], 1 * SIZE
	(p13) LDFPD	f54,  f55  = [Y1], 2 * SIZE
	tbit.nz	p15, p0 = N, 0
	}
	;;
	{ .mmi
	(p14) LDFD	f56  = [X1], 2 * SIZE
	(p14) LDFPD	f58,  f59  = [Y1], 2 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p14) LDFD	f57  = [X3]
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p15) LDFD	f60  = [X1]
	(p15) LDFD	f61  = [Y1]
	nop	__LINE__
	}
	;;
	(p12) FMA	f6  = ALPHA, f32, f34
	(p12) FMA	f7  = ALPHA, f36, f38
	(p12) FMA	f10 = ALPHA, f33, f35
	(p12) FMA	f11 = ALPHA, f37, f39
	(p12) FMA	f12 = ALPHA, f40, f42
	(p12) FMA	f13 = ALPHA, f44, f46
	(p12) FMA	f14 = ALPHA, f41, f43
	(p12) FMA	f15 = ALPHA, f45, f47
	;;
	{ .mmf
	(p12) STFD	[YY1] = f6,   1 * SIZE
	(p12) STFD	[YY2] = f7,   1 * SIZE
	(p13) FMA	f6  = ALPHA, f48, f50
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f7  = ALPHA, f52, f54
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f10,  3 * SIZE
	(p12) STFD	[YY2] = f11,  3 * SIZE
	(p13) FMA	f10 = ALPHA, f49, f51
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p13) FMA	f11 = ALPHA, f53, f55
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f12,  1 * SIZE
	(p12) STFD	[YY2] = f13,  1 * SIZE
	(p14) FMA	f12 = ALPHA, f56, f58
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f14,  3 * SIZE
	(p12) STFD	[YY2] = f15,  3 * SIZE
	(p14) FMA	f13 = ALPHA, f57, f59
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f6,   1 * SIZE
	(p13) STFD	[YY2] = f7,   1 * SIZE
	(p15) FMA	f14 = ALPHA, f60, f61
	}
	;;
	{ .mmi
	(p13) STFD	[YY1] = f10,  3 * SIZE
	(p13) STFD	[YY2] = f11,  3 * SIZE
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f12,  1 * SIZE
	;;
	(p14) STFD	[YY1] = f13,  1 * SIZE
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f14
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L100:
	{ .mfi
	cmp.eq	p16, p0 = r0, r0
	(p10) FMA f9 = ALPHA, f32, f33
	shr	I =  N,  4
	}
	;;
	{ .mmi
	cmp.eq	p11, p0  = r0, J
	adds	I = -1, I
	mov	ar.ec = 3
	}
	{ .mmi
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
	tbit.nz	p12, p0 = N, 3
	}
	;;
	{ .mmi
	(p10) STFD [YY] = f9
	cmp.eq	p7 ,p0  = -1, I
	mov	ar.lc = I
	}
	{ .mib
	adds	PREX = (PREFETCHSIZE + 2) * SIZE, X1
	adds	PREY = (PREFETCHSIZE + 2) * SIZE, Y1
	(p7) br.cond.dpnt  .L115
	}
	;;
	.align 32

.L112:
	{ .mmf
	(p18) STFD	[YY1] = f6
	(p16) lfetch.fault.nt1	[PREX], INCX16
	(p18) FMA	f12 = ALPHA, f46, f94
	}
	{ .mmi
	(p16) LDFD	f32  = [X1], INCX
	(p16) LDFD	f80  = [Y1], INCY
	(p18) add	YY1 = YY1, INCY
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f7
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f13 = ALPHA, f49, f97
	}
	{ .mmi
	(p16) LDFD	f35  = [X1], INCX
	(p16) LDFD	f83  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f14 = ALPHA, f52, f100
	}
	{ .mmi
	(p16) LDFD	f38  = [X1], INCX
	(p16) LDFD	f86  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f11
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f15 = ALPHA, f55, f103
	}
	{ .mmi
	(p16) LDFD	f41  = [X1], INCX
	(p16) LDFD	f89  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f6  = ALPHA, f58, f106
	}
	{ .mmi
	(p16) LDFD	f44  = [X1], INCX
	(p16) LDFD	f92  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f13
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f7  = ALPHA, f61, f109
	}
	{ .mmi
	(p16) LDFD	f47  = [X1], INCX
	(p16) LDFD	f95  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f10 = ALPHA, f64, f112
	}
	{ .mmi
	(p16) LDFD	f50  = [X1], INCX
	(p16) LDFD	f98  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f15
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f11 = ALPHA, f67, f115
	}
	{ .mmi
	(p16) LDFD	f53  = [X1], INCX
	(p16) LDFD	f101 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f6
	(p16) lfetch.fault.excl.nt1	[PREY], INCY16
	(p18) FMA	f12 = ALPHA, f70, f118
	}
	{ .mmi
	(p16) LDFD	f56  = [X1], INCX
	(p16) LDFD	f104 = [Y1], INCY
	(p18) add	YY1 = YY1, INCY
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f7
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f13 = ALPHA, f73, f121
	}
	{ .mmi
	(p16) LDFD	f59  = [X1], INCX
	(p16) LDFD	f107 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f10
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f14 = ALPHA, f76, f124
	}
	{ .mmi
	(p16) LDFD	f62  = [X1], INCX
	(p16) LDFD	f110 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f11
	(p18) add	YY1 = YY1, INCY
	(p18) FMA	f15 = ALPHA, f79, f127
	}
	{ .mmi
	(p16) LDFD	f65  = [X1], INCX
	(p16) LDFD	f113 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f12
	(p18) add	YY1 = YY1, INCY
	(p17) FMA	f6  = ALPHA, f33, f81
	}
	{ .mmi
	(p16) LDFD	f68  = [X1], INCX
	(p16) LDFD	f116 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f13
	(p18) add	YY1 = YY1, INCY
	(p17) FMA	f7  = ALPHA, f36, f84
	}
	{ .mmi
	(p16) LDFD	f71  = [X1], INCX
	(p16) LDFD	f119 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f14
	(p18) add	YY1 = YY1, INCY
	(p17) FMA	f10 = ALPHA, f39, f87
	}
	{ .mmi
	(p16) LDFD	f74  = [X1], INCX
	(p16) LDFD	f122 = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p18) STFD	[YY1] = f15
	(p18) add	YY1 = YY1, INCY
	(p17) FMA	f11 = ALPHA, f42, f90
	}
	{ .mmb
	(p16) LDFD	f77  = [X1], INCX
	(p16) LDFD	f125 = [Y1], INCY
	br.ctop.sptk.few .L112
	}
	;;
	.align 32

.L115:
	{ .mmi
	(p12) LDFD	f32  = [X1], INCX
	(p12) LDFD	f34  = [Y1], INCY
	mov	pr = PR, -65474
	}
	;;
	{ .mmi
	(p12) LDFD	f33  = [X1], INCX
	(p12) LDFD	f35  = [Y1], INCY
	mov	ar.lc  = ARLC
	}
	;;
	{ .mmb
	(p12) LDFD	f36  = [X1], INCX
	(p12) LDFD	f38  = [Y1], INCY
	(p11) br.ret.dpnt.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f37  = [X1], INCX
	(p12) LDFD	f39  = [Y1], INCY
	tbit.nz	p13, p0 = N, 2
	}
	;;
	{ .mmi
	(p12) LDFD	f40  = [X1], INCX
	(p12) LDFD	f42  = [Y1], INCY
	tbit.nz	p14, p0 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFD	f41  = [X1], INCX
	(p12) LDFD	f43  = [Y1], INCY
	tbit.nz	p15, p0 = N, 0
	}
	;;
	{ .mmf
	(p12) LDFD	f44  = [X1], INCX
	(p12) LDFD	f46  = [Y1], INCY
	(p12) FMA	f6  = ALPHA, f32, f34
	}
	;;
	{ .mmf
	(p12) LDFD	f45  = [X1], INCX
	(p12) LDFD	f47  = [Y1], INCY
	(p12) FMA	f7  = ALPHA, f33, f35
	}
	;;
	{ .mmf
	(p13) LDFD	f48  = [X1], INCX
	(p13) LDFD	f50  = [Y1], INCY
	(p12) FMA	f10 = ALPHA, f36, f38
	}
	;;
	{ .mmf
	(p13) LDFD	f49  = [X1], INCX
	(p13) LDFD	f51  = [Y1], INCY
	(p12) FMA	f11 = ALPHA, f37, f39
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f6
	(p12) add	YY1 = YY1, INCY
	(p12) FMA	f12 = ALPHA, f40, f42
	}
	{ .mmi
	(p13) LDFD	f52  = [X1], INCX
	(p13) LDFD	f54  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f7
	(p12) add	YY1 = YY1, INCY
	(p12) FMA	f13 = ALPHA, f41, f43
	}
	{ .mmi
	(p13) LDFD	f53  = [X1], INCX
	(p13) LDFD	f55  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f10
	(p12) add	YY1 = YY1, INCY
	(p12) FMA	f14 = ALPHA, f44, f46
	}
	{ .mmi
	(p14) LDFD	f56  = [X1], INCX
	(p14) LDFD	f58  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f11
	(p12) add	YY1 = YY1, INCY
	(p12) FMA	f15 = ALPHA, f45, f47
	}
	{ .mmi
	(p14) LDFD	f57  = [X1], INCX
	(p14) LDFD	f59  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f12
	(p12) add	YY1 = YY1, INCY
	(p13) FMA	f6  = ALPHA, f48, f50
	}
	{ .mmi
	(p15) LDFD	f60  = [X1], INCX
	(p15) LDFD	f61  = [Y1], INCY
	nop   __LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f13
	(p12) add	YY1 = YY1, INCY
	(p13) FMA	f7  = ALPHA, f49, f51
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f14
	(p12) add	YY1 = YY1, INCY
	(p13) FMA	f10 = ALPHA, f52, f54
	}
	;;
	{ .mmf
	(p12) STFD	[YY1] = f15
	(p12) add	YY1 = YY1, INCY
	(p13) FMA	f11 = ALPHA, f53, f55
	}
	;;
	;;
	{ .mmf
	(p13) STFD	[YY1] = f6
	(p13) add	YY1 = YY1, INCY
	(p14) FMA	f12 = ALPHA, f56, f58
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f7
	(p13) add	YY1 = YY1, INCY
	(p14) FMA	f13 = ALPHA, f57, f59
	}
	;;
	{ .mmf
	(p13) STFD	[YY1] = f10
	(p13) add	YY1 = YY1, INCY
	(p15) FMA	f14 = ALPHA, f60, f61
	}
	;;
	{ .mmi
	(p13) STFD	[YY1] = f11
	(p13) add	YY1 = YY1, INCY
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f12
	(p14) add	YY1 = YY1, INCY
	nop   __LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[YY1] = f13
	(p14) add	YY1 = YY1, INCY
	nop   __LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[YY1] = f14
	nop   __LINE__
	br.ret.sptk.many b0
	}
	;;


	EPILOGUE

