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

#ifdef DOUBLE
#define PREFETCH_SIZE (8 * 16)
#else
#define PREFETCH_SIZE (1 * 64)
#endif

#define ALPHA		f8

#define N		r32
#define X1		r36
#define INCX		r37

#define	X2		r14
#define Y1		r15
#define Y2		r16
#define PRE1		r17
#define I		r18
#define NAND15		r19
#define INCX5		r20
#define INCX16		r21
#define XX		r22
#define PR		r30
#define ARLC		r31

	PROLOGUE
	.prologue
	PROFCODE

	{ .mfi
	shladd	INCX = INCX, BASE_SHIFT, r0
	fcmp.eq	p0, p6 = ALPHA, f0
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.ge	p7, p0 = 0, N
	tbit.z p0, p10 = X1, BASE_SHIFT
	(p7) br.ret.sptk.many b0
	}
	.body
	;;
	{ .mmi
	mov	XX = X1
	(p10) LDFD f32 = [X1], INCX
	mov	PR = pr
	}
	{ .mmi
	shladd	INCX5  = INCX, 2, INCX
	shladd	INCX16 = INCX, 4, r0
	(p10) adds N = -1, N
	}
	;;
	{ .mmi
	shladd	X2 = INCX, 2, X1
	nop	__LINE__
	mov	ar.ec = 5
	}
	{ .mmi
	and	NAND15 = 15, N
	nop	__LINE__
	shr	I = N, 4
	}
	;;
	{ .mmi
	adds	I = -1, I
	nop	__LINE__
	tbit.z	p0, p12 = N, 3
	}
	{ .mmb
	cmp.ge	p9, p0 = 0, NAND15
	adds PRE1 = PREFETCH_SIZE * SIZE + 192, XX
	(p6) br.cond.dptk .L100		// if (alpha != 0) goto L3
	}
	;;
	{ .mmi
	(p10) STFD [XX] = f0
	nop   __LINE__
	mov	ar.lc = I
	}
	{ .mmb
	cmp.gt	p8, p0 = 0, I
	(p8) br.cond.dpnt .L30
	}
	;;
	.align 32

.L20:
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	lfetch.excl.nt1	[PRE1], INCX16
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop	__LINE__
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop	__LINE__
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	add	X1 = INCX5, X1
	add	X2 = INCX5, X2
	nop	__LINE__
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop	__LINE__
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop	__LINE__
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop	__LINE__
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop	__LINE__
	}
	{.mmb
	add	X1 = INCX5, X1
	add	X2 = INCX5, X2
	br.cloop.sptk.few .L20
	}
	;;
	.align 16

.L30:
	{ .mmi
	(p12) STFD	[X1] = f0
	(p12) STFD	[X2] = f0
	mov	ar.lc = ARLC
	}
	{ .mmb
	(p12) add	X1 = INCX, X1
	(p12) add	X2 = INCX, X2
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) STFD	[X1]  = f0
	(p12) add	X1 = INCX, X1
	tbit.z	p0, p13 = N, 2
	}
	{ .mmi
	(p12) STFD	[X2] = f0
	(p12) add	X2 = INCX, X2
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p12) STFD	[X1]  = f0
	(p12) add	X1 = INCX, X1
	tbit.z	p0, p15 = N, 0
	}
	{ .mmb
	(p12) STFD	[X2] = f0
	(p12) add	X2 = INCX, X2
	nop	__LINE__
	}
	;;
	{ .mmb
	(p12) STFD	[X1]  = f0
	(p12) add	X1 = INCX5, X1
	nop	__LINE__
	}
	{ .mmb
	(p12) STFD	[X2] = f0
	(p12) add	X2 = INCX5, X2
	nop	__LINE__
	}
	;;
	{ .mmb
	(p13) STFD	[X1]  = f0
	(p13) add	X1 = INCX, X1
	nop	__LINE__
	}
	;;
	{ .mmb
	(p13) STFD	[X1]  = f0
	(p13) add	X1 = INCX, X1
	nop	__LINE__
	}
	;;
	{ .mmb
	(p13) STFD	[X1]  = f0
	(p13) add	X1 = INCX, X1
	nop	__LINE__
	}
	;;
	{ .mmb
	(p13) STFD	[X1]  = f0
	(p13) add	X1 = INCX, X1
	nop	__LINE__
	}
	;;
	{ .mmb
	(p14) STFD	[X1]  = f0
	(p14) add	X1 = INCX, X1
	nop	__LINE__
	}
	;;
	{ .mmb
	(p14) STFD	[X1]  = f0
	(p14) add	X1 = INCX, X1
	nop	__LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[X1]  = f0
	nop	__LINE__
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L100:
	{ .mmi
	mov	Y1 = X1
	shladd	Y2 = INCX, 2, X1
	mov	pr.rot = 0
	}
	{ .mmf
	cmp.gt	p8, p0 = 0, I
	shladd	X2 = INCX, 2, X1
	(p10) FMPY f32 = ALPHA, f32
	}
	;;
	{ .mmi
	(p10) STFD [XX] = f32
	cmp.eq	p0, p7 = SIZE, INCX
	mov	ar.lc = I
	}
	{ .mbb
	cmp.eq	p16, p0 = r0, r0
	(p7)  br.cond.dpnt .L300
	(p8)  br.cond.dpnt .L120
	}
	;;
	.align 32

.L110:
	{ .mmf
	(p21) STFD	[Y1] = f6,   1 * SIZE
	(p21) STFD	[Y2] = f7,   1 * SIZE
	(p20) FMPY	f112 = ALPHA, f36
	}
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE1], 16 * SIZE
	(p16) LDFPD	f32,  f37  = [X1], 2 * SIZE
	(p20) FMPY	f113 = ALPHA, f56
	}
	;;
	{ .mmf
	(p21) STFD	[Y1] = f10,  1 * SIZE
	(p21) STFD	[Y2] = f11,  1 * SIZE
	(p20) FMPY	f114 = ALPHA, f41
	}
	{ .mfi
	(p16) LDFPD	f42,  f47  = [X1], 2 * SIZE
	(p20) FMPY	f115 = ALPHA, f61
	nop	__LINE__
	}
	;;
	{ .mmf
	(p21) STFD	[Y1] = f12,  1 * SIZE
	(p21) STFD	[Y2] = f13,  1 * SIZE
	(p20) FMPY	f116 = ALPHA, f46
	}
	{ .mfi
	(p16) LDFPD	f52,  f57  = [X1], 2 * SIZE
	(p20) FMPY	f117 = ALPHA, f66
	nop	__LINE__
	}
	;;
	{ .mmf
	(p21) STFD	[Y1] = f14,  5 * SIZE
	(p21) STFD	[Y2] = f15,  5 * SIZE
	(p20) FMPY	f118 = ALPHA, f51
	}
	{ .mfi
	(p16) LDFPD	f62,  f67  = [X1], 2 * SIZE
	(p20) FMPY	f119 = ALPHA, f71
	nop	__LINE__
	}
	;;
	{ .mmf
	(p20) STFD	[Y1] = f112,  1 * SIZE
	(p20) STFD	[Y2] = f113,  1 * SIZE
	(p20) FMPY	f6   = ALPHA, f76
	}
	{ .mfi
	(p16) LDFPD	f72,  f77  = [X1], 2 * SIZE
	(p20) FMPY	f7   = ALPHA, f96
	nop	__LINE__
	}
	;;
	{ .mmf
	(p20) STFD	[Y1] = f114,  1 * SIZE
	(p20) STFD	[Y2] = f115,  1 * SIZE
	(p20) FMPY	f10  = ALPHA, f81
	}
	{ .mfi
	(p16) LDFPD	f82,  f87  = [X1], 2 * SIZE
	(p20) FMPY	f11  = ALPHA, f101
	nop	__LINE__
	}
	;;
	{ .mmf
	(p20) STFD	[Y1] = f116,  1 * SIZE
	(p20) STFD	[Y2] = f117,  1 * SIZE
	(p20) FMPY	f12  = ALPHA, f86
	}
	{ .mfi
	(p16) LDFPD	f92,  f97  = [X1], 2 * SIZE
	(p20) FMPY	f13  = ALPHA, f106
	(p20) shladd	X2 = INCX, 2, X1
	}
	;;
	{ .mmf
	(p20) STFD	[Y1] = f118,  5 * SIZE
	(p20) STFD	[Y2] = f119,  5 * SIZE
	(p20) FMPY	f14  = ALPHA, f91
	}
	{ .mfb
	(p16) LDFPD	f102, f107 = [X1], 2 * SIZE
	(p20) FMPY	f15  = ALPHA, f111
	br.ctop.sptk.few .L110
	}
	;;
	.align 32

.L120:
	{ .mmi
	(p21) STFD	[Y1] = f6,   1 * SIZE
	(p21) STFD	[Y2] = f7,   1 * SIZE
	tbit.z	p0, p13 = N, 2
	}
	{ .mmi
	(p12) LDFPD	f32, f33 = [X1], 2 * SIZE
	(p12) LDFPD	f36, f37 = [X2], 2 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f10,  1 * SIZE
	(p21) STFD	[Y2] = f11,  1 * SIZE
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) LDFPD	f34, f35 = [X1]
	(p12) LDFPD	f38, f39 = [X2]
	(p12) adds	X1 = 6 * SIZE,X1
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f12,  1 * SIZE
	(p21) STFD	[Y2] = f13,  1 * SIZE
	tbit.z	p0, p14 = N, 1
	}
	{ .mmi
	(p13) LDFPD	f40, f41 = [X1], 2 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f14,  5 * SIZE
	(p21) STFD	[Y2] = f15,  5 * SIZE
	mov	pr = PR, -65474
	}
	{ .mib
	(p13) LDFPD	f42, f43 = [X1], 2 * SIZE
	nop	__LINE__
	(p9)	br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p14) LDFPD	f44, f45 = [X1], 2 * SIZE
	nop	__LINE__
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmi
	(p15) LDFD	f46 = [X1]
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f32 = ALPHA, f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f36 = ALPHA, f36
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f33 = ALPHA, f33
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f37 = ALPHA, f37
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f34 = ALPHA, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f38 = ALPHA, f38
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f35 = ALPHA, f35
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f39 = ALPHA, f39
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f32, 1 * SIZE
	nop	__LINE__
	(p13) FMPY	f40 = ALPHA, f40
	}
	{ .mmf
	(p12) STFD	[Y2] = f36, 1 * SIZE
	nop	__LINE__
	(p13) FMPY	f41 = ALPHA, f41
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f33, 1 * SIZE
	nop	__LINE__
	(p13) FMPY	f42 = ALPHA, f42
	}
	{ .mmf
	(p12) STFD	[Y2] = f37, 1 * SIZE
	nop	__LINE__
	(p13) FMPY	f43 = ALPHA, f43
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f34, 1 * SIZE
	nop	__LINE__
	(p14) FMPY	f44 = ALPHA, f44
	}
	{ .mmf
	(p12) STFD	[Y2] = f38, 1 * SIZE
	nop	__LINE__
	(p14) FMPY	f45 = ALPHA, f45
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f35, 5 * SIZE
	(p12) STFD	[Y2] = f39, 5 * SIZE
	(p15) FMPY	f46 = ALPHA, f46
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f40, 1 * SIZE
	;;
	(p13) STFD	[Y1] = f41, 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f42, 1 * SIZE
	;;
	(p13) STFD	[Y1] = f43, 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p14) STFD	[Y1] = f44, 1 * SIZE
	;;
	(p14) STFD	[Y1] = f45, 1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p15) STFD	[Y1] = f46
	nop	__LINE__
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L300:
	{ .mmi
	adds	PRE1 = PREFETCH_SIZE * SIZE + 64, X1
	nop	__LINE__
	mov.i	ar.ec = 6
	}
	{ .mmb
	cmp.gt	p8, p0 = 0, I
	nop	__LINE__
	(p8) br.cond.dpnt .L320
	}
	;;
	.align 32

.L310:
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE1], INCX16
	(p16) LDFD	f32  = [X1], INCX
	(p21) FMPY	f6   = ALPHA, f37
	}
	{ .mmb
	(p22) STFD	[Y1]  = f12
	(p22) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f38  = [X1], INCX
	(p21) FMPY	f7   = ALPHA, f43
	nop	__LINE__
	}
	{ .mmb
	(p22) STFD	[Y1]  = f13
	(p22) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f44  = [X1], INCX
	(p21) FMPY	f10  = ALPHA, f49
	nop	__LINE__
	}
	{ .mmb
	(p22) STFD	[Y1]  = f14
	(p22) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f50  = [X1], INCX
	(p21) FMPY	f11  = ALPHA, f55
	nop	__LINE__
	}
	{ .mmb
	(p22) STFD	[Y1]  = f15
	(p22) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f56  = [X1], INCX
	(p21) FMPY	f12  = ALPHA, f61
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f6
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f62  = [X1], INCX
	(p21) FMPY	f13  = ALPHA, f67
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f7
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f68  = [X1], INCX
	(p21) FMPY	f14  = ALPHA, f73
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f10
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f74  = [X1], INCX
	(p21) FMPY	f15  = ALPHA, f79
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f11
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f80  = [X1], INCX
	(p21) FMPY	f6   = ALPHA, f85
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f12
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f86  = [X1], INCX
	(p21) FMPY	f7   = ALPHA, f91
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f13
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f92  = [X1], INCX
	(p21) FMPY	f10  = ALPHA, f97
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f14
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f98  = [X1], INCX
	(p21) FMPY	f11  = ALPHA, f103
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f15
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f104  = [X1], INCX
	(p21) FMPY	f12  = ALPHA, f109
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f6
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f110  = [X1], INCX
	(p21) FMPY	f13  = ALPHA, f115
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f7
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f116  = [X1], INCX
	(p21) FMPY	f14  = ALPHA, f121
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f10
	(p21) add Y1 = INCX, Y1
	nop	__LINE__
	}
	;;
	{ .mfb
	(p16) LDFD	f122  = [X1], INCX
	(p21) FMPY	f15  = ALPHA, f127
	nop	__LINE__
	}
	{ .mmb
	(p21) STFD	[Y1]  = f11
	(p21) add Y1 = INCX, Y1
	br.ctop.sptk.few .L310
	}
	;;
	STFD	[Y1]  = f12
	add	Y1 = INCX, Y1
	shladd	Y2 = INCX, 2, X1
	;;
	STFD	[Y1]  = f13
	add	Y1 = INCX, Y1
	shladd	X2 = INCX, 2, X1
	;;
	STFD	[Y1]  = f14
	add	Y1 = INCX, Y1
	;;
	STFD	[Y1]  = f15
	add	Y1 = INCX, Y1
	;;
	.align 16

.L320:
	{ .mmi
	(p12) LDFD	f48 = [X1], INCX
	(p12) LDFD	f52 = [X2], INCX
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p12) LDFD	f49 = [X1], INCX
	(p12) LDFD	f53 = [X2], INCX
	mov	pr = PR, -65474
	}
	{ .mmb
	nop.m	0
	nop.m	0
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], INCX
	(p12) LDFD	f54 = [X2], INCX
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	(p12) LDFD	f51 = [X1], INCX5
	(p12) LDFD	f55 = [X2], INCX5
	tbit.z	p0, p14 = N, 1
	}
	;;
	(p13) LDFD	f56 = [X1], INCX
	tbit.z	p0, p15 = N, 0
	;;
	(p13) LDFD	f57 = [X1], INCX
	;;
	{ .mfi
	(p13) LDFD	f58 = [X1], INCX
	(p12) FMPY	f48 = ALPHA, f48
	}
	{ .mfi
	(p12) FMPY	f52 = ALPHA, f52
	}
	;;
	{ .mfi
	(p13) LDFD	f59 = [X1], INCX
	(p12) FMPY	f49 = ALPHA, f49
	}
	{ .mfi
	(p12) FMPY	f53 = ALPHA, f53
	}
	;;
	{ .mfi
	(p14) LDFD	f60 = [X1], INCX
	(p12) FMPY	f50 = ALPHA, f50
	}
	{ .mfi
	(p12) FMPY	f54 = ALPHA, f54
	}
	;;
	{ .mfi
	(p14) LDFD	f61 = [X1], INCX
	(p12) FMPY	f51 = ALPHA, f51
	}
	{ .mfi
	(p12) FMPY	f55 = ALPHA, f55
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f48
	(p12) STFD	[Y2] = f52
	(p13) FMPY	f56 = ALPHA, f56
	}
	{ .mmi
	(p15) LDFD	f62 = [X1]
	(p12) add Y1 = INCX, Y1
	(p12) add Y2 = INCX, Y2
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p13) FMPY	f57 = ALPHA, f57
	}
	{ .mmi
	(p12) add Y1 = INCX, Y1
	(p12) add Y2 = INCX, Y2
	nop	__LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[Y1] = f50
	(p12) STFD	[Y2] = f54
	(p13) FMPY	f58 = ALPHA, f58
	}
	{ .mmi
	(p12) add Y1 = INCX, Y1
	(p12) add Y2 = INCX, Y2
	nop	__LINE__
	}
	;;
	{ .mmf
	(p12) STFD	[Y1]  = f51
	(p12) STFD	[Y2] = f55
	(p13) FMPY	f59 = ALPHA, f59
	}
	{ .mmi
	(p12) add Y1 = INCX5, Y1
	(p12) add Y2 = INCX5, Y2
	nop	__LINE__
	}
	;;
	{ .mfi
	(p13) STFD	[Y1]  = f56
	(p14) FMPY	f60 = ALPHA, f60
	(p13) add Y1 = INCX, Y1
	}
	;;
	{ .mfi
	(p13) STFD	[Y1]  = f57
	(p14) FMPY	f61 = ALPHA, f61
	(p13) add Y1 = INCX, Y1
	}
	;;
	{ .mfi
	(p13) STFD	[Y1]  = f58
	(p15) FMPY	f62 = ALPHA, f62
	(p13) add Y1 = INCX, Y1
	}
	;;
	{ .mmi
	(p13) STFD	[Y1]  = f59
	(p13) add Y1 = INCX, Y1
	}
	;;
	{ .mmi
	(p14) STFD	[Y1]  = f60
	(p14) add Y1 = INCX, Y1
	}
	;;
	{ .mmi
	(p14) STFD	[Y1]  = f61
	(p14) add Y1 = INCX, Y1
	}
	;;
	{ .mib
	(p15) STFD	[Y1]  = f62
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	}
	EPILOGUE

