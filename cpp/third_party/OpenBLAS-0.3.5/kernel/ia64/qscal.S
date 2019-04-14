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

#define PREFETCH_SIZE (16 * 16)

#define ALPHA		f8

#define N		r32
#define X1		r38
#define INCX		r39

#define	X2		r14
#define Y1		r15
#define Y2		r16
#define PRE1		r17
#define I		r18
#define NAND15		r19
#define INCX5		r20
#define INCX8		r21
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
	.body
	{ .mib
	cmp.ge	p7, p0 = 0, N
	(p7) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	mov	XX = X1
	mov	PR = pr
	}
	{ .mmi
	shladd	INCX5  = INCX, 2, INCX
	shladd	INCX8 = INCX, 3, r0
	}
	;;
	{ .mmi
	shladd	X2 = INCX, 2, X1
	nop.m 0
	mov	ar.ec = 5
	}
	{ .mmi
	and	NAND15 = 15, N
	nop.m 0
	shr	I = N, 4
	}
	;;
	{ .mmi
	adds	I = -1, I
	nop.m 0
	tbit.z	p0, p12 = N, 3
	}
	{ .mmb
	cmp.ge	p9, p0 = 0, NAND15
	nop.m 0
	(p6) br.cond.dptk .L100		// if (alpha != 0) goto L3
	}
	;;
	{ .mmi
	adds	PRE1 = (PREFETCH_SIZE + 4) * SIZE, X1
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
	nop.i	0
	}
	{.mmi
	lfetch.excl.nt1	[PRE1], INCX8
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop.i	0
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop.i	0
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
	}
	{.mmi
	add	X1 = INCX5, X1
	add	X2 = INCX5, X2
	nop.i	0
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
	}
	{.mmi
	lfetch.excl.nt1	[PRE1], INCX8
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop.i	0
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
	}
	{.mmi
	add	X1 = INCX, X1
	add	X2 = INCX, X2
	nop.i	0
	}
	;;
	{.mmi
	STFD	[X1] = f0
	STFD	[X2] = f0
	nop.i	0
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
	nop.m 0
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L100:
	{ .mmi
	mov	Y1 = X1
	shladd	Y2 = INCX, 2, X1
	mov	pr.rot= 0
	}
	;;
	{ .mmi
	mov	ar.lc = I
	}
	cmp.eq	p16, p0 = r0, r0
	;;

	{ .mmi
	adds	PRE1 = (PREFETCH_SIZE + 4) * SIZE, X1
	nop.m 0
	mov.i	ar.ec = 6
	}
	{ .mmb
	cmp.gt	p8, p0 = 0, I
	nop.m 0
	(p8) br.cond.dpnt .L320
	}
	;;
	.align 32

.L310:
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE1], INCX8
	(p22) STFD	[Y1]  = f12
	(p21) FMPY	f6   = ALPHA, f37
	}
	{ .mmi
	(p16) LDFD	f32  = [X1], INCX
	nop	__LINE__
	(p22) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p22) STFD	[Y1]  = f13
	(p16) LDFD	f38  = [X1], INCX
	(p21) FMPY	f7   = ALPHA, f43
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p22) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p22) STFD	[Y1]  = f14
	(p16) LDFD	f44  = [X1], INCX
	(p21) FMPY	f10  = ALPHA, f49
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p22) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p22) STFD	[Y1]  = f15
	(p16) LDFD	f50  = [X1], INCX
	(p21) FMPY	f11  = ALPHA, f55
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p22) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f6
	(p16) LDFD	f56  = [X1], INCX
	(p21) FMPY	f12  = ALPHA, f61
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE1], INCX8
	(p21) STFD	[Y1]  = f7
	(p21) FMPY	f13  = ALPHA, f67
	}
	{ .mmi
	(p16) LDFD	f62  = [X1], INCX
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f10
	(p16) LDFD	f68  = [X1], INCX
	(p21) FMPY	f14  = ALPHA, f73
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f11
	(p16) LDFD	f74  = [X1], INCX
	(p21) FMPY	f15  = ALPHA, f79
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f12
	(p16) LDFD	f80  = [X1], INCX
	(p21) FMPY	f6   = ALPHA, f85
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f13
	(p16) LDFD	f86  = [X1], INCX
	(p21) FMPY	f7   = ALPHA, f91
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f14
	(p16) LDFD	f92  = [X1], INCX
	(p21) FMPY	f10  = ALPHA, f97
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f15
	(p16) LDFD	f98  = [X1], INCX
	(p21) FMPY	f11  = ALPHA, f103
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f6
	(p16) LDFD	f104  = [X1], INCX
	(p21) FMPY	f12  = ALPHA, f109
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f7
	(p16) LDFD	f110  = [X1], INCX
	(p21) FMPY	f13  = ALPHA, f115
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f10
	(p16) LDFD	f116  = [X1], INCX
	(p21) FMPY	f14  = ALPHA, f121
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	}
	;;
	{ .mmf
	(p21) STFD	[Y1]  = f11
	(p16) LDFD	f122  = [X1], INCX
	(p21) FMPY	f15  = ALPHA, f127
	}
	{ .mmb
	nop	__LINE__
	(p21) add Y1 = INCX, Y1
	br.ctop.sptk.few .L310
	}
	;;
	{ .mmi
	STFD	[Y1]  = f12
	add	Y1 = INCX, Y1
	shladd	Y2 = INCX, 2, X1
	}
	;;
	{ .mmi
	STFD	[Y1]  = f13
	add	Y1 = INCX, Y1
	shladd	X2 = INCX, 2, X1
	}
	;;
	{ .mmi
	STFD	[Y1]  = f14
	nop	__LINE__
	add	Y1 = INCX, Y1
	}
	;;
	{ .mmi
	STFD	[Y1]  = f15
	nop	__LINE__
	add	Y1 = INCX, Y1
	}
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
	nop	__LINE__
	nop	__LINE__
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
	{ .mmf
	(p13) LDFD	f58 = [X1], INCX
	nop	__LINE__
	(p12) FMPY	f48 = ALPHA, f48
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f52 = ALPHA, f52
	}
	;;
	{ .mmf
	(p13) LDFD	f59 = [X1], INCX
	nop	__LINE__
	(p12) FMPY	f49 = ALPHA, f49
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f53 = ALPHA, f53
	}
	;;
	{ .mmf
	(p14) LDFD	f60 = [X1], INCX
	nop	__LINE__
	(p12) FMPY	f50 = ALPHA, f50
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p12) FMPY	f54 = ALPHA, f54
	}
	;;
	{ .mmf
	(p14) LDFD	f61 = [X1], INCX
	nop	__LINE__
	(p12) FMPY	f51 = ALPHA, f51
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
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
	nop	__LINE__
	(p13) add Y1 = INCX, Y1
	}
	;;
	{ .mmi
	(p14) STFD	[Y1]  = f60
	nop	__LINE__
	(p14) add Y1 = INCX, Y1
	}
	;;
	{ .mmi
	(p14) STFD	[Y1]  = f61
	nop	__LINE__
	(p14) add Y1 = INCX, Y1
	}
	;;
	{ .mib
	(p15) STFD	[Y1]  = f62
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	}
	EPILOGUE

