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

#define N	r32
#define X1	r33
#define INCX	r34
#define Y1	r35
#define INCY	r36

#define PREX	r2
#define PREY	r3

#define I	r14
#define J	r15

#define X2	r16
#define Y2	r17
#define INCX2	r18
#define INCY2	r19
#define INCX8	r20
#define INCY8	r21
#define PR	r30
#define ARLC	r31

#define PREFETCH_SIZE (8 * 16)

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	shr	I = N, 3
	(p6) br.ret.sptk.many b0
	}
	;;
	shl	INCX = INCX, ZBASE_SHIFT
	shl	INCY = INCY, ZBASE_SHIFT
	;;
	.body
	{ .mmi
	sub	r8 = X1, Y1
	mov	r9 = 0xf0
	mov	PR = pr
	}
	{ .mmi
	shladd	INCX2 = INCX, 1, r0
	shladd	INCY2 = INCY, 1, r0
	and	J = 15, N
	}
	;;
	{ .mmi
	shladd	INCX8 = INCX, 2, r0
	shladd	INCY8 = INCY, 2, r0
	mov	pr.rot = 0
	}
	{ .mmi
	and	r8 = r9, r8
	cmp.eq	p9,  p0 = r0, J
	adds	I = -1, I
	}
	;;
	{ .mmi
	adds	X2 = 1 * SIZE, X1
	adds	Y2 = 1 * SIZE, Y1
	mov	ar.ec = 4
	}
	{ .mmb
	cmp.gt	p6, p0 = 127, r8
	cmp.eq	p16, p0 = r0, r0
	(p6) br.cond.dpnt     .L20
	}
	;;
	{ .mmi
	adds	PREX  = (PREFETCH_SIZE + 0) * SIZE, X1
	adds	PREY  = (PREFETCH_SIZE + 2) * SIZE, Y1
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p8 ,p0  =   -1, I
	tbit.z	p0, p12 = N, 2
	(p8) br.cond.dpnt  .L15
	}
	;;
	.align 16

.L12:
	{ .mmi
	(p19) STFD	[Y1] = f35
	(p19) STFD	[Y2] = f39
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p17) LDFD	f81  = [X1], INCX
	(p17) LDFD	f85  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f43
	(p19) STFD	[Y2] = f47
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p17) LDFD	f89  = [X1], INCX
	(p17) LDFD	f93  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f51
	(p19) STFD	[Y2] = f55
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f32  = [X1], INCX
	(p16) LDFD	f36  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f59
	(p19) STFD	[Y2] = f63
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	lfetch.fault.nt1	[PREX], INCX8
	lfetch.fault.excl.nt1	[PREY], INCY8
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p16) LDFD	f40  = [X1], INCX
	(p16) LDFD	f44  = [X2], INCX
	nop	__LINE__
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f67
	(p19) STFD	[Y2] = f71
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f48  = [X1], INCX
	(p16) LDFD	f52  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f75
	(p19) STFD	[Y2] = f79
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f56  = [X1], INCX
	(p16) LDFD	f60  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f83
	(p19) STFD	[Y2] = f87
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	lfetch.fault.nt1	[PREX], INCX8
	lfetch.fault.excl.nt1	[PREY], INCY8
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f91
	(p19) STFD	[Y2] = f95
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
 	(p16) LDFD	f64  = [X1], INCX
	(p16) LDFD	f68  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmb
 	(p16) LDFD	f72  = [X1], INCX
	(p16) LDFD	f76  = [X2], INCX
	br.ctop.sptk.few .L12
	}
	;;
	.align 32

.L15:
	{ .mmi
	(p12) LDFD	f48 = [X1], INCX
	(p12) LDFD	f49 = [X2], INCX
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], INCX
	(p12) LDFD	f51 = [X2], INCX
	mov	pr = PR, -65474
	}
	;;
	{ .mmb
	(p12) LDFD	f52 = [X1], INCX
	(p12) LDFD	f53 = [X2], INCX
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f54 = [X1], INCX
	(p12) LDFD	f55 = [X2], INCX
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], INCX
	(p13) LDFD	f57 = [X2], INCX
	tbit.z	p0, p14 = N, 0
	}
	;;
	{ .mmi
	(p13) LDFD	f58 = [X1], INCX
	(p13) LDFD	f59 = [X2], INCX
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48
	(p12) STFD	[Y2] = f49
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	(p14) LDFD	f60 = [X1], INCX
	(p14) LDFD	f61 = [X2], INCX
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50
	(p12) STFD	[Y2] = f51
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	nop   __LINE__
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f52
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f54
	(p12) STFD	[Y2] = f55
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56
	(p13) STFD	[Y2] = f57
	(p13) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58
	(p13) STFD	[Y2] = f59
	(p13) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add Y2 = INCY, Y2
	}
	;;
	{ .mmb
	(p14) STFD	[Y1] = f60
	(p14) STFD	[Y2] = f61
	br.ret.sptk.many b0
	}
	;;
	.align 16

.L20:
	{ .mmi
	adds	PREX  = (PREFETCH_SIZE +  0) * SIZE, X1
	adds	PREY  = (PREFETCH_SIZE + 10) * SIZE, Y1
	mov	ar.lc = I
	}
	{ .mib
	cmp.eq	p8 ,p0  =   -1, I
	tbit.z	p0, p12 = N, 2
	(p8) br.cond.dpnt  .L25
	}
	;;
	.align 16

.L22:
	{ .mmi
	(p19) STFD	[Y1] = f67
	(p19) STFD	[Y2] = f71
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p17) LDFD	f81  = [X1], INCX
	(p17) LDFD	f85  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f75
	(p19) STFD	[Y2] = f79
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p17) LDFD	f89  = [X1], INCX
	(p17) LDFD	f93  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f83
	(p19) STFD	[Y2] = f87
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f32  = [X1], INCX
	(p16) LDFD	f36  = [X2], INCX
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f91
	(p19) STFD	[Y2] = f95
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	lfetch.fault.nt1	[PREX], INCX8
	lfetch.fault.excl.nt1	[PREY], INCY8
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p16) LDFD	f40  = [X1], INCX
	(p16) LDFD	f44  = [X2], INCX
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f34
	(p18) STFD	[Y2] = f38
	(p18) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f48  = [X1], INCX
	(p16) LDFD	f52  = [X2], INCX
	(p18) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f42
	(p18) STFD	[Y2] = f46
	(p18) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f56  = [X1], INCX
	(p16) LDFD	f60  = [X2], INCX
	(p18) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f50
	(p18) STFD	[Y2] = f54
	(p18) add   Y1 = INCY, Y1
	}
	{ .mmi
	lfetch.fault.nt1	[PREX], INCX8
	lfetch.fault.excl.nt1	[PREY], INCY8
	(p18) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p18) STFD	[Y1] = f58
	(p18) STFD	[Y2] = f62
	(p18) add   Y1 = INCY, Y1
	}
	{ .mmi
 	(p16) LDFD	f64  = [X1], INCX
	(p16) LDFD	f68  = [X2], INCX
	(p18) add   Y2 = INCY, Y2
	}
	;;
	{ .mmb
 	(p16) LDFD	f72  = [X1], INCX
	(p16) LDFD	f76  = [X2], INCX
	br.ctop.sptk.few .L22
	}
	;;
	.align 32

.L25:
	{ .mmi
	(p12) LDFD	f48 = [X1], INCX
	(p12) LDFD	f49 = [X2], INCX
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], INCX
	(p12) LDFD	f51 = [X2], INCX
	mov	pr = PR, -65474
	}
	;;
	{ .mmb
	(p12) LDFD	f52 = [X1], INCX
	(p12) LDFD	f53 = [X2], INCX
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f54 = [X1], INCX
	(p12) LDFD	f55 = [X2], INCX
	tbit.z	p0, p13 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], INCX
	(p13) LDFD	f57 = [X2], INCX
	tbit.z	p0, p14 = N, 0
	}
	;;
	{ .mmi
	(p13) LDFD	f58 = [X1], INCX
	(p13) LDFD	f59 = [X2], INCX
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48
	(p12) STFD	[Y2] = f49
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	(p14) LDFD	f60 = [X1], INCX
	(p14) LDFD	f61 = [X2], INCX
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50
	(p12) STFD	[Y2] = f51
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	nop   __LINE__
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f52
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f54
	(p12) STFD	[Y2] = f55
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p12) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56
	(p13) STFD	[Y2] = f57
	(p13) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58
	(p13) STFD	[Y2] = f59
	(p13) add Y1 = INCY, Y1
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p13) add Y2 = INCY, Y2
	}
	;;
	{ .mmb
	(p14) STFD	[Y1] = f60
	(p14) STFD	[Y2] = f61
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

