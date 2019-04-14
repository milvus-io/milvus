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

#define PREA	r2
#define PREB	r3

#define I	r14
#define J	r15

#define X2	r16
#define Y2	r17
#define INCX3	r18
#define INCY3	r19
#define INCX5	r20
#define INCY5	r21
#define INCX16	r22
#define INCY16	r23
#define XX	r24
#define YY	r25
#define XA	r26
#define YA	r27
#define PR	r30
#define ARLC	r31

#ifdef DOUBLE
#define PREFETCH_SIZE (4 * 32)
#else
#define PREFETCH_SIZE (4 * 64)
#endif

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	.save ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mib
	cmp.lt	p0, p6 = r0, N
	tbit.z	p0, p7 = X1, BASE_SHIFT
	(p6) br.ret.sptk.many b0
	}
	;;
	.body
	{ .mmi
	sub	XA = Y1, X1
	(p7) LDFD f32 = [X1], INCX
	mov	PR = pr
	}
	{ .mmi
	mov	YY = Y1
	(p7) adds N = -1, N
	(p7) add Y1 = Y1, INCY
	}
	;;
	{ .mmi
	shladd	INCX5  = INCX, 2, INCX
	shladd	INCY5  = INCY, 2, INCY
	mov	pr.rot = 0
	}
	{ .mmi
	mov	XX = X1
	nop.m	0
	shr.u  XA = XA, BASE_SHIFT
	}
	;;
	{ .mmi
	and	J = 15, N
	cmp.eq	p16, p0 = r0, r0
	shr	I = N, 4
	}
	{ .mmb
	cmp.ne	p6, p0 = SIZE, INCX
#ifdef DOUBLE
	adds	XA = 2, XA
#else
	nop.m	0
#endif
	(p6) br.cond.dpnt .L100
	}
	;;
/* INCX == 1 */
	{ .mmi
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
	tbit.z	p0, p12 = N, 3
	}
	{ .mmi
#ifdef DOUBLE
	and	XA = 31, XA
#else
	and	XA = 63, XA
#endif
	adds	I = -1, I
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .mmi
	shladd	X2 = INCX, 2, X1
	shladd	Y2 = INCY, 2, Y1
	mov	ar.lc = I
	}
	{ .mib
#ifdef DOUBLE
	cmp.gt	p8, p0 = 15, XA
#else
	cmp.gt	p8, p0 = 30, XA
#endif
	cmp.eq	p9, p0  =   r0, J
	(p8)br.cond.dpnt  .L30
	}
	;;
	{ .mmi
	(p7) STFD [YY] = f32
	cmp.gt	p8 ,p0  =   r0, I
	mov	ar.ec = 5
	}
	{ .mmb
	adds	PREA  = PREFETCH_SIZE * SIZE + 32, X1
#ifdef DOUBLE
	adds	PREB  = PREFETCH_SIZE * SIZE + 32, Y1
#else
	adds	PREB  = PREFETCH_SIZE * SIZE - 40, Y1
#endif
	(p8) br.cond.dpnt  .L25
	}
	;;
	.align 32

.L22:
	{ .mmi
	(p20) STFD	[Y1] = f36
	(p20) STFD	[Y2] = f56
	(p20) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX16
	(p16) LDFPD	f32, f37  = [X1], 2 * SIZE
	(p20) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f41
	(p20) STFD	[Y2] = f61
	(p20) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY16
	(p16) LDFPD	f42, f47  = [X1], 2 * SIZE
	(p20) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f46
	(p20) STFD	[Y2] = f66
	(p20) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFPD	f52, f57  = [X1], 2 * SIZE
	nop.m 0
	(p20) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f51
	(p20) STFD	[Y2] = f71
	(p20) add   Y1 = INCY5, Y1
	}
	{ .mmi
	(p16) LDFPD	f62, f67  = [X1], 2 * SIZE
	nop.m 0
	(p20) add   Y2 = INCY5, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f76
	(p20) STFD	[Y2] = f96
	(p16) adds	XX = 8 * SIZE, X1
	}
	{ .mmi
	(p16) LDFPD	f72, f77  = [X1], 2 * SIZE
	(p20) add   Y1 = INCY, Y1
	(p20) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f81
	(p20) STFD	[Y2] = f101
	(p20) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFPD	f82, f87  = [X1], 2 * SIZE
	nop.m 0
	(p20) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f86
	(p20) STFD	[Y2] = f106
	(p16) shladd	X2 = INCX, 2, XX
	}
	{ .mmi
	(p16) LDFPD	f92, f97  = [X1], 2 * SIZE
	(p20) add   Y1 = INCY, Y1
	(p20) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p20) STFD	[Y1] = f91
	(p20) STFD	[Y2] = f111
	(p20) add   Y1 = INCY5, Y1
	}
	{ .mmb
	(p16) LDFPD	f102, f107 = [X1], 2 * SIZE
	(p20) add   Y2 = INCY5, Y2
	br.ctop.sptk.few .L22
	}
	;;
	.align 32
.L25:
	{ .mmi
	(p12) LDFPD	f48, f49 = [X1], 2 * SIZE
	(p12) LDFPD	f52, f53 = [X2], 2 * SIZE
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) adds XX = 8 * SIZE, XX
	nop.m 0
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFPD	f50, f51 = [X1]
	(p12) LDFPD	f54, f55 = [X2]
	mov	pr = PR, -65474
	}
	{ .mmb
	(p12) adds	X1 = 6 * SIZE, X1
	(p13) adds	XX = 4 * SIZE, XX
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p13) LDFPD	f56, f57 = [X1], 2 * SIZE
	(p14) LDFPD	f60, f61 = [XX], 2 * SIZE
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmi
	(p13) LDFPD	f58, f59 = [X1], 2 * SIZE
	(p15) LDFD	f62 = [XX]
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48
	(p12) STFD	[Y2] = f52
	mov	YY = Y1
	}
	{ .mmi
	(p12) add Y1 = INCY, Y1
	(p12) add Y2 = INCY, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	(p12) add Y2 = INCY, Y2
	(p12) shladd YY = INCY, 3, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50
	(p12) STFD	[Y2] = f54
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	(p12) add Y2 = INCY, Y2
	(p13) shladd YY = INCY, 2, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
 	(p12) STFD	[Y2] = f55
	(p12) add Y1 = INCY5, Y1
	}
	{ .mmi
	(p12) add Y2 = INCY5, Y2
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56
	(p14) STFD	[YY] = f60
	(p13) add Y1 = INCY, Y1
	}
	{ .mmi
	(p14) add YY = INCY, YY
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f61
	(p13) add Y1 = INCY, Y1
	}
	{ .mmi
	(p14) add YY = INCY, YY
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58
	(p15) STFD	[YY] = f62
	(p13) add Y1 = INCY, Y1
	}
	;;
	{ .mmb
	(p13) STFD	[Y1] = f59
	nop.m 0
	br.ret.sptk.many b0
	}
	.align 32
	;;
.L30:
	{ .mmi
	(p7) STFD [YY] = f32
	cmp.gt	p8 ,p0  =   r0, I
	mov	ar.ec = 4
	}
	{ .mmb
	adds	PREA  = PREFETCH_SIZE * SIZE + 24, X1
#ifdef DOUBLE
	adds	PREB  = PREFETCH_SIZE * SIZE + 64, Y1
#else
	adds	PREB  = PREFETCH_SIZE * SIZE + 72, Y1
#endif
	(p8) br.cond.dpnt  .L35
	}
	;;
	.align 32
.L32:
	{ .mmi
	(p19) STFD	[Y1] = f35
	(p19) STFD	[Y2] = f55
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX16
	(p16) LDFPD	f32, f37  = [X1], 2 * SIZE
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f40
	(p19) STFD	[Y2] = f60
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) lfetch.excl.nt1 [PREB], INCY16
	(p16) LDFPD	f42, f47  = [X1], 2 * SIZE
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f45
	(p19) STFD	[Y2] = f65
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFPD	f52, f57  = [X1], 2 * SIZE
	nop.m 0
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f50
	(p19) STFD	[Y2] = f70
	(p19) add   Y1 = INCY5, Y1
	}
	{ .mmi
	(p16) LDFPD	f62, f67  = [X1], 2 * SIZE
	nop.m 0
	(p19) add   Y2 = INCY5, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f75
	(p19) STFD	[Y2] = f95
	(p16) adds	XX = 8 * SIZE, X1
	}
	{ .mmi
	(p16) LDFPD	f72, f77  = [X1], 2 * SIZE
	(p19) add   Y1 = INCY, Y1
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f80
	(p19) STFD	[Y2] = f100
	(p19) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFPD	f82, f87  = [X1], 2 * SIZE
	nop.m 0
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f85
	(p19) STFD	[Y2] = f105
	(p16) shladd	X2 = INCX, 2, XX
	}
	{ .mmi
	(p16) LDFPD	f92, f97  = [X1], 2 * SIZE
	(p19) add   Y1 = INCY, Y1
	(p19) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p19) STFD	[Y1] = f90
	(p19) STFD	[Y2] = f110
	(p19) add   Y1 = INCY5, Y1
	}
	{ .mmb
	(p16) LDFPD	f102, f107 = [X1], 2 * SIZE
	(p19) add   Y2 = INCY5, Y2
	br.ctop.sptk.few .L32
	}
	;;
	.align 32
.L35:
	{ .mmi
	(p12) LDFPD	f48, f49 = [X1], 2 * SIZE
	(p12) LDFPD	f52, f53 = [X2], 2 * SIZE
	mov	ar.lc = ARLC
	}
	{ .mmi
	(p12) adds XX = 8 * SIZE, XX
	nop.m 0
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p12) LDFPD	f50, f51 = [X1]
	(p12) LDFPD	f54, f55 = [X2]
	mov	pr = PR, -65474
	}
	{ .mmi
	(p12) adds	X1 = 6 * SIZE, X1
	(p12) adds	X2 = 6 * SIZE, X2
	(p13) adds XX = 4 * SIZE, XX
	}
	;;
	{ .mmi
	(p13) LDFPD	f56, f57 = [X1], 2 * SIZE
	(p14) LDFPD	f60, f61 = [XX], 2 * SIZE
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmb
	(p13) LDFPD	f58, f59 = [X1], 2 * SIZE
	(p15) LDFD	f62 = [XX]
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48
	(p12) STFD	[Y2] = f52
	mov	YY = Y1
	}
	{ .mmi
	(p12) add Y1 = INCY, Y1
	(p12) add Y2 = INCY, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	(p12) add Y2 = INCY, Y2
	(p12) shladd YY = INCY, 3, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50
	(p12) STFD	[Y2] = f54
	(p12) add Y1 = INCY, Y1
	}
	{ .mmi
	(p12) add Y2 = INCY, Y2
	(p13) shladd YY = INCY, 2, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
	(p12) STFD	[Y2] = f55
	nop.i 0
	}
	{ .mmi
	(p12) add Y1 = INCY5, Y1
	(p12) add Y2 = INCY5, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56
	(p14) STFD	[YY] = f60
	nop.i 0
	}
	{ .mmi
	(p13) add Y1 = INCY, Y1
	(p14) add YY = INCY, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f61
	nop.i 0
	}
	{ .mmi
	(p13) add Y1 = INCY, Y1
	(p14) add YY = INCY, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58
	(p15) STFD	[YY] = f62
	(p13) add Y1 = INCY, Y1
	}
	;;
	{ .mib
	(p13) STFD	[Y1] = f59
	nop.i 0
	br.ret.sptk.many b0
	}
	.align 32
	;;

 /* INCX != 1 */
.L100:
	{ .mmi
	shladd	INCX16 = INCX, 4, r0
	shladd	INCY16 = INCY, 4, r0
	tbit.z	p0, p12 = N, 3
	}
	{ .mmi
	nop.m 0
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	adds	PREA  = PREFETCH_SIZE * SIZE, X1
	adds	PREB  = PREFETCH_SIZE * SIZE, Y1
	mov	ar.ec = 6
	}
	{ .mmi
	cmp.eq	p8 ,p0  =   r0, I
	cmp.eq	p9, p0  =   r0, J
	adds	I = -1, I
	}
	;;
	{ .mmi
	(p7) STFD [YY] = f32
	shladd	X2 = INCX, 2,  X1
	mov	ar.lc = I
	}
	{ .mib
	shladd	Y2 = INCY, 2,  Y1
	cmp.eq	p16, p0 = r0, r0
	(p8) br.cond.dpnt  .L120
	}
	;;
	.align 32

.L110:
	{ .mmi
	(p21) STFD	[Y1] = f37
	(p21) STFD	[Y2] = f61
	(p21) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], INCX16
	(p16) lfetch.excl.nt1 [PREB], INCY16
	(p21) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f43
	(p21) STFD	[Y2] = f67
	(p21) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f56  = [X2], INCX
	(p16) LDFD	f32  = [X1], INCX
	(p21) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f49
	(p21) STFD	[Y2] = f73
	(p21) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f38  = [X1], INCX
	(p16) LDFD	f62  = [X2], INCX
	(p21) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f55
	(p21) STFD	[Y2] = f79
	(p21) add   Y1 = INCY5, Y1
	}
	{ .mmi
	(p16) LDFD	f44  = [X1], INCX
	(p16) LDFD	f68  = [X2], INCX
	(p21) add   Y2 = INCY5, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f85
	(p21) STFD	[Y2] = f109
	(p21) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f50  = [X1], INCX5
	(p16) LDFD	f74  = [X2], INCX5
	(p21) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f91
	(p21) STFD	[Y2] = f115
	(p21) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f80  = [X1], INCX
	(p16) LDFD	f104 = [X2], INCX
	(p21) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f97
	(p21) STFD	[Y2] = f121
	(p21) add   Y1 = INCY, Y1
	}
	{ .mmi
	(p16) LDFD	f86   = [X1], INCX
	(p16) LDFD	f110  = [X2], INCX
	(p21) add   Y2 = INCY, Y2
	}
	;;
	{ .mmi
	(p21) STFD	[Y1] = f103
	(p21) STFD	[Y2] = f127
	(p21) add   Y1 = INCY5, Y1
	}
	{ .mmi
	(p16) LDFD	f92  = [X1], INCX
	(p16) LDFD	f116 = [X2], INCX
	(p21) add   Y2 = INCY5, Y2
	}
	;;
	{ .mmi
	nop.m	0
	(p16) add XX = INCX5, X1
	nop.i	0
	}
	{ .mmb
	(p16) LDFD	f98  = [X1], INCX5
	(p16) LDFD	f122 = [X2], INCX5
	br.ctop.sptk.few .L110
	}
	;;
	.align 32

.L120:
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
	;;
	{ .mmi
	(p12) LDFD	f50 = [X1], INCX
	(p12) LDFD	f54 = [X2], INCX
	tbit.z	p0, p13 = N, 2
	}
	{ .mmb
	nop.m 0
	nop.m 0
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmi
	(p12) LDFD	f51 = [X1], INCX5
	(p12) LDFD	f55 = [X2], INCX5
	(p12) shladd XX = INCX, 3, XX
	}
	;;
	{ .mmi
	(p13) LDFD	f56 = [X1], INCX
	(p13) shladd XX = INCX, 2, XX
	tbit.z	p0, p14 = N, 1
	}
	;;
	{ .mmi
	(p13) LDFD	f57 = [X1], INCX
	(p14) LDFD	f60 = [XX], INCX
	}
	;;
	{ .mmi
	(p13) LDFD	f58 = [X1], INCX
	(p14) LDFD	f61 = [XX], INCX
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmi
	(p13) LDFD	f59 = [X1], INCX
	(p15) LDFD	f62 = [XX]
	mov	YY = Y1
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f48
	(p12) STFD	[Y2] = f52
	nop.i 0
	}
	{ .mmi
	(p12) add Y1 = INCY, Y1
	(p12) add Y2 = INCY, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f49
	(p12) STFD	[Y2] = f53
	nop.i 0
	}
	{ .mmi
	(p12) add Y1 = INCY, Y1
	(p12) add Y2 = INCY, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f50
	(p12) STFD	[Y2] = f54
	nop.i 0
	}
	{ .mmi
	(p12) add Y1 = INCY, Y1
	(p12) add Y2 = INCY, Y2
	nop.i 0
	}
	;;
	{ .mmi
	(p12) STFD	[Y1] = f51
	(p12) STFD	[Y2] = f55
	(p12) add Y1 = INCY5, Y1
	}
	{ .mmi
	(p12) add Y2 = INCY5, Y2
	(p12) shladd YY = INCY, 3, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f56
	(p13) add Y1 = INCY, Y1
	(p13) shladd YY =INCY, 2, YY
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f57
	(p14) STFD	[YY] = f60
	nop.i 0
	}
	{ .mmi
	(p13) add Y1 = INCY, Y1
	(p14) add YY = INCY, YY
	nop.i 0
	}
	;;
	{ .mmi
	(p13) STFD	[Y1] = f58
	(p14) STFD	[YY] = f61
	nop.i 0
	}
	{ .mmi
	(p13) add Y1 = INCY, Y1
	(p14) add YY = INCY, YY
	nop.i 0
	}
	;;
	{ .mmb
	(p13) STFD	[Y1] = f59
	(p15) STFD	[YY] = f62
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

