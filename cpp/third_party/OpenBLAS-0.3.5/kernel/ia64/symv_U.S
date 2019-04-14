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

#define SP	r12

#define M	r32
#define A	r34
#define LDA	r35
#define X	r36
#define INCX	r37
#define Y	r38
#define INCY	r39
#define BUFFER	r33

#define I	r14
#define IS	r15
#define A1	r16
#define A2	r17
#define A3	r18
#define A4	r19

#define NEW_X	r20
#define NEW_Y	r21
#define XX	r22
#define YY	r23
#define TEMP	r24
#define YYS	r25

#define PREA1	loc0
#define PREA2	loc1
#define PREA3	loc2
#define PREA4	loc3

#define A11	loc4
#define A21	loc5
#define A31	loc6
#define A41	loc7

#define PREX	r8
#define PREY	r9

#define ARLC	r29
#define PR	r30
#define ARPFS	r31

#ifdef DOUBLE
#define RPREFETCH	(16 * 3 +  4)
#else
#define RPREFETCH	(16 * 3 + 16)
#endif
#define PREFETCH	lfetch.nt1
#define PREFETCHW	lfetch.excl.nt1

#define alpha	f8
#define atemp1	f6
#define atemp2	f7
#define atemp3	f10
#define atemp4	f11

#define xsum1	f12
#define xsum2	f13
#define xsum3	f14
#define xsum4	f15


	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 16, 8, 0
	mov	ARLC  = ar.lc
	}
	;;
	mov	PR = pr
	adds	r14 = 16, SP
	;;
	adds	r8 = -8 * 16, SP
	adds	r9 = -7 * 16, SP
	adds	SP = -8 * 16, SP
	;;
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	;;
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	;;
	stf.spill  [r8] = f20, 32
	stf.spill  [r9] = f21, 32
	;;
	stf.spill  [r8] = f22
	stf.spill  [r9] = f23
	.body
	;;
	ld8	BUFFER = [r14]
	;;
	shladd	LDA  = LDA,  BASE_SHIFT, r0
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	;;
	cmp.ge	p7, p0 = 0, M
	;;
	(p7) br.cond.dpnt .L999
	;;
	mov	NEW_X = X
	cmp.eq	p10, p0 = SIZE, INCX
	(p10) br.cond.dptk .L10
	;;
.L10:
	mov	NEW_Y = Y
	cmp.eq	p10, p0 = SIZE, INCY
	(p10) br.cond.dptk .L20
	;;

.L20:
	mov	IS = 0
	cmp.gt	p10, p0 = 4, M
	(p10) br.cond.dpnt .L30
	;;
.L21:
	mov	A1 = A
	add	A2 = LDA, A
	;;
	shladd	A3 = LDA, 1, A
	shladd	A4 = LDA, 1, A2
	shladd	A  = LDA, 2, A
	;;
	;;
	adds	PREX   = RPREFETCH * SIZE, NEW_X
	adds	PREY   = RPREFETCH * SIZE, NEW_Y
	adds	PREA1  = RPREFETCH * SIZE, A1
	adds	PREA2  = RPREFETCH * SIZE, A2
	adds	PREA3  = RPREFETCH * SIZE, A3
	adds	PREA4  = RPREFETCH * SIZE, A4
	;;
	shladd	TEMP   = IS, BASE_SHIFT, NEW_X
	;;
	LDFD	atemp1 = [TEMP], 1 * SIZE
	;;
	LDFD	atemp2 = [TEMP], 1 * SIZE
	;;
	LDFD	atemp3 = [TEMP], 1 * SIZE
	;;
	LDFD	atemp4 = [TEMP], 1 * SIZE
	;;
	FMPY	atemp1 = alpha, atemp1
	FMPY	atemp2 = alpha, atemp2
	FMPY	atemp3 = alpha, atemp3
	FMPY	atemp4 = alpha, atemp4
	;;
	mov	xsum1 = f0
	mov	xsum2 = f0
	mov	xsum3 = f0
	mov	xsum4 = f0
	;;
	mov	XX  = NEW_X
	mov	YY  = NEW_Y
	mov	YYS = NEW_Y
	;;
	shr	I = IS, 2
	mov	pr.rot = 0
	;;
	mov	ar.ec = 3
	cmp.eq	p16, p0 = r0, r0
	;;
	cmp.eq	p6, p0 = 0, I
	adds	I = -1, I
	;;
	mov	ar.lc = I
	(p6) br.cond.dpnt .L28
	;;
	.align 16

.L22:
	{ .mmf
	(p16) LDFPD	f32, f35  = [A1], 2 * SIZE
	(p19) STFD	[YYS] = f95, 1 * SIZE
	(p18) FMA	xsum1 = f82, f34,  xsum1
	}
	{ .mmf
	(p18) FMA	f94   = atemp1, f34, f94
	}
	;;
	{ .mmf
	(p17) LDFD	f90  = [XX], 1 * SIZE
	(p18) FMA	xsum2 = f82, f46,  xsum2
	}
	{ .mmf
	(p18) FMA	f98   = atemp1, f37, f98
	}
	;;
	{ .mmf
	(p16) LDFPD	f44, f47  = [A2], 2 * SIZE
	(p19) STFD	[YYS] = f99, 1 * SIZE
	(p18) FMA	xsum3 = f82, f58,  xsum3
	}
	{ .mmf
	(p18) FMA	f102  = atemp1, f40, f102
	}
	;;
	{ .mmf
	(p16) PREFETCHW	[PREY], 4 * SIZE
	(p16) LDFD	f92  = [YY], 1 * SIZE
	(p18) FMA	xsum4 = f82, f70,  xsum4
	}
	{ .mmf
	(p18) FMA	f106  = atemp1, f43, f106
	}
	;;
	{ .mmf
	(p16) LDFPD	f56, f59  = [A3], 2 * SIZE
	(p19) STFD	[YYS] = f103, 1 * SIZE
	(p18) FMA	xsum1 = f85, f37,  xsum1
	}
	{ .mmf
	(p18) FMA	f94   = atemp2, f46, f94
	}
	;;
	{ .mmf
	(p16) LDFD	f96  = [YY], 1 * SIZE
	(p18) FMA	xsum2 = f85, f49,  xsum2
	}
	{ .mmf
	(p18) FMA	f98   = atemp2, f49, f98
	}
	;;
	{ .mmf
	(p16) LDFPD	f68, f71  = [A4], 2 * SIZE
	(p19) STFD	[YYS] = f107, 1 * SIZE
	(p18) FMA	xsum3 = f85, f61,  xsum3
	}
	{ .mmf
	(p18) FMA	f102  = atemp2, f52, f102
	}
	;;
	{ .mmf
	(p16) LDFD	f100 = [YY], 1 * SIZE
	(p18) FMA	xsum4 = f85, f73,  xsum4
	}
	{ .mmf
	(p18) FMA	f106  = atemp2, f55, f106
	}
	;;
	{ .mmf
	(p16) PREFETCH	[PREA1], 4 * SIZE
	(p16) LDFPD	f38, f41  = [A1], 2 * SIZE
	(p18) FMA	xsum1 = f88, f40,  xsum1
	}
	{ .mmf
	(p18) FMA	f94   = atemp3, f58, f94
	}
	;;
	{ .mmf
	(p16) LDFD	f104 = [YY], 1 * SIZE
	(p18) FMA	xsum2 = f88, f52,  xsum2
	}
	{ .mmf
	(p18) FMA	f98   = atemp3, f61, f98
	}
	;;
	{ .mmf
	(p16) PREFETCH	[PREA2], 4 * SIZE
	(p16) LDFPD	f50, f53  = [A2], 2 * SIZE
	(p18) FMA	xsum3 = f88, f64,  xsum3
	}
	{ .mmf
	(p18) FMA	f102  = atemp3, f64, f102
	}
	;;
	{ .mmf
	(p16) PREFETCH	[PREX], 4 * SIZE
	(p16) LDFD	f80  = [XX], 1 * SIZE
	(p18) FMA	xsum4 = f88, f76,  xsum4
	}
	{ .mmf
	(p18) FMA	f106  = atemp3, f67, f106
	}
	;;
	{ .mmf
	(p16) PREFETCH	[PREA3], 4 * SIZE
	(p16) LDFPD	f62, f65  = [A3], 2 * SIZE
	(p18) FMA	xsum1 = f91, f43,  xsum1
	}
	{ .mmf
	(p18) FMA	f94   = atemp4, f70, f94
	}
	;;
	{ .mmf
	(p16) LDFD	f83  = [XX], 1 * SIZE
	(p18) FMA	xsum2 = f91, f55,  xsum2
	}
	{ .mmf
	(p18) FMA	f98   = atemp4, f73, f98
	}
	;;
	{ .mmf
	(p16) PREFETCH	[PREA4], 4 * SIZE
	(p16) LDFPD	f74, f77  = [A4], 2 * SIZE
	(p18) FMA	xsum3 = f91, f67,  xsum3
	}
	{ .mmf
	(p18) FMA	f102  = atemp4, f76, f102
	}
	;;
	{ .mmf
	(p16) LDFD	f86  = [XX], 1 * SIZE
	(p18) FMA	xsum4 = f91, f79,  xsum4
	}
	{ .mfb
	(p18) FMA	f106  = atemp4, f79, f106
	br.ctop.sptk.few .L22
	}
	;;
	(p19) STFD	[YYS] = f95, 1 * SIZE
	;;
	(p19) STFD	[YYS] = f99, 1 * SIZE
	;;
	(p19) STFD	[YYS] = f103, 1 * SIZE
	;;
	(p19) STFD	[YYS] = f107, 1 * SIZE
	;;
	;;
	.align 16

.L28:
	FMPY	xsum1 = alpha, xsum1
	FMPY	xsum2 = alpha, xsum2
	FMPY	xsum3 = alpha, xsum3
	FMPY	xsum4 = alpha, xsum4
	;;
	LDFD	f64  = [A1], 1 * SIZE
	LDFD	f65  = [A2], 1 * SIZE
	LDFD	f66  = [A3], 1 * SIZE
	LDFD	f67  = [A4], 1 * SIZE
	;;
	LDFD	f68  = [A1], 1 * SIZE
	LDFD	f69  = [A2], 1 * SIZE
	LDFD	f70  = [A3], 1 * SIZE
	LDFD	f71  = [A4], 1 * SIZE
	;;
	LDFD	f72  = [A1], 1 * SIZE
	LDFD	f73  = [A2], 1 * SIZE
	LDFD	f74  = [A3], 1 * SIZE
	LDFD	f75  = [A4], 1 * SIZE
	;;
	LDFD	f76  = [A1], 1 * SIZE
	LDFD	f77  = [A2], 1 * SIZE
	LDFD	f78  = [A3], 1 * SIZE
	LDFD	f79  = [A4], 1 * SIZE
	;;
	FMA	xsum1 = atemp1, f64, xsum1
	FMA	xsum2 = atemp1, f65, xsum2
	FMA	xsum3 = atemp1, f66, xsum3
	FMA	xsum4 = atemp1, f67, xsum4
	;;
	FMA	xsum1 = atemp2, f65, xsum1
	FMA	xsum2 = atemp2, f69, xsum2
	FMA	xsum3 = atemp2, f70, xsum3
	FMA	xsum4 = atemp2, f71, xsum4
	;;
	FMA	xsum1 = atemp3, f66, xsum1
	FMA	xsum2 = atemp3, f70, xsum2
	FMA	xsum3 = atemp3, f74, xsum3
	FMA	xsum4 = atemp3, f75, xsum4
	;;
	FMA	xsum1 = atemp4, f67, xsum1
	FMA	xsum2 = atemp4, f71, xsum2
	FMA	xsum3 = atemp4, f75, xsum3
	FMA	xsum4 = atemp4, f79, xsum4
	;;
	LDFD	f36  = [YY], 1 * SIZE
	;;
	LDFD	f37  = [YY], 1 * SIZE
	;;
	LDFD	f38  = [YY], 1 * SIZE
	;;
	LDFD	f39  = [YY], 1 * SIZE
	;;
	FADD	f36 = f36, xsum1
	FADD	f37 = f37, xsum2
	FADD	f38 = f38, xsum3
	FADD	f39 = f39, xsum4
	;;
	STFD	[YYS] = f36, 1 * SIZE
	;;
	STFD	[YYS] = f37, 1 * SIZE
	;;
	STFD	[YYS] = f38, 1 * SIZE
	;;
	STFD	[YYS] = f39, 1 * SIZE
	;;
	adds	IS = 4, IS
	;;
	adds	TEMP = 4, IS
	;;
	cmp.le	p6, p0 = TEMP, M
	;;
	(p6) br.cond.dpnt .L21
	;;
.L30:


.L990:

.L999:
	mov	r8 = r0
	adds	r9 = 1 * 16, SP
	;;
	ldf.fill  f16 = [SP], 32
	ldf.fill  f17 = [r9], 32
	mov	 ar.lc = ARLC
	;;
	ldf.fill  f18 = [SP], 32
	ldf.fill  f19 = [r9], 32
	mov pr    = PR, -1
	;;
	ldf.fill  f20 = [SP], 32
	ldf.fill  f21 = [r9], 32
	mov	ar.pfs = ARPFS
	;;
	ldf.fill  f22 = [SP], 32
	ldf.fill  f23 = [r9]
	br.ret.sptk.many b0
	;;
	EPILOGUE
