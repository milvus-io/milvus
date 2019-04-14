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

#define PREFETCHSIZE   24
#define WPREFETCHSIZE  48

#define LD	LDF8
#define ST	STF8_NTA

#define PREA	r2
#define PREB	r3

#define I	r14
#define J	r15

#define A1	r16
#define A2	r17
#define A3	r18
#define A4	r19
#define A5	r20
#define A6	r21
#define A7	r22
#define A8	r23
#define B1	r24
#define B2	r25

#define COUNT	r26
#define TEMP	r27

#define BO2	r28
#define BO3	r29
#define LDB	r8

#define ARLC	r30
#define PR	r31

#define M	r32
#define N	r33
#define A	r34
#define LDA	r35
#define B	r36

	PROLOGUE
	.prologue
	PROFCODE

	.body
	{ .mmi
	setf.sig f32 = M
	and	r8  = -4, N
	mov	ARLC  = ar.lc
	}
	;;
	{ .mmi
	setf.sig f33  = r8
	and	r9  = -2, N
	mov	PR = pr
	}
	;;
	{ .mmi
	setf.sig f34  = r9
	shladd	LDA = LDA, ZBASE_SHIFT, r0
	shl	LDB = M, BASE_SHIFT + 3
	}
	;;
	{ .mfi
	nop	 __LINE__
	xmpy.l	f33  = f32, f33
	shr	J = M, 2
	}
	{ .mfi
	nop	 __LINE__
	xmpy.l	f34  = f32, f34
	nop	 __LINE__
	}
	;;
	{ .mmb
	getf.sig BO2 = f33
	getf.sig BO3 = f34
	nop	 __LINE__
	}
	;;
	{ .mmi
	shladd	BO2 = BO2, ZBASE_SHIFT, B
	shladd	BO3 = BO3, ZBASE_SHIFT, B
	tbit.nz p10, p0 =N, 1
	}
	{ .mib
	cmp.eq	p6, p0 = 0, J
	tbit.nz p11, p0 =N, 0
	(p6)	br.cond.dpnt .L20
	}
	;;
	.align 32

.L11:
	{ .mmi
	mov	A1 = A
	add	A2 = A, LDA
	mov	pr.rot = 0
	}
	{ .mmi
	shladd A3 = LDA, 1, A
	mov    B1 = B
	shr    I  = N, 2
	}
	;;
	{ .mmi
	shladd	A4 = LDA, 1, A2
	cmp.eq	p16,p0 = r0, r0
	mov	ar.ec = 3
	}
	{ .mmi
	cmp.eq	p6,p0 = 0,I
	adds	I =-1, I
	adds	J =-1, J
	}
	;;
	{ .mmi
	shladd	A = LDA, 2, A
	adds	A5 = 4 * SIZE, A1
	adds	A6 = 4 * SIZE, A2
	}
	{ .mmi
	adds	A7 = 4 * SIZE, A3
	adds	A8 = 4 * SIZE, A4
	adds	PREA = PREFETCHSIZE * SIZE,A1
	}
	;;
	{ .mmb
	adds   B2 = 4 * SIZE, B
	adds	PREB = WPREFETCHSIZE * SIZE, B
	nop	__LINE__
	}
	{ .mib
	adds   B  = 32 * SIZE, B
	mov	ar.lc = I
	(p6) br.cond.dpnt.few .L15
	}
	;;

.L12:
	{ .mmb
	(p16)	lfetch.nt1	[PREA], LDA
	(p16)	lfetch.excl.nt1	[PREB], LDB
	nop	__LINE__
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f34, SIZE
	(p18)	ST	[B2] = f37, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f32 = [A1], SIZE
	(p16)	LD	f35 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f40, SIZE
	(p18)	ST	[B2] = f43, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f38 = [A1], SIZE
	(p16)	LD	f41 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f46,  SIZE
	(p18)	ST	[B2] = f49,  SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f44 = [A1], SIZE
	(p16)	LD	f47 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f52,  5 * SIZE
	(p18)	ST	[B2] = f55,  5 * SIZE
	tbit.z	p0,p7 = COUNT,0
	}
	{ .mmb
	(p16)	LD	f50 = [A1], 5 * SIZE
	(p16)	LD	f53 = [A5], 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f58, SIZE
	(p18)	ST	[B2] = f61, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f56 = [A2], SIZE
	(p16)	LD	f59 = [A6], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f64, SIZE
	(p18)	ST	[B2] = f67, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f62 = [A2], SIZE
	(p16)	LD	f65 = [A6], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f70, SIZE
	(p18)	ST	[B2] = f73, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f68 = [A2], SIZE
	(p16)	LD	f71 = [A6], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1]  = f76, 5 * SIZE
	(p18)	ST	[B2]  = f79, 5 * SIZE
	shladd	TEMP = LDA, 2, r0
	}
	{ .mmb
	(p16)	LD	f74 = [A2], 5 * SIZE
	(p16)	LD	f77 = [A6], 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f82, SIZE
	(p18)	ST	[B2] = f85, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	lfetch.nt1	[PREA], LDA
	(p16)	lfetch.excl.nt1	[PREB], LDB
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f88, SIZE
	(p18)	ST	[B2] = f91, SIZE
	adds	TEMP = -16 * SIZE, TEMP
	}
	{ .mmb
	(p16)	LD	f80 = [A3], SIZE
	(p16)	LD	f83 = [A7], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f94, SIZE
	(p18)	ST	[B2] = f97, SIZE
	(p7)	sub	PREA = PREA, TEMP
	}
	{ .mmb
	(p16)	LD	f86 = [A3], SIZE
	(p16)	LD	f89 = [A7], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f100, 5 * SIZE
	(p18)	ST	[B2] = f103, 5 * SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f92 = [A3], SIZE
	(p16)	LD	f95 = [A7], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f106, SIZE
	(p18)	ST	[B2] = f109, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f98  = [A3], 5 * SIZE
	(p16)	LD	f101 = [A7], 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f112, SIZE
	(p18)	ST	[B2] = f115, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f104 = [A4], SIZE
	(p16)	LD	f107 = [A8], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f118, SIZE
	(p18)	ST	[B2] = f121, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f110 = [A4], SIZE
	(p16)	LD	f113 = [A8], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f124, -27 * SIZE
	(p18)	ST	[B2] = f127, -27 * SIZE
	(p16)	adds	COUNT =  1, COUNT
	}
	{ .mmb
	(p16)	LD	f116 = [A4], SIZE
	(p16)	LD	f119 = [A8], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18) add	B1 = B1, LDB
	(p18) add	B2 = B2, LDB
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f122 = [A4], 5 * SIZE
	(p16)	LD	f125 = [A8], 5 * SIZE
	br.ctop.sptk.few .L12
	}
	;;
	.align 32

.L15:
	{ .mmb
	(p10)	LD	f32 = [A1], SIZE
	(p10)	LD	f40 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f33 = [A1], SIZE
	(p10)	LD	f41 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f34 = [A1], SIZE
	(p10)	LD	f42 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f35 = [A1], SIZE
	(p10)	LD	f43 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f50 = [A3], SIZE
	(p10)	LD	f60 = [A4], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f51 = [A3], SIZE
	(p10)	LD	f61 = [A4], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f52 = [A3], SIZE
	(p10)	LD	f62 = [A4], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f53 = [A3], SIZE
	(p10)	LD	f63 = [A4], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p11)	LD	f36 = [A1], SIZE
	(p11)	LD	f44 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p11)	LD	f37 = [A1]
	(p11)	LD	f45 = [A2]
	nop	__LINE__
	}
	;;
	{ .mmb
	(p11)	LD	f54 = [A3], SIZE
	(p11)	LD	f64 = [A4], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p11)	LD	f55 = [A3]
	(p11)	LD	f65 = [A4]
	adds	B2 = 4 * SIZE, BO2
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f32, SIZE
	(p10)	ST	[B2]  = f40, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f33, SIZE
	(p10)	ST	[B2]  = f41, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f34, SIZE
	(p10)	ST	[B2]  = f42, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f35, 5 * SIZE
	(p10)	ST	[B2]  = f43, 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f50, SIZE
	(p10)	ST	[B2]  = f60, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f51, SIZE
	(p10)	ST	[B2]  = f61, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f52, SIZE
	(p10)	ST	[B2]  = f62, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p10)	ST	[BO2] = f53, 5 * SIZE
	(p10)	ST	[B2]  = f63
	adds	B2 = 4 * SIZE, BO3
	}
	;;
	{ .mmb
	(p11)	ST	[BO3] = f36, SIZE
	(p11)	ST	[B2] = f54, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p11)	ST	[BO3] = f37, SIZE
	(p11)	ST	[B2] = f55, SIZE
	mov	COUNT = r0
	}
	;;
	{ .mmi
	(p11)	ST	[BO3] = f44, SIZE
	(p11)	ST	[B2] = f64, SIZE
	cmp.eq	p0,p6 = 0,J
	}
	;;
	{ .mmb
	(p11)	ST	[BO3] = f45, 5 * SIZE
	(p11)	ST	[B2] = f65, 5 * SIZE
	(p6)	br.cond.dptk.few .L11
	}
	;;
	.align 32

.L20:
	{ .mmi
	mov	A1 = A
	add	A2 = A, LDA
	mov	pr.rot = 0
	}
	{ .mmi
	mov    B1 = B
	adds	PREA = PREFETCHSIZE * SIZE,A
	tbit.z	p6, p0 = M, 1
	}
	;;
	{ .mmi
	cmp.eq	p16,p0 = r0, r0
	adds   B2 = 4 * SIZE, B
	mov	ar.ec = 3
	}
	{ .mib
	adds	PREB = WPREFETCHSIZE * SIZE, B
	shr    I  = N, 2
	(p6)	br.cond.dpnt .L30
	}
	;;
	{ .mmi
	cmp.eq	p6, p0 = 0, I
	adds	I =-1, I
	nop	__LINE__
	}
	{ .mmi
	shladd	A = LDA, 1, A
	adds	A5 = 4 * SIZE, A1
	adds	A6 = 4 * SIZE, A2
	}
	;;
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	nop	__LINE__
	}
	{ .mib
	adds   B  = 16 * SIZE, B
	mov	ar.lc = I
	(p6) br.cond.dpnt.few .L25
	}
	;;

.L22:
	{ .mmi
	(p16)	lfetch.nt1	[PREA], LDA
	(p16)	lfetch.excl.nt1	[PREB], LDB
	shladd	TEMP = LDA, 1, r0
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f34, SIZE
	(p18)	ST	[B2] = f37, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f32 = [A1], SIZE
	(p16)	LD	f35 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f40, SIZE
	(p18)	ST	[B2] = f43, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f38 = [A1], SIZE
	(p16)	LD	f41 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f46,  SIZE
	(p18)	ST	[B2] = f49,  SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f44 = [A1], SIZE
	(p16)	LD	f47 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f52,  5 * SIZE
	(p18)	ST	[B2] = f55,  5 * SIZE
	tbit.z	p0,p7 = COUNT,0
	}
	{ .mmb
	(p16)	LD	f50 = [A1], 5 * SIZE
	(p16)	LD	f53 = [A5], 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f58, SIZE
	(p18)	ST	[B2] = f61, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f56 = [A2], SIZE
	(p16)	LD	f59 = [A6], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f64, SIZE
	(p18)	ST	[B2] = f67, SIZE
	adds	TEMP = -16 * SIZE, TEMP
	}
	{ .mmb
	(p16)	LD	f62 = [A2], SIZE
	(p16)	LD	f65 = [A6], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f70,  SIZE
	(p18)	ST	[B2] = f73,  SIZE
	(p7)	sub	PREA = PREA, TEMP
	}
	{ .mmb
	(p16)	LD	f68 = [A2], SIZE
	(p16)	LD	f71 = [A6], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f76, -11 * SIZE
	(p18)	ST	[B2] = f79, -11 * SIZE
	(p16)	adds	COUNT =  1, COUNT
	}
	{ .mmb
	(p16)	LD	f74 = [A2], 5 * SIZE
	(p16)	LD	f77 = [A6], 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18) add	B1 = B1, LDB
	(p18) add	B2 = B2, LDB
	br.ctop.sptk.few .L22
	}
	;;
	.align 32

.L25:
	{ .mmb
	(p10)	LD	f32 = [A1], SIZE
	(p10)	LD	f40 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f33 = [A1], SIZE
	(p10)	LD	f41 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f34 = [A1], SIZE
	(p10)	LD	f42 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	LD	f35 = [A1], SIZE
	(p10)	LD	f43 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p11)	LD	f36 = [A1], SIZE
	(p11)	LD	f44 = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p11)	LD	f37 = [A1]
	(p11)	LD	f45 = [A2]
	adds	B2 = 4 * SIZE, BO2
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f32, SIZE
	(p10)	ST	[B2]  = f40, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f33, SIZE
	(p10)	ST	[B2]  = f41, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f34, SIZE
	(p10)	ST	[B2]  = f42, SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p10)	ST	[BO2] = f35, 5 * SIZE
	(p10)	ST	[B2]  = f43, 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p11)	ST	[BO3] = f36, SIZE
	;;
	(p11)	ST	[BO3] = f37, SIZE
	mov	COUNT = r0
	}
	;;
	{ .mmi
	(p11)	ST	[BO3] = f44, SIZE
	;;
	(p11)	ST	[BO3] = f45, SIZE
	nop	__LINE__
	}
	;;
	.align 32

.L30:
	{ .mmi
	mov	A1 = A
	adds	A5 = 4 * SIZE, A
	mov	pr.rot = 0
	}
	{ .mmi
	mov    B1 = B
	adds   B2 = 4 * SIZE, B
	tbit.z	p6, p0 = M, 0
	}
	;;
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	nop	__LINE__
	}
	{ .mib
	cmp.eq	p16,p0 = r0, r0
	shr    I  = N, 2
	(p6)	br.cond.dpnt .L999
	}
	;;
	{ .mmi
	cmp.eq	p6, p0 = 0, I
	adds	I =-1, I
	mov	ar.ec = 3
	}
	;;
	{ .mib
	nop	__LINE__
	mov	ar.lc = I
	(p6) br.cond.dpnt.few .L35
	}
	;;
	.align 32

.L32:
	{ .mmb
	(p18)	ST	[B1] = f34, SIZE
	(p18)	ST	[B2] = f37, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f32 = [A1], SIZE
	(p16)	LD	f35 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f40, SIZE
	(p18)	ST	[B2] = f43, SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f38 = [A1], SIZE
	(p16)	LD	f41 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	(p18)	ST	[B1] = f46,  SIZE
	(p18)	ST	[B2] = f49,  SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f44 = [A1], SIZE
	(p16)	LD	f47 = [A5], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18)	ST	[B1] = f52, -3 * SIZE
	(p18)	ST	[B2] = f55, -3 * SIZE
	nop	__LINE__
	}
	{ .mmb
	(p16)	LD	f50 = [A1], 5 * SIZE
	(p16)	LD	f53 = [A5], 5 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	nop	__LINE__
	}
	{ .mmb
	(p18) add	B1 = B1, LDB
	(p18) add	B2 = B2, LDB
	br.ctop.sptk.few .L32
	}
	;;
	.align 32

.L35:
	{ .mmi
	(p10)	LD	f32 = [A1], SIZE
	;;
	(p10)	LD	f33 = [A1], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p10)	LD	f34 = [A1], SIZE
	;;
	(p10)	LD	f35 = [A1], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p11)	LD	f36 = [A1], SIZE
	;;
	(p11)	LD	f37 = [A1]
	nop	__LINE__
	}
	;;
	{ .mmi
	(p10)	ST	[BO2] = f32, SIZE
	;;
	(p10)	ST	[BO2] = f33, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p10)	ST	[BO2] = f34, SIZE
	;;
	(p10)	ST	[BO2] = f35, SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p11)	ST	[BO3] = f36, SIZE
	;;
	(p11)	ST	[BO3] = f37, SIZE
	nop	__LINE__
	}
	;;
	.align 32

.L999:
	mov pr    = PR, -1
	mov	 ar.lc = ARLC
	br.ret.sptk.many b0
	EPILOGUE
