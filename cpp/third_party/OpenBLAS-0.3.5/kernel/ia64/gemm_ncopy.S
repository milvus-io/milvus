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

#define PREFETCHSIZE   64
#define WPREFETCHSIZE  32

#ifndef XDOUBLE
#define LD	LDF8
#define ST	STF8_NTA
#else
#define LD	LDFD
#define ST	STFD_NTA
#endif

#define J	r15
#define PREB	r17
#define PREA	r18

#define A1	r19
#define A2	r20
#define A3	r21
#define A4	r22
#define A5	r23
#define A6	r24
#define A7	r25
#define A8	r26
#define B1	r27
#define B2	r28

#define COUNT	r9
#define I	r10
#define II	r11

#define ARLC	r29
#define PR	r30

#define M	r32
#define N	r33
#define A	r34
#define LDA	r35
#define B	r36

	PROLOGUE
	.prologue
	PROFCODE

	.body
	{ .mii
	shladd	LDA = LDA, BASE_SHIFT, r0
	mov	PR = pr
	shr	J = N, 3
	}
	;;
	{ .mib
	cmp.eq	p8,  p0 = 0, J
	mov	ARLC  = ar.lc
	(p8)  br.cond.dpnt .L20
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
	shladd	A3 = LDA, 1, A
	shladd	A5 = LDA, 2, A
	adds	I = 1, M
	}
	;;
	{ .mmi
	shladd	A4 = LDA, 1, A2
	shladd	A6 = LDA, 2, A2
	mov	ar.ec  = 6
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	shladd	A7 = LDA, 2, A3
	shr	I = I, 1
	}
	;;
	{ .mmi
	adds	B1 = 8 * SIZE, B
	shladd	A8 = LDA, 2, A4
	shladd	A = LDA, 3, A
	}
	{ .mmi
	adds	I = -1, I
	mov	COUNT = 0
	adds	J = -1, J
	}
	;;
	{ .mmi
	adds	PREA =  PREFETCHSIZE * SIZE, A
	adds	PREB = WPREFETCHSIZE * SIZE, B
	mov	ar.lc = I
	}
	{ .mmi
	mov	I  = M
	mov	II = M
	cmp.ne p14, p0 = r0, r0
	}
	;;
	.align 32

.L12:
	{ .mmi
	(p21) ST	[B ] = f37,  1 * SIZE
	(p14) ST	[B1] = f49,  1 * SIZE
	(p16) cmp.ne.unc p13, p0 = 1, I
	}
	{ .mmi
	lfetch.nt1	[PREA], LDA
	lfetch.excl.nt1 [PREB]
	adds	PREB = 16 * SIZE, PREB
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f43,  1 * SIZE
	(p14) ST	[B1] = f55,  1 * SIZE
	cmp.eq	p9, p0 = 8, COUNT
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f38  = [A2], SIZE
	(p16) adds	I  = -2, I
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f61,  1 * SIZE
	(p14) ST	[B1] = f73,  1 * SIZE
	(p9) mov COUNT = 0
	}
	{ .mmi
	(p13) LD	f44  = [A1], SIZE
	(p13) LD	f50  = [A2], SIZE
	(p21) adds	II = -2, II
	}
	;;
	{ .mmb
	(p21) ST	[B ] = f67,  1 * SIZE
	(p14) ST	[B1] = f79,  1 * SIZE
	nop   __LINE__
	}
	{ .mmb
	(p16) LD	f56  = [A3], SIZE
	(p16) LD	f62  = [A4], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f85,  1 * SIZE
	(p14) ST	[B1] = f97,  1 * SIZE
	(p9) adds PREA =  (PREFETCHSIZE - 2)* SIZE, A1
	}
	{ .mmb
	(p13) LD	f68  = [A3], SIZE
	(p13) LD	f74  = [A4], SIZE
	nop   __LINE__
	}
	;;
	{ .mmb
	(p21) ST	[B ] = f91,  1 * SIZE
	(p14) ST	[B1] = f103, 1 * SIZE
	nop   __LINE__
	}
	{ .mmb
	(p16) LD	f80  = [A5], SIZE
	(p16) LD	f86  = [A6], SIZE
	nop   __LINE__
	}
	;;
	{ .mmb
	(p21) ST	[B ] = f109, 1 * SIZE
	(p14) ST	[B1] = f121, 1 * SIZE
	nop   __LINE__
	}
	{ .mmb
	(p13) LD	f92  = [A5], SIZE
	(p13) LD	f98  = [A6], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f115, 1 * SIZE
	(p14) ST	[B1] = f127, 9 * SIZE
	(p16) adds	COUNT = 1, COUNT
	}
	{ .mmb
	(p16) LD	f104 = [A7], SIZE
	(p16) LD	f110 = [A8], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) LD	f116 = [A7], SIZE
	(p13) LD	f122 = [A8], SIZE
	(p14) adds	B = 8 * SIZE, B
	}
	{ .mmb
	(p20) cmp.ne.unc p14, p0 = 1, II
	nop   __LINE__
	br.ctop.sptk.few .L12
	}
	;;
	{ .mmb
	cmp.ne	p6, p0 = 0, J
	nop   __LINE__
	(p6) br.cond.dptk .L11
	}
	;;
	.align 32

.L20:
	{ .mmi
	adds	I = 1, M
	mov	A1 = A
	mov	pr.rot = 0
	}
	{ .mmi
	add	A2 = A, LDA
	shladd	A3 = LDA, 1, A
	tbit.z	p6, p0 = N, 2
	}
	;;
	{ .mmi
	shladd	A4 = LDA, 1, A2
	adds	B1 = 4 * SIZE, B
	mov	ar.ec  = 6
	}
	{ .mib
	cmp.eq	p16, p0 = r0, r0
	shr	I = I, 1
	(p6)  br.cond.dpnt .L30
	}
	;;
	{ .mmi
	shladd	A = LDA, 2, A
	nop	__LINE__
	nop	__LINE__
	}
	{ .mmi
	adds	I = -1, I
	mov	COUNT = 0
	adds	J = -1, J
	}
	;;
	{ .mmi
	adds	PREA =  PREFETCHSIZE * SIZE, A
	adds	PREB = WPREFETCHSIZE * SIZE, B
	mov	ar.lc = I
	}
	{ .mmi
	mov	I  = M
	mov	II = M
	cmp.ne p14, p0 = r0, r0
	}
	;;
	.align 32

.L22:
	{ .mmi
	(p21) ST	[B ] = f37,  1 * SIZE
	(p14) ST	[B1] = f49,  1 * SIZE
	(p16) cmp.ne.unc p13, p0 = 1, I
	}
	{ .mmi
	lfetch.nt1	[PREA], LDA
	lfetch.excl.nt1 [PREB], 8 * SIZE
	cmp.eq	p9, p0 = 4, COUNT
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f43,  1 * SIZE
	(p14) ST	[B1] = f55,  1 * SIZE
	(p16) adds	I  = -2, I
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f38  = [A2], SIZE
	(p21) adds	II = -2, II
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f61,  1 * SIZE
	(p14) ST	[B1] = f73,  1 * SIZE
	(p9) mov COUNT = 0
	}
	{ .mmi
	(p13) LD	f44  = [A1], SIZE
	(p13) LD	f50  = [A2], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f67,  1 * SIZE
	(p14) ST	[B1] = f79,  5 * SIZE
	(p9) adds PREA =  PREFETCHSIZE * SIZE, A1
	}
	{ .mmb
	(p16) LD	f56  = [A3], SIZE
	(p16) LD	f62  = [A4], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) LD	f68  = [A3], SIZE
	(p13) LD	f74  = [A4], SIZE
	(p16) adds	COUNT = 1, COUNT
	}
	{ .mmb
	(p14) adds	B = 4 * SIZE, B
	(p20) cmp.ne.unc p14, p0 = 1, II
	br.ctop.sptk.few .L22
	}
	;;
	.align 32

.L30:
	{ .mmi
	adds	I = 1, M
	mov	A1 = A
	mov	pr.rot = 0
	}
	{ .mmi
	add	A2 = A, LDA
	adds	B1 = 2 * SIZE, B
	tbit.z	p6, p0 = N, 1
	}
	;;
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	mov	ar.ec  = 6
	}
	{ .mib
	cmp.eq	p16, p0 = r0, r0
	shr	I = I, 1
	(p6)  br.cond.dpnt .L40
	}
	;;
	{ .mmi
	adds	I = -1, I
	;;
	shladd	A = LDA, 1, A
	mov	ar.lc = I
	}
	{ .mmi
	mov	I  = M
	mov	II = M
	cmp.ne p14, p0 = r0, r0
	}
	;;
	.align 32

.L32:
	{ .mmi
	(p21) ST	[B ] = f37,  1 * SIZE
	(p14) ST	[B1] = f49,  1 * SIZE
	(p16) cmp.ne.unc p13, p0 = 1, I
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	(p21) adds	II = -2, II
	}
	;;
	{ .mmi
	(p21) ST	[B ] = f43,  1 * SIZE
	(p14) ST	[B1] = f55,  3 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f38  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) LD	f44  = [A1], SIZE
	(p13) LD	f50  = [A2], SIZE
	(p16) adds	I  = -2, I
	}
	{ .mmb
	(p14) adds	B = 2 * SIZE, B
	(p20) cmp.ne.unc p14, p0 = 1, II
	br.ctop.sptk.few .L32
	}
	;;
	.align 32

.L40:
	{ .mmi
	adds	I = 1, M
	mov	A1 = A
	mov	pr.rot = 0
	}
	{ .mmi
	tbit.z	p6, p0 = N, 0
	}
	;;
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	mov	ar.ec  = 6
	}
	{ .mib
	cmp.eq	p16, p0 = r0, r0
	shr	I = I, 1
	(p6)  br.cond.dpnt .L999
	}
	;;
	{ .mmi
	adds	I = -1, I
	;;
	mov	ar.lc = I
	}
	{ .mmi
	mov	I  = M
	mov	II = M
	cmp.ne p14, p0 = r0, r0
	}
	;;
	.align 32

.L42:
	{ .mmi
	(p21) ST	[B ] = f37,  1 * SIZE
	(p16) cmp.ne.unc p13, p0 = 1, I
	(p21) adds	II = -2, II
	}
	;;
	{ .mmi
	(p14) ST	[B ] = f49,  1 * SIZE
	(p16) LD	f32  = [A1], SIZE
	(p16) adds	I  = -2, I
	}
	;;
	{ .mmb
	(p13) LD	f44  = [A1], SIZE
	(p20) cmp.ne.unc p14, p0 = 1, II
	br.ctop.sptk.few .L42
	}
	;;
	.align 32

.L999:
	mov pr    = PR, -1
	mov	 ar.lc = ARLC
	br.ret.sptk.many b0
	EPILOGUE

