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
#define WPREFETCHSIZE  32

#ifndef XDOUBLE
#define LD	LDFD
#define ST	STFD_NTA
#else
#define LD	LDFD
#define ST	STFD_NTA
#endif

#define PREA	r2
#define PREB	r3

#define A1	r14
#define A2	r15
#define B1	r16
#define B2	r17
#define I	r18
#define J	r19

#define BO2	r20
#define BO3	r21
#define BO4	r22

#define LDB	r23
#define II	r24
#define TEMP1	r25
#define TEMP2	r26
#define TEMP3	r27
#define LCOUNT	r28
#define SCOUNT	r29

#define ARLC	r30
#define PR	r31

#define MLDA8	r8

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
	and	r8  = -8, N
	mov	ARLC  = ar.lc
	}
	;;
	{ .mmi
	setf.sig f33  = r8
	and	r9  = -4, N
	mov	PR = pr
	}
	;;
	{ .mmi
	setf.sig f34  = r9
	and	r10 = -2, N
	shladd	LDA = LDA, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	setf.sig f35 = r10
	shladd	MLDA8 = LDA, 3, r0
	shl	LDB = M, BASE_SHIFT + 3
	}
	;;
	{ .mfi
	sub	MLDA8 = r0, MLDA8
	xmpy.l	f33  = f32, f33
	shr	J = M, 3
	}
	{ .mfi
	xmpy.l	f34  = f32, f34
	}
	;;
	{ .mmf
	getf.sig BO2 = f33
	adds	MLDA8 = 16 * SIZE, MLDA8
	xmpy.l	f35 = f32, f35
	}
	;;
	{ .mmi
	getf.sig BO3 = f34
	getf.sig BO4 = f35
	nop	 __LINE__
	}
	;;
	{ .mmi
	shladd	BO2 = BO2, BASE_SHIFT, B
	shladd	BO3 = BO3, BASE_SHIFT, B
	shladd	BO4 = BO4, BASE_SHIFT, B
	}
	{ .mib
	cmp.eq	p6, p0 = 0, J
	nop	__LINE__
	(p6)	br.cond.dpnt .L100
	}
	;;
	.align 32

.L11:
	{ .mmi
	add	I = 8, N
	mov	A1 = A
	mov	pr.rot = 0
	}
	{ .mmi
	adds	A2 = 4 * SIZE, A
	shladd	A = LDA, 3, A
	shr	II = N, 3
	}
	;;
	{ .mmi
	mov	B1 =  B
	cmp.eq	p16, p0 = r0, r0
	mov	ar.ec  = 3
	}
	{ .mmi
	adds	B2 =  4 * SIZE, B
	adds	B  = 64 * SIZE, B
	shr	I = I, 4
	}
	;;
	{ .mmi
	cmp.eq	p8, p0 = 0, I
	shladd	I = I, 2, r0
	nop	__LINE__
	}
	;;
	{ .mmi
	mov	LCOUNT = 0
	mov	SCOUNT = 0
	adds	I = -1, I
	}
	;;
	{ .mmi
	adds  PREA =  PREFETCHSIZE * SIZE, A1
	adds  PREB = WPREFETCHSIZE * SIZE, B1
	mov	ar.lc = I
	}
	{ .mib
	adds	J = -1, J
	mov	I = II
	(p8)	br.cond.dpnt .L20
	}
	;;
	.align 32

.L12:
	{ .mmi
	(p18) ST	[B1] = f34,  1 * SIZE
	(p18) ST	[B2] = f46,  1 * SIZE
	(p18) cmp.ne.unc p13, p0 = 1, II
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], LDA
	(p16) lfetch.excl.nt1 [PREB], LDB
	(p16) cmp.ne.unc p12, p0 = 1, I
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f37,  1 * SIZE
	(p18) ST	[B2] = f49,  1 * SIZE
	(p18) adds	SCOUNT = 1, SCOUNT
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f44  = [A2], SIZE
	(p16) adds	LCOUNT = 1, LCOUNT
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f40,  1 * SIZE
	(p18) ST	[B2] = f52,  1 * SIZE
	(p16) cmp.eq.unc p14, p0 = 4, LCOUNT
	}
	{ .mmi
	(p16) LD	f35  = [A1], SIZE
	(p16) LD	f47  = [A2], SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f43,  5 * SIZE
	(p18) ST	[B2] = f55,  5 * SIZE
	(p18) cmp.eq.unc p15, p0 = 4, SCOUNT
	}
	{ .mmi
	(p16) LD	f38  = [A1], SIZE
	(p16) LD	f50  = [A2], SIZE
	(p12) mov TEMP1 = 5 * SIZE
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f82,  1 * SIZE
	(p18) ST	[B2] = f94,  1 * SIZE
	}
	{ .mmi
	(p16) LD	f41  = [A1], TEMP1
	(p16) LD	f53  = [A2], TEMP1
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f85,  1 * SIZE
	(p18) ST	[B2] = f97,  1 * SIZE
	mov	TEMP2 = 5 * SIZE
	}
	{ .mmi
	(p12) LD	f56  = [A1], SIZE
	(p12) LD	f68  = [A2], SIZE
	shladd TEMP3 = LDA, 3, r0
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f88,  1 * SIZE
	(p18) ST	[B2] = f100, 1 * SIZE
	(p13) adds TEMP2 = - 11 * SIZE, LDB
	}
	{ .mmi
	(p12) LD	f59  = [A1], SIZE
	(p12) LD	f71  = [A2], SIZE
	(p12) adds  TEMP1 = - 11 * SIZE, LDA
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f91
	(p18) ST	[B2] = f103
	(p18) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f62  = [A1], SIZE
	(p12) LD	f74  = [A2], SIZE
	(p18) add	B2 = B2, TEMP2
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f58,  1 * SIZE
	(p13) ST	[B2] = f70,  1 * SIZE
	}
	{ .mmi
	(p12) LD	f65  = [A1], TEMP1
	(p12) LD	f77  = [A2], TEMP1
	sub TEMP3 = LDA, TEMP3
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f61,  1 * SIZE
	(p13) ST	[B2] = f73,  1 * SIZE
	}
	{ .mmi
	(p16) lfetch.nt1 [PREA], LDA
	(p16) lfetch.excl.nt1 [PREB]
	adds TEMP3 = 5 * SIZE, TEMP3
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f64,  1 * SIZE
	(p13) ST	[B2] = f76,  1 * SIZE
	}
	{ .mmi
	(p16) LD	f80  = [A1], SIZE
	(p16) LD	f92  = [A2], SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f67,  5 * SIZE
	(p13) ST	[B2] = f79,  5 * SIZE
	}
	{ .mmi
	(p16) LD	f83  = [A1], SIZE
	(p16) LD	f95  = [A2], SIZE
	(p14) mov TEMP1 = TEMP3
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f106,  1 * SIZE
	(p13) ST	[B2] = f118,  1 * SIZE
	mov	TEMP2 = 5 * SIZE
	}
	{ .mmi
	(p16) LD	f86  = [A1], SIZE
	(p16) LD	f98  = [A2], SIZE
	(p12) mov TEMP1	= 5 * SIZE
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f109,  1 * SIZE
	(p13) ST	[B2] = f121,  1 * SIZE
	sub	TEMP2 = TEMP2, LDB
	}
	{ .mmi
	(p16) LD	f89  = [A1], TEMP1
	(p16) LD	f101 = [A2], TEMP1
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f112,  1 * SIZE
	(p13) ST	[B2] = f124,  1 * SIZE
	(p15) adds TEMP2 = -59 * SIZE, LDB
	}
	{ .mmi
	(p12) LD	f104 = [A1], SIZE
	(p12) LD	f116 = [A2], SIZE
	(p14)	add PREA = PREA, MLDA8
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f115
	(p13) ST	[B2] = f127
	(p13) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f107 = [A1], SIZE
	(p12) LD	f119 = [A2], SIZE
	adds	TEMP1 = -11 * SIZE, LDA
	}
	;;
	{ .mmi
	(p12) LD	f110 = [A1], SIZE
	(p12) LD	f122 = [A2], SIZE
	(p14) mov TEMP1 = TEMP3
	}
	{ .mmi
	(p14) mov	LCOUNT = 0
	(p15) mov	SCOUNT = 0
	adds  PREB = WPREFETCHSIZE * SIZE, B1
	}
	;;
	{ .mmi
	(p12) LD	f113 = [A1], TEMP1
	(p12) LD	f125 = [A2], TEMP1
	(p13) add	B2 = B2, TEMP2
	}
	{ .mib
	(p14) adds	I = -2, I
	(p15) adds	II = -2, II
	br.ctop.sptk .L12
	}
	;;
	.align 32

.L20:
	{ .mmi
	add	A2 = A1, LDA
	and	TEMP3 = 7, N
	tbit.nz p7, p0 = N, 2
	}
	;;
	{ .mmi
	(p7) LD	f32  = [A1], SIZE
	(p7) LD	f36  = [A2], SIZE
	cmp.eq	p6, p0 = 0, TEMP3
	}
	;;
	{ .mmi
	(p7) LD	f33  = [A1], SIZE
	(p7) LD	f37  = [A2], SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	;;
	{ .mmi
	(p7) LD	f34  = [A1], SIZE
	(p7) LD	f38  = [A2], SIZE
	add	TEMP1 = TEMP1, LDA
	}
	;;
	{ .mmi
	(p7) LD	f35  = [A1], TEMP1
	(p7) LD	f39  = [A2], TEMP1
	(p6) cmp.ne.unc	p10, p0 = 0, J
	}
	;;
	{ .mmb
	(p7) LD	f40  = [A1], SIZE
	(p7) LD	f44  = [A2], SIZE
	(p10) br.cond.dptk .L11
	}
	;;
	{ .mmi
	(p7) LD	f41  = [A1], SIZE
	(p7) LD	f45  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) LD	f42  = [A1], SIZE
	(p7) LD	f46  = [A2], SIZE
	tbit.nz p8, p0 = N, 1
	}
	;;
	{ .mmi
	(p7) LD	f43  = [A1], TEMP1
	(p7) LD	f47  = [A2], TEMP1
	adds	B2 = 4 * SIZE, BO2
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f32,  1 * SIZE
	(p7) ST	[B2 ] = f36,  1 * SIZE
	tbit.nz p9, p0 = N, 0
	}
	{ .mmi
	(p7) LD	f48  = [A1], SIZE
	(p7) LD	f52  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f33,  1 * SIZE
	(p7) ST	[B2 ] = f37,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f49  = [A1], SIZE
	(p7) LD	f53  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f34,  1 * SIZE
	(p7) ST	[B2 ] = f38,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f50  = [A1], SIZE
	(p7) LD	f54  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f35,  5 * SIZE
	(p7) ST	[B2 ] = f39,  5 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f51  = [A1], TEMP1
	(p7) LD	f55  = [A2], TEMP1
	mov	TEMP1 = -1 * SIZE
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f40,  1 * SIZE
	(p7) ST	[B2 ] = f44,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f56  = [A1], SIZE
	(p7) LD	f60  = [A2], SIZE
	shladd	TEMP1 = LDA, 3, TEMP1
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f41,  1 * SIZE
	(p7) ST	[B2 ] = f45,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f57  = [A1], SIZE
	(p7) LD	f61  = [A2], SIZE
	sub	TEMP1 = 0, TEMP1
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f42,  1 * SIZE
	(p7) ST	[B2 ] = f46,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f58  = [A1], SIZE
	(p7) LD	f62  = [A2], SIZE
	shladd	TEMP1 = LDA, 1, TEMP1
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f43,  5 * SIZE
	(p7) ST	[B2 ] = f47,  5 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p7) LD	f59  = [A1], TEMP1
	(p7) LD	f63  = [A2], TEMP1
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f48,  1 * SIZE
	(p7) ST	[B2 ] = f52,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	add	A2 = A1, LDA
	adds	TEMP1 = -1 * SIZE, LDA
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f49,  1 * SIZE
	(p7) ST	[B2 ] = f53,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f64  = [A1], SIZE
	(p8) LD	f66  = [A2], SIZE
	add	TEMP1 = TEMP1, LDA
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f50,  1 * SIZE
	(p7) ST	[B2 ] = f54,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f65  = [A1], TEMP1
	(p8) LD	f67  = [A2], TEMP1
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f51,  5 * SIZE
	(p7) ST	[B2 ] = f55,  5 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f68  = [A1], SIZE
	(p8) LD	f70  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f56,  1 * SIZE
	(p7) ST	[B2 ] = f60,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f69  = [A1], TEMP1
	(p8) LD	f71  = [A2], TEMP1
	mov	TEMP3 = -1 * SIZE
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f57,  1 * SIZE
	(p7) ST	[B2 ] = f61,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f72  = [A1], SIZE
	(p8) LD	f74  = [A2], SIZE
	shladd	TEMP3 = LDA, 3, TEMP3
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f58,  1 * SIZE
	(p7) ST	[B2 ] = f62,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f73  = [A1], TEMP1
	(p8) LD	f75  = [A2], TEMP1
	sub	TEMP3 = 0, TEMP3
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f59,  5 * SIZE
	(p7) ST	[B2 ] = f63
	adds	B2 = 4 * SIZE, BO3
	}
	{ .mmi
	(p8) LD	f76  = [A1], SIZE
	(p8) LD	f78  = [A2], SIZE
	shladd	TEMP3 = LDA, 1, TEMP3
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f64,  1 * SIZE
	(p8) ST	[B2 ] = f68,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p8) LD	f77  = [A1], TEMP3
	(p8) LD	f79  = [A2], TEMP3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f65,  1 * SIZE
	(p8) ST	[B2 ] = f69,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	add	A2 = A1, LDA
	shladd	TEMP3 = LDA, 1, r0
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f66,  1 * SIZE
	(p8) ST	[B2 ] = f70,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p9) LD	f80  = [A1], TEMP3
	(p9) LD	f81  = [A2], TEMP3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f67,  5 * SIZE
	(p8) ST	[B2 ] = f71,  5 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p9) LD	f82  = [A1], TEMP3
	(p9) LD	f83  = [A2], TEMP3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f72,  1 * SIZE
	(p8) ST	[B2 ] = f76,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p9) LD	f84  = [A1], TEMP3
	(p9) LD	f85  = [A2], TEMP3
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f73,  1 * SIZE
	(p8) ST	[B2 ] = f77,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p9) LD	f86  = [A1]
	(p9) LD	f87  = [A2]
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f74,  1 * SIZE
	(p8) ST	[B2 ] = f78,  1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f75,  5 * SIZE
	(p8) ST	[B2 ] = f79
	adds	B2 = 4 * SIZE, BO4
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f80,  1 * SIZE
	(p9) ST	[B2 ] = f84,  1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f81,  1 * SIZE
	(p9) ST	[B2 ] = f85,  1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f82,  1 * SIZE
	(p9) ST	[B2 ] = f86,  1 * SIZE
	cmp.ne	p8, p0 = 0, J
	}
	;;
	{ .mmb
	(p9) ST	[BO4] = f83,  5 * SIZE
	(p9) ST	[B2 ] = f87,  5 * SIZE
	(p8)	br.cond.dptk .L11
	}
	;;
	.align 32

.L100:
	{ .mmi
	mov	A1 = A
	add	I = 8, N
	mov	pr.rot = 0
	}
	{ .mmi
	adds	A2 = 4 * SIZE, A
	tbit.z p6, p0 = M, 2
	}
	;;
	{ .mmi
	mov	B1 =  B
	adds	B2 =  4 * SIZE, B
	mov	ar.ec  = 3
	}
	{ .mib
	cmp.eq	p16, p0 = r0, r0
	shr	I = I, 4
	(p6)	br.cond.dpnt .L200
	}
	;;
	{ .mmi
	cmp.eq	p8, p0 = 0, I
	shladd	I = I, 1, r0
	shladd	A = LDA, 2, A
	}
	;;
	{ .mmi
	adds	B  = 32 * SIZE, B
	adds	I = -1, I
	shr	II = N, 3
	}
	;;
	{ .mmi
	mov	LCOUNT = 0
	mov	SCOUNT = 0
	mov	ar.lc = I
	}
	{ .mib
	nop	__LINE__
	mov	I = II
	(p8)	br.cond.dpnt .L120
	}
	;;
	.align 32

.L112:
	{ .mmi
	(p18) ST	[B1] = f34,  1 * SIZE
	(p18) ST	[B2] = f46,  1 * SIZE
	(p16) cmp.ne.unc p12, p0 = 1, I
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f44  = [A2], SIZE
	(p18) cmp.ne.unc p13, p0 = 1, II
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f37,  1 * SIZE
	(p18) ST	[B2] = f49,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f35  = [A1], SIZE
	(p16) LD	f47  = [A2], SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f40,  1 * SIZE
	(p18) ST	[B2] = f52,  1 * SIZE
	shladd TEMP3 = LDA, 2, r0
	}
	{ .mmi
	(p16) LD	f38  = [A1], SIZE
	(p16) LD	f50  = [A2], SIZE
	(p12) mov TEMP1 = 5 * SIZE
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f43,  5 * SIZE
	(p18) ST	[B2] = f55,  5 * SIZE
	(p16) adds	LCOUNT = 1, LCOUNT
	}
	{ .mmi
	(p16) LD	f41  = [A1], TEMP1
	(p16) LD	f53  = [A2], TEMP1
	(p18) adds	SCOUNT = 1, SCOUNT
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f82,  1 * SIZE
	(p18) ST	[B2] = f94,  1 * SIZE
	(p16) cmp.eq.unc p14, p0 = 2, LCOUNT
	}
	{ .mmi
	(p12) LD	f56  = [A1], SIZE
	(p12) LD	f68  = [A2], SIZE
	(p18) cmp.eq.unc p15, p0 = 2, SCOUNT
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f85,  1 * SIZE
	(p18) ST	[B2] = f97,  1 * SIZE
	mov	TEMP2 = 5 * SIZE
	}
	{ .mmi
	(p12) LD	f59  = [A1], SIZE
	(p12) LD	f71  = [A2], SIZE
	sub TEMP3 = LDA, TEMP3
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f88,  1 * SIZE
	(p18) ST	[B2] = f100, 1 * SIZE
	(p13) adds TEMP2 = - 11 * SIZE, LDB
	}
	{ .mmi
	(p12) LD	f62  = [A1], SIZE
	(p12) LD	f74  = [A2], SIZE
	(p12) adds  TEMP1 = - 11 * SIZE, LDA
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f91
	(p18) ST	[B2] = f103
	(p18) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f65  = [A1], TEMP1
	(p12) LD	f77  = [A2], TEMP1
	(p18) add	B2 = B2, TEMP2
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f58,  1 * SIZE
	(p13) ST	[B2] = f70,  1 * SIZE
	adds TEMP3 = 5 * SIZE, TEMP3
	}
	{ .mmi
	(p16) LD	f80  = [A1], SIZE
	(p16) LD	f92  = [A2], SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f61,  1 * SIZE
	(p13) ST	[B2] = f73,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f83  = [A1], SIZE
	(p16) LD	f95  = [A2], SIZE
	(p14) mov  TEMP1 = TEMP3
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f64,  1 * SIZE
	(p13) ST	[B2] = f76,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f86  = [A1], SIZE
	(p16) LD	f98  = [A2], SIZE
	(p12) mov TEMP1	= 5 * SIZE
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f67,  5 * SIZE
	(p13) ST	[B2] = f79,  5 * SIZE
	(p14) mov	LCOUNT = 0
	}
	{ .mmi
	(p16) LD	f89  = [A1], TEMP1
	(p16) LD	f101 = [A2], TEMP1
	(p15) mov	SCOUNT = 0
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f106,  1 * SIZE
	(p13) ST	[B2] = f118,  1 * SIZE
	mov	TEMP2 = 5 * SIZE
	}
	{ .mmi
	(p12) LD	f104 = [A1], SIZE
	(p12) LD	f116 = [A2], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f109,  1 * SIZE
	(p13) ST	[B2] = f121,  1 * SIZE
	sub	TEMP2 = TEMP2, LDB
	}
	{ .mmi
	(p12) LD	f107 = [A1], SIZE
	(p12) LD	f119 = [A2], SIZE
	adds	TEMP1 = -11 * SIZE, LDA
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f112,  1 * SIZE
	(p13) ST	[B2] = f124,  1 * SIZE
	(p15) adds TEMP2 = -27 * SIZE, LDB
	}
	{ .mmi
	(p12) LD	f110 = [A1], SIZE
	(p12) LD	f122 = [A2], SIZE
	(p14) mov TEMP1 = TEMP3
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f115
	(p13) ST	[B2] = f127
	(p13) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f113 = [A1], TEMP1
	(p12) LD	f125 = [A2], TEMP1
	(p13) add	B2 = B2, TEMP2
	}
	;;
	{ .mmb
	(p14) adds	I = -2, I
	(p15) adds	II = -2, II
	br.ctop.sptk .L112
	}
	;;
	.align 32

.L120:
	{ .mmi
	add	A2 = A1, LDA
	nop	__LINE__
	tbit.nz p7, p0 = N, 2
	}
	;;
	{ .mmi
	(p7) LD	f32  = [A1], SIZE
	(p7) LD	f36  = [A2], SIZE
	tbit.nz p8, p0 = N, 1
	}
	;;
	{ .mmi
	(p7) LD	f33  = [A1], SIZE
	(p7) LD	f37  = [A2], SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	;;
	{ .mmi
	(p7) LD	f34  = [A1], SIZE
	(p7) LD	f38  = [A2], SIZE
	add	TEMP1 = TEMP1, LDA
	}
	;;
	{ .mmi
	(p7) LD	f35  = [A1], TEMP1
	(p7) LD	f39  = [A2], TEMP1
	tbit.nz p9, p0 = N, 0
	}
	;;
	{ .mmi
	(p7) LD	f40  = [A1], SIZE
	(p7) LD	f44  = [A2], SIZE
	mov	TEMP2 = -1 * SIZE
	}
	;;
	{ .mmi
	(p7) LD	f41  = [A1], SIZE
	(p7) LD	f45  = [A2], SIZE
	shladd	TEMP2 = LDA, 1, TEMP2
	}
	;;
	{ .mmi
	(p7) LD	f42  = [A1], SIZE
	(p7) LD	f46  = [A2], SIZE
	sub	TEMP2 = 0, TEMP2
	}
	;;
	{ .mmi
	(p7) LD	f43  = [A1], TEMP2
	(p7) LD	f47  = [A2]
	nop	__LINE__
	}
	;;
	{ .mmi
	add	A2 = A1, LDA
	adds	TEMP1 = -1 * SIZE, LDA
	mov	TEMP2 = -1 * SIZE
	}
	;;
	{ .mmi
	(p8) LD	f48  = [A1], SIZE
	(p8) LD	f50  = [A2], SIZE
	add	TEMP1 = TEMP1, LDA
	}
	;;
	{ .mmi
	(p8) LD	f49  = [A1], TEMP1
	(p8) LD	f51  = [A2], TEMP1
	shladd	TEMP2 = LDA, 1, TEMP2
	}
	;;
	{ .mmi
	(p8) LD	f52  = [A1], SIZE
	(p8) LD	f54  = [A2], SIZE
	sub	TEMP2 = r0, TEMP2
	}
	;;
	{ .mmi
	(p8) LD	f53  = [A1], TEMP2
	(p8) LD	f55  = [A2], TEMP2
	nop	__LINE__
	}
	;;
	{ .mmi
	add	A2 = A1, LDA
	adds	B2 = 4 * SIZE, BO2
	nop	__LINE__
	}
	;;
	{ .mmi
	(p9) LD	f56  = [A1]
	nop	__LINE__
	(p9) shladd	A1 = LDA, 1, A1
	}
	{ .mmi
	(p9) LD	f57  = [A2]
	nop	__LINE__
	(p9) shladd	A2 = LDA, 1, A2
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f32,  1 * SIZE
	(p7) ST	[B2 ] = f36,  1 * SIZE
	nop  __LINE__
	}
	{ .mmi
	(p9) LD	f58  = [A1]
	(p9) LD	f59  = [A2]
	nop  __LINE__
	}
	;;
	;;
	{ .mmi
	(p7) ST	[BO2] = f33,  1 * SIZE
	(p7) ST	[B2 ] = f37,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f34,  1 * SIZE
	(p7) ST	[B2 ] = f38,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f35,  5 * SIZE
	(p7) ST	[B2 ] = f39,  5 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f40,  1 * SIZE
	(p7) ST	[B2 ] = f44,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f41,  1 * SIZE
	(p7) ST	[B2 ] = f45,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f42,  1 * SIZE
	(p7) ST	[B2 ] = f46,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f43,  5 * SIZE
	(p7) ST	[B2 ] = f47
	adds	B2 = 4 * SIZE, BO3
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f48,  1 * SIZE
	(p8) ST	[B2 ] = f52,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f49,  1 * SIZE
	(p8) ST	[B2 ] = f53,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f50,  1 * SIZE
	(p8) ST	[B2 ] = f54,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f51,  5 * SIZE
	(p8) ST	[B2 ] = f55
	adds	B2 = 2 * SIZE, BO4
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f56,  1 * SIZE
	(p9) ST	[B2 ] = f58,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f57,  3 * SIZE
	(p9) ST	[B2 ] = f59
	nop  __LINE__
	}
	;;
	.align 32

.L200:
	{ .mmi
	add	I = 8, N
	mov	A1 = A
	mov	pr.rot = 0
	}
	{ .mmi
	adds	A2 = 4 * SIZE, A
	nop	__LINE__
	tbit.z p6, p0 = M, 1
	}
	;;
	{ .mmi
	mov	B1 =  B
	cmp.eq	p16, p0 = r0, r0
	mov	ar.ec  = 3
	}
	{ .mib
	adds	B2 =  4 * SIZE, B
	shr	I = I, 4
	(p6)	br.cond.dpnt .L300
	}
	;;
	{ .mmi
	shladd	A = LDA, 1, A
	adds	B  = 16 * SIZE, B
	shr	II = N, 3
	}
	{ .mmi
	cmp.eq	p8, p0 = 0, I
	adds	I = -1, I
	nop	__LINE__
	}
	;;
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	mov	ar.lc = I
	}
	{ .mib
	mov	I = II
	nop	__LINE__
	(p8)	br.cond.dpnt .L220
	}
	;;
	.align 32

.L212:
	{ .mmi
	(p18) ST	[B1] = f34,  1 * SIZE
	(p18) ST	[B2] = f46,  1 * SIZE
	(p16) cmp.ne.unc p12, p0 = 1, I
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f44  = [A2], SIZE
	(p18) cmp.ne.unc p13, p0 = 1, II
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f37,  1 * SIZE
	(p18) ST	[B2] = f49,  1 * SIZE
	adds	TEMP1 = -3 * SIZE, LDA
	}
	{ .mmi
	(p16) LD	f35  = [A1], SIZE
	(p16) LD	f47  = [A2], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f40,  1 * SIZE
	(p18) ST	[B2] = f52,  1 * SIZE
	(p12) mov TEMP1 = 5 * SIZE
	}
	{ .mmi
	(p16) LD	f38  = [A1], SIZE
	(p16) LD	f50  = [A2], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f43,  5 * SIZE
	(p18) ST	[B2] = f55,  5 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f41  = [A1], TEMP1
	(p16) LD	f53  = [A2], TEMP1
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f82,  1 * SIZE
	(p18) ST	[B2] = f94,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p12) LD	f56  = [A1], SIZE
	(p12) LD	f68  = [A2], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f85,  1 * SIZE
	(p18) ST	[B2] = f97,  1 * SIZE
	mov	TEMP2 = 5 * SIZE
	}
	{ .mmi
	(p12) LD	f59  = [A1], SIZE
	(p12) LD	f71  = [A2], SIZE
	nop   __LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f88,  1 * SIZE
	(p18) ST	[B2] = f100, 1 * SIZE
	(p13) adds TEMP2 = - 11 * SIZE, LDB
	}
	{ .mmi
	(p12) LD	f62  = [A1], SIZE
	(p12) LD	f74  = [A2], SIZE
	(p12) adds  TEMP1 = - 11 * SIZE, LDA
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f91
	(p18) ST	[B2] = f103
	(p18) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f65  = [A1], TEMP1
	(p12) LD	f77  = [A2], TEMP1
	(p18) add	B2 = B2, TEMP2
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f58,  1 * SIZE
	(p13) ST	[B2] = f70,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f80  = [A1], SIZE
	(p16) LD	f92  = [A2], SIZE
	sub TEMP1 = r0, LDA
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f61,  1 * SIZE
	(p13) ST	[B2] = f73,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f83  = [A1], SIZE
	(p16) LD	f95  = [A2], SIZE
	(p16) adds TEMP1 = 5 * SIZE, TEMP1
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f64,  1 * SIZE
	(p13) ST	[B2] = f76,  1 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f86  = [A1], SIZE
	(p16) LD	f98  = [A2], SIZE
	(p12) mov TEMP1	= 5 * SIZE
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f67,  5 * SIZE
	(p13) ST	[B2] = f79,  5 * SIZE
	nop   __LINE__
	}
	{ .mmi
	(p16) LD	f89  = [A1], TEMP1
	(p16) LD	f101 = [A2], TEMP1
	adds	TEMP1 = -11 * SIZE, LDA
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f106,  1 * SIZE
	(p13) ST	[B2] = f118,  1 * SIZE
	mov	TEMP2 = 5 * SIZE
	}
	{ .mmi
	(p12) LD	f104 = [A1], SIZE
	(p12) LD	f116 = [A2], SIZE
	(p16) shladd TEMP1 = LDA, 1, r0
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f109,  1 * SIZE
	(p13) ST	[B2] = f121,  1 * SIZE
	sub	TEMP2 = TEMP2, LDB
	}
	{ .mmi
	(p12) LD	f107 = [A1], SIZE
	(p12) LD	f119 = [A2], SIZE
	(p16) sub TEMP1 = LDA, TEMP1
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f112,  1 * SIZE
	(p13) ST	[B2] = f124,  1 * SIZE
	(p18) adds TEMP2 = -11 * SIZE, LDB
	}
	{ .mmi
	(p12) LD	f110 = [A1], SIZE
	(p12) LD	f122 = [A2], SIZE
	(p16) adds TEMP1 = 5 * SIZE, TEMP1
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f115
	(p13) ST	[B2] = f127
	(p13) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f113 = [A1], TEMP1
	(p12) LD	f125 = [A2], TEMP1
	(p13) add	B2 = B2, TEMP2
	}
	;;
	{ .mmb
	(p16) adds	I = -2, I
	(p18) adds	II = -2, II
	br.ctop.sptk .L212
	}
	;;
	.align 32

.L220:
	{ .mmi
	add	A2 = A1, LDA
	nop  __LINE__
	tbit.nz p7, p0 = N, 2
	}
	;;
	{ .mmi
	(p7) LD	f32  = [A1], SIZE
	(p7) LD	f36  = [A2], SIZE
	tbit.nz p8, p0 = N, 1
	}
	;;
	{ .mmi
	(p7) LD	f33  = [A1], SIZE
	(p7) LD	f37  = [A2], SIZE
	tbit.nz p9, p0 = N, 0
	}
	;;
	{ .mmi
	(p7) LD	f34  = [A1], SIZE
	(p7) LD	f38  = [A2], SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) LD	f35  = [A1], SIZE
	(p7) LD	f39  = [A2]
	nop  __LINE__
	}
	;;
	{ .mmi
	add	A2 = A1, LDA
	nop  __LINE__
	nop  __LINE__
	}
	;;
	{ .mmi
	(p8) LD	f40  = [A1], SIZE
	(p8) LD	f42  = [A2], SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p8) LD	f41  = [A1], SIZE
	(p8) LD	f43  = [A2]
	nop  __LINE__
	}
	;;
	{ .mmi
	add	A2 = A1, LDA
	nop  __LINE__
	nop  __LINE__
	}
	;;
	{ .mmi
	(p9) LD	f44  = [A1]
	(p9) LD	f45  = [A2]
	adds	B2 = 4 * SIZE, BO2
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f32,  1 * SIZE
	(p7) ST	[B2 ] = f36,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f33,  1 * SIZE
	(p7) ST	[B2 ] = f37,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f34,  1 * SIZE
	(p7) ST	[B2 ] = f38,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f35,  5 * SIZE
	(p7) ST	[B2 ] = f39
	adds	B2 = 2 * SIZE, BO3
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f40,  1 * SIZE
	(p8) ST	[B2 ] = f42,  1 * SIZE
	nop  __LINE__
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f41,  3 * SIZE
	(p8) ST	[B2 ] = f43
	adds	B2 = 1 * SIZE, BO4
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f44,  2 * SIZE
	(p9) ST	[B2 ] = f45
	nop  __LINE__
	}
	;;
	.align 32

.L300:
	{ .mmi
	add	I = 8, N
	mov	A1 = A
	mov	pr.rot = 0
	}
	{ .mmi
	mov	B1 =  B
	adds	A2 = 4 * SIZE, A
	tbit.z p6, p0 = M, 0
	}
	;;
	{ .mmi
	adds	B2 =  4 * SIZE, B
	cmp.eq	p16, p0 = r0, r0
	mov	ar.ec  = 3
	}
	{ .mib
	nop	__LINE__
	shr	I = I, 4
	(p6)	br.cond.dpnt .L999
	}
	;;
	{ .mmi
	cmp.eq	p8, p0 = 0, I
	adds	I = -1, I
	shr	II = N, 3
	}
	;;
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	mov	ar.lc = I
	}
	{ .mib
	nop	__LINE__
	mov	I = II
	(p8)	br.cond.dpnt .L320
	}
	;;
	.align 32

.L312:
	{ .mmi
	(p18) ST	[B1] = f34,  1 * SIZE
	(p18) ST	[B2] = f46,  1 * SIZE
	(p16) cmp.ne.unc p12, p0 = 1, I
	}
	{ .mmi
	(p16) LD	f32  = [A1], SIZE
	(p16) LD	f44  = [A2], SIZE
	(p18) cmp.ne.unc p13, p0 = 1, II
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f37,  1 * SIZE
	(p18) ST	[B2] = f49,  1 * SIZE
	adds	TEMP2 = - 3 * SIZE, LDB
	}
	{ .mmi
	(p16) LD	f35  = [A1], SIZE
	(p16) LD	f47  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f40,  1 * SIZE
	(p18) ST	[B2] = f52,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p16) LD	f38  = [A1], SIZE
	(p16) LD	f50  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p18) ST	[B1] = f43
	(p18) ST	[B2] = f55
	(p18) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p16) LD	f41  = [A1], 5 * SIZE
	(p16) LD	f53  = [A2], 5 * SIZE
	(p18) add	B2 = B2, TEMP2
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f58,  1 * SIZE
	(p13) ST	[B2] = f70,  1 * SIZE
	(p16) adds	I = -2, I
	}
	{ .mmi
	(p12) LD	f56  = [A1], SIZE
	(p12) LD	f68  = [A2], SIZE
	(p18) adds	II = -2, II
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f61,  1 * SIZE
	(p13) ST	[B2] = f73,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p12) LD	f59  = [A1], SIZE
	(p12) LD	f71  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f64,  1 * SIZE
	(p13) ST	[B2] = f76,  1 * SIZE
	nop	__LINE__
	}
	{ .mmi
	(p12) LD	f62  = [A1], SIZE
	(p12) LD	f74  = [A2], SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p13) ST	[B1] = f67
	(p13) ST	[B2] = f79
	(p13) add	B1 = B1, TEMP2
	}
	{ .mmi
	(p12) LD	f65  = [A1], 5 * SIZE
	(p12) LD	f77  = [A2], 5 * SIZE
	(p13) add	B2 = B2, TEMP2
	}
	;;
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	br.ctop.sptk .L312
	}
	;;
	.align 32

.L320:
	{ .mmi
	adds	A2 = 2 * SIZE, A1
	adds	B2 = 2 * SIZE, BO2
	tbit.nz p7, p0 = N, 2
	}
	;;
	{ .mmi
	(p7) LD	f32  = [A1], SIZE
	(p7) LD	f34  = [A2], SIZE
	tbit.nz p8, p0 = N, 1
	}
	;;
	{ .mmi
	(p7) LD	f33  = [A1], 3 * SIZE
	(p7) LD	f35  = [A2]
	nop  __LINE__
	}
	;;
	{ .mmi
	adds	A2 = SIZE, A1
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p8) LD	f36  = [A1], 2 * SIZE
	(p8) LD	f37  = [A2]
	tbit.nz p9, p0 = N, 0
	}
	;;
	{ .mmi
	(p9) LD	f38  = [A1]
	nop	__LINE__
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f32,  1 * SIZE
	(p7) ST	[B2 ] = f34,  1 * SIZE
	nop	__LINE__
	}
	;;
	{ .mmi
	(p7) ST	[BO2] = f33,  3 * SIZE
	(p7) ST	[B2 ] = f35
	adds	B2 = SIZE, BO3
	}
	;;
	{ .mmi
	(p8) ST	[BO3] = f36,  2 * SIZE
	(p8) ST	[B2 ] = f37
	nop	__LINE__
	}
	;;
	{ .mmi
	(p9) ST	[BO4] = f38,  1 * SIZE
	nop	__LINE__
	nop	__LINE__
	}
	;;
	.align 32

.L999:
	mov pr    = PR, -1
	mov	 ar.lc = ARLC
	br.ret.sptk.many b0
	EPILOGUE
