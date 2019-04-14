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

#define M	%i0
#define N	%i1
#define K	%i2

#if defined(DOUBLE) && !defined(__64BIT__)
#define A	%i5
#define B	%i4
#else
#define A	%i4
#define B	%i5
#endif

#define C	%o4
#define LDC	%o5

#define AO	%l0
#define BO	%l1
#define I	%l2
#define J	%l3
#define L	%l4

#define C1	%o0
#define C2	%o1
#define C3	%o2
#define C4	%o3

#define OFFSET	%l5
#define	KK	%l6
#define TEMP1	%l7
#define TEMP2	%i3
#define AORIG	%g1

#ifdef DOUBLE
#define c01	%f0
#define c02	%f2
#define c03	%f4
#define c04	%f6
#define c05	%f8
#define c06	%f10
#define c07	%f12
#define c08	%f14
#define c09	%f16
#define c10	%f18
#define c11	%f20
#define c12	%f22
#define c13	%f24
#define c14	%f26
#define c15	%f28
#define c16	%f30

#define t1	%f32
#define	t2 	%f34
#define t3	%f36
#define	t4 	%f38

#define a1	%f40
#define a2	%f42
#define a3	%f44
#define a4	%f46
#define a5	%f58

#define b1	%f48
#define b2	%f50
#define b3	%f52
#define b4	%f54
#define b5	%f56

#define FZERO	%f60
#else
#define c01	%f0
#define c02	%f1
#define c03	%f2
#define c04	%f3
#define c05	%f4
#define c06	%f5
#define c07	%f6
#define c08	%f7
#define c09	%f8
#define c10	%f9
#define c11	%f10
#define c12	%f11
#define c13	%f12
#define c14	%f13
#define c15	%f14
#define c16	%f15

#define t1	%f16
#define	t2 	%f17
#define t3	%f18
#define	t4 	%f19

#define a1	%f20
#define a2	%f21
#define a3	%f22
#define a4	%f23
#define a5	%f31

#define b1	%f24
#define b2	%f25
#define b3	%f26
#define b4	%f27
#define b5	%f28

#define FZERO	%f29
#endif

	PROLOGUE
	SAVESP
	nop

#ifndef __64BIT__
#ifdef DOUBLE
	ld	[%sp + STACK_START + 28], B
	ld	[%sp + STACK_START + 32], C
	ld	[%sp + STACK_START + 36], LDC
	ld	[%sp + STACK_START + 40], OFFSET
#else
	ld	[%sp + STACK_START + 28], C
	ld	[%sp + STACK_START + 32], LDC
	ld	[%sp + STACK_START + 36], OFFSET
#endif
#else
	ldx	[%sp+  STACK_START + 56], C
	ldx	[%sp+  STACK_START + 64], LDC
	ldx	[%sp+  STACK_START + 72], OFFSET
#endif

       FCLR(29)

	sll	LDC, BASE_SHIFT, LDC

#ifdef LN
	smul	M, K, TEMP1
	sll	TEMP1, BASE_SHIFT, TEMP1
	add	A, TEMP1, A

	sll	M, BASE_SHIFT, TEMP1
	add	C, TEMP1, C
#endif

#ifdef RN
	neg	OFFSET, KK
#endif

#ifdef RT
	smul	N, K, TEMP1
	sll	TEMP1, BASE_SHIFT, TEMP1
	add	B, TEMP1, B

	smul	N, LDC, TEMP1
	add	C, TEMP1, C

	sub	N, OFFSET, KK
#endif

	sra	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL100
	nop

.LL11:
#ifdef RT
	sll	K, 2 + BASE_SHIFT, TEMP1
	sub	B, TEMP1, B

	sll	LDC, 2, TEMP1
	sub	C, TEMP1, C
#endif

	add	C, LDC, C2
	FMOV	FZERO, t1
	nop
	mov	C, C1

	add	C2, LDC, C3
	FMOV	FZERO, t2
	sra	M, 2, I
	add	C3, LDC, C4
	FMOV	FZERO, t3

#ifdef LN
	add	M, OFFSET, KK
#endif

#ifdef LT
	mov	OFFSET, KK
#endif

#if defined(LN) || defined(RT)
	mov	A, AORIG
#else
	mov	A, AO
#endif

	cmp	I, 0
#ifndef RT
	add	C4, LDC, C
#endif
	FMOV	FZERO, t4

	ble,pn	%icc, .LL50
	FMOV	FZERO, c01

.LL21:
	FMOV	FZERO, c02
	FMOV	FZERO, c03

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  2 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 2 + BASE_SHIFT, TEMP1

	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO

	sub	K, KK, TEMP1

	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c04
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, c05
	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c06
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, c07

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c08
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, c09
	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c10
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, c11
	LDF	[BO +  4 * SIZE], b5	/* ***** */

	LDF	[AO +  4 * SIZE], a5	/* ***** */

	prefetch [C1 + 3 * SIZE], 3
	FMOV	FZERO, c12
	prefetch [C2 + 3 * SIZE], 3
	FMOV	FZERO, c13
	prefetch [C3 + 3 * SIZE], 3
	FMOV	FZERO, c14
	prefetch [C4 + 3 * SIZE], 3
	FMOV	FZERO, c15

	ble,pn	%icc, .LL25
	FMOV	FZERO, c16

#define APREFETCHSIZE 40
#define BPREFETCHSIZE 40

#define APREFETCH_CATEGORY 0
#define BPREFETCH_CATEGORY 0

.LL22:
	FADD	c04, t1, c04
	prefetch [AO + APREFETCHSIZE * SIZE], APREFETCH_CATEGORY
	FMUL	a1, b1, t1
	nop

	FADD	c08, t2, c08
	prefetch [BO + BPREFETCHSIZE * SIZE], BPREFETCH_CATEGORY
	FMUL	a1, b2, t2
	add	AO, 16 * SIZE, AO

	FADD	c12, t3, c12
	LDF	[AO - 13 * SIZE], a4
	FMUL	a1, b3, t3
	add	BO, 16 * SIZE, BO

	FADD	c16, t4, c16
	nop
	FMUL	a1, b4, t4
	LDF	[AO -  8 * SIZE], a1

	FADD	c01, t1, c01
	nop
	FMUL	a2, b1, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c13, t4, c13
	add	L, -1, L
	FMUL	a2, b4, t4
	LDF	[AO - 11 * SIZE], a2

	FADD	c02, t1, c02
	nop
	FMUL	a3, b1, t1
	nop

	FADD	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO - 10 * SIZE], a3

	FADD	c03, t1, c03
	nop
	FMUL	a4, b1, t1
	LDF	[BO -  8 * SIZE], b1

	FADD	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO - 11 * SIZE], b2

	FADD	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO - 10 * SIZE], b3

	FADD	c15, t4, c15
	nop
	FMUL	a4, b4, t4
	LDF	[BO -  9 * SIZE], b4

	FADD	c04, t1, c04
	nop
	FMUL	a5, b5, t1
	LDF	[AO -  9 * SIZE], a4

	FADD	c08, t2, c08
	nop
	FMUL	a5, b2, t2
	nop

	FADD	c12, t3, c12
	nop
	FMUL	a5, b3, t3
	nop

	FADD	c16, t4, c16
	nop
	FMUL	a5, b4, t4
	LDF	[AO - 4 * SIZE], a5

	FADD	c01, t1, c01
	nop
	FMUL	a2, b5, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	LDF	[AO -  7 * SIZE], a2

	FADD	c02, t1, c02
	nop
	FMUL	a3, b5, t1
	nop

	FADD	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO -  6 * SIZE], a3

	FADD	c03, t1, c03
	nop
	FMUL	a4, b5, t1
	LDF	[BO - 4 * SIZE], b5

	FADD	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO -  7 * SIZE], b2

	FADD	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO -  6 * SIZE], b3

	FADD	c15, t4, c15
	nop
	FMUL	a4, b4, t4
	LDF	[BO -  5 * SIZE], b4

	FADD	c04, t1, c04
	nop
	FMUL	a1, b1, t1
	LDF	[AO -  5 * SIZE], a4

	FADD	c08, t2, c08
	nop
	FMUL	a1, b2, t2
	nop

	FADD	c12, t3, c12
	nop
	FMUL	a1, b3, t3
	nop

	FADD	c16, t4, c16
	nop
	FMUL	a1, b4, t4
	LDF	[AO -  0 * SIZE], a1

	FADD	c01, t1, c01
	nop
	FMUL	a2, b1, t1
	nop

#ifdef DOUBLE
	prefetch [AO + (APREFETCHSIZE + 8) * SIZE], APREFETCH_CATEGORY
#else
	nop
#endif
	FADD	c05, t2, c05
	nop
	FMUL	a2, b2, t2

	FADD	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	nop

	FADD	c02, t1, c02
	nop
	FMUL	a3, b1, t1
	LDF	[AO - 3 * SIZE], a2

	FADD	c06, t2, c06
#ifdef DOUBLE
	prefetch [BO + (BPREFETCHSIZE + 8) * SIZE], BPREFETCH_CATEGORY
#else
	nop
#endif
	FMUL	a3, b2, t2
	nop

	FADD	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO - 2 * SIZE], a3

	FADD	c03, t1, c03
	nop
	FMUL	a4, b1, t1
	LDF	[BO -  0 * SIZE], b1

	FADD	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO - 3 * SIZE], b2

	FADD	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO - 2 * SIZE], b3

	FADD	c15, t4, c15
	nop
	FMUL	a4, b4, t4
	LDF	[BO - 1 * SIZE], b4

	FADD	c04, t1, c04
	nop
	FMUL	a5, b5, t1
	LDF	[AO - 1 * SIZE], a4

	FADD	c08, t2, c08
	FMUL	a5, b2, t2
	FADD	c12, t3, c12
	FMUL	a5, b3, t3

	FADD	c16, t4, c16
	nop
	FMUL	a5, b4, t4
	LDF	[AO +  4 * SIZE], a5

	FADD	c01, t1, c01
	nop
	FMUL	a2, b5, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	LDF	[AO +  1 * SIZE], a2

	FADD	c02, t1, c02
	nop
	FMUL	a3, b5, t1
	nop

	FADD	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO +  2 * SIZE], a3

	FADD	c03, t1, c03
	cmp	L, 0
	FMUL	a4, b5, t1
	LDF	[BO +  4 * SIZE], b5

	FADD	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO +  1 * SIZE], b2

	FADD	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3

	FADD	c15, t4, c15
	FMUL	a4, b4, t4
	bg,pt	%icc, .LL22
	LDF	[BO +  3 * SIZE], b4

.LL25:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL29
	nop

.LL26:
	FADD	c04, t1, c04
	LDF	[AO +  3 * SIZE], a4
	FMUL	a1, b1, t1
	add	AO, 4 * SIZE, AO

	FADD	c08, t2, c08
	add	BO, 4 * SIZE, BO
	FMUL	a1, b2, t2
	add	L, -1, L

	FADD	c12, t3, c12
	nop
	FMUL	a1, b3, t3
	cmp	L, 0

	FADD	c16, t4, c16
	nop
	FMUL	a1, b4, t4
	LDF	[AO + 0 * SIZE], a1

	FADD	c01, t1, c01
	nop
	FMUL	a2, b1, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	LDF	[AO + 1 * SIZE], a2

	FADD	c02, t1, c02
	nop
	FMUL	a3, b1, t1
	nop

	FADD	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO + 2 * SIZE], a3

	FADD	c03, t1, c03
	nop
	FMUL	a4, b1, t1
	LDF	[BO + 0 * SIZE], b1

	FADD	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO + 1 * SIZE], b2

	FADD	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO + 2 * SIZE], b3

	FADD	c15, t4, c15
	FMUL	a4, b4, t4
	bg,pt	%icc, .LL26
	LDF	[BO + 3 * SIZE], b4

.LL29:
#if defined(LN) || defined(RT)
	sub	KK, 4, TEMP1
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO
#endif

	FADD	c04, t1, c04
	FADD	c08, t2, c08
	FADD	c12, t3, c12
	FADD	c16, t4, c16

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	LDF	[BO +  4 * SIZE], b1
	LDF	[BO +  5 * SIZE], b2
	LDF	[BO +  6 * SIZE], b3
	LDF	[BO +  7 * SIZE], b4

	FSUB	a1, c01, c01
	FSUB	a2, c05, c05
	FSUB	a3, c09, c09
	FSUB	a4, c13, c13

	FSUB	b1, c02, c02
	FSUB	b2, c06, c06
	FSUB	b3, c10, c10
	FSUB	b4, c14, c14

	LDF	[BO +  8 * SIZE], a1
	LDF	[BO +  9 * SIZE], a2
	LDF	[BO + 10 * SIZE], a3
	LDF	[BO + 11 * SIZE], a4

	LDF	[BO + 12 * SIZE], b1
	LDF	[BO + 13 * SIZE], b2
	LDF	[BO + 14 * SIZE], b3
	LDF	[BO + 15 * SIZE], b4

	FSUB	a1, c03, c03
	FSUB	a2, c07, c07
	FSUB	a3, c11, c11
	FSUB	a4, c15, c15

	FSUB	b1, c04, c04
	FSUB	b2, c08, c08
	FSUB	b3, c12, c12
	FSUB	b4, c16, c16
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	LDF	[AO +  4 * SIZE], b1
	LDF	[AO +  5 * SIZE], b2
	LDF	[AO +  6 * SIZE], b3
	LDF	[AO +  7 * SIZE], b4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04

	FSUB	b1, c05, c05
	FSUB	b2, c06, c06
	FSUB	b3, c07, c07
	FSUB	b4, c08, c08

	LDF	[AO +  8 * SIZE], a1
	LDF	[AO +  9 * SIZE], a2
	LDF	[AO + 10 * SIZE], a3
	LDF	[AO + 11 * SIZE], a4

	LDF	[AO + 12 * SIZE], b1
	LDF	[AO + 13 * SIZE], b2
	LDF	[AO + 14 * SIZE], b3
	LDF	[AO + 15 * SIZE], b4

	FSUB	a1, c09, c09
	FSUB	a2, c10, c10
	FSUB	a3, c11, c11
	FSUB	a4, c12, c12

	FSUB	b1, c13, c13
	FSUB	b2, c14, c14
	FSUB	b3, c15, c15
	FSUB	b4, c16, c16
#endif

#ifdef LN
	LDF	[AO + 15 * SIZE], a1
	LDF	[AO + 14 * SIZE], a2
	LDF	[AO + 13 * SIZE], a3
	LDF	[AO + 12 * SIZE], a4

	FMUL	a1, c04, c04
	FMUL	a1, c08, c08
	FMUL	a1, c12, c12
	FMUL	a1, c16, c16

	FMUL	a2, c04, t1
	FMUL	a2, c08, t2
	FMUL	a2, c12, t3
	FMUL	a2, c16, t4

	FSUB	c03, t1, c03
	FSUB	c07, t2, c07
	FSUB	c11, t3, c11
	FSUB	c15, t4, c15

	FMUL	a3, c04, t1
	FMUL	a3, c08, t2
	FMUL	a3, c12, t3
	FMUL	a3, c16, t4

	FSUB	c02, t1, c02
	FSUB	c06, t2, c06
	FSUB	c10, t3, c10
	FSUB	c14, t4, c14

	FMUL	a4, c04, t1
	FMUL	a4, c08, t2
	FMUL	a4, c12, t3
	FMUL	a4, c16, t4

	FSUB	c01, t1, c01
	FSUB	c05, t2, c05
	FSUB	c09, t3, c09
	FSUB	c13, t4, c13

	LDF	[AO + 10 * SIZE], a1
	LDF	[AO +  9 * SIZE], a2
	LDF	[AO +  8 * SIZE], a3

	FMUL	a1, c03, c03
	FMUL	a1, c07, c07
	FMUL	a1, c11, c11
	FMUL	a1, c15, c15

	FMUL	a2, c03, t1
	FMUL	a2, c07, t2
	FMUL	a2, c11, t3
	FMUL	a2, c15, t4

	FSUB	c02, t1, c02
	FSUB	c06, t2, c06
	FSUB	c10, t3, c10
	FSUB	c14, t4, c14

	FMUL	a3, c03, t1
	FMUL	a3, c07, t2
	FMUL	a3, c11, t3
	FMUL	a3, c15, t4

	FSUB	c01, t1, c01
	FSUB	c05, t2, c05
	FSUB	c09, t3, c09
	FSUB	c13, t4, c13

	LDF	[AO +  5 * SIZE], a1
	LDF	[AO +  4 * SIZE], a2

	FMUL	a1, c02, c02
	FMUL	a1, c06, c06
	FMUL	a1, c10, c10
	FMUL	a1, c14, c14

	FMUL	a2, c02, t1
	FMUL	a2, c06, t2
	FMUL	a2, c10, t3
	FMUL	a2, c14, t4

	FSUB	c01, t1, c01
	FSUB	c05, t2, c05
	FSUB	c09, t3, c09
	FSUB	c13, t4, c13

	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c05, c05
	FMUL	a1, c09, c09
	FMUL	a1, c13, c13
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FMUL	a1, c01, c01
	FMUL	a1, c05, c05
	FMUL	a1, c09, c09
	FMUL	a1, c13, c13

	FMUL	a2, c01, t1
	FMUL	a2, c05, t2
	FMUL	a2, c09, t3
	FMUL	a2, c13, t4

	FSUB	c02, t1, c02
	FSUB	c06, t2, c06
	FSUB	c10, t3, c10
	FSUB	c14, t4, c14

	FMUL	a3, c01, t1
	FMUL	a3, c05, t2
	FMUL	a3, c09, t3
	FMUL	a3, c13, t4

	FSUB	c03, t1, c03
	FSUB	c07, t2, c07
	FSUB	c11, t3, c11
	FSUB	c15, t4, c15

	FMUL	a4, c01, t1
	FMUL	a4, c05, t2
	FMUL	a4, c09, t3
	FMUL	a4, c13, t4

	FSUB	c04, t1, c04
	FSUB	c08, t2, c08
	FSUB	c12, t3, c12
	FSUB	c16, t4, c16

	LDF	[AO +  5 * SIZE], a1
	LDF	[AO +  6 * SIZE], a2
	LDF	[AO +  7 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a1, c06, c06
	FMUL	a1, c10, c10
	FMUL	a1, c14, c14

	FMUL	a2, c02, t1
	FMUL	a2, c06, t2
	FMUL	a2, c10, t3
	FMUL	a2, c14, t4

	FSUB	c03, t1, c03
	FSUB	c07, t2, c07
	FSUB	c11, t3, c11
	FSUB	c15, t4, c15

	FMUL	a3, c02, t1
	FMUL	a3, c06, t2
	FMUL	a3, c10, t3
	FMUL	a3, c14, t4

	FSUB	c04, t1, c04
	FSUB	c08, t2, c08
	FSUB	c12, t3, c12
	FSUB	c16, t4, c16

	LDF	[AO + 10 * SIZE], a1
	LDF	[AO + 11 * SIZE], a2

	FMUL	a1, c03, c03
	FMUL	a1, c07, c07
	FMUL	a1, c11, c11
	FMUL	a1, c15, c15

	FMUL	a2, c03, t1
	FMUL	a2, c07, t2
	FMUL	a2, c11, t3
	FMUL	a2, c15, t4

	FSUB	c04, t1, c04
	FSUB	c08, t2, c08
	FSUB	c12, t3, c12
	FSUB	c16, t4, c16

	LDF	[AO + 15 * SIZE], a1

	FMUL	a1, c04, c04
	FMUL	a1, c08, c08
	FMUL	a1, c12, c12
	FMUL	a1, c16, c16
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04

	FMUL	a2, c01, t1
	FMUL	a2, c02, t2
	FMUL	a2, c03, t3
	FMUL	a2, c04, t4

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06
	FSUB	c07, t3, c07
	FSUB	c08, t4, c08

	FMUL	a3, c01, t1
	FMUL	a3, c02, t2
	FMUL	a3, c03, t3
	FMUL	a3, c04, t4

	FSUB	c09, t1, c09
	FSUB	c10, t2, c10
	FSUB	c11, t3, c11
	FSUB	c12, t4, c12

	FMUL	a4, c01, t1
	FMUL	a4, c02, t2
	FMUL	a4, c03, t3
	FMUL	a4, c04, t4

	FSUB	c13, t1, c13
	FSUB	c14, t2, c14
	FSUB	c15, t3, c15
	FSUB	c16, t4, c16

	LDF	[BO +  5 * SIZE], a1
	LDF	[BO +  6 * SIZE], a2
	LDF	[BO +  7 * SIZE], a3

	FMUL	a1, c05, c05
	FMUL	a1, c06, c06
	FMUL	a1, c07, c07
	FMUL	a1, c08, c08

	FMUL	a2, c05, t1
	FMUL	a2, c06, t2
	FMUL	a2, c07, t3
	FMUL	a2, c08, t4

	FSUB	c09, t1, c09
	FSUB	c10, t2, c10
	FSUB	c11, t3, c11
	FSUB	c12, t4, c12

	FMUL	a3, c05, t1
	FMUL	a3, c06, t2
	FMUL	a3, c07, t3
	FMUL	a3, c08, t4

	FSUB	c13, t1, c13
	FSUB	c14, t2, c14
	FSUB	c15, t3, c15
	FSUB	c16, t4, c16

	LDF	[BO + 10 * SIZE], a1
	LDF	[BO + 11 * SIZE], a2

	FMUL	a1, c09, c09
	FMUL	a1, c10, c10
	FMUL	a1, c11, c11
	FMUL	a1, c12, c12

	FMUL	a2, c09, t1
	FMUL	a2, c10, t2
	FMUL	a2, c11, t3
	FMUL	a2, c12, t4

	FSUB	c13, t1, c13
	FSUB	c14, t2, c14
	FSUB	c15, t3, c15
	FSUB	c16, t4, c16

	LDF	[BO + 15 * SIZE], a1

	FMUL	a1, c13, c13
	FMUL	a1, c14, c14
	FMUL	a1, c15, c15
	FMUL	a1, c16, c16
#endif

#ifdef RT
	LDF	[BO + 15 * SIZE], a1
	LDF	[BO + 14 * SIZE], a2
	LDF	[BO + 13 * SIZE], a3
	LDF	[BO + 12 * SIZE], a4

	FMUL	a1, c13, c13
	FMUL	a1, c14, c14
	FMUL	a1, c15, c15
	FMUL	a1, c16, c16

	FMUL	a2, c13, t1
	FMUL	a2, c14, t2
	FMUL	a2, c15, t3
	FMUL	a2, c16, t4

	FSUB	c09, t1, c09
	FSUB	c10, t2, c10
	FSUB	c11, t3, c11
	FSUB	c12, t4, c12

	FMUL	a3, c13, t1
	FMUL	a3, c14, t2
	FMUL	a3, c15, t3
	FMUL	a3, c16, t4

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06
	FSUB	c07, t3, c07
	FSUB	c08, t4, c08

	FMUL	a4, c13, t1
	FMUL	a4, c14, t2
	FMUL	a4, c15, t3
	FMUL	a4, c16, t4

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FSUB	c03, t3, c03
	FSUB	c04, t4, c04

	LDF	[BO + 10 * SIZE], a1
	LDF	[BO +  9 * SIZE], a2
	LDF	[BO +  8 * SIZE], a3

	FMUL	a1, c09, c09
	FMUL	a1, c10, c10
	FMUL	a1, c11, c11
	FMUL	a1, c12, c12

	FMUL	a2, c09, t1
	FMUL	a2, c10, t2
	FMUL	a2, c11, t3
	FMUL	a2, c12, t4

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06
	FSUB	c07, t3, c07
	FSUB	c08, t4, c08

	FMUL	a3, c09, t1
	FMUL	a3, c10, t2
	FMUL	a3, c11, t3
	FMUL	a3, c12, t4

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FSUB	c03, t3, c03
	FSUB	c04, t4, c04

	LDF	[BO +  5 * SIZE], a1
	LDF	[BO +  4 * SIZE], a2

	FMUL	a1, c05, c05
	FMUL	a1, c06, c06
	FMUL	a1, c07, c07
	FMUL	a1, c08, c08

	FMUL	a2, c05, t1
	FMUL	a2, c06, t2
	FMUL	a2, c07, t3
	FMUL	a2, c08, t4

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FSUB	c03, t3, c03
	FSUB	c04, t4, c04

	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04
#endif

#ifdef LN
	add	C1, -4 * SIZE, C1
	add	C2, -4 * SIZE, C2
	add	C3, -4 * SIZE, C3
	add	C4, -4 * SIZE, C4
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c05, [BO +  1 * SIZE]
	STF	c09, [BO +  2 * SIZE]
	STF	c13, [BO +  3 * SIZE]

	STF	c02, [BO +  4 * SIZE]
	STF	c06, [BO +  5 * SIZE]
	STF	c10, [BO +  6 * SIZE]
	STF	c14, [BO +  7 * SIZE]

	STF	c03, [BO +  8 * SIZE]
	STF	c07, [BO +  9 * SIZE]
	STF	c11, [BO + 10 * SIZE]
	STF	c15, [BO + 11 * SIZE]

	STF	c04, [BO + 12 * SIZE]
	STF	c08, [BO + 13 * SIZE]
	STF	c12, [BO + 14 * SIZE]
	STF	c16, [BO + 15 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c03, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]

	STF	c05, [AO +  4 * SIZE]
	STF	c06, [AO +  5 * SIZE]
	STF	c07, [AO +  6 * SIZE]
	STF	c08, [AO +  7 * SIZE]

	STF	c09, [AO +  8 * SIZE]
	STF	c10, [AO +  9 * SIZE]
	STF	c11, [AO + 10 * SIZE]
	STF	c12, [AO + 11 * SIZE]

	STF	c13, [AO + 12 * SIZE]
	STF	c14, [AO + 13 * SIZE]
	STF	c15, [AO + 14 * SIZE]
	STF	c16, [AO + 15 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]

	STF	c05, [C2 + 0 * SIZE]
	STF	c06, [C2 + 1 * SIZE]
	STF	c07, [C2 + 2 * SIZE]
	STF	c08, [C2 + 3 * SIZE]

	STF	c09, [C3 + 0 * SIZE]
	STF	c10, [C3 + 1 * SIZE]
	STF	c11, [C3 + 2 * SIZE]
	STF	c12, [C3 + 3 * SIZE]

	STF	c13, [C4 + 0 * SIZE]
	STF	c14, [C4 + 1 * SIZE]
	STF	c15, [C4 + 2 * SIZE]
	STF	c16, [C4 + 3 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 4 * SIZE, C1
	add	C2, 4 * SIZE, C2
	add	C3, 4 * SIZE, C3
	add	C4, 4 * SIZE, C4
#endif

#ifdef RT
	sll	K, 2 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1
	add	AO, TEMP1, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 4, KK
#endif

#ifdef LN
	sub	KK, 4, KK
#endif

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL21
	FMOV	FZERO, c01

.LL50:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL70
	nop

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  1 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 1 + BASE_SHIFT, TEMP1
	sll	KK, 2 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	FMOV	FZERO, c02
	FMOV	FZERO, t1
	FMOV	FZERO, c04

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, t2
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, c06
	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, t3
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, c08
	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, t4
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, c01
	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c03
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, c05

	ble,pn	%icc, .LL55
	FMOV	FZERO, c07

.LL52:
	FADD	c02, t1, c02
	add	AO,  8 * SIZE, AO
	prefetch [AO + APREFETCHSIZE * SIZE], 0

	FMUL	a1, b1, t1
	add	BO, 16 * SIZE, BO

	FADD	c04, t2, c04
	add	L, -1, L
	FMUL	a1, b2, t2

	FADD	c06, t3, c06
	cmp	L, 0
	FMUL	a1, b3, t3

	FADD	c08, t4, c08
	FMUL	a1, b4, t4
	LDF	[AO -  4 * SIZE], a1

	FADD	c01, t1, c01
	FMUL	a2, b1, t1
	LDF	[BO - 12 * SIZE], b1
	FADD	c03, t2, c03
	FMUL	a2, b2, t2
	LDF	[BO - 11 * SIZE], b2

	FADD	c05, t3, c05
	FMUL	a2, b3, t3
	LDF	[BO - 10 * SIZE], b3
	FADD	c07, t4, c07
	FMUL	a2, b4, t4
	LDF	[BO -  9 * SIZE], b4

	FADD	c02, t1, c02
	FMUL	a3, b1, t1
	LDF	[AO -  3 * SIZE], a2
	FADD	c04, t2, c04
	FMUL	a3, b2, t2

	FADD	c06, t3, c06
	FMUL	a3, b3, t3
	FADD	c08, t4, c08
	FMUL	a3, b4, t4
	LDF	[AO -  2 * SIZE], a3

	FADD	c01, t1, c01
	FMUL	a4, b1, t1
	LDF	[BO -  8 * SIZE], b1
	FADD	c03, t2, c03
	FMUL	a4, b2, t2
	LDF	[BO -  7 * SIZE], b2

	FADD	c05, t3, c05
	FMUL	a4, b3, t3
	LDF	[BO -  6 * SIZE], b3
	FADD	c07, t4, c07
	FMUL	a4, b4, t4
	LDF	[BO -  5 * SIZE], b4

	FADD	c02, t1, c02
	FMUL	a1, b1, t1
	LDF	[AO -  1 * SIZE], a4
	FADD	c04, t2, c04
	FMUL	a1, b2, t2

	FADD	c06, t3, c06
	FMUL	a1, b3, t3
	FADD	c08, t4, c08
	FMUL	a1, b4, t4
	LDF	[AO +  0 * SIZE], a1

	FADD	c01, t1, c01
	FMUL	a2, b1, t1
	LDF	[BO -  4 * SIZE], b1

	FADD	c03, t2, c03
	FMUL	a2, b2, t2
	LDF	[BO -  3 * SIZE], b2

	FADD	c05, t3, c05
	FMUL	a2, b3, t3
	LDF	[BO -  2 * SIZE], b3
	FADD	c07, t4, c07
	FMUL	a2, b4, t4
	LDF	[BO -  1 * SIZE], b4

	FADD	c02, t1, c02
	FMUL	a3, b1, t1
	LDF	[AO +  1 * SIZE], a2
	FADD	c04, t2, c04
	FMUL	a3, b2, t2

	FADD	c06, t3, c06
	FMUL	a3, b3, t3
	FADD	c08, t4, c08
	FMUL	a3, b4, t4
	LDF	[AO +  2 * SIZE], a3

	FADD	c01, t1, c01
	FMUL	a4, b1, t1
	LDF	[BO +  0 * SIZE], b1
	FADD	c03, t2, c03
	FMUL	a4, b2, t2
	LDF	[BO +  1 * SIZE], b2

	FADD	c05, t3, c05
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3
	FADD	c07, t4, c07
	FMUL	a4, b4, t4
	LDF	[BO +  3 * SIZE], b4

	bg,pt	%icc, .LL52
	LDF	[AO +  3 * SIZE], a4

.LL55:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	FADD	c02, t1, c02
	add	AO, 2 * SIZE, AO
	FMUL	a1, b1, t1
	add	L, -1, L

	add	BO, 4 * SIZE, BO
	FADD	c04, t2, c04
	cmp	L, 0
	FMUL	a1, b2, t2

	FADD	c06, t3, c06
	FMUL	a1, b3, t3
	FADD	c08, t4, c08
	FMUL	a1, b4, t4
	LDF	[AO + 0 * SIZE], a1

	FADD	c01, t1, c01
	FMUL	a2, b1, t1
	LDF	[BO + 0 * SIZE], b1
	FADD	c03, t2, c03
	FMUL	a2, b2, t2
	LDF	[BO + 1 * SIZE], b2

	FADD	c05, t3, c05
	FMUL	a2, b3, t3
	LDF	[BO + 2 * SIZE], b3
	FADD	c07, t4, c07
	FMUL	a2, b4, t4
	LDF	[BO + 3 * SIZE], b4

	bg,pt	%icc, .LL56
	LDF	[AO + 1 * SIZE], a2

.LL59:
#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 2, TEMP1
#else
	sub	KK, 4, TEMP1
#endif
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

	FADD	c02, t1, c02
	FADD	c04, t2, c04
	FADD	c06, t3, c06
	FADD	c08, t4, c08

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	LDF	[BO +  4 * SIZE], b1
	LDF	[BO +  5 * SIZE], b2
	LDF	[BO +  6 * SIZE], b3
	LDF	[BO +  7 * SIZE], b4

	FSUB	a1, c01, c01
	FSUB	a2, c03, c03
	FSUB	a3, c05, c05
	FSUB	a4, c07, c07

	FSUB	b1, c02, c02
	FSUB	b2, c04, c04
	FSUB	b3, c06, c06
	FSUB	b4, c08, c08
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	LDF	[AO +  4 * SIZE], b1
	LDF	[AO +  5 * SIZE], b2
	LDF	[AO +  6 * SIZE], b3
	LDF	[AO +  7 * SIZE], b4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04

	FSUB	b1, c05, c05
	FSUB	b2, c06, c06
	FSUB	b3, c07, c07
	FSUB	b4, c08, c08
#endif

#ifdef LN
	LDF	[AO +  3 * SIZE], a1
	LDF	[AO +  2 * SIZE], a2
	LDF	[AO +  0 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a1, c04, c04
	FMUL	a1, c06, c06
	FMUL	a1, c08, c08

	FMUL	a2, c02, t1
	FMUL	a2, c04, t2
	FMUL	a2, c06, t3
	FMUL	a2, c08, t4

	FSUB	c01, t1, c01
	FSUB	c03, t2, c03
	FSUB	c05, t3, c05
	FSUB	c07, t4, c07

	FMUL	a3, c01, c01
	FMUL	a3, c03, c03
	FMUL	a3, c05, c05
	FMUL	a3, c07, c07
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  3 * SIZE], a3

	FMUL	a1, c01, c01
	FMUL	a1, c03, c03
	FMUL	a1, c05, c05
	FMUL	a1, c07, c07

	FMUL	a2, c01, t1
	FMUL	a2, c03, t2
	FMUL	a2, c05, t3
	FMUL	a2, c07, t4

	FSUB	c02, t1, c02
	FSUB	c04, t2, c04
	FSUB	c06, t3, c06
	FSUB	c08, t4, c08

	FMUL	a3, c02, c02
	FMUL	a3, c04, c04
	FMUL	a3, c06, c06
	FMUL	a3, c08, c08
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02

	FMUL	a2, c01, t1
	FMUL	a2, c02, t2

	FSUB	c03, t1, c03
	FSUB	c04, t2, c04

	FMUL	a3, c01, t1
	FMUL	a3, c02, t2

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06

	FMUL	a4, c01, t1
	FMUL	a4, c02, t2

	FSUB	c07, t1, c07
	FSUB	c08, t2, c08

	LDF	[BO +  5 * SIZE], a1
	LDF	[BO +  6 * SIZE], a2
	LDF	[BO +  7 * SIZE], a3

	FMUL	a1, c03, c03
	FMUL	a1, c04, c04

	FMUL	a2, c03, t1
	FMUL	a2, c04, t2

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06

	FMUL	a3, c03, t1
	FMUL	a3, c04, t2

	FSUB	c07, t1, c07
	FSUB	c08, t2, c08

	LDF	[BO + 10 * SIZE], a1
	LDF	[BO + 11 * SIZE], a2

	FMUL	a1, c05, c05
	FMUL	a1, c06, c06

	FMUL	a2, c05, t1
	FMUL	a2, c06, t2

	FSUB	c07, t1, c07
	FSUB	c08, t2, c08

	LDF	[BO + 15 * SIZE], a1

	FMUL	a1, c07, c07
	FMUL	a1, c08, c08
#endif

#ifdef RT
	LDF	[BO + 15 * SIZE], a1
	LDF	[BO + 14 * SIZE], a2
	LDF	[BO + 13 * SIZE], a3
	LDF	[BO + 12 * SIZE], a4

	FMUL	a1, c07, c07
	FMUL	a1, c08, c08

	FMUL	a2, c07, t1
	FMUL	a2, c08, t2

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06

	FMUL	a3, c07, t1
	FMUL	a3, c08, t2

	FSUB	c03, t1, c03
	FSUB	c04, t2, c04

	FMUL	a4, c07, t1
	FMUL	a4, c08, t2

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02

	LDF	[BO + 10 * SIZE], a1
	LDF	[BO +  9 * SIZE], a2
	LDF	[BO +  8 * SIZE], a3

	FMUL	a1, c05, c05
	FMUL	a1, c06, c06

	FMUL	a2, c05, t1
	FMUL	a2, c06, t2

	FSUB	c03, t1, c03
	FSUB	c04, t2, c04

	FMUL	a3, c05, t1
	FMUL	a3, c06, t2

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02

	LDF	[BO +  5 * SIZE], a1
	LDF	[BO +  4 * SIZE], a2

	FMUL	a1, c03, c03
	FMUL	a1, c04, c04

	FMUL	a2, c03, t1
	FMUL	a2, c04, t2

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02

	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
#endif

#ifdef LN
	add	C1, -2 * SIZE, C1
	add	C2, -2 * SIZE, C2
	add	C3, -2 * SIZE, C3
	add	C4, -2 * SIZE, C4
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c03, [BO +  1 * SIZE]
	STF	c05, [BO +  2 * SIZE]
	STF	c07, [BO +  3 * SIZE]

	STF	c02, [BO +  4 * SIZE]
	STF	c04, [BO +  5 * SIZE]
	STF	c06, [BO +  6 * SIZE]
	STF	c08, [BO +  7 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c03, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]

	STF	c05, [AO +  4 * SIZE]
	STF	c06, [AO +  5 * SIZE]
	STF	c07, [AO +  6 * SIZE]
	STF	c08, [AO +  7 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C2 + 0 * SIZE]
	STF	c04, [C2 + 1 * SIZE]

	STF	c05, [C3 + 0 * SIZE]
	STF	c06, [C3 + 1 * SIZE]
	STF	c07, [C4 + 0 * SIZE]
	STF	c08, [C4 + 1 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 2 * SIZE, C1
	add	C2, 2 * SIZE, C2
	add	C3, 2 * SIZE, C3
	add	C4, 2 * SIZE, C4
#endif

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 2, KK
#endif

#ifdef LN
	sub	KK, 2, KK
#endif

.LL70:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL99
	nop

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  0 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 0 + BASE_SHIFT, TEMP1
	sll	KK, 2 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1
 	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c02
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t2
	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, t3
	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, t4

	ble,pn	%icc, .LL75
	nop

.LL72:
	FADD	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	LDF	[BO + 4 * SIZE], b1

	FADD	c02, t2, c02
	cmp	L, 0
	FMUL	a1, b2, t2
	LDF	[BO + 5 * SIZE], b2

	FADD	c03, t3, c03
	FMUL	a1, b3, t3
	LDF	[BO + 6 * SIZE], b3

	FADD	c04, t4, c04
	FMUL	a1, b4, t4
	LDF	[BO + 7 * SIZE], b4
	LDF	[AO +  4 * SIZE], a1

	FADD	c01, t1, c01
	add	AO,  4 * SIZE, AO
	FMUL	a2, b1, t1
	LDF	[BO +  8 * SIZE], b1

	FADD	c02, t2, c02
	FMUL	a2, b2, t2
	LDF	[BO +  9 * SIZE], b2

	FADD	c03, t3, c03
	FMUL	a2, b3, t3
	LDF	[BO + 10 * SIZE], b3

	FADD	c04, t4, c04
	FMUL	a2, b4, t4
	LDF	[BO + 11 * SIZE], b4
	LDF	[AO +  1 * SIZE], a2

	FADD	c01, t1, c01
	FMUL	a3, b1, t1
	LDF	[BO + 12 * SIZE], b1

	FADD	c02, t2, c02
	FMUL	a3, b2, t2
	LDF	[BO + 13 * SIZE], b2

	FADD	c03, t3, c03
	FMUL	a3, b3, t3
	LDF	[BO + 14 * SIZE], b3

	FADD	c04, t4, c04
	FMUL	a3, b4, t4
	LDF	[BO + 15 * SIZE], b4
	LDF	[AO +  2 * SIZE], a3

	FADD	c01, t1, c01
	FMUL	a4, b1, t1
	LDF	[BO + 16 * SIZE], b1

	FADD	c02, t2, c02
	FMUL	a4, b2, t2
	LDF	[BO + 17 * SIZE], b2

	FADD	c03, t3, c03
	FMUL	a4, b3, t3
	LDF	[BO + 18 * SIZE], b3

	FADD	c04, t4, c04
	FMUL	a4, b4, t4
	LDF	[BO + 19 * SIZE], b4

	add	BO, 16 * SIZE, BO
	bg,pt	%icc, .LL72
	LDF	[AO +  3 * SIZE], a4

.LL75:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL79
	nop

.LL76:
	FADD	c01, t1, c01
	add	AO, 1 * SIZE, AO
	FMUL	a1, b1, t1
	LDF	[BO + 4 * SIZE], b1

	FADD	c02, t2, c02
	add	L, -1, L
	FMUL	a1, b2, t2
	LDF	[BO + 5 * SIZE], b2

	FADD	c03, t3, c03
	cmp	L, 0
	FMUL	a1, b3, t3
	LDF	[BO + 6 * SIZE], b3

	FADD	c04, t4, c04
	add	BO, 4 * SIZE, BO
	FMUL	a1, b4, t4
	LDF	[AO + 0 * SIZE], a1

	bg,pt	%icc, .LL76
	LDF	[BO + 3 * SIZE], b4


.LL79:
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 1, TEMP1
#else
	sub	KK, 4, TEMP1
#endif
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04
#endif

#ifdef LN
	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FMUL	a1, c01, c01
	FMUL	a2, c01, t1
	FSUB	c02, t1, c02
	FMUL	a3, c01, t1
	FSUB	c03, t1, c03
	FMUL	a4, c01, t1
	FSUB	c04, t1, c04

	LDF	[BO +  5 * SIZE], a1
	LDF	[BO +  6 * SIZE], a2
	LDF	[BO +  7 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a2, c02, t1
	FSUB	c03, t1, c03
	FMUL	a3, c02, t1
	FSUB	c04, t1, c04

	LDF	[BO + 10 * SIZE], a1
	LDF	[BO + 11 * SIZE], a2

	FMUL	a1, c03, c03
	FMUL	a2, c03, t1
	FSUB	c04, t1, c04

	LDF	[BO + 15 * SIZE], a1

	FMUL	a1, c04, c04
#endif

#ifdef RT
	LDF	[BO + 15 * SIZE], a1
	LDF	[BO + 14 * SIZE], a2
	LDF	[BO + 13 * SIZE], a3
	LDF	[BO + 12 * SIZE], a4

	FMUL	a1, c04, c04
	FMUL	a2, c04, t1
	FSUB	c03, t1, c03
	FMUL	a3, c04, t1
	FSUB	c02, t1, c02
	FMUL	a4, c04, t1
	FSUB	c01, t1, c01

	LDF	[BO + 10 * SIZE], a1
	LDF	[BO +  9 * SIZE], a2
	LDF	[BO +  8 * SIZE], a3

	FMUL	a1, c03, c03
	FMUL	a2, c03, t1
	FSUB	c02, t1, c02
	FMUL	a3, c03, t1
	FSUB	c01, t1, c01

	LDF	[BO +  5 * SIZE], a1
	LDF	[BO +  4 * SIZE], a2

	FMUL	a1, c02, c02
	FMUL	a2, c02, t1
	FSUB	c01, t1, c01

	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
#endif

#ifdef LN
	add	C1, -1 * SIZE, C1
	add	C2, -1 * SIZE, C2
	add	C3, -1 * SIZE, C3
	add	C4, -1 * SIZE, C4
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
	STF	c03, [BO +  2 * SIZE]
	STF	c04, [BO +  3 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c03, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
	STF	c03, [C3 + 0 * SIZE]
	STF	c04, [C4 + 0 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 1 * SIZE, C1
	add	C2, 1 * SIZE, C2
	add	C3, 1 * SIZE, C3
	add	C4, 1 * SIZE, C4
#endif

#ifdef RT
	sll	K, 0 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 1, KK
#endif

#ifdef LN
	sub	KK, 1, KK
#endif

.LL99:
#ifdef LN
	sll	K, 2 + BASE_SHIFT, TEMP1
	add	B, TEMP1, B
#endif

#if defined(LT) || defined(RN)
	mov	BO, B
#endif

#ifdef RN
	add	KK, 4, KK
#endif

#ifdef RT
	sub	KK, 4, KK
#endif

	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	nop

.LL100:  /* n & 2 */
	and	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL200
	nop

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TEMP1
	sub	B, TEMP1, B

	sll	LDC, 1, TEMP1
	sub	C, TEMP1, C
#endif

	mov	C, C1
	add	C, LDC, C2

#ifdef LN
	add	M, OFFSET, KK
#endif

#ifdef LT
	mov	OFFSET, KK
#endif

#if defined(LN) || defined(RT)
	mov	A, AORIG
#else
	mov	A, AO
#endif

#ifndef RT
	add	C2, LDC, C
#endif

	sra	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL150
	FMOV	FZERO, c03

.LL121:
#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  2 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 2 + BASE_SHIFT, TEMP1
	sll	KK, 1 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, t1
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, c07

	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, t2
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, c04

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, t3
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, c08

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, t4
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, c01

	prefetch [C1 + 3 * SIZE], 2
	FMOV	FZERO, c05
	prefetch [C2 + 3 * SIZE], 2
	FMOV	FZERO, c02

	ble,pn	%icc, .LL125
	FMOV	FZERO, c06

.LL122:
	FADD	c03, t1, c03
	add	L, -1, L
	FMUL	a1, b1, t1
	prefetch [AO + APREFETCHSIZE * SIZE], 0

	FADD	c07, t2, c07
	add	BO,  8 * SIZE, BO
	FMUL	a1, b2, t2
	LDF	[AO + 4 * SIZE], a1

	FADD	c04, t3, c04
	add	AO, 16 * SIZE, AO
	FMUL	a2, b1, t3
	cmp	L,  0

	FADD	c08, t4, c08
	nop
	FMUL	a2, b2, t4
	LDF	[AO - 11 * SIZE], a2

	FADD	c01, t1, c01
	nop
	FMUL	a3, b1, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a3, b2, t2
	LDF	[AO - 10 * SIZE], a3

	FADD	c02, t3, c02
	nop
	FMUL	a4, b1, t3
	LDF	[BO -  4 * SIZE], b1

	FADD	c06, t4, c06
	nop
	FMUL	a4, b2, t4
	LDF	[BO -  3 * SIZE], b2

	FADD	c03, t1, c03
	nop
	FMUL	a1, b3, t1
	LDF	[AO -  9 * SIZE], a4

	FADD	c07, t2, c07
	nop
	FMUL	a1, b4, t2
	LDF	[AO -  8 * SIZE], a1

	FADD	c04, t3, c04
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c08, t4, c08
	nop
	FMUL	a2, b4, t4
	LDF	[AO -  7 * SIZE], a2

	FADD	c01, t1, c01
	nop
	FMUL	a3, b3, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a3, b4, t2
	LDF	[AO -  6 * SIZE], a3

	FADD	c02, t3, c02
	nop
	FMUL	a4, b3, t3
	LDF	[BO -  2 * SIZE], b3

	FADD	c06, t4, c06
	nop
	FMUL	a4, b4, t4
	LDF	[BO -  1 * SIZE], b4

	FADD	c03, t1, c03
	nop
	FMUL	a1, b1, t1
	LDF	[AO -  5 * SIZE], a4

	FADD	c07, t2, c07
	nop
	FMUL	a1, b2, t2
	LDF	[AO -  4 * SIZE], a1

	FADD	c04, t3, c04
	nop
	FMUL	a2, b1, t3
	nop

	FADD	c08, t4, c08
	nop
	FMUL	a2, b2, t4
	LDF	[AO -  3 * SIZE], a2

	FADD	c01, t1, c01
	nop
	FMUL	a3, b1, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a3, b2, t2
	LDF	[AO -  2 * SIZE], a3

	FADD	c02, t3, c02
	nop
	FMUL	a4, b1, t3
	LDF	[BO +  0 * SIZE], b1

	FADD	c06, t4, c06
	nop
	FMUL	a4, b2, t4
	LDF	[BO +  1 * SIZE], b2

	FADD	c03, t1, c03
	nop
	FMUL	a1, b3, t1
	LDF	[AO -  1 * SIZE], a4

	FADD	c07, t2, c07
	nop
	FMUL	a1, b4, t2
	LDF	[AO +  0 * SIZE], a1

	FADD	c04, t3, c04
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c08, t4, c08
	nop
	FMUL	a2, b4, t4
	LDF	[AO +  1 * SIZE], a2

	FADD	c01, t1, c01
	nop
	FMUL	a3, b3, t1
	nop

	FADD	c05, t2, c05
	nop
	FMUL	a3, b4, t2
	LDF	[AO +  2 * SIZE], a3

	FADD	c02, t3, c02
	nop
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3

	FADD	c06, t4, c06
	FMUL	a4, b4, t4
	LDF	[AO +  3 * SIZE], a4

	bg,pt	%icc, .LL122
	LDF	[BO +  3 * SIZE], b4

.LL125:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL129
	nop

.LL126:
	FADD	c03, t1, c03
	add	AO, 4 * SIZE, AO
	FMUL	a1, b1, t1
	add	BO, 2 * SIZE, BO

	FADD	c07, t2, c07
	add	L, -1, L
	FMUL	a1, b2, t2
	LDF	[AO + 0 * SIZE], a1

	FADD	c04, t3, c04
	cmp	L, 0
	FMUL	a2, b1, t3

	FADD	c08, t4, c08
	FMUL	a2, b2, t4
	LDF	[AO + 1 * SIZE], a2

	FADD	c01, t1, c01
	FMUL	a3, b1, t1
	FADD	c05, t2, c05
	FMUL	a3, b2, t2
	LDF	[AO + 2 * SIZE], a3

	FADD	c02, t3, c02
	FMUL	a4, b1, t3
	LDF	[BO + 0 * SIZE], b1
	FADD	c06, t4, c06
	FMUL	a4, b2, t4
	LDF	[BO + 1 * SIZE], b2
	bg,pt	%icc, .LL126
	LDF	[AO + 3 * SIZE], a4

.LL129:
	FADD	c03, t1, c03
	FADD	c07, t2, c07
	FADD	c04, t3, c04
	FADD	c08, t4, c08

#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 4, TEMP1
#else
	sub	KK, 2, TEMP1
#endif
	sll	TEMP1, 2 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	LDF	[BO +  4 * SIZE], b1
	LDF	[BO +  5 * SIZE], b2
	LDF	[BO +  6 * SIZE], b3
	LDF	[BO +  7 * SIZE], b4

	FSUB	a1, c01, c01
	FSUB	a2, c05, c05
	FSUB	a3, c02, c02
	FSUB	a4, c06, c06

	FSUB	b1, c03, c03
	FSUB	b2, c07, c07
	FSUB	b3, c04, c04
	FSUB	b4, c08, c08
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	LDF	[AO +  4 * SIZE], b1
	LDF	[AO +  5 * SIZE], b2
	LDF	[AO +  6 * SIZE], b3
	LDF	[AO +  7 * SIZE], b4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04

	FSUB	b1, c05, c05
	FSUB	b2, c06, c06
	FSUB	b3, c07, c07
	FSUB	b4, c08, c08
#endif

#ifdef LN
	LDF	[AO + 15 * SIZE], a1
	LDF	[AO + 14 * SIZE], a2
	LDF	[AO + 13 * SIZE], a3
	LDF	[AO + 12 * SIZE], a4

	FMUL	a1, c04, c04
	FMUL	a1, c08, c08
	FMUL	a2, c04, t1
	FMUL	a2, c08, t2

	FSUB	c03, t1, c03
	FSUB	c07, t2, c07
	FMUL	a3, c04, t1
	FMUL	a3, c08, t2

	FSUB	c02, t1, c02
	FSUB	c06, t2, c06
	FMUL	a4, c04, t1
	FMUL	a4, c08, t2

	FSUB	c01, t1, c01
	FSUB	c05, t2, c05

	LDF	[AO + 10 * SIZE], a1
	LDF	[AO +  9 * SIZE], a2
	LDF	[AO +  8 * SIZE], a3

	FMUL	a1, c03, c03
	FMUL	a1, c07, c07
	FMUL	a2, c03, t1
	FMUL	a2, c07, t2

	FSUB	c02, t1, c02
	FSUB	c06, t2, c06
	FMUL	a3, c03, t1
	FMUL	a3, c07, t2

	FSUB	c01, t1, c01
	FSUB	c05, t2, c05

	LDF	[AO +  5 * SIZE], a1
	LDF	[AO +  4 * SIZE], a2

	FMUL	a1, c02, c02
	FMUL	a1, c06, c06
	FMUL	a2, c02, t1
	FMUL	a2, c06, t2

	FSUB	c01, t1, c01
	FSUB	c05, t2, c05

	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c05, c05
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FMUL	a1, c01, c01
	FMUL	a1, c05, c05
	FMUL	a2, c01, t1
	FMUL	a2, c05, t2

	FSUB	c02, t1, c02
	FSUB	c06, t2, c06
	FMUL	a3, c01, t1
	FMUL	a3, c05, t2

	FSUB	c03, t1, c03
	FSUB	c07, t2, c07
	FMUL	a4, c01, t1
	FMUL	a4, c05, t2

	FSUB	c04, t1, c04
	FSUB	c08, t2, c08

	LDF	[AO +  5 * SIZE], a1
	LDF	[AO +  6 * SIZE], a2
	LDF	[AO +  7 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a1, c06, c06
	FMUL	a2, c02, t1
	FMUL	a2, c06, t2

	FSUB	c03, t1, c03
	FSUB	c07, t2, c07
	FMUL	a3, c02, t1
	FMUL	a3, c06, t2
	FSUB	c04, t1, c04
	FSUB	c08, t2, c08

	LDF	[AO + 10 * SIZE], a1
	LDF	[AO + 11 * SIZE], a2

	FMUL	a1, c03, c03
	FMUL	a1, c07, c07
	FMUL	a2, c03, t1
	FMUL	a2, c07, t2

	FSUB	c04, t1, c04
	FSUB	c08, t2, c08

	LDF	[AO + 15 * SIZE], a1

	FMUL	a1, c04, c04
	FMUL	a1, c08, c08
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  3 * SIZE], a3

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04

	FMUL	a2, c01, t1
	FMUL	a2, c02, t2
	FMUL	a2, c03, t3
	FMUL	a2, c04, t4

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06
	FSUB	c07, t3, c07
	FSUB	c08, t4, c08

	FMUL	a3, c05, c05
	FMUL	a3, c06, c06
	FMUL	a3, c07, c07
	FMUL	a3, c08, c08
#endif

#ifdef RT
	LDF	[BO +  3 * SIZE], a1
	LDF	[BO +  2 * SIZE], a2
	LDF	[BO +  0 * SIZE], a3

	FMUL	a1, c05, c05
	FMUL	a1, c06, c06
	FMUL	a1, c07, c07
	FMUL	a1, c08, c08

	FMUL	a2, c05, t1
	FMUL	a2, c06, t2
	FMUL	a2, c07, t3
	FMUL	a2, c08, t4

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FSUB	c03, t3, c03
	FSUB	c04, t4, c04

	FMUL	a3, c01, c01
	FMUL	a3, c02, c02
	FMUL	a3, c03, c03
	FMUL	a3, c04, c04
#endif

#ifdef LN
	add	C1, -4 * SIZE, C1
	add	C2, -4 * SIZE, C2
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c05, [BO +  1 * SIZE]
	STF	c02, [BO +  2 * SIZE]
	STF	c06, [BO +  3 * SIZE]

	STF	c03, [BO +  4 * SIZE]
	STF	c07, [BO +  5 * SIZE]
	STF	c04, [BO +  6 * SIZE]
	STF	c08, [BO +  7 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c03, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]

	STF	c05, [AO +  4 * SIZE]
	STF	c06, [AO +  5 * SIZE]
	STF	c07, [AO +  6 * SIZE]
	STF	c08, [AO +  7 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]

	STF	c05, [C2 + 0 * SIZE]
	STF	c06, [C2 + 1 * SIZE]
	STF	c07, [C2 + 2 * SIZE]
	STF	c08, [C2 + 3 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 4 * SIZE, C1
	add	C2, 4 * SIZE, C2
#endif

#ifdef RT
	sll	K, 2 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 2 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 4, KK
#endif

#ifdef LN
	sub	KK, 4, KK
#endif

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL121
	FMOV	FZERO, c03

.LL150:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL170
	nop

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  1 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 1 + BASE_SHIFT, TEMP1
	sll	KK, 1 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1

	LDF	[AO + 1 * SIZE], a2
	cmp	L,  0
	FMOV	FZERO, c02
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t2

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, t3

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, t4
	ble,pn	%icc, .LL155
	nop

.LL152:
	FADD	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	prefetch [AO + APREFETCHSIZE * SIZE], 0

	FADD	c02, t2, c02
	add	BO,  8 * SIZE, BO
	FMUL	a1, b2, t2
	LDF	[AO + 4 * SIZE], a1

	FADD	c03, t3, c03
	cmp	L, 0
	FMUL	a2, b1, t3
	LDF	[BO - 4 * SIZE], b1

	FADD	c04, t4, c04
	nop
	FMUL	a2, b2, t4
	LDF	[AO + 5 * SIZE], a2

	FADD	c01, t1, c01
	nop
	FMUL	a3, b3, t1
	LDF	[BO - 3 * SIZE], b2

	FADD	c02, t2, c02
	nop
	FMUL	a3, b4, t2
	LDF	[AO + 6 * SIZE], a3

	FADD	c03, t3, c03
	nop
	FMUL	a4, b3, t3
	LDF	[BO - 2 * SIZE], b3

	FADD	c04, t4, c04
	nop
	FMUL	a4, b4, t4
	LDF	[AO + 7 * SIZE], a4

	FADD	c01, t1, c01
	nop
	FMUL	a1, b1, t1
	LDF	[BO - 1 * SIZE], b4

	FADD	c02, t2, c02
	FMUL	a1, b2, t2
	LDF	[AO +  8 * SIZE], a1

	FADD	c03, t3, c03
	FMUL	a2, b1, t3
	LDF	[BO +  0 * SIZE], b1

	FADD	c04, t4, c04
	FMUL	a2, b2, t4
	LDF	[AO +  9 * SIZE], a2

	FADD	c01, t1, c01
	FMUL	a3, b3, t1
	LDF	[BO +  1 * SIZE], b2

	FADD	c02, t2, c02
	FMUL	a3, b4, t2
	LDF	[AO + 10 * SIZE], a3

	FADD	c03, t3, c03
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3

	FADD	c04, t4, c04
	FMUL	a4, b4, t4
	LDF	[AO + 11 * SIZE], a4

	add	AO,  8 * SIZE, AO
	bg,pt	%icc, .LL152
	LDF	[BO +  3 * SIZE], b4

.LL155:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL159
	nop

.LL156:
	LDF	[AO + 0 * SIZE], a1
	LDF	[AO + 1 * SIZE], a2

	LDF	[BO + 0 * SIZE], b1
	LDF	[BO + 1 * SIZE], b2

	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FMUL	a1, b1, t1
	FMUL	a1, b2, t2
	FMUL	a2, b1, t3
	FMUL	a2, b2, t4

	add	AO, 2 * SIZE, AO
	add	BO, 2 * SIZE, BO

	add	L, -1, L
	cmp	L, 0
	bg,pt	%icc, .LL156
	nop

.LL159:
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 2, TEMP1
#else
	sub	KK, 2, TEMP1
#endif
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c03, c03
	FSUB	a3, c02, c02
	FSUB	a4, c04, c04
#endif

#ifdef LN
	LDF	[AO +  3 * SIZE], a1
	LDF	[AO +  2 * SIZE], a2
	LDF	[AO +  0 * SIZE], a3

	FMUL	a1, c03, c03
	FMUL	a1, c04, c04
	FMUL	a2, c03, t1
	FMUL	a2, c04, t2

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FMUL	a3, c01, c01
	FMUL	a3, c02, c02
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  3 * SIZE], a3

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02

	FMUL	a2, c01, t1
	FMUL	a2, c02, t2

	FSUB	c03, t1, c03
	FSUB	c04, t2, c04

	FMUL	a3, c03, c03
	FMUL	a3, c04, c04
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  3 * SIZE], a3

	FMUL	a1, c01, c01
	FMUL	a1, c03, c03
	FMUL	a2, c01, t1
	FMUL	a2, c03, t2

	FSUB	c02, t1, c02
	FSUB	c04, t2, c04
	FMUL	a3, c02, c02
	FMUL	a3, c04, c04
#endif

#ifdef RT
	LDF	[BO +  3 * SIZE], a1
	LDF	[BO +  2 * SIZE], a2
	LDF	[BO +  0 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a1, c04, c04

	FMUL	a2, c02, t1
	FMUL	a2, c04, t2
	FSUB	c01, t1, c01
	FSUB	c03, t2, c03

	FMUL	a3, c01, c01
	FMUL	a3, c03, c03
#endif

#ifdef LN
	add	C1, -2 * SIZE, C1
	add	C2, -2 * SIZE, C2
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
	STF	c03, [BO +  2 * SIZE]
	STF	c04, [BO +  3 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c03, [AO +  1 * SIZE]
	STF	c02, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c03, [C1 + 1 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
	STF	c04, [C2 + 1 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 2 * SIZE, C1
	add	C2, 2 * SIZE, C2
#endif

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 2, KK
#endif

#ifdef LN
	sub	KK, 2, KK
#endif


.LL170:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL199
	nop


#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  0 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 0 + BASE_SHIFT, TEMP1
	sll	KK, 1 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1

 	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c02
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t2

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03

	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, t3

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, t4

	ble,pn	%icc, .LL175
	nop

.LL172:
	FADD	c01, t1, c01
	add	AO,  4 * SIZE, AO
	FMUL	a1, b1, t1
	LDF	[BO + 4 * SIZE], b1

	FADD	c02, t2, c02
	FMUL	a1, b2, t2
	LDF	[BO + 5 * SIZE], b2

	add	L, -1, L
	LDF	[AO + 0 * SIZE], a1

	FADD	c03, t3, c03
	cmp	L, 0
	FMUL	a2, b3, t3
	LDF	[BO + 6 * SIZE], b3

	FADD	c04, t4, c04
	FMUL	a2, b4, t4
	LDF	[BO + 7 * SIZE], b4
	LDF	[AO + 1 * SIZE], a2

	FADD	c01, t1, c01
	FMUL	a3, b1, t1
	LDF	[BO +  8 * SIZE], b1

	FADD	c02, t2, c02
	FMUL	a3, b2, t2
	LDF	[BO +  9 * SIZE], b2
	LDF	[AO + 2 * SIZE], a3

	FADD	c03, t3, c03
	FMUL	a4, b3, t3
	LDF	[BO + 10 * SIZE], b3
	FADD	c04, t4, c04
	FMUL	a4, b4, t4
	LDF	[BO + 11 * SIZE], b4
	add	BO,  8 * SIZE, BO

	bg,pt	%icc, .LL172
	LDF	[AO + 3 * SIZE], a4

.LL175:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL179
	nop

.LL176:
	FADD	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	add	AO, 1 * SIZE, AO
	LDF	[BO + 2 * SIZE], b1
	FADD	c02, t2, c02
	cmp	L, 0
	FMUL	a1, b2, t2
	LDF	[BO + 3 * SIZE], b2

	add	BO, 2 * SIZE, BO
	bg,pt	%icc, .LL176
	LDF	[AO + 0 * SIZE], a1

.LL179:
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FADD	c01, c03, c01
	FADD	c02, c04, c02


#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 1, TEMP1
#else
	sub	KK, 2, TEMP1
#endif
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
#endif

#ifdef LN
	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  3 * SIZE], a3

	FMUL	a1, c01, c01
	FMUL	a2, c01, t1
	FSUB	c02, t1, c02
	FMUL	a3, c02, c02
#endif

#ifdef RT
	LDF	[BO +  3 * SIZE], a1
	LDF	[BO +  2 * SIZE], a2
	LDF	[BO +  0 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a2, c02, t1
	FSUB	c01, t1, c01
	FMUL	a3, c01, c01
#endif

#ifdef LN
	add	C1, -1 * SIZE, C1
	add	C2, -1 * SIZE, C2
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 1 * SIZE, C1
	add	C2, 1 * SIZE, C2
#endif

#ifdef RT
	sll	K, 0 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 1, KK
#endif

#ifdef LN
	sub	KK, 1, KK
#endif

.LL199:
#ifdef LN
	sll	K, 1 + BASE_SHIFT, TEMP1
	add	B, TEMP1, B
#endif

#if defined(LT) || defined(RN)
	mov	BO, B
#endif

#ifdef RN
	add	KK, 2, KK
#endif

#ifdef RT
	sub	KK, 2, KK
#endif

.LL200:
	and	N, 1, J

	cmp	J, 0
	ble,pn	%icc, .LL999
	nop

#ifdef RT
	sll	K, 0 + BASE_SHIFT, TEMP1
	sub	B, TEMP1, B

	sub	C, LDC, C
#endif

	mov	C, C1

#ifdef LN
	add	M, OFFSET, KK
#endif

#ifdef LT
	mov	OFFSET, KK
#endif

#if defined(LN) || defined(RT)
	mov	A, AORIG
#else
	mov	A, AO
#endif

#ifndef RT
	add	C, LDC, C
#endif

	sra	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL250
	nop

.LL221:
#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  2 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 2 + BASE_SHIFT, TEMP1
	sll	KK, 0 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1

	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c02
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t2

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, t3

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, t4

	ble,pn	%icc, .LL225
	prefetch [C1 + 4 * SIZE], 2

.LL222:
	FADD	c01, t1, c01
	add	BO,  4 * SIZE, BO
	FMUL	a1, b1, t1
	LDF	[AO +  4 * SIZE], a1

	FADD	c02, t2, c02
	FMUL	a2, b1, t2
	LDF	[AO +  5 * SIZE], a2

	FADD	c03, t3, c03
	add	L, -1, L
	FMUL	a3, b1, t3
	LDF	[AO +  6 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b1, t4
	LDF	[AO +  7 * SIZE], a4
	LDF	[BO +  0 * SIZE], b1

	FADD	c01, t1, c01
	cmp	L,  0
	FMUL	a1, b2, t1
	LDF	[AO +  8 * SIZE], a1

	FADD	c02, t2, c02
	FMUL	a2, b2, t2
	LDF	[AO +  9 * SIZE], a2

	FADD	c03, t3, c03
	FMUL	a3, b2, t3
	LDF	[AO + 10 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b2, t4
	LDF	[AO + 11 * SIZE], a4
	LDF	[BO +  1 * SIZE], b2

	FADD	c01, t1, c01
	FMUL	a1, b3, t1
	LDF	[AO + 12 * SIZE], a1

	FADD	c02, t2, c02
	FMUL	a2, b3, t2
	LDF	[AO + 13 * SIZE], a2

	FADD	c03, t3, c03
	FMUL	a3, b3, t3
	LDF	[AO + 14 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b3, t4
	LDF	[AO + 15 * SIZE], a4
	LDF	[BO +  2 * SIZE], b3

	FADD	c01, t1, c01
	FMUL	a1, b4, t1
	LDF	[AO + 16 * SIZE], a1

	FADD	c02, t2, c02
	FMUL	a2, b4, t2
	LDF	[AO + 17 * SIZE], a2

	FADD	c03, t3, c03
	FMUL	a3, b4, t3
	LDF	[AO + 18 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b4, t4
	LDF	[AO + 19 * SIZE], a4
	add	AO, 16 * SIZE, AO

	bg,pt	%icc, .LL222
	LDF	[BO +  3 * SIZE], b4

.LL225:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL229
	nop

.LL226:
	FADD	c01, t1, c01
	add	BO, 1 * SIZE, BO
	FMUL	a1, b1, t1
	LDF	[AO + 4 * SIZE], a1

	FADD	c02, t2, c02
	add	L, -1, L
	FMUL	a2, b1, t2
	LDF	[AO + 5 * SIZE], a2

	FADD	c03, t3, c03
	cmp	L, 0
	FMUL	a3, b1, t3
	LDF	[AO + 6 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b1, t4
	LDF	[AO + 7 * SIZE], a4
	add	AO, 4 * SIZE, AO

	bg,pt	%icc, .LL226
	LDF	[BO + 0 * SIZE], b1

.LL229:
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 4, TEMP1
#else
	sub	KK, 1, TEMP1
#endif
	sll	TEMP1, 2 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c03, c03
	FSUB	a4, c04, c04
#endif

#ifdef LN
	LDF	[AO + 15 * SIZE], a1
	LDF	[AO + 14 * SIZE], a2
	LDF	[AO + 13 * SIZE], a3
	LDF	[AO + 12 * SIZE], a4

	FMUL	a1, c04, c04
	FMUL	a2, c04, t1

	FSUB	c03, t1, c03
	FMUL	a3, c04, t1

	FSUB	c02, t1, c02
	FMUL	a4, c04, t1

	FSUB	c01, t1, c01

	LDF	[AO + 10 * SIZE], a1
	LDF	[AO +  9 * SIZE], a2
	LDF	[AO +  8 * SIZE], a3

	FMUL	a1, c03, c03
	FMUL	a2, c03, t1

	FSUB	c02, t1, c02
	FMUL	a3, c03, t1
	FSUB	c01, t1, c01

	LDF	[AO +  5 * SIZE], a1
	LDF	[AO +  4 * SIZE], a2

	FMUL	a1, c02, c02
	FMUL	a2, c02, t1
	FSUB	c01, t1, c01

	LDF	[AO +  0 * SIZE], a1

	FMUL	a1, c01, c01
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FMUL	a1, c01, c01
	FMUL	a2, c01, t1
	FSUB	c02, t1, c02
	FMUL	a3, c01, t1
	FSUB	c03, t1, c03
	FMUL	a4, c01, t1
	FSUB	c04, t1, c04

	LDF	[AO +  5 * SIZE], a1
	LDF	[AO +  6 * SIZE], a2
	LDF	[AO +  7 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a2, c02, t1
	FSUB	c03, t1, c03
	FMUL	a3, c02, t1
	FSUB	c04, t1, c04

	LDF	[AO + 10 * SIZE], a1
	LDF	[AO + 11 * SIZE], a2

	FMUL	a1, c03, c03
	FMUL	a2, c03, t1

	FSUB	c04, t1, c04

	LDF	[AO + 15 * SIZE], a1

	FMUL	a1, c04, c04
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04
#endif

#ifdef RT
	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
	FMUL	a1, c03, c03
	FMUL	a1, c04, c04
#endif

#ifdef LN
	add	C1, -4 * SIZE, C1
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
	STF	c03, [BO +  2 * SIZE]
	STF	c04, [BO +  3 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c03, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 4 * SIZE, C1
#endif

#ifdef RT
	sll	K, 2 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 2 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 4, KK
#endif

#ifdef LN
	sub	KK, 4, KK
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL221
	nop

.LL250:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL270
	nop

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  1 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 1 + BASE_SHIFT, TEMP1
	sll	KK, 0 + BASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1

	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c02
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t2

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, t3

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, t4

	ble,pn	%icc, .LL255
	nop

.LL252:
	FADD	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	LDF	[AO + 4 * SIZE], a1

	FADD	c02, t2, c02
	FMUL	a2, b1, t2
	LDF	[AO +  5 * SIZE], a2
	LDF	[BO +  4 * SIZE], b1

	FADD	c03, t3, c03
	cmp	L, 0
	FMUL	a3, b2, t3
	LDF	[AO +  6 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b2, t4
	LDF	[AO +  7 * SIZE], a4
	LDF	[BO +  5 * SIZE], b2

	FADD	c01, t1, c01
	FMUL	a1, b3, t1
	LDF	[AO +  8 * SIZE], a1

	FADD	c02, t2, c02
	FMUL	a2, b3, t2
	LDF	[AO +  9 * SIZE], a2
	LDF	[BO +  6 * SIZE], b3

	FADD	c03, t3, c03
	FMUL	a3, b4, t3
	LDF	[AO + 10 * SIZE], a3

	FADD	c04, t4, c04
	FMUL	a4, b4, t4
	LDF	[AO + 11 * SIZE], a4
	add	AO,  8 * SIZE, AO

	LDF	[BO +  7 * SIZE], b4
	bg,pt	%icc, .LL252
	add	BO,  4 * SIZE, BO

.LL255:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif

	cmp	L,  0
	ble,a,pn %icc, .LL259
	nop

.LL256:
	FADD	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	LDF	[AO + 2 * SIZE], a1

	FADD	c02, t2, c02
	cmp	L, 0
	FMUL	a2, b1, t2
	LDF	[AO + 3 * SIZE], a2

	LDF	[BO + 1 * SIZE], b1
	add	AO, 2 * SIZE, AO

	bg,pt	%icc, .LL256
	add	BO, 1 * SIZE, BO

.LL259:
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FADD	c01, c03, c01
	FADD	c02, c04, c02

#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 2, TEMP1
#else
	sub	KK, 1, TEMP1
#endif
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
#endif

#ifdef LN
	LDF	[AO +  3 * SIZE], a1
	LDF	[AO +  2 * SIZE], a2
	LDF	[AO +  0 * SIZE], a3

	FMUL	a1, c02, c02
	FMUL	a2, c02, t1
	FSUB	c01, t1, c01
	FMUL	a3, c01, c01
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  3 * SIZE], a3

	FMUL	a1, c01, c01
	FMUL	a2, c01, t1
	FSUB	c02, t1, c02
	FMUL	a3, c02, c02
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
#endif

#ifdef RT
	LDF	[BO +  0 * SIZE], a1

	FMUL	a1, c01, c01
	FMUL	a1, c02, c02
#endif

#ifdef LN
	add	C1, -2 * SIZE, C1
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 2 * SIZE, C1
#endif

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 2, KK
#endif

#ifdef LN
	sub	KK, 2, KK
#endif

.LL270:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL299
	nop

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  0 + BASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 0 + BASE_SHIFT, TEMP1

	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, t1
 	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c01

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, t2
	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c02

	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t3
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t4
	LDF	[BO + 2 * SIZE], b3

	ble,pn	%icc, .LL275
	LDF	[BO + 3 * SIZE], b4

.LL272:
	FADD	c01, t1, c01
	add	L, -1, L
	add	AO,  4 * SIZE, AO

	FMUL	a1, b1, t1
	add	BO,  4 * SIZE, BO
	LDF	[AO + 0 * SIZE], a1

	FADD	c02, t2, c02
	cmp	L, 0
	LDF	[BO + 0 * SIZE], b1
	FMUL	a2, b2, t2

	LDF	[AO + 1 * SIZE], a2
	FADD	c01, t3, c01
	LDF	[BO + 1 * SIZE], b2
	FMUL	a3, b3, t3

	LDF	[AO + 2 * SIZE], a3
	FADD	c02, t4, c02
	LDF	[BO + 2 * SIZE], b3
	FMUL	a4, b4, t4
	LDF	[AO + 3 * SIZE], a4

	bg,pt	%icc, .LL272
	LDF	[BO + 3 * SIZE], b4

.LL275:
#if defined(LT) || defined(RN)
	and	KK,  3, L
#else
	and	TEMP1, 3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL279
	nop

.LL276:
	FADD	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	LDF	[AO + 1 * SIZE], a1

	LDF	[BO + 1 * SIZE], b1
	add	BO, 1 * SIZE, BO
	cmp	L, 0
	bg,pt	%icc, .LL276
	add	AO, 1 * SIZE, AO

.LL279:
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c01, t3, c01
	FADD	c02, t4, c02

	FADD	c01, c02, c01

#if defined(LN) || defined(RT)
	sub	KK, 1, TEMP1
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO
#endif

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	FSUB	a1, c01, c01
#else
	LDF	[AO +  0 * SIZE], a1
	FSUB	a1, c01, c01
#endif

#ifdef LN
	LDF	[AO +  0 * SIZE], a1
	FMUL	a1, c01, c01
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	FMUL	a1, c01, c01
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	FMUL	a1, c01, c01
#endif

#ifdef RT
	LDF	[BO +  0 * SIZE], a1
	FMUL	a1, c01, c01
#endif

#ifdef LN
	add	C1, -1 * SIZE, C1
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 1 * SIZE, C1
#endif

#ifdef RT
	sll	K, 0 + BASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1
	add	AO, TEMP1, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 1, KK
#endif

#ifdef LN
	sub	KK, 1, KK
#endif


.LL299:
#ifdef LN
	sll	K, 0 + BASE_SHIFT, TEMP1
	add	B, TEMP1, B
#endif

#if defined(LT) || defined(RN)
	mov	BO, B
#endif

#ifdef RN
	add	KK, 1, KK
#endif

#ifdef RT
	sub	KK, 1, KK
#endif


.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
