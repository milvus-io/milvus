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
#define A	%i5
#define B	%i3
#define C	%i4

#define LDC	%o0
#define AO	%o1
#define BO	%o2
#define I	%o3
#define J	%o4
#define L	%o5

#define C1	%l0
#define C2	%l1

#define OFFSET	%l2
#define	KK	%l3
#define TEMP1	%l4
#define TEMP2	%l5
#define AORIG	%l6

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
#define a5	%f62

#define b1	%f48
#define b2	%f50
#define b3	%f52
#define b4	%f54
#define b5	%f56

#define FZERO	%f58

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

#define	t5	c13
#define	t6	c14
#define	t7	c15
#define	t8	c16

#ifndef CONJ
#define FADD1	FADD
#define FADD2	FADD
#define FADD3	FADD
#define FADD4	FSUB
#else

#if defined(LN) || defined(LT)
#define FADD1	FADD
#define FADD2	FSUB
#define FADD3	FADD
#define FADD4	FADD
#endif

#if defined(RN) || defined(RT)
#define FADD1	FADD
#define FADD2	FADD
#define FADD3	FSUB
#define FADD4	FADD
#endif
#endif

#define APREFETCHSIZE 40
#define BPREFETCHSIZE 40

#define APREFETCH_CATEGORY 0
#define BPREFETCH_CATEGORY 0

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	ld	[%sp + STACK_START + 32], A
	ld	[%sp + STACK_START + 36], B
	ld	[%sp + STACK_START + 40], C
	ld	[%sp + STACK_START + 44], LDC
	ld	[%sp + STACK_START + 48], OFFSET
#else
	ld	[%sp + STACK_START + 28], B
	ld	[%sp + STACK_START + 32], C
	ld	[%sp + STACK_START + 36], LDC
	ld	[%sp + STACK_START + 40], OFFSET
#endif
#else
	ldx	[%sp+  STACK_START + 56], B
	ldx	[%sp+  STACK_START + 64], C
	ldx	[%sp+  STACK_START + 72], LDC
	ldx	[%sp+  STACK_START + 80], OFFSET
#endif

#ifdef DOUBLE
       FCLR(27)
#else
       FCLR(29)
#endif

	sll	LDC, ZBASE_SHIFT, LDC

#ifdef LN
	smul	M, K, TEMP1
	sll	TEMP1, ZBASE_SHIFT, TEMP1
	add	A, TEMP1, A

	sll	M, ZBASE_SHIFT, TEMP1
	add	C, TEMP1, C
#endif

#ifdef RN
	neg	OFFSET, KK
#endif

#ifdef RT
	smul	N, K, TEMP1
	sll	TEMP1, ZBASE_SHIFT, TEMP1
	add	B, TEMP1, B

	smul	N, LDC, TEMP1
	add	C, TEMP1, C

	sub	N, OFFSET, KK
#endif

	and	N, 1, J

	cmp	J, 0
	ble,pn	%icc, .LL100
	nop

#ifdef RT
	sll	K, 0 + ZBASE_SHIFT, TEMP1
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
	sra	M, 1, I
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
	sll	K,  1 + ZBASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 1 + ZBASE_SHIFT, TEMP1
	sll	KK, 0 + ZBASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1
	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	FMOV	FZERO, c03

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

	prefetch [C1 + 3 * SIZE], 3
	FMOV	FZERO, c05
	FMOV	FZERO, c02

	ble,pn	%icc, .LL125
	FMOV	FZERO, c06

.LL122:
	FADD1	c03, t1, c03
	add	L, -1, L
	FMUL	a1, b1, t1
	prefetch [AO + APREFETCHSIZE * SIZE], 0

	FADD3	c07, t2, c07
	add	BO,  8 * SIZE, BO
	FMUL	a1, b2, t2
	LDF	[AO + 4 * SIZE], a1

	FADD2	c04, t3, c04
	add	AO, 16 * SIZE, AO
	FMUL	a2, b1, t3
	cmp	L,  0

	FADD4	c08, t4, c08
	nop
	FMUL	a2, b2, t4
	LDF	[AO - 11 * SIZE], a2

	FADD1	c01, t1, c01
	nop
	FMUL	a3, b1, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a3, b2, t2
	LDF	[AO - 10 * SIZE], a3

	FADD2	c02, t3, c02
	nop
	FMUL	a4, b1, t3
	LDF	[BO -  4 * SIZE], b1

	FADD4	c06, t4, c06
	nop
	FMUL	a4, b2, t4
	LDF	[BO -  3 * SIZE], b2

	FADD1	c03, t1, c03
	nop
	FMUL	a1, b3, t1
	LDF	[AO -  9 * SIZE], a4

	FADD3	c07, t2, c07
	nop
	FMUL	a1, b4, t2
	LDF	[AO -  8 * SIZE], a1

	FADD2	c04, t3, c04
	nop
	FMUL	a2, b3, t3
	nop

	FADD4	c08, t4, c08
	nop
	FMUL	a2, b4, t4
	LDF	[AO -  7 * SIZE], a2

	FADD1	c01, t1, c01
	nop
	FMUL	a3, b3, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a3, b4, t2
	LDF	[AO -  6 * SIZE], a3

	FADD2	c02, t3, c02
	nop
	FMUL	a4, b3, t3
	LDF	[BO -  2 * SIZE], b3

	FADD4	c06, t4, c06
	nop
	FMUL	a4, b4, t4
	LDF	[BO -  1 * SIZE], b4

	FADD1	c03, t1, c03
	nop
	FMUL	a1, b1, t1
	LDF	[AO -  5 * SIZE], a4

	FADD3	c07, t2, c07
	nop
	FMUL	a1, b2, t2
	LDF	[AO -  4 * SIZE], a1

	FADD2	c04, t3, c04
	nop
	FMUL	a2, b1, t3
	nop

	FADD4	c08, t4, c08
	nop
	FMUL	a2, b2, t4
	LDF	[AO -  3 * SIZE], a2

	FADD1	c01, t1, c01
	nop
	FMUL	a3, b1, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a3, b2, t2
	LDF	[AO -  2 * SIZE], a3

	FADD2	c02, t3, c02
	nop
	FMUL	a4, b1, t3
	LDF	[BO +  0 * SIZE], b1

	FADD4	c06, t4, c06
	nop
	FMUL	a4, b2, t4
	LDF	[BO +  1 * SIZE], b2

	FADD1	c03, t1, c03
	nop
	FMUL	a1, b3, t1
	LDF	[AO -  1 * SIZE], a4

	FADD3	c07, t2, c07
	nop
	FMUL	a1, b4, t2
	LDF	[AO +  0 * SIZE], a1

	FADD2	c04, t3, c04
	nop
	FMUL	a2, b3, t3
	nop

	FADD4	c08, t4, c08
	nop
	FMUL	a2, b4, t4
	LDF	[AO +  1 * SIZE], a2

	FADD1	c01, t1, c01
	nop
	FMUL	a3, b3, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a3, b4, t2
	LDF	[AO +  2 * SIZE], a3

	FADD2	c02, t3, c02
	nop
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3

	FADD4	c06, t4, c06
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
	FADD1	c03, t1, c03
	add	AO, 4 * SIZE, AO
	FMUL	a1, b1, t1
	add	BO, 2 * SIZE, BO

	FADD3	c07, t2, c07
	add	L, -1, L
	FMUL	a1, b2, t2
	LDF	[AO + 0 * SIZE], a1

	FADD2	c04, t3, c04
	cmp	L, 0
	FMUL	a2, b1, t3

	FADD4	c08, t4, c08
	FMUL	a2, b2, t4
	LDF	[AO + 1 * SIZE], a2

	FADD1	c01, t1, c01
	FMUL	a3, b1, t1
	FADD3	c05, t2, c05
	FMUL	a3, b2, t2
	LDF	[AO + 2 * SIZE], a3

	FADD2	c02, t3, c02
	FMUL	a4, b1, t3
	LDF	[BO + 0 * SIZE], b1
	FADD4	c06, t4, c06
	FMUL	a4, b2, t4
	LDF	[BO + 1 * SIZE], b2
	bg,pt	%icc, .LL126
	LDF	[AO + 3 * SIZE], a4

.LL129:
	FADD1	c03, t1, c03
	FADD3	c07, t2, c07
	FADD2	c04, t3, c04
	FADD4	c08, t4, c08

	FADD	c01, c06, c01
	FADD	c02, c05, c02
	FADD	c03, c08, c03
	FADD	c04, c07, c04

#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 2, TEMP1
#else
	sub	KK, 1, TEMP1
#endif
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP1
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
	LDF	[AO +  6 * SIZE], a1
	LDF	[AO +  7 * SIZE], a2
	LDF	[AO +  4 * SIZE], a3
	LDF	[AO +  5 * SIZE], a4
	LDF	[AO +  0 * SIZE], b1
	LDF	[AO +  1 * SIZE], b2

	FMUL	a1, c03, t1
	FMUL	a2, c04, t2
	FMUL	a1, c04, t3
	FMUL	a2, c03, t4

	FADD4	t1, t2, c03
	FADD2	t3, t4, c04

	FMUL	a3, c03, t1
	FMUL	a3, c04, t2

	FMUL	a4, c04, t5
	FMUL	a4, c03, t6

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02

	FADD2	c01, t5, c01
	FADD4	c02, t6, c02

	FMUL	b1, c01, t1
	FMUL	b2, c02, t2
	FMUL	b1, c02, t3
	FMUL	b2, c01, t4

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4
	LDF	[AO +  6 * SIZE], b1
	LDF	[AO +  7 * SIZE], b2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02

	FMUL	a3, c01, t1
	FMUL	a3, c02, t2
	FMUL	a4, c02, t5
	FMUL	a4, c01, t6

	FSUB	c03, t1, c03
	FSUB	c04, t2, c04
	FADD2	c03, t5, c03
	FADD4	c04, t6, c04

	FMUL	b1, c03, t1
	FMUL	b2, c04, t2
	FMUL	b1, c04, t3
	FMUL	b2, c03, t4

	FADD4	t1, t2, c03
	FADD2	t3, t4, c04
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FMUL	a1, c03, t5
	FMUL	a2, c04, t6
	FMUL	a1, c04, t7
	FMUL	a2, c03, t8

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
	FADD4	t5, t6, c03
	FADD3	t7, t8, c04
#endif

#ifdef RT
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FMUL	a1, c03, t5
	FMUL	a2, c04, t6
	FMUL	a1, c04, t7
	FMUL	a2, c03, t8

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
	FADD4	t5, t6, c03
	FADD3	t7, t8, c04
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
	sll	K, 1 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP1
	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 2, KK
#endif

#ifdef LN
	sub	KK, 2, KK
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL121
	FMOV	FZERO, c03

.LL150:
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
	sll	K,  0 + ZBASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 0 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO

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

	ble,pn	%icc, .LL155
	nop

.LL152:
	FADD1	c01, t1, c01
	add	L, -1, L
	FMUL	a1, b1, t1
	prefetch [AO + APREFETCHSIZE * SIZE], 0

	FADD3	c02, t2, c02
	add	BO,  8 * SIZE, BO
	FMUL	a1, b2, t2
	LDF	[AO + 4 * SIZE], a1

	FADD2	c03, t3, c03
	cmp	L, 0
	FMUL	a2, b1, t3
	LDF	[BO - 4 * SIZE], b1

	FADD4	c04, t4, c04
	nop
	FMUL	a2, b2, t4
	LDF	[AO + 5 * SIZE], a2

	FADD1	c01, t1, c01
	nop
	FMUL	a3, b3, t1
	LDF	[BO - 3 * SIZE], b2

	FADD3	c02, t2, c02
	nop
	FMUL	a3, b4, t2
	LDF	[AO + 6 * SIZE], a3

	FADD2	c03, t3, c03
	nop
	FMUL	a4, b3, t3
	LDF	[BO - 2 * SIZE], b3

	FADD4	c04, t4, c04
	nop
	FMUL	a4, b4, t4
	LDF	[AO + 7 * SIZE], a4

	FADD1	c01, t1, c01
	nop
	FMUL	a1, b1, t1
	LDF	[BO - 1 * SIZE], b4

	FADD3	c02, t2, c02
	FMUL	a1, b2, t2
	LDF	[AO +  8 * SIZE], a1

	FADD2	c03, t3, c03
	FMUL	a2, b1, t3
	LDF	[BO +  0 * SIZE], b1

	FADD4	c04, t4, c04
	FMUL	a2, b2, t4
	LDF	[AO +  9 * SIZE], a2

	FADD1	c01, t1, c01
	FMUL	a3, b3, t1
	LDF	[BO +  1 * SIZE], b2

	FADD3	c02, t2, c02
	FMUL	a3, b4, t2
	LDF	[AO + 10 * SIZE], a3

	FADD2	c03, t3, c03
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3

	FADD4	c04, t4, c04
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
	FADD1	c01, t1, c01
	add	AO, 2 * SIZE, AO
	FMUL	a1, b1, t1
	add	BO, 2 * SIZE, BO
	FADD3	c02, t2, c02
	add	L, -1, L
	FMUL	a1, b2, t2
	LDF	[AO + 0 * SIZE], a1
	FADD2	c03, t3, c03
	FMUL	a2, b1, t3
	LDF	[BO + 0 * SIZE], b1
	cmp	L, 0
	FADD4	c04, t4, c04
	FMUL	a2, b2, t4
	LDF	[BO + 1 * SIZE], b2

	bg,pt	%icc, .LL156
	LDF	[AO + 1 * SIZE], a2

.LL159:
	FADD1	c01, t1, c01
	FADD3	c02, t2, c02
	FADD2	c03, t3, c03
	FADD4	c04, t4, c04

	FADD	c01, c04, c01
	FADD	c02, c03, c02

#if defined(LN) || defined(RT)
	sub	KK, 1, TEMP1

	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AO
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
	LDF	[AO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
#endif

#ifdef RT
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
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
	sll	K, 0 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP1
	add	AO, TEMP1, AO
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
	sll	K, 0 + ZBASE_SHIFT, TEMP1
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

.LL100:
	sra	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL999
	nop

.LL11:
#ifdef RT
	sll	K, 1 + ZBASE_SHIFT, TEMP1
	sub	B, TEMP1, B

	add	LDC, LDC, TEMP1
	sub	C, TEMP1, C
#endif

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3

	sra	M, 1, I
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

	cmp	I, 0
#ifndef RT
	add	C2, LDC, C
#endif
	ble,pn	%icc, .LL50
	FMOV	FZERO, t4


.LL21:
#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  1 + ZBASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 1 + ZBASE_SHIFT, TEMP1

	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO

	sub	K, KK, TEMP1

	sra	TEMP1, 2, L
	cmp	L,  0
#endif

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

	FMOV	FZERO, c01
	FMOV	FZERO, c02

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c03
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, c04

	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c05
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, c06

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c07
	LDF	[BO + 2 * SIZE], b3
	FMOV	FZERO, c08

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c09
	LDF	[BO + 3 * SIZE], b4
	FMOV	FZERO, c10

	LDF	[BO +  4 * SIZE], b5
	FMOV	FZERO, c11
	LDF	[AO +  4 * SIZE], a5
	FMOV	FZERO, c12

	prefetch [C1 + 3 * SIZE], 3
	FMOV	FZERO, c13
	prefetch [C2 + 3 * SIZE], 3
	FMOV	FZERO, c14

	FMOV	FZERO, c15
	ble,pn	%icc, .LL25
	FMOV	FZERO, c16

.LL22:
	FADD2	c04, t1, c04
	prefetch [AO + APREFETCHSIZE * SIZE], APREFETCH_CATEGORY
	FMUL	a1, b1, t1
	nop

	FADD4	c08, t2, c08
	prefetch [BO + BPREFETCHSIZE * SIZE], BPREFETCH_CATEGORY
	FMUL	a1, b2, t2
	add	AO, 16 * SIZE, AO

	FADD2	c12, t3, c12
	LDF	[AO - 13 * SIZE], a4
	FMUL	a1, b3, t3
	add	BO, 16 * SIZE, BO

	FADD4	c16, t4, c16
	nop
	FMUL	a1, b4, t4
	LDF	[AO -  8 * SIZE], a1

	FADD1	c01, t1, c01
	nop
	FMUL	a2, b1, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD1	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD3	c13, t4, c13
	add	L, -1, L
	FMUL	a2, b4, t4
	LDF	[AO - 11 * SIZE], a2

	FADD2	c02, t1, c02
	nop
	FMUL	a3, b1, t1
	nop

	FADD4	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD2	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD4	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO - 10 * SIZE], a3

	FADD1	c03, t1, c03
	nop
	FMUL	a4, b1, t1
	LDF	[BO -  8 * SIZE], b1

	FADD3	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO - 11 * SIZE], b2

	FADD1	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO - 10 * SIZE], b3

	FADD3	c15, t4, c15
	nop
	FMUL	a4, b4, t4
	LDF	[BO -  9 * SIZE], b4

	FADD2	c04, t1, c04
	nop
	FMUL	a5, b5, t1
	LDF	[AO -  9 * SIZE], a4

	FADD4	c08, t2, c08
	nop
	FMUL	a5, b2, t2
	nop

	FADD2	c12, t3, c12
	nop
	FMUL	a5, b3, t3
	nop

	FADD4	c16, t4, c16
	nop
	FMUL	a5, b4, t4
	LDF	[AO - 4 * SIZE], a5

	FADD1	c01, t1, c01
	nop
	FMUL	a2, b5, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD1	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD3	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	LDF	[AO -  7 * SIZE], a2

	FADD2	c02, t1, c02
	nop
	FMUL	a3, b5, t1
	nop

	FADD4	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD2	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD4	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO -  6 * SIZE], a3

	FADD1	c03, t1, c03
	nop
	FMUL	a4, b5, t1
	LDF	[BO - 4 * SIZE], b5

	FADD3	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO -  7 * SIZE], b2

	FADD1	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO -  6 * SIZE], b3

	FADD3	c15, t4, c15
	nop
	FMUL	a4, b4, t4
	LDF	[BO -  5 * SIZE], b4

	FADD2	c04, t1, c04
	nop
	FMUL	a1, b1, t1
	LDF	[AO -  5 * SIZE], a4

	FADD4	c08, t2, c08
	nop
	FMUL	a1, b2, t2
	nop

	FADD2	c12, t3, c12
	nop
	FMUL	a1, b3, t3
	nop

	FADD4	c16, t4, c16
	nop
	FMUL	a1, b4, t4
	LDF	[AO -  0 * SIZE], a1

	FADD1	c01, t1, c01
	nop
	FMUL	a2, b1, t1
	nop

#ifdef DOUBLE
	prefetch [AO + (APREFETCHSIZE + 8) * SIZE], APREFETCH_CATEGORY
#else
	nop
#endif
	FADD3	c05, t2, c05
	nop
	FMUL	a2, b2, t2

	FADD1	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD3	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	nop

	FADD2	c02, t1, c02
	nop
	FMUL	a3, b1, t1
	LDF	[AO - 3 * SIZE], a2

	FADD4	c06, t2, c06
#ifdef DOUBLE
	prefetch [BO + (BPREFETCHSIZE + 8) * SIZE], BPREFETCH_CATEGORY
#else
	nop
#endif
	FMUL	a3, b2, t2
	nop

	FADD2	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD4	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO - 2 * SIZE], a3

	FADD1	c03, t1, c03
	nop
	FMUL	a4, b1, t1
	LDF	[BO -  0 * SIZE], b1

	FADD3	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO - 3 * SIZE], b2

	FADD1	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO - 2 * SIZE], b3

	FADD3	c15, t4, c15
	nop
	FMUL	a4, b4, t4
	LDF	[BO - 1 * SIZE], b4

	FADD2	c04, t1, c04
	nop
	FMUL	a5, b5, t1
	LDF	[AO - 1 * SIZE], a4

	FADD4	c08, t2, c08
	FMUL	a5, b2, t2
	FADD2	c12, t3, c12
	FMUL	a5, b3, t3

	FADD4	c16, t4, c16
	nop
	FMUL	a5, b4, t4
	LDF	[AO +  4 * SIZE], a5

	FADD1	c01, t1, c01
	nop
	FMUL	a2, b5, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD1	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD3	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	LDF	[AO +  1 * SIZE], a2

	FADD2	c02, t1, c02
	nop
	FMUL	a3, b5, t1
	nop

	FADD4	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD2	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD4	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO +  2 * SIZE], a3

	FADD1	c03, t1, c03
	cmp	L, 0
	FMUL	a4, b5, t1
	LDF	[BO +  4 * SIZE], b5

	FADD3	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO +  1 * SIZE], b2

	FADD1	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3

	FADD3	c15, t4, c15
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
	ble,pn %icc, .LL29
	nop

.LL26:
	FADD2	c04, t1, c04
	LDF	[AO +  3 * SIZE], a4
	FMUL	a1, b1, t1
	add	AO, 4 * SIZE, AO

	FADD4	c08, t2, c08
	add	BO, 4 * SIZE, BO
	FMUL	a1, b2, t2
	add	L, -1, L

	FADD2	c12, t3, c12
	nop
	FMUL	a1, b3, t3
	cmp	L, 0

	FADD4	c16, t4, c16
	nop
	FMUL	a1, b4, t4
	LDF	[AO + 0 * SIZE], a1

	FADD1	c01, t1, c01
	nop
	FMUL	a2, b1, t1
	nop

	FADD3	c05, t2, c05
	nop
	FMUL	a2, b2, t2
	nop

	FADD1	c09, t3, c09
	nop
	FMUL	a2, b3, t3
	nop

	FADD3	c13, t4, c13
	nop
	FMUL	a2, b4, t4
	LDF	[AO + 1 * SIZE], a2

	FADD2	c02, t1, c02
	nop
	FMUL	a3, b1, t1
	nop

	FADD4	c06, t2, c06
	nop
	FMUL	a3, b2, t2
	nop

	FADD2	c10, t3, c10
	nop
	FMUL	a3, b3, t3
	nop

	FADD4	c14, t4, c14
	nop
	FMUL	a3, b4, t4
	LDF	[AO + 2 * SIZE], a3

	FADD1	c03, t1, c03
	nop
	FMUL	a4, b1, t1
	LDF	[BO + 0 * SIZE], b1

	FADD3	c07, t2, c07
	nop
	FMUL	a4, b2, t2
	LDF	[BO + 1 * SIZE], b2

	FADD1	c11, t3, c11
	nop
	FMUL	a4, b3, t3
	LDF	[BO + 2 * SIZE], b3

	FADD3	c15, t4, c15
	FMUL	a4, b4, t4
	bg,pt	%icc, .LL26
	LDF	[BO + 3 * SIZE], b4

.LL29:
#if defined(LN) || defined(RT)
	sub	KK, 2, TEMP1
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AO
	add	B,     TEMP1, BO
#endif

	FADD2	c04, t1, c04
	FADD4	c08, t2, c08
	FADD2	c12, t3, c12
	FADD4	c16, t4, c16

	FADD	c01, c06, c01
	FADD	c02, c05, c02
	FADD	c03, c08, c03
	FADD	c04, c07, c04

	FADD	c09, c14, c09
	FADD	c10, c13, c10
	FADD	c11, c16, c11
	FADD	c12, c15, c12

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
	FSUB	a2, c02, c02
	FSUB	a3, c09, c09
	FSUB	a4, c10, c10

	FSUB	b1, c03, c03
	FSUB	b2, c04, c04
	FSUB	b3, c11, c11
	FSUB	b4, c12, c12
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

	FSUB	b1, c09, c09
	FSUB	b2, c10, c10
	FSUB	b3, c11, c11
	FSUB	b4, c12, c12
#endif

#ifdef LN
	LDF	[AO +  6 * SIZE], a1
	LDF	[AO +  7 * SIZE], a2
	LDF	[AO +  4 * SIZE], a3
	LDF	[AO +  5 * SIZE], a4
	LDF	[AO +  0 * SIZE], b1
	LDF	[AO +  1 * SIZE], b2

	FMUL	a1, c03, t1
	FMUL	a2, c04, t2
	FMUL	a1, c04, t3
	FMUL	a2, c03, t4

	FMUL	a1, c11, t5
	FMUL	a2, c12, t6
	FMUL	a1, c12, t7
	FMUL	a2, c11, t8

	FADD4	t1, t2, c03
	FADD2	t3, t4, c04
	FADD4	t5, t6, c11
	FADD2	t7, t8, c12

	FMUL	a3, c03, t1
	FMUL	a3, c04, t2
	FMUL	a3, c11, t3
	FMUL	a3, c12, t4

	FMUL	a4, c04, t5
	FMUL	a4, c03, t6
	FMUL	a4, c12, t7
	FMUL	a4, c11, t8

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FSUB	c09, t3, c09
	FSUB	c10, t4, c10

	FADD2	c01, t5, c01
	FADD4	c02, t6, c02
	FADD2	c09, t7, c09
	FADD4	c10, t8, c10

	FMUL	b1, c01, t1
	FMUL	b2, c02, t2
	FMUL	b1, c02, t3
	FMUL	b2, c01, t4

	FMUL	b1, c09, t5
	FMUL	b2, c10, t6
	FMUL	b1, c10, t7
	FMUL	b2, c09, t8

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
	FADD4	t5, t6, c09
	FADD2	t7, t8, c10
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4
	LDF	[AO +  6 * SIZE], b1
	LDF	[AO +  7 * SIZE], b2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FMUL	a1, c09, t5
	FMUL	a2, c10, t6
	FMUL	a1, c10, t7
	FMUL	a2, c09, t8

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
	FADD4	t5, t6, c09
	FADD2	t7, t8, c10

	FMUL	a3, c01, t1
	FMUL	a3, c02, t2
	FMUL	a3, c09, t3
	FMUL	a3, c10, t4

	FMUL	a4, c02, t5
	FMUL	a4, c01, t6
	FMUL	a4, c10, t7
	FMUL	a4, c09, t8

	FSUB	c03, t1, c03
	FSUB	c04, t2, c04
	FSUB	c11, t3, c11
	FSUB	c12, t4, c12

	FADD2	c03, t5, c03
	FADD4	c04, t6, c04
	FADD2	c11, t7, c11
	FADD4	c12, t8, c12

	FMUL	b1, c03, t1
	FMUL	b2, c04, t2
	FMUL	b1, c04, t3
	FMUL	b2, c03, t4

	FMUL	b1, c11, t5
	FMUL	b2, c12, t6
	FMUL	b1, c12, t7
	FMUL	b2, c11, t8

	FADD4	t1, t2, c03
	FADD2	t3, t4, c04
	FADD4	t5, t6, c11
	FADD2	t7, t8, c12
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4
	LDF	[BO +  6 * SIZE], b1
	LDF	[BO +  7 * SIZE], b2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FMUL	a1, c03, t5
	FMUL	a2, c04, t6
	FMUL	a1, c04, t7
	FMUL	a2, c03, t8

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
	FADD4	t5, t6, c03
	FADD3	t7, t8, c04

	FMUL	a3, c01, t1
	FMUL	a3, c02, t2
	FMUL	a3, c03, t3
	FMUL	a3, c04, t4

	FMUL	a4, c02, t5
	FMUL	a4, c01, t6
	FMUL	a4, c04, t7
	FMUL	a4, c03, t8

	FSUB	c09, t1, c09
	FSUB	c10, t2, c10
	FSUB	c11, t3, c11
	FSUB	c12, t4, c12

	FADD3	c09, t5, c09
	FADD4	c10, t6, c10
	FADD3	c11, t7, c11
	FADD4	c12, t8, c12

	FMUL	b1, c09, t1
	FMUL	b2, c10, t2
	FMUL	b1, c10, t3
	FMUL	b2, c09, t4

	FMUL	b1, c11, t5
	FMUL	b2, c12, t6
	FMUL	b1, c12, t7
	FMUL	b2, c11, t8

	FADD4	t1, t2, c09
	FADD3	t3, t4, c10
	FADD4	t5, t6, c11
	FADD3	t7, t8, c12
#endif

#ifdef RT
	LDF	[BO +  6 * SIZE], a1
	LDF	[BO +  7 * SIZE], a2
	LDF	[BO +  4 * SIZE], a3
	LDF	[BO +  5 * SIZE], a4
	LDF	[BO +  0 * SIZE], b1
	LDF	[BO +  1 * SIZE], b2

	FMUL	a1, c09, t1
	FMUL	a2, c10, t2
	FMUL	a1, c10, t3
	FMUL	a2, c09, t4

	FMUL	a1, c11, t5
	FMUL	a2, c12, t6
	FMUL	a1, c12, t7
	FMUL	a2, c11, t8

	FADD4	t1, t2, c09
	FADD3	t3, t4, c10
	FADD4	t5, t6, c11
	FADD3	t7, t8, c12

	FMUL	a3, c09, t1
	FMUL	a3, c10, t2
	FMUL	a3, c11, t3
	FMUL	a3, c12, t4

	FMUL	a4, c10, t5
	FMUL	a4, c09, t6
	FMUL	a4, c12, t7
	FMUL	a4, c11, t8

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FSUB	c03, t3, c03
	FSUB	c04, t4, c04

	FADD3	c01, t5, c01
	FADD4	c02, t6, c02
	FADD3	c03, t7, c03
	FADD4	c04, t8, c04

	FMUL	b1, c01, t1
	FMUL	b2, c02, t2
	FMUL	b1, c02, t3
	FMUL	b2, c01, t4

	FMUL	b1, c03, t5
	FMUL	b2, c04, t6
	FMUL	b1, c04, t7
	FMUL	b2, c03, t8

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
	FADD4	t5, t6, c03
	FADD3	t7, t8, c04
#endif

#ifdef LN
	add	C1, -4 * SIZE, C1
	add	C2, -4 * SIZE, C2
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
	STF	c09, [BO +  2 * SIZE]
	STF	c10, [BO +  3 * SIZE]

	STF	c03, [BO +  4 * SIZE]
	STF	c04, [BO +  5 * SIZE]
	STF	c11, [BO +  6 * SIZE]
	STF	c12, [BO +  7 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c03, [AO +  2 * SIZE]
	STF	c04, [AO +  3 * SIZE]

	STF	c09, [AO +  4 * SIZE]
	STF	c10, [AO +  5 * SIZE]
	STF	c11, [AO +  6 * SIZE]
	STF	c12, [AO +  7 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]

	STF	c09, [C2 + 0 * SIZE]
	STF	c10, [C2 + 1 * SIZE]
	STF	c11, [C2 + 2 * SIZE]
	STF	c12, [C2 + 3 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 4 * SIZE, C1
	add	C2, 4 * SIZE, C2
#endif

#ifdef RT
	sll	K, 1 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP1
	add	AO, TEMP1, AO
	add	BO, TEMP1, BO
#endif

#ifdef LT
	add	KK, 2, KK
#endif

#ifdef LN
	sub	KK, 2, KK
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL21
	FMOV	FZERO, c01

.LL50:
	and	M, 1, I
	FMOV	FZERO, c02
	cmp	I, 0
	FMOV	FZERO, t1
	ble,pn	%icc, .LL99
	FMOV	FZERO, c04

#if defined(LT) || defined(RN)
	sra	KK, 2, L

	mov	B, BO
	cmp	L,  0
#else

#ifdef LN
	sll	K,  0 + ZBASE_SHIFT, TEMP1
	sub	AORIG, TEMP1, AORIG
#endif

	sll	KK, 0 + ZBASE_SHIFT, TEMP1
	sll	KK, 1 + ZBASE_SHIFT, TEMP2

	add	AORIG, TEMP1, AO
	add	B,     TEMP2, BO

	sub	K, KK, TEMP1

	sra	TEMP1, 2, L
	cmp	L,  0
#endif

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
	FADD2	c02, t1, c02
	add	AO,  8 * SIZE, AO
	prefetch [AO + APREFETCHSIZE * SIZE], 0

	FMUL	a1, b1, t1
	add	BO, 16 * SIZE, BO

	FADD4	c04, t2, c04
	add	L, -1, L
	FMUL	a1, b2, t2

	FADD2	c06, t3, c06
	cmp	L, 0
	FMUL	a1, b3, t3

	FADD4	c08, t4, c08
	FMUL	a1, b4, t4
	LDF	[AO -  4 * SIZE], a1

	FADD1	c01, t1, c01
	FMUL	a2, b1, t1
	LDF	[BO - 12 * SIZE], b1
	FADD3	c03, t2, c03
	FMUL	a2, b2, t2
	LDF	[BO - 11 * SIZE], b2

	FADD1	c05, t3, c05
	FMUL	a2, b3, t3
	LDF	[BO - 10 * SIZE], b3
	FADD3	c07, t4, c07
	FMUL	a2, b4, t4
	LDF	[BO -  9 * SIZE], b4

	FADD2	c02, t1, c02
	FMUL	a3, b1, t1
	LDF	[AO -  3 * SIZE], a2
	FADD4	c04, t2, c04
	FMUL	a3, b2, t2

	FADD2	c06, t3, c06
	FMUL	a3, b3, t3
	FADD4	c08, t4, c08
	FMUL	a3, b4, t4
	LDF	[AO -  2 * SIZE], a3

	FADD1	c01, t1, c01
	FMUL	a4, b1, t1
	LDF	[BO -  8 * SIZE], b1
	FADD3	c03, t2, c03
	FMUL	a4, b2, t2
	LDF	[BO -  7 * SIZE], b2

	FADD1	c05, t3, c05
	FMUL	a4, b3, t3
	LDF	[BO -  6 * SIZE], b3
	FADD3	c07, t4, c07
	FMUL	a4, b4, t4
	LDF	[BO -  5 * SIZE], b4

	FADD2	c02, t1, c02
	FMUL	a1, b1, t1
	LDF	[AO -  1 * SIZE], a4
	FADD4	c04, t2, c04
	FMUL	a1, b2, t2

	FADD2	c06, t3, c06
	FMUL	a1, b3, t3
	FADD4	c08, t4, c08
	FMUL	a1, b4, t4
	LDF	[AO +  0 * SIZE], a1

	FADD1	c01, t1, c01
	FMUL	a2, b1, t1
	LDF	[BO -  4 * SIZE], b1

	FADD3	c03, t2, c03
	FMUL	a2, b2, t2
	LDF	[BO -  3 * SIZE], b2

	FADD1	c05, t3, c05
	FMUL	a2, b3, t3
	LDF	[BO -  2 * SIZE], b3
	FADD3	c07, t4, c07
	FMUL	a2, b4, t4
	LDF	[BO -  1 * SIZE], b4

	FADD2	c02, t1, c02
	FMUL	a3, b1, t1
	LDF	[AO +  1 * SIZE], a2
	FADD4	c04, t2, c04
	FMUL	a3, b2, t2

	FADD2	c06, t3, c06
	FMUL	a3, b3, t3
	FADD4	c08, t4, c08
	FMUL	a3, b4, t4
	LDF	[AO +  2 * SIZE], a3

	FADD1	c01, t1, c01
	FMUL	a4, b1, t1
	LDF	[BO +  0 * SIZE], b1
	FADD3	c03, t2, c03
	FMUL	a4, b2, t2
	LDF	[BO +  1 * SIZE], b2

	FADD1	c05, t3, c05
	FMUL	a4, b3, t3
	LDF	[BO +  2 * SIZE], b3
	FADD3	c07, t4, c07
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
	FADD2	c02, t1, c02
	add	AO, 2 * SIZE, AO
	FMUL	a1, b1, t1
	add	L, -1, L

	add	BO, 4 * SIZE, BO
	FADD4	c04, t2, c04
	cmp	L, 0
	FMUL	a1, b2, t2

	FADD2	c06, t3, c06
	FMUL	a1, b3, t3
	FADD4	c08, t4, c08
	FMUL	a1, b4, t4
	LDF	[AO + 0 * SIZE], a1

	FADD1	c01, t1, c01
	FMUL	a2, b1, t1
	LDF	[BO + 0 * SIZE], b1
	FADD3	c03, t2, c03
	FMUL	a2, b2, t2
	LDF	[BO + 1 * SIZE], b2

	FADD1	c05, t3, c05
	FMUL	a2, b3, t3
	LDF	[BO + 2 * SIZE], b3
	FADD3	c07, t4, c07
	FMUL	a2, b4, t4
	LDF	[BO + 3 * SIZE], b4

	bg,pt	%icc, .LL56
	LDF	[AO + 1 * SIZE], a2

.LL59:
#if defined(LN) || defined(RT)
#ifdef LN
	sub	KK, 1, TEMP1
#else
	sub	KK, 2, TEMP1
#endif
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP2, AO
	add	B,     TEMP1, BO
#endif

	FADD2	c02, t1, c02
	FADD4	c04, t2, c04
	FADD2	c06, t3, c06
	FADD4	c08, t4, c08

	FADD	c01, c04, c01
	FADD	c02, c03, c02
	FADD	c05, c08, c05
	FADD	c06, c07, c06

#if defined(LN) || defined(LT)
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c05, c05
	FSUB	a4, c06, c06

#else
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	FSUB	a1, c01, c01
	FSUB	a2, c02, c02
	FSUB	a3, c05, c05
	FSUB	a4, c06, c06
#endif

#ifdef LN
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FMUL	a1, c05, t5
	FMUL	a2, c06, t6
	FMUL	a1, c06, t7
	FMUL	a2, c05, t8

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
	FADD4	t5, t6, c05
	FADD2	t7, t8, c06
#endif

#ifdef LT
	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FMUL	a1, c05, t5
	FMUL	a2, c06, t6
	FMUL	a1, c06, t7
	FMUL	a2, c05, t8

	FADD4	t1, t2, c01
	FADD2	t3, t4, c02
	FADD4	t5, t6, c05
	FADD2	t7, t8, c06
#endif

#ifdef RN
	LDF	[BO +  0 * SIZE], a1
	LDF	[BO +  1 * SIZE], a2
	LDF	[BO +  2 * SIZE], a3
	LDF	[BO +  3 * SIZE], a4
	LDF	[BO +  6 * SIZE], b1
	LDF	[BO +  7 * SIZE], b2

	FMUL	a1, c01, t1
	FMUL	a2, c02, t2
	FMUL	a1, c02, t3
	FMUL	a2, c01, t4

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02

	FMUL	a3, c01, t1
	FMUL	a3, c02, t2
	FMUL	a4, c02, t3
	FMUL	a4, c01, t4

	FSUB	c05, t1, c05
	FSUB	c06, t2, c06
	FADD3	c05, t3, c05
	FADD4	c06, t4, c06

	FMUL	b1, c05, t1
	FMUL	b2, c06, t2
	FMUL	b1, c06, t3
	FMUL	b2, c05, t4

	FADD4	t1, t2, c05
	FADD3	t3, t4, c06
#endif

#ifdef RT
	LDF	[BO +  6 * SIZE], a1
	LDF	[BO +  7 * SIZE], a2
	LDF	[BO +  4 * SIZE], a3
	LDF	[BO +  5 * SIZE], a4
	LDF	[BO +  0 * SIZE], b1
	LDF	[BO +  1 * SIZE], b2

	FMUL	a1, c05, t1
	FMUL	a2, c06, t2
	FMUL	a1, c06, t3
	FMUL	a2, c05, t4

	FADD4	t1, t2, c05
	FADD3	t3, t4, c06

	FMUL	a3, c05, t1
	FMUL	a3, c06, t2
	FMUL	a4, c06, t3
	FMUL	a4, c05, t4

	FSUB	c01, t1, c01
	FSUB	c02, t2, c02
	FADD3	c01, t3, c01
	FADD4	c02, t4, c02

	FMUL	b1, c01, t1
	FMUL	b2, c02, t2
	FMUL	b1, c02, t3
	FMUL	b2, c01, t4

	FADD4	t1, t2, c01
	FADD3	t3, t4, c02
#endif

#ifdef LN
	add	C1, -2 * SIZE, C1
	add	C2, -2 * SIZE, C2
#endif

#if defined(LN) || defined(LT)
	STF	c01, [BO +  0 * SIZE]
	STF	c02, [BO +  1 * SIZE]
	STF	c05, [BO +  2 * SIZE]
	STF	c06, [BO +  3 * SIZE]
#else
	STF	c01, [AO +  0 * SIZE]
	STF	c02, [AO +  1 * SIZE]
	STF	c05, [AO +  2 * SIZE]
	STF	c06, [AO +  3 * SIZE]
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c05, [C2 + 0 * SIZE]
	STF	c06, [C2 + 1 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

#ifndef LN
	add	C1, 2 * SIZE, C1
	add	C2, 2 * SIZE, C2
#endif

#ifdef RT
	sll	K, 0 + ZBASE_SHIFT, TEMP1
	add	AORIG, TEMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	sub	K, KK, TEMP1
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP1
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
	sll	K, 1 + ZBASE_SHIFT, TEMP1
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

	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	nop

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
