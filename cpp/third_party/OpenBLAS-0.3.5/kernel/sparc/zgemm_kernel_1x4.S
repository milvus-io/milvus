/*********************************************************************/
/* Copyright 2005-2010 The University of Texas at Austin.           */
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

#define APREFETCHSIZE 24
#define APREFETCH_CATEGORY 0

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

#define BB	%o7

#define C1	%l0
#define C2	%l1
#define C3	%l2
#define C4	%l3

#define OFFSET	%l4
#define	KK	%l5
#define TEMP1	%l6
#define TEMP2	%l7

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

#define a1	%f32
#define a2	%f34
#define a3	%f36
#define a4	%f38
#define a5	%f40

#define b1	%f42
#define b2	%f44
#define b3	%f46
#define b4	%f48
#define b5	%f50
#define b6	%f52
#define b7	%f54
#define b8	%f56
#define b9	%f58

#define ALPHA_R	%f60
#define ALPHA_I	%f62

#define cc01	0
#define cc02	2
#define cc03	4
#define cc04	6
#define cc05	8
#define cc06	10
#define cc07	12
#define cc08	14
#define cc09	16
#define cc10	18
#define cc11	20
#define cc12	22
#define cc13	24
#define cc14	26
#define cc15	28
#define cc16	30

#define aa1	 1
#define aa2	 3
#define aa3	 5
#define aa4	 7
#define aa5	 9

#define bb1	11
#define bb2	13
#define bb3	15
#define bb4	17
#define bb5	19
#define bb6	21
#define bb7	23
#define bb8	25
#define bb9	27

#define alpha_r	29
#define alpha_i	31
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

#define a1	%f16
#define a2	%f17
#define a3	%f18
#define a4	%f19
#define a5	%f20

#define b1	%f21
#define b2	%f22
#define b3	%f23
#define b4	%f24
#define b5	%f25
#define b6	%f26
#define b7	%f27
#define b8	%f28
#define b9	%f29

#define ALPHA_R	%f30
#define ALPHA_I	%f31

#define cc01	0
#define cc02	1
#define cc03	2
#define cc04	3
#define cc05	4
#define cc06	5
#define cc07	6
#define cc08	7
#define cc09	8
#define cc10	9
#define cc11	10
#define cc12	11
#define cc13	12
#define cc14	13
#define cc15	14
#define cc16	15

#define aa1	16
#define aa2	17
#define aa3	18
#define aa4	19
#define aa5	20

#define bb1	21
#define bb2	22
#define bb3	23
#define bb4	24
#define bb5	25
#define bb6	26
#define bb7	27
#define bb8	28
#define bb9	29

#define alpha_r	30
#define alpha_i	31

#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define FMADD1	FMADD
#define FMADD2	FMADD
#define FMADD3	FMADD
#define FMADD4	FNMSUB
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define FMADD1	FMADD
#define FMADD2	FMADD
#define FMADD3	FNMSUB
#define FMADD4	FMADD
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define FMADD1	FMADD
#define FMADD2	FNMSUB
#define FMADD3	FMADD
#define FMADD4	FMADD
#else
#define FMADD1	FMADD
#define FMADD2	FNMSUB
#define FMADD3	FNMSUB
#define FMADD4	FNMSUB
#endif

        .register %g2, #scratch
        .register %g3, #scratch

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]
	st	%i5, [%sp + STACK_START + 24]

	ld	[%sp + STACK_START + 32], A
	ld	[%sp + STACK_START + 36], B
	ld	[%sp + STACK_START + 40], C
	ld	[%sp + STACK_START + 44], LDC
#ifdef TRMMKERNEL
	ld	[%sp + STACK_START + 48], OFFSET
#endif

	ldd	[%sp + STACK_START + 16], ALPHA_R
	ldd	[%sp + STACK_START + 24], ALPHA_I
#else
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]

	ld	[%sp + STACK_START + 28], B
	ld	[%sp + STACK_START + 32], C
	ld	[%sp + STACK_START + 36], LDC
#ifdef TRMMKERNEL
	ld	[%sp + STACK_START + 40], OFFSET
#endif

	ld	[%sp + STACK_START + 16], ALPHA_R
	ld	[%sp + STACK_START + 20], ALPHA_I
#endif
#else
	ldx	[%sp + STACK_START + 56], B
	ldx	[%sp + STACK_START + 64], C
	ldx	[%sp + STACK_START + 72], LDC
#ifdef TRMMKERNEL
	ldx	[%sp + STACK_START + 80], OFFSET
#endif

#ifdef DOUBLE
	FMOV	%f6, ALPHA_R
	FMOV	%f8, ALPHA_I
#else
	FMOV	%f7, ALPHA_R
	FMOV	%f9, ALPHA_I
#endif
#endif

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	OFFSET, KK
#endif

	cmp	M, 0
	ble,pn	%icc, .LL999
	nop

	sra	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL20
	sll	LDC, ZBASE_SHIFT, LDC

.LL11:
	mov	C,  C1
	add	C,  LDC, C2
	add	C2, LDC, C3
	add	C3, LDC, C4
	add	C4,  LDC, C

	sll	K, ZBASE_SHIFT + 2, BB

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	mov	A, AO

	mov	M, I
	add	B, BB, BB
	.align 4

.LL12:
	prefetch [BB +  0 * SIZE], 1
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, ZBASE_SHIFT + 0, TEMP1
	sll	KK, ZBASE_SHIFT + 2, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

	LDF	[AO +  0 * SIZE], a1
	FCLR	(cc01)
	LDF	[AO +  1 * SIZE], a2
	FCLR	(cc05)
	LDF	[AO +  8 * SIZE], a5
	FCLR	(cc09)
	LDF	[BO +  0 * SIZE], b1
	FCLR	(cc13)

	LDF	[BO +  1 * SIZE], b2
	FCLR	(cc02)
	LDF	[BO +  2 * SIZE], b3
	FCLR	(cc06)
	LDF	[BO +  3 * SIZE], b4
	FCLR	(cc10)
	LDF	[BO +  4 * SIZE], b5
	FCLR	(cc14)

	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc03)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc07)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc11)
	LDF	[BO +  8 * SIZE], b9
	FCLR	(cc15)

	prefetch [C1 + 1 * SIZE], 3
	FCLR	(cc04)
	prefetch [C2 + 2 * SIZE], 3
	FCLR	(cc08)
	prefetch [C3 + 1 * SIZE], 3
	FCLR	(cc12)
	prefetch [C4 + 2 * SIZE], 3
	FCLR	(cc16)

#ifndef TRMMKERNEL
	sra	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 4, L
#endif
	sra	L,  3, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL15
	add	 BB, 32 * SIZE, BB
	.align 4

.LL13:
	FMADD1	(aa1, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa1, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa1, bb3, cc05, cc05)
	LDF	[BO + 16 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO +  9 * SIZE], b2

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD1	(aa1, bb5, cc09, cc09)
	LDF	[AO +  2 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO +  3 * SIZE], a4

	FMADD3	(aa1, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD4	(aa2, bb6, cc12, cc12)
	nop

	FMADD1	(aa1, bb7, cc13, cc13)
	LDF	[BO + 12 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO + 13 * SIZE], b6

	FMADD3	(aa1, bb8, cc15, cc15)
	LDF	[BO + 14 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO + 15 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO + 24 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO + 17 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO + 18 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO + 19 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO +  4 * SIZE], a1
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO +  5 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	add	L, -1, L
	FMADD4	(aa4, bb6, cc12, cc12)
	nop

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO + 20 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO + 21 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO + 22 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	LDF	[BO + 23 * SIZE], b8

	FMADD1	(aa1, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa1, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa1, bb3, cc05, cc05)
	LDF	[BO + 32 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 25 * SIZE], b2

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO + 26 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 27 * SIZE], b4

	FMADD1	(aa1, bb5, cc09, cc09)
	LDF	[AO +  6 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO +  7 * SIZE], a4

	FMADD3	(aa1, bb6, cc11, cc11)
	nop
	FMADD4	(aa2, bb6, cc12, cc12)
	nop

	FMADD1	(aa1, bb7, cc13, cc13)
	LDF	[BO + 28 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO + 29 * SIZE], b6

	FMADD3	(aa1, bb8, cc15, cc15)
	LDF	[BO + 30 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO + 31 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO + 40 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO + 33 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO + 34 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO + 35 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO + 16 * SIZE], a1  /****/
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO +  9 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	nop
	FMADD4	(aa4, bb6, cc12, cc12)
	nop

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO + 36 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO + 37 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO + 38 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	LDF	[BO + 39 * SIZE], b8

	FMADD1	(aa5, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa5, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa5, bb3, cc05, cc05)
	LDF	[BO + 48 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 41 * SIZE], b2

	FMADD3	(aa5, bb4, cc07, cc07)
	LDF	[BO + 42 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 43 * SIZE], b4

	FMADD1	(aa5, bb5, cc09, cc09)
	LDF	[AO + 10 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO + 11 * SIZE], a4

	FMADD3	(aa5, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  8) * SIZE], APREFETCH_CATEGORY
	FMADD4	(aa2, bb6, cc12, cc12)
	nop

	FMADD1	(aa5, bb7, cc13, cc13)
	LDF	[BO + 44 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO + 45 * SIZE], b6

	FMADD3	(aa5, bb8, cc15, cc15)
	LDF	[BO + 46 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO + 47 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO + 56 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO + 49 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO + 50 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO + 51 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO + 12 * SIZE], a5
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO + 13 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	cmp	L, 0
	FMADD4	(aa4, bb6, cc12, cc12)
	nop

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO + 52 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO + 53 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO + 54 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	LDF	[BO + 55 * SIZE], b8

	FMADD1	(aa5, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa5, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa5, bb3, cc05, cc05)
	LDF	[BO + 64 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 57 * SIZE], b2

	FMADD3	(aa5, bb4, cc07, cc07)
	LDF	[BO + 58 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 59 * SIZE], b4

	FMADD1	(aa5, bb5, cc09, cc09)
	LDF	[AO + 14 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO + 15 * SIZE], a4

	FMADD3	(aa5, bb6, cc11, cc11)
	add	BO, 64 * SIZE, BO
	FMADD4	(aa2, bb6, cc12, cc12)
	add	AO, 16 * SIZE, AO

	FMADD1	(aa5, bb7, cc13, cc13)
	LDF	[BO -  4 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO -  3 * SIZE], b6

	FMADD3	(aa5, bb8, cc15, cc15)
	LDF	[BO -  2 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO -  1 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO +  8 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO +  1 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO +  2 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO +  3 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO +  8 * SIZE], a5  /****/
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO +  1 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	FMADD4	(aa4, bb6, cc12, cc12)

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO +  4 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO +  5 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO +  6 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	ble,pn	%icc, .LL15
	LDF	[BO +  7 * SIZE], b8

	FMADD1	(aa1, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa1, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa1, bb3, cc05, cc05)
	LDF	[BO + 16 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO +  9 * SIZE], b2

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD1	(aa1, bb5, cc09, cc09)
	LDF	[AO +  2 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO +  3 * SIZE], a4

	FMADD3	(aa1, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD4	(aa2, bb6, cc12, cc12)
	nop

	FMADD1	(aa1, bb7, cc13, cc13)
	LDF	[BO + 12 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO + 13 * SIZE], b6

	FMADD3	(aa1, bb8, cc15, cc15)
	LDF	[BO + 14 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO + 15 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO + 24 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO + 17 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO + 18 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO + 19 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO +  4 * SIZE], a1
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO +  5 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	add	L, -1, L
	FMADD4	(aa4, bb6, cc12, cc12)
	nop

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO + 20 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO + 21 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO + 22 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	LDF	[BO + 23 * SIZE], b8

	FMADD1	(aa1, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa1, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa1, bb3, cc05, cc05)
	LDF	[BO + 32 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 25 * SIZE], b2

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO + 26 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 27 * SIZE], b4

	FMADD1	(aa1, bb5, cc09, cc09)
	LDF	[AO +  6 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO +  7 * SIZE], a4

	FMADD3	(aa1, bb6, cc11, cc11)
	nop
	FMADD4	(aa2, bb6, cc12, cc12)
	nop

	FMADD1	(aa1, bb7, cc13, cc13)
	LDF	[BO + 28 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO + 29 * SIZE], b6

	FMADD3	(aa1, bb8, cc15, cc15)
	LDF	[BO + 30 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO + 31 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO + 40 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO + 33 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO + 34 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO + 35 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO + 16 * SIZE], a1  /****/
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO +  9 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	nop
	FMADD4	(aa4, bb6, cc12, cc12)
	nop

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO + 36 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO + 37 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO + 38 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	LDF	[BO + 39 * SIZE], b8

	FMADD1	(aa5, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa5, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa5, bb3, cc05, cc05)
	LDF	[BO + 48 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 41 * SIZE], b2

	FMADD3	(aa5, bb4, cc07, cc07)
	LDF	[BO + 42 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 43 * SIZE], b4

	FMADD1	(aa5, bb5, cc09, cc09)
	LDF	[AO + 10 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO + 11 * SIZE], a4

	FMADD3	(aa5, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  8) * SIZE], APREFETCH_CATEGORY
	FMADD4	(aa2, bb6, cc12, cc12)
	nop

	FMADD1	(aa5, bb7, cc13, cc13)
	LDF	[BO + 44 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO + 45 * SIZE], b6

	FMADD3	(aa5, bb8, cc15, cc15)
	LDF	[BO + 46 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO + 47 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO + 56 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO + 49 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO + 50 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO + 51 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO + 12 * SIZE], a5
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO + 13 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	cmp	L, 0
	FMADD4	(aa4, bb6, cc12, cc12)
	nop

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO + 52 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO + 53 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO + 54 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	LDF	[BO + 55 * SIZE], b8

	FMADD1	(aa5, bb1, cc01, cc01)
	FMADD2	(aa2, bb1, cc02, cc02)
	FMADD3	(aa5, bb2, cc03, cc03)
	FMADD4	(aa2, bb2, cc04, cc04)

	FMADD1	(aa5, bb3, cc05, cc05)
	LDF	[BO + 64 * SIZE], b1
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 57 * SIZE], b2

	FMADD3	(aa5, bb4, cc07, cc07)
	LDF	[BO + 58 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 59 * SIZE], b4

	FMADD1	(aa5, bb5, cc09, cc09)
	LDF	[AO + 14 * SIZE], a3
	FMADD2	(aa2, bb5, cc10, cc10)
	LDF	[AO + 15 * SIZE], a4

	FMADD3	(aa5, bb6, cc11, cc11)
	add	BO, 64 * SIZE, BO
	FMADD4	(aa2, bb6, cc12, cc12)
	add	AO, 16 * SIZE, AO

	FMADD1	(aa5, bb7, cc13, cc13)
	LDF	[BO -  4 * SIZE], b5
	FMADD2	(aa2, bb7, cc14, cc14)
	LDF	[BO -  3 * SIZE], b6

	FMADD3	(aa5, bb8, cc15, cc15)
	LDF	[BO -  2 * SIZE], b7
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[BO -  1 * SIZE], b8

	FMADD1	(aa3, bb9, cc01, cc01)
	FMADD2	(aa4, bb9, cc02, cc02)
	FMADD3	(aa3, bb2, cc03, cc03)
	FMADD4	(aa4, bb2, cc04, cc04)

	FMADD1	(aa3, bb3, cc05, cc05)
	LDF	[BO +  8 * SIZE], b9
	FMADD2	(aa4, bb3, cc06, cc06)
	LDF	[BO +  1 * SIZE], b2

	FMADD3	(aa3, bb4, cc07, cc07)
	LDF	[BO +  2 * SIZE], b3
	FMADD4	(aa4, bb4, cc08, cc08)
	LDF	[BO +  3 * SIZE], b4

	FMADD1	(aa3, bb5, cc09, cc09)
	LDF	[AO +  8 * SIZE], a5  /****/
	FMADD2	(aa4, bb5, cc10, cc10)
	LDF	[AO +  1 * SIZE], a2

	FMADD3	(aa3, bb6, cc11, cc11)
	FMADD4	(aa4, bb6, cc12, cc12)

	FMADD1	(aa3, bb7, cc13, cc13)
	LDF	[BO +  4 * SIZE], b5
	FMADD2	(aa4, bb7, cc14, cc14)
	LDF	[BO +  5 * SIZE], b6

	FMADD3	(aa3, bb8, cc15, cc15)
	LDF	[BO +  6 * SIZE], b7
	FMADD4	(aa4, bb8, cc16, cc16)
	bg,pt	%icc, .LL13
	LDF	[BO +  7 * SIZE], b8
	.align 4

.LL15:
#ifndef TRMMKERNEL
	and	K,  7, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 4, L
#endif
	and	L,  7, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL18
	nop
	.align 4

.LL17:
	FMADD1	(aa1, bb1, cc01, cc01)
	add	L, -1, L
	FMADD2	(aa2, bb1, cc02, cc02)
	nop

	FMADD3	(aa1, bb2, cc03, cc03)
	LDF	[BO +  8 * SIZE], b1
	FMADD4	(aa2, bb2, cc04, cc04)
	LDF	[BO +  9 * SIZE], b2

	FMADD1	(aa1, bb3, cc05, cc05)
	cmp	L, 0
	FMADD2	(aa2, bb3, cc06, cc06)
	nop

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD1	(aa1, bb5, cc09, cc09)
	nop
	FMADD2	(aa2, bb5, cc10, cc10)
	nop

	FMADD3	(aa1, bb6, cc11, cc11)
	LDF	[BO + 12 * SIZE], b5
	FMADD4	(aa2, bb6, cc12, cc12)
	LDF	[BO + 13 * SIZE], b6

	FMADD1	(aa1, bb7, cc13, cc13)
	add	AO, 2 * SIZE, AO
	FMADD2	(aa2, bb7, cc14, cc14)
	add	BO, 8 * SIZE, BO

	FMADD3	(aa1, bb8, cc15, cc15)
	LDF	[AO +  0 * SIZE], a1
	FMADD4	(aa2, bb8, cc16, cc16)
	LDF	[AO +  1 * SIZE], a2

	LDF	[BO +  6 * SIZE], b7
	bg,pt	%icc, .LL17
	LDF	[BO +  7 * SIZE], b8
	nop
	.align 4

.LL18:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	FADD	  c01, c04, c01
	LDF	[C1 + 1 * SIZE], a2
	FADD	  c02, c03, c02
	LDF	[C2 + 0 * SIZE], a3
	FADD	  c05, c08, c05
	LDF	[C2 + 1 * SIZE], a4
	FADD	  c06, c07, c06

	LDF	[C3 + 0 * SIZE], b1
	FADD	  c09, c12, c09
	LDF	[C3 + 1 * SIZE], b2
	FADD	  c10, c11, c10
	LDF	[C4 + 0 * SIZE], b3
	FADD	  c13, c16, c13
	LDF	[C4 + 1 * SIZE], b4
	FADD	  c14, c15, c14

	FMADD	(alpha_r, cc01, aa1, aa1)
	FMADD	(alpha_r, cc02, aa2, aa2)
	FMADD	(alpha_r, cc05, aa3, aa3)
	FMADD	(alpha_r, cc06, aa4, aa4)

	FMADD	(alpha_r, cc09, bb1, bb1)
	FMADD	(alpha_r, cc10, bb2, bb2)
	FMADD	(alpha_r, cc13, bb3, bb3)
	FMADD	(alpha_r, cc14, bb4, bb4)

#else
	FADD	c01, c04, c01
	FADD	c02, c03, c02
	FADD	c05, c08, c05
	FADD	c06, c07, c06

	FADD	c09, c12, c09
	FADD	c10, c11, c10
	FADD	c13, c16, c13
	FADD	c14, c15, c14

	FMUL	ALPHA_R, c01, a1
	FMUL	ALPHA_R, c02, a2
	FMUL	ALPHA_R, c05, a3
	FMUL	ALPHA_R, c06, a4

	FMUL	ALPHA_R, c09, b1
	FMUL	ALPHA_R, c10, b2
	FMUL	ALPHA_R, c13, b3
	FMUL	ALPHA_R, c14, b4
#endif

	FNMSUB	(alpha_i, cc02, aa1, aa1)
	FMADD	(alpha_i, cc01, aa2, aa2)
	FNMSUB	(alpha_i, cc06, aa3, aa3)
	FMADD	(alpha_i, cc05, aa4, aa4)

	FNMSUB	(alpha_i, cc10, bb1, bb1)
	STF	a1, [C1 + 0 * SIZE]
	FMADD	(alpha_i, cc09, bb2, bb2)
	STF	a2, [C1 + 1 * SIZE]
	FNMSUB	(alpha_i, cc14, bb3, bb3)
	STF	a3, [C2 + 0 * SIZE]
	FMADD	(alpha_i, cc13, bb4, bb4)
	STF	a4, [C2 + 1 * SIZE]

	STF	b1, [C3 + 0 * SIZE]
	add	C1, 2 * SIZE, C1
	STF	b2, [C3 + 1 * SIZE]
	add	C2, 2 * SIZE, C2
	STF	b3, [C4 + 0 * SIZE]
	add	C3, 2 * SIZE, C3
	STF	b4, [C4 + 1 * SIZE]
	add	C4, 2 * SIZE, C4

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -4, TEMP1
#endif
	sll	TEMP1, ZBASE_SHIFT + 0, TEMP2
	sll	TEMP1, ZBASE_SHIFT + 2, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL12
	nop

#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 4, KK
#endif

	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	mov	BO, B
	.align 4

.LL20:
	and	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL30
	mov	C,  C1

	add	C,  LDC, C2
	add	C2, LDC, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	mov	M, I
	mov	A, AO
	.align 4

.LL22:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, ZBASE_SHIFT + 0, TEMP1
	sll	KK, ZBASE_SHIFT + 1, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2

	LDF	[BO +  0 * SIZE], b1
	LDF	[BO +  1 * SIZE], b2
	LDF	[BO +  2 * SIZE], b3
	LDF	[BO +  3 * SIZE], b4
	LDF	[BO +  4 * SIZE], b5
	FCLR	(cc01)

	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc02)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc03)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc04)
	LDF	[BO +  8 * SIZE], b9
	FCLR	(cc05)

	prefetch [C1 + 2 * SIZE], 3
	FCLR	(cc06)
	prefetch [C2 + 2 * SIZE], 3
	FCLR	(cc07)

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 2, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL25
	FCLR	(cc08)
	.align 4

.LL23:
	FMADD1	(aa1, bb1, cc01, cc01)
	LDF	[AO +  2 * SIZE], a3
	FMADD2	(aa2, bb1, cc02, cc02)
	LDF	[AO +  3 * SIZE], a4

	FMADD3	(aa1, bb2, cc03, cc03)
	LDF	[BO + 16 * SIZE], b1
	FMADD4	(aa2, bb2, cc04, cc04)
	LDF	[BO +  9 * SIZE], b2

	FMADD1	(aa1, bb3, cc05, cc05)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD2	(aa2, bb3, cc06, cc06)
	add	L, -1, L

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD1	(aa3, bb5, cc01, cc01)
	LDF	[AO +  4 * SIZE], a1
	FMADD2	(aa4, bb5, cc02, cc02)
	LDF	[AO +  5 * SIZE], a2

	FMADD3	(aa3, bb6, cc03, cc03)
	LDF	[BO + 12 * SIZE], b5
	FMADD4	(aa4, bb6, cc04, cc04)
	LDF	[BO + 13 * SIZE], b6

	FMADD1	(aa3, bb7, cc05, cc05)
	cmp	L, 0
	FMADD2	(aa4, bb7, cc06, cc06)
	add	AO,  8 * SIZE, AO

	FMADD3	(aa3, bb8, cc07, cc07)
	LDF	[BO + 14 * SIZE], b7
	FMADD4	(aa4, bb8, cc08, cc08)
	LDF	[BO + 15 * SIZE], b8

	FMADD1	(aa1, bb9, cc01, cc01)
	LDF	[AO -  2 * SIZE], a3
	FMADD2	(aa2, bb9, cc02, cc02)
	LDF	[AO -  1 * SIZE], a4

	FMADD3	(aa1, bb2, cc03, cc03)
	LDF	[BO + 24 * SIZE], b9
	FMADD4	(aa2, bb2, cc04, cc04)
	LDF	[BO + 17 * SIZE], b2

	FMADD1	(aa1, bb3, cc05, cc05)
	add	BO, 16 * SIZE, BO
	FMADD2	(aa2, bb3, cc06, cc06)
	nop

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[BO +  2 * SIZE], b3
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[BO +  3 * SIZE], b4

	FMADD1	(aa3, bb5, cc01, cc01)
	LDF	[AO +  0 * SIZE], a1
	FMADD2	(aa4, bb5, cc02, cc02)
	LDF	[AO +  1 * SIZE], a2
	FMADD3	(aa3, bb6, cc03, cc03)
	LDF	[BO +  4 * SIZE], b5
	FMADD4	(aa4, bb6, cc04, cc04)
	LDF	[BO +  5 * SIZE], b6

	FMADD1	(aa3, bb7, cc05, cc05)
	nop
	FMADD2	(aa4, bb7, cc06, cc06)
	LDF	[BO +  6 * SIZE], b7

	FMADD3	(aa3, bb8, cc07, cc07)
	FMADD4	(aa4, bb8, cc08, cc08)
	bg,pt	%icc, .LL23
	LDF	[BO +  7 * SIZE], b8
	.align 4

.LL25:
#ifndef TRMMKERNEL
	and	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 2, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL28
	nop
	.align 4

.LL27:
	FMADD1	(aa1, bb1, cc01, cc01)
	add	L, -1, L
	FMADD2	(aa2, bb1, cc02, cc02)
	LDF	[BO + 4 * SIZE], b1

	FMADD3	(aa1, bb2, cc03, cc03)
	add	AO, 2 * SIZE, AO
	FMADD4	(aa2, bb2, cc04, cc04)
	LDF	[BO + 5 * SIZE], b2

	FMADD1	(aa1, bb3, cc05, cc05)
	cmp	L, 0
	FMADD2	(aa2, bb3, cc06, cc06)
	LDF	[BO + 6 * SIZE], b3

	FMADD3	(aa1, bb4, cc07, cc07)
	LDF	[AO + 0 * SIZE], a1
	FMADD4	(aa2, bb4, cc08, cc08)
	LDF	[AO + 1 * SIZE], a2

	LDF	[BO + 7 * SIZE], b4
	bg,pt	%icc, .LL27
	add	BO, 4 * SIZE, BO
	.align 4

.LL28:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	FADD	  c01, c04, c01
	LDF	[C1 + 1 * SIZE], a2
	FADD	  c02, c03, c02
	LDF	[C2 + 0 * SIZE], a3
	FADD	  c05, c08, c05
	LDF	[C2 + 1 * SIZE], a4
	FADD	  c06, c07, c06

	FMADD	(alpha_r, cc01, aa1, aa1)
	FMADD	(alpha_r, cc02, aa2, aa2)
	FMADD	(alpha_r, cc05, aa3, aa3)
	FMADD	(alpha_r, cc06, aa4, aa4)
#else
	FADD	  c01, c04, c01
	FADD	  c02, c03, c02
	FADD	  c05, c08, c05
	FADD	  c06, c07, c06

	FMUL	ALPHA_R, c01, a1
	FMUL	ALPHA_R, c02, a2
	FMUL	ALPHA_R, c05, a3
	FMUL	ALPHA_R, c06, a4
#endif

	FNMSUB	(alpha_i, cc02, aa1, aa1)
	FMADD	(alpha_i, cc01, aa2, aa2)
	FNMSUB	(alpha_i, cc06, aa3, aa3)
	FMADD	(alpha_i, cc05, aa4, aa4)

	STF	a1, [C1 + 0 * SIZE]
	add	I, -1, I
	STF	a2, [C1 + 1 * SIZE]
	cmp	I, 0
	STF	a3, [C2 + 0 * SIZE]
	add	C1, 2 * SIZE, C1
	STF	a4, [C2 + 1 * SIZE]
	add	C2, 2 * SIZE, C2

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, ZBASE_SHIFT + 0, TEMP2
	sll	TEMP1, ZBASE_SHIFT + 1, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif

	bg,pt	%icc, .LL22
	nop

#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 2, KK
#endif

	mov	BO, B
	.align 4

.LL30:
	and	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL999
	mov	C,  C1

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	mov	M, I
	mov	A, AO
	.align 4

.LL32:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, ZBASE_SHIFT + 0, TEMP1
	sll	KK, ZBASE_SHIFT + 0, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	LDF	[BO +  0 * SIZE], b1
	LDF	[BO +  1 * SIZE], b2
	LDF	[BO +  2 * SIZE], b3
	FCLR	(cc01)
	LDF	[BO +  3 * SIZE], b4
	FCLR	(cc02)

	LDF	[BO +  4 * SIZE], b5
	FCLR	(cc03)
	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc04)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc05)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc06)

	prefetch [C1 + 2 * SIZE], 3
	FCLR	(cc07)

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 1, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL35
	FCLR	(cc08)
	.align 4

.LL33:
	FMADD1	(aa1, bb1, cc01, cc01)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD2	(aa2, bb1, cc02, cc02)
	LDF	[BO +  8 * SIZE], b1

	FMADD3	(aa1, bb2, cc03, cc03)
	LDF	[AO +  4 * SIZE], a1
	FMADD4	(aa2, bb2, cc04, cc04)
	LDF	[AO +  5 * SIZE], a2

	FMADD1	(aa3, bb3, cc01, cc01)
	LDF	[BO +  9 * SIZE], b2
	FMADD2	(aa4, bb3, cc02, cc02)
	LDF	[BO + 10 * SIZE], b3

	FMADD3	(aa3, bb4, cc03, cc03)
	LDF	[AO +  6 * SIZE], a3
	FMADD4	(aa4, bb4, cc04, cc04)
	LDF	[AO +  7 * SIZE], a4

	FMADD1	(aa1, bb5, cc01, cc01)
	LDF	[BO + 11 * SIZE], b4
	FMADD2	(aa2, bb5, cc02, cc02)
	LDF	[BO + 12 * SIZE], b5

	FMADD3	(aa1, bb6, cc03, cc03)
	LDF	[AO +  8 * SIZE], a1
	FMADD4	(aa2, bb6, cc04, cc04)
	LDF	[AO +  9 * SIZE], a2

	FMADD1	(aa3, bb7, cc01, cc01)
	LDF	[BO + 13 * SIZE], b6

	FMADD2	(aa4, bb7, cc02, cc02)
	LDF	[BO + 14 * SIZE], b7

	FMADD3	(aa3, bb8, cc03, cc03)
	LDF	[AO + 10 * SIZE], a3
	FMADD4	(aa4, bb8, cc04, cc04)
	LDF	[AO + 11 * SIZE], a4

	add	AO,  8 * SIZE, AO
	add	L, -1, L
	add	BO,  8 * SIZE, BO
	cmp	L, 0

	bg,pt	%icc, .LL33
	LDF	[BO +  7 * SIZE], b8
	.align 4

.LL35:
#ifndef TRMMKERNEL
	and	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 1, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL38
	nop
	.align 4

.LL37:
	FMADD1	(aa1, bb1, cc01, cc01)
	add	L, -1, L
	FMADD2	(aa2, bb1, cc02, cc02)
	LDF	[BO + 2 * SIZE], b1

	FMADD3	(aa1, bb2, cc03, cc03)
	LDF	[AO + 2 * SIZE], a1
	FMADD4	(aa2, bb2, cc04, cc04)
	LDF	[AO + 3 * SIZE], a2

	add	AO, 2 * SIZE, AO
	cmp	L, 0
	add	BO, 2 * SIZE, BO
	bg,pt	%icc, .LL37
	LDF	[BO + 1 * SIZE], b2
	.align 4

.LL38:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	FADD	  c01, c04, c01
	LDF	[C1 + 1 * SIZE], a2
	FADD	  c02, c03, c02

	FMADD	(alpha_r, cc01, aa1, aa1)
	FMADD	(alpha_r, cc02, aa2, aa2)
#else
	FADD	  c01, c04, c01
	FADD	  c02, c03, c02

	FMUL	ALPHA_R, c01, a1
	FMUL	ALPHA_R, c02, a2
#endif

	FNMSUB	(alpha_i, cc02, aa1, aa1)
	FMADD	(alpha_i, cc01, aa2, aa2)

	STF	a1, [C1 + 0 * SIZE]
	STF	a2, [C1 + 1 * SIZE]

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif
	sll	TEMP1, ZBASE_SHIFT + 0, TEMP2
	sll	TEMP1, ZBASE_SHIFT + 0, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL32
	add	C1, 2 * SIZE, C1
	.align 4

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
