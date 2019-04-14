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

#define BB	%o7

#define C1	%o0
#define C2	%o1
#define C3	%o2
#define C4	%o3

#define C5	%l5
#define	C6	%l6
#define C7	%l7
#define C8	%i3

#define OFFSET	%g1
#define	KK	%g2
#define TEMP1	%g3
#define TEMP2	%g4

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

#define ALPHA	%f62

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

#define alpha	31
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

#define ALPHA	%f31

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

#define alpha	31

#endif

        .register %g2, #scratch
        .register %g3, #scratch

	PROLOGUE
	SAVESP
	nop

#ifndef __64BIT__

#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]

	ld	[%sp + STACK_START + 28], B
	ld	[%sp + STACK_START + 32], C
	ld	[%sp + STACK_START + 36], LDC
#ifdef TRMMKERNEL
	ld	[%sp + STACK_START + 40], OFFSET
#endif
#else
	st	%i3, [%sp + STACK_START + 16]

	ld	[%sp + STACK_START + 28], C
	ld	[%sp + STACK_START + 32], LDC
#ifdef TRMMKERNEL
	ld	[%sp + STACK_START + 36], OFFSET
#endif
#endif
	LDF	[%sp + STACK_START + 16], ALPHA
#ifdef TRMMKERNEL
	st	%g1, [%sp + STACK_START +  8]
	st	%g2, [%sp + STACK_START + 12]
	st	%g3, [%sp + STACK_START + 16]
	st	%g4, [%sp + STACK_START + 20]
#endif
#else

	ldx	[%sp+  STACK_START + 56], C
	ldx	[%sp+  STACK_START + 64], LDC
#ifdef TRMMKERNEL
	ldx	[%sp+  STACK_START + 72], OFFSET
#endif

#ifdef DOUBLE
	FMOV	%f6, ALPHA
#else
	FMOV	%f7, ALPHA
#endif

#ifdef TRMMKERNEL
	stx	%g1, [%sp + STACK_START + 32]
	stx	%g2, [%sp + STACK_START + 40]
	stx	%g3, [%sp + STACK_START + 48]
	stx	%g4, [%sp + STACK_START + 56]
#endif

#endif

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	OFFSET, KK
#endif

	sra	N, 3, J
	cmp	J, 0
	ble,pn	%icc, .LL30
	sll	LDC, BASE_SHIFT, LDC

.LL11:
	mov	C,  C1
	add	C,  LDC, C2
	add	C2, LDC, C3
	add	C3, LDC, C4
	add	C4, LDC, C5
	add	C5, LDC, C6
	add	C6, LDC, C7
	add	C7, LDC, C8
	add	C8, LDC, C

	sll	K, BASE_SHIFT + 3, BB

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	mov	A, AO

	sra	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL20
	add	B, BB, BB
	.align 4

.LL12:
	prefetch [BB +  0 * SIZE], 1

#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 1, TEMP1
	sll	KK, BASE_SHIFT + 3, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  8 * SIZE], a5

	LDF	[BO +  0 * SIZE], b1

	LDF	[BO +  1 * SIZE], b2
	FCLR	(cc01)
	LDF	[BO +  2 * SIZE], b3
	FCLR	(cc05)
	LDF	[BO +  3 * SIZE], b4
	FCLR	(cc09)
	LDF	[BO +  4 * SIZE], b5
	FCLR	(cc13)

	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc02)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc06)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc10)
	LDF	[BO +  8 * SIZE], b9
	FCLR	(cc14)

	prefetch [C1 + 1 * SIZE], 3
	FCLR	(cc03)
	prefetch [C2 + 2 * SIZE], 3
	FCLR	(cc07)
	prefetch [C3 + 1 * SIZE], 3
	FCLR	(cc11)
	prefetch [C4 + 2 * SIZE], 3
	FCLR	(cc15)

	prefetch [C5 + 1 * SIZE], 3
	FCLR	(cc04)
	prefetch [C6 + 2 * SIZE], 3
	FCLR	(cc08)
	prefetch [C7 + 1 * SIZE], 3
	FCLR	(cc12)
	prefetch [C8 + 2 * SIZE], 3
	FCLR	(cc16)

#ifndef TRMMKERNEL
	sra	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 8, L
#endif
	sra	L,  3, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL15
	add	 BB, 32 * SIZE, BB
	.align 4

.LL13:
	FMADD	(aa1, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa1, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 16 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO +  9 * SIZE], b2

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	LDF	[AO +  2 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO +  3 * SIZE], a4

	FMADD	(aa1, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD	(aa2, bb6, cc12, cc12)
	nop

	FMADD	(aa1, bb7, cc13, cc13)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[BO + 14 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO + 15 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 24 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO + 17 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 18 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO + 19 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO +  4 * SIZE], a1
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO +  5 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	add	L, -1, L
	FMADD	(aa4, bb6, cc12, cc12)
	nop

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 20 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO + 21 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 22 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	LDF	[BO + 23 * SIZE], b8

	FMADD	(aa1, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa1, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 32 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 25 * SIZE], b2

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 26 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 27 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	LDF	[AO +  6 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO +  7 * SIZE], a4

	FMADD	(aa1, bb6, cc11, cc11)
	nop
	FMADD	(aa2, bb6, cc12, cc12)
	nop

	FMADD	(aa1, bb7, cc13, cc13)
	LDF	[BO + 28 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO + 29 * SIZE], b6

	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[BO + 30 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO + 31 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 40 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO + 33 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 34 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO + 35 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO + 16 * SIZE], a1  /****/
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO +  9 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	nop
	FMADD	(aa4, bb6, cc12, cc12)
	nop

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 36 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO + 37 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 38 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	LDF	[BO + 39 * SIZE], b8

	FMADD	(aa5, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa5, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa5, bb3, cc05, cc05)
	LDF	[BO + 48 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 41 * SIZE], b2

	FMADD	(aa5, bb4, cc07, cc07)
	LDF	[BO + 42 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 43 * SIZE], b4

	FMADD	(aa5, bb5, cc09, cc09)
	LDF	[AO + 10 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO + 11 * SIZE], a4

	FMADD	(aa5, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  8) * SIZE], APREFETCH_CATEGORY
	FMADD	(aa2, bb6, cc12, cc12)
	nop

	FMADD	(aa5, bb7, cc13, cc13)
	LDF	[BO + 44 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO + 45 * SIZE], b6

	FMADD	(aa5, bb8, cc15, cc15)
	LDF	[BO + 46 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO + 47 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 56 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO + 49 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 50 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO + 51 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO + 12 * SIZE], a5
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO + 13 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	cmp	L, 0
	FMADD	(aa4, bb6, cc12, cc12)
	nop

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 52 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO + 53 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 54 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	LDF	[BO + 55 * SIZE], b8

	FMADD	(aa5, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa5, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa5, bb3, cc05, cc05)
	LDF	[BO + 64 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 57 * SIZE], b2

	FMADD	(aa5, bb4, cc07, cc07)
	LDF	[BO + 58 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 59 * SIZE], b4

	FMADD	(aa5, bb5, cc09, cc09)
	LDF	[AO + 14 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO + 15 * SIZE], a4

	FMADD	(aa5, bb6, cc11, cc11)
	add	BO, 64 * SIZE, BO
	FMADD	(aa2, bb6, cc12, cc12)
	add	AO, 16 * SIZE, AO

	FMADD	(aa5, bb7, cc13, cc13)
	LDF	[BO -  4 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO -  3 * SIZE], b6

	FMADD	(aa5, bb8, cc15, cc15)
	LDF	[BO -  2 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO -  1 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO +  8 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO +  1 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO +  2 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO +  3 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO +  8 * SIZE], a5  /****/
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO +  1 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	FMADD	(aa4, bb6, cc12, cc12)

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO +  4 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO +  5 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO +  6 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	ble,pn	%icc, .LL15
	LDF	[BO +  7 * SIZE], b8

	FMADD	(aa1, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa1, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 16 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO +  9 * SIZE], b2

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	LDF	[AO +  2 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO +  3 * SIZE], a4

	FMADD	(aa1, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD	(aa2, bb6, cc12, cc12)
	nop

	FMADD	(aa1, bb7, cc13, cc13)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[BO + 14 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO + 15 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 24 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO + 17 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 18 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO + 19 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO +  4 * SIZE], a1
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO +  5 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	add	L, -1, L
	FMADD	(aa4, bb6, cc12, cc12)
	nop

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 20 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO + 21 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 22 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	LDF	[BO + 23 * SIZE], b8

	FMADD	(aa1, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa1, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 32 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 25 * SIZE], b2

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 26 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 27 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	LDF	[AO +  6 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO +  7 * SIZE], a4

	FMADD	(aa1, bb6, cc11, cc11)
	nop
	FMADD	(aa2, bb6, cc12, cc12)
	nop

	FMADD	(aa1, bb7, cc13, cc13)
	LDF	[BO + 28 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO + 29 * SIZE], b6

	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[BO + 30 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO + 31 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 40 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO + 33 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 34 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO + 35 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO + 16 * SIZE], a1  /****/
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO +  9 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	nop
	FMADD	(aa4, bb6, cc12, cc12)
	nop

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 36 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO + 37 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 38 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	LDF	[BO + 39 * SIZE], b8

	FMADD	(aa5, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa5, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa5, bb3, cc05, cc05)
	LDF	[BO + 48 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 41 * SIZE], b2

	FMADD	(aa5, bb4, cc07, cc07)
	LDF	[BO + 42 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 43 * SIZE], b4

	FMADD	(aa5, bb5, cc09, cc09)
	LDF	[AO + 10 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO + 11 * SIZE], a4

	FMADD	(aa5, bb6, cc11, cc11)
	prefetch [AO + (APREFETCHSIZE +  8) * SIZE], APREFETCH_CATEGORY
	FMADD	(aa2, bb6, cc12, cc12)
	nop

	FMADD	(aa5, bb7, cc13, cc13)
	LDF	[BO + 44 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO + 45 * SIZE], b6

	FMADD	(aa5, bb8, cc15, cc15)
	LDF	[BO + 46 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO + 47 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 56 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO + 49 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 50 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO + 51 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO + 12 * SIZE], a5
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO + 13 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	cmp	L, 0
	FMADD	(aa4, bb6, cc12, cc12)
	nop

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 52 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO + 53 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 54 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
	LDF	[BO + 55 * SIZE], b8

	FMADD	(aa5, bb1, cc01, cc01)
	FMADD	(aa2, bb1, cc02, cc02)
	FMADD	(aa5, bb2, cc03, cc03)
	FMADD	(aa2, bb2, cc04, cc04)

	FMADD	(aa5, bb3, cc05, cc05)
	LDF	[BO + 64 * SIZE], b1
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 57 * SIZE], b2

	FMADD	(aa5, bb4, cc07, cc07)
	LDF	[BO + 58 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 59 * SIZE], b4

	FMADD	(aa5, bb5, cc09, cc09)
	LDF	[AO + 14 * SIZE], a3
	FMADD	(aa2, bb5, cc10, cc10)
	LDF	[AO + 15 * SIZE], a4

	FMADD	(aa5, bb6, cc11, cc11)
	add	BO, 64 * SIZE, BO
	FMADD	(aa2, bb6, cc12, cc12)
	add	AO, 16 * SIZE, AO

	FMADD	(aa5, bb7, cc13, cc13)
	LDF	[BO -  4 * SIZE], b5
	FMADD	(aa2, bb7, cc14, cc14)
	LDF	[BO -  3 * SIZE], b6

	FMADD	(aa5, bb8, cc15, cc15)
	LDF	[BO -  2 * SIZE], b7
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[BO -  1 * SIZE], b8

	FMADD	(aa3, bb9, cc01, cc01)
	FMADD	(aa4, bb9, cc02, cc02)
	FMADD	(aa3, bb2, cc03, cc03)
	FMADD	(aa4, bb2, cc04, cc04)

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO +  8 * SIZE], b9
	FMADD	(aa4, bb3, cc06, cc06)
	LDF	[BO +  1 * SIZE], b2

	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO +  2 * SIZE], b3
	FMADD	(aa4, bb4, cc08, cc08)
	LDF	[BO +  3 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[AO +  8 * SIZE], a5  /****/
	FMADD	(aa4, bb5, cc10, cc10)
	LDF	[AO +  1 * SIZE], a2

	FMADD	(aa3, bb6, cc11, cc11)
	FMADD	(aa4, bb6, cc12, cc12)

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO +  4 * SIZE], b5
	FMADD	(aa4, bb7, cc14, cc14)
	LDF	[BO +  5 * SIZE], b6

	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO +  6 * SIZE], b7
	FMADD	(aa4, bb8, cc16, cc16)
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
	add	KK, 2, L
#else
	add	KK, 8, L
#endif
	and	L,  7, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL18
	nop
	.align 4

.LL17:
	FMADD	(aa1, bb1, cc01, cc01)
	add	L, -1, L
	FMADD	(aa2, bb1, cc02, cc02)
	nop

	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO +  8 * SIZE], b1
	FMADD	(aa2, bb2, cc04, cc04)
	LDF	[BO +  9 * SIZE], b2

	FMADD	(aa1, bb3, cc05, cc05)
	cmp	L, 0
	FMADD	(aa2, bb3, cc06, cc06)
	nop

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	nop
	FMADD	(aa2, bb5, cc10, cc10)
	nop

	FMADD	(aa1, bb6, cc11, cc11)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa2, bb6, cc12, cc12)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa1, bb7, cc13, cc13)
	add	AO, 2 * SIZE, AO
	FMADD	(aa2, bb7, cc14, cc14)
	add	BO, 8 * SIZE, BO

	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[AO +  0 * SIZE], a1
	FMADD	(aa2, bb8, cc16, cc16)
	LDF	[AO +  1 * SIZE], a2

	LDF	[BO +  6 * SIZE], b7
	bg,pt	%icc, .LL17
	LDF	[BO +  7 * SIZE], b8
	nop
	.align 4

.LL18:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C1 + 1 * SIZE], a2
	LDF	[C2 + 0 * SIZE], a3
	LDF	[C2 + 1 * SIZE], a4

	LDF	[C3 + 0 * SIZE], b1
	LDF	[C3 + 1 * SIZE], b2
	LDF	[C4 + 0 * SIZE], b3
	LDF	[C4 + 1 * SIZE], b4

	FMADD	(alpha, cc01, aa1, cc01)
	LDF	[C5 + 0 * SIZE], a1
	FMADD	(alpha, cc02, aa2, cc02)
	LDF	[C5 + 1 * SIZE], a2
	FMADD	(alpha, cc03, aa3, cc03)
	LDF	[C6 + 0 * SIZE], a3
	FMADD	(alpha, cc04, aa4, cc04)
	LDF	[C6 + 1 * SIZE], a4

	FMADD	(alpha, cc05, bb1, cc05)
	LDF	[C7 + 0 * SIZE], b1
	FMADD	(alpha, cc06, bb2, cc06)
	LDF	[C7 + 1 * SIZE], b2
	FMADD	(alpha, cc07, bb3, cc07)
	LDF	[C8 + 0 * SIZE], b3
	FMADD	(alpha, cc08, bb4, cc08)
	LDF	[C8 + 1 * SIZE], b4

	FMADD	(alpha, cc09, aa1, cc09)
	STF	c01, [C1 + 0 * SIZE]
	FMADD	(alpha, cc10, aa2, cc10)
	STF	c02, [C1 + 1 * SIZE]
	FMADD	(alpha, cc11, aa3, cc11)
	STF	c03, [C2 + 0 * SIZE]
	FMADD	(alpha, cc12, aa4, cc12)
	STF	c04, [C2 + 1 * SIZE]

	FMADD	(alpha, cc13, bb1, cc13)
	STF	c05, [C3 + 0 * SIZE]
	FMADD	(alpha, cc14, bb2, cc14)
	STF	c06, [C3 + 1 * SIZE]
	FMADD	(alpha, cc15, bb3, cc15)
	STF	c07, [C4 + 0 * SIZE]
	FMADD	(alpha, cc16, bb4, cc16)
	STF	c08, [C4 + 1 * SIZE]

#else
	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c02, c02
	FMUL	ALPHA, c03, c03
	FMUL	ALPHA, c04, c04

	FMUL	ALPHA, c05, c05
	FMUL	ALPHA, c06, c06
	FMUL	ALPHA, c07, c07
	FMUL	ALPHA, c08, c08

	FMUL	ALPHA, c09, c09
	STF	c01, [C1 + 0 * SIZE]
	FMUL	ALPHA, c10, c10
	STF	c02, [C1 + 1 * SIZE]
	FMUL	ALPHA, c11, c11
	STF	c03, [C2 + 0 * SIZE]
	FMUL	ALPHA, c12, c12
	STF	c04, [C2 + 1 * SIZE]

	FMUL	ALPHA, c13, c13
	STF	c05, [C3 + 0 * SIZE]
	FMUL	ALPHA, c14, c14
	STF	c06, [C3 + 1 * SIZE]
	FMUL	ALPHA, c15, c15
	STF	c07, [C4 + 0 * SIZE]
	FMUL	ALPHA, c16, c16
	STF	c08, [C4 + 1 * SIZE]
#endif

	STF	c09, [C5 + 0 * SIZE]
	add	C1, 2 * SIZE, C1
	STF	c10, [C5 + 1 * SIZE]
	add	C2, 2 * SIZE, C2
	STF	c11, [C6 + 0 * SIZE]
	add	C3, 2 * SIZE, C3
	STF	c12, [C6 + 1 * SIZE]
	add	C4, 2 * SIZE, C4

	STF	c13, [C7 + 0 * SIZE]
	add	C5, 2 * SIZE, C5
	STF	c14, [C7 + 1 * SIZE]
	add	C6, 2 * SIZE, C6
	STF	c15, [C8 + 0 * SIZE]
	add	C7, 2 * SIZE, C7
	STF	c16, [C8 + 1 * SIZE]
	add	C8, 2 * SIZE, C8

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -8, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 1, TEMP2
	sll	TEMP1, BASE_SHIFT + 3, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL12
	nop
	.align 4

.LL20:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL29
	nop

#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 0, TEMP1
	sll	KK, BASE_SHIFT + 3, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

	LDF	[AO +  0 * SIZE], a1
	LDF	[AO +  1 * SIZE], a2
	LDF	[AO +  2 * SIZE], a3
	LDF	[AO +  3 * SIZE], a4

	LDF	[BO +  0 * SIZE], b1
	FCLR	(cc01)
	LDF	[BO +  1 * SIZE], b2
	FCLR	(cc03)
	LDF	[BO +  2 * SIZE], b3
	FCLR	(cc05)
	LDF	[BO +  3 * SIZE], b4
	FCLR	(cc07)
	LDF	[BO +  4 * SIZE], b5
	FCLR	(cc09)
	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc11)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc13)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc15)

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 8, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL25
	LDF	[BO +  8 * SIZE], b9
	.align 4

.LL23:
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	add	L, -1, L

	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[BO + 16 * SIZE], b1
	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO +  9 * SIZE], b2

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 11 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa1, bb6, cc11, cc11)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa1, bb7, cc13, cc13)
	LDF	[BO + 14 * SIZE], b7
	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[BO + 15 * SIZE], b8

	FMADD	(aa2, bb9, cc01, cc01)
	LDF	[BO + 24 * SIZE], b9
	FMADD	(aa2, bb2, cc03, cc03)
	LDF	[BO + 17 * SIZE], b2

	FMADD	(aa2, bb3, cc05, cc05)
	LDF	[BO + 18 * SIZE], b3
	FMADD	(aa2, bb4, cc07, cc07)
	LDF	[BO + 19 * SIZE], b4

	FMADD	(aa2, bb5, cc09, cc09)
	LDF	[BO + 20 * SIZE], b5
	FMADD	(aa2, bb6, cc11, cc11)
	LDF	[BO + 21 * SIZE], b6

	FMADD	(aa2, bb7, cc13, cc13)
	LDF	[BO + 22 * SIZE], b7
	FMADD	(aa2, bb8, cc15, cc15)
	LDF	[BO + 23 * SIZE], b8

	LDF	[AO +  4 * SIZE], a1
	LDF	[AO +  5 * SIZE], a2

	FMADD	(aa3, bb1, cc01, cc01)
	LDF	[BO + 32 * SIZE], b1
	FMADD	(aa3, bb2, cc03, cc03)
	LDF	[BO + 25 * SIZE], b2

	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 26 * SIZE], b3
	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 27 * SIZE], b4

	FMADD	(aa3, bb5, cc09, cc09)
	LDF	[BO + 28 * SIZE], b5
	FMADD	(aa3, bb6, cc11, cc11)
	LDF	[BO + 29 * SIZE], b6

	FMADD	(aa3, bb7, cc13, cc13)
	LDF	[BO + 30 * SIZE], b7
	FMADD	(aa3, bb8, cc15, cc15)
	LDF	[BO + 31 * SIZE], b8

	FMADD	(aa4, bb9, cc01, cc01)
	LDF	[BO + 40 * SIZE], b9
	FMADD	(aa4, bb2, cc03, cc03)
	LDF	[BO + 33 * SIZE], b2

	FMADD	(aa4, bb3, cc05, cc05)
	LDF	[BO + 34 * SIZE], b3
	FMADD	(aa4, bb4, cc07, cc07)
	LDF	[BO + 35 * SIZE], b4

	FMADD	(aa4, bb5, cc09, cc09)
	LDF	[BO + 36 * SIZE], b5
	FMADD	(aa4, bb6, cc11, cc11)
	LDF	[BO + 37 * SIZE], b6

	FMADD	(aa4, bb7, cc13, cc13)
	LDF	[BO + 38 * SIZE], b7
	FMADD	(aa4, bb8, cc15, cc15)
	LDF	[BO + 39 * SIZE], b8

	LDF	[AO +  6 * SIZE], a3
	LDF	[AO +  7 * SIZE], a4

	add	AO,  4 * SIZE, AO
	cmp	L, 0
	bg,pt	%icc, .LL23
	add	BO, 32 * SIZE, BO
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
	add	KK, 8, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL28
	nop
	.align 4

.LL27:
	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[BO +  8 * SIZE], b1
	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO +  9 * SIZE], b2

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 11 * SIZE], b4

	FMADD	(aa1, bb5, cc09, cc09)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa1, bb6, cc11, cc11)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa1, bb7, cc13, cc13)
	LDF	[BO + 14 * SIZE], b7
	FMADD	(aa1, bb8, cc15, cc15)
	LDF	[BO + 15 * SIZE], b8

	LDF	[AO +  1 * SIZE], a1
	add	AO, 1 * SIZE, AO

	add	L, -1, L
	cmp	L, 0
	bg,pt	%icc, .LL27
	add	BO, 8 * SIZE, BO
	.align 4

.LL28:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C2 + 0 * SIZE], a2
	LDF	[C3 + 0 * SIZE], a3
	LDF	[C4 + 0 * SIZE], a4

	FMADD	(alpha, cc01, aa1, cc01)
	LDF	[C5 + 0 * SIZE], b1
	FMADD	(alpha, cc03, aa2, cc03)
	LDF	[C6 + 0 * SIZE], b2

	FMADD	(alpha, cc05, aa3, cc05)
	LDF	[C7 + 0 * SIZE], b3
	FMADD	(alpha, cc07, aa4, cc07)
	LDF	[C8 + 0 * SIZE], b4

	FMADD	(alpha, cc09, bb1, cc09)
	STF	c01, [C1 + 0 * SIZE]
	FMADD	(alpha, cc11, bb2, cc11)
	STF	c03, [C2 + 0 * SIZE]
	FMADD	(alpha, cc13, bb3, cc13)
	STF	c05, [C3 + 0 * SIZE]
	FMADD	(alpha, cc15, bb4, cc15)
	STF	c07, [C4 + 0 * SIZE]
#else
	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c03, c03
	FMUL	ALPHA, c05, c05
	FMUL	ALPHA, c07, c07

	FMUL	ALPHA, c09, c09
	STF	c01, [C1 + 0 * SIZE]
	FMUL	ALPHA, c11, c11
	STF	c03, [C2 + 0 * SIZE]

	FMUL	ALPHA, c13, c13
	STF	c05, [C3 + 0 * SIZE]
	FMUL	ALPHA, c15, c15
	STF	c07, [C4 + 0 * SIZE]
#endif

	STF	c09, [C5 + 0 * SIZE]
	STF	c11, [C6 + 0 * SIZE]
	STF	c13, [C7 + 0 * SIZE]
	STF	c15, [C8 + 0 * SIZE]

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -8, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 0, TEMP2
	sll	TEMP1, BASE_SHIFT + 3, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif
	.align 4

.LL29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 8, KK
#endif

	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	mov	BO, B
	.align 4

.LL30:
	and	N, 4, J
	cmp	J, 0
	ble,pn	%icc, .LL50
	mov	C,  C1

	add	C,  LDC, C2
	add	C2, LDC, C3
	add	C3, LDC, C4
	add	C4, LDC, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	sra	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL40
	mov	A, AO
	.align 4

.LL32:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 1, TEMP1
	sll	KK, BASE_SHIFT + 2, TEMP2

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

	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc01)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc02)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc03)
	LDF	[BO +  8 * SIZE], b9
	FCLR	(cc04)

	prefetch [C1 + 2 * SIZE], 3
	FCLR	(cc05)
	prefetch [C2 + 2 * SIZE], 3
	FCLR	(cc06)
	prefetch [C3 + 2 * SIZE], 3
	FCLR	(cc07)
	prefetch [C4 + 2 * SIZE], 3
	FCLR	(cc08)

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 4, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL35
	nop
	.align 4

.LL33:
	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[AO +  2 * SIZE], a3
	FMADD	(aa2, bb1, cc02, cc02)
	LDF	[AO +  3 * SIZE], a4

	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO + 16 * SIZE], b1
	FMADD	(aa2, bb2, cc04, cc04)
	LDF	[BO +  9 * SIZE], b2

	FMADD	(aa1, bb3, cc05, cc05)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD	(aa2, bb3, cc06, cc06)
	add	L, -1, L

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO + 11 * SIZE], b4

	FMADD	(aa3, bb5, cc01, cc01)
	LDF	[AO +  4 * SIZE], a1
	FMADD	(aa4, bb5, cc02, cc02)
	LDF	[AO +  5 * SIZE], a2

	FMADD	(aa3, bb6, cc03, cc03)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa4, bb6, cc04, cc04)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa3, bb7, cc05, cc05)
	cmp	L, 0
	FMADD	(aa4, bb7, cc06, cc06)
	add	AO,  8 * SIZE, AO

	FMADD	(aa3, bb8, cc07, cc07)
	LDF	[BO + 14 * SIZE], b7
	FMADD	(aa4, bb8, cc08, cc08)
	LDF	[BO + 15 * SIZE], b8

	FMADD	(aa1, bb9, cc01, cc01)
	LDF	[AO -  2 * SIZE], a3
	FMADD	(aa2, bb9, cc02, cc02)
	LDF	[AO -  1 * SIZE], a4

	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO + 24 * SIZE], b9
	FMADD	(aa2, bb2, cc04, cc04)
	LDF	[BO + 17 * SIZE], b2

	FMADD	(aa1, bb3, cc05, cc05)
	add	BO, 16 * SIZE, BO
	FMADD	(aa2, bb3, cc06, cc06)
	nop

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO +  2 * SIZE], b3
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[BO +  3 * SIZE], b4

	FMADD	(aa3, bb5, cc01, cc01)
	LDF	[AO +  0 * SIZE], a1
	FMADD	(aa4, bb5, cc02, cc02)
	LDF	[AO +  1 * SIZE], a2
	FMADD	(aa3, bb6, cc03, cc03)
	LDF	[BO +  4 * SIZE], b5
	FMADD	(aa4, bb6, cc04, cc04)
	LDF	[BO +  5 * SIZE], b6

	FMADD	(aa3, bb7, cc05, cc05)
	nop
	FMADD	(aa4, bb7, cc06, cc06)
	LDF	[BO +  6 * SIZE], b7

	FMADD	(aa3, bb8, cc07, cc07)
	FMADD	(aa4, bb8, cc08, cc08)
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
	add	KK, 2, L
#else
	add	KK, 4, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL38
	nop
	.align 4

.LL37:

	FMADD	(aa1, bb1, cc01, cc01)
	add	L, -1, L
	FMADD	(aa2, bb1, cc02, cc02)
	LDF	[BO + 4 * SIZE], b1

	FMADD	(aa1, bb2, cc03, cc03)
	add	AO, 2 * SIZE, AO
	FMADD	(aa2, bb2, cc04, cc04)
	LDF	[BO + 5 * SIZE], b2

	FMADD	(aa1, bb3, cc05, cc05)
	cmp	L, 0
	FMADD	(aa2, bb3, cc06, cc06)
	LDF	[BO + 6 * SIZE], b3

	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[AO + 0 * SIZE], a1
	FMADD	(aa2, bb4, cc08, cc08)
	LDF	[AO + 1 * SIZE], a2

	LDF	[BO + 7 * SIZE], b4
	bg,pt	%icc, .LL37
	add	BO, 4 * SIZE, BO
	.align 4

.LL38:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C1 + 1 * SIZE], a2
	LDF	[C2 + 0 * SIZE], a3
	LDF	[C2 + 1 * SIZE], a4

	FMADD	(alpha, cc01, aa1, cc01)
	LDF	[C3 + 0 * SIZE], b1
	FMADD	(alpha, cc02, aa2, cc02)
	LDF	[C3 + 1 * SIZE], b2
	FMADD	(alpha, cc03, aa3, cc03)
	LDF	[C4 + 0 * SIZE], b3
	FMADD	(alpha, cc04, aa4, cc04)
	LDF	[C4 + 1 * SIZE], b4

	FMADD	(alpha, cc05, bb1, cc05)
	STF	c01, [C1 + 0 * SIZE]
	FMADD	(alpha, cc06, bb2, cc06)
	STF	c02, [C1 + 1 * SIZE]
	FMADD	(alpha, cc07, bb3, cc07)
	STF	c03, [C2 + 0 * SIZE]
	FMADD	(alpha, cc08, bb4, cc08)
	STF	c04, [C2 + 1 * SIZE]
#else

	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c02, c02
	FMUL	ALPHA, c03, c03
	FMUL	ALPHA, c04, c04

	FMUL	ALPHA, c05, c05
	STF	c01, [C1 + 0 * SIZE]
	FMUL	ALPHA, c06, c06
	STF	c02, [C1 + 1 * SIZE]
	FMUL	ALPHA, c07, c07
	STF	c03, [C2 + 0 * SIZE]
	FMUL	ALPHA, c08, c08
	STF	c04, [C2 + 1 * SIZE]
#endif

	STF	c05, [C3 + 0 * SIZE]
	add	C1, 2 * SIZE, C1
	STF	c06, [C3 + 1 * SIZE]
	add	C2, 2 * SIZE, C2
	STF	c07, [C4 + 0 * SIZE]
	add	C3, 2 * SIZE, C3
	STF	c08, [C4 + 1 * SIZE]
	add	C4, 2 * SIZE, C4

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -4, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 1, TEMP2
	sll	TEMP1, BASE_SHIFT + 2, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL32
	nop

.LL40:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL49
	nop

#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 0, TEMP1
	sll	KK, BASE_SHIFT + 2, TEMP2

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
	LDF	[BO +  3 * SIZE], b4
	LDF	[BO +  4 * SIZE], b5
	LDF	[BO +  5 * SIZE], b6
	FCLR	(cc01)
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc03)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc05)
	LDF	[BO +  8 * SIZE], b9
	FCLR	(cc07)

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 4, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL45
	nop

.LL43:
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	add	L, -1, L

	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[BO + 16 * SIZE], b1
	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO +  9 * SIZE], b2
	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 11 * SIZE], b4

	LDF	[AO +  4 * SIZE], a1
	cmp	L, 0

	FMADD	(aa2, bb5, cc01, cc01)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa2, bb6, cc03, cc03)
	LDF	[BO + 13 * SIZE], b6
	FMADD	(aa2, bb7, cc05, cc05)
	LDF	[BO + 14 * SIZE], b7
	FMADD	(aa2, bb8, cc07, cc07)
	LDF	[BO + 15 * SIZE], b8

	LDF	[AO +  5 * SIZE], a2
	add	AO,  4 * SIZE, AO

	FMADD	(aa3, bb9, cc01, cc01)
	LDF	[BO + 24 * SIZE], b9
	FMADD	(aa3, bb2, cc03, cc03)
	LDF	[BO + 17 * SIZE], b2
	FMADD	(aa3, bb3, cc05, cc05)
	LDF	[BO + 18 * SIZE], b3
	FMADD	(aa3, bb4, cc07, cc07)
	LDF	[BO + 19 * SIZE], b4

	LDF	[AO +  2 * SIZE], a3
	add	BO, 16 * SIZE, BO

	FMADD	(aa4, bb5, cc01, cc01)
	LDF	[BO +  4 * SIZE], b5
	FMADD	(aa4, bb6, cc03, cc03)
	LDF	[BO +  5 * SIZE], b6
	FMADD	(aa4, bb7, cc05, cc05)
	LDF	[BO +  6 * SIZE], b7
	FMADD	(aa4, bb8, cc07, cc07)
	LDF	[BO +  7 * SIZE], b8

	bg,pt	%icc, .LL43
	LDF	[AO +  3 * SIZE], a4
	.align 4

.LL45:
#ifndef TRMMKERNEL
	and	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 4, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL48
	nop
	.align 4

.LL47:
	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[BO + 4 * SIZE], b1
	add	L, -1, L
	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO + 5 * SIZE], b2
	add	AO, 1 * SIZE, AO

	FMADD	(aa1, bb3, cc05, cc05)
	LDF	[BO + 6 * SIZE], b3
	cmp	L, 0
	FMADD	(aa1, bb4, cc07, cc07)
	LDF	[BO + 7 * SIZE], b4
	add	BO, 4 * SIZE, BO

	bg,pt	%icc, .LL47
	LDF	[AO + 0 * SIZE], a1
	.align 4

.LL48:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C2 + 0 * SIZE], a2
	LDF	[C3 + 0 * SIZE], a3
	LDF	[C4 + 0 * SIZE], a4

	FMADD	(alpha, cc01, aa1, cc01)
	FMADD	(alpha, cc03, aa2, cc03)
	FMADD	(alpha, cc05, aa3, cc05)
	FMADD	(alpha, cc07, aa4, cc07)
#else
	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c03, c03
	FMUL	ALPHA, c05, c05
	FMUL	ALPHA, c07, c07
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c03, [C2 + 0 * SIZE]
	STF	c05, [C3 + 0 * SIZE]
	STF	c07, [C4 + 0 * SIZE]

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -4, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 0, TEMP2
	sll	TEMP1, BASE_SHIFT + 2, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif
	.align 4

.LL49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 4, KK
#endif
	mov	BO, B
	.align 4

.LL50:
	and	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL70
	mov	C,  C1

	add	C,  LDC, C2
	add	C2, LDC, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	sra	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL60
	mov	A, AO
	.align 4

.LL52:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 1, TEMP1
	sll	KK, BASE_SHIFT + 1, TEMP2

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
	prefetch [C2 + 2 * SIZE], 3
	FCLR	(cc08)

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 2, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL55
	nop
	.align 4

.LL53:
	FMADD	(aa1, bb1, cc01, cc01)
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	FMADD	(aa2, bb1, cc02, cc02)
	LDF	[BO +  8 * SIZE], b1

	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[AO +  4 * SIZE], a1
	FMADD	(aa2, bb2, cc04, cc04)
	LDF	[AO +  5 * SIZE], a2

	FMADD	(aa3, bb3, cc01, cc01)
	LDF	[BO +  9 * SIZE], b2
	FMADD	(aa4, bb3, cc02, cc02)
	LDF	[BO + 10 * SIZE], b3

	FMADD	(aa3, bb4, cc03, cc03)
	LDF	[AO +  6 * SIZE], a3
	FMADD	(aa4, bb4, cc04, cc04)
	LDF	[AO +  7 * SIZE], a4

	FMADD	(aa1, bb5, cc01, cc01)
	LDF	[BO + 11 * SIZE], b4
	FMADD	(aa2, bb5, cc02, cc02)
	LDF	[BO + 12 * SIZE], b5

	FMADD	(aa1, bb6, cc03, cc03)
	LDF	[AO +  8 * SIZE], a1
	FMADD	(aa2, bb6, cc04, cc04)
	LDF	[AO +  9 * SIZE], a2

	FMADD	(aa3, bb7, cc01, cc01)
	LDF	[BO + 13 * SIZE], b6

	FMADD	(aa4, bb7, cc02, cc02)
	LDF	[BO + 14 * SIZE], b7

	FMADD	(aa3, bb8, cc03, cc03)
	LDF	[AO + 10 * SIZE], a3
	FMADD	(aa4, bb8, cc04, cc04)
	LDF	[AO + 11 * SIZE], a4

	add	AO,  8 * SIZE, AO
	add	L, -1, L
	add	BO,  8 * SIZE, BO
	cmp	L, 0

	bg,pt	%icc, .LL53
	LDF	[BO +  7 * SIZE], b8
	.align 4

.LL55:
#ifndef TRMMKERNEL
	and	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 2, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL58
	nop
	.align 4

.LL57:
	FMADD	(aa1, bb1, cc01, cc01)
	add	L, -1, L
	FMADD	(aa2, bb1, cc02, cc02)
	LDF	[BO + 2 * SIZE], b1

	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[AO + 2 * SIZE], a1
	FMADD	(aa2, bb2, cc04, cc04)
	LDF	[AO + 3 * SIZE], a2

	add	AO, 2 * SIZE, AO
	cmp	L, 0
	add	BO, 2 * SIZE, BO
	bg,pt	%icc, .LL57
	LDF	[BO + 1 * SIZE], b2
	.align 4

.LL58:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C1 + 1 * SIZE], a2
	LDF	[C2 + 0 * SIZE], a3
	LDF	[C2 + 1 * SIZE], a4

	FMADD	(alpha, cc01, aa1, cc01)
	FMADD	(alpha, cc02, aa2, cc02)
	FMADD	(alpha, cc03, aa3, cc03)
	FMADD	(alpha, cc04, aa4, cc04)
#else
	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c02, c02
	FMUL	ALPHA, c03, c03
	FMUL	ALPHA, c04, c04
#endif

	STF	c01, [C1 + 0 * SIZE]
	add	I, -1, I
	STF	c02, [C1 + 1 * SIZE]
	add	C1, 2 * SIZE, C1

	STF	c03, [C2 + 0 * SIZE]
	cmp	I, 0
	STF	c04, [C2 + 1 * SIZE]
	add	C2, 2 * SIZE, C2

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 1, TEMP2
	sll	TEMP1, BASE_SHIFT + 1, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

	bg,pt	%icc, .LL52
	nop
	.align 4

.LL60:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL69
	nop

#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 0, TEMP1
	sll	KK, BASE_SHIFT + 1, TEMP2

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
	LDF	[BO +  3 * SIZE], b4
	LDF	[BO +  4 * SIZE], b5
	LDF	[BO +  5 * SIZE], b6
	LDF	[BO +  6 * SIZE], b7
	FCLR	(cc01)
	LDF	[BO +  7 * SIZE], b8
	FCLR	(cc03)

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
	ble,pn	%icc, .LL65
	nop
	.align 4

.LL63:
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	add	L, -1, L

	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[BO +  8 * SIZE], b1
	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO +  9 * SIZE], b2

	LDF	[AO +  4 * SIZE], a1
	cmp	L, 0

	FMADD	(aa2, bb3, cc01, cc01)
	LDF	[BO + 10 * SIZE], b3
	FMADD	(aa2, bb4, cc03, cc03)
	LDF	[BO + 11 * SIZE], b4

	LDF	[AO +  5 * SIZE], a2
	add	AO,  4 * SIZE, AO

	FMADD	(aa3, bb5, cc01, cc01)
	LDF	[BO + 12 * SIZE], b5
	FMADD	(aa3, bb6, cc03, cc03)
	LDF	[BO + 13 * SIZE], b6

	LDF	[AO +  2 * SIZE], a3
	add	BO,  8 * SIZE, BO

	FMADD	(aa4, bb7, cc01, cc01)
	LDF	[BO +  6 * SIZE], b7
	FMADD	(aa4, bb8, cc03, cc03)
	LDF	[BO + 7 * SIZE], b8

	bg,pt	%icc, .LL63
	LDF	[AO +  3 * SIZE], a4
	.align 4

.LL65:
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
	ble,a,pn %icc, .LL68
	nop
	.align 4

.LL67:
	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[BO + 2 * SIZE], b1
	FMADD	(aa1, bb2, cc03, cc03)
	LDF	[BO + 3 * SIZE], b2

	LDF	[AO + 1 * SIZE], a1
	add	L, -1, L
	add	AO, 1 * SIZE, AO
	cmp	L, 0

	bg,pt	%icc, .LL67
	add	BO, 2 * SIZE, BO
	.align 4

.LL68:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C2 + 0 * SIZE], a2

	FMADD	(alpha, cc01, aa1, cc01)
	FMADD	(alpha, cc03, aa2, cc03)
#else
	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c03, c03
#endif

	STF	c01, [C1 + 0 * SIZE]
	STF	c03, [C2 + 0 * SIZE]

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 0, TEMP2
	sll	TEMP1, BASE_SHIFT + 1, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif
	.align 4

.LL69:
#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 2, KK
#endif
	mov	BO, B
	.align 4

.LL70:
	and	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL999
	mov	C,  C1

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	sra	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL80
	mov	A, AO
	.align 4

.LL72:
#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 1, TEMP1
	sll	KK, BASE_SHIFT + 0, TEMP2

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

	prefetch [C1 + 2 * SIZE], 3

#ifndef TRMMKERNEL
	sra	K,  2, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 1, L
#endif
	sra	L,  2, L
#endif
	cmp	L,  0
	ble,pn	%icc, .LL75
	nop

.LL73:
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	add	L, -1, L

	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[AO +  4 * SIZE], a1
	FMADD	(aa2, bb1, cc02, cc02)
	LDF	[AO +  5 * SIZE], a2

	LDF	[BO +  4 * SIZE], b1
	cmp	L, 0

	FMADD	(aa3, bb2, cc01, cc01)
	LDF	[AO +  6 * SIZE], a3
	FMADD	(aa4, bb2, cc02, cc02)
	LDF	[AO +  7 * SIZE], a4

	LDF	[BO +  5 * SIZE], b2
	add	BO,  4 * SIZE, BO

	FMADD	(aa1, bb3, cc01, cc01)
	LDF	[AO +  8 * SIZE], a1
	FMADD	(aa2, bb3, cc02, cc02)
	LDF	[AO +  9 * SIZE], a2

	LDF	[BO +  2 * SIZE], b3
	add	AO,  8 * SIZE, AO

	FMADD	(aa3, bb4, cc01, cc01)
	LDF	[AO +  2 * SIZE], a3
	FMADD	(aa4, bb4, cc02, cc02)
	LDF	[AO +  3 * SIZE], a4

	bg,pt	%icc, .LL73
	LDF	[BO +  3 * SIZE], b4
	.align 4

.LL75:
#ifndef TRMMKERNEL
	and	K,  3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 1, L
#endif
	and	L,  3, L
#endif
	cmp	L,  0
	ble,a,pn %icc, .LL78
	nop
	.align 4

.LL77:
	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[AO + 2 * SIZE], a1
	FMADD	(aa2, bb1, cc02, cc02)
	LDF	[AO + 3 * SIZE], a2

	LDF	[BO + 1 * SIZE], b1
	add	L, -1, L
	add	AO, 2 * SIZE, AO
	cmp	L, 0
	bg,pt	%icc, .LL77
	add	BO, 1 * SIZE, BO
	.align 4

.LL78:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C1 + 1 * SIZE], a2

	FMADD	(alpha, cc01, aa1, cc01)
	FMADD	(alpha, cc02, aa2, cc02)
#else
	FMUL	ALPHA, c01, c01
	FMUL	ALPHA, c02, c02
#endif

	STF	c01, [C1 + 0 * SIZE]
	add	I, -1, I
	STF	c02, [C1 + 1 * SIZE]
	cmp	I, 0

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif
	sll	TEMP1, BASE_SHIFT + 1, TEMP2
	sll	TEMP1, BASE_SHIFT + 0, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

	bg,pt	%icc, .LL72
	add	C1, 2 * SIZE, C1
	.align 4

.LL80:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL999
	nop

#if !defined(TRMMKERNEL) || (defined(TRMMKERNEL) && ((defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))))
	mov	B, BO
#else
	sll	KK, BASE_SHIFT + 0, TEMP1
	sll	KK, BASE_SHIFT + 0, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

	LDF	[AO +  0 * SIZE], a1
	LDF	[BO +  0 * SIZE], b1
	LDF	[AO +  1 * SIZE], a2
	LDF	[BO +  1 * SIZE], b2
	LDF	[AO +  2 * SIZE], a3
	LDF	[BO +  2 * SIZE], b3
	LDF	[AO +  3 * SIZE], a4
	LDF	[BO +  3 * SIZE], b4

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
	ble,pn	%icc, .LL85
	FCLR	(cc01)
	.align 4

.LL83:
	prefetch [AO + (APREFETCHSIZE +  0) * SIZE], APREFETCH_CATEGORY
	add	L, -1, L

	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[AO +  4 * SIZE], a1
	LDF	[BO +  4 * SIZE], b1

	FMADD	(aa2, bb2, cc01, cc01)
	LDF	[AO +  5 * SIZE], a2
	LDF	[BO +  5 * SIZE], b2

	FMADD	(aa3, bb3, cc01, cc01)
	LDF	[AO +  6 * SIZE], a3
	LDF	[BO +  6 * SIZE], b3

	FMADD	(aa4, bb4, cc01, cc01)
	LDF	[AO +  7 * SIZE], a4
	LDF	[BO +  7 * SIZE], b4

	add	AO,  4 * SIZE, AO
	cmp	L, 0

	bg,pt	%icc, .LL83
	add	BO,  4 * SIZE, BO
	.align 4

.LL85:
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
	ble,a,pn %icc, .LL88
	nop
	.align 4

.LL87:
	FMADD	(aa1, bb1, cc01, cc01)
	LDF	[AO + 1 * SIZE], a1
	LDF	[BO + 1 * SIZE], b1

	add	AO, 1 * SIZE, AO
	add	L, -1, L
	cmp	L, 0
	bg,pt	%icc, .LL87
	add	BO, 1 * SIZE, BO
	.align 4

.LL88:
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1

	FMADD	(alpha, cc01, aa1, cc01)
#else
	FMUL	ALPHA, c01, c01
#endif

	STF	c01, [C1 + 0 * SIZE]
	.align 4

.LL999:
#ifdef TRMMKERNEL
#ifndef __64BIT__
	ld	[%sp + STACK_START +  8], %g1
	ld	[%sp + STACK_START + 12], %g2
	ld	[%sp + STACK_START + 16], %g3
	ld	[%sp + STACK_START + 20], %g4
#else
	ldx	[%sp + STACK_START + 32], %g1
	ldx	[%sp + STACK_START + 40], %g2
	ldx	[%sp + STACK_START + 48], %g3
	ldx	[%sp + STACK_START + 56], %g4
#endif
#endif

	return	%i7 + 8
	clr	%o0

	EPILOGUE
