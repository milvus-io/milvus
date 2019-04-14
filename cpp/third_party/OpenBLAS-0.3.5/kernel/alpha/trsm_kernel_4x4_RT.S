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
#include "version.h"

#if !defined(EV4) && !defined(EV5) && !defined(EV6)
#error "Architecture is not specified."
#endif

#ifdef EV6
#define PREFETCHSIZE 56
#define UNOP unop
#endif

#ifdef EV5
#define PREFETCHSIZE 56
#define UNOP
#endif

#ifdef EV4
#define UNOP
#endif

#define STACKSIZE 80

#define M	$16
#define N	$17
#define K	$18
#define A	$20
#define	B	$21
#define C	$22
#define	LDC	$23

#define C1	$19
#define C2	$24
#define	C3	$25
#define C4	$27

#define AO	$at
#define	BO	$5
#define I	$6
#define J	$7
#define L	$8

#define a1	$f16
#define a2	$f17
#define a3	$f18
#define a4	$f19

#define b1	$f20
#define b2	$f21
#define b3	$f22
#define b4	$f23

#define t1	$f24
#define t2	$f25
#define t3	$f26
#define t4	$f27

#define a5	$f28
#define a6	$f30
#define b5	$f29

#define alpha	$f30

#define c01	$f0
#define c02	$f1
#define c03	$f2
#define c04	$f3

#define c05	$f4
#define c06	$f5
#define c07	$f6
#define c08	$f7

#define c09	$f8
#define c10	$f9
#define c11	$f10
#define c12	$f11

#define c13	$f12
#define c14	$f13
#define c15	$f14
#define c16	$f15

#define TMP1	$0
#define TMP2	$1
#define KK	$2
#define AORIG	$3
#define OFFSET  $4

	PROLOGUE
	PROFCODE
	.frame	$sp, STACKSIZE, $26, 0

	lda	$sp, -STACKSIZE($sp)

	ldq	C,        0 + STACKSIZE($sp)
	ldq	LDC,      8 + STACKSIZE($sp)
	ldq	OFFSET,  16 + STACKSIZE($sp)

	SXADDQ	LDC, 0, LDC

	stt	$f2,   0($sp)
	stt	$f3,   8($sp)
	stt	$f4,  16($sp)
	stt	$f5,  24($sp)
	stt	$f6,  32($sp)
	stt	$f7,  40($sp)
	stt	$f8,  48($sp)
	stt	$f9,  56($sp)

	cmple	M, 0, $0
	cmple	N, 0, $1
	cmple	K, 0, $2

	or	$0, $1, $0
	or	$0, $2, $0
	bne	$0, $L999

#ifdef LN
	mulq	M, K, TMP1
	SXADDQ	TMP1, A, A
	SXADDQ	M,    C, C
#endif

#ifdef RN
	negq	OFFSET, KK
#endif

#ifdef RT
	mulq	N, K, TMP1
	SXADDQ	TMP1, B, B

	mulq	N, LDC, TMP1
	addq	TMP1, C, C

	subq	N, OFFSET, KK
#endif

	and	N, 1, J
	ble	J, $L40

#ifdef RT
	sll	K, BASE_SHIFT, TMP1
	subq	B, TMP1, B

	subq	C, LDC, C
#endif

	mov	C,  C1
#ifndef RT
	addq	C, LDC, C
#endif

#ifdef LN
	addq	M, OFFSET, KK
#endif

#ifdef LT
	mov	OFFSET, KK
#endif

#if defined(LN) || defined(RT)
	mov	A, AORIG
#else
	mov	A, AO
#endif

	sra	M,  2, I
	ble	I, $L100
	.align 4

$L91:
#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
	fclr	t1
	LD	a2,  1 * SIZE(AO)
	fclr	t2
	LD	a3,  2 * SIZE(AO)
	fclr	t3
	LD	a4,  3 * SIZE(AO)
	fclr	t4

	LD	b1,  0 * SIZE(B)
	fclr	c01
	LD	b2,  1 * SIZE(B)
	fclr	c02
	LD	b3,  2 * SIZE(B)
 	fclr	c03
	LD	b4,  3 * SIZE(B)
	fclr	c04

	sra	KK, 2, L
	mov	B, BO
	ble	L, $L95

#else
#ifdef LN
	sll	K, BASE_SHIFT + 2, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 2, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 0, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	t1
	LD	a2,  1 * SIZE(AO)
	fclr	t2
	LD	a3,  2 * SIZE(AO)
	fclr	t3
	LD	a4,  3 * SIZE(AO)
	fclr	t4

	LD	b1,  0 * SIZE(BO)
	fclr	c01
	LD	b2,  1 * SIZE(BO)
	fclr	c02
	LD	b3,  2 * SIZE(BO)
 	fclr	c03
	LD	b4,  3 * SIZE(BO)
	fclr	c04

	sra	TMP1, 2, L
	unop
	ble	L, $L95
#endif
	.align	5

$L92:
	ADD	c01, t1, c01
	unop
	MUL	a1, b1, t1
	LD	a1,  4 * SIZE(AO)

	ADD	c02, t2, c02
	lda	L,   -1(L)
	MUL	a2, b1, t2
	LD	a2,  5 * SIZE(AO)

	ADD	c03, t3, c03
	unop
	MUL	a3, b1, t3
	LD	a3,  6 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a4, b1, t4
	LD	a4,  7 * SIZE(AO)
	LD	b1,  4 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b2, t1
	LD	a1,  8 * SIZE(AO)

	ADD	c02, t2, c02
	unop
	MUL	a2, b2, t2
	LD	a2,  9 * SIZE(AO)

	ADD	c03, t3, c03
	unop
	MUL	a3, b2, t3
	LD	a3, 10 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a4, b2, t4
	LD	a4, 11 * SIZE(AO)
	LD	b2,  5 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b3, t1
	LD	a1, 12 * SIZE(AO)

	ADD	c02, t2, c02
	unop
	MUL	a2, b3, t2
	LD	a2, 13 * SIZE(AO)

	ADD	c03, t3, c03
	unop
	MUL	a3, b3, t3
	LD	a3, 14 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a4, b3, t4
	LD	a5, 15 * SIZE(AO)
	LD	b3,  6 * SIZE(BO)

	ADD	c01, t1, c01
	MUL	a1, b4, t1
	LD	a1, 16 * SIZE(AO)
	lda	AO, 16 * SIZE(AO)

	ADD	c02, t2, c02
	lda	BO,  4 * SIZE(BO)
	MUL	a2, b4, t2
	LD	a2,  1 * SIZE(AO)

	ADD	c03, t3, c03
	LD	a4,  3 * SIZE(AO)
	MUL	a3, b4, t3
	LD	a3,  2 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a5, b4, t4
	LD	b4,  3 * SIZE(BO)
	bgt	L,  $L92
	.align 4

$L95:
#if defined(LT) || defined(RN)
	and	KK, 3, L
#else
	and	TMP1, 3, L
#endif
	unop
	ble	L, $L98
	.align 4

$L96:
	ADD	c01, t1, c01
	lda	L,   -1(L)
	MUL	a1, b1, t1
	LD	a1,  4 * SIZE(AO)

	ADD	c02, t2, c02
	lda	BO,  1 * SIZE(BO)
	MUL	a2, b1, t2
	LD	a2,  5 * SIZE(AO)

	ADD	c03, t3, c03
	unop
	MUL	a3, b1, t3
	LD	a3,  6 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a4, b1, t4
	LD	a4,  7 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	lda	AO,  4 * SIZE(AO)
	bgt	L,  $L96
	.align 4

$L98:
	ADD	c01, t1, c01
	ADD	c02, t2, c02
	ADD	c03, t3, c03
	ADD	c04, t4, c04

#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 4, TMP1
#else
	subq	KK, 1, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	B,     TMP2, BO
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c03, c03
	SUB	a4, c04, c04
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c03, c03
	SUB	a4, c04, c04
#endif

#ifdef LN
	LD	a1, 15 * SIZE(AO)
	LD	a2, 14 * SIZE(AO)
	LD	a3, 13 * SIZE(AO)
	LD	a4, 12 * SIZE(AO)

	MUL	a1, c04, c04
	MUL	a2, c04, t1
	SUB	c03, t1, c03
	MUL	a3, c04, t1
	SUB	c02, t1, c02
	MUL	a4, c04, t1
	SUB	c01, t1, c01

	LD	b1, 10 * SIZE(AO)
	LD	b2,  9 * SIZE(AO)
	LD	b3,  8 * SIZE(AO)

	MUL	b1, c03, c03
	MUL	b2, c03, t1
	SUB	c02, t1, c02
	MUL	b3, c03, t1
	SUB	c01, t1, c01

	LD	a1,  5 * SIZE(AO)
	LD	a2,  4 * SIZE(AO)
	LD	a3,  0 * SIZE(AO)

	MUL	a1, c02, c02
	MUL	a2, c02, t1
	SUB	c01, t1, c01
	MUL	a3, c01, c01
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a2, c01, t1
	SUB	c02, t1, c02
	MUL	a3, c01, t1
	SUB	c03, t1, c03
	MUL	a4, c01, t1
	SUB	c04, t1, c04

	LD	b1,  5 * SIZE(AO)
	LD	b2,  6 * SIZE(AO)
	LD	b3,  7 * SIZE(AO)

	MUL	b1, c02, c02
	MUL	b2, c02, t1
	SUB	c03, t1, c03
	MUL	b3, c02, t1
	SUB	c04, t1, c04

	LD	a1, 10 * SIZE(AO)
	LD	a2, 11 * SIZE(AO)
	LD	a3, 15 * SIZE(AO)

	MUL	a1, c03, c03
	MUL	a2, c03, t1
	SUB	c04, t1, c04
	MUL	a3, c04, c04
#endif

#if defined(RN) || defined(RT)
	LD	a1,  0 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c03, c03
	MUL	a1, c04, c04
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c02,  1 * SIZE(BO)
	ST	c03,  2 * SIZE(BO)
	ST	c04,  3 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c03,  2 * SIZE(AO)
	ST	c04,  3 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -4 * SIZE(C1)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c03,  2 * SIZE(C1)
	ST	c04,  3 * SIZE(C1)

#ifndef LN
	lda	C1,   4 * SIZE(C1)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 2 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 4, KK
#endif

#ifdef LN
	subq	KK, 4, KK
#endif

	lda	I,        -1(I)
	bgt	I, $L91
	.align 4

$L100:
	and	M,  2, I
	ble	I, $L110

#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
	fclr	t1
	LD	a2,  1 * SIZE(AO)
	fclr	t2
	LD	a3,  2 * SIZE(AO)
	fclr	t3
	LD	a4,  3 * SIZE(AO)
	fclr	t4

	LD	b1,  0 * SIZE(B)
	fclr	c01
	LD	b2,  1 * SIZE(B)
	fclr	c02
	LD	b3,  2 * SIZE(B)
 	fclr	c03
	LD	b4,  3 * SIZE(B)
	fclr	c04

	sra	KK, 2, L
	mov	B, BO
	ble	L, $L105
#else
#ifdef LN
	sll	K, BASE_SHIFT + 1, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 0, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	t1
	LD	a2,  1 * SIZE(AO)
	fclr	t2
	LD	a3,  2 * SIZE(AO)
	fclr	t3
	LD	a4,  3 * SIZE(AO)
	fclr	t4

	LD	b1,  0 * SIZE(BO)
	fclr	c01
	LD	b2,  1 * SIZE(BO)
	fclr	c02
	LD	b3,  2 * SIZE(BO)
 	fclr	c03
	LD	b4,  3 * SIZE(BO)
	fclr	c04

	sra	TMP1, 2, L
	ble	L, $L105
#endif
	.align	5

$L102:
	ADD	c01, t1, c01
	lda	L,        -1(L)
	MUL	a1, b1, t1
	LD	a1,  4 * SIZE(AO)

	ADD	c02, t2, c02
	MUL	a2, b1, t2
	LD	a2,  5 * SIZE(AO)
	LD	b1,  4 * SIZE(BO)

	ADD	c03, t3, c03
	lda	BO,    4 * SIZE(BO)
	MUL	a3, b2, t3
	LD	a3,  6 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a4, b2, t4
	LD	a5,  7 * SIZE(AO)
	LD	b2,  1 * SIZE(BO)

	ADD	c01, t1, c01
	MUL	a1, b3, t1
	LD	a1,  8 * SIZE(AO)
	lda	AO,  8 * SIZE(AO)

	ADD	c02, t2, c02
	MUL	a2, b3, t2
	LD	b3,  2 * SIZE(BO)
	LD	a2,  1 * SIZE(AO)

	ADD	c03, t3, c03
	LD	a4,  3 * SIZE(AO)
	MUL	a3, b4, t3
	LD	a3,  2 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a5, b4, t4
	LD	b4,  3 * SIZE(BO)
	bgt	L,  $L102
	.align 4

$L105:
#if defined(LT) || defined(RN)
	and	KK, 3, L
#else
	and	TMP1, 3, L
#endif
	ble	L, $L108
	.align 4

$L106:
	ADD	c01, t1, c01
	lda	L,        -1(L)
	MUL	a1, b1, t1
	LD	a1,  2 * SIZE(AO)

	ADD	c02, t2, c02
	MUL	a2, b1, t2
	LD	a2,  3 * SIZE(AO)
	LD	b1,  1 * SIZE(BO)

	lda	AO,  2 * SIZE(AO)
	unop
	lda	BO,  1 * SIZE(BO)
	bgt	L,  $L106
	.align 4

$L108:
	ADD	c01, t1, c01
	ADD	c02, t2, c02
	ADD	c03, t3, c03
	ADD	c04, t4, c04

	ADD	c01, c03, c01
	ADD	c02, c04, c02

#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 2, TMP1
#else
	subq	KK, 1, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	B,     TMP2, BO
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
#endif

#ifdef LN
	LD	a1,  3 * SIZE(AO)
	LD	a2,  2 * SIZE(AO)
	LD	a3,  0 * SIZE(AO)

	MUL	a1, c02, c02
	MUL	a2, c02, t1
	SUB	c01, t1, c01
	MUL	a3, c01, c01
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  3 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a2, c01, t1
	SUB	c02, t1, c02
	MUL	a3, c02, c02
#endif

#if defined(RN) || defined(RT)
	LD	a1,  0 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a1, c02, c02
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c02,  1 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -2 * SIZE(C1)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)

#ifndef LN
	lda	C1,   2 * SIZE(C1)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 2, KK
#endif

#ifdef LN
	subq	KK, 2, KK
#endif
	.align 4

$L110:
	and	M,  1, I
	ble	I, $L119

#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
	fclr	t1
	LD	a2,  1 * SIZE(AO)
	fclr	t2
	LD	a3,  2 * SIZE(AO)
	fclr	t3
	LD	a4,  3 * SIZE(AO)
	fclr	t4

	LD	b1,  0 * SIZE(B)
	fclr	c01
	LD	b2,  1 * SIZE(B)
	fclr	c02
	LD	b3,  2 * SIZE(B)
 	fclr	c03
	LD	b4,  3 * SIZE(B)
	fclr	c04

	sra	KK, 2, L
	mov	B, BO
	unop
	ble	L, $L115
#else
#ifdef LN
	sll	K, BASE_SHIFT + 0, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 0, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 0, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	t1
	LD	a2,  1 * SIZE(AO)
	fclr	t2
	LD	a3,  2 * SIZE(AO)
	fclr	t3
	LD	a4,  3 * SIZE(AO)
	fclr	t4

	LD	b1,  0 * SIZE(BO)
	fclr	c01
	LD	b2,  1 * SIZE(BO)
	fclr	c02
	LD	b3,  2 * SIZE(BO)
 	fclr	c03
	LD	b4,  3 * SIZE(BO)
	fclr	c04

	sra	TMP1, 2, L
	unop
	ble	L, $L115
#endif
	.align	4

$L112:
	ADD	c01, t1, c01
	MUL	a1,  b1, t1
	LD	a1,  4 * SIZE(AO)
	LD	b1,  4 * SIZE(BO)

	ADD	c02, t2, c02
	MUL	a2,  b2, t2
	LD	a2,  5 * SIZE(AO)
	LD	b2,  5 * SIZE(BO)

	ADD	c03, t3, c03
	MUL	a3,  b3, t3
	LD	a3,  6 * SIZE(AO)
	LD	b3,  6 * SIZE(BO)

	ADD	c04, t4, c04
	MUL	a4,  b4, t4
	LD	a4,  7 * SIZE(AO)
	LD	b4,  7 * SIZE(BO)

	lda	L,        -1(L)
	lda	AO,    4 * SIZE(AO)
	lda	BO,    4 * SIZE(BO)
	bgt	L,  $L112
	.align 4

$L115:
#if defined(LT) || defined(RN)
	and	KK, 3, L
#else
	and	TMP1, 3, L
#endif
	ble	L, $L118
	.align	4

$L116:
	ADD	c01, t1, c01
	MUL	a1,  b1, t1
	LD	a1,  1 * SIZE(AO)
	LD	b1,  1 * SIZE(BO)

	lda	L,        -1(L)
	lda	AO,  1 * SIZE(AO)
	lda	BO,  1 * SIZE(BO)
	bgt	L,  $L116
	.align 4

$L118:
	ADD	c01, t1, c01
	ADD	c02, t2, c02
	ADD	c03, t3, c03
	ADD	c04, t4, c04

	ADD	c01, c02, c01
	ADD	c03, c04, c03
	ADD	c01, c03, c01

#if defined(LN) || defined(RT)
	subq	KK, 1, TMP1
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	AORIG, TMP2, AO
	addq	B,     TMP2, BO
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)

	SUB	a1, c01, c01
#else
	LD	a1,  0 * SIZE(AO)

	SUB	a1, c01, c01
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(AO)

	MUL	a1, c01, c01
#endif

#if defined(RN) || defined(RT)
	LD	a1,  0 * SIZE(BO)

	MUL	a1, c01, c01
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -1 * SIZE(C1)
#endif

	ST	c01,  0 * SIZE(C1)

#ifndef LN
	lda	C1,   1 * SIZE(C1)
#endif

#ifdef RT
	SXADDQ	K, AORIG, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	AO, TMP2, AO
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 1, KK
#endif

#ifdef LN
	subq	KK, 1, KK
#endif
	.align 4

$L119:
#ifdef LN
	SXADDQ	K, B, B
#endif

#if defined(LT) || defined(RN)
	mov	BO,  B
#endif

#ifdef RN
	addq	KK, 1, KK
#endif

#ifdef RT
	subq	KK, 1, KK
#endif
	.align 4

$L40:
	and	N, 2, J
	ble	J, $L80

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TMP1
	subq	B, TMP1, B

	addq	LDC, LDC, TMP1
	subq	C, TMP1, C
#endif

	mov	C,  C1
	addq	C,  LDC, C2
	fclr	t1
#ifndef RT
	addq	C2, LDC, C
#endif
	fclr	t2

#ifdef LN
	addq	M, OFFSET, KK
#endif

#ifdef LT
	mov	OFFSET, KK
#endif

#if defined(LN) || defined(RT)
	mov	A, AORIG
#else
	mov	A, AO
#endif

	sra	M,  2, I
	fclr	t3
	fclr	t4
	ble	I, $L60
	.align 4

$L51:
#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
	fclr	c03
	LD	a2,  1 * SIZE(AO)
	fclr	c07
	LD	a3,  2 * SIZE(AO)
	fclr	c04
	LD	a4,  3 * SIZE(AO)
	fclr	c08

	LD	b1,  0 * SIZE(B)
	fclr	c01
	LD	b2,  1 * SIZE(B)
	fclr	c05
	LD	b3,  2 * SIZE(B)
 	fclr	c02
	LD	b4,  3 * SIZE(B)
	fclr	c06

	lda	L,        -2(KK)

	lda	BO,  2 * SIZE(B)
	lda	AO,  4 * SIZE(AO)

	ble	KK, $L58

	ble	L, $L55
#else
#ifdef LN
	sll	K, BASE_SHIFT + 2, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 2, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 1, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	c03
	LD	a2,  1 * SIZE(AO)
	fclr	c07
	LD	a3,  2 * SIZE(AO)
	fclr	c04
	LD	a4,  3 * SIZE(AO)
	fclr	c08

	LD	b1,  0 * SIZE(BO)
	fclr	c01
	LD	b2,  1 * SIZE(BO)
	fclr	c05
	LD	b3,  2 * SIZE(BO)
 	fclr	c02
	LD	b4,  3 * SIZE(BO)
	fclr	c06

	lda	L,        -2(TMP1)
	lda	BO,  2 * SIZE(BO)
	lda	AO,  4 * SIZE(AO)

	ble	TMP1, $L58

	ble	L, $L55
#endif
	.align	4

$L52:
	ADD	c05, t1, c05
	unop
	MUL	a1, b1, t1
	unop

	ADD	c06, t2, c06
	lda	L,   -2(L)
	MUL	a2, b1, t2
	unop

	ADD	c07, t3, c07
	unop
	MUL	a3, b1, t3
	unop

	ADD	c08, t4, c08
	unop
	MUL	a4, b1, t4
	LD	b1,  2 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b2, t1
	LD	a1,  0 * SIZE(AO)

	ADD	c02, t2, c02
	lda	BO,  4 * SIZE(BO)
	MUL	a2, b2, t2
	LD	a2,  1 * SIZE(AO)

	ADD	c03, t3, c03
	unop
	MUL	a3, b2, t3
	LD	a3,  2 * SIZE(AO)

	ADD	c04, t4, c04
	unop
	MUL	a4, b2, t4
	LD	a5,  3 * SIZE(AO)

	ADD	c05, t1, c05
	unop
	MUL	a1, b3, t1
	LD	b2, -1 * SIZE(BO)

	ADD	c06, t2, c06
	unop
	MUL	a2, b3, t2
	unop

	ADD	c07, t3, c07
	unop
	MUL	a3, b3, t3
	lda	AO,  8 * SIZE(AO)

	ADD	c08, t4, c08
	unop
	MUL	a5, b3, t4
	LD	b3,  0 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b4, t1
	LD	a1, -4 * SIZE(AO)

	ADD	c02, t2, c02
	unop
	MUL	a2, b4, t2
	LD	a2, -3 * SIZE(AO)

	ADD	c03, t3, c03
	LD	a4, -1 * SIZE(AO)
	MUL	a3, b4, t3
	LD	a3, -2 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a5, b4, t4
	LD	b4,  1 * SIZE(BO)
	bgt	L,  $L52
	.align 4

$L55:
	ADD	c05, t1, c05
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L57
#else
	blbs	TMP1, $L57
#endif
	.align 4

	ADD	c06, t2, c06
	MUL	a2, b1, t2
	ADD	c07, t3, c07
	MUL	a3, b1, t3

	ADD	c08, t4, c08
	unop
	MUL	a4, b1, t4
	LD	b1,  0 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b2, t1
	LD	a1,  0 * SIZE(AO)

	ADD	c02, t2, c02
	unop
	MUL	a2, b2, t2
	LD	a2,  1 * SIZE(AO)

	ADD	c03, t3, c03
	unop
	MUL	a3, b2, t3
	LD	a3,  2 * SIZE(AO)

	ADD	c04, t4, c04
	MUL	a4, b2, t4
	LD	a4,  3 * SIZE(AO)
	lda	AO,  4 * SIZE(AO)

	ADD	c05, t1, c05
	LD	b2,  1 * SIZE(BO)
	MUL	a1, b1, t1
	lda	BO,  2 * SIZE(BO)
	.align 4

$L57:
	ADD	c06, t2, c06
	MUL	a2, b1, t2
	ADD	c07, t3, c07
	MUL	a3, b1, t3

	ADD	c08, t4, c08
	MUL	a4, b1, t4
	ADD	c01, t1, c01
	MUL	a1, b2, t1

	ADD	c02, t2, c02
	MUL	a2, b2, t2
	ADD	c03, t3, c03
	MUL	a3, b2, t3

	ADD	c04, t4, c04
	lda	AO,  4 * SIZE(AO)
	MUL	a4, b2, t4
	lda	BO,  2 * SIZE(BO)

	ADD	c05, t1, c05
	ADD	c06, t2, c06
	ADD	c07, t3, c07
	ADD	c08, t4, c08
	.align 4

$L58:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 4, TMP1
#else
	subq	KK, 2, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -4 * SIZE(AO)
	lda	BO,   -2 * SIZE(BO)
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	LD	b1,  4 * SIZE(BO)
 	LD	b2,  5 * SIZE(BO)
	LD	b3,  6 * SIZE(BO)
	LD	b4,  7 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
	SUB	a3, c02, c02
	SUB	a4, c06, c06

	SUB	b1, c03, c03
	SUB	b2, c07, c07
	SUB	b3, c04, c04
	SUB	b4, c08, c08
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  4 * SIZE(AO)
 	LD	b2,  5 * SIZE(AO)
	LD	b3,  6 * SIZE(AO)
	LD	b4,  7 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c03, c03
	SUB	a4, c04, c04

	SUB	b1, c05, c05
	SUB	b2, c06, c06
	SUB	b3, c07, c07
	SUB	b4, c08, c08
#endif

#ifdef LN
	LD	a1, 15 * SIZE(AO)
	LD	a2, 14 * SIZE(AO)
	LD	a3, 13 * SIZE(AO)
	LD	a4, 12 * SIZE(AO)

	MUL	a1, c04, c04
	MUL	a1, c08, c08

	MUL	a2, c04, t1
	MUL	a2, c08, t2

	SUB	c03, t1, c03
	SUB	c07, t2, c07

	MUL	a3, c04, t1
	MUL	a3, c08, t2

	SUB	c02, t1, c02
	SUB	c06, t2, c06

	MUL	a4, c04, t1
	MUL	a4, c08, t2

	SUB	c01, t1, c01
	SUB	c05, t2, c05

	LD	b1, 10 * SIZE(AO)
	LD	b2,  9 * SIZE(AO)
	LD	b3,  8 * SIZE(AO)

	MUL	b1, c03, c03
	MUL	b1, c07, c07

	MUL	b2, c03, t1
	MUL	b2, c07, t2

	SUB	c02, t1, c02
	SUB	c06, t2, c06

	MUL	b3, c03, t1
	MUL	b3, c07, t2

	SUB	c01, t1, c01
	SUB	c05, t2, c05

	LD	a1,  5 * SIZE(AO)
	LD	a2,  4 * SIZE(AO)
	LD	a3,  0 * SIZE(AO)

	MUL	a1, c02, c02
	MUL	a1, c06, c06

	MUL	a2, c02, t1
	MUL	a2, c06, t2

	SUB	c01, t1, c01
	SUB	c05, t2, c05

	MUL	a3, c01, c01
	MUL	a3, c05, c05
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a1, c05, c05

	MUL	a2, c01, t1
	MUL	a2, c05, t2

	SUB	c02, t1, c02
	SUB	c06, t2, c06

	MUL	a3, c01, t1
	MUL	a3, c05, t2

	SUB	c03, t1, c03
	SUB	c07, t2, c07

	MUL	a4, c01, t1
	MUL	a4, c05, t2

	SUB	c04, t1, c04
	SUB	c08, t2, c08

	LD	b1,  5 * SIZE(AO)
	LD	b2,  6 * SIZE(AO)
	LD	b3,  7 * SIZE(AO)

	MUL	b1, c02, c02
	MUL	b1, c06, c06

	MUL	b2, c02, t1
	MUL	b2, c06, t2

	SUB	c03, t1, c03
	SUB	c07, t2, c07

	MUL	b3, c02, t1
	MUL	b3, c06, t2

	SUB	c04, t1, c04
	SUB	c08, t2, c08

	LD	a1, 10 * SIZE(AO)
	LD	a2, 11 * SIZE(AO)
	LD	a3, 15 * SIZE(AO)

	MUL	a1, c03, c03
	MUL	a1, c07, c07

	MUL	a2, c03, t1
	MUL	a2, c07, t2

	SUB	c04, t1, c04
	SUB	c08, t2, c08

	MUL	a3, c04, c04
	MUL	a3, c08, c08
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  3 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	MUL	a2, c01, t1
	MUL	a2, c02, t2
	MUL	a2, c03, t3
	MUL	a2, c04, t4

	SUB	c05, t1, c05
	SUB	c06, t2, c06
	SUB	c07, t3, c07
	SUB	c08, t4, c08

	MUL	a3, c05, c05
	MUL	a3, c06, c06
	MUL	a3, c07, c07
	MUL	a3, c08, c08
#endif

#ifdef RT
	LD	a1,  3 * SIZE(BO)
	LD	a2,  2 * SIZE(BO)
	LD	a3,  0 * SIZE(BO)

	MUL	a1, c05, c05
	MUL	a1, c06, c06
	MUL	a1, c07, c07
	MUL	a1, c08, c08

	MUL	a2, c05, t1
	MUL	a2, c06, t2
	MUL	a2, c07, t3
	MUL	a2, c08, t4

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	SUB	c03, t3, c03
	SUB	c04, t4, c04

	MUL	a3, c01, c01
	MUL	a3, c02, c02
	MUL	a3, c03, c03
	MUL	a3, c04, c04
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c05,  1 * SIZE(BO)
	ST	c02,  2 * SIZE(BO)
	ST	c06,  3 * SIZE(BO)

	ST	c03,  4 * SIZE(BO)
	ST	c07,  5 * SIZE(BO)
	ST	c04,  6 * SIZE(BO)
	ST	c08,  7 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c03,  2 * SIZE(AO)
	ST	c04,  3 * SIZE(AO)

	ST	c05,  4 * SIZE(AO)
	ST	c06,  5 * SIZE(AO)
	ST	c07,  6 * SIZE(AO)
	ST	c08,  7 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -4 * SIZE(C1)
	lda	C2,  -4 * SIZE(C2)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c03,  2 * SIZE(C1)
	ST	c04,  3 * SIZE(C1)

	ST	c05,  0 * SIZE(C2)
 	ST	c06,  1 * SIZE(C2)
	ST	c07,  2 * SIZE(C2)
	ST	c08,  3 * SIZE(C2)

#ifndef LN
	lda	C1,   4 * SIZE(C1)
	lda	C2,   4 * SIZE(C2)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 2 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 4, KK
#endif

#ifdef LN
	subq	KK, 4, KK
#endif

	lda	I,        -1(I)

	bgt	I, $L51
	.align 4

$L60:
	and	M,  2, I
	ble	I, $L70

#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
	fclr	c01
	LD	a2,  1 * SIZE(AO)
	fclr	c05
	LD	a3,  2 * SIZE(AO)
 	fclr	c02
	LD	a4,  3 * SIZE(AO)
	fclr	c06

	LD	b1,  0 * SIZE(B)
	lda	L,        -2(KK)
	LD	b2,  1 * SIZE(B)
	lda	AO,  2 * SIZE(AO)

	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)
	lda	BO,  2 * SIZE(B)

	ble	KK, $L68

	ble	L, $L65
#else
#ifdef LN
	sll	K, BASE_SHIFT + 1, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 1, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	c01
	LD	a2,  1 * SIZE(AO)
	fclr	c05
	LD	a3,  2 * SIZE(AO)
 	fclr	c02
	LD	a4,  3 * SIZE(AO)
	fclr	c06

	LD	b1,  0 * SIZE(BO)
	lda	L,        -2(TMP1)
	LD	b2,  1 * SIZE(BO)
	lda	AO,  2 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	lda	BO,  2 * SIZE(BO)

	ble	TMP1, $L68

	ble	L, $L65
#endif
	.align	4

$L62:
	ADD	c01, t1, c01
	unop
	MUL	a1, b1, t1
	unop

	ADD	c02, t2, c02
	lda	AO,    4 * SIZE(AO)
	MUL	a2, b1, t2
	LD	b1,  2 * SIZE(BO)

	ADD	c05, t3, c05
	lda	L,        -2(L)
	MUL	a1, b2, t3
	LD	a1, -2 * SIZE(AO)

	ADD	c06, t4, c06
	unop
	MUL	a2, b2, t4
	LD	a2, -1 * SIZE(AO)

	ADD	c01, t1, c01
	LD	b2,  3 * SIZE(BO)
	MUL	a3, b3, t1
	lda	BO,    4 * SIZE(BO)

	ADD	c02, t2, c02
	unop
	MUL	a4, b3, t2
	LD	b3,  0 * SIZE(BO)

	ADD	c05, t3, c05
	unop
	MUL	a3, b4, t3
	LD	a3,  0 * SIZE(AO)

	ADD	c06, t4, c06
	MUL	a4, b4, t4
	LD	b4,  1 * SIZE(BO)
	unop

	LD	a4,  1 * SIZE(AO)
	unop
	unop
	bgt	L,  $L62
	.align 4

$L65:
	ADD	c01, t1, c01
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L67
#else
	blbs	TMP1, $L67
#endif
	.align 4

	ADD	c02, t2, c02
	unop
	MUL	a2, b1, t2
	LD	b1,  0 * SIZE(BO)

	ADD	c05, t3, c05
	lda	BO,  2 * SIZE(BO)
	MUL	a1, b2, t3
	LD	a1,  0 * SIZE(AO)

	ADD	c06, t4, c06
	unop
	MUL	a2, b2, t4
	LD	a2,  1 * SIZE(AO)

	ADD	c01, t1, c01
	LD	b2, -1 * SIZE(BO)
	MUL	a1, b1, t1
	lda	AO,  2 * SIZE(AO)
	.align 4

$L67:
	ADD	c02, t2, c02
	MUL	a2, b1, t2
	ADD	c05, t3, c05
	MUL	a1, b2, t3

	ADD	c06, t4, c06
	lda	AO,  2 * SIZE(AO)
	MUL	a2, b2, t4
 	lda	BO,  2 * SIZE(BO)

	ADD	c01, t1, c01
	ADD	c02, t2, c02
	ADD	c05, t3, c05
	ADD	c06, t4, c06
	.align 4

$L68:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 2, TMP1
#else
	subq	KK, 2, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -2 * SIZE(AO)
	lda	BO,   -2 * SIZE(BO)
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
	SUB	a3, c02, c02
	SUB	a4, c06, c06
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c05, c05
	SUB	a4, c06, c06
#endif

#ifdef LN
	LD	a1,  3 * SIZE(AO)
	LD	a2,  2 * SIZE(AO)
	LD	a3,  0 * SIZE(AO)

	MUL	a1, c02, c02
	MUL	a1, c06, c06

	MUL	a2, c02, t1
	MUL	a2, c06, t2

	SUB	c01, t1, c01
	SUB	c05, t2, c05

	MUL	a3, c01, c01
	MUL	a3, c05, c05
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  3 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a1, c05, c05

	MUL	a2, c01, t1
	MUL	a2, c05, t2

	SUB	c02, t1, c02
	SUB	c06, t2, c06

	MUL	a3, c02, c02
	MUL	a3, c06, c06
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  3 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a1, c02, c02

	MUL	a2, c01, t1
	MUL	a2, c02, t2

	SUB	c05, t1, c05
	SUB	c06, t2, c06

	MUL	a3, c05, c05
	MUL	a3, c06, c06
#endif

#ifdef RT
	LD	a1,  3 * SIZE(BO)
	LD	a2,  2 * SIZE(BO)
	LD	a3,  0 * SIZE(BO)

	MUL	a1, c05, c05
	MUL	a1, c06, c06

	MUL	a2, c05, t1
	MUL	a2, c06, t2

	SUB	c01, t1, c01
	SUB	c02, t2, c02

	MUL	a3, c01, c01
	MUL	a3, c02, c02
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c05,  1 * SIZE(BO)
	ST	c02,  2 * SIZE(BO)
	ST	c06,  3 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c05,  2 * SIZE(AO)
	ST	c06,  3 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -2 * SIZE(C1)
	lda	C2,  -2 * SIZE(C2)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c05,  0 * SIZE(C2)
 	ST	c06,  1 * SIZE(C2)

#ifndef LN
	lda	C1,   2 * SIZE(C1)
	lda	C2,   2 * SIZE(C2)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 2, KK
#endif

#ifdef LN
	subq	KK, 2, KK
#endif
	.align 4

$L70:
	and	M,  1, I
	ble	I, $L79

#if defined(LT) || defined(RN)


	LD	a1,  0 * SIZE(AO)
	fclr	c01
	LD	a2,  1 * SIZE(AO)
	fclr	c05

	LD	b1,  0 * SIZE(B)
 	fclr	c02
	LD	b2,  1 * SIZE(B)
	fclr	c06

	lda	L,        -2(KK)

	LD	b3,  2 * SIZE(B)
	lda	AO,  1 * SIZE(AO)
	LD	b4,  3 * SIZE(B)
	lda	BO,  2 * SIZE(B)

	ble	KK, $L78

	ble	L, $L75
#else
#ifdef LN
	sll	K, BASE_SHIFT + 0, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 0, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 1, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	c01
	LD	a2,  1 * SIZE(AO)
	fclr	c05

	LD	b1,  0 * SIZE(BO)
 	fclr	c02
	LD	b2,  1 * SIZE(BO)
	fclr	c06

	lda	L,        -2(TMP1)

	LD	b3,  2 * SIZE(BO)
	lda	AO,  1 * SIZE(AO)
	LD	b4,  3 * SIZE(BO)
	lda	BO,  2 * SIZE(BO)

	ble	TMP1, $L78

	ble	L, $L75
#endif
	.align	4

$L72:
	ADD	c01, t1, c01
	lda	L,        -2(L)
	MUL	a1, b1, t1
	LD	b1,  2 * SIZE(BO)

	ADD	c05, t2, c05
	MUL	a1, b2, t2
	LD	a1,  1 * SIZE(AO)
	LD	b2,  3 * SIZE(BO)

	ADD	c02, t3, c02
	lda	AO,    2 * SIZE(AO)
	MUL	a2, b3, t3
	LD	b3,  4 * SIZE(BO)

	ADD	c06, t4, c06
	MUL	a2, b4, t4
	LD	a2,  0 * SIZE(AO)
	LD	b4,  5 * SIZE(BO)

	lda	BO,    4 * SIZE(BO)
	unop
	unop
	bgt	L,  $L72
	.align 4

$L75:
	ADD	c01, t1, c01
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L77
#else
	blbs	TMP1, $L77
#endif
	.align 4

	ADD	c05, t2, c05
	MUL	a1, b2, t2
	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	ADD	c01, t1, c01
	LD	b2,  1 * SIZE(BO)
	lda	AO,  1 * SIZE(AO)
	MUL	a1, b1, t1
	lda	BO,  2 * SIZE(BO)
	.align 4

$L77:
	ADD	c05, t2, c05
	MUL	a1, b2, t2
	ADD	c02, t3, c02
	ADD	c06, t4, c06

	ADD	c01, c02, c01
	lda	AO,  1 * SIZE(AO)
	ADD	c05, c06, c05
 	lda	BO,  2 * SIZE(BO)

	ADD	c01, t1, c01
	ADD	c05, t2, c05

	.align 4

$L78:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 1, TMP1
#else
	subq	KK, 2, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -1 * SIZE(AO)
	lda	BO,   -2 * SIZE(BO)
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a1, c05, c05
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  3 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a2, c01, t1
	SUB	c05, t1, c05
	MUL	a3, c05, c05
#endif

#ifdef RT
	LD	a1,  3 * SIZE(BO)
	LD	a2,  2 * SIZE(BO)
	LD	a3,  0 * SIZE(BO)

	MUL	a1, c05, c05
	MUL	a2, c05, t1
	SUB	c01, t1, c01
	MUL	a3, c01, c01
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c05,  1 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c05,  1 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -1 * SIZE(C1)
	lda	C2,  -1 * SIZE(C2)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c05,  0 * SIZE(C2)

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 0 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 1, KK
#endif

#ifdef LN
	subq	KK, 1, KK
#endif
	.align 4

$L79:
#ifdef LN
	sll	K, 1 + BASE_SHIFT, TMP1
	addq	B, TMP1, B
#endif

#if defined(LT) || defined(RN)
	mov	BO,  B
#endif

#ifdef RN
	addq	KK, 2, KK
#endif

#ifdef RT
	subq	KK, 2, KK
#endif
	.align 4

$L80:
	sra	N, 2, J
	ble	J, $L999
	.align 4

$L01:
#ifdef RT
	sll	K, 2 + BASE_SHIFT, TMP1
	subq	B, TMP1, B

	s4addq	LDC, 0, TMP1
	subq	C, TMP1, C
#endif

	mov	C,  C1
	addq	C,  LDC, C2
	addq	C2, LDC, C3
#ifndef RT
	s4addq	LDC, C, C
#endif

	fclr	t1
	addq	C3, LDC, C4
	fclr	t2

#ifdef LN
	addq	M, OFFSET, KK
#endif

#ifdef LT
	mov	OFFSET, KK
#endif

#if defined(LN) || defined(RT)
	mov	A, AORIG
#else
	mov	A, AO
#endif

	sra	M,  2, I
	fclr	t3
	fclr	t4
	ble	I, $L20
	.align 4

$L11:
#if defined(LT) || defined(RN)

	LD	a1,    0 * SIZE(AO)
	fclr	c11
	LD	a2,    1 * SIZE(AO)
	fclr	c12

	LD	a3,    2 * SIZE(AO)
	fclr	c16
	LD	a4,    3 * SIZE(AO)
	fclr	c15

	LD	b1,    0 * SIZE(B)
	fclr	c01
	LD	b2,    1 * SIZE(B)
	fclr	c02

	LD	b3,    2 * SIZE(B)
	fclr	c06
	LD	b4,    3 * SIZE(B)
	fclr	c05

 	lds	$f31,  4 * SIZE(C1)
	fclr	c03
	lda	L,        -2(KK)
	fclr	c04

	lds	$f31,  7 * SIZE(C2)
	fclr	c08
	lda	BO,    4 * SIZE(B)
	fclr	c13

 	lds	$f31,  4 * SIZE(C3)
 	fclr	c09
	lda	AO,    4 * SIZE(AO)
	fclr	c10

	lds	$f31,  7 * SIZE(C4)
	fclr	c14
	fclr	c07
	ble	KK, $L18
#else

#ifdef LN
	sll	K, BASE_SHIFT + 2, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 2, TMP1
	addq	AORIG, TMP1, AO
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,    0 * SIZE(AO)
	fclr	c11
	LD	a2,    1 * SIZE(AO)
	fclr	c12

	LD	a3,    2 * SIZE(AO)
	fclr	c16
	LD	a4,    3 * SIZE(AO)
	fclr	c15

	LD	b1,    0 * SIZE(BO)
	fclr	c01
	LD	b2,    1 * SIZE(BO)
	fclr	c02

	LD	b3,    2 * SIZE(BO)
	fclr	c06
	LD	b4,    3 * SIZE(BO)
	fclr	c05

 	lds	$f31,  4 * SIZE(C1)
	fclr	c03
	lda	L,        -2(TMP1)
	fclr	c04

	lds	$f31,  7 * SIZE(C2)
	fclr	c08
	lda	BO,    4 * SIZE(BO)
	fclr	c13

 	lds	$f31,  4 * SIZE(C3)
 	fclr	c09
	lda	AO,    4 * SIZE(AO)
	fclr	c10

	lds	$f31,  7 * SIZE(C4)
	fclr	c14
	fclr	c07
	ble	TMP1, $L18
#endif

	ble	L, $L15
	.align	5

$L12:
/*  1 */
	ADD	c11,  t1, c11
#ifndef EV4
	ldq	$31,   PREFETCHSIZE * SIZE(AO)
#else
	unop
#endif
	MUL	b1, a1, t1
#ifndef EV4
	ldl	$31,   PREFETCHSIZE * SIZE(BO)
#else
	unop
#endif

	ADD	c12,  t2, c12
	unop
	MUL	b1, a2, t2
	unop

	ADD	c16,  t3, c16
	unop
	MUL	b2, a2, t3
	LD	a5,   0 * SIZE(AO)

	ADD	c15, t4, c15
	unop
	MUL	b2, a1, t4
	LD	b5,   0 * SIZE(BO)

/*  2 */
	ADD	c01, t1, c01
	UNOP
	MUL	b1, a3, t1
	UNOP

	ADD	c02, t2, c02
	UNOP
	MUL	b1, a4, t2
	UNOP

	ADD	c06,  t3, c06
	unop
	MUL	b2, a4, t3
	unop

	ADD	c05, t4, c05
	unop
	MUL	b4, a1, t4
	unop

/*  3 */
	ADD	c03, t1, c03
	unop
	MUL	b3, a1, t1
	unop

	ADD	c04, t2, c04
	unop
	MUL	b3, a2, t2
	unop

	ADD	c08,  t3, c08
	unop
	MUL	b4, a2, t3
	LD	a2,  1 * SIZE(AO)

	ADD	c13, t4, c13
	unop
	MUL	b2, a3, t4
	LD	b2,  1 * SIZE(BO)

/*  4 */
	ADD	c09,  t1, c09
	unop
	MUL	b3, a3, t1
	LD	a6,  2 * SIZE(AO)

	ADD	c10,  t2, c10
	unop
	MUL	b3, a4, t2
	LD	b3,  2 * SIZE(BO)

	ADD	c14, t3, c14
	unop
	MUL	b4, a4, t3
	LD	a4,  3 * SIZE(AO)

	ADD	c07,  t4, c07
	unop
	MUL	b4, a3, t4
	LD	b4,  3 * SIZE(BO)

/*  5 */
	ADD	c11,  t1, c11
	unop
	MUL	b5,  a5,  t1
	LD	a1,  4 * SIZE(AO)

	ADD	c12,  t2, c12
	lda	L,        -2(L)
	MUL	b5,  a2, t2
	LD	b1,  4 * SIZE(BO)

	ADD	c16,  t3, c16
	unop
	MUL	b2, a2, t3
	unop

	ADD	c15, t4, c15
	unop
	MUL	b2, a5,  t4
	unop

/*  6 */
	ADD	c01, t1, c01
	unop
	MUL	b5,  a6, t1
	unop

	ADD	c02, t2, c02
	unop
	MUL	b5,  a4, t2
	unop

	ADD	c06,  t3, c06
	unop
	MUL	b2, a4, t3
	unop

	ADD	c05, t4, c05
	unop
	MUL	b4, a5,  t4
	unop

/*  7 */
	ADD	c03, t1, c03
	lda	AO,    8 * SIZE(AO)
	MUL	b3, a5,  t1
	unop

	ADD	c04, t2, c04
	lda	BO,    8 * SIZE(BO)
	MUL	b3, a2, t2
	unop

	ADD	c08,  t3, c08
	unop
	MUL	b4, a2, t3
	LD	a2, -3 * SIZE(AO)

	ADD	c13, t4, c13
	unop
	MUL	b2, a6, t4
	LD	b2, -3 * SIZE(BO)

/*  8 */
	ADD	c09,  t1, c09
	unop
	MUL	b3, a6, t1
	LD	a3, -2 * SIZE(AO)

	ADD	c10,  t2, c10
	unop
	MUL	b3, a4, t2
	LD	b3, -2 * SIZE(BO)

	ADD	c14, t3, c14
	unop
	MUL	b4, a4, t3
	LD	a4, -1 * SIZE(AO)

	ADD	c07,  t4, c07
	MUL	b4, a6, t4
	LD	b4, -1 * SIZE(BO)
	bgt	L,  $L12
	.align 4

$L15:
	ADD	c11,  t1, c11
	MUL	b1, a1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L17
#else
	blbs	TMP1, $L17
#endif
	.align 4

	ADD	c12,  t2, c12
	MUL	b1, a2, t2
	ADD	c16,  t3, c16
	MUL	b2, a2, t3

	ADD	c15, t4, c15
	MUL	b2, a1, t4
	ADD	c01, t1, c01
	MUL	b1, a3, t1

	ADD	c02, t2, c02
	unop
	MUL	b1, a4, t2
	LD	b1,  0 * SIZE(BO)

	ADD	c06,  t3, c06
	MUL	b2, a4, t3
	ADD	c05, t4, c05
	MUL	b4, a1, t4

	ADD	c03, t1, c03
	unop
	MUL	b3, a1, t1
	LD	a1,  0 * SIZE(AO)

	ADD	c04, t2, c04
	unop
	MUL	b3, a2, t2
	unop

	ADD	c08,  t3, c08
	unop
	MUL	b4, a2, t3
	LD	a2,  1 * SIZE(AO)

	ADD	c13, t4, c13
	unop
	MUL	b2, a3, t4
	LD	b2,  1 * SIZE(BO)

	ADD	c09,  t1, c09
	unop
	MUL	b3, a3, t1
	lda	AO,  4 * SIZE(AO)

	ADD	c10,  t2, c10
	unop
	MUL	b3, a4, t2
 	LD	b3,  2 * SIZE(BO)

	ADD	c14, t3, c14
	unop
	MUL	b4, a4, t3
	LD	a4, -1 * SIZE(AO)

	ADD	c07,  t4, c07
	unop
	MUL	b4, a3, t4
	LD	a3, -2 * SIZE(AO)

	ADD	c11,  t1, c11
	LD	b4,  3 * SIZE(BO)
	MUL	b1, a1, t1
	lda	BO,  4 * SIZE(BO)
	.align 4

$L17:
	ADD	c12,  t2, c12
	MUL	b1, a2, t2
	ADD	c16,  t3, c16
	MUL	b2, a2, t3

	ADD	c15, t4, c15
	MUL	b2, a1, t4
	ADD	c01, t1, c01
	MUL	b1, a3, t1

	ADD	c02, t2, c02
	MUL	b1, a4, t2
	ADD	c06,  t3, c06
	MUL	b2, a4, t3

	ADD	c05, t4, c05
	MUL	b4, a1, t4
	ADD	c03, t1, c03
	MUL	b3, a1, t1

	ADD	c04, t2, c04
	MUL	b3, a2, t2
	ADD	c08,  t3, c08
	MUL	b4, a2, t3

	ADD	c13, t4, c13
	MUL	b2, a3, t4
	ADD	c09,  t1, c09
	MUL	b3, a3, t1

	ADD	c10,  t2, c10
	MUL	b3, a4, t2
	ADD	c14, t3, c14
	MUL	b4, a4, t3

	ADD	c07,  t4, c07
	lda	AO,   4 * SIZE(AO)
	MUL	b4, a3, t4
	lda	BO,   4 * SIZE(BO)

	ADD	c11,  t1, c11
	ADD	c12,  t2, c12
	ADD	c16,  t3, c16
	ADD	c15,  t4, c15
	.align 4

$L18:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 4, TMP1
#else
	subq	KK, 4, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -4 * SIZE(AO)
	lda	BO,   -4 * SIZE(BO)
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	LD	b1,  4 * SIZE(BO)
 	LD	b2,  5 * SIZE(BO)
	LD	b3,  6 * SIZE(BO)
	LD	b4,  7 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
	SUB	a3, c09, c09
	SUB	a4, c13, c13

	SUB	b1, c02, c02
	SUB	b2, c06, c06
	SUB	b3, c10, c10
	SUB	b4, c14, c14

	LD	a1,  8 * SIZE(BO)
	LD	a2,  9 * SIZE(BO)
	LD	a3, 10 * SIZE(BO)
	LD	a4, 11 * SIZE(BO)

	LD	b1, 12 * SIZE(BO)
	LD	b2, 13 * SIZE(BO)
	LD	b3, 14 * SIZE(BO)
	LD	b4, 15 * SIZE(BO)

	SUB	a1, c03, c03
	SUB	a2, c07, c07
	SUB	a3, c11, c11
	SUB	a4, c15, c15

	SUB	b1, c04, c04
	SUB	b2, c08, c08
	SUB	b3, c12, c12
	SUB	b4, c16, c16
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  4 * SIZE(AO)
 	LD	b2,  5 * SIZE(AO)
	LD	b3,  6 * SIZE(AO)
	LD	b4,  7 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c03, c03
	SUB	a4, c04, c04

	SUB	b1, c05, c05
	SUB	b2, c06, c06
	SUB	b3, c07, c07
	SUB	b4, c08, c08

	LD	a1,  8 * SIZE(AO)
	LD	a2,  9 * SIZE(AO)
	LD	a3, 10 * SIZE(AO)
	LD	a4, 11 * SIZE(AO)

	LD	b1, 12 * SIZE(AO)
	LD	b2, 13 * SIZE(AO)
	LD	b3, 14 * SIZE(AO)
	LD	b4, 15 * SIZE(AO)

	SUB	a1, c09, c09
	SUB	a2, c10, c10
	SUB	a3, c11, c11
	SUB	a4, c12, c12

	SUB	b1, c13, c13
	SUB	b2, c14, c14
	SUB	b3, c15, c15
	SUB	b4, c16, c16
#endif

#ifdef LN
	LD	a1, 15 * SIZE(AO)
	LD	a2, 14 * SIZE(AO)
	LD	a3, 13 * SIZE(AO)
	LD	a4, 12 * SIZE(AO)

	MUL	a1, c04, c04
	MUL	a1, c08, c08
	MUL	a1, c12, c12
	MUL	a1, c16, c16

	MUL	a2, c04, t1
	MUL	a2, c08, t2
	MUL	a2, c12, t3
	MUL	a2, c16, t4

	SUB	c03, t1, c03
	SUB	c07, t2, c07
	SUB	c11, t3, c11
	SUB	c15, t4, c15

	MUL	a3, c04, t1
	MUL	a3, c08, t2
	MUL	a3, c12, t3
	MUL	a3, c16, t4

	SUB	c02, t1, c02
	SUB	c06, t2, c06
	SUB	c10, t3, c10
	SUB	c14, t4, c14

	MUL	a4, c04, t1
	MUL	a4, c08, t2
	MUL	a4, c12, t3
	MUL	a4, c16, t4

	SUB	c01, t1, c01
	SUB	c05, t2, c05
	SUB	c09, t3, c09
	SUB	c13, t4, c13

	LD	b1, 10 * SIZE(AO)
	LD	b2,  9 * SIZE(AO)
	LD	b3,  8 * SIZE(AO)

	MUL	b1, c03, c03
	MUL	b1, c07, c07
	MUL	b1, c11, c11
	MUL	b1, c15, c15

	MUL	b2, c03, t1
	MUL	b2, c07, t2
	MUL	b2, c11, t3
	MUL	b2, c15, t4

	SUB	c02, t1, c02
	SUB	c06, t2, c06
	SUB	c10, t3, c10
	SUB	c14, t4, c14

	MUL	b3, c03, t1
	MUL	b3, c07, t2
	MUL	b3, c11, t3
	MUL	b3, c15, t4

	SUB	c01, t1, c01
	SUB	c05, t2, c05
	SUB	c09, t3, c09
	SUB	c13, t4, c13

	LD	a1,  5 * SIZE(AO)
	LD	a2,  4 * SIZE(AO)
	LD	a3,  0 * SIZE(AO)

	MUL	a1, c02, c02
	MUL	a1, c06, c06
	MUL	a1, c10, c10
	MUL	a1, c14, c14

	MUL	a2, c02, t1
	MUL	a2, c06, t2
	MUL	a2, c10, t3
	MUL	a2, c14, t4

	SUB	c01, t1, c01
	SUB	c05, t2, c05
	SUB	c09, t3, c09
	SUB	c13, t4, c13

	MUL	a3, c01, c01
	MUL	a3, c05, c05
	MUL	a3, c09, c09
	MUL	a3, c13, c13
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a1, c05, c05
	MUL	a1, c09, c09
	MUL	a1, c13, c13

	MUL	a2, c01, t1
	MUL	a2, c05, t2
	MUL	a2, c09, t3
	MUL	a2, c13, t4

	SUB	c02, t1, c02
	SUB	c06, t2, c06
	SUB	c10, t3, c10
	SUB	c14, t4, c14

	MUL	a3, c01, t1
	MUL	a3, c05, t2
	MUL	a3, c09, t3
	MUL	a3, c13, t4

	SUB	c03, t1, c03
	SUB	c07, t2, c07
	SUB	c11, t3, c11
	SUB	c15, t4, c15

	MUL	a4, c01, t1
	MUL	a4, c05, t2
	MUL	a4, c09, t3
	MUL	a4, c13, t4

	SUB	c04, t1, c04
	SUB	c08, t2, c08
	SUB	c12, t3, c12
	SUB	c16, t4, c16

	LD	b1,  5 * SIZE(AO)
	LD	b2,  6 * SIZE(AO)
	LD	b3,  7 * SIZE(AO)

	MUL	b1, c02, c02
	MUL	b1, c06, c06
	MUL	b1, c10, c10
	MUL	b1, c14, c14

	MUL	b2, c02, t1
	MUL	b2, c06, t2
	MUL	b2, c10, t3
	MUL	b2, c14, t4

	SUB	c03, t1, c03
	SUB	c07, t2, c07
	SUB	c11, t3, c11
	SUB	c15, t4, c15

	MUL	b3, c02, t1
	MUL	b3, c06, t2
	MUL	b3, c10, t3
	MUL	b3, c14, t4

	SUB	c04, t1, c04
	SUB	c08, t2, c08
	SUB	c12, t3, c12
	SUB	c16, t4, c16

	LD	a1, 10 * SIZE(AO)
	LD	a2, 11 * SIZE(AO)
	LD	a3, 15 * SIZE(AO)

	MUL	a1, c03, c03
	MUL	a1, c07, c07
	MUL	a1, c11, c11
	MUL	a1, c15, c15

	MUL	a2, c03, t1
	MUL	a2, c07, t2
	MUL	a2, c11, t3
	MUL	a2, c15, t4

	SUB	c04, t1, c04
	SUB	c08, t2, c08
	SUB	c12, t3, c12
	SUB	c16, t4, c16

	MUL	a3, c04, c04
	MUL	a3, c08, c08
	MUL	a3, c12, c12
	MUL	a3, c16, c16
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	MUL	a2, c01, t1
	MUL	a2, c02, t2
	MUL	a2, c03, t3
	MUL	a2, c04, t4

	SUB	c05, t1, c05
	SUB	c06, t2, c06
	SUB	c07, t3, c07
	SUB	c08, t4, c08

	MUL	a3, c01, t1
	MUL	a3, c02, t2
	MUL	a3, c03, t3
	MUL	a3, c04, t4

	SUB	c09, t1, c09
	SUB	c10, t2, c10
	SUB	c11, t3, c11
	SUB	c12, t4, c12

	MUL	a4, c01, t1
	MUL	a4, c02, t2
	MUL	a4, c03, t3
	MUL	a4, c04, t4

	SUB	c13, t1, c13
	SUB	c14, t2, c14
	SUB	c15, t3, c15
	SUB	c16, t4, c16

	LD	b1,  5 * SIZE(BO)
	LD	b2,  6 * SIZE(BO)
	LD	b3,  7 * SIZE(BO)

	MUL	b1, c05, c05
	MUL	b1, c06, c06
	MUL	b1, c07, c07
	MUL	b1, c08, c08

	MUL	b2, c05, t1
	MUL	b2, c06, t2
	MUL	b2, c07, t3
	MUL	b2, c08, t4

	SUB	c09, t1, c09
	SUB	c10, t2, c10
	SUB	c11, t3, c11
	SUB	c12, t4, c12

	MUL	b3, c05, t1
	MUL	b3, c06, t2
	MUL	b3, c07, t3
	MUL	b3, c08, t4

	SUB	c13, t1, c13
	SUB	c14, t2, c14
	SUB	c15, t3, c15
	SUB	c16, t4, c16

	LD	a1, 10 * SIZE(BO)
	LD	a2, 11 * SIZE(BO)
	LD	a3, 15 * SIZE(BO)

	MUL	a1, c09, c09
	MUL	a1, c10, c10
	MUL	a1, c11, c11
	MUL	a1, c12, c12

	MUL	a2, c09, t1
	MUL	a2, c10, t2
	MUL	a2, c11, t3
	MUL	a2, c12, t4

	SUB	c13, t1, c13
	SUB	c14, t2, c14
	SUB	c15, t3, c15
	SUB	c16, t4, c16

	MUL	a3, c13, c13
	MUL	a3, c14, c14
	MUL	a3, c15, c15
	MUL	a3, c16, c16
#endif

#ifdef RT
	LD	a1, 15 * SIZE(BO)
	LD	a2, 14 * SIZE(BO)
	LD	a3, 13 * SIZE(BO)
	LD	a4, 12 * SIZE(BO)

	MUL	a1, c13, c13
	MUL	a1, c14, c14
	MUL	a1, c15, c15
	MUL	a1, c16, c16

	MUL	a2, c13, t1
	MUL	a2, c14, t2
	MUL	a2, c15, t3
	MUL	a2, c16, t4

	SUB	c09, t1, c09
	SUB	c10, t2, c10
	SUB	c11, t3, c11
	SUB	c12, t4, c12

	MUL	a3, c13, t1
	MUL	a3, c14, t2
	MUL	a3, c15, t3
	MUL	a3, c16, t4

	SUB	c05, t1, c05
	SUB	c06, t2, c06
	SUB	c07, t3, c07
	SUB	c08, t4, c08

	MUL	a4, c13, t1
	MUL	a4, c14, t2
	MUL	a4, c15, t3
	MUL	a4, c16, t4

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	SUB	c03, t3, c03
	SUB	c04, t4, c04

	LD	b1, 10 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3,  8 * SIZE(BO)

	MUL	b1, c09, c09
	MUL	b1, c10, c10
	MUL	b1, c11, c11
	MUL	b1, c12, c12

	MUL	b2, c09, t1
	MUL	b2, c10, t2
	MUL	b2, c11, t3
	MUL	b2, c12, t4

	SUB	c05, t1, c05
	SUB	c06, t2, c06
	SUB	c07, t3, c07
	SUB	c08, t4, c08

	MUL	b3, c09, t1
	MUL	b3, c10, t2
	MUL	b3, c11, t3
	MUL	b3, c12, t4

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	SUB	c03, t3, c03
	SUB	c04, t4, c04

	LD	a1,  5 * SIZE(BO)
	LD	a2,  4 * SIZE(BO)
	LD	a3,  0 * SIZE(BO)

	MUL	a1, c05, c05
	MUL	a1, c06, c06
	MUL	a1, c07, c07
	MUL	a1, c08, c08

	MUL	a2, c05, t1
	MUL	a2, c06, t2
	MUL	a2, c07, t3
	MUL	a2, c08, t4

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	SUB	c03, t3, c03
	SUB	c04, t4, c04

	MUL	a3, c01, c01
	MUL	a3, c02, c02
	MUL	a3, c03, c03
	MUL	a3, c04, c04
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c05,  1 * SIZE(BO)
	ST	c09,  2 * SIZE(BO)
	ST	c13,  3 * SIZE(BO)

	ST	c02,  4 * SIZE(BO)
	ST	c06,  5 * SIZE(BO)
	ST	c10,  6 * SIZE(BO)
	ST	c14,  7 * SIZE(BO)

	ST	c03,  8 * SIZE(BO)
	ST	c07,  9 * SIZE(BO)
	ST	c11, 10 * SIZE(BO)
	ST	c15, 11 * SIZE(BO)

	ST	c04, 12 * SIZE(BO)
	ST	c08, 13 * SIZE(BO)
	ST	c12, 14 * SIZE(BO)
	ST	c16, 15 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c03,  2 * SIZE(AO)
	ST	c04,  3 * SIZE(AO)

	ST	c05,  4 * SIZE(AO)
	ST	c06,  5 * SIZE(AO)
	ST	c07,  6 * SIZE(AO)
	ST	c08,  7 * SIZE(AO)

	ST	c09,  8 * SIZE(AO)
	ST	c10,  9 * SIZE(AO)
	ST	c11, 10 * SIZE(AO)
	ST	c12, 11 * SIZE(AO)

	ST	c13, 12 * SIZE(AO)
	ST	c14, 13 * SIZE(AO)
	ST	c15, 14 * SIZE(AO)
	ST	c16, 15 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -4 * SIZE(C1)
	lda	C2,  -4 * SIZE(C2)
	lda	C3,  -4 * SIZE(C3)
	lda	C4,  -4 * SIZE(C4)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c03,  2 * SIZE(C1)
	ST	c04,  3 * SIZE(C1)

	ST	c05,  0 * SIZE(C2)
 	ST	c06,  1 * SIZE(C2)
	ST	c07,  2 * SIZE(C2)
	ST	c08,  3 * SIZE(C2)

	ST	c09,  0 * SIZE(C3)
	ST	c10,  1 * SIZE(C3)
	ST	c11,  2 * SIZE(C3)
	ST	c12,  3 * SIZE(C3)

	ST	c13,  0 * SIZE(C4)
	ST	c14,  1 * SIZE(C4)
	ST	c15,  2 * SIZE(C4)
	ST	c16,  3 * SIZE(C4)

#ifndef LN
	lda	C1,   4 * SIZE(C1)
	lda	C2,   4 * SIZE(C2)
	lda	C3,   4 * SIZE(C3)
	lda	C4,   4 * SIZE(C4)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 2 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 2, TMP1
	addq	AO, TMP1, AO
	addq	BO, TMP1, BO
#endif

#ifdef LT
	addq	KK, 4, KK
#endif

#ifdef LN
	subq	KK, 4, KK
#endif

	lda	I,        -1(I)

	bgt	I, $L11
	.align 4

$L20:
	and	M,  2, I
	ble	I, $L30

#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
 	fclr	c09
	LD	a2,  1 * SIZE(AO)
	fclr	c13

	LD	a3,  2 * SIZE(AO)
	fclr	c10
	LD	a4,  3 * SIZE(AO)
	fclr	c14

	LD	b1,  0 * SIZE(B)
	lda	L,        -2(KK)
	LD	b2,  1 * SIZE(B)
	lda	AO,  2 * SIZE(AO)

	LD	b3,  2 * SIZE(B)
	fclr	c01
	LD	b4,  3 * SIZE(B)
	fclr	c05

	lda	BO,  4 * SIZE(B)
	fclr	c02
	fclr	c06
	ble	KK, $L28

	ble	L, $L25

#else
#ifdef LN
	sll	K, BASE_SHIFT + 1, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 2, TMP2
	addq	B,     TMP2, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
 	fclr	c09
	LD	a2,  1 * SIZE(AO)
	fclr	c13

	LD	a3,  2 * SIZE(AO)
	fclr	c10
	LD	a4,  3 * SIZE(AO)
	fclr	c14

	LD	b1,  0 * SIZE(BO)
	lda	L,        -2(TMP1)
	LD	b2,  1 * SIZE(BO)
	lda	AO,  2 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)
	fclr	c01
	LD	b4,  3 * SIZE(BO)
	fclr	c05

	lda	BO,  4 * SIZE(BO)
	fclr	c02
	fclr	c06
	ble	TMP1, $L28

	ble	L, $L25
#endif
	.align	4

$L22:
	ADD	c09, t1, c09
	unop
	MUL	a1, b1, t1
	unop

	ADD	c10, t2, c10
	unop
	MUL	a2, b1, t2
	LD	b1,  0 * SIZE(BO)

	ADD	c13, t3, c13
	unop
	MUL	a1, b2, t3
	lda	BO,    8 * SIZE(BO)

	ADD	c14, t4, c14
	unop
	MUL	a2, b2, t4
	LD	b2, -7 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b3, t1
	unop

	ADD	c02, t2, c02
	unop
	MUL	a2, b3, t2
	LD	b3, -6 * SIZE(BO)

	ADD	c05, t3, c05
	unop
	MUL	a1, b4, t3
	LD	a1,  2 * SIZE(AO)

	ADD	c06, t4, c06
	MUL	a2, b4, t4
	LD	b5, -5 * SIZE(BO)

	ADD	c09, t1, c09
	unop
	MUL	a3, b1, t1
	LD	a2,  3 * SIZE(AO)

	ADD	c10, t2, c10
	unop
	MUL	a4, b1, t2
	LD	b1, -4 * SIZE(BO)

	ADD	c13, t3, c13
	unop
	MUL	a3, b2, t3
	lda	AO,    4 * SIZE(AO)

	ADD	c14, t4, c14
	MUL	a4, b2, t4
	LD	b2, -3 * SIZE(BO)

	ADD	c01, t1, c01
	lda	L,        -2(L)
	MUL	a3, b3, t1
	LD	b4, -1 * SIZE(BO)

	ADD	c02, t2, c02
	unop
	MUL	a4, b3, t2
	LD	b3, -2 * SIZE(BO)

	ADD	c05, t3, c05
	unop
	MUL	a3, b5, t3
	LD	a3,  0 * SIZE(AO)

	ADD	c06, t4, c06
	MUL	a4, b5, t4
	LD	a4,  1 * SIZE(AO)
	bgt	L,  $L22
	.align 4

$L25:
	ADD	c09, t1, c09
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L27
#else
	blbs	TMP1, $L27
#endif

	ADD	c10, t2, c10
	unop
	MUL	a2, b1, t2
	LD	b1,  0 * SIZE(BO)

	ADD	c13, t3, c13
	unop
	MUL	a1, b2, t3
	unop

	ADD	c14, t4, c14
	unop
	MUL	a2, b2, t4
	LD	b2,  1 * SIZE(BO)

	ADD	c01, t1, c01
	unop
	MUL	a1, b3, t1
	lda	AO,  2 * SIZE(AO)

	ADD	c02, t2, c02
	unop
	MUL	a2, b3, t2
	LD	b3,  2 * SIZE(BO)

	ADD	c05, t3, c05
	unop
	MUL	a1, b4, t3
	LD	a1, -2 * SIZE(AO)

	ADD	c06, t4, c06
	unop
	MUL	a2, b4, t4
	LD	a2, -1 * SIZE(AO)

	ADD	c09, t1, c09
	LD	b4,  3 * SIZE(BO)
	MUL	a1, b1, t1
	lda	BO,  4 * SIZE(BO)
	.align 4

$L27:
	ADD	c10, t2, c10
	MUL	a2, b1, t2
	ADD	c13, t3, c13
	MUL	a1, b2, t3

	ADD	c14, t4, c14
	MUL	a2, b2, t4
	ADD	c01, t1, c01
	MUL	a1, b3, t1

	ADD	c02, t2, c02
	MUL	a2, b3, t2
	ADD	c05, t3, c05
	MUL	a1, b4, t3

	ADD	c06, t4, c06
	lda	AO,   2 * SIZE(AO)
	MUL	a2, b4, t4
	lda	BO,   4 * SIZE(BO)

	ADD	c09, t1, c09
	ADD	c10, t2, c10
	ADD	c13, t3, c13
	ADD	c14, t4, c14
	.align 4

$L28:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 2, TMP1
#else
	subq	KK, 4, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -2 * SIZE(AO)
	lda	BO,   -4 * SIZE(BO)
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	LD	b1,  4 * SIZE(BO)
 	LD	b2,  5 * SIZE(BO)
	LD	b3,  6 * SIZE(BO)
	LD	b4,  7 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
	SUB	a3, c09, c09
	SUB	a4, c13, c13

	SUB	b1, c02, c02
	SUB	b2, c06, c06
	SUB	b3, c10, c10
	SUB	b4, c14, c14

#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  4 * SIZE(AO)
 	LD	b2,  5 * SIZE(AO)
	LD	b3,  6 * SIZE(AO)
	LD	b4,  7 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c05, c05
	SUB	a4, c06, c06

	SUB	b1, c09, c09
	SUB	b2, c10, c10
	SUB	b3, c13, c13
	SUB	b4, c14, c14
#endif

#ifdef LN
	LD	a1,  3 * SIZE(AO)
	LD	a2,  2 * SIZE(AO)
	LD	a3,  0 * SIZE(AO)

	MUL	a1, c02, c02
	MUL	a1, c06, c06
	MUL	a1, c10, c10
	MUL	a1, c14, c14

	MUL	a2, c02, t1
	MUL	a2, c06, t2
	MUL	a2, c10, t3
	MUL	a2, c14, t4

	SUB	c01, t1, c01
	SUB	c05, t2, c05
	SUB	c09, t3, c09
	SUB	c13, t4, c13

	MUL	a3, c01, c01
	MUL	a3, c05, c05
	MUL	a3, c09, c09
	MUL	a3, c13, c13
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  3 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a1, c05, c05
	MUL	a1, c09, c09
	MUL	a1, c13, c13

	MUL	a2, c01, t1
	MUL	a2, c05, t2
	MUL	a2, c09, t3
	MUL	a2, c13, t4

	SUB	c02, t1, c02
	SUB	c06, t2, c06
	SUB	c10, t3, c10
	SUB	c14, t4, c14

	MUL	a3, c02, c02
	MUL	a3, c06, c06
	MUL	a3, c10, c10
	MUL	a3, c14, c14
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a1, c02, c02

	MUL	a2, c01, t1
	MUL	a2, c02, t2

	SUB	c05, t1, c05
	SUB	c06, t2, c06

	MUL	a3, c01, t1
	MUL	a3, c02, t2

	SUB	c09, t1, c09
	SUB	c10, t2, c10

	MUL	a4, c01, t1
	MUL	a4, c02, t2

	SUB	c13, t1, c13
	SUB	c14, t2, c14

	LD	b1,  5 * SIZE(BO)
	LD	b2,  6 * SIZE(BO)
	LD	b3,  7 * SIZE(BO)

	MUL	b1, c05, c05
	MUL	b1, c06, c06

	MUL	b2, c05, t1
	MUL	b2, c06, t2

	SUB	c09, t1, c09
	SUB	c10, t2, c10

	MUL	b3, c05, t1
	MUL	b3, c06, t2

	SUB	c13, t1, c13
	SUB	c14, t2, c14

	LD	a1, 10 * SIZE(BO)
	LD	a2, 11 * SIZE(BO)
	LD	a3, 15 * SIZE(BO)

	MUL	a1, c09, c09
	MUL	a1, c10, c10

	MUL	a2, c09, t1
	MUL	a2, c10, t2

	SUB	c13, t1, c13
	SUB	c14, t2, c14

	MUL	a3, c13, c13
	MUL	a3, c14, c14
#endif

#ifdef RT
	LD	a1, 15 * SIZE(BO)
	LD	a2, 14 * SIZE(BO)
	LD	a3, 13 * SIZE(BO)
	LD	a4, 12 * SIZE(BO)

	MUL	a1, c13, c13
	MUL	a1, c14, c14

	MUL	a2, c13, t1
	MUL	a2, c14, t2

	SUB	c09, t1, c09
	SUB	c10, t2, c10

	MUL	a3, c13, t1
	MUL	a3, c14, t2

	SUB	c05, t1, c05
	SUB	c06, t2, c06

	MUL	a4, c13, t1
	MUL	a4, c14, t2

	SUB	c01, t1, c01
	SUB	c02, t2, c02

	LD	b1, 10 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3,  8 * SIZE(BO)

	MUL	b1, c09, c09
	MUL	b1, c10, c10

	MUL	b2, c09, t1
	MUL	b2, c10, t2

	SUB	c05, t1, c05
	SUB	c06, t2, c06

	MUL	b3, c09, t1
	MUL	b3, c10, t2

	SUB	c01, t1, c01
	SUB	c02, t2, c02

	LD	a1,  5 * SIZE(BO)
	LD	a2,  4 * SIZE(BO)
	LD	a3,  0 * SIZE(BO)

	MUL	a1, c05, c05
	MUL	a1, c06, c06

	MUL	a2, c05, t1
	MUL	a2, c06, t2

	SUB	c01, t1, c01
	SUB	c02, t2, c02

	MUL	a3, c01, c01
	MUL	a3, c02, c02
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c05,  1 * SIZE(BO)
	ST	c09,  2 * SIZE(BO)
	ST	c13,  3 * SIZE(BO)

	ST	c02,  4 * SIZE(BO)
	ST	c06,  5 * SIZE(BO)
	ST	c10,  6 * SIZE(BO)
	ST	c14,  7 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c05,  2 * SIZE(AO)
	ST	c06,  3 * SIZE(AO)

	ST	c09,  4 * SIZE(AO)
	ST	c10,  5 * SIZE(AO)
	ST	c13,  6 * SIZE(AO)
	ST	c14,  7 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -2 * SIZE(C1)
	lda	C2,  -2 * SIZE(C2)
	lda	C3,  -2 * SIZE(C3)
	lda	C4,  -2 * SIZE(C4)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c05,  0 * SIZE(C2)
 	ST	c06,  1 * SIZE(C2)

	ST	c09,  0 * SIZE(C3)
	ST	c10,  1 * SIZE(C3)
	ST	c13,  0 * SIZE(C4)
	ST	c14,  1 * SIZE(C4)

#ifndef LN
	lda	C1,   2 * SIZE(C1)
	lda	C2,   2 * SIZE(C2)
	lda	C3,   2 * SIZE(C3)
	lda	C4,   2 * SIZE(C4)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, 1 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 1, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 2, KK
#endif

#ifdef LN
	subq	KK, 2, KK
#endif
	.align 4

$L30:
	and	M,  1, I
	ble	I, $L39

#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
	fclr	c01
	LD	a2,  1 * SIZE(AO)
	fclr	c05

	LD	b1,  0 * SIZE(B)
	lda	L,        -2(KK)
	LD	b2,  1 * SIZE(B)
	lda	AO,  1 * SIZE(AO)

	LD	b3,  2 * SIZE(B)
 	fclr	c09
	LD	b4,  3 * SIZE(B)
	fclr	c13

	lda	BO,  4 * SIZE(B)
	ble	KK, $L38

	ble	L, $L35
#else
#ifdef LN
	sll	K, BASE_SHIFT + 0, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, BASE_SHIFT + 0, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, BASE_SHIFT + 2, TMP2
	addq	B,     TMP2, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
	fclr	c01
	LD	a2,  1 * SIZE(AO)
	fclr	c05

	LD	b1,  0 * SIZE(BO)
	lda	L,        -2(TMP1)
	LD	b2,  1 * SIZE(BO)
	lda	AO,  1 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)
 	fclr	c09
	LD	b4,  3 * SIZE(BO)
	fclr	c13

	lda	BO,  4 * SIZE(BO)
	ble	TMP1, $L38

	ble	L, $L35
#endif
	.align	4

$L32:
	ADD	c01, t1, c01
	lda	L,        -2(L)
	MUL	a1, b1, t1
	LD	b1,  0 * SIZE(BO)

	ADD	c05, t2, c05
	lda	AO,    2 * SIZE(AO)
	MUL	a1, b2, t2
	LD	b2,  1 * SIZE(BO)

	ADD	c09, t3, c09
	LD	b5,  3 * SIZE(BO)
	MUL	a1, b3, t3
	LD	b3,  2 * SIZE(BO)

	ADD	c13, t4, c13
	MUL	a1, b4, t4
	LD	a1, -1 * SIZE(AO)

	ADD	c01, t1, c01
	MUL	a2, b1, t1
	LD	b1,  4 * SIZE(BO)
	lda	BO,    8 * SIZE(BO)

	ADD	c05, t2, c05
	MUL	a2, b2, t2
	LD	b2, -3 * SIZE(BO)

	ADD	c09, t3, c09
	LD	b4, -1 * SIZE(BO)
	MUL	a2, b3, t3
	LD	b3, -2 * SIZE(BO)

	ADD	c13, t4, c13
	MUL	a2, b5, t4
	LD	a2,  0 * SIZE(AO)
	bgt	L,  $L32
	.align 4

$L35:
	ADD	c01, t1, c01
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L37
#else
	blbs	TMP1, $L37
#endif
	.align 4

	ADD	c05, t2, c05
	LD	b1,  0 * SIZE(BO)
	MUL	a1, b2, t2
	LD	b2,  1 * SIZE(BO)

	ADD	c09, t3, c09
	MUL	a1, b3, t3
	LD	b3,  2 * SIZE(BO)

	ADD	c13, t4, c13
	MUL	a1, b4, t4
	LD	a1,  0 * SIZE(AO)
	lda	AO,  1 * SIZE(AO)

	ADD	c01, t1, c01
	LD	b4,  3 * SIZE(BO)
	MUL	a1, b1, t1
	lda	BO,  4 * SIZE(BO)
	.align 4

$L37:
	ADD	c05, t2, c05
	MUL	a1, b2, t2
	ADD	c09, t3, c09
	MUL	a1, b3, t3

	ADD	c13, t4, c13
	lda	AO,  1 * SIZE(AO)
	MUL	a1, b4, t4
	lda	BO,  4 * SIZE(BO)

	ADD	c01, t1, c01
	ADD	c05, t2, c05
	ADD	c09, t3, c09
	ADD	c13, t4, c13

$L38:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 1, TMP1
#else
	subq	KK, 4, TMP1
#endif
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -1 * SIZE(AO)
	lda	BO,   -4 * SIZE(BO)
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
	SUB	a3, c09, c09
	SUB	a4, c13, c13
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c05, c05
	SUB	a3, c09, c09
	SUB	a4, c13, c13
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(AO)

	MUL	a1, c01, c01
	MUL	a1, c05, c05
	MUL	a1, c09, c09
	MUL	a1, c13, c13
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	MUL	a1, c01, c01
	MUL	a2, c01, t1
	SUB	c05, t1, c05
	MUL	a3, c01, t1
	SUB	c09, t1, c09
	MUL	a4, c01, t1
	SUB	c13, t1, c13

	LD	b1,  5 * SIZE(BO)
	LD	b2,  6 * SIZE(BO)
	LD	b3,  7 * SIZE(BO)

	MUL	b1, c05, c05
	MUL	b2, c05, t1
	SUB	c09, t1, c09
	MUL	b3, c05, t1
	SUB	c13, t1, c13

	LD	a1, 10 * SIZE(BO)
	LD	a2, 11 * SIZE(BO)
	LD	a3, 15 * SIZE(BO)

	MUL	a1, c09, c09
	MUL	a2, c09, t1
	SUB	c13, t1, c13
	MUL	a3, c13, c13
#endif

#ifdef RT
	LD	a1, 15 * SIZE(BO)
	LD	a2, 14 * SIZE(BO)
	LD	a3, 13 * SIZE(BO)
	LD	a4, 12 * SIZE(BO)

	MUL	a1, c13, c13
	MUL	a2, c13, t1
	SUB	c09, t1, c09
	MUL	a3, c13, t1
	SUB	c05, t1, c05
	MUL	a4, c13, t1
	SUB	c01, t1, c01

	LD	b1, 10 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3,  8 * SIZE(BO)

	MUL	b1, c09, c09
	MUL	b2, c09, t1
	SUB	c05, t1, c05
	MUL	b3, c09, t1
	SUB	c01, t1, c01

	LD	a1,  5 * SIZE(BO)
	LD	a2,  4 * SIZE(BO)
	LD	a3,  0 * SIZE(BO)

	MUL	a1, c05, c05
	MUL	a2, c05, t1
	SUB	c01, t1, c01
	MUL	a3, c01, c01
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c05,  1 * SIZE(BO)
	ST	c09,  2 * SIZE(BO)
	ST	c13,  3 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c05,  1 * SIZE(AO)
	ST	c09,  2 * SIZE(AO)
	ST	c13,  3 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -1 * SIZE(C1)
	lda	C2,  -1 * SIZE(C2)
	lda	C3,  -1 * SIZE(C3)
	lda	C4,  -1 * SIZE(C4)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c05,  0 * SIZE(C2)
	ST	c09,  0 * SIZE(C3)
	ST	c13,  0 * SIZE(C4)

#ifdef RT
	sll	K, 0 + BASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, BASE_SHIFT + 0, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, BASE_SHIFT + 2, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 1, KK
#endif

#ifdef LN
	subq	KK, 1, KK
#endif
	.align 4

$L39:
#ifdef LN
	sll	K, 2 + BASE_SHIFT, TMP1
	addq	B, TMP1, B
#endif

#if defined(LT) || defined(RN)
	mov	BO,  B
#endif

#ifdef RN
	addq	KK, 4, KK
#endif

#ifdef RT
	subq	KK, 4, KK
#endif
	lda	J,        -1(J)
	bgt	J, $L01
	.align 4

$L999:
	ldt	$f2,   0($sp)
	ldt	$f3,   8($sp)
	ldt	$f4,  16($sp)
	ldt	$f5,  24($sp)
	ldt	$f6,  32($sp)
	ldt	$f7,  40($sp)
	ldt	$f8,  48($sp)
	ldt	$f9,  56($sp)
	clr	$0
	lda	$sp, STACKSIZE($sp)
	ret
	EPILOGUE
