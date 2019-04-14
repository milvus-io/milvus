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
#define PREFETCHSIZE 48
#define UNOP
#endif

#ifdef EV4
#define UNOP
#endif

	.set	noat
	.set	noreorder
	.arch ev6

.text
	.align	5
	.globl	CNAME
	.ent	CNAME

#define STACKSIZE 80

#define M	$16
#define N	$17
#define K	$18
#define A	$21
#define	B	$22
#define C	$20
#define	LDC	$23

#define C1	$19
#define C2	$24

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

#define alpha_i	$f29
#define alpha_r	$f30

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

#if defined(LN) || defined(LT)
#ifndef CONJ
#define ADD1	  ADD
#define ADD2	  SUB
#define ADD3	  ADD
#define ADD4	  ADD
#define ADD5	  SUB
#define ADD6	  ADD
#else
#define ADD1	  ADD
#define ADD2	  ADD
#define ADD3	  SUB
#define ADD4	  ADD
#define ADD5	  ADD
#define ADD6	  SUB
#endif
#else
#ifndef CONJ
#define ADD1	  ADD
#define ADD2	  SUB
#define ADD3	  ADD
#define ADD4	  ADD
#define ADD5	  SUB
#define ADD6	  ADD
#else
#define ADD1	  ADD
#define ADD2	  ADD
#define ADD3	  ADD
#define ADD4	  SUB
#define ADD5	  ADD
#define ADD6	  SUB
#endif
#endif


CNAME:
	.frame	$sp, STACKSIZE, $26, 0

#ifdef PROFILE
	ldgp	$gp, 0($27)
	lda	$at, _mcount
	jsr	$at, ($at), _mcount
#endif

#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	lda	$sp, -STACKSIZE($sp)

	ldq	B,        0 + STACKSIZE($sp)
	ldq	C,        8 + STACKSIZE($sp)
	ldq	LDC,     16 + STACKSIZE($sp)
	ldq	OFFSET,  24 + STACKSIZE($sp)

	sll	LDC, ZBASE_SHIFT, LDC

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
	addq	M, M, TMP2
	mulq	TMP2, K, TMP1
	SXADDQ	TMP1, A, A
	SXADDQ	TMP2, C, C
#endif

#ifdef RN
	negq	OFFSET, KK
#endif

#ifdef RT
	mulq	N, K, TMP1
	addq	TMP1, TMP1, TMP1
	SXADDQ	TMP1, B, B

	mulq	N, LDC, TMP1
	addq	TMP1, C, C

	subq	N, OFFSET, KK
#endif

	and	N, 1, J
	ble	J, $L30

#ifdef RT
	sll	K, ZBASE_SHIFT, TMP1
	subq	B, TMP1, B

	subq	C, LDC, C1
	subq	C, LDC, C
#else
	mov	C,  C1
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

	sra	M,  1, I
	ble	I, $L50
	.align 4

$L41:
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
	fclr	c05
	LD	b3,  2 * SIZE(B)
 	fclr	c02
	LD	b4,  3 * SIZE(B)
	fclr	c06

	lda	BO,  2 * SIZE(B)
	fclr	c03
	lda	AO,  4 * SIZE(AO)
	fclr	c07

	lda	L,        -2(KK)
	fclr	c04
	fclr	c08

	ble	KK, $L48
	ble	L, $L45
#else
#ifdef LN
	sll	K, ZBASE_SHIFT + 1, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, ZBASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, ZBASE_SHIFT, TMP1
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
	fclr	c05
	LD	b3,  2 * SIZE(BO)
 	fclr	c02
	LD	b4,  3 * SIZE(BO)
	fclr	c06

	lda	BO,  2 * SIZE(BO)
	fclr	c03
	lda	AO,  4 * SIZE(AO)
	fclr	c07

	lda	L,        -2(TMP1)
	fclr	c04
	fclr	c08

	ble	TMP1, $L48
	ble	L, $L45
#endif
	.align	5

$L42:
	ADD4	c05, t1, c05
	unop
	MUL	a1, b1, t1
	unop

	ADD2	c06, t2, c06
	lda	L,   -2(L)
	MUL	a2, b1, t2
	unop

	ADD4	c07, t3, c07
	unop
	MUL	a3, b1, t3
	unop

	ADD2	c08, t4, c08
	unop
	MUL	a4, b1, t4
	LD	b1,  2 * SIZE(BO)

	ADD1	c01, t1, c01
	unop
	MUL	a1, b2, t1
	LD	a1,  0 * SIZE(AO)

	ADD3	c02, t2, c02
	lda	BO,  4 * SIZE(BO)
	MUL	a2, b2, t2
	LD	a2,  1 * SIZE(AO)

	ADD1	c03, t3, c03
	unop
	MUL	a3, b2, t3
	LD	a3,  2 * SIZE(AO)

	ADD3	c04, t4, c04
	unop
	MUL	a4, b2, t4
	LD	a5,  3 * SIZE(AO)

	ADD4	c05, t1, c05
	unop
	MUL	a1, b3, t1
	LD	b2, -1 * SIZE(BO)

	ADD2	c06, t2, c06
	unop
	MUL	a2, b3, t2
	unop

	ADD4	c07, t3, c07
	unop
	MUL	a3, b3, t3
	lda	AO,  8 * SIZE(AO)

	ADD2	c08, t4, c08
	unop
	MUL	a5, b3, t4
	LD	b3,  0 * SIZE(BO)

	ADD1	c01, t1, c01
	unop
	MUL	a1, b4, t1
	LD	a1, -4 * SIZE(AO)

	ADD3	c02, t2, c02
	unop
	MUL	a2, b4, t2
	LD	a2, -3 * SIZE(AO)

	ADD1	c03, t3, c03
	LD	a4, -1 * SIZE(AO)
	MUL	a3, b4, t3
	LD	a3, -2 * SIZE(AO)

	ADD3	c04, t4, c04
	MUL	a5, b4, t4
	LD	b4,  1 * SIZE(BO)
	bgt	L,  $L42
	.align 4

$L45:
	ADD4	c05, t1, c05
	MUL	b1, a1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L47
#else
	blbs	TMP1, $L47
#endif
	.align 4

	ADD2	c06, t2, c06
	MUL	a2, b1, t2
	ADD4	c07, t3, c07
	MUL	a3, b1, t3

	ADD2	c08, t4, c08
	unop
	MUL	a4, b1, t4
	LD	b1,  0 * SIZE(BO)

	ADD1	c01, t1, c01
	unop
	MUL	a1, b2, t1
	LD	a1,  0 * SIZE(AO)

	ADD3	c02, t2, c02
	unop
	MUL	a2, b2, t2
	LD	a2,  1 * SIZE(AO)

	ADD1	c03, t3, c03
	unop
	MUL	a3, b2, t3
	LD	a3,  2 * SIZE(AO)

	ADD3	c04, t4, c04
	MUL	a4, b2, t4
	LD	a4,  3 * SIZE(AO)
	lda	AO,  4 * SIZE(AO)

	ADD4	c05, t1, c05
	LD	b2,  1 * SIZE(BO)
	MUL	a1, b1, t1
	lda	BO,  2 * SIZE(BO)
	.align 4

$L47:
	ADD2	c06, t2, c06
	MUL	a2, b1, t2
	ADD4	c07, t3, c07
	MUL	a3, b1, t3

	ADD2	c08, t4, c08
	MUL	a4, b1, t4
	ADD1	c01, t1, c01
	MUL	a1, b2, t1

	ADD3	c02, t2, c02
	MUL	a2, b2, t2
	ADD1	c03, t3, c03
	MUL	a3, b2, t3

	ADD3	c04, t4, c04
	lda	AO,    4 * SIZE(AO)
	MUL	a4, b2, t4
	lda	BO,    2 * SIZE(BO)

	ADD4	c05, t1, c05
	ADD2	c06, t2, c06
	ADD4	c07, t3, c07
	ADD2	c08, t4, c08

	ADD	c01, c06, c01
	ADD	c02, c05, c02
	ADD	c03, c08, c03
	ADD	c04, c07, c04

$L48:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 2, TMP1
#else
	subq	KK, 1, TMP1
#endif
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, ZBASE_SHIFT, TMP2
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
	LD	a1,  6 * SIZE(AO)
	LD	a2,  7 * SIZE(AO)
	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)

	MUL	a2, c04, t1
	MUL	a2, c03, t2
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	ADD5	c03, t1, c03
	ADD6	c04, t2, c04
	MUL	a3, c03, t1
	MUL	a3, c04, t2

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	MUL	a4, c04, t1
	MUL	a4, c03, t2

	ADD6	c01, t1, c01
	ADD5	c02, t2, c02

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a1, c01, c01
	MUL	a1, c02, c02

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a1, c01, c01
	MUL	a1, c02, c02

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	MUL	a3, c01, t1
	MUL	a3, c02, t2

	SUB	c03, t1, c03
	SUB	c04, t2, c04

	MUL	a4, c02, t1
	MUL	a4, c01, t2
	ADD6	c03, t1, c03
	ADD5	c04, t2, c04

	LD	a1,  6 * SIZE(AO)
	LD	a2,  7 * SIZE(AO)

	MUL	a2, c04, t1
	MUL	a2, c03, t2
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	ADD5	c03, t1, c03
	ADD6	c04, t2, c04
#endif

#if defined(RN) || defined(RT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a2, c04, t3
	MUL	a2, c03, t4

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	ADD5	c03, t3, c03
	ADD6	c04, t4, c04
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

#ifdef RT
	sll	K, ZBASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, ZBASE_SHIFT, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 2, KK
#endif

#ifdef LN
	subq	KK, 2, KK
#endif

	lda	I,        -1(I)
	bgt	I, $L41
	.align 4

$L50:
	and	M,  1, I
	ble	I, $L59

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
	fclr	c05

	LD	b3,  2 * SIZE(B)
 	fclr	c02
	LD	b4,  3 * SIZE(B)
	fclr	c06

	lda	AO,  2 * SIZE(AO)
	lda	BO,  2 * SIZE(B)

	lda	L,        -2(KK)

	ble	KK, $L58
	ble	L, $L55
#else
#ifdef LN
	sll	K, ZBASE_SHIFT, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, ZBASE_SHIFT, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, ZBASE_SHIFT, TMP1
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
	fclr	c05

	LD	b3,  2 * SIZE(BO)
 	fclr	c02
	LD	b4,  3 * SIZE(BO)
	fclr	c06

	lda	AO,  2 * SIZE(AO)
	lda	BO,  2 * SIZE(BO)

	lda	L,        -2(TMP1)

	ble	TMP1, $L58
	ble	L, $L55
#endif
	.align	5

$L52:
	ADD1	c01, t1, c01
	unop
	MUL	a1, b1, t1
	unop

	ADD3	c02, t2, c02
	lda	AO,  4 * SIZE(AO)
	MUL	a2, b1, t2
	LD	b1,  2 * SIZE(BO)

	ADD4	c05, t3, c05
	lda	L,        -2(L)
	MUL	a1, b2, t3
	LD	a1, -2 * SIZE(AO)

	ADD2	c06, t4, c06
	unop
	MUL	a2, b2, t4
	LD	a2, -1 * SIZE(AO)

	ADD1	c01, t1, c01
	LD	b2,  3 * SIZE(BO)
	MUL	a3, b3, t1
	lda	BO,    4 * SIZE(BO)

	ADD3	c02, t2, c02
	unop
	MUL	a4, b3, t2
	LD	b3,  0 * SIZE(BO)

	ADD4	c05, t3, c05
	unop
	MUL	a3, b4, t3
	LD	a3,  0 * SIZE(AO)

	ADD2	c06, t4, c06
	MUL	a4, b4, t4
	LD	b4,  1 * SIZE(BO)
	unop

	LD	a4,  1 * SIZE(AO)
	unop
	unop
	bgt	L,  $L52
	.align 4

$L55:
	ADD1	c01, t1, c01
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L57
#else
	blbs	TMP1, $L57
#endif
	.align 4

	ADD3	c02, t2, c02
	unop
	MUL	a2, b1, t2
	LD	b1,  0 * SIZE(BO)

	ADD4	c05, t3, c05
	lda	BO,  2 * SIZE(BO)
	MUL	a1, b2, t3
	LD	a1,  0 * SIZE(AO)

	ADD2	c06, t4, c06
	unop
	MUL	a2, b2, t4
	LD	a2,  1 * SIZE(AO)

	ADD1	c01, t1, c01
	LD	b2, -1 * SIZE(BO)
	MUL	a1, b1, t1
	lda	AO,  2 * SIZE(AO)
	.align 4

$L57:
	ADD3	c02, t2, c02
	MUL	a2, b1, t2
	ADD4	c05, t3, c05
	MUL	a1, b2, t3

	ADD2	c06, t4, c06
	lda	AO,    2 * SIZE(AO)
	MUL	a2, b2, t4
	lda	BO,    2 * SIZE(BO)

	ADD1	c01, t1, c01
	ADD3	c02, t2, c02
	ADD4	c05, t3, c05
	ADD2	c06, t4, c06

	ADD	c01, c06, c01
	ADD	c02, c05, c02

$L58:
#if defined(LN) || defined(RT)
	subq	KK, 1, TMP1

	sll	TMP1, ZBASE_SHIFT, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, ZBASE_SHIFT, TMP2
	addq	B,     TMP2, BO
#else
	lda	AO,   -2 * SIZE(AO)
	lda	BO,   -2 * SIZE(BO)
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

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a1, c01, c01
	MUL	a1, c02, c02

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
#endif

#if defined(RN) || defined(RT)
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a1, c01, c01
	MUL	a1, c02, c02

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
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

#ifdef RT
	sll	K, ZBASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, ZBASE_SHIFT, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, ZBASE_SHIFT, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 1, KK
#endif

#ifdef LN
	subq	KK, 1, KK
#endif
	.align 4

$L59:
#ifdef LN
	sll	K, ZBASE_SHIFT, TMP1
	addq	B, TMP1, B
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

$L30:
	sra	N, 1, J
	ble	J, $L999
	.align 4

$L01:
#ifdef RT
	sll	K, ZBASE_SHIFT + 1, TMP1
	subq	B, TMP1, B

	subq	C,  LDC, C2
	subq	C2, LDC, C1
	subq	C2, LDC, C
#else
	mov	C,  C1
	addq	C,  LDC, C2
	addq	C2, LDC, C
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

	sra	M,  1, I
	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

	fclr	c01
	fclr	c05

	ble	I, $L20
	.align 4

$L11:
#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
 	fclr	c09
	LD	a2,  1 * SIZE(AO)
	fclr	c13

	LD	a3,  2 * SIZE(AO)
	fclr	c02
	LD	a4,  3 * SIZE(AO)
	fclr	c06

	LD	b1,  0 * SIZE(B)
	fclr	c10
	LD	b2,  1 * SIZE(B)
	fclr	c14

	LD	b3,  2 * SIZE(B)
	fclr	c03
	LD	b4,  3 * SIZE(B)
	fclr	c07

	lda	BO,  4 * SIZE(B)
	fclr	c11
	lda	AO,  4 * SIZE(AO)
	fclr	c15

 	lds	$f31,  4 * SIZE(C1)
	fclr	c04
	lda	L,        -2(KK)
	fclr	c08

	lds	$f31,  4 * SIZE(C2)
	fclr	c12
	fclr	c16
	ble	KK, $L18
	ble	L, $L15
#else
#ifdef LN
	sll	K, ZBASE_SHIFT + 1, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, ZBASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AO
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
 	fclr	c09
	LD	a2,  1 * SIZE(AO)
	fclr	c13

	LD	a3,  2 * SIZE(AO)
	fclr	c02
	LD	a4,  3 * SIZE(AO)
	fclr	c06

	LD	b1,  0 * SIZE(BO)
	fclr	c10
	LD	b2,  1 * SIZE(BO)
	fclr	c14

	LD	b3,  2 * SIZE(BO)
	fclr	c03
	LD	b4,  3 * SIZE(BO)
	fclr	c07

	lda	BO,  4 * SIZE(BO)
	fclr	c11
	lda	AO,  4 * SIZE(AO)
	fclr	c15

 	lds	$f31,  4 * SIZE(C1)
	fclr	c04
	lda	L,        -2(TMP1)
	fclr	c08

	lds	$f31,  4 * SIZE(C2)
	fclr	c12
	fclr	c16
	ble	TMP1, $L18
	ble	L, $L15
#endif
	.align	5

$L12:
/*  1 */
	ADD1	c11,  t1, c11
#ifndef EV4
	ldq	$31,  PREFETCHSIZE * SIZE(AO)
#else
	unop
#endif
	MUL	b1, a1, t1
#ifndef EV4
	ldl	$31,  PREFETCHSIZE * SIZE(BO)
#else
	unop
#endif

	ADD3	c12,  t2, c12
	unop
	MUL	b1, a2, t2
	unop

	ADD2	c16,  t3, c16
	unop
	MUL	b2, a2, t3
	LD	a5,   0 * SIZE(AO)

	ADD4	c15, t4, c15
	unop
	MUL	b2, a1, t4
	LD	b5,   0 * SIZE(BO)

/*  2 */
	ADD1	c01, t1, c01
	UNOP
	MUL	b1, a3, t1
	UNOP

	ADD3	c02, t2, c02
	UNOP
	MUL	b1, a4, t2
	UNOP

	ADD2	c06,  t3, c06
	unop
	MUL	b2, a4, t3
	unop

	ADD4	c05, t4, c05
	unop
	MUL	b4, a1, t4
	unop

/*  3 */
	ADD1	c03, t1, c03
	unop
	MUL	b3, a1, t1
	unop

	ADD3	c04, t2, c04
	unop
	MUL	b3, a2, t2
	unop

	ADD2	c08,  t3, c08
	unop
	MUL	b4, a2, t3
	LD	a2,  1 * SIZE(AO)

	ADD4	c13, t4, c13
	unop
	MUL	b2, a3, t4
	LD	b2,  1 * SIZE(BO)

/*  4 */
	ADD1	c09,  t1, c09
	unop
	MUL	b3, a3, t1
	LD	a6,  2 * SIZE(AO)

	ADD3	c10,  t2, c10
	unop
	MUL	b3, a4, t2
	LD	b3,  2 * SIZE(BO)

	ADD2	c14, t3, c14
	unop
	MUL	b4, a4, t3
	LD	a4,  3 * SIZE(AO)

	ADD4	c07,  t4, c07
	unop
	MUL	b4, a3, t4
	LD	b4,  3 * SIZE(BO)

/*  5 */
	ADD1	c11,  t1, c11
	unop
	MUL	b5,  a5,  t1
	LD	a1,  4 * SIZE(AO)

	ADD3	c12,  t2, c12
	lda	L,        -2(L)
	MUL	b5,  a2, t2
	LD	b1,  4 * SIZE(BO)

	ADD2	c16,  t3, c16
	unop
	MUL	b2, a2, t3
	unop

	ADD4	c15, t4, c15
	unop
	MUL	b2, a5,  t4
	unop

/*  6 */
	ADD1	c01, t1, c01
	unop
	MUL	b5,  a6, t1
	unop

	ADD3	c02, t2, c02
	unop
	MUL	b5,  a4, t2
	unop

	ADD2	c06,  t3, c06
	unop
	MUL	b2, a4, t3
	unop

	ADD4	c05, t4, c05
	unop
	MUL	b4, a5,  t4
	unop

/*  7 */
	ADD1	c03, t1, c03
	lda	AO,    8 * SIZE(AO)
	MUL	b3, a5,  t1
	unop

	ADD3	c04, t2, c04
	lda	BO,    8 * SIZE(BO)
	MUL	b3, a2, t2
	unop

	ADD2	c08,  t3, c08
	unop
	MUL	b4, a2, t3
	LD	a2, -3 * SIZE(AO)

	ADD4	c13, t4, c13
	unop
	MUL	b2, a6, t4
	LD	b2, -3 * SIZE(BO)

/*  8 */
	ADD1	c09,  t1, c09
	unop
	MUL	b3, a6, t1
	LD	a3, -2 * SIZE(AO)

	ADD3	c10,  t2, c10
	unop
	MUL	b3, a4, t2
	LD	b3, -2 * SIZE(BO)

	ADD2	c14, t3, c14
	unop
	MUL	b4, a4, t3
	LD	a4, -1 * SIZE(AO)

	ADD4	c07,  t4, c07
	MUL	b4, a6, t4
	LD	b4, -1 * SIZE(BO)
	bgt	L,  $L12
	.align 4

$L15:
	ADD1	c11,  t1, c11
	unop
	MUL	b1, a1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L17
#else
	blbs	TMP1, $L17
#endif
	.align 4

	ADD3	c12,  t2, c12
	MUL	b1, a2, t2
	ADD2	c16,  t3, c16
	MUL	b2, a2, t3

	ADD4	c15, t4, c15
	MUL	b2, a1, t4
	ADD1	c01, t1, c01
	MUL	b1, a3, t1

	ADD3	c02, t2, c02
	unop
	MUL	b1, a4, t2
	LD	b1,  0 * SIZE(BO)

	ADD2	c06,  t3, c06
	MUL	b2, a4, t3
	ADD4	c05, t4, c05
	MUL	b4, a1, t4

	ADD1	c03, t1, c03
	unop
	MUL	b3, a1, t1
	LD	a1,  0 * SIZE(AO)

	ADD3	c04, t2, c04
	unop
	MUL	b3, a2, t2
	unop

	ADD2	c08,  t3, c08
	unop
	MUL	b4, a2, t3
	LD	a2,  1 * SIZE(AO)

	ADD4	c13, t4, c13
	unop
	MUL	b2, a3, t4
	LD	b2,  1 * SIZE(BO)

	ADD1	c09,  t1, c09
	unop
	MUL	b3, a3, t1
	lda	AO,  4 * SIZE(AO)

	ADD3	c10,  t2, c10
	unop
	MUL	b3, a4, t2
 	LD	b3,  2 * SIZE(BO)

	ADD2	c14, t3, c14
	unop
	MUL	b4, a4, t3
	LD	a4, -1 * SIZE(AO)

	ADD4	c07,  t4, c07
	unop
	MUL	b4, a3, t4
	LD	a3, -2 * SIZE(AO)

	ADD1	c11,  t1, c11
	LD	b4,  3 * SIZE(BO)
	MUL	b1, a1, t1
	lda	BO,  4 * SIZE(BO)
	.align 4

$L17:
	ADD3	c12,  t2, c12
	MUL	b1, a2, t2
	ADD2	c16,  t3, c16
	MUL	b2, a2, t3

	ADD4	c15, t4, c15
	MUL	b2, a1, t4
	ADD1	c01, t1, c01
	MUL	b1, a3, t1

	ADD3	c02, t2, c02
	MUL	b1, a4, t2
	ADD2	c06,  t3, c06
	MUL	b2, a4, t3

	ADD4	c05, t4, c05
	MUL	b4, a1, t4
	ADD1	c03, t1, c03
	MUL	b3, a1, t1

	ADD3	c04, t2, c04
	MUL	b3, a2, t2
	ADD2	c08,  t3, c08
	MUL	b4, a2, t3

	ADD4	c13, t4, c13
	MUL	b2, a3, t4
	ADD1	c09,  t1, c09
	MUL	b3, a3, t1

	ADD3	c10,  t2, c10
	MUL	b3, a4, t2
	ADD2	c14, t3, c14
	MUL	b4, a4, t3

	ADD4	c07,  t4, c07
	lda	AO,    4 * SIZE(AO)
	MUL	b4, a3, t4
	lda	BO,    4 * SIZE(BO)

	ADD1	c11,  t1, c11
	ADD3	c12,  t2, c12
	ADD2	c16,  t3, c16
	ADD4	c15,  t4, c15

	ADD	c01, c06, c01
	ADD	c02, c05, c02
	ADD	c03, c08, c03
	ADD	c04, c07, c04

	ADD	c09, c14, c09
	ADD	c10, c13, c10
	ADD	c11, c16, c11
	ADD	c12, c15, c12
	.align 4

$L18:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 2, TMP1
#else
	subq	KK, 2, TMP1
#endif
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
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
	SUB	a2, c02, c02
	SUB	a3, c09, c09
	SUB	a4, c10, c10

	SUB	b1, c03, c03
	SUB	b2, c04, c04
	SUB	b3, c11, c11
	SUB	b4, c12, c12
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

	SUB	b1, c09, c09
	SUB	b2, c10, c10
	SUB	b3, c11, c11
	SUB	b4, c12, c12
#endif

#ifdef LN
	LD	a1,  6 * SIZE(AO)
	LD	a2,  7 * SIZE(AO)
	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)

	MUL	a2, c04, t1
	MUL	a2, c03, t2
	MUL	a2, c12, t3
	MUL	a2, c11, t4

	MUL	a1, c03, c03
	MUL	a1, c04, c04
	MUL	a1, c11, c11
	MUL	a1, c12, c12

	ADD5	c03, t1, c03
	ADD6	c04, t2, c04
	ADD5	c11, t3, c11
	ADD6	c12, t4, c12

	MUL	a3, c03, t1
	MUL	a3, c04, t2
	MUL	a3, c11, t3
	MUL	a3, c12, t4

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	SUB	c09, t3, c09
	SUB	c10, t4, c10

	MUL	a4, c04, t1
	MUL	a4, c03, t2
	MUL	a4, c12, t3
	MUL	a4, c11, t4

	ADD6	c01, t1, c01
	ADD5	c02, t2, c02
	ADD6	c09, t3, c09
	ADD5	c10, t4, c10

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a2, c10, t3
	MUL	a2, c09, t4

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c09, c09
	MUL	a1, c10, c10

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	ADD5	c09, t3, c09
	ADD6	c10, t4, c10
#endif

#ifdef LT
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a2, c10, t3
	MUL	a2, c09, t4

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c09, c09
	MUL	a1, c10, c10

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	ADD5	c09, t3, c09
	ADD6	c10, t4, c10

	MUL	a3, c01, t1
	MUL	a3, c02, t2
	MUL	a3, c09, t3
	MUL	a3, c10, t4

	SUB	c03, t1, c03
	SUB	c04, t2, c04
	SUB	c11, t3, c11
	SUB	c12, t4, c12

	MUL	a4, c02, t1
	MUL	a4, c01, t2
	MUL	a4, c10, t3
	MUL	a4, c09, t4

	ADD6	c03, t1, c03
	ADD5	c04, t2, c04
	ADD6	c11, t3, c11
	ADD5	c12, t4, c12

	LD	a1,  6 * SIZE(AO)
	LD	a2,  7 * SIZE(AO)

	MUL	a2, c04, t1
	MUL	a2, c03, t2
	MUL	a2, c12, t3
	MUL	a2, c11, t4

	MUL	a1, c03, c03
	MUL	a1, c04, c04
	MUL	a1, c11, c11
	MUL	a1, c12, c12

	ADD5	c03, t1, c03
	ADD6	c04, t2, c04
	ADD5	c11, t3, c11
	ADD6	c12, t4, c12
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a2, c04, t3
	MUL	a2, c03, t4

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	ADD5	c03, t3, c03
	ADD6	c04, t4, c04

	MUL	a3, c01, t1
	MUL	a3, c02, t2
	MUL	a3, c03, t3
	MUL	a3, c04, t4

	SUB	c09, t1, c09
	SUB	c10, t2, c10
	SUB	c11, t3, c11
	SUB	c12, t4, c12

	MUL	a4, c02, t1
	MUL	a4, c01, t2
	MUL	a4, c04, t3
	MUL	a4, c03, t4

	ADD6	c09, t1, c09
	ADD5	c10, t2, c10
	ADD6	c11, t3, c11
	ADD5	c12, t4, c12

	LD	a1,  6 * SIZE(BO)
	LD	a2,  7 * SIZE(BO)

	MUL	a2, c10, t1
	MUL	a2, c09, t2
	MUL	a2, c12, t3
	MUL	a2, c11, t4

	MUL	a1, c09, c09
	MUL	a1, c10, c10
	MUL	a1, c11, c11
	MUL	a1, c12, c12

	ADD5	c09, t1, c09
	ADD6	c10, t2, c10
	ADD5	c11, t3, c11
	ADD6	c12, t4, c12
#endif

#ifdef RT
	LD	a1,  6 * SIZE(BO)
	LD	a2,  7 * SIZE(BO)
	LD	a3,  4 * SIZE(BO)
	LD	a4,  5 * SIZE(BO)

	MUL	a2, c10, t1
	MUL	a2, c09, t2
	MUL	a2, c12, t3
	MUL	a2, c11, t4

	MUL	a1, c09, c09
	MUL	a1, c10, c10
	MUL	a1, c11, c11
	MUL	a1, c12, c12

	ADD5	c09, t1, c09
	ADD6	c10, t2, c10
	ADD5	c11, t3, c11
	ADD6	c12, t4, c12

	MUL	a3, c09, t1
	MUL	a3, c10, t2
	MUL	a3, c11, t3
	MUL	a3, c12, t4

	SUB	c01, t1, c01
	SUB	c02, t2, c02
	SUB	c03, t3, c03
	SUB	c04, t4, c04

	MUL	a4, c10, t1
	MUL	a4, c09, t2
	MUL	a4, c12, t3
	MUL	a4, c11, t4

	ADD6	c01, t1, c01
	ADD5	c02, t2, c02
	ADD6	c03, t3, c03
	ADD5	c04, t4, c04

	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a2, c04, t3
	MUL	a2, c03, t4

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c03, c03
	MUL	a1, c04, c04

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	ADD5	c03, t3, c03
	ADD6	c04, t4, c04
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c02,  1 * SIZE(BO)
	ST	c09,  2 * SIZE(BO)
	ST	c10,  3 * SIZE(BO)

	ST	c03,  4 * SIZE(BO)
	ST	c04,  5 * SIZE(BO)
	ST	c11,  6 * SIZE(BO)
	ST	c12,  7 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c03,  2 * SIZE(AO)
	ST	c04,  3 * SIZE(AO)

	ST	c09,  4 * SIZE(AO)
	ST	c10,  5 * SIZE(AO)
	ST	c11,  6 * SIZE(AO)
	ST	c12,  7 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -4 * SIZE(C1)
	lda	C2,  -4 * SIZE(C2)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c03,  2 * SIZE(C1)
	ST	c04,  3 * SIZE(C1)

	ST	c09,  0 * SIZE(C2)
 	ST	c10,  1 * SIZE(C2)
	ST	c11,  2 * SIZE(C2)
	ST	c12,  3 * SIZE(C2)

#ifndef LN
	lda	C1,   4 * SIZE(C1)
	lda	C2,   4 * SIZE(C2)
#endif

	fclr	t1
	fclr	t2
	fclr	t3
	fclr	t4

#ifdef RT
	sll	K, ZBASE_SHIFT + 1, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, ZBASE_SHIFT + 1, TMP1
	addq	AO, TMP1, AO
	addq	BO, TMP1, BO
#endif

#ifdef LT
	addq	KK, 2, KK
#endif

#ifdef LN
	subq	KK, 2, KK
#endif
	fclr	c01
	fclr	c05

	lda	I,        -1(I)
	bgt	I, $L11
	.align 4

$L20:
	and	M,  1, I
	ble	I, $L29

#if defined(LT) || defined(RN)

	LD	a1,  0 * SIZE(AO)
 	fclr	c09
	LD	a2,  1 * SIZE(AO)
	fclr	c13

	LD	a3,  2 * SIZE(AO)
	fclr	c02
	LD	a4,  3 * SIZE(AO)
	fclr	c06

	LD	b1,  0 * SIZE(B)
	fclr	c10
	LD	b2,  1 * SIZE(B)
	fclr	c14

	LD	b3,  2 * SIZE(B)
	lda	AO,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(B)
	lda	BO,  4 * SIZE(B)

	lda	L,        -2(KK)

	ble	KK, $L28
	ble	L, $L25
#else
#ifdef LN
	sll	K, ZBASE_SHIFT + 0, TMP1
	subq	AORIG, TMP1, AORIG
#endif

	sll	KK, ZBASE_SHIFT + 0, TMP1
	addq	AORIG, TMP1, AO
	sll	KK, ZBASE_SHIFT + 1, TMP1
	addq	B,     TMP1, BO

	subq	K, KK, TMP1

	LD	a1,  0 * SIZE(AO)
 	fclr	c09
	LD	a2,  1 * SIZE(AO)
	fclr	c13

	LD	a3,  2 * SIZE(AO)
	fclr	c02
	LD	a4,  3 * SIZE(AO)
	fclr	c06

	LD	b1,  0 * SIZE(BO)
	fclr	c10
	LD	b2,  1 * SIZE(BO)
	fclr	c14

	LD	b3,  2 * SIZE(BO)
	lda	AO,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(BO)
	lda	BO,  4 * SIZE(BO)

	lda	L,        -2(TMP1)

	ble	TMP1, $L28
	ble	L, $L25
#endif
	.align	5

$L22:
	ADD1	c09, t1, c09
	unop
	MUL	a1, b1, t1
	unop

	ADD3	c10, t2, c10
	unop
	MUL	a2, b1, t2
	LD	b1,  0 * SIZE(BO)

	ADD4	c13, t3, c13
	unop
	MUL	a1, b2, t3
	lda	BO,    8 * SIZE(BO)

	ADD2	c14, t4, c14
	unop
	MUL	a2, b2, t4
	LD	b2, -7 * SIZE(BO)

	ADD1	c01, t1, c01
	unop
	MUL	a1, b3, t1
	unop

	ADD3	c02, t2, c02
	unop
	MUL	a2, b3, t2
	LD	b3, -6 * SIZE(BO)

	ADD4	c05, t3, c05
	unop
	MUL	a1, b4, t3
	LD	a1,  2 * SIZE(AO)

	ADD2	c06, t4, c06
	MUL	a2, b4, t4
	LD	b5, -5 * SIZE(BO)

	ADD1	c09, t1, c09
	unop
	MUL	a3, b1, t1
	LD	a2,  3 * SIZE(AO)

	ADD3	c10, t2, c10
	unop
	MUL	a4, b1, t2
	LD	b1, -4 * SIZE(BO)

	ADD4	c13, t3, c13
	unop
	MUL	a3, b2, t3
	lda	AO,    4 * SIZE(AO)

	ADD2	c14, t4, c14
	MUL	a4, b2, t4
	LD	b2, -3 * SIZE(BO)

	ADD1	c01, t1, c01
	lda	L,        -2(L)
	MUL	a3, b3, t1
	LD	b4, -1 * SIZE(BO)

	ADD3	c02, t2, c02
	unop
	MUL	a4, b3, t2
	LD	b3, -2 * SIZE(BO)

	ADD4	c05, t3, c05
	unop
	MUL	a3, b5, t3
	LD	a3,  0 * SIZE(AO)

	ADD2	c06, t4, c06
	MUL	a4, b5, t4
	LD	a4,  1 * SIZE(AO)
	bgt	L,  $L22
	.align 4

$L25:
	ADD1	c09, t1, c09
	MUL	a1, b1, t1
#if defined(LT) || defined(RN)
	blbs	KK, $L27
#else
	blbs	TMP1, $L27
#endif
	.align 4

	ADD3	c10, t2, c10
	unop
	MUL	a2, b1, t2
	LD	b1,  0 * SIZE(BO)

	ADD4	c13, t3, c13
	unop
	MUL	a1, b2, t3
	unop

	ADD2	c14, t4, c14
	unop
	MUL	a2, b2, t4
	LD	b2,  1 * SIZE(BO)

	ADD1	c01, t1, c01
	unop
	MUL	a1, b3, t1
	lda	AO,  2 * SIZE(AO)

	ADD3	c02, t2, c02
	unop
	MUL	a2, b3, t2
	LD	b3,  2 * SIZE(BO)

	ADD4	c05, t3, c05
	unop
	MUL	a1, b4, t3
	LD	a1, -2 * SIZE(AO)

	ADD2	c06, t4, c06
	unop
	MUL	a2, b4, t4
	LD	a2, -1 * SIZE(AO)

	ADD1	c09, t1, c09
	LD	b4,  3 * SIZE(BO)
	MUL	a1, b1, t1
	lda	BO,  4 * SIZE(BO)
	.align 4

$L27:
	ADD3	c10, t2, c10
	MUL	a2, b1, t2
	ADD4	c13, t3, c13
	MUL	a1, b2, t3

	ADD2	c14, t4, c14
	MUL	a2, b2, t4
	ADD1	c01, t1, c01
	MUL	a1, b3, t1

	ADD3	c02, t2, c02
	MUL	a2, b3, t2
	ADD4	c05, t3, c05
	MUL	a1, b4, t3

	ADD2	c06, t4, c06
	lda	AO,    2 * SIZE(AO)
	MUL	a2, b4, t4
	lda	BO,    4 * SIZE(BO)

	ADD1	c09, t1, c09
	ADD3	c10, t2, c10
	ADD4	c13, t3, c13
	ADD2	c14, t4, c14

	ADD	c01, c06, c01
	ADD	c02, c05, c02
	ADD	c09, c14, c09
	ADD	c10, c13, c10
	.align 4

$L28:
#if defined(LN) || defined(RT)
#ifdef LN
	subq	KK, 1, TMP1
#else
	subq	KK, 2, TMP1
#endif
	sll	TMP1, ZBASE_SHIFT + 0, TMP2
	addq	AORIG, TMP2, AO
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
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

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c09, c09
	SUB	a4, c10, c10
#else
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	SUB	a1, c01, c01
	SUB	a2, c02, c02
	SUB	a3, c09, c09
	SUB	a4, c10, c10
#endif

#if defined(LN) || defined(LT)
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a2, c10, t3
	MUL	a2, c09, t4

	MUL	a1, c01, c01
	MUL	a1, c02, c02
	MUL	a1, c09, c09
	MUL	a1, c10, c10

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
	ADD5	c09, t3, c09
	ADD6	c10, t4, c10
#endif

#ifdef RN
	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)
	LD	a3,  2 * SIZE(BO)
	LD	a4,  3 * SIZE(BO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a1, c01, c01
	MUL	a1, c02, c02

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02

	MUL	a3, c01, t1
	MUL	a3, c02, t2
	SUB	c09, t1, c09
	SUB	c10, t2, c10

	MUL	a4, c02, t1
	MUL	a4, c01, t2
	ADD6	c09, t1, c09
	ADD5	c10, t2, c10

	LD	a1,  6 * SIZE(BO)
	LD	a2,  7 * SIZE(BO)

	MUL	a2, c10, t1
	MUL	a2, c09, t2
	MUL	a1, c09, c09
	MUL	a1, c10, c10

	ADD5	c09, t1, c09
	ADD6	c10, t2, c10
#endif

#ifdef RT
	LD	a1,  6 * SIZE(BO)
	LD	a2,  7 * SIZE(BO)
	LD	a3,  4 * SIZE(BO)
	LD	a4,  5 * SIZE(BO)

	MUL	a2, c10, t1
	MUL	a2, c09, t2
	MUL	a1, c09, c09
	MUL	a1, c10, c10

	ADD5	c09, t1, c09
	ADD6	c10, t2, c10

	MUL	a3, c09, t1
	MUL	a3, c10, t2
	SUB	c01, t1, c01
	SUB	c02, t2, c02

	MUL	a4, c10, t1
	MUL	a4, c09, t2
	ADD6	c01, t1, c01
	ADD5	c02, t2, c02

	LD	a1,  0 * SIZE(BO)
	LD	a2,  1 * SIZE(BO)

	MUL	a2, c02, t1
	MUL	a2, c01, t2
	MUL	a1, c01, c01
	MUL	a1, c02, c02

	ADD5	c01, t1, c01
	ADD6	c02, t2, c02
#endif

#if defined(LN) || defined(LT)
	ST	c01,  0 * SIZE(BO)
	ST	c02,  1 * SIZE(BO)
	ST	c09,  2 * SIZE(BO)
	ST	c10,  3 * SIZE(BO)
#else
	ST	c01,  0 * SIZE(AO)
	ST	c02,  1 * SIZE(AO)
	ST	c09,  2 * SIZE(AO)
	ST	c10,  3 * SIZE(AO)
#endif

#ifdef LN
	lda	C1,  -2 * SIZE(C1)
	lda	C2,  -2 * SIZE(C2)
#endif

	ST	c01,  0 * SIZE(C1)
	ST	c02,  1 * SIZE(C1)
	ST	c09,  0 * SIZE(C2)
 	ST	c10,  1 * SIZE(C2)

#ifndef LN
	lda	C1,   2 * SIZE(C1)
	lda	C2,   2 * SIZE(C2)
#endif

#ifdef RT
	sll	K, ZBASE_SHIFT, TMP1
	addq	AORIG, TMP1, AORIG
#endif

#if defined(LT) || defined(RN)
	subq	K, KK, TMP1
	sll	TMP1, ZBASE_SHIFT + 0, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
	addq	BO, TMP2, BO
#endif

#ifdef LT
	addq	KK, 1, KK
#endif

#ifdef LN
	subq	KK, 1, KK
#endif
	.align 4

$L29:
#ifdef LN
	sll	K, ZBASE_SHIFT + 1, TMP1
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
	.ident	VERSION
	.end	CNAME
