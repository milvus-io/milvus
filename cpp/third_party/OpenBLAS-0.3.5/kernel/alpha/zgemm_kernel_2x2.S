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
#define	BB	$3
#define OFFSET  $4

#define ALPHA_R	64($sp)
#define ALPHA_I	72($sp)

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  ADD
#define ADD2	  SUB
#define ADD3	  ADD
#define ADD4	  ADD
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  ADD
#define ADD2	  ADD
#define ADD3	  SUB
#define ADD4	  ADD
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  ADD
#define ADD2	  ADD
#define ADD3	  ADD
#define ADD4	  SUB
#else
#define ADD1	  ADD
#define ADD2	  SUB
#define ADD3	  SUB
#define ADD4	  SUB
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
#ifdef TRMMKERNEL
	ldq	OFFSET,  24 + STACKSIZE($sp)
#endif

	sll	LDC, ZBASE_SHIFT, LDC

	stt	$f2,   0($sp)
	stt	$f3,   8($sp)
	stt	$f4,  16($sp)
	stt	$f5,  24($sp)
	stt	$f6,  32($sp)
	stt	$f7,  40($sp)
	stt	$f8,  48($sp)
	stt	$f9,  56($sp)
	stt	$f19, ALPHA_R
	stt	$f20, ALPHA_I

	cmple	M, 0, $0
	cmple	N, 0, $1
	cmple	K, 0, $2

	or	$0, $1, $0
	or	$0, $2, $0
	bne	$0, $L999

#if defined(TRMMKERNEL) && !defined(LEFT)
	subq	$31, OFFSET, KK
#endif

	sra	N, 1, J
	ble	J, $L30
	.align 4

$L01:
	mov	C,  C1
	addq	C,  LDC, C2
	mov	A, AO
	s4addq	K, 0, BB


#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	SXADDQ	BB, B, BB
	addq	C2, LDC, C
	unop

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
#ifndef EV4
	ldl	$31,   0 * SIZE(BB)
	ldl	$31,   8 * SIZE(BB)
	unop
	lda	BB,   16 * SIZE(BB)
#endif

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef TRMMKERNEL
#ifdef LEFT
	addq	KK, 2, TMP1
#else
	addq	KK, 2, TMP1
#endif
#endif

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
#ifndef TRMMKERNEL
	lda	L,        -2(K)
#else
	lda	L,        -2(TMP1)
#endif
	fclr	c08

	lds	$f31,  4 * SIZE(C2)
	fclr	c12
	fclr	c16
	ble	L, $L15
#else
	sll	KK, ZBASE_SHIFT + 1, TMP1
	addq	AO, TMP1, AO
	addq	B,  TMP1, BO
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
	ldt	alpha_r, ALPHA_R
	MUL	b1, a1, t1
#ifndef TRMMKERNEL
	blbs	K, $L18
#else
	blbs	TMP1, $L18
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

$L18:
	ADD3	c12,  t2, c12
	unop
	MUL	b1, a2, t2
	ldt	alpha_i, ALPHA_I

	ADD2	c16,  t3, c16
	unop
	MUL	b2, a2, t3
#ifndef TRMMKERNEL
	LD	a5, 0 * SIZE(C1)
#else
	unop
#endif

	ADD4	c15, t4, c15
	MUL	b2, a1, t4
	ADD1	c01, t1, c01
	MUL	b1, a3, t1

	ADD3	c02, t2, c02
	unop
	MUL	b1, a4, t2
#ifndef TRMMKERNEL
	LD	b1, 1 * SIZE(C1)
#else
	unop
#endif

	ADD2	c06,  t3, c06
	MUL	b2, a4, t3
	ADD4	c05, t4, c05
	MUL	b4, a1, t4

	ADD1	c03, t1, c03
	unop
	MUL	b3, a1, t1
#ifndef TRMMKERNEL
	LD	a1, 2 * SIZE(C1)
#else
	unop
#endif

	ADD3	c04, t2, c04
	unop
	MUL	b3, a2, t2
	unop

	ADD2	c08,  t3, c08
	unop
	MUL	b4, a2, t3
#ifndef TRMMKERNEL
	LD	a2, 3 * SIZE(C1)
#else
	unop
#endif

	ADD4	c13, t4, c13
	unop
	MUL	b2, a3, t4
#ifndef TRMMKERNEL
	LD	b2, 0 * SIZE(C2)
#else
	unop
#endif

	ADD1	c09,  t1, c09
	lda	I,        -1(I)
	MUL	b3, a3, t1
	unop

	ADD3	c10,  t2, c10
	unop
	MUL	b3, a4, t2
#ifndef TRMMKERNEL
	LD	b3, 1 * SIZE(C2)
#else
	unop
#endif

	ADD2	c14, t3, c14
	unop
	MUL	b4, a4, t3
#ifndef TRMMKERNEL
  	LD	a4, 2 * SIZE(C2)
#else
	unop
#endif

	ADD4	c07,  t4, c07
	unop
	MUL	b4, a3, t4
#ifndef TRMMKERNEL
	LD	a3, 3 * SIZE(C2)
#else
	unop
#endif

	ADD1	c11,  t1, c11
	ADD3	c12,  t2, c12
	ADD2	c16,  t3, c16
	ADD4	c15,  t4, c15

	ADD	c01, c06, c01
	ADD	c02, c05, c02
	ADD	c03, c08, c03
	ADD	c04, c07, c04

	ADD	c09, c14, c09
	MUL	  alpha_r, c01, t1
	ADD	c10, c13, c10
	MUL	  alpha_r, c02, t2

	ADD	c11, c16, c11
	MUL	  alpha_r, c03, t3
	ADD	c12, c15, c12
	MUL	  alpha_r, c04, t4

#ifndef TRMMKERNEL
	ADD	  a5,  t1,  a5
	MUL	  alpha_i, c02, t1
	ADD	  b1,  t2,  b1
	MUL	  alpha_i, c01, t2

	ADD	  a1,  t3,  a1
	MUL	  alpha_i, c04, t3
	ADD	  a2,  t4,  a2
	MUL	  alpha_i, c03, t4
#else
	ADD	  $f31,  t1,  a5
	MUL	  alpha_i, c02, t1
	ADD	  $f31,  t2,  b1
	MUL	  alpha_i, c01, t2

	ADD	  $f31,  t3,  a1
	MUL	  alpha_i, c04, t3
	ADD	  $f31,  t4,  a2
	MUL	  alpha_i, c03, t4
#endif

	SUB	  a5,  t1,  a5
	MUL	  alpha_r, c09, t1
	ADD	  b1,  t2,  b1
	MUL	  alpha_r, c10, t2

	SUB	  a1,  t3,  a1
	MUL	  alpha_r, c11, t3
	ADD	  a2,  t4,  a2
	MUL	  alpha_r, c12, t4

#ifndef TRMMKERNEL
	ADD	  b2,  t1,  b2
	MUL	  alpha_i, c10, t1
	ADD	  b3,  t2,  b3
	MUL	  alpha_i, c09, t2

	ADD	  a4,  t3,  a4
	MUL	  alpha_i, c12, t3
	ADD	  a3,  t4,  a3
	MUL	  alpha_i, c11, t4
#else
	ADD	  $f31,  t1,  b2
	MUL	  alpha_i, c10, t1
	ADD	  $f31,  t2,  b3
	MUL	  alpha_i, c09, t2

	ADD	  $f31,  t3,  a4
	MUL	  alpha_i, c12, t3
	ADD	  $f31,  t4,  a3
	MUL	  alpha_i, c11, t4
#endif

	SUB	  b2,  t1,  b2
	ST	a5,  0 * SIZE(C1)
	fclr	t1
	unop

	ADD	  b3,  t2,  b3
	ST	b1,  1 * SIZE(C1)
	fclr	t2
	unop

	SUB	  a4,  t3,  a4
	ST	a1,  2 * SIZE(C1)
	fclr	t3
	unop

	ADD	  a3,  t4,  a3
	ST	a2,  3 * SIZE(C1)
	fclr	t4
	unop

	ST	b2,  0 * SIZE(C2)
	fclr	c01
 	ST	b3,  1 * SIZE(C2)
	fclr	c05

	ST	a4,  2 * SIZE(C2)
	lda	C1,  4 * SIZE(C1)
	ST	a3,  3 * SIZE(C2)
	lda	C2,  4 * SIZE(C2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	subq	K, KK, TMP1
#ifdef LEFT
	subq	TMP1, 2, TMP1
#else
	subq	TMP1, 2, TMP1
#endif
	sll	TMP1, ZBASE_SHIFT + 1, TMP1
	addq	AO, TMP1, AO
	addq	BO, TMP1, BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	KK, 2, KK
#endif
	bgt	I, $L11
	.align 4

$L20:
	and	M,  1, I
	ble	I, $L29

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef TRMMKERNEL
#ifdef LEFT
	addq	KK, 1, TMP1
#else
	addq	KK, 2, TMP1
#endif
#endif

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

#ifndef TRMMKERNEL
	lda	L,        -2(K)
#else
	lda	L,        -2(TMP1)
#endif
	ble	L, $L25
#else
	sll	KK, ZBASE_SHIFT + 0, TMP1
	addq	AO, TMP1, AO
	sll	KK, ZBASE_SHIFT + 1, TMP1
	addq	B,  TMP1, BO
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
	ldt	alpha_r, ALPHA_R
	MUL	a1, b1, t1
#ifndef TRMMKERNEL
	blbs	K, $L28
#else
	blbs	TMP1, $L28
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

$L28:
	ADD3	c10, t2, c10
	unop
	MUL	a2, b1, t2
	ldt	alpha_i, ALPHA_I

	ADD4	c13, t3, c13
	unop
	MUL	a1, b2, t3
#ifndef TRMMKERNEL
	LD	c03, 0 * SIZE(C1)
#else
	unop
#endif

	ADD2	c14, t4, c14
	unop
	MUL	a2, b2, t4
#ifndef TRMMKERNEL
	LD	c04, 1 * SIZE(C1)
#else
	unop
#endif

	ADD1	c01, t1, c01
	unop
	MUL	a1, b3, t1
#ifndef TRMMKERNEL
	LD	c11, 0 * SIZE(C2)
#else
	unop
#endif

	ADD3	c02, t2, c02
	unop
	MUL	a2, b3, t2
#ifndef TRMMKERNEL
	LD	c12, 1 * SIZE(C2)
#else
	unop
#endif

	ADD4	c05, t3, c05
	MUL	a1, b4, t3
	ADD2	c06, t4, c06
	MUL	a2, b4, t4

	ADD1	c09, t1, c09
	ADD3	c10, t2, c10
	ADD4	c13, t3, c13
	ADD2	c14, t4, c14

	ADD	c01, c06, c01
	ADD	c02, c05, c02
	ADD	c09, c14, c09
	ADD	c10, c13, c10

	MUL	  alpha_r, c01, t1
	MUL	  alpha_r, c02, t2
	MUL	  alpha_r, c09, t3
	MUL	  alpha_r, c10, t4

#ifndef TRMMKERNEL
	ADD	  c03,  t1,  c03
	MUL	  alpha_i, c02, t1
	ADD	  c04,  t2,  c04
	MUL	  alpha_i, c01, t2

	ADD	  c11,  t3,  c11
	MUL	  alpha_i, c10, t3
	ADD	  c12,  t4,  c12
	MUL	  alpha_i, c09, t4
#else
	ADD	  $f31,  t1,  c03
	MUL	  alpha_i, c02, t1
	ADD	  $f31,  t2,  c04
	MUL	  alpha_i, c01, t2

	ADD	  $f31,  t3,  c11
	MUL	  alpha_i, c10, t3
	ADD	  $f31,  t4,  c12
	MUL	  alpha_i, c09, t4
#endif

	SUB	  c03,  t1,  c03
	ADD	  c04,  t2,  c04
	SUB	  c11,  t3,  c11
	ADD	  c12,  t4,  c12

	ST	c03,  0 * SIZE(C1)
	ST	c04,  1 * SIZE(C1)
	ST	c11,  0 * SIZE(C2)
 	ST	c12,  1 * SIZE(C2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	subq	K, KK, TMP1
#ifdef LEFT
	subq	TMP1, 1, TMP1
#else
	subq	TMP1, 2, TMP1
#endif
	sll	TMP1, ZBASE_SHIFT + 0, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
	addq	BO, TMP2, BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	KK, 1, KK
#endif
	.align 4

$L29:
 	mov	BO, B
	lda	J,        -1(J)
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	KK, 2, KK
#else
	unop
#endif
	bgt	J, $L01
	.align 4

$L30:
	and	N, 1, J
	ble	J, $L999

	mov	C,  C1
	mov	A, AO

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	sra	M,  1, I
	ble	I, $L50
	.align 4

$L41:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef TRMMKERNEL
#ifdef LEFT
	addq	KK, 2, TMP1
#else
	addq	KK, 1, TMP1
#endif
#endif

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

#ifndef TRMMKERNEL
	lda	L,        -2(K)
#else
	lda	L,        -2(TMP1)
#endif
	fclr	c04
	fclr	c08
	ble	L, $L45
#else
	sll	KK, ZBASE_SHIFT + 1, TMP1
	addq	AO, TMP1, AO
	sll	KK, ZBASE_SHIFT + 0, TMP1
	addq	B,  TMP1, BO
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
	ldt	alpha_r, ALPHA_R
	MUL	b1, a1, t1
#ifndef TRMMKERNEL
	blbs	K, $L48
#else
	blbs	TMP1, $L48
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

$L48:
	ADD2	c06, t2, c06
	unop
	MUL	a2, b1, t2
	ldt	alpha_i, ALPHA_I

	ADD4	c07, t3, c07
	lda	I,        -1(I)
	MUL	a3, b1, t3
#ifndef TRMMKERNEL
	LD	c09, 0 * SIZE(C1)
#else
	unop
#endif

	ADD2	c08, t4, c08
	unop
	MUL	a4, b1, t4
#ifndef TRMMKERNEL
	LD	c10, 1 * SIZE(C1)
#else
	unop
#endif

	ADD1	c01, t1, c01
	unop
	MUL	a1, b2, t1
#ifndef TRMMKERNEL
	LD	c11, 2 * SIZE(C1)
#else
	unop
#endif

	ADD3	c02, t2, c02
	unop
	MUL	a2, b2, t2
#ifndef TRMMKERNEL
	LD	c12, 3 * SIZE(C1)
#else
	unop
#endif

	ADD1	c03, t3, c03
	MUL	a3, b2, t3
	ADD3	c04, t4, c04
	MUL	a4, b2, t4

	ADD4	c05, t1, c05
	ADD2	c06, t2, c06
	ADD4	c07, t3, c07
	ADD2	c08, t4, c08

	ADD	c01, c06, c01
	ADD	c02, c05, c02
	ADD	c03, c08, c03
	ADD	c04, c07, c04

	MUL	  alpha_r, c01, t1
	MUL	  alpha_r, c02, t2
	MUL	  alpha_r, c03, t3
	MUL	  alpha_r, c04, t4

#ifndef TRMMKERNEL
	ADD	  c09,  t1,  c09
	MUL	  alpha_i, c02, t1
	ADD	  c10,  t2,  c10
	MUL	  alpha_i, c01, t2

	ADD	  c11,  t3,  c11
	MUL	  alpha_i, c04, t3
	ADD	  c12,  t4,  c12
	MUL	  alpha_i, c03, t4
#else
	ADD	  $f31,  t1,  c09
	MUL	  alpha_i, c02, t1
	ADD	  $f31,  t2,  c10
	MUL	  alpha_i, c01, t2

	ADD	  $f31,  t3,  c11
	MUL	  alpha_i, c04, t3
	ADD	  $f31,  t4,  c12
	MUL	  alpha_i, c03, t4
#endif

	SUB	  c09,  t1,  c09
	ADD	  c10,  t2,  c10
	SUB	  c11,  t3,  c11
	ADD	  c12,  t4,  c12

	ST	c09,  0 * SIZE(C1)
	ST	c10,  1 * SIZE(C1)
	ST	c11,  2 * SIZE(C1)
	ST	c12,  3 * SIZE(C1)

	lda	C1,   4 * SIZE(C1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	subq	K, KK, TMP1
#ifdef LEFT
	subq	TMP1, 2, TMP1
#else
	subq	TMP1, 1, TMP1
#endif
	sll	TMP1, ZBASE_SHIFT + 1, TMP2
	addq	AO, TMP2, AO
	sll	TMP1, ZBASE_SHIFT + 0, TMP2
	addq	BO, TMP2, BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	KK, 2, KK
#endif

	bgt	I, $L41
	.align 4

$L50:
	and	M,  1, I
	ble	I, $L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef TRMMKERNEL
#ifdef LEFT
	addq	KK, 1, TMP1
#else
	addq	KK, 1, TMP1
#endif
#endif

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

#ifndef TRMMKERNEL
	lda	L,        -2(K)
#else
	lda	L,        -2(TMP1)
#endif
	ble	L, $L55
#else
	sll	KK, ZBASE_SHIFT + 0, TMP1
	addq	AO, TMP1, AO
	addq	B,  TMP1, BO
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
	ldt	alpha_r, ALPHA_R
	MUL	a1, b1, t1
#ifndef TRMMKERNEL
	blbs	K, $L58
#else
	blbs	TMP1, $L58
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

$L58:
	ADD3	c02, t2, c02
	unop
	MUL	a2, b1, t2
	ldt	alpha_i, ALPHA_I

	ADD4	c05, t3, c05
	unop
	MUL	a1, b2, t3
#ifndef TRMMKERNEL
	LD	c03, 0 * SIZE(C1)
#else
	unop
#endif

	ADD2	c06, t4, c06
	unop
	MUL	a2, b2, t4
#ifndef TRMMKERNEL
	LD	c04, 1 * SIZE(C1)
#else
	unop
#endif

	ADD1	c01, t1, c01
	ADD3	c02, t2, c02
	ADD4	c05, t3, c05
	ADD2	c06, t4, c06

	ADD	c01, c06, c01
	ADD	c02, c05, c02

	MUL	  alpha_r, c01, t1
	MUL	  alpha_r, c02, t2
	MUL	  alpha_i, c02, t3
	MUL	  alpha_i, c01, t4

#ifndef TRMMKERNEL
	ADD	  c03,  t1,  c03
	ADD	  c04,  t2,  c04
#else
	ADD	  $f31,  t1,  c03
	ADD	  $f31,  t2,  c04
#endif

	SUB	  c03,  t3,  c03
	ADD	  c04,  t4,  c04

	ST	c03,  0 * SIZE(C1)
	ST	c04,  1 * SIZE(C1)
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
