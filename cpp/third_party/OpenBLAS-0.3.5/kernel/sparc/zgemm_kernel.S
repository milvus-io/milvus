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
#define ALPHA_R	%f60
#define ALPHA_I	%f62

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
#define ALPHA_R	%f30
#define ALPHA_I	%f31
#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define FADD1	FADD
#define FADD2	FADD
#define FADD3	FADD
#define FADD4	FSUB
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define FADD1	FADD
#define FADD2	FADD
#define FADD3	FSUB
#define FADD4	FADD
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define FADD1	FADD
#define FADD2	FSUB
#define FADD3	FADD
#define FADD4	FADD
#else
#define FADD1	FADD
#define FADD2	FSUB
#define FADD3	FSUB
#define FADD4	FSUB
#endif


#define APREFETCHSIZE 40
#define BPREFETCHSIZE 40

#define APREFETCH_CATEGORY 0
#define BPREFETCH_CATEGORY 0

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
#define STACK_ALPHA	[%sp + STACK_START + 24]
#else
#define STACK_ALPHA	[%sp + STACK_START + 20]
#endif
#else
#define STACK_ALPHA	[%sp + STACK_START + 40]
#endif

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

#ifdef DOUBLE
	FMOV	%f6, ALPHA_R
	FMOV	%f8, ALPHA_I
	STF	%f8, STACK_ALPHA
#else
	FMOV	%f7, ALPHA_R
	FMOV	%f9, ALPHA_I
	STF	%f9, STACK_ALPHA
#endif

	ldx	[%sp+  STACK_START + 56], B
	nop
	ldx	[%sp+  STACK_START + 64], C
	nop
	ldx	[%sp+  STACK_START + 72], LDC
#ifdef TRMMKERNEL
	ldx	[%sp+  STACK_START + 80], OFFSET
#endif

	LDF	[%sp + STACK_START + 32], FZERO
#endif

#ifdef DOUBLE
	FCLR(27)
#else
	FCLR(29)
#endif

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	OFFSET, KK
#endif

	sra	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL100
	sll	LDC, ZBASE_SHIFT, LDC

.LL11:
	sra	M, 1, I
	FMOV	FZERO, t1
	add	C, LDC, C2
	FMOV	FZERO, t2

	mov	C, C1
	FMOV	FZERO, t3
	cmp	I, 0

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	mov	A, AO
	add	C2, LDC, C
	nop
	ble,pn	%icc, .LL50
	FMOV	FZERO, t4


.LL21:
#if !defined(TRMMKERNEL)
	sra	K, 2, L
	FMOV	FZERO, c01
	cmp	L,  0
	FMOV	FZERO, c02

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c03
	LDF	[B  + 0 * SIZE], b1
	FMOV	FZERO, c04

	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c05
	LDF	[B  + 1 * SIZE], b2
	FMOV	FZERO, c06

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c07
	LDF	[B  + 2 * SIZE], b3
	FMOV	FZERO, c08

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c09
	LDF	[B  + 3 * SIZE], b4
	FMOV	FZERO, c10

	LDF	[B  +  4 * SIZE], b5
	FMOV	FZERO, c11
	LDF	[AO +  4 * SIZE], a5
	FMOV	FZERO, c12

	prefetch [C1 + 3 * SIZE], 3
	FMOV	FZERO, c13
	prefetch [C2 + 3 * SIZE], 3
	FMOV	FZERO, c14
	mov	B, BO

#else
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 1 + ZBASE_SHIFT, TEMP1

	add	AO, TEMP1, AO
	add	B,  TEMP1, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 2, L
#endif
	sra	L, 2, L
	cmp	L,  0

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

#endif
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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 2, L
#endif
	and	L, 3, L
#endif
	cmp	L,  0
	ble,pn %icc, .LL29
	LDF	STACK_ALPHA, ALPHA_I

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
#ifndef TRMMKERNEL
	FADD2	c04, t1, c04
	LDF	[C1 + 0 * SIZE], a1
	FADD4	c08, t2, c08
	LDF	[C1 + 1 * SIZE], a2
	FADD2	c12, t3, c12
	LDF	[C1 + 2 * SIZE], a3
	FADD4	c16, t4, c16
	LDF	[C1 + 3 * SIZE], a4

	FADD	  c01, c06, c01
	LDF	[C2 + 0 * SIZE], b1
	FADD	  c02, c05, c02
	LDF	[C2 + 1 * SIZE], b2
	FADD	  c03, c08, c03
	LDF	[C2 + 2 * SIZE], b3
	FADD	  c04, c07, c04
	LDF	[C2 + 3 * SIZE], b4

	FADD	  c09, c14, c09
	FMUL	ALPHA_R, c01, t1
	FADD	  c10, c13, c10
	FMUL	ALPHA_R, c02, t2
	FADD	  c11, c16, c11
	FMUL	ALPHA_R, c03, t3
	FADD	  c12, c15, c12
	FMUL	ALPHA_R, c04, t4

	FADD	a1, t1, a1
	FMUL	ALPHA_I, c02, t1
	FADD	a2, t2, a2
	FMUL	ALPHA_I, c01, t2
	FADD	a3, t3, a3
	FMUL	ALPHA_I, c04, t3
	FADD	a4, t4, a4
	FMUL	ALPHA_I, c03, t4

	FSUB	a1, t1, a1
	FMUL	ALPHA_R, c09, t1
	FADD	a2, t2, a2
	FMUL	ALPHA_R, c10, t2
	FSUB	a3, t3, a3
	FMUL	ALPHA_R, c11, t3
	FADD	a4, t4, a4
	FMUL	ALPHA_R, c12, t4

	FADD	b1, t1, b1
	FMUL	ALPHA_I, c10, t1
	FADD	b2, t2, b2
	FMUL	ALPHA_I, c09, t2
	FADD	b3, t3, b3
	FMUL	ALPHA_I, c12, t3
	FADD	b4, t4, b4
	FMUL	ALPHA_I, c11, t4

	STF	a1, [C1 + 0 * SIZE]
	FSUB	b1, t1, b1
	STF	a2, [C1 + 1 * SIZE]
	FADD	b2, t2, b2
	STF	a3, [C1 + 2 * SIZE]
	FSUB	b3, t3, b3
	STF	a4, [C1 + 3 * SIZE]
	FADD	b4, t4, b4

	STF	b1, [C2 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	b2, [C2 + 1 * SIZE]
	FMOV	FZERO, t2
	STF	b3, [C2 + 2 * SIZE]
	FMOV	FZERO, t3
	STF	b4, [C2 + 3 * SIZE]
	FMOV	FZERO, t4
#else
	FADD2	c04, t1, c04
	FADD4	c08, t2, c08
	FADD2	c12, t3, c12
	FADD4	c16, t4, c16

	FADD	c01, c06, c01
	FADD	c02, c05, c02
	FADD	c03, c08, c03
	FADD	c04, c07, c04

	STF	c01, [C1 + 0 * SIZE]
	FADD	c09, c14, c09
	STF	c02, [C1 + 1 * SIZE]
	FADD	c10, c13, c10
	STF	c03, [C1 + 2 * SIZE]
	FADD	c11, c16, c11
	STF	c04, [C1 + 3 * SIZE]
	FADD	c12, c15, c12

	STF	c09, [C2 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	c10, [C2 + 1 * SIZE]
	FMOV	FZERO, t2
	STF	c11, [C2 + 2 * SIZE]
	FMOV	FZERO, t3
	STF	c12, [C2 + 3 * SIZE]
	FMOV	FZERO, t4
#endif

	add	C1, 4 * SIZE, C1
	add	C2, 4 * SIZE, C2

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP1

	add	AO, TEMP1, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
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


#if !defined(TRMMKERNEL)
	LDF	[AO + 0 * SIZE], a1
	sra	K, 2, L
	FMOV	FZERO, t2
	LDF	[B  + 0 * SIZE], b1
	mov	B, BO
	FMOV	FZERO, c06
	LDF	[AO + 1 * SIZE], a2
	cmp	L,  0
	FMOV	FZERO, t3
	LDF	[B  + 1 * SIZE], b2
	FMOV	FZERO, c08
	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, t4
	LDF	[B  + 2 * SIZE], b3
	FMOV	FZERO, c01
	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c03
	LDF	[B  + 3 * SIZE], b4
	FMOV	FZERO, c05
#else
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 0 + ZBASE_SHIFT, TEMP1
	sll	KK, 1 + ZBASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 2, L
#endif
	sra	L, 2, L
	cmp	L,  0

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

#endif
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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 2, L
#endif
	and	L, 3, L
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
#ifndef TRMMKERNEL
	FADD2	c02, t1, c02
	LDF	[C1 + 0 * SIZE], a1
	FADD4	c04, t2, c04
	LDF	[C1 + 1 * SIZE], a2
	FADD2	c06, t3, c06
	LDF	[C2 + 0 * SIZE], a3
	FADD4	c08, t4, c08
	LDF	[C2 + 1 * SIZE], a4

	FADD	c01, c04, c01
	FMUL	ALPHA_R, c01, t1
	FADD	c02, c03, c02
	FMUL	ALPHA_R, c02, t2
	FADD	c05, c08, c05
	FMUL	ALPHA_R, c05, t3
	FADD	c06, c07, c06
	FMUL	ALPHA_R, c06, t4

	FADD	a1, t1, a1
	FMUL	ALPHA_I, c02, t1
	FADD	a2, t2, a2
	FMUL	ALPHA_I, c01, t2
	FADD	a3, t3, a3
	FMUL	ALPHA_I, c06, t3
	FADD	a4, t4, a4
	FMUL	ALPHA_I, c05, t4

	FSUB	a1, t1, a1
	FADD	a2, t2, a2
	FSUB	a3, t3, a3
	FADD	a4, t4, a4

	STF	a1, [C1 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	a2, [C1 + 1 * SIZE]
	FMOV	FZERO, t2
	STF	a3, [C2 + 0 * SIZE]
	FMOV	FZERO, t3
	STF	a4, [C2 + 1 * SIZE]
	FMOV	FZERO, t4
#else
	FADD2	c02, t1, c02
	FADD4	c04, t2, c04
	FADD2	c06, t3, c06
	FADD4	c08, t4, c08

	FADD	c01, c04, c01
	FADD	c02, c03, c02
	FADD	c05, c08, c05
	FADD	c06, c07, c06

	STF	c01, [C1 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	c02, [C1 + 1 * SIZE]
	FMOV	FZERO, t2
	STF	c05, [C2 + 0 * SIZE]
	FMOV	FZERO, t3
	STF	c06, [C2 + 1 * SIZE]
	FMOV	FZERO, t4
#endif

	add	C1, 2 * SIZE, C1
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
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif

.LL99:
	add	J, -1, J
	mov	BO, B
	cmp	J, 0
	bg,pt	%icc, .LL11
#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 2, KK
#else
	nop
#endif

.LL100:
	sra	M, 1, I
	and	N, 1, J

	cmp	J, 0
	ble,pn	%icc, .LL999
	mov	A, AO

	mov	C, C1
	add	C, LDC, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	cmp	I, 0
	ble,pn	%icc, .LL150
	FMOV	FZERO, c03

.LL121:
#if !defined(TRMMKERNEL)
	LDF	[AO + 0 * SIZE], a1
	sra	K, 2, L
	FMOV	FZERO, t1
	LDF	[B  + 0 * SIZE], b1
	mov	B, BO
	FMOV	FZERO, c07

	LDF	[AO + 1 * SIZE], a2
	cmp	L,  0
	FMOV	FZERO, t2
	LDF	[B  + 1 * SIZE], b2
	FMOV	FZERO, c04

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, t3
	LDF	[B  + 2 * SIZE], b3
	FMOV	FZERO, c08

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, t4
	LDF	[B  + 3 * SIZE], b4
	FMOV	FZERO, c01

	prefetch [C1 + 3 * SIZE], 3
	FMOV	FZERO, c05
	FMOV	FZERO, c02
#else
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 1 + ZBASE_SHIFT, TEMP1
	sll	KK, 0 + ZBASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 1, L
#endif
	sra	L, 2, L
	cmp	L,  0

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
#endif

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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 1, L
#endif
	and	L, 3, L
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
#ifndef TRMMKERNEL
	FADD1	c03, t1, c03
	LDF	[C1 + 0 * SIZE], a1
	FADD3	c07, t2, c07
	LDF	[C1 + 1 * SIZE], a2
	FADD2	c04, t3, c04
	LDF	[C1 + 2 * SIZE], a3
	FADD4	c08, t4, c08
	LDF	[C1 + 3 * SIZE], a4

	FADD	c01, c06, c01
	FMUL	ALPHA_R, c01, t1
	FADD	c02, c05, c02
	FMUL	ALPHA_R, c02, t2
	FADD	c03, c08, c03
	FMUL	ALPHA_R, c03, t3
	FADD	c04, c07, c04
	FMUL	ALPHA_R, c04, t4

	FADD	a1, t1, a1
	FMUL	ALPHA_I, c02, t1
	FADD	a2, t2, a2
	FMUL	ALPHA_I, c01, t2
	FADD	a3, t3, a3
	FMUL	ALPHA_I, c04, t3
	FADD	a4, t4, a4
	FMUL	ALPHA_I, c03, t4

	FSUB	a1, t1, a1
	FADD	a2, t2, a2
	FSUB	a3, t3, a3
	FADD	a4, t4, a4

	STF	a1, [C1 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	a2, [C1 + 1 * SIZE]
	FMOV	FZERO, t2
	STF	a3, [C1 + 2 * SIZE]
	FMOV	FZERO, t3
	STF	a4, [C1 + 3 * SIZE]
	FMOV	FZERO, t4
#else
	FADD1	c03, t1, c03
	FADD3	c07, t2, c07
	FADD2	c04, t3, c04
	FADD4	c08, t4, c08

	FADD	c01, c06, c01
	FADD	c02, c05, c02
	FADD	c03, c08, c03
	FADD	c04, c07, c04

	STF	c01, [C1 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	c02, [C1 + 1 * SIZE]
	FMOV	FZERO, t2
	STF	c03, [C1 + 2 * SIZE]
	FMOV	FZERO, t3
	STF	c04, [C1 + 3 * SIZE]
	FMOV	FZERO, t4
#endif

	add	C1, 4 * SIZE, C1

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif

	sll	TEMP1, 1 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL121
	FMOV	FZERO, c03

.LL150:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL999
	nop

#if !defined(TRMMKERNEL)
	LDF	[AO + 0 * SIZE], a1
	sra	K, 2, L
	FMOV	FZERO, c01

	LDF	[B  + 0 * SIZE], b1
	mov	B, BO
	FMOV	FZERO, t1

	LDF	[AO + 1 * SIZE], a2
	cmp	L,  0
	FMOV	FZERO, c02
	LDF	[B  + 1 * SIZE], b2
	FMOV	FZERO, t2

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03
	LDF	[B  + 2 * SIZE], b3
	FMOV	FZERO, t3

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[B  + 3 * SIZE], b4
	FMOV	FZERO, t4
#else
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 0 + ZBASE_SHIFT, TEMP1
	sll	KK, 0 + ZBASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 1, L
#endif
	sra	L, 2, L
	cmp	L,  0

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1

	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c02
	LDF	[BO  + 1 * SIZE], b2
	FMOV	FZERO, t2

	LDF	[AO + 2 * SIZE], a3
	FMOV	FZERO, c03
	LDF	[BO  + 2 * SIZE], b3
	FMOV	FZERO, t3

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c04
	LDF	[BO  + 3 * SIZE], b4
	FMOV	FZERO, t4
#endif

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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 1, L
#endif
	and	L, 3, L
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
#ifndef TRMMKERNEL
	FADD1	c01, t1, c01
	FADD3	c02, t2, c02
	FADD2	c03, t3, c03
	FADD4	c04, t4, c04

	LDF	[C1 + 0 * SIZE], a1
	LDF	[C1 + 1 * SIZE], a2

	FADD	c01, c04, c01
	FADD	c02, c03, c02

	FMUL	ALPHA_R, c01, t1
	FMUL	ALPHA_R, c02, t2
	FMUL	ALPHA_I, c02, t3
	FMUL	ALPHA_I, c01, t4

	FADD	a1, t1, a1
	FADD	a2, t2, a2
	FSUB	a1, t3, a1
	FADD	a2, t4, a2

	STF	a1, [C1 + 0 * SIZE]
	STF	a2, [C1 + 1 * SIZE]
#else
	FADD1	c01, t1, c01
	FADD3	c02, t2, c02
	FADD2	c03, t3, c03
	FADD4	c04, t4, c04

	FADD	c01, c04, c01
	FADD	c02, c03, c02

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
#endif

	add	C1, 2 * SIZE, C1

#ifndef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP2
	sll	TEMP1, 0 + ZBASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
