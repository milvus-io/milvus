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
#define ALPHA	%f62
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
#define ALPHA	%f30
#endif

	PROLOGUE
	SAVESP
	nop

#ifndef __64BIT__

#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]   /* ALPHA */
	st	%i4, [%sp + STACK_START + 20]

	ld	[%sp + STACK_START + 28], B
	ld	[%sp + STACK_START + 32], C
	ld	[%sp + STACK_START + 36], LDC
#ifdef TRMMKERNEL
	ld	[%sp + STACK_START + 40], OFFSET
#endif
#else
	st	%i3, [%sp + STACK_START + 16]   /* ALPHA */

	ld	[%sp + STACK_START + 28], C
	ld	[%sp + STACK_START + 32], LDC
#ifdef TRMMKERNEL
	ld	[%sp + STACK_START + 36], OFFSET
#endif
#endif
	LDF	[%sp + STACK_START + 16], ALPHA
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
#endif

	FCLR(29)

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	OFFSET, KK
#endif

	sra	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL100
	sll	LDC, BASE_SHIFT, LDC

.LL11:
	add	C, LDC, C2
	FMOV	FZERO, t1
	nop
	mov	C, C1

	add	C2, LDC, C3
	FMOV	FZERO, t2
	sra	K, 2, L
	mov	A, AO

	sra	M, 2, I
	add	C3, LDC, C4
	FMOV	FZERO, t3

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	cmp	I, 0
	add	C4, LDC, C
	FMOV	FZERO, t4

	ble,pn	%icc, .LL50
	FMOV	FZERO, c01

.LL21:
#if !defined(TRMMKERNEL)
	FMOV	FZERO, c02
	mov	B, BO

	FMOV	FZERO, c03
	cmp	L,  0
#else
	FMOV	FZERO, c02
	FMOV	FZERO, c03

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 2 + BASE_SHIFT, TEMP1

	add	AO, TEMP1, AO
	add	B,  TEMP1, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 4, L
#else
	add	KK, 4, L
#endif
	sra	L, 2, L
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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 4, L
#else
	add	KK, 4, L
#endif
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c04, t1, c04
	add	I, -1, I
	FMUL	c01, ALPHA, c01
	LDF	[C1 + 0 * SIZE], a1

	FADD	c08, t2, c08
	cmp	I, 0
	FMUL	c02, ALPHA, c02
	LDF	[C1 + 1 * SIZE], a2

	FADD	c12, t3, c12
	nop
	FMUL	c03, ALPHA, c03
	LDF	[C1 + 2 * SIZE], a3

	FADD	c16, t4, c16
	nop
	FMUL	c04, ALPHA, c04
	LDF	[C1 + 3 * SIZE], a4

	FMUL	c05, ALPHA, c05
	LDF	[C2 + 0 * SIZE], b1
	FMUL	c06, ALPHA, c06
	LDF	[C2 + 1 * SIZE], b2

	FMUL	c07, ALPHA, c07
	LDF	[C2 + 2 * SIZE], b3
	FMUL	c08, ALPHA, c08
	LDF	[C2 + 3 * SIZE], b4

	FMUL	c09, ALPHA, c09
	LDF	[C3 + 0 * SIZE], t1
	FMUL	c10, ALPHA, c10
	LDF	[C3 + 1 * SIZE], t2

	FMUL	c11, ALPHA, c11
	LDF	[C3 + 2 * SIZE], t3
	FMUL	c12, ALPHA, c12
	LDF	[C3 + 3 * SIZE], t4

	FMUL	c13, ALPHA, c13
	add	C1, 4 * SIZE, C1
	FADD	c01, a1, c01
	LDF	[C4 + 0 * SIZE], a1

	FMUL	c14, ALPHA, c14
	add	C2, 4 * SIZE, C2
	FADD	c02, a2, c02
	LDF	[C4 + 1 * SIZE], a2

	FMUL	c15, ALPHA, c15
	add	C3, 4 * SIZE, C3
	FADD	c03, a3, c03
	LDF	[C4 + 2 * SIZE], a3

	FMUL	c16, ALPHA, c16
	nop
	FADD	c04, a4, c04
	LDF	[C4 + 3 * SIZE], a4

	STF	c01, [C1 - 4 * SIZE]
	FADD	c05, b1, c05
	STF	c02, [C1 - 3 * SIZE]
	FADD	c06, b2, c06

	STF	c03, [C1 - 2 * SIZE]
	FADD	c07, b3, c07
	STF	c04, [C1 - 1 * SIZE]
	FADD	c08, b4, c08

	STF	c05, [C2 - 4 * SIZE]
	FADD	c09, t1, c09
	STF	c06, [C2 - 3 * SIZE]
	FADD	c10, t2, c10

	STF	c07, [C2 - 2 * SIZE]
	FADD	c11, t3, c11
	STF	c08, [C2 - 1 * SIZE]
	FADD	c12, t4, c12

	STF	c09, [C3 - 4 * SIZE]
	FADD	c13, a1, c13
	STF	c10, [C3 - 3 * SIZE]
	FADD	c14, a2, c14

	STF	c11, [C3 - 2 * SIZE]
	FADD	c15, a3, c15
	STF	c12, [C3 - 1 * SIZE]
	FADD	c16, a4, c16

	STF	c13, [C4 + 0 * SIZE]
	FMOV	FZERO, t1
	STF	c14, [C4 + 1 * SIZE]
	FMOV	FZERO, t2

	STF	c15, [C4 + 2 * SIZE]
	FMOV	FZERO, t3
	STF	c16, [C4 + 3 * SIZE]
	FMOV	FZERO, t4

	add	C4, 4 * SIZE, C4
#else

	FADD	c04, t1, c04
	FMUL	c01, ALPHA, c01
	FADD	c08, t2, c08
	FMUL	c02, ALPHA, c02
	FADD	c12, t3, c12
	FMUL	c03, ALPHA, c03
	FADD	c16, t4, c16
	FMUL	c04, ALPHA, c04

	STF	c01, [C1 + 0 * SIZE]
	FMUL	c05, ALPHA, c05
	STF	c02, [C1 + 1 * SIZE]
	FMUL	c06, ALPHA, c06
	STF	c03, [C1 + 2 * SIZE]
	FMUL	c07, ALPHA, c07
	STF	c04, [C1 + 3 * SIZE]
	FMUL	c08, ALPHA, c08

	STF	c05, [C2 + 0 * SIZE]
	FMUL	c09, ALPHA, c09
	STF	c06, [C2 + 1 * SIZE]
	FMUL	c10, ALPHA, c10
	STF	c07, [C2 + 2 * SIZE]
	FMUL	c11, ALPHA, c11
	STF	c08, [C2 + 3 * SIZE]
	FMUL	c12, ALPHA, c12

	STF	c09, [C3 + 0 * SIZE]
	FMUL	c13, ALPHA, c13
	STF	c10, [C3 + 1 * SIZE]
	FMUL	c14, ALPHA, c14
	STF	c11, [C3 + 2 * SIZE]
	FMUL	c15, ALPHA, c15
	STF	c12, [C3 + 3 * SIZE]
	FMUL	c16, ALPHA, c16

	STF	c13, [C4 + 0 * SIZE]
	STF	c14, [C4 + 1 * SIZE]
	STF	c15, [C4 + 2 * SIZE]
	STF	c16, [C4 + 3 * SIZE]

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

	add	C1, 4 * SIZE, C1
	add	C2, 4 * SIZE, C2
	add	C3, 4 * SIZE, C3
	add	C4, 4 * SIZE, C4

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -4, TEMP1
#else
	add	TEMP1, -4, TEMP1
#endif
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1

	add	AO, TEMP1, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 4, KK
#endif

	add	I, -1, I
	cmp	I, 0

#endif

	sra	K, 2, L
	bg,pt	%icc, .LL21
	FMOV	FZERO, c01

.LL50:
	and	M, 2, I
	FMOV	FZERO, c02
	cmp	I, 0

	FMOV	FZERO, t1
	ble,pn	%icc, .LL70
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
	sll	KK, 1 + BASE_SHIFT, TEMP1
	sll	KK, 2 + BASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 4, L
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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 2, L
#else
	add	KK, 4, L
#endif
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c02, t1, c02
	FMUL	c01, ALPHA, c01
	LDF	[C1 + 0 * SIZE], a1
	FADD	c04, t2, c04
	FMUL	c03, ALPHA, c03
	LDF	[C1 + 1 * SIZE], a2
	FADD	c06, t3, c06
	FMUL	c05, ALPHA, c05
	LDF	[C2 + 0 * SIZE], a3
	FADD	c08, t4, c08
	FMUL	c07, ALPHA, c07
	LDF	[C2 + 1 * SIZE], a4

	FMUL	c02, ALPHA, c02
	FADD	c01, a1, c01
	LDF	[C3 + 0 * SIZE], b1

	FMUL	c04, ALPHA, c04
	FADD	c02, a2, c02
	LDF	[C3 + 1 * SIZE], b2

	FMUL	c06, ALPHA, c06
	FADD	c03, a3, c03
	LDF	[C4 + 0 * SIZE], b3

	FMUL	c08, ALPHA, c08
	FADD	c04, a4, c04
	LDF	[C4 + 1 * SIZE], b4

	STF	c01, [C1 + 0 * SIZE]
	FADD	c05, b1, c05
	STF	c02, [C1 + 1 * SIZE]
	FADD	c06, b2, c06
	add	C1, 2 * SIZE, C1

	STF	c03, [C2 + 0 * SIZE]
	FADD	c07, b3, c07
	STF	c04, [C2 + 1 * SIZE]
	FADD	c08, b4, c08
	add	C2, 2 * SIZE, C2

	STF	c05, [C3 + 0 * SIZE]
	STF	c06, [C3 + 1 * SIZE]
	add	C3, 2 * SIZE, C3

	STF	c07, [C4 + 0 * SIZE]
	STF	c08, [C4 + 1 * SIZE]
	add	C4, 2 * SIZE, C4
#else

	FADD	c02, t1, c02
	FADD	c04, t2, c04
	FADD	c06, t3, c06
	FADD	c08, t4, c08

	FMUL	c01, ALPHA, c01
	FMUL	c03, ALPHA, c03
	FMUL	c05, ALPHA, c05
	FMUL	c07, ALPHA, c07

	FMUL	c02, ALPHA, c02
	FMUL	c04, ALPHA, c04
	FMUL	c06, ALPHA, c06
	FMUL	c08, ALPHA, c08

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]

	STF	c03, [C2 + 0 * SIZE]
	STF	c04, [C2 + 1 * SIZE]

	STF	c05, [C3 + 0 * SIZE]
	STF	c06, [C3 + 1 * SIZE]

	STF	c07, [C4 + 0 * SIZE]
	STF	c08, [C4 + 1 * SIZE]

	add	C1, 2 * SIZE, C1
	add	C2, 2 * SIZE, C2
	add	C3, 2 * SIZE, C3
	add	C4, 2 * SIZE, C4

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -4, TEMP1
#endif
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

.LL70:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL99
	nop

.LL71:
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
	sll	KK, 0 + BASE_SHIFT, TEMP1
	sll	KK, 2 + BASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 4, L
#endif
	sra	L, 2, L
	cmp	L,  0

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO  + 0 * SIZE], b1
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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 1, L
#else
	add	KK, 4, L
#endif
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c01, t1, c01
	LDF	[C1 + 0 * SIZE], a1
	FADD	c02, t2, c02
	LDF	[C2 + 0 * SIZE], a2
	FADD	c03, t3, c03
	LDF	[C3 + 0 * SIZE], a3
	FADD	c04, t4, c04
	LDF	[C4 + 0 * SIZE], a4

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	FADD	c01, a1, c01
	FADD	c02, a2, c02
	FADD	c03, a3, c03
	FADD	c04, a4, c04

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
	STF	c03, [C3 + 0 * SIZE]
	STF	c04, [C4 + 0 * SIZE]
#else
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
	STF	c03, [C3 + 0 * SIZE]
	STF	c04, [C4 + 0 * SIZE]

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -4, TEMP1
#endif
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 2 + BASE_SHIFT, TEMP1

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
	add	KK, 4, KK
#else
	nop
#endif

.LL100:  /* n & 2 */
	sra	M, 2, I
	and	N, 2, J

	cmp	J, 0
	add	C, LDC, C2
	ble,pn	%icc, .LL200
	mov	A, AO

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	mov	C, C1
	add	C2, LDC, C

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

	prefetch [C1 + 3 * SIZE], 2
	FMOV	FZERO, c05
	prefetch [C2 + 3 * SIZE], 2
	FMOV	FZERO, c02
#else

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 2 + BASE_SHIFT, TEMP1
	sll	KK, 1 + BASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 4, L
#else
	add	KK, 2, L
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

	prefetch [C1 + 3 * SIZE], 2
	FMOV	FZERO, c05
	prefetch [C2 + 3 * SIZE], 2
	FMOV	FZERO, c02
#endif

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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 4, L
#else
	add	KK, 2, L
#endif
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c03, t1, c03
	add	I, -1, I
	LDF	[C1 + 0 * SIZE], a1
	FADD	c07, t2, c07
	cmp	I, 0
	LDF	[C1 + 1 * SIZE], a2
	FADD	c04, t3, c04
	LDF	[C1 + 2 * SIZE], a3
	FADD	c08, t4, c08
	LDF	[C1 + 3 * SIZE], a4

	LDF	[C2 + 0 * SIZE], b1
	FMUL	c01, ALPHA, c01
	LDF	[C2 + 1 * SIZE], b2
	FMUL	c02, ALPHA, c02
	LDF	[C2 + 2 * SIZE], b3
	FMUL	c03, ALPHA, c03
	LDF	[C2 + 3 * SIZE], b4
	FMUL	c04, ALPHA, c04

	FMUL	c05, ALPHA, c05
	FADD	c01, a1, c01
	FMUL	c06, ALPHA, c06
	FADD	c02, a2, c02
	FMUL	c07, ALPHA, c07
	FADD	c03, a3, c03
	FMUL	c08, ALPHA, c08
	FADD	c04, a4, c04

	STF	c01, [C1 + 0 * SIZE]
	FADD	c05, b1, c05
	STF	c02, [C1 + 1 * SIZE]
	FADD	c06, b2, c06
	STF	c03, [C1 + 2 * SIZE]
	FADD	c07, b3, c07
	STF	c04, [C1 + 3 * SIZE]
	add	C1, 4 * SIZE, C1
	FADD	c08, b4, c08

	STF	c05, [C2 + 0 * SIZE]
	STF	c06, [C2 + 1 * SIZE]
	STF	c07, [C2 + 2 * SIZE]
	STF	c08, [C2 + 3 * SIZE]
	add	C2, 4 * SIZE, C2
#else
	FADD	c03, t1, c03
	FADD	c07, t2, c07
	FADD	c04, t3, c04
	FADD	c08, t4, c08

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	FMUL	c05, ALPHA, c05
	FMUL	c06, ALPHA, c06
	FMUL	c07, ALPHA, c07
	FMUL	c08, ALPHA, c08

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]

	STF	c05, [C2 + 0 * SIZE]
	STF	c06, [C2 + 1 * SIZE]
	STF	c07, [C2 + 2 * SIZE]
	STF	c08, [C2 + 3 * SIZE]
	add	C1, 4 * SIZE, C1
	add	C2, 4 * SIZE, C2

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -4, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, 2 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 4, KK
#endif

	add	I, -1, I
	cmp	I, 0
#endif

	bg,pt	%icc, .LL121
	FMOV	FZERO, c03

.LL150:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL170
	nop

.LL151:
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
	sll	KK, 1 + BASE_SHIFT, TEMP1
	sll	KK, 1 + BASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
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
#endif

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
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	LDF	[C1 + 0 * SIZE], a1
	LDF	[C2 + 0 * SIZE], a2
	LDF	[C1 + 1 * SIZE], a3
	LDF	[C2 + 1 * SIZE], a4

	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	FADD	c01, a1, c01
	FADD	c02, a2, c02
	FADD	c03, a3, c03
	FADD	c04, a4, c04

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
	STF	c03, [C1 + 1 * SIZE]
	add	C1, 2 * SIZE, C1
	STF	c04, [C2 + 1 * SIZE]
	add	C2, 2 * SIZE, C2
#else
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
	STF	c03, [C1 + 1 * SIZE]
	STF	c04, [C2 + 1 * SIZE]
	add	C1, 2 * SIZE, C1
	add	C2, 2 * SIZE, C2

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

.LL170:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL199
	nop

.LL171:
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
	sll	KK, 0 + BASE_SHIFT, TEMP1
	sll	KK, 1 + BASE_SHIFT, TEMP2

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
	FMOV	FZERO, c01
	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t1

 	LDF	[AO + 1 * SIZE], a2
	FMOV	FZERO, c02
	LDF	[BO + 1 * SIZE], b2
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
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c01, t1, c01
	LDF	[C1 + 0 * SIZE], a1
	FADD	c02, t2, c02
	LDF	[C2 + 0 * SIZE], a2
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FADD	c01, c03, c01
	FADD	c02, c04, c02

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02

	FADD	c01, a1, c01
	FADD	c02, a2, c02

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]
#else

	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FADD	c01, c03, c01
	FADD	c02, c04, c02

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C2 + 0 * SIZE]

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -2, TEMP1
#endif
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 1 + BASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 1, KK
#endif
#endif

.LL199:
	mov	BO, B
#if defined(TRMMKERNEL) && !defined(LEFT)
	add	KK, 2, KK
#else
	nop
#endif

.LL200:
	and	N, 1, J
	sra	M, 2, I

	cmp	J, 0
	ble,pn	%icc, .LL999
	mov	A, AO

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mov	OFFSET, KK
#endif

	cmp	I, 0
	ble,pn	%icc, .LL250
	mov	C, C1

.LL221:
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
	sll	KK, 2 + BASE_SHIFT, TEMP1
	sll	KK, 0 + BASE_SHIFT, TEMP2

	add	AO, TEMP1, AO
	add	B,  TEMP2, BO
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 4, L
#else
	add	KK, 1, L
#endif
	sra	L, 2, L
	cmp	L,  0

	LDF	[AO + 0 * SIZE], a1
	FMOV	FZERO, c01
	LDF	[BO  + 0 * SIZE], b1
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
#endif

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
#ifndef TRMMKERNEL
	and	K, 3, L
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	K, KK, L
#elif defined(LEFT)
	add	KK, 4, L
#else
	add	KK, 1, L
#endif
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c01, t1, c01
	add	I, -1, I
	FADD	c02, t2, c02
	cmp	I, 0
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	LDF	[C1 + 0 * SIZE], a1
	LDF	[C1 + 1 * SIZE], a2
	LDF	[C1 + 2 * SIZE], a3
	LDF	[C1 + 3 * SIZE], a4

	FADD	c01, a1, c01
	FADD	c02, a2, c02
	FADD	c03, a3, c03
	FADD	c04, a4, c04

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]
	add	C1, 4 * SIZE, C1
#else
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FMUL	c03, ALPHA, c03
	FMUL	c04, ALPHA, c04

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	STF	c03, [C1 + 2 * SIZE]
	STF	c04, [C1 + 3 * SIZE]
	add	C1, 4 * SIZE, C1

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -4, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif
	sll	TEMP1, 2 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 4, KK
#endif

	add	I, -1, I
	cmp	I, 0
#endif

	bg,pt	%icc, .LL221
	nop

.LL250:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL270
	nop

.LL251:
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
	sll	KK, 1 + BASE_SHIFT, TEMP1
	sll	KK, 0 + BASE_SHIFT, TEMP2

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
#endif

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
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c01, t1, c01
	LDF	[C1 + 0 * SIZE], a1
	FADD	c02, t2, c02
	LDF	[C1 + 1 * SIZE], a2
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FADD	c01, c03, c01
	FADD	c02, c04, c02
	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02
	FADD	c01, a1, c01
	FADD	c02, a2, c02

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	add	C1, 2 * SIZE, C1
#else
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c03, t3, c03
	FADD	c04, t4, c04

	FADD	c01, c03, c01
	FADD	c02, c04, c02
	FMUL	c01, ALPHA, c01
	FMUL	c02, ALPHA, c02

	STF	c01, [C1 + 0 * SIZE]
	STF	c02, [C1 + 1 * SIZE]
	add	C1, 2 * SIZE, C1

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -2, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif
	sll	TEMP1, 1 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1

	add	AO, TEMP2, AO
	add	BO, TEMP1, BO
#endif

#ifdef LEFT
	add	KK, 2, KK
#endif
#endif

.LL270:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL999
	nop

.LL271:
#if !defined(TRMMKERNEL)
	LDF	[AO + 0 * SIZE], a1
	sra	K, 2, L
	FMOV	FZERO, t1

 	LDF	[AO + 1 * SIZE], a2
	mov	B, BO
	FMOV	FZERO, c01

	LDF	[AO + 2 * SIZE], a3
	cmp	L,  0
	FMOV	FZERO, t2

	LDF	[AO + 3 * SIZE], a4
	FMOV	FZERO, c02

	LDF	[BO + 0 * SIZE], b1
	FMOV	FZERO, t3
	LDF	[BO + 1 * SIZE], b2
	FMOV	FZERO, t4
	LDF	[BO + 2 * SIZE], b3
#else
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	B, BO
#else
	sll	KK, 0 + BASE_SHIFT, TEMP1
	sll	KK, 0 + BASE_SHIFT, TEMP2

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
#endif

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
	and	L, 3,  L
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
#ifndef TRMMKERNEL
	FADD	c01, t1, c01

	LDF	[C1 + 0 * SIZE], a1
	FADD	c02, t2, c02
	FADD	c01, t3, c01
	FADD	c02, t4, c02
	FADD	c01, c02, c01

	FMUL	c01, ALPHA, c01
	FADD	c01, a1, c01
	STF	c01, [C1 + 0 * SIZE]
#else
	FADD	c01, t1, c01
	FADD	c02, t2, c02
	FADD	c01, t3, c01
	FADD	c02, t4, c02
	FADD	c01, c02, c01

	FMUL	c01, ALPHA, c01
	STF	c01, [C1 + 0 * SIZE]

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	K, KK, TEMP1
#ifdef LEFT
	add	TEMP1, -1, TEMP1
#else
	add	TEMP1, -1, TEMP1
#endif
	sll	TEMP1, 0 + BASE_SHIFT, TEMP2
	sll	TEMP1, 0 + BASE_SHIFT, TEMP1

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
