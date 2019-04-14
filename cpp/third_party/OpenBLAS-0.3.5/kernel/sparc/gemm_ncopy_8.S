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

#define  PREFETCHSIZE 42
#define WPREFETCHSIZE 20

#define M	%i0
#define N	%i1
#define A	%i2
#define LDA	%i3
#define B	%i4

#define A1	%l0
#define A2	%l1
#define A3	%l2
#define A4	%l3
#define A5	%o0
#define A6	%o1
#define A7	%o2
#define A8	%o3

#define I	%l4
#define J	%l5

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
#endif

	PROLOGUE
	SAVESP

	sra	N, 3, J
	cmp	J, 0
	ble,pn	%icc, .LL20
	sll	LDA, BASE_SHIFT, LDA

.LL11:
	add	A,  LDA, A2
	mov	A,  A1
	add	A2, LDA, A3
	sra	M, 3, I
	add	A3, LDA, A4
	cmp	I, 0

	add	A4, LDA, A5
	add	A5, LDA, A6
	add	A6, LDA, A7
	add	A7, LDA, A8

	ble,pn	%icc, .LL13
	add	A8, LDA, A
	.align 4

.LL12:
	prefetch [A1 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A5 +  0 * SIZE], c05
	LDF	[A6 +  0 * SIZE], c06
	LDF	[A7 +  0 * SIZE], c07
	LDF	[A8 +  0 * SIZE], c08

	prefetch [A2 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  1 * SIZE], c09
	LDF	[A2 +  1 * SIZE], c10
	LDF	[A3 +  1 * SIZE], c11
	LDF	[A4 +  1 * SIZE], c12
	LDF	[A5 +  1 * SIZE], c13
	LDF	[A6 +  1 * SIZE], c14
	LDF	[A7 +  1 * SIZE], c15
	LDF	[A8 +  1 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE +  0) * SIZE], 2
	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	prefetch [B + (WPREFETCHSIZE +  8) * SIZE], 2
	STF	c09, [B +  8 * SIZE]
	STF	c10, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c12, [B + 11 * SIZE]
	STF	c13, [B + 12 * SIZE]
	STF	c14, [B + 13 * SIZE]
	STF	c15, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]

	prefetch [A3 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  2 * SIZE], c01
	LDF	[A2 +  2 * SIZE], c02
	LDF	[A3 +  2 * SIZE], c03
	LDF	[A4 +  2 * SIZE], c04
	LDF	[A5 +  2 * SIZE], c05
	LDF	[A6 +  2 * SIZE], c06
	LDF	[A7 +  2 * SIZE], c07
	LDF	[A8 +  2 * SIZE], c08

	prefetch [A4 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  3 * SIZE], c09
	LDF	[A2 +  3 * SIZE], c10
	LDF	[A3 +  3 * SIZE], c11
	LDF	[A4 +  3 * SIZE], c12
	LDF	[A5 +  3 * SIZE], c13
	LDF	[A6 +  3 * SIZE], c14
	LDF	[A7 +  3 * SIZE], c15
	LDF	[A8 +  3 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE + 16) * SIZE], 2
	STF	c01, [B + 16 * SIZE]
	STF	c02, [B + 17 * SIZE]
	STF	c03, [B + 18 * SIZE]
	STF	c04, [B + 19 * SIZE]
	STF	c05, [B + 20 * SIZE]
	STF	c06, [B + 21 * SIZE]
	STF	c07, [B + 22 * SIZE]
	STF	c08, [B + 23 * SIZE]

	prefetch [B + (WPREFETCHSIZE + 24) * SIZE], 2
	STF	c09, [B + 24 * SIZE]
	STF	c10, [B + 25 * SIZE]
	STF	c11, [B + 26 * SIZE]
	STF	c12, [B + 27 * SIZE]
	STF	c13, [B + 28 * SIZE]
	STF	c14, [B + 29 * SIZE]
	STF	c15, [B + 30 * SIZE]
	STF	c16, [B + 31 * SIZE]

	prefetch [A5 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  4 * SIZE], c01
	LDF	[A2 +  4 * SIZE], c02
	LDF	[A3 +  4 * SIZE], c03
	LDF	[A4 +  4 * SIZE], c04
	LDF	[A5 +  4 * SIZE], c05
	LDF	[A6 +  4 * SIZE], c06
	LDF	[A7 +  4 * SIZE], c07
	LDF	[A8 +  4 * SIZE], c08

	prefetch [A6 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  5 * SIZE], c09
	LDF	[A2 +  5 * SIZE], c10
	LDF	[A3 +  5 * SIZE], c11
	LDF	[A4 +  5 * SIZE], c12
	LDF	[A5 +  5 * SIZE], c13
	LDF	[A6 +  5 * SIZE], c14
	LDF	[A7 +  5 * SIZE], c15
	LDF	[A8 +  5 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE + 32) * SIZE], 2
	STF	c01, [B + 32 * SIZE]
	STF	c02, [B + 33 * SIZE]
	STF	c03, [B + 34 * SIZE]
	STF	c04, [B + 35 * SIZE]
	STF	c05, [B + 36 * SIZE]
	STF	c06, [B + 37 * SIZE]
	STF	c07, [B + 38 * SIZE]
	STF	c08, [B + 39 * SIZE]

	prefetch [B + (WPREFETCHSIZE + 40) * SIZE], 2
	STF	c09, [B + 40 * SIZE]
	STF	c10, [B + 41 * SIZE]
	STF	c11, [B + 42 * SIZE]
	STF	c12, [B + 43 * SIZE]
	STF	c13, [B + 44 * SIZE]
	STF	c14, [B + 45 * SIZE]
	STF	c15, [B + 46 * SIZE]
	STF	c16, [B + 47 * SIZE]

	prefetch [A7 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  6 * SIZE], c01
	LDF	[A2 +  6 * SIZE], c02
	LDF	[A3 +  6 * SIZE], c03
	LDF	[A4 +  6 * SIZE], c04
	LDF	[A5 +  6 * SIZE], c05
	LDF	[A6 +  6 * SIZE], c06
	LDF	[A7 +  6 * SIZE], c07
	LDF	[A8 +  6 * SIZE], c08

	prefetch [A8 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  7 * SIZE], c09
	LDF	[A2 +  7 * SIZE], c10
	LDF	[A3 +  7 * SIZE], c11
	LDF	[A4 +  7 * SIZE], c12
	LDF	[A5 +  7 * SIZE], c13
	LDF	[A6 +  7 * SIZE], c14
	LDF	[A7 +  7 * SIZE], c15
	LDF	[A8 +  7 * SIZE], c16

	add	A1,  8 * SIZE, A1
	add	A2,  8 * SIZE, A2
	add	A3,  8 * SIZE, A3
	add	A4,  8 * SIZE, A4

	prefetch [B + (WPREFETCHSIZE + 48) * SIZE], 2
	STF	c01, [B + 48 * SIZE]
	STF	c02, [B + 49 * SIZE]
	STF	c03, [B + 50 * SIZE]
	STF	c04, [B + 51 * SIZE]
	STF	c05, [B + 52 * SIZE]
	STF	c06, [B + 53 * SIZE]
	STF	c07, [B + 54 * SIZE]
	STF	c08, [B + 55 * SIZE]

	add	A5,  8 * SIZE, A5
	add	A6,  8 * SIZE, A6
	add	A7,  8 * SIZE, A7
	add	A8,  8 * SIZE, A8

	prefetch [B + (WPREFETCHSIZE + 56) * SIZE], 2
	STF	c09, [B + 56 * SIZE]
	STF	c10, [B + 57 * SIZE]
	STF	c11, [B + 58 * SIZE]
	STF	c12, [B + 59 * SIZE]
	STF	c13, [B + 60 * SIZE]
	STF	c14, [B + 61 * SIZE]
	STF	c15, [B + 62 * SIZE]
	STF	c16, [B + 63 * SIZE]

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL12
	add	B, 64 * SIZE, B
	.align 4

.LL13:
	and	M, 4, I
	cmp	I, 0
	ble,pn	%icc, .LL14
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A5 +  0 * SIZE], c05
	LDF	[A6 +  0 * SIZE], c06
	LDF	[A7 +  0 * SIZE], c07
	LDF	[A8 +  0 * SIZE], c08

	LDF	[A1 +  1 * SIZE], c09
	LDF	[A2 +  1 * SIZE], c10
	LDF	[A3 +  1 * SIZE], c11
	LDF	[A4 +  1 * SIZE], c12
	LDF	[A5 +  1 * SIZE], c13
	LDF	[A6 +  1 * SIZE], c14
	LDF	[A7 +  1 * SIZE], c15
	LDF	[A8 +  1 * SIZE], c16

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	STF	c09, [B +  8 * SIZE]
	STF	c10, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c12, [B + 11 * SIZE]
	STF	c13, [B + 12 * SIZE]
	STF	c14, [B + 13 * SIZE]
	STF	c15, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]

	LDF	[A1 +  2 * SIZE], c01
	LDF	[A2 +  2 * SIZE], c02
	LDF	[A3 +  2 * SIZE], c03
	LDF	[A4 +  2 * SIZE], c04
	LDF	[A5 +  2 * SIZE], c05
	LDF	[A6 +  2 * SIZE], c06
	LDF	[A7 +  2 * SIZE], c07
	LDF	[A8 +  2 * SIZE], c08

	LDF	[A1 +  3 * SIZE], c09
	LDF	[A2 +  3 * SIZE], c10
	LDF	[A3 +  3 * SIZE], c11
	LDF	[A4 +  3 * SIZE], c12
	LDF	[A5 +  3 * SIZE], c13
	LDF	[A6 +  3 * SIZE], c14
	LDF	[A7 +  3 * SIZE], c15
	LDF	[A8 +  3 * SIZE], c16

	STF	c01, [B + 16 * SIZE]
	STF	c02, [B + 17 * SIZE]
	STF	c03, [B + 18 * SIZE]
	STF	c04, [B + 19 * SIZE]
	STF	c05, [B + 20 * SIZE]
	STF	c06, [B + 21 * SIZE]
	STF	c07, [B + 22 * SIZE]
	STF	c08, [B + 23 * SIZE]

	STF	c09, [B + 24 * SIZE]
	STF	c10, [B + 25 * SIZE]
	STF	c11, [B + 26 * SIZE]
	STF	c12, [B + 27 * SIZE]
	STF	c13, [B + 28 * SIZE]
	STF	c14, [B + 29 * SIZE]
	STF	c15, [B + 30 * SIZE]
	STF	c16, [B + 31 * SIZE]

	add	A1,  4 * SIZE, A1
	add	A2,  4 * SIZE, A2
	add	A3,  4 * SIZE, A3
	add	A4,  4 * SIZE, A4

	add	A5,  4 * SIZE, A5
	add	A6,  4 * SIZE, A6
	add	A7,  4 * SIZE, A7
	add	A8,  4 * SIZE, A8

	add	B, 32 * SIZE, B
	.align 4

.LL14:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A5 +  0 * SIZE], c05
	LDF	[A6 +  0 * SIZE], c06
	LDF	[A7 +  0 * SIZE], c07
	LDF	[A8 +  0 * SIZE], c08

	LDF	[A1 +  1 * SIZE], c09
	LDF	[A2 +  1 * SIZE], c10
	LDF	[A3 +  1 * SIZE], c11
	LDF	[A4 +  1 * SIZE], c12
	LDF	[A5 +  1 * SIZE], c13
	LDF	[A6 +  1 * SIZE], c14
	LDF	[A7 +  1 * SIZE], c15
	LDF	[A8 +  1 * SIZE], c16

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	STF	c09, [B +  8 * SIZE]
	STF	c10, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c12, [B + 11 * SIZE]
	STF	c13, [B + 12 * SIZE]
	STF	c14, [B + 13 * SIZE]
	STF	c15, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]

	add	A1,  2 * SIZE, A1
	add	A2,  2 * SIZE, A2
	add	A3,  2 * SIZE, A3
	add	A4,  2 * SIZE, A4

	add	A5,  2 * SIZE, A5
	add	A6,  2 * SIZE, A6
	add	A7,  2 * SIZE, A7
	add	A8,  2 * SIZE, A8

	add	B, 16 * SIZE, B
	.align 4

.LL15:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL19
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A5 +  0 * SIZE], c05
	LDF	[A6 +  0 * SIZE], c06
	LDF	[A7 +  0 * SIZE], c07
	LDF	[A8 +  0 * SIZE], c08

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	add	B, 8 * SIZE, B
	.align 4

.LL19:
	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	nop
	.align 4

.LL20:
	and	N, 4, J
	cmp	J, 0
	ble,pn	%icc, .LL30
	nop

	add	A,  LDA, A2
	mov	A,  A1
	add	A2, LDA, A3
	sra	M, 3, I
	add	A3, LDA, A4
	cmp	I, 0

	ble,pn	%icc, .LL23
	add	A4, LDA, A
	.align 4

.LL22:
	prefetch [A1 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A1 +  1 * SIZE], c05
	LDF	[A2 +  1 * SIZE], c06
	LDF	[A3 +  1 * SIZE], c07
	LDF	[A4 +  1 * SIZE], c08

	prefetch [A2 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  2 * SIZE], c09
	LDF	[A2 +  2 * SIZE], c10
	LDF	[A3 +  2 * SIZE], c11
	LDF	[A4 +  2 * SIZE], c12
	LDF	[A1 +  3 * SIZE], c13
	LDF	[A2 +  3 * SIZE], c14
	LDF	[A3 +  3 * SIZE], c15
	LDF	[A4 +  3 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE +  0) * SIZE], 2
	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	prefetch [B + (WPREFETCHSIZE +  8) * SIZE], 2
	STF	c09, [B +  8 * SIZE]
	STF	c10, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c12, [B + 11 * SIZE]
	STF	c13, [B + 12 * SIZE]
	STF	c14, [B + 13 * SIZE]
	STF	c15, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]

	prefetch [A3 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  4 * SIZE], c01
	LDF	[A2 +  4 * SIZE], c02
	LDF	[A3 +  4 * SIZE], c03
	LDF	[A4 +  4 * SIZE], c04
	LDF	[A1 +  5 * SIZE], c05
	LDF	[A2 +  5 * SIZE], c06
	LDF	[A3 +  5 * SIZE], c07
	LDF	[A4 +  5 * SIZE], c08

	prefetch [A4 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  6 * SIZE], c09
	LDF	[A2 +  6 * SIZE], c10
	LDF	[A3 +  6 * SIZE], c11
	LDF	[A4 +  6 * SIZE], c12
	LDF	[A1 +  7 * SIZE], c13
	LDF	[A2 +  7 * SIZE], c14
	LDF	[A3 +  7 * SIZE], c15
	LDF	[A4 +  7 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE + 16) * SIZE], 2
	STF	c01, [B + 16 * SIZE]
	STF	c02, [B + 17 * SIZE]
	STF	c03, [B + 18 * SIZE]
	STF	c04, [B + 19 * SIZE]
	STF	c05, [B + 20 * SIZE]
	STF	c06, [B + 21 * SIZE]
	STF	c07, [B + 22 * SIZE]
	STF	c08, [B + 23 * SIZE]

	prefetch [B + (WPREFETCHSIZE + 24) * SIZE], 2
	STF	c09, [B + 24 * SIZE]
	STF	c10, [B + 25 * SIZE]
	STF	c11, [B + 26 * SIZE]
	STF	c12, [B + 27 * SIZE]
	STF	c13, [B + 28 * SIZE]
	STF	c14, [B + 29 * SIZE]
	STF	c15, [B + 30 * SIZE]
	STF	c16, [B + 31 * SIZE]

	add	A1,  8 * SIZE, A1
	add	A2,  8 * SIZE, A2
	add	A3,  8 * SIZE, A3
	add	A4,  8 * SIZE, A4

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL22
	add	B, 32 * SIZE, B
	.align 4

.LL23:
	and	M, 4, I
	cmp	I, 0
	ble,pn	%icc, .LL24
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A1 +  1 * SIZE], c05
	LDF	[A2 +  1 * SIZE], c06
	LDF	[A3 +  1 * SIZE], c07
	LDF	[A4 +  1 * SIZE], c08

	LDF	[A1 +  2 * SIZE], c09
	LDF	[A2 +  2 * SIZE], c10
	LDF	[A3 +  2 * SIZE], c11
	LDF	[A4 +  2 * SIZE], c12
	LDF	[A1 +  3 * SIZE], c13
	LDF	[A2 +  3 * SIZE], c14
	LDF	[A3 +  3 * SIZE], c15
	LDF	[A4 +  3 * SIZE], c16

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	STF	c09, [B +  8 * SIZE]
	STF	c10, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c12, [B + 11 * SIZE]
	STF	c13, [B + 12 * SIZE]
	STF	c14, [B + 13 * SIZE]
	STF	c15, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]

	add	A1,  4 * SIZE, A1
	add	A2,  4 * SIZE, A2
	add	A3,  4 * SIZE, A3
	add	A4,  4 * SIZE, A4

	add	B, 16 * SIZE, B
	.align 4

.LL24:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL25
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04
	LDF	[A1 +  1 * SIZE], c05
	LDF	[A2 +  1 * SIZE], c06
	LDF	[A3 +  1 * SIZE], c07
	LDF	[A4 +  1 * SIZE], c08

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	add	A1,  2 * SIZE, A1
	add	A2,  2 * SIZE, A2
	add	A3,  2 * SIZE, A3
	add	A4,  2 * SIZE, A4

	add	B,  8 * SIZE, B
	.align 4

.LL25:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL30
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]

	add	B, 4 * SIZE, B
	.align 4

.LL30:
	and	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL40
	nop

	add	A,  LDA, A2
	mov	A,  A1
	sra	M, 3, I
	cmp	I, 0

	ble,pn	%icc, .LL33
	add	A2, LDA, A
	.align 4

.LL32:
	prefetch [A1 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A1 +  1 * SIZE], c03
	LDF	[A2 +  1 * SIZE], c04
	LDF	[A1 +  2 * SIZE], c05
	LDF	[A2 +  2 * SIZE], c06
	LDF	[A1 +  3 * SIZE], c07
	LDF	[A2 +  3 * SIZE], c08

	prefetch [A2 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  4 * SIZE], c09
	LDF	[A2 +  4 * SIZE], c10
	LDF	[A1 +  5 * SIZE], c11
	LDF	[A2 +  5 * SIZE], c12
	LDF	[A1 +  6 * SIZE], c13
	LDF	[A2 +  6 * SIZE], c14
	LDF	[A1 +  7 * SIZE], c15
	LDF	[A2 +  7 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE +  0) * SIZE], 2
	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	prefetch [B + (WPREFETCHSIZE +  8) * SIZE], 2
	STF	c09, [B +  8 * SIZE]
	STF	c10, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c12, [B + 11 * SIZE]
	STF	c13, [B + 12 * SIZE]
	STF	c14, [B + 13 * SIZE]
	STF	c15, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]

	add	A1,  8 * SIZE, A1
	add	A2,  8 * SIZE, A2

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL32
	add	B, 16 * SIZE, B
	.align 4

.LL33:
	and	M, 4, I
	cmp	I, 0
	ble,pn	%icc, .LL34
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A1 +  1 * SIZE], c03
	LDF	[A2 +  1 * SIZE], c04
	LDF	[A1 +  2 * SIZE], c05
	LDF	[A2 +  2 * SIZE], c06
	LDF	[A1 +  3 * SIZE], c07
	LDF	[A2 +  3 * SIZE], c08

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	add	A1,  4 * SIZE, A1
	add	A2,  4 * SIZE, A2

	add	B, 8 * SIZE, B
	.align 4

.LL34:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL35
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A1 +  1 * SIZE], c03
	LDF	[A2 +  1 * SIZE], c04

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]

	add	A1,  2 * SIZE, A1
	add	A2,  2 * SIZE, A2

	add	B,  4 * SIZE, B
	.align 4

.LL35:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL40
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]

	add	B, 2 * SIZE, B
	.align 4

.LL40:
	and	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL999
	nop

	sra	M, 3, I
	cmp	I, 0

	ble,pn	%icc, .LL43
	mov	A,  A1
	.align 4

.LL42:
	prefetch [A1 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A1 +  2 * SIZE], c03
	LDF	[A1 +  3 * SIZE], c04
	LDF	[A1 +  4 * SIZE], c05
	LDF	[A1 +  5 * SIZE], c06
	LDF	[A1 +  6 * SIZE], c07
	LDF	[A1 +  7 * SIZE], c08

	prefetch [B + (WPREFETCHSIZE +  0) * SIZE], 2
	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]
	STF	c05, [B +  4 * SIZE]
	STF	c06, [B +  5 * SIZE]
	STF	c07, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	add	A1,  8 * SIZE, A1

	add	I, -1, I
	cmp	I, 0

	bg,pt	%icc, .LL42
	add	B, 8 * SIZE, B
	.align 4

.LL43:
	and	M, 4, I
	cmp	I, 0
	ble,pn	%icc, .LL44
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A1 +  2 * SIZE], c03
	LDF	[A1 +  3 * SIZE], c04

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]
	STF	c03, [B +  2 * SIZE]
	STF	c04, [B +  3 * SIZE]

	add	A1,  4 * SIZE, A1

	add	B, 4 * SIZE, B
	.align 4

.LL44:
	and	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL45
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02

	STF	c01, [B +  0 * SIZE]
	STF	c02, [B +  1 * SIZE]

	add	A1,  2 * SIZE, A1

	add	B,  2 * SIZE, B
	.align 4

.LL45:
	and	M, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL999
	nop

	LDF	[A1 +  0 * SIZE], c01
	STF	c01, [B +  0 * SIZE]
	.align 4

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
