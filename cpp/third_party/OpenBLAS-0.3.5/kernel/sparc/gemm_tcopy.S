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
#define A	%i2
#define LDA	%i3
#define B	%i4

#define A1	%l0
#define A2	%l1
#define A3	%l2
#define A4	%l3

#define I	%l4
#define J	%l5

#define B1	%o0
#define B2	%o1
#define B3	%o3
#define M4	%o4

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

	sll	M, BASE_SHIFT + 2, M4

	and	N, -4, B2
	and	N, -2, B3
	sll	M, BASE_SHIFT, B1
	smul	B1, B2, B2
	smul	B1, B3, B3
	add	B, B2, B2
	add	B, B3, B3

	sra	M, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL100
	sll	LDA, BASE_SHIFT, LDA

.LL11:
	add	A,  LDA, A2
	mov	A,  A1
	add	A2, LDA, A3
	sra	N, 2, I
	add	A3, LDA, A4
	cmp	I, 0

	mov	B, B1
	add	B, 16 * SIZE, B

	ble,pn	%icc, .LL15
	add	A4, LDA, A

#define PREFETCHSIZE 8

.LL12:
	prefetch [A1 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A1 +  2 * SIZE], c03
	LDF	[A1 +  3 * SIZE], c04

	prefetch [A2 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A2 +  0 * SIZE], c05
	LDF	[A2 +  1 * SIZE], c06
	LDF	[A2 +  2 * SIZE], c07
	LDF	[A2 +  3 * SIZE], c08

	prefetch [A3 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A3 +  0 * SIZE], c09
	LDF	[A3 +  1 * SIZE], c10
	LDF	[A3 +  2 * SIZE], c11
	LDF	[A3 +  3 * SIZE], c12

	prefetch [A4 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A4 +  0 * SIZE], c13
	LDF	[A4 +  1 * SIZE], c14
	LDF	[A4 +  2 * SIZE], c15
	LDF	[A4 +  3 * SIZE], c16

	prefetch [B1 + (PREFETCHSIZE + 0) * SIZE], 0
	STF	c01, [B1 +  0 * SIZE]
	add	A1,  4 * SIZE, A1
	STF	c02, [B1 +  1 * SIZE]
	add	A2,  4 * SIZE, A2
	STF	c03, [B1 +  2 * SIZE]
	add	A3,  4 * SIZE, A3
	STF	c04, [B1 +  3 * SIZE]
	add	A4,  4 * SIZE, A4
	STF	c05, [B1 +  4 * SIZE]
	add	I, -1, I
	STF	c06, [B1 +  5 * SIZE]
	cmp	I, 0
	STF	c07, [B1 +  6 * SIZE]
	STF	c08, [B1 +  7 * SIZE]

#ifdef DOUBLE
	prefetch [B1 + (PREFETCHSIZE + 8) * SIZE], 0
#endif
	STF	c09, [B1 +  8 * SIZE]
	STF	c10, [B1 +  9 * SIZE]
	STF	c11, [B1 + 10 * SIZE]
	STF	c12, [B1 + 11 * SIZE]
	STF	c13, [B1 + 12 * SIZE]
	STF	c14, [B1 + 13 * SIZE]
	STF	c15, [B1 + 14 * SIZE]
	STF	c16, [B1 + 15 * SIZE]
	bg,pt	%icc, .LL12
	add	B1, M4, B1

.LL15:
	and	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL17
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A2 +  0 * SIZE], c03
	LDF	[A2 +  1 * SIZE], c04

	LDF	[A3 +  0 * SIZE], c05
	LDF	[A3 +  1 * SIZE], c06
	LDF	[A4 +  0 * SIZE], c07
	LDF	[A4 +  1 * SIZE], c08

	STF	c01, [B2 +  0 * SIZE]
	add	A1,  2 * SIZE, A1
	STF	c02, [B2 +  1 * SIZE]
	add	A2,  2 * SIZE, A2
	STF	c03, [B2 +  2 * SIZE]
	add	A3,  2 * SIZE, A3
	STF	c04, [B2 +  3 * SIZE]
	add	A4,  2 * SIZE, A4
	STF	c05, [B2 +  4 * SIZE]
	STF	c06, [B2 +  5 * SIZE]
	STF	c07, [B2 +  6 * SIZE]
	STF	c08, [B2 +  7 * SIZE]
	add	B2, 8 * SIZE, B2

.LL17:
	and	N, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL99
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02
	LDF	[A3 +  0 * SIZE], c03
	LDF	[A4 +  0 * SIZE], c04

	STF	c01, [B3 +  0 * SIZE]
	STF	c02, [B3 +  1 * SIZE]
	STF	c03, [B3 +  2 * SIZE]
	STF	c04, [B3 +  3 * SIZE]
	add	B3, 4 * SIZE, B3

.LL99:
	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	nop

.LL100:
	and	M, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL200
	nop

.LL111:
	sra	N, 2, I
	add	A,  LDA, A2
	cmp	I, 0
	mov	A,  A1

	mov	B, B1
	add	B, 8 * SIZE, B

	ble,pn	%icc, .LL115
	add	A2, LDA, A

.LL112:
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A1 +  2 * SIZE], c03
	LDF	[A1 +  3 * SIZE], c04

	LDF	[A2 +  0 * SIZE], c05
	LDF	[A2 +  1 * SIZE], c06
	LDF	[A2 +  2 * SIZE], c07
	LDF	[A2 +  3 * SIZE], c08

	STF	c01, [B1 +  0 * SIZE]
	add	A1,  4 * SIZE, A1
	STF	c02, [B1 +  1 * SIZE]
	add	A2,  4 * SIZE, A2
	STF	c03, [B1 +  2 * SIZE]
	add	I, -1, I
	STF	c04, [B1 +  3 * SIZE]
	cmp	I, 0
	STF	c05, [B1 +  4 * SIZE]
	STF	c06, [B1 +  5 * SIZE]
	STF	c07, [B1 +  6 * SIZE]
	STF	c08, [B1 +  7 * SIZE]

	bg,pt	%icc, .LL112
	add	B1,  M4, B1

.LL115:
	and	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL117
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A2 +  0 * SIZE], c03
	LDF	[A2 +  1 * SIZE], c04

	STF	c01, [B2 +  0 * SIZE]
	add	A1,  2 * SIZE, A1
	STF	c02, [B2 +  1 * SIZE]
	add	A2,  2 * SIZE, A2
	STF	c03, [B2 +  2 * SIZE]
	add	I, -1, I
	STF	c04, [B2 +  3 * SIZE]
	cmp	I, 0
	add	B2,  4 * SIZE, B2

.LL117:
	and	N, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL200
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c02

	STF	c01, [B3 +  0 * SIZE]
	STF	c02, [B3 +  1 * SIZE]
	add	B3,  2 * SIZE, B3

.LL200:
	and	M, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL999
	nop

.LL211:
	sra	N, 2, I
	cmp	I, 0

	mov	B, B1

	ble,pn	%icc, .LL215
	mov	A,  A1

.LL212:
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A1 +  2 * SIZE], c03
	LDF	[A1 +  3 * SIZE], c04

	STF	c01, [B +  0 * SIZE]
	add	I, -1, I
	STF	c02, [B +  1 * SIZE]
	cmp	I, 0
	STF	c03, [B +  2 * SIZE]
	add	A1,  4 * SIZE, A1
	STF	c04, [B +  3 * SIZE]

	bg,pt	%icc, .LL212
	add	B,   M4, B

.LL215:
	and	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL217
	nop

	LDF	[A1 +  0 * SIZE], c01
	LDF	[A1 +  1 * SIZE], c02

	STF	c01, [B2 +  0 * SIZE]
	STF	c02, [B2 +  1 * SIZE]
	add	A1,  2 * SIZE, A1

.LL217:
	and	N, 1, I
	cmp	I, 0
	ble,pn	%icc, .LL999
	nop

	LDF	[A1 +  0 * SIZE], c01
	STF	c01, [B3 +  0 * SIZE]

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
