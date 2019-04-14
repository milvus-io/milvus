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

	sra	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL100
	sll	LDA, BASE_SHIFT, LDA

.LL11:
	add	A,  LDA, A2
	mov	A,  A1
	add	A2, LDA, A3
	sra	M, 2, I
	add	A3, LDA, A4
	cmp	I, 0

	ble,pn	%icc, .LL15
	add	A4, LDA, A

#define  PREFETCHSIZE 36
#define WPREFETCHSIZE 20

.LL12:
	prefetch [A1 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c05
	LDF	[A3 +  0 * SIZE], c09
	LDF	[A4 +  0 * SIZE], c13

	prefetch [A2 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A2 +  1 * SIZE], c06
	LDF	[A3 +  1 * SIZE], c10
	LDF	[A4 +  1 * SIZE], c14

	prefetch [A3 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  2 * SIZE], c03
	LDF	[A2 +  2 * SIZE], c07
	LDF	[A3 +  2 * SIZE], c11
	LDF	[A4 +  2 * SIZE], c15

	prefetch [A4 + (PREFETCHSIZE + 0) * SIZE], 0
	LDF	[A1 +  3 * SIZE], c04
	LDF	[A2 +  3 * SIZE], c08
	LDF	[A3 +  3 * SIZE], c12
	LDF	[A4 +  3 * SIZE], c16

	prefetch [B + (WPREFETCHSIZE + 0) * SIZE], 2
	STF	c01, [B +  0 * SIZE]
	add	A1,  4 * SIZE, A1
	STF	c05, [B +  1 * SIZE]
	add	A2,  4 * SIZE, A2
	STF	c09, [B +  2 * SIZE]
	add	A3,  4 * SIZE, A3
	STF	c13, [B +  3 * SIZE]
	add	A4,  4 * SIZE, A4
	STF	c02, [B +  4 * SIZE]
	add	I, -1, I
	STF	c06, [B +  5 * SIZE]
	cmp	I, 0
	STF	c10, [B +  6 * SIZE]
	STF	c14, [B +  7 * SIZE]
#ifdef DOUBLE
	prefetch [B + (WPREFETCHSIZE + 8) * SIZE], 2
#endif
	STF	c03, [B +  8 * SIZE]
	STF	c07, [B +  9 * SIZE]
	STF	c11, [B + 10 * SIZE]
	STF	c15, [B + 11 * SIZE]
	STF	c04, [B + 12 * SIZE]
	STF	c08, [B + 13 * SIZE]
	STF	c12, [B + 14 * SIZE]
	STF	c16, [B + 15 * SIZE]
	bg,pt	%icc, .LL12
	add	B, 16 * SIZE, B

.LL15:
	and	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL99
	nop

.LL16:
	LDF	[A1 +  0 * SIZE], c01
	add	A1,  1 * SIZE, A1
	LDF	[A2 +  0 * SIZE], c05
	add	A2,  1 * SIZE, A2
	LDF	[A3 +  0 * SIZE], c09
	add	A3,  1 * SIZE, A3
	LDF	[A4 +  0 * SIZE], c13
	add	A4,  1 * SIZE, A4

	STF	c01, [B +  0 * SIZE]
	add	I, -1, I
	STF	c05, [B +  1 * SIZE]
	cmp	I, 0
	STF	c09, [B +  2 * SIZE]
	STF	c13, [B +  3 * SIZE]
	bg,pt	%icc, .LL16
	add	B,   4 * SIZE, B

.LL99:
	add	J, -1, J
	cmp	J, 0
	bg,pt	%icc, .LL11
	nop

.LL100:
	and	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL200
	nop

.LL111:
	sra	M, 2, I
	add	A,  LDA, A2
	cmp	I, 0
	mov	A,  A1

	ble,pn	%icc, .LL115
	add	A2, LDA, A

.LL112:
	LDF	[A1 +  0 * SIZE], c01
	LDF	[A2 +  0 * SIZE], c05
	LDF	[A1 +  1 * SIZE], c02
	LDF	[A2 +  1 * SIZE], c06

	LDF	[A1 +  2 * SIZE], c03
	LDF	[A2 +  2 * SIZE], c07
	LDF	[A1 +  3 * SIZE], c04
	LDF	[A2 +  3 * SIZE], c08

	STF	c01, [B +  0 * SIZE]
	add	A1,  4 * SIZE, A1
	STF	c05, [B +  1 * SIZE]
	add	A2,  4 * SIZE, A2
	STF	c02, [B +  2 * SIZE]
	add	I, -1, I
	STF	c06, [B +  3 * SIZE]
	cmp	I, 0
	STF	c03, [B +  4 * SIZE]
	STF	c07, [B +  5 * SIZE]
	STF	c04, [B +  6 * SIZE]
	STF	c08, [B +  7 * SIZE]

	bg,pt	%icc, .LL112
	add	B,   8 * SIZE, B

.LL115:
	and	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL200
	nop

.LL116:
	LDF	[A1 +  0 * SIZE], c01
	add	A1,  1 * SIZE, A1
	add	I, -1, I
	LDF	[A2 +  0 * SIZE], c05
	add	A2,  1 * SIZE, A2
	cmp	I, 0

	STF	c01, [B +  0 * SIZE]
	STF	c05, [B +  1 * SIZE]
	bg,pt	%icc, .LL116
	add	B,   2 * SIZE, B

.LL200:
	and	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL999
	nop

.LL211:
	sra	M, 2, I
	cmp	I, 0
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
	add	B,   4 * SIZE, B

.LL215:
	and	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL999
	nop

.LL216:
	LDF	[A1 +  0 * SIZE], c01
	add	A1,  1 * SIZE, A1
	add	I, -1, I
	cmp	I, 0

	STF	c01, [B +  0 * SIZE]
	bg,pt	%icc, .LL216
	add	B,   1 * SIZE, B

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
