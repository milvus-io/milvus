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

#ifdef linux
#ifndef __64BIT__
#define M	r3
#define N	r4
#define A	r5
#define LDA	r6
#define X	r7
#define	INCX	r8
#define	Y	r9
#define	INCY	r10
#define BUFFER	r14
#else
#define M	r3
#define N	r4
#define A	r6
#define LDA	r7
#define X	r8
#define	INCX	r9
#define	Y	r10
#define	INCY	r5
#define BUFFER	r14
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define M	r3
#define N	r4
#define A	r7
#define LDA	r8
#define X	r9
#define	INCX	r10
#define	Y	r5
#define	INCY	r6
#define BUFFER	r14
#else
#define M	r3
#define N	r4
#define A	r6
#define LDA	r7
#define X	r8
#define	INCX	r9
#define	Y	r10
#define	INCY	r5
#define BUFFER	r14
#endif
#endif

#define I	r11
#define	J	r12

#define AO1	r15
#define AO2	r16
#define AO3	r17
#define AO4	r18
#define XX	r19
#define YY	r20
#define	NEW_Y	r21
#define TEMP	r22
#define	PREA	r24
#define IS	r25

#define y01 f0
#define y02 f1
#define y03 f2
#define y04 f3

#define atemp1 f4
#define atemp2 f5
#define atemp3 f6
#define atemp4 f7

#define xtemp1 f8
#define xtemp2 f9
#define xtemp3 f10
#define xtemp4 f11

#define xsum1  f12
#define xsum2  f13
#define xsum3  f14
#define xsum4  f15

#define a1     f16
#define a2     f17
#define a3     f18
#define a4     f19
#define a5     f20
#define a6     f21
#define a7     f22
#define a8     f23
#define a9     f24
#define a10    f25
#define a11    f26
#define a12    f27
#define a13    f28
#define a14    f29
#define a15    f30
#define a16    f31

#define alpha  f1

#if defined(PPCG4)
#define PREFETCHSIZE_A  24
#endif

#if defined(PPC440) || defined(PPC440FP2)
#define PREFETCHSIZE_A  24
#endif

#ifdef PPC970
#define PREFETCHSIZE_A  64
#endif

#ifdef CELL
#define PREFETCHSIZE_A  72
#endif

#ifdef POWER4
#define PREFETCHSIZE_A  16
#endif

#ifdef POWER5
#define PREFETCHSIZE_A  96
#endif

#ifdef POWER6
#define PREFETCHSIZE_A  40
#endif

#if defined(POWER4) || defined(POWER5) || defined(POWER6) || defined(PPC970)
#define NOP1
#define NOP2
#else
#define NOP1   mr	LDA, LDA
#define NOP2   mr	INCX, INCX
#endif

#ifndef NEEDPARAM

#ifndef __64BIT__
#define STACKSIZE 224
#define ALPHA     200(SP)
#define	FZERO	  208(SP)
#else
#define STACKSIZE 280
#define ALPHA     256(SP)
#define FZERO	  264(SP)
#endif

	PROLOGUE
	PROFCODE

	addi	SP,   SP, -STACKSIZE
	li	r0,   0

	stfd	f14,     0(SP)
	stfd	f15,     8(SP)
	stfd	f16,    16(SP)
	stfd	f17,    24(SP)
	stfd	f18,    32(SP)
	stfd	f19,    40(SP)
	stfd	f20,    48(SP)
	stfd	f21,    56(SP)
	stfd	f22,    64(SP)
	stfd	f23,    72(SP)
	stfd	f24,    80(SP)
	stfd	f25,    88(SP)
	stfd	f26,    96(SP)
	stfd	f27,   104(SP)
	stfd	f28,   112(SP)
	stfd	f29,   120(SP)
	stfd	f30,   128(SP)
	stfd	f31,   136(SP)

#ifdef __64BIT__
	std	r0,    FZERO
	std	r14,   144(SP)
	std	r15,   152(SP)
	std	r16,   160(SP)
	std	r17,   168(SP)
	std	r18,   176(SP)
	std	r19,   184(SP)
	std	r20,   192(SP)
	std	r21,   200(SP)
	std	r22,   208(SP)
	std	r23,   216(SP)
	std	r24,   224(SP)
	std	r25,   232(SP)
	std	r26,   240(SP)
	std	r27,   248(SP)
#else
	stw	r0,    0 + FZERO
	stw	r0,    4 + FZERO
	stw	r14,   144(SP)
	stw	r15,   148(SP)
	stw	r16,   152(SP)
	stw	r17,   156(SP)
	stw	r18,   160(SP)
	stw	r19,   164(SP)
	stw	r20,   168(SP)
	stw	r21,   172(SP)
	stw	r22,   176(SP)
	stw	r23,   180(SP)
	stw	r24,   184(SP)
	stw	r25,   188(SP)
	stw	r26,   192(SP)
	stw	r27,   196(SP)
#endif

#ifdef linux
#ifndef __64BIT__
	lwz	BUFFER,  FRAMESLOT(0) + STACKSIZE(SP)
#else
	ld	INCY,    FRAMESLOT(0) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(1) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifndef __64BIT__
#ifdef DOUBLE
	lwz	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#else
	lwz	  INCY,  FRAMESLOT(0) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(1) + STACKSIZE(SP)
#endif
#else
	ld	INCY,    FRAMESLOT(0) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(1) + STACKSIZE(SP)
#endif
#endif

	STFD	alpha, ALPHA

	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE

	cmpwi	cr0, M, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, SIZE
	beq	LL(05)

	mr	XX, X
	mr	X, BUFFER

	srawi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(03)
	.align 4

LL(01):
	LFD	a1, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a2, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a3, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a4, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a5, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a6, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a7, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a8, 0 * SIZE(XX)
	add	XX, XX, INCX

	dcbt	XX, PREA
	dcbtst	BUFFER, PREA

	STFD	a1, 0 * SIZE(BUFFER)
	STFD	a2, 1 * SIZE(BUFFER)
	STFD	a3, 2 * SIZE(BUFFER)
	STFD	a4, 3 * SIZE(BUFFER)
	STFD	a5, 4 * SIZE(BUFFER)
	STFD	a6, 5 * SIZE(BUFFER)
	STFD	a7, 6 * SIZE(BUFFER)
	STFD	a8, 7 * SIZE(BUFFER)

	addi	BUFFER, BUFFER, 8 * SIZE
	bdnz	LL(01)
	.align 4

LL(03):
	andi.	r0, M, 7
	mtspr	CTR, r0
	ble	LL(05)
	.align 4

LL(04):
	LFD	a1, 0 * SIZE(XX)
	add	XX, XX, INCX

	STFD	a1, 0 * SIZE(BUFFER)
	addi	BUFFER, BUFFER, 1 * SIZE
	bdnz	LL(04)
	.align 4

LL(05):
	mr	NEW_Y, Y
	lfd	f0, FZERO

	cmpwi	cr0, INCY, SIZE
	beq	LL(10)

	mr	NEW_Y, BUFFER

	addi	r0, M,  7
	srawi.	r0, r0, 3
	mtspr	CTR, r0
	.align 4

LL(06):
	STFD	f0, 0 * SIZE(BUFFER)
	STFD	f0, 1 * SIZE(BUFFER)
	STFD	f0, 2 * SIZE(BUFFER)
	STFD	f0, 3 * SIZE(BUFFER)
	STFD	f0, 4 * SIZE(BUFFER)
	STFD	f0, 5 * SIZE(BUFFER)
	STFD	f0, 6 * SIZE(BUFFER)
	STFD	f0, 7 * SIZE(BUFFER)
	addi	BUFFER, BUFFER, 8 * SIZE
	bdnz	LL(06)
	.align 4

LL(10):
	li	IS, 0

	cmpwi	cr0, N, 4
	blt	LL(20)
	.align 4

LL(11):
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA
	addi	A, A, 4 * SIZE

	slwi	TEMP,  IS,  BASE_SHIFT
	add	XX, X,     TEMP
	add	YY, NEW_Y, TEMP

	LFD	atemp1, 0 * SIZE(XX)
	LFD	atemp2, 1 * SIZE(XX)
	LFD	atemp3, 2 * SIZE(XX)
	LFD	atemp4, 3 * SIZE(XX)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	LFD	a11, 2 * SIZE(AO3)
	LFD	a12, 3 * SIZE(AO3)

	LFD	a16, 3 * SIZE(AO4)

	LFD	a5, ALPHA

	FMUL	xsum1, atemp1, a1
	FMUL	xsum2, atemp1, a2
	FMUL	xsum3, atemp1, a3
	FMUL	xsum4, atemp1, a4

	FMADD	xsum1, atemp2, a2,  xsum1
	FMADD	xsum2, atemp2, a6,  xsum2
	FMADD	xsum3, atemp2, a7,  xsum3
	FMADD	xsum4, atemp2, a8,  xsum4

	FMADD	xsum1, atemp3, a3,  xsum1
	FMADD	xsum2, atemp3, a7,  xsum2
	FMADD	xsum3, atemp3, a11, xsum3
	FMADD	xsum4, atemp3, a12, xsum4

	FMADD	xsum1, atemp4, a4,  xsum1
	FMADD	xsum2, atemp4, a8,  xsum2
	FMADD	xsum3, atemp4, a12, xsum3
	FMADD	xsum4, atemp4, a16, xsum4

	FMUL	atemp1, a5, atemp1
	FMUL	atemp2, a5, atemp2
	FMUL	atemp3, a5, atemp3
	FMUL	atemp4, a5, atemp4

	LFD	xtemp1,  4 * SIZE(XX)
	LFD	xtemp2,  5 * SIZE(XX)
	LFD	xtemp3,  6 * SIZE(XX)
	LFD	xtemp4,  7 * SIZE(XX)

	LFD	y01,  4 * SIZE(YY)
	LFD	y02,  5 * SIZE(YY)
	LFD	y03,  6 * SIZE(YY)
	LFD	y04,  7 * SIZE(YY)

	LFD	a1,  4 * SIZE(AO1)
	LFD	a2,  5 * SIZE(AO1)
	LFD	a3,  6 * SIZE(AO1)
	LFD	a4,  7 * SIZE(AO1)

	LFD	a5,  4 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	LFD	a9,  4 * SIZE(AO3)
	LFD	a10, 5 * SIZE(AO3)
	LFD	a11, 6 * SIZE(AO3)
	LFD	a12, 7 * SIZE(AO3)

	LFD	a13, 4 * SIZE(AO4)
	LFD	a14, 5 * SIZE(AO4)
	LFD	a15, 6 * SIZE(AO4)
	LFD	a16, 7 * SIZE(AO4)

	addi	AO1, AO1, 4 * SIZE
	addi	AO2, AO2, 4 * SIZE
	addi	AO3, AO3, 4 * SIZE
	addi	AO4, AO4, 4 * SIZE

	addi	XX, XX, 4 * SIZE
	addi	YY, YY, 4 * SIZE

	sub	TEMP, M, IS
	addi	TEMP, TEMP, -4
	srawi.	r0,  TEMP, 4
	mtspr	CTR, r0
	ble	LL(14)
	.align 4

LL(12):
	FMADD	xsum1, xtemp1, a1,  xsum1
	DCBT(AO1, PREA)
	FMADD	y01, atemp1, a1,  y01
	LFD	a1,  4 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1,  4 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	NOP2

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2,  5 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5,  4 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	NOP1
	FMADD	y02, atemp2, a6,  y02
	LFD	a6,  5 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	NOP1
	FMADD	y03, atemp2, a7,  y03
	NOP2

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2,  5 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
#	DCBT(X, PREX)
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  4 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10, 5 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11, 6 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3,  6 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	NOP2

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4,  7 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13, 4 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8,  7 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14, 5 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12, 7 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15, 6 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4,  7 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16, 7 * SIZE(AO4)

	STFD	y01,  0 * SIZE(YY)
	LFD	y01,  4 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	LFD	y02,  5 * SIZE(YY)

	STFD	y03,  2 * SIZE(YY)
	LFD	y03,  6 * SIZE(YY)
	STFD	y04,  3 * SIZE(YY)
	LFD	y04,  7 * SIZE(YY)

	FMADD	xsum1, xtemp1, a1,  xsum1
	DCBT(AO2, PREA)
	FMADD	y01, atemp1, a1,  y01
	LFD	a1,  8 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1,  8 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	NOP2

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5,  8 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	NOP1
	FMADD	y02, atemp2, a6,  y02
	LFD	a6,  9 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	NOP1
	FMADD	y03, atemp2, a7,  y03
	NOP2

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2,  9 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  8 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7, 10 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10, 9 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11, 10 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3, 10 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	NOP2

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4, 11 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13, 8 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8, 11 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14, 9 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12, 11 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15, 10 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4, 11 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16, 11 * SIZE(AO4)

	STFD	y01,  4 * SIZE(YY)
	LFD	y01,  8 * SIZE(YY)
	STFD	y02,  5 * SIZE(YY)
	LFD	y02,  9 * SIZE(YY)

	STFD	y03,  6 * SIZE(YY)
	LFD	y03, 10 * SIZE(YY)
	STFD	y04,  7 * SIZE(YY)
	LFD	y04, 11 * SIZE(YY)


	FMADD	xsum1, xtemp1, a1,  xsum1
	DCBT(AO3, PREA)
	FMADD	y01, atemp1, a1,  y01
	LFD	a1, 12 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1, 12 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	NOP2

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2, 13 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5, 12 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	NOP1
	FMADD	y02, atemp2, a6,  y02
	LFD	a6, 13 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	NOP1
	FMADD	y03, atemp2, a7,  y03
#	DCBT(Y1, PREY)
	NOP2

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2, 13 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3, 14 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9, 12 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7, 14 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10,13 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11, 14 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3, 14 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	NOP2

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4, 15 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13,12 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8, 15 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14, 13 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12, 15 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15, 14 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4,  15 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16, 15 * SIZE(AO4)

	STFD	y01,  8 * SIZE(YY)
	LFD	y01, 12 * SIZE(YY)
	STFD	y02,  9 * SIZE(YY)
	LFD	y02, 13 * SIZE(YY)

	STFD	y03, 10 * SIZE(YY)
	LFD	y03, 14 * SIZE(YY)
	STFD	y04, 11 * SIZE(YY)
	LFD	y04, 15 * SIZE(YY)

	FMADD	xsum1, xtemp1, a1,  xsum1
	DCBT(AO4, PREA)
	FMADD	y01, atemp1, a1,  y01
	LFD	a1, 16 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1, 16 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	addi	YY, YY, 16 * SIZE

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2, 17 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5, 16 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	addi	AO3, AO3, 16 * SIZE
	FMADD	y02, atemp2, a6,  y02
	LFD	a6, 17 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	addi	AO1, AO1, 16 * SIZE
	FMADD	y03, atemp2, a7,  y03
	addi	AO2, AO2, 16 * SIZE

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2, 17 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
	addi	AO4, AO4, 16 * SIZE

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3,  2 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  0 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7,  2 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10,  1 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11,  2 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3, 18 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	addi	XX, XX, 16 * SIZE

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4,  3 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13,  0 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8,  3 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14,  1 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12,  3 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15,  2 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4,  3 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16,  3 * SIZE(AO4)

	STFD	y01, -4 * SIZE(YY)
	LFD	y01,  0 * SIZE(YY)
	STFD	y02, -3 * SIZE(YY)
	LFD	y02,  1 * SIZE(YY)

	STFD	y03, -2 * SIZE(YY)
	LFD	y03,  2 * SIZE(YY)
	STFD	y04, -1 * SIZE(YY)
	LFD	y04,  3 * SIZE(YY)
	bdnz	LL(12)
	.align 4

LL(14):
	sub	TEMP, M, IS
	addi	TEMP, TEMP, -4
	andi.	r0,  TEMP, 8
	ble	LL(15)

	FMADD	xsum1, xtemp1, a1,  xsum1
	NOP1
	FMADD	y01, atemp1, a1,  y01
	LFD	a1,  4 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1,  4 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	NOP2

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2,  5 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5,  4 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	NOP1
	FMADD	y02, atemp2, a6,  y02
	LFD	a6,  5 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	NOP1
	FMADD	y03, atemp2, a7,  y03
	NOP2

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2,  5 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  4 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10, 5 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11, 6 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3,  6 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	NOP2

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4,  7 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13, 4 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8,  7 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14, 5 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12, 7 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15, 6 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4,  7 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16, 7 * SIZE(AO4)

	STFD	y01,  0 * SIZE(YY)
	LFD	y01,  4 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	LFD	y02,  5 * SIZE(YY)

	STFD	y03,  2 * SIZE(YY)
	LFD	y03,  6 * SIZE(YY)
	STFD	y04,  3 * SIZE(YY)
	LFD	y04,  7 * SIZE(YY)

	FMADD	xsum1, xtemp1, a1,  xsum1
	NOP1
	FMADD	y01, atemp1, a1,  y01
	LFD	a1,  8 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1,  8 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	NOP2

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5,  8 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	NOP1
	FMADD	y02, atemp2, a6,  y02
	LFD	a6,  9 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	NOP1
	FMADD	y03, atemp2, a7,  y03
	NOP2

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2,  9 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  8 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7, 10 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10, 9 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11, 10 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3, 10 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	NOP2

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4, 11 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13, 8 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8, 11 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14, 9 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12, 11 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15, 10 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4, 11 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16, 11 * SIZE(AO4)

	addi	AO1, AO1, 8 * SIZE
	addi	AO2, AO2, 8 * SIZE
	addi	AO3, AO3, 8 * SIZE
	addi	AO4, AO4, 8 * SIZE

	STFD	y01,  4 * SIZE(YY)
	LFD	y01,  8 * SIZE(YY)
	STFD	y02,  5 * SIZE(YY)
	LFD	y02,  9 * SIZE(YY)

	STFD	y03,  6 * SIZE(YY)
	LFD	y03, 10 * SIZE(YY)
	STFD	y04,  7 * SIZE(YY)
	LFD	y04, 11 * SIZE(YY)

	addi	XX, XX, 8 * SIZE
	addi	YY, YY, 8 * SIZE
	.align 4

LL(15):
	sub	TEMP, M, IS
	addi	TEMP, TEMP, -4
	andi.	r0,  TEMP, 4
	ble	LL(16)

	FMADD	xsum1, xtemp1, a1,  xsum1
	NOP1
	FMADD	y01, atemp1, a1,  y01
	LFD	a1,  4 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	NOP1
	FMADD	y02, atemp1, a2,  y02
	NOP2

	FMADD	xsum3, xtemp1, a9,  xsum3
	NOP1
	FMADD	y03, atemp1, a3,  y03
	NOP2

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1,  4 * SIZE(XX)
	FMADD	y04, atemp1, a4,  y04
	NOP2

	FMADD	xsum1, xtemp2, a2,  xsum1
	LFD	a2,  5 * SIZE(AO1)
	FMADD	y01, atemp2, a5,  y01
	LFD	a5,  4 * SIZE(AO2)

	FMADD	xsum2, xtemp2, a6,  xsum2
	NOP1
	FMADD	y02, atemp2, a6,  y02
	LFD	a6,  5 * SIZE(AO2)

	FMADD	xsum3, xtemp2, a10, xsum3
	NOP1
	FMADD	y03, atemp2, a7,  y03
	NOP2

	FMADD	xsum4, xtemp2, a14, xsum4
	LFD	xtemp2,  5 * SIZE(XX)
	FMADD	y04, atemp2, a8,  y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  4 * SIZE(AO3)

	FMADD	xsum2, xtemp3, a7,  xsum2
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y02, atemp3, a10, y02
	LFD	a10, 5 * SIZE(AO3)

	FMADD	xsum3, xtemp3, a11, xsum3
	NOP1
	FMADD	y03, atemp3, a11, y03
	LFD	a11, 6 * SIZE(AO3)

	FMADD	xsum4, xtemp3, a15, xsum4
	LFD	xtemp3,  6 * SIZE(XX)
	FMADD	y04, atemp3, a12, y04
	NOP2

	FMADD	xsum1, xtemp4, a4,  xsum1
	LFD	a4,  7 * SIZE(AO1)
	FMADD	y01, atemp4, a13, y01
	LFD	a13, 4 * SIZE(AO4)

	FMADD	xsum2, xtemp4, a8,  xsum2
	LFD	a8,  7 * SIZE(AO2)
	FMADD	y02, atemp4, a14, y02
	LFD	a14, 5 * SIZE(AO4)

	FMADD	xsum3, xtemp4, a12, xsum3
	LFD	a12, 7 * SIZE(AO3)
	FMADD	y03, atemp4, a15, y03
	LFD	a15, 6 * SIZE(AO4)

	FMADD	xsum4, xtemp4, a16, xsum4
	LFD	xtemp4,  7 * SIZE(XX)
	FMADD	y04, atemp4, a16, y04
	LFD	a16, 7 * SIZE(AO4)

	addi	AO1, AO1, 4 * SIZE
	addi	AO2, AO2, 4 * SIZE
	addi	AO3, AO3, 4 * SIZE
	addi	AO4, AO4, 4 * SIZE

	STFD	y01,  0 * SIZE(YY)
	LFD	y01,  4 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	LFD	y02,  5 * SIZE(YY)

	STFD	y03,  2 * SIZE(YY)
	LFD	y03,  6 * SIZE(YY)
	STFD	y04,  3 * SIZE(YY)
	LFD	y04,  7 * SIZE(YY)

	addi	XX, XX, 4 * SIZE
	addi	YY, YY, 4 * SIZE
	.align 4

LL(16):
	andi.	r0,  M, 2
	ble	LL(17)

	FMADD	xsum1, xtemp1, a1,  xsum1
	FMADD	y01, atemp1, a1,  y01
	LFD	a1,  2 * SIZE(AO1)

	FMADD	xsum2, xtemp1, a5,  xsum2
	FMADD	y02, atemp1, a2,  y02

	FMADD	xsum3, xtemp1, a9,  xsum3
	FMADD	y01, atemp2, a5,  y01
	LFD	a5,  2 * SIZE(AO2)

	FMADD	xsum4, xtemp1, a13, xsum4
	LFD	xtemp1,  2 * SIZE(XX)
	FMADD	y02, atemp2, a6,  y02

	FMADD	xsum1, xtemp2, a2,  xsum1
	FMADD	y01, atemp3, a9,  y01
	LFD	a9,  2 * SIZE(AO3)

	FMADD	xsum2, xtemp2, a6,  xsum2
	FMADD	y02, atemp3, a10, y02

	FMADD	xsum3, xtemp2, a10, xsum3
	FMADD	y01, atemp4, a13, y01
	LFD	a13, 2 * SIZE(AO4)

	FMADD	xsum4, xtemp2, a14, xsum4
	FMADD	y02, atemp4, a14, y02

	STFD	y01,  0 * SIZE(YY)
	LFD	y01,  2 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	addi	YY, YY, 2 * SIZE
	.align 4

LL(17):
	andi.	r0,  M, 1
	ble	LL(18)

	FMADD	xsum1, xtemp1, a1,  xsum1
	FMADD	y01, atemp1, a1,  y01
	FMADD	xsum2, xtemp1, a5,  xsum2
	FMADD	y01, atemp2, a5,  y01
	FMADD	xsum3, xtemp1, a9,  xsum3
	FMADD	y01, atemp3, a9,  y01
	FMADD	xsum4, xtemp1, a13, xsum4
	FMADD	y01, atemp4, a13, y01

	STFD	y01,  0 * SIZE(YY)
	.align 4

LL(18):
	slwi	TEMP,  IS,  BASE_SHIFT
	add	YY, NEW_Y, TEMP

	LFD	y01,  0 * SIZE(YY)
	LFD	y02,  1 * SIZE(YY)
	LFD	y03,  2 * SIZE(YY)
	LFD	y04,  3 * SIZE(YY)

	LFD	xtemp1, ALPHA

	FMUL	xsum1, xtemp1, xsum1
	FMUL	xsum2, xtemp1, xsum2
	FMUL	xsum3, xtemp1, xsum3
	FMUL	xsum4, xtemp1, xsum4

	FADD	y01, y01, xsum1
	FADD	y02, y02, xsum2
	FADD	y03, y03, xsum3
	FADD	y04, y04, xsum4

	STFD	y01,  0 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	STFD	y03,  2 * SIZE(YY)
	STFD	y04,  3 * SIZE(YY)

	addi	TEMP, IS, 8
	addi	IS,   IS, 4
	cmpw	cr0, TEMP, N
	ble	LL(11)
	.align 4

LL(20):
	andi.	TEMP, N, 2
	ble	LL(30)

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA
	addi	A, A, 2 * SIZE

	slwi	TEMP,  IS,  BASE_SHIFT
	add	XX, X,     TEMP
	add	YY, NEW_Y, TEMP

	LFD	atemp1, 0 * SIZE(XX)
	LFD	atemp2, 1 * SIZE(XX)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a6,  1 * SIZE(AO2)

	LFD	a5, ALPHA

	FMUL	xsum1, atemp1, a1
	FMUL	xsum2, atemp1, a2

	FMADD	xsum1, atemp2, a2,  xsum1
	FMADD	xsum2, atemp2, a6,  xsum2

	FMUL	atemp1, a5, atemp1
	FMUL	atemp2, a5, atemp2

	LFD	xtemp1,  2 * SIZE(XX)
	LFD	y01,     2 * SIZE(YY)
	LFD	a1,      2 * SIZE(AO1)
	LFD	a5,      2 * SIZE(AO2)

	andi.	r0,  M, 1
	ble	LL(28)

	FMADD	xsum1, xtemp1, a1,  xsum1
	FMADD	y01, atemp1, a1,  y01
	FMADD	xsum2, xtemp1, a5,  xsum2
	FMADD	y01, atemp2, a5,  y01

	STFD	y01,  2 * SIZE(YY)
	.align 4

LL(28):
	slwi	TEMP,  IS,  BASE_SHIFT
	add	YY, NEW_Y, TEMP

	LFD	y01,  0 * SIZE(YY)
	LFD	y02,  1 * SIZE(YY)

	LFD	xtemp1, ALPHA

	FMUL	xsum1, xtemp1, xsum1
	FMUL	xsum2, xtemp1, xsum2

	FADD	y01, y01, xsum1
	FADD	y02, y02, xsum2

	STFD	y01,  0 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)

	addi	IS,   IS, 2
	.align 4

LL(30):
	andi.	TEMP, N, 1
	ble	LL(990)

	mr	AO1, A

	slwi	TEMP,  IS,  BASE_SHIFT
	add	XX, X,     TEMP
	add	YY, NEW_Y, TEMP

	LFD	atemp1, 0 * SIZE(XX)
	LFD	a1,     0 * SIZE(AO1)
	LFD	xtemp1, ALPHA
	LFD	y01,    0 * SIZE(YY)

	FMUL	xsum1, atemp1, a1
	FMUL	xsum1, xtemp1, xsum1

	FADD	y01, y01, xsum1

	STFD	y01,  0 * SIZE(YY)
	.align 4

LL(990):
	cmpwi	cr0, INCY, SIZE
	beq	LL(999)

	mr	YY, Y

	srawi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(995)
	.align 4

LL(991):
	LFD	f0,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f1,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f2,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f3,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f4,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f5,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f6,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f7,  0 * SIZE(Y)
	add	Y, Y, INCY

	LFD	f8,   0 * SIZE(NEW_Y)
	LFD	f9,   1 * SIZE(NEW_Y)
	LFD	f10,  2 * SIZE(NEW_Y)
	LFD	f11,  3 * SIZE(NEW_Y)
	LFD	f12,  4 * SIZE(NEW_Y)
	LFD	f13,  5 * SIZE(NEW_Y)
	LFD	f14,  6 * SIZE(NEW_Y)
	LFD	f15,  7 * SIZE(NEW_Y)
	addi	NEW_Y, NEW_Y, 8 * SIZE

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3
	FADD	f12, f12, f4
	FADD	f13, f13, f5
	FADD	f14, f14, f6
	FADD	f15, f15, f7

	STFD	f8,  0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f9,  0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f10, 0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f11, 0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f12, 0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f13, 0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f14, 0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f15, 0 * SIZE(YY)
	add	YY, YY, INCY
	bdnz	LL(991)
	.align 4

LL(995):
	andi.	J, M, 4
	ble	LL(996)

	LFD	f0,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f1,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f2,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f3,  0 * SIZE(Y)
	add	Y, Y, INCY

	LFD	f8,   0 * SIZE(NEW_Y)
	LFD	f9,   1 * SIZE(NEW_Y)
	LFD	f10,  2 * SIZE(NEW_Y)
	LFD	f11,  3 * SIZE(NEW_Y)
	addi	NEW_Y, NEW_Y, 4 * SIZE

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3

	STFD	f8,  0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f9,  0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f10, 0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f11, 0 * SIZE(YY)
	add	YY, YY, INCY
	.align 4

LL(996):
	andi.	J, M, 2
	ble	LL(997)

	LFD	f0,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f1,  0 * SIZE(Y)
	add	Y, Y, INCY

	LFD	f8,   0 * SIZE(NEW_Y)
	LFD	f9,   1 * SIZE(NEW_Y)
	addi	NEW_Y, NEW_Y, 2 * SIZE

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1

	STFD	f8,  0 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f9,  0 * SIZE(YY)
	add	YY, YY, INCY
	.align 4

LL(997):
	andi.	J, M, 1
	ble	LL(999)

	LFD	f0,  0 * SIZE(Y)
	LFD	f8,   0 * SIZE(NEW_Y)

	FADD	f8,  f8,  f0

	STFD	f8,  0 * SIZE(YY)
	.align 4

LL(999):
	li	r3, 0

	lfd	f14,     0(SP)
	lfd	f15,     8(SP)
	lfd	f16,    16(SP)
	lfd	f17,    24(SP)
	lfd	f18,    32(SP)
	lfd	f19,    40(SP)
	lfd	f20,    48(SP)
	lfd	f21,    56(SP)
	lfd	f22,    64(SP)
	lfd	f23,    72(SP)
	lfd	f24,    80(SP)
	lfd	f25,    88(SP)
	lfd	f26,    96(SP)
	lfd	f27,   104(SP)
	lfd	f28,   112(SP)
	lfd	f29,   120(SP)
	lfd	f30,   128(SP)
	lfd	f31,   136(SP)

#ifdef __64BIT__
	ld	r14,   144(SP)
	ld	r15,   152(SP)
	ld	r16,   160(SP)
	ld	r17,   168(SP)
	ld	r18,   176(SP)
	ld	r19,   184(SP)
	ld	r20,   192(SP)
	ld	r21,   200(SP)
	ld	r22,   208(SP)
	ld	r23,   216(SP)
	ld	r24,   224(SP)
	ld	r25,   232(SP)
	ld	r26,   240(SP)
	ld	r27,   248(SP)
#else
	lwz	r14,   144(SP)
	lwz	r15,   148(SP)
	lwz	r16,   152(SP)
	lwz	r17,   156(SP)
	lwz	r18,   160(SP)
	lwz	r19,   164(SP)
	lwz	r20,   168(SP)
	lwz	r21,   172(SP)
	lwz	r22,   176(SP)
	lwz	r23,   180(SP)
	lwz	r24,   184(SP)
	lwz	r25,   188(SP)
	lwz	r26,   192(SP)
	lwz	r27,   196(SP)
#endif

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
#endif
