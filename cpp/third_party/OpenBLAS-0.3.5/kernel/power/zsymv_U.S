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
#define IS	r4
#define A	r5
#define LDA	r6
#define X	r7
#define	INCX	r8
#define	Y	r9
#define	INCY	r10
#define BUFFER	r14
#else
#define M	r3
#define IS	r4
#define A	r7
#define LDA	r8
#define X	r9
#define	INCX	r10
#define	Y	r5
#define	INCY	r6
#define BUFFER	r14
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define M	r3
#define IS	r4
#define A	r9
#define LDA	r10
#define X	r5
#define	INCX	r6
#define	Y	r7
#define	INCY	r8
#define BUFFER	r14
#else
#define M	r3
#define IS	r4
#define A	r7
#define LDA	r8
#define X	r9
#define	INCX	r10
#define	Y	r5
#define	INCY	r6
#define BUFFER	r14
#endif
#endif

#define I	r11
#define	J	r12

#define AO1	r15
#define AO2	r16
#define XX	r19
#define YY	r20
#define	NEW_Y	r21
#define TEMP	r22
#define	PREA	r24

#define y01 f0
#define y02 f1
#define y03 f2
#define y04 f3
#define y05 f4
#define y06 f5
#define y07 f6
#define y08 f7

#define xtemp1 f8
#define xtemp2 f9
#define xtemp3 f10
#define xtemp4 f11
#define xtemp5 f12
#define xtemp6 f13
#define xtemp7 f14
#define xtemp8 f15

#define atemp1 f16
#define atemp2 f17
#define atemp3 f18
#define atemp4 f19

#define xsum1  f20
#define xsum2  f21
#define xsum3  f22
#define xsum4  f23

#define a1     f24
#define a2     f25
#define a3     f26
#define a4     f27
#define a5     f28
#define a6     f29
#define a7     f30
#define a8     f31

#define alpha_r  f1
#define alpha_i  f2

#if defined(PPCG4)
#define PREFETCHSIZE_A  24
#endif

#if defined(PPC440) || defined(PPC440FP2)
#define PREFETCHSIZE_A  24
#endif

#ifdef PPC970
#define PREFETCHSIZE_A  32
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
#define PREFETCHSIZE_A  112
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
#define ALPHA_R     200(SP)
#define ALPHA_I     208(SP)
#define	FZERO	    216(SP)
#else
#define STACKSIZE 280
#define ALPHA_R     256(SP)
#define ALPHA_I     264(SP)
#define FZERO	    272(SP)
#endif

#ifndef HEMV
#define FMADD1	FNMSUB
#define FMADD2	FMADD
#else
#define FMADD1	FMADD
#define FMADD2	FNMSUB
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
	ld	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	ld	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifndef __64BIT__
#ifdef DOUBLE
	lwz	X,       FRAMESLOT(0) + STACKSIZE(SP)
	lwz	INCX,    FRAMESLOT(1) + STACKSIZE(SP)
	lwz	Y,       FRAMESLOT(2) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(3) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(4) + STACKSIZE(SP)
#else
	lwz	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#else
	ld	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	ld	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

	STFD	alpha_r, ALPHA_R
	STFD	alpha_i, ALPHA_I

	slwi	LDA,  LDA,  ZBASE_SHIFT
	slwi	INCX, INCX, ZBASE_SHIFT
	slwi	INCY, INCY, ZBASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE
	sub	IS, M, IS

	cmpwi	cr0, M, 0
	ble-	LL(999)

	mullw	TEMP, IS, LDA
	add	A, A, TEMP

	cmpwi	cr0, INCX, 2 * SIZE
	beq	LL(05)

	mr	XX, X
	mr	X, BUFFER

	srawi.	r0, M, 2
	mtspr	CTR, r0
	ble	LL(03)
	.align 4

LL(01):
	LFD	a1, 0 * SIZE(XX)
	LFD	a2, 1 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a3, 0 * SIZE(XX)
	LFD	a4, 1 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a5, 0 * SIZE(XX)
	LFD	a6, 1 * SIZE(XX)
	add	XX, XX, INCX
	LFD	a7, 0 * SIZE(XX)
	LFD	a8, 1 * SIZE(XX)
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
	andi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(05)
	.align 4

LL(04):
	LFD	a1, 0 * SIZE(XX)
	LFD	a2, 1 * SIZE(XX)
	add	XX, XX, INCX

	STFD	a1, 0 * SIZE(BUFFER)
	STFD	a2, 1 * SIZE(BUFFER)

	addi	BUFFER, BUFFER, 2 * SIZE
	bdnz	LL(04)
	.align 4

LL(05):
	mr	NEW_Y, Y
	lfd	f0, FZERO

	cmpwi	cr0, INCY, 2 * SIZE
	beq	LL(10)

	mr	NEW_Y, BUFFER

	addi	r0, M,  3
	srawi.	r0, r0, 2
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
	addi	TEMP, IS, 2
	cmpw	cr0, TEMP, M
	bgt	LL(20)
	.align 4

LL(11):
	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	slwi	TEMP,  IS,  ZBASE_SHIFT
	add	TEMP, X, TEMP

	LFD	y05, ALPHA_R
	LFD	y06, ALPHA_I

	LFD	xtemp1, 0 * SIZE(TEMP)
	LFD	xtemp2, 1 * SIZE(TEMP)
	LFD	xtemp3, 2 * SIZE(TEMP)
	LFD	xtemp4, 3 * SIZE(TEMP)

	FMUL	atemp1, y05, xtemp1
	FMUL	atemp2, y06, xtemp1
	FMUL	atemp3, y05, xtemp3
	FMUL	atemp4, y06, xtemp3

	FNMSUB	atemp1, y06, xtemp2, atemp1
	FMADD	atemp2, y05, xtemp2, atemp2
	FNMSUB	atemp3, y06, xtemp4, atemp3
	FMADD	atemp4, y05, xtemp4, atemp4

	lfd	xsum1, FZERO
	fmr	xsum2, xsum1
	fmr	xsum3, xsum1
	fmr	xsum4, xsum1

	mr	XX, X
	mr	YY, NEW_Y

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	LFD	xtemp1,  0 * SIZE(XX)
	LFD	xtemp2,  1 * SIZE(XX)
	LFD	xtemp3,  2 * SIZE(XX)
	LFD	xtemp4,  3 * SIZE(XX)

	LFD	y01,  0 * SIZE(YY)
	LFD	y02,  1 * SIZE(YY)
	LFD	y03,  2 * SIZE(YY)
	LFD	y04,  3 * SIZE(YY)

	srawi.	r0,  IS, 3
	mtspr	CTR, r0
	ble	LL(15)

	FMADD	xsum1, xtemp1, a1,  xsum1
	DCBT(AO1, PREA)
	FMADD	y01, atemp1, a1, y01
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	NOP1
	FMADD	y02, atemp2, a1, y02
	LFD	a1,  4 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	NOP1
	FMADD	y03, atemp1, a3, y03
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	NOP1
	FMADD	y04, atemp2, a3, y04
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y05,  4 * SIZE(YY)
	FNMSUB	y01, atemp2, a2, y01
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y06,  5 * SIZE(YY)
	FMADD	y02, atemp1, a2, y02
	LFD	a2,  5 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2,  5 * SIZE(XX)
	FNMSUB	y03, atemp2, a4, y03
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1,  4 * SIZE(XX)
	FMADD	y04, atemp1, a4, y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y07,  6 * SIZE(YY)
	FMADD	y01, atemp3, a5, y01
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y02, atemp4, a5, y02
	LFD	a5,  4 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y08,  7 * SIZE(YY)
	FMADD	y03, atemp3, a7, y03
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y04, atemp4, a7, y04
	LFD	a7,  6 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y01, atemp4, a6, y01
#	DCBT(X, PREX)
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4,  7 * SIZE(AO1)
	FMADD	y02, atemp3, a6, y02
	LFD	a6,  5 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4,  7 * SIZE(XX)
	FNMSUB	y03, atemp4, a8, y03
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3,  6 * SIZE(XX)
	FMADD	y04, atemp3, a8, y04
	LFD	a8,  7 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y01,  0 * SIZE(YY)
	FMADD	y05, atemp1, a1, y05
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y02,  1 * SIZE(YY)
	FMADD	y06, atemp2, a1, y06
	LFD	a1,  8 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y03,  2 * SIZE(YY)
	FMADD	y07, atemp1, a3, y07
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y04,  3 * SIZE(YY)
	FMADD	y08, atemp2, a3, y08
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y01,  8 * SIZE(YY)
	FNMSUB	y05, atemp2, a2, y05
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y02,  9 * SIZE(YY)
	FMADD	y06, atemp1, a2, y06
	LFD	a2,  9 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2,  9 * SIZE(XX)
	FNMSUB	y07, atemp2, a4, y07
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1,  8 * SIZE(XX)
	FMADD	y08, atemp1, a4, y08
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y03, 10 * SIZE(YY)
	FMADD	y05, atemp3, a5, y05
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y06, atemp4, a5, y06
	LFD	a5,  8 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y04, 11 * SIZE(YY)
	FMADD	y07, atemp3, a7, y07
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y08, atemp4, a7, y08
	LFD	a7, 10 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y05, atemp4, a6, y05
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4, 11 * SIZE(AO1)
	FMADD	y06, atemp3, a6, y06
	LFD	a6,  9 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4, 11 * SIZE(XX)
	FNMSUB	y07, atemp4, a8, y07
	bdz	LL(13)
	.align 4

LL(12):
	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3, 10 * SIZE(XX)
	FMADD	y08, atemp3, a8, y08
	LFD	a8, 11 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y05,  4 * SIZE(YY)
	FMADD	y01, atemp1, a1, y01
	DCBT(AO2, PREA)

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y06,  5 * SIZE(YY)
	FMADD	y02, atemp2, a1, y02
	LFD	a1, 12 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y07,  6 * SIZE(YY)
	FMADD	y03, atemp1, a3, y03
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y08,  7 * SIZE(YY)
	FMADD	y04, atemp2, a3, y04
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y05, 12 * SIZE(YY)
	FNMSUB	y01, atemp2, a2, y01
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y06, 13 * SIZE(YY)
	FMADD	y02, atemp1, a2, y02
	LFD	a2, 13 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2, 13 * SIZE(XX)
	FNMSUB	y03, atemp2, a4, y03
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1, 12 * SIZE(XX)
	FMADD	y04, atemp1, a4, y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y07, 14 * SIZE(YY)
	FMADD	y01, atemp3, a5, y01
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 14 * SIZE(AO1)
	FMADD	y02, atemp4, a5, y02
	LFD	a5, 12 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y08, 15 * SIZE(YY)
	FMADD	y03, atemp3, a7, y03
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y04, atemp4, a7, y04
	LFD	a7, 14 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y01, atemp4, a6, y01
#	DCBT(Y1, PREY)
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4, 15 * SIZE(AO1)
	FMADD	y02, atemp3, a6, y02
	LFD	a6, 13 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4, 15 * SIZE(XX)
	FNMSUB	y03, atemp4, a8, y03
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3, 14 * SIZE(XX)
	FMADD	y04, atemp3, a8, y04
	LFD	a8, 15 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y01,  8 * SIZE(YY)
	FMADD	y05, atemp1, a1, y05
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y02,  9 * SIZE(YY)
	FMADD	y06, atemp2, a1, y06
	LFD	a1, 16 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y03, 10 * SIZE(YY)
	FMADD	y07, atemp1, a3, y07
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y04, 11 * SIZE(YY)
	FMADD	y08, atemp2, a3, y08
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y01, 16 * SIZE(YY)
	FNMSUB	y05, atemp2, a2, y05
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y02, 17 * SIZE(YY)
	FMADD	y06, atemp1, a2, y06
	LFD	a2, 17 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2, 17 * SIZE(XX)
	FNMSUB	y07, atemp2, a4, y07
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1, 16 * SIZE(XX)
	FMADD	y08, atemp1, a4, y08
	addi	AO2, AO2, 16 * SIZE

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y03, 18 * SIZE(YY)
	FMADD	y05, atemp3, a5, y05
	addi	XX, XX, 16 * SIZE

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 18 * SIZE(AO1)
	FMADD	y06, atemp4, a5, y06
	LFD	a5,  0 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y04, 19 * SIZE(YY)
	FMADD	y07, atemp3, a7, y07
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	addi	AO1, AO1, 16 * SIZE
	FMADD	y08, atemp4, a7, y08
	LFD	a7,  2 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	addi	YY, YY, 16 * SIZE
	FNMSUB	y05, atemp4, a6, y05
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4,  3 * SIZE(AO1)
	FMADD	y06, atemp3, a6, y06
	LFD	a6,  1 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4,  3 * SIZE(XX)
	FNMSUB	y07, atemp4, a8, y07
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3,  2 * SIZE(XX)
	FMADD	y08, atemp3, a8, y08
	LFD	a8,  3 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y05, -4 * SIZE(YY)
	FMADD	y01, atemp1, a1, y01
	DCBT(AO1, PREA)

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y06, -3 * SIZE(YY)
	FMADD	y02, atemp2, a1, y02
	LFD	a1,  4 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y07, -2 * SIZE(YY)
	FMADD	y03, atemp1, a3, y03
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y08, -1 * SIZE(YY)
	FMADD	y04, atemp2, a3, y04
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y05,  4 * SIZE(YY)
	FNMSUB	y01, atemp2, a2, y01
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y06,  5 * SIZE(YY)
	FMADD	y02, atemp1, a2, y02
	LFD	a2,  5 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2,  5 * SIZE(XX)
	FNMSUB	y03, atemp2, a4, y03
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1,  4 * SIZE(XX)
	FMADD	y04, atemp1, a4, y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y07,  6 * SIZE(YY)
	FMADD	y01, atemp3, a5, y01
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y02, atemp4, a5, y02
	LFD	a5,  4 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y08,  7 * SIZE(YY)
	FMADD	y03, atemp3, a7, y03
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y04, atemp4, a7, y04
	LFD	a7,  6 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y01, atemp4, a6, y01
#	DCBT(X, PREX)
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4,  7 * SIZE(AO1)
	FMADD	y02, atemp3, a6, y02
	LFD	a6,  5 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4,  7 * SIZE(XX)
	FNMSUB	y03, atemp4, a8, y03
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3,  6 * SIZE(XX)
	FMADD	y04, atemp3, a8, y04
	LFD	a8,  7 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y01,  0 * SIZE(YY)
	FMADD	y05, atemp1, a1, y05
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y02,  1 * SIZE(YY)
	FMADD	y06, atemp2, a1, y06
	LFD	a1,  8 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y03,  2 * SIZE(YY)
	FMADD	y07, atemp1, a3, y07
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y04,  3 * SIZE(YY)
	FMADD	y08, atemp2, a3, y08
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y01,  8 * SIZE(YY)
	FNMSUB	y05, atemp2, a2, y05
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y02,  9 * SIZE(YY)
	FMADD	y06, atemp1, a2, y06
	LFD	a2,  9 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2,  9 * SIZE(XX)
	FNMSUB	y07, atemp2, a4, y07
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1,  8 * SIZE(XX)
	FMADD	y08, atemp1, a4, y08
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y03, 10 * SIZE(YY)
	FMADD	y05, atemp3, a5, y05
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y06, atemp4, a5, y06
	LFD	a5,  8 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y04, 11 * SIZE(YY)
	FMADD	y07, atemp3, a7, y07
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y08, atemp4, a7, y08
	LFD	a7, 10 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y05, atemp4, a6, y05
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4, 11 * SIZE(AO1)
	FMADD	y06, atemp3, a6, y06
	LFD	a6,  9 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4, 11 * SIZE(XX)
	FNMSUB	y07, atemp4, a8, y07
	bdnz	LL(12)
	.align 4

LL(13):
	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3, 10 * SIZE(XX)
	FMADD	y08, atemp3, a8, y08
	LFD	a8, 11 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y05,  4 * SIZE(YY)
	FMADD	y01, atemp1, a1, y01
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y06,  5 * SIZE(YY)
	FMADD	y02, atemp2, a1, y02
	LFD	a1, 12 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y07,  6 * SIZE(YY)
	FMADD	y03, atemp1, a3, y03
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y08,  7 * SIZE(YY)
	FMADD	y04, atemp2, a3, y04
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y05, 12 * SIZE(YY)
	FNMSUB	y01, atemp2, a2, y01
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y06, 13 * SIZE(YY)
	FMADD	y02, atemp1, a2, y02
	LFD	a2, 13 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2, 13 * SIZE(XX)
	FNMSUB	y03, atemp2, a4, y03
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1, 12 * SIZE(XX)
	FMADD	y04, atemp1, a4, y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y07, 14 * SIZE(YY)
	FMADD	y01, atemp3, a5, y01
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 14 * SIZE(AO1)
	FMADD	y02, atemp4, a5, y02
	LFD	a5, 12 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y08, 15 * SIZE(YY)
	FMADD	y03, atemp3, a7, y03
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y04, atemp4, a7, y04
	LFD	a7, 14 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y01, atemp4, a6, y01
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4, 15 * SIZE(AO1)
	FMADD	y02, atemp3, a6, y02
	LFD	a6, 13 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4, 15 * SIZE(XX)
	FNMSUB	y03, atemp4, a8, y03
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3, 14 * SIZE(XX)
	FMADD	y04, atemp3, a8, y04
	LFD	a8, 15 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y01,  8 * SIZE(YY)
	FMADD	y05, atemp1, a1, y05
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y02,  9 * SIZE(YY)
	FMADD	y06, atemp2, a1, y06
	LFD	a1, 16 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y03, 10 * SIZE(YY)
	FMADD	y07, atemp1, a3, y07
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y04, 11 * SIZE(YY)
	FMADD	y08, atemp2, a3, y08
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y01, 16 * SIZE(YY)
	FNMSUB	y05, atemp2, a2, y05
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y02, 17 * SIZE(YY)
	FMADD	y06, atemp1, a2, y06
	LFD	a2, 17 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2, 17 * SIZE(XX)
	FNMSUB	y07, atemp2, a4, y07
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1, 16 * SIZE(XX)
	FMADD	y08, atemp1, a4, y08
	addi	AO2, AO2, 16 * SIZE

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y03, 18 * SIZE(YY)
	FMADD	y05, atemp3, a5, y05
	addi	XX, XX, 16 * SIZE

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 18 * SIZE(AO1)
	FMADD	y06, atemp4, a5, y06
	LFD	a5,  0 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y04, 19 * SIZE(YY)
	FMADD	y07, atemp3, a7, y07
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	addi	AO1, AO1, 16 * SIZE
	FMADD	y08, atemp4, a7, y08
	LFD	a7,  2 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	addi	YY, YY, 16 * SIZE
	FNMSUB	y05, atemp4, a6, y05
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4,  3 * SIZE(AO1)
	FMADD	y06, atemp3, a6, y06
	LFD	a6,  1 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4,  3 * SIZE(XX)
	FNMSUB	y07, atemp4, a8, y07
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3,  2 * SIZE(XX)
	FMADD	y08, atemp3, a8, y08
	LFD	a8,  3 * SIZE(AO2)

	STFD	y05, -4 * SIZE(YY)
	STFD	y06, -3 * SIZE(YY)
	STFD	y07, -2 * SIZE(YY)
	STFD	y08, -1 * SIZE(YY)
	.align 4

LL(15):
	andi.	r0,  IS, 4
	ble	LL(16)

	FMADD	xsum1, xtemp1, a1,  xsum1
	NOP1
	FMADD	y01, atemp1, a1, y01
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	NOP1
	FMADD	y02, atemp2, a1, y02
	LFD	a1,  4 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	NOP1
	FMADD	y03, atemp1, a3, y03
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	NOP1
	FMADD	y04, atemp2, a3, y04
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y05,  4 * SIZE(YY)
	FNMSUB	y01, atemp2, a2, y01
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y06,  5 * SIZE(YY)
	FMADD	y02, atemp1, a2, y02
	LFD	a2,  5 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2,  5 * SIZE(XX)
	FNMSUB	y03, atemp2, a4, y03
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1,  4 * SIZE(XX)
	FMADD	y04, atemp1, a4, y04
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y07,  6 * SIZE(YY)
	FMADD	y01, atemp3, a5, y01
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y02, atemp4, a5, y02
	LFD	a5,  4 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y08,  7 * SIZE(YY)
	FMADD	y03, atemp3, a7, y03
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y04, atemp4, a7, y04
	LFD	a7,  6 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y01, atemp4, a6, y01
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4,  7 * SIZE(AO1)
	FMADD	y02, atemp3, a6, y02
	LFD	a6,  5 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4,  7 * SIZE(XX)
	FNMSUB	y03, atemp4, a8, y03
	NOP2

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3,  6 * SIZE(XX)
	FMADD	y04, atemp3, a8, y04
	LFD	a8,  7 * SIZE(AO2)

	FMADD	xsum1, xtemp1, a1,  xsum1
	STFD	y01,  0 * SIZE(YY)
	FMADD	y05, atemp1, a1, y05
	NOP2

	FMADD	xsum2, xtemp2, a1,  xsum2
	STFD	y02,  1 * SIZE(YY)
	FMADD	y06, atemp2, a1, y06
	LFD	a1,  8 * SIZE(AO1)

	FMADD	xsum3, xtemp1, a5,  xsum3
	STFD	y03,  2 * SIZE(YY)
	FMADD	y07, atemp1, a3, y07
	NOP2

	FMADD	xsum4, xtemp2, a5,  xsum4
	STFD	y04,  3 * SIZE(YY)
	FMADD	y08, atemp2, a3, y08
	NOP2

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	y01,  8 * SIZE(YY)
	FNMSUB	y05, atemp2, a2, y05
	NOP2

	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	y02,  9 * SIZE(YY)
	FMADD	y06, atemp1, a2, y06
	LFD	a2,  9 * SIZE(AO1)

	FMADD1	xsum3, xtemp2, a6,  xsum3
	LFD	xtemp2,  9 * SIZE(XX)
	FNMSUB	y07, atemp2, a4, y07
	NOP2

	FMADD2	xsum4, xtemp1, a6,  xsum4
	LFD	xtemp1,  8 * SIZE(XX)
	FMADD	y08, atemp1, a4, y08
	NOP2

	FMADD	xsum1, xtemp3, a3,  xsum1
	LFD	y03, 10 * SIZE(YY)
	FMADD	y05, atemp3, a5, y05
	NOP2

	FMADD	xsum2, xtemp4, a3,  xsum2
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y06, atemp4, a5, y06
	LFD	a5,  8 * SIZE(AO2)

	FMADD	xsum3, xtemp3, a7,  xsum3
	LFD	y04, 11 * SIZE(YY)
	FMADD	y07, atemp3, a7, y07
	NOP2

	FMADD	xsum4, xtemp4, a7,  xsum4
	NOP1
	FMADD	y08, atemp4, a7, y08
	LFD	a7, 10 * SIZE(AO2)

	FMADD1	xsum1, xtemp4, a4,  xsum1
	NOP1
	FNMSUB	y05, atemp4, a6, y05
	NOP2

	FMADD2	xsum2, xtemp3, a4,  xsum2
	LFD	a4, 11 * SIZE(AO1)
	FMADD	y06, atemp3, a6, y06
	LFD	a6,  9 * SIZE(AO2)

	FMADD1	xsum3, xtemp4, a8,  xsum3
	LFD	xtemp4, 11 * SIZE(XX)
	FNMSUB	y07, atemp4, a8, y07

	FMADD2	xsum4, xtemp3, a8,  xsum4
	LFD	xtemp3, 10 * SIZE(XX)
	FMADD	y08, atemp3, a8, y08
	LFD	a8, 11 * SIZE(AO2)

	STFD	y05,  4 * SIZE(YY)
	STFD	y06,  5 * SIZE(YY)
	STFD	y07,  6 * SIZE(YY)
	STFD	y08,  7 * SIZE(YY)

	addi	AO1, AO1, 8 * SIZE
	addi	AO2, AO2, 8 * SIZE

	addi	XX, XX, 8 * SIZE
	addi	YY, YY, 8 * SIZE
	.align 4

LL(16):
	andi.	r0,  IS, 2
	ble	LL(18)

	FMADD	xsum1, xtemp1, a1,  xsum1
	FMADD	y01, atemp1, a1, y01
	FMADD	xsum2, xtemp2, a1,  xsum2
	FMADD	y02, atemp2, a1, y02
	FMADD	xsum3, xtemp1, a5,  xsum3
	FMADD	y03, atemp1, a3, y03
	FMADD	xsum4, xtemp2, a5,  xsum4
	FMADD	y04, atemp2, a3, y04

	FMADD1	xsum1, xtemp2, a2,  xsum1
	FNMSUB	y01, atemp2, a2, y01
	FMADD2	xsum2, xtemp1, a2,  xsum2
	FMADD	y02, atemp1, a2, y02
	FMADD1	xsum3, xtemp2, a6,  xsum3
	FNMSUB	y03, atemp2, a4, y03
	FMADD2	xsum4, xtemp1, a6,  xsum4
	FMADD	y04, atemp1, a4, y04

	FMADD	xsum1, xtemp3, a3,  xsum1
	FMADD	y01, atemp3, a5, y01
	FMADD	xsum2, xtemp4, a3,  xsum2
	FMADD	y02, atemp4, a5, y02
	FMADD	xsum3, xtemp3, a7,  xsum3
	FMADD	y03, atemp3, a7, y03
	FMADD	xsum4, xtemp4, a7,  xsum4
	FMADD	y04, atemp4, a7, y04

	FMADD1	xsum1, xtemp4, a4,  xsum1
	FNMSUB	y01, atemp4, a6, y01
	FMADD2	xsum2, xtemp3, a4,  xsum2
	FMADD	y02, atemp3, a6, y02
	FMADD1	xsum3, xtemp4, a8,  xsum3
	FNMSUB	y03, atemp4, a8, y03
	FMADD2	xsum4, xtemp3, a8,  xsum4
	FMADD	y04, atemp3, a8, y04

	STFD	y01,  0 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	STFD	y03,  2 * SIZE(YY)
	STFD	y04,  3 * SIZE(YY)

	LFD	a1,  4 * SIZE(AO1)
	LFD	a2,  5 * SIZE(AO1)

	LFD	a5,  4 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	LFD	y01,  4 * SIZE(YY)
	LFD	y02,  5 * SIZE(YY)
	LFD	y03,  6 * SIZE(YY)
	LFD	y04,  7 * SIZE(YY)

	addi	YY, YY, 4 * SIZE
	.align 4

LL(18):
	LFD	y05, ALPHA_R
	LFD	y06, ALPHA_I

	FMUL	xtemp1, y05, xsum1
	FMUL	xtemp2, y06, xsum1
	FMUL	xtemp3, y05, xsum3
	FMUL	xtemp4, y06, xsum3

	FNMSUB	xsum1, y06, xsum2, xtemp1
	FMADD	xsum2, y05, xsum2, xtemp2
	FNMSUB	xsum3, y06, xsum4, xtemp3
	FMADD	xsum4, y05, xsum4, xtemp4

	FMADD	xsum1, atemp1, a1, xsum1
	FMADD	xsum2, atemp2, a1, xsum2
	FMADD	xsum3, atemp1, a5, xsum3
	FMADD	xsum4, atemp2, a5, xsum4

#ifndef HEMV
	FMADD1	xsum1, atemp2, a2, xsum1
	FMADD2	xsum2, atemp1, a2, xsum2
#endif
	FMADD1	xsum3, atemp2, a6, xsum3
	FMADD2	xsum4, atemp1, a6, xsum4

	FMADD	xsum1, atemp3, a5, xsum1
	FMADD	xsum2, atemp4, a5, xsum2
	FMADD	xsum3, atemp3, a7, xsum3
	FMADD	xsum4, atemp4, a7, xsum4

	FNMSUB	xsum1, atemp4, a6, xsum1
	FMADD	xsum2, atemp3, a6, xsum2
#ifndef HEMV
	FNMSUB	xsum3, atemp4, a8, xsum3
	FMADD	xsum4, atemp3, a8, xsum4
#endif

	FADD	y01, y01, xsum1
	FADD	y02, y02, xsum2
	FADD	y03, y03, xsum3
	FADD	y04, y04, xsum4

	STFD	y01,  0 * SIZE(YY)
	addi	TEMP, IS, 4
	STFD	y02,  1 * SIZE(YY)
	addi	IS,   IS, 2
	STFD	y03,  2 * SIZE(YY)
	cmpw	cr0, TEMP, M
	STFD	y04,  3 * SIZE(YY)
	ble	LL(11)
	.align 4

LL(20):
	andi.	TEMP, M, 1
	ble	LL(990)

	mr	AO1, A

	slwi	TEMP,  IS,  ZBASE_SHIFT
	add	TEMP, X, TEMP

	LFD	y05, ALPHA_R
	LFD	y06, ALPHA_I

	LFD	xtemp1, 0 * SIZE(TEMP)
	LFD	xtemp2, 1 * SIZE(TEMP)

	FMUL	atemp1, y05, xtemp1
	FMUL	atemp2, y06, xtemp1

	FNMSUB	atemp1, y06, xtemp2, atemp1
	FMADD	atemp2, y05, xtemp2, atemp2

	lfd	xsum1, FZERO
	fmr	xsum2, xsum1

	mr	XX, X
	mr	YY, NEW_Y

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)

	LFD	xtemp1,  0 * SIZE(XX)
	LFD	xtemp2,  1 * SIZE(XX)

	LFD	y01,  0 * SIZE(YY)
	LFD	y02,  1 * SIZE(YY)

	mtspr	CTR, IS
	cmpwi	cr0, IS, 0
	ble	LL(28)
	.align 4

LL(22):
	FMADD	xsum1, xtemp1, a1,  xsum1
	FMADD	y01, atemp1, a1, y01
	FMADD	xsum2, xtemp2, a1,  xsum2
	FMADD	y02, atemp2, a1, y02
	LFD	a1,  2 * SIZE(AO1)

	FMADD1	xsum1, xtemp2, a2,  xsum1
	LFD	xtemp2,  3 * SIZE(XX)
	FNMSUB	y01, atemp2, a2, y01
	FMADD2	xsum2, xtemp1, a2,  xsum2
	LFD	xtemp1,  2 * SIZE(XX)
	FMADD	y02, atemp1, a2, y02
	LFD	a2,  3 * SIZE(AO1)

	addi	AO1, AO1, 2 * SIZE
	addi	XX, XX, 2 * SIZE
	addi	YY, YY, 2 * SIZE

	STFD	y01, -2 * SIZE(YY)
	LFD	y01,  0 * SIZE(YY)
	STFD	y02, -1 * SIZE(YY)
	LFD	y02,  1 * SIZE(YY)
	bdnz	LL(22)
	.align 4

LL(28):
	LFD	y05, ALPHA_R
	LFD	y06, ALPHA_I

	FMUL	xtemp1, y05, xsum1
	FMUL	xtemp2, y06, xsum1

	FNMSUB	xsum1, y06, xsum2, xtemp1
	FMADD	xsum2, y05, xsum2, xtemp2

	FMADD	xsum1, atemp1, a1, xsum1
	FMADD	xsum2, atemp2, a1, xsum2

#ifndef HEMV
	FNMSUB	xsum1, atemp2, a2, xsum1
	FMADD	xsum2, atemp1, a2, xsum2
#endif

	FADD	y01, y01, xsum1
	FADD	y02, y02, xsum2

	STFD	y01,  0 * SIZE(YY)
	STFD	y02,  1 * SIZE(YY)
	.align 4

LL(990):
	cmpwi	cr0, INCY, 2 * SIZE
	beq	LL(999)

	mr	YY, Y

	srawi.	r0, M, 2
	mtspr	CTR, r0
	ble	LL(995)
	.align 4

LL(991):
	LFD	f0,  0 * SIZE(Y)
	LFD	f1,  1 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f2,  0 * SIZE(Y)
	LFD	f3,  1 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f4,  0 * SIZE(Y)
	LFD	f5,  1 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f6,  0 * SIZE(Y)
	LFD	f7,  1 * SIZE(Y)
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
	STFD	f9,  1 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f10, 0 * SIZE(YY)
	STFD	f11, 1 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f12, 0 * SIZE(YY)
	STFD	f13, 1 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f14, 0 * SIZE(YY)
	STFD	f15, 1 * SIZE(YY)
	add	YY, YY, INCY
	bdnz	LL(991)
	.align 4

LL(995):
	andi.	J, M, 2
	ble	LL(996)

	LFD	f0,  0 * SIZE(Y)
	LFD	f1,  1 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f2,  0 * SIZE(Y)
	LFD	f3,  1 * SIZE(Y)
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
	STFD	f9,  1 * SIZE(YY)
	add	YY, YY, INCY
	STFD	f10, 0 * SIZE(YY)
	STFD	f11, 1 * SIZE(YY)
	add	YY, YY, INCY
	.align 4

LL(996):
	andi.	J, M, 1
	ble	LL(999)

	LFD	f0,  0 * SIZE(Y)
	LFD	f1,  1 * SIZE(Y)

	LFD	f8,   0 * SIZE(NEW_Y)
	LFD	f9,   1 * SIZE(NEW_Y)

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1

	STFD	f8,  0 * SIZE(YY)
	STFD	f9,  1 * SIZE(YY)
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
