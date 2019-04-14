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
#define N	r3
#define X	r6
#define INCX	r7
#define Y	r8
#define INCY	r9
#define	YY	r5
#define PRE	r4
#else
#define N	r3
#define X	r7
#define INCX	r8
#define Y	r9
#define INCY	r10
#define	YY	r5
#define PRE	r4
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define	YY	r6
#define PRE	r5
#else
#define N	r3
#define X	r7
#define INCX	r8
#define Y	r9
#define INCY	r10
#define	YY	r5
#define PRE	r4
#endif
#endif

#define ALPHA	f24

#define STACKSIZE 96

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0,   0

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	stfd	f18,   32(SP)
	stfd	f19,   40(SP)
	stfd	f20,   48(SP)
	stfd	f21,   56(SP)

	stfd	f22,   64(SP)
	stfd	f23,   72(SP)
	stfd	f24,   80(SP)

#if (defined(_AIX) || defined(__APPLE__)) && !defined(__64BIT__) && defined(DOUBLE)
	lwz	INCY, FRAMESLOT(0) + STACKSIZE(SP)
#endif

	fmr	ALPHA, f1
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT
	li	PRE, 2 * 16 * SIZE

	cmpwi	cr0, N, 0
	ble-	LL(999)

	sub	X, X, INCX
	sub	Y, Y, INCY
	mr	YY, Y

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(150)
	.align 4

	LFDUX	f0,    X, INCX
	LFDUX	f1,    X, INCX
	LFDUX	f2,    X, INCX
	LFDUX	f3,    X, INCX

	LFDUX	f8,    Y, INCY
	LFDUX	f9,    Y, INCY
	LFDUX	f10,   Y, INCY
	LFDUX	f11,   Y, INCY

	LFDUX	f4,    X, INCX
	LFDUX	f5,    X, INCX
	LFDUX	f6,    X, INCX
	LFDUX	f7,    X, INCX

	LFDUX	f12,   Y, INCY
	LFDUX	f13,   Y, INCY
	LFDUX	f14,   Y, INCY
	LFDUX	f15,   Y, INCY
	bdz	LL(120)
	.align 4

LL(110):
	FMADD	f16,  ALPHA, f0, f8
	LFDUX	f0,    X, INCX
	LFDUX	f8,    Y, INCY
#ifdef PPCG4
	dcbt	X, PRE
#endif
	FMADD	f17,  ALPHA, f1, f9
	LFDUX	f1,    X, INCX
	LFDUX	f9,    Y, INCY
	FMADD	f18,  ALPHA, f2, f10
	LFDUX	f2,    X, INCX
	LFDUX	f10,   Y, INCY
#ifdef PPCG4
	dcbtst	Y, PRE
#endif
	FMADD	f19,  ALPHA, f3, f11
	LFDUX	f3,    X, INCX
	LFDUX	f11,   Y, INCY

	FMADD	f20,  ALPHA, f4, f12
	LFDUX	f4,    X, INCX
	LFDUX	f12,   Y, INCY
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif
	FMADD	f21,  ALPHA, f5, f13
	LFDUX	f5,    X, INCX
	LFDUX	f13,   Y, INCY
	FMADD	f22,  ALPHA, f6, f14
	LFDUX	f6,    X, INCX
 	LFDUX	f14,   Y, INCY
#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y, PRE
#endif
	FMADD	f23,  ALPHA, f7, f15
	LFDUX	f7,    X, INCX
	LFDUX	f15,   Y, INCY

	STFDUX	f16,   YY, INCY
	STFDUX	f17,   YY, INCY
	STFDUX	f18,   YY, INCY
	STFDUX	f19,   YY, INCY

	FMADD	f16,  ALPHA, f0, f8
	LFDUX	f0,    X, INCX
	LFDUX	f8,    Y, INCY
#ifdef PPCG4
	dcbt	X, PRE
#endif
	FMADD	f17,  ALPHA, f1, f9
	LFDUX	f1,    X, INCX
	LFDUX	f9,    Y, INCY
	FMADD	f18,  ALPHA, f2, f10
	LFDUX	f2,    X, INCX
	LFDUX	f10,   Y, INCY
#ifdef PPCG4
	dcbtst	Y, PRE
#endif
	FMADD	f19,  ALPHA, f3, f11
	LFDUX	f3,    X, INCX
	LFDUX	f11,   Y, INCY

	STFDUX	f20,   YY, INCY
	STFDUX	f21,   YY, INCY
	STFDUX	f22,   YY, INCY
	STFDUX	f23,   YY, INCY

	FMADD	f20,  ALPHA, f4, f12
	LFDUX	f4,    X, INCX
	LFDUX	f12,   Y, INCY
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif
	FMADD	f21,  ALPHA, f5, f13
	LFDUX	f5,    X, INCX
	LFDUX	f13,   Y, INCY
	FMADD	f22,  ALPHA, f6, f14
	LFDUX	f6,    X, INCX
	LFDUX	f14,   Y, INCY
#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y, PRE
#endif
	FMADD	f23,  ALPHA, f7, f15
	LFDUX	f7,    X, INCX
	LFDUX	f15,   Y, INCY

	STFDUX	f16,   YY, INCY
	STFDUX	f17,   YY, INCY
	STFDUX	f18,   YY, INCY
	STFDUX	f19,   YY, INCY

	STFDUX	f20,   YY, INCY
	STFDUX	f21,   YY, INCY
	STFDUX	f22,   YY, INCY
	STFDUX	f23,   YY, INCY
	bdnz	LL(110)
	.align 4

LL(120):
	FMADD	f16,  ALPHA, f0, f8
	LFDUX	f0,    X, INCX
	LFDUX	f8,    Y, INCY
	FMADD	f17,  ALPHA, f1, f9
	LFDUX	f1,    X, INCX
	LFDUX	f9,    Y, INCY
	FMADD	f18,  ALPHA, f2, f10
	LFDUX	f2,    X, INCX
	LFDUX	f10,   Y, INCY
	FMADD	f19,  ALPHA, f3, f11
	LFDUX	f3,    X, INCX
	LFDUX	f11,   Y, INCY

	FMADD	f20,  ALPHA, f4, f12
	LFDUX	f4,    X, INCX
	LFDUX	f12,   Y, INCY
	FMADD	f21,  ALPHA, f5, f13
	LFDUX	f5,    X, INCX
	LFDUX	f13,   Y, INCY
	FMADD	f22,  ALPHA, f6, f14
	LFDUX	f6,    X, INCX
	LFDUX	f14,   Y, INCY
	FMADD	f23,  ALPHA, f7, f15
	LFDUX	f7,    X, INCX
	LFDUX	f15,   Y, INCY

	STFDUX	f16,   YY, INCY
	STFDUX	f17,   YY, INCY
	STFDUX	f18,   YY, INCY
	STFDUX	f19,   YY, INCY

	FMADD	f16,  ALPHA, f0, f8
	STFDUX	f20,   YY, INCY
	FMADD	f17,  ALPHA, f1, f9
	STFDUX	f21,   YY, INCY
	FMADD	f18,  ALPHA, f2, f10
	STFDUX	f22,   YY, INCY
	FMADD	f19,  ALPHA, f3, f11
	STFDUX	f23,   YY, INCY

	FMADD	f20,  ALPHA, f4, f12
	STFDUX	f16,   YY, INCY
	FMADD	f21,  ALPHA, f5, f13
	STFDUX	f17,   YY, INCY
	FMADD	f22,  ALPHA, f6, f14
	STFDUX	f18,   YY, INCY
	FMADD	f23,  ALPHA, f7, f15
	STFDUX	f19,   YY, INCY

	STFDUX	f20,   YY, INCY
	STFDUX	f21,   YY, INCY
	STFDUX	f22,   YY, INCY
	STFDUX	f23,   YY, INCY
	.align 4

LL(150):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f0,   X, INCX
	LFDUX	f8,   Y, INCY

	FMADD	f16,  ALPHA, f0, f8

	STFDUX	f16,  YY, INCY
	bdnz	LL(160)
	.align 4

LL(999):
	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	lfd	f18,   32(SP)
	lfd	f19,   40(SP)
	lfd	f20,   48(SP)
	lfd	f21,   56(SP)

	lfd	f22,   64(SP)
	lfd	f23,   72(SP)
	lfd	f24,   80(SP)
	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
