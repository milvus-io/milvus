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

#define N	r3
#define X	r4
#define INCX	r5
#define Y	r6
#define INCY	r7
#define PRE	r8
#define	XX	r9
#define	YY	r10

#define C	f1
#define S	f2

#define STACKSIZE 32

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0,   0

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	li	PRE, 2 * 16 * SIZE

	cmpwi	cr0, N, 0
	ble-	LL(999)

	sub	X, X, INCX
	sub	Y, Y, INCY

	mr	XX, X
	mr	YY, Y

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(150)

	LFDUX	f0,    X,  INCX
	LFDUX	f3,    Y,  INCY
	LFDUX	f4,    X,  INCX

	FMUL	f10, C, f0
	LFDUX	f5,    Y,  INCY
	FMUL	f11, C, f3
	LFDUX	f6,    X,  INCX
	FMUL	f12, C, f4
	LFDUX	f7,    Y,  INCY
	FMUL	f13, C, f5
	LFDUX	f8,    X,  INCX

	FMADD	f10, S, f3, f10
	LFDUX	f9,    Y,  INCY
	FNMSUB	f11, S, f0, f11
	LFDUX	f0,    X,  INCX
	FMADD	f12, S, f5, f12
	LFDUX	f3,    Y,  INCY
	FNMSUB	f13, S, f4, f13
	LFDUX	f4,    X,  INCX

	bdz	LL(111)
	.align 4

LL(110):
	FMUL	f14, C, f6
	LFDUX	f5,    Y,  INCY
	FMUL	f15, C, f7
	STFDUX	f10,    XX, INCX
	FMUL	f16, C, f8
	STFDUX	f11,    YY, INCY
	FMUL	f17, C, f9
	STFDUX	f12,    XX, INCX

#ifdef PPCG4
	dcbtst	X, PRE
#endif

	FMADD	f14, S, f7, f14
	STFDUX	f13,    YY, INCY
	FNMSUB	f15, S, f6, f15
	LFDUX	f6,    X,  INCX
	FMADD	f16, S, f9, f16
	LFDUX	f7,    Y,  INCY
	FNMSUB	f17, S, f8, f17
	LFDUX	f8,    X,  INCX

	FMUL	f10, C, f0
	LFDUX	f9,    Y,  INCY
	FMUL	f11, C, f3
	STFDUX	f14,    XX, INCX
	FMUL	f12, C, f4
	STFDUX	f15,    YY, INCY
	FMUL	f13, C, f5
	STFDUX	f16,    XX, INCX

#ifdef PPCG4
	dcbtst	Y, PRE
#endif

	FMADD	f10, S, f3, f10
	STFDUX	f17,    YY, INCY
	FNMSUB	f11, S, f0, f11
	LFDUX	f0,    X,  INCX
	FMADD	f12, S, f5, f12
	LFDUX	f3,    Y,  INCY
	FNMSUB	f13, S, f4, f13
	LFDUX	f4,    X,  INCX

	FMUL	f14, C, f6
	LFDUX	f5,    Y,  INCY
	FMUL	f15, C, f7
	STFDUX	f10,    XX, INCX
	FMUL	f16, C, f8
	STFDUX	f11,    YY, INCY
	FMUL	f17, C, f9
	STFDUX	f12,    XX, INCX

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif

	FMADD	f14, S, f7, f14
	STFDUX	f13,    YY, INCY
	FNMSUB	f15, S, f6, f15
	LFDUX	f6,    X,  INCX
	FMADD	f16, S, f9, f16
	LFDUX	f7,    Y,  INCY
	FNMSUB	f17, S, f8, f17
	LFDUX	f8,    X,  INCX

	FMUL	f10, C, f0
	LFDUX	f9,    Y,  INCY
	FMUL	f11, C, f3
	STFDUX	f14,    XX, INCX
	FMUL	f12, C, f4
	STFDUX	f15,    YY, INCY
	FMUL	f13, C, f5
	STFDUX	f16,    XX, INCX

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y, PRE
#endif

	FMADD	f10, S, f3, f10
	STFDUX	f17,    YY, INCY
	FNMSUB	f11, S, f0, f11
	LFDUX	f0,    X,  INCX
	FMADD	f12, S, f5, f12
	LFDUX	f3,    Y,  INCY
	FNMSUB	f13, S, f4, f13
	LFDUX	f4,    X,  INCX

	bdnz	LL(110)
	.align 4

LL(111):
	FMUL	f14, C, f6
	LFDUX	f5,    Y,  INCY
	FMUL	f15, C, f7
	STFDUX	f10,    XX, INCX
	FMUL	f16, C, f8
	STFDUX	f11,    YY, INCY
	FMUL	f17, C, f9
	STFDUX	f12,    XX, INCX

	FMADD	f14, S, f7, f14
	STFDUX	f13,    YY, INCY
	FNMSUB	f15, S, f6, f15
	LFDUX	f6,    X,  INCX
	FMADD	f16, S, f9, f16
	LFDUX	f7,    Y,  INCY
	FNMSUB	f17, S, f8, f17
	LFDUX	f8,    X,  INCX

	FMUL	f10, C, f0
	LFDUX	f9,    Y,  INCY
	FMUL	f11, C, f3
	STFDUX	f14,    XX, INCX
	FMUL	f12, C, f4
	STFDUX	f15,    YY, INCY
	FMUL	f13, C, f5
	STFDUX	f16,    XX, INCX

	FMUL	f14, C, f6
	STFDUX	f17,    YY, INCY
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3, f10
	FNMSUB	f11, S, f0, f11
	FMADD	f12, S, f5, f12
	FNMSUB	f13, S, f4, f13

	FMADD	f14, S, f7, f14
	STFDUX	f10,    XX, INCX
	FNMSUB	f15, S, f6, f15
	STFDUX	f11,    YY, INCY
	FMADD	f16, S, f9, f16
	STFDUX	f12,    XX, INCX
	FNMSUB	f17, S, f8, f17
	STFDUX	f13,    YY, INCY

	STFDUX	f14,    XX, INCX
	STFDUX	f15,    YY, INCY
	STFDUX	f16,    XX, INCX
	STFDUX	f17,    YY, INCY
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f0,    X,  INCX
	LFDUX	f3,    Y,  INCY

	FMUL	f10, C, f0
	FMUL	f11, C, f3

	FMADD	f10, S, f3, f10
	FNMSUB	f11, S, f0, f11

	STFDUX	f10,    XX, INCX
	STFDUX	f11,    YY, INCY
	bdnz	LL(160)
	.align 4

LL(999):
	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
