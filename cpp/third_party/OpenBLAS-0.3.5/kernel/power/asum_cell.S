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

#define PREA	r8

#define FZERO	f0

#define STACKSIZE 16

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0,   0

	stw	r0,    0(SP)

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	lfs	FZERO, 0(SP)

	slwi	INCX, INCX, BASE_SHIFT
	fmr	f1,  FZERO
	li	PREA, 8 * 16 * SIZE
	fmr	f2,  FZERO

	cmpwi	cr0, N, 0
	fmr	f3,  FZERO
	ble-	LL(999)

	cmpwi	cr0, INCX, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(20)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(15)
	.align 4

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f4, f8
	LFD	f10,   2 * SIZE(X)
	fabs	f5, f9
	LFD	f11,   3 * SIZE(X)
	fabs	f6, f10
	LFD	f8,    4 * SIZE(X)
	fabs	f7, f11
	bdz	LL(13)
	.align 4

LL(12):
	FADD	f0, f0, f4
	dcbt	X, PREA
	fabs	f4, f8
	LFD	f9,    5 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,   6 * SIZE(X)

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFD	f11,   7 * SIZE(X)

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFD	f8,    8 * SIZE(X)

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,    9 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,  10 * SIZE(X)

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFD	f11,  11 * SIZE(X)

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFD	f8,   12 * SIZE(X)

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,   13 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,  14 * SIZE(X)

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFD	f11,  15 * SIZE(X)

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFD	f8,   16 * SIZE(X)

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,   17 * SIZE(X)

	FADD	f1, f1, f5
	addi	X, X, 16 * SIZE
	fabs	f5, f9
	LFD	f10,   2 * SIZE(X)

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFD	f11,   3 * SIZE(X)

	FADD	f3, f3, f7
	LFD	f8,    4 * SIZE(X)
	fabs	f7, f11
	bdnz	LL(12)
	.align 4

LL(13):
	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,    5 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,   6 * SIZE(X)

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFD	f11,   7 * SIZE(X)

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFD	f8,    8 * SIZE(X)

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,    9 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,  10 * SIZE(X)

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFD	f11,  11 * SIZE(X)

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFD	f8,   12 * SIZE(X)

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,   13 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,  14 * SIZE(X)

	FADD	f2, f2, f6
	addi	X, X, 16 * SIZE
	fabs	f6, f10
	LFD	f11,  -1 * SIZE(X)

	FADD	f3, f3, f7
	fabs	f7, f11

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7
	.align 4

LL(15):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(16)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f4, f8
	LFD	f10,   2 * SIZE(X)
	fabs	f5, f9
	LFD	f11,   3 * SIZE(X)
	fabs	f6, f10
	LFD	f8,    4 * SIZE(X)
	fabs	f7, f11

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFD	f9,    5 * SIZE(X)

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFD	f10,   6 * SIZE(X)

	FADD	f2, f2, f6
	addi	X, X, 8 * SIZE
	fabs	f6, f10
	LFD	f11,  -1 * SIZE(X)

	FADD	f3, f3, f7
	fabs	f7, f11

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f4, f8
	LFD	f10,   2 * SIZE(X)
	fabs	f5, f9
	LFD	f11,   3 * SIZE(X)
	fabs	f6, f10
	addi	X, X, 4 * SIZE
	fabs	f7, f11
	nop

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f4, f8
	fabs	f5, f9

	FADD	f0, f0, f4
	addi	X, X, 2 * SIZE
	FADD	f1, f1, f5
	nop
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)

	LFD	f8,    0 * SIZE(X)
	fabs	f4, f8
	FADD	f0, f0, f4
	b	LL(999)
	.align 4

LL(20):
	sub	X, X, INCX

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(25)
	.align 4

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f4, f8
	LFDUX	f10,   X, INCX
	fabs	f5, f9
	LFDUX	f11,   X, INCX
	fabs	f6, f10
	LFDUX	f8,    X, INCX
	fabs	f7, f11
	bdz	LL(23)
	.align 4

LL(22):
	FADD	f0, f0, f4
	dcbt	X, PREA
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFDUX	f8,    X, INCX

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFDUX	f8,    X, INCX

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFDUX	f8,    X, INCX

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	LFDUX	f8,    X, INCX
	fabs	f7, f11
	bdnz	LL(22)
	.align 4

LL(23):
	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFDUX	f8,    X, INCX

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	nop
	fabs	f7, f11
	LFDUX	f8,    X, INCX

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	nop
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	fabs	f7, f11

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7
	.align 4

LL(25):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(26)

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f4, f8
	LFDUX	f10,   X, INCX
	fabs	f5, f9
	LFDUX	f11,   X, INCX
	fabs	f6, f10
	LFDUX	f8,    X, INCX
	fabs	f7, f11

	FADD	f0, f0, f4
	nop
	fabs	f4, f8
	LFDUX	f9,    X, INCX

	FADD	f1, f1, f5
	nop
	fabs	f5, f9
	LFDUX	f10,   X, INCX

	FADD	f2, f2, f6
	fabs	f6, f10
	LFDUX	f11,   X, INCX

	FADD	f3, f3, f7
	fabs	f7, f11

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7
	.align 4

LL(26):
	andi.	r0,  N, 4
	beq	LL(27)

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f4, f8
	LFDUX	f10,   X, INCX
	fabs	f5, f9
	LFDUX	f11,   X, INCX

	fabs	f6, f10
	fabs	f7, f11

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7
	.align 4

LL(27):
	andi.	r0,  N, 2
	beq	LL(28)

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f4, f8
	fabs	f5, f9

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	.align 4

LL(28):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	f8,    X, INCX
	fabs	f4, f8
	FADD	f0, f0, f4
	.align 4

LL(999):
	FADD	f0,  f0,  f1
	FADD	f2,  f2,  f3

	FADD	f1,  f0,  f2

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
