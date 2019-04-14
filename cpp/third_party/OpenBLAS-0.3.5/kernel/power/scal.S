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
#define XX	r4
#define PREA	r5

#ifdef linux
#ifndef __64BIT__
#define X r6
#define INCX r7
#else
#define X r7
#define INCX r8
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define X r8
#define INCX r9
#else
#define X r7
#define INCX r8
#endif
#endif

#define FZERO	f0
#define ALPHA	f1

	PROLOGUE
	PROFCODE

	addi	SP, SP, -8
	li	r0,   0

	stw	r0,      0(SP)
	lfs	FZERO,   0(SP)

	addi	SP, SP,  8

	slwi	INCX, INCX, BASE_SHIFT

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	blelr-	cr0

	fcmpu	cr0, FZERO, ALPHA
	bne-	cr0, LL(A1I1)

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(A0IN)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(A0I1_Remain)
	.align 4

LL(A0I1_kernel):
	STFD	FZERO,  0 * SIZE(X)
	STFD	FZERO,  1 * SIZE(X)
	STFD	FZERO,  2 * SIZE(X)
	STFD	FZERO,  3 * SIZE(X)
	STFD	FZERO,  4 * SIZE(X)
	STFD	FZERO,  5 * SIZE(X)
	STFD	FZERO,  6 * SIZE(X)
	STFD	FZERO,  7 * SIZE(X)

	STFD	FZERO,  8 * SIZE(X)
	STFD	FZERO,  9 * SIZE(X)
	STFD	FZERO, 10 * SIZE(X)
	STFD	FZERO, 11 * SIZE(X)
	STFD	FZERO, 12 * SIZE(X)
	STFD	FZERO, 13 * SIZE(X)
	STFD	FZERO, 14 * SIZE(X)
	STFD	FZERO, 15 * SIZE(X)

	addi	X, X, 16 * SIZE
	bdnz	LL(A0I1_kernel)
	.align 4

LL(A0I1_Remain):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A0I1_RemainKernel):
	STFD	FZERO, 0 * SIZE(X)
	addi	X, X,  1 * SIZE
	bdnz	LL(A0I1_RemainKernel)
	blr
	.align 4

LL(A0IN):
	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(A0IN_Remain)
	.align 4

LL(A0IN_Kernel):
 	dcbtst	X, PREA
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	bdnz	LL(A0IN_Kernel)
	.align 4

LL(A0IN_Remain):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A0IN_RemainKernel):
	STFD	FZERO, 0 * SIZE(X)
	add	X, X, INCX
	bdnz	LL(A0IN_RemainKernel)
	blr
	.align 4

LL(A1I1):
	cmpwi	cr0, INCX, SIZE
	bne-	LL(A1IN)

	mr	XX, X

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq+	LL(A1I1_Remain)

	LFD	f2,  0 * SIZE(X)
	LFD	f3,  1 * SIZE(X)
	LFD	f4,  2 * SIZE(X)
	LFD	f5,  3 * SIZE(X)
	LFD	f6,  4 * SIZE(X)
	LFD	f7,  5 * SIZE(X)
	LFD	f8,  6 * SIZE(X)
	LFD	f9,  7 * SIZE(X)
	bdz	LL(13)
	.align 4

LL(A1I1_kernel):
	FMUL	f10, ALPHA, f2
	FMUL	f11, ALPHA, f3
	FMUL	f12, ALPHA, f4
	FMUL	f13, ALPHA, f5

	LFD	f2,  8 * SIZE(X)
	LFD	f3,  9 * SIZE(X)
	LFD	f4, 10 * SIZE(X)
	LFD	f5, 11 * SIZE(X)

	STFD	f10,  0 * SIZE(X)
	STFD	f11,  1 * SIZE(X)
	STFD	f12,  2 * SIZE(X)
	STFD	f13,  3 * SIZE(X)

	FMUL	f10, ALPHA, f6
	FMUL	f11, ALPHA, f7
	FMUL	f12, ALPHA, f8
	FMUL	f13, ALPHA, f9

	LFD	f6, 12 * SIZE(X)
	LFD	f7, 13 * SIZE(X)
	LFD	f8, 14 * SIZE(X)
	LFD	f9, 15 * SIZE(X)

	STFD	f10,  4 * SIZE(X)
	STFD	f11,  5 * SIZE(X)
	STFD	f12,  6 * SIZE(X)
	STFD	f13,  7 * SIZE(X)

	FMUL	f10, ALPHA, f2
	FMUL	f11, ALPHA, f3
	FMUL	f12, ALPHA, f4
	FMUL	f13, ALPHA, f5

	LFD	f2,  16 * SIZE(X)
	LFD	f3,  17 * SIZE(X)
	LFD	f4,  18 * SIZE(X)
	LFD	f5,  19 * SIZE(X)

	STFD	f10,  8 * SIZE(X)
	STFD	f11,  9 * SIZE(X)
	STFD	f12, 10 * SIZE(X)
	STFD	f13, 11 * SIZE(X)

	FMUL	f10, ALPHA, f6
	FMUL	f11, ALPHA, f7
	FMUL	f12, ALPHA, f8
	FMUL	f13, ALPHA, f9

	LFD	f6,  20 * SIZE(X)
	LFD	f7,  21 * SIZE(X)
	LFD	f8,  22 * SIZE(X)
	LFD	f9,  23 * SIZE(X)

	STFD	f10, 12 * SIZE(X)
	STFD	f11, 13 * SIZE(X)
	STFD	f12, 14 * SIZE(X)
	STFD	f13, 15 * SIZE(X)

	addi	X, X,  16 * SIZE
 	dcbtst	X, PREA
	bdnz	LL(A1I1_kernel)
	.align 4

LL(13):
	FMUL	f10, ALPHA, f2
	FMUL	f11, ALPHA, f3
	FMUL	f12, ALPHA, f4
	FMUL	f13, ALPHA, f5

	LFD	f2,  8 * SIZE(X)
	LFD	f3,  9 * SIZE(X)
	LFD	f4, 10 * SIZE(X)
	LFD	f5, 11 * SIZE(X)

	STFD	f10,  0 * SIZE(X)
	STFD	f11,  1 * SIZE(X)
	STFD	f12,  2 * SIZE(X)
	STFD	f13,  3 * SIZE(X)

	FMUL	f10, ALPHA, f6
	FMUL	f11, ALPHA, f7
	FMUL	f12, ALPHA, f8
	FMUL	f13, ALPHA, f9

	LFD	f6, 12 * SIZE(X)
	LFD	f7, 13 * SIZE(X)
	LFD	f8, 14 * SIZE(X)
	LFD	f9, 15 * SIZE(X)

	STFD	f10,  4 * SIZE(X)
	STFD	f11,  5 * SIZE(X)
	STFD	f12,  6 * SIZE(X)
	STFD	f13,  7 * SIZE(X)

	FMUL	f10, ALPHA, f2
	FMUL	f11, ALPHA, f3
	FMUL	f12, ALPHA, f4
	FMUL	f13, ALPHA, f5

	STFD	f10,  8 * SIZE(X)
	STFD	f11,  9 * SIZE(X)
	STFD	f12, 10 * SIZE(X)
	STFD	f13, 11 * SIZE(X)

	FMUL	f10, ALPHA, f6
	FMUL	f11, ALPHA, f7
	FMUL	f12, ALPHA, f8
	FMUL	f13, ALPHA, f9

	STFD	f10, 12 * SIZE(X)
	STFD	f11, 13 * SIZE(X)
	STFD	f12, 14 * SIZE(X)
	STFD	f13, 15 * SIZE(X)

	addi	X, X,  16 * SIZE
	.align 4

LL(A1I1_Remain):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A1I1_RemainKernel):
	LFD	f2,  0 * SIZE(X)
	FMUL	f2, ALPHA, f2
	STFD	f2,  0 * SIZE(X)
	addi	X, X,  1 * SIZE
	bdnz	LL(A1I1_RemainKernel)
	blr
	.align 4

LL(A1IN):
	mr	XX, X

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(A1IN_Remain)
	.align 4

LL(A1IN_Kernel):
	LFD	f2, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f3, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f4, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f5, 0 * SIZE(XX)
	add	XX, XX, INCX

	FMUL	f2, ALPHA, f2
	FMUL	f3, ALPHA, f3
	FMUL	f4, ALPHA, f4
	FMUL	f5, ALPHA, f5

	LFD	f6, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f7, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f8, 0 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f9, 0 * SIZE(XX)
	add	XX, XX, INCX

	FMUL	f6, ALPHA, f6
	FMUL	f7, ALPHA, f7
	FMUL	f8, ALPHA, f8
	FMUL	f9, ALPHA, f9

	STFD	f2, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f3, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f4, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f5, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f6, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f7, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f8, 0 * SIZE(X)
	add	X, X, INCX
	STFD	f9, 0 * SIZE(X)
	add	X, X, INCX
	bdnz	LL(A1IN_Kernel)
	.align 4

LL(A1IN_Remain):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A1IN_RemainKernel):
	LFD	f2, 0 * SIZE(XX)
	add	XX, XX, INCX
	FMUL	f2, ALPHA, f2
	STFD	f2, 0 * SIZE(X)
	add	X, X, INCX
	bdnz	LL(A1IN_RemainKernel)
	blr

	EPILOGUE
