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
#define X r8
#define INCX r9
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define X r10
#define INCX r8
#else
#define X r8
#define INCX r9
#endif
#endif

#define FZERO	f0
#define ALPHA_R	f1
#define ALPHA_I	f2

	PROLOGUE
	PROFCODE

	addi	SP, SP, -8
	li	r0,   0

	stw	r0,      0(SP)
	lfs	FZERO,   0(SP)
	addi	SP, SP,  8

#if (defined(_AIX) || defined(__APPLE__)) && !defined(__64BIT__) && defined(DOUBLE)
	lwz	INCX,    56(SP)
#endif

	slwi	INCX, INCX, ZBASE_SHIFT

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	blelr-	cr0

	fcmpu	cr0, FZERO, ALPHA_R
	bne-	cr0, LL(A1I1)

	fcmpu	cr0, FZERO, ALPHA_I
	bne-	cr0, LL(A1I1)

	cmpwi	cr0, INCX, 2 * SIZE
	bne-	cr0, LL(A0IN)

	srawi.	r0, N, 3
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
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A0I1_RemainKernel):
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	addi	X, X,  2 * SIZE
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
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	STFD	FZERO, 0 * SIZE(X)
	STFD	FZERO, 1 * SIZE(X)
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
	STFD	FZERO, 1 * SIZE(X)
	add	X, X, INCX
	bdnz	LL(A0IN_RemainKernel)
	blr
	.align 4

LL(A1I1):
	cmpwi	cr0, INCX, 2 * SIZE
	bne-	LL(A1IN)

	mr	XX, X
	srawi.	r0, N, 3
	mtspr	CTR, r0
	beq+	LL(A1I1_Remain)
	.align 4

LL(A1I1_kernel):
	LFD	f3,  0 * SIZE(X)
	LFD	f4,  1 * SIZE(X)
	LFD	f5,  2 * SIZE(X)
	LFD	f6,  3 * SIZE(X)
	LFD	f7,  4 * SIZE(X)
	LFD	f8,  5 * SIZE(X)
	LFD	f9,  6 * SIZE(X)
	LFD	f10, 7 * SIZE(X)

	FMUL	f0,  ALPHA_I, f4
	FMUL	f4,  ALPHA_R, f4
	FMUL	f11, ALPHA_I, f6
	FMUL	f6,  ALPHA_R, f6

	FMUL	f12, ALPHA_I, f8
	FMUL	f8,  ALPHA_R, f8
	FMUL	f13, ALPHA_I, f10
	FMUL	f10, ALPHA_R, f10

	FMADD	f4,  ALPHA_I, f3, f4
	FMSUB	f3,  ALPHA_R, f3, f0
	FMADD	f6,  ALPHA_I, f5, f6
	FMSUB	f5,  ALPHA_R, f5, f11

	FMADD	f8,  ALPHA_I, f7, f8
	FMSUB	f7,  ALPHA_R, f7, f12
	FMADD	f10, ALPHA_I, f9, f10
	FMSUB	f9,  ALPHA_R, f9, f13

	STFD	f3,  0 * SIZE(X)
	STFD	f4,  1 * SIZE(X)
	STFD	f5,  2 * SIZE(X)
	STFD	f6,  3 * SIZE(X)
	STFD	f7,  4 * SIZE(X)
	STFD	f8,  5 * SIZE(X)
	STFD	f9,  6 * SIZE(X)
	STFD	f10, 7 * SIZE(X)

	LFD	f3,  8 * SIZE(X)
	LFD	f4,  9 * SIZE(X)
	LFD	f5, 10 * SIZE(X)
	LFD	f6, 11 * SIZE(X)
	LFD	f7, 12 * SIZE(X)
	LFD	f8, 13 * SIZE(X)
	LFD	f9, 14 * SIZE(X)
	LFD	f10,15 * SIZE(X)

	FMUL	f0,  ALPHA_I, f4
	FMUL	f4,  ALPHA_R, f4
	FMUL	f11, ALPHA_I, f6
	FMUL	f6,  ALPHA_R, f6

	FMUL	f12, ALPHA_I, f8
	FMUL	f8,  ALPHA_R, f8
	FMUL	f13, ALPHA_I, f10
	FMUL	f10, ALPHA_R, f10

	FMADD	f4,  ALPHA_I, f3, f4
	FMSUB	f3,  ALPHA_R, f3, f0
	FMADD	f6,  ALPHA_I, f5, f6
	FMSUB	f5,  ALPHA_R, f5, f11

	FMADD	f8,  ALPHA_I, f7, f8
	FMSUB	f7,  ALPHA_R, f7, f12
	FMADD	f10, ALPHA_I, f9, f10
	FMSUB	f9,  ALPHA_R, f9, f13

	STFD	f3,  8 * SIZE(X)
	STFD	f4,  9 * SIZE(X)
	STFD	f5, 10 * SIZE(X)
	STFD	f6, 11 * SIZE(X)
	STFD	f7, 12 * SIZE(X)
	STFD	f8, 13 * SIZE(X)
	STFD	f9, 14 * SIZE(X)
	STFD	f10,15 * SIZE(X)

	addi	X, X, 16 * SIZE
 	dcbtst	X, PREA
	bdnz	LL(A1I1_kernel)
	.align 4

LL(A1I1_Remain):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A1I1_RemainKernel):
	LFD	f3,  0 * SIZE(X)
	LFD	f4,  1 * SIZE(X)

	FMUL	f5, ALPHA_I, f4
	FMUL	f4, ALPHA_R, f4
	FMADD	f4, ALPHA_I, f3, f4
	FMSUB	f3, ALPHA_R, f3, f5

	STFD	f3,  0 * SIZE(X)
	STFD	f4,  1 * SIZE(X)
	addi	X, X,  2 * SIZE
	bdnz	LL(A1I1_RemainKernel)
	blr
	.align 4

LL(A1IN):
	mr	XX, X

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(A1IN_Remain)
	.align 4

LL(A1IN_Kernel):
	LFD	f3,  0 * SIZE(XX)
	LFD	f4,  1 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f5,  0 * SIZE(XX)
	LFD	f6,  1 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f7,  0 * SIZE(XX)
	LFD	f8,  1 * SIZE(XX)
	add	XX, XX, INCX
	LFD	f9,  0 * SIZE(XX)
	LFD	f10, 1 * SIZE(XX)
	add	XX, XX, INCX

	FMUL	f0,  ALPHA_I, f4
	FMUL	f4,  ALPHA_R, f4
	FMUL	f11, ALPHA_I, f6
	FMUL	f6,  ALPHA_R, f6

	FMUL	f12, ALPHA_I, f8
	FMUL	f8,  ALPHA_R, f8
	FMUL	f13, ALPHA_I, f10
	FMUL	f10, ALPHA_R, f10

	FMADD	f4,  ALPHA_I, f3, f4
	FMSUB	f3,  ALPHA_R, f3, f0
	FMADD	f6,  ALPHA_I, f5, f6
	FMSUB	f5,  ALPHA_R, f5, f11

	FMADD	f8,  ALPHA_I, f7, f8
	FMSUB	f7,  ALPHA_R, f7, f12
	FMADD	f10, ALPHA_I, f9, f10
	FMSUB	f9,  ALPHA_R, f9, f13

	STFD	f3,  0 * SIZE(X)
	STFD	f4,  1 * SIZE(X)
	add	X, X, INCX
	STFD	f5,  0 * SIZE(X)
	STFD	f6,  1 * SIZE(X)
	add	X, X, INCX
	STFD	f7,  0 * SIZE(X)
	STFD	f8,  1 * SIZE(X)
	add	X, X, INCX
	STFD	f9,  0 * SIZE(X)
	STFD	f10, 1 * SIZE(X)
	add	X, X, INCX
 	dcbtst	X, PREA
	bdnz	LL(A1IN_Kernel)
	.align 4

LL(A1IN_Remain):
	andi.	r0,  N, 3
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A1IN_RemainKernel):
	LFD	f3, 0 * SIZE(XX)
	LFD	f4, 1 * SIZE(XX)
	add	XX, XX, INCX

	FMUL	f5, ALPHA_I, f4
	FMUL	f4, ALPHA_R, f4
	FMADD	f4, ALPHA_I, f3, f4
	FMSUB	f3, ALPHA_R, f3, f5

	STFD	f3, 0 * SIZE(X)
	STFD	f4, 1 * SIZE(X)
	add	X, X, INCX
	bdnz	LL(A1IN_RemainKernel)
	blr

	EPILOGUE
