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
#define PRE	r5

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
	li	PRE, 3 * 16 * SIZE

	cmpwi	cr0, N, 0
	blelr-	cr0

	sub	X, X, INCX

	fcmpu	cr0, FZERO, ALPHA
	bne-	cr0, LL(A1I1)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(A0I1_Remain)
	.align 4

LL(A0I1_kernel):
#ifdef PPCG4
	dcbtst	X, PRE
#endif

	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	X, PRE
#endif

	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX

#ifdef PPCG4
	dcbtst	X, PRE
#endif

	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	X, PRE
#endif

	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	STFDUX	FZERO, X, INCX
	bdnz	LL(A0I1_kernel)
	.align 4

LL(A0I1_Remain):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A0I1_RemainKernel):
	STFDUX	FZERO,  X, INCX
	bdnz	LL(A0I1_RemainKernel)
	blr
	.align 4

LL(A1I1):
	mr	XX, X

	srawi.	r0, N, 3
	mtspr	CTR, r0
	beq+	LL(A1I1_Remain)

	LFDUX	f2, X, INCX
	LFDUX	f3, X, INCX
	LFDUX	f4, X, INCX
	LFDUX	f5, X, INCX
	bdz	LL(12)
	.align 4

LL(11):
	LFDUX	f6, X, INCX
	FMUL	f2, ALPHA, f2
	LFDUX	f7, X, INCX
	FMUL	f3, ALPHA, f3
	LFDUX	f8, X, INCX
	FMUL	f4, ALPHA, f4
	LFDUX	f9, X, INCX
	FMUL	f5, ALPHA, f5

#ifdef PPCG4
	dcbtst	X, PRE
#endif
	STFDUX	f2, XX, INCX
	STFDUX	f3, XX, INCX
	STFDUX	f4, XX, INCX
	STFDUX	f5, XX, INCX

	LFDUX	f2, X, INCX
	FMUL	f6, ALPHA, f6
	LFDUX	f3, X, INCX
	FMUL	f7, ALPHA, f7
	LFDUX	f4, X, INCX
	FMUL	f8, ALPHA, f8
	LFDUX	f5, X, INCX
	FMUL	f9, ALPHA, f9

	STFDUX	f6, XX, INCX
	STFDUX	f7, XX, INCX
	STFDUX	f8, XX, INCX
	STFDUX	f9, XX, INCX

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	X, PRE
#endif

	bdnz	LL(11)
	.align 4

LL(12):
	LFDUX	f6, X, INCX
	FMUL	f2, ALPHA, f2
	LFDUX	f7, X, INCX
	FMUL	f3, ALPHA, f3
	LFDUX	f8, X, INCX
	FMUL	f4, ALPHA, f4
	LFDUX	f9, X, INCX
	FMUL	f5, ALPHA, f5

	STFDUX	f2, XX, INCX
	FMUL	f6, ALPHA, f6
	STFDUX	f3, XX, INCX
	FMUL	f7, ALPHA, f7
	STFDUX	f4, XX, INCX
	FMUL	f8, ALPHA, f8
	STFDUX	f5, XX, INCX
	FMUL	f9, ALPHA, f9

	STFDUX	f6, XX, INCX
	STFDUX	f7, XX, INCX
	STFDUX	f8, XX, INCX
	STFDUX	f9, XX, INCX
	.align 4

LL(A1I1_Remain):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beqlr+
	.align 4

LL(A1I1_RemainKernel):
	LFDUX	f2,  X, INCX
	FMUL	f2, ALPHA, f2
	STFDUX	f2,  XX, INCX
	bdnz	LL(A1I1_RemainKernel)
	blr
	.align 4

	EPILOGUE
