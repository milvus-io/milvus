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

#define N	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */
#define Y	ARG4	/* rcx */
#ifndef WINDOWS_ABI
#define INCY	ARG5	/* r8  */
#else
#define INCY	%r10
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	pxor	%xmm0, %xmm0
	salq	$ZBASE_SHIFT, INCY
	pxor	%xmm1, %xmm1

	pxor	%xmm2, %xmm2
	cmpq	$0, N
	pxor	%xmm3, %xmm3
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L20
	cmpq	$2 * SIZE, INCY
	jne	.L20

	movq	N,  %rax
	sarq	$2, %rax
	jle	.L15

	movsd	 0 * SIZE(X), %xmm4
	movsd	 0 * SIZE(Y), %xmm6
	movsd	 1 * SIZE(X), %xmm5
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	movsd	 2 * SIZE(X), %xmm10
	mulsd	 %xmm7, %xmm8
	movsd	 2 * SIZE(Y), %xmm11
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	movsd	 3 * SIZE(X), %xmm12
	mulsd	 %xmm6, %xmm9
	movsd	 3 * SIZE(Y), %xmm13

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	addsd	 %xmm4, %xmm0
	movaps	 %xmm10, %xmm14
	mulsd	 %xmm11, %xmm10
	movsd	 4 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	mulsd	 %xmm13, %xmm14
	movsd	 4 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movaps	 %xmm12, %xmm15
	mulsd	 %xmm13, %xmm12
	movsd	 5 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	mulsd	 %xmm11, %xmm15
	movsd	 5 * SIZE(Y), %xmm7

	addsd	 %xmm10, %xmm0
	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	movsd	 6 * SIZE(X), %xmm10
	addsd	 %xmm14, %xmm1
	mulsd	 %xmm7, %xmm8
	movsd	 6 * SIZE(Y), %xmm11
	addsd	 %xmm12, %xmm2
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	movsd	 7 * SIZE(X), %xmm12
	addsd	 %xmm15, %xmm3
	mulsd	 %xmm6, %xmm9
	movsd	 7 * SIZE(Y), %xmm13

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	addsd	 %xmm4, %xmm0
	movaps	 %xmm10, %xmm14
	mulsd	 %xmm11, %xmm10
	movsd	 8 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	mulsd	 %xmm13, %xmm14
	movsd	 8 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movaps	 %xmm12, %xmm15
	mulsd	 %xmm13, %xmm12
	movsd	 9 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	mulsd	 %xmm11, %xmm15
	movsd	 9 * SIZE(Y), %xmm7

	addsd	 %xmm10, %xmm0
	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	movsd	10 * SIZE(X), %xmm10
	addsd	 %xmm14, %xmm1
	mulsd	 %xmm7, %xmm8
	movsd	10 * SIZE(Y), %xmm11
	addsd	 %xmm12, %xmm2
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	movsd	11 * SIZE(X), %xmm12
	addsd	 %xmm15, %xmm3
	mulsd	 %xmm6, %xmm9
	movsd	11 * SIZE(Y), %xmm13

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y

	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	addsd	 %xmm4, %xmm0
	movaps	 %xmm10, %xmm14
	mulsd	 %xmm11, %xmm10
	movsd	 4 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	mulsd	 %xmm13, %xmm14
	movsd	 4 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movaps	 %xmm12, %xmm15
	mulsd	 %xmm13, %xmm12
	movsd	 5 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	mulsd	 %xmm11, %xmm15
	movsd	 5 * SIZE(Y), %xmm7

	addsd	 %xmm10, %xmm0
	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	movsd	 6 * SIZE(X), %xmm10
	addsd	 %xmm14, %xmm1
	mulsd	 %xmm7, %xmm8
	movsd	 6 * SIZE(Y), %xmm11
	addsd	 %xmm12, %xmm2
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	movsd	 7 * SIZE(X), %xmm12
	addsd	 %xmm15, %xmm3
	mulsd	 %xmm6, %xmm9
	movsd	 7 * SIZE(Y), %xmm13

	addsd	 %xmm4, %xmm0
	movaps	 %xmm10, %xmm14
	mulsd	 %xmm11, %xmm10
	addsd	 %xmm8, %xmm1
	mulsd	 %xmm13, %xmm14
	addsd	 %xmm5, %xmm2
	movaps	 %xmm12, %xmm15
	mulsd	 %xmm13, %xmm12
	addsd	 %xmm9, %xmm3
	mulsd	 %xmm11, %xmm15

	addsd	 %xmm10, %xmm0
	addsd	 %xmm14, %xmm1
	addsd	 %xmm12, %xmm2
	addsd	 %xmm15, %xmm3

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L15:
	movq	N,  %rax
	andq	$2, %rax
	jle	.L17

	movsd	 0 * SIZE(X), %xmm4
	movsd	 0 * SIZE(Y), %xmm6
	movsd	 1 * SIZE(X), %xmm5
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	movsd	 2 * SIZE(X), %xmm10
	mulsd	 %xmm7, %xmm8
	movsd	 2 * SIZE(Y), %xmm11
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	movsd	 3 * SIZE(X), %xmm12
	mulsd	 %xmm6, %xmm9
	movsd	 3 * SIZE(Y), %xmm13

	addsd	 %xmm4, %xmm0
	movaps	 %xmm10, %xmm14
	mulsd	 %xmm11, %xmm10
	addsd	 %xmm8, %xmm1
	mulsd	 %xmm13, %xmm14
	addsd	 %xmm5, %xmm2
	movaps	 %xmm12, %xmm15
	mulsd	 %xmm13, %xmm12
	addsd	 %xmm9, %xmm3
	mulsd	 %xmm11, %xmm15

	addsd	 %xmm10, %xmm0
	addsd	 %xmm14, %xmm1
	addsd	 %xmm12, %xmm2
	addsd	 %xmm15, %xmm3

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L17:
	movq	N,  %rax
	andq	$1, %rax
	jle	.L999

	movsd	 0 * SIZE(X), %xmm4
	movsd	 0 * SIZE(Y), %xmm6
	movsd	 1 * SIZE(X), %xmm5
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm6, %xmm9

	addsd	 %xmm4, %xmm0
	addsd	 %xmm8, %xmm1
	addsd	 %xmm5, %xmm2
	addsd	 %xmm9, %xmm3
	jmp	 .L999
	ALIGN_3

.L20:
	movq	N,  %rax
	sarq	$2, %rax
	jle	.L25
	ALIGN_3

.L23:
	movsd	 0 * SIZE(X), %xmm4
	movsd	 0 * SIZE(Y), %xmm6
	movsd	 1 * SIZE(X), %xmm5
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	addq	 INCX, X
	mulsd	 %xmm6, %xmm9
	addq	 INCY, Y

	addsd	 %xmm4, %xmm0
	movsd	 0 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	movsd	 0 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movsd	 1 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	addq	 INCX, X
	mulsd	 %xmm6, %xmm9
	addq	 INCY, Y

	addsd	 %xmm4, %xmm0
	movsd	 0 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	movsd	 0 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movsd	 1 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	addq	 INCX, X
	mulsd	 %xmm6, %xmm9
	addq	 INCY, Y

	addsd	 %xmm4, %xmm0
	movsd	 0 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	movsd	 0 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movsd	 1 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	addq	 INCX, X
	mulsd	 %xmm6, %xmm9
	addq	 INCY, Y

	addsd	 %xmm4, %xmm0
	addsd	 %xmm8, %xmm1
	addsd	 %xmm5, %xmm2
	addsd	 %xmm9, %xmm3

	decq	%rax
	jg	.L23
	ALIGN_3

.L25:
	testq	$3, N
	je	.L999

	movq	N,  %rax
	andq	$2, %rax
	jle	.L27

	movsd	 0 * SIZE(X), %xmm4
	movsd	 0 * SIZE(Y), %xmm6
	movsd	 1 * SIZE(X), %xmm5
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	addq	 INCX, X
	mulsd	 %xmm6, %xmm9
	addq	 INCY, Y

	addsd	 %xmm4, %xmm0
	movsd	 0 * SIZE(X), %xmm4
	addsd	 %xmm8, %xmm1
	movsd	 0 * SIZE(Y), %xmm6
	addsd	 %xmm5, %xmm2
	movsd	 1 * SIZE(X), %xmm5
	addsd	 %xmm9, %xmm3
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	addq	 INCX, X
	mulsd	 %xmm6, %xmm9
	addq	 INCY, Y

	addsd	 %xmm4, %xmm0
	addsd	 %xmm8, %xmm1
	addsd	 %xmm5, %xmm2
	addsd	 %xmm9, %xmm3

	ALIGN_3

.L27:
	movq	N,  %rax
	andq	$1, %rax
	jle	.L999

	movsd	 0 * SIZE(X), %xmm4
	movsd	 0 * SIZE(Y), %xmm6
	movsd	 1 * SIZE(X), %xmm5
	movsd	 1 * SIZE(Y), %xmm7

	movaps	 %xmm4, %xmm8
	mulsd	 %xmm6, %xmm4
	mulsd	 %xmm7, %xmm8
	movaps	 %xmm5, %xmm9
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm6, %xmm9

	addsd	 %xmm4, %xmm0
	addsd	 %xmm8, %xmm1
	addsd	 %xmm5, %xmm2
	addsd	 %xmm9, %xmm3
	ALIGN_3

.L999:
#ifndef CONJ
	subsd	 %xmm2, %xmm0
	addsd	 %xmm3, %xmm1
#else
	addsd	 %xmm2, %xmm0
	subsd	 %xmm3, %xmm1
#endif

	RESTOREREGISTERS

	ret

	EPILOGUE
