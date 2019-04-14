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

#define M	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */
#define Y	ARG4	/* rcx */
#ifndef WINDOWS_ABI
#define INCY	ARG5	/* r8  */
#else
#define INCY	%r10
#endif

#include "l1param.h"

#define VLOAD(OFFSET, ADDR, REG)		vmovups	OFFSET(ADDR), REG
#define VSHUFPD_1(REG1 , REG2)			vshufpd $0x01, REG1, REG2, REG2
#define A_PRE 640
#define B_PRE 640

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), INCY
#endif

	SAVEREGISTERS

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	cmpq	$SIZE, INCX
	jne	.L40
	cmpq	$SIZE, INCY
	jne	.L40

	testq	$SIZE, X
	je	.L10

	vmovsd	(X), %xmm0
	vmovsd	%xmm0, (Y)
	addq	$1 * SIZE, X
	addq	$1 * SIZE, Y
	decq	M
	jle	.L19
	ALIGN_4

.L10:
	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y


	movq	M,  %rax
	sarq	$4, %rax
	jle	.L13

	vmovups	-16 * SIZE(X), %xmm0
	vmovups	-14 * SIZE(X), %xmm1
	vmovups	-12 * SIZE(X), %xmm2
	vmovups	-10 * SIZE(X), %xmm3
	vmovups	 -8 * SIZE(X), %xmm4
	vmovups	 -6 * SIZE(X), %xmm5
	vmovups	 -4 * SIZE(X), %xmm6
	vmovups	 -2 * SIZE(X), %xmm7

	decq	%rax
	jle .L12
	ALIGN_4

.L11:

	prefetchnta	A_PRE(X)
	nop
	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)
	prefetchnta	B_PRE(Y)
	nop
	vmovups	%xmm2, -12 * SIZE(Y)
	vmovups	%xmm3, -10 * SIZE(Y)

	VLOAD( 0 * SIZE, X, %xmm0)
	VLOAD( 2 * SIZE, X, %xmm1)
	VLOAD( 4 * SIZE, X, %xmm2)
	VLOAD( 6 * SIZE, X, %xmm3)

	prefetchnta	A_PRE+64(X)
	nop
	vmovups	%xmm4, -8 * SIZE(Y)
	vmovups	%xmm5, -6 * SIZE(Y)
	prefetchnta	B_PRE+64(Y)
	nop
	vmovups	%xmm6, -4 * SIZE(Y)
	vmovups	%xmm7, -2 * SIZE(Y)

	VLOAD( 8 * SIZE, X, %xmm4)
	VLOAD(10 * SIZE, X, %xmm5)
	subq	$-16 * SIZE, Y
	VLOAD(12 * SIZE, X, %xmm6)
	VLOAD(14 * SIZE, X, %xmm7)

	subq	$-16 * SIZE, X
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)
	vmovups	%xmm2, -12 * SIZE(Y)
	vmovups	%xmm3, -10 * SIZE(Y)
	vmovups	%xmm4,  -8 * SIZE(Y)
	vmovups	%xmm5,  -6 * SIZE(Y)
	vmovups	%xmm6,  -4 * SIZE(Y)
	vmovups	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	ALIGN_3

.L13:
	testq	$8, M
	jle	.L14
	ALIGN_3

	vmovups	-16 * SIZE(X), %xmm0
	vmovups	-14 * SIZE(X), %xmm1
	vmovups	-12 * SIZE(X), %xmm2
	vmovups	-10 * SIZE(X), %xmm3

	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)
	vmovups	%xmm2, -12 * SIZE(Y)
	vmovups	%xmm3, -10 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L14:
	testq	$4, M
	jle	.L15
	ALIGN_3

	vmovups	-16 * SIZE(X), %xmm0
	vmovups	-14 * SIZE(X), %xmm1

	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L15:
	testq	$2, M
	jle	.L16
	ALIGN_3

	vmovups	-16 * SIZE(X), %xmm0
	vmovups	%xmm0, -16 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L16:
	testq	$1, M
	jle	.L19
	ALIGN_3

	vmovsd	-16 * SIZE(X), %xmm0
	vmovsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3



.L40:
	movq	M,  %rax
	sarq	$3, %rax
	jle	.L45
	ALIGN_3

.L41:
	vmovsd	(X), %xmm0
	addq	INCX, X
	vmovsd	(X), %xmm4
	addq	INCX, X
	vmovsd	(X), %xmm1
	addq	INCX, X
	vmovsd	(X), %xmm5
	addq	INCX, X
	vmovsd	(X), %xmm2
	addq	INCX, X
	vmovsd	(X), %xmm6
	addq	INCX, X
	vmovsd	(X), %xmm3
	addq	INCX, X
	vmovsd	(X), %xmm7
	addq	INCX, X

	vmovsd	%xmm0, (Y)
	addq	INCY, Y
	vmovsd	%xmm4, (Y)
	addq	INCY, Y
	vmovsd	%xmm1, (Y)
	addq	INCY, Y
	vmovsd	%xmm5, (Y)
	addq	INCY, Y
	vmovsd	%xmm2, (Y)
	addq	INCY, Y
	vmovsd	%xmm6, (Y)
	addq	INCY, Y
	vmovsd	%xmm3, (Y)
	addq	INCY, Y
	vmovsd	%xmm7, (Y)
	addq	INCY, Y

	decq	%rax
	jg	.L41
	ALIGN_3

.L45:
	movq	M,  %rax
	andq	$7, %rax
	jle	.L47
	ALIGN_3

.L46:
	vmovsd	(X), %xmm0
	addq	INCX, X
	vmovsd	%xmm0, (Y)
	addq	INCY, Y
	decq	%rax
	jg	.L46
	ALIGN_3

.L47:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
