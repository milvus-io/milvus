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

#ifndef WINDOWS_ABI
#define M	ARG1
#define X	ARG4
#define INCX	ARG5
#define Y	ARG6
#define INCY	ARG2
#else
#define M	ARG1
#define X	ARG2
#define INCX	ARG3
#define Y	ARG4
#define INCY	%r10
#endif

#define	YY	%r11
#define ALPHA	%xmm15

#define A_PRE 640

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifndef WINDOWS_ABI
#ifndef XDOUBLE
	movq	 8(%rsp), INCY
#else
	movq	24(%rsp), INCY
#endif
	vmovups	%xmm0,  ALPHA
#else
	vmovups	%xmm3,  ALPHA

	movq	40(%rsp), X
	movq	48(%rsp), INCX
	movq	56(%rsp), Y
	movq	64(%rsp), INCY
#endif

	SAVEREGISTERS

	unpcklpd ALPHA, ALPHA

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	testq	M, M
	jle	.L47

	cmpq	$SIZE, INCX
	jne	.L40
	cmpq	$SIZE, INCY
	jne	.L40

	testq	$SIZE, Y
	je	.L10

	movsd	(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	(Y), %xmm0
	movsd	%xmm0, (Y)
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

	decq	%rax
	jle .L12
	ALIGN_3

.L11:

	prefetchnta	A_PRE(Y)

	vmovups	 -8 * SIZE(X), %xmm4
	vfmaddpd -16 * SIZE(Y), ALPHA, %xmm0 , %xmm0
	vfmaddpd -14 * SIZE(Y), ALPHA, %xmm1 , %xmm1
	vmovups	 -6 * SIZE(X), %xmm5
	vmovups	 -4 * SIZE(X), %xmm6
	vfmaddpd -12 * SIZE(Y), ALPHA, %xmm2 , %xmm2
	vfmaddpd -10 * SIZE(Y), ALPHA, %xmm3 , %xmm3
	vmovups	 -2 * SIZE(X), %xmm7


	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)
	prefetchnta	A_PRE(X)
	nop
	vmovups	%xmm2, -12 * SIZE(Y)
	vmovups	%xmm3, -10 * SIZE(Y)

	prefetchnta	A_PRE+64(Y)

	vmovups	 0 * SIZE(X), %xmm0
	vfmaddpd  -8 * SIZE(Y), ALPHA, %xmm4 , %xmm4
	vfmaddpd  -6 * SIZE(Y), ALPHA, %xmm5 , %xmm5
	vmovups	 2 * SIZE(X), %xmm1
	vmovups	 4 * SIZE(X), %xmm2
	vfmaddpd  -4 * SIZE(Y), ALPHA, %xmm6 , %xmm6
	vfmaddpd  -2 * SIZE(Y), ALPHA, %xmm7 , %xmm7
	vmovups	 6 * SIZE(X), %xmm3


	vmovups	%xmm4, -8 * SIZE(Y)
	vmovups	%xmm5, -6 * SIZE(Y)
	prefetchnta	A_PRE+64(X)
	nop
	vmovups	%xmm6, -4 * SIZE(Y)
	vmovups	%xmm7, -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:

	vmovups	 -8 * SIZE(X), %xmm4
	vfmaddpd -16 * SIZE(Y), ALPHA, %xmm0 , %xmm0
	vfmaddpd -14 * SIZE(Y), ALPHA, %xmm1 , %xmm1
	vmovups	 -6 * SIZE(X), %xmm5
	vmovups	 -4 * SIZE(X), %xmm6
	vfmaddpd -12 * SIZE(Y), ALPHA, %xmm2 , %xmm2
	vfmaddpd -10 * SIZE(Y), ALPHA, %xmm3 , %xmm3
	vmovups	 -2 * SIZE(X), %xmm7


	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)
	vmovups	%xmm2, -12 * SIZE(Y)
	vmovups	%xmm3, -10 * SIZE(Y)

	vfmaddpd  -8 * SIZE(Y), ALPHA, %xmm4 , %xmm4
	vfmaddpd  -6 * SIZE(Y), ALPHA, %xmm5 , %xmm5
	vfmaddpd  -4 * SIZE(Y), ALPHA, %xmm6 , %xmm6
	vfmaddpd  -2 * SIZE(Y), ALPHA, %xmm7 , %xmm7

	vmovups	%xmm4, -8 * SIZE(Y)
	vmovups	%xmm5, -6 * SIZE(Y)
	vmovups	%xmm6, -4 * SIZE(Y)
	vmovups	%xmm7, -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	ALIGN_3

.L13:


	movq	M,  %rax
	andq	$8, %rax
	jle	.L14
	ALIGN_3

	vmovups	-16 * SIZE(X), %xmm0
	vmovups	-14 * SIZE(X), %xmm1
	vmovups	-12 * SIZE(X), %xmm2
	vmovups	-10 * SIZE(X), %xmm3

	vfmaddpd -16 * SIZE(Y), ALPHA, %xmm0 , %xmm0
	vfmaddpd -14 * SIZE(Y), ALPHA, %xmm1 , %xmm1
	vfmaddpd -12 * SIZE(Y), ALPHA, %xmm2 , %xmm2
	vfmaddpd -10 * SIZE(Y), ALPHA, %xmm3 , %xmm3

	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)
	vmovups	%xmm2, -12 * SIZE(Y)
	vmovups	%xmm3, -10 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L14:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L15
	ALIGN_3

	vmovups	-16 * SIZE(X), %xmm0
	vmovups	-14 * SIZE(X), %xmm1

	vfmaddpd -16 * SIZE(Y), ALPHA, %xmm0 , %xmm0
	vfmaddpd -14 * SIZE(Y), ALPHA, %xmm1 , %xmm1

	vmovups	%xmm0, -16 * SIZE(Y)
	vmovups	%xmm1, -14 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L15:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L16
	ALIGN_3

	vmovups	-16 * SIZE(X), %xmm0
	vfmaddpd -16 * SIZE(Y), ALPHA, %xmm0 , %xmm0
	vmovups	%xmm0, -16 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L16:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L19
	ALIGN_3

	vmovsd	-16 * SIZE(X), %xmm0
	vfmaddsd -16 * SIZE(Y), ALPHA, %xmm0 , %xmm0

	vmovsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3


.L40:
	movq	Y, YY
	movq	M,  %rax
//If incx==0 || incy==0, avoid unloop.
	cmpq	$0, INCX
	je  .L46
	cmpq	$0, INCY
	je  .L46

	sarq	$3, %rax
	jle	.L45

	prefetchnta	512(X)
	prefetchnta	512+64(X)
	prefetchnta	512+128(X)
	prefetchnta	512+192(X)

	prefetchnta	512(Y)
	prefetchnta	512+64(Y)
	prefetchnta	512+128(Y)
	prefetchnta	512+192(Y)
	ALIGN_3

.L41:

	vmovsd	0 * SIZE(X), %xmm0
	addq	INCX, X
	vmovhpd	0 * SIZE(X), %xmm0 , %xmm0
	addq	INCX, X

	vmovsd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	vmovhpd	0 * SIZE(YY), %xmm6 , %xmm6
	addq	INCY, YY


	vmovsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	vmovhpd	0 * SIZE(X), %xmm1 , %xmm1
	addq	INCX, X

	vmovsd	0 * SIZE(YY), %xmm7
	addq	INCY, YY
	vmovhpd	0 * SIZE(YY), %xmm7 , %xmm7
	addq	INCY, YY

	vfmaddpd %xmm6 , ALPHA , %xmm0 , %xmm0

	vmovsd	0 * SIZE(X), %xmm2
	addq	INCX, X
	vmovhpd	0 * SIZE(X), %xmm2 , %xmm2
	addq	INCX, X

	vmovsd	0 * SIZE(YY), %xmm8
	addq	INCY, YY
	vmovhpd	0 * SIZE(YY), %xmm8 , %xmm8
	addq	INCY, YY

	vfmaddpd %xmm7 , ALPHA , %xmm1 , %xmm1

	vmovsd	0 * SIZE(X), %xmm3
	addq	INCX, X
	vmovhpd	0 * SIZE(X), %xmm3 , %xmm3
	addq	INCX, X

	vfmaddpd %xmm8 , ALPHA , %xmm2 , %xmm2

	vmovsd	0 * SIZE(YY), %xmm9
	addq	INCY, YY
	vmovhpd	0 * SIZE(YY), %xmm9 , %xmm9
	addq	INCY, YY


	vmovsd	%xmm0, 0 * SIZE(Y)
	addq	INCY, Y
	vmovhpd	%xmm0, 0 * SIZE(Y)
	addq	INCY, Y
	vmovsd	%xmm1, 0 * SIZE(Y)
	addq	INCY, Y
	vmovhpd	%xmm1, 0 * SIZE(Y)
	addq	INCY, Y
	vmovsd	%xmm2, 0 * SIZE(Y)
	addq	INCY, Y
	vmovhpd	%xmm2, 0 * SIZE(Y)
	addq	INCY, Y

	vfmaddpd %xmm9 , ALPHA , %xmm3 , %xmm3

	vmovsd	%xmm3, 0 * SIZE(Y)
	addq	INCY, Y
	vmovhpd	%xmm3, 0 * SIZE(Y)
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

	vfmaddsd (Y) , ALPHA , %xmm0 , %xmm0

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
