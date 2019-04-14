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

#define STACK	16
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define ALPHA		16 + STACK + ARGS(%esp)
#define STACK_X		24 + STACK + ARGS(%esp)
#define STACK_INCX	28 + STACK + ARGS(%esp)
#define STACK_Y		32 + STACK + ARGS(%esp)
#define STACK_INCY	36 + STACK + ARGS(%esp)

#define M	 %ebx
#define X	 %esi
#define Y	 %edi
#define INCX	 %ecx
#define INCY	 %edx

#define PREFETCHSIZE	 64

	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx
	pushl	%ebp

	PROFCODE

	movlpd	ALPHA, %xmm7
	unpcklpd %xmm7, %xmm7

	movl	STACK_M,    M
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_Y,    Y
	movl	STACK_INCY, INCY

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	testl	M, M
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L100
	cmpl	$SIZE, INCY
	jne	.L100

	testl	$SIZE, Y
	je	.L00

	movlpd	0 * SIZE(X), %xmm0
	mulsd	%xmm7, %xmm0
	addsd	0 * SIZE(Y), %xmm0
	movlpd	%xmm0, 0 * SIZE(Y)
	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	M
	jle	.L999
	ALIGN_3

.L00:
	testl	$SIZE, X
	jne	.L20

	movl	M,  %eax
	sarl	$4,  %eax
	jle	.L15
	ALIGN_3

.L11:
	prefetch     (PREFETCHSIZE + 0) * SIZE(X)

	movapd	 0 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	movapd	 2 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	 2 * SIZE(Y), %xmm1
	movapd	%xmm1,  2 * SIZE(Y)

	prefetchw      (PREFETCHSIZE + 0) * SIZE(Y)

	movapd	 4 * SIZE(X), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	 4 * SIZE(Y), %xmm2
	movapd	%xmm2,  4 * SIZE(Y)

	movapd	 6 * SIZE(X), %xmm3
	mulpd	%xmm7, %xmm3
	addpd	 6 * SIZE(Y), %xmm3
	movapd	%xmm3,  6 * SIZE(Y)

	prefetch     (PREFETCHSIZE + 8) * SIZE(X)

	movapd	 8 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 8 * SIZE(Y), %xmm0
	movapd	%xmm0, 8 * SIZE(Y)

	movapd	10 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	10 * SIZE(Y), %xmm1
	movapd	%xmm1, 10 * SIZE(Y)

	prefetchw      (PREFETCHSIZE + 8) * SIZE(Y)

	movapd	12 * SIZE(X), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	12 * SIZE(Y), %xmm2
	movapd	%xmm2, 12 * SIZE(Y)

	movapd	14 * SIZE(X), %xmm3
	mulpd	%xmm7, %xmm3
	addpd	14 * SIZE(Y), %xmm3
	movapd	%xmm3, 14 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	decl	%eax
	jg	.L11
	ALIGN_3

.L15:
	movl	M,  %eax
	testl	$8, %eax
	jle	.L16

	movapd	 0 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	movapd	 2 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	 2 * SIZE(Y), %xmm1
	movapd	%xmm1,  2 * SIZE(Y)

	movapd	 4 * SIZE(X), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	 4 * SIZE(Y), %xmm2
	movapd	%xmm2,  4 * SIZE(Y)

	movapd	 6 * SIZE(X), %xmm3
	mulpd	%xmm7, %xmm3
	addpd	 6 * SIZE(Y), %xmm3
	movapd	%xmm3,  6 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L16:
	testl	$4, %eax
	jle	.L17

	movapd	 0 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	movapd	 2 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	 2 * SIZE(Y), %xmm1
	movapd	%xmm1,  2 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L17:
	testl	$2, %eax
	jle	.L18

	movapd	 0 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L18:
	testl	$1, %eax
	jle	.L99

	movlpd	0 * SIZE(X), %xmm0
	mulsd	%xmm7, %xmm0
	addsd	0 * SIZE(Y), %xmm0
	movlpd	%xmm0, 	0 * SIZE(Y)
	jmp	.L99
	ALIGN_3

.L20:
	movl	M, %eax
	sarl	$4,   %eax
	jle	.L25
	ALIGN_4

.L21:
#ifdef OPTERON
	prefetcht0     (PREFETCHSIZE + 0) * SIZE(X)
	prefetchw      (PREFETCHSIZE + 0) * SIZE(Y)
#endif

	movlpd	 0 * SIZE(X), %xmm0
	movhpd	 1 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	movlpd	 2 * SIZE(X), %xmm1
	movhpd	 3 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	 2 * SIZE(Y), %xmm1
	movapd	%xmm1,  2 * SIZE(Y)

	movlpd	 4 * SIZE(X), %xmm2
	movhpd	 5 * SIZE(X), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	 4 * SIZE(Y), %xmm2
	movapd	%xmm2,  4 * SIZE(Y)

	movlpd	 6 * SIZE(X), %xmm3
	movhpd	 7 * SIZE(X), %xmm3
	mulpd	%xmm7, %xmm3
	addpd	 6 * SIZE(Y), %xmm3
	movapd	%xmm3,  6 * SIZE(Y)

#ifdef OPTERON
	prefetcht0     (PREFETCHSIZE + 8) * SIZE(X)
	prefetchw      (PREFETCHSIZE + 8) * SIZE(Y)
#endif

	movlpd	 8 * SIZE(X), %xmm0
	movhpd	 9 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 8 * SIZE(Y), %xmm0
	movapd	%xmm0, 8 * SIZE(Y)

	movlpd	10 * SIZE(X), %xmm1
	movhpd	11 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	10 * SIZE(Y), %xmm1
	movapd	%xmm1, 10 * SIZE(Y)

	movlpd	12 * SIZE(X), %xmm2
	movhpd	13 * SIZE(X), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	12 * SIZE(Y), %xmm2
	movapd	%xmm2, 12 * SIZE(Y)

	movlpd	14 * SIZE(X), %xmm3
	movhpd	15 * SIZE(X), %xmm3
	mulpd	%xmm7, %xmm3
	addpd	14 * SIZE(Y), %xmm3
	movapd	%xmm3, 14 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L25:
	movl	M,  %eax
	testl	$8, %eax
	jle	.L26

	movlpd	 0 * SIZE(X), %xmm0
	movhpd	 1 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	movlpd	 2 * SIZE(X), %xmm1
	movhpd	 3 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	 2 * SIZE(Y), %xmm1
	movapd	%xmm1,  2 * SIZE(Y)

	movlpd	 4 * SIZE(X), %xmm2
	movhpd	 5 * SIZE(X), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	 4 * SIZE(Y), %xmm2
	movapd	%xmm2,  4 * SIZE(Y)

	movlpd	 6 * SIZE(X), %xmm3
	movhpd	 7 * SIZE(X), %xmm3
	mulpd	%xmm7, %xmm3
	addpd	 6 * SIZE(Y), %xmm3
	movapd	%xmm3,  6 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L26:
	testl	$4, %eax
	jle	.L27

	movlpd	 0 * SIZE(X), %xmm0
	movhpd	 1 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	movlpd	 2 * SIZE(X), %xmm1
	movhpd	 3 * SIZE(X), %xmm1
	mulpd	%xmm7, %xmm1
	addpd	 2 * SIZE(Y), %xmm1
	movapd	%xmm1,  2 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L27:
	testl	$2, %eax
	jle	.L28

	movlpd	 0 * SIZE(X), %xmm0
	movhpd	 1 * SIZE(X), %xmm0
	mulpd	%xmm7, %xmm0
	addpd	 0 * SIZE(Y), %xmm0
	movapd	%xmm0,  0 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L28:
	testl	$1, %eax
	jle	.L99

	movlpd	0 * SIZE(X), %xmm0
	mulsd	%xmm7, %xmm0
	addsd	0 * SIZE(Y), %xmm0
	movlpd	%xmm0, 	0 * SIZE(Y)
	ALIGN_3

.L99:
	xorl	%eax,%eax
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

.L100:
	movl	M, %eax
	movl	Y, %ebp
	sarl	$3,   %eax
	jle	.L114
	ALIGN_3

.L110:
	movlpd	0 * SIZE(X), %xmm0
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm0
	addl	INCX, X
	mulpd	%xmm7, %xmm0

	movlpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	movhpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	addpd	%xmm6, %xmm0

	movlpd	0 * SIZE(X), %xmm1
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm1
	addl	INCX, X
	mulpd	%xmm7, %xmm1

	movlpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	movhpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	addpd	%xmm6, %xmm1

	movlpd	0 * SIZE(X), %xmm2
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm2
	addl	INCX, X
	mulpd	%xmm7, %xmm2

	movlpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	movhpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	addpd	%xmm6, %xmm2

	movlpd	0 * SIZE(X), %xmm3
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm3
	addl	INCX, X
	mulpd	%xmm7, %xmm3

	movlpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	movhpd	0 * SIZE(%ebp), %xmm6
	addl	INCY, %ebp
	addpd	%xmm6, %xmm3

	movlpd	%xmm0, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm0, 0 * SIZE(Y)
	addl	INCY, Y
	movlpd	%xmm1, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm1, 0 * SIZE(Y)
	addl	INCY, Y
	movlpd	%xmm2, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm2, 0 * SIZE(Y)
	addl	INCY, Y
	movlpd	%xmm3, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm3, 0 * SIZE(Y)
	addl	INCY, Y

	decl	%eax
	jg	.L110
	ALIGN_3

.L114:
	movl	M, %eax
	andl	$7,   %eax
	jle	.L999
	ALIGN_3

.L115:
	movlpd	(X), %xmm0
	addl	INCX, X
	mulsd	%xmm7, %xmm0
	addsd	(Y), %xmm0
	movlpd	%xmm0, (Y)
	addl	INCY, Y
	decl	%eax
	jg	.L115
	ALIGN_3

.L999:
	xorl	%eax,%eax
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
