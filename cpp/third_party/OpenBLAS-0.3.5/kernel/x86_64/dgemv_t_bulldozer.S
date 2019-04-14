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
#include "l2param.h"


#define A_PRE 256

#define VMOVUPS_A1(OFF, ADDR, REGS) vmovups     OFF(ADDR), REGS
#define VMOVUPS_A2(OFF, ADDR, BASE, SCALE, REGS) vmovups        OFF(ADDR, BASE, SCALE), REGS
#define VMOVUPS_XL1(OFF, ADDR, REGS) vmovups    OFF(ADDR), REGS



#if GEMV_UNROLL < 2
#undef  GEMV_UNROLL
#define GEMV_UNROLL 2
#endif

#ifndef WINDOWS_ABI

#define STACKSIZE	128

#define OLD_M	  %rdi
#define OLD_N	  %rsi
#define OLD_A	  %rcx
#define OLD_LDA	  %r8
#define STACK_INCX	 8 + STACKSIZE(%rsp)
#define STACK_Y		16 + STACKSIZE(%rsp)
#define STACK_INCY	24 + STACKSIZE(%rsp)
#define STACK_BUFFER	32 + STACKSIZE(%rsp)
#define MMM	56(%rsp)
#define NN	64(%rsp)
#define AA	72(%rsp)
#define LDAX	80(%rsp)
#else

#define STACKSIZE	256

#define OLD_M	  %rcx
#define OLD_N	  %rdx
#define OLD_A		 40 + STACKSIZE(%rsp)
#define OLD_LDA		 48 + STACKSIZE(%rsp)
#define OLD_X		 56 + STACKSIZE(%rsp)
#define STACK_INCX	 64 + STACKSIZE(%rsp)
#define STACK_Y		 72 + STACKSIZE(%rsp)
#define STACK_INCY	 80 + STACKSIZE(%rsp)
#define STACK_BUFFER	 88 + STACKSIZE(%rsp)
//Temp variables for M,N,A,LDA
#define MMM	224(%rsp)
#define NN	232(%rsp)
#define AA	240(%rsp)
#define LDAX	248(%rsp)

#endif

#define LDA	%r8
#define X	%r9

#define INCX	%rsi
#define INCY	%rdi

#define M	%r10
#define N	%r11
#define A	%r12
#define Y	%r14
#define BUFFER	%r13

#define I	%rax
#define A1	%rbx
#define A2	%rcx
#define LDA3	%rdx
#define Y1	%rbp
#define X1	%r15

#define MM	M

#define ALPHA	%xmm15

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	vmovups	%xmm6,   64(%rsp)
	vmovups	%xmm7,   80(%rsp)
	vmovups	%xmm8,   96(%rsp)
	vmovups	%xmm9,  112(%rsp)
	vmovups	%xmm10, 128(%rsp)
	vmovups	%xmm11, 144(%rsp)
	vmovups	%xmm12, 160(%rsp)
	vmovups	%xmm13, 176(%rsp)
	vmovups	%xmm14, 192(%rsp)
	vmovups	%xmm15, 208(%rsp)

	movq	OLD_M,	      M
	movq	OLD_N,        N
	movq	OLD_A,        A
	movq	OLD_LDA,      LDA
	movq	OLD_X,        X

	movq	M,	      MMM
	movq	N,            NN
	movq	A,            AA
	movq	LDA,	      LDAX

#else
	movq	OLD_M,	      MMM
	movq	OLD_N,        NN
	movq	OLD_A,        AA
	movq	OLD_LDA,      LDAX
#endif
#ifdef HAVE_SSE3
#ifndef WINDOWS_ABI
	movddup	%xmm0, ALPHA
#else
	movddup	%xmm3, ALPHA
#endif
#else
#ifndef WINDOWS_ABI
	vmovups	%xmm0, ALPHA
#else
	vmovups	%xmm3, ALPHA
#endif
	unpcklpd ALPHA, ALPHA
#endif



.L0x:
	xorq	M,M
	addq	$1,M
	salq	$22,M
	subq	M,MMM
	jge .L00

	movq	MMM,%rax
	addq	M,%rax
	jle	.L999x
	movq	%rax,M

.L00:
	movq	LDAX,LDA
	movq	NN,N
	movq	AA,A
	movq	STACK_INCX,   INCX
	movq	STACK_Y,      Y
	movq	STACK_INCY,   INCY
	movq	STACK_BUFFER, BUFFER

	leaq	-1(INCX), %rax

	leaq	(,LDA,  SIZE), LDA
	leaq	(,INCX, SIZE), INCX
	leaq	(,INCY, SIZE), INCY

	leaq	(LDA, LDA, 2), LDA3

	subq	$-16 * SIZE, A

	testq	M, M
	jle	.L999
	testq	N, N
	jle	.L999

	movq	BUFFER, X1

	movq	M,  I
	sarq	$3, I
	jle	.L05
	ALIGN_4

.L02:
	vmovsd	(X), %xmm0
	addq	INCX, X
	vmovhpd	(X), %xmm0 , %xmm0
	addq	INCX, X

	vmovsd	(X), %xmm1
	addq	INCX, X
	vmovhpd	(X), %xmm1 , %xmm1
	addq	INCX, X

	vmovsd	(X), %xmm2
	addq	INCX, X
	vmovhpd	(X), %xmm2 , %xmm2
	addq	INCX, X

	vmovsd	(X), %xmm3
	addq	INCX, X
	vmovhpd	(X), %xmm3 , %xmm3
	addq	INCX, X

	vmovups	%xmm0, 0 * SIZE(X1)
	vmovups	%xmm1, 2 * SIZE(X1)
	vmovups	%xmm2, 4 * SIZE(X1)
	vmovups	%xmm3, 6 * SIZE(X1)

	addq	$8 * SIZE, X1
	decq	I
	jg	.L02
	ALIGN_4

.L05:
	movq	M,  I
	andq	$7, I
	jle	.L10
	ALIGN_2

.L06:
	vmovsd	(X), %xmm0
	addq	INCX, X
	vmovsd	%xmm0, 0 * SIZE(X1)
	addq	$SIZE, X1
	decq	I
	jg	.L06
	ALIGN_4

.L10:
	movq	Y, Y1


#if GEMV_UNROLL >= 8
	cmpq	$8, N
	jl	.L20
	ALIGN_3

.L11:
	subq	$8, N

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 4), A2
	leaq	(A1, LDA, 8), A

	vxorps %xmm0 , 	%xmm0, %xmm0
	vxorps %xmm1 , 	%xmm1, %xmm1
	vxorps %xmm2 , 	%xmm2, %xmm2
	vxorps %xmm3 , 	%xmm3, %xmm3
	vxorps %xmm4 , 	%xmm4, %xmm4
	vxorps %xmm5 , 	%xmm5, %xmm5
	vxorps %xmm6 , 	%xmm6, %xmm6
	vxorps %xmm7 , 	%xmm7, %xmm7


	movq	M,  I
	sarq	$3,  I
	jle	.L15


	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

	decq	I
	jle	.L13
	ALIGN_4

.L12:

	prefetchnta     A_PRE(A1)
	prefetchnta     A_PRE(A2)
	vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
	vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
	vfmaddpd %xmm2 , -16 * SIZE(A1 , LDA , 2) , %xmm12 , %xmm2
	vfmaddpd %xmm3 , -16 * SIZE(A1 , LDA3, 1) , %xmm12 , %xmm3

	prefetchnta     A_PRE(A1,LDA,1)
	prefetchnta     A_PRE(A2,LDA,1)
	vfmaddpd %xmm4 , -16 * SIZE(A2)           , %xmm12 , %xmm4
	vfmaddpd %xmm5 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm5
	vfmaddpd %xmm6 , -16 * SIZE(A2 , LDA , 2) , %xmm12 , %xmm6
	vfmaddpd %xmm7 , -16 * SIZE(A2 , LDA3, 1) , %xmm12 , %xmm7

	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)

	prefetchnta     A_PRE(A1,LDA,2)
	prefetchnta     A_PRE(A2,LDA,2)
	vfmaddpd %xmm0 , -14 * SIZE(A1)           , %xmm13 , %xmm0
	vfmaddpd %xmm1 , -14 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
	vfmaddpd %xmm2 , -14 * SIZE(A1 , LDA , 2) , %xmm13 , %xmm2
	vfmaddpd %xmm3 , -14 * SIZE(A1 , LDA3, 1) , %xmm13 , %xmm3

	prefetchnta     A_PRE(A1,LDA3,1)
	prefetchnta     A_PRE(A2,LDA3,1)
	vfmaddpd %xmm4 , -14 * SIZE(A2)           , %xmm13 , %xmm4
	vfmaddpd %xmm5 , -14 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm5
	vfmaddpd %xmm6 , -14 * SIZE(A2 , LDA , 2) , %xmm13 , %xmm6
	vfmaddpd %xmm7 , -14 * SIZE(A2 , LDA3, 1) , %xmm13 , %xmm7

	prefetchnta     A_PRE(X1)
	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

	vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
	vfmaddpd %xmm1 , -12 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
	vfmaddpd %xmm2 , -12 * SIZE(A1 , LDA , 2) , %xmm12 , %xmm2
	vfmaddpd %xmm3 , -12 * SIZE(A1 , LDA3, 1) , %xmm12 , %xmm3

	vfmaddpd %xmm4 , -12 * SIZE(A2)           , %xmm12 , %xmm4
	vfmaddpd %xmm5 , -12 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm5
	vfmaddpd %xmm6 , -12 * SIZE(A2 , LDA , 2) , %xmm12 , %xmm6
	vfmaddpd %xmm7 , -12 * SIZE(A2 , LDA3, 1) , %xmm12 , %xmm7

	VMOVUPS_XL1(-8 * SIZE, X1, %xmm12)

	vfmaddpd %xmm0 , -10 * SIZE(A1)           , %xmm13 , %xmm0
	vfmaddpd %xmm1 , -10 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
	vfmaddpd %xmm2 , -10 * SIZE(A1 , LDA , 2) , %xmm13 , %xmm2
	vfmaddpd %xmm3 , -10 * SIZE(A1 , LDA3, 1) , %xmm13 , %xmm3

	vfmaddpd %xmm4 , -10 * SIZE(A2)           , %xmm13 , %xmm4
	vfmaddpd %xmm5 , -10 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm5
	vfmaddpd %xmm6 , -10 * SIZE(A2 , LDA , 2) , %xmm13 , %xmm6
	vfmaddpd %xmm7 , -10 * SIZE(A2 , LDA3, 1) , %xmm13 , %xmm7


	VMOVUPS_XL1(-6 * SIZE, X1, %xmm13)

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1

	decq	I
	jg	.L12
	ALIGN_4

.L13:

	vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
	vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
	vfmaddpd %xmm2 , -16 * SIZE(A1 , LDA , 2) , %xmm12 , %xmm2
	vfmaddpd %xmm3 , -16 * SIZE(A1 , LDA3, 1) , %xmm12 , %xmm3

	vfmaddpd %xmm4 , -16 * SIZE(A2)           , %xmm12 , %xmm4
	vfmaddpd %xmm5 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm5
	vfmaddpd %xmm6 , -16 * SIZE(A2 , LDA , 2) , %xmm12 , %xmm6
	vfmaddpd %xmm7 , -16 * SIZE(A2 , LDA3, 1) , %xmm12 , %xmm7

	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)

	vfmaddpd %xmm0 , -14 * SIZE(A1)           , %xmm13 , %xmm0
	vfmaddpd %xmm1 , -14 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
	vfmaddpd %xmm2 , -14 * SIZE(A1 , LDA , 2) , %xmm13 , %xmm2
	vfmaddpd %xmm3 , -14 * SIZE(A1 , LDA3, 1) , %xmm13 , %xmm3

	vfmaddpd %xmm4 , -14 * SIZE(A2)           , %xmm13 , %xmm4
	vfmaddpd %xmm5 , -14 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm5
	vfmaddpd %xmm6 , -14 * SIZE(A2 , LDA , 2) , %xmm13 , %xmm6
	vfmaddpd %xmm7 , -14 * SIZE(A2 , LDA3, 1) , %xmm13 , %xmm7

	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

	vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
	vfmaddpd %xmm1 , -12 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
	vfmaddpd %xmm2 , -12 * SIZE(A1 , LDA , 2) , %xmm12 , %xmm2
	vfmaddpd %xmm3 , -12 * SIZE(A1 , LDA3, 1) , %xmm12 , %xmm3

	vfmaddpd %xmm4 , -12 * SIZE(A2)           , %xmm12 , %xmm4
	vfmaddpd %xmm5 , -12 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm5
	vfmaddpd %xmm6 , -12 * SIZE(A2 , LDA , 2) , %xmm12 , %xmm6
	vfmaddpd %xmm7 , -12 * SIZE(A2 , LDA3, 1) , %xmm12 , %xmm7

	vfmaddpd %xmm0 , -10 * SIZE(A1)           , %xmm13 , %xmm0
	vfmaddpd %xmm1 , -10 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
	vfmaddpd %xmm2 , -10 * SIZE(A1 , LDA , 2) , %xmm13 , %xmm2
	vfmaddpd %xmm3 , -10 * SIZE(A1 , LDA3, 1) , %xmm13 , %xmm3

	vfmaddpd %xmm4 , -10 * SIZE(A2)           , %xmm13 , %xmm4
	vfmaddpd %xmm5 , -10 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm5
	vfmaddpd %xmm6 , -10 * SIZE(A2 , LDA , 2) , %xmm13 , %xmm6
	vfmaddpd %xmm7 , -10 * SIZE(A2 , LDA3, 1) , %xmm13 , %xmm7



	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L15:
	testq	$4, M
	jle	.L16


	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)


	vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
	vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
	vfmaddpd %xmm2 , -16 * SIZE(A1 , LDA , 2) , %xmm12 , %xmm2
	vfmaddpd %xmm3 , -16 * SIZE(A1 , LDA3, 1) , %xmm12 , %xmm3

	vfmaddpd %xmm4 , -16 * SIZE(A2)           , %xmm12 , %xmm4
	vfmaddpd %xmm5 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm5
	vfmaddpd %xmm6 , -16 * SIZE(A2 , LDA , 2) , %xmm12 , %xmm6
	vfmaddpd %xmm7 , -16 * SIZE(A2 , LDA3, 1) , %xmm12 , %xmm7

	vfmaddpd %xmm0 , -14 * SIZE(A1)           , %xmm13 , %xmm0
	vfmaddpd %xmm1 , -14 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
	vfmaddpd %xmm2 , -14 * SIZE(A1 , LDA , 2) , %xmm13 , %xmm2
	vfmaddpd %xmm3 , -14 * SIZE(A1 , LDA3, 1) , %xmm13 , %xmm3

	vfmaddpd %xmm4 , -14 * SIZE(A2)           , %xmm13 , %xmm4
	vfmaddpd %xmm5 , -14 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm5
	vfmaddpd %xmm6 , -14 * SIZE(A2 , LDA , 2) , %xmm13 , %xmm6
	vfmaddpd %xmm7 , -14 * SIZE(A2 , LDA3, 1) , %xmm13 , %xmm7


	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L16:
	testq	$2, M
	jle	.L17

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)

	vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
	vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
	vfmaddpd %xmm2 , -16 * SIZE(A1 , LDA , 2) , %xmm12 , %xmm2
	vfmaddpd %xmm3 , -16 * SIZE(A1 , LDA3, 1) , %xmm12 , %xmm3

	vfmaddpd %xmm4 , -16 * SIZE(A2)           , %xmm12 , %xmm4
	vfmaddpd %xmm5 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm5
	vfmaddpd %xmm6 , -16 * SIZE(A2 , LDA , 2) , %xmm12 , %xmm6
	vfmaddpd %xmm7 , -16 * SIZE(A2 , LDA3, 1) , %xmm12 , %xmm7

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L17:
	testq	$1, M
	je	.L18

	vmovsd	-16 * SIZE(X1), %xmm12

        vmovsd   -16 * SIZE(A1), %xmm8
        vmovsd   -16 * SIZE(A1, LDA), %xmm9
        vmovsd   -16 * SIZE(A1, LDA, 2), %xmm10
        vmovsd   -16 * SIZE(A1, LDA3), %xmm11

	vfmaddpd %xmm0, %xmm8 , %xmm12, %xmm0
	vfmaddpd %xmm1, %xmm9 , %xmm12, %xmm1
	vfmaddpd %xmm2, %xmm10, %xmm12, %xmm2
	vfmaddpd %xmm3, %xmm11, %xmm12, %xmm3

        vmovsd   -16 * SIZE(A2), %xmm8
        vmovsd   -16 * SIZE(A2, LDA), %xmm9
        vmovsd   -16 * SIZE(A2, LDA, 2), %xmm10
        vmovsd   -16 * SIZE(A2, LDA3), %xmm11

	vfmaddpd %xmm4, %xmm8 , %xmm12, %xmm4
	vfmaddpd %xmm5, %xmm9 , %xmm12, %xmm5
	vfmaddpd %xmm6, %xmm10, %xmm12, %xmm6
	vfmaddpd %xmm7, %xmm11, %xmm12, %xmm7

	ALIGN_4

.L18:
	vhaddpd	%xmm1, %xmm0 , %xmm0
	vhaddpd	%xmm3, %xmm2 , %xmm2
	vhaddpd	%xmm5, %xmm4 , %xmm4
	vhaddpd	%xmm7, %xmm6 , %xmm6

	vmulpd	ALPHA, %xmm0 , %xmm0
	vmulpd	ALPHA, %xmm2 , %xmm2
	vmulpd	ALPHA, %xmm4 , %xmm4
	vmulpd	ALPHA, %xmm6 , %xmm6

	cmpq	$SIZE, INCY
	jne	.L19

	vaddpd	0 * SIZE(Y),  %xmm0 , %xmm0
	vaddpd	2 * SIZE(Y),  %xmm2 , %xmm2
	vaddpd	4 * SIZE(Y),  %xmm4 , %xmm4
	vaddpd	6 * SIZE(Y),  %xmm6 , %xmm6
	addq	$8 * SIZE, Y

	vmovups	%xmm0,  0 * SIZE(Y1)
	vmovups	%xmm2,  2 * SIZE(Y1)
	vmovups	%xmm4,  4 * SIZE(Y1)
	vmovups	%xmm6,  6 * SIZE(Y1)
	addq	$8 * SIZE, Y1

	cmpq	$8, N
	jge	.L11
	jmp	.L20
	ALIGN_4

.L19:

	vmovsd	(Y), %xmm8
	addq	INCY, Y
	vmovhpd	(Y), %xmm8 , %xmm8
	addq	INCY, Y
	vmovsd	(Y), %xmm9
	addq	INCY, Y
	vmovhpd	(Y), %xmm9 , %xmm9
	addq	INCY, Y
	vmovsd	(Y), %xmm10
	addq	INCY, Y
	vmovhpd	(Y), %xmm10 , %xmm10
	addq	INCY, Y
	vmovsd	(Y), %xmm11
	addq	INCY, Y
	vmovhpd	(Y), %xmm11 , %xmm11
	addq	INCY, Y

	vaddpd	%xmm8,  %xmm0 , %xmm0
	vaddpd	%xmm9,  %xmm2 , %xmm2
	vaddpd	%xmm10, %xmm4 , %xmm4
	vaddpd	%xmm11, %xmm6 , %xmm6

	vmovlpd	%xmm0, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm0, (Y1)
	addq	INCY, Y1
	vmovlpd	%xmm2, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm2, (Y1)
	addq	INCY, Y1
	vmovlpd	%xmm4, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm4, (Y1)
	addq	INCY, Y1
	vmovlpd	%xmm6, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm6, (Y1)
	addq	INCY, Y1

	cmpq	$8, N
	jge	.L11
	ALIGN_4

.L20:
#endif

#if GEMV_UNROLL >= 4

	cmpq	$4, N
	jl	.L30

#if GEMV_UNROLL == 4
	ALIGN_3

.L21:
#endif
	subq	$4, N

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA, 2), A2
	leaq	(A1, LDA, 4), A

	vxorps %xmm0 , 	%xmm0, %xmm0
	vxorps %xmm1 , 	%xmm1, %xmm1
	vxorps %xmm2 , 	%xmm2, %xmm2
	vxorps %xmm3 , 	%xmm3, %xmm3


	movq	M,  I
	sarq	$3,  I
	jle	.L25

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

	decq	I
	jle	.L23
	ALIGN_4

.L22:

	prefetchnta     A_PRE(A1)
	prefetchnta     A_PRE(A2)
        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
        vfmaddpd %xmm2 , -16 * SIZE(A2)           , %xmm12 , %xmm2
        vfmaddpd %xmm3 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm3

	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)

	prefetchnta     A_PRE(A1,LDA,1)
	prefetchnta     A_PRE(A2,LDA,1)
        vfmaddpd %xmm0 , -14 * SIZE(A1)           , %xmm13 , %xmm0
        vfmaddpd %xmm1 , -14 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
        vfmaddpd %xmm2 , -14 * SIZE(A2)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -14 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm3

	prefetchnta     A_PRE(X1)
	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -12 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
        vfmaddpd %xmm2 , -12 * SIZE(A2)           , %xmm12 , %xmm2
        vfmaddpd %xmm3 , -12 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm3

	VMOVUPS_XL1( -8 * SIZE, X1, %xmm12)

        vfmaddpd %xmm0 , -10 * SIZE(A1)           , %xmm13 , %xmm0
        vfmaddpd %xmm1 , -10 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
        vfmaddpd %xmm2 , -10 * SIZE(A2)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -10 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm3

	VMOVUPS_XL1( -6 * SIZE, X1, %xmm13)

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1

	decq	I
	jg	.L22
	ALIGN_4

.L23:

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
        vfmaddpd %xmm2 , -16 * SIZE(A2)           , %xmm12 , %xmm2
        vfmaddpd %xmm3 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm3

	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)

        vfmaddpd %xmm0 , -14 * SIZE(A1)           , %xmm13 , %xmm0
        vfmaddpd %xmm1 , -14 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
        vfmaddpd %xmm2 , -14 * SIZE(A2)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -14 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm3

	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -12 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
        vfmaddpd %xmm2 , -12 * SIZE(A2)           , %xmm12 , %xmm2
        vfmaddpd %xmm3 , -12 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm3

        vfmaddpd %xmm0 , -10 * SIZE(A1)           , %xmm13 , %xmm0
        vfmaddpd %xmm1 , -10 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
        vfmaddpd %xmm2 , -10 * SIZE(A2)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -10 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm3


	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L25:
	testq	$4, M
	jle	.L26

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
        vfmaddpd %xmm2 , -16 * SIZE(A2)           , %xmm12 , %xmm2
        vfmaddpd %xmm3 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm3

        vfmaddpd %xmm0 , -14 * SIZE(A1)           , %xmm13 , %xmm0
        vfmaddpd %xmm1 , -14 * SIZE(A1 , LDA , 1) , %xmm13 , %xmm1
        vfmaddpd %xmm2 , -14 * SIZE(A2)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -14 * SIZE(A2 , LDA , 1) , %xmm13 , %xmm3


	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L26:
	testq	$2, M
	jle	.L27

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A1 , LDA , 1) , %xmm12 , %xmm1
        vfmaddpd %xmm2 , -16 * SIZE(A2)           , %xmm12 , %xmm2
        vfmaddpd %xmm3 , -16 * SIZE(A2 , LDA , 1) , %xmm12 , %xmm3

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L27:
	testq	$1, M
	je	.L28

	vmovsd	-16 * SIZE(X1), %xmm12

	vmovsd	-16 * SIZE(A1), %xmm8
	vmovsd	-16 * SIZE(A1, LDA), %xmm9
	vmovsd	-16 * SIZE(A2), %xmm10
	vmovsd	-16 * SIZE(A2, LDA), %xmm11

	vfmaddpd %xmm0, %xmm8 , %xmm12, %xmm0
	vfmaddpd %xmm1, %xmm9 , %xmm12, %xmm1
	vfmaddpd %xmm2, %xmm10, %xmm12, %xmm2
	vfmaddpd %xmm3, %xmm11, %xmm12, %xmm3

	ALIGN_4

.L28:
	vhaddpd	%xmm1, %xmm0 , %xmm0
	vhaddpd	%xmm3, %xmm2 , %xmm2

	vmulpd	ALPHA, %xmm0 , %xmm0
	vmulpd	ALPHA, %xmm2 , %xmm2

	cmpq	$SIZE, INCY
	jne	.L29

	vmovups	 0 * SIZE(Y), %xmm4
	vmovups  2 * SIZE(Y), %xmm5
	addq	$4 * SIZE, Y

	vaddpd	%xmm4, %xmm0 , %xmm0
	vaddpd	%xmm5, %xmm2 , %xmm2

	vmovups	%xmm0,  0 * SIZE(Y1)
	vmovups	%xmm2,  2 * SIZE(Y1)
	addq	$4 * SIZE, Y1

#if GEMV_UNROLL == 4
	cmpq	$4, N
	jge	.L21
#endif
	jmp	.L30
	ALIGN_4

.L29:

	vmovsd	(Y), %xmm4
	addq	INCY, Y
	vmovhpd	(Y), %xmm4 , %xmm4
	addq	INCY, Y
	vmovsd	(Y), %xmm5
	addq	INCY, Y
	vmovhpd	(Y), %xmm5 , %xmm5
	addq	INCY, Y

	vaddpd	%xmm4, %xmm0 , %xmm0
	vaddpd	%xmm5, %xmm2 , %xmm2

	vmovlpd	%xmm0, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm0, (Y1)
	addq	INCY, Y1
	vmovlpd	%xmm2, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm2, (Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 4
	cmpq	$4, N
	jge	.L21
#endif
	ALIGN_4

.L30:
#endif

#if GEMV_UNROLL >= 2

	cmpq	$2, N
	jl	.L40

#if GEMV_UNROLL == 2
	ALIGN_3

.L31:
#endif

	subq	$2, N

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1
	leaq	(A1, LDA), A2
	leaq	(A1, LDA, 2), A

        vxorps %xmm0 ,  %xmm0, %xmm0
        vxorps %xmm1 ,  %xmm1, %xmm1
        vxorps %xmm2 ,  %xmm2, %xmm2
        vxorps %xmm3 ,  %xmm3, %xmm3


	movq	M,  I
	sarq	$3,  I
	jle	.L35

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

	decq	I
	jle	.L33
	ALIGN_4

.L32:

        prefetchnta     A_PRE(A1)
        prefetchnta     A_PRE(A2)
        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A2)           , %xmm12 , %xmm1

        prefetchnta     A_PRE(X1)
	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)

        vfmaddpd %xmm2 , -14 * SIZE(A1)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -14 * SIZE(A2)           , %xmm13 , %xmm3

	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -12 * SIZE(A2)           , %xmm12 , %xmm1

	VMOVUPS_XL1( -8 * SIZE, X1, %xmm12)

        vfmaddpd %xmm2 , -10 * SIZE(A1)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -10 * SIZE(A2)           , %xmm13 , %xmm3

	VMOVUPS_XL1( -6 * SIZE, X1, %xmm13)

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1

	decq	I
	jg	.L32
	ALIGN_4

.L33:

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A2)           , %xmm12 , %xmm1

	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)

        vfmaddpd %xmm2 , -14 * SIZE(A1)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -14 * SIZE(A2)           , %xmm13 , %xmm3

	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -12 * SIZE(A2)           , %xmm12 , %xmm1

        vfmaddpd %xmm2 , -10 * SIZE(A1)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -10 * SIZE(A2)           , %xmm13 , %xmm3

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, A2
	addq	$8 * SIZE, X1
	ALIGN_4

.L35:
	testq	$4, M
	jle	.L36

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A2)           , %xmm12 , %xmm1

        vfmaddpd %xmm2 , -14 * SIZE(A1)           , %xmm13 , %xmm2
        vfmaddpd %xmm3 , -14 * SIZE(A2)           , %xmm13 , %xmm3


	addq	$4 * SIZE, A1
	addq	$4 * SIZE, A2
	addq	$4 * SIZE, X1
	ALIGN_4

.L36:
	testq	$2, M
	jle	.L37

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm1 , -16 * SIZE(A2)           , %xmm12 , %xmm1

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, A2
	addq	$2 * SIZE, X1
	ALIGN_4

.L37:
	testq	$1, M
	je	.L38

	vmovsd	-16 * SIZE(X1), %xmm12

	vmovsd	-16 * SIZE(A1), %xmm8
	vmovsd	-16 * SIZE(A2), %xmm9

        vfmaddpd %xmm0, %xmm8 , %xmm12, %xmm0
        vfmaddpd %xmm1, %xmm9 , %xmm12, %xmm1

	ALIGN_4

.L38:
	vaddpd	%xmm2, %xmm0 , %xmm0
	vaddpd	%xmm3, %xmm1 , %xmm1

	vhaddpd	%xmm1, %xmm0 , %xmm0

	mulpd	ALPHA, %xmm0

	vmovsd	(Y), %xmm4
	addq	INCY, Y
	vmovhpd	(Y), %xmm4 , %xmm4
	addq	INCY, Y

	vaddpd	%xmm4, %xmm0 , %xmm0

	vmovlpd	%xmm0, (Y1)
	addq	INCY, Y1
	vmovhpd	%xmm0, (Y1)
	addq	INCY, Y1

#if GEMV_UNROLL == 2
	cmpq	$2, N
	jge	.L31
#endif
	ALIGN_4

.L40:
	cmpq	$1, N
	jl	.L999

#endif

	leaq	16 * SIZE(BUFFER), X1

	movq	A, A1

        vxorps %xmm0 ,  %xmm0, %xmm0
        vxorps %xmm1 ,  %xmm1, %xmm1
        vxorps %xmm2 ,  %xmm2, %xmm2
        vxorps %xmm3 ,  %xmm3, %xmm3



	movq	M,  I
	sarq	$3,  I
	jle	.L45


	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

	decq	I
	jle	.L43
	ALIGN_4

.L42:

	prefetchnta     A_PRE(A1)
        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm2 , -14 * SIZE(A1)           , %xmm13 , %xmm2

	prefetchnta     A_PRE(X1)
	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm2 , -10 * SIZE(A1)           , %xmm13 , %xmm2

	VMOVUPS_XL1( -8 * SIZE, X1, %xmm12)
	VMOVUPS_XL1( -6 * SIZE, X1, %xmm13)

	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1

	decq	I
	jg	.L42
	ALIGN_4

.L43:

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm2 , -14 * SIZE(A1)           , %xmm13 , %xmm2

	VMOVUPS_XL1(-12 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-10 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -12 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm2 , -10 * SIZE(A1)           , %xmm13 , %xmm2



	addq	$8 * SIZE, A1
	addq	$8 * SIZE, X1
	ALIGN_4

.L45:
	testq	$4, M
	jle	.L46


	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)
	VMOVUPS_XL1(-14 * SIZE, X1, %xmm13)

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0
        vfmaddpd %xmm2 , -14 * SIZE(A1)           , %xmm13 , %xmm2

	addq	$4 * SIZE, A1
	addq	$4 * SIZE, X1
	ALIGN_4

.L46:
	testq	$2, M
	jle	.L47

	VMOVUPS_XL1(-16 * SIZE, X1, %xmm12)

        vfmaddpd %xmm0 , -16 * SIZE(A1)           , %xmm12 , %xmm0

	addq	$2 * SIZE, A1
	addq	$2 * SIZE, X1
	ALIGN_4

.L47:
	testq	$1, M
	je	.L48

	vmovsd	-16 * SIZE(X1), %xmm12
	vmovsd	-16 * SIZE(A1), %xmm8

	vfmaddpd %xmm0, %xmm8 , %xmm12, %xmm0
	ALIGN_4

.L48:
	vaddpd	%xmm2, %xmm0 , %xmm0
	vaddpd	%xmm3, %xmm1 , %xmm1

	vaddpd	%xmm1, %xmm0 , %xmm0

	vhaddpd	%xmm1, %xmm0 , %xmm0

	vmulsd	ALPHA, %xmm0 , %xmm0

	vmovsd	(Y), %xmm4
	addq	INCY, Y

	vaddsd	%xmm4, %xmm0 , %xmm0

	vmovlpd	%xmm0, (Y1)
	addq	INCY, Y1

	ALIGN_4

.L999:
	leaq	(, M, SIZE), %rax
	addq %rax,AA
	jmp .L0x;
	ALIGN_4

.L999x:
	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	vmovups	 64(%rsp), %xmm6
	vmovups	 80(%rsp), %xmm7
	vmovups	 96(%rsp), %xmm8
	vmovups	112(%rsp), %xmm9
	vmovups	128(%rsp), %xmm10
	vmovups	144(%rsp), %xmm11
	vmovups	160(%rsp), %xmm12
	vmovups	176(%rsp), %xmm13
	vmovups	192(%rsp), %xmm14
	vmovups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret
	ALIGN_4

	EPILOGUE
