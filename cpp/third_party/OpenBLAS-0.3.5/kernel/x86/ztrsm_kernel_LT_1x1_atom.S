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
#define ARGS	16

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	24 + STACK + ARGS(%esp)
#define A	32 + STACK + ARGS(%esp)
#define ARG_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define ARG_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)
#define AORIG	12 + STACK(%esp)

#define PREFETCH     prefetcht0
#define PREFETCHSIZE (8 * 8 + 3)

#ifndef CONJ
#define ADDSD1	addsd
#define ADDSD2	addsd
#define ADDSD3	addsd
#define ADDSD4	subsd

#elif defined(LN) || defined(LT)
#define ADDSD1	addsd
#define ADDSD2	addsd
#define ADDSD3	subsd
#define ADDSD4	addsd
#else
#define ADDSD1	addsd
#define ADDSD2	subsd
#define ADDSD3	addsd
#define ADDSD4	addsd
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define CO1	%esi

	PROLOGUE

	subl	$ARGS, %esp

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_B,   B
	movl	ARG_LDC, LDC
	movl	OFFSET, %eax
#ifdef RN
	negl	%eax
#endif
	movl	%eax, KK

	sall	$ZBASE_SHIFT, LDC

#ifdef LN
       movl	M, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       sall	$ZBASE_SHIFT, %eax
       imull	K, %eax
       addl	%eax, B

       movl	N, %eax
       imull	LDC, %eax
       addl	%eax, C
#endif

#ifdef RT
       movl	N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

	movl	N, %eax
	testl	%eax, %eax
	movl	%eax, J			# j = n
	jle	.L999
	ALIGN_4

.L01:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	subl	%eax, B
#endif

#ifdef RT
	subl	LDC, C
#endif
	movl	C, CO1
#ifndef RT
	addl	LDC, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,    %ebx
	testl	%ebx, %ebx
	jle	.L99
	ALIGN_4

.L10:
#ifdef LN
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	xorps	%xmm4, %xmm4
	prefetcht0	1 * SIZE(CO1)
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	je	.L15
	ALIGN_4

.L12:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)

	ADDSD3	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	ADDSD3	%xmm2, %xmm6
	movsd	 3 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 2 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 3 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 4 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 2 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 3 * SIZE(BB), %xmm3

	ADDSD3	%xmm2, %xmm6
	movsd	 5 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 4 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 5 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 6 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 4 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 5 * SIZE(BB), %xmm3

	ADDSD3	%xmm2, %xmm6
	movsd	 7 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 6 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 7 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 8 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 6 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 7 * SIZE(BB), %xmm3

	addl   $8 * SIZE, BB
	addl   $8 * SIZE, AA
	decl   %eax
	jne    .L12
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_3

.L16:
	ADDSD3	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	ADDSD3	%xmm2, %xmm6
	ADDSD4	%xmm3, %xmm7

	addsd	%xmm7, %xmm4
	addsd	%xmm5, %xmm6

#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BB), %xmm0
	movsd	 1 * SIZE(BB), %xmm1
#else
	movsd	 0 * SIZE(AA), %xmm0
	movsd	 1 * SIZE(AA), %xmm1
#endif

	subsd	%xmm4,  %xmm0
	subsd	%xmm6,  %xmm1

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(AA), %xmm6
	movaps	 %xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm7
	movaps	 %xmm1, %xmm4

	mulsd	 %xmm6, %xmm0
	mulsd	 %xmm6, %xmm1
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4

	ADDSD4	 %xmm4, %xmm0
	ADDSD3	 %xmm5, %xmm1
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BB), %xmm6
	movaps	 %xmm0, %xmm5
	movsd	 1 * SIZE(BB), %xmm7
	movaps	 %xmm1, %xmm4

	mulsd	 %xmm6, %xmm0
	mulsd	 %xmm6, %xmm1
	mulsd	 %xmm7, %xmm5
	mulsd	 %xmm7, %xmm4

	ADDSD4	 %xmm4, %xmm0
	ADDSD2	 %xmm5, %xmm1
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movsd	%xmm0,   0 * SIZE(CO1)
	movsd	%xmm1,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm1,   1 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm1,   1 * SIZE(AA)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
	addl	%eax, BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L10
	ALIGN_4

.L99:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif

	decl	J			# j --
	jg	.L01
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
