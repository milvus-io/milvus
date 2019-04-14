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

#ifdef PENTIUM4
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#ifdef PENTIUMM
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define CO1	%esi

#define ADDSUB	addpd

#define KERNEL1(address) \
	mulpd	 %xmm0, %xmm2; \
	PREFETCH  (PREFETCHSIZE + 0) * SIZE + (address) * 1 * SIZE(AA); \
	addpd	 %xmm2, %xmm4; \
	movddup	 1 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	 2 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	 3 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	movapd	 2 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm2, %xmm7; \
	movddup	 4 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL2(address) \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	 5 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	 6 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	 7 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm0, %xmm2; \
	movapd	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm2, %xmm7; \
	movddup	16 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL3(address) \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	 9 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	10 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	11 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	movapd	 6 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	12 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL4(address) \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	13 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	14 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	15 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm0, %xmm3; \
	movapd	16 * SIZE + (address) * 1 * SIZE(AA), %xmm0; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	24 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL5(address) \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	17 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	18 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	19 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	movapd	10 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm2, %xmm7; \
	movddup	20 * SIZE + (address) * 2 * SIZE(BB), %xmm2

#define KERNEL6(address) \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm4; \
	movddup	21 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	ADDSUB	 %xmm2, %xmm5; \
	movddup	22 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	addpd	 %xmm2, %xmm6; \
	movddup	23 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm2; \
	movapd	12 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm2, %xmm7

#define KERNEL7(address) \
	movddup	32 * SIZE + (address) * 2 * SIZE(BB), %xmm2; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	25 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	26 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	27 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	movapd	14 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	28 * SIZE + (address) * 2 * SIZE(BB), %xmm3

#define KERNEL8(address) \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm4; \
	movddup	29 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	ADDSUB	 %xmm3, %xmm5; \
	movddup	30 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	addpd	 %xmm3, %xmm6; \
	movddup	31 * SIZE + (address) * 2 * SIZE(BB), %xmm3; \
	mulpd	 %xmm1, %xmm3; \
	movapd	24 * SIZE + (address) * 1 * SIZE(AA), %xmm1; \
	ADDSUB	 %xmm3, %xmm7; \
	movddup	40 * SIZE + (address) * 2 * SIZE(BB), %xmm3

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
	sarl	$1, %eax
	movl	%eax, J			# j = n
	jle	.L100
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
	sall	$1 + ZBASE_SHIFT, %eax
	subl	%eax, B
#endif

	leal	(, LDC, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
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

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L100
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
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef LN
	prefetchnta	-2 * SIZE(CO1)
	prefetchnta	-2 * SIZE(CO1, LDC, 1)
#else
	prefetchnta	 2 * SIZE(CO1)
	prefetchnta	 2 * SIZE(CO1, LDC, 1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L12
	ALIGN_4

.L11:
	KERNEL1(16  *  0)
	KERNEL2(16  *  0)
	KERNEL3(16  *  0)
	KERNEL4(16  *  0)
	KERNEL5(16  *  0)
	KERNEL6(16  *  0)
	KERNEL7(16  *  0)
	KERNEL8(16  *  0)

	addl   $32 * SIZE, BB
	addl   $16 * SIZE, AA
	decl   %eax
	jne    .L11
	ALIGN_4

.L12:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14
	ALIGN_4

.L13:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	ADDSUB	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L13
	ALIGN_4

.L14:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

	pcmpeqb	%xmm1, %xmm1
	psllq	$63,   %xmm1

	shufps	$0x40, %xmm1, %xmm1

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#ifndef CONJ
	xorpd	%xmm1, %xmm5
	xorpd	%xmm1, %xmm7

	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6
#else
#if defined(LN) || defined(LT)
	xorpd	%xmm1, %xmm4
	xorpd	%xmm1, %xmm6
#else
	xorpd	%xmm1, %xmm5
	xorpd	%xmm1, %xmm7
#endif
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BB), %xmm5
	movapd	 2 * SIZE(BB), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	 0 * SIZE(AA), %xmm5
	movapd	 2 * SIZE(AA), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#if defined(LN) || defined(LT)
	movddup	 0 * SIZE(AA), %xmm2
	movddup	 1 * SIZE(AA), %xmm3

	movapd	%xmm5, %xmm4
	movapd	%xmm7, %xmm6

	SHUFPD_1 %xmm4, %xmm4
	SHUFPD_1 %xmm6, %xmm6

	xorpd	 %xmm1, %xmm4
	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4
	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm4, %xmm5
	addpd	 %xmm6, %xmm7
#endif

#ifdef RN
	movddup	 0 * SIZE(BB), %xmm2
	movddup	 1 * SIZE(BB), %xmm3

	movapd	%xmm5, %xmm4
	SHUFPD_1 %xmm4, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5

	movddup	 2 * SIZE(BB), %xmm2
	movddup	 3 * SIZE(BB), %xmm3

	movapd	 %xmm5, %xmm4
	movapd	 %xmm5, %xmm6
	SHUFPD_1 %xmm6, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm7
	subpd	 %xmm6, %xmm7

	movddup	 6 * SIZE(BB), %xmm2
	movddup	 7 * SIZE(BB), %xmm3

	movapd	 %xmm7, %xmm6
	SHUFPD_1 %xmm6, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7
#endif

#ifdef RT
	movddup	 6 * SIZE(BB), %xmm2
	movddup	 7 * SIZE(BB), %xmm3

	movapd	 %xmm7, %xmm6
	SHUFPD_1 %xmm6, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7

	movddup	 4 * SIZE(BB), %xmm2
	movddup	 5 * SIZE(BB), %xmm3

	movapd	 %xmm7, %xmm4
	movapd	 %xmm7, %xmm6
	SHUFPD_1 %xmm6, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm5
	subpd	 %xmm6, %xmm5

	movddup	 0 * SIZE(BB), %xmm2
	movddup	 1 * SIZE(BB), %xmm3

	movapd	 %xmm5, %xmm4
	SHUFPD_1 %xmm4, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movlpd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

	movlpd	%xmm7,   0 * SIZE(CO1, LDC)
	movhpd	%xmm7,   1 * SIZE(CO1, LDC)

#if defined(LN) || defined(LT)
	movapd	%xmm5,   0 * SIZE(BB)
	movapd	%xmm7,   2 * SIZE(BB)
#else
	movapd	%xmm5,   0 * SIZE(AA)
	movapd	%xmm7,   2 * SIZE(AA)

#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
	leal	(BB, %eax, 2), BB
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
       sall	$1 + ZBASE_SHIFT, %eax
       addl	%eax, B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif

	decl	J			# j --
	jg	.L01
	ALIGN_4

.L100:
	movl	N, %eax
	testl	$1, %eax
	jle	.L500

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

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L500
	ALIGN_4

L110:
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

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef LN
	prefetchnta	-2 * SIZE(CO1)
#else
	prefetchnta	 2 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	L112
	ALIGN_4

L111:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 4 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 5 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 6 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm5
	movddup	 6 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 7 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	16 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm7
	movddup	16 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	 9 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm5
	movddup	10 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	11 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	12 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	13 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	14 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm5
	movddup	14 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	15 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	24 * SIZE(AA), %xmm1
	ADDSUB	 %xmm3, %xmm7
	movddup	24 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    L111
	ALIGN_4

L112:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je L114
	ALIGN_4

L113:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	ADDSUB	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	L113
	ALIGN_4

L114:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), BB
#endif

	pcmpeqb	%xmm1, %xmm1
	psllq	$63,   %xmm1

	shufps	$0x40, %xmm1, %xmm1

	SHUFPD_1 %xmm5, %xmm5

#ifndef CONJ
	xorpd	%xmm1, %xmm5

	subpd	%xmm5, %xmm4
#else
#if defined(LN) || defined(LT)
	xorpd	%xmm1, %xmm4
#else
	xorpd	%xmm1, %xmm5
#endif
	addpd	%xmm5, %xmm4
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BB), %xmm5
	subpd	%xmm4,  %xmm5
#else
	movapd	 0 * SIZE(AA), %xmm5
	subpd	%xmm4,  %xmm5
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#if defined(LN) || defined(LT)
	movddup	 0 * SIZE(AA), %xmm2
	movddup	 1 * SIZE(AA), %xmm3

	movapd	 %xmm5, %xmm4
	SHUFPD_1 %xmm4, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#if defined(RN) || defined(RT)
	movddup	 0 * SIZE(BB), %xmm2
	movddup	 1 * SIZE(BB), %xmm3

	movapd	 %xmm5, %xmm4
	SHUFPD_1 %xmm4, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movlpd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm5,   0 * SIZE(BB)
#else
	movapd	%xmm5,   0 * SIZE(AA)
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
	jg	L110

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
	ALIGN_4

.L500:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
