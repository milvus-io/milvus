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

#define OLD_M	 4 + STACK(%esi)
#define OLD_N	 8 + STACK(%esi)
#define OLD_K	12 + STACK(%esi)
#define OLD_ALPHA	16 + STACK(%esi)
#define OLD_A	20 + STACK(%esi)
#define OLD_B	24 + STACK(%esi)
#define OLD_C	28 + STACK(%esi)
#define OLD_LDC	32 + STACK(%esi)
#define STACK_OFFT	36 + STACK(%esi)

#define ALPHA	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	32(%esp)
#define J	36(%esp)
#define BX	40(%esp)
#define OLD_STACK 44(%esp)
#define OFFSET  48(%esp)
#define KK	52(%esp)
#define KKK	56(%esp)
#define BUFFER 128(%esp)

#ifdef ATHLON
#define PREFETCH     prefetch
#define PREFETCHSIZE  64
#endif

#if defined(OPTERON) || defined(BARCELONA)
#define PREFETCH     prefetch
#define PREFETCHSIZE (16 * 10 + 8)
#endif

#ifdef PENTIUM4
#define PREFETCH     prefetcht0
#define PREFETCHSIZE   96
#endif

#define AA	%edx
#define	BB	%ecx
#define LDC	%ebp

#if defined(OPTERON) || !defined(HAVE_SSE2)
#define movsd	movlps
#endif

#ifdef HAVE_SSE2
#define	xorps	pxor
#endif

#if defined(OPTERON) || defined(BARCELONA)
#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE + (address) * 1 * SIZE(AA); \
	movaps	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	 8 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	12 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	32 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	20 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	24 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	28 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	48 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 8 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL3(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	36 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	40 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	44 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	64 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	12 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL4(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	52 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	56 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	60 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	80 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 32 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL5(address) \
	PREFETCH (PREFETCHSIZE + 16) * SIZE + (address) * 1 * SIZE(AA); \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	68 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	72 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	76 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	 96 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	20 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL6(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	84 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	88 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	92 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	112 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 24 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL7(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	100 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	104 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	108 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	128 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	28 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	116 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	120 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	124 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	144 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 48 * SIZE + (address) * 1 * SIZE(AA), %xmm1;
#endif

#ifdef PENTIUM4
#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE + (address) * 1 * SIZE(AA); \
	addps	%xmm2, %xmm5; \
	movaps	 8 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	12 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	32 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	20 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	24 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	28 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	48 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 8 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL3(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	36 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	40 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	44 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	64 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	12 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL4(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	52 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	56 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	60 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	80 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 32 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL5(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	68 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	72 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	76 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	 96 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	20 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL6(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	84 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	88 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	92 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	112 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 24 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL7(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	100 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	104 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	108 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	128 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	28 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	116 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	120 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	124 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	144 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 48 * SIZE + (address) * 1 * SIZE(AA), %xmm1
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	%esp, %esi	# save old stack
	subl	$128 + LOCAL_BUFFER_SIZE, %esp
	movl	OLD_M, %ebx
	andl	$-1024, %esp	# align stack

        STACK_TOUCHING

	movl	OLD_N, %eax
	movl	OLD_K, %ecx
	movl	OLD_A, %edx
	movss	OLD_ALPHA,  %xmm3
#ifdef TRMMKERNEL
	movss	STACK_OFFT, %xmm4
#endif

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK

	shufps	$0, %xmm3, %xmm3
	movl	OLD_B, %edi
	movl	OLD_C, %ebx
	movaps	 %xmm3, ALPHA

	movl	%ebx, C
	movl	OLD_LDC, LDC
#ifdef TRMMKERNEL
	movss	%xmm4, OFFSET
	movss	%xmm4, KK
#ifndef LEFT
	negl	KK
#endif
#endif
	leal	(, LDC, SIZE), LDC

	sarl	$2, %eax
	movl	%eax, J
	jle	.L40

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	leal	BUFFER, %ecx

	movl	K, %eax
	sarl	$1, %eax
	jle	.L05
	ALIGN_4

.L02:
#ifdef HAVE_SSE2
	movss	 0 * SIZE(%edi), %xmm0
	movss	 1 * SIZE(%edi), %xmm1
	movss	 2 * SIZE(%edi), %xmm2
	movss	 3 * SIZE(%edi), %xmm3
	movss	 4 * SIZE(%edi), %xmm4
	movss	 5 * SIZE(%edi), %xmm5
	movss	 6 * SIZE(%edi), %xmm6
	movss	 7 * SIZE(%edi), %xmm7

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1
	shufps	 $0, %xmm2, %xmm2
	shufps	 $0, %xmm3, %xmm3
	shufps	 $0, %xmm4, %xmm4
	shufps	 $0, %xmm5, %xmm5
	shufps	 $0, %xmm6, %xmm6
	shufps	 $0, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)
#else
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3
	movd	 4 * SIZE(%edi), %mm4
	movd	 5 * SIZE(%edi), %mm5
	movd	 6 * SIZE(%edi), %mm6
	movd	 7 * SIZE(%edi), %mm7

	movd	%mm0,  0 * SIZE(%ecx)
	movd	%mm0,  1 * SIZE(%ecx)
	movd	%mm0,  2 * SIZE(%ecx)
	movd	%mm0,  3 * SIZE(%ecx)
	movd	%mm1,  4 * SIZE(%ecx)
	movd	%mm1,  5 * SIZE(%ecx)
	movd	%mm1,  6 * SIZE(%ecx)
	movd	%mm1,  7 * SIZE(%ecx)
	movd	%mm2,  8 * SIZE(%ecx)
	movd	%mm2,  9 * SIZE(%ecx)
	movd	%mm2, 10 * SIZE(%ecx)
	movd	%mm2, 11 * SIZE(%ecx)
	movd	%mm3, 12 * SIZE(%ecx)
	movd	%mm3, 13 * SIZE(%ecx)
	movd	%mm3, 14 * SIZE(%ecx)
	movd	%mm3, 15 * SIZE(%ecx)
	movd	%mm4, 16 * SIZE(%ecx)
	movd	%mm4, 17 * SIZE(%ecx)
	movd	%mm4, 18 * SIZE(%ecx)
	movd	%mm4, 19 * SIZE(%ecx)
	movd	%mm5, 20 * SIZE(%ecx)
	movd	%mm5, 21 * SIZE(%ecx)
	movd	%mm5, 22 * SIZE(%ecx)
	movd	%mm5, 23 * SIZE(%ecx)
	movd	%mm6, 24 * SIZE(%ecx)
	movd	%mm6, 25 * SIZE(%ecx)
	movd	%mm6, 26 * SIZE(%ecx)
	movd	%mm6, 27 * SIZE(%ecx)
	movd	%mm7, 28 * SIZE(%ecx)
	movd	%mm7, 29 * SIZE(%ecx)
	movd	%mm7, 30 * SIZE(%ecx)
	movd	%mm7, 31 * SIZE(%ecx)
#endif

#ifdef PENTIUM4
     	prefetcht2	112 * SIZE(%ecx)
#endif

#if defined(OPTERON) || defined(BARCELONA)
	prefetchnta	80 * SIZE(%edi)
	prefetchw	112 * SIZE(%ecx)
	prefetchw	120 * SIZE(%ecx)
#endif

	addl	$ 8 * SIZE, %edi
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L02
	ALIGN_2

.L05:
	movl	K, %eax
	andl	$1, %eax
	BRANCH
	jle	.L10


#ifdef HAVE_SSE2
	movss	 0 * SIZE(%edi), %xmm0
	movss	 1 * SIZE(%edi), %xmm1
	movss	 2 * SIZE(%edi), %xmm2
	movss	 3 * SIZE(%edi), %xmm3

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1
	shufps	 $0, %xmm2, %xmm2
	shufps	 $0, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
#else
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3

	movd	%mm0,  0 * SIZE(%ecx)
	movd	%mm0,  1 * SIZE(%ecx)
	movd	%mm0,  2 * SIZE(%ecx)
	movd	%mm0,  3 * SIZE(%ecx)
	movd	%mm1,  4 * SIZE(%ecx)
	movd	%mm1,  5 * SIZE(%ecx)
	movd	%mm1,  6 * SIZE(%ecx)
	movd	%mm1,  7 * SIZE(%ecx)
	movd	%mm2,  8 * SIZE(%ecx)
	movd	%mm2,  9 * SIZE(%ecx)
	movd	%mm2, 10 * SIZE(%ecx)
	movd	%mm2, 11 * SIZE(%ecx)
	movd	%mm3, 12 * SIZE(%ecx)
	movd	%mm3, 13 * SIZE(%ecx)
	movd	%mm3, 14 * SIZE(%ecx)
	movd	%mm3, 15 * SIZE(%ecx)
#endif
	addl	$4 * SIZE, %edi
	ALIGN_4

.L10:
	movl	%edi, BX

	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

	movl	BX, %eax

#ifdef HAVE_SSE

	prefetcht2  0 * SIZE(%eax)
	prefetcht2  4 * SIZE(%eax)

#if   L2_SIZE > 262144

	subl	$-8 * SIZE, BX

#elif L2_SIZE > 131072

	prefetcht2  8 * SIZE(%eax)
	prefetcht2 12 * SIZE(%eax)


	subl	$-16 * SIZE, BX
#else
	prefetcht2 16 * SIZE(%eax)
	prefetcht2 20 * SIZE(%eax)
	prefetcht2 24 * SIZE(%eax)
	prefetcht2 28 * SIZE(%eax)

	subl	$-32 * SIZE, BX
#endif
#endif

	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movaps	16 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

#if defined(OPTERON) || defined(BARCELONA)
	prefetchw	4 * SIZE(%esi)
	prefetchw	4 * SIZE(%esi, LDC)
	prefetchw	4 * SIZE(%esi, LDC, 2)
	prefetchw	4 * SIZE(%esi, %eax)
#endif

#ifdef PENTIUM4
	prefetchnta	4 * SIZE(%esi)
	prefetchnta	4 * SIZE(%esi, LDC)
	prefetchnta	4 * SIZE(%esi, LDC, 2)
	prefetchnta	4 * SIZE(%esi, %eax)
#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$4, %eax
#else
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif

#if 1
	andl	$-8, %eax
	sall	$4, %eax
	je	.L15
.L1X:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)
	cmpl	$128 *  1, %eax
	jle	.L12
	KERNEL1(32  *  1)
	KERNEL2(32  *  1)
	KERNEL3(32  *  1)
	KERNEL4(32  *  1)
	KERNEL5(32  *  1)
	KERNEL6(32  *  1)
	KERNEL7(32  *  1)
	KERNEL8(32  *  1)
	cmpl	$128 *  2, %eax
	jle	.L12
	KERNEL1(32  *  2)
	KERNEL2(32  *  2)
	KERNEL3(32  *  2)
	KERNEL4(32  *  2)
	KERNEL5(32  *  2)
	KERNEL6(32  *  2)
	KERNEL7(32  *  2)
	KERNEL8(32  *  2)
	cmpl	$128 *  3, %eax
	jle	.L12
	KERNEL1(32  *  3)
	KERNEL2(32  *  3)
	KERNEL3(32  *  3)
	KERNEL4(32  *  3)
	KERNEL5(32  *  3)
	KERNEL6(32  *  3)
	KERNEL7(32  *  3)
	KERNEL8(32  *  3)
	cmpl	$128 *  4, %eax
	jle	.L12
	KERNEL1(32  *  4)
	KERNEL2(32  *  4)
	KERNEL3(32  *  4)
	KERNEL4(32  *  4)
	KERNEL5(32  *  4)
	KERNEL6(32  *  4)
	KERNEL7(32  *  4)
	KERNEL8(32  *  4)
	cmpl	$128 *  5, %eax
	jle	.L12
	KERNEL1(32  *  5)
	KERNEL2(32  *  5)
	KERNEL3(32  *  5)
	KERNEL4(32  *  5)
	KERNEL5(32  *  5)
	KERNEL6(32  *  5)
	KERNEL7(32  *  5)
	KERNEL8(32  *  5)
	cmpl	$128 *  6, %eax
	jle	.L12
	KERNEL1(32  *  6)
	KERNEL2(32  *  6)
	KERNEL3(32  *  6)
	KERNEL4(32  *  6)
	KERNEL5(32  *  6)
	KERNEL6(32  *  6)
	KERNEL7(32  *  6)
	KERNEL8(32  *  6)
	cmpl	$128 *  7, %eax
	jle	.L12
	KERNEL1(32  *  7)
	KERNEL2(32  *  7)
	KERNEL3(32  *  7)
	KERNEL4(32  *  7)
	KERNEL5(32  *  7)
	KERNEL6(32  *  7)
	KERNEL7(32  *  7)
	KERNEL8(32  *  7)

	addl	$128 * 8  * SIZE, BB
	addl	$128 * 2  * SIZE, AA
	subl	$128 * 8, %eax
	jg	.L1X
	jmp	.L15

.L12:
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
	ALIGN_4
#else
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)

	addl   $128 * SIZE, BB
	addl   $32 * SIZE, AA
	decl   %eax
	jne    .L12
	ALIGN_4
#endif

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_4

.L16:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 4 * SIZE(AA), %xmm0

	addl	$ 4 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	shufps	$0xe4, %xmm0, %xmm0
	shufps	$0xe4, %xmm1, %xmm1
	shufps	$0xe4, %xmm2, %xmm2
	shufps	$0xe4, %xmm3, %xmm3

	mulps	%xmm3, %xmm4
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0
	mulps	%xmm3, %xmm5
	movsd	0 * SIZE(%esi, LDC, 1), %xmm1
	movhps	2 * SIZE(%esi, LDC, 1), %xmm1
	mulps	%xmm3, %xmm6
	movsd	0 * SIZE(%esi, LDC, 2), %xmm2
	movhps	2 * SIZE(%esi, LDC, 2), %xmm2
	mulps	%xmm3, %xmm7
	movsd	0 * SIZE(%esi, %eax, 1), %xmm3
	movhps	2 * SIZE(%esi, %eax, 1), %xmm3

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm5
	addps	%xmm2, %xmm6
	addps	%xmm3, %xmm7
#else
	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5
	mulps	%xmm3, %xmm6
	mulps	%xmm3, %xmm7
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)
	movlps	%xmm5, 0 * SIZE(%esi, LDC, 1)
	movhps	%xmm5, 2 * SIZE(%esi, LDC, 1)
	movlps	%xmm6, 0 * SIZE(%esi, LDC, 2)
	movhps	%xmm6, 2 * SIZE(%esi, LDC, 2)
	movlps	%xmm7, 0 * SIZE(%esi, %eax, 1)
	movhps	%xmm7, 2 * SIZE(%esi, %eax, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L20:
	testl	$2, M
	je	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	68 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	72 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	76 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	96 * SIZE(BB), %xmm2

	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	84 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	88 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	92 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	112 * SIZE(BB), %xmm3

	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	100 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	104 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	108 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	 14 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	128 * SIZE(BB), %xmm2

	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	116 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	120 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	124 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	 24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	144 * SIZE(BB), %xmm3

	addl	$ 16 * SIZE, AA
	addl	$128 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	16 * SIZE(BB), %xmm2

	addl	$ 2 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	mulps	%xmm3, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(%esi), %xmm0
	mulps	%xmm3, %xmm5
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	0 * SIZE(%esi, LDC, 1), %xmm1
	mulps	%xmm3, %xmm6
#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	0 * SIZE(%esi, LDC, 2), %xmm2
	mulps	%xmm3, %xmm7
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	0 * SIZE(%esi, %eax, 1), %xmm3

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm5
	addps	%xmm2, %xmm6
	addps	%xmm3, %xmm7
#else
	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5
	mulps	%xmm3, %xmm6
	mulps	%xmm3, %xmm7
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movlps	%xmm5, 0 * SIZE(%esi, LDC, 1)
	movlps	%xmm6, 0 * SIZE(%esi, LDC, 2)
	movlps	%xmm7, 0 * SIZE(%esi, %eax, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 2
	ALIGN_4

.L30:
	testl	$1, M
	je	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
	leal	(BB, %eax, 8), BB
#endif

	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movss	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$1, %eax
#else
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L35
	ALIGN_4

.L32:
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movss	 4 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm5
	movss	 8 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 1 * SIZE(AA), %xmm0

	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm4
	movss	20 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm5
	movss	24 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	mulss	28 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 2 * SIZE(AA), %xmm0

	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4
	movss	36 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm5
	movss	40 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	mulss	44 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	64 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 3 * SIZE(AA), %xmm0

	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm4
	movss	52 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm5
	movss	56 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	mulss	60 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	80 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0

	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm4
	movss	68 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm5
	movss	72 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	mulss	76 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	96 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 5 * SIZE(AA), %xmm1

	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm4
	movss	84 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm5
	movss	88 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	mulss	92 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	112 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	 6 * SIZE(AA), %xmm1

	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm4
	movss	100 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm5
	movss	104 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	mulss	108 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	128 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 7 * SIZE(AA), %xmm1

	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm4
	movss	116 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm5
	movss	120 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	mulss	124 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	144 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl	$  8 * SIZE, AA
	addl	$128 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4
	movss	 4 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm5
	movss	 8 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	16 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 1 * SIZE(AA), %xmm0

	addl	$ 1 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	mulss	%xmm3, %xmm4
	movss	0 * SIZE(%esi), %xmm0
	mulss	%xmm3, %xmm5
	movss	0 * SIZE(%esi, LDC, 1), %xmm1
	mulss	%xmm3, %xmm6
	movss	0 * SIZE(%esi, LDC, 2), %xmm2
	mulss	%xmm3, %xmm7
	movss	0 * SIZE(%esi, %eax, 1), %xmm3

	addss	%xmm0, %xmm4
	addss	%xmm1, %xmm5
	addss	%xmm2, %xmm6
	addss	%xmm3, %xmm7
#else
	mulss	%xmm3, %xmm4
	mulss	%xmm3, %xmm5
	mulss	%xmm3, %xmm6
	mulss	%xmm3, %xmm7
#endif

	movss	%xmm4, 0 * SIZE(%esi)
	movss	%xmm5, 0 * SIZE(%esi, LDC, 1)
	movss	%xmm6, 0 * SIZE(%esi, LDC, 2)
	movss	%xmm7, 0 * SIZE(%esi, %eax, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leal	(, LDC, 4), %eax
	addl	%eax, C			# c += 4 * ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testl	$2, N
	je	.L80

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$2, %eax
	jle	.L45
	ALIGN_4

.L42:
	prefetchnta	80 * SIZE(%edi)

#if defined(OPTERON) || defined(BARCELONA)
	prefetchw	112 * SIZE(%ecx)
	prefetchw	120 * SIZE(%ecx)
#endif

#ifdef PENTIUM4
     	prefetcht1	112 * SIZE(%ecx)
#endif

#ifdef HAVE_SSE2
	movss	 0 * SIZE(%edi), %xmm0
	movss	 1 * SIZE(%edi), %xmm1
	movss	 2 * SIZE(%edi), %xmm2
	movss	 3 * SIZE(%edi), %xmm3
	movss	 4 * SIZE(%edi), %xmm4
	movss	 5 * SIZE(%edi), %xmm5
	movss	 6 * SIZE(%edi), %xmm6
	movss	 7 * SIZE(%edi), %xmm7

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1
	shufps	 $0, %xmm2, %xmm2
	shufps	 $0, %xmm3, %xmm3
	shufps	 $0, %xmm4, %xmm4
	shufps	 $0, %xmm5, %xmm5
	shufps	 $0, %xmm6, %xmm6
	shufps	 $0, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)
#else
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3
	movd	 4 * SIZE(%edi), %mm4
	movd	 5 * SIZE(%edi), %mm5
	movd	 6 * SIZE(%edi), %mm6
	movd	 7 * SIZE(%edi), %mm7

	movd	%mm0,  0 * SIZE(%ecx)
	movd	%mm0,  1 * SIZE(%ecx)
	movd	%mm0,  2 * SIZE(%ecx)
	movd	%mm0,  3 * SIZE(%ecx)
	movd	%mm1,  4 * SIZE(%ecx)
	movd	%mm1,  5 * SIZE(%ecx)
	movd	%mm1,  6 * SIZE(%ecx)
	movd	%mm1,  7 * SIZE(%ecx)
	movd	%mm2,  8 * SIZE(%ecx)
	movd	%mm2,  9 * SIZE(%ecx)
	movd	%mm2, 10 * SIZE(%ecx)
	movd	%mm2, 11 * SIZE(%ecx)
	movd	%mm3, 12 * SIZE(%ecx)
	movd	%mm3, 13 * SIZE(%ecx)
	movd	%mm3, 14 * SIZE(%ecx)
	movd	%mm3, 15 * SIZE(%ecx)
	movd	%mm4, 16 * SIZE(%ecx)
	movd	%mm4, 17 * SIZE(%ecx)
	movd	%mm4, 18 * SIZE(%ecx)
	movd	%mm4, 19 * SIZE(%ecx)
	movd	%mm5, 20 * SIZE(%ecx)
	movd	%mm5, 21 * SIZE(%ecx)
	movd	%mm5, 22 * SIZE(%ecx)
	movd	%mm5, 23 * SIZE(%ecx)
	movd	%mm6, 24 * SIZE(%ecx)
	movd	%mm6, 25 * SIZE(%ecx)
	movd	%mm6, 26 * SIZE(%ecx)
	movd	%mm6, 27 * SIZE(%ecx)
	movd	%mm7, 28 * SIZE(%ecx)
	movd	%mm7, 29 * SIZE(%ecx)
	movd	%mm7, 30 * SIZE(%ecx)
	movd	%mm7, 31 * SIZE(%ecx)
#endif
	addl	$ 8 * SIZE, %edi
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L42
	ALIGN_4

.L45:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L50
	ALIGN_4

.L46:

#ifdef HAVE_SSE2
	movss	 0 * SIZE(%edi), %xmm0
	movss	 1 * SIZE(%edi), %xmm1

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
#else
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1

	movd	%mm0,  0 * SIZE(%ecx)
	movd	%mm0,  1 * SIZE(%ecx)
	movd	%mm0,  2 * SIZE(%ecx)
	movd	%mm0,  3 * SIZE(%ecx)
	movd	%mm1,  4 * SIZE(%ecx)
	movd	%mm1,  5 * SIZE(%ecx)
	movd	%mm1,  6 * SIZE(%ecx)
	movd	%mm1,  7 * SIZE(%ecx)
#endif
	addl	$2 * SIZE, %edi
	addl	$8 * SIZE, %ecx
	decl	%eax
	jne	.L46
	ALIGN_4

.L50:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	16 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

#ifdef HAVE_3DNOW
	prefetchw	4 * SIZE(%esi)
	prefetchw	4 * SIZE(%esi, LDC)
#elif defined(HAVE_SSE) || defined(HAVE_SSE2)
	prefetcht2	4 * SIZE(%esi)
	prefetcht2	4 * SIZE(%esi, LDC)
#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$4, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L55
	ALIGN_4

.L52:
	mulps	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 8 * SIZE(AA), %xmm0

	mulps	%xmm0, %xmm3
	mulps	20 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	12 * SIZE(AA), %xmm0

	mulps	%xmm0, %xmm3
	mulps	28 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	32 * SIZE(AA), %xmm0

#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 16) * SIZE(AA)
#endif
	mulps	%xmm1, %xmm2
	mulps	36 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	40 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	20 * SIZE(AA), %xmm1

	mulps	%xmm1, %xmm2
	mulps	44 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	24 * SIZE(AA), %xmm1

	mulps	%xmm1, %xmm3
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1

	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	80 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	48 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$64 * SIZE, BB
	decl   %eax
	jne    .L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC, 1), %xmm1
	movhps	2 * SIZE(%esi, LDC, 1), %xmm1

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm5
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)
	movlps	%xmm5, 0 * SIZE(%esi, LDC, 1)
	movhps	%xmm5, 2 * SIZE(%esi, LDC, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L51
	ALIGN_4

.L60:
	testl	$2, M
	je	.L70

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	 8 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L65
	ALIGN_4

.L62:
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif

	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2

	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2

	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2

	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3

	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$64 * SIZE, BB
	decl   %eax
	jne    .L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5

#ifndef TRMMKERNEL
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(%esi), %xmm0
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	0 * SIZE(%esi, LDC, 1), %xmm1

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm5
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movlps	%xmm5, 0 * SIZE(%esi, LDC, 1)
	addl	$2 * SIZE, %esi		# coffset += 2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif
	ALIGN_4

.L70:
	testl	$1, M
	je	.L79

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movss	 0 * SIZE(AA), %xmm0
	movss	 4 * SIZE(AA), %xmm1
	movss	 0 * SIZE(BB), %xmm2
	movss	16 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$1, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L75
	ALIGN_4

.L72:
	mulss	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 2 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	20 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm4
	movss	24 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm5
	movss	 3 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	28 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm2
	mulss	36 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm4
	movss	40 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm5
	movss	 5 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm2
	mulss	44 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	64 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 6 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	52 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm4
	movss	56 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm5
	movss	 7 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	60 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	80 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl	$ 8 * SIZE, AA
	addl	$64 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0

	addl	$ 1 * SIZE, AA
	addl	$ 8 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	addss	%xmm6, %xmm4
	addss	%xmm7, %xmm5

	mulss	%xmm3, %xmm4
	mulss	%xmm3, %xmm5

#ifndef TRMMKERNEL
	movss	0 * SIZE(%esi), %xmm0
	movss	0 * SIZE(%esi, LDC, 1), %xmm1

	addss	%xmm0, %xmm4
	addss	%xmm1, %xmm5
#endif

	movss	%xmm4, 0 * SIZE(%esi)
	movss	%xmm5, 0 * SIZE(%esi, LDC, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leal	(, LDC, 2), %eax
	addl	%eax, C
	ALIGN_4

.L80:
	testl	$1, N
	je	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$3, %eax
	jle	.L85
	ALIGN_4

.L82:
	prefetchnta	80 * SIZE(%edi)

#if defined(OPTERON) || defined(BARCELONA)
	prefetchw	112 * SIZE(%ecx)
	prefetchw	120 * SIZE(%ecx)
#endif

#ifdef PENTIUM4
     	prefetcht1	112 * SIZE(%ecx)
#endif

#ifdef HAVE_SSE2
	movss	 0 * SIZE(%edi), %xmm0
	movss	 1 * SIZE(%edi), %xmm1
	movss	 2 * SIZE(%edi), %xmm2
	movss	 3 * SIZE(%edi), %xmm3
	movss	 4 * SIZE(%edi), %xmm4
	movss	 5 * SIZE(%edi), %xmm5
	movss	 6 * SIZE(%edi), %xmm6
	movss	 7 * SIZE(%edi), %xmm7

	shufps	 $0, %xmm0, %xmm0
	shufps	 $0, %xmm1, %xmm1
	shufps	 $0, %xmm2, %xmm2
	shufps	 $0, %xmm3, %xmm3
	shufps	 $0, %xmm4, %xmm4
	shufps	 $0, %xmm5, %xmm5
	shufps	 $0, %xmm6, %xmm6
	shufps	 $0, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)
#else
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3
	movd	 4 * SIZE(%edi), %mm4
	movd	 5 * SIZE(%edi), %mm5
	movd	 6 * SIZE(%edi), %mm6
	movd	 7 * SIZE(%edi), %mm7

	movd	%mm0,  0 * SIZE(%ecx)
	movd	%mm0,  1 * SIZE(%ecx)
	movd	%mm0,  2 * SIZE(%ecx)
	movd	%mm0,  3 * SIZE(%ecx)
	movd	%mm1,  4 * SIZE(%ecx)
	movd	%mm1,  5 * SIZE(%ecx)
	movd	%mm1,  6 * SIZE(%ecx)
	movd	%mm1,  7 * SIZE(%ecx)
	movd	%mm2,  8 * SIZE(%ecx)
	movd	%mm2,  9 * SIZE(%ecx)
	movd	%mm2, 10 * SIZE(%ecx)
	movd	%mm2, 11 * SIZE(%ecx)
	movd	%mm3, 12 * SIZE(%ecx)
	movd	%mm3, 13 * SIZE(%ecx)
	movd	%mm3, 14 * SIZE(%ecx)
	movd	%mm3, 15 * SIZE(%ecx)
	movd	%mm4, 16 * SIZE(%ecx)
	movd	%mm4, 17 * SIZE(%ecx)
	movd	%mm4, 18 * SIZE(%ecx)
	movd	%mm4, 19 * SIZE(%ecx)
	movd	%mm5, 20 * SIZE(%ecx)
	movd	%mm5, 21 * SIZE(%ecx)
	movd	%mm5, 22 * SIZE(%ecx)
	movd	%mm5, 23 * SIZE(%ecx)
	movd	%mm6, 24 * SIZE(%ecx)
	movd	%mm6, 25 * SIZE(%ecx)
	movd	%mm6, 26 * SIZE(%ecx)
	movd	%mm6, 27 * SIZE(%ecx)
	movd	%mm7, 28 * SIZE(%ecx)
	movd	%mm7, 29 * SIZE(%ecx)
	movd	%mm7, 30 * SIZE(%ecx)
	movd	%mm7, 31 * SIZE(%ecx)
#endif
	addl	$ 8 * SIZE, %edi
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L82
	ALIGN_4

.L85:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L90
	ALIGN_4

.L86:

#ifdef HAVE_SSE2
	movss	 0 * SIZE(%edi), %xmm0
	shufps	 $0, %xmm0, %xmm0
	movaps	%xmm0,  0 * SIZE(%ecx)
#else
	movd	 0 * SIZE(%edi), %mm0
	movd	%mm0,  0 * SIZE(%ecx)
	movd	%mm0,  1 * SIZE(%ecx)
	movd	%mm0,  2 * SIZE(%ecx)
	movd	%mm0,  3 * SIZE(%ecx)
#endif
	addl	$1 * SIZE, %edi
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L86
	ALIGN_4

.L90:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L100
	ALIGN_4

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	16 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

#ifdef HAVE_3DNOW
	prefetchw	4 * SIZE(%esi)
#elif defined(HAVE_SSE) || defined(HAVE_SSE2)
	prefetcht2	4 * SIZE(%esi)
#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$4, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L95
	ALIGN_4

.L92:
	mulps	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm5
	movaps	 8 * SIZE(AA), %xmm0
	mulps	 8 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm6
	movaps	12 * SIZE(AA), %xmm0
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 16) * SIZE(AA)
#endif
	mulps	%xmm1, %xmm3
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movaps	48 * SIZE(BB), %xmm3
	mulps	20 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm5
	movaps	24 * SIZE(AA), %xmm1
	mulps	24 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm6
	movaps	28 * SIZE(AA), %xmm1
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm7
	movaps	48 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
	jne    .L92
	ALIGN_4

.L95:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L98
	ALIGN_4

.L96:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_4

.L98:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
	addps	%xmm6, %xmm4
	mulps	%xmm3, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0

	addps	%xmm0, %xmm4
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L91
	ALIGN_4

.L100:
	testl	$2, M
	je	.L110

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	 8 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L105
	ALIGN_4

.L102:
	mulps	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
	jne    .L102
	ALIGN_4

.L105:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L108
	ALIGN_4

.L106:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L106
	ALIGN_4

.L108:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
	addps	%xmm6, %xmm4

	mulps	%xmm3, %xmm4

#ifndef TRMMKERNEL
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(%esi), %xmm0

	addps	%xmm0, %xmm4
#endif
	movlps	%xmm4, 0 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 2
	ALIGN_4

.L110:
	testl	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movss	 0 * SIZE(AA), %xmm0
	movss	 4 * SIZE(AA), %xmm1
	movss	 0 * SIZE(BB), %xmm2
	movss	16 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$1, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L115
	ALIGN_4

.L112:
	mulss	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss	32 * SIZE(BB), %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm5
	movss	 2 * SIZE(AA), %xmm0
	mulss	 8 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm6
	movss	 3 * SIZE(AA), %xmm0
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm3
	movss	 5 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm4
	movss	48 * SIZE(BB), %xmm3
	mulss	20 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm5
	movss	 6 * SIZE(AA), %xmm1
	mulss	24 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm6
	movss	 7 * SIZE(AA), %xmm1
	mulss	28 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl	$ 8 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
	jne    .L112
	ALIGN_4

.L115:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulss	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss	 4 * SIZE(BB), %xmm2

	addl	$ 1 * SIZE, AA
	addl	$ 4 * SIZE, BB
	decl	%eax
	jg	.L116
	ALIGN_4

.L118:
	addss	%xmm5, %xmm4
	addss	%xmm7, %xmm6
	addss	%xmm6, %xmm4
	mulss	%xmm3, %xmm4
#ifndef TRMMKERNEL
	movss	0 * SIZE(%esi), %xmm0
	addss	%xmm0, %xmm4
#endif
	movss	%xmm4, 0 * SIZE(%esi)
	ALIGN_4

.L999:
	EMMS

	movl	OLD_STACK, %esp
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
