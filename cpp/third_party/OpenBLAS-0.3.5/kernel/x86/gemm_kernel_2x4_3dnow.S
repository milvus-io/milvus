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

#define OLD_M	 4 + STACK + ARGS(%esi)
#define OLD_N	 8 + STACK + ARGS(%esi)
#define OLD_K	12 + STACK + ARGS(%esi)
#define OLD_ALPHA	16 + STACK + ARGS(%esi)
#define OLD_A	20 + STACK + ARGS(%esi)
#define OLD_B	24 + STACK + ARGS(%esi)
#define OLD_C	28 + STACK + ARGS(%esi)
#define OLD_LDC	32 + STACK + ARGS(%esi)
#define OLD_OFFSET 36 + STACK + ARGS(%esi)

#define ALPHA	 0(%esp)
#define K	 8(%esp)
#define N	12(%esp)
#define M	16(%esp)
#define A	20(%esp)
#define C	24(%esp)
#define J	28(%esp)
#define OLD_STACK 32(%esp)
#define OFFSET	36(%esp)
#define KK	40(%esp)
#define KKK	44(%esp)
#define BUFFER  64(%esp)

#define AA	%edx
#define BB	%ecx

#define PREFETCHSIZE (16 * 2 + 6)

#define AOFFSET -32
#define BOFFSET 128

/*

  A hint of scheduling is received from following URL

https://sourceforge.net/mailarchive/forum.php?forum_id=426&max_rows=25&style=flat&viewmonth=200309&viewday=11

*/

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
	movd	OLD_ALPHA,  %mm3

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	subl	$AOFFSET * SIZE, %edx
	movl	%edx, A
	movl	%esi, OLD_STACK

	movl	OLD_B, %edi
	movl	OLD_C, %ebx
	punpckldq %mm3, %mm3

	movq	 %mm3, ALPHA

	movl	%ebx, C
	movl	OLD_LDC, %ebp
	leal	(, %ebp, SIZE), %ebp

#ifdef TRMMKERNEL
	movl	OLD_OFFSET, %eax
	movl	%eax, OFFSET
#ifndef LEFT
	negl	%eax
	movl	%eax, KK
#endif
#endif

	movl	N, %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_3

.L01:
/* Copying to Sub Buffer */
	leal	BUFFER, %ecx

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$2, %eax
	jle	.L03
	ALIGN_3

.L02:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3
	movd	 4 * SIZE(%edi), %mm4
	movd	 5 * SIZE(%edi), %mm5
	movd	 6 * SIZE(%edi), %mm6
	movd	 7 * SIZE(%edi), %mm7

	prefetchnta	72 * SIZE(%edi)

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm4,  8 * SIZE(%ecx)
	movq	%mm5, 10 * SIZE(%ecx)
	movq	%mm6, 12 * SIZE(%ecx)
	movq	%mm7, 14 * SIZE(%ecx)

	movd	 8 * SIZE(%edi), %mm0
	movd	 9 * SIZE(%edi), %mm1
	movd	10 * SIZE(%edi), %mm2
	movd	11 * SIZE(%edi), %mm3
	movd	12 * SIZE(%edi), %mm4
	movd	13 * SIZE(%edi), %mm5
	movd	14 * SIZE(%edi), %mm6
	movd	15 * SIZE(%edi), %mm7

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0, 16 * SIZE(%ecx)
	movq	%mm1, 18 * SIZE(%ecx)
	movq	%mm2, 20 * SIZE(%ecx)
	movq	%mm3, 22 * SIZE(%ecx)
	movq	%mm4, 24 * SIZE(%ecx)
	movq	%mm5, 26 * SIZE(%ecx)
	movq	%mm6, 28 * SIZE(%ecx)
	movq	%mm7, 30 * SIZE(%ecx)

	addl	$16 * SIZE, %edi
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L02

.L03:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L10
	ALIGN_2

.L04:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	addl	$4 * SIZE, %edi
	addl	$8 * SIZE, %ecx
	decl	%eax
	jne	.L04
	ALIGN_4

.L10:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
	leal	- BOFFSET * SIZE + BUFFER, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

	movq	        (  0 + AOFFSET) * SIZE(AA), %mm0
	pxor	%mm4, %mm4
	movq	        ( 16 + AOFFSET) * SIZE(AA), %mm1
	pxor	%mm5, %mm5
	PADDING movq	(  0 + BOFFSET) * SIZE(BB), %mm2
	pxor	%mm6, %mm6
	PADDING movq	( 16 + BOFFSET) * SIZE(BB), %mm3
	pxor	%mm7, %mm7

	leal	(%ebp, %ebp, 2), %eax

	prefetchw 2 * SIZE(%esi)
	prefetchw 2 * SIZE(%esi, %ebp)
	prefetchw 2 * SIZE(%esi, %ebp, 2)
	prefetchw 2 * SIZE(%esi, %eax)

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	je	.L15
	ALIGN_4

.L12:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	(  2 + BOFFSET) * SIZE(BB), %mm2

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	(  4 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING prefetch	(PREFETCHSIZE +  0) * SIZE(AA)

	PADDING movq	(  8 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 10 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	( 12 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 32 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 14 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  4 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 18 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	( 20 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 24 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 22 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  6 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 26 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	( 28 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 48 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 30 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  8 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 34 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	( 36 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 40 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 38 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 10 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 42 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	( 44 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 64 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 46 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 12 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 50 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	( 52 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 56 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 54 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 14 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 58 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	( 60 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 80 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 62 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 32 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 66 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	( 68 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 72 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 70 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 18 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 74 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	( 76 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 96 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 78 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 20 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 82 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	( 84 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 88 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 86 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 22 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 90 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	( 92 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	(112 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 94 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 24 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 98 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	(100 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	(104 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (102 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 26 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	(106 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	(108 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	(128 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (110 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 28 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	(114 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	(116 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	(120 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        (118 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 30 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	(122 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movq	(124 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	(144 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        (126 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 48 + AOFFSET) * SIZE(AA), %mm1

	subl	$-32 * SIZE, AA
	addl	$128 * SIZE, BB
	decl	%eax
	jne    .L12
	ALIGN_3

.L15:
	movq	ALPHA,  %mm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax
	BRANCH
	je .L18
	ALIGN_3

.L16:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	(  2 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movq	(  4 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	(  8 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_3

.L18:
	leal	(%ebp, %ebp, 2), %eax

#ifndef TRMMKERNEL
	pfmul	%mm3, %mm4
	pfadd	0 * SIZE(%esi), %mm4
	pfmul	%mm3, %mm5
	pfadd	0 * SIZE(%esi, %ebp, 1), %mm5
	pfmul	%mm3, %mm6
	pfadd	0 * SIZE(%esi, %ebp, 2), %mm6
	pfmul	%mm3, %mm7
	pfadd	0 * SIZE(%esi, %eax, 1), %mm7
#else
	pfmul	%mm3, %mm4
	pfmul	%mm3, %mm5
	pfmul	%mm3, %mm6
	pfmul	%mm3, %mm7
#endif

	movq	%mm4, 0 * SIZE(%esi)
	movq	%mm5, 0 * SIZE(%esi, %ebp, 1)
	movq	%mm6, 0 * SIZE(%esi, %ebp, 2)
	movq	%mm7, 0 * SIZE(%esi, %eax, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L29
	ALIGN_4

.L21:
	leal	- BOFFSET * SIZE + BUFFER, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

	movq	        (  0 + AOFFSET) * SIZE(AA), %mm0
	pxor	%mm4, %mm4
	movq	        (  8 + AOFFSET) * SIZE(AA), %mm1
	pxor	%mm5, %mm5
	PADDING movq	(  0 + BOFFSET) * SIZE(BB), %mm2
	pxor	%mm6, %mm6
	PADDING movq	( 16 + BOFFSET) * SIZE(BB), %mm3
	pxor	%mm7, %mm7

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	je	.L25
	ALIGN_4

.L22:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	(  2 + BOFFSET) * SIZE(BB), %mm2

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	(  4 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING prefetch	(PREFETCHSIZE +  0) * SIZE(AA)

	PADDING movd	(  8 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  1 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 10 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	( 12 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 32 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 14 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  2 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 18 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	( 20 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 24 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 22 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  3 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 26 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	( 28 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 48 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 30 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  4 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 34 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	( 36 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 40 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 38 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  5 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 42 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	( 44 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 64 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 46 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  6 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 50 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	( 52 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 56 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 54 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  7 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 58 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	( 60 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 80 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 62 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        ( 16 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 66 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	( 68 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 72 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 70 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        (  9 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 74 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	( 76 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 96 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        ( 78 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 10 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 82 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	( 84 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 88 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 86 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 11 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 90 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	( 92 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	(112 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        ( 94 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 12 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 98 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	(100 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	(104 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (102 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 13 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	(106 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	(108 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	(128 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (110 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 14 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	(114 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	(116 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	(120 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        (118 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 15 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	(122 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm5
	PADDING movd	(124 + BOFFSET) * SIZE(BB), %mm3
	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	(144 + BOFFSET) * SIZE(BB), %mm3
	pfmul	        (126 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 24 + AOFFSET) * SIZE(AA), %mm1

	subl	$-16 * SIZE, AA
	addl	$128 * SIZE, BB
	decl	%eax
	jne    .L22
	ALIGN_3

.L25:
	movd	ALPHA,  %mm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax
	BRANCH
	je .L28
	ALIGN_3

.L26:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	(  2 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm5
	PADDING movd	(  4 + BOFFSET) * SIZE(BB), %mm2
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	(  8 + BOFFSET) * SIZE(BB), %mm2
	pfmul	        (  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  1 + AOFFSET) * SIZE(AA), %mm0

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_3

.L28:
	leal	(%ebp, %ebp, 2), %eax

	pfmul	%mm3, %mm4
	pfmul	%mm3, %mm5
	pfmul	%mm3, %mm6
	pfmul	%mm3, %mm7

#ifndef TRMMKERNEL
	movd	0 * SIZE(%esi)         , %mm0
	movd	0 * SIZE(%esi, %ebp, 1), %mm1
	movd	0 * SIZE(%esi, %ebp, 2), %mm2
	movd	0 * SIZE(%esi, %eax, 1), %mm3

	pfadd	%mm0, %mm4
	pfadd	%mm1, %mm5
	pfadd	%mm2, %mm6
	pfadd	%mm3, %mm7
#endif

	movd	%mm4, 0 * SIZE(%esi)
	movd	%mm5, 0 * SIZE(%esi, %ebp, 1)
	movd	%mm6, 0 * SIZE(%esi, %ebp, 2)
	movd	%mm7, 0 * SIZE(%esi, %eax, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leal	(, %ebp, 4), %eax
	addl	%eax, C			# c += 4 * ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L30:
	movl	N, %eax
	testl	$2, %eax
	jle	.L60
	ALIGN_3

.L31:
/* Copying to Sub Buffer */
	leal	BUFFER, %ecx

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$2, %eax
	jle	.L33
	ALIGN_3

.L32:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3
	movd	 4 * SIZE(%edi), %mm4
	movd	 5 * SIZE(%edi), %mm5
	movd	 6 * SIZE(%edi), %mm6
	movd	 7 * SIZE(%edi), %mm7

	prefetchnta	72 * SIZE(%edi)

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm4,  8 * SIZE(%ecx)
	movq	%mm5, 10 * SIZE(%ecx)
	movq	%mm6, 12 * SIZE(%ecx)
	movq	%mm7, 14 * SIZE(%ecx)

	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, %ecx
	decl	%eax
	jne	.L32

.L33:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L40
	ALIGN_2

.L34:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)

	addl	$2 * SIZE, %edi
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L34
	ALIGN_4

.L40:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L41:
	leal	- BOFFSET * SIZE + BUFFER, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	movq	        (  0 + AOFFSET) * SIZE(AA), %mm0
	pxor	%mm4, %mm4
	movq	        ( 16 + AOFFSET) * SIZE(AA), %mm1
	pxor	%mm5, %mm5
	PADDING movq	(  0 + BOFFSET) * SIZE(BB), %mm2
	pxor	%mm6, %mm6
	PADDING movq	( 16 + BOFFSET) * SIZE(BB), %mm3
	pxor	%mm7, %mm7

	prefetchw 2 * SIZE(%esi)
	prefetchw 2 * SIZE(%esi, %ebp)

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	je	.L45
	ALIGN_4

.L42:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	(  4 + BOFFSET) * SIZE(BB), %mm2
	pfmul	(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	PADDING prefetch	(PREFETCHSIZE +  0) * SIZE(AA)

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	(  8 + BOFFSET) * SIZE(BB), %mm2
	pfmul	(  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  4 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 12 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 10 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        (  6 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 32 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 14 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  8 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 20 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 18 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        ( 10 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 24 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 22 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 12 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 28 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 26 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        ( 14 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 48 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 30 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 32 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 36 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 34 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movq	        ( 18 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 40 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 38 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 20 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	( 44 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 42 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movq	        ( 22 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movq	( 64 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 46 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 24 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 52 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 50 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movq	        ( 26 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 56 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 54 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 28 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movq	( 60 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 58 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movq	        ( 30 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movq	( 80 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 62 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 48 + AOFFSET) * SIZE(AA), %mm1

	subl	$-32 * SIZE, AA
	addl	$ 64 * SIZE, BB
	decl	%eax
	jne    .L42
	ALIGN_3

.L45:
	movq	ALPHA,  %mm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax
	BRANCH
	je .L48
	ALIGN_3

.L46:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	(  4 + BOFFSET) * SIZE(BB), %mm2

	pfmul	(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_3

.L48:
	pfadd	%mm6, %mm4
	pfadd	%mm7, %mm5

	pfmul	%mm3, %mm4
	pfmul	%mm3, %mm5

#ifndef TRMMKERNEL
	pfadd	0 * SIZE(%esi), %mm4
	pfadd	0 * SIZE(%esi, %ebp, 1), %mm5
#endif

	movq	%mm4, 0 * SIZE(%esi)
	movq	%mm5, 0 * SIZE(%esi, %ebp, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L41
	ALIGN_4

.L50:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L59
	ALIGN_4

.L51:
	leal	- BOFFSET * SIZE + BUFFER, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

	movq	        (  0 + AOFFSET) * SIZE(AA), %mm0
	pxor	%mm4, %mm4
	movq	        (  8 + AOFFSET) * SIZE(AA), %mm1
	pxor	%mm5, %mm5
	PADDING movq	(  0 + BOFFSET) * SIZE(BB), %mm2
	pxor	%mm6, %mm6
	PADDING movq	( 16 + BOFFSET) * SIZE(BB), %mm3
	pxor	%mm7, %mm7

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	je	.L55
	ALIGN_4

.L52:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	(  4 + BOFFSET) * SIZE(BB), %mm2
	pfmul	(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  1 + AOFFSET) * SIZE(AA), %mm0

	PADDING prefetch	(PREFETCHSIZE +  0) * SIZE(AA)

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	(  8 + BOFFSET) * SIZE(BB), %mm2
	pfmul	(  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  2 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 12 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 10 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  3 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 32 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 14 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  4 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 20 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 18 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  5 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 24 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 22 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  6 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 28 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 26 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  7 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm0, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 48 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 30 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        ( 16 + AOFFSET) * SIZE(AA), %mm0

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 36 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 34 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movd	        (  9 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 40 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 38 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 10 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	( 44 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 42 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movd	        ( 11 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm2
	pfadd	%mm2, %mm6
	PADDING movd	( 64 + BOFFSET) * SIZE(BB), %mm2
	pfmul	( 46 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 12 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 52 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 50 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movd	        ( 13 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 56 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 54 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 14 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm4
	PADDING movd	( 60 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 58 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movd	        ( 15 + AOFFSET) * SIZE(AA), %mm1

	pfmul	%mm1, %mm3
	pfadd	%mm3, %mm6
	PADDING movd	( 80 + BOFFSET) * SIZE(BB), %mm3
	pfmul	( 62 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 24 + AOFFSET) * SIZE(AA), %mm1

	subl	$-16 * SIZE, AA
	addl	$ 64 * SIZE, BB
	decl	%eax
	jne    .L52
	ALIGN_3

.L55:
	movd	ALPHA,  %mm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax
	BRANCH
	je .L58
	ALIGN_3

.L56:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movd	(  4 + BOFFSET) * SIZE(BB), %mm2

	pfmul	(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  1 + AOFFSET) * SIZE(AA), %mm0

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_3

.L58:
	pfadd	%mm6, %mm4
	pfadd	%mm7, %mm5

	pfmul	%mm3, %mm4
	pfmul	%mm3, %mm5

#ifndef TRMMKERNEL
	movd	0 * SIZE(%esi)         , %mm0
	movd	0 * SIZE(%esi, %ebp, 1), %mm1

	pfadd	%mm0, %mm4
	pfadd	%mm1, %mm5
#endif

	movd	%mm4, 0 * SIZE(%esi)
	movd	%mm5, 0 * SIZE(%esi, %ebp, 1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L59:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, %ebp, 2), %eax
	addl	%eax, C			# c += 4 * ldc
	ALIGN_4

.L60:
	movl	N, %eax
	testl	$1, %eax
	jle	.L999
	ALIGN_3

.L61:
/* Copying to Sub Buffer */
	leal	BUFFER, %ecx

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$3, %eax
	jle	.L63
	ALIGN_3

.L62:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3
	movd	 4 * SIZE(%edi), %mm4
	movd	 5 * SIZE(%edi), %mm5
	movd	 6 * SIZE(%edi), %mm6
	movd	 7 * SIZE(%edi), %mm7

	prefetchnta	72 * SIZE(%edi)

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3
	punpckldq %mm4, %mm4
	punpckldq %mm5, %mm5
	punpckldq %mm6, %mm6
	punpckldq %mm7, %mm7

	movq	%mm0,  0 * SIZE(%ecx)
	movq	%mm1,  2 * SIZE(%ecx)
	movq	%mm2,  4 * SIZE(%ecx)
	movq	%mm3,  6 * SIZE(%ecx)
	movq	%mm4,  8 * SIZE(%ecx)
	movq	%mm5, 10 * SIZE(%ecx)
	movq	%mm6, 12 * SIZE(%ecx)
	movq	%mm7, 14 * SIZE(%ecx)

	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, %ecx
	decl	%eax
	jne	.L62

.L63:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L70
	ALIGN_2

.L64:
	movd	 0 * SIZE(%edi), %mm0
	punpckldq %mm0, %mm0
	movq	%mm0,  0 * SIZE(%ecx)

	addl	$1 * SIZE, %edi
	addl	$2 * SIZE, %ecx
	decl	%eax
	jne	.L64
	ALIGN_4

.L70:
	movl	C, %esi		# coffset = c
	movl	A, %edx		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L90
	ALIGN_4

.L71:
	leal	- BOFFSET * SIZE + BUFFER, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	movq	        (  0 + AOFFSET) * SIZE(AA), %mm0
	pxor	%mm4, %mm4
	movq	        ( 16 + AOFFSET) * SIZE(AA), %mm1
	pxor	%mm5, %mm5
	PADDING movq	(  0 + BOFFSET) * SIZE(BB), %mm2
	pxor	%mm6, %mm6
	pxor	%mm7, %mm7

	prefetchw 2 * SIZE(%esi)
	prefetchw 2 * SIZE(%esi, %ebp)

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	je	.L75
	ALIGN_4

.L72:
	pfmul		(  0 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm4
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	PADDING prefetch	(PREFETCHSIZE +  0) * SIZE(AA)

	pfmul		(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        (  4 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  4 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm6
	movq	        (  6 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        (  8 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  8 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm4
	movq	        ( 10 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 10 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        ( 12 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 12 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm6
	movq	        ( 14 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 14 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movq	        ( 32 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 16 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm4
	movq	        ( 18 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 18 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movq	        ( 20 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 20 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm6
	movq	        ( 22 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 22 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 24 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 24 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm4
	movq	        ( 26 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 26 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movq	        ( 28 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 28 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm6
	movq	        ( 30 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 30 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movq	        ( 48 + AOFFSET) * SIZE(AA), %mm1

	subl	$-32 * SIZE, AA
	addl	$ 32 * SIZE, BB
	decl	%eax
	jne    .L72
	ALIGN_3

.L75:
	movq	ALPHA,  %mm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax
	BRANCH
	je .L78
	ALIGN_3

.L76:
	pfmul		(  0 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm4
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_3

.L78:
	pfadd	%mm5, %mm4
	pfadd	%mm7, %mm6
	pfadd	%mm6, %mm4

	pfmul	%mm3, %mm4
#ifndef TRMMKERNEL
	pfadd	0 * SIZE(%esi), %mm4
#endif
	movq	%mm4, 0 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 2
	decl	%ebx			# i --
	jg	.L71
	ALIGN_4

.L90:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L999
	ALIGN_4

.L91:
	leal	- BOFFSET * SIZE + BUFFER, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

	movq	        (  0 + AOFFSET) * SIZE(AA), %mm0
	pxor	%mm4, %mm4
	movq	        (  8 + AOFFSET) * SIZE(AA), %mm1
	pxor	%mm5, %mm5
	PADDING movq	(  0 + BOFFSET) * SIZE(BB), %mm2
	pxor	%mm6, %mm6
	pxor	%mm7, %mm7

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	je	.L95
	ALIGN_4

.L92:
	PADDING prefetch	(PREFETCHSIZE +  0) * SIZE(AA)

	pfmul		(  0 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm4
	movd	        (  1 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  2 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  4 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm6
	movd	        (  3 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  6 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        (  4 + AOFFSET) * SIZE(AA), %mm0

	pfmul		(  8 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm4
	movd	        (  5 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 10 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movd	        (  6 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 12 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm6
	movd	        (  7 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 14 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm7
	movd	        ( 16 + AOFFSET) * SIZE(AA), %mm0

	pfmul		( 16 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm4
	movd	        (  9 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 18 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movd	        ( 10 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 20 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm6
	movd	        ( 11 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 22 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 12 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 24 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm4
	movd	        ( 13 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 26 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm5
	movd	        ( 14 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 28 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm6
	movd	        ( 15 + AOFFSET) * SIZE(AA), %mm1

	pfmul		( 30 + BOFFSET) * SIZE(BB), %mm1
	pfadd	%mm1, %mm7
	movd	        ( 24 + AOFFSET) * SIZE(AA), %mm1

	subl	$-16 * SIZE, AA
	addl	$ 32 * SIZE, BB
	decl	%eax
	jne    .L92
	ALIGN_3

.L95:
	movd	ALPHA,  %mm3
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax
	BRANCH
	je .L98
	ALIGN_3

.L96:
	pfmul		(  0 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm4
	movd	        (  1 + AOFFSET) * SIZE(AA), %mm0

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_3

.L98:
#ifndef TRMMKERNEL
	movd	0 * SIZE(%esi), %mm0
#endif

	pfadd	%mm5, %mm4
	pfadd	%mm7, %mm6
	pfadd	%mm6, %mm4

	pfmul	%mm3, %mm4
	pfmul	%mm3, %mm5

#ifndef TRMMKERNEL
	pfadd	%mm0, %mm4
#endif
	movd	%mm4, 0 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
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
