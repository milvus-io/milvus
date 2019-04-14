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
#define OLD_ALPHA_R	16 + STACK + ARGS(%esi)
#define OLD_ALPHA_I	20 + STACK + ARGS(%esi)
#define OLD_A	24 + STACK + ARGS(%esi)
#define OLD_B	28 + STACK + ARGS(%esi)
#define OLD_C	32 + STACK + ARGS(%esi)
#define OLD_LDC	36 + STACK + ARGS(%esi)
#define OLD_OFFSET 40 + STACK + ARGS(%esi)

#define GAMMA_R  0(%esp)
#define GAMMA_I  8(%esp)
#define ALPHA	16(%esp)
#define K	24(%esp)
#define N	28(%esp)
#define M	32(%esp)
#define A	36(%esp)
#define C	40(%esp)
#define J	44(%esp)
#define OLD_STACK 48(%esp)
#define OFFSET	52(%esp)
#define KK	56(%esp)
#define KKK	60(%esp)
#define BUFFER 128(%esp)

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

	movl	%esp, %esi	# save old stack
	subl	$128 + LOCAL_BUFFER_SIZE, %esp
	movl	OLD_M, %ebx
	andl	$-1024, %esp	# align stack

	STACK_TOUCHING

	movl	OLD_N, %eax
	movl	OLD_K, %ecx
	movl	OLD_A, %edx

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	subl	$AOFFSET * SIZE, %edx
	movl	%edx, A
	movl	%esi, OLD_STACK

	testl	%ebx, %ebx
	jle	.L999

	movl	OLD_B, %edi
	movl	OLD_C, %ebx

	EMMS

	movd	OLD_ALPHA_R, %mm0
	movd	OLD_ALPHA_I, %mm1

	movd	%mm0, 0 + ALPHA
	movd	%mm1, 4 + ALPHA

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	movl	 $0x3f800000,  0 + GAMMA_R
	movl	 $0x3f800000,  4 + GAMMA_R
	movl	 $0xbf800000,  0 + GAMMA_I
	movl	 $0x3f800000,  4 + GAMMA_I
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	movl	 $0x3f800000,  0 + GAMMA_R
	movl	 $0x3f800000,  4 + GAMMA_R
	movl	 $0x3f800000,  0 + GAMMA_I
	movl	 $0xbf800000,  4 + GAMMA_I
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	movl	 $0x3f800000,  0 + GAMMA_R
	movl	 $0xbF800000,  4 + GAMMA_R
	movl	 $0x3f800000,  0 + GAMMA_I
	movl	 $0x3F800000,  4 + GAMMA_I
#else
	movl	 $0x3f800000,  0 + GAMMA_R
	movl	 $0xbf800000,  4 + GAMMA_R
	movl	 $0xbf800000,  0 + GAMMA_I
	movl	 $0xbf800000,  4 + GAMMA_I
#endif
	movl	%ebx, C
	movl	OLD_LDC, %ebp
	leal	(, %ebp, SIZE * 2), %ebp

#ifdef TRMMKERNEL
	movl	OLD_OFFSET, %eax
	movl	%eax, OFFSET
#ifndef LEFT
	negl	%eax
	movl	%eax, KK
#endif
#endif

	movl	N, %eax
	sarl	$1, %eax
	movl	%eax, J			# j = n
	jle	.L20
	ALIGN_4

.L01:
/* Copying to Sub Buffer */
	leal	BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K,  %eax
	sarl	$2, %eax
	jle	.L03
	ALIGN_4

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

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)
	movq	%mm4,  8 * SIZE(BB)
	movq	%mm5, 10 * SIZE(BB)
	movq	%mm6, 12 * SIZE(BB)
	movq	%mm7, 14 * SIZE(BB)

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

	movq	%mm0, 16 * SIZE(BB)
	movq	%mm1, 18 * SIZE(BB)
	movq	%mm2, 20 * SIZE(BB)
	movq	%mm3, 22 * SIZE(BB)
	movq	%mm4, 24 * SIZE(BB)
	movq	%mm5, 26 * SIZE(BB)
	movq	%mm6, 28 * SIZE(BB)
	movq	%mm7, 30 * SIZE(BB)

	addl	$16 * SIZE, %edi
	addl	$32 * SIZE, BB
	decl	%eax
	jne	.L02
	ALIGN_4

.L03:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L10
	ALIGN_4

.L04:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1
	movd	 2 * SIZE(%edi), %mm2
	movd	 3 * SIZE(%edi), %mm3

	punpckldq %mm0, %mm0
	punpckldq %mm1, %mm1
	punpckldq %mm2, %mm2
	punpckldq %mm3, %mm3

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)

	addl	$4 * SIZE, %edi
	addl	$8 * SIZE, BB
	decl	%eax
	jne	.L04
	ALIGN_4

.L10:
	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
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
	addl	$1, %eax
#else
	addl	$2, %eax
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
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax		# if (k & 1)
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
	ALIGN_4

.L18:
	movq	GAMMA_R, %mm0
	movq	GAMMA_I, %mm1
	movq	ALPHA,   %mm2

	pswapd	%mm5, %mm5
	pswapd	%mm7, %mm7

	pfmul	%mm0, %mm4
	pfmul	%mm1, %mm5
	pfmul	%mm0, %mm6
	pfmul	%mm1, %mm7

	pfadd	%mm5, %mm4
	pfadd	%mm7, %mm6

	pswapd	%mm4, %mm5
	pswapd	%mm6, %mm7
	pfmul	%mm2, %mm4
	pfmul	%mm2, %mm6
	pfmul	%mm2, %mm5
	pfmul	%mm2, %mm7

	pfpnacc	%mm5, %mm4
	pfpnacc	%mm7, %mm6

#ifndef TRMMKERNEL
	pfadd	(%esi), %mm4
	pfadd	(%esi, %ebp), %mm6
#endif
	movq	%mm4, (%esi)
	movq	%mm6, (%esi, %ebp)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, %esi
	decl	%ebx
	jg	.L11
	ALIGN_4

.L19:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, %ebp, 2), %eax
	addl	%eax, C			# c += ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L20:
	movl	N, %eax
	andl	$1, %eax
	jle	.L999
	ALIGN_4

.L21:
/* Copying to Sub Buffer */
	movl	K, %eax
	leal	BUFFER, BB
	sarl	$2, %eax
	jle	.L25
	ALIGN_4

.L22:
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

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)
	movq	%mm4,  8 * SIZE(BB)
	movq	%mm5, 10 * SIZE(BB)
	movq	%mm6, 12 * SIZE(BB)
	movq	%mm7, 14 * SIZE(BB)

	addl	$ 8 * SIZE, %edi
	addl	$16 * SIZE, BB
	decl	%eax
	jne	.L22
	ALIGN_4

.L25:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L30
	ALIGN_4

.L26:
	movd	 0 * SIZE(%edi), %mm0
	movd	 1 * SIZE(%edi), %mm1

	movd	%mm0,  0 * SIZE(BB)
	movd	%mm0,  1 * SIZE(BB)
	movd	%mm1,  2 * SIZE(BB)
	movd	%mm1,  3 * SIZE(BB)

	addl	$2 * SIZE, %edi
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L26
	ALIGN_4

.L30:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	ALIGN_3

.L31:
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
	je	.L35
	ALIGN_4

.L32:
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

	subl   $-32 * SIZE, AA
	addl   $ 64 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_3

.L35:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_3

.L36:
	pfmul	%mm0, %mm2
	pfadd	%mm2, %mm4
	PADDING movq	(  4 + BOFFSET) * SIZE(BB), %mm2

	pfmul	(  2 + BOFFSET) * SIZE(BB), %mm0
	pfadd	%mm0, %mm5
	movq	        (  2 + AOFFSET) * SIZE(AA), %mm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	pfadd	%mm6, %mm4
	pfadd	%mm7, %mm5

	movq	ALPHA,   %mm2
	pswapd	%mm5, %mm5

	pfmul	GAMMA_R, %mm4
	pfmul	GAMMA_I, %mm5

	pfadd	%mm5, %mm4

	pswapd	%mm4, %mm5
	pfmul	%mm2, %mm4
	pfmul	%mm2, %mm5
	pfpnacc	%mm5, %mm4

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
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	jg	.L31
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
