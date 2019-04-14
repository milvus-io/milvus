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

/*********************************************************************
* 2013/10/20 Saar
*        BLASTEST               : OK
*        CTEST                  : OK
*        TEST                   : OK

*
*
* 2013/10/20 Saar
* Parameter:
*       DGEMM_DEFAULT_UNROLL_N  2
*       DGEMM_DEFAULT_UNROLL_M  16
*       DGEMM_DEFAULT_P         192
*       DGEMM_DEFAULT_Q         128
*	A_PR1			512
*
*
* Performance without prefetch of B:
*       1 thread:       45.8 GFLOPS (MKL:  45)
*       2 threads:      80.0 GFLOPS (MKL:  91)
*       4 threads:     135.0 GFLOPS (MKL: 135)
*********************************************************************/


#define ASSEMBLER
#include "common.h"
 
#define OLD_M	%rdi
#define OLD_N	%rsi
#define M	%r13
#define J	%r14
#define OLD_K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10
	
#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%r15
#define K	%r12
#define BI	%rbp
#define	SP	%rbx

#define BO1	%rdi
#define BO2	%r15

#ifndef WINDOWS_ABI

#define STACKSIZE 96

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define L_BUFFER_SIZE 512*8*4
#define LB2_OFFSET    512*8*2

#define Ndiv6	 24(%rsp)
#define Nmod6	 32(%rsp)
#define N	 40(%rsp)
#define ALPHA	 48(%rsp)
#define OFFSET	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)
#define BUFFER1	           128(%rsp)
#define BUFFER2	LB2_OFFSET+128(%rsp)

#if defined(OS_WINDOWS)
#if   L_BUFFER_SIZE > 16384
#define STACK_TOUCH \
        movl    $0,  4096 * 4(%rsp);\
        movl    $0,  4096 * 3(%rsp);\
        movl    $0,  4096 * 2(%rsp);\
        movl    $0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 12288
#define STACK_TOUCH \
        movl    $0,  4096 * 3(%rsp);\
        movl    $0,  4096 * 2(%rsp);\
        movl    $0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 8192
#define STACK_TOUCH \
        movl    $0,  4096 * 2(%rsp);\
        movl    $0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 4096
#define STACK_TOUCH \
        movl    $0,  4096 * 1(%rsp);
#else
#define STACK_TOUCH
#endif
#else
#define STACK_TOUCH
#endif

#if defined(BULLDOZER)

.macro VFMADD231PD_ y0,y1,y2
	vfmaddpd \y0,\y1,\y2,\y0
.endm

.macro VFMADD231SD_ x0,x1,x2
	vfmaddsd \x0,\x1,\x2,\x0
.endm

#else

.macro VFMADD231PD_ y0,y1,y2
	vfmadd231pd \y2,\y1,\y0
.endm

.macro VFMADD231SD_ x0,x1,x2
	vfmadd231sd \x2,\x1,\x0
.endm

#endif


#define	A_PR1	1024
#define	B_PR1	256

/*******************************************************************************************
* 3 lines of N
*******************************************************************************************/

.macro KERNEL16x3_SUBN
	vbroadcastsd	-12 * SIZE(BO), %zmm1
	vbroadcastsd	-11 * SIZE(BO), %zmm2
	vbroadcastsd	-10 * SIZE(BO), %zmm3

	vmovaps 	-16 * SIZE(AO), %zmm0
	VFMADD231PD_  	%zmm4,%zmm1,%zmm0
	VFMADD231PD_  	%zmm5,%zmm2,%zmm0
	VFMADD231PD_  	%zmm6,%zmm3,%zmm0

	vmovaps 	 -8 * SIZE(AO), %zmm9
	VFMADD231PD_  	%zmm10,%zmm1,%zmm9
	VFMADD231PD_  	%zmm11,%zmm2,%zmm9
	VFMADD231PD_  	%zmm12,%zmm3,%zmm9
	addq	$ 3*SIZE , BO	
	addq	$ 16*SIZE, AO
.endm


.macro KERNEL8x3_SUBN
	vbroadcastsd	-12 * SIZE(BO), %ymm1
	vmovaps 	-16 * SIZE(AO), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	-11 * SIZE(BO), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	-10 * SIZE(BO), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	vmovaps 	-12 * SIZE(AO), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	VFMADD231PD_  	%ymm9,%ymm3,%ymm0
	prefetcht0	B_PR1(BO)
	addq	$ 3*SIZE , BO	
	addq	$ 8*SIZE, AO
.endm

.macro KERNEL4x3_SUBN
	vbroadcastsd	-12 * SIZE(BO), %ymm1
	vmovaps 	-16 * SIZE(AO), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	-11 * SIZE(BO), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	-10 * SIZE(BO), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	addq	$ 3*SIZE , BO	
	addq	$ 4*SIZE, AO
.endm

.macro KERNEL2x3_SUBN
	vmovsd	-12 * SIZE(BO), %xmm1
	vmovsd 	-16 * SIZE(AO), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	-11 * SIZE(BO), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	-10 * SIZE(BO), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	vmovsd 	-15 * SIZE(AO), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	VFMADD231SD_  	%xmm12,%xmm3,%xmm0
	addq	$ 3*SIZE , BO	
	addq	$ 2*SIZE, AO
.endm

.macro KERNEL1x3_SUBN
	vmovsd	-12 * SIZE(BO), %xmm1
	vmovsd 	-16 * SIZE(AO), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	-11 * SIZE(BO), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	-10 * SIZE(BO), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	addq	$ 3*SIZE , BO	
	addq	$ 1*SIZE, AO
.endm






/******************************************************************************************/

.macro KERNEL16x3_1
	vbroadcastsd	 -6 * SIZE(BO, BI, SIZE), %zmm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm4,%zmm1,%zmm0
	vbroadcastsd	 -5 * SIZE(BO, BI, SIZE), %zmm2
	VFMADD231PD_  	%zmm5,%zmm2,%zmm0
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %zmm3
	VFMADD231PD_  	%zmm6,%zmm3,%zmm0
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm10,%zmm1,%zmm0
	VFMADD231PD_  	%zmm11,%zmm2,%zmm0
	VFMADD231PD_  	%zmm12,%zmm3,%zmm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %zmm1
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %zmm2
.endm




.macro KERNEL16x3_2
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm4,%zmm1,%zmm0
	VFMADD231PD_  	%zmm5,%zmm2,%zmm0
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %zmm3
	VFMADD231PD_  	%zmm6,%zmm3,%zmm0
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm10,%zmm1,%zmm0
	VFMADD231PD_  	%zmm11,%zmm2,%zmm0
	VFMADD231PD_  	%zmm12,%zmm3,%zmm0
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %zmm1
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %zmm2
.endm

.macro KERNEL16x3_3
	vmovups 	  0 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm4,%zmm1,%zmm0
	VFMADD231PD_  	%zmm5,%zmm2,%zmm0
	vbroadcastsd	  2 * SIZE(BO, BI, SIZE), %zmm3
	VFMADD231PD_  	%zmm6,%zmm3,%zmm0
	vmovups 	  8 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm10,%zmm1,%zmm0
	VFMADD231PD_  	%zmm11,%zmm2,%zmm0
	VFMADD231PD_  	%zmm12,%zmm3,%zmm0
	vbroadcastsd	  3 * SIZE(BO, BI, SIZE), %zmm1
	vbroadcastsd	  4 * SIZE(BO, BI, SIZE), %zmm2
.endm

.macro KERNEL16x3_4
	vmovups 	 16 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm4,%zmm1,%zmm0
	VFMADD231PD_  	%zmm5,%zmm2,%zmm0
	vbroadcastsd	  5 * SIZE(BO, BI, SIZE), %zmm3
	VFMADD231PD_  	%zmm6,%zmm3,%zmm0
	vmovups 	 24 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm10,%zmm1,%zmm0
	VFMADD231PD_  	%zmm11,%zmm2,%zmm0
	addq	$12, BI	
	VFMADD231PD_  	%zmm12,%zmm3,%zmm0
	addq	$64, %rax 
.endm

.macro KERNEL16x3_SUB
	vbroadcastsd	 -6 * SIZE(BO, BI, SIZE), %zmm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm4,%zmm1,%zmm0
	vbroadcastsd	 -5 * SIZE(BO, BI, SIZE), %zmm2
	VFMADD231PD_  	%zmm5,%zmm2,%zmm0
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %zmm3
	VFMADD231PD_  	%zmm6,%zmm3,%zmm0
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %zmm0
	VFMADD231PD_  	%zmm10,%zmm1,%zmm0
	VFMADD231PD_  	%zmm11,%zmm2,%zmm0
	VFMADD231PD_  	%zmm12,%zmm3,%zmm0
	addq	$3 , BI	
	addq	$16, %rax 
.endm

.macro SAVE16x3

	vbroadcastsd	ALPHA, %zmm0

	vmulpd	%zmm0 , %zmm4 , %zmm4
	vmulpd	%zmm0 , %zmm10, %zmm10

	vmulpd	%zmm0 , %zmm5 , %zmm5
	vmulpd	%zmm0 , %zmm11, %zmm11

	vmulpd	%zmm0 , %zmm6 , %zmm6
	vmulpd	%zmm0 , %zmm12, %zmm12

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %zmm4,%zmm4
	vaddpd  8 * SIZE(CO1), %zmm10,%zmm10

	vaddpd 	        (CO1, LDC), %zmm5,%zmm5
	vaddpd  8 * SIZE(CO1, LDC), %zmm11,%zmm11

	vaddpd 	        (CO1, LDC, 2), %zmm6,%zmm6
	vaddpd  8 * SIZE(CO1, LDC, 2), %zmm12,%zmm12

#endif

	vmovups	%zmm4 ,  	(CO1)
	vmovups	%zmm10, 8 * SIZE(CO1)

	vmovups	%zmm5 ,  	(CO1, LDC)
	vmovups	%zmm11, 8 * SIZE(CO1, LDC)

	vmovups	%zmm6 ,  	(CO1, LDC, 2)
	vmovups	%zmm12, 8 * SIZE(CO1, LDC, 2)

.endm



/*******************************************************************************************/

.macro KERNEL8x3_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -6 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -5 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	VFMADD231PD_  	%ymm9,%ymm3,%ymm0
.endm

.macro KERNEL8x3_2
	prefetcht0	64+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	VFMADD231PD_  	%ymm9,%ymm3,%ymm0
.endm

.macro KERNEL8x3_3
	prefetcht0	128+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	  2 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	VFMADD231PD_  	%ymm9,%ymm3,%ymm0
.endm

.macro KERNEL8x3_4
	prefetcht0	192+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  3 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  4 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	  5 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	VFMADD231PD_  	%ymm9,%ymm3,%ymm0
	addq	$12, BI
	addq	$32, %rax
.endm

.macro KERNEL8x3_SUB
	vbroadcastsd	 -6 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -5 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	VFMADD231PD_  	%ymm9,%ymm3,%ymm0
	addq	$3 , BI
	addq	$8 , %rax
.endm

.macro SAVE8x3

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm7 , %ymm7

	vmulpd	%ymm0 , %ymm5 , %ymm5
	vmulpd	%ymm0 , %ymm8 , %ymm8

	vmulpd	%ymm0 , %ymm6 , %ymm6
	vmulpd	%ymm0 , %ymm9 , %ymm9

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd  4 * SIZE(CO1), %ymm7,%ymm7

	vaddpd 	        (CO1, LDC), %ymm5,%ymm5
	vaddpd  4 * SIZE(CO1, LDC), %ymm8,%ymm8

	vaddpd 	        (CO1, LDC, 2), %ymm6,%ymm6
	vaddpd  4 * SIZE(CO1, LDC, 2), %ymm9,%ymm9

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm7 , 4 * SIZE(CO1)

	vmovups	%ymm5 ,  	(CO1, LDC)
	vmovups	%ymm8 , 4 * SIZE(CO1, LDC)

	vmovups	%ymm6 ,  	(CO1, LDC, 2)
	vmovups	%ymm9 , 4 * SIZE(CO1, LDC, 2)

.endm



/*******************************************************************************************/

.macro KERNEL4x3_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -6 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -5 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
.endm

.macro KERNEL4x3_2
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
.endm

.macro KERNEL4x3_3
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	  2 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
.endm

.macro KERNEL4x3_4
	vbroadcastsd	  3 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  4 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	  5 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	addq	$12, BI
	addq	$16, %rax
.endm

.macro KERNEL4x3_SUB
	vbroadcastsd	 -6 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -5 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm3
	VFMADD231PD_  	%ymm6,%ymm3,%ymm0
	addq	$3 , BI
	addq	$4 , %rax
.endm

.macro SAVE4x3

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm5 , %ymm5
	vmulpd	%ymm0 , %ymm6 , %ymm6

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd 	        (CO1, LDC), %ymm5,%ymm5
	vaddpd 	        (CO1, LDC, 2), %ymm6,%ymm6

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 ,  	(CO1, LDC)
	vmovups	%ymm6 ,  	(CO1, LDC, 2)

.endm


/*******************************************************************************************/

.macro KERNEL2x3_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vmovsd	 -6 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -5 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	VFMADD231SD_  	%xmm12,%xmm3,%xmm0
.endm

.macro KERNEL2x3_2
	vmovsd	 -3 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-30 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	 -1 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	vmovsd 	-29 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	VFMADD231SD_  	%xmm12,%xmm3,%xmm0
.endm

.macro KERNEL2x3_3
	vmovsd	  0 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-28 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  1 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	  2 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	vmovsd 	-27 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	VFMADD231SD_  	%xmm12,%xmm3,%xmm0
.endm

.macro KERNEL2x3_4
	vmovsd	  3 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-26 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  4 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	  5 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	vmovsd 	-25 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	VFMADD231SD_  	%xmm12,%xmm3,%xmm0
	addq	$12, BI
	addq	$8, %rax
.endm

.macro KERNEL2x3_SUB
	vmovsd	 -6 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -5 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	VFMADD231SD_  	%xmm12,%xmm3,%xmm0
	addq	$3 , BI
	addq	$2 , %rax
.endm

.macro SAVE2x3

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm8 , %xmm8
	vmulsd	%xmm0 , %xmm5 , %xmm5
	vmulsd	%xmm0 , %xmm10, %xmm10
	vmulsd	%xmm0 , %xmm6 , %xmm6
	vmulsd	%xmm0 , %xmm12, %xmm12

#if !defined(TRMMKERNEL)

	vaddsd 	 (CO1), %xmm4,%xmm4
	vaddsd 1 * SIZE(CO1), %xmm8,%xmm8
	vaddsd 	 (CO1, LDC), %xmm5,%xmm5
	vaddsd 1 * SIZE(CO1, LDC), %xmm10,%xmm10
	vaddsd 	 (CO1, LDC, 2), %xmm6,%xmm6
	vaddsd 1 * SIZE(CO1, LDC, 2), %xmm12,%xmm12

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm8 , 1 * SIZE(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm10, 1 * SIZE(CO1, LDC)
	vmovsd	%xmm6 ,  	(CO1, LDC, 2)
	vmovsd	%xmm12, 1 * SIZE(CO1, LDC, 2)

.endm

/*******************************************************************************************/

.macro KERNEL1x3_1
	vmovsd	 -6 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -5 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
.endm

.macro KERNEL1x3_2
	vmovsd	 -3 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	 -1 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
.endm

.macro KERNEL1x3_3
	vmovsd	  0 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-30 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  1 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	  2 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
.endm

.macro KERNEL1x3_4
	vmovsd	  3 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-29 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  4 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	  5 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	addq	$12, BI
	addq	$4, %rax
.endm

.macro KERNEL1x3_SUB
	vmovsd	 -6 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -5 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm3
	VFMADD231SD_  	%xmm6,%xmm3,%xmm0
	addq	$3 , BI
	addq	$1 , %rax
.endm

.macro SAVE1x3

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm5 , %xmm5
	vmulsd	%xmm0 , %xmm6 , %xmm6

#if !defined(TRMMKERNEL)

	vaddsd 	 (CO1), %xmm4,%xmm4
	vaddsd 	 (CO1, LDC), %xmm5,%xmm5
	vaddsd 	 (CO1, LDC, 2), %xmm6,%xmm6

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm6 ,  	(CO1, LDC, 2)

.endm


/*******************************************************************************************/

/*******************************************************************************************
* 2 lines of N
*******************************************************************************************/

.macro KERNEL16x2_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	prefetcht0	64+A_PR1(AO, %rax, SIZE)
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	VFMADD231PD_  	%ymm11,%ymm2,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	VFMADD231PD_  	%ymm14,%ymm2,%ymm0
.endm

.macro KERNEL16x2_2
	prefetcht0	128+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	prefetcht0	192+A_PR1(AO, %rax, SIZE)
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	VFMADD231PD_  	%ymm11,%ymm2,%ymm0
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	VFMADD231PD_  	%ymm14,%ymm2,%ymm0
.endm

.macro KERNEL16x2_3
	prefetcht0	256+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	  0 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	  4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	prefetcht0	320+A_PR1(AO, %rax, SIZE)
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	vmovups 	  8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	VFMADD231PD_  	%ymm11,%ymm2,%ymm0
	vmovups 	 12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	VFMADD231PD_  	%ymm14,%ymm2,%ymm0
.endm

.macro KERNEL16x2_4
	prefetcht0	384+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	 16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	 20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	prefetcht0	448+A_PR1(AO, %rax, SIZE)
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	vmovups 	 24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	VFMADD231PD_  	%ymm11,%ymm2,%ymm0
	vmovups 	 28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	VFMADD231PD_  	%ymm14,%ymm2,%ymm0
	addq	$8, BI
	addq	$64, %rax
.endm

.macro KERNEL16x2_SUB
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	VFMADD231PD_  	%ymm11,%ymm2,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	VFMADD231PD_  	%ymm14,%ymm2,%ymm0
	addq	$2, BI
	addq	$16, %rax
.endm

.macro SAVE16x2

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm7 , %ymm7
	vmulpd	%ymm0 , %ymm10, %ymm10
	vmulpd	%ymm0 , %ymm13, %ymm13

	vmulpd	%ymm0 , %ymm5 , %ymm5
	vmulpd	%ymm0 , %ymm8 , %ymm8
	vmulpd	%ymm0 , %ymm11, %ymm11
	vmulpd	%ymm0 , %ymm14, %ymm14

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd  4 * SIZE(CO1), %ymm7,%ymm7
	vaddpd  8 * SIZE(CO1), %ymm10,%ymm10
	vaddpd 12 * SIZE(CO1), %ymm13,%ymm13

	vaddpd 	        (CO1, LDC), %ymm5,%ymm5
	vaddpd  4 * SIZE(CO1, LDC), %ymm8,%ymm8
	vaddpd  8 * SIZE(CO1, LDC), %ymm11,%ymm11
	vaddpd 12 * SIZE(CO1, LDC), %ymm14,%ymm14

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm7 , 4 * SIZE(CO1)
	vmovups	%ymm10, 8 * SIZE(CO1)
	vmovups	%ymm13,12 * SIZE(CO1)

	vmovups	%ymm5 ,  	(CO1, LDC)
	vmovups	%ymm8 , 4 * SIZE(CO1, LDC)
	vmovups	%ymm11, 8 * SIZE(CO1, LDC)
	vmovups	%ymm14,12 * SIZE(CO1, LDC)

.endm



/*******************************************************************************************/

.macro KERNEL8x2_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
.endm

.macro KERNEL8x2_2
	prefetcht0	64+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
.endm

.macro KERNEL8x2_3
	prefetcht0	128+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
.endm

.macro KERNEL8x2_4
	prefetcht0	192+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	addq	$8, BI				 
	addq	$32, %rax 			 
.endm

.macro KERNEL8x2_SUB
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	VFMADD231PD_  	%ymm8,%ymm2,%ymm0
	addq	$2, BI				 
	addq	$8 , %rax 			 
.endm

.macro SAVE8x2

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm7 , %ymm7

	vmulpd	%ymm0 , %ymm5 , %ymm5
	vmulpd	%ymm0 , %ymm8 , %ymm8

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd  4 * SIZE(CO1), %ymm7,%ymm7

	vaddpd 	        (CO1, LDC), %ymm5,%ymm5
	vaddpd  4 * SIZE(CO1, LDC), %ymm8,%ymm8

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm7 , 4 * SIZE(CO1)

	vmovups	%ymm5 ,  	(CO1, LDC)
	vmovups	%ymm8 , 4 * SIZE(CO1, LDC)

.endm



/*******************************************************************************************/

.macro KERNEL4x2_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
.endm

.macro KERNEL4x2_2
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
.endm

.macro KERNEL4x2_3
	prefetcht0	64+A_PR1(AO, %rax, SIZE)
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
.endm

.macro KERNEL4x2_4
	vbroadcastsd	  2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	  3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	addq	$8, BI				 
	addq	$16, %rax 			 
.endm

.macro KERNEL4x2_SUB
	vbroadcastsd	 -4 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vbroadcastsd	 -3 * SIZE(BO, BI, SIZE), %ymm2
	VFMADD231PD_  	%ymm5,%ymm2,%ymm0
	addq	$2, BI				 
	addq	$4 , %rax 			 
.endm

.macro SAVE4x2

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm5 , %ymm5

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd 	        (CO1, LDC), %ymm5,%ymm5

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 ,  	(CO1, LDC)

.endm


/*******************************************************************************************/

.macro KERNEL2x2_1
	prefetcht0	A_PR1(AO, %rax, SIZE)
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -3 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
.endm

.macro KERNEL2x2_2
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-30 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -1 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd 	-29 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
.endm

.macro KERNEL2x2_3
	vmovsd	  0 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-28 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  1 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd 	-27 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
.endm

.macro KERNEL2x2_4
	vmovsd	  2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-26 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  3 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd 	-25 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	addq	$8, BI				 
	addq	$8, %rax 			 
.endm

.macro KERNEL2x2_SUB
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -3 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	VFMADD231SD_  	%xmm10,%xmm2,%xmm0
	addq	$2, BI				 
	addq	$2, %rax 			 
.endm

.macro SAVE2x2

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm8 , %xmm8
	vmulsd	%xmm0 , %xmm5 , %xmm5
	vmulsd	%xmm0 , %xmm10, %xmm10

#if !defined(TRMMKERNEL)

	vaddsd 	 (CO1), %xmm4,%xmm4
	vaddsd 1 * SIZE(CO1), %xmm8,%xmm8
	vaddsd 	 (CO1, LDC), %xmm5,%xmm5
	vaddsd 1 * SIZE(CO1, LDC), %xmm10,%xmm10

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm8 , 1 * SIZE(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm10, 1 * SIZE(CO1, LDC)

.endm


/*******************************************************************************************/

.macro KERNEL1x2_1
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -3 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
.endm

.macro KERNEL1x2_2
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -1 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
.endm

.macro KERNEL1x2_3
	vmovsd	  0 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-30 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  1 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
.endm

.macro KERNEL1x2_4
	vmovsd	  2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-29 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	  3 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	addq	$8, BI				 
	addq	$4, %rax 			 
.endm

.macro KERNEL1x2_SUB
	vmovsd	 -4 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd	 -3 * SIZE(BO, BI, SIZE), %xmm2
	VFMADD231SD_  	%xmm5,%xmm2,%xmm0
	addq	$2, BI				 
	addq	$1, %rax 			 
.endm

.macro SAVE1x2

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm5 , %xmm5

#if !defined(TRMMKERNEL)

	vaddsd 	 (CO1), %xmm4,%xmm4
	vaddsd 	 (CO1, LDC), %xmm5,%xmm5

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)

.endm


/*******************************************************************************************/

/*******************************************************************************************
* 1 line of N
*******************************************************************************************/

.macro KERNEL16x1_1
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
.endm

.macro KERNEL16x1_2
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
.endm

.macro KERNEL16x1_3
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	  0 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	  4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	vmovups 	  8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	vmovups 	 12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
.endm

.macro KERNEL16x1_4
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	 16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	 20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	vmovups 	 24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	vmovups 	 28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	addq	$4, BI				 
	addq	$64, %rax 			 
.endm

.macro KERNEL16x1_SUB
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm10,%ymm1,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm13,%ymm1,%ymm0
	addq	$1, BI				 
	addq	$16, %rax 			 
.endm

.macro SAVE16x1

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm7 , %ymm7
	vmulpd	%ymm0 , %ymm10, %ymm10
	vmulpd	%ymm0 , %ymm13, %ymm13

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd  4 * SIZE(CO1), %ymm7,%ymm7
	vaddpd  8 * SIZE(CO1), %ymm10,%ymm10
	vaddpd 12 * SIZE(CO1), %ymm13,%ymm13

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm7 , 4 * SIZE(CO1)
	vmovups	%ymm10, 8 * SIZE(CO1)
	vmovups	%ymm13,12 * SIZE(CO1)

.endm



/*******************************************************************************************/

.macro KERNEL8x1_1
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
.endm

.macro KERNEL8x1_2
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
.endm

.macro KERNEL8x1_3
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
.endm

.macro KERNEL8x1_4
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	addq	$4, BI				 
	addq	$32, %rax 			 
.endm

.macro KERNEL8x1_SUB
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm7,%ymm1,%ymm0
	addq	$1, BI				 
	addq	$8 , %rax 			 
.endm

.macro SAVE8x1

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm7 , %ymm7

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4
	vaddpd  4 * SIZE(CO1), %ymm7,%ymm7

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm7 , 4 * SIZE(CO1)

.endm



/*******************************************************************************************/

.macro KERNEL4x1_1
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
.endm

.macro KERNEL4x1_2
	vbroadcastsd	 -1 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
.endm

.macro KERNEL4x1_3
	vbroadcastsd	  0 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
.endm

.macro KERNEL4x1_4
	vbroadcastsd	  1 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	addq	$4, BI				 
	addq	$16, %rax 			 
.endm

.macro KERNEL4x1_SUB
	vbroadcastsd	 -2 * SIZE(BO, BI, SIZE), %ymm1
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %ymm0
	VFMADD231PD_  	%ymm4,%ymm1,%ymm0
	addq	$1, BI				 
	addq	$4 , %rax 			 
.endm

.macro SAVE4x1

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4

#if !defined(TRMMKERNEL)

	vaddpd 	        (CO1), %ymm4,%ymm4

#endif

	vmovups	%ymm4 ,  	(CO1)

.endm


/*******************************************************************************************/

.macro KERNEL2x1_1
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
.endm

.macro KERNEL2x1_2
	vmovsd	 -1 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-30 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd 	-29 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
.endm

.macro KERNEL2x1_3
	vmovsd	  0 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-28 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd 	-27 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
.endm

.macro KERNEL2x1_4
	vmovsd	  1 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-26 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd 	-25 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	addq	$4, BI				 
	addq	$8, %rax 			 
.endm

.macro KERNEL2x1_SUB
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm8,%xmm1,%xmm0
	addq	$1, BI				 
	addq	$2 , %rax 			 
.endm

.macro SAVE2x1

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm8 , %xmm8

#if !defined(TRMMKERNEL)

	vaddsd 	 (CO1), %xmm4,%xmm4
	vaddsd 1 * SIZE(CO1), %xmm8,%xmm8

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm8 , 1 * SIZE(CO1)

.endm


/*******************************************************************************************/

.macro KERNEL1x1_1
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
.endm

.macro KERNEL1x1_2
	vmovsd	 -1 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-31 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
.endm

.macro KERNEL1x1_3
	vmovsd	  0 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-30 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
.endm

.macro KERNEL1x1_4
	vmovsd	  1 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-29 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	addq	$ 4, BI				 
	addq	$ 4, %rax 			 
.endm

.macro KERNEL1x1_SUB
	vmovsd	 -2 * SIZE(BO, BI, SIZE), %xmm1
	vmovsd 	-32 * SIZE(AO, %rax, SIZE), %xmm0
	VFMADD231SD_  	%xmm4,%xmm1,%xmm0
	addq	$ 1, BI				 
	addq	$ 1 , %rax 			 
.endm

.macro SAVE1x1

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4

#if !defined(TRMMKERNEL)

	vaddsd 	 (CO1), %xmm4,%xmm4

#endif

	vmovsd	%xmm4 ,  	(CO1)

.endm


/*******************************************************************************************/

#if !defined(TRMMKERNEL)


	PROLOGUE
	PROFCODE
	
	subq	$STACKSIZE, %rsp
	movq	%rbx,   (%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

	vzeroupper

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC

	vmovaps	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC

#endif

	movq    %rsp, SP      # save old stack
        subq    $128 + L_BUFFER_SIZE, %rsp
        andq    $-4096, %rsp    # align stack

        STACK_TOUCH

	cmpq	$0, OLD_M
	je	.L999

	cmpq	$0, OLD_N
	je	.L999

	cmpq	$0, OLD_K
	je	.L999

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	vmovsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $6,  %rdi
        divq    %rdi                    //    N / 6
        movq    %rax, Ndiv6             //    N / 6
        movq    %rdx, Nmod6             //    N % 6


	movq	Ndiv6,  J
	cmpq	$0, J
	je	.L2_0
	ALIGN_4

.L6_01:
        // copy to sub buffer
        movq    K, %rax
        salq    $1,%rax                 // K * 2 ; read 2 values
        movq    B, BO1
        leaq    (B,%rax, SIZE), BO2     // next offset to BO2
        leaq    BUFFER1, BO             // first buffer to BO
        movq    K, %rax
	sarq	$3 , %rax		// K / 8
	jz	.L6_01a_2
        ALIGN_4

.L6_01a_1:

        prefetcht0 512(BO1)
        prefetcht0 512(BO2)
        prefetchw  512(BO)


	vmovups	0 * SIZE(BO1), %xmm0
	vmovups	2 * SIZE(BO1), %xmm2
	vmovups	4 * SIZE(BO1), %xmm4
	vmovups	6 * SIZE(BO1), %xmm6
	vmovsd  0 * SIZE(BO2), %xmm1
	vmovsd  2 * SIZE(BO2), %xmm3
	vmovsd  4 * SIZE(BO2), %xmm5
	vmovsd  6 * SIZE(BO2), %xmm7
	vmovups	%xmm0, 0*SIZE(BO)
	vmovsd	%xmm1, 2*SIZE(BO)
	vmovups	%xmm2, 3*SIZE(BO)
	vmovsd	%xmm3, 5*SIZE(BO)
	vmovups	%xmm4, 6*SIZE(BO)
	vmovsd	%xmm5, 8*SIZE(BO)
	vmovups	%xmm6, 9*SIZE(BO)
	vmovsd	%xmm7,11*SIZE(BO)
	addq	$ 8*SIZE,BO1
	addq	$ 8*SIZE,BO2
	addq	$ 12*SIZE,BO

	vmovups	0 * SIZE(BO1), %xmm0
	vmovups	2 * SIZE(BO1), %xmm2
	vmovups	4 * SIZE(BO1), %xmm4
	vmovups	6 * SIZE(BO1), %xmm6
	vmovsd  0 * SIZE(BO2), %xmm1
	vmovsd  2 * SIZE(BO2), %xmm3
	vmovsd  4 * SIZE(BO2), %xmm5
	vmovsd  6 * SIZE(BO2), %xmm7
	vmovups	%xmm0, 0*SIZE(BO)
	vmovsd	%xmm1, 2*SIZE(BO)
	vmovups	%xmm2, 3*SIZE(BO)
	vmovsd	%xmm3, 5*SIZE(BO)
	vmovups	%xmm4, 6*SIZE(BO)
	vmovsd	%xmm5, 8*SIZE(BO)
	vmovups	%xmm6, 9*SIZE(BO)
	vmovsd	%xmm7,11*SIZE(BO)
	addq	$ 8*SIZE,BO1
	addq	$ 8*SIZE,BO2
	addq	$ 12*SIZE,BO

	decq	%rax
	jnz	.L6_01a_1



.L6_01a_2:

	movq    K, %rax
        andq    $7, %rax                // K % 8
        jz      .L6_02c
        ALIGN_4


.L6_02b:

	vmovups	0 * SIZE(BO1), %xmm0
	vmovsd  0 * SIZE(BO2), %xmm2
	vmovups	%xmm0, 0*SIZE(BO)
	vmovsd	%xmm2, 2*SIZE(BO)
	addq	$ 2*SIZE,BO1
	addq	$ 2*SIZE,BO2
	addq	$ 3*SIZE,BO
	decq	%rax
	jnz	.L6_02b

.L6_02c:

	movq	K, %rax
	salq	$1,%rax			// K * 2
	leaq	(B,%rax, SIZE), BO1	// next offset to BO1
	leaq	(BO1,%rax, SIZE), BO2	// next offset to BO2
	leaq    BUFFER2, BO		// second buffer to BO
	movq	K, %rax
	sarq	$3 , %rax		// K / 8
	jz	.L6_02c_2
	ALIGN_4

.L6_02c_1:

	prefetcht0 512(BO2)
        prefetchw  512(BO)

	vmovups	0 * SIZE(BO2), %xmm0
	vmovups	2 * SIZE(BO2), %xmm2
	vmovups	4 * SIZE(BO2), %xmm4
	vmovups	6 * SIZE(BO2), %xmm6
	vmovsd  1 * SIZE(BO1), %xmm1
	vmovsd  3 * SIZE(BO1), %xmm3
	vmovsd  5 * SIZE(BO1), %xmm5
	vmovsd  7 * SIZE(BO1), %xmm7
	vmovsd	%xmm1, 0*SIZE(BO)
	vmovups	%xmm0, 1*SIZE(BO)
	vmovsd	%xmm3, 3*SIZE(BO)
	vmovups	%xmm2, 4*SIZE(BO)
	vmovsd	%xmm5, 6*SIZE(BO)
	vmovups	%xmm4, 7*SIZE(BO)
	vmovsd	%xmm7, 9*SIZE(BO)
	vmovups	%xmm6,10*SIZE(BO)
	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO2
	addq	$12*SIZE,BO


	vmovups	0 * SIZE(BO2), %xmm0
	vmovups	2 * SIZE(BO2), %xmm2
	vmovups	4 * SIZE(BO2), %xmm4
	vmovups	6 * SIZE(BO2), %xmm6
	vmovsd  1 * SIZE(BO1), %xmm1
	vmovsd  3 * SIZE(BO1), %xmm3
	vmovsd  5 * SIZE(BO1), %xmm5
	vmovsd  7 * SIZE(BO1), %xmm7
	vmovsd	%xmm1, 0*SIZE(BO)
	vmovups	%xmm0, 1*SIZE(BO)
	vmovsd	%xmm3, 3*SIZE(BO)
	vmovups	%xmm2, 4*SIZE(BO)
	vmovsd	%xmm5, 6*SIZE(BO)
	vmovups	%xmm4, 7*SIZE(BO)
	vmovsd	%xmm7, 9*SIZE(BO)
	vmovups	%xmm6,10*SIZE(BO)
	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO2
	addq	$12*SIZE,BO

	decq	%rax
	jnz	.L6_02c_1


.L6_02c_2:

	movq    K, %rax
        andq    $7, %rax                // K % 8
        jz      .L6_03c
        ALIGN_4

.L6_03b:

	vmovsd	  1*SIZE(BO1), %xmm0
	vmovups	  0*SIZE(BO2), %xmm1
	vmovsd	%xmm0, 0*SIZE(BO)
	vmovups	%xmm1, 1*SIZE(BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO2
	addq	$3*SIZE,BO
	decq	%rax
	jnz	.L6_03b


.L6_03c:

	movq	BO2, B			// next offset of B

.L6_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		 
	leaq	(C, LDC, 1), C		// c += 3 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L6_20

	ALIGN_4

.L6_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	prefetcht0	(CO1)
	prefetcht0	(CO1,LDC,1)
	prefetcht0	(CO1,LDC,2)
	prefetcht0	64(CO1)
	prefetcht0	64(CO1,LDC,1)
	prefetcht0	64(CO1,LDC,2)

	vzeroall

        movq    K, %rax

	sarq $1, %rax			//  K / 8
	je	.L6_16

	ALIGN_5

.L6_12:
/*
	prefetcht0	B_PR1(BO)
	prefetcht0	B_PR1+64(BO)
	prefetcht0	B_PR1+128(BO)
*/
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
/*
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN

	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
*/
	dec	%rax
	jne	.L6_12

.L6_16:
        movq    K, %rax

	andq	$1, %rax		# if (k & 1)
	je .L6_19

	ALIGN_4

.L6_17:

	KERNEL16x3_SUBN

	dec	%rax
	jne	.L6_17
	ALIGN_4


.L6_19:

	SAVE16x3

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L6_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L6_20:
	// Test rest of M

	testq	$15, M
	jz	.L7_10		// to next 3 lines of N

	testq	$8, M		
	jz	.L6_21pre
	ALIGN_4

/**************************************************************************/

.L6_20_1:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L6_20_6

	ALIGN_4

.L6_20_2:

	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN

	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	dec	%rax
	jne	.L6_20_2
	ALIGN_4

.L6_20_6:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_20_9


	ALIGN_4

.L6_20_7:

	KERNEL8x3_SUBN

	dec	%rax
	jne	.L6_20_7
	ALIGN_4


.L6_20_9:

	SAVE8x3

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L6_21pre:

	testq	$4, M		
	jz	.L6_30
	ALIGN_4

.L6_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L6_26

	ALIGN_4

.L6_22:

	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN

	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	dec	%rax
	jne	.L6_22
	ALIGN_4

.L6_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_29

	ALIGN_4

.L6_27:

	KERNEL4x3_SUBN

	dec %rax
	jne	.L6_27
	ALIGN_4


.L6_29:

	SAVE4x3

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L6_30:
	testq	$2, M		
	jz	.L6_40

	ALIGN_4

.L6_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L6_36
	ALIGN_4

.L6_32:

	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN

	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	dec %rax
	jne	.L6_32
	ALIGN_4

.L6_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_39

	ALIGN_4

.L6_37:

	KERNEL2x3_SUBN

	dec %rax
	jne	.L6_37
	ALIGN_4


.L6_39:

	SAVE2x3

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L6_40:
	testq	$1, M		
	jz	.L7_10		// to next 3 lines of N

	ALIGN_4

.L6_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3,%rax
	je	.L6_46

	ALIGN_4

.L6_42:

	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN

	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN

	dec %rax
	jne	.L6_42
	ALIGN_4

.L6_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_49

	ALIGN_4

.L6_47:

	KERNEL1x3_SUBN

	dec	%rax
	jne	.L6_47
	ALIGN_4


.L6_49:

	SAVE1x3

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	



/***************************************************************************************************************/

.L7_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		 
	leaq	(C, LDC, 1), C		// c += 3 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L7_20

	ALIGN_4

.L7_11:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $12 * SIZE, BO

	prefetcht0	(CO1)
	prefetcht0	(CO1,LDC,1)
	prefetcht0	(CO1,LDC,2)
	prefetcht0	64(CO1)
	prefetcht0	64(CO1,LDC,1)
	prefetcht0	64(CO1,LDC,2)

	vzeroall

        movq    K, %rax

	sarq $3, %rax			// K / 8
	je	.L7_16
	ALIGN_5

.L7_12:
/*
	prefetcht0	B_PR1(BO)
	prefetcht0	B_PR1+64(BO)
	prefetcht0	B_PR1+128(BO)
*/
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN

	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	KERNEL16x3_SUBN
	dec %rax
	jne	.L7_12
	ALIGN_4

.L7_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_19

	ALIGN_5

.L7_17:

	KERNEL16x3_SUBN

	dec	%rax
	jne	.L7_17


.L7_19:

	SAVE16x3

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L7_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L7_20:
	// Test rest of M

	testq	$15, M
	jz	.L7_60		// to next 3 lines of N

	testq	$8, M		
	jz	.L7_21pre
	ALIGN_4

/**************************************************************************/

.L7_20_1:
        leaq    BUFFER2, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L7_20_6

	ALIGN_4

.L7_20_2:

	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN

	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN
	KERNEL8x3_SUBN

	dec %rax
	jne	.L7_20_2
	ALIGN_4

.L7_20_6:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_20_9

	ALIGN_4

.L7_20_7:

	KERNEL8x3_SUBN

	dec %rax
	jne	.L7_20_7
	ALIGN_4

.L7_20_9:

	SAVE8x3

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L7_21pre:

	testq	$4, M		
	jz	.L7_30
	ALIGN_4

.L7_21:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L7_26

	ALIGN_4

.L7_22:

	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN

	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN
	KERNEL4x3_SUBN

	dec %rax
	jne	.L7_22
	ALIGN_4

.L7_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_29

	ALIGN_4

.L7_27:

	KERNEL4x3_SUBN

	dec %rax
	jne	.L7_27
	ALIGN_4


.L7_29:

	SAVE4x3

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L7_30:
	testq	$2, M		
	jz	.L7_40

	ALIGN_4

.L7_31:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L7_36

	ALIGN_4

.L7_32:

	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN

	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN
	KERNEL2x3_SUBN

	dec %rax
	jne	.L7_32
	ALIGN_4

.L7_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_39

	ALIGN_4

.L7_37:

	KERNEL2x3_SUBN

	dec %rax
	jne	.L7_37
	ALIGN_4


.L7_39:

	SAVE2x3

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L7_40:
	testq	$1, M		
	jz	.L7_60		// to next 3 lines of N

	ALIGN_4

.L7_41:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $12 * SIZE, BO

	vzeroall

        movq    K, %rax

	sarq	$3, %rax
	je	.L7_46

	ALIGN_4

.L7_42:
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN

	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN
	KERNEL1x3_SUBN

	dec %rax
	jne	.L7_42
	ALIGN_4

.L7_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_49

	ALIGN_4

.L7_47:

	KERNEL1x3_SUBN

	dec %rax
	jne	.L7_47
	ALIGN_4


.L7_49:

	SAVE1x3

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	


.L7_60:

	decq	J			// j --
	jg	.L6_01


.L2_0:
	cmpq	$0, Nmod6		// N % 6 == 0
	je	.L999

/************************************************************************************************
* Loop for Nmod6 / 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	sarq	$1, J			// j = j / 2
	je	.L1_0
	ALIGN_4

.L2_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	sarq	$2, %rax		// K / 4
	jz	.L2_01b
	ALIGN_4

.L2_01a:
        prefetcht0 512(BO1)
        prefetchw  512(BO)

	vmovups	      (BO1), %xmm0
	vmovups	2*SIZE(BO1), %xmm1
	vmovups	4*SIZE(BO1), %xmm2
	vmovups	6*SIZE(BO1), %xmm3

	vmovups	%xmm0,       (BO)
	vmovups	%xmm1, 2*SIZE(BO)
	vmovups	%xmm2, 4*SIZE(BO)
	vmovups	%xmm3, 6*SIZE(BO)

	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO
	decq	%rax
	jnz	.L2_01a


.L2_01b:

        movq    K, %rax
        andq    $3, %rax                // K % 4
        jz      .L2_02d
        ALIGN_4

.L2_02c:

	vmovups	(BO1), %xmm0
	vmovups	%xmm0, (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
	decq	%rax
	jnz	.L2_02c

.L2_02d:

	movq	BO1, B			// next offset of B

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L2_20

	ALIGN_4

.L2_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_12:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	je	.L2_16

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	je	.L2_16

	jmp	.L2_12
	ALIGN_4

.L2_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_19

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_17:

	KERNEL16x2_SUB

	jl	.L2_17
	ALIGN_4


.L2_19:

	SAVE16x2

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$15, M
	jz	.L2_60		// to next 3 lines of N

	testq	$8, M		
	jz	.L2_21pre
	ALIGN_4

/**************************************************************************/

.L2_20_1:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L2_20_6
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_20_2:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	je	.L2_20_6

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	je	.L2_20_6

	jmp	.L2_20_2
	ALIGN_4

.L2_20_6:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_20_9

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_20_7:

	KERNEL8x2_SUB

	jl	.L2_20_7
	ALIGN_4


.L2_20_9:

	SAVE8x2

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L2_21pre:

	testq	$4, M		
	jz	.L2_30
	ALIGN_4

.L2_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L2_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 1 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_22:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	je	.L2_26

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	je	.L2_26

	jmp	.L2_22
	ALIGN_4

.L2_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_27:

	KERNEL4x2_SUB

	jl	.L2_27
	ALIGN_4


.L2_29:

	SAVE4x2

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L2_30:
	testq	$2, M		
	jz	.L2_40

	ALIGN_4

.L2_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L2_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_32:

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	je	.L2_36

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	je	.L2_36

	jmp	.L2_32
	ALIGN_4

.L2_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_37:

	KERNEL2x2_SUB

	jl	.L2_37
	ALIGN_4


.L2_39:

	SAVE2x2

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L2_40:
	testq	$1, M		
	jz	.L2_60		// to next 2 lines of N

	ALIGN_4

.L2_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L2_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_42:

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	je	.L2_46

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	je	.L2_46

	jmp	.L2_42
	ALIGN_4

.L2_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_47:

	KERNEL1x2_SUB

	jl	.L2_47
	ALIGN_4


.L2_49:

	SAVE1x2

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	
.L2_60:

	decq	J			// j --
	jg	.L2_01			// next 2 lines of N



.L1_0:

/************************************************************************************************
* Loop for Nmod6 % 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	andq	$1, J			// j % 2
	je	.L999
	ALIGN_4

.L1_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L1_02b:

	vmovsd	(BO1), %xmm0
	vmovsd	%xmm0,       (BO)
	addq	$1*SIZE,BO1
	addq	$1*SIZE,BO
	decq	%rax
	jnz	.L1_02b

.L1_02c:

	movq	BO1, B			// next offset of B

.L1_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L1_20

	ALIGN_4

.L1_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L1_16
	movq    %rax, BI                        //  Index for BO

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_12:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	je	.L1_16

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	je	.L1_16

	jmp	.L1_12
	ALIGN_4

.L1_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_19

	movq    %rax, BI                        //  Index for BO

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_17:

	KERNEL16x1_SUB

	jl	.L1_17
	ALIGN_4


.L1_19:

	SAVE16x1

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L1_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$15, M
	jz	.L999

	testq	$8, M		
	jz	.L1_21pre
	ALIGN_4

/**************************************************************************/

.L1_20_1:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L1_20_6
	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_20_2:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	je	.L1_20_6

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	je	.L1_20_6

	jmp	.L1_20_2
	ALIGN_4

.L1_20_6:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_20_9

	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_20_7:

	KERNEL8x1_SUB

	jl	.L1_20_7
	ALIGN_4


.L1_20_9:

	SAVE8x1

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L1_21pre:

	testq	$4, M		
	jz	.L1_30
	ALIGN_4

.L1_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L1_26
	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_22:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	je	.L1_26

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	je	.L1_26

	jmp	.L1_22
	ALIGN_4

.L1_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_29

	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_27:

	KERNEL4x1_SUB

	jl	.L1_27
	ALIGN_4


.L1_29:

	SAVE4x1

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L1_30:
	testq	$2, M		
	jz	.L1_40

	ALIGN_4

.L1_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L1_36
	movq    %rax, BI                        //  Index for BO

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_32:

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	je	.L1_36

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	je	.L1_36

	jmp	.L1_32
	ALIGN_4

.L1_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_39

	movq    %rax, BI                        //  Index for BO
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_37:

	KERNEL2x1_SUB

	jl	.L1_37
	ALIGN_4


.L1_39:

	SAVE2x1

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L1_40:
	testq	$1, M		
	jz	.L999

	ALIGN_4

.L1_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L1_46
	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	je	.L1_46

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	je	.L1_46

	jmp	.L1_42
	ALIGN_4

.L1_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB

	jl	.L1_47
	ALIGN_4


.L1_49:

	SAVE1x1

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	

.L999:
	movq   		SP, %rsp
	movq	   (%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE


#else
/*************************************************************************************
* TRMM Kernel
*************************************************************************************/


	PROLOGUE
	PROFCODE
	
	subq	$STACKSIZE, %rsp
	movq	%rbx,   (%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

	vzeroupper

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
	vmovaps	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	STACKSIZE + 16(%rsp), %xmm12
#endif

#endif

	movq    %rsp, SP      # save old stack
        subq    $128 + L_BUFFER_SIZE, %rsp
        andq    $-4096, %rsp    # align stack

        STACK_TOUCH

	cmpq	$0, OLD_M
	je	.L999

	cmpq	$0, OLD_N
	je	.L999

	cmpq	$0, OLD_K
	je	.L999

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	vmovsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $2,  %rdi
        divq    %rdi                    //    N / 6
        movq    %rax, Ndiv6             //    N / 6
        movq    %rdx, Nmod6             //    N % 6

	

#ifdef TRMMKERNEL
	vmovsd	%xmm12, OFFSET
	vmovsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif	
#endif

	movq	Ndiv6,  J
	cmpq	$0, J
	je	.L1_0
	ALIGN_4

.L2_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	sarq	$2, %rax		// K / 4
	jz	.L2_01b
	ALIGN_4

.L2_01a:
        prefetcht0 512(BO1)
        prefetchw  512(BO)

	vmovups	      (BO1), %xmm0
	vmovups	2*SIZE(BO1), %xmm1
	vmovups	4*SIZE(BO1), %xmm2
	vmovups	6*SIZE(BO1), %xmm3

	vmovups	%xmm0,       (BO)
	vmovups	%xmm1, 2*SIZE(BO)
	vmovups	%xmm2, 4*SIZE(BO)
	vmovups	%xmm3, 6*SIZE(BO)

	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO
	decq	%rax
	jnz	.L2_01a


.L2_01b:

        movq    K, %rax
        andq    $3, %rax                // K % 4
        jz      .L2_02d
        ALIGN_4

.L2_02c:

	vmovups	(BO1), %xmm0
	vmovups	%xmm0, (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
	decq	%rax
	jnz	.L2_02c

.L2_02d:

	movq	BO1, B			// next offset of B

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L2_20

	ALIGN_4

.L2_11:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $16, %rax	// number of values in AO
#else
        addq    $2, %rax	// number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_12:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	je	.L2_16

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x2_1
	KERNEL16x2_2
	KERNEL16x2_3
	KERNEL16x2_4

	je	.L2_16

	jmp	.L2_12
	ALIGN_4

.L2_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_19

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_17:

	KERNEL16x2_SUB

	jl	.L2_17
	ALIGN_4


.L2_19:

	SAVE16x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $16, KK				
#endif

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$15, M
	jz	.L2_60		// to next 3 lines of N

	testq	$8, M		
	jz	.L2_21pre
	ALIGN_4

/**************************************************************************/

.L2_20_1:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $8, %rax        // number of values in A
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_20_6
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_20_2:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	je	.L2_20_6

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x2_1
	KERNEL8x2_2
	KERNEL8x2_3
	KERNEL8x2_4

	je	.L2_20_6

	jmp	.L2_20_2
	ALIGN_4

.L2_20_6:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_20_9

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_20_7:

	KERNEL8x2_SUB

	jl	.L2_20_7
	ALIGN_4


.L2_20_9:

	SAVE8x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L2_21pre:

	testq	$4, M		
	jz	.L2_30
	ALIGN_4

.L2_21:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $4, %rax        // number of values in A
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 1 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_22:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	je	.L2_26

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_3
	KERNEL4x2_4

	je	.L2_26

	jmp	.L2_22
	ALIGN_4

.L2_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_27:

	KERNEL4x2_SUB

	jl	.L2_27
	ALIGN_4


.L2_29:

	SAVE4x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L2_30:
	testq	$2, M		
	jz	.L2_40

	ALIGN_4

.L2_31:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $2, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_32:

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	je	.L2_36

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_3
	KERNEL2x2_4

	je	.L2_36

	jmp	.L2_32
	ALIGN_4

.L2_36:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_37:

	KERNEL2x2_SUB

	jl	.L2_37
	ALIGN_4


.L2_39:

	SAVE2x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L2_40:
	testq	$1, M		
	jz	.L2_60		// to next 2 lines of N

	ALIGN_4

.L2_41:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $1, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax
	je	.L2_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_42:

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	je	.L2_46

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_3
	KERNEL1x2_4

	je	.L2_46

	jmp	.L2_42
	ALIGN_4

.L2_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_47:

	KERNEL1x2_SUB

	jl	.L2_47
	ALIGN_4


.L2_49:

	SAVE1x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	



	
.L2_60:
#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $2, KK
#endif

	decq	J			// j --
	jg	.L2_01			// next 2 lines of N



.L1_0:

/************************************************************************************************
* Loop for Nmod6 % 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	andq	$1, J			// j % 2
	je	.L999
	ALIGN_4

.L1_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L1_02b:

	vmovsd	(BO1), %xmm0
	vmovsd	%xmm0,       (BO)
	addq	$1*SIZE,BO1
	addq	$1*SIZE,BO
	decq	%rax
	jnz	.L1_02b

.L1_02c:

	movq	BO1, B			// next offset of B

.L1_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L1_20

	ALIGN_4

.L1_11:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $16, %rax	// number of values in AO
#else
        addq    $1, %rax	// number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L1_16
	movq    %rax, BI                        //  Index for BO

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_12:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	je	.L1_16

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	KERNEL16x1_1
	KERNEL16x1_2
	KERNEL16x1_3
	KERNEL16x1_4

	je	.L1_16

	jmp	.L1_12
	ALIGN_4

.L1_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_19

	movq    %rax, BI                        //  Index for BO

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_17:

	KERNEL16x1_SUB

	jl	.L1_17
	ALIGN_4


.L1_19:

	SAVE16x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $16, KK				
#endif

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L1_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$15, M
	jz	.L999

	testq	$8, M		
	jz	.L1_21pre
	ALIGN_4

/**************************************************************************/

.L1_20_1:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $8, %rax        // number of values in A
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_20_6
	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_20_2:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	je	.L1_20_6

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	KERNEL8x1_1
	KERNEL8x1_2
	KERNEL8x1_3
	KERNEL8x1_4

	je	.L1_20_6

	jmp	.L1_20_2
	ALIGN_4

.L1_20_6:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_20_9

	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_20_7:

	KERNEL8x1_SUB

	jl	.L1_20_7
	ALIGN_4


.L1_20_9:

	SAVE8x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L1_21pre:

	testq	$4, M		
	jz	.L1_30
	ALIGN_4

.L1_21:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $4, %rax        // number of values in A
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_26
	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_22:

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	je	.L1_26

	prefetcht0      B_PR1(BO,BI,8)
	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_3
	KERNEL4x1_4

	je	.L1_26

	jmp	.L1_22
	ALIGN_4

.L1_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_29

	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_27:

	KERNEL4x1_SUB

	jl	.L1_27
	ALIGN_4


.L1_29:

	SAVE4x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L1_30:
	testq	$2, M		
	jz	.L1_40

	ALIGN_4

.L1_31:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $2, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_36
	movq    %rax, BI                        //  Index for BO

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_32:

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	je	.L1_36

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_3
	KERNEL2x1_4

	je	.L1_36

	jmp	.L1_32
	ALIGN_4

.L1_36:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_39

	movq    %rax, BI                        //  Index for BO
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_37:

	KERNEL2x1_SUB

	jl	.L1_37
	ALIGN_4


.L1_39:

	SAVE2x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L1_40:
	testq	$1, M		
	jz	.L999

	ALIGN_4

.L1_41:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        leaq    (AO, %rax, SIZE), AO
#endif


	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $1, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax
	je	.L1_46
	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	je	.L1_46

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_3
	KERNEL1x1_4

	je	.L1_46

	jmp	.L1_42
	ALIGN_4

.L1_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB

	jl	.L1_47
	ALIGN_4


.L1_49:

	SAVE1x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	

.L999:
	movq   		SP, %rsp
	movq	   (%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE





#endif
