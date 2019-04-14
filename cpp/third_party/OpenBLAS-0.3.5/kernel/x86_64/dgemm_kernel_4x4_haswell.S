/*********************************************************************************
Copyright (c) 2013, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************************/


/*********************************************************************
* 2013/10/28 Saar
*        BLASTEST               : OK
*        CTEST                  : OK
*        TEST                   : OK

*
*
* 2013/10/27 Saar
* Parameter:
*       DGEMM_DEFAULT_UNROLL_N  4
*       DGEMM_DEFAULT_UNROLL_M  4
*       DGEMM_DEFAULT_P         512
*       DGEMM_DEFAULT_Q         256
*	A_PR1			512
*	B_PR1			512
*
*
* Performance at 9216x9216x9216:
*       1 thread:       53.3 GFLOPS (MKL:  54)
*       2 threads:     100.0 GFLOPS (MKL:  97)
*       3 threads:     147.0 GFLOPS (MKL: 133)
*       4 threads:     184.0 GFLOPS (MKL: 170)
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
#define	SP	%rbx

#define BO1	%rdi
#define BO2	%r15
#define BO3	%rbp

#ifndef WINDOWS_ABI

#define STACKSIZE 96
#define L_BUFFER_SIZE 256*8*12+4096

#else

#define STACKSIZE 256
#define L_BUFFER_SIZE 128*8*12+512

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif


#define Ndiv12	 24(%rsp)
#define Nmod12	 32(%rsp)
#define N	 40(%rsp)
#define ALPHA	 48(%rsp)
#define OFFSET	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)
#define BUFFER1	           128(%rsp)

#if defined(OS_WINDOWS)
#if   L_BUFFER_SIZE > 16384
#define STACK_TOUCH \
        movl    $ 0,  4096 * 4(%rsp);\
        movl    $ 0,  4096 * 3(%rsp);\
        movl    $ 0,  4096 * 2(%rsp);\
        movl    $ 0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 12288
#define STACK_TOUCH \
        movl    $ 0,  4096 * 3(%rsp);\
        movl    $ 0,  4096 * 2(%rsp);\
        movl    $ 0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 8192
#define STACK_TOUCH \
        movl    $ 0,  4096 * 2(%rsp);\
        movl    $ 0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 4096
#define STACK_TOUCH \
        movl    $ 0,  4096 * 1(%rsp);
#else
#define STACK_TOUCH
#endif
#else
#define STACK_TOUCH
#endif

#define	A_PR1	512
#define	B_PR1	512

/*******************************************************************************************
* Macro definitions
*******************************************************************************************/

.macro INIT4x12

	vxorpd		%ymm4 , %ymm4 , %ymm4
	vxorpd		%ymm5 , %ymm5 , %ymm5
	vxorpd		%ymm6 , %ymm6 , %ymm6
	vxorpd		%ymm7 , %ymm7 , %ymm7
	vxorpd		%ymm8 , %ymm8 , %ymm8
	vxorpd		%ymm9 , %ymm9 , %ymm9
	vxorpd		%ymm10, %ymm10, %ymm10
	vxorpd		%ymm11, %ymm11, %ymm11
	vxorpd		%ymm12, %ymm12, %ymm12
	vxorpd		%ymm13, %ymm13, %ymm13
	vxorpd		%ymm14, %ymm14, %ymm14
	vxorpd		%ymm15, %ymm15, %ymm15

.endm

.macro KERNEL4x12_I
	prefetcht0	A_PR1(AO)
	vmovups		-12 * SIZE(BO), %ymm1
	prefetcht0	B_PR1(BO)
	vmovups 	-16 * SIZE(AO), %ymm0
	prefetcht0	B_PR1+64(BO)
	vmovups		 -8 * SIZE(BO), %ymm2
	prefetcht0	B_PR1+128(BO)
	vmovups		 -4 * SIZE(BO), %ymm3
	vmulpd  	%ymm0 ,%ymm1  , %ymm4
	prefetcht0	B_PR1+192(BO)
	vmulpd  	%ymm0 ,%ymm2  , %ymm8
	vmulpd  	%ymm0 ,%ymm3  , %ymm12
	prefetcht0	B_PR1+256(BO)
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm5
	vmulpd  	%ymm0 ,%ymm2  , %ymm9
	vmulpd  	%ymm0 ,%ymm3  , %ymm13
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm6
	vmulpd  	%ymm0 ,%ymm2  , %ymm10

	addq		$ 12*SIZE, BO
	vmulpd  	%ymm0 ,%ymm3  , %ymm14
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm7
	vmovups		-12 * SIZE(BO), %ymm1
	vmulpd  	%ymm0 ,%ymm2  , %ymm11
	vmovups		 -8 * SIZE(BO), %ymm2
	vmulpd  	%ymm0 ,%ymm3  , %ymm15
	vmovups		 -4 * SIZE(BO), %ymm3

.endm

.macro KERNEL4x12_M1
	prefetcht0	A_PR1(AO)
	vmovups 	-16 * SIZE(AO), %ymm0
	prefetcht0	B_PR1(BO)
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	prefetcht0	B_PR1+64(BO)
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm8
	prefetcht0	B_PR1+128(BO)
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm12
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm9
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm13
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm10

	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm14
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	vmovups		-12 * SIZE(BO), %ymm1
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm11
	vmovups		 -8 * SIZE(BO), %ymm2
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm15
	vmovups		 -4 * SIZE(BO), %ymm3

.endm

.macro KERNEL4x12_M2
	vmovups 	-12 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm8
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm12
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm9
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm13
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm10

	addq		$ 8*SIZE, AO
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm14
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	vmovups		  0 * SIZE(BO), %ymm1
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm11
	vmovups		  4 * SIZE(BO), %ymm2
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm15
	vmovups		  8 * SIZE(BO), %ymm3
	addq		$ 24*SIZE, BO
.endm


.macro KERNEL4x12_E
	vmovups 	-12 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm8
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm12
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm9
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm13
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm10

	addq		$ 8*SIZE, AO
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm14
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm11
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm15
	addq		$ 12*SIZE, BO
.endm

.macro KERNEL4x12_SUB
	vmovups		-12 * SIZE(BO), %ymm1
	vmovups 	-16 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vmovups		 -8 * SIZE(BO), %ymm2
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm8
	vmovups		 -4 * SIZE(BO), %ymm3
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm12
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm9
	addq		$ 12*SIZE, BO
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm13
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm10
	addq		$ 4*SIZE, AO
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm14
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm11
	vfmadd231pd  	%ymm0 ,%ymm3  , %ymm15

.endm


.macro SAVE4x12

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm5 , %ymm5
	vmulpd	%ymm0 , %ymm6 , %ymm6
	vmulpd	%ymm0 , %ymm7 , %ymm7

	vmulpd	%ymm0 , %ymm8 , %ymm8
	vmulpd	%ymm0 , %ymm9 , %ymm9
	vmulpd	%ymm0 , %ymm10, %ymm10
	vmulpd	%ymm0 , %ymm11, %ymm11

	vmulpd	%ymm0 , %ymm12, %ymm12
	vmulpd	%ymm0 , %ymm13, %ymm13
	vmulpd	%ymm0 , %ymm14, %ymm14
	vmulpd	%ymm0 , %ymm15, %ymm15

	vpermpd $ 0xb1 , %ymm5, %ymm5
	vpermpd $ 0xb1 , %ymm7, %ymm7

	vblendpd $ 0x0a, %ymm5, %ymm4, %ymm0
	vblendpd $ 0x05, %ymm5, %ymm4, %ymm1
	vblendpd $ 0x0a, %ymm7, %ymm6, %ymm2
	vblendpd $ 0x05, %ymm7, %ymm6, %ymm3

	vpermpd $ 0x1b , %ymm2, %ymm2
	vpermpd $ 0x1b , %ymm3, %ymm3
	vpermpd $ 0xb1 , %ymm2, %ymm2
	vpermpd $ 0xb1 , %ymm3, %ymm3

	vblendpd $ 0x03, %ymm0, %ymm2 , %ymm4
	vblendpd $ 0x03, %ymm1, %ymm3 , %ymm5
	vblendpd $ 0x03, %ymm2, %ymm0 , %ymm6
	vblendpd $ 0x03, %ymm3, %ymm1 , %ymm7

        leaq    (CO1, LDC, 2), %rax     
	

#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1), %ymm4, %ymm4
	vaddpd 	           (CO1, LDC), %ymm5, %ymm5
	vaddpd 	               (%rax), %ymm6, %ymm6
	vaddpd 	          (%rax, LDC), %ymm7, %ymm7

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 ,  	(CO1, LDC)
	vmovups	%ymm6 ,  	(%rax)
	vmovups	%ymm7 ,  	(%rax, LDC)

	prefetcht0	32(CO1)
	prefetcht0	32(CO1,LDC)
	prefetcht0	32(%rax)
	prefetcht0	32(%rax,LDC)

	vpermpd $ 0xb1 , %ymm9 , %ymm9
	vpermpd $ 0xb1 , %ymm11, %ymm11

	vblendpd $ 0x0a, %ymm9 , %ymm8 , %ymm0
	vblendpd $ 0x05, %ymm9 , %ymm8 , %ymm1
	vblendpd $ 0x0a, %ymm11, %ymm10, %ymm2
	vblendpd $ 0x05, %ymm11, %ymm10, %ymm3

	vpermpd $ 0x1b , %ymm2, %ymm2
	vpermpd $ 0x1b , %ymm3, %ymm3
	vpermpd $ 0xb1 , %ymm2, %ymm2
	vpermpd $ 0xb1 , %ymm3, %ymm3

	vblendpd $ 0x03, %ymm0, %ymm2 , %ymm4
	vblendpd $ 0x03, %ymm1, %ymm3 , %ymm5
	vblendpd $ 0x03, %ymm2, %ymm0 , %ymm6
	vblendpd $ 0x03, %ymm3, %ymm1 , %ymm7


	leaq	(%rax, LDC, 2), %rax
	leaq	(%rax, LDC, 2), %rbp

#if !defined(TRMMKERNEL)

	vaddpd 	                (%rax), %ymm4, %ymm4
	vaddpd 	           (%rax, LDC), %ymm5, %ymm5
	vaddpd 	                (%rbp), %ymm6, %ymm6
	vaddpd 	           (%rbp, LDC), %ymm7, %ymm7

#endif

	vmovups	%ymm4 ,  	(%rax)
	vmovups	%ymm5 ,  	(%rax, LDC)
	vmovups	%ymm6 ,  	(%rbp)
	vmovups	%ymm7 ,  	(%rbp, LDC)

	prefetcht0	32(%rax)
	prefetcht0	32(%rax,LDC)
	prefetcht0	32(%rbp)
	prefetcht0	32(%rbp,LDC)

	vpermpd $ 0xb1 , %ymm13, %ymm13
	vpermpd $ 0xb1 , %ymm15, %ymm15

	vblendpd $ 0x0a, %ymm13, %ymm12, %ymm0
	vblendpd $ 0x05, %ymm13, %ymm12, %ymm1
	vblendpd $ 0x0a, %ymm15, %ymm14, %ymm2
	vblendpd $ 0x05, %ymm15, %ymm14, %ymm3

	vpermpd $ 0x1b , %ymm2, %ymm2
	vpermpd $ 0x1b , %ymm3, %ymm3
	vpermpd $ 0xb1 , %ymm2, %ymm2
	vpermpd $ 0xb1 , %ymm3, %ymm3

	vblendpd $ 0x03, %ymm0, %ymm2 , %ymm4
	vblendpd $ 0x03, %ymm1, %ymm3 , %ymm5
	vblendpd $ 0x03, %ymm2, %ymm0 , %ymm6
	vblendpd $ 0x03, %ymm3, %ymm1 , %ymm7


	leaq	(%rax, LDC, 4), %rax
	leaq	(%rbp, LDC, 4), %rbp

#if !defined(TRMMKERNEL)

	vaddpd 	                (%rax), %ymm4, %ymm4
	vaddpd 	           (%rax, LDC), %ymm5, %ymm5
	vaddpd 	                (%rbp), %ymm6, %ymm6
	vaddpd 	           (%rbp, LDC), %ymm7, %ymm7

#endif

	vmovups	%ymm4 ,  	(%rax)
	vmovups	%ymm5 ,  	(%rax, LDC)
	vmovups	%ymm6 ,  	(%rbp)
	vmovups	%ymm7 ,  	(%rbp, LDC)

	prefetcht0	32(%rax)
	prefetcht0	32(%rax,LDC)
	prefetcht0	32(%rbp)
	prefetcht0	32(%rbp,LDC)

	addq	$ 4*SIZE, CO1
.endm

/******************************************************************************************/

.macro INIT2x12

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm5 , %xmm5 , %xmm5
	vxorpd		%xmm6 , %xmm6 , %xmm6
	vxorpd		%xmm7 , %xmm7 , %xmm7
	vxorpd		%xmm8 , %xmm8 , %xmm8
	vxorpd		%xmm9 , %xmm9 , %xmm9
	vxorpd		%xmm10, %xmm10, %xmm10
	vxorpd		%xmm11, %xmm11, %xmm11
	vxorpd		%xmm12, %xmm12, %xmm12
	vxorpd		%xmm13, %xmm13, %xmm13
	vxorpd		%xmm14, %xmm14, %xmm14
	vxorpd		%xmm15, %xmm15, %xmm15

.endm

.macro KERNEL2x12_SUB
	vmovups 	-16 * SIZE(AO), %xmm0
	vmovddup	-12 * SIZE(BO), %xmm1
	vmovddup	-11 * SIZE(BO), %xmm2
	vmovddup	-10 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm1  , %xmm4
	vmovddup	 -9 * SIZE(BO), %xmm1
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm5
	vmovddup	 -8 * SIZE(BO), %xmm2
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm6
	vmovddup	 -7 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm1  , %xmm7
	vmovddup	 -6 * SIZE(BO), %xmm1
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm8
	vmovddup	 -5 * SIZE(BO), %xmm2
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm9
	vmovddup	 -4 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm1  , %xmm10
	vmovddup	 -3 * SIZE(BO), %xmm1
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm11
	vmovddup	 -2 * SIZE(BO), %xmm2
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm12
	vmovddup	 -1 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm1  , %xmm13
	addq		$ 12*SIZE, BO
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm14
	addq		$ 2*SIZE, AO
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm15

.endm

.macro SAVE2x12

	vmovddup	ALPHA, %xmm0

	vmulpd	%xmm0 , %xmm4 , %xmm4
	vmulpd	%xmm0 , %xmm5 , %xmm5
	vmulpd	%xmm0 , %xmm6 , %xmm6
	vmulpd	%xmm0 , %xmm7 , %xmm7

	vmulpd	%xmm0 , %xmm8 , %xmm8
	vmulpd	%xmm0 , %xmm9 , %xmm9
	vmulpd	%xmm0 , %xmm10, %xmm10
	vmulpd	%xmm0 , %xmm11, %xmm11

	vmulpd	%xmm0 , %xmm12, %xmm12
	vmulpd	%xmm0 , %xmm13, %xmm13
	vmulpd	%xmm0 , %xmm14, %xmm14
	vmulpd	%xmm0 , %xmm15, %xmm15


        leaq    (CO1, LDC, 2), %rax     
	

#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1), %xmm4, %xmm4
	vaddpd 	           (CO1, LDC), %xmm5, %xmm5
	vaddpd 	               (%rax), %xmm6, %xmm6
	vaddpd 	          (%rax, LDC), %xmm7, %xmm7

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm6 ,  	(%rax)
	vmovups	%xmm7 ,  	(%rax, LDC)


	leaq	(%rax, LDC, 2), %rax
	leaq	(%rax, LDC, 2), %rbp

#if !defined(TRMMKERNEL)

	vaddpd 	                (%rax), %xmm8 , %xmm4
	vaddpd 	           (%rax, LDC), %xmm9 , %xmm5
	vaddpd 	                (%rbp), %xmm10, %xmm6
	vaddpd 	           (%rbp, LDC), %xmm11, %xmm7

#endif

	vmovups	%xmm4 ,  	(%rax)
	vmovups	%xmm5 ,  	(%rax, LDC)
	vmovups	%xmm6 ,  	(%rbp)
	vmovups	%xmm7 ,  	(%rbp, LDC)


	leaq	(%rax, LDC, 4), %rax
	leaq	(%rbp, LDC, 4), %rbp

#if !defined(TRMMKERNEL)

	vaddpd 	                (%rax), %xmm12, %xmm4
	vaddpd 	           (%rax, LDC), %xmm13, %xmm5
	vaddpd 	                (%rbp), %xmm14, %xmm6
	vaddpd 	           (%rbp, LDC), %xmm15, %xmm7

#endif

	vmovups	%xmm4 ,  	(%rax)
	vmovups	%xmm5 ,  	(%rax, LDC)
	vmovups	%xmm6 ,  	(%rbp)
	vmovups	%xmm7 ,  	(%rbp, LDC)

	addq	$ 2*SIZE, CO1
.endm


/******************************************************************************************/

.macro INIT1x12

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm5 , %xmm5 , %xmm5
	vxorpd		%xmm6 , %xmm6 , %xmm6
	vxorpd		%xmm7 , %xmm7 , %xmm7
	vxorpd		%xmm8 , %xmm8 , %xmm8
	vxorpd		%xmm9 , %xmm9 , %xmm9
	vxorpd		%xmm10, %xmm10, %xmm10
	vxorpd		%xmm11, %xmm11, %xmm11
	vxorpd		%xmm12, %xmm12, %xmm12
	vxorpd		%xmm13, %xmm13, %xmm13
	vxorpd		%xmm14, %xmm14, %xmm14
	vxorpd		%xmm15, %xmm15, %xmm15

.endm

.macro KERNEL1x12_SUB
	vmovsd 	-16 * SIZE(AO), %xmm0
	vmovsd	-12 * SIZE(BO), %xmm1
	vmovsd	-11 * SIZE(BO), %xmm2
	vmovsd	-10 * SIZE(BO), %xmm3
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm4
	vmovsd	 -9 * SIZE(BO), %xmm1
	vfmadd231sd  	%xmm0 ,%xmm2  , %xmm5
	vmovsd	 -8 * SIZE(BO), %xmm2
	vfmadd231sd  	%xmm0 ,%xmm3  , %xmm6
	vmovsd	 -7 * SIZE(BO), %xmm3
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm7
	vmovsd	 -6 * SIZE(BO), %xmm1
	vfmadd231sd  	%xmm0 ,%xmm2  , %xmm8
	vmovsd	 -5 * SIZE(BO), %xmm2
	vfmadd231sd  	%xmm0 ,%xmm3  , %xmm9
	vmovsd	 -4 * SIZE(BO), %xmm3
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm10
	vmovsd	 -3 * SIZE(BO), %xmm1
	vfmadd231sd  	%xmm0 ,%xmm2  , %xmm11
	vmovsd	 -2 * SIZE(BO), %xmm2
	vfmadd231sd  	%xmm0 ,%xmm3  , %xmm12
	vmovsd	 -1 * SIZE(BO), %xmm3
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm13
	addq		$ 12*SIZE, BO
	vfmadd231sd  	%xmm0 ,%xmm2  , %xmm14
	addq		$ 1*SIZE, AO
	vfmadd231sd  	%xmm0 ,%xmm3  , %xmm15

.endm

.macro SAVE1x12

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm5 , %xmm5
	vmulsd	%xmm0 , %xmm6 , %xmm6
	vmulsd	%xmm0 , %xmm7 , %xmm7

	vmulsd	%xmm0 , %xmm8 , %xmm8
	vmulsd	%xmm0 , %xmm9 , %xmm9
	vmulsd	%xmm0 , %xmm10, %xmm10
	vmulsd	%xmm0 , %xmm11, %xmm11

	vmulsd	%xmm0 , %xmm12, %xmm12
	vmulsd	%xmm0 , %xmm13, %xmm13
	vmulsd	%xmm0 , %xmm14, %xmm14
	vmulsd	%xmm0 , %xmm15, %xmm15


        leaq    (CO1, LDC, 2), %rax     
	

#if !defined(TRMMKERNEL)

	vaddsd 	                (CO1), %xmm4, %xmm4
	vaddsd 	           (CO1, LDC), %xmm5, %xmm5
	vaddsd 	               (%rax), %xmm6, %xmm6
	vaddsd 	          (%rax, LDC), %xmm7, %xmm7

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm6 ,  	(%rax)
	vmovsd	%xmm7 ,  	(%rax, LDC)


	leaq	(%rax, LDC, 2), %rax
	leaq	(%rax, LDC, 2), %rbp

#if !defined(TRMMKERNEL)

	vaddsd 	                (%rax), %xmm8 , %xmm4
	vaddsd 	           (%rax, LDC), %xmm9 , %xmm5
	vaddsd 	                (%rbp), %xmm10, %xmm6
	vaddsd 	           (%rbp, LDC), %xmm11, %xmm7

#endif

	vmovsd	%xmm4 ,  	(%rax)
	vmovsd	%xmm5 ,  	(%rax, LDC)
	vmovsd	%xmm6 ,  	(%rbp)
	vmovsd	%xmm7 ,  	(%rbp, LDC)


	leaq	(%rax, LDC, 4), %rax
	leaq	(%rbp, LDC, 4), %rbp

#if !defined(TRMMKERNEL)

	vaddsd 	                (%rax), %xmm12, %xmm4
	vaddsd 	           (%rax, LDC), %xmm13, %xmm5
	vaddsd 	                (%rbp), %xmm14, %xmm6
	vaddsd 	           (%rbp, LDC), %xmm15, %xmm7

#endif

	vmovsd	%xmm4 ,  	(%rax)
	vmovsd	%xmm5 ,  	(%rax, LDC)
	vmovsd	%xmm6 ,  	(%rbp)
	vmovsd	%xmm7 ,  	(%rbp, LDC)

	addq	$ 1*SIZE, CO1
.endm




/******************************************************************************************/
/******************************************************************************************/

.macro INIT4x4

	vxorpd		%ymm4 , %ymm4 , %ymm4
	vxorpd		%ymm5 , %ymm5 , %ymm5
	vxorpd		%ymm6 , %ymm6 , %ymm6
	vxorpd		%ymm7 , %ymm7 , %ymm7

.endm

.macro KERNEL4x4_I
	prefetcht0	A_PR1(AO)
	vmovups		-12 * SIZE(BO), %ymm1
	vmovups 	-16 * SIZE(AO), %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm4
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm5
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm6

	addq		$ 4*SIZE, BO
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vmulpd  	%ymm0 ,%ymm1  , %ymm7
	vmovups		-12 * SIZE(BO), %ymm1

.endm

.macro KERNEL4x4_M1
	prefetcht0	A_PR1(AO)
	vmovups 	-16 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6

	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	vmovups		-12 * SIZE(BO), %ymm1

.endm

.macro KERNEL4x4_M2
	vmovups 	-12 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6

	addq		$ 8*SIZE, AO
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	vmovups		 -8 * SIZE(BO), %ymm1
	addq		$ 8*SIZE, BO
.endm


.macro KERNEL4x4_E
	vmovups 	-12 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6

	addq		$ 8*SIZE, AO
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7
	addq		$ 4*SIZE, BO
.endm

.macro KERNEL4x4_SUB
	vmovups		-12 * SIZE(BO), %ymm1
	vmovups 	-16 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm4
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm5
	addq		$ 4*SIZE, BO
	vpermpd		$ 0x1b, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm6
	addq		$ 4*SIZE, AO
	vpermpd		$ 0xb1, %ymm0  , %ymm0
	vfmadd231pd  	%ymm0 ,%ymm1  , %ymm7

.endm

.macro SAVE4x4

	vbroadcastsd	ALPHA, %ymm0

	vmulpd	%ymm0 , %ymm4 , %ymm4
	vmulpd	%ymm0 , %ymm7 , %ymm7
	vmulpd	%ymm0 , %ymm5 , %ymm5
	vmulpd	%ymm0 , %ymm6 , %ymm6

	vpermpd $ 0xb1 , %ymm5, %ymm5
	vpermpd $ 0xb1 , %ymm7, %ymm7

	vblendpd $ 0x0a, %ymm5, %ymm4, %ymm0
	vblendpd $ 0x05, %ymm5, %ymm4, %ymm1
	vblendpd $ 0x0a, %ymm7, %ymm6, %ymm2
	vblendpd $ 0x05, %ymm7, %ymm6, %ymm3

	vpermpd $ 0x1b , %ymm2, %ymm2
	vpermpd $ 0x1b , %ymm3, %ymm3
	vpermpd $ 0xb1 , %ymm2, %ymm2
	vpermpd $ 0xb1 , %ymm3, %ymm3

	vblendpd $ 0x03, %ymm0, %ymm2 , %ymm4
	vblendpd $ 0x03, %ymm1, %ymm3 , %ymm5
	vblendpd $ 0x03, %ymm2, %ymm0 , %ymm6
	vblendpd $ 0x03, %ymm3, %ymm1 , %ymm7

        leaq    (CO1, LDC, 2), %rax     
	

#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1), %ymm4, %ymm4
	vaddpd 	           (CO1, LDC), %ymm5, %ymm5
	vaddpd 	               (%rax), %ymm6, %ymm6
	vaddpd 	          (%rax, LDC), %ymm7, %ymm7

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 ,  	(CO1, LDC)
	vmovups	%ymm6 ,  	(%rax)
	vmovups	%ymm7 ,  	(%rax, LDC)

	addq	$ 4*SIZE, CO1
.endm

/******************************************************************************************/
/******************************************************************************************/

.macro INIT2x4

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm5 , %xmm5 , %xmm5
	vxorpd		%xmm6 , %xmm6 , %xmm6
	vxorpd		%xmm7 , %xmm7 , %xmm7

.endm


.macro KERNEL2x4_SUB
	vmovddup	-12 * SIZE(BO), %xmm1
	vmovups 	-16 * SIZE(AO), %xmm0
	vmovddup	-11 * SIZE(BO), %xmm2
	vfmadd231pd  	%xmm0 ,%xmm1  , %xmm4
	vmovddup	-10 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm5
	vmovddup	 -9 * SIZE(BO), %xmm8
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm6
	addq		$ 4*SIZE, BO
	vfmadd231pd  	%xmm0 ,%xmm8  , %xmm7
	addq		$ 2*SIZE, AO

.endm


.macro SAVE2x4

	vmovddup	ALPHA, %xmm0

	vmulpd	%xmm0 , %xmm4 , %xmm4
	vmulpd	%xmm0 , %xmm5 , %xmm5
	vmulpd	%xmm0 , %xmm6 , %xmm6
	vmulpd	%xmm0 , %xmm7 , %xmm7

        leaq    (CO1, LDC, 2), %rax     

#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1), %xmm4, %xmm4
	vaddpd 	           (CO1, LDC), %xmm5, %xmm5
	vaddpd 	               (%rax), %xmm6, %xmm6
	vaddpd 	          (%rax, LDC), %xmm7, %xmm7

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm6 ,  	(%rax)
	vmovups	%xmm7 ,  	(%rax, LDC)

	addq	$ 2*SIZE, CO1
.endm

/******************************************************************************************/
/******************************************************************************************/

.macro INIT1x4

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm5 , %xmm5 , %xmm5
	vxorpd		%xmm6 , %xmm6 , %xmm6
	vxorpd		%xmm7 , %xmm7 , %xmm7

.endm


.macro KERNEL1x4_SUB
	vmovsd	-12 * SIZE(BO), %xmm1
	vmovsd 	-16 * SIZE(AO), %xmm0
	vmovsd	-11 * SIZE(BO), %xmm2
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm4
	vmovsd	-10 * SIZE(BO), %xmm3
	vfmadd231sd  	%xmm0 ,%xmm2  , %xmm5
	vmovsd	 -9 * SIZE(BO), %xmm8
	vfmadd231sd  	%xmm0 ,%xmm3  , %xmm6
	addq		$ 4*SIZE, BO
	vfmadd231sd  	%xmm0 ,%xmm8  , %xmm7
	addq		$ 1*SIZE, AO

.endm


.macro SAVE1x4

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm5 , %xmm5
	vmulsd	%xmm0 , %xmm6 , %xmm6
	vmulsd	%xmm0 , %xmm7 , %xmm7

        leaq    (CO1, LDC, 2), %rax     

#if !defined(TRMMKERNEL)

	vaddsd 	                (CO1), %xmm4, %xmm4
	vaddsd 	           (CO1, LDC), %xmm5, %xmm5
	vaddsd 	               (%rax), %xmm6, %xmm6
	vaddsd 	          (%rax, LDC), %xmm7, %xmm7

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm6 ,  	(%rax)
	vmovsd	%xmm7 ,  	(%rax, LDC)

	addq	$ 1*SIZE, CO1
.endm


/******************************************************************************************/
/******************************************************************************************/

.macro INIT4x2

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm5 , %xmm5 , %xmm5
	vxorpd		%xmm6 , %xmm6 , %xmm6
	vxorpd		%xmm7 , %xmm7 , %xmm7

.endm


.macro KERNEL4x2_SUB
	vmovddup	-12 * SIZE(BO), %xmm2
	vmovups 	-16 * SIZE(AO), %xmm0
	vmovups 	-14 * SIZE(AO), %xmm1
	vmovddup	-11 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm4
	vfmadd231pd  	%xmm1 ,%xmm2  , %xmm5
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm6
	vfmadd231pd  	%xmm1 ,%xmm3  , %xmm7
	addq		$ 2*SIZE, BO
	addq		$ 4*SIZE, AO

.endm


.macro SAVE4x2

	vmovddup	ALPHA, %xmm0

	vmulpd	%xmm0 , %xmm4 , %xmm4
	vmulpd	%xmm0 , %xmm5 , %xmm5
	vmulpd	%xmm0 , %xmm6 , %xmm6
	vmulpd	%xmm0 , %xmm7 , %xmm7


#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1)     , %xmm4, %xmm4
	vaddpd 	        2 * SIZE(CO1)     , %xmm5, %xmm5
	vaddpd 	                (CO1, LDC), %xmm6, %xmm6
	vaddpd 	        2 * SIZE(CO1, LDC), %xmm7, %xmm7

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 , 2 * SIZE(CO1)
	vmovups	%xmm6 ,  	(CO1, LDC)
	vmovups	%xmm7 , 2 * SIZE(CO1, LDC)

	addq	$ 4*SIZE, CO1
.endm


/******************************************************************************************/
/******************************************************************************************/

.macro INIT2x2

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm6 , %xmm6 , %xmm6

.endm


.macro KERNEL2x2_SUB
	vmovddup	-12 * SIZE(BO), %xmm2
	vmovups 	-16 * SIZE(AO), %xmm0
	vmovddup	-11 * SIZE(BO), %xmm3
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm4
	vfmadd231pd  	%xmm0 ,%xmm3  , %xmm6
	addq		$ 2*SIZE, BO
	addq		$ 2*SIZE, AO

.endm


.macro SAVE2x2

	vmovddup	ALPHA, %xmm0

	vmulpd	%xmm0 , %xmm4 , %xmm4
	vmulpd	%xmm0 , %xmm6 , %xmm6


#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1)     , %xmm4, %xmm4
	vaddpd 	                (CO1, LDC), %xmm6, %xmm6

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm6 ,  	(CO1, LDC)

	addq	$ 2*SIZE, CO1
.endm

/******************************************************************************************/
/******************************************************************************************/

.macro INIT1x2

	vxorpd		%xmm4 , %xmm4 , %xmm4
	vxorpd		%xmm5 , %xmm5 , %xmm5

.endm


.macro KERNEL1x2_SUB
	vmovsd	-12 * SIZE(BO), %xmm1
	vmovsd 	-16 * SIZE(AO), %xmm0
	vmovsd	-11 * SIZE(BO), %xmm2
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm4
	vfmadd231sd  	%xmm0 ,%xmm2  , %xmm5
	addq		$ 2*SIZE, BO
	addq		$ 1*SIZE, AO

.endm


.macro SAVE1x2

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4
	vmulsd	%xmm0 , %xmm5 , %xmm5


#if !defined(TRMMKERNEL)

	vaddsd 	                (CO1), %xmm4, %xmm4
	vaddsd 	           (CO1, LDC), %xmm5, %xmm5

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)

	addq	$ 1*SIZE, CO1
.endm


/******************************************************************************************/
/******************************************************************************************/

.macro INIT4x1

	vxorpd		%ymm4 , %ymm4 , %ymm4
	vxorpd		%ymm5 , %ymm5 , %ymm5
	vxorpd		%ymm6 , %ymm6 , %ymm6
	vxorpd		%ymm7 , %ymm7 , %ymm7

.endm


.macro KERNEL4x1

	vbroadcastsd	-12 * SIZE(BO), %ymm0
	vbroadcastsd	-11 * SIZE(BO), %ymm1
	vbroadcastsd	-10 * SIZE(BO), %ymm2
	vbroadcastsd	-9  * SIZE(BO), %ymm3

	vfmadd231pd  	-16 * SIZE(AO) ,%ymm0  , %ymm4
	vfmadd231pd  	-12 * SIZE(AO) ,%ymm1  , %ymm5

	vbroadcastsd	-8  * SIZE(BO), %ymm0
	vbroadcastsd	-7  * SIZE(BO), %ymm1

	vfmadd231pd  	-8  * SIZE(AO) ,%ymm2  , %ymm6
	vfmadd231pd  	-4  * SIZE(AO) ,%ymm3  , %ymm7

	vbroadcastsd	-6  * SIZE(BO), %ymm2
	vbroadcastsd	-5  * SIZE(BO), %ymm3

	vfmadd231pd  	 0  * SIZE(AO) ,%ymm0  , %ymm4
	vfmadd231pd  	 4  * SIZE(AO) ,%ymm1  , %ymm5
	vfmadd231pd  	 8  * SIZE(AO) ,%ymm2  , %ymm6
	vfmadd231pd  	 12 * SIZE(AO) ,%ymm3  , %ymm7

	addq		$ 8 *SIZE, BO
	addq		$ 32*SIZE, AO

.endm


.macro KERNEL4x1_SUB
	vbroadcastsd	-12 * SIZE(BO), %ymm2
	vmovups 	-16 * SIZE(AO), %ymm0
	vfmadd231pd  	%ymm0 ,%ymm2  , %ymm4
	addq		$ 1*SIZE, BO
	addq		$ 4*SIZE, AO

.endm


.macro SAVE4x1

	vbroadcastsd	ALPHA, %ymm0

	vaddpd	%ymm4,%ymm5, %ymm4 
	vaddpd	%ymm6,%ymm7, %ymm6 
	vaddpd	%ymm4,%ymm6, %ymm4 

	vmulpd	%ymm0 , %ymm4 , %ymm4


#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1)     , %ymm4, %ymm4

#endif

	vmovups	%ymm4 ,  	(CO1)

	addq	$ 4*SIZE, CO1
.endm


/******************************************************************************************/
/******************************************************************************************/

.macro INIT2x1

	vxorpd		%xmm4 , %xmm4 , %xmm4

.endm


.macro KERNEL2x1_SUB
	vmovddup	-12 * SIZE(BO), %xmm2
	vmovups 	-16 * SIZE(AO), %xmm0
	vfmadd231pd  	%xmm0 ,%xmm2  , %xmm4
	addq		$ 1*SIZE, BO
	addq		$ 2*SIZE, AO

.endm


.macro SAVE2x1

	vmovddup	ALPHA, %xmm0

	vmulpd	%xmm0 , %xmm4 , %xmm4


#if !defined(TRMMKERNEL)

	vaddpd 	                (CO1)     , %xmm4, %xmm4

#endif

	vmovups	%xmm4 ,  	(CO1)

	addq	$ 2*SIZE, CO1
.endm


/******************************************************************************************/
/******************************************************************************************/

.macro INIT1x1

	vxorpd		%xmm4 , %xmm4 , %xmm4

.endm


.macro KERNEL1x1_SUB
	vmovsd	-12 * SIZE(BO), %xmm1
	vmovsd 	-16 * SIZE(AO), %xmm0
	vfmadd231sd  	%xmm0 ,%xmm1  , %xmm4
	addq		$ 1*SIZE, BO
	addq		$ 1*SIZE, AO

.endm


.macro SAVE1x1

	vmovsd	ALPHA, %xmm0

	vmulsd	%xmm0 , %xmm4 , %xmm4


#if !defined(TRMMKERNEL)

	vaddsd 	                (CO1), %xmm4, %xmm4

#endif

	vmovsd	%xmm4 ,  	(CO1)

	addq	$ 1*SIZE, CO1
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

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC

	vmovups	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC

#endif

	movq    %rsp, SP      # save old stack
        subq    $128 + L_BUFFER_SIZE, %rsp
        andq    $-4096, %rsp    # align stack

        STACK_TOUCH

	cmpq	$ 0, OLD_M
	je	.L999

	cmpq	$ 0, OLD_N
	je	.L999

	cmpq	$ 0, OLD_K
	je	.L999

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	vmovsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $12,  %rdi
        divq    %rdi                    //    N / 12
        movq    %rax, Ndiv12             //    N / 12
        movq    %rdx, Nmod12             //    N % 12


	movq	Ndiv12,  J
	cmpq	$ 0, J
	je	.L4_0
	ALIGN_4

.L12_01:
        // copy to sub buffer
        movq    K, %rax
        salq    $2,%rax                 // K * 4 ; read 2 values
        movq    B, BO1
        leaq    (B,%rax, SIZE), BO2     // next offset to BO2
        leaq    (BO2,%rax, SIZE), BO3     // next offset to BO2


        leaq    BUFFER1, BO             // first buffer to BO
        movq    K, %rax
	sarq	$1 , %rax		// K / 2
	jz	.L12_01a_2
        ALIGN_4

.L12_01a_1:

        prefetcht0 512(BO1)
        prefetcht0 512(BO2)
        prefetcht0 512(BO3)
        prefetchw  512(BO)


	vmovups	0 * SIZE(BO1), %ymm1
	vmovups	4 * SIZE(BO1), %ymm5
	vmovups	0 * SIZE(BO2), %ymm2
	vmovups	4 * SIZE(BO2), %ymm6
	vmovups	0 * SIZE(BO3), %ymm3
	vmovups	4 * SIZE(BO3), %ymm7

	vmovups	%ymm1,  0 * SIZE(BO)
	vmovups	%ymm2,  4 * SIZE(BO)
	vmovups	%ymm3,  8 * SIZE(BO)

	vmovups	%ymm5, 12 * SIZE(BO)
	vmovups	%ymm6, 16 * SIZE(BO)
	vmovups	%ymm7, 20 * SIZE(BO)

	addq	$ 8 * SIZE ,BO1
	addq	$ 8 * SIZE ,BO2
	addq	$ 8 * SIZE ,BO3
	addq    $ 24 *SIZE ,BO

	decq	%rax
	jnz	.L12_01a_1



.L12_01a_2:

	movq    K, %rax
        andq    $1, %rax                // K % 2
        jz      .L12_03c
        ALIGN_4


.L12_02b:

	vmovups	0 * SIZE(BO1), %ymm1
	vmovups	0 * SIZE(BO2), %ymm2
	vmovups	0 * SIZE(BO3), %ymm3
	vmovups	%ymm1, 0 * SIZE(BO)
	vmovups	%ymm2, 4 * SIZE(BO)
	vmovups	%ymm3, 8 * SIZE(BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO2
	addq	$ 4*SIZE,BO3
	addq	$ 12*SIZE,BO
	decq	%rax
	jnz	.L12_02b

.L12_03c:

	movq	BO3, B			// next offset of B

.L12_10:
	movq	C, CO1
	leaq	(C, LDC, 8), C		 
	leaq	(C, LDC, 4), C		// c += 12 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L12_20

	ALIGN_4

.L12_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

        movq    K, %rax

	sarq $3, %rax			//  K / 8
	cmpq $2, %rax

	jl	.L12_13


	KERNEL4x12_I
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_M2

	KERNEL4x12_M1
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_M2

	subq $2, %rax
	je	.L12_12a

	ALIGN_5
.L12_12:

	KERNEL4x12_M1
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_M2

	KERNEL4x12_M1
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_M2

	dec	%rax
	jne	.L12_12

.L12_12a:

	KERNEL4x12_M1
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_M2

	KERNEL4x12_M1
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_E

	jmp .L12_16


.L12_13:

	test $1, %rax
	jz .L12_14

	KERNEL4x12_I
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_M2

	KERNEL4x12_M1
	KERNEL4x12_M2
	KERNEL4x12_M1
	KERNEL4x12_E

	jmp .L12_16


.L12_14:

	INIT4x12


.L12_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L12_19

	ALIGN_4

.L12_17:

	KERNEL4x12_SUB

	dec	%rax
	jne	.L12_17
	ALIGN_4


.L12_19:

	SAVE4x12

	decq	I			# i --
	jne	.L12_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L12_20:
	// Test rest of M

	testq	$3, M
	jz	.L12_100			// to next 16 lines of N


.L12_30:
	testq	$2, M		
	jz	.L12_40

	ALIGN_4

.L12_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT2x12

        movq    K, %rax

	sarq	$3, %rax
	je	.L12_36
	ALIGN_4

.L12_32:

	KERNEL2x12_SUB
	KERNEL2x12_SUB
	KERNEL2x12_SUB
	KERNEL2x12_SUB

	KERNEL2x12_SUB
	KERNEL2x12_SUB
	KERNEL2x12_SUB
	KERNEL2x12_SUB

	dec %rax
	jne	.L12_32
	ALIGN_4

.L12_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L12_39

	ALIGN_4

.L12_37:

	KERNEL2x12_SUB

	dec %rax
	jne	.L12_37
	ALIGN_4


.L12_39:

	SAVE2x12

	ALIGN_4

.L12_40:
	testq	$1, M		
	jz	.L12_100		// to next 3 lines of N

	ALIGN_4

.L12_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT1x12

        movq    K, %rax

	sarq	$3,%rax
	je	.L12_46

	ALIGN_4

.L12_42:

	KERNEL1x12_SUB
	KERNEL1x12_SUB
	KERNEL1x12_SUB
	KERNEL1x12_SUB

	KERNEL1x12_SUB
	KERNEL1x12_SUB
	KERNEL1x12_SUB
	KERNEL1x12_SUB


	dec %rax
	jne	.L12_42
	ALIGN_4

.L12_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L12_49

	ALIGN_4

.L12_47:

	KERNEL1x12_SUB

	dec	%rax
	jne	.L12_47
	ALIGN_4


.L12_49:

	SAVE1x12

	ALIGN_4
	
.L12_100:

	decq	J			// j --
	jg	.L12_01


.L4_0:

	cmpq	$ 0, Nmod12		// N % 12 == 0
	je	.L999

	movq	Nmod12, J		
	sarq	$2, J			// j = j / 4
	je	.L2_0

.L4_10:
	movq	C, CO1
	leaq	(C, LDC, 4), C		// c += 4 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L4_20

	ALIGN_4

.L4_11:
        movq    B, BO        
        addq    $12 * SIZE, BO

        movq    K, %rax

	sarq	$3, %rax			//  K / 8
	cmpq    $2, %rax
	jl	.L4_13


	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subq $2, %rax
	je	.L4_12a

	ALIGN_5

.L4_12:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	dec	%rax
	jne	.L4_12

.L4_12a:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	jmp .L4_16


.L4_13:

	test $1, %rax
	jz .L4_14

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	jmp .L4_16


.L4_14:

	INIT4x4


.L4_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L4_19

	ALIGN_4

.L4_17:

	KERNEL4x4_SUB

	dec	%rax
	jne	.L4_17
	ALIGN_4


.L4_19:

	SAVE4x4

	decq	I			# i --
	jg	.L4_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L4_20:
	// Test rest of M

	testq	$3, M
	jz	.L4_100			// to next 16 lines of N


.L4_30:
	testq	$2, M		
	jz	.L4_40

	ALIGN_4

.L4_31:
        movq    B, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT2x4

        movq    K, %rax

	sarq	$3, %rax
	je	.L4_36
	ALIGN_4

.L4_32:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	dec %rax
	jne	.L4_32
	ALIGN_4

.L4_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L4_39

	ALIGN_4

.L4_37:

	KERNEL2x4_SUB

	dec %rax
	jne	.L4_37


.L4_39:

	SAVE2x4

.L4_40:
	testq	$1, M		
	jz	.L4_100		// to next 3 lines of N

	ALIGN_4

.L4_41:
        movq    B, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT1x4

        movq    K, %rax

	sarq	$3,%rax
	je	.L4_46

	ALIGN_4

.L4_42:

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	dec %rax
	jne	.L4_42
	ALIGN_4

.L4_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L4_49

	ALIGN_4

.L4_47:

	KERNEL1x4_SUB

	dec	%rax
	jne	.L4_47
	ALIGN_4


.L4_49:

	SAVE1x4

	ALIGN_4
	
.L4_100:

	movq	K, %rax
	salq	$2, %rax		// * 4
	leaq	(B , %rax, SIZE), B
	decq	J			// j --
	jg	.L4_10




/***************************************************************************************************************/

.L2_0:

	movq	Nmod12, J		
	testq	$2, J
	je	.L1_0

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L2_20

	ALIGN_4

.L2_11:
        movq    B, BO        
        addq    $12 * SIZE, BO

	INIT4x2

        movq    K, %rax
	sarq $3, %rax			//  K / 8

	je	.L2_16

	ALIGN_5

.L2_12:

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	dec	%rax
	jne	.L2_12


.L2_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_19

	ALIGN_4

.L2_17:

	KERNEL4x2_SUB

	dec	%rax
	jne	.L2_17
	ALIGN_4


.L2_19:

	SAVE4x2

	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$3, M
	jz	.L2_100			// to next 16 lines of N


.L2_30:
	testq	$2, M		
	jz	.L2_40

	ALIGN_4

.L2_31:
        movq    B, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT2x2

        movq    K, %rax

	sarq	$3, %rax
	je	.L2_36
	ALIGN_4

.L2_32:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	dec %rax
	jne	.L2_32

.L2_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_39

	ALIGN_4

.L2_37:

	KERNEL2x2_SUB

	dec %rax
	jne	.L2_37


.L2_39:

	SAVE2x2

.L2_40:
	testq	$1, M		
	jz	.L2_100		// to next 3 lines of N

.L2_41:
        movq    B, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT1x2

        movq    K, %rax

	sarq	$3,%rax
	je	.L2_46

	ALIGN_4

.L2_42:

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	dec %rax
	jne	.L2_42

.L2_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_49

	ALIGN_4

.L2_47:

	KERNEL1x2_SUB

	dec	%rax
	jne	.L2_47

.L2_49:

	SAVE1x2

.L2_100:

	movq	K, %rax
	salq	$1, %rax		// * 2
	leaq	(B , %rax, SIZE), B

/***************************************************************************************************************/

.L1_0:

	movq	Nmod12, J		
	testq	$1, J
	je	.L999

.L1_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L1_20

	ALIGN_4

.L1_11:
        movq    B, BO        
        addq    $12 * SIZE, BO

	INIT4x1

        movq    K, %rax

	sarq	$3, %rax			//  K / 8
	je	.L1_16

	ALIGN_5

.L1_12:

	KERNEL4x1

	dec	%rax
	jne	.L1_12


.L1_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_19

	ALIGN_4

.L1_17:

	KERNEL4x1_SUB

	dec	%rax
	jne	.L1_17
	ALIGN_4


.L1_19:

	SAVE4x1

	decq	I			# i --
	jg	.L1_11

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$3, M
	jz	.L1_100	


.L1_30:
	testq	$2, M		
	jz	.L1_40

	ALIGN_4

.L1_31:
        movq    B, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT2x1

        movq    K, %rax

	sarq	$3, %rax
	je	.L1_36
	ALIGN_4

.L1_32:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB


	dec %rax
	jne	.L1_32

.L1_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_39

	ALIGN_4

.L1_37:

	KERNEL2x1_SUB

	dec %rax
	jne	.L1_37

.L1_39:

	SAVE2x1

.L1_40:
	testq	$1, M		
	jz	.L1_100		// to next 3 lines of N


.L1_41:
        movq    B, BO             // first buffer to BO
        addq    $12 * SIZE, BO

	INIT1x1

        movq    K, %rax

	sarq	$3,%rax
	je	.L1_46

	ALIGN_4

.L1_42:

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	dec %rax
	jne	.L1_42

.L1_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	ALIGN_4

.L1_47:

	KERNEL1x1_SUB

	dec	%rax
	jne	.L1_47


.L1_49:

	SAVE1x1

.L1_100:




.L999:
	vzeroupper

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

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	vmovsd	OLD_OFFSET, %xmm12
#endif
	vmovups	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	vmovsd	STACKSIZE + 16(%rsp), %xmm12
#endif

#endif

	movq    %rsp, SP      # save old stack
        subq    $128 + L_BUFFER_SIZE, %rsp
        andq    $-4096, %rsp    # align stack

        STACK_TOUCH

	cmpq	$ 0, OLD_M
	je	.L999

	cmpq	$ 0, OLD_N
	je	.L999

	cmpq	$ 0, OLD_K
	je	.L999

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	vmovsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $4,  %rdi
        divq    %rdi                     //    N / 4
        movq    %rax, Ndiv12             //    N / 4
        movq    %rdx, Nmod12             //    N % 4

#ifdef TRMMKERNEL
        vmovsd  %xmm12, OFFSET
        vmovsd  %xmm12, KK
#ifndef LEFT
        negq    KK
#endif  
#endif



	movq	Ndiv12,  J
	cmpq	$ 0, J
	je	.L2_0
	ALIGN_4

.L4_10:
	movq	C, CO1
	leaq	(C, LDC, 4), C		// c += 4 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L4_20

	ALIGN_4

.L4_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,4), BO		// add number of values in B
	leaq	(AO,%rax,4), AO		// add number of values in A
#endif


#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $4, %rax        // number of values in AO
#else
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	sarq	$3, %rax			//  K / 8
	cmpq    $2, %rax
	jl	.L4_13


	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subq $2, %rax
	je	.L4_12a

	ALIGN_5

.L4_12:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	dec	%rax
	jne	.L4_12

.L4_12a:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	jmp .L4_16


.L4_13:

	test $1, %rax
	jz .L4_14

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	jmp .L4_16


.L4_14:

	INIT4x4


.L4_16:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L4_19

	ALIGN_4

.L4_17:

	KERNEL4x4_SUB

	dec	%rax
	jne	.L4_17
	ALIGN_4


.L4_19:

	SAVE4x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax + SIZE
        leaq    (BO, %rax, 4), BO		// number of values in B
        leaq    (AO, %rax, 4), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK				// number of values in A
#endif

	decq	I			# i --
	jg	.L4_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L4_20:
	// Test rest of M

	testq	$3, M
	jz	.L4_100			// to next 16 lines of N


.L4_30:
	testq	$2, M		
	jz	.L4_40

	ALIGN_4

.L4_31:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,4), BO		// add number of values in B
	leaq	(AO,%rax,2), AO		// add number of values in A
#endif


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
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	INIT2x4

	sarq	$3, %rax
	je	.L4_36
	ALIGN_4

.L4_32:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	dec %rax
	jne	.L4_32
	ALIGN_4

.L4_36:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L4_39

	ALIGN_4

.L4_37:

	KERNEL2x4_SUB

	dec %rax
	jne	.L4_37


.L4_39:

	SAVE2x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax + SIZE
        leaq    (BO, %rax, 4), BO		// number of values in B
        leaq    (AO, %rax, 2), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK				// number of values in A
#endif


.L4_40:
	testq	$1, M		
	jz	.L4_100		// to next 3 lines of N

	ALIGN_4

.L4_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,4), BO		// add number of values in B
	leaq	(AO,%rax,1), AO		// add number of values in A
#endif


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
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	INIT1x4

	sarq	$3,%rax
	je	.L4_46

	ALIGN_4

.L4_42:

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	dec %rax
	jne	.L4_42
	ALIGN_4

.L4_46:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L4_49

	ALIGN_4

.L4_47:

	KERNEL1x4_SUB

	dec	%rax
	jne	.L4_47
	ALIGN_4


.L4_49:

	SAVE1x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax + SIZE
        leaq    (BO, %rax, 4), BO		// number of values in B
        leaq    (AO, %rax, 1), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK				// number of values in A
#endif

.L4_100:

#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $4, KK				// number of values in B
#endif


	movq	K, %rax
	salq	$2, %rax		// * 4
	leaq	(B , %rax, SIZE), B
	decq	J			// j --
	jg	.L4_10




/***************************************************************************************************************/

.L2_0:

	movq	Nmod12, J		
	testq	$2, J
	je	.L1_0

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif


	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L2_20

	ALIGN_4

.L2_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,2), BO		// add number of values in B
	leaq	(AO,%rax,4), AO		// add number of values in A
#endif


#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $4, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	INIT4x2

	sarq $3, %rax			//  K / 8

	je	.L2_16

	ALIGN_5

.L2_12:

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	dec	%rax
	jne	.L2_12


.L2_16:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_19

	ALIGN_4

.L2_17:

	KERNEL4x2_SUB

	dec	%rax
	jne	.L2_17
	ALIGN_4


.L2_19:

	SAVE4x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax + SIZE
        leaq    (BO, %rax, 2), BO		// number of values in B
        leaq    (AO, %rax, 4), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK				// number of values in A
#endif


	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$3, M
	jz	.L2_100			// to next 16 lines of N


.L2_30:
	testq	$2, M		
	jz	.L2_40

	ALIGN_4

.L2_31:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,2), BO		// add number of values in B
	leaq	(AO,%rax,2), AO		// add number of values in A
#endif


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

	INIT2x2

	sarq	$3, %rax
	je	.L2_36
	ALIGN_4

.L2_32:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	dec %rax
	jne	.L2_32

.L2_36:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_39

	ALIGN_4

.L2_37:

	KERNEL2x2_SUB

	dec %rax
	jne	.L2_37


.L2_39:

	SAVE2x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax + SIZE
        leaq    (BO, %rax, 2), BO		// number of values in B
        leaq    (AO, %rax, 2), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK				// number of values in A
#endif


.L2_40:
	testq	$1, M		
	jz	.L2_100		// to next 3 lines of N

.L2_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,2), BO		// add number of values in B
	leaq	(AO,%rax,1), AO		// add number of values in A
#endif


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

	INIT1x2

	sarq	$3,%rax
	je	.L2_46

	ALIGN_4

.L2_42:

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	dec %rax
	jne	.L2_42

.L2_46:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_49

	ALIGN_4

.L2_47:

	KERNEL1x2_SUB

	dec	%rax
	jne	.L2_47

.L2_49:

	SAVE1x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax * SIZE
        leaq    (BO, %rax, 2), BO		// number of values in B
        leaq    (AO, %rax, 1), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK				// number of values in A
#endif


.L2_100:


#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $2, KK				// number of values in B
#endif

	movq	K, %rax
	salq	$1, %rax		// * 2
	leaq	(B , %rax, SIZE), B

/***************************************************************************************************************/

.L1_0:

	movq	Nmod12, J		
	testq	$1, J
	je	.L999

.L1_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$2, I			// i = m / 4
	je	.L1_20

	ALIGN_4

.L1_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,1), BO		// add number of values in B
	leaq	(AO,%rax,4), AO		// add number of values in A
#endif


#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $4, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	INIT4x1

	sarq	$3, %rax			//  K / 8
	je	.L1_16

	ALIGN_5

.L1_12:

	KERNEL4x1

	dec	%rax
	jne	.L1_12


.L1_16:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_19

	ALIGN_4

.L1_17:

	KERNEL4x1_SUB

	dec	%rax
	jne	.L1_17
	ALIGN_4


.L1_19:

	SAVE4x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax * SIZE
        leaq    (BO, %rax, 1), BO		// number of values in B
        leaq    (AO, %rax, 4), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK				// number of values in A
#endif


	decq	I			# i --
	jg	.L1_11

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$3, M
	jz	.L1_100	


.L1_30:
	testq	$2, M		
	jz	.L1_40

	ALIGN_4

.L1_31:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,1), BO		// add number of values in B
	leaq	(AO,%rax,2), AO		// add number of values in A
#endif


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

	INIT2x1

	sarq	$3, %rax
	je	.L1_36
	ALIGN_4

.L1_32:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB


	dec %rax
	jne	.L1_32

.L1_36:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_39

	ALIGN_4

.L1_37:

	KERNEL2x1_SUB

	dec %rax
	jne	.L1_37

.L1_39:

	SAVE2x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax * SIZE
        leaq    (BO, %rax, 1), BO		// number of values in B
        leaq    (AO, %rax, 2), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK				// number of values in A
#endif


.L1_40:
	testq	$1, M		
	jz	.L1_100		// to next 3 lines of N


.L1_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    B, BO        
        addq    $12 * SIZE, BO
#else
        movq    B, BO        
        addq    $12 * SIZE, BO
        movq    KK, %rax
	salq	$3, %rax		// rax * SIZE
	leaq	(BO,%rax,1), BO		// add number of values in B
	leaq	(AO,%rax,1), AO		// add number of values in A
#endif


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

	INIT1x1

	sarq	$3,%rax
	je	.L1_46

	ALIGN_4

.L1_42:

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	dec %rax
	jne	.L1_42

.L1_46:
        movq    KKK, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	ALIGN_4

.L1_47:

	KERNEL1x1_SUB

	dec	%rax
	jne	.L1_47


.L1_49:

	SAVE1x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	salq	$3, %rax			// rax * SIZE
        leaq    (BO, %rax, 1), BO		// number of values in B
        leaq    (AO, %rax, 1), AO		// number of values in A
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK				// number of values in A
#endif



.L1_100:


#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $1, KK				// number of values in B
#endif



.L999:

	vzeroupper

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

	EPILOGUE





#endif
