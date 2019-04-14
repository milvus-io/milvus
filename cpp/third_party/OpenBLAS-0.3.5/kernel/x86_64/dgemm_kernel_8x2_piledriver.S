/***************************************************************************
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
*****************************************************************************/


/*********************************************************************
*
* 2013/11/13 Saar
*        BLASTEST               : OK
*        CTEST                  : OK
*        TEST                   : OK
*
*
* 2013/10/31 Saar
*
* Parameter:
* 	UNROLL_M	8
*	UNROLL_N	2
*	DGEMM_P		768
*	DGEMM_Q		168
*	DGEMM_R		12288
*	A_PR1		512
*	B_PR1		256
*
* Performance at m x n on AMD 8320 (ACML-Version: 5.3.1):
* 
* 4608x4608	83.9	GFLOPS with 8 threads on 4 modules (ACML: 78.4 GFLOPS)
* 4608x4608	80.9	GFLOPS with 4 threads on 4 modules (ACML: 78.4 GFLOPS)
* 4608x4608	41.3    GFLOPS with 2 threads on 2 modules (ACML: 40.9 GFLOPS)
* 4608x4608	20.7	GFLOPS with 1 threads on 1 modules (ACML: 20.8 GFLOPS)
*
* Performance at m x n on AMD 6380  (ACML-Version: 5.3.1):
* 
* 13824x13824  234.5	GFLOPS with 32 threads on 16 modules (ACML:  88.5 GFLOPS) !strange thermal behavior
* 13824x13824  241.9	GFLOPS with 16 threads on 16 modules (ACML: 191.5 GFLOPS) !strange thermal behavior
* 9216x9216    137.6	GFLOPS with  8 threads on  8 modules (ACML: 106.5 GFLOPS)
* 4608x4608	75.7	GFLOPS with  4 threads on  4 modules (ACML:  56.3 GFLOPS)
* 4608x4608	38.6	GFLOPS with  2 threads on  2 modules (ACML:  34.1 GFLOPS)
* 4608x4608	19.6	GFLOPS with  1 threads on  1 modules (ACML:  18.3 GFLOPS)
*
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

#define L_BUFFER_SIZE 8192
#define LB2_OFFSET    4096

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

#define VFMADD231PD_( y1,y2,y0 ) vfmaddpd y0,y1,y2,y0

#define VFMADD231SD_( x1,x2,x0 ) vfmaddsd x0,x1,x2,x0

#else

#define VFMADD231PD_( y1,y2,y0 ) vfmadd231pd y2,y1,y0

#define VFMADD231SD_( x1,x2,x0 ) vfmadd231sd x2,x1,x0

#endif




#define	A_PR1	512
#define	B_PR1	256
#define	C_PR1	64

.macro INIT8x3
	vxorpd	%xmm4 , %xmm4 , %xmm4
	vxorpd	%xmm5 , %xmm5 , %xmm5
	vxorpd	%xmm6 , %xmm6 , %xmm6
	vxorpd	%xmm7 , %xmm7 , %xmm7
	vxorpd	%xmm8 , %xmm8 , %xmm8
	vxorpd	%xmm9 , %xmm9 , %xmm9
	vxorpd	%xmm10, %xmm10, %xmm10
	vxorpd	%xmm11, %xmm11, %xmm11
	vxorpd	%xmm12, %xmm12, %xmm12
	vxorpd	%xmm13, %xmm13, %xmm13
	vxorpd	%xmm14, %xmm14, %xmm14
	vxorpd	%xmm15, %xmm15, %xmm15
.endm

.macro KERNEL8x3_INIT 
	vmovddup	-12 * SIZE(BO), %xmm1
	vmovups 	-16 * SIZE(AO), %xmm0
	prefetcht0	A_PR1(AO)
	vmulpd  	%xmm1,%xmm0,%xmm4
	vmovddup	-11 * SIZE(BO), %xmm2
	vmulpd  	%xmm2,%xmm0,%xmm5
	vmovddup	-10 * SIZE(BO), %xmm3
	vmulpd  	%xmm3,%xmm0,%xmm6
	vmovups 	-14 * SIZE(AO), %xmm0
	vmulpd  	%xmm1,%xmm0,%xmm7
	vmulpd  	%xmm2,%xmm0,%xmm8
	vmulpd  	%xmm3,%xmm0,%xmm9
	vmovups 	-12 * SIZE(AO), %xmm0
	vmulpd  	%xmm1,%xmm0,%xmm10
	vmulpd  	%xmm2,%xmm0,%xmm11
	addq		$ 3 * SIZE, BO
	vmulpd  	%xmm3,%xmm0,%xmm12
	vmovups 	-10 * SIZE(AO), %xmm0
	vmulpd  	%xmm1,%xmm0,%xmm13
	vmovddup	-12 * SIZE(BO), %xmm1
	vmulpd  	%xmm2,%xmm0,%xmm14
	vmovddup	-11 * SIZE(BO), %xmm2
	vmulpd  	%xmm3,%xmm0,%xmm15
.endm


.macro KERNEL8x3_M1 
	vmovups 	-16 * SIZE(AO), %xmm0
	prefetcht0	A_PR1(AO)
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	-14 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	-12 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	-10 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	-12 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	-11 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro KERNEL8x3_M2 
	vmovups 	 -8 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+64(AO)
	vmovddup	-10 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	 -6 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	 -4 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	 -2 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	 -9 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	 -8 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm


.macro KERNEL8x3_M3 
	vmovups 	  0 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+128(AO)
	vmovddup	 -7 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	  2 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	  4 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	  6 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	 -6 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	 -5 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro KERNEL8x3_M4 
	vmovups 	  8 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+192(AO)
	vmovddup	 -4 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	 10 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	 12 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	 14 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	 -3 * SIZE(BO), %xmm1
	addq		$ 32 * SIZE, AO
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	 -2 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro KERNEL8x3_M5 
	vmovups 	-16 * SIZE(AO), %xmm0
	prefetcht0	A_PR1(AO)
	vmovddup	 -1 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	-14 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	-12 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	-10 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	  0 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	  1 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro KERNEL8x3_M6 
	vmovups 	 -8 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+64(AO)
	vmovddup	  2 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	 -6 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	 -4 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	 -2 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	  3 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	  4 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm


.macro KERNEL8x3_M7 
	vmovups 	  0 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+128(AO)
	vmovddup	  5 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	  2 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	  4 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	  6 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	  6 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	  7 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro KERNEL8x3_M8 
	vmovups 	  8 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+192(AO)
	vmovddup	  8 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	 10 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	 12 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	 14 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	vmovddup	  9 * SIZE(BO), %xmm1
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	vmovddup	 10 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
	vmovddup	 11 * SIZE(BO), %xmm3
	addq		$ 32 * SIZE, AO
	addq		$ 24 * SIZE, BO
.endm


.macro KERNEL8x3_E 
	vmovups 	  8 * SIZE(AO), %xmm0
	prefetcht0	A_PR1+192(AO)
	vmovddup	  8 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	 10 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	 12 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	 14 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	addq		$ 32 * SIZE, AO
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	addq		$ 21 * SIZE, BO
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro KERNEL8x3_SUBN 
	vmovddup	-12 * SIZE(BO), %xmm1
	vmovups 	-16 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm4 )
	vmovddup	-11 * SIZE(BO), %xmm2
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm5 )
	vmovddup	-10 * SIZE(BO), %xmm3
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm6 )
	vmovups 	-14 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm7 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm8 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm9 )
	vmovups 	-12 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm10 )
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm11 )
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm12 )
	vmovups 	-10 * SIZE(AO), %xmm0
	VFMADD231PD_(  	%xmm1,%xmm0,%xmm13 )
	addq		$ 3 * SIZE, BO
	VFMADD231PD_(  	%xmm2,%xmm0,%xmm14 )
	addq		$ 8 * SIZE, AO
	VFMADD231PD_(  	%xmm3,%xmm0,%xmm15 )
.endm

.macro	SAVE8x3
	vmovddup	ALPHA, %xmm0


	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6

	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddpd 4 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddpd 6 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddpd 4 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddpd 6 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14

	vfmaddpd 2 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9
	vfmaddpd 4 * SIZE(CO1, LDC, 2),%xmm0, %xmm12,%xmm12
	vfmaddpd 6 * SIZE(CO1, LDC, 2),%xmm0, %xmm15,%xmm15

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)
	vmovups	%xmm10, 4 * SIZE(CO1)
	vmovups	%xmm13, 6 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)
	vmovups	%xmm11, 4 * SIZE(CO1, LDC)
	vmovups	%xmm14, 6 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 2 * SIZE(CO1, LDC, 2)
	vmovups	%xmm12, 4 * SIZE(CO1, LDC, 2)
	vmovups	%xmm15, 6 * SIZE(CO1, LDC, 2)

	prefetcht0	 C_PR1(CO1)
	prefetcht0	 C_PR1(CO1,LDC)
	prefetcht0	 C_PR1(CO1,LDC,2)

	addq	$ 8 * SIZE, CO1		# coffset += 8
.endm


/*******************************************************************************************/

#define KERNEL4x3_1(xx) \
	vmovddup	 -6 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -5 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddpd  	%xmm9,%xmm3,%xmm0,%xmm9 ;\

#define KERNEL4x3_2(xx) \
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddpd  	%xmm9,%xmm3,%xmm0,%xmm9 ;\

#define KERNEL4x3_3(xx) \
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	  2 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	 -6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddpd  	%xmm9,%xmm3,%xmm0,%xmm9 ;\

#define KERNEL4x3_4(xx) \
	vmovddup	  3 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  4 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	  5 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	 -2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddpd  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	addq	$12, BI				  ;\
	addq	$16, %rax 			  ;\

#define KERNEL4x3_SUB(xx) \
	vmovddup	 -6 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -5 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddpd  	%xmm9,%xmm3,%xmm0,%xmm9 ;\





/*******************************************************************************************/

#define KERNEL2x3_1(xx) \
	vmovddup	 -6 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -5 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL2x3_2(xx) \
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL2x3_3(xx) \
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	  2 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL2x3_4(xx) \
	vmovddup	  3 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  4 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	  5 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	addq	$12, BI				  ;\
	addq	$8, %rax 			  ;\

#define KERNEL2x3_SUB(xx) \
	vmovddup	 -6 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -5 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddpd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

/*******************************************************************************************/

#define KERNEL1x3_1(xx) \
	vmovsd	 -6 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	 -5 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovsd	 -4 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddsd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL1x3_2(xx) \
	vmovsd	 -3 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-15 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	 -2 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovsd	 -1 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddsd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL1x3_3(xx) \
	vmovsd	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovsd	  2 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddsd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL1x3_4(xx) \
	vmovsd	  3 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-13 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	  4 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovsd	  5 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddsd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	addq	$12, BI				  ;\
	addq	$4, %rax 			  ;\

#define KERNEL1x3_SUB(xx) \
	vmovsd	 -6 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	 -5 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovsd	 -4 * SIZE(BO, BI, 8), %xmm3 ;\
	vfmaddsd  	%xmm6,%xmm3,%xmm0,%xmm6 ;\



/*******************************************************************************************
* 2 lines of N
*******************************************************************************************/

#define KERNEL8x2_1(xx) \
	prefetcht0	A_PR1(AO,%rax,8)	;\
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddpd  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddpd  	%xmm14,%xmm2,%xmm0,%xmm14 ;\

#define KERNEL8x2_2(xx) \
	prefetcht0	A_PR1+64(AO,%rax,8)	;\
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	 -6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	 -4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddpd  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	 -2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddpd  	%xmm14,%xmm2,%xmm0,%xmm14 ;\

#define KERNEL8x2_3(xx) \
	prefetcht0	A_PR1+128(AO,%rax,8)	;\
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	  0 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	  2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	  4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddpd  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	  6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddpd  	%xmm14,%xmm2,%xmm0,%xmm14 ;\

#define KERNEL8x2_4(xx) \
	prefetcht0	A_PR1+192(AO,%rax,8)	;\
	vmovddup	  2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	  8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	 10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	 12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddpd  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	 14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddpd  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	addq	$8, BI				  ;\
	addq	$32, %rax 			  ;\

#define KERNEL8x2_SUB(xx) \
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddpd  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddpd  	%xmm14,%xmm2,%xmm0,%xmm14 ;\


/*******************************************************************************************/

#define KERNEL4x2_1(xx) \
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\

#define KERNEL4x2_2(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\

#define KERNEL4x2_3(xx) \
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	 -6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\

#define KERNEL4x2_4(xx) \
	vmovddup	  2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	 -2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	addq	$8, BI				  ;\
	addq	$16, %rax 			  ;\

#define KERNEL4x2_SUB(xx) \
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddpd  	%xmm8,%xmm2,%xmm0,%xmm8 ;\


/*******************************************************************************************/

#define KERNEL2x2_1(xx) \
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL2x2_2(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL2x2_3(xx) \
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL2x2_4(xx) \
	vmovddup	  2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	  3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	addq	$8, BI				  ;\
	addq	$8, %rax 			  ;\

#define KERNEL2x2_SUB(xx) \
	vmovddup	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovddup	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddpd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

/*******************************************************************************************/

#define KERNEL1x2_1(xx) \
	vmovsd	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL1x2_2(xx) \
	vmovsd	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-15 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	 -1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL1x2_3(xx) \
	vmovsd	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	  1 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL1x2_4(xx) \
	vmovsd	  2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-13 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	  3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	addq	$8, BI				  ;\
	addq	$4, %rax 			  ;\

#define KERNEL1x2_SUB(xx) \
	vmovsd	 -4 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovsd	 -3 * SIZE(BO, BI, 8), %xmm2 ;\
	vfmaddsd  	%xmm5,%xmm2,%xmm0,%xmm5 ;\



/*******************************************************************************************
* 1 line of N
*******************************************************************************************/

#define KERNEL8x1_1(xx) \
	prefetcht0	A_PR1(AO,%rax,8)	;\
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\

#define KERNEL8x1_2(xx) \
	prefetcht0	A_PR1+64(AO,%rax,8)	;\
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	 -6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	 -4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	 -2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\

#define KERNEL8x1_3(xx) \
	prefetcht0	A_PR1+128(AO,%rax,8)	;\
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	  0 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	  2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	  4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	  6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\

#define KERNEL8x1_4(xx) \
	prefetcht0	A_PR1+192(AO,%rax,8)	;\
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	  8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	 10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	 12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	 14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	addq	$4, BI				  ;\
	addq	$32, %rax 			  ;\

#define KERNEL8x1_SUB(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm13,%xmm1,%xmm0,%xmm13 ;\


/*******************************************************************************************/

#define KERNEL4x1_1(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\

#define KERNEL4x1_2(xx) \
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\

#define KERNEL4x1_3(xx) \
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	 -6 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\

#define KERNEL4x1_4(xx) \
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	 -4 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	 -2 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	addq	$4, BI				  ;\
	addq	$16, %rax 			  ;\

#define KERNEL4x1_SUB(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm7,%xmm1,%xmm0,%xmm7 ;\


/*******************************************************************************************/

#define KERNEL2x1_1(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL2x1_2(xx) \
	vmovddup	 -1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL2x1_3(xx) \
	vmovddup	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-12 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL2x1_4(xx) \
	vmovddup	  1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-10 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	addq	$4, BI				  ;\
	addq	$8, %rax 			  ;\

#define KERNEL2x1_SUB(xx) \
	vmovddup	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddpd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

/*******************************************************************************************/

#define KERNEL1x1_1(xx) \
	vmovsd	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL1x1_2(xx) \
	vmovsd	 -1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-15 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL1x1_3(xx) \
	vmovsd	  0 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-14 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL1x1_4(xx) \
	vmovsd	  1 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-13 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	addq	$4, BI				  ;\
	addq	$4, %rax 			  ;\

#define KERNEL1x1_SUB(xx) \
	vmovsd	 -2 * SIZE(BO, BI, 8), %xmm1 ;\
	vmovsd 	-16 * SIZE(AO, %rax, 8), %xmm0 ;\
	vfmaddsd  	%xmm4,%xmm1,%xmm0,%xmm4 ;\


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
        salq    $1,%rax                 // K * 2
        movq    B, BO1
        leaq    (B,%rax,8), BO2         // next offset to BO2
        leaq    BUFFER1, BO             // first buffer to BO
        movq    K, %rax
        sarq    $2, %rax                // K / 4
        jz      .L6_02a
        ALIGN_4

.L6_02:
	prefetcht0 B_PR1(BO1)
	prefetcht0 B_PR1(BO2)
	prefetchw  B_PR1(BO)
	vmovups	      (BO1), %xmm0
	vmovups	2*SIZE(BO1), %xmm2
	vmovups	4*SIZE(BO1), %xmm4
	vmovups	6*SIZE(BO1), %xmm6
	vmovsd        (BO2), %xmm1
	vmovsd  2*SIZE(BO2), %xmm3
	vmovsd  4*SIZE(BO2), %xmm5
	vmovsd  6*SIZE(BO2), %xmm7
	vmovups	%xmm0,       (BO)
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
	jnz	.L6_02

.L6_02a:

	movq	K, %rax
	andq	$3, %rax		// K % 4
	jz	.L6_02c
	ALIGN_4

.L6_02b:

	vmovups	(BO1), %xmm0
	vmovsd  (BO2), %xmm1
	vmovups	%xmm0,       (BO)
	vmovsd	%xmm1, 2*SIZE(BO)
	addq	$ 2*SIZE,BO1
	addq	$ 2*SIZE,BO2
	addq	$ 3*SIZE,BO
	decq	%rax
	jnz	.L6_02b

.L6_02c:

	movq	K, %rax
	salq	$1,%rax			// K * 2
	leaq	(B,%rax,8), BO1		// next offset to BO1
	leaq	(BO1,%rax,8), BO2	// next offset to BO1
	leaq    BUFFER2, BO		// second buffer to BO
	movq	K, %rax
	sarq	$2, %rax		// k / 4
	jz	.L6_03a
	ALIGN_4


.L6_03:

	prefetcht0 B_PR1(BO2)
	prefetchw  B_PR1(BO)
	vmovups	      (BO2), %xmm0
	vmovups	2*SIZE(BO2), %xmm2
	vmovups	4*SIZE(BO2), %xmm4
	vmovups	6*SIZE(BO2), %xmm6
	vmovsd  1*SIZE(BO1), %xmm1
	vmovsd  3*SIZE(BO1), %xmm3
	vmovsd  5*SIZE(BO1), %xmm5
	vmovsd  7*SIZE(BO1), %xmm7
	vmovsd	%xmm1, 0*SIZE(BO)
	vmovups	%xmm0, 1*SIZE(BO)
	vmovsd	%xmm3, 3*SIZE(BO)
	vmovups	%xmm2, 4*SIZE(BO)
	vmovsd	%xmm5, 6*SIZE(BO)
	vmovups	%xmm4, 7*SIZE(BO)
	vmovsd	%xmm7, 9*SIZE(BO)
	vmovups	%xmm6,10*SIZE(BO)
	addq	$ 8*SIZE,BO1
	addq	$ 8*SIZE,BO2
	addq	$ 12*SIZE,BO
	decq	%rax
	jnz	.L6_03

.L6_03a:

        movq    K, %rax
        andq    $3, %rax                // K % 4
        jz      .L6_03c
        ALIGN_4


.L6_03b:

	vmovsd	  1*SIZE(BO1), %xmm0
	vmovups  	(BO2), %xmm1
	vmovsd	%xmm0,       (BO)
	vmovups %xmm1, 1*SIZE(BO)
	addq	$ 2*SIZE,BO1
	addq	$ 2*SIZE,BO2
	addq	$ 3*SIZE,BO
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
	sarq	$3, I			// i = (m >> 3)
	je	.L6_20

	ALIGN_4

.L6_11:

        leaq    BUFFER1, BO             // first buffer to BO
	addq	$12 * SIZE, BO
        movq    K, %rax
	sarq	$3, %rax			//  K / 8
	cmpq	$3, %rax
	jl	.L6_13

	prefetcht0 B_PR1(BO)
	prefetcht0 B_PR1+64(BO)
	prefetcht0 B_PR1+128(BO)
	KERNEL8x3_INIT
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_M8

	subq	$2, %rax

	ALIGN_5

.L6_12:

	prefetcht0 B_PR1-24(BO)
	prefetcht0 B_PR1+40(BO)
	KERNEL8x3_M1
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	prefetcht0 B_PR1+104(BO)
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_M8

	dec	%rax
	jne	.L6_12

.L6_12_E:

	prefetcht0 B_PR1(BO)
	prefetcht0 B_PR1+64(BO)
	KERNEL8x3_M1
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_E

	jmp	.L6_16

.L6_13:

	test $2, %rax
	jz	.L6_14

	KERNEL8x3_INIT
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_M8

	KERNEL8x3_M1
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_E

	jmp	.L6_16


.L6_14:

	test $1, %rax
	jz	.L6_15
	
	KERNEL8x3_INIT
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_E


	jmp	.L6_16

.L6_15:

	INIT8x3

.L6_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_19

	ALIGN_4

.L6_17:

	KERNEL8x3_SUBN
	dec	%rax
	jne	.L6_17
	ALIGN_4


.L6_19:

	SAVE8x3

	decq	I			# i --
	jg	.L6_11

/**************************************************************************
* Rest of M 
***************************************************************************/
.L6_20:
	// Test rest of M

	testq	$7, M
	jz	.L7_10		// to next 3 lines of N

	testq	$4, M		
	jz	.L6_30

	ALIGN_4

.L6_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_22:

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L6_26

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L6_26

	jmp	.L6_22
	ALIGN_4

.L6_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_27:

	KERNEL4x3_SUB(xxx)
	addq	$3, BI
	addq	$4, %rax
	jl	.L6_27
	ALIGN_4


.L6_29:

	vmovddup	ALPHA, %xmm0


	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

	vfmaddpd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddpd 2 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 2 * SIZE(CO1, LDC, 2)


	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L6_30:
	testq	$2, M		
	jz	.L6_40

	ALIGN_4

.L6_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO


	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_32:

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L6_36

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L6_36

	jmp	.L6_32
	ALIGN_4

.L6_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_37:

	KERNEL2x3_SUB(xxx)
	addq	$3, BI
	addq	$2, %rax
	jl	.L6_37
	ALIGN_4


.L6_39:

	vmovddup	ALPHA, %xmm0


	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm6 ,  	(CO1, LDC, 2)


	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L6_40:
	testq	$1, M		
	jz	.L7_10		// to next 3 lines of N

	ALIGN_4

.L6_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_42:

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L6_46

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L6_46

	jmp	.L6_42
	ALIGN_4

.L6_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_47:

	KERNEL1x3_SUB(xxx)
	addq	$3, BI
	addq	$1, %rax
	jl	.L6_47
	ALIGN_4


.L6_49:

	vmovddup	ALPHA, %xmm0


	vfmaddsd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddsd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddsd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm6 ,  	(CO1, LDC, 2)


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
	sarq	$3, I			// i = (m >> 3)
	je	.L7_20
	ALIGN_4

.L7_11:

        leaq    BUFFER2, BO             // first buffer to BO
	addq	$12 * SIZE, BO
        movq    K, %rax
	sarq	$3, %rax			//  K / 8
	cmpq	$3, %rax
	jl	.L7_13

	prefetcht0 B_PR1(BO)
	prefetcht0 B_PR1+64(BO)
	prefetcht0 B_PR1+128(BO)
	KERNEL8x3_INIT
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_M8

	subq	$2, %rax

	ALIGN_5

.L7_12:

	prefetcht0 B_PR1-24(BO)
	prefetcht0 B_PR1+40(BO)
	KERNEL8x3_M1
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	prefetcht0 B_PR1+104(BO)
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_M8

	dec	%rax
	jne	.L7_12

.L7_12_E:

	prefetcht0 B_PR1(BO)
	prefetcht0 B_PR1+64(BO)
	KERNEL8x3_M1
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_E

	jmp	.L7_16



.L7_13:

	test $2, %rax
	jz	.L7_14

	KERNEL8x3_INIT
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_M8

	KERNEL8x3_M1
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_E

	jmp	.L7_16


.L7_14:

	test $1, %rax
	jz	.L7_15
	
	KERNEL8x3_INIT
	KERNEL8x3_M2
	KERNEL8x3_M3
	KERNEL8x3_M4
	KERNEL8x3_M5
	KERNEL8x3_M6
	KERNEL8x3_M7
	KERNEL8x3_E

	jmp	.L7_16



.L7_15:

	INIT8x3

.L7_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_19


	ALIGN_4

.L7_17:

	KERNEL8x3_SUBN
	dec	%rax
	jne	.L7_17
	ALIGN_4


.L7_19:

	SAVE8x3

	decq	I			# i --
	jg	.L7_11
	ALIGN_4	

.L7_20:
	// Test rest of M

	testq	$7, M
	jz	.L7_60		// to next 6 lines of N

	testq	$4, M		
	jz	.L7_30

	ALIGN_4

.L7_21:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO


	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L7_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_22:

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L7_26

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L7_26

	jmp	.L7_22
	ALIGN_4

.L7_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_27:

	KERNEL4x3_SUB(xxx)
	addq	$3, BI
	addq	$4, %rax
	jl	.L7_27
	ALIGN_4


.L7_29:

	vmovddup	ALPHA, %xmm0


	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

	vfmaddpd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddpd 2 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 2 * SIZE(CO1, LDC, 2)


	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L7_30:
	testq	$2, M		
	jz	.L7_40

	ALIGN_4

.L7_31:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L7_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_32:

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L7_36

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L7_36

	jmp	.L7_32
	ALIGN_4

.L7_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_37:

	KERNEL2x3_SUB(xxx)
	addq	$3, BI
	addq	$2, %rax
	jl	.L7_37
	ALIGN_4


.L7_39:

	vmovddup	ALPHA, %xmm0


	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm6 ,  	(CO1, LDC, 2)


	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4
	




.L7_40:
	testq	$1, M		
	jz	.L7_60		// to next 6 lines of N

	ALIGN_4

.L7_41:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax


	andq	$-8, %rax
	je	.L7_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_42:

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L7_46

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L7_46

	jmp	.L7_42
	ALIGN_4

.L7_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_47:

	KERNEL1x3_SUB(xxx)
	addq	$3, BI
	addq	$1, %rax
	jl	.L7_47
	ALIGN_4


.L7_49:

	vmovddup	ALPHA, %xmm0


	vfmaddsd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddsd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddsd 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6


	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)
	vmovsd	%xmm6 ,  	(CO1, LDC, 2)


	addq	$1 * SIZE, CO1		# coffset += 1

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
	ALIGN_4

.L2_02b:

	vmovups	(BO1), %xmm0
	vmovups	%xmm0,       (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
	decq	%rax
	jnz	.L2_02b

.L2_02c:

	movq	BO1, B			// next offset of B

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$3, I			// i = (m >> 3)
	je	.L2_20

	ALIGN_4

.L2_11:

	leaq	BUFFER1, BO		// first buffer to BO
	addq	$4 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_12:

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	je	.L2_16

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	je	.L2_16

	jmp	.L2_12
	ALIGN_4

.L2_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_19

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_17:

	KERNEL8x2_SUB(xxx)
	addq	$2, BI
	addq	$8, %rax
	jl	.L2_17
	ALIGN_4


.L2_19:

	vmovddup	ALPHA, %xmm0

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddpd 4 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddpd 6 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddpd 4 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddpd 6 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)
	vmovups	%xmm10, 4 * SIZE(CO1)
	vmovups	%xmm13, 6 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)
	vmovups	%xmm11, 4 * SIZE(CO1, LDC)
	vmovups	%xmm14, 6 * SIZE(CO1, LDC)


	addq	$8 * SIZE, CO1		# coffset += 8
	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$7, M
	jz	.L2_60		// to next 2 lines of N

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
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_22:

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	je	.L2_26

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_27:

	KERNEL4x2_SUB(xxx)
	addq	$2, BI
	addq	$4, %rax
	jl	.L2_27
	ALIGN_4


.L2_29:

	vmovddup	ALPHA, %xmm0

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_32:

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	je	.L2_36

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_37:

	KERNEL2x2_SUB(xxx)
	addq	$2, BI
	addq	$2, %rax
	jl	.L2_37
	ALIGN_4


.L2_39:

	vmovddup	ALPHA, %xmm0

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)

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

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_42:

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	jmp	.L2_42
	ALIGN_4

.L2_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L2_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_47:

	KERNEL1x2_SUB(xxx)
	addq	$2, BI
	addq	$1, %rax
	jl	.L2_47
	ALIGN_4


.L2_49:

	vmovddup	ALPHA, %xmm0

	vfmaddsd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddsd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)


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
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$3, I			// i = (m >> 3)
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

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_12:

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	je	.L1_16

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	je	.L1_16

	jmp	.L1_12
	ALIGN_4

.L1_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_19

	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_17:

	KERNEL8x1_SUB(xxx)
	addq	$1, BI
	addq	$8, %rax
	jl	.L1_17
	ALIGN_4


.L1_19:

	vmovddup	ALPHA, %xmm0

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddpd 4 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddpd 6 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)
	vmovups	%xmm10, 4 * SIZE(CO1)
	vmovups	%xmm13, 6 * SIZE(CO1)

	addq	$8 * SIZE, CO1		# coffset += 8
	decq	I			# i --
	jg	.L1_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$7, M
	jz	.L999

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_22:

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	je	.L1_26

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	je	.L1_26

	jmp	.L1_22
	ALIGN_4

.L1_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_29

	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_27:

	KERNEL4x1_SUB(xxx)
	addq	$1, BI
	addq	$4, %rax
	jl	.L1_27
	ALIGN_4


.L1_29:

	vmovddup	ALPHA, %xmm0

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_32:

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_36

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_36

	jmp	.L1_32
	ALIGN_4

.L1_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_39

	movq    %rax, BI                        //  Index for BO
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_37:

	KERNEL2x1_SUB(xxx)
	addq	$1, BI
	addq	$2, %rax
	jl	.L1_37
	ALIGN_4


.L1_39:

	vmovddup	ALPHA, %xmm0

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4

	vmovups	%xmm4 ,  	(CO1)

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

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	jmp	.L1_42
	ALIGN_4

.L1_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB(xxx)
	addq	$1, BI
	addq	$1, %rax
	jl	.L1_47
	ALIGN_4


.L1_49:

	vmovddup	ALPHA, %xmm0

	vfmaddsd 	 (CO1),%xmm0, %xmm4,%xmm4

	vmovsd	%xmm4 ,  	(CO1)

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
        divq    %rdi                    //    N / 2
        movq    %rax, Ndiv6             //    N / 2
        movq    %rdx, Nmod6             //    N % 2

	

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

.L2_0:

.L2_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L2_02b:

	vmovups	(BO1), %xmm0
	vmovups	%xmm0,       (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
	decq	%rax
	jnz	.L2_02b

.L2_02c:

	movq	BO1, B			// next offset of B

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
	sarq	$3, I			// i = (m >> 3)
	je	.L2_20

	ALIGN_4

.L2_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, 8), BO
	salq	$3, %rax			// rax = rax * 8 ; number of values
        leaq    (AO, %rax, 8), AO
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
        addq    $8, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_12:

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	je	.L2_16

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

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

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_17:

	KERNEL8x2_SUB(xxx)
	addq	$2, BI
	addq	$8, %rax
	jl	.L2_17
	ALIGN_4


.L2_19:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddpd 4 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddpd 6 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddpd 4 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddpd 6 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14


#else
	vmulpd	%xmm0, %xmm4,%xmm4
	vmulpd	%xmm0, %xmm7,%xmm7
	vmulpd	%xmm0, %xmm10,%xmm10
	vmulpd	%xmm0, %xmm13,%xmm13

	vmulpd	%xmm0, %xmm5,%xmm5
	vmulpd	%xmm0, %xmm8,%xmm8
	vmulpd	%xmm0, %xmm11,%xmm11
	vmulpd	%xmm0, %xmm14,%xmm14

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)
	vmovups	%xmm10, 4 * SIZE(CO1)
	vmovups	%xmm13, 6 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)
	vmovups	%xmm11, 4 * SIZE(CO1, LDC)
	vmovups	%xmm14, 6 * SIZE(CO1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, 8), BO
	salq	$3, %rax			// rax = rax * 8 ; number of values
        leaq    (AO, %rax, 8), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 8
	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$7, M
	jz	.L2_60		// to next 2 lines of N

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
        leaq    (BO, BI, 8), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, 8), AO
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
        addq    $4, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_22:

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	je	.L2_26

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_27:

	KERNEL4x2_SUB(xxx)
	addq	$2, BI
	addq	$4, %rax
	jl	.L2_27
	ALIGN_4


.L2_29:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddpd 2 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8


#else
	vmulpd	%xmm0, %xmm4,%xmm4
	vmulpd	%xmm0, %xmm7,%xmm7

	vmulpd	%xmm0, %xmm5,%xmm5
	vmulpd	%xmm0, %xmm8,%xmm8

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 2 * SIZE(CO1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, 8), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, 8), AO
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
        leaq    (BO, BI, 8), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, 8), AO
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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_32:

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	je	.L2_36

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_37:

	KERNEL2x2_SUB(xxx)
	addq	$2, BI
	addq	$2, %rax
	jl	.L2_37
	ALIGN_4


.L2_39:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

#else
	vmulpd	%xmm0, %xmm4,%xmm4
	vmulpd	%xmm0, %xmm5,%xmm5

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, 8), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, 8), AO
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
        leaq    (BO, BI, 8), BO
        leaq    (AO, %rax, 8), AO
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

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_42:

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

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

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_47:

	KERNEL1x2_SUB(xxx)
	addq	$2, BI
	addq	$1, %rax
	jl	.L2_47
	ALIGN_4


.L2_49:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddsd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddsd 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

#else
	vmulsd	%xmm0, %xmm4,%xmm4
	vmulsd	%xmm0, %xmm5,%xmm5

#endif

	vmovsd	%xmm4 ,  	(CO1)
	vmovsd	%xmm5 ,  	(CO1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, 8), BO
        leaq    (AO, %rax, 8), AO
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
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$3, I			// i = (m >> 3)
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
        leaq    (BO, BI, 8), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, 8), AO
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
        addq    $8, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L1_16
	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_12:

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	je	.L1_16

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

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

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_17:

	KERNEL8x1_SUB(xxx)
	addq	$1, BI
	addq	$8, %rax
	jl	.L1_17
	ALIGN_4


.L1_19:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddpd 4 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddpd 6 * SIZE(CO1),%xmm0, %xmm13,%xmm13

#else
	vmulpd	%xmm0, %xmm4,%xmm4
	vmulpd	%xmm0, %xmm7,%xmm7
	vmulpd	%xmm0, %xmm10,%xmm10
	vmulpd	%xmm0, %xmm13,%xmm13

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)
	vmovups	%xmm10, 4 * SIZE(CO1)
	vmovups	%xmm13, 6 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, 8), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, 8), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif
	addq	$8 * SIZE, CO1		# coffset += 8
	decq	I			# i --
	jg	.L1_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$7, M
	jz	.L999

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
        leaq    (BO, BI, 8), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, 8), AO
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
        addq    $4, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_26
	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_22:

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	je	.L1_26

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_27:

	KERNEL4x1_SUB(xxx)
	addq	$1, BI
	addq	$4, %rax
	jl	.L1_27
	ALIGN_4


.L1_29:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddpd 2 * SIZE(CO1),%xmm0, %xmm7,%xmm7

#else
	vmulpd	%xmm0, %xmm4,%xmm4
	vmulpd	%xmm0, %xmm7,%xmm7

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, 8), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, 8), AO
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
        leaq    (BO, BI, 8), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, 8), AO
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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_32:

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_36

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

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
	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_37:

	KERNEL2x1_SUB(xxx)
	addq	$1, BI
	addq	$2, %rax
	jl	.L1_37
	ALIGN_4


.L1_39:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddpd 	 (CO1),%xmm0, %xmm4,%xmm4

#else
	vmulpd	%xmm0, %xmm4,%xmm4

#endif

	vmovups	%xmm4 ,  	(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, 8), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, 8), AO
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
        leaq    (BO, BI, 8), BO
        leaq    (AO, %rax, 8), AO
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

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

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

	leaq	(AO, %rax, 8), AO
	leaq	(BO, BI, 8), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB(xxx)
	addq	$1, BI
	addq	$1, %rax
	jl	.L1_47
	ALIGN_4


.L1_49:

	vmovddup	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddsd 	 (CO1),%xmm0, %xmm4,%xmm4

#else
	vmulsd	%xmm0, %xmm4,%xmm4

#endif

	vmovsd	%xmm4 ,  	(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, 8), BO
        leaq    (AO, %rax, 8), AO
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
