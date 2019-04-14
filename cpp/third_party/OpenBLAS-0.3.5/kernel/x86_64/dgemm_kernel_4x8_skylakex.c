/*********************************************************************************
Copyright (c) 2015, The OpenBLAS Project
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

/*
 * This file is based on dgemm_kernel_4x8_haswell.s (original copyright above).
 * The content got translated from ASM to C+intrinsics, significantly simplified,
 * and AVX512 support added by Arjan van de Ven <arjan@linux.intel.com>
 */


#include "common.h"
#include <immintrin.h>


/*******************************************************************************************
* Macro definitions
*******************************************************************************************/


/******************************************************************************************/


#define INIT4x8()				\
	ymm4 = _mm256_setzero_pd();		\
	ymm5 = _mm256_setzero_pd();		\
	ymm6 = _mm256_setzero_pd();		\
	ymm7 = _mm256_setzero_pd();		\
	ymm8 = _mm256_setzero_pd();		\
	ymm9 = _mm256_setzero_pd();		\
	ymm10 = _mm256_setzero_pd();		\
	ymm11 = _mm256_setzero_pd();		\


#define KERNEL4x8_SUB()				\
	ymm0  = _mm256_loadu_pd(AO - 16);	\
/*	ymm0 [ A B C D ] */			\
	ymm1  = _mm256_loadu_pd(BO - 12);	\
	ymm2  = _mm256_loadu_pd(BO - 8);	\
/* 	ymm1 [ 1 2 3 4 ] */			\
/* 	ymm2 [ 5 6 7 8 ] */			\
						\
	ymm4 += ymm0 * ymm1;			\
/*	ymm4 +=  [ A*1 | B*2 | C*3 | D*4 ] */	\
	ymm8 += ymm0 * ymm2;			\
/*	ymm8 +=  [ A*5 | B*6 | C*7 | D*8 ] */   \
						\
	ymm0  = _mm256_permute4x64_pd(ymm0, 0xb1);	\
/*	ymm0 [ B A D C ] */			\
	ymm5 += ymm0 * ymm1;			\
/*	ymm5 +=  [ B*1 | A*2 | D*3 | C*4 ] */	\
	ymm9 += ymm0 * ymm2;			\
/*	ymm9 +=  [ B*5 | A*6 | D*7 | C*8 ] */	\
						\
	ymm0  = _mm256_permute4x64_pd(ymm0, 0x1b);	\
/*	ymm0 [ C D A B ]] */ 			\
	ymm6 += ymm0 * ymm1;			\
/*	ymm6 +=  [ C*1 | D*2 | A*3 | B*4 ] */ 	\
	ymm10+= ymm0 * ymm2;			\
/*	ymm10 += [ C*5 | D*6 | A*7 | B*8 ] */ 	\
						\
	ymm0  = _mm256_permute4x64_pd(ymm0, 0xb1);	\
/*	ymm0 [ D C B A ] */			\
	ymm7 += ymm0 * ymm1;			\
/*	ymm7  += [ D*1 | C*2 | B*3 | A*4 ] */	\
	ymm11+= ymm0 * ymm2;			\
/*	ymm11 += [ D*5 | C*6 | B*7 | A*8 ] */	\
	AO += 4;				\
	BO += 8;


#define SAVE4x8(ALPHA)					\
	ymm0 = _mm256_set1_pd(ALPHA);			\
	ymm4 *= ymm0;					\
	ymm5 *= ymm0;					\
	ymm6 *= ymm0;					\
	ymm7 *= ymm0;					\
	ymm8 *= ymm0;					\
	ymm9 *= ymm0;					\
	ymm10 *= ymm0;					\
	ymm11 *= ymm0;					\
							\
/*	Entry values:  			    */		\
/*	ymm4  = a [ A*1 | B*2 | C*3 | D*4 ] */		\
/*	ymm5  = a [ B*1 | A*2 | D*3 | C*4 ] */		\
/*	ymm6  = a [ C*1 | D*2 | A*3 | B*4 ] */ 		\
/*	ymm7  = a [ D*1 | C*2 | B*3 | A*4 ] */		\
/*	ymm8  = a [ A*5 | B*6 | C*7 | D*8 ] */		\
/*	ymm9  = a [ B*5 | A*6 | D*7 | C*8 ] */		\
/*	ymm10 = a [ C*5 | D*6 | A*7 | B*8 ] */ 		\
/*	ymm11 = a [ D*5 | C*6 | B*7 | A*8 ] */		\
							\
	ymm5 = _mm256_permute4x64_pd(ymm5, 0xb1);	\
/*	ymm5 =  a [ A*2 | B*1 | C*4 | D*3 ] */		\
	ymm7 = _mm256_permute4x64_pd(ymm7, 0xb1);	\
/*	ymm7 =  a [ C*2 | D*1 | A*4 | B*3 ] */		\
							\
	ymm0 = _mm256_blend_pd(ymm4, ymm5, 0x0a);	\
	ymm1 = _mm256_blend_pd(ymm4, ymm5, 0x05);	\
/*	ymm0 =  a [ A*1 | B*1 | C*3 | D*3 ] */		\
/*	ymm1 =  a [ A*2 | B*2 | C*4 | D*4 ] */		\
	ymm2 = _mm256_blend_pd(ymm6, ymm7, 0x0a);	\
	ymm3 = _mm256_blend_pd(ymm6, ymm7, 0x05);	\
/*	ymm2 =  a [ C*1 | D*1 | A*3 | B*3 ] */		\
/*	ymm3 =  a [ C*2 | D*2 | A*4 | B*4 ] */		\
							\
	ymm2 = _mm256_permute4x64_pd(ymm2, 0x1b);	\
	ymm3 = _mm256_permute4x64_pd(ymm3, 0x1b);	\
/*	ymm2 =  a [ B*3 | A*3 | D*1 | C*1 ] */		\
/*	ymm3 =  a [ B*4 | A*4 | D*2 | C*2 ] */		\
	ymm2 = _mm256_permute4x64_pd(ymm2, 0xb1);	\
	ymm3 = _mm256_permute4x64_pd(ymm3, 0xb1);	\
/*	ymm2 =  a [ A*3 | B*3 | C*1 | D*1 ] */		\
/*	ymm3 =  a [ A*4 | B*4 | C*2 | D*2 ] */		\
							\
	ymm4 = _mm256_blend_pd(ymm2, ymm0, 0x03);	\
	ymm5 = _mm256_blend_pd(ymm3, ymm1, 0x03);	\
/*	ymm4 =  a [ A*1 | B*1 | C*1 | D*1 ] */		\
/*	ymm5 =  a [ A*2 | B*2 | C*2 | D*2 ] */		\
	ymm6 = _mm256_blend_pd(ymm0, ymm2, 0x03);	\
	ymm7 = _mm256_blend_pd(ymm1, ymm3, 0x03);	\
/*	ymm5 =  a [ A*3 | B*3 | C*3 | D*3 ] */		\
/*	ymm7 =  a [ A*4 | B*4 | C*4 | D*4 ] */		\
							\
	ymm4 += _mm256_loadu_pd(CO1 + (0 * ldc));	\
	ymm5 += _mm256_loadu_pd(CO1 + (1 * ldc));	\
	ymm6 += _mm256_loadu_pd(CO1 + (2 * ldc));	\
	ymm7 += _mm256_loadu_pd(CO1 + (3 * ldc));	\
	_mm256_storeu_pd(CO1 + (0 * ldc), ymm4);	\
	_mm256_storeu_pd(CO1 + (1 * ldc), ymm5);	\
	_mm256_storeu_pd(CO1 + (2 * ldc), ymm6);	\
	_mm256_storeu_pd(CO1 + (3 * ldc), ymm7);	\
							\
	ymm9 = _mm256_permute4x64_pd(ymm9, 0xb1);	\
	ymm11 = _mm256_permute4x64_pd(ymm11, 0xb1);	\
							\
	ymm0 = _mm256_blend_pd(ymm8, ymm9, 0x0a);	\
	ymm1 = _mm256_blend_pd(ymm8, ymm9, 0x05);	\
	ymm2 = _mm256_blend_pd(ymm10, ymm11, 0x0a);	\
	ymm3 = _mm256_blend_pd(ymm10, ymm11, 0x05);	\
							\
	ymm2 = _mm256_permute4x64_pd(ymm2, 0x1b);	\
	ymm3 = _mm256_permute4x64_pd(ymm3, 0x1b);	\
	ymm2 = _mm256_permute4x64_pd(ymm2, 0xb1);	\
	ymm3 = _mm256_permute4x64_pd(ymm3, 0xb1);	\
							\
	ymm4 = _mm256_blend_pd(ymm2, ymm0, 0x03);	\
	ymm5 = _mm256_blend_pd(ymm3, ymm1, 0x03);	\
	ymm6 = _mm256_blend_pd(ymm0, ymm2, 0x03);	\
	ymm7 = _mm256_blend_pd(ymm1, ymm3, 0x03);	\
							\
	ymm4 += _mm256_loadu_pd(CO1 + (4 * ldc));	\
	ymm5 += _mm256_loadu_pd(CO1 + (5 * ldc));	\
	ymm6 += _mm256_loadu_pd(CO1 + (6 * ldc));	\
	ymm7 += _mm256_loadu_pd(CO1 + (7 * ldc));	\
	_mm256_storeu_pd(CO1 + (4 * ldc), ymm4);	\
	_mm256_storeu_pd(CO1 + (5 * ldc), ymm5);	\
	_mm256_storeu_pd(CO1 + (6 * ldc), ymm6);	\
	_mm256_storeu_pd(CO1 + (7 * ldc), ymm7);	\
							\
	CO1 += 4;

/******************************************************************************************/

#define INIT2x8()				\
	xmm4 = _mm_setzero_pd(); 		\
	xmm5 = _mm_setzero_pd(); 		\
	xmm6 = _mm_setzero_pd(); 		\
	xmm7 = _mm_setzero_pd(); 		\
	xmm8 = _mm_setzero_pd(); 		\
	xmm9 = _mm_setzero_pd(); 		\
	xmm10 = _mm_setzero_pd(); 		\
	xmm11 = _mm_setzero_pd(); 		\


#define KERNEL2x8_SUB()				\
	xmm0 = _mm_loadu_pd(AO - 16);		\
	xmm1 = _mm_set1_pd(*(BO - 12));		\
	xmm2 = _mm_set1_pd(*(BO - 11));		\
	xmm3 = _mm_set1_pd(*(BO - 10));		\
	xmm4 += xmm0 * xmm1;			\
	xmm1 = _mm_set1_pd(*(BO - 9));		\
	xmm5 += xmm0 * xmm2;			\
	xmm2 = _mm_set1_pd(*(BO - 8));		\
	xmm6 += xmm0 * xmm3;			\
	xmm3 = _mm_set1_pd(*(BO - 7));		\
	xmm7 += xmm0 * xmm1;			\
	xmm1 = _mm_set1_pd(*(BO - 6));		\
	xmm8 += xmm0 * xmm2;			\
	xmm2 = _mm_set1_pd(*(BO - 5));		\
	xmm9 += xmm0 * xmm3;			\
	xmm10 += xmm0 * xmm1;			\
	xmm11 += xmm0 * xmm2;			\
	BO += 8;				\
	AO += 2;

#define  SAVE2x8(ALPHA)					\
	xmm0 = _mm_set1_pd(ALPHA);			\
	xmm4 *= xmm0;					\
	xmm5 *= xmm0;					\
	xmm6 *= xmm0;					\
	xmm7 *= xmm0;					\
	xmm8 *= xmm0;					\
	xmm9 *= xmm0;					\
	xmm10 *= xmm0;					\
	xmm11 *= xmm0;					\
							\
	xmm4 += _mm_loadu_pd(CO1 + (0 * ldc));		\
	xmm5 += _mm_loadu_pd(CO1 + (1 * ldc));		\
	xmm6 += _mm_loadu_pd(CO1 + (2 * ldc));		\
	xmm7 += _mm_loadu_pd(CO1 + (3 * ldc));		\
							\
	_mm_storeu_pd(CO1 + (0 * ldc), xmm4);		\
	_mm_storeu_pd(CO1 + (1 * ldc), xmm5);		\
	_mm_storeu_pd(CO1 + (2 * ldc), xmm6);		\
	_mm_storeu_pd(CO1 + (3 * ldc), xmm7);		\
							\
	xmm8 += _mm_loadu_pd(CO1 + (4 * ldc));		\
	xmm9 += _mm_loadu_pd(CO1 + (5 * ldc));		\
	xmm10+= _mm_loadu_pd(CO1 + (6 * ldc));		\
	xmm11+= _mm_loadu_pd(CO1 + (7 * ldc));		\
	_mm_storeu_pd(CO1 + (4 * ldc), xmm8);		\
	_mm_storeu_pd(CO1 + (5 * ldc), xmm9);		\
	_mm_storeu_pd(CO1 + (6 * ldc), xmm10);		\
	_mm_storeu_pd(CO1 + (7 * ldc), xmm11);		\
	CO1 += 2;




/******************************************************************************************/

#define INIT1x8()				\
	dbl4 = 0;	\
	dbl5 = 0;	\
	dbl6 = 0;	\
	dbl7 = 0;	\
	dbl8 = 0;	\
	dbl9 = 0;	\
	dbl10 = 0;	\
	dbl11 = 0;	


#define KERNEL1x8_SUB()				\
	dbl0 = *(AO - 16);			\
	dbl1 = *(BO - 12);			\
	dbl2 = *(BO - 11);			\
	dbl3 = *(BO - 10);			\
	dbl4 += dbl0 * dbl1;			\
	dbl1 = *(BO - 9);			\
	dbl5 += dbl0 * dbl2;			\
	dbl2 = *(BO - 8);			\
	dbl6 += dbl0 * dbl3;			\
	dbl3 = *(BO - 7);			\
	dbl7 += dbl0 * dbl1;			\
	dbl1 = *(BO - 6);			\
	dbl8 += dbl0 * dbl2;			\
	dbl2 = *(BO - 5);			\
	dbl9  += dbl0 * dbl3;			\
	dbl10 += dbl0 * dbl1;			\
	dbl11 += dbl0 * dbl2;			\
	BO += 8;				\
	AO += 1;


#define SAVE1x8(ALPHA)				\
	dbl0 = ALPHA;				\
	dbl4 *= dbl0;				\
	dbl5 *= dbl0;				\
	dbl6 *= dbl0;				\
	dbl7 *= dbl0;				\
	dbl8 *= dbl0;				\
	dbl9 *= dbl0;				\
	dbl10 *= dbl0;				\
	dbl11 *= dbl0;				\
						\
	dbl4 += *(CO1 + (0 * ldc));		\
	dbl5 += *(CO1 + (1 * ldc));		\
	dbl6 += *(CO1 + (2 * ldc));		\
	dbl7 += *(CO1 + (3 * ldc));		\
	*(CO1 + (0 * ldc)) = dbl4;		\
	*(CO1 + (1 * ldc)) = dbl5;		\
	*(CO1 + (2 * ldc)) = dbl6;		\
	*(CO1 + (3 * ldc)) = dbl7;		\
						\
	dbl8  += *(CO1 + (4 * ldc));		\
	dbl9  += *(CO1 + (5 * ldc));		\
	dbl10 += *(CO1 + (6 * ldc));		\
	dbl11 += *(CO1 + (7 * ldc));		\
	*(CO1 + (4 * ldc)) = dbl8;		\
	*(CO1 + (5 * ldc)) = dbl9;		\
	*(CO1 + (6 * ldc)) = dbl10;		\
	*(CO1 + (7 * ldc)) = dbl11;		\
						\
	CO1 += 1;






/******************************************************************************************/

#define INIT4x4()				\
	ymm4 = _mm256_setzero_pd();		\
	ymm5 = _mm256_setzero_pd();		\
	ymm6 = _mm256_setzero_pd();		\
	ymm7 = _mm256_setzero_pd();		\


#define KERNEL4x4_SUB() 				\
	ymm0  = _mm256_loadu_pd(AO - 16);		\
	ymm1  = _mm256_broadcastsd_pd(_mm_load_sd(BO - 12));	\
							\
	ymm4 += ymm0 * ymm1;				\
							\
	ymm1  = _mm256_broadcastsd_pd(_mm_load_sd(BO - 11));	\
	ymm5 += ymm0 * ymm1;				\
							\
	ymm1  = _mm256_broadcastsd_pd(_mm_load_sd(BO - 10));	\
	ymm6 += ymm0 * ymm1;				\
							\
	ymm1  = _mm256_broadcastsd_pd(_mm_load_sd(BO - 9));	\
	ymm7 += ymm0 * ymm1;				\
	AO += 4;					\
	BO += 4;


#define SAVE4x4(ALPHA)					\
	ymm0 = _mm256_set1_pd(ALPHA);			\
	ymm4 *= ymm0;					\
	ymm5 *= ymm0;					\
	ymm6 *= ymm0;					\
	ymm7 *= ymm0;					\
							\
	ymm4 += _mm256_loadu_pd(CO1 + (0 * ldc));	\
	ymm5 += _mm256_loadu_pd(CO1 + (1 * ldc));	\
	ymm6 += _mm256_loadu_pd(CO1 + (2 * ldc));	\
	ymm7 += _mm256_loadu_pd(CO1 + (3 * ldc));	\
	_mm256_storeu_pd(CO1 + (0 * ldc), ymm4);	\
	_mm256_storeu_pd(CO1 + (1 * ldc), ymm5);	\
	_mm256_storeu_pd(CO1 + (2 * ldc), ymm6);	\
	_mm256_storeu_pd(CO1 + (3 * ldc), ymm7);	\
							\
	CO1 += 4;


/******************************************************************************************/
/******************************************************************************************/

#define  INIT2x4()				\
	xmm4 = _mm_setzero_pd(); 		\
	xmm5 = _mm_setzero_pd(); 		\
	xmm6 = _mm_setzero_pd(); 		\
	xmm7 = _mm_setzero_pd(); 		\



#define KERNEL2x4_SUB()				\
	xmm0 = _mm_loadu_pd(AO - 16);		\
	xmm1 = _mm_set1_pd(*(BO - 12));		\
	xmm2 = _mm_set1_pd(*(BO - 11));		\
	xmm3 = _mm_set1_pd(*(BO - 10));		\
	xmm4 += xmm0 * xmm1;			\
	xmm1 = _mm_set1_pd(*(BO - 9));		\
	xmm5 += xmm0 * xmm2;			\
	xmm6 += xmm0 * xmm3;			\
	xmm7 += xmm0 * xmm1;			\
	BO += 4;				\
	AO += 2;



#define  SAVE2x4(ALPHA)					\
	xmm0 = _mm_set1_pd(ALPHA);			\
	xmm4 *= xmm0;					\
	xmm5 *= xmm0;					\
	xmm6 *= xmm0;					\
	xmm7 *= xmm0;					\
							\
	xmm4 += _mm_loadu_pd(CO1 + (0 * ldc));	\
	xmm5 += _mm_loadu_pd(CO1 + (1 * ldc));	\
	xmm6 += _mm_loadu_pd(CO1 + (2 * ldc));	\
	xmm7 += _mm_loadu_pd(CO1 + (3 * ldc));	\
							\
	_mm_storeu_pd(CO1 + (0 * ldc), xmm4);		\
	_mm_storeu_pd(CO1 + (1 * ldc), xmm5);		\
	_mm_storeu_pd(CO1 + (2 * ldc), xmm6);		\
	_mm_storeu_pd(CO1 + (3 * ldc), xmm7);		\
							\
	CO1 += 2;

/******************************************************************************************/
/******************************************************************************************/

#define  INIT1x4()		\
	dbl4 = 0; 		\
	dbl5 = 0; 		\
	dbl6 = 0; 		\
	dbl7 = 0; 		\

#define KERNEL1x4_SUB()				\
	dbl0 = *(AO - 16);			\
	dbl1 = *(BO - 12);			\
	dbl2 = *(BO - 11);			\
	dbl3 = *(BO - 10);			\
	dbl8  = *(BO - 9);			\
						\
	dbl4 += dbl0 * dbl1;			\
	dbl5 += dbl0 * dbl2;			\
	dbl6 += dbl0 * dbl3;			\
	dbl7 += dbl0 * dbl8;			\
	BO += 4;				\
	AO += 1;


#define SAVE1x4(ALPHA)				\
	dbl0 = ALPHA;				\
	dbl4 *= dbl0;				\
	dbl5 *= dbl0;				\
	dbl6 *= dbl0;				\
	dbl7 *= dbl0;				\
						\
	dbl4 += *(CO1 + (0 * ldc));		\
	dbl5 += *(CO1 + (1 * ldc));		\
	dbl6 += *(CO1 + (2 * ldc));		\
	dbl7 += *(CO1 + (3 * ldc));		\
	*(CO1 + (0 * ldc)) = dbl4;		\
	*(CO1 + (1 * ldc)) = dbl5;		\
	*(CO1 + (2 * ldc)) = dbl6;		\
	*(CO1 + (3 * ldc)) = dbl7;		\
						\
						\
	CO1 += 1;


/******************************************************************************************/
/******************************************************************************************/

#define  INIT8x4()				\
	ymm10 = _mm256_setzero_pd(); 		\
	ymm11 = _mm256_setzero_pd(); 		\
	ymm12 = _mm256_setzero_pd(); 		\
	ymm13 = _mm256_setzero_pd(); 		\
	ymm14 = _mm256_setzero_pd(); 		\
	ymm15 = _mm256_setzero_pd(); 		\
	ymm16 = _mm256_setzero_pd(); 		\
	ymm17 = _mm256_setzero_pd(); 		\


#define KERNEL8x4_SUB()				\
	ymm0 = _mm256_loadu_pd(AO - 16);	\
	ymm1 = _mm256_loadu_pd(AO - 12);	\
	ymm2 = _mm256_set1_pd(*(BO - 12));	\
	ymm3 = _mm256_set1_pd(*(BO - 11));	\
	ymm4 = _mm256_set1_pd(*(BO - 10));	\
	ymm5 = _mm256_set1_pd(*(BO - 9));	\
	ymm10 += ymm0 * ymm2;			\
	ymm11 += ymm1 * ymm2;			\
	ymm12 += ymm0 * ymm3;			\
	ymm13 += ymm1 * ymm3;			\
	ymm14 += ymm0 * ymm4;			\
	ymm15 += ymm1 * ymm4;			\
	ymm16 += ymm0 * ymm5;			\
	ymm17 += ymm1 * ymm5;			\
	BO += 4;				\
	AO += 8;



#define SAVE8x4(ALPHA)					\
	ymm0 = _mm256_set1_pd(ALPHA);			\
	ymm10 *= ymm0;					\
	ymm11 *= ymm0;					\
	ymm12 *= ymm0;					\
	ymm13 *= ymm0;					\
	ymm14 *= ymm0;					\
	ymm15 *= ymm0;					\
	ymm16 *= ymm0;					\
	ymm17 *= ymm0;					\
							\
	ymm10 += _mm256_loadu_pd(CO1);			\
	ymm11 += _mm256_loadu_pd(CO1 + 4);		\
	ymm12 += _mm256_loadu_pd(CO1 + (ldc));		\
	ymm13 += _mm256_loadu_pd(CO1 + (ldc) + 4);	\
	ymm14 += _mm256_loadu_pd(CO1 + (ldc*2));	\
	ymm15 += _mm256_loadu_pd(CO1 + (ldc*2) + 4);	\
	ymm16 += _mm256_loadu_pd(CO1 + (ldc*3));	\
	ymm17 += _mm256_loadu_pd(CO1 + (ldc*3) + 4);	\
							\
	_mm256_storeu_pd(CO1, ymm10);			\
	_mm256_storeu_pd(CO1 + 4, ymm11);		\
	_mm256_storeu_pd(CO1 + ldc, ymm12);		\
	_mm256_storeu_pd(CO1 + ldc + 4, ymm13);		\
	_mm256_storeu_pd(CO1 + ldc*2, ymm14);		\
	_mm256_storeu_pd(CO1 + ldc*2 + 4, ymm15);	\
	_mm256_storeu_pd(CO1 + ldc*3, ymm16);		\
	_mm256_storeu_pd(CO1 + ldc*3 + 4, ymm17);	\
							\
	CO1 += 8;


/******************************************************************************************/
/******************************************************************************************/
#define  INIT8x2()				\
	ymm4 = _mm256_setzero_pd(); 		\
	ymm5 = _mm256_setzero_pd(); 		\
	ymm6 = _mm256_setzero_pd(); 		\
	ymm7 = _mm256_setzero_pd(); 		\


#define KERNEL8x2_SUB()				\
	ymm0 = _mm256_loadu_pd(AO - 16);	\
	ymm1 = _mm256_loadu_pd(AO - 12);	\
	ymm2 = _mm256_set1_pd(*(BO - 12));	\
	ymm3 = _mm256_set1_pd(*(BO - 11));	\
	ymm4 += ymm0 * ymm2;			\
	ymm5 += ymm1 * ymm2;			\
	ymm6 += ymm0 * ymm3;			\
	ymm7 += ymm1 * ymm3;			\
	BO += 2;				\
	AO += 8;



#define SAVE8x2(ALPHA)					\
	ymm0 = _mm256_set1_pd(ALPHA);			\
	ymm4 *= ymm0;					\
	ymm5 *= ymm0;					\
	ymm6 *= ymm0;					\
	ymm7 *= ymm0;					\
							\
	ymm4 += _mm256_loadu_pd(CO1);			\
	ymm5 += _mm256_loadu_pd(CO1 + 4);		\
	ymm6 += _mm256_loadu_pd(CO1 + (ldc));		\
	ymm7 += _mm256_loadu_pd(CO1 + (ldc) + 4);	\
							\
	_mm256_storeu_pd(CO1, ymm4);			\
	_mm256_storeu_pd(CO1 + 4, ymm5);		\
	_mm256_storeu_pd(CO1 + ldc, ymm6);		\
	_mm256_storeu_pd(CO1 + ldc + 4, ymm7);		\
							\
	CO1 += 8;


/******************************************************************************************/
/******************************************************************************************/
#define  INIT4x2()				\
	xmm4 = _mm_setzero_pd(); 		\
	xmm5 = _mm_setzero_pd(); 		\
	xmm6 = _mm_setzero_pd(); 		\
	xmm7 = _mm_setzero_pd(); 		\


#define KERNEL4x2_SUB()				\
	xmm0 = _mm_loadu_pd(AO - 16);		\
	xmm1 = _mm_loadu_pd(AO - 14);		\
	xmm2 = _mm_set1_pd(*(BO - 12));		\
	xmm3 = _mm_set1_pd(*(BO - 11));		\
	xmm4 += xmm0 * xmm2;			\
	xmm5 += xmm1 * xmm2;			\
	xmm6 += xmm0 * xmm3;			\
	xmm7 += xmm1 * xmm3;			\
	BO += 2;				\
	AO += 4;



#define SAVE4x2(ALPHA)					\
	xmm0 = _mm_set1_pd(ALPHA);			\
	xmm4 *= xmm0;					\
	xmm5 *= xmm0;					\
	xmm6 *= xmm0;					\
	xmm7 *= xmm0;					\
							\
	xmm4 += _mm_loadu_pd(CO1);			\
	xmm5 += _mm_loadu_pd(CO1 + 2);			\
	xmm6 += _mm_loadu_pd(CO1 + (ldc));		\
	xmm7 += _mm_loadu_pd(CO1 + (ldc) + 2);		\
							\
	_mm_storeu_pd(CO1, xmm4);			\
	_mm_storeu_pd(CO1 + 2, xmm5);			\
	_mm_storeu_pd(CO1 + ldc, xmm6);			\
	_mm_storeu_pd(CO1 + ldc + 2, xmm7);		\
							\
	CO1 += 4;


/******************************************************************************************/
/******************************************************************************************/

#define  INIT2x2()				\
	xmm4 = _mm_setzero_pd(); 		\
	xmm6 = _mm_setzero_pd(); 		\



#define KERNEL2x2_SUB()				\
	xmm2 = _mm_set1_pd(*(BO - 12));		\
	xmm0 = _mm_loadu_pd(AO - 16);		\
	xmm3 = _mm_set1_pd(*(BO - 11));		\
	xmm4 += xmm0 * xmm2;			\
	xmm6 += xmm0 * xmm3;			\
	BO += 2;				\
	AO += 2;


#define  SAVE2x2(ALPHA)					\
	xmm0 = _mm_set1_pd(ALPHA);			\
	xmm4 *= xmm0;					\
	xmm6 *= xmm0;					\
							\
	xmm4 += _mm_loadu_pd(CO1);			\
	xmm6 += _mm_loadu_pd(CO1 + ldc);		\
							\
	_mm_storeu_pd(CO1, xmm4);			\
	_mm_storeu_pd(CO1 + ldc, xmm6);			\
							\
	CO1 += 2;


/******************************************************************************************/
/******************************************************************************************/

#define INIT1x2()				\
	dbl4 = 0;				\
	dbl5 = 0;			


#define KERNEL1x2_SUB()				\
	dbl0 = *(AO - 16);			\
	dbl1 = *(BO - 12);			\
	dbl2 = *(BO - 11);			\
	dbl4 += dbl0 * dbl1;			\
	dbl5 += dbl0 * dbl2;			\
	BO += 2;				\
	AO += 1;


#define SAVE1x2(ALPHA)				\
	dbl0 = ALPHA;				\
	dbl4 *= dbl0;				\
	dbl5 *= dbl0;				\
						\
	dbl4 += *(CO1 + (0 * ldc));		\
	dbl5 += *(CO1 + (1 * ldc));		\
	*(CO1 + (0 * ldc)) = dbl4;		\
	*(CO1 + (1 * ldc)) = dbl5;		\
						\
						\
	CO1 += 1;



/******************************************************************************************/
/******************************************************************************************/

#define INIT4x1()				\
	ymm4 = _mm256_setzero_pd();		\
	ymm5 = _mm256_setzero_pd();		\
	ymm6 = _mm256_setzero_pd();		\
	ymm7 = _mm256_setzero_pd();		


#define KERNEL4x1()					\
	ymm0 =  _mm256_set1_pd(*(BO - 12));		\
	ymm1 =  _mm256_set1_pd(*(BO - 11));		\
	ymm2 =  _mm256_set1_pd(*(BO - 10));		\
	ymm3 =  _mm256_set1_pd(*(BO -  9));		\
							\
	ymm4 += _mm256_loadu_pd(AO - 16) * ymm0;	\
	ymm5 += _mm256_loadu_pd(AO - 12) * ymm1;	\
							\
	ymm0 =  _mm256_set1_pd(*(BO - 8));		\
	ymm1 =  _mm256_set1_pd(*(BO - 7));		\
							\
	ymm6 += _mm256_loadu_pd(AO - 8) * ymm2;		\
	ymm7 += _mm256_loadu_pd(AO - 4) * ymm3;		\
							\
	ymm2 =  _mm256_set1_pd(*(BO - 6));		\
	ymm3 =  _mm256_set1_pd(*(BO - 5));		\
							\
	ymm4 += _mm256_loadu_pd(AO + 0) * ymm0;		\
	ymm5 += _mm256_loadu_pd(AO + 4) * ymm1;		\
	ymm6 += _mm256_loadu_pd(AO + 8) * ymm2;		\
	ymm7 += _mm256_loadu_pd(AO + 12) * ymm3;	\
							\
	BO += 8;					\
	AO += 32;


#define INIT8x1()				\
	zmm4 = _mm512_setzero_pd();		\


#define KERNEL8x1_SUB() 					\
	zmm2 = _mm512_set1_pd(*(BO - 12));			\
	zmm0 = _mm512_loadu_pd(AO - 16);			\
	zmm4 += zmm0 * zmm2;					\
	BO += 1;						\
	AO += 8;


#define SAVE8x1(ALPHA)						\
	zmm0 = _mm512_set1_pd(ALPHA);				\
	zmm4 *= zmm0;						\
								\
	zmm4 += _mm512_loadu_pd(CO1);				\
	_mm512_storeu_pd(CO1, zmm4);				\
	CO1 += 8;

#define KERNEL4x1_SUB() 					\
	ymm2 = _mm256_set1_pd(*(BO - 12));			\
	ymm0 = _mm256_loadu_pd(AO - 16);			\
	ymm4 += ymm0 * ymm2;					\
	BO += 1;						\
	AO += 4;


#define SAVE4x1(ALPHA)						\
	ymm0 = _mm256_set1_pd(ALPHA);				\
	ymm4 += ymm5;						\
	ymm6 += ymm7;						\
	ymm4 += ymm6;						\
	ymm4 *= ymm0;						\
								\
	ymm4 += _mm256_loadu_pd(CO1);				\
	_mm256_storeu_pd(CO1, ymm4);				\
	CO1 += 4;


/******************************************************************************************/
/******************************************************************************************/

#define INIT2x1()					\
	xmm4 = _mm_setzero_pd(); 		


#define KERNEL2x1_SUB()				\
	xmm2 = _mm_set1_pd(*(BO - 12));		\
	xmm0 = _mm_loadu_pd(AO - 16);		\
	xmm4 += xmm0 * xmm2;			\
	BO += 1;				\
	AO += 2;


#define  SAVE2x1(ALPHA)					\
	xmm0 = _mm_set1_pd(ALPHA);			\
	xmm4 *= xmm0;					\
							\
	xmm4 += _mm_loadu_pd(CO1);			\
							\
	_mm_storeu_pd(CO1, xmm4);			\
							\
	CO1 += 2;


/******************************************************************************************/
/******************************************************************************************/

#define INIT1x1()	\
	dbl4 = 0;

#define KERNEL1x1_SUB() \
	dbl1 = *(BO - 12);	\
	dbl0 = *(AO - 16);	\
	dbl4 += dbl0 * dbl1;	\
	BO += 1;		\
	AO += 1;

#define SAVE1x1(ALPHA)	\
	dbl0 = ALPHA;	\
	dbl4 *= dbl0; 	\
	dbl4 += *CO1;	\
	*CO1 = dbl4;	\
	CO1 += 1;


/*******************************************************************************************/

/* START */


int __attribute__ ((noinline))
CNAME(BLASLONG m, BLASLONG n, BLASLONG k, double alpha, double * __restrict__ A, double * __restrict__ B, double * __restrict__ C, BLASLONG ldc)
{
	unsigned long M=m, N=n, K=k;

	
	if (M == 0)
		return 0;
	if (N == 0)
		return 0;
	if (K == 0)
		return 0;

	while (N >= 8) {
		double *CO1;
		double *AO;
		int i;
	
		CO1 = C;
		C += 8 * ldc;

		AO = A + 16;

		i = m;

		while (i >= 24) {
			double *BO;
			double *A1, *A2;
			int kloop = K;

			BO = B + 12;
			A1 = AO + 8 * K;
			A2 = AO + 16 * K;
			/*
			 *  This is the inner loop for the hot hot path
			 *  Written in inline asm because compilers like GCC 8 and earlier
			 *  struggle with register allocation and are not good at using
			 *  the AVX512 built in broadcast ability (1to8)
			 */
			asm(
			"vxorpd  %%zmm1, %%zmm1, %%zmm1\n"
			"vmovapd %%zmm1, %%zmm2\n"
			"vmovapd %%zmm1, %%zmm3\n"
			"vmovapd %%zmm1, %%zmm4\n"
			"vmovapd %%zmm1, %%zmm5\n"
			"vmovapd %%zmm1, %%zmm6\n"
			"vmovapd %%zmm1, %%zmm7\n"
			"vmovapd %%zmm1, %%zmm8\n"
			"vmovapd %%zmm1, %%zmm11\n"
			"vmovapd %%zmm1, %%zmm12\n"
			"vmovapd %%zmm1, %%zmm13\n"
			"vmovapd %%zmm1, %%zmm14\n"
			"vmovapd %%zmm1, %%zmm15\n"
			"vmovapd %%zmm1, %%zmm16\n"
			"vmovapd %%zmm1, %%zmm17\n"
			"vmovapd %%zmm1, %%zmm18\n"
			"vmovapd %%zmm1, %%zmm21\n"
			"vmovapd %%zmm1, %%zmm22\n"
			"vmovapd %%zmm1, %%zmm23\n"
			"vmovapd %%zmm1, %%zmm24\n"
			"vmovapd %%zmm1, %%zmm25\n"
			"vmovapd %%zmm1, %%zmm26\n"
			"vmovapd %%zmm1, %%zmm27\n"
			"vmovapd %%zmm1, %%zmm28\n"
			"jmp .label24\n"
			".p2align 5\n"
			/* Inner math loop */
			".label24:\n"
			"vmovupd     -128(%[AO]),%%zmm0\n"
			"vmovupd     -128(%[A1]),%%zmm10\n"
			"vmovupd     -128(%[A2]),%%zmm20\n"

			"vbroadcastsd       -96(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm1\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm11\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm21\n"

			"vbroadcastsd       -88(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm2\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm12\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm22\n"

			"vbroadcastsd       -80(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm3\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm13\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm23\n"

			"vbroadcastsd       -72(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm4\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm14\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm24\n"

			"vbroadcastsd       -64(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm5\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm15\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm25\n"

			"vbroadcastsd       -56(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm6\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm16\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm26\n"

			"vbroadcastsd       -48(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm7\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm17\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm27\n"

			"vbroadcastsd       -40(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm8\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm18\n"
			"vfmadd231pd    %%zmm9, %%zmm20, %%zmm28\n"
			"add $64, %[AO]\n"
			"add $64, %[A1]\n"
			"add $64, %[A2]\n"
			"add $64, %[BO]\n"
			"prefetch 512(%[AO])\n"
			"prefetch 512(%[A1])\n"
			"prefetch 512(%[A2])\n"
			"prefetch 512(%[BO])\n"
			"subl $1, %[kloop]\n"
			"jg .label24\n"
			/* multiply the result by alpha */
			"vbroadcastsd (%[alpha]), %%zmm9\n"
			/* And store additively in C */
			"vfmadd213pd (%[C0]), %%zmm9, %%zmm1\n"
			"vfmadd213pd (%[C1]), %%zmm9, %%zmm2\n"
			"vfmadd213pd (%[C2]), %%zmm9, %%zmm3\n"
			"vfmadd213pd (%[C3]), %%zmm9, %%zmm4\n"
			"vfmadd213pd (%[C4]), %%zmm9, %%zmm5\n"
			"vfmadd213pd (%[C5]), %%zmm9, %%zmm6\n"
			"vfmadd213pd (%[C6]), %%zmm9, %%zmm7\n"
			"vfmadd213pd (%[C7]), %%zmm9, %%zmm8\n"
			"vmovupd %%zmm1, (%[C0])\n"
			"vmovupd %%zmm2, (%[C1])\n"
			"vmovupd %%zmm3, (%[C2])\n"
			"vmovupd %%zmm4, (%[C3])\n"
			"vmovupd %%zmm5, (%[C4])\n"
			"vmovupd %%zmm6, (%[C5])\n"
			"vmovupd %%zmm7, (%[C6])\n"
			"vmovupd %%zmm8, (%[C7])\n"

			"vfmadd213pd 64(%[C0]), %%zmm9, %%zmm11\n"
			"vfmadd213pd 64(%[C1]), %%zmm9, %%zmm12\n"
			"vfmadd213pd 64(%[C2]), %%zmm9, %%zmm13\n"
			"vfmadd213pd 64(%[C3]), %%zmm9, %%zmm14\n"
			"vfmadd213pd 64(%[C4]), %%zmm9, %%zmm15\n"
			"vfmadd213pd 64(%[C5]), %%zmm9, %%zmm16\n"
			"vfmadd213pd 64(%[C6]), %%zmm9, %%zmm17\n"
			"vfmadd213pd 64(%[C7]), %%zmm9, %%zmm18\n"
			"vmovupd %%zmm11, 64(%[C0])\n"
			"vmovupd %%zmm12, 64(%[C1])\n"
			"vmovupd %%zmm13, 64(%[C2])\n"
			"vmovupd %%zmm14, 64(%[C3])\n"
			"vmovupd %%zmm15, 64(%[C4])\n"
			"vmovupd %%zmm16, 64(%[C5])\n"
			"vmovupd %%zmm17, 64(%[C6])\n"
			"vmovupd %%zmm18, 64(%[C7])\n"

			"vfmadd213pd 128(%[C0]), %%zmm9, %%zmm21\n"
			"vfmadd213pd 128(%[C1]), %%zmm9, %%zmm22\n"
			"vfmadd213pd 128(%[C2]), %%zmm9, %%zmm23\n"
			"vfmadd213pd 128(%[C3]), %%zmm9, %%zmm24\n"
			"vfmadd213pd 128(%[C4]), %%zmm9, %%zmm25\n"
			"vfmadd213pd 128(%[C5]), %%zmm9, %%zmm26\n"
			"vfmadd213pd 128(%[C6]), %%zmm9, %%zmm27\n"
			"vfmadd213pd 128(%[C7]), %%zmm9, %%zmm28\n"
			"vmovupd %%zmm21, 128(%[C0])\n"
			"vmovupd %%zmm22, 128(%[C1])\n"
			"vmovupd %%zmm23, 128(%[C2])\n"
			"vmovupd %%zmm24, 128(%[C3])\n"
			"vmovupd %%zmm25, 128(%[C4])\n"
			"vmovupd %%zmm26, 128(%[C5])\n"
			"vmovupd %%zmm27, 128(%[C6])\n"
			"vmovupd %%zmm28, 128(%[C7])\n"

			   :
				[AO]	"+r" (AO),
				[A1]	"+r" (A1),
				[A2]	"+r" (A2),
				[BO]	"+r" (BO),
				[C0]	"+r" (CO1),
				[kloop]	"+r" (kloop)
			   :
				[alpha] 	"r" (&alpha),
				[C1] 	"r" (CO1 + 1 * ldc),
				[C2] 	"r" (CO1 + 2 * ldc),
				[C3] 	"r" (CO1 + 3 * ldc),
				[C4] 	"r" (CO1 + 4 * ldc),
				[C5] 	"r" (CO1 + 5 * ldc),
				[C6] 	"r" (CO1 + 6 * ldc),
				[C7] 	"r" (CO1 + 7 * ldc)

			     :  "memory", "zmm0",  "zmm1",  "zmm2",  "zmm3",  "zmm4",  "zmm5",  "zmm6",  "zmm7",  "zmm8", "zmm9",
					  "zmm10", "zmm11", "zmm12", "zmm13", "zmm14", "zmm15", "zmm16", "zmm17", "zmm18",
					  "zmm20", "zmm21", "zmm22", "zmm23", "zmm24", "zmm25", "zmm26", "zmm27", "zmm28"
			);
			CO1 += 24;
			AO += 16 * K;
			i-= 24;
		}


		while (i >= 16) {
			double *BO;
			double *A1;
			int kloop = K;

			BO = B + 12;
			A1 = AO + 8 * K;
			/*
			 *  This is the inner loop for the hot hot path 
			 *  Written in inline asm because compilers like GCC 8 and earlier
			 *  struggle with register allocation and are not good at using
		 	 *  the AVX512 built in broadcast ability (1to8)
			 */
			asm(
			"vxorpd  %%zmm1, %%zmm1, %%zmm1\n"
			"vmovapd %%zmm1, %%zmm2\n"
			"vmovapd %%zmm1, %%zmm3\n"
			"vmovapd %%zmm1, %%zmm4\n"
			"vmovapd %%zmm1, %%zmm5\n"
			"vmovapd %%zmm1, %%zmm6\n"
			"vmovapd %%zmm1, %%zmm7\n"
			"vmovapd %%zmm1, %%zmm8\n"
			"vmovapd %%zmm1, %%zmm11\n"
			"vmovapd %%zmm1, %%zmm12\n"
			"vmovapd %%zmm1, %%zmm13\n"
			"vmovapd %%zmm1, %%zmm14\n"
			"vmovapd %%zmm1, %%zmm15\n"
			"vmovapd %%zmm1, %%zmm16\n"
			"vmovapd %%zmm1, %%zmm17\n"
			"vmovapd %%zmm1, %%zmm18\n"
			"jmp .label16\n"
			".p2align 5\n"
			/* Inner math loop */
			".label16:\n"
			"vmovupd     -128(%[AO]),%%zmm0\n"
			"vmovupd     -128(%[A1]),%%zmm10\n"

			"vbroadcastsd       -96(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm1\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm11\n"

			"vbroadcastsd       -88(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm2\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm12\n"

			"vbroadcastsd       -80(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm3\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm13\n"

			"vbroadcastsd       -72(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm4\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm14\n"

			"vbroadcastsd       -64(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm5\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm15\n"

			"vbroadcastsd       -56(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm6\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm16\n"

			"vbroadcastsd       -48(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm7\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm17\n"

			"vbroadcastsd       -40(%[BO]),  %%zmm9\n"
			"vfmadd231pd    %%zmm9, %%zmm0,  %%zmm8\n"
			"vfmadd231pd    %%zmm9, %%zmm10, %%zmm18\n"
			"add $64, %[AO]\n"
			"add $64, %[A1]\n"
			"add $64, %[BO]\n"
			"prefetch 512(%[AO])\n"
			"prefetch 512(%[A1])\n"
			"prefetch 512(%[BO])\n"
			"subl $1, %[kloop]\n"
			"jg .label16\n"
			/* multiply the result by alpha */
			"vbroadcastsd (%[alpha]), %%zmm9\n"
			/* And store additively in C */
			"vfmadd213pd (%[C0]), %%zmm9, %%zmm1\n"
			"vfmadd213pd (%[C1]), %%zmm9, %%zmm2\n"
			"vfmadd213pd (%[C2]), %%zmm9, %%zmm3\n"
			"vfmadd213pd (%[C3]), %%zmm9, %%zmm4\n"
			"vfmadd213pd (%[C4]), %%zmm9, %%zmm5\n"
			"vfmadd213pd (%[C5]), %%zmm9, %%zmm6\n"
			"vfmadd213pd (%[C6]), %%zmm9, %%zmm7\n"
			"vfmadd213pd (%[C7]), %%zmm9, %%zmm8\n"
			"vmovupd %%zmm1, (%[C0])\n"
			"vmovupd %%zmm2, (%[C1])\n"
			"vmovupd %%zmm3, (%[C2])\n"
			"vmovupd %%zmm4, (%[C3])\n"
			"vmovupd %%zmm5, (%[C4])\n"
			"vmovupd %%zmm6, (%[C5])\n"
			"vmovupd %%zmm7, (%[C6])\n"
			"vmovupd %%zmm8, (%[C7])\n"

			"vfmadd213pd 64(%[C0]), %%zmm9, %%zmm11\n"
			"vfmadd213pd 64(%[C1]), %%zmm9, %%zmm12\n"
			"vfmadd213pd 64(%[C2]), %%zmm9, %%zmm13\n"
			"vfmadd213pd 64(%[C3]), %%zmm9, %%zmm14\n"
			"vfmadd213pd 64(%[C4]), %%zmm9, %%zmm15\n"
			"vfmadd213pd 64(%[C5]), %%zmm9, %%zmm16\n"
			"vfmadd213pd 64(%[C6]), %%zmm9, %%zmm17\n"
			"vfmadd213pd 64(%[C7]), %%zmm9, %%zmm18\n"
			"vmovupd %%zmm11, 64(%[C0])\n"
			"vmovupd %%zmm12, 64(%[C1])\n"
			"vmovupd %%zmm13, 64(%[C2])\n"
			"vmovupd %%zmm14, 64(%[C3])\n"
			"vmovupd %%zmm15, 64(%[C4])\n"
			"vmovupd %%zmm16, 64(%[C5])\n"
			"vmovupd %%zmm17, 64(%[C6])\n"
			"vmovupd %%zmm18, 64(%[C7])\n"

			   :
				[AO]	"+r" (AO),
				[A1]	"+r" (A1),
				[BO]	"+r" (BO),
				[C0]	"+r" (CO1),
				[kloop]	"+r" (kloop)
			   :
				[alpha] 	"r" (&alpha),
				[C1] 	"r" (CO1 + 1 * ldc),
				[C2] 	"r" (CO1 + 2 * ldc),
				[C3] 	"r" (CO1 + 3 * ldc),
				[C4] 	"r" (CO1 + 4 * ldc),
				[C5] 	"r" (CO1 + 5 * ldc),
				[C6] 	"r" (CO1 + 6 * ldc),
				[C7] 	"r" (CO1 + 7 * ldc)

			     :  "memory", "zmm0",  "zmm1",  "zmm2",  "zmm3",  "zmm4",  "zmm5",  "zmm6",  "zmm7",  "zmm8", "zmm9",
					  "zmm10", "zmm11", "zmm12", "zmm13", "zmm14", "zmm15", "zmm16", "zmm17", "zmm18"
			);
			CO1 += 16;
			AO += 8 * K;
			i-= 16;
		}

		while (i >= 8) {
			double *BO;
			int kloop = K;

			BO = B + 12;
			/*
			 *  This is the inner loop for the hot hot path
			 *  Written in inline asm because compilers like GCC 8 and earlier
			 *  struggle with register allocation and are not good at using
			 *  the AVX512 built in broadcast ability (1to8)
			 */
			asm(
			"vxorpd  %%zmm1, %%zmm1, %%zmm1\n" 
			"vmovapd %%zmm1, %%zmm2\n"
			"vmovapd %%zmm1, %%zmm3\n"
			"vmovapd %%zmm1, %%zmm4\n"
			"vmovapd %%zmm1, %%zmm5\n"
			"vmovapd %%zmm1, %%zmm6\n"
			"vmovapd %%zmm1, %%zmm7\n"
			"vmovapd %%zmm1, %%zmm8\n"
			"vbroadcastsd (%[alpha]), %%zmm9\n"
			"jmp .label1\n"
			".p2align 5\n"
			/* Inner math loop */
			".label1:\n"
			"vmovupd     -128(%[AO]),%%zmm0\n"
			"vfmadd231pd  -96(%[BO])%{1to8%}, %%zmm0, %%zmm1\n"
			"vfmadd231pd  -88(%[BO])%{1to8%}, %%zmm0, %%zmm2\n"
			"vfmadd231pd  -80(%[BO])%{1to8%}, %%zmm0, %%zmm3\n"
			"vfmadd231pd  -72(%[BO])%{1to8%}, %%zmm0, %%zmm4\n"
			"vfmadd231pd  -64(%[BO])%{1to8%}, %%zmm0, %%zmm5\n"
			"vfmadd231pd  -56(%[BO])%{1to8%}, %%zmm0, %%zmm6\n"
			"vfmadd231pd  -48(%[BO])%{1to8%}, %%zmm0, %%zmm7\n"
			"vfmadd231pd  -40(%[BO])%{1to8%}, %%zmm0, %%zmm8\n"
			"add $64, %[AO]\n"
			"add $64, %[BO]\n"
			"subl $1, %[kloop]\n"
			"jg .label1\n"
			/* multiply the result by alpha and add to the memory */
			"vfmadd213pd (%[C0]), %%zmm9, %%zmm1\n"
			"vfmadd213pd (%[C1]), %%zmm9, %%zmm2\n"
			"vfmadd213pd (%[C2]), %%zmm9, %%zmm3\n"
			"vfmadd213pd (%[C3]), %%zmm9, %%zmm4\n"
			"vfmadd213pd (%[C4]), %%zmm9, %%zmm5\n"
			"vfmadd213pd (%[C5]), %%zmm9, %%zmm6\n"
			"vfmadd213pd (%[C6]), %%zmm9, %%zmm7\n"
			"vfmadd213pd (%[C7]), %%zmm9, %%zmm8\n"
			"vmovupd %%zmm1, (%[C0])\n"
			"vmovupd %%zmm2, (%[C1])\n"
			"vmovupd %%zmm3, (%[C2])\n"
			"vmovupd %%zmm4, (%[C3])\n"
			"vmovupd %%zmm5, (%[C4])\n"
			"vmovupd %%zmm6, (%[C5])\n"
			"vmovupd %%zmm7, (%[C6])\n"
			"vmovupd %%zmm8, (%[C7])\n"
			   : 
  			     [AO]	"+r" (AO),
			     [BO]	"+r" (BO),
			     [C0]	"+r" (CO1),
		             [kloop]	"+r" (kloop)
			   :
			     [alpha] 	"r" (&alpha),
			     [C1] 	"r" (CO1 + 1 * ldc),
			     [C2] 	"r" (CO1 + 2 * ldc),
			     [C3] 	"r" (CO1 + 3 * ldc),
			     [C4] 	"r" (CO1 + 4 * ldc),
			     [C5] 	"r" (CO1 + 5 * ldc),
			     [C6] 	"r" (CO1 + 6 * ldc),
			     [C7] 	"r" (CO1 + 7 * ldc)

			     :  "memory", "zmm0", "zmm1", "zmm2", "zmm3", "zmm4", "zmm5", "zmm6", "zmm7", "zmm8", "zmm9"
			);
			CO1 += 8;
			i-= 8;
		}



		while (i >= 4) {
			double *BO;
			__m256d ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6, ymm7, ymm8, ymm9, ymm10, ymm11;
			int kloop = K;

			BO = B + 12;
			INIT4x8()

			while (kloop > 0) {
				KERNEL4x8_SUB()
				kloop--;
			}				
			SAVE4x8(alpha)
			i-= 4;
		}


		while (i >= 2) {
			double *BO;
			__m128d xmm0, xmm1, xmm2, xmm3, xmm4, xmm5, xmm6, xmm7, xmm8, xmm9, xmm10, xmm11;
			int kloop = K;

			BO = B + 12;
			INIT2x8()
				
			while (kloop > 0) {
				KERNEL2x8_SUB()
				kloop--;
			}
			SAVE2x8(alpha)
			i -= 2;
		}

		while (i >= 1) {
			double *BO;
			double dbl0, dbl1, dbl2, dbl3, dbl4, dbl5, dbl6, dbl7, dbl8, dbl9, dbl10, dbl11;
			int kloop = K;

			BO = B + 12;
			INIT1x8()
										
			while (kloop > 0) {
				KERNEL1x8_SUB()
				kloop--;
			}
			SAVE1x8(alpha)
			i -= 1;
		}
		B += K * 8;
		N -= 8;
	}

	if (N == 0)
		return 0;	
	


	// L8_0
	while (N >= 4) {
		double *CO1;
		double *AO;
		int i;
		// L8_10
		CO1 = C;
		C += 4 * ldc;

		AO = A + 16;

		i = m;
		while (i >= 8) {
			double *BO;
			// L8_11
			__m256d ymm0, ymm1, ymm2, ymm3, ymm4, ymm5,  ymm10, ymm11,ymm12,ymm13,ymm14,ymm15,ymm16,ymm17;
			BO = B + 12;
			int kloop = K;
	
			INIT8x4()

			while (kloop > 0) {
				// L12_17
				KERNEL8x4_SUB()
				kloop--;
			}
			// L8_19
			SAVE8x4(alpha)
	
			i -= 8;
		}
		while (i >= 4) {
			// L8_11
			double *BO;
			__m256d ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6, ymm7;
			BO = B + 12;
			int kloop = K;

			INIT4x4()
			// L8_16
			while (kloop > 0) {
				// L12_17
				KERNEL4x4_SUB()
				kloop--;
			}
			// L8_19
			SAVE4x4(alpha)

			i -= 4;
		}

/**************************************************************************
* Rest of M 
***************************************************************************/

		while (i >= 2) {
			double *BO;
			__m128d xmm0, xmm1, xmm2, xmm3, xmm4, xmm5, xmm6, xmm7;
			BO = B;
			BO += 12;

			INIT2x4()
			int kloop = K;
			
			while (kloop > 0) {
				KERNEL2x4_SUB()
				kloop--;
			}
			SAVE2x4(alpha)
			i -= 2;
		}
			// L13_40
		while (i >= 1) {
			double *BO;
			double dbl0, dbl1, dbl2, dbl3, dbl4, dbl5, dbl6, dbl7, dbl8;
			int kloop = K;
			BO = B + 12;
			INIT1x4()
				
			while (kloop > 0) {
				KERNEL1x4_SUB()
				kloop--;
			}
			SAVE1x4(alpha)
			i -= 1;
		}
			
		B += K * 4;
		N -= 4;
	}

/**************************************************************************************************/

		// L8_0
	while (N >= 2) {
		double *CO1;
		double *AO;
		int i;
		// L8_10
		CO1 = C;
		C += 2 * ldc;

		AO = A + 16;

		i = m;
		while (i >= 8) {
			double *BO;
			__m256d ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6, ymm7;
			// L8_11
			BO = B + 12;
			int kloop = K;

			INIT8x2()

			// L8_16
			while (kloop > 0) {
				// L12_17
				KERNEL8x2_SUB()
				kloop--;
			}
			// L8_19
			SAVE8x2(alpha)

			i-=8;
		}

		while (i >= 4) {
			double *BO;
			__m128d xmm0, xmm1, xmm2, xmm3, xmm4, xmm5, xmm6, xmm7;
			// L8_11
			BO = B + 12;
			int kloop = K;
	
			INIT4x2()

			// L8_16
			while (kloop > 0) {
				// L12_17
				KERNEL4x2_SUB()
				kloop--;
			}
			// L8_19
			SAVE4x2(alpha)
	
			i-=4;
		}

/**************************************************************************
* Rest of M 
***************************************************************************/

		while (i >= 2) {
			double *BO;
			__m128d xmm0, xmm2, xmm3, xmm4, xmm6;
			int kloop = K;
			BO = B + 12;

			INIT2x2()
				
			while (kloop > 0) {
				KERNEL2x2_SUB()
				kloop--;
			}
			SAVE2x2(alpha)
			i -= 2;
		}
			// L13_40
		while (i >= 1) {
			double *BO;
			double dbl0, dbl1, dbl2, dbl4, dbl5;
			int kloop = K;
			BO = B + 12;

			INIT1x2()
					
			while (kloop > 0) {
				KERNEL1x2_SUB()
				kloop--;
			}
			SAVE1x2(alpha)
			i -= 1;
		}
			
		B += K * 2;
		N -= 2;
	}

		// L8_0
	while (N >= 1) {
		// L8_10
		double *CO1;
		double *AO;
		int i;

		CO1 = C;
		C += ldc;

		AO = A + 16;

		i = m;
		while (i >= 8) {
			double *BO;
			__m512d zmm0, zmm2, zmm4;
			// L8_11
			BO = B + 12;
			int kloop = K;

			INIT8x1()
			// L8_16
			while (kloop > 0) {
				// L12_17
				KERNEL8x1_SUB()
				kloop--;
			}
			// L8_19
			SAVE8x1(alpha)

			i-= 8;
		}
		while (i >= 4) {
			double *BO;
			__m256d ymm0, ymm2, ymm4, ymm5, ymm6, ymm7;
			// L8_11
			BO = B + 12;
			int kloop = K;

			INIT4x1()
			// L8_16
			while (kloop > 0) {
				// L12_17
				KERNEL4x1_SUB()
				kloop--;
			}
			// L8_19
			SAVE4x1(alpha)

			i-= 4;
		}

/**************************************************************************
* Rest of M 
***************************************************************************/

		while (i >= 2) {
			double *BO;
			__m128d xmm0, xmm2, xmm4;
			int kloop = K;
			BO = B;
			BO += 12;

			INIT2x1()
				
			while (kloop > 0) {
				KERNEL2x1_SUB()
				kloop--;
			}
			SAVE2x1(alpha)
			i -= 2;
		}
				// L13_40
		while (i >= 1) {
			double *BO;
			double dbl0, dbl1, dbl4;
			int kloop = K;

			BO = B;
			BO += 12;
			INIT1x1()
				

			while (kloop > 0) {
				KERNEL1x1_SUB()
				kloop--;
			}
			SAVE1x1(alpha)
			i -= 1;
		}
			
		B += K * 1;
		N -= 1;
	}


	return 0;
}
