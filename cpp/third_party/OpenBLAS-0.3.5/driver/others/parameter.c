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

#include <stdio.h>
#include <string.h>
#include "common.h"

extern int openblas_block_factor();
int get_L2_size(void);

#define DEFAULT_GEMM_P 128
#define DEFAULT_GEMM_Q 128
#define DEFAULT_GEMM_R 128
#define DEFAULT_GEMM_OFFSET_A 0
#define DEFAULT_GEMM_OFFSET_B 0

/* Global Parameter */
#if GEMM_OFFSET_A == gemm_offset_a
BLASLONG gemm_offset_a = DEFAULT_GEMM_OFFSET_A;
#else
BLASLONG gemm_offset_a = GEMM_OFFSET_A;
#endif

#if GEMM_OFFSET_B == gemm_offset_b
BLASLONG gemm_offset_b = DEFAULT_GEMM_OFFSET_B;
#else
BLASLONG gemm_offset_b = GEMM_OFFSET_B;
#endif

#if SGEMM_P == sgemm_p
BLASLONG sgemm_p = DEFAULT_GEMM_P;
#else
BLASLONG sgemm_p = SGEMM_P;
#endif
#if DGEMM_P == dgemm_p
BLASLONG dgemm_p = DEFAULT_GEMM_P;
#else
BLASLONG dgemm_p = DGEMM_P;
#endif
#if CGEMM_P == cgemm_p
BLASLONG cgemm_p = DEFAULT_GEMM_P;
#else
BLASLONG cgemm_p = CGEMM_P;
#endif
#if ZGEMM_P == zgemm_p
BLASLONG zgemm_p = DEFAULT_GEMM_P;
#else
BLASLONG zgemm_p = ZGEMM_P;
#endif

#if SGEMM_Q == sgemm_q
BLASLONG sgemm_q = DEFAULT_GEMM_Q;
#else
BLASLONG sgemm_q = SGEMM_Q;
#endif
#if DGEMM_Q == dgemm_q
BLASLONG dgemm_q = DEFAULT_GEMM_Q;
#else
BLASLONG dgemm_q = DGEMM_Q;
#endif
#if CGEMM_Q == cgemm_q
BLASLONG cgemm_q = DEFAULT_GEMM_Q;
#else
BLASLONG cgemm_q = CGEMM_Q;
#endif
#if ZGEMM_Q == zgemm_q
BLASLONG zgemm_q = DEFAULT_GEMM_Q;
#else
BLASLONG zgemm_q = ZGEMM_Q;
#endif

#if SGEMM_R == sgemm_r
BLASLONG sgemm_r = DEFAULT_GEMM_R;
#else
BLASLONG sgemm_r = SGEMM_R;
#endif
#if DGEMM_R == dgemm_r
BLASLONG dgemm_r = DEFAULT_GEMM_R;
#else
BLASLONG dgemm_r = DGEMM_R;
#endif
#if CGEMM_R == cgemm_r
BLASLONG cgemm_r = DEFAULT_GEMM_R;
#else
BLASLONG cgemm_r = CGEMM_R;
#endif
#if ZGEMM_R == zgemm_r
BLASLONG zgemm_r = DEFAULT_GEMM_R;
#else
BLASLONG zgemm_r = ZGEMM_R;
#endif

#if defined(EXPRECISION) || defined(QUAD_PRECISION)
#if QGEMM_P == qgemm_p
BLASLONG qgemm_p = DEFAULT_GEMM_P;
#else
BLASLONG qgemm_p = QGEMM_P;
#endif
#if XGEMM_P == xgemm_p
BLASLONG xgemm_p = DEFAULT_GEMM_P;
#else
BLASLONG xgemm_p = XGEMM_P;
#endif
#if QGEMM_Q == qgemm_q
BLASLONG qgemm_q = DEFAULT_GEMM_Q;
#else
BLASLONG qgemm_q = QGEMM_Q;
#endif
#if XGEMM_Q == xgemm_q
BLASLONG xgemm_q = DEFAULT_GEMM_Q;
#else
BLASLONG xgemm_q = XGEMM_Q;
#endif
#if QGEMM_R == qgemm_r
BLASLONG qgemm_r = DEFAULT_GEMM_R;
#else
BLASLONG qgemm_r = QGEMM_R;
#endif
#if XGEMM_R == xgemm_r
BLASLONG xgemm_r = DEFAULT_GEMM_R;
#else
BLASLONG xgemm_r = XGEMM_R;
#endif
#endif

#if defined(ARCH_X86) || defined(ARCH_X86_64)

int get_L2_size(void){

  int eax, ebx, ecx, edx;

#if defined(ATHLON) || defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BULLDOZER) || \
    defined(CORE_PRESCOTT) || defined(CORE_CORE2) || defined(PENRYN) || defined(DUNNINGTON) || \
    defined(CORE_NEHALEM) || defined(CORE_SANDYBRIDGE) || defined(ATOM) || defined(GENERIC) || \
    defined(PILEDRIVER) || defined(HASWELL) || defined(STEAMROLLER) || defined(EXCAVATOR) || defined(ZEN) || defined(SKYLAKEX)

  cpuid(0x80000006, &eax, &ebx, &ecx, &edx);

  return BITMASK(ecx, 16, 0xffff);

#else

  int info[15];
  int i;

  cpuid(2, &eax, &ebx, &ecx, &edx);

  info[ 0] = BITMASK(eax,  8, 0xff);
  info[ 1] = BITMASK(eax, 16, 0xff);
  info[ 2] = BITMASK(eax, 24, 0xff);

  info[ 3] = BITMASK(ebx,  0, 0xff);
  info[ 4] = BITMASK(ebx,  8, 0xff);
  info[ 5] = BITMASK(ebx, 16, 0xff);
  info[ 6] = BITMASK(ebx, 24, 0xff);

  info[ 7] = BITMASK(ecx,  0, 0xff);
  info[ 8] = BITMASK(ecx,  8, 0xff);
  info[ 9] = BITMASK(ecx, 16, 0xff);
  info[10] = BITMASK(ecx, 24, 0xff);

  info[11] = BITMASK(edx,  0, 0xff);
  info[12] = BITMASK(edx,  8, 0xff);
  info[13] = BITMASK(edx, 16, 0xff);
  info[14] = BITMASK(edx, 24, 0xff);

  for (i = 0; i < 15; i++){

    switch (info[i]){
      case 0x3b :
      case 0x41 :
      case 0x79 :
	return  128;
	break;

      case 0x3c :
      case 0x42 :
      case 0x7a :
      case 0x7e :
      case 0x82 :
	return  256;
	break;

      case 0x43 :
      case 0x7b :
      case 0x7f :
      case 0x83 :
      case 0x86 :
	return  512;
	break;

      case 0x44 :
      case 0x78 :
      case 0x7c :
      case 0x84 :
      case 0x87 :
	return 1024;
	break;

      case 0x45 :
      case 0x7d :
      case 0x85 :
	return 2048;

      case 0x49 :
	return 4096;
	break;
    }
  }

  /* Never reached */
  return 0;
#endif
}

void blas_set_parameter(void){

  int factor;
#if defined(BULLDOZER) || defined(PILEDRIVER) || defined(SANDYBRIDGE) || defined(NEHALEM) || defined(HASWELL) || defined(STEAMROLLER) || defined(EXCAVATOR) || defined(ZEN) || defined(SKYLAKEX)
  int size = 16;
#else
  int size = get_L2_size();
#endif

#if defined(CORE_KATMAI)  || defined(CORE_COPPERMINE) || defined(CORE_BANIAS)
  size >>= 7;

#if defined(CORE_BANIAS) && (HAVE_HIT > 1)
  sgemm_p =  64 / HAVE_HIT * size;
  dgemm_p =  32 / HAVE_HIT * size;
  cgemm_p =  32 / HAVE_HIT * size;
  zgemm_p =  16 / HAVE_HIT * size;
#ifdef EXPRECISION
  qgemm_p =  16 / HAVE_HIT * size;
  xgemm_p =   8 / HAVE_HIT * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =   8 / HAVE_HIT * size;
  xgemm_p =   4 / HAVE_HIT * size;
#endif
#else
  sgemm_p =  64 * size;
  dgemm_p =  32 * size;
  cgemm_p =  32 * size;
  zgemm_p =  16 * size;
#ifdef EXPRECISION
  qgemm_p =  16 * size;
  xgemm_p =   8 * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =   8 * size;
  xgemm_p =   4 * size;
#endif
#endif
#endif

#if defined(CORE_NORTHWOOD)
  size >>= 7;

#ifdef ALLOC_HUGETLB
  sgemm_p = 128 * size;
  dgemm_p =  64 * size;
  cgemm_p =  64 * size;
  zgemm_p =  32 * size;
#ifdef EXPRECISION
  qgemm_p =  32 * size;
  xgemm_p =  16 * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  16 * size;
  xgemm_p =   8 * size;
#endif
#else
  sgemm_p =  96 * size;
  dgemm_p =  48 * size;
  cgemm_p =  48 * size;
  zgemm_p =  24 * size;
#ifdef EXPRECISION
  qgemm_p =  24 * size;
  xgemm_p =  12 * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  12 * size;
  xgemm_p =   6 * size;
#endif
#endif
#endif

#if defined(CORE_CORE2)

  size >>= 9;

  sgemm_p =  92 * size;
  dgemm_p =  46 * size;
  cgemm_p =  46 * size;
  zgemm_p =  23 * size;

#ifdef EXPRECISION
  qgemm_p =  23 * size;
  xgemm_p =  11 * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  11 * size;
  xgemm_p =   5 * size;
#endif
#endif

#if defined(PENRYN)

  size >>= 9;

  sgemm_p = 1024;
  dgemm_p =  512;
  cgemm_p =  512;
  zgemm_p =  256;

#ifdef EXPRECISION
  qgemm_p =  256;
  xgemm_p =  128;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  21 * size + 4;
  xgemm_p =  10 * size + 2;
#endif
#endif

#if defined(DUNNINGTON)

  size >>= 9;

  sgemm_p = 384;
  dgemm_p = 384;
  cgemm_p = 384;
  zgemm_p = 384;

#ifdef EXPRECISION
  qgemm_p = 384;
  xgemm_p = 384;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  21 * size + 4;
  xgemm_p =  10 * size + 2;
#endif
#endif

#if defined(NEHALEM)
  sgemm_p = 1024;
  dgemm_p =  512;
  cgemm_p =  512;
  zgemm_p =  256;
#ifdef EXPRECISION
  qgemm_p =  256;
  xgemm_p =  128;
#endif
#endif

#if defined(SANDYBRIDGE)
  sgemm_p = 1024;
  dgemm_p =  512;
  cgemm_p =  512;
  zgemm_p =  256;
#ifdef EXPRECISION
  qgemm_p =  256;
  xgemm_p =  128;
#endif
#endif

#if defined(CORE_PRESCOTT)  || defined(GENERIC)
  size >>= 6;

  if (size > 16) size = 16;

  sgemm_p =  56 * size;
  dgemm_p =  28 * size;
  cgemm_p =  28 * size;
  zgemm_p =  14 * size;
#ifdef EXPRECISION
  qgemm_p =  14 * size;
  xgemm_p =   7 * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =   7 * size;
  xgemm_p =   3 * size;
#endif
#endif

#if defined(CORE_OPTERON)
  sgemm_p =  224 + 14 * (size >> 5);
  dgemm_p =  112 + 14 * (size >> 6);
  cgemm_p =  116 + 14 * (size >> 6);
  zgemm_p =   58 + 14 * (size >> 7);
#ifdef EXPRECISION
  qgemm_p =   58 + 14 * (size >> 7);
  xgemm_p =   29 + 14 * (size >> 8);
#endif
#ifdef QUAD_PRECISION
  qgemm_p =   29 + 14 * (size >> 8);
  xgemm_p =   15 + 14 * (size >> 9);
#endif
#endif

#if defined(ATOM)
  size >>= 8;

  sgemm_p =  256;
  dgemm_p =  128;
  cgemm_p =  128;
  zgemm_p =   64;
#ifdef EXPRECISION
  qgemm_p =   64;
  xgemm_p =   32;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =   32;
  xgemm_p =   16;
#endif
#endif

#if defined(CORE_BARCELONA) || defined(CORE_BOBCAT)
  size >>= 8;

  sgemm_p = 232 * size;
  dgemm_p = 116 * size;
  cgemm_p = 116 * size;
  zgemm_p =  58 * size;
#ifdef EXPRECISION
  qgemm_p =  58 * size;
  xgemm_p =  26 * size;
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  26 * size;
  xgemm_p =  13 * size;
#endif
#endif

  factor=openblas_block_factor();
  if (factor>0) {
    if (factor <  10) factor =  10;
    if (factor > 200) factor = 200;

    sgemm_p = ((long)((double)sgemm_p * (double)factor * 1.e-2)) & ~7L;
    dgemm_p = ((long)((double)dgemm_p * (double)factor * 1.e-2)) & ~7L;
    cgemm_p = ((long)((double)cgemm_p * (double)factor * 1.e-2)) & ~7L;
    zgemm_p = ((long)((double)zgemm_p * (double)factor * 1.e-2)) & ~7L;
#ifdef EXPRECISION
    qgemm_p = ((long)((double)qgemm_p * (double)factor * 1.e-2)) & ~7L;
    xgemm_p = ((long)((double)xgemm_p * (double)factor * 1.e-2)) & ~7L;
#endif
  }

  if (sgemm_p == 0) sgemm_p = 64;
  if (dgemm_p == 0) dgemm_p = 64;
  if (cgemm_p == 0) cgemm_p = 64;
  if (zgemm_p == 0) zgemm_p = 64;
#ifdef EXPRECISION
  if (qgemm_p == 0) qgemm_p = 64;
  if (xgemm_p == 0) xgemm_p = 64;
#endif

#ifdef QUAD_PRECISION
  if (qgemm_p == 0) qgemm_p = 64;
  if (xgemm_p == 0) xgemm_p = 64;
#endif

  sgemm_p = ((sgemm_p + SGEMM_UNROLL_M - 1)/SGEMM_UNROLL_M) * SGEMM_UNROLL_M;
  dgemm_p = ((dgemm_p + DGEMM_UNROLL_M - 1)/DGEMM_UNROLL_M) * DGEMM_UNROLL_M;
  cgemm_p = ((cgemm_p + CGEMM_UNROLL_M - 1)/CGEMM_UNROLL_M) * CGEMM_UNROLL_M;
  zgemm_p = ((zgemm_p + ZGEMM_UNROLL_M - 1)/ZGEMM_UNROLL_M) * ZGEMM_UNROLL_M;
#ifdef QUAD_PRECISION
  qgemm_p = ((qgemm_p + QGEMM_UNROLL_M - 1)/QGEMM_UNROLL_M) * QGEMM_UNROLL_M;
  xgemm_p = ((xgemm_p + XGEMM_UNROLL_M - 1)/XGEMM_UNROLL_M) * XGEMM_UNROLL_M;
#endif

  sgemm_r = (((BUFFER_SIZE - ((SGEMM_P * SGEMM_Q *  4 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (SGEMM_Q *  4)) - 15) & ~15;
  dgemm_r = (((BUFFER_SIZE - ((DGEMM_P * DGEMM_Q *  8 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (DGEMM_Q *  8)) - 15) & ~15;
  cgemm_r = (((BUFFER_SIZE - ((CGEMM_P * CGEMM_Q *  8 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (CGEMM_Q *  8)) - 15) & ~15;
  zgemm_r = (((BUFFER_SIZE - ((ZGEMM_P * ZGEMM_Q * 16 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (ZGEMM_Q * 16)) - 15) & ~15;
#if defined(EXPRECISION) || defined(QUAD_PRECISION)
  qgemm_r = (((BUFFER_SIZE - ((QGEMM_P * QGEMM_Q * 16 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (QGEMM_Q * 16)) - 15) & ~15;
  xgemm_r = (((BUFFER_SIZE - ((XGEMM_P * XGEMM_Q * 32 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (XGEMM_Q * 32)) - 15) & ~15;
#endif

#if 0
  fprintf(stderr, "SGEMM ... %3d, %3d, %3d\n", SGEMM_P, SGEMM_Q, SGEMM_R);
  fprintf(stderr, "DGEMM ... %3d, %3d, %3d\n", DGEMM_P, DGEMM_Q, DGEMM_R);
  fprintf(stderr, "CGEMM ... %3d, %3d, %3d\n", CGEMM_P, CGEMM_Q, CGEMM_R);
  fprintf(stderr, "ZGEMM ... %3d, %3d, %3d\n", ZGEMM_P, ZGEMM_Q, ZGEMM_R);
#endif

  return;
}

#if 0

int get_current_cpu_info(void){

  int nlprocs, ncores, cmplegacy;
  int htt     = 0;
  int apicid  = 0;

#if defined(CORE_PRESCOTT) || defined(CORE_OPTERON)
  int eax, ebx, ecx, edx;

  cpuid(1, &eax, &ebx, &ecx, &edx);
  nlprocs = BITMASK(ebx, 16, 0xff);
  apicid  = BITMASK(ebx, 24, 0xff);
  htt     = BITMASK(edx, 28, 0x01);
#endif

#if defined(CORE_PRESCOTT)
  cpuid(4, &eax, &ebx, &ecx, &edx);
  ncores = BITMASK(eax, 26, 0x3f);

  if (htt == 0)  nlprocs = 0;
#endif

#if defined(CORE_OPTERON)
  cpuid(0x80000008, &eax, &ebx, &ecx, &edx);
  ncores = BITMASK(ecx,  0, 0xff);

  cpuid(0x80000001, &eax, &ebx, &ecx, &edx);
  cmplegacy = BITMASK(ecx,  1, 0x01);

  if (htt == 0) {
    nlprocs = 0;
    ncores  = 0;
    cmplegacy = 0;
  }
#endif

  ncores  ++;

  fprintf(stderr, "APICID = %d  Number of core = %d\n", apicid, ncores);

  return 0;
}
#endif

#endif

#if defined(ARCH_IA64)

static inline BLASULONG cpuid(BLASULONG regnum){
  BLASULONG value;

#ifndef __ECC
  asm ("mov %0=cpuid[%r1]" : "=r"(value) : "rO"(regnum));
#else
 value = __getIndReg(_IA64_REG_INDR_CPUID, regnum);
#endif

  return value;
}

#if 1

void blas_set_parameter(void){

  BLASULONG cpuid3, size;

  cpuid3 = cpuid(3);

  size = BITMASK(cpuid3, 16, 0xff);

  sgemm_p = 192 * (size + 1);
  dgemm_p =  96 * (size + 1);
  cgemm_p =  96 * (size + 1);
  zgemm_p =  48 * (size + 1);
#ifdef EXPRECISION
  qgemm_p =  64 * (size + 1);
  xgemm_p =  32 * (size + 1);
#endif
#ifdef QUAD_PRECISION
  qgemm_p =  32 * (size + 1);
  xgemm_p =  16 * (size + 1);
#endif

  sgemm_r = (((BUFFER_SIZE - ((SGEMM_P * SGEMM_Q *  4 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (SGEMM_Q *  4)) - 15) & ~15;
  dgemm_r = (((BUFFER_SIZE - ((DGEMM_P * DGEMM_Q *  8 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (DGEMM_Q *  8)) - 15) & ~15;
  cgemm_r = (((BUFFER_SIZE - ((CGEMM_P * CGEMM_Q *  8 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (CGEMM_Q *  8)) - 15) & ~15;
  zgemm_r = (((BUFFER_SIZE - ((ZGEMM_P * ZGEMM_Q * 16 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (ZGEMM_Q * 16)) - 15) & ~15;
#if defined(EXPRECISION) || defined(QUAD_PRECISION)
  qgemm_r = (((BUFFER_SIZE - ((QGEMM_P * QGEMM_Q * 16 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (QGEMM_Q * 16)) - 15) & ~15;
  xgemm_r = (((BUFFER_SIZE - ((XGEMM_P * XGEMM_Q * 32 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (XGEMM_Q * 32)) - 15) & ~15;
#endif

  return;
}

#else

#define IA64_SYS_NAME  "/sys/devices/system/cpu/cpu0/cache/index3/size"
#define IA64_PROC_NAME "/proc/pal/cpu0/cache_info"

void blas_set_parameter(void){

  BLASULONG cpuid3;
  int size = 0;

#if 1
  char buffer[128];
  FILE *infile;

  if ((infile = fopen(IA64_SYS_NAME, "r")) != NULL) {

    fgets(buffer, sizeof(buffer), infile);
    fclose(infile);

    size = atoi(buffer) / 1536;
  }

  if (size <= 0) {
    if ((infile = fopen(IA64_PROC_NAME, "r")) != NULL) {

      while(fgets(buffer, sizeof(buffer), infile) != NULL) {
	if ((!strncmp("Data/Instruction Cache level 3", buffer, 30))) break;
      }

      fgets(buffer, sizeof(buffer), infile);

      fclose(infile);

      *strstr(buffer, "bytes") = (char)NULL;

      size = atoi(strchr(buffer, ':') + 1) / 1572864;
    }
  }
#endif

  /* The last resort */

  if (size <= 0) {
    cpuid3 = cpuid(3);

    size = BITMASK(cpuid3, 16, 0xff) + 1;
  }

  sgemm_p = 320 * size;
  dgemm_p = 160 * size;
  cgemm_p = 160 * size;
  zgemm_p =  80 * size;
#ifdef EXPRECISION
  qgemm_p =  80 * size;
  xgemm_p =  40 * size;
#endif

  sgemm_r = (((BUFFER_SIZE - ((SGEMM_P * SGEMM_Q *  4 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (SGEMM_Q *  4)) - 15) & ~15;
  dgemm_r = (((BUFFER_SIZE - ((DGEMM_P * DGEMM_Q *  8 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (DGEMM_Q *  8)) - 15) & ~15;
  cgemm_r = (((BUFFER_SIZE - ((CGEMM_P * CGEMM_Q *  8 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (CGEMM_Q *  8)) - 15) & ~15;
  zgemm_r = (((BUFFER_SIZE - ((ZGEMM_P * ZGEMM_Q * 16 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (ZGEMM_Q * 16)) - 15) & ~15;
#ifdef EXPRECISION
  qgemm_r = (((BUFFER_SIZE - ((QGEMM_P * QGEMM_Q * 16 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (QGEMM_Q * 16)) - 15) & ~15;
  xgemm_r = (((BUFFER_SIZE - ((XGEMM_P * XGEMM_Q * 32 + GEMM_OFFSET_A + GEMM_ALIGN) & ~GEMM_ALIGN)) / (XGEMM_Q * 32)) - 15) & ~15;
#endif

  return;
}

#endif

#endif

#if defined(ARCH_MIPS64)
void blas_set_parameter(void){
#if defined(LOONGSON3A)
#ifdef SMP
  if(blas_num_threads == 1){
#endif
    //single thread
    dgemm_r = 1024;
#ifdef SMP
  }else{
    //multi thread
    dgemm_r = 200;
  }
#endif
#endif

#if defined(LOONGSON3B)
#ifdef SMP
  if(blas_num_threads == 1 || blas_num_threads == 2){
#endif
    //single thread
    dgemm_r = 640;
#ifdef SMP
  }else{
    //multi thread
    dgemm_r = 160;
  }
#endif
#endif

}
#endif

#if defined(ARCH_ARM64)

void blas_set_parameter(void)
{
}

#endif
