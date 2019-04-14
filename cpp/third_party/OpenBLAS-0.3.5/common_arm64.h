/*****************************************************************************
Copyright (c) 2011-2015, The OpenBLAS Project
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
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************************/

#ifndef COMMON_ARM64
#define COMMON_ARM64

#define MB   __asm__ __volatile__ ("dmb  ish" : : : "memory")
#define WMB  __asm__ __volatile__ ("dmb  ishst" : : : "memory")


#define INLINE inline

#ifdef F_INTERFACE_FLANG
#define RETURN_BY_STACK
#else
#define RETURN_BY_COMPLEX
#endif

#ifndef ASSEMBLER


static void __inline blas_lock(volatile BLASULONG *address){

  BLASULONG ret;

  do {
    while (*address) {YIELDING;};

    __asm__ __volatile__(
			 "mov	x4, #1							\n\t"
                         "1:                                                            \n\t"
                         "ldaxr x2, [%1]                                                \n\t"
                         "cbnz  x2, 1b                                                  \n\t"
			 "2:								\n\t"
                         "stxr  w3, x4, [%1]                                            \n\t"
                         "cbnz  w3, 1b                                                  \n\t"
                         "mov   %0, #0                                                  \n\t"
                         : "=r"(ret), "=r"(address)
                         : "1"(address)
                         : "memory", "x2" , "x3", "x4"


    );


  } while (ret);

}

#define BLAS_LOCK_DEFINED



static inline int blas_quickdivide(blasint x, blasint y){
  return x / y;
}

#if defined(DOUBLE)
#define GET_IMAGE(res)  __asm__ __volatile__("str d1, %0" : "=m"(res) : : "memory")
#else
#define GET_IMAGE(res)  __asm__ __volatile__("str s1, %0" : "=m"(res) : : "memory")
#endif

#define GET_IMAGE_CANCEL

#endif


#ifndef F_INTERFACE
#define REALNAME ASMNAME
#else
#define REALNAME ASMFNAME
#endif

#if defined(ASSEMBLER) && !defined(NEEDPARAM)

#define PROLOGUE \
	.text ;\
	.align	4 ;\
	.global	REALNAME ;\
	.type	REALNAME, %function ;\
REALNAME:

#define EPILOGUE

#define PROFCODE

#endif


#define SEEK_ADDRESS

#ifndef PAGESIZE
#define PAGESIZE        ( 4 << 10)
#endif
#define HUGE_PAGESIZE   ( 4 << 20)

#if defined(CORTEXA57)
#define BUFFER_SIZE     (20 << 20)
#else
#define BUFFER_SIZE     (16 << 20)
#endif


#define BASE_ADDRESS (START_ADDRESS - BUFFER_SIZE * MAX_CPU_NUMBER)

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#endif

