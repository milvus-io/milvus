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

#ifndef COMMON_ARM
#define COMMON_ARM

#if defined(ARMV5) || defined(ARMV6)

#define MB
#define WMB

#else

#define MB   __asm__ __volatile__ ("dmb  ish" : : : "memory")
#define WMB  __asm__ __volatile__ ("dmb  ishst" : : : "memory")

#endif

#define INLINE inline

#define RETURN_BY_COMPLEX

#ifndef ASSEMBLER

#if defined(ARMV6) || defined(ARMV7) || defined(ARMV8)

static void __inline blas_lock(volatile BLASULONG *address){

  int register ret;

  do {
    while (*address) {YIELDING;};

    __asm__ __volatile__(
                         "ldrex r2, [%1]      \n\t"
                         "strex %0, %2, [%1]  \n\t"
                         "orr   %0, r2        \n\t"
                         : "=&r"(ret)
                         : "r"(address), "r"(1)
                         : "memory", "r2"
    );

  } while (ret);
  MB;
}

#define BLAS_LOCK_DEFINED
#endif

static inline int blas_quickdivide(blasint x, blasint y){
  return x / y;
}

#if !defined(HAVE_VFP)
/* no FPU, soft float */
#define GET_IMAGE(res)
#elif defined(DOUBLE)
#define GET_IMAGE(res)  __asm__ __volatile__("vstr.f64 d1, %0" : "=m"(res) : : "memory")
#else
#define GET_IMAGE(res)  __asm__ __volatile__("vstr.f32 s1, %0" : "=m"(res) : : "memory")
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
	.arm		 ;\
	.global	REALNAME ;\
REALNAME:

#define EPILOGUE

#define PROFCODE

#endif


#define SEEK_ADDRESS

#ifndef PAGESIZE
#define PAGESIZE        ( 4 << 10)
#endif
#define HUGE_PAGESIZE   ( 4 << 20)

#define BUFFER_SIZE     (16 << 20)


#define BASE_ADDRESS (START_ADDRESS - BUFFER_SIZE * MAX_CPU_NUMBER)

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#if !defined(ARMV5) && !defined(ARMV6) && !defined(ARMV7) && !defined(ARMV8)
#error "you must define ARMV5, ARMV6, ARMV7 or ARMV8"
#endif

#endif
