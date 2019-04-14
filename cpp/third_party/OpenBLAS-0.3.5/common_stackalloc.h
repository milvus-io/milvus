/*******************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
*******************************************************************************/

#define STACK_ALLOC_PROTECT
#ifdef STACK_ALLOC_PROTECT
// Try to detect stack smashing
#include <assert.h>
#define STACK_ALLOC_PROTECT_SET volatile int stack_check = 0x7fc01234;
#define STACK_ALLOC_PROTECT_CHECK assert(stack_check == 0x7fc01234);
#else
#define STACK_ALLOC_PROTECT_SET
#define STACK_ALLOC_PROTECT_CHECK
#endif

#if defined(MAX_STACK_ALLOC) && MAX_STACK_ALLOC > 0

/*
 * Allocate a buffer on the stack if the size is smaller than MAX_STACK_ALLOC.
 * Stack allocation is much faster than blas_memory_alloc or malloc, particularly
 * when OpenBLAS is used from a multi-threaded application.
 * SIZE must be carefully chosen to be:
 * - as small as possible to maximize the number of stack allocation
 * - large enough to support all architectures and kernel
 * Chosing a too small SIZE will lead to a stack smashing.
 */
#define STACK_ALLOC(SIZE, TYPE, BUFFER)                                        \
  /* make it volatile because some function (ex: dgemv_n.S) */                 \
  /* do not restore all register */                                            \
  volatile int stack_alloc_size = SIZE;                                        \
  if (stack_alloc_size > MAX_STACK_ALLOC / sizeof(TYPE)) stack_alloc_size = 0; \
  STACK_ALLOC_PROTECT_SET                                                      \
  /* Avoid declaring an array of length 0 */                                   \
  TYPE stack_buffer[stack_alloc_size ? stack_alloc_size : 1]                   \
      __attribute__((aligned(0x20)));                                          \
  BUFFER = stack_alloc_size ? stack_buffer : (TYPE *)blas_memory_alloc(1);
#else
  //Original OpenBLAS/GotoBLAS codes.
  #define STACK_ALLOC(SIZE, TYPE, BUFFER) BUFFER = (TYPE *)blas_memory_alloc(1)
#endif


#if defined(MAX_STACK_ALLOC) && MAX_STACK_ALLOC > 0
#define STACK_FREE(BUFFER)    \
  STACK_ALLOC_PROTECT_CHECK   \
  if(!stack_alloc_size)       \
    blas_memory_free(BUFFER);
#else
#define STACK_FREE(BUFFER) blas_memory_free(BUFFER)
#endif

