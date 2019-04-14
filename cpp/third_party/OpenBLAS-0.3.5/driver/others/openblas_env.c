/***************************************************************************
Copyright (c) 2011-2016,                              The OpenBLAS Project
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

#include "common.h"

static int openblas_env_verbose=0;
static unsigned int openblas_env_thread_timeout=0;
static int openblas_env_block_factor=0;
static int openblas_env_openblas_num_threads=0;
static int openblas_env_goto_num_threads=0;
static int openblas_env_omp_num_threads=0;

int openblas_verbose() { return openblas_env_verbose;}
unsigned int openblas_thread_timeout() { return openblas_env_thread_timeout;}
int openblas_block_factor() { return openblas_env_block_factor;}
int openblas_num_threads_env() { return openblas_env_openblas_num_threads;}
int openblas_goto_num_threads_env() { return openblas_env_goto_num_threads;}
int openblas_omp_num_threads_env() { return openblas_env_omp_num_threads;}

void openblas_read_env() {
  int ret=0;
  env_var_t p;
  if (readenv(p,"OPENBLAS_VERBOSE")) ret = atoi(p);
  if(ret<0) ret=0;
  openblas_env_verbose=ret;

  ret=0;
  if (readenv(p,"OPENBLAS_BLOCK_FACTOR")) ret = atoi(p);
  if(ret<0) ret=0;
  openblas_env_block_factor=ret;

  ret=0;
  if (readenv(p,"OPENBLAS_THREAD_TIMEOUT")) ret = atoi(p);
  if(ret<0) ret=0;
  openblas_env_thread_timeout=(unsigned int)ret;

  ret=0;
  if (readenv(p,"OPENBLAS_NUM_THREADS")) ret = atoi(p);
  if(ret<0) ret=0;
  openblas_env_openblas_num_threads=ret;

  ret=0;
  if (readenv(p,"GOTO_NUM_THREADS")) ret = atoi(p);
  if(ret<0) ret=0;
  openblas_env_goto_num_threads=ret;

  ret=0;
  if (readenv(p,"OMP_NUM_THREADS")) ret = atoi(p);
  if(ret<0) ret=0;
  openblas_env_omp_num_threads=ret;

}


