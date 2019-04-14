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

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
//#include <sys/mman.h>
#include "common.h"

#ifndef USE_OPENMP

#include "blas_server.c"

#else

#ifndef OMP_SCHED
#define OMP_SCHED static
#endif

int blas_server_avail = 0;

static void * blas_thread_buffer[MAX_PARALLEL_NUMBER][MAX_CPU_NUMBER];
#if __STDC_VERSION__ >= 201112L
static atomic_bool blas_buffer_inuse[MAX_PARALLEL_NUMBER];
#else
static _Bool blas_buffer_inuse[MAX_PARALLEL_NUMBER];
#endif

void goto_set_num_threads(int num_threads) {

  int i=0, j=0;

  if (num_threads < 1) num_threads = blas_num_threads;

  if (num_threads > MAX_CPU_NUMBER) num_threads = MAX_CPU_NUMBER;

  if (num_threads > blas_num_threads) {
    blas_num_threads = num_threads;
  }

  blas_cpu_number  = num_threads;

  omp_set_num_threads(blas_cpu_number);

  //adjust buffer for each thread
  for(i=0; i<MAX_PARALLEL_NUMBER; i++) {
    for(j=0; j<blas_cpu_number; j++){
      if(blas_thread_buffer[i][j]==NULL){
        blas_thread_buffer[i][j]=blas_memory_alloc(2);
      }
    }
    for(; j<MAX_CPU_NUMBER; j++){
      if(blas_thread_buffer[i][j]!=NULL){
        blas_memory_free(blas_thread_buffer[i][j]);
        blas_thread_buffer[i][j]=NULL;
      }
    }
  }
#if defined(ARCH_MIPS64)
  //set parameters for different number of threads.
  blas_set_parameter();
#endif

}
void openblas_set_num_threads(int num_threads) {

	goto_set_num_threads(num_threads);
}

int blas_thread_init(void){

  int i=0, j=0;

  blas_get_cpu_number();

  blas_server_avail = 1;

  for(i=0; i<MAX_PARALLEL_NUMBER; i++) {
    for(j=0; j<blas_num_threads; j++){
      blas_thread_buffer[i][j]=blas_memory_alloc(2);
    }
    for(; j<MAX_CPU_NUMBER; j++){
      blas_thread_buffer[i][j]=NULL;
    }
  }

  return 0;
}

int BLASFUNC(blas_thread_shutdown)(void){
  int i=0, j=0;
  blas_server_avail = 0;

  for(i=0; i<MAX_PARALLEL_NUMBER; i++) {
    for(j=0; j<MAX_CPU_NUMBER; j++){
      if(blas_thread_buffer[i][j]!=NULL){
        blas_memory_free(blas_thread_buffer[i][j]);
        blas_thread_buffer[i][j]=NULL;
      }
    }
  }

  return 0;
}

static void legacy_exec(void *func, int mode, blas_arg_t *args, void *sb){

      if (!(mode & BLAS_COMPLEX)){
#ifdef EXPRECISION
	if (mode & BLAS_XDOUBLE){
	  /* REAL / Extended Double */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, xdouble,
			xdouble *, BLASLONG, xdouble *, BLASLONG,
			xdouble *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((xdouble *)args -> alpha)[0],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	} else
#endif
	  if (mode & BLAS_DOUBLE){
	    /* REAL / Double */
	    void (*afunc)(BLASLONG, BLASLONG, BLASLONG, double,
			  double *, BLASLONG, double *, BLASLONG,
			  double *, BLASLONG, void *) = func;

	    afunc(args -> m, args -> n, args -> k,
		  ((double *)args -> alpha)[0],
		  args -> a, args -> lda,
		  args -> b, args -> ldb,
		  args -> c, args -> ldc, sb);
	  } else {
	    /* REAL / Single */
	    void (*afunc)(BLASLONG, BLASLONG, BLASLONG, float,
			  float *, BLASLONG, float *, BLASLONG,
			  float *, BLASLONG, void *) = func;

	    afunc(args -> m, args -> n, args -> k,
		  ((float *)args -> alpha)[0],
		  args -> a, args -> lda,
		  args -> b, args -> ldb,
		  args -> c, args -> ldc, sb);
	  }
      } else {
#ifdef EXPRECISION
	if (mode & BLAS_XDOUBLE){
	  /* COMPLEX / Extended Double */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble,
			xdouble *, BLASLONG, xdouble *, BLASLONG,
			xdouble *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((xdouble *)args -> alpha)[0],
		((xdouble *)args -> alpha)[1],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	} else
#endif
	  if (mode & BLAS_DOUBLE){
	    /* COMPLEX / Double */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, double, double,
			double *, BLASLONG, double *, BLASLONG,
			double *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((double *)args -> alpha)[0],
		((double *)args -> alpha)[1],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	  } else {
	    /* COMPLEX / Single */
	  void (*afunc)(BLASLONG, BLASLONG, BLASLONG, float, float,
			float *, BLASLONG, float *, BLASLONG,
			float *, BLASLONG, void *) = func;

	  afunc(args -> m, args -> n, args -> k,
		((float *)args -> alpha)[0],
		((float *)args -> alpha)[1],
		args -> a, args -> lda,
		args -> b, args -> ldb,
		args -> c, args -> ldc, sb);
	  }
      }
}

static void exec_threads(blas_queue_t *queue, int buf_index){

  void *buffer, *sa, *sb;
  int pos=0, release_flag=0;

  buffer = NULL;
  sa = queue -> sa;
  sb = queue -> sb;

#ifdef CONSISTENT_FPCSR
  __asm__ __volatile__ ("ldmxcsr %0" : : "m" (queue -> sse_mode));
  __asm__ __volatile__ ("fldcw %0"   : : "m" (queue -> x87_mode));
#endif

  if ((sa == NULL) && (sb == NULL) && ((queue -> mode & BLAS_PTHREAD) == 0)) {

    pos = omp_get_thread_num();
    buffer = blas_thread_buffer[buf_index][pos];

    //fallback
    if(buffer==NULL) {
      buffer = blas_memory_alloc(2);
      release_flag=1;
    }

    if (sa == NULL) {
      sa = (void *)((BLASLONG)buffer + GEMM_OFFSET_A);
      queue->sa=sa;
    }

    if (sb == NULL) {
      if (!(queue -> mode & BLAS_COMPLEX)){
#ifdef EXPRECISION
	if (queue -> mode & BLAS_XDOUBLE){
	  sb = (void *)(((BLASLONG)sa + ((QGEMM_P * QGEMM_Q * sizeof(xdouble)
					  + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	} else
#endif
	  if (queue -> mode & BLAS_DOUBLE){
	    sb = (void *)(((BLASLONG)sa + ((DGEMM_P * DGEMM_Q * sizeof(double)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);

	  } else {
	    sb = (void *)(((BLASLONG)sa + ((SGEMM_P * SGEMM_Q * sizeof(float)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	  }
      } else {
#ifdef EXPRECISION
	if (queue -> mode & BLAS_XDOUBLE){
	  sb = (void *)(((BLASLONG)sa + ((XGEMM_P * XGEMM_Q * 2 * sizeof(xdouble)
					  + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	} else
#endif
	  if (queue -> mode & BLAS_DOUBLE){
	    sb = (void *)(((BLASLONG)sa + ((ZGEMM_P * ZGEMM_Q * 2 * sizeof(double)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	  } else {
	    sb = (void *)(((BLASLONG)sa + ((CGEMM_P * CGEMM_Q * 2 * sizeof(float)
					    + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);
	  }
      }
      queue->sb=sb;
    }
  }

  if (queue -> mode & BLAS_LEGACY) {
    legacy_exec(queue -> routine, queue -> mode, queue -> args, sb);
  } else
    if (queue -> mode & BLAS_PTHREAD) {
      void (*pthreadcompat)(void *) = queue -> routine;
      (pthreadcompat)(queue -> args);

    } else {
      int (*routine)(blas_arg_t *, void *, void *, void *, void *, BLASLONG) = queue -> routine;

      (routine)(queue -> args, queue -> range_m, queue -> range_n, sa, sb, queue -> position);

    }

  if (release_flag) blas_memory_free(buffer);

}

int exec_blas(BLASLONG num, blas_queue_t *queue){

  BLASLONG i, buf_index;

  if ((num <= 0) || (queue == NULL)) return 0;

#ifdef CONSISTENT_FPCSR
  for (i = 0; i < num; i ++) {
    __asm__ __volatile__ ("fnstcw %0"  : "=m" (queue[i].x87_mode));
    __asm__ __volatile__ ("stmxcsr %0" : "=m" (queue[i].sse_mode));
  }
#endif

  while(true) {
    for(i=0; i < MAX_PARALLEL_NUMBER; i++) {
#if __STDC_VERSION__ >= 201112L
      _Bool inuse = false;
      if(atomic_compare_exchange_weak(&blas_buffer_inuse[i], &inuse, true)) {
#else
      if(blas_buffer_inuse[i] == false) {
        blas_buffer_inuse[i] = true;
#endif
        buf_index = i;
        break;
      }
    }
    if(i != MAX_PARALLEL_NUMBER)
      break;
  }

#pragma omp parallel for schedule(OMP_SCHED)
  for (i = 0; i < num; i ++) {

#ifndef USE_SIMPLE_THREADED_LEVEL3
    queue[i].position = i;
#endif

    exec_threads(&queue[i], buf_index);
  }

#if __STDC_VERSION__ >= 201112L
  atomic_store(&blas_buffer_inuse[buf_index], false);
#else
  blas_buffer_inuse[buf_index] = false;
#endif

  return 0;
}

#endif
