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
#include <stdlib.h>
#include "common.h"

#if defined(OS_CYGWIN_NT) && !defined(unlikely)
#ifdef __GNUC__
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define unlikely(x) (x)
#endif
#endif

/* This is a thread implementation for Win32 lazy implementation */

/* Thread server common infomation */
typedef struct{
  CRITICAL_SECTION lock;
  HANDLE filled;
  HANDLE killed;

  blas_queue_t	*queue;    /* Parameter Pointer */
  int		shutdown;  /* server shutdown flag */

} blas_pool_t;

/* We need this global for cheking if initialization is finished.   */
int blas_server_avail = 0;

/* Local Variables */
static BLASULONG server_lock       = 0;

static blas_pool_t   pool;
static HANDLE	    blas_threads   [MAX_CPU_NUMBER];
static DWORD	    blas_threads_id[MAX_CPU_NUMBER];



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

/* This is a main routine of threads. Each thread waits until job is */
/* queued.                                                           */

static DWORD WINAPI blas_thread_server(void *arg){

  /* Thread identifier */
#ifdef SMP_DEBUG
  BLASLONG  cpu = (BLASLONG)arg;
#endif

  void *buffer, *sa, *sb;
  blas_queue_t	*queue;
  DWORD action;
  HANDLE handles[] = {pool.filled, pool.killed};

  /* Each server needs each buffer */
  buffer   = blas_memory_alloc(2);

#ifdef SMP_DEBUG
  fprintf(STDERR, "Server[%2ld] Thread is started!\n", cpu);
#endif

  while (1){

    /* Waiting for Queue */

#ifdef SMP_DEBUG
    fprintf(STDERR, "Server[%2ld] Waiting for Queue.\n", cpu);
#endif

    do {
      action = WaitForMultipleObjects(2, handles, FALSE, INFINITE);
    } while ((action != WAIT_OBJECT_0) && (action != WAIT_OBJECT_0 + 1));

    if (action == WAIT_OBJECT_0 + 1) break;

#ifdef SMP_DEBUG
    fprintf(STDERR, "Server[%2ld] Got it.\n", cpu);
#endif

    EnterCriticalSection(&pool.lock);

    queue = pool.queue;
    if (queue) pool.queue = queue->next;

    LeaveCriticalSection(&pool.lock);

    if (queue)  {
      int (*routine)(blas_arg_t *, void *, void *, void *, void *, BLASLONG) = queue -> routine;

      if (pool.queue) SetEvent(pool.filled);

      sa = queue -> sa;
      sb = queue -> sb;

#ifdef CONSISTENT_FPCSR
      __asm__ __volatile__ ("ldmxcsr %0" : : "m" (queue -> sse_mode));
      __asm__ __volatile__ ("fldcw %0"   : : "m" (queue -> x87_mode));
#endif

#ifdef SMP_DEBUG
      fprintf(STDERR, "Server[%2ld] Started.  Mode = 0x%03x M = %3ld N=%3ld K=%3ld\n",
	      cpu, queue->mode, queue-> args ->m, queue->args->n, queue->args->k);
#endif

      // fprintf(stderr, "queue start[%ld]!!!\n", cpu);

#ifdef MONITOR
      main_status[cpu] = MAIN_RUNNING1;
#endif

      if (sa == NULL) sa = (void *)((BLASLONG)buffer + GEMM_OFFSET_A);

      if (sb == NULL) {
	if (!(queue -> mode & BLAS_COMPLEX)){
#ifdef EXPRECISION
	  if (queue -> mode & BLAS_XDOUBLE){
	    sb = (void *)(((BLASLONG)sa + ((XGEMM_P * XGEMM_Q * sizeof(xdouble)
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

#ifdef MONITOR
      main_status[cpu] = MAIN_RUNNING2;
#endif

      if (!(queue -> mode & BLAS_LEGACY)) {

	(routine)(queue -> args, queue -> range_m, queue -> range_n, sa, sb, queue -> position);
      } else {
	legacy_exec(routine, queue -> mode, queue -> args, sb);
      }
    }else{
		continue; //if queue == NULL
	}

#ifdef SMP_DEBUG
    fprintf(STDERR, "Server[%2ld] Finished!\n", cpu);
#endif

    EnterCriticalSection(&queue->lock);

    queue -> status = BLAS_STATUS_FINISHED;

    LeaveCriticalSection(&queue->lock);

    SetEvent(queue->finish);
  }

  /* Shutdown procedure */

#ifdef SMP_DEBUG
  fprintf(STDERR, "Server[%2ld] Shutdown!\n",  cpu);
#endif

  blas_memory_free(buffer);

  return 0;
  }

/* Initializing routine */
int blas_thread_init(void){
  BLASLONG i;

  if (blas_server_avail || (blas_cpu_number <= 1)) return 0;

  LOCK_COMMAND(&server_lock);

#ifdef SMP_DEBUG
  fprintf(STDERR, "Initializing Thread(Num. threads = %d)\n",
	  blas_cpu_number);
#endif

  if (!blas_server_avail){

    InitializeCriticalSection(&pool.lock);
    pool.filled = CreateEvent(NULL, FALSE, FALSE, NULL);
    pool.killed = CreateEvent(NULL, TRUE,  FALSE, NULL);

    pool.shutdown = 0;
    pool.queue    = NULL;

    for(i = 0; i < blas_cpu_number - 1; i++){
      blas_threads[i] = CreateThread(NULL, 0,
				     blas_thread_server, (void *)i,
				     0, &blas_threads_id[i]);
    }

    blas_server_avail = 1;
  }

  UNLOCK_COMMAND(&server_lock);

  return 0;
}

/*
   User can call one of two routines.

     exec_blas_async ... immediately returns after jobs are queued.

     exec_blas       ... returns after jobs are finished.
*/

int exec_blas_async(BLASLONG pos, blas_queue_t *queue){

#if defined(SMP_SERVER) && defined(OS_CYGWIN_NT)
  // Handle lazy re-init of the thread-pool after a POSIX fork
  if (unlikely(blas_server_avail == 0)) blas_thread_init();
#endif

  blas_queue_t *current;

  current = queue;

  while (current) {
    InitializeCriticalSection(&current -> lock);
    current -> finish = CreateEvent(NULL, FALSE, FALSE, NULL);
    current -> position = pos;

#ifdef CONSISTENT_FPCSR
    __asm__ __volatile__ ("fnstcw %0"  : "=m" (current -> x87_mode));
    __asm__ __volatile__ ("stmxcsr %0" : "=m" (current -> sse_mode));
#endif

    current = current -> next;
    pos ++;
  }

  EnterCriticalSection(&pool.lock);

  if (pool.queue) {
    current = pool.queue;
    while (current -> next) current = current -> next;
    current -> next = queue;
  } else {
    pool.queue = queue;
  }

  LeaveCriticalSection(&pool.lock);

  SetEvent(pool.filled);

  return 0;
}

int exec_blas_async_wait(BLASLONG num, blas_queue_t *queue){

#ifdef SMP_DEBUG
    fprintf(STDERR, "Synchronization Waiting.\n");
#endif

    while (num){
#ifdef SMP_DEBUG
    fprintf(STDERR, "Waiting Queue ..\n");
#endif

      WaitForSingleObject(queue->finish, INFINITE);

      CloseHandle(queue->finish);
      DeleteCriticalSection(&queue -> lock);

      queue = queue -> next;
      num --;
    }

#ifdef SMP_DEBUG
    fprintf(STDERR, "Completely Done.\n\n");
#endif

  return 0;
}

/* Execute Threads */
int exec_blas(BLASLONG num, blas_queue_t *queue){

#if defined(SMP_SERVER) && defined(OS_CYGWIN_NT)
  // Handle lazy re-init of the thread-pool after a POSIX fork
  if (unlikely(blas_server_avail == 0)) blas_thread_init();
#endif

#ifndef ALL_THREADED
   int (*routine)(blas_arg_t *, void *, void *, double *, double *, BLASLONG);
#endif

  if ((num <= 0) || (queue == NULL)) return 0;

  if ((num > 1) && queue -> next) exec_blas_async(1, queue -> next);

  routine = queue -> routine;

    if (!(queue -> mode & BLAS_LEGACY)) {
      (routine)(queue -> args, queue -> range_m, queue -> range_n,
		queue -> sa, queue -> sb, 0);
    } else {
      legacy_exec(routine, queue -> mode, queue -> args, queue -> sb);
    }

  if ((num > 1) && queue -> next) exec_blas_async_wait(num - 1, queue -> next);

  return 0;
}

/* Shutdown procedure, but user don't have to call this routine. The */
/* kernel automatically kill threads.                                */

int BLASFUNC(blas_thread_shutdown)(void){

  int i;

  if (!blas_server_avail) return 0;

  LOCK_COMMAND(&server_lock);

  if (blas_server_avail){

    SetEvent(pool.killed);

    for(i = 0; i < blas_num_threads - 1; i++){
      WaitForSingleObject(blas_threads[i], 5);  //INFINITE);
#ifndef OS_WINDOWSSTORE
// TerminateThread is only available with WINAPI_DESKTOP and WINAPI_SYSTEM not WINAPI_APP in UWP
      TerminateThread(blas_threads[i],0);
#endif
    }

    blas_server_avail = 0;
  }

  UNLOCK_COMMAND(&server_lock);

  return 0;
}

void goto_set_num_threads(int num_threads)
{
	long i;

#if defined(SMP_SERVER) && defined(OS_CYGWIN_NT)
	// Handle lazy re-init of the thread-pool after a POSIX fork
	if (unlikely(blas_server_avail == 0)) blas_thread_init();
#endif

	if (num_threads < 1) num_threads = blas_cpu_number;

	if (num_threads > MAX_CPU_NUMBER) num_threads = MAX_CPU_NUMBER;

	if (num_threads > blas_num_threads) {

		LOCK_COMMAND(&server_lock);

		//increased_threads = 1;
	    if (!blas_server_avail){

			InitializeCriticalSection(&pool.lock);
			pool.filled = CreateEvent(NULL, FALSE, FALSE, NULL);
			pool.killed = CreateEvent(NULL, TRUE,  FALSE, NULL);

			pool.shutdown = 0;
			pool.queue    = NULL;
			blas_server_avail = 1;
		}

		for(i = blas_num_threads - 1; i < num_threads - 1; i++){

			blas_threads[i] = CreateThread(NULL, 0,
				     blas_thread_server, (void *)i,
				     0, &blas_threads_id[i]);
		}

		blas_num_threads = num_threads;

		UNLOCK_COMMAND(&server_lock);
	}

	blas_cpu_number  = num_threads;
}

void openblas_set_num_threads(int num)
{
	goto_set_num_threads(num);
}
