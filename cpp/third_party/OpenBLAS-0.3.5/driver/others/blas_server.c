/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
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

#include "common.h"
#if defined(OS_LINUX) || defined(OS_NETBSD) || defined(OS_DARWIN) || defined(OS_ANDROID) || defined(OS_SUNOS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLY) || defined(OS_HAIKU)
#include <dlfcn.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/time.h>
#endif

#ifndef likely
#ifdef __GNUC__
#define likely(x) __builtin_expect(!!(x), 1)
#else
#define likely(x) (x)
#endif
#endif
#ifndef unlikely
#ifdef __GNUC__
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define unlikely(x) (x)
#endif
#endif

extern unsigned int openblas_thread_timeout();

#ifdef SMP_SERVER

#undef MONITOR
#undef TIMING
#undef TIMING_DEBUG
#undef NEED_STACKATTR

#define ATTRIBUTE_SIZE 128

/* This is a thread server model implementation.  The threads are   */
/* spawned at first access to blas library, and still remains until */
/* destruction routine is called.  The number of threads are        */
/* equal to "OMP_NUM_THREADS - 1" and thread only wakes up when     */
/* jobs is queued.                                                  */

/* We need this grobal for cheking if initialization is finished.   */
int blas_server_avail   __attribute__((aligned(ATTRIBUTE_SIZE))) = 0;

/* Local Variables */
#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t  server_lock    = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t  server_lock = 0;
#else
static unsigned long server_lock       = 0;
#endif

#define THREAD_STATUS_SLEEP		2
#define THREAD_STATUS_WAKEUP		4

static pthread_t       blas_threads [MAX_CPU_NUMBER];

typedef struct {
  blas_queue_t * volatile queue   __attribute__((aligned(ATTRIBUTE_SIZE)));

#if defined(OS_LINUX) && !defined(NO_AFFINITY)
  int	node;
#endif

  volatile long		 status;

  pthread_mutex_t	 lock;
  pthread_cond_t	 wakeup;

} thread_status_t;

static thread_status_t thread_status[MAX_CPU_NUMBER] __attribute__((aligned(ATTRIBUTE_SIZE)));

#ifndef THREAD_TIMEOUT
#define THREAD_TIMEOUT	28
#endif

static unsigned int thread_timeout = (1U << (THREAD_TIMEOUT));

#ifdef MONITOR

/* Monitor is a function to see thread's status for every seconds. */
/* Usually it turns off and it's for debugging.                    */

static pthread_t      monitor_thread;
static int main_status[MAX_CPU_NUMBER];
#define MAIN_ENTER	 0x01
#define MAIN_EXIT	 0x02
#define MAIN_TRYLOCK	 0x03
#define MAIN_LOCKSUCCESS 0x04
#define MAIN_QUEUING	 0x05
#define MAIN_RECEIVING   0x06
#define MAIN_RUNNING1    0x07
#define MAIN_RUNNING2    0x08
#define MAIN_RUNNING3    0x09
#define MAIN_WAITING	 0x0a
#define MAIN_SLEEPING	 0x0b
#define MAIN_FINISH      0x0c
#define MAIN_DONE	 0x0d
#endif

#define BLAS_QUEUE_FINISHED	3
#define BLAS_QUEUE_RUNNING	4

#ifdef TIMING
BLASLONG	exit_time[MAX_CPU_NUMBER];
#endif

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

#if defined(OS_LINUX) && !defined(NO_AFFINITY)
int gotoblas_set_affinity(int);
int gotoblas_set_affinity2(int);
int get_node(void);
#endif

static int increased_threads = 0;

static void* blas_thread_server(void *arg){

  /* Thread identifier */
  BLASLONG  cpu = (BLASLONG)arg;
  unsigned int last_tick;
  void *buffer, *sa, *sb;
  blas_queue_t	*queue;

blas_queue_t *tscq;

#ifdef TIMING_DEBUG
  unsigned long start, stop;
#endif

#if defined(OS_LINUX) && !defined(NO_AFFINITY)
  if (!increased_threads)
    thread_status[cpu].node = gotoblas_set_affinity(cpu + 1);
  else
    thread_status[cpu].node = gotoblas_set_affinity(-1);
#endif

#ifdef MONITOR
  main_status[cpu] = MAIN_ENTER;
#endif

  buffer = blas_memory_alloc(2);

#ifdef SMP_DEBUG
  fprintf(STDERR, "Server[%2ld] Thread has just been spawned!\n", cpu);
#endif

  while (1){

#ifdef MONITOR
    main_status[cpu] = MAIN_QUEUING;
#endif

#ifdef TIMING
    exit_time[cpu] = rpcc();
#endif

      last_tick = (unsigned int)rpcc();

	pthread_mutex_lock  (&thread_status[cpu].lock);
        tscq=thread_status[cpu].queue;
	pthread_mutex_unlock  (&thread_status[cpu].lock);

	while(!tscq) {
	YIELDING;

	if ((unsigned int)rpcc() - last_tick > thread_timeout) {

	  pthread_mutex_lock  (&thread_status[cpu].lock);

	  if (!thread_status[cpu].queue) {
	    thread_status[cpu].status = THREAD_STATUS_SLEEP;
	    while (thread_status[cpu].status == THREAD_STATUS_SLEEP) {

#ifdef MONITOR
	      main_status[cpu] = MAIN_SLEEPING;
#endif

	      pthread_cond_wait(&thread_status[cpu].wakeup, &thread_status[cpu].lock);
	    }
	  }

	  pthread_mutex_unlock(&thread_status[cpu].lock);

	  last_tick = (unsigned int)rpcc();
	}
	pthread_mutex_lock  (&thread_status[cpu].lock);
        tscq=thread_status[cpu].queue;
	pthread_mutex_unlock  (&thread_status[cpu].lock);

      }

    queue = thread_status[cpu].queue;

    if ((long)queue == -1) break;

#ifdef MONITOR
    main_status[cpu] = MAIN_RECEIVING;
#endif

#ifdef TIMING_DEBUG
    start = rpcc();
#endif

    if (queue) {
      int (*routine)(blas_arg_t *, void *, void *, void *, void *, BLASLONG) = queue -> routine;

      pthread_mutex_lock  (&thread_status[cpu].lock);
      thread_status[cpu].queue = (blas_queue_t *)1;
      pthread_mutex_unlock  (&thread_status[cpu].lock);

      sa = queue -> sa;
      sb = queue -> sb;

#ifdef SMP_DEBUG
      if (queue -> args) {
	fprintf(STDERR, "Server[%2ld] Calculation started.  Mode = 0x%03x M = %3ld N=%3ld K=%3ld\n",
		cpu, queue->mode, queue-> args ->m, queue->args->n, queue->args->k);
      }
#endif

#ifdef CONSISTENT_FPCSR
      __asm__ __volatile__ ("ldmxcsr %0" : : "m" (queue -> sse_mode));
      __asm__ __volatile__ ("fldcw %0"   : : "m" (queue -> x87_mode));
#endif

#ifdef MONITOR
      main_status[cpu] = MAIN_RUNNING1;
#endif

      if (sa == NULL) sa = (void *)((BLASLONG)buffer + GEMM_OFFSET_A);

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

#ifdef MONITOR
	main_status[cpu] = MAIN_RUNNING2;
#endif

      if (queue -> mode & BLAS_LEGACY) {
	legacy_exec(routine, queue -> mode, queue -> args, sb);
      } else
	if (queue -> mode & BLAS_PTHREAD) {
	  void (*pthreadcompat)(void *) = queue -> routine;
	  (pthreadcompat)(queue -> args);
	} else
	  (routine)(queue -> args, queue -> range_m, queue -> range_n, sa, sb, queue -> position);

#ifdef SMP_DEBUG
      fprintf(STDERR, "Server[%2ld] Calculation finished!\n", cpu);
#endif

#ifdef MONITOR
      main_status[cpu] = MAIN_FINISH;
#endif

      // arm: make sure all results are written out _before_
      // thread is marked as done and other threads use them
      WMB;

      pthread_mutex_lock  (&thread_status[cpu].lock);
      thread_status[cpu].queue = (blas_queue_t * volatile) ((long)thread_status[cpu].queue & 0);  /* Need a trick */
      pthread_mutex_unlock  (&thread_status[cpu].lock);
      
      WMB;

    }

#ifdef MONITOR
      main_status[cpu] = MAIN_DONE;
#endif

#ifdef TIMING_DEBUG
    stop = rpcc();

    fprintf(STDERR, "Thread[%ld] : %16lu %16lu (%8lu cycles)\n", cpu + 1,
	    start, stop,
	    stop - start);
#endif

  }

  /* Shutdown procedure */

#ifdef SMP_DEBUG
      fprintf(STDERR, "Server[%2ld] Shutdown!\n",  cpu);
#endif

  blas_memory_free(buffer);

  //pthread_exit(NULL);

  return NULL;
}

#ifdef MONITOR

static BLASLONG num_suspend = 0;

static int blas_monitor(void *arg){
  int i;

  while(1){
    for (i = 0; i < blas_num_threads - 1; i++){
      switch (main_status[i]) {
      case MAIN_ENTER :
	fprintf(STDERR, "THREAD[%2d] : Entering.\n", i);
	break;
      case MAIN_EXIT :
	fprintf(STDERR, "THREAD[%2d] : Exiting.\n", i);
	break;
      case MAIN_TRYLOCK :
	fprintf(STDERR, "THREAD[%2d] : Trying lock operation.\n", i);
	break;
      case MAIN_QUEUING :
	fprintf(STDERR, "THREAD[%2d] : Queuing.\n", i);
	break;
      case MAIN_RECEIVING :
	fprintf(STDERR, "THREAD[%2d] : Receiving.\n", i);
	break;
      case MAIN_RUNNING1 :
	fprintf(STDERR, "THREAD[%2d] : Running1.\n", i);
	break;
      case MAIN_RUNNING2 :
	fprintf(STDERR, "THREAD[%2d] : Running2.\n", i);
	break;
      case MAIN_RUNNING3 :
	fprintf(STDERR, "THREAD[%2d] : Running3.\n", i);
	break;
      case MAIN_WAITING :
	fprintf(STDERR, "THREAD[%2d] : Waiting.\n", i);
	break;
      case MAIN_SLEEPING :
	fprintf(STDERR, "THREAD[%2d] : Sleeping.\n", i);
	break;
      case MAIN_FINISH :
	fprintf(STDERR, "THREAD[%2d] : Finishing.\n", i);
	break;
      case MAIN_DONE :
	fprintf(STDERR, "THREAD[%2d] : Job is done.\n", i);
	break;
      }

      fprintf(stderr, "Total number of suspended ... %ld\n", num_suspend);
    }
    sleep(1);
  }

 return 0;
}
#endif

/* Initializing routine */
int blas_thread_init(void){
  BLASLONG i;
  int ret;
  int thread_timeout_env;
#ifdef NEED_STACKATTR
  pthread_attr_t attr;
#endif

  if (blas_server_avail) return 0;

#ifdef NEED_STACKATTR
  pthread_attr_init(&attr);
  pthread_attr_setguardsize(&attr,  0x1000U);
  pthread_attr_setstacksize( &attr, 0x1000U);
#endif

  LOCK_COMMAND(&server_lock);

  if (!blas_server_avail){

    thread_timeout_env=openblas_thread_timeout();
    if (thread_timeout_env>0) {
      if (thread_timeout_env <  4) thread_timeout_env =  4;
      if (thread_timeout_env > 30) thread_timeout_env = 30;
      thread_timeout = (1 << thread_timeout_env);
    }

    for(i = 0; i < blas_num_threads - 1; i++){

      thread_status[i].queue  = (blas_queue_t *)NULL;
      thread_status[i].status = THREAD_STATUS_WAKEUP;

      pthread_mutex_init(&thread_status[i].lock, NULL);
      pthread_cond_init (&thread_status[i].wakeup, NULL);

#ifdef NEED_STACKATTR
      ret=pthread_create(&blas_threads[i], &attr,
		     &blas_thread_server, (void *)i);
#else
      ret=pthread_create(&blas_threads[i], NULL,
		     &blas_thread_server, (void *)i);
#endif
      if(ret!=0){
	struct rlimit rlim;
        const char *msg = strerror(ret);
        fprintf(STDERR, "OpenBLAS blas_thread_init: pthread_create failed for thread %ld of %ld: %s\n", i+1,blas_num_threads,msg);
#ifdef RLIMIT_NPROC
        if(0 == getrlimit(RLIMIT_NPROC, &rlim)) {
          fprintf(STDERR, "OpenBLAS blas_thread_init: RLIMIT_NPROC "
                  "%ld current, %ld max\n", (long)(rlim.rlim_cur), (long)(rlim.rlim_max));
        }
#endif
        if(0 != raise(SIGINT)) {
          fprintf(STDERR, "OpenBLAS blas_thread_init: calling exit(3)\n");
          exit(EXIT_FAILURE);
        }
      }
    }

#ifdef MONITOR
    pthread_create(&monitor_thread, NULL,
		     (void *)&blas_monitor, (void *)NULL);
#endif

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

static BLASULONG exec_queue_lock = 0;

int exec_blas_async(BLASLONG pos, blas_queue_t *queue){

#ifdef SMP_SERVER
  // Handle lazy re-init of the thread-pool after a POSIX fork
  if (unlikely(blas_server_avail == 0)) blas_thread_init();
#endif
  BLASLONG i = 0;
  blas_queue_t *current = queue;
  blas_queue_t *tsiq,*tspq;
#if defined(OS_LINUX) && !defined(NO_AFFINITY) && !defined(PARAMTEST)
  int node  = get_node();
  int nodes = get_num_nodes();
#endif

#ifdef SMP_DEBUG
  int exec_count = 0;
  fprintf(STDERR, "Exec_blas_async is called. Position = %d\n", pos);
#endif

  blas_lock(&exec_queue_lock);

    while (queue) {
      queue -> position  = pos;

#ifdef CONSISTENT_FPCSR
      __asm__ __volatile__ ("fnstcw %0"  : "=m" (queue -> x87_mode));
      __asm__ __volatile__ ("stmxcsr %0" : "=m" (queue -> sse_mode));
#endif

#if defined(OS_LINUX) && !defined(NO_AFFINITY) && !defined(PARAMTEST)

      /* Node Mapping Mode */

      if (queue -> mode & BLAS_NODE) {

	do {
	  while((thread_status[i].node != node || thread_status[i].queue) && (i < blas_num_threads - 1)) i ++;

	  if (i < blas_num_threads - 1) break;

	  i ++;
	  if (i >= blas_num_threads - 1) {
	    i = 0;
	    node ++;
	    if (node >= nodes) node = 0;
	  }

	} while (1);

      } else {
	pthread_mutex_lock (&thread_status[i].lock);
	tsiq = thread_status[i].queue;
	pthread_mutex_unlock (&thread_status[i].lock);      
	while(tsiq) {
	  i ++;
	  if (i >= blas_num_threads - 1) i = 0;
	  pthread_mutex_lock (&thread_status[i].lock);
	  tsiq = thread_status[i].queue;
	  pthread_mutex_unlock (&thread_status[i].lock);
	}
      }
#else
      pthread_mutex_lock  (&thread_status[i].lock);
      tsiq=thread_status[i].queue ;
      pthread_mutex_unlock  (&thread_status[i].lock);
      while(tsiq) {
	i ++;
	if (i >= blas_num_threads - 1) i = 0;
        pthread_mutex_lock  (&thread_status[i].lock);
        tsiq=thread_status[i].queue ;
        pthread_mutex_unlock  (&thread_status[i].lock);
      }
#endif

      queue -> assigned = i;
      WMB;
      pthread_mutex_lock  (&thread_status[i].lock);
      thread_status[i].queue = queue;
      pthread_mutex_unlock  (&thread_status[i].lock);
      WMB;

      queue = queue -> next;
      pos ++;
#ifdef SMP_DEBUG
      exec_count ++;
#endif

    }

    blas_unlock(&exec_queue_lock);

#ifdef SMP_DEBUG
    fprintf(STDERR, "Done(Number of threads = %2ld).\n", exec_count);
#endif

    while (current) {

      pos = current -> assigned;

      pthread_mutex_lock  (&thread_status[pos].lock);
      tspq=thread_status[pos].queue;
      pthread_mutex_unlock  (&thread_status[pos].lock);

      if ((BLASULONG)tspq > 1) {
	pthread_mutex_lock  (&thread_status[pos].lock);

	if (thread_status[pos].status == THREAD_STATUS_SLEEP) {


#ifdef MONITOR
	  num_suspend ++;
#endif

	  if (thread_status[pos].status == THREAD_STATUS_SLEEP) {
	    thread_status[pos].status = THREAD_STATUS_WAKEUP;
	    pthread_cond_signal(&thread_status[pos].wakeup);
	  }

	}
	  pthread_mutex_unlock(&thread_status[pos].lock);
      }

      current = current -> next;
    }

  return 0;
}

int exec_blas_async_wait(BLASLONG num, blas_queue_t *queue){
  blas_queue_t * tsqq;

    while ((num > 0) && queue) {

      pthread_mutex_lock(&thread_status[queue->assigned].lock);
      tsqq=thread_status[queue -> assigned].queue;
      pthread_mutex_unlock(&thread_status[queue->assigned].lock);


      while(tsqq) {
	YIELDING;
        pthread_mutex_lock(&thread_status[queue->assigned].lock);
        tsqq=thread_status[queue -> assigned].queue;
        pthread_mutex_unlock(&thread_status[queue->assigned].lock);


      };

      queue = queue -> next;
      num --;
    }

#ifdef SMP_DEBUG
  fprintf(STDERR, "Done.\n\n");
#endif

  return 0;
}

/* Execute Threads */
int exec_blas(BLASLONG num, blas_queue_t *queue){

#ifdef SMP_SERVER
  // Handle lazy re-init of the thread-pool after a POSIX fork
  if (unlikely(blas_server_avail == 0)) blas_thread_init();
#endif
  int (*routine)(blas_arg_t *, void *, void *, double *, double *, BLASLONG);

#ifdef TIMING_DEBUG
  BLASULONG start, stop;
#endif

  if ((num <= 0) || (queue == NULL)) return 0;

#ifdef SMP_DEBUG
  fprintf(STDERR, "Exec_blas is called. Number of executing threads : %ld\n", num);
#endif

#ifdef __ELF__
  if (omp_in_parallel && (num > 1)) {
    if (omp_in_parallel() > 0) {
      fprintf(stderr,
	      "OpenBLAS Warning : Detect OpenMP Loop and this application may hang. "
	      "Please rebuild the library with USE_OPENMP=1 option.\n");
    }
  }
#endif

  if ((num > 1) && queue -> next) exec_blas_async(1, queue -> next);

#ifdef TIMING_DEBUG
  start = rpcc();

  fprintf(STDERR, "\n");
#endif

  routine = queue -> routine;

  if (queue -> mode & BLAS_LEGACY) {
    legacy_exec(routine, queue -> mode, queue -> args, queue -> sb);
  } else
    if (queue -> mode & BLAS_PTHREAD) {
      void (*pthreadcompat)(void *) = queue -> routine;
      (pthreadcompat)(queue -> args);
    } else
      (routine)(queue -> args, queue -> range_m, queue -> range_n,
		queue -> sa, queue -> sb, 0);

#ifdef TIMING_DEBUG
  stop = rpcc();
#endif

  if ((num > 1) && queue -> next) {
    exec_blas_async_wait(num - 1, queue -> next);

    // arm: make sure results from other threads are visible
    MB;
  }

#ifdef TIMING_DEBUG
  fprintf(STDERR, "Thread[0] : %16lu %16lu (%8lu cycles)\n",
	  start, stop,
	  stop - start);
#endif

  return 0;
}

void goto_set_num_threads(int num_threads) {

  long i;

#ifdef SMP_SERVER
  // Handle lazy re-init of the thread-pool after a POSIX fork
  if (unlikely(blas_server_avail == 0)) blas_thread_init();
#endif

  if (num_threads < 1) num_threads = blas_num_threads;

#ifndef NO_AFFINITY
  if (num_threads == 1) {
    if (blas_cpu_number == 1){
      //OpenBLAS is already single thread.
      return;
    }else{
      //From multi-threads to single thread
      //Restore the original affinity mask
      gotoblas_set_affinity(-1);
    }
  }
#endif

  if (num_threads > MAX_CPU_NUMBER) num_threads = MAX_CPU_NUMBER;

  if (num_threads > blas_num_threads) {

    LOCK_COMMAND(&server_lock);

    increased_threads = 1;

    for(i = blas_num_threads - 1; i < num_threads - 1; i++){

      thread_status[i].queue  = (blas_queue_t *)NULL;
      thread_status[i].status = THREAD_STATUS_WAKEUP;

      pthread_mutex_init(&thread_status[i].lock, NULL);
      pthread_cond_init (&thread_status[i].wakeup, NULL);

#ifdef NEED_STACKATTR
      pthread_create(&blas_threads[i], &attr,
		     &blas_thread_server, (void *)i);
#else
      pthread_create(&blas_threads[i], NULL,
		     &blas_thread_server, (void *)i);
#endif
    }

    blas_num_threads = num_threads;

    UNLOCK_COMMAND(&server_lock);
  }

#ifndef NO_AFFINITY
  if(blas_cpu_number == 1 && num_threads > 1){
    //Restore the thread 0 affinity.
    gotoblas_set_affinity(0);
  }
#endif

  blas_cpu_number  = num_threads;

#if defined(ARCH_MIPS64)
  //set parameters for different number of threads.
  blas_set_parameter();
#endif

}

void openblas_set_num_threads(int num_threads) {
	goto_set_num_threads(num_threads);

}

/* Compatible function with pthread_create / join */

int gotoblas_pthread(int numthreads, void *function, void *args, int stride) {

  blas_queue_t queue[MAX_CPU_NUMBER];
  int i;

  if (numthreads <= 0) return 0;

#ifdef SMP
  if (blas_cpu_number == 0) blas_get_cpu_number();
#ifdef SMP_SERVER
  if (blas_server_avail == 0) blas_thread_init();
#endif
#endif

  for (i = 0; i < numthreads; i ++) {

    queue[i].mode    = BLAS_PTHREAD;
    queue[i].routine = function;
    queue[i].args    = args;
    queue[i].range_m = NULL;
    queue[i].range_n = NULL;
    queue[i].sa	     = args;
    queue[i].sb	     = args;
    queue[i].next    = &queue[i + 1];

    args += stride;
  }

  queue[numthreads - 1].next = NULL;

  exec_blas(numthreads, queue);

  return 0;
}

/* Shutdown procedure, but user don't have to call this routine. The */
/* kernel automatically kill threads.                                */

int BLASFUNC(blas_thread_shutdown)(void){

  int i;

  if (!blas_server_avail) return 0;

  LOCK_COMMAND(&server_lock);

  for (i = 0; i < blas_num_threads - 1; i++) {

    pthread_mutex_lock (&thread_status[i].lock);

    thread_status[i].queue = (blas_queue_t *)-1;

    thread_status[i].status = THREAD_STATUS_WAKEUP;

    pthread_cond_signal (&thread_status[i].wakeup);

    pthread_mutex_unlock(&thread_status[i].lock);

  }

  for(i = 0; i < blas_num_threads - 1; i++){
    pthread_join(blas_threads[i], NULL);
  }

  for(i = 0; i < blas_num_threads - 1; i++){
    pthread_mutex_destroy(&thread_status[i].lock);
    pthread_cond_destroy (&thread_status[i].wakeup);
  }

#ifdef NEED_STACKATTR
  pthread_attr_destory(&attr);
#endif

  blas_server_avail = 0;

  UNLOCK_COMMAND(&server_lock);

  return 0;
}

#endif

