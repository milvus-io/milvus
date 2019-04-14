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

//#undef  DEBUG

#include "common.h"

#if defined(USE_TLS) && defined(SMP)
#define COMPILE_TLS

#if USE_TLS != 1
#undef COMPILE_TLS
#endif

#if defined(__GLIBC_PREREQ) 
#if !__GLIBC_PREREQ(2,20)
#undef COMPILE_TLS
#endif
#endif
#endif

#if defined(COMPILE_TLS)

#include <errno.h>

#if defined(OS_WINDOWS) && !defined(OS_CYGWIN_NT)
#define ALLOC_WINDOWS
#ifndef MEM_LARGE_PAGES
#define MEM_LARGE_PAGES  0x20000000
#endif
#else
#define ALLOC_MMAP
#define ALLOC_MALLOC
#endif

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

#if !defined(OS_WINDOWS) || defined(OS_CYGWIN_NT)
#include <sys/mman.h>
#ifndef NO_SYSV_IPC
#include <sys/shm.h>
#endif
#include <sys/ipc.h>
#endif

#include <sys/types.h>

#ifdef OS_LINUX
#include <sys/sysinfo.h>
#include <sched.h>
#include <errno.h>
#include <linux/unistd.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif

#ifdef OS_HAIKU
#include <unistd.h>
#endif

#if defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN)
#include <sys/sysctl.h>
#include <sys/resource.h>
#endif

#if defined(OS_WINDOWS) && (defined(__MINGW32__) || defined(__MINGW64__))
#include <conio.h>
#undef  printf
#define printf	_cprintf
#endif

#ifdef OS_LINUX

#ifndef MPOL_PREFERRED
#define MPOL_PREFERRED  1
#endif

#endif

#if (defined(PPC440) || !defined(OS_LINUX) || defined(HPL)) && !defined(NO_WARMUP)
#define NO_WARMUP
#endif

#ifndef SHM_HUGETLB
#define SHM_HUGETLB 04000
#endif

#ifndef FIXED_PAGESIZE
#define FIXED_PAGESIZE 4096
#endif

#define BITMASK(a, b, c) ((((a) >> (b)) & (c)))

#if defined(_MSC_VER) && !defined(__clang__)
#define CONSTRUCTOR __cdecl
#define DESTRUCTOR __cdecl
#elif (defined(OS_DARWIN) || defined(OS_SUNOS)) && defined(C_GCC)
#define CONSTRUCTOR	__attribute__ ((constructor))
#define DESTRUCTOR	__attribute__ ((destructor))
#elif __GNUC__ && INIT_PRIORITY && ((GCC_VERSION >= 40300) || (CLANG_VERSION >= 20900))
#define CONSTRUCTOR	__attribute__ ((constructor(101)))
#define DESTRUCTOR	__attribute__ ((destructor(101)))
#else
#define CONSTRUCTOR	__attribute__ ((constructor))
#define DESTRUCTOR	__attribute__ ((destructor))
#endif

#ifdef DYNAMIC_ARCH
gotoblas_t *gotoblas = NULL;
#endif
extern void openblas_warning(int verbose, const char * msg);

#ifndef SMP

#define blas_cpu_number 1
#define blas_num_threads 1

/* Dummy Function */
int  goto_get_num_procs  (void) { return 1;};
void goto_set_num_threads(int num_threads) {};

#else

#if defined(OS_LINUX) || defined(OS_SUNOS) || defined(OS_NETBSD)
#ifndef NO_AFFINITY
int get_num_procs(void);
#else
int get_num_procs(void) {
  static int nums = 0;
cpu_set_t *cpusetp;
size_t size;
int ret;
int i,n;

  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
#if !defined(OS_LINUX)
     return nums;
#endif

#if !defined(__GLIBC_PREREQ)
   return nums;
#else
 #if !__GLIBC_PREREQ(2, 3)
   return nums;
 #endif

 #if !__GLIBC_PREREQ(2, 7)
  ret = sched_getaffinity(0,sizeof(cpu_set_t), cpusetp);
  if (ret!=0) return nums;
  n=0;
  #if !__GLIBC_PREREQ(2, 6)
  for (i=0;i<nums;i++)
     if (CPU_ISSET(i,cpusetp)) n++;
  nums=n;
  #else
  nums = CPU_COUNT(sizeof(cpu_set_t),cpusetp);
  #endif
  return nums;
 #else
  cpusetp = CPU_ALLOC(nums);
  if (cpusetp == NULL) return nums;
  size = CPU_ALLOC_SIZE(nums);
  ret = sched_getaffinity(0,size,cpusetp);
  if (ret!=0) return nums;
  ret = CPU_COUNT_S(size,cpusetp);
  if (ret > 0 && ret < nums) nums = ret;
  CPU_FREE(cpusetp);
  return nums;
 #endif
#endif
}
#endif
#endif

#ifdef OS_ANDROID
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_HAIKU
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_AIX
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif



#ifdef OS_WINDOWS

int get_num_procs(void) {

  static int nums = 0;

  if (nums == 0) {

    SYSTEM_INFO sysinfo;

    GetSystemInfo(&sysinfo);

    nums = sysinfo.dwNumberOfProcessors;
  }

  return nums;
}

#endif

#if defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLY)

int get_num_procs(void) {

  static int nums = 0;

  int m[2];
  size_t len;

  if (nums == 0) {
    m[0] = CTL_HW;
    m[1] = HW_NCPU;
    len = sizeof(int);
    sysctl(m, 2, &nums, &len, NULL, 0);
  }

  return nums;
}

#endif

#if defined(OS_DARWIN)
int get_num_procs(void) {
  static int nums = 0;
  size_t len;
  if (nums == 0){
    len = sizeof(int);
    sysctlbyname("hw.physicalcpu", &nums, &len, NULL, 0);
  }
  return nums;
}
/*
void set_stack_limit(int limitMB){
  int result=0;
  struct rlimit rl;
  rlim_t StackSize;

  StackSize=limitMB*1024*1024;
  result=getrlimit(RLIMIT_STACK, &rl);
  if(result==0){
    if(rl.rlim_cur < StackSize){
      rl.rlim_cur=StackSize;
      result=setrlimit(RLIMIT_STACK, &rl);
      if(result !=0){
        fprintf(stderr, "OpenBLAS: set stack limit error =%d\n", result);
      }
    }
  }
}
*/
#endif


/*
OpenBLAS uses the numbers of CPU cores in multithreading.
It can be set by openblas_set_num_threads(int num_threads);
*/
int blas_cpu_number  = 0;
/*
The numbers of threads in the thread pool.
This value is equal or large than blas_cpu_number. This means some threads are sleep.
*/
int blas_num_threads = 0;

int  goto_get_num_procs  (void) {
  return blas_cpu_number;
}

static void blas_memory_init();

void openblas_fork_handler()
{
  // This handler shuts down the OpenBLAS-managed PTHREAD pool when OpenBLAS is
  // built with "make USE_OPENMP=0".
  // Hanging can still happen when OpenBLAS is built against the libgomp
  // implementation of OpenMP. The problem is tracked at:
  //   http://gcc.gnu.org/bugzilla/show_bug.cgi?id=60035
  // In the mean time build with USE_OPENMP=0 or link against another
  // implementation of OpenMP.
#if !((defined(OS_WINDOWS) && !defined(OS_CYGWIN_NT)) || defined(OS_ANDROID)) && defined(SMP_SERVER)
  int err;
  err = pthread_atfork ((void (*)(void)) BLASFUNC(blas_thread_shutdown), NULL, blas_memory_init);
  if(err != 0)
    openblas_warning(0, "OpenBLAS Warning ... cannot install fork handler. You may meet hang after fork.\n");
#endif
}

extern int openblas_num_threads_env();
extern int openblas_goto_num_threads_env();
extern int openblas_omp_num_threads_env();

int blas_get_cpu_number(void){
#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN) || defined(OS_ANDROID)
  int max_num;
#endif
  int blas_goto_num   = 0;
  int blas_omp_num    = 0;

  if (blas_num_threads) return blas_num_threads;

#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN) || defined(OS_ANDROID)
  max_num = get_num_procs();
#endif

  // blas_goto_num = 0;
#ifndef USE_OPENMP_UNUSED
  blas_goto_num=openblas_num_threads_env();
  if (blas_goto_num < 0) blas_goto_num = 0;

  if (blas_goto_num == 0) {
    blas_goto_num=openblas_goto_num_threads_env();
    if (blas_goto_num < 0) blas_goto_num = 0;
  }

#endif

  // blas_omp_num = 0;
  blas_omp_num=openblas_omp_num_threads_env();
  if (blas_omp_num < 0) blas_omp_num = 0;

  if (blas_goto_num > 0) blas_num_threads = blas_goto_num;
  else if (blas_omp_num > 0) blas_num_threads = blas_omp_num;
  else blas_num_threads = MAX_CPU_NUMBER;

#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLY) || defined(OS_DARWIN) || defined(OS_ANDROID)
  if (blas_num_threads > max_num) blas_num_threads = max_num;
#endif

  if (blas_num_threads > MAX_CPU_NUMBER) blas_num_threads = MAX_CPU_NUMBER;

#ifdef DEBUG
  printf( "Adjusted number of threads : %3d\n", blas_num_threads);
#endif

  blas_cpu_number = blas_num_threads;

  return blas_num_threads;
}
#endif


int openblas_get_num_procs(void) {
#ifndef SMP
  return 1;
#else
  return get_num_procs();
#endif
}

int openblas_get_num_threads(void) {
#ifndef SMP
  return 1;
#else
  // init blas_cpu_number if needed
  blas_get_cpu_number();
  return blas_cpu_number;
#endif
}

int hugetlb_allocated = 0;

#if defined(OS_WINDOWS)
#define LIKELY_ONE(x) (x)
#else
#define LIKELY_ONE(x) (__builtin_expect(x, 1))
#endif

/* Stores information about the allocation and how to release it */
struct alloc_t {
  /* Whether this allocation is being used */
  int used;
  /* Any special attributes needed when releasing this allocation */
  int attr;
  /* Function that can properly release this memory */
  void (*release_func)(struct alloc_t *);
  /* Pad to 64-byte alignment */
  char pad[64 - 2 * sizeof(int) - sizeof(void(*))];
};

/* Convenience macros for storing release funcs */
#define STORE_RELEASE_FUNC(address, func)                   \
  if (address != (void *)-1) {                              \
    struct alloc_t *alloc_info = (struct alloc_t *)address; \
    alloc_info->release_func = func;                        \
  }

#define STORE_RELEASE_FUNC_WITH_ATTR(address, func, attr)   \
  if (address != (void *)-1) {                              \
    struct alloc_t *alloc_info = (struct alloc_t *)address; \
    alloc_info->release_func = func;                        \
    alloc_info->attr = attr;                                \
  }

/* The number of bytes that will be allocated for each buffer. When allocating
   memory, we store an alloc_t followed by the actual buffer memory. This means
   that each allocation always has its associated alloc_t, without the need
   for an auxiliary tracking structure. */
static const int allocation_block_size = BUFFER_SIZE + sizeof(struct alloc_t);

#if defined(SMP)
#  if defined(OS_WINDOWS)
static DWORD local_storage_key = 0;
DWORD lsk;

#  else
static pthread_key_t local_storage_key = 0;
pthread_key_t lsk;
#  endif /* defined(OS_WINDOWS) */
#endif /* defined(SMP) */

#if defined(OS_LINUX) && !defined(NO_WARMUP)
static int hot_alloc = 0;
#endif

/* Global lock for memory allocation */

#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    alloc_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t alloc_lock = 0;
#else
static BLASULONG  alloc_lock = 0UL;
#endif

#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    key_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t key_lock = 0;
#else
static BLASULONG  key_lock = 0UL;
#endif

/* Returns a pointer to the start of the per-thread memory allocation data */
static __inline struct alloc_t ** get_memory_table() {
#if defined(SMP)
LOCK_COMMAND(&key_lock);
lsk=local_storage_key;
UNLOCK_COMMAND(&key_lock);
  if (!lsk) {
    blas_memory_init();
  }
#  if defined(OS_WINDOWS)
  struct alloc_t ** local_memory_table = (struct alloc_t **)TlsGetValue(local_storage_key);
#  else
  struct alloc_t ** local_memory_table = (struct alloc_t **)pthread_getspecific(local_storage_key);
#  endif /* defined(OS_WINDOWS) */
#else
  static struct alloc_t ** local_memory_table = NULL;
#endif /* defined(SMP) */
#if defined (SMP)
LOCK_COMMAND(&key_lock);
lsk=local_storage_key;
UNLOCK_COMMAND(&key_lock);
  if (lsk && !local_memory_table) {
#else
 if (!local_memory_table) {
#endif /* defined(SMP) */
    local_memory_table = (struct alloc_t **)malloc(sizeof(struct alloc_t *) * NUM_BUFFERS);
    memset(local_memory_table, 0, sizeof(struct alloc_t *) * NUM_BUFFERS);
#if defined(SMP)
#  if defined(OS_WINDOWS)
LOCK_COMMAND(&key_lock);
    TlsSetValue(local_storage_key, (void*)local_memory_table);
UNLOCK_COMMAND(&key_lock);
#  else
LOCK_COMMAND(&key_lock);
    pthread_setspecific(local_storage_key, (void*)local_memory_table);
UNLOCK_COMMAND(&key_lock);
#  endif /* defined(OS_WINDOWS) */
#endif /* defined(SMP) */
  }
  return local_memory_table;
}

#ifdef ALLOC_MMAP

static void alloc_mmap_free(struct alloc_t *alloc_info){

  if (munmap(alloc_info, allocation_block_size)) {
    printf("OpenBLAS : munmap failed\n");
  }
}



#ifdef NO_WARMUP

static void *alloc_mmap(void *address){
  void *map_address;

  if (address){
    map_address = mmap(address,
		       allocation_block_size,
		       MMAP_ACCESS, MMAP_POLICY | MAP_FIXED, -1, 0);
  } else {
    map_address = mmap(address,
		       allocation_block_size,
		       MMAP_ACCESS, MMAP_POLICY, -1, 0);
  }

  STORE_RELEASE_FUNC(map_address, alloc_mmap_free);

#ifdef OS_LINUX
  my_mbind(map_address, allocation_block_size, MPOL_PREFERRED, NULL, 0, 0);
#endif

  return map_address;
}

#else

#define BENCH_ITERATION 4
#define SCALING		2

static inline BLASULONG run_bench(BLASULONG address, BLASULONG size) {

  BLASULONG original, *p;
  BLASULONG start, stop, min;
  int iter, i, count;

  min = (BLASULONG)-1;

  original = *(BLASULONG *)(address + size - PAGESIZE);

  *(BLASULONG *)(address + size - PAGESIZE) = (BLASULONG)address;

  for (iter = 0; iter < BENCH_ITERATION; iter ++ ) {

    p = (BLASULONG *)address;

    count = size / PAGESIZE;

    start = rpcc();

    for (i = 0; i < count; i ++) {
      p = (BLASULONG *)(*p);
    }

    stop = rpcc();

    if (min > stop - start) min = stop - start;
  }

  *(BLASULONG *)(address + size - PAGESIZE +  0) = original;
  *(BLASULONG *)(address + size - PAGESIZE +  8) = (BLASULONG)p;

  return min;
}

static void *alloc_mmap(void *address){
  void *map_address, *best_address;
  BLASULONG best, start, current, original;
  BLASULONG allocsize;

  if (address){
    /* Just give up use advanced operation */
    map_address = mmap(address, allocation_block_size, MMAP_ACCESS, MMAP_POLICY | MAP_FIXED, -1, 0);

#ifdef OS_LINUX
    my_mbind(map_address, allocation_block_size, MPOL_PREFERRED, NULL, 0, 0);
#endif

  } else {
#if defined(OS_LINUX) && !defined(NO_WARMUP)
    if (hot_alloc == 0) {
      map_address = mmap(NULL, allocation_block_size, MMAP_ACCESS, MMAP_POLICY, -1, 0);

#ifdef OS_LINUX
      my_mbind(map_address, allocation_block_size, MPOL_PREFERRED, NULL, 0, 0);
#endif

    } else {
#endif

      map_address = mmap(NULL, allocation_block_size * SCALING,
			 MMAP_ACCESS, MMAP_POLICY, -1, 0);

      if (map_address != (void *)-1) {

#ifdef OS_LINUX
#ifdef DEBUG
		  int ret=0;
		  ret=my_mbind(map_address, allocation_block_size * SCALING, MPOL_PREFERRED, NULL, 0, 0);
		  if(ret==-1){
			  int errsv=errno;
			  perror("OpenBLAS alloc_mmap:");
			  printf("error code=%d,\tmap_address=%lx\n",errsv,map_address);
		  }

#else
		  my_mbind(map_address, allocation_block_size * SCALING, MPOL_PREFERRED, NULL, 0, 0);
#endif
#endif


	allocsize = DGEMM_P * DGEMM_Q * sizeof(double);

	start   = (BLASULONG)map_address;
	current = (SCALING - 1) * allocation_block_size;
	original = current;

	while(current > 0 && current <= original) {
	  *(BLASLONG *)start = (BLASLONG)start + PAGESIZE;
	  start += PAGESIZE;
	  current -= PAGESIZE;
	}

	*(BLASLONG *)(start - PAGESIZE) = (BLASULONG)map_address;

	start = (BLASULONG)map_address;

	best = (BLASULONG)-1;
	best_address = map_address;

	while ((start + allocsize  < (BLASULONG)map_address + (SCALING - 1) * allocation_block_size)) {

	  current = run_bench(start, allocsize);

	  if (best > current) {
	    best = current;
	    best_address = (void *)start;
	  }

	  start += PAGESIZE;

	}

      if ((BLASULONG)best_address > (BLASULONG)map_address)
	munmap(map_address,  (BLASULONG)best_address - (BLASULONG)map_address);

      munmap((void *)((BLASULONG)best_address + allocation_block_size), (SCALING - 1) * allocation_block_size + (BLASULONG)map_address - (BLASULONG)best_address);

      map_address = best_address;

#if defined(OS_LINUX) && !defined(NO_WARMUP)
      hot_alloc = 2;
#endif
      }
    }
#if defined(OS_LINUX) && !defined(NO_WARMUP)
  }
#endif

  STORE_RELEASE_FUNC(map_address, alloc_mmap_free);

  return map_address;
}

#endif

#endif


#ifdef ALLOC_MALLOC

static void alloc_malloc_free(struct alloc_t *alloc_info){

  free(alloc_info);

}

static void *alloc_malloc(void *address){

  void *map_address;

  map_address = (void *)malloc(allocation_block_size + FIXED_PAGESIZE);

  if (map_address == (void *)NULL) map_address = (void *)-1;

  STORE_RELEASE_FUNC(map_address, alloc_malloc_free);

  return map_address;

}

#endif

#ifdef ALLOC_QALLOC

void *qalloc(int flags, size_t bytes);
void *qfree (void *address);

#define QNONCACHE 0x1
#define QCOMMS    0x2
#define QFAST     0x4

static void alloc_qalloc_free(struct alloc_t *alloc_info){

  qfree(alloc_info);

}

static void *alloc_qalloc(void *address){
  void *map_address;

  map_address = (void *)qalloc(QCOMMS | QFAST, allocation_block_size + FIXED_PAGESIZE);

  if (map_address == (void *)NULL) map_address = (void *)-1;

  STORE_RELEASE_FUNC(map_address, alloc_qalloc_free);

  return (void *)(((BLASULONG)map_address + FIXED_PAGESIZE - 1) & ~(FIXED_PAGESIZE - 1));
}

#endif

#ifdef ALLOC_WINDOWS

static void alloc_windows_free(struct alloc_t *alloc_info){

  VirtualFree(alloc_info, allocation_block_size, MEM_DECOMMIT);

}

static void *alloc_windows(void *address){
  void *map_address;

  map_address  = VirtualAlloc(address,
			      allocation_block_size,
			      MEM_RESERVE | MEM_COMMIT,
			      PAGE_READWRITE);

  if (map_address == (void *)NULL) map_address = (void *)-1;

  STORE_RELEASE_FUNC(map_address, alloc_windows_free);

  return map_address;
}

#endif

#ifdef ALLOC_DEVICEDRIVER
#ifndef DEVICEDRIVER_NAME
#define DEVICEDRIVER_NAME "/dev/mapper"
#endif

static void alloc_devicedirver_free(struct alloc_t *alloc_info){

  int attr = alloc_info -> attr;
  if (munmap(address, allocation_block_size)) {
    printf("OpenBLAS : Bugphysarea unmap failed.\n");
  }

  if (close(attr)) {
    printf("OpenBLAS : Bugphysarea close failed.\n");
  }

}

static void *alloc_devicedirver(void *address){

  int fd;
  void *map_address;

  if ((fd = open(DEVICEDRIVER_NAME, O_RDWR | O_SYNC)) < 0) {

    return (void *)-1;

  }

  map_address = mmap(address, allocation_block_size,
		     PROT_READ | PROT_WRITE,
		     MAP_FILE | MAP_SHARED,
		     fd, 0);

  STORE_RELEASE_FUNC_WITH_ATTR(map_address, alloc_devicedirver_free, fd);

  return map_address;
}

#endif

#ifdef ALLOC_SHM

static void alloc_shm_free(struct alloc_t *alloc_info){

  if (shmdt(alloc_info)) {
    printf("OpenBLAS : Shared memory unmap failed.\n");
    }
}

static void *alloc_shm(void *address){
  void *map_address;
  int shmid;

  shmid = shmget(IPC_PRIVATE, allocation_block_size,IPC_CREAT | 0600);

  map_address = (void *)shmat(shmid, address, 0);

  if (map_address != (void *)-1){

#ifdef OS_LINUX
    my_mbind(map_address, allocation_block_size, MPOL_PREFERRED, NULL, 0, 0);
#endif

    shmctl(shmid, IPC_RMID, 0);

    struct alloc_t *alloc_info = (struct alloc_t *)map_address;
    alloc_info->release_func = alloc_shm_free;
    alloc_info->attr = shmid;
  }

  return map_address;
}

#if defined OS_LINUX  || defined OS_AIX  || defined __sun__  || defined OS_WINDOWS

static void alloc_hugetlb_free(struct alloc_t *alloc_info){

#if defined(OS_LINUX) || defined(OS_AIX)
  if (shmdt(alloc_info)) {
    printf("OpenBLAS : Hugepage unmap failed.\n");
  }
#endif

#ifdef __sun__

  munmap(alloc_info, allocation_block_size);

#endif

#ifdef OS_WINDOWS

  VirtualFree(alloc_info, allocation_block_size, MEM_LARGE_PAGES | MEM_DECOMMIT);

#endif

}

static void *alloc_hugetlb(void *address){

  void *map_address = (void *)-1;

#if defined(OS_LINUX) || defined(OS_AIX)
  int shmid;

  shmid = shmget(IPC_PRIVATE, allocation_block_size,
#ifdef OS_LINUX
		 SHM_HUGETLB |
#endif
#ifdef OS_AIX
		 SHM_LGPAGE | SHM_PIN |
#endif
		 IPC_CREAT | SHM_R | SHM_W);

  if (shmid != -1) {
    map_address = (void *)shmat(shmid, address, SHM_RND);

#ifdef OS_LINUX
    my_mbind(map_address, allocation_block_size, MPOL_PREFERRED, NULL, 0, 0);
#endif

    if (map_address != (void *)-1){
      shmctl(shmid, IPC_RMID, 0);
    }
  }
#endif

#ifdef __sun__
  struct memcntl_mha mha;

  mha.mha_cmd = MHA_MAPSIZE_BSSBRK;
  mha.mha_flags = 0;
  mha.mha_pagesize = HUGE_PAGESIZE;
  memcntl(NULL, 0, MC_HAT_ADVISE, (char *)&mha, 0, 0);

  map_address = (BLASULONG)memalign(HUGE_PAGESIZE, allocation_block_size);
#endif

#ifdef OS_WINDOWS

  HANDLE hToken;
  TOKEN_PRIVILEGES tp;

  if (OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hToken) != TRUE) return (void *) -1;

  tp.PrivilegeCount = 1;
  tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;

  if (LookupPrivilegeValue(NULL, SE_LOCK_MEMORY_NAME, &tp.Privileges[0].Luid) != TRUE) {
      CloseHandle(hToken);
      return (void*)-1;
  }

  if (AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL) != TRUE) {
      CloseHandle(hToken);
      return (void*)-1;
  }

  map_address  = (void *)VirtualAlloc(address,
				      allocation_block_size,
				      MEM_LARGE_PAGES | MEM_RESERVE | MEM_COMMIT,
				      PAGE_READWRITE);

  tp.Privileges[0].Attributes = 0;
  AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL);

  if (map_address == (void *)NULL) map_address = (void *)-1;

#endif

  STORE_RELEASE_FUNC(map_address, alloc_hugetlb_free);

  return map_address;
}
#endif

#endif

#ifdef  ALLOC_HUGETLBFILE

static int hugetlb_pid = 0;

static void alloc_hugetlbfile_free(struct alloc_t *alloc_info){

  int attr = alloc_info -> attr;
  if (munmap(alloc_info, allocation_block_size)) {
    printf("OpenBLAS : HugeTLBfs unmap failed.\n");
  }

  if (close(attr)) {
    printf("OpenBLAS : HugeTLBfs close failed.\n");
  }
}

static void *alloc_hugetlbfile(void *address){

  void *map_address = (void *)-1;
  int fd;
  char filename[64];

  if (!hugetlb_pid) hugetlb_pid = getpid();

  sprintf(filename, "%s/gotoblas.%d", HUGETLB_FILE_NAME, hugetlb_pid);

  if ((fd = open(filename, O_RDWR | O_CREAT, 0700)) < 0) {
    return (void *)-1;
  }

  unlink(filename);

  map_address = mmap(address, allocation_block_size,
		     PROT_READ | PROT_WRITE,
		     MAP_SHARED,
		     fd, 0);

  STORE_RELEASE_FUNC_WITH_ATTR(map_address, alloc_hugetlbfile_free, fd);

  return map_address;
}
#endif


#ifdef SEEK_ADDRESS
static BLASULONG base_address      = 0UL;
#else
static BLASULONG base_address      = BASE_ADDRESS;
#endif

#if __STDC_VERSION__ >= 201112L
static _Atomic int memory_initialized = 0;
#else
static volatile int memory_initialized = 0;
#endif

/*       Memory allocation routine           */
/* procpos ... indicates where it comes from */
/*                0 : Level 3 functions      */
/*                1 : Level 2 functions      */
/*                2 : Thread                 */

	static void blas_memory_cleanup(void* ptr){
  if (ptr) {
    struct alloc_t ** table = (struct alloc_t **)ptr;
    int pos;
    for (pos = 0; pos < NUM_BUFFERS; pos ++){
      struct alloc_t *alloc_info = table[pos];
      if (alloc_info) {
        alloc_info->release_func(alloc_info);
        table[pos] = (void *)0;
      }
    }
    free(table);
  }
#if defined(OS_WINDOWS)
  TlsFree(local_storage_key);
#else
  pthread_key_delete(local_storage_key);
#endif		
}

static void blas_memory_init(){
#if defined(SMP)
#  if defined(OS_WINDOWS)
  local_storage_key = TlsAlloc();
#  else
  pthread_key_create(&local_storage_key, blas_memory_cleanup);
#  endif /* defined(OS_WINDOWS) */
#endif /* defined(SMP) */
}

void *blas_memory_alloc(int procpos){

  int position;

  void *map_address;

  void *(*memoryalloc[])(void *address) = {
#ifdef ALLOC_DEVICEDRIVER
    alloc_devicedirver,
#endif
/* Hugetlb implicitly assumes ALLOC_SHM */
#ifdef ALLOC_SHM
    alloc_shm,
#endif
#if ((defined ALLOC_SHM) && (defined OS_LINUX  || defined OS_AIX  || defined __sun__  || defined OS_WINDOWS))
    alloc_hugetlb,
#endif
#ifdef ALLOC_MMAP
    alloc_mmap,
#endif
#ifdef ALLOC_QALLOC
    alloc_qalloc,
#endif
#ifdef ALLOC_WINDOWS
    alloc_windows,
#endif
#ifdef ALLOC_MALLOC
    alloc_malloc,
#endif
    NULL,
  };
  void *(**func)(void *address);
  struct alloc_t * alloc_info;
  struct alloc_t ** alloc_table;


#if defined(SMP) && !defined(USE_OPENMP)
int mi;
LOCK_COMMAND(&alloc_lock);
mi=memory_initialized;
UNLOCK_COMMAND(&alloc_lock);
  if (!LIKELY_ONE(mi)) {
#else
  if (!LIKELY_ONE(memory_initialized)) {
#endif
#if defined(SMP) && !defined(USE_OPENMP)
    /* Only allow a single thread to initialize memory system */
    LOCK_COMMAND(&alloc_lock);

    if (!memory_initialized) {
#endif
      blas_memory_init();
#ifdef DYNAMIC_ARCH
      gotoblas_dynamic_init();
#endif

#if defined(SMP) && defined(OS_LINUX) && !defined(NO_AFFINITY)
      gotoblas_affinity_init();
#endif

#ifdef SMP
      if (!blas_num_threads) blas_cpu_number = blas_get_cpu_number();
#endif

#if defined(ARCH_X86) || defined(ARCH_X86_64) || defined(ARCH_IA64) || defined(ARCH_MIPS64) || defined(ARCH_ARM64)
#ifndef DYNAMIC_ARCH
      blas_set_parameter();
#endif
#endif

      memory_initialized = 1;

#if defined(SMP) && !defined(USE_OPENMP)
    }
    UNLOCK_COMMAND(&alloc_lock);
#endif
  }

#ifdef DEBUG
  printf("Alloc Start ...\n");
#endif

  position = 0;
  alloc_table = get_memory_table();
  do {
      if (!alloc_table[position] || !alloc_table[position]->used) goto allocation;
    position ++;

  } while (position < NUM_BUFFERS);

  goto error;

  allocation :

#ifdef DEBUG
  printf("  Position -> %d\n", position);
#endif

  alloc_info = alloc_table[position];
  if (!alloc_info) {
    do {
#ifdef DEBUG
      printf("Allocation Start : %lx\n", base_address);
#endif

      map_address = (void *)-1;

      func = &memoryalloc[0];

      while ((func != NULL) && (map_address == (void *) -1)) {

  map_address = (*func)((void *)base_address);

#ifdef ALLOC_DEVICEDRIVER
	if ((*func ==  alloc_devicedirver) && (map_address == (void *)-1)) {
	    fprintf(stderr, "OpenBLAS Warning ... Physically contiguous allocation failed.\n");
	}
#endif

#ifdef ALLOC_HUGETLBFILE
	if ((*func == alloc_hugetlbfile) && (map_address == (void *)-1)) {
#ifndef OS_WINDOWS
	    fprintf(stderr, "OpenBLAS Warning ... HugeTLB(File) allocation failed.\n");
#endif
	}
#endif

#if (defined ALLOC_SHM) && (defined OS_LINUX  || defined OS_AIX  || defined __sun__  || defined OS_WINDOWS)
	if ((*func == alloc_hugetlb) && (map_address != (void *)-1)) hugetlb_allocated = 1;
#endif

	func ++;
      }

#ifdef DEBUG
      printf("  Success -> %08lx\n", map_address);
#endif
      if (((BLASLONG) map_address) == -1) base_address = 0UL;

      if (base_address) base_address += allocation_block_size + FIXED_PAGESIZE;

    } while ((BLASLONG)map_address == -1);

    alloc_table[position] = alloc_info = map_address;

#ifdef DEBUG
    printf("  Mapping Succeeded. %p(%d)\n", (void *)alloc_info, position);
#endif
  }

#ifdef DEBUG
  printf("Mapped   : %p  %3d\n\n", (void *)alloc_info, position);
#endif

  alloc_info->used = 1;

  return (void *)(((char *)alloc_info) + sizeof(struct alloc_t));

 error:
  printf("OpenBLAS : Program will terminate because you tried to allocate too many memory regions.\n");

  return NULL;
}

void blas_memory_free(void *buffer){
#ifdef DEBUG
  int position;
  struct alloc_t ** alloc_table;
#endif
  /* Since we passed an offset pointer to the caller, get back to the actual allocation */
  struct alloc_t *alloc_info = (void *)(((char *)buffer) - sizeof(struct alloc_t));

#ifdef DEBUG
  printf("Unmapped Start : %p ...\n", alloc_info);
#endif

  alloc_info->used = 0;

#ifdef DEBUG
  printf("Unmap Succeeded.\n\n");
#endif

  return;

#ifdef DEBUG
  alloc_table = get_memory_table();
  for (position = 0; position < NUM_BUFFERS; position++){
    if (alloc_table[position]) {
      printf("%4ld  %p : %d\n", position, alloc_table[position], alloc_table[position]->used);
    }
  }
#endif
  return;
}

void *blas_memory_alloc_nolock(int unused) {
  void *map_address;
  map_address = (void *)malloc(BUFFER_SIZE + FIXED_PAGESIZE);
  return map_address;
}

void blas_memory_free_nolock(void * map_address) {
  free(map_address);
}

void blas_shutdown(void){
#ifdef SMP
  BLASFUNC(blas_thread_shutdown)();
#endif

#ifdef SMP
  /* Only cleanupIf we were built for threading and TLS was initialized */
  if (local_storage_key)
#endif
    blas_memory_cleanup((void*)get_memory_table());

#ifdef SEEK_ADDRESS
  base_address      = 0UL;
#else
  base_address      = BASE_ADDRESS;
#endif

  return;
}

#if defined(OS_LINUX) && !defined(NO_WARMUP)

#ifdef SMP
#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    init_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t init_lock = 0;
#else
static BLASULONG   init_lock = 0UL;
#endif
#endif

static void _touch_memory(blas_arg_t *arg, BLASLONG *range_m, BLASLONG *range_n,
			  void *sa, void *sb, BLASLONG pos) {

#if !defined(ARCH_POWER) && !defined(ARCH_SPARC)

  size_t size;
  BLASULONG buffer;

  size   = allocation_block_size - PAGESIZE;
  buffer = (BLASULONG)sa + GEMM_OFFSET_A;

#if defined(OS_LINUX) && !defined(NO_WARMUP)
    if (hot_alloc != 2) {
#endif

#ifdef SMP
  LOCK_COMMAND(&init_lock);
#endif

  while (size > 0) {
    *(int *)buffer = size;
    buffer  += PAGESIZE;
    size    -= PAGESIZE;
  }

#ifdef SMP
  UNLOCK_COMMAND(&init_lock);
#endif

  size = MIN((allocation_block_size - PAGESIZE), L2_SIZE);
  buffer = (BLASULONG)sa + GEMM_OFFSET_A;

  while (size > 0) {
    *(int *)buffer = size;
    buffer  += 64;
    size    -= 64;
  }

#if defined(OS_LINUX) && !defined(NO_WARMUP)
    }
#endif

#endif
}

#ifdef SMP

static void _init_thread_memory(void *buffer) {

  blas_queue_t queue[MAX_CPU_NUMBER];
  int num_cpu;

  for (num_cpu = 0; num_cpu < blas_num_threads; num_cpu++) {

    blas_queue_init(&queue[num_cpu]);
    queue[num_cpu].mode    = BLAS_DOUBLE | BLAS_REAL;
    queue[num_cpu].routine = &_touch_memory;
    queue[num_cpu].args    = NULL;
    queue[num_cpu].next    = &queue[num_cpu + 1];
  }

  queue[num_cpu - 1].next = NULL;
  queue[0].sa = buffer;

  exec_blas(num_cpu, queue);

}
#endif

static void gotoblas_memory_init(void) {

  void *buffer;

  hot_alloc = 1;

  buffer = (void *)blas_memory_alloc(0);

#ifdef SMP
  if (blas_cpu_number == 0) blas_get_cpu_number();
#ifdef SMP_SERVER
  if (blas_server_avail == 0) blas_thread_init();
#endif

  _init_thread_memory((void *)((BLASULONG)buffer + GEMM_OFFSET_A));

#else

  _touch_memory(NULL, NULL, NULL, (void *)((BLASULONG)buffer + GEMM_OFFSET_A), NULL, 0);

#endif

  blas_memory_free(buffer);
}
#endif

/* Initialization for all function; this function should be called before main */

static int gotoblas_initialized = 0;
extern void openblas_read_env();

void CONSTRUCTOR gotoblas_init(void) {

  if (gotoblas_initialized) return;

#ifdef SMP
  openblas_fork_handler();
#endif

  openblas_read_env();

#ifdef PROFILE
   moncontrol (0);
#endif

#ifdef DYNAMIC_ARCH
   gotoblas_dynamic_init();
#endif

#if defined(SMP) && defined(OS_LINUX) && !defined(NO_AFFINITY)
   gotoblas_affinity_init();
#endif

#if defined(OS_LINUX) && !defined(NO_WARMUP)
   gotoblas_memory_init();
#endif

//#if defined(OS_LINUX)
#if 0
   struct rlimit curlimit;
   if ( getrlimit(RLIMIT_STACK, &curlimit ) == 0 )
   {
	if ( curlimit.rlim_cur != curlimit.rlim_max )
	{
		curlimit.rlim_cur = curlimit.rlim_max;
		setrlimit(RLIMIT_STACK, &curlimit);
	}
   }
#endif

#ifdef SMP
  if (blas_cpu_number == 0) blas_get_cpu_number();
#ifdef SMP_SERVER
  if (blas_server_avail == 0) blas_thread_init();
#endif
#endif

#ifdef FUNCTION_PROFILE
   gotoblas_profile_init();
#endif

   gotoblas_initialized = 1;

#ifdef PROFILE
   moncontrol (1);
#endif

}

void DESTRUCTOR gotoblas_quit(void) {

  if (gotoblas_initialized == 0) return;

  blas_shutdown();

#ifdef PROFILE
   moncontrol (0);
#endif

#ifdef FUNCTION_PROFILE
   gotoblas_profile_quit();
#endif

#if defined(SMP) && defined(OS_LINUX) && !defined(NO_AFFINITY)
   gotoblas_affinity_quit();
#endif

#ifdef DYNAMIC_ARCH
   gotoblas_dynamic_quit();
#endif

   gotoblas_initialized = 0;

#ifdef PROFILE
   moncontrol (1);
#endif
}

#if defined(_MSC_VER) && !defined(__clang__)
BOOL APIENTRY DllMain(HMODULE hModule, DWORD  ul_reason_for_call, LPVOID lpReserved)
{
  switch (ul_reason_for_call)
  {
    case DLL_PROCESS_ATTACH:
      gotoblas_init();
      break;
    case DLL_THREAD_ATTACH:
      break;
    case DLL_THREAD_DETACH:
#if defined(SMP)
      blas_memory_cleanup((void*)get_memory_table());
#endif
      break;
    case DLL_PROCESS_DETACH:
      gotoblas_quit();
      break;
    default:
      break;
  }
  return TRUE;
}

/*
  This is to allow static linking.
  Code adapted from Google performance tools:
  https://gperftools.googlecode.com/git-history/perftools-1.0/src/windows/port.cc
  Reference:
  https://sourceware.org/ml/pthreads-win32/2008/msg00028.html
  http://ci.boost.org/svn-trac/browser/trunk/libs/thread/src/win32/tss_pe.cpp
*/
static int on_process_term(void)
{
	gotoblas_quit();
	return 0;
}
#ifdef _WIN64
#pragma comment(linker, "/INCLUDE:_tls_used")
#else
#pragma comment(linker, "/INCLUDE:__tls_used")
#endif

#ifdef _WIN64
#pragma const_seg(".CRT$XLB")
#else
#pragma data_seg(".CRT$XLB")
#endif
static void (APIENTRY *dll_callback)(HINSTANCE h, DWORD ul_reason_for_call, PVOID pv) = DllMain;
#ifdef _WIN64
#pragma const_seg()
#else
#pragma data_seg()
#endif

#ifdef _WIN64
#pragma const_seg(".CRT$XTU")
#else
#pragma data_seg(".CRT$XTU")
#endif
static int(*p_process_term)(void) = on_process_term;
#ifdef _WIN64
#pragma const_seg()
#else
#pragma data_seg()
#endif
#endif

#if (defined(C_PGI) || (!defined(C_SUN) && defined(F_INTERFACE_SUN))) && (defined(ARCH_X86) || defined(ARCH_X86_64))
/* Don't call me; this is just work around for PGI / Sun bug */
void gotoblas_dummy_for_PGI(void) {

  gotoblas_init();
  gotoblas_quit();

#if 0
  asm ("\t.section\t.ctors,\"aw\",@progbits; .align 8; .quad gotoblas_init; .section .text");
  asm ("\t.section\t.dtors,\"aw\",@progbits; .align 8; .quad gotoblas_quit; .section .text");
#else
  asm (".section .init,\"ax\"; call gotoblas_init@PLT; .section .text");
  asm (".section .fini,\"ax\"; call gotoblas_quit@PLT; .section .text");
#endif
}
#endif

#else
#include <errno.h>

#ifdef OS_WINDOWS
#define ALLOC_WINDOWS
#ifndef MEM_LARGE_PAGES
#define MEM_LARGE_PAGES  0x20000000
#endif
#else
#define ALLOC_MMAP
#define ALLOC_MALLOC
#endif

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>

#ifndef OS_WINDOWS
#include <sys/mman.h>
#ifndef NO_SYSV_IPC
#include <sys/shm.h>
#endif
#include <sys/ipc.h>
#endif

#include <sys/types.h>

#ifdef OS_LINUX
#include <sys/sysinfo.h>
#include <sched.h>
#include <errno.h>
#include <linux/unistd.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif

#if defined(OS_FREEBSD) || defined(OS_DARWIN)
#include <sys/sysctl.h>
#include <sys/resource.h>
#endif

#if defined(OS_WINDOWS) && (defined(__MINGW32__) || defined(__MINGW64__))
#include <conio.h>
#undef  printf
#define printf	_cprintf
#endif

#ifdef OS_LINUX

#ifndef MPOL_PREFERRED
#define MPOL_PREFERRED  1
#endif

#endif

#if (defined(PPC440) || !defined(OS_LINUX) || defined(HPL)) && !defined(NO_WARMUP)
#define NO_WARMUP
#endif

#ifndef SHM_HUGETLB
#define SHM_HUGETLB 04000
#endif

#ifndef FIXED_PAGESIZE
#define FIXED_PAGESIZE 4096
#endif

#define BITMASK(a, b, c) ((((a) >> (b)) & (c)))

#if defined(_MSC_VER) && !defined(__clang__)
#define CONSTRUCTOR __cdecl
#define DESTRUCTOR __cdecl
#elif (defined(OS_DARWIN) || defined(OS_SUNOS)) && defined(C_GCC)
#define CONSTRUCTOR	__attribute__ ((constructor))
#define DESTRUCTOR	__attribute__ ((destructor))
#else
#define CONSTRUCTOR	__attribute__ ((constructor(101)))
#define DESTRUCTOR	__attribute__ ((destructor(101)))
#endif

#ifdef DYNAMIC_ARCH
gotoblas_t *gotoblas = NULL;
#endif
extern void openblas_warning(int verbose, const char * msg);

#ifndef SMP

#define blas_cpu_number 1
#define blas_num_threads 1

/* Dummy Function */
int  goto_get_num_procs  (void) { return 1;};
void goto_set_num_threads(int num_threads) {};

#else

#if defined(OS_LINUX) || defined(OS_SUNOS) || defined(OS_NETBSD)
#ifndef NO_AFFINITY
int get_num_procs(void);
#else
int get_num_procs(void) {
  static int nums = 0;
cpu_set_t *cpusetp;
size_t size;
int ret;
int i,n;

  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
#if !defined(OS_LINUX)
     return nums;
#endif

#if !defined(__GLIBC_PREREQ)
   return nums;
#else
 #if !__GLIBC_PREREQ(2, 3)
   return nums;
 #endif

 #if !__GLIBC_PREREQ(2, 7)
  ret = sched_getaffinity(0,sizeof(cpu_set_t), cpusetp);
  if (ret!=0) return nums;
  n=0;
  #if !__GLIBC_PREREQ(2, 6)
  for (i=0;i<nums;i++)
     if (CPU_ISSET(i,cpusetp)) n++;
  nums=n;
  #else
  nums = CPU_COUNT(sizeof(cpu_set_t),cpusetp);
  #endif
  return nums;
 #else
  cpusetp = CPU_ALLOC(nums);
  if (cpusetp == NULL) return nums;
  size = CPU_ALLOC_SIZE(nums);
  ret = sched_getaffinity(0,size,cpusetp);
  if (ret!=0) return nums;
  nums = CPU_COUNT_S(size,cpusetp);
  CPU_FREE(cpusetp);
  return nums;
 #endif
#endif
}
#endif
#endif

#ifdef OS_ANDROID
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif
	
#ifdef OS_HAIKU
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_AIX
int get_num_procs(void) {
  static int nums = 0;
  if (!nums) nums = sysconf(_SC_NPROCESSORS_CONF);
  return nums;
}
#endif

#ifdef OS_WINDOWS

int get_num_procs(void) {

  static int nums = 0;

  if (nums == 0) {

    SYSTEM_INFO sysinfo;

    GetSystemInfo(&sysinfo);

    nums = sysinfo.dwNumberOfProcessors;
  }

  return nums;
}

#endif

#if defined(OS_FREEBSD)

int get_num_procs(void) {

  static int nums = 0;

  int m[2];
  size_t len;

  if (nums == 0) {
    m[0] = CTL_HW;
    m[1] = HW_NCPU;
    len = sizeof(int);
    sysctl(m, 2, &nums, &len, NULL, 0);
  }

  return nums;
}

#endif

#if defined(OS_DARWIN)
int get_num_procs(void) {
  static int nums = 0;
  size_t len;
  if (nums == 0){
    len = sizeof(int);
    sysctlbyname("hw.physicalcpu", &nums, &len, NULL, 0);
  }
  return nums;
}
/*
void set_stack_limit(int limitMB){
  int result=0;
  struct rlimit rl;
  rlim_t StackSize;

  StackSize=limitMB*1024*1024;
  result=getrlimit(RLIMIT_STACK, &rl);
  if(result==0){
    if(rl.rlim_cur < StackSize){
      rl.rlim_cur=StackSize;
      result=setrlimit(RLIMIT_STACK, &rl);
      if(result !=0){
        fprintf(stderr, "OpenBLAS: set stack limit error =%d\n", result);
      }
    }
  }
}
*/
#endif


/*
OpenBLAS uses the numbers of CPU cores in multithreading.
It can be set by openblas_set_num_threads(int num_threads);
*/
int blas_cpu_number  = 0;
/*
The numbers of threads in the thread pool.
This value is equal or large than blas_cpu_number. This means some threads are sleep.
*/
int blas_num_threads = 0;

int  goto_get_num_procs  (void) {
  return blas_cpu_number;
}

void openblas_fork_handler()
{
  // This handler shuts down the OpenBLAS-managed PTHREAD pool when OpenBLAS is
  // built with "make USE_OPENMP=0".
  // Hanging can still happen when OpenBLAS is built against the libgomp
  // implementation of OpenMP. The problem is tracked at:
  //   http://gcc.gnu.org/bugzilla/show_bug.cgi?id=60035
  // In the mean time build with USE_OPENMP=0 or link against another
  // implementation of OpenMP.
#if !(defined(OS_WINDOWS) || defined(OS_ANDROID)) && defined(SMP_SERVER)
  int err;
  err = pthread_atfork ((void (*)(void)) BLASFUNC(blas_thread_shutdown), NULL, NULL);
  if(err != 0)
    openblas_warning(0, "OpenBLAS Warning ... cannot install fork handler. You may meet hang after fork.\n");
#endif
}

extern int openblas_num_threads_env();
extern int openblas_goto_num_threads_env();
extern int openblas_omp_num_threads_env();

int blas_get_cpu_number(void){
#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_DARWIN) || defined(OS_ANDROID)
  int max_num;
#endif
  int blas_goto_num   = 0;
  int blas_omp_num    = 0;

  if (blas_num_threads) return blas_num_threads;

#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_DARWIN) || defined(OS_ANDROID)
  max_num = get_num_procs();
#endif

  blas_goto_num = 0;
#ifndef USE_OPENMP
  blas_goto_num=openblas_num_threads_env();
  if (blas_goto_num < 0) blas_goto_num = 0;

  if (blas_goto_num == 0) {
    blas_goto_num=openblas_goto_num_threads_env();
    if (blas_goto_num < 0) blas_goto_num = 0;
  }

#endif

  blas_omp_num = 0;
  blas_omp_num=openblas_omp_num_threads_env();
  if (blas_omp_num < 0) blas_omp_num = 0;

  if (blas_goto_num > 0) blas_num_threads = blas_goto_num;
  else if (blas_omp_num > 0) blas_num_threads = blas_omp_num;
  else blas_num_threads = MAX_CPU_NUMBER;

#if defined(OS_LINUX) || defined(OS_WINDOWS) || defined(OS_FREEBSD) || defined(OS_DARWIN) || defined(OS_ANDROID)
  if (blas_num_threads > max_num) blas_num_threads = max_num;
#endif

  if (blas_num_threads > MAX_CPU_NUMBER) blas_num_threads = MAX_CPU_NUMBER;

#ifdef DEBUG
  printf( "Adjusted number of threads : %3d\n", blas_num_threads);
#endif

  blas_cpu_number = blas_num_threads;

  return blas_num_threads;
}
#endif


int openblas_get_num_procs(void) {
#ifndef SMP
  return 1;
#else
  return get_num_procs();
#endif
}

int openblas_get_num_threads(void) {
#ifndef SMP
  return 1;
#else
  // init blas_cpu_number if needed
  blas_get_cpu_number();
  return blas_cpu_number;
#endif
}

struct release_t {
  void *address;
  void (*func)(struct release_t *);
  long attr;
};

int hugetlb_allocated = 0;

static struct release_t release_info[NUM_BUFFERS];
static int release_pos = 0;

#if defined(OS_LINUX) && !defined(NO_WARMUP)
static int hot_alloc = 0;
#endif

/* Global lock for memory allocation */

#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    alloc_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t alloc_lock = 0;
#else
static BLASULONG  alloc_lock = 0UL;
#endif

#ifdef ALLOC_MMAP

static void alloc_mmap_free(struct release_t *release){

  if (munmap(release -> address, BUFFER_SIZE)) {
    printf("OpenBLAS : munmap failed\n");
  }
}



#ifdef NO_WARMUP

static void *alloc_mmap(void *address){
  void *map_address;

  if (address){
    map_address = mmap(address,
		       BUFFER_SIZE,
		       MMAP_ACCESS, MMAP_POLICY | MAP_FIXED, -1, 0);
  } else {
    map_address = mmap(address,
		       BUFFER_SIZE,
		       MMAP_ACCESS, MMAP_POLICY, -1, 0);
  }

  if (map_address != (void *)-1) {
    LOCK_COMMAND(&alloc_lock);
    release_info[release_pos].address = map_address;
    release_info[release_pos].func    = alloc_mmap_free;
    release_pos ++;
    UNLOCK_COMMAND(&alloc_lock);
  }

#ifdef OS_LINUX
  my_mbind(map_address, BUFFER_SIZE, MPOL_PREFERRED, NULL, 0, 0);
#endif

  return map_address;
}

#else

#define BENCH_ITERATION 4
#define SCALING		2

static inline BLASULONG run_bench(BLASULONG address, BLASULONG size) {

  BLASULONG original, *p;
  BLASULONG start, stop, min;
  int iter, i, count;

  min = (BLASULONG)-1;

  original = *(BLASULONG *)(address + size - PAGESIZE);

  *(BLASULONG *)(address + size - PAGESIZE) = (BLASULONG)address;

  for (iter = 0; iter < BENCH_ITERATION; iter ++ ) {

    p = (BLASULONG *)address;

    count = size / PAGESIZE;

    start = rpcc();

    for (i = 0; i < count; i ++) {
      p = (BLASULONG *)(*p);
    }

    stop = rpcc();

    if (min > stop - start) min = stop - start;
  }

  *(BLASULONG *)(address + size - PAGESIZE +  0) = original;
  *(BLASULONG *)(address + size - PAGESIZE +  8) = (BLASULONG)p;

  return min;
}

static void *alloc_mmap(void *address){
  void *map_address, *best_address;
  BLASULONG best, start, current;
  BLASULONG allocsize;

  if (address){
    /* Just give up use advanced operation */
    map_address = mmap(address, BUFFER_SIZE, MMAP_ACCESS, MMAP_POLICY | MAP_FIXED, -1, 0);

#ifdef OS_LINUX
    my_mbind(map_address, BUFFER_SIZE, MPOL_PREFERRED, NULL, 0, 0);
#endif

  } else {
#if defined(OS_LINUX) && !defined(NO_WARMUP)
    if (hot_alloc == 0) {
      map_address = mmap(NULL, BUFFER_SIZE, MMAP_ACCESS, MMAP_POLICY, -1, 0);

#ifdef OS_LINUX
      my_mbind(map_address, BUFFER_SIZE, MPOL_PREFERRED, NULL, 0, 0);
#endif

    } else {
#endif

      map_address = mmap(NULL, BUFFER_SIZE * SCALING,
			 MMAP_ACCESS, MMAP_POLICY, -1, 0);

      if (map_address != (void *)-1) {

#ifdef OS_LINUX
#ifdef DEBUG
		  int ret=0;
		  ret=my_mbind(map_address, BUFFER_SIZE * SCALING, MPOL_PREFERRED, NULL, 0, 0);
		  if(ret==-1){
			  int errsv=errno;
			  perror("OpenBLAS alloc_mmap:");
			  printf("error code=%d,\tmap_address=%lx\n",errsv,map_address);
		  }

#else
		  my_mbind(map_address, BUFFER_SIZE * SCALING, MPOL_PREFERRED, NULL, 0, 0);
#endif
#endif


	allocsize = DGEMM_P * DGEMM_Q * sizeof(double);

	start   = (BLASULONG)map_address;
	current = (SCALING - 1) * BUFFER_SIZE;

	while(current > 0) {
	  *(BLASLONG *)start = (BLASLONG)start + PAGESIZE;
	  start += PAGESIZE;
	  current -= PAGESIZE;
	}

	*(BLASLONG *)(start - PAGESIZE) = (BLASULONG)map_address;

	start = (BLASULONG)map_address;

	best = (BLASULONG)-1;
	best_address = map_address;

	while ((start + allocsize  < (BLASULONG)map_address + (SCALING - 1) * BUFFER_SIZE)) {

	  current = run_bench(start, allocsize);

	  if (best > current) {
	    best = current;
	    best_address = (void *)start;
	  }

	  start += PAGESIZE;

	}

      if ((BLASULONG)best_address > (BLASULONG)map_address)
	munmap(map_address,  (BLASULONG)best_address - (BLASULONG)map_address);

      munmap((void *)((BLASULONG)best_address + BUFFER_SIZE), (SCALING - 1) * BUFFER_SIZE + (BLASULONG)map_address - (BLASULONG)best_address);

      map_address = best_address;

#if defined(OS_LINUX) && !defined(NO_WARMUP)
      hot_alloc = 2;
#endif
      }
    }
#if defined(OS_LINUX) && !defined(NO_WARMUP)
  }
#endif
  LOCK_COMMAND(&alloc_lock);

  if (map_address != (void *)-1) {
    release_info[release_pos].address = map_address;
    release_info[release_pos].func    = alloc_mmap_free;
    release_pos ++;
  }
  UNLOCK_COMMAND(&alloc_lock);

  return map_address;
}

#endif

#endif


#ifdef ALLOC_MALLOC

static void alloc_malloc_free(struct release_t *release){

  free(release -> address);

}

static void *alloc_malloc(void *address){

  void *map_address;

  map_address = (void *)malloc(BUFFER_SIZE + FIXED_PAGESIZE);

  if (map_address == (void *)NULL) map_address = (void *)-1;

  if (map_address != (void *)-1) {
    release_info[release_pos].address = map_address;
    release_info[release_pos].func    = alloc_malloc_free;
    release_pos ++;
  }

  return map_address;

}

#endif

#ifdef ALLOC_QALLOC

void *qalloc(int flags, size_t bytes);
void *qfree (void *address);

#define QNONCACHE 0x1
#define QCOMMS    0x2
#define QFAST     0x4

static void alloc_qalloc_free(struct release_t *release){

  qfree(release -> address);

}

static void *alloc_qalloc(void *address){
  void *map_address;

  map_address = (void *)qalloc(QCOMMS | QFAST, BUFFER_SIZE + FIXED_PAGESIZE);

  if (map_address == (void *)NULL) map_address = (void *)-1;

  if (map_address != (void *)-1) {
    release_info[release_pos].address = map_address;
    release_info[release_pos].func    = alloc_qalloc_free;
    release_pos ++;
  }

  return (void *)(((BLASULONG)map_address + FIXED_PAGESIZE - 1) & ~(FIXED_PAGESIZE - 1));
}

#endif

#ifdef ALLOC_WINDOWS

static void alloc_windows_free(struct release_t *release){

  VirtualFree(release -> address, BUFFER_SIZE, MEM_DECOMMIT);

}

static void *alloc_windows(void *address){
  void *map_address;

  map_address  = VirtualAlloc(address,
			      BUFFER_SIZE,
			      MEM_RESERVE | MEM_COMMIT,
			      PAGE_READWRITE);

  if (map_address == (void *)NULL) map_address = (void *)-1;

  if (map_address != (void *)-1) {
    release_info[release_pos].address = map_address;
    release_info[release_pos].func    = alloc_windows_free;
    release_pos ++;
  }

  return map_address;
}

#endif

#ifdef ALLOC_DEVICEDRIVER
#ifndef DEVICEDRIVER_NAME
#define DEVICEDRIVER_NAME "/dev/mapper"
#endif

static void alloc_devicedirver_free(struct release_t *release){

  if (munmap(release -> address, BUFFER_SIZE)) {
    printf("OpenBLAS : Bugphysarea unmap failed.\n");
  }

  if (close(release -> attr)) {
    printf("OpenBLAS : Bugphysarea close failed.\n");
  }

}

static void *alloc_devicedirver(void *address){

  int fd;
  void *map_address;

  if ((fd = open(DEVICEDRIVER_NAME, O_RDWR | O_SYNC)) < 0) {

    return (void *)-1;

  }

  map_address = mmap(address, BUFFER_SIZE,
		     PROT_READ | PROT_WRITE,
		     MAP_FILE | MAP_SHARED,
		     fd, 0);

  if (map_address != (void *)-1) {
    release_info[release_pos].address = map_address;
    release_info[release_pos].attr    = fd;
    release_info[release_pos].func    = alloc_devicedirver_free;
    release_pos ++;
  }

  return map_address;
}

#endif

#ifdef ALLOC_SHM

static void alloc_shm_free(struct release_t *release){

  if (shmdt(release -> address)) {
    printf("OpenBLAS : Shared memory unmap failed.\n");
    }
}

static void *alloc_shm(void *address){
  void *map_address;
  int shmid;

  shmid = shmget(IPC_PRIVATE, BUFFER_SIZE,IPC_CREAT | 0600);

  map_address = (void *)shmat(shmid, address, 0);

  if (map_address != (void *)-1){

#ifdef OS_LINUX
    my_mbind(map_address, BUFFER_SIZE, MPOL_PREFERRED, NULL, 0, 0);
#endif

    shmctl(shmid, IPC_RMID, 0);

    release_info[release_pos].address = map_address;
    release_info[release_pos].attr    = shmid;
    release_info[release_pos].func    = alloc_shm_free;
    release_pos ++;
  }

  return map_address;
}

#if defined OS_LINUX  || defined OS_AIX  || defined __sun__  || defined OS_WINDOWS

static void alloc_hugetlb_free(struct release_t *release){

#if defined(OS_LINUX) || defined(OS_AIX)
  if (shmdt(release -> address)) {
    printf("OpenBLAS : Hugepage unmap failed.\n");
  }
#endif

#ifdef __sun__

  munmap(release -> address, BUFFER_SIZE);

#endif

#ifdef OS_WINDOWS

  VirtualFree(release -> address, BUFFER_SIZE, MEM_LARGE_PAGES | MEM_DECOMMIT);

#endif

}

static void *alloc_hugetlb(void *address){

  void *map_address = (void *)-1;

#if defined(OS_LINUX) || defined(OS_AIX)
  int shmid;

  shmid = shmget(IPC_PRIVATE, BUFFER_SIZE,
#ifdef OS_LINUX
		 SHM_HUGETLB |
#endif
#ifdef OS_AIX
		 SHM_LGPAGE | SHM_PIN |
#endif
		 IPC_CREAT | SHM_R | SHM_W);

  if (shmid != -1) {
    map_address = (void *)shmat(shmid, address, SHM_RND);

#ifdef OS_LINUX
    my_mbind(map_address, BUFFER_SIZE, MPOL_PREFERRED, NULL, 0, 0);
#endif

    if (map_address != (void *)-1){
      shmctl(shmid, IPC_RMID, 0);
    }
  }
#endif

#ifdef __sun__
  struct memcntl_mha mha;

  mha.mha_cmd = MHA_MAPSIZE_BSSBRK;
  mha.mha_flags = 0;
  mha.mha_pagesize = HUGE_PAGESIZE;
  memcntl(NULL, 0, MC_HAT_ADVISE, (char *)&mha, 0, 0);

  map_address = (BLASULONG)memalign(HUGE_PAGESIZE, BUFFER_SIZE);
#endif

#ifdef OS_WINDOWS

  HANDLE hToken;
  TOKEN_PRIVILEGES tp;

  if (OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hToken) != TRUE) return (void *) -1;

  tp.PrivilegeCount = 1;
  tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
  
  if (LookupPrivilegeValue(NULL, SE_LOCK_MEMORY_NAME, &tp.Privileges[0].Luid) != TRUE) {
      CloseHandle(hToken);
      return (void*)-1;
  }

  if (AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL) != TRUE) {
      CloseHandle(hToken);
      return (void*)-1;
  }

  map_address  = (void *)VirtualAlloc(address,
				      BUFFER_SIZE,
				      MEM_LARGE_PAGES | MEM_RESERVE | MEM_COMMIT,
				      PAGE_READWRITE);

  tp.Privileges[0].Attributes = 0;
  AdjustTokenPrivileges(hToken, FALSE, &tp, 0, NULL, NULL);

  if (map_address == (void *)NULL) map_address = (void *)-1;

#endif

  if (map_address != (void *)-1){
    release_info[release_pos].address = map_address;
    release_info[release_pos].func    = alloc_hugetlb_free;
    release_pos ++;
  }

  return map_address;
}
#endif

#endif

#ifdef  ALLOC_HUGETLBFILE

static int hugetlb_pid = 0;

static void alloc_hugetlbfile_free(struct release_t *release){

  if (munmap(release -> address, BUFFER_SIZE)) {
    printf("OpenBLAS : HugeTLBfs unmap failed.\n");
  }

  if (close(release -> attr)) {
    printf("OpenBLAS : HugeTLBfs close failed.\n");
  }
}

static void *alloc_hugetlbfile(void *address){

  void *map_address = (void *)-1;
  int fd;
  char filename[64];

  if (!hugetlb_pid) hugetlb_pid = getpid();

  sprintf(filename, "%s/gotoblas.%d", HUGETLB_FILE_NAME, hugetlb_pid);

  if ((fd = open(filename, O_RDWR | O_CREAT, 0700)) < 0) {
    return (void *)-1;
  }

  unlink(filename);

  map_address = mmap(address, BUFFER_SIZE,
		     PROT_READ | PROT_WRITE,
		     MAP_SHARED,
		     fd, 0);

  if (map_address != (void *)-1) {
    release_info[release_pos].address = map_address;
    release_info[release_pos].attr    = fd;
    release_info[release_pos].func    = alloc_hugetlbfile_free;
    release_pos ++;
  }

  return map_address;
}
#endif


#ifdef SEEK_ADDRESS
static BLASULONG base_address      = 0UL;
#else
static BLASULONG base_address      = BASE_ADDRESS;
#endif

static volatile struct {
  BLASULONG lock;
  void *addr;
#if defined(WHEREAMI) && !defined(USE_OPENMP)
  int   pos;
#endif
  int used;
#ifndef __64BIT__
  char dummy[48];
#else
  char dummy[40];
#endif

} memory[NUM_BUFFERS];

static int memory_initialized = 0;

/*       Memory allocation routine           */
/* procpos ... indicates where it comes from */
/*                0 : Level 3 functions      */
/*                1 : Level 2 functions      */
/*                2 : Thread                 */

void *blas_memory_alloc(int procpos){

  int position;
#if defined(WHEREAMI) && !defined(USE_OPENMP)
  int mypos;
#endif

  void *map_address;

  void *(*memoryalloc[])(void *address) = {
#ifdef ALLOC_DEVICEDRIVER
    alloc_devicedirver,
#endif
/* Hugetlb implicitly assumes ALLOC_SHM */
#ifdef ALLOC_SHM
    alloc_shm,
#endif
#if ((defined ALLOC_SHM) && (defined OS_LINUX  || defined OS_AIX  || defined __sun__  || defined OS_WINDOWS))
    alloc_hugetlb,
#endif
#ifdef ALLOC_MMAP
    alloc_mmap,
#endif
#ifdef ALLOC_QALLOC
    alloc_qalloc,
#endif
#ifdef ALLOC_WINDOWS
    alloc_windows,
#endif
#ifdef ALLOC_MALLOC
    alloc_malloc,
#endif
    NULL,
  };
  void *(**func)(void *address);
  LOCK_COMMAND(&alloc_lock);

  if (!memory_initialized) {

#if defined(WHEREAMI) && !defined(USE_OPENMP)
    for (position = 0; position < NUM_BUFFERS; position ++){
      memory[position].addr   = (void *)0;
      memory[position].pos    = -1;
      memory[position].used   = 0;
      memory[position].lock   = 0;
    }
#endif

#ifdef DYNAMIC_ARCH
    gotoblas_dynamic_init();
#endif

#if defined(SMP) && defined(OS_LINUX) && !defined(NO_AFFINITY)
    gotoblas_affinity_init();
#endif

#ifdef SMP
    if (!blas_num_threads) blas_cpu_number = blas_get_cpu_number();
#endif

#if defined(ARCH_X86) || defined(ARCH_X86_64) || defined(ARCH_IA64) || defined(ARCH_MIPS64) || defined(ARCH_ARM64)
#ifndef DYNAMIC_ARCH
    blas_set_parameter();
#endif
#endif

    memory_initialized = 1;

  }
  UNLOCK_COMMAND(&alloc_lock);

#ifdef DEBUG
  printf("Alloc Start ...\n");
#endif

/* #if defined(WHEREAMI) && !defined(USE_OPENMP)

  mypos = WhereAmI();

  position = mypos;
  while (position >= NUM_BUFFERS) position >>= 1;

  do {
    if (!memory[position].used && (memory[position].pos == mypos)) {
      LOCK_COMMAND(&alloc_lock);
//      blas_lock(&memory[position].lock);

      if (!memory[position].used) goto allocation;

      UNLOCK_COMMAND(&alloc_lock);
//      blas_unlock(&memory[position].lock);
    }

    position ++;

  } while (position < NUM_BUFFERS);


#endif */

  position = 0;

  LOCK_COMMAND(&alloc_lock);
  do {
/*    if (!memory[position].used) { */
/*      blas_lock(&memory[position].lock);*/

      if (!memory[position].used) goto allocation;
      
/*      blas_unlock(&memory[position].lock);*/
/*    } */

    position ++;

  } while (position < NUM_BUFFERS);
  UNLOCK_COMMAND(&alloc_lock);

  goto error;

  allocation :

#ifdef DEBUG
  printf("  Position -> %d\n", position);
#endif

  memory[position].used = 1;

  UNLOCK_COMMAND(&alloc_lock);
/*  blas_unlock(&memory[position].lock);*/

  if (!memory[position].addr) {
    do {
#ifdef DEBUG
      printf("Allocation Start : %lx\n", base_address);
#endif

      map_address = (void *)-1;

      func = &memoryalloc[0];

      while ((func != NULL) && (map_address == (void *) -1)) {

	map_address = (*func)((void *)base_address);

#ifdef ALLOC_DEVICEDRIVER
	if ((*func ==  alloc_devicedirver) && (map_address == (void *)-1)) {
	    fprintf(stderr, "OpenBLAS Warning ... Physically contigous allocation was failed.\n");
	}
#endif

#ifdef ALLOC_HUGETLBFILE
	if ((*func == alloc_hugetlbfile) && (map_address == (void *)-1)) {
#ifndef OS_WINDOWS
	    fprintf(stderr, "OpenBLAS Warning ... HugeTLB(File) allocation was failed.\n");
#endif
	}
#endif

#if (defined ALLOC_SHM) && (defined OS_LINUX  || defined OS_AIX  || defined __sun__  || defined OS_WINDOWS)
	if ((*func == alloc_hugetlb) && (map_address != (void *)-1)) hugetlb_allocated = 1;
#endif

	func ++;
      }

#ifdef DEBUG
      printf("  Success -> %08lx\n", map_address);
#endif
      if (((BLASLONG) map_address) == -1) base_address = 0UL;

      if (base_address) base_address += BUFFER_SIZE + FIXED_PAGESIZE;

    } while ((BLASLONG)map_address == -1);

    LOCK_COMMAND(&alloc_lock);
    memory[position].addr = map_address;
    UNLOCK_COMMAND(&alloc_lock);

#ifdef DEBUG
    printf("  Mapping Succeeded. %p(%d)\n", (void *)memory[position].addr, position);
#endif
  }

#if defined(WHEREAMI) && !defined(USE_OPENMP)

  if (memory[position].pos == -1) memory[position].pos = mypos;

#endif

#ifdef DYNAMIC_ARCH

  if (memory_initialized == 1) {

    LOCK_COMMAND(&alloc_lock);

    if (memory_initialized == 1) {

      if (!gotoblas) gotoblas_dynamic_init();

      memory_initialized = 2;
    }

    UNLOCK_COMMAND(&alloc_lock);

  }
#endif


#ifdef DEBUG
  printf("Mapped   : %p  %3d\n\n",
	  (void *)memory[position].addr, position);
#endif

  return (void *)memory[position].addr;

 error:
  printf("BLAS : Program is Terminated. Because you tried to allocate too many memory regions.\n");

  return NULL;
}

void blas_memory_free(void *free_area){

  int position;

#ifdef DEBUG
  printf("Unmapped Start : %p ...\n", free_area);
#endif

  position = 0;
  LOCK_COMMAND(&alloc_lock);

  while ((position < NUM_BUFFERS) && (memory[position].addr != free_area))
    position++;

  if (memory[position].addr != free_area) goto error;

#ifdef DEBUG
  printf("  Position : %d\n", position);
#endif

  // arm: ensure all writes are finished before other thread takes this memory
  WMB;

  memory[position].used = 0;
  UNLOCK_COMMAND(&alloc_lock);

#ifdef DEBUG
  printf("Unmap Succeeded.\n\n");
#endif

  return;

 error:
  printf("BLAS : Bad memory unallocation! : %4d  %p\n", position,  free_area);

#ifdef DEBUG
  for (position = 0; position < NUM_BUFFERS; position++)
    printf("%4ld  %p : %d\n", position, memory[position].addr, memory[position].used);
#endif
  UNLOCK_COMMAND(&alloc_lock);

  return;
}

void *blas_memory_alloc_nolock(int unused) {
  void *map_address;
  map_address = (void *)malloc(BUFFER_SIZE + FIXED_PAGESIZE);
  return map_address;
}

void blas_memory_free_nolock(void * map_address) {
  free(map_address);
}

void blas_shutdown(void){

  int pos;

#ifdef SMP
  BLASFUNC(blas_thread_shutdown)();
#endif

  LOCK_COMMAND(&alloc_lock);

  for (pos = 0; pos < release_pos; pos ++) {
    release_info[pos].func(&release_info[pos]);
  }

#ifdef SEEK_ADDRESS
  base_address      = 0UL;
#else
  base_address      = BASE_ADDRESS;
#endif

  for (pos = 0; pos < NUM_BUFFERS; pos ++){
    memory[pos].addr   = (void *)0;
    memory[pos].used   = 0;
#if defined(WHEREAMI) && !defined(USE_OPENMP)
    memory[pos].pos    = -1;
#endif
    memory[pos].lock   = 0;
  }

  UNLOCK_COMMAND(&alloc_lock);

  return;
}

#if defined(OS_LINUX) && !defined(NO_WARMUP)

#ifdef SMP
#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    init_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t init_lock = 0;
#else
static BLASULONG   init_lock = 0UL;
#endif
#endif

static void _touch_memory(blas_arg_t *arg, BLASLONG *range_m, BLASLONG *range_n,
			  void *sa, void *sb, BLASLONG pos) {

#if !defined(ARCH_POWER) && !defined(ARCH_SPARC)

  size_t size;
  BLASULONG buffer;

  size   = BUFFER_SIZE - PAGESIZE;
  buffer = (BLASULONG)sa + GEMM_OFFSET_A;

#if defined(OS_LINUX) && !defined(NO_WARMUP)
    if (hot_alloc != 2) {
#endif

#ifdef SMP
  LOCK_COMMAND(&init_lock);
#endif

  while (size > 0) {
    *(int *)buffer = size;
    buffer  += PAGESIZE;
    size    -= PAGESIZE;
  }

#ifdef SMP
  UNLOCK_COMMAND(&init_lock);
#endif

  size = MIN((BUFFER_SIZE - PAGESIZE), L2_SIZE);
  buffer = (BLASULONG)sa + GEMM_OFFSET_A;

  while (size > 0) {
    *(int *)buffer = size;
    buffer  += 64;
    size    -= 64;
  }

#if defined(OS_LINUX) && !defined(NO_WARMUP)
    }
#endif

#endif
}

#ifdef SMP

static void _init_thread_memory(void *buffer) {

  blas_queue_t queue[MAX_CPU_NUMBER];
  int num_cpu;

  for (num_cpu = 0; num_cpu < blas_num_threads; num_cpu++) {

    blas_queue_init(&queue[num_cpu]);
    queue[num_cpu].mode    = BLAS_DOUBLE | BLAS_REAL;
    queue[num_cpu].routine = &_touch_memory;
    queue[num_cpu].args    = NULL;
    queue[num_cpu].next    = &queue[num_cpu + 1];
  }

  queue[num_cpu - 1].next = NULL;
  queue[0].sa = buffer;

  exec_blas(num_cpu, queue);

}
#endif

static void gotoblas_memory_init(void) {

  void *buffer;

  hot_alloc = 1;

  buffer = (void *)blas_memory_alloc(0);

#ifdef SMP
  if (blas_cpu_number == 0) blas_get_cpu_number();
#ifdef SMP_SERVER
  if (blas_server_avail == 0) blas_thread_init();
#endif

  _init_thread_memory((void *)((BLASULONG)buffer + GEMM_OFFSET_A));

#else

  _touch_memory(NULL, NULL, NULL, (void *)((BLASULONG)buffer + GEMM_OFFSET_A), NULL, 0);

#endif

  blas_memory_free(buffer);
}
#endif

/* Initialization for all function; this function should be called before main */

static int gotoblas_initialized = 0;
extern void openblas_read_env();

void CONSTRUCTOR gotoblas_init(void) {

  if (gotoblas_initialized) return;

#ifdef SMP
  openblas_fork_handler();
#endif

  openblas_read_env();

#ifdef PROFILE
   moncontrol (0);
#endif

#ifdef DYNAMIC_ARCH
   gotoblas_dynamic_init();
#endif

#if defined(SMP) && defined(OS_LINUX) && !defined(NO_AFFINITY)
   gotoblas_affinity_init();
#endif

#if defined(OS_LINUX) && !defined(NO_WARMUP)
   gotoblas_memory_init();
#endif

//#if defined(OS_LINUX)
#if 0
   struct rlimit curlimit;
   if ( getrlimit(RLIMIT_STACK, &curlimit ) == 0 )
   {
	if ( curlimit.rlim_cur != curlimit.rlim_max )
	{
		curlimit.rlim_cur = curlimit.rlim_max;
		setrlimit(RLIMIT_STACK, &curlimit);
	}
   }
#endif

#ifdef SMP
  if (blas_cpu_number == 0) blas_get_cpu_number();
#ifdef SMP_SERVER
  if (blas_server_avail == 0) blas_thread_init();
#endif
#endif

#ifdef FUNCTION_PROFILE
   gotoblas_profile_init();
#endif

   gotoblas_initialized = 1;

#ifdef PROFILE
   moncontrol (1);
#endif

}

void DESTRUCTOR gotoblas_quit(void) {

  if (gotoblas_initialized == 0) return;

  blas_shutdown();

#ifdef PROFILE
   moncontrol (0);
#endif

#ifdef FUNCTION_PROFILE
   gotoblas_profile_quit();
#endif

#if defined(SMP) && defined(OS_LINUX) && !defined(NO_AFFINITY)
   gotoblas_affinity_quit();
#endif

#ifdef DYNAMIC_ARCH
   gotoblas_dynamic_quit();
#endif

   gotoblas_initialized = 0;

#ifdef PROFILE
   moncontrol (1);
#endif
}

#if defined(_MSC_VER) && !defined(__clang__)
BOOL APIENTRY DllMain(HMODULE hModule, DWORD  ul_reason_for_call, LPVOID lpReserved)
{
  switch (ul_reason_for_call)
  {
    case DLL_PROCESS_ATTACH:
      gotoblas_init();
      break;
    case DLL_THREAD_ATTACH:
      break;
    case DLL_THREAD_DETACH:
      break;
    case DLL_PROCESS_DETACH:
      gotoblas_quit();
      break;
    default:
      break;
  }
  return TRUE;
}

/*
  This is to allow static linking.
  Code adapted from Google performance tools:
  https://gperftools.googlecode.com/git-history/perftools-1.0/src/windows/port.cc
  Reference:
  https://sourceware.org/ml/pthreads-win32/2008/msg00028.html
  http://ci.boost.org/svn-trac/browser/trunk/libs/thread/src/win32/tss_pe.cpp
*/
static int on_process_term(void)
{
	gotoblas_quit();
	return 0;
}
#ifdef _WIN64
#pragma comment(linker, "/INCLUDE:_tls_used")
#else
#pragma comment(linker, "/INCLUDE:__tls_used")
#endif

#ifdef _WIN64
#pragma const_seg(".CRT$XLB")
#else
#pragma data_seg(".CRT$XLB")
#endif
static void (APIENTRY *dll_callback)(HINSTANCE h, DWORD ul_reason_for_call, PVOID pv) = DllMain;
#ifdef _WIN64
#pragma const_seg()
#else
#pragma data_seg()
#endif

#ifdef _WIN64
#pragma const_seg(".CRT$XTU")
#else
#pragma data_seg(".CRT$XTU")
#endif
static int(*p_process_term)(void) = on_process_term;
#ifdef _WIN64
#pragma const_seg()
#else
#pragma data_seg()
#endif
#endif

#if (defined(C_PGI) || (!defined(C_SUN) && defined(F_INTERFACE_SUN))) && (defined(ARCH_X86) || defined(ARCH_X86_64))
/* Don't call me; this is just work around for PGI / Sun bug */
void gotoblas_dummy_for_PGI(void) {

  gotoblas_init();
  gotoblas_quit();

#if 0
  asm ("\t.section\t.ctors,\"aw\",@progbits; .align 8; .quad gotoblas_init; .section .text");
  asm ("\t.section\t.dtors,\"aw\",@progbits; .align 8; .quad gotoblas_quit; .section .text");
#else
  asm (".section .init,\"ax\"; call gotoblas_init@PLT; .section .text");
  asm (".section .fini,\"ax\"; call gotoblas_quit@PLT; .section .text");
#endif
}
#endif

#endif
