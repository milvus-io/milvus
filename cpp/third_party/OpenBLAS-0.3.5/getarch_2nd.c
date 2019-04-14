#include <stdio.h>
#ifndef BUILD_KERNEL
#include "config.h"
#else
#include "config_kernel.h"
#endif
#include "param.h"

int main(int argc, char **argv) {

  if ( (argc <= 1) || ((argc >= 2) && (*argv[1] == '0'))) {
    printf("SGEMM_UNROLL_M=%d\n", SGEMM_DEFAULT_UNROLL_M);
    printf("SGEMM_UNROLL_N=%d\n", SGEMM_DEFAULT_UNROLL_N);
    printf("DGEMM_UNROLL_M=%d\n", DGEMM_DEFAULT_UNROLL_M);
    printf("DGEMM_UNROLL_N=%d\n", DGEMM_DEFAULT_UNROLL_N);
    printf("QGEMM_UNROLL_M=%d\n", QGEMM_DEFAULT_UNROLL_M);
    printf("QGEMM_UNROLL_N=%d\n", QGEMM_DEFAULT_UNROLL_N);

    printf("CGEMM_UNROLL_M=%d\n", CGEMM_DEFAULT_UNROLL_M);
    printf("CGEMM_UNROLL_N=%d\n", CGEMM_DEFAULT_UNROLL_N);
    printf("ZGEMM_UNROLL_M=%d\n", ZGEMM_DEFAULT_UNROLL_M);
    printf("ZGEMM_UNROLL_N=%d\n", ZGEMM_DEFAULT_UNROLL_N);
    printf("XGEMM_UNROLL_M=%d\n", XGEMM_DEFAULT_UNROLL_M);
    printf("XGEMM_UNROLL_N=%d\n", XGEMM_DEFAULT_UNROLL_N);

#ifdef CGEMM3M_DEFAULT_UNROLL_M
    printf("CGEMM3M_UNROLL_M=%d\n", CGEMM3M_DEFAULT_UNROLL_M);
#else
    printf("CGEMM3M_UNROLL_M=%d\n", SGEMM_DEFAULT_UNROLL_M);
#endif

#ifdef CGEMM3M_DEFAULT_UNROLL_N
    printf("CGEMM3M_UNROLL_N=%d\n", CGEMM3M_DEFAULT_UNROLL_N);
#else
    printf("CGEMM3M_UNROLL_N=%d\n", SGEMM_DEFAULT_UNROLL_N);
#endif

#ifdef ZGEMM3M_DEFAULT_UNROLL_M
    printf("ZGEMM3M_UNROLL_M=%d\n", ZGEMM3M_DEFAULT_UNROLL_M);
#else
    printf("ZGEMM3M_UNROLL_M=%d\n", DGEMM_DEFAULT_UNROLL_M);
#endif

#ifdef ZGEMM3M_DEFAULT_UNROLL_N
    printf("ZGEMM3M_UNROLL_N=%d\n", ZGEMM3M_DEFAULT_UNROLL_N);
#else
    printf("ZGEMM3M_UNROLL_N=%d\n", DGEMM_DEFAULT_UNROLL_N);
#endif

#ifdef XGEMM3M_DEFAULT_UNROLL_M
    printf("XGEMM3M_UNROLL_M=%d\n", ZGEMM3M_DEFAULT_UNROLL_M);
#else
    printf("XGEMM3M_UNROLL_M=%d\n", QGEMM_DEFAULT_UNROLL_M);
#endif

#ifdef XGEMM3M_DEFAULT_UNROLL_N
    printf("XGEMM3M_UNROLL_N=%d\n", ZGEMM3M_DEFAULT_UNROLL_N);
#else
    printf("XGEMM3M_UNROLL_N=%d\n", QGEMM_DEFAULT_UNROLL_N);
#endif


  }


  if ((argc >= 2) && (*argv[1] == '1')) {

#if defined(ARCH_X86) || defined(ARCH_X86_64)
    printf("#define SLOCAL_BUFFER_SIZE\t%ld\n", (SGEMM_DEFAULT_Q * SGEMM_DEFAULT_UNROLL_N * 4 * 1 *  sizeof(float)));
    printf("#define DLOCAL_BUFFER_SIZE\t%ld\n", (DGEMM_DEFAULT_Q * DGEMM_DEFAULT_UNROLL_N * 2 * 1 *  sizeof(double)));
    printf("#define CLOCAL_BUFFER_SIZE\t%ld\n", (CGEMM_DEFAULT_Q * CGEMM_DEFAULT_UNROLL_N * 4 * 2 *  sizeof(float)));
    printf("#define ZLOCAL_BUFFER_SIZE\t%ld\n", (ZGEMM_DEFAULT_Q * ZGEMM_DEFAULT_UNROLL_N * 2 * 2 *  sizeof(double)));
#endif

#ifdef USE64BITINT
	printf("#define USE64BITINT\n");
#endif
	printf("#define GEMM_MULTITHREAD_THRESHOLD\t%ld\n", (long int)GEMM_MULTITHREAD_THRESHOLD);
  }

  return 0;
}
