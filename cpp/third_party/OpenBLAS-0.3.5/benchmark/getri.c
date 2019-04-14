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
#ifdef __CYGWIN32__
#include <sys/time.h>
#endif
#include "common.h"

#undef GETRF
#undef GETRI

#ifndef COMPLEX
#ifdef XDOUBLE
#define GETRF   BLASFUNC(qgetrf)
#define GETRI   BLASFUNC(qgetri)
#elif defined(DOUBLE)
#define GETRF   BLASFUNC(dgetrf)
#define GETRI   BLASFUNC(dgetri)
#else
#define GETRF   BLASFUNC(sgetrf)
#define GETRI   BLASFUNC(sgetri)
#endif
#else
#ifdef XDOUBLE
#define GETRF   BLASFUNC(xgetrf)
#define GETRI   BLASFUNC(xgetri)
#elif defined(DOUBLE)
#define GETRF   BLASFUNC(zgetrf)
#define GETRI   BLASFUNC(zgetri)
#else
#define GETRF   BLASFUNC(cgetrf)
#define GETRI   BLASFUNC(cgetri)
#endif
#endif

extern void GETRI(blasint *m, FLOAT *a, blasint *lda, blasint *ipiv, FLOAT *work, blasint *lwork, blasint *info);

#if defined(__WIN32__) || defined(__WIN64__)

#ifndef DELTA_EPOCH_IN_MICROSECS
#define DELTA_EPOCH_IN_MICROSECS 11644473600000000ULL
#endif

int gettimeofday(struct timeval *tv, void *tz){

  FILETIME ft;
  unsigned __int64 tmpres = 0;
  static int tzflag;

  if (NULL != tv)
    {
      GetSystemTimeAsFileTime(&ft);

      tmpres |= ft.dwHighDateTime;
      tmpres <<= 32;
      tmpres |= ft.dwLowDateTime;

      /*converting file time to unix epoch*/
      tmpres /= 10;  /*convert into microseconds*/
      tmpres -= DELTA_EPOCH_IN_MICROSECS;
      tv->tv_sec = (long)(tmpres / 1000000UL);
      tv->tv_usec = (long)(tmpres % 1000000UL);
    }

  return 0;
}

#endif

#if !defined(__WIN32__) && !defined(__WIN64__) && !defined(__CYGWIN32__) && 0

static void *huge_malloc(BLASLONG size){
  int shmid;
  void *address;

#ifndef SHM_HUGETLB
#define SHM_HUGETLB 04000
#endif

  if ((shmid =shmget(IPC_PRIVATE,
		     (size + HUGE_PAGESIZE) & ~(HUGE_PAGESIZE - 1),
		     SHM_HUGETLB | IPC_CREAT |0600)) < 0) {
    printf( "Memory allocation failed(shmget).\n");
    exit(1);
  }

  address = shmat(shmid, NULL, SHM_RND);

  if ((BLASLONG)address == -1){
    printf( "Memory allocation failed(shmat).\n");
    exit(1);
  }

  shmctl(shmid, IPC_RMID, 0);

  return address;
}

#define malloc huge_malloc

#endif

int main(int argc, char *argv[]){

  FLOAT *a,*work;
  FLOAT wkopt[4];
  blasint *ipiv;
  blasint m, i, j, info,lwork;

  int from =   1;
  int to   = 200;
  int step =   1;

  struct timeval start, stop;
  double time1;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}


  fprintf(stderr, "From : %3d  To : %3d Step = %3d\n", from, to, step);

  if (( a = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( ipiv = (blasint *)malloc(sizeof(blasint) * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }



    for(j = 0; j < to; j++){
      for(i = 0; i < to * COMPSIZE; i++){
	a[i + j * to * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
      }
    }


    lwork = -1;
    m=to;

  GETRI(&m, a, &m, ipiv, wkopt, &lwork, &info);

  lwork = (blasint)wkopt[0];
  if (( work = (FLOAT *)malloc(sizeof(FLOAT) * lwork * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }


#ifdef linux
  srandom(getpid());
#endif

  fprintf(stderr, "   SIZE           FLops           Time          Lwork\n");

  for(m = from; m <= to; m += step){

    fprintf(stderr, " %6d : ", (int)m);

    GETRF (&m, &m, a, &m, ipiv, &info);

    if (info) {
      fprintf(stderr, "Matrix is not singular .. %d\n", info);
      exit(1);
    }

    gettimeofday( &start, (struct timezone *)0);

    lwork = -1;
    GETRI(&m, a, &m, ipiv, wkopt, &lwork, &info);

    lwork = (blasint)wkopt[0];
    GETRI(&m, a, &m, ipiv, work, &lwork, &info);
    gettimeofday( &stop, (struct timezone *)0);

    if (info) {
      fprintf(stderr, "failed compute inverse matrix .. %d\n", info);
      exit(1);
    }

    time1 = (double)(stop.tv_sec - start.tv_sec) + (double)((stop.tv_usec - start.tv_usec)) * 1.e-6;

    fprintf(stderr,
	    " %10.2f MFlops : %10.2f Sec : %d\n",
	    COMPSIZE * COMPSIZE * (4.0/3.0 * (double)m * (double)m *(double)m - (double)m *(double)m + 5.0/3.0* (double)m) / time1 * 1.e-6,time1,lwork);


  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
