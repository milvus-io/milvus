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

double fabs(double);

#undef POTRF

#ifndef COMPLEX
#ifdef XDOUBLE
#define POTRF   BLASFUNC(qpotrf)
#define SYRK    BLASFUNC(qsyrk)
#elif defined(DOUBLE)
#define POTRF   BLASFUNC(dpotrf)
#define SYRK    BLASFUNC(dsyrk)
#else
#define POTRF   BLASFUNC(spotrf)
#define SYRK    BLASFUNC(ssyrk)
#endif
#else
#ifdef XDOUBLE
#define POTRF   BLASFUNC(xpotrf)
#define SYRK    BLASFUNC(xherk)
#elif defined(DOUBLE)
#define POTRF   BLASFUNC(zpotrf)
#define SYRK    BLASFUNC(zherk)
#else
#define POTRF   BLASFUNC(cpotrf)
#define SYRK    BLASFUNC(cherk)
#endif
#endif



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


static __inline double getmflops(int ratio, int m, double secs){

  double mm = (double)m;
  double mulflops, addflops;

  if (secs==0.) return 0.;

  mulflops = mm * (1./3. + mm * (1./2. + mm * 1./6.));
  addflops = 1./6. * mm * (mm * mm - 1);

  if (ratio == 1) {
    return (mulflops + addflops) / secs * 1.e-6;
  } else {
    return (2. * mulflops + 6. * addflops) / secs * 1.e-6;
  }
}


int main(int argc, char *argv[]){

#ifndef COMPLEX
  char *trans[] = {"T", "N"};
#else
  char *trans[] = {"C", "N"};
#endif
  char *uplo[]  = {"U", "L"};
  FLOAT alpha[] = {1.0, 0.0};
  FLOAT beta [] = {0.0, 0.0};

  FLOAT *a, *b;

  blasint m, i, j, info, uplos;

  int from =   1;
  int to   = 200;
  int step =   1;

  FLOAT maxerr;

  struct timeval start, stop;
  double time1;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  fprintf(stderr, "From : %3d  To : %3d Step = %3d\n", from, to, step);

  if (( a    = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( b    = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  for(m = from; m <= to; m += step){

    fprintf(stderr, "M = %6d : ", (int)m);

    for (uplos = 0; uplos < 2; uplos ++) {

#ifndef COMPLEX
      if (uplos & 1) {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++)     a[i + j * m] = 0.;
	                             a[j + j * m] = ((double) rand() / (double) RAND_MAX) + 8.;
	  for(i = j + 1; i < m; i++) a[i + j * m] = ((double) rand() / (double) RAND_MAX) - 0.5;
	}
      } else {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++)     a[i + j * m] = ((double) rand() / (double) RAND_MAX) - 0.5;
	                             a[j + j * m] = ((double) rand() / (double) RAND_MAX) + 8.;
	  for(i = j + 1; i < m; i++) a[i + j * m] = 0.;
	}
      }
#else
      if (uplos & 1) {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++) {
	    a[(i + j * m) * 2 + 0] = 0.;
	    a[(i + j * m) * 2 + 1] = 0.;
	  }

	  a[(j + j * m) * 2 + 0] = ((double) rand() / (double) RAND_MAX) + 8.;
	  a[(j + j * m) * 2 + 1] = 0.;

	  for(i = j + 1; i < m; i++) {
	    a[(i + j * m) * 2 + 0] = ((double) rand() / (double) RAND_MAX) - 0.5;
	    a[(i + j * m) * 2 + 1] = ((double) rand() / (double) RAND_MAX) - 0.5;
	  }
	}
      } else {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++) {
	    a[(i + j * m) * 2 + 0] = ((double) rand() / (double) RAND_MAX) - 0.5;
	    a[(i + j * m) * 2 + 1] = ((double) rand() / (double) RAND_MAX) - 0.5;
	  }

	  a[(j + j * m) * 2 + 0] = ((double) rand() / (double) RAND_MAX) + 8.;
	  a[(j + j * m) * 2 + 1] = 0.;

	  for(i = j + 1; i < m; i++) {
	    a[(i + j * m) * 2 + 0] = 0.;
	    a[(i + j * m) * 2 + 1] = 0.;
	  }
	}
      }
#endif

      SYRK(uplo[uplos], trans[uplos], &m, &m, alpha, a, &m, beta, b, &m);

      gettimeofday( &start, (struct timezone *)0);

      POTRF(uplo[uplos], &m, b, &m, &info);

      gettimeofday( &stop, (struct timezone *)0);

      if (info != 0) {
	fprintf(stderr, "Info = %d\n", info);
	exit(1);
      }

     time1 = (double)(stop.tv_sec - start.tv_sec) + (double)((stop.tv_usec - start.tv_usec)) * 1.e-6;

      maxerr = 0.;

      if (!(uplos & 1)) {
	for (j = 0; j < m; j++) {
	  for(i = 0; i <= j; i++) {
#ifndef COMPLEX
	    if (maxerr < fabs(a[i + j * m] - b[i + j * m])) maxerr = fabs(a[i + j * m] - b[i + j * m]);
#else
	    if (maxerr < fabs(a[(i + j * m) * 2 + 0] - b[(i + j * m) * 2 + 0])) maxerr = fabs(a[(i + j * m) * 2 + 0] - b[(i + j * m) * 2 + 0]);
	    if (maxerr < fabs(a[(i + j * m) * 2 + 1] - b[(i + j * m) * 2 + 1])) maxerr = fabs(a[(i + j * m) * 2 + 1] - b[(i + j * m) * 2 + 1]);
#endif
	  }
	}
      } else {
	for (j = 0; j < m; j++) {
	  for(i = j; i < m; i++) {
#ifndef COMPLEX
	    if (maxerr < fabs(a[i + j * m] - b[i + j * m])) maxerr = fabs(a[i + j * m] - b[i + j * m]);
#else
	    if (maxerr < fabs(a[(i + j * m) * 2 + 0] - b[(i + j * m) * 2 + 0])) maxerr = fabs(a[(i + j * m) * 2 + 0] - b[(i + j * m) * 2 + 0]);
	    if (maxerr < fabs(a[(i + j * m) * 2 + 1] - b[(i + j * m) * 2 + 1])) maxerr = fabs(a[(i + j * m) * 2 + 1] - b[(i + j * m) * 2 + 1]);
#endif
	  }
	}
      }

      fprintf(stderr,
#ifdef XDOUBLE
	      "  %Le  %10.3f MFlops", maxerr,
#else
	      "  %e  %10.3f MFlops", maxerr,
#endif
	      getmflops(COMPSIZE * COMPSIZE, m, time1));

      if (maxerr > 1.e-3) {
	fprintf(stderr, "Hmm, probably it has bug.\n");
	exit(1);
      }

    }
    fprintf(stderr, "\n");

  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
