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

int blas_level1_thread(int mode, BLASLONG m, BLASLONG n, BLASLONG k, void *alpha,
		       void *a, BLASLONG lda,
		       void *b, BLASLONG ldb,
		       void *c, BLASLONG ldc, int (*function)(), int nthreads){

  blas_queue_t queue[MAX_CPU_NUMBER];
  blas_arg_t   args [MAX_CPU_NUMBER];

  BLASLONG i, width, astride, bstride;
  int num_cpu, calc_type;

  calc_type = (mode & BLAS_PREC) + ((mode & BLAS_COMPLEX) != 0) + 2;

  mode |= BLAS_LEGACY;

  for (i = 0; i < nthreads; i++) blas_queue_init(&queue[i]);

  num_cpu = 0;
  i = m;

  while (i > 0){

    /* Adjust Parameters */
    width  = blas_quickdivide(i + nthreads - num_cpu - 1,
			      nthreads - num_cpu);

    i -= width;
    if (i < 0) width = width + i;

    astride = width * lda;

    if (!(mode & BLAS_TRANSB_T)) {
      bstride = width * ldb;
    } else {
      bstride = width;
    }

    astride <<= calc_type;
    bstride <<= calc_type;

    args[num_cpu].m = width;
    args[num_cpu].n = n;
    args[num_cpu].k = k;
    args[num_cpu].a = (void *)a;
    args[num_cpu].b = (void *)b;
    args[num_cpu].c = (void *)c;
    args[num_cpu].lda = lda;
    args[num_cpu].ldb = ldb;
    args[num_cpu].ldc = ldc;
    args[num_cpu].alpha = alpha;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = function;
    queue[num_cpu].args    = &args[num_cpu];
    queue[num_cpu].next    = &queue[num_cpu + 1];

    a = (void *)((BLASULONG)a + astride);
    b = (void *)((BLASULONG)b + bstride);

    num_cpu ++;
  }

  if (num_cpu) {
    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  return 0;
}

int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n, BLASLONG k, void *alpha,
		       void *a, BLASLONG lda,
		       void *b, BLASLONG ldb,
		       void *c, BLASLONG ldc, int (*function)(), int nthreads){

  blas_queue_t queue[MAX_CPU_NUMBER];
  blas_arg_t   args [MAX_CPU_NUMBER];

  BLASLONG i, width, astride, bstride;
  int num_cpu, calc_type;

  calc_type = (mode & BLAS_PREC) + ((mode & BLAS_COMPLEX) != 0) + 2;

  mode |= BLAS_LEGACY;

  for (i = 0; i < nthreads; i++) blas_queue_init(&queue[i]);

  num_cpu = 0;
  i = m;

  while (i > 0){

    /* Adjust Parameters */
    width  = blas_quickdivide(i + nthreads - num_cpu - 1,
			      nthreads - num_cpu);

    i -= width;
    if (i < 0) width = width + i;

    astride = width * lda;

    if (!(mode & BLAS_TRANSB_T)) {
      bstride = width * ldb;
    } else {
      bstride = width;
    }

    astride <<= calc_type;
    bstride <<= calc_type;

    args[num_cpu].m = width;
    args[num_cpu].n = n;
    args[num_cpu].k = k;
    args[num_cpu].a = (void *)a;
    args[num_cpu].b = (void *)b;
    args[num_cpu].c = (void *)((char *)c + num_cpu * sizeof(double)*2);
    args[num_cpu].lda = lda;
    args[num_cpu].ldb = ldb;
    args[num_cpu].ldc = ldc;
    args[num_cpu].alpha = alpha;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = function;
    queue[num_cpu].args    = &args[num_cpu];
    queue[num_cpu].next    = &queue[num_cpu + 1];

    a = (void *)((BLASULONG)a + astride);
    b = (void *)((BLASULONG)b + bstride);

    num_cpu ++;
  }

  if (num_cpu) {
    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  return 0;
}
