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

int CNAME(int mode,
	  blas_arg_t *arg, BLASLONG *range_m, BLASLONG *range_n,
	  int (*function)(), void *sa, void *sb, BLASLONG divM, BLASLONG divN) {

  blas_queue_t queue[MAX_CPU_NUMBER];

  BLASLONG range_M[MAX_CPU_NUMBER + 1], range_N[MAX_CPU_NUMBER + 1];
  BLASLONG procs, num_cpu_m, num_cpu_n;

  BLASLONG width, i, j;

  if (!range_m) {
    range_M[0] = 0;
    i          = arg -> m;
  } else {
    range_M[0] = range_m[0];
    i          = range_m[1] - range_m[0];
  }

  num_cpu_m  = 0;

  while (i > 0){

    width  = blas_quickdivide(i + divM - num_cpu_m - 1, divM - num_cpu_m);

    i -= width;
    if (i < 0) width = width + i;

    range_M[num_cpu_m + 1] = range_M[num_cpu_m] + width;

    num_cpu_m ++;
  }

  if (!range_n) {
    range_N[0] = 0;
    i          = arg -> n;
  } else {
    range_N[0] = range_n[0];
    i          = range_n[1] - range_n[0];
  }

  num_cpu_n  = 0;

  while (i > 0){

    width  = blas_quickdivide(i + divN - num_cpu_n - 1, divN - num_cpu_n);

    i -= width;
    if (i < 0) width = width + i;

    range_N[num_cpu_n + 1] = range_N[num_cpu_n] + width;

    num_cpu_n ++;
  }

  procs = 0;

  for (j = 0; j < num_cpu_n; j++) {
    for (i = 0; i < num_cpu_m; i++) {

    queue[procs].mode    = mode;
    queue[procs].routine = function;
    queue[procs].args    = arg;
    queue[procs].range_m = &range_M[i];
    queue[procs].range_n = &range_N[j];
    queue[procs].sa      = NULL;
    queue[procs].sb      = NULL;
    queue[procs].next    = &queue[procs + 1];

    procs ++;
    }
  }

  if (procs) {
    queue[0].sa = sa;
    queue[0].sb = sb;

    queue[procs - 1].next = NULL;

    exec_blas(procs, queue);
  }

  return 0;
}

