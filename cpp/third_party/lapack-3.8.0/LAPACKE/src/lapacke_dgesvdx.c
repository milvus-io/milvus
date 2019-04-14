/*****************************************************************************
  Copyright (c) 2014, Intel Corp.
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Intel Corporation nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
  THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************
* Contents: Native high-level C interface to LAPACK function dgesvdx
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dgesvdx( int matrix_layout, char jobu, char jobvt, char range,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda, double vl, double vu,
                           lapack_int il, lapack_int iu, lapack_int* ns,
                           double* s, double* u, lapack_int ldu,
                           double* vt, lapack_int ldvt,
                           lapack_int* superb )
{
    lapack_int info = 0;
    lapack_int lwork = -1;
    double* work = NULL;
    double work_query;
    lapack_int* iwork = NULL;
    lapack_int i;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_dgesvdx", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        if( LAPACKE_dge_nancheck( matrix_layout, m, n, a, lda ) ) {
            return -6;
        }
    }
#endif
    /* Query optimal working array(s) size */
    info = LAPACKE_dgesvdx_work( matrix_layout, jobu, jobvt, range,
                                 m, n, a, lda, vl, vu, il, iu, ns, s, u,
                                 ldu, vt, ldvt, &work_query, lwork, iwork );
    if( info != 0 ) {
        goto exit_level_0;
    }
    lwork = (lapack_int)work_query;
    /* Allocate memory for work arrays */
    work = (double*)LAPACKE_malloc( sizeof(double) * lwork );
    if( work == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    iwork = (lapack_int*)LAPACKE_malloc( sizeof(lapack_int) * MAX(1,(12*MIN(m,n))) );
    if( iwork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_1;
    }
    /* Call middle-level interface */
    info = LAPACKE_dgesvdx_work( matrix_layout, jobu, jobvt,  range,
                                 m, n, a, lda, vl, vu, il, iu, ns, s, u,
                                 ldu, vt, ldvt, work, lwork, iwork );
    /* Backup significant data from working array(s) */
    for( i=0; i<12*MIN(m,n)-1; i++ ) {
        superb[i] = iwork[i+1];
    }
    /* Release memory and exit */
    LAPACKE_free( iwork );
exit_level_1:
    LAPACKE_free( work );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_dgesvdx", info );
    }
    return info;
}
