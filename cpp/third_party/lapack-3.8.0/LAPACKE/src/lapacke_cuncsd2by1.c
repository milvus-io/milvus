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
* Contents: Native high-level C interface to LAPACK function cuncsd2by1
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_cuncsd2by1( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, lapack_int m, lapack_int p, lapack_int q,
                           lapack_complex_float* x11, lapack_int ldx11,
                           lapack_complex_float* x21, lapack_int ldx21,
                           float* theta, lapack_complex_float* u1,
                           lapack_int ldu1, lapack_complex_float* u2,
                           lapack_int ldu2, lapack_complex_float* v1t, lapack_int ldv1t )
{
    lapack_int info = 0;
    lapack_int lrwork = -1;
    lapack_int lwork = -1;
    lapack_int* iwork = NULL;
    float* rwork = NULL;
    lapack_complex_float* work = NULL;
    float rwork_query;
    lapack_complex_float work_query;
    lapack_int nrows_x11, nrows_x21;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_cuncsd2by1", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        nrows_x11 = p;
        nrows_x21 = m-p;
        if( LAPACKE_cge_nancheck( matrix_layout, nrows_x11, q, x11, ldx11 ) ) {
            return -8;
        }

        if( LAPACKE_cge_nancheck( matrix_layout, nrows_x21, q, x21, ldx21 ) ) {
            return -9;
        }
    }
#endif
    /* Allocate memory for working array(s) */
    iwork = (lapack_int*)LAPACKE_malloc( sizeof(lapack_int) * MAX(1,m-MIN(MIN(p,m-p),MIN(q,m-q))) );
    if( iwork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Query optimal working array(s) size */
    info = LAPACKE_cuncsd2by1_work( matrix_layout, jobu1, jobu2, jobv1t, m, p, q,
                                x11, ldx11, x21, ldx21, theta, u1, ldu1, u2,
                                ldu2, v1t, ldv1t, &work_query,
                                lwork, &rwork_query, lrwork, iwork );
    if( info != 0 ) {
        goto exit_level_2;
    }
    lrwork = (lapack_int)rwork_query;
    lwork = LAPACK_C2INT( work_query );
    /* Allocate memory for work arrays */
    rwork = (float*)LAPACKE_malloc( sizeof(float) * lrwork );
    if( rwork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_1;
    }
    work = (lapack_complex_float*)LAPACKE_malloc( sizeof(lapack_complex_float) * lwork );
    if( work == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_2;
    }
    /* Call middle-level interface */
    info = LAPACKE_cuncsd2by1_work( matrix_layout, jobu1, jobu2, jobv1t, m, p, q,
                                x11, ldx11, x21, ldx21, theta, u1, ldu1, u2,
                                ldu2, v1t, ldv1t, work, lwork, rwork, lrwork, iwork );
    /* Release memory and exit */
    LAPACKE_free( work );
exit_level_2:
    LAPACKE_free( rwork );
exit_level_1:
    LAPACKE_free( iwork );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_cuncsd2by1", info );
    }
    return info;
}
