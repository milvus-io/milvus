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
* Contents: Native high-level C interface to LAPACK function ctgsna
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_ctgsna( int matrix_layout, char job, char howmny,
                           const lapack_logical* select, lapack_int n,
                           const lapack_complex_float* a, lapack_int lda,
                           const lapack_complex_float* b, lapack_int ldb,
                           const lapack_complex_float* vl, lapack_int ldvl,
                           const lapack_complex_float* vr, lapack_int ldvr,
                           float* s, float* dif, lapack_int mm, lapack_int* m )
{
    lapack_int info = 0;
    lapack_int lwork = -1;
    lapack_int* iwork = NULL;
    lapack_complex_float* work = NULL;
    lapack_complex_float work_query;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_ctgsna", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        if( LAPACKE_cge_nancheck( matrix_layout, n, n, a, lda ) ) {
            return -6;
        }
        if( LAPACKE_cge_nancheck( matrix_layout, n, n, b, ldb ) ) {
            return -8;
        }
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'e' ) ) {
            if( LAPACKE_cge_nancheck( matrix_layout, n, mm, vl, ldvl ) ) {
                return -10;
            }
        }
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'e' ) ) {
            if( LAPACKE_cge_nancheck( matrix_layout, n, mm, vr, ldvr ) ) {
                return -12;
            }
        }
    }
#endif
    /* Allocate memory for working array(s) */
    if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'v' ) ) {
        iwork = (lapack_int*)LAPACKE_malloc( sizeof(lapack_int) * MAX(1,n+2) );
        if( iwork == NULL ) {
            info = LAPACK_WORK_MEMORY_ERROR;
            goto exit_level_0;
        }
    }
    /* Query optimal working array(s) size */
    info = LAPACKE_ctgsna_work( matrix_layout, job, howmny, select, n, a, lda, b,
                                ldb, vl, ldvl, vr, ldvr, s, dif, mm, m,
                                &work_query, lwork, iwork );
    if( info != 0 ) {
        goto exit_level_1;
    }
    lwork = LAPACK_C2INT( work_query );
    /* Allocate memory for work arrays */
    if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'v' ) ) {
        work = (lapack_complex_float*)
            LAPACKE_malloc( sizeof(lapack_complex_float) * lwork );
        if( work == NULL ) {
            info = LAPACK_WORK_MEMORY_ERROR;
            goto exit_level_1;
        }
    }
    /* Call middle-level interface */
    info = LAPACKE_ctgsna_work( matrix_layout, job, howmny, select, n, a, lda, b,
                                ldb, vl, ldvl, vr, ldvr, s, dif, mm, m, work,
                                lwork, iwork );
    /* Release memory and exit */
    if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'v' ) ) {
        LAPACKE_free( work );
    }
exit_level_1:
    if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'v' ) ) {
        LAPACKE_free( iwork );
    }
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_ctgsna", info );
    }
    return info;
}
