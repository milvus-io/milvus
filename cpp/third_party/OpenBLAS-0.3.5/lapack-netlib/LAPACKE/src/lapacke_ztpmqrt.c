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
******************************************************************************
* Contents: Native high-level C interface to LAPACK function ztpmqrt
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_ztpmqrt( int matrix_layout, char side, char trans,
                            lapack_int m, lapack_int n, lapack_int k,
                            lapack_int l, lapack_int nb,
                            const lapack_complex_double* v, lapack_int ldv,
                            const lapack_complex_double* t, lapack_int ldt,
                            lapack_complex_double* a, lapack_int lda,
                            lapack_complex_double* b, lapack_int ldb )
{
    lapack_int ncols_a, nrows_a;
    lapack_int nrows_v;
    lapack_int lwork;
    lapack_int info = 0;
    lapack_complex_double* work = NULL;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_ztpmqrt", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        ncols_a = LAPACKE_lsame( side, 'L' ) ? n :
                             ( LAPACKE_lsame( side, 'R' ) ? k : 0 );
        nrows_a = LAPACKE_lsame( side, 'L' ) ? k :
                             ( LAPACKE_lsame( side, 'R' ) ? m : 0 );
        nrows_v = LAPACKE_lsame( side, 'L' ) ? m :
                             ( LAPACKE_lsame( side, 'R' ) ? n : 0 );
        if( LAPACKE_zge_nancheck( matrix_layout, nrows_a, ncols_a, a, lda ) ) {
            return -13;
        }
        if( LAPACKE_zge_nancheck( matrix_layout, m, n, b, ldb ) ) {
            return -15;
        }
        if( LAPACKE_zge_nancheck( matrix_layout, nb, k, t, ldt ) ) {
            return -11;
        }
        if( LAPACKE_zge_nancheck( matrix_layout, nrows_v, k, v, ldv ) ) {
            return -9;
        }
    }
#endif
    /* Allocate memory for working array(s) */
    lwork = LAPACKE_lsame( side, 'L' ) ? MAX(1,nb) * MAX(1,n) :
                       ( LAPACKE_lsame( side, 'R' ) ? MAX(1,m) * MAX(1,nb) : 0 );
    work = (lapack_complex_double*)
        LAPACKE_malloc( sizeof(lapack_complex_double) * lwork );
    if( work == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Call middle-level interface */
    info = LAPACKE_ztpmqrt_work( matrix_layout, side, trans, m, n, k, l, nb, v,
                                 ldv, t, ldt, a, lda, b, ldb, work );
    /* Release memory and exit */
    LAPACKE_free( work );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_ztpmqrt", info );
    }
    return info;
}
