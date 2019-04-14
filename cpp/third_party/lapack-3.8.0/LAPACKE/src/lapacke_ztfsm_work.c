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
* Contents: Native middle-level C interface to LAPACK function ztfsm
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_ztfsm_work( int matrix_layout, char transr, char side,
                               char uplo, char trans, char diag, lapack_int m,
                               lapack_int n, lapack_complex_double alpha,
                               const lapack_complex_double* a,
                               lapack_complex_double* b, lapack_int ldb )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_ztfsm( &transr, &side, &uplo, &trans, &diag, &m, &n, &alpha, a,
                      b, &ldb );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldb_t = MAX(1,m);
        lapack_complex_double* b_t = NULL;
        lapack_complex_double* a_t = NULL;
        /* Check leading dimension(s) */
        if( ldb < n ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_ztfsm_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        b_t = (lapack_complex_double*)
            LAPACKE_malloc( sizeof(lapack_complex_double) * ldb_t * MAX(1,n) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( IS_Z_NONZERO(alpha) ) {
            a_t = (lapack_complex_double*)
                LAPACKE_malloc( sizeof(lapack_complex_double) *
                                ( MAX(1,n) * MAX(2,n+1) ) / 2 );
            if( a_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        /* Transpose input matrices */
        if( IS_Z_NONZERO(alpha) ) {
            LAPACKE_zge_trans( matrix_layout, m, n, b, ldb, b_t, ldb_t );
        }
        if( IS_Z_NONZERO(alpha) ) {
            LAPACKE_ztf_trans( matrix_layout, transr, uplo, diag, n, a, a_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_ztfsm( &transr, &side, &uplo, &trans, &diag, &m, &n, &alpha, a_t,
                      b_t, &ldb_t );
        info = 0;  /* LAPACK call is ok! */
        /* Transpose output matrices */
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, m, n, b_t, ldb_t, b, ldb );
        /* Release memory and exit */
        if( IS_Z_NONZERO(alpha) ) {
            LAPACKE_free( a_t );
        }
exit_level_1:
        LAPACKE_free( b_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_ztfsm_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_ztfsm_work", info );
    }
    return info;
}
