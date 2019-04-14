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
* Contents: Native middle-level C interface to LAPACK function dsfrk
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dsfrk_work( int matrix_layout, char transr, char uplo,
                               char trans, lapack_int n, lapack_int k,
                               double alpha, const double* a, lapack_int lda,
                               double beta, double* c )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dsfrk( &transr, &uplo, &trans, &n, &k, &alpha, a, &lda, &beta,
                      c );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int na = LAPACKE_lsame( trans, 'n' ) ? n : k;
        lapack_int ka = LAPACKE_lsame( trans, 'n' ) ? k : n;
        lapack_int lda_t = MAX(1,na);
        double* a_t = NULL;
        double* c_t = NULL;
        /* Check leading dimension(s) */
        if( lda < ka ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_dsfrk_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (double*)LAPACKE_malloc( sizeof(double) * lda_t * MAX(1,ka) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        c_t = (double*)
            LAPACKE_malloc( sizeof(double) * ( MAX(1,n) * MAX(2,n+1) ) / 2 );
        if( c_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        /* Transpose input matrices */
        LAPACKE_dge_trans( matrix_layout, na, ka, a, lda, a_t, lda_t );
        LAPACKE_dpf_trans( matrix_layout, transr, uplo, n, c, c_t );
        /* Call LAPACK function and adjust info */
        LAPACK_dsfrk( &transr, &uplo, &trans, &n, &k, &alpha, a_t, &lda_t,
                      &beta, c_t );
        info = 0;  /* LAPACK call is ok! */
        /* Transpose output matrices */
        LAPACKE_dpf_trans( LAPACK_COL_MAJOR, transr, uplo, n, c_t, c );
        /* Release memory and exit */
        LAPACKE_free( c_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dsfrk_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dsfrk_work", info );
    }
    return info;
}
