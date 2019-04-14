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
* Contents: Native middle-level C interface to LAPACK function dggbal
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dggbal_work( int matrix_layout, char job, lapack_int n,
                                double* a, lapack_int lda, double* b,
                                lapack_int ldb, lapack_int* ilo,
                                lapack_int* ihi, double* lscale, double* rscale,
                                double* work )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dggbal( &job, &n, a, &lda, b, &ldb, ilo, ihi, lscale, rscale,
                       work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,n);
        lapack_int ldb_t = MAX(1,n);
        double* a_t = NULL;
        double* b_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -5;
            LAPACKE_xerbla( "LAPACKE_dggbal_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_dggbal_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            a_t = (double*)LAPACKE_malloc( sizeof(double) * lda_t * MAX(1,n) );
            if( a_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_0;
            }
        }
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            b_t = (double*)LAPACKE_malloc( sizeof(double) * ldb_t * MAX(1,n) );
            if( b_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        /* Transpose input matrices */
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            LAPACKE_dge_trans( matrix_layout, n, n, a, lda, a_t, lda_t );
        }
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            LAPACKE_dge_trans( matrix_layout, n, n, b, ldb, b_t, ldb_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_dggbal( &job, &n, a_t, &lda_t, b_t, &ldb_t, ilo, ihi, lscale,
                       rscale, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, a_t, lda_t, a, lda );
        }
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, b_t, ldb_t, b, ldb );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            LAPACKE_free( b_t );
        }
exit_level_1:
        if( LAPACKE_lsame( job, 'p' ) || LAPACKE_lsame( job, 's' ) ||
            LAPACKE_lsame( job, 'b' ) ) {
            LAPACKE_free( a_t );
        }
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dggbal_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dggbal_work", info );
    }
    return info;
}
