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
* Contents: Native middle-level C interface to LAPACK function sgges
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_sgges_work( int matrix_layout, char jobvsl, char jobvsr,
                               char sort, LAPACK_S_SELECT3 selctg, lapack_int n,
                               float* a, lapack_int lda, float* b,
                               lapack_int ldb, lapack_int* sdim, float* alphar,
                               float* alphai, float* beta, float* vsl,
                               lapack_int ldvsl, float* vsr, lapack_int ldvsr,
                               float* work, lapack_int lwork,
                               lapack_logical* bwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_sgges( &jobvsl, &jobvsr, &sort, selctg, &n, a, &lda, b, &ldb,
                      sdim, alphar, alphai, beta, vsl, &ldvsl, vsr, &ldvsr,
                      work, &lwork, bwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,n);
        lapack_int ldb_t = MAX(1,n);
        lapack_int ldvsl_t = MAX(1,n);
        lapack_int ldvsr_t = MAX(1,n);
        float* a_t = NULL;
        float* b_t = NULL;
        float* vsl_t = NULL;
        float* vsr_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_sgges_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_sgges_work", info );
            return info;
        }
        if( ldvsl < n ) {
            info = -16;
            LAPACKE_xerbla( "LAPACKE_sgges_work", info );
            return info;
        }
        if( ldvsr < n ) {
            info = -18;
            LAPACKE_xerbla( "LAPACKE_sgges_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_sgges( &jobvsl, &jobvsr, &sort, selctg, &n, a, &lda_t, b,
                          &ldb_t, sdim, alphar, alphai, beta, vsl, &ldvsl_t,
                          vsr, &ldvsr_t, work, &lwork, bwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (float*)LAPACKE_malloc( sizeof(float) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        b_t = (float*)LAPACKE_malloc( sizeof(float) * ldb_t * MAX(1,n) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        if( LAPACKE_lsame( jobvsl, 'v' ) ) {
            vsl_t = (float*)
                LAPACKE_malloc( sizeof(float) * ldvsl_t * MAX(1,n) );
            if( vsl_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( LAPACKE_lsame( jobvsr, 'v' ) ) {
            vsr_t = (float*)
                LAPACKE_malloc( sizeof(float) * ldvsr_t * MAX(1,n) );
            if( vsr_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, n, n, a, lda, a_t, lda_t );
        LAPACKE_sge_trans( matrix_layout, n, n, b, ldb, b_t, ldb_t );
        /* Call LAPACK function and adjust info */
        LAPACK_sgges( &jobvsl, &jobvsr, &sort, selctg, &n, a_t, &lda_t, b_t,
                      &ldb_t, sdim, alphar, alphai, beta, vsl_t, &ldvsl_t,
                      vsr_t, &ldvsr_t, work, &lwork, bwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, n, a_t, lda_t, a, lda );
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, n, b_t, ldb_t, b, ldb );
        if( LAPACKE_lsame( jobvsl, 'v' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, n, vsl_t, ldvsl_t, vsl,
                               ldvsl );
        }
        if( LAPACKE_lsame( jobvsr, 'v' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, n, vsr_t, ldvsr_t, vsr,
                               ldvsr );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobvsr, 'v' ) ) {
            LAPACKE_free( vsr_t );
        }
exit_level_3:
        if( LAPACKE_lsame( jobvsl, 'v' ) ) {
            LAPACKE_free( vsl_t );
        }
exit_level_2:
        LAPACKE_free( b_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_sgges_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_sgges_work", info );
    }
    return info;
}
