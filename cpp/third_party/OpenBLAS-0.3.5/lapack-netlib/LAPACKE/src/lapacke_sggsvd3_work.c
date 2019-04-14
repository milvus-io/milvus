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
* Contents: Native middle-level C interface to LAPACK function sggsvd3
* Author: Intel Corporation
* Generated August, 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_sggsvd3_work( int matrix_layout, char jobu, char jobv,
                                 char jobq, lapack_int m, lapack_int n,
                                 lapack_int p, lapack_int* k, lapack_int* l,
                                 float* a, lapack_int lda, float* b,
                                 lapack_int ldb, float* alpha, float* beta,
                                 float* u, lapack_int ldu, float* v,
                                 lapack_int ldv, float* q, lapack_int ldq,
                                 float* work, lapack_int lwork,
                                 lapack_int* iwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_sggsvd3( &jobu, &jobv, &jobq, &m, &n, &p, k, l, a, &lda,
                        b, &ldb, alpha, beta, u, &ldu, v, &ldv, q, &ldq,
                        work, &lwork, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,m);
        lapack_int ldb_t = MAX(1,p);
        lapack_int ldq_t = MAX(1,n);
        lapack_int ldu_t = MAX(1,m);
        lapack_int ldv_t = MAX(1,p);
        float* a_t = NULL;
        float* b_t = NULL;
        float* u_t = NULL;
        float* v_t = NULL;
        float* q_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
            return info;
        }
        if( ldq < n ) {
            info = -21;
            LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
            return info;
        }
        if( ldu < m ) {
            info = -17;
            LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
            return info;
        }
        if( ldv < p ) {
            info = -19;
            LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
          LAPACK_sggsvd3( &jobu, &jobv, &jobq, &m, &n, &p, k, l, a, &lda_t,
                          b, &ldb_t, alpha, beta, u, &ldu_t, v, &ldv_t,
                          q, &ldq_t, work, &lwork, iwork, &info );
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
        if( LAPACKE_lsame( jobu, 'u' ) ) {
            u_t = (float*)LAPACKE_malloc( sizeof(float) * ldu_t * MAX(1,m) );
            if( u_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( LAPACKE_lsame( jobv, 'v' ) ) {
            v_t = (float*)LAPACKE_malloc( sizeof(float) * ldv_t * MAX(1,p) );
            if( v_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        if( LAPACKE_lsame( jobq, 'q' ) ) {
            q_t = (float*)LAPACKE_malloc( sizeof(float) * ldq_t * MAX(1,n) );
            if( q_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_4;
            }
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, m, n, a, lda, a_t, lda_t );
        LAPACKE_sge_trans( matrix_layout, p, n, b, ldb, b_t, ldb_t );
        /* Call LAPACK function and adjust info */
        LAPACK_sggsvd3( &jobu, &jobv, &jobq, &m, &n, &p, k, l, a_t, &lda_t, b_t,
                        &ldb_t, alpha, beta, u_t, &ldu_t, v_t, &ldv_t, q_t,
                        &ldq_t, work, &lwork, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, m, n, a_t, lda_t, a, lda );
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, p, n, b_t, ldb_t, b, ldb );
        if( LAPACKE_lsame( jobu, 'u' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, m, m, u_t, ldu_t, u, ldu );
        }
        if( LAPACKE_lsame( jobv, 'v' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, p, p, v_t, ldv_t, v, ldv );
        }
        if( LAPACKE_lsame( jobq, 'q' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, n, q_t, ldq_t, q, ldq );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobq, 'q' ) ) {
            LAPACKE_free( q_t );
        }
exit_level_4:
        if( LAPACKE_lsame( jobv, 'v' ) ) {
            LAPACKE_free( v_t );
        }
exit_level_3:
        if( LAPACKE_lsame( jobu, 'u' ) ) {
            LAPACKE_free( u_t );
        }
exit_level_2:
        LAPACKE_free( b_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_sggsvd3_work", info );
    }
    return info;
}
