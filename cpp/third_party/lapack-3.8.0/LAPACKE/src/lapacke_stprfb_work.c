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
* Contents: Native middle-level C interface to LAPACK function stprfb
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_stprfb_work( int matrix_layout, char side, char trans,
                                char direct, char storev, lapack_int m,
                                lapack_int n, lapack_int k, lapack_int l,
                                const float* v, lapack_int ldv, const float* t,
                                lapack_int ldt, float* a, lapack_int lda,
                                float* b, lapack_int ldb, float* work,
                                lapack_int ldwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_stprfb( &side, &trans, &direct, &storev, &m, &n, &k, &l, v, &ldv,
                       t, &ldt, a, &lda, b, &ldb, work, &ldwork );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,k);
        lapack_int ldb_t = MAX(1,m);
        lapack_int ldt_t = MAX(1,ldt);
        lapack_int ldv_t = MAX(1,ldv);
        float* v_t = NULL;
        float* t_t = NULL;
        float* a_t = NULL;
        float* b_t = NULL;
        /* Check leading dimension(s) */
        if( lda < m ) {
            info = -15;
            LAPACKE_xerbla( "LAPACKE_stprfb_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -17;
            LAPACKE_xerbla( "LAPACKE_stprfb_work", info );
            return info;
        }
        if( ldt < k ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_stprfb_work", info );
            return info;
        }
        if( ldv < k ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_stprfb_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        v_t = (float*)LAPACKE_malloc( sizeof(float) * ldv_t * MAX(1,k) );
        if( v_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        t_t = (float*)LAPACKE_malloc( sizeof(float) * ldt_t * MAX(1,k) );
        if( t_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        a_t = (float*)LAPACKE_malloc( sizeof(float) * lda_t * MAX(1,m) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_2;
        }
        b_t = (float*)LAPACKE_malloc( sizeof(float) * ldb_t * MAX(1,n) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_3;
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, ldv, k, v, ldv, v_t, ldv_t );
        LAPACKE_sge_trans( matrix_layout, ldt, k, t, ldt, t_t, ldt_t );
        LAPACKE_sge_trans( matrix_layout, k, m, a, lda, a_t, lda_t );
        LAPACKE_sge_trans( matrix_layout, m, n, b, ldb, b_t, ldb_t );
        /* Call LAPACK function and adjust info */
        LAPACK_stprfb( &side, &trans, &direct, &storev, &m, &n, &k, &l, v_t,
                       &ldv_t, t_t, &ldt_t, a_t, &lda_t, b_t, &ldb_t, work,
                       &ldwork );
        info = 0;  /* LAPACK call is ok! */
        /* Transpose output matrices */
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, k, m, a_t, lda_t, a, lda );
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, m, n, b_t, ldb_t, b, ldb );
        /* Release memory and exit */
        LAPACKE_free( b_t );
exit_level_3:
        LAPACKE_free( a_t );
exit_level_2:
        LAPACKE_free( t_t );
exit_level_1:
        LAPACKE_free( v_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_stprfb_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_stprfb_work", info );
    }
    return info;
}
