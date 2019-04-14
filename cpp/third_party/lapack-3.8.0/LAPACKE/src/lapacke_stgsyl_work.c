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
* Contents: Native middle-level C interface to LAPACK function stgsyl
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_stgsyl_work( int matrix_layout, char trans, lapack_int ijob,
                                lapack_int m, lapack_int n, const float* a,
                                lapack_int lda, const float* b, lapack_int ldb,
                                float* c, lapack_int ldc, const float* d,
                                lapack_int ldd, const float* e, lapack_int lde,
                                float* f, lapack_int ldf, float* scale,
                                float* dif, float* work, lapack_int lwork,
                                lapack_int* iwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_stgsyl( &trans, &ijob, &m, &n, a, &lda, b, &ldb, c, &ldc, d,
                       &ldd, e, &lde, f, &ldf, scale, dif, work, &lwork, iwork,
                       &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,m);
        lapack_int ldb_t = MAX(1,n);
        lapack_int ldc_t = MAX(1,m);
        lapack_int ldd_t = MAX(1,m);
        lapack_int lde_t = MAX(1,n);
        lapack_int ldf_t = MAX(1,m);
        float* a_t = NULL;
        float* b_t = NULL;
        float* c_t = NULL;
        float* d_t = NULL;
        float* e_t = NULL;
        float* f_t = NULL;
        /* Check leading dimension(s) */
        if( lda < m ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
            return info;
        }
        if( ldc < n ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
            return info;
        }
        if( ldd < m ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
            return info;
        }
        if( lde < n ) {
            info = -15;
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
            return info;
        }
        if( ldf < n ) {
            info = -17;
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_stgsyl( &trans, &ijob, &m, &n, a, &lda_t, b, &ldb_t, c,
                           &ldc_t, d, &ldd_t, e, &lde_t, f, &ldf_t, scale, dif,
                           work, &lwork, iwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (float*)LAPACKE_malloc( sizeof(float) * lda_t * MAX(1,m) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        b_t = (float*)LAPACKE_malloc( sizeof(float) * ldb_t * MAX(1,n) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        c_t = (float*)LAPACKE_malloc( sizeof(float) * ldc_t * MAX(1,n) );
        if( c_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_2;
        }
        d_t = (float*)LAPACKE_malloc( sizeof(float) * ldd_t * MAX(1,m) );
        if( d_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_3;
        }
        e_t = (float*)LAPACKE_malloc( sizeof(float) * lde_t * MAX(1,n) );
        if( e_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_4;
        }
        f_t = (float*)LAPACKE_malloc( sizeof(float) * ldf_t * MAX(1,n) );
        if( f_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_5;
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, m, m, a, lda, a_t, lda_t );
        LAPACKE_sge_trans( matrix_layout, n, n, b, ldb, b_t, ldb_t );
        LAPACKE_sge_trans( matrix_layout, m, n, c, ldc, c_t, ldc_t );
        LAPACKE_sge_trans( matrix_layout, m, m, d, ldd, d_t, ldd_t );
        LAPACKE_sge_trans( matrix_layout, n, n, e, lde, e_t, lde_t );
        LAPACKE_sge_trans( matrix_layout, m, n, f, ldf, f_t, ldf_t );
        /* Call LAPACK function and adjust info */
        LAPACK_stgsyl( &trans, &ijob, &m, &n, a_t, &lda_t, b_t, &ldb_t, c_t,
                       &ldc_t, d_t, &ldd_t, e_t, &lde_t, f_t, &ldf_t, scale,
                       dif, work, &lwork, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, m, n, c_t, ldc_t, c, ldc );
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, m, n, f_t, ldf_t, f, ldf );
        /* Release memory and exit */
        LAPACKE_free( f_t );
exit_level_5:
        LAPACKE_free( e_t );
exit_level_4:
        LAPACKE_free( d_t );
exit_level_3:
        LAPACKE_free( c_t );
exit_level_2:
        LAPACKE_free( b_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_stgsyl_work", info );
    }
    return info;
}
