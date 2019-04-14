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
* Contents: Native middle-level C interface to LAPACK function zgghrd
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zgghrd_work( int matrix_layout, char compq, char compz,
                                lapack_int n, lapack_int ilo, lapack_int ihi,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* q, lapack_int ldq,
                                lapack_complex_double* z, lapack_int ldz )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_zgghrd( &compq, &compz, &n, &ilo, &ihi, a, &lda, b, &ldb, q,
                       &ldq, z, &ldz, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,n);
        lapack_int ldb_t = MAX(1,n);
        lapack_int ldq_t = MAX(1,n);
        lapack_int ldz_t = MAX(1,n);
        lapack_complex_double* a_t = NULL;
        lapack_complex_double* b_t = NULL;
        lapack_complex_double* q_t = NULL;
        lapack_complex_double* z_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_zgghrd_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_zgghrd_work", info );
            return info;
        }
        if( ldq < n ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_zgghrd_work", info );
            return info;
        }
        if( ldz < n ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_zgghrd_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (lapack_complex_double*)
            LAPACKE_malloc( sizeof(lapack_complex_double) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        b_t = (lapack_complex_double*)
            LAPACKE_malloc( sizeof(lapack_complex_double) * ldb_t * MAX(1,n) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        if( LAPACKE_lsame( compq, 'i' ) || LAPACKE_lsame( compq, 'v' ) ) {
            q_t = (lapack_complex_double*)
                LAPACKE_malloc( sizeof(lapack_complex_double) *
                                ldq_t * MAX(1,n) );
            if( q_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( LAPACKE_lsame( compz, 'i' ) || LAPACKE_lsame( compz, 'v' ) ) {
            z_t = (lapack_complex_double*)
                LAPACKE_malloc( sizeof(lapack_complex_double) *
                                ldz_t * MAX(1,n) );
            if( z_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        /* Transpose input matrices */
        LAPACKE_zge_trans( matrix_layout, n, n, a, lda, a_t, lda_t );
        LAPACKE_zge_trans( matrix_layout, n, n, b, ldb, b_t, ldb_t );
        if( LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_zge_trans( matrix_layout, n, n, q, ldq, q_t, ldq_t );
        }
        if( LAPACKE_lsame( compz, 'v' ) ) {
            LAPACKE_zge_trans( matrix_layout, n, n, z, ldz, z_t, ldz_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_zgghrd( &compq, &compz, &n, &ilo, &ihi, a_t, &lda_t, b_t, &ldb_t,
                       q_t, &ldq_t, z_t, &ldz_t, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, a_t, lda_t, a, lda );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, b_t, ldb_t, b, ldb );
        if( LAPACKE_lsame( compq, 'i' ) || LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, q_t, ldq_t, q, ldq );
        }
        if( LAPACKE_lsame( compz, 'i' ) || LAPACKE_lsame( compz, 'v' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, z_t, ldz_t, z, ldz );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( compz, 'i' ) || LAPACKE_lsame( compz, 'v' ) ) {
            LAPACKE_free( z_t );
        }
exit_level_3:
        if( LAPACKE_lsame( compq, 'i' ) || LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_free( q_t );
        }
exit_level_2:
        LAPACKE_free( b_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_zgghrd_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_zgghrd_work", info );
    }
    return info;
}
