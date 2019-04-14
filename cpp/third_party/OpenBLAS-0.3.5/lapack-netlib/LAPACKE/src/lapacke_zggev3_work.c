/*****************************************************************************
  Copyright (c) 2015, Intel Corp.
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
* Contents: Native middle-level C interface to LAPACK function zggev3
* Author: Intel Corporation
* Generated February, 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zggev3_work( int matrix_layout,
                                char jobvl, char jobvr, lapack_int n,
                                lapack_complex_double* a, lapack_int lda,
                                lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* alpha,
                                lapack_complex_double* beta,
                                lapack_complex_double* vl, lapack_int ldvl,
                                lapack_complex_double* vr, lapack_int ldvr,
                                lapack_complex_double* work, lapack_int lwork,
                                double* rwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_zggev3( &jobvl, &jobvr, &n, a, &lda, b, &ldb, alpha, beta, vl,
                       &ldvl, vr, &ldvr, work, &lwork, rwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int nrows_vl = LAPACKE_lsame( jobvl, 'v' ) ? n : 1;
        lapack_int ncols_vl = LAPACKE_lsame( jobvl, 'v' ) ? n : 1;
        lapack_int nrows_vr = LAPACKE_lsame( jobvr, 'v' ) ? n : 1;
        lapack_int ncols_vr = LAPACKE_lsame( jobvr, 'v' ) ? n : 1;
        lapack_int lda_t = MAX(1,n);
        lapack_int ldb_t = MAX(1,n);
        lapack_int ldvl_t = MAX(1,nrows_vl);
        lapack_int ldvr_t = MAX(1,nrows_vr);
        lapack_complex_double* a_t = NULL;
        lapack_complex_double* b_t = NULL;
        lapack_complex_double* vl_t = NULL;
        lapack_complex_double* vr_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -6;
            LAPACKE_xerbla( "LAPACKE_zggev3_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_zggev3_work", info );
            return info;
        }
        if( ldvl < ncols_vl ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_zggev3_work", info );
            return info;
        }
        if( ldvr < ncols_vr ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_zggev3_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_zggev3( &jobvl, &jobvr, &n, a, &lda_t, b, &ldb_t,
                           alpha, beta, vl, &ldvl_t, vr, &ldvr_t,
                           work, &lwork, rwork, &info );
            return (info < 0) ? (info - 1) : info;
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
        if( LAPACKE_lsame( jobvl, 'v' ) ) {
            vl_t = (lapack_complex_double*)
                LAPACKE_malloc( sizeof(lapack_complex_double) *
                                ldvl_t * MAX(1,ncols_vl) );
            if( vl_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( LAPACKE_lsame( jobvr, 'v' ) ) {
            vr_t = (lapack_complex_double*)
                LAPACKE_malloc( sizeof(lapack_complex_double) *
                                ldvr_t * MAX(1,ncols_vr) );
            if( vr_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        /* Transpose input matrices */
        LAPACKE_zge_trans( matrix_layout, n, n, a, lda, a_t, lda_t );
        LAPACKE_zge_trans( matrix_layout, n, n, b, ldb, b_t, ldb_t );
        /* Call LAPACK function and adjust info */
        LAPACK_zggev3( &jobvl, &jobvr, &n, a_t, &lda_t, b_t, &ldb_t,
                       alpha, beta, vl_t, &ldvl_t, vr_t, &ldvr_t,
                       work, &lwork, rwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, a_t, lda_t, a, lda );
        LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, b_t, ldb_t, b, ldb );
        if( LAPACKE_lsame( jobvl, 'v' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, nrows_vl, ncols_vl, vl_t,
                               ldvl_t, vl, ldvl );
        }
        if( LAPACKE_lsame( jobvr, 'v' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, nrows_vr, ncols_vr, vr_t,
                               ldvr_t, vr, ldvr );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobvr, 'v' ) ) {
            LAPACKE_free( vr_t );
        }
exit_level_3:
        if( LAPACKE_lsame( jobvl, 'v' ) ) {
            LAPACKE_free( vl_t );
        }
exit_level_2:
        LAPACKE_free( b_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_zggev3_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_zggev3_work", info );
    }
    return info;
}
