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
* Contents: Native middle-level C interface to LAPACK function ssysvxx
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_ssysvxx_work( int matrix_layout, char fact, char uplo,
                                 lapack_int n, lapack_int nrhs, float* a,
                                 lapack_int lda, float* af, lapack_int ldaf,
                                 lapack_int* ipiv, char* equed, float* s,
                                 float* b, lapack_int ldb, float* x,
                                 lapack_int ldx, float* rcond, float* rpvgrw,
                                 float* berr, lapack_int n_err_bnds,
                                 float* err_bnds_norm, float* err_bnds_comp,
                                 lapack_int nparams, float* params, float* work,
                                 lapack_int* iwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_ssysvxx( &fact, &uplo, &n, &nrhs, a, &lda, af, &ldaf, ipiv,
                        equed, s, b, &ldb, x, &ldx, rcond, rpvgrw, berr,
                        &n_err_bnds, err_bnds_norm, err_bnds_comp, &nparams,
                        params, work, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,n);
        lapack_int ldaf_t = MAX(1,n);
        lapack_int ldb_t = MAX(1,n);
        lapack_int ldx_t = MAX(1,n);
        float* a_t = NULL;
        float* af_t = NULL;
        float* b_t = NULL;
        float* x_t = NULL;
        float* err_bnds_norm_t = NULL;
        float* err_bnds_comp_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_ssysvxx_work", info );
            return info;
        }
        if( ldaf < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_ssysvxx_work", info );
            return info;
        }
        if( ldb < nrhs ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_ssysvxx_work", info );
            return info;
        }
        if( ldx < nrhs ) {
            info = -16;
            LAPACKE_xerbla( "LAPACKE_ssysvxx_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (float*)LAPACKE_malloc( sizeof(float) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        af_t = (float*)LAPACKE_malloc( sizeof(float) * ldaf_t * MAX(1,n) );
        if( af_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        b_t = (float*)LAPACKE_malloc( sizeof(float) * ldb_t * MAX(1,nrhs) );
        if( b_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_2;
        }
        x_t = (float*)LAPACKE_malloc( sizeof(float) * ldx_t * MAX(1,nrhs) );
        if( x_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_3;
        }
        err_bnds_norm_t = (float*)
            LAPACKE_malloc( sizeof(float) * nrhs * MAX(1,n_err_bnds) );
        if( err_bnds_norm_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_4;
        }
        err_bnds_comp_t = (float*)
            LAPACKE_malloc( sizeof(float) * nrhs * MAX(1,n_err_bnds) );
        if( err_bnds_comp_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_5;
        }
        /* Transpose input matrices */
        LAPACKE_ssy_trans( matrix_layout, uplo, n, a, lda, a_t, lda_t );
        if( LAPACKE_lsame( fact, 'f' ) ) {
            LAPACKE_ssy_trans( matrix_layout, uplo, n, af, ldaf, af_t, ldaf_t );
        }
        LAPACKE_sge_trans( matrix_layout, n, nrhs, b, ldb, b_t, ldb_t );
        /* Call LAPACK function and adjust info */
        LAPACK_ssysvxx( &fact, &uplo, &n, &nrhs, a_t, &lda_t, af_t, &ldaf_t,
                        ipiv, equed, s, b_t, &ldb_t, x_t, &ldx_t, rcond, rpvgrw,
                        berr, &n_err_bnds, err_bnds_norm_t, err_bnds_comp_t,
                        &nparams, params, work, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( fact, 'e' ) && LAPACKE_lsame( *equed, 'y' ) ) {
            LAPACKE_ssy_trans( LAPACK_COL_MAJOR, uplo, n, a_t, lda_t, a, lda );
        }
        if( LAPACKE_lsame( fact, 'e' ) || LAPACKE_lsame( fact, 'n' ) ) {
            LAPACKE_ssy_trans( LAPACK_COL_MAJOR, uplo, n, af_t, ldaf_t, af,
                               ldaf );
        }
        if( LAPACKE_lsame( fact, 'f' ) && LAPACKE_lsame( *equed, 'y' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, nrhs, b_t, ldb_t, b, ldb );
        }
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, nrhs, x_t, ldx_t, x, ldx );
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, nrhs, n_err_bnds, err_bnds_norm_t,
                           nrhs, err_bnds_norm, n_err_bnds );
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, nrhs, n_err_bnds, err_bnds_comp_t,
                           nrhs, err_bnds_comp, n_err_bnds );
        /* Release memory and exit */
        LAPACKE_free( err_bnds_comp_t );
exit_level_5:
        LAPACKE_free( err_bnds_norm_t );
exit_level_4:
        LAPACKE_free( x_t );
exit_level_3:
        LAPACKE_free( b_t );
exit_level_2:
        LAPACKE_free( af_t );
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_ssysvxx_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_ssysvxx_work", info );
    }
    return info;
}
