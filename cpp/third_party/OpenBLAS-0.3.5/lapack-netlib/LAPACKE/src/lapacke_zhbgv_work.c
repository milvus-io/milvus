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
* Contents: Native middle-level C interface to LAPACK function zhbgv
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zhbgv_work( int matrix_layout, char jobz, char uplo,
                               lapack_int n, lapack_int ka, lapack_int kb,
                               lapack_complex_double* ab, lapack_int ldab,
                               lapack_complex_double* bb, lapack_int ldbb,
                               double* w, lapack_complex_double* z,
                               lapack_int ldz, lapack_complex_double* work,
                               double* rwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_zhbgv( &jobz, &uplo, &n, &ka, &kb, ab, &ldab, bb, &ldbb, w, z,
                      &ldz, work, rwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldab_t = MAX(1,ka+1);
        lapack_int ldbb_t = MAX(1,kb+1);
        lapack_int ldz_t = MAX(1,n);
        lapack_complex_double* ab_t = NULL;
        lapack_complex_double* bb_t = NULL;
        lapack_complex_double* z_t = NULL;
        /* Check leading dimension(s) */
        if( ldab < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_zhbgv_work", info );
            return info;
        }
        if( ldbb < n ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_zhbgv_work", info );
            return info;
        }
        if( ldz < n ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_zhbgv_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        ab_t = (lapack_complex_double*)
            LAPACKE_malloc( sizeof(lapack_complex_double) * ldab_t * MAX(1,n) );
        if( ab_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        bb_t = (lapack_complex_double*)
            LAPACKE_malloc( sizeof(lapack_complex_double) * ldbb_t * MAX(1,n) );
        if( bb_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            z_t = (lapack_complex_double*)
                LAPACKE_malloc( sizeof(lapack_complex_double) *
                                ldz_t * MAX(1,n) );
            if( z_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        /* Transpose input matrices */
        LAPACKE_zhb_trans( matrix_layout, uplo, n, ka, ab, ldab, ab_t, ldab_t );
        LAPACKE_zhb_trans( matrix_layout, uplo, n, kb, bb, ldbb, bb_t, ldbb_t );
        /* Call LAPACK function and adjust info */
        LAPACK_zhbgv( &jobz, &uplo, &n, &ka, &kb, ab_t, &ldab_t, bb_t, &ldbb_t,
                      w, z_t, &ldz_t, work, rwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_zhb_trans( LAPACK_COL_MAJOR, uplo, n, ka, ab_t, ldab_t, ab,
                           ldab );
        LAPACKE_zhb_trans( LAPACK_COL_MAJOR, uplo, n, kb, bb_t, ldbb_t, bb,
                           ldbb );
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, n, n, z_t, ldz_t, z, ldz );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_free( z_t );
        }
exit_level_2:
        LAPACKE_free( bb_t );
exit_level_1:
        LAPACKE_free( ab_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_zhbgv_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_zhbgv_work", info );
    }
    return info;
}
