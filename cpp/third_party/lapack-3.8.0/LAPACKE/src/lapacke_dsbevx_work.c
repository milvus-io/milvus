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
* Contents: Native middle-level C interface to LAPACK function dsbevx
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dsbevx_work( int matrix_layout, char jobz, char range,
                                char uplo, lapack_int n, lapack_int kd,
                                double* ab, lapack_int ldab, double* q,
                                lapack_int ldq, double vl, double vu,
                                lapack_int il, lapack_int iu, double abstol,
                                lapack_int* m, double* w, double* z,
                                lapack_int ldz, double* work, lapack_int* iwork,
                                lapack_int* ifail )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dsbevx( &jobz, &range, &uplo, &n, &kd, ab, &ldab, q, &ldq, &vl,
                       &vu, &il, &iu, &abstol, m, w, z, &ldz, work, iwork,
                       ifail, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ncols_z = ( LAPACKE_lsame( range, 'a' ) ||
                             LAPACKE_lsame( range, 'v' ) ) ? n :
                             ( LAPACKE_lsame( range, 'i' ) ? (iu-il+1) : 1);
        lapack_int ldab_t = MAX(1,kd+1);
        lapack_int ldq_t = MAX(1,n);
        lapack_int ldz_t = MAX(1,n);
        double* ab_t = NULL;
        double* q_t = NULL;
        double* z_t = NULL;
        /* Check leading dimension(s) */
        if( ldab < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_dsbevx_work", info );
            return info;
        }
        if( ldq < n ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_dsbevx_work", info );
            return info;
        }
        if( ldz < ncols_z ) {
            info = -19;
            LAPACKE_xerbla( "LAPACKE_dsbevx_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        ab_t = (double*)LAPACKE_malloc( sizeof(double) * ldab_t * MAX(1,n) );
        if( ab_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            q_t = (double*)LAPACKE_malloc( sizeof(double) * ldq_t * MAX(1,n) );
            if( q_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            z_t = (double*)
                LAPACKE_malloc( sizeof(double) * ldz_t * MAX(1,ncols_z) );
            if( z_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        /* Transpose input matrices */
        LAPACKE_dsb_trans( matrix_layout, uplo, n, kd, ab, ldab, ab_t, ldab_t );
        /* Call LAPACK function and adjust info */
        LAPACK_dsbevx( &jobz, &range, &uplo, &n, &kd, ab_t, &ldab_t, q_t,
                       &ldq_t, &vl, &vu, &il, &iu, &abstol, m, w, z_t, &ldz_t,
                       work, iwork, ifail, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_dsb_trans( LAPACK_COL_MAJOR, uplo, n, kd, ab_t, ldab_t, ab,
                           ldab );
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, q_t, ldq_t, q, ldq );
        }
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, ncols_z, z_t, ldz_t, z,
                               ldz );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_free( z_t );
        }
exit_level_2:
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_free( q_t );
        }
exit_level_1:
        LAPACKE_free( ab_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dsbevx_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dsbevx_work", info );
    }
    return info;
}
