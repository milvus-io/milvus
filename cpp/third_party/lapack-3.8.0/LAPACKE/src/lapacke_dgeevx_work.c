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
* Contents: Native middle-level C interface to LAPACK function dgeevx
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dgeevx_work( int matrix_layout, char balanc, char jobvl,
                                char jobvr, char sense, lapack_int n, double* a,
                                lapack_int lda, double* wr, double* wi,
                                double* vl, lapack_int ldvl, double* vr,
                                lapack_int ldvr, lapack_int* ilo,
                                lapack_int* ihi, double* scale, double* abnrm,
                                double* rconde, double* rcondv, double* work,
                                lapack_int lwork, lapack_int* iwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dgeevx( &balanc, &jobvl, &jobvr, &sense, &n, a, &lda, wr, wi, vl,
                       &ldvl, vr, &ldvr, ilo, ihi, scale, abnrm, rconde, rcondv,
                       work, &lwork, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int lda_t = MAX(1,n);
        lapack_int ldvl_t = MAX(1,n);
        lapack_int ldvr_t = MAX(1,n);
        double* a_t = NULL;
        double* vl_t = NULL;
        double* vr_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_dgeevx_work", info );
            return info;
        }
        if( ldvl < n ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_dgeevx_work", info );
            return info;
        }
        if( ldvr < n ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_dgeevx_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_dgeevx( &balanc, &jobvl, &jobvr, &sense, &n, a, &lda_t, wr,
                           wi, vl, &ldvl_t, vr, &ldvr_t, ilo, ihi, scale, abnrm,
                           rconde, rcondv, work, &lwork, iwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (double*)LAPACKE_malloc( sizeof(double) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( jobvl, 'v' ) ) {
            vl_t = (double*)
                LAPACKE_malloc( sizeof(double) * ldvl_t * MAX(1,n) );
            if( vl_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        if( LAPACKE_lsame( jobvr, 'v' ) ) {
            vr_t = (double*)
                LAPACKE_malloc( sizeof(double) * ldvr_t * MAX(1,n) );
            if( vr_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        /* Transpose input matrices */
        LAPACKE_dge_trans( matrix_layout, n, n, a, lda, a_t, lda_t );
        /* Call LAPACK function and adjust info */
        LAPACK_dgeevx( &balanc, &jobvl, &jobvr, &sense, &n, a_t, &lda_t, wr, wi,
                       vl_t, &ldvl_t, vr_t, &ldvr_t, ilo, ihi, scale, abnrm,
                       rconde, rcondv, work, &lwork, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, a_t, lda_t, a, lda );
        if( LAPACKE_lsame( jobvl, 'v' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, vl_t, ldvl_t, vl, ldvl );
        }
        if( LAPACKE_lsame( jobvr, 'v' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, vr_t, ldvr_t, vr, ldvr );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobvr, 'v' ) ) {
            LAPACKE_free( vr_t );
        }
exit_level_2:
        if( LAPACKE_lsame( jobvl, 'v' ) ) {
            LAPACKE_free( vl_t );
        }
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dgeevx_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dgeevx_work", info );
    }
    return info;
}
