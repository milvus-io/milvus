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
* Contents: Native middle-level C interface to LAPACK function dbdsdc
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dbdsdc_work( int matrix_layout, char uplo, char compq,
                                lapack_int n, double* d, double* e, double* u,
                                lapack_int ldu, double* vt, lapack_int ldvt,
                                double* q, lapack_int* iq, double* work,
                                lapack_int* iwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dbdsdc( &uplo, &compq, &n, d, e, u, &ldu, vt, &ldvt, q, iq, work,
                       iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldu_t = MAX(1,n);
        lapack_int ldvt_t = MAX(1,n);
        double* u_t = NULL;
        double* vt_t = NULL;
        /* Check leading dimension(s) */
        if( ldu < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_dbdsdc_work", info );
            return info;
        }
        if( ldvt < n ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_dbdsdc_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        if( LAPACKE_lsame( compq, 'i' ) ) {
            u_t = (double*)LAPACKE_malloc( sizeof(double) * ldu_t * MAX(1,n) );
            if( u_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_0;
            }
        }
        if( LAPACKE_lsame( compq, 'i' ) ) {
            vt_t = (double*)
                LAPACKE_malloc( sizeof(double) * ldvt_t * MAX(1,n) );
            if( vt_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        /* Call LAPACK function and adjust info */
        LAPACK_dbdsdc( &uplo, &compq, &n, d, e, u_t, &ldu_t, vt_t, &ldvt_t, q,
                       iq, work, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( compq, 'i' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, u_t, ldu_t, u, ldu );
        }
        if( LAPACKE_lsame( compq, 'i' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, vt_t, ldvt_t, vt, ldvt );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( compq, 'i' ) ) {
            LAPACKE_free( vt_t );
        }
exit_level_1:
        if( LAPACKE_lsame( compq, 'i' ) ) {
            LAPACKE_free( u_t );
        }
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dbdsdc_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dbdsdc_work", info );
    }
    return info;
}
