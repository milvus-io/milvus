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
* Contents: Native middle-level C interface to LAPACK function sbdsqr
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_sbdsqr_work( int matrix_layout, char uplo, lapack_int n,
                                lapack_int ncvt, lapack_int nru, lapack_int ncc,
                                float* d, float* e, float* vt, lapack_int ldvt,
                                float* u, lapack_int ldu, float* c,
                                lapack_int ldc, float* work )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_sbdsqr( &uplo, &n, &ncvt, &nru, &ncc, d, e, vt, &ldvt, u, &ldu,
                       c, &ldc, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldc_t = MAX(1,n);
        lapack_int ldu_t = MAX(1,nru);
        lapack_int ldvt_t = MAX(1,n);
        float* vt_t = NULL;
        float* u_t = NULL;
        float* c_t = NULL;
        /* Check leading dimension(s) */
        if( ldc < ncc ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_sbdsqr_work", info );
            return info;
        }
        if( ldu < n ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_sbdsqr_work", info );
            return info;
        }
        if( ldvt < ncvt ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_sbdsqr_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        if( ncvt != 0 ) {
            vt_t = (float*)
                LAPACKE_malloc( sizeof(float) * ldvt_t * MAX(1,ncvt) );
            if( vt_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_0;
            }
        }
        if( nru != 0 ) {
            u_t = (float*)LAPACKE_malloc( sizeof(float) * ldu_t * MAX(1,n) );
            if( u_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        if( ncc != 0 ) {
            c_t = (float*)LAPACKE_malloc( sizeof(float) * ldc_t * MAX(1,ncc) );
            if( c_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        /* Transpose input matrices */
        if( ncvt != 0 ) {
            LAPACKE_sge_trans( matrix_layout, n, ncvt, vt, ldvt, vt_t, ldvt_t );
        }
        if( nru != 0 ) {
            LAPACKE_sge_trans( matrix_layout, nru, n, u, ldu, u_t, ldu_t );
        }
        if( ncc != 0 ) {
            LAPACKE_sge_trans( matrix_layout, n, ncc, c, ldc, c_t, ldc_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_sbdsqr( &uplo, &n, &ncvt, &nru, &ncc, d, e, vt_t, &ldvt_t, u_t,
                       &ldu_t, c_t, &ldc_t, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( ncvt != 0 ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, ncvt, vt_t, ldvt_t, vt,
                               ldvt );
        }
        if( nru != 0 ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, nru, n, u_t, ldu_t, u, ldu );
        }
        if( ncc != 0 ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, ncc, c_t, ldc_t, c, ldc );
        }
        /* Release memory and exit */
        if( ncc != 0 ) {
            LAPACKE_free( c_t );
        }
exit_level_2:
        if( nru != 0 ) {
            LAPACKE_free( u_t );
        }
exit_level_1:
        if( ncvt != 0 ) {
            LAPACKE_free( vt_t );
        }
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_sbdsqr_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_sbdsqr_work", info );
    }
    return info;
}
