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
* Contents: Native middle-level C interface to LAPACK function sgesvj
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_sgesvj_work( int matrix_layout, char joba, char jobu,
                                char jobv, lapack_int m, lapack_int n, float* a,
                                lapack_int lda, float* sva, lapack_int mv,
                                float* v, lapack_int ldv, float* work,
                                lapack_int lwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_sgesvj( &joba, &jobu, &jobv, &m, &n, a, &lda, sva, &mv, v, &ldv,
                       work, &lwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int nrows_v = LAPACKE_lsame( jobv, 'v' ) ? MAX(0,n) :
                           ( LAPACKE_lsame( jobv, 'a' ) ? MAX(0,mv) : 0);
        lapack_int lda_t = MAX(1,m);
        lapack_int ldv_t = MAX(1,nrows_v);
        float* a_t = NULL;
        float* v_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_sgesvj_work", info );
            return info;
        }
        if( ldv < n ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_sgesvj_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (float*)LAPACKE_malloc( sizeof(float) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( jobv, 'a' ) || LAPACKE_lsame( jobv, 'v' ) ) {
            v_t = (float*)LAPACKE_malloc( sizeof(float) * ldv_t * MAX(1,n) );
            if( v_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, m, n, a, lda, a_t, lda_t );
        if( LAPACKE_lsame( jobv, 'a' ) ) {
            LAPACKE_sge_trans( matrix_layout, nrows_v, n, v, ldv, v_t, ldv_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_sgesvj( &joba, &jobu, &jobv, &m, &n, a_t, &lda_t, sva, &mv, v_t,
                       &ldv_t, work, &lwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, m, n, a_t, lda_t, a, lda );
        if( LAPACKE_lsame( jobv, 'a' ) || LAPACKE_lsame( jobv, 'v' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, nrows_v, n, v_t, ldv_t, v,
                               ldv );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobv, 'a' ) || LAPACKE_lsame( jobv, 'v' ) ) {
            LAPACKE_free( v_t );
        }
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_sgesvj_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_sgesvj_work", info );
    }
    return info;
}
