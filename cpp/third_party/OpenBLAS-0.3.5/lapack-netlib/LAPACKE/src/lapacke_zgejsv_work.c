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
* Contents: Native middle-level C interface to LAPACK function zgejsv
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zgejsv_work( int matrix_layout, char joba, char jobu,
                                char jobv, char jobr, char jobt, char jobp,
                                lapack_int m, lapack_int n, lapack_complex_double* a,
                                lapack_int lda, double* sva, lapack_complex_double* u,
                                lapack_int ldu, lapack_complex_double* v, lapack_int ldv,
                                lapack_complex_double* cwork, lapack_int lwork,
                                double* rwork, lapack_int lrwork,
                                lapack_int* iwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_zgejsv( &joba, &jobu, &jobv, &jobr, &jobt, &jobp, &m, &n, a,
                       &lda, sva, u, &ldu, v, &ldv, cwork, &lwork, rwork, &lrwork,
                       iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int nu = LAPACKE_lsame( jobu, 'n' ) ? 1 : m;
        lapack_int nv = LAPACKE_lsame( jobv, 'n' ) ? 1 : n;
        lapack_int ncols_u = LAPACKE_lsame( jobu, 'n' ) ? 1 :
            LAPACKE_lsame( jobu, 'f' ) ? m : n;
        lapack_int lda_t = MAX(1,m);
        lapack_int ldu_t = MAX(1,nu);
        lapack_int ldv_t = MAX(1,nv);
        lapack_complex_double* a_t = NULL;
        lapack_complex_double* u_t = NULL;
        lapack_complex_double* v_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_zgejsv_work", info );
            return info;
        }
        if( ldu < ncols_u ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_zgejsv_work", info );
            return info;
        }
        if( ldv < n ) {
            info = -16;
            LAPACKE_xerbla( "LAPACKE_zgejsv_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (lapack_complex_double*)
           LAPACKE_malloc( sizeof(lapack_complex_double) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( jobu, 'f' ) || LAPACKE_lsame( jobu, 'u' ) ||
            LAPACKE_lsame( jobu, 'w' ) ) {
            u_t = (lapack_complex_double*)
               LAPACKE_malloc( sizeof(lapack_complex_double) * ldu_t * MAX(1,ncols_u) );
            if( u_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        if( LAPACKE_lsame( jobv, 'j' ) || LAPACKE_lsame( jobv, 'v' ) ||
            LAPACKE_lsame( jobv, 'w' ) ) {
            v_t = (lapack_complex_double*)
               LAPACKE_malloc( sizeof(lapack_complex_double) * ldv_t * MAX(1,n) );
            if( v_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        /* Transpose input matrices */
        LAPACKE_zge_trans( matrix_layout, m, n, a, lda, a_t, lda_t );
        /* Call LAPACK function and adjust info */
        LAPACK_zgejsv( &joba, &jobu, &jobv, &jobr, &jobt, &jobp, &m, &n, a_t,
                       &lda_t, sva, u_t, &ldu_t, v_t, &ldv_t, cwork, &lwork,
                       rwork, &lrwork, iwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( jobu, 'f' ) || LAPACKE_lsame( jobu, 'u' ) ||
            LAPACKE_lsame( jobu, 'w' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, nu, ncols_u, u_t, ldu_t, u, ldu );
        }
        if( LAPACKE_lsame( jobv, 'j' ) || LAPACKE_lsame( jobv, 'v' ) ||
            LAPACKE_lsame( jobv, 'w' ) ) {
            LAPACKE_zge_trans( LAPACK_COL_MAJOR, nv, n, v_t, ldv_t, v, ldv );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobv, 'j' ) || LAPACKE_lsame( jobv, 'v' ) ||
            LAPACKE_lsame( jobv, 'w' ) ) {
            LAPACKE_free( v_t );
        }
exit_level_2:
        if( LAPACKE_lsame( jobu, 'f' ) || LAPACKE_lsame( jobu, 'u' ) ||
            LAPACKE_lsame( jobu, 'w' ) ) {
            LAPACKE_free( u_t );
        }
exit_level_1:
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_zgejsv_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_zgejsv_work", info );
    }
    return info;
}
