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
* Contents: Native middle-level C interface to LAPACK function dgbbrd
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dgbbrd_work( int matrix_layout, char vect, lapack_int m,
                                lapack_int n, lapack_int ncc, lapack_int kl,
                                lapack_int ku, double* ab, lapack_int ldab,
                                double* d, double* e, double* q, lapack_int ldq,
                                double* pt, lapack_int ldpt, double* c,
                                lapack_int ldc, double* work )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dgbbrd( &vect, &m, &n, &ncc, &kl, &ku, ab, &ldab, d, e, q, &ldq,
                       pt, &ldpt, c, &ldc, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldab_t = MAX(1,kl+ku+1);
        lapack_int ldc_t = MAX(1,m);
        lapack_int ldpt_t = MAX(1,n);
        lapack_int ldq_t = MAX(1,m);
        double* ab_t = NULL;
        double* q_t = NULL;
        double* pt_t = NULL;
        double* c_t = NULL;
        /* Check leading dimension(s) */
        if( ldab < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_dgbbrd_work", info );
            return info;
        }
        if( ldc < ncc ) {
            info = -17;
            LAPACKE_xerbla( "LAPACKE_dgbbrd_work", info );
            return info;
        }
        if( ldpt < n ) {
            info = -15;
            LAPACKE_xerbla( "LAPACKE_dgbbrd_work", info );
            return info;
        }
        if( ldq < m ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_dgbbrd_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        ab_t = (double*)LAPACKE_malloc( sizeof(double) * ldab_t * MAX(1,n) );
        if( ab_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( vect, 'b' ) || LAPACKE_lsame( vect, 'q' ) ) {
            q_t = (double*)LAPACKE_malloc( sizeof(double) * ldq_t * MAX(1,m) );
            if( q_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        if( LAPACKE_lsame( vect, 'b' ) || LAPACKE_lsame( vect, 'p' ) ) {
            pt_t = (double*)
                LAPACKE_malloc( sizeof(double) * ldpt_t * MAX(1,n) );
            if( pt_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( ncc != 0 ) {
            c_t = (double*)
                LAPACKE_malloc( sizeof(double) * ldc_t * MAX(1,ncc) );
            if( c_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        /* Transpose input matrices */
        LAPACKE_dgb_trans( matrix_layout, m, n, kl, ku, ab, ldab, ab_t, ldab_t );
        if( ncc != 0 ) {
            LAPACKE_dge_trans( matrix_layout, m, ncc, c, ldc, c_t, ldc_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_dgbbrd( &vect, &m, &n, &ncc, &kl, &ku, ab_t, &ldab_t, d, e, q_t,
                       &ldq_t, pt_t, &ldpt_t, c_t, &ldc_t, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_dgb_trans( LAPACK_COL_MAJOR, m, n, kl, ku, ab_t, ldab_t, ab,
                           ldab );
        if( LAPACKE_lsame( vect, 'b' ) || LAPACKE_lsame( vect, 'q' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, m, m, q_t, ldq_t, q, ldq );
        }
        if( LAPACKE_lsame( vect, 'b' ) || LAPACKE_lsame( vect, 'p' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, pt_t, ldpt_t, pt, ldpt );
        }
        if( ncc != 0 ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, m, ncc, c_t, ldc_t, c, ldc );
        }
        /* Release memory and exit */
        if( ncc != 0 ) {
            LAPACKE_free( c_t );
        }
exit_level_3:
        if( LAPACKE_lsame( vect, 'b' ) || LAPACKE_lsame( vect, 'p' ) ) {
            LAPACKE_free( pt_t );
        }
exit_level_2:
        if( LAPACKE_lsame( vect, 'b' ) || LAPACKE_lsame( vect, 'q' ) ) {
            LAPACKE_free( q_t );
        }
exit_level_1:
        LAPACKE_free( ab_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dgbbrd_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dgbbrd_work", info );
    }
    return info;
}
