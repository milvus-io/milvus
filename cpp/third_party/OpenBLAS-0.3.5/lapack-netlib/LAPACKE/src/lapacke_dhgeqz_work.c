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
* Contents: Native middle-level C interface to LAPACK function dhgeqz
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dhgeqz_work( int matrix_layout, char job, char compq,
                                char compz, lapack_int n, lapack_int ilo,
                                lapack_int ihi, double* h, lapack_int ldh,
                                double* t, lapack_int ldt, double* alphar,
                                double* alphai, double* beta, double* q,
                                lapack_int ldq, double* z, lapack_int ldz,
                                double* work, lapack_int lwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dhgeqz( &job, &compq, &compz, &n, &ilo, &ihi, h, &ldh, t, &ldt,
                       alphar, alphai, beta, q, &ldq, z, &ldz, work, &lwork,
                       &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldh_t = MAX(1,n);
        lapack_int ldq_t = MAX(1,n);
        lapack_int ldt_t = MAX(1,n);
        lapack_int ldz_t = MAX(1,n);
        double* h_t = NULL;
        double* t_t = NULL;
        double* q_t = NULL;
        double* z_t = NULL;
        /* Check leading dimension(s) */
        if( ldh < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_dhgeqz_work", info );
            return info;
        }
        if( ldq < n ) {
            info = -16;
            LAPACKE_xerbla( "LAPACKE_dhgeqz_work", info );
            return info;
        }
        if( ldt < n ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_dhgeqz_work", info );
            return info;
        }
        if( ldz < n ) {
            info = -18;
            LAPACKE_xerbla( "LAPACKE_dhgeqz_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_dhgeqz( &job, &compq, &compz, &n, &ilo, &ihi, h, &ldh_t, t,
                           &ldt_t, alphar, alphai, beta, q, &ldq_t, z, &ldz_t,
                           work, &lwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        h_t = (double*)LAPACKE_malloc( sizeof(double) * ldh_t * MAX(1,n) );
        if( h_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        t_t = (double*)LAPACKE_malloc( sizeof(double) * ldt_t * MAX(1,n) );
        if( t_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        if( LAPACKE_lsame( compq, 'i' ) || LAPACKE_lsame( compq, 'v' ) ) {
            q_t = (double*)LAPACKE_malloc( sizeof(double) * ldq_t * MAX(1,n) );
            if( q_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( LAPACKE_lsame( compz, 'i' ) || LAPACKE_lsame( compz, 'v' ) ) {
            z_t = (double*)LAPACKE_malloc( sizeof(double) * ldz_t * MAX(1,n) );
            if( z_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        /* Transpose input matrices */
        LAPACKE_dge_trans( matrix_layout, n, n, h, ldh, h_t, ldh_t );
        LAPACKE_dge_trans( matrix_layout, n, n, t, ldt, t_t, ldt_t );
        if( LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_dge_trans( matrix_layout, n, n, q, ldq, q_t, ldq_t );
        }
        if( LAPACKE_lsame( compz, 'v' ) ) {
            LAPACKE_dge_trans( matrix_layout, n, n, z, ldz, z_t, ldz_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_dhgeqz( &job, &compq, &compz, &n, &ilo, &ihi, h_t, &ldh_t, t_t,
                       &ldt_t, alphar, alphai, beta, q_t, &ldq_t, z_t, &ldz_t,
                       work, &lwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, h_t, ldh_t, h, ldh );
        LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, t_t, ldt_t, t, ldt );
        if( LAPACKE_lsame( compq, 'i' ) || LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, q_t, ldq_t, q, ldq );
        }
        if( LAPACKE_lsame( compz, 'i' ) || LAPACKE_lsame( compz, 'v' ) ) {
            LAPACKE_dge_trans( LAPACK_COL_MAJOR, n, n, z_t, ldz_t, z, ldz );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( compz, 'i' ) || LAPACKE_lsame( compz, 'v' ) ) {
            LAPACKE_free( z_t );
        }
exit_level_3:
        if( LAPACKE_lsame( compq, 'i' ) || LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_free( q_t );
        }
exit_level_2:
        LAPACKE_free( t_t );
exit_level_1:
        LAPACKE_free( h_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dhgeqz_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dhgeqz_work", info );
    }
    return info;
}
