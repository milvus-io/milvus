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
******************************************************************************
* Contents: Native middle-level C interface to LAPACK function dgemqrt
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dgemqrt_work( int matrix_layout, char side, char trans,
                                 lapack_int m, lapack_int n, lapack_int k,
                                 lapack_int nb, const double* v, lapack_int ldv,
                                 const double* t, lapack_int ldt, double* c,
                                 lapack_int ldc, double* work )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_dgemqrt( &side, &trans, &m, &n, &k, &nb, v, &ldv, t, &ldt, c,
                        &ldc, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldc_t = MAX(1,m);
        lapack_int ldt_t = MAX(1,ldt);
        lapack_int ldv_t = MAX(1,ldv);
        double* v_t = NULL;
        double* t_t = NULL;
        double* c_t = NULL;
        /* Check leading dimension(s) */
        if( ldc < n ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_dgemqrt_work", info );
            return info;
        }
        if( ldt < nb ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_dgemqrt_work", info );
            return info;
        }
        if( ldv < k ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_dgemqrt_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        v_t = (double*)LAPACKE_malloc( sizeof(double) * ldv_t * MAX(1,k) );
        if( v_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        t_t = (double*)LAPACKE_malloc( sizeof(double) * ldt_t * MAX(1,nb) );
        if( t_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        c_t = (double*)LAPACKE_malloc( sizeof(double) * ldc_t * MAX(1,n) );
        if( c_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_2;
        }
        /* Transpose input matrices */
        LAPACKE_dge_trans( matrix_layout, ldv, k, v, ldv, v_t, ldv_t );
        LAPACKE_dge_trans( matrix_layout, ldt, nb, t, ldt, t_t, ldt_t );
        LAPACKE_dge_trans( matrix_layout, m, n, c, ldc, c_t, ldc_t );
        /* Call LAPACK function and adjust info */
        LAPACK_dgemqrt( &side, &trans, &m, &n, &k, &nb, v_t, &ldv_t, t_t,
                        &ldt_t, c_t, &ldc_t, work, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_dge_trans( LAPACK_COL_MAJOR, m, n, c_t, ldc_t, c, ldc );
        /* Release memory and exit */
        LAPACKE_free( c_t );
exit_level_2:
        LAPACKE_free( t_t );
exit_level_1:
        LAPACKE_free( v_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_dgemqrt_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_dgemqrt_work", info );
    }
    return info;
}
