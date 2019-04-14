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
* Contents: Native middle-level C interface to LAPACK function ctrsen
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_ctrsen_work( int matrix_layout, char job, char compq,
                                const lapack_logical* select, lapack_int n,
                                lapack_complex_float* t, lapack_int ldt,
                                lapack_complex_float* q, lapack_int ldq,
                                lapack_complex_float* w, lapack_int* m,
                                float* s, float* sep,
                                lapack_complex_float* work, lapack_int lwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_ctrsen( &job, &compq, select, &n, t, &ldt, q, &ldq, w, m, s, sep,
                       work, &lwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldq_t = MAX(1,n);
        lapack_int ldt_t = MAX(1,n);
        lapack_complex_float* t_t = NULL;
        lapack_complex_float* q_t = NULL;
        /* Check leading dimension(s) */
        if( ldq < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_ctrsen_work", info );
            return info;
        }
        if( ldt < n ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_ctrsen_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( lwork == -1 ) {
            LAPACK_ctrsen( &job, &compq, select, &n, t, &ldt_t, q, &ldq_t, w, m,
                           s, sep, work, &lwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        t_t = (lapack_complex_float*)
            LAPACKE_malloc( sizeof(lapack_complex_float) * ldt_t * MAX(1,n) );
        if( t_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( compq, 'v' ) ) {
            q_t = (lapack_complex_float*)
                LAPACKE_malloc( sizeof(lapack_complex_float) *
                                ldq_t * MAX(1,n) );
            if( q_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        /* Transpose input matrices */
        LAPACKE_cge_trans( matrix_layout, n, n, t, ldt, t_t, ldt_t );
        if( LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_cge_trans( matrix_layout, n, n, q, ldq, q_t, ldq_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_ctrsen( &job, &compq, select, &n, t_t, &ldt_t, q_t, &ldq_t, w, m,
                       s, sep, work, &lwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_cge_trans( LAPACK_COL_MAJOR, n, n, t_t, ldt_t, t, ldt );
        if( LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_cge_trans( LAPACK_COL_MAJOR, n, n, q_t, ldq_t, q, ldq );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( compq, 'v' ) ) {
            LAPACKE_free( q_t );
        }
exit_level_1:
        LAPACKE_free( t_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_ctrsen_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_ctrsen_work", info );
    }
    return info;
}
