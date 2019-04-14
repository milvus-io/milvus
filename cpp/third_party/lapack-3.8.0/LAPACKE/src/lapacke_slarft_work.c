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
* Contents: Native middle-level C interface to LAPACK function slarft
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_slarft_work( int matrix_layout, char direct, char storev,
                                lapack_int n, lapack_int k, const float* v,
                                lapack_int ldv, const float* tau, float* t,
                                lapack_int ldt )
{
    lapack_int info = 0;
    lapack_int nrows_v, ncols_v;
    lapack_int ldt_t, ldv_t;
    float *v_t = NULL, *t_t = NULL;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_slarft( &direct, &storev, &n, &k, v, &ldv, tau, t, &ldt );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        nrows_v = LAPACKE_lsame( storev, 'c' ) ? n :
                             ( LAPACKE_lsame( storev, 'r' ) ? k : 1);
        ncols_v = LAPACKE_lsame( storev, 'c' ) ? k :
                             ( LAPACKE_lsame( storev, 'r' ) ? n : 1);
        ldt_t = MAX(1,k);
        ldv_t = MAX(1,nrows_v);
        /* Check leading dimension(s) */
        if( ldt < k ) {
            info = -10;
            LAPACKE_xerbla( "LAPACKE_slarft_work", info );
            return info;
        }
        if( ldv < ncols_v ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_slarft_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        v_t = (float*)LAPACKE_malloc( sizeof(float) * ldv_t * MAX(1,ncols_v) );
        if( v_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        t_t = (float*)LAPACKE_malloc( sizeof(float) * ldt_t * MAX(1,k) );
        if( t_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, nrows_v, ncols_v, v, ldv, v_t, ldv_t );
        /* Call LAPACK function and adjust info */
        LAPACK_slarft( &direct, &storev, &n, &k, v_t, &ldv_t, tau, t_t,
                       &ldt_t );
        info = 0;  /* LAPACK call is ok! */
        /* Transpose output matrices */
        LAPACKE_sge_trans( LAPACK_COL_MAJOR, k, k, t_t, ldt_t, t, ldt );
        /* Release memory and exit */
        LAPACKE_free( t_t );
exit_level_1:
        LAPACKE_free( v_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_slarft_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_slarft_work", info );
    }
    return info;
}
