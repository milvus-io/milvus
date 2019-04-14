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
* Contents: Native middle-level C interface to LAPACK function ctgevc
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_ctgevc_work( int matrix_layout, char side, char howmny,
                                const lapack_logical* select, lapack_int n,
                                const lapack_complex_float* s, lapack_int lds,
                                const lapack_complex_float* p, lapack_int ldp,
                                lapack_complex_float* vl, lapack_int ldvl,
                                lapack_complex_float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m,
                                lapack_complex_float* work, float* rwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_ctgevc( &side, &howmny, select, &n, s, &lds, p, &ldp, vl, &ldvl,
                       vr, &ldvr, &mm, m, work, rwork, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldp_t = MAX(1,n);
        lapack_int lds_t = MAX(1,n);
        lapack_int ldvl_t = MAX(1,n);
        lapack_int ldvr_t = MAX(1,n);
        lapack_complex_float* s_t = NULL;
        lapack_complex_float* p_t = NULL;
        lapack_complex_float* vl_t = NULL;
        lapack_complex_float* vr_t = NULL;
        /* Check leading dimension(s) */
        if( ldp < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_ctgevc_work", info );
            return info;
        }
        if( lds < n ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_ctgevc_work", info );
            return info;
        }
        if( ldvl < mm ) {
            info = -11;
            LAPACKE_xerbla( "LAPACKE_ctgevc_work", info );
            return info;
        }
        if( ldvr < mm ) {
            info = -13;
            LAPACKE_xerbla( "LAPACKE_ctgevc_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        s_t = (lapack_complex_float*)
            LAPACKE_malloc( sizeof(lapack_complex_float) * lds_t * MAX(1,n) );
        if( s_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        p_t = (lapack_complex_float*)
            LAPACKE_malloc( sizeof(lapack_complex_float) * ldp_t * MAX(1,n) );
        if( p_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        if( LAPACKE_lsame( side, 'b' ) || LAPACKE_lsame( side, 'l' ) ) {
            vl_t = (lapack_complex_float*)
                LAPACKE_malloc( sizeof(lapack_complex_float) *
                                ldvl_t * MAX(1,mm) );
            if( vl_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        if( LAPACKE_lsame( side, 'b' ) || LAPACKE_lsame( side, 'r' ) ) {
            vr_t = (lapack_complex_float*)
                LAPACKE_malloc( sizeof(lapack_complex_float) *
                                ldvr_t * MAX(1,mm) );
            if( vr_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_3;
            }
        }
        /* Transpose input matrices */
        LAPACKE_cge_trans( matrix_layout, n, n, s, lds, s_t, lds_t );
        LAPACKE_cge_trans( matrix_layout, n, n, p, ldp, p_t, ldp_t );
        if( ( LAPACKE_lsame( side, 'l' ) || LAPACKE_lsame( side, 'b' ) ) &&
            LAPACKE_lsame( howmny, 'b' ) ) {
            LAPACKE_cge_trans( matrix_layout, n, mm, vl, ldvl, vl_t, ldvl_t );
        }
        if( ( LAPACKE_lsame( side, 'r' ) || LAPACKE_lsame( side, 'b' ) ) &&
            LAPACKE_lsame( howmny, 'b' ) ) {
            LAPACKE_cge_trans( matrix_layout, n, mm, vr, ldvr, vr_t, ldvr_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_ctgevc( &side, &howmny, select, &n, s_t, &lds_t, p_t, &ldp_t,
                       vl_t, &ldvl_t, vr_t, &ldvr_t, &mm, m, work, rwork,
                       &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( side, 'b' ) || LAPACKE_lsame( side, 'l' ) ) {
            LAPACKE_cge_trans( LAPACK_COL_MAJOR, n, mm, vl_t, ldvl_t, vl,
                               ldvl );
        }
        if( LAPACKE_lsame( side, 'b' ) || LAPACKE_lsame( side, 'r' ) ) {
            LAPACKE_cge_trans( LAPACK_COL_MAJOR, n, mm, vr_t, ldvr_t, vr,
                               ldvr );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( side, 'b' ) || LAPACKE_lsame( side, 'r' ) ) {
            LAPACKE_free( vr_t );
        }
exit_level_3:
        if( LAPACKE_lsame( side, 'b' ) || LAPACKE_lsame( side, 'l' ) ) {
            LAPACKE_free( vl_t );
        }
exit_level_2:
        LAPACKE_free( p_t );
exit_level_1:
        LAPACKE_free( s_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_ctgevc_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_ctgevc_work", info );
    }
    return info;
}
