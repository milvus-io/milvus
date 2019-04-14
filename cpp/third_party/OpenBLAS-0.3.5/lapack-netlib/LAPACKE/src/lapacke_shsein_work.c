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
* Contents: Native middle-level C interface to LAPACK function shsein
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_shsein_work( int matrix_layout, char job, char eigsrc,
                                char initv, lapack_logical* select,
                                lapack_int n, const float* h, lapack_int ldh,
                                float* wr, const float* wi, float* vl,
                                lapack_int ldvl, float* vr, lapack_int ldvr,
                                lapack_int mm, lapack_int* m, float* work,
                                lapack_int* ifaill, lapack_int* ifailr )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_shsein( &job, &eigsrc, &initv, select, &n, h, &ldh, wr, wi, vl,
                       &ldvl, vr, &ldvr, &mm, m, work, ifaill, ifailr, &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldh_t = MAX(1,n);
        lapack_int ldvl_t = MAX(1,n);
        lapack_int ldvr_t = MAX(1,n);
        float* h_t = NULL;
        float* vl_t = NULL;
        float* vr_t = NULL;
        /* Check leading dimension(s) */
        if( ldh < n ) {
            info = -8;
            LAPACKE_xerbla( "LAPACKE_shsein_work", info );
            return info;
        }
        if( ldvl < mm ) {
            info = -12;
            LAPACKE_xerbla( "LAPACKE_shsein_work", info );
            return info;
        }
        if( ldvr < mm ) {
            info = -14;
            LAPACKE_xerbla( "LAPACKE_shsein_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        h_t = (float*)LAPACKE_malloc( sizeof(float) * ldh_t * MAX(1,n) );
        if( h_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'l' ) ) {
            vl_t = (float*)LAPACKE_malloc( sizeof(float) * ldvl_t * MAX(1,mm) );
            if( vl_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_1;
            }
        }
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'r' ) ) {
            vr_t = (float*)LAPACKE_malloc( sizeof(float) * ldvr_t * MAX(1,mm) );
            if( vr_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_2;
            }
        }
        /* Transpose input matrices */
        LAPACKE_sge_trans( matrix_layout, n, n, h, ldh, h_t, ldh_t );
        if( ( LAPACKE_lsame( job, 'l' ) || LAPACKE_lsame( job, 'b' ) ) &&
            LAPACKE_lsame( initv, 'v' ) ) {
            LAPACKE_sge_trans( matrix_layout, n, mm, vl, ldvl, vl_t, ldvl_t );
        }
        if( ( LAPACKE_lsame( job, 'r' ) || LAPACKE_lsame( job, 'b' ) ) &&
            LAPACKE_lsame( initv, 'v' ) ) {
            LAPACKE_sge_trans( matrix_layout, n, mm, vr, ldvr, vr_t, ldvr_t );
        }
        /* Call LAPACK function and adjust info */
        LAPACK_shsein( &job, &eigsrc, &initv, select, &n, h_t, &ldh_t, wr, wi,
                       vl_t, &ldvl_t, vr_t, &ldvr_t, &mm, m, work, ifaill,
                       ifailr, &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'l' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, mm, vl_t, ldvl_t, vl,
                               ldvl );
        }
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'r' ) ) {
            LAPACKE_sge_trans( LAPACK_COL_MAJOR, n, mm, vr_t, ldvr_t, vr,
                               ldvr );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'r' ) ) {
            LAPACKE_free( vr_t );
        }
exit_level_2:
        if( LAPACKE_lsame( job, 'b' ) || LAPACKE_lsame( job, 'l' ) ) {
            LAPACKE_free( vl_t );
        }
exit_level_1:
        LAPACKE_free( h_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_shsein_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_shsein_work", info );
    }
    return info;
}
