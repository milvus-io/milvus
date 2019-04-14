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
* Contents: Native middle-level C interface to LAPACK function cstegr
* Author: Intel Corporation
* Generated November 2015
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_cstegr_work( int matrix_layout, char jobz, char range,
                                lapack_int n, float* d, float* e, float vl,
                                float vu, lapack_int il, lapack_int iu,
                                float abstol, lapack_int* m, float* w,
                                lapack_complex_float* z, lapack_int ldz,
                                lapack_int* isuppz, float* work,
                                lapack_int lwork, lapack_int* iwork,
                                lapack_int liwork )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_cstegr( &jobz, &range, &n, d, e, &vl, &vu, &il, &iu, &abstol, m,
                       w, z, &ldz, isuppz, work, &lwork, iwork, &liwork,
                       &info );
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int ldz_t = MAX(1,n);
        lapack_complex_float* z_t = NULL;
        /* Check leading dimension(s) */
        if( ( LAPACKE_lsame( jobz, 'v' ) && ( ldz < ldz_t )  ) || ( ldz < 1 ) ) {
            info = -15;
            LAPACKE_xerbla( "LAPACKE_cstegr_work", info );
            return info;
        }
        /* Query optimal working array(s) size if requested */
        if( liwork == -1 || lwork == -1 ) {
            LAPACK_cstegr( &jobz, &range, &n, d, e, &vl, &vu, &il, &iu, &abstol,
                           m, w, z, &ldz_t, isuppz, work, &lwork, iwork,
                           &liwork, &info );
            return (info < 0) ? (info - 1) : info;
        }
        /* Allocate memory for temporary array(s) */
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            /* Let be always 'n' instead of 'm' */
            z_t = (lapack_complex_float*)
                LAPACKE_malloc( sizeof(lapack_complex_float) *
                                ldz_t * MAX(1,n) );
            if( z_t == NULL ) {
                info = LAPACK_TRANSPOSE_MEMORY_ERROR;
                goto exit_level_0;
            }
        }
        /* Call LAPACK function and adjust info */
        LAPACK_cstegr( &jobz, &range, &n, d, e, &vl, &vu, &il, &iu, &abstol, m,
                       w, z_t, &ldz_t, isuppz, work, &lwork, iwork, &liwork,
                       &info );
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_cge_trans( LAPACK_COL_MAJOR, n, *m, z_t, ldz_t, z, ldz );
        }
        /* Release memory and exit */
        if( LAPACKE_lsame( jobz, 'v' ) ) {
            LAPACKE_free( z_t );
        }
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_cstegr_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_cstegr_work", info );
    }
    return info;
}
