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
* Contents: Native high-level C interface to LAPACK function dbbcsd
* Author: Intel Corporation
* Generated June 2017
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dbbcsd( int matrix_layout, char jobu1, char jobu2,
                           char jobv1t, char jobv2t, char trans, lapack_int m,
                           lapack_int p, lapack_int q, double* theta,
                           double* phi, double* u1, lapack_int ldu1, double* u2,
                           lapack_int ldu2, double* v1t, lapack_int ldv1t,
                           double* v2t, lapack_int ldv2t, double* b11d,
                           double* b11e, double* b12d, double* b12e,
                           double* b21d, double* b21e, double* b22d,
                           double* b22e )
{
    lapack_int info = 0;
    lapack_int lwork = -1;
    double* work = NULL;
    double work_query;
    int lapack_layout;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_dbbcsd", -1 );
        return -1;
    }
    if( LAPACKE_lsame( trans, 'n' ) && matrix_layout == LAPACK_COL_MAJOR ) {
        lapack_layout = LAPACK_COL_MAJOR;
    } else {
        lapack_layout = LAPACK_ROW_MAJOR;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        if( LAPACKE_d_nancheck( q-1, phi, 1 ) ) {
            return -11;
        }
        if( LAPACKE_d_nancheck( q, theta, 1 ) ) {
            return -10;
        }
        if( LAPACKE_lsame( jobu1, 'y' ) ) {
            if( LAPACKE_dge_nancheck( lapack_layout, p, p, u1, ldu1 ) ) {
                return -12;
            }
        }
        if( LAPACKE_lsame( jobu2, 'y' ) ) {
            if( LAPACKE_dge_nancheck( lapack_layout, m-p, m-p, u2, ldu2 ) ) {
                return -14;
            }
        }
        if( LAPACKE_lsame( jobv1t, 'y' ) ) {
            if( LAPACKE_dge_nancheck( lapack_layout, q, q, v1t, ldv1t ) ) {
                return -16;
            }
        }
        if( LAPACKE_lsame( jobv2t, 'y' ) ) {
            if( LAPACKE_dge_nancheck( lapack_layout, m-q, m-q, v2t, ldv2t ) ) {
                return -18;
            }
        }
    }
#endif
    /* Query optimal working array(s) size */
    info = LAPACKE_dbbcsd_work( matrix_layout, jobu1, jobu2, jobv1t, jobv2t,
                                trans, m, p, q, theta, phi, u1, ldu1, u2, ldu2,
                                v1t, ldv1t, v2t, ldv2t, b11d, b11e, b12d, b12e,
                                b21d, b21e, b22d, b22e, &work_query, lwork );
    if( info != 0 ) {
        goto exit_level_0;
    }
    lwork = (lapack_int)work_query;
    /* Allocate memory for work arrays */
    work = (double*)LAPACKE_malloc( sizeof(double) * lwork );
    if( work == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Call middle-level interface */
    info = LAPACKE_dbbcsd_work( matrix_layout, jobu1, jobu2, jobv1t, jobv2t,
                                trans, m, p, q, theta, phi, u1, ldu1, u2, ldu2,
                                v1t, ldv1t, v2t, ldv2t, b11d, b11e, b12d, b12e,
                                b21d, b21e, b22d, b22e, work, lwork );
    /* Release memory and exit */
    LAPACKE_free( work );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_dbbcsd", info );
    }
    return info;
}
