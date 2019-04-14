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
* Contents: Native high-level C interface to LAPACK function zgesvj
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zgesvj( int matrix_layout, char joba, char jobu, char jobv,
                           lapack_int m, lapack_int n,
                           lapack_complex_double * a, lapack_int lda,
                           double* sva, lapack_int mv,
                           lapack_complex_double* v, lapack_int ldv, double* stat )
{
    lapack_int info = 0;
    lapack_int lwork = m+n;
    lapack_int lrwork = MAX(6,m+n);
    double* rwork = NULL;
    lapack_complex_double* cwork = NULL;
    lapack_int i;
    lapack_int nrows_v;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_zgesvj", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        nrows_v = LAPACKE_lsame( jobv, 'v' ) ? MAX(0,n) :
                ( LAPACKE_lsame( jobv, 'a' ) ? MAX(0,mv) : 0);
        if( LAPACKE_zge_nancheck( matrix_layout, m, n, a, lda ) ) {
            return -7;
        }
        if( LAPACKE_lsame( jobv, 'a' ) || LAPACKE_lsame( jobv, 'v' ) ) {
            if( LAPACKE_zge_nancheck( matrix_layout, nrows_v, n, v, ldv ) ) {
                return -11;
            }
        }
    }
#endif
    /* Allocate memory for working array(s) */
    cwork = (lapack_complex_double*)
        LAPACKE_malloc( sizeof(lapack_complex_double)  * lwork );
    if( cwork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    rwork = (double*)LAPACKE_malloc( sizeof(double) * lrwork );
    if( rwork == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_1;
    }
    rwork[0] = stat[0];  /* Significant if jobu = 'c' */
    /* Call middle-level interface */
    info = LAPACKE_zgesvj_work( matrix_layout, joba, jobu, jobv, m, n, a, lda,
                                sva, mv, v, ldv, cwork, lwork, rwork, lrwork );
    /* Backup significant data from working array(s) */
    for( i=0; i<6; i++ ) {
        stat[i] = rwork[i];
    }

    /* Release memory and exit */
    LAPACKE_free( rwork );
exit_level_1:
    LAPACKE_free( cwork );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_zgesvj", info );
    }
    return info;
}
