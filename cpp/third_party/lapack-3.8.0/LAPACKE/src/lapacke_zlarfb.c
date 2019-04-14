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
* Contents: Native high-level C interface to LAPACK function zlarfb
* Author: Intel Corporation
* Generated June 2017
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zlarfb( int matrix_layout, char side, char trans, char direct,
                           char storev, lapack_int m, lapack_int n,
                           lapack_int k, const lapack_complex_double* v,
                           lapack_int ldv, const lapack_complex_double* t,
                           lapack_int ldt, lapack_complex_double* c,
                           lapack_int ldc )
{
    lapack_int info = 0;
    lapack_int ldwork;
    lapack_complex_double* work = NULL;
    lapack_int ncols_v, nrows_v;
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_zlarfb", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        ncols_v = LAPACKE_lsame( storev, 'c' ) ? k :
                             ( ( LAPACKE_lsame( storev, 'r' ) &&
                             LAPACKE_lsame( side, 'l' ) ) ? m :
                             ( ( LAPACKE_lsame( storev, 'r' ) &&
                             LAPACKE_lsame( side, 'r' ) ) ? n : 1) );
        nrows_v = ( LAPACKE_lsame( storev, 'c' ) &&
                             LAPACKE_lsame( side, 'l' ) ) ? m :
                             ( ( LAPACKE_lsame( storev, 'c' ) &&
                             LAPACKE_lsame( side, 'r' ) ) ? n :
                             ( LAPACKE_lsame( storev, 'r' ) ? k : 1) );
        if( LAPACKE_zge_nancheck( matrix_layout, m, n, c, ldc ) ) {
            return -13;
        }
        if( LAPACKE_zge_nancheck( matrix_layout, k, k, t, ldt ) ) {
            return -11;
        }
        if( LAPACKE_lsame( storev, 'c' ) && LAPACKE_lsame( direct, 'f' ) ) {
            if( LAPACKE_ztr_nancheck( matrix_layout, 'l', 'u', k, v, ldv ) )
                return -9;
            if( LAPACKE_zge_nancheck( matrix_layout, nrows_v-k, ncols_v, &v[k*ldv],
                ldv ) )
                return -9;
        } else if( LAPACKE_lsame( storev, 'c' ) && LAPACKE_lsame( direct, 'b' ) ) {
            if( k > nrows_v ) {
                LAPACKE_xerbla( "LAPACKE_zlarfb", -8 );
                return -8;
            }
            if( LAPACKE_ztr_nancheck( matrix_layout, 'u', 'u', k,
                &v[(nrows_v-k)*ldv], ldv ) )
                return -9;
            if( LAPACKE_zge_nancheck( matrix_layout, nrows_v-k, ncols_v, v, ldv ) )
                return -9;
        } else if( LAPACKE_lsame( storev, 'r' ) && LAPACKE_lsame( direct, 'f' ) ) {
            if( LAPACKE_ztr_nancheck( matrix_layout, 'u', 'u', k, v, ldv ) )
                return -9;
            if( LAPACKE_zge_nancheck( matrix_layout, nrows_v, ncols_v-k, &v[k],
                ldv ) )
                return -9;
        } else if( LAPACKE_lsame( storev, 'r' ) && LAPACKE_lsame( direct, 'f' ) ) {
            if( k > ncols_v ) {
                LAPACKE_xerbla( "LAPACKE_zlarfb", -8 );
                return -8;
            }
            if( LAPACKE_ztr_nancheck( matrix_layout, 'l', 'u', k, &v[ncols_v-k],
                ldv ) )
                return -9;
            if( LAPACKE_zge_nancheck( matrix_layout, nrows_v, ncols_v-k, v, ldv ) )
                return -9;
        }
    }
#endif
    if( LAPACKE_lsame( side, 'l' ) ) {
        ldwork = n;
    } else if( LAPACKE_lsame( side, 'r' ) ) {
        ldwork = m;
    } else {
        ldwork = 1;
    }
    /* Allocate memory for working array(s) */
    work = (lapack_complex_double*)
        LAPACKE_malloc( sizeof(lapack_complex_double) * ldwork * MAX(1,k) );
    if( work == NULL ) {
        info = LAPACK_WORK_MEMORY_ERROR;
        goto exit_level_0;
    }
    /* Call middle-level interface */
    info = LAPACKE_zlarfb_work( matrix_layout, side, trans, direct, storev, m, n,
                                k, v, ldv, t, ldt, c, ldc, work, ldwork );
    /* Release memory and exit */
    LAPACKE_free( work );
exit_level_0:
    if( info == LAPACK_WORK_MEMORY_ERROR ) {
        LAPACKE_xerbla( "LAPACKE_zlarfb", info );
    }
    return info;
}
