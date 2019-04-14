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
* Contents: Native high-level C interface to LAPACK function dlaswp
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_dlascl( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, double cfrom, double cto,
                           lapack_int m, lapack_int n, double* a,
                           lapack_int lda )
{
    if( matrix_layout != LAPACK_COL_MAJOR && matrix_layout != LAPACK_ROW_MAJOR ) {
        LAPACKE_xerbla( "LAPACKE_dlascl", -1 );
        return -1;
    }
#ifndef LAPACK_DISABLE_NAN_CHECK
    if( LAPACKE_get_nancheck() ) {
        /* Optionally check input matrices for NaNs */
        switch (type) {
        case 'G':
            if( LAPACKE_dge_nancheck( matrix_layout, m, n, a, lda ) ) {
                return -9;
            }
            break;
        case 'L':
            // TYPE = 'L' - lower triangle of general matrix
            if( matrix_layout == LAPACK_COL_MAJOR &&
                LAPACKE_dgb_nancheck( matrix_layout, m, n, m-1, 0, a, lda+1 ) ) {
                return -9;
            }
            if( matrix_layout == LAPACK_ROW_MAJOR &&
                LAPACKE_dgb_nancheck( LAPACK_COL_MAJOR, n, m, 0, m-1, a-m+1, lda+1 ) ) {
                return -9;
            }
            break;
        case 'U':
            // TYPE = 'U' - upper triangle of general matrix
            if( matrix_layout == LAPACK_COL_MAJOR &&
                LAPACKE_dgb_nancheck( matrix_layout, m, n, 0, n-1, a-n+1, lda+1 ) ) {
                return -9;
            }
            if( matrix_layout == LAPACK_ROW_MAJOR &&
                LAPACKE_dgb_nancheck( LAPACK_COL_MAJOR, n, m, n-1, 0, a, lda+1 ) ) {
                return -9;
            }
            break;
        case 'H':
            // TYPE = 'H' - part of upper Hessenberg matrix in general matrix
            if( matrix_layout == LAPACK_COL_MAJOR &&
                LAPACKE_dgb_nancheck( matrix_layout, m, n, 1, n-1, a-n+1, lda+1 ) ) {
                return -9;
            }
            if( matrix_layout == LAPACK_ROW_MAJOR &&
                LAPACKE_dgb_nancheck( LAPACK_COL_MAJOR, n, m, n-1, 1, a-1, lda+1 ) ) {
                return -9;
            }
        case 'B':
            // TYPE = 'B' - lower part of symmetric band matrix (assume m==n)
            if( LAPACKE_dsb_nancheck( matrix_layout, 'L', n, kl, a, lda ) ) {
                return -9;
            }
            break;
        case 'Q':
            // TYPE = 'Q' - upper part of symmetric band matrix (assume m==n)
            if( LAPACKE_dsb_nancheck( matrix_layout, 'U', n, ku, a, lda ) ) {
                return -9;
            }
            break;
        case 'Z':
            // TYPE = 'Z' -  band matrix laid out for ?GBTRF
            if( matrix_layout == LAPACK_COL_MAJOR &&
                LAPACKE_dgb_nancheck( matrix_layout, m, n, kl, ku, a+kl, lda ) ) {
                return -9;
            }
            if( matrix_layout == LAPACK_ROW_MAJOR &&
                LAPACKE_dgb_nancheck( matrix_layout, m, n, kl, ku, a+lda*kl, lda ) ) {
                return -9;
            }
            break;
        }
    }
#endif
    return LAPACKE_dlascl_work( matrix_layout, type, kl, ku, cfrom, cto, m,  n, a, lda );
}
