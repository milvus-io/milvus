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
* Contents: Native middle-level C interface to LAPACK function slaswp
* Author: Intel Corporation
* Generated June 2016
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_clascl_work( int matrix_layout, char type, lapack_int kl,
                           lapack_int ku, float cfrom, float cto,
                           lapack_int m, lapack_int n, lapack_complex_float* a,
                           lapack_int lda )
{
    lapack_int info = 0;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function and adjust info */
        LAPACK_clascl( &type, &kl, &ku, &cfrom, &cto, &m, &n, a, &lda, &info);
        if( info < 0 ) {
            info = info - 1;
        }
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        lapack_int nrows_a = LAPACKE_lsame(type, 'b') ? kl + 1 :
                             LAPACKE_lsame(type, 'q') ? ku + 1 :
                             LAPACKE_lsame(type, 'z') ? 2 * kl + ku + 1 : m;
        lapack_int lda_t = MAX(1,nrows_a);
        lapack_complex_float* a_t = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_clascl_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (lapack_complex_float*)
           LAPACKE_malloc( sizeof(lapack_complex_float) * lda_t * MAX(1,n) );
        if( a_t == NULL ) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        /* Transpose input matrices */
        LAPACKE_cge_trans( matrix_layout, nrows_a, n, a, lda, a_t, lda_t );
        /* Call LAPACK function and adjust info */
        LAPACK_clascl( &type, &kl, &ku, &cfrom, &cto, &m, &n, a_t, &lda_t, &info);
        if( info < 0 ) {
            info = info - 1;
        }
        /* Transpose output matrices */
        LAPACKE_cge_trans( LAPACK_COL_MAJOR, nrows_a, n, a_t, lda_t, a, lda );
        /* Release memory and exit */
        LAPACKE_free( a_t );
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_clascl_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_clascl_work", info );
    }
    return info;
}
