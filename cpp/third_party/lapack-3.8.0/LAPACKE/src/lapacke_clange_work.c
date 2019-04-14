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
* Contents: Native middle-level C interface to LAPACK function clange
* Author: Intel Corporation
* Generated June 2017
*****************************************************************************/

#include "lapacke_utils.h"

float LAPACKE_clange_work( int matrix_layout, char norm, lapack_int m,
                                lapack_int n, const lapack_complex_float* a,
                                lapack_int lda, float* work )
{
    lapack_int info = 0;
    float res = 0.;
    char norm_lapack;
    if( matrix_layout == LAPACK_COL_MAJOR ) {
        /* Call LAPACK function */
        res = LAPACK_clange( &norm, &m, &n, a, &lda, work );
    } else if( matrix_layout == LAPACK_ROW_MAJOR ) {
        float* work_lapack = NULL;
        /* Check leading dimension(s) */
        if( lda < n ) {
            info = -6;
            LAPACKE_xerbla( "LAPACKE_clange_work", info );
            return info;
        }
        if( LAPACKE_lsame( norm, '1' ) || LAPACKE_lsame( norm, 'o' ) ) {
            norm_lapack = 'i';
        } else if( LAPACKE_lsame( norm, 'i' ) ) {
            norm_lapack = '1';
        } else {
            norm_lapack = norm;
        }
        /* Allocate memory for work array(s) */
        if( LAPACKE_lsame( norm_lapack, 'i' ) ) {
            work_lapack = (float*)LAPACKE_malloc( sizeof(float) * MAX(1,n) );
            if( work_lapack == NULL ) {
                info = LAPACK_WORK_MEMORY_ERROR;
                goto exit_level_0;
            }
        }
        /* Call LAPACK function */
        res = LAPACK_clange( &norm_lapack, &n, &m, a, &lda, work_lapack );
        /* Release memory and exit */
        if( work_lapack ) {
            LAPACKE_free( work_lapack );
        }
exit_level_0:
        if( info == LAPACK_WORK_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_clange_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla( "LAPACKE_clange_work", info );
    }
    return res;
}
