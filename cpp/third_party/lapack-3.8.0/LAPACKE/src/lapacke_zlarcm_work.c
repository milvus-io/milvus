/*****************************************************************************
  Copyright (c) 2017, Intel Corp.
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
* Contents: Native middle-level C interface to LAPACK function zlarcm
* Author: Intel Corporation
* Generated November 2017
*****************************************************************************/

#include "lapacke_utils.h"

lapack_int LAPACKE_zlarcm_work(int matrix_layout, lapack_int m, lapack_int n,
                                const double* a, lapack_int lda,
                                const lapack_complex_double* b, lapack_int ldb,
                                lapack_complex_double* c, lapack_int ldc,
                                double* rwork)
{
    lapack_int info = 0;
    if (matrix_layout == LAPACK_COL_MAJOR) {
        /* Call LAPACK function */
        LAPACK_zlarcm(&m, &n, a, &lda, b, &ldb, c, &ldc, rwork);
    } else if (matrix_layout == LAPACK_ROW_MAJOR) {
        lapack_int lda_t = MAX(1,m);
        lapack_int ldb_t = MAX(1,m);
        lapack_int ldc_t = MAX(1,m);
        double* a_t = NULL;
        lapack_complex_double* b_t = NULL;
        lapack_complex_double* c_t = NULL;
        /* Check leading dimension(s) */
        if( lda < m ) {
            info = -5;
            LAPACKE_xerbla( "LAPACKE_zlarcm_work", info );
            return info;
        }
        if( ldb < n ) {
            info = -7;
            LAPACKE_xerbla( "LAPACKE_zlarcm_work", info );
            return info;
        }
        if( ldc < n ) {
            info = -9;
            LAPACKE_xerbla( "LAPACKE_zlarcm_work", info );
            return info;
        }
        /* Allocate memory for temporary array(s) */
        a_t = (double*)
            LAPACKE_malloc(sizeof(double) * lda_t * MAX(1,m));
        b_t = (lapack_complex_double*)
            LAPACKE_malloc(sizeof(lapack_complex_double) * ldb_t * MAX(1,n));
        c_t = (lapack_complex_double*)
            LAPACKE_malloc((sizeof(lapack_complex_double) * ldc_t * MAX(1,n)));
        if (a_t == NULL) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_0;
        }
        if (b_t == NULL) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_1;
        }
        if (c_t == NULL) {
            info = LAPACK_TRANSPOSE_MEMORY_ERROR;
            goto exit_level_2;
        }
        /* Transpose input matrices */
        LAPACKE_dge_trans(matrix_layout, m, m, a, lda, a_t, lda_t);
        LAPACKE_zge_trans(matrix_layout, m, n, b, ldb, b_t, ldb_t);
        /* Call LAPACK function */
        LAPACK_zlarcm(&m, &n, a_t, &lda_t, b_t, &ldb_t, c_t, &ldc_t, rwork);
        /* Transpose output matrices */
        LAPACKE_zge_trans(LAPACK_COL_MAJOR, m, n, c_t, ldc_t, c, ldc);
        /* Release memory and exit */
        LAPACKE_free(c_t);
exit_level_2:
        LAPACKE_free(b_t);
exit_level_1:
        LAPACKE_free(a_t);
exit_level_0:
        if( info == LAPACK_TRANSPOSE_MEMORY_ERROR ) {
            LAPACKE_xerbla( "LAPACKE_zlarcm_work", info );
        }
    } else {
        info = -1;
        LAPACKE_xerbla("LAPACKE_zlarcm_work", -1);
    }
    return info;
}
