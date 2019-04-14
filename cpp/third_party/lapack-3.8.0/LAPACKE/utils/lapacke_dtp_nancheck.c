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
******************************************************************************
* Contents: Native C interface to LAPACK utility function
* Author: Intel Corporation
* Created in February, 2010
*****************************************************************************/
#include "lapacke_utils.h"

/* Check a matrix for NaN entries.
 * Since matrix in packed format stored continiously it just required to
 * check 1d array for NaNs. It doesn't depend upon uplo or matrix_layout.
 */

lapack_logical LAPACKE_dtp_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const double *ap )
{
    lapack_int i, len;
    lapack_logical colmaj, upper, unit;

    if( ap == NULL ) return (lapack_logical) 0;

    colmaj = ( matrix_layout == LAPACK_COL_MAJOR );
    upper  = LAPACKE_lsame( uplo, 'u' );
    unit   = LAPACKE_lsame( diag, 'u' );

    if( ( !colmaj && ( matrix_layout != LAPACK_ROW_MAJOR ) ) ||
        ( !upper  && !LAPACKE_lsame( uplo, 'l' ) ) ||
        ( !unit   && !LAPACKE_lsame( diag, 'n' ) ) ) {
        /* Just exit if any of input parameters are wrong */
        return (lapack_logical) 0;
    }

    if( unit ) {
        /* Unit case, diagonal should be excluded from the check for NaN. */

        /* Since col_major upper and row_major lower are equal,
         * and col_major lower and row_major upper are equals too -
         * using one code for equal cases. XOR( colmaj, upper )
         */
        if( ( colmaj || upper ) && !( colmaj && upper ) ) {
            for( i = 1; i < n; i++ )
                if( LAPACKE_d_nancheck( i, &ap[ ((size_t)i+1)*i/2 ], 1 ) )
                    return (lapack_logical) 1;
        } else {
            for( i = 0; i < n-1; i++ )
                if( LAPACKE_d_nancheck( n-i-1,
                    &ap[ (size_t)i+1 + i*((size_t)2*n-i+1)/2 ], 1 ) )
                    return (lapack_logical) 1;
        }
        return (lapack_logical) 0;
    } else {
        /* Non-unit case - just check whole array for NaNs. */
        len = n*(n+1)/2;
        return LAPACKE_d_nancheck( len, ap, 1 );
    }
}
