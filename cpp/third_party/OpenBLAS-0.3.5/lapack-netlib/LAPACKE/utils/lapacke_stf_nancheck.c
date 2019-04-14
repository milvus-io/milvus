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

/* Check a matrix for NaN entries. */

lapack_logical LAPACKE_stf_nancheck( int matrix_layout, char transr,
                                      char uplo, char diag,
                                      lapack_int n,
                                      const float *a )
{
    lapack_int len;
    lapack_logical rowmaj, ntr, lower, unit;
    lapack_int n1, n2, k;

    if( a == NULL ) return (lapack_logical) 0;

    rowmaj = (matrix_layout == LAPACK_ROW_MAJOR);
    ntr    = LAPACKE_lsame( transr, 'n' );
    lower  = LAPACKE_lsame( uplo,   'l' );
    unit   = LAPACKE_lsame( diag,   'u' );

    if( ( !rowmaj && ( matrix_layout != LAPACK_COL_MAJOR ) ) ||
        ( !ntr    && !LAPACKE_lsame( transr, 't' )
                  && !LAPACKE_lsame( transr, 'c' ) ) ||
        ( !lower  && !LAPACKE_lsame( uplo,   'u' ) ) ||
        ( !unit   && !LAPACKE_lsame( diag,   'n' ) ) ) {
        /* Just exit if any of input parameters are wrong */
        return (lapack_logical) 0;
    }

    if( unit ) {
        /* Unit case, diagonal should be excluded from the check for NaN.
         * Decoding RFP and checking both triangulars and rectangular
         * for NaNs.
         */
        if( lower ) {
            n2 = n / 2;
            n1 = n - n2;
        } else {
            n1 = n / 2;
            n2 = n - n1;
        }
        if( n % 2 == 1 ) {
            /* N is odd */
            if( ( rowmaj || ntr ) && !( rowmaj && ntr ) ) {
                /* N is odd and ( TRANSR = 'N' .XOR. ROWMAJOR) */
                if( lower ) {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 n1, &a[0], n )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, n2, n1,
                                                 &a[n1], n )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 n2, &a[n], n );
                } else {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 n1, &a[n2], n )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, n1, n2,
                                                 &a[0], n )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 n2, &a[n1], n );
                }
            } else {
                /* N is odd and
                   ( ( TRANSR = 'C' || TRANSR = 'T' ) .XOR. COLMAJOR ) */
                if( lower ) {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 n1, &a[0], n1 )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, n1, n2,
                                                 &a[1], n1 )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 n2, &a[1], n1 );
                } else {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 n1, &a[(size_t)n2*n2], n2 )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, n2, n1,
                                                 &a[0], n2 )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 n2, &a[(size_t)n1*n2], n2 );
                }
            }
        } else {
            /* N is even */
            k = n / 2;
            if( ( rowmaj || ntr ) && !( rowmaj && ntr ) ) {
                /* N is even and ( TRANSR = 'N' .XOR. ROWMAJOR) */
                if( lower ) {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 k, &a[1], n+1 )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, k, k,
                                                 &a[k+1], n+1 )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 k, &a[0], n+1 );
                } else {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 k, &a[k+1], n+1 )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, k, k,
                                                 &a[0], n+1 )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 k, &a[k], n+1 );
                }
            } else {
                /* N is even and
                 * ( ( TRANSR = 'C' || TRANSR = 'T' ) .XOR. COLMAJOR )
                 */
                if( lower ) {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 k, &a[k], k )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, k, k,
                                                 &a[(size_t)k*(k+1)], k )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 k, &a[0], k );
                } else {
                    return LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'u', 'u',
                                                 k, &a[(size_t)k*(k+1)], k )
                        || LAPACKE_sge_nancheck( LAPACK_ROW_MAJOR, k, k,
                                                 &a[0], k )
                        || LAPACKE_str_nancheck( LAPACK_ROW_MAJOR, 'l', 'u',
                                                 k, &a[(size_t)k*k], k );
                }
            }
        }
    } else {
        /* Non-unit case - just check whole array for NaNs. */
        len = n*(n+1)/2;
        return LAPACKE_sge_nancheck( LAPACK_COL_MAJOR, len, 1, a, len );
    }
}
