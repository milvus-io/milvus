*> \brief \b SLAQR1 sets a scalar multiple of the first column of the product of 2-by-2 or 3-by-3 matrix H and specified shifts.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAQR1 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaqr1.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaqr1.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaqr1.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAQR1( N, H, LDH, SR1, SI1, SR2, SI2, V )
*
*       .. Scalar Arguments ..
*       REAL               SI1, SI2, SR1, SR2
*       INTEGER            LDH, N
*       ..
*       .. Array Arguments ..
*       REAL               H( LDH, * ), V( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      Given a 2-by-2 or 3-by-3 matrix H, SLAQR1 sets v to a
*>      scalar multiple of the first column of the product
*>
*>      (*)  K = (H - (sr1 + i*si1)*I)*(H - (sr2 + i*si2)*I)
*>
*>      scaling to avoid overflows and most underflows. It
*>      is assumed that either
*>
*>              1) sr1 = sr2 and si1 = -si2
*>          or
*>              2) si1 = si2 = 0.
*>
*>      This is useful for starting double implicit shift bulges
*>      in the QR algorithm.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>              Order of the matrix H. N must be either 2 or 3.
*> \endverbatim
*>
*> \param[in] H
*> \verbatim
*>          H is REAL array, dimension (LDH,N)
*>              The 2-by-2 or 3-by-3 matrix H in (*).
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>              The leading dimension of H as declared in
*>              the calling procedure.  LDH.GE.N
*> \endverbatim
*>
*> \param[in] SR1
*> \verbatim
*>          SR1 is REAL
*> \endverbatim
*>
*> \param[in] SI1
*> \verbatim
*>          SI1 is REAL
*> \endverbatim
*>
*> \param[in] SR2
*> \verbatim
*>          SR2 is REAL
*> \endverbatim
*>
*> \param[in] SI2
*> \verbatim
*>          SI2 is REAL
*>              The shifts in (*).
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is REAL array, dimension (N)
*>              A scalar multiple of the first column of the
*>              matrix K in (*).
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date June 2017
*
*> \ingroup realOTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>       Karen Braman and Ralph Byers, Department of Mathematics,
*>       University of Kansas, USA
*>
*  =====================================================================
      SUBROUTINE SLAQR1( N, H, LDH, SR1, SI1, SR2, SI2, V )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      REAL               SI1, SI2, SR1, SR2
      INTEGER            LDH, N
*     ..
*     .. Array Arguments ..
      REAL               H( LDH, * ), V( * )
*     ..
*
*  ================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0e0 )
*     ..
*     .. Local Scalars ..
      REAL               H21S, H31S, S
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
      IF( N.EQ.2 ) THEN
         S = ABS( H( 1, 1 )-SR2 ) + ABS( SI2 ) + ABS( H( 2, 1 ) )
         IF( S.EQ.ZERO ) THEN
            V( 1 ) = ZERO
            V( 2 ) = ZERO
         ELSE
            H21S = H( 2, 1 ) / S
            V( 1 ) = H21S*H( 1, 2 ) + ( H( 1, 1 )-SR1 )*
     $               ( ( H( 1, 1 )-SR2 ) / S ) - SI1*( SI2 / S )
            V( 2 ) = H21S*( H( 1, 1 )+H( 2, 2 )-SR1-SR2 )
         END IF
      ELSE
         S = ABS( H( 1, 1 )-SR2 ) + ABS( SI2 ) + ABS( H( 2, 1 ) ) +
     $       ABS( H( 3, 1 ) )
         IF( S.EQ.ZERO ) THEN
            V( 1 ) = ZERO
            V( 2 ) = ZERO
            V( 3 ) = ZERO
         ELSE
            H21S = H( 2, 1 ) / S
            H31S = H( 3, 1 ) / S
            V( 1 ) = ( H( 1, 1 )-SR1 )*( ( H( 1, 1 )-SR2 ) / S ) -
     $               SI1*( SI2 / S ) + H( 1, 2 )*H21S + H( 1, 3 )*H31S
            V( 2 ) = H21S*( H( 1, 1 )+H( 2, 2 )-SR1-SR2 ) +
     $               H( 2, 3 )*H31S
            V( 3 ) = H31S*( H( 1, 1 )+H( 3, 3 )-SR1-SR2 ) +
     $               H21S*H( 3, 2 )
         END IF
      END IF
      END
