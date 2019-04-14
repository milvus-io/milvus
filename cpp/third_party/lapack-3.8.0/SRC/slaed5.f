*> \brief \b SLAED5 used by sstedc. Solves the 2-by-2 secular equation.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAED5 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaed5.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaed5.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaed5.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAED5( I, D, Z, DELTA, RHO, DLAM )
*
*       .. Scalar Arguments ..
*       INTEGER            I
*       REAL               DLAM, RHO
*       ..
*       .. Array Arguments ..
*       REAL               D( 2 ), DELTA( 2 ), Z( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This subroutine computes the I-th eigenvalue of a symmetric rank-one
*> modification of a 2-by-2 diagonal matrix
*>
*>            diag( D )  +  RHO * Z * transpose(Z) .
*>
*> The diagonal elements in the array D are assumed to satisfy
*>
*>            D(i) < D(j)  for  i < j .
*>
*> We also assume RHO > 0 and that the Euclidean norm of the vector
*> Z is one.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] I
*> \verbatim
*>          I is INTEGER
*>         The index of the eigenvalue to be computed.  I = 1 or I = 2.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (2)
*>         The original eigenvalues.  We assume D(1) < D(2).
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is REAL array, dimension (2)
*>         The components of the updating vector.
*> \endverbatim
*>
*> \param[out] DELTA
*> \verbatim
*>          DELTA is REAL array, dimension (2)
*>         The vector DELTA contains the information necessary
*>         to construct the eigenvectors.
*> \endverbatim
*>
*> \param[in] RHO
*> \verbatim
*>          RHO is REAL
*>         The scalar in the symmetric updating formula.
*> \endverbatim
*>
*> \param[out] DLAM
*> \verbatim
*>          DLAM is REAL
*>         The computed lambda_I, the I-th updated eigenvalue.
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
*> \date December 2016
*
*> \ingroup auxOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*>     Ren-Cang Li, Computer Science Division, University of California
*>     at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE SLAED5( I, D, Z, DELTA, RHO, DLAM )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            I
      REAL               DLAM, RHO
*     ..
*     .. Array Arguments ..
      REAL               D( 2 ), DELTA( 2 ), Z( 2 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO, FOUR
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0, TWO = 2.0E0,
     $                   FOUR = 4.0E0 )
*     ..
*     .. Local Scalars ..
      REAL               B, C, DEL, TAU, TEMP, W
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, SQRT
*     ..
*     .. Executable Statements ..
*
      DEL = D( 2 ) - D( 1 )
      IF( I.EQ.1 ) THEN
         W = ONE + TWO*RHO*( Z( 2 )*Z( 2 )-Z( 1 )*Z( 1 ) ) / DEL
         IF( W.GT.ZERO ) THEN
            B = DEL + RHO*( Z( 1 )*Z( 1 )+Z( 2 )*Z( 2 ) )
            C = RHO*Z( 1 )*Z( 1 )*DEL
*
*           B > ZERO, always
*
            TAU = TWO*C / ( B+SQRT( ABS( B*B-FOUR*C ) ) )
            DLAM = D( 1 ) + TAU
            DELTA( 1 ) = -Z( 1 ) / TAU
            DELTA( 2 ) = Z( 2 ) / ( DEL-TAU )
         ELSE
            B = -DEL + RHO*( Z( 1 )*Z( 1 )+Z( 2 )*Z( 2 ) )
            C = RHO*Z( 2 )*Z( 2 )*DEL
            IF( B.GT.ZERO ) THEN
               TAU = -TWO*C / ( B+SQRT( B*B+FOUR*C ) )
            ELSE
               TAU = ( B-SQRT( B*B+FOUR*C ) ) / TWO
            END IF
            DLAM = D( 2 ) + TAU
            DELTA( 1 ) = -Z( 1 ) / ( DEL+TAU )
            DELTA( 2 ) = -Z( 2 ) / TAU
         END IF
         TEMP = SQRT( DELTA( 1 )*DELTA( 1 )+DELTA( 2 )*DELTA( 2 ) )
         DELTA( 1 ) = DELTA( 1 ) / TEMP
         DELTA( 2 ) = DELTA( 2 ) / TEMP
      ELSE
*
*     Now I=2
*
         B = -DEL + RHO*( Z( 1 )*Z( 1 )+Z( 2 )*Z( 2 ) )
         C = RHO*Z( 2 )*Z( 2 )*DEL
         IF( B.GT.ZERO ) THEN
            TAU = ( B+SQRT( B*B+FOUR*C ) ) / TWO
         ELSE
            TAU = TWO*C / ( -B+SQRT( B*B+FOUR*C ) )
         END IF
         DLAM = D( 2 ) + TAU
         DELTA( 1 ) = -Z( 1 ) / ( DEL+TAU )
         DELTA( 2 ) = -Z( 2 ) / TAU
         TEMP = SQRT( DELTA( 1 )*DELTA( 1 )+DELTA( 2 )*DELTA( 2 ) )
         DELTA( 1 ) = DELTA( 1 ) / TEMP
         DELTA( 2 ) = DELTA( 2 ) / TEMP
      END IF
      RETURN
*
*     End OF SLAED5
*
      END
