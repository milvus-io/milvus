*> \brief \b SGET53
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGET53( A, LDA, B, LDB, SCALE, WR, WI, RESULT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDB
*       REAL               RESULT, SCALE, WI, WR
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGET53  checks the generalized eigenvalues computed by SLAG2.
*>
*> The basic test for an eigenvalue is:
*>
*>                              | det( s A - w B ) |
*>     RESULT =  ---------------------------------------------------
*>               ulp max( s norm(A), |w| norm(B) )*norm( s A - w B )
*>
*> Two "safety checks" are performed:
*>
*> (1)  ulp*max( s*norm(A), |w|*norm(B) )  must be at least
*>      safe_minimum.  This insures that the test performed is
*>      not essentially  det(0*A + 0*B)=0.
*>
*> (2)  s*norm(A) + |w|*norm(B) must be less than 1/safe_minimum.
*>      This insures that  s*A - w*B  will not overflow.
*>
*> If these tests are not passed, then  s  and  w  are scaled and
*> tested anyway, if this is possible.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA, 2)
*>          The 2x2 matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 2.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB, N)
*>          The 2x2 upper-triangular matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  It must be at least 2.
*> \endverbatim
*>
*> \param[in] SCALE
*> \verbatim
*>          SCALE is REAL
*>          The "scale factor" s in the formula  s A - w B .  It is
*>          assumed to be non-negative.
*> \endverbatim
*>
*> \param[in] WR
*> \verbatim
*>          WR is REAL
*>          The real part of the eigenvalue  w  in the formula
*>          s A - w B .
*> \endverbatim
*>
*> \param[in] WI
*> \verbatim
*>          WI is REAL
*>          The imaginary part of the eigenvalue  w  in the formula
*>          s A - w B .
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL
*>          If INFO is 2 or less, the value computed by the test
*>             described above.
*>          If INFO=3, this will just be 1/ulp.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          =0:  The input data pass the "safety checks".
*>          =1:  s*norm(A) + |w|*norm(B) > 1/safe_minimum.
*>          =2:  ulp*max( s*norm(A), |w|*norm(B) ) < safe_minimum
*>          =3:  same as INFO=2, but  s  and  w  could not be scaled so
*>               as to compute the test.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SGET53( A, LDA, B, LDB, SCALE, WR, WI, RESULT, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDB
      REAL               RESULT, SCALE, WI, WR
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0, ONE = 1.0 )
*     ..
*     .. Local Scalars ..
      REAL               ABSW, ANORM, BNORM, CI11, CI12, CI22, CNORM,
     $                   CR11, CR12, CR21, CR22, CSCALE, DETI, DETR, S1,
     $                   SAFMIN, SCALES, SIGMIN, TEMP, ULP, WIS, WRS
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Initialize
*
      INFO = 0
      RESULT = ZERO
      SCALES = SCALE
      WRS = WR
      WIS = WI
*
*     Machine constants and norms
*
      SAFMIN = SLAMCH( 'Safe minimum' )
      ULP = SLAMCH( 'Epsilon' )*SLAMCH( 'Base' )
      ABSW = ABS( WRS ) + ABS( WIS )
      ANORM = MAX( ABS( A( 1, 1 ) )+ABS( A( 2, 1 ) ),
     $        ABS( A( 1, 2 ) )+ABS( A( 2, 2 ) ), SAFMIN )
      BNORM = MAX( ABS( B( 1, 1 ) ), ABS( B( 1, 2 ) )+ABS( B( 2, 2 ) ),
     $        SAFMIN )
*
*     Check for possible overflow.
*
      TEMP = ( SAFMIN*BNORM )*ABSW + ( SAFMIN*ANORM )*SCALES
      IF( TEMP.GE.ONE ) THEN
*
*        Scale down to avoid overflow
*
         INFO = 1
         TEMP = ONE / TEMP
         SCALES = SCALES*TEMP
         WRS = WRS*TEMP
         WIS = WIS*TEMP
         ABSW = ABS( WRS ) + ABS( WIS )
      END IF
      S1 = MAX( ULP*MAX( SCALES*ANORM, ABSW*BNORM ),
     $     SAFMIN*MAX( SCALES, ABSW ) )
*
*     Check for W and SCALE essentially zero.
*
      IF( S1.LT.SAFMIN ) THEN
         INFO = 2
         IF( SCALES.LT.SAFMIN .AND. ABSW.LT.SAFMIN ) THEN
            INFO = 3
            RESULT = ONE / ULP
            RETURN
         END IF
*
*        Scale up to avoid underflow
*
         TEMP = ONE / MAX( SCALES*ANORM+ABSW*BNORM, SAFMIN )
         SCALES = SCALES*TEMP
         WRS = WRS*TEMP
         WIS = WIS*TEMP
         ABSW = ABS( WRS ) + ABS( WIS )
         S1 = MAX( ULP*MAX( SCALES*ANORM, ABSW*BNORM ),
     $        SAFMIN*MAX( SCALES, ABSW ) )
         IF( S1.LT.SAFMIN ) THEN
            INFO = 3
            RESULT = ONE / ULP
            RETURN
         END IF
      END IF
*
*     Compute C = s A - w B
*
      CR11 = SCALES*A( 1, 1 ) - WRS*B( 1, 1 )
      CI11 = -WIS*B( 1, 1 )
      CR21 = SCALES*A( 2, 1 )
      CR12 = SCALES*A( 1, 2 ) - WRS*B( 1, 2 )
      CI12 = -WIS*B( 1, 2 )
      CR22 = SCALES*A( 2, 2 ) - WRS*B( 2, 2 )
      CI22 = -WIS*B( 2, 2 )
*
*     Compute the smallest singular value of s A - w B:
*
*                 |det( s A - w B )|
*     sigma_min = ------------------
*                 norm( s A - w B )
*
      CNORM = MAX( ABS( CR11 )+ABS( CI11 )+ABS( CR21 ),
     $        ABS( CR12 )+ABS( CI12 )+ABS( CR22 )+ABS( CI22 ), SAFMIN )
      CSCALE = ONE / SQRT( CNORM )
      DETR = ( CSCALE*CR11 )*( CSCALE*CR22 ) -
     $       ( CSCALE*CI11 )*( CSCALE*CI22 ) -
     $       ( CSCALE*CR12 )*( CSCALE*CR21 )
      DETI = ( CSCALE*CR11 )*( CSCALE*CI22 ) +
     $       ( CSCALE*CI11 )*( CSCALE*CR22 ) -
     $       ( CSCALE*CI12 )*( CSCALE*CR21 )
      SIGMIN = ABS( DETR ) + ABS( DETI )
      RESULT = SIGMIN / S1
      RETURN
*
*     End of SGET53
*
      END
