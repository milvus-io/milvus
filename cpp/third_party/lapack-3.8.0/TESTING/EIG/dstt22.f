*> \brief \b DSTT22
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSTT22( N, M, KBAND, AD, AE, SD, SE, U, LDU, WORK,
*                          LDWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            KBAND, LDU, LDWORK, M, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   AD( * ), AE( * ), RESULT( 2 ), SD( * ),
*      $                   SE( * ), U( LDU, * ), WORK( LDWORK, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSTT22  checks a set of M eigenvalues and eigenvectors,
*>
*>     A U = U S
*>
*> where A is symmetric tridiagonal, the columns of U are orthogonal,
*> and S is diagonal (if KBAND=0) or symmetric tridiagonal (if KBAND=1).
*> Two tests are performed:
*>
*>    RESULT(1) = | U' A U - S | / ( |A| m ulp )
*>
*>    RESULT(2) = | I - U'U | / ( m ulp )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrix.  If it is zero, DSTT22 does nothing.
*>          It must be at least zero.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of eigenpairs to check.  If it is zero, DSTT22
*>          does nothing.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] KBAND
*> \verbatim
*>          KBAND is INTEGER
*>          The bandwidth of the matrix S.  It may only be zero or one.
*>          If zero, then S is diagonal, and SE is not referenced.  If
*>          one, then S is symmetric tri-diagonal.
*> \endverbatim
*>
*> \param[in] AD
*> \verbatim
*>          AD is DOUBLE PRECISION array, dimension (N)
*>          The diagonal of the original (unfactored) matrix A.  A is
*>          assumed to be symmetric tridiagonal.
*> \endverbatim
*>
*> \param[in] AE
*> \verbatim
*>          AE is DOUBLE PRECISION array, dimension (N)
*>          The off-diagonal of the original (unfactored) matrix A.  A
*>          is assumed to be symmetric tridiagonal.  AE(1) is ignored,
*>          AE(2) is the (1,2) and (2,1) element, etc.
*> \endverbatim
*>
*> \param[in] SD
*> \verbatim
*>          SD is DOUBLE PRECISION array, dimension (N)
*>          The diagonal of the (symmetric tri-) diagonal matrix S.
*> \endverbatim
*>
*> \param[in] SE
*> \verbatim
*>          SE is DOUBLE PRECISION array, dimension (N)
*>          The off-diagonal of the (symmetric tri-) diagonal matrix S.
*>          Not referenced if KBSND=0.  If KBAND=1, then AE(1) is
*>          ignored, SE(2) is the (1,2) and (2,1) element, etc.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU, N)
*>          The orthogonal matrix in the decomposition.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  LDU must be at least N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LDWORK, M+1)
*> \endverbatim
*>
*> \param[in] LDWORK
*> \verbatim
*>          LDWORK is INTEGER
*>          The leading dimension of WORK.  LDWORK must be at least
*>          max(1,M).
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (2)
*>          The values computed by the two tests described above.  The
*>          values are currently limited to 1/ulp, to avoid overflow.
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DSTT22( N, M, KBAND, AD, AE, SD, SE, U, LDU, WORK,
     $                   LDWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KBAND, LDU, LDWORK, M, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   AD( * ), AE( * ), RESULT( 2 ), SD( * ),
     $                   SE( * ), U( LDU, * ), WORK( LDWORK, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, K
      DOUBLE PRECISION   ANORM, AUKJ, ULP, UNFL, WNORM
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE, DLANSY
      EXTERNAL           DLAMCH, DLANGE, DLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      RESULT( 1 ) = ZERO
      RESULT( 2 ) = ZERO
      IF( N.LE.0 .OR. M.LE.0 )
     $   RETURN
*
      UNFL = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Epsilon' )
*
*     Do Test 1
*
*     Compute the 1-norm of A.
*
      IF( N.GT.1 ) THEN
         ANORM = ABS( AD( 1 ) ) + ABS( AE( 1 ) )
         DO 10 J = 2, N - 1
            ANORM = MAX( ANORM, ABS( AD( J ) )+ABS( AE( J ) )+
     $              ABS( AE( J-1 ) ) )
   10    CONTINUE
         ANORM = MAX( ANORM, ABS( AD( N ) )+ABS( AE( N-1 ) ) )
      ELSE
         ANORM = ABS( AD( 1 ) )
      END IF
      ANORM = MAX( ANORM, UNFL )
*
*     Norm of U'AU - S
*
      DO 40 I = 1, M
         DO 30 J = 1, M
            WORK( I, J ) = ZERO
            DO 20 K = 1, N
               AUKJ = AD( K )*U( K, J )
               IF( K.NE.N )
     $            AUKJ = AUKJ + AE( K )*U( K+1, J )
               IF( K.NE.1 )
     $            AUKJ = AUKJ + AE( K-1 )*U( K-1, J )
               WORK( I, J ) = WORK( I, J ) + U( K, I )*AUKJ
   20       CONTINUE
   30    CONTINUE
         WORK( I, I ) = WORK( I, I ) - SD( I )
         IF( KBAND.EQ.1 ) THEN
            IF( I.NE.1 )
     $         WORK( I, I-1 ) = WORK( I, I-1 ) - SE( I-1 )
            IF( I.NE.N )
     $         WORK( I, I+1 ) = WORK( I, I+1 ) - SE( I )
         END IF
   40 CONTINUE
*
      WNORM = DLANSY( '1', 'L', M, WORK, M, WORK( 1, M+1 ) )
*
      IF( ANORM.GT.WNORM ) THEN
         RESULT( 1 ) = ( WNORM / ANORM ) / ( M*ULP )
      ELSE
         IF( ANORM.LT.ONE ) THEN
            RESULT( 1 ) = ( MIN( WNORM, M*ANORM ) / ANORM ) / ( M*ULP )
         ELSE
            RESULT( 1 ) = MIN( WNORM / ANORM, DBLE( M ) ) / ( M*ULP )
         END IF
      END IF
*
*     Do Test 2
*
*     Compute  U'U - I
*
      CALL DGEMM( 'T', 'N', M, M, N, ONE, U, LDU, U, LDU, ZERO, WORK,
     $            M )
*
      DO 50 J = 1, M
         WORK( J, J ) = WORK( J, J ) - ONE
   50 CONTINUE
*
      RESULT( 2 ) = MIN( DBLE( M ), DLANGE( '1', M, M, WORK, M, WORK( 1,
     $              M+1 ) ) ) / ( M*ULP )
*
      RETURN
*
*     End of DSTT22
*
      END
