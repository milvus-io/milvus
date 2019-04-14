*> \brief \b CSTT21
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CSTT21( N, KBAND, AD, AE, SD, SE, U, LDU, WORK, RWORK,
*                          RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            KBAND, LDU, N
*       ..
*       .. Array Arguments ..
*       REAL               AD( * ), AE( * ), RESULT( 2 ), RWORK( * ),
*      $                   SD( * ), SE( * )
*       COMPLEX            U( LDU, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CSTT21  checks a decomposition of the form
*>
*>    A = U S UC>
*> where * means conjugate transpose, A is real symmetric tridiagonal,
*> U is unitary, and S is real and diagonal (if KBAND=0) or symmetric
*> tridiagonal (if KBAND=1).  Two tests are performed:
*>
*>    RESULT(1) = | A - U S U* | / ( |A| n ulp )
*>
*>    RESULT(2) = | I - UU* | / ( n ulp )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrix.  If it is zero, CSTT21 does nothing.
*>          It must be at least zero.
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
*>          AD is REAL array, dimension (N)
*>          The diagonal of the original (unfactored) matrix A.  A is
*>          assumed to be real symmetric tridiagonal.
*> \endverbatim
*>
*> \param[in] AE
*> \verbatim
*>          AE is REAL array, dimension (N-1)
*>          The off-diagonal of the original (unfactored) matrix A.  A
*>          is assumed to be symmetric tridiagonal.  AE(1) is the (1,2)
*>          and (2,1) element, AE(2) is the (2,3) and (3,2) element, etc.
*> \endverbatim
*>
*> \param[in] SD
*> \verbatim
*>          SD is REAL array, dimension (N)
*>          The diagonal of the real (symmetric tri-) diagonal matrix S.
*> \endverbatim
*>
*> \param[in] SE
*> \verbatim
*>          SE is REAL array, dimension (N-1)
*>          The off-diagonal of the (symmetric tri-) diagonal matrix S.
*>          Not referenced if KBSND=0.  If KBAND=1, then AE(1) is the
*>          (1,2) and (2,1) element, SE(2) is the (2,3) and (3,2)
*>          element, etc.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is COMPLEX array, dimension (LDU, N)
*>          The unitary matrix in the decomposition.
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
*>          WORK is COMPLEX array, dimension (N**2)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (2)
*>          The values computed by the two tests described above.  The
*>          values are currently limited to 1/ulp, to avoid overflow.
*>          RESULT(1) is always modified.
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CSTT21( N, KBAND, AD, AE, SD, SE, U, LDU, WORK, RWORK,
     $                   RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KBAND, LDU, N
*     ..
*     .. Array Arguments ..
      REAL               AD( * ), AE( * ), RESULT( 2 ), RWORK( * ),
     $                   SD( * ), SE( * )
      COMPLEX            U( LDU, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            J
      REAL               ANORM, TEMP1, TEMP2, ULP, UNFL, WNORM
*     ..
*     .. External Functions ..
      REAL               CLANGE, CLANHE, SLAMCH
      EXTERNAL           CLANGE, CLANHE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMM, CHER, CHER2, CLASET
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CMPLX, MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
*     1)      Constants
*
      RESULT( 1 ) = ZERO
      RESULT( 2 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      UNFL = SLAMCH( 'Safe minimum' )
      ULP = SLAMCH( 'Precision' )
*
*     Do Test 1
*
*     Copy A & Compute its 1-Norm:
*
      CALL CLASET( 'Full', N, N, CZERO, CZERO, WORK, N )
*
      ANORM = ZERO
      TEMP1 = ZERO
*
      DO 10 J = 1, N - 1
         WORK( ( N+1 )*( J-1 )+1 ) = AD( J )
         WORK( ( N+1 )*( J-1 )+2 ) = AE( J )
         TEMP2 = ABS( AE( J ) )
         ANORM = MAX( ANORM, ABS( AD( J ) )+TEMP1+TEMP2 )
         TEMP1 = TEMP2
   10 CONTINUE
*
      WORK( N**2 ) = AD( N )
      ANORM = MAX( ANORM, ABS( AD( N ) )+TEMP1, UNFL )
*
*     Norm of A - USU*
*
      DO 20 J = 1, N
         CALL CHER( 'L', N, -SD( J ), U( 1, J ), 1, WORK, N )
   20 CONTINUE
*
      IF( N.GT.1 .AND. KBAND.EQ.1 ) THEN
         DO 30 J = 1, N - 1
            CALL CHER2( 'L', N, -CMPLX( SE( J ) ), U( 1, J ), 1,
     $                  U( 1, J+1 ), 1, WORK, N )
   30    CONTINUE
      END IF
*
      WNORM = CLANHE( '1', 'L', N, WORK, N, RWORK )
*
      IF( ANORM.GT.WNORM ) THEN
         RESULT( 1 ) = ( WNORM / ANORM ) / ( N*ULP )
      ELSE
         IF( ANORM.LT.ONE ) THEN
            RESULT( 1 ) = ( MIN( WNORM, N*ANORM ) / ANORM ) / ( N*ULP )
         ELSE
            RESULT( 1 ) = MIN( WNORM / ANORM, REAL( N ) ) / ( N*ULP )
         END IF
      END IF
*
*     Do Test 2
*
*     Compute  UU* - I
*
      CALL CGEMM( 'N', 'C', N, N, N, CONE, U, LDU, U, LDU, CZERO, WORK,
     $            N )
*
      DO 40 J = 1, N
         WORK( ( N+1 )*( J-1 )+1 ) = WORK( ( N+1 )*( J-1 )+1 ) - CONE
   40 CONTINUE
*
      RESULT( 2 ) = MIN( REAL( N ), CLANGE( '1', N, N, WORK, N,
     $              RWORK ) ) / ( N*ULP )
*
      RETURN
*
*     End of CSTT21
*
      END
