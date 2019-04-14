*> \brief \b ZHET21
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHET21( ITYPE, UPLO, N, KBAND, A, LDA, D, E, U, LDU, V,
*                          LDV, TAU, WORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            ITYPE, KBAND, LDA, LDU, LDV, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * ), E( * ), RESULT( 2 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), TAU( * ), U( LDU, * ),
*      $                   V( LDV, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHET21 generally checks a decomposition of the form
*>
*>    A = U S UC>
*> where * means conjugate transpose, A is hermitian, U is unitary, and
*> S is diagonal (if KBAND=0) or (real) symmetric tridiagonal (if
*> KBAND=1).
*>
*> If ITYPE=1, then U is represented as a dense matrix; otherwise U is
*> expressed as a product of Householder transformations, whose vectors
*> are stored in the array "V" and whose scaling constants are in "TAU".
*> We shall use the letter "V" to refer to the product of Householder
*> transformations (which should be equal to U).
*>
*> Specifically, if ITYPE=1, then:
*>
*>    RESULT(1) = | A - U S U* | / ( |A| n ulp ) *andC>    RESULT(2) = | I - UU* | / ( n ulp )
*>
*> If ITYPE=2, then:
*>
*>    RESULT(1) = | A - V S V* | / ( |A| n ulp )
*>
*> If ITYPE=3, then:
*>
*>    RESULT(1) = | I - UV* | / ( n ulp )
*>
*> For ITYPE > 1, the transformation U is expressed as a product
*> V = H(1)...H(n-2),  where H(j) = I  -  tau(j) v(j) v(j)C> and each
*> vector v(j) has its first j elements 0 and the remaining n-j elements
*> stored in V(j+1:n,j).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ITYPE
*> \verbatim
*>          ITYPE is INTEGER
*>          Specifies the type of tests to be performed.
*>          1: U expressed as a dense unitary matrix:
*>             RESULT(1) = | A - U S U* | / ( |A| n ulp )   *andC>             RESULT(2) = | I - UU* | / ( n ulp )
*>
*>          2: U expressed as a product V of Housholder transformations:
*>             RESULT(1) = | A - V S V* | / ( |A| n ulp )
*>
*>          3: U expressed both as a dense unitary matrix and
*>             as a product of Housholder transformations:
*>             RESULT(1) = | I - UV* | / ( n ulp )
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER
*>          If UPLO='U', the upper triangle of A and V will be used and
*>          the (strictly) lower triangle will not be referenced.
*>          If UPLO='L', the lower triangle of A and V will be used and
*>          the (strictly) upper triangle will not be referenced.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrix.  If it is zero, ZHET21 does nothing.
*>          It must be at least zero.
*> \endverbatim
*>
*> \param[in] KBAND
*> \verbatim
*>          KBAND is INTEGER
*>          The bandwidth of the matrix.  It may only be zero or one.
*>          If zero, then S is diagonal, and E is not referenced.  If
*>          one, then S is symmetric tri-diagonal.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          The original (unfactored) matrix.  It is assumed to be
*>          hermitian, and only the upper (UPLO='U') or only the lower
*>          (UPLO='L') will be referenced.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The diagonal of the (symmetric tri-) diagonal matrix.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The off-diagonal of the (symmetric tri-) diagonal matrix.
*>          E(1) is the (1,2) and (2,1) element, E(2) is the (2,3) and
*>          (3,2) element, etc.
*>          Not referenced if KBAND=0.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is COMPLEX*16 array, dimension (LDU, N)
*>          If ITYPE=1 or 3, this contains the unitary matrix in
*>          the decomposition, expressed as a dense matrix.  If ITYPE=2,
*>          then it is not referenced.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  LDU must be at least N and
*>          at least 1.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is COMPLEX*16 array, dimension (LDV, N)
*>          If ITYPE=2 or 3, the columns of this array contain the
*>          Householder vectors used to describe the unitary matrix
*>          in the decomposition.  If UPLO='L', then the vectors are in
*>          the lower triangle, if UPLO='U', then in the upper
*>          triangle.
*>          *NOTE* If ITYPE=2 or 3, V is modified and restored.  The
*>          subdiagonal (if UPLO='L') or the superdiagonal (if UPLO='U')
*>          is set to one, and later reset to its original value, during
*>          the course of the calculation.
*>          If ITYPE=1, then it is neither referenced nor modified.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of V.  LDV must be at least N and
*>          at least 1.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array, dimension (N)
*>          If ITYPE >= 2, then TAU(j) is the scalar factor of
*>          v(j) v(j)* in the Householder transformation H(j) of
*>          the product  U = H(1)...H(n-2)
*>          If ITYPE < 2, then TAU is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (2*N**2)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (2)
*>          The values computed by the two tests described above.  The
*>          values are currently limited to 1/ulp, to avoid overflow.
*>          RESULT(1) is always modified.  RESULT(2) is modified only
*>          if ITYPE=1.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZHET21( ITYPE, UPLO, N, KBAND, A, LDA, D, E, U, LDU, V,
     $                   LDV, TAU, WORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            ITYPE, KBAND, LDA, LDU, LDV, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * ), E( * ), RESULT( 2 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), TAU( * ), U( LDU, * ),
     $                   V( LDV, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TEN = 10.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LOWER
      CHARACTER          CUPLO
      INTEGER            IINFO, J, JCOL, JR, JROW
      DOUBLE PRECISION   ANORM, ULP, UNFL, WNORM
      COMPLEX*16         VSAVE
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, ZLANGE, ZLANHE
      EXTERNAL           LSAME, DLAMCH, ZLANGE, ZLANHE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZHER, ZHER2, ZLACPY, ZLARFY, ZLASET,
     $                   ZUNM2L, ZUNM2R
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCMPLX, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      RESULT( 1 ) = ZERO
      IF( ITYPE.EQ.1 )
     $   RESULT( 2 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         LOWER = .FALSE.
         CUPLO = 'U'
      ELSE
         LOWER = .TRUE.
         CUPLO = 'L'
      END IF
*
      UNFL = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
*
*     Some Error Checks
*
      IF( ITYPE.LT.1 .OR. ITYPE.GT.3 ) THEN
         RESULT( 1 ) = TEN / ULP
         RETURN
      END IF
*
*     Do Test 1
*
*     Norm of A:
*
      IF( ITYPE.EQ.3 ) THEN
         ANORM = ONE
      ELSE
         ANORM = MAX( ZLANHE( '1', CUPLO, N, A, LDA, RWORK ), UNFL )
      END IF
*
*     Compute error matrix:
*
      IF( ITYPE.EQ.1 ) THEN
*
*        ITYPE=1: error = A - U S U*
*
         CALL ZLASET( 'Full', N, N, CZERO, CZERO, WORK, N )
         CALL ZLACPY( CUPLO, N, N, A, LDA, WORK, N )
*
         DO 10 J = 1, N
            CALL ZHER( CUPLO, N, -D( J ), U( 1, J ), 1, WORK, N )
   10    CONTINUE
*
         IF( N.GT.1 .AND. KBAND.EQ.1 ) THEN
            DO 20 J = 1, N - 1
               CALL ZHER2( CUPLO, N, -DCMPLX( E( J ) ), U( 1, J ), 1,
     $                     U( 1, J-1 ), 1, WORK, N )
   20       CONTINUE
         END IF
         WNORM = ZLANHE( '1', CUPLO, N, WORK, N, RWORK )
*
      ELSE IF( ITYPE.EQ.2 ) THEN
*
*        ITYPE=2: error = V S V* - A
*
         CALL ZLASET( 'Full', N, N, CZERO, CZERO, WORK, N )
*
         IF( LOWER ) THEN
            WORK( N**2 ) = D( N )
            DO 40 J = N - 1, 1, -1
               IF( KBAND.EQ.1 ) THEN
                  WORK( ( N+1 )*( J-1 )+2 ) = ( CONE-TAU( J ) )*E( J )
                  DO 30 JR = J + 2, N
                     WORK( ( J-1 )*N+JR ) = -TAU( J )*E( J )*V( JR, J )
   30             CONTINUE
               END IF
*
               VSAVE = V( J+1, J )
               V( J+1, J ) = ONE
               CALL ZLARFY( 'L', N-J, V( J+1, J ), 1, TAU( J ),
     $                      WORK( ( N+1 )*J+1 ), N, WORK( N**2+1 ) )
               V( J+1, J ) = VSAVE
               WORK( ( N+1 )*( J-1 )+1 ) = D( J )
   40       CONTINUE
         ELSE
            WORK( 1 ) = D( 1 )
            DO 60 J = 1, N - 1
               IF( KBAND.EQ.1 ) THEN
                  WORK( ( N+1 )*J ) = ( CONE-TAU( J ) )*E( J )
                  DO 50 JR = 1, J - 1
                     WORK( J*N+JR ) = -TAU( J )*E( J )*V( JR, J+1 )
   50             CONTINUE
               END IF
*
               VSAVE = V( J, J+1 )
               V( J, J+1 ) = ONE
               CALL ZLARFY( 'U', J, V( 1, J+1 ), 1, TAU( J ), WORK, N,
     $                      WORK( N**2+1 ) )
               V( J, J+1 ) = VSAVE
               WORK( ( N+1 )*J+1 ) = D( J+1 )
   60       CONTINUE
         END IF
*
         DO 90 JCOL = 1, N
            IF( LOWER ) THEN
               DO 70 JROW = JCOL, N
                  WORK( JROW+N*( JCOL-1 ) ) = WORK( JROW+N*( JCOL-1 ) )
     $                - A( JROW, JCOL )
   70          CONTINUE
            ELSE
               DO 80 JROW = 1, JCOL
                  WORK( JROW+N*( JCOL-1 ) ) = WORK( JROW+N*( JCOL-1 ) )
     $                - A( JROW, JCOL )
   80          CONTINUE
            END IF
   90    CONTINUE
         WNORM = ZLANHE( '1', CUPLO, N, WORK, N, RWORK )
*
      ELSE IF( ITYPE.EQ.3 ) THEN
*
*        ITYPE=3: error = U V* - I
*
         IF( N.LT.2 )
     $      RETURN
         CALL ZLACPY( ' ', N, N, U, LDU, WORK, N )
         IF( LOWER ) THEN
            CALL ZUNM2R( 'R', 'C', N, N-1, N-1, V( 2, 1 ), LDV, TAU,
     $                   WORK( N+1 ), N, WORK( N**2+1 ), IINFO )
         ELSE
            CALL ZUNM2L( 'R', 'C', N, N-1, N-1, V( 1, 2 ), LDV, TAU,
     $                   WORK, N, WORK( N**2+1 ), IINFO )
         END IF
         IF( IINFO.NE.0 ) THEN
            RESULT( 1 ) = TEN / ULP
            RETURN
         END IF
*
         DO 100 J = 1, N
            WORK( ( N+1 )*( J-1 )+1 ) = WORK( ( N+1 )*( J-1 )+1 ) - CONE
  100    CONTINUE
*
         WNORM = ZLANGE( '1', N, N, WORK, N, RWORK )
      END IF
*
      IF( ANORM.GT.WNORM ) THEN
         RESULT( 1 ) = ( WNORM / ANORM ) / ( N*ULP )
      ELSE
         IF( ANORM.LT.ONE ) THEN
            RESULT( 1 ) = ( MIN( WNORM, N*ANORM ) / ANORM ) / ( N*ULP )
         ELSE
            RESULT( 1 ) = MIN( WNORM / ANORM, DBLE( N ) ) / ( N*ULP )
         END IF
      END IF
*
*     Do Test 2
*
*     Compute  UU* - I
*
      IF( ITYPE.EQ.1 ) THEN
         CALL ZGEMM( 'N', 'C', N, N, N, CONE, U, LDU, U, LDU, CZERO,
     $               WORK, N )
*
         DO 110 J = 1, N
            WORK( ( N+1 )*( J-1 )+1 ) = WORK( ( N+1 )*( J-1 )+1 ) - CONE
  110    CONTINUE
*
         RESULT( 2 ) = MIN( ZLANGE( '1', N, N, WORK, N, RWORK ),
     $                 DBLE( N ) ) / ( N*ULP )
      END IF
*
      RETURN
*
*     End of ZHET21
*
      END
