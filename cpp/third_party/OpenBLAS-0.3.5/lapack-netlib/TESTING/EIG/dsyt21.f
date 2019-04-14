*> \brief \b DSYT21
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSYT21( ITYPE, UPLO, N, KBAND, A, LDA, D, E, U, LDU, V,
*                          LDV, TAU, WORK, RESULT )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            ITYPE, KBAND, LDA, LDU, LDV, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), D( * ), E( * ), RESULT( 2 ),
*      $                   TAU( * ), U( LDU, * ), V( LDV, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSYT21 generally checks a decomposition of the form
*>
*>    A = U S U'
*>
*> where ' means transpose, A is symmetric, U is orthogonal, and S is
*> diagonal (if KBAND=0) or symmetric tridiagonal (if KBAND=1).
*>
*> If ITYPE=1, then U is represented as a dense matrix; otherwise U is
*> expressed as a product of Householder transformations, whose vectors
*> are stored in the array "V" and whose scaling constants are in "TAU".
*> We shall use the letter "V" to refer to the product of Householder
*> transformations (which should be equal to U).
*>
*> Specifically, if ITYPE=1, then:
*>
*>    RESULT(1) = | A - U S U' | / ( |A| n ulp ) *andC>    RESULT(2) = | I - UU' | / ( n ulp )
*>
*> If ITYPE=2, then:
*>
*>    RESULT(1) = | A - V S V' | / ( |A| n ulp )
*>
*> If ITYPE=3, then:
*>
*>    RESULT(1) = | I - VU' | / ( n ulp )
*>
*> For ITYPE > 1, the transformation U is expressed as a product
*> V = H(1)...H(n-2),  where H(j) = I  -  tau(j) v(j) v(j)' and each
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
*>          1: U expressed as a dense orthogonal matrix:
*>             RESULT(1) = | A - U S U' | / ( |A| n ulp )   *andC>             RESULT(2) = | I - UU' | / ( n ulp )
*>
*>          2: U expressed as a product V of Housholder transformations:
*>             RESULT(1) = | A - V S V' | / ( |A| n ulp )
*>
*>          3: U expressed both as a dense orthogonal matrix and
*>             as a product of Housholder transformations:
*>             RESULT(1) = | I - VU' | / ( n ulp )
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
*>          The size of the matrix.  If it is zero, DSYT21 does nothing.
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
*>          A is DOUBLE PRECISION array, dimension (LDA, N)
*>          The original (unfactored) matrix.  It is assumed to be
*>          symmetric, and only the upper (UPLO='U') or only the lower
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
*>          U is DOUBLE PRECISION array, dimension (LDU, N)
*>          If ITYPE=1 or 3, this contains the orthogonal matrix in
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
*>          V is DOUBLE PRECISION array, dimension (LDV, N)
*>          If ITYPE=2 or 3, the columns of this array contain the
*>          Householder vectors used to describe the orthogonal matrix
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
*>          TAU is DOUBLE PRECISION array, dimension (N)
*>          If ITYPE >= 2, then TAU(j) is the scalar factor of
*>          v(j) v(j)' in the Householder transformation H(j) of
*>          the product  U = H(1)...H(n-2)
*>          If ITYPE < 2, then TAU is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (2*N**2)
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DSYT21( ITYPE, UPLO, N, KBAND, A, LDA, D, E, U, LDU, V,
     $                   LDV, TAU, WORK, RESULT )
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
      DOUBLE PRECISION   A( LDA, * ), D( * ), E( * ), RESULT( 2 ),
     $                   TAU( * ), U( LDU, * ), V( LDV, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TEN = 10.0D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LOWER
      CHARACTER          CUPLO
      INTEGER            IINFO, J, JCOL, JR, JROW
      DOUBLE PRECISION   ANORM, ULP, UNFL, VSAVE, WNORM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLANGE, DLANSY
      EXTERNAL           LSAME, DLAMCH, DLANGE, DLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLACPY, DLARFY, DLASET, DORM2L, DORM2R,
     $                   DSYR, DSYR2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
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
         ANORM = MAX( DLANSY( '1', CUPLO, N, A, LDA, WORK ), UNFL )
      END IF
*
*     Compute error matrix:
*
      IF( ITYPE.EQ.1 ) THEN
*
*        ITYPE=1: error = A - U S U'
*
         CALL DLASET( 'Full', N, N, ZERO, ZERO, WORK, N )
         CALL DLACPY( CUPLO, N, N, A, LDA, WORK, N )
*
         DO 10 J = 1, N
            CALL DSYR( CUPLO, N, -D( J ), U( 1, J ), 1, WORK, N )
   10    CONTINUE
*
         IF( N.GT.1 .AND. KBAND.EQ.1 ) THEN
            DO 20 J = 1, N - 1
               CALL DSYR2( CUPLO, N, -E( J ), U( 1, J ), 1, U( 1, J+1 ),
     $                     1, WORK, N )
   20       CONTINUE
         END IF
         WNORM = DLANSY( '1', CUPLO, N, WORK, N, WORK( N**2+1 ) )
*
      ELSE IF( ITYPE.EQ.2 ) THEN
*
*        ITYPE=2: error = V S V' - A
*
         CALL DLASET( 'Full', N, N, ZERO, ZERO, WORK, N )
*
         IF( LOWER ) THEN
            WORK( N**2 ) = D( N )
            DO 40 J = N - 1, 1, -1
               IF( KBAND.EQ.1 ) THEN
                  WORK( ( N+1 )*( J-1 )+2 ) = ( ONE-TAU( J ) )*E( J )
                  DO 30 JR = J + 2, N
                     WORK( ( J-1 )*N+JR ) = -TAU( J )*E( J )*V( JR, J )
   30             CONTINUE
               END IF
*
               VSAVE = V( J+1, J )
               V( J+1, J ) = ONE
               CALL DLARFY( 'L', N-J, V( J+1, J ), 1, TAU( J ),
     $                      WORK( ( N+1 )*J+1 ), N, WORK( N**2+1 ) )
               V( J+1, J ) = VSAVE
               WORK( ( N+1 )*( J-1 )+1 ) = D( J )
   40       CONTINUE
         ELSE
            WORK( 1 ) = D( 1 )
            DO 60 J = 1, N - 1
               IF( KBAND.EQ.1 ) THEN
                  WORK( ( N+1 )*J ) = ( ONE-TAU( J ) )*E( J )
                  DO 50 JR = 1, J - 1
                     WORK( J*N+JR ) = -TAU( J )*E( J )*V( JR, J+1 )
   50             CONTINUE
               END IF
*
               VSAVE = V( J, J+1 )
               V( J, J+1 ) = ONE
               CALL DLARFY( 'U', J, V( 1, J+1 ), 1, TAU( J ), WORK, N,
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
         WNORM = DLANSY( '1', CUPLO, N, WORK, N, WORK( N**2+1 ) )
*
      ELSE IF( ITYPE.EQ.3 ) THEN
*
*        ITYPE=3: error = U V' - I
*
         IF( N.LT.2 )
     $      RETURN
         CALL DLACPY( ' ', N, N, U, LDU, WORK, N )
         IF( LOWER ) THEN
            CALL DORM2R( 'R', 'T', N, N-1, N-1, V( 2, 1 ), LDV, TAU,
     $                   WORK( N+1 ), N, WORK( N**2+1 ), IINFO )
         ELSE
            CALL DORM2L( 'R', 'T', N, N-1, N-1, V( 1, 2 ), LDV, TAU,
     $                   WORK, N, WORK( N**2+1 ), IINFO )
         END IF
         IF( IINFO.NE.0 ) THEN
            RESULT( 1 ) = TEN / ULP
            RETURN
         END IF
*
         DO 100 J = 1, N
            WORK( ( N+1 )*( J-1 )+1 ) = WORK( ( N+1 )*( J-1 )+1 ) - ONE
  100    CONTINUE
*
         WNORM = DLANGE( '1', N, N, WORK, N, WORK( N**2+1 ) )
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
*     Compute  UU' - I
*
      IF( ITYPE.EQ.1 ) THEN
         CALL DGEMM( 'N', 'C', N, N, N, ONE, U, LDU, U, LDU, ZERO, WORK,
     $               N )
*
         DO 110 J = 1, N
            WORK( ( N+1 )*( J-1 )+1 ) = WORK( ( N+1 )*( J-1 )+1 ) - ONE
  110    CONTINUE
*
         RESULT( 2 ) = MIN( DLANGE( '1', N, N, WORK, N,
     $                 WORK( N**2+1 ) ), DBLE( N ) ) / ( N*ULP )
      END IF
*
      RETURN
*
*     End of DSYT21
*
      END
