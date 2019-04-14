*> \brief \b CSBMV
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CSBMV( UPLO, N, K, ALPHA, A, LDA, X, INCX, BETA, Y,
*                         INCY )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INCX, INCY, K, LDA, N
*       COMPLEX            ALPHA, BETA
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), X( * ), Y( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CSBMV  performs the matrix-vector  operation
*>
*>    y := alpha*A*x + beta*y,
*>
*> where alpha and beta are scalars, x and y are n element vectors and
*> A is an n by n symmetric band matrix, with k super-diagonals.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  UPLO   - CHARACTER*1
*>           On entry, UPLO specifies whether the upper or lower
*>           triangular part of the band matrix A is being supplied as
*>           follows:
*>
*>              UPLO = 'U' or 'u'   The upper triangular part of A is
*>                                  being supplied.
*>
*>              UPLO = 'L' or 'l'   The lower triangular part of A is
*>                                  being supplied.
*>
*>           Unchanged on exit.
*>
*>  N      - INTEGER
*>           On entry, N specifies the order of the matrix A.
*>           N must be at least zero.
*>           Unchanged on exit.
*>
*>  K      - INTEGER
*>           On entry, K specifies the number of super-diagonals of the
*>           matrix A. K must satisfy  0 .le. K.
*>           Unchanged on exit.
*>
*>  ALPHA  - COMPLEX
*>           On entry, ALPHA specifies the scalar alpha.
*>           Unchanged on exit.
*>
*>  A      - COMPLEX array, dimension( LDA, N )
*>           Before entry with UPLO = 'U' or 'u', the leading ( k + 1 )
*>           by n part of the array A must contain the upper triangular
*>           band part of the symmetric matrix, supplied column by
*>           column, with the leading diagonal of the matrix in row
*>           ( k + 1 ) of the array, the first super-diagonal starting at
*>           position 2 in row k, and so on. The top left k by k triangle
*>           of the array A is not referenced.
*>           The following program segment will transfer the upper
*>           triangular part of a symmetric band matrix from conventional
*>           full matrix storage to band storage:
*>
*>                 DO 20, J = 1, N
*>                    M = K + 1 - J
*>                    DO 10, I = MAX( 1, J - K ), J
*>                       A( M + I, J ) = matrix( I, J )
*>              10    CONTINUE
*>              20 CONTINUE
*>
*>           Before entry with UPLO = 'L' or 'l', the leading ( k + 1 )
*>           by n part of the array A must contain the lower triangular
*>           band part of the symmetric matrix, supplied column by
*>           column, with the leading diagonal of the matrix in row 1 of
*>           the array, the first sub-diagonal starting at position 1 in
*>           row 2, and so on. The bottom right k by k triangle of the
*>           array A is not referenced.
*>           The following program segment will transfer the lower
*>           triangular part of a symmetric band matrix from conventional
*>           full matrix storage to band storage:
*>
*>                 DO 20, J = 1, N
*>                    M = 1 - J
*>                    DO 10, I = J, MIN( N, J + K )
*>                       A( M + I, J ) = matrix( I, J )
*>              10    CONTINUE
*>              20 CONTINUE
*>
*>           Unchanged on exit.
*>
*>  LDA    - INTEGER
*>           On entry, LDA specifies the first dimension of A as declared
*>           in the calling (sub) program. LDA must be at least
*>           ( k + 1 ).
*>           Unchanged on exit.
*>
*>  X      - COMPLEX array, dimension at least
*>           ( 1 + ( N - 1 )*abs( INCX ) ).
*>           Before entry, the incremented array X must contain the
*>           vector x.
*>           Unchanged on exit.
*>
*>  INCX   - INTEGER
*>           On entry, INCX specifies the increment for the elements of
*>           X. INCX must not be zero.
*>           Unchanged on exit.
*>
*>  BETA   - COMPLEX
*>           On entry, BETA specifies the scalar beta.
*>           Unchanged on exit.
*>
*>  Y      - COMPLEX array, dimension at least
*>           ( 1 + ( N - 1 )*abs( INCY ) ).
*>           Before entry, the incremented array Y must contain the
*>           vector y. On exit, Y is overwritten by the updated vector y.
*>
*>  INCY   - INTEGER
*>           On entry, INCY specifies the increment for the elements of
*>           Y. INCY must not be zero.
*>           Unchanged on exit.
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
      SUBROUTINE CSBMV( UPLO, N, K, ALPHA, A, LDA, X, INCX, BETA, Y,
     $                  INCY )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INCX, INCY, K, LDA, N
      COMPLEX            ALPHA, BETA
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), X( * ), Y( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ONE
      PARAMETER          ( ONE = ( 1.0E+0, 0.0E+0 ) )
      COMPLEX            ZERO
      PARAMETER          ( ZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, IX, IY, J, JX, JY, KPLUS1, KX, KY, L
      COMPLEX            TEMP1, TEMP2
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = 1
      ELSE IF( N.LT.0 ) THEN
         INFO = 2
      ELSE IF( K.LT.0 ) THEN
         INFO = 3
      ELSE IF( LDA.LT.( K+1 ) ) THEN
         INFO = 6
      ELSE IF( INCX.EQ.0 ) THEN
         INFO = 8
      ELSE IF( INCY.EQ.0 ) THEN
         INFO = 11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CSBMV ', INFO )
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( ( N.EQ.0 ) .OR. ( ( ALPHA.EQ.ZERO ) .AND. ( BETA.EQ.ONE ) ) )
     $   RETURN
*
*     Set up the start points in  X  and  Y.
*
      IF( INCX.GT.0 ) THEN
         KX = 1
      ELSE
         KX = 1 - ( N-1 )*INCX
      END IF
      IF( INCY.GT.0 ) THEN
         KY = 1
      ELSE
         KY = 1 - ( N-1 )*INCY
      END IF
*
*     Start the operations. In this version the elements of the array A
*     are accessed sequentially with one pass through A.
*
*     First form  y := beta*y.
*
      IF( BETA.NE.ONE ) THEN
         IF( INCY.EQ.1 ) THEN
            IF( BETA.EQ.ZERO ) THEN
               DO 10 I = 1, N
                  Y( I ) = ZERO
   10          CONTINUE
            ELSE
               DO 20 I = 1, N
                  Y( I ) = BETA*Y( I )
   20          CONTINUE
            END IF
         ELSE
            IY = KY
            IF( BETA.EQ.ZERO ) THEN
               DO 30 I = 1, N
                  Y( IY ) = ZERO
                  IY = IY + INCY
   30          CONTINUE
            ELSE
               DO 40 I = 1, N
                  Y( IY ) = BETA*Y( IY )
                  IY = IY + INCY
   40          CONTINUE
            END IF
         END IF
      END IF
      IF( ALPHA.EQ.ZERO )
     $   RETURN
      IF( LSAME( UPLO, 'U' ) ) THEN
*
*        Form  y  when upper triangle of A is stored.
*
         KPLUS1 = K + 1
         IF( ( INCX.EQ.1 ) .AND. ( INCY.EQ.1 ) ) THEN
            DO 60 J = 1, N
               TEMP1 = ALPHA*X( J )
               TEMP2 = ZERO
               L = KPLUS1 - J
               DO 50 I = MAX( 1, J-K ), J - 1
                  Y( I ) = Y( I ) + TEMP1*A( L+I, J )
                  TEMP2 = TEMP2 + A( L+I, J )*X( I )
   50          CONTINUE
               Y( J ) = Y( J ) + TEMP1*A( KPLUS1, J ) + ALPHA*TEMP2
   60       CONTINUE
         ELSE
            JX = KX
            JY = KY
            DO 80 J = 1, N
               TEMP1 = ALPHA*X( JX )
               TEMP2 = ZERO
               IX = KX
               IY = KY
               L = KPLUS1 - J
               DO 70 I = MAX( 1, J-K ), J - 1
                  Y( IY ) = Y( IY ) + TEMP1*A( L+I, J )
                  TEMP2 = TEMP2 + A( L+I, J )*X( IX )
                  IX = IX + INCX
                  IY = IY + INCY
   70          CONTINUE
               Y( JY ) = Y( JY ) + TEMP1*A( KPLUS1, J ) + ALPHA*TEMP2
               JX = JX + INCX
               JY = JY + INCY
               IF( J.GT.K ) THEN
                  KX = KX + INCX
                  KY = KY + INCY
               END IF
   80       CONTINUE
         END IF
      ELSE
*
*        Form  y  when lower triangle of A is stored.
*
         IF( ( INCX.EQ.1 ) .AND. ( INCY.EQ.1 ) ) THEN
            DO 100 J = 1, N
               TEMP1 = ALPHA*X( J )
               TEMP2 = ZERO
               Y( J ) = Y( J ) + TEMP1*A( 1, J )
               L = 1 - J
               DO 90 I = J + 1, MIN( N, J+K )
                  Y( I ) = Y( I ) + TEMP1*A( L+I, J )
                  TEMP2 = TEMP2 + A( L+I, J )*X( I )
   90          CONTINUE
               Y( J ) = Y( J ) + ALPHA*TEMP2
  100       CONTINUE
         ELSE
            JX = KX
            JY = KY
            DO 120 J = 1, N
               TEMP1 = ALPHA*X( JX )
               TEMP2 = ZERO
               Y( JY ) = Y( JY ) + TEMP1*A( 1, J )
               L = 1 - J
               IX = JX
               IY = JY
               DO 110 I = J + 1, MIN( N, J+K )
                  IX = IX + INCX
                  IY = IY + INCY
                  Y( IY ) = Y( IY ) + TEMP1*A( L+I, J )
                  TEMP2 = TEMP2 + A( L+I, J )*X( IX )
  110          CONTINUE
               Y( JY ) = Y( JY ) + ALPHA*TEMP2
               JX = JX + INCX
               JY = JY + INCY
  120       CONTINUE
         END IF
      END IF
*
      RETURN
*
*     End of CSBMV
*
      END
