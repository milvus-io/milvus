*> \brief \b SLANTR returns the value of the 1-norm, or the Frobenius norm, or the infinity norm, or the element of largest absolute value of a trapezoidal or triangular matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLANTR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slantr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slantr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slantr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       REAL             FUNCTION SLANTR( NORM, UPLO, DIAG, M, N, A, LDA,
*                        WORK )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, NORM, UPLO
*       INTEGER            LDA, M, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLANTR  returns the value of the one norm,  or the Frobenius norm, or
*> the  infinity norm,  or the  element of  largest absolute value  of a
*> trapezoidal or triangular matrix A.
*> \endverbatim
*>
*> \return SLANTR
*> \verbatim
*>
*>    SLANTR = ( max(abs(A(i,j))), NORM = 'M' or 'm'
*>             (
*>             ( norm1(A),         NORM = '1', 'O' or 'o'
*>             (
*>             ( normI(A),         NORM = 'I' or 'i'
*>             (
*>             ( normF(A),         NORM = 'F', 'f', 'E' or 'e'
*>
*> where  norm1  denotes the  one norm of a matrix (maximum column sum),
*> normI  denotes the  infinity norm  of a matrix  (maximum row sum) and
*> normF  denotes the  Frobenius norm of a matrix (square root of sum of
*> squares).  Note that  max(abs(A(i,j)))  is not a consistent matrix norm.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NORM
*> \verbatim
*>          NORM is CHARACTER*1
*>          Specifies the value to be returned in SLANTR as described
*>          above.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the matrix A is upper or lower trapezoidal.
*>          = 'U':  Upper trapezoidal
*>          = 'L':  Lower trapezoidal
*>          Note that A is triangular instead of trapezoidal if M = N.
*> \endverbatim
*>
*> \param[in] DIAG
*> \verbatim
*>          DIAG is CHARACTER*1
*>          Specifies whether or not the matrix A has unit diagonal.
*>          = 'N':  Non-unit diagonal
*>          = 'U':  Unit diagonal
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0, and if
*>          UPLO = 'U', M <= N.  When M = 0, SLANTR is set to zero.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0, and if
*>          UPLO = 'L', N <= M.  When N = 0, SLANTR is set to zero.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          The trapezoidal matrix A (A is triangular if M = N).
*>          If UPLO = 'U', the leading m by n upper trapezoidal part of
*>          the array A contains the upper trapezoidal matrix, and the
*>          strictly lower triangular part of A is not referenced.
*>          If UPLO = 'L', the leading m by n lower trapezoidal part of
*>          the array A contains the lower trapezoidal matrix, and the
*>          strictly upper triangular part of A is not referenced.  Note
*>          that when DIAG = 'U', the diagonal elements of A are not
*>          referenced and are assumed to be one.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(M,1).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK)),
*>          where LWORK >= M when NORM = 'I'; otherwise, WORK is not
*>          referenced.
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
*> \ingroup realOTHERauxiliary
*
*  =====================================================================
      REAL             FUNCTION SLANTR( NORM, UPLO, DIAG, M, N, A, LDA,
     $                 WORK )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, NORM, UPLO
      INTEGER            LDA, M, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), WORK( * )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UDIAG
      INTEGER            I, J
      REAL               SCALE, SUM, VALUE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLASSQ
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, SISNAN
      EXTERNAL           LSAME, SISNAN
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
      IF( MIN( M, N ).EQ.0 ) THEN
         VALUE = ZERO
      ELSE IF( LSAME( NORM, 'M' ) ) THEN
*
*        Find max(abs(A(i,j))).
*
         IF( LSAME( DIAG, 'U' ) ) THEN
            VALUE = ONE
            IF( LSAME( UPLO, 'U' ) ) THEN
               DO 20 J = 1, N
                  DO 10 I = 1, MIN( M, J-1 )
                     SUM = ABS( A( I, J ) )
                     IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
   10             CONTINUE
   20          CONTINUE
            ELSE
               DO 40 J = 1, N
                  DO 30 I = J + 1, M
                     SUM = ABS( A( I, J ) )
                     IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
   30             CONTINUE
   40          CONTINUE
            END IF
         ELSE
            VALUE = ZERO
            IF( LSAME( UPLO, 'U' ) ) THEN
               DO 60 J = 1, N
                  DO 50 I = 1, MIN( M, J )
                     SUM = ABS( A( I, J ) )
                     IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
   50             CONTINUE
   60          CONTINUE
            ELSE
               DO 80 J = 1, N
                  DO 70 I = J, M
                     SUM = ABS( A( I, J ) )
                     IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
   70             CONTINUE
   80          CONTINUE
            END IF
         END IF
      ELSE IF( ( LSAME( NORM, 'O' ) ) .OR. ( NORM.EQ.'1' ) ) THEN
*
*        Find norm1(A).
*
         VALUE = ZERO
         UDIAG = LSAME( DIAG, 'U' )
         IF( LSAME( UPLO, 'U' ) ) THEN
            DO 110 J = 1, N
               IF( ( UDIAG ) .AND. ( J.LE.M ) ) THEN
                  SUM = ONE
                  DO 90 I = 1, J - 1
                     SUM = SUM + ABS( A( I, J ) )
   90             CONTINUE
               ELSE
                  SUM = ZERO
                  DO 100 I = 1, MIN( M, J )
                     SUM = SUM + ABS( A( I, J ) )
  100             CONTINUE
               END IF
               IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
  110       CONTINUE
         ELSE
            DO 140 J = 1, N
               IF( UDIAG ) THEN
                  SUM = ONE
                  DO 120 I = J + 1, M
                     SUM = SUM + ABS( A( I, J ) )
  120             CONTINUE
               ELSE
                  SUM = ZERO
                  DO 130 I = J, M
                     SUM = SUM + ABS( A( I, J ) )
  130             CONTINUE
               END IF
               IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
  140       CONTINUE
         END IF
      ELSE IF( LSAME( NORM, 'I' ) ) THEN
*
*        Find normI(A).
*
         IF( LSAME( UPLO, 'U' ) ) THEN
            IF( LSAME( DIAG, 'U' ) ) THEN
               DO 150 I = 1, M
                  WORK( I ) = ONE
  150          CONTINUE
               DO 170 J = 1, N
                  DO 160 I = 1, MIN( M, J-1 )
                     WORK( I ) = WORK( I ) + ABS( A( I, J ) )
  160             CONTINUE
  170          CONTINUE
            ELSE
               DO 180 I = 1, M
                  WORK( I ) = ZERO
  180          CONTINUE
               DO 200 J = 1, N
                  DO 190 I = 1, MIN( M, J )
                     WORK( I ) = WORK( I ) + ABS( A( I, J ) )
  190             CONTINUE
  200          CONTINUE
            END IF
         ELSE
            IF( LSAME( DIAG, 'U' ) ) THEN
               DO 210 I = 1, N
                  WORK( I ) = ONE
  210          CONTINUE
               DO 220 I = N + 1, M
                  WORK( I ) = ZERO
  220          CONTINUE
               DO 240 J = 1, N
                  DO 230 I = J + 1, M
                     WORK( I ) = WORK( I ) + ABS( A( I, J ) )
  230             CONTINUE
  240          CONTINUE
            ELSE
               DO 250 I = 1, M
                  WORK( I ) = ZERO
  250          CONTINUE
               DO 270 J = 1, N
                  DO 260 I = J, M
                     WORK( I ) = WORK( I ) + ABS( A( I, J ) )
  260             CONTINUE
  270          CONTINUE
            END IF
         END IF
         VALUE = ZERO
         DO 280 I = 1, M
            SUM = WORK( I )
            IF( VALUE .LT. SUM .OR. SISNAN( SUM ) ) VALUE = SUM
  280    CONTINUE
      ELSE IF( ( LSAME( NORM, 'F' ) ) .OR. ( LSAME( NORM, 'E' ) ) ) THEN
*
*        Find normF(A).
*
         IF( LSAME( UPLO, 'U' ) ) THEN
            IF( LSAME( DIAG, 'U' ) ) THEN
               SCALE = ONE
               SUM = MIN( M, N )
               DO 290 J = 2, N
                  CALL SLASSQ( MIN( M, J-1 ), A( 1, J ), 1, SCALE, SUM )
  290          CONTINUE
            ELSE
               SCALE = ZERO
               SUM = ONE
               DO 300 J = 1, N
                  CALL SLASSQ( MIN( M, J ), A( 1, J ), 1, SCALE, SUM )
  300          CONTINUE
            END IF
         ELSE
            IF( LSAME( DIAG, 'U' ) ) THEN
               SCALE = ONE
               SUM = MIN( M, N )
               DO 310 J = 1, N
                  CALL SLASSQ( M-J, A( MIN( M, J+1 ), J ), 1, SCALE,
     $                         SUM )
  310          CONTINUE
            ELSE
               SCALE = ZERO
               SUM = ONE
               DO 320 J = 1, N
                  CALL SLASSQ( M-J+1, A( J, J ), 1, SCALE, SUM )
  320          CONTINUE
            END IF
         END IF
         VALUE = SCALE*SQRT( SUM )
      END IF
*
      SLANTR = VALUE
      RETURN
*
*     End of SLANTR
*
      END
