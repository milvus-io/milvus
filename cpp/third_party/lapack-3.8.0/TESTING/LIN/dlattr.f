*> \brief \b DLATTR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLATTR( IMAT, UPLO, TRANS, DIAG, ISEED, N, A, LDA, B,
*                          WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, TRANS, UPLO
*       INTEGER            IMAT, INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 )
*       DOUBLE PRECISION   A( LDA, * ), B( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLATTR generates a triangular test matrix.
*> IMAT and UPLO uniquely specify the properties of the test
*> matrix, which is returned in the array A.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IMAT
*> \verbatim
*>          IMAT is INTEGER
*>          An integer key describing which matrix to generate for this
*>          path.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the matrix A will be upper or lower
*>          triangular.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies whether the matrix or its transpose will be used.
*>          = 'N':  No transpose
*>          = 'T':  Transpose
*>          = 'C':  Conjugate transpose (= Transpose)
*> \endverbatim
*>
*> \param[out] DIAG
*> \verbatim
*>          DIAG is CHARACTER*1
*>          Specifies whether or not the matrix A is unit triangular.
*>          = 'N':  Non-unit triangular
*>          = 'U':  Unit triangular
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          The seed vector for the random number generator (used in
*>          DLATMS).  Modified on exit.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The triangular matrix A.  If UPLO = 'U', the leading n by n
*>          upper triangular part of the array A contains the upper
*>          triangular matrix, and the strictly lower triangular part of
*>          A is not referenced.  If UPLO = 'L', the leading n by n lower
*>          triangular part of the array A contains the lower triangular
*>          matrix, and the strictly upper triangular part of A is not
*>          referenced.  If DIAG = 'U', the diagonal elements of A are
*>          set so that A(k,k) = k for 1 <= k <= n.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (N)
*>          The right hand side vector, if IMAT > 10.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (3*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0: if INFO = -k, the k-th argument had an illegal value
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DLATTR( IMAT, UPLO, TRANS, DIAG, ISEED, N, A, LDA, B,
     $                   WORK, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, TRANS, UPLO
      INTEGER            IMAT, INFO, LDA, N
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
      DOUBLE PRECISION   A( LDA, * ), B( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, TWO, ZERO
      PARAMETER          ( ONE = 1.0D+0, TWO = 2.0D+0, ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      CHARACTER          DIST, TYPE
      CHARACTER*3        PATH
      INTEGER            I, IY, J, JCOUNT, KL, KU, MODE
      DOUBLE PRECISION   ANORM, BIGNUM, BNORM, BSCAL, C, CNDNUM, PLUS1,
     $                   PLUS2, RA, RB, REXP, S, SFAC, SMLNUM, STAR1,
     $                   TEXP, TLEFT, TSCAL, ULP, UNFL, X, Y, Z
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            IDAMAX
      DOUBLE PRECISION   DLAMCH, DLARND
      EXTERNAL           LSAME, IDAMAX, DLAMCH, DLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLABAD, DLARNV, DLATB4, DLATMS, DROT,
     $                   DROTG, DSCAL, DSWAP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
      PATH( 1: 1 ) = 'Double precision'
      PATH( 2: 3 ) = 'TR'
      UNFL = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
      SMLNUM = UNFL
      BIGNUM = ( ONE-ULP ) / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
      IF( ( IMAT.GE.7 .AND. IMAT.LE.10 ) .OR. IMAT.EQ.18 ) THEN
         DIAG = 'U'
      ELSE
         DIAG = 'N'
      END IF
      INFO = 0
*
*     Quick return if N.LE.0.
*
      IF( N.LE.0 )
     $   RETURN
*
*     Call DLATB4 to set parameters for SLATMS.
*
      UPPER = LSAME( UPLO, 'U' )
      IF( UPPER ) THEN
         CALL DLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM, MODE,
     $                CNDNUM, DIST )
      ELSE
         CALL DLATB4( PATH, -IMAT, N, N, TYPE, KL, KU, ANORM, MODE,
     $                CNDNUM, DIST )
      END IF
*
*     IMAT <= 6:  Non-unit triangular matrix
*
      IF( IMAT.LE.6 ) THEN
         CALL DLATMS( N, N, DIST, ISEED, TYPE, B, MODE, CNDNUM, ANORM,
     $                KL, KU, 'No packing', A, LDA, WORK, INFO )
*
*     IMAT > 6:  Unit triangular matrix
*     The diagonal is deliberately set to something other than 1.
*
*     IMAT = 7:  Matrix is the identity
*
      ELSE IF( IMAT.EQ.7 ) THEN
         IF( UPPER ) THEN
            DO 20 J = 1, N
               DO 10 I = 1, J - 1
                  A( I, J ) = ZERO
   10          CONTINUE
               A( J, J ) = J
   20       CONTINUE
         ELSE
            DO 40 J = 1, N
               A( J, J ) = J
               DO 30 I = J + 1, N
                  A( I, J ) = ZERO
   30          CONTINUE
   40       CONTINUE
         END IF
*
*     IMAT > 7:  Non-trivial unit triangular matrix
*
*     Generate a unit triangular matrix T with condition CNDNUM by
*     forming a triangular matrix with known singular values and
*     filling in the zero entries with Givens rotations.
*
      ELSE IF( IMAT.LE.10 ) THEN
         IF( UPPER ) THEN
            DO 60 J = 1, N
               DO 50 I = 1, J - 1
                  A( I, J ) = ZERO
   50          CONTINUE
               A( J, J ) = J
   60       CONTINUE
         ELSE
            DO 80 J = 1, N
               A( J, J ) = J
               DO 70 I = J + 1, N
                  A( I, J ) = ZERO
   70          CONTINUE
   80       CONTINUE
         END IF
*
*        Since the trace of a unit triangular matrix is 1, the product
*        of its singular values must be 1.  Let s = sqrt(CNDNUM),
*        x = sqrt(s) - 1/sqrt(s), y = sqrt(2/(n-2))*x, and z = x**2.
*        The following triangular matrix has singular values s, 1, 1,
*        ..., 1, 1/s:
*
*        1  y  y  y  ...  y  y  z
*           1  0  0  ...  0  0  y
*              1  0  ...  0  0  y
*                 .  ...  .  .  .
*                     .   .  .  .
*                         1  0  y
*                            1  y
*                               1
*
*        To fill in the zeros, we first multiply by a matrix with small
*        condition number of the form
*
*        1  0  0  0  0  ...
*           1  +  *  0  0  ...
*              1  +  0  0  0
*                 1  +  *  0  0
*                    1  +  0  0
*                       ...
*                          1  +  0
*                             1  0
*                                1
*
*        Each element marked with a '*' is formed by taking the product
*        of the adjacent elements marked with '+'.  The '*'s can be
*        chosen freely, and the '+'s are chosen so that the inverse of
*        T will have elements of the same magnitude as T.  If the *'s in
*        both T and inv(T) have small magnitude, T is well conditioned.
*        The two offdiagonals of T are stored in WORK.
*
*        The product of these two matrices has the form
*
*        1  y  y  y  y  y  .  y  y  z
*           1  +  *  0  0  .  0  0  y
*              1  +  0  0  .  0  0  y
*                 1  +  *  .  .  .  .
*                    1  +  .  .  .  .
*                       .  .  .  .  .
*                          .  .  .  .
*                             1  +  y
*                                1  y
*                                   1
*
*        Now we multiply by Givens rotations, using the fact that
*
*              [  c   s ] [  1   w ] [ -c  -s ] =  [  1  -w ]
*              [ -s   c ] [  0   1 ] [  s  -c ]    [  0   1 ]
*        and
*              [ -c  -s ] [  1   0 ] [  c   s ] =  [  1   0 ]
*              [  s  -c ] [  w   1 ] [ -s   c ]    [ -w   1 ]
*
*        where c = w / sqrt(w**2+4) and s = 2 / sqrt(w**2+4).
*
         STAR1 = 0.25D0
         SFAC = 0.5D0
         PLUS1 = SFAC
         DO 90 J = 1, N, 2
            PLUS2 = STAR1 / PLUS1
            WORK( J ) = PLUS1
            WORK( N+J ) = STAR1
            IF( J+1.LE.N ) THEN
               WORK( J+1 ) = PLUS2
               WORK( N+J+1 ) = ZERO
               PLUS1 = STAR1 / PLUS2
               REXP = DLARND( 2, ISEED )
               STAR1 = STAR1*( SFAC**REXP )
               IF( REXP.LT.ZERO ) THEN
                  STAR1 = -SFAC**( ONE-REXP )
               ELSE
                  STAR1 = SFAC**( ONE+REXP )
               END IF
            END IF
   90    CONTINUE
*
         X = SQRT( CNDNUM ) - 1 / SQRT( CNDNUM )
         IF( N.GT.2 ) THEN
            Y = SQRT( 2.D0 / ( N-2 ) )*X
         ELSE
            Y = ZERO
         END IF
         Z = X*X
*
         IF( UPPER ) THEN
            IF( N.GT.3 ) THEN
               CALL DCOPY( N-3, WORK, 1, A( 2, 3 ), LDA+1 )
               IF( N.GT.4 )
     $            CALL DCOPY( N-4, WORK( N+1 ), 1, A( 2, 4 ), LDA+1 )
            END IF
            DO 100 J = 2, N - 1
               A( 1, J ) = Y
               A( J, N ) = Y
  100       CONTINUE
            A( 1, N ) = Z
         ELSE
            IF( N.GT.3 ) THEN
               CALL DCOPY( N-3, WORK, 1, A( 3, 2 ), LDA+1 )
               IF( N.GT.4 )
     $            CALL DCOPY( N-4, WORK( N+1 ), 1, A( 4, 2 ), LDA+1 )
            END IF
            DO 110 J = 2, N - 1
               A( J, 1 ) = Y
               A( N, J ) = Y
  110       CONTINUE
            A( N, 1 ) = Z
         END IF
*
*        Fill in the zeros using Givens rotations.
*
         IF( UPPER ) THEN
            DO 120 J = 1, N - 1
               RA = A( J, J+1 )
               RB = 2.0D0
               CALL DROTG( RA, RB, C, S )
*
*              Multiply by [ c  s; -s  c] on the left.
*
               IF( N.GT.J+1 )
     $            CALL DROT( N-J-1, A( J, J+2 ), LDA, A( J+1, J+2 ),
     $                       LDA, C, S )
*
*              Multiply by [-c -s;  s -c] on the right.
*
               IF( J.GT.1 )
     $            CALL DROT( J-1, A( 1, J+1 ), 1, A( 1, J ), 1, -C, -S )
*
*              Negate A(J,J+1).
*
               A( J, J+1 ) = -A( J, J+1 )
  120       CONTINUE
         ELSE
            DO 130 J = 1, N - 1
               RA = A( J+1, J )
               RB = 2.0D0
               CALL DROTG( RA, RB, C, S )
*
*              Multiply by [ c -s;  s  c] on the right.
*
               IF( N.GT.J+1 )
     $            CALL DROT( N-J-1, A( J+2, J+1 ), 1, A( J+2, J ), 1, C,
     $                       -S )
*
*              Multiply by [-c  s; -s -c] on the left.
*
               IF( J.GT.1 )
     $            CALL DROT( J-1, A( J, 1 ), LDA, A( J+1, 1 ), LDA, -C,
     $                       S )
*
*              Negate A(J+1,J).
*
               A( J+1, J ) = -A( J+1, J )
  130       CONTINUE
         END IF
*
*     IMAT > 10:  Pathological test cases.  These triangular matrices
*     are badly scaled or badly conditioned, so when used in solving a
*     triangular system they may cause overflow in the solution vector.
*
      ELSE IF( IMAT.EQ.11 ) THEN
*
*        Type 11:  Generate a triangular matrix with elements between
*        -1 and 1. Give the diagonal norm 2 to make it well-conditioned.
*        Make the right hand side large so that it requires scaling.
*
         IF( UPPER ) THEN
            DO 140 J = 1, N
               CALL DLARNV( 2, ISEED, J, A( 1, J ) )
               A( J, J ) = SIGN( TWO, A( J, J ) )
  140       CONTINUE
         ELSE
            DO 150 J = 1, N
               CALL DLARNV( 2, ISEED, N-J+1, A( J, J ) )
               A( J, J ) = SIGN( TWO, A( J, J ) )
  150       CONTINUE
         END IF
*
*        Set the right hand side so that the largest value is BIGNUM.
*
         CALL DLARNV( 2, ISEED, N, B )
         IY = IDAMAX( N, B, 1 )
         BNORM = ABS( B( IY ) )
         BSCAL = BIGNUM / MAX( ONE, BNORM )
         CALL DSCAL( N, BSCAL, B, 1 )
*
      ELSE IF( IMAT.EQ.12 ) THEN
*
*        Type 12:  Make the first diagonal element in the solve small to
*        cause immediate overflow when dividing by T(j,j).
*        In type 12, the offdiagonal elements are small (CNORM(j) < 1).
*
         CALL DLARNV( 2, ISEED, N, B )
         TSCAL = ONE / MAX( ONE, DBLE( N-1 ) )
         IF( UPPER ) THEN
            DO 160 J = 1, N
               CALL DLARNV( 2, ISEED, J, A( 1, J ) )
               CALL DSCAL( J-1, TSCAL, A( 1, J ), 1 )
               A( J, J ) = SIGN( ONE, A( J, J ) )
  160       CONTINUE
            A( N, N ) = SMLNUM*A( N, N )
         ELSE
            DO 170 J = 1, N
               CALL DLARNV( 2, ISEED, N-J+1, A( J, J ) )
               IF( N.GT.J )
     $            CALL DSCAL( N-J, TSCAL, A( J+1, J ), 1 )
               A( J, J ) = SIGN( ONE, A( J, J ) )
  170       CONTINUE
            A( 1, 1 ) = SMLNUM*A( 1, 1 )
         END IF
*
      ELSE IF( IMAT.EQ.13 ) THEN
*
*        Type 13:  Make the first diagonal element in the solve small to
*        cause immediate overflow when dividing by T(j,j).
*        In type 13, the offdiagonal elements are O(1) (CNORM(j) > 1).
*
         CALL DLARNV( 2, ISEED, N, B )
         IF( UPPER ) THEN
            DO 180 J = 1, N
               CALL DLARNV( 2, ISEED, J, A( 1, J ) )
               A( J, J ) = SIGN( ONE, A( J, J ) )
  180       CONTINUE
            A( N, N ) = SMLNUM*A( N, N )
         ELSE
            DO 190 J = 1, N
               CALL DLARNV( 2, ISEED, N-J+1, A( J, J ) )
               A( J, J ) = SIGN( ONE, A( J, J ) )
  190       CONTINUE
            A( 1, 1 ) = SMLNUM*A( 1, 1 )
         END IF
*
      ELSE IF( IMAT.EQ.14 ) THEN
*
*        Type 14:  T is diagonal with small numbers on the diagonal to
*        make the growth factor underflow, but a small right hand side
*        chosen so that the solution does not overflow.
*
         IF( UPPER ) THEN
            JCOUNT = 1
            DO 210 J = N, 1, -1
               DO 200 I = 1, J - 1
                  A( I, J ) = ZERO
  200          CONTINUE
               IF( JCOUNT.LE.2 ) THEN
                  A( J, J ) = SMLNUM
               ELSE
                  A( J, J ) = ONE
               END IF
               JCOUNT = JCOUNT + 1
               IF( JCOUNT.GT.4 )
     $            JCOUNT = 1
  210       CONTINUE
         ELSE
            JCOUNT = 1
            DO 230 J = 1, N
               DO 220 I = J + 1, N
                  A( I, J ) = ZERO
  220          CONTINUE
               IF( JCOUNT.LE.2 ) THEN
                  A( J, J ) = SMLNUM
               ELSE
                  A( J, J ) = ONE
               END IF
               JCOUNT = JCOUNT + 1
               IF( JCOUNT.GT.4 )
     $            JCOUNT = 1
  230       CONTINUE
         END IF
*
*        Set the right hand side alternately zero and small.
*
         IF( UPPER ) THEN
            B( 1 ) = ZERO
            DO 240 I = N, 2, -2
               B( I ) = ZERO
               B( I-1 ) = SMLNUM
  240       CONTINUE
         ELSE
            B( N ) = ZERO
            DO 250 I = 1, N - 1, 2
               B( I ) = ZERO
               B( I+1 ) = SMLNUM
  250       CONTINUE
         END IF
*
      ELSE IF( IMAT.EQ.15 ) THEN
*
*        Type 15:  Make the diagonal elements small to cause gradual
*        overflow when dividing by T(j,j).  To control the amount of
*        scaling needed, the matrix is bidiagonal.
*
         TEXP = ONE / MAX( ONE, DBLE( N-1 ) )
         TSCAL = SMLNUM**TEXP
         CALL DLARNV( 2, ISEED, N, B )
         IF( UPPER ) THEN
            DO 270 J = 1, N
               DO 260 I = 1, J - 2
                  A( I, J ) = 0.D0
  260          CONTINUE
               IF( J.GT.1 )
     $            A( J-1, J ) = -ONE
               A( J, J ) = TSCAL
  270       CONTINUE
            B( N ) = ONE
         ELSE
            DO 290 J = 1, N
               DO 280 I = J + 2, N
                  A( I, J ) = 0.D0
  280          CONTINUE
               IF( J.LT.N )
     $            A( J+1, J ) = -ONE
               A( J, J ) = TSCAL
  290       CONTINUE
            B( 1 ) = ONE
         END IF
*
      ELSE IF( IMAT.EQ.16 ) THEN
*
*        Type 16:  One zero diagonal element.
*
         IY = N / 2 + 1
         IF( UPPER ) THEN
            DO 300 J = 1, N
               CALL DLARNV( 2, ISEED, J, A( 1, J ) )
               IF( J.NE.IY ) THEN
                  A( J, J ) = SIGN( TWO, A( J, J ) )
               ELSE
                  A( J, J ) = ZERO
               END IF
  300       CONTINUE
         ELSE
            DO 310 J = 1, N
               CALL DLARNV( 2, ISEED, N-J+1, A( J, J ) )
               IF( J.NE.IY ) THEN
                  A( J, J ) = SIGN( TWO, A( J, J ) )
               ELSE
                  A( J, J ) = ZERO
               END IF
  310       CONTINUE
         END IF
         CALL DLARNV( 2, ISEED, N, B )
         CALL DSCAL( N, TWO, B, 1 )
*
      ELSE IF( IMAT.EQ.17 ) THEN
*
*        Type 17:  Make the offdiagonal elements large to cause overflow
*        when adding a column of T.  In the non-transposed case, the
*        matrix is constructed to cause overflow when adding a column in
*        every other step.
*
         TSCAL = UNFL / ULP
         TSCAL = ( ONE-ULP ) / TSCAL
         DO 330 J = 1, N
            DO 320 I = 1, N
               A( I, J ) = 0.D0
  320       CONTINUE
  330    CONTINUE
         TEXP = ONE
         IF( UPPER ) THEN
            DO 340 J = N, 2, -2
               A( 1, J ) = -TSCAL / DBLE( N+1 )
               A( J, J ) = ONE
               B( J ) = TEXP*( ONE-ULP )
               A( 1, J-1 ) = -( TSCAL / DBLE( N+1 ) ) / DBLE( N+2 )
               A( J-1, J-1 ) = ONE
               B( J-1 ) = TEXP*DBLE( N*N+N-1 )
               TEXP = TEXP*2.D0
  340       CONTINUE
            B( 1 ) = ( DBLE( N+1 ) / DBLE( N+2 ) )*TSCAL
         ELSE
            DO 350 J = 1, N - 1, 2
               A( N, J ) = -TSCAL / DBLE( N+1 )
               A( J, J ) = ONE
               B( J ) = TEXP*( ONE-ULP )
               A( N, J+1 ) = -( TSCAL / DBLE( N+1 ) ) / DBLE( N+2 )
               A( J+1, J+1 ) = ONE
               B( J+1 ) = TEXP*DBLE( N*N+N-1 )
               TEXP = TEXP*2.D0
  350       CONTINUE
            B( N ) = ( DBLE( N+1 ) / DBLE( N+2 ) )*TSCAL
         END IF
*
      ELSE IF( IMAT.EQ.18 ) THEN
*
*        Type 18:  Generate a unit triangular matrix with elements
*        between -1 and 1, and make the right hand side large so that it
*        requires scaling.
*
         IF( UPPER ) THEN
            DO 360 J = 1, N
               CALL DLARNV( 2, ISEED, J-1, A( 1, J ) )
               A( J, J ) = ZERO
  360       CONTINUE
         ELSE
            DO 370 J = 1, N
               IF( J.LT.N )
     $            CALL DLARNV( 2, ISEED, N-J, A( J+1, J ) )
               A( J, J ) = ZERO
  370       CONTINUE
         END IF
*
*        Set the right hand side so that the largest value is BIGNUM.
*
         CALL DLARNV( 2, ISEED, N, B )
         IY = IDAMAX( N, B, 1 )
         BNORM = ABS( B( IY ) )
         BSCAL = BIGNUM / MAX( ONE, BNORM )
         CALL DSCAL( N, BSCAL, B, 1 )
*
      ELSE IF( IMAT.EQ.19 ) THEN
*
*        Type 19:  Generate a triangular matrix with elements between
*        BIGNUM/(n-1) and BIGNUM so that at least one of the column
*        norms will exceed BIGNUM.
*        1/3/91:  DLATRS no longer can handle this case
*
         TLEFT = BIGNUM / MAX( ONE, DBLE( N-1 ) )
         TSCAL = BIGNUM*( DBLE( N-1 ) / MAX( ONE, DBLE( N ) ) )
         IF( UPPER ) THEN
            DO 390 J = 1, N
               CALL DLARNV( 2, ISEED, J, A( 1, J ) )
               DO 380 I = 1, J
                  A( I, J ) = SIGN( TLEFT, A( I, J ) ) + TSCAL*A( I, J )
  380          CONTINUE
  390       CONTINUE
         ELSE
            DO 410 J = 1, N
               CALL DLARNV( 2, ISEED, N-J+1, A( J, J ) )
               DO 400 I = J, N
                  A( I, J ) = SIGN( TLEFT, A( I, J ) ) + TSCAL*A( I, J )
  400          CONTINUE
  410       CONTINUE
         END IF
         CALL DLARNV( 2, ISEED, N, B )
         CALL DSCAL( N, TWO, B, 1 )
      END IF
*
*     Flip the matrix if the transpose will be used.
*
      IF( .NOT.LSAME( TRANS, 'N' ) ) THEN
         IF( UPPER ) THEN
            DO 420 J = 1, N / 2
               CALL DSWAP( N-2*J+1, A( J, J ), LDA, A( J+1, N-J+1 ),
     $                     -1 )
  420       CONTINUE
         ELSE
            DO 430 J = 1, N / 2
               CALL DSWAP( N-2*J+1, A( J, J ), 1, A( N-J+1, J+1 ),
     $                     -LDA )
  430       CONTINUE
         END IF
      END IF
*
      RETURN
*
*     End of DLATTR
*
      END
