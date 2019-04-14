*> \brief \b ZLATM5
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLATM5( PRTYPE, M, N, A, LDA, B, LDB, C, LDC, D, LDD,
*                          E, LDE, F, LDF, R, LDR, L, LDL, ALPHA, QBLCKA,
*                          QBLCKB )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LDC, LDD, LDE, LDF, LDL, LDR, M, N,
*      $                   PRTYPE, QBLCKA, QBLCKB
*       DOUBLE PRECISION   ALPHA
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), C( LDC, * ),
*      $                   D( LDD, * ), E( LDE, * ), F( LDF, * ),
*      $                   L( LDL, * ), R( LDR, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLATM5 generates matrices involved in the Generalized Sylvester
*> equation:
*>
*>     A * R - L * B = C
*>     D * R - L * E = F
*>
*> They also satisfy (the diagonalization condition)
*>
*>  [ I -L ] ( [ A  -C ], [ D -F ] ) [ I  R ] = ( [ A    ], [ D    ] )
*>  [    I ] ( [     B ]  [    E ] ) [    I ]   ( [    B ]  [    E ] )
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PRTYPE
*> \verbatim
*>          PRTYPE is INTEGER
*>          "Points" to a certain type of the matrices to generate
*>          (see further details).
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          Specifies the order of A and D and the number of rows in
*>          C, F,  R and L.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          Specifies the order of B and E and the number of columns in
*>          C, F, R and L.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, M).
*>          On exit A M-by-M is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N).
*>          On exit B N-by-N is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is COMPLEX*16 array, dimension (LDC, N).
*>          On exit C M-by-N is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of C.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is COMPLEX*16 array, dimension (LDD, M).
*>          On exit D M-by-M is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDD
*> \verbatim
*>          LDD is INTEGER
*>          The leading dimension of D.
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is COMPLEX*16 array, dimension (LDE, N).
*>          On exit E N-by-N is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDE
*> \verbatim
*>          LDE is INTEGER
*>          The leading dimension of E.
*> \endverbatim
*>
*> \param[out] F
*> \verbatim
*>          F is COMPLEX*16 array, dimension (LDF, N).
*>          On exit F M-by-N is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDF
*> \verbatim
*>          LDF is INTEGER
*>          The leading dimension of F.
*> \endverbatim
*>
*> \param[out] R
*> \verbatim
*>          R is COMPLEX*16 array, dimension (LDR, N).
*>          On exit R M-by-N is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDR
*> \verbatim
*>          LDR is INTEGER
*>          The leading dimension of R.
*> \endverbatim
*>
*> \param[out] L
*> \verbatim
*>          L is COMPLEX*16 array, dimension (LDL, N).
*>          On exit L M-by-N is initialized according to PRTYPE.
*> \endverbatim
*>
*> \param[in] LDL
*> \verbatim
*>          LDL is INTEGER
*>          The leading dimension of L.
*> \endverbatim
*>
*> \param[in] ALPHA
*> \verbatim
*>          ALPHA is DOUBLE PRECISION
*>          Parameter used in generating PRTYPE = 1 and 5 matrices.
*> \endverbatim
*>
*> \param[in] QBLCKA
*> \verbatim
*>          QBLCKA is INTEGER
*>          When PRTYPE = 3, specifies the distance between 2-by-2
*>          blocks on the diagonal in A. Otherwise, QBLCKA is not
*>          referenced. QBLCKA > 1.
*> \endverbatim
*>
*> \param[in] QBLCKB
*> \verbatim
*>          QBLCKB is INTEGER
*>          When PRTYPE = 3, specifies the distance between 2-by-2
*>          blocks on the diagonal in B. Otherwise, QBLCKB is not
*>          referenced. QBLCKB > 1.
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
*> \date June 2016
*
*> \ingroup complex16_matgen
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  PRTYPE = 1: A and B are Jordan blocks, D and E are identity matrices
*>
*>             A : if (i == j) then A(i, j) = 1.0
*>                 if (j == i + 1) then A(i, j) = -1.0
*>                 else A(i, j) = 0.0,            i, j = 1...M
*>
*>             B : if (i == j) then B(i, j) = 1.0 - ALPHA
*>                 if (j == i + 1) then B(i, j) = 1.0
*>                 else B(i, j) = 0.0,            i, j = 1...N
*>
*>             D : if (i == j) then D(i, j) = 1.0
*>                 else D(i, j) = 0.0,            i, j = 1...M
*>
*>             E : if (i == j) then E(i, j) = 1.0
*>                 else E(i, j) = 0.0,            i, j = 1...N
*>
*>             L =  R are chosen from [-10...10],
*>                  which specifies the right hand sides (C, F).
*>
*>  PRTYPE = 2 or 3: Triangular and/or quasi- triangular.
*>
*>             A : if (i <= j) then A(i, j) = [-1...1]
*>                 else A(i, j) = 0.0,             i, j = 1...M
*>
*>                 if (PRTYPE = 3) then
*>                    A(k + 1, k + 1) = A(k, k)
*>                    A(k + 1, k) = [-1...1]
*>                    sign(A(k, k + 1) = -(sin(A(k + 1, k))
*>                        k = 1, M - 1, QBLCKA
*>
*>             B : if (i <= j) then B(i, j) = [-1...1]
*>                 else B(i, j) = 0.0,            i, j = 1...N
*>
*>                 if (PRTYPE = 3) then
*>                    B(k + 1, k + 1) = B(k, k)
*>                    B(k + 1, k) = [-1...1]
*>                    sign(B(k, k + 1) = -(sign(B(k + 1, k))
*>                        k = 1, N - 1, QBLCKB
*>
*>             D : if (i <= j) then D(i, j) = [-1...1].
*>                 else D(i, j) = 0.0,            i, j = 1...M
*>
*>
*>             E : if (i <= j) then D(i, j) = [-1...1]
*>                 else E(i, j) = 0.0,            i, j = 1...N
*>
*>                 L, R are chosen from [-10...10],
*>                 which specifies the right hand sides (C, F).
*>
*>  PRTYPE = 4 Full
*>             A(i, j) = [-10...10]
*>             D(i, j) = [-1...1]    i,j = 1...M
*>             B(i, j) = [-10...10]
*>             E(i, j) = [-1...1]    i,j = 1...N
*>             R(i, j) = [-10...10]
*>             L(i, j) = [-1...1]    i = 1..M ,j = 1...N
*>
*>             L, R specifies the right hand sides (C, F).
*>
*>  PRTYPE = 5 special case common and/or close eigs.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZLATM5( PRTYPE, M, N, A, LDA, B, LDB, C, LDC, D, LDD,
     $                   E, LDE, F, LDF, R, LDR, L, LDL, ALPHA, QBLCKA,
     $                   QBLCKB )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LDC, LDD, LDE, LDF, LDL, LDR, M, N,
     $                   PRTYPE, QBLCKA, QBLCKB
      DOUBLE PRECISION   ALPHA
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), C( LDC, * ),
     $                   D( LDD, * ), E( LDE, * ), F( LDF, * ),
     $                   L( LDL, * ), R( LDR, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         ONE, TWO, ZERO, HALF, TWENTY
      PARAMETER          ( ONE = ( 1.0D+0, 0.0D+0 ),
     $                   TWO = ( 2.0D+0, 0.0D+0 ),
     $                   ZERO = ( 0.0D+0, 0.0D+0 ),
     $                   HALF = ( 0.5D+0, 0.0D+0 ),
     $                   TWENTY = ( 2.0D+1, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, K
      COMPLEX*16         IMEPS, REEPS
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX, MOD, SIN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM
*     ..
*     .. Executable Statements ..
*
      IF( PRTYPE.EQ.1 ) THEN
         DO 20 I = 1, M
            DO 10 J = 1, M
               IF( I.EQ.J ) THEN
                  A( I, J ) = ONE
                  D( I, J ) = ONE
               ELSE IF( I.EQ.J-1 ) THEN
                  A( I, J ) = -ONE
                  D( I, J ) = ZERO
               ELSE
                  A( I, J ) = ZERO
                  D( I, J ) = ZERO
               END IF
   10       CONTINUE
   20    CONTINUE
*
         DO 40 I = 1, N
            DO 30 J = 1, N
               IF( I.EQ.J ) THEN
                  B( I, J ) = ONE - ALPHA
                  E( I, J ) = ONE
               ELSE IF( I.EQ.J-1 ) THEN
                  B( I, J ) = ONE
                  E( I, J ) = ZERO
               ELSE
                  B( I, J ) = ZERO
                  E( I, J ) = ZERO
               END IF
   30       CONTINUE
   40    CONTINUE
*
         DO 60 I = 1, M
            DO 50 J = 1, N
               R( I, J ) = ( HALF-SIN( DCMPLX( I / J ) ) )*TWENTY
               L( I, J ) = R( I, J )
   50       CONTINUE
   60    CONTINUE
*
      ELSE IF( PRTYPE.EQ.2 .OR. PRTYPE.EQ.3 ) THEN
         DO 80 I = 1, M
            DO 70 J = 1, M
               IF( I.LE.J ) THEN
                  A( I, J ) = ( HALF-SIN( DCMPLX( I ) ) )*TWO
                  D( I, J ) = ( HALF-SIN( DCMPLX( I*J ) ) )*TWO
               ELSE
                  A( I, J ) = ZERO
                  D( I, J ) = ZERO
               END IF
   70       CONTINUE
   80    CONTINUE
*
         DO 100 I = 1, N
            DO 90 J = 1, N
               IF( I.LE.J ) THEN
                  B( I, J ) = ( HALF-SIN( DCMPLX( I+J ) ) )*TWO
                  E( I, J ) = ( HALF-SIN( DCMPLX( J ) ) )*TWO
               ELSE
                  B( I, J ) = ZERO
                  E( I, J ) = ZERO
               END IF
   90       CONTINUE
  100    CONTINUE
*
         DO 120 I = 1, M
            DO 110 J = 1, N
               R( I, J ) = ( HALF-SIN( DCMPLX( I*J ) ) )*TWENTY
               L( I, J ) = ( HALF-SIN( DCMPLX( I+J ) ) )*TWENTY
  110       CONTINUE
  120    CONTINUE
*
         IF( PRTYPE.EQ.3 ) THEN
            IF( QBLCKA.LE.1 )
     $         QBLCKA = 2
            DO 130 K = 1, M - 1, QBLCKA
               A( K+1, K+1 ) = A( K, K )
               A( K+1, K ) = -SIN( A( K, K+1 ) )
  130       CONTINUE
*
            IF( QBLCKB.LE.1 )
     $         QBLCKB = 2
            DO 140 K = 1, N - 1, QBLCKB
               B( K+1, K+1 ) = B( K, K )
               B( K+1, K ) = -SIN( B( K, K+1 ) )
  140       CONTINUE
         END IF
*
      ELSE IF( PRTYPE.EQ.4 ) THEN
         DO 160 I = 1, M
            DO 150 J = 1, M
               A( I, J ) = ( HALF-SIN( DCMPLX( I*J ) ) )*TWENTY
               D( I, J ) = ( HALF-SIN( DCMPLX( I+J ) ) )*TWO
  150       CONTINUE
  160    CONTINUE
*
         DO 180 I = 1, N
            DO 170 J = 1, N
               B( I, J ) = ( HALF-SIN( DCMPLX( I+J ) ) )*TWENTY
               E( I, J ) = ( HALF-SIN( DCMPLX( I*J ) ) )*TWO
  170       CONTINUE
  180    CONTINUE
*
         DO 200 I = 1, M
            DO 190 J = 1, N
               R( I, J ) = ( HALF-SIN( DCMPLX( J / I ) ) )*TWENTY
               L( I, J ) = ( HALF-SIN( DCMPLX( I*J ) ) )*TWO
  190       CONTINUE
  200    CONTINUE
*
      ELSE IF( PRTYPE.GE.5 ) THEN
         REEPS = HALF*TWO*TWENTY / ALPHA
         IMEPS = ( HALF-TWO ) / ALPHA
         DO 220 I = 1, M
            DO 210 J = 1, N
               R( I, J ) = ( HALF-SIN( DCMPLX( I*J ) ) )*ALPHA / TWENTY
               L( I, J ) = ( HALF-SIN( DCMPLX( I+J ) ) )*ALPHA / TWENTY
  210       CONTINUE
  220    CONTINUE
*
         DO 230 I = 1, M
            D( I, I ) = ONE
  230    CONTINUE
*
         DO 240 I = 1, M
            IF( I.LE.4 ) THEN
               A( I, I ) = ONE
               IF( I.GT.2 )
     $            A( I, I ) = ONE + REEPS
               IF( MOD( I, 2 ).NE.0 .AND. I.LT.M ) THEN
                  A( I, I+1 ) = IMEPS
               ELSE IF( I.GT.1 ) THEN
                  A( I, I-1 ) = -IMEPS
               END IF
            ELSE IF( I.LE.8 ) THEN
               IF( I.LE.6 ) THEN
                  A( I, I ) = REEPS
               ELSE
                  A( I, I ) = -REEPS
               END IF
               IF( MOD( I, 2 ).NE.0 .AND. I.LT.M ) THEN
                  A( I, I+1 ) = ONE
               ELSE IF( I.GT.1 ) THEN
                  A( I, I-1 ) = -ONE
               END IF
            ELSE
               A( I, I ) = ONE
               IF( MOD( I, 2 ).NE.0 .AND. I.LT.M ) THEN
                  A( I, I+1 ) = IMEPS*2
               ELSE IF( I.GT.1 ) THEN
                  A( I, I-1 ) = -IMEPS*2
               END IF
            END IF
  240    CONTINUE
*
         DO 250 I = 1, N
            E( I, I ) = ONE
            IF( I.LE.4 ) THEN
               B( I, I ) = -ONE
               IF( I.GT.2 )
     $            B( I, I ) = ONE - REEPS
               IF( MOD( I, 2 ).NE.0 .AND. I.LT.N ) THEN
                  B( I, I+1 ) = IMEPS
               ELSE IF( I.GT.1 ) THEN
                  B( I, I-1 ) = -IMEPS
               END IF
            ELSE IF( I.LE.8 ) THEN
               IF( I.LE.6 ) THEN
                  B( I, I ) = REEPS
               ELSE
                  B( I, I ) = -REEPS
               END IF
               IF( MOD( I, 2 ).NE.0 .AND. I.LT.N ) THEN
                  B( I, I+1 ) = ONE + IMEPS
               ELSE IF( I.GT.1 ) THEN
                  B( I, I-1 ) = -ONE - IMEPS
               END IF
            ELSE
               B( I, I ) = ONE - REEPS
               IF( MOD( I, 2 ).NE.0 .AND. I.LT.N ) THEN
                  B( I, I+1 ) = IMEPS*2
               ELSE IF( I.GT.1 ) THEN
                  B( I, I-1 ) = -IMEPS*2
               END IF
            END IF
  250    CONTINUE
      END IF
*
*     Compute rhs (C, F)
*
      CALL ZGEMM( 'N', 'N', M, N, M, ONE, A, LDA, R, LDR, ZERO, C, LDC )
      CALL ZGEMM( 'N', 'N', M, N, N, -ONE, L, LDL, B, LDB, ONE, C, LDC )
      CALL ZGEMM( 'N', 'N', M, N, M, ONE, D, LDD, R, LDR, ZERO, F, LDF )
      CALL ZGEMM( 'N', 'N', M, N, N, -ONE, L, LDL, E, LDE, ONE, F, LDF )
*
*     End of ZLATM5
*
      END
