*> \brief \b SGSVTS3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGSVTS3( M, P, N, A, AF, LDA, B, BF, LDB, U, LDU, V,
*                           LDV, Q, LDQ, ALPHA, BETA, R, LDR, IWORK, WORK,
*                           LWORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LDQ, LDR, LDU, LDV, LWORK, M, N, P
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       REAL               A( LDA, * ), AF( LDA, * ), ALPHA( * ),
*      $                   B( LDB, * ), BETA( * ), BF( LDB, * ),
*      $                   Q( LDQ, * ), R( LDR, * ), RESULT( 6 ),
*      $                   RWORK( * ), U( LDU, * ), V( LDV, * ),
*      $                   WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGSVTS3 tests SGGSVD3, which computes the GSVD of an M-by-N matrix A
*> and a P-by-N matrix B:
*>              U'*A*Q = D1*R and V'*B*Q = D2*R.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of rows of the matrix B.  P >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,M)
*>          The M-by-N matrix A.
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is REAL array, dimension (LDA,N)
*>          Details of the GSVD of A and B, as returned by SGGSVD3,
*>          see SGGSVD3 for further details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays A and AF.
*>          LDA >= max( 1,M ).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB,P)
*>          On entry, the P-by-N matrix B.
*> \endverbatim
*>
*> \param[out] BF
*> \verbatim
*>          BF is REAL array, dimension (LDB,N)
*>          Details of the GSVD of A and B, as returned by SGGSVD3,
*>          see SGGSVD3 for further details.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the arrays B and BF.
*>          LDB >= max(1,P).
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is REAL array, dimension(LDU,M)
*>          The M by M orthogonal matrix U.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U. LDU >= max(1,M).
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is REAL array, dimension(LDV,M)
*>          The P by P orthogonal matrix V.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V. LDV >= max(1,P).
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is REAL array, dimension(LDQ,N)
*>          The N by N orthogonal matrix Q.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q. LDQ >= max(1,N).
*> \endverbatim
*>
*> \param[out] ALPHA
*> \verbatim
*>          ALPHA is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (N)
*>
*>          The generalized singular value pairs of A and B, the
*>          ``diagonal'' matrices D1 and D2 are constructed from
*>          ALPHA and BETA, see subroutine SGGSVD3 for details.
*> \endverbatim
*>
*> \param[out] R
*> \verbatim
*>          R is REAL array, dimension(LDQ,N)
*>          The upper triangular matrix R.
*> \endverbatim
*>
*> \param[in] LDR
*> \verbatim
*>          LDR is INTEGER
*>          The leading dimension of the array R. LDR >= max(1,N).
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK,
*>          LWORK >= max(M,P,N)*max(M,P,N).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (max(M,P,N))
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (6)
*>          The test ratios:
*>          RESULT(1) = norm( U'*A*Q - D1*R ) / ( MAX(M,N)*norm(A)*ULP)
*>          RESULT(2) = norm( V'*B*Q - D2*R ) / ( MAX(P,N)*norm(B)*ULP)
*>          RESULT(3) = norm( I - U'*U ) / ( M*ULP )
*>          RESULT(4) = norm( I - V'*V ) / ( P*ULP )
*>          RESULT(5) = norm( I - Q'*Q ) / ( N*ULP )
*>          RESULT(6) = 0        if ALPHA is in decreasing order;
*>                    = ULPINV   otherwise.
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
*> \date August 2015
*
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SGSVTS3( M, P, N, A, AF, LDA, B, BF, LDB, U, LDU, V,
     $                    LDV, Q, LDQ, ALPHA, BETA, R, LDR, IWORK, WORK,
     $                    LWORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     August 2015
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LDQ, LDR, LDU, LDV, LWORK, M, N, P
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               A( LDA, * ), AF( LDA, * ), ALPHA( * ),
     $                   B( LDB, * ), BETA( * ), BF( LDB, * ),
     $                   Q( LDQ, * ), R( LDR, * ), RESULT( 6 ),
     $                   RWORK( * ), U( LDU, * ), V( LDV, * ),
     $                   WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J, K, L
      REAL               ANORM, BNORM, RESID, TEMP, ULP, ULPINV, UNFL
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLANGE, SLANSY
      EXTERNAL           SLAMCH, SLANGE, SLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEMM, SGGSVD3, SLACPY, SLASET, SSYRK
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
      ULP = SLAMCH( 'Precision' )
      ULPINV = ONE / ULP
      UNFL = SLAMCH( 'Safe minimum' )
*
*     Copy the matrix A to the array AF.
*
      CALL SLACPY( 'Full', M, N, A, LDA, AF, LDA )
      CALL SLACPY( 'Full', P, N, B, LDB, BF, LDB )
*
      ANORM = MAX( SLANGE( '1', M, N, A, LDA, RWORK ), UNFL )
      BNORM = MAX( SLANGE( '1', P, N, B, LDB, RWORK ), UNFL )
*
*     Factorize the matrices A and B in the arrays AF and BF.
*
      CALL SGGSVD3( 'U', 'V', 'Q', M, N, P, K, L, AF, LDA, BF, LDB,
     $              ALPHA, BETA, U, LDU, V, LDV, Q, LDQ, WORK, LWORK,
     $              IWORK, INFO )
*
*     Copy R
*
      DO 20 I = 1, MIN( K+L, M )
         DO 10 J = I, K + L
            R( I, J ) = AF( I, N-K-L+J )
   10    CONTINUE
   20 CONTINUE
*
      IF( M-K-L.LT.0 ) THEN
         DO 40 I = M + 1, K + L
            DO 30 J = I, K + L
               R( I, J ) = BF( I-K, N-K-L+J )
   30       CONTINUE
   40    CONTINUE
      END IF
*
*     Compute A:= U'*A*Q - D1*R
*
      CALL SGEMM( 'No transpose', 'No transpose', M, N, N, ONE, A, LDA,
     $            Q, LDQ, ZERO, WORK, LDA )
*
      CALL SGEMM( 'Transpose', 'No transpose', M, N, M, ONE, U, LDU,
     $            WORK, LDA, ZERO, A, LDA )
*
      DO 60 I = 1, K
         DO 50 J = I, K + L
            A( I, N-K-L+J ) = A( I, N-K-L+J ) - R( I, J )
   50    CONTINUE
   60 CONTINUE
*
      DO 80 I = K + 1, MIN( K+L, M )
         DO 70 J = I, K + L
            A( I, N-K-L+J ) = A( I, N-K-L+J ) - ALPHA( I )*R( I, J )
   70    CONTINUE
   80 CONTINUE
*
*     Compute norm( U'*A*Q - D1*R ) / ( MAX(1,M,N)*norm(A)*ULP ) .
*
      RESID = SLANGE( '1', M, N, A, LDA, RWORK )
*
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = ( ( RESID / REAL( MAX( 1, M, N ) ) ) / ANORM ) /
     $                 ULP
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute B := V'*B*Q - D2*R
*
      CALL SGEMM( 'No transpose', 'No transpose', P, N, N, ONE, B, LDB,
     $            Q, LDQ, ZERO, WORK, LDB )
*
      CALL SGEMM( 'Transpose', 'No transpose', P, N, P, ONE, V, LDV,
     $            WORK, LDB, ZERO, B, LDB )
*
      DO 100 I = 1, L
         DO 90 J = I, L
            B( I, N-L+J ) = B( I, N-L+J ) - BETA( K+I )*R( K+I, K+J )
   90    CONTINUE
  100 CONTINUE
*
*     Compute norm( V'*B*Q - D2*R ) / ( MAX(P,N)*norm(B)*ULP ) .
*
      RESID = SLANGE( '1', P, N, B, LDB, RWORK )
      IF( BNORM.GT.ZERO ) THEN
         RESULT( 2 ) = ( ( RESID / REAL( MAX( 1, P, N ) ) ) / BNORM ) /
     $                 ULP
      ELSE
         RESULT( 2 ) = ZERO
      END IF
*
*     Compute I - U'*U
*
      CALL SLASET( 'Full', M, M, ZERO, ONE, WORK, LDQ )
      CALL SSYRK( 'Upper', 'Transpose', M, M, -ONE, U, LDU, ONE, WORK,
     $            LDU )
*
*     Compute norm( I - U'*U ) / ( M * ULP ) .
*
      RESID = SLANSY( '1', 'Upper', M, WORK, LDU, RWORK )
      RESULT( 3 ) = ( RESID / REAL( MAX( 1, M ) ) ) / ULP
*
*     Compute I - V'*V
*
      CALL SLASET( 'Full', P, P, ZERO, ONE, WORK, LDV )
      CALL SSYRK( 'Upper', 'Transpose', P, P, -ONE, V, LDV, ONE, WORK,
     $            LDV )
*
*     Compute norm( I - V'*V ) / ( P * ULP ) .
*
      RESID = SLANSY( '1', 'Upper', P, WORK, LDV, RWORK )
      RESULT( 4 ) = ( RESID / REAL( MAX( 1, P ) ) ) / ULP
*
*     Compute I - Q'*Q
*
      CALL SLASET( 'Full', N, N, ZERO, ONE, WORK, LDQ )
      CALL SSYRK( 'Upper', 'Transpose', N, N, -ONE, Q, LDQ, ONE, WORK,
     $            LDQ )
*
*     Compute norm( I - Q'*Q ) / ( N * ULP ) .
*
      RESID = SLANSY( '1', 'Upper', N, WORK, LDQ, RWORK )
      RESULT( 5 ) = ( RESID / REAL( MAX( 1, N ) ) ) / ULP
*
*     Check sorting
*
      CALL SCOPY( N, ALPHA, 1, WORK, 1 )
      DO 110 I = K + 1, MIN( K+L, M )
         J = IWORK( I )
         IF( I.NE.J ) THEN
            TEMP = WORK( I )
            WORK( I ) = WORK( J )
            WORK( J ) = TEMP
         END IF
  110 CONTINUE
*
      RESULT( 6 ) = ZERO
      DO 120 I = K + 1, MIN( K+L, M ) - 1
         IF( WORK( I ).LT.WORK( I+1 ) )
     $      RESULT( 6 ) = ULPINV
  120 CONTINUE
*
      RETURN
*
*     End of SGSVTS3
*
      END
