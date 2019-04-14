*> \brief \b SGQRTS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGQRTS( N, M, P, A, AF, Q, R, LDA, TAUA, B, BF, Z, T,
*                          BWK, LDB, TAUB, WORK, LWORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LWORK, M, P, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), AF( LDA, * ), R( LDA, * ),
*      $                   Q( LDA, * ), B( LDB, * ), BF( LDB, * ),
*      $                   T( LDB, * ), Z( LDB, * ), BWK( LDB, * ),
*      $                   TAUA( * ), TAUB( * ), RESULT( 4 ),
*      $                   RWORK( * ), WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGQRTS tests SGGQRF, which computes the GQR factorization of an
*> N-by-M matrix A and a N-by-P matrix B: A = Q*R and B = Q*T*Z.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of columns of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of columns of the matrix B.  P >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,M)
*>          The N-by-M matrix A.
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is REAL array, dimension (LDA,N)
*>          Details of the GQR factorization of A and B, as returned
*>          by SGGQRF, see SGGQRF for further details.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is REAL array, dimension (LDA,N)
*>          The M-by-M orthogonal matrix Q.
*> \endverbatim
*>
*> \param[out] R
*> \verbatim
*>          R is REAL array, dimension (LDA,MAX(M,N))
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays A, AF, R and Q.
*>          LDA >= max(M,N).
*> \endverbatim
*>
*> \param[out] TAUA
*> \verbatim
*>          TAUA is REAL array, dimension (min(M,N))
*>          The scalar factors of the elementary reflectors, as returned
*>          by SGGQRF.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB,P)
*>          On entry, the N-by-P matrix A.
*> \endverbatim
*>
*> \param[out] BF
*> \verbatim
*>          BF is REAL array, dimension (LDB,N)
*>          Details of the GQR factorization of A and B, as returned
*>          by SGGQRF, see SGGQRF for further details.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is REAL array, dimension (LDB,P)
*>          The P-by-P orthogonal matrix Z.
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is REAL array, dimension (LDB,max(P,N))
*> \endverbatim
*>
*> \param[out] BWK
*> \verbatim
*>          BWK is REAL array, dimension (LDB,N)
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the arrays B, BF, Z and T.
*>          LDB >= max(P,N).
*> \endverbatim
*>
*> \param[out] TAUB
*> \verbatim
*>          TAUB is REAL array, dimension (min(P,N))
*>          The scalar factors of the elementary reflectors, as returned
*>          by SGGRQF.
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
*>          The dimension of the array WORK, LWORK >= max(N,M,P)**2.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (max(N,M,P))
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (4)
*>          The test ratios:
*>            RESULT(1) = norm( R - Q'*A ) / ( MAX(M,N)*norm(A)*ULP)
*>            RESULT(2) = norm( T*Z - Q'*B ) / (MAX(P,N)*norm(B)*ULP)
*>            RESULT(3) = norm( I - Q'*Q ) / ( M*ULP )
*>            RESULT(4) = norm( I - Z'*Z ) / ( P*ULP )
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
      SUBROUTINE SGQRTS( N, M, P, A, AF, Q, R, LDA, TAUA, B, BF, Z, T,
     $                   BWK, LDB, TAUB, WORK, LWORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LWORK, M, P, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), AF( LDA, * ), R( LDA, * ),
     $                   Q( LDA, * ), B( LDB, * ), BF( LDB, * ),
     $                   T( LDB, * ), Z( LDB, * ), BWK( LDB, * ),
     $                   TAUA( * ), TAUB( * ), RESULT( 4 ),
     $                   RWORK( * ), WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      REAL               ROGUE
      PARAMETER          ( ROGUE = -1.0E+10 )
*     ..
*     .. Local Scalars ..
      INTEGER            INFO
      REAL               ANORM, BNORM, ULP, UNFL, RESID
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLANGE, SLANSY
      EXTERNAL           SLAMCH, SLANGE, SLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SLACPY, SLASET, SORGQR,
     $                   SORGRQ, SSYRK
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
      ULP = SLAMCH( 'Precision' )
      UNFL = SLAMCH( 'Safe minimum' )
*
*     Copy the matrix A to the array AF.
*
      CALL SLACPY( 'Full', N, M, A, LDA, AF, LDA )
      CALL SLACPY( 'Full', N, P, B, LDB, BF, LDB )
*
      ANORM = MAX( SLANGE( '1', N, M, A, LDA, RWORK ), UNFL )
      BNORM = MAX( SLANGE( '1', N, P, B, LDB, RWORK ), UNFL )
*
*     Factorize the matrices A and B in the arrays AF and BF.
*
      CALL SGGQRF( N, M, P, AF, LDA, TAUA, BF, LDB, TAUB, WORK,
     $             LWORK, INFO )
*
*     Generate the N-by-N matrix Q
*
      CALL SLASET( 'Full', N, N, ROGUE, ROGUE, Q, LDA )
      CALL SLACPY( 'Lower', N-1, M, AF( 2,1 ), LDA, Q( 2,1 ), LDA )
      CALL SORGQR( N, N, MIN( N, M ), Q, LDA, TAUA, WORK, LWORK, INFO )
*
*     Generate the P-by-P matrix Z
*
      CALL SLASET( 'Full', P, P, ROGUE, ROGUE, Z, LDB )
      IF( N.LE.P ) THEN
         IF( N.GT.0 .AND. N.LT.P )
     $      CALL SLACPY( 'Full', N, P-N, BF, LDB, Z( P-N+1, 1 ), LDB )
         IF( N.GT.1 )
     $      CALL SLACPY( 'Lower', N-1, N-1, BF( 2, P-N+1 ), LDB,
     $                    Z( P-N+2, P-N+1 ), LDB )
      ELSE
         IF( P.GT.1)
     $      CALL SLACPY( 'Lower', P-1, P-1, BF( N-P+2, 1 ), LDB,
     $                    Z( 2, 1 ), LDB )
      END IF
      CALL SORGRQ( P, P, MIN( N, P ), Z, LDB, TAUB, WORK, LWORK, INFO )
*
*     Copy R
*
      CALL SLASET( 'Full', N, M, ZERO, ZERO, R, LDA )
      CALL SLACPY( 'Upper', N, M, AF, LDA, R, LDA )
*
*     Copy T
*
      CALL SLASET( 'Full', N, P, ZERO, ZERO, T, LDB )
      IF( N.LE.P ) THEN
         CALL SLACPY( 'Upper', N, N, BF( 1, P-N+1 ), LDB, T( 1, P-N+1 ),
     $                LDB )
      ELSE
         CALL SLACPY( 'Full', N-P, P, BF, LDB, T, LDB )
         CALL SLACPY( 'Upper', P, P, BF( N-P+1, 1 ), LDB, T( N-P+1, 1 ),
     $                LDB )
      END IF
*
*     Compute R - Q'*A
*
      CALL SGEMM( 'Transpose', 'No transpose', N, M, N, -ONE, Q, LDA, A,
     $            LDA, ONE, R, LDA )
*
*     Compute norm( R - Q'*A ) / ( MAX(M,N)*norm(A)*ULP ) .
*
      RESID = SLANGE( '1', N, M, R, LDA, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = ( ( RESID / REAL( MAX(1,M,N) ) ) / ANORM ) / ULP
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute T*Z - Q'*B
*
      CALL SGEMM( 'No Transpose', 'No transpose', N, P, P, ONE, T, LDB,
     $            Z, LDB, ZERO, BWK, LDB )
      CALL SGEMM( 'Transpose', 'No transpose', N, P, N, -ONE, Q, LDA,
     $            B, LDB, ONE, BWK, LDB )
*
*     Compute norm( T*Z - Q'*B ) / ( MAX(P,N)*norm(A)*ULP ) .
*
      RESID = SLANGE( '1', N, P, BWK, LDB, RWORK )
      IF( BNORM.GT.ZERO ) THEN
         RESULT( 2 ) = ( ( RESID / REAL( MAX(1,P,N ) ) )/BNORM ) / ULP
      ELSE
         RESULT( 2 ) = ZERO
      END IF
*
*     Compute I - Q'*Q
*
      CALL SLASET( 'Full', N, N, ZERO, ONE, R, LDA )
      CALL SSYRK( 'Upper', 'Transpose', N, N, -ONE, Q, LDA, ONE, R,
     $            LDA )
*
*     Compute norm( I - Q'*Q ) / ( N * ULP ) .
*
      RESID = SLANSY( '1', 'Upper', N, R, LDA, RWORK )
      RESULT( 3 ) = ( RESID / REAL( MAX( 1, N ) ) ) / ULP
*
*     Compute I - Z'*Z
*
      CALL SLASET( 'Full', P, P, ZERO, ONE, T, LDB )
      CALL SSYRK( 'Upper', 'Transpose', P, P, -ONE, Z, LDB, ONE, T,
     $            LDB )
*
*     Compute norm( I - Z'*Z ) / ( P*ULP ) .
*
      RESID = SLANSY( '1', 'Upper', P, T, LDB, RWORK )
      RESULT( 4 ) = ( RESID / REAL( MAX( 1, P ) ) ) / ULP
*
      RETURN
*
*     End of SGQRTS
*
      END
