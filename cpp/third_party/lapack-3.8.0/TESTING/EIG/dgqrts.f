*> \brief \b DGQRTS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGQRTS( N, M, P, A, AF, Q, R, LDA, TAUA, B, BF, Z, T,
*                          BWK, LDB, TAUB, WORK, LWORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LWORK, M, N, P
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), AF( LDA, * ), B( LDB, * ),
*      $                   BF( LDB, * ), BWK( LDB, * ), Q( LDA, * ),
*      $                   R( LDA, * ), RESULT( 4 ), RWORK( * ),
*      $                   T( LDB, * ), TAUA( * ), TAUB( * ),
*      $                   WORK( LWORK ), Z( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGQRTS tests DGGQRF, which computes the GQR factorization of an
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
*>          A is DOUBLE PRECISION array, dimension (LDA,M)
*>          The N-by-M matrix A.
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is DOUBLE PRECISION array, dimension (LDA,N)
*>          Details of the GQR factorization of A and B, as returned
*>          by DGGQRF, see SGGQRF for further details.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDA,N)
*>          The M-by-M orthogonal matrix Q.
*> \endverbatim
*>
*> \param[out] R
*> \verbatim
*>          R is DOUBLE PRECISION array, dimension (LDA,MAX(M,N))
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
*>          TAUA is DOUBLE PRECISION array, dimension (min(M,N))
*>          The scalar factors of the elementary reflectors, as returned
*>          by DGGQRF.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,P)
*>          On entry, the N-by-P matrix A.
*> \endverbatim
*>
*> \param[out] BF
*> \verbatim
*>          BF is DOUBLE PRECISION array, dimension (LDB,N)
*>          Details of the GQR factorization of A and B, as returned
*>          by DGGQRF, see SGGQRF for further details.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDB,P)
*>          The P-by-P orthogonal matrix Z.
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is DOUBLE PRECISION array, dimension (LDB,max(P,N))
*> \endverbatim
*>
*> \param[out] BWK
*> \verbatim
*>          BWK is DOUBLE PRECISION array, dimension (LDB,N)
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
*>          TAUB is DOUBLE PRECISION array, dimension (min(P,N))
*>          The scalar factors of the elementary reflectors, as returned
*>          by DGGRQF.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
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
*>          RWORK is DOUBLE PRECISION array, dimension (max(N,M,P))
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (4)
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DGQRTS( N, M, P, A, AF, Q, R, LDA, TAUA, B, BF, Z, T,
     $                   BWK, LDB, TAUB, WORK, LWORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LWORK, M, N, P
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), AF( LDA, * ), B( LDB, * ),
     $                   BF( LDB, * ), BWK( LDB, * ), Q( LDA, * ),
     $                   R( LDA, * ), RESULT( 4 ), RWORK( * ),
     $                   T( LDB, * ), TAUA( * ), TAUB( * ),
     $                   WORK( LWORK ), Z( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      DOUBLE PRECISION   ROGUE
      PARAMETER          ( ROGUE = -1.0D+10 )
*     ..
*     .. Local Scalars ..
      INTEGER            INFO
      DOUBLE PRECISION   ANORM, BNORM, RESID, ULP, UNFL
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE, DLANSY
      EXTERNAL           DLAMCH, DLANGE, DLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DGGQRF, DLACPY, DLASET, DORGQR, DORGRQ,
     $                   DSYRK
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      ULP = DLAMCH( 'Precision' )
      UNFL = DLAMCH( 'Safe minimum' )
*
*     Copy the matrix A to the array AF.
*
      CALL DLACPY( 'Full', N, M, A, LDA, AF, LDA )
      CALL DLACPY( 'Full', N, P, B, LDB, BF, LDB )
*
      ANORM = MAX( DLANGE( '1', N, M, A, LDA, RWORK ), UNFL )
      BNORM = MAX( DLANGE( '1', N, P, B, LDB, RWORK ), UNFL )
*
*     Factorize the matrices A and B in the arrays AF and BF.
*
      CALL DGGQRF( N, M, P, AF, LDA, TAUA, BF, LDB, TAUB, WORK, LWORK,
     $             INFO )
*
*     Generate the N-by-N matrix Q
*
      CALL DLASET( 'Full', N, N, ROGUE, ROGUE, Q, LDA )
      CALL DLACPY( 'Lower', N-1, M, AF( 2, 1 ), LDA, Q( 2, 1 ), LDA )
      CALL DORGQR( N, N, MIN( N, M ), Q, LDA, TAUA, WORK, LWORK, INFO )
*
*     Generate the P-by-P matrix Z
*
      CALL DLASET( 'Full', P, P, ROGUE, ROGUE, Z, LDB )
      IF( N.LE.P ) THEN
         IF( N.GT.0 .AND. N.LT.P )
     $      CALL DLACPY( 'Full', N, P-N, BF, LDB, Z( P-N+1, 1 ), LDB )
         IF( N.GT.1 )
     $      CALL DLACPY( 'Lower', N-1, N-1, BF( 2, P-N+1 ), LDB,
     $                   Z( P-N+2, P-N+1 ), LDB )
      ELSE
         IF( P.GT.1 )
     $      CALL DLACPY( 'Lower', P-1, P-1, BF( N-P+2, 1 ), LDB,
     $                   Z( 2, 1 ), LDB )
      END IF
      CALL DORGRQ( P, P, MIN( N, P ), Z, LDB, TAUB, WORK, LWORK, INFO )
*
*     Copy R
*
      CALL DLASET( 'Full', N, M, ZERO, ZERO, R, LDA )
      CALL DLACPY( 'Upper', N, M, AF, LDA, R, LDA )
*
*     Copy T
*
      CALL DLASET( 'Full', N, P, ZERO, ZERO, T, LDB )
      IF( N.LE.P ) THEN
         CALL DLACPY( 'Upper', N, N, BF( 1, P-N+1 ), LDB, T( 1, P-N+1 ),
     $                LDB )
      ELSE
         CALL DLACPY( 'Full', N-P, P, BF, LDB, T, LDB )
         CALL DLACPY( 'Upper', P, P, BF( N-P+1, 1 ), LDB, T( N-P+1, 1 ),
     $                LDB )
      END IF
*
*     Compute R - Q'*A
*
      CALL DGEMM( 'Transpose', 'No transpose', N, M, N, -ONE, Q, LDA, A,
     $            LDA, ONE, R, LDA )
*
*     Compute norm( R - Q'*A ) / ( MAX(M,N)*norm(A)*ULP ) .
*
      RESID = DLANGE( '1', N, M, R, LDA, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = ( ( RESID / DBLE( MAX( 1, M, N ) ) ) / ANORM ) /
     $                 ULP
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute T*Z - Q'*B
*
      CALL DGEMM( 'No Transpose', 'No transpose', N, P, P, ONE, T, LDB,
     $            Z, LDB, ZERO, BWK, LDB )
      CALL DGEMM( 'Transpose', 'No transpose', N, P, N, -ONE, Q, LDA, B,
     $            LDB, ONE, BWK, LDB )
*
*     Compute norm( T*Z - Q'*B ) / ( MAX(P,N)*norm(A)*ULP ) .
*
      RESID = DLANGE( '1', N, P, BWK, LDB, RWORK )
      IF( BNORM.GT.ZERO ) THEN
         RESULT( 2 ) = ( ( RESID / DBLE( MAX( 1, P, N ) ) ) / BNORM ) /
     $                 ULP
      ELSE
         RESULT( 2 ) = ZERO
      END IF
*
*     Compute I - Q'*Q
*
      CALL DLASET( 'Full', N, N, ZERO, ONE, R, LDA )
      CALL DSYRK( 'Upper', 'Transpose', N, N, -ONE, Q, LDA, ONE, R,
     $            LDA )
*
*     Compute norm( I - Q'*Q ) / ( N * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', N, R, LDA, RWORK )
      RESULT( 3 ) = ( RESID / DBLE( MAX( 1, N ) ) ) / ULP
*
*     Compute I - Z'*Z
*
      CALL DLASET( 'Full', P, P, ZERO, ONE, T, LDB )
      CALL DSYRK( 'Upper', 'Transpose', P, P, -ONE, Z, LDB, ONE, T,
     $            LDB )
*
*     Compute norm( I - Z'*Z ) / ( P*ULP ) .
*
      RESID = DLANSY( '1', 'Upper', P, T, LDB, RWORK )
      RESULT( 4 ) = ( RESID / DBLE( MAX( 1, P ) ) ) / ULP
*
      RETURN
*
*     End of DGQRTS
*
      END
