*> \brief \b ZQLT02
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZQLT02( M, N, K, A, AF, Q, L, LDA, TAU, WORK, LWORK,
*                          RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            K, LDA, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RESULT( * ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), AF( LDA, * ), L( LDA, * ),
*      $                   Q( LDA, * ), TAU( * ), WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZQLT02 tests ZUNGQL, which generates an m-by-n matrix Q with
*> orthonornmal columns that is defined as the product of k elementary
*> reflectors.
*>
*> Given the QL factorization of an m-by-n matrix A, ZQLT02 generates
*> the orthogonal matrix Q defined by the factorization of the last k
*> columns of A; it compares L(m-n+1:m,n-k+1:n) with
*> Q(1:m,m-n+1:m)'*A(1:m,n-k+1:n), and checks that the columns of Q are
*> orthonormal.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix Q to be generated.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix Q to be generated.
*>          M >= N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of elementary reflectors whose product defines the
*>          matrix Q. N >= K >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The m-by-n matrix A which was factorized by ZQLT01.
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is COMPLEX*16 array, dimension (LDA,N)
*>          Details of the QL factorization of A, as returned by ZGEQLF.
*>          See ZGEQLF for further details.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDA,N)
*> \endverbatim
*>
*> \param[out] L
*> \verbatim
*>          L is COMPLEX*16 array, dimension (LDA,N)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays A, AF, Q and L. LDA >= M.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array, dimension (N)
*>          The scalar factors of the elementary reflectors corresponding
*>          to the QL factorization in AF.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (M)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (2)
*>          The test ratios:
*>          RESULT(1) = norm( L - Q'*A ) / ( M * norm(A) * EPS )
*>          RESULT(2) = norm( I - Q'*Q ) / ( M * EPS )
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZQLT02( M, N, K, A, AF, Q, L, LDA, TAU, WORK, LWORK,
     $                   RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            K, LDA, LWORK, M, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RESULT( * ), RWORK( * )
      COMPLEX*16         A( LDA, * ), AF( LDA, * ), L( LDA, * ),
     $                   Q( LDA, * ), TAU( * ), WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         ROGUE
      PARAMETER          ( ROGUE = ( -1.0D+10, -1.0D+10 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            INFO
      DOUBLE PRECISION   ANORM, EPS, RESID
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE, ZLANSY
      EXTERNAL           DLAMCH, ZLANGE, ZLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZHERK, ZLACPY, ZLASET, ZUNGQL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCMPLX, MAX
*     ..
*     .. Scalars in Common ..
      CHARACTER*32       SRNAMT
*     ..
*     .. Common blocks ..
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 .OR. K.EQ.0 ) THEN
         RESULT( 1 ) = ZERO
         RESULT( 2 ) = ZERO
         RETURN
      END IF
*
      EPS = DLAMCH( 'Epsilon' )
*
*     Copy the last k columns of the factorization to the array Q
*
      CALL ZLASET( 'Full', M, N, ROGUE, ROGUE, Q, LDA )
      IF( K.LT.M )
     $   CALL ZLACPY( 'Full', M-K, K, AF( 1, N-K+1 ), LDA,
     $                Q( 1, N-K+1 ), LDA )
      IF( K.GT.1 )
     $   CALL ZLACPY( 'Upper', K-1, K-1, AF( M-K+1, N-K+2 ), LDA,
     $                Q( M-K+1, N-K+2 ), LDA )
*
*     Generate the last n columns of the matrix Q
*
      SRNAMT = 'ZUNGQL'
      CALL ZUNGQL( M, N, K, Q, LDA, TAU( N-K+1 ), WORK, LWORK, INFO )
*
*     Copy L(m-n+1:m,n-k+1:n)
*
      CALL ZLASET( 'Full', N, K, DCMPLX( ZERO ), DCMPLX( ZERO ),
     $             L( M-N+1, N-K+1 ), LDA )
      CALL ZLACPY( 'Lower', K, K, AF( M-K+1, N-K+1 ), LDA,
     $             L( M-K+1, N-K+1 ), LDA )
*
*     Compute L(m-n+1:m,n-k+1:n) - Q(1:m,m-n+1:m)' * A(1:m,n-k+1:n)
*
      CALL ZGEMM( 'Conjugate transpose', 'No transpose', N, K, M,
     $            DCMPLX( -ONE ), Q, LDA, A( 1, N-K+1 ), LDA,
     $            DCMPLX( ONE ), L( M-N+1, N-K+1 ), LDA )
*
*     Compute norm( L - Q'*A ) / ( M * norm(A) * EPS ) .
*
      ANORM = ZLANGE( '1', M, K, A( 1, N-K+1 ), LDA, RWORK )
      RESID = ZLANGE( '1', N, K, L( M-N+1, N-K+1 ), LDA, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = ( ( RESID / DBLE( MAX( 1, M ) ) ) / ANORM ) / EPS
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute I - Q'*Q
*
      CALL ZLASET( 'Full', N, N, DCMPLX( ZERO ), DCMPLX( ONE ), L, LDA )
      CALL ZHERK( 'Upper', 'Conjugate transpose', N, M, -ONE, Q, LDA,
     $            ONE, L, LDA )
*
*     Compute norm( I - Q'*Q ) / ( M * EPS ) .
*
      RESID = ZLANSY( '1', 'Upper', N, L, LDA, RWORK )
*
      RESULT( 2 ) = ( RESID / DBLE( MAX( 1, M ) ) ) / EPS
*
      RETURN
*
*     End of ZQLT02
*
      END
