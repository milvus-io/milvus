*> \brief \b CPOT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CPOT01( UPLO, N, A, LDA, AFAC, LDAFAC, RWORK, RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            LDA, LDAFAC, N
*       REAL               RESID
*       ..
*       .. Array Arguments ..
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * ), AFAC( LDAFAC, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CPOT01 reconstructs a Hermitian positive definite matrix  A  from
*> its L*L' or U'*U factorization and computes the residual
*>    norm( L*L' - A ) / ( N * norm(A) * EPS ) or
*>    norm( U'*U - A ) / ( N * norm(A) * EPS ),
*> where EPS is the machine epsilon, L' is the conjugate transpose of L,
*> and U' is the conjugate transpose of U.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          Hermitian matrix A is stored:
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows and columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The original Hermitian matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N)
*> \endverbatim
*>
*> \param[in,out] AFAC
*> \verbatim
*>          AFAC is COMPLEX array, dimension (LDAFAC,N)
*>          On entry, the factor L or U from the L*L' or U'*U
*>          factorization of A.
*>          Overwritten with the reconstructed matrix, and then with the
*>          difference L*L' - A (or U'*U - A).
*> \endverbatim
*>
*> \param[in] LDAFAC
*> \verbatim
*>          LDAFAC is INTEGER
*>          The leading dimension of the array AFAC.  LDAFAC >= max(1,N).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is REAL
*>          If UPLO = 'L', norm(L*L' - A) / ( N * norm(A) * EPS )
*>          If UPLO = 'U', norm(U'*U - A) / ( N * norm(A) * EPS )
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CPOT01( UPLO, N, A, LDA, AFAC, LDAFAC, RWORK, RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            LDA, LDAFAC, N
      REAL               RESID
*     ..
*     .. Array Arguments ..
      REAL               RWORK( * )
      COMPLEX            A( LDA, * ), AFAC( LDAFAC, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, K
      REAL               ANORM, EPS, TR
      COMPLEX            TC
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANHE, SLAMCH
      COMPLEX            CDOTC
      EXTERNAL           LSAME, CLANHE, SLAMCH, CDOTC
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHER, CSCAL, CTRMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          AIMAG, REAL
*     ..
*     .. Executable Statements ..
*
*     Quick exit if N = 0.
*
      IF( N.LE.0 ) THEN
         RESID = ZERO
         RETURN
      END IF
*
*     Exit with RESID = 1/EPS if ANORM = 0.
*
      EPS = SLAMCH( 'Epsilon' )
      ANORM = CLANHE( '1', UPLO, N, A, LDA, RWORK )
      IF( ANORM.LE.ZERO ) THEN
         RESID = ONE / EPS
         RETURN
      END IF
*
*     Check the imaginary parts of the diagonal elements and return with
*     an error code if any are nonzero.
*
      DO 10 J = 1, N
         IF( AIMAG( AFAC( J, J ) ).NE.ZERO ) THEN
            RESID = ONE / EPS
            RETURN
         END IF
   10 CONTINUE
*
*     Compute the product U'*U, overwriting U.
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         DO 20 K = N, 1, -1
*
*           Compute the (K,K) element of the result.
*
            TR = CDOTC( K, AFAC( 1, K ), 1, AFAC( 1, K ), 1 )
            AFAC( K, K ) = TR
*
*           Compute the rest of column K.
*
            CALL CTRMV( 'Upper', 'Conjugate', 'Non-unit', K-1, AFAC,
     $                  LDAFAC, AFAC( 1, K ), 1 )
*
   20    CONTINUE
*
*     Compute the product L*L', overwriting L.
*
      ELSE
         DO 30 K = N, 1, -1
*
*           Add a multiple of column K of the factor L to each of
*           columns K+1 through N.
*
            IF( K+1.LE.N )
     $         CALL CHER( 'Lower', N-K, ONE, AFAC( K+1, K ), 1,
     $                    AFAC( K+1, K+1 ), LDAFAC )
*
*           Scale column K by the diagonal element.
*
            TC = AFAC( K, K )
            CALL CSCAL( N-K+1, TC, AFAC( K, K ), 1 )
*
   30    CONTINUE
      END IF
*
*     Compute the difference  L*L' - A (or U'*U - A).
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         DO 50 J = 1, N
            DO 40 I = 1, J - 1
               AFAC( I, J ) = AFAC( I, J ) - A( I, J )
   40       CONTINUE
            AFAC( J, J ) = AFAC( J, J ) - REAL( A( J, J ) )
   50    CONTINUE
      ELSE
         DO 70 J = 1, N
            AFAC( J, J ) = AFAC( J, J ) - REAL( A( J, J ) )
            DO 60 I = J + 1, N
               AFAC( I, J ) = AFAC( I, J ) - A( I, J )
   60       CONTINUE
   70    CONTINUE
      END IF
*
*     Compute norm( L*U - A ) / ( N * norm(A) * EPS )
*
      RESID = CLANHE( '1', UPLO, N, AFAC, LDAFAC, RWORK )
*
      RESID = ( ( RESID / REAL( N ) ) / ANORM ) / EPS
*
      RETURN
*
*     End of CPOT01
*
      END
