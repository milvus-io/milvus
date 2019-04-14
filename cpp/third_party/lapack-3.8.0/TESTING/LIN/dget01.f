*> \brief \b DGET01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET01( M, N, A, LDA, AFAC, LDAFAC, IPIV, RWORK,
*                          RESID )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDAFAC, M, N
*       DOUBLE PRECISION   RESID
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       DOUBLE PRECISION   A( LDA, * ), AFAC( LDAFAC, * ), RWORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGET01 reconstructs a matrix A from its L*U factorization and
*> computes the residual
*>    norm(L*U - A) / ( N * norm(A) * EPS ),
*> where EPS is the machine epsilon.
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
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The original M x N matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] AFAC
*> \verbatim
*>          AFAC is DOUBLE PRECISION array, dimension (LDAFAC,N)
*>          The factored form of the matrix A.  AFAC contains the factors
*>          L and U from the L*U factorization as computed by DGETRF.
*>          Overwritten with the reconstructed matrix, and then with the
*>          difference L*U - A.
*> \endverbatim
*>
*> \param[in] LDAFAC
*> \verbatim
*>          LDAFAC is INTEGER
*>          The leading dimension of the array AFAC.  LDAFAC >= max(1,M).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          The pivot indices from DGETRF.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (M)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
*>          norm(L*U - A) / ( N * norm(A) * EPS )
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
      SUBROUTINE DGET01( M, N, A, LDA, AFAC, LDAFAC, IPIV, RWORK,
     $                   RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDAFAC, M, N
      DOUBLE PRECISION   RESID
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      DOUBLE PRECISION   A( LDA, * ), AFAC( LDAFAC, * ), RWORK( * )
*     ..
*
*  =====================================================================
*
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, K
      DOUBLE PRECISION   ANORM, EPS, T
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DDOT, DLAMCH, DLANGE
      EXTERNAL           DDOT, DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMV, DLASWP, DSCAL, DTRMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick exit if M = 0 or N = 0.
*
      IF( M.LE.0 .OR. N.LE.0 ) THEN
         RESID = ZERO
         RETURN
      END IF
*
*     Determine EPS and the norm of A.
*
      EPS = DLAMCH( 'Epsilon' )
      ANORM = DLANGE( '1', M, N, A, LDA, RWORK )
*
*     Compute the product L*U and overwrite AFAC with the result.
*     A column at a time of the product is obtained, starting with
*     column N.
*
      DO 10 K = N, 1, -1
         IF( K.GT.M ) THEN
            CALL DTRMV( 'Lower', 'No transpose', 'Unit', M, AFAC,
     $                  LDAFAC, AFAC( 1, K ), 1 )
         ELSE
*
*           Compute elements (K+1:M,K)
*
            T = AFAC( K, K )
            IF( K+1.LE.M ) THEN
               CALL DSCAL( M-K, T, AFAC( K+1, K ), 1 )
               CALL DGEMV( 'No transpose', M-K, K-1, ONE,
     $                     AFAC( K+1, 1 ), LDAFAC, AFAC( 1, K ), 1, ONE,
     $                     AFAC( K+1, K ), 1 )
            END IF
*
*           Compute the (K,K) element
*
            AFAC( K, K ) = T + DDOT( K-1, AFAC( K, 1 ), LDAFAC,
     $                     AFAC( 1, K ), 1 )
*
*           Compute elements (1:K-1,K)
*
            CALL DTRMV( 'Lower', 'No transpose', 'Unit', K-1, AFAC,
     $                  LDAFAC, AFAC( 1, K ), 1 )
         END IF
   10 CONTINUE
      CALL DLASWP( N, AFAC, LDAFAC, 1, MIN( M, N ), IPIV, -1 )
*
*     Compute the difference  L*U - A  and store in AFAC.
*
      DO 30 J = 1, N
         DO 20 I = 1, M
            AFAC( I, J ) = AFAC( I, J ) - A( I, J )
   20    CONTINUE
   30 CONTINUE
*
*     Compute norm( L*U - A ) / ( N * norm(A) * EPS )
*
      RESID = DLANGE( '1', M, N, AFAC, LDAFAC, RWORK )
*
      IF( ANORM.LE.ZERO ) THEN
         IF( RESID.NE.ZERO )
     $      RESID = ONE / EPS
      ELSE
         RESID = ( ( RESID / DBLE( N ) ) / ANORM ) / EPS
      END IF
*
      RETURN
*
*     End of DGET01
*
      END
