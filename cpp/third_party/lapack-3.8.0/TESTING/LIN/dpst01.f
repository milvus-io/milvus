*> \brief \b DPST01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DPST01( UPLO, N, A, LDA, AFAC, LDAFAC, PERM, LDPERM,
*                          PIV, RWORK, RESID, RANK )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   RESID
*       INTEGER            LDA, LDAFAC, LDPERM, N, RANK
*       CHARACTER          UPLO
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), AFAC( LDAFAC, * ),
*      $                   PERM( LDPERM, * ), RWORK( * )
*       INTEGER            PIV( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DPST01 reconstructs a symmetric positive semidefinite matrix A
*> from its L or U factors and the permutation matrix P and computes
*> the residual
*>    norm( P*L*L'*P' - A ) / ( N * norm(A) * EPS ) or
*>    norm( P*U'*U*P' - A ) / ( N * norm(A) * EPS ),
*> where EPS is the machine epsilon.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          symmetric matrix A is stored:
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
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The original symmetric matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N)
*> \endverbatim
*>
*> \param[in] AFAC
*> \verbatim
*>          AFAC is DOUBLE PRECISION array, dimension (LDAFAC,N)
*>          The factor L or U from the L*L' or U'*U
*>          factorization of A.
*> \endverbatim
*>
*> \param[in] LDAFAC
*> \verbatim
*>          LDAFAC is INTEGER
*>          The leading dimension of the array AFAC.  LDAFAC >= max(1,N).
*> \endverbatim
*>
*> \param[out] PERM
*> \verbatim
*>          PERM is DOUBLE PRECISION array, dimension (LDPERM,N)
*>          Overwritten with the reconstructed matrix, and then with the
*>          difference P*L*L'*P' - A (or P*U'*U*P' - A)
*> \endverbatim
*>
*> \param[in] LDPERM
*> \verbatim
*>          LDPERM is INTEGER
*>          The leading dimension of the array PERM.
*>          LDAPERM >= max(1,N).
*> \endverbatim
*>
*> \param[in] PIV
*> \verbatim
*>          PIV is INTEGER array, dimension (N)
*>          PIV is such that the nonzero entries are
*>          P( PIV( K ), K ) = 1.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
*>          If UPLO = 'L', norm(L*L' - A) / ( N * norm(A) * EPS )
*>          If UPLO = 'U', norm(U'*U - A) / ( N * norm(A) * EPS )
*> \endverbatim
*>
*> \param[in] RANK
*> \verbatim
*>          RANK is INTEGER
*>          number of nonzero singular values of A.
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
      SUBROUTINE DPST01( UPLO, N, A, LDA, AFAC, LDAFAC, PERM, LDPERM,
     $                   PIV, RWORK, RESID, RANK )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   RESID
      INTEGER            LDA, LDAFAC, LDPERM, N, RANK
      CHARACTER          UPLO
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), AFAC( LDAFAC, * ),
     $                   PERM( LDPERM, * ), RWORK( * )
      INTEGER            PIV( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   ANORM, EPS, T
      INTEGER            I, J, K
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DDOT, DLAMCH, DLANSY
      LOGICAL            LSAME
      EXTERNAL           DDOT, DLAMCH, DLANSY, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DSCAL, DSYR, DTRMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE
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
      EPS = DLAMCH( 'Epsilon' )
      ANORM = DLANSY( '1', UPLO, N, A, LDA, RWORK )
      IF( ANORM.LE.ZERO ) THEN
         RESID = ONE / EPS
         RETURN
      END IF
*
*     Compute the product U'*U, overwriting U.
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
         IF( RANK.LT.N ) THEN
            DO 110 J = RANK + 1, N
               DO 100 I = RANK + 1, J
                  AFAC( I, J ) = ZERO
  100          CONTINUE
  110       CONTINUE
         END IF
*
         DO 120 K = N, 1, -1
*
*           Compute the (K,K) element of the result.
*
            T = DDOT( K, AFAC( 1, K ), 1, AFAC( 1, K ), 1 )
            AFAC( K, K ) = T
*
*           Compute the rest of column K.
*
            CALL DTRMV( 'Upper', 'Transpose', 'Non-unit', K-1, AFAC,
     $                  LDAFAC, AFAC( 1, K ), 1 )
*
  120    CONTINUE
*
*     Compute the product L*L', overwriting L.
*
      ELSE
*
         IF( RANK.LT.N ) THEN
            DO 140 J = RANK + 1, N
               DO 130 I = J, N
                  AFAC( I, J ) = ZERO
  130          CONTINUE
  140       CONTINUE
         END IF
*
         DO 150 K = N, 1, -1
*           Add a multiple of column K of the factor L to each of
*           columns K+1 through N.
*
            IF( K+1.LE.N )
     $         CALL DSYR( 'Lower', N-K, ONE, AFAC( K+1, K ), 1,
     $                    AFAC( K+1, K+1 ), LDAFAC )
*
*           Scale column K by the diagonal element.
*
            T = AFAC( K, K )
            CALL DSCAL( N-K+1, T, AFAC( K, K ), 1 )
  150    CONTINUE
*
      END IF
*
*        Form P*L*L'*P' or P*U'*U*P'
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
         DO 170 J = 1, N
            DO 160 I = 1, N
               IF( PIV( I ).LE.PIV( J ) ) THEN
                  IF( I.LE.J ) THEN
                     PERM( PIV( I ), PIV( J ) ) = AFAC( I, J )
                  ELSE
                     PERM( PIV( I ), PIV( J ) ) = AFAC( J, I )
                  END IF
               END IF
  160       CONTINUE
  170    CONTINUE
*
*
      ELSE
*
         DO 190 J = 1, N
            DO 180 I = 1, N
               IF( PIV( I ).GE.PIV( J ) ) THEN
                  IF( I.GE.J ) THEN
                     PERM( PIV( I ), PIV( J ) ) = AFAC( I, J )
                  ELSE
                     PERM( PIV( I ), PIV( J ) ) = AFAC( J, I )
                  END IF
               END IF
  180       CONTINUE
  190    CONTINUE
*
      END IF
*
*     Compute the difference  P*L*L'*P' - A (or P*U'*U*P' - A).
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         DO 210 J = 1, N
            DO 200 I = 1, J
               PERM( I, J ) = PERM( I, J ) - A( I, J )
  200       CONTINUE
  210    CONTINUE
      ELSE
         DO 230 J = 1, N
            DO 220 I = J, N
               PERM( I, J ) = PERM( I, J ) - A( I, J )
  220       CONTINUE
  230    CONTINUE
      END IF
*
*     Compute norm( P*L*L'P - A ) / ( N * norm(A) * EPS ), or
*     ( P*U'*U*P' - A )/ ( N * norm(A) * EPS ).
*
      RESID = DLANSY( '1', UPLO, N, PERM, LDAFAC, RWORK )
*
      RESID = ( ( RESID / DBLE( N ) ) / ANORM ) / EPS
*
      RETURN
*
*     End of DPST01
*
      END
