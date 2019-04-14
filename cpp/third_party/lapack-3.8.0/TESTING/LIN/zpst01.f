*> \brief \b ZPST01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZPST01( UPLO, N, A, LDA, AFAC, LDAFAC, PERM, LDPERM,
*                          PIV, RWORK, RESID, RANK )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   RESID
*       INTEGER            LDA, LDAFAC, LDPERM, N, RANK
*       CHARACTER          UPLO
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), AFAC( LDAFAC, * ),
*      $                   PERM( LDPERM, * )
*       DOUBLE PRECISION   RWORK( * )
*       INTEGER            PIV( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZPST01 reconstructs an Hermitian positive semidefinite matrix A
*> from its L or U factors and the permutation matrix P and computes
*> the residual
*>    norm( P*L*L'*P' - A ) / ( N * norm(A) * EPS ) or
*>    norm( P*U'*U*P' - A ) / ( N * norm(A) * EPS ),
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
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The original Hermitian matrix A.
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
*>          AFAC is COMPLEX*16 array, dimension (LDAFAC,N)
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
*>          PERM is COMPLEX*16 array, dimension (LDPERM,N)
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZPST01( UPLO, N, A, LDA, AFAC, LDAFAC, PERM, LDPERM,
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
      COMPLEX*16         A( LDA, * ), AFAC( LDAFAC, * ),
     $                   PERM( LDPERM, * )
      DOUBLE PRECISION   RWORK( * )
      INTEGER            PIV( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      COMPLEX*16         TC
      DOUBLE PRECISION   ANORM, EPS, TR
      INTEGER            I, J, K
*     ..
*     .. External Functions ..
      COMPLEX*16         ZDOTC
      DOUBLE PRECISION   DLAMCH, ZLANHE
      LOGICAL            LSAME
      EXTERNAL           ZDOTC, DLAMCH, ZLANHE, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZHER, ZSCAL, ZTRMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCONJG, DIMAG
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
      ANORM = ZLANHE( '1', UPLO, N, A, LDA, RWORK )
      IF( ANORM.LE.ZERO ) THEN
         RESID = ONE / EPS
         RETURN
      END IF
*
*     Check the imaginary parts of the diagonal elements and return with
*     an error code if any are nonzero.
*
      DO 100 J = 1, N
         IF( DIMAG( AFAC( J, J ) ).NE.ZERO ) THEN
            RESID = ONE / EPS
            RETURN
         END IF
  100 CONTINUE
*
*     Compute the product U'*U, overwriting U.
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
         IF( RANK.LT.N ) THEN
            DO 120 J = RANK + 1, N
               DO 110 I = RANK + 1, J
                  AFAC( I, J ) = CZERO
  110          CONTINUE
  120       CONTINUE
         END IF
*
         DO 130 K = N, 1, -1
*
*           Compute the (K,K) element of the result.
*
            TR = ZDOTC( K, AFAC( 1, K ), 1, AFAC( 1, K ), 1 )
            AFAC( K, K ) = TR
*
*           Compute the rest of column K.
*
            CALL ZTRMV( 'Upper', 'Conjugate', 'Non-unit', K-1, AFAC,
     $                  LDAFAC, AFAC( 1, K ), 1 )
*
  130    CONTINUE
*
*     Compute the product L*L', overwriting L.
*
      ELSE
*
         IF( RANK.LT.N ) THEN
            DO 150 J = RANK + 1, N
               DO 140 I = J, N
                  AFAC( I, J ) = CZERO
  140          CONTINUE
  150       CONTINUE
         END IF
*
         DO 160 K = N, 1, -1
*           Add a multiple of column K of the factor L to each of
*           columns K+1 through N.
*
            IF( K+1.LE.N )
     $         CALL ZHER( 'Lower', N-K, ONE, AFAC( K+1, K ), 1,
     $                    AFAC( K+1, K+1 ), LDAFAC )
*
*           Scale column K by the diagonal element.
*
            TC = AFAC( K, K )
            CALL ZSCAL( N-K+1, TC, AFAC( K, K ), 1 )
  160    CONTINUE
*
      END IF
*
*        Form P*L*L'*P' or P*U'*U*P'
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
         DO 180 J = 1, N
            DO 170 I = 1, N
               IF( PIV( I ).LE.PIV( J ) ) THEN
                  IF( I.LE.J ) THEN
                     PERM( PIV( I ), PIV( J ) ) = AFAC( I, J )
                  ELSE
                     PERM( PIV( I ), PIV( J ) ) = DCONJG( AFAC( J, I ) )
                  END IF
               END IF
  170       CONTINUE
  180    CONTINUE
*
*
      ELSE
*
         DO 200 J = 1, N
            DO 190 I = 1, N
               IF( PIV( I ).GE.PIV( J ) ) THEN
                  IF( I.GE.J ) THEN
                     PERM( PIV( I ), PIV( J ) ) = AFAC( I, J )
                  ELSE
                     PERM( PIV( I ), PIV( J ) ) = DCONJG( AFAC( J, I ) )
                  END IF
               END IF
  190       CONTINUE
  200    CONTINUE
*
      END IF
*
*     Compute the difference  P*L*L'*P' - A (or P*U'*U*P' - A).
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         DO 220 J = 1, N
            DO 210 I = 1, J - 1
               PERM( I, J ) = PERM( I, J ) - A( I, J )
  210       CONTINUE
            PERM( J, J ) = PERM( J, J ) - DBLE( A( J, J ) )
  220    CONTINUE
      ELSE
         DO 240 J = 1, N
            PERM( J, J ) = PERM( J, J ) - DBLE( A( J, J ) )
            DO 230 I = J + 1, N
               PERM( I, J ) = PERM( I, J ) - A( I, J )
  230       CONTINUE
  240    CONTINUE
      END IF
*
*     Compute norm( P*L*L'P - A ) / ( N * norm(A) * EPS ), or
*     ( P*U'*U*P' - A )/ ( N * norm(A) * EPS ).
*
      RESID = ZLANHE( '1', UPLO, N, PERM, LDAFAC, RWORK )
*
      RESID = ( ( RESID / DBLE( N ) ) / ANORM ) / EPS
*
      RETURN
*
*     End of ZPST01
*
      END
