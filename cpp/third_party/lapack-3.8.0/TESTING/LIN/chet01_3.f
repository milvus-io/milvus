*> \brief \b CHET01_3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHET01_3( UPLO, N, A, LDA, AFAC, LDAFAC, E, IPIV, C,
*                            LDC, RWORK, RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            LDA, LDAFAC, LDC, N
*       REAL               RESID
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * ), AFAC( LDAFAC, * ), C( LDC, * ),
*                          E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CHET01_3 reconstructs a Hermitian indefinite matrix A from its
*> block L*D*L' or U*D*U' factorization computed by CHETRF_RK
*> (or CHETRF_BK) and computes the residual
*>    norm( C - A ) / ( N * norm(A) * EPS ),
*> where C is the reconstructed matrix and EPS is the machine epsilon.
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
*>          AFAC is COMPLEX array, dimension (LDAFAC,N)
*>          Diagonal of the block diagonal matrix D and factors U or L
*>          as computed by CHETRF_RK and CHETRF_BK:
*>            a) ONLY diagonal elements of the Hermitian block diagonal
*>               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k);
*>               (superdiagonal (or subdiagonal) elements of D
*>                should be provided on entry in array E), and
*>            b) If UPLO = 'U': factor U in the superdiagonal part of A.
*>               If UPLO = 'L': factor L in the subdiagonal part of A.
*> \endverbatim
*>
*> \param[in] LDAFAC
*> \verbatim
*>          LDAFAC is INTEGER
*>          The leading dimension of the array AFAC.
*>          LDAFAC >= max(1,N).
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is COMPLEX array, dimension (N)
*>          On entry, contains the superdiagonal (or subdiagonal)
*>          elements of the Hermitian block diagonal matrix D
*>          with 1-by-1 or 2-by-2 diagonal blocks, where
*>          If UPLO = 'U': E(i) = D(i-1,i),i=2:N, E(1) not referenced;
*>          If UPLO = 'L': E(i) = D(i+1,i),i=1:N-1, E(N) not referenced.
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          The pivot indices from CHETRF_RK (or CHETRF_BK).
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is COMPLEX array, dimension (LDC,N)
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of the array C.  LDC >= max(1,N).
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
*>          If UPLO = 'L', norm(L*D*L' - A) / ( N * norm(A) * EPS )
*>          If UPLO = 'U', norm(U*D*U' - A) / ( N * norm(A) * EPS )
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
*> \date June 2017
*
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CHET01_3( UPLO, N, A, LDA, AFAC, LDAFAC, E, IPIV, C,
     $                     LDC, RWORK, RESID )
*
*  -- LAPACK test routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            LDA, LDAFAC, LDC, N
      REAL               RESID
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      REAL               RWORK( * )
      COMPLEX            A( LDA, * ), AFAC( LDAFAC, * ), C( LDC, * ),
     $                   E( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
      REAL               ANORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANHE, SLAMCH
      EXTERNAL           LSAME, CLANHE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLASET, CLAVHE_ROOK, CSYCONVF_ROOK
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
*     a) Revert to multiplyers of L
*
      CALL CSYCONVF_ROOK( UPLO, 'R', N, AFAC, LDAFAC, E, IPIV, INFO )
*
*     1) Determine EPS and the norm of A.
*
      EPS = SLAMCH( 'Epsilon' )
      ANORM = CLANHE( '1', UPLO, N, A, LDA, RWORK )
*
*     Check the imaginary parts of the diagonal elements and return with
*     an error code if any are nonzero.
*
      DO J = 1, N
         IF( AIMAG( AFAC( J, J ) ).NE.ZERO ) THEN
            RESID = ONE / EPS
            RETURN
         END IF
      END DO
*
*     2) Initialize C to the identity matrix.
*
      CALL CLASET( 'Full', N, N, CZERO, CONE, C, LDC )
*
*     3) Call CLAVHE_ROOK to form the product D * U' (or D * L' ).
*
      CALL CLAVHE_ROOK( UPLO, 'Conjugate', 'Non-unit', N, N, AFAC,
     $                  LDAFAC, IPIV, C, LDC, INFO )
*
*     4) Call ZLAVHE_RK again to multiply by U (or L ).
*
      CALL CLAVHE_ROOK( UPLO, 'No transpose', 'Unit', N, N, AFAC,
     $                  LDAFAC, IPIV, C, LDC, INFO )
*
*     5) Compute the difference  C - A .
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         DO J = 1, N
            DO I = 1, J - 1
               C( I, J ) = C( I, J ) - A( I, J )
            END DO
            C( J, J ) = C( J, J ) - REAL( A( J, J ) )
         END DO
      ELSE
         DO J = 1, N
            C( J, J ) = C( J, J ) - REAL( A( J, J ) )
            DO I = J + 1, N
               C( I, J ) = C( I, J ) - A( I, J )
            END DO
         END DO
      END IF
*
*     6) Compute norm( C - A ) / ( N * norm(A) * EPS )
*
      RESID = CLANHE( '1', UPLO, N, C, LDC, RWORK )
*
      IF( ANORM.LE.ZERO ) THEN
         IF( RESID.NE.ZERO )
     $      RESID = ONE / EPS
      ELSE
         RESID = ( ( RESID/REAL( N ) )/ANORM ) / EPS
      END IF
*
*     b) Convert to factor of L (or U)
*
      CALL CSYCONVF_ROOK( UPLO, 'C', N, AFAC, LDAFAC, E, IPIV, INFO )
*
      RETURN
*
*     End of CHET01_3
*
      END
