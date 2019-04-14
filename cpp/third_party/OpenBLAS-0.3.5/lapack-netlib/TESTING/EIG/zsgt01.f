*> \brief \b ZSGT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZSGT01( ITYPE, UPLO, N, M, A, LDA, B, LDB, Z, LDZ, D,
*                          WORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            ITYPE, LDA, LDB, LDZ, M, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * ), RESULT( * ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * ),
*      $                   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CDGT01 checks a decomposition of the form
*>
*>    A Z   =  B Z D or
*>    A B Z =  Z D or
*>    B A Z =  Z D
*>
*> where A is a Hermitian matrix, B is Hermitian positive definite,
*> Z is unitary, and D is diagonal.
*>
*> One of the following test ratios is computed:
*>
*> ITYPE = 1:  RESULT(1) = | A Z - B Z D | / ( |A| |Z| n ulp )
*>
*> ITYPE = 2:  RESULT(1) = | A B Z - Z D | / ( |A| |Z| n ulp )
*>
*> ITYPE = 3:  RESULT(1) = | B A Z - Z D | / ( |A| |Z| n ulp )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ITYPE
*> \verbatim
*>          ITYPE is INTEGER
*>          The form of the Hermitian generalized eigenproblem.
*>          = 1:  A*z = (lambda)*B*z
*>          = 2:  A*B*z = (lambda)*z
*>          = 3:  B*A*z = (lambda)*z
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          Hermitian matrices A and B is stored.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of eigenvalues found.  M >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          The original Hermitian matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N)
*>          The original Hermitian positive definite matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension (LDZ, M)
*>          The computed eigenvectors of the generalized eigenproblem.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.  LDZ >= max(1,N).
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (M)
*>          The computed eigenvalues of the generalized eigenproblem.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N*N)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (1)
*>          The test ratio as described above.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZSGT01( ITYPE, UPLO, N, M, A, LDA, B, LDB, Z, LDZ, D,
     $                   WORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            ITYPE, LDA, LDB, LDZ, M, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * ), RESULT( * ), RWORK( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * ),
     $                   Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      DOUBLE PRECISION   ANORM, ULP
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE, ZLANHE
      EXTERNAL           DLAMCH, ZLANGE, ZLANHE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZDSCAL, ZHEMM
*     ..
*     .. Executable Statements ..
*
      RESULT( 1 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      ULP = DLAMCH( 'Epsilon' )
*
*     Compute product of 1-norms of A and Z.
*
      ANORM = ZLANHE( '1', UPLO, N, A, LDA, RWORK )*
     $        ZLANGE( '1', N, M, Z, LDZ, RWORK )
      IF( ANORM.EQ.ZERO )
     $   ANORM = ONE
*
      IF( ITYPE.EQ.1 ) THEN
*
*        Norm of AZ - BZD
*
         CALL ZHEMM( 'Left', UPLO, N, M, CONE, A, LDA, Z, LDZ, CZERO,
     $               WORK, N )
         DO 10 I = 1, M
            CALL ZDSCAL( N, D( I ), Z( 1, I ), 1 )
   10    CONTINUE
         CALL ZHEMM( 'Left', UPLO, N, M, CONE, B, LDB, Z, LDZ, -CONE,
     $               WORK, N )
*
         RESULT( 1 ) = ( ZLANGE( '1', N, M, WORK, N, RWORK ) / ANORM ) /
     $                 ( N*ULP )
*
      ELSE IF( ITYPE.EQ.2 ) THEN
*
*        Norm of ABZ - ZD
*
         CALL ZHEMM( 'Left', UPLO, N, M, CONE, B, LDB, Z, LDZ, CZERO,
     $               WORK, N )
         DO 20 I = 1, M
            CALL ZDSCAL( N, D( I ), Z( 1, I ), 1 )
   20    CONTINUE
         CALL ZHEMM( 'Left', UPLO, N, M, CONE, A, LDA, WORK, N, -CONE,
     $               Z, LDZ )
*
         RESULT( 1 ) = ( ZLANGE( '1', N, M, Z, LDZ, RWORK ) / ANORM ) /
     $                 ( N*ULP )
*
      ELSE IF( ITYPE.EQ.3 ) THEN
*
*        Norm of BAZ - ZD
*
         CALL ZHEMM( 'Left', UPLO, N, M, CONE, A, LDA, Z, LDZ, CZERO,
     $               WORK, N )
         DO 30 I = 1, M
            CALL ZDSCAL( N, D( I ), Z( 1, I ), 1 )
   30    CONTINUE
         CALL ZHEMM( 'Left', UPLO, N, M, CONE, B, LDB, WORK, N, -CONE,
     $               Z, LDZ )
*
         RESULT( 1 ) = ( ZLANGE( '1', N, M, Z, LDZ, RWORK ) / ANORM ) /
     $                 ( N*ULP )
      END IF
*
      RETURN
*
*     End of CDGT01
*
      END
