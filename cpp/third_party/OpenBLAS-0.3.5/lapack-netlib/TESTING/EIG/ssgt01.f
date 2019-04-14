*> \brief \b SSGT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SSGT01( ITYPE, UPLO, N, M, A, LDA, B, LDB, Z, LDZ, D,
*                          WORK, RESULT )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            ITYPE, LDA, LDB, LDZ, M, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), B( LDB, * ), D( * ), RESULT( * ),
*      $                   WORK( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SSGT01 checks a decomposition of the form
*>
*>    A Z   =  B Z D or
*>    A B Z =  Z D or
*>    B A Z =  Z D
*>
*> where A is a symmetric matrix, B is
*> symmetric positive definite, Z is orthogonal, and D is diagonal.
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
*>          The form of the symmetric generalized eigenproblem.
*>          = 1:  A*z = (lambda)*B*z
*>          = 2:  A*B*z = (lambda)*z
*>          = 3:  B*A*z = (lambda)*z
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          symmetric matrices A and B is stored.
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
*>          The number of eigenvalues found.  0 <= M <= N.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA, N)
*>          The original symmetric matrix A.
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
*>          B is REAL array, dimension (LDB, N)
*>          The original symmetric positive definite matrix B.
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
*>          Z is REAL array, dimension (LDZ, M)
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
*>          D is REAL array, dimension (M)
*>          The computed eigenvalues of the generalized eigenproblem.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (N*N)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (1)
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SSGT01( ITYPE, UPLO, N, M, A, LDA, B, LDB, Z, LDZ, D,
     $                   WORK, RESULT )
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
      REAL               A( LDA, * ), B( LDB, * ), D( * ), RESULT( * ),
     $                   WORK( * ), Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      REAL               ANORM, ULP
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLANGE, SLANSY
      EXTERNAL           SLAMCH, SLANGE, SLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           SSCAL, SSYMM
*     ..
*     .. Executable Statements ..
*
      RESULT( 1 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      ULP = SLAMCH( 'Epsilon' )
*
*     Compute product of 1-norms of A and Z.
*
      ANORM = SLANSY( '1', UPLO, N, A, LDA, WORK )*
     $        SLANGE( '1', N, M, Z, LDZ, WORK )
      IF( ANORM.EQ.ZERO )
     $   ANORM = ONE
*
      IF( ITYPE.EQ.1 ) THEN
*
*        Norm of AZ - BZD
*
         CALL SSYMM( 'Left', UPLO, N, M, ONE, A, LDA, Z, LDZ, ZERO,
     $               WORK, N )
         DO 10 I = 1, M
            CALL SSCAL( N, D( I ), Z( 1, I ), 1 )
   10    CONTINUE
         CALL SSYMM( 'Left', UPLO, N, M, ONE, B, LDB, Z, LDZ, -ONE,
     $               WORK, N )
*
         RESULT( 1 ) = ( SLANGE( '1', N, M, WORK, N, WORK ) / ANORM ) /
     $                 ( N*ULP )
*
      ELSE IF( ITYPE.EQ.2 ) THEN
*
*        Norm of ABZ - ZD
*
         CALL SSYMM( 'Left', UPLO, N, M, ONE, B, LDB, Z, LDZ, ZERO,
     $               WORK, N )
         DO 20 I = 1, M
            CALL SSCAL( N, D( I ), Z( 1, I ), 1 )
   20    CONTINUE
         CALL SSYMM( 'Left', UPLO, N, M, ONE, A, LDA, WORK, N, -ONE, Z,
     $               LDZ )
*
         RESULT( 1 ) = ( SLANGE( '1', N, M, Z, LDZ, WORK ) / ANORM ) /
     $                 ( N*ULP )
*
      ELSE IF( ITYPE.EQ.3 ) THEN
*
*        Norm of BAZ - ZD
*
         CALL SSYMM( 'Left', UPLO, N, M, ONE, A, LDA, Z, LDZ, ZERO,
     $               WORK, N )
         DO 30 I = 1, M
            CALL SSCAL( N, D( I ), Z( 1, I ), 1 )
   30    CONTINUE
         CALL SSYMM( 'Left', UPLO, N, M, ONE, B, LDB, WORK, N, -ONE, Z,
     $               LDZ )
*
         RESULT( 1 ) = ( SLANGE( '1', N, M, Z, LDZ, WORK ) / ANORM ) /
     $                 ( N*ULP )
      END IF
*
      RETURN
*
*     End of SSGT01
*
      END
