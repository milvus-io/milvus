*> \brief \b ZGET54
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGET54( N, A, LDA, B, LDB, S, LDS, T, LDT, U, LDU, V,
*                          LDV, WORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LDS, LDT, LDU, LDV, N
*       DOUBLE PRECISION   RESULT
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), S( LDS, * ),
*      $                   T( LDT, * ), U( LDU, * ), V( LDV, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGET54 checks a generalized decomposition of the form
*>
*>          A = U*S*V'  and B = U*T* V'
*>
*> where ' means conjugate transpose and U and V are unitary.
*>
*> Specifically,
*>
*>   RESULT = ||( A - U*S*V', B - U*T*V' )|| / (||( A, B )||*n*ulp )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrix.  If it is zero, DGET54 does nothing.
*>          It must be at least zero.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          The original (unfactored) matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N)
*>          The original (unfactored) matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is COMPLEX*16 array, dimension (LDS, N)
*>          The factored matrix S.
*> \endverbatim
*>
*> \param[in] LDS
*> \verbatim
*>          LDS is INTEGER
*>          The leading dimension of S.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is COMPLEX*16 array, dimension (LDT, N)
*>          The factored matrix T.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of T.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is COMPLEX*16 array, dimension (LDU, N)
*>          The orthogonal matrix on the left-hand side in the
*>          decomposition.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  LDU must be at least N and
*>          at least 1.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is COMPLEX*16 array, dimension (LDV, N)
*>          The orthogonal matrix on the left-hand side in the
*>          decomposition.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of V.  LDV must be at least N and
*>          at least 1.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (3*N**2)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION
*>          The value RESULT, It is currently limited to 1/ulp, to
*>          avoid overflow. Errors are flagged by RESULT=10/ulp.
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
      SUBROUTINE ZGET54( N, A, LDA, B, LDB, S, LDS, T, LDT, U, LDU, V,
     $                   LDV, WORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LDS, LDT, LDU, LDV, N
      DOUBLE PRECISION   RESULT
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), S( LDS, * ),
     $                   T( LDT, * ), U( LDU, * ), V( LDV, * ),
     $                   WORK( * )
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
      DOUBLE PRECISION   ABNORM, ULP, UNFL, WNORM
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   DUM( 1 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZLACPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      RESULT = ZERO
      IF( N.LE.0 )
     $   RETURN
*
*     Constants
*
      UNFL = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
*
*     compute the norm of (A,B)
*
      CALL ZLACPY( 'Full', N, N, A, LDA, WORK, N )
      CALL ZLACPY( 'Full', N, N, B, LDB, WORK( N*N+1 ), N )
      ABNORM = MAX( ZLANGE( '1', N, 2*N, WORK, N, DUM ), UNFL )
*
*     Compute W1 = A - U*S*V', and put in the array WORK(1:N*N)
*
      CALL ZLACPY( ' ', N, N, A, LDA, WORK, N )
      CALL ZGEMM( 'N', 'N', N, N, N, CONE, U, LDU, S, LDS, CZERO,
     $            WORK( N*N+1 ), N )
*
      CALL ZGEMM( 'N', 'C', N, N, N, -CONE, WORK( N*N+1 ), N, V, LDV,
     $            CONE, WORK, N )
*
*     Compute W2 = B - U*T*V', and put in the workarray W(N*N+1:2*N*N)
*
      CALL ZLACPY( ' ', N, N, B, LDB, WORK( N*N+1 ), N )
      CALL ZGEMM( 'N', 'N', N, N, N, CONE, U, LDU, T, LDT, CZERO,
     $            WORK( 2*N*N+1 ), N )
*
      CALL ZGEMM( 'N', 'C', N, N, N, -CONE, WORK( 2*N*N+1 ), N, V, LDV,
     $            CONE, WORK( N*N+1 ), N )
*
*     Compute norm(W)/ ( ulp*norm((A,B)) )
*
      WNORM = ZLANGE( '1', N, 2*N, WORK, N, DUM )
*
      IF( ABNORM.GT.WNORM ) THEN
         RESULT = ( WNORM / ABNORM ) / ( 2*N*ULP )
      ELSE
         IF( ABNORM.LT.ONE ) THEN
            RESULT = ( MIN( WNORM, 2*N*ABNORM ) / ABNORM ) / ( 2*N*ULP )
         ELSE
            RESULT = MIN( WNORM / ABNORM, DBLE( 2*N ) ) / ( 2*N*ULP )
         END IF
      END IF
*
      RETURN
*
*     End of ZGET54
*
      END
