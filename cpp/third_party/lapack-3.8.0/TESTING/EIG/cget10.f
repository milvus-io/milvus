*> \brief \b CGET10
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGET10( M, N, A, LDA, B, LDB, WORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, M, N
*       REAL               RESULT
*       ..
*       .. Array Arguments ..
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGET10 compares two matrices A and B and computes the ratio
*> RESULT = norm( A - B ) / ( norm(A) * M * EPS )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrices A and B.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and B.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The m by n matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,N)
*>          The m by n matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,M).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (M)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is COMPLEX array, dimension (M)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL
*>          RESULT = norm( A - B ) / ( norm(A) * M * EPS )
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CGET10( M, N, A, LDA, B, LDB, WORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, M, N
      REAL               RESULT
*     ..
*     .. Array Arguments ..
      REAL               RWORK( * )
      COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            J
      REAL               ANORM, EPS, UNFL, WNORM
*     ..
*     .. External Functions ..
      REAL               SCASUM, SLAMCH, CLANGE
      EXTERNAL           SCASUM, SLAMCH, CLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           CAXPY, CCOPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( M.LE.0 .OR. N.LE.0 ) THEN
         RESULT = ZERO
         RETURN
      END IF
*
      UNFL = SLAMCH( 'Safe minimum' )
      EPS = SLAMCH( 'Precision' )
*
      WNORM = ZERO
      DO 10 J = 1, N
         CALL CCOPY( M, A( 1, J ), 1, WORK, 1 )
         CALL CAXPY( M, CMPLX( -ONE ), B( 1, J ), 1, WORK, 1 )
         WNORM = MAX( WNORM, SCASUM( N, WORK, 1 ) )
   10 CONTINUE
*
      ANORM = MAX( CLANGE( '1', M, N, A, LDA, RWORK ), UNFL )
*
      IF( ANORM.GT.WNORM ) THEN
         RESULT = ( WNORM / ANORM ) / ( M*EPS )
      ELSE
         IF( ANORM.LT.ONE ) THEN
            RESULT = ( MIN( WNORM, M*ANORM ) / ANORM ) / ( M*EPS )
         ELSE
            RESULT = MIN( WNORM / ANORM, REAL( M ) ) / ( M*EPS )
         END IF
      END IF
*
      RETURN
*
*     End of CGET10
*
      END
