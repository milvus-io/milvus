*> \brief \b CGENND
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL FUNCTION CGENND (M, N, A, LDA)
*
*       .. Scalar Arguments ..
*       INTEGER M, N, LDA
*       ..
*       .. Array Arguments ..
*       COMPLEX A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CGENND tests that its argument has a real, non-negative diagonal.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows in A.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns in A.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA, N)
*>          The matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          Leading dimension of A.
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
      LOGICAL FUNCTION CGENND (M, N, A, LDA)
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER M, N, LDA
*     ..
*     .. Array Arguments ..
      COMPLEX A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER I, K
      COMPLEX AII
*     ..
*     .. Intrinsics ..
      INTRINSIC MIN, REAL, AIMAG
*     ..
*     .. Executable Statements ..
      K = MIN( M, N )
      DO I = 1, K
         AII = A( I, I )
         IF( REAL( AII ).LT.ZERO.OR.AIMAG( AII ).NE.ZERO ) THEN
            CGENND = .FALSE.
            RETURN
         END IF
      END DO
      CGENND = .TRUE.
      RETURN
      END
