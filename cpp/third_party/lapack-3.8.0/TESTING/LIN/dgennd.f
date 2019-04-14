*> \brief \b DGENND
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL FUNCTION DGENND (M, N, A, LDA)
*
*       .. Scalar Arguments ..
*       INTEGER M, N, LDA
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DGENND tests that its argument has a non-negative diagonal.
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
*>          A is DOUBLE PRECISION array, dimension (LDA, N)
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
*> \ingroup double_lin
*
*  =====================================================================
      LOGICAL FUNCTION DGENND (M, N, A, LDA)
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
      DOUBLE PRECISION A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER I, K
*     ..
*     .. Intrinsics ..
      INTRINSIC MIN
*     ..
*     .. Executable Statements ..
      K = MIN( M, N )
      DO I = 1, K
         IF( A( I, I ).LT.ZERO ) THEN
            DGENND = .FALSE.
            RETURN
         END IF
      END DO
      DGENND = .TRUE.
      RETURN
      END
