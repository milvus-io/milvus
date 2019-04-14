*> \brief \b ZGENND
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL FUNCTION ZGENND (M, N, A, LDA)
*
*       .. Scalar Arguments ..
*       INTEGER M, N, LDA
*       ..
*       .. Array Arguments ..
*       COMPLEX*16 A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZGENND tests that its argument has a real, non-negative diagonal.
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
*>          A is COMPLEX*16 array, dimension (LDA, N)
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
*> \ingroup complex16_lin
*
*  =====================================================================
      LOGICAL FUNCTION ZGENND (M, N, A, LDA)
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
      COMPLEX*16 A( LDA, * )
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
      COMPLEX*16 AII
*     ..
*     .. Intrinsics ..
      INTRINSIC MIN, DBLE, DIMAG
*     ..
*     .. Executable Statements ..
      K = MIN( M, N )
      DO I = 1, K
         AII = A( I, I )
         IF( DBLE( AII ).LT.ZERO.OR.DIMAG( AII ).NE.ZERO ) THEN
            ZGENND = .FALSE.
            RETURN
         END IF
      END DO
      ZGENND = .TRUE.
      RETURN
      END
