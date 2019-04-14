*> \brief \b SROTG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SROTG(SA,SB,C,S)
*
*       .. Scalar Arguments ..
*       REAL C,S,SA,SB
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    SROTG construct givens plane rotation.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SA
*> \verbatim
*>          SA is REAL
*> \endverbatim
*>
*> \param[in] SB
*> \verbatim
*>          SB is REAL
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is REAL
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL
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
*> \date November 2017
*
*> \ingroup single_blas_level1
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>     jack dongarra, linpack, 3/11/78.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SROTG(SA,SB,C,S)
*
*  -- Reference BLAS level1 routine (version 3.8.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      REAL C,S,SA,SB
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      REAL R,ROE,SCALE,Z
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC ABS,SIGN,SQRT
*     ..
      ROE = SB
      IF (ABS(SA).GT.ABS(SB)) ROE = SA
      SCALE = ABS(SA) + ABS(SB)
      IF (SCALE.EQ.0.0) THEN
         C = 1.0
         S = 0.0
         R = 0.0
         Z = 0.0
      ELSE
         R = SCALE*SQRT((SA/SCALE)**2+ (SB/SCALE)**2)
         R = SIGN(1.0,ROE)*R
         C = SA/R
         S = SB/R
         Z = 1.0
         IF (ABS(SA).GT.ABS(SB)) Z = S
         IF (ABS(SB).GE.ABS(SA) .AND. C.NE.0.0) Z = 1.0/C
      END IF
      SA = R
      SB = Z
      RETURN
      END
