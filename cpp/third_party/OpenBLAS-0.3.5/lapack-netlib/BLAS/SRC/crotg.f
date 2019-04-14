*> \brief \b CROTG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CROTG(CA,CB,C,S)
*
*       .. Scalar Arguments ..
*       COMPLEX CA,CB,S
*       REAL C
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CROTG determines a complex Givens rotation.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] CA
*> \verbatim
*>          CA is COMPLEX
*> \endverbatim
*>
*> \param[in] CB
*> \verbatim
*>          CB is COMPLEX
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is REAL
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is COMPLEX
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
*> \ingroup complex_blas_level1
*
*  =====================================================================
      SUBROUTINE CROTG(CA,CB,C,S)
*
*  -- Reference BLAS level1 routine (version 3.8.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      COMPLEX CA,CB,S
      REAL C
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      COMPLEX ALPHA
      REAL NORM,SCALE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC CABS,CONJG,SQRT
*     ..
      IF (CABS(CA).EQ.0.) THEN
         C = 0.
         S = (1.,0.)
         CA = CB
      ELSE
         SCALE = CABS(CA) + CABS(CB)
         NORM = SCALE*SQRT((CABS(CA/SCALE))**2+ (CABS(CB/SCALE))**2)
         ALPHA = CA/CABS(CA)
         C = CABS(CA)/NORM
         S = ALPHA*CONJG(CB)/NORM
         CA = ALPHA*NORM
      END IF
      RETURN
      END
