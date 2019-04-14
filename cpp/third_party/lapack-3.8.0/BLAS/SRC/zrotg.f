*> \brief \b ZROTG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZROTG(CA,CB,C,S)
*
*       .. Scalar Arguments ..
*       COMPLEX*16 CA,CB,S
*       DOUBLE PRECISION C
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZROTG determines a double complex Givens rotation.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] CA
*> \verbatim
*>          CA is COMPLEX*16
*> \endverbatim
*>
*> \param[in] CB
*> \verbatim
*>          CB is COMPLEX*16
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is COMPLEX*16
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
*> \ingroup complex16_blas_level1
*
*  =====================================================================
      SUBROUTINE ZROTG(CA,CB,C,S)
*
*  -- Reference BLAS level1 routine (version 3.8.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      COMPLEX*16 CA,CB,S
      DOUBLE PRECISION C
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      COMPLEX*16 ALPHA
      DOUBLE PRECISION NORM,SCALE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC CDABS,DCMPLX,DCONJG,DSQRT
*     ..
      IF (CDABS(CA).EQ.0.0d0) THEN
         C = 0.0d0
         S = (1.0d0,0.0d0)
         CA = CB
      ELSE
         SCALE = CDABS(CA) + CDABS(CB)
         NORM = SCALE*DSQRT((CDABS(CA/DCMPLX(SCALE,0.0d0)))**2+
     $       (CDABS(CB/DCMPLX(SCALE,0.0d0)))**2)
         ALPHA = CA/CDABS(CA)
         C = CDABS(CA)/NORM
         S = ALPHA*DCONJG(CB)/NORM
         CA = ALPHA*NORM
      END IF
      RETURN
      END
