*> \brief \b DLCTES
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL          FUNCTION DLCTES( ZR, ZI, D )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   D, ZI, ZR
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLCTES returns .TRUE. if the eigenvalue (ZR/D) + sqrt(-1)*(ZI/D)
*> is to be selected (specifically, in this subroutine, if the real
*> part of the eigenvalue is negative), and otherwise it returns
*> .FALSE..
*>
*> It is used by the test routine DDRGES to test whether the driver
*> routine DGGES successfully sorts eigenvalues.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ZR
*> \verbatim
*>          ZR is DOUBLE PRECISION
*>          The numerator of the real part of a complex eigenvalue
*>          (ZR/D) + i*(ZI/D).
*> \endverbatim
*>
*> \param[in] ZI
*> \verbatim
*>          ZI is DOUBLE PRECISION
*>          The numerator of the imaginary part of a complex eigenvalue
*>          (ZR/D) + i*(ZI).
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION
*>          The denominator part of a complex eigenvalue
*>          (ZR/D) + i*(ZI/D).
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
*> \date June 2016
*
*> \ingroup double_eig
*
*  =====================================================================
      LOGICAL          FUNCTION DLCTES( ZR, ZI, D )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   D, ZI, ZR
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          SIGN
*     ..
*     .. Executable Statements ..
*
      IF( D.EQ.ZERO ) THEN
         DLCTES = ( ZR.LT.ZERO )
      ELSE
         DLCTES = ( SIGN( ONE, ZR ).NE.SIGN( ONE, D ) )
      END IF
*
      RETURN
*
*     End of DLCTES
*
      END
