*> \brief \b ZLCTES
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL          FUNCTION ZLCTES( Z, D )
*
*       .. Scalar Arguments ..
*       COMPLEX*16         D, Z
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLCTES returns .TRUE. if the eigenvalue Z/D is to be selected
*> (specifically, in this subroutine, if the real part of the
*> eigenvalue is negative), and otherwise it returns .FALSE..
*>
*> It is used by the test routine ZDRGES to test whether the driver
*> routine ZGGES successfully sorts eigenvalues.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] Z
*> \verbatim
*>          Z is COMPLEX*16
*>          The numerator part of a complex eigenvalue Z/D.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is COMPLEX*16
*>          The denominator part of a complex eigenvalue Z/D.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      LOGICAL          FUNCTION ZLCTES( Z, D )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      COMPLEX*16         D, Z
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
*
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   ZMAX
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DIMAG, MAX, SIGN
*     ..
*     .. Executable Statements ..
*
      IF( D.EQ.CZERO ) THEN
         ZLCTES = ( DBLE( Z ).LT.ZERO )
      ELSE
         IF( DBLE( Z ).EQ.ZERO .OR. DBLE( D ).EQ.ZERO ) THEN
            ZLCTES = ( SIGN( ONE, DIMAG( Z ) ).NE.
     $               SIGN( ONE, DIMAG( D ) ) )
         ELSE IF( DIMAG( Z ).EQ.ZERO .OR. DIMAG( D ).EQ.ZERO ) THEN
            ZLCTES = ( SIGN( ONE, DBLE( Z ) ).NE.
     $               SIGN( ONE, DBLE( D ) ) )
         ELSE
            ZMAX = MAX( ABS( DBLE( Z ) ), ABS( DIMAG( Z ) ) )
            ZLCTES = ( ( DBLE( Z ) / ZMAX )*DBLE( D )+
     $               ( DIMAG( Z ) / ZMAX )*DIMAG( D ).LT.ZERO )
         END IF
      END IF
*
      RETURN
*
*     End of ZLCTES
*
      END
