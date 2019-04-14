*> \brief \b CLCTES
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL          FUNCTION CLCTES( Z, D )
*
*       .. Scalar Arguments ..
*       COMPLEX            D, Z
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLCTES returns .TRUE. if the eigenvalue Z/D is to be selected
*> (specifically, in this subroutine, if the real part of the
*> eigenvalue is negative), and otherwise it returns .FALSE..
*>
*> It is used by the test routine CDRGES to test whether the driver
*> routine CGGES successfully sorts eigenvalues.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] Z
*> \verbatim
*>          Z is COMPLEX
*>          The numerator part of a complex eigenvalue Z/D.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is COMPLEX
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
*> \ingroup complex_eig
*
*  =====================================================================
      LOGICAL          FUNCTION CLCTES( Z, D )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      COMPLEX            D, Z
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
*
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      REAL               ZMAX
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, MAX, REAL, SIGN
*     ..
*     .. Executable Statements ..
*
      IF( D.EQ.CZERO ) THEN
         CLCTES = ( REAL( Z ).LT.ZERO )
      ELSE
         IF( REAL( Z ).EQ.ZERO .OR. REAL( D ).EQ.ZERO ) THEN
            CLCTES = ( SIGN( ONE, AIMAG( Z ) ).NE.
     $               SIGN( ONE, AIMAG( D ) ) )
         ELSE IF( AIMAG( Z ).EQ.ZERO .OR. AIMAG( D ).EQ.ZERO ) THEN
            CLCTES = ( SIGN( ONE, REAL( Z ) ).NE.
     $               SIGN( ONE, REAL( D ) ) )
         ELSE
            ZMAX = MAX( ABS( REAL( Z ) ), ABS( AIMAG( Z ) ) )
            CLCTES = ( ( REAL( Z ) / ZMAX )*REAL( D )+
     $               ( AIMAG( Z ) / ZMAX )*AIMAG( D ).LT.ZERO )
         END IF
      END IF
*
      RETURN
*
*     End of CLCTES
*
      END
