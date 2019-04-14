*> \brief \b CHKXER
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHKXER( SRNAMT, INFOT, NOUT, LERR, OK )
*
*       .. Scalar Arguments ..
*       LOGICAL            LERR, OK
*       CHARACTER*(*)      SRNAMT
*       INTEGER            INFOT, NOUT
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> \endverbatim
*
*  Arguments:
*  ==========
*
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date June 2017
*
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CHKXER( SRNAMT, INFOT, NOUT, LERR, OK )
*
*  -- LAPACK test routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      LOGICAL            LERR, OK
      CHARACTER*(*)      SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          LEN_TRIM
*     ..
*     .. Executable Statements ..
      IF( .NOT.LERR ) THEN
         WRITE( NOUT, FMT = 9999 )INFOT,
     $        SRNAMT( 1:LEN_TRIM( SRNAMT ) )
         OK = .FALSE.
      END IF
      LERR = .FALSE.
      RETURN
*
 9999 FORMAT( ' *** Illegal value of parameter number ', I2,
     $      ' not detected by ', A6, ' ***' )
*
*     End of CHKXER.
*
      END
