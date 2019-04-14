*> \brief \b ZERRRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRRFP( NUNIT )
*
*       .. Scalar Arguments ..
*       INTEGER            NUNIT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZERRRFP tests the error exits for the COMPLEX*16 driver routines
*> for solving linear systems of equations.
*>
*> ZDRVRFP tests the COMPLEX*16 LAPACK RFP routines:
*>     ZTFSM, ZTFTRI, ZHFRK, ZTFTTP, ZTFTTR, ZPFTRF, ZPFTRS, ZTPTTF,
*>     ZTPTTR, ZTRTTF, and ZTRTTP
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NUNIT
*> \verbatim
*>          NUNIT is INTEGER
*>          The unit number for output.
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
      SUBROUTINE ZERRRFP( NUNIT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     ..
*     .. Local Scalars ..
      INTEGER            INFO
      DOUBLE PRECISION   ALPHA, BETA
      COMPLEX*16         CALPHA
*     ..
*     .. Local Arrays ..
      COMPLEX*16         A( 1, 1), B( 1, 1)
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, ZTFSM, ZTFTRI, ZHFRK, ZTFTTP, ZTFTTR,
     +                   ZPFTRI, ZPFTRF, ZPFTRS, ZTPTTF, ZTPTTR, ZTRTTF,
     +                   ZTRTTP
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      OK = .TRUE.
      A( 1, 1 ) = DCMPLX( 1.0D0 , 1.0D0  )
      B( 1, 1 ) = DCMPLX( 1.0D0 , 1.0D0  )
      ALPHA     = 1.0D0
      CALPHA    = DCMPLX( 1.0D0 , 1.0D0  )
      BETA      = 1.0D0
*
      SRNAMT = 'ZPFTRF'
      INFOT = 1
      CALL ZPFTRF( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'ZPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZPFTRF( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'ZPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZPFTRF( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'ZPFTRF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZPFTRS'
      INFOT = 1
      CALL ZPFTRS( '/', 'U', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'ZPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZPFTRS( 'N', '/', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'ZPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZPFTRS( 'N', 'U', -1, 0, A, B, 1, INFO )
      CALL CHKXER( 'ZPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZPFTRS( 'N', 'U', 0, -1, A, B, 1, INFO )
      CALL CHKXER( 'ZPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZPFTRS( 'N', 'U', 0, 0, A, B, 0, INFO )
      CALL CHKXER( 'ZPFTRS', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZPFTRI'
      INFOT = 1
      CALL ZPFTRI( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'ZPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZPFTRI( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'ZPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZPFTRI( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'ZPFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTFSM '
      INFOT = 1
      CALL ZTFSM( '/', 'L', 'U', 'C', 'U', 0, 0, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTFSM( 'N', '/', 'U', 'C', 'U', 0, 0, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTFSM( 'N', 'L', '/', 'C', 'U', 0, 0, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZTFSM( 'N', 'L', 'U', '/', 'U', 0, 0, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZTFSM( 'N', 'L', 'U', 'C', '/', 0, 0, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZTFSM( 'N', 'L', 'U', 'C', 'U', -1, 0, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZTFSM( 'N', 'L', 'U', 'C', 'U', 0, -1, CALPHA, A, B, 1 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL ZTFSM( 'N', 'L', 'U', 'C', 'U', 0, 0, CALPHA, A, B, 0 )
      CALL CHKXER( 'ZTFSM ', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTFTRI'
      INFOT = 1
      CALL ZTFTRI( '/', 'L', 'N', 0, A, INFO )
      CALL CHKXER( 'ZTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTFTRI( 'N', '/', 'N', 0, A, INFO )
      CALL CHKXER( 'ZTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTFTRI( 'N', 'L', '/', 0, A, INFO )
      CALL CHKXER( 'ZTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZTFTRI( 'N', 'L', 'N', -1, A, INFO )
      CALL CHKXER( 'ZTFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTFTTR'
      INFOT = 1
      CALL ZTFTTR( '/', 'U', 0, A, B, 1, INFO )
      CALL CHKXER( 'ZTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTFTTR( 'N', '/', 0, A, B, 1, INFO )
      CALL CHKXER( 'ZTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTFTTR( 'N', 'U', -1, A, B, 1, INFO )
      CALL CHKXER( 'ZTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZTFTTR( 'N', 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'ZTFTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTRTTF'
      INFOT = 1
      CALL ZTRTTF( '/', 'U', 0, A, 1, B, INFO )
      CALL CHKXER( 'ZTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTRTTF( 'N', '/', 0, A, 1, B, INFO )
      CALL CHKXER( 'ZTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTRTTF( 'N', 'U', -1, A, 1, B, INFO )
      CALL CHKXER( 'ZTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZTRTTF( 'N', 'U', 0, A, 0, B, INFO )
      CALL CHKXER( 'ZTRTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTFTTP'
      INFOT = 1
      CALL ZTFTTP( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'ZTFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTFTTP( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'ZTFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTFTTP( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'ZTFTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTPTTF'
      INFOT = 1
      CALL ZTPTTF( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'ZTPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTPTTF( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'ZTPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTPTTF( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'ZTPTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTRTTP'
      INFOT = 1
      CALL ZTRTTP( '/', 0, A, 1,  B, INFO )
      CALL CHKXER( 'ZTRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTRTTP( 'U', -1, A, 1,  B, INFO )
      CALL CHKXER( 'ZTRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZTRTTP( 'U', 0, A, 0,  B, INFO )
      CALL CHKXER( 'ZTRTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZTPTTR'
      INFOT = 1
      CALL ZTPTTR( '/', 0, A, B, 1,  INFO )
      CALL CHKXER( 'ZTPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTPTTR( 'U', -1, A, B, 1,  INFO )
      CALL CHKXER( 'ZTPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZTPTTR( 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'ZTPTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'ZHFRK '
      INFOT = 1
      CALL ZHFRK( '/', 'U', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'ZHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZHFRK( 'N', '/', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'ZHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZHFRK( 'N', 'U', '/', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'ZHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZHFRK( 'N', 'U', 'N', -1, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'ZHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZHFRK( 'N', 'U', 'N', 0, -1, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'ZHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZHFRK( 'N', 'U', 'N', 0, 0, ALPHA, A, 0, BETA, B )
      CALL CHKXER( 'ZHFRK ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )
      ELSE
         WRITE( NOUT, FMT = 9998 )
      END IF
*
 9999 FORMAT( 1X, 'COMPLEX*16 RFP routines passed the tests of the ',
     $        'error exits' )
 9998 FORMAT( ' *** RFP routines failed the tests of the error ',
     $        'exits ***' )
      RETURN
*
*     End of ZERRRFP
*
      END
