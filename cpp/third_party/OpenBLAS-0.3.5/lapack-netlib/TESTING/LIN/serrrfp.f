*> \brief \b SERRRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRRFP( NUNIT )
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
*> SERRRFP tests the error exits for the REAL driver routines
*> for solving linear systems of equations.
*>
*> SDRVRFP tests the REAL LAPACK RFP routines:
*>     STFSM, STFTRI, SSFRK, STFTTP, STFTTR, SPFTRF, SPFTRS, STPTTF,
*>     STPTTR, STRTTF, and STRTTP
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRRFP( NUNIT )
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
      REAL               ALPHA, BETA
*     ..
*     .. Local Arrays ..
      REAL               A( 1, 1), B( 1, 1)
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, STFSM, STFTRI, SSFRK, STFTTP, STFTTR,
     +                   SPFTRI, SPFTRF, SPFTRS, STPTTF, STPTTR, STRTTF,
     +                   STRTTP
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      OK = .TRUE.
      A( 1, 1 ) = 1.0E+0
      B( 1, 1 ) = 1.0E+0
      ALPHA     = 1.0E+0
      BETA      = 1.0E+0
*
      SRNAMT = 'SPFTRF'
      INFOT = 1
      CALL SPFTRF( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'SPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SPFTRF( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'SPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SPFTRF( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'SPFTRF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'SPFTRS'
      INFOT = 1
      CALL SPFTRS( '/', 'U', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'SPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SPFTRS( 'N', '/', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'SPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SPFTRS( 'N', 'U', -1, 0, A, B, 1, INFO )
      CALL CHKXER( 'SPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SPFTRS( 'N', 'U', 0, -1, A, B, 1, INFO )
      CALL CHKXER( 'SPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SPFTRS( 'N', 'U', 0, 0, A, B, 0, INFO )
      CALL CHKXER( 'SPFTRS', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'SPFTRI'
      INFOT = 1
      CALL SPFTRI( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'SPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SPFTRI( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'SPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SPFTRI( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'SPFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STFSM '
      INFOT = 1
      CALL STFSM( '/', 'L', 'U', 'T', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STFSM( 'N', '/', 'U', 'T', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL STFSM( 'N', 'L', '/', 'T', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL STFSM( 'N', 'L', 'U', '/', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL STFSM( 'N', 'L', 'U', 'T', '/', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL STFSM( 'N', 'L', 'U', 'T', 'U', -1, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL STFSM( 'N', 'L', 'U', 'T', 'U', 0, -1, ALPHA, A, B, 1 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL STFSM( 'N', 'L', 'U', 'T', 'U', 0, 0, ALPHA, A, B, 0 )
      CALL CHKXER( 'STFSM ', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STFTRI'
      INFOT = 1
      CALL STFTRI( '/', 'L', 'N', 0, A, INFO )
      CALL CHKXER( 'STFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STFTRI( 'N', '/', 'N', 0, A, INFO )
      CALL CHKXER( 'STFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL STFTRI( 'N', 'L', '/', 0, A, INFO )
      CALL CHKXER( 'STFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL STFTRI( 'N', 'L', 'N', -1, A, INFO )
      CALL CHKXER( 'STFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STFTTR'
      INFOT = 1
      CALL STFTTR( '/', 'U', 0, A, B, 1, INFO )
      CALL CHKXER( 'STFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STFTTR( 'N', '/', 0, A, B, 1, INFO )
      CALL CHKXER( 'STFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL STFTTR( 'N', 'U', -1, A, B, 1, INFO )
      CALL CHKXER( 'STFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL STFTTR( 'N', 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'STFTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STRTTF'
      INFOT = 1
      CALL STRTTF( '/', 'U', 0, A, 1, B, INFO )
      CALL CHKXER( 'STRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STRTTF( 'N', '/', 0, A, 1, B, INFO )
      CALL CHKXER( 'STRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL STRTTF( 'N', 'U', -1, A, 1, B, INFO )
      CALL CHKXER( 'STRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL STRTTF( 'N', 'U', 0, A, 0, B, INFO )
      CALL CHKXER( 'STRTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STFTTP'
      INFOT = 1
      CALL STFTTP( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'STFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STFTTP( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'STFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL STFTTP( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'STFTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STPTTF'
      INFOT = 1
      CALL STPTTF( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'STPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STPTTF( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'STPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL STPTTF( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'STPTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STRTTP'
      INFOT = 1
      CALL STRTTP( '/', 0, A, 1,  B, INFO )
      CALL CHKXER( 'STRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STRTTP( 'U', -1, A, 1,  B, INFO )
      CALL CHKXER( 'STRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL STRTTP( 'U', 0, A, 0,  B, INFO )
      CALL CHKXER( 'STRTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'STPTTR'
      INFOT = 1
      CALL STPTTR( '/', 0, A, B, 1,  INFO )
      CALL CHKXER( 'STPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL STPTTR( 'U', -1, A, B, 1,  INFO )
      CALL CHKXER( 'STPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL STPTTR( 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'STPTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'SSFRK '
      INFOT = 1
      CALL SSFRK( '/', 'U', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'SSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SSFRK( 'N', '/', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'SSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SSFRK( 'N', 'U', '/', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'SSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SSFRK( 'N', 'U', 'N', -1, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'SSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SSFRK( 'N', 'U', 'N', 0, -1, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'SSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SSFRK( 'N', 'U', 'N', 0, 0, ALPHA, A, 0, BETA, B )
      CALL CHKXER( 'SSFRK ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )
      ELSE
         WRITE( NOUT, FMT = 9998 )
      END IF
*
 9999 FORMAT( 1X, 'REAL RFP routines passed the tests of ',
     $        'the error exits' )
 9998 FORMAT( ' *** RFP routines failed the tests of the error ',
     $        'exits ***' )
      RETURN
*
*     End of SERRRFP
*
      END
