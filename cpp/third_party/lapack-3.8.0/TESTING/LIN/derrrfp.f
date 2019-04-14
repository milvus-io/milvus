*> \brief \b DERRRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRRFP( NUNIT )
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
*> DERRRFP tests the error exits for the DOUBLE PRECISION driver routines
*> for solving linear systems of equations.
*>
*> DDRVRFP tests the DOUBLE PRECISION LAPACK RFP routines:
*>     DTFSM, DTFTRI, DSFRK, DTFTTP, DTFTTR, DPFTRF, DPFTRS, DTPTTF,
*>     DTPTTR, DTRTTF, and DTRTTP
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DERRRFP( NUNIT )
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
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   A( 1, 1), B( 1, 1)
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DTFSM, DTFTRI, DSFRK, DTFTTP, DTFTTR,
     +                   DPFTRI, DPFTRF, DPFTRS, DTPTTF, DTPTTR, DTRTTF,
     +                   DTRTTP
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
      A( 1, 1 ) = 1.0D+0
      B( 1, 1 ) = 1.0D+0
      ALPHA     = 1.0D+0
      BETA      = 1.0D+0
*
      SRNAMT = 'DPFTRF'
      INFOT = 1
      CALL DPFTRF( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'DPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DPFTRF( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'DPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DPFTRF( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'DPFTRF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DPFTRS'
      INFOT = 1
      CALL DPFTRS( '/', 'U', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'DPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DPFTRS( 'N', '/', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'DPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DPFTRS( 'N', 'U', -1, 0, A, B, 1, INFO )
      CALL CHKXER( 'DPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DPFTRS( 'N', 'U', 0, -1, A, B, 1, INFO )
      CALL CHKXER( 'DPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DPFTRS( 'N', 'U', 0, 0, A, B, 0, INFO )
      CALL CHKXER( 'DPFTRS', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DPFTRI'
      INFOT = 1
      CALL DPFTRI( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'DPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DPFTRI( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'DPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DPFTRI( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'DPFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTFSM '
      INFOT = 1
      CALL DTFSM( '/', 'L', 'U', 'T', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTFSM( 'N', '/', 'U', 'T', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTFSM( 'N', 'L', '/', 'T', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTFSM( 'N', 'L', 'U', '/', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DTFSM( 'N', 'L', 'U', 'T', '/', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DTFSM( 'N', 'L', 'U', 'T', 'U', -1, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DTFSM( 'N', 'L', 'U', 'T', 'U', 0, -1, ALPHA, A, B, 1 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL DTFSM( 'N', 'L', 'U', 'T', 'U', 0, 0, ALPHA, A, B, 0 )
      CALL CHKXER( 'DTFSM ', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTFTRI'
      INFOT = 1
      CALL DTFTRI( '/', 'L', 'N', 0, A, INFO )
      CALL CHKXER( 'DTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTFTRI( 'N', '/', 'N', 0, A, INFO )
      CALL CHKXER( 'DTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTFTRI( 'N', 'L', '/', 0, A, INFO )
      CALL CHKXER( 'DTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTFTRI( 'N', 'L', 'N', -1, A, INFO )
      CALL CHKXER( 'DTFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTFTTR'
      INFOT = 1
      CALL DTFTTR( '/', 'U', 0, A, B, 1, INFO )
      CALL CHKXER( 'DTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTFTTR( 'N', '/', 0, A, B, 1, INFO )
      CALL CHKXER( 'DTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTFTTR( 'N', 'U', -1, A, B, 1, INFO )
      CALL CHKXER( 'DTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DTFTTR( 'N', 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'DTFTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTRTTF'
      INFOT = 1
      CALL DTRTTF( '/', 'U', 0, A, 1, B, INFO )
      CALL CHKXER( 'DTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTRTTF( 'N', '/', 0, A, 1, B, INFO )
      CALL CHKXER( 'DTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTRTTF( 'N', 'U', -1, A, 1, B, INFO )
      CALL CHKXER( 'DTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DTRTTF( 'N', 'U', 0, A, 0, B, INFO )
      CALL CHKXER( 'DTRTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTFTTP'
      INFOT = 1
      CALL DTFTTP( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'DTFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTFTTP( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'DTFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTFTTP( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'DTFTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTPTTF'
      INFOT = 1
      CALL DTPTTF( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'DTPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTPTTF( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'DTPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTPTTF( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'DTPTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTRTTP'
      INFOT = 1
      CALL DTRTTP( '/', 0, A, 1,  B, INFO )
      CALL CHKXER( 'DTRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTRTTP( 'U', -1, A, 1,  B, INFO )
      CALL CHKXER( 'DTRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTRTTP( 'U', 0, A, 0,  B, INFO )
      CALL CHKXER( 'DTRTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DTPTTR'
      INFOT = 1
      CALL DTPTTR( '/', 0, A, B, 1,  INFO )
      CALL CHKXER( 'DTPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTPTTR( 'U', -1, A, B, 1,  INFO )
      CALL CHKXER( 'DTPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DTPTTR( 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'DTPTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'DSFRK '
      INFOT = 1
      CALL DSFRK( '/', 'U', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'DSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DSFRK( 'N', '/', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'DSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DSFRK( 'N', 'U', '/', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'DSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DSFRK( 'N', 'U', 'N', -1, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'DSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DSFRK( 'N', 'U', 'N', 0, -1, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'DSFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DSFRK( 'N', 'U', 'N', 0, 0, ALPHA, A, 0, BETA, B )
      CALL CHKXER( 'DSFRK ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )
      ELSE
         WRITE( NOUT, FMT = 9998 )
      END IF
*
 9999 FORMAT( 1X, 'DOUBLE PRECISION RFP routines passed the tests of ',
     $        'the error exits' )
 9998 FORMAT( ' *** RFP routines failed the tests of the error ',
     $        'exits ***' )
      RETURN
*
*     End of DERRRFP
*
      END
