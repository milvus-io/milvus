*> \brief \b CERRRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRRFP( NUNIT )
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
*> CERRRFP tests the error exits for the COMPLEX driver routines
*> for solving linear systems of equations.
*>
*> CDRVRFP tests the COMPLEX LAPACK RFP routines:
*>     CTFSM, CTFTRI, CHFRK, CTFTTP, CTFTTR, CPFTRF, CPFTRS, CTPTTF,
*>     CTPTTR, CTRTTF, and CTRTTP
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRRFP( NUNIT )
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
      COMPLEX            ALPHA, BETA
*     ..
*     .. Local Arrays ..
      COMPLEX            A( 1, 1), B( 1, 1)
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, CTFSM, CTFTRI, CHFRK, CTFTTP, CTFTTR,
     +                   CPFTRI, CPFTRF, CPFTRS, CTPTTF, CTPTTR, CTRTTF,
     +                   CTRTTP
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CMPLX
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      OK = .TRUE.
      A( 1, 1 ) = CMPLX( 1.D0 , 1.D0  )
      B( 1, 1 ) = CMPLX( 1.D0 , 1.D0  )
      ALPHA     = CMPLX( 1.D0 , 1.D0  )
      BETA      = CMPLX( 1.D0 , 1.D0  )
*
      SRNAMT = 'CPFTRF'
      INFOT = 1
      CALL CPFTRF( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'CPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CPFTRF( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'CPFTRF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CPFTRF( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'CPFTRF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CPFTRS'
      INFOT = 1
      CALL CPFTRS( '/', 'U', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'CPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CPFTRS( 'N', '/', 0, 0, A, B, 1, INFO )
      CALL CHKXER( 'CPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CPFTRS( 'N', 'U', -1, 0, A, B, 1, INFO )
      CALL CHKXER( 'CPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CPFTRS( 'N', 'U', 0, -1, A, B, 1, INFO )
      CALL CHKXER( 'CPFTRS', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CPFTRS( 'N', 'U', 0, 0, A, B, 0, INFO )
      CALL CHKXER( 'CPFTRS', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CPFTRI'
      INFOT = 1
      CALL CPFTRI( '/', 'U', 0, A, INFO )
      CALL CHKXER( 'CPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CPFTRI( 'N', '/', 0, A, INFO )
      CALL CHKXER( 'CPFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CPFTRI( 'N', 'U', -1, A, INFO )
      CALL CHKXER( 'CPFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTFSM '
      INFOT = 1
      CALL CTFSM( '/', 'L', 'U', 'C', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTFSM( 'N', '/', 'U', 'C', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CTFSM( 'N', 'L', '/', 'C', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CTFSM( 'N', 'L', 'U', '/', 'U', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CTFSM( 'N', 'L', 'U', 'C', '/', 0, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL CTFSM( 'N', 'L', 'U', 'C', 'U', -1, 0, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CTFSM( 'N', 'L', 'U', 'C', 'U', 0, -1, ALPHA, A, B, 1 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL CTFSM( 'N', 'L', 'U', 'C', 'U', 0, 0, ALPHA, A, B, 0 )
      CALL CHKXER( 'CTFSM ', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTFTRI'
      INFOT = 1
      CALL CTFTRI( '/', 'L', 'N', 0, A, INFO )
      CALL CHKXER( 'CTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTFTRI( 'N', '/', 'N', 0, A, INFO )
      CALL CHKXER( 'CTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CTFTRI( 'N', 'L', '/', 0, A, INFO )
      CALL CHKXER( 'CTFTRI', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CTFTRI( 'N', 'L', 'N', -1, A, INFO )
      CALL CHKXER( 'CTFTRI', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTFTTR'
      INFOT = 1
      CALL CTFTTR( '/', 'U', 0, A, B, 1, INFO )
      CALL CHKXER( 'CTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTFTTR( 'N', '/', 0, A, B, 1, INFO )
      CALL CHKXER( 'CTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CTFTTR( 'N', 'U', -1, A, B, 1, INFO )
      CALL CHKXER( 'CTFTTR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL CTFTTR( 'N', 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'CTFTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTRTTF'
      INFOT = 1
      CALL CTRTTF( '/', 'U', 0, A, 1, B, INFO )
      CALL CHKXER( 'CTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTRTTF( 'N', '/', 0, A, 1, B, INFO )
      CALL CHKXER( 'CTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CTRTTF( 'N', 'U', -1, A, 1, B, INFO )
      CALL CHKXER( 'CTRTTF', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CTRTTF( 'N', 'U', 0, A, 0, B, INFO )
      CALL CHKXER( 'CTRTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTFTTP'
      INFOT = 1
      CALL CTFTTP( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'CTFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTFTTP( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'CTFTTP', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CTFTTP( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'CTFTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTPTTF'
      INFOT = 1
      CALL CTPTTF( '/', 'U', 0, A, B, INFO )
      CALL CHKXER( 'CTPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTPTTF( 'N', '/', 0, A, B, INFO )
      CALL CHKXER( 'CTPTTF', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CTPTTF( 'N', 'U', -1, A, B, INFO )
      CALL CHKXER( 'CTPTTF', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTRTTP'
      INFOT = 1
      CALL CTRTTP( '/', 0, A, 1,  B, INFO )
      CALL CHKXER( 'CTRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTRTTP( 'U', -1, A, 1,  B, INFO )
      CALL CHKXER( 'CTRTTP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CTRTTP( 'U', 0, A, 0,  B, INFO )
      CALL CHKXER( 'CTRTTP', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CTPTTR'
      INFOT = 1
      CALL CTPTTR( '/', 0, A, B, 1,  INFO )
      CALL CHKXER( 'CTPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CTPTTR( 'U', -1, A, B, 1,  INFO )
      CALL CHKXER( 'CTPTTR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CTPTTR( 'U', 0, A, B, 0, INFO )
      CALL CHKXER( 'CTPTTR', INFOT, NOUT, LERR, OK )
*
      SRNAMT = 'CHFRK '
      INFOT = 1
      CALL CHFRK( '/', 'U', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'CHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CHFRK( 'N', '/', 'N', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'CHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CHFRK( 'N', 'U', '/', 0, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'CHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CHFRK( 'N', 'U', 'N', -1, 0, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'CHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CHFRK( 'N', 'U', 'N', 0, -1, ALPHA, A, 1, BETA, B )
      CALL CHKXER( 'CHFRK ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CHFRK( 'N', 'U', 'N', 0, 0, ALPHA, A, 0, BETA, B )
      CALL CHKXER( 'CHFRK ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )
      ELSE
         WRITE( NOUT, FMT = 9998 )
      END IF
*
 9999 FORMAT( 1X, 'COMPLEX RFP routines passed the tests of the ',
     $        'error exits' )
 9998 FORMAT( ' *** RFP routines failed the tests of the error ',
     $        'exits ***' )
      RETURN
*
*     End of CERRRFP
*
      END
