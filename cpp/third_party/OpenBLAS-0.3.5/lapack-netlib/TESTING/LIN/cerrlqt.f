*> \brief \b CERRLQT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRLQT( PATH, NUNIT )
*
*       .. Scalar Arguments ..
*       CHARACTER*3        PATH
*       INTEGER            NUNIT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CERRLQT tests the error exits for the COMPLEX routines
*> that use the LQT decomposition of a general matrix.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name for the routines to be tested.
*> \endverbatim
*>
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
      SUBROUTINE CERRLQT( PATH, NUNIT )
      IMPLICIT NONE
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 2 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
*     ..
*     .. Local Arrays ..
      COMPLEX            A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, CGELQT3, CGELQT,
     $                   CGEMLQT
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
*     .. Intrinsic Functions ..
      INTRINSIC          REAL, CMPLX
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
*
*     Set the variables to innocuous values.
*
      DO J = 1, NMAX
         DO I = 1, NMAX
            A( I, J ) = 1.E0 / CMPLX( REAL( I+J ), 0.E0 )
            C( I, J ) = 1.E0 / CMPLX( REAL( I+J ), 0.E0 )
            T( I, J ) = 1.E0 / CMPLX( REAL( I+J ), 0.E0 )
         END DO
         W( J ) = 0.E0
      END DO
      OK = .TRUE.
*
*     Error exits for LQT factorization
*
*     CGELQT
*
      SRNAMT = 'CGELQT'
      INFOT = 1
      CALL CGELQT( -1, 0, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'CGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQT( 0, -1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'CGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGELQT( 0, 0, 0, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'CGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGELQT( 2, 1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'CGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CGELQT( 2, 2, 2, A, 2, T, 1, W, INFO )
      CALL CHKXER( 'CGELQT', INFOT, NOUT, LERR, OK )
*
*     CGELQT3
*
      SRNAMT = 'CGELQT3'
      INFOT = 1
      CALL CGELQT3( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'CGELQT3', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQT3( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'CGELQT3', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGELQT3( 2, 2, A, 1, T, 1, INFO )
      CALL CHKXER( 'CGELQT3', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL CGELQT3( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'CGELQT3', INFOT, NOUT, LERR, OK )
*
*     CGEMLQT
*
      SRNAMT = 'CGEMLQT'
      INFOT = 1
      CALL CGEMLQT( '/', 'N', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEMLQT( 'L', '/', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGEMLQT( 'L', 'N', -1, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGEMLQT( 'L', 'N', 0, -1, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEMLQT( 'L', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEMLQT( 'R', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL CGEMLQT( 'L', 'N', 0, 0, 0, 0, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGEMLQT( 'R', 'N', 2, 2, 2, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGEMLQT( 'L', 'N', 2, 2, 2, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CGEMLQT( 'R', 'N', 1, 1, 1, 1, A, 1, T, 0, C, 1, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CGEMLQT( 'L', 'N', 1, 1, 1, 1, A, 1, T, 1, C, 0, W, INFO )
      CALL CHKXER( 'CGEMLQT', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRLQT
*
      END
