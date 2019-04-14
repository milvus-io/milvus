*> \brief \b SERRGE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRGE( PATH, NUNIT )
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
*> SERRGE tests the error exits for the REAL routines
*> for general matrices.
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRGE( PATH, NUNIT )
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
      INTEGER            NMAX, LW
      PARAMETER          ( NMAX = 4, LW = 3*NMAX )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, INFO, J
      REAL               ANRM, CCOND, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      REAL               A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   R1( NMAX ), R2( NMAX ), W( LW ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SGBCON, SGBEQU, SGBRFS, SGBTF2,
     $                   SGBTRF, SGBTRS, SGECON, SGEEQU, SGERFS, SGETF2,
     $                   SGETRF, SGETRI, SGETRS
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
      INTRINSIC          REAL
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
      C2 = PATH( 2: 3 )
*
*     Set the variables to innocuous values.
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = 1. / REAL( I+J )
            AF( I, J ) = 1. / REAL( I+J )
   10    CONTINUE
         B( J ) = 0.
         R1( J ) = 0.
         R2( J ) = 0.
         W( J ) = 0.
         X( J ) = 0.
         IP( J ) = J
         IW( J ) = J
   20 CONTINUE
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GE' ) ) THEN
*
*        Test error exits of the routines that use the LU decomposition
*        of a general matrix.
*
*        SGETRF
*
         SRNAMT = 'SGETRF'
         INFOT = 1
         CALL SGETRF( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGETRF( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'SGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGETRF( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'SGETRF', INFOT, NOUT, LERR, OK )
*
*        SGETF2
*
         SRNAMT = 'SGETF2'
         INFOT = 1
         CALL SGETF2( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGETF2( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'SGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGETF2( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'SGETF2', INFOT, NOUT, LERR, OK )
*
*        SGETRI
*
         SRNAMT = 'SGETRI'
         INFOT = 1
         CALL SGETRI( -1, A, 1, IP, W, LW, INFO )
         CALL CHKXER( 'SGETRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGETRI( 2, A, 1, IP, W, LW, INFO )
         CALL CHKXER( 'SGETRI', INFOT, NOUT, LERR, OK )
*
*        SGETRS
*
         SRNAMT = 'SGETRS'
         INFOT = 1
         CALL SGETRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGETRS( 'N', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGETRS( 'N', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGETRS( 'N', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'SGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGETRS( 'N', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'SGETRS', INFOT, NOUT, LERR, OK )
*
*        SGERFS
*
         SRNAMT = 'SGERFS'
         INFOT = 1
         CALL SGERFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGERFS( 'N', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGERFS( 'N', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGERFS( 'N', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGERFS( 'N', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SGERFS', INFOT, NOUT, LERR, OK )
*
*        SGECON
*
         SRNAMT = 'SGECON'
         INFOT = 1
         CALL SGECON( '/', 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SGECON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGECON( '1', -1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SGECON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGECON( '1', 2, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SGECON', INFOT, NOUT, LERR, OK )
*
*        SGEEQU
*
         SRNAMT = 'SGEEQU'
         INFOT = 1
         CALL SGEEQU( -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'SGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEEQU( 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'SGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEEQU( 2, 2, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'SGEEQU', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'GB' ) ) THEN
*
*        Test error exits of the routines that use the LU decomposition
*        of a general band matrix.
*
*        SGBTRF
*
         SRNAMT = 'SGBTRF'
         INFOT = 1
         CALL SGBTRF( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBTRF( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBTRF( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBTRF( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGBTRF( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'SGBTRF', INFOT, NOUT, LERR, OK )
*
*        SGBTF2
*
         SRNAMT = 'SGBTF2'
         INFOT = 1
         CALL SGBTF2( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBTF2( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBTF2( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBTF2( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'SGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGBTF2( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'SGBTF2', INFOT, NOUT, LERR, OK )
*
*        SGBTRS
*
         SRNAMT = 'SGBTRS'
         INFOT = 1
         CALL SGBTRS( '/', 0, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBTRS( 'N', -1, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBTRS( 'N', 1, -1, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBTRS( 'N', 1, 0, -1, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGBTRS( 'N', 1, 0, 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGBTRS( 'N', 2, 1, 1, 1, A, 3, IP, B, 2, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGBTRS( 'N', 2, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBTRS', INFOT, NOUT, LERR, OK )
*
*        SGBRFS
*
         SRNAMT = 'SGBRFS'
         INFOT = 1
         CALL SGBRFS( '/', 0, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBRFS( 'N', -1, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBRFS( 'N', 1, -1, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBRFS( 'N', 1, 0, -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGBRFS( 'N', 1, 0, 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGBRFS( 'N', 2, 1, 1, 1, A, 2, AF, 4, IP, B, 2, X, 2, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGBRFS( 'N', 2, 1, 1, 1, A, 3, AF, 3, IP, B, 2, X, 2, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 1, X, 2, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 2, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SGBRFS', INFOT, NOUT, LERR, OK )
*
*        SGBCON
*
         SRNAMT = 'SGBCON'
         INFOT = 1
         CALL SGBCON( '/', 0, 0, 0, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBCON( '1', -1, 0, 0, A, 1, IP, ANRM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'SGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBCON( '1', 1, -1, 0, A, 1, IP, ANRM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'SGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBCON( '1', 1, 0, -1, A, 1, IP, ANRM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'SGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGBCON( '1', 2, 1, 1, A, 3, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SGBCON', INFOT, NOUT, LERR, OK )
*
*        SGBEQU
*
         SRNAMT = 'SGBEQU'
         INFOT = 1
         CALL SGBEQU( -1, 0, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'SGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBEQU( 0, -1, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'SGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBEQU( 1, 1, -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'SGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBEQU( 1, 1, 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'SGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGBEQU( 2, 2, 1, 1, A, 2, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'SGBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRGE
*
      END
