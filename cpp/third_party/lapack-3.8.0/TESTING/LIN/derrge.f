*> \brief \b DERRGE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRGE( PATH, NUNIT )
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
*> DERRGE tests the error exits for the DOUBLE PRECISION routines
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DERRGE( PATH, NUNIT )
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
      DOUBLE PRECISION   ANRM, CCOND, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   R1( NMAX ), R2( NMAX ), W( LW ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DGBCON, DGBEQU, DGBRFS, DGBTF2,
     $                   DGBTRF, DGBTRS, DGECON, DGEEQU, DGERFS, DGETF2,
     $                   DGETRF, DGETRI, DGETRS
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
      INTRINSIC          DBLE
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
            A( I, J ) = 1.D0 / DBLE( I+J )
            AF( I, J ) = 1.D0 / DBLE( I+J )
   10    CONTINUE
         B( J ) = 0.D0
         R1( J ) = 0.D0
         R2( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
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
*        DGETRF
*
         SRNAMT = 'DGETRF'
         INFOT = 1
         CALL DGETRF( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGETRF( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'DGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGETRF( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'DGETRF', INFOT, NOUT, LERR, OK )
*
*        DGETF2
*
         SRNAMT = 'DGETF2'
         INFOT = 1
         CALL DGETF2( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGETF2( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'DGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGETF2( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'DGETF2', INFOT, NOUT, LERR, OK )
*
*        DGETRI
*
         SRNAMT = 'DGETRI'
         INFOT = 1
         CALL DGETRI( -1, A, 1, IP, W, LW, INFO )
         CALL CHKXER( 'DGETRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGETRI( 2, A, 1, IP, W, LW, INFO )
         CALL CHKXER( 'DGETRI', INFOT, NOUT, LERR, OK )
*
*        DGETRS
*
         SRNAMT = 'DGETRS'
         INFOT = 1
         CALL DGETRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGETRS( 'N', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGETRS( 'N', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGETRS( 'N', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'DGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGETRS( 'N', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'DGETRS', INFOT, NOUT, LERR, OK )
*
*        DGERFS
*
         SRNAMT = 'DGERFS'
         INFOT = 1
         CALL DGERFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGERFS( 'N', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGERFS( 'N', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGERFS( 'N', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGERFS( 'N', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DGERFS', INFOT, NOUT, LERR, OK )
*
*        DGECON
*
         SRNAMT = 'DGECON'
         INFOT = 1
         CALL DGECON( '/', 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DGECON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGECON( '1', -1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DGECON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGECON( '1', 2, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DGECON', INFOT, NOUT, LERR, OK )
*
*        DGEEQU
*
         SRNAMT = 'DGEEQU'
         INFOT = 1
         CALL DGEEQU( -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'DGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEEQU( 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'DGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEEQU( 2, 2, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'DGEEQU', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'GB' ) ) THEN
*
*        Test error exits of the routines that use the LU decomposition
*        of a general band matrix.
*
*        DGBTRF
*
         SRNAMT = 'DGBTRF'
         INFOT = 1
         CALL DGBTRF( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBTRF( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBTRF( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBTRF( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBTRF( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'DGBTRF', INFOT, NOUT, LERR, OK )
*
*        DGBTF2
*
         SRNAMT = 'DGBTF2'
         INFOT = 1
         CALL DGBTF2( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBTF2( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBTF2( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBTF2( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'DGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBTF2( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'DGBTF2', INFOT, NOUT, LERR, OK )
*
*        DGBTRS
*
         SRNAMT = 'DGBTRS'
         INFOT = 1
         CALL DGBTRS( '/', 0, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBTRS( 'N', -1, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBTRS( 'N', 1, -1, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBTRS( 'N', 1, 0, -1, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGBTRS( 'N', 1, 0, 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGBTRS( 'N', 2, 1, 1, 1, A, 3, IP, B, 2, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGBTRS( 'N', 2, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBTRS', INFOT, NOUT, LERR, OK )
*
*        DGBRFS
*
         SRNAMT = 'DGBRFS'
         INFOT = 1
         CALL DGBRFS( '/', 0, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBRFS( 'N', -1, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBRFS( 'N', 1, -1, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBRFS( 'N', 1, 0, -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGBRFS( 'N', 1, 0, 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGBRFS( 'N', 2, 1, 1, 1, A, 2, AF, 4, IP, B, 2, X, 2, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGBRFS( 'N', 2, 1, 1, 1, A, 3, AF, 3, IP, B, 2, X, 2, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 1, X, 2, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 2, X, 1, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DGBRFS', INFOT, NOUT, LERR, OK )
*
*        DGBCON
*
         SRNAMT = 'DGBCON'
         INFOT = 1
         CALL DGBCON( '/', 0, 0, 0, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBCON( '1', -1, 0, 0, A, 1, IP, ANRM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBCON( '1', 1, -1, 0, A, 1, IP, ANRM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBCON( '1', 1, 0, -1, A, 1, IP, ANRM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBCON( '1', 2, 1, 1, A, 3, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DGBCON', INFOT, NOUT, LERR, OK )
*
*        DGBEQU
*
         SRNAMT = 'DGBEQU'
         INFOT = 1
         CALL DGBEQU( -1, 0, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'DGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBEQU( 0, -1, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'DGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBEQU( 1, 1, -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'DGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBEQU( 1, 1, 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'DGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBEQU( 2, 2, 1, 1, A, 2, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'DGBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRGE
*
      END
