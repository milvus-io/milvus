*> \brief \b DERRPO
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRPO( PATH, NUNIT )
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
*> DERRPO tests the error exits for the DOUBLE PRECISION routines
*> for symmetric positive definite matrices.
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
      SUBROUTINE DERRPO( PATH, NUNIT )
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
      PARAMETER          ( NMAX = 4 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, INFO, J
      DOUBLE PRECISION   ANRM, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IW( NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   R1( NMAX ), R2( NMAX ), W( 3*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DPBCON, DPBEQU, DPBRFS, DPBTF2,
     $                   DPBTRF, DPBTRS, DPOCON, DPOEQU, DPORFS, DPOTF2,
     $                   DPOTRF, DPOTRI, DPOTRS, DPPCON, DPPEQU, DPPRFS,
     $                   DPPTRF, DPPTRI, DPPTRS
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
         IW( J ) = J
   20 CONTINUE
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive definite matrix.
*
*        DPOTRF
*
         SRNAMT = 'DPOTRF'
         INFOT = 1
         CALL DPOTRF( '/', 0, A, 1, INFO )
         CALL CHKXER( 'DPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOTRF( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'DPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPOTRF( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'DPOTRF', INFOT, NOUT, LERR, OK )
*
*        DPOTF2
*
         SRNAMT = 'DPOTF2'
         INFOT = 1
         CALL DPOTF2( '/', 0, A, 1, INFO )
         CALL CHKXER( 'DPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOTF2( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'DPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPOTF2( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'DPOTF2', INFOT, NOUT, LERR, OK )
*
*        DPOTRI
*
         SRNAMT = 'DPOTRI'
         INFOT = 1
         CALL DPOTRI( '/', 0, A, 1, INFO )
         CALL CHKXER( 'DPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOTRI( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'DPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPOTRI( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'DPOTRI', INFOT, NOUT, LERR, OK )
*
*        DPOTRS
*
         SRNAMT = 'DPOTRS'
         INFOT = 1
         CALL DPOTRS( '/', 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOTRS( 'U', -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPOTRS( 'U', 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPOTRS( 'U', 2, 1, A, 1, B, 2, INFO )
         CALL CHKXER( 'DPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DPOTRS( 'U', 2, 1, A, 2, B, 1, INFO )
         CALL CHKXER( 'DPOTRS', INFOT, NOUT, LERR, OK )
*
*        DPORFS
*
         SRNAMT = 'DPORFS'
         INFOT = 1
         CALL DPORFS( '/', 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPORFS( 'U', -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPORFS( 'U', 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPORFS( 'U', 2, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DPORFS( 'U', 2, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DPORFS( 'U', 2, 1, A, 2, AF, 2, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DPORFS( 'U', 2, 1, A, 2, AF, 2, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPORFS', INFOT, NOUT, LERR, OK )
*
*        DPOCON
*
         SRNAMT = 'DPOCON'
         INFOT = 1
         CALL DPOCON( '/', 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOCON( 'U', -1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPOCON( 'U', 2, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPOCON', INFOT, NOUT, LERR, OK )
*
*        DPOEQU
*
         SRNAMT = 'DPOEQU'
         INFOT = 1
         CALL DPOEQU( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPOEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPOEQU( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPOEQU', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PP' ) ) THEN
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive definite packed matrix.
*
*        DPPTRF
*
         SRNAMT = 'DPPTRF'
         INFOT = 1
         CALL DPPTRF( '/', 0, A, INFO )
         CALL CHKXER( 'DPPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPTRF( 'U', -1, A, INFO )
         CALL CHKXER( 'DPPTRF', INFOT, NOUT, LERR, OK )
*
*        DPPTRI
*
         SRNAMT = 'DPPTRI'
         INFOT = 1
         CALL DPPTRI( '/', 0, A, INFO )
         CALL CHKXER( 'DPPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPTRI( 'U', -1, A, INFO )
         CALL CHKXER( 'DPPTRI', INFOT, NOUT, LERR, OK )
*
*        DPPTRS
*
         SRNAMT = 'DPPTRS'
         INFOT = 1
         CALL DPPTRS( '/', 0, 0, A, B, 1, INFO )
         CALL CHKXER( 'DPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPTRS( 'U', -1, 0, A, B, 1, INFO )
         CALL CHKXER( 'DPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPPTRS( 'U', 0, -1, A, B, 1, INFO )
         CALL CHKXER( 'DPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPPTRS( 'U', 2, 1, A, B, 1, INFO )
         CALL CHKXER( 'DPPTRS', INFOT, NOUT, LERR, OK )
*
*        DPPRFS
*
         SRNAMT = 'DPPRFS'
         INFOT = 1
         CALL DPPRFS( '/', 0, 0, A, AF, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPRFS( 'U', -1, 0, A, AF, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPPRFS( 'U', 0, -1, A, AF, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DPPRFS( 'U', 2, 1, A, AF, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DPPRFS( 'U', 2, 1, A, AF, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DPPRFS', INFOT, NOUT, LERR, OK )
*
*        DPPCON
*
         SRNAMT = 'DPPCON'
         INFOT = 1
         CALL DPPCON( '/', 0, A, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPCON( 'U', -1, A, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPPCON', INFOT, NOUT, LERR, OK )
*
*        DPPEQU
*
         SRNAMT = 'DPPEQU'
         INFOT = 1
         CALL DPPEQU( '/', 0, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPPEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPEQU( 'U', -1, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPPEQU', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PB' ) ) THEN
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive definite band matrix.
*
*        DPBTRF
*
         SRNAMT = 'DPBTRF'
         INFOT = 1
         CALL DPBTRF( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'DPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBTRF( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'DPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBTRF( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'DPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPBTRF( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'DPBTRF', INFOT, NOUT, LERR, OK )
*
*        DPBTF2
*
         SRNAMT = 'DPBTF2'
         INFOT = 1
         CALL DPBTF2( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'DPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBTF2( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'DPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBTF2( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'DPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPBTF2( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'DPBTF2', INFOT, NOUT, LERR, OK )
*
*        DPBTRS
*
         SRNAMT = 'DPBTRS'
         INFOT = 1
         CALL DPBTRS( '/', 0, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBTRS( 'U', -1, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBTRS( 'U', 1, -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPBTRS( 'U', 0, 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPBTRS( 'U', 2, 1, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DPBTRS( 'U', 2, 0, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBTRS', INFOT, NOUT, LERR, OK )
*
*        DPBRFS
*
         SRNAMT = 'DPBRFS'
         INFOT = 1
         CALL DPBRFS( '/', 0, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBRFS( 'U', -1, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBRFS( 'U', 1, -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPBRFS( 'U', 0, 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPBRFS( 'U', 2, 1, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DPBRFS( 'U', 2, 1, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DPBRFS', INFOT, NOUT, LERR, OK )
*
*        DPBCON
*
         SRNAMT = 'DPBCON'
         INFOT = 1
         CALL DPBCON( '/', 0, 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBCON( 'U', -1, 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBCON( 'U', 1, -1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPBCON( 'U', 2, 1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'DPBCON', INFOT, NOUT, LERR, OK )
*
*        DPBEQU
*
         SRNAMT = 'DPBEQU'
         INFOT = 1
         CALL DPBEQU( '/', 0, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBEQU( 'U', -1, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBEQU( 'U', 1, -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPBEQU( 'U', 2, 1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'DPBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRPO
*
      END
