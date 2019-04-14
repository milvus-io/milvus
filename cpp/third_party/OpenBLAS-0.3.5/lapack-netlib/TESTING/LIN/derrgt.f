*> \brief \b DERRGT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRGT( PATH, NUNIT )
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
*> DERRGT tests the error exits for the DOUBLE PRECISION tridiagonal
*> routines.
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
      SUBROUTINE DERRGT( PATH, NUNIT )
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
      CHARACTER*2        C2
      INTEGER            INFO
      DOUBLE PRECISION   ANORM, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      DOUBLE PRECISION   B( NMAX ), C( NMAX ), CF( NMAX ), D( NMAX ),
     $                   DF( NMAX ), E( NMAX ), EF( NMAX ), F( NMAX ),
     $                   R1( NMAX ), R2( NMAX ), W( NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DGTCON, DGTRFS, DGTTRF, DGTTRS,
     $                   DPTCON, DPTRFS, DPTTRF, DPTTRS
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
      WRITE( NOUT, FMT = * )
      C2 = PATH( 2: 3 )
      D( 1 ) = 1.D0
      D( 2 ) = 2.D0
      DF( 1 ) = 1.D0
      DF( 2 ) = 2.D0
      E( 1 ) = 3.D0
      E( 2 ) = 4.D0
      EF( 1 ) = 3.D0
      EF( 2 ) = 4.D0
      ANORM = 1.0D0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GT' ) ) THEN
*
*        Test error exits for the general tridiagonal routines.
*
*        DGTTRF
*
         SRNAMT = 'DGTTRF'
         INFOT = 1
         CALL DGTTRF( -1, C, D, E, F, IP, INFO )
         CALL CHKXER( 'DGTTRF', INFOT, NOUT, LERR, OK )
*
*        DGTTRS
*
         SRNAMT = 'DGTTRS'
         INFOT = 1
         CALL DGTTRS( '/', 0, 0, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'DGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGTTRS( 'N', -1, 0, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'DGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGTTRS( 'N', 0, -1, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'DGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGTTRS( 'N', 2, 1, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'DGTTRS', INFOT, NOUT, LERR, OK )
*
*        DGTRFS
*
         SRNAMT = 'DGTRFS'
         INFOT = 1
         CALL DGTRFS( '/', 0, 0, C, D, E, CF, DF, EF, F, IP, B, 1, X, 1,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGTRFS( 'N', -1, 0, C, D, E, CF, DF, EF, F, IP, B, 1, X,
     $                1, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGTRFS( 'N', 0, -1, C, D, E, CF, DF, EF, F, IP, B, 1, X,
     $                1, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGTRFS( 'N', 2, 1, C, D, E, CF, DF, EF, F, IP, B, 1, X, 2,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGTRFS( 'N', 2, 1, C, D, E, CF, DF, EF, F, IP, B, 2, X, 1,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTRFS', INFOT, NOUT, LERR, OK )
*
*        DGTCON
*
         SRNAMT = 'DGTCON'
         INFOT = 1
         CALL DGTCON( '/', 0, C, D, E, F, IP, ANORM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'DGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGTCON( 'I', -1, C, D, E, F, IP, ANORM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'DGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGTCON( 'I', 0, C, D, E, F, IP, -ANORM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'DGTCON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PT' ) ) THEN
*
*        Test error exits for the positive definite tridiagonal
*        routines.
*
*        DPTTRF
*
         SRNAMT = 'DPTTRF'
         INFOT = 1
         CALL DPTTRF( -1, D, E, INFO )
         CALL CHKXER( 'DPTTRF', INFOT, NOUT, LERR, OK )
*
*        DPTTRS
*
         SRNAMT = 'DPTTRS'
         INFOT = 1
         CALL DPTTRS( -1, 0, D, E, X, 1, INFO )
         CALL CHKXER( 'DPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPTTRS( 0, -1, D, E, X, 1, INFO )
         CALL CHKXER( 'DPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPTTRS( 2, 1, D, E, X, 1, INFO )
         CALL CHKXER( 'DPTTRS', INFOT, NOUT, LERR, OK )
*
*        DPTRFS
*
         SRNAMT = 'DPTRFS'
         INFOT = 1
         CALL DPTRFS( -1, 0, D, E, DF, EF, B, 1, X, 1, R1, R2, W, INFO )
         CALL CHKXER( 'DPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPTRFS( 0, -1, D, E, DF, EF, B, 1, X, 1, R1, R2, W, INFO )
         CALL CHKXER( 'DPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DPTRFS( 2, 1, D, E, DF, EF, B, 1, X, 2, R1, R2, W, INFO )
         CALL CHKXER( 'DPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DPTRFS( 2, 1, D, E, DF, EF, B, 2, X, 1, R1, R2, W, INFO )
         CALL CHKXER( 'DPTRFS', INFOT, NOUT, LERR, OK )
*
*        DPTCON
*
         SRNAMT = 'DPTCON'
         INFOT = 1
         CALL DPTCON( -1, D, E, ANORM, RCOND, W, INFO )
         CALL CHKXER( 'DPTCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPTCON( 0, D, E, -ANORM, RCOND, W, INFO )
         CALL CHKXER( 'DPTCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRGT
*
      END
