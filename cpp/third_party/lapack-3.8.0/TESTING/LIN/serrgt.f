*> \brief \b SERRGT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRGT( PATH, NUNIT )
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
*> SERRGT tests the error exits for the REAL tridiagonal
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRGT( PATH, NUNIT )
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
      REAL               ANORM, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      REAL               B( NMAX ), C( NMAX ), CF( NMAX ), D( NMAX ),
     $                   DF( NMAX ), E( NMAX ), EF( NMAX ), F( NMAX ),
     $                   R1( NMAX ), R2( NMAX ), W( NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SGTCON, SGTRFS, SGTTRF, SGTTRS,
     $                   SPTCON, SPTRFS, SPTTRF, SPTTRS
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
      D( 1 ) = 1.
      D( 2 ) = 2.
      DF( 1 ) = 1.
      DF( 2 ) = 2.
      E( 1 ) = 3.
      E( 2 ) = 4.
      EF( 1 ) = 3.
      EF( 2 ) = 4.
      ANORM = 1.0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GT' ) ) THEN
*
*        Test error exits for the general tridiagonal routines.
*
*        SGTTRF
*
         SRNAMT = 'SGTTRF'
         INFOT = 1
         CALL SGTTRF( -1, C, D, E, F, IP, INFO )
         CALL CHKXER( 'SGTTRF', INFOT, NOUT, LERR, OK )
*
*        SGTTRS
*
         SRNAMT = 'SGTTRS'
         INFOT = 1
         CALL SGTTRS( '/', 0, 0, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'SGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGTTRS( 'N', -1, 0, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'SGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGTTRS( 'N', 0, -1, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'SGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGTTRS( 'N', 2, 1, C, D, E, F, IP, X, 1, INFO )
         CALL CHKXER( 'SGTTRS', INFOT, NOUT, LERR, OK )
*
*        SGTRFS
*
         SRNAMT = 'SGTRFS'
         INFOT = 1
         CALL SGTRFS( '/', 0, 0, C, D, E, CF, DF, EF, F, IP, B, 1, X, 1,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGTRFS( 'N', -1, 0, C, D, E, CF, DF, EF, F, IP, B, 1, X,
     $                1, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGTRFS( 'N', 0, -1, C, D, E, CF, DF, EF, F, IP, B, 1, X,
     $                1, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGTRFS( 'N', 2, 1, C, D, E, CF, DF, EF, F, IP, B, 1, X, 2,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGTRFS( 'N', 2, 1, C, D, E, CF, DF, EF, F, IP, B, 2, X, 1,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTRFS', INFOT, NOUT, LERR, OK )
*
*        SGTCON
*
         SRNAMT = 'SGTCON'
         INFOT = 1
         CALL SGTCON( '/', 0, C, D, E, F, IP, ANORM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'SGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGTCON( 'I', -1, C, D, E, F, IP, ANORM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'SGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGTCON( 'I', 0, C, D, E, F, IP, -ANORM, RCOND, W, IW,
     $                INFO )
         CALL CHKXER( 'SGTCON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PT' ) ) THEN
*
*        Test error exits for the positive definite tridiagonal
*        routines.
*
*        SPTTRF
*
         SRNAMT = 'SPTTRF'
         INFOT = 1
         CALL SPTTRF( -1, D, E, INFO )
         CALL CHKXER( 'SPTTRF', INFOT, NOUT, LERR, OK )
*
*        SPTTRS
*
         SRNAMT = 'SPTTRS'
         INFOT = 1
         CALL SPTTRS( -1, 0, D, E, X, 1, INFO )
         CALL CHKXER( 'SPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPTTRS( 0, -1, D, E, X, 1, INFO )
         CALL CHKXER( 'SPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPTTRS( 2, 1, D, E, X, 1, INFO )
         CALL CHKXER( 'SPTTRS', INFOT, NOUT, LERR, OK )
*
*        SPTRFS
*
         SRNAMT = 'SPTRFS'
         INFOT = 1
         CALL SPTRFS( -1, 0, D, E, DF, EF, B, 1, X, 1, R1, R2, W, INFO )
         CALL CHKXER( 'SPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPTRFS( 0, -1, D, E, DF, EF, B, 1, X, 1, R1, R2, W, INFO )
         CALL CHKXER( 'SPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SPTRFS( 2, 1, D, E, DF, EF, B, 1, X, 2, R1, R2, W, INFO )
         CALL CHKXER( 'SPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SPTRFS( 2, 1, D, E, DF, EF, B, 2, X, 1, R1, R2, W, INFO )
         CALL CHKXER( 'SPTRFS', INFOT, NOUT, LERR, OK )
*
*        SPTCON
*
         SRNAMT = 'SPTCON'
         INFOT = 1
         CALL SPTCON( -1, D, E, ANORM, RCOND, W, INFO )
         CALL CHKXER( 'SPTCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPTCON( 0, D, E, -ANORM, RCOND, W, INFO )
         CALL CHKXER( 'SPTCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRGT
*
      END
