*> \brief \b ZERRGT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRGT( PATH, NUNIT )
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
*> ZERRGT tests the error exits for the COMPLEX*16 tridiagonal
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZERRGT( PATH, NUNIT )
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
      INTEGER            I, INFO
      DOUBLE PRECISION   ANORM, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX )
      DOUBLE PRECISION   D( NMAX ), DF( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   RW( NMAX )
      COMPLEX*16         B( NMAX ), DL( NMAX ), DLF( NMAX ), DU( NMAX ),
     $                   DU2( NMAX ), DUF( NMAX ), E( NMAX ),
     $                   EF( NMAX ), W( NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZGTCON, ZGTRFS, ZGTTRF, ZGTTRS,
     $                   ZPTCON, ZPTRFS, ZPTTRF, ZPTTRS
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
      DO 10 I = 1, NMAX
         D( I ) = 1.D0
         E( I ) = 2.D0
         DL( I ) = 3.D0
         DU( I ) = 4.D0
   10 CONTINUE
      ANORM = 1.0D0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GT' ) ) THEN
*
*        Test error exits for the general tridiagonal routines.
*
*        ZGTTRF
*
         SRNAMT = 'ZGTTRF'
         INFOT = 1
         CALL ZGTTRF( -1, DL, E, DU, DU2, IP, INFO )
         CALL CHKXER( 'ZGTTRF', INFOT, NOUT, LERR, OK )
*
*        ZGTTRS
*
         SRNAMT = 'ZGTTRS'
         INFOT = 1
         CALL ZGTTRS( '/', 0, 0, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'ZGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGTTRS( 'N', -1, 0, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'ZGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGTTRS( 'N', 0, -1, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'ZGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGTTRS( 'N', 2, 1, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'ZGTTRS', INFOT, NOUT, LERR, OK )
*
*        ZGTRFS
*
         SRNAMT = 'ZGTRFS'
         INFOT = 1
         CALL ZGTRFS( '/', 0, 0, DL, E, DU, DLF, EF, DUF, DU2, IP, B, 1,
     $                X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'ZGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGTRFS( 'N', -1, 0, DL, E, DU, DLF, EF, DUF, DU2, IP, B,
     $                1, X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'ZGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGTRFS( 'N', 0, -1, DL, E, DU, DLF, EF, DUF, DU2, IP, B,
     $                1, X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'ZGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGTRFS( 'N', 2, 1, DL, E, DU, DLF, EF, DUF, DU2, IP, B, 1,
     $                X, 2, R1, R2, W, RW, INFO )
         CALL CHKXER( 'ZGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGTRFS( 'N', 2, 1, DL, E, DU, DLF, EF, DUF, DU2, IP, B, 2,
     $                X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'ZGTRFS', INFOT, NOUT, LERR, OK )
*
*        ZGTCON
*
         SRNAMT = 'ZGTCON'
         INFOT = 1
         CALL ZGTCON( '/', 0, DL, E, DU, DU2, IP, ANORM, RCOND, W,
     $                INFO )
         CALL CHKXER( 'ZGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGTCON( 'I', -1, DL, E, DU, DU2, IP, ANORM, RCOND, W,
     $                INFO )
         CALL CHKXER( 'ZGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGTCON( 'I', 0, DL, E, DU, DU2, IP, -ANORM, RCOND, W,
     $                INFO )
         CALL CHKXER( 'ZGTCON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PT' ) ) THEN
*
*        Test error exits for the positive definite tridiagonal
*        routines.
*
*        ZPTTRF
*
         SRNAMT = 'ZPTTRF'
         INFOT = 1
         CALL ZPTTRF( -1, D, E, INFO )
         CALL CHKXER( 'ZPTTRF', INFOT, NOUT, LERR, OK )
*
*        ZPTTRS
*
         SRNAMT = 'ZPTTRS'
         INFOT = 1
         CALL ZPTTRS( '/', 1, 0, D, E, X, 1, INFO )
         CALL CHKXER( 'ZPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPTTRS( 'U', -1, 0, D, E, X, 1, INFO )
         CALL CHKXER( 'ZPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPTTRS( 'U', 0, -1, D, E, X, 1, INFO )
         CALL CHKXER( 'ZPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZPTTRS( 'U', 2, 1, D, E, X, 1, INFO )
         CALL CHKXER( 'ZPTTRS', INFOT, NOUT, LERR, OK )
*
*        ZPTRFS
*
         SRNAMT = 'ZPTRFS'
         INFOT = 1
         CALL ZPTRFS( '/', 1, 0, D, E, DF, EF, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPTRFS( 'U', -1, 0, D, E, DF, EF, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPTRFS( 'U', 0, -1, D, E, DF, EF, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZPTRFS( 'U', 2, 1, D, E, DF, EF, B, 1, X, 2, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZPTRFS( 'U', 2, 1, D, E, DF, EF, B, 2, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZPTRFS', INFOT, NOUT, LERR, OK )
*
*        ZPTCON
*
         SRNAMT = 'ZPTCON'
         INFOT = 1
         CALL ZPTCON( -1, D, E, ANORM, RCOND, RW, INFO )
         CALL CHKXER( 'ZPTCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPTCON( 0, D, E, -ANORM, RCOND, RW, INFO )
         CALL CHKXER( 'ZPTCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRGT
*
      END
