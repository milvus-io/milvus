*> \brief \b CERRGT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRGT( PATH, NUNIT )
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
*> CERRGT tests the error exits for the COMPLEX tridiagonal
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRGT( PATH, NUNIT )
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
      REAL               ANORM, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX )
      REAL               D( NMAX ), DF( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   RW( NMAX )
      COMPLEX            B( NMAX ), DL( NMAX ), DLF( NMAX ), DU( NMAX ),
     $                   DU2( NMAX ), DUF( NMAX ), E( NMAX ),
     $                   EF( NMAX ), W( NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CGTCON, CGTRFS, CGTTRF, CGTTRS, CHKXER,
     $                   CPTCON, CPTRFS, CPTTRF, CPTTRS
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
         D( I ) = 1.
         E( I ) = 2.
         DL( I ) = 3.
         DU( I ) = 4.
   10 CONTINUE
      ANORM = 1.0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GT' ) ) THEN
*
*        Test error exits for the general tridiagonal routines.
*
*        CGTTRF
*
         SRNAMT = 'CGTTRF'
         INFOT = 1
         CALL CGTTRF( -1, DL, E, DU, DU2, IP, INFO )
         CALL CHKXER( 'CGTTRF', INFOT, NOUT, LERR, OK )
*
*        CGTTRS
*
         SRNAMT = 'CGTTRS'
         INFOT = 1
         CALL CGTTRS( '/', 0, 0, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'CGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGTTRS( 'N', -1, 0, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'CGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGTTRS( 'N', 0, -1, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'CGTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CGTTRS( 'N', 2, 1, DL, E, DU, DU2, IP, X, 1, INFO )
         CALL CHKXER( 'CGTTRS', INFOT, NOUT, LERR, OK )
*
*        CGTRFS
*
         SRNAMT = 'CGTRFS'
         INFOT = 1
         CALL CGTRFS( '/', 0, 0, DL, E, DU, DLF, EF, DUF, DU2, IP, B, 1,
     $                X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'CGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGTRFS( 'N', -1, 0, DL, E, DU, DLF, EF, DUF, DU2, IP, B,
     $                1, X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'CGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGTRFS( 'N', 0, -1, DL, E, DU, DLF, EF, DUF, DU2, IP, B,
     $                1, X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'CGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL CGTRFS( 'N', 2, 1, DL, E, DU, DLF, EF, DUF, DU2, IP, B, 1,
     $                X, 2, R1, R2, W, RW, INFO )
         CALL CHKXER( 'CGTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL CGTRFS( 'N', 2, 1, DL, E, DU, DLF, EF, DUF, DU2, IP, B, 2,
     $                X, 1, R1, R2, W, RW, INFO )
         CALL CHKXER( 'CGTRFS', INFOT, NOUT, LERR, OK )
*
*        CGTCON
*
         SRNAMT = 'CGTCON'
         INFOT = 1
         CALL CGTCON( '/', 0, DL, E, DU, DU2, IP, ANORM, RCOND, W,
     $                INFO )
         CALL CHKXER( 'CGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGTCON( 'I', -1, DL, E, DU, DU2, IP, ANORM, RCOND, W,
     $                INFO )
         CALL CHKXER( 'CGTCON', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CGTCON( 'I', 0, DL, E, DU, DU2, IP, -ANORM, RCOND, W,
     $                INFO )
         CALL CHKXER( 'CGTCON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PT' ) ) THEN
*
*        Test error exits for the positive definite tridiagonal
*        routines.
*
*        CPTTRF
*
         SRNAMT = 'CPTTRF'
         INFOT = 1
         CALL CPTTRF( -1, D, E, INFO )
         CALL CHKXER( 'CPTTRF', INFOT, NOUT, LERR, OK )
*
*        CPTTRS
*
         SRNAMT = 'CPTTRS'
         INFOT = 1
         CALL CPTTRS( '/', 1, 0, D, E, X, 1, INFO )
         CALL CHKXER( 'CPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPTTRS( 'U', -1, 0, D, E, X, 1, INFO )
         CALL CHKXER( 'CPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPTTRS( 'U', 0, -1, D, E, X, 1, INFO )
         CALL CHKXER( 'CPTTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CPTTRS( 'U', 2, 1, D, E, X, 1, INFO )
         CALL CHKXER( 'CPTTRS', INFOT, NOUT, LERR, OK )
*
*        CPTRFS
*
         SRNAMT = 'CPTRFS'
         INFOT = 1
         CALL CPTRFS( '/', 1, 0, D, E, DF, EF, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPTRFS( 'U', -1, 0, D, E, DF, EF, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPTRFS( 'U', 0, -1, D, E, DF, EF, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CPTRFS( 'U', 2, 1, D, E, DF, EF, B, 1, X, 2, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CPTRFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CPTRFS( 'U', 2, 1, D, E, DF, EF, B, 2, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CPTRFS', INFOT, NOUT, LERR, OK )
*
*        CPTCON
*
         SRNAMT = 'CPTCON'
         INFOT = 1
         CALL CPTCON( -1, D, E, ANORM, RCOND, RW, INFO )
         CALL CHKXER( 'CPTCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPTCON( 0, D, E, -ANORM, RCOND, RW, INFO )
         CALL CHKXER( 'CPTCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRGT
*
      END
