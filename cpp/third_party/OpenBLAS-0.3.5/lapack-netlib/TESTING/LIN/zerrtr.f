*> \brief \b ZERRTR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRTR( PATH, NUNIT )
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
*> ZERRTR tests the error exits for the COMPLEX*16 triangular routines.
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
      SUBROUTINE ZERRTR( PATH, NUNIT )
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
      DOUBLE PRECISION   RCOND, SCALE
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   R1( NMAX ), R2( NMAX ), RW( NMAX )
      COMPLEX*16         A( NMAX, NMAX ), B( NMAX ), W( NMAX ),
     $                   X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZLATBS, ZLATPS, ZLATRS, ZTBCON,
     $                   ZTBRFS, ZTBTRS, ZTPCON, ZTPRFS, ZTPTRI, ZTPTRS,
     $                   ZTRCON, ZTRRFS, ZTRTI2, ZTRTRI, ZTRTRS
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
      A( 1, 1 ) = 1.D0
      A( 1, 2 ) = 2.D0
      A( 2, 2 ) = 3.D0
      A( 2, 1 ) = 4.D0
      OK = .TRUE.
*
*     Test error exits for the general triangular routines.
*
      IF( LSAMEN( 2, C2, 'TR' ) ) THEN
*
*        ZTRTRI
*
         SRNAMT = 'ZTRTRI'
         INFOT = 1
         CALL ZTRTRI( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'ZTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTRTRI( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'ZTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTRTRI( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'ZTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTRTRI( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'ZTRTRI', INFOT, NOUT, LERR, OK )
*
*        ZTRTI2
*
         SRNAMT = 'ZTRTI2'
         INFOT = 1
         CALL ZTRTI2( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'ZTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTRTI2( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'ZTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTRTI2( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'ZTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTRTI2( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'ZTRTI2', INFOT, NOUT, LERR, OK )
*
*
*        ZTRTRS
*
         SRNAMT = 'ZTRTRS'
         INFOT = 1
         CALL ZTRTRS( '/', 'N', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTRTRS( 'U', '/', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTRTRS( 'U', 'N', '/', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTRTRS( 'U', 'N', 'N', -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTRTRS( 'U', 'N', 'N', 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
*
*        ZTRRFS
*
         SRNAMT = 'ZTRRFS'
         INFOT = 1
         CALL ZTRRFS( '/', 'N', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTRRFS( 'U', '/', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTRRFS( 'U', 'N', '/', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTRRFS( 'U', 'N', 'N', -1, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTRRFS( 'U', 'N', 'N', 0, -1, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZTRRFS( 'U', 'N', 'N', 2, 1, A, 1, B, 2, X, 2, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZTRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 1, X, 2, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZTRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 2, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTRRFS', INFOT, NOUT, LERR, OK )
*
*        ZTRCON
*
         SRNAMT = 'ZTRCON'
         INFOT = 1
         CALL ZTRCON( '/', 'U', 'N', 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTRCON( '1', '/', 'N', 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTRCON( '1', 'U', '/', 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTRCON( '1', 'U', 'N', -1, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTRCON( '1', 'U', 'N', 2, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTRCON', INFOT, NOUT, LERR, OK )
*
*        ZLATRS
*
         SRNAMT = 'ZLATRS'
         INFOT = 1
         CALL ZLATRS( '/', 'N', 'N', 'N', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZLATRS( 'U', '/', 'N', 'N', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZLATRS( 'U', 'N', '/', 'N', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZLATRS( 'U', 'N', 'N', '/', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZLATRS( 'U', 'N', 'N', 'N', -1, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZLATRS( 'U', 'N', 'N', 'N', 2, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATRS', INFOT, NOUT, LERR, OK )
*
*     Test error exits for the packed triangular routines.
*
      ELSE IF( LSAMEN( 2, C2, 'TP' ) ) THEN
*
*        ZTPTRI
*
         SRNAMT = 'ZTPTRI'
         INFOT = 1
         CALL ZTPTRI( '/', 'N', 0, A, INFO )
         CALL CHKXER( 'ZTPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTPTRI( 'U', '/', 0, A, INFO )
         CALL CHKXER( 'ZTPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTPTRI( 'U', 'N', -1, A, INFO )
         CALL CHKXER( 'ZTPTRI', INFOT, NOUT, LERR, OK )
*
*        ZTPTRS
*
         SRNAMT = 'ZTPTRS'
         INFOT = 1
         CALL ZTPTRS( '/', 'N', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'ZTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTPTRS( 'U', '/', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'ZTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTPTRS( 'U', 'N', '/', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'ZTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTPTRS( 'U', 'N', 'N', -1, 0, A, X, 1, INFO )
         CALL CHKXER( 'ZTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTPTRS( 'U', 'N', 'N', 0, -1, A, X, 1, INFO )
         CALL CHKXER( 'ZTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTPTRS( 'U', 'N', 'N', 2, 1, A, X, 1, INFO )
         CALL CHKXER( 'ZTPTRS', INFOT, NOUT, LERR, OK )
*
*        ZTPRFS
*
         SRNAMT = 'ZTPRFS'
         INFOT = 1
         CALL ZTPRFS( '/', 'N', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTPRFS( 'U', '/', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTPRFS( 'U', 'N', '/', 0, 0, A, B, 1, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTPRFS( 'U', 'N', 'N', -1, 0, A, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTPRFS( 'U', 'N', 'N', 0, -1, A, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTPRFS( 'U', 'N', 'N', 2, 1, A, B, 1, X, 2, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTPRFS( 'U', 'N', 'N', 2, 1, A, B, 2, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTPRFS', INFOT, NOUT, LERR, OK )
*
*        ZTPCON
*
         SRNAMT = 'ZTPCON'
         INFOT = 1
         CALL ZTPCON( '/', 'U', 'N', 0, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTPCON( '1', '/', 'N', 0, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTPCON( '1', 'U', '/', 0, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTPCON( '1', 'U', 'N', -1, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTPCON', INFOT, NOUT, LERR, OK )
*
*        ZLATPS
*
         SRNAMT = 'ZLATPS'
         INFOT = 1
         CALL ZLATPS( '/', 'N', 'N', 'N', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZLATPS( 'U', '/', 'N', 'N', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZLATPS( 'U', 'N', '/', 'N', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZLATPS( 'U', 'N', 'N', '/', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZLATPS( 'U', 'N', 'N', 'N', -1, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'ZLATPS', INFOT, NOUT, LERR, OK )
*
*     Test error exits for the banded triangular routines.
*
      ELSE IF( LSAMEN( 2, C2, 'TB' ) ) THEN
*
*        ZTBTRS
*
         SRNAMT = 'ZTBTRS'
         INFOT = 1
         CALL ZTBTRS( '/', 'N', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTBTRS( 'U', '/', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTBTRS( 'U', 'N', '/', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTBTRS( 'U', 'N', 'N', -1, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTBTRS( 'U', 'N', 'N', 0, -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTBTRS( 'U', 'N', 'N', 0, 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTBTRS( 'U', 'N', 'N', 2, 1, 1, A, 1, X, 2, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTBTRS( 'U', 'N', 'N', 2, 0, 1, A, 1, X, 1, INFO )
         CALL CHKXER( 'ZTBTRS', INFOT, NOUT, LERR, OK )
*
*        ZTBRFS
*
         SRNAMT = 'ZTBRFS'
         INFOT = 1
         CALL ZTBRFS( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTBRFS( 'U', '/', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTBRFS( 'U', 'N', '/', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTBRFS( 'U', 'N', 'N', -1, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTBRFS( 'U', 'N', 'N', 0, -1, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTBRFS( 'U', 'N', 'N', 0, 0, -1, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 1, B, 2, X, 2, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 1, X, 2, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 2, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTBRFS', INFOT, NOUT, LERR, OK )
*
*        ZTBCON
*
         SRNAMT = 'ZTBCON'
         INFOT = 1
         CALL ZTBCON( '/', 'U', 'N', 0, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTBCON( '1', '/', 'N', 0, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTBCON( '1', 'U', '/', 0, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTBCON( '1', 'U', 'N', -1, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTBCON( '1', 'U', 'N', 0, -1, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZTBCON( '1', 'U', 'N', 2, 1, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'ZTBCON', INFOT, NOUT, LERR, OK )
*
*        ZLATBS
*
         SRNAMT = 'ZLATBS'
         INFOT = 1
         CALL ZLATBS( '/', 'N', 'N', 'N', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZLATBS( 'U', '/', 'N', 'N', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZLATBS( 'U', 'N', '/', 'N', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZLATBS( 'U', 'N', 'N', '/', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZLATBS( 'U', 'N', 'N', 'N', -1, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZLATBS( 'U', 'N', 'N', 'N', 1, -1, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZLATBS( 'U', 'N', 'N', 'N', 2, 1, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'ZLATBS', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRTR
*
      END
