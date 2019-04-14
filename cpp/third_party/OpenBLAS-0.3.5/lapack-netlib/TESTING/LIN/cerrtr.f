*> \brief \b CERRTR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRTR( PATH, NUNIT )
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
*> CERRTR tests the error exits for the COMPLEX triangular routines.
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
      SUBROUTINE CERRTR( PATH, NUNIT )
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
      REAL               RCOND, SCALE
*     ..
*     .. Local Arrays ..
      REAL               R1( NMAX ), R2( NMAX ), RW( NMAX )
      COMPLEX            A( NMAX, NMAX ), B( NMAX ), W( NMAX ),
     $                   X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, CLATBS, CLATPS, CLATRS, CTBCON,
     $                   CTBRFS, CTBTRS, CTPCON, CTPRFS, CTPTRI, CTPTRS,
     $                   CTRCON, CTRRFS, CTRTI2, CTRTRI, CTRTRS
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
      A( 1, 1 ) = 1.
      A( 1, 2 ) = 2.
      A( 2, 2 ) = 3.
      A( 2, 1 ) = 4.
      OK = .TRUE.
*
*     Test error exits for the general triangular routines.
*
      IF( LSAMEN( 2, C2, 'TR' ) ) THEN
*
*        CTRTRI
*
         SRNAMT = 'CTRTRI'
         INFOT = 1
         CALL CTRTRI( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'CTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTRTRI( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'CTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTRTRI( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'CTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTRTRI( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'CTRTRI', INFOT, NOUT, LERR, OK )
*
*        CTRTI2
*
         SRNAMT = 'CTRTI2'
         INFOT = 1
         CALL CTRTI2( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'CTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTRTI2( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'CTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTRTI2( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'CTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTRTI2( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'CTRTI2', INFOT, NOUT, LERR, OK )
*
*
*        CTRTRS
*
         SRNAMT = 'CTRTRS'
         INFOT = 1
         CALL CTRTRS( '/', 'N', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTRTRS( 'U', '/', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTRTRS( 'U', 'N', '/', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTRTRS( 'U', 'N', 'N', -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTRTRS( 'U', 'N', 'N', 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
*
*        CTRRFS
*
         SRNAMT = 'CTRRFS'
         INFOT = 1
         CALL CTRRFS( '/', 'N', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTRRFS( 'U', '/', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTRRFS( 'U', 'N', '/', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTRRFS( 'U', 'N', 'N', -1, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTRRFS( 'U', 'N', 'N', 0, -1, A, 1, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CTRRFS( 'U', 'N', 'N', 2, 1, A, 1, B, 2, X, 2, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CTRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 1, X, 2, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CTRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 2, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTRRFS', INFOT, NOUT, LERR, OK )
*
*        CTRCON
*
         SRNAMT = 'CTRCON'
         INFOT = 1
         CALL CTRCON( '/', 'U', 'N', 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTRCON( '1', '/', 'N', 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTRCON( '1', 'U', '/', 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTRCON( '1', 'U', 'N', -1, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CTRCON( '1', 'U', 'N', 2, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTRCON', INFOT, NOUT, LERR, OK )
*
*        CLATRS
*
         SRNAMT = 'CLATRS'
         INFOT = 1
         CALL CLATRS( '/', 'N', 'N', 'N', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CLATRS( 'U', '/', 'N', 'N', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CLATRS( 'U', 'N', '/', 'N', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CLATRS( 'U', 'N', 'N', '/', 0, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CLATRS( 'U', 'N', 'N', 'N', -1, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CLATRS( 'U', 'N', 'N', 'N', 2, A, 1, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATRS', INFOT, NOUT, LERR, OK )
*
*     Test error exits for the packed triangular routines.
*
      ELSE IF( LSAMEN( 2, C2, 'TP' ) ) THEN
*
*        CTPTRI
*
         SRNAMT = 'CTPTRI'
         INFOT = 1
         CALL CTPTRI( '/', 'N', 0, A, INFO )
         CALL CHKXER( 'CTPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTPTRI( 'U', '/', 0, A, INFO )
         CALL CHKXER( 'CTPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTPTRI( 'U', 'N', -1, A, INFO )
         CALL CHKXER( 'CTPTRI', INFOT, NOUT, LERR, OK )
*
*        CTPTRS
*
         SRNAMT = 'CTPTRS'
         INFOT = 1
         CALL CTPTRS( '/', 'N', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'CTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTPTRS( 'U', '/', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'CTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTPTRS( 'U', 'N', '/', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'CTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTPTRS( 'U', 'N', 'N', -1, 0, A, X, 1, INFO )
         CALL CHKXER( 'CTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTPTRS( 'U', 'N', 'N', 0, -1, A, X, 1, INFO )
         CALL CHKXER( 'CTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CTPTRS( 'U', 'N', 'N', 2, 1, A, X, 1, INFO )
         CALL CHKXER( 'CTPTRS', INFOT, NOUT, LERR, OK )
*
*        CTPRFS
*
         SRNAMT = 'CTPRFS'
         INFOT = 1
         CALL CTPRFS( '/', 'N', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTPRFS( 'U', '/', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTPRFS( 'U', 'N', '/', 0, 0, A, B, 1, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTPRFS( 'U', 'N', 'N', -1, 0, A, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTPRFS( 'U', 'N', 'N', 0, -1, A, B, 1, X, 1, R1, R2, W,
     $                RW, INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CTPRFS( 'U', 'N', 'N', 2, 1, A, B, 1, X, 2, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CTPRFS( 'U', 'N', 'N', 2, 1, A, B, 2, X, 1, R1, R2, W, RW,
     $                INFO )
         CALL CHKXER( 'CTPRFS', INFOT, NOUT, LERR, OK )
*
*        CTPCON
*
         SRNAMT = 'CTPCON'
         INFOT = 1
         CALL CTPCON( '/', 'U', 'N', 0, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTPCON( '1', '/', 'N', 0, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTPCON( '1', 'U', '/', 0, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTPCON( '1', 'U', 'N', -1, A, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTPCON', INFOT, NOUT, LERR, OK )
*
*        CLATPS
*
         SRNAMT = 'CLATPS'
         INFOT = 1
         CALL CLATPS( '/', 'N', 'N', 'N', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CLATPS( 'U', '/', 'N', 'N', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CLATPS( 'U', 'N', '/', 'N', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CLATPS( 'U', 'N', 'N', '/', 0, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CLATPS( 'U', 'N', 'N', 'N', -1, A, X, SCALE, RW, INFO )
         CALL CHKXER( 'CLATPS', INFOT, NOUT, LERR, OK )
*
*     Test error exits for the banded triangular routines.
*
      ELSE IF( LSAMEN( 2, C2, 'TB' ) ) THEN
*
*        CTBTRS
*
         SRNAMT = 'CTBTRS'
         INFOT = 1
         CALL CTBTRS( '/', 'N', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTBTRS( 'U', '/', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTBTRS( 'U', 'N', '/', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTBTRS( 'U', 'N', 'N', -1, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTBTRS( 'U', 'N', 'N', 0, -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CTBTRS( 'U', 'N', 'N', 0, 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CTBTRS( 'U', 'N', 'N', 2, 1, 1, A, 1, X, 2, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CTBTRS( 'U', 'N', 'N', 2, 0, 1, A, 1, X, 1, INFO )
         CALL CHKXER( 'CTBTRS', INFOT, NOUT, LERR, OK )
*
*        CTBRFS
*
         SRNAMT = 'CTBRFS'
         INFOT = 1
         CALL CTBRFS( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTBRFS( 'U', '/', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTBRFS( 'U', 'N', '/', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTBRFS( 'U', 'N', 'N', -1, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTBRFS( 'U', 'N', 'N', 0, -1, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CTBRFS( 'U', 'N', 'N', 0, 0, -1, A, 1, B, 1, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 1, B, 2, X, 2, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 1, X, 2, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 2, X, 1, R1, R2,
     $                W, RW, INFO )
         CALL CHKXER( 'CTBRFS', INFOT, NOUT, LERR, OK )
*
*        CTBCON
*
         SRNAMT = 'CTBCON'
         INFOT = 1
         CALL CTBCON( '/', 'U', 'N', 0, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTBCON( '1', '/', 'N', 0, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CTBCON( '1', 'U', '/', 0, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTBCON( '1', 'U', 'N', -1, 0, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CTBCON( '1', 'U', 'N', 0, -1, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CTBCON( '1', 'U', 'N', 2, 1, A, 1, RCOND, W, RW, INFO )
         CALL CHKXER( 'CTBCON', INFOT, NOUT, LERR, OK )
*
*        CLATBS
*
         SRNAMT = 'CLATBS'
         INFOT = 1
         CALL CLATBS( '/', 'N', 'N', 'N', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CLATBS( 'U', '/', 'N', 'N', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CLATBS( 'U', 'N', '/', 'N', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CLATBS( 'U', 'N', 'N', '/', 0, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CLATBS( 'U', 'N', 'N', 'N', -1, 0, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CLATBS( 'U', 'N', 'N', 'N', 1, -1, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CLATBS( 'U', 'N', 'N', 'N', 2, 1, A, 1, X, SCALE, RW,
     $                INFO )
         CALL CHKXER( 'CLATBS', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRTR
*
      END
