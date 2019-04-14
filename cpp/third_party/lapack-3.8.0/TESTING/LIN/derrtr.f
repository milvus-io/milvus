*> \brief \b DERRTR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRTR( PATH, NUNIT )
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
*> DERRTR tests the error exits for the DOUBLE PRECISION triangular
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
      SUBROUTINE DERRTR( PATH, NUNIT )
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
      INTEGER            IW( NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), B( NMAX ), R1( NMAX ),
     $                   R2( NMAX ), W( NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DLATBS, DLATPS, DLATRS, DTBCON,
     $                   DTBRFS, DTBTRS, DTPCON, DTPRFS, DTPTRI, DTPTRS,
     $                   DTRCON, DTRRFS, DTRTI2, DTRTRI, DTRTRS
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
      IF( LSAMEN( 2, C2, 'TR' ) ) THEN
*
*        Test error exits for the general triangular routines.
*
*        DTRTRI
*
         SRNAMT = 'DTRTRI'
         INFOT = 1
         CALL DTRTRI( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'DTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTRTRI( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'DTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTRTRI( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'DTRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTRTRI( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'DTRTRI', INFOT, NOUT, LERR, OK )
*
*        DTRTI2
*
         SRNAMT = 'DTRTI2'
         INFOT = 1
         CALL DTRTI2( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'DTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTRTI2( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'DTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTRTI2( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'DTRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTRTI2( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'DTRTI2', INFOT, NOUT, LERR, OK )
*
*        DTRTRS
*
         SRNAMT = 'DTRTRS'
         INFOT = 1
         CALL DTRTRS( '/', 'N', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTRTRS( 'U', '/', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTRTRS( 'U', 'N', '/', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTRTRS( 'U', 'N', 'N', -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTRTRS( 'U', 'N', 'N', 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DTRTRS( 'U', 'N', 'N', 2, 1, A, 1, X, 2, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DTRTRS( 'U', 'N', 'N', 2, 1, A, 2, X, 1, INFO )
         CALL CHKXER( 'DTRTRS', INFOT, NOUT, LERR, OK )
*
*        DTRRFS
*
         SRNAMT = 'DTRRFS'
         INFOT = 1
         CALL DTRRFS( '/', 'N', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTRRFS( 'U', '/', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTRRFS( 'U', 'N', '/', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTRRFS( 'U', 'N', 'N', -1, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTRRFS( 'U', 'N', 'N', 0, -1, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DTRRFS( 'U', 'N', 'N', 2, 1, A, 1, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DTRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DTRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTRRFS', INFOT, NOUT, LERR, OK )
*
*        DTRCON
*
         SRNAMT = 'DTRCON'
         INFOT = 1
         CALL DTRCON( '/', 'U', 'N', 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTRCON( '1', '/', 'N', 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTRCON( '1', 'U', '/', 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTRCON( '1', 'U', 'N', -1, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTRCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTRCON( '1', 'U', 'N', 2, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTRCON', INFOT, NOUT, LERR, OK )
*
*        DLATRS
*
         SRNAMT = 'DLATRS'
         INFOT = 1
         CALL DLATRS( '/', 'N', 'N', 'N', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DLATRS( 'U', '/', 'N', 'N', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DLATRS( 'U', 'N', '/', 'N', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DLATRS( 'U', 'N', 'N', '/', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DLATRS( 'U', 'N', 'N', 'N', -1, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DLATRS( 'U', 'N', 'N', 'N', 2, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATRS', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'TP' ) ) THEN
*
*        Test error exits for the packed triangular routines.
*
*        DTPTRI
*
         SRNAMT = 'DTPTRI'
         INFOT = 1
         CALL DTPTRI( '/', 'N', 0, A, INFO )
         CALL CHKXER( 'DTPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTPTRI( 'U', '/', 0, A, INFO )
         CALL CHKXER( 'DTPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTPTRI( 'U', 'N', -1, A, INFO )
         CALL CHKXER( 'DTPTRI', INFOT, NOUT, LERR, OK )
*
*        DTPTRS
*
         SRNAMT = 'DTPTRS'
         INFOT = 1
         CALL DTPTRS( '/', 'N', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'DTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTPTRS( 'U', '/', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'DTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTPTRS( 'U', 'N', '/', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'DTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTPTRS( 'U', 'N', 'N', -1, 0, A, X, 1, INFO )
         CALL CHKXER( 'DTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTPTRS( 'U', 'N', 'N', 0, -1, A, X, 1, INFO )
         CALL CHKXER( 'DTPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTPTRS( 'U', 'N', 'N', 2, 1, A, X, 1, INFO )
         CALL CHKXER( 'DTPTRS', INFOT, NOUT, LERR, OK )
*
*        DTPRFS
*
         SRNAMT = 'DTPRFS'
         INFOT = 1
         CALL DTPRFS( '/', 'N', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTPRFS( 'U', '/', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTPRFS( 'U', 'N', '/', 0, 0, A, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTPRFS( 'U', 'N', 'N', -1, 0, A, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTPRFS( 'U', 'N', 'N', 0, -1, A, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTPRFS( 'U', 'N', 'N', 2, 1, A, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTPRFS( 'U', 'N', 'N', 2, 1, A, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'DTPRFS', INFOT, NOUT, LERR, OK )
*
*        DTPCON
*
         SRNAMT = 'DTPCON'
         INFOT = 1
         CALL DTPCON( '/', 'U', 'N', 0, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTPCON( '1', '/', 'N', 0, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTPCON( '1', 'U', '/', 0, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTPCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTPCON( '1', 'U', 'N', -1, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTPCON', INFOT, NOUT, LERR, OK )
*
*        DLATPS
*
         SRNAMT = 'DLATPS'
         INFOT = 1
         CALL DLATPS( '/', 'N', 'N', 'N', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DLATPS( 'U', '/', 'N', 'N', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DLATPS( 'U', 'N', '/', 'N', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DLATPS( 'U', 'N', 'N', '/', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DLATPS( 'U', 'N', 'N', 'N', -1, A, X, SCALE, W, INFO )
         CALL CHKXER( 'DLATPS', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'TB' ) ) THEN
*
*        Test error exits for the banded triangular routines.
*
*        DTBTRS
*
         SRNAMT = 'DTBTRS'
         INFOT = 1
         CALL DTBTRS( '/', 'N', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTBTRS( 'U', '/', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTBTRS( 'U', 'N', '/', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTBTRS( 'U', 'N', 'N', -1, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTBTRS( 'U', 'N', 'N', 0, -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTBTRS( 'U', 'N', 'N', 0, 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTBTRS( 'U', 'N', 'N', 2, 1, 1, A, 1, X, 2, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTBTRS( 'U', 'N', 'N', 2, 0, 1, A, 1, X, 1, INFO )
         CALL CHKXER( 'DTBTRS', INFOT, NOUT, LERR, OK )
*
*        DTBRFS
*
         SRNAMT = 'DTBRFS'
         INFOT = 1
         CALL DTBRFS( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTBRFS( 'U', '/', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTBRFS( 'U', 'N', '/', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTBRFS( 'U', 'N', 'N', -1, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTBRFS( 'U', 'N', 'N', 0, -1, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTBRFS( 'U', 'N', 'N', 0, 0, -1, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 1, B, 2, X, 2, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 1, X, 2, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DTBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 2, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'DTBRFS', INFOT, NOUT, LERR, OK )
*
*        DTBCON
*
         SRNAMT = 'DTBCON'
         INFOT = 1
         CALL DTBCON( '/', 'U', 'N', 0, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTBCON( '1', '/', 'N', 0, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTBCON( '1', 'U', '/', 0, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTBCON( '1', 'U', 'N', -1, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTBCON( '1', 'U', 'N', 0, -1, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTBCON', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DTBCON( '1', 'U', 'N', 2, 1, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'DTBCON', INFOT, NOUT, LERR, OK )
*
*        DLATBS
*
         SRNAMT = 'DLATBS'
         INFOT = 1
         CALL DLATBS( '/', 'N', 'N', 'N', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DLATBS( 'U', '/', 'N', 'N', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DLATBS( 'U', 'N', '/', 'N', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DLATBS( 'U', 'N', 'N', '/', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DLATBS( 'U', 'N', 'N', 'N', -1, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DLATBS( 'U', 'N', 'N', 'N', 1, -1, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DLATBS( 'U', 'N', 'N', 'N', 2, 1, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'DLATBS', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRTR
*
      END
