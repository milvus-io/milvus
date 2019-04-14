*> \brief \b SERRTR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRTR( PATH, NUNIT )
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
*> SERRTR tests the error exits for the REAL triangular
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
      SUBROUTINE SERRTR( PATH, NUNIT )
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
      INTEGER            IW( NMAX )
      REAL               A( NMAX, NMAX ), B( NMAX ), R1( NMAX ),
     $                   R2( NMAX ), W( NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SLATBS, SLATPS, SLATRS, STBCON,
     $                   STBRFS, STBTRS, STPCON, STPRFS, STPTRI, STPTRS,
     $                   STRCON, STRRFS, STRTI2, STRTRI, STRTRS
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
      IF( LSAMEN( 2, C2, 'TR' ) ) THEN
*
*        Test error exits for the general triangular routines.
*
*        STRTRI
*
         SRNAMT = 'STRTRI'
         INFOT = 1
         CALL STRTRI( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'STRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STRTRI( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'STRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STRTRI( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'STRTRI', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STRTRI( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'STRTRI', INFOT, NOUT, LERR, OK )
*
*        STRTI2
*
         SRNAMT = 'STRTI2'
         INFOT = 1
         CALL STRTI2( '/', 'N', 0, A, 1, INFO )
         CALL CHKXER( 'STRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STRTI2( 'U', '/', 0, A, 1, INFO )
         CALL CHKXER( 'STRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STRTI2( 'U', 'N', -1, A, 1, INFO )
         CALL CHKXER( 'STRTI2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STRTI2( 'U', 'N', 2, A, 1, INFO )
         CALL CHKXER( 'STRTI2', INFOT, NOUT, LERR, OK )
*
*        STRTRS
*
         SRNAMT = 'STRTRS'
         INFOT = 1
         CALL STRTRS( '/', 'N', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STRTRS( 'U', '/', 'N', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STRTRS( 'U', 'N', '/', 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STRTRS( 'U', 'N', 'N', -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STRTRS( 'U', 'N', 'N', 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STRTRS( 'U', 'N', 'N', 2, 1, A, 1, X, 2, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL STRTRS( 'U', 'N', 'N', 2, 1, A, 2, X, 1, INFO )
         CALL CHKXER( 'STRTRS', INFOT, NOUT, LERR, OK )
*
*        STRRFS
*
         SRNAMT = 'STRRFS'
         INFOT = 1
         CALL STRRFS( '/', 'N', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STRRFS( 'U', '/', 'N', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STRRFS( 'U', 'N', '/', 0, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STRRFS( 'U', 'N', 'N', -1, 0, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STRRFS( 'U', 'N', 'N', 0, -1, A, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STRRFS( 'U', 'N', 'N', 2, 1, A, 1, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL STRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL STRRFS( 'U', 'N', 'N', 2, 1, A, 2, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STRRFS', INFOT, NOUT, LERR, OK )
*
*        STRCON
*
         SRNAMT = 'STRCON'
         INFOT = 1
         CALL STRCON( '/', 'U', 'N', 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STRCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STRCON( '1', '/', 'N', 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STRCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STRCON( '1', 'U', '/', 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STRCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STRCON( '1', 'U', 'N', -1, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STRCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STRCON( '1', 'U', 'N', 2, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STRCON', INFOT, NOUT, LERR, OK )
*
*        SLATRS
*
         SRNAMT = 'SLATRS'
         INFOT = 1
         CALL SLATRS( '/', 'N', 'N', 'N', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SLATRS( 'U', '/', 'N', 'N', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SLATRS( 'U', 'N', '/', 'N', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SLATRS( 'U', 'N', 'N', '/', 0, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SLATRS( 'U', 'N', 'N', 'N', -1, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SLATRS( 'U', 'N', 'N', 'N', 2, A, 1, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATRS', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'TP' ) ) THEN
*
*        Test error exits for the packed triangular routines.
*
*        STPTRI
*
         SRNAMT = 'STPTRI'
         INFOT = 1
         CALL STPTRI( '/', 'N', 0, A, INFO )
         CALL CHKXER( 'STPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STPTRI( 'U', '/', 0, A, INFO )
         CALL CHKXER( 'STPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STPTRI( 'U', 'N', -1, A, INFO )
         CALL CHKXER( 'STPTRI', INFOT, NOUT, LERR, OK )
*
*        STPTRS
*
         SRNAMT = 'STPTRS'
         INFOT = 1
         CALL STPTRS( '/', 'N', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'STPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STPTRS( 'U', '/', 'N', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'STPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STPTRS( 'U', 'N', '/', 0, 0, A, X, 1, INFO )
         CALL CHKXER( 'STPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STPTRS( 'U', 'N', 'N', -1, 0, A, X, 1, INFO )
         CALL CHKXER( 'STPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STPTRS( 'U', 'N', 'N', 0, -1, A, X, 1, INFO )
         CALL CHKXER( 'STPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STPTRS( 'U', 'N', 'N', 2, 1, A, X, 1, INFO )
         CALL CHKXER( 'STPTRS', INFOT, NOUT, LERR, OK )
*
*        STPRFS
*
         SRNAMT = 'STPRFS'
         INFOT = 1
         CALL STPRFS( '/', 'N', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STPRFS( 'U', '/', 'N', 0, 0, A, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STPRFS( 'U', 'N', '/', 0, 0, A, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STPRFS( 'U', 'N', 'N', -1, 0, A, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STPRFS( 'U', 'N', 'N', 0, -1, A, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STPRFS( 'U', 'N', 'N', 2, 1, A, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STPRFS( 'U', 'N', 'N', 2, 1, A, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'STPRFS', INFOT, NOUT, LERR, OK )
*
*        STPCON
*
         SRNAMT = 'STPCON'
         INFOT = 1
         CALL STPCON( '/', 'U', 'N', 0, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'STPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STPCON( '1', '/', 'N', 0, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'STPCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STPCON( '1', 'U', '/', 0, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'STPCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STPCON( '1', 'U', 'N', -1, A, RCOND, W, IW, INFO )
         CALL CHKXER( 'STPCON', INFOT, NOUT, LERR, OK )
*
*        SLATPS
*
         SRNAMT = 'SLATPS'
         INFOT = 1
         CALL SLATPS( '/', 'N', 'N', 'N', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SLATPS( 'U', '/', 'N', 'N', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SLATPS( 'U', 'N', '/', 'N', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SLATPS( 'U', 'N', 'N', '/', 0, A, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATPS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SLATPS( 'U', 'N', 'N', 'N', -1, A, X, SCALE, W, INFO )
         CALL CHKXER( 'SLATPS', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'TB' ) ) THEN
*
*        Test error exits for the banded triangular routines.
*
*        STBTRS
*
         SRNAMT = 'STBTRS'
         INFOT = 1
         CALL STBTRS( '/', 'N', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STBTRS( 'U', '/', 'N', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STBTRS( 'U', 'N', '/', 0, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STBTRS( 'U', 'N', 'N', -1, 0, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STBTRS( 'U', 'N', 'N', 0, -1, 0, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STBTRS( 'U', 'N', 'N', 0, 0, -1, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STBTRS( 'U', 'N', 'N', 2, 1, 1, A, 1, X, 2, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STBTRS( 'U', 'N', 'N', 2, 0, 1, A, 1, X, 1, INFO )
         CALL CHKXER( 'STBTRS', INFOT, NOUT, LERR, OK )
*
*        STBRFS
*
         SRNAMT = 'STBRFS'
         INFOT = 1
         CALL STBRFS( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STBRFS( 'U', '/', 'N', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STBRFS( 'U', 'N', '/', 0, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STBRFS( 'U', 'N', 'N', -1, 0, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STBRFS( 'U', 'N', 'N', 0, -1, 0, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STBRFS( 'U', 'N', 'N', 0, 0, -1, A, 1, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STBRFS( 'U', 'N', 'N', 2, 1, 1, A, 1, B, 2, X, 2, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 1, X, 2, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL STBRFS( 'U', 'N', 'N', 2, 1, 1, A, 2, B, 2, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'STBRFS', INFOT, NOUT, LERR, OK )
*
*        STBCON
*
         SRNAMT = 'STBCON'
         INFOT = 1
         CALL STBCON( '/', 'U', 'N', 0, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STBCON( '1', '/', 'N', 0, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STBCON( '1', 'U', '/', 0, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STBCON( '1', 'U', 'N', -1, 0, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STBCON( '1', 'U', 'N', 0, -1, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STBCON', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STBCON( '1', 'U', 'N', 2, 1, A, 1, RCOND, W, IW, INFO )
         CALL CHKXER( 'STBCON', INFOT, NOUT, LERR, OK )
*
*        SLATBS
*
         SRNAMT = 'SLATBS'
         INFOT = 1
         CALL SLATBS( '/', 'N', 'N', 'N', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SLATBS( 'U', '/', 'N', 'N', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SLATBS( 'U', 'N', '/', 'N', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SLATBS( 'U', 'N', 'N', '/', 0, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SLATBS( 'U', 'N', 'N', 'N', -1, 0, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SLATBS( 'U', 'N', 'N', 'N', 1, -1, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SLATBS( 'U', 'N', 'N', 'N', 2, 1, A, 1, X, SCALE, W,
     $                INFO )
         CALL CHKXER( 'SLATBS', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRTR
*
      END
