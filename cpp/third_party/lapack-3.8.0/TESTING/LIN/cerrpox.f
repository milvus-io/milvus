*> \brief \b CERRPOX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRPO( PATH, NUNIT )
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
*> CERRPO tests the error exits for the COMPLEX routines
*> for Hermitian positive definite matrices.
*>
*> Note that this file is used only when the XBLAS are available,
*> otherwise cerrpo.f defines this subroutine.
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
      SUBROUTINE CERRPO( PATH, NUNIT )
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
      CHARACTER          EQ
      CHARACTER*2        C2
      INTEGER            I, INFO, J, N_ERR_BNDS, NPARAMS
      REAL               ANRM, RCOND, BERR
*     ..
*     .. Local Arrays ..
      REAL               S( NMAX ), R( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   ERR_BNDS_N( NMAX, 3 ), ERR_BNDS_C( NMAX, 3 ),
     $                   PARAMS( 1 )
      COMPLEX            A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( 2*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, CPBCON, CPBEQU, CPBRFS, CPBTF2,
     $                   CPBTRF, CPBTRS, CPOCON, CPOEQU, CPORFS, CPOTF2,
     $                   CPOTRF, CPOTRI, CPOTRS, CPPCON, CPPEQU, CPPRFS,
     $                   CPPTRF, CPPTRI, CPPTRS, CPOEQUB, CPORFSX
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
      INTRINSIC          CMPLX, REAL
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
            A( I, J ) = CMPLX( 1. / REAL( I+J ), -1. / REAL( I+J ) )
            AF( I, J ) = CMPLX( 1. / REAL( I+J ), -1. / REAL( I+J ) )
   10    CONTINUE
         B( J ) = 0.
         R1( J ) = 0.
         R2( J ) = 0.
         W( J ) = 0.
         X( J ) = 0.
         S( J ) = 0.
   20 CONTINUE
      ANRM = 1.
      OK = .TRUE.
*
*     Test error exits of the routines that use the Cholesky
*     decomposition of a Hermitian positive definite matrix.
*
      IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        CPOTRF
*
         SRNAMT = 'CPOTRF'
         INFOT = 1
         CALL CPOTRF( '/', 0, A, 1, INFO )
         CALL CHKXER( 'CPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPOTRF( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'CPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPOTRF( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'CPOTRF', INFOT, NOUT, LERR, OK )
*
*        CPOTF2
*
         SRNAMT = 'CPOTF2'
         INFOT = 1
         CALL CPOTF2( '/', 0, A, 1, INFO )
         CALL CHKXER( 'CPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPOTF2( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'CPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPOTF2( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'CPOTF2', INFOT, NOUT, LERR, OK )
*
*        CPOTRI
*
         SRNAMT = 'CPOTRI'
         INFOT = 1
         CALL CPOTRI( '/', 0, A, 1, INFO )
         CALL CHKXER( 'CPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPOTRI( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'CPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPOTRI( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'CPOTRI', INFOT, NOUT, LERR, OK )
*
*        CPOTRS
*
         SRNAMT = 'CPOTRS'
         INFOT = 1
         CALL CPOTRS( '/', 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPOTRS( 'U', -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPOTRS( 'U', 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPOTRS( 'U', 2, 1, A, 1, B, 2, INFO )
         CALL CHKXER( 'CPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CPOTRS( 'U', 2, 1, A, 2, B, 1, INFO )
         CALL CHKXER( 'CPOTRS', INFOT, NOUT, LERR, OK )
*
*        CPORFS
*
         SRNAMT = 'CPORFS'
         INFOT = 1
         CALL CPORFS( '/', 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPORFS( 'U', -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPORFS( 'U', 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPORFS( 'U', 2, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CPORFS( 'U', 2, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CPORFS( 'U', 2, 1, A, 2, AF, 2, B, 1, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CPORFS( 'U', 2, 1, A, 2, AF, 2, B, 2, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPORFS', INFOT, NOUT, LERR, OK )
*
*        CPORFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'CPORFSX'
         INFOT = 1
         CALL CPORFSX( '/', EQ, 0, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPORFSX( 'U', '/', -1, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         EQ = 'N'
         INFOT = 3
         CALL CPORFSX( 'U', EQ, -1, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPORFSX( 'U', EQ, 0, -1, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CPORFSX( 'U', EQ, 2, 1, A, 1, AF, 2, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CPORFSX( 'U', EQ, 2, 1, A, 2, AF, 1, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CPORFSX( 'U', EQ, 2, 1, A, 2, AF, 2, S, B, 1, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL CPORFSX( 'U', EQ, 2, 1, A, 2, AF, 2, S, B, 2, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CPORFSX', INFOT, NOUT, LERR, OK )
*
*        CPOCON
*
         SRNAMT = 'CPOCON'
         INFOT = 1
         CALL CPOCON( '/', 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPOCON( 'U', -1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPOCON( 'U', 2, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPOCON( 'U', 1, A, 1, -ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPOCON', INFOT, NOUT, LERR, OK )
*
*        CPOEQU
*
         SRNAMT = 'CPOEQU'
         INFOT = 1
         CALL CPOEQU( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPOEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPOEQU( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPOEQU', INFOT, NOUT, LERR, OK )
*
*        CPOEQUB
*
         SRNAMT = 'CPOEQUB'
         INFOT = 1
         CALL CPOEQUB( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPOEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPOEQUB( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPOEQUB', INFOT, NOUT, LERR, OK )
*
*     Test error exits of the routines that use the Cholesky
*     decomposition of a Hermitian positive definite packed matrix.
*
      ELSE IF( LSAMEN( 2, C2, 'PP' ) ) THEN
*
*        CPPTRF
*
         SRNAMT = 'CPPTRF'
         INFOT = 1
         CALL CPPTRF( '/', 0, A, INFO )
         CALL CHKXER( 'CPPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPPTRF( 'U', -1, A, INFO )
         CALL CHKXER( 'CPPTRF', INFOT, NOUT, LERR, OK )
*
*        CPPTRI
*
         SRNAMT = 'CPPTRI'
         INFOT = 1
         CALL CPPTRI( '/', 0, A, INFO )
         CALL CHKXER( 'CPPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPPTRI( 'U', -1, A, INFO )
         CALL CHKXER( 'CPPTRI', INFOT, NOUT, LERR, OK )
*
*        CPPTRS
*
         SRNAMT = 'CPPTRS'
         INFOT = 1
         CALL CPPTRS( '/', 0, 0, A, B, 1, INFO )
         CALL CHKXER( 'CPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPPTRS( 'U', -1, 0, A, B, 1, INFO )
         CALL CHKXER( 'CPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPPTRS( 'U', 0, -1, A, B, 1, INFO )
         CALL CHKXER( 'CPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CPPTRS( 'U', 2, 1, A, B, 1, INFO )
         CALL CHKXER( 'CPPTRS', INFOT, NOUT, LERR, OK )
*
*        CPPRFS
*
         SRNAMT = 'CPPRFS'
         INFOT = 1
         CALL CPPRFS( '/', 0, 0, A, AF, B, 1, X, 1, R1, R2, W, R, INFO )
         CALL CHKXER( 'CPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPPRFS( 'U', -1, 0, A, AF, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPPRFS( 'U', 0, -1, A, AF, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CPPRFS( 'U', 2, 1, A, AF, B, 1, X, 2, R1, R2, W, R, INFO )
         CALL CHKXER( 'CPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CPPRFS( 'U', 2, 1, A, AF, B, 2, X, 1, R1, R2, W, R, INFO )
         CALL CHKXER( 'CPPRFS', INFOT, NOUT, LERR, OK )
*
*        CPPCON
*
         SRNAMT = 'CPPCON'
         INFOT = 1
         CALL CPPCON( '/', 0, A, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPPCON( 'U', -1, A, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPPCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPPCON( 'U', 1, A, -ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPPCON', INFOT, NOUT, LERR, OK )
*
*        CPPEQU
*
         SRNAMT = 'CPPEQU'
         INFOT = 1
         CALL CPPEQU( '/', 0, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPPEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPPEQU( 'U', -1, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPPEQU', INFOT, NOUT, LERR, OK )
*
*     Test error exits of the routines that use the Cholesky
*     decomposition of a Hermitian positive definite band matrix.
*
      ELSE IF( LSAMEN( 2, C2, 'PB' ) ) THEN
*
*        CPBTRF
*
         SRNAMT = 'CPBTRF'
         INFOT = 1
         CALL CPBTRF( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'CPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPBTRF( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'CPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPBTRF( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'CPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPBTRF( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'CPBTRF', INFOT, NOUT, LERR, OK )
*
*        CPBTF2
*
         SRNAMT = 'CPBTF2'
         INFOT = 1
         CALL CPBTF2( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'CPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPBTF2( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'CPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPBTF2( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'CPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPBTF2( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'CPBTF2', INFOT, NOUT, LERR, OK )
*
*        CPBTRS
*
         SRNAMT = 'CPBTRS'
         INFOT = 1
         CALL CPBTRS( '/', 0, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPBTRS( 'U', -1, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPBTRS( 'U', 1, -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPBTRS( 'U', 0, 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CPBTRS( 'U', 2, 1, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CPBTRS( 'U', 2, 0, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'CPBTRS', INFOT, NOUT, LERR, OK )
*
*        CPBRFS
*
         SRNAMT = 'CPBRFS'
         INFOT = 1
         CALL CPBRFS( '/', 0, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPBRFS( 'U', -1, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPBRFS( 'U', 1, -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CPBRFS( 'U', 0, 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CPBRFS( 'U', 2, 1, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CPBRFS( 'U', 2, 1, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 1, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 2, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CPBRFS', INFOT, NOUT, LERR, OK )
*
*        CPBCON
*
         SRNAMT = 'CPBCON'
         INFOT = 1
         CALL CPBCON( '/', 0, 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPBCON( 'U', -1, 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPBCON( 'U', 1, -1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPBCON( 'U', 2, 1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CPBCON( 'U', 1, 0, A, 1, -ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CPBCON', INFOT, NOUT, LERR, OK )
*
*        CPBEQU
*
         SRNAMT = 'CPBEQU'
         INFOT = 1
         CALL CPBEQU( '/', 0, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CPBEQU( 'U', -1, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CPBEQU( 'U', 1, -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CPBEQU( 'U', 2, 1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'CPBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRPO
*
      END
