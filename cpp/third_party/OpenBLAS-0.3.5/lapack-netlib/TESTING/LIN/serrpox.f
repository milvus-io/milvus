*> \brief \b SERRPOX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRPO( PATH, NUNIT )
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
*> SERRPO tests the error exits for the REAL routines
*> for symmetric positive definite matrices.
*>
*> Note that this file is used only when the XBLAS are available,
*> otherwise serrpo.f defines this subroutine.
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
      SUBROUTINE SERRPO( PATH, NUNIT )
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
      INTEGER            IW( NMAX )
      REAL               A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   R1( NMAX ), R2( NMAX ), W( 3*NMAX ), X( NMAX ),
     $                   S( NMAX ), ERR_BNDS_N( NMAX, 3 ),
     $                   ERR_BNDS_C( NMAX, 3 ), PARAMS( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SPBCON, SPBEQU, SPBRFS, SPBTF2,
     $                   SPBTRF, SPBTRS, SPOCON, SPOEQU, SPORFS, SPOTF2,
     $                   SPOTRF, SPOTRI, SPOTRS, SPPCON, SPPEQU, SPPRFS,
     $                   SPPTRF, SPPTRI, SPPTRS, SPOEQUB, SPORFSX
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
      INTRINSIC          REAL
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
            A( I, J ) = 1. / REAL( I+J )
            AF( I, J ) = 1. / REAL( I+J )
   10    CONTINUE
         B( J ) = 0.
         R1( J ) = 0.
         R2( J ) = 0.
         W( J ) = 0.
         X( J ) = 0.
         S( J ) = 0.
         IW( J ) = J
   20 CONTINUE
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive definite matrix.
*
*        SPOTRF
*
         SRNAMT = 'SPOTRF'
         INFOT = 1
         CALL SPOTRF( '/', 0, A, 1, INFO )
         CALL CHKXER( 'SPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOTRF( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'SPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPOTRF( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'SPOTRF', INFOT, NOUT, LERR, OK )
*
*        SPOTF2
*
         SRNAMT = 'SPOTF2'
         INFOT = 1
         CALL SPOTF2( '/', 0, A, 1, INFO )
         CALL CHKXER( 'SPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOTF2( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'SPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPOTF2( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'SPOTF2', INFOT, NOUT, LERR, OK )
*
*        SPOTRI
*
         SRNAMT = 'SPOTRI'
         INFOT = 1
         CALL SPOTRI( '/', 0, A, 1, INFO )
         CALL CHKXER( 'SPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOTRI( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'SPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPOTRI( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'SPOTRI', INFOT, NOUT, LERR, OK )
*
*        SPOTRS
*
         SRNAMT = 'SPOTRS'
         INFOT = 1
         CALL SPOTRS( '/', 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOTRS( 'U', -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPOTRS( 'U', 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPOTRS( 'U', 2, 1, A, 1, B, 2, INFO )
         CALL CHKXER( 'SPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SPOTRS( 'U', 2, 1, A, 2, B, 1, INFO )
         CALL CHKXER( 'SPOTRS', INFOT, NOUT, LERR, OK )
*
*        SPORFS
*
         SRNAMT = 'SPORFS'
         INFOT = 1
         CALL SPORFS( '/', 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPORFS( 'U', -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPORFS( 'U', 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPORFS( 'U', 2, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SPORFS( 'U', 2, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SPORFS( 'U', 2, 1, A, 2, AF, 2, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SPORFS( 'U', 2, 1, A, 2, AF, 2, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPORFS', INFOT, NOUT, LERR, OK )
*
*        SPORFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'SPORFSX'
         INFOT = 1
         CALL SPORFSX( '/', EQ, 0, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPORFSX( 'U', "/", -1, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         EQ = 'N'
         INFOT = 3
         CALL SPORFSX( 'U', EQ, -1, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPORFSX( 'U', EQ, 0, -1, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPORFSX( 'U', EQ, 2, 1, A, 1, AF, 2, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SPORFSX( 'U', EQ, 2, 1, A, 2, AF, 1, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SPORFSX( 'U', EQ, 2, 1, A, 2, AF, 2, S, B, 1, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SPORFSX( 'U', EQ, 2, 1, A, 2, AF, 2, S, B, 2, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SPORFSX', INFOT, NOUT, LERR, OK )
*
*        SPOCON
*
         SRNAMT = 'SPOCON'
         INFOT = 1
         CALL SPOCON( '/', 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOCON( 'U', -1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPOCON( 'U', 2, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPOCON', INFOT, NOUT, LERR, OK )
*
*        SPOEQU
*
         SRNAMT = 'SPOEQU'
         INFOT = 1
         CALL SPOEQU( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPOEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPOEQU( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPOEQU', INFOT, NOUT, LERR, OK )
*
*        SPOEQUB
*
         SRNAMT = 'SPOEQUB'
         INFOT = 1
         CALL SPOEQUB( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPOEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPOEQUB( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPOEQUB', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PP' ) ) THEN
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive definite packed matrix.
*
*        SPPTRF
*
         SRNAMT = 'SPPTRF'
         INFOT = 1
         CALL SPPTRF( '/', 0, A, INFO )
         CALL CHKXER( 'SPPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPTRF( 'U', -1, A, INFO )
         CALL CHKXER( 'SPPTRF', INFOT, NOUT, LERR, OK )
*
*        SPPTRI
*
         SRNAMT = 'SPPTRI'
         INFOT = 1
         CALL SPPTRI( '/', 0, A, INFO )
         CALL CHKXER( 'SPPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPTRI( 'U', -1, A, INFO )
         CALL CHKXER( 'SPPTRI', INFOT, NOUT, LERR, OK )
*
*        SPPTRS
*
         SRNAMT = 'SPPTRS'
         INFOT = 1
         CALL SPPTRS( '/', 0, 0, A, B, 1, INFO )
         CALL CHKXER( 'SPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPTRS( 'U', -1, 0, A, B, 1, INFO )
         CALL CHKXER( 'SPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPPTRS( 'U', 0, -1, A, B, 1, INFO )
         CALL CHKXER( 'SPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPPTRS( 'U', 2, 1, A, B, 1, INFO )
         CALL CHKXER( 'SPPTRS', INFOT, NOUT, LERR, OK )
*
*        SPPRFS
*
         SRNAMT = 'SPPRFS'
         INFOT = 1
         CALL SPPRFS( '/', 0, 0, A, AF, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPRFS( 'U', -1, 0, A, AF, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPPRFS( 'U', 0, -1, A, AF, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SPPRFS( 'U', 2, 1, A, AF, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SPPRFS( 'U', 2, 1, A, AF, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SPPRFS', INFOT, NOUT, LERR, OK )
*
*        SPPCON
*
         SRNAMT = 'SPPCON'
         INFOT = 1
         CALL SPPCON( '/', 0, A, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPCON( 'U', -1, A, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPPCON', INFOT, NOUT, LERR, OK )
*
*        SPPEQU
*
         SRNAMT = 'SPPEQU'
         INFOT = 1
         CALL SPPEQU( '/', 0, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPPEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPEQU( 'U', -1, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPPEQU', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PB' ) ) THEN
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive definite band matrix.
*
*        SPBTRF
*
         SRNAMT = 'SPBTRF'
         INFOT = 1
         CALL SPBTRF( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'SPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBTRF( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'SPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBTRF( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'SPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPBTRF( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'SPBTRF', INFOT, NOUT, LERR, OK )
*
*        SPBTF2
*
         SRNAMT = 'SPBTF2'
         INFOT = 1
         CALL SPBTF2( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'SPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBTF2( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'SPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBTF2( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'SPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPBTF2( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'SPBTF2', INFOT, NOUT, LERR, OK )
*
*        SPBTRS
*
         SRNAMT = 'SPBTRS'
         INFOT = 1
         CALL SPBTRS( '/', 0, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBTRS( 'U', -1, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBTRS( 'U', 1, -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPBTRS( 'U', 0, 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPBTRS( 'U', 2, 1, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SPBTRS( 'U', 2, 0, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBTRS', INFOT, NOUT, LERR, OK )
*
*        SPBRFS
*
         SRNAMT = 'SPBRFS'
         INFOT = 1
         CALL SPBRFS( '/', 0, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBRFS( 'U', -1, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBRFS( 'U', 1, -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPBRFS( 'U', 0, 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPBRFS( 'U', 2, 1, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SPBRFS( 'U', 2, 1, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SPBRFS', INFOT, NOUT, LERR, OK )
*
*        SPBCON
*
         SRNAMT = 'SPBCON'
         INFOT = 1
         CALL SPBCON( '/', 0, 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBCON( 'U', -1, 0, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBCON( 'U', 1, -1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPBCON( 'U', 2, 1, A, 1, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SPBCON', INFOT, NOUT, LERR, OK )
*
*        SPBEQU
*
         SRNAMT = 'SPBEQU'
         INFOT = 1
         CALL SPBEQU( '/', 0, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBEQU( 'U', -1, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBEQU( 'U', 1, -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPBEQU( 'U', 2, 1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'SPBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRPO
*
      END
