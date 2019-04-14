*> \brief \b ZERRPOX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRPO( PATH, NUNIT )
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
*> ZERRPO tests the error exits for the COMPLEX*16 routines
*> for Hermitian positive definite matrices.
*>
*> Note that this file is used only when the XBLAS are available,
*> otherwise zerrpo.f defines this subroutine.
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
      SUBROUTINE ZERRPO( PATH, NUNIT )
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
      DOUBLE PRECISION   ANRM, RCOND, BERR
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   S( NMAX ), R( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   ERR_BNDS_N( NMAX, 3 ), ERR_BNDS_C( NMAX, 3 ),
     $                   PARAMS( 1 )
      COMPLEX*16         A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( 2*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZPBCON, ZPBEQU, ZPBRFS, ZPBTF2,
     $                   ZPBTRF, ZPBTRS, ZPOCON, ZPOEQU, ZPORFS, ZPOTF2,
     $                   ZPOTRF, ZPOTRI, ZPOTRS, ZPPCON, ZPPEQU, ZPPRFS,
     $                   ZPPTRF, ZPPTRI, ZPPTRS, ZPOEQUB, ZPORFSX
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
      INTRINSIC          DBLE, DCMPLX
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
            A( I, J ) = DCMPLX( 1.D0 / DBLE( I+J ),
     $                  -1.D0 / DBLE( I+J ) )
            AF( I, J ) = DCMPLX( 1.D0 / DBLE( I+J ),
     $                   -1.D0 / DBLE( I+J ) )
   10    CONTINUE
         B( J ) = 0.D0
         R1( J ) = 0.D0
         R2( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
         S( J ) = 0.D0
   20 CONTINUE
      ANRM = 1.D0
      OK = .TRUE.
*
*     Test error exits of the routines that use the Cholesky
*     decomposition of a Hermitian positive definite matrix.
*
      IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        ZPOTRF
*
         SRNAMT = 'ZPOTRF'
         INFOT = 1
         CALL ZPOTRF( '/', 0, A, 1, INFO )
         CALL CHKXER( 'ZPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPOTRF( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'ZPOTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPOTRF( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'ZPOTRF', INFOT, NOUT, LERR, OK )
*
*        ZPOTF2
*
         SRNAMT = 'ZPOTF2'
         INFOT = 1
         CALL ZPOTF2( '/', 0, A, 1, INFO )
         CALL CHKXER( 'ZPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPOTF2( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'ZPOTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPOTF2( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'ZPOTF2', INFOT, NOUT, LERR, OK )
*
*        ZPOTRI
*
         SRNAMT = 'ZPOTRI'
         INFOT = 1
         CALL ZPOTRI( '/', 0, A, 1, INFO )
         CALL CHKXER( 'ZPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPOTRI( 'U', -1, A, 1, INFO )
         CALL CHKXER( 'ZPOTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPOTRI( 'U', 2, A, 1, INFO )
         CALL CHKXER( 'ZPOTRI', INFOT, NOUT, LERR, OK )
*
*        ZPOTRS
*
         SRNAMT = 'ZPOTRS'
         INFOT = 1
         CALL ZPOTRS( '/', 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPOTRS( 'U', -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPOTRS( 'U', 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPOTRS( 'U', 2, 1, A, 1, B, 2, INFO )
         CALL CHKXER( 'ZPOTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZPOTRS( 'U', 2, 1, A, 2, B, 1, INFO )
         CALL CHKXER( 'ZPOTRS', INFOT, NOUT, LERR, OK )
*
*        ZPORFS
*
         SRNAMT = 'ZPORFS'
         INFOT = 1
         CALL ZPORFS( '/', 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPORFS( 'U', -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPORFS( 'U', 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPORFS( 'U', 2, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZPORFS( 'U', 2, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZPORFS( 'U', 2, 1, A, 2, AF, 2, B, 1, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZPORFS( 'U', 2, 1, A, 2, AF, 2, B, 2, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPORFS', INFOT, NOUT, LERR, OK )
*
*        ZPORFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'ZPORFSX'
         INFOT = 1
         CALL ZPORFSX( '/', EQ, 0, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPORFSX( 'U', "/", -1, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         EQ = 'N'
         INFOT = 3
         CALL ZPORFSX( 'U', EQ, -1, 0, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPORFSX( 'U', EQ, 0, -1, A, 1, AF, 1, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZPORFSX( 'U', EQ, 2, 1, A, 1, AF, 2, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZPORFSX( 'U', EQ, 2, 1, A, 2, AF, 1, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZPORFSX( 'U', EQ, 2, 1, A, 2, AF, 2, S, B, 1, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZPORFSX( 'U', EQ, 2, 1, A, 2, AF, 2, S, B, 2, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'ZPORFSX', INFOT, NOUT, LERR, OK )
*
*        ZPOCON
*
         SRNAMT = 'ZPOCON'
         INFOT = 1
         CALL ZPOCON( '/', 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPOCON( 'U', -1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPOCON( 'U', 2, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPOCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPOCON( 'U', 1, A, 1, -ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPOCON', INFOT, NOUT, LERR, OK )
*
*        ZPOEQU
*
         SRNAMT = 'ZPOEQU'
         INFOT = 1
         CALL ZPOEQU( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPOEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPOEQU( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPOEQU', INFOT, NOUT, LERR, OK )
*
*        ZPOEQUB
*
         SRNAMT = 'ZPOEQUB'
         INFOT = 1
         CALL ZPOEQUB( -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPOEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPOEQUB( 2, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPOEQUB', INFOT, NOUT, LERR, OK )
*
*     Test error exits of the routines that use the Cholesky
*     decomposition of a Hermitian positive definite packed matrix.
*
      ELSE IF( LSAMEN( 2, C2, 'PP' ) ) THEN
*
*        ZPPTRF
*
         SRNAMT = 'ZPPTRF'
         INFOT = 1
         CALL ZPPTRF( '/', 0, A, INFO )
         CALL CHKXER( 'ZPPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPPTRF( 'U', -1, A, INFO )
         CALL CHKXER( 'ZPPTRF', INFOT, NOUT, LERR, OK )
*
*        ZPPTRI
*
         SRNAMT = 'ZPPTRI'
         INFOT = 1
         CALL ZPPTRI( '/', 0, A, INFO )
         CALL CHKXER( 'ZPPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPPTRI( 'U', -1, A, INFO )
         CALL CHKXER( 'ZPPTRI', INFOT, NOUT, LERR, OK )
*
*        ZPPTRS
*
         SRNAMT = 'ZPPTRS'
         INFOT = 1
         CALL ZPPTRS( '/', 0, 0, A, B, 1, INFO )
         CALL CHKXER( 'ZPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPPTRS( 'U', -1, 0, A, B, 1, INFO )
         CALL CHKXER( 'ZPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPPTRS( 'U', 0, -1, A, B, 1, INFO )
         CALL CHKXER( 'ZPPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZPPTRS( 'U', 2, 1, A, B, 1, INFO )
         CALL CHKXER( 'ZPPTRS', INFOT, NOUT, LERR, OK )
*
*        ZPPRFS
*
         SRNAMT = 'ZPPRFS'
         INFOT = 1
         CALL ZPPRFS( '/', 0, 0, A, AF, B, 1, X, 1, R1, R2, W, R, INFO )
         CALL CHKXER( 'ZPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPPRFS( 'U', -1, 0, A, AF, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPPRFS( 'U', 0, -1, A, AF, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'ZPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZPPRFS( 'U', 2, 1, A, AF, B, 1, X, 2, R1, R2, W, R, INFO )
         CALL CHKXER( 'ZPPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZPPRFS( 'U', 2, 1, A, AF, B, 2, X, 1, R1, R2, W, R, INFO )
         CALL CHKXER( 'ZPPRFS', INFOT, NOUT, LERR, OK )
*
*        ZPPCON
*
         SRNAMT = 'ZPPCON'
         INFOT = 1
         CALL ZPPCON( '/', 0, A, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPPCON( 'U', -1, A, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPPCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPPCON( 'U', 1, A, -ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPPCON', INFOT, NOUT, LERR, OK )
*
*        ZPPEQU
*
         SRNAMT = 'ZPPEQU'
         INFOT = 1
         CALL ZPPEQU( '/', 0, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPPEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPPEQU( 'U', -1, A, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPPEQU', INFOT, NOUT, LERR, OK )
*
*     Test error exits of the routines that use the Cholesky
*     decomposition of a Hermitian positive definite band matrix.
*
      ELSE IF( LSAMEN( 2, C2, 'PB' ) ) THEN
*
*        ZPBTRF
*
         SRNAMT = 'ZPBTRF'
         INFOT = 1
         CALL ZPBTRF( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'ZPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPBTRF( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'ZPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPBTRF( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'ZPBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPBTRF( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'ZPBTRF', INFOT, NOUT, LERR, OK )
*
*        ZPBTF2
*
         SRNAMT = 'ZPBTF2'
         INFOT = 1
         CALL ZPBTF2( '/', 0, 0, A, 1, INFO )
         CALL CHKXER( 'ZPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPBTF2( 'U', -1, 0, A, 1, INFO )
         CALL CHKXER( 'ZPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPBTF2( 'U', 1, -1, A, 1, INFO )
         CALL CHKXER( 'ZPBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPBTF2( 'U', 2, 1, A, 1, INFO )
         CALL CHKXER( 'ZPBTF2', INFOT, NOUT, LERR, OK )
*
*        ZPBTRS
*
         SRNAMT = 'ZPBTRS'
         INFOT = 1
         CALL ZPBTRS( '/', 0, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPBTRS( 'U', -1, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPBTRS( 'U', 1, -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPBTRS( 'U', 0, 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZPBTRS( 'U', 2, 1, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZPBTRS( 'U', 2, 0, 1, A, 1, B, 1, INFO )
         CALL CHKXER( 'ZPBTRS', INFOT, NOUT, LERR, OK )
*
*        ZPBRFS
*
         SRNAMT = 'ZPBRFS'
         INFOT = 1
         CALL ZPBRFS( '/', 0, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPBRFS( 'U', -1, 0, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPBRFS( 'U', 1, -1, 0, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZPBRFS( 'U', 0, 0, -1, A, 1, AF, 1, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZPBRFS( 'U', 2, 1, 1, A, 1, AF, 2, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZPBRFS( 'U', 2, 1, 1, A, 2, AF, 1, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 1, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZPBRFS( 'U', 2, 0, 1, A, 1, AF, 1, B, 2, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZPBRFS', INFOT, NOUT, LERR, OK )
*
*        ZPBCON
*
         SRNAMT = 'ZPBCON'
         INFOT = 1
         CALL ZPBCON( '/', 0, 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPBCON( 'U', -1, 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPBCON( 'U', 1, -1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPBCON( 'U', 2, 1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPBCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZPBCON( 'U', 1, 0, A, 1, -ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZPBCON', INFOT, NOUT, LERR, OK )
*
*        ZPBEQU
*
         SRNAMT = 'ZPBEQU'
         INFOT = 1
         CALL ZPBEQU( '/', 0, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZPBEQU( 'U', -1, 0, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZPBEQU( 'U', 1, -1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZPBEQU( 'U', 2, 1, A, 1, R1, RCOND, ANRM, INFO )
         CALL CHKXER( 'ZPBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRPO
*
      END
