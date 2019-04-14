*> \brief \b CERRGEX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRGE( PATH, NUNIT )
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
*> CERRGE tests the error exits for the COMPLEX routines
*> for general matrices.
*>
*> Note that this file is used only when the XBLAS are available,
*> otherwise cerrge.f defines this subroutine.
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
      SUBROUTINE CERRGE( PATH, NUNIT )
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
      REAL               ANRM, CCOND, RCOND, BERR
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX )
      REAL               R( NMAX ), R1( NMAX ), R2( NMAX ), CS( NMAX ),
     $                   RS( NMAX )
      COMPLEX            A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( 2*NMAX ), X( NMAX ), ERR_BNDS_N( NMAX, 3 ),
     $                   ERR_BNDS_C( NMAX, 3 ), PARAMS( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CGBCON, CGBEQU, CGBRFS, CGBTF2, CGBTRF,
     $                   CGBTRS, CGECON, CGEEQU, CGERFS, CGETF2, CGETRF,
     $                   CGETRI, CGETRS, CHKXER, CGEEQUB, CGERFSX,
     $                   CGBEQUB, CGBRFSX
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
         CS( J ) = 0.
         RS( J ) = 0.
         IP( J ) = J
   20 CONTINUE
      OK = .TRUE.
*
*     Test error exits of the routines that use the LU decomposition
*     of a general matrix.
*
      IF( LSAMEN( 2, C2, 'GE' ) ) THEN
*
*        CGETRF
*
         SRNAMT = 'CGETRF'
         INFOT = 1
         CALL CGETRF( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGETRF( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'CGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGETRF( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'CGETRF', INFOT, NOUT, LERR, OK )
*
*        CGETF2
*
         SRNAMT = 'CGETF2'
         INFOT = 1
         CALL CGETF2( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGETF2( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'CGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGETF2( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'CGETF2', INFOT, NOUT, LERR, OK )
*
*        CGETRI
*
         SRNAMT = 'CGETRI'
         INFOT = 1
         CALL CGETRI( -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CGETRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGETRI( 2, A, 1, IP, W, 2, INFO )
         CALL CHKXER( 'CGETRI', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGETRI( 2, A, 2, IP, W, 1, INFO )
         CALL CHKXER( 'CGETRI', INFOT, NOUT, LERR, OK )
*
*        CGETRS
*
         SRNAMT = 'CGETRS'
         INFOT = 1
         CALL CGETRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGETRS( 'N', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGETRS( 'N', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGETRS( 'N', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'CGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CGETRS( 'N', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'CGETRS', INFOT, NOUT, LERR, OK )
*
*        CGERFS
*
         SRNAMT = 'CGERFS'
         INFOT = 1
         CALL CGERFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGERFS( 'N', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGERFS( 'N', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGERFS( 'N', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CGERFS( 'N', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CGERFS', INFOT, NOUT, LERR, OK )
*
*        CGERFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'CGERFSX'
         INFOT = 1
         CALL CGERFSX( '/', EQ, 0, 0, A, 1, AF, 1, IP, RS, CS, B, 1, X,
     $                1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         EQ = '/'
         CALL CGERFSX( 'N', EQ, 2, 1, A, 1, AF, 2, IP, RS, CS, B, 2, X,
     $                2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         EQ = 'R'
         CALL CGERFSX( 'N', EQ, -1, 0, A, 1, AF, 1, IP, RS, CS, B, 1, X,
     $                1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGERFSX( 'N', EQ, 0, -1, A, 1, AF, 1, IP, RS, CS, B, 1, X,
     $                1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGERFSX( 'N', EQ, 2, 1, A, 1, AF, 2, IP, RS, CS, B, 2, X,
     $                2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CGERFSX( 'N', EQ, 2, 1, A, 2, AF, 1, IP, RS, CS, B, 2, X,
     $                2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         EQ = 'C'
         CALL CGERFSX( 'N', EQ, 2, 1, A, 2, AF, 2, IP, RS, CS, B, 1, X,
     $                2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL CGERFSX( 'N', EQ, 2, 1, A, 2, AF, 2, IP, RS, CS, B, 2, X,
     $                1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGERFSX', INFOT, NOUT, LERR, OK )
*
*        CGECON
*
         SRNAMT = 'CGECON'
         INFOT = 1
         CALL CGECON( '/', 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGECON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGECON( '1', -1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGECON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGECON( '1', 2, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGECON', INFOT, NOUT, LERR, OK )
*
*        CGEEQU
*
         SRNAMT = 'CGEEQU'
         INFOT = 1
         CALL CGEEQU( -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'CGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGEEQU( 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'CGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGEEQU( 2, 2, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'CGEEQU', INFOT, NOUT, LERR, OK )
*
*        CGEEQUB
*
         SRNAMT = 'CGEEQUB'
         INFOT = 1
         CALL CGEEQUB( -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'CGEEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGEEQUB( 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'CGEEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGEEQUB( 2, 2, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'CGEEQUB', INFOT, NOUT, LERR, OK )
*
*     Test error exits of the routines that use the LU decomposition
*     of a general band matrix.
*
      ELSE IF( LSAMEN( 2, C2, 'GB' ) ) THEN
*
*        CGBTRF
*
         SRNAMT = 'CGBTRF'
         INFOT = 1
         CALL CGBTRF( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBTRF( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBTRF( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBTRF( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGBTRF( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'CGBTRF', INFOT, NOUT, LERR, OK )
*
*        CGBTF2
*
         SRNAMT = 'CGBTF2'
         INFOT = 1
         CALL CGBTF2( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBTF2( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBTF2( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBTF2( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'CGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGBTF2( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'CGBTF2', INFOT, NOUT, LERR, OK )
*
*        CGBTRS
*
         SRNAMT = 'CGBTRS'
         INFOT = 1
         CALL CGBTRS( '/', 0, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBTRS( 'N', -1, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBTRS( 'N', 1, -1, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBTRS( 'N', 1, 0, -1, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGBTRS( 'N', 1, 0, 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CGBTRS( 'N', 2, 1, 1, 1, A, 3, IP, B, 2, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CGBTRS( 'N', 2, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CGBTRS', INFOT, NOUT, LERR, OK )
*
*        CGBRFS
*
         SRNAMT = 'CGBRFS'
         INFOT = 1
         CALL CGBRFS( '/', 0, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBRFS( 'N', -1, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBRFS( 'N', 1, -1, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBRFS( 'N', 1, 0, -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGBRFS( 'N', 1, 0, 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CGBRFS( 'N', 2, 1, 1, 1, A, 2, AF, 4, IP, B, 2, X, 2, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CGBRFS( 'N', 2, 1, 1, 1, A, 3, AF, 3, IP, B, 2, X, 2, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 1, X, 2, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL CGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 2, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'CGBRFS', INFOT, NOUT, LERR, OK )
*
*        CGBRFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'CGBRFSX'
         INFOT = 1
         CALL CGBRFSX( '/', EQ, 0, 0, 0, 0, A, 1, AF, 1, IP, RS, CS, B,
     $                1, X, 1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS,  W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         EQ = '/'
         CALL CGBRFSX( 'N', EQ, 2, 1, 1, 1, A, 1, AF, 2, IP, RS, CS, B,
     $                2, X, 2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         EQ = 'R'
         CALL CGBRFSX( 'N', EQ, -1, 1, 1, 0, A, 1, AF, 1, IP, RS, CS, B,
     $                1, X, 1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         EQ = 'R'
         CALL CGBRFSX( 'N', EQ, 2, -1, 1, 1, A, 3, AF, 4, IP, RS, CS, B,
     $                1, X, 1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         EQ = 'R'
         CALL CGBRFSX( 'N', EQ, 2, 1, -1, 1, A, 3, AF, 4, IP, RS, CS, B,
     $                1, X, 1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGBRFSX( 'N', EQ, 0, 0, 0, -1, A, 1, AF, 1, IP, RS, CS, B,
     $                1, X, 1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CGBRFSX( 'N', EQ, 2, 1, 1, 1, A, 1, AF, 2, IP, RS, CS, B,
     $                2, X, 2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CGBRFSX( 'N', EQ, 2, 1, 1, 1, A, 3, AF, 3, IP, RS, CS, B,
     $                2, X, 2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         EQ = 'C'
         CALL CGBRFSX( 'N', EQ, 2, 1, 1, 1, A, 3, AF, 5, IP, RS, CS, B,
     $                1, X, 2, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL CGBRFSX( 'N', EQ, 2, 1, 1, 1, A, 3, AF, 5, IP, RS, CS, B,
     $                2, X, 1, RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, R, INFO )
         CALL CHKXER( 'CGBRFSX', INFOT, NOUT, LERR, OK )
*
*        CGBCON
*
         SRNAMT = 'CGBCON'
         INFOT = 1
         CALL CGBCON( '/', 0, 0, 0, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBCON( '1', -1, 0, 0, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBCON( '1', 1, -1, 0, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBCON( '1', 1, 0, -1, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGBCON( '1', 2, 1, 1, A, 3, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'CGBCON', INFOT, NOUT, LERR, OK )
*
*        CGBEQU
*
         SRNAMT = 'CGBEQU'
         INFOT = 1
         CALL CGBEQU( -1, 0, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBEQU( 0, -1, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBEQU( 1, 1, -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBEQU( 1, 1, 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGBEQU( 2, 2, 1, 1, A, 2, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQU', INFOT, NOUT, LERR, OK )
*
*        CGBEQUB
*
         SRNAMT = 'CGBEQUB'
         INFOT = 1
         CALL CGBEQUB( -1, 0, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGBEQUB( 0, -1, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGBEQUB( 1, 1, -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGBEQUB( 1, 1, 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQUB', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CGBEQUB( 2, 2, 1, 1, A, 2, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'CGBEQUB', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRGE
*
      END
