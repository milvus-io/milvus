*> \brief \b DERRVXX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRVX( PATH, NUNIT )
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
*> DERRVX tests the error exits for the DOUBLE PRECISION driver routines
*> for solving linear systems of equations.
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
      SUBROUTINE DERRVX( PATH, NUNIT )
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
      REAL               ONE
      PARAMETER          ( ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      CHARACTER          EQ
      CHARACTER*2        C2
      INTEGER            I, INFO, J, N_ERR_BNDS, NPARAMS
      DOUBLE PRECISION   RCOND, RPVGRW, BERR
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   C( NMAX ), E( NMAX ), R( NMAX ), R1( NMAX ),
     $                   R2( NMAX ), W( 2*NMAX ), X( NMAX ),
     $                   ERR_BNDS_N( NMAX, 3 ), ERR_BNDS_C( NMAX, 3 ),
     $                   PARAMS( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DGBSV, DGBSVX, DGESV, DGESVX, DGTSV,
     $                   DGTSVX, DPBSV, DPBSVX, DPOSV, DPOSVX, DPPSV,
     $                   DPPSVX, DPTSV, DPTSVX, DSPSV, DSPSVX, DSYSV,
     $                   DSYSV_RK, DSYSV_ROOK, DSYSVX, DGESVXX, DSYSVXX,
     $                   DPOSVXX, DGBSVXX
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
      INTRINSIC          DBLE
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
            A( I, J ) = 1.D0 / DBLE( I+J )
            AF( I, J ) = 1.D0 / DBLE( I+J )
   10    CONTINUE
         B( J ) = 0.D+0
         E( J ) = 0.D+0
         R1( J ) = 0.D+0
         R2( J ) = 0.D+0
         W( J ) = 0.D+0
         X( J ) = 0.D+0
         C( J ) = 0.D+0
         R( J ) = 0.D+0
         IP( J ) = J
   20 CONTINUE
      EQ = ' '
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GE' ) ) THEN
*
*        DGESV
*
         SRNAMT = 'DGESV '
         INFOT = 1
         CALL DGESV( -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGESV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESV( 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGESV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGESV( 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'DGESV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGESV( 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'DGESV ', INFOT, NOUT, LERR, OK )
*
*        DGESVX
*
         SRNAMT = 'DGESVX'
         INFOT = 1
         CALL DGESVX( '/', 'N', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESVX( 'N', '/', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGESVX( 'N', 'N', -1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGESVX( 'N', 'N', 0, -1, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGESVX( 'N', 'N', 2, 1, A, 1, AF, 2, IP, EQ, R, C, B, 2,
     $                X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGESVX( 'N', 'N', 2, 1, A, 2, AF, 1, IP, EQ, R, C, B, 2,
     $                X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = '/'
         CALL DGESVX( 'F', 'N', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ = 'R'
         CALL DGESVX( 'F', 'N', 1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         EQ = 'C'
         CALL DGESVX( 'F', 'N', 1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGESVX( 'N', 'N', 2, 1, A, 2, AF, 2, IP, EQ, R, C, B, 1,
     $                X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGESVX( 'N', 'N', 2, 1, A, 2, AF, 2, IP, EQ, R, C, B, 2,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGESVX', INFOT, NOUT, LERR, OK )
*
*        DGESVXX
*
         N_ERR_BNDS = 3
         NPARAMS = 1
         SRNAMT = 'DGESVXX'
         INFOT = 1
         CALL DGESVXX( '/', 'N', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESVXX( 'N', '/', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGESVXX( 'N', 'N', -1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGESVXX( 'N', 'N', 0, -1, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGESVXX( 'N', 'N', 2, 1, A, 1, AF, 2, IP, EQ, R, C, B, 2,
     $                X, 2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGESVXX( 'N', 'N', 2, 1, A, 2, AF, 1, IP, EQ, R, C, B, 2,
     $                X, 2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = '/'
         CALL DGESVXX( 'F', 'N', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ = 'R'
         CALL DGESVXX( 'F', 'N', 1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         EQ = 'C'
         CALL DGESVXX( 'F', 'N', 1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGESVXX( 'N', 'N', 2, 1, A, 2, AF, 2, IP, EQ, R, C, B, 1,
     $                X, 2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGESVXX( 'N', 'N', 2, 1, A, 2, AF, 2, IP, EQ, R, C, B, 2,
     $                X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DGESVXX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'GB' ) ) THEN
*
*        DGBSV
*
         SRNAMT = 'DGBSV '
         INFOT = 1
         CALL DGBSV( -1, 0, 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBSV( 1, -1, 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBSV( 1, 0, -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBSV( 0, 0, 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBSV( 1, 1, 1, 0, A, 3, IP, B, 1, INFO )
         CALL CHKXER( 'DGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGBSV( 2, 0, 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'DGBSV ', INFOT, NOUT, LERR, OK )
*
*        DGBSVX
*
         SRNAMT = 'DGBSVX'
         INFOT = 1
         CALL DGBSVX( '/', 'N', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBSVX( 'N', '/', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBSVX( 'N', 'N', -1, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBSVX( 'N', 'N', 1, -1, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGBSVX( 'N', 'N', 1, 0, -1, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBSVX( 'N', 'N', 0, 0, 0, -1, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGBSVX( 'N', 'N', 1, 1, 1, 0, A, 2, AF, 4, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGBSVX( 'N', 'N', 1, 1, 1, 0, A, 3, AF, 3, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         EQ = '/'
         CALL DGBSVX( 'F', 'N', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         EQ = 'R'
         CALL DGBSVX( 'F', 'N', 1, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         EQ = 'C'
         CALL DGBSVX( 'F', 'N', 1, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGBSVX( 'N', 'N', 2, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DGBSVX( 'N', 'N', 2, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 2, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGBSVX', INFOT, NOUT, LERR, OK )
*
*        DGBSVXX
*
         N_ERR_BNDS = 3
         NPARAMS = 1
         SRNAMT = 'DGBSVXX'
         INFOT = 1
         CALL DGBSVXX( '/', 'N', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGBSVXX( 'N', '/', 0, 1, 1, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGBSVXX( 'N', 'N', -1, 1, 1, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGBSVXX( 'N', 'N', 2, -1, 1, 0, A, 1, AF, 1, IP, EQ,
     $                R, C, B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGBSVXX( 'N', 'N', 2, 1, -1, 0, A, 1, AF, 1, IP, EQ,
     $                R, C, B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGBSVXX( 'N', 'N', 0, 1, 1, -1, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGBSVXX( 'N', 'N', 2, 1, 1, 1, A, 2, AF, 2, IP, EQ, R, C,
     $                B, 2, X, 2, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGBSVXX( 'N', 'N', 2, 1, 1, 1, A, 3, AF, 3, IP, EQ, R, C,
     $                B, 2, X, 2, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         EQ = '/'
         CALL DGBSVXX( 'F', 'N', 0, 1, 1, 0, A, 3, AF, 4, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         EQ = 'R'
         CALL DGBSVXX( 'F', 'N', 1, 1, 1, 0, A, 3, AF, 4, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         EQ = 'C'
         CALL DGBSVXX( 'F', 'N', 1, 1, 1, 0, A, 3, AF, 4, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGBSVXX( 'N', 'N', 2, 1, 1, 1, A, 3, AF, 4, IP, EQ, R, C,
     $                B, 1, X, 2, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGBSVXX( 'N', 'N', 2, 1, 1, 1, A, 3, AF, 4, IP, EQ, R, C,
     $                B, 2, X, 1, RCOND, RPVGRW, BERR, N_ERR_BNDS,
     $                ERR_BNDS_N, ERR_BNDS_C, NPARAMS, PARAMS, W, IW,
     $                INFO )
         CALL CHKXER( 'DGBSVXX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'GT' ) ) THEN
*
*        DGTSV
*
         SRNAMT = 'DGTSV '
         INFOT = 1
         CALL DGTSV( -1, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ), B, 1,
     $               INFO )
         CALL CHKXER( 'DGTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGTSV( 0, -1, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ), B, 1,
     $               INFO )
         CALL CHKXER( 'DGTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGTSV( 2, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ), B, 1, INFO )
         CALL CHKXER( 'DGTSV ', INFOT, NOUT, LERR, OK )
*
*        DGTSVX
*
         SRNAMT = 'DGTSVX'
         INFOT = 1
         CALL DGTSVX( '/', 'N', 0, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGTSVX( 'N', '/', 0, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGTSVX( 'N', 'N', -1, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGTSVX( 'N', 'N', 0, -1, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGTSVX( 'N', 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGTSVX( 'N', 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 2, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DGTSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        DPOSV
*
         SRNAMT = 'DPOSV '
         INFOT = 1
         CALL DPOSV( '/', 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOSV( 'U', -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPOSV( 'U', 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPOSV( 'U', 2, 0, A, 1, B, 2, INFO )
         CALL CHKXER( 'DPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DPOSV( 'U', 2, 0, A, 2, B, 1, INFO )
         CALL CHKXER( 'DPOSV ', INFOT, NOUT, LERR, OK )
*
*        DPOSVX
*
         SRNAMT = 'DPOSVX'
         INFOT = 1
         CALL DPOSVX( '/', 'U', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOSVX( 'N', '/', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPOSVX( 'N', 'U', -1, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPOSVX( 'N', 'U', 0, -1, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPOSVX( 'N', 'U', 2, 0, A, 1, AF, 2, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DPOSVX( 'N', 'U', 2, 0, A, 2, AF, 1, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         EQ = '/'
         CALL DPOSVX( 'F', 'U', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = 'Y'
         CALL DPOSVX( 'F', 'U', 1, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DPOSVX( 'N', 'U', 2, 0, A, 2, AF, 2, EQ, C, B, 1, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DPOSVX( 'N', 'U', 2, 0, A, 2, AF, 2, EQ, C, B, 2, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPOSVX', INFOT, NOUT, LERR, OK )
*
*        DPOSVXX
*
         N_ERR_BNDS = 3
         NPARAMS = 1
         SRNAMT = 'DPOSVXX'
         INFOT = 1
         CALL DPOSVXX( '/', 'U', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPOSVXX( 'N', '/', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPOSVXX( 'N', 'U', -1, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPOSVXX( 'N', 'U', 0, -1, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPOSVXX( 'N', 'U', 2, 0, A, 1, AF, 2, EQ, C, B, 2, X, 2,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DPOSVXX( 'N', 'U', 2, 0, A, 2, AF, 1, EQ, C, B, 2, X, 2,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         EQ = '/'
         CALL DPOSVXX( 'F', 'U', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = 'Y'
         CALL DPOSVXX( 'F', 'U', 1, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DPOSVXX( 'N', 'U', 2, 0, A, 2, AF, 2, EQ, C, B, 1, X, 2,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DPOSVXX( 'N', 'U', 2, 0, A, 2, AF, 2, EQ, C, B, 2, X, 1,
     $                RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $                ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DPOSVXX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PP' ) ) THEN
*
*        DPPSV
*
         SRNAMT = 'DPPSV '
         INFOT = 1
         CALL DPPSV( '/', 0, 0, A, B, 1, INFO )
         CALL CHKXER( 'DPPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPSV( 'U', -1, 0, A, B, 1, INFO )
         CALL CHKXER( 'DPPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPPSV( 'U', 0, -1, A, B, 1, INFO )
         CALL CHKXER( 'DPPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPPSV( 'U', 2, 0, A, B, 1, INFO )
         CALL CHKXER( 'DPPSV ', INFOT, NOUT, LERR, OK )
*
*        DPPSVX
*
         SRNAMT = 'DPPSVX'
         INFOT = 1
         CALL DPPSVX( '/', 'U', 0, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPPSVX( 'N', '/', 0, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPPSVX( 'N', 'U', -1, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPPSVX( 'N', 'U', 0, -1, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         EQ = '/'
         CALL DPPSVX( 'F', 'U', 0, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         EQ = 'Y'
         CALL DPPSVX( 'F', 'U', 1, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DPPSVX( 'N', 'U', 2, 0, A, AF, EQ, C, B, 1, X, 2, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DPPSVX( 'N', 'U', 2, 0, A, AF, EQ, C, B, 2, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPPSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PB' ) ) THEN
*
*        DPBSV
*
         SRNAMT = 'DPBSV '
         INFOT = 1
         CALL DPBSV( '/', 0, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBSV( 'U', -1, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBSV( 'U', 1, -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPBSV( 'U', 0, 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPBSV( 'U', 1, 1, 0, A, 1, B, 2, INFO )
         CALL CHKXER( 'DPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DPBSV( 'U', 2, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'DPBSV ', INFOT, NOUT, LERR, OK )
*
*        DPBSVX
*
         SRNAMT = 'DPBSVX'
         INFOT = 1
         CALL DPBSVX( '/', 'U', 0, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPBSVX( 'N', '/', 0, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPBSVX( 'N', 'U', -1, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X,
     $                1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DPBSVX( 'N', 'U', 1, -1, 0, A, 1, AF, 1, EQ, C, B, 1, X,
     $                1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DPBSVX( 'N', 'U', 0, 0, -1, A, 1, AF, 1, EQ, C, B, 1, X,
     $                1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DPBSVX( 'N', 'U', 1, 1, 0, A, 1, AF, 2, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DPBSVX( 'N', 'U', 1, 1, 0, A, 2, AF, 1, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = '/'
         CALL DPBSVX( 'F', 'U', 0, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ = 'Y'
         CALL DPBSVX( 'F', 'U', 1, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DPBSVX( 'N', 'U', 2, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DPBSVX( 'N', 'U', 2, 0, 0, A, 1, AF, 1, EQ, C, B, 2, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'DPBSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PT' ) ) THEN
*
*        DPTSV
*
         SRNAMT = 'DPTSV '
         INFOT = 1
         CALL DPTSV( -1, 0, A( 1, 1 ), A( 1, 2 ), B, 1, INFO )
         CALL CHKXER( 'DPTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPTSV( 0, -1, A( 1, 1 ), A( 1, 2 ), B, 1, INFO )
         CALL CHKXER( 'DPTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPTSV( 2, 0, A( 1, 1 ), A( 1, 2 ), B, 1, INFO )
         CALL CHKXER( 'DPTSV ', INFOT, NOUT, LERR, OK )
*
*        DPTSVX
*
         SRNAMT = 'DPTSVX'
         INFOT = 1
         CALL DPTSVX( '/', 0, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'DPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPTSVX( 'N', -1, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'DPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DPTSVX( 'N', 0, -1, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'DPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DPTSVX( 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 2, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'DPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DPTSVX( 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 2, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'DPTSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SY' ) ) THEN
*
*        DSYSV
*
         SRNAMT = 'DSYSV '
         INFOT = 1
         CALL DSYSV( '/', 0, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYSV( 'U', -1, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYSV( 'U', 0, -1, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYSV( 'U', 2, 0, A, 1, IP, B, 2, W, 1, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYSV( 'U', 2, 0, A, 2, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYSV( 'U', 0, 0, A, 1, IP, B, 1, W, 0, INFO )
         CALL CHKXER( 'DSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYSV( 'U', 0, 0, A, 1, IP, B, 1, W, -2, INFO )
         CALL CHKXER( 'DSYSV ', INFOT, NOUT, LERR, OK )
*
*        DSYSVX
*
         SRNAMT = 'DSYSVX'
         INFOT = 1
         CALL DSYSVX( '/', 'U', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYSVX( 'N', '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYSVX( 'N', 'U', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSYSVX( 'N', 'U', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSYSVX( 'N', 'U', 2, 0, A, 1, AF, 2, IP, B, 2, X, 2,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYSVX( 'N', 'U', 2, 0, A, 2, AF, 1, IP, B, 2, X, 2,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSYSVX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, B, 1, X, 2,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSYSVX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, B, 2, X, 1,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DSYSVX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, B, 2, X, 2,
     $                RCOND, R1, R2, W, 3, IW, INFO )
         CALL CHKXER( 'DSYSVX', INFOT, NOUT, LERR, OK )
*
*        DSYSVXX
*
         N_ERR_BNDS = 3
         NPARAMS = 1
         SRNAMT = 'DSYSVXX'
         INFOT = 1
         EQ = 'N'
         CALL DSYSVXX( '/', 'U', 0, 0, A, 1, AF, 1, IP, EQ, R, B, 1, X,
     $        1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C,  NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYSVXX( 'N', '/', 0, 0, A, 1, AF, 1, IP, EQ, R, B, 1, X,
     $        1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYSVXX( 'N', 'U', -1, 0, A, 1, AF, 1, IP, EQ, R, B, 1, X,
     $        1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C,  NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         EQ = '/'
         CALL DSYSVXX( 'N', 'U', 0, -1, A, 1, AF, 1, IP, EQ, R, B, 1, X,
     $        1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         EQ = 'Y'
         INFOT = 6
         CALL DSYSVXX( 'N', 'U', 2, 0, A, 1, AF, 2, IP, EQ, R, B, 2, X,
     $        2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYSVXX( 'N', 'U', 2, 0, A, 2, AF, 1, IP, EQ, R, B, 2, X,
     $        2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYSVXX( 'F', 'U', 2, 0, A, 2, AF, 2, IP, 'A', R, B, 2, X,
     $        2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ='Y'
         CALL DSYSVXX( 'F', 'U', 2, 0, A, 2, AF, 2, IP, EQ, R, B, 2, X,
     $        2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ='Y'
         R(1) = -ONE
         CALL DSYSVXX( 'F', 'U', 2, 0, A, 2, AF, 2, IP, EQ, R, B, 2, X,
     $        2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         EQ = 'N'
         CALL DSYSVXX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, EQ, R, B, 1, X,
     $        2, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DSYSVXX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, EQ, R, B, 2, X,
     $        1, RCOND, RPVGRW, BERR, N_ERR_BNDS, ERR_BNDS_N,
     $        ERR_BNDS_C, NPARAMS, PARAMS, W, IW, INFO )
         CALL CHKXER( 'DSYSVXX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SR' ) ) THEN
*
*        DSYSV_ROOK
*
         SRNAMT = 'DSYSV_ROOK'
         INFOT = 1
         CALL DSYSV_ROOK( '/', 0, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYSV_ROOK( 'U', -1, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYSV_ROOK( 'U', 0, -1, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYSV_ROOK( 'U', 2, 0, A, 1, IP, B, 2, W, 1, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYSV_ROOK( 'U', 2, 0, A, 2, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYSV_ROOK( 'U', 0, 0, A, 1, IP, B, 1, W, 0, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYSV_ROOK( 'U', 0, 0, A, 1, IP, B, 1, W, -2, INFO )
         CALL CHKXER( 'DSYSV_ROOK', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SK' ) ) THEN
*
*        DSYSV_RK
*
*        Test error exits of the driver that uses factorization
*        of a symmetric indefinite matrix with rook
*        (bounded Bunch-Kaufman) pivoting with the new storage
*        format for factors L ( or U) and D.
*
*        L (or U) is stored in A, diagonal of D is stored on the
*        diagonal of A, subdiagonal of D is stored in a separate array E.
*
         SRNAMT = 'DSYSV_RK'
         INFOT = 1
         CALL DSYSV_RK( '/', 0, 0, A, 1, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYSV_RK( 'U', -1, 0, A, 1, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYSV_RK( 'U', 0, -1, A, 1, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYSV_RK( 'U', 2, 0, A, 1, E, IP, B, 2, W, 1, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYSV_RK( 'U', 2, 0, A, 2, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSYSV_RK( 'U', 0, 0, A, 1, E, IP, B, 1, W, 0, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSYSV_RK( 'U', 0, 0, A, 1, E, IP, B, 1, W, -2, INFO )
         CALL CHKXER( 'DSYSV_RK', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SP' ) ) THEN
*
*        DSPSV
*
         SRNAMT = 'DSPSV '
         INFOT = 1
         CALL DSPSV( '/', 0, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'DSPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSPSV( 'U', -1, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'DSPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSPSV( 'U', 0, -1, A, IP, B, 1, INFO )
         CALL CHKXER( 'DSPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSPSV( 'U', 2, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'DSPSV ', INFOT, NOUT, LERR, OK )
*
*        DSPSVX
*
         SRNAMT = 'DSPSVX'
         INFOT = 1
         CALL DSPSVX( '/', 'U', 0, 0, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSPSVX( 'N', '/', 0, 0, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSPSVX( 'N', 'U', -1, 0, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSPSVX( 'N', 'U', 0, -1, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSPSVX( 'N', 'U', 2, 0, A, AF, IP, B, 1, X, 2, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSPSVX( 'N', 'U', 2, 0, A, AF, IP, B, 2, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'DSPSVX', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )PATH
      ELSE
         WRITE( NOUT, FMT = 9998 )PATH
      END IF
*
 9999 FORMAT( 1X, A3, ' drivers passed the tests of the error exits' )
 9998 FORMAT( ' *** ', A3, ' drivers failed the tests of the error ',
     $      'exits ***' )
*
      RETURN
*
*     End of DERRVX
*
      END
