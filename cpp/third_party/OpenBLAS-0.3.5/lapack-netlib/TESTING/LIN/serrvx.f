*> \brief \b SERRVX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRVX( PATH, NUNIT )
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
*> SERRVX tests the error exits for the REAL driver routines
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
*> \date November 2017
*
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRVX( PATH, NUNIT )
*
*  -- LAPACK test routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
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
      INTEGER            I, INFO, J
      REAL               RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      REAL               A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   C( NMAX ), E( NMAX ), R( NMAX ), R1( NMAX ),
     $                   R2( NMAX ), W( 2*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, SGBSV, SGBSVX, SGESV, SGESVX, SGTSV,
     $                   SGTSVX, SPBSV, SPBSVX, SPOSV, SPOSVX, SPPSV,
     $                   SPPSVX, SPTSV, SPTSVX, SSPSV, SSPSVX, SSYSV,
     $                   SSYSV_AA, SSYSV_RK, SSYSV_ROOK, SSYSVX,
     $                   SSYSV_AA_2STAGE
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
         B( J ) = 0.E+0
         E( J ) = 0.E+0
         R1( J ) = 0.E+0
         R2( J ) = 0.E+0
         W( J ) = 0.E+0
         X( J ) = 0.E+0
         C( J ) = 0.E+0
         R( J ) = 0.E+0
         IP( J ) = J
   20 CONTINUE
      EQ = ' '
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'GE' ) ) THEN
*
*        SGESV
*
         SRNAMT = 'SGESV '
         INFOT = 1
         CALL SGESV( -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGESV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGESV( 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGESV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGESV( 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'SGESV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGESV( 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'SGESV ', INFOT, NOUT, LERR, OK )
*
*        SGESVX
*
         SRNAMT = 'SGESVX'
         INFOT = 1
         CALL SGESVX( '/', 'N', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGESVX( 'N', '/', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGESVX( 'N', 'N', -1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGESVX( 'N', 'N', 0, -1, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGESVX( 'N', 'N', 2, 1, A, 1, AF, 2, IP, EQ, R, C, B, 2,
     $                X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGESVX( 'N', 'N', 2, 1, A, 2, AF, 1, IP, EQ, R, C, B, 2,
     $                X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = '/'
         CALL SGESVX( 'F', 'N', 0, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ = 'R'
         CALL SGESVX( 'F', 'N', 1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         EQ = 'C'
         CALL SGESVX( 'F', 'N', 1, 0, A, 1, AF, 1, IP, EQ, R, C, B, 1,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGESVX( 'N', 'N', 2, 1, A, 2, AF, 2, IP, EQ, R, C, B, 1,
     $                X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGESVX( 'N', 'N', 2, 1, A, 2, AF, 2, IP, EQ, R, C, B, 2,
     $                X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGESVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'GB' ) ) THEN
*
*        SGBSV
*
         SRNAMT = 'SGBSV '
         INFOT = 1
         CALL SGBSV( -1, 0, 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBSV( 1, -1, 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBSV( 1, 0, -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBSV( 0, 0, 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGBSV( 1, 1, 1, 0, A, 3, IP, B, 1, INFO )
         CALL CHKXER( 'SGBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGBSV( 2, 0, 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SGBSV ', INFOT, NOUT, LERR, OK )
*
*        SGBSVX
*
         SRNAMT = 'SGBSVX'
         INFOT = 1
         CALL SGBSVX( '/', 'N', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGBSVX( 'N', '/', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGBSVX( 'N', 'N', -1, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGBSVX( 'N', 'N', 1, -1, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGBSVX( 'N', 'N', 1, 0, -1, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGBSVX( 'N', 'N', 0, 0, 0, -1, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGBSVX( 'N', 'N', 1, 1, 1, 0, A, 2, AF, 4, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGBSVX( 'N', 'N', 1, 1, 1, 0, A, 3, AF, 3, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         EQ = '/'
         CALL SGBSVX( 'F', 'N', 0, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         EQ = 'R'
         CALL SGBSVX( 'F', 'N', 1, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         EQ = 'C'
         CALL SGBSVX( 'F', 'N', 1, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGBSVX( 'N', 'N', 2, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 1, X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL SGBSVX( 'N', 'N', 2, 0, 0, 0, A, 1, AF, 1, IP, EQ, R, C,
     $                B, 2, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGBSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'GT' ) ) THEN
*
*        SGTSV
*
         SRNAMT = 'SGTSV '
         INFOT = 1
         CALL SGTSV( -1, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ), B, 1,
     $               INFO )
         CALL CHKXER( 'SGTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGTSV( 0, -1, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ), B, 1,
     $               INFO )
         CALL CHKXER( 'SGTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGTSV( 2, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ), B, 1, INFO )
         CALL CHKXER( 'SGTSV ', INFOT, NOUT, LERR, OK )
*
*        SGTSVX
*
         SRNAMT = 'SGTSVX'
         INFOT = 1
         CALL SGTSVX( '/', 'N', 0, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGTSVX( 'N', '/', 0, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGTSVX( 'N', 'N', -1, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGTSVX( 'N', 'N', 0, -1, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGTSVX( 'N', 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 1, X, 2, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGTSVX( 'N', 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), A( 1, 3 ),
     $                AF( 1, 1 ), AF( 1, 2 ), AF( 1, 3 ), AF( 1, 4 ),
     $                IP, B, 2, X, 1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SGTSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        SPOSV
*
         SRNAMT = 'SPOSV '
         INFOT = 1
         CALL SPOSV( '/', 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOSV( 'U', -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPOSV( 'U', 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPOSV( 'U', 2, 0, A, 1, B, 2, INFO )
         CALL CHKXER( 'SPOSV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SPOSV( 'U', 2, 0, A, 2, B, 1, INFO )
         CALL CHKXER( 'SPOSV ', INFOT, NOUT, LERR, OK )
*
*        SPOSVX
*
         SRNAMT = 'SPOSVX'
         INFOT = 1
         CALL SPOSVX( '/', 'U', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPOSVX( 'N', '/', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPOSVX( 'N', 'U', -1, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPOSVX( 'N', 'U', 0, -1, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPOSVX( 'N', 'U', 2, 0, A, 1, AF, 2, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SPOSVX( 'N', 'U', 2, 0, A, 2, AF, 1, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         EQ = '/'
         CALL SPOSVX( 'F', 'U', 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = 'Y'
         CALL SPOSVX( 'F', 'U', 1, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SPOSVX( 'N', 'U', 2, 0, A, 2, AF, 2, EQ, C, B, 1, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SPOSVX( 'N', 'U', 2, 0, A, 2, AF, 2, EQ, C, B, 2, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPOSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PP' ) ) THEN
*
*        SPPSV
*
         SRNAMT = 'SPPSV '
         INFOT = 1
         CALL SPPSV( '/', 0, 0, A, B, 1, INFO )
         CALL CHKXER( 'SPPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPSV( 'U', -1, 0, A, B, 1, INFO )
         CALL CHKXER( 'SPPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPPSV( 'U', 0, -1, A, B, 1, INFO )
         CALL CHKXER( 'SPPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPPSV( 'U', 2, 0, A, B, 1, INFO )
         CALL CHKXER( 'SPPSV ', INFOT, NOUT, LERR, OK )
*
*        SPPSVX
*
         SRNAMT = 'SPPSVX'
         INFOT = 1
         CALL SPPSVX( '/', 'U', 0, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPPSVX( 'N', '/', 0, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPPSVX( 'N', 'U', -1, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPPSVX( 'N', 'U', 0, -1, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         EQ = '/'
         CALL SPPSVX( 'F', 'U', 0, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         EQ = 'Y'
         CALL SPPSVX( 'F', 'U', 1, 0, A, AF, EQ, C, B, 1, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SPPSVX( 'N', 'U', 2, 0, A, AF, EQ, C, B, 1, X, 2, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SPPSVX( 'N', 'U', 2, 0, A, AF, EQ, C, B, 2, X, 1, RCOND,
     $                R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPPSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PB' ) ) THEN
*
*        SPBSV
*
         SRNAMT = 'SPBSV '
         INFOT = 1
         CALL SPBSV( '/', 0, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBSV( 'U', -1, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBSV( 'U', 1, -1, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPBSV( 'U', 0, 0, -1, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPBSV( 'U', 1, 1, 0, A, 1, B, 2, INFO )
         CALL CHKXER( 'SPBSV ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SPBSV( 'U', 2, 0, 0, A, 1, B, 1, INFO )
         CALL CHKXER( 'SPBSV ', INFOT, NOUT, LERR, OK )
*
*        SPBSVX
*
         SRNAMT = 'SPBSVX'
         INFOT = 1
         CALL SPBSVX( '/', 'U', 0, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPBSVX( 'N', '/', 0, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPBSVX( 'N', 'U', -1, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X,
     $                1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SPBSVX( 'N', 'U', 1, -1, 0, A, 1, AF, 1, EQ, C, B, 1, X,
     $                1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SPBSVX( 'N', 'U', 0, 0, -1, A, 1, AF, 1, EQ, C, B, 1, X,
     $                1, RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SPBSVX( 'N', 'U', 1, 1, 0, A, 1, AF, 2, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SPBSVX( 'N', 'U', 1, 1, 0, A, 2, AF, 1, EQ, C, B, 2, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         EQ = '/'
         CALL SPBSVX( 'F', 'U', 0, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         EQ = 'Y'
         CALL SPBSVX( 'F', 'U', 1, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SPBSVX( 'N', 'U', 2, 0, 0, A, 1, AF, 1, EQ, C, B, 1, X, 2,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SPBSVX( 'N', 'U', 2, 0, 0, A, 1, AF, 1, EQ, C, B, 2, X, 1,
     $                RCOND, R1, R2, W, IW, INFO )
         CALL CHKXER( 'SPBSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'PT' ) ) THEN
*
*        SPTSV
*
         SRNAMT = 'SPTSV '
         INFOT = 1
         CALL SPTSV( -1, 0, A( 1, 1 ), A( 1, 2 ), B, 1, INFO )
         CALL CHKXER( 'SPTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPTSV( 0, -1, A( 1, 1 ), A( 1, 2 ), B, 1, INFO )
         CALL CHKXER( 'SPTSV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SPTSV( 2, 0, A( 1, 1 ), A( 1, 2 ), B, 1, INFO )
         CALL CHKXER( 'SPTSV ', INFOT, NOUT, LERR, OK )
*
*        SPTSVX
*
         SRNAMT = 'SPTSVX'
         INFOT = 1
         CALL SPTSVX( '/', 0, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'SPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SPTSVX( 'N', -1, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'SPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SPTSVX( 'N', 0, -1, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'SPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SPTSVX( 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 1, X, 2, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'SPTSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SPTSVX( 'N', 2, 0, A( 1, 1 ), A( 1, 2 ), AF( 1, 1 ),
     $                AF( 1, 2 ), B, 2, X, 1, RCOND, R1, R2, W, INFO )
         CALL CHKXER( 'SPTSVX', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SY' ) ) THEN
*
*        SSYSV
*
         SRNAMT = 'SSYSV '
         INFOT = 1
         CALL SSYSV( '/', 0, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYSV( 'U', -1, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYSV( 'U', 0, -1, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYSV( 'U', 2, 0, A, 2, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SSYSV( 'U', 0, 0, A, 1, IP, B, 1, W, 0, INFO )
         CALL CHKXER( 'SSYSV ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SSYSV( 'U', 0, 0, A, 1, IP, B, 1, W, -2, INFO )
         CALL CHKXER( 'SSYSV ', INFOT, NOUT, LERR, OK )
*
*        SSYSVX
*
         SRNAMT = 'SSYSVX'
         INFOT = 1
         CALL SSYSVX( '/', 'U', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYSVX( 'N', '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYSVX( 'N', 'U', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYSVX( 'N', 'U', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1,
     $                RCOND, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SSYSVX( 'N', 'U', 2, 0, A, 1, AF, 2, IP, B, 2, X, 2,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYSVX( 'N', 'U', 2, 0, A, 2, AF, 1, IP, B, 2, X, 2,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SSYSVX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, B, 1, X, 2,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SSYSVX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, B, 2, X, 1,
     $                RCOND, R1, R2, W, 4, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL SSYSVX( 'N', 'U', 2, 0, A, 2, AF, 2, IP, B, 2, X, 2,
     $                RCOND, R1, R2, W, 3, IW, INFO )
         CALL CHKXER( 'SSYSVX', INFOT, NOUT, LERR, OK )
*
*
      ELSE IF( LSAMEN( 2, C2, 'SR' ) ) THEN
*
*        SSYSV_ROOK
*
         SRNAMT = 'SSYSV_ROOK'
         INFOT = 1
         CALL SSYSV_ROOK( '/', 0, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYSV_ROOK( 'U', -1, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYSV_ROOK( 'U', 0, -1, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYSV_ROOK( 'U', 2, 0, A, 2, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SSYSV_ROOK( 'U', 0, 0, A, 1, IP, B, 1, W, 0, INFO )
         CALL CHKXER( 'SSYSV_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SSYSV_ROOK( 'U', 0, 0, A, 1, IP, B, 1, W, -2, INFO )
         CALL CHKXER( 'SSYSV_ROOK', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SK' ) ) THEN
*
*        SSYSV_RK
*
*        Test error exits of the driver that uses factorization
*        of a symmetric indefinite matrix with rook
*        (bounded Bunch-Kaufman) pivoting with the new storage
*        format for factors L ( or U) and D.
*
*        L (or U) is stored in A, diagonal of D is stored on the
*        diagonal of A, subdiagonal of D is stored in a separate array E.
*
         SRNAMT = 'SSYSV_RK'
         INFOT = 1
         CALL SSYSV_RK( '/', 0, 0, A, 1, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYSV_RK( 'U', -1, 0, A, 1, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYSV_RK( 'U', 0, -1, A, 1, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSYSV_RK( 'U', 2, 0, A, 1, E, IP, B, 2, W, 1, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SSYSV_RK( 'U', 2, 0, A, 2, E, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SSYSV_RK( 'U', 0, 0, A, 1, E, IP, B, 1, W, 0, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SSYSV_RK( 'U', 0, 0, A, 1, E, IP, B, 1, W, -2, INFO )
         CALL CHKXER( 'SSYSV_RK', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SA' ) ) THEN
*
*        SSYSV_AA
*
         SRNAMT = 'SSYSV_AA'
         INFOT = 1
         CALL SSYSV_AA( '/', 0, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYSV_AA( 'U', -1, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYSV_AA( 'U', 0, -1, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYSV_AA( 'U', 2, 0, A, 2, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'S2' ) ) THEN
*
*        DSYSV_AASEN_2STAGE
*
         SRNAMT = 'SSYSV_AA_2STAGE'
         INFOT = 1
         CALL SSYSV_AA_2STAGE( '/', 0, 0, A, 1, A, 1, IP, IP, B, 1,
     $                         W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYSV_AA_2STAGE( 'U', -1, 0, A, 1, A, 1, IP, IP, B, 1,
     $                         W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYSV_AA_2STAGE( 'U', 0, -1, A, 1, A, 1, IP, IP, B, 1,
     $                         W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSYSV_AA_2STAGE( 'U', 2, 1, A, 1, A, 1, IP, IP, B, 1,
     $                         W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SSYSV_AA_2STAGE( 'U', 2, 1, A, 2, A, 2, IP, IP, B, 1,
     $                         W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYSV_AA_2STAGE( 'U', 2, 1, A, 2, A, 1, IP, IP, B, 2,
     $                         W, 1, INFO )
         CALL CHKXER( 'SSYSV_AA_2STAGE', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SP' ) ) THEN
*
*        SSPSV
*
         SRNAMT = 'SSPSV '
         INFOT = 1
         CALL SSPSV( '/', 0, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPSV( 'U', -1, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSPSV( 'U', 0, -1, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPSV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSPSV( 'U', 2, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPSV ', INFOT, NOUT, LERR, OK )
*
*        SSPSVX
*
         SRNAMT = 'SSPSVX'
         INFOT = 1
         CALL SSPSVX( '/', 'U', 0, 0, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPSVX( 'N', '/', 0, 0, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSPSVX( 'N', 'U', -1, 0, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSPSVX( 'N', 'U', 0, -1, A, AF, IP, B, 1, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SSPSVX( 'N', 'U', 2, 0, A, AF, IP, B, 1, X, 2, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SSPSVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SSPSVX( 'N', 'U', 2, 0, A, AF, IP, B, 2, X, 1, RCOND, R1,
     $                R2, W, IW, INFO )
         CALL CHKXER( 'SSPSVX', INFOT, NOUT, LERR, OK )
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
*     End of SERRVX
*
      END
