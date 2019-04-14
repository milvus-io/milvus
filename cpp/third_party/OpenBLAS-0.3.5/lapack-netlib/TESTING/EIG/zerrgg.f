*> \brief \b ZERRGG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRGG( PATH, NUNIT )
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
*> ZERRGG tests the error exits for ZGGES, ZGGESX, ZGGEV, ZGGEVX,
*> ZGGES3, ZGGEV3, ZGGGLM, ZGGHRD, ZGGLSE, ZGGQRF, ZGGRQF,
*> ZGGSVD3, ZGGSVP3, ZHGEQZ, ZTGEVC, ZTGEXC, ZTGSEN, ZTGSJA,
*> ZTGSNA, ZTGSYL, and ZUNCSD.
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
*> \date June 2016
*
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZERRGG( PATH, NUNIT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX, LW
      PARAMETER          ( NMAX = 3, LW = 6*NMAX )
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            DUMMYK, DUMMYL, I, IFST, IHI, ILO, ILST, INFO,
     $                   J, M, NCYCLE, NT, SDIM, LWORK
      DOUBLE PRECISION   ANRM, BNRM, DIF, SCALE, TOLA, TOLB
*     ..
*     .. Local Arrays ..
      LOGICAL            BW( NMAX ), SEL( NMAX )
      INTEGER            IW( LW ), IDUM(NMAX)
      DOUBLE PRECISION   LS( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   RCE( NMAX ), RCV( NMAX ), RS( NMAX ), RW( LW )
      COMPLEX*16         A( NMAX, NMAX ), ALPHA( NMAX ),
     $                   B( NMAX, NMAX ), BETA( NMAX ), Q( NMAX, NMAX ),
     $                   TAU( NMAX ), U( NMAX, NMAX ), V( NMAX, NMAX ),
     $                   W( LW ), Z( NMAX, NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN, ZLCTES, ZLCTSX
      EXTERNAL           LSAMEN, ZLCTES, ZLCTSX
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, ZGGES,  ZGGESX, ZGGEV,  ZGGEVX, ZGGGLM,
     $                   ZGGHRD, ZGGLSE, ZGGQRF, ZGGRQF,
     $                   ZHGEQZ, ZTGEVC, ZTGEXC, ZTGSEN, ZTGSJA, ZTGSNA,
     $                   ZTGSYL, ZUNCSD, ZGGES3, ZGGEV3, ZGGHD3,
     $                   ZGGSVD3, ZGGSVP3
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
*
*     Set the variables to innocuous values.
*
      DO 20 J = 1, NMAX
         SEL( J ) = .TRUE.
         DO 10 I = 1, NMAX
            A( I, J ) = ZERO
            B( I, J ) = ZERO
   10    CONTINUE
   20 CONTINUE
      DO 30 I = 1, NMAX
         A( I, I ) = ONE
         B( I, I ) = ONE
   30 CONTINUE
      OK = .TRUE.
      TOLA = 1.0D0
      TOLB = 1.0D0
      IFST = 1
      ILST = 1
      NT = 0
      LWORK = 1
*
*     Test error exits for the GG path.
*
      IF( LSAMEN( 2, C2, 'GG' ) ) THEN
*
*        ZGGHRD
*
         SRNAMT = 'ZGGHRD'
         INFOT = 1
         CALL ZGGHRD( '/', 'N', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGHRD( 'N', '/', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGHRD( 'N', 'N', -1, 0, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGGHRD( 'N', 'N', 0, 0, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGHRD( 'N', 'N', 0, 1, 1, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGHRD( 'N', 'N', 2, 1, 1, A, 1, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGGHRD( 'N', 'N', 2, 1, 1, A, 2, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGHRD( 'V', 'N', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGHRD( 'N', 'V', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'ZGGHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        ZGGHD3
*
         SRNAMT = 'ZGGHD3'
         INFOT = 1
         CALL ZGGHD3( '/', 'N', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGHD3( 'N', '/', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGHD3( 'N', 'N', -1, 0, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGGHD3( 'N', 'N', 0, 0, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGHD3( 'N', 'N', 0, 1, 1, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGHD3( 'N', 'N', 2, 1, 1, A, 1, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGGHD3( 'N', 'N', 2, 1, 1, A, 2, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGHD3( 'V', 'N', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGHD3( 'N', 'V', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGHD3', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        ZHGEQZ
*
         SRNAMT = 'ZHGEQZ'
         INFOT = 1
         CALL ZHGEQZ( '/', 'N', 'N', 0, 1, 0, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZHGEQZ( 'E', '/', 'N', 0, 1, 0, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZHGEQZ( 'E', 'N', '/', 0, 1, 0, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZHGEQZ( 'E', 'N', 'N', -1, 0, 0, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZHGEQZ( 'E', 'N', 'N', 0, 0, 0, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZHGEQZ( 'E', 'N', 'N', 0, 1, 1, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZHGEQZ( 'E', 'N', 'N', 2, 1, 1, A, 1, B, 2, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZHGEQZ( 'E', 'N', 'N', 2, 1, 1, A, 2, B, 1, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZHGEQZ( 'E', 'V', 'N', 2, 1, 1, A, 2, B, 2, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZHGEQZ( 'E', 'N', 'V', 2, 1, 1, A, 2, B, 2, ALPHA, BETA,
     $                Q, 1, Z, 1, W, 1, RW, INFO )
         CALL CHKXER( 'ZHGEQZ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        ZTGEVC
*
         SRNAMT = 'ZTGEVC'
         INFOT = 1
         CALL ZTGEVC( '/', 'A', SEL, 0, A, 1, B, 1, Q, 1, Z, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTGEVC( 'R', '/', SEL, 0, A, 1, B, 1, Q, 1, Z, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTGEVC( 'R', 'A', SEL, -1, A, 1, B, 1, Q, 1, Z, 1, 0, M,
     $                W, RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTGEVC( 'R', 'A', SEL, 2, A, 1, B, 2, Q, 1, Z, 2, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTGEVC( 'R', 'A', SEL, 2, A, 2, B, 1, Q, 1, Z, 2, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTGEVC( 'L', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZTGEVC( 'R', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZTGEVC( 'R', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 2, 1, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTGEVC', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the GSV path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GSV' ) ) THEN
*
*        ZGGSVD3
*
         SRNAMT = 'ZGGSVD3'
         INFOT = 1
         CALL ZGGSVD3( '/', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGSVD3( 'N', '/', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGSVD3( 'N', 'N', '/', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGGSVD3( 'N', 'N', 'N', -1, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGSVD3( 'N', 'N', 'N', 0, -1, 0, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGGSVD3( 'N', 'N', 'N', 0, 0, -1, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGGSVD3( 'N', 'N', 'N', 2, 1, 1, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGGSVD3( 'N', 'N', 'N', 1, 1, 2, DUMMYK, DUMMYL, A, 1, B,
     $                 1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZGGSVD3( 'U', 'N', 'N', 2, 2, 2, DUMMYK, DUMMYL, A, 2, B,
     $                 2, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL ZGGSVD3( 'N', 'V', 'N', 2, 2, 2, DUMMYK, DUMMYL, A, 2, B,
     $                 2, R1, R2, U, 2, V, 1, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZGGSVD3( 'N', 'N', 'Q', 2, 2, 2, DUMMYK, DUMMYL, A, 2, B,
     $                 2, R1, R2, U, 2, V, 2, Q, 1, W, LWORK, RW, IDUM,
     $                 INFO )
         CALL CHKXER( 'ZGGSVD3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        ZGGSVP3
*
         SRNAMT = 'ZGGSVP3'
         INFOT = 1
         CALL ZGGSVP3( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGSVP3( 'N', '/', 'N', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGSVP3( 'N', 'N', '/', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGGSVP3( 'N', 'N', 'N', -1, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGSVP3( 'N', 'N', 'N', 0, -1, 0, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGGSVP3( 'N', 'N', 'N', 0, 0, -1, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGGSVP3( 'N', 'N', 'N', 2, 1, 1, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGGSVP3( 'N', 'N', 'N', 1, 2, 1, A, 1, B, 1, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZGGSVP3( 'U', 'N', 'N', 2, 2, 2, A, 2, B, 2, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL ZGGSVP3( 'N', 'V', 'N', 2, 2, 2, A, 2, B, 2, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 2, V, 1, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZGGSVP3( 'N', 'N', 'Q', 2, 2, 2, A, 2, B, 2, TOLA, TOLB,
     $                DUMMYK, DUMMYL, U, 2, V, 2, Q, 1, IW, RW, TAU, W,
     $                LWORK, INFO )
         CALL CHKXER( 'ZGGSVP3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        ZTGSJA
*
         SRNAMT = 'ZTGSJA'
         INFOT = 1
         CALL ZTGSJA( '/', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTGSJA( 'N', '/', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTGSJA( 'N', 'N', '/', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTGSJA( 'N', 'N', 'N', -1, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTGSJA( 'N', 'N', 'N', 0, -1, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTGSJA( 'N', 'N', 'N', 0, 0, -1, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTGSJA( 'N', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 0, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZTGSJA( 'N', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                0, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL ZTGSJA( 'U', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 0, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZTGSJA( 'N', 'V', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 0, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL ZTGSJA( 'N', 'N', 'Q', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 0, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'ZTGSJA', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*     Test error exits for the GLM path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GLM' ) ) THEN
*
*        ZGGGLM
*
         SRNAMT = 'ZGGGLM'
         INFOT = 1
         CALL ZGGGLM( -1, 0, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGGLM( 0, -1, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGGLM( 0, 1, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGGLM( 0, 0, -1, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGGLM( 1, 0, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGGLM( 0, 0, 0, A, 0, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGGLM( 0, 0, 0, A, 1, B, 0, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGGGLM( 1, 1, 1, A, 1, B, 1, TAU, ALPHA, BETA, W, 1,
     $                INFO )
         CALL CHKXER( 'ZGGGLM', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the LSE path.
*
      ELSE IF( LSAMEN( 3, PATH, 'LSE' ) ) THEN
*
*        ZGGLSE
*
         SRNAMT = 'ZGGLSE'
         INFOT = 1
         CALL ZGGLSE( -1, 0, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGLSE( 0, -1, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGLSE( 0, 0, -1, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGLSE( 0, 0, 1, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGLSE( 0, 1, 0, A, 1, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGLSE( 0, 0, 0, A, 0, B, 1, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGLSE( 0, 0, 0, A, 1, B, 0, TAU, ALPHA, BETA, W, LW,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGGLSE( 1, 1, 1, A, 1, B, 1, TAU, ALPHA, BETA, W, 1,
     $                INFO )
         CALL CHKXER( 'ZGGLSE', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the CSD path.
*
      ELSE IF( LSAMEN( 3, PATH, 'CSD' ) ) THEN
*
*        ZUNCSD
*
         SRNAMT = 'ZUNCSD'
         INFOT = 7
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 -1, 0, 0, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, -1, 0, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, -1, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, -1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, -1, A, 1, A, 1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, -1, A, 1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, 1, A, -1, A,
     $                 1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         INFOT = 26
         CALL ZUNCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, RS,
     $                 A, 1, A, 1, A, 1, A,
     $                 -1, W, LW, RW, LW, IW, INFO )
         CALL CHKXER( 'ZUNCSD', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the GQR path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GQR' ) ) THEN
*
*        ZGGQRF
*
         SRNAMT = 'ZGGQRF'
         INFOT = 1
         CALL ZGGQRF( -1, 0, 0, A, 1, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGQRF( 0, -1, 0, A, 1, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGQRF( 0, 0, -1, A, 1, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGQRF( 0, 0, 0, A, 0, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGGQRF( 0, 0, 0, A, 1, ALPHA, B, 0, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGQRF( 1, 1, 2, A, 1, ALPHA, B, 1, BETA, W, 1, INFO )
         CALL CHKXER( 'ZGGQRF', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        ZGGRQF
*
         SRNAMT = 'ZGGRQF'
         INFOT = 1
         CALL ZGGRQF( -1, 0, 0, A, 1, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGRQF( 0, -1, 0, A, 1, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGRQF( 0, 0, -1, A, 1, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGRQF( 0, 0, 0, A, 0, ALPHA, B, 1, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGGRQF( 0, 0, 0, A, 1, ALPHA, B, 0, BETA, W, LW, INFO )
         CALL CHKXER( 'ZGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGRQF( 1, 1, 2, A, 1, ALPHA, B, 1, BETA, W, 1, INFO )
         CALL CHKXER( 'ZGGRQF', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*     Test error exits for the ZGS, ZGV, ZGX, and ZXV paths.
*
      ELSE IF( LSAMEN( 3, PATH, 'ZGS' ) .OR.
     $         LSAMEN( 3, PATH, 'ZGV' ) .OR.
     $         LSAMEN( 3, PATH, 'ZGX' ) .OR. LSAMEN( 3, PATH, 'ZXV' ) )
     $          THEN
*
*        ZGGES
*
         SRNAMT = 'ZGGES '
         INFOT = 1
         CALL ZGGES( '/', 'N', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $               BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGES( 'N', '/', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $               BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGES( 'N', 'V', '/', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $               BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGES( 'N', 'V', 'S', ZLCTES, -1, A, 1, B, 1, SDIM, ALPHA,
     $               BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGES( 'N', 'V', 'S', ZLCTES, 1, A, 0, B, 1, SDIM, ALPHA,
     $               BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGGES( 'N', 'V', 'S', ZLCTES, 1, A, 1, B, 0, SDIM, ALPHA,
     $               BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZGGES( 'N', 'V', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $               BETA, Q, 0, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZGGES( 'V', 'V', 'S', ZLCTES, 2, A, 2, B, 2, SDIM, ALPHA,
     $               BETA, Q, 1, U, 2, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZGGES( 'N', 'V', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $               BETA, Q, 1, U, 0, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZGGES( 'V', 'V', 'S', ZLCTES, 2, A, 2, B, 2, SDIM, ALPHA,
     $               BETA, Q, 2, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL ZGGES( 'V', 'V', 'S', ZLCTES, 2, A, 2, B, 2, SDIM, ALPHA,
     $               BETA, Q, 2, U, 2, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES ', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        ZGGES3
*
         SRNAMT = 'ZGGES3'
         INFOT = 1
         CALL ZGGES3( '/', 'N', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $                BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGES3( 'N', '/', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $                BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGES3( 'N', 'V', '/', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $                BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGES3( 'N', 'V', 'S', ZLCTES, -1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGES3( 'N', 'V', 'S', ZLCTES, 1, A, 0, B, 1, SDIM, ALPHA,
     $                BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGGES3( 'N', 'V', 'S', ZLCTES, 1, A, 1, B, 0, SDIM, ALPHA,
     $                BETA, Q, 1, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZGGES3( 'N', 'V', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $                BETA, Q, 0, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZGGES3( 'V', 'V', 'S', ZLCTES, 2, A, 2, B, 2, SDIM, ALPHA,
     $                BETA, Q, 1, U, 2, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZGGES3( 'N', 'V', 'S', ZLCTES, 1, A, 1, B, 1, SDIM, ALPHA,
     $                BETA, Q, 1, U, 0, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZGGES3( 'V', 'V', 'S', ZLCTES, 2, A, 2, B, 2, SDIM, ALPHA,
     $                BETA, Q, 2, U, 1, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL ZGGES3( 'V', 'V', 'S', ZLCTES, 2, A, 2, B, 2, SDIM, ALPHA,
     $                BETA, Q, 2, U, 2, W, 1, RW, BW, INFO )
         CALL CHKXER( 'ZGGES3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        ZGGESX
*
         SRNAMT = 'ZGGESX'
         INFOT = 1
         CALL ZGGESX( '/', 'N', 'S', ZLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGESX( 'N', '/', 'S', ZLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGESX( 'V', 'V', '/', ZLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, '/', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', -1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 1, A, 0, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 1, A, 1, B, 0, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 0, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 0, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                ALPHA, BETA, Q, 2, U, 1, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                ALPHA, BETA, Q, 2, U, 2, RCE, RCV, W, 1, RW, IW,
     $                1, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL ZGGESX( 'V', 'V', 'S', ZLCTSX, 'V', 1, A, 1, B, 1, SDIM,
     $                ALPHA, BETA, Q, 1, U, 1, RCE, RCV, W, 32, RW, IW,
     $                0, BW, INFO )
         CALL CHKXER( 'ZGGESX', INFOT, NOUT, LERR, OK )
         NT = NT + 13
*
*        ZGGEV
*
         SRNAMT = 'ZGGEV '
         INFOT = 1
         CALL ZGGEV( '/', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGEV( 'N', '/', 1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGEV( 'V', 'V', -1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGEV( 'V', 'V', 1, A, 0, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGEV( 'V', 'V', 1, A, 1, B, 0, ALPHA, BETA, Q, 1, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGEV( 'N', 'V', 1, A, 1, B, 1, ALPHA, BETA, Q, 0, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGEV( 'V', 'V', 2, A, 2, B, 2, ALPHA, BETA, Q, 1, U, 2,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGEV( 'V', 'N', 2, A, 2, B, 2, ALPHA, BETA, Q, 2, U, 0,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGEV( 'V', 'V', 2, A, 2, B, 2, ALPHA, BETA, Q, 2, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGGEV( 'V', 'V', 1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $               W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        ZGGEV3
*
         SRNAMT = 'ZGGEV3'
         INFOT = 1
         CALL ZGGEV3( '/', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGEV3( 'N', '/', 1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGEV3( 'V', 'V', -1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGEV3( 'V', 'V', 1, A, 0, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGEV3( 'V', 'V', 1, A, 1, B, 0, ALPHA, BETA, Q, 1, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGEV3( 'N', 'V', 1, A, 1, B, 1, ALPHA, BETA, Q, 0, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGGEV3( 'V', 'V', 2, A, 2, B, 2, ALPHA, BETA, Q, 1, U, 2,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGEV3( 'V', 'N', 2, A, 2, B, 2, ALPHA, BETA, Q, 2, U, 0,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGEV3( 'V', 'V', 2, A, 2, B, 2, ALPHA, BETA, Q, 2, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGGEV3( 'V', 'V', 1, A, 1, B, 1, ALPHA, BETA, Q, 1, U, 1,
     $                W, 1, RW, INFO )
         CALL CHKXER( 'ZGGEV3', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        ZGGEVX
*
         SRNAMT = 'ZGGEVX'
         INFOT = 1
         CALL ZGGEVX( '/', 'N', 'N', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGGEVX( 'N', '/', 'N', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGGEVX( 'N', 'N', '/', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGGEVX( 'N', 'N', 'N', '/', 1, A, 1, B, 1, ALPHA, BETA, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGGEVX( 'N', 'N', 'N', 'N', -1, A, 1, B, 1, ALPHA, BETA,
     $                Q, 1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE,
     $                RCV, W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGGEVX( 'N', 'N', 'N', 'N', 1, A, 0, B, 1, ALPHA, BETA, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 0, ALPHA, BETA, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q,
     $                0, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGGEVX( 'N', 'V', 'N', 'N', 2, A, 2, B, 2, ALPHA, BETA, Q,
     $                1, U, 2, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 1, ALPHA, BETA, Q,
     $                1, U, 0, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGGEVX( 'N', 'N', 'V', 'N', 2, A, 2, B, 2, ALPHA, BETA, Q,
     $                2, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 25
         CALL ZGGEVX( 'N', 'N', 'V', 'N', 2, A, 2, B, 2, ALPHA, BETA, Q,
     $                2, U, 2, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 0, RW, IW, BW, INFO )
         CALL CHKXER( 'ZGGEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        ZTGEXC
*
         SRNAMT = 'ZTGEXC'
         INFOT = 3
         CALL ZTGEXC( .TRUE., .TRUE., -1, A, 1, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTGEXC( .TRUE., .TRUE., 1, A, 0, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZTGEXC( .TRUE., .TRUE., 1, A, 1, B, 0, Q, 1, Z, 1, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZTGEXC( .FALSE., .TRUE., 1, A, 1, B, 1, Q, 0, Z, 1, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZTGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 0, Z, 1, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZTGEXC( .TRUE., .FALSE., 1, A, 1, B, 1, Q, 1, Z, 0, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZTGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 1, Z, 0, IFST,
     $                ILST, INFO )
         CALL CHKXER( 'ZTGEXC', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        ZTGSEN
*
         SRNAMT = 'ZTGSEN'
         INFOT = 1
         CALL ZTGSEN( -1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZTGSEN( 1, .TRUE., .TRUE., SEL, -1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 0, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 0, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 0, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 0, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL ZTGSEN( 3, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, -5, IW,
     $                1, INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 23
         CALL ZTGSEN( 0, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW,
     $                0, INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 23
         CALL ZTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW,
     $                0, INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 23
         CALL ZTGSEN( 5, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, ALPHA,
     $                BETA, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW,
     $                1, INFO )
         CALL CHKXER( 'ZTGSEN', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        ZTGSNA
*
         SRNAMT = 'ZTGSNA'
         INFOT = 1
         CALL ZTGSNA( '/', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTGSNA( 'B', '/', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTGSNA( 'B', 'A', SEL, -1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTGSNA( 'B', 'A', SEL, 1, A, 0, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTGSNA( 'B', 'A', SEL, 1, A, 1, B, 0, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 0, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 0, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                0, M, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL ZTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 0, IW, INFO )
         CALL CHKXER( 'ZTGSNA', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        ZTGSYL
*
         SRNAMT = 'ZTGSYL'
         INFOT = 1
         CALL ZTGSYL( '/', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTGSYL( 'N', -1, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZTGSYL( 'N', 0, 0, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTGSYL( 'N', 0, 1, 0, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTGSYL( 'N', 0, 1, 1, A, 0, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTGSYL( 'N', 0, 1, 1, A, 1, B, 0, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 0, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 0, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 0, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL ZTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 0,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZTGSYL( 'N', 1, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZTGSYL( 'N', 2, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'ZTGSYL', INFOT, NOUT, LERR, OK )
         NT = NT + 12
      END IF
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )PATH, NT
      ELSE
         WRITE( NOUT, FMT = 9998 )PATH
      END IF
*
 9999 FORMAT( 1X, A3, ' routines passed the tests of the error exits (',
     $      I3, ' tests done)' )
 9998 FORMAT( ' *** ', A3, ' routines failed the tests of the error ',
     $      'exits ***' )
*
      RETURN
*
*     End of ZERRGG
*
      END
