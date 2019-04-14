*> \brief \b DERRGG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRGG( PATH, NUNIT )
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
*> DERRGG tests the error exits for DGGES, DGGESX, DGGEV,  DGGEVX,
*> DGGGLM, DGGHRD, DGGLSE, DGGQRF, DGGRQF, DGGSVD3,
*> DGGSVP3, DHGEQZ, DORCSD, DTGEVC, DTGEXC, DTGSEN, DTGSJA, DTGSNA,
*> DGGES3, DGGEV3, and DTGSYL.
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DERRGG( PATH, NUNIT )
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
      INTEGER            DUMMYK, DUMMYL, I, IFST, ILO, IHI, ILST, INFO,
     $                   J, M, NCYCLE, NT, SDIM, LWORK
      DOUBLE PRECISION   ANRM, BNRM, DIF, SCALE, TOLA, TOLB
*     ..
*     .. Local Arrays ..
      LOGICAL            BW( NMAX ), SEL( NMAX )
      INTEGER            IW( NMAX ), IDUM(NMAX)
      DOUBLE PRECISION   A( NMAX, NMAX ), B( NMAX, NMAX ), LS( NMAX ),
     $                   Q( NMAX, NMAX ), R1( NMAX ), R2( NMAX ),
     $                   R3( NMAX ), RCE( 2 ), RCV( 2 ), RS( NMAX ),
     $                   TAU( NMAX ), U( NMAX, NMAX ), V( NMAX, NMAX ),
     $                   W( LW ), Z( NMAX, NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            DLCTES, DLCTSX, LSAMEN
      EXTERNAL           DLCTES, DLCTSX, LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DGGES, DGGESX, DGGEV, DGGEVX, DGGGLM,
     $                   DGGHRD, DGGLSE, DGGQRF, DGGRQF,
     $                   DHGEQZ, DORCSD, DTGEVC, DTGEXC, DTGSEN, DTGSJA,
     $                   DTGSNA, DTGSYL, DGGHD3, DGGES3, DGGEV3,
     $                   DGGSVD3, DGGSVP3
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
*        DGGHRD
*
         SRNAMT = 'DGGHRD'
         INFOT = 1
         CALL DGGHRD( '/', 'N', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGHRD( 'N', '/', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGHRD( 'N', 'N', -1, 0, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGGHRD( 'N', 'N', 0, 0, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGHRD( 'N', 'N', 0, 1, 1, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGHRD( 'N', 'N', 2, 1, 1, A, 1, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGGHRD( 'N', 'N', 2, 1, 1, A, 2, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGGHRD( 'V', 'N', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGGHRD( 'N', 'V', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'DGGHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DGGHD3
*
         SRNAMT = 'DGGHD3'
         INFOT = 1
         CALL DGGHD3( '/', 'N', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGHD3( 'N', '/', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGHD3( 'N', 'N', -1, 0, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGGHD3( 'N', 'N', 0, 0, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGHD3( 'N', 'N', 0, 1, 1, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGHD3( 'N', 'N', 2, 1, 1, A, 1, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGGHD3( 'N', 'N', 2, 1, 1, A, 2, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGGHD3( 'V', 'N', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGGHD3( 'N', 'V', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'DGGHD3', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DHGEQZ
*
         SRNAMT = 'DHGEQZ'
         INFOT = 1
         CALL DHGEQZ( '/', 'N', 'N', 0, 1, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DHGEQZ( 'E', '/', 'N', 0, 1, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DHGEQZ( 'E', 'N', '/', 0, 1, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DHGEQZ( 'E', 'N', 'N', -1, 0, 0, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DHGEQZ( 'E', 'N', 'N', 0, 0, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DHGEQZ( 'E', 'N', 'N', 0, 1, 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DHGEQZ( 'E', 'N', 'N', 2, 1, 1, A, 1, B, 2, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DHGEQZ( 'E', 'N', 'N', 2, 1, 1, A, 2, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DHGEQZ( 'E', 'V', 'N', 2, 1, 1, A, 2, B, 2, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DHGEQZ( 'E', 'N', 'V', 2, 1, 1, A, 2, B, 2, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'DHGEQZ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        DTGEVC
*
         SRNAMT = 'DTGEVC'
         INFOT = 1
         CALL DTGEVC( '/', 'A', SEL, 0, A, 1, B, 1, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTGEVC( 'R', '/', SEL, 0, A, 1, B, 1, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTGEVC( 'R', 'A', SEL, -1, A, 1, B, 1, Q, 1, Z, 1, 0, M,
     $                W, INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTGEVC( 'R', 'A', SEL, 2, A, 1, B, 2, Q, 1, Z, 2, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTGEVC( 'R', 'A', SEL, 2, A, 2, B, 1, Q, 1, Z, 2, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTGEVC( 'L', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DTGEVC( 'R', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DTGEVC( 'R', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 2, 1, M, W,
     $                INFO )
         CALL CHKXER( 'DTGEVC', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the GSV path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GSV' ) ) THEN
*
*        DGGSVD3
*
         SRNAMT = 'DGGSVD3'
         INFOT = 1
         CALL DGGSVD3( '/', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGSVD3( 'N', '/', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGSVD3( 'N', 'N', '/', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGGSVD3( 'N', 'N', 'N', -1, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGSVD3( 'N', 'N', 'N', 0, -1, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGGSVD3( 'N', 'N', 'N', 0, 0, -1, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGGSVD3( 'N', 'N', 'N', 2, 1, 1, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGSVD3( 'N', 'N', 'N', 1, 1, 2, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGSVD3( 'U', 'N', 'N', 2, 2, 2, DUMMYK, DUMMYL, A, 2, B,
     $               2, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DGGSVD3( 'N', 'V', 'N', 1, 1, 2, DUMMYK, DUMMYL, A, 1, B,
     $               2, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DGGSVD3( 'N', 'N', 'Q', 1, 2, 1, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'DGGSVD3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        DGGSVP3
*
         SRNAMT = 'DGGSVP3'
         INFOT = 1
         CALL DGGSVP3( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGSVP3( 'N', '/', 'N', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGSVP3( 'N', 'N', '/', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGGSVP3( 'N', 'N', 'N', -1, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGSVP3( 'N', 'N', 'N', 0, -1, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGGSVP3( 'N', 'N', 'N', 0, 0, -1, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGGSVP3( 'N', 'N', 'N', 2, 1, 1, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGGSVP3( 'N', 'N', 'N', 1, 2, 1, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGSVP3( 'U', 'N', 'N', 2, 2, 2, A, 2, B, 2, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DGGSVP3( 'N', 'V', 'N', 1, 2, 1, A, 1, B, 2, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DGGSVP3( 'N', 'N', 'Q', 1, 1, 2, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'DGGSVP3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        DTGSJA
*
         SRNAMT = 'DTGSJA'
         INFOT = 1
         CALL DTGSJA( '/', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTGSJA( 'N', '/', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTGSJA( 'N', 'N', '/', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTGSJA( 'N', 'N', 'N', -1, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTGSJA( 'N', 'N', 'N', 0, -1, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTGSJA( 'N', 'N', 'N', 0, 0, -1, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTGSJA( 'N', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 0, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DTGSJA( 'N', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                0, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DTGSJA( 'U', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 0, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DTGSJA( 'N', 'V', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 0, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL DTGSJA( 'N', 'N', 'Q', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 0, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'DTGSJA', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*     Test error exits for the GLM path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GLM' ) ) THEN
*
*        DGGGLM
*
         SRNAMT = 'DGGGLM'
         INFOT = 1
         CALL DGGGLM( -1, 0, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGGLM( 0, -1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGGLM( 0, 1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGGLM( 0, 0, -1, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGGLM( 1, 0, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGGLM( 0, 0, 0, A, 0, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGGLM( 0, 0, 0, A, 1, B, 0, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGGLM( 1, 1, 1, A, 1, B, 1, R1, R2, R3, W, 1, INFO )
         CALL CHKXER( 'DGGGLM', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the LSE path.
*
      ELSE IF( LSAMEN( 3, PATH, 'LSE' ) ) THEN
*
*        DGGLSE
*
         SRNAMT = 'DGGLSE'
         INFOT = 1
         CALL DGGLSE( -1, 0, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGLSE( 0, -1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGLSE( 0, 0, -1, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGLSE( 0, 0, 1, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGLSE( 0, 1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGLSE( 0, 0, 0, A, 0, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGLSE( 0, 0, 0, A, 1, B, 0, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGLSE( 1, 1, 1, A, 1, B, 1, R1, R2, R3, W, 1, INFO )
         CALL CHKXER( 'DGGLSE', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the CSD path.
*
      ELSE IF( LSAMEN( 3, PATH, 'CSD' ) ) THEN
*
*        DORCSD
*
         SRNAMT = 'DORCSD'
         INFOT = 7
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 -1, 0, 0, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, -1, 0, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, -1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, -1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, -1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, -1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, -1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 26
         CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 -1, W, LW, IW, INFO )
         CALL CHKXER( 'DORCSD', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the GQR path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GQR' ) ) THEN
*
*        DGGQRF
*
         SRNAMT = 'DGGQRF'
         INFOT = 1
         CALL DGGQRF( -1, 0, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGQRF( 0, -1, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGQRF( 0, 0, -1, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGQRF( 0, 0, 0, A, 0, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGGQRF( 0, 0, 0, A, 1, R1, B, 0, R2, W, LW, INFO )
         CALL CHKXER( 'DGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGGQRF( 1, 1, 2, A, 1, R1, B, 1, R2, W, 1, INFO )
         CALL CHKXER( 'DGGQRF', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        DGGRQF
*
         SRNAMT = 'DGGRQF'
         INFOT = 1
         CALL DGGRQF( -1, 0, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGRQF( 0, -1, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGRQF( 0, 0, -1, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGRQF( 0, 0, 0, A, 0, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'DGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGGRQF( 0, 0, 0, A, 1, R1, B, 0, R2, W, LW, INFO )
         CALL CHKXER( 'DGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGGRQF( 1, 1, 2, A, 1, R1, B, 1, R2, W, 1, INFO )
         CALL CHKXER( 'DGGRQF', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*     Test error exits for the DGS, DGV, DGX, and DXV paths.
*
      ELSE IF( LSAMEN( 3, PATH, 'DGS' ) .OR.
     $         LSAMEN( 3, PATH, 'DGV' ) .OR.
     $         LSAMEN( 3, PATH, 'DGX' ) .OR. LSAMEN( 3, PATH, 'DXV' ) )
     $          THEN
*
*        DGGES
*
         SRNAMT = 'DGGES '
         INFOT = 1
         CALL DGGES( '/', 'N', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGES( 'N', '/', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGES( 'N', 'V', '/', DLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGES( 'N', 'V', 'S', DLCTES, -1, A, 1, B, 1, SDIM, R1,
     $               R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGES( 'N', 'V', 'S', DLCTES, 1, A, 0, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGGES( 'N', 'V', 'S', DLCTES, 1, A, 1, B, 0, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGGES( 'N', 'V', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 0, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGGES( 'V', 'V', 'S', DLCTES, 2, A, 2, B, 2, SDIM, R1, R2,
     $               R3, Q, 1, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DGGES( 'N', 'V', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 0, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DGGES( 'V', 'V', 'S', DLCTES, 2, A, 2, B, 2, SDIM, R1, R2,
     $               R3, Q, 2, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 19
         CALL DGGES( 'V', 'V', 'S', DLCTES, 2, A, 2, B, 2, SDIM, R1, R2,
     $               R3, Q, 2, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES ', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        DGGES3
*
         SRNAMT = 'DGGES3 '
         INFOT = 1
         CALL DGGES3( '/', 'N', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGES3( 'N', '/', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGES3( 'N', 'V', '/', DLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGES3( 'N', 'V', 'S', DLCTES, -1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGES3( 'N', 'V', 'S', DLCTES, 1, A, 0, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGGES3( 'N', 'V', 'S', DLCTES, 1, A, 1, B, 0, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGGES3( 'N', 'V', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 0, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGGES3( 'V', 'V', 'S', DLCTES, 2, A, 2, B, 2, SDIM, R1,
     $                R2, R3, Q, 1, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DGGES3( 'N', 'V', 'S', DLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 0, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DGGES3( 'V', 'V', 'S', DLCTES, 2, A, 2, B, 2, SDIM, R1,
     $                R2, R3, Q, 2, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 19
         CALL DGGES3( 'V', 'V', 'S', DLCTES, 2, A, 2, B, 2, SDIM, R1,
     $                R2, R3, Q, 2, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'DGGES3 ', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        DGGESX
*
         SRNAMT = 'DGGESX'
         INFOT = 1
         CALL DGGESX( '/', 'N', 'S', DLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGESX( 'N', '/', 'S', DLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGESX( 'V', 'V', '/', DLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, '/', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', -1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 1, A, 0, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 1, A, 1, B, 0, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 0, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 0, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                R1, R2, R3, Q, 2, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                R1, R2, R3, Q, 2, U, 2, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL DGGESX( 'V', 'V', 'S', DLCTSX, 'V', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 32, IW, 0,
     $                BW, INFO )
         CALL CHKXER( 'DGGESX', INFOT, NOUT, LERR, OK )
         NT = NT + 13
*
*        DGGEV
*
         SRNAMT = 'DGGEV '
         INFOT = 1
         CALL DGGEV( '/', 'N', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGEV( 'N', '/', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGEV( 'V', 'V', -1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $               W, 1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGEV( 'V', 'V', 1, A, 0, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGEV( 'V', 'V', 1, A, 1, B, 0, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGEV( 'N', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 0, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGEV( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 1, U, 2, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGGEV( 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 0, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGGEV( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGEV( 'V', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'DGGEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        DGGEV3
*
         SRNAMT = 'DGGEV3 '
         INFOT = 1
         CALL DGGEV3( '/', 'N', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGEV3( 'N', '/', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGEV3( 'V', 'V', -1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGEV3( 'V', 'V', 1, A, 0, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGEV3( 'V', 'V', 1, A, 1, B, 0, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGEV3( 'N', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 0, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGGEV3( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 1, U, 2,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGGEV3( 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 0,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGGEV3( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGEV3( 'V', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'DGGEV3 ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        DGGEVX
*
         SRNAMT = 'DGGEVX'
         INFOT = 1
         CALL DGGEVX( '/', 'N', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGGEVX( 'N', '/', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGGEVX( 'N', 'N', '/', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGGEVX( 'N', 'N', 'N', '/', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGGEVX( 'N', 'N', 'N', 'N', -1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGGEVX( 'N', 'N', 'N', 'N', 1, A, 0, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 0, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                0, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DGGEVX( 'N', 'V', 'N', 'N', 2, A, 2, B, 2, R1, R2, R3, Q,
     $                1, U, 2, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 0, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGGEVX( 'N', 'N', 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q,
     $                2, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 26
         CALL DGGEVX( 'N', 'N', 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q,
     $                2, U, 2, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'DGGEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        DTGEXC
*
         SRNAMT = 'DTGEXC'
         INFOT = 3
         CALL DTGEXC( .TRUE., .TRUE., -1, A, 1, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTGEXC( .TRUE., .TRUE., 1, A, 0, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DTGEXC( .TRUE., .TRUE., 1, A, 1, B, 0, Q, 1, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DTGEXC( .FALSE., .TRUE., 1, A, 1, B, 1, Q, 0, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DTGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 0, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DTGEXC( .TRUE., .FALSE., 1, A, 1, B, 1, Q, 1, Z, 0, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DTGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 1, Z, 0, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DTGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, W, 0, INFO )
         CALL CHKXER( 'DTGEXC', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        DTGSEN
*
         SRNAMT = 'DTGSEN'
         INFOT = 1
         CALL DTGSEN( -1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2,
     $                R3, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, -1, A, 1, B, 1, R1, R2,
     $                R3, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 0, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 0, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 0, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 0, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL DTGSEN( 0, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL DTGSEN( 2, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL DTGSEN( 0, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW, 0,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL DTGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW, 0,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL DTGSEN( 2, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW, 1,
     $                INFO )
         CALL CHKXER( 'DTGSEN', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        DTGSNA
*
         SRNAMT = 'DTGSNA'
         INFOT = 1
         CALL DTGSNA( '/', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTGSNA( 'B', '/', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTGSNA( 'B', 'A', SEL, -1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTGSNA( 'B', 'A', SEL, 1, A, 0, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTGSNA( 'B', 'A', SEL, 1, A, 1, B, 0, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 0, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 0, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                0, M, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DTGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 0, IW, INFO )
         CALL CHKXER( 'DTGSNA', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DTGSYL
*
         SRNAMT = 'DTGSYL'
         INFOT = 1
         CALL DTGSYL( '/', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTGSYL( 'N', -1, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DTGSYL( 'N', 0, 0, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTGSYL( 'N', 0, 1, 0, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTGSYL( 'N', 0, 1, 1, A, 0, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTGSYL( 'N', 0, 1, 1, A, 1, B, 0, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 0, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 0, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 0, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DTGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 0,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DTGSYL( 'N', 1, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DTGSYL( 'N', 2, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'DTGSYL', INFOT, NOUT, LERR, OK )
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
*     End of DERRGG
*
      END
