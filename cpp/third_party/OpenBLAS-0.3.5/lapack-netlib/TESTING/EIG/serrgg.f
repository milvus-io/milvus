*> \brief \b SERRGG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRGG( PATH, NUNIT )
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
*> SERRGG tests the error exits for SGGES, SGGESX, SGGEV, SGGEVX,
*> SGGES3, SGGEV3, SGGGLM, SGGHRD, SGGLSE, SGGQRF, SGGRQF,
*> SGGSVD3, SGGSVP3, SHGEQZ, SORCSD, STGEVC, STGEXC, STGSEN,
*> STGSJA, STGSNA, and STGSYL.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SERRGG( PATH, NUNIT )
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
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            DUMMYK, DUMMYL, I, IFST, ILO, IHI, ILST, INFO,
     $                   J, M, NCYCLE, NT, SDIM, LWORK
      REAL               ANRM, BNRM, DIF, SCALE, TOLA, TOLB
*     ..
*     .. Local Arrays ..
      LOGICAL            BW( NMAX ), SEL( NMAX )
      INTEGER            IW( NMAX ), IDUM(NMAX)
      REAL               A( NMAX, NMAX ), B( NMAX, NMAX ), LS( NMAX ),
     $                   Q( NMAX, NMAX ), R1( NMAX ), R2( NMAX ),
     $                   R3( NMAX ), RCE( 2 ), RCV( 2 ), RS( NMAX ),
     $                   TAU( NMAX ), U( NMAX, NMAX ), V( NMAX, NMAX ),
     $                   W( LW ), Z( NMAX, NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN, SLCTES, SLCTSX
      EXTERNAL           LSAMEN, SLCTES, SLCTSX
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, SGGES, SGGESX, SGGEV, SGGEVX, SGGGLM,
     $                   SGGHRD, SGGLSE, SGGQRF, SGGRQF,
     $                   SHGEQZ, SORCSD, STGEVC, STGEXC, STGSEN, STGSJA,
     $                   STGSNA, STGSYL, SGGES3, SGGEV3, SGGHD3,
     $                   SGGSVD3, SGGSVP3
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
      TOLA = 1.0E0
      TOLB = 1.0E0
      IFST = 1
      ILST = 1
      NT = 0
      LWORK = 1
*
*     Test error exits for the GG path.
*
      IF( LSAMEN( 2, C2, 'GG' ) ) THEN
*
*        SGGHRD
*
         SRNAMT = 'SGGHRD'
         INFOT = 1
         CALL SGGHRD( '/', 'N', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGHRD( 'N', '/', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGHRD( 'N', 'N', -1, 0, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGGHRD( 'N', 'N', 0, 0, 0, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGHRD( 'N', 'N', 0, 1, 1, A, 1, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGHRD( 'N', 'N', 2, 1, 1, A, 1, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGGHRD( 'N', 'N', 2, 1, 1, A, 2, B, 1, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGGHRD( 'V', 'N', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGGHRD( 'N', 'V', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, INFO )
         CALL CHKXER( 'SGGHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        SGGHD3
*
         SRNAMT = 'SGGHD3'
         INFOT = 1
         CALL SGGHD3( '/', 'N', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGHD3( 'N', '/', 0, 1, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGHD3( 'N', 'N', -1, 0, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGGHD3( 'N', 'N', 0, 0, 0, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGHD3( 'N', 'N', 0, 1, 1, A, 1, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGHD3( 'N', 'N', 2, 1, 1, A, 1, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGGHD3( 'N', 'N', 2, 1, 1, A, 2, B, 1, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGGHD3( 'V', 'N', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGGHD3( 'N', 'V', 2, 1, 1, A, 2, B, 2, Q, 1, Z, 1, W, LW,
     $                INFO )
         CALL CHKXER( 'SGGHD3', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        SHGEQZ
*
         SRNAMT = 'SHGEQZ'
         INFOT = 1
         CALL SHGEQZ( '/', 'N', 'N', 0, 1, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SHGEQZ( 'E', '/', 'N', 0, 1, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SHGEQZ( 'E', 'N', '/', 0, 1, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SHGEQZ( 'E', 'N', 'N', -1, 0, 0, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SHGEQZ( 'E', 'N', 'N', 0, 0, 0, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SHGEQZ( 'E', 'N', 'N', 0, 1, 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SHGEQZ( 'E', 'N', 'N', 2, 1, 1, A, 1, B, 2, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SHGEQZ( 'E', 'N', 'N', 2, 1, 1, A, 2, B, 1, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SHGEQZ( 'E', 'V', 'N', 2, 1, 1, A, 2, B, 2, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL SHGEQZ( 'E', 'N', 'V', 2, 1, 1, A, 2, B, 2, R1, R2, R3, Q,
     $                1, Z, 1, W, LW, INFO )
         CALL CHKXER( 'SHGEQZ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        STGEVC
*
         SRNAMT = 'STGEVC'
         INFOT = 1
         CALL STGEVC( '/', 'A', SEL, 0, A, 1, B, 1, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STGEVC( 'R', '/', SEL, 0, A, 1, B, 1, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STGEVC( 'R', 'A', SEL, -1, A, 1, B, 1, Q, 1, Z, 1, 0, M,
     $                W, INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STGEVC( 'R', 'A', SEL, 2, A, 1, B, 2, Q, 1, Z, 2, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STGEVC( 'R', 'A', SEL, 2, A, 2, B, 1, Q, 1, Z, 2, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STGEVC( 'L', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL STGEVC( 'R', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL STGEVC( 'R', 'A', SEL, 2, A, 2, B, 2, Q, 1, Z, 2, 1, M, W,
     $                INFO )
         CALL CHKXER( 'STGEVC', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the GSV path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GSV' ) ) THEN
*
*        SGGSVD3
*
         SRNAMT = 'SGGSVD3'
         INFOT = 1
         CALL SGGSVD3( '/', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGSVD3( 'N', '/', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGSVD3( 'N', 'N', '/', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGGSVD3( 'N', 'N', 'N', -1, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGSVD3( 'N', 'N', 'N', 0, -1, 0, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGGSVD3( 'N', 'N', 'N', 0, 0, -1, DUMMYK, DUMMYL, A, 1, B,
     $        1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGGSVD3( 'N', 'N', 'N', 2, 1, 1, DUMMYK, DUMMYL, A, 1, B,
     $             1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGSVD3( 'N', 'N', 'N', 1, 1, 2, DUMMYK, DUMMYL, A, 1, B,
     $            1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGSVD3( 'U', 'N', 'N', 2, 2, 2, DUMMYK, DUMMYL, A, 2, B,
     $               2, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL SGGSVD3( 'N', 'V', 'N', 1, 1, 2, DUMMYK, DUMMYL, A, 1, B,
     $               2, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL SGGSVD3( 'N', 'N', 'Q', 1, 2, 1, DUMMYK, DUMMYL, A, 1, B,
     $               1, R1, R2, U, 1, V, 1, Q, 1, W, LWORK, IDUM, INFO )
         CALL CHKXER( 'SGGSVD3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        SGGSVP3
*
         SRNAMT = 'SGGSVP3'
         INFOT = 1
         CALL SGGSVP3( '/', 'N', 'N', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGSVP3( 'N', '/', 'N', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGSVP3( 'N', 'N', '/', 0, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGGSVP3( 'N', 'N', 'N', -1, 0, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGSVP3( 'N', 'N', 'N', 0, -1, 0, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGGSVP3( 'N', 'N', 'N', 0, 0, -1, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGGSVP3( 'N', 'N', 'N', 2, 1, 1, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGGSVP3( 'N', 'N', 'N', 1, 2, 1, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGSVP3( 'U', 'N', 'N', 2, 2, 2, A, 2, B, 2, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL SGGSVP3( 'N', 'V', 'N', 1, 2, 1, A, 1, B, 2, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL SGGSVP3( 'N', 'N', 'Q', 1, 1, 2, A, 1, B, 1, TOLA, TOLB,
     $                 DUMMYK, DUMMYL, U, 1, V, 1, Q, 1, IW, TAU, W,
     $                 LWORK, INFO )
         CALL CHKXER( 'SGGSVP3', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        STGSJA
*
         SRNAMT = 'STGSJA'
         INFOT = 1
         CALL STGSJA( '/', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STGSJA( 'N', '/', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STGSJA( 'N', 'N', '/', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STGSJA( 'N', 'N', 'N', -1, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STGSJA( 'N', 'N', 'N', 0, -1, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STGSJA( 'N', 'N', 'N', 0, 0, -1, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STGSJA( 'N', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 0, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL STGSJA( 'N', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                0, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL STGSJA( 'U', 'N', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 0, V, 1, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL STGSJA( 'N', 'V', 'N', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 0, Q, 1, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL STGSJA( 'N', 'N', 'Q', 0, 0, 0, DUMMYK, DUMMYL, A, 1, B,
     $                1, TOLA, TOLB, R1, R2, U, 1, V, 1, Q, 0, W,
     $                NCYCLE, INFO )
         CALL CHKXER( 'STGSJA', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*     Test error exits for the GLM path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GLM' ) ) THEN
*
*        SGGGLM
*
         SRNAMT = 'SGGGLM'
         INFOT = 1
         CALL SGGGLM( -1, 0, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGGLM( 0, -1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGGLM( 0, 1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGGLM( 0, 0, -1, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGGLM( 1, 0, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGGLM( 0, 0, 0, A, 0, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGGLM( 0, 0, 0, A, 1, B, 0, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGGLM( 1, 1, 1, A, 1, B, 1, R1, R2, R3, W, 1, INFO )
         CALL CHKXER( 'SGGGLM', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the LSE path.
*
      ELSE IF( LSAMEN( 3, PATH, 'LSE' ) ) THEN
*
*        SGGLSE
*
         SRNAMT = 'SGGLSE'
         INFOT = 1
         CALL SGGLSE( -1, 0, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGLSE( 0, -1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGLSE( 0, 0, -1, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGLSE( 0, 0, 1, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGLSE( 0, 1, 0, A, 1, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGLSE( 0, 0, 0, A, 0, B, 1, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGLSE( 0, 0, 0, A, 1, B, 0, R1, R2, R3, W, LW, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGLSE( 1, 1, 1, A, 1, B, 1, R1, R2, R3, W, 1, INFO )
         CALL CHKXER( 'SGGLSE', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the CSD path.
*
      ELSE IF( LSAMEN( 3, PATH, 'CSD' ) ) THEN
*
*        SORCSD
*
         SRNAMT = 'SORCSD'
         INFOT = 7
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 -1, 0, 0, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, -1, 0, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, -1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, -1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, -1, A, 1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, -1, A, 1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, -1, A,
     $                 1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         INFOT = 26
         CALL SORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'N',
     $                 1, 1, 1, A, 1, A,
     $                 1, A, 1, A, 1, A,
     $                 A, 1, A, 1, A, 1, A,
     $                 -1, W, LW, IW, INFO )
         CALL CHKXER( 'SORCSD', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*     Test error exits for the GQR path.
*
      ELSE IF( LSAMEN( 3, PATH, 'GQR' ) ) THEN
*
*        SGGQRF
*
         SRNAMT = 'SGGQRF'
         INFOT = 1
         CALL SGGQRF( -1, 0, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGQRF( 0, -1, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGQRF( 0, 0, -1, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGQRF( 0, 0, 0, A, 0, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGGQRF( 0, 0, 0, A, 1, R1, B, 0, R2, W, LW, INFO )
         CALL CHKXER( 'SGGQRF', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGGQRF( 1, 1, 2, A, 1, R1, B, 1, R2, W, 1, INFO )
         CALL CHKXER( 'SGGQRF', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        SGGRQF
*
         SRNAMT = 'SGGRQF'
         INFOT = 1
         CALL SGGRQF( -1, 0, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGRQF( 0, -1, 0, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGRQF( 0, 0, -1, A, 1, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGRQF( 0, 0, 0, A, 0, R1, B, 1, R2, W, LW, INFO )
         CALL CHKXER( 'SGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGGRQF( 0, 0, 0, A, 1, R1, B, 0, R2, W, LW, INFO )
         CALL CHKXER( 'SGGRQF', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGGRQF( 1, 1, 2, A, 1, R1, B, 1, R2, W, 1, INFO )
         CALL CHKXER( 'SGGRQF', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*     Test error exits for the SGS, SGV, SGX, and SXV paths.
*
      ELSE IF( LSAMEN( 3, PATH, 'SGS' ) .OR.
     $         LSAMEN( 3, PATH, 'SGV' ) .OR.
     $         LSAMEN( 3, PATH, 'SGX' ) .OR. LSAMEN( 3, PATH, 'SXV' ) )
     $          THEN
*
*        SGGES
*
         SRNAMT = 'SGGES '
         INFOT = 1
         CALL SGGES( '/', 'N', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGES( 'N', '/', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGES( 'N', 'V', '/', SLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGES( 'N', 'V', 'S', SLCTES, -1, A, 1, B, 1, SDIM, R1,
     $               R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGES( 'N', 'V', 'S', SLCTES, 1, A, 0, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGGES( 'N', 'V', 'S', SLCTES, 1, A, 1, B, 0, SDIM, R1, R2,
     $               R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGGES( 'N', 'V', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 0, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGGES( 'V', 'V', 'S', SLCTES, 2, A, 2, B, 2, SDIM, R1, R2,
     $               R3, Q, 1, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL SGGES( 'N', 'V', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1, R2,
     $               R3, Q, 1, U, 0, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL SGGES( 'V', 'V', 'S', SLCTES, 2, A, 2, B, 2, SDIM, R1, R2,
     $               R3, Q, 2, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         INFOT = 19
         CALL SGGES( 'V', 'V', 'S', SLCTES, 2, A, 2, B, 2, SDIM, R1, R2,
     $               R3, Q, 2, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES ', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        SGGES3
*
         SRNAMT = 'SGGES3'
         INFOT = 1
         CALL SGGES3( '/', 'N', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGES3( 'N', '/', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGES3( 'N', 'V', '/', SLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGES3( 'N', 'V', 'S', SLCTES, -1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGES3( 'N', 'V', 'S', SLCTES, 1, A, 0, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGGES3( 'N', 'V', 'S', SLCTES, 1, A, 1, B, 0, SDIM, R1,
     $                R2, R3, Q, 1, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGGES3( 'N', 'V', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 0, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGGES3( 'V', 'V', 'S', SLCTES, 2, A, 2, B, 2, SDIM, R1,
     $                R2, R3, Q, 1, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL SGGES3( 'N', 'V', 'S', SLCTES, 1, A, 1, B, 1, SDIM, R1,
     $                R2, R3, Q, 1, U, 0, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL SGGES3( 'V', 'V', 'S', SLCTES, 2, A, 2, B, 2, SDIM, R1,
     $                R2, R3, Q, 2, U, 1, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         INFOT = 19
         CALL SGGES3( 'V', 'V', 'S', SLCTES, 2, A, 2, B, 2, SDIM, R1,
     $                R2, R3, Q, 2, U, 2, W, 1, BW, INFO )
         CALL CHKXER( 'SGGES3 ', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        SGGESX
*
         SRNAMT = 'SGGESX'
         INFOT = 1
         CALL SGGESX( '/', 'N', 'S', SLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGESX( 'N', '/', 'S', SLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGESX( 'V', 'V', '/', SLCTSX, 'N', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, '/', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', -1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 1, A, 0, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 1, A, 1, B, 0, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 0, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 0, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                R1, R2, R3, Q, 2, U, 1, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', 2, A, 2, B, 2, SDIM,
     $                R1, R2, R3, Q, 2, U, 2, RCE, RCV, W, 1, IW, 1, BW,
     $                INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'V', 1, A, 1, B, 1, SDIM,
     $                R1, R2, R3, Q, 1, U, 1, RCE, RCV, W, 32, IW, 0,
     $                BW, INFO )
         CALL CHKXER( 'SGGESX', INFOT, NOUT, LERR, OK )
         NT = NT + 13
*
*        SGGEV
*
         SRNAMT = 'SGGEV '
         INFOT = 1
         CALL SGGEV( '/', 'N', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGEV( 'N', '/', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGEV( 'V', 'V', -1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $               W, 1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGEV( 'V', 'V', 1, A, 0, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGEV( 'V', 'V', 1, A, 1, B, 0, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGEV( 'N', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 0, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGEV( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 1, U, 2, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGGEV( 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 0, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGGEV( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGEV( 'V', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1, W,
     $               1, INFO )
         CALL CHKXER( 'SGGEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        SGGEV3
*
         SRNAMT = 'SGGEV3 '
         INFOT = 1
         CALL SGGEV3( '/', 'N', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGEV3( 'N', '/', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGEV3( 'V', 'V', -1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $               W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGEV3( 'V', 'V', 1, A, 0, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGEV3( 'V', 'V', 1, A, 1, B, 0, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGEV3( 'N', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 0, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGGEV3( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 1, U, 2,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGGEV3( 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 0,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGGEV3( 'V', 'V', 2, A, 2, B, 2, R1, R2, R3, Q, 2, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGEV3( 'V', 'V', 1, A, 1, B, 1, R1, R2, R3, Q, 1, U, 1,
     $                W, 1, INFO )
         CALL CHKXER( 'SGGEV3 ', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        SGGEVX
*
         SRNAMT = 'SGGEVX'
         INFOT = 1
         CALL SGGEVX( '/', 'N', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGGEVX( 'N', '/', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGGEVX( 'N', 'N', '/', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGGEVX( 'N', 'N', 'N', '/', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGGEVX( 'N', 'N', 'N', 'N', -1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGGEVX( 'N', 'N', 'N', 'N', 1, A, 0, B, 1, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 0, R1, R2, R3, Q,
     $                1, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                0, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SGGEVX( 'N', 'V', 'N', 'N', 2, A, 2, B, 2, R1, R2, R3, Q,
     $                1, U, 2, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGEVX( 'N', 'N', 'N', 'N', 1, A, 1, B, 1, R1, R2, R3, Q,
     $                1, U, 0, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGGEVX( 'N', 'N', 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q,
     $                2, U, 1, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         INFOT = 26
         CALL SGGEVX( 'N', 'N', 'V', 'N', 2, A, 2, B, 2, R1, R2, R3, Q,
     $                2, U, 2, ILO, IHI, LS, RS, ANRM, BNRM, RCE, RCV,
     $                W, 1, IW, BW, INFO )
         CALL CHKXER( 'SGGEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        STGEXC
*
         SRNAMT = 'STGEXC'
         INFOT = 3
         CALL STGEXC( .TRUE., .TRUE., -1, A, 1, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STGEXC( .TRUE., .TRUE., 1, A, 0, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STGEXC( .TRUE., .TRUE., 1, A, 1, B, 0, Q, 1, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL STGEXC( .FALSE., .TRUE., 1, A, 1, B, 1, Q, 0, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL STGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 0, Z, 1, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL STGEXC( .TRUE., .FALSE., 1, A, 1, B, 1, Q, 1, Z, 0, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL STGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 1, Z, 0, IFST,
     $                ILST, W, 1, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL STGEXC( .TRUE., .TRUE., 1, A, 1, B, 1, Q, 1, Z, 1, IFST,
     $                ILST, W, 0, INFO )
         CALL CHKXER( 'STGEXC', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        STGSEN
*
         SRNAMT = 'STGSEN'
         INFOT = 1
         CALL STGSEN( -1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2,
     $                R3, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, -1, A, 1, B, 1, R1, R2,
     $                R3, Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 0, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 0, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 0, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 0, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL STGSEN( 0, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 22
         CALL STGSEN( 2, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL STGSEN( 0, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW, 0,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL STGSEN( 1, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW, 0,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         INFOT = 24
         CALL STGSEN( 2, .TRUE., .TRUE., SEL, 1, A, 1, B, 1, R1, R2, R3,
     $                Q, 1, Z, 1, M, TOLA, TOLB, RCV, W, 20, IW, 1,
     $                INFO )
         CALL CHKXER( 'STGSEN', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        STGSNA
*
         SRNAMT = 'STGSNA'
         INFOT = 1
         CALL STGSNA( '/', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STGSNA( 'B', '/', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STGSNA( 'B', 'A', SEL, -1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STGSNA( 'B', 'A', SEL, 1, A, 0, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STGSNA( 'B', 'A', SEL, 1, A, 1, B, 0, Q, 1, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 0, U, 1, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL STGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 0, R1, R2,
     $                1, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL STGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                0, M, W, 1, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL STGSNA( 'E', 'A', SEL, 1, A, 1, B, 1, Q, 1, U, 1, R1, R2,
     $                1, M, W, 0, IW, INFO )
         CALL CHKXER( 'STGSNA', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        STGSYL
*
         SRNAMT = 'STGSYL'
         INFOT = 1
         CALL STGSYL( '/', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STGSYL( 'N', -1, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL STGSYL( 'N', 0, 0, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STGSYL( 'N', 0, 1, 0, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STGSYL( 'N', 0, 1, 1, A, 0, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STGSYL( 'N', 0, 1, 1, A, 1, B, 0, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 0, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL STGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 0, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL STGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 0, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL STGSYL( 'N', 0, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 0,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL STGSYL( 'N', 1, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL STGSYL( 'N', 2, 1, 1, A, 1, B, 1, Q, 1, U, 1, V, 1, Z, 1,
     $                SCALE, DIF, W, 1, IW, INFO )
         CALL CHKXER( 'STGSYL', INFOT, NOUT, LERR, OK )
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
*     End of SERRGG
*
      END
