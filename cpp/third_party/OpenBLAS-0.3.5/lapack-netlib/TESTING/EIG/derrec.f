*> \brief \b DERREC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERREC( PATH, NUNIT )
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
*> DERREC tests the error exits for the routines for eigen- condition
*> estimation for DOUBLE PRECISION matrices:
*>    DTRSYL, DTREXC, DTRSNA and DTRSEN.
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DERREC( PATH, NUNIT )
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
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( NMAX = 4, ONE = 1.0D0, ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IFST, ILST, INFO, J, M, NT
      DOUBLE PRECISION   SCALE
*     ..
*     .. Local Arrays ..
      LOGICAL            SEL( NMAX )
      INTEGER            IWORK( NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), B( NMAX, NMAX ),
     $                   C( NMAX, NMAX ), S( NMAX ), SEP( NMAX ),
     $                   WI( NMAX ), WORK( NMAX ), WR( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DTREXC, DTRSEN, DTRSNA, DTRSYL
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
      OK = .TRUE.
      NT = 0
*
*     Initialize A, B and SEL
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = ZERO
            B( I, J ) = ZERO
   10    CONTINUE
   20 CONTINUE
      DO 30 I = 1, NMAX
         A( I, I ) = ONE
         SEL( I ) = .TRUE.
   30 CONTINUE
*
*     Test DTRSYL
*
      SRNAMT = 'DTRSYL'
      INFOT = 1
      CALL DTRSYL( 'X', 'N', 1, 0, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTRSYL( 'N', 'X', 1, 0, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTRSYL( 'N', 'N', 0, 0, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTRSYL( 'N', 'N', 1, -1, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DTRSYL( 'N', 'N', 1, 0, -1, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DTRSYL( 'N', 'N', 1, 2, 0, A, 1, B, 1, C, 2, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DTRSYL( 'N', 'N', 1, 0, 2, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL DTRSYL( 'N', 'N', 1, 2, 0, A, 2, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'DTRSYL', INFOT, NOUT, LERR, OK )
      NT = NT + 8
*
*     Test DTREXC
*
      SRNAMT = 'DTREXC'
      IFST = 1
      ILST = 1
      INFOT = 1
      CALL DTREXC( 'X', 1, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTREXC( 'N', -1, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 4
      ILST = 2
      CALL DTREXC( 'N', 2, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DTREXC( 'V', 2, A, 2, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 7
      IFST = 0
      ILST = 1
      CALL DTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 7
      IFST = 2
      CALL DTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 8
      IFST = 1
      ILST = 0
      CALL DTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 8
      ILST = 2
      CALL DTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, WORK, INFO )
      CALL CHKXER( 'DTREXC', INFOT, NOUT, LERR, OK )
      NT = NT + 8
*
*     Test DTRSNA
*
      SRNAMT = 'DTRSNA'
      INFOT = 1
      CALL DTRSNA( 'X', 'A', SEL, 0, A, 1, B, 1, C, 1, S, SEP, 1, M,
     $             WORK, 1, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTRSNA( 'B', 'X', SEL, 0, A, 1, B, 1, C, 1, S, SEP, 1, M,
     $             WORK, 1, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTRSNA( 'B', 'A', SEL, -1, A, 1, B, 1, C, 1, S, SEP, 1, M,
     $             WORK, 1, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DTRSNA( 'V', 'A', SEL, 2, A, 1, B, 1, C, 1, S, SEP, 2, M,
     $             WORK, 2, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DTRSNA( 'B', 'A', SEL, 2, A, 2, B, 1, C, 2, S, SEP, 2, M,
     $             WORK, 2, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DTRSNA( 'B', 'A', SEL, 2, A, 2, B, 2, C, 1, S, SEP, 2, M,
     $             WORK, 2, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL DTRSNA( 'B', 'A', SEL, 1, A, 1, B, 1, C, 1, S, SEP, 0, M,
     $             WORK, 1, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL DTRSNA( 'B', 'S', SEL, 2, A, 2, B, 2, C, 2, S, SEP, 1, M,
     $             WORK, 2, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 16
      CALL DTRSNA( 'B', 'A', SEL, 2, A, 2, B, 2, C, 2, S, SEP, 2, M,
     $             WORK, 1, IWORK, INFO )
      CALL CHKXER( 'DTRSNA', INFOT, NOUT, LERR, OK )
      NT = NT + 9
*
*     Test DTRSEN
*
      SEL( 1 ) = .FALSE.
      SRNAMT = 'DTRSEN'
      INFOT = 1
      CALL DTRSEN( 'X', 'N', SEL, 0, A, 1, B, 1, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTRSEN( 'N', 'X', SEL, 0, A, 1, B, 1, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTRSEN( 'N', 'N', SEL, -1, A, 1, B, 1, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DTRSEN( 'N', 'N', SEL, 2, A, 1, B, 1, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 2, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DTRSEN( 'N', 'V', SEL, 2, A, 2, B, 1, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 15
      CALL DTRSEN( 'N', 'V', SEL, 2, A, 2, B, 2, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 0, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 15
      CALL DTRSEN( 'E', 'V', SEL, 3, A, 3, B, 3, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 15
      CALL DTRSEN( 'V', 'V', SEL, 3, A, 3, B, 3, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 3, IWORK, 2, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 17
      CALL DTRSEN( 'E', 'V', SEL, 2, A, 2, B, 2, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, IWORK, 0, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 17
      CALL DTRSEN( 'V', 'V', SEL, 3, A, 3, B, 3, WR, WI, M, S( 1 ),
     $             SEP( 1 ), WORK, 4, IWORK, 1, INFO )
      CALL CHKXER( 'DTRSEN', INFOT, NOUT, LERR, OK )
      NT = NT + 10
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )PATH, NT
      ELSE
         WRITE( NOUT, FMT = 9998 )PATH
      END IF
*
      RETURN
 9999 FORMAT( 1X, A3, ' routines passed the tests of the error exits (',
     $      I3, ' tests done)' )
 9998 FORMAT( ' *** ', A3, ' routines failed the tests of the error ex',
     $      'its ***' )
*
*     End of DERREC
*
      END
