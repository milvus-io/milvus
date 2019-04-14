*> \brief \b ZERREC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERREC( PATH, NUNIT )
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
*> ZERREC tests the error exits for the routines for eigen- condition
*> estimation for DOUBLE PRECISION matrices:
*>    ZTRSYL, ZTREXC, ZTRSNA and ZTRSEN.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZERREC( PATH, NUNIT )
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
      INTEGER            NMAX, LW
      PARAMETER          ( NMAX = 4, LW = NMAX*( NMAX+2 ) )
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D0, ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IFST, ILST, INFO, J, M, NT
      DOUBLE PRECISION   SCALE
*     ..
*     .. Local Arrays ..
      LOGICAL            SEL( NMAX )
      DOUBLE PRECISION   RW( LW ), S( NMAX ), SEP( NMAX )
      COMPLEX*16         A( NMAX, NMAX ), B( NMAX, NMAX ),
     $                   C( NMAX, NMAX ), WORK( LW ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, ZTREXC, ZTRSEN, ZTRSNA, ZTRSYL
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
*     Test ZTRSYL
*
      SRNAMT = 'ZTRSYL'
      INFOT = 1
      CALL ZTRSYL( 'X', 'N', 1, 0, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTRSYL( 'N', 'X', 1, 0, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZTRSYL( 'N', 'N', 0, 0, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZTRSYL( 'N', 'N', 1, -1, 0, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZTRSYL( 'N', 'N', 1, 0, -1, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZTRSYL( 'N', 'N', 1, 2, 0, A, 1, B, 1, C, 2, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL ZTRSYL( 'N', 'N', 1, 0, 2, A, 1, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL ZTRSYL( 'N', 'N', 1, 2, 0, A, 2, B, 1, C, 1, SCALE, INFO )
      CALL CHKXER( 'ZTRSYL', INFOT, NOUT, LERR, OK )
      NT = NT + 8
*
*     Test ZTREXC
*
      SRNAMT = 'ZTREXC'
      IFST = 1
      ILST = 1
      INFOT = 1
      CALL ZTREXC( 'X', 1, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTREXC( 'N', -1, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 4
      ILST = 2
      CALL ZTREXC( 'N', 2, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZTREXC( 'V', 2, A, 2, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 7
      IFST = 0
      ILST = 1
      CALL ZTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 7
      IFST = 2
      CALL ZTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 8
      IFST = 1
      ILST = 0
      CALL ZTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      INFOT = 8
      ILST = 2
      CALL ZTREXC( 'V', 1, A, 1, B, 1, IFST, ILST, INFO )
      CALL CHKXER( 'ZTREXC', INFOT, NOUT, LERR, OK )
      NT = NT + 8
*
*     Test ZTRSNA
*
      SRNAMT = 'ZTRSNA'
      INFOT = 1
      CALL ZTRSNA( 'X', 'A', SEL, 0, A, 1, B, 1, C, 1, S, SEP, 1, M,
     $             WORK, 1, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTRSNA( 'B', 'X', SEL, 0, A, 1, B, 1, C, 1, S, SEP, 1, M,
     $             WORK, 1, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZTRSNA( 'B', 'A', SEL, -1, A, 1, B, 1, C, 1, S, SEP, 1, M,
     $             WORK, 1, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZTRSNA( 'V', 'A', SEL, 2, A, 1, B, 1, C, 1, S, SEP, 2, M,
     $             WORK, 2, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZTRSNA( 'B', 'A', SEL, 2, A, 2, B, 1, C, 2, S, SEP, 2, M,
     $             WORK, 2, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZTRSNA( 'B', 'A', SEL, 2, A, 2, B, 2, C, 1, S, SEP, 2, M,
     $             WORK, 2, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL ZTRSNA( 'B', 'A', SEL, 1, A, 1, B, 1, C, 1, S, SEP, 0, M,
     $             WORK, 1, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL ZTRSNA( 'B', 'S', SEL, 2, A, 2, B, 2, C, 2, S, SEP, 1, M,
     $             WORK, 1, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      INFOT = 16
      CALL ZTRSNA( 'B', 'A', SEL, 2, A, 2, B, 2, C, 2, S, SEP, 2, M,
     $             WORK, 1, RW, INFO )
      CALL CHKXER( 'ZTRSNA', INFOT, NOUT, LERR, OK )
      NT = NT + 9
*
*     Test ZTRSEN
*
      SEL( 1 ) = .FALSE.
      SRNAMT = 'ZTRSEN'
      INFOT = 1
      CALL ZTRSEN( 'X', 'N', SEL, 0, A, 1, B, 1, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 1, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZTRSEN( 'N', 'X', SEL, 0, A, 1, B, 1, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 1, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZTRSEN( 'N', 'N', SEL, -1, A, 1, B, 1, X, M, S( 1 ),
     $             SEP( 1 ), WORK, 1, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZTRSEN( 'N', 'N', SEL, 2, A, 1, B, 1, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 2, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZTRSEN( 'N', 'V', SEL, 2, A, 2, B, 1, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 1, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 14
      CALL ZTRSEN( 'N', 'V', SEL, 2, A, 2, B, 2, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 0, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 14
      CALL ZTRSEN( 'E', 'V', SEL, 3, A, 3, B, 3, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 1, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      INFOT = 14
      CALL ZTRSEN( 'V', 'V', SEL, 3, A, 3, B, 3, X, M, S( 1 ), SEP( 1 ),
     $             WORK, 3, INFO )
      CALL CHKXER( 'ZTRSEN', INFOT, NOUT, LERR, OK )
      NT = NT + 8
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
      RETURN
*
*     End of ZERREC
*
      END
