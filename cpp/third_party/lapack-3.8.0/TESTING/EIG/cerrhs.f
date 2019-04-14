*> \brief \b CERRHS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRHS( PATH, NUNIT )
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
*> CERRHS tests the error exits for CGEBAK, CGEBAL, CGEHRD, CUNGHR,
*> CUNMHR, CHSEQR, CHSEIN, and CTREVC.
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CERRHS( PATH, NUNIT )
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
      PARAMETER          ( NMAX = 3, LW = NMAX*NMAX )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, IHI, ILO, INFO, J, M, NT
*     ..
*     .. Local Arrays ..
      LOGICAL            SEL( NMAX )
      INTEGER            IFAILL( NMAX ), IFAILR( NMAX )
      REAL               RW( NMAX ), S( NMAX )
      COMPLEX            A( NMAX, NMAX ), C( NMAX, NMAX ), TAU( NMAX ),
     $                   VL( NMAX, NMAX ), VR( NMAX, NMAX ), W( LW ),
     $                   X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, CGEBAK, CGEBAL, CGEHRD, CHSEIN, CHSEQR,
     $                   CUNGHR, CUNMHR, CTREVC
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          REAL
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
         DO 10 I = 1, NMAX
            A( I, J ) = 1. / REAL( I+J )
   10    CONTINUE
         SEL( J ) = .TRUE.
   20 CONTINUE
      OK = .TRUE.
      NT = 0
*
*     Test error exits of the nonsymmetric eigenvalue routines.
*
      IF( LSAMEN( 2, C2, 'HS' ) ) THEN
*
*        CGEBAL
*
         SRNAMT = 'CGEBAL'
         INFOT = 1
         CALL CGEBAL( '/', 0, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'CGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGEBAL( 'N', -1, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'CGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGEBAL( 'N', 2, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'CGEBAL', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        CGEBAK
*
         SRNAMT = 'CGEBAK'
         INFOT = 1
         CALL CGEBAK( '/', 'R', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGEBAK( 'N', '/', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGEBAK( 'N', 'R', -1, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGEBAK( 'N', 'R', 0, 0, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CGEBAK( 'N', 'R', 0, 2, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGEBAK( 'N', 'R', 2, 2, 1, S, 0, A, 2, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGEBAK( 'N', 'R', 0, 1, 1, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CGEBAK( 'N', 'R', 0, 1, 0, S, -1, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CGEBAK( 'N', 'R', 2, 1, 2, S, 0, A, 1, INFO )
         CALL CHKXER( 'CGEBAK', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        CGEHRD
*
         SRNAMT = 'CGEHRD'
         INFOT = 1
         CALL CGEHRD( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGEHRD( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CGEHRD( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGEHRD( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CGEHRD( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CGEHRD( 2, 1, 1, A, 1, TAU, W, 2, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CGEHRD( 2, 1, 2, A, 2, TAU, W, 1, INFO )
         CALL CHKXER( 'CGEHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        CUNGHR
*
         SRNAMT = 'CUNGHR'
         INFOT = 1
         CALL CUNGHR( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CUNGHR( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CUNGHR( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CUNGHR( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CUNGHR( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CUNGHR( 2, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CUNGHR( 3, 1, 3, A, 3, TAU, W, 1, INFO )
         CALL CHKXER( 'CUNGHR', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        CUNMHR
*
         SRNAMT = 'CUNMHR'
         INFOT = 1
         CALL CUNMHR( '/', 'N', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CUNMHR( 'L', '/', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CUNMHR( 'L', 'N', -1, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CUNMHR( 'L', 'N', 0, -1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CUNMHR( 'L', 'N', 0, 0, 0, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CUNMHR( 'L', 'N', 0, 0, 2, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CUNMHR( 'L', 'N', 1, 2, 2, 1, A, 1, TAU, C, 1, W, 2,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CUNMHR( 'R', 'N', 2, 1, 2, 1, A, 1, TAU, C, 2, W, 2,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CUNMHR( 'L', 'N', 1, 1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CUNMHR( 'L', 'N', 0, 1, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CUNMHR( 'R', 'N', 1, 0, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CUNMHR( 'L', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CUNMHR( 'R', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CUNMHR( 'L', 'N', 2, 1, 1, 1, A, 2, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL CUNMHR( 'L', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL CUNMHR( 'R', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'CUNMHR', INFOT, NOUT, LERR, OK )
         NT = NT + 16
*
*        CHSEQR
*
         SRNAMT = 'CHSEQR'
         INFOT = 1
         CALL CHSEQR( '/', 'N', 0, 1, 0, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHSEQR( 'E', '/', 0, 1, 0, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHSEQR( 'E', 'N', -1, 1, 0, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHSEQR( 'E', 'N', 0, 0, 0, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHSEQR( 'E', 'N', 0, 2, 0, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHSEQR( 'E', 'N', 1, 1, 0, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHSEQR( 'E', 'N', 1, 1, 2, A, 1, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHSEQR( 'E', 'N', 2, 1, 2, A, 1, X, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHSEQR( 'E', 'V', 2, 1, 2, A, 2, X, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'CHSEQR', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        CHSEIN
*
         SRNAMT = 'CHSEIN'
         INFOT = 1
         CALL CHSEIN( '/', 'N', 'N', SEL, 0, A, 1, X, VL, 1, VR, 1,
     $                0, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHSEIN( 'R', '/', 'N', SEL, 0, A, 1, X, VL, 1, VR, 1,
     $                0, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHSEIN( 'R', 'N', '/', SEL, 0, A, 1, X, VL, 1, VR, 1,
     $                0, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHSEIN( 'R', 'N', 'N', SEL, -1, A, 1, X, VL, 1, VR,
     $                1, 0, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHSEIN( 'R', 'N', 'N', SEL, 2, A, 1, X, VL, 1, VR, 2,
     $                4, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHSEIN( 'L', 'N', 'N', SEL, 2, A, 2, X, VL, 1, VR, 1,
     $                4, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, X, VL, 1, VR, 1,
     $                4, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL CHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, X, VL, 1, VR, 2,
     $                1, M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'CHSEIN', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        CTREVC
*
         SRNAMT = 'CTREVC'
         INFOT = 1
         CALL CTREVC( '/', 'A', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CTREVC( 'L', '/', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CTREVC( 'L', 'A', SEL, -1, A, 1, VL, 1, VR, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CTREVC( 'L', 'A', SEL, 2, A, 1, VL, 2, VR, 1, 4, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CTREVC( 'L', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CTREVC( 'R', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CTREVC( 'L', 'A', SEL, 2, A, 2, VL, 2, VR, 1, 1, M, W,
     $                RW, INFO )
         CALL CHKXER( 'CTREVC', INFOT, NOUT, LERR, OK )
         NT = NT + 7
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
 9999 FORMAT( 1X, A3, ' routines passed the tests of the error exits',
     $      ' (', I3, ' tests done)' )
 9998 FORMAT( ' *** ', A3, ' routines failed the tests of the error ',
     $      'exits ***' )
*
      RETURN
*
*     End of CERRHS
*
      END
