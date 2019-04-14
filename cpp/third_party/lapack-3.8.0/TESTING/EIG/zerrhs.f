*> \brief \b ZERRHS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRHS( PATH, NUNIT )
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
*> ZERRHS tests the error exits for ZGEBAK, CGEBAL, CGEHRD, ZUNGHR,
*> ZUNMHR, ZHSEQR, CHSEIN, and ZTREVC.
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
      SUBROUTINE ZERRHS( PATH, NUNIT )
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
      DOUBLE PRECISION   RW( NMAX ), S( NMAX )
      COMPLEX*16         A( NMAX, NMAX ), C( NMAX, NMAX ), TAU( NMAX ),
     $                   VL( NMAX, NMAX ), VR( NMAX, NMAX ), W( LW ),
     $                   X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, ZGEBAK, ZGEBAL, ZGEHRD, ZHSEIN, ZHSEQR,
     $                   ZTREVC, ZUNGHR, ZUNMHR
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE
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
            A( I, J ) = 1.D0 / DBLE( I+J )
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
*        ZGEBAL
*
         SRNAMT = 'ZGEBAL'
         INFOT = 1
         CALL ZGEBAL( '/', 0, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'ZGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEBAL( 'N', -1, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'ZGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEBAL( 'N', 2, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'ZGEBAL', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        ZGEBAK
*
         SRNAMT = 'ZGEBAK'
         INFOT = 1
         CALL ZGEBAK( '/', 'R', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEBAK( 'N', '/', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGEBAK( 'N', 'R', -1, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEBAK( 'N', 'R', 0, 0, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEBAK( 'N', 'R', 0, 2, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEBAK( 'N', 'R', 2, 2, 1, S, 0, A, 2, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEBAK( 'N', 'R', 0, 1, 1, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGEBAK( 'N', 'R', 0, 1, 0, S, -1, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGEBAK( 'N', 'R', 2, 1, 2, S, 0, A, 1, INFO )
         CALL CHKXER( 'ZGEBAK', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        ZGEHRD
*
         SRNAMT = 'ZGEHRD'
         INFOT = 1
         CALL ZGEHRD( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEHRD( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEHRD( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGEHRD( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGEHRD( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEHRD( 2, 1, 1, A, 1, TAU, W, 2, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGEHRD( 2, 1, 2, A, 2, TAU, W, 1, INFO )
         CALL CHKXER( 'ZGEHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        ZUNGHR
*
         SRNAMT = 'ZUNGHR'
         INFOT = 1
         CALL ZUNGHR( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZUNGHR( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZUNGHR( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZUNGHR( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZUNGHR( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZUNGHR( 2, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZUNGHR( 3, 1, 3, A, 3, TAU, W, 1, INFO )
         CALL CHKXER( 'ZUNGHR', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        ZUNMHR
*
         SRNAMT = 'ZUNMHR'
         INFOT = 1
         CALL ZUNMHR( '/', 'N', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZUNMHR( 'L', '/', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZUNMHR( 'L', 'N', -1, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZUNMHR( 'L', 'N', 0, -1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZUNMHR( 'L', 'N', 0, 0, 0, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZUNMHR( 'L', 'N', 0, 0, 2, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZUNMHR( 'L', 'N', 1, 2, 2, 1, A, 1, TAU, C, 1, W, 2,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZUNMHR( 'R', 'N', 2, 1, 2, 1, A, 1, TAU, C, 2, W, 2,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZUNMHR( 'L', 'N', 1, 1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZUNMHR( 'L', 'N', 0, 1, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZUNMHR( 'R', 'N', 1, 0, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZUNMHR( 'L', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZUNMHR( 'R', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZUNMHR( 'L', 'N', 2, 1, 1, 1, A, 2, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZUNMHR( 'L', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZUNMHR( 'R', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'ZUNMHR', INFOT, NOUT, LERR, OK )
         NT = NT + 16
*
*        ZHSEQR
*
         SRNAMT = 'ZHSEQR'
         INFOT = 1
         CALL ZHSEQR( '/', 'N', 0, 1, 0, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZHSEQR( 'E', '/', 0, 1, 0, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZHSEQR( 'E', 'N', -1, 1, 0, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZHSEQR( 'E', 'N', 0, 0, 0, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZHSEQR( 'E', 'N', 0, 2, 0, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZHSEQR( 'E', 'N', 1, 1, 0, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZHSEQR( 'E', 'N', 1, 1, 2, A, 1, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZHSEQR( 'E', 'N', 2, 1, 2, A, 1, X, C, 2, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZHSEQR( 'E', 'V', 2, 1, 2, A, 2, X, C, 1, W, 1, INFO )
         CALL CHKXER( 'ZHSEQR', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        ZHSEIN
*
         SRNAMT = 'ZHSEIN'
         INFOT = 1
         CALL ZHSEIN( '/', 'N', 'N', SEL, 0, A, 1, X, VL, 1, VR, 1, 0,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZHSEIN( 'R', '/', 'N', SEL, 0, A, 1, X, VL, 1, VR, 1, 0,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZHSEIN( 'R', 'N', '/', SEL, 0, A, 1, X, VL, 1, VR, 1, 0,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZHSEIN( 'R', 'N', 'N', SEL, -1, A, 1, X, VL, 1, VR, 1, 0,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZHSEIN( 'R', 'N', 'N', SEL, 2, A, 1, X, VL, 1, VR, 2, 4,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZHSEIN( 'L', 'N', 'N', SEL, 2, A, 2, X, VL, 1, VR, 1, 4,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, X, VL, 1, VR, 1, 4,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, X, VL, 1, VR, 2, 1,
     $                M, W, RW, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'ZHSEIN', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        ZTREVC
*
         SRNAMT = 'ZTREVC'
         INFOT = 1
         CALL ZTREVC( '/', 'A', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZTREVC( 'L', '/', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZTREVC( 'L', 'A', SEL, -1, A, 1, VL, 1, VR, 1, 0, M, W,
     $                RW, INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZTREVC( 'L', 'A', SEL, 2, A, 1, VL, 2, VR, 1, 4, M, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZTREVC( 'L', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZTREVC( 'R', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZTREVC( 'L', 'A', SEL, 2, A, 2, VL, 2, VR, 1, 1, M, W, RW,
     $                INFO )
         CALL CHKXER( 'ZTREVC', INFOT, NOUT, LERR, OK )
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
*     End of ZERRHS
*
      END
