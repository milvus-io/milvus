*> \brief \b SERRHS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRHS( PATH, NUNIT )
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
*> SERRHS tests the error exits for SGEBAK, SGEBAL, SGEHRD, SORGHR,
*> SORMHR, SHSEQR, SHSEIN, and STREVC.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SERRHS( PATH, NUNIT )
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
      PARAMETER          ( NMAX = 3, LW = ( NMAX+2 )*( NMAX+2 )+NMAX )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, ILO, IHI, INFO, J, M, NT
*     ..
*     .. Local Arrays ..
      LOGICAL            SEL( NMAX )
      INTEGER            IFAILL( NMAX ), IFAILR( NMAX )
      REAL               A( NMAX, NMAX ), C( NMAX, NMAX ), TAU( NMAX ),
     $                   VL( NMAX, NMAX ), VR( NMAX, NMAX ), W( LW ),
     $                   WI( NMAX ), WR( NMAX ), S( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, SGEBAK, SGEBAL, SGEHRD, SHSEIN, SHSEQR,
     $                   SORGHR, SORMHR, STREVC
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
         WI( J ) = REAL( J )
         SEL( J ) = .TRUE.
   20 CONTINUE
      OK = .TRUE.
      NT = 0
*
*     Test error exits of the nonsymmetric eigenvalue routines.
*
      IF( LSAMEN( 2, C2, 'HS' ) ) THEN
*
*        SGEBAL
*
         SRNAMT = 'SGEBAL'
         INFOT = 1
         CALL SGEBAL( '/', 0, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'SGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEBAL( 'N', -1, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'SGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEBAL( 'N', 2, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'SGEBAL', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        SGEBAK
*
         SRNAMT = 'SGEBAK'
         INFOT = 1
         CALL SGEBAK( '/', 'R', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEBAK( 'N', '/', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGEBAK( 'N', 'R', -1, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEBAK( 'N', 'R', 0, 0, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEBAK( 'N', 'R', 0, 2, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEBAK( 'N', 'R', 2, 2, 1, S, 0, A, 2, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEBAK( 'N', 'R', 0, 1, 1, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGEBAK( 'N', 'R', 0, 1, 0, S, -1, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGEBAK( 'N', 'R', 2, 1, 2, S, 0, A, 1, INFO )
         CALL CHKXER( 'SGEBAK', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        SGEHRD
*
         SRNAMT = 'SGEHRD'
         INFOT = 1
         CALL SGEHRD( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEHRD( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEHRD( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGEHRD( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGEHRD( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEHRD( 2, 1, 1, A, 1, TAU, W, 2, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGEHRD( 2, 1, 2, A, 2, TAU, W, 1, INFO )
         CALL CHKXER( 'SGEHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        SORGHR
*
         SRNAMT = 'SORGHR'
         INFOT = 1
         CALL SORGHR( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SORGHR( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SORGHR( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SORGHR( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SORGHR( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SORGHR( 2, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SORGHR( 3, 1, 3, A, 3, TAU, W, 1, INFO )
         CALL CHKXER( 'SORGHR', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        SORMHR
*
         SRNAMT = 'SORMHR'
         INFOT = 1
         CALL SORMHR( '/', 'N', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SORMHR( 'L', '/', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SORMHR( 'L', 'N', -1, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SORMHR( 'L', 'N', 0, -1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SORMHR( 'L', 'N', 0, 0, 0, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SORMHR( 'L', 'N', 0, 0, 2, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SORMHR( 'L', 'N', 1, 2, 2, 1, A, 1, TAU, C, 1, W, 2,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SORMHR( 'R', 'N', 2, 1, 2, 1, A, 1, TAU, C, 2, W, 2,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SORMHR( 'L', 'N', 1, 1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SORMHR( 'L', 'N', 0, 1, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SORMHR( 'R', 'N', 1, 0, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SORMHR( 'L', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SORMHR( 'R', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SORMHR( 'L', 'N', 2, 1, 1, 1, A, 2, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SORMHR( 'L', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SORMHR( 'R', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'SORMHR', INFOT, NOUT, LERR, OK )
         NT = NT + 16
*
*        SHSEQR
*
         SRNAMT = 'SHSEQR'
         INFOT = 1
         CALL SHSEQR( '/', 'N', 0, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SHSEQR( 'E', '/', 0, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SHSEQR( 'E', 'N', -1, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SHSEQR( 'E', 'N', 0, 0, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SHSEQR( 'E', 'N', 0, 2, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SHSEQR( 'E', 'N', 1, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SHSEQR( 'E', 'N', 1, 1, 2, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SHSEQR( 'E', 'N', 2, 1, 2, A, 1, WR, WI, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SHSEQR( 'E', 'V', 2, 1, 2, A, 2, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SHSEQR', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        SHSEIN
*
         SRNAMT = 'SHSEIN'
         INFOT = 1
         CALL SHSEIN( '/', 'N', 'N', SEL, 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SHSEIN( 'R', '/', 'N', SEL, 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SHSEIN( 'R', 'N', '/', SEL, 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SHSEIN( 'R', 'N', 'N', SEL, -1, A, 1, WR, WI, VL, 1, VR,
     $                1, 0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SHSEIN( 'R', 'N', 'N', SEL, 2, A, 1, WR, WI, VL, 1, VR, 2,
     $                4, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SHSEIN( 'L', 'N', 'N', SEL, 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                4, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                4, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, WR, WI, VL, 1, VR, 2,
     $                1, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'SHSEIN', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        STREVC
*
         SRNAMT = 'STREVC'
         INFOT = 1
         CALL STREVC( '/', 'A', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STREVC( 'L', '/', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STREVC( 'L', 'A', SEL, -1, A, 1, VL, 1, VR, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL STREVC( 'L', 'A', SEL, 2, A, 1, VL, 2, VR, 1, 4, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL STREVC( 'L', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL STREVC( 'R', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL STREVC( 'L', 'A', SEL, 2, A, 2, VL, 2, VR, 1, 1, M, W,
     $                INFO )
         CALL CHKXER( 'STREVC', INFOT, NOUT, LERR, OK )
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
     $        ' (', I3, ' tests done)' )
 9998 FORMAT( ' *** ', A3, ' routines failed the tests of the error ',
     $      'exits ***' )
*
      RETURN
*
*     End of SERRHS
*
      END
