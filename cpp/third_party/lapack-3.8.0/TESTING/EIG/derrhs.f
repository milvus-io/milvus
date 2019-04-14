*> \brief \b DERRHS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRHS( PATH, NUNIT )
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
*> DERRHS tests the error exits for DGEBAK, SGEBAL, SGEHRD, DORGHR,
*> DORMHR, DHSEQR, SHSEIN, and DTREVC.
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
      SUBROUTINE DERRHS( PATH, NUNIT )
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
      INTEGER            I, IHI, ILO, INFO, J, M, NT
*     ..
*     .. Local Arrays ..
      LOGICAL            SEL( NMAX )
      INTEGER            IFAILL( NMAX ), IFAILR( NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), C( NMAX, NMAX ), S( NMAX ),
     $                   TAU( NMAX ), VL( NMAX, NMAX ),
     $                   VR( NMAX, NMAX ), W( LW ), WI( NMAX ),
     $                   WR( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DGEBAK, DGEBAL, DGEHRD, DHSEIN, DHSEQR,
     $                   DORGHR, DORMHR, DTREVC
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
         WI( J ) = DBLE( J )
         SEL( J ) = .TRUE.
   20 CONTINUE
      OK = .TRUE.
      NT = 0
*
*     Test error exits of the nonsymmetric eigenvalue routines.
*
      IF( LSAMEN( 2, C2, 'HS' ) ) THEN
*
*        DGEBAL
*
         SRNAMT = 'DGEBAL'
         INFOT = 1
         CALL DGEBAL( '/', 0, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'DGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEBAL( 'N', -1, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'DGEBAL', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEBAL( 'N', 2, A, 1, ILO, IHI, S, INFO )
         CALL CHKXER( 'DGEBAL', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        DGEBAK
*
         SRNAMT = 'DGEBAK'
         INFOT = 1
         CALL DGEBAK( '/', 'R', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEBAK( 'N', '/', 0, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGEBAK( 'N', 'R', -1, 1, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEBAK( 'N', 'R', 0, 0, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEBAK( 'N', 'R', 0, 2, 0, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEBAK( 'N', 'R', 2, 2, 1, S, 0, A, 2, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEBAK( 'N', 'R', 0, 1, 1, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGEBAK( 'N', 'R', 0, 1, 0, S, -1, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGEBAK( 'N', 'R', 2, 1, 2, S, 0, A, 1, INFO )
         CALL CHKXER( 'DGEBAK', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DGEHRD
*
         SRNAMT = 'DGEHRD'
         INFOT = 1
         CALL DGEHRD( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEHRD( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEHRD( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGEHRD( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGEHRD( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEHRD( 2, 1, 1, A, 1, TAU, W, 2, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGEHRD( 2, 1, 2, A, 2, TAU, W, 1, INFO )
         CALL CHKXER( 'DGEHRD', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        DORGHR
*
         SRNAMT = 'DORGHR'
         INFOT = 1
         CALL DORGHR( -1, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DORGHR( 0, 0, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DORGHR( 0, 2, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DORGHR( 1, 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DORGHR( 0, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DORGHR( 2, 1, 1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DORGHR( 3, 1, 3, A, 3, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGHR', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        DORMHR
*
         SRNAMT = 'DORMHR'
         INFOT = 1
         CALL DORMHR( '/', 'N', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DORMHR( 'L', '/', 0, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DORMHR( 'L', 'N', -1, 0, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DORMHR( 'L', 'N', 0, -1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DORMHR( 'L', 'N', 0, 0, 0, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DORMHR( 'L', 'N', 0, 0, 2, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DORMHR( 'L', 'N', 1, 2, 2, 1, A, 1, TAU, C, 1, W, 2,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DORMHR( 'R', 'N', 2, 1, 2, 1, A, 1, TAU, C, 2, W, 2,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DORMHR( 'L', 'N', 1, 1, 1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DORMHR( 'L', 'N', 0, 1, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DORMHR( 'R', 'N', 1, 0, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DORMHR( 'L', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DORMHR( 'R', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DORMHR( 'L', 'N', 2, 1, 1, 1, A, 2, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DORMHR( 'L', 'N', 1, 2, 1, 1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DORMHR( 'R', 'N', 2, 1, 1, 1, A, 1, TAU, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMHR', INFOT, NOUT, LERR, OK )
         NT = NT + 16
*
*        DHSEQR
*
         SRNAMT = 'DHSEQR'
         INFOT = 1
         CALL DHSEQR( '/', 'N', 0, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DHSEQR( 'E', '/', 0, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DHSEQR( 'E', 'N', -1, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DHSEQR( 'E', 'N', 0, 0, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DHSEQR( 'E', 'N', 0, 2, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DHSEQR( 'E', 'N', 1, 1, 0, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DHSEQR( 'E', 'N', 1, 1, 2, A, 1, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DHSEQR( 'E', 'N', 2, 1, 2, A, 1, WR, WI, C, 2, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DHSEQR( 'E', 'V', 2, 1, 2, A, 2, WR, WI, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DHSEQR', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DHSEIN
*
         SRNAMT = 'DHSEIN'
         INFOT = 1
         CALL DHSEIN( '/', 'N', 'N', SEL, 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DHSEIN( 'R', '/', 'N', SEL, 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DHSEIN( 'R', 'N', '/', SEL, 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DHSEIN( 'R', 'N', 'N', SEL, -1, A, 1, WR, WI, VL, 1, VR,
     $                1, 0, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DHSEIN( 'R', 'N', 'N', SEL, 2, A, 1, WR, WI, VL, 1, VR, 2,
     $                4, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DHSEIN( 'L', 'N', 'N', SEL, 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                4, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                4, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DHSEIN( 'R', 'N', 'N', SEL, 2, A, 2, WR, WI, VL, 1, VR, 2,
     $                1, M, W, IFAILL, IFAILR, INFO )
         CALL CHKXER( 'DHSEIN', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        DTREVC
*
         SRNAMT = 'DTREVC'
         INFOT = 1
         CALL DTREVC( '/', 'A', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DTREVC( 'L', '/', SEL, 0, A, 1, VL, 1, VR, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DTREVC( 'L', 'A', SEL, -1, A, 1, VL, 1, VR, 1, 0, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DTREVC( 'L', 'A', SEL, 2, A, 1, VL, 2, VR, 1, 4, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DTREVC( 'L', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DTREVC( 'R', 'A', SEL, 2, A, 2, VL, 1, VR, 1, 4, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DTREVC( 'L', 'A', SEL, 2, A, 2, VL, 2, VR, 1, 1, M, W,
     $                INFO )
         CALL CHKXER( 'DTREVC', INFOT, NOUT, LERR, OK )
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
*     End of DERRHS
*
      END
