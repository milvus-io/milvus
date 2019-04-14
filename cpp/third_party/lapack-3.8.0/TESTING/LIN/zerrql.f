*> \brief \b ZERRQL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRQL( PATH, NUNIT )
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
*> ZERRQL tests the error exits for the COMPLEX*16 routines
*> that use the QL decomposition of a general matrix.
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZERRQL( PATH, NUNIT )
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
      PARAMETER          ( NMAX = 2 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
*     ..
*     .. Local Arrays ..
      COMPLEX*16         A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZGEQL2, ZGEQLF, ZGEQLS, ZUNG2L,
     $                   ZUNGQL, ZUNM2L, ZUNMQL
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
      INTRINSIC          DBLE, DCMPLX
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
*
*     Set the variables to innocuous values.
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = DCMPLX( 1.D0 / DBLE( I+J ),
     $                  -1.D0 / DBLE( I+J ) )
            AF( I, J ) = DCMPLX( 1.D0 / DBLE( I+J ),
     $                   -1.D0 / DBLE( I+J ) )
   10    CONTINUE
         B( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
   20 CONTINUE
      OK = .TRUE.
*
*     Error exits for QL factorization
*
*     ZGEQLF
*
      SRNAMT = 'ZGEQLF'
      INFOT = 1
      CALL ZGEQLF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQLF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQLF( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGEQLF( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQLF', INFOT, NOUT, LERR, OK )
*
*     ZGEQL2
*
      SRNAMT = 'ZGEQL2'
      INFOT = 1
      CALL ZGEQL2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQL2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQL2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQL2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQL2', INFOT, NOUT, LERR, OK )
*
*     ZGEQLS
*
      SRNAMT = 'ZGEQLS'
      INFOT = 1
      CALL ZGEQLS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQLS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQLS( 1, 2, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGEQLS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEQLS( 2, 1, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGEQLS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZGEQLS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQLS', INFOT, NOUT, LERR, OK )
*
*     ZUNGQL
*
      SRNAMT = 'ZUNGQL'
      INFOT = 1
      CALL ZUNGQL( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGQL( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGQL( 1, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGQL( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGQL( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNGQL( 2, 1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZUNGQL( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQL', INFOT, NOUT, LERR, OK )
*
*     ZUNG2L
*
      SRNAMT = 'ZUNG2L'
      INFOT = 1
      CALL ZUNG2L( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNG2L( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNG2L( 1, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNG2L( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNG2L( 2, 1, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'ZUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNG2L( 2, 1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2L', INFOT, NOUT, LERR, OK )
*
*     ZUNMQL
*
      SRNAMT = 'ZUNMQL'
      INFOT = 1
      CALL ZUNMQL( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNMQL( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNMQL( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZUNMQL( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMQL( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMQL( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMQL( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMQL( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMQL( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZUNMQL( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZUNMQL( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZUNMQL( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'ZUNMQL', INFOT, NOUT, LERR, OK )
*
*     ZUNM2L
*
      SRNAMT = 'ZUNM2L'
      INFOT = 1
      CALL ZUNM2L( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNM2L( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNM2L( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZUNM2L( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNM2L( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNM2L( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNM2L( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNM2L( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNM2L( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZUNM2L( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2L', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRQL
*
      END
