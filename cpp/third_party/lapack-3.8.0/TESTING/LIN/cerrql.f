*> \brief \b CERRQL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRQL( PATH, NUNIT )
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
*> CERRQL tests the error exits for the COMPLEX routines
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRQL( PATH, NUNIT )
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
      COMPLEX            A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CGEQL2, CGEQLF, CGEQLS, CHKXER, CUNG2L,
     $                   CUNGQL, CUNM2L, CUNMQL
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
      INTRINSIC          CMPLX, REAL
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
            A( I, J ) = CMPLX( 1. / REAL( I+J ), -1. / REAL( I+J ) )
            AF( I, J ) = CMPLX( 1. / REAL( I+J ), -1. / REAL( I+J ) )
   10    CONTINUE
         B( J ) = 0.
         W( J ) = 0.
         X( J ) = 0.
   20 CONTINUE
      OK = .TRUE.
*
*     Error exits for QL factorization
*
*     CGEQLF
*
      SRNAMT = 'CGEQLF'
      INFOT = 1
      CALL CGEQLF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEQLF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGEQLF( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CGEQLF( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGEQLF', INFOT, NOUT, LERR, OK )
*
*     CGEQL2
*
      SRNAMT = 'CGEQL2'
      INFOT = 1
      CALL CGEQL2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'CGEQL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEQL2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'CGEQL2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGEQL2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'CGEQL2', INFOT, NOUT, LERR, OK )
*
*     CGEQLS
*
      SRNAMT = 'CGEQLS'
      INFOT = 1
      CALL CGEQLS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEQLS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEQLS( 1, 2, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGEQLS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEQLS( 2, 1, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGEQLS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CGEQLS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQLS', INFOT, NOUT, LERR, OK )
*
*     CUNGQL
*
      SRNAMT = 'CUNGQL'
      INFOT = 1
      CALL CUNGQL( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGQL( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGQL( 1, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGQL( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGQL( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNGQL( 2, 1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CUNGQL( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'CUNGQL', INFOT, NOUT, LERR, OK )
*
*     CUNG2L
*
      SRNAMT = 'CUNG2L'
      INFOT = 1
      CALL CUNG2L( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNG2L( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNG2L( 1, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNG2L( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNG2L( 2, 1, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'CUNG2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNG2L( 2, 1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNG2L', INFOT, NOUT, LERR, OK )
*
*     CUNMQL
*
      SRNAMT = 'CUNMQL'
      INFOT = 1
      CALL CUNMQL( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNMQL( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNMQL( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CUNMQL( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMQL( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMQL( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMQL( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMQL( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMQL( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CUNMQL( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CUNMQL( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CUNMQL( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'CUNMQL', INFOT, NOUT, LERR, OK )
*
*     CUNM2L
*
      SRNAMT = 'CUNM2L'
      INFOT = 1
      CALL CUNM2L( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNM2L( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNM2L( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CUNM2L( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNM2L( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNM2L( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNM2L( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNM2L( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNM2L( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CUNM2L( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNM2L', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRQL
*
      END
