*> \brief \b SERRQL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRQL( PATH, NUNIT )
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
*> SERRQL tests the error exits for the REAL routines
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRQL( PATH, NUNIT )
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
      REAL               A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SGEQL2, SGEQLF, SGEQLS, SORG2L,
     $                   SORGQL, SORM2L, SORMQL
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
      INTRINSIC          REAL
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
            A( I, J ) = 1. / REAL( I+J )
            AF( I, J ) = 1. / REAL( I+J )
   10    CONTINUE
         B( J ) = 0.
         W( J ) = 0.
         X( J ) = 0.
   20 CONTINUE
      OK = .TRUE.
*
*     Error exits for QL factorization
*
*     SGEQLF
*
      SRNAMT = 'SGEQLF'
      INFOT = 1
      CALL SGEQLF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQLF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQLF( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQLF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGEQLF( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQLF', INFOT, NOUT, LERR, OK )
*
*     SGEQL2
*
      SRNAMT = 'SGEQL2'
      INFOT = 1
      CALL SGEQL2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQL2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQL2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQL2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQL2', INFOT, NOUT, LERR, OK )
*
*     SGEQLS
*
      SRNAMT = 'SGEQLS'
      INFOT = 1
      CALL SGEQLS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQLS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQLS( 1, 2, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGEQLS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEQLS( 2, 1, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGEQLS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SGEQLS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQLS', INFOT, NOUT, LERR, OK )
*
*     SORGQL
*
      SRNAMT = 'SORGQL'
      INFOT = 1
      CALL SORGQL( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGQL( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGQL( 1, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGQL( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGQL( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORGQL( 2, 1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SORGQL( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'SORGQL', INFOT, NOUT, LERR, OK )
*
*     SORG2L
*
      SRNAMT = 'SORG2L'
      INFOT = 1
      CALL SORG2L( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORG2L( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORG2L( 1, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORG2L( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORG2L( 2, 1, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'SORG2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORG2L( 2, 1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2L', INFOT, NOUT, LERR, OK )
*
*     SORMQL
*
      SRNAMT = 'SORMQL'
      INFOT = 1
      CALL SORMQL( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORMQL( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORMQL( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORMQL( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMQL( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMQL( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMQL( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMQL( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMQL( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORMQL( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMQL( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMQL( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMQL', INFOT, NOUT, LERR, OK )
*
*     SORM2L
*
      SRNAMT = 'SORM2L'
      INFOT = 1
      CALL SORM2L( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORM2L( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORM2L( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORM2L( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORM2L( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORM2L( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORM2L( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORM2L( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORM2L( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORM2L( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2L', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRQL
*
      END
