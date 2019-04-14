*> \brief \b SERRRQ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRRQ( PATH, NUNIT )
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
*> SERRRQ tests the error exits for the REAL routines
*> that use the RQ decomposition of a general matrix.
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
      SUBROUTINE SERRRQ( PATH, NUNIT )
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
      EXTERNAL           ALAESM, CHKXER, SGERQ2, SGERQF, SGERQS, SORGR2,
     $                   SORGRQ, SORMR2, SORMRQ
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
*     Error exits for RQ factorization
*
*     SGERQF
*
      SRNAMT = 'SGERQF'
      INFOT = 1
      CALL SGERQF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGERQF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGERQF( 2, 1, A, 1, B, W, 2, INFO )
      CALL CHKXER( 'SGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGERQF( 2, 1, A, 2, B, W, 1, INFO )
      CALL CHKXER( 'SGERQF', INFOT, NOUT, LERR, OK )
*
*     SGERQ2
*
      SRNAMT = 'SGERQ2'
      INFOT = 1
      CALL SGERQ2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'SGERQ2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGERQ2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGERQ2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGERQ2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGERQ2', INFOT, NOUT, LERR, OK )
*
*     SGERQS
*
      SRNAMT = 'SGERQS'
      INFOT = 1
      CALL SGERQS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGERQS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGERQS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGERQS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGERQS( 2, 2, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGERQS( 2, 2, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SGERQS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGERQS', INFOT, NOUT, LERR, OK )
*
*     SORGRQ
*
      SRNAMT = 'SORGRQ'
      INFOT = 1
      CALL SORGRQ( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGRQ( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGRQ( 2, 1, 0, A, 2, X, W, 2, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGRQ( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGRQ( 1, 2, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORGRQ( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SORGRQ( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'SORGRQ', INFOT, NOUT, LERR, OK )
*
*     SORGR2
*
      SRNAMT = 'SORGR2'
      INFOT = 1
      CALL SORGR2( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGR2( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGR2( 2, 1, 0, A, 2, X, W, INFO )
      CALL CHKXER( 'SORGR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGR2( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGR2( 1, 2, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'SORGR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORGR2( 2, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGR2', INFOT, NOUT, LERR, OK )
*
*     SORMRQ
*
      SRNAMT = 'SORMRQ'
      INFOT = 1
      CALL SORMRQ( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORMRQ( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORMRQ( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORMRQ( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMRQ( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMRQ( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMRQ( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMRQ( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMRQ( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORMRQ( 'L', 'N', 2, 1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMRQ( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMRQ( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMRQ', INFOT, NOUT, LERR, OK )
*
*     SORMR2
*
      SRNAMT = 'SORMR2'
      INFOT = 1
      CALL SORMR2( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORMR2( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORMR2( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORMR2( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMR2( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMR2( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMR2( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMR2( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMR2( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORMR2( 'L', 'N', 2, 1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORMR2', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRRQ
*
      END
