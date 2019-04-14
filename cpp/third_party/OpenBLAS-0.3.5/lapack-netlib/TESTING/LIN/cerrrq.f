*> \brief \b CERRRQ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRRQ( PATH, NUNIT )
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
*> CERRRQ tests the error exits for the COMPLEX routines
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRRQ( PATH, NUNIT )
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
      EXTERNAL           ALAESM, CGERQ2, CGERQF, CGERQS, CHKXER, CUNGR2,
     $                   CUNGRQ, CUNMR2, CUNMRQ
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
*     Error exits for RQ factorization
*
*     CGERQF
*
      SRNAMT = 'CGERQF'
      INFOT = 1
      CALL CGERQF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGERQF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGERQF( 2, 1, A, 1, B, W, 2, INFO )
      CALL CHKXER( 'CGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CGERQF( 2, 1, A, 2, B, W, 1, INFO )
      CALL CHKXER( 'CGERQF', INFOT, NOUT, LERR, OK )
*
*     CGERQ2
*
      SRNAMT = 'CGERQ2'
      INFOT = 1
      CALL CGERQ2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'CGERQ2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGERQ2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'CGERQ2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGERQ2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'CGERQ2', INFOT, NOUT, LERR, OK )
*
*     CGERQS
*
      SRNAMT = 'CGERQS'
      INFOT = 1
      CALL CGERQS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGERQS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGERQS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGERQS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGERQS( 2, 2, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGERQS( 2, 2, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CGERQS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGERQS', INFOT, NOUT, LERR, OK )
*
*     CUNGRQ
*
      SRNAMT = 'CUNGRQ'
      INFOT = 1
      CALL CUNGRQ( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGRQ( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGRQ( 2, 1, 0, A, 2, X, W, 2, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGRQ( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGRQ( 1, 2, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNGRQ( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CUNGRQ( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'CUNGRQ', INFOT, NOUT, LERR, OK )
*
*     CUNGR2
*
      SRNAMT = 'CUNGR2'
      INFOT = 1
      CALL CUNGR2( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGR2( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGR2( 2, 1, 0, A, 2, X, W, INFO )
      CALL CHKXER( 'CUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGR2( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGR2( 1, 2, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'CUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNGR2( 2, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGR2', INFOT, NOUT, LERR, OK )
*
*     CUNMRQ
*
      SRNAMT = 'CUNMRQ'
      INFOT = 1
      CALL CUNMRQ( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNMRQ( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNMRQ( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CUNMRQ( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMRQ( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMRQ( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMRQ( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMRQ( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMRQ( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CUNMRQ( 'L', 'N', 2, 1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CUNMRQ( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CUNMRQ( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'CUNMRQ', INFOT, NOUT, LERR, OK )
*
*     CUNMR2
*
      SRNAMT = 'CUNMR2'
      INFOT = 1
      CALL CUNMR2( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNMR2( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNMR2( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CUNMR2( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMR2( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMR2( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMR2( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMR2( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMR2( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CUNMR2( 'L', 'N', 2, 1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNMR2', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRRQ
*
      END
