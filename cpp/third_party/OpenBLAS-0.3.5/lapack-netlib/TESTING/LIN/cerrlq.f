*> \brief \b CERRLQ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRLQ( PATH, NUNIT )
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
*> CERRLQ tests the error exits for the COMPLEX routines
*> that use the LQ decomposition of a general matrix.
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
      SUBROUTINE CERRLQ( PATH, NUNIT )
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
      EXTERNAL           ALAESM, CGELQ2, CGELQF, CGELQS, CHKXER, CUNGL2,
     $                   CUNGLQ, CUNML2, CUNMLQ
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
*     Error exits for LQ factorization
*
*     CGELQF
*
      SRNAMT = 'CGELQF'
      INFOT = 1
      CALL CGELQF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGELQF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'CGELQF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGELQF( 2, 1, A, 1, B, W, 2, INFO )
      CALL CHKXER( 'CGELQF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CGELQF( 2, 1, A, 2, B, W, 1, INFO )
      CALL CHKXER( 'CGELQF', INFOT, NOUT, LERR, OK )
*
*     CGELQ2
*
      SRNAMT = 'CGELQ2'
      INFOT = 1
      CALL CGELQ2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'CGELQ2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQ2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'CGELQ2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGELQ2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'CGELQ2', INFOT, NOUT, LERR, OK )
*
*     CGELQS
*
      SRNAMT = 'CGELQS'
      INFOT = 1
      CALL CGELQS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGELQS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGELQS( 2, 2, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGELQS( 1, 2, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CGELQS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQS', INFOT, NOUT, LERR, OK )
*
*     CUNGLQ
*
      SRNAMT = 'CUNGLQ'
      INFOT = 1
      CALL CUNGLQ( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGLQ( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGLQ( 2, 1, 0, A, 2, X, W, 2, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGLQ( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGLQ( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNGLQ( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CUNGLQ( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'CUNGLQ', INFOT, NOUT, LERR, OK )
*
*     CUNGL2
*
      SRNAMT = 'CUNGL2'
      INFOT = 1
      CALL CUNGL2( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGL2( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNGL2( 2, 1, 0, A, 2, X, W, INFO )
      CALL CHKXER( 'CUNGL2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGL2( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGL2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNGL2( 1, 1, 2, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGL2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNGL2( 2, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'CUNGL2', INFOT, NOUT, LERR, OK )
*
*     CUNMLQ
*
      SRNAMT = 'CUNMLQ'
      INFOT = 1
      CALL CUNMLQ( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNMLQ( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNMLQ( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CUNMLQ( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMLQ( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMLQ( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNMLQ( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMLQ( 'L', 'N', 2, 0, 2, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNMLQ( 'R', 'N', 0, 2, 2, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CUNMLQ( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CUNMLQ( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL CUNMLQ( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'CUNMLQ', INFOT, NOUT, LERR, OK )
*
*     CUNML2
*
      SRNAMT = 'CUNML2'
      INFOT = 1
      CALL CUNML2( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CUNML2( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CUNML2( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CUNML2( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNML2( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNML2( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CUNML2( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNML2( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CUNML2( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL CUNML2( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'CUNML2', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRLQ
*
      END
