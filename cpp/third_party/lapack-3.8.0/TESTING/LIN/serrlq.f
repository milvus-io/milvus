*> \brief \b SERRLQ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRLQ( PATH, NUNIT )
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
*> SERRLQ tests the error exits for the REAL routines
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRLQ( PATH, NUNIT )
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
      EXTERNAL           ALAESM, CHKXER, SGELQ2, SGELQF, SGELQS, SORGL2,
     $                   SORGLQ, SORML2, SORMLQ
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
*     Error exits for LQ factorization
*
*     SGELQF
*
      SRNAMT = 'SGELQF'
      INFOT = 1
      CALL SGELQF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGELQF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGELQF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGELQF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGELQF( 2, 1, A, 1, B, W, 2, INFO )
      CALL CHKXER( 'SGELQF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGELQF( 2, 1, A, 2, B, W, 1, INFO )
      CALL CHKXER( 'SGELQF', INFOT, NOUT, LERR, OK )
*
*     SGELQ2
*
      SRNAMT = 'SGELQ2'
      INFOT = 1
      CALL SGELQ2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'SGELQ2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGELQ2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGELQ2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGELQ2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGELQ2', INFOT, NOUT, LERR, OK )
*
*     SGELQS
*
      SRNAMT = 'SGELQS'
      INFOT = 1
      CALL SGELQS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGELQS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGELQS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGELQS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGELQS( 2, 2, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGELQS( 1, 2, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SGELQS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQS', INFOT, NOUT, LERR, OK )
*
*     SORGLQ
*
      SRNAMT = 'SORGLQ'
      INFOT = 1
      CALL SORGLQ( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGLQ( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGLQ( 2, 1, 0, A, 2, X, W, 2, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGLQ( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGLQ( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORGLQ( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SORGLQ( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'SORGLQ', INFOT, NOUT, LERR, OK )
*
*     SORGL2
*
      SRNAMT = 'SORGL2'
      INFOT = 1
      CALL SORGL2( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGL2( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGL2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGL2( 2, 1, 0, A, 2, X, W, INFO )
      CALL CHKXER( 'SORGL2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGL2( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGL2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGL2( 1, 1, 2, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGL2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORGL2( 2, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORGL2', INFOT, NOUT, LERR, OK )
*
*     SORMLQ
*
      SRNAMT = 'SORMLQ'
      INFOT = 1
      CALL SORMLQ( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORMLQ( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORMLQ( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORMLQ( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMLQ( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMLQ( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMLQ( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMLQ( 'L', 'N', 2, 0, 2, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMLQ( 'R', 'N', 0, 2, 2, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORMLQ( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMLQ( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMLQ( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMLQ', INFOT, NOUT, LERR, OK )
*
*     SORML2
*
      SRNAMT = 'SORML2'
      INFOT = 1
      CALL SORML2( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORML2( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORML2( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORML2( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORML2( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORML2( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORML2( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORML2( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORML2( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORML2( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORML2', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRLQ
*
      END
