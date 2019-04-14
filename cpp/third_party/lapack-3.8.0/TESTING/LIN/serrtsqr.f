*> \brief \b DERRTSQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRTSQR( PATH, NUNIT )
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
*> DERRTSQR tests the error exits for the REAL routines
*> that use the TSQR decomposition of a general matrix.
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE SERRTSQR( PATH, NUNIT )
      IMPLICIT NONE
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
      INTEGER            I, INFO, J, NB
*     ..
*     .. Local Arrays ..
      REAL               A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX ), TAU(NMAX)
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SGEQR,
     $                   SGEMQR, SGELQ, SGEMLQ
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
      DO J = 1, NMAX
         DO I = 1, NMAX
            A( I, J ) = 1.D0 / REAL( I+J )
            C( I, J ) = 1.D0 / REAL( I+J )
            T( I, J ) = 1.D0 / REAL( I+J )
         END DO
         W( J ) = 0.D0
      END DO
      OK = .TRUE.
*
*     Error exits for TS factorization
*
*     SGEQR
*
      SRNAMT = 'SGEQR'
      INFOT = 1
      CALL SGEQR( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQR( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQR( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL SGEQR( 3, 2, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGEQR( 3, 2, A, 3, TAU, 7, W, 0, INFO )
      CALL CHKXER( 'SGEQR', INFOT, NOUT, LERR, OK )
*
*     SGEMQR
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'SGEMQR'
      NB=1
      INFOT = 1
      CALL SGEMQR( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEMQR( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGEMQR( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEMQR( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEMQR( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEMQR( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGEMQR( 'L', 'N', 2, 1, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL SGEMQR( 'R', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL SGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL SGEMQR( 'L', 'N', 2, 1, 1, A, 2, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL SGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'SGEMQR', INFOT, NOUT, LERR, OK )
*
*     SGELQ
*
      SRNAMT = 'SGELQ'
      INFOT = 1
      CALL SGELQ( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGELQ( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGELQ( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL SGELQ( 2, 3, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'SGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGELQ( 2, 3, A, 3, TAU, 7, W, 0, INFO )
      CALL CHKXER( 'SGELQ', INFOT, NOUT, LERR, OK )
*
*     SGEMLQ
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'SGEMLQ'
      NB=1
      INFOT = 1
      CALL SGEMLQ( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEMLQ( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGEMLQ( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEMLQ( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEMLQ( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEMLQ( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGEMLQ( 'L', 'N', 1, 2, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL SGEMLQ( 'R', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL SGEMLQ( 'L', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL SGEMLQ( 'L', 'N', 1, 2, 1, A, 1, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL SGEMLQ( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'SGEMLQ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRTSQR
*
      END
