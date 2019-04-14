*> \brief \b CERRTSQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRTSQR( PATH, NUNIT )
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
*> CERRTSQR tests the error exits for the COMPLEX routines
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
*> \author Univ. of Colorado Zenver
*> \author NAG Ltd.
*
*> \date December 2016
*
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE CERRTSQR( PATH, NUNIT )
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
      COMPLEX            A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX ), TAU(NMAX)
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, CGEQR,
     $                   CGEMQR, CGELQ, CGEMLQ
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
            A( I, J ) = 1.E0 / CMPLX( REAL( I+J ), 0.E0 )
            C( I, J ) = 1.E0 / CMPLX( REAL( I+J ), 0.E0 )
            T( I, J ) = 1.E0 / CMPLX( REAL( I+J ), 0.E0 )
         END DO
         W( J ) = 0.E0
      END DO
      OK = .TRUE.
*
*     Error exits for TS factorization
*
*     CGEQR
*
      SRNAMT = 'CGEQR'
      INFOT = 1
      CALL CGEQR( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEQR( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGEQR( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL CGEQR( 3, 2, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGEQR( 3, 2, A, 3, TAU, 8, W, 0, INFO )
      CALL CHKXER( 'CGEQR', INFOT, NOUT, LERR, OK )
*
*     CGEMQR
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'CGEMQR'
      NB=1
      INFOT = 1
      CALL CGEMQR( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEMQR( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGEMQR( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGEMQR( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEMQR( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEMQR( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CGEMQR( 'L', 'N', 2, 1, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL CGEMQR( 'R', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL CGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL CGEMQR( 'L', 'N', 2, 1, 1, A, 2, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL CGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'CGEMQR', INFOT, NOUT, LERR, OK )
*
*     CGELQ
*
      SRNAMT = 'CGELQ'
      INFOT = 1
      CALL CGELQ( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGELQ( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGELQ( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL CGELQ( 2, 3, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'CGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL CGELQ( 2, 3, A, 3, TAU, 8, W, 0, INFO )
      CALL CHKXER( 'CGELQ', INFOT, NOUT, LERR, OK )
*
*     CGEMLQ
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'CGEMLQ'
      NB=1
      INFOT = 1
      CALL CGEMLQ( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL CGEMLQ( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL CGEMLQ( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL CGEMLQ( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEMLQ( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL CGEMLQ( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL CGEMLQ( 'L', 'N', 1, 2, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL CGEMLQ( 'R', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL CGEMLQ( 'L', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL CGEMLQ( 'L', 'N', 1, 2, 1, A, 1, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL CGEMLQ( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'CGEMLQ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRTSQR
*
      END
