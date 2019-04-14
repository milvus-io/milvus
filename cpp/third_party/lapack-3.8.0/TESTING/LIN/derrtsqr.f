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
*       SUBROUTINE DERRTSQR( PATH, NUNIT )
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
*> DERRTSQR tests the error exits for the DOUBLE PRECISION routines
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
      SUBROUTINE DERRTSQR( PATH, NUNIT )
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
      DOUBLE PRECISION   A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX ), TAU(NMAX)
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DGEQR,
     $                   DGEMQR, DGELQ, DGEMLQ
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
      INTRINSIC          DBLE
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
            A( I, J ) = 1.D0 / DBLE( I+J )
            C( I, J ) = 1.D0 / DBLE( I+J )
            T( I, J ) = 1.D0 / DBLE( I+J )
         END DO
         W( J ) = 0.D0
      END DO
      OK = .TRUE.
*
*     Error exits for TS factorization
*
*     DGEQR
*
      SRNAMT = 'DGEQR'
      INFOT = 1
      CALL DGEQR( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQR( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQR( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGEQR( 3, 2, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGEQR( 3, 2, A, 3, TAU, 7, W, 0, INFO )
      CALL CHKXER( 'DGEQR', INFOT, NOUT, LERR, OK )
*
*     DGEMQR
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'DGEMQR'
      NB=1
      INFOT = 1
      CALL DGEMQR( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEMQR( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGEMQR( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEMQR( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMQR( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMQR( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DGEMQR( 'L', 'N', 2, 1, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DGEMQR( 'R', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL DGEMQR( 'L', 'N', 2, 1, 1, A, 2, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL DGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'DGEMQR', INFOT, NOUT, LERR, OK )
*
*     DGELQ
*
      SRNAMT = 'DGELQ'
      INFOT = 1
      CALL DGELQ( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGELQ( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGELQ( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGELQ( 2, 3, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'DGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGELQ( 2, 3, A, 3, TAU, 7, W, 0, INFO )
      CALL CHKXER( 'DGELQ', INFOT, NOUT, LERR, OK )
*
*     DGEMLQ
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'DGEMLQ'
      NB=1
      INFOT = 1
      CALL DGEMLQ( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEMLQ( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGEMLQ( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEMLQ( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMLQ( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMLQ( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DGEMLQ( 'L', 'N', 1, 2, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DGEMLQ( 'R', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DGEMLQ( 'L', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL DGEMLQ( 'L', 'N', 1, 2, 1, A, 1, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL DGEMLQ( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'DGEMLQ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRTSQR
*
      END
