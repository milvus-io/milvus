*> \brief \b ZERRRQ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRRQ( PATH, NUNIT )
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
*> ZERRRQ tests the error exits for the COMPLEX*16 routines
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZERRRQ( PATH, NUNIT )
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
      EXTERNAL           ALAESM, CHKXER, ZGERQ2, ZGERQF, ZGERQS, ZUNGR2,
     $                   ZUNGRQ, ZUNMR2, ZUNMRQ
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
*     Error exits for RQ factorization
*
*     ZGERQF
*
      SRNAMT = 'ZGERQF'
      INFOT = 1
      CALL ZGERQF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGERQF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGERQF( 2, 1, A, 1, B, W, 2, INFO )
      CALL CHKXER( 'ZGERQF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGERQF( 2, 1, A, 2, B, W, 1, INFO )
      CALL CHKXER( 'ZGERQF', INFOT, NOUT, LERR, OK )
*
*     ZGERQ2
*
      SRNAMT = 'ZGERQ2'
      INFOT = 1
      CALL ZGERQ2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGERQ2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGERQ2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGERQ2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGERQ2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGERQ2', INFOT, NOUT, LERR, OK )
*
*     ZGERQS
*
      SRNAMT = 'ZGERQS'
      INFOT = 1
      CALL ZGERQS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGERQS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGERQS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGERQS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGERQS( 2, 2, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGERQS( 2, 2, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZGERQS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGERQS', INFOT, NOUT, LERR, OK )
*
*     ZUNGRQ
*
      SRNAMT = 'ZUNGRQ'
      INFOT = 1
      CALL ZUNGRQ( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGRQ( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGRQ( 2, 1, 0, A, 2, X, W, 2, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGRQ( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGRQ( 1, 2, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNGRQ( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZUNGRQ( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGRQ', INFOT, NOUT, LERR, OK )
*
*     ZUNGR2
*
      SRNAMT = 'ZUNGR2'
      INFOT = 1
      CALL ZUNGR2( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGR2( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGR2( 2, 1, 0, A, 2, X, W, INFO )
      CALL CHKXER( 'ZUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGR2( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGR2( 1, 2, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'ZUNGR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNGR2( 2, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNGR2', INFOT, NOUT, LERR, OK )
*
*     ZUNMRQ
*
      SRNAMT = 'ZUNMRQ'
      INFOT = 1
      CALL ZUNMRQ( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNMRQ( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNMRQ( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZUNMRQ( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMRQ( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMRQ( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMRQ( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMRQ( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMRQ( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZUNMRQ( 'L', 'N', 2, 1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZUNMRQ( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZUNMRQ( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'ZUNMRQ', INFOT, NOUT, LERR, OK )
*
*     ZUNMR2
*
      SRNAMT = 'ZUNMR2'
      INFOT = 1
      CALL ZUNMR2( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNMR2( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNMR2( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZUNMR2( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMR2( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMR2( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMR2( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMR2( 'L', 'N', 2, 1, 2, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMR2( 'R', 'N', 1, 2, 2, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZUNMR2( 'L', 'N', 2, 1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNMR2', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRRQ
*
      END
