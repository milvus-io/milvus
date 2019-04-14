*> \brief \b ZERRAB
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRAB( NUNIT )
*
*       .. Scalar Arguments ..
*       INTEGER            NUNIT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DERRAB tests the error exits for ZCGESV.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
      SUBROUTINE ZERRAB( NUNIT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 4 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, ITER, J
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX )
      COMPLEX*16         A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   C( NMAX ), R( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   W( 2*NMAX ), X( NMAX )
      COMPLEX*16         WORK(1)
      COMPLEX            SWORK(1)
      DOUBLE PRECISION   RWORK(1)
*     ..
*     .. External Functions ..
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, ZCGESV
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
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = 1.D0 / DBLE( I+J )
            AF( I, J ) = 1.D0 / DBLE( I+J )
   10    CONTINUE
         B( J ) = 0.D0
         R1( J ) = 0.D0
         R2( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
         C( J ) = 0.D0
         R( J ) = 0.D0
         IP( J ) = J
   20 CONTINUE
      OK = .TRUE.
*
      SRNAMT = 'ZCGESV'
      INFOT = 1
      CALL ZCGESV(-1,0,A,1,IP,B,1,X,1,WORK,SWORK,RWORK,ITER,INFO)
      CALL CHKXER( 'ZCGESV', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZCGESV(0,-1,A,1,IP,B,1,X,1,WORK,SWORK,RWORK,ITER,INFO)
      CALL CHKXER( 'ZCGESV', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZCGESV(2,1,A,1,IP,B,2,X,2,WORK,SWORK,RWORK,ITER,INFO)
      CALL CHKXER( 'ZCGESV', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZCGESV(2,1,A,2,IP,B,1,X,2,WORK,SWORK,RWORK,ITER,INFO)
      CALL CHKXER( 'ZCGESV', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL ZCGESV(2,1,A,2,IP,B,2,X,1,WORK,SWORK,RWORK,ITER,INFO)
      CALL CHKXER( 'ZCGESV', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )'ZCGESV'
      ELSE
         WRITE( NOUT, FMT = 9998 )'ZCGESV'
      END IF
*
 9999 FORMAT( 1X, A6, ' drivers passed the tests of the error exits' )
 9998 FORMAT( ' *** ', A6, ' drivers failed the tests of the error ',
     $      'exits ***' )
*
      RETURN
*
*     End of ZERRAB
*
      END
