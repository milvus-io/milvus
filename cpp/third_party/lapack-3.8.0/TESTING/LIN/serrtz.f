*> \brief \b SERRTZ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRTZ( PATH, NUNIT )
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
*> SERRTZ tests the error exits for STZRZF.
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
      SUBROUTINE SERRTZ( PATH, NUNIT )
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
      CHARACTER*2        C2
      INTEGER            INFO
*     ..
*     .. Local Arrays ..
      REAL               A( NMAX, NMAX ), TAU( NMAX ), W( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, STZRZF
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
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
      C2 = PATH( 2: 3 )
      A( 1, 1 ) = 1.E+0
      A( 1, 2 ) = 2.E+0
      A( 2, 2 ) = 3.E+0
      A( 2, 1 ) = 4.E+0
      W( 1 ) = 0.0E+0
      W( 2 ) = 0.0E+0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'TZ' ) ) THEN
*
*        Test error exits for the trapezoidal routines.
*
*        STZRZF
*
         SRNAMT = 'STZRZF'
         INFOT = 1
         CALL STZRZF( -1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'STZRZF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL STZRZF( 1, 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'STZRZF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL STZRZF( 2, 2, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'STZRZF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STZRZF( 2, 2, A, 2, TAU, W, 0, INFO )
         CALL CHKXER( 'STZRZF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL STZRZF( 2, 3, A, 2, TAU, W, 1, INFO )
         CALL CHKXER( 'STZRZF', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRTZ
*
      END
