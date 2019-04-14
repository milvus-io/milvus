*> \brief \b ALASVM
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ALASVM( TYPE, NOUT, NFAIL, NRUN, NERRS )
*
*       .. Scalar Arguments ..
*       CHARACTER*3        TYPE
*       INTEGER            NFAIL, NOUT, NRUN, NERRS
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ALASVM prints a summary of results from one of the -DRV- routines.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TYPE
*> \verbatim
*>          TYPE is CHARACTER*3
*>          The LAPACK path name.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number on which results are to be printed.
*>          NOUT >= 0.
*> \endverbatim
*>
*> \param[in] NFAIL
*> \verbatim
*>          NFAIL is INTEGER
*>          The number of tests which did not pass the threshold ratio.
*> \endverbatim
*>
*> \param[in] NRUN
*> \verbatim
*>          NRUN is INTEGER
*>          The total number of tests.
*> \endverbatim
*>
*> \param[in] NERRS
*> \verbatim
*>          NERRS is INTEGER
*>          The number of error messages recorded.
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
*> \ingroup aux_eig
*
*  =====================================================================
      SUBROUTINE ALASVM( TYPE, NOUT, NFAIL, NRUN, NERRS )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        TYPE
      INTEGER            NFAIL, NOUT, NRUN, NERRS
*     ..
*
*  =====================================================================
*
*     .. Executable Statements ..
*
      IF( NFAIL.GT.0 ) THEN
         WRITE( NOUT, FMT = 9999 )TYPE, NFAIL, NRUN
      ELSE
         WRITE( NOUT, FMT = 9998 )TYPE, NRUN
      END IF
      IF( NERRS.GT.0 ) THEN
         WRITE( NOUT, FMT = 9997 )NERRS
      END IF
*
 9999 FORMAT( 1X, A3, ' drivers: ', I6, ' out of ', I6,
     $      ' tests failed to pass the threshold' )
 9998 FORMAT( /1X, 'All tests for ', A3, ' drivers  passed the ',
     $      'threshold ( ', I6, ' tests run)' )
 9997 FORMAT( 14X, I6, ' error messages recorded' )
      RETURN
*
*     End of ALASVM
*
      END
