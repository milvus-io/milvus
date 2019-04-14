*> \brief \b ALAESM
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ALAESM( PATH, OK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            OK
*       CHARACTER*3        PATH
*       INTEGER            NOUT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ALAESM prints a summary of results from one of the -ERR- routines.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name.
*> \endverbatim
*>
*> \param[in] OK
*> \verbatim
*>          OK is LOGICAL
*>          The flag from CHKXER that indicates whether or not the tests
*>          of error exits passed.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number on which results are to be printed.
*>          NOUT >= 0.
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
*> \ingroup aux_lin
*
*  =====================================================================
      SUBROUTINE ALAESM( PATH, OK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            OK
      CHARACTER*3        PATH
      INTEGER            NOUT
*     ..
*
*  =====================================================================
*
*     .. Executable Statements ..
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )PATH
      ELSE
         WRITE( NOUT, FMT = 9998 )PATH
      END IF
*
 9999 FORMAT( 1X, A3, ' routines passed the tests of the error exits'
     $       )
 9998 FORMAT( ' *** ', A3, ' routines failed the tests of the error ',
     $      'exits ***' )
      RETURN
*
*     End of ALAESM
*
      END
