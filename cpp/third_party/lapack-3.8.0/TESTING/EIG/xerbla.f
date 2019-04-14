*> \brief \b XERBLA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE XERBLA( SRNAME, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER*(*)      SRNAME
*       INTEGER            INFO
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This is a special version of XERBLA to be used only as part of
*> the test program for testing error exits from the LAPACK routines.
*> Error messages are printed if INFO.NE.INFOT or if SRNAME.NE.SRNAMT,
*> where INFOT and SRNAMT are values stored in COMMON.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SRNAME
*> \verbatim
*>          SRNAME is CHARACTER*(*)
*>          The name of the subroutine calling XERBLA.  This name should
*>          match the COMMON variable SRNAMT.
*> \endverbatim
*>
*> \param[in] INFO
*> \verbatim
*>          INFO is INTEGER
*>          The error return code from the calling subroutine.  INFO
*>          should equal the COMMON variable INFOT.
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
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The following variables are passed via the common blocks INFOC and
*>  SRNAMC:
*>
*>  INFOT   INTEGER      Expected integer return code
*>  NOUT    INTEGER      Unit number for printing error messages
*>  OK      LOGICAL      Set to .TRUE. if INFO = INFOT and
*>                       SRNAME = SRNAMT, otherwise set to .FALSE.
*>  LERR    LOGICAL      Set to .TRUE., indicating that XERBLA was called
*>  SRNAMT  CHARACTER*(*) Expected name of calling subroutine
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE XERBLA( SRNAME, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*(*)      SRNAME
      INTEGER            INFO
*     ..
*
*  =====================================================================
*
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          LEN_TRIM
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Executable Statements ..
*
      LERR = .TRUE.
      IF( INFO.NE.INFOT ) THEN
         IF( INFOT.NE.0 ) THEN
            WRITE( NOUT, FMT = 9999 )
     $     SRNAMT( 1:LEN_TRIM( SRNAMT ) ), INFO, INFOT
         ELSE
            WRITE( NOUT, FMT = 9997 )
     $     SRNAME( 1:LEN_TRIM( SRNAME ) ), INFO
         END IF
         OK = .FALSE.
      END IF
      IF( SRNAME.NE.SRNAMT ) THEN
         WRITE( NOUT, FMT = 9998 )
     $     SRNAME( 1:LEN_TRIM( SRNAME ) ),
     $     SRNAMT( 1:LEN_TRIM( SRNAMT ) )
         OK = .FALSE.
      END IF
      RETURN
*
 9999 FORMAT( ' *** XERBLA was called from ', A, ' with INFO = ', I6,
     $      ' instead of ', I2, ' ***' )
 9998 FORMAT( ' *** XERBLA was called with SRNAME = ', A,
     $      ' instead of ', A6, ' ***' )
 9997 FORMAT( ' *** On entry to ', A, ' parameter number ', I6,
     $      ' had an illegal value ***' )
*
*     End of XERBLA
*
      END
