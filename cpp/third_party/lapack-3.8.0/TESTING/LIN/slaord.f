*> \brief \b SLAORD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAORD( JOB, N, X, INCX )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOB
*       INTEGER            INCX, N
*       ..
*       .. Array Arguments ..
*       REAL               X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAORD sorts the elements of a vector x in increasing or decreasing
*> order.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER
*>          = 'I':  Sort in increasing order
*>          = 'D':  Sort in decreasing order
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The length of the vector X.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is REAL array, dimension
*>                         (1+(N-1)*INCX)
*>          On entry, the vector of length n to be sorted.
*>          On exit, the vector x is sorted in the prescribed order.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The spacing between successive elements of X.  INCX >= 0.
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
      SUBROUTINE SLAORD( JOB, N, X, INCX )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOB
      INTEGER            INCX, N
*     ..
*     .. Array Arguments ..
      REAL               X( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, INC, IX, IXNEXT
      REAL               TEMP
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
      INC = ABS( INCX )
      IF( LSAME( JOB, 'I' ) ) THEN
*
*        Sort in increasing order
*
         DO 20 I = 2, N
            IX = 1 + ( I-1 )*INC
   10       CONTINUE
            IF( IX.EQ.1 )
     $         GO TO 20
            IXNEXT = IX - INC
            IF( X( IX ).GT.X( IXNEXT ) ) THEN
               GO TO 20
            ELSE
               TEMP = X( IX )
               X( IX ) = X( IXNEXT )
               X( IXNEXT ) = TEMP
            END IF
            IX = IXNEXT
            GO TO 10
   20    CONTINUE
*
      ELSE IF( LSAME( JOB, 'D' ) ) THEN
*
*        Sort in decreasing order
*
         DO 40 I = 2, N
            IX = 1 + ( I-1 )*INC
   30       CONTINUE
            IF( IX.EQ.1 )
     $         GO TO 40
            IXNEXT = IX - INC
            IF( X( IX ).LT.X( IXNEXT ) ) THEN
               GO TO 40
            ELSE
               TEMP = X( IX )
               X( IX ) = X( IXNEXT )
               X( IXNEXT ) = TEMP
            END IF
            IX = IXNEXT
            GO TO 30
   40    CONTINUE
      END IF
      RETURN
*
*     End of SLAORD
*
      END
