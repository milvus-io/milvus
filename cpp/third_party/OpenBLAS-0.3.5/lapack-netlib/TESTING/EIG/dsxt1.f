*> \brief \b DSXT1
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       DOUBLE PRECISION FUNCTION DSXT1( IJOB, D1, N1, D2, N2, ABSTOL,
*                        ULP, UNFL )
*
*       .. Scalar Arguments ..
*       INTEGER            IJOB, N1, N2
*       DOUBLE PRECISION   ABSTOL, ULP, UNFL
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D1( * ), D2( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSXT1  computes the difference between a set of eigenvalues.
*> The result is returned as the function value.
*>
*> IJOB = 1:   Computes   max { min | D1(i)-D2(j) | }
*>                         i     j
*>
*> IJOB = 2:   Computes   max { min | D1(i)-D2(j) | /
*>                         i     j
*>                              ( ABSTOL + |D1(i)|*ULP ) }
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IJOB
*> \verbatim
*>          IJOB is INTEGER
*>          Specifies the type of tests to be performed.  (See above.)
*> \endverbatim
*>
*> \param[in] D1
*> \verbatim
*>          D1 is DOUBLE PRECISION array, dimension (N1)
*>          The first array.  D1 should be in increasing order, i.e.,
*>          D1(j) <= D1(j+1).
*> \endverbatim
*>
*> \param[in] N1
*> \verbatim
*>          N1 is INTEGER
*>          The length of D1.
*> \endverbatim
*>
*> \param[in] D2
*> \verbatim
*>          D2 is DOUBLE PRECISION array, dimension (N2)
*>          The second array.  D2 should be in increasing order, i.e.,
*>          D2(j) <= D2(j+1).
*> \endverbatim
*>
*> \param[in] N2
*> \verbatim
*>          N2 is INTEGER
*>          The length of D2.
*> \endverbatim
*>
*> \param[in] ABSTOL
*> \verbatim
*>          ABSTOL is DOUBLE PRECISION
*>          The absolute tolerance, used as a measure of the error.
*> \endverbatim
*>
*> \param[in] ULP
*> \verbatim
*>          ULP is DOUBLE PRECISION
*>          Machine precision.
*> \endverbatim
*>
*> \param[in] UNFL
*> \verbatim
*>          UNFL is DOUBLE PRECISION
*>          The smallest positive number whose reciprocal does not
*>          overflow.
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
*> \ingroup double_eig
*
*  =====================================================================
      DOUBLE PRECISION FUNCTION DSXT1( IJOB, D1, N1, D2, N2, ABSTOL,
     $                 ULP, UNFL )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            IJOB, N1, N2
      DOUBLE PRECISION   ABSTOL, ULP, UNFL
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D1( * ), D2( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
      DOUBLE PRECISION   TEMP1, TEMP2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      TEMP1 = ZERO
*
      J = 1
      DO 20 I = 1, N1
   10    CONTINUE
         IF( D2( J ).LT.D1( I ) .AND. J.LT.N2 ) THEN
            J = J + 1
            GO TO 10
         END IF
         IF( J.EQ.1 ) THEN
            TEMP2 = ABS( D2( J )-D1( I ) )
            IF( IJOB.EQ.2 )
     $         TEMP2 = TEMP2 / MAX( UNFL, ABSTOL+ULP*ABS( D1( I ) ) )
         ELSE
            TEMP2 = MIN( ABS( D2( J )-D1( I ) ),
     $              ABS( D1( I )-D2( J-1 ) ) )
            IF( IJOB.EQ.2 )
     $         TEMP2 = TEMP2 / MAX( UNFL, ABSTOL+ULP*ABS( D1( I ) ) )
         END IF
         TEMP1 = MAX( TEMP1, TEMP2 )
   20 CONTINUE
*
      DSXT1 = TEMP1
      RETURN
*
*     End of DSXT1
*
      END
