*> \brief \b ICOPY
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ICOPY( N, SX, INCX, SY, INCY )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX, INCY, N
*       ..
*       .. Array Arguments ..
*       INTEGER            SX( * ), SY( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ICOPY copies an integer vector x to an integer vector y.
*> Uses unrolled loops for increments equal to 1.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The length of the vectors SX and SY.
*> \endverbatim
*>
*> \param[in] SX
*> \verbatim
*>          SX is INTEGER array, dimension (1+(N-1)*abs(INCX))
*>          The vector X.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The spacing between consecutive elements of SX.
*> \endverbatim
*>
*> \param[out] SY
*> \verbatim
*>          SY is INTEGER array, dimension (1+(N-1)*abs(INCY))
*>          The vector Y.
*> \endverbatim
*>
*> \param[in] INCY
*> \verbatim
*>          INCY is INTEGER
*>          The spacing between consecutive elements of SY.
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
      SUBROUTINE ICOPY( N, SX, INCX, SY, INCY )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INCX, INCY, N
*     ..
*     .. Array Arguments ..
      INTEGER            SX( * ), SY( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, IX, IY, M, MP1
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MOD
*     ..
*     .. Executable Statements ..
*
      IF( N.LE.0 )
     $   RETURN
      IF( INCX.EQ.1 .AND. INCY.EQ.1 )
     $   GO TO 20
*
*     Code for unequal increments or equal increments not equal to 1
*
      IX = 1
      IY = 1
      IF( INCX.LT.0 )
     $   IX = ( -N+1 )*INCX + 1
      IF( INCY.LT.0 )
     $   IY = ( -N+1 )*INCY + 1
      DO 10 I = 1, N
         SY( IY ) = SX( IX )
         IX = IX + INCX
         IY = IY + INCY
   10 CONTINUE
      RETURN
*
*     Code for both increments equal to 1
*
*     Clean-up loop
*
   20 CONTINUE
      M = MOD( N, 7 )
      IF( M.EQ.0 )
     $   GO TO 40
      DO 30 I = 1, M
         SY( I ) = SX( I )
   30 CONTINUE
      IF( N.LT.7 )
     $   RETURN
   40 CONTINUE
      MP1 = M + 1
      DO 50 I = MP1, N, 7
         SY( I ) = SX( I )
         SY( I+1 ) = SX( I+1 )
         SY( I+2 ) = SX( I+2 )
         SY( I+3 ) = SX( I+3 )
         SY( I+4 ) = SX( I+4 )
         SY( I+5 ) = SX( I+5 )
         SY( I+6 ) = SX( I+6 )
   50 CONTINUE
      RETURN
*
*     End of ICOPY
*
      END
