*> \brief \b DLARGV generates a vector of plane rotations with real cosines and real sines.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLARGV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlargv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlargv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlargv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLARGV( N, X, INCX, Y, INCY, C, INCC )
*
*       .. Scalar Arguments ..
*       INTEGER            INCC, INCX, INCY, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   C( * ), X( * ), Y( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLARGV generates a vector of real plane rotations, determined by
*> elements of the real vectors x and y. For i = 1,2,...,n
*>
*>    (  c(i)  s(i) ) ( x(i) ) = ( a(i) )
*>    ( -s(i)  c(i) ) ( y(i) ) = (   0  )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of plane rotations to be generated.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCX)
*>          On entry, the vector x.
*>          On exit, x(i) is overwritten by a(i), for i = 1,...,n.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The increment between elements of X. INCX > 0.
*> \endverbatim
*>
*> \param[in,out] Y
*> \verbatim
*>          Y is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCY)
*>          On entry, the vector y.
*>          On exit, the sines of the plane rotations.
*> \endverbatim
*>
*> \param[in] INCY
*> \verbatim
*>          INCY is INTEGER
*>          The increment between elements of Y. INCY > 0.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is DOUBLE PRECISION array, dimension (1+(N-1)*INCC)
*>          The cosines of the plane rotations.
*> \endverbatim
*>
*> \param[in] INCC
*> \verbatim
*>          INCC is INTEGER
*>          The increment between elements of C. INCC > 0.
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
*> \ingroup doubleOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE DLARGV( N, X, INCX, Y, INCY, C, INCC )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INCC, INCX, INCY, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   C( * ), X( * ), Y( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IC, IX, IY
      DOUBLE PRECISION   F, G, T, TT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, SQRT
*     ..
*     .. Executable Statements ..
*
      IX = 1
      IY = 1
      IC = 1
      DO 10 I = 1, N
         F = X( IX )
         G = Y( IY )
         IF( G.EQ.ZERO ) THEN
            C( IC ) = ONE
         ELSE IF( F.EQ.ZERO ) THEN
            C( IC ) = ZERO
            Y( IY ) = ONE
            X( IX ) = G
         ELSE IF( ABS( F ).GT.ABS( G ) ) THEN
            T = G / F
            TT = SQRT( ONE+T*T )
            C( IC ) = ONE / TT
            Y( IY ) = T*C( IC )
            X( IX ) = F*TT
         ELSE
            T = F / G
            TT = SQRT( ONE+T*T )
            Y( IY ) = ONE / TT
            C( IC ) = T*Y( IY )
            X( IX ) = G*TT
         END IF
         IC = IC + INCC
         IY = IY + INCY
         IX = IX + INCX
   10 CONTINUE
      RETURN
*
*     End of DLARGV
*
      END
