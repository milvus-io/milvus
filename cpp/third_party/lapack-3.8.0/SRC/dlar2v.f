*> \brief \b DLAR2V applies a vector of plane rotations with real cosines and real sines from both sides to a sequence of 2-by-2 symmetric/Hermitian matrices.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAR2V + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlar2v.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlar2v.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlar2v.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAR2V( N, X, Y, Z, INCX, C, S, INCC )
*
*       .. Scalar Arguments ..
*       INTEGER            INCC, INCX, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   C( * ), S( * ), X( * ), Y( * ), Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLAR2V applies a vector of real plane rotations from both sides to
*> a sequence of 2-by-2 real symmetric matrices, defined by the elements
*> of the vectors x, y and z. For i = 1,2,...,n
*>
*>    ( x(i)  z(i) ) := (  c(i)  s(i) ) ( x(i)  z(i) ) ( c(i) -s(i) )
*>    ( z(i)  y(i) )    ( -s(i)  c(i) ) ( z(i)  y(i) ) ( s(i)  c(i) )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of plane rotations to be applied.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCX)
*>          The vector x.
*> \endverbatim
*>
*> \param[in,out] Y
*> \verbatim
*>          Y is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCX)
*>          The vector y.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCX)
*>          The vector z.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The increment between elements of X, Y and Z. INCX > 0.
*> \endverbatim
*>
*> \param[in] C
*> \verbatim
*>          C is DOUBLE PRECISION array, dimension (1+(N-1)*INCC)
*>          The cosines of the plane rotations.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (1+(N-1)*INCC)
*>          The sines of the plane rotations.
*> \endverbatim
*>
*> \param[in] INCC
*> \verbatim
*>          INCC is INTEGER
*>          The increment between elements of C and S. INCC > 0.
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
      SUBROUTINE DLAR2V( N, X, Y, Z, INCX, C, S, INCC )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INCC, INCX, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   C( * ), S( * ), X( * ), Y( * ), Z( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, IC, IX
      DOUBLE PRECISION   CI, SI, T1, T2, T3, T4, T5, T6, XI, YI, ZI
*     ..
*     .. Executable Statements ..
*
      IX = 1
      IC = 1
      DO 10 I = 1, N
         XI = X( IX )
         YI = Y( IX )
         ZI = Z( IX )
         CI = C( IC )
         SI = S( IC )
         T1 = SI*ZI
         T2 = CI*ZI
         T3 = T2 - SI*XI
         T4 = T2 + SI*YI
         T5 = CI*XI + T1
         T6 = CI*YI - T1
         X( IX ) = CI*T5 + SI*T4
         Y( IX ) = CI*T6 - SI*T3
         Z( IX ) = CI*T4 - SI*T5
         IX = IX + INCX
         IC = IC + INCC
   10 CONTINUE
*
*     End of DLAR2V
*
      RETURN
      END
