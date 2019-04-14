*> \brief \b CLAR2V applies a vector of plane rotations with real cosines and complex sines from both sides to a sequence of 2-by-2 symmetric/Hermitian matrices.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLAR2V + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clar2v.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clar2v.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clar2v.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAR2V( N, X, Y, Z, INCX, C, S, INCC )
*
*       .. Scalar Arguments ..
*       INTEGER            INCC, INCX, N
*       ..
*       .. Array Arguments ..
*       REAL               C( * )
*       COMPLEX            S( * ), X( * ), Y( * ), Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLAR2V applies a vector of complex plane rotations with real cosines
*> from both sides to a sequence of 2-by-2 complex Hermitian matrices,
*> defined by the elements of the vectors x, y and z. For i = 1,2,...,n
*>
*>    (       x(i)  z(i) ) :=
*>    ( conjg(z(i)) y(i) )
*>
*>      (  c(i) conjg(s(i)) ) (       x(i)  z(i) ) ( c(i) -conjg(s(i)) )
*>      ( -s(i)       c(i)  ) ( conjg(z(i)) y(i) ) ( s(i)        c(i)  )
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
*>          X is COMPLEX array, dimension (1+(N-1)*INCX)
*>          The vector x; the elements of x are assumed to be real.
*> \endverbatim
*>
*> \param[in,out] Y
*> \verbatim
*>          Y is COMPLEX array, dimension (1+(N-1)*INCX)
*>          The vector y; the elements of y are assumed to be real.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is COMPLEX array, dimension (1+(N-1)*INCX)
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
*>          C is REAL array, dimension (1+(N-1)*INCC)
*>          The cosines of the plane rotations.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is COMPLEX array, dimension (1+(N-1)*INCC)
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
*> \ingroup complexOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE CLAR2V( N, X, Y, Z, INCX, C, S, INCC )
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
      REAL               C( * )
      COMPLEX            S( * ), X( * ), Y( * ), Z( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, IC, IX
      REAL               CI, SII, SIR, T1I, T1R, T5, T6, XI, YI, ZII,
     $                   ZIR
      COMPLEX            SI, T2, T3, T4, ZI
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          AIMAG, CMPLX, CONJG, REAL
*     ..
*     .. Executable Statements ..
*
      IX = 1
      IC = 1
      DO 10 I = 1, N
         XI = REAL( X( IX ) )
         YI = REAL( Y( IX ) )
         ZI = Z( IX )
         ZIR = REAL( ZI )
         ZII = AIMAG( ZI )
         CI = C( IC )
         SI = S( IC )
         SIR = REAL( SI )
         SII = AIMAG( SI )
         T1R = SIR*ZIR - SII*ZII
         T1I = SIR*ZII + SII*ZIR
         T2 = CI*ZI
         T3 = T2 - CONJG( SI )*XI
         T4 = CONJG( T2 ) + SI*YI
         T5 = CI*XI + T1R
         T6 = CI*YI - T1R
         X( IX ) = CI*T5 + ( SIR*REAL( T4 )+SII*AIMAG( T4 ) )
         Y( IX ) = CI*T6 - ( SIR*REAL( T3 )-SII*AIMAG( T3 ) )
         Z( IX ) = CI*T3 + CONJG( SI )*CMPLX( T6, T1I )
         IX = IX + INCX
         IC = IC + INCC
   10 CONTINUE
      RETURN
*
*     End of CLAR2V
*
      END
