*> \brief \b ICMAX1 finds the index of the first vector element of maximum absolute value.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ICMAX1 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/icmax1.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/icmax1.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/icmax1.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       INTEGER          FUNCTION ICMAX1( N, CX, INCX )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            CX( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ICMAX1 finds the index of the first vector element of maximum absolute value.
*>
*> Based on ICAMAX from Level 1 BLAS.
*> The change is to use the 'genuine' absolute value.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of elements in the vector CX.
*> \endverbatim
*>
*> \param[in] CX
*> \verbatim
*>          CX is COMPLEX array, dimension (N)
*>          The vector CX. The ICMAX1 function returns the index of its first
*>          element of maximum absolute value.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The spacing between successive values of CX.  INCX >= 1.
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
*> \date February 2014
*
*> \ingroup complexOTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*> Nick Higham for use with CLACON.
*
*  =====================================================================
      INTEGER FUNCTION ICMAX1( N, CX, INCX )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     February 2014
*
*     .. Scalar Arguments ..
      INTEGER            INCX, N
*     ..
*     .. Array Arguments ..
      COMPLEX            CX(*)
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      REAL               SMAX
      INTEGER            I, IX
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
      ICMAX1 = 0
      IF (N.LT.1 .OR. INCX.LE.0) RETURN
      ICMAX1 = 1
      IF (N.EQ.1) RETURN
      IF (INCX.EQ.1) THEN
*
*        code for increment equal to 1
*
         SMAX = ABS(CX(1))
         DO I = 2,N
            IF (ABS(CX(I)).GT.SMAX) THEN
               ICMAX1 = I
               SMAX = ABS(CX(I))
            END IF
         END DO
      ELSE
*
*        code for increment not equal to 1
*
         IX = 1
         SMAX = ABS(CX(1))
         IX = IX + INCX
         DO I = 2,N
            IF (ABS(CX(IX)).GT.SMAX) THEN
               ICMAX1 = I
               SMAX = ABS(CX(IX))
            END IF
            IX = IX + INCX
         END DO
      END IF
      RETURN
*
*     End of ICMAX1
*
      END
