*> \brief \b IZMAX1 finds the index of the first vector element of maximum absolute value.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download IZMAX1 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/izmax1.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/izmax1.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/izmax1.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       INTEGER          FUNCTION IZMAX1( N, ZX, INCX )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         ZX( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> IZMAX1 finds the index of the first vector element of maximum absolute value.
*>
*> Based on IZAMAX from Level 1 BLAS.
*> The change is to use the 'genuine' absolute value.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of elements in the vector ZX.
*> \endverbatim
*>
*> \param[in] ZX
*> \verbatim
*>          ZX is COMPLEX*16 array, dimension (N)
*>          The vector ZX. The IZMAX1 function returns the index of its first
*>          element of maximum absolute value.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The spacing between successive values of ZX.  INCX >= 1.
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
*> Nick Higham for use with ZLACON.
*
*  =====================================================================
      INTEGER FUNCTION IZMAX1( N, ZX, INCX )
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
      COMPLEX*16         ZX(*)
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      DOUBLE PRECISION   DMAX
      INTEGER            I, IX
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
      IZMAX1 = 0
      IF (N.LT.1 .OR. INCX.LE.0) RETURN
      IZMAX1 = 1
      IF (N.EQ.1) RETURN
      IF (INCX.EQ.1) THEN
*
*        code for increment equal to 1
*
         DMAX = ABS(ZX(1))
         DO I = 2,N
            IF (ABS(ZX(I)).GT.DMAX) THEN
               IZMAX1 = I
               DMAX = ABS(ZX(I))
            END IF
         END DO
      ELSE
*
*        code for increment not equal to 1
*
         IX = 1
         DMAX = ABS(ZX(1))
         IX = IX + INCX
         DO I = 2,N
            IF (ABS(ZX(IX)).GT.DMAX) THEN
               IZMAX1 = I
               DMAX = ABS(ZX(IX))
            END IF
            IX = IX + INCX
         END DO
      END IF
      RETURN
*
*     End of IZMAX1
*
      END
