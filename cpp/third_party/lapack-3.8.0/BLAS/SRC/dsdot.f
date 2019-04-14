*> \brief \b DSDOT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       DOUBLE PRECISION FUNCTION DSDOT(N,SX,INCX,SY,INCY)
*
*       .. Scalar Arguments ..
*       INTEGER INCX,INCY,N
*       ..
*       .. Array Arguments ..
*       REAL SX(*),SY(*)
*       ..
*
*    AUTHORS
*    =======
*    Lawson, C. L., (JPL), Hanson, R. J., (SNLA),
*    Kincaid, D. R., (U. of Texas), Krogh, F. T., (JPL)
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Compute the inner product of two vectors with extended
*> precision accumulation and result.
*>
*> Returns D.P. dot product accumulated in D.P., for S.P. SX and SY
*> DSDOT = sum for I = 0 to N-1 of  SX(LX+I*INCX) * SY(LY+I*INCY),
*> where LX = 1 if INCX .GE. 0, else LX = 1+(1-N)*INCX, and LY is
*> defined in a similar way using INCY.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         number of elements in input vector(s)
*> \endverbatim
*>
*> \param[in] SX
*> \verbatim
*>          SX is REAL array, dimension(N)
*>         single precision vector with N elements
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          storage spacing between elements of SX
*> \endverbatim
*>
*> \param[in] SY
*> \verbatim
*>          SY is REAL array, dimension(N)
*>         single precision vector with N elements
*> \endverbatim
*>
*> \param[in] INCY
*> \verbatim
*>          INCY is INTEGER
*>         storage spacing between elements of SY
*> \endverbatim
*>
*> \result DSDOT
*> \verbatim
*>          DSDOT is DOUBLE PRECISION
*>         DSDOT  double precision dot product (zero if N.LE.0)
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
*> \ingroup double_blas_level1
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*> \endverbatim
*
*> \par References:
*  ================
*>
*> \verbatim
*>
*>
*>  C. L. Lawson, R. J. Hanson, D. R. Kincaid and F. T.
*>  Krogh, Basic linear algebra subprograms for Fortran
*>  usage, Algorithm No. 539, Transactions on Mathematical
*>  Software 5, 3 (September 1979), pp. 308-323.
*>
*>  REVISION HISTORY  (YYMMDD)
*>
*>  791001  DATE WRITTEN
*>  890831  Modified array declarations.  (WRB)
*>  890831  REVISION DATE from Version 3.2
*>  891214  Prologue converted to Version 4.0 format.  (BAB)
*>  920310  Corrected definition of LX in DESCRIPTION.  (WRB)
*>  920501  Reformatted the REFERENCES section.  (WRB)
*>  070118  Reformat to LAPACK style (JL)
*> \endverbatim
*>
*  =====================================================================
      DOUBLE PRECISION FUNCTION DSDOT(N,SX,INCX,SY,INCY)
*
*  -- Reference BLAS level1 routine (version 3.7.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER INCX,INCY,N
*     ..
*     .. Array Arguments ..
      REAL SX(*),SY(*)
*     ..
*
*  Authors:
*  ========
*  Lawson, C. L., (JPL), Hanson, R. J., (SNLA),
*  Kincaid, D. R., (U. of Texas), Krogh, F. T., (JPL)
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER I,KX,KY,NS
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC DBLE
*     ..
      DSDOT = 0.0D0
      IF (N.LE.0) RETURN
      IF (INCX.EQ.INCY .AND. INCX.GT.0) THEN
*
*     Code for equal, positive, non-unit increments.
*
         NS = N*INCX
         DO I = 1,NS,INCX
            DSDOT = DSDOT + DBLE(SX(I))*DBLE(SY(I))
         END DO
      ELSE
*
*     Code for unequal or nonpositive increments.
*
         KX = 1
         KY = 1
         IF (INCX.LT.0) KX = 1 + (1-N)*INCX
         IF (INCY.LT.0) KY = 1 + (1-N)*INCY
         DO I = 1,N
            DSDOT = DSDOT + DBLE(SX(KX))*DBLE(SY(KY))
            KX = KX + INCX
            KY = KY + INCY
         END DO
      END IF
      RETURN
      END
