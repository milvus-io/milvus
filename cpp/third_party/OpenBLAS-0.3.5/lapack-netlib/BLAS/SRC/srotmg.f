*> \brief \b SROTMG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SROTMG(SD1,SD2,SX1,SY1,SPARAM)
*
*       .. Scalar Arguments ..
*       REAL SD1,SD2,SX1,SY1
*       ..
*       .. Array Arguments ..
*       REAL SPARAM(5)
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CONSTRUCT THE MODIFIED GIVENS TRANSFORMATION MATRIX H WHICH ZEROS
*>    THE SECOND COMPONENT OF THE 2-VECTOR  (SQRT(SD1)*SX1,SQRT(SD2)*>    SY2)**T.
*>    WITH SPARAM(1)=SFLAG, H HAS ONE OF THE FOLLOWING FORMS..
*>
*>    SFLAG=-1.E0     SFLAG=0.E0        SFLAG=1.E0     SFLAG=-2.E0
*>
*>      (SH11  SH12)    (1.E0  SH12)    (SH11  1.E0)    (1.E0  0.E0)
*>    H=(          )    (          )    (          )    (          )
*>      (SH21  SH22),   (SH21  1.E0),   (-1.E0 SH22),   (0.E0  1.E0).
*>    LOCATIONS 2-4 OF SPARAM CONTAIN SH11,SH21,SH12, AND SH22
*>    RESPECTIVELY. (VALUES OF 1.E0, -1.E0, OR 0.E0 IMPLIED BY THE
*>    VALUE OF SPARAM(1) ARE NOT STORED IN SPARAM.)
*>
*>    THE VALUES OF GAMSQ AND RGAMSQ SET IN THE DATA STATEMENT MAY BE
*>    INEXACT.  THIS IS OK AS THEY ARE ONLY USED FOR TESTING THE SIZE
*>    OF SD1 AND SD2.  ALL ACTUAL SCALING OF DATA IS DONE USING GAM.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in,out] SD1
*> \verbatim
*>          SD1 is REAL
*> \endverbatim
*>
*> \param[in,out] SD2
*> \verbatim
*>          SD2 is REAL
*> \endverbatim
*>
*> \param[in,out] SX1
*> \verbatim
*>          SX1 is REAL
*> \endverbatim
*>
*> \param[in] SY1
*> \verbatim
*>          SY1 is REAL
*> \endverbatim
*>
*> \param[out] SPARAM
*> \verbatim
*>          SPARAM is REAL array, dimension (5)
*>     SPARAM(1)=SFLAG
*>     SPARAM(2)=SH11
*>     SPARAM(3)=SH21
*>     SPARAM(4)=SH12
*>     SPARAM(5)=SH22
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
*> \date November 2017
*
*> \ingroup single_blas_level1
*
*  =====================================================================
      SUBROUTINE SROTMG(SD1,SD2,SX1,SY1,SPARAM)
*
*  -- Reference BLAS level1 routine (version 3.8.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      REAL SD1,SD2,SX1,SY1
*     ..
*     .. Array Arguments ..
      REAL SPARAM(5)
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      REAL GAM,GAMSQ,ONE,RGAMSQ,SFLAG,SH11,SH12,SH21,SH22,SP1,SP2,SQ1,
     $     SQ2,STEMP,SU,TWO,ZERO
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC ABS
*     ..
*     .. Data statements ..
*
      DATA ZERO,ONE,TWO/0.E0,1.E0,2.E0/
      DATA GAM,GAMSQ,RGAMSQ/4096.E0,1.67772E7,5.96046E-8/
*     ..

      IF (SD1.LT.ZERO) THEN
*        GO ZERO-H-D-AND-SX1..
         SFLAG = -ONE
         SH11 = ZERO
         SH12 = ZERO
         SH21 = ZERO
         SH22 = ZERO
*
         SD1 = ZERO
         SD2 = ZERO
         SX1 = ZERO
      ELSE
*        CASE-SD1-NONNEGATIVE
         SP2 = SD2*SY1
         IF (SP2.EQ.ZERO) THEN
            SFLAG = -TWO
            SPARAM(1) = SFLAG
            RETURN
         END IF
*        REGULAR-CASE..
         SP1 = SD1*SX1
         SQ2 = SP2*SY1
         SQ1 = SP1*SX1
*
         IF (ABS(SQ1).GT.ABS(SQ2)) THEN
            SH21 = -SY1/SX1
            SH12 = SP2/SP1
*
            SU = ONE - SH12*SH21
*
           IF (SU.GT.ZERO) THEN
             SFLAG = ZERO
             SD1 = SD1/SU
             SD2 = SD2/SU
             SX1 = SX1*SU
           END IF
         ELSE

            IF (SQ2.LT.ZERO) THEN
*              GO ZERO-H-D-AND-SX1..
               SFLAG = -ONE
               SH11 = ZERO
               SH12 = ZERO
               SH21 = ZERO
               SH22 = ZERO
*
               SD1 = ZERO
               SD2 = ZERO
               SX1 = ZERO
            ELSE
               SFLAG = ONE
               SH11 = SP1/SP2
               SH22 = SX1/SY1
               SU = ONE + SH11*SH22
               STEMP = SD2/SU
               SD2 = SD1/SU
               SD1 = STEMP
               SX1 = SY1*SU
            END IF
         END IF

*     PROCESURE..SCALE-CHECK
         IF (SD1.NE.ZERO) THEN
            DO WHILE ((SD1.LE.RGAMSQ) .OR. (SD1.GE.GAMSQ))
               IF (SFLAG.EQ.ZERO) THEN
                  SH11 = ONE
                  SH22 = ONE
                  SFLAG = -ONE
               ELSE
                  SH21 = -ONE
                  SH12 = ONE
                  SFLAG = -ONE
               END IF
               IF (SD1.LE.RGAMSQ) THEN
                  SD1 = SD1*GAM**2
                  SX1 = SX1/GAM
                  SH11 = SH11/GAM
                  SH12 = SH12/GAM
               ELSE
                  SD1 = SD1/GAM**2
                  SX1 = SX1*GAM
                  SH11 = SH11*GAM
                  SH12 = SH12*GAM
               END IF
            ENDDO
         END IF

         IF (SD2.NE.ZERO) THEN
            DO WHILE ( (ABS(SD2).LE.RGAMSQ) .OR. (ABS(SD2).GE.GAMSQ) )
               IF (SFLAG.EQ.ZERO) THEN
                  SH11 = ONE
                  SH22 = ONE
                  SFLAG = -ONE
               ELSE
                  SH21 = -ONE
                  SH12 = ONE
                  SFLAG = -ONE
               END IF
               IF (ABS(SD2).LE.RGAMSQ) THEN
                  SD2 = SD2*GAM**2
                  SH21 = SH21/GAM
                  SH22 = SH22/GAM
               ELSE
                  SD2 = SD2/GAM**2
                  SH21 = SH21*GAM
                  SH22 = SH22*GAM
               END IF
            END DO
         END IF

      END IF

      IF (SFLAG.LT.ZERO) THEN
         SPARAM(2) = SH11
         SPARAM(3) = SH21
         SPARAM(4) = SH12
         SPARAM(5) = SH22
      ELSE IF (SFLAG.EQ.ZERO) THEN
         SPARAM(3) = SH21
         SPARAM(4) = SH12
      ELSE
         SPARAM(2) = SH11
         SPARAM(5) = SH22
      END IF

      SPARAM(1) = SFLAG
      RETURN
      END




