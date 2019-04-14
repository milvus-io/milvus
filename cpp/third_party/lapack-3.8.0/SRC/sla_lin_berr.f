*> \brief \b SLA_LIN_BERR computes a component-wise relative backward error.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLA_LIN_BERR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sla_lin_berr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sla_lin_berr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sla_lin_berr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLA_LIN_BERR ( N, NZ, NRHS, RES, AYB, BERR )
*
*       .. Scalar Arguments ..
*       INTEGER            N, NZ, NRHS
*       ..
*       .. Array Arguments ..
*       REAL               AYB( N, NRHS ), BERR( NRHS )
*       REAL               RES( N, NRHS )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    SLA_LIN_BERR computes componentwise relative backward error from
*>    the formula
*>        max(i) ( abs(R(i)) / ( abs(op(A_s))*abs(Y) + abs(B_s) )(i) )
*>    where abs(Z) is the componentwise absolute value of the matrix
*>    or vector Z.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>     The number of linear equations, i.e., the order of the
*>     matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NZ
*> \verbatim
*>          NZ is INTEGER
*>     We add (NZ+1)*SLAMCH( 'Safe minimum' ) to R(i) in the numerator to
*>     guard against spuriously zero residuals. Default value is N.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>     The number of right hand sides, i.e., the number of columns
*>     of the matrices AYB, RES, and BERR.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] RES
*> \verbatim
*>          RES is REAL array, dimension (N,NRHS)
*>     The residual matrix, i.e., the matrix R in the relative backward
*>     error formula above.
*> \endverbatim
*>
*> \param[in] AYB
*> \verbatim
*>          AYB is REAL array, dimension (N, NRHS)
*>     The denominator in the relative backward error formula above, i.e.,
*>     the matrix abs(op(A_s))*abs(Y) + abs(B_s). The matrices A, Y, and B
*>     are from iterative refinement (see sla_gerfsx_extended.f).
*> \endverbatim
*>
*> \param[out] BERR
*> \verbatim
*>          BERR is REAL array, dimension (NRHS)
*>     The componentwise relative backward error from the formula above.
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
*> \ingroup realOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SLA_LIN_BERR ( N, NZ, NRHS, RES, AYB, BERR )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            N, NZ, NRHS
*     ..
*     .. Array Arguments ..
      REAL               AYB( N, NRHS ), BERR( NRHS )
      REAL               RES( N, NRHS )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      REAL               TMP
      INTEGER            I, J
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. External Functions ..
      EXTERNAL           SLAMCH
      REAL               SLAMCH
      REAL               SAFE1
*     ..
*     .. Executable Statements ..
*
*     Adding SAFE1 to the numerator guards against spuriously zero
*     residuals.  A similar safeguard is in the SLA_yyAMV routine used
*     to compute AYB.
*
      SAFE1 = SLAMCH( 'Safe minimum' )
      SAFE1 = (NZ+1)*SAFE1

      DO J = 1, NRHS
         BERR(J) = 0.0
         DO I = 1, N
            IF (AYB(I,J) .NE. 0.0) THEN
               TMP = (SAFE1+ABS(RES(I,J)))/AYB(I,J)
               BERR(J) = MAX( BERR(J), TMP )
            END IF
*
*     If AYB is exactly 0.0 (and if computed by SLA_yyAMV), then we know
*     the true residual also must be exactly 0.0.
*
         END DO
      END DO
      END
