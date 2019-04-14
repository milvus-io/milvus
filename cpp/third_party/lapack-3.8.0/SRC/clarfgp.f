*> \brief \b CLARFGP generates an elementary reflector (Householder matrix) with non-negative beta.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLARFGP + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clarfgp.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clarfgp.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clarfgp.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLARFGP( N, ALPHA, X, INCX, TAU )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX, N
*       COMPLEX            ALPHA, TAU
*       ..
*       .. Array Arguments ..
*       COMPLEX            X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLARFGP generates a complex elementary reflector H of order n, such
*> that
*>
*>       H**H * ( alpha ) = ( beta ),   H**H * H = I.
*>              (   x   )   (   0  )
*>
*> where alpha and beta are scalars, beta is real and non-negative, and
*> x is an (n-1)-element complex vector.  H is represented in the form
*>
*>       H = I - tau * ( 1 ) * ( 1 v**H ) ,
*>                     ( v )
*>
*> where tau is a complex scalar and v is a complex (n-1)-element
*> vector. Note that H is not hermitian.
*>
*> If the elements of x are all zero and alpha is real, then tau = 0
*> and H is taken to be the unit matrix.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the elementary reflector.
*> \endverbatim
*>
*> \param[in,out] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX
*>          On entry, the value alpha.
*>          On exit, it is overwritten with the value beta.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX array, dimension
*>                         (1+(N-2)*abs(INCX))
*>          On entry, the vector x.
*>          On exit, it is overwritten with the vector v.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The increment between elements of X. INCX > 0.
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX
*>          The value tau.
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
*> \ingroup complexOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE CLARFGP( N, ALPHA, X, INCX, TAU )
*
*  -- LAPACK auxiliary routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      INTEGER            INCX, N
      COMPLEX            ALPHA, TAU
*     ..
*     .. Array Arguments ..
      COMPLEX            X( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               TWO, ONE, ZERO
      PARAMETER          ( TWO = 2.0E+0, ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            J, KNT
      REAL               ALPHI, ALPHR, BETA, BIGNUM, SMLNUM, XNORM
      COMPLEX            SAVEALPHA
*     ..
*     .. External Functions ..
      REAL               SCNRM2, SLAMCH, SLAPY3, SLAPY2
      COMPLEX            CLADIV
      EXTERNAL           SCNRM2, SLAMCH, SLAPY3, SLAPY2, CLADIV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, CMPLX, REAL, SIGN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CSCAL, CSSCAL
*     ..
*     .. Executable Statements ..
*
      IF( N.LE.0 ) THEN
         TAU = ZERO
         RETURN
      END IF
*
      XNORM = SCNRM2( N-1, X, INCX )
      ALPHR = REAL( ALPHA )
      ALPHI = AIMAG( ALPHA )
*
      IF( XNORM.EQ.ZERO ) THEN
*
*        H  =  [1-alpha/abs(alpha) 0; 0 I], sign chosen so ALPHA >= 0.
*
         IF( ALPHI.EQ.ZERO ) THEN
            IF( ALPHR.GE.ZERO ) THEN
*              When TAU.eq.ZERO, the vector is special-cased to be
*              all zeros in the application routines.  We do not need
*              to clear it.
               TAU = ZERO
            ELSE
*              However, the application routines rely on explicit
*              zero checks when TAU.ne.ZERO, and we must clear X.
               TAU = TWO
               DO J = 1, N-1
                  X( 1 + (J-1)*INCX ) = ZERO
               END DO
               ALPHA = -ALPHA
            END IF
         ELSE
*           Only "reflecting" the diagonal entry to be real and non-negative.
            XNORM = SLAPY2( ALPHR, ALPHI )
            TAU = CMPLX( ONE - ALPHR / XNORM, -ALPHI / XNORM )
            DO J = 1, N-1
               X( 1 + (J-1)*INCX ) = ZERO
            END DO
            ALPHA = XNORM
         END IF
      ELSE
*
*        general case
*
         BETA = SIGN( SLAPY3( ALPHR, ALPHI, XNORM ), ALPHR )
         SMLNUM = SLAMCH( 'S' ) / SLAMCH( 'E' )
         BIGNUM = ONE / SMLNUM
*
         KNT = 0
         IF( ABS( BETA ).LT.SMLNUM ) THEN
*
*           XNORM, BETA may be inaccurate; scale X and recompute them
*
   10       CONTINUE
            KNT = KNT + 1
            CALL CSSCAL( N-1, BIGNUM, X, INCX )
            BETA = BETA*BIGNUM
            ALPHI = ALPHI*BIGNUM
            ALPHR = ALPHR*BIGNUM
            IF( (ABS( BETA ).LT.SMLNUM) .AND. (KNT .LT. 20) )
     $         GO TO 10
*
*           New BETA is at most 1, at least SMLNUM
*
            XNORM = SCNRM2( N-1, X, INCX )
            ALPHA = CMPLX( ALPHR, ALPHI )
            BETA = SIGN( SLAPY3( ALPHR, ALPHI, XNORM ), ALPHR )
         END IF
         SAVEALPHA = ALPHA
         ALPHA = ALPHA + BETA
         IF( BETA.LT.ZERO ) THEN
            BETA = -BETA
            TAU = -ALPHA / BETA
         ELSE
            ALPHR = ALPHI * (ALPHI/REAL( ALPHA ))
            ALPHR = ALPHR + XNORM * (XNORM/REAL( ALPHA ))
            TAU = CMPLX( ALPHR/BETA, -ALPHI/BETA )
            ALPHA = CMPLX( -ALPHR, ALPHI )
         END IF
         ALPHA = CLADIV( CMPLX( ONE ), ALPHA )
*
         IF ( ABS(TAU).LE.SMLNUM ) THEN
*
*           In the case where the computed TAU ends up being a denormalized number,
*           it loses relative accuracy. This is a BIG problem. Solution: flush TAU
*           to ZERO (or TWO or whatever makes a nonnegative real number for BETA).
*
*           (Bug report provided by Pat Quillen from MathWorks on Jul 29, 2009.)
*           (Thanks Pat. Thanks MathWorks.)
*
            ALPHR = REAL( SAVEALPHA )
            ALPHI = AIMAG( SAVEALPHA )
            IF( ALPHI.EQ.ZERO ) THEN
               IF( ALPHR.GE.ZERO ) THEN
                  TAU = ZERO
               ELSE
                  TAU = TWO
                  DO J = 1, N-1
                     X( 1 + (J-1)*INCX ) = ZERO
                  END DO
                  BETA = -SAVEALPHA
               END IF
            ELSE
               XNORM = SLAPY2( ALPHR, ALPHI )
               TAU = CMPLX( ONE - ALPHR / XNORM, -ALPHI / XNORM )
               DO J = 1, N-1
                  X( 1 + (J-1)*INCX ) = ZERO
               END DO
               BETA = XNORM
            END IF
*
         ELSE
*
*           This is the general case.
*
            CALL CSCAL( N-1, ALPHA, X, INCX )
*
         END IF
*
*        If BETA is subnormal, it may lose relative accuracy
*
         DO 20 J = 1, KNT
            BETA = BETA*SMLNUM
 20      CONTINUE
         ALPHA = BETA
      END IF
*
      RETURN
*
*     End of CLARFGP
*
      END
