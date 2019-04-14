*> \brief \b SLASD8 finds the square roots of the roots of the secular equation, and stores, for each element in D, the distance to its two nearest poles. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLASD8 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasd8.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasd8.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasd8.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLASD8( ICOMPQ, K, D, Z, VF, VL, DIFL, DIFR, LDDIFR,
*                          DSIGMA, WORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            ICOMPQ, INFO, K, LDDIFR
*       ..
*       .. Array Arguments ..
*       REAL               D( * ), DIFL( * ), DIFR( LDDIFR, * ),
*      $                   DSIGMA( * ), VF( * ), VL( * ), WORK( * ),
*      $                   Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLASD8 finds the square roots of the roots of the secular equation,
*> as defined by the values in DSIGMA and Z. It makes the appropriate
*> calls to SLASD4, and stores, for each  element in D, the distance
*> to its two nearest poles (elements in DSIGMA). It also updates
*> the arrays VF and VL, the first and last components of all the
*> right singular vectors of the original bidiagonal matrix.
*>
*> SLASD8 is called from SLASD6.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ICOMPQ
*> \verbatim
*>          ICOMPQ is INTEGER
*>          Specifies whether singular vectors are to be computed in
*>          factored form in the calling routine:
*>          = 0: Compute singular values only.
*>          = 1: Compute singular vectors in factored form as well.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of terms in the rational function to be solved
*>          by SLASD4.  K >= 1.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is REAL array, dimension ( K )
*>          On output, D contains the updated singular values.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is REAL array, dimension ( K )
*>          On entry, the first K elements of this array contain the
*>          components of the deflation-adjusted updating row vector.
*>          On exit, Z is updated.
*> \endverbatim
*>
*> \param[in,out] VF
*> \verbatim
*>          VF is REAL array, dimension ( K )
*>          On entry, VF contains  information passed through DBEDE8.
*>          On exit, VF contains the first K components of the first
*>          components of all right singular vectors of the bidiagonal
*>          matrix.
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is REAL array, dimension ( K )
*>          On entry, VL contains  information passed through DBEDE8.
*>          On exit, VL contains the first K components of the last
*>          components of all right singular vectors of the bidiagonal
*>          matrix.
*> \endverbatim
*>
*> \param[out] DIFL
*> \verbatim
*>          DIFL is REAL array, dimension ( K )
*>          On exit, DIFL(I) = D(I) - DSIGMA(I).
*> \endverbatim
*>
*> \param[out] DIFR
*> \verbatim
*>          DIFR is REAL array,
*>                   dimension ( LDDIFR, 2 ) if ICOMPQ = 1 and
*>                   dimension ( K ) if ICOMPQ = 0.
*>          On exit, DIFR(I,1) = D(I) - DSIGMA(I+1), DIFR(K,1) is not
*>          defined and will not be referenced.
*>
*>          If ICOMPQ = 1, DIFR(1:K,2) is an array containing the
*>          normalizing factors for the right singular vector matrix.
*> \endverbatim
*>
*> \param[in] LDDIFR
*> \verbatim
*>          LDDIFR is INTEGER
*>          The leading dimension of DIFR, must be at least K.
*> \endverbatim
*>
*> \param[in,out] DSIGMA
*> \verbatim
*>          DSIGMA is REAL array, dimension ( K )
*>          On entry, the first K elements of this array contain the old
*>          roots of the deflated updating problem.  These are the poles
*>          of the secular equation.
*>          On exit, the elements of DSIGMA may be very slightly altered
*>          in value.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (3*K)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if INFO = 1, a singular value did not converge
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
*> \date June 2017
*
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Huan Ren, Computer Science Division, University of
*>     California at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE SLASD8( ICOMPQ, K, D, Z, VF, VL, DIFL, DIFR, LDDIFR,
     $                   DSIGMA, WORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            ICOMPQ, INFO, K, LDDIFR
*     ..
*     .. Array Arguments ..
      REAL               D( * ), DIFL( * ), DIFR( LDDIFR, * ),
     $                   DSIGMA( * ), VF( * ), VL( * ), WORK( * ),
     $                   Z( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE
      PARAMETER          ( ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IWK1, IWK2, IWK2I, IWK3, IWK3I, J
      REAL               DIFLJ, DIFRJ, DJ, DSIGJ, DSIGJP, RHO, TEMP
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SLASCL, SLASD4, SLASET, XERBLA
*     ..
*     .. External Functions ..
      REAL               SDOT, SLAMC3, SNRM2
      EXTERNAL           SDOT, SLAMC3, SNRM2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( ( ICOMPQ.LT.0 ) .OR. ( ICOMPQ.GT.1 ) ) THEN
         INFO = -1
      ELSE IF( K.LT.1 ) THEN
         INFO = -2
      ELSE IF( LDDIFR.LT.K ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLASD8', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( K.EQ.1 ) THEN
         D( 1 ) = ABS( Z( 1 ) )
         DIFL( 1 ) = D( 1 )
         IF( ICOMPQ.EQ.1 ) THEN
            DIFL( 2 ) = ONE
            DIFR( 1, 2 ) = ONE
         END IF
         RETURN
      END IF
*
*     Modify values DSIGMA(i) to make sure all DSIGMA(i)-DSIGMA(j) can
*     be computed with high relative accuracy (barring over/underflow).
*     This is a problem on machines without a guard digit in
*     add/subtract (Cray XMP, Cray YMP, Cray C 90 and Cray 2).
*     The following code replaces DSIGMA(I) by 2*DSIGMA(I)-DSIGMA(I),
*     which on any of these machines zeros out the bottommost
*     bit of DSIGMA(I) if it is 1; this makes the subsequent
*     subtractions DSIGMA(I)-DSIGMA(J) unproblematic when cancellation
*     occurs. On binary machines with a guard digit (almost all
*     machines) it does not change DSIGMA(I) at all. On hexadecimal
*     and decimal machines with a guard digit, it slightly
*     changes the bottommost bits of DSIGMA(I). It does not account
*     for hexadecimal or decimal machines without guard digits
*     (we know of none). We use a subroutine call to compute
*     2*DLAMBDA(I) to prevent optimizing compilers from eliminating
*     this code.
*
      DO 10 I = 1, K
         DSIGMA( I ) = SLAMC3( DSIGMA( I ), DSIGMA( I ) ) - DSIGMA( I )
   10 CONTINUE
*
*     Book keeping.
*
      IWK1 = 1
      IWK2 = IWK1 + K
      IWK3 = IWK2 + K
      IWK2I = IWK2 - 1
      IWK3I = IWK3 - 1
*
*     Normalize Z.
*
      RHO = SNRM2( K, Z, 1 )
      CALL SLASCL( 'G', 0, 0, RHO, ONE, K, 1, Z, K, INFO )
      RHO = RHO*RHO
*
*     Initialize WORK(IWK3).
*
      CALL SLASET( 'A', K, 1, ONE, ONE, WORK( IWK3 ), K )
*
*     Compute the updated singular values, the arrays DIFL, DIFR,
*     and the updated Z.
*
      DO 40 J = 1, K
         CALL SLASD4( K, J, DSIGMA, Z, WORK( IWK1 ), RHO, D( J ),
     $                WORK( IWK2 ), INFO )
*
*        If the root finder fails, report the convergence failure.
*
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
         WORK( IWK3I+J ) = WORK( IWK3I+J )*WORK( J )*WORK( IWK2I+J )
         DIFL( J ) = -WORK( J )
         DIFR( J, 1 ) = -WORK( J+1 )
         DO 20 I = 1, J - 1
            WORK( IWK3I+I ) = WORK( IWK3I+I )*WORK( I )*
     $                        WORK( IWK2I+I ) / ( DSIGMA( I )-
     $                        DSIGMA( J ) ) / ( DSIGMA( I )+
     $                        DSIGMA( J ) )
   20    CONTINUE
         DO 30 I = J + 1, K
            WORK( IWK3I+I ) = WORK( IWK3I+I )*WORK( I )*
     $                        WORK( IWK2I+I ) / ( DSIGMA( I )-
     $                        DSIGMA( J ) ) / ( DSIGMA( I )+
     $                        DSIGMA( J ) )
   30    CONTINUE
   40 CONTINUE
*
*     Compute updated Z.
*
      DO 50 I = 1, K
         Z( I ) = SIGN( SQRT( ABS( WORK( IWK3I+I ) ) ), Z( I ) )
   50 CONTINUE
*
*     Update VF and VL.
*
      DO 80 J = 1, K
         DIFLJ = DIFL( J )
         DJ = D( J )
         DSIGJ = -DSIGMA( J )
         IF( J.LT.K ) THEN
            DIFRJ = -DIFR( J, 1 )
            DSIGJP = -DSIGMA( J+1 )
         END IF
         WORK( J ) = -Z( J ) / DIFLJ / ( DSIGMA( J )+DJ )
         DO 60 I = 1, J - 1
            WORK( I ) = Z( I ) / ( SLAMC3( DSIGMA( I ), DSIGJ )-DIFLJ )
     $                   / ( DSIGMA( I )+DJ )
   60    CONTINUE
         DO 70 I = J + 1, K
            WORK( I ) = Z( I ) / ( SLAMC3( DSIGMA( I ), DSIGJP )+DIFRJ )
     $                   / ( DSIGMA( I )+DJ )
   70    CONTINUE
         TEMP = SNRM2( K, WORK, 1 )
         WORK( IWK2I+J ) = SDOT( K, WORK, 1, VF, 1 ) / TEMP
         WORK( IWK3I+J ) = SDOT( K, WORK, 1, VL, 1 ) / TEMP
         IF( ICOMPQ.EQ.1 ) THEN
            DIFR( J, 2 ) = TEMP
         END IF
   80 CONTINUE
*
      CALL SCOPY( K, WORK( IWK2 ), 1, VF, 1 )
      CALL SCOPY( K, WORK( IWK3 ), 1, VL, 1 )
*
      RETURN
*
*     End of SLASD8
*
      END

