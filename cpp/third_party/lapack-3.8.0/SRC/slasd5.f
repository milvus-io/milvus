*> \brief \b SLASD5 computes the square root of the i-th eigenvalue of a positive symmetric rank-one modification of a 2-by-2 diagonal matrix. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLASD5 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasd5.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasd5.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasd5.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLASD5( I, D, Z, DELTA, RHO, DSIGMA, WORK )
*
*       .. Scalar Arguments ..
*       INTEGER            I
*       REAL               DSIGMA, RHO
*       ..
*       .. Array Arguments ..
*       REAL               D( 2 ), DELTA( 2 ), WORK( 2 ), Z( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This subroutine computes the square root of the I-th eigenvalue
*> of a positive symmetric rank-one modification of a 2-by-2 diagonal
*> matrix
*>
*>            diag( D ) * diag( D ) +  RHO * Z * transpose(Z) .
*>
*> The diagonal entries in the array D are assumed to satisfy
*>
*>            0 <= D(i) < D(j)  for  i < j .
*>
*> We also assume RHO > 0 and that the Euclidean norm of the vector
*> Z is one.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] I
*> \verbatim
*>          I is INTEGER
*>         The index of the eigenvalue to be computed.  I = 1 or I = 2.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (2)
*>         The original eigenvalues.  We assume 0 <= D(1) < D(2).
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is REAL array, dimension (2)
*>         The components of the updating vector.
*> \endverbatim
*>
*> \param[out] DELTA
*> \verbatim
*>          DELTA is REAL array, dimension (2)
*>         Contains (D(j) - sigma_I) in its  j-th component.
*>         The vector DELTA contains the information necessary
*>         to construct the eigenvectors.
*> \endverbatim
*>
*> \param[in] RHO
*> \verbatim
*>          RHO is REAL
*>         The scalar in the symmetric updating formula.
*> \endverbatim
*>
*> \param[out] DSIGMA
*> \verbatim
*>          DSIGMA is REAL
*>         The computed sigma_I, the I-th updated eigenvalue.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (2)
*>         WORK contains (D(j) + sigma_I) in its  j-th component.
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
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Ren-Cang Li, Computer Science Division, University of California
*>     at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE SLASD5( I, D, Z, DELTA, RHO, DSIGMA, WORK )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            I
      REAL               DSIGMA, RHO
*     ..
*     .. Array Arguments ..
      REAL               D( 2 ), DELTA( 2 ), WORK( 2 ), Z( 2 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO, THREE, FOUR
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0,
     $                   THREE = 3.0E+0, FOUR = 4.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               B, C, DEL, DELSQ, TAU, W
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, SQRT
*     ..
*     .. Executable Statements ..
*
      DEL = D( 2 ) - D( 1 )
      DELSQ = DEL*( D( 2 )+D( 1 ) )
      IF( I.EQ.1 ) THEN
         W = ONE + FOUR*RHO*( Z( 2 )*Z( 2 ) / ( D( 1 )+THREE*D( 2 ) )-
     $       Z( 1 )*Z( 1 ) / ( THREE*D( 1 )+D( 2 ) ) ) / DEL
         IF( W.GT.ZERO ) THEN
            B = DELSQ + RHO*( Z( 1 )*Z( 1 )+Z( 2 )*Z( 2 ) )
            C = RHO*Z( 1 )*Z( 1 )*DELSQ
*
*           B > ZERO, always
*
*           The following TAU is DSIGMA * DSIGMA - D( 1 ) * D( 1 )
*
            TAU = TWO*C / ( B+SQRT( ABS( B*B-FOUR*C ) ) )
*
*           The following TAU is DSIGMA - D( 1 )
*
            TAU = TAU / ( D( 1 )+SQRT( D( 1 )*D( 1 )+TAU ) )
            DSIGMA = D( 1 ) + TAU
            DELTA( 1 ) = -TAU
            DELTA( 2 ) = DEL - TAU
            WORK( 1 ) = TWO*D( 1 ) + TAU
            WORK( 2 ) = ( D( 1 )+TAU ) + D( 2 )
*           DELTA( 1 ) = -Z( 1 ) / TAU
*           DELTA( 2 ) = Z( 2 ) / ( DEL-TAU )
         ELSE
            B = -DELSQ + RHO*( Z( 1 )*Z( 1 )+Z( 2 )*Z( 2 ) )
            C = RHO*Z( 2 )*Z( 2 )*DELSQ
*
*           The following TAU is DSIGMA * DSIGMA - D( 2 ) * D( 2 )
*
            IF( B.GT.ZERO ) THEN
               TAU = -TWO*C / ( B+SQRT( B*B+FOUR*C ) )
            ELSE
               TAU = ( B-SQRT( B*B+FOUR*C ) ) / TWO
            END IF
*
*           The following TAU is DSIGMA - D( 2 )
*
            TAU = TAU / ( D( 2 )+SQRT( ABS( D( 2 )*D( 2 )+TAU ) ) )
            DSIGMA = D( 2 ) + TAU
            DELTA( 1 ) = -( DEL+TAU )
            DELTA( 2 ) = -TAU
            WORK( 1 ) = D( 1 ) + TAU + D( 2 )
            WORK( 2 ) = TWO*D( 2 ) + TAU
*           DELTA( 1 ) = -Z( 1 ) / ( DEL+TAU )
*           DELTA( 2 ) = -Z( 2 ) / TAU
         END IF
*        TEMP = SQRT( DELTA( 1 )*DELTA( 1 )+DELTA( 2 )*DELTA( 2 ) )
*        DELTA( 1 ) = DELTA( 1 ) / TEMP
*        DELTA( 2 ) = DELTA( 2 ) / TEMP
      ELSE
*
*        Now I=2
*
         B = -DELSQ + RHO*( Z( 1 )*Z( 1 )+Z( 2 )*Z( 2 ) )
         C = RHO*Z( 2 )*Z( 2 )*DELSQ
*
*        The following TAU is DSIGMA * DSIGMA - D( 2 ) * D( 2 )
*
         IF( B.GT.ZERO ) THEN
            TAU = ( B+SQRT( B*B+FOUR*C ) ) / TWO
         ELSE
            TAU = TWO*C / ( -B+SQRT( B*B+FOUR*C ) )
         END IF
*
*        The following TAU is DSIGMA - D( 2 )
*
         TAU = TAU / ( D( 2 )+SQRT( D( 2 )*D( 2 )+TAU ) )
         DSIGMA = D( 2 ) + TAU
         DELTA( 1 ) = -( DEL+TAU )
         DELTA( 2 ) = -TAU
         WORK( 1 ) = D( 1 ) + TAU + D( 2 )
         WORK( 2 ) = TWO*D( 2 ) + TAU
*        DELTA( 1 ) = -Z( 1 ) / ( DEL+TAU )
*        DELTA( 2 ) = -Z( 2 ) / TAU
*        TEMP = SQRT( DELTA( 1 )*DELTA( 1 )+DELTA( 2 )*DELTA( 2 ) )
*        DELTA( 1 ) = DELTA( 1 ) / TEMP
*        DELTA( 2 ) = DELTA( 2 ) / TEMP
      END IF
      RETURN
*
*     End of SLASD5
*
      END
