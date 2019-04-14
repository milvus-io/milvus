*> \brief \b DLASD3 finds all square roots of the roots of the secular equation, as defined by the values in D and Z, and then updates the singular vectors by matrix multiplication. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLASD3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasd3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasd3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasd3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLASD3( NL, NR, SQRE, K, D, Q, LDQ, DSIGMA, U, LDU, U2,
*                          LDU2, VT, LDVT, VT2, LDVT2, IDXC, CTOT, Z,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, K, LDQ, LDU, LDU2, LDVT, LDVT2, NL, NR,
*      $                   SQRE
*       ..
*       .. Array Arguments ..
*       INTEGER            CTOT( * ), IDXC( * )
*       DOUBLE PRECISION   D( * ), DSIGMA( * ), Q( LDQ, * ), U( LDU, * ),
*      $                   U2( LDU2, * ), VT( LDVT, * ), VT2( LDVT2, * ),
*      $                   Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLASD3 finds all the square roots of the roots of the secular
*> equation, as defined by the values in D and Z.  It makes the
*> appropriate calls to DLASD4 and then updates the singular
*> vectors by matrix multiplication.
*>
*> This code makes very mild assumptions about floating point
*> arithmetic. It will work on machines with a guard digit in
*> add/subtract, or on those binary machines without guard digits
*> which subtract like the Cray XMP, Cray YMP, Cray C 90, or Cray 2.
*> It could conceivably fail on hexadecimal or decimal machines
*> without guard digits, but we know of none.
*>
*> DLASD3 is called from DLASD1.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NL
*> \verbatim
*>          NL is INTEGER
*>         The row dimension of the upper block.  NL >= 1.
*> \endverbatim
*>
*> \param[in] NR
*> \verbatim
*>          NR is INTEGER
*>         The row dimension of the lower block.  NR >= 1.
*> \endverbatim
*>
*> \param[in] SQRE
*> \verbatim
*>          SQRE is INTEGER
*>         = 0: the lower block is an NR-by-NR square matrix.
*>         = 1: the lower block is an NR-by-(NR+1) rectangular matrix.
*>
*>         The bidiagonal matrix has N = NL + NR + 1 rows and
*>         M = N + SQRE >= N columns.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>         The size of the secular equation, 1 =< K = < N.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension(K)
*>         On exit the square roots of the roots of the secular equation,
*>         in ascending order.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ,K)
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>         The leading dimension of the array Q.  LDQ >= K.
*> \endverbatim
*>
*> \param[in,out] DSIGMA
*> \verbatim
*>          DSIGMA is DOUBLE PRECISION array, dimension(K)
*>         The first K elements of this array contain the old roots
*>         of the deflated updating problem.  These are the poles
*>         of the secular equation.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU, N)
*>         The last N - K columns of this matrix contain the deflated
*>         left singular vectors.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>         The leading dimension of the array U.  LDU >= N.
*> \endverbatim
*>
*> \param[in] U2
*> \verbatim
*>          U2 is DOUBLE PRECISION array, dimension (LDU2, N)
*>         The first K columns of this matrix contain the non-deflated
*>         left singular vectors for the split problem.
*> \endverbatim
*>
*> \param[in] LDU2
*> \verbatim
*>          LDU2 is INTEGER
*>         The leading dimension of the array U2.  LDU2 >= N.
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension (LDVT, M)
*>         The last M - K columns of VT**T contain the deflated
*>         right singular vectors.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>         The leading dimension of the array VT.  LDVT >= N.
*> \endverbatim
*>
*> \param[in,out] VT2
*> \verbatim
*>          VT2 is DOUBLE PRECISION array, dimension (LDVT2, N)
*>         The first K columns of VT2**T contain the non-deflated
*>         right singular vectors for the split problem.
*> \endverbatim
*>
*> \param[in] LDVT2
*> \verbatim
*>          LDVT2 is INTEGER
*>         The leading dimension of the array VT2.  LDVT2 >= N.
*> \endverbatim
*>
*> \param[in] IDXC
*> \verbatim
*>          IDXC is INTEGER array, dimension ( N )
*>         The permutation used to arrange the columns of U (and rows of
*>         VT) into three groups:  the first group contains non-zero
*>         entries only at and above (or before) NL +1; the second
*>         contains non-zero entries only at and below (or after) NL+2;
*>         and the third is dense. The first column of U and the row of
*>         VT are treated separately, however.
*>
*>         The rows of the singular vectors found by DLASD4
*>         must be likewise permuted before the matrix multiplies can
*>         take place.
*> \endverbatim
*>
*> \param[in] CTOT
*> \verbatim
*>          CTOT is INTEGER array, dimension ( 4 )
*>         A count of the total number of the various types of columns
*>         in U (or rows in VT), as described in IDXC. The fourth column
*>         type is any column which has been deflated.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (K)
*>         The first K elements of this array contain the components
*>         of the deflation-adjusted updating row vector.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>         = 0:  successful exit.
*>         < 0:  if INFO = -i, the i-th argument had an illegal value.
*>         > 0:  if INFO = 1, a singular value did not converge
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
      SUBROUTINE DLASD3( NL, NR, SQRE, K, D, Q, LDQ, DSIGMA, U, LDU, U2,
     $                   LDU2, VT, LDVT, VT2, LDVT2, IDXC, CTOT, Z,
     $                   INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, K, LDQ, LDU, LDU2, LDVT, LDVT2, NL, NR,
     $                   SQRE
*     ..
*     .. Array Arguments ..
      INTEGER            CTOT( * ), IDXC( * )
      DOUBLE PRECISION   D( * ), DSIGMA( * ), Q( LDQ, * ), U( LDU, * ),
     $                   U2( LDU2, * ), VT( LDVT, * ), VT2( LDVT2, * ),
     $                   Z( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO, NEGONE
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0,
     $                   NEGONE = -1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            CTEMP, I, J, JC, KTEMP, M, N, NLP1, NLP2, NRP1
      DOUBLE PRECISION   RHO, TEMP
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMC3, DNRM2
      EXTERNAL           DLAMC3, DNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DGEMM, DLACPY, DLASCL, DLASD4, XERBLA
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
      IF( NL.LT.1 ) THEN
         INFO = -1
      ELSE IF( NR.LT.1 ) THEN
         INFO = -2
      ELSE IF( ( SQRE.NE.1 ) .AND. ( SQRE.NE.0 ) ) THEN
         INFO = -3
      END IF
*
      N = NL + NR + 1
      M = N + SQRE
      NLP1 = NL + 1
      NLP2 = NL + 2
*
      IF( ( K.LT.1 ) .OR. ( K.GT.N ) ) THEN
         INFO = -4
      ELSE IF( LDQ.LT.K ) THEN
         INFO = -7
      ELSE IF( LDU.LT.N ) THEN
         INFO = -10
      ELSE IF( LDU2.LT.N ) THEN
         INFO = -12
      ELSE IF( LDVT.LT.M ) THEN
         INFO = -14
      ELSE IF( LDVT2.LT.M ) THEN
         INFO = -16
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLASD3', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( K.EQ.1 ) THEN
         D( 1 ) = ABS( Z( 1 ) )
         CALL DCOPY( M, VT2( 1, 1 ), LDVT2, VT( 1, 1 ), LDVT )
         IF( Z( 1 ).GT.ZERO ) THEN
            CALL DCOPY( N, U2( 1, 1 ), 1, U( 1, 1 ), 1 )
         ELSE
            DO 10 I = 1, N
               U( I, 1 ) = -U2( I, 1 )
   10       CONTINUE
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
*     2*DSIGMA(I) to prevent optimizing compilers from eliminating
*     this code.
*
      DO 20 I = 1, K
         DSIGMA( I ) = DLAMC3( DSIGMA( I ), DSIGMA( I ) ) - DSIGMA( I )
   20 CONTINUE
*
*     Keep a copy of Z.
*
      CALL DCOPY( K, Z, 1, Q, 1 )
*
*     Normalize Z.
*
      RHO = DNRM2( K, Z, 1 )
      CALL DLASCL( 'G', 0, 0, RHO, ONE, K, 1, Z, K, INFO )
      RHO = RHO*RHO
*
*     Find the new singular values.
*
      DO 30 J = 1, K
         CALL DLASD4( K, J, DSIGMA, Z, U( 1, J ), RHO, D( J ),
     $                VT( 1, J ), INFO )
*
*        If the zero finder fails, report the convergence failure.
*
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
   30 CONTINUE
*
*     Compute updated Z.
*
      DO 60 I = 1, K
         Z( I ) = U( I, K )*VT( I, K )
         DO 40 J = 1, I - 1
            Z( I ) = Z( I )*( U( I, J )*VT( I, J ) /
     $               ( DSIGMA( I )-DSIGMA( J ) ) /
     $               ( DSIGMA( I )+DSIGMA( J ) ) )
   40    CONTINUE
         DO 50 J = I, K - 1
            Z( I ) = Z( I )*( U( I, J )*VT( I, J ) /
     $               ( DSIGMA( I )-DSIGMA( J+1 ) ) /
     $               ( DSIGMA( I )+DSIGMA( J+1 ) ) )
   50    CONTINUE
         Z( I ) = SIGN( SQRT( ABS( Z( I ) ) ), Q( I, 1 ) )
   60 CONTINUE
*
*     Compute left singular vectors of the modified diagonal matrix,
*     and store related information for the right singular vectors.
*
      DO 90 I = 1, K
         VT( 1, I ) = Z( 1 ) / U( 1, I ) / VT( 1, I )
         U( 1, I ) = NEGONE
         DO 70 J = 2, K
            VT( J, I ) = Z( J ) / U( J, I ) / VT( J, I )
            U( J, I ) = DSIGMA( J )*VT( J, I )
   70    CONTINUE
         TEMP = DNRM2( K, U( 1, I ), 1 )
         Q( 1, I ) = U( 1, I ) / TEMP
         DO 80 J = 2, K
            JC = IDXC( J )
            Q( J, I ) = U( JC, I ) / TEMP
   80    CONTINUE
   90 CONTINUE
*
*     Update the left singular vector matrix.
*
      IF( K.EQ.2 ) THEN
         CALL DGEMM( 'N', 'N', N, K, K, ONE, U2, LDU2, Q, LDQ, ZERO, U,
     $               LDU )
         GO TO 100
      END IF
      IF( CTOT( 1 ).GT.0 ) THEN
         CALL DGEMM( 'N', 'N', NL, K, CTOT( 1 ), ONE, U2( 1, 2 ), LDU2,
     $               Q( 2, 1 ), LDQ, ZERO, U( 1, 1 ), LDU )
         IF( CTOT( 3 ).GT.0 ) THEN
            KTEMP = 2 + CTOT( 1 ) + CTOT( 2 )
            CALL DGEMM( 'N', 'N', NL, K, CTOT( 3 ), ONE, U2( 1, KTEMP ),
     $                  LDU2, Q( KTEMP, 1 ), LDQ, ONE, U( 1, 1 ), LDU )
         END IF
      ELSE IF( CTOT( 3 ).GT.0 ) THEN
         KTEMP = 2 + CTOT( 1 ) + CTOT( 2 )
         CALL DGEMM( 'N', 'N', NL, K, CTOT( 3 ), ONE, U2( 1, KTEMP ),
     $               LDU2, Q( KTEMP, 1 ), LDQ, ZERO, U( 1, 1 ), LDU )
      ELSE
         CALL DLACPY( 'F', NL, K, U2, LDU2, U, LDU )
      END IF
      CALL DCOPY( K, Q( 1, 1 ), LDQ, U( NLP1, 1 ), LDU )
      KTEMP = 2 + CTOT( 1 )
      CTEMP = CTOT( 2 ) + CTOT( 3 )
      CALL DGEMM( 'N', 'N', NR, K, CTEMP, ONE, U2( NLP2, KTEMP ), LDU2,
     $            Q( KTEMP, 1 ), LDQ, ZERO, U( NLP2, 1 ), LDU )
*
*     Generate the right singular vectors.
*
  100 CONTINUE
      DO 120 I = 1, K
         TEMP = DNRM2( K, VT( 1, I ), 1 )
         Q( I, 1 ) = VT( 1, I ) / TEMP
         DO 110 J = 2, K
            JC = IDXC( J )
            Q( I, J ) = VT( JC, I ) / TEMP
  110    CONTINUE
  120 CONTINUE
*
*     Update the right singular vector matrix.
*
      IF( K.EQ.2 ) THEN
         CALL DGEMM( 'N', 'N', K, M, K, ONE, Q, LDQ, VT2, LDVT2, ZERO,
     $               VT, LDVT )
         RETURN
      END IF
      KTEMP = 1 + CTOT( 1 )
      CALL DGEMM( 'N', 'N', K, NLP1, KTEMP, ONE, Q( 1, 1 ), LDQ,
     $            VT2( 1, 1 ), LDVT2, ZERO, VT( 1, 1 ), LDVT )
      KTEMP = 2 + CTOT( 1 ) + CTOT( 2 )
      IF( KTEMP.LE.LDVT2 )
     $   CALL DGEMM( 'N', 'N', K, NLP1, CTOT( 3 ), ONE, Q( 1, KTEMP ),
     $               LDQ, VT2( KTEMP, 1 ), LDVT2, ONE, VT( 1, 1 ),
     $               LDVT )
*
      KTEMP = CTOT( 1 ) + 1
      NRP1 = NR + SQRE
      IF( KTEMP.GT.1 ) THEN
         DO 130 I = 1, K
            Q( I, KTEMP ) = Q( I, 1 )
  130    CONTINUE
         DO 140 I = NLP2, M
            VT2( KTEMP, I ) = VT2( 1, I )
  140    CONTINUE
      END IF
      CTEMP = 1 + CTOT( 2 ) + CTOT( 3 )
      CALL DGEMM( 'N', 'N', K, NRP1, CTEMP, ONE, Q( 1, KTEMP ), LDQ,
     $            VT2( KTEMP, NLP2 ), LDVT2, ZERO, VT( 1, NLP2 ), LDVT )
*
      RETURN
*
*     End of DLASD3
*
      END
