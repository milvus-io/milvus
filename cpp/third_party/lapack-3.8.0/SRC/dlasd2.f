*> \brief \b DLASD2 merges the two sets of singular values together into a single sorted set. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLASD2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasd2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasd2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasd2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLASD2( NL, NR, SQRE, K, D, Z, ALPHA, BETA, U, LDU, VT,
*                          LDVT, DSIGMA, U2, LDU2, VT2, LDVT2, IDXP, IDX,
*                          IDXC, IDXQ, COLTYP, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, K, LDU, LDU2, LDVT, LDVT2, NL, NR, SQRE
*       DOUBLE PRECISION   ALPHA, BETA
*       ..
*       .. Array Arguments ..
*       INTEGER            COLTYP( * ), IDX( * ), IDXC( * ), IDXP( * ),
*      $                   IDXQ( * )
*       DOUBLE PRECISION   D( * ), DSIGMA( * ), U( LDU, * ),
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
*> DLASD2 merges the two sets of singular values together into a single
*> sorted set.  Then it tries to deflate the size of the problem.
*> There are two ways in which deflation can occur:  when two or more
*> singular values are close together or if there is a tiny entry in the
*> Z vector.  For each such occurrence the order of the related secular
*> equation problem is reduced by one.
*>
*> DLASD2 is called from DLASD1.
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
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*>         Contains the dimension of the non-deflated matrix,
*>         This is the order of the related secular equation. 1 <= K <=N.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension(N)
*>         On entry D contains the singular values of the two submatrices
*>         to be combined.  On exit D contains the trailing (N-K) updated
*>         singular values (those which were deflated) sorted into
*>         increasing order.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension(N)
*>         On exit Z contains the updating row vector in the secular
*>         equation.
*> \endverbatim
*>
*> \param[in] ALPHA
*> \verbatim
*>          ALPHA is DOUBLE PRECISION
*>         Contains the diagonal element associated with the added row.
*> \endverbatim
*>
*> \param[in] BETA
*> \verbatim
*>          BETA is DOUBLE PRECISION
*>         Contains the off-diagonal element associated with the added
*>         row.
*> \endverbatim
*>
*> \param[in,out] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension(LDU,N)
*>         On entry U contains the left singular vectors of two
*>         submatrices in the two square blocks with corners at (1,1),
*>         (NL, NL), and (NL+2, NL+2), (N,N).
*>         On exit U contains the trailing (N-K) updated left singular
*>         vectors (those which were deflated) in its last N-K columns.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>         The leading dimension of the array U.  LDU >= N.
*> \endverbatim
*>
*> \param[in,out] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension(LDVT,M)
*>         On entry VT**T contains the right singular vectors of two
*>         submatrices in the two square blocks with corners at (1,1),
*>         (NL+1, NL+1), and (NL+2, NL+2), (M,M).
*>         On exit VT**T contains the trailing (N-K) updated right singular
*>         vectors (those which were deflated) in its last N-K columns.
*>         In case SQRE =1, the last row of VT spans the right null
*>         space.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>         The leading dimension of the array VT.  LDVT >= M.
*> \endverbatim
*>
*> \param[out] DSIGMA
*> \verbatim
*>          DSIGMA is DOUBLE PRECISION array, dimension (N)
*>         Contains a copy of the diagonal elements (K-1 singular values
*>         and one zero) in the secular equation.
*> \endverbatim
*>
*> \param[out] U2
*> \verbatim
*>          U2 is DOUBLE PRECISION array, dimension(LDU2,N)
*>         Contains a copy of the first K-1 left singular vectors which
*>         will be used by DLASD3 in a matrix multiply (DGEMM) to solve
*>         for the new left singular vectors. U2 is arranged into four
*>         blocks. The first block contains a column with 1 at NL+1 and
*>         zero everywhere else; the second block contains non-zero
*>         entries only at and above NL; the third contains non-zero
*>         entries only below NL+1; and the fourth is dense.
*> \endverbatim
*>
*> \param[in] LDU2
*> \verbatim
*>          LDU2 is INTEGER
*>         The leading dimension of the array U2.  LDU2 >= N.
*> \endverbatim
*>
*> \param[out] VT2
*> \verbatim
*>          VT2 is DOUBLE PRECISION array, dimension(LDVT2,N)
*>         VT2**T contains a copy of the first K right singular vectors
*>         which will be used by DLASD3 in a matrix multiply (DGEMM) to
*>         solve for the new right singular vectors. VT2 is arranged into
*>         three blocks. The first block contains a row that corresponds
*>         to the special 0 diagonal element in SIGMA; the second block
*>         contains non-zeros only at and before NL +1; the third block
*>         contains non-zeros only at and after  NL +2.
*> \endverbatim
*>
*> \param[in] LDVT2
*> \verbatim
*>          LDVT2 is INTEGER
*>         The leading dimension of the array VT2.  LDVT2 >= M.
*> \endverbatim
*>
*> \param[out] IDXP
*> \verbatim
*>          IDXP is INTEGER array, dimension(N)
*>         This will contain the permutation used to place deflated
*>         values of D at the end of the array. On output IDXP(2:K)
*>         points to the nondeflated D-values and IDXP(K+1:N)
*>         points to the deflated singular values.
*> \endverbatim
*>
*> \param[out] IDX
*> \verbatim
*>          IDX is INTEGER array, dimension(N)
*>         This will contain the permutation used to sort the contents of
*>         D into ascending order.
*> \endverbatim
*>
*> \param[out] IDXC
*> \verbatim
*>          IDXC is INTEGER array, dimension(N)
*>         This will contain the permutation used to arrange the columns
*>         of the deflated U matrix into three groups:  the first group
*>         contains non-zero entries only at and above NL, the second
*>         contains non-zero entries only below NL+2, and the third is
*>         dense.
*> \endverbatim
*>
*> \param[in,out] IDXQ
*> \verbatim
*>          IDXQ is INTEGER array, dimension(N)
*>         This contains the permutation which separately sorts the two
*>         sub-problems in D into ascending order.  Note that entries in
*>         the first hlaf of this permutation must first be moved one
*>         position backward; and entries in the second half
*>         must first have NL+1 added to their values.
*> \endverbatim
*>
*> \param[out] COLTYP
*> \verbatim
*>          COLTYP is INTEGER array, dimension(N)
*>         As workspace, this will contain a label which will indicate
*>         which of the following types a column in the U2 matrix or a
*>         row in the VT2 matrix is:
*>         1 : non-zero in the upper half only
*>         2 : non-zero in the lower half only
*>         3 : dense
*>         4 : deflated
*>
*>         On exit, it is an array of dimension 4, with COLTYP(I) being
*>         the dimension of the I-th type columns.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
      SUBROUTINE DLASD2( NL, NR, SQRE, K, D, Z, ALPHA, BETA, U, LDU, VT,
     $                   LDVT, DSIGMA, U2, LDU2, VT2, LDVT2, IDXP, IDX,
     $                   IDXC, IDXQ, COLTYP, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, K, LDU, LDU2, LDVT, LDVT2, NL, NR, SQRE
      DOUBLE PRECISION   ALPHA, BETA
*     ..
*     .. Array Arguments ..
      INTEGER            COLTYP( * ), IDX( * ), IDXC( * ), IDXP( * ),
     $                   IDXQ( * )
      DOUBLE PRECISION   D( * ), DSIGMA( * ), U( LDU, * ),
     $                   U2( LDU2, * ), VT( LDVT, * ), VT2( LDVT2, * ),
     $                   Z( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO, EIGHT
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TWO = 2.0D+0,
     $                   EIGHT = 8.0D+0 )
*     ..
*     .. Local Arrays ..
      INTEGER            CTOT( 4 ), PSM( 4 )
*     ..
*     .. Local Scalars ..
      INTEGER            CT, I, IDXI, IDXJ, IDXJP, J, JP, JPREV, K2, M,
     $                   N, NLP1, NLP2
      DOUBLE PRECISION   C, EPS, HLFTOL, S, TAU, TOL, Z1
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLAPY2
      EXTERNAL           DLAMCH, DLAPY2
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLACPY, DLAMRG, DLASET, DROT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
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
*
      IF( LDU.LT.N ) THEN
         INFO = -10
      ELSE IF( LDVT.LT.M ) THEN
         INFO = -12
      ELSE IF( LDU2.LT.N ) THEN
         INFO = -15
      ELSE IF( LDVT2.LT.M ) THEN
         INFO = -17
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLASD2', -INFO )
         RETURN
      END IF
*
      NLP1 = NL + 1
      NLP2 = NL + 2
*
*     Generate the first part of the vector Z; and move the singular
*     values in the first part of D one position backward.
*
      Z1 = ALPHA*VT( NLP1, NLP1 )
      Z( 1 ) = Z1
      DO 10 I = NL, 1, -1
         Z( I+1 ) = ALPHA*VT( I, NLP1 )
         D( I+1 ) = D( I )
         IDXQ( I+1 ) = IDXQ( I ) + 1
   10 CONTINUE
*
*     Generate the second part of the vector Z.
*
      DO 20 I = NLP2, M
         Z( I ) = BETA*VT( I, NLP2 )
   20 CONTINUE
*
*     Initialize some reference arrays.
*
      DO 30 I = 2, NLP1
         COLTYP( I ) = 1
   30 CONTINUE
      DO 40 I = NLP2, N
         COLTYP( I ) = 2
   40 CONTINUE
*
*     Sort the singular values into increasing order
*
      DO 50 I = NLP2, N
         IDXQ( I ) = IDXQ( I ) + NLP1
   50 CONTINUE
*
*     DSIGMA, IDXC, IDXC, and the first column of U2
*     are used as storage space.
*
      DO 60 I = 2, N
         DSIGMA( I ) = D( IDXQ( I ) )
         U2( I, 1 ) = Z( IDXQ( I ) )
         IDXC( I ) = COLTYP( IDXQ( I ) )
   60 CONTINUE
*
      CALL DLAMRG( NL, NR, DSIGMA( 2 ), 1, 1, IDX( 2 ) )
*
      DO 70 I = 2, N
         IDXI = 1 + IDX( I )
         D( I ) = DSIGMA( IDXI )
         Z( I ) = U2( IDXI, 1 )
         COLTYP( I ) = IDXC( IDXI )
   70 CONTINUE
*
*     Calculate the allowable deflation tolerance
*
      EPS = DLAMCH( 'Epsilon' )
      TOL = MAX( ABS( ALPHA ), ABS( BETA ) )
      TOL = EIGHT*EPS*MAX( ABS( D( N ) ), TOL )
*
*     There are 2 kinds of deflation -- first a value in the z-vector
*     is small, second two (or more) singular values are very close
*     together (their difference is small).
*
*     If the value in the z-vector is small, we simply permute the
*     array so that the corresponding singular value is moved to the
*     end.
*
*     If two values in the D-vector are close, we perform a two-sided
*     rotation designed to make one of the corresponding z-vector
*     entries zero, and then permute the array so that the deflated
*     singular value is moved to the end.
*
*     If there are multiple singular values then the problem deflates.
*     Here the number of equal singular values are found.  As each equal
*     singular value is found, an elementary reflector is computed to
*     rotate the corresponding singular subspace so that the
*     corresponding components of Z are zero in this new basis.
*
      K = 1
      K2 = N + 1
      DO 80 J = 2, N
         IF( ABS( Z( J ) ).LE.TOL ) THEN
*
*           Deflate due to small z component.
*
            K2 = K2 - 1
            IDXP( K2 ) = J
            COLTYP( J ) = 4
            IF( J.EQ.N )
     $         GO TO 120
         ELSE
            JPREV = J
            GO TO 90
         END IF
   80 CONTINUE
   90 CONTINUE
      J = JPREV
  100 CONTINUE
      J = J + 1
      IF( J.GT.N )
     $   GO TO 110
      IF( ABS( Z( J ) ).LE.TOL ) THEN
*
*        Deflate due to small z component.
*
         K2 = K2 - 1
         IDXP( K2 ) = J
         COLTYP( J ) = 4
      ELSE
*
*        Check if singular values are close enough to allow deflation.
*
         IF( ABS( D( J )-D( JPREV ) ).LE.TOL ) THEN
*
*           Deflation is possible.
*
            S = Z( JPREV )
            C = Z( J )
*
*           Find sqrt(a**2+b**2) without overflow or
*           destructive underflow.
*
            TAU = DLAPY2( C, S )
            C = C / TAU
            S = -S / TAU
            Z( J ) = TAU
            Z( JPREV ) = ZERO
*
*           Apply back the Givens rotation to the left and right
*           singular vector matrices.
*
            IDXJP = IDXQ( IDX( JPREV )+1 )
            IDXJ = IDXQ( IDX( J )+1 )
            IF( IDXJP.LE.NLP1 ) THEN
               IDXJP = IDXJP - 1
            END IF
            IF( IDXJ.LE.NLP1 ) THEN
               IDXJ = IDXJ - 1
            END IF
            CALL DROT( N, U( 1, IDXJP ), 1, U( 1, IDXJ ), 1, C, S )
            CALL DROT( M, VT( IDXJP, 1 ), LDVT, VT( IDXJ, 1 ), LDVT, C,
     $                 S )
            IF( COLTYP( J ).NE.COLTYP( JPREV ) ) THEN
               COLTYP( J ) = 3
            END IF
            COLTYP( JPREV ) = 4
            K2 = K2 - 1
            IDXP( K2 ) = JPREV
            JPREV = J
         ELSE
            K = K + 1
            U2( K, 1 ) = Z( JPREV )
            DSIGMA( K ) = D( JPREV )
            IDXP( K ) = JPREV
            JPREV = J
         END IF
      END IF
      GO TO 100
  110 CONTINUE
*
*     Record the last singular value.
*
      K = K + 1
      U2( K, 1 ) = Z( JPREV )
      DSIGMA( K ) = D( JPREV )
      IDXP( K ) = JPREV
*
  120 CONTINUE
*
*     Count up the total number of the various types of columns, then
*     form a permutation which positions the four column types into
*     four groups of uniform structure (although one or more of these
*     groups may be empty).
*
      DO 130 J = 1, 4
         CTOT( J ) = 0
  130 CONTINUE
      DO 140 J = 2, N
         CT = COLTYP( J )
         CTOT( CT ) = CTOT( CT ) + 1
  140 CONTINUE
*
*     PSM(*) = Position in SubMatrix (of types 1 through 4)
*
      PSM( 1 ) = 2
      PSM( 2 ) = 2 + CTOT( 1 )
      PSM( 3 ) = PSM( 2 ) + CTOT( 2 )
      PSM( 4 ) = PSM( 3 ) + CTOT( 3 )
*
*     Fill out the IDXC array so that the permutation which it induces
*     will place all type-1 columns first, all type-2 columns next,
*     then all type-3's, and finally all type-4's, starting from the
*     second column. This applies similarly to the rows of VT.
*
      DO 150 J = 2, N
         JP = IDXP( J )
         CT = COLTYP( JP )
         IDXC( PSM( CT ) ) = J
         PSM( CT ) = PSM( CT ) + 1
  150 CONTINUE
*
*     Sort the singular values and corresponding singular vectors into
*     DSIGMA, U2, and VT2 respectively.  The singular values/vectors
*     which were not deflated go into the first K slots of DSIGMA, U2,
*     and VT2 respectively, while those which were deflated go into the
*     last N - K slots, except that the first column/row will be treated
*     separately.
*
      DO 160 J = 2, N
         JP = IDXP( J )
         DSIGMA( J ) = D( JP )
         IDXJ = IDXQ( IDX( IDXP( IDXC( J ) ) )+1 )
         IF( IDXJ.LE.NLP1 ) THEN
            IDXJ = IDXJ - 1
         END IF
         CALL DCOPY( N, U( 1, IDXJ ), 1, U2( 1, J ), 1 )
         CALL DCOPY( M, VT( IDXJ, 1 ), LDVT, VT2( J, 1 ), LDVT2 )
  160 CONTINUE
*
*     Determine DSIGMA(1), DSIGMA(2) and Z(1)
*
      DSIGMA( 1 ) = ZERO
      HLFTOL = TOL / TWO
      IF( ABS( DSIGMA( 2 ) ).LE.HLFTOL )
     $   DSIGMA( 2 ) = HLFTOL
      IF( M.GT.N ) THEN
         Z( 1 ) = DLAPY2( Z1, Z( M ) )
         IF( Z( 1 ).LE.TOL ) THEN
            C = ONE
            S = ZERO
            Z( 1 ) = TOL
         ELSE
            C = Z1 / Z( 1 )
            S = Z( M ) / Z( 1 )
         END IF
      ELSE
         IF( ABS( Z1 ).LE.TOL ) THEN
            Z( 1 ) = TOL
         ELSE
            Z( 1 ) = Z1
         END IF
      END IF
*
*     Move the rest of the updating row to Z.
*
      CALL DCOPY( K-1, U2( 2, 1 ), 1, Z( 2 ), 1 )
*
*     Determine the first column of U2, the first row of VT2 and the
*     last row of VT.
*
      CALL DLASET( 'A', N, 1, ZERO, ZERO, U2, LDU2 )
      U2( NLP1, 1 ) = ONE
      IF( M.GT.N ) THEN
         DO 170 I = 1, NLP1
            VT( M, I ) = -S*VT( NLP1, I )
            VT2( 1, I ) = C*VT( NLP1, I )
  170    CONTINUE
         DO 180 I = NLP2, M
            VT2( 1, I ) = S*VT( M, I )
            VT( M, I ) = C*VT( M, I )
  180    CONTINUE
      ELSE
         CALL DCOPY( M, VT( NLP1, 1 ), LDVT, VT2( 1, 1 ), LDVT2 )
      END IF
      IF( M.GT.N ) THEN
         CALL DCOPY( M, VT( M, 1 ), LDVT, VT2( M, 1 ), LDVT2 )
      END IF
*
*     The deflated singular values and their corresponding vectors go
*     into the back of D, U, and V respectively.
*
      IF( N.GT.K ) THEN
         CALL DCOPY( N-K, DSIGMA( K+1 ), 1, D( K+1 ), 1 )
         CALL DLACPY( 'A', N, N-K, U2( 1, K+1 ), LDU2, U( 1, K+1 ),
     $                LDU )
         CALL DLACPY( 'A', N-K, M, VT2( K+1, 1 ), LDVT2, VT( K+1, 1 ),
     $                LDVT )
      END IF
*
*     Copy CTOT into COLTYP for referencing in DLASD3.
*
      DO 190 J = 1, 4
         COLTYP( J ) = CTOT( J )
  190 CONTINUE
*
      RETURN
*
*     End of DLASD2
*
      END
