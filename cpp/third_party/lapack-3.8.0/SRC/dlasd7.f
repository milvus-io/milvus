*> \brief \b DLASD7 merges the two sets of singular values together into a single sorted set. Then it tries to deflate the size of the problem. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLASD7 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasd7.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasd7.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasd7.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLASD7( ICOMPQ, NL, NR, SQRE, K, D, Z, ZW, VF, VFW, VL,
*                          VLW, ALPHA, BETA, DSIGMA, IDX, IDXP, IDXQ,
*                          PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM,
*                          C, S, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            GIVPTR, ICOMPQ, INFO, K, LDGCOL, LDGNUM, NL,
*      $                   NR, SQRE
*       DOUBLE PRECISION   ALPHA, BETA, C, S
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( LDGCOL, * ), IDX( * ), IDXP( * ),
*      $                   IDXQ( * ), PERM( * )
*       DOUBLE PRECISION   D( * ), DSIGMA( * ), GIVNUM( LDGNUM, * ),
*      $                   VF( * ), VFW( * ), VL( * ), VLW( * ), Z( * ),
*      $                   ZW( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLASD7 merges the two sets of singular values together into a single
*> sorted set. Then it tries to deflate the size of the problem. There
*> are two ways in which deflation can occur:  when two or more singular
*> values are close together or if there is a tiny entry in the Z
*> vector. For each such occurrence the order of the related
*> secular equation problem is reduced by one.
*>
*> DLASD7 is called from DLASD6.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ICOMPQ
*> \verbatim
*>          ICOMPQ is INTEGER
*>          Specifies whether singular vectors are to be computed
*>          in compact form, as follows:
*>          = 0: Compute singular values only.
*>          = 1: Compute singular vectors of upper
*>               bidiagonal matrix in compact form.
*> \endverbatim
*>
*> \param[in] NL
*> \verbatim
*>          NL is INTEGER
*>         The row dimension of the upper block. NL >= 1.
*> \endverbatim
*>
*> \param[in] NR
*> \verbatim
*>          NR is INTEGER
*>         The row dimension of the lower block. NR >= 1.
*> \endverbatim
*>
*> \param[in] SQRE
*> \verbatim
*>          SQRE is INTEGER
*>         = 0: the lower block is an NR-by-NR square matrix.
*>         = 1: the lower block is an NR-by-(NR+1) rectangular matrix.
*>
*>         The bidiagonal matrix has
*>         N = NL + NR + 1 rows and
*>         M = N + SQRE >= N columns.
*> \endverbatim
*>
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*>         Contains the dimension of the non-deflated matrix, this is
*>         the order of the related secular equation. 1 <= K <=N.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension ( N )
*>         On entry D contains the singular values of the two submatrices
*>         to be combined. On exit D contains the trailing (N-K) updated
*>         singular values (those which were deflated) sorted into
*>         increasing order.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension ( M )
*>         On exit Z contains the updating row vector in the secular
*>         equation.
*> \endverbatim
*>
*> \param[out] ZW
*> \verbatim
*>          ZW is DOUBLE PRECISION array, dimension ( M )
*>         Workspace for Z.
*> \endverbatim
*>
*> \param[in,out] VF
*> \verbatim
*>          VF is DOUBLE PRECISION array, dimension ( M )
*>         On entry, VF(1:NL+1) contains the first components of all
*>         right singular vectors of the upper block; and VF(NL+2:M)
*>         contains the first components of all right singular vectors
*>         of the lower block. On exit, VF contains the first components
*>         of all right singular vectors of the bidiagonal matrix.
*> \endverbatim
*>
*> \param[out] VFW
*> \verbatim
*>          VFW is DOUBLE PRECISION array, dimension ( M )
*>         Workspace for VF.
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is DOUBLE PRECISION array, dimension ( M )
*>         On entry, VL(1:NL+1) contains the  last components of all
*>         right singular vectors of the upper block; and VL(NL+2:M)
*>         contains the last components of all right singular vectors
*>         of the lower block. On exit, VL contains the last components
*>         of all right singular vectors of the bidiagonal matrix.
*> \endverbatim
*>
*> \param[out] VLW
*> \verbatim
*>          VLW is DOUBLE PRECISION array, dimension ( M )
*>         Workspace for VL.
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
*> \param[out] DSIGMA
*> \verbatim
*>          DSIGMA is DOUBLE PRECISION array, dimension ( N )
*>         Contains a copy of the diagonal elements (K-1 singular values
*>         and one zero) in the secular equation.
*> \endverbatim
*>
*> \param[out] IDX
*> \verbatim
*>          IDX is INTEGER array, dimension ( N )
*>         This will contain the permutation used to sort the contents of
*>         D into ascending order.
*> \endverbatim
*>
*> \param[out] IDXP
*> \verbatim
*>          IDXP is INTEGER array, dimension ( N )
*>         This will contain the permutation used to place deflated
*>         values of D at the end of the array. On output IDXP(2:K)
*>         points to the nondeflated D-values and IDXP(K+1:N)
*>         points to the deflated singular values.
*> \endverbatim
*>
*> \param[in] IDXQ
*> \verbatim
*>          IDXQ is INTEGER array, dimension ( N )
*>         This contains the permutation which separately sorts the two
*>         sub-problems in D into ascending order.  Note that entries in
*>         the first half of this permutation must first be moved one
*>         position backward; and entries in the second half
*>         must first have NL+1 added to their values.
*> \endverbatim
*>
*> \param[out] PERM
*> \verbatim
*>          PERM is INTEGER array, dimension ( N )
*>         The permutations (from deflation and sorting) to be applied
*>         to each singular block. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[out] GIVPTR
*> \verbatim
*>          GIVPTR is INTEGER
*>         The number of Givens rotations which took place in this
*>         subproblem. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[out] GIVCOL
*> \verbatim
*>          GIVCOL is INTEGER array, dimension ( LDGCOL, 2 )
*>         Each pair of numbers indicates a pair of columns to take place
*>         in a Givens rotation. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[in] LDGCOL
*> \verbatim
*>          LDGCOL is INTEGER
*>         The leading dimension of GIVCOL, must be at least N.
*> \endverbatim
*>
*> \param[out] GIVNUM
*> \verbatim
*>          GIVNUM is DOUBLE PRECISION array, dimension ( LDGNUM, 2 )
*>         Each number indicates the C or S value to be used in the
*>         corresponding Givens rotation. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[in] LDGNUM
*> \verbatim
*>          LDGNUM is INTEGER
*>         The leading dimension of GIVNUM, must be at least N.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is DOUBLE PRECISION
*>         C contains garbage if SQRE =0 and the C-value of a Givens
*>         rotation related to the right null space if SQRE = 1.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION
*>         S contains garbage if SQRE =0 and the S-value of a Givens
*>         rotation related to the right null space if SQRE = 1.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>         = 0:  successful exit.
*>         < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*>     Ming Gu and Huan Ren, Computer Science Division, University of
*>     California at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE DLASD7( ICOMPQ, NL, NR, SQRE, K, D, Z, ZW, VF, VFW, VL,
     $                   VLW, ALPHA, BETA, DSIGMA, IDX, IDXP, IDXQ,
     $                   PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM,
     $                   C, S, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            GIVPTR, ICOMPQ, INFO, K, LDGCOL, LDGNUM, NL,
     $                   NR, SQRE
      DOUBLE PRECISION   ALPHA, BETA, C, S
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( LDGCOL, * ), IDX( * ), IDXP( * ),
     $                   IDXQ( * ), PERM( * )
      DOUBLE PRECISION   D( * ), DSIGMA( * ), GIVNUM( LDGNUM, * ),
     $                   VF( * ), VFW( * ), VL( * ), VLW( * ), Z( * ),
     $                   ZW( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO, EIGHT
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TWO = 2.0D+0,
     $                   EIGHT = 8.0D+0 )
*     ..
*     .. Local Scalars ..
*
      INTEGER            I, IDXI, IDXJ, IDXJP, J, JP, JPREV, K2, M, N,
     $                   NLP1, NLP2
      DOUBLE PRECISION   EPS, HLFTOL, TAU, TOL, Z1
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLAMRG, DROT, XERBLA
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLAPY2
      EXTERNAL           DLAMCH, DLAPY2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      N = NL + NR + 1
      M = N + SQRE
*
      IF( ( ICOMPQ.LT.0 ) .OR. ( ICOMPQ.GT.1 ) ) THEN
         INFO = -1
      ELSE IF( NL.LT.1 ) THEN
         INFO = -2
      ELSE IF( NR.LT.1 ) THEN
         INFO = -3
      ELSE IF( ( SQRE.LT.0 ) .OR. ( SQRE.GT.1 ) ) THEN
         INFO = -4
      ELSE IF( LDGCOL.LT.N ) THEN
         INFO = -22
      ELSE IF( LDGNUM.LT.N ) THEN
         INFO = -24
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLASD7', -INFO )
         RETURN
      END IF
*
      NLP1 = NL + 1
      NLP2 = NL + 2
      IF( ICOMPQ.EQ.1 ) THEN
         GIVPTR = 0
      END IF
*
*     Generate the first part of the vector Z and move the singular
*     values in the first part of D one position backward.
*
      Z1 = ALPHA*VL( NLP1 )
      VL( NLP1 ) = ZERO
      TAU = VF( NLP1 )
      DO 10 I = NL, 1, -1
         Z( I+1 ) = ALPHA*VL( I )
         VL( I ) = ZERO
         VF( I+1 ) = VF( I )
         D( I+1 ) = D( I )
         IDXQ( I+1 ) = IDXQ( I ) + 1
   10 CONTINUE
      VF( 1 ) = TAU
*
*     Generate the second part of the vector Z.
*
      DO 20 I = NLP2, M
         Z( I ) = BETA*VF( I )
         VF( I ) = ZERO
   20 CONTINUE
*
*     Sort the singular values into increasing order
*
      DO 30 I = NLP2, N
         IDXQ( I ) = IDXQ( I ) + NLP1
   30 CONTINUE
*
*     DSIGMA, IDXC, IDXC, and ZW are used as storage space.
*
      DO 40 I = 2, N
         DSIGMA( I ) = D( IDXQ( I ) )
         ZW( I ) = Z( IDXQ( I ) )
         VFW( I ) = VF( IDXQ( I ) )
         VLW( I ) = VL( IDXQ( I ) )
   40 CONTINUE
*
      CALL DLAMRG( NL, NR, DSIGMA( 2 ), 1, 1, IDX( 2 ) )
*
      DO 50 I = 2, N
         IDXI = 1 + IDX( I )
         D( I ) = DSIGMA( IDXI )
         Z( I ) = ZW( IDXI )
         VF( I ) = VFW( IDXI )
         VL( I ) = VLW( IDXI )
   50 CONTINUE
*
*     Calculate the allowable deflation tolerence
*
      EPS = DLAMCH( 'Epsilon' )
      TOL = MAX( ABS( ALPHA ), ABS( BETA ) )
      TOL = EIGHT*EIGHT*EPS*MAX( ABS( D( N ) ), TOL )
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
      DO 60 J = 2, N
         IF( ABS( Z( J ) ).LE.TOL ) THEN
*
*           Deflate due to small z component.
*
            K2 = K2 - 1
            IDXP( K2 ) = J
            IF( J.EQ.N )
     $         GO TO 100
         ELSE
            JPREV = J
            GO TO 70
         END IF
   60 CONTINUE
   70 CONTINUE
      J = JPREV
   80 CONTINUE
      J = J + 1
      IF( J.GT.N )
     $   GO TO 90
      IF( ABS( Z( J ) ).LE.TOL ) THEN
*
*        Deflate due to small z component.
*
         K2 = K2 - 1
         IDXP( K2 ) = J
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
            Z( J ) = TAU
            Z( JPREV ) = ZERO
            C = C / TAU
            S = -S / TAU
*
*           Record the appropriate Givens rotation
*
            IF( ICOMPQ.EQ.1 ) THEN
               GIVPTR = GIVPTR + 1
               IDXJP = IDXQ( IDX( JPREV )+1 )
               IDXJ = IDXQ( IDX( J )+1 )
               IF( IDXJP.LE.NLP1 ) THEN
                  IDXJP = IDXJP - 1
               END IF
               IF( IDXJ.LE.NLP1 ) THEN
                  IDXJ = IDXJ - 1
               END IF
               GIVCOL( GIVPTR, 2 ) = IDXJP
               GIVCOL( GIVPTR, 1 ) = IDXJ
               GIVNUM( GIVPTR, 2 ) = C
               GIVNUM( GIVPTR, 1 ) = S
            END IF
            CALL DROT( 1, VF( JPREV ), 1, VF( J ), 1, C, S )
            CALL DROT( 1, VL( JPREV ), 1, VL( J ), 1, C, S )
            K2 = K2 - 1
            IDXP( K2 ) = JPREV
            JPREV = J
         ELSE
            K = K + 1
            ZW( K ) = Z( JPREV )
            DSIGMA( K ) = D( JPREV )
            IDXP( K ) = JPREV
            JPREV = J
         END IF
      END IF
      GO TO 80
   90 CONTINUE
*
*     Record the last singular value.
*
      K = K + 1
      ZW( K ) = Z( JPREV )
      DSIGMA( K ) = D( JPREV )
      IDXP( K ) = JPREV
*
  100 CONTINUE
*
*     Sort the singular values into DSIGMA. The singular values which
*     were not deflated go into the first K slots of DSIGMA, except
*     that DSIGMA(1) is treated separately.
*
      DO 110 J = 2, N
         JP = IDXP( J )
         DSIGMA( J ) = D( JP )
         VFW( J ) = VF( JP )
         VLW( J ) = VL( JP )
  110 CONTINUE
      IF( ICOMPQ.EQ.1 ) THEN
         DO 120 J = 2, N
            JP = IDXP( J )
            PERM( J ) = IDXQ( IDX( JP )+1 )
            IF( PERM( J ).LE.NLP1 ) THEN
               PERM( J ) = PERM( J ) - 1
            END IF
  120    CONTINUE
      END IF
*
*     The deflated singular values go back into the last N - K slots of
*     D.
*
      CALL DCOPY( N-K, DSIGMA( K+1 ), 1, D( K+1 ), 1 )
*
*     Determine DSIGMA(1), DSIGMA(2), Z(1), VF(1), VL(1), VF(M), and
*     VL(M).
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
            S = -Z( M ) / Z( 1 )
         END IF
         CALL DROT( 1, VF( M ), 1, VF( 1 ), 1, C, S )
         CALL DROT( 1, VL( M ), 1, VL( 1 ), 1, C, S )
      ELSE
         IF( ABS( Z1 ).LE.TOL ) THEN
            Z( 1 ) = TOL
         ELSE
            Z( 1 ) = Z1
         END IF
      END IF
*
*     Restore Z, VF, and VL.
*
      CALL DCOPY( K-1, ZW( 2 ), 1, Z( 2 ), 1 )
      CALL DCOPY( N-1, VFW( 2 ), 1, VF( 2 ), 1 )
      CALL DCOPY( N-1, VLW( 2 ), 1, VL( 2 ), 1 )
*
      RETURN
*
*     End of DLASD7
*
      END
