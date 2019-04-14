*> \brief \b SLAED2 used by sstedc. Merges eigenvalues and deflates secular equation. Used when the original matrix is tridiagonal.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAED2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaed2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaed2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaed2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAED2( K, N, N1, D, Q, LDQ, INDXQ, RHO, Z, DLAMDA, W,
*                          Q2, INDX, INDXC, INDXP, COLTYP, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, K, LDQ, N, N1
*       REAL               RHO
*       ..
*       .. Array Arguments ..
*       INTEGER            COLTYP( * ), INDX( * ), INDXC( * ), INDXP( * ),
*      $                   INDXQ( * )
*       REAL               D( * ), DLAMDA( * ), Q( LDQ, * ), Q2( * ),
*      $                   W( * ), Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAED2 merges the two sets of eigenvalues together into a single
*> sorted set.  Then it tries to deflate the size of the problem.
*> There are two ways in which deflation can occur:  when two or more
*> eigenvalues are close together or if there is a tiny entry in the
*> Z vector.  For each such occurrence the order of the related secular
*> equation problem is reduced by one.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*>         The number of non-deflated eigenvalues, and the order of the
*>         related secular equation. 0 <= K <=N.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         The dimension of the symmetric tridiagonal matrix.  N >= 0.
*> \endverbatim
*>
*> \param[in] N1
*> \verbatim
*>          N1 is INTEGER
*>         The location of the last eigenvalue in the leading sub-matrix.
*>         min(1,N) <= N1 <= N/2.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>         On entry, D contains the eigenvalues of the two submatrices to
*>         be combined.
*>         On exit, D contains the trailing (N-K) updated eigenvalues
*>         (those which were deflated) sorted into increasing order.
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is REAL array, dimension (LDQ, N)
*>         On entry, Q contains the eigenvectors of two submatrices in
*>         the two square blocks with corners at (1,1), (N1,N1)
*>         and (N1+1, N1+1), (N,N).
*>         On exit, Q contains the trailing (N-K) updated eigenvectors
*>         (those which were deflated) in its last N-K columns.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>         The leading dimension of the array Q.  LDQ >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] INDXQ
*> \verbatim
*>          INDXQ is INTEGER array, dimension (N)
*>         The permutation which separately sorts the two sub-problems
*>         in D into ascending order.  Note that elements in the second
*>         half of this permutation must first have N1 added to their
*>         values. Destroyed on exit.
*> \endverbatim
*>
*> \param[in,out] RHO
*> \verbatim
*>          RHO is REAL
*>         On entry, the off-diagonal element associated with the rank-1
*>         cut which originally split the two submatrices which are now
*>         being recombined.
*>         On exit, RHO has been modified to the value required by
*>         SLAED3.
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is REAL array, dimension (N)
*>         On entry, Z contains the updating vector (the last
*>         row of the first sub-eigenvector matrix and the first row of
*>         the second sub-eigenvector matrix).
*>         On exit, the contents of Z have been destroyed by the updating
*>         process.
*> \endverbatim
*>
*> \param[out] DLAMDA
*> \verbatim
*>          DLAMDA is REAL array, dimension (N)
*>         A copy of the first K eigenvalues which will be used by
*>         SLAED3 to form the secular equation.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is REAL array, dimension (N)
*>         The first k values of the final deflation-altered z-vector
*>         which will be passed to SLAED3.
*> \endverbatim
*>
*> \param[out] Q2
*> \verbatim
*>          Q2 is REAL array, dimension (N1**2+(N-N1)**2)
*>         A copy of the first K eigenvectors which will be used by
*>         SLAED3 in a matrix multiply (SGEMM) to solve for the new
*>         eigenvectors.
*> \endverbatim
*>
*> \param[out] INDX
*> \verbatim
*>          INDX is INTEGER array, dimension (N)
*>         The permutation used to sort the contents of DLAMDA into
*>         ascending order.
*> \endverbatim
*>
*> \param[out] INDXC
*> \verbatim
*>          INDXC is INTEGER array, dimension (N)
*>         The permutation used to arrange the columns of the deflated
*>         Q matrix into three groups:  the first group contains non-zero
*>         elements only at and above N1, the second contains
*>         non-zero elements only below N1, and the third is dense.
*> \endverbatim
*>
*> \param[out] INDXP
*> \verbatim
*>          INDXP is INTEGER array, dimension (N)
*>         The permutation used to place deflated values of D at the end
*>         of the array.  INDXP(1:K) points to the nondeflated D-values
*>         and INDXP(K+1:N) points to the deflated eigenvalues.
*> \endverbatim
*>
*> \param[out] COLTYP
*> \verbatim
*>          COLTYP is INTEGER array, dimension (N)
*>         During execution, a label which will indicate which of the
*>         following types a column in the Q2 matrix is:
*>         1 : non-zero in the upper half only;
*>         2 : dense;
*>         3 : non-zero in the lower half only;
*>         4 : deflated.
*>         On exit, COLTYP(i) is the number of columns of type i,
*>         for i=1 to 4 only.
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
*> \date December 2016
*
*> \ingroup auxOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*> Jeff Rutter, Computer Science Division, University of California
*> at Berkeley, USA \n
*>  Modified by Francoise Tisseur, University of Tennessee
*>
*  =====================================================================
      SUBROUTINE SLAED2( K, N, N1, D, Q, LDQ, INDXQ, RHO, Z, DLAMDA, W,
     $                   Q2, INDX, INDXC, INDXP, COLTYP, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, K, LDQ, N, N1
      REAL               RHO
*     ..
*     .. Array Arguments ..
      INTEGER            COLTYP( * ), INDX( * ), INDXC( * ), INDXP( * ),
     $                   INDXQ( * )
      REAL               D( * ), DLAMDA( * ), Q( LDQ, * ), Q2( * ),
     $                   W( * ), Z( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               MONE, ZERO, ONE, TWO, EIGHT
      PARAMETER          ( MONE = -1.0E0, ZERO = 0.0E0, ONE = 1.0E0,
     $                   TWO = 2.0E0, EIGHT = 8.0E0 )
*     ..
*     .. Local Arrays ..
      INTEGER            CTOT( 4 ), PSM( 4 )
*     ..
*     .. Local Scalars ..
      INTEGER            CT, I, IMAX, IQ1, IQ2, J, JMAX, JS, K2, N1P1,
     $                   N2, NJ, PJ
      REAL               C, EPS, S, T, TAU, TOL
*     ..
*     .. External Functions ..
      INTEGER            ISAMAX
      REAL               SLAMCH, SLAPY2
      EXTERNAL           ISAMAX, SLAMCH, SLAPY2
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SLACPY, SLAMRG, SROT, SSCAL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDQ.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( MIN( 1, ( N / 2 ) ).GT.N1 .OR. ( N / 2 ).LT.N1 ) THEN
         INFO = -3
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLAED2', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      N2 = N - N1
      N1P1 = N1 + 1
*
      IF( RHO.LT.ZERO ) THEN
         CALL SSCAL( N2, MONE, Z( N1P1 ), 1 )
      END IF
*
*     Normalize z so that norm(z) = 1.  Since z is the concatenation of
*     two normalized vectors, norm2(z) = sqrt(2).
*
      T = ONE / SQRT( TWO )
      CALL SSCAL( N, T, Z, 1 )
*
*     RHO = ABS( norm(z)**2 * RHO )
*
      RHO = ABS( TWO*RHO )
*
*     Sort the eigenvalues into increasing order
*
      DO 10 I = N1P1, N
         INDXQ( I ) = INDXQ( I ) + N1
   10 CONTINUE
*
*     re-integrate the deflated parts from the last pass
*
      DO 20 I = 1, N
         DLAMDA( I ) = D( INDXQ( I ) )
   20 CONTINUE
      CALL SLAMRG( N1, N2, DLAMDA, 1, 1, INDXC )
      DO 30 I = 1, N
         INDX( I ) = INDXQ( INDXC( I ) )
   30 CONTINUE
*
*     Calculate the allowable deflation tolerance
*
      IMAX = ISAMAX( N, Z, 1 )
      JMAX = ISAMAX( N, D, 1 )
      EPS = SLAMCH( 'Epsilon' )
      TOL = EIGHT*EPS*MAX( ABS( D( JMAX ) ), ABS( Z( IMAX ) ) )
*
*     If the rank-1 modifier is small enough, no more needs to be done
*     except to reorganize Q so that its columns correspond with the
*     elements in D.
*
      IF( RHO*ABS( Z( IMAX ) ).LE.TOL ) THEN
         K = 0
         IQ2 = 1
         DO 40 J = 1, N
            I = INDX( J )
            CALL SCOPY( N, Q( 1, I ), 1, Q2( IQ2 ), 1 )
            DLAMDA( J ) = D( I )
            IQ2 = IQ2 + N
   40    CONTINUE
         CALL SLACPY( 'A', N, N, Q2, N, Q, LDQ )
         CALL SCOPY( N, DLAMDA, 1, D, 1 )
         GO TO 190
      END IF
*
*     If there are multiple eigenvalues then the problem deflates.  Here
*     the number of equal eigenvalues are found.  As each equal
*     eigenvalue is found, an elementary reflector is computed to rotate
*     the corresponding eigensubspace so that the corresponding
*     components of Z are zero in this new basis.
*
      DO 50 I = 1, N1
         COLTYP( I ) = 1
   50 CONTINUE
      DO 60 I = N1P1, N
         COLTYP( I ) = 3
   60 CONTINUE
*
*
      K = 0
      K2 = N + 1
      DO 70 J = 1, N
         NJ = INDX( J )
         IF( RHO*ABS( Z( NJ ) ).LE.TOL ) THEN
*
*           Deflate due to small z component.
*
            K2 = K2 - 1
            COLTYP( NJ ) = 4
            INDXP( K2 ) = NJ
            IF( J.EQ.N )
     $         GO TO 100
         ELSE
            PJ = NJ
            GO TO 80
         END IF
   70 CONTINUE
   80 CONTINUE
      J = J + 1
      NJ = INDX( J )
      IF( J.GT.N )
     $   GO TO 100
      IF( RHO*ABS( Z( NJ ) ).LE.TOL ) THEN
*
*        Deflate due to small z component.
*
         K2 = K2 - 1
         COLTYP( NJ ) = 4
         INDXP( K2 ) = NJ
      ELSE
*
*        Check if eigenvalues are close enough to allow deflation.
*
         S = Z( PJ )
         C = Z( NJ )
*
*        Find sqrt(a**2+b**2) without overflow or
*        destructive underflow.
*
         TAU = SLAPY2( C, S )
         T = D( NJ ) - D( PJ )
         C = C / TAU
         S = -S / TAU
         IF( ABS( T*C*S ).LE.TOL ) THEN
*
*           Deflation is possible.
*
            Z( NJ ) = TAU
            Z( PJ ) = ZERO
            IF( COLTYP( NJ ).NE.COLTYP( PJ ) )
     $         COLTYP( NJ ) = 2
            COLTYP( PJ ) = 4
            CALL SROT( N, Q( 1, PJ ), 1, Q( 1, NJ ), 1, C, S )
            T = D( PJ )*C**2 + D( NJ )*S**2
            D( NJ ) = D( PJ )*S**2 + D( NJ )*C**2
            D( PJ ) = T
            K2 = K2 - 1
            I = 1
   90       CONTINUE
            IF( K2+I.LE.N ) THEN
               IF( D( PJ ).LT.D( INDXP( K2+I ) ) ) THEN
                  INDXP( K2+I-1 ) = INDXP( K2+I )
                  INDXP( K2+I ) = PJ
                  I = I + 1
                  GO TO 90
               ELSE
                  INDXP( K2+I-1 ) = PJ
               END IF
            ELSE
               INDXP( K2+I-1 ) = PJ
            END IF
            PJ = NJ
         ELSE
            K = K + 1
            DLAMDA( K ) = D( PJ )
            W( K ) = Z( PJ )
            INDXP( K ) = PJ
            PJ = NJ
         END IF
      END IF
      GO TO 80
  100 CONTINUE
*
*     Record the last eigenvalue.
*
      K = K + 1
      DLAMDA( K ) = D( PJ )
      W( K ) = Z( PJ )
      INDXP( K ) = PJ
*
*     Count up the total number of the various types of columns, then
*     form a permutation which positions the four column types into
*     four uniform groups (although one or more of these groups may be
*     empty).
*
      DO 110 J = 1, 4
         CTOT( J ) = 0
  110 CONTINUE
      DO 120 J = 1, N
         CT = COLTYP( J )
         CTOT( CT ) = CTOT( CT ) + 1
  120 CONTINUE
*
*     PSM(*) = Position in SubMatrix (of types 1 through 4)
*
      PSM( 1 ) = 1
      PSM( 2 ) = 1 + CTOT( 1 )
      PSM( 3 ) = PSM( 2 ) + CTOT( 2 )
      PSM( 4 ) = PSM( 3 ) + CTOT( 3 )
      K = N - CTOT( 4 )
*
*     Fill out the INDXC array so that the permutation which it induces
*     will place all type-1 columns first, all type-2 columns next,
*     then all type-3's, and finally all type-4's.
*
      DO 130 J = 1, N
         JS = INDXP( J )
         CT = COLTYP( JS )
         INDX( PSM( CT ) ) = JS
         INDXC( PSM( CT ) ) = J
         PSM( CT ) = PSM( CT ) + 1
  130 CONTINUE
*
*     Sort the eigenvalues and corresponding eigenvectors into DLAMDA
*     and Q2 respectively.  The eigenvalues/vectors which were not
*     deflated go into the first K slots of DLAMDA and Q2 respectively,
*     while those which were deflated go into the last N - K slots.
*
      I = 1
      IQ1 = 1
      IQ2 = 1 + ( CTOT( 1 )+CTOT( 2 ) )*N1
      DO 140 J = 1, CTOT( 1 )
         JS = INDX( I )
         CALL SCOPY( N1, Q( 1, JS ), 1, Q2( IQ1 ), 1 )
         Z( I ) = D( JS )
         I = I + 1
         IQ1 = IQ1 + N1
  140 CONTINUE
*
      DO 150 J = 1, CTOT( 2 )
         JS = INDX( I )
         CALL SCOPY( N1, Q( 1, JS ), 1, Q2( IQ1 ), 1 )
         CALL SCOPY( N2, Q( N1+1, JS ), 1, Q2( IQ2 ), 1 )
         Z( I ) = D( JS )
         I = I + 1
         IQ1 = IQ1 + N1
         IQ2 = IQ2 + N2
  150 CONTINUE
*
      DO 160 J = 1, CTOT( 3 )
         JS = INDX( I )
         CALL SCOPY( N2, Q( N1+1, JS ), 1, Q2( IQ2 ), 1 )
         Z( I ) = D( JS )
         I = I + 1
         IQ2 = IQ2 + N2
  160 CONTINUE
*
      IQ1 = IQ2
      DO 170 J = 1, CTOT( 4 )
         JS = INDX( I )
         CALL SCOPY( N, Q( 1, JS ), 1, Q2( IQ2 ), 1 )
         IQ2 = IQ2 + N
         Z( I ) = D( JS )
         I = I + 1
  170 CONTINUE
*
*     The deflated eigenvalues and their corresponding vectors go back
*     into the last N - K slots of D and Q respectively.
*
      IF( K.LT.N ) THEN
         CALL SLACPY( 'A', N, CTOT( 4 ), Q2( IQ1 ), N,
     $                Q( 1, K+1 ), LDQ )
         CALL SCOPY( N-K, Z( K+1 ), 1, D( K+1 ), 1 )
      END IF
*
*     Copy CTOT into COLTYP for referencing in SLAED3.
*
      DO 180 J = 1, 4
         COLTYP( J ) = CTOT( J )
  180 CONTINUE
*
  190 CONTINUE
      RETURN
*
*     End of SLAED2
*
      END
