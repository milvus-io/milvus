*> \brief \b CGGBAL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CGGBAL + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cggbal.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cggbal.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cggbal.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGGBAL( JOB, N, A, LDA, B, LDB, ILO, IHI, LSCALE,
*                          RSCALE, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOB
*       INTEGER            IHI, ILO, INFO, LDA, LDB, N
*       ..
*       .. Array Arguments ..
*       REAL               LSCALE( * ), RSCALE( * ), WORK( * )
*       COMPLEX            A( LDA, * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGGBAL balances a pair of general complex matrices (A,B).  This
*> involves, first, permuting A and B by similarity transformations to
*> isolate eigenvalues in the first 1 to ILO$-$1 and last IHI+1 to N
*> elements on the diagonal; and second, applying a diagonal similarity
*> transformation to rows and columns ILO to IHI to make the rows
*> and columns as close in norm as possible. Both steps are optional.
*>
*> Balancing may reduce the 1-norm of the matrices, and improve the
*> accuracy of the computed eigenvalues and/or eigenvectors in the
*> generalized eigenvalue problem A*x = lambda*B*x.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER*1
*>          Specifies the operations to be performed on A and B:
*>          = 'N':  none:  simply set ILO = 1, IHI = N, LSCALE(I) = 1.0
*>                  and RSCALE(I) = 1.0 for i=1,...,N;
*>          = 'P':  permute only;
*>          = 'S':  scale only;
*>          = 'B':  both permute and scale.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the input matrix A.
*>          On exit, A is overwritten by the balanced matrix.
*>          If JOB = 'N', A is not referenced.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,N)
*>          On entry, the input matrix B.
*>          On exit, B is overwritten by the balanced matrix.
*>          If JOB = 'N', B is not referenced.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[out] IHI
*> \verbatim
*>          IHI is INTEGER
*>          ILO and IHI are set to integers such that on exit
*>          A(i,j) = 0 and B(i,j) = 0 if i > j and
*>          j = 1,...,ILO-1 or i = IHI+1,...,N.
*>          If JOB = 'N' or 'S', ILO = 1 and IHI = N.
*> \endverbatim
*>
*> \param[out] LSCALE
*> \verbatim
*>          LSCALE is REAL array, dimension (N)
*>          Details of the permutations and scaling factors applied
*>          to the left side of A and B.  If P(j) is the index of the
*>          row interchanged with row j, and D(j) is the scaling factor
*>          applied to row j, then
*>            LSCALE(j) = P(j)    for J = 1,...,ILO-1
*>                      = D(j)    for J = ILO,...,IHI
*>                      = P(j)    for J = IHI+1,...,N.
*>          The order in which the interchanges are made is N to IHI+1,
*>          then 1 to ILO-1.
*> \endverbatim
*>
*> \param[out] RSCALE
*> \verbatim
*>          RSCALE is REAL array, dimension (N)
*>          Details of the permutations and scaling factors applied
*>          to the right side of A and B.  If P(j) is the index of the
*>          column interchanged with column j, and D(j) is the scaling
*>          factor applied to column j, then
*>            RSCALE(j) = P(j)    for J = 1,...,ILO-1
*>                      = D(j)    for J = ILO,...,IHI
*>                      = P(j)    for J = IHI+1,...,N.
*>          The order in which the interchanges are made is N to IHI+1,
*>          then 1 to ILO-1.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (lwork)
*>          lwork must be at least max(1,6*N) when JOB = 'S' or 'B', and
*>          at least 1 when JOB = 'N' or 'P'.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
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
*> \ingroup complexGBcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  See R.C. WARD, Balancing the generalized eigenvalue problem,
*>                 SIAM J. Sci. Stat. Comp. 2 (1981), 141-152.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE CGGBAL( JOB, N, A, LDA, B, LDB, ILO, IHI, LSCALE,
     $                   RSCALE, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOB
      INTEGER            IHI, ILO, INFO, LDA, LDB, N
*     ..
*     .. Array Arguments ..
      REAL               LSCALE( * ), RSCALE( * ), WORK( * )
      COMPLEX            A( LDA, * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0E+0, HALF = 0.5E+0, ONE = 1.0E+0 )
      REAL               THREE, SCLFAC
      PARAMETER          ( THREE = 3.0E+0, SCLFAC = 1.0E+1 )
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, ICAB, IFLOW, IP1, IR, IRAB, IT, J, JC, JP1,
     $                   K, KOUNT, L, LCAB, LM1, LRAB, LSFMAX, LSFMIN,
     $                   M, NR, NRP2
      REAL               ALPHA, BASL, BETA, CAB, CMAX, COEF, COEF2,
     $                   COEF5, COR, EW, EWC, GAMMA, PGAMMA, RAB, SFMAX,
     $                   SFMIN, SUM, T, TA, TB, TC
      COMPLEX            CDUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ICAMAX
      REAL               SDOT, SLAMCH
      EXTERNAL           LSAME, ICAMAX, SDOT, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CSSCAL, CSWAP, SAXPY, SSCAL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, INT, LOG10, MAX, MIN, REAL, SIGN
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( CDUM ) = ABS( REAL( CDUM ) ) + ABS( AIMAG( CDUM ) )
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      INFO = 0
      IF( .NOT.LSAME( JOB, 'N' ) .AND. .NOT.LSAME( JOB, 'P' ) .AND.
     $    .NOT.LSAME( JOB, 'S' ) .AND. .NOT.LSAME( JOB, 'B' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CGGBAL', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 ) THEN
         ILO = 1
         IHI = N
         RETURN
      END IF
*
      IF( N.EQ.1 ) THEN
         ILO = 1
         IHI = N
         LSCALE( 1 ) = ONE
         RSCALE( 1 ) = ONE
         RETURN
      END IF
*
      IF( LSAME( JOB, 'N' ) ) THEN
         ILO = 1
         IHI = N
         DO 10 I = 1, N
            LSCALE( I ) = ONE
            RSCALE( I ) = ONE
   10    CONTINUE
         RETURN
      END IF
*
      K = 1
      L = N
      IF( LSAME( JOB, 'S' ) )
     $   GO TO 190
*
      GO TO 30
*
*     Permute the matrices A and B to isolate the eigenvalues.
*
*     Find row with one nonzero in columns 1 through L
*
   20 CONTINUE
      L = LM1
      IF( L.NE.1 )
     $   GO TO 30
*
      RSCALE( 1 ) = ONE
      LSCALE( 1 ) = ONE
      GO TO 190
*
   30 CONTINUE
      LM1 = L - 1
      DO 80 I = L, 1, -1
         DO 40 J = 1, LM1
            JP1 = J + 1
            IF( A( I, J ).NE.CZERO .OR. B( I, J ).NE.CZERO )
     $         GO TO 50
   40    CONTINUE
         J = L
         GO TO 70
*
   50    CONTINUE
         DO 60 J = JP1, L
            IF( A( I, J ).NE.CZERO .OR. B( I, J ).NE.CZERO )
     $         GO TO 80
   60    CONTINUE
         J = JP1 - 1
*
   70    CONTINUE
         M = L
         IFLOW = 1
         GO TO 160
   80 CONTINUE
      GO TO 100
*
*     Find column with one nonzero in rows K through N
*
   90 CONTINUE
      K = K + 1
*
  100 CONTINUE
      DO 150 J = K, L
         DO 110 I = K, LM1
            IP1 = I + 1
            IF( A( I, J ).NE.CZERO .OR. B( I, J ).NE.CZERO )
     $         GO TO 120
  110    CONTINUE
         I = L
         GO TO 140
  120    CONTINUE
         DO 130 I = IP1, L
            IF( A( I, J ).NE.CZERO .OR. B( I, J ).NE.CZERO )
     $         GO TO 150
  130    CONTINUE
         I = IP1 - 1
  140    CONTINUE
         M = K
         IFLOW = 2
         GO TO 160
  150 CONTINUE
      GO TO 190
*
*     Permute rows M and I
*
  160 CONTINUE
      LSCALE( M ) = I
      IF( I.EQ.M )
     $   GO TO 170
      CALL CSWAP( N-K+1, A( I, K ), LDA, A( M, K ), LDA )
      CALL CSWAP( N-K+1, B( I, K ), LDB, B( M, K ), LDB )
*
*     Permute columns M and J
*
  170 CONTINUE
      RSCALE( M ) = J
      IF( J.EQ.M )
     $   GO TO 180
      CALL CSWAP( L, A( 1, J ), 1, A( 1, M ), 1 )
      CALL CSWAP( L, B( 1, J ), 1, B( 1, M ), 1 )
*
  180 CONTINUE
      GO TO ( 20, 90 )IFLOW
*
  190 CONTINUE
      ILO = K
      IHI = L
*
      IF( LSAME( JOB, 'P' ) ) THEN
         DO 195 I = ILO, IHI
            LSCALE( I ) = ONE
            RSCALE( I ) = ONE
  195    CONTINUE
         RETURN
      END IF
*
      IF( ILO.EQ.IHI )
     $   RETURN
*
*     Balance the submatrix in rows ILO to IHI.
*
      NR = IHI - ILO + 1
      DO 200 I = ILO, IHI
         RSCALE( I ) = ZERO
         LSCALE( I ) = ZERO
*
         WORK( I ) = ZERO
         WORK( I+N ) = ZERO
         WORK( I+2*N ) = ZERO
         WORK( I+3*N ) = ZERO
         WORK( I+4*N ) = ZERO
         WORK( I+5*N ) = ZERO
  200 CONTINUE
*
*     Compute right side vector in resulting linear equations
*
      BASL = LOG10( SCLFAC )
      DO 240 I = ILO, IHI
         DO 230 J = ILO, IHI
            IF( A( I, J ).EQ.CZERO ) THEN
               TA = ZERO
               GO TO 210
            END IF
            TA = LOG10( CABS1( A( I, J ) ) ) / BASL
*
  210       CONTINUE
            IF( B( I, J ).EQ.CZERO ) THEN
               TB = ZERO
               GO TO 220
            END IF
            TB = LOG10( CABS1( B( I, J ) ) ) / BASL
*
  220       CONTINUE
            WORK( I+4*N ) = WORK( I+4*N ) - TA - TB
            WORK( J+5*N ) = WORK( J+5*N ) - TA - TB
  230    CONTINUE
  240 CONTINUE
*
      COEF = ONE / REAL( 2*NR )
      COEF2 = COEF*COEF
      COEF5 = HALF*COEF2
      NRP2 = NR + 2
      BETA = ZERO
      IT = 1
*
*     Start generalized conjugate gradient iteration
*
  250 CONTINUE
*
      GAMMA = SDOT( NR, WORK( ILO+4*N ), 1, WORK( ILO+4*N ), 1 ) +
     $        SDOT( NR, WORK( ILO+5*N ), 1, WORK( ILO+5*N ), 1 )
*
      EW = ZERO
      EWC = ZERO
      DO 260 I = ILO, IHI
         EW = EW + WORK( I+4*N )
         EWC = EWC + WORK( I+5*N )
  260 CONTINUE
*
      GAMMA = COEF*GAMMA - COEF2*( EW**2+EWC**2 ) - COEF5*( EW-EWC )**2
      IF( GAMMA.EQ.ZERO )
     $   GO TO 350
      IF( IT.NE.1 )
     $   BETA = GAMMA / PGAMMA
      T = COEF5*( EWC-THREE*EW )
      TC = COEF5*( EW-THREE*EWC )
*
      CALL SSCAL( NR, BETA, WORK( ILO ), 1 )
      CALL SSCAL( NR, BETA, WORK( ILO+N ), 1 )
*
      CALL SAXPY( NR, COEF, WORK( ILO+4*N ), 1, WORK( ILO+N ), 1 )
      CALL SAXPY( NR, COEF, WORK( ILO+5*N ), 1, WORK( ILO ), 1 )
*
      DO 270 I = ILO, IHI
         WORK( I ) = WORK( I ) + TC
         WORK( I+N ) = WORK( I+N ) + T
  270 CONTINUE
*
*     Apply matrix to vector
*
      DO 300 I = ILO, IHI
         KOUNT = 0
         SUM = ZERO
         DO 290 J = ILO, IHI
            IF( A( I, J ).EQ.CZERO )
     $         GO TO 280
            KOUNT = KOUNT + 1
            SUM = SUM + WORK( J )
  280       CONTINUE
            IF( B( I, J ).EQ.CZERO )
     $         GO TO 290
            KOUNT = KOUNT + 1
            SUM = SUM + WORK( J )
  290    CONTINUE
         WORK( I+2*N ) = REAL( KOUNT )*WORK( I+N ) + SUM
  300 CONTINUE
*
      DO 330 J = ILO, IHI
         KOUNT = 0
         SUM = ZERO
         DO 320 I = ILO, IHI
            IF( A( I, J ).EQ.CZERO )
     $         GO TO 310
            KOUNT = KOUNT + 1
            SUM = SUM + WORK( I+N )
  310       CONTINUE
            IF( B( I, J ).EQ.CZERO )
     $         GO TO 320
            KOUNT = KOUNT + 1
            SUM = SUM + WORK( I+N )
  320    CONTINUE
         WORK( J+3*N ) = REAL( KOUNT )*WORK( J ) + SUM
  330 CONTINUE
*
      SUM = SDOT( NR, WORK( ILO+N ), 1, WORK( ILO+2*N ), 1 ) +
     $      SDOT( NR, WORK( ILO ), 1, WORK( ILO+3*N ), 1 )
      ALPHA = GAMMA / SUM
*
*     Determine correction to current iteration
*
      CMAX = ZERO
      DO 340 I = ILO, IHI
         COR = ALPHA*WORK( I+N )
         IF( ABS( COR ).GT.CMAX )
     $      CMAX = ABS( COR )
         LSCALE( I ) = LSCALE( I ) + COR
         COR = ALPHA*WORK( I )
         IF( ABS( COR ).GT.CMAX )
     $      CMAX = ABS( COR )
         RSCALE( I ) = RSCALE( I ) + COR
  340 CONTINUE
      IF( CMAX.LT.HALF )
     $   GO TO 350
*
      CALL SAXPY( NR, -ALPHA, WORK( ILO+2*N ), 1, WORK( ILO+4*N ), 1 )
      CALL SAXPY( NR, -ALPHA, WORK( ILO+3*N ), 1, WORK( ILO+5*N ), 1 )
*
      PGAMMA = GAMMA
      IT = IT + 1
      IF( IT.LE.NRP2 )
     $   GO TO 250
*
*     End generalized conjugate gradient iteration
*
  350 CONTINUE
      SFMIN = SLAMCH( 'S' )
      SFMAX = ONE / SFMIN
      LSFMIN = INT( LOG10( SFMIN ) / BASL+ONE )
      LSFMAX = INT( LOG10( SFMAX ) / BASL )
      DO 360 I = ILO, IHI
         IRAB = ICAMAX( N-ILO+1, A( I, ILO ), LDA )
         RAB = ABS( A( I, IRAB+ILO-1 ) )
         IRAB = ICAMAX( N-ILO+1, B( I, ILO ), LDB )
         RAB = MAX( RAB, ABS( B( I, IRAB+ILO-1 ) ) )
         LRAB = INT( LOG10( RAB+SFMIN ) / BASL+ONE )
         IR = LSCALE( I ) + SIGN( HALF, LSCALE( I ) )
         IR = MIN( MAX( IR, LSFMIN ), LSFMAX, LSFMAX-LRAB )
         LSCALE( I ) = SCLFAC**IR
         ICAB = ICAMAX( IHI, A( 1, I ), 1 )
         CAB = ABS( A( ICAB, I ) )
         ICAB = ICAMAX( IHI, B( 1, I ), 1 )
         CAB = MAX( CAB, ABS( B( ICAB, I ) ) )
         LCAB = INT( LOG10( CAB+SFMIN ) / BASL+ONE )
         JC = RSCALE( I ) + SIGN( HALF, RSCALE( I ) )
         JC = MIN( MAX( JC, LSFMIN ), LSFMAX, LSFMAX-LCAB )
         RSCALE( I ) = SCLFAC**JC
  360 CONTINUE
*
*     Row scaling of matrices A and B
*
      DO 370 I = ILO, IHI
         CALL CSSCAL( N-ILO+1, LSCALE( I ), A( I, ILO ), LDA )
         CALL CSSCAL( N-ILO+1, LSCALE( I ), B( I, ILO ), LDB )
  370 CONTINUE
*
*     Column scaling of matrices A and B
*
      DO 380 J = ILO, IHI
         CALL CSSCAL( IHI, RSCALE( J ), A( 1, J ), 1 )
         CALL CSSCAL( IHI, RSCALE( J ), B( 1, J ), 1 )
  380 CONTINUE
*
      RETURN
*
*     End of CGGBAL
*
      END
