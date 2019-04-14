*> \brief \b DTGSNA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTGSNA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtgsna.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtgsna.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtgsna.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTGSNA( JOB, HOWMNY, SELECT, N, A, LDA, B, LDB, VL,
*                          LDVL, VR, LDVR, S, DIF, MM, M, WORK, LWORK,
*                          IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          HOWMNY, JOB
*       INTEGER            INFO, LDA, LDB, LDVL, LDVR, LWORK, M, MM, N
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), DIF( * ), S( * ),
*      $                   VL( LDVL, * ), VR( LDVR, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTGSNA estimates reciprocal condition numbers for specified
*> eigenvalues and/or eigenvectors of a matrix pair (A, B) in
*> generalized real Schur canonical form (or of any matrix pair
*> (Q*A*Z**T, Q*B*Z**T) with orthogonal matrices Q and Z, where
*> Z**T denotes the transpose of Z.
*>
*> (A, B) must be in generalized real Schur form (as returned by DGGES),
*> i.e. A is block upper triangular with 1-by-1 and 2-by-2 diagonal
*> blocks. B is upper triangular.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER*1
*>          Specifies whether condition numbers are required for
*>          eigenvalues (S) or eigenvectors (DIF):
*>          = 'E': for eigenvalues only (S);
*>          = 'V': for eigenvectors only (DIF);
*>          = 'B': for both eigenvalues and eigenvectors (S and DIF).
*> \endverbatim
*>
*> \param[in] HOWMNY
*> \verbatim
*>          HOWMNY is CHARACTER*1
*>          = 'A': compute condition numbers for all eigenpairs;
*>          = 'S': compute condition numbers for selected eigenpairs
*>                 specified by the array SELECT.
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          If HOWMNY = 'S', SELECT specifies the eigenpairs for which
*>          condition numbers are required. To select condition numbers
*>          for the eigenpair corresponding to a real eigenvalue w(j),
*>          SELECT(j) must be set to .TRUE.. To select condition numbers
*>          corresponding to a complex conjugate pair of eigenvalues w(j)
*>          and w(j+1), either SELECT(j) or SELECT(j+1) or both, must be
*>          set to .TRUE..
*>          If HOWMNY = 'A', SELECT is not referenced.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the square matrix pair (A, B). N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The upper quasi-triangular matrix A in the pair (A,B).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,N)
*>          The upper triangular matrix B in the pair (A,B).
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is DOUBLE PRECISION array, dimension (LDVL,M)
*>          If JOB = 'E' or 'B', VL must contain left eigenvectors of
*>          (A, B), corresponding to the eigenpairs specified by HOWMNY
*>          and SELECT. The eigenvectors must be stored in consecutive
*>          columns of VL, as returned by DTGEVC.
*>          If JOB = 'V', VL is not referenced.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the array VL. LDVL >= 1.
*>          If JOB = 'E' or 'B', LDVL >= N.
*> \endverbatim
*>
*> \param[in] VR
*> \verbatim
*>          VR is DOUBLE PRECISION array, dimension (LDVR,M)
*>          If JOB = 'E' or 'B', VR must contain right eigenvectors of
*>          (A, B), corresponding to the eigenpairs specified by HOWMNY
*>          and SELECT. The eigenvectors must be stored in consecutive
*>          columns ov VR, as returned by DTGEVC.
*>          If JOB = 'V', VR is not referenced.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR. LDVR >= 1.
*>          If JOB = 'E' or 'B', LDVR >= N.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (MM)
*>          If JOB = 'E' or 'B', the reciprocal condition numbers of the
*>          selected eigenvalues, stored in consecutive elements of the
*>          array. For a complex conjugate pair of eigenvalues two
*>          consecutive elements of S are set to the same value. Thus
*>          S(j), DIF(j), and the j-th columns of VL and VR all
*>          correspond to the same eigenpair (but not in general the
*>          j-th eigenpair, unless all eigenpairs are selected).
*>          If JOB = 'V', S is not referenced.
*> \endverbatim
*>
*> \param[out] DIF
*> \verbatim
*>          DIF is DOUBLE PRECISION array, dimension (MM)
*>          If JOB = 'V' or 'B', the estimated reciprocal condition
*>          numbers of the selected eigenvectors, stored in consecutive
*>          elements of the array. For a complex eigenvector two
*>          consecutive elements of DIF are set to the same value. If
*>          the eigenvalues cannot be reordered to compute DIF(j), DIF(j)
*>          is set to 0; this can only occur when the true value would be
*>          very small anyway.
*>          If JOB = 'E', DIF is not referenced.
*> \endverbatim
*>
*> \param[in] MM
*> \verbatim
*>          MM is INTEGER
*>          The number of elements in the arrays S and DIF. MM >= M.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The number of elements of the arrays S and DIF used to store
*>          the specified condition numbers; for each selected real
*>          eigenvalue one element is used, and for each selected complex
*>          conjugate pair of eigenvalues, two elements are used.
*>          If HOWMNY = 'A', M is set to N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= max(1,N).
*>          If JOB = 'V' or 'B' LWORK >= 2*N*(N+2)+16.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N + 6)
*>          If JOB = 'E', IWORK is not referenced.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          =0: Successful exit
*>          <0: If INFO = -i, the i-th argument had an illegal value
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
*> \ingroup doubleOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The reciprocal of the condition number of a generalized eigenvalue
*>  w = (a, b) is defined as
*>
*>       S(w) = (|u**TAv|**2 + |u**TBv|**2)**(1/2) / (norm(u)*norm(v))
*>
*>  where u and v are the left and right eigenvectors of (A, B)
*>  corresponding to w; |z| denotes the absolute value of the complex
*>  number, and norm(u) denotes the 2-norm of the vector u.
*>  The pair (a, b) corresponds to an eigenvalue w = a/b (= u**TAv/u**TBv)
*>  of the matrix pair (A, B). If both a and b equal zero, then (A B) is
*>  singular and S(I) = -1 is returned.
*>
*>  An approximate error bound on the chordal distance between the i-th
*>  computed generalized eigenvalue w and the corresponding exact
*>  eigenvalue lambda is
*>
*>       chord(w, lambda) <= EPS * norm(A, B) / S(I)
*>
*>  where EPS is the machine precision.
*>
*>  The reciprocal of the condition number DIF(i) of right eigenvector u
*>  and left eigenvector v corresponding to the generalized eigenvalue w
*>  is defined as follows:
*>
*>  a) If the i-th eigenvalue w = (a,b) is real
*>
*>     Suppose U and V are orthogonal transformations such that
*>
*>              U**T*(A, B)*V  = (S, T) = ( a   *  ) ( b  *  )  1
*>                                        ( 0  S22 ),( 0 T22 )  n-1
*>                                          1  n-1     1 n-1
*>
*>     Then the reciprocal condition number DIF(i) is
*>
*>                Difl((a, b), (S22, T22)) = sigma-min( Zl ),
*>
*>     where sigma-min(Zl) denotes the smallest singular value of the
*>     2(n-1)-by-2(n-1) matrix
*>
*>         Zl = [ kron(a, In-1)  -kron(1, S22) ]
*>              [ kron(b, In-1)  -kron(1, T22) ] .
*>
*>     Here In-1 is the identity matrix of size n-1. kron(X, Y) is the
*>     Kronecker product between the matrices X and Y.
*>
*>     Note that if the default method for computing DIF(i) is wanted
*>     (see DLATDF), then the parameter DIFDRI (see below) should be
*>     changed from 3 to 4 (routine DLATDF(IJOB = 2 will be used)).
*>     See DTGSYL for more details.
*>
*>  b) If the i-th and (i+1)-th eigenvalues are complex conjugate pair,
*>
*>     Suppose U and V are orthogonal transformations such that
*>
*>              U**T*(A, B)*V = (S, T) = ( S11  *   ) ( T11  *  )  2
*>                                       ( 0    S22 ),( 0    T22) n-2
*>                                         2    n-2     2    n-2
*>
*>     and (S11, T11) corresponds to the complex conjugate eigenvalue
*>     pair (w, conjg(w)). There exist unitary matrices U1 and V1 such
*>     that
*>
*>       U1**T*S11*V1 = ( s11 s12 ) and U1**T*T11*V1 = ( t11 t12 )
*>                      (  0  s22 )                    (  0  t22 )
*>
*>     where the generalized eigenvalues w = s11/t11 and
*>     conjg(w) = s22/t22.
*>
*>     Then the reciprocal condition number DIF(i) is bounded by
*>
*>         min( d1, max( 1, |real(s11)/real(s22)| )*d2 )
*>
*>     where, d1 = Difl((s11, t11), (s22, t22)) = sigma-min(Z1), where
*>     Z1 is the complex 2-by-2 matrix
*>
*>              Z1 =  [ s11  -s22 ]
*>                    [ t11  -t22 ],
*>
*>     This is done by computing (using real arithmetic) the
*>     roots of the characteristical polynomial det(Z1**T * Z1 - lambda I),
*>     where Z1**T denotes the transpose of Z1 and det(X) denotes
*>     the determinant of X.
*>
*>     and d2 is an upper bound on Difl((S11, T11), (S22, T22)), i.e. an
*>     upper bound on sigma-min(Z2), where Z2 is (2n-2)-by-(2n-2)
*>
*>              Z2 = [ kron(S11**T, In-2)  -kron(I2, S22) ]
*>                   [ kron(T11**T, In-2)  -kron(I2, T22) ]
*>
*>     Note that if the default method for computing DIF is wanted (see
*>     DLATDF), then the parameter DIFDRI (see below) should be changed
*>     from 3 to 4 (routine DLATDF(IJOB = 2 will be used)). See DTGSYL
*>     for more details.
*>
*>  For each eigenvalue/vector specified by SELECT, DIF stores a
*>  Frobenius norm-based estimate of Difl.
*>
*>  An approximate error bound for the i-th computed eigenvector VL(i) or
*>  VR(i) is given by
*>
*>             EPS * norm(A, B) / DIF(i).
*>
*>  See ref. [2-3] for more details and further references.
*> \endverbatim
*
*> \par Contributors:
*  ==================
*>
*>     Bo Kagstrom and Peter Poromaa, Department of Computing Science,
*>     Umea University, S-901 87 Umea, Sweden.
*
*> \par References:
*  ================
*>
*> \verbatim
*>
*>  [1] B. Kagstrom; A Direct Method for Reordering Eigenvalues in the
*>      Generalized Real Schur Form of a Regular Matrix Pair (A, B), in
*>      M.S. Moonen et al (eds), Linear Algebra for Large Scale and
*>      Real-Time Applications, Kluwer Academic Publ. 1993, pp 195-218.
*>
*>  [2] B. Kagstrom and P. Poromaa; Computing Eigenspaces with Specified
*>      Eigenvalues of a Regular Matrix Pair (A, B) and Condition
*>      Estimation: Theory, Algorithms and Software,
*>      Report UMINF - 94.04, Department of Computing Science, Umea
*>      University, S-901 87 Umea, Sweden, 1994. Also as LAPACK Working
*>      Note 87. To appear in Numerical Algorithms, 1996.
*>
*>  [3] B. Kagstrom and P. Poromaa, LAPACK-Style Algorithms and Software
*>      for Solving the Generalized Sylvester Equation and Estimating the
*>      Separation between Regular Matrix Pairs, Report UMINF - 93.23,
*>      Department of Computing Science, Umea University, S-901 87 Umea,
*>      Sweden, December 1993, Revised April 1994, Also as LAPACK Working
*>      Note 75.  To appear in ACM Trans. on Math. Software, Vol 22,
*>      No 1, 1996.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DTGSNA( JOB, HOWMNY, SELECT, N, A, LDA, B, LDB, VL,
     $                   LDVL, VR, LDVR, S, DIF, MM, M, WORK, LWORK,
     $                   IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          HOWMNY, JOB
      INTEGER            INFO, LDA, LDB, LDVL, LDVR, LWORK, M, MM, N
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      INTEGER            IWORK( * )
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), DIF( * ), S( * ),
     $                   VL( LDVL, * ), VR( LDVR, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            DIFDRI
      PARAMETER          ( DIFDRI = 3 )
      DOUBLE PRECISION   ZERO, ONE, TWO, FOUR
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TWO = 2.0D+0,
     $                   FOUR = 4.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, PAIR, SOMCON, WANTBH, WANTDF, WANTS
      INTEGER            I, IERR, IFST, ILST, IZ, K, KS, LWMIN, N1, N2
      DOUBLE PRECISION   ALPHAI, ALPHAR, ALPRQT, BETA, C1, C2, COND,
     $                   EPS, LNRM, RNRM, ROOT1, ROOT2, SCALE, SMLNUM,
     $                   TMPII, TMPIR, TMPRI, TMPRR, UHAV, UHAVI, UHBV,
     $                   UHBVI
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   DUMMY( 1 ), DUMMY1( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DDOT, DLAMCH, DLAPY2, DNRM2
      EXTERNAL           LSAME, DDOT, DLAMCH, DLAPY2, DNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMV, DLACPY, DLAG2, DTGEXC, DTGSYL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters
*
      WANTBH = LSAME( JOB, 'B' )
      WANTS = LSAME( JOB, 'E' ) .OR. WANTBH
      WANTDF = LSAME( JOB, 'V' ) .OR. WANTBH
*
      SOMCON = LSAME( HOWMNY, 'S' )
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
*
      IF( .NOT.WANTS .AND. .NOT.WANTDF ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( HOWMNY, 'A' ) .AND. .NOT.SOMCON ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( WANTS .AND. LDVL.LT.N ) THEN
         INFO = -10
      ELSE IF( WANTS .AND. LDVR.LT.N ) THEN
         INFO = -12
      ELSE
*
*        Set M to the number of eigenpairs for which condition numbers
*        are required, and test MM.
*
         IF( SOMCON ) THEN
            M = 0
            PAIR = .FALSE.
            DO 10 K = 1, N
               IF( PAIR ) THEN
                  PAIR = .FALSE.
               ELSE
                  IF( K.LT.N ) THEN
                     IF( A( K+1, K ).EQ.ZERO ) THEN
                        IF( SELECT( K ) )
     $                     M = M + 1
                     ELSE
                        PAIR = .TRUE.
                        IF( SELECT( K ) .OR. SELECT( K+1 ) )
     $                     M = M + 2
                     END IF
                  ELSE
                     IF( SELECT( N ) )
     $                  M = M + 1
                  END IF
               END IF
   10       CONTINUE
         ELSE
            M = N
         END IF
*
         IF( N.EQ.0 ) THEN
            LWMIN = 1
         ELSE IF( LSAME( JOB, 'V' ) .OR. LSAME( JOB, 'B' ) ) THEN
            LWMIN = 2*N*( N + 2 ) + 16
         ELSE
            LWMIN = N
         END IF
         WORK( 1 ) = LWMIN
*
         IF( MM.LT.M ) THEN
            INFO = -15
         ELSE IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -18
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTGSNA', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Get machine constants
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      KS = 0
      PAIR = .FALSE.
*
      DO 20 K = 1, N
*
*        Determine whether A(k,k) begins a 1-by-1 or 2-by-2 block.
*
         IF( PAIR ) THEN
            PAIR = .FALSE.
            GO TO 20
         ELSE
            IF( K.LT.N )
     $         PAIR = A( K+1, K ).NE.ZERO
         END IF
*
*        Determine whether condition numbers are required for the k-th
*        eigenpair.
*
         IF( SOMCON ) THEN
            IF( PAIR ) THEN
               IF( .NOT.SELECT( K ) .AND. .NOT.SELECT( K+1 ) )
     $            GO TO 20
            ELSE
               IF( .NOT.SELECT( K ) )
     $            GO TO 20
            END IF
         END IF
*
         KS = KS + 1
*
         IF( WANTS ) THEN
*
*           Compute the reciprocal condition number of the k-th
*           eigenvalue.
*
            IF( PAIR ) THEN
*
*              Complex eigenvalue pair.
*
               RNRM = DLAPY2( DNRM2( N, VR( 1, KS ), 1 ),
     $                DNRM2( N, VR( 1, KS+1 ), 1 ) )
               LNRM = DLAPY2( DNRM2( N, VL( 1, KS ), 1 ),
     $                DNRM2( N, VL( 1, KS+1 ), 1 ) )
               CALL DGEMV( 'N', N, N, ONE, A, LDA, VR( 1, KS ), 1, ZERO,
     $                     WORK, 1 )
               TMPRR = DDOT( N, WORK, 1, VL( 1, KS ), 1 )
               TMPRI = DDOT( N, WORK, 1, VL( 1, KS+1 ), 1 )
               CALL DGEMV( 'N', N, N, ONE, A, LDA, VR( 1, KS+1 ), 1,
     $                     ZERO, WORK, 1 )
               TMPII = DDOT( N, WORK, 1, VL( 1, KS+1 ), 1 )
               TMPIR = DDOT( N, WORK, 1, VL( 1, KS ), 1 )
               UHAV = TMPRR + TMPII
               UHAVI = TMPIR - TMPRI
               CALL DGEMV( 'N', N, N, ONE, B, LDB, VR( 1, KS ), 1, ZERO,
     $                     WORK, 1 )
               TMPRR = DDOT( N, WORK, 1, VL( 1, KS ), 1 )
               TMPRI = DDOT( N, WORK, 1, VL( 1, KS+1 ), 1 )
               CALL DGEMV( 'N', N, N, ONE, B, LDB, VR( 1, KS+1 ), 1,
     $                     ZERO, WORK, 1 )
               TMPII = DDOT( N, WORK, 1, VL( 1, KS+1 ), 1 )
               TMPIR = DDOT( N, WORK, 1, VL( 1, KS ), 1 )
               UHBV = TMPRR + TMPII
               UHBVI = TMPIR - TMPRI
               UHAV = DLAPY2( UHAV, UHAVI )
               UHBV = DLAPY2( UHBV, UHBVI )
               COND = DLAPY2( UHAV, UHBV )
               S( KS ) = COND / ( RNRM*LNRM )
               S( KS+1 ) = S( KS )
*
            ELSE
*
*              Real eigenvalue.
*
               RNRM = DNRM2( N, VR( 1, KS ), 1 )
               LNRM = DNRM2( N, VL( 1, KS ), 1 )
               CALL DGEMV( 'N', N, N, ONE, A, LDA, VR( 1, KS ), 1, ZERO,
     $                     WORK, 1 )
               UHAV = DDOT( N, WORK, 1, VL( 1, KS ), 1 )
               CALL DGEMV( 'N', N, N, ONE, B, LDB, VR( 1, KS ), 1, ZERO,
     $                     WORK, 1 )
               UHBV = DDOT( N, WORK, 1, VL( 1, KS ), 1 )
               COND = DLAPY2( UHAV, UHBV )
               IF( COND.EQ.ZERO ) THEN
                  S( KS ) = -ONE
               ELSE
                  S( KS ) = COND / ( RNRM*LNRM )
               END IF
            END IF
         END IF
*
         IF( WANTDF ) THEN
            IF( N.EQ.1 ) THEN
               DIF( KS ) = DLAPY2( A( 1, 1 ), B( 1, 1 ) )
               GO TO 20
            END IF
*
*           Estimate the reciprocal condition number of the k-th
*           eigenvectors.
            IF( PAIR ) THEN
*
*              Copy the  2-by 2 pencil beginning at (A(k,k), B(k, k)).
*              Compute the eigenvalue(s) at position K.
*
               WORK( 1 ) = A( K, K )
               WORK( 2 ) = A( K+1, K )
               WORK( 3 ) = A( K, K+1 )
               WORK( 4 ) = A( K+1, K+1 )
               WORK( 5 ) = B( K, K )
               WORK( 6 ) = B( K+1, K )
               WORK( 7 ) = B( K, K+1 )
               WORK( 8 ) = B( K+1, K+1 )
               CALL DLAG2( WORK, 2, WORK( 5 ), 2, SMLNUM*EPS, BETA,
     $                     DUMMY1( 1 ), ALPHAR, DUMMY( 1 ), ALPHAI )
               ALPRQT = ONE
               C1 = TWO*( ALPHAR*ALPHAR+ALPHAI*ALPHAI+BETA*BETA )
               C2 = FOUR*BETA*BETA*ALPHAI*ALPHAI
               ROOT1 = C1 + SQRT( C1*C1-4.0D0*C2 )
               ROOT2 = C2 / ROOT1
               ROOT1 = ROOT1 / TWO
               COND = MIN( SQRT( ROOT1 ), SQRT( ROOT2 ) )
            END IF
*
*           Copy the matrix (A, B) to the array WORK and swap the
*           diagonal block beginning at A(k,k) to the (1,1) position.
*
            CALL DLACPY( 'Full', N, N, A, LDA, WORK, N )
            CALL DLACPY( 'Full', N, N, B, LDB, WORK( N*N+1 ), N )
            IFST = K
            ILST = 1
*
            CALL DTGEXC( .FALSE., .FALSE., N, WORK, N, WORK( N*N+1 ), N,
     $                   DUMMY, 1, DUMMY1, 1, IFST, ILST,
     $                   WORK( N*N*2+1 ), LWORK-2*N*N, IERR )
*
            IF( IERR.GT.0 ) THEN
*
*              Ill-conditioned problem - swap rejected.
*
               DIF( KS ) = ZERO
            ELSE
*
*              Reordering successful, solve generalized Sylvester
*              equation for R and L,
*                         A22 * R - L * A11 = A12
*                         B22 * R - L * B11 = B12,
*              and compute estimate of Difl((A11,B11), (A22, B22)).
*
               N1 = 1
               IF( WORK( 2 ).NE.ZERO )
     $            N1 = 2
               N2 = N - N1
               IF( N2.EQ.0 ) THEN
                  DIF( KS ) = COND
               ELSE
                  I = N*N + 1
                  IZ = 2*N*N + 1
                  CALL DTGSYL( 'N', DIFDRI, N2, N1, WORK( N*N1+N1+1 ),
     $                         N, WORK, N, WORK( N1+1 ), N,
     $                         WORK( N*N1+N1+I ), N, WORK( I ), N,
     $                         WORK( N1+I ), N, SCALE, DIF( KS ),
     $                         WORK( IZ+1 ), LWORK-2*N*N, IWORK, IERR )
*
                  IF( PAIR )
     $               DIF( KS ) = MIN( MAX( ONE, ALPRQT )*DIF( KS ),
     $                           COND )
               END IF
            END IF
            IF( PAIR )
     $         DIF( KS+1 ) = DIF( KS )
         END IF
         IF( PAIR )
     $      KS = KS + 1
*
   20 CONTINUE
      WORK( 1 ) = LWMIN
      RETURN
*
*     End of DTGSNA
*
      END
