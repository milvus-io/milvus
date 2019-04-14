*> \brief \b DTGSEN
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTGSEN + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtgsen.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtgsen.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtgsen.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTGSEN( IJOB, WANTQ, WANTZ, SELECT, N, A, LDA, B, LDB,
*                          ALPHAR, ALPHAI, BETA, Q, LDQ, Z, LDZ, M, PL,
*                          PR, DIF, WORK, LWORK, IWORK, LIWORK, INFO )
*
*       .. Scalar Arguments ..
*       LOGICAL            WANTQ, WANTZ
*       INTEGER            IJOB, INFO, LDA, LDB, LDQ, LDZ, LIWORK, LWORK,
*      $                   M, N
*       DOUBLE PRECISION   PL, PR
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
*      $                   B( LDB, * ), BETA( * ), DIF( * ), Q( LDQ, * ),
*      $                   WORK( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTGSEN reorders the generalized real Schur decomposition of a real
*> matrix pair (A, B) (in terms of an orthonormal equivalence trans-
*> formation Q**T * (A, B) * Z), so that a selected cluster of eigenvalues
*> appears in the leading diagonal blocks of the upper quasi-triangular
*> matrix A and the upper triangular B. The leading columns of Q and
*> Z form orthonormal bases of the corresponding left and right eigen-
*> spaces (deflating subspaces). (A, B) must be in generalized real
*> Schur canonical form (as returned by DGGES), i.e. A is block upper
*> triangular with 1-by-1 and 2-by-2 diagonal blocks. B is upper
*> triangular.
*>
*> DTGSEN also computes the generalized eigenvalues
*>
*>             w(j) = (ALPHAR(j) + i*ALPHAI(j))/BETA(j)
*>
*> of the reordered matrix pair (A, B).
*>
*> Optionally, DTGSEN computes the estimates of reciprocal condition
*> numbers for eigenvalues and eigenspaces. These are Difu[(A11,B11),
*> (A22,B22)] and Difl[(A11,B11), (A22,B22)], i.e. the separation(s)
*> between the matrix pairs (A11, B11) and (A22,B22) that correspond to
*> the selected cluster and the eigenvalues outside the cluster, resp.,
*> and norms of "projections" onto left and right eigenspaces w.r.t.
*> the selected cluster in the (1,1)-block.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IJOB
*> \verbatim
*>          IJOB is INTEGER
*>          Specifies whether condition numbers are required for the
*>          cluster of eigenvalues (PL and PR) or the deflating subspaces
*>          (Difu and Difl):
*>           =0: Only reorder w.r.t. SELECT. No extras.
*>           =1: Reciprocal of norms of "projections" onto left and right
*>               eigenspaces w.r.t. the selected cluster (PL and PR).
*>           =2: Upper bounds on Difu and Difl. F-norm-based estimate
*>               (DIF(1:2)).
*>           =3: Estimate of Difu and Difl. 1-norm-based estimate
*>               (DIF(1:2)).
*>               About 5 times as expensive as IJOB = 2.
*>           =4: Compute PL, PR and DIF (i.e. 0, 1 and 2 above): Economic
*>               version to get it all.
*>           =5: Compute PL, PR and DIF (i.e. 0, 1 and 3 above)
*> \endverbatim
*>
*> \param[in] WANTQ
*> \verbatim
*>          WANTQ is LOGICAL
*>          .TRUE. : update the left transformation matrix Q;
*>          .FALSE.: do not update Q.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is LOGICAL
*>          .TRUE. : update the right transformation matrix Z;
*>          .FALSE.: do not update Z.
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          SELECT specifies the eigenvalues in the selected cluster.
*>          To select a real eigenvalue w(j), SELECT(j) must be set to
*>          .TRUE.. To select a complex conjugate pair of eigenvalues
*>          w(j) and w(j+1), corresponding to a 2-by-2 diagonal block,
*>          either SELECT(j) or SELECT(j+1) or both must be set to
*>          .TRUE.; a complex conjugate pair of eigenvalues must be
*>          either both included in the cluster or both excluded.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B. N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension(LDA,N)
*>          On entry, the upper quasi-triangular matrix A, with (A, B) in
*>          generalized real Schur canonical form.
*>          On exit, A is overwritten by the reordered matrix A.
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
*>          B is DOUBLE PRECISION array, dimension(LDB,N)
*>          On entry, the upper triangular matrix B, with (A, B) in
*>          generalized real Schur canonical form.
*>          On exit, B is overwritten by the reordered matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is DOUBLE PRECISION array, dimension (N)
*>
*>          On exit, (ALPHAR(j) + ALPHAI(j)*i)/BETA(j), j=1,...,N, will
*>          be the generalized eigenvalues.  ALPHAR(j) + ALPHAI(j)*i
*>          and BETA(j),j=1,...,N  are the diagonals of the complex Schur
*>          form (S,T) that would result if the 2-by-2 diagonal blocks of
*>          the real generalized Schur form of (A,B) were further reduced
*>          to triangular form using complex unitary transformations.
*>          If ALPHAI(j) is zero, then the j-th eigenvalue is real; if
*>          positive, then the j-th and (j+1)-st eigenvalues are a
*>          complex conjugate pair, with ALPHAI(j+1) negative.
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ,N)
*>          On entry, if WANTQ = .TRUE., Q is an N-by-N matrix.
*>          On exit, Q has been postmultiplied by the left orthogonal
*>          transformation matrix which reorder (A, B); The leading M
*>          columns of Q form orthonormal bases for the specified pair of
*>          left eigenspaces (deflating subspaces).
*>          If WANTQ = .FALSE., Q is not referenced.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.  LDQ >= 1;
*>          and if WANTQ = .TRUE., LDQ >= N.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDZ,N)
*>          On entry, if WANTZ = .TRUE., Z is an N-by-N matrix.
*>          On exit, Z has been postmultiplied by the left orthogonal
*>          transformation matrix which reorder (A, B); The leading M
*>          columns of Z form orthonormal bases for the specified pair of
*>          left eigenspaces (deflating subspaces).
*>          If WANTZ = .FALSE., Z is not referenced.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z. LDZ >= 1;
*>          If WANTZ = .TRUE., LDZ >= N.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The dimension of the specified pair of left and right eigen-
*>          spaces (deflating subspaces). 0 <= M <= N.
*> \endverbatim
*>
*> \param[out] PL
*> \verbatim
*>          PL is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[out] PR
*> \verbatim
*>          PR is DOUBLE PRECISION
*>
*>          If IJOB = 1, 4 or 5, PL, PR are lower bounds on the
*>          reciprocal of the norm of "projections" onto left and right
*>          eigenspaces with respect to the selected cluster.
*>          0 < PL, PR <= 1.
*>          If M = 0 or M = N, PL = PR  = 1.
*>          If IJOB = 0, 2 or 3, PL and PR are not referenced.
*> \endverbatim
*>
*> \param[out] DIF
*> \verbatim
*>          DIF is DOUBLE PRECISION array, dimension (2).
*>          If IJOB >= 2, DIF(1:2) store the estimates of Difu and Difl.
*>          If IJOB = 2 or 4, DIF(1:2) are F-norm-based upper bounds on
*>          Difu and Difl. If IJOB = 3 or 5, DIF(1:2) are 1-norm-based
*>          estimates of Difu and Difl.
*>          If M = 0 or N, DIF(1:2) = F-norm([A, B]).
*>          If IJOB = 0 or 1, DIF is not referenced.
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
*>          The dimension of the array WORK. LWORK >=  4*N+16.
*>          If IJOB = 1, 2 or 4, LWORK >= MAX(4*N+16, 2*M*(N-M)).
*>          If IJOB = 3 or 5, LWORK >= MAX(4*N+16, 4*M*(N-M)).
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MAX(1,LIWORK))
*>          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK. LIWORK >= 1.
*>          If IJOB = 1, 2 or 4, LIWORK >=  N+6.
*>          If IJOB = 3 or 5, LIWORK >= MAX(2*M*(N-M), N+6).
*>
*>          If LIWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal size of the IWORK array,
*>          returns this value as the first entry of the IWORK array, and
*>          no error message related to LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>            =0: Successful exit.
*>            <0: If INFO = -i, the i-th argument had an illegal value.
*>            =1: Reordering of (A, B) failed because the transformed
*>                matrix pair (A, B) would be too far from generalized
*>                Schur form; the problem is very ill-conditioned.
*>                (A, B) may have been partially reordered.
*>                If requested, 0 is returned in DIF(*), PL and PR.
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
*> \date June 2016
*
*> \ingroup doubleOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  DTGSEN first collects the selected eigenvalues by computing
*>  orthogonal U and W that move them to the top left corner of (A, B).
*>  In other words, the selected eigenvalues are the eigenvalues of
*>  (A11, B11) in:
*>
*>              U**T*(A, B)*W = (A11 A12) (B11 B12) n1
*>                              ( 0  A22),( 0  B22) n2
*>                                n1  n2    n1  n2
*>
*>  where N = n1+n2 and U**T means the transpose of U. The first n1 columns
*>  of U and W span the specified pair of left and right eigenspaces
*>  (deflating subspaces) of (A, B).
*>
*>  If (A, B) has been obtained from the generalized real Schur
*>  decomposition of a matrix pair (C, D) = Q*(A, B)*Z**T, then the
*>  reordered generalized real Schur form of (C, D) is given by
*>
*>           (C, D) = (Q*U)*(U**T*(A, B)*W)*(Z*W)**T,
*>
*>  and the first n1 columns of Q*U and Z*W span the corresponding
*>  deflating subspaces of (C, D) (Q and Z store Q*U and Z*W, resp.).
*>
*>  Note that if the selected eigenvalue is sufficiently ill-conditioned,
*>  then its value may differ significantly from its value before
*>  reordering.
*>
*>  The reciprocal condition numbers of the left and right eigenspaces
*>  spanned by the first n1 columns of U and W (or Q*U and Z*W) may
*>  be returned in DIF(1:2), corresponding to Difu and Difl, resp.
*>
*>  The Difu and Difl are defined as:
*>
*>       Difu[(A11, B11), (A22, B22)] = sigma-min( Zu )
*>  and
*>       Difl[(A11, B11), (A22, B22)] = Difu[(A22, B22), (A11, B11)],
*>
*>  where sigma-min(Zu) is the smallest singular value of the
*>  (2*n1*n2)-by-(2*n1*n2) matrix
*>
*>       Zu = [ kron(In2, A11)  -kron(A22**T, In1) ]
*>            [ kron(In2, B11)  -kron(B22**T, In1) ].
*>
*>  Here, Inx is the identity matrix of size nx and A22**T is the
*>  transpose of A22. kron(X, Y) is the Kronecker product between
*>  the matrices X and Y.
*>
*>  When DIF(2) is small, small changes in (A, B) can cause large changes
*>  in the deflating subspace. An approximate (asymptotic) bound on the
*>  maximum angular error in the computed deflating subspaces is
*>
*>       EPS * norm((A, B)) / DIF(2),
*>
*>  where EPS is the machine precision.
*>
*>  The reciprocal norm of the projectors on the left and right
*>  eigenspaces associated with (A11, B11) may be returned in PL and PR.
*>  They are computed as follows. First we compute L and R so that
*>  P*(A, B)*Q is block diagonal, where
*>
*>       P = ( I -L ) n1           Q = ( I R ) n1
*>           ( 0  I ) n2    and        ( 0 I ) n2
*>             n1 n2                    n1 n2
*>
*>  and (L, R) is the solution to the generalized Sylvester equation
*>
*>       A11*R - L*A22 = -A12
*>       B11*R - L*B22 = -B12
*>
*>  Then PL = (F-norm(L)**2+1)**(-1/2) and PR = (F-norm(R)**2+1)**(-1/2).
*>  An approximate (asymptotic) bound on the average absolute error of
*>  the selected eigenvalues is
*>
*>       EPS * norm((A, B)) / PL.
*>
*>  There are also global error bounds which valid for perturbations up
*>  to a certain restriction:  A lower bound (x) on the smallest
*>  F-norm(E,F) for which an eigenvalue of (A11, B11) may move and
*>  coalesce with an eigenvalue of (A22, B22) under perturbation (E,F),
*>  (i.e. (A + E, B + F), is
*>
*>   x = min(Difu,Difl)/((1/(PL*PL)+1/(PR*PR))**(1/2)+2*max(1/PL,1/PR)).
*>
*>  An approximate bound on x can be computed from DIF(1:2), PL and PR.
*>
*>  If y = ( F-norm(E,F) / x) <= 1, the angles between the perturbed
*>  (L', R') and unperturbed (L, R) left and right deflating subspaces
*>  associated with the selected cluster in the (1,1)-blocks can be
*>  bounded as
*>
*>   max-angle(L, L') <= arctan( y * PL / (1 - y * (1 - PL * PL)**(1/2))
*>   max-angle(R, R') <= arctan( y * PR / (1 - y * (1 - PR * PR)**(1/2))
*>
*>  See LAPACK User's Guide section 4.11 or the following references
*>  for more information.
*>
*>  Note that if the default method for computing the Frobenius-norm-
*>  based estimate DIF is not wanted (see DLATDF), then the parameter
*>  IDIFJB (see below) should be changed from 3 to 4 (routine DLATDF
*>  (IJOB = 2 will be used)). See DTGSYL for more details.
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
*>      Note 75. To appear in ACM Trans. on Math. Software, Vol 22, No 1,
*>      1996.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DTGSEN( IJOB, WANTQ, WANTZ, SELECT, N, A, LDA, B, LDB,
     $                   ALPHAR, ALPHAI, BETA, Q, LDQ, Z, LDZ, M, PL,
     $                   PR, DIF, WORK, LWORK, IWORK, LIWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      LOGICAL            WANTQ, WANTZ
      INTEGER            IJOB, INFO, LDA, LDB, LDQ, LDZ, LIWORK, LWORK,
     $                   M, N
      DOUBLE PRECISION   PL, PR
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      INTEGER            IWORK( * )
      DOUBLE PRECISION   A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
     $                   B( LDB, * ), BETA( * ), DIF( * ), Q( LDQ, * ),
     $                   WORK( * ), Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            IDIFJB
      PARAMETER          ( IDIFJB = 3 )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, PAIR, SWAP, WANTD, WANTD1, WANTD2,
     $                   WANTP
      INTEGER            I, IERR, IJB, K, KASE, KK, KS, LIWMIN, LWMIN,
     $                   MN2, N1, N2
      DOUBLE PRECISION   DSCALE, DSUM, EPS, RDSCAL, SMLNUM
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLACN2, DLACPY, DLAG2, DLASSQ, DTGEXC, DTGSYL,
     $                   XERBLA
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 .OR. LIWORK.EQ.-1 )
*
      IF( IJOB.LT.0 .OR. IJOB.GT.5 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LDQ.LT.1 .OR. ( WANTQ .AND. LDQ.LT.N ) ) THEN
         INFO = -14
      ELSE IF( LDZ.LT.1 .OR. ( WANTZ .AND. LDZ.LT.N ) ) THEN
         INFO = -16
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTGSEN', -INFO )
         RETURN
      END IF
*
*     Get machine constants
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      IERR = 0
*
      WANTP = IJOB.EQ.1 .OR. IJOB.GE.4
      WANTD1 = IJOB.EQ.2 .OR. IJOB.EQ.4
      WANTD2 = IJOB.EQ.3 .OR. IJOB.EQ.5
      WANTD = WANTD1 .OR. WANTD2
*
*     Set M to the dimension of the specified pair of deflating
*     subspaces.
*
      M = 0
      PAIR = .FALSE.
      IF( .NOT.LQUERY .OR. IJOB.NE.0 ) THEN
      DO 10 K = 1, N
         IF( PAIR ) THEN
            PAIR = .FALSE.
         ELSE
            IF( K.LT.N ) THEN
               IF( A( K+1, K ).EQ.ZERO ) THEN
                  IF( SELECT( K ) )
     $               M = M + 1
               ELSE
                  PAIR = .TRUE.
                  IF( SELECT( K ) .OR. SELECT( K+1 ) )
     $               M = M + 2
               END IF
            ELSE
               IF( SELECT( N ) )
     $            M = M + 1
            END IF
         END IF
   10 CONTINUE
      END IF
*
      IF( IJOB.EQ.1 .OR. IJOB.EQ.2 .OR. IJOB.EQ.4 ) THEN
         LWMIN = MAX( 1, 4*N+16, 2*M*( N-M ) )
         LIWMIN = MAX( 1, N+6 )
      ELSE IF( IJOB.EQ.3 .OR. IJOB.EQ.5 ) THEN
         LWMIN = MAX( 1, 4*N+16, 4*M*( N-M ) )
         LIWMIN = MAX( 1, 2*M*( N-M ), N+6 )
      ELSE
         LWMIN = MAX( 1, 4*N+16 )
         LIWMIN = 1
      END IF
*
      WORK( 1 ) = LWMIN
      IWORK( 1 ) = LIWMIN
*
      IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
         INFO = -22
      ELSE IF( LIWORK.LT.LIWMIN .AND. .NOT.LQUERY ) THEN
         INFO = -24
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTGSEN', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( M.EQ.N .OR. M.EQ.0 ) THEN
         IF( WANTP ) THEN
            PL = ONE
            PR = ONE
         END IF
         IF( WANTD ) THEN
            DSCALE = ZERO
            DSUM = ONE
            DO 20 I = 1, N
               CALL DLASSQ( N, A( 1, I ), 1, DSCALE, DSUM )
               CALL DLASSQ( N, B( 1, I ), 1, DSCALE, DSUM )
   20       CONTINUE
            DIF( 1 ) = DSCALE*SQRT( DSUM )
            DIF( 2 ) = DIF( 1 )
         END IF
         GO TO 60
      END IF
*
*     Collect the selected blocks at the top-left corner of (A, B).
*
      KS = 0
      PAIR = .FALSE.
      DO 30 K = 1, N
         IF( PAIR ) THEN
            PAIR = .FALSE.
         ELSE
*
            SWAP = SELECT( K )
            IF( K.LT.N ) THEN
               IF( A( K+1, K ).NE.ZERO ) THEN
                  PAIR = .TRUE.
                  SWAP = SWAP .OR. SELECT( K+1 )
               END IF
            END IF
*
            IF( SWAP ) THEN
               KS = KS + 1
*
*              Swap the K-th block to position KS.
*              Perform the reordering of diagonal blocks in (A, B)
*              by orthogonal transformation matrices and update
*              Q and Z accordingly (if requested):
*
               KK = K
               IF( K.NE.KS )
     $            CALL DTGEXC( WANTQ, WANTZ, N, A, LDA, B, LDB, Q, LDQ,
     $                         Z, LDZ, KK, KS, WORK, LWORK, IERR )
*
               IF( IERR.GT.0 ) THEN
*
*                 Swap is rejected: exit.
*
                  INFO = 1
                  IF( WANTP ) THEN
                     PL = ZERO
                     PR = ZERO
                  END IF
                  IF( WANTD ) THEN
                     DIF( 1 ) = ZERO
                     DIF( 2 ) = ZERO
                  END IF
                  GO TO 60
               END IF
*
               IF( PAIR )
     $            KS = KS + 1
            END IF
         END IF
   30 CONTINUE
      IF( WANTP ) THEN
*
*        Solve generalized Sylvester equation for R and L
*        and compute PL and PR.
*
         N1 = M
         N2 = N - M
         I = N1 + 1
         IJB = 0
         CALL DLACPY( 'Full', N1, N2, A( 1, I ), LDA, WORK, N1 )
         CALL DLACPY( 'Full', N1, N2, B( 1, I ), LDB, WORK( N1*N2+1 ),
     $                N1 )
         CALL DTGSYL( 'N', IJB, N1, N2, A, LDA, A( I, I ), LDA, WORK,
     $                N1, B, LDB, B( I, I ), LDB, WORK( N1*N2+1 ), N1,
     $                DSCALE, DIF( 1 ), WORK( N1*N2*2+1 ),
     $                LWORK-2*N1*N2, IWORK, IERR )
*
*        Estimate the reciprocal of norms of "projections" onto left
*        and right eigenspaces.
*
         RDSCAL = ZERO
         DSUM = ONE
         CALL DLASSQ( N1*N2, WORK, 1, RDSCAL, DSUM )
         PL = RDSCAL*SQRT( DSUM )
         IF( PL.EQ.ZERO ) THEN
            PL = ONE
         ELSE
            PL = DSCALE / ( SQRT( DSCALE*DSCALE / PL+PL )*SQRT( PL ) )
         END IF
         RDSCAL = ZERO
         DSUM = ONE
         CALL DLASSQ( N1*N2, WORK( N1*N2+1 ), 1, RDSCAL, DSUM )
         PR = RDSCAL*SQRT( DSUM )
         IF( PR.EQ.ZERO ) THEN
            PR = ONE
         ELSE
            PR = DSCALE / ( SQRT( DSCALE*DSCALE / PR+PR )*SQRT( PR ) )
         END IF
      END IF
*
      IF( WANTD ) THEN
*
*        Compute estimates of Difu and Difl.
*
         IF( WANTD1 ) THEN
            N1 = M
            N2 = N - M
            I = N1 + 1
            IJB = IDIFJB
*
*           Frobenius norm-based Difu-estimate.
*
            CALL DTGSYL( 'N', IJB, N1, N2, A, LDA, A( I, I ), LDA, WORK,
     $                   N1, B, LDB, B( I, I ), LDB, WORK( N1*N2+1 ),
     $                   N1, DSCALE, DIF( 1 ), WORK( 2*N1*N2+1 ),
     $                   LWORK-2*N1*N2, IWORK, IERR )
*
*           Frobenius norm-based Difl-estimate.
*
            CALL DTGSYL( 'N', IJB, N2, N1, A( I, I ), LDA, A, LDA, WORK,
     $                   N2, B( I, I ), LDB, B, LDB, WORK( N1*N2+1 ),
     $                   N2, DSCALE, DIF( 2 ), WORK( 2*N1*N2+1 ),
     $                   LWORK-2*N1*N2, IWORK, IERR )
         ELSE
*
*
*           Compute 1-norm-based estimates of Difu and Difl using
*           reversed communication with DLACN2. In each step a
*           generalized Sylvester equation or a transposed variant
*           is solved.
*
            KASE = 0
            N1 = M
            N2 = N - M
            I = N1 + 1
            IJB = 0
            MN2 = 2*N1*N2
*
*           1-norm-based estimate of Difu.
*
   40       CONTINUE
            CALL DLACN2( MN2, WORK( MN2+1 ), WORK, IWORK, DIF( 1 ),
     $                   KASE, ISAVE )
            IF( KASE.NE.0 ) THEN
               IF( KASE.EQ.1 ) THEN
*
*                 Solve generalized Sylvester equation.
*
                  CALL DTGSYL( 'N', IJB, N1, N2, A, LDA, A( I, I ), LDA,
     $                         WORK, N1, B, LDB, B( I, I ), LDB,
     $                         WORK( N1*N2+1 ), N1, DSCALE, DIF( 1 ),
     $                         WORK( 2*N1*N2+1 ), LWORK-2*N1*N2, IWORK,
     $                         IERR )
               ELSE
*
*                 Solve the transposed variant.
*
                  CALL DTGSYL( 'T', IJB, N1, N2, A, LDA, A( I, I ), LDA,
     $                         WORK, N1, B, LDB, B( I, I ), LDB,
     $                         WORK( N1*N2+1 ), N1, DSCALE, DIF( 1 ),
     $                         WORK( 2*N1*N2+1 ), LWORK-2*N1*N2, IWORK,
     $                         IERR )
               END IF
               GO TO 40
            END IF
            DIF( 1 ) = DSCALE / DIF( 1 )
*
*           1-norm-based estimate of Difl.
*
   50       CONTINUE
            CALL DLACN2( MN2, WORK( MN2+1 ), WORK, IWORK, DIF( 2 ),
     $                   KASE, ISAVE )
            IF( KASE.NE.0 ) THEN
               IF( KASE.EQ.1 ) THEN
*
*                 Solve generalized Sylvester equation.
*
                  CALL DTGSYL( 'N', IJB, N2, N1, A( I, I ), LDA, A, LDA,
     $                         WORK, N2, B( I, I ), LDB, B, LDB,
     $                         WORK( N1*N2+1 ), N2, DSCALE, DIF( 2 ),
     $                         WORK( 2*N1*N2+1 ), LWORK-2*N1*N2, IWORK,
     $                         IERR )
               ELSE
*
*                 Solve the transposed variant.
*
                  CALL DTGSYL( 'T', IJB, N2, N1, A( I, I ), LDA, A, LDA,
     $                         WORK, N2, B( I, I ), LDB, B, LDB,
     $                         WORK( N1*N2+1 ), N2, DSCALE, DIF( 2 ),
     $                         WORK( 2*N1*N2+1 ), LWORK-2*N1*N2, IWORK,
     $                         IERR )
               END IF
               GO TO 50
            END IF
            DIF( 2 ) = DSCALE / DIF( 2 )
*
         END IF
      END IF
*
   60 CONTINUE
*
*     Compute generalized eigenvalues of reordered pair (A, B) and
*     normalize the generalized Schur form.
*
      PAIR = .FALSE.
      DO 80 K = 1, N
         IF( PAIR ) THEN
            PAIR = .FALSE.
         ELSE
*
            IF( K.LT.N ) THEN
               IF( A( K+1, K ).NE.ZERO ) THEN
                  PAIR = .TRUE.
               END IF
            END IF
*
            IF( PAIR ) THEN
*
*             Compute the eigenvalue(s) at position K.
*
               WORK( 1 ) = A( K, K )
               WORK( 2 ) = A( K+1, K )
               WORK( 3 ) = A( K, K+1 )
               WORK( 4 ) = A( K+1, K+1 )
               WORK( 5 ) = B( K, K )
               WORK( 6 ) = B( K+1, K )
               WORK( 7 ) = B( K, K+1 )
               WORK( 8 ) = B( K+1, K+1 )
               CALL DLAG2( WORK, 2, WORK( 5 ), 2, SMLNUM*EPS, BETA( K ),
     $                     BETA( K+1 ), ALPHAR( K ), ALPHAR( K+1 ),
     $                     ALPHAI( K ) )
               ALPHAI( K+1 ) = -ALPHAI( K )
*
            ELSE
*
               IF( SIGN( ONE, B( K, K ) ).LT.ZERO ) THEN
*
*                 If B(K,K) is negative, make it positive
*
                  DO 70 I = 1, N
                     A( K, I ) = -A( K, I )
                     B( K, I ) = -B( K, I )
                     IF( WANTQ ) Q( I, K ) = -Q( I, K )
   70             CONTINUE
               END IF
*
               ALPHAR( K ) = A( K, K )
               ALPHAI( K ) = ZERO
               BETA( K ) = B( K, K )
*
            END IF
         END IF
   80 CONTINUE
*
      WORK( 1 ) = LWMIN
      IWORK( 1 ) = LIWMIN
*
      RETURN
*
*     End of DTGSEN
*
      END
