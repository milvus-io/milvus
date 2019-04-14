*> \brief \b ZTGSNA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTGSNA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztgsna.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztgsna.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztgsna.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTGSNA( JOB, HOWMNY, SELECT, N, A, LDA, B, LDB, VL,
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
*       DOUBLE PRECISION   DIF( * ), S( * )
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), VL( LDVL, * ),
*      $                   VR( LDVR, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTGSNA estimates reciprocal condition numbers for specified
*> eigenvalues and/or eigenvectors of a matrix pair (A, B).
*>
*> (A, B) must be in generalized Schur canonical form, that is, A and
*> B are both upper triangular.
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
*>          for the corresponding j-th eigenvalue and/or eigenvector,
*>          SELECT(j) must be set to .TRUE..
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
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The upper triangular matrix A in the pair (A,B).
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
*>          B is COMPLEX*16 array, dimension (LDB,N)
*>          The upper triangular matrix B in the pair (A, B).
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
*>          VL is COMPLEX*16 array, dimension (LDVL,M)
*>          IF JOB = 'E' or 'B', VL must contain left eigenvectors of
*>          (A, B), corresponding to the eigenpairs specified by HOWMNY
*>          and SELECT.  The eigenvectors must be stored in consecutive
*>          columns of VL, as returned by ZTGEVC.
*>          If JOB = 'V', VL is not referenced.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the array VL. LDVL >= 1; and
*>          If JOB = 'E' or 'B', LDVL >= N.
*> \endverbatim
*>
*> \param[in] VR
*> \verbatim
*>          VR is COMPLEX*16 array, dimension (LDVR,M)
*>          IF JOB = 'E' or 'B', VR must contain right eigenvectors of
*>          (A, B), corresponding to the eigenpairs specified by HOWMNY
*>          and SELECT.  The eigenvectors must be stored in consecutive
*>          columns of VR, as returned by ZTGEVC.
*>          If JOB = 'V', VR is not referenced.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR. LDVR >= 1;
*>          If JOB = 'E' or 'B', LDVR >= N.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (MM)
*>          If JOB = 'E' or 'B', the reciprocal condition numbers of the
*>          selected eigenvalues, stored in consecutive elements of the
*>          array.
*>          If JOB = 'V', S is not referenced.
*> \endverbatim
*>
*> \param[out] DIF
*> \verbatim
*>          DIF is DOUBLE PRECISION array, dimension (MM)
*>          If JOB = 'V' or 'B', the estimated reciprocal condition
*>          numbers of the selected eigenvectors, stored in consecutive
*>          elements of the array.
*>          If the eigenvalues cannot be reordered to compute DIF(j),
*>          DIF(j) is set to 0; this can only occur when the true value
*>          would be very small anyway.
*>          For each eigenvalue/vector specified by SELECT, DIF stores
*>          a Frobenius norm-based estimate of Difl.
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
*>          the specified condition numbers; for each selected eigenvalue
*>          one element is used. If HOWMNY = 'A', M is set to N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= max(1,N).
*>          If JOB = 'V' or 'B', LWORK >= max(1,2*N*N).
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N+2)
*>          If JOB = 'E', IWORK is not referenced.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: Successful exit
*>          < 0: If INFO = -i, the i-th argument had an illegal value
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
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The reciprocal of the condition number of the i-th generalized
*>  eigenvalue w = (a, b) is defined as
*>
*>          S(I) = (|v**HAu|**2 + |v**HBu|**2)**(1/2) / (norm(u)*norm(v))
*>
*>  where u and v are the right and left eigenvectors of (A, B)
*>  corresponding to w; |z| denotes the absolute value of the complex
*>  number, and norm(u) denotes the 2-norm of the vector u. The pair
*>  (a, b) corresponds to an eigenvalue w = a/b (= v**HAu/v**HBu) of the
*>  matrix pair (A, B). If both a and b equal zero, then (A,B) is
*>  singular and S(I) = -1 is returned.
*>
*>  An approximate error bound on the chordal distance between the i-th
*>  computed generalized eigenvalue w and the corresponding exact
*>  eigenvalue lambda is
*>
*>          chord(w, lambda) <=   EPS * norm(A, B) / S(I),
*>
*>  where EPS is the machine precision.
*>
*>  The reciprocal of the condition number of the right eigenvector u
*>  and left eigenvector v corresponding to the generalized eigenvalue w
*>  is defined as follows. Suppose
*>
*>                   (A, B) = ( a   *  ) ( b  *  )  1
*>                            ( 0  A22 ),( 0 B22 )  n-1
*>                              1  n-1     1 n-1
*>
*>  Then the reciprocal condition number DIF(I) is
*>
*>          Difl[(a, b), (A22, B22)]  = sigma-min( Zl )
*>
*>  where sigma-min(Zl) denotes the smallest singular value of
*>
*>         Zl = [ kron(a, In-1) -kron(1, A22) ]
*>              [ kron(b, In-1) -kron(1, B22) ].
*>
*>  Here In-1 is the identity matrix of size n-1 and X**H is the conjugate
*>  transpose of X. kron(X, Y) is the Kronecker product between the
*>  matrices X and Y.
*>
*>  We approximate the smallest singular value of Zl with an upper
*>  bound. This is done by ZLATDF.
*>
*>  An approximate error bound for a computed eigenvector VL(i) or
*>  VR(i) is given by
*>
*>                      EPS * norm(A, B) / DIF(i).
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
*>      Estimation: Theory, Algorithms and Software, Report
*>      UMINF - 94.04, Department of Computing Science, Umea University,
*>      S-901 87 Umea, Sweden, 1994. Also as LAPACK Working Note 87.
*>      To appear in Numerical Algorithms, 1996.
*>
*>  [3] B. Kagstrom and P. Poromaa, LAPACK-Style Algorithms and Software
*>      for Solving the Generalized Sylvester Equation and Estimating the
*>      Separation between Regular Matrix Pairs, Report UMINF - 93.23,
*>      Department of Computing Science, Umea University, S-901 87 Umea,
*>      Sweden, December 1993, Revised April 1994, Also as LAPACK Working
*>      Note 75.
*>      To appear in ACM Trans. on Math. Software, Vol 22, No 1, 1996.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZTGSNA( JOB, HOWMNY, SELECT, N, A, LDA, B, LDB, VL,
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
      DOUBLE PRECISION   DIF( * ), S( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), VL( LDVL, * ),
     $                   VR( LDVR, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      INTEGER            IDIFJB
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, IDIFJB = 3 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, SOMCON, WANTBH, WANTDF, WANTS
      INTEGER            I, IERR, IFST, ILST, K, KS, LWMIN, N1, N2
      DOUBLE PRECISION   BIGNUM, COND, EPS, LNRM, RNRM, SCALE, SMLNUM
      COMPLEX*16         YHAX, YHBX
*     ..
*     .. Local Arrays ..
      COMPLEX*16         DUMMY( 1 ), DUMMY1( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLAPY2, DZNRM2
      COMPLEX*16         ZDOTC
      EXTERNAL           LSAME, DLAMCH, DLAPY2, DZNRM2, ZDOTC
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, XERBLA, ZGEMV, ZLACPY, ZTGEXC, ZTGSYL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DCMPLX, MAX
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
            DO 10 K = 1, N
               IF( SELECT( K ) )
     $            M = M + 1
   10       CONTINUE
         ELSE
            M = N
         END IF
*
         IF( N.EQ.0 ) THEN
            LWMIN = 1
         ELSE IF( LSAME( JOB, 'V' ) .OR. LSAME( JOB, 'B' ) ) THEN
            LWMIN = 2*N*N
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
         CALL XERBLA( 'ZTGSNA', -INFO )
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
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
      KS = 0
      DO 20 K = 1, N
*
*        Determine whether condition numbers are required for the k-th
*        eigenpair.
*
         IF( SOMCON ) THEN
            IF( .NOT.SELECT( K ) )
     $         GO TO 20
         END IF
*
         KS = KS + 1
*
         IF( WANTS ) THEN
*
*           Compute the reciprocal condition number of the k-th
*           eigenvalue.
*
            RNRM = DZNRM2( N, VR( 1, KS ), 1 )
            LNRM = DZNRM2( N, VL( 1, KS ), 1 )
            CALL ZGEMV( 'N', N, N, DCMPLX( ONE, ZERO ), A, LDA,
     $                  VR( 1, KS ), 1, DCMPLX( ZERO, ZERO ), WORK, 1 )
            YHAX = ZDOTC( N, WORK, 1, VL( 1, KS ), 1 )
            CALL ZGEMV( 'N', N, N, DCMPLX( ONE, ZERO ), B, LDB,
     $                  VR( 1, KS ), 1, DCMPLX( ZERO, ZERO ), WORK, 1 )
            YHBX = ZDOTC( N, WORK, 1, VL( 1, KS ), 1 )
            COND = DLAPY2( ABS( YHAX ), ABS( YHBX ) )
            IF( COND.EQ.ZERO ) THEN
               S( KS ) = -ONE
            ELSE
               S( KS ) = COND / ( RNRM*LNRM )
            END IF
         END IF
*
         IF( WANTDF ) THEN
            IF( N.EQ.1 ) THEN
               DIF( KS ) = DLAPY2( ABS( A( 1, 1 ) ), ABS( B( 1, 1 ) ) )
            ELSE
*
*              Estimate the reciprocal condition number of the k-th
*              eigenvectors.
*
*              Copy the matrix (A, B) to the array WORK and move the
*              (k,k)th pair to the (1,1) position.
*
               CALL ZLACPY( 'Full', N, N, A, LDA, WORK, N )
               CALL ZLACPY( 'Full', N, N, B, LDB, WORK( N*N+1 ), N )
               IFST = K
               ILST = 1
*
               CALL ZTGEXC( .FALSE., .FALSE., N, WORK, N, WORK( N*N+1 ),
     $                      N, DUMMY, 1, DUMMY1, 1, IFST, ILST, IERR )
*
               IF( IERR.GT.0 ) THEN
*
*                 Ill-conditioned problem - swap rejected.
*
                  DIF( KS ) = ZERO
               ELSE
*
*                 Reordering successful, solve generalized Sylvester
*                 equation for R and L,
*                            A22 * R - L * A11 = A12
*                            B22 * R - L * B11 = B12,
*                 and compute estimate of Difl[(A11,B11), (A22, B22)].
*
                  N1 = 1
                  N2 = N - N1
                  I = N*N + 1
                  CALL ZTGSYL( 'N', IDIFJB, N2, N1, WORK( N*N1+N1+1 ),
     $                         N, WORK, N, WORK( N1+1 ), N,
     $                         WORK( N*N1+N1+I ), N, WORK( I ), N,
     $                         WORK( N1+I ), N, SCALE, DIF( KS ), DUMMY,
     $                         1, IWORK, IERR )
               END IF
            END IF
         END IF
*
   20 CONTINUE
      WORK( 1 ) = LWMIN
      RETURN
*
*     End of ZTGSNA
*
      END
