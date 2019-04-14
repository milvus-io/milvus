*> \brief <b> SGGES computes the eigenvalues, the Schur form, and, optionally, the matrix of Schur vectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGGES + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgges.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgges.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgges.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGGES( JOBVSL, JOBVSR, SORT, SELCTG, N, A, LDA, B, LDB,
*                         SDIM, ALPHAR, ALPHAI, BETA, VSL, LDVSL, VSR,
*                         LDVSR, WORK, LWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBVSL, JOBVSR, SORT
*       INTEGER            INFO, LDA, LDB, LDVSL, LDVSR, LWORK, N, SDIM
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
*      $                   B( LDB, * ), BETA( * ), VSL( LDVSL, * ),
*      $                   VSR( LDVSR, * ), WORK( * )
*       ..
*       .. Function Arguments ..
*       LOGICAL            SELCTG
*       EXTERNAL           SELCTG
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGGES computes for a pair of N-by-N real nonsymmetric matrices (A,B),
*> the generalized eigenvalues, the generalized real Schur form (S,T),
*> optionally, the left and/or right matrices of Schur vectors (VSL and
*> VSR). This gives the generalized Schur factorization
*>
*>          (A,B) = ( (VSL)*S*(VSR)**T, (VSL)*T*(VSR)**T )
*>
*> Optionally, it also orders the eigenvalues so that a selected cluster
*> of eigenvalues appears in the leading diagonal blocks of the upper
*> quasi-triangular matrix S and the upper triangular matrix T.The
*> leading columns of VSL and VSR then form an orthonormal basis for the
*> corresponding left and right eigenspaces (deflating subspaces).
*>
*> (If only the generalized eigenvalues are needed, use the driver
*> SGGEV instead, which is faster.)
*>
*> A generalized eigenvalue for a pair of matrices (A,B) is a scalar w
*> or a ratio alpha/beta = w, such that  A - w*B is singular.  It is
*> usually represented as the pair (alpha,beta), as there is a
*> reasonable interpretation for beta=0 or both being zero.
*>
*> A pair of matrices (S,T) is in generalized real Schur form if T is
*> upper triangular with non-negative diagonal and S is block upper
*> triangular with 1-by-1 and 2-by-2 blocks.  1-by-1 blocks correspond
*> to real generalized eigenvalues, while 2-by-2 blocks of S will be
*> "standardized" by making the corresponding elements of T have the
*> form:
*>         [  a  0  ]
*>         [  0  b  ]
*>
*> and the pair of corresponding 2-by-2 blocks in S and T will have a
*> complex conjugate pair of generalized eigenvalues.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBVSL
*> \verbatim
*>          JOBVSL is CHARACTER*1
*>          = 'N':  do not compute the left Schur vectors;
*>          = 'V':  compute the left Schur vectors.
*> \endverbatim
*>
*> \param[in] JOBVSR
*> \verbatim
*>          JOBVSR is CHARACTER*1
*>          = 'N':  do not compute the right Schur vectors;
*>          = 'V':  compute the right Schur vectors.
*> \endverbatim
*>
*> \param[in] SORT
*> \verbatim
*>          SORT is CHARACTER*1
*>          Specifies whether or not to order the eigenvalues on the
*>          diagonal of the generalized Schur form.
*>          = 'N':  Eigenvalues are not ordered;
*>          = 'S':  Eigenvalues are ordered (see SELCTG);
*> \endverbatim
*>
*> \param[in] SELCTG
*> \verbatim
*>          SELCTG is a LOGICAL FUNCTION of three REAL arguments
*>          SELCTG must be declared EXTERNAL in the calling subroutine.
*>          If SORT = 'N', SELCTG is not referenced.
*>          If SORT = 'S', SELCTG is used to select eigenvalues to sort
*>          to the top left of the Schur form.
*>          An eigenvalue (ALPHAR(j)+ALPHAI(j))/BETA(j) is selected if
*>          SELCTG(ALPHAR(j),ALPHAI(j),BETA(j)) is true; i.e. if either
*>          one of a complex conjugate pair of eigenvalues is selected,
*>          then both complex eigenvalues are selected.
*>
*>          Note that in the ill-conditioned case, a selected complex
*>          eigenvalue may no longer satisfy SELCTG(ALPHAR(j),ALPHAI(j),
*>          BETA(j)) = .TRUE. after ordering. INFO is to be set to N+2
*>          in this case.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A, B, VSL, and VSR.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA, N)
*>          On entry, the first of the pair of matrices.
*>          On exit, A has been overwritten by its generalized Schur
*>          form S.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB, N)
*>          On entry, the second of the pair of matrices.
*>          On exit, B has been overwritten by its generalized Schur
*>          form T.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] SDIM
*> \verbatim
*>          SDIM is INTEGER
*>          If SORT = 'N', SDIM = 0.
*>          If SORT = 'S', SDIM = number of eigenvalues (after sorting)
*>          for which SELCTG is true.  (Complex conjugate pairs for which
*>          SELCTG is true for either eigenvalue count as 2.)
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (N)
*>          On exit, (ALPHAR(j) + ALPHAI(j)*i)/BETA(j), j=1,...,N, will
*>          be the generalized eigenvalues.  ALPHAR(j) + ALPHAI(j)*i,
*>          and  BETA(j),j=1,...,N are the diagonals of the complex Schur
*>          form (S,T) that would result if the 2-by-2 diagonal blocks of
*>          the real Schur form of (A,B) were further reduced to
*>          triangular form using 2-by-2 complex unitary transformations.
*>          If ALPHAI(j) is zero, then the j-th eigenvalue is real; if
*>          positive, then the j-th and (j+1)-st eigenvalues are a
*>          complex conjugate pair, with ALPHAI(j+1) negative.
*>
*>          Note: the quotients ALPHAR(j)/BETA(j) and ALPHAI(j)/BETA(j)
*>          may easily over- or underflow, and BETA(j) may even be zero.
*>          Thus, the user should avoid naively computing the ratio.
*>          However, ALPHAR and ALPHAI will be always less than and
*>          usually comparable with norm(A) in magnitude, and BETA always
*>          less than and usually comparable with norm(B).
*> \endverbatim
*>
*> \param[out] VSL
*> \verbatim
*>          VSL is REAL array, dimension (LDVSL,N)
*>          If JOBVSL = 'V', VSL will contain the left Schur vectors.
*>          Not referenced if JOBVSL = 'N'.
*> \endverbatim
*>
*> \param[in] LDVSL
*> \verbatim
*>          LDVSL is INTEGER
*>          The leading dimension of the matrix VSL. LDVSL >=1, and
*>          if JOBVSL = 'V', LDVSL >= N.
*> \endverbatim
*>
*> \param[out] VSR
*> \verbatim
*>          VSR is REAL array, dimension (LDVSR,N)
*>          If JOBVSR = 'V', VSR will contain the right Schur vectors.
*>          Not referenced if JOBVSR = 'N'.
*> \endverbatim
*>
*> \param[in] LDVSR
*> \verbatim
*>          LDVSR is INTEGER
*>          The leading dimension of the matrix VSR. LDVSR >= 1, and
*>          if JOBVSR = 'V', LDVSR >= N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If N = 0, LWORK >= 1, else LWORK >= max(8*N,6*N+16).
*>          For good performance , LWORK must generally be larger.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] BWORK
*> \verbatim
*>          BWORK is LOGICAL array, dimension (N)
*>          Not referenced if SORT = 'N'.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          = 1,...,N:
*>                The QZ iteration failed.  (A,B) are not in Schur
*>                form, but ALPHAR(j), ALPHAI(j), and BETA(j) should
*>                be correct for j=INFO+1,...,N.
*>          > N:  =N+1: other than QZ iteration failed in SHGEQZ.
*>                =N+2: after reordering, roundoff changed values of
*>                      some complex eigenvalues so that leading
*>                      eigenvalues in the Generalized Schur form no
*>                      longer satisfy SELCTG=.TRUE.  This could also
*>                      be caused due to scaling.
*>                =N+3: reordering failed in STGSEN.
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
*> \ingroup realGEeigen
*
*  =====================================================================
      SUBROUTINE SGGES( JOBVSL, JOBVSR, SORT, SELCTG, N, A, LDA, B, LDB,
     $                  SDIM, ALPHAR, ALPHAI, BETA, VSL, LDVSL, VSR,
     $                  LDVSR, WORK, LWORK, BWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBVSL, JOBVSR, SORT
      INTEGER            INFO, LDA, LDB, LDVSL, LDVSR, LWORK, N, SDIM
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
     $                   B( LDB, * ), BETA( * ), VSL( LDVSL, * ),
     $                   VSR( LDVSR, * ), WORK( * )
*     ..
*     .. Function Arguments ..
      LOGICAL            SELCTG
      EXTERNAL           SELCTG
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            CURSL, ILASCL, ILBSCL, ILVSL, ILVSR, LASTSL,
     $                   LQUERY, LST2SL, WANTST
      INTEGER            I, ICOLS, IERR, IHI, IJOBVL, IJOBVR, ILEFT,
     $                   ILO, IP, IRIGHT, IROWS, ITAU, IWRK, MAXWRK,
     $                   MINWRK
      REAL               ANRM, ANRMTO, BIGNUM, BNRM, BNRMTO, EPS, PVSL,
     $                   PVSR, SAFMAX, SAFMIN, SMLNUM
*     ..
*     .. Local Arrays ..
      INTEGER            IDUM( 1 )
      REAL               DIF( 2 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEQRF, SGGBAK, SGGBAL, SGGHRD, SHGEQZ, SLABAD,
     $                   SLACPY, SLASCL, SLASET, SORGQR, SORMQR, STGSEN,
     $                   XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           LSAME, ILAENV, SLAMCH, SLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Decode the input arguments
*
      IF( LSAME( JOBVSL, 'N' ) ) THEN
         IJOBVL = 1
         ILVSL = .FALSE.
      ELSE IF( LSAME( JOBVSL, 'V' ) ) THEN
         IJOBVL = 2
         ILVSL = .TRUE.
      ELSE
         IJOBVL = -1
         ILVSL = .FALSE.
      END IF
*
      IF( LSAME( JOBVSR, 'N' ) ) THEN
         IJOBVR = 1
         ILVSR = .FALSE.
      ELSE IF( LSAME( JOBVSR, 'V' ) ) THEN
         IJOBVR = 2
         ILVSR = .TRUE.
      ELSE
         IJOBVR = -1
         ILVSR = .FALSE.
      END IF
*
      WANTST = LSAME( SORT, 'S' )
*
*     Test the input arguments
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      IF( IJOBVL.LE.0 ) THEN
         INFO = -1
      ELSE IF( IJOBVR.LE.0 ) THEN
         INFO = -2
      ELSE IF( ( .NOT.WANTST ) .AND. ( .NOT.LSAME( SORT, 'N' ) ) ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LDVSL.LT.1 .OR. ( ILVSL .AND. LDVSL.LT.N ) ) THEN
         INFO = -15
      ELSE IF( LDVSR.LT.1 .OR. ( ILVSR .AND. LDVSR.LT.N ) ) THEN
         INFO = -17
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       NB refers to the optimal block size for the immediately
*       following subroutine, as returned by ILAENV.)
*
      IF( INFO.EQ.0 ) THEN
         IF( N.GT.0 )THEN
            MINWRK = MAX( 8*N, 6*N + 16 )
            MAXWRK = MINWRK - N +
     $               N*ILAENV( 1, 'SGEQRF', ' ', N, 1, N, 0 )
            MAXWRK = MAX( MAXWRK, MINWRK - N +
     $                    N*ILAENV( 1, 'SORMQR', ' ', N, 1, N, -1 ) )
            IF( ILVSL ) THEN
               MAXWRK = MAX( MAXWRK, MINWRK - N +
     $                       N*ILAENV( 1, 'SORGQR', ' ', N, 1, N, -1 ) )
            END IF
         ELSE
            MINWRK = 1
            MAXWRK = 1
         END IF
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY )
     $      INFO = -19
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGGES ', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 ) THEN
         SDIM = 0
         RETURN
      END IF
*
*     Get machine constants
*
      EPS = SLAMCH( 'P' )
      SAFMIN = SLAMCH( 'S' )
      SAFMAX = ONE / SAFMIN
      CALL SLABAD( SAFMIN, SAFMAX )
      SMLNUM = SQRT( SAFMIN ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = SLANGE( 'M', N, N, A, LDA, WORK )
      ILASCL = .FALSE.
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         ANRMTO = SMLNUM
         ILASCL = .TRUE.
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         ANRMTO = BIGNUM
         ILASCL = .TRUE.
      END IF
      IF( ILASCL )
     $   CALL SLASCL( 'G', 0, 0, ANRM, ANRMTO, N, N, A, LDA, IERR )
*
*     Scale B if max element outside range [SMLNUM,BIGNUM]
*
      BNRM = SLANGE( 'M', N, N, B, LDB, WORK )
      ILBSCL = .FALSE.
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
         BNRMTO = SMLNUM
         ILBSCL = .TRUE.
      ELSE IF( BNRM.GT.BIGNUM ) THEN
         BNRMTO = BIGNUM
         ILBSCL = .TRUE.
      END IF
      IF( ILBSCL )
     $   CALL SLASCL( 'G', 0, 0, BNRM, BNRMTO, N, N, B, LDB, IERR )
*
*     Permute the matrix to make it more nearly triangular
*     (Workspace: need 6*N + 2*N space for storing balancing factors)
*
      ILEFT = 1
      IRIGHT = N + 1
      IWRK = IRIGHT + N
      CALL SGGBAL( 'P', N, A, LDA, B, LDB, ILO, IHI, WORK( ILEFT ),
     $             WORK( IRIGHT ), WORK( IWRK ), IERR )
*
*     Reduce B to triangular form (QR decomposition of B)
*     (Workspace: need N, prefer N*NB)
*
      IROWS = IHI + 1 - ILO
      ICOLS = N + 1 - ILO
      ITAU = IWRK
      IWRK = ITAU + IROWS
      CALL SGEQRF( IROWS, ICOLS, B( ILO, ILO ), LDB, WORK( ITAU ),
     $             WORK( IWRK ), LWORK+1-IWRK, IERR )
*
*     Apply the orthogonal transformation to matrix A
*     (Workspace: need N, prefer N*NB)
*
      CALL SORMQR( 'L', 'T', IROWS, ICOLS, IROWS, B( ILO, ILO ), LDB,
     $             WORK( ITAU ), A( ILO, ILO ), LDA, WORK( IWRK ),
     $             LWORK+1-IWRK, IERR )
*
*     Initialize VSL
*     (Workspace: need N, prefer N*NB)
*
      IF( ILVSL ) THEN
         CALL SLASET( 'Full', N, N, ZERO, ONE, VSL, LDVSL )
         IF( IROWS.GT.1 ) THEN
            CALL SLACPY( 'L', IROWS-1, IROWS-1, B( ILO+1, ILO ), LDB,
     $                   VSL( ILO+1, ILO ), LDVSL )
         END IF
         CALL SORGQR( IROWS, IROWS, IROWS, VSL( ILO, ILO ), LDVSL,
     $                WORK( ITAU ), WORK( IWRK ), LWORK+1-IWRK, IERR )
      END IF
*
*     Initialize VSR
*
      IF( ILVSR )
     $   CALL SLASET( 'Full', N, N, ZERO, ONE, VSR, LDVSR )
*
*     Reduce to generalized Hessenberg form
*     (Workspace: none needed)
*
      CALL SGGHRD( JOBVSL, JOBVSR, N, ILO, IHI, A, LDA, B, LDB, VSL,
     $             LDVSL, VSR, LDVSR, IERR )
*
*     Perform QZ algorithm, computing Schur vectors if desired
*     (Workspace: need N)
*
      IWRK = ITAU
      CALL SHGEQZ( 'S', JOBVSL, JOBVSR, N, ILO, IHI, A, LDA, B, LDB,
     $             ALPHAR, ALPHAI, BETA, VSL, LDVSL, VSR, LDVSR,
     $             WORK( IWRK ), LWORK+1-IWRK, IERR )
      IF( IERR.NE.0 ) THEN
         IF( IERR.GT.0 .AND. IERR.LE.N ) THEN
            INFO = IERR
         ELSE IF( IERR.GT.N .AND. IERR.LE.2*N ) THEN
            INFO = IERR - N
         ELSE
            INFO = N + 1
         END IF
         GO TO 40
      END IF
*
*     Sort eigenvalues ALPHA/BETA if desired
*     (Workspace: need 4*N+16 )
*
      SDIM = 0
      IF( WANTST ) THEN
*
*        Undo scaling on eigenvalues before SELCTGing
*
         IF( ILASCL ) THEN
            CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHAR, N,
     $                   IERR )
            CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHAI, N,
     $                   IERR )
         END IF
         IF( ILBSCL )
     $      CALL SLASCL( 'G', 0, 0, BNRMTO, BNRM, N, 1, BETA, N, IERR )
*
*        Select eigenvalues
*
         DO 10 I = 1, N
            BWORK( I ) = SELCTG( ALPHAR( I ), ALPHAI( I ), BETA( I ) )
   10    CONTINUE
*
         CALL STGSEN( 0, ILVSL, ILVSR, BWORK, N, A, LDA, B, LDB, ALPHAR,
     $                ALPHAI, BETA, VSL, LDVSL, VSR, LDVSR, SDIM, PVSL,
     $                PVSR, DIF, WORK( IWRK ), LWORK-IWRK+1, IDUM, 1,
     $                IERR )
         IF( IERR.EQ.1 )
     $      INFO = N + 3
*
      END IF
*
*     Apply back-permutation to VSL and VSR
*     (Workspace: none needed)
*
      IF( ILVSL )
     $   CALL SGGBAK( 'P', 'L', N, ILO, IHI, WORK( ILEFT ),
     $                WORK( IRIGHT ), N, VSL, LDVSL, IERR )
*
      IF( ILVSR )
     $   CALL SGGBAK( 'P', 'R', N, ILO, IHI, WORK( ILEFT ),
     $                WORK( IRIGHT ), N, VSR, LDVSR, IERR )
*
*     Check if unscaling would cause over/underflow, if so, rescale
*     (ALPHAR(I),ALPHAI(I),BETA(I)) so BETA(I) is on the order of
*     B(I,I) and ALPHAR(I) and ALPHAI(I) are on the order of A(I,I)
*
      IF( ILASCL )THEN
         DO 50 I = 1, N
            IF( ALPHAI( I ).NE.ZERO ) THEN
               IF( ( ALPHAR( I )/SAFMAX ).GT.( ANRMTO/ANRM ) .OR.
     $             ( SAFMIN/ALPHAR( I ) ).GT.( ANRM/ANRMTO ) ) THEN
                  WORK( 1 ) = ABS( A( I, I )/ALPHAR( I ) )
                  BETA( I ) = BETA( I )*WORK( 1 )
                  ALPHAR( I ) = ALPHAR( I )*WORK( 1 )
                  ALPHAI( I ) = ALPHAI( I )*WORK( 1 )
               ELSE IF( ( ALPHAI( I )/SAFMAX ).GT.( ANRMTO/ANRM ) .OR.
     $             ( SAFMIN/ALPHAI( I ) ).GT.( ANRM/ANRMTO ) ) THEN
                  WORK( 1 ) = ABS( A( I, I+1 )/ALPHAI( I ) )
                  BETA( I ) = BETA( I )*WORK( 1 )
                  ALPHAR( I ) = ALPHAR( I )*WORK( 1 )
                  ALPHAI( I ) = ALPHAI( I )*WORK( 1 )
               END IF
            END IF
   50    CONTINUE
      END IF
*
      IF( ILBSCL )THEN
         DO 60 I = 1, N
            IF( ALPHAI( I ).NE.ZERO ) THEN
                IF( ( BETA( I )/SAFMAX ).GT.( BNRMTO/BNRM ) .OR.
     $              ( SAFMIN/BETA( I ) ).GT.( BNRM/BNRMTO ) ) THEN
                   WORK( 1 ) = ABS(B( I, I )/BETA( I ))
                   BETA( I ) = BETA( I )*WORK( 1 )
                   ALPHAR( I ) = ALPHAR( I )*WORK( 1 )
                   ALPHAI( I ) = ALPHAI( I )*WORK( 1 )
                END IF
             END IF
   60    CONTINUE
      END IF
*
*     Undo scaling
*
      IF( ILASCL ) THEN
         CALL SLASCL( 'H', 0, 0, ANRMTO, ANRM, N, N, A, LDA, IERR )
         CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHAR, N, IERR )
         CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHAI, N, IERR )
      END IF
*
      IF( ILBSCL ) THEN
         CALL SLASCL( 'U', 0, 0, BNRMTO, BNRM, N, N, B, LDB, IERR )
         CALL SLASCL( 'G', 0, 0, BNRMTO, BNRM, N, 1, BETA, N, IERR )
      END IF
*
      IF( WANTST ) THEN
*
*        Check if reordering is correct
*
         LASTSL = .TRUE.
         LST2SL = .TRUE.
         SDIM = 0
         IP = 0
         DO 30 I = 1, N
            CURSL = SELCTG( ALPHAR( I ), ALPHAI( I ), BETA( I ) )
            IF( ALPHAI( I ).EQ.ZERO ) THEN
               IF( CURSL )
     $            SDIM = SDIM + 1
               IP = 0
               IF( CURSL .AND. .NOT.LASTSL )
     $            INFO = N + 2
            ELSE
               IF( IP.EQ.1 ) THEN
*
*                 Last eigenvalue of conjugate pair
*
                  CURSL = CURSL .OR. LASTSL
                  LASTSL = CURSL
                  IF( CURSL )
     $               SDIM = SDIM + 2
                  IP = -1
                  IF( CURSL .AND. .NOT.LST2SL )
     $               INFO = N + 2
               ELSE
*
*                 First eigenvalue of conjugate pair
*
                  IP = 1
               END IF
            END IF
            LST2SL = LASTSL
            LASTSL = CURSL
   30    CONTINUE
*
      END IF
*
   40 CONTINUE
*
      WORK( 1 ) = MAXWRK
*
      RETURN
*
*     End of SGGES
*
      END
