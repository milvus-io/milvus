*> \brief <b> SGEES computes the eigenvalues, the Schur form, and, optionally, the matrix of Schur vectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGEES + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgees.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgees.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgees.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGEES( JOBVS, SORT, SELECT, N, A, LDA, SDIM, WR, WI,
*                         VS, LDVS, WORK, LWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBVS, SORT
*       INTEGER            INFO, LDA, LDVS, LWORK, N, SDIM
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       REAL               A( LDA, * ), VS( LDVS, * ), WI( * ), WORK( * ),
*      $                   WR( * )
*       ..
*       .. Function Arguments ..
*       LOGICAL            SELECT
*       EXTERNAL           SELECT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGEES computes for an N-by-N real nonsymmetric matrix A, the
*> eigenvalues, the real Schur form T, and, optionally, the matrix of
*> Schur vectors Z.  This gives the Schur factorization A = Z*T*(Z**T).
*>
*> Optionally, it also orders the eigenvalues on the diagonal of the
*> real Schur form so that selected eigenvalues are at the top left.
*> The leading columns of Z then form an orthonormal basis for the
*> invariant subspace corresponding to the selected eigenvalues.
*>
*> A matrix is in real Schur form if it is upper quasi-triangular with
*> 1-by-1 and 2-by-2 blocks. 2-by-2 blocks will be standardized in the
*> form
*>         [  a  b  ]
*>         [  c  a  ]
*>
*> where b*c < 0. The eigenvalues of such a block are a +- sqrt(bc).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBVS
*> \verbatim
*>          JOBVS is CHARACTER*1
*>          = 'N': Schur vectors are not computed;
*>          = 'V': Schur vectors are computed.
*> \endverbatim
*>
*> \param[in] SORT
*> \verbatim
*>          SORT is CHARACTER*1
*>          Specifies whether or not to order the eigenvalues on the
*>          diagonal of the Schur form.
*>          = 'N': Eigenvalues are not ordered;
*>          = 'S': Eigenvalues are ordered (see SELECT).
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is a LOGICAL FUNCTION of two REAL arguments
*>          SELECT must be declared EXTERNAL in the calling subroutine.
*>          If SORT = 'S', SELECT is used to select eigenvalues to sort
*>          to the top left of the Schur form.
*>          If SORT = 'N', SELECT is not referenced.
*>          An eigenvalue WR(j)+sqrt(-1)*WI(j) is selected if
*>          SELECT(WR(j),WI(j)) is true; i.e., if either one of a complex
*>          conjugate pair of eigenvalues is selected, then both complex
*>          eigenvalues are selected.
*>          Note that a selected complex eigenvalue may no longer
*>          satisfy SELECT(WR(j),WI(j)) = .TRUE. after ordering, since
*>          ordering may change the value of complex eigenvalues
*>          (especially if the eigenvalue is ill-conditioned); in this
*>          case INFO is set to N+2 (see INFO below).
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A. N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the N-by-N matrix A.
*>          On exit, A has been overwritten by its real Schur form T.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] SDIM
*> \verbatim
*>          SDIM is INTEGER
*>          If SORT = 'N', SDIM = 0.
*>          If SORT = 'S', SDIM = number of eigenvalues (after sorting)
*>                         for which SELECT is true. (Complex conjugate
*>                         pairs for which SELECT is true for either
*>                         eigenvalue count as 2.)
*> \endverbatim
*>
*> \param[out] WR
*> \verbatim
*>          WR is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] WI
*> \verbatim
*>          WI is REAL array, dimension (N)
*>          WR and WI contain the real and imaginary parts,
*>          respectively, of the computed eigenvalues in the same order
*>          that they appear on the diagonal of the output Schur form T.
*>          Complex conjugate pairs of eigenvalues will appear
*>          consecutively with the eigenvalue having the positive
*>          imaginary part first.
*> \endverbatim
*>
*> \param[out] VS
*> \verbatim
*>          VS is REAL array, dimension (LDVS,N)
*>          If JOBVS = 'V', VS contains the orthogonal matrix Z of Schur
*>          vectors.
*>          If JOBVS = 'N', VS is not referenced.
*> \endverbatim
*>
*> \param[in] LDVS
*> \verbatim
*>          LDVS is INTEGER
*>          The leading dimension of the array VS.  LDVS >= 1; if
*>          JOBVS = 'V', LDVS >= N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) contains the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.  LWORK >= max(1,3*N).
*>          For good performance, LWORK must generally be larger.
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
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value.
*>          > 0: if INFO = i, and i is
*>             <= N: the QR algorithm failed to compute all the
*>                   eigenvalues; elements 1:ILO-1 and i+1:N of WR and WI
*>                   contain those eigenvalues which have converged; if
*>                   JOBVS = 'V', VS contains the matrix which reduces A
*>                   to its partially converged Schur form.
*>             = N+1: the eigenvalues could not be reordered because some
*>                   eigenvalues were too close to separate (the problem
*>                   is very ill-conditioned);
*>             = N+2: after reordering, roundoff changed values of some
*>                   complex eigenvalues so that leading eigenvalues in
*>                   the Schur form no longer satisfy SELECT=.TRUE.  This
*>                   could also be caused by underflow due to scaling.
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
*> \ingroup realGEeigen
*
*  =====================================================================
      SUBROUTINE SGEES( JOBVS, SORT, SELECT, N, A, LDA, SDIM, WR, WI,
     $                  VS, LDVS, WORK, LWORK, BWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          JOBVS, SORT
      INTEGER            INFO, LDA, LDVS, LWORK, N, SDIM
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      REAL               A( LDA, * ), VS( LDVS, * ), WI( * ), WORK( * ),
     $                   WR( * )
*     ..
*     .. Function Arguments ..
      LOGICAL            SELECT
      EXTERNAL           SELECT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            CURSL, LASTSL, LQUERY, LST2SL, SCALEA, WANTST,
     $                   WANTVS
      INTEGER            HSWORK, I, I1, I2, IBAL, ICOND, IERR, IEVAL,
     $                   IHI, ILO, INXT, IP, ITAU, IWRK, MAXWRK, MINWRK
      REAL               ANRM, BIGNUM, CSCALE, EPS, S, SEP, SMLNUM
*     ..
*     .. Local Arrays ..
      INTEGER            IDUM( 1 )
      REAL               DUM( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEBAK, SGEBAL, SGEHRD, SHSEQR, SLABAD,
     $                   SLACPY, SLASCL, SORGHR, SSWAP, STRSEN, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           LSAME, ILAENV, SLAMCH, SLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      WANTVS = LSAME( JOBVS, 'V' )
      WANTST = LSAME( SORT, 'S' )
      IF( ( .NOT.WANTVS ) .AND. ( .NOT.LSAME( JOBVS, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( ( .NOT.WANTST ) .AND. ( .NOT.LSAME( SORT, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDVS.LT.1 .OR. ( WANTVS .AND. LDVS.LT.N ) ) THEN
         INFO = -11
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       NB refers to the optimal block size for the immediately
*       following subroutine, as returned by ILAENV.
*       HSWORK refers to the workspace preferred by SHSEQR, as
*       calculated below. HSWORK is computed assuming ILO=1 and IHI=N,
*       the worst case.)
*
      IF( INFO.EQ.0 ) THEN
         IF( N.EQ.0 ) THEN
            MINWRK = 1
            MAXWRK = 1
         ELSE
            MAXWRK = 2*N + N*ILAENV( 1, 'SGEHRD', ' ', N, 1, N, 0 )
            MINWRK = 3*N
*
            CALL SHSEQR( 'S', JOBVS, N, 1, N, A, LDA, WR, WI, VS, LDVS,
     $             WORK, -1, IEVAL )
            HSWORK = WORK( 1 )
*
            IF( .NOT.WANTVS ) THEN
               MAXWRK = MAX( MAXWRK, N + HSWORK )
            ELSE
               MAXWRK = MAX( MAXWRK, 2*N + ( N - 1 )*ILAENV( 1,
     $                       'SORGHR', ' ', N, 1, N, -1 ) )
               MAXWRK = MAX( MAXWRK, N + HSWORK )
            END IF
         END IF
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -13
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGEES ', -INFO )
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
      SMLNUM = SLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
      SMLNUM = SQRT( SMLNUM ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = SLANGE( 'M', N, N, A, LDA, DUM )
      SCALEA = .FALSE.
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         SCALEA = .TRUE.
         CSCALE = SMLNUM
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         SCALEA = .TRUE.
         CSCALE = BIGNUM
      END IF
      IF( SCALEA )
     $   CALL SLASCL( 'G', 0, 0, ANRM, CSCALE, N, N, A, LDA, IERR )
*
*     Permute the matrix to make it more nearly triangular
*     (Workspace: need N)
*
      IBAL = 1
      CALL SGEBAL( 'P', N, A, LDA, ILO, IHI, WORK( IBAL ), IERR )
*
*     Reduce to upper Hessenberg form
*     (Workspace: need 3*N, prefer 2*N+N*NB)
*
      ITAU = N + IBAL
      IWRK = N + ITAU
      CALL SGEHRD( N, ILO, IHI, A, LDA, WORK( ITAU ), WORK( IWRK ),
     $             LWORK-IWRK+1, IERR )
*
      IF( WANTVS ) THEN
*
*        Copy Householder vectors to VS
*
         CALL SLACPY( 'L', N, N, A, LDA, VS, LDVS )
*
*        Generate orthogonal matrix in VS
*        (Workspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*
         CALL SORGHR( N, ILO, IHI, VS, LDVS, WORK( ITAU ), WORK( IWRK ),
     $                LWORK-IWRK+1, IERR )
      END IF
*
      SDIM = 0
*
*     Perform QR iteration, accumulating Schur vectors in VS if desired
*     (Workspace: need N+1, prefer N+HSWORK (see comments) )
*
      IWRK = ITAU
      CALL SHSEQR( 'S', JOBVS, N, ILO, IHI, A, LDA, WR, WI, VS, LDVS,
     $             WORK( IWRK ), LWORK-IWRK+1, IEVAL )
      IF( IEVAL.GT.0 )
     $   INFO = IEVAL
*
*     Sort eigenvalues if desired
*
      IF( WANTST .AND. INFO.EQ.0 ) THEN
         IF( SCALEA ) THEN
            CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, N, 1, WR, N, IERR )
            CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, N, 1, WI, N, IERR )
         END IF
         DO 10 I = 1, N
            BWORK( I ) = SELECT( WR( I ), WI( I ) )
   10    CONTINUE
*
*        Reorder eigenvalues and transform Schur vectors
*        (Workspace: none needed)
*
         CALL STRSEN( 'N', JOBVS, BWORK, N, A, LDA, VS, LDVS, WR, WI,
     $                SDIM, S, SEP, WORK( IWRK ), LWORK-IWRK+1, IDUM, 1,
     $                ICOND )
         IF( ICOND.GT.0 )
     $      INFO = N + ICOND
      END IF
*
      IF( WANTVS ) THEN
*
*        Undo balancing
*        (Workspace: need N)
*
         CALL SGEBAK( 'P', 'R', N, ILO, IHI, WORK( IBAL ), N, VS, LDVS,
     $                IERR )
      END IF
*
      IF( SCALEA ) THEN
*
*        Undo scaling for the Schur form of A
*
         CALL SLASCL( 'H', 0, 0, CSCALE, ANRM, N, N, A, LDA, IERR )
         CALL SCOPY( N, A, LDA+1, WR, 1 )
         IF( CSCALE.EQ.SMLNUM ) THEN
*
*           If scaling back towards underflow, adjust WI if an
*           offdiagonal element of a 2-by-2 block in the Schur form
*           underflows.
*
            IF( IEVAL.GT.0 ) THEN
               I1 = IEVAL + 1
               I2 = IHI - 1
               CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, ILO-1, 1, WI,
     $                      MAX( ILO-1, 1 ), IERR )
            ELSE IF( WANTST ) THEN
               I1 = 1
               I2 = N - 1
            ELSE
               I1 = ILO
               I2 = IHI - 1
            END IF
            INXT = I1 - 1
            DO 20 I = I1, I2
               IF( I.LT.INXT )
     $            GO TO 20
               IF( WI( I ).EQ.ZERO ) THEN
                  INXT = I + 1
               ELSE
                  IF( A( I+1, I ).EQ.ZERO ) THEN
                     WI( I ) = ZERO
                     WI( I+1 ) = ZERO
                  ELSE IF( A( I+1, I ).NE.ZERO .AND. A( I, I+1 ).EQ.
     $                     ZERO ) THEN
                     WI( I ) = ZERO
                     WI( I+1 ) = ZERO
                     IF( I.GT.1 )
     $                  CALL SSWAP( I-1, A( 1, I ), 1, A( 1, I+1 ), 1 )
                     IF( N.GT.I+1 )
     $                  CALL SSWAP( N-I-1, A( I, I+2 ), LDA,
     $                              A( I+1, I+2 ), LDA )
                     IF( WANTVS ) THEN
                        CALL SSWAP( N, VS( 1, I ), 1, VS( 1, I+1 ), 1 )
                     END IF
                     A( I, I+1 ) = A( I+1, I )
                     A( I+1, I ) = ZERO
                  END IF
                  INXT = I + 2
               END IF
   20       CONTINUE
         END IF
*
*        Undo scaling for the imaginary part of the eigenvalues
*
         CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, N-IEVAL, 1,
     $                WI( IEVAL+1 ), MAX( N-IEVAL, 1 ), IERR )
      END IF
*
      IF( WANTST .AND. INFO.EQ.0 ) THEN
*
*        Check if reordering successful
*
         LASTSL = .TRUE.
         LST2SL = .TRUE.
         SDIM = 0
         IP = 0
         DO 30 I = 1, N
            CURSL = SELECT( WR( I ), WI( I ) )
            IF( WI( I ).EQ.ZERO ) THEN
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
      END IF
*
      WORK( 1 ) = MAXWRK
      RETURN
*
*     End of SGEES
*
      END
