*> \brief <b> SGEESX computes the eigenvalues, the Schur form, and, optionally, the matrix of Schur vectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGEESX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgeesx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgeesx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgeesx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGEESX( JOBVS, SORT, SELECT, SENSE, N, A, LDA, SDIM,
*                          WR, WI, VS, LDVS, RCONDE, RCONDV, WORK, LWORK,
*                          IWORK, LIWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBVS, SENSE, SORT
*       INTEGER            INFO, LDA, LDVS, LIWORK, LWORK, N, SDIM
*       REAL               RCONDE, RCONDV
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       INTEGER            IWORK( * )
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
*> SGEESX computes for an N-by-N real nonsymmetric matrix A, the
*> eigenvalues, the real Schur form T, and, optionally, the matrix of
*> Schur vectors Z.  This gives the Schur factorization A = Z*T*(Z**T).
*>
*> Optionally, it also orders the eigenvalues on the diagonal of the
*> real Schur form so that selected eigenvalues are at the top left;
*> computes a reciprocal condition number for the average of the
*> selected eigenvalues (RCONDE); and computes a reciprocal condition
*> number for the right invariant subspace corresponding to the
*> selected eigenvalues (RCONDV).  The leading columns of Z form an
*> orthonormal basis for this invariant subspace.
*>
*> For further explanation of the reciprocal condition numbers RCONDE
*> and RCONDV, see Section 4.10 of the LAPACK Users' Guide (where
*> these quantities are called s and sep respectively).
*>
*> A real matrix is in real Schur form if it is upper quasi-triangular
*> with 1-by-1 and 2-by-2 blocks. 2-by-2 blocks will be standardized in
*> the form
*>           [  a  b  ]
*>           [  c  a  ]
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
*>          SELECT(WR(j),WI(j)) is true; i.e., if either one of a
*>          complex conjugate pair of eigenvalues is selected, then both
*>          are.  Note that a selected complex eigenvalue may no longer
*>          satisfy SELECT(WR(j),WI(j)) = .TRUE. after ordering, since
*>          ordering may change the value of complex eigenvalues
*>          (especially if the eigenvalue is ill-conditioned); in this
*>          case INFO may be set to N+3 (see INFO below).
*> \endverbatim
*>
*> \param[in] SENSE
*> \verbatim
*>          SENSE is CHARACTER*1
*>          Determines which reciprocal condition numbers are computed.
*>          = 'N': None are computed;
*>          = 'E': Computed for average of selected eigenvalues only;
*>          = 'V': Computed for selected right invariant subspace only;
*>          = 'B': Computed for both.
*>          If SENSE = 'E', 'V' or 'B', SORT must equal 'S'.
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
*>          A is REAL array, dimension (LDA, N)
*>          On entry, the N-by-N matrix A.
*>          On exit, A is overwritten by its real Schur form T.
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
*>          WR and WI contain the real and imaginary parts, respectively,
*>          of the computed eigenvalues, in the same order that they
*>          appear on the diagonal of the output Schur form T.  Complex
*>          conjugate pairs of eigenvalues appear consecutively with the
*>          eigenvalue having the positive imaginary part first.
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
*>          The leading dimension of the array VS.  LDVS >= 1, and if
*>          JOBVS = 'V', LDVS >= N.
*> \endverbatim
*>
*> \param[out] RCONDE
*> \verbatim
*>          RCONDE is REAL
*>          If SENSE = 'E' or 'B', RCONDE contains the reciprocal
*>          condition number for the average of the selected eigenvalues.
*>          Not referenced if SENSE = 'N' or 'V'.
*> \endverbatim
*>
*> \param[out] RCONDV
*> \verbatim
*>          RCONDV is REAL
*>          If SENSE = 'V' or 'B', RCONDV contains the reciprocal
*>          condition number for the selected right invariant subspace.
*>          Not referenced if SENSE = 'N' or 'E'.
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
*>          The dimension of the array WORK.  LWORK >= max(1,3*N).
*>          Also, if SENSE = 'E' or 'V' or 'B',
*>          LWORK >= N+2*SDIM*(N-SDIM), where SDIM is the number of
*>          selected eigenvalues computed by this routine.  Note that
*>          N+2*SDIM*(N-SDIM) <= N+N*N/2. Note also that an error is only
*>          returned if LWORK < max(1,3*N), but if SENSE = 'E' or 'V' or
*>          'B' this may not be large enough.
*>          For good performance, LWORK must generally be larger.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates upper bounds on the optimal sizes of the
*>          arrays WORK and IWORK, returns these values as the first
*>          entries of the WORK and IWORK arrays, and no error messages
*>          related to LWORK or LIWORK are issued by XERBLA.
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
*>          The dimension of the array IWORK.
*>          LIWORK >= 1; if SENSE = 'V' or 'B', LIWORK >= SDIM*(N-SDIM).
*>          Note that SDIM*(N-SDIM) <= N*N/4. Note also that an error is
*>          only returned if LIWORK < 1, but if SENSE = 'V' or 'B' this
*>          may not be large enough.
*>
*>          If LIWORK = -1, then a workspace query is assumed; the
*>          routine only calculates upper bounds on the optimal sizes of
*>          the arrays WORK and IWORK, returns these values as the first
*>          entries of the WORK and IWORK arrays, and no error messages
*>          related to LWORK or LIWORK are issued by XERBLA.
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
*>                   JOBVS = 'V', VS contains the transformation which
*>                   reduces A to its partially converged Schur form.
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
*> \date June 2016
*
*> \ingroup realGEeigen
*
*  =====================================================================
      SUBROUTINE SGEESX( JOBVS, SORT, SELECT, SENSE, N, A, LDA, SDIM,
     $                   WR, WI, VS, LDVS, RCONDE, RCONDV, WORK, LWORK,
     $                   IWORK, LIWORK, BWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBVS, SENSE, SORT
      INTEGER            INFO, LDA, LDVS, LIWORK, LWORK, N, SDIM
      REAL               RCONDE, RCONDV
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      INTEGER            IWORK( * )
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
      LOGICAL            CURSL, LASTSL, LQUERY, LST2SL, SCALEA, WANTSB,
     $                   WANTSE, WANTSN, WANTST, WANTSV, WANTVS
      INTEGER            HSWORK, I, I1, I2, IBAL, ICOND, IERR, IEVAL,
     $                   IHI, ILO, INXT, IP, ITAU, IWRK, LWRK, LIWRK,
     $                   MAXWRK, MINWRK
      REAL               ANRM, BIGNUM, CSCALE, EPS, SMLNUM
*     ..
*     .. Local Arrays ..
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
      WANTVS = LSAME( JOBVS, 'V' )
      WANTST = LSAME( SORT, 'S' )
      WANTSN = LSAME( SENSE, 'N' )
      WANTSE = LSAME( SENSE, 'E' )
      WANTSV = LSAME( SENSE, 'V' )
      WANTSB = LSAME( SENSE, 'B' )
      LQUERY = ( LWORK.EQ.-1 .OR. LIWORK.EQ.-1 )
*
      IF( ( .NOT.WANTVS ) .AND. ( .NOT.LSAME( JOBVS, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( ( .NOT.WANTST ) .AND. ( .NOT.LSAME( SORT, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( WANTSN .OR. WANTSE .OR. WANTSV .OR. WANTSB ) .OR.
     $         ( .NOT.WANTST .AND. .NOT.WANTSN ) ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDVS.LT.1 .OR. ( WANTVS .AND. LDVS.LT.N ) ) THEN
         INFO = -12
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "RWorkspace:" describe the
*       minimal amount of real workspace needed at that point in the
*       code, as well as the preferred amount for good performance.
*       IWorkspace refers to integer workspace.
*       NB refers to the optimal block size for the immediately
*       following subroutine, as returned by ILAENV.
*       HSWORK refers to the workspace preferred by SHSEQR, as
*       calculated below. HSWORK is computed assuming ILO=1 and IHI=N,
*       the worst case.
*       If SENSE = 'E', 'V' or 'B', then the amount of workspace needed
*       depends on SDIM, which is computed by the routine STRSEN later
*       in the code.)
*
      IF( INFO.EQ.0 ) THEN
         LIWRK = 1
         IF( N.EQ.0 ) THEN
            MINWRK = 1
            LWRK = 1
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
            LWRK = MAXWRK
            IF( .NOT.WANTSN )
     $         LWRK = MAX( LWRK, N + ( N*N )/2 )
            IF( WANTSV .OR. WANTSB )
     $         LIWRK = ( N*N )/4
         END IF
         IWORK( 1 ) = LIWRK
         WORK( 1 ) = LWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -16
         ELSE IF( LIWORK.LT.1 .AND. .NOT.LQUERY ) THEN
            INFO = -18
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGEESX', -INFO )
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
*     (RWorkspace: need N)
*
      IBAL = 1
      CALL SGEBAL( 'P', N, A, LDA, ILO, IHI, WORK( IBAL ), IERR )
*
*     Reduce to upper Hessenberg form
*     (RWorkspace: need 3*N, prefer 2*N+N*NB)
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
*        (RWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*
         CALL SORGHR( N, ILO, IHI, VS, LDVS, WORK( ITAU ), WORK( IWRK ),
     $                LWORK-IWRK+1, IERR )
      END IF
*
      SDIM = 0
*
*     Perform QR iteration, accumulating Schur vectors in VS if desired
*     (RWorkspace: need N+1, prefer N+HSWORK (see comments) )
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
*        Reorder eigenvalues, transform Schur vectors, and compute
*        reciprocal condition numbers
*        (RWorkspace: if SENSE is not 'N', need N+2*SDIM*(N-SDIM)
*                     otherwise, need N )
*        (IWorkspace: if SENSE is 'V' or 'B', need SDIM*(N-SDIM)
*                     otherwise, need 0 )
*
         CALL STRSEN( SENSE, JOBVS, BWORK, N, A, LDA, VS, LDVS, WR, WI,
     $                SDIM, RCONDE, RCONDV, WORK( IWRK ), LWORK-IWRK+1,
     $                IWORK, LIWORK, ICOND )
         IF( .NOT.WANTSN )
     $      MAXWRK = MAX( MAXWRK, N+2*SDIM*( N-SDIM ) )
         IF( ICOND.EQ.-15 ) THEN
*
*           Not enough real workspace
*
            INFO = -16
         ELSE IF( ICOND.EQ.-17 ) THEN
*
*           Not enough integer workspace
*
            INFO = -18
         ELSE IF( ICOND.GT.0 ) THEN
*
*           STRSEN failed to reorder or to restore standard Schur form
*
            INFO = ICOND + N
         END IF
      END IF
*
      IF( WANTVS ) THEN
*
*        Undo balancing
*        (RWorkspace: need N)
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
         IF( ( WANTSV .OR. WANTSB ) .AND. INFO.EQ.0 ) THEN
            DUM( 1 ) = RCONDV
            CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, 1, 1, DUM, 1, IERR )
            RCONDV = DUM( 1 )
         END IF
         IF( CSCALE.EQ.SMLNUM ) THEN
*
*           If scaling back towards underflow, adjust WI if an
*           offdiagonal element of a 2-by-2 block in the Schur form
*           underflows.
*
            IF( IEVAL.GT.0 ) THEN
               I1 = IEVAL + 1
               I2 = IHI - 1
               CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, ILO-1, 1, WI, N,
     $                      IERR )
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
                     CALL SSWAP( N, VS( 1, I ), 1, VS( 1, I+1 ), 1 )
                     A( I, I+1 ) = A( I+1, I )
                     A( I+1, I ) = ZERO
                  END IF
                  INXT = I + 2
               END IF
   20       CONTINUE
         END IF
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
      IF( WANTSV .OR. WANTSB ) THEN
         IWORK( 1 ) = SDIM*(N-SDIM)
      ELSE
         IWORK( 1 ) = 1
      END IF
*
      RETURN
*
*     End of SGEESX
*
      END
