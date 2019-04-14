*> \brief <b> CGEEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CGEEVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgeevx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgeevx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgeevx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGEEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, W, VL,
*                          LDVL, VR, LDVR, ILO, IHI, SCALE, ABNRM, RCONDE,
*                          RCONDV, WORK, LWORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          BALANC, JOBVL, JOBVR, SENSE
*       INTEGER            IHI, ILO, INFO, LDA, LDVL, LDVR, LWORK, N
*       REAL               ABNRM
*       ..
*       .. Array Arguments ..
*       REAL               RCONDE( * ), RCONDV( * ), RWORK( * ),
*      $                   SCALE( * )
*       COMPLEX            A( LDA, * ), VL( LDVL, * ), VR( LDVR, * ),
*      $                   W( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGEEVX computes for an N-by-N complex nonsymmetric matrix A, the
*> eigenvalues and, optionally, the left and/or right eigenvectors.
*>
*> Optionally also, it computes a balancing transformation to improve
*> the conditioning of the eigenvalues and eigenvectors (ILO, IHI,
*> SCALE, and ABNRM), reciprocal condition numbers for the eigenvalues
*> (RCONDE), and reciprocal condition numbers for the right
*> eigenvectors (RCONDV).
*>
*> The right eigenvector v(j) of A satisfies
*>                  A * v(j) = lambda(j) * v(j)
*> where lambda(j) is its eigenvalue.
*> The left eigenvector u(j) of A satisfies
*>               u(j)**H * A = lambda(j) * u(j)**H
*> where u(j)**H denotes the conjugate transpose of u(j).
*>
*> The computed eigenvectors are normalized to have Euclidean norm
*> equal to 1 and largest component real.
*>
*> Balancing a matrix means permuting the rows and columns to make it
*> more nearly upper triangular, and applying a diagonal similarity
*> transformation D * A * D**(-1), where D is a diagonal matrix, to
*> make its rows and columns closer in norm and the condition numbers
*> of its eigenvalues and eigenvectors smaller.  The computed
*> reciprocal condition numbers correspond to the balanced matrix.
*> Permuting rows and columns will not change the condition numbers
*> (in exact arithmetic) but diagonal scaling will.  For further
*> explanation of balancing, see section 4.10.2 of the LAPACK
*> Users' Guide.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] BALANC
*> \verbatim
*>          BALANC is CHARACTER*1
*>          Indicates how the input matrix should be diagonally scaled
*>          and/or permuted to improve the conditioning of its
*>          eigenvalues.
*>          = 'N': Do not diagonally scale or permute;
*>          = 'P': Perform permutations to make the matrix more nearly
*>                 upper triangular. Do not diagonally scale;
*>          = 'S': Diagonally scale the matrix, ie. replace A by
*>                 D*A*D**(-1), where D is a diagonal matrix chosen
*>                 to make the rows and columns of A more equal in
*>                 norm. Do not permute;
*>          = 'B': Both diagonally scale and permute A.
*>
*>          Computed reciprocal condition numbers will be for the matrix
*>          after balancing and/or permuting. Permuting does not change
*>          condition numbers (in exact arithmetic), but balancing does.
*> \endverbatim
*>
*> \param[in] JOBVL
*> \verbatim
*>          JOBVL is CHARACTER*1
*>          = 'N': left eigenvectors of A are not computed;
*>          = 'V': left eigenvectors of A are computed.
*>          If SENSE = 'E' or 'B', JOBVL must = 'V'.
*> \endverbatim
*>
*> \param[in] JOBVR
*> \verbatim
*>          JOBVR is CHARACTER*1
*>          = 'N': right eigenvectors of A are not computed;
*>          = 'V': right eigenvectors of A are computed.
*>          If SENSE = 'E' or 'B', JOBVR must = 'V'.
*> \endverbatim
*>
*> \param[in] SENSE
*> \verbatim
*>          SENSE is CHARACTER*1
*>          Determines which reciprocal condition numbers are computed.
*>          = 'N': None are computed;
*>          = 'E': Computed for eigenvalues only;
*>          = 'V': Computed for right eigenvectors only;
*>          = 'B': Computed for eigenvalues and right eigenvectors.
*>
*>          If SENSE = 'E' or 'B', both left and right eigenvectors
*>          must also be computed (JOBVL = 'V' and JOBVR = 'V').
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
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the N-by-N matrix A.
*>          On exit, A has been overwritten.  If JOBVL = 'V' or
*>          JOBVR = 'V', A contains the Schur form of the balanced
*>          version of the matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX array, dimension (N)
*>          W contains the computed eigenvalues.
*> \endverbatim
*>
*> \param[out] VL
*> \verbatim
*>          VL is COMPLEX array, dimension (LDVL,N)
*>          If JOBVL = 'V', the left eigenvectors u(j) are stored one
*>          after another in the columns of VL, in the same order
*>          as their eigenvalues.
*>          If JOBVL = 'N', VL is not referenced.
*>          u(j) = VL(:,j), the j-th column of VL.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the array VL.  LDVL >= 1; if
*>          JOBVL = 'V', LDVL >= N.
*> \endverbatim
*>
*> \param[out] VR
*> \verbatim
*>          VR is COMPLEX array, dimension (LDVR,N)
*>          If JOBVR = 'V', the right eigenvectors v(j) are stored one
*>          after another in the columns of VR, in the same order
*>          as their eigenvalues.
*>          If JOBVR = 'N', VR is not referenced.
*>          v(j) = VR(:,j), the j-th column of VR.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR.  LDVR >= 1; if
*>          JOBVR = 'V', LDVR >= N.
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
*>          ILO and IHI are integer values determined when A was
*>          balanced.  The balanced A(i,j) = 0 if I > J and
*>          J = 1,...,ILO-1 or I = IHI+1,...,N.
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is REAL array, dimension (N)
*>          Details of the permutations and scaling factors applied
*>          when balancing A.  If P(j) is the index of the row and column
*>          interchanged with row and column j, and D(j) is the scaling
*>          factor applied to row and column j, then
*>          SCALE(J) = P(J),    for J = 1,...,ILO-1
*>                   = D(J),    for J = ILO,...,IHI
*>                   = P(J)     for J = IHI+1,...,N.
*>          The order in which the interchanges are made is N to IHI+1,
*>          then 1 to ILO-1.
*> \endverbatim
*>
*> \param[out] ABNRM
*> \verbatim
*>          ABNRM is REAL
*>          The one-norm of the balanced matrix (the maximum
*>          of the sum of absolute values of elements of any column).
*> \endverbatim
*>
*> \param[out] RCONDE
*> \verbatim
*>          RCONDE is REAL array, dimension (N)
*>          RCONDE(j) is the reciprocal condition number of the j-th
*>          eigenvalue.
*> \endverbatim
*>
*> \param[out] RCONDV
*> \verbatim
*>          RCONDV is REAL array, dimension (N)
*>          RCONDV(j) is the reciprocal condition number of the j-th
*>          right eigenvector.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.  If SENSE = 'N' or 'E',
*>          LWORK >= max(1,2*N), and if SENSE = 'V' or 'B',
*>          LWORK >= N*N+2*N.
*>          For good performance, LWORK must generally be larger.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if INFO = i, the QR algorithm failed to compute all the
*>                eigenvalues, and no eigenvectors or condition numbers
*>                have been computed; elements 1:ILO-1 and i+1:N of W
*>                contain eigenvalues which have converged.
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
*  @generated from zgeevx.f, fortran z -> c, Tue Apr 19 01:47:44 2016
*
*> \ingroup complexGEeigen
*
*  =====================================================================
      SUBROUTINE CGEEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, W, VL,
     $                   LDVL, VR, LDVR, ILO, IHI, SCALE, ABNRM, RCONDE,
     $                   RCONDV, WORK, LWORK, RWORK, INFO )
      implicit none
*
*  -- LAPACK driver routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          BALANC, JOBVL, JOBVR, SENSE
      INTEGER            IHI, ILO, INFO, LDA, LDVL, LDVR, LWORK, N
      REAL               ABNRM
*     ..
*     .. Array Arguments ..
      REAL               RCONDE( * ), RCONDV( * ), RWORK( * ),
     $                   SCALE( * )
      COMPLEX            A( LDA, * ), VL( LDVL, * ), VR( LDVR, * ),
     $                   W( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, SCALEA, WANTVL, WANTVR, WNTSNB, WNTSNE,
     $                   WNTSNN, WNTSNV
      CHARACTER          JOB, SIDE
      INTEGER            HSWORK, I, ICOND, IERR, ITAU, IWRK, K,
     $                   LWORK_TREVC, MAXWRK, MINWRK, NOUT
      REAL               ANRM, BIGNUM, CSCALE, EPS, SCL, SMLNUM
      COMPLEX            TMP
*     ..
*     .. Local Arrays ..
      LOGICAL            SELECT( 1 )
      REAL   DUM( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLABAD, SLASCL, XERBLA, CSSCAL, CGEBAK, CGEBAL,
     $                   CGEHRD, CHSEQR, CLACPY, CLASCL, CSCAL, CTREVC3,
     $                   CTRSNA, CUNGHR
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ISAMAX, ILAENV
      REAL   SLAMCH, SCNRM2, CLANGE
      EXTERNAL           LSAME, ISAMAX, ILAENV, SLAMCH, SCNRM2, CLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          REAL, CMPLX, CONJG, AIMAG, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      WANTVL = LSAME( JOBVL, 'V' )
      WANTVR = LSAME( JOBVR, 'V' )
      WNTSNN = LSAME( SENSE, 'N' )
      WNTSNE = LSAME( SENSE, 'E' )
      WNTSNV = LSAME( SENSE, 'V' )
      WNTSNB = LSAME( SENSE, 'B' )
      IF( .NOT.( LSAME( BALANC, 'N' ) .OR. LSAME( BALANC, 'S' ) .OR.
     $    LSAME( BALANC, 'P' ) .OR. LSAME( BALANC, 'B' ) ) ) THEN
         INFO = -1
      ELSE IF( ( .NOT.WANTVL ) .AND. ( .NOT.LSAME( JOBVL, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( ( .NOT.WANTVR ) .AND. ( .NOT.LSAME( JOBVR, 'N' ) ) ) THEN
         INFO = -3
      ELSE IF( .NOT.( WNTSNN .OR. WNTSNE .OR. WNTSNB .OR. WNTSNV ) .OR.
     $         ( ( WNTSNE .OR. WNTSNB ) .AND. .NOT.( WANTVL .AND.
     $         WANTVR ) ) ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDVL.LT.1 .OR. ( WANTVL .AND. LDVL.LT.N ) ) THEN
         INFO = -10
      ELSE IF( LDVR.LT.1 .OR. ( WANTVR .AND. LDVR.LT.N ) ) THEN
         INFO = -12
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       CWorkspace refers to complex workspace, and RWorkspace to real
*       workspace. NB refers to the optimal block size for the
*       immediately following subroutine, as returned by ILAENV.
*       HSWORK refers to the workspace preferred by CHSEQR, as
*       calculated below. HSWORK is computed assuming ILO=1 and IHI=N,
*       the worst case.)
*
      IF( INFO.EQ.0 ) THEN
         IF( N.EQ.0 ) THEN
            MINWRK = 1
            MAXWRK = 1
         ELSE
            MAXWRK = N + N*ILAENV( 1, 'CGEHRD', ' ', N, 1, N, 0 )
*
            IF( WANTVL ) THEN
               CALL CTREVC3( 'L', 'B', SELECT, N, A, LDA,
     $                       VL, LDVL, VR, LDVR,
     $                       N, NOUT, WORK, -1, RWORK, -1, IERR )
               LWORK_TREVC = INT( WORK(1) )
               MAXWRK = MAX( MAXWRK, LWORK_TREVC )
               CALL CHSEQR( 'S', 'V', N, 1, N, A, LDA, W, VL, LDVL,
     $                WORK, -1, INFO )
            ELSE IF( WANTVR ) THEN
               CALL CTREVC3( 'R', 'B', SELECT, N, A, LDA,
     $                       VL, LDVL, VR, LDVR,
     $                       N, NOUT, WORK, -1, RWORK, -1, IERR )
               LWORK_TREVC = INT( WORK(1) )
               MAXWRK = MAX( MAXWRK, LWORK_TREVC )
               CALL CHSEQR( 'S', 'V', N, 1, N, A, LDA, W, VR, LDVR,
     $                WORK, -1, INFO )
            ELSE
               IF( WNTSNN ) THEN
                  CALL CHSEQR( 'E', 'N', N, 1, N, A, LDA, W, VR, LDVR,
     $                WORK, -1, INFO )
               ELSE
                  CALL CHSEQR( 'S', 'N', N, 1, N, A, LDA, W, VR, LDVR,
     $                WORK, -1, INFO )
               END IF
            END IF
            HSWORK = INT( WORK(1) )
*
            IF( ( .NOT.WANTVL ) .AND. ( .NOT.WANTVR ) ) THEN
               MINWRK = 2*N
               IF( .NOT.( WNTSNN .OR. WNTSNE ) )
     $            MINWRK = MAX( MINWRK, N*N + 2*N )
               MAXWRK = MAX( MAXWRK, HSWORK )
               IF( .NOT.( WNTSNN .OR. WNTSNE ) )
     $            MAXWRK = MAX( MAXWRK, N*N + 2*N )
            ELSE
               MINWRK = 2*N
               IF( .NOT.( WNTSNN .OR. WNTSNE ) )
     $            MINWRK = MAX( MINWRK, N*N + 2*N )
               MAXWRK = MAX( MAXWRK, HSWORK )
               MAXWRK = MAX( MAXWRK, N + ( N - 1 )*ILAENV( 1, 'CUNGHR',
     $                       ' ', N, 1, N, -1 ) )
               IF( .NOT.( WNTSNN .OR. WNTSNE ) )
     $            MAXWRK = MAX( MAXWRK, N*N + 2*N )
               MAXWRK = MAX( MAXWRK, 2*N )
            END IF
            MAXWRK = MAX( MAXWRK, MINWRK )
         END IF
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -20
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CGEEVX', -INFO )
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
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
      SMLNUM = SQRT( SMLNUM ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ICOND = 0
      ANRM = CLANGE( 'M', N, N, A, LDA, DUM )
      SCALEA = .FALSE.
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         SCALEA = .TRUE.
         CSCALE = SMLNUM
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         SCALEA = .TRUE.
         CSCALE = BIGNUM
      END IF
      IF( SCALEA )
     $   CALL CLASCL( 'G', 0, 0, ANRM, CSCALE, N, N, A, LDA, IERR )
*
*     Balance the matrix and compute ABNRM
*
      CALL CGEBAL( BALANC, N, A, LDA, ILO, IHI, SCALE, IERR )
      ABNRM = CLANGE( '1', N, N, A, LDA, DUM )
      IF( SCALEA ) THEN
         DUM( 1 ) = ABNRM
         CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, 1, 1, DUM, 1, IERR )
         ABNRM = DUM( 1 )
      END IF
*
*     Reduce to upper Hessenberg form
*     (CWorkspace: need 2*N, prefer N+N*NB)
*     (RWorkspace: none)
*
      ITAU = 1
      IWRK = ITAU + N
      CALL CGEHRD( N, ILO, IHI, A, LDA, WORK( ITAU ), WORK( IWRK ),
     $             LWORK-IWRK+1, IERR )
*
      IF( WANTVL ) THEN
*
*        Want left eigenvectors
*        Copy Householder vectors to VL
*
         SIDE = 'L'
         CALL CLACPY( 'L', N, N, A, LDA, VL, LDVL )
*
*        Generate unitary matrix in VL
*        (CWorkspace: need 2*N-1, prefer N+(N-1)*NB)
*        (RWorkspace: none)
*
         CALL CUNGHR( N, ILO, IHI, VL, LDVL, WORK( ITAU ), WORK( IWRK ),
     $                LWORK-IWRK+1, IERR )
*
*        Perform QR iteration, accumulating Schur vectors in VL
*        (CWorkspace: need 1, prefer HSWORK (see comments) )
*        (RWorkspace: none)
*
         IWRK = ITAU
         CALL CHSEQR( 'S', 'V', N, ILO, IHI, A, LDA, W, VL, LDVL,
     $                WORK( IWRK ), LWORK-IWRK+1, INFO )
*
         IF( WANTVR ) THEN
*
*           Want left and right eigenvectors
*           Copy Schur vectors to VR
*
            SIDE = 'B'
            CALL CLACPY( 'F', N, N, VL, LDVL, VR, LDVR )
         END IF
*
      ELSE IF( WANTVR ) THEN
*
*        Want right eigenvectors
*        Copy Householder vectors to VR
*
         SIDE = 'R'
         CALL CLACPY( 'L', N, N, A, LDA, VR, LDVR )
*
*        Generate unitary matrix in VR
*        (CWorkspace: need 2*N-1, prefer N+(N-1)*NB)
*        (RWorkspace: none)
*
         CALL CUNGHR( N, ILO, IHI, VR, LDVR, WORK( ITAU ), WORK( IWRK ),
     $                LWORK-IWRK+1, IERR )
*
*        Perform QR iteration, accumulating Schur vectors in VR
*        (CWorkspace: need 1, prefer HSWORK (see comments) )
*        (RWorkspace: none)
*
         IWRK = ITAU
         CALL CHSEQR( 'S', 'V', N, ILO, IHI, A, LDA, W, VR, LDVR,
     $                WORK( IWRK ), LWORK-IWRK+1, INFO )
*
      ELSE
*
*        Compute eigenvalues only
*        If condition numbers desired, compute Schur form
*
         IF( WNTSNN ) THEN
            JOB = 'E'
         ELSE
            JOB = 'S'
         END IF
*
*        (CWorkspace: need 1, prefer HSWORK (see comments) )
*        (RWorkspace: none)
*
         IWRK = ITAU
         CALL CHSEQR( JOB, 'N', N, ILO, IHI, A, LDA, W, VR, LDVR,
     $                WORK( IWRK ), LWORK-IWRK+1, INFO )
      END IF
*
*     If INFO .NE. 0 from CHSEQR, then quit
*
      IF( INFO.NE.0 )
     $   GO TO 50
*
      IF( WANTVL .OR. WANTVR ) THEN
*
*        Compute left and/or right eigenvectors
*        (CWorkspace: need 2*N, prefer N + 2*N*NB)
*        (RWorkspace: need N)
*
         CALL CTREVC3( SIDE, 'B', SELECT, N, A, LDA, VL, LDVL, VR, LDVR,
     $                 N, NOUT, WORK( IWRK ), LWORK-IWRK+1,
     $                 RWORK, N, IERR )
      END IF
*
*     Compute condition numbers if desired
*     (CWorkspace: need N*N+2*N unless SENSE = 'E')
*     (RWorkspace: need 2*N unless SENSE = 'E')
*
      IF( .NOT.WNTSNN ) THEN
         CALL CTRSNA( SENSE, 'A', SELECT, N, A, LDA, VL, LDVL, VR, LDVR,
     $                RCONDE, RCONDV, N, NOUT, WORK( IWRK ), N, RWORK,
     $                ICOND )
      END IF
*
      IF( WANTVL ) THEN
*
*        Undo balancing of left eigenvectors
*
         CALL CGEBAK( BALANC, 'L', N, ILO, IHI, SCALE, N, VL, LDVL,
     $                IERR )
*
*        Normalize left eigenvectors and make largest component real
*
         DO 20 I = 1, N
            SCL = ONE / SCNRM2( N, VL( 1, I ), 1 )
            CALL CSSCAL( N, SCL, VL( 1, I ), 1 )
            DO 10 K = 1, N
               RWORK( K ) = REAL( VL( K, I ) )**2 +
     $                      AIMAG( VL( K, I ) )**2
   10       CONTINUE
            K = ISAMAX( N, RWORK, 1 )
            TMP = CONJG( VL( K, I ) ) / SQRT( RWORK( K ) )
            CALL CSCAL( N, TMP, VL( 1, I ), 1 )
            VL( K, I ) = CMPLX( REAL( VL( K, I ) ), ZERO )
   20    CONTINUE
      END IF
*
      IF( WANTVR ) THEN
*
*        Undo balancing of right eigenvectors
*
         CALL CGEBAK( BALANC, 'R', N, ILO, IHI, SCALE, N, VR, LDVR,
     $                IERR )
*
*        Normalize right eigenvectors and make largest component real
*
         DO 40 I = 1, N
            SCL = ONE / SCNRM2( N, VR( 1, I ), 1 )
            CALL CSSCAL( N, SCL, VR( 1, I ), 1 )
            DO 30 K = 1, N
               RWORK( K ) = REAL( VR( K, I ) )**2 +
     $                      AIMAG( VR( K, I ) )**2
   30       CONTINUE
            K = ISAMAX( N, RWORK, 1 )
            TMP = CONJG( VR( K, I ) ) / SQRT( RWORK( K ) )
            CALL CSCAL( N, TMP, VR( 1, I ), 1 )
            VR( K, I ) = CMPLX( REAL( VR( K, I ) ), ZERO )
   40    CONTINUE
      END IF
*
*     Undo scaling if necessary
*
   50 CONTINUE
      IF( SCALEA ) THEN
         CALL CLASCL( 'G', 0, 0, CSCALE, ANRM, N-INFO, 1, W( INFO+1 ),
     $                MAX( N-INFO, 1 ), IERR )
         IF( INFO.EQ.0 ) THEN
            IF( ( WNTSNV .OR. WNTSNB ) .AND. ICOND.EQ.0 )
     $         CALL SLASCL( 'G', 0, 0, CSCALE, ANRM, N, 1, RCONDV, N,
     $                      IERR )
         ELSE
            CALL CLASCL( 'G', 0, 0, CSCALE, ANRM, ILO-1, 1, W, N, IERR )
         END IF
      END IF
*
      WORK( 1 ) = MAXWRK
      RETURN
*
*     End of CGEEVX
*
      END
