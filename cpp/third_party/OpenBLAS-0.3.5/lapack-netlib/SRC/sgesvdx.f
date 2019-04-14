*> \brief <b> SGESVDX computes the singular value decomposition (SVD) for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGESVDX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgesvdx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgesvdx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgesvdx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*     SUBROUTINE SGESVDX( JOBU, JOBVT, RANGE, M, N, A, LDA, VL, VU,
*    $                    IL, IU, NS, S, U, LDU, VT, LDVT, WORK,
*    $                    LWORK, IWORK, INFO )
*
*
*     .. Scalar Arguments ..
*      CHARACTER          JOBU, JOBVT, RANGE
*      INTEGER            IL, INFO, IU, LDA, LDU, LDVT, LWORK, M, N, NS
*      REAL               VL, VU
*     ..
*     .. Array Arguments ..
*     INTEGER            IWORK( * )
*     REAL               A( LDA, * ), S( * ), U( LDU, * ),
*    $                   VT( LDVT, * ), WORK( * )
*     ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>  SGESVDX computes the singular value decomposition (SVD) of a real
*>  M-by-N matrix A, optionally computing the left and/or right singular
*>  vectors. The SVD is written
*>
*>      A = U * SIGMA * transpose(V)
*>
*>  where SIGMA is an M-by-N matrix which is zero except for its
*>  min(m,n) diagonal elements, U is an M-by-M orthogonal matrix, and
*>  V is an N-by-N orthogonal matrix.  The diagonal elements of SIGMA
*>  are the singular values of A; they are real and non-negative, and
*>  are returned in descending order.  The first min(m,n) columns of
*>  U and V are the left and right singular vectors of A.
*>
*>  SGESVDX uses an eigenvalue problem for obtaining the SVD, which
*>  allows for the computation of a subset of singular values and
*>  vectors. See SBDSVDX for details.
*>
*>  Note that the routine returns V**T, not V.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>          Specifies options for computing all or part of the matrix U:
*>          = 'V':  the first min(m,n) columns of U (the left singular
*>                  vectors) or as specified by RANGE are returned in
*>                  the array U;
*>          = 'N':  no columns of U (no left singular vectors) are
*>                  computed.
*> \endverbatim
*>
*> \param[in] JOBVT
*> \verbatim
*>          JOBVT is CHARACTER*1
*>           Specifies options for computing all or part of the matrix
*>           V**T:
*>           = 'V':  the first min(m,n) rows of V**T (the right singular
*>                   vectors) or as specified by RANGE are returned in
*>                   the array VT;
*>           = 'N':  no rows of V**T (no right singular vectors) are
*>                   computed.
*> \endverbatim
*>
*> \param[in] RANGE
*> \verbatim
*>          RANGE is CHARACTER*1
*>          = 'A': all singular values will be found.
*>          = 'V': all singular values in the half-open interval (VL,VU]
*>                 will be found.
*>          = 'I': the IL-th through IU-th singular values will be found.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the input matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the input matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, the contents of A are destroyed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is REAL
*>          If RANGE='V', the lower bound of the interval to
*>          be searched for singular values. VU > VL.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>          VU is REAL
*>          If RANGE='V', the upper bound of the interval to
*>          be searched for singular values. VU > VL.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] IL
*> \verbatim
*>          IL is INTEGER
*>          If RANGE='I', the index of the
*>          smallest singular value to be returned.
*>          1 <= IL <= IU <= min(M,N), if min(M,N) > 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[in] IU
*> \verbatim
*>          IU is INTEGER
*>          If RANGE='I', the index of the
*>          largest singular value to be returned.
*>          1 <= IL <= IU <= min(M,N), if min(M,N) > 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[out] NS
*> \verbatim
*>          NS is INTEGER
*>          The total number of singular values found,
*>          0 <= NS <= min(M,N).
*>          If RANGE = 'A', NS = min(M,N); if RANGE = 'I', NS = IU-IL+1.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (min(M,N))
*>          The singular values of A, sorted so that S(i) >= S(i+1).
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is REAL array, dimension (LDU,UCOL)
*>          If JOBU = 'V', U contains columns of U (the left singular
*>          vectors, stored columnwise) as specified by RANGE; if
*>          JOBU = 'N', U is not referenced.
*>          Note: The user must ensure that UCOL >= NS; if RANGE = 'V',
*>          the exact value of NS is not known in advance and an upper
*>          bound must be used.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U.  LDU >= 1; if
*>          JOBU = 'V', LDU >= M.
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is REAL array, dimension (LDVT,N)
*>          If JOBVT = 'V', VT contains the rows of V**T (the right singular
*>          vectors, stored rowwise) as specified by RANGE; if JOBVT = 'N',
*>          VT is not referenced.
*>          Note: The user must ensure that LDVT >= NS; if RANGE = 'V',
*>          the exact value of NS is not known in advance and an upper
*>          bound must be used.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>          The leading dimension of the array VT.  LDVT >= 1; if
*>          JOBVT = 'V', LDVT >= NS (see above).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK;
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          LWORK >= MAX(1,MIN(M,N)*(MIN(M,N)+4)) for the paths (see
*>          comments inside the code):
*>             - PATH 1  (M much larger than N)
*>             - PATH 1t (N much larger than M)
*>          LWORK >= MAX(1,MIN(M,N)*2+MAX(M,N)) for the other paths.
*>          For good performance, LWORK should generally be larger.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (12*MIN(M,N))
*>          If INFO = 0, the first NS elements of IWORK are zero. If INFO > 0,
*>          then IWORK contains the indices of the eigenvectors that failed
*>          to converge in SBDSVDX/SSTEVX.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>     INFO is INTEGER
*>           = 0:  successful exit
*>           < 0:  if INFO = -i, the i-th argument had an illegal value
*>           > 0:  if INFO = i, then i eigenvectors failed to converge
*>                 in SBDSVDX/SSTEVX.
*>                 if INFO = N*2 + 1, an internal error occurred in
*>                 SBDSVDX
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
*> \ingroup realGEsing
*
*  =====================================================================
      SUBROUTINE SGESVDX( JOBU, JOBVT, RANGE, M, N, A, LDA, VL, VU,
     $                    IL, IU, NS, S, U, LDU, VT, LDVT, WORK,
     $                    LWORK, IWORK, INFO )
*
*  -- LAPACK driver routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBU, JOBVT, RANGE
      INTEGER            IL, INFO, IU, LDA, LDU, LDVT, LWORK, M, N, NS
      REAL               VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               A( LDA, * ), S( * ), U( LDU, * ),
     $                   VT( LDVT, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      CHARACTER          JOBZ, RNGTGK
      LOGICAL            ALLS, INDS, LQUERY, VALS, WANTU, WANTVT
      INTEGER            I, ID, IE, IERR, ILQF, ILTGK, IQRF, ISCL,
     $                   ITAU, ITAUP, ITAUQ, ITEMP, ITGKZ, IUTGK,
     $                   J, MAXWRK, MINMN, MINWRK, MNTHR
      REAL               ABSTOL, ANRM, BIGNUM, EPS, SMLNUM
*     ..
*     .. Local Arrays ..
      REAL               DUM( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SBDSVDX, SGEBRD, SGELQF, SGEQRF, SLACPY,
     $                   SLASCL, SLASET, SORMBR, SORMLQ, SORMQR,
     $                   SCOPY, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           LSAME, ILAENV, SLAMCH, SLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments.
*
      NS = 0
      INFO = 0
      ABSTOL = 2*SLAMCH('S')
      LQUERY = ( LWORK.EQ.-1 )
      MINMN = MIN( M, N )

      WANTU = LSAME( JOBU, 'V' )
      WANTVT = LSAME( JOBVT, 'V' )
      IF( WANTU .OR. WANTVT ) THEN
         JOBZ = 'V'
      ELSE
         JOBZ = 'N'
      END IF
      ALLS = LSAME( RANGE, 'A' )
      VALS = LSAME( RANGE, 'V' )
      INDS = LSAME( RANGE, 'I' )
*
      INFO = 0
      IF( .NOT.LSAME( JOBU, 'V' ) .AND.
     $    .NOT.LSAME( JOBU, 'N' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( JOBVT, 'V' ) .AND.
     $         .NOT.LSAME( JOBVT, 'N' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( ALLS .OR. VALS .OR. INDS ) ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( M.GT.LDA ) THEN
         INFO = -7
      ELSE IF( MINMN.GT.0 ) THEN
         IF( VALS ) THEN
            IF( VL.LT.ZERO ) THEN
               INFO = -8
            ELSE IF( VU.LE.VL ) THEN
               INFO = -9
            END IF
         ELSE IF( INDS ) THEN
            IF( IL.LT.1 .OR. IL.GT.MAX( 1, MINMN ) ) THEN
               INFO = -10
            ELSE IF( IU.LT.MIN( MINMN, IL ) .OR. IU.GT.MINMN ) THEN
               INFO = -11
            END IF
         END IF
         IF( INFO.EQ.0 ) THEN
            IF( WANTU .AND. LDU.LT.M ) THEN
               INFO = -15
            ELSE IF( WANTVT ) THEN
               IF( INDS ) THEN
                   IF( LDVT.LT.IU-IL+1 ) THEN
                       INFO = -17
                   END IF
               ELSE IF( LDVT.LT.MINMN ) THEN
                   INFO = -17
               END IF
            END IF
         END IF
      END IF
*
*     Compute workspace
*     (Note: Comments in the code beginning "Workspace:" describe the
*     minimal amount of workspace needed at that point in the code,
*     as well as the preferred amount for good performance.
*     NB refers to the optimal block size for the immediately
*     following subroutine, as returned by ILAENV.)
*
      IF( INFO.EQ.0 ) THEN
         MINWRK = 1
         MAXWRK = 1
         IF( MINMN.GT.0 ) THEN
            IF( M.GE.N ) THEN
               MNTHR = ILAENV( 6, 'SGESVD', JOBU // JOBVT, M, N, 0, 0 )
               IF( M.GE.MNTHR ) THEN
*
*                 Path 1 (M much larger than N)
*
                  MAXWRK = N +
     $                     N*ILAENV( 1, 'SGEQRF', ' ', M, N, -1, -1 )
                  MAXWRK = MAX( MAXWRK, N*(N+5) + 2*N*
     $                     ILAENV( 1, 'SGEBRD', ' ', N, N, -1, -1 ) )
                  IF (WANTU) THEN
                      MAXWRK = MAX(MAXWRK,N*(N*3+6)+N*
     $                     ILAENV( 1, 'SORMQR', ' ', N, N, -1, -1 ) )
                  END IF
                  IF (WANTVT) THEN
                      MAXWRK = MAX(MAXWRK,N*(N*3+6)+N*
     $                     ILAENV( 1, 'SORMLQ', ' ', N, N, -1, -1 ) )
                  END IF
                  MINWRK = N*(N*3+20)
               ELSE
*
*                 Path 2 (M at least N, but not much larger)
*
                  MAXWRK = 4*N + ( M+N )*
     $                     ILAENV( 1, 'SGEBRD', ' ', M, N, -1, -1 )
                  IF (WANTU) THEN
                      MAXWRK = MAX(MAXWRK,N*(N*2+5)+N*
     $                     ILAENV( 1, 'SORMQR', ' ', N, N, -1, -1 ) )
                  END IF
                  IF (WANTVT) THEN
                      MAXWRK = MAX(MAXWRK,N*(N*2+5)+N*
     $                     ILAENV( 1, 'SORMLQ', ' ', N, N, -1, -1 ) )
                  END IF
                  MINWRK = MAX(N*(N*2+19),4*N+M)
               END IF
            ELSE
               MNTHR = ILAENV( 6, 'SGESVD', JOBU // JOBVT, M, N, 0, 0 )
               IF( N.GE.MNTHR ) THEN
*
*                 Path 1t (N much larger than M)
*
                  MAXWRK = M +
     $                     M*ILAENV( 1, 'SGELQF', ' ', M, N, -1, -1 )
                  MAXWRK = MAX( MAXWRK, M*(M+5) + 2*M*
     $                     ILAENV( 1, 'SGEBRD', ' ', M, M, -1, -1 ) )
                  IF (WANTU) THEN
                      MAXWRK = MAX(MAXWRK,M*(M*3+6)+M*
     $                     ILAENV( 1, 'SORMQR', ' ', M, M, -1, -1 ) )
                  END IF
                  IF (WANTVT) THEN
                      MAXWRK = MAX(MAXWRK,M*(M*3+6)+M*
     $                     ILAENV( 1, 'SORMLQ', ' ', M, M, -1, -1 ) )
                  END IF
                  MINWRK = M*(M*3+20)
               ELSE
*
*                 Path 2t (N at least M, but not much larger)
*
                  MAXWRK = 4*M + ( M+N )*
     $                     ILAENV( 1, 'SGEBRD', ' ', M, N, -1, -1 )
                  IF (WANTU) THEN
                      MAXWRK = MAX(MAXWRK,M*(M*2+5)+M*
     $                     ILAENV( 1, 'SORMQR', ' ', M, M, -1, -1 ) )
                  END IF
                  IF (WANTVT) THEN
                      MAXWRK = MAX(MAXWRK,M*(M*2+5)+M*
     $                     ILAENV( 1, 'SORMLQ', ' ', M, M, -1, -1 ) )
                  END IF
                  MINWRK = MAX(M*(M*2+19),4*M+N)
               END IF
            END IF
         END IF
         MAXWRK = MAX( MAXWRK, MINWRK )
         WORK( 1 ) = REAL( MAXWRK )
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
             INFO = -19
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGESVDX', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 ) THEN
         RETURN
      END IF
*
*     Set singular values indices accord to RANGE.
*
      IF( ALLS ) THEN
         RNGTGK = 'I'
         ILTGK = 1
         IUTGK = MIN( M, N )
      ELSE IF( INDS ) THEN
         RNGTGK = 'I'
         ILTGK = IL
         IUTGK = IU
      ELSE
         RNGTGK = 'V'
         ILTGK = 0
         IUTGK = 0
      END IF
*
*     Get machine constants
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SQRT( SLAMCH( 'S' ) ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = SLANGE( 'M', M, N, A, LDA, DUM )
      ISCL = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         ISCL = 1
         CALL SLASCL( 'G', 0, 0, ANRM, SMLNUM, M, N, A, LDA, INFO )
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         ISCL = 1
         CALL SLASCL( 'G', 0, 0, ANRM, BIGNUM, M, N, A, LDA, INFO )
      END IF
*
      IF( M.GE.N ) THEN
*
*        A has at least as many rows as columns. If A has sufficiently
*        more rows than columns, first reduce A using the QR
*        decomposition.
*
         IF( M.GE.MNTHR ) THEN
*
*           Path 1 (M much larger than N):
*           A = Q * R = Q * ( QB * B * PB**T )
*                     = Q * ( QB * ( UB * S * VB**T ) * PB**T )
*           U = Q * QB * UB; V**T = VB**T * PB**T
*
*           Compute A=Q*R
*           (Workspace: need 2*N, prefer N+N*NB)
*
            ITAU = 1
            ITEMP = ITAU + N
            CALL SGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( ITEMP ),
     $                   LWORK-ITEMP+1, INFO )
*
*           Copy R into WORK and bidiagonalize it:
*           (Workspace: need N*N+5*N, prefer N*N+4*N+2*N*NB)
*
            IQRF = ITEMP
            ID = IQRF + N*N
            IE = ID + N
            ITAUQ = IE + N
            ITAUP = ITAUQ + N
            ITEMP = ITAUP + N
            CALL SLACPY( 'U', N, N, A, LDA, WORK( IQRF ), N )
            CALL SLASET( 'L', N-1, N-1, ZERO, ZERO, WORK( IQRF+1 ), N )
            CALL SGEBRD( N, N, WORK( IQRF ), N, WORK( ID ), WORK( IE ),
     $                   WORK( ITAUQ ), WORK( ITAUP ), WORK( ITEMP ),
     $                   LWORK-ITEMP+1, INFO )
*
*           Solve eigenvalue problem TGK*Z=Z*S.
*           (Workspace: need 14*N + 2*N*(N+1))
*
            ITGKZ = ITEMP
            ITEMP = ITGKZ + N*(N*2+1)
            CALL SBDSVDX( 'U', JOBZ, RNGTGK, N, WORK( ID ), WORK( IE ),
     $                    VL, VU, ILTGK, IUTGK, NS, S, WORK( ITGKZ ),
     $                    N*2, WORK( ITEMP ), IWORK, INFO)
*
*           If needed, compute left singular vectors.
*
            IF( WANTU ) THEN
               J = ITGKZ
               DO I = 1, NS
                  CALL SCOPY( N, WORK( J ), 1, U( 1,I ), 1 )
                  J = J + N*2
               END DO
               CALL SLASET( 'A', M-N, NS, ZERO, ZERO, U( N+1,1 ), LDU )
*
*              Call SORMBR to compute QB*UB.
*              (Workspace in WORK( ITEMP ): need N, prefer N*NB)
*
               CALL SORMBR( 'Q', 'L', 'N', N, NS, N, WORK( IQRF ), N,
     $                      WORK( ITAUQ ), U, LDU, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
*
*              Call SORMQR to compute Q*(QB*UB).
*              (Workspace in WORK( ITEMP ): need N, prefer N*NB)
*
               CALL SORMQR( 'L', 'N', M, NS, N, A, LDA,
     $                      WORK( ITAU ), U, LDU, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
            END IF
*
*           If needed, compute right singular vectors.
*
            IF( WANTVT) THEN
               J = ITGKZ + N
               DO I = 1, NS
                  CALL SCOPY( N, WORK( J ), 1, VT( I,1 ), LDVT )
                  J = J + N*2
               END DO
*
*              Call SORMBR to compute VB**T * PB**T
*              (Workspace in WORK( ITEMP ): need N, prefer N*NB)
*
               CALL SORMBR( 'P', 'R', 'T', NS, N, N, WORK( IQRF ), N,
     $                      WORK( ITAUP ), VT, LDVT, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
            END IF
         ELSE
*
*           Path 2 (M at least N, but not much larger)
*           Reduce A to bidiagonal form without QR decomposition
*           A = QB * B * PB**T = QB * ( UB * S * VB**T ) * PB**T
*           U = QB * UB; V**T = VB**T * PB**T
*
*           Bidiagonalize A
*           (Workspace: need 4*N+M, prefer 4*N+(M+N)*NB)
*
            ID = 1
            IE = ID + N
            ITAUQ = IE + N
            ITAUP = ITAUQ + N
            ITEMP = ITAUP + N
            CALL SGEBRD( M, N, A, LDA, WORK( ID ), WORK( IE ),
     $                   WORK( ITAUQ ), WORK( ITAUP ), WORK( ITEMP ),
     $                   LWORK-ITEMP+1, INFO )
*
*           Solve eigenvalue problem TGK*Z=Z*S.
*           (Workspace: need 14*N + 2*N*(N+1))
*
            ITGKZ = ITEMP
            ITEMP = ITGKZ + N*(N*2+1)
            CALL SBDSVDX( 'U', JOBZ, RNGTGK, N, WORK( ID ), WORK( IE ),
     $                    VL, VU, ILTGK, IUTGK, NS, S, WORK( ITGKZ ),
     $                    N*2, WORK( ITEMP ), IWORK, INFO)
*
*           If needed, compute left singular vectors.
*
            IF( WANTU ) THEN
               J = ITGKZ
               DO I = 1, NS
                  CALL SCOPY( N, WORK( J ), 1, U( 1,I ), 1 )
                  J = J + N*2
               END DO
               CALL SLASET( 'A', M-N, NS, ZERO, ZERO, U( N+1,1 ), LDU )
*
*              Call SORMBR to compute QB*UB.
*              (Workspace in WORK( ITEMP ): need N, prefer N*NB)
*
               CALL SORMBR( 'Q', 'L', 'N', M, NS, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, IERR )
            END IF
*
*           If needed, compute right singular vectors.
*
            IF( WANTVT) THEN
               J = ITGKZ + N
               DO I = 1, NS
                  CALL SCOPY( N, WORK( J ), 1, VT( I,1 ), LDVT )
                  J = J + N*2
               END DO
*
*              Call SORMBR to compute VB**T * PB**T
*              (Workspace in WORK( ITEMP ): need N, prefer N*NB)
*
               CALL SORMBR( 'P', 'R', 'T', NS, N, N, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, IERR )
            END IF
         END IF
      ELSE
*
*        A has more columns than rows. If A has sufficiently more
*        columns than rows, first reduce A using the LQ decomposition.
*
         IF( N.GE.MNTHR ) THEN
*
*           Path 1t (N much larger than M):
*           A = L * Q = ( QB * B * PB**T ) * Q
*                     = ( QB * ( UB * S * VB**T ) * PB**T ) * Q
*           U = QB * UB ; V**T = VB**T * PB**T * Q
*
*           Compute A=L*Q
*           (Workspace: need 2*M, prefer M+M*NB)
*
            ITAU = 1
            ITEMP = ITAU + M
            CALL SGELQF( M, N, A, LDA, WORK( ITAU ), WORK( ITEMP ),
     $                   LWORK-ITEMP+1, INFO )

*           Copy L into WORK and bidiagonalize it:
*           (Workspace in WORK( ITEMP ): need M*M+5*N, prefer M*M+4*M+2*M*NB)
*
            ILQF = ITEMP
            ID = ILQF + M*M
            IE = ID + M
            ITAUQ = IE + M
            ITAUP = ITAUQ + M
            ITEMP = ITAUP + M
            CALL SLACPY( 'L', M, M, A, LDA, WORK( ILQF ), M )
            CALL SLASET( 'U', M-1, M-1, ZERO, ZERO, WORK( ILQF+M ), M )
            CALL SGEBRD( M, M, WORK( ILQF ), M, WORK( ID ), WORK( IE ),
     $                   WORK( ITAUQ ), WORK( ITAUP ), WORK( ITEMP ),
     $                   LWORK-ITEMP+1, INFO )
*
*           Solve eigenvalue problem TGK*Z=Z*S.
*           (Workspace: need 2*M*M+14*M)
*
            ITGKZ = ITEMP
            ITEMP = ITGKZ + M*(M*2+1)
            CALL SBDSVDX( 'U', JOBZ, RNGTGK, M, WORK( ID ), WORK( IE ),
     $                    VL, VU, ILTGK, IUTGK, NS, S, WORK( ITGKZ ),
     $                    M*2, WORK( ITEMP ), IWORK, INFO)
*
*           If needed, compute left singular vectors.
*
            IF( WANTU ) THEN
               J = ITGKZ
               DO I = 1, NS
                  CALL SCOPY( M, WORK( J ), 1, U( 1,I ), 1 )
                  J = J + M*2
               END DO
*
*              Call SORMBR to compute QB*UB.
*              (Workspace in WORK( ITEMP ): need M, prefer M*NB)
*
               CALL SORMBR( 'Q', 'L', 'N', M, NS, M, WORK( ILQF ), M,
     $                      WORK( ITAUQ ), U, LDU, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
            END IF
*
*           If needed, compute right singular vectors.
*
            IF( WANTVT) THEN
               J = ITGKZ + M
               DO I = 1, NS
                  CALL SCOPY( M, WORK( J ), 1, VT( I,1 ), LDVT )
                  J = J + M*2
               END DO
               CALL SLASET( 'A', NS, N-M, ZERO, ZERO, VT( 1,M+1 ), LDVT)
*
*              Call SORMBR to compute (VB**T)*(PB**T)
*              (Workspace in WORK( ITEMP ): need M, prefer M*NB)
*
               CALL SORMBR( 'P', 'R', 'T', NS, M, M, WORK( ILQF ), M,
     $                      WORK( ITAUP ), VT, LDVT, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
*
*              Call SORMLQ to compute ((VB**T)*(PB**T))*Q.
*              (Workspace in WORK( ITEMP ): need M, prefer M*NB)
*
               CALL SORMLQ( 'R', 'N', NS, N, M, A, LDA,
     $                      WORK( ITAU ), VT, LDVT, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
            END IF
         ELSE
*
*           Path 2t (N greater than M, but not much larger)
*           Reduce to bidiagonal form without LQ decomposition
*           A = QB * B * PB**T = QB * ( UB * S * VB**T ) * PB**T
*           U = QB * UB; V**T = VB**T * PB**T
*
*           Bidiagonalize A
*           (Workspace: need 4*M+N, prefer 4*M+(M+N)*NB)
*
            ID = 1
            IE = ID + M
            ITAUQ = IE + M
            ITAUP = ITAUQ + M
            ITEMP = ITAUP + M
            CALL SGEBRD( M, N, A, LDA, WORK( ID ), WORK( IE ),
     $                   WORK( ITAUQ ), WORK( ITAUP ), WORK( ITEMP ),
     $                   LWORK-ITEMP+1, INFO )
*
*           Solve eigenvalue problem TGK*Z=Z*S.
*           (Workspace: need 2*M*M+14*M)
*
            ITGKZ = ITEMP
            ITEMP = ITGKZ + M*(M*2+1)
            CALL SBDSVDX( 'L', JOBZ, RNGTGK, M, WORK( ID ), WORK( IE ),
     $                    VL, VU, ILTGK, IUTGK, NS, S, WORK( ITGKZ ),
     $                    M*2, WORK( ITEMP ), IWORK, INFO)
*
*           If needed, compute left singular vectors.
*
            IF( WANTU ) THEN
               J = ITGKZ
               DO I = 1, NS
                  CALL SCOPY( M, WORK( J ), 1, U( 1,I ), 1 )
                  J = J + M*2
               END DO
*
*              Call SORMBR to compute QB*UB.
*              (Workspace in WORK( ITEMP ): need M, prefer M*NB)
*
               CALL SORMBR( 'Q', 'L', 'N', M, NS, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
            END IF
*
*           If needed, compute right singular vectors.
*
            IF( WANTVT) THEN
               J = ITGKZ + M
               DO I = 1, NS
                  CALL SCOPY( M, WORK( J ), 1, VT( I,1 ), LDVT )
                  J = J + M*2
               END DO
               CALL SLASET( 'A', NS, N-M, ZERO, ZERO, VT( 1,M+1 ), LDVT)
*
*              Call SORMBR to compute VB**T * PB**T
*              (Workspace in WORK( ITEMP ): need M, prefer M*NB)
*
               CALL SORMBR( 'P', 'R', 'T', NS, N, M, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( ITEMP ),
     $                      LWORK-ITEMP+1, INFO )
            END IF
         END IF
      END IF
*
*     Undo scaling if necessary
*
      IF( ISCL.EQ.1 ) THEN
         IF( ANRM.GT.BIGNUM )
     $      CALL SLASCL( 'G', 0, 0, BIGNUM, ANRM, MINMN, 1,
     $                   S, MINMN, INFO )
         IF( ANRM.LT.SMLNUM )
     $      CALL SLASCL( 'G', 0, 0, SMLNUM, ANRM, MINMN, 1,
     $                   S, MINMN, INFO )
      END IF
*
*     Return optimal workspace in WORK(1)
*
      WORK( 1 ) = REAL( MAXWRK )
*
      RETURN
*
*     End of SGESVDX
*
      END
