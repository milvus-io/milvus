*> \brief <b> SGELSD computes the minimum-norm solution to a linear least squares problem for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGELSD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgelsd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgelsd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgelsd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGELSD( M, N, NRHS, A, LDA, B, LDB, S, RCOND,
*                          RANK, WORK, LWORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS, RANK
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       REAL               A( LDA, * ), B( LDB, * ), S( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGELSD computes the minimum-norm solution to a real linear least
*> squares problem:
*>     minimize 2-norm(| b - A*x |)
*> using the singular value decomposition (SVD) of A. A is an M-by-N
*> matrix which may be rank-deficient.
*>
*> Several right hand side vectors b and solution vectors x can be
*> handled in a single call; they are stored as the columns of the
*> M-by-NRHS right hand side matrix B and the N-by-NRHS solution
*> matrix X.
*>
*> The problem is solved in three steps:
*> (1) Reduce the coefficient matrix A to bidiagonal form with
*>     Householder transformations, reducing the original problem
*>     into a "bidiagonal least squares problem" (BLS)
*> (2) Solve the BLS using a divide and conquer approach.
*> (3) Apply back all the Householder transformations to solve
*>     the original least squares problem.
*>
*> The effective rank of A is determined by treating as zero those
*> singular values which are less than RCOND times the largest singular
*> value.
*>
*> The divide and conquer algorithm makes very mild assumptions about
*> floating point arithmetic. It will work on machines with a guard
*> digit in add/subtract, or on those binary machines without guard
*> digits which subtract like the Cray X-MP, Cray Y-MP, Cray C-90, or
*> Cray-2. It could conceivably fail on hexadecimal or decimal machines
*> without guard digits, but we know of none.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of A. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of A. N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrices B and X. NRHS >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, A has been destroyed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB,NRHS)
*>          On entry, the M-by-NRHS right hand side matrix B.
*>          On exit, B is overwritten by the N-by-NRHS solution
*>          matrix X.  If m >= n and RANK = n, the residual
*>          sum-of-squares for the solution in the i-th column is given
*>          by the sum of squares of elements n+1:m in that column.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,max(M,N)).
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (min(M,N))
*>          The singular values of A in decreasing order.
*>          The condition number of A in the 2-norm = S(1)/S(min(m,n)).
*> \endverbatim
*>
*> \param[in] RCOND
*> \verbatim
*>          RCOND is REAL
*>          RCOND is used to determine the effective rank of A.
*>          Singular values S(i) <= RCOND*S(1) are treated as zero.
*>          If RCOND < 0, machine precision is used instead.
*> \endverbatim
*>
*> \param[out] RANK
*> \verbatim
*>          RANK is INTEGER
*>          The effective rank of A, i.e., the number of singular values
*>          which are greater than RCOND*S(1).
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
*>          The dimension of the array WORK. LWORK must be at least 1.
*>          The exact minimum amount of workspace needed depends on M,
*>          N and NRHS. As long as LWORK is at least
*>              12*N + 2*N*SMLSIZ + 8*N*NLVL + N*NRHS + (SMLSIZ+1)**2,
*>          if M is greater than or equal to N or
*>              12*M + 2*M*SMLSIZ + 8*M*NLVL + M*NRHS + (SMLSIZ+1)**2,
*>          if M is less than N, the code will execute correctly.
*>          SMLSIZ is returned by ILAENV and is equal to the maximum
*>          size of the subproblems at the bottom of the computation
*>          tree (usually about 25), and
*>             NLVL = MAX( 0, INT( LOG_2( MIN( M,N )/(SMLSIZ+1) ) ) + 1 )
*>          For good performance, LWORK should generally be larger.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the array WORK and the
*>          minimum size of the array IWORK, and returns these values as
*>          the first entries of the WORK and IWORK arrays, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MAX(1,LIWORK))
*>          LIWORK >= max(1, 3*MINMN*NLVL + 11*MINMN),
*>          where MINMN = MIN( M,N ).
*>          On exit, if INFO = 0, IWORK(1) returns the minimum LIWORK.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  the algorithm for computing the SVD failed to converge;
*>                if INFO = i, i off-diagonal elements of an intermediate
*>                bidiagonal form did not converge to zero.
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
*> \ingroup realGEsolve
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Ren-Cang Li, Computer Science Division, University of
*>       California at Berkeley, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*
*  =====================================================================
      SUBROUTINE SGELSD( M, N, NRHS, A, LDA, B, LDB, S, RCOND,
     $                   RANK, WORK, LWORK, IWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS, RANK
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               A( LDA, * ), B( LDB, * ), S( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0, TWO = 2.0E0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            IASCL, IBSCL, IE, IL, ITAU, ITAUP, ITAUQ,
     $                   LDWORK, LIWORK, MAXMN, MAXWRK, MINMN, MINWRK,
     $                   MM, MNTHR, NLVL, NWORK, SMLSIZ, WLALSD
      REAL               ANRM, BIGNUM, BNRM, EPS, SFMIN, SMLNUM
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEBRD, SGELQF, SGEQRF, SLABAD, SLACPY, SLALSD,
     $                   SLASCL, SLASET, SORMBR, SORMLQ, SORMQR, XERBLA
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           SLAMCH, SLANGE, ILAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          INT, LOG, MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments.
*
      INFO = 0
      MINMN = MIN( M, N )
      MAXMN = MAX( M, N )
      LQUERY = ( LWORK.EQ.-1 )
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, MAXMN ) ) THEN
         INFO = -7
      END IF
*
*     Compute workspace.
*     (Note: Comments in the code beginning "Workspace:" describe the
*     minimal amount of workspace needed at that point in the code,
*     as well as the preferred amount for good performance.
*     NB refers to the optimal block size for the immediately
*     following subroutine, as returned by ILAENV.)
*
      IF( INFO.EQ.0 ) THEN
         MINWRK = 1
         MAXWRK = 1
         LIWORK = 1
         IF( MINMN.GT.0 ) THEN
            SMLSIZ = ILAENV( 9, 'SGELSD', ' ', 0, 0, 0, 0 )
            MNTHR = ILAENV( 6, 'SGELSD', ' ', M, N, NRHS, -1 )
            NLVL = MAX( INT( LOG( REAL( MINMN ) / REAL( SMLSIZ + 1 ) ) /
     $                  LOG( TWO ) ) + 1, 0 )
            LIWORK = 3*MINMN*NLVL + 11*MINMN
            MM = M
            IF( M.GE.N .AND. M.GE.MNTHR ) THEN
*
*              Path 1a - overdetermined, with many more rows than
*                        columns.
*
               MM = N
               MAXWRK = MAX( MAXWRK, N + N*ILAENV( 1, 'SGEQRF', ' ', M,
     $                       N, -1, -1 ) )
               MAXWRK = MAX( MAXWRK, N + NRHS*ILAENV( 1, 'SORMQR', 'LT',
     $                       M, NRHS, N, -1 ) )
            END IF
            IF( M.GE.N ) THEN
*
*              Path 1 - overdetermined or exactly determined.
*
               MAXWRK = MAX( MAXWRK, 3*N + ( MM + N )*ILAENV( 1,
     $                       'SGEBRD', ' ', MM, N, -1, -1 ) )
               MAXWRK = MAX( MAXWRK, 3*N + NRHS*ILAENV( 1, 'SORMBR',
     $                       'QLT', MM, NRHS, N, -1 ) )
               MAXWRK = MAX( MAXWRK, 3*N + ( N - 1 )*ILAENV( 1,
     $                       'SORMBR', 'PLN', N, NRHS, N, -1 ) )
               WLALSD = 9*N + 2*N*SMLSIZ + 8*N*NLVL + N*NRHS +
     $                  ( SMLSIZ + 1 )**2
               MAXWRK = MAX( MAXWRK, 3*N + WLALSD )
               MINWRK = MAX( 3*N + MM, 3*N + NRHS, 3*N + WLALSD )
            END IF
            IF( N.GT.M ) THEN
               WLALSD = 9*M + 2*M*SMLSIZ + 8*M*NLVL + M*NRHS +
     $                  ( SMLSIZ + 1 )**2
               IF( N.GE.MNTHR ) THEN
*
*                 Path 2a - underdetermined, with many more columns
*                           than rows.
*
                  MAXWRK = M + M*ILAENV( 1, 'SGELQF', ' ', M, N, -1,
     $                                  -1 )
                  MAXWRK = MAX( MAXWRK, M*M + 4*M + 2*M*ILAENV( 1,
     $                          'SGEBRD', ' ', M, M, -1, -1 ) )
                  MAXWRK = MAX( MAXWRK, M*M + 4*M + NRHS*ILAENV( 1,
     $                          'SORMBR', 'QLT', M, NRHS, M, -1 ) )
                  MAXWRK = MAX( MAXWRK, M*M + 4*M + ( M - 1 )*ILAENV( 1,
     $                          'SORMBR', 'PLN', M, NRHS, M, -1 ) )
                  IF( NRHS.GT.1 ) THEN
                     MAXWRK = MAX( MAXWRK, M*M + M + M*NRHS )
                  ELSE
                     MAXWRK = MAX( MAXWRK, M*M + 2*M )
                  END IF
                  MAXWRK = MAX( MAXWRK, M + NRHS*ILAENV( 1, 'SORMLQ',
     $                          'LT', N, NRHS, M, -1 ) )
                  MAXWRK = MAX( MAXWRK, M*M + 4*M + WLALSD )
!     XXX: Ensure the Path 2a case below is triggered.  The workspace
!     calculation should use queries for all routines eventually.
                  MAXWRK = MAX( MAXWRK,
     $                 4*M+M*M+MAX( M, 2*M-4, NRHS, N-3*M ) )
               ELSE
*
*                 Path 2 - remaining underdetermined cases.
*
                  MAXWRK = 3*M + ( N + M )*ILAENV( 1, 'SGEBRD', ' ', M,
     $                     N, -1, -1 )
                  MAXWRK = MAX( MAXWRK, 3*M + NRHS*ILAENV( 1, 'SORMBR',
     $                          'QLT', M, NRHS, N, -1 ) )
                  MAXWRK = MAX( MAXWRK, 3*M + M*ILAENV( 1, 'SORMBR',
     $                          'PLN', N, NRHS, M, -1 ) )
                  MAXWRK = MAX( MAXWRK, 3*M + WLALSD )
               END IF
               MINWRK = MAX( 3*M + NRHS, 3*M + M, 3*M + WLALSD )
            END IF
         END IF
         MINWRK = MIN( MINWRK, MAXWRK )
         WORK( 1 ) = MAXWRK
         IWORK( 1 ) = LIWORK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -12
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGELSD', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( M.EQ.0 .OR. N.EQ.0 ) THEN
         RANK = 0
         RETURN
      END IF
*
*     Get machine parameters.
*
      EPS = SLAMCH( 'P' )
      SFMIN = SLAMCH( 'S' )
      SMLNUM = SFMIN / EPS
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
*
*     Scale A if max entry outside range [SMLNUM,BIGNUM].
*
      ANRM = SLANGE( 'M', M, N, A, LDA, WORK )
      IASCL = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM.
*
         CALL SLASCL( 'G', 0, 0, ANRM, SMLNUM, M, N, A, LDA, INFO )
         IASCL = 1
      ELSE IF( ANRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM.
*
         CALL SLASCL( 'G', 0, 0, ANRM, BIGNUM, M, N, A, LDA, INFO )
         IASCL = 2
      ELSE IF( ANRM.EQ.ZERO ) THEN
*
*        Matrix all zero. Return zero solution.
*
         CALL SLASET( 'F', MAX( M, N ), NRHS, ZERO, ZERO, B, LDB )
         CALL SLASET( 'F', MINMN, 1, ZERO, ZERO, S, 1 )
         RANK = 0
         GO TO 10
      END IF
*
*     Scale B if max entry outside range [SMLNUM,BIGNUM].
*
      BNRM = SLANGE( 'M', M, NRHS, B, LDB, WORK )
      IBSCL = 0
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM.
*
         CALL SLASCL( 'G', 0, 0, BNRM, SMLNUM, M, NRHS, B, LDB, INFO )
         IBSCL = 1
      ELSE IF( BNRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM.
*
         CALL SLASCL( 'G', 0, 0, BNRM, BIGNUM, M, NRHS, B, LDB, INFO )
         IBSCL = 2
      END IF
*
*     If M < N make sure certain entries of B are zero.
*
      IF( M.LT.N )
     $   CALL SLASET( 'F', N-M, NRHS, ZERO, ZERO, B( M+1, 1 ), LDB )
*
*     Overdetermined case.
*
      IF( M.GE.N ) THEN
*
*        Path 1 - overdetermined or exactly determined.
*
         MM = M
         IF( M.GE.MNTHR ) THEN
*
*           Path 1a - overdetermined, with many more rows than columns.
*
            MM = N
            ITAU = 1
            NWORK = ITAU + N
*
*           Compute A=Q*R.
*           (Workspace: need 2*N, prefer N+N*NB)
*
            CALL SGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                   LWORK-NWORK+1, INFO )
*
*           Multiply B by transpose(Q).
*           (Workspace: need N+NRHS, prefer N+NRHS*NB)
*
            CALL SORMQR( 'L', 'T', M, NRHS, N, A, LDA, WORK( ITAU ), B,
     $                   LDB, WORK( NWORK ), LWORK-NWORK+1, INFO )
*
*           Zero out below R.
*
            IF( N.GT.1 ) THEN
               CALL SLASET( 'L', N-1, N-1, ZERO, ZERO, A( 2, 1 ), LDA )
            END IF
         END IF
*
         IE = 1
         ITAUQ = IE + N
         ITAUP = ITAUQ + N
         NWORK = ITAUP + N
*
*        Bidiagonalize R in A.
*        (Workspace: need 3*N+MM, prefer 3*N+(MM+N)*NB)
*
         CALL SGEBRD( MM, N, A, LDA, S, WORK( IE ), WORK( ITAUQ ),
     $                WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                INFO )
*
*        Multiply B by transpose of left bidiagonalizing vectors of R.
*        (Workspace: need 3*N+NRHS, prefer 3*N+NRHS*NB)
*
         CALL SORMBR( 'Q', 'L', 'T', MM, NRHS, N, A, LDA, WORK( ITAUQ ),
     $                B, LDB, WORK( NWORK ), LWORK-NWORK+1, INFO )
*
*        Solve the bidiagonal least squares problem.
*
         CALL SLALSD( 'U', SMLSIZ, N, NRHS, S, WORK( IE ), B, LDB,
     $                RCOND, RANK, WORK( NWORK ), IWORK, INFO )
         IF( INFO.NE.0 ) THEN
            GO TO 10
         END IF
*
*        Multiply B by right bidiagonalizing vectors of R.
*
         CALL SORMBR( 'P', 'L', 'N', N, NRHS, N, A, LDA, WORK( ITAUP ),
     $                B, LDB, WORK( NWORK ), LWORK-NWORK+1, INFO )
*
      ELSE IF( N.GE.MNTHR .AND. LWORK.GE.4*M+M*M+
     $         MAX( M, 2*M-4, NRHS, N-3*M, WLALSD ) ) THEN
*
*        Path 2a - underdetermined, with many more columns than rows
*        and sufficient workspace for an efficient algorithm.
*
         LDWORK = M
         IF( LWORK.GE.MAX( 4*M+M*LDA+MAX( M, 2*M-4, NRHS, N-3*M ),
     $       M*LDA+M+M*NRHS, 4*M+M*LDA+WLALSD ) )LDWORK = LDA
         ITAU = 1
         NWORK = M + 1
*
*        Compute A=L*Q.
*        (Workspace: need 2*M, prefer M+M*NB)
*
         CALL SGELQF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                LWORK-NWORK+1, INFO )
         IL = NWORK
*
*        Copy L to WORK(IL), zeroing out above its diagonal.
*
         CALL SLACPY( 'L', M, M, A, LDA, WORK( IL ), LDWORK )
         CALL SLASET( 'U', M-1, M-1, ZERO, ZERO, WORK( IL+LDWORK ),
     $                LDWORK )
         IE = IL + LDWORK*M
         ITAUQ = IE + M
         ITAUP = ITAUQ + M
         NWORK = ITAUP + M
*
*        Bidiagonalize L in WORK(IL).
*        (Workspace: need M*M+5*M, prefer M*M+4*M+2*M*NB)
*
         CALL SGEBRD( M, M, WORK( IL ), LDWORK, S, WORK( IE ),
     $                WORK( ITAUQ ), WORK( ITAUP ), WORK( NWORK ),
     $                LWORK-NWORK+1, INFO )
*
*        Multiply B by transpose of left bidiagonalizing vectors of L.
*        (Workspace: need M*M+4*M+NRHS, prefer M*M+4*M+NRHS*NB)
*
         CALL SORMBR( 'Q', 'L', 'T', M, NRHS, M, WORK( IL ), LDWORK,
     $                WORK( ITAUQ ), B, LDB, WORK( NWORK ),
     $                LWORK-NWORK+1, INFO )
*
*        Solve the bidiagonal least squares problem.
*
         CALL SLALSD( 'U', SMLSIZ, M, NRHS, S, WORK( IE ), B, LDB,
     $                RCOND, RANK, WORK( NWORK ), IWORK, INFO )
         IF( INFO.NE.0 ) THEN
            GO TO 10
         END IF
*
*        Multiply B by right bidiagonalizing vectors of L.
*
         CALL SORMBR( 'P', 'L', 'N', M, NRHS, M, WORK( IL ), LDWORK,
     $                WORK( ITAUP ), B, LDB, WORK( NWORK ),
     $                LWORK-NWORK+1, INFO )
*
*        Zero out below first M rows of B.
*
         CALL SLASET( 'F', N-M, NRHS, ZERO, ZERO, B( M+1, 1 ), LDB )
         NWORK = ITAU + M
*
*        Multiply transpose(Q) by B.
*        (Workspace: need M+NRHS, prefer M+NRHS*NB)
*
         CALL SORMLQ( 'L', 'T', N, NRHS, M, A, LDA, WORK( ITAU ), B,
     $                LDB, WORK( NWORK ), LWORK-NWORK+1, INFO )
*
      ELSE
*
*        Path 2 - remaining underdetermined cases.
*
         IE = 1
         ITAUQ = IE + M
         ITAUP = ITAUQ + M
         NWORK = ITAUP + M
*
*        Bidiagonalize A.
*        (Workspace: need 3*M+N, prefer 3*M+(M+N)*NB)
*
         CALL SGEBRD( M, N, A, LDA, S, WORK( IE ), WORK( ITAUQ ),
     $                WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                INFO )
*
*        Multiply B by transpose of left bidiagonalizing vectors.
*        (Workspace: need 3*M+NRHS, prefer 3*M+NRHS*NB)
*
         CALL SORMBR( 'Q', 'L', 'T', M, NRHS, N, A, LDA, WORK( ITAUQ ),
     $                B, LDB, WORK( NWORK ), LWORK-NWORK+1, INFO )
*
*        Solve the bidiagonal least squares problem.
*
         CALL SLALSD( 'L', SMLSIZ, M, NRHS, S, WORK( IE ), B, LDB,
     $                RCOND, RANK, WORK( NWORK ), IWORK, INFO )
         IF( INFO.NE.0 ) THEN
            GO TO 10
         END IF
*
*        Multiply B by right bidiagonalizing vectors of A.
*
         CALL SORMBR( 'P', 'L', 'N', N, NRHS, M, A, LDA, WORK( ITAUP ),
     $                B, LDB, WORK( NWORK ), LWORK-NWORK+1, INFO )
*
      END IF
*
*     Undo scaling.
*
      IF( IASCL.EQ.1 ) THEN
         CALL SLASCL( 'G', 0, 0, ANRM, SMLNUM, N, NRHS, B, LDB, INFO )
         CALL SLASCL( 'G', 0, 0, SMLNUM, ANRM, MINMN, 1, S, MINMN,
     $                INFO )
      ELSE IF( IASCL.EQ.2 ) THEN
         CALL SLASCL( 'G', 0, 0, ANRM, BIGNUM, N, NRHS, B, LDB, INFO )
         CALL SLASCL( 'G', 0, 0, BIGNUM, ANRM, MINMN, 1, S, MINMN,
     $                INFO )
      END IF
      IF( IBSCL.EQ.1 ) THEN
         CALL SLASCL( 'G', 0, 0, SMLNUM, BNRM, N, NRHS, B, LDB, INFO )
      ELSE IF( IBSCL.EQ.2 ) THEN
         CALL SLASCL( 'G', 0, 0, BIGNUM, BNRM, N, NRHS, B, LDB, INFO )
      END IF
*
   10 CONTINUE
      WORK( 1 ) = MAXWRK
      IWORK( 1 ) = LIWORK
      RETURN
*
*     End of SGELSD
*
      END
