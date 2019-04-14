*> \brief <b> ZGELSS solves overdetermined or underdetermined systems for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGELSS + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgelss.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgelss.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgelss.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGELSS( M, N, NRHS, A, LDA, B, LDB, S, RCOND, RANK,
*                          WORK, LWORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS, RANK
*       DOUBLE PRECISION   RCOND
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RWORK( * ), S( * )
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGELSS computes the minimum norm solution to a complex linear
*> least squares problem:
*>
*> Minimize 2-norm(| b - A*x |).
*>
*> using the singular value decomposition (SVD) of A. A is an M-by-N
*> matrix which may be rank-deficient.
*>
*> Several right hand side vectors b and solution vectors x can be
*> handled in a single call; they are stored as the columns of the
*> M-by-NRHS right hand side matrix B and the N-by-NRHS solution matrix
*> X.
*>
*> The effective rank of A is determined by treating as zero those
*> singular values which are less than RCOND times the largest singular
*> value.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A. N >= 0.
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
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, the first min(m,n) rows of A are overwritten with
*>          its right singular vectors, stored rowwise.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,NRHS)
*>          On entry, the M-by-NRHS right hand side matrix B.
*>          On exit, B is overwritten by the N-by-NRHS solution matrix X.
*>          If m >= n and RANK = n, the residual sum-of-squares for
*>          the solution in the i-th column is given by the sum of
*>          squares of the modulus of elements n+1:m in that column.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,M,N).
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (min(M,N))
*>          The singular values of A in decreasing order.
*>          The condition number of A in the 2-norm = S(1)/S(min(m,n)).
*> \endverbatim
*>
*> \param[in] RCOND
*> \verbatim
*>          RCOND is DOUBLE PRECISION
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
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= 1, and also:
*>          LWORK >=  2*min(M,N) + max(M,N,NRHS)
*>          For good performance, LWORK should generally be larger.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (5*min(M,N))
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
*> \date June 2016
*
*> \ingroup complex16GEsolve
*
*  =====================================================================
      SUBROUTINE ZGELSS( M, N, NRHS, A, LDA, B, LDB, S, RCOND, RANK,
     $                   WORK, LWORK, RWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS, RANK
      DOUBLE PRECISION   RCOND
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RWORK( * ), S( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            BL, CHUNK, I, IASCL, IBSCL, IE, IL, IRWORK,
     $                   ITAU, ITAUP, ITAUQ, IWORK, LDWORK, MAXMN,
     $                   MAXWRK, MINMN, MINWRK, MM, MNTHR
      INTEGER            LWORK_ZGEQRF, LWORK_ZUNMQR, LWORK_ZGEBRD,
     $                   LWORK_ZUNMBR, LWORK_ZUNGBR, LWORK_ZUNMLQ,
     $                   LWORK_ZGELQF
      DOUBLE PRECISION   ANRM, BIGNUM, BNRM, EPS, SFMIN, SMLNUM, THR
*     ..
*     .. Local Arrays ..
      COMPLEX*16         DUM( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLASCL, DLASET, XERBLA, ZBDSQR, ZCOPY,
     $                   ZDRSCL, ZGEBRD, ZGELQF, ZGEMM, ZGEMV, ZGEQRF,
     $                   ZLACPY, ZLASCL, ZLASET, ZUNGBR, ZUNMBR, ZUNMLQ,
     $                   ZUNMQR
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           ILAENV, DLAMCH, ZLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
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
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       CWorkspace refers to complex workspace, and RWorkspace refers
*       to real workspace. NB refers to the optimal block size for the
*       immediately following subroutine, as returned by ILAENV.)
*
      IF( INFO.EQ.0 ) THEN
         MINWRK = 1
         MAXWRK = 1
         IF( MINMN.GT.0 ) THEN
            MM = M
            MNTHR = ILAENV( 6, 'ZGELSS', ' ', M, N, NRHS, -1 )
            IF( M.GE.N .AND. M.GE.MNTHR ) THEN
*
*              Path 1a - overdetermined, with many more rows than
*                        columns
*
*              Compute space needed for ZGEQRF
               CALL ZGEQRF( M, N, A, LDA, DUM(1), DUM(1), -1, INFO )
               LWORK_ZGEQRF=DUM(1)
*              Compute space needed for ZUNMQR
               CALL ZUNMQR( 'L', 'C', M, NRHS, N, A, LDA, DUM(1), B,
     $                   LDB, DUM(1), -1, INFO )
               LWORK_ZUNMQR=DUM(1)
               MM = N
               MAXWRK = MAX( MAXWRK, N + N*ILAENV( 1, 'ZGEQRF', ' ', M,
     $                       N, -1, -1 ) )
               MAXWRK = MAX( MAXWRK, N + NRHS*ILAENV( 1, 'ZUNMQR', 'LC',
     $                       M, NRHS, N, -1 ) )
            END IF
            IF( M.GE.N ) THEN
*
*              Path 1 - overdetermined or exactly determined
*
*              Compute space needed for ZGEBRD
               CALL ZGEBRD( MM, N, A, LDA, S, S, DUM(1), DUM(1), DUM(1),
     $                      -1, INFO )
               LWORK_ZGEBRD=DUM(1)
*              Compute space needed for ZUNMBR
               CALL ZUNMBR( 'Q', 'L', 'C', MM, NRHS, N, A, LDA, DUM(1),
     $                B, LDB, DUM(1), -1, INFO )
               LWORK_ZUNMBR=DUM(1)
*              Compute space needed for ZUNGBR
               CALL ZUNGBR( 'P', N, N, N, A, LDA, DUM(1),
     $                   DUM(1), -1, INFO )
               LWORK_ZUNGBR=DUM(1)
*              Compute total workspace needed
               MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZGEBRD )
               MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR )
               MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR )
               MAXWRK = MAX( MAXWRK, N*NRHS )
               MINWRK = 2*N + MAX( NRHS, M )
            END IF
            IF( N.GT.M ) THEN
               MINWRK = 2*M + MAX( NRHS, N )
               IF( N.GE.MNTHR ) THEN
*
*                 Path 2a - underdetermined, with many more columns
*                 than rows
*
*                 Compute space needed for ZGELQF
                  CALL ZGELQF( M, N, A, LDA, DUM(1), DUM(1),
     $                -1, INFO )
                  LWORK_ZGELQF=DUM(1)
*                 Compute space needed for ZGEBRD
                  CALL ZGEBRD( M, M, A, LDA, S, S, DUM(1), DUM(1),
     $                         DUM(1), -1, INFO )
                  LWORK_ZGEBRD=DUM(1)
*                 Compute space needed for ZUNMBR
                  CALL ZUNMBR( 'Q', 'L', 'C', M, NRHS, N, A, LDA,
     $                DUM(1), B, LDB, DUM(1), -1, INFO )
                  LWORK_ZUNMBR=DUM(1)
*                 Compute space needed for ZUNGBR
                  CALL ZUNGBR( 'P', M, M, M, A, LDA, DUM(1),
     $                   DUM(1), -1, INFO )
                  LWORK_ZUNGBR=DUM(1)
*                 Compute space needed for ZUNMLQ
                  CALL ZUNMLQ( 'L', 'C', N, NRHS, M, A, LDA, DUM(1),
     $                 B, LDB, DUM(1), -1, INFO )
                  LWORK_ZUNMLQ=DUM(1)
*                 Compute total workspace needed
                  MAXWRK = M + LWORK_ZGELQF
                  MAXWRK = MAX( MAXWRK, 3*M + M*M + LWORK_ZGEBRD )
                  MAXWRK = MAX( MAXWRK, 3*M + M*M + LWORK_ZUNMBR )
                  MAXWRK = MAX( MAXWRK, 3*M + M*M + LWORK_ZUNGBR )
                  IF( NRHS.GT.1 ) THEN
                     MAXWRK = MAX( MAXWRK, M*M + M + M*NRHS )
                  ELSE
                     MAXWRK = MAX( MAXWRK, M*M + 2*M )
                  END IF
                  MAXWRK = MAX( MAXWRK, M + LWORK_ZUNMLQ )
               ELSE
*
*                 Path 2 - underdetermined
*
*                 Compute space needed for ZGEBRD
                  CALL ZGEBRD( M, N, A, LDA, S, S, DUM(1), DUM(1),
     $                         DUM(1), -1, INFO )
                  LWORK_ZGEBRD=DUM(1)
*                 Compute space needed for ZUNMBR
                  CALL ZUNMBR( 'Q', 'L', 'C', M, NRHS, M, A, LDA,
     $                DUM(1), B, LDB, DUM(1), -1, INFO )
                  LWORK_ZUNMBR=DUM(1)
*                 Compute space needed for ZUNGBR
                  CALL ZUNGBR( 'P', M, N, M, A, LDA, DUM(1),
     $                   DUM(1), -1, INFO )
                  LWORK_ZUNGBR=DUM(1)
                  MAXWRK = 2*M + LWORK_ZGEBRD
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR )
                  MAXWRK = MAX( MAXWRK, N*NRHS )
               END IF
            END IF
            MAXWRK = MAX( MINWRK, MAXWRK )
         END IF
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY )
     $      INFO = -12
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGELSS', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 ) THEN
         RANK = 0
         RETURN
      END IF
*
*     Get machine parameters
*
      EPS = DLAMCH( 'P' )
      SFMIN = DLAMCH( 'S' )
      SMLNUM = SFMIN / EPS
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = ZLANGE( 'M', M, N, A, LDA, RWORK )
      IASCL = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM
*
         CALL ZLASCL( 'G', 0, 0, ANRM, SMLNUM, M, N, A, LDA, INFO )
         IASCL = 1
      ELSE IF( ANRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM
*
         CALL ZLASCL( 'G', 0, 0, ANRM, BIGNUM, M, N, A, LDA, INFO )
         IASCL = 2
      ELSE IF( ANRM.EQ.ZERO ) THEN
*
*        Matrix all zero. Return zero solution.
*
         CALL ZLASET( 'F', MAX( M, N ), NRHS, CZERO, CZERO, B, LDB )
         CALL DLASET( 'F', MINMN, 1, ZERO, ZERO, S, MINMN )
         RANK = 0
         GO TO 70
      END IF
*
*     Scale B if max element outside range [SMLNUM,BIGNUM]
*
      BNRM = ZLANGE( 'M', M, NRHS, B, LDB, RWORK )
      IBSCL = 0
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM
*
         CALL ZLASCL( 'G', 0, 0, BNRM, SMLNUM, M, NRHS, B, LDB, INFO )
         IBSCL = 1
      ELSE IF( BNRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM
*
         CALL ZLASCL( 'G', 0, 0, BNRM, BIGNUM, M, NRHS, B, LDB, INFO )
         IBSCL = 2
      END IF
*
*     Overdetermined case
*
      IF( M.GE.N ) THEN
*
*        Path 1 - overdetermined or exactly determined
*
         MM = M
         IF( M.GE.MNTHR ) THEN
*
*           Path 1a - overdetermined, with many more rows than columns
*
            MM = N
            ITAU = 1
            IWORK = ITAU + N
*
*           Compute A=Q*R
*           (CWorkspace: need 2*N, prefer N+N*NB)
*           (RWorkspace: none)
*
            CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( IWORK ),
     $                   LWORK-IWORK+1, INFO )
*
*           Multiply B by transpose(Q)
*           (CWorkspace: need N+NRHS, prefer N+NRHS*NB)
*           (RWorkspace: none)
*
            CALL ZUNMQR( 'L', 'C', M, NRHS, N, A, LDA, WORK( ITAU ), B,
     $                   LDB, WORK( IWORK ), LWORK-IWORK+1, INFO )
*
*           Zero out below R
*
            IF( N.GT.1 )
     $         CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO, A( 2, 1 ),
     $                      LDA )
         END IF
*
         IE = 1
         ITAUQ = 1
         ITAUP = ITAUQ + N
         IWORK = ITAUP + N
*
*        Bidiagonalize R in A
*        (CWorkspace: need 2*N+MM, prefer 2*N+(MM+N)*NB)
*        (RWorkspace: need N)
*
         CALL ZGEBRD( MM, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                WORK( ITAUP ), WORK( IWORK ), LWORK-IWORK+1,
     $                INFO )
*
*        Multiply B by transpose of left bidiagonalizing vectors of R
*        (CWorkspace: need 2*N+NRHS, prefer 2*N+NRHS*NB)
*        (RWorkspace: none)
*
         CALL ZUNMBR( 'Q', 'L', 'C', MM, NRHS, N, A, LDA, WORK( ITAUQ ),
     $                B, LDB, WORK( IWORK ), LWORK-IWORK+1, INFO )
*
*        Generate right bidiagonalizing vectors of R in A
*        (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*        (RWorkspace: none)
*
         CALL ZUNGBR( 'P', N, N, N, A, LDA, WORK( ITAUP ),
     $                WORK( IWORK ), LWORK-IWORK+1, INFO )
         IRWORK = IE + N
*
*        Perform bidiagonal QR iteration
*          multiply B by transpose of left singular vectors
*          compute right singular vectors in A
*        (CWorkspace: none)
*        (RWorkspace: need BDSPAC)
*
         CALL ZBDSQR( 'U', N, N, 0, NRHS, S, RWORK( IE ), A, LDA, DUM,
     $                1, B, LDB, RWORK( IRWORK ), INFO )
         IF( INFO.NE.0 )
     $      GO TO 70
*
*        Multiply B by reciprocals of singular values
*
         THR = MAX( RCOND*S( 1 ), SFMIN )
         IF( RCOND.LT.ZERO )
     $      THR = MAX( EPS*S( 1 ), SFMIN )
         RANK = 0
         DO 10 I = 1, N
            IF( S( I ).GT.THR ) THEN
               CALL ZDRSCL( NRHS, S( I ), B( I, 1 ), LDB )
               RANK = RANK + 1
            ELSE
               CALL ZLASET( 'F', 1, NRHS, CZERO, CZERO, B( I, 1 ), LDB )
            END IF
   10    CONTINUE
*
*        Multiply B by right singular vectors
*        (CWorkspace: need N, prefer N*NRHS)
*        (RWorkspace: none)
*
         IF( LWORK.GE.LDB*NRHS .AND. NRHS.GT.1 ) THEN
            CALL ZGEMM( 'C', 'N', N, NRHS, N, CONE, A, LDA, B, LDB,
     $                  CZERO, WORK, LDB )
            CALL ZLACPY( 'G', N, NRHS, WORK, LDB, B, LDB )
         ELSE IF( NRHS.GT.1 ) THEN
            CHUNK = LWORK / N
            DO 20 I = 1, NRHS, CHUNK
               BL = MIN( NRHS-I+1, CHUNK )
               CALL ZGEMM( 'C', 'N', N, BL, N, CONE, A, LDA, B( 1, I ),
     $                     LDB, CZERO, WORK, N )
               CALL ZLACPY( 'G', N, BL, WORK, N, B( 1, I ), LDB )
   20       CONTINUE
         ELSE
            CALL ZGEMV( 'C', N, N, CONE, A, LDA, B, 1, CZERO, WORK, 1 )
            CALL ZCOPY( N, WORK, 1, B, 1 )
         END IF
*
      ELSE IF( N.GE.MNTHR .AND. LWORK.GE.3*M+M*M+MAX( M, NRHS, N-2*M ) )
     $          THEN
*
*        Underdetermined case, M much less than N
*
*        Path 2a - underdetermined, with many more columns than rows
*        and sufficient workspace for an efficient algorithm
*
         LDWORK = M
         IF( LWORK.GE.3*M+M*LDA+MAX( M, NRHS, N-2*M ) )
     $      LDWORK = LDA
         ITAU = 1
         IWORK = M + 1
*
*        Compute A=L*Q
*        (CWorkspace: need 2*M, prefer M+M*NB)
*        (RWorkspace: none)
*
         CALL ZGELQF( M, N, A, LDA, WORK( ITAU ), WORK( IWORK ),
     $                LWORK-IWORK+1, INFO )
         IL = IWORK
*
*        Copy L to WORK(IL), zeroing out above it
*
         CALL ZLACPY( 'L', M, M, A, LDA, WORK( IL ), LDWORK )
         CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO, WORK( IL+LDWORK ),
     $                LDWORK )
         IE = 1
         ITAUQ = IL + LDWORK*M
         ITAUP = ITAUQ + M
         IWORK = ITAUP + M
*
*        Bidiagonalize L in WORK(IL)
*        (CWorkspace: need M*M+4*M, prefer M*M+3*M+2*M*NB)
*        (RWorkspace: need M)
*
         CALL ZGEBRD( M, M, WORK( IL ), LDWORK, S, RWORK( IE ),
     $                WORK( ITAUQ ), WORK( ITAUP ), WORK( IWORK ),
     $                LWORK-IWORK+1, INFO )
*
*        Multiply B by transpose of left bidiagonalizing vectors of L
*        (CWorkspace: need M*M+3*M+NRHS, prefer M*M+3*M+NRHS*NB)
*        (RWorkspace: none)
*
         CALL ZUNMBR( 'Q', 'L', 'C', M, NRHS, M, WORK( IL ), LDWORK,
     $                WORK( ITAUQ ), B, LDB, WORK( IWORK ),
     $                LWORK-IWORK+1, INFO )
*
*        Generate right bidiagonalizing vectors of R in WORK(IL)
*        (CWorkspace: need M*M+4*M-1, prefer M*M+3*M+(M-1)*NB)
*        (RWorkspace: none)
*
         CALL ZUNGBR( 'P', M, M, M, WORK( IL ), LDWORK, WORK( ITAUP ),
     $                WORK( IWORK ), LWORK-IWORK+1, INFO )
         IRWORK = IE + M
*
*        Perform bidiagonal QR iteration, computing right singular
*        vectors of L in WORK(IL) and multiplying B by transpose of
*        left singular vectors
*        (CWorkspace: need M*M)
*        (RWorkspace: need BDSPAC)
*
         CALL ZBDSQR( 'U', M, M, 0, NRHS, S, RWORK( IE ), WORK( IL ),
     $                LDWORK, A, LDA, B, LDB, RWORK( IRWORK ), INFO )
         IF( INFO.NE.0 )
     $      GO TO 70
*
*        Multiply B by reciprocals of singular values
*
         THR = MAX( RCOND*S( 1 ), SFMIN )
         IF( RCOND.LT.ZERO )
     $      THR = MAX( EPS*S( 1 ), SFMIN )
         RANK = 0
         DO 30 I = 1, M
            IF( S( I ).GT.THR ) THEN
               CALL ZDRSCL( NRHS, S( I ), B( I, 1 ), LDB )
               RANK = RANK + 1
            ELSE
               CALL ZLASET( 'F', 1, NRHS, CZERO, CZERO, B( I, 1 ), LDB )
            END IF
   30    CONTINUE
         IWORK = IL + M*LDWORK
*
*        Multiply B by right singular vectors of L in WORK(IL)
*        (CWorkspace: need M*M+2*M, prefer M*M+M+M*NRHS)
*        (RWorkspace: none)
*
         IF( LWORK.GE.LDB*NRHS+IWORK-1 .AND. NRHS.GT.1 ) THEN
            CALL ZGEMM( 'C', 'N', M, NRHS, M, CONE, WORK( IL ), LDWORK,
     $                  B, LDB, CZERO, WORK( IWORK ), LDB )
            CALL ZLACPY( 'G', M, NRHS, WORK( IWORK ), LDB, B, LDB )
         ELSE IF( NRHS.GT.1 ) THEN
            CHUNK = ( LWORK-IWORK+1 ) / M
            DO 40 I = 1, NRHS, CHUNK
               BL = MIN( NRHS-I+1, CHUNK )
               CALL ZGEMM( 'C', 'N', M, BL, M, CONE, WORK( IL ), LDWORK,
     $                     B( 1, I ), LDB, CZERO, WORK( IWORK ), M )
               CALL ZLACPY( 'G', M, BL, WORK( IWORK ), M, B( 1, I ),
     $                      LDB )
   40       CONTINUE
         ELSE
            CALL ZGEMV( 'C', M, M, CONE, WORK( IL ), LDWORK, B( 1, 1 ),
     $                  1, CZERO, WORK( IWORK ), 1 )
            CALL ZCOPY( M, WORK( IWORK ), 1, B( 1, 1 ), 1 )
         END IF
*
*        Zero out below first M rows of B
*
         CALL ZLASET( 'F', N-M, NRHS, CZERO, CZERO, B( M+1, 1 ), LDB )
         IWORK = ITAU + M
*
*        Multiply transpose(Q) by B
*        (CWorkspace: need M+NRHS, prefer M+NHRS*NB)
*        (RWorkspace: none)
*
         CALL ZUNMLQ( 'L', 'C', N, NRHS, M, A, LDA, WORK( ITAU ), B,
     $                LDB, WORK( IWORK ), LWORK-IWORK+1, INFO )
*
      ELSE
*
*        Path 2 - remaining underdetermined cases
*
         IE = 1
         ITAUQ = 1
         ITAUP = ITAUQ + M
         IWORK = ITAUP + M
*
*        Bidiagonalize A
*        (CWorkspace: need 3*M, prefer 2*M+(M+N)*NB)
*        (RWorkspace: need N)
*
         CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                WORK( ITAUP ), WORK( IWORK ), LWORK-IWORK+1,
     $                INFO )
*
*        Multiply B by transpose of left bidiagonalizing vectors
*        (CWorkspace: need 2*M+NRHS, prefer 2*M+NRHS*NB)
*        (RWorkspace: none)
*
         CALL ZUNMBR( 'Q', 'L', 'C', M, NRHS, N, A, LDA, WORK( ITAUQ ),
     $                B, LDB, WORK( IWORK ), LWORK-IWORK+1, INFO )
*
*        Generate right bidiagonalizing vectors in A
*        (CWorkspace: need 3*M, prefer 2*M+M*NB)
*        (RWorkspace: none)
*
         CALL ZUNGBR( 'P', M, N, M, A, LDA, WORK( ITAUP ),
     $                WORK( IWORK ), LWORK-IWORK+1, INFO )
         IRWORK = IE + M
*
*        Perform bidiagonal QR iteration,
*           computing right singular vectors of A in A and
*           multiplying B by transpose of left singular vectors
*        (CWorkspace: none)
*        (RWorkspace: need BDSPAC)
*
         CALL ZBDSQR( 'L', M, N, 0, NRHS, S, RWORK( IE ), A, LDA, DUM,
     $                1, B, LDB, RWORK( IRWORK ), INFO )
         IF( INFO.NE.0 )
     $      GO TO 70
*
*        Multiply B by reciprocals of singular values
*
         THR = MAX( RCOND*S( 1 ), SFMIN )
         IF( RCOND.LT.ZERO )
     $      THR = MAX( EPS*S( 1 ), SFMIN )
         RANK = 0
         DO 50 I = 1, M
            IF( S( I ).GT.THR ) THEN
               CALL ZDRSCL( NRHS, S( I ), B( I, 1 ), LDB )
               RANK = RANK + 1
            ELSE
               CALL ZLASET( 'F', 1, NRHS, CZERO, CZERO, B( I, 1 ), LDB )
            END IF
   50    CONTINUE
*
*        Multiply B by right singular vectors of A
*        (CWorkspace: need N, prefer N*NRHS)
*        (RWorkspace: none)
*
         IF( LWORK.GE.LDB*NRHS .AND. NRHS.GT.1 ) THEN
            CALL ZGEMM( 'C', 'N', N, NRHS, M, CONE, A, LDA, B, LDB,
     $                  CZERO, WORK, LDB )
            CALL ZLACPY( 'G', N, NRHS, WORK, LDB, B, LDB )
         ELSE IF( NRHS.GT.1 ) THEN
            CHUNK = LWORK / N
            DO 60 I = 1, NRHS, CHUNK
               BL = MIN( NRHS-I+1, CHUNK )
               CALL ZGEMM( 'C', 'N', N, BL, M, CONE, A, LDA, B( 1, I ),
     $                     LDB, CZERO, WORK, N )
               CALL ZLACPY( 'F', N, BL, WORK, N, B( 1, I ), LDB )
   60       CONTINUE
         ELSE
            CALL ZGEMV( 'C', M, N, CONE, A, LDA, B, 1, CZERO, WORK, 1 )
            CALL ZCOPY( N, WORK, 1, B, 1 )
         END IF
      END IF
*
*     Undo scaling
*
      IF( IASCL.EQ.1 ) THEN
         CALL ZLASCL( 'G', 0, 0, ANRM, SMLNUM, N, NRHS, B, LDB, INFO )
         CALL DLASCL( 'G', 0, 0, SMLNUM, ANRM, MINMN, 1, S, MINMN,
     $                INFO )
      ELSE IF( IASCL.EQ.2 ) THEN
         CALL ZLASCL( 'G', 0, 0, ANRM, BIGNUM, N, NRHS, B, LDB, INFO )
         CALL DLASCL( 'G', 0, 0, BIGNUM, ANRM, MINMN, 1, S, MINMN,
     $                INFO )
      END IF
      IF( IBSCL.EQ.1 ) THEN
         CALL ZLASCL( 'G', 0, 0, SMLNUM, BNRM, N, NRHS, B, LDB, INFO )
      ELSE IF( IBSCL.EQ.2 ) THEN
         CALL ZLASCL( 'G', 0, 0, BIGNUM, BNRM, N, NRHS, B, LDB, INFO )
      END IF
   70 CONTINUE
      WORK( 1 ) = MAXWRK
      RETURN
*
*     End of ZGELSS
*
      END
