*> \brief <b> ZGESVD computes the singular value decomposition (SVD) for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGESVD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgesvd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgesvd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgesvd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGESVD( JOBU, JOBVT, M, N, A, LDA, S, U, LDU, VT, LDVT,
*                          WORK, LWORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBU, JOBVT
*       INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RWORK( * ), S( * )
*       COMPLEX*16         A( LDA, * ), U( LDU, * ), VT( LDVT, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGESVD computes the singular value decomposition (SVD) of a complex
*> M-by-N matrix A, optionally computing the left and/or right singular
*> vectors. The SVD is written
*>
*>      A = U * SIGMA * conjugate-transpose(V)
*>
*> where SIGMA is an M-by-N matrix which is zero except for its
*> min(m,n) diagonal elements, U is an M-by-M unitary matrix, and
*> V is an N-by-N unitary matrix.  The diagonal elements of SIGMA
*> are the singular values of A; they are real and non-negative, and
*> are returned in descending order.  The first min(m,n) columns of
*> U and V are the left and right singular vectors of A.
*>
*> Note that the routine returns V**H, not V.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>          Specifies options for computing all or part of the matrix U:
*>          = 'A':  all M columns of U are returned in array U:
*>          = 'S':  the first min(m,n) columns of U (the left singular
*>                  vectors) are returned in the array U;
*>          = 'O':  the first min(m,n) columns of U (the left singular
*>                  vectors) are overwritten on the array A;
*>          = 'N':  no columns of U (no left singular vectors) are
*>                  computed.
*> \endverbatim
*>
*> \param[in] JOBVT
*> \verbatim
*>          JOBVT is CHARACTER*1
*>          Specifies options for computing all or part of the matrix
*>          V**H:
*>          = 'A':  all N rows of V**H are returned in the array VT;
*>          = 'S':  the first min(m,n) rows of V**H (the right singular
*>                  vectors) are returned in the array VT;
*>          = 'O':  the first min(m,n) rows of V**H (the right singular
*>                  vectors) are overwritten on the array A;
*>          = 'N':  no rows of V**H (no right singular vectors) are
*>                  computed.
*>
*>          JOBVT and JOBU cannot both be 'O'.
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
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit,
*>          if JOBU = 'O',  A is overwritten with the first min(m,n)
*>                          columns of U (the left singular vectors,
*>                          stored columnwise);
*>          if JOBVT = 'O', A is overwritten with the first min(m,n)
*>                          rows of V**H (the right singular vectors,
*>                          stored rowwise);
*>          if JOBU .ne. 'O' and JOBVT .ne. 'O', the contents of A
*>                          are destroyed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (min(M,N))
*>          The singular values of A, sorted so that S(i) >= S(i+1).
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX*16 array, dimension (LDU,UCOL)
*>          (LDU,M) if JOBU = 'A' or (LDU,min(M,N)) if JOBU = 'S'.
*>          If JOBU = 'A', U contains the M-by-M unitary matrix U;
*>          if JOBU = 'S', U contains the first min(m,n) columns of U
*>          (the left singular vectors, stored columnwise);
*>          if JOBU = 'N' or 'O', U is not referenced.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U.  LDU >= 1; if
*>          JOBU = 'S' or 'A', LDU >= M.
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is COMPLEX*16 array, dimension (LDVT,N)
*>          If JOBVT = 'A', VT contains the N-by-N unitary matrix
*>          V**H;
*>          if JOBVT = 'S', VT contains the first min(m,n) rows of
*>          V**H (the right singular vectors, stored rowwise);
*>          if JOBVT = 'N' or 'O', VT is not referenced.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>          The leading dimension of the array VT.  LDVT >= 1; if
*>          JOBVT = 'A', LDVT >= N; if JOBVT = 'S', LDVT >= min(M,N).
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
*>          The dimension of the array WORK.
*>          LWORK >=  MAX(1,2*MIN(M,N)+MAX(M,N)).
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
*>          On exit, if INFO > 0, RWORK(1:MIN(M,N)-1) contains the
*>          unconverged superdiagonal elements of an upper bidiagonal
*>          matrix B whose diagonal is in S (not necessarily sorted).
*>          B satisfies A = U * B * VT, so it has the same singular
*>          values as A, and singular vectors related by U and VT.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if ZBDSQR did not converge, INFO specifies how many
*>                superdiagonals of an intermediate bidiagonal form B
*>                did not converge to zero. See the description of RWORK
*>                above for details.
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
*> \date April 2012
*
*> \ingroup complex16GEsing
*
*  =====================================================================
      SUBROUTINE ZGESVD( JOBU, JOBVT, M, N, A, LDA, S, U, LDU,
     $                   VT, LDVT, WORK, LWORK, RWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          JOBU, JOBVT
      INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RWORK( * ), S( * )
      COMPLEX*16         A( LDA, * ), U( LDU, * ), VT( LDVT, * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D0, 0.0D0 ),
     $                   CONE = ( 1.0D0, 0.0D0 ) )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, WNTUA, WNTUAS, WNTUN, WNTUO, WNTUS,
     $                   WNTVA, WNTVAS, WNTVN, WNTVO, WNTVS
      INTEGER            BLK, CHUNK, I, IE, IERR, IR, IRWORK, ISCL,
     $                   ITAU, ITAUP, ITAUQ, IU, IWORK, LDWRKR, LDWRKU,
     $                   MAXWRK, MINMN, MINWRK, MNTHR, NCU, NCVT, NRU,
     $                   NRVT, WRKBL
      INTEGER            LWORK_ZGEQRF, LWORK_ZUNGQR_N, LWORK_ZUNGQR_M,
     $                   LWORK_ZGEBRD, LWORK_ZUNGBR_P, LWORK_ZUNGBR_Q,
     $                   LWORK_ZGELQF, LWORK_ZUNGLQ_N, LWORK_ZUNGLQ_M
      DOUBLE PRECISION   ANRM, BIGNUM, EPS, SMLNUM
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   DUM( 1 )
      COMPLEX*16         CDUM( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLASCL, XERBLA, ZBDSQR, ZGEBRD, ZGELQF, ZGEMM,
     $                   ZGEQRF, ZLACPY, ZLASCL, ZLASET, ZUNGBR, ZUNGLQ,
     $                   ZUNGQR, ZUNMBR
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           LSAME, ILAENV, DLAMCH, ZLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      MINMN = MIN( M, N )
      WNTUA = LSAME( JOBU, 'A' )
      WNTUS = LSAME( JOBU, 'S' )
      WNTUAS = WNTUA .OR. WNTUS
      WNTUO = LSAME( JOBU, 'O' )
      WNTUN = LSAME( JOBU, 'N' )
      WNTVA = LSAME( JOBVT, 'A' )
      WNTVS = LSAME( JOBVT, 'S' )
      WNTVAS = WNTVA .OR. WNTVS
      WNTVO = LSAME( JOBVT, 'O' )
      WNTVN = LSAME( JOBVT, 'N' )
      LQUERY = ( LWORK.EQ.-1 )
*
      IF( .NOT.( WNTUA .OR. WNTUS .OR. WNTUO .OR. WNTUN ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( WNTVA .OR. WNTVS .OR. WNTVO .OR. WNTVN ) .OR.
     $         ( WNTVO .AND. WNTUO ) ) THEN
         INFO = -2
      ELSE IF( M.LT.0 ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -6
      ELSE IF( LDU.LT.1 .OR. ( WNTUAS .AND. LDU.LT.M ) ) THEN
         INFO = -9
      ELSE IF( LDVT.LT.1 .OR. ( WNTVA .AND. LDVT.LT.N ) .OR.
     $         ( WNTVS .AND. LDVT.LT.MINMN ) ) THEN
         INFO = -11
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       CWorkspace refers to complex workspace, and RWorkspace to
*       real workspace. NB refers to the optimal block size for the
*       immediately following subroutine, as returned by ILAENV.)
*
      IF( INFO.EQ.0 ) THEN
         MINWRK = 1
         MAXWRK = 1
         IF( M.GE.N .AND. MINMN.GT.0 ) THEN
*
*           Space needed for ZBDSQR is BDSPAC = 5*N
*
            MNTHR = ILAENV( 6, 'ZGESVD', JOBU // JOBVT, M, N, 0, 0 )
*           Compute space needed for ZGEQRF
            CALL ZGEQRF( M, N, A, LDA, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEQRF = INT( CDUM(1) )
*           Compute space needed for ZUNGQR
            CALL ZUNGQR( M, N, N, A, LDA, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZUNGQR_N = INT( CDUM(1) )
            CALL ZUNGQR( M, M, N, A, LDA, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZUNGQR_M = INT( CDUM(1) )
*           Compute space needed for ZGEBRD
            CALL ZGEBRD( N, N, A, LDA, S, DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEBRD = INT( CDUM(1) )
*           Compute space needed for ZUNGBR
            CALL ZUNGBR( 'P', N, N, N, A, LDA, CDUM(1),
     $                   CDUM(1), -1, IERR )
            LWORK_ZUNGBR_P = INT( CDUM(1) )
            CALL ZUNGBR( 'Q', N, N, N, A, LDA, CDUM(1),
     $                   CDUM(1), -1, IERR )
            LWORK_ZUNGBR_Q = INT( CDUM(1) )
*
            IF( M.GE.MNTHR ) THEN
               IF( WNTUN ) THEN
*
*                 Path 1 (M much larger than N, JOBU='N')
*
                  MAXWRK = N + LWORK_ZGEQRF
                  MAXWRK = MAX( MAXWRK, 2*N+LWORK_ZGEBRD )
                  IF( WNTVO .OR. WNTVAS )
     $               MAXWRK = MAX( MAXWRK, 2*N+LWORK_ZUNGBR_P )
                  MINWRK = 3*N
               ELSE IF( WNTUO .AND. WNTVN ) THEN
*
*                 Path 2 (M much larger than N, JOBU='O', JOBVT='N')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_N )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  MAXWRK = MAX( N*N+WRKBL, N*N+M*N )
                  MINWRK = 2*N + M
               ELSE IF( WNTUO .AND. WNTVAS ) THEN
*
*                 Path 3 (M much larger than N, JOBU='O', JOBVT='S' or
*                 'A')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_N )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_P )
                  MAXWRK = MAX( N*N+WRKBL, N*N+M*N )
                  MINWRK = 2*N + M
               ELSE IF( WNTUS .AND. WNTVN ) THEN
*
*                 Path 4 (M much larger than N, JOBU='S', JOBVT='N')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_N )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  MAXWRK = N*N + WRKBL
                  MINWRK = 2*N + M
               ELSE IF( WNTUS .AND. WNTVO ) THEN
*
*                 Path 5 (M much larger than N, JOBU='S', JOBVT='O')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_N )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_P )
                  MAXWRK = 2*N*N + WRKBL
                  MINWRK = 2*N + M
               ELSE IF( WNTUS .AND. WNTVAS ) THEN
*
*                 Path 6 (M much larger than N, JOBU='S', JOBVT='S' or
*                 'A')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_N )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_P )
                  MAXWRK = N*N + WRKBL
                  MINWRK = 2*N + M
               ELSE IF( WNTUA .AND. WNTVN ) THEN
*
*                 Path 7 (M much larger than N, JOBU='A', JOBVT='N')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_M )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  MAXWRK = N*N + WRKBL
                  MINWRK = 2*N + M
               ELSE IF( WNTUA .AND. WNTVO ) THEN
*
*                 Path 8 (M much larger than N, JOBU='A', JOBVT='O')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_M )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_P )
                  MAXWRK = 2*N*N + WRKBL
                  MINWRK = 2*N + M
               ELSE IF( WNTUA .AND. WNTVAS ) THEN
*
*                 Path 9 (M much larger than N, JOBU='A', JOBVT='S' or
*                 'A')
*
                  WRKBL = N + LWORK_ZGEQRF
                  WRKBL = MAX( WRKBL, N+LWORK_ZUNGQR_M )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_Q )
                  WRKBL = MAX( WRKBL, 2*N+LWORK_ZUNGBR_P )
                  MAXWRK = N*N + WRKBL
                  MINWRK = 2*N + M
               END IF
            ELSE
*
*              Path 10 (M at least N, but not much larger)
*
               CALL ZGEBRD( M, N, A, LDA, S, DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
               LWORK_ZGEBRD = INT( CDUM(1) )
               MAXWRK = 2*N + LWORK_ZGEBRD
               IF( WNTUS .OR. WNTUO ) THEN
                  CALL ZUNGBR( 'Q', M, N, N, A, LDA, CDUM(1),
     $                   CDUM(1), -1, IERR )
                  LWORK_ZUNGBR_Q = INT( CDUM(1) )
                  MAXWRK = MAX( MAXWRK, 2*N+LWORK_ZUNGBR_Q )
               END IF
               IF( WNTUA ) THEN
                  CALL ZUNGBR( 'Q', M, M, N, A, LDA, CDUM(1),
     $                   CDUM(1), -1, IERR )
                  LWORK_ZUNGBR_Q = INT( CDUM(1) )
                  MAXWRK = MAX( MAXWRK, 2*N+LWORK_ZUNGBR_Q )
               END IF
               IF( .NOT.WNTVN ) THEN
                  MAXWRK = MAX( MAXWRK, 2*N+LWORK_ZUNGBR_P )
               END IF
               MINWRK = 2*N + M
            END IF
         ELSE IF( MINMN.GT.0 ) THEN
*
*           Space needed for ZBDSQR is BDSPAC = 5*M
*
            MNTHR = ILAENV( 6, 'ZGESVD', JOBU // JOBVT, M, N, 0, 0 )
*           Compute space needed for ZGELQF
            CALL ZGELQF( M, N, A, LDA, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGELQF = INT( CDUM(1) )
*           Compute space needed for ZUNGLQ
            CALL ZUNGLQ( N, N, M, CDUM(1), N, CDUM(1), CDUM(1), -1,
     $                   IERR )
            LWORK_ZUNGLQ_N = INT( CDUM(1) )
            CALL ZUNGLQ( M, N, M, A, LDA, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZUNGLQ_M = INT( CDUM(1) )
*           Compute space needed for ZGEBRD
            CALL ZGEBRD( M, M, A, LDA, S, DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEBRD = INT( CDUM(1) )
*            Compute space needed for ZUNGBR P
            CALL ZUNGBR( 'P', M, M, M, A, N, CDUM(1),
     $                   CDUM(1), -1, IERR )
            LWORK_ZUNGBR_P = INT( CDUM(1) )
*           Compute space needed for ZUNGBR Q
            CALL ZUNGBR( 'Q', M, M, M, A, N, CDUM(1),
     $                   CDUM(1), -1, IERR )
            LWORK_ZUNGBR_Q = INT( CDUM(1) )
            IF( N.GE.MNTHR ) THEN
               IF( WNTVN ) THEN
*
*                 Path 1t(N much larger than M, JOBVT='N')
*
                  MAXWRK = M + LWORK_ZGELQF
                  MAXWRK = MAX( MAXWRK, 2*M+LWORK_ZGEBRD )
                  IF( WNTUO .OR. WNTUAS )
     $               MAXWRK = MAX( MAXWRK, 2*M+LWORK_ZUNGBR_Q )
                  MINWRK = 3*M
               ELSE IF( WNTVO .AND. WNTUN ) THEN
*
*                 Path 2t(N much larger than M, JOBU='N', JOBVT='O')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_M )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  MAXWRK = MAX( M*M+WRKBL, M*M+M*N )
                  MINWRK = 2*M + N
               ELSE IF( WNTVO .AND. WNTUAS ) THEN
*
*                 Path 3t(N much larger than M, JOBU='S' or 'A',
*                 JOBVT='O')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_M )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_Q )
                  MAXWRK = MAX( M*M+WRKBL, M*M+M*N )
                  MINWRK = 2*M + N
               ELSE IF( WNTVS .AND. WNTUN ) THEN
*
*                 Path 4t(N much larger than M, JOBU='N', JOBVT='S')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_M )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  MAXWRK = M*M + WRKBL
                  MINWRK = 2*M + N
               ELSE IF( WNTVS .AND. WNTUO ) THEN
*
*                 Path 5t(N much larger than M, JOBU='O', JOBVT='S')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_M )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_Q )
                  MAXWRK = 2*M*M + WRKBL
                  MINWRK = 2*M + N
               ELSE IF( WNTVS .AND. WNTUAS ) THEN
*
*                 Path 6t(N much larger than M, JOBU='S' or 'A',
*                 JOBVT='S')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_M )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_Q )
                  MAXWRK = M*M + WRKBL
                  MINWRK = 2*M + N
               ELSE IF( WNTVA .AND. WNTUN ) THEN
*
*                 Path 7t(N much larger than M, JOBU='N', JOBVT='A')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_N )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  MAXWRK = M*M + WRKBL
                  MINWRK = 2*M + N
               ELSE IF( WNTVA .AND. WNTUO ) THEN
*
*                 Path 8t(N much larger than M, JOBU='O', JOBVT='A')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_N )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_Q )
                  MAXWRK = 2*M*M + WRKBL
                  MINWRK = 2*M + N
               ELSE IF( WNTVA .AND. WNTUAS ) THEN
*
*                 Path 9t(N much larger than M, JOBU='S' or 'A',
*                 JOBVT='A')
*
                  WRKBL = M + LWORK_ZGELQF
                  WRKBL = MAX( WRKBL, M+LWORK_ZUNGLQ_N )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZGEBRD )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_P )
                  WRKBL = MAX( WRKBL, 2*M+LWORK_ZUNGBR_Q )
                  MAXWRK = M*M + WRKBL
                  MINWRK = 2*M + N
               END IF
            ELSE
*
*              Path 10t(N greater than M, but not much larger)
*
               CALL ZGEBRD( M, N, A, LDA, S, DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
               LWORK_ZGEBRD = INT( CDUM(1) )
               MAXWRK = 2*M + LWORK_ZGEBRD
               IF( WNTVS .OR. WNTVO ) THEN
*                Compute space needed for ZUNGBR P
                 CALL ZUNGBR( 'P', M, N, M, A, N, CDUM(1),
     $                   CDUM(1), -1, IERR )
                 LWORK_ZUNGBR_P = INT( CDUM(1) )
                 MAXWRK = MAX( MAXWRK, 2*M+LWORK_ZUNGBR_P )
               END IF
               IF( WNTVA ) THEN
                 CALL ZUNGBR( 'P', N,  N, M, A, N, CDUM(1),
     $                   CDUM(1), -1, IERR )
                 LWORK_ZUNGBR_P = INT( CDUM(1) )
                 MAXWRK = MAX( MAXWRK, 2*M+LWORK_ZUNGBR_P )
               END IF
               IF( .NOT.WNTUN ) THEN
                  MAXWRK = MAX( MAXWRK, 2*M+LWORK_ZUNGBR_Q )
               END IF
               MINWRK = 2*M + N
            END IF
         END IF
         MAXWRK = MAX( MAXWRK, MINWRK )
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -13
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGESVD', -INFO )
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
*     Get machine constants
*
      EPS = DLAMCH( 'P' )
      SMLNUM = SQRT( DLAMCH( 'S' ) ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = ZLANGE( 'M', M, N, A, LDA, DUM )
      ISCL = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         ISCL = 1
         CALL ZLASCL( 'G', 0, 0, ANRM, SMLNUM, M, N, A, LDA, IERR )
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         ISCL = 1
         CALL ZLASCL( 'G', 0, 0, ANRM, BIGNUM, M, N, A, LDA, IERR )
      END IF
*
      IF( M.GE.N ) THEN
*
*        A has at least as many rows as columns. If A has sufficiently
*        more rows than columns, first reduce using the QR
*        decomposition (if sufficient workspace available)
*
         IF( M.GE.MNTHR ) THEN
*
            IF( WNTUN ) THEN
*
*              Path 1 (M much larger than N, JOBU='N')
*              No left singular vectors to be computed
*
               ITAU = 1
               IWORK = ITAU + N
*
*              Compute A=Q*R
*              (CWorkspace: need 2*N, prefer N+N*NB)
*              (RWorkspace: need 0)
*
               CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( IWORK ),
     $                      LWORK-IWORK+1, IERR )
*
*              Zero out below R
*
               IF( N .GT. 1 ) THEN
                  CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO, A( 2, 1 ),
     $                         LDA )
               END IF
               IE = 1
               ITAUQ = 1
               ITAUP = ITAUQ + N
               IWORK = ITAUP + N
*
*              Bidiagonalize R in A
*              (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*              (RWorkspace: need N)
*
               CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                      WORK( ITAUP ), WORK( IWORK ), LWORK-IWORK+1,
     $                      IERR )
               NCVT = 0
               IF( WNTVO .OR. WNTVAS ) THEN
*
*                 If right singular vectors desired, generate P'.
*                 (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'P', N, N, N, A, LDA, WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  NCVT = N
               END IF
               IRWORK = IE + N
*
*              Perform bidiagonal QR iteration, computing right
*              singular vectors of A in A if desired
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'U', N, NCVT, 0, 0, S, RWORK( IE ), A, LDA,
     $                      CDUM, 1, CDUM, 1, RWORK( IRWORK ), INFO )
*
*              If right singular vectors desired in VT, copy them there
*
               IF( WNTVAS )
     $            CALL ZLACPY( 'F', N, N, A, LDA, VT, LDVT )
*
            ELSE IF( WNTUO .AND. WNTVN ) THEN
*
*              Path 2 (M much larger than N, JOBU='O', JOBVT='N')
*              N left singular vectors to be overwritten on A and
*              no right singular vectors to be computed
*
               IF( LWORK.GE.N*N+3*N ) THEN
*
*                 Sufficient workspace for a fast algorithm
*
                  IR = 1
                  IF( LWORK.GE.MAX( WRKBL, LDA*N )+LDA*N ) THEN
*
*                    WORK(IU) is LDA by N, WORK(IR) is LDA by N
*
                     LDWRKU = LDA
                     LDWRKR = LDA
                  ELSE IF( LWORK.GE.MAX( WRKBL, LDA*N )+N*N ) THEN
*
*                    WORK(IU) is LDA by N, WORK(IR) is N by N
*
                     LDWRKU = LDA
                     LDWRKR = N
                  ELSE
*
*                    WORK(IU) is LDWRKU by N, WORK(IR) is N by N
*
                     LDWRKU = ( LWORK-N*N ) / N
                     LDWRKR = N
                  END IF
                  ITAU = IR + LDWRKR*N
                  IWORK = ITAU + N
*
*                 Compute A=Q*R
*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Copy R to WORK(IR) and zero out below it
*
                  CALL ZLACPY( 'U', N, N, A, LDA, WORK( IR ), LDWRKR )
                  CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                         WORK( IR+1 ), LDWRKR )
*
*                 Generate Q in A
*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IE = 1
                  ITAUQ = ITAU
                  ITAUP = ITAUQ + N
                  IWORK = ITAUP + N
*
*                 Bidiagonalize R in WORK(IR)
*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB)
*                 (RWorkspace: need N)
*
                  CALL ZGEBRD( N, N, WORK( IR ), LDWRKR, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Generate left vectors bidiagonalizing R
*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB)
*                 (RWorkspace: need 0)
*
                  CALL ZUNGBR( 'Q', N, N, N, WORK( IR ), LDWRKR,
     $                         WORK( ITAUQ ), WORK( IWORK ),
     $                         LWORK-IWORK+1, IERR )
                  IRWORK = IE + N
*
*                 Perform bidiagonal QR iteration, computing left
*                 singular vectors of R in WORK(IR)
*                 (CWorkspace: need N*N)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', N, 0, N, 0, S, RWORK( IE ), CDUM, 1,
     $                         WORK( IR ), LDWRKR, CDUM, 1,
     $                         RWORK( IRWORK ), INFO )
                  IU = ITAUQ
*
*                 Multiply Q in A by left singular vectors of R in
*                 WORK(IR), storing result in WORK(IU) and copying to A
*                 (CWorkspace: need N*N+N, prefer N*N+M*N)
*                 (RWorkspace: 0)
*
                  DO 10 I = 1, M, LDWRKU
                     CHUNK = MIN( M-I+1, LDWRKU )
                     CALL ZGEMM( 'N', 'N', CHUNK, N, N, CONE, A( I, 1 ),
     $                           LDA, WORK( IR ), LDWRKR, CZERO,
     $                           WORK( IU ), LDWRKU )
                     CALL ZLACPY( 'F', CHUNK, N, WORK( IU ), LDWRKU,
     $                            A( I, 1 ), LDA )
   10             CONTINUE
*
               ELSE
*
*                 Insufficient workspace for a fast algorithm
*
                  IE = 1
                  ITAUQ = 1
                  ITAUP = ITAUQ + N
                  IWORK = ITAUP + N
*
*                 Bidiagonalize A
*                 (CWorkspace: need 2*N+M, prefer 2*N+(M+N)*NB)
*                 (RWorkspace: N)
*
                  CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Generate left vectors bidiagonalizing A
*                 (CWorkspace: need 3*N, prefer 2*N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'Q', M, N, N, A, LDA, WORK( ITAUQ ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IRWORK = IE + N
*
*                 Perform bidiagonal QR iteration, computing left
*                 singular vectors of A in A
*                 (CWorkspace: need 0)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', N, 0, M, 0, S, RWORK( IE ), CDUM, 1,
     $                         A, LDA, CDUM, 1, RWORK( IRWORK ), INFO )
*
               END IF
*
            ELSE IF( WNTUO .AND. WNTVAS ) THEN
*
*              Path 3 (M much larger than N, JOBU='O', JOBVT='S' or 'A')
*              N left singular vectors to be overwritten on A and
*              N right singular vectors to be computed in VT
*
               IF( LWORK.GE.N*N+3*N ) THEN
*
*                 Sufficient workspace for a fast algorithm
*
                  IR = 1
                  IF( LWORK.GE.MAX( WRKBL, LDA*N )+LDA*N ) THEN
*
*                    WORK(IU) is LDA by N and WORK(IR) is LDA by N
*
                     LDWRKU = LDA
                     LDWRKR = LDA
                  ELSE IF( LWORK.GE.MAX( WRKBL, LDA*N )+N*N ) THEN
*
*                    WORK(IU) is LDA by N and WORK(IR) is N by N
*
                     LDWRKU = LDA
                     LDWRKR = N
                  ELSE
*
*                    WORK(IU) is LDWRKU by N and WORK(IR) is N by N
*
                     LDWRKU = ( LWORK-N*N ) / N
                     LDWRKR = N
                  END IF
                  ITAU = IR + LDWRKR*N
                  IWORK = ITAU + N
*
*                 Compute A=Q*R
*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Copy R to VT, zeroing out below it
*
                  CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
                  IF( N.GT.1 )
     $               CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            VT( 2, 1 ), LDVT )
*
*                 Generate Q in A
*                 (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IE = 1
                  ITAUQ = ITAU
                  ITAUP = ITAUQ + N
                  IWORK = ITAUP + N
*
*                 Bidiagonalize R in VT, copying result to WORK(IR)
*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB)
*                 (RWorkspace: need N)
*
                  CALL ZGEBRD( N, N, VT, LDVT, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  CALL ZLACPY( 'L', N, N, VT, LDVT, WORK( IR ), LDWRKR )
*
*                 Generate left vectors bidiagonalizing R in WORK(IR)
*                 (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'Q', N, N, N, WORK( IR ), LDWRKR,
     $                         WORK( ITAUQ ), WORK( IWORK ),
     $                         LWORK-IWORK+1, IERR )
*
*                 Generate right vectors bidiagonalizing R in VT
*                 (CWorkspace: need N*N+3*N-1, prefer N*N+2*N+(N-1)*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IRWORK = IE + N
*
*                 Perform bidiagonal QR iteration, computing left
*                 singular vectors of R in WORK(IR) and computing right
*                 singular vectors of R in VT
*                 (CWorkspace: need N*N)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', N, N, N, 0, S, RWORK( IE ), VT,
     $                         LDVT, WORK( IR ), LDWRKR, CDUM, 1,
     $                         RWORK( IRWORK ), INFO )
                  IU = ITAUQ
*
*                 Multiply Q in A by left singular vectors of R in
*                 WORK(IR), storing result in WORK(IU) and copying to A
*                 (CWorkspace: need N*N+N, prefer N*N+M*N)
*                 (RWorkspace: 0)
*
                  DO 20 I = 1, M, LDWRKU
                     CHUNK = MIN( M-I+1, LDWRKU )
                     CALL ZGEMM( 'N', 'N', CHUNK, N, N, CONE, A( I, 1 ),
     $                           LDA, WORK( IR ), LDWRKR, CZERO,
     $                           WORK( IU ), LDWRKU )
                     CALL ZLACPY( 'F', CHUNK, N, WORK( IU ), LDWRKU,
     $                            A( I, 1 ), LDA )
   20             CONTINUE
*
               ELSE
*
*                 Insufficient workspace for a fast algorithm
*
                  ITAU = 1
                  IWORK = ITAU + N
*
*                 Compute A=Q*R
*                 (CWorkspace: need 2*N, prefer N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Copy R to VT, zeroing out below it
*
                  CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
                  IF( N.GT.1 )
     $               CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            VT( 2, 1 ), LDVT )
*
*                 Generate Q in A
*                 (CWorkspace: need 2*N, prefer N+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IE = 1
                  ITAUQ = ITAU
                  ITAUP = ITAUQ + N
                  IWORK = ITAUP + N
*
*                 Bidiagonalize R in VT
*                 (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                 (RWorkspace: N)
*
                  CALL ZGEBRD( N, N, VT, LDVT, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Multiply Q in A by left vectors bidiagonalizing R
*                 (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, VT, LDVT,
     $                         WORK( ITAUQ ), A, LDA, WORK( IWORK ),
     $                         LWORK-IWORK+1, IERR )
*
*                 Generate right vectors bidiagonalizing R in VT
*                 (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IRWORK = IE + N
*
*                 Perform bidiagonal QR iteration, computing left
*                 singular vectors of A in A and computing right
*                 singular vectors of A in VT
*                 (CWorkspace: 0)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', N, N, M, 0, S, RWORK( IE ), VT,
     $                         LDVT, A, LDA, CDUM, 1, RWORK( IRWORK ),
     $                         INFO )
*
               END IF
*
            ELSE IF( WNTUS ) THEN
*
               IF( WNTVN ) THEN
*
*                 Path 4 (M much larger than N, JOBU='S', JOBVT='N')
*                 N left singular vectors to be computed in U and
*                 no right singular vectors to be computed
*
                  IF( LWORK.GE.N*N+3*N ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IR = 1
                     IF( LWORK.GE.WRKBL+LDA*N ) THEN
*
*                       WORK(IR) is LDA by N
*
                        LDWRKR = LDA
                     ELSE
*
*                       WORK(IR) is N by N
*
                        LDWRKR = N
                     END IF
                     ITAU = IR + LDWRKR*N
                     IWORK = ITAU + N
*
*                    Compute A=Q*R
*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R to WORK(IR), zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, WORK( IR ),
     $                            LDWRKR )
                     CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            WORK( IR+1 ), LDWRKR )
*
*                    Generate Q in A
*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in WORK(IR)
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, WORK( IR ), LDWRKR, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate left vectors bidiagonalizing R in WORK(IR)
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', N, N, N, WORK( IR ), LDWRKR,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of R in WORK(IR)
*                    (CWorkspace: need N*N)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, 0, N, 0, S, RWORK( IE ), CDUM,
     $                            1, WORK( IR ), LDWRKR, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply Q in A by left singular vectors of R in
*                    WORK(IR), storing result in U
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, N, CONE, A, LDA,
     $                           WORK( IR ), LDWRKR, CZERO, U, LDU )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, N, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Zero out below R in A
*
                     IF( N .GT. 1 ) THEN
                        CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                               A( 2, 1 ), LDA )
                     END IF
*
*                    Bidiagonalize R in A
*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply Q in U by left vectors bidiagonalizing R
*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, A, LDA,
     $                            WORK( ITAUQ ), U, LDU, WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, 0, M, 0, S, RWORK( IE ), CDUM,
     $                            1, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
                  END IF
*
               ELSE IF( WNTVO ) THEN
*
*                 Path 5 (M much larger than N, JOBU='S', JOBVT='O')
*                 N left singular vectors to be computed in U and
*                 N right singular vectors to be overwritten on A
*
                  IF( LWORK.GE.2*N*N+3*N ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+2*LDA*N ) THEN
*
*                       WORK(IU) is LDA by N and WORK(IR) is LDA by N
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*N
                        LDWRKR = LDA
                     ELSE IF( LWORK.GE.WRKBL+( LDA+N )*N ) THEN
*
*                       WORK(IU) is LDA by N and WORK(IR) is N by N
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*N
                        LDWRKR = N
                     ELSE
*
*                       WORK(IU) is N by N and WORK(IR) is N by N
*
                        LDWRKU = N
                        IR = IU + LDWRKU*N
                        LDWRKR = N
                     END IF
                     ITAU = IR + LDWRKR*N
                     IWORK = ITAU + N
*
*                    Compute A=Q*R
*                    (CWorkspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R to WORK(IU), zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            WORK( IU+1 ), LDWRKU )
*
*                    Generate Q in A
*                    (CWorkspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in WORK(IU), copying result to
*                    WORK(IR)
*                    (CWorkspace: need   2*N*N+3*N,
*                                 prefer 2*N*N+2*N+2*N*NB)
*                    (RWorkspace: need   N)
*
                     CALL ZGEBRD( N, N, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', N, N, WORK( IU ), LDWRKU,
     $                            WORK( IR ), LDWRKR )
*
*                    Generate left bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need 2*N*N+3*N, prefer 2*N*N+2*N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', N, N, N, WORK( IU ), LDWRKU,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in WORK(IR)
*                    (CWorkspace: need   2*N*N+3*N-1,
*                                 prefer 2*N*N+2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, WORK( IR ), LDWRKR,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of R in WORK(IU) and computing
*                    right singular vectors of R in WORK(IR)
*                    (CWorkspace: need 2*N*N)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, N, 0, S, RWORK( IE ),
     $                            WORK( IR ), LDWRKR, WORK( IU ),
     $                            LDWRKU, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
*                    Multiply Q in A by left singular vectors of R in
*                    WORK(IU), storing result in U
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, N, CONE, A, LDA,
     $                           WORK( IU ), LDWRKU, CZERO, U, LDU )
*
*                    Copy right singular vectors of R to A
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZLACPY( 'F', N, N, WORK( IR ), LDWRKR, A,
     $                            LDA )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, N, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Zero out below R in A
*
                     IF( N .GT. 1 ) THEN
                        CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                               A( 2, 1 ), LDA )
                     END IF
*
*                    Bidiagonalize R in A
*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply Q in U by left vectors bidiagonalizing R
*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, A, LDA,
     $                            WORK( ITAUQ ), U, LDU, WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right vectors bidiagonalizing R in A
*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, A, LDA, WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U and computing right
*                    singular vectors of A in A
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, M, 0, S, RWORK( IE ), A,
     $                            LDA, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
                  END IF
*
               ELSE IF( WNTVAS ) THEN
*
*                 Path 6 (M much larger than N, JOBU='S', JOBVT='S'
*                         or 'A')
*                 N left singular vectors to be computed in U and
*                 N right singular vectors to be computed in VT
*
                  IF( LWORK.GE.N*N+3*N ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+LDA*N ) THEN
*
*                       WORK(IU) is LDA by N
*
                        LDWRKU = LDA
                     ELSE
*
*                       WORK(IU) is N by N
*
                        LDWRKU = N
                     END IF
                     ITAU = IU + LDWRKU*N
                     IWORK = ITAU + N
*
*                    Compute A=Q*R
*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R to WORK(IU), zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            WORK( IU+1 ), LDWRKU )
*
*                    Generate Q in A
*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in WORK(IU), copying result to VT
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', N, N, WORK( IU ), LDWRKU, VT,
     $                            LDVT )
*
*                    Generate left bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', N, N, N, WORK( IU ), LDWRKU,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in VT
*                    (CWorkspace: need   N*N+3*N-1,
*                                 prefer N*N+2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of R in WORK(IU) and computing
*                    right singular vectors of R in VT
*                    (CWorkspace: need N*N)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, N, 0, S, RWORK( IE ), VT,
     $                            LDVT, WORK( IU ), LDWRKU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply Q in A by left singular vectors of R in
*                    WORK(IU), storing result in U
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, N, CONE, A, LDA,
     $                           WORK( IU ), LDWRKU, CZERO, U, LDU )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, N, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R to VT, zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
                     IF( N.GT.1 )
     $                  CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                               VT( 2, 1 ), LDVT )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in VT
*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, VT, LDVT, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply Q in U by left bidiagonalizing vectors
*                    in VT
*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, VT, LDVT,
     $                            WORK( ITAUQ ), U, LDU, WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in VT
*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U and computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, M, 0, S, RWORK( IE ), VT,
     $                            LDVT, U, LDU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               END IF
*
            ELSE IF( WNTUA ) THEN
*
               IF( WNTVN ) THEN
*
*                 Path 7 (M much larger than N, JOBU='A', JOBVT='N')
*                 M left singular vectors to be computed in U and
*                 no right singular vectors to be computed
*
                  IF( LWORK.GE.N*N+MAX( N+M, 3*N ) ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IR = 1
                     IF( LWORK.GE.WRKBL+LDA*N ) THEN
*
*                       WORK(IR) is LDA by N
*
                        LDWRKR = LDA
                     ELSE
*
*                       WORK(IR) is N by N
*
                        LDWRKR = N
                     END IF
                     ITAU = IR + LDWRKR*N
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Copy R to WORK(IR), zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, WORK( IR ),
     $                            LDWRKR )
                     CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            WORK( IR+1 ), LDWRKR )
*
*                    Generate Q in U
*                    (CWorkspace: need N*N+N+M, prefer N*N+N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in WORK(IR)
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, WORK( IR ), LDWRKR, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in WORK(IR)
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', N, N, N, WORK( IR ), LDWRKR,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of R in WORK(IR)
*                    (CWorkspace: need N*N)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, 0, N, 0, S, RWORK( IE ), CDUM,
     $                            1, WORK( IR ), LDWRKR, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply Q in U by left singular vectors of R in
*                    WORK(IR), storing result in A
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, N, CONE, U, LDU,
     $                           WORK( IR ), LDWRKR, CZERO, A, LDA )
*
*                    Copy left singular vectors of A from A to U
*
                     CALL ZLACPY( 'F', M, N, A, LDA, U, LDU )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need N+M, prefer N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Zero out below R in A
*
                     IF( N .GT. 1 ) THEN
                        CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                               A( 2, 1 ), LDA )
                     END IF
*
*                    Bidiagonalize R in A
*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply Q in U by left bidiagonalizing vectors
*                    in A
*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, A, LDA,
     $                            WORK( ITAUQ ), U, LDU, WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, 0, M, 0, S, RWORK( IE ), CDUM,
     $                            1, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
                  END IF
*
               ELSE IF( WNTVO ) THEN
*
*                 Path 8 (M much larger than N, JOBU='A', JOBVT='O')
*                 M left singular vectors to be computed in U and
*                 N right singular vectors to be overwritten on A
*
                  IF( LWORK.GE.2*N*N+MAX( N+M, 3*N ) ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+2*LDA*N ) THEN
*
*                       WORK(IU) is LDA by N and WORK(IR) is LDA by N
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*N
                        LDWRKR = LDA
                     ELSE IF( LWORK.GE.WRKBL+( LDA+N )*N ) THEN
*
*                       WORK(IU) is LDA by N and WORK(IR) is N by N
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*N
                        LDWRKR = N
                     ELSE
*
*                       WORK(IU) is N by N and WORK(IR) is N by N
*
                        LDWRKU = N
                        IR = IU + LDWRKU*N
                        LDWRKR = N
                     END IF
                     ITAU = IR + LDWRKR*N
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N*N+2*N, prefer 2*N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need 2*N*N+N+M, prefer 2*N*N+N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R to WORK(IU), zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            WORK( IU+1 ), LDWRKU )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in WORK(IU), copying result to
*                    WORK(IR)
*                    (CWorkspace: need   2*N*N+3*N,
*                                 prefer 2*N*N+2*N+2*N*NB)
*                    (RWorkspace: need   N)
*
                     CALL ZGEBRD( N, N, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', N, N, WORK( IU ), LDWRKU,
     $                            WORK( IR ), LDWRKR )
*
*                    Generate left bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need 2*N*N+3*N, prefer 2*N*N+2*N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', N, N, N, WORK( IU ), LDWRKU,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in WORK(IR)
*                    (CWorkspace: need   2*N*N+3*N-1,
*                                 prefer 2*N*N+2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, WORK( IR ), LDWRKR,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of R in WORK(IU) and computing
*                    right singular vectors of R in WORK(IR)
*                    (CWorkspace: need 2*N*N)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, N, 0, S, RWORK( IE ),
     $                            WORK( IR ), LDWRKR, WORK( IU ),
     $                            LDWRKU, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
*                    Multiply Q in U by left singular vectors of R in
*                    WORK(IU), storing result in A
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, N, CONE, U, LDU,
     $                           WORK( IU ), LDWRKU, CZERO, A, LDA )
*
*                    Copy left singular vectors of A from A to U
*
                     CALL ZLACPY( 'F', M, N, A, LDA, U, LDU )
*
*                    Copy right singular vectors of R from WORK(IR) to A
*
                     CALL ZLACPY( 'F', N, N, WORK( IR ), LDWRKR, A,
     $                            LDA )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need N+M, prefer N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Zero out below R in A
*
                     IF( N .GT. 1 ) THEN
                        CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                               A( 2, 1 ), LDA )
                     END IF
*
*                    Bidiagonalize R in A
*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply Q in U by left bidiagonalizing vectors
*                    in A
*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, A, LDA,
     $                            WORK( ITAUQ ), U, LDU, WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in A
*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, A, LDA, WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U and computing right
*                    singular vectors of A in A
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, M, 0, S, RWORK( IE ), A,
     $                            LDA, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
                  END IF
*
               ELSE IF( WNTVAS ) THEN
*
*                 Path 9 (M much larger than N, JOBU='A', JOBVT='S'
*                         or 'A')
*                 M left singular vectors to be computed in U and
*                 N right singular vectors to be computed in VT
*
                  IF( LWORK.GE.N*N+MAX( N+M, 3*N ) ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+LDA*N ) THEN
*
*                       WORK(IU) is LDA by N
*
                        LDWRKU = LDA
                     ELSE
*
*                       WORK(IU) is N by N
*
                        LDWRKU = N
                     END IF
                     ITAU = IU + LDWRKU*N
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need N*N+2*N, prefer N*N+N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need N*N+N+M, prefer N*N+N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R to WORK(IU), zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                            WORK( IU+1 ), LDWRKU )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in WORK(IU), copying result to VT
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', N, N, WORK( IU ), LDWRKU, VT,
     $                            LDVT )
*
*                    Generate left bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need N*N+3*N, prefer N*N+2*N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', N, N, N, WORK( IU ), LDWRKU,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in VT
*                    (CWorkspace: need   N*N+3*N-1,
*                                 prefer N*N+2*N+(N-1)*NB)
*                    (RWorkspace: need   0)
*
                     CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of R in WORK(IU) and computing
*                    right singular vectors of R in VT
*                    (CWorkspace: need N*N)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, N, 0, S, RWORK( IE ), VT,
     $                            LDVT, WORK( IU ), LDWRKU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply Q in U by left singular vectors of R in
*                    WORK(IU), storing result in A
*                    (CWorkspace: need N*N)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, N, CONE, U, LDU,
     $                           WORK( IU ), LDWRKU, CZERO, A, LDA )
*
*                    Copy left singular vectors of A from A to U
*
                     CALL ZLACPY( 'F', M, N, A, LDA, U, LDU )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + N
*
*                    Compute A=Q*R, copying result to U
*                    (CWorkspace: need 2*N, prefer N+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*                    Generate Q in U
*                    (CWorkspace: need N+M, prefer N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy R from A to VT, zeroing out below it
*
                     CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
                     IF( N.GT.1 )
     $                  CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO,
     $                               VT( 2, 1 ), LDVT )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + N
                     IWORK = ITAUP + N
*
*                    Bidiagonalize R in VT
*                    (CWorkspace: need 3*N, prefer 2*N+2*N*NB)
*                    (RWorkspace: need N)
*
                     CALL ZGEBRD( N, N, VT, LDVT, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply Q in U by left bidiagonalizing vectors
*                    in VT
*                    (CWorkspace: need 2*N+M, prefer 2*N+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'Q', 'R', 'N', M, N, N, VT, LDVT,
     $                            WORK( ITAUQ ), U, LDU, WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in VT
*                    (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + N
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U and computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', N, N, M, 0, S, RWORK( IE ), VT,
     $                            LDVT, U, LDU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               END IF
*
            END IF
*
         ELSE
*
*           M .LT. MNTHR
*
*           Path 10 (M at least N, but not much larger)
*           Reduce to bidiagonal form without QR decomposition
*
            IE = 1
            ITAUQ = 1
            ITAUP = ITAUQ + N
            IWORK = ITAUP + N
*
*           Bidiagonalize A
*           (CWorkspace: need 2*N+M, prefer 2*N+(M+N)*NB)
*           (RWorkspace: need N)
*
            CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                   WORK( ITAUP ), WORK( IWORK ), LWORK-IWORK+1,
     $                   IERR )
            IF( WNTUAS ) THEN
*
*              If left singular vectors desired in U, copy result to U
*              and generate left bidiagonalizing vectors in U
*              (CWorkspace: need 2*N+NCU, prefer 2*N+NCU*NB)
*              (RWorkspace: 0)
*
               CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
               IF( WNTUS )
     $            NCU = N
               IF( WNTUA )
     $            NCU = M
               CALL ZUNGBR( 'Q', M, NCU, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IF( WNTVAS ) THEN
*
*              If right singular vectors desired in VT, copy result to
*              VT and generate right bidiagonalizing vectors in VT
*              (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*              (RWorkspace: 0)
*
               CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
               CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IF( WNTUO ) THEN
*
*              If left singular vectors desired in A, generate left
*              bidiagonalizing vectors in A
*              (CWorkspace: need 3*N, prefer 2*N+N*NB)
*              (RWorkspace: 0)
*
               CALL ZUNGBR( 'Q', M, N, N, A, LDA, WORK( ITAUQ ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IF( WNTVO ) THEN
*
*              If right singular vectors desired in A, generate right
*              bidiagonalizing vectors in A
*              (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB)
*              (RWorkspace: 0)
*
               CALL ZUNGBR( 'P', N, N, N, A, LDA, WORK( ITAUP ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IRWORK = IE + N
            IF( WNTUAS .OR. WNTUO )
     $         NRU = M
            IF( WNTUN )
     $         NRU = 0
            IF( WNTVAS .OR. WNTVO )
     $         NCVT = N
            IF( WNTVN )
     $         NCVT = 0
            IF( ( .NOT.WNTUO ) .AND. ( .NOT.WNTVO ) ) THEN
*
*              Perform bidiagonal QR iteration, if desired, computing
*              left singular vectors in U and computing right singular
*              vectors in VT
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'U', N, NCVT, NRU, 0, S, RWORK( IE ), VT,
     $                      LDVT, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                      INFO )
            ELSE IF( ( .NOT.WNTUO ) .AND. WNTVO ) THEN
*
*              Perform bidiagonal QR iteration, if desired, computing
*              left singular vectors in U and computing right singular
*              vectors in A
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'U', N, NCVT, NRU, 0, S, RWORK( IE ), A,
     $                      LDA, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                      INFO )
            ELSE
*
*              Perform bidiagonal QR iteration, if desired, computing
*              left singular vectors in A and computing right singular
*              vectors in VT
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'U', N, NCVT, NRU, 0, S, RWORK( IE ), VT,
     $                      LDVT, A, LDA, CDUM, 1, RWORK( IRWORK ),
     $                      INFO )
            END IF
*
         END IF
*
      ELSE
*
*        A has more columns than rows. If A has sufficiently more
*        columns than rows, first reduce using the LQ decomposition (if
*        sufficient workspace available)
*
         IF( N.GE.MNTHR ) THEN
*
            IF( WNTVN ) THEN
*
*              Path 1t(N much larger than M, JOBVT='N')
*              No right singular vectors to be computed
*
               ITAU = 1
               IWORK = ITAU + M
*
*              Compute A=L*Q
*              (CWorkspace: need 2*M, prefer M+M*NB)
*              (RWorkspace: 0)
*
               CALL ZGELQF( M, N, A, LDA, WORK( ITAU ), WORK( IWORK ),
     $                      LWORK-IWORK+1, IERR )
*
*              Zero out above L
*
               CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO, A( 1, 2 ),
     $                      LDA )
               IE = 1
               ITAUQ = 1
               ITAUP = ITAUQ + M
               IWORK = ITAUP + M
*
*              Bidiagonalize L in A
*              (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*              (RWorkspace: need M)
*
               CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                      WORK( ITAUP ), WORK( IWORK ), LWORK-IWORK+1,
     $                      IERR )
               IF( WNTUO .OR. WNTUAS ) THEN
*
*                 If left singular vectors desired, generate Q
*                 (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'Q', M, M, M, A, LDA, WORK( ITAUQ ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
               END IF
               IRWORK = IE + M
               NRU = 0
               IF( WNTUO .OR. WNTUAS )
     $            NRU = M
*
*              Perform bidiagonal QR iteration, computing left singular
*              vectors of A in A if desired
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'U', M, 0, NRU, 0, S, RWORK( IE ), CDUM, 1,
     $                      A, LDA, CDUM, 1, RWORK( IRWORK ), INFO )
*
*              If left singular vectors desired in U, copy them there
*
               IF( WNTUAS )
     $            CALL ZLACPY( 'F', M, M, A, LDA, U, LDU )
*
            ELSE IF( WNTVO .AND. WNTUN ) THEN
*
*              Path 2t(N much larger than M, JOBU='N', JOBVT='O')
*              M right singular vectors to be overwritten on A and
*              no left singular vectors to be computed
*
               IF( LWORK.GE.M*M+3*M ) THEN
*
*                 Sufficient workspace for a fast algorithm
*
                  IR = 1
                  IF( LWORK.GE.MAX( WRKBL, LDA*N )+LDA*M ) THEN
*
*                    WORK(IU) is LDA by N and WORK(IR) is LDA by M
*
                     LDWRKU = LDA
                     CHUNK = N
                     LDWRKR = LDA
                  ELSE IF( LWORK.GE.MAX( WRKBL, LDA*N )+M*M ) THEN
*
*                    WORK(IU) is LDA by N and WORK(IR) is M by M
*
                     LDWRKU = LDA
                     CHUNK = N
                     LDWRKR = M
                  ELSE
*
*                    WORK(IU) is M by CHUNK and WORK(IR) is M by M
*
                     LDWRKU = M
                     CHUNK = ( LWORK-M*M ) / M
                     LDWRKR = M
                  END IF
                  ITAU = IR + LDWRKR*M
                  IWORK = ITAU + M
*
*                 Compute A=L*Q
*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Copy L to WORK(IR) and zero out above it
*
                  CALL ZLACPY( 'L', M, M, A, LDA, WORK( IR ), LDWRKR )
                  CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                         WORK( IR+LDWRKR ), LDWRKR )
*
*                 Generate Q in A
*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IE = 1
                  ITAUQ = ITAU
                  ITAUP = ITAUQ + M
                  IWORK = ITAUP + M
*
*                 Bidiagonalize L in WORK(IR)
*                 (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB)
*                 (RWorkspace: need M)
*
                  CALL ZGEBRD( M, M, WORK( IR ), LDWRKR, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Generate right vectors bidiagonalizing L
*                 (CWorkspace: need M*M+3*M-1, prefer M*M+2*M+(M-1)*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'P', M, M, M, WORK( IR ), LDWRKR,
     $                         WORK( ITAUP ), WORK( IWORK ),
     $                         LWORK-IWORK+1, IERR )
                  IRWORK = IE + M
*
*                 Perform bidiagonal QR iteration, computing right
*                 singular vectors of L in WORK(IR)
*                 (CWorkspace: need M*M)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', M, M, 0, 0, S, RWORK( IE ),
     $                         WORK( IR ), LDWRKR, CDUM, 1, CDUM, 1,
     $                         RWORK( IRWORK ), INFO )
                  IU = ITAUQ
*
*                 Multiply right singular vectors of L in WORK(IR) by Q
*                 in A, storing result in WORK(IU) and copying to A
*                 (CWorkspace: need M*M+M, prefer M*M+M*N)
*                 (RWorkspace: 0)
*
                  DO 30 I = 1, N, CHUNK
                     BLK = MIN( N-I+1, CHUNK )
                     CALL ZGEMM( 'N', 'N', M, BLK, M, CONE, WORK( IR ),
     $                           LDWRKR, A( 1, I ), LDA, CZERO,
     $                           WORK( IU ), LDWRKU )
                     CALL ZLACPY( 'F', M, BLK, WORK( IU ), LDWRKU,
     $                            A( 1, I ), LDA )
   30             CONTINUE
*
               ELSE
*
*                 Insufficient workspace for a fast algorithm
*
                  IE = 1
                  ITAUQ = 1
                  ITAUP = ITAUQ + M
                  IWORK = ITAUP + M
*
*                 Bidiagonalize A
*                 (CWorkspace: need 2*M+N, prefer 2*M+(M+N)*NB)
*                 (RWorkspace: need M)
*
                  CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Generate right vectors bidiagonalizing A
*                 (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'P', M, N, M, A, LDA, WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IRWORK = IE + M
*
*                 Perform bidiagonal QR iteration, computing right
*                 singular vectors of A in A
*                 (CWorkspace: 0)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'L', M, N, 0, 0, S, RWORK( IE ), A, LDA,
     $                         CDUM, 1, CDUM, 1, RWORK( IRWORK ), INFO )
*
               END IF
*
            ELSE IF( WNTVO .AND. WNTUAS ) THEN
*
*              Path 3t(N much larger than M, JOBU='S' or 'A', JOBVT='O')
*              M right singular vectors to be overwritten on A and
*              M left singular vectors to be computed in U
*
               IF( LWORK.GE.M*M+3*M ) THEN
*
*                 Sufficient workspace for a fast algorithm
*
                  IR = 1
                  IF( LWORK.GE.MAX( WRKBL, LDA*N )+LDA*M ) THEN
*
*                    WORK(IU) is LDA by N and WORK(IR) is LDA by M
*
                     LDWRKU = LDA
                     CHUNK = N
                     LDWRKR = LDA
                  ELSE IF( LWORK.GE.MAX( WRKBL, LDA*N )+M*M ) THEN
*
*                    WORK(IU) is LDA by N and WORK(IR) is M by M
*
                     LDWRKU = LDA
                     CHUNK = N
                     LDWRKR = M
                  ELSE
*
*                    WORK(IU) is M by CHUNK and WORK(IR) is M by M
*
                     LDWRKU = M
                     CHUNK = ( LWORK-M*M ) / M
                     LDWRKR = M
                  END IF
                  ITAU = IR + LDWRKR*M
                  IWORK = ITAU + M
*
*                 Compute A=L*Q
*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Copy L to U, zeroing about above it
*
                  CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
                  CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO, U( 1, 2 ),
     $                         LDU )
*
*                 Generate Q in A
*                 (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IE = 1
                  ITAUQ = ITAU
                  ITAUP = ITAUQ + M
                  IWORK = ITAUP + M
*
*                 Bidiagonalize L in U, copying result to WORK(IR)
*                 (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB)
*                 (RWorkspace: need M)
*
                  CALL ZGEBRD( M, M, U, LDU, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  CALL ZLACPY( 'U', M, M, U, LDU, WORK( IR ), LDWRKR )
*
*                 Generate right vectors bidiagonalizing L in WORK(IR)
*                 (CWorkspace: need M*M+3*M-1, prefer M*M+2*M+(M-1)*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'P', M, M, M, WORK( IR ), LDWRKR,
     $                         WORK( ITAUP ), WORK( IWORK ),
     $                         LWORK-IWORK+1, IERR )
*
*                 Generate left vectors bidiagonalizing L in U
*                 (CWorkspace: need M*M+3*M, prefer M*M+2*M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'Q', M, M, M, U, LDU, WORK( ITAUQ ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IRWORK = IE + M
*
*                 Perform bidiagonal QR iteration, computing left
*                 singular vectors of L in U, and computing right
*                 singular vectors of L in WORK(IR)
*                 (CWorkspace: need M*M)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', M, M, M, 0, S, RWORK( IE ),
     $                         WORK( IR ), LDWRKR, U, LDU, CDUM, 1,
     $                         RWORK( IRWORK ), INFO )
                  IU = ITAUQ
*
*                 Multiply right singular vectors of L in WORK(IR) by Q
*                 in A, storing result in WORK(IU) and copying to A
*                 (CWorkspace: need M*M+M, prefer M*M+M*N))
*                 (RWorkspace: 0)
*
                  DO 40 I = 1, N, CHUNK
                     BLK = MIN( N-I+1, CHUNK )
                     CALL ZGEMM( 'N', 'N', M, BLK, M, CONE, WORK( IR ),
     $                           LDWRKR, A( 1, I ), LDA, CZERO,
     $                           WORK( IU ), LDWRKU )
                     CALL ZLACPY( 'F', M, BLK, WORK( IU ), LDWRKU,
     $                            A( 1, I ), LDA )
   40             CONTINUE
*
               ELSE
*
*                 Insufficient workspace for a fast algorithm
*
                  ITAU = 1
                  IWORK = ITAU + M
*
*                 Compute A=L*Q
*                 (CWorkspace: need 2*M, prefer M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Copy L to U, zeroing out above it
*
                  CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
                  CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO, U( 1, 2 ),
     $                         LDU )
*
*                 Generate Q in A
*                 (CWorkspace: need 2*M, prefer M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IE = 1
                  ITAUQ = ITAU
                  ITAUP = ITAUQ + M
                  IWORK = ITAUP + M
*
*                 Bidiagonalize L in U
*                 (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                 (RWorkspace: need M)
*
                  CALL ZGEBRD( M, M, U, LDU, S, RWORK( IE ),
     $                         WORK( ITAUQ ), WORK( ITAUP ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                 Multiply right vectors bidiagonalizing L by Q in A
*                 (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNMBR( 'P', 'L', 'C', M, N, M, U, LDU,
     $                         WORK( ITAUP ), A, LDA, WORK( IWORK ),
     $                         LWORK-IWORK+1, IERR )
*
*                 Generate left vectors bidiagonalizing L in U
*                 (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                 (RWorkspace: 0)
*
                  CALL ZUNGBR( 'Q', M, M, M, U, LDU, WORK( ITAUQ ),
     $                         WORK( IWORK ), LWORK-IWORK+1, IERR )
                  IRWORK = IE + M
*
*                 Perform bidiagonal QR iteration, computing left
*                 singular vectors of A in U and computing right
*                 singular vectors of A in A
*                 (CWorkspace: 0)
*                 (RWorkspace: need BDSPAC)
*
                  CALL ZBDSQR( 'U', M, N, M, 0, S, RWORK( IE ), A, LDA,
     $                         U, LDU, CDUM, 1, RWORK( IRWORK ), INFO )
*
               END IF
*
            ELSE IF( WNTVS ) THEN
*
               IF( WNTUN ) THEN
*
*                 Path 4t(N much larger than M, JOBU='N', JOBVT='S')
*                 M right singular vectors to be computed in VT and
*                 no left singular vectors to be computed
*
                  IF( LWORK.GE.M*M+3*M ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IR = 1
                     IF( LWORK.GE.WRKBL+LDA*M ) THEN
*
*                       WORK(IR) is LDA by M
*
                        LDWRKR = LDA
                     ELSE
*
*                       WORK(IR) is M by M
*
                        LDWRKR = M
                     END IF
                     ITAU = IR + LDWRKR*M
                     IWORK = ITAU + M
*
*                    Compute A=L*Q
*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to WORK(IR), zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, WORK( IR ),
     $                            LDWRKR )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            WORK( IR+LDWRKR ), LDWRKR )
*
*                    Generate Q in A
*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in WORK(IR)
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, WORK( IR ), LDWRKR, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right vectors bidiagonalizing L in
*                    WORK(IR)
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+(M-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', M, M, M, WORK( IR ), LDWRKR,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing right
*                    singular vectors of L in WORK(IR)
*                    (CWorkspace: need M*M)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, M, 0, 0, S, RWORK( IE ),
     $                            WORK( IR ), LDWRKR, CDUM, 1, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply right singular vectors of L in WORK(IR) by
*                    Q in A, storing result in VT
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IR ),
     $                           LDWRKR, A, LDA, CZERO, VT, LDVT )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + M
*
*                    Compute A=L*Q
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy result to VT
*
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( M, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Zero out above L in A
*
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            A( 1, 2 ), LDA )
*
*                    Bidiagonalize L in A
*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply right vectors bidiagonalizing L by Q in VT
*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'P', 'L', 'C', M, N, M, A, LDA,
     $                            WORK( ITAUP ), VT, LDVT,
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, N, 0, 0, S, RWORK( IE ), VT,
     $                            LDVT, CDUM, 1, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               ELSE IF( WNTUO ) THEN
*
*                 Path 5t(N much larger than M, JOBU='O', JOBVT='S')
*                 M right singular vectors to be computed in VT and
*                 M left singular vectors to be overwritten on A
*
                  IF( LWORK.GE.2*M*M+3*M ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+2*LDA*M ) THEN
*
*                       WORK(IU) is LDA by M and WORK(IR) is LDA by M
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*M
                        LDWRKR = LDA
                     ELSE IF( LWORK.GE.WRKBL+( LDA+M )*M ) THEN
*
*                       WORK(IU) is LDA by M and WORK(IR) is M by M
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*M
                        LDWRKR = M
                     ELSE
*
*                       WORK(IU) is M by M and WORK(IR) is M by M
*
                        LDWRKU = M
                        IR = IU + LDWRKU*M
                        LDWRKR = M
                     END IF
                     ITAU = IR + LDWRKR*M
                     IWORK = ITAU + M
*
*                    Compute A=L*Q
*                    (CWorkspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to WORK(IU), zeroing out below it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            WORK( IU+LDWRKU ), LDWRKU )
*
*                    Generate Q in A
*                    (CWorkspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in WORK(IU), copying result to
*                    WORK(IR)
*                    (CWorkspace: need   2*M*M+3*M,
*                                 prefer 2*M*M+2*M+2*M*NB)
*                    (RWorkspace: need   M)
*
                     CALL ZGEBRD( M, M, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, M, WORK( IU ), LDWRKU,
     $                            WORK( IR ), LDWRKR )
*
*                    Generate right bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need   2*M*M+3*M-1,
*                                 prefer 2*M*M+2*M+(M-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', M, M, M, WORK( IU ), LDWRKU,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in WORK(IR)
*                    (CWorkspace: need 2*M*M+3*M, prefer 2*M*M+2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, WORK( IR ), LDWRKR,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of L in WORK(IR) and computing
*                    right singular vectors of L in WORK(IU)
*                    (CWorkspace: need 2*M*M)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, M, M, 0, S, RWORK( IE ),
     $                            WORK( IU ), LDWRKU, WORK( IR ),
     $                            LDWRKR, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
*                    Multiply right singular vectors of L in WORK(IU) by
*                    Q in A, storing result in VT
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IU ),
     $                           LDWRKU, A, LDA, CZERO, VT, LDVT )
*
*                    Copy left singular vectors of L to A
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZLACPY( 'F', M, M, WORK( IR ), LDWRKR, A,
     $                            LDA )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( M, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Zero out above L in A
*
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            A( 1, 2 ), LDA )
*
*                    Bidiagonalize L in A
*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply right vectors bidiagonalizing L by Q in VT
*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'P', 'L', 'C', M, N, M, A, LDA,
     $                            WORK( ITAUP ), VT, LDVT,
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors of L in A
*                    (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, A, LDA, WORK( ITAUQ ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in A and computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, N, M, 0, S, RWORK( IE ), VT,
     $                            LDVT, A, LDA, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               ELSE IF( WNTUAS ) THEN
*
*                 Path 6t(N much larger than M, JOBU='S' or 'A',
*                         JOBVT='S')
*                 M right singular vectors to be computed in VT and
*                 M left singular vectors to be computed in U
*
                  IF( LWORK.GE.M*M+3*M ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+LDA*M ) THEN
*
*                       WORK(IU) is LDA by N
*
                        LDWRKU = LDA
                     ELSE
*
*                       WORK(IU) is LDA by M
*
                        LDWRKU = M
                     END IF
                     ITAU = IU + LDWRKU*M
                     IWORK = ITAU + M
*
*                    Compute A=L*Q
*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to WORK(IU), zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            WORK( IU+LDWRKU ), LDWRKU )
*
*                    Generate Q in A
*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in WORK(IU), copying result to U
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, M, WORK( IU ), LDWRKU, U,
     $                            LDU )
*
*                    Generate right bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need   M*M+3*M-1,
*                                 prefer M*M+2*M+(M-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', M, M, M, WORK( IU ), LDWRKU,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in U
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, U, LDU, WORK( ITAUQ ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of L in U and computing right
*                    singular vectors of L in WORK(IU)
*                    (CWorkspace: need M*M)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, M, M, 0, S, RWORK( IE ),
     $                            WORK( IU ), LDWRKU, U, LDU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply right singular vectors of L in WORK(IU) by
*                    Q in A, storing result in VT
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IU ),
     $                           LDWRKU, A, LDA, CZERO, VT, LDVT )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( M, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to U, zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            U( 1, 2 ), LDU )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in U
*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, U, LDU, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply right bidiagonalizing vectors in U by Q
*                    in VT
*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'P', 'L', 'C', M, N, M, U, LDU,
     $                            WORK( ITAUP ), VT, LDVT,
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in U
*                    (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, U, LDU, WORK( ITAUQ ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U and computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, N, M, 0, S, RWORK( IE ), VT,
     $                            LDVT, U, LDU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               END IF
*
            ELSE IF( WNTVA ) THEN
*
               IF( WNTUN ) THEN
*
*                 Path 7t(N much larger than M, JOBU='N', JOBVT='A')
*                 N right singular vectors to be computed in VT and
*                 no left singular vectors to be computed
*
                  IF( LWORK.GE.M*M+MAX( N+M, 3*M ) ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IR = 1
                     IF( LWORK.GE.WRKBL+LDA*M ) THEN
*
*                       WORK(IR) is LDA by M
*
                        LDWRKR = LDA
                     ELSE
*
*                       WORK(IR) is M by M
*
                        LDWRKR = M
                     END IF
                     ITAU = IR + LDWRKR*M
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Copy L to WORK(IR), zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, WORK( IR ),
     $                            LDWRKR )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            WORK( IR+LDWRKR ), LDWRKR )
*
*                    Generate Q in VT
*                    (CWorkspace: need M*M+M+N, prefer M*M+M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in WORK(IR)
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, WORK( IR ), LDWRKR, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate right bidiagonalizing vectors in WORK(IR)
*                    (CWorkspace: need   M*M+3*M-1,
*                                 prefer M*M+2*M+(M-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', M, M, M, WORK( IR ), LDWRKR,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing right
*                    singular vectors of L in WORK(IR)
*                    (CWorkspace: need M*M)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, M, 0, 0, S, RWORK( IE ),
     $                            WORK( IR ), LDWRKR, CDUM, 1, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply right singular vectors of L in WORK(IR) by
*                    Q in VT, storing result in A
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IR ),
     $                           LDWRKR, VT, LDVT, CZERO, A, LDA )
*
*                    Copy right singular vectors of A from A to VT
*
                     CALL ZLACPY( 'F', M, N, A, LDA, VT, LDVT )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need M+N, prefer M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Zero out above L in A
*
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            A( 1, 2 ), LDA )
*
*                    Bidiagonalize L in A
*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply right bidiagonalizing vectors in A by Q
*                    in VT
*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'P', 'L', 'C', M, N, M, A, LDA,
     $                            WORK( ITAUP ), VT, LDVT,
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, N, 0, 0, S, RWORK( IE ), VT,
     $                            LDVT, CDUM, 1, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               ELSE IF( WNTUO ) THEN
*
*                 Path 8t(N much larger than M, JOBU='O', JOBVT='A')
*                 N right singular vectors to be computed in VT and
*                 M left singular vectors to be overwritten on A
*
                  IF( LWORK.GE.2*M*M+MAX( N+M, 3*M ) ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+2*LDA*M ) THEN
*
*                       WORK(IU) is LDA by M and WORK(IR) is LDA by M
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*M
                        LDWRKR = LDA
                     ELSE IF( LWORK.GE.WRKBL+( LDA+M )*M ) THEN
*
*                       WORK(IU) is LDA by M and WORK(IR) is M by M
*
                        LDWRKU = LDA
                        IR = IU + LDWRKU*M
                        LDWRKR = M
                     ELSE
*
*                       WORK(IU) is M by M and WORK(IR) is M by M
*
                        LDWRKU = M
                        IR = IU + LDWRKU*M
                        LDWRKR = M
                     END IF
                     ITAU = IR + LDWRKR*M
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need 2*M*M+2*M, prefer 2*M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need 2*M*M+M+N, prefer 2*M*M+M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to WORK(IU), zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            WORK( IU+LDWRKU ), LDWRKU )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in WORK(IU), copying result to
*                    WORK(IR)
*                    (CWorkspace: need   2*M*M+3*M,
*                                 prefer 2*M*M+2*M+2*M*NB)
*                    (RWorkspace: need   M)
*
                     CALL ZGEBRD( M, M, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, M, WORK( IU ), LDWRKU,
     $                            WORK( IR ), LDWRKR )
*
*                    Generate right bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need   2*M*M+3*M-1,
*                                 prefer 2*M*M+2*M+(M-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', M, M, M, WORK( IU ), LDWRKU,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in WORK(IR)
*                    (CWorkspace: need 2*M*M+3*M, prefer 2*M*M+2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, WORK( IR ), LDWRKR,
     $                            WORK( ITAUQ ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of L in WORK(IR) and computing
*                    right singular vectors of L in WORK(IU)
*                    (CWorkspace: need 2*M*M)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, M, M, 0, S, RWORK( IE ),
     $                            WORK( IU ), LDWRKU, WORK( IR ),
     $                            LDWRKR, CDUM, 1, RWORK( IRWORK ),
     $                            INFO )
*
*                    Multiply right singular vectors of L in WORK(IU) by
*                    Q in VT, storing result in A
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IU ),
     $                           LDWRKU, VT, LDVT, CZERO, A, LDA )
*
*                    Copy right singular vectors of A from A to VT
*
                     CALL ZLACPY( 'F', M, N, A, LDA, VT, LDVT )
*
*                    Copy left singular vectors of A from WORK(IR) to A
*
                     CALL ZLACPY( 'F', M, M, WORK( IR ), LDWRKR, A,
     $                            LDA )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need M+N, prefer M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Zero out above L in A
*
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            A( 1, 2 ), LDA )
*
*                    Bidiagonalize L in A
*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply right bidiagonalizing vectors in A by Q
*                    in VT
*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'P', 'L', 'C', M, N, M, A, LDA,
     $                            WORK( ITAUP ), VT, LDVT,
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in A
*                    (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, A, LDA, WORK( ITAUQ ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in A and computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, N, M, 0, S, RWORK( IE ), VT,
     $                            LDVT, A, LDA, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               ELSE IF( WNTUAS ) THEN
*
*                 Path 9t(N much larger than M, JOBU='S' or 'A',
*                         JOBVT='A')
*                 N right singular vectors to be computed in VT and
*                 M left singular vectors to be computed in U
*
                  IF( LWORK.GE.M*M+MAX( N+M, 3*M ) ) THEN
*
*                    Sufficient workspace for a fast algorithm
*
                     IU = 1
                     IF( LWORK.GE.WRKBL+LDA*M ) THEN
*
*                       WORK(IU) is LDA by M
*
                        LDWRKU = LDA
                     ELSE
*
*                       WORK(IU) is M by M
*
                        LDWRKU = M
                     END IF
                     ITAU = IU + LDWRKU*M
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need M*M+2*M, prefer M*M+M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need M*M+M+N, prefer M*M+M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to WORK(IU), zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, WORK( IU ),
     $                            LDWRKU )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            WORK( IU+LDWRKU ), LDWRKU )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in WORK(IU), copying result to U
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, WORK( IU ), LDWRKU, S,
     $                            RWORK( IE ), WORK( ITAUQ ),
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'L', M, M, WORK( IU ), LDWRKU, U,
     $                            LDU )
*
*                    Generate right bidiagonalizing vectors in WORK(IU)
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+(M-1)*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'P', M, M, M, WORK( IU ), LDWRKU,
     $                            WORK( ITAUP ), WORK( IWORK ),
     $                            LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in U
*                    (CWorkspace: need M*M+3*M, prefer M*M+2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, U, LDU, WORK( ITAUQ ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of L in U and computing right
*                    singular vectors of L in WORK(IU)
*                    (CWorkspace: need M*M)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, M, M, 0, S, RWORK( IE ),
     $                            WORK( IU ), LDWRKU, U, LDU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
*                    Multiply right singular vectors of L in WORK(IU) by
*                    Q in VT, storing result in A
*                    (CWorkspace: need M*M)
*                    (RWorkspace: 0)
*
                     CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IU ),
     $                           LDWRKU, VT, LDVT, CZERO, A, LDA )
*
*                    Copy right singular vectors of A from A to VT
*
                     CALL ZLACPY( 'F', M, N, A, LDA, VT, LDVT )
*
                  ELSE
*
*                    Insufficient workspace for a fast algorithm
*
                     ITAU = 1
                     IWORK = ITAU + M
*
*                    Compute A=L*Q, copying result to VT
*                    (CWorkspace: need 2*M, prefer M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZGELQF( M, N, A, LDA, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*                    Generate Q in VT
*                    (CWorkspace: need M+N, prefer M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Copy L to U, zeroing out above it
*
                     CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
                     CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                            U( 1, 2 ), LDU )
                     IE = 1
                     ITAUQ = ITAU
                     ITAUP = ITAUQ + M
                     IWORK = ITAUP + M
*
*                    Bidiagonalize L in U
*                    (CWorkspace: need 3*M, prefer 2*M+2*M*NB)
*                    (RWorkspace: need M)
*
                     CALL ZGEBRD( M, M, U, LDU, S, RWORK( IE ),
     $                            WORK( ITAUQ ), WORK( ITAUP ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Multiply right bidiagonalizing vectors in U by Q
*                    in VT
*                    (CWorkspace: need 2*M+N, prefer 2*M+N*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNMBR( 'P', 'L', 'C', M, N, M, U, LDU,
     $                            WORK( ITAUP ), VT, LDVT,
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
*
*                    Generate left bidiagonalizing vectors in U
*                    (CWorkspace: need 3*M, prefer 2*M+M*NB)
*                    (RWorkspace: 0)
*
                     CALL ZUNGBR( 'Q', M, M, M, U, LDU, WORK( ITAUQ ),
     $                            WORK( IWORK ), LWORK-IWORK+1, IERR )
                     IRWORK = IE + M
*
*                    Perform bidiagonal QR iteration, computing left
*                    singular vectors of A in U and computing right
*                    singular vectors of A in VT
*                    (CWorkspace: 0)
*                    (RWorkspace: need BDSPAC)
*
                     CALL ZBDSQR( 'U', M, N, M, 0, S, RWORK( IE ), VT,
     $                            LDVT, U, LDU, CDUM, 1,
     $                            RWORK( IRWORK ), INFO )
*
                  END IF
*
               END IF
*
            END IF
*
         ELSE
*
*           N .LT. MNTHR
*
*           Path 10t(N greater than M, but not much larger)
*           Reduce to bidiagonal form without LQ decomposition
*
            IE = 1
            ITAUQ = 1
            ITAUP = ITAUQ + M
            IWORK = ITAUP + M
*
*           Bidiagonalize A
*           (CWorkspace: need 2*M+N, prefer 2*M+(M+N)*NB)
*           (RWorkspace: M)
*
            CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                   WORK( ITAUP ), WORK( IWORK ), LWORK-IWORK+1,
     $                   IERR )
            IF( WNTUAS ) THEN
*
*              If left singular vectors desired in U, copy result to U
*              and generate left bidiagonalizing vectors in U
*              (CWorkspace: need 3*M-1, prefer 2*M+(M-1)*NB)
*              (RWorkspace: 0)
*
               CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
               CALL ZUNGBR( 'Q', M, M, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IF( WNTVAS ) THEN
*
*              If right singular vectors desired in VT, copy result to
*              VT and generate right bidiagonalizing vectors in VT
*              (CWorkspace: need 2*M+NRVT, prefer 2*M+NRVT*NB)
*              (RWorkspace: 0)
*
               CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
               IF( WNTVA )
     $            NRVT = N
               IF( WNTVS )
     $            NRVT = M
               CALL ZUNGBR( 'P', NRVT, N, M, VT, LDVT, WORK( ITAUP ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IF( WNTUO ) THEN
*
*              If left singular vectors desired in A, generate left
*              bidiagonalizing vectors in A
*              (CWorkspace: need 3*M-1, prefer 2*M+(M-1)*NB)
*              (RWorkspace: 0)
*
               CALL ZUNGBR( 'Q', M, M, N, A, LDA, WORK( ITAUQ ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IF( WNTVO ) THEN
*
*              If right singular vectors desired in A, generate right
*              bidiagonalizing vectors in A
*              (CWorkspace: need 3*M, prefer 2*M+M*NB)
*              (RWorkspace: 0)
*
               CALL ZUNGBR( 'P', M, N, M, A, LDA, WORK( ITAUP ),
     $                      WORK( IWORK ), LWORK-IWORK+1, IERR )
            END IF
            IRWORK = IE + M
            IF( WNTUAS .OR. WNTUO )
     $         NRU = M
            IF( WNTUN )
     $         NRU = 0
            IF( WNTVAS .OR. WNTVO )
     $         NCVT = N
            IF( WNTVN )
     $         NCVT = 0
            IF( ( .NOT.WNTUO ) .AND. ( .NOT.WNTVO ) ) THEN
*
*              Perform bidiagonal QR iteration, if desired, computing
*              left singular vectors in U and computing right singular
*              vectors in VT
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'L', M, NCVT, NRU, 0, S, RWORK( IE ), VT,
     $                      LDVT, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                      INFO )
            ELSE IF( ( .NOT.WNTUO ) .AND. WNTVO ) THEN
*
*              Perform bidiagonal QR iteration, if desired, computing
*              left singular vectors in U and computing right singular
*              vectors in A
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'L', M, NCVT, NRU, 0, S, RWORK( IE ), A,
     $                      LDA, U, LDU, CDUM, 1, RWORK( IRWORK ),
     $                      INFO )
            ELSE
*
*              Perform bidiagonal QR iteration, if desired, computing
*              left singular vectors in A and computing right singular
*              vectors in VT
*              (CWorkspace: 0)
*              (RWorkspace: need BDSPAC)
*
               CALL ZBDSQR( 'L', M, NCVT, NRU, 0, S, RWORK( IE ), VT,
     $                      LDVT, A, LDA, CDUM, 1, RWORK( IRWORK ),
     $                      INFO )
            END IF
*
         END IF
*
      END IF
*
*     Undo scaling if necessary
*
      IF( ISCL.EQ.1 ) THEN
         IF( ANRM.GT.BIGNUM )
     $      CALL DLASCL( 'G', 0, 0, BIGNUM, ANRM, MINMN, 1, S, MINMN,
     $                   IERR )
         IF( INFO.NE.0 .AND. ANRM.GT.BIGNUM )
     $      CALL DLASCL( 'G', 0, 0, BIGNUM, ANRM, MINMN-1, 1,
     $                   RWORK( IE ), MINMN, IERR )
         IF( ANRM.LT.SMLNUM )
     $      CALL DLASCL( 'G', 0, 0, SMLNUM, ANRM, MINMN, 1, S, MINMN,
     $                   IERR )
         IF( INFO.NE.0 .AND. ANRM.LT.SMLNUM )
     $      CALL DLASCL( 'G', 0, 0, SMLNUM, ANRM, MINMN-1, 1,
     $                   RWORK( IE ), MINMN, IERR )
      END IF
*
*     Return optimal workspace in WORK(1)
*
      WORK( 1 ) = MAXWRK
*
      RETURN
*
*     End of ZGESVD
*
      END
