*> \brief \b ZGESDD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGESDD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgesdd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgesdd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgesdd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGESDD( JOBZ, M, N, A, LDA, S, U, LDU, VT, LDVT,
*                          WORK, LWORK, RWORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBZ
*       INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
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
*> ZGESDD computes the singular value decomposition (SVD) of a complex
*> M-by-N matrix A, optionally computing the left and/or right singular
*> vectors, by using divide-and-conquer method. The SVD is written
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
*> Note that the routine returns VT = V**H, not V.
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
*> \param[in] JOBZ
*> \verbatim
*>          JOBZ is CHARACTER*1
*>          Specifies options for computing all or part of the matrix U:
*>          = 'A':  all M columns of U and all N rows of V**H are
*>                  returned in the arrays U and VT;
*>          = 'S':  the first min(M,N) columns of U and the first
*>                  min(M,N) rows of V**H are returned in the arrays U
*>                  and VT;
*>          = 'O':  If M >= N, the first N columns of U are overwritten
*>                  in the array A and all rows of V**H are returned in
*>                  the array VT;
*>                  otherwise, all columns of U are returned in the
*>                  array U and the first M rows of V**H are overwritten
*>                  in the array A;
*>          = 'N':  no columns of U or rows of V**H are computed.
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
*>          if JOBZ = 'O',  A is overwritten with the first N columns
*>                          of U (the left singular vectors, stored
*>                          columnwise) if M >= N;
*>                          A is overwritten with the first M rows
*>                          of V**H (the right singular vectors, stored
*>                          rowwise) otherwise.
*>          if JOBZ .ne. 'O', the contents of A are destroyed.
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
*>          UCOL = M if JOBZ = 'A' or JOBZ = 'O' and M < N;
*>          UCOL = min(M,N) if JOBZ = 'S'.
*>          If JOBZ = 'A' or JOBZ = 'O' and M < N, U contains the M-by-M
*>          unitary matrix U;
*>          if JOBZ = 'S', U contains the first min(M,N) columns of U
*>          (the left singular vectors, stored columnwise);
*>          if JOBZ = 'O' and M >= N, or JOBZ = 'N', U is not referenced.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U.  LDU >= 1;
*>          if JOBZ = 'S' or 'A' or JOBZ = 'O' and M < N, LDU >= M.
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is COMPLEX*16 array, dimension (LDVT,N)
*>          If JOBZ = 'A' or JOBZ = 'O' and M >= N, VT contains the
*>          N-by-N unitary matrix V**H;
*>          if JOBZ = 'S', VT contains the first min(M,N) rows of
*>          V**H (the right singular vectors, stored rowwise);
*>          if JOBZ = 'O' and M < N, or JOBZ = 'N', VT is not referenced.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>          The leading dimension of the array VT.  LDVT >= 1;
*>          if JOBZ = 'A' or JOBZ = 'O' and M >= N, LDVT >= N;
*>          if JOBZ = 'S', LDVT >= min(M,N).
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
*>          The dimension of the array WORK. LWORK >= 1.
*>          If LWORK = -1, a workspace query is assumed.  The optimal
*>          size for the WORK array is calculated and stored in WORK(1),
*>          and no other work except argument checking is performed.
*>
*>          Let mx = max(M,N) and mn = min(M,N).
*>          If JOBZ = 'N', LWORK >= 2*mn + mx.
*>          If JOBZ = 'O', LWORK >= 2*mn*mn + 2*mn + mx.
*>          If JOBZ = 'S', LWORK >=   mn*mn + 3*mn.
*>          If JOBZ = 'A', LWORK >=   mn*mn + 2*mn + mx.
*>          These are not tight minimums in all cases; see comments inside code.
*>          For good performance, LWORK should generally be larger;
*>          a query is recommended.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (MAX(1,LRWORK))
*>          Let mx = max(M,N) and mn = min(M,N).
*>          If JOBZ = 'N',    LRWORK >= 5*mn (LAPACK <= 3.6 needs 7*mn);
*>          else if mx >> mn, LRWORK >= 5*mn*mn + 5*mn;
*>          else              LRWORK >= max( 5*mn*mn + 5*mn,
*>                                           2*mx*mn + 2*mn*mn + mn ).
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (8*min(M,N))
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  The updating process of DBDSDC did not converge.
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
*> \ingroup complex16GEsing
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Huan Ren, Computer Science Division, University of
*>     California at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE ZGESDD( JOBZ, M, N, A, LDA, S, U, LDU, VT, LDVT,
     $                   WORK, LWORK, RWORK, IWORK, INFO )
      implicit none
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBZ
      INTEGER            INFO, LDA, LDU, LDVT, LWORK, M, N
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   RWORK( * ), S( * )
      COMPLEX*16         A( LDA, * ), U( LDU, * ), VT( LDVT, * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, WNTQA, WNTQAS, WNTQN, WNTQO, WNTQS
      INTEGER            BLK, CHUNK, I, IE, IERR, IL, IR, IRU, IRVT,
     $                   ISCL, ITAU, ITAUP, ITAUQ, IU, IVT, LDWKVT,
     $                   LDWRKL, LDWRKR, LDWRKU, MAXWRK, MINMN, MINWRK,
     $                   MNTHR1, MNTHR2, NRWORK, NWORK, WRKBL
      INTEGER            LWORK_ZGEBRD_MN, LWORK_ZGEBRD_MM,
     $                   LWORK_ZGEBRD_NN, LWORK_ZGELQF_MN,
     $                   LWORK_ZGEQRF_MN,
     $                   LWORK_ZUNGBR_P_MN, LWORK_ZUNGBR_P_NN,
     $                   LWORK_ZUNGBR_Q_MN, LWORK_ZUNGBR_Q_MM,
     $                   LWORK_ZUNGLQ_MN, LWORK_ZUNGLQ_NN,
     $                   LWORK_ZUNGQR_MM, LWORK_ZUNGQR_MN,
     $                   LWORK_ZUNMBR_PRC_MM, LWORK_ZUNMBR_QLN_MM,
     $                   LWORK_ZUNMBR_PRC_MN, LWORK_ZUNMBR_QLN_MN,
     $                   LWORK_ZUNMBR_PRC_NN, LWORK_ZUNMBR_QLN_NN
      DOUBLE PRECISION   ANRM, BIGNUM, EPS, SMLNUM
*     ..
*     .. Local Arrays ..
      INTEGER            IDUM( 1 )
      DOUBLE PRECISION   DUM( 1 )
      COMPLEX*16         CDUM( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DBDSDC, DLASCL, XERBLA, ZGEBRD, ZGELQF, ZGEMM,
     $                   ZGEQRF, ZLACP2, ZLACPY, ZLACRM, ZLARCM, ZLASCL,
     $                   ZLASET, ZUNGBR, ZUNGLQ, ZUNGQR, ZUNMBR
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           LSAME, DLAMCH, ZLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          INT, MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO   = 0
      MINMN  = MIN( M, N )
      MNTHR1 = INT( MINMN*17.0D0 / 9.0D0 )
      MNTHR2 = INT( MINMN*5.0D0 / 3.0D0 )
      WNTQA  = LSAME( JOBZ, 'A' )
      WNTQS  = LSAME( JOBZ, 'S' )
      WNTQAS = WNTQA .OR. WNTQS
      WNTQO  = LSAME( JOBZ, 'O' )
      WNTQN  = LSAME( JOBZ, 'N' )
      LQUERY = ( LWORK.EQ.-1 )
      MINWRK = 1
      MAXWRK = 1
*
      IF( .NOT.( WNTQA .OR. WNTQS .OR. WNTQO .OR. WNTQN ) ) THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -5
      ELSE IF( LDU.LT.1 .OR. ( WNTQAS .AND. LDU.LT.M ) .OR.
     $         ( WNTQO .AND. M.LT.N .AND. LDU.LT.M ) ) THEN
         INFO = -8
      ELSE IF( LDVT.LT.1 .OR. ( WNTQA .AND. LDVT.LT.N ) .OR.
     $         ( WNTQS .AND. LDVT.LT.MINMN ) .OR.
     $         ( WNTQO .AND. M.GE.N .AND. LDVT.LT.N ) ) THEN
         INFO = -10
      END IF
*
*     Compute workspace
*       Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace allocated at that point in the code,
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
*           There is no complex work space needed for bidiagonal SVD
*           The real work space needed for bidiagonal SVD (dbdsdc) is
*           BDSPAC = 3*N*N + 4*N for singular values and vectors;
*           BDSPAC = 4*N         for singular values only;
*           not including e, RU, and RVT matrices.
*
*           Compute space preferred for each routine
            CALL ZGEBRD( M, N, CDUM(1), M, DUM(1), DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEBRD_MN = INT( CDUM(1) )
*
            CALL ZGEBRD( N, N, CDUM(1), N, DUM(1), DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEBRD_NN = INT( CDUM(1) )
*
            CALL ZGEQRF( M, N, CDUM(1), M, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEQRF_MN = INT( CDUM(1) )
*
            CALL ZUNGBR( 'P', N, N, N, CDUM(1), N, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGBR_P_NN = INT( CDUM(1) )
*
            CALL ZUNGBR( 'Q', M, M, N, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGBR_Q_MM = INT( CDUM(1) )
*
            CALL ZUNGBR( 'Q', M, N, N, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGBR_Q_MN = INT( CDUM(1) )
*
            CALL ZUNGQR( M, M, N, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGQR_MM = INT( CDUM(1) )
*
            CALL ZUNGQR( M, N, N, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGQR_MN = INT( CDUM(1) )
*
            CALL ZUNMBR( 'P', 'R', 'C', N, N, N, CDUM(1), N, CDUM(1),
     $                   CDUM(1), N, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_PRC_NN = INT( CDUM(1) )
*
            CALL ZUNMBR( 'Q', 'L', 'N', M, M, N, CDUM(1), M, CDUM(1),
     $                   CDUM(1), M, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_QLN_MM = INT( CDUM(1) )
*
            CALL ZUNMBR( 'Q', 'L', 'N', M, N, N, CDUM(1), M, CDUM(1),
     $                   CDUM(1), M, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_QLN_MN = INT( CDUM(1) )
*
            CALL ZUNMBR( 'Q', 'L', 'N', N, N, N, CDUM(1), N, CDUM(1),
     $                   CDUM(1), N, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_QLN_NN = INT( CDUM(1) )
*
            IF( M.GE.MNTHR1 ) THEN
               IF( WNTQN ) THEN
*
*                 Path 1 (M >> N, JOBZ='N')
*
                  MAXWRK = N + LWORK_ZGEQRF_MN
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZGEBRD_NN )
                  MINWRK = 3*N
               ELSE IF( WNTQO ) THEN
*
*                 Path 2 (M >> N, JOBZ='O')
*
                  WRKBL = N + LWORK_ZGEQRF_MN
                  WRKBL = MAX( WRKBL,   N + LWORK_ZUNGQR_MN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZGEBRD_NN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZUNMBR_QLN_NN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZUNMBR_PRC_NN )
                  MAXWRK = M*N + N*N + WRKBL
                  MINWRK = 2*N*N + 3*N
               ELSE IF( WNTQS ) THEN
*
*                 Path 3 (M >> N, JOBZ='S')
*
                  WRKBL = N + LWORK_ZGEQRF_MN
                  WRKBL = MAX( WRKBL,   N + LWORK_ZUNGQR_MN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZGEBRD_NN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZUNMBR_QLN_NN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZUNMBR_PRC_NN )
                  MAXWRK = N*N + WRKBL
                  MINWRK = N*N + 3*N
               ELSE IF( WNTQA ) THEN
*
*                 Path 4 (M >> N, JOBZ='A')
*
                  WRKBL = N + LWORK_ZGEQRF_MN
                  WRKBL = MAX( WRKBL,   N + LWORK_ZUNGQR_MM )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZGEBRD_NN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZUNMBR_QLN_NN )
                  WRKBL = MAX( WRKBL, 2*N + LWORK_ZUNMBR_PRC_NN )
                  MAXWRK = N*N + WRKBL
                  MINWRK = N*N + MAX( 3*N, N + M )
               END IF
            ELSE IF( M.GE.MNTHR2 ) THEN
*
*              Path 5 (M >> N, but not as much as MNTHR1)
*
               MAXWRK = 2*N + LWORK_ZGEBRD_MN
               MINWRK = 2*N + M
               IF( WNTQO ) THEN
*                 Path 5o (M >> N, JOBZ='O')
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR_P_NN )
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR_Q_MN )
                  MAXWRK = MAXWRK + M*N
                  MINWRK = MINWRK + N*N
               ELSE IF( WNTQS ) THEN
*                 Path 5s (M >> N, JOBZ='S')
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR_P_NN )
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR_Q_MN )
               ELSE IF( WNTQA ) THEN
*                 Path 5a (M >> N, JOBZ='A')
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR_P_NN )
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNGBR_Q_MM )
               END IF
            ELSE
*
*              Path 6 (M >= N, but not much larger)
*
               MAXWRK = 2*N + LWORK_ZGEBRD_MN
               MINWRK = 2*N + M
               IF( WNTQO ) THEN
*                 Path 6o (M >= N, JOBZ='O')
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR_PRC_NN )
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR_QLN_MN )
                  MAXWRK = MAXWRK + M*N
                  MINWRK = MINWRK + N*N
               ELSE IF( WNTQS ) THEN
*                 Path 6s (M >= N, JOBZ='S')
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR_QLN_MN )
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR_PRC_NN )
               ELSE IF( WNTQA ) THEN
*                 Path 6a (M >= N, JOBZ='A')
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR_QLN_MM )
                  MAXWRK = MAX( MAXWRK, 2*N + LWORK_ZUNMBR_PRC_NN )
               END IF
            END IF
         ELSE IF( MINMN.GT.0 ) THEN
*
*           There is no complex work space needed for bidiagonal SVD
*           The real work space needed for bidiagonal SVD (dbdsdc) is
*           BDSPAC = 3*M*M + 4*M for singular values and vectors;
*           BDSPAC = 4*M         for singular values only;
*           not including e, RU, and RVT matrices.
*
*           Compute space preferred for each routine
            CALL ZGEBRD( M, N, CDUM(1), M, DUM(1), DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEBRD_MN = INT( CDUM(1) )
*
            CALL ZGEBRD( M, M, CDUM(1), M, DUM(1), DUM(1), CDUM(1),
     $                   CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGEBRD_MM = INT( CDUM(1) )
*
            CALL ZGELQF( M, N, CDUM(1), M, CDUM(1), CDUM(1), -1, IERR )
            LWORK_ZGELQF_MN = INT( CDUM(1) )
*
            CALL ZUNGBR( 'P', M, N, M, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGBR_P_MN = INT( CDUM(1) )
*
            CALL ZUNGBR( 'P', N, N, M, CDUM(1), N, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGBR_P_NN = INT( CDUM(1) )
*
            CALL ZUNGBR( 'Q', M, M, N, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGBR_Q_MM = INT( CDUM(1) )
*
            CALL ZUNGLQ( M, N, M, CDUM(1), M, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGLQ_MN = INT( CDUM(1) )
*
            CALL ZUNGLQ( N, N, M, CDUM(1), N, CDUM(1), CDUM(1),
     $                   -1, IERR )
            LWORK_ZUNGLQ_NN = INT( CDUM(1) )
*
            CALL ZUNMBR( 'P', 'R', 'C', M, M, M, CDUM(1), M, CDUM(1),
     $                   CDUM(1), M, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_PRC_MM = INT( CDUM(1) )
*
            CALL ZUNMBR( 'P', 'R', 'C', M, N, M, CDUM(1), M, CDUM(1),
     $                   CDUM(1), M, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_PRC_MN = INT( CDUM(1) )
*
            CALL ZUNMBR( 'P', 'R', 'C', N, N, M, CDUM(1), N, CDUM(1),
     $                   CDUM(1), N, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_PRC_NN = INT( CDUM(1) )
*
            CALL ZUNMBR( 'Q', 'L', 'N', M, M, M, CDUM(1), M, CDUM(1),
     $                   CDUM(1), M, CDUM(1), -1, IERR )
            LWORK_ZUNMBR_QLN_MM = INT( CDUM(1) )
*
            IF( N.GE.MNTHR1 ) THEN
               IF( WNTQN ) THEN
*
*                 Path 1t (N >> M, JOBZ='N')
*
                  MAXWRK = M + LWORK_ZGELQF_MN
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZGEBRD_MM )
                  MINWRK = 3*M
               ELSE IF( WNTQO ) THEN
*
*                 Path 2t (N >> M, JOBZ='O')
*
                  WRKBL = M + LWORK_ZGELQF_MN
                  WRKBL = MAX( WRKBL,   M + LWORK_ZUNGLQ_MN )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZGEBRD_MM )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZUNMBR_QLN_MM )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZUNMBR_PRC_MM )
                  MAXWRK = M*N + M*M + WRKBL
                  MINWRK = 2*M*M + 3*M
               ELSE IF( WNTQS ) THEN
*
*                 Path 3t (N >> M, JOBZ='S')
*
                  WRKBL = M + LWORK_ZGELQF_MN
                  WRKBL = MAX( WRKBL,   M + LWORK_ZUNGLQ_MN )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZGEBRD_MM )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZUNMBR_QLN_MM )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZUNMBR_PRC_MM )
                  MAXWRK = M*M + WRKBL
                  MINWRK = M*M + 3*M
               ELSE IF( WNTQA ) THEN
*
*                 Path 4t (N >> M, JOBZ='A')
*
                  WRKBL = M + LWORK_ZGELQF_MN
                  WRKBL = MAX( WRKBL,   M + LWORK_ZUNGLQ_NN )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZGEBRD_MM )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZUNMBR_QLN_MM )
                  WRKBL = MAX( WRKBL, 2*M + LWORK_ZUNMBR_PRC_MM )
                  MAXWRK = M*M + WRKBL
                  MINWRK = M*M + MAX( 3*M, M + N )
               END IF
            ELSE IF( N.GE.MNTHR2 ) THEN
*
*              Path 5t (N >> M, but not as much as MNTHR1)
*
               MAXWRK = 2*M + LWORK_ZGEBRD_MN
               MINWRK = 2*M + N
               IF( WNTQO ) THEN
*                 Path 5to (N >> M, JOBZ='O')
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR_Q_MM )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR_P_MN )
                  MAXWRK = MAXWRK + M*N
                  MINWRK = MINWRK + M*M
               ELSE IF( WNTQS ) THEN
*                 Path 5ts (N >> M, JOBZ='S')
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR_Q_MM )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR_P_MN )
               ELSE IF( WNTQA ) THEN
*                 Path 5ta (N >> M, JOBZ='A')
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR_Q_MM )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNGBR_P_NN )
               END IF
            ELSE
*
*              Path 6t (N > M, but not much larger)
*
               MAXWRK = 2*M + LWORK_ZGEBRD_MN
               MINWRK = 2*M + N
               IF( WNTQO ) THEN
*                 Path 6to (N > M, JOBZ='O')
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR_QLN_MM )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR_PRC_MN )
                  MAXWRK = MAXWRK + M*N
                  MINWRK = MINWRK + M*M
               ELSE IF( WNTQS ) THEN
*                 Path 6ts (N > M, JOBZ='S')
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR_QLN_MM )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR_PRC_MN )
               ELSE IF( WNTQA ) THEN
*                 Path 6ta (N > M, JOBZ='A')
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR_QLN_MM )
                  MAXWRK = MAX( MAXWRK, 2*M + LWORK_ZUNMBR_PRC_NN )
               END IF
            END IF
         END IF
         MAXWRK = MAX( MAXWRK, MINWRK )
      END IF
      IF( INFO.EQ.0 ) THEN
         WORK( 1 ) = MAXWRK
         IF( LWORK.LT.MINWRK .AND. .NOT. LQUERY ) THEN
            INFO = -12
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGESDD', -INFO )
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
         IF( M.GE.MNTHR1 ) THEN
*
            IF( WNTQN ) THEN
*
*              Path 1 (M >> N, JOBZ='N')
*              No singular vectors to be computed
*
               ITAU = 1
               NWORK = ITAU + N
*
*              Compute A=Q*R
*              CWorkspace: need   N [tau] + N    [work]
*              CWorkspace: prefer N [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Zero out below R
*
               CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO, A( 2, 1 ),
     $                      LDA )
               IE = 1
               ITAUQ = 1
               ITAUP = ITAUQ + N
               NWORK = ITAUP + N
*
*              Bidiagonalize R in A
*              CWorkspace: need   2*N [tauq, taup] + N      [work]
*              CWorkspace: prefer 2*N [tauq, taup] + 2*N*NB [work]
*              RWorkspace: need   N [e]
*
               CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                      WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                      IERR )
               NRWORK = IE + N
*
*              Perform bidiagonal SVD, compute singular values only
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + BDSPAC
*
               CALL DBDSDC( 'U', 'N', N, S, RWORK( IE ), DUM,1,DUM,1,
     $                      DUM, IDUM, RWORK( NRWORK ), IWORK, INFO )
*
            ELSE IF( WNTQO ) THEN
*
*              Path 2 (M >> N, JOBZ='O')
*              N left singular vectors to be overwritten on A and
*              N right singular vectors to be computed in VT
*
               IU = 1
*
*              WORK(IU) is N by N
*
               LDWRKU = N
               IR = IU + LDWRKU*N
               IF( LWORK .GE. M*N + N*N + 3*N ) THEN
*
*                 WORK(IR) is M by N
*
                  LDWRKR = M
               ELSE
                  LDWRKR = ( LWORK - N*N - 3*N ) / N
               END IF
               ITAU = IR + LDWRKR*N
               NWORK = ITAU + N
*
*              Compute A=Q*R
*              CWorkspace: need   N*N [U] + N*N [R] + N [tau] + N    [work]
*              CWorkspace: prefer N*N [U] + N*N [R] + N [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy R to WORK( IR ), zeroing out below it
*
               CALL ZLACPY( 'U', N, N, A, LDA, WORK( IR ), LDWRKR )
               CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO, WORK( IR+1 ),
     $                      LDWRKR )
*
*              Generate Q in A
*              CWorkspace: need   N*N [U] + N*N [R] + N [tau] + N    [work]
*              CWorkspace: prefer N*N [U] + N*N [R] + N [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
               IE = 1
               ITAUQ = ITAU
               ITAUP = ITAUQ + N
               NWORK = ITAUP + N
*
*              Bidiagonalize R in WORK(IR)
*              CWorkspace: need   N*N [U] + N*N [R] + 2*N [tauq, taup] + N      [work]
*              CWorkspace: prefer N*N [U] + N*N [R] + 2*N [tauq, taup] + 2*N*NB [work]
*              RWorkspace: need   N [e]
*
               CALL ZGEBRD( N, N, WORK( IR ), LDWRKR, S, RWORK( IE ),
     $                      WORK( ITAUQ ), WORK( ITAUP ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of R in WORK(IRU) and computing right singular vectors
*              of R in WORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               IRU = IE + N
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix WORK(IU)
*              Overwrite WORK(IU) by the left singular vectors of R
*              CWorkspace: need   N*N [U] + N*N [R] + 2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer N*N [U] + N*N [R] + 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', N, N, RWORK( IRU ), N, WORK( IU ),
     $                      LDWRKU )
               CALL ZUNMBR( 'Q', 'L', 'N', N, N, N, WORK( IR ), LDWRKR,
     $                      WORK( ITAUQ ), WORK( IU ), LDWRKU,
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by the right singular vectors of R
*              CWorkspace: need   N*N [U] + N*N [R] + 2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer N*N [U] + N*N [R] + 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', N, N, RWORK( IRVT ), N, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, N, WORK( IR ), LDWRKR,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Multiply Q in A by left singular vectors of R in
*              WORK(IU), storing result in WORK(IR) and copying to A
*              CWorkspace: need   N*N [U] + N*N [R]
*              CWorkspace: prefer N*N [U] + M*N [R]
*              RWorkspace: need   0
*
               DO 10 I = 1, M, LDWRKR
                  CHUNK = MIN( M-I+1, LDWRKR )
                  CALL ZGEMM( 'N', 'N', CHUNK, N, N, CONE, A( I, 1 ),
     $                        LDA, WORK( IU ), LDWRKU, CZERO,
     $                        WORK( IR ), LDWRKR )
                  CALL ZLACPY( 'F', CHUNK, N, WORK( IR ), LDWRKR,
     $                         A( I, 1 ), LDA )
   10          CONTINUE
*
            ELSE IF( WNTQS ) THEN
*
*              Path 3 (M >> N, JOBZ='S')
*              N left singular vectors to be computed in U and
*              N right singular vectors to be computed in VT
*
               IR = 1
*
*              WORK(IR) is N by N
*
               LDWRKR = N
               ITAU = IR + LDWRKR*N
               NWORK = ITAU + N
*
*              Compute A=Q*R
*              CWorkspace: need   N*N [R] + N [tau] + N    [work]
*              CWorkspace: prefer N*N [R] + N [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy R to WORK(IR), zeroing out below it
*
               CALL ZLACPY( 'U', N, N, A, LDA, WORK( IR ), LDWRKR )
               CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO, WORK( IR+1 ),
     $                      LDWRKR )
*
*              Generate Q in A
*              CWorkspace: need   N*N [R] + N [tau] + N    [work]
*              CWorkspace: prefer N*N [R] + N [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGQR( M, N, N, A, LDA, WORK( ITAU ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
               IE = 1
               ITAUQ = ITAU
               ITAUP = ITAUQ + N
               NWORK = ITAUP + N
*
*              Bidiagonalize R in WORK(IR)
*              CWorkspace: need   N*N [R] + 2*N [tauq, taup] + N      [work]
*              CWorkspace: prefer N*N [R] + 2*N [tauq, taup] + 2*N*NB [work]
*              RWorkspace: need   N [e]
*
               CALL ZGEBRD( N, N, WORK( IR ), LDWRKR, S, RWORK( IE ),
     $                      WORK( ITAUQ ), WORK( ITAUP ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               IRU = IE + N
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of R
*              CWorkspace: need   N*N [R] + 2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer N*N [R] + 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', N, N, RWORK( IRU ), N, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', N, N, N, WORK( IR ), LDWRKR,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of R
*              CWorkspace: need   N*N [R] + 2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer N*N [R] + 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', N, N, RWORK( IRVT ), N, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, N, WORK( IR ), LDWRKR,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Multiply Q in A by left singular vectors of R in
*              WORK(IR), storing result in U
*              CWorkspace: need   N*N [R]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'F', N, N, U, LDU, WORK( IR ), LDWRKR )
               CALL ZGEMM( 'N', 'N', M, N, N, CONE, A, LDA, WORK( IR ),
     $                     LDWRKR, CZERO, U, LDU )
*
            ELSE IF( WNTQA ) THEN
*
*              Path 4 (M >> N, JOBZ='A')
*              M left singular vectors to be computed in U and
*              N right singular vectors to be computed in VT
*
               IU = 1
*
*              WORK(IU) is N by N
*
               LDWRKU = N
               ITAU = IU + LDWRKU*N
               NWORK = ITAU + N
*
*              Compute A=Q*R, copying result to U
*              CWorkspace: need   N*N [U] + N [tau] + N    [work]
*              CWorkspace: prefer N*N [U] + N [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZGEQRF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
               CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
*
*              Generate Q in U
*              CWorkspace: need   N*N [U] + N [tau] + M    [work]
*              CWorkspace: prefer N*N [U] + N [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGQR( M, M, N, U, LDU, WORK( ITAU ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Produce R in A, zeroing out below it
*
               CALL ZLASET( 'L', N-1, N-1, CZERO, CZERO, A( 2, 1 ),
     $                      LDA )
               IE = 1
               ITAUQ = ITAU
               ITAUP = ITAUQ + N
               NWORK = ITAUP + N
*
*              Bidiagonalize R in A
*              CWorkspace: need   N*N [U] + 2*N [tauq, taup] + N      [work]
*              CWorkspace: prefer N*N [U] + 2*N [tauq, taup] + 2*N*NB [work]
*              RWorkspace: need   N [e]
*
               CALL ZGEBRD( N, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                      WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                      IERR )
               IRU = IE + N
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix WORK(IU)
*              Overwrite WORK(IU) by left singular vectors of R
*              CWorkspace: need   N*N [U] + 2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer N*N [U] + 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', N, N, RWORK( IRU ), N, WORK( IU ),
     $                      LDWRKU )
               CALL ZUNMBR( 'Q', 'L', 'N', N, N, N, A, LDA,
     $                      WORK( ITAUQ ), WORK( IU ), LDWRKU,
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of R
*              CWorkspace: need   N*N [U] + 2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer N*N [U] + 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', N, N, RWORK( IRVT ), N, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, N, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Multiply Q in U by left singular vectors of R in
*              WORK(IU), storing result in A
*              CWorkspace: need   N*N [U]
*              RWorkspace: need   0
*
               CALL ZGEMM( 'N', 'N', M, N, N, CONE, U, LDU, WORK( IU ),
     $                     LDWRKU, CZERO, A, LDA )
*
*              Copy left singular vectors of A from A to U
*
               CALL ZLACPY( 'F', M, N, A, LDA, U, LDU )
*
            END IF
*
         ELSE IF( M.GE.MNTHR2 ) THEN
*
*           MNTHR2 <= M < MNTHR1
*
*           Path 5 (M >> N, but not as much as MNTHR1)
*           Reduce to bidiagonal form without QR decomposition, use
*           ZUNGBR and matrix multiplication to compute singular vectors
*
            IE = 1
            NRWORK = IE + N
            ITAUQ = 1
            ITAUP = ITAUQ + N
            NWORK = ITAUP + N
*
*           Bidiagonalize A
*           CWorkspace: need   2*N [tauq, taup] + M        [work]
*           CWorkspace: prefer 2*N [tauq, taup] + (M+N)*NB [work]
*           RWorkspace: need   N [e]
*
            CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                   WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                   IERR )
            IF( WNTQN ) THEN
*
*              Path 5n (M >> N, JOBZ='N')
*              Compute singular values only
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + BDSPAC
*
               CALL DBDSDC( 'U', 'N', N, S, RWORK( IE ), DUM, 1,DUM,1,
     $                      DUM, IDUM, RWORK( NRWORK ), IWORK, INFO )
            ELSE IF( WNTQO ) THEN
               IU = NWORK
               IRU = NRWORK
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
*
*              Path 5o (M >> N, JOBZ='O')
*              Copy A to VT, generate P**H
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
               CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Generate Q in A
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGBR( 'Q', M, N, N, A, LDA, WORK( ITAUQ ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
               IF( LWORK .GE. M*N + 3*N ) THEN
*
*                 WORK( IU ) is M by N
*
                  LDWRKU = M
               ELSE
*
*                 WORK(IU) is LDWRKU by N
*
                  LDWRKU = ( LWORK - 3*N ) / N
               END IF
               NWORK = IU + LDWRKU*N
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Multiply real matrix RWORK(IRVT) by P**H in VT,
*              storing the result in WORK(IU), copying to VT
*              CWorkspace: need   2*N [tauq, taup] + N*N [U]
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + 2*N*N [rwork]
*
               CALL ZLARCM( N, N, RWORK( IRVT ), N, VT, LDVT,
     $                      WORK( IU ), LDWRKU, RWORK( NRWORK ) )
               CALL ZLACPY( 'F', N, N, WORK( IU ), LDWRKU, VT, LDVT )
*
*              Multiply Q in A by real matrix RWORK(IRU), storing the
*              result in WORK(IU), copying to A
*              CWorkspace: need   2*N [tauq, taup] + N*N [U]
*              CWorkspace: prefer 2*N [tauq, taup] + M*N [U]
*              RWorkspace: need   N [e] + N*N [RU] + 2*N*N [rwork]
*              RWorkspace: prefer N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here
*
               NRWORK = IRVT
               DO 20 I = 1, M, LDWRKU
                  CHUNK = MIN( M-I+1, LDWRKU )
                  CALL ZLACRM( CHUNK, N, A( I, 1 ), LDA, RWORK( IRU ),
     $                         N, WORK( IU ), LDWRKU, RWORK( NRWORK ) )
                  CALL ZLACPY( 'F', CHUNK, N, WORK( IU ), LDWRKU,
     $                         A( I, 1 ), LDA )
   20          CONTINUE
*
            ELSE IF( WNTQS ) THEN
*
*              Path 5s (M >> N, JOBZ='S')
*              Copy A to VT, generate P**H
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
               CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Copy A to U, generate Q
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
               CALL ZUNGBR( 'Q', M, N, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               IRU = NRWORK
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Multiply real matrix RWORK(IRVT) by P**H in VT,
*              storing the result in A, copying to VT
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + 2*N*N [rwork]
*
               CALL ZLARCM( N, N, RWORK( IRVT ), N, VT, LDVT, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', N, N, A, LDA, VT, LDVT )
*
*              Multiply Q in U by real matrix RWORK(IRU), storing the
*              result in A, copying to U
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here
*
               NRWORK = IRVT
               CALL ZLACRM( M, N, U, LDU, RWORK( IRU ), N, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, N, A, LDA, U, LDU )
            ELSE
*
*              Path 5a (M >> N, JOBZ='A')
*              Copy A to VT, generate P**H
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'U', N, N, A, LDA, VT, LDVT )
               CALL ZUNGBR( 'P', N, N, N, VT, LDVT, WORK( ITAUP ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Copy A to U, generate Q
*              CWorkspace: need   2*N [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'L', M, N, A, LDA, U, LDU )
               CALL ZUNGBR( 'Q', M, M, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               IRU = NRWORK
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Multiply real matrix RWORK(IRVT) by P**H in VT,
*              storing the result in A, copying to VT
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + 2*N*N [rwork]
*
               CALL ZLARCM( N, N, RWORK( IRVT ), N, VT, LDVT, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', N, N, A, LDA, VT, LDVT )
*
*              Multiply Q in U by real matrix RWORK(IRU), storing the
*              result in A, copying to U
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here
*
               NRWORK = IRVT
               CALL ZLACRM( M, N, U, LDU, RWORK( IRU ), N, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, N, A, LDA, U, LDU )
            END IF
*
         ELSE
*
*           M .LT. MNTHR2
*
*           Path 6 (M >= N, but not much larger)
*           Reduce to bidiagonal form without QR decomposition
*           Use ZUNMBR to compute singular vectors
*
            IE = 1
            NRWORK = IE + N
            ITAUQ = 1
            ITAUP = ITAUQ + N
            NWORK = ITAUP + N
*
*           Bidiagonalize A
*           CWorkspace: need   2*N [tauq, taup] + M        [work]
*           CWorkspace: prefer 2*N [tauq, taup] + (M+N)*NB [work]
*           RWorkspace: need   N [e]
*
            CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                   WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                   IERR )
            IF( WNTQN ) THEN
*
*              Path 6n (M >= N, JOBZ='N')
*              Compute singular values only
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + BDSPAC
*
               CALL DBDSDC( 'U', 'N', N, S, RWORK( IE ), DUM,1,DUM,1,
     $                      DUM, IDUM, RWORK( NRWORK ), IWORK, INFO )
            ELSE IF( WNTQO ) THEN
               IU = NWORK
               IRU = NRWORK
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               IF( LWORK .GE. M*N + 3*N ) THEN
*
*                 WORK( IU ) is M by N
*
                  LDWRKU = M
               ELSE
*
*                 WORK( IU ) is LDWRKU by N
*
                  LDWRKU = ( LWORK - 3*N ) / N
               END IF
               NWORK = IU + LDWRKU*N
*
*              Path 6o (M >= N, JOBZ='O')
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of A
*              CWorkspace: need   2*N [tauq, taup] + N*N [U] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*N [U] + N*NB [work]
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT]
*
               CALL ZLACP2( 'F', N, N, RWORK( IRVT ), N, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, N, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
               IF( LWORK .GE. M*N + 3*N ) THEN
*
*                 Path 6o-fast
*                 Copy real matrix RWORK(IRU) to complex matrix WORK(IU)
*                 Overwrite WORK(IU) by left singular vectors of A, copying
*                 to A
*                 CWorkspace: need   2*N [tauq, taup] + M*N [U] + N    [work]
*                 CWorkspace: prefer 2*N [tauq, taup] + M*N [U] + N*NB [work]
*                 RWorkspace: need   N [e] + N*N [RU]
*
                  CALL ZLASET( 'F', M, N, CZERO, CZERO, WORK( IU ),
     $                         LDWRKU )
                  CALL ZLACP2( 'F', N, N, RWORK( IRU ), N, WORK( IU ),
     $                         LDWRKU )
                  CALL ZUNMBR( 'Q', 'L', 'N', M, N, N, A, LDA,
     $                         WORK( ITAUQ ), WORK( IU ), LDWRKU,
     $                         WORK( NWORK ), LWORK-NWORK+1, IERR )
                  CALL ZLACPY( 'F', M, N, WORK( IU ), LDWRKU, A, LDA )
               ELSE
*
*                 Path 6o-slow
*                 Generate Q in A
*                 CWorkspace: need   2*N [tauq, taup] + N*N [U] + N    [work]
*                 CWorkspace: prefer 2*N [tauq, taup] + N*N [U] + N*NB [work]
*                 RWorkspace: need   0
*
                  CALL ZUNGBR( 'Q', M, N, N, A, LDA, WORK( ITAUQ ),
     $                         WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*                 Multiply Q in A by real matrix RWORK(IRU), storing the
*                 result in WORK(IU), copying to A
*                 CWorkspace: need   2*N [tauq, taup] + N*N [U]
*                 CWorkspace: prefer 2*N [tauq, taup] + M*N [U]
*                 RWorkspace: need   N [e] + N*N [RU] + 2*N*N [rwork]
*                 RWorkspace: prefer N [e] + N*N [RU] + 2*M*N [rwork] < N + 5*N*N since M < 2*N here
*
                  NRWORK = IRVT
                  DO 30 I = 1, M, LDWRKU
                     CHUNK = MIN( M-I+1, LDWRKU )
                     CALL ZLACRM( CHUNK, N, A( I, 1 ), LDA,
     $                            RWORK( IRU ), N, WORK( IU ), LDWRKU,
     $                            RWORK( NRWORK ) )
                     CALL ZLACPY( 'F', CHUNK, N, WORK( IU ), LDWRKU,
     $                            A( I, 1 ), LDA )
   30             CONTINUE
               END IF
*
            ELSE IF( WNTQS ) THEN
*
*              Path 6s (M >= N, JOBZ='S')
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               IRU = NRWORK
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of A
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT]
*
               CALL ZLASET( 'F', M, N, CZERO, CZERO, U, LDU )
               CALL ZLACP2( 'F', N, N, RWORK( IRU ), N, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, N, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of A
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT]
*
               CALL ZLACP2( 'F', N, N, RWORK( IRVT ), N, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, N, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
            ELSE
*
*              Path 6a (M >= N, JOBZ='A')
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT] + BDSPAC
*
               IRU = NRWORK
               IRVT = IRU + N*N
               NRWORK = IRVT + N*N
               CALL DBDSDC( 'U', 'I', N, S, RWORK( IE ), RWORK( IRU ),
     $                      N, RWORK( IRVT ), N, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Set the right corner of U to identity matrix
*
               CALL ZLASET( 'F', M, M, CZERO, CZERO, U, LDU )
               IF( M.GT.N ) THEN
                  CALL ZLASET( 'F', M-N, M-N, CZERO, CONE,
     $                         U( N+1, N+1 ), LDU )
               END IF
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of A
*              CWorkspace: need   2*N [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + M*NB [work]
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT]
*
               CALL ZLACP2( 'F', N, N, RWORK( IRU ), N, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of A
*              CWorkspace: need   2*N [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*N [tauq, taup] + N*NB [work]
*              RWorkspace: need   N [e] + N*N [RU] + N*N [RVT]
*
               CALL ZLACP2( 'F', N, N, RWORK( IRVT ), N, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, N, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
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
         IF( N.GE.MNTHR1 ) THEN
*
            IF( WNTQN ) THEN
*
*              Path 1t (N >> M, JOBZ='N')
*              No singular vectors to be computed
*
               ITAU = 1
               NWORK = ITAU + M
*
*              Compute A=L*Q
*              CWorkspace: need   M [tau] + M    [work]
*              CWorkspace: prefer M [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZGELQF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Zero out above L
*
               CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO, A( 1, 2 ),
     $                      LDA )
               IE = 1
               ITAUQ = 1
               ITAUP = ITAUQ + M
               NWORK = ITAUP + M
*
*              Bidiagonalize L in A
*              CWorkspace: need   2*M [tauq, taup] + M      [work]
*              CWorkspace: prefer 2*M [tauq, taup] + 2*M*NB [work]
*              RWorkspace: need   M [e]
*
               CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                      WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                      IERR )
               NRWORK = IE + M
*
*              Perform bidiagonal SVD, compute singular values only
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + BDSPAC
*
               CALL DBDSDC( 'U', 'N', M, S, RWORK( IE ), DUM,1,DUM,1,
     $                      DUM, IDUM, RWORK( NRWORK ), IWORK, INFO )
*
            ELSE IF( WNTQO ) THEN
*
*              Path 2t (N >> M, JOBZ='O')
*              M right singular vectors to be overwritten on A and
*              M left singular vectors to be computed in U
*
               IVT = 1
               LDWKVT = M
*
*              WORK(IVT) is M by M
*
               IL = IVT + LDWKVT*M
               IF( LWORK .GE. M*N + M*M + 3*M ) THEN
*
*                 WORK(IL) M by N
*
                  LDWRKL = M
                  CHUNK = N
               ELSE
*
*                 WORK(IL) is M by CHUNK
*
                  LDWRKL = M
                  CHUNK = ( LWORK - M*M - 3*M ) / M
               END IF
               ITAU = IL + LDWRKL*CHUNK
               NWORK = ITAU + M
*
*              Compute A=L*Q
*              CWorkspace: need   M*M [VT] + M*M [L] + M [tau] + M    [work]
*              CWorkspace: prefer M*M [VT] + M*M [L] + M [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZGELQF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy L to WORK(IL), zeroing about above it
*
               CALL ZLACPY( 'L', M, M, A, LDA, WORK( IL ), LDWRKL )
               CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                      WORK( IL+LDWRKL ), LDWRKL )
*
*              Generate Q in A
*              CWorkspace: need   M*M [VT] + M*M [L] + M [tau] + M    [work]
*              CWorkspace: prefer M*M [VT] + M*M [L] + M [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
               IE = 1
               ITAUQ = ITAU
               ITAUP = ITAUQ + M
               NWORK = ITAUP + M
*
*              Bidiagonalize L in WORK(IL)
*              CWorkspace: need   M*M [VT] + M*M [L] + 2*M [tauq, taup] + M      [work]
*              CWorkspace: prefer M*M [VT] + M*M [L] + 2*M [tauq, taup] + 2*M*NB [work]
*              RWorkspace: need   M [e]
*
               CALL ZGEBRD( M, M, WORK( IL ), LDWRKL, S, RWORK( IE ),
     $                      WORK( ITAUQ ), WORK( ITAUP ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RU] + M*M [RVT] + BDSPAC
*
               IRU = IE + M
               IRVT = IRU + M*M
               NRWORK = IRVT + M*M
               CALL DBDSDC( 'U', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix WORK(IU)
*              Overwrite WORK(IU) by the left singular vectors of L
*              CWorkspace: need   M*M [VT] + M*M [L] + 2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer M*M [VT] + M*M [L] + 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', M, M, RWORK( IRU ), M, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, M, WORK( IL ), LDWRKL,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix WORK(IVT)
*              Overwrite WORK(IVT) by the right singular vectors of L
*              CWorkspace: need   M*M [VT] + M*M [L] + 2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer M*M [VT] + M*M [L] + 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', M, M, RWORK( IRVT ), M, WORK( IVT ),
     $                      LDWKVT )
               CALL ZUNMBR( 'P', 'R', 'C', M, M, M, WORK( IL ), LDWRKL,
     $                      WORK( ITAUP ), WORK( IVT ), LDWKVT,
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Multiply right singular vectors of L in WORK(IL) by Q
*              in A, storing result in WORK(IL) and copying to A
*              CWorkspace: need   M*M [VT] + M*M [L]
*              CWorkspace: prefer M*M [VT] + M*N [L]
*              RWorkspace: need   0
*
               DO 40 I = 1, N, CHUNK
                  BLK = MIN( N-I+1, CHUNK )
                  CALL ZGEMM( 'N', 'N', M, BLK, M, CONE, WORK( IVT ), M,
     $                        A( 1, I ), LDA, CZERO, WORK( IL ),
     $                        LDWRKL )
                  CALL ZLACPY( 'F', M, BLK, WORK( IL ), LDWRKL,
     $                         A( 1, I ), LDA )
   40          CONTINUE
*
            ELSE IF( WNTQS ) THEN
*
*              Path 3t (N >> M, JOBZ='S')
*              M right singular vectors to be computed in VT and
*              M left singular vectors to be computed in U
*
               IL = 1
*
*              WORK(IL) is M by M
*
               LDWRKL = M
               ITAU = IL + LDWRKL*M
               NWORK = ITAU + M
*
*              Compute A=L*Q
*              CWorkspace: need   M*M [L] + M [tau] + M    [work]
*              CWorkspace: prefer M*M [L] + M [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZGELQF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy L to WORK(IL), zeroing out above it
*
               CALL ZLACPY( 'L', M, M, A, LDA, WORK( IL ), LDWRKL )
               CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO,
     $                      WORK( IL+LDWRKL ), LDWRKL )
*
*              Generate Q in A
*              CWorkspace: need   M*M [L] + M [tau] + M    [work]
*              CWorkspace: prefer M*M [L] + M [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGLQ( M, N, M, A, LDA, WORK( ITAU ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
               IE = 1
               ITAUQ = ITAU
               ITAUP = ITAUQ + M
               NWORK = ITAUP + M
*
*              Bidiagonalize L in WORK(IL)
*              CWorkspace: need   M*M [L] + 2*M [tauq, taup] + M      [work]
*              CWorkspace: prefer M*M [L] + 2*M [tauq, taup] + 2*M*NB [work]
*              RWorkspace: need   M [e]
*
               CALL ZGEBRD( M, M, WORK( IL ), LDWRKL, S, RWORK( IE ),
     $                      WORK( ITAUQ ), WORK( ITAUP ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RU] + M*M [RVT] + BDSPAC
*
               IRU = IE + M
               IRVT = IRU + M*M
               NRWORK = IRVT + M*M
               CALL DBDSDC( 'U', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of L
*              CWorkspace: need   M*M [L] + 2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer M*M [L] + 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', M, M, RWORK( IRU ), M, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, M, WORK( IL ), LDWRKL,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by left singular vectors of L
*              CWorkspace: need   M*M [L] + 2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer M*M [L] + 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', M, M, RWORK( IRVT ), M, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', M, M, M, WORK( IL ), LDWRKL,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy VT to WORK(IL), multiply right singular vectors of L
*              in WORK(IL) by Q in A, storing result in VT
*              CWorkspace: need   M*M [L]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'F', M, M, VT, LDVT, WORK( IL ), LDWRKL )
               CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IL ), LDWRKL,
     $                     A, LDA, CZERO, VT, LDVT )
*
            ELSE IF( WNTQA ) THEN
*
*              Path 4t (N >> M, JOBZ='A')
*              N right singular vectors to be computed in VT and
*              M left singular vectors to be computed in U
*
               IVT = 1
*
*              WORK(IVT) is M by M
*
               LDWKVT = M
               ITAU = IVT + LDWKVT*M
               NWORK = ITAU + M
*
*              Compute A=L*Q, copying result to VT
*              CWorkspace: need   M*M [VT] + M [tau] + M    [work]
*              CWorkspace: prefer M*M [VT] + M [tau] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZGELQF( M, N, A, LDA, WORK( ITAU ), WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
               CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
*
*              Generate Q in VT
*              CWorkspace: need   M*M [VT] + M [tau] + N    [work]
*              CWorkspace: prefer M*M [VT] + M [tau] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGLQ( N, N, M, VT, LDVT, WORK( ITAU ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Produce L in A, zeroing out above it
*
               CALL ZLASET( 'U', M-1, M-1, CZERO, CZERO, A( 1, 2 ),
     $                      LDA )
               IE = 1
               ITAUQ = ITAU
               ITAUP = ITAUQ + M
               NWORK = ITAUP + M
*
*              Bidiagonalize L in A
*              CWorkspace: need   M*M [VT] + 2*M [tauq, taup] + M      [work]
*              CWorkspace: prefer M*M [VT] + 2*M [tauq, taup] + 2*M*NB [work]
*              RWorkspace: need   M [e]
*
               CALL ZGEBRD( M, M, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                      WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                      IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RU] + M*M [RVT] + BDSPAC
*
               IRU = IE + M
               IRVT = IRU + M*M
               NRWORK = IRVT + M*M
               CALL DBDSDC( 'U', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of L
*              CWorkspace: need   M*M [VT] + 2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer M*M [VT] + 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', M, M, RWORK( IRU ), M, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, M, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix WORK(IVT)
*              Overwrite WORK(IVT) by right singular vectors of L
*              CWorkspace: need   M*M [VT] + 2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer M*M [VT] + 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACP2( 'F', M, M, RWORK( IRVT ), M, WORK( IVT ),
     $                      LDWKVT )
               CALL ZUNMBR( 'P', 'R', 'C', M, M, M, A, LDA,
     $                      WORK( ITAUP ), WORK( IVT ), LDWKVT,
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Multiply right singular vectors of L in WORK(IVT) by
*              Q in VT, storing result in A
*              CWorkspace: need   M*M [VT]
*              RWorkspace: need   0
*
               CALL ZGEMM( 'N', 'N', M, N, M, CONE, WORK( IVT ), LDWKVT,
     $                     VT, LDVT, CZERO, A, LDA )
*
*              Copy right singular vectors of A from A to VT
*
               CALL ZLACPY( 'F', M, N, A, LDA, VT, LDVT )
*
            END IF
*
         ELSE IF( N.GE.MNTHR2 ) THEN
*
*           MNTHR2 <= N < MNTHR1
*
*           Path 5t (N >> M, but not as much as MNTHR1)
*           Reduce to bidiagonal form without QR decomposition, use
*           ZUNGBR and matrix multiplication to compute singular vectors
*
            IE = 1
            NRWORK = IE + M
            ITAUQ = 1
            ITAUP = ITAUQ + M
            NWORK = ITAUP + M
*
*           Bidiagonalize A
*           CWorkspace: need   2*M [tauq, taup] + N        [work]
*           CWorkspace: prefer 2*M [tauq, taup] + (M+N)*NB [work]
*           RWorkspace: need   M [e]
*
            CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                   WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                   IERR )
*
            IF( WNTQN ) THEN
*
*              Path 5tn (N >> M, JOBZ='N')
*              Compute singular values only
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + BDSPAC
*
               CALL DBDSDC( 'L', 'N', M, S, RWORK( IE ), DUM,1,DUM,1,
     $                      DUM, IDUM, RWORK( NRWORK ), IWORK, INFO )
            ELSE IF( WNTQO ) THEN
               IRVT = NRWORK
               IRU = IRVT + M*M
               NRWORK = IRU + M*M
               IVT = NWORK
*
*              Path 5to (N >> M, JOBZ='O')
*              Copy A to U, generate Q
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
               CALL ZUNGBR( 'Q', M, M, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Generate P**H in A
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZUNGBR( 'P', M, N, M, A, LDA, WORK( ITAUP ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
               LDWKVT = M
               IF( LWORK .GE. M*N + 3*M ) THEN
*
*                 WORK( IVT ) is M by N
*
                  NWORK = IVT + LDWKVT*N
                  CHUNK = N
               ELSE
*
*                 WORK( IVT ) is M by CHUNK
*
                  CHUNK = ( LWORK - 3*M ) / M
                  NWORK = IVT + LDWKVT*CHUNK
               END IF
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC
*
               CALL DBDSDC( 'L', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Multiply Q in U by real matrix RWORK(IRVT)
*              storing the result in WORK(IVT), copying to U
*              CWorkspace: need   2*M [tauq, taup] + M*M [VT]
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + 2*M*M [rwork]
*
               CALL ZLACRM( M, M, U, LDU, RWORK( IRU ), M, WORK( IVT ),
     $                      LDWKVT, RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, M, WORK( IVT ), LDWKVT, U, LDU )
*
*              Multiply RWORK(IRVT) by P**H in A, storing the
*              result in WORK(IVT), copying to A
*              CWorkspace: need   2*M [tauq, taup] + M*M [VT]
*              CWorkspace: prefer 2*M [tauq, taup] + M*N [VT]
*              RWorkspace: need   M [e] + M*M [RVT] + 2*M*M [rwork]
*              RWorkspace: prefer M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here
*
               NRWORK = IRU
               DO 50 I = 1, N, CHUNK
                  BLK = MIN( N-I+1, CHUNK )
                  CALL ZLARCM( M, BLK, RWORK( IRVT ), M, A( 1, I ), LDA,
     $                         WORK( IVT ), LDWKVT, RWORK( NRWORK ) )
                  CALL ZLACPY( 'F', M, BLK, WORK( IVT ), LDWKVT,
     $                         A( 1, I ), LDA )
   50          CONTINUE
            ELSE IF( WNTQS ) THEN
*
*              Path 5ts (N >> M, JOBZ='S')
*              Copy A to U, generate Q
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
               CALL ZUNGBR( 'Q', M, M, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Copy A to VT, generate P**H
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
               CALL ZUNGBR( 'P', M, N, M, VT, LDVT, WORK( ITAUP ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC
*
               IRVT = NRWORK
               IRU = IRVT + M*M
               NRWORK = IRU + M*M
               CALL DBDSDC( 'L', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Multiply Q in U by real matrix RWORK(IRU), storing the
*              result in A, copying to U
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + 2*M*M [rwork]
*
               CALL ZLACRM( M, M, U, LDU, RWORK( IRU ), M, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, M, A, LDA, U, LDU )
*
*              Multiply real matrix RWORK(IRVT) by P**H in VT,
*              storing the result in A, copying to VT
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here
*
               NRWORK = IRU
               CALL ZLARCM( M, N, RWORK( IRVT ), M, VT, LDVT, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, N, A, LDA, VT, LDVT )
            ELSE
*
*              Path 5ta (N >> M, JOBZ='A')
*              Copy A to U, generate Q
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'L', M, M, A, LDA, U, LDU )
               CALL ZUNGBR( 'Q', M, M, N, U, LDU, WORK( ITAUQ ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Copy A to VT, generate P**H
*              CWorkspace: need   2*M [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + N*NB [work]
*              RWorkspace: need   0
*
               CALL ZLACPY( 'U', M, N, A, LDA, VT, LDVT )
               CALL ZUNGBR( 'P', N, N, M, VT, LDVT, WORK( ITAUP ),
     $                      WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC
*
               IRVT = NRWORK
               IRU = IRVT + M*M
               NRWORK = IRU + M*M
               CALL DBDSDC( 'L', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Multiply Q in U by real matrix RWORK(IRU), storing the
*              result in A, copying to U
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + 2*M*M [rwork]
*
               CALL ZLACRM( M, M, U, LDU, RWORK( IRU ), M, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, M, A, LDA, U, LDU )
*
*              Multiply real matrix RWORK(IRVT) by P**H in VT,
*              storing the result in A, copying to VT
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here
*
               NRWORK = IRU
               CALL ZLARCM( M, N, RWORK( IRVT ), M, VT, LDVT, A, LDA,
     $                      RWORK( NRWORK ) )
               CALL ZLACPY( 'F', M, N, A, LDA, VT, LDVT )
            END IF
*
         ELSE
*
*           N .LT. MNTHR2
*
*           Path 6t (N > M, but not much larger)
*           Reduce to bidiagonal form without LQ decomposition
*           Use ZUNMBR to compute singular vectors
*
            IE = 1
            NRWORK = IE + M
            ITAUQ = 1
            ITAUP = ITAUQ + M
            NWORK = ITAUP + M
*
*           Bidiagonalize A
*           CWorkspace: need   2*M [tauq, taup] + N        [work]
*           CWorkspace: prefer 2*M [tauq, taup] + (M+N)*NB [work]
*           RWorkspace: need   M [e]
*
            CALL ZGEBRD( M, N, A, LDA, S, RWORK( IE ), WORK( ITAUQ ),
     $                   WORK( ITAUP ), WORK( NWORK ), LWORK-NWORK+1,
     $                   IERR )
            IF( WNTQN ) THEN
*
*              Path 6tn (N > M, JOBZ='N')
*              Compute singular values only
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + BDSPAC
*
               CALL DBDSDC( 'L', 'N', M, S, RWORK( IE ), DUM,1,DUM,1,
     $                      DUM, IDUM, RWORK( NRWORK ), IWORK, INFO )
            ELSE IF( WNTQO ) THEN
*              Path 6to (N > M, JOBZ='O')
               LDWKVT = M
               IVT = NWORK
               IF( LWORK .GE. M*N + 3*M ) THEN
*
*                 WORK( IVT ) is M by N
*
                  CALL ZLASET( 'F', M, N, CZERO, CZERO, WORK( IVT ),
     $                         LDWKVT )
                  NWORK = IVT + LDWKVT*N
               ELSE
*
*                 WORK( IVT ) is M by CHUNK
*
                  CHUNK = ( LWORK - 3*M ) / M
                  NWORK = IVT + LDWKVT*CHUNK
               END IF
*
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC
*
               IRVT = NRWORK
               IRU = IRVT + M*M
               NRWORK = IRU + M*M
               CALL DBDSDC( 'L', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of A
*              CWorkspace: need   2*M [tauq, taup] + M*M [VT] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*M [VT] + M*NB [work]
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU]
*
               CALL ZLACP2( 'F', M, M, RWORK( IRU ), M, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
               IF( LWORK .GE. M*N + 3*M ) THEN
*
*                 Path 6to-fast
*                 Copy real matrix RWORK(IRVT) to complex matrix WORK(IVT)
*                 Overwrite WORK(IVT) by right singular vectors of A,
*                 copying to A
*                 CWorkspace: need   2*M [tauq, taup] + M*N [VT] + M    [work]
*                 CWorkspace: prefer 2*M [tauq, taup] + M*N [VT] + M*NB [work]
*                 RWorkspace: need   M [e] + M*M [RVT]
*
                  CALL ZLACP2( 'F', M, M, RWORK( IRVT ), M, WORK( IVT ),
     $                         LDWKVT )
                  CALL ZUNMBR( 'P', 'R', 'C', M, N, M, A, LDA,
     $                         WORK( ITAUP ), WORK( IVT ), LDWKVT,
     $                         WORK( NWORK ), LWORK-NWORK+1, IERR )
                  CALL ZLACPY( 'F', M, N, WORK( IVT ), LDWKVT, A, LDA )
               ELSE
*
*                 Path 6to-slow
*                 Generate P**H in A
*                 CWorkspace: need   2*M [tauq, taup] + M*M [VT] + M    [work]
*                 CWorkspace: prefer 2*M [tauq, taup] + M*M [VT] + M*NB [work]
*                 RWorkspace: need   0
*
                  CALL ZUNGBR( 'P', M, N, M, A, LDA, WORK( ITAUP ),
     $                         WORK( NWORK ), LWORK-NWORK+1, IERR )
*
*                 Multiply Q in A by real matrix RWORK(IRU), storing the
*                 result in WORK(IU), copying to A
*                 CWorkspace: need   2*M [tauq, taup] + M*M [VT]
*                 CWorkspace: prefer 2*M [tauq, taup] + M*N [VT]
*                 RWorkspace: need   M [e] + M*M [RVT] + 2*M*M [rwork]
*                 RWorkspace: prefer M [e] + M*M [RVT] + 2*M*N [rwork] < M + 5*M*M since N < 2*M here
*
                  NRWORK = IRU
                  DO 60 I = 1, N, CHUNK
                     BLK = MIN( N-I+1, CHUNK )
                     CALL ZLARCM( M, BLK, RWORK( IRVT ), M, A( 1, I ),
     $                            LDA, WORK( IVT ), LDWKVT,
     $                            RWORK( NRWORK ) )
                     CALL ZLACPY( 'F', M, BLK, WORK( IVT ), LDWKVT,
     $                            A( 1, I ), LDA )
   60             CONTINUE
               END IF
            ELSE IF( WNTQS ) THEN
*
*              Path 6ts (N > M, JOBZ='S')
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC
*
               IRVT = NRWORK
               IRU = IRVT + M*M
               NRWORK = IRU + M*M
               CALL DBDSDC( 'L', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of A
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU]
*
               CALL ZLACP2( 'F', M, M, RWORK( IRU ), M, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of A
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   M [e] + M*M [RVT]
*
               CALL ZLASET( 'F', M, N, CZERO, CZERO, VT, LDVT )
               CALL ZLACP2( 'F', M, M, RWORK( IRVT ), M, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', M, N, M, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
            ELSE
*
*              Path 6ta (N > M, JOBZ='A')
*              Perform bidiagonal SVD, computing left singular vectors
*              of bidiagonal matrix in RWORK(IRU) and computing right
*              singular vectors of bidiagonal matrix in RWORK(IRVT)
*              CWorkspace: need   0
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU] + BDSPAC
*
               IRVT = NRWORK
               IRU = IRVT + M*M
               NRWORK = IRU + M*M
*
               CALL DBDSDC( 'L', 'I', M, S, RWORK( IE ), RWORK( IRU ),
     $                      M, RWORK( IRVT ), M, DUM, IDUM,
     $                      RWORK( NRWORK ), IWORK, INFO )
*
*              Copy real matrix RWORK(IRU) to complex matrix U
*              Overwrite U by left singular vectors of A
*              CWorkspace: need   2*M [tauq, taup] + M    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + M*NB [work]
*              RWorkspace: need   M [e] + M*M [RVT] + M*M [RU]
*
               CALL ZLACP2( 'F', M, M, RWORK( IRU ), M, U, LDU )
               CALL ZUNMBR( 'Q', 'L', 'N', M, M, N, A, LDA,
     $                      WORK( ITAUQ ), U, LDU, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
*
*              Set all of VT to identity matrix
*
               CALL ZLASET( 'F', N, N, CZERO, CONE, VT, LDVT )
*
*              Copy real matrix RWORK(IRVT) to complex matrix VT
*              Overwrite VT by right singular vectors of A
*              CWorkspace: need   2*M [tauq, taup] + N    [work]
*              CWorkspace: prefer 2*M [tauq, taup] + N*NB [work]
*              RWorkspace: need   M [e] + M*M [RVT]
*
               CALL ZLACP2( 'F', M, M, RWORK( IRVT ), M, VT, LDVT )
               CALL ZUNMBR( 'P', 'R', 'C', N, N, M, A, LDA,
     $                      WORK( ITAUP ), VT, LDVT, WORK( NWORK ),
     $                      LWORK-NWORK+1, IERR )
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
*     End of ZGESDD
*
      END
