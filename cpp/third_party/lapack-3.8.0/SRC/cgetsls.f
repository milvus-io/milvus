*  Definition:
*  ===========
*
*       SUBROUTINE CGETSLS( TRANS, M, N, NRHS, A, LDA, B, LDB,
*     $                     WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGETSLS solves overdetermined or underdetermined complex linear systems
*> involving an M-by-N matrix A, using a tall skinny QR or short wide LQ
*> factorization of A.  It is assumed that A has full rank.
*>
*>
*>
*> The following options are provided:
*>
*> 1. If TRANS = 'N' and m >= n:  find the least squares solution of
*>    an overdetermined system, i.e., solve the least squares problem
*>                 minimize || B - A*X ||.
*>
*> 2. If TRANS = 'N' and m < n:  find the minimum norm solution of
*>    an underdetermined system A * X = B.
*>
*> 3. If TRANS = 'C' and m >= n:  find the minimum norm solution of
*>    an undetermined system A**T * X = B.
*>
*> 4. If TRANS = 'C' and m < n:  find the least squares solution of
*>    an overdetermined system, i.e., solve the least squares problem
*>                 minimize || B - A**T * X ||.
*>
*> Several right hand side vectors b and solution vectors x can be
*> handled in a single call; they are stored as the columns of the
*> M-by-NRHS right hand side matrix B and the N-by-NRHS solution
*> matrix X.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N': the linear system involves A;
*>          = 'C': the linear system involves A**H.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of
*>          columns of the matrices B and X. NRHS >=0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit,
*>          A is overwritten by details of its QR or LQ
*>          factorization as returned by CGEQR or CGELQ.
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
*>          B is COMPLEX array, dimension (LDB,NRHS)
*>          On entry, the matrix B of right hand side vectors, stored
*>          columnwise; B is M-by-NRHS if TRANS = 'N', or N-by-NRHS
*>          if TRANS = 'C'.
*>          On exit, if INFO = 0, B is overwritten by the solution
*>          vectors, stored columnwise:
*>          if TRANS = 'N' and m >= n, rows 1 to n of B contain the least
*>          squares solution vectors.
*>          if TRANS = 'N' and m < n, rows 1 to N of B contain the
*>          minimum norm solution vectors;
*>          if TRANS = 'C' and m >= n, rows 1 to M of B contain the
*>          minimum norm solution vectors;
*>          if TRANS = 'C' and m < n, rows 1 to M of B contain the
*>          least squares solution vectors.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= MAX(1,M,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          (workspace) COMPLEX array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) contains optimal (or either minimal
*>          or optimal, if query was assumed) LWORK.
*>          See LWORK for details.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If LWORK = -1 or -2, then a workspace query is assumed.
*>          If LWORK = -1, the routine calculates optimal size of WORK for the
*>          optimal performance and returns this value in WORK(1).
*>          If LWORK = -2, the routine calculates minimal size of WORK and 
*>          returns this value in WORK(1).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO =  i, the i-th diagonal element of the
*>                triangular factor of A is zero, so that A does not have
*>                full rank; the least squares solution could not be
*>                computed.
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
*> \ingroup complexGEsolve
*
*  =====================================================================
      SUBROUTINE CGETSLS( TRANS, M, N, NRHS, A, LDA, B, LDB,
     $                    WORK, LWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * )
*
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, TRAN
      INTEGER            I, IASCL, IBSCL, J, MINMN, MAXMN, BROW,
     $                   SCLLEN, MNK, TSZO, TSZM, LWO, LWM, LW1, LW2,
     $                   WSIZEO, WSIZEM, INFO2
      REAL               ANRM, BIGNUM, BNRM, SMLNUM, DUM( 1 )
      COMPLEX            TQ( 5 ), WORKQ( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               SLAMCH, CLANGE
      EXTERNAL           LSAME, ILAENV, SLABAD, SLAMCH, CLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEQR, CGEMQR, CLASCL, CLASET,
     $                   CTRTRS, XERBLA, CGELQ, CGEMLQ
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          REAL, MAX, MIN, INT
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments.
*
      INFO = 0
      MINMN = MIN( M, N )
      MAXMN = MAX( M, N )
      MNK   = MAX( MINMN, NRHS )
      TRAN  = LSAME( TRANS, 'C' )
*
      LQUERY = ( LWORK.EQ.-1 .OR. LWORK.EQ.-2 )
      IF( .NOT.( LSAME( TRANS, 'N' ) .OR.
     $    LSAME( TRANS, 'C' ) ) ) THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -6
      ELSE IF( LDB.LT.MAX( 1, M, N ) ) THEN
         INFO = -8
      END IF
*
      IF( INFO.EQ.0 ) THEN
*
*     Determine the block size and minimum LWORK
*
       IF( M.GE.N ) THEN
         CALL CGEQR( M, N, A, LDA, TQ, -1, WORKQ, -1, INFO2 )
         TSZO = INT( TQ( 1 ) )
         LWO  = INT( WORKQ( 1 ) )
         CALL CGEMQR( 'L', TRANS, M, NRHS, N, A, LDA, TQ,
     $                TSZO, B, LDB, WORKQ, -1, INFO2 )
         LWO  = MAX( LWO, INT( WORKQ( 1 ) ) )
         CALL CGEQR( M, N, A, LDA, TQ, -2, WORKQ, -2, INFO2 )
         TSZM = INT( TQ( 1 ) )
         LWM  = INT( WORKQ( 1 ) )
         CALL CGEMQR( 'L', TRANS, M, NRHS, N, A, LDA, TQ,
     $                TSZM, B, LDB, WORKQ, -1, INFO2 )
         LWM = MAX( LWM, INT( WORKQ( 1 ) ) )
         WSIZEO = TSZO + LWO
         WSIZEM = TSZM + LWM
       ELSE
         CALL CGELQ( M, N, A, LDA, TQ, -1, WORKQ, -1, INFO2 )
         TSZO = INT( TQ( 1 ) )
         LWO  = INT( WORKQ( 1 ) )
         CALL CGEMLQ( 'L', TRANS, N, NRHS, M, A, LDA, TQ,
     $                TSZO, B, LDB, WORKQ, -1, INFO2 )
         LWO  = MAX( LWO, INT( WORKQ( 1 ) ) )
         CALL CGELQ( M, N, A, LDA, TQ, -2, WORKQ, -2, INFO2 )
         TSZM = INT( TQ( 1 ) )
         LWM  = INT( WORKQ( 1 ) )
         CALL CGEMLQ( 'L', TRANS, N, NRHS, M, A, LDA, TQ,
     $                TSZO, B, LDB, WORKQ, -1, INFO2 )
         LWM  = MAX( LWM, INT( WORKQ( 1 ) ) )
         WSIZEO = TSZO + LWO
         WSIZEM = TSZM + LWM
       END IF
*
       IF( ( LWORK.LT.WSIZEM ).AND.( .NOT.LQUERY ) ) THEN
          INFO = -10
       END IF
*
      END IF
*
      IF( INFO.NE.0 ) THEN
        CALL XERBLA( 'CGETSLS', -INFO )
        WORK( 1 ) = REAL( WSIZEO )
        RETURN
      END IF
      IF( LQUERY ) THEN
        IF( LWORK.EQ.-1 ) WORK( 1 ) = REAL( WSIZEO )
        IF( LWORK.EQ.-2 ) WORK( 1 ) = REAL( WSIZEM )
        RETURN
      END IF
      IF( LWORK.LT.WSIZEO ) THEN
        LW1 = TSZM
        LW2 = LWM
      ELSE
        LW1 = TSZO
        LW2 = LWO
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N, NRHS ).EQ.0 ) THEN
           CALL CLASET( 'FULL', MAX( M, N ), NRHS, CZERO, CZERO,
     $                  B, LDB )
           RETURN
      END IF
*
*     Get machine parameters
*
       SMLNUM = SLAMCH( 'S' ) / SLAMCH( 'P' )
       BIGNUM = ONE / SMLNUM
       CALL SLABAD( SMLNUM, BIGNUM )
*
*     Scale A, B if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = CLANGE( 'M', M, N, A, LDA, DUM )
      IASCL = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM
*
         CALL CLASCL( 'G', 0, 0, ANRM, SMLNUM, M, N, A, LDA, INFO )
         IASCL = 1
      ELSE IF( ANRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM
*
         CALL CLASCL( 'G', 0, 0, ANRM, BIGNUM, M, N, A, LDA, INFO )
         IASCL = 2
      ELSE IF( ANRM.EQ.ZERO ) THEN
*
*        Matrix all zero. Return zero solution.
*
         CALL CLASET( 'F', MAXMN, NRHS, CZERO, CZERO, B, LDB )
         GO TO 50
      END IF
*
      BROW = M
      IF ( TRAN ) THEN
        BROW = N
      END IF
      BNRM = CLANGE( 'M', BROW, NRHS, B, LDB, DUM )
      IBSCL = 0
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
*
*        Scale matrix norm up to SMLNUM
*
         CALL CLASCL( 'G', 0, 0, BNRM, SMLNUM, BROW, NRHS, B, LDB,
     $                INFO )
         IBSCL = 1
      ELSE IF( BNRM.GT.BIGNUM ) THEN
*
*        Scale matrix norm down to BIGNUM
*
         CALL CLASCL( 'G', 0, 0, BNRM, BIGNUM, BROW, NRHS, B, LDB,
     $                INFO )
         IBSCL = 2
      END IF
*
      IF ( M.GE.N ) THEN
*
*        compute QR factorization of A
*
        CALL CGEQR( M, N, A, LDA, WORK( LW2+1 ), LW1,
     $              WORK( 1 ), LW2, INFO )
        IF ( .NOT.TRAN ) THEN
*
*           Least-Squares Problem min || A * X - B ||
*
*           B(1:M,1:NRHS) := Q**T * B(1:M,1:NRHS)
*
          CALL CGEMQR( 'L' , 'C', M, NRHS, N, A, LDA,
     $                 WORK( LW2+1 ), LW1, B, LDB, WORK( 1 ), LW2,
     $                 INFO )
*
*           B(1:N,1:NRHS) := inv(R) * B(1:N,1:NRHS)
*
          CALL CTRTRS( 'U', 'N', 'N', N, NRHS,
     $                  A, LDA, B, LDB, INFO )
          IF( INFO.GT.0 ) THEN
            RETURN
          END IF
          SCLLEN = N
        ELSE
*
*           Overdetermined system of equations A**T * X = B
*
*           B(1:N,1:NRHS) := inv(R**T) * B(1:N,1:NRHS)
*
            CALL CTRTRS( 'U', 'C', 'N', N, NRHS,
     $                   A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
*           B(N+1:M,1:NRHS) = CZERO
*
            DO 20 J = 1, NRHS
               DO 10 I = N + 1, M
                  B( I, J ) = CZERO
   10          CONTINUE
   20       CONTINUE
*
*           B(1:M,1:NRHS) := Q(1:N,:) * B(1:N,1:NRHS)
*
            CALL CGEMQR( 'L', 'N', M, NRHS, N, A, LDA,
     $                   WORK( LW2+1 ), LW1, B, LDB, WORK( 1 ), LW2,
     $                   INFO )
*
            SCLLEN = M
*
         END IF
*
      ELSE
*
*        Compute LQ factorization of A
*
         CALL CGELQ( M, N, A, LDA, WORK( LW2+1 ), LW1,
     $               WORK( 1 ), LW2, INFO )
*
*        workspace at least M, optimally M*NB.
*
         IF( .NOT.TRAN ) THEN
*
*           underdetermined system of equations A * X = B
*
*           B(1:M,1:NRHS) := inv(L) * B(1:M,1:NRHS)
*
            CALL CTRTRS( 'L', 'N', 'N', M, NRHS,
     $                   A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
*           B(M+1:N,1:NRHS) = 0
*
            DO 40 J = 1, NRHS
               DO 30 I = M + 1, N
                  B( I, J ) = CZERO
   30          CONTINUE
   40       CONTINUE
*
*           B(1:N,1:NRHS) := Q(1:N,:)**T * B(1:M,1:NRHS)
*
            CALL CGEMLQ( 'L', 'C', N, NRHS, M, A, LDA,
     $                   WORK( LW2+1 ), LW1, B, LDB, WORK( 1 ), LW2,
     $                   INFO )
*
*           workspace at least NRHS, optimally NRHS*NB
*
            SCLLEN = N
*
         ELSE
*
*           overdetermined system min || A**T * X - B ||
*
*           B(1:N,1:NRHS) := Q * B(1:N,1:NRHS)
*
            CALL CGEMLQ( 'L', 'N', N, NRHS, M, A, LDA,
     $                   WORK( LW2+1 ), LW1, B, LDB, WORK( 1 ), LW2,
     $                   INFO )
*
*           workspace at least NRHS, optimally NRHS*NB
*
*           B(1:M,1:NRHS) := inv(L**T) * B(1:M,1:NRHS)
*
            CALL CTRTRS( 'L', 'C', 'N', M, NRHS,
     $                   A, LDA, B, LDB, INFO )
*
            IF( INFO.GT.0 ) THEN
               RETURN
            END IF
*
            SCLLEN = M
*
         END IF
*
      END IF
*
*     Undo scaling
*
      IF( IASCL.EQ.1 ) THEN
        CALL CLASCL( 'G', 0, 0, ANRM, SMLNUM, SCLLEN, NRHS, B, LDB,
     $               INFO )
      ELSE IF( IASCL.EQ.2 ) THEN
        CALL CLASCL( 'G', 0, 0, ANRM, BIGNUM, SCLLEN, NRHS, B, LDB,
     $               INFO )
      END IF
      IF( IBSCL.EQ.1 ) THEN
        CALL CLASCL( 'G', 0, 0, SMLNUM, BNRM, SCLLEN, NRHS, B, LDB,
     $               INFO )
      ELSE IF( IBSCL.EQ.2 ) THEN
        CALL CLASCL( 'G', 0, 0, BIGNUM, BNRM, SCLLEN, NRHS, B, LDB,
     $               INFO )
      END IF
*
   50 CONTINUE
      WORK( 1 ) = REAL( TSZO + LWO )
      RETURN
*
*     End of ZGETSLS
*
      END
