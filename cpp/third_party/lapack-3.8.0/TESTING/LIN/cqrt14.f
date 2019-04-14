*> \brief \b CQRT14
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       REAL             FUNCTION CQRT14( TRANS, M, N, NRHS, A, LDA, X,
*                        LDX, WORK, LWORK )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            LDA, LDX, LWORK, M, N, NRHS
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), WORK( LWORK ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CQRT14 checks whether X is in the row space of A or A'.  It does so
*> by scaling both X and A such that their norms are in the range
*> [sqrt(eps), 1/sqrt(eps)], then computing a QR factorization of [A,X]
*> (if TRANS = 'C') or an LQ factorization of [A',X]' (if TRANS = 'N'),
*> and returning the norm of the trailing triangle, scaled by
*> MAX(M,N,NRHS)*eps.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N':  No transpose, check for X in the row space of A
*>          = 'C':  Conjugate transpose, check for X in row space of A'.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of X.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The M-by-N matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is COMPLEX array, dimension (LDX,NRHS)
*>          If TRANS = 'N', the N-by-NRHS matrix X.
*>          IF TRANS = 'C', the M-by-NRHS matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          length of workspace array required
*>          If TRANS = 'N', LWORK >= (M+NRHS)*(N+2);
*>          if TRANS = 'C', LWORK >= (N+NRHS)*(M+2).
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
*> \ingroup complex_lin
*
*  =====================================================================
      REAL             FUNCTION CQRT14( TRANS, M, N, NRHS, A, LDA, X,
     $                 LDX, WORK, LWORK )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            LDA, LDX, LWORK, M, N, NRHS
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), WORK( LWORK ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            TPSD
      INTEGER            I, INFO, J, LDWORK
      REAL               ANRM, ERR, XNRM
*     ..
*     .. Local Arrays ..
      REAL               RWORK( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANGE, SLAMCH
      EXTERNAL           LSAME, CLANGE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGELQ2, CGEQR2, CLACPY, CLASCL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CONJG, MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
      CQRT14 = ZERO
      IF( LSAME( TRANS, 'N' ) ) THEN
         LDWORK = M + NRHS
         TPSD = .FALSE.
         IF( LWORK.LT.( M+NRHS )*( N+2 ) ) THEN
            CALL XERBLA( 'CQRT14', 10 )
            RETURN
         ELSE IF( N.LE.0 .OR. NRHS.LE.0 ) THEN
            RETURN
         END IF
      ELSE IF( LSAME( TRANS, 'C' ) ) THEN
         LDWORK = M
         TPSD = .TRUE.
         IF( LWORK.LT.( N+NRHS )*( M+2 ) ) THEN
            CALL XERBLA( 'CQRT14', 10 )
            RETURN
         ELSE IF( M.LE.0 .OR. NRHS.LE.0 ) THEN
            RETURN
         END IF
      ELSE
         CALL XERBLA( 'CQRT14', 1 )
         RETURN
      END IF
*
*     Copy and scale A
*
      CALL CLACPY( 'All', M, N, A, LDA, WORK, LDWORK )
      ANRM = CLANGE( 'M', M, N, WORK, LDWORK, RWORK )
      IF( ANRM.NE.ZERO )
     $   CALL CLASCL( 'G', 0, 0, ANRM, ONE, M, N, WORK, LDWORK, INFO )
*
*     Copy X or X' into the right place and scale it
*
      IF( TPSD ) THEN
*
*        Copy X into columns n+1:n+nrhs of work
*
         CALL CLACPY( 'All', M, NRHS, X, LDX, WORK( N*LDWORK+1 ),
     $                LDWORK )
         XNRM = CLANGE( 'M', M, NRHS, WORK( N*LDWORK+1 ), LDWORK,
     $          RWORK )
         IF( XNRM.NE.ZERO )
     $      CALL CLASCL( 'G', 0, 0, XNRM, ONE, M, NRHS,
     $                   WORK( N*LDWORK+1 ), LDWORK, INFO )
         ANRM = CLANGE( 'One-norm', M, N+NRHS, WORK, LDWORK, RWORK )
*
*        Compute QR factorization of X
*
         CALL CGEQR2( M, N+NRHS, WORK, LDWORK,
     $                WORK( LDWORK*( N+NRHS )+1 ),
     $                WORK( LDWORK*( N+NRHS )+MIN( M, N+NRHS )+1 ),
     $                INFO )
*
*        Compute largest entry in upper triangle of
*        work(n+1:m,n+1:n+nrhs)
*
         ERR = ZERO
         DO 20 J = N + 1, N + NRHS
            DO 10 I = N + 1, MIN( M, J )
               ERR = MAX( ERR, ABS( WORK( I+( J-1 )*M ) ) )
   10       CONTINUE
   20    CONTINUE
*
      ELSE
*
*        Copy X' into rows m+1:m+nrhs of work
*
         DO 40 I = 1, N
            DO 30 J = 1, NRHS
               WORK( M+J+( I-1 )*LDWORK ) = CONJG( X( I, J ) )
   30       CONTINUE
   40    CONTINUE
*
         XNRM = CLANGE( 'M', NRHS, N, WORK( M+1 ), LDWORK, RWORK )
         IF( XNRM.NE.ZERO )
     $      CALL CLASCL( 'G', 0, 0, XNRM, ONE, NRHS, N, WORK( M+1 ),
     $                   LDWORK, INFO )
*
*        Compute LQ factorization of work
*
         CALL CGELQ2( LDWORK, N, WORK, LDWORK, WORK( LDWORK*N+1 ),
     $                WORK( LDWORK*( N+1 )+1 ), INFO )
*
*        Compute largest entry in lower triangle in
*        work(m+1:m+nrhs,m+1:n)
*
         ERR = ZERO
         DO 60 J = M + 1, N
            DO 50 I = J, LDWORK
               ERR = MAX( ERR, ABS( WORK( I+( J-1 )*LDWORK ) ) )
   50       CONTINUE
   60    CONTINUE
*
      END IF
*
      CQRT14 = ERR / ( REAL( MAX( M, N, NRHS ) )*SLAMCH( 'Epsilon' ) )
*
      RETURN
*
*     End of CQRT14
*
      END
