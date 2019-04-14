*> \brief \b CGGSVP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CGGSVP + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cggsvp.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cggsvp.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cggsvp.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGGSVP( JOBU, JOBV, JOBQ, M, P, N, A, LDA, B, LDB,
*                          TOLA, TOLB, K, L, U, LDU, V, LDV, Q, LDQ,
*                          IWORK, RWORK, TAU, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBQ, JOBU, JOBV
*       INTEGER            INFO, K, L, LDA, LDB, LDQ, LDU, LDV, M, N, P
*       REAL               TOLA, TOLB
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
*      $                   TAU( * ), U( LDU, * ), V( LDV, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This routine is deprecated and has been replaced by routine CGGSVP3.
*>
*> CGGSVP computes unitary matrices U, V and Q such that
*>
*>                    N-K-L  K    L
*>  U**H*A*Q =     K ( 0    A12  A13 )  if M-K-L >= 0;
*>                 L ( 0     0   A23 )
*>             M-K-L ( 0     0    0  )
*>
*>                  N-K-L  K    L
*>         =     K ( 0    A12  A13 )  if M-K-L < 0;
*>             M-K ( 0     0   A23 )
*>
*>                  N-K-L  K    L
*>  V**H*B*Q =   L ( 0     0   B13 )
*>             P-L ( 0     0    0  )
*>
*> where the K-by-K matrix A12 and L-by-L matrix B13 are nonsingular
*> upper triangular; A23 is L-by-L upper triangular if M-K-L >= 0,
*> otherwise A23 is (M-K)-by-L upper trapezoidal.  K+L = the effective
*> numerical rank of the (M+P)-by-N matrix (A**H,B**H)**H.
*>
*> This decomposition is the preprocessing step for computing the
*> Generalized Singular Value Decomposition (GSVD), see subroutine
*> CGGSVD.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>          = 'U':  Unitary matrix U is computed;
*>          = 'N':  U is not computed.
*> \endverbatim
*>
*> \param[in] JOBV
*> \verbatim
*>          JOBV is CHARACTER*1
*>          = 'V':  Unitary matrix V is computed;
*>          = 'N':  V is not computed.
*> \endverbatim
*>
*> \param[in] JOBQ
*> \verbatim
*>          JOBQ is CHARACTER*1
*>          = 'Q':  Unitary matrix Q is computed;
*>          = 'N':  Q is not computed.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of rows of the matrix B.  P >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, A contains the triangular (or trapezoidal) matrix
*>          described in the Purpose section.
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
*>          B is COMPLEX array, dimension (LDB,N)
*>          On entry, the P-by-N matrix B.
*>          On exit, B contains the triangular matrix described in
*>          the Purpose section.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,P).
*> \endverbatim
*>
*> \param[in] TOLA
*> \verbatim
*>          TOLA is REAL
*> \endverbatim
*>
*> \param[in] TOLB
*> \verbatim
*>          TOLB is REAL
*>
*>          TOLA and TOLB are the thresholds to determine the effective
*>          numerical rank of matrix B and a subblock of A. Generally,
*>          they are set to
*>             TOLA = MAX(M,N)*norm(A)*MACHEPS,
*>             TOLB = MAX(P,N)*norm(B)*MACHEPS.
*>          The size of TOLA and TOLB may affect the size of backward
*>          errors of the decomposition.
*> \endverbatim
*>
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*> \endverbatim
*>
*> \param[out] L
*> \verbatim
*>          L is INTEGER
*>
*>          On exit, K and L specify the dimension of the subblocks
*>          described in Purpose section.
*>          K + L = effective numerical rank of (A**H,B**H)**H.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX array, dimension (LDU,M)
*>          If JOBU = 'U', U contains the unitary matrix U.
*>          If JOBU = 'N', U is not referenced.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U. LDU >= max(1,M) if
*>          JOBU = 'U'; LDU >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is COMPLEX array, dimension (LDV,P)
*>          If JOBV = 'V', V contains the unitary matrix V.
*>          If JOBV = 'N', V is not referenced.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V. LDV >= max(1,P) if
*>          JOBV = 'V'; LDV >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is COMPLEX array, dimension (LDQ,N)
*>          If JOBQ = 'Q', Q contains the unitary matrix Q.
*>          If JOBQ = 'N', Q is not referenced.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q. LDQ >= max(1,N) if
*>          JOBQ = 'Q'; LDQ >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX array, dimension (N)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (max(3*N,M,P))
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \ingroup complexOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*>  The subroutine uses LAPACK subroutine CGEQPF for the QR factorization
*>  with column pivoting to detect the effective numerical rank of the
*>  a matrix. It may be replaced by a better rank determination strategy.
*>
*  =====================================================================
      SUBROUTINE CGGSVP( JOBU, JOBV, JOBQ, M, P, N, A, LDA, B, LDB,
     $                   TOLA, TOLB, K, L, U, LDU, V, LDV, Q, LDQ,
     $                   IWORK, RWORK, TAU, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBQ, JOBU, JOBV
      INTEGER            INFO, K, L, LDA, LDB, LDQ, LDU, LDV, M, N, P
      REAL               TOLA, TOLB
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               RWORK( * )
      COMPLEX            A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
     $                   TAU( * ), U( LDU, * ), V( LDV, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            FORWRD, WANTQ, WANTU, WANTV
      INTEGER            I, J
      COMPLEX            T
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEQPF, CGEQR2, CGERQ2, CLACPY, CLAPMT, CLASET,
     $                   CUNG2R, CUNM2R, CUNMR2, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, MAX, MIN, REAL
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( T ) = ABS( REAL( T ) ) + ABS( AIMAG( T ) )
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      WANTU = LSAME( JOBU, 'U' )
      WANTV = LSAME( JOBV, 'V' )
      WANTQ = LSAME( JOBQ, 'Q' )
      FORWRD = .TRUE.
*
      INFO = 0
      IF( .NOT.( WANTU .OR. LSAME( JOBU, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( WANTV .OR. LSAME( JOBV, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( WANTQ .OR. LSAME( JOBQ, 'N' ) ) ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( P.LT.0 ) THEN
         INFO = -5
      ELSE IF( N.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -8
      ELSE IF( LDB.LT.MAX( 1, P ) ) THEN
         INFO = -10
      ELSE IF( LDU.LT.1 .OR. ( WANTU .AND. LDU.LT.M ) ) THEN
         INFO = -16
      ELSE IF( LDV.LT.1 .OR. ( WANTV .AND. LDV.LT.P ) ) THEN
         INFO = -18
      ELSE IF( LDQ.LT.1 .OR. ( WANTQ .AND. LDQ.LT.N ) ) THEN
         INFO = -20
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CGGSVP', -INFO )
         RETURN
      END IF
*
*     QR with column pivoting of B: B*P = V*( S11 S12 )
*                                           (  0   0  )
*
      DO 10 I = 1, N
         IWORK( I ) = 0
   10 CONTINUE
      CALL CGEQPF( P, N, B, LDB, IWORK, TAU, WORK, RWORK, INFO )
*
*     Update A := A*P
*
      CALL CLAPMT( FORWRD, M, N, A, LDA, IWORK )
*
*     Determine the effective rank of matrix B.
*
      L = 0
      DO 20 I = 1, MIN( P, N )
         IF( CABS1( B( I, I ) ).GT.TOLB )
     $      L = L + 1
   20 CONTINUE
*
      IF( WANTV ) THEN
*
*        Copy the details of V, and form V.
*
         CALL CLASET( 'Full', P, P, CZERO, CZERO, V, LDV )
         IF( P.GT.1 )
     $      CALL CLACPY( 'Lower', P-1, N, B( 2, 1 ), LDB, V( 2, 1 ),
     $                   LDV )
         CALL CUNG2R( P, P, MIN( P, N ), V, LDV, TAU, WORK, INFO )
      END IF
*
*     Clean up B
*
      DO 40 J = 1, L - 1
         DO 30 I = J + 1, L
            B( I, J ) = CZERO
   30    CONTINUE
   40 CONTINUE
      IF( P.GT.L )
     $   CALL CLASET( 'Full', P-L, N, CZERO, CZERO, B( L+1, 1 ), LDB )
*
      IF( WANTQ ) THEN
*
*        Set Q = I and Update Q := Q*P
*
         CALL CLASET( 'Full', N, N, CZERO, CONE, Q, LDQ )
         CALL CLAPMT( FORWRD, N, N, Q, LDQ, IWORK )
      END IF
*
      IF( P.GE.L .AND. N.NE.L ) THEN
*
*        RQ factorization of ( S11 S12 ) = ( 0 S12 )*Z
*
         CALL CGERQ2( L, N, B, LDB, TAU, WORK, INFO )
*
*        Update A := A*Z**H
*
         CALL CUNMR2( 'Right', 'Conjugate transpose', M, N, L, B, LDB,
     $                TAU, A, LDA, WORK, INFO )
         IF( WANTQ ) THEN
*
*           Update Q := Q*Z**H
*
            CALL CUNMR2( 'Right', 'Conjugate transpose', N, N, L, B,
     $                   LDB, TAU, Q, LDQ, WORK, INFO )
         END IF
*
*        Clean up B
*
         CALL CLASET( 'Full', L, N-L, CZERO, CZERO, B, LDB )
         DO 60 J = N - L + 1, N
            DO 50 I = J - N + L + 1, L
               B( I, J ) = CZERO
   50       CONTINUE
   60    CONTINUE
*
      END IF
*
*     Let              N-L     L
*                A = ( A11    A12 ) M,
*
*     then the following does the complete QR decomposition of A11:
*
*              A11 = U*(  0  T12 )*P1**H
*                      (  0   0  )
*
      DO 70 I = 1, N - L
         IWORK( I ) = 0
   70 CONTINUE
      CALL CGEQPF( M, N-L, A, LDA, IWORK, TAU, WORK, RWORK, INFO )
*
*     Determine the effective rank of A11
*
      K = 0
      DO 80 I = 1, MIN( M, N-L )
         IF( CABS1( A( I, I ) ).GT.TOLA )
     $      K = K + 1
   80 CONTINUE
*
*     Update A12 := U**H*A12, where A12 = A( 1:M, N-L+1:N )
*
      CALL CUNM2R( 'Left', 'Conjugate transpose', M, L, MIN( M, N-L ),
     $             A, LDA, TAU, A( 1, N-L+1 ), LDA, WORK, INFO )
*
      IF( WANTU ) THEN
*
*        Copy the details of U, and form U
*
         CALL CLASET( 'Full', M, M, CZERO, CZERO, U, LDU )
         IF( M.GT.1 )
     $      CALL CLACPY( 'Lower', M-1, N-L, A( 2, 1 ), LDA, U( 2, 1 ),
     $                   LDU )
         CALL CUNG2R( M, M, MIN( M, N-L ), U, LDU, TAU, WORK, INFO )
      END IF
*
      IF( WANTQ ) THEN
*
*        Update Q( 1:N, 1:N-L )  = Q( 1:N, 1:N-L )*P1
*
         CALL CLAPMT( FORWRD, N, N-L, Q, LDQ, IWORK )
      END IF
*
*     Clean up A: set the strictly lower triangular part of
*     A(1:K, 1:K) = 0, and A( K+1:M, 1:N-L ) = 0.
*
      DO 100 J = 1, K - 1
         DO 90 I = J + 1, K
            A( I, J ) = CZERO
   90    CONTINUE
  100 CONTINUE
      IF( M.GT.K )
     $   CALL CLASET( 'Full', M-K, N-L, CZERO, CZERO, A( K+1, 1 ), LDA )
*
      IF( N-L.GT.K ) THEN
*
*        RQ factorization of ( T11 T12 ) = ( 0 T12 )*Z1
*
         CALL CGERQ2( K, N-L, A, LDA, TAU, WORK, INFO )
*
         IF( WANTQ ) THEN
*
*           Update Q( 1:N,1:N-L ) = Q( 1:N,1:N-L )*Z1**H
*
            CALL CUNMR2( 'Right', 'Conjugate transpose', N, N-L, K, A,
     $                   LDA, TAU, Q, LDQ, WORK, INFO )
         END IF
*
*        Clean up A
*
         CALL CLASET( 'Full', K, N-L-K, CZERO, CZERO, A, LDA )
         DO 120 J = N - L - K + 1, N - L
            DO 110 I = J - N + L + K + 1, K
               A( I, J ) = CZERO
  110       CONTINUE
  120    CONTINUE
*
      END IF
*
      IF( M.GT.K ) THEN
*
*        QR factorization of A( K+1:M,N-L+1:N )
*
         CALL CGEQR2( M-K, L, A( K+1, N-L+1 ), LDA, TAU, WORK, INFO )
*
         IF( WANTU ) THEN
*
*           Update U(:,K+1:M) := U(:,K+1:M)*U1
*
            CALL CUNM2R( 'Right', 'No transpose', M, M-K, MIN( M-K, L ),
     $                   A( K+1, N-L+1 ), LDA, TAU, U( 1, K+1 ), LDU,
     $                   WORK, INFO )
         END IF
*
*        Clean up
*
         DO 140 J = N - L + 1, N
            DO 130 I = J - N + K + L + 1, M
               A( I, J ) = CZERO
  130       CONTINUE
  140    CONTINUE
*
      END IF
*
      RETURN
*
*     End of CGGSVP
*
      END
