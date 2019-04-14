*> \brief \b ZTGSJA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTGSJA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztgsja.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztgsja.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztgsja.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTGSJA( JOBU, JOBV, JOBQ, M, P, N, K, L, A, LDA, B,
*                          LDB, TOLA, TOLB, ALPHA, BETA, U, LDU, V, LDV,
*                          Q, LDQ, WORK, NCYCLE, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBQ, JOBU, JOBV
*       INTEGER            INFO, K, L, LDA, LDB, LDQ, LDU, LDV, M, N,
*      $                   NCYCLE, P
*       DOUBLE PRECISION   TOLA, TOLB
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   ALPHA( * ), BETA( * )
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
*      $                   U( LDU, * ), V( LDV, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTGSJA computes the generalized singular value decomposition (GSVD)
*> of two complex upper triangular (or trapezoidal) matrices A and B.
*>
*> On entry, it is assumed that matrices A and B have the following
*> forms, which may be obtained by the preprocessing subroutine ZGGSVP
*> from a general M-by-N matrix A and P-by-N matrix B:
*>
*>              N-K-L  K    L
*>    A =    K ( 0    A12  A13 ) if M-K-L >= 0;
*>           L ( 0     0   A23 )
*>       M-K-L ( 0     0    0  )
*>
*>            N-K-L  K    L
*>    A =  K ( 0    A12  A13 ) if M-K-L < 0;
*>       M-K ( 0     0   A23 )
*>
*>            N-K-L  K    L
*>    B =  L ( 0     0   B13 )
*>       P-L ( 0     0    0  )
*>
*> where the K-by-K matrix A12 and L-by-L matrix B13 are nonsingular
*> upper triangular; A23 is L-by-L upper triangular if M-K-L >= 0,
*> otherwise A23 is (M-K)-by-L upper trapezoidal.
*>
*> On exit,
*>
*>        U**H *A*Q = D1*( 0 R ),    V**H *B*Q = D2*( 0 R ),
*>
*> where U, V and Q are unitary matrices.
*> R is a nonsingular upper triangular matrix, and D1
*> and D2 are ``diagonal'' matrices, which are of the following
*> structures:
*>
*> If M-K-L >= 0,
*>
*>                     K  L
*>        D1 =     K ( I  0 )
*>                 L ( 0  C )
*>             M-K-L ( 0  0 )
*>
*>                    K  L
*>        D2 = L   ( 0  S )
*>             P-L ( 0  0 )
*>
*>                N-K-L  K    L
*>   ( 0 R ) = K (  0   R11  R12 ) K
*>             L (  0    0   R22 ) L
*>
*> where
*>
*>   C = diag( ALPHA(K+1), ... , ALPHA(K+L) ),
*>   S = diag( BETA(K+1),  ... , BETA(K+L) ),
*>   C**2 + S**2 = I.
*>
*>   R is stored in A(1:K+L,N-K-L+1:N) on exit.
*>
*> If M-K-L < 0,
*>
*>                K M-K K+L-M
*>     D1 =   K ( I  0    0   )
*>          M-K ( 0  C    0   )
*>
*>                  K M-K K+L-M
*>     D2 =   M-K ( 0  S    0   )
*>          K+L-M ( 0  0    I   )
*>            P-L ( 0  0    0   )
*>
*>                N-K-L  K   M-K  K+L-M
*> ( 0 R ) =    K ( 0    R11  R12  R13  )
*>           M-K ( 0     0   R22  R23  )
*>         K+L-M ( 0     0    0   R33  )
*>
*> where
*> C = diag( ALPHA(K+1), ... , ALPHA(M) ),
*> S = diag( BETA(K+1),  ... , BETA(M) ),
*> C**2 + S**2 = I.
*>
*> R = ( R11 R12 R13 ) is stored in A(1:M, N-K-L+1:N) and R33 is stored
*>     (  0  R22 R23 )
*> in B(M-K+1:L,N+M-K-L+1:N) on exit.
*>
*> The computation of the unitary transformation matrices U, V or Q
*> is optional.  These matrices may either be formed explicitly, or they
*> may be postmultiplied into input matrices U1, V1, or Q1.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>          = 'U':  U must contain a unitary matrix U1 on entry, and
*>                  the product U1*U is returned;
*>          = 'I':  U is initialized to the unit matrix, and the
*>                  unitary matrix U is returned;
*>          = 'N':  U is not computed.
*> \endverbatim
*>
*> \param[in] JOBV
*> \verbatim
*>          JOBV is CHARACTER*1
*>          = 'V':  V must contain a unitary matrix V1 on entry, and
*>                  the product V1*V is returned;
*>          = 'I':  V is initialized to the unit matrix, and the
*>                  unitary matrix V is returned;
*>          = 'N':  V is not computed.
*> \endverbatim
*>
*> \param[in] JOBQ
*> \verbatim
*>          JOBQ is CHARACTER*1
*>          = 'Q':  Q must contain a unitary matrix Q1 on entry, and
*>                  the product Q1*Q is returned;
*>          = 'I':  Q is initialized to the unit matrix, and the
*>                  unitary matrix Q is returned;
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
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*> \endverbatim
*>
*> \param[in] L
*> \verbatim
*>          L is INTEGER
*>
*>          K and L specify the subblocks in the input matrices A and B:
*>          A23 = A(K+1:MIN(K+L,M),N-L+1:N) and B13 = B(1:L,,N-L+1:N)
*>          of A and B, whose GSVD is going to be computed by ZTGSJA.
*>          See Further Details.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, A(N-K+1:N,1:MIN(K+L,M) ) contains the triangular
*>          matrix R or part of R.  See Purpose for details.
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
*>          B is COMPLEX*16 array, dimension (LDB,N)
*>          On entry, the P-by-N matrix B.
*>          On exit, if necessary, B(M-K+1:L,N+M-K-L+1:N) contains
*>          a part of R.  See Purpose for details.
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
*>          TOLA is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[in] TOLB
*> \verbatim
*>          TOLB is DOUBLE PRECISION
*>
*>          TOLA and TOLB are the convergence criteria for the Jacobi-
*>          Kogbetliantz iteration procedure. Generally, they are the
*>          same as used in the preprocessing step, say
*>              TOLA = MAX(M,N)*norm(A)*MAZHEPS,
*>              TOLB = MAX(P,N)*norm(B)*MAZHEPS.
*> \endverbatim
*>
*> \param[out] ALPHA
*> \verbatim
*>          ALPHA is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is DOUBLE PRECISION array, dimension (N)
*>
*>          On exit, ALPHA and BETA contain the generalized singular
*>          value pairs of A and B;
*>            ALPHA(1:K) = 1,
*>            BETA(1:K)  = 0,
*>          and if M-K-L >= 0,
*>            ALPHA(K+1:K+L) = diag(C),
*>            BETA(K+1:K+L)  = diag(S),
*>          or if M-K-L < 0,
*>            ALPHA(K+1:M)= C, ALPHA(M+1:K+L)= 0
*>            BETA(K+1:M) = S, BETA(M+1:K+L) = 1.
*>          Furthermore, if K+L < N,
*>            ALPHA(K+L+1:N) = 0 and
*>            BETA(K+L+1:N)  = 0.
*> \endverbatim
*>
*> \param[in,out] U
*> \verbatim
*>          U is COMPLEX*16 array, dimension (LDU,M)
*>          On entry, if JOBU = 'U', U must contain a matrix U1 (usually
*>          the unitary matrix returned by ZGGSVP).
*>          On exit,
*>          if JOBU = 'I', U contains the unitary matrix U;
*>          if JOBU = 'U', U contains the product U1*U.
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
*> \param[in,out] V
*> \verbatim
*>          V is COMPLEX*16 array, dimension (LDV,P)
*>          On entry, if JOBV = 'V', V must contain a matrix V1 (usually
*>          the unitary matrix returned by ZGGSVP).
*>          On exit,
*>          if JOBV = 'I', V contains the unitary matrix V;
*>          if JOBV = 'V', V contains the product V1*V.
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
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDQ,N)
*>          On entry, if JOBQ = 'Q', Q must contain a matrix Q1 (usually
*>          the unitary matrix returned by ZGGSVP).
*>          On exit,
*>          if JOBQ = 'I', Q contains the unitary matrix Q;
*>          if JOBQ = 'Q', Q contains the product Q1*Q.
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
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] NCYCLE
*> \verbatim
*>          NCYCLE is INTEGER
*>          The number of cycles required for convergence.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          = 1:  the procedure does not converge after MAXIT cycles.
*> \endverbatim
*
*> \par Internal Parameters:
*  =========================
*>
*> \verbatim
*>  MAXIT   INTEGER
*>          MAXIT specifies the total loops that the iterative procedure
*>          may take. If after MAXIT cycles, the routine fails to
*>          converge, we return INFO = 1.
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
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  ZTGSJA essentially uses a variant of Kogbetliantz algorithm to reduce
*>  min(L,M-K)-by-L triangular (or trapezoidal) matrix A23 and L-by-L
*>  matrix B13 to the form:
*>
*>           U1**H *A13*Q1 = C1*R1; V1**H *B13*Q1 = S1*R1,
*>
*>  where U1, V1 and Q1 are unitary matrix.
*>  C1 and S1 are diagonal matrices satisfying
*>
*>                C1**2 + S1**2 = I,
*>
*>  and R1 is an L-by-L nonsingular upper triangular matrix.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZTGSJA( JOBU, JOBV, JOBQ, M, P, N, K, L, A, LDA, B,
     $                   LDB, TOLA, TOLB, ALPHA, BETA, U, LDU, V, LDV,
     $                   Q, LDQ, WORK, NCYCLE, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBQ, JOBU, JOBV
      INTEGER            INFO, K, L, LDA, LDB, LDQ, LDU, LDV, M, N,
     $                   NCYCLE, P
      DOUBLE PRECISION   TOLA, TOLB
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   ALPHA( * ), BETA( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
     $                   U( LDU, * ), V( LDV, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            MAXIT
      PARAMETER          ( MAXIT = 40 )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
*
      LOGICAL            INITQ, INITU, INITV, UPPER, WANTQ, WANTU, WANTV
      INTEGER            I, J, KCYCLE
      DOUBLE PRECISION   A1, A3, B1, B3, CSQ, CSU, CSV, ERROR, GAMMA,
     $                   RWK, SSMIN
      COMPLEX*16         A2, B2, SNQ, SNU, SNV
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLARTG, XERBLA, ZCOPY, ZDSCAL, ZLAGS2, ZLAPLL,
     $                   ZLASET, ZROT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters
*
      INITU = LSAME( JOBU, 'I' )
      WANTU = INITU .OR. LSAME( JOBU, 'U' )
*
      INITV = LSAME( JOBV, 'I' )
      WANTV = INITV .OR. LSAME( JOBV, 'V' )
*
      INITQ = LSAME( JOBQ, 'I' )
      WANTQ = INITQ .OR. LSAME( JOBQ, 'Q' )
*
      INFO = 0
      IF( .NOT.( INITU .OR. WANTU .OR. LSAME( JOBU, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( INITV .OR. WANTV .OR. LSAME( JOBV, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( INITQ .OR. WANTQ .OR. LSAME( JOBQ, 'N' ) ) ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( P.LT.0 ) THEN
         INFO = -5
      ELSE IF( N.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -10
      ELSE IF( LDB.LT.MAX( 1, P ) ) THEN
         INFO = -12
      ELSE IF( LDU.LT.1 .OR. ( WANTU .AND. LDU.LT.M ) ) THEN
         INFO = -18
      ELSE IF( LDV.LT.1 .OR. ( WANTV .AND. LDV.LT.P ) ) THEN
         INFO = -20
      ELSE IF( LDQ.LT.1 .OR. ( WANTQ .AND. LDQ.LT.N ) ) THEN
         INFO = -22
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTGSJA', -INFO )
         RETURN
      END IF
*
*     Initialize U, V and Q, if necessary
*
      IF( INITU )
     $   CALL ZLASET( 'Full', M, M, CZERO, CONE, U, LDU )
      IF( INITV )
     $   CALL ZLASET( 'Full', P, P, CZERO, CONE, V, LDV )
      IF( INITQ )
     $   CALL ZLASET( 'Full', N, N, CZERO, CONE, Q, LDQ )
*
*     Loop until convergence
*
      UPPER = .FALSE.
      DO 40 KCYCLE = 1, MAXIT
*
         UPPER = .NOT.UPPER
*
         DO 20 I = 1, L - 1
            DO 10 J = I + 1, L
*
               A1 = ZERO
               A2 = CZERO
               A3 = ZERO
               IF( K+I.LE.M )
     $            A1 = DBLE( A( K+I, N-L+I ) )
               IF( K+J.LE.M )
     $            A3 = DBLE( A( K+J, N-L+J ) )
*
               B1 = DBLE( B( I, N-L+I ) )
               B3 = DBLE( B( J, N-L+J ) )
*
               IF( UPPER ) THEN
                  IF( K+I.LE.M )
     $               A2 = A( K+I, N-L+J )
                  B2 = B( I, N-L+J )
               ELSE
                  IF( K+J.LE.M )
     $               A2 = A( K+J, N-L+I )
                  B2 = B( J, N-L+I )
               END IF
*
               CALL ZLAGS2( UPPER, A1, A2, A3, B1, B2, B3, CSU, SNU,
     $                      CSV, SNV, CSQ, SNQ )
*
*              Update (K+I)-th and (K+J)-th rows of matrix A: U**H *A
*
               IF( K+J.LE.M )
     $            CALL ZROT( L, A( K+J, N-L+1 ), LDA, A( K+I, N-L+1 ),
     $                       LDA, CSU, DCONJG( SNU ) )
*
*              Update I-th and J-th rows of matrix B: V**H *B
*
               CALL ZROT( L, B( J, N-L+1 ), LDB, B( I, N-L+1 ), LDB,
     $                    CSV, DCONJG( SNV ) )
*
*              Update (N-L+I)-th and (N-L+J)-th columns of matrices
*              A and B: A*Q and B*Q
*
               CALL ZROT( MIN( K+L, M ), A( 1, N-L+J ), 1,
     $                    A( 1, N-L+I ), 1, CSQ, SNQ )
*
               CALL ZROT( L, B( 1, N-L+J ), 1, B( 1, N-L+I ), 1, CSQ,
     $                    SNQ )
*
               IF( UPPER ) THEN
                  IF( K+I.LE.M )
     $               A( K+I, N-L+J ) = CZERO
                  B( I, N-L+J ) = CZERO
               ELSE
                  IF( K+J.LE.M )
     $               A( K+J, N-L+I ) = CZERO
                  B( J, N-L+I ) = CZERO
               END IF
*
*              Ensure that the diagonal elements of A and B are real.
*
               IF( K+I.LE.M )
     $            A( K+I, N-L+I ) = DBLE( A( K+I, N-L+I ) )
               IF( K+J.LE.M )
     $            A( K+J, N-L+J ) = DBLE( A( K+J, N-L+J ) )
               B( I, N-L+I ) = DBLE( B( I, N-L+I ) )
               B( J, N-L+J ) = DBLE( B( J, N-L+J ) )
*
*              Update unitary matrices U, V, Q, if desired.
*
               IF( WANTU .AND. K+J.LE.M )
     $            CALL ZROT( M, U( 1, K+J ), 1, U( 1, K+I ), 1, CSU,
     $                       SNU )
*
               IF( WANTV )
     $            CALL ZROT( P, V( 1, J ), 1, V( 1, I ), 1, CSV, SNV )
*
               IF( WANTQ )
     $            CALL ZROT( N, Q( 1, N-L+J ), 1, Q( 1, N-L+I ), 1, CSQ,
     $                       SNQ )
*
   10       CONTINUE
   20    CONTINUE
*
         IF( .NOT.UPPER ) THEN
*
*           The matrices A13 and B13 were lower triangular at the start
*           of the cycle, and are now upper triangular.
*
*           Convergence test: test the parallelism of the corresponding
*           rows of A and B.
*
            ERROR = ZERO
            DO 30 I = 1, MIN( L, M-K )
               CALL ZCOPY( L-I+1, A( K+I, N-L+I ), LDA, WORK, 1 )
               CALL ZCOPY( L-I+1, B( I, N-L+I ), LDB, WORK( L+1 ), 1 )
               CALL ZLAPLL( L-I+1, WORK, 1, WORK( L+1 ), 1, SSMIN )
               ERROR = MAX( ERROR, SSMIN )
   30       CONTINUE
*
            IF( ABS( ERROR ).LE.MIN( TOLA, TOLB ) )
     $         GO TO 50
         END IF
*
*        End of cycle loop
*
   40 CONTINUE
*
*     The algorithm has not converged after MAXIT cycles.
*
      INFO = 1
      GO TO 100
*
   50 CONTINUE
*
*     If ERROR <= MIN(TOLA,TOLB), then the algorithm has converged.
*     Compute the generalized singular value pairs (ALPHA, BETA), and
*     set the triangular matrix R to array A.
*
      DO 60 I = 1, K
         ALPHA( I ) = ONE
         BETA( I ) = ZERO
   60 CONTINUE
*
      DO 70 I = 1, MIN( L, M-K )
*
         A1 = DBLE( A( K+I, N-L+I ) )
         B1 = DBLE( B( I, N-L+I ) )
*
         IF( A1.NE.ZERO ) THEN
            GAMMA = B1 / A1
*
            IF( GAMMA.LT.ZERO ) THEN
               CALL ZDSCAL( L-I+1, -ONE, B( I, N-L+I ), LDB )
               IF( WANTV )
     $            CALL ZDSCAL( P, -ONE, V( 1, I ), 1 )
            END IF
*
            CALL DLARTG( ABS( GAMMA ), ONE, BETA( K+I ), ALPHA( K+I ),
     $                   RWK )
*
            IF( ALPHA( K+I ).GE.BETA( K+I ) ) THEN
               CALL ZDSCAL( L-I+1, ONE / ALPHA( K+I ), A( K+I, N-L+I ),
     $                      LDA )
            ELSE
               CALL ZDSCAL( L-I+1, ONE / BETA( K+I ), B( I, N-L+I ),
     $                      LDB )
               CALL ZCOPY( L-I+1, B( I, N-L+I ), LDB, A( K+I, N-L+I ),
     $                     LDA )
            END IF
*
         ELSE
*
            ALPHA( K+I ) = ZERO
            BETA( K+I ) = ONE
            CALL ZCOPY( L-I+1, B( I, N-L+I ), LDB, A( K+I, N-L+I ),
     $                  LDA )
         END IF
   70 CONTINUE
*
*     Post-assignment
*
      DO 80 I = M + 1, K + L
         ALPHA( I ) = ZERO
         BETA( I ) = ONE
   80 CONTINUE
*
      IF( K+L.LT.N ) THEN
         DO 90 I = K + L + 1, N
            ALPHA( I ) = ZERO
            BETA( I ) = ZERO
   90    CONTINUE
      END IF
*
  100 CONTINUE
      NCYCLE = KCYCLE
*
      RETURN
*
*     End of ZTGSJA
*
      END
