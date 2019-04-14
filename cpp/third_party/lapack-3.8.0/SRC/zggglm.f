*> \brief \b ZGGGLM
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGGGLM + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zggglm.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zggglm.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zggglm.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGGGLM( N, M, P, A, LDA, B, LDB, D, X, Y, WORK, LWORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDB, LWORK, M, N, P
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), D( * ), WORK( * ),
*      $                   X( * ), Y( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGGGLM solves a general Gauss-Markov linear model (GLM) problem:
*>
*>         minimize || y ||_2   subject to   d = A*x + B*y
*>             x
*>
*> where A is an N-by-M matrix, B is an N-by-P matrix, and d is a
*> given N-vector. It is assumed that M <= N <= M+P, and
*>
*>            rank(A) = M    and    rank( A B ) = N.
*>
*> Under these assumptions, the constrained equation is always
*> consistent, and there is a unique solution x and a minimal 2-norm
*> solution y, which is obtained using a generalized QR factorization
*> of the matrices (A, B) given by
*>
*>    A = Q*(R),   B = Q*T*Z.
*>          (0)
*>
*> In particular, if matrix B is square nonsingular, then the problem
*> GLM is equivalent to the following weighted linear least squares
*> problem
*>
*>              minimize || inv(B)*(d-A*x) ||_2
*>                  x
*>
*> where inv(B) denotes the inverse of B.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of columns of the matrix A.  0 <= M <= N.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of columns of the matrix B.  P >= N-M.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,M)
*>          On entry, the N-by-M matrix A.
*>          On exit, the upper triangular part of the array A contains
*>          the M-by-M upper triangular matrix R.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,P)
*>          On entry, the N-by-P matrix B.
*>          On exit, if N <= P, the upper triangle of the subarray
*>          B(1:N,P-N+1:P) contains the N-by-N upper triangular matrix T;
*>          if N > P, the elements on and above the (N-P)th subdiagonal
*>          contain the N-by-P upper trapezoidal matrix T.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is COMPLEX*16 array, dimension (N)
*>          On entry, D is the left hand side of the GLM equation.
*>          On exit, D is destroyed.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (M)
*> \endverbatim
*>
*> \param[out] Y
*> \verbatim
*>          Y is COMPLEX*16 array, dimension (P)
*>
*>          On exit, X and Y are the solutions of the GLM problem.
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
*>          The dimension of the array WORK. LWORK >= max(1,N+M+P).
*>          For optimum performance, LWORK >= M+min(N,P)+max(N,P)*NB,
*>          where NB is an upper bound for the optimal blocksizes for
*>          ZGEQRF, ZGERQF, ZUNMQR and ZUNMRQ.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          = 1:  the upper triangular factor R associated with A in the
*>                generalized QR factorization of the pair (A, B) is
*>                singular, so that rank(A) < M; the least squares
*>                solution could not be computed.
*>          = 2:  the bottom (N-M) by (N-M) part of the upper trapezoidal
*>                factor T associated with B in the generalized QR
*>                factorization of the pair (A, B) is singular, so that
*>                rank( A B ) < N; the least squares solution could not
*>                be computed.
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
*> \ingroup complex16OTHEReigen
*
*  =====================================================================
      SUBROUTINE ZGGGLM( N, M, P, A, LDA, B, LDB, D, X, Y, WORK, LWORK,
     $                   INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDB, LWORK, M, N, P
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), D( * ), WORK( * ),
     $                   X( * ), Y( * )
*     ..
*
*  ===================================================================
*
*     .. Parameters ..
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            I, LOPT, LWKMIN, LWKOPT, NB, NB1, NB2, NB3,
     $                   NB4, NP
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZCOPY, ZGEMV, ZGGQRF, ZTRTRS, ZUNMQR,
     $                   ZUNMRQ
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      EXTERNAL           ILAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          INT, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      INFO = 0
      NP = MIN( N, P )
      LQUERY = ( LWORK.EQ.-1 )
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( M.LT.0 .OR. M.GT.N ) THEN
         INFO = -2
      ELSE IF( P.LT.0 .OR. P.LT.N-M ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -7
      END IF
*
*     Calculate workspace
*
      IF( INFO.EQ.0) THEN
         IF( N.EQ.0 ) THEN
            LWKMIN = 1
            LWKOPT = 1
         ELSE
            NB1 = ILAENV( 1, 'ZGEQRF', ' ', N, M, -1, -1 )
            NB2 = ILAENV( 1, 'ZGERQF', ' ', N, M, -1, -1 )
            NB3 = ILAENV( 1, 'ZUNMQR', ' ', N, M, P, -1 )
            NB4 = ILAENV( 1, 'ZUNMRQ', ' ', N, M, P, -1 )
            NB = MAX( NB1, NB2, NB3, NB4 )
            LWKMIN = M + N + P
            LWKOPT = M + NP + MAX( N, P )*NB
         END IF
         WORK( 1 ) = LWKOPT
*
         IF( LWORK.LT.LWKMIN .AND. .NOT.LQUERY ) THEN
            INFO = -12
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGGGLM', -INFO )
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
*     Compute the GQR factorization of matrices A and B:
*
*          Q**H*A = ( R11 ) M,    Q**H*B*Z**H = ( T11   T12 ) M
*                   (  0  ) N-M                 (  0    T22 ) N-M
*                      M                         M+P-N  N-M
*
*     where R11 and T22 are upper triangular, and Q and Z are
*     unitary.
*
      CALL ZGGQRF( N, M, P, A, LDA, WORK, B, LDB, WORK( M+1 ),
     $             WORK( M+NP+1 ), LWORK-M-NP, INFO )
      LOPT = WORK( M+NP+1 )
*
*     Update left-hand-side vector d = Q**H*d = ( d1 ) M
*                                               ( d2 ) N-M
*
      CALL ZUNMQR( 'Left', 'Conjugate transpose', N, 1, M, A, LDA, WORK,
     $             D, MAX( 1, N ), WORK( M+NP+1 ), LWORK-M-NP, INFO )
      LOPT = MAX( LOPT, INT( WORK( M+NP+1 ) ) )
*
*     Solve T22*y2 = d2 for y2
*
      IF( N.GT.M ) THEN
         CALL ZTRTRS( 'Upper', 'No transpose', 'Non unit', N-M, 1,
     $                B( M+1, M+P-N+1 ), LDB, D( M+1 ), N-M, INFO )
*
         IF( INFO.GT.0 ) THEN
            INFO = 1
            RETURN
         END IF
*
         CALL ZCOPY( N-M, D( M+1 ), 1, Y( M+P-N+1 ), 1 )
      END IF
*
*     Set y1 = 0
*
      DO 10 I = 1, M + P - N
         Y( I ) = CZERO
   10 CONTINUE
*
*     Update d1 = d1 - T12*y2
*
      CALL ZGEMV( 'No transpose', M, N-M, -CONE, B( 1, M+P-N+1 ), LDB,
     $            Y( M+P-N+1 ), 1, CONE, D, 1 )
*
*     Solve triangular system: R11*x = d1
*
      IF( M.GT.0 ) THEN
         CALL ZTRTRS( 'Upper', 'No Transpose', 'Non unit', M, 1, A, LDA,
     $                D, M, INFO )
*
         IF( INFO.GT.0 ) THEN
            INFO = 2
            RETURN
         END IF
*
*        Copy D to X
*
         CALL ZCOPY( M, D, 1, X, 1 )
      END IF
*
*     Backward transformation y = Z**H *y
*
      CALL ZUNMRQ( 'Left', 'Conjugate transpose', P, 1, NP,
     $             B( MAX( 1, N-P+1 ), 1 ), LDB, WORK( M+1 ), Y,
     $             MAX( 1, P ), WORK( M+NP+1 ), LWORK-M-NP, INFO )
      WORK( 1 ) = M + NP + MAX( LOPT, INT( WORK( M+NP+1 ) ) )
*
      RETURN
*
*     End of ZGGGLM
*
      END
