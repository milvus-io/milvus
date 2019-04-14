*> \brief \b ZGGQRF
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGGQRF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zggqrf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zggqrf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zggqrf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGGQRF( N, M, P, A, LDA, TAUA, B, LDB, TAUB, WORK,
*                          LWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDB, LWORK, M, N, P
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), TAUA( * ), TAUB( * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGGQRF computes a generalized QR factorization of an N-by-M matrix A
*> and an N-by-P matrix B:
*>
*>             A = Q*R,        B = Q*T*Z,
*>
*> where Q is an N-by-N unitary matrix, Z is a P-by-P unitary matrix,
*> and R and T assume one of the forms:
*>
*> if N >= M,  R = ( R11 ) M  ,   or if N < M,  R = ( R11  R12 ) N,
*>                 (  0  ) N-M                         N   M-N
*>                    M
*>
*> where R11 is upper triangular, and
*>
*> if N <= P,  T = ( 0  T12 ) N,   or if N > P,  T = ( T11 ) N-P,
*>                  P-N  N                           ( T21 ) P
*>                                                      P
*>
*> where T12 or T21 is upper triangular.
*>
*> In particular, if B is square and nonsingular, the GQR factorization
*> of A and B implicitly gives the QR factorization of inv(B)*A:
*>
*>              inv(B)*A = Z**H * (inv(T)*R)
*>
*> where inv(B) denotes the inverse of the matrix B, and Z**H denotes the
*> conjugate transpose of matrix Z.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows of the matrices A and B. N >= 0.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of columns of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of columns of the matrix B.  P >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,M)
*>          On entry, the N-by-M matrix A.
*>          On exit, the elements on and above the diagonal of the array
*>          contain the min(N,M)-by-M upper trapezoidal matrix R (R is
*>          upper triangular if N >= M); the elements below the diagonal,
*>          with the array TAUA, represent the unitary matrix Q as a
*>          product of min(N,M) elementary reflectors (see Further
*>          Details).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] TAUA
*> \verbatim
*>          TAUA is COMPLEX*16 array, dimension (min(N,M))
*>          The scalar factors of the elementary reflectors which
*>          represent the unitary matrix Q (see Further Details).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,P)
*>          On entry, the N-by-P matrix B.
*>          On exit, if N <= P, the upper triangle of the subarray
*>          B(1:N,P-N+1:P) contains the N-by-N upper triangular matrix T;
*>          if N > P, the elements on and above the (N-P)-th subdiagonal
*>          contain the N-by-P upper trapezoidal matrix T; the remaining
*>          elements, with the array TAUB, represent the unitary
*>          matrix Z as a product of elementary reflectors (see Further
*>          Details).
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] TAUB
*> \verbatim
*>          TAUB is COMPLEX*16 array, dimension (min(N,P))
*>          The scalar factors of the elementary reflectors which
*>          represent the unitary matrix Z (see Further Details).
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
*>          The dimension of the array WORK. LWORK >= max(1,N,M,P).
*>          For optimum performance LWORK >= max(N,M,P)*max(NB1,NB2,NB3),
*>          where NB1 is the optimal blocksize for the QR factorization
*>          of an N-by-M matrix, NB2 is the optimal blocksize for the
*>          RQ factorization of an N-by-P matrix, and NB3 is the optimal
*>          blocksize for a call of ZUNMQR.
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
*>           = 0:  successful exit
*>           < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*>  The matrix Q is represented as a product of elementary reflectors
*>
*>     Q = H(1) H(2) . . . H(k), where k = min(n,m).
*>
*>  Each H(i) has the form
*>
*>     H(i) = I - taua * v * v**H
*>
*>  where taua is a complex scalar, and v is a complex vector with
*>  v(1:i-1) = 0 and v(i) = 1; v(i+1:n) is stored on exit in A(i+1:n,i),
*>  and taua in TAUA(i).
*>  To form Q explicitly, use LAPACK subroutine ZUNGQR.
*>  To use Q to update another matrix, use LAPACK subroutine ZUNMQR.
*>
*>  The matrix Z is represented as a product of elementary reflectors
*>
*>     Z = H(1) H(2) . . . H(k), where k = min(n,p).
*>
*>  Each H(i) has the form
*>
*>     H(i) = I - taub * v * v**H
*>
*>  where taub is a complex scalar, and v is a complex vector with
*>  v(p-k+i+1:p) = 0 and v(p-k+i) = 1; v(1:p-k+i-1) is stored on exit in
*>  B(n-k+i,1:p-k+i-1), and taub in TAUB(i).
*>  To form Z explicitly, use LAPACK subroutine ZUNGRQ.
*>  To use Z to update another matrix, use LAPACK subroutine ZUNMRQ.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZGGQRF( N, M, P, A, LDA, TAUA, B, LDB, TAUB, WORK,
     $                   LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDB, LWORK, M, N, P
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), TAUA( * ), TAUB( * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            LOPT, LWKOPT, NB, NB1, NB2, NB3
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZGEQRF, ZGERQF, ZUNMQR
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
      NB1 = ILAENV( 1, 'ZGEQRF', ' ', N, M, -1, -1 )
      NB2 = ILAENV( 1, 'ZGERQF', ' ', N, P, -1, -1 )
      NB3 = ILAENV( 1, 'ZUNMQR', ' ', N, M, P, -1 )
      NB = MAX( NB1, NB2, NB3 )
      LWKOPT = MAX( N, M, P )*NB
      WORK( 1 ) = LWKOPT
      LQUERY = ( LWORK.EQ.-1 )
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -2
      ELSE IF( P.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( LWORK.LT.MAX( 1, N, M, P ) .AND. .NOT.LQUERY ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGGQRF', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     QR factorization of N-by-M matrix A: A = Q*R
*
      CALL ZGEQRF( N, M, A, LDA, TAUA, WORK, LWORK, INFO )
      LOPT = WORK( 1 )
*
*     Update B := Q**H*B.
*
      CALL ZUNMQR( 'Left', 'Conjugate Transpose', N, P, MIN( N, M ), A,
     $             LDA, TAUA, B, LDB, WORK, LWORK, INFO )
      LOPT = MAX( LOPT, INT( WORK( 1 ) ) )
*
*     RQ factorization of N-by-P matrix B: B = T*Z.
*
      CALL ZGERQF( N, P, B, LDB, TAUB, WORK, LWORK, INFO )
      WORK( 1 ) = MAX( LOPT, INT( WORK( 1 ) ) )
*
      RETURN
*
*     End of ZGGQRF
*
      END
