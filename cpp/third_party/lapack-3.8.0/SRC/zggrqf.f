*> \brief \b ZGGRQF
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGGRQF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zggrqf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zggrqf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zggrqf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGGRQF( M, P, N, A, LDA, TAUA, B, LDB, TAUB, WORK,
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
*> ZGGRQF computes a generalized RQ factorization of an M-by-N matrix A
*> and a P-by-N matrix B:
*>
*>             A = R*Q,        B = Z*T*Q,
*>
*> where Q is an N-by-N unitary matrix, Z is a P-by-P unitary
*> matrix, and R and T assume one of the forms:
*>
*> if M <= N,  R = ( 0  R12 ) M,   or if M > N,  R = ( R11 ) M-N,
*>                  N-M  M                           ( R21 ) N
*>                                                      N
*>
*> where R12 or R21 is upper triangular, and
*>
*> if P >= N,  T = ( T11 ) N  ,   or if P < N,  T = ( T11  T12 ) P,
*>                 (  0  ) P-N                         P   N-P
*>                    N
*>
*> where T11 is upper triangular.
*>
*> In particular, if B is square and nonsingular, the GRQ factorization
*> of A and B implicitly gives the RQ factorization of A*inv(B):
*>
*>              A*inv(B) = (R*inv(T))*Z**H
*>
*> where inv(B) denotes the inverse of the matrix B, and Z**H denotes the
*> conjugate transpose of the matrix Z.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
*>          The number of columns of the matrices A and B. N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, if M <= N, the upper triangle of the subarray
*>          A(1:M,N-M+1:N) contains the M-by-M upper triangular matrix R;
*>          if M > N, the elements on and above the (M-N)-th subdiagonal
*>          contain the M-by-N upper trapezoidal matrix R; the remaining
*>          elements, with the array TAUA, represent the unitary
*>          matrix Q as a product of elementary reflectors (see Further
*>          Details).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] TAUA
*> \verbatim
*>          TAUA is COMPLEX*16 array, dimension (min(M,N))
*>          The scalar factors of the elementary reflectors which
*>          represent the unitary matrix Q (see Further Details).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,N)
*>          On entry, the P-by-N matrix B.
*>          On exit, the elements on and above the diagonal of the array
*>          contain the min(P,N)-by-N upper trapezoidal matrix T (T is
*>          upper triangular if P >= N); the elements below the diagonal,
*>          with the array TAUB, represent the unitary matrix Z as a
*>          product of elementary reflectors (see Further Details).
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,P).
*> \endverbatim
*>
*> \param[out] TAUB
*> \verbatim
*>          TAUB is COMPLEX*16 array, dimension (min(P,N))
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
*>          where NB1 is the optimal blocksize for the RQ factorization
*>          of an M-by-N matrix, NB2 is the optimal blocksize for the
*>          QR factorization of a P-by-N matrix, and NB3 is the optimal
*>          blocksize for a call of ZUNMRQ.
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
*>          = 0:  successful exit
*>          < 0:  if INFO=-i, the i-th argument had an illegal value.
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
*>     Q = H(1) H(2) . . . H(k), where k = min(m,n).
*>
*>  Each H(i) has the form
*>
*>     H(i) = I - taua * v * v**H
*>
*>  where taua is a complex scalar, and v is a complex vector with
*>  v(n-k+i+1:n) = 0 and v(n-k+i) = 1; v(1:n-k+i-1) is stored on exit in
*>  A(m-k+i,1:n-k+i-1), and taua in TAUA(i).
*>  To form Q explicitly, use LAPACK subroutine ZUNGRQ.
*>  To use Q to update another matrix, use LAPACK subroutine ZUNMRQ.
*>
*>  The matrix Z is represented as a product of elementary reflectors
*>
*>     Z = H(1) H(2) . . . H(k), where k = min(p,n).
*>
*>  Each H(i) has the form
*>
*>     H(i) = I - taub * v * v**H
*>
*>  where taub is a complex scalar, and v is a complex vector with
*>  v(1:i-1) = 0 and v(i) = 1; v(i+1:p) is stored on exit in B(i+1:p,i),
*>  and taub in TAUB(i).
*>  To form Z explicitly, use LAPACK subroutine ZUNGQR.
*>  To use Z to update another matrix, use LAPACK subroutine ZUNMQR.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZGGRQF( M, P, N, A, LDA, TAUA, B, LDB, TAUB, WORK,
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
      EXTERNAL           XERBLA, ZGEQRF, ZGERQF, ZUNMRQ
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
      NB1 = ILAENV( 1, 'ZGERQF', ' ', M, N, -1, -1 )
      NB2 = ILAENV( 1, 'ZGEQRF', ' ', P, N, -1, -1 )
      NB3 = ILAENV( 1, 'ZUNMRQ', ' ', M, N, P, -1 )
      NB = MAX( NB1, NB2, NB3 )
      LWKOPT = MAX( N, M, P )*NB
      WORK( 1 ) = LWKOPT
      LQUERY = ( LWORK.EQ.-1 )
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( P.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, P ) ) THEN
         INFO = -8
      ELSE IF( LWORK.LT.MAX( 1, M, P, N ) .AND. .NOT.LQUERY ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGGRQF', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     RQ factorization of M-by-N matrix A: A = R*Q
*
      CALL ZGERQF( M, N, A, LDA, TAUA, WORK, LWORK, INFO )
      LOPT = WORK( 1 )
*
*     Update B := B*Q**H
*
      CALL ZUNMRQ( 'Right', 'Conjugate Transpose', P, N, MIN( M, N ),
     $             A( MAX( 1, M-N+1 ), 1 ), LDA, TAUA, B, LDB, WORK,
     $             LWORK, INFO )
      LOPT = MAX( LOPT, INT( WORK( 1 ) ) )
*
*     QR factorization of P-by-N matrix B: B = Z*T
*
      CALL ZGEQRF( P, N, B, LDB, TAUB, WORK, LWORK, INFO )
      WORK( 1 ) = MAX( LOPT, INT( WORK( 1 ) ) )
*
      RETURN
*
*     End of ZGGRQF
*
      END
