*> \brief \b CLANHF returns the value of the 1-norm, or the Frobenius norm, or the infinity norm, or the element of largest absolute value of a Hermitian matrix in RFP format.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLANHF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clanhf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clanhf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clanhf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       REAL FUNCTION CLANHF( NORM, TRANSR, UPLO, N, A, WORK )
*
*       .. Scalar Arguments ..
*       CHARACTER          NORM, TRANSR, UPLO
*       INTEGER            N
*       ..
*       .. Array Arguments ..
*       REAL               WORK( 0: * )
*       COMPLEX            A( 0: * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLANHF  returns the value of the one norm,  or the Frobenius norm, or
*> the  infinity norm,  or the  element of  largest absolute value  of a
*> complex Hermitian matrix A in RFP format.
*> \endverbatim
*>
*> \return CLANHF
*> \verbatim
*>
*>    CLANHF = ( max(abs(A(i,j))), NORM = 'M' or 'm'
*>             (
*>             ( norm1(A),         NORM = '1', 'O' or 'o'
*>             (
*>             ( normI(A),         NORM = 'I' or 'i'
*>             (
*>             ( normF(A),         NORM = 'F', 'f', 'E' or 'e'
*>
*> where  norm1  denotes the  one norm of a matrix (maximum column sum),
*> normI  denotes the  infinity norm  of a matrix  (maximum row sum) and
*> normF  denotes the  Frobenius norm of a matrix (square root of sum of
*> squares).  Note that  max(abs(A(i,j)))  is not a  matrix norm.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NORM
*> \verbatim
*>          NORM is CHARACTER
*>            Specifies the value to be returned in CLANHF as described
*>            above.
*> \endverbatim
*>
*> \param[in] TRANSR
*> \verbatim
*>          TRANSR is CHARACTER
*>            Specifies whether the RFP format of A is normal or
*>            conjugate-transposed format.
*>            = 'N':  RFP format is Normal
*>            = 'C':  RFP format is Conjugate-transposed
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER
*>            On entry, UPLO specifies whether the RFP matrix A came from
*>            an upper or lower triangular matrix as follows:
*>
*>            UPLO = 'U' or 'u' RFP A came from an upper triangular
*>            matrix
*>
*>            UPLO = 'L' or 'l' RFP A came from a  lower triangular
*>            matrix
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>            The order of the matrix A.  N >= 0.  When N = 0, CLANHF is
*>            set to zero.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension ( N*(N+1)/2 );
*>            On entry, the matrix A in RFP Format.
*>            RFP Format is described by TRANSR, UPLO and N as follows:
*>            If TRANSR='N' then RFP A is (0:N,0:K-1) when N is even;
*>            K=N/2. RFP A is (0:N-1,0:K) when N is odd; K=N/2. If
*>            TRANSR = 'C' then RFP is the Conjugate-transpose of RFP A
*>            as defined when TRANSR = 'N'. The contents of RFP A are
*>            defined by UPLO as follows: If UPLO = 'U' the RFP A
*>            contains the ( N*(N+1)/2 ) elements of upper packed A
*>            either in normal or conjugate-transpose Format. If
*>            UPLO = 'L' the RFP A contains the ( N*(N+1) /2 ) elements
*>            of lower packed A either in normal or conjugate-transpose
*>            Format. The LDA of RFP A is (N+1)/2 when TRANSR = 'C'. When
*>            TRANSR is 'N' the LDA is N+1 when N is even and is N when
*>            is odd. See the Note below for more details.
*>            Unchanged on exit.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK),
*>            where LWORK >= N when NORM = 'I' or '1' or 'O'; otherwise,
*>            WORK is not referenced.
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
*> \verbatim
*>
*>  We first consider Standard Packed Format when N is even.
*>  We give an example where N = 6.
*>
*>      AP is Upper             AP is Lower
*>
*>   00 01 02 03 04 05       00
*>      11 12 13 14 15       10 11
*>         22 23 24 25       20 21 22
*>            33 34 35       30 31 32 33
*>               44 45       40 41 42 43 44
*>                  55       50 51 52 53 54 55
*>
*>
*>  Let TRANSR = 'N'. RFP holds AP as follows:
*>  For UPLO = 'U' the upper trapezoid A(0:5,0:2) consists of the last
*>  three columns of AP upper. The lower triangle A(4:6,0:2) consists of
*>  conjugate-transpose of the first three columns of AP upper.
*>  For UPLO = 'L' the lower trapezoid A(1:6,0:2) consists of the first
*>  three columns of AP lower. The upper triangle A(0:2,0:2) consists of
*>  conjugate-transpose of the last three columns of AP lower.
*>  To denote conjugate we place -- above the element. This covers the
*>  case N even and TRANSR = 'N'.
*>
*>         RFP A                   RFP A
*>
*>                                -- -- --
*>        03 04 05                33 43 53
*>                                   -- --
*>        13 14 15                00 44 54
*>                                      --
*>        23 24 25                10 11 55
*>
*>        33 34 35                20 21 22
*>        --
*>        00 44 45                30 31 32
*>        -- --
*>        01 11 55                40 41 42
*>        -- -- --
*>        02 12 22                50 51 52
*>
*>  Now let TRANSR = 'C'. RFP A in both UPLO cases is just the conjugate-
*>  transpose of RFP A above. One therefore gets:
*>
*>
*>           RFP A                   RFP A
*>
*>     -- -- -- --                -- -- -- -- -- --
*>     03 13 23 33 00 01 02    33 00 10 20 30 40 50
*>     -- -- -- -- --                -- -- -- -- --
*>     04 14 24 34 44 11 12    43 44 11 21 31 41 51
*>     -- -- -- -- -- --                -- -- -- --
*>     05 15 25 35 45 55 22    53 54 55 22 32 42 52
*>
*>
*>  We next  consider Standard Packed Format when N is odd.
*>  We give an example where N = 5.
*>
*>     AP is Upper                 AP is Lower
*>
*>   00 01 02 03 04              00
*>      11 12 13 14              10 11
*>         22 23 24              20 21 22
*>            33 34              30 31 32 33
*>               44              40 41 42 43 44
*>
*>
*>  Let TRANSR = 'N'. RFP holds AP as follows:
*>  For UPLO = 'U' the upper trapezoid A(0:4,0:2) consists of the last
*>  three columns of AP upper. The lower triangle A(3:4,0:1) consists of
*>  conjugate-transpose of the first two   columns of AP upper.
*>  For UPLO = 'L' the lower trapezoid A(0:4,0:2) consists of the first
*>  three columns of AP lower. The upper triangle A(0:1,1:2) consists of
*>  conjugate-transpose of the last two   columns of AP lower.
*>  To denote conjugate we place -- above the element. This covers the
*>  case N odd  and TRANSR = 'N'.
*>
*>         RFP A                   RFP A
*>
*>                                   -- --
*>        02 03 04                00 33 43
*>                                      --
*>        12 13 14                10 11 44
*>
*>        22 23 24                20 21 22
*>        --
*>        00 33 34                30 31 32
*>        -- --
*>        01 11 44                40 41 42
*>
*>  Now let TRANSR = 'C'. RFP A in both UPLO cases is just the conjugate-
*>  transpose of RFP A above. One therefore gets:
*>
*>
*>           RFP A                   RFP A
*>
*>     -- -- --                   -- -- -- -- -- --
*>     02 12 22 00 01             00 10 20 30 40 50
*>     -- -- -- --                   -- -- -- -- --
*>     03 13 23 33 11             33 11 21 31 41 51
*>     -- -- -- -- --                   -- -- -- --
*>     04 14 24 34 44             43 44 22 32 42 52
*> \endverbatim
*>
*  =====================================================================
      REAL FUNCTION CLANHF( NORM, TRANSR, UPLO, N, A, WORK )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          NORM, TRANSR, UPLO
      INTEGER            N
*     ..
*     .. Array Arguments ..
      REAL               WORK( 0: * )
      COMPLEX            A( 0: * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, IFM, ILU, NOE, N1, K, L, LDA
      REAL               SCALE, S, VALUE, AA, TEMP
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, SISNAN
      EXTERNAL           LSAME, SISNAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLASSQ
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
      IF( N.EQ.0 ) THEN
         CLANHF = ZERO
         RETURN
      ELSE IF( N.EQ.1 ) THEN
         CLANHF = ABS(REAL(A(0)))
         RETURN
      END IF
*
*     set noe = 1 if n is odd. if n is even set noe=0
*
      NOE = 1
      IF( MOD( N, 2 ).EQ.0 )
     $   NOE = 0
*
*     set ifm = 0 when form='C' or 'c' and 1 otherwise
*
      IFM = 1
      IF( LSAME( TRANSR, 'C' ) )
     $   IFM = 0
*
*     set ilu = 0 when uplo='U or 'u' and 1 otherwise
*
      ILU = 1
      IF( LSAME( UPLO, 'U' ) )
     $   ILU = 0
*
*     set lda = (n+1)/2 when ifm = 0
*     set lda = n when ifm = 1 and noe = 1
*     set lda = n+1 when ifm = 1 and noe = 0
*
      IF( IFM.EQ.1 ) THEN
         IF( NOE.EQ.1 ) THEN
            LDA = N
         ELSE
*           noe=0
            LDA = N + 1
         END IF
      ELSE
*        ifm=0
         LDA = ( N+1 ) / 2
      END IF
*
      IF( LSAME( NORM, 'M' ) ) THEN
*
*       Find max(abs(A(i,j))).
*
         K = ( N+1 ) / 2
         VALUE = ZERO
         IF( NOE.EQ.1 ) THEN
*           n is odd & n = k + k - 1
            IF( IFM.EQ.1 ) THEN
*              A is n by k
               IF( ILU.EQ.1 ) THEN
*                 uplo ='L'
                  J = 0
*                 -> L(0,0)
                  TEMP = ABS( REAL( A( J+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
                  DO I = 1, N - 1
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  DO J = 1, K - 1
                     DO I = 0, J - 2
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = J - 1
*                    L(k+j,k+j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = J
*                    -> L(j,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = J + 1, N - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
               ELSE
*                 uplo = 'U'
                  DO J = 0, K - 2
                     DO I = 0, K + J - 2
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = K + J - 1
*                    -> U(i,i)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = I + 1
*                    =k+j; i -> U(j,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = K + J + 1, N - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  DO I = 0, N - 2
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
*                    j=k-1
                  END DO
*                 i=n-1 -> U(n-1,n-1)
                  TEMP = ABS( REAL( A( I+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
               END IF
            ELSE
*              xpose case; A is k by n
               IF( ILU.EQ.1 ) THEN
*                 uplo ='L'
                  DO J = 0, K - 2
                     DO I = 0, J - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = J
*                    L(i,i)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = J + 1
*                    L(j+k,j+k)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = J + 2, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  J = K - 1
                  DO I = 0, K - 2
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  I = K - 1
*                 -> L(i,i) is at A(i,j)
                  TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  DO J = K, N - 1
                     DO I = 0, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
               ELSE
*                 uplo = 'U'
                  DO J = 0, K - 2
                     DO I = 0, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  J = K - 1
*                 -> U(j,j) is at A(0,j)
                  TEMP = ABS( REAL( A( 0+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  DO I = 1, K - 1
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  DO J = K, N - 1
                     DO I = 0, J - K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = J - K
*                    -> U(i,i) at A(i,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = J - K + 1
*                    U(j,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = J - K + 2, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
               END IF
            END IF
         ELSE
*           n is even & k = n/2
            IF( IFM.EQ.1 ) THEN
*              A is n+1 by k
               IF( ILU.EQ.1 ) THEN
*                 uplo ='L'
                  J = 0
*                 -> L(k,k) & j=1 -> L(0,0)
                  TEMP = ABS( REAL( A( J+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
                  TEMP = ABS( REAL( A( J+1+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
                  DO I = 2, N
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  DO J = 1, K - 1
                     DO I = 0, J - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = J
*                    L(k+j,k+j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = J + 1
*                    -> L(j,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = J + 2, N
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
               ELSE
*                 uplo = 'U'
                  DO J = 0, K - 2
                     DO I = 0, K + J - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = K + J
*                    -> U(i,i)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = I + 1
*                    =k+j+1; i -> U(j,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = K + J + 2, N
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  DO I = 0, N - 2
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
*                 j=k-1
                  END DO
*                 i=n-1 -> U(n-1,n-1)
                  TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  I = N
*                 -> U(k-1,k-1)
                  TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
               END IF
            ELSE
*              xpose case; A is k by n+1
               IF( ILU.EQ.1 ) THEN
*                 uplo ='L'
                  J = 0
*                 -> L(k,k) at A(0,0)
                  TEMP = ABS( REAL( A( J+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  DO I = 1, K - 1
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  DO J = 1, K - 1
                     DO I = 0, J - 2
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = J - 1
*                    L(i,i)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = J
*                    L(j+k,j+k)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = J + 1, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  J = K
                  DO I = 0, K - 2
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  I = K - 1
*                 -> L(i,i) is at A(i,j)
                  TEMP = ABS( REAL( A( I+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
                  DO J = K + 1, N
                     DO I = 0, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
               ELSE
*                 uplo = 'U'
                  DO J = 0, K - 1
                     DO I = 0, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  J = K
*                 -> U(j,j) is at A(0,j)
                  TEMP = ABS( REAL( A( 0+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
                  DO I = 1, K - 1
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  DO J = K + 1, N - 1
                     DO I = 0, J - K - 2
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                     I = J - K - 1
*                    -> U(i,i) at A(i,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     I = J - K
*                    U(j,j)
                     TEMP = ABS( REAL( A( I+J*LDA ) ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                     DO I = J - K + 1, K - 1
                        TEMP = ABS( A( I+J*LDA ) )
                        IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                       VALUE = TEMP
                     END DO
                  END DO
                  J = N
                  DO I = 0, K - 2
                     TEMP = ABS( A( I+J*LDA ) )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
                  I = K - 1
*                 U(k,k) at A(i,j)
                  TEMP = ABS( REAL( A( I+J*LDA ) ) )
                  IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                 VALUE = TEMP
               END IF
            END IF
         END IF
      ELSE IF( ( LSAME( NORM, 'I' ) ) .OR. ( LSAME( NORM, 'O' ) ) .OR.
     $         ( NORM.EQ.'1' ) ) THEN
*
*       Find normI(A) ( = norm1(A), since A is Hermitian).
*
         IF( IFM.EQ.1 ) THEN
*           A is 'N'
            K = N / 2
            IF( NOE.EQ.1 ) THEN
*              n is odd & A is n by (n+1)/2
               IF( ILU.EQ.0 ) THEN
*                 uplo = 'U'
                  DO I = 0, K - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = 0, K
                     S = ZERO
                     DO I = 0, K + J - 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(i,j+k)
                        S = S + AA
                        WORK( I ) = WORK( I ) + AA
                     END DO
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j+k,j+k)
                     WORK( J+K ) = S + AA
                     IF( I.EQ.K+K )
     $                  GO TO 10
                     I = I + 1
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j,j)
                     WORK( J ) = WORK( J ) + AA
                     S = ZERO
                     DO L = J + 1, K - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(l,j)
                        S = S + AA
                        WORK( L ) = WORK( L ) + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
   10             CONTINUE
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               ELSE
*                 ilu = 1 & uplo = 'L'
                  K = K + 1
*                 k=(n+1)/2 for n odd and ilu=1
                  DO I = K, N - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = K - 1, 0, -1
                     S = ZERO
                     DO I = 0, J - 2
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(j+k,i+k)
                        S = S + AA
                        WORK( I+K ) = WORK( I+K ) + AA
                     END DO
                     IF( J.GT.0 ) THEN
                        AA = ABS( REAL( A( I+J*LDA ) ) )
*                       -> A(j+k,j+k)
                        S = S + AA
                        WORK( I+K ) = WORK( I+K ) + S
*                       i=j
                        I = I + 1
                     END IF
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j,j)
                     WORK( J ) = AA
                     S = ZERO
                     DO L = J + 1, N - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(l,j)
                        S = S + AA
                        WORK( L ) = WORK( L ) + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               END IF
            ELSE
*              n is even & A is n+1 by k = n/2
               IF( ILU.EQ.0 ) THEN
*                 uplo = 'U'
                  DO I = 0, K - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = 0, K - 1
                     S = ZERO
                     DO I = 0, K + J - 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(i,j+k)
                        S = S + AA
                        WORK( I ) = WORK( I ) + AA
                     END DO
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j+k,j+k)
                     WORK( J+K ) = S + AA
                     I = I + 1
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j,j)
                     WORK( J ) = WORK( J ) + AA
                     S = ZERO
                     DO L = J + 1, K - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(l,j)
                        S = S + AA
                        WORK( L ) = WORK( L ) + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               ELSE
*                 ilu = 1 & uplo = 'L'
                  DO I = K, N - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = K - 1, 0, -1
                     S = ZERO
                     DO I = 0, J - 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(j+k,i+k)
                        S = S + AA
                        WORK( I+K ) = WORK( I+K ) + AA
                     END DO
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j+k,j+k)
                     S = S + AA
                     WORK( I+K ) = WORK( I+K ) + S
*                    i=j
                     I = I + 1
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    -> A(j,j)
                     WORK( J ) = AA
                     S = ZERO
                     DO L = J + 1, N - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       -> A(l,j)
                        S = S + AA
                        WORK( L ) = WORK( L ) + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               END IF
            END IF
         ELSE
*           ifm=0
            K = N / 2
            IF( NOE.EQ.1 ) THEN
*              n is odd & A is (n+1)/2 by n
               IF( ILU.EQ.0 ) THEN
*                 uplo = 'U'
                  N1 = K
*                 n/2
                  K = K + 1
*                 k is the row size and lda
                  DO I = N1, N - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = 0, N1 - 1
                     S = ZERO
                     DO I = 0, K - 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j,n1+i)
                        WORK( I+N1 ) = WORK( I+N1 ) + AA
                        S = S + AA
                     END DO
                     WORK( J ) = S
                  END DO
*                 j=n1=k-1 is special
                  S = ABS( REAL( A( 0+J*LDA ) ) )
*                 A(k-1,k-1)
                  DO I = 1, K - 1
                     AA = ABS( A( I+J*LDA ) )
*                    A(k-1,i+n1)
                     WORK( I+N1 ) = WORK( I+N1 ) + AA
                     S = S + AA
                  END DO
                  WORK( J ) = WORK( J ) + S
                  DO J = K, N - 1
                     S = ZERO
                     DO I = 0, J - K - 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(i,j-k)
                        WORK( I ) = WORK( I ) + AA
                        S = S + AA
                     END DO
*                    i=j-k
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    A(j-k,j-k)
                     S = S + AA
                     WORK( J-K ) = WORK( J-K ) + S
                     I = I + 1
                     S = ABS( REAL( A( I+J*LDA ) ) )
*                    A(j,j)
                     DO L = J + 1, N - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j,l)
                        WORK( L ) = WORK( L ) + AA
                        S = S + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               ELSE
*                 ilu=1 & uplo = 'L'
                  K = K + 1
*                 k=(n+1)/2 for n odd and ilu=1
                  DO I = K, N - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = 0, K - 2
*                    process
                     S = ZERO
                     DO I = 0, J - 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j,i)
                        WORK( I ) = WORK( I ) + AA
                        S = S + AA
                     END DO
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    i=j so process of A(j,j)
                     S = S + AA
                     WORK( J ) = S
*                    is initialised here
                     I = I + 1
*                    i=j process A(j+k,j+k)
                     AA = ABS( REAL( A( I+J*LDA ) ) )
                     S = AA
                     DO L = K + J + 1, N - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(l,k+j)
                        S = S + AA
                        WORK( L ) = WORK( L ) + AA
                     END DO
                     WORK( K+J ) = WORK( K+J ) + S
                  END DO
*                 j=k-1 is special :process col A(k-1,0:k-1)
                  S = ZERO
                  DO I = 0, K - 2
                     AA = ABS( A( I+J*LDA ) )
*                    A(k,i)
                     WORK( I ) = WORK( I ) + AA
                     S = S + AA
                  END DO
*                 i=k-1
                  AA = ABS( REAL( A( I+J*LDA ) ) )
*                 A(k-1,k-1)
                  S = S + AA
                  WORK( I ) = S
*                 done with col j=k+1
                  DO J = K, N - 1
*                    process col j of A = A(j,0:k-1)
                     S = ZERO
                     DO I = 0, K - 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j,i)
                        WORK( I ) = WORK( I ) + AA
                        S = S + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               END IF
            ELSE
*              n is even & A is k=n/2 by n+1
               IF( ILU.EQ.0 ) THEN
*                 uplo = 'U'
                  DO I = K, N - 1
                     WORK( I ) = ZERO
                  END DO
                  DO J = 0, K - 1
                     S = ZERO
                     DO I = 0, K - 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j,i+k)
                        WORK( I+K ) = WORK( I+K ) + AA
                        S = S + AA
                     END DO
                     WORK( J ) = S
                  END DO
*                 j=k
                  AA = ABS( REAL( A( 0+J*LDA ) ) )
*                 A(k,k)
                  S = AA
                  DO I = 1, K - 1
                     AA = ABS( A( I+J*LDA ) )
*                    A(k,k+i)
                     WORK( I+K ) = WORK( I+K ) + AA
                     S = S + AA
                  END DO
                  WORK( J ) = WORK( J ) + S
                  DO J = K + 1, N - 1
                     S = ZERO
                     DO I = 0, J - 2 - K
                        AA = ABS( A( I+J*LDA ) )
*                       A(i,j-k-1)
                        WORK( I ) = WORK( I ) + AA
                        S = S + AA
                     END DO
*                    i=j-1-k
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    A(j-k-1,j-k-1)
                     S = S + AA
                     WORK( J-K-1 ) = WORK( J-K-1 ) + S
                     I = I + 1
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    A(j,j)
                     S = AA
                     DO L = J + 1, N - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j,l)
                        WORK( L ) = WORK( L ) + AA
                        S = S + AA
                     END DO
                     WORK( J ) = WORK( J ) + S
                  END DO
*                 j=n
                  S = ZERO
                  DO I = 0, K - 2
                     AA = ABS( A( I+J*LDA ) )
*                    A(i,k-1)
                     WORK( I ) = WORK( I ) + AA
                     S = S + AA
                  END DO
*                 i=k-1
                  AA = ABS( REAL( A( I+J*LDA ) ) )
*                 A(k-1,k-1)
                  S = S + AA
                  WORK( I ) = WORK( I ) + S
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               ELSE
*                 ilu=1 & uplo = 'L'
                  DO I = K, N - 1
                     WORK( I ) = ZERO
                  END DO
*                 j=0 is special :process col A(k:n-1,k)
                  S = ABS( REAL( A( 0 ) ) )
*                 A(k,k)
                  DO I = 1, K - 1
                     AA = ABS( A( I ) )
*                    A(k+i,k)
                     WORK( I+K ) = WORK( I+K ) + AA
                     S = S + AA
                  END DO
                  WORK( K ) = WORK( K ) + S
                  DO J = 1, K - 1
*                    process
                     S = ZERO
                     DO I = 0, J - 2
                        AA = ABS( A( I+J*LDA ) )
*                       A(j-1,i)
                        WORK( I ) = WORK( I ) + AA
                        S = S + AA
                     END DO
                     AA = ABS( REAL( A( I+J*LDA ) ) )
*                    i=j-1 so process of A(j-1,j-1)
                     S = S + AA
                     WORK( J-1 ) = S
*                    is initialised here
                     I = I + 1
*                    i=j process A(j+k,j+k)
                     AA = ABS( REAL( A( I+J*LDA ) ) )
                     S = AA
                     DO L = K + J + 1, N - 1
                        I = I + 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(l,k+j)
                        S = S + AA
                        WORK( L ) = WORK( L ) + AA
                     END DO
                     WORK( K+J ) = WORK( K+J ) + S
                  END DO
*                 j=k is special :process col A(k,0:k-1)
                  S = ZERO
                  DO I = 0, K - 2
                     AA = ABS( A( I+J*LDA ) )
*                    A(k,i)
                     WORK( I ) = WORK( I ) + AA
                     S = S + AA
                  END DO
*
*                 i=k-1
                  AA = ABS( REAL( A( I+J*LDA ) ) )
*                 A(k-1,k-1)
                  S = S + AA
                  WORK( I ) = S
*                 done with col j=k+1
                  DO J = K + 1, N
*
*                    process col j-1 of A = A(j-1,0:k-1)
                     S = ZERO
                     DO I = 0, K - 1
                        AA = ABS( A( I+J*LDA ) )
*                       A(j-1,i)
                        WORK( I ) = WORK( I ) + AA
                        S = S + AA
                     END DO
                     WORK( J-1 ) = WORK( J-1 ) + S
                  END DO
                  VALUE = WORK( 0 )
                  DO I = 1, N-1
                     TEMP = WORK( I )
                     IF( VALUE .LT. TEMP .OR. SISNAN( TEMP ) )
     $                    VALUE = TEMP
                  END DO
               END IF
            END IF
         END IF
      ELSE IF( ( LSAME( NORM, 'F' ) ) .OR. ( LSAME( NORM, 'E' ) ) ) THEN
*
*       Find normF(A).
*
         K = ( N+1 ) / 2
         SCALE = ZERO
         S = ONE
         IF( NOE.EQ.1 ) THEN
*           n is odd
            IF( IFM.EQ.1 ) THEN
*              A is normal & A is n by k
               IF( ILU.EQ.0 ) THEN
*                 A is upper
                  DO J = 0, K - 3
                     CALL CLASSQ( K-J-2, A( K+J+1+J*LDA ), 1, SCALE, S )
*                    L at A(k,0)
                  END DO
                  DO J = 0, K - 1
                     CALL CLASSQ( K+J-1, A( 0+J*LDA ), 1, SCALE, S )
*                    trap U at A(0,0)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = K - 1
*                 -> U(k,k) at A(k-1,0)
                  DO I = 0, K - 2
                     AA = REAL( A( L ) )
*                    U(k+i,k+i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    U(i,i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
                  AA = REAL( A( L ) )
*                 U(n-1,n-1)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
               ELSE
*                 ilu=1 & A is lower
                  DO J = 0, K - 1
                     CALL CLASSQ( N-J-1, A( J+1+J*LDA ), 1, SCALE, S )
*                    trap L at A(0,0)
                  END DO
                  DO J = 1, K - 2
                     CALL CLASSQ( J, A( 0+( 1+J )*LDA ), 1, SCALE, S )
*                    U at A(0,1)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  AA = REAL( A( 0 ) )
*                 L(0,0) at A(0,0)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
                  L = LDA
*                 -> L(k,k) at A(0,1)
                  DO I = 1, K - 1
                     AA = REAL( A( L ) )
*                    L(k-1+i,k-1+i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    L(i,i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
               END IF
            ELSE
*              A is xpose & A is k by n
               IF( ILU.EQ.0 ) THEN
*                 A**H is upper
                  DO J = 1, K - 2
                     CALL CLASSQ( J, A( 0+( K+J )*LDA ), 1, SCALE, S )
*                    U at A(0,k)
                  END DO
                  DO J = 0, K - 2
                     CALL CLASSQ( K, A( 0+J*LDA ), 1, SCALE, S )
*                    k by k-1 rect. at A(0,0)
                  END DO
                  DO J = 0, K - 2
                     CALL CLASSQ( K-J-1, A( J+1+( J+K-1 )*LDA ), 1,
     $                            SCALE, S )
*                    L at A(0,k-1)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = 0 + K*LDA - LDA
*                 -> U(k-1,k-1) at A(0,k-1)
                  AA = REAL( A( L ) )
*                 U(k-1,k-1)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
                  L = L + LDA
*                 -> U(0,0) at A(0,k)
                  DO J = K, N - 1
                     AA = REAL( A( L ) )
*                    -> U(j-k,j-k)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    -> U(j,j)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
               ELSE
*                 A**H is lower
                  DO J = 1, K - 1
                     CALL CLASSQ( J, A( 0+J*LDA ), 1, SCALE, S )
*                    U at A(0,0)
                  END DO
                  DO J = K, N - 1
                     CALL CLASSQ( K, A( 0+J*LDA ), 1, SCALE, S )
*                    k by k-1 rect. at A(0,k)
                  END DO
                  DO J = 0, K - 3
                     CALL CLASSQ( K-J-2, A( J+2+J*LDA ), 1, SCALE, S )
*                    L at A(1,0)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = 0
*                 -> L(0,0) at A(0,0)
                  DO I = 0, K - 2
                     AA = REAL( A( L ) )
*                    L(i,i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    L(k+i,k+i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
*                 L-> k-1 + (k-1)*lda or L(k-1,k-1) at A(k-1,k-1)
                  AA = REAL( A( L ) )
*                 L(k-1,k-1) at A(k-1,k-1)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
               END IF
            END IF
         ELSE
*           n is even
            IF( IFM.EQ.1 ) THEN
*              A is normal
               IF( ILU.EQ.0 ) THEN
*                 A is upper
                  DO J = 0, K - 2
                     CALL CLASSQ( K-J-1, A( K+J+2+J*LDA ), 1, SCALE, S )
*                 L at A(k+1,0)
                  END DO
                  DO J = 0, K - 1
                     CALL CLASSQ( K+J, A( 0+J*LDA ), 1, SCALE, S )
*                 trap U at A(0,0)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = K
*                 -> U(k,k) at A(k,0)
                  DO I = 0, K - 1
                     AA = REAL( A( L ) )
*                    U(k+i,k+i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    U(i,i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
               ELSE
*                 ilu=1 & A is lower
                  DO J = 0, K - 1
                     CALL CLASSQ( N-J-1, A( J+2+J*LDA ), 1, SCALE, S )
*                    trap L at A(1,0)
                  END DO
                  DO J = 1, K - 1
                     CALL CLASSQ( J, A( 0+J*LDA ), 1, SCALE, S )
*                    U at A(0,0)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = 0
*                 -> L(k,k) at A(0,0)
                  DO I = 0, K - 1
                     AA = REAL( A( L ) )
*                    L(k-1+i,k-1+i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    L(i,i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
               END IF
            ELSE
*              A is xpose
               IF( ILU.EQ.0 ) THEN
*                 A**H is upper
                  DO J = 1, K - 1
                     CALL CLASSQ( J, A( 0+( K+1+J )*LDA ), 1, SCALE, S )
*                 U at A(0,k+1)
                  END DO
                  DO J = 0, K - 1
                     CALL CLASSQ( K, A( 0+J*LDA ), 1, SCALE, S )
*                 k by k rect. at A(0,0)
                  END DO
                  DO J = 0, K - 2
                     CALL CLASSQ( K-J-1, A( J+1+( J+K )*LDA ), 1, SCALE,
     $                            S )
*                 L at A(0,k)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = 0 + K*LDA
*                 -> U(k,k) at A(0,k)
                  AA = REAL( A( L ) )
*                 U(k,k)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
                  L = L + LDA
*                 -> U(0,0) at A(0,k+1)
                  DO J = K + 1, N - 1
                     AA = REAL( A( L ) )
*                    -> U(j-k-1,j-k-1)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    -> U(j,j)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
*                 L=k-1+n*lda
*                 -> U(k-1,k-1) at A(k-1,n)
                  AA = REAL( A( L ) )
*                 U(k,k)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
               ELSE
*                 A**H is lower
                  DO J = 1, K - 1
                     CALL CLASSQ( J, A( 0+( J+1 )*LDA ), 1, SCALE, S )
*                 U at A(0,1)
                  END DO
                  DO J = K + 1, N
                     CALL CLASSQ( K, A( 0+J*LDA ), 1, SCALE, S )
*                 k by k rect. at A(0,k+1)
                  END DO
                  DO J = 0, K - 2
                     CALL CLASSQ( K-J-1, A( J+1+J*LDA ), 1, SCALE, S )
*                 L at A(0,0)
                  END DO
                  S = S + S
*                 double s for the off diagonal elements
                  L = 0
*                 -> L(k,k) at A(0,0)
                  AA = REAL( A( L ) )
*                 L(k,k) at A(0,0)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
                  L = LDA
*                 -> L(0,0) at A(0,1)
                  DO I = 0, K - 2
                     AA = REAL( A( L ) )
*                    L(i,i)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     AA = REAL( A( L+1 ) )
*                    L(k+i+1,k+i+1)
                     IF( AA.NE.ZERO ) THEN
                        IF( SCALE.LT.AA ) THEN
                           S = ONE + S*( SCALE / AA )**2
                           SCALE = AA
                        ELSE
                           S = S + ( AA / SCALE )**2
                        END IF
                     END IF
                     L = L + LDA + 1
                  END DO
*                 L-> k - 1 + k*lda or L(k-1,k-1) at A(k-1,k)
                  AA = REAL( A( L ) )
*                 L(k-1,k-1) at A(k-1,k)
                  IF( AA.NE.ZERO ) THEN
                     IF( SCALE.LT.AA ) THEN
                        S = ONE + S*( SCALE / AA )**2
                        SCALE = AA
                     ELSE
                        S = S + ( AA / SCALE )**2
                     END IF
                  END IF
               END IF
            END IF
         END IF
         VALUE = SCALE*SQRT( S )
      END IF
*
      CLANHF = VALUE
      RETURN
*
*     End of CLANHF
*
      END
