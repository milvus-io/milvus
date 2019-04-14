*> \brief \b SSFRK performs a symmetric rank-k operation for matrix in RFP format.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SSFRK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ssfrk.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ssfrk.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ssfrk.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SSFRK( TRANSR, UPLO, TRANS, N, K, ALPHA, A, LDA, BETA,
*                         C )
*
*       .. Scalar Arguments ..
*       REAL               ALPHA, BETA
*       INTEGER            K, LDA, N
*       CHARACTER          TRANS, TRANSR, UPLO
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), C( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Level 3 BLAS like routine for C in RFP Format.
*>
*> SSFRK performs one of the symmetric rank--k operations
*>
*>    C := alpha*A*A**T + beta*C,
*>
*> or
*>
*>    C := alpha*A**T*A + beta*C,
*>
*> where alpha and beta are real scalars, C is an n--by--n symmetric
*> matrix and A is an n--by--k matrix in the first case and a k--by--n
*> matrix in the second case.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANSR
*> \verbatim
*>          TRANSR is CHARACTER*1
*>          = 'N':  The Normal Form of RFP A is stored;
*>          = 'T':  The Transpose Form of RFP A is stored.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>           On  entry, UPLO specifies whether the upper or lower
*>           triangular part of the array C is to be referenced as
*>           follows:
*>
*>              UPLO = 'U' or 'u'   Only the upper triangular part of C
*>                                  is to be referenced.
*>
*>              UPLO = 'L' or 'l'   Only the lower triangular part of C
*>                                  is to be referenced.
*>
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>           On entry, TRANS specifies the operation to be performed as
*>           follows:
*>
*>              TRANS = 'N' or 'n'   C := alpha*A*A**T + beta*C.
*>
*>              TRANS = 'T' or 't'   C := alpha*A**T*A + beta*C.
*>
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           On entry, N specifies the order of the matrix C. N must be
*>           at least zero.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>           On entry with TRANS = 'N' or 'n', K specifies the number
*>           of  columns of the matrix A, and on entry with TRANS = 'T'
*>           or 't', K specifies the number of rows of the matrix A. K
*>           must be at least zero.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] ALPHA
*> \verbatim
*>          ALPHA is REAL
*>           On entry, ALPHA specifies the scalar alpha.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,ka)
*>           where KA
*>           is K  when TRANS = 'N' or 'n', and is N otherwise. Before
*>           entry with TRANS = 'N' or 'n', the leading N--by--K part of
*>           the array A must contain the matrix A, otherwise the leading
*>           K--by--N part of the array A must contain the matrix A.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>           On entry, LDA specifies the first dimension of A as declared
*>           in  the  calling  (sub)  program.   When  TRANS = 'N' or 'n'
*>           then  LDA must be at least  max( 1, n ), otherwise  LDA must
*>           be at least  max( 1, k ).
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] BETA
*> \verbatim
*>          BETA is REAL
*>           On entry, BETA specifies the scalar beta.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is REAL array, dimension (NT)
*>           NT = N*(N+1)/2. On entry, the symmetric matrix C in RFP
*>           Format. RFP Format is described by TRANSR, UPLO and N.
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
*> \ingroup realOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SSFRK( TRANSR, UPLO, TRANS, N, K, ALPHA, A, LDA, BETA,
     $                  C )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      REAL               ALPHA, BETA
      INTEGER            K, LDA, N
      CHARACTER          TRANS, TRANSR, UPLO
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), C( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LOWER, NORMALTRANSR, NISODD, NOTRANS
      INTEGER            INFO, NROWA, J, NK, N1, N2
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SSYRK, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      NORMALTRANSR = LSAME( TRANSR, 'N' )
      LOWER = LSAME( UPLO, 'L' )
      NOTRANS = LSAME( TRANS, 'N' )
*
      IF( NOTRANS ) THEN
         NROWA = N
      ELSE
         NROWA = K
      END IF
*
      IF( .NOT.NORMALTRANSR .AND. .NOT.LSAME( TRANSR, 'T' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LOWER .AND. .NOT.LSAME( UPLO, 'U' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.NOTRANS .AND. .NOT.LSAME( TRANS, 'T' ) ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( K.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, NROWA ) ) THEN
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SSFRK ', -INFO )
         RETURN
      END IF
*
*     Quick return if possible.
*
*     The quick return case: ((ALPHA.EQ.0).AND.(BETA.NE.ZERO)) is not
*     done (it is in SSYRK for example) and left in the general case.
*
      IF( ( N.EQ.0 ) .OR. ( ( ( ALPHA.EQ.ZERO ) .OR. ( K.EQ.0 ) ) .AND.
     $    ( BETA.EQ.ONE ) ) )RETURN
*
      IF( ( ALPHA.EQ.ZERO ) .AND. ( BETA.EQ.ZERO ) ) THEN
         DO J = 1, ( ( N*( N+1 ) ) / 2 )
            C( J ) = ZERO
         END DO
         RETURN
      END IF
*
*     C is N-by-N.
*     If N is odd, set NISODD = .TRUE., and N1 and N2.
*     If N is even, NISODD = .FALSE., and NK.
*
      IF( MOD( N, 2 ).EQ.0 ) THEN
         NISODD = .FALSE.
         NK = N / 2
      ELSE
         NISODD = .TRUE.
         IF( LOWER ) THEN
            N2 = N / 2
            N1 = N - N2
         ELSE
            N1 = N / 2
            N2 = N - N1
         END IF
      END IF
*
      IF( NISODD ) THEN
*
*        N is odd
*
         IF( NORMALTRANSR ) THEN
*
*           N is odd and TRANSR = 'N'
*
            IF( LOWER ) THEN
*
*              N is odd, TRANSR = 'N', and UPLO = 'L'
*
               IF( NOTRANS ) THEN
*
*                 N is odd, TRANSR = 'N', UPLO = 'L', and TRANS = 'N'
*
                  CALL SSYRK( 'L', 'N', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( 1 ), N )
                  CALL SSYRK( 'U', 'N', N2, K, ALPHA, A( N1+1, 1 ), LDA,
     $                        BETA, C( N+1 ), N )
                  CALL SGEMM( 'N', 'T', N2, N1, K, ALPHA, A( N1+1, 1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( N1+1 ), N )
*
               ELSE
*
*                 N is odd, TRANSR = 'N', UPLO = 'L', and TRANS = 'T'
*
                  CALL SSYRK( 'L', 'T', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( 1 ), N )
                  CALL SSYRK( 'U', 'T', N2, K, ALPHA, A( 1, N1+1 ), LDA,
     $                        BETA, C( N+1 ), N )
                  CALL SGEMM( 'T', 'N', N2, N1, K, ALPHA, A( 1, N1+1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( N1+1 ), N )
*
               END IF
*
            ELSE
*
*              N is odd, TRANSR = 'N', and UPLO = 'U'
*
               IF( NOTRANS ) THEN
*
*                 N is odd, TRANSR = 'N', UPLO = 'U', and TRANS = 'N'
*
                  CALL SSYRK( 'L', 'N', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( N2+1 ), N )
                  CALL SSYRK( 'U', 'N', N2, K, ALPHA, A( N2, 1 ), LDA,
     $                        BETA, C( N1+1 ), N )
                  CALL SGEMM( 'N', 'T', N1, N2, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( N2, 1 ), LDA, BETA, C( 1 ), N )
*
               ELSE
*
*                 N is odd, TRANSR = 'N', UPLO = 'U', and TRANS = 'T'
*
                  CALL SSYRK( 'L', 'T', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( N2+1 ), N )
                  CALL SSYRK( 'U', 'T', N2, K, ALPHA, A( 1, N2 ), LDA,
     $                        BETA, C( N1+1 ), N )
                  CALL SGEMM( 'T', 'N', N1, N2, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( 1, N2 ), LDA, BETA, C( 1 ), N )
*
               END IF
*
            END IF
*
         ELSE
*
*           N is odd, and TRANSR = 'T'
*
            IF( LOWER ) THEN
*
*              N is odd, TRANSR = 'T', and UPLO = 'L'
*
               IF( NOTRANS ) THEN
*
*                 N is odd, TRANSR = 'T', UPLO = 'L', and TRANS = 'N'
*
                  CALL SSYRK( 'U', 'N', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( 1 ), N1 )
                  CALL SSYRK( 'L', 'N', N2, K, ALPHA, A( N1+1, 1 ), LDA,
     $                        BETA, C( 2 ), N1 )
                  CALL SGEMM( 'N', 'T', N1, N2, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( N1+1, 1 ), LDA, BETA,
     $                        C( N1*N1+1 ), N1 )
*
               ELSE
*
*                 N is odd, TRANSR = 'T', UPLO = 'L', and TRANS = 'T'
*
                  CALL SSYRK( 'U', 'T', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( 1 ), N1 )
                  CALL SSYRK( 'L', 'T', N2, K, ALPHA, A( 1, N1+1 ), LDA,
     $                        BETA, C( 2 ), N1 )
                  CALL SGEMM( 'T', 'N', N1, N2, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( 1, N1+1 ), LDA, BETA,
     $                        C( N1*N1+1 ), N1 )
*
               END IF
*
            ELSE
*
*              N is odd, TRANSR = 'T', and UPLO = 'U'
*
               IF( NOTRANS ) THEN
*
*                 N is odd, TRANSR = 'T', UPLO = 'U', and TRANS = 'N'
*
                  CALL SSYRK( 'U', 'N', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( N2*N2+1 ), N2 )
                  CALL SSYRK( 'L', 'N', N2, K, ALPHA, A( N1+1, 1 ), LDA,
     $                        BETA, C( N1*N2+1 ), N2 )
                  CALL SGEMM( 'N', 'T', N2, N1, K, ALPHA, A( N1+1, 1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( 1 ), N2 )
*
               ELSE
*
*                 N is odd, TRANSR = 'T', UPLO = 'U', and TRANS = 'T'
*
                  CALL SSYRK( 'U', 'T', N1, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( N2*N2+1 ), N2 )
                  CALL SSYRK( 'L', 'T', N2, K, ALPHA, A( 1, N1+1 ), LDA,
     $                        BETA, C( N1*N2+1 ), N2 )
                  CALL SGEMM( 'T', 'N', N2, N1, K, ALPHA, A( 1, N1+1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( 1 ), N2 )
*
               END IF
*
            END IF
*
         END IF
*
      ELSE
*
*        N is even
*
         IF( NORMALTRANSR ) THEN
*
*           N is even and TRANSR = 'N'
*
            IF( LOWER ) THEN
*
*              N is even, TRANSR = 'N', and UPLO = 'L'
*
               IF( NOTRANS ) THEN
*
*                 N is even, TRANSR = 'N', UPLO = 'L', and TRANS = 'N'
*
                  CALL SSYRK( 'L', 'N', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( 2 ), N+1 )
                  CALL SSYRK( 'U', 'N', NK, K, ALPHA, A( NK+1, 1 ), LDA,
     $                        BETA, C( 1 ), N+1 )
                  CALL SGEMM( 'N', 'T', NK, NK, K, ALPHA, A( NK+1, 1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( NK+2 ),
     $                        N+1 )
*
               ELSE
*
*                 N is even, TRANSR = 'N', UPLO = 'L', and TRANS = 'T'
*
                  CALL SSYRK( 'L', 'T', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( 2 ), N+1 )
                  CALL SSYRK( 'U', 'T', NK, K, ALPHA, A( 1, NK+1 ), LDA,
     $                        BETA, C( 1 ), N+1 )
                  CALL SGEMM( 'T', 'N', NK, NK, K, ALPHA, A( 1, NK+1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( NK+2 ),
     $                        N+1 )
*
               END IF
*
            ELSE
*
*              N is even, TRANSR = 'N', and UPLO = 'U'
*
               IF( NOTRANS ) THEN
*
*                 N is even, TRANSR = 'N', UPLO = 'U', and TRANS = 'N'
*
                  CALL SSYRK( 'L', 'N', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( NK+2 ), N+1 )
                  CALL SSYRK( 'U', 'N', NK, K, ALPHA, A( NK+1, 1 ), LDA,
     $                        BETA, C( NK+1 ), N+1 )
                  CALL SGEMM( 'N', 'T', NK, NK, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( NK+1, 1 ), LDA, BETA, C( 1 ),
     $                        N+1 )
*
               ELSE
*
*                 N is even, TRANSR = 'N', UPLO = 'U', and TRANS = 'T'
*
                  CALL SSYRK( 'L', 'T', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( NK+2 ), N+1 )
                  CALL SSYRK( 'U', 'T', NK, K, ALPHA, A( 1, NK+1 ), LDA,
     $                        BETA, C( NK+1 ), N+1 )
                  CALL SGEMM( 'T', 'N', NK, NK, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( 1, NK+1 ), LDA, BETA, C( 1 ),
     $                        N+1 )
*
               END IF
*
            END IF
*
         ELSE
*
*           N is even, and TRANSR = 'T'
*
            IF( LOWER ) THEN
*
*              N is even, TRANSR = 'T', and UPLO = 'L'
*
               IF( NOTRANS ) THEN
*
*                 N is even, TRANSR = 'T', UPLO = 'L', and TRANS = 'N'
*
                  CALL SSYRK( 'U', 'N', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( NK+1 ), NK )
                  CALL SSYRK( 'L', 'N', NK, K, ALPHA, A( NK+1, 1 ), LDA,
     $                        BETA, C( 1 ), NK )
                  CALL SGEMM( 'N', 'T', NK, NK, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( NK+1, 1 ), LDA, BETA,
     $                        C( ( ( NK+1 )*NK )+1 ), NK )
*
               ELSE
*
*                 N is even, TRANSR = 'T', UPLO = 'L', and TRANS = 'T'
*
                  CALL SSYRK( 'U', 'T', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( NK+1 ), NK )
                  CALL SSYRK( 'L', 'T', NK, K, ALPHA, A( 1, NK+1 ), LDA,
     $                        BETA, C( 1 ), NK )
                  CALL SGEMM( 'T', 'N', NK, NK, K, ALPHA, A( 1, 1 ),
     $                        LDA, A( 1, NK+1 ), LDA, BETA,
     $                        C( ( ( NK+1 )*NK )+1 ), NK )
*
               END IF
*
            ELSE
*
*              N is even, TRANSR = 'T', and UPLO = 'U'
*
               IF( NOTRANS ) THEN
*
*                 N is even, TRANSR = 'T', UPLO = 'U', and TRANS = 'N'
*
                  CALL SSYRK( 'U', 'N', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( NK*( NK+1 )+1 ), NK )
                  CALL SSYRK( 'L', 'N', NK, K, ALPHA, A( NK+1, 1 ), LDA,
     $                        BETA, C( NK*NK+1 ), NK )
                  CALL SGEMM( 'N', 'T', NK, NK, K, ALPHA, A( NK+1, 1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( 1 ), NK )
*
               ELSE
*
*                 N is even, TRANSR = 'T', UPLO = 'U', and TRANS = 'T'
*
                  CALL SSYRK( 'U', 'T', NK, K, ALPHA, A( 1, 1 ), LDA,
     $                        BETA, C( NK*( NK+1 )+1 ), NK )
                  CALL SSYRK( 'L', 'T', NK, K, ALPHA, A( 1, NK+1 ), LDA,
     $                        BETA, C( NK*NK+1 ), NK )
                  CALL SGEMM( 'T', 'N', NK, NK, K, ALPHA, A( 1, NK+1 ),
     $                        LDA, A( 1, 1 ), LDA, BETA, C( 1 ), NK )
*
               END IF
*
            END IF
*
         END IF
*
      END IF
*
      RETURN
*
*     End of SSFRK
*
      END
