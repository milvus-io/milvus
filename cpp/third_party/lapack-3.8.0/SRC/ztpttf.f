*> \brief \b ZTPTTF copies a triangular matrix from the standard packed format (TP) to the rectangular full packed format (TF).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTPTTF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztpttf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztpttf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztpttf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTPTTF( TRANSR, UPLO, N, AP, ARF, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANSR, UPLO
*       INTEGER            INFO, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         AP( 0: * ), ARF( 0: * )
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTPTTF copies a triangular matrix A from standard packed format (TP)
*> to rectangular full packed format (TF).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANSR
*> \verbatim
*>          TRANSR is CHARACTER*1
*>          = 'N':  ARF in Normal format is wanted;
*>          = 'C':  ARF in Conjugate-transpose format is wanted.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  A is upper triangular;
*>          = 'L':  A is lower triangular.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] AP
*> \verbatim
*>          AP is COMPLEX*16 array, dimension ( N*(N+1)/2 ),
*>          On entry, the upper or lower triangular matrix A, packed
*>          columnwise in a linear array. The j-th column of A is stored
*>          in the array AP as follows:
*>          if UPLO = 'U', AP(i + (j-1)*j/2) = A(i,j) for 1<=i<=j;
*>          if UPLO = 'L', AP(i + (j-1)*(2n-j)/2) = A(i,j) for j<=i<=n.
*> \endverbatim
*>
*> \param[out] ARF
*> \verbatim
*>          ARF is COMPLEX*16 array, dimension ( N*(N+1)/2 ),
*>          On exit, the upper or lower triangular matrix A stored in
*>          RFP format. For a further discussion see Notes below.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
      SUBROUTINE ZTPTTF( TRANSR, UPLO, N, AP, ARF, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANSR, UPLO
      INTEGER            INFO, N
*     ..
*     .. Array Arguments ..
      COMPLEX*16         AP( 0: * ), ARF( 0: * )
*
*  =====================================================================
*
*     .. Parameters ..
*     ..
*     .. Local Scalars ..
      LOGICAL            LOWER, NISODD, NORMALTRANSR
      INTEGER            N1, N2, K, NT
      INTEGER            I, J, IJ
      INTEGER            IJP, JP, LDA, JS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCONJG, MOD
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      NORMALTRANSR = LSAME( TRANSR, 'N' )
      LOWER = LSAME( UPLO, 'L' )
      IF( .NOT.NORMALTRANSR .AND. .NOT.LSAME( TRANSR, 'C' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LOWER .AND. .NOT.LSAME( UPLO, 'U' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTPTTF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      IF( N.EQ.1 ) THEN
         IF( NORMALTRANSR ) THEN
            ARF( 0 ) = AP( 0 )
         ELSE
            ARF( 0 ) = DCONJG( AP( 0 ) )
         END IF
         RETURN
      END IF
*
*     Size of array ARF(0:NT-1)
*
      NT = N*( N+1 ) / 2
*
*     Set N1 and N2 depending on LOWER
*
      IF( LOWER ) THEN
         N2 = N / 2
         N1 = N - N2
      ELSE
         N1 = N / 2
         N2 = N - N1
      END IF
*
*     If N is odd, set NISODD = .TRUE.
*     If N is even, set K = N/2 and NISODD = .FALSE.
*
*     set lda of ARF^C; ARF^C is (0:(N+1)/2-1,0:N-noe)
*     where noe = 0 if n is even, noe = 1 if n is odd
*
      IF( MOD( N, 2 ).EQ.0 ) THEN
         K = N / 2
         NISODD = .FALSE.
         LDA = N + 1
      ELSE
         NISODD = .TRUE.
         LDA = N
      END IF
*
*     ARF^C has lda rows and n+1-noe cols
*
      IF( .NOT.NORMALTRANSR )
     $   LDA = ( N+1 ) / 2
*
*     start execution: there are eight cases
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
*             SRPA for LOWER, NORMAL and N is odd ( a(0:n-1,0:n1-1) )
*             T1 -> a(0,0), T2 -> a(0,1), S -> a(n1,0)
*             T1 -> a(0), T2 -> a(n), S -> a(n1); lda = n
*
               IJP = 0
               JP = 0
               DO J = 0, N2
                  DO I = J, N - 1
                     IJ = I + JP
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JP = JP + LDA
               END DO
               DO I = 0, N2 - 1
                  DO J = 1 + I, N2
                     IJ = I + J*LDA
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                  END DO
               END DO
*
            ELSE
*
*             SRPA for UPPER, NORMAL and N is odd ( a(0:n-1,0:n2-1)
*             T1 -> a(n1+1,0), T2 -> a(n1,0), S -> a(0,0)
*             T1 -> a(n2), T2 -> a(n1), S -> a(0)
*
               IJP = 0
               DO J = 0, N1 - 1
                  IJ = N2 + J
                  DO I = 0, J
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                     IJ = IJ + LDA
                  END DO
               END DO
               JS = 0
               DO J = N1, N - 1
                  IJ = JS
                  DO IJ = JS, JS + J
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JS = JS + LDA
               END DO
*
            END IF
*
         ELSE
*
*           N is odd and TRANSR = 'C'
*
            IF( LOWER ) THEN
*
*              SRPA for LOWER, TRANSPOSE and N is odd
*              T1 -> A(0,0) , T2 -> A(1,0) , S -> A(0,n1)
*              T1 -> a(0+0) , T2 -> a(1+0) , S -> a(0+n1*n1); lda=n1
*
               IJP = 0
               DO I = 0, N2
                  DO IJ = I*( LDA+1 ), N*LDA - 1, LDA
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                  END DO
               END DO
               JS = 1
               DO J = 0, N2 - 1
                  DO IJ = JS, JS + N2 - J - 1
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JS = JS + LDA + 1
               END DO
*
            ELSE
*
*              SRPA for UPPER, TRANSPOSE and N is odd
*              T1 -> A(0,n1+1), T2 -> A(0,n1), S -> A(0,0)
*              T1 -> a(n2*n2), T2 -> a(n1*n2), S -> a(0); lda = n2
*
               IJP = 0
               JS = N2*LDA
               DO J = 0, N1 - 1
                  DO IJ = JS, JS + J
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JS = JS + LDA
               END DO
               DO I = 0, N1
                  DO IJ = I, I + ( N1+I )*LDA, LDA
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                  END DO
               END DO
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
*              SRPA for LOWER, NORMAL, and N is even ( a(0:n,0:k-1) )
*              T1 -> a(1,0), T2 -> a(0,0), S -> a(k+1,0)
*              T1 -> a(1), T2 -> a(0), S -> a(k+1)
*
               IJP = 0
               JP = 0
               DO J = 0, K - 1
                  DO I = J, N - 1
                     IJ = 1 + I + JP
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JP = JP + LDA
               END DO
               DO I = 0, K - 1
                  DO J = I, K - 1
                     IJ = I + J*LDA
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                  END DO
               END DO
*
            ELSE
*
*              SRPA for UPPER, NORMAL, and N is even ( a(0:n,0:k-1) )
*              T1 -> a(k+1,0) ,  T2 -> a(k,0),   S -> a(0,0)
*              T1 -> a(k+1), T2 -> a(k), S -> a(0)
*
               IJP = 0
               DO J = 0, K - 1
                  IJ = K + 1 + J
                  DO I = 0, J
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                     IJ = IJ + LDA
                  END DO
               END DO
               JS = 0
               DO J = K, N - 1
                  IJ = JS
                  DO IJ = JS, JS + J
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JS = JS + LDA
               END DO
*
            END IF
*
         ELSE
*
*           N is even and TRANSR = 'C'
*
            IF( LOWER ) THEN
*
*              SRPA for LOWER, TRANSPOSE and N is even (see paper)
*              T1 -> B(0,1), T2 -> B(0,0), S -> B(0,k+1)
*              T1 -> a(0+k), T2 -> a(0+0), S -> a(0+k*(k+1)); lda=k
*
               IJP = 0
               DO I = 0, K - 1
                  DO IJ = I + ( I+1 )*LDA, ( N+1 )*LDA - 1, LDA
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                  END DO
               END DO
               JS = 0
               DO J = 0, K - 1
                  DO IJ = JS, JS + K - J - 1
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JS = JS + LDA + 1
               END DO
*
            ELSE
*
*              SRPA for UPPER, TRANSPOSE and N is even (see paper)
*              T1 -> B(0,k+1),     T2 -> B(0,k),   S -> B(0,0)
*              T1 -> a(0+k*(k+1)), T2 -> a(0+k*k), S -> a(0+0)); lda=k
*
               IJP = 0
               JS = ( K+1 )*LDA
               DO J = 0, K - 1
                  DO IJ = JS, JS + J
                     ARF( IJ ) = AP( IJP )
                     IJP = IJP + 1
                  END DO
                  JS = JS + LDA
               END DO
               DO I = 0, K - 1
                  DO IJ = I, I + ( K+I )*LDA, LDA
                     ARF( IJ ) = DCONJG( AP( IJP ) )
                     IJP = IJP + 1
                  END DO
               END DO
*
            END IF
*
         END IF
*
      END IF
*
      RETURN
*
*     End of ZTPTTF
*
      END
