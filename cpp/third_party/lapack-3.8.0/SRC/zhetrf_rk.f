*> \brief \b ZHETRF_RK computes the factorization of a complex Hermitian indefinite matrix using the bounded Bunch-Kaufman (rook) diagonal pivoting method (BLAS3 blocked algorithm).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHETRF_RK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhetrf_rk.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhetrf_rk.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhetrf_rk.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHETRF_RK( UPLO, N, A, LDA, E, IPIV, WORK, LWORK,
*                             INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, LWORK, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX*16         A( LDA, * ), E ( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> ZHETRF_RK computes the factorization of a complex Hermitian matrix A
*> using the bounded Bunch-Kaufman (rook) diagonal pivoting method:
*>
*>    A = P*U*D*(U**H)*(P**T) or A = P*L*D*(L**H)*(P**T),
*>
*> where U (or L) is unit upper (or lower) triangular matrix,
*> U**H (or L**H) is the conjugate of U (or L), P is a permutation
*> matrix, P**T is the transpose of P, and D is Hermitian and block
*> diagonal with 1-by-1 and 2-by-2 diagonal blocks.
*>
*> This is the blocked version of the algorithm, calling Level 3 BLAS.
*> For more information see Further Details section.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          Hermitian matrix A is stored:
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the Hermitian matrix A.
*>            If UPLO = 'U': the leading N-by-N upper triangular part
*>            of A contains the upper triangular part of the matrix A,
*>            and the strictly lower triangular part of A is not
*>            referenced.
*>
*>            If UPLO = 'L': the leading N-by-N lower triangular part
*>            of A contains the lower triangular part of the matrix A,
*>            and the strictly upper triangular part of A is not
*>            referenced.
*>
*>          On exit, contains:
*>            a) ONLY diagonal elements of the Hermitian block diagonal
*>               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k);
*>               (superdiagonal (or subdiagonal) elements of D
*>                are stored on exit in array E), and
*>            b) If UPLO = 'U': factor U in the superdiagonal part of A.
*>               If UPLO = 'L': factor L in the subdiagonal part of A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is COMPLEX*16 array, dimension (N)
*>          On exit, contains the superdiagonal (or subdiagonal)
*>          elements of the Hermitian block diagonal matrix D
*>          with 1-by-1 or 2-by-2 diagonal blocks, where
*>          If UPLO = 'U': E(i) = D(i-1,i), i=2:N, E(1) is set to 0;
*>          If UPLO = 'L': E(i) = D(i+1,i), i=1:N-1, E(N) is set to 0.
*>
*>          NOTE: For 1-by-1 diagonal block D(k), where
*>          1 <= k <= N, the element E(k) is set to 0 in both
*>          UPLO = 'U' or UPLO = 'L' cases.
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          IPIV describes the permutation matrix P in the factorization
*>          of matrix A as follows. The absolute value of IPIV(k)
*>          represents the index of row and column that were
*>          interchanged with the k-th row and column. The value of UPLO
*>          describes the order in which the interchanges were applied.
*>          Also, the sign of IPIV represents the block structure of
*>          the Hermitian block diagonal matrix D with 1-by-1 or 2-by-2
*>          diagonal blocks which correspond to 1 or 2 interchanges
*>          at each factorization step. For more info see Further
*>          Details section.
*>
*>          If UPLO = 'U',
*>          ( in factorization order, k decreases from N to 1 ):
*>            a) A single positive entry IPIV(k) > 0 means:
*>               D(k,k) is a 1-by-1 diagonal block.
*>               If IPIV(k) != k, rows and columns k and IPIV(k) were
*>               interchanged in the matrix A(1:N,1:N);
*>               If IPIV(k) = k, no interchange occurred.
*>
*>            b) A pair of consecutive negative entries
*>               IPIV(k) < 0 and IPIV(k-1) < 0 means:
*>               D(k-1:k,k-1:k) is a 2-by-2 diagonal block.
*>               (NOTE: negative entries in IPIV appear ONLY in pairs).
*>               1) If -IPIV(k) != k, rows and columns
*>                  k and -IPIV(k) were interchanged
*>                  in the matrix A(1:N,1:N).
*>                  If -IPIV(k) = k, no interchange occurred.
*>               2) If -IPIV(k-1) != k-1, rows and columns
*>                  k-1 and -IPIV(k-1) were interchanged
*>                  in the matrix A(1:N,1:N).
*>                  If -IPIV(k-1) = k-1, no interchange occurred.
*>
*>            c) In both cases a) and b), always ABS( IPIV(k) ) <= k.
*>
*>            d) NOTE: Any entry IPIV(k) is always NONZERO on output.
*>
*>          If UPLO = 'L',
*>          ( in factorization order, k increases from 1 to N ):
*>            a) A single positive entry IPIV(k) > 0 means:
*>               D(k,k) is a 1-by-1 diagonal block.
*>               If IPIV(k) != k, rows and columns k and IPIV(k) were
*>               interchanged in the matrix A(1:N,1:N).
*>               If IPIV(k) = k, no interchange occurred.
*>
*>            b) A pair of consecutive negative entries
*>               IPIV(k) < 0 and IPIV(k+1) < 0 means:
*>               D(k:k+1,k:k+1) is a 2-by-2 diagonal block.
*>               (NOTE: negative entries in IPIV appear ONLY in pairs).
*>               1) If -IPIV(k) != k, rows and columns
*>                  k and -IPIV(k) were interchanged
*>                  in the matrix A(1:N,1:N).
*>                  If -IPIV(k) = k, no interchange occurred.
*>               2) If -IPIV(k+1) != k+1, rows and columns
*>                  k-1 and -IPIV(k-1) were interchanged
*>                  in the matrix A(1:N,1:N).
*>                  If -IPIV(k+1) = k+1, no interchange occurred.
*>
*>            c) In both cases a) and b), always ABS( IPIV(k) ) >= k.
*>
*>            d) NOTE: Any entry IPIV(k) is always NONZERO on output.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension ( MAX(1,LWORK) ).
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of WORK.  LWORK >=1.  For best performance
*>          LWORK >= N*NB, where NB is the block size returned
*>          by ILAENV.
*>
*>          If LWORK = -1, then a workspace query is assumed;
*>          the routine only calculates the optimal size of the WORK
*>          array, returns this value as the first entry of the WORK
*>          array, and no error message related to LWORK is issued
*>          by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>
*>          < 0: If INFO = -k, the k-th argument had an illegal value
*>
*>          > 0: If INFO = k, the matrix A is singular, because:
*>                 If UPLO = 'U': column k in the upper
*>                 triangular part of A contains all zeros.
*>                 If UPLO = 'L': column k in the lower
*>                 triangular part of A contains all zeros.
*>
*>               Therefore D(k,k) is exactly zero, and superdiagonal
*>               elements of column k of U (or subdiagonal elements of
*>               column k of L ) are all zeros. The factorization has
*>               been completed, but the block diagonal matrix D is
*>               exactly singular, and division by zero will occur if
*>               it is used to solve a system of equations.
*>
*>               NOTE: INFO only stores the first occurrence of
*>               a singularity, any subsequent occurrence of singularity
*>               is not stored in INFO even though the factorization
*>               always completes.
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
*> \ingroup complex16HEcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*> TODO: put correct description
*> \endverbatim
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  December 2016,  Igor Kozachenko,
*>                  Computer Science Division,
*>                  University of California, Berkeley
*>
*>  September 2007, Sven Hammarling, Nicholas J. Higham, Craig Lucas,
*>                  School of Mathematics,
*>                  University of Manchester
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZHETRF_RK( UPLO, N, A, LDA, E, IPIV, WORK, LWORK,
     $                      INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, LWORK, N
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX*16         A( LDA, * ), E( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            LQUERY, UPPER
      INTEGER            I, IINFO, IP, IWS, K, KB, LDWORK, LWKOPT,
     $                   NB, NBMIN
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      EXTERNAL           LSAME, ILAENV
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZLAHEF_RK, ZHETF2_RK, ZSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      LQUERY = ( LWORK.EQ.-1 )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      ELSE IF( LWORK.LT.1 .AND. .NOT.LQUERY ) THEN
         INFO = -8
      END IF
*
      IF( INFO.EQ.0 ) THEN
*
*        Determine the block size
*
         NB = ILAENV( 1, 'ZHETRF_RK', UPLO, N, -1, -1, -1 )
         LWKOPT = N*NB
         WORK( 1 ) = LWKOPT
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHETRF_RK', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
      NBMIN = 2
      LDWORK = N
      IF( NB.GT.1 .AND. NB.LT.N ) THEN
         IWS = LDWORK*NB
         IF( LWORK.LT.IWS ) THEN
            NB = MAX( LWORK / LDWORK, 1 )
            NBMIN = MAX( 2, ILAENV( 2, 'ZHETRF_RK',
     $                              UPLO, N, -1, -1, -1 ) )
         END IF
      ELSE
         IWS = 1
      END IF
      IF( NB.LT.NBMIN )
     $   NB = N
*
      IF( UPPER ) THEN
*
*        Factorize A as U*D*U**T using the upper triangle of A
*
*        K is the main loop index, decreasing from N to 1 in steps of
*        KB, where KB is the number of columns factorized by ZLAHEF_RK;
*        KB is either NB or NB-1, or K for the last block
*
         K = N
   10    CONTINUE
*
*        If K < 1, exit from loop
*
         IF( K.LT.1 )
     $      GO TO 15
*
         IF( K.GT.NB ) THEN
*
*           Factorize columns k-kb+1:k of A and use blocked code to
*           update columns 1:k-kb
*
            CALL ZLAHEF_RK( UPLO, K, NB, KB, A, LDA, E,
     $                      IPIV, WORK, LDWORK, IINFO )
         ELSE
*
*           Use unblocked code to factorize columns 1:k of A
*
            CALL ZHETF2_RK( UPLO, K, A, LDA, E, IPIV, IINFO )
            KB = K
         END IF
*
*        Set INFO on the first occurrence of a zero pivot
*
         IF( INFO.EQ.0 .AND. IINFO.GT.0 )
     $      INFO = IINFO
*
*        No need to adjust IPIV
*
*
*        Apply permutations to the leading panel 1:k-1
*
*        Read IPIV from the last block factored, i.e.
*        indices  k-kb+1:k and apply row permutations to the
*        last k+1 colunms k+1:N after that block
*        (We can do the simple loop over IPIV with decrement -1,
*        since the ABS value of IPIV( I ) represents the row index
*        of the interchange with row i in both 1x1 and 2x2 pivot cases)
*
         IF( K.LT.N ) THEN
            DO I = K, ( K - KB + 1 ), -1
               IP = ABS( IPIV( I ) )
               IF( IP.NE.I ) THEN
                  CALL ZSWAP( N-K, A( I, K+1 ), LDA,
     $                        A( IP, K+1 ), LDA )
               END IF
            END DO
         END IF
*
*        Decrease K and return to the start of the main loop
*
         K = K - KB
         GO TO 10
*
*        This label is the exit from main loop over K decreasing
*        from N to 1 in steps of KB
*
   15    CONTINUE
*
      ELSE
*
*        Factorize A as L*D*L**T using the lower triangle of A
*
*        K is the main loop index, increasing from 1 to N in steps of
*        KB, where KB is the number of columns factorized by ZLAHEF_RK;
*        KB is either NB or NB-1, or N-K+1 for the last block
*
         K = 1
   20    CONTINUE
*
*        If K > N, exit from loop
*
         IF( K.GT.N )
     $      GO TO 35
*
         IF( K.LE.N-NB ) THEN
*
*           Factorize columns k:k+kb-1 of A and use blocked code to
*           update columns k+kb:n
*
            CALL ZLAHEF_RK( UPLO, N-K+1, NB, KB, A( K, K ), LDA, E( K ),
     $                        IPIV( K ), WORK, LDWORK, IINFO )


         ELSE
*
*           Use unblocked code to factorize columns k:n of A
*
            CALL ZHETF2_RK( UPLO, N-K+1, A( K, K ), LDA, E( K ),
     $                      IPIV( K ), IINFO )
            KB = N - K + 1
*
         END IF
*
*        Set INFO on the first occurrence of a zero pivot
*
         IF( INFO.EQ.0 .AND. IINFO.GT.0 )
     $      INFO = IINFO + K - 1
*
*        Adjust IPIV
*
         DO I = K, K + KB - 1
            IF( IPIV( I ).GT.0 ) THEN
               IPIV( I ) = IPIV( I ) + K - 1
            ELSE
               IPIV( I ) = IPIV( I ) - K + 1
            END IF
         END DO
*
*        Apply permutations to the leading panel 1:k-1
*
*        Read IPIV from the last block factored, i.e.
*        indices  k:k+kb-1 and apply row permutations to the
*        first k-1 colunms 1:k-1 before that block
*        (We can do the simple loop over IPIV with increment 1,
*        since the ABS value of IPIV( I ) represents the row index
*        of the interchange with row i in both 1x1 and 2x2 pivot cases)
*
         IF( K.GT.1 ) THEN
            DO I = K, ( K + KB - 1 ), 1
               IP = ABS( IPIV( I ) )
               IF( IP.NE.I ) THEN
                  CALL ZSWAP( K-1, A( I, 1 ), LDA,
     $                        A( IP, 1 ), LDA )
               END IF
            END DO
         END IF
*
*        Increase K and return to the start of the main loop
*
         K = K + KB
         GO TO 20
*
*        This label is the exit from main loop over K increasing
*        from 1 to N in steps of KB
*
   35    CONTINUE
*
*     End Lower
*
      END IF
*
      WORK( 1 ) = LWKOPT
      RETURN
*
*     End of ZHETRF_RK
*
      END
