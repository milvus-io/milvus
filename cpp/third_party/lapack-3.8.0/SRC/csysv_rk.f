*> \brief <b> CSYSV_RK computes the solution to system of linear equations A * X = B for SY matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CSYSV_RK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/csysv_rk.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/csysv_rk.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/csysv_rk.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CSYSV_RK( UPLO, N, NRHS, A, LDA, E, IPIV, B, LDB,
*                            WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, LDB, LWORK, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * ), B( LDB, * ), E( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> CSYSV_RK computes the solution to a complex system of linear
*> equations A * X = B, where A is an N-by-N symmetric matrix
*> and X and B are N-by-NRHS matrices.
*>
*> The bounded Bunch-Kaufman (rook) diagonal pivoting method is used
*> to factor A as
*>    A = P*U*D*(U**T)*(P**T),  if UPLO = 'U', or
*>    A = P*L*D*(L**T)*(P**T),  if UPLO = 'L',
*> where U (or L) is unit upper (or lower) triangular matrix,
*> U**T (or L**T) is the transpose of U (or L), P is a permutation
*> matrix, P**T is the transpose of P, and D is symmetric and block
*> diagonal with 1-by-1 and 2-by-2 diagonal blocks.
*>
*> CSYTRF_RK is called to compute the factorization of a complex
*> symmetric matrix.  The factored form of A is then used to solve
*> the system of equations A * X = B by calling BLAS3 routine CSYTRS_3.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          symmetric matrix A is stored:
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of linear equations, i.e., the order of the
*>          matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrix B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the symmetric matrix A.
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
*>          On exit, if INFO = 0, diagonal of the block diagonal
*>          matrix D and factors U or L  as computed by CSYTRF_RK:
*>            a) ONLY diagonal elements of the symmetric block diagonal
*>               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k);
*>               (superdiagonal (or subdiagonal) elements of D
*>                are stored on exit in array E), and
*>            b) If UPLO = 'U': factor U in the superdiagonal part of A.
*>               If UPLO = 'L': factor L in the subdiagonal part of A.
*>
*>          For more info see the description of CSYTRF_RK routine.
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
*>          E is COMPLEX array, dimension (N)
*>          On exit, contains the output computed by the factorization
*>          routine CSYTRF_RK, i.e. the superdiagonal (or subdiagonal)
*>          elements of the symmetric block diagonal matrix D
*>          with 1-by-1 or 2-by-2 diagonal blocks, where
*>          If UPLO = 'U': E(i) = D(i-1,i), i=2:N, E(1) is set to 0;
*>          If UPLO = 'L': E(i) = D(i+1,i), i=1:N-1, E(N) is set to 0.
*>
*>          NOTE: For 1-by-1 diagonal block D(k), where
*>          1 <= k <= N, the element E(k) is set to 0 in both
*>          UPLO = 'U' or UPLO = 'L' cases.
*>
*>          For more info see the description of CSYTRF_RK routine.
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges and the block structure of D,
*>          as determined by CSYTRF_RK.
*>
*>          For more info see the description of CSYTRF_RK routine.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
*>          On entry, the N-by-NRHS right hand side matrix B.
*>          On exit, if INFO = 0, the N-by-NRHS solution matrix X.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension ( MAX(1,LWORK) ).
*>          Work array used in the factorization stage.
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of WORK.  LWORK >= 1. For best performance
*>          of factorization stage LWORK >= max(1,N*NB), where NB is
*>          the optimal blocksize for CSYTRF_RK.
*>
*>          If LWORK = -1, then a workspace query is assumed;
*>          the routine only calculates the optimal size of the WORK
*>          array for factorization stage, returns this value as
*>          the first entry of the WORK array, and no error message
*>          related to LWORK is issued by XERBLA.
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
*> \ingroup complexSYsolve
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
      SUBROUTINE CSYSV_RK( UPLO, N, NRHS, A, LDA, E, IPIV, B, LDB, WORK,
     $                     LWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, LDB, LWORK, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * ), B( LDB, * ), E( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            LWKOPT
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, CSYTRF_RK, CSYTRS_3
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LWORK.LT.1 .AND. .NOT.LQUERY ) THEN
         INFO = -11
      END IF
*
      IF( INFO.EQ.0 ) THEN
         IF( N.EQ.0 ) THEN
            LWKOPT = 1
         ELSE
            CALL CSYTRF_RK( UPLO, N, A, LDA, E, IPIV, WORK, -1, INFO )
            LWKOPT = WORK(1)
         END IF
         WORK( 1 ) = LWKOPT
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CSYSV_RK ', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Compute the factorization A = U*D*U**T or A = L*D*L**T.
*
      CALL CSYTRF_RK( UPLO, N, A, LDA, E, IPIV, WORK, LWORK, INFO )
*
      IF( INFO.EQ.0 ) THEN
*
*        Solve the system A*X = B with BLAS3 solver, overwriting B with X.
*
         CALL CSYTRS_3( UPLO, N, NRHS, A, LDA, E, IPIV, B, LDB, INFO )
*
      END IF
*
      WORK( 1 ) = LWKOPT
*
      RETURN
*
*     End of CSYSV_RK
*
      END
