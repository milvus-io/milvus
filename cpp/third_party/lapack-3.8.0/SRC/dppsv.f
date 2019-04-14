*> \brief <b> DPPSV computes the solution to system of linear equations A * X = B for OTHER matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DPPSV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dppsv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dppsv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dppsv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DPPSV( UPLO, N, NRHS, AP, B, LDB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDB, N, NRHS
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   AP( * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DPPSV computes the solution to a real system of linear equations
*>    A * X = B,
*> where A is an N-by-N symmetric positive definite matrix stored in
*> packed format and X and B are N-by-NRHS matrices.
*>
*> The Cholesky decomposition is used to factor A as
*>    A = U**T* U,  if UPLO = 'U', or
*>    A = L * L**T,  if UPLO = 'L',
*> where U is an upper triangular matrix and L is a lower triangular
*> matrix.  The factored form of A is then used to solve the system of
*> equations A * X = B.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
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
*> \param[in,out] AP
*> \verbatim
*>          AP is DOUBLE PRECISION array, dimension (N*(N+1)/2)
*>          On entry, the upper or lower triangle of the symmetric matrix
*>          A, packed columnwise in a linear array.  The j-th column of A
*>          is stored in the array AP as follows:
*>          if UPLO = 'U', AP(i + (j-1)*j/2) = A(i,j) for 1<=i<=j;
*>          if UPLO = 'L', AP(i + (j-1)*(2n-j)/2) = A(i,j) for j<=i<=n.
*>          See below for further details.
*>
*>          On exit, if INFO = 0, the factor U or L from the Cholesky
*>          factorization A = U**T*U or A = L*L**T, in the same storage
*>          format as A.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
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
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, the leading minor of order i of A is not
*>                positive definite, so the factorization could not be
*>                completed, and the solution has not been computed.
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
*> \ingroup doubleOTHERsolve
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The packed storage scheme is illustrated by the following example
*>  when N = 4, UPLO = 'U':
*>
*>  Two-dimensional storage of the symmetric matrix A:
*>
*>     a11 a12 a13 a14
*>         a22 a23 a24
*>             a33 a34     (aij = conjg(aji))
*>                 a44
*>
*>  Packed storage of the upper triangle of A:
*>
*>  AP = [ a11, a12, a22, a13, a23, a33, a14, a24, a34, a44 ]
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DPPSV( UPLO, N, NRHS, AP, B, LDB, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDB, N, NRHS
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   AP( * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DPPTRF, DPPTRS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DPPSV ', -INFO )
         RETURN
      END IF
*
*     Compute the Cholesky factorization A = U**T*U or A = L*L**T.
*
      CALL DPPTRF( UPLO, N, AP, INFO )
      IF( INFO.EQ.0 ) THEN
*
*        Solve the system A*X = B, overwriting B with X.
*
         CALL DPPTRS( UPLO, N, NRHS, AP, B, LDB, INFO )
*
      END IF
      RETURN
*
*     End of DPPSV
*
      END
