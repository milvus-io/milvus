*> \brief <b> CHPSV computes the solution to system of linear equations A * X = B for OTHER matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CHPSV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chpsv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chpsv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chpsv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHPSV( UPLO, N, NRHS, AP, IPIV, B, LDB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDB, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            AP( * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CHPSV computes the solution to a complex system of linear equations
*>    A * X = B,
*> where A is an N-by-N Hermitian matrix stored in packed format and X
*> and B are N-by-NRHS matrices.
*>
*> The diagonal pivoting method is used to factor A as
*>    A = U * D * U**H,  if UPLO = 'U', or
*>    A = L * D * L**H,  if UPLO = 'L',
*> where U (or L) is a product of permutation and unit upper (lower)
*> triangular matrices, D is Hermitian and block diagonal with 1-by-1
*> and 2-by-2 diagonal blocks.  The factored form of A is then used to
*> solve the system of equations A * X = B.
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
*>          AP is COMPLEX array, dimension (N*(N+1)/2)
*>          On entry, the upper or lower triangle of the Hermitian matrix
*>          A, packed columnwise in a linear array.  The j-th column of A
*>          is stored in the array AP as follows:
*>          if UPLO = 'U', AP(i + (j-1)*j/2) = A(i,j) for 1<=i<=j;
*>          if UPLO = 'L', AP(i + (j-1)*(2n-j)/2) = A(i,j) for j<=i<=n.
*>          See below for further details.
*>
*>          On exit, the block diagonal matrix D and the multipliers used
*>          to obtain the factor U or L from the factorization
*>          A = U*D*U**H or A = L*D*L**H as computed by CHPTRF, stored as
*>          a packed triangular matrix in the same storage format as A.
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges and the block structure of D, as
*>          determined by CHPTRF.  If IPIV(k) > 0, then rows and columns
*>          k and IPIV(k) were interchanged, and D(k,k) is a 1-by-1
*>          diagonal block.  If UPLO = 'U' and IPIV(k) = IPIV(k-1) < 0,
*>          then rows and columns k-1 and -IPIV(k) were interchanged and
*>          D(k-1:k,k-1:k) is a 2-by-2 diagonal block.  If UPLO = 'L' and
*>          IPIV(k) = IPIV(k+1) < 0, then rows and columns k+1 and
*>          -IPIV(k) were interchanged and D(k:k+1,k:k+1) is a 2-by-2
*>          diagonal block.
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
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, D(i,i) is exactly zero.  The factorization
*>                has been completed, but the block diagonal matrix D is
*>                exactly singular, so the solution could not be
*>                computed.
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
*> \ingroup complexOTHERsolve
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The packed storage scheme is illustrated by the following example
*>  when N = 4, UPLO = 'U':
*>
*>  Two-dimensional storage of the Hermitian matrix A:
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
      SUBROUTINE CHPSV( UPLO, N, NRHS, AP, IPIV, B, LDB, INFO )
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
      INTEGER            IPIV( * )
      COMPLEX            AP( * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHPTRF, CHPTRS, XERBLA
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
         INFO = -7
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CHPSV ', -INFO )
         RETURN
      END IF
*
*     Compute the factorization A = U*D*U**H or A = L*D*L**H.
*
      CALL CHPTRF( UPLO, N, AP, IPIV, INFO )
      IF( INFO.EQ.0 ) THEN
*
*        Solve the system A*X = B, overwriting B with X.
*
         CALL CHPTRS( UPLO, N, NRHS, AP, IPIV, B, LDB, INFO )
*
      END IF
      RETURN
*
*     End of CHPSV
*
      END
