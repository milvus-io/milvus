*> \brief \b SPTTS2 solves a tridiagonal system of the form AX=B using the L D LH factorization computed by spttrf.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SPTTS2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sptts2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sptts2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sptts2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SPTTS2( N, NRHS, D, E, B, LDB )
*
*       .. Scalar Arguments ..
*       INTEGER            LDB, N, NRHS
*       ..
*       .. Array Arguments ..
*       REAL               B( LDB, * ), D( * ), E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SPTTS2 solves a tridiagonal system of the form
*>    A * X = B
*> using the L*D*L**T factorization of A computed by SPTTRF.  D is a
*> diagonal matrix specified in the vector D, L is a unit bidiagonal
*> matrix whose subdiagonal is specified in the vector E, and X and B
*> are N by NRHS matrices.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the tridiagonal matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrix B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          The n diagonal elements of the diagonal matrix D from the
*>          L*D*L**T factorization of A.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is REAL array, dimension (N-1)
*>          The (n-1) subdiagonal elements of the unit bidiagonal factor
*>          L from the L*D*L**T factorization of A.  E can also be regarded
*>          as the superdiagonal of the unit bidiagonal factor U from the
*>          factorization A = U**T*D*U.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB,NRHS)
*>          On entry, the right hand side vectors B for the system of
*>          linear equations.
*>          On exit, the solution vectors, X.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
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
*> \ingroup realPTcomputational
*
*  =====================================================================
      SUBROUTINE SPTTS2( N, NRHS, D, E, B, LDB )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDB, N, NRHS
*     ..
*     .. Array Arguments ..
      REAL               B( LDB, * ), D( * ), E( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, J
*     ..
*     .. External Subroutines ..
      EXTERNAL           SSCAL
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( N.LE.1 ) THEN
         IF( N.EQ.1 )
     $      CALL SSCAL( NRHS, 1. / D( 1 ), B, LDB )
         RETURN
      END IF
*
*     Solve A * X = B using the factorization A = L*D*L**T,
*     overwriting each right hand side vector with its solution.
*
      DO 30 J = 1, NRHS
*
*           Solve L * x = b.
*
         DO 10 I = 2, N
            B( I, J ) = B( I, J ) - B( I-1, J )*E( I-1 )
   10    CONTINUE
*
*           Solve D * L**T * x = b.
*
         B( N, J ) = B( N, J ) / D( N )
         DO 20 I = N - 1, 1, -1
            B( I, J ) = B( I, J ) / D( I ) - B( I+1, J )*E( I )
   20    CONTINUE
   30 CONTINUE
*
      RETURN
*
*     End of SPTTS2
*
      END
