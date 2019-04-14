*> \brief \b DLAPLL measures the linear dependence of two vectors.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAPLL + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlapll.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlapll.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlapll.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAPLL( N, X, INCX, Y, INCY, SSMIN )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX, INCY, N
*       DOUBLE PRECISION   SSMIN
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   X( * ), Y( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Given two column vectors X and Y, let
*>
*>                      A = ( X Y ).
*>
*> The subroutine first computes the QR factorization of A = Q*R,
*> and then computes the SVD of the 2-by-2 upper triangular matrix R.
*> The smaller singular value of R is returned in SSMIN, which is used
*> as the measurement of the linear dependency of the vectors X and Y.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The length of the vectors X and Y.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCX)
*>          On entry, X contains the N-vector X.
*>          On exit, X is overwritten.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>          The increment between successive elements of X. INCX > 0.
*> \endverbatim
*>
*> \param[in,out] Y
*> \verbatim
*>          Y is DOUBLE PRECISION array,
*>                         dimension (1+(N-1)*INCY)
*>          On entry, Y contains the N-vector Y.
*>          On exit, Y is overwritten.
*> \endverbatim
*>
*> \param[in] INCY
*> \verbatim
*>          INCY is INTEGER
*>          The increment between successive elements of Y. INCY > 0.
*> \endverbatim
*>
*> \param[out] SSMIN
*> \verbatim
*>          SSMIN is DOUBLE PRECISION
*>          The smallest singular value of the N-by-2 matrix A = ( X Y ).
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
*> \ingroup doubleOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE DLAPLL( N, X, INCX, Y, INCY, SSMIN )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INCX, INCY, N
      DOUBLE PRECISION   SSMIN
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   X( * ), Y( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   A11, A12, A22, C, SSMAX, TAU
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DDOT
      EXTERNAL           DDOT
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DLARFG, DLAS2
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( N.LE.1 ) THEN
         SSMIN = ZERO
         RETURN
      END IF
*
*     Compute the QR factorization of the N-by-2 matrix ( X Y )
*
      CALL DLARFG( N, X( 1 ), X( 1+INCX ), INCX, TAU )
      A11 = X( 1 )
      X( 1 ) = ONE
*
      C = -TAU*DDOT( N, X, INCX, Y, INCY )
      CALL DAXPY( N, C, X, INCX, Y, INCY )
*
      CALL DLARFG( N-1, Y( 1+INCY ), Y( 1+2*INCY ), INCY, TAU )
*
      A12 = Y( 1 )
      A22 = Y( 1+INCY )
*
*     Compute the SVD of 2-by-2 Upper triangular matrix.
*
      CALL DLAS2( A11, A12, A22, SSMIN, SSMAX )
*
      RETURN
*
*     End of DLAPLL
*
      END
