*> \brief \b ZTZRQF
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTZRQF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztzrqf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztzrqf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztzrqf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTZRQF( M, N, A, LDA, TAU, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, M, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), TAU( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This routine is deprecated and has been replaced by routine ZTZRZF.
*>
*> ZTZRQF reduces the M-by-N ( M<=N ) complex upper trapezoidal matrix A
*> to upper triangular form by means of unitary transformations.
*>
*> The upper trapezoidal matrix A is factored as
*>
*>    A = ( R  0 ) * Z,
*>
*> where Z is an N-by-N unitary matrix and R is an M-by-M upper
*> triangular matrix.
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
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= M.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the leading M-by-N upper trapezoidal part of the
*>          array A must contain the matrix to be factorized.
*>          On exit, the leading M-by-M upper triangular part of A
*>          contains the upper triangular matrix R, and elements M+1 to
*>          N of the first M rows of A, with the array TAU, represent the
*>          unitary matrix Z as a product of M elementary reflectors.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array, dimension (M)
*>          The scalar factors of the elementary reflectors.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
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
*>  The  factorization is obtained by Householder's method.  The kth
*>  transformation matrix, Z( k ), whose conjugate transpose is used to
*>  introduce zeros into the (m - k + 1)th row of A, is given in the form
*>
*>     Z( k ) = ( I     0   ),
*>              ( 0  T( k ) )
*>
*>  where
*>
*>     T( k ) = I - tau*u( k )*u( k )**H,   u( k ) = (   1    ),
*>                                                   (   0    )
*>                                                   ( z( k ) )
*>
*>  tau is a scalar and z( k ) is an ( n - m ) element vector.
*>  tau and z( k ) are chosen to annihilate the elements of the kth row
*>  of X.
*>
*>  The scalar tau is returned in the kth element of TAU and the vector
*>  u( k ) in the kth row of A, such that the elements of z( k ) are
*>  in  a( k, m + 1 ), ..., a( k, n ). The elements of R are returned in
*>  the upper triangular part of A.
*>
*>  Z is given by
*>
*>     Z =  Z( 1 ) * Z( 2 ) * ... * Z( m ).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZTZRQF( M, N, A, LDA, TAU, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, M, N
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), TAU( * )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CONE, CZERO
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ),
     $                   CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, K, M1
      COMPLEX*16         ALPHA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCONJG, MAX, MIN
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZAXPY, ZCOPY, ZGEMV, ZGERC, ZLACGV,
     $                   ZLARFG
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.M ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTZRQF', -INFO )
         RETURN
      END IF
*
*     Perform the factorization.
*
      IF( M.EQ.0 )
     $   RETURN
      IF( M.EQ.N ) THEN
         DO 10 I = 1, N
            TAU( I ) = CZERO
   10    CONTINUE
      ELSE
         M1 = MIN( M+1, N )
         DO 20 K = M, 1, -1
*
*           Use a Householder reflection to zero the kth row of A.
*           First set up the reflection.
*
            A( K, K ) = DCONJG( A( K, K ) )
            CALL ZLACGV( N-M, A( K, M1 ), LDA )
            ALPHA = A( K, K )
            CALL ZLARFG( N-M+1, ALPHA, A( K, M1 ), LDA, TAU( K ) )
            A( K, K ) = ALPHA
            TAU( K ) = DCONJG( TAU( K ) )
*
            IF( TAU( K ).NE.CZERO .AND. K.GT.1 ) THEN
*
*              We now perform the operation  A := A*P( k )**H.
*
*              Use the first ( k - 1 ) elements of TAU to store  a( k ),
*              where  a( k ) consists of the first ( k - 1 ) elements of
*              the  kth column  of  A.  Also  let  B  denote  the  first
*              ( k - 1 ) rows of the last ( n - m ) columns of A.
*
               CALL ZCOPY( K-1, A( 1, K ), 1, TAU, 1 )
*
*              Form   w = a( k ) + B*z( k )  in TAU.
*
               CALL ZGEMV( 'No transpose', K-1, N-M, CONE, A( 1, M1 ),
     $                     LDA, A( K, M1 ), LDA, CONE, TAU, 1 )
*
*              Now form  a( k ) := a( k ) - conjg(tau)*w
*              and       B      := B      - conjg(tau)*w*z( k )**H.
*
               CALL ZAXPY( K-1, -DCONJG( TAU( K ) ), TAU, 1, A( 1, K ),
     $                     1 )
               CALL ZGERC( K-1, N-M, -DCONJG( TAU( K ) ), TAU, 1,
     $                     A( K, M1 ), LDA, A( 1, M1 ), LDA )
            END IF
   20    CONTINUE
      END IF
*
      RETURN
*
*     End of ZTZRQF
*
      END
