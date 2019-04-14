*> \brief \b ZGET52
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGET52( LEFT, N, A, LDA, B, LDB, E, LDE, ALPHA, BETA,
*                          WORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       LOGICAL            LEFT
*       INTEGER            LDA, LDB, LDE, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RESULT( 2 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ),
*      $                   BETA( * ), E( LDE, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGET52  does an eigenvector check for the generalized eigenvalue
*> problem.
*>
*> The basic test for right eigenvectors is:
*>
*>                           | b(i) A E(i) -  a(i) B E(i) |
*>         RESULT(1) = max   -------------------------------
*>                      i    n ulp max( |b(i) A|, |a(i) B| )
*>
*> using the 1-norm.  Here, a(i)/b(i) = w is the i-th generalized
*> eigenvalue of A - w B, or, equivalently, b(i)/a(i) = m is the i-th
*> generalized eigenvalue of m A - B.
*>
*>                         H   H  _      _
*> For left eigenvectors, A , B , a, and b  are used.
*>
*> ZGET52 also tests the normalization of E.  Each eigenvector is
*> supposed to be normalized so that the maximum "absolute value"
*> of its elements is 1, where in this case, "absolute value"
*> of a complex value x is  |Re(x)| + |Im(x)| ; let us call this
*> maximum "absolute value" norm of a vector v  M(v).
*> If a(i)=b(i)=0, then the eigenvector is set to be the jth coordinate
*> vector. The normalization test is:
*>
*>         RESULT(2) =      max       | M(v(i)) - 1 | / ( n ulp )
*>                    eigenvectors v(i)
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] LEFT
*> \verbatim
*>          LEFT is LOGICAL
*>          =.TRUE.:  The eigenvectors in the columns of E are assumed
*>                    to be *left* eigenvectors.
*>          =.FALSE.: The eigenvectors in the columns of E are assumed
*>                    to be *right* eigenvectors.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrices.  If it is zero, ZGET52 does
*>          nothing.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          The matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N)
*>          The matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is COMPLEX*16 array, dimension (LDE, N)
*>          The matrix of eigenvectors.  It must be O( 1 ).
*> \endverbatim
*>
*> \param[in] LDE
*> \verbatim
*>          LDE is INTEGER
*>          The leading dimension of E.  It must be at least 1 and at
*>          least N.
*> \endverbatim
*>
*> \param[in] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX*16 array, dimension (N)
*>          The values a(i) as described above, which, along with b(i),
*>          define the generalized eigenvalues.
*> \endverbatim
*>
*> \param[in] BETA
*> \verbatim
*>          BETA is COMPLEX*16 array, dimension (N)
*>          The values b(i) as described above, which, along with a(i),
*>          define the generalized eigenvalues.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N**2)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (2)
*>          The values computed by the test described above.  If A E or
*>          B E is likely to overflow, then RESULT(1:2) is set to
*>          10 / ulp.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZGET52( LEFT, N, A, LDA, B, LDB, E, LDE, ALPHA, BETA,
     $                   WORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            LEFT
      INTEGER            LDA, LDB, LDE, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RESULT( 2 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ),
     $                   BETA( * ), E( LDE, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      CHARACTER          NORMAB, TRANS
      INTEGER            J, JVEC
      DOUBLE PRECISION   ABMAX, ALFMAX, ANORM, BETMAX, BNORM, ENORM,
     $                   ENRMER, ERRNRM, SAFMAX, SAFMIN, SCALE, TEMP1,
     $                   ULP
      COMPLEX*16         ACOEFF, ALPHAI, BCOEFF, BETAI, X
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, DIMAG, MAX
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   ABS1
*     ..
*     .. Statement Function definitions ..
      ABS1( X ) = ABS( DBLE( X ) ) + ABS( DIMAG( X ) )
*     ..
*     .. Executable Statements ..
*
      RESULT( 1 ) = ZERO
      RESULT( 2 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      SAFMIN = DLAMCH( 'Safe minimum' )
      SAFMAX = ONE / SAFMIN
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
*
      IF( LEFT ) THEN
         TRANS = 'C'
         NORMAB = 'I'
      ELSE
         TRANS = 'N'
         NORMAB = 'O'
      END IF
*
*     Norm of A, B, and E:
*
      ANORM = MAX( ZLANGE( NORMAB, N, N, A, LDA, RWORK ), SAFMIN )
      BNORM = MAX( ZLANGE( NORMAB, N, N, B, LDB, RWORK ), SAFMIN )
      ENORM = MAX( ZLANGE( 'O', N, N, E, LDE, RWORK ), ULP )
      ALFMAX = SAFMAX / MAX( ONE, BNORM )
      BETMAX = SAFMAX / MAX( ONE, ANORM )
*
*     Compute error matrix.
*     Column i = ( b(i) A - a(i) B ) E(i) / max( |a(i) B| |b(i) A| )
*
      DO 10 JVEC = 1, N
         ALPHAI = ALPHA( JVEC )
         BETAI = BETA( JVEC )
         ABMAX = MAX( ABS1( ALPHAI ), ABS1( BETAI ) )
         IF( ABS1( ALPHAI ).GT.ALFMAX .OR. ABS1( BETAI ).GT.BETMAX .OR.
     $       ABMAX.LT.ONE ) THEN
            SCALE = ONE / MAX( ABMAX, SAFMIN )
            ALPHAI = SCALE*ALPHAI
            BETAI = SCALE*BETAI
         END IF
         SCALE = ONE / MAX( ABS1( ALPHAI )*BNORM, ABS1( BETAI )*ANORM,
     $           SAFMIN )
         ACOEFF = SCALE*BETAI
         BCOEFF = SCALE*ALPHAI
         IF( LEFT ) THEN
            ACOEFF = DCONJG( ACOEFF )
            BCOEFF = DCONJG( BCOEFF )
         END IF
         CALL ZGEMV( TRANS, N, N, ACOEFF, A, LDA, E( 1, JVEC ), 1,
     $               CZERO, WORK( N*( JVEC-1 )+1 ), 1 )
         CALL ZGEMV( TRANS, N, N, -BCOEFF, B, LDA, E( 1, JVEC ), 1,
     $               CONE, WORK( N*( JVEC-1 )+1 ), 1 )
   10 CONTINUE
*
      ERRNRM = ZLANGE( 'One', N, N, WORK, N, RWORK ) / ENORM
*
*     Compute RESULT(1)
*
      RESULT( 1 ) = ERRNRM / ULP
*
*     Normalization of E:
*
      ENRMER = ZERO
      DO 30 JVEC = 1, N
         TEMP1 = ZERO
         DO 20 J = 1, N
            TEMP1 = MAX( TEMP1, ABS1( E( J, JVEC ) ) )
   20    CONTINUE
         ENRMER = MAX( ENRMER, TEMP1-ONE )
   30 CONTINUE
*
*     Compute RESULT(2) : the normalization error in E.
*
      RESULT( 2 ) = ENRMER / ( DBLE( N )*ULP )
*
      RETURN
*
*     End of ZGET52
*
      END
