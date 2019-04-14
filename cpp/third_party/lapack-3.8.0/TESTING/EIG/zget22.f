*> \brief \b ZGET22
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGET22( TRANSA, TRANSE, TRANSW, N, A, LDA, E, LDE, W,
*                          WORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANSA, TRANSE, TRANSW
*       INTEGER            LDA, LDE, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RESULT( 2 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), E( LDE, * ), W( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGET22 does an eigenvector check.
*>
*> The basic test is:
*>
*>    RESULT(1) = | A E  -  E W | / ( |A| |E| ulp )
*>
*> using the 1-norm.  It also tests the normalization of E:
*>
*>    RESULT(2) = max | m-norm(E(j)) - 1 | / ( n ulp )
*>                 j
*>
*> where E(j) is the j-th eigenvector, and m-norm is the max-norm of a
*> vector.  The max-norm of a complex n-vector x in this case is the
*> maximum of |re(x(i)| + |im(x(i)| over i = 1, ..., n.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANSA
*> \verbatim
*>          TRANSA is CHARACTER*1
*>          Specifies whether or not A is transposed.
*>          = 'N':  No transpose
*>          = 'T':  Transpose
*>          = 'C':  Conjugate transpose
*> \endverbatim
*>
*> \param[in] TRANSE
*> \verbatim
*>          TRANSE is CHARACTER*1
*>          Specifies whether or not E is transposed.
*>          = 'N':  No transpose, eigenvectors are in columns of E
*>          = 'T':  Transpose, eigenvectors are in rows of E
*>          = 'C':  Conjugate transpose, eigenvectors are in rows of E
*> \endverbatim
*>
*> \param[in] TRANSW
*> \verbatim
*>          TRANSW is CHARACTER*1
*>          Specifies whether or not W is transposed.
*>          = 'N':  No transpose
*>          = 'T':  Transpose, same as TRANSW = 'N'
*>          = 'C':  Conjugate transpose, use -WI(j) instead of WI(j)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The matrix whose eigenvectors are in E.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is COMPLEX*16 array, dimension (LDE,N)
*>          The matrix of eigenvectors. If TRANSE = 'N', the eigenvectors
*>          are stored in the columns of E, if TRANSE = 'T' or 'C', the
*>          eigenvectors are stored in the rows of E.
*> \endverbatim
*>
*> \param[in] LDE
*> \verbatim
*>          LDE is INTEGER
*>          The leading dimension of the array E.  LDE >= max(1,N).
*> \endverbatim
*>
*> \param[in] W
*> \verbatim
*>          W is COMPLEX*16 array, dimension (N)
*>          The eigenvalues of A.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N*N)
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
*>          RESULT(1) = | A E  -  E W | / ( |A| |E| ulp )
*>          RESULT(2) = max | m-norm(E(j)) - 1 | / ( n ulp )
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
      SUBROUTINE ZGET22( TRANSA, TRANSE, TRANSW, N, A, LDA, E, LDE, W,
     $                   WORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANSA, TRANSE, TRANSW
      INTEGER            LDA, LDE, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RESULT( 2 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), E( LDE, * ), W( * ), WORK( * )
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
      CHARACTER          NORMA, NORME
      INTEGER            ITRNSE, ITRNSW, J, JCOL, JOFF, JROW, JVEC
      DOUBLE PRECISION   ANORM, ENORM, ENRMAX, ENRMIN, ERRNRM, TEMP1,
     $                   ULP, UNFL
      COMPLEX*16         WTEMP
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           LSAME, DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZLASET
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, DIMAG, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Initialize RESULT (in case N=0)
*
      RESULT( 1 ) = ZERO
      RESULT( 2 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      UNFL = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Precision' )
*
      ITRNSE = 0
      ITRNSW = 0
      NORMA = 'O'
      NORME = 'O'
*
      IF( LSAME( TRANSA, 'T' ) .OR. LSAME( TRANSA, 'C' ) ) THEN
         NORMA = 'I'
      END IF
*
      IF( LSAME( TRANSE, 'T' ) ) THEN
         ITRNSE = 1
         NORME = 'I'
      ELSE IF( LSAME( TRANSE, 'C' ) ) THEN
         ITRNSE = 2
         NORME = 'I'
      END IF
*
      IF( LSAME( TRANSW, 'C' ) ) THEN
         ITRNSW = 1
      END IF
*
*     Normalization of E:
*
      ENRMIN = ONE / ULP
      ENRMAX = ZERO
      IF( ITRNSE.EQ.0 ) THEN
         DO 20 JVEC = 1, N
            TEMP1 = ZERO
            DO 10 J = 1, N
               TEMP1 = MAX( TEMP1, ABS( DBLE( E( J, JVEC ) ) )+
     $                 ABS( DIMAG( E( J, JVEC ) ) ) )
   10       CONTINUE
            ENRMIN = MIN( ENRMIN, TEMP1 )
            ENRMAX = MAX( ENRMAX, TEMP1 )
   20    CONTINUE
      ELSE
         DO 30 JVEC = 1, N
            RWORK( JVEC ) = ZERO
   30    CONTINUE
*
         DO 50 J = 1, N
            DO 40 JVEC = 1, N
               RWORK( JVEC ) = MAX( RWORK( JVEC ),
     $                         ABS( DBLE( E( JVEC, J ) ) )+
     $                         ABS( DIMAG( E( JVEC, J ) ) ) )
   40       CONTINUE
   50    CONTINUE
*
         DO 60 JVEC = 1, N
            ENRMIN = MIN( ENRMIN, RWORK( JVEC ) )
            ENRMAX = MAX( ENRMAX, RWORK( JVEC ) )
   60    CONTINUE
      END IF
*
*     Norm of A:
*
      ANORM = MAX( ZLANGE( NORMA, N, N, A, LDA, RWORK ), UNFL )
*
*     Norm of E:
*
      ENORM = MAX( ZLANGE( NORME, N, N, E, LDE, RWORK ), ULP )
*
*     Norm of error:
*
*     Error =  AE - EW
*
      CALL ZLASET( 'Full', N, N, CZERO, CZERO, WORK, N )
*
      JOFF = 0
      DO 100 JCOL = 1, N
         IF( ITRNSW.EQ.0 ) THEN
            WTEMP = W( JCOL )
         ELSE
            WTEMP = DCONJG( W( JCOL ) )
         END IF
*
         IF( ITRNSE.EQ.0 ) THEN
            DO 70 JROW = 1, N
               WORK( JOFF+JROW ) = E( JROW, JCOL )*WTEMP
   70       CONTINUE
         ELSE IF( ITRNSE.EQ.1 ) THEN
            DO 80 JROW = 1, N
               WORK( JOFF+JROW ) = E( JCOL, JROW )*WTEMP
   80       CONTINUE
         ELSE
            DO 90 JROW = 1, N
               WORK( JOFF+JROW ) = DCONJG( E( JCOL, JROW ) )*WTEMP
   90       CONTINUE
         END IF
         JOFF = JOFF + N
  100 CONTINUE
*
      CALL ZGEMM( TRANSA, TRANSE, N, N, N, CONE, A, LDA, E, LDE, -CONE,
     $            WORK, N )
*
      ERRNRM = ZLANGE( 'One', N, N, WORK, N, RWORK ) / ENORM
*
*     Compute RESULT(1) (avoiding under/overflow)
*
      IF( ANORM.GT.ERRNRM ) THEN
         RESULT( 1 ) = ( ERRNRM / ANORM ) / ULP
      ELSE
         IF( ANORM.LT.ONE ) THEN
            RESULT( 1 ) = ( MIN( ERRNRM, ANORM ) / ANORM ) / ULP
         ELSE
            RESULT( 1 ) = MIN( ERRNRM / ANORM, ONE ) / ULP
         END IF
      END IF
*
*     Compute RESULT(2) : the normalization error in E.
*
      RESULT( 2 ) = MAX( ABS( ENRMAX-ONE ), ABS( ENRMIN-ONE ) ) /
     $              ( DBLE( N )*ULP )
*
      RETURN
*
*     End of ZGET22
*
      END
