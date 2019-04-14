*> \brief \b ZSPR performs the symmetrical rank-1 update of a complex symmetric packed matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZSPR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zspr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zspr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zspr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZSPR( UPLO, N, ALPHA, X, INCX, AP )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INCX, N
*       COMPLEX*16         ALPHA
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         AP( * ), X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZSPR    performs the symmetric rank 1 operation
*>
*>    A := alpha*x*x**H + A,
*>
*> where alpha is a complex scalar, x is an n element vector and A is an
*> n by n symmetric matrix, supplied in packed form.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>           On entry, UPLO specifies whether the upper or lower
*>           triangular part of the matrix A is supplied in the packed
*>           array AP as follows:
*>
*>              UPLO = 'U' or 'u'   The upper triangular part of A is
*>                                  supplied in AP.
*>
*>              UPLO = 'L' or 'l'   The lower triangular part of A is
*>                                  supplied in AP.
*>
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           On entry, N specifies the order of the matrix A.
*>           N must be at least zero.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX*16
*>           On entry, ALPHA specifies the scalar alpha.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension at least
*>           ( 1 + ( N - 1 )*abs( INCX ) ).
*>           Before entry, the incremented array X must contain the N-
*>           element vector x.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>           On entry, INCX specifies the increment for the elements of
*>           X. INCX must not be zero.
*>           Unchanged on exit.
*> \endverbatim
*>
*> \param[in,out] AP
*> \verbatim
*>          AP is COMPLEX*16 array, dimension at least
*>           ( ( N*( N + 1 ) )/2 ).
*>           Before entry, with  UPLO = 'U' or 'u', the array AP must
*>           contain the upper triangular part of the symmetric matrix
*>           packed sequentially, column by column, so that AP( 1 )
*>           contains a( 1, 1 ), AP( 2 ) and AP( 3 ) contain a( 1, 2 )
*>           and a( 2, 2 ) respectively, and so on. On exit, the array
*>           AP is overwritten by the upper triangular part of the
*>           updated matrix.
*>           Before entry, with UPLO = 'L' or 'l', the array AP must
*>           contain the lower triangular part of the symmetric matrix
*>           packed sequentially, column by column, so that AP( 1 )
*>           contains a( 1, 1 ), AP( 2 ) and AP( 3 ) contain a( 2, 1 )
*>           and a( 3, 1 ) respectively, and so on. On exit, the array
*>           AP is overwritten by the lower triangular part of the
*>           updated matrix.
*>           Note that the imaginary parts of the diagonal elements need
*>           not be set, they are assumed to be zero, and on exit they
*>           are set to zero.
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
*> \ingroup complex16OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE ZSPR( UPLO, N, ALPHA, X, INCX, AP )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INCX, N
      COMPLEX*16         ALPHA
*     ..
*     .. Array Arguments ..
      COMPLEX*16         AP( * ), X( * )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         ZERO
      PARAMETER          ( ZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, IX, J, JX, K, KK, KX
      COMPLEX*16         TEMP
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = 1
      ELSE IF( N.LT.0 ) THEN
         INFO = 2
      ELSE IF( INCX.EQ.0 ) THEN
         INFO = 5
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZSPR  ', INFO )
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( ( N.EQ.0 ) .OR. ( ALPHA.EQ.ZERO ) )
     $   RETURN
*
*     Set the start point in X if the increment is not unity.
*
      IF( INCX.LE.0 ) THEN
         KX = 1 - ( N-1 )*INCX
      ELSE IF( INCX.NE.1 ) THEN
         KX = 1
      END IF
*
*     Start the operations. In this version the elements of the array AP
*     are accessed sequentially with one pass through AP.
*
      KK = 1
      IF( LSAME( UPLO, 'U' ) ) THEN
*
*        Form  A  when upper triangle is stored in AP.
*
         IF( INCX.EQ.1 ) THEN
            DO 20 J = 1, N
               IF( X( J ).NE.ZERO ) THEN
                  TEMP = ALPHA*X( J )
                  K = KK
                  DO 10 I = 1, J - 1
                     AP( K ) = AP( K ) + X( I )*TEMP
                     K = K + 1
   10             CONTINUE
                  AP( KK+J-1 ) = AP( KK+J-1 ) + X( J )*TEMP
               ELSE
                  AP( KK+J-1 ) = AP( KK+J-1 )
               END IF
               KK = KK + J
   20       CONTINUE
         ELSE
            JX = KX
            DO 40 J = 1, N
               IF( X( JX ).NE.ZERO ) THEN
                  TEMP = ALPHA*X( JX )
                  IX = KX
                  DO 30 K = KK, KK + J - 2
                     AP( K ) = AP( K ) + X( IX )*TEMP
                     IX = IX + INCX
   30             CONTINUE
                  AP( KK+J-1 ) = AP( KK+J-1 ) + X( JX )*TEMP
               ELSE
                  AP( KK+J-1 ) = AP( KK+J-1 )
               END IF
               JX = JX + INCX
               KK = KK + J
   40       CONTINUE
         END IF
      ELSE
*
*        Form  A  when lower triangle is stored in AP.
*
         IF( INCX.EQ.1 ) THEN
            DO 60 J = 1, N
               IF( X( J ).NE.ZERO ) THEN
                  TEMP = ALPHA*X( J )
                  AP( KK ) = AP( KK ) + TEMP*X( J )
                  K = KK + 1
                  DO 50 I = J + 1, N
                     AP( K ) = AP( K ) + X( I )*TEMP
                     K = K + 1
   50             CONTINUE
               ELSE
                  AP( KK ) = AP( KK )
               END IF
               KK = KK + N - J + 1
   60       CONTINUE
         ELSE
            JX = KX
            DO 80 J = 1, N
               IF( X( JX ).NE.ZERO ) THEN
                  TEMP = ALPHA*X( JX )
                  AP( KK ) = AP( KK ) + TEMP*X( JX )
                  IX = JX
                  DO 70 K = KK + 1, KK + N - J
                     IX = IX + INCX
                     AP( K ) = AP( K ) + X( IX )*TEMP
   70             CONTINUE
               ELSE
                  AP( KK ) = AP( KK )
               END IF
               JX = JX + INCX
               KK = KK + N - J + 1
   80       CONTINUE
         END IF
      END IF
*
      RETURN
*
*     End of ZSPR
*
      END
