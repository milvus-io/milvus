*> \brief \b CTPSV
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CTPSV(UPLO,TRANS,DIAG,N,AP,X,INCX)
*
*       .. Scalar Arguments ..
*       INTEGER INCX,N
*       CHARACTER DIAG,TRANS,UPLO
*       ..
*       .. Array Arguments ..
*       COMPLEX AP(*),X(*)
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CTPSV  solves one of the systems of equations
*>
*>    A*x = b,   or   A**T*x = b,   or   A**H*x = b,
*>
*> where b and x are n element vectors and A is an n by n unit, or
*> non-unit, upper or lower triangular matrix, supplied in packed form.
*>
*> No test for singularity or near-singularity is included in this
*> routine. Such tests must be performed before calling this routine.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>           On entry, UPLO specifies whether the matrix is an upper or
*>           lower triangular matrix as follows:
*>
*>              UPLO = 'U' or 'u'   A is an upper triangular matrix.
*>
*>              UPLO = 'L' or 'l'   A is a lower triangular matrix.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>           On entry, TRANS specifies the equations to be solved as
*>           follows:
*>
*>              TRANS = 'N' or 'n'   A*x = b.
*>
*>              TRANS = 'T' or 't'   A**T*x = b.
*>
*>              TRANS = 'C' or 'c'   A**H*x = b.
*> \endverbatim
*>
*> \param[in] DIAG
*> \verbatim
*>          DIAG is CHARACTER*1
*>           On entry, DIAG specifies whether or not A is unit
*>           triangular as follows:
*>
*>              DIAG = 'U' or 'u'   A is assumed to be unit triangular.
*>
*>              DIAG = 'N' or 'n'   A is not assumed to be unit
*>                                  triangular.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           On entry, N specifies the order of the matrix A.
*>           N must be at least zero.
*> \endverbatim
*>
*> \param[in] AP
*> \verbatim
*>          AP is COMPLEX array, dimension at least
*>           ( ( n*( n + 1 ) )/2 ).
*>           Before entry with  UPLO = 'U' or 'u', the array AP must
*>           contain the upper triangular matrix packed sequentially,
*>           column by column, so that AP( 1 ) contains a( 1, 1 ),
*>           AP( 2 ) and AP( 3 ) contain a( 1, 2 ) and a( 2, 2 )
*>           respectively, and so on.
*>           Before entry with UPLO = 'L' or 'l', the array AP must
*>           contain the lower triangular matrix packed sequentially,
*>           column by column, so that AP( 1 ) contains a( 1, 1 ),
*>           AP( 2 ) and AP( 3 ) contain a( 2, 1 ) and a( 3, 1 )
*>           respectively, and so on.
*>           Note that when  DIAG = 'U' or 'u', the diagonal elements of
*>           A are not referenced, but are assumed to be unity.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX array, dimension at least
*>           ( 1 + ( n - 1 )*abs( INCX ) ).
*>           Before entry, the incremented array X must contain the n
*>           element right-hand side vector b. On exit, X is overwritten
*>           with the solution vector x.
*> \endverbatim
*>
*> \param[in] INCX
*> \verbatim
*>          INCX is INTEGER
*>           On entry, INCX specifies the increment for the elements of
*>           X. INCX must not be zero.
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
*> \ingroup complex_blas_level2
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Level 2 Blas routine.
*>
*>  -- Written on 22-October-1986.
*>     Jack Dongarra, Argonne National Lab.
*>     Jeremy Du Croz, Nag Central Office.
*>     Sven Hammarling, Nag Central Office.
*>     Richard Hanson, Sandia National Labs.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE CTPSV(UPLO,TRANS,DIAG,N,AP,X,INCX)
*
*  -- Reference BLAS level2 routine (version 3.7.0) --
*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER INCX,N
      CHARACTER DIAG,TRANS,UPLO
*     ..
*     .. Array Arguments ..
      COMPLEX AP(*),X(*)
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX ZERO
      PARAMETER (ZERO= (0.0E+0,0.0E+0))
*     ..
*     .. Local Scalars ..
      COMPLEX TEMP
      INTEGER I,INFO,IX,J,JX,K,KK,KX
      LOGICAL NOCONJ,NOUNIT
*     ..
*     .. External Functions ..
      LOGICAL LSAME
      EXTERNAL LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC CONJG
*     ..
*
*     Test the input parameters.
*
      INFO = 0
      IF (.NOT.LSAME(UPLO,'U') .AND. .NOT.LSAME(UPLO,'L')) THEN
          INFO = 1
      ELSE IF (.NOT.LSAME(TRANS,'N') .AND. .NOT.LSAME(TRANS,'T') .AND.
     +         .NOT.LSAME(TRANS,'C')) THEN
          INFO = 2
      ELSE IF (.NOT.LSAME(DIAG,'U') .AND. .NOT.LSAME(DIAG,'N')) THEN
          INFO = 3
      ELSE IF (N.LT.0) THEN
          INFO = 4
      ELSE IF (INCX.EQ.0) THEN
          INFO = 7
      END IF
      IF (INFO.NE.0) THEN
          CALL XERBLA('CTPSV ',INFO)
          RETURN
      END IF
*
*     Quick return if possible.
*
      IF (N.EQ.0) RETURN
*
      NOCONJ = LSAME(TRANS,'T')
      NOUNIT = LSAME(DIAG,'N')
*
*     Set up the start point in X if the increment is not unity. This
*     will be  ( N - 1 )*INCX  too small for descending loops.
*
      IF (INCX.LE.0) THEN
          KX = 1 - (N-1)*INCX
      ELSE IF (INCX.NE.1) THEN
          KX = 1
      END IF
*
*     Start the operations. In this version the elements of AP are
*     accessed sequentially with one pass through AP.
*
      IF (LSAME(TRANS,'N')) THEN
*
*        Form  x := inv( A )*x.
*
          IF (LSAME(UPLO,'U')) THEN
              KK = (N* (N+1))/2
              IF (INCX.EQ.1) THEN
                  DO 20 J = N,1,-1
                      IF (X(J).NE.ZERO) THEN
                          IF (NOUNIT) X(J) = X(J)/AP(KK)
                          TEMP = X(J)
                          K = KK - 1
                          DO 10 I = J - 1,1,-1
                              X(I) = X(I) - TEMP*AP(K)
                              K = K - 1
   10                     CONTINUE
                      END IF
                      KK = KK - J
   20             CONTINUE
              ELSE
                  JX = KX + (N-1)*INCX
                  DO 40 J = N,1,-1
                      IF (X(JX).NE.ZERO) THEN
                          IF (NOUNIT) X(JX) = X(JX)/AP(KK)
                          TEMP = X(JX)
                          IX = JX
                          DO 30 K = KK - 1,KK - J + 1,-1
                              IX = IX - INCX
                              X(IX) = X(IX) - TEMP*AP(K)
   30                     CONTINUE
                      END IF
                      JX = JX - INCX
                      KK = KK - J
   40             CONTINUE
              END IF
          ELSE
              KK = 1
              IF (INCX.EQ.1) THEN
                  DO 60 J = 1,N
                      IF (X(J).NE.ZERO) THEN
                          IF (NOUNIT) X(J) = X(J)/AP(KK)
                          TEMP = X(J)
                          K = KK + 1
                          DO 50 I = J + 1,N
                              X(I) = X(I) - TEMP*AP(K)
                              K = K + 1
   50                     CONTINUE
                      END IF
                      KK = KK + (N-J+1)
   60             CONTINUE
              ELSE
                  JX = KX
                  DO 80 J = 1,N
                      IF (X(JX).NE.ZERO) THEN
                          IF (NOUNIT) X(JX) = X(JX)/AP(KK)
                          TEMP = X(JX)
                          IX = JX
                          DO 70 K = KK + 1,KK + N - J
                              IX = IX + INCX
                              X(IX) = X(IX) - TEMP*AP(K)
   70                     CONTINUE
                      END IF
                      JX = JX + INCX
                      KK = KK + (N-J+1)
   80             CONTINUE
              END IF
          END IF
      ELSE
*
*        Form  x := inv( A**T )*x  or  x := inv( A**H )*x.
*
          IF (LSAME(UPLO,'U')) THEN
              KK = 1
              IF (INCX.EQ.1) THEN
                  DO 110 J = 1,N
                      TEMP = X(J)
                      K = KK
                      IF (NOCONJ) THEN
                          DO 90 I = 1,J - 1
                              TEMP = TEMP - AP(K)*X(I)
                              K = K + 1
   90                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/AP(KK+J-1)
                      ELSE
                          DO 100 I = 1,J - 1
                              TEMP = TEMP - CONJG(AP(K))*X(I)
                              K = K + 1
  100                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/CONJG(AP(KK+J-1))
                      END IF
                      X(J) = TEMP
                      KK = KK + J
  110             CONTINUE
              ELSE
                  JX = KX
                  DO 140 J = 1,N
                      TEMP = X(JX)
                      IX = KX
                      IF (NOCONJ) THEN
                          DO 120 K = KK,KK + J - 2
                              TEMP = TEMP - AP(K)*X(IX)
                              IX = IX + INCX
  120                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/AP(KK+J-1)
                      ELSE
                          DO 130 K = KK,KK + J - 2
                              TEMP = TEMP - CONJG(AP(K))*X(IX)
                              IX = IX + INCX
  130                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/CONJG(AP(KK+J-1))
                      END IF
                      X(JX) = TEMP
                      JX = JX + INCX
                      KK = KK + J
  140             CONTINUE
              END IF
          ELSE
              KK = (N* (N+1))/2
              IF (INCX.EQ.1) THEN
                  DO 170 J = N,1,-1
                      TEMP = X(J)
                      K = KK
                      IF (NOCONJ) THEN
                          DO 150 I = N,J + 1,-1
                              TEMP = TEMP - AP(K)*X(I)
                              K = K - 1
  150                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/AP(KK-N+J)
                      ELSE
                          DO 160 I = N,J + 1,-1
                              TEMP = TEMP - CONJG(AP(K))*X(I)
                              K = K - 1
  160                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/CONJG(AP(KK-N+J))
                      END IF
                      X(J) = TEMP
                      KK = KK - (N-J+1)
  170             CONTINUE
              ELSE
                  KX = KX + (N-1)*INCX
                  JX = KX
                  DO 200 J = N,1,-1
                      TEMP = X(JX)
                      IX = KX
                      IF (NOCONJ) THEN
                          DO 180 K = KK,KK - (N- (J+1)),-1
                              TEMP = TEMP - AP(K)*X(IX)
                              IX = IX - INCX
  180                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/AP(KK-N+J)
                      ELSE
                          DO 190 K = KK,KK - (N- (J+1)),-1
                              TEMP = TEMP - CONJG(AP(K))*X(IX)
                              IX = IX - INCX
  190                     CONTINUE
                          IF (NOUNIT) TEMP = TEMP/CONJG(AP(KK-N+J))
                      END IF
                      X(JX) = TEMP
                      JX = JX - INCX
                      KK = KK - (N-J+1)
  200             CONTINUE
              END IF
          END IF
      END IF
*
      RETURN
*
*     End of CTPSV .
*
      END
