*> \brief \b CLACN2 estimates the 1-norm of a square matrix, using reverse communication for evaluating matrix-vector products.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLACN2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clacn2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clacn2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clacn2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLACN2( N, V, X, EST, KASE, ISAVE )
*
*       .. Scalar Arguments ..
*       INTEGER            KASE, N
*       REAL               EST
*       ..
*       .. Array Arguments ..
*       INTEGER            ISAVE( 3 )
*       COMPLEX            V( * ), X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLACN2 estimates the 1-norm of a square, complex matrix A.
*> Reverse communication is used for evaluating matrix-vector products.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         The order of the matrix.  N >= 1.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is COMPLEX array, dimension (N)
*>         On the final return, V = A*W,  where  EST = norm(V)/norm(W)
*>         (W is not returned).
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX array, dimension (N)
*>         On an intermediate return, X should be overwritten by
*>               A * X,   if KASE=1,
*>               A**H * X,  if KASE=2,
*>         where A**H is the conjugate transpose of A, and CLACN2 must be
*>         re-called with all the other parameters unchanged.
*> \endverbatim
*>
*> \param[in,out] EST
*> \verbatim
*>          EST is REAL
*>         On entry with KASE = 1 or 2 and ISAVE(1) = 3, EST should be
*>         unchanged from the previous call to CLACN2.
*>         On exit, EST is an estimate (a lower bound) for norm(A).
*> \endverbatim
*>
*> \param[in,out] KASE
*> \verbatim
*>          KASE is INTEGER
*>         On the initial call to CLACN2, KASE should be 0.
*>         On an intermediate return, KASE will be 1 or 2, indicating
*>         whether X should be overwritten by A * X  or A**H * X.
*>         On the final return from CLACN2, KASE will again be 0.
*> \endverbatim
*>
*> \param[in,out] ISAVE
*> \verbatim
*>          ISAVE is INTEGER array, dimension (3)
*>         ISAVE is used to save variables between calls to SLACN2
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
*> \ingroup complexOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Originally named CONEST, dated March 16, 1988.
*>
*>  Last modified:  April, 1999
*>
*>  This is a thread safe version of CLACON, which uses the array ISAVE
*>  in place of a SAVE statement, as follows:
*>
*>     CLACON     CLACN2
*>      JUMP     ISAVE(1)
*>      J        ISAVE(2)
*>      ITER     ISAVE(3)
*> \endverbatim
*
*> \par Contributors:
*  ==================
*>
*>     Nick Higham, University of Manchester
*
*> \par References:
*  ================
*>
*>  N.J. Higham, "FORTRAN codes for estimating the one-norm of
*>  a real or complex matrix, with applications to condition estimation",
*>  ACM Trans. Math. Soft., vol. 14, no. 4, pp. 381-396, December 1988.
*>
*  =====================================================================
      SUBROUTINE CLACN2( N, V, X, EST, KASE, ISAVE )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KASE, N
      REAL               EST
*     ..
*     .. Array Arguments ..
      INTEGER            ISAVE( 3 )
      COMPLEX            V( * ), X( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER              ITMAX
      PARAMETER          ( ITMAX = 5 )
      REAL                 ONE,         TWO
      PARAMETER          ( ONE = 1.0E0, TWO = 2.0E0 )
      COMPLEX              CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E0, 0.0E0 ),
     $                            CONE = ( 1.0E0, 0.0E0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, JLAST
      REAL               ABSXI, ALTSGN, ESTOLD, SAFMIN, TEMP
*     ..
*     .. External Functions ..
      INTEGER            ICMAX1
      REAL               SCSUM1, SLAMCH
      EXTERNAL           ICMAX1, SCSUM1, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CCOPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, CMPLX, REAL
*     ..
*     .. Executable Statements ..
*
      SAFMIN = SLAMCH( 'Safe minimum' )
      IF( KASE.EQ.0 ) THEN
         DO 10 I = 1, N
            X( I ) = CMPLX( ONE / REAL( N ) )
   10    CONTINUE
         KASE = 1
         ISAVE( 1 ) = 1
         RETURN
      END IF
*
      GO TO ( 20, 40, 70, 90, 120 )ISAVE( 1 )
*
*     ................ ENTRY   (ISAVE( 1 ) = 1)
*     FIRST ITERATION.  X HAS BEEN OVERWRITTEN BY A*X.
*
   20 CONTINUE
      IF( N.EQ.1 ) THEN
         V( 1 ) = X( 1 )
         EST = ABS( V( 1 ) )
*        ... QUIT
         GO TO 130
      END IF
      EST = SCSUM1( N, X, 1 )
*
      DO 30 I = 1, N
         ABSXI = ABS( X( I ) )
         IF( ABSXI.GT.SAFMIN ) THEN
            X( I ) = CMPLX( REAL( X( I ) ) / ABSXI,
     $               AIMAG( X( I ) ) / ABSXI )
         ELSE
            X( I ) = CONE
         END IF
   30 CONTINUE
      KASE = 2
      ISAVE( 1 ) = 2
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 2)
*     FIRST ITERATION.  X HAS BEEN OVERWRITTEN BY CTRANS(A)*X.
*
   40 CONTINUE
      ISAVE( 2 ) = ICMAX1( N, X, 1 )
      ISAVE( 3 ) = 2
*
*     MAIN LOOP - ITERATIONS 2,3,...,ITMAX.
*
   50 CONTINUE
      DO 60 I = 1, N
         X( I ) = CZERO
   60 CONTINUE
      X( ISAVE( 2 ) ) = CONE
      KASE = 1
      ISAVE( 1 ) = 3
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 3)
*     X HAS BEEN OVERWRITTEN BY A*X.
*
   70 CONTINUE
      CALL CCOPY( N, X, 1, V, 1 )
      ESTOLD = EST
      EST = SCSUM1( N, V, 1 )
*
*     TEST FOR CYCLING.
      IF( EST.LE.ESTOLD )
     $   GO TO 100
*
      DO 80 I = 1, N
         ABSXI = ABS( X( I ) )
         IF( ABSXI.GT.SAFMIN ) THEN
            X( I ) = CMPLX( REAL( X( I ) ) / ABSXI,
     $               AIMAG( X( I ) ) / ABSXI )
         ELSE
            X( I ) = CONE
         END IF
   80 CONTINUE
      KASE = 2
      ISAVE( 1 ) = 4
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 4)
*     X HAS BEEN OVERWRITTEN BY CTRANS(A)*X.
*
   90 CONTINUE
      JLAST = ISAVE( 2 )
      ISAVE( 2 ) = ICMAX1( N, X, 1 )
      IF( ( ABS( X( JLAST ) ).NE.ABS( X( ISAVE( 2 ) ) ) ) .AND.
     $    ( ISAVE( 3 ).LT.ITMAX ) ) THEN
         ISAVE( 3 ) = ISAVE( 3 ) + 1
         GO TO 50
      END IF
*
*     ITERATION COMPLETE.  FINAL STAGE.
*
  100 CONTINUE
      ALTSGN = ONE
      DO 110 I = 1, N
         X( I ) = CMPLX( ALTSGN*( ONE + REAL( I-1 ) / REAL( N-1 ) ) )
         ALTSGN = -ALTSGN
  110 CONTINUE
      KASE = 1
      ISAVE( 1 ) = 5
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 5)
*     X HAS BEEN OVERWRITTEN BY A*X.
*
  120 CONTINUE
      TEMP = TWO*( SCSUM1( N, X, 1 ) / REAL( 3*N ) )
      IF( TEMP.GT.EST ) THEN
         CALL CCOPY( N, X, 1, V, 1 )
         EST = TEMP
      END IF
*
  130 CONTINUE
      KASE = 0
      RETURN
*
*     End of CLACN2
*
      END
