*> \brief \b SLACN2 estimates the 1-norm of a square matrix, using reverse communication for evaluating matrix-vector products.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLACN2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slacn2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slacn2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slacn2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLACN2( N, V, X, ISGN, EST, KASE, ISAVE )
*
*       .. Scalar Arguments ..
*       INTEGER            KASE, N
*       REAL               EST
*       ..
*       .. Array Arguments ..
*       INTEGER            ISGN( * ), ISAVE( 3 )
*       REAL               V( * ), X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLACN2 estimates the 1-norm of a square, real matrix A.
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
*>          V is REAL array, dimension (N)
*>         On the final return, V = A*W,  where  EST = norm(V)/norm(W)
*>         (W is not returned).
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is REAL array, dimension (N)
*>         On an intermediate return, X should be overwritten by
*>               A * X,   if KASE=1,
*>               A**T * X,  if KASE=2,
*>         and SLACN2 must be re-called with all the other parameters
*>         unchanged.
*> \endverbatim
*>
*> \param[out] ISGN
*> \verbatim
*>          ISGN is INTEGER array, dimension (N)
*> \endverbatim
*>
*> \param[in,out] EST
*> \verbatim
*>          EST is REAL
*>         On entry with KASE = 1 or 2 and ISAVE(1) = 3, EST should be
*>         unchanged from the previous call to SLACN2.
*>         On exit, EST is an estimate (a lower bound) for norm(A).
*> \endverbatim
*>
*> \param[in,out] KASE
*> \verbatim
*>          KASE is INTEGER
*>         On the initial call to SLACN2, KASE should be 0.
*>         On an intermediate return, KASE will be 1 or 2, indicating
*>         whether X should be overwritten by A * X  or A**T * X.
*>         On the final return from SLACN2, KASE will again be 0.
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
*> \ingroup realOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Originally named SONEST, dated March 16, 1988.
*>
*>  This is a thread safe version of SLACON, which uses the array ISAVE
*>  in place of a SAVE statement, as follows:
*>
*>     SLACON     SLACN2
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
      SUBROUTINE SLACN2( N, V, X, ISGN, EST, KASE, ISAVE )
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
      INTEGER            ISGN( * ), ISAVE( 3 )
      REAL               V( * ), X( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            ITMAX
      PARAMETER          ( ITMAX = 5 )
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, JLAST
      REAL               ALTSGN, ESTOLD, TEMP
*     ..
*     .. External Functions ..
      INTEGER            ISAMAX
      REAL               SASUM
      EXTERNAL           ISAMAX, SASUM
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, NINT, REAL, SIGN
*     ..
*     .. Executable Statements ..
*
      IF( KASE.EQ.0 ) THEN
         DO 10 I = 1, N
            X( I ) = ONE / REAL( N )
   10    CONTINUE
         KASE = 1
         ISAVE( 1 ) = 1
         RETURN
      END IF
*
      GO TO ( 20, 40, 70, 110, 140 )ISAVE( 1 )
*
*     ................ ENTRY   (ISAVE( 1 ) = 1)
*     FIRST ITERATION.  X HAS BEEN OVERWRITTEN BY A*X.
*
   20 CONTINUE
      IF( N.EQ.1 ) THEN
         V( 1 ) = X( 1 )
         EST = ABS( V( 1 ) )
*        ... QUIT
         GO TO 150
      END IF
      EST = SASUM( N, X, 1 )
*
      DO 30 I = 1, N
         X( I ) = SIGN( ONE, X( I ) )
         ISGN( I ) = NINT( X( I ) )
   30 CONTINUE
      KASE = 2
      ISAVE( 1 ) = 2
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 2)
*     FIRST ITERATION.  X HAS BEEN OVERWRITTEN BY TRANSPOSE(A)*X.
*
   40 CONTINUE
      ISAVE( 2 ) = ISAMAX( N, X, 1 )
      ISAVE( 3 ) = 2
*
*     MAIN LOOP - ITERATIONS 2,3,...,ITMAX.
*
   50 CONTINUE
      DO 60 I = 1, N
         X( I ) = ZERO
   60 CONTINUE
      X( ISAVE( 2 ) ) = ONE
      KASE = 1
      ISAVE( 1 ) = 3
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 3)
*     X HAS BEEN OVERWRITTEN BY A*X.
*
   70 CONTINUE
      CALL SCOPY( N, X, 1, V, 1 )
      ESTOLD = EST
      EST = SASUM( N, V, 1 )
      DO 80 I = 1, N
         IF( NINT( SIGN( ONE, X( I ) ) ).NE.ISGN( I ) )
     $      GO TO 90
   80 CONTINUE
*     REPEATED SIGN VECTOR DETECTED, HENCE ALGORITHM HAS CONVERGED.
      GO TO 120
*
   90 CONTINUE
*     TEST FOR CYCLING.
      IF( EST.LE.ESTOLD )
     $   GO TO 120
*
      DO 100 I = 1, N
         X( I ) = SIGN( ONE, X( I ) )
         ISGN( I ) = NINT( X( I ) )
  100 CONTINUE
      KASE = 2
      ISAVE( 1 ) = 4
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 4)
*     X HAS BEEN OVERWRITTEN BY TRANSPOSE(A)*X.
*
  110 CONTINUE
      JLAST = ISAVE( 2 )
      ISAVE( 2 ) = ISAMAX( N, X, 1 )
      IF( ( X( JLAST ).NE.ABS( X( ISAVE( 2 ) ) ) ) .AND.
     $    ( ISAVE( 3 ).LT.ITMAX ) ) THEN
         ISAVE( 3 ) = ISAVE( 3 ) + 1
         GO TO 50
      END IF
*
*     ITERATION COMPLETE.  FINAL STAGE.
*
  120 CONTINUE
      ALTSGN = ONE
      DO 130 I = 1, N
         X( I ) = ALTSGN*( ONE+REAL( I-1 ) / REAL( N-1 ) )
         ALTSGN = -ALTSGN
  130 CONTINUE
      KASE = 1
      ISAVE( 1 ) = 5
      RETURN
*
*     ................ ENTRY   (ISAVE( 1 ) = 5)
*     X HAS BEEN OVERWRITTEN BY A*X.
*
  140 CONTINUE
      TEMP = TWO*( SASUM( N, X, 1 ) / REAL( 3*N ) )
      IF( TEMP.GT.EST ) THEN
         CALL SCOPY( N, X, 1, V, 1 )
         EST = TEMP
      END IF
*
  150 CONTINUE
      KASE = 0
      RETURN
*
*     End of SLACN2
*
      END
