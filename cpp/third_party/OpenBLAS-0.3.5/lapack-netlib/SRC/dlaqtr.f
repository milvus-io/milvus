*> \brief \b DLAQTR solves a real quasi-triangular system of equations, or a complex quasi-triangular system of special form, in real arithmetic.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAQTR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqtr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqtr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqtr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAQTR( LTRAN, LREAL, N, T, LDT, B, W, SCALE, X, WORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       LOGICAL            LREAL, LTRAN
*       INTEGER            INFO, LDT, N
*       DOUBLE PRECISION   SCALE, W
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   B( * ), T( LDT, * ), WORK( * ), X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLAQTR solves the real quasi-triangular system
*>
*>              op(T)*p = scale*c,               if LREAL = .TRUE.
*>
*> or the complex quasi-triangular systems
*>
*>            op(T + iB)*(p+iq) = scale*(c+id),  if LREAL = .FALSE.
*>
*> in real arithmetic, where T is upper quasi-triangular.
*> If LREAL = .FALSE., then the first diagonal block of T must be
*> 1 by 1, B is the specially structured matrix
*>
*>                B = [ b(1) b(2) ... b(n) ]
*>                    [       w            ]
*>                    [           w        ]
*>                    [              .     ]
*>                    [                 w  ]
*>
*> op(A) = A or A**T, A**T denotes the transpose of
*> matrix A.
*>
*> On input, X = [ c ].  On output, X = [ p ].
*>               [ d ]                  [ q ]
*>
*> This subroutine is designed for the condition number estimation
*> in routine DTRSNA.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] LTRAN
*> \verbatim
*>          LTRAN is LOGICAL
*>          On entry, LTRAN specifies the option of conjugate transpose:
*>             = .FALSE.,    op(T+i*B) = T+i*B,
*>             = .TRUE.,     op(T+i*B) = (T+i*B)**T.
*> \endverbatim
*>
*> \param[in] LREAL
*> \verbatim
*>          LREAL is LOGICAL
*>          On entry, LREAL specifies the input matrix structure:
*>             = .FALSE.,    the input is complex
*>             = .TRUE.,     the input is real
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          On entry, N specifies the order of T+i*B. N >= 0.
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is DOUBLE PRECISION array, dimension (LDT,N)
*>          On entry, T contains a matrix in Schur canonical form.
*>          If LREAL = .FALSE., then the first diagonal block of T mu
*>          be 1 by 1.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the matrix T. LDT >= max(1,N).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (N)
*>          On entry, B contains the elements to form the matrix
*>          B as described above.
*>          If LREAL = .TRUE., B is not referenced.
*> \endverbatim
*>
*> \param[in] W
*> \verbatim
*>          W is DOUBLE PRECISION
*>          On entry, W is the diagonal element of the matrix B.
*>          If LREAL = .TRUE., W is not referenced.
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is DOUBLE PRECISION
*>          On exit, SCALE is the scale factor.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (2*N)
*>          On entry, X contains the right hand side of the system.
*>          On exit, X is overwritten by the solution.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          On exit, INFO is set to
*>             0: successful exit.
*>               1: the some diagonal 1 by 1 block has been perturbed by
*>                  a small number SMIN to keep nonsingularity.
*>               2: the some diagonal 2 by 2 block has been perturbed by
*>                  a small number in DLALN2 to keep nonsingularity.
*>          NOTE: In the interests of speed, this routine does not
*>                check the inputs for errors.
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
      SUBROUTINE DLAQTR( LTRAN, LREAL, N, T, LDT, B, W, SCALE, X, WORK,
     $                   INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            LREAL, LTRAN
      INTEGER            INFO, LDT, N
      DOUBLE PRECISION   SCALE, W
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   B( * ), T( LDT, * ), WORK( * ), X( * )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRAN
      INTEGER            I, IERR, J, J1, J2, JNEXT, K, N1, N2
      DOUBLE PRECISION   BIGNUM, EPS, REC, SCALOC, SI, SMIN, SMINW,
     $                   SMLNUM, SR, TJJ, TMP, XJ, XMAX, XNORM, Z
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   D( 2, 2 ), V( 2, 2 )
*     ..
*     .. External Functions ..
      INTEGER            IDAMAX
      DOUBLE PRECISION   DASUM, DDOT, DLAMCH, DLANGE
      EXTERNAL           IDAMAX, DASUM, DDOT, DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DLADIV, DLALN2, DSCAL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
*     Do not test the input parameters for errors
*
      NOTRAN = .NOT.LTRAN
      INFO = 0
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Set constants to control overflow
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
*
      XNORM = DLANGE( 'M', N, N, T, LDT, D )
      IF( .NOT.LREAL )
     $   XNORM = MAX( XNORM, ABS( W ), DLANGE( 'M', N, 1, B, N, D ) )
      SMIN = MAX( SMLNUM, EPS*XNORM )
*
*     Compute 1-norm of each column of strictly upper triangular
*     part of T to control overflow in triangular solver.
*
      WORK( 1 ) = ZERO
      DO 10 J = 2, N
         WORK( J ) = DASUM( J-1, T( 1, J ), 1 )
   10 CONTINUE
*
      IF( .NOT.LREAL ) THEN
         DO 20 I = 2, N
            WORK( I ) = WORK( I ) + ABS( B( I ) )
   20    CONTINUE
      END IF
*
      N2 = 2*N
      N1 = N
      IF( .NOT.LREAL )
     $   N1 = N2
      K = IDAMAX( N1, X, 1 )
      XMAX = ABS( X( K ) )
      SCALE = ONE
*
      IF( XMAX.GT.BIGNUM ) THEN
         SCALE = BIGNUM / XMAX
         CALL DSCAL( N1, SCALE, X, 1 )
         XMAX = BIGNUM
      END IF
*
      IF( LREAL ) THEN
*
         IF( NOTRAN ) THEN
*
*           Solve T*p = scale*c
*
            JNEXT = N
            DO 30 J = N, 1, -1
               IF( J.GT.JNEXT )
     $            GO TO 30
               J1 = J
               J2 = J
               JNEXT = J - 1
               IF( J.GT.1 ) THEN
                  IF( T( J, J-1 ).NE.ZERO ) THEN
                     J1 = J - 1
                     JNEXT = J - 2
                  END IF
               END IF
*
               IF( J1.EQ.J2 ) THEN
*
*                 Meet 1 by 1 diagonal block
*
*                 Scale to avoid overflow when computing
*                     x(j) = b(j)/T(j,j)
*
                  XJ = ABS( X( J1 ) )
                  TJJ = ABS( T( J1, J1 ) )
                  TMP = T( J1, J1 )
                  IF( TJJ.LT.SMIN ) THEN
                     TMP = SMIN
                     TJJ = SMIN
                     INFO = 1
                  END IF
*
                  IF( XJ.EQ.ZERO )
     $               GO TO 30
*
                  IF( TJJ.LT.ONE ) THEN
                     IF( XJ.GT.BIGNUM*TJJ ) THEN
                        REC = ONE / XJ
                        CALL DSCAL( N, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
                  X( J1 ) = X( J1 ) / TMP
                  XJ = ABS( X( J1 ) )
*
*                 Scale x if necessary to avoid overflow when adding a
*                 multiple of column j1 of T.
*
                  IF( XJ.GT.ONE ) THEN
                     REC = ONE / XJ
                     IF( WORK( J1 ).GT.( BIGNUM-XMAX )*REC ) THEN
                        CALL DSCAL( N, REC, X, 1 )
                        SCALE = SCALE*REC
                     END IF
                  END IF
                  IF( J1.GT.1 ) THEN
                     CALL DAXPY( J1-1, -X( J1 ), T( 1, J1 ), 1, X, 1 )
                     K = IDAMAX( J1-1, X, 1 )
                     XMAX = ABS( X( K ) )
                  END IF
*
               ELSE
*
*                 Meet 2 by 2 diagonal block
*
*                 Call 2 by 2 linear system solve, to take
*                 care of possible overflow by scaling factor.
*
                  D( 1, 1 ) = X( J1 )
                  D( 2, 1 ) = X( J2 )
                  CALL DLALN2( .FALSE., 2, 1, SMIN, ONE, T( J1, J1 ),
     $                         LDT, ONE, ONE, D, 2, ZERO, ZERO, V, 2,
     $                         SCALOC, XNORM, IERR )
                  IF( IERR.NE.0 )
     $               INFO = 2
*
                  IF( SCALOC.NE.ONE ) THEN
                     CALL DSCAL( N, SCALOC, X, 1 )
                     SCALE = SCALE*SCALOC
                  END IF
                  X( J1 ) = V( 1, 1 )
                  X( J2 ) = V( 2, 1 )
*
*                 Scale V(1,1) (= X(J1)) and/or V(2,1) (=X(J2))
*                 to avoid overflow in updating right-hand side.
*
                  XJ = MAX( ABS( V( 1, 1 ) ), ABS( V( 2, 1 ) ) )
                  IF( XJ.GT.ONE ) THEN
                     REC = ONE / XJ
                     IF( MAX( WORK( J1 ), WORK( J2 ) ).GT.
     $                   ( BIGNUM-XMAX )*REC ) THEN
                        CALL DSCAL( N, REC, X, 1 )
                        SCALE = SCALE*REC
                     END IF
                  END IF
*
*                 Update right-hand side
*
                  IF( J1.GT.1 ) THEN
                     CALL DAXPY( J1-1, -X( J1 ), T( 1, J1 ), 1, X, 1 )
                     CALL DAXPY( J1-1, -X( J2 ), T( 1, J2 ), 1, X, 1 )
                     K = IDAMAX( J1-1, X, 1 )
                     XMAX = ABS( X( K ) )
                  END IF
*
               END IF
*
   30       CONTINUE
*
         ELSE
*
*           Solve T**T*p = scale*c
*
            JNEXT = 1
            DO 40 J = 1, N
               IF( J.LT.JNEXT )
     $            GO TO 40
               J1 = J
               J2 = J
               JNEXT = J + 1
               IF( J.LT.N ) THEN
                  IF( T( J+1, J ).NE.ZERO ) THEN
                     J2 = J + 1
                     JNEXT = J + 2
                  END IF
               END IF
*
               IF( J1.EQ.J2 ) THEN
*
*                 1 by 1 diagonal block
*
*                 Scale if necessary to avoid overflow in forming the
*                 right-hand side element by inner product.
*
                  XJ = ABS( X( J1 ) )
                  IF( XMAX.GT.ONE ) THEN
                     REC = ONE / XMAX
                     IF( WORK( J1 ).GT.( BIGNUM-XJ )*REC ) THEN
                        CALL DSCAL( N, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
*
                  X( J1 ) = X( J1 ) - DDOT( J1-1, T( 1, J1 ), 1, X, 1 )
*
                  XJ = ABS( X( J1 ) )
                  TJJ = ABS( T( J1, J1 ) )
                  TMP = T( J1, J1 )
                  IF( TJJ.LT.SMIN ) THEN
                     TMP = SMIN
                     TJJ = SMIN
                     INFO = 1
                  END IF
*
                  IF( TJJ.LT.ONE ) THEN
                     IF( XJ.GT.BIGNUM*TJJ ) THEN
                        REC = ONE / XJ
                        CALL DSCAL( N, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
                  X( J1 ) = X( J1 ) / TMP
                  XMAX = MAX( XMAX, ABS( X( J1 ) ) )
*
               ELSE
*
*                 2 by 2 diagonal block
*
*                 Scale if necessary to avoid overflow in forming the
*                 right-hand side elements by inner product.
*
                  XJ = MAX( ABS( X( J1 ) ), ABS( X( J2 ) ) )
                  IF( XMAX.GT.ONE ) THEN
                     REC = ONE / XMAX
                     IF( MAX( WORK( J2 ), WORK( J1 ) ).GT.( BIGNUM-XJ )*
     $                   REC ) THEN
                        CALL DSCAL( N, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
*
                  D( 1, 1 ) = X( J1 ) - DDOT( J1-1, T( 1, J1 ), 1, X,
     $                        1 )
                  D( 2, 1 ) = X( J2 ) - DDOT( J1-1, T( 1, J2 ), 1, X,
     $                        1 )
*
                  CALL DLALN2( .TRUE., 2, 1, SMIN, ONE, T( J1, J1 ),
     $                         LDT, ONE, ONE, D, 2, ZERO, ZERO, V, 2,
     $                         SCALOC, XNORM, IERR )
                  IF( IERR.NE.0 )
     $               INFO = 2
*
                  IF( SCALOC.NE.ONE ) THEN
                     CALL DSCAL( N, SCALOC, X, 1 )
                     SCALE = SCALE*SCALOC
                  END IF
                  X( J1 ) = V( 1, 1 )
                  X( J2 ) = V( 2, 1 )
                  XMAX = MAX( ABS( X( J1 ) ), ABS( X( J2 ) ), XMAX )
*
               END IF
   40       CONTINUE
         END IF
*
      ELSE
*
         SMINW = MAX( EPS*ABS( W ), SMIN )
         IF( NOTRAN ) THEN
*
*           Solve (T + iB)*(p+iq) = c+id
*
            JNEXT = N
            DO 70 J = N, 1, -1
               IF( J.GT.JNEXT )
     $            GO TO 70
               J1 = J
               J2 = J
               JNEXT = J - 1
               IF( J.GT.1 ) THEN
                  IF( T( J, J-1 ).NE.ZERO ) THEN
                     J1 = J - 1
                     JNEXT = J - 2
                  END IF
               END IF
*
               IF( J1.EQ.J2 ) THEN
*
*                 1 by 1 diagonal block
*
*                 Scale if necessary to avoid overflow in division
*
                  Z = W
                  IF( J1.EQ.1 )
     $               Z = B( 1 )
                  XJ = ABS( X( J1 ) ) + ABS( X( N+J1 ) )
                  TJJ = ABS( T( J1, J1 ) ) + ABS( Z )
                  TMP = T( J1, J1 )
                  IF( TJJ.LT.SMINW ) THEN
                     TMP = SMINW
                     TJJ = SMINW
                     INFO = 1
                  END IF
*
                  IF( XJ.EQ.ZERO )
     $               GO TO 70
*
                  IF( TJJ.LT.ONE ) THEN
                     IF( XJ.GT.BIGNUM*TJJ ) THEN
                        REC = ONE / XJ
                        CALL DSCAL( N2, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
                  CALL DLADIV( X( J1 ), X( N+J1 ), TMP, Z, SR, SI )
                  X( J1 ) = SR
                  X( N+J1 ) = SI
                  XJ = ABS( X( J1 ) ) + ABS( X( N+J1 ) )
*
*                 Scale x if necessary to avoid overflow when adding a
*                 multiple of column j1 of T.
*
                  IF( XJ.GT.ONE ) THEN
                     REC = ONE / XJ
                     IF( WORK( J1 ).GT.( BIGNUM-XMAX )*REC ) THEN
                        CALL DSCAL( N2, REC, X, 1 )
                        SCALE = SCALE*REC
                     END IF
                  END IF
*
                  IF( J1.GT.1 ) THEN
                     CALL DAXPY( J1-1, -X( J1 ), T( 1, J1 ), 1, X, 1 )
                     CALL DAXPY( J1-1, -X( N+J1 ), T( 1, J1 ), 1,
     $                           X( N+1 ), 1 )
*
                     X( 1 ) = X( 1 ) + B( J1 )*X( N+J1 )
                     X( N+1 ) = X( N+1 ) - B( J1 )*X( J1 )
*
                     XMAX = ZERO
                     DO 50 K = 1, J1 - 1
                        XMAX = MAX( XMAX, ABS( X( K ) )+
     $                         ABS( X( K+N ) ) )
   50                CONTINUE
                  END IF
*
               ELSE
*
*                 Meet 2 by 2 diagonal block
*
                  D( 1, 1 ) = X( J1 )
                  D( 2, 1 ) = X( J2 )
                  D( 1, 2 ) = X( N+J1 )
                  D( 2, 2 ) = X( N+J2 )
                  CALL DLALN2( .FALSE., 2, 2, SMINW, ONE, T( J1, J1 ),
     $                         LDT, ONE, ONE, D, 2, ZERO, -W, V, 2,
     $                         SCALOC, XNORM, IERR )
                  IF( IERR.NE.0 )
     $               INFO = 2
*
                  IF( SCALOC.NE.ONE ) THEN
                     CALL DSCAL( 2*N, SCALOC, X, 1 )
                     SCALE = SCALOC*SCALE
                  END IF
                  X( J1 ) = V( 1, 1 )
                  X( J2 ) = V( 2, 1 )
                  X( N+J1 ) = V( 1, 2 )
                  X( N+J2 ) = V( 2, 2 )
*
*                 Scale X(J1), .... to avoid overflow in
*                 updating right hand side.
*
                  XJ = MAX( ABS( V( 1, 1 ) )+ABS( V( 1, 2 ) ),
     $                 ABS( V( 2, 1 ) )+ABS( V( 2, 2 ) ) )
                  IF( XJ.GT.ONE ) THEN
                     REC = ONE / XJ
                     IF( MAX( WORK( J1 ), WORK( J2 ) ).GT.
     $                   ( BIGNUM-XMAX )*REC ) THEN
                        CALL DSCAL( N2, REC, X, 1 )
                        SCALE = SCALE*REC
                     END IF
                  END IF
*
*                 Update the right-hand side.
*
                  IF( J1.GT.1 ) THEN
                     CALL DAXPY( J1-1, -X( J1 ), T( 1, J1 ), 1, X, 1 )
                     CALL DAXPY( J1-1, -X( J2 ), T( 1, J2 ), 1, X, 1 )
*
                     CALL DAXPY( J1-1, -X( N+J1 ), T( 1, J1 ), 1,
     $                           X( N+1 ), 1 )
                     CALL DAXPY( J1-1, -X( N+J2 ), T( 1, J2 ), 1,
     $                           X( N+1 ), 1 )
*
                     X( 1 ) = X( 1 ) + B( J1 )*X( N+J1 ) +
     $                        B( J2 )*X( N+J2 )
                     X( N+1 ) = X( N+1 ) - B( J1 )*X( J1 ) -
     $                          B( J2 )*X( J2 )
*
                     XMAX = ZERO
                     DO 60 K = 1, J1 - 1
                        XMAX = MAX( ABS( X( K ) )+ABS( X( K+N ) ),
     $                         XMAX )
   60                CONTINUE
                  END IF
*
               END IF
   70       CONTINUE
*
         ELSE
*
*           Solve (T + iB)**T*(p+iq) = c+id
*
            JNEXT = 1
            DO 80 J = 1, N
               IF( J.LT.JNEXT )
     $            GO TO 80
               J1 = J
               J2 = J
               JNEXT = J + 1
               IF( J.LT.N ) THEN
                  IF( T( J+1, J ).NE.ZERO ) THEN
                     J2 = J + 1
                     JNEXT = J + 2
                  END IF
               END IF
*
               IF( J1.EQ.J2 ) THEN
*
*                 1 by 1 diagonal block
*
*                 Scale if necessary to avoid overflow in forming the
*                 right-hand side element by inner product.
*
                  XJ = ABS( X( J1 ) ) + ABS( X( J1+N ) )
                  IF( XMAX.GT.ONE ) THEN
                     REC = ONE / XMAX
                     IF( WORK( J1 ).GT.( BIGNUM-XJ )*REC ) THEN
                        CALL DSCAL( N2, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
*
                  X( J1 ) = X( J1 ) - DDOT( J1-1, T( 1, J1 ), 1, X, 1 )
                  X( N+J1 ) = X( N+J1 ) - DDOT( J1-1, T( 1, J1 ), 1,
     $                        X( N+1 ), 1 )
                  IF( J1.GT.1 ) THEN
                     X( J1 ) = X( J1 ) - B( J1 )*X( N+1 )
                     X( N+J1 ) = X( N+J1 ) + B( J1 )*X( 1 )
                  END IF
                  XJ = ABS( X( J1 ) ) + ABS( X( J1+N ) )
*
                  Z = W
                  IF( J1.EQ.1 )
     $               Z = B( 1 )
*
*                 Scale if necessary to avoid overflow in
*                 complex division
*
                  TJJ = ABS( T( J1, J1 ) ) + ABS( Z )
                  TMP = T( J1, J1 )
                  IF( TJJ.LT.SMINW ) THEN
                     TMP = SMINW
                     TJJ = SMINW
                     INFO = 1
                  END IF
*
                  IF( TJJ.LT.ONE ) THEN
                     IF( XJ.GT.BIGNUM*TJJ ) THEN
                        REC = ONE / XJ
                        CALL DSCAL( N2, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
                  CALL DLADIV( X( J1 ), X( N+J1 ), TMP, -Z, SR, SI )
                  X( J1 ) = SR
                  X( J1+N ) = SI
                  XMAX = MAX( ABS( X( J1 ) )+ABS( X( J1+N ) ), XMAX )
*
               ELSE
*
*                 2 by 2 diagonal block
*
*                 Scale if necessary to avoid overflow in forming the
*                 right-hand side element by inner product.
*
                  XJ = MAX( ABS( X( J1 ) )+ABS( X( N+J1 ) ),
     $                 ABS( X( J2 ) )+ABS( X( N+J2 ) ) )
                  IF( XMAX.GT.ONE ) THEN
                     REC = ONE / XMAX
                     IF( MAX( WORK( J1 ), WORK( J2 ) ).GT.
     $                   ( BIGNUM-XJ ) / XMAX ) THEN
                        CALL DSCAL( N2, REC, X, 1 )
                        SCALE = SCALE*REC
                        XMAX = XMAX*REC
                     END IF
                  END IF
*
                  D( 1, 1 ) = X( J1 ) - DDOT( J1-1, T( 1, J1 ), 1, X,
     $                        1 )
                  D( 2, 1 ) = X( J2 ) - DDOT( J1-1, T( 1, J2 ), 1, X,
     $                        1 )
                  D( 1, 2 ) = X( N+J1 ) - DDOT( J1-1, T( 1, J1 ), 1,
     $                        X( N+1 ), 1 )
                  D( 2, 2 ) = X( N+J2 ) - DDOT( J1-1, T( 1, J2 ), 1,
     $                        X( N+1 ), 1 )
                  D( 1, 1 ) = D( 1, 1 ) - B( J1 )*X( N+1 )
                  D( 2, 1 ) = D( 2, 1 ) - B( J2 )*X( N+1 )
                  D( 1, 2 ) = D( 1, 2 ) + B( J1 )*X( 1 )
                  D( 2, 2 ) = D( 2, 2 ) + B( J2 )*X( 1 )
*
                  CALL DLALN2( .TRUE., 2, 2, SMINW, ONE, T( J1, J1 ),
     $                         LDT, ONE, ONE, D, 2, ZERO, W, V, 2,
     $                         SCALOC, XNORM, IERR )
                  IF( IERR.NE.0 )
     $               INFO = 2
*
                  IF( SCALOC.NE.ONE ) THEN
                     CALL DSCAL( N2, SCALOC, X, 1 )
                     SCALE = SCALOC*SCALE
                  END IF
                  X( J1 ) = V( 1, 1 )
                  X( J2 ) = V( 2, 1 )
                  X( N+J1 ) = V( 1, 2 )
                  X( N+J2 ) = V( 2, 2 )
                  XMAX = MAX( ABS( X( J1 ) )+ABS( X( N+J1 ) ),
     $                   ABS( X( J2 ) )+ABS( X( N+J2 ) ), XMAX )
*
               END IF
*
   80       CONTINUE
*
         END IF
*
      END IF
*
      RETURN
*
*     End of DLAQTR
*
      END
