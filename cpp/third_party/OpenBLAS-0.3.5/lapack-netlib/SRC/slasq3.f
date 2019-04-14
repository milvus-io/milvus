*> \brief \b SLASQ3 checks for deflation, computes a shift and calls dqds. Used by sbdsqr.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLASQ3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasq3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasq3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasq3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLASQ3( I0, N0, Z, PP, DMIN, SIGMA, DESIG, QMAX, NFAIL,
*                          ITER, NDIV, IEEE, TTYPE, DMIN1, DMIN2, DN, DN1,
*                          DN2, G, TAU )
*
*       .. Scalar Arguments ..
*       LOGICAL            IEEE
*       INTEGER            I0, ITER, N0, NDIV, NFAIL, PP
*       REAL               DESIG, DMIN, DMIN1, DMIN2, DN, DN1, DN2, G,
*      $                   QMAX, SIGMA, TAU
*       ..
*       .. Array Arguments ..
*       REAL               Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLASQ3 checks for deflation, computes a shift (TAU) and calls dqds.
*> In case of failure it changes shifts, and tries again until output
*> is positive.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] I0
*> \verbatim
*>          I0 is INTEGER
*>         First index.
*> \endverbatim
*>
*> \param[in,out] N0
*> \verbatim
*>          N0 is INTEGER
*>         Last index.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is REAL array, dimension ( 4*N0 )
*>         Z holds the qd array.
*> \endverbatim
*>
*> \param[in,out] PP
*> \verbatim
*>          PP is INTEGER
*>         PP=0 for ping, PP=1 for pong.
*>         PP=2 indicates that flipping was applied to the Z array
*>         and that the initial tests for deflation should not be
*>         performed.
*> \endverbatim
*>
*> \param[out] DMIN
*> \verbatim
*>          DMIN is REAL
*>         Minimum value of d.
*> \endverbatim
*>
*> \param[out] SIGMA
*> \verbatim
*>          SIGMA is REAL
*>         Sum of shifts used in current segment.
*> \endverbatim
*>
*> \param[in,out] DESIG
*> \verbatim
*>          DESIG is REAL
*>         Lower order part of SIGMA
*> \endverbatim
*>
*> \param[in] QMAX
*> \verbatim
*>          QMAX is REAL
*>         Maximum value of q.
*> \endverbatim
*>
*> \param[in,out] NFAIL
*> \verbatim
*>          NFAIL is INTEGER
*>         Increment NFAIL by 1 each time the shift was too big.
*> \endverbatim
*>
*> \param[in,out] ITER
*> \verbatim
*>          ITER is INTEGER
*>         Increment ITER by 1 for each iteration.
*> \endverbatim
*>
*> \param[in,out] NDIV
*> \verbatim
*>          NDIV is INTEGER
*>         Increment NDIV by 1 for each division.
*> \endverbatim
*>
*> \param[in] IEEE
*> \verbatim
*>          IEEE is LOGICAL
*>         Flag for IEEE or non IEEE arithmetic (passed to SLASQ5).
*> \endverbatim
*>
*> \param[in,out] TTYPE
*> \verbatim
*>          TTYPE is INTEGER
*>         Shift type.
*> \endverbatim
*>
*> \param[in,out] DMIN1
*> \verbatim
*>          DMIN1 is REAL
*> \endverbatim
*>
*> \param[in,out] DMIN2
*> \verbatim
*>          DMIN2 is REAL
*> \endverbatim
*>
*> \param[in,out] DN
*> \verbatim
*>          DN is REAL
*> \endverbatim
*>
*> \param[in,out] DN1
*> \verbatim
*>          DN1 is REAL
*> \endverbatim
*>
*> \param[in,out] DN2
*> \verbatim
*>          DN2 is REAL
*> \endverbatim
*>
*> \param[in,out] G
*> \verbatim
*>          G is REAL
*> \endverbatim
*>
*> \param[in,out] TAU
*> \verbatim
*>          TAU is REAL
*>
*>         These are passed as arguments in order to save their values
*>         between calls to SLASQ3.
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
*> \date June 2016
*
*> \ingroup auxOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SLASQ3( I0, N0, Z, PP, DMIN, SIGMA, DESIG, QMAX, NFAIL,
     $                   ITER, NDIV, IEEE, TTYPE, DMIN1, DMIN2, DN, DN1,
     $                   DN2, G, TAU )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      LOGICAL            IEEE
      INTEGER            I0, ITER, N0, NDIV, NFAIL, PP
      REAL               DESIG, DMIN, DMIN1, DMIN2, DN, DN1, DN2, G,
     $                   QMAX, SIGMA, TAU
*     ..
*     .. Array Arguments ..
      REAL               Z( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               CBIAS
      PARAMETER          ( CBIAS = 1.50E0 )
      REAL               ZERO, QURTR, HALF, ONE, TWO, HUNDRD
      PARAMETER          ( ZERO = 0.0E0, QURTR = 0.250E0, HALF = 0.5E0,
     $                     ONE = 1.0E0, TWO = 2.0E0, HUNDRD = 100.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            IPN4, J4, N0IN, NN, TTYPE
      REAL               EPS, S, T, TEMP, TOL, TOL2
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLASQ4, SLASQ5, SLASQ6
*     ..
*     .. External Function ..
      REAL               SLAMCH
      LOGICAL            SISNAN
      EXTERNAL           SISNAN, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
      N0IN = N0
      EPS = SLAMCH( 'Precision' )
      TOL = EPS*HUNDRD
      TOL2 = TOL**2
*
*     Check for deflation.
*
   10 CONTINUE
*
      IF( N0.LT.I0 )
     $   RETURN
      IF( N0.EQ.I0 )
     $   GO TO 20
      NN = 4*N0 + PP
      IF( N0.EQ.( I0+1 ) )
     $   GO TO 40
*
*     Check whether E(N0-1) is negligible, 1 eigenvalue.
*
      IF( Z( NN-5 ).GT.TOL2*( SIGMA+Z( NN-3 ) ) .AND.
     $    Z( NN-2*PP-4 ).GT.TOL2*Z( NN-7 ) )
     $   GO TO 30
*
   20 CONTINUE
*
      Z( 4*N0-3 ) = Z( 4*N0+PP-3 ) + SIGMA
      N0 = N0 - 1
      GO TO 10
*
*     Check  whether E(N0-2) is negligible, 2 eigenvalues.
*
   30 CONTINUE
*
      IF( Z( NN-9 ).GT.TOL2*SIGMA .AND.
     $    Z( NN-2*PP-8 ).GT.TOL2*Z( NN-11 ) )
     $   GO TO 50
*
   40 CONTINUE
*
      IF( Z( NN-3 ).GT.Z( NN-7 ) ) THEN
         S = Z( NN-3 )
         Z( NN-3 ) = Z( NN-7 )
         Z( NN-7 ) = S
      END IF
      T = HALF*( ( Z( NN-7 )-Z( NN-3 ) )+Z( NN-5 ) )
      IF( Z( NN-5 ).GT.Z( NN-3 )*TOL2.AND.T.NE.ZERO ) THEN
         S = Z( NN-3 )*( Z( NN-5 ) / T )
         IF( S.LE.T ) THEN
            S = Z( NN-3 )*( Z( NN-5 ) /
     $          ( T*( ONE+SQRT( ONE+S / T ) ) ) )
         ELSE
            S = Z( NN-3 )*( Z( NN-5 ) / ( T+SQRT( T )*SQRT( T+S ) ) )
         END IF
         T = Z( NN-7 ) + ( S+Z( NN-5 ) )
         Z( NN-3 ) = Z( NN-3 )*( Z( NN-7 ) / T )
         Z( NN-7 ) = T
      END IF
      Z( 4*N0-7 ) = Z( NN-7 ) + SIGMA
      Z( 4*N0-3 ) = Z( NN-3 ) + SIGMA
      N0 = N0 - 2
      GO TO 10
*
   50 CONTINUE
      IF( PP.EQ.2 )
     $   PP = 0
*
*     Reverse the qd-array, if warranted.
*
      IF( DMIN.LE.ZERO .OR. N0.LT.N0IN ) THEN
         IF( CBIAS*Z( 4*I0+PP-3 ).LT.Z( 4*N0+PP-3 ) ) THEN
            IPN4 = 4*( I0+N0 )
            DO 60 J4 = 4*I0, 2*( I0+N0-1 ), 4
               TEMP = Z( J4-3 )
               Z( J4-3 ) = Z( IPN4-J4-3 )
               Z( IPN4-J4-3 ) = TEMP
               TEMP = Z( J4-2 )
               Z( J4-2 ) = Z( IPN4-J4-2 )
               Z( IPN4-J4-2 ) = TEMP
               TEMP = Z( J4-1 )
               Z( J4-1 ) = Z( IPN4-J4-5 )
               Z( IPN4-J4-5 ) = TEMP
               TEMP = Z( J4 )
               Z( J4 ) = Z( IPN4-J4-4 )
               Z( IPN4-J4-4 ) = TEMP
   60       CONTINUE
            IF( N0-I0.LE.4 ) THEN
               Z( 4*N0+PP-1 ) = Z( 4*I0+PP-1 )
               Z( 4*N0-PP ) = Z( 4*I0-PP )
            END IF
            DMIN2 = MIN( DMIN2, Z( 4*N0+PP-1 ) )
            Z( 4*N0+PP-1 ) = MIN( Z( 4*N0+PP-1 ), Z( 4*I0+PP-1 ),
     $                            Z( 4*I0+PP+3 ) )
            Z( 4*N0-PP ) = MIN( Z( 4*N0-PP ), Z( 4*I0-PP ),
     $                          Z( 4*I0-PP+4 ) )
            QMAX = MAX( QMAX, Z( 4*I0+PP-3 ), Z( 4*I0+PP+1 ) )
            DMIN = -ZERO
         END IF
      END IF
*
*     Choose a shift.
*
      CALL SLASQ4( I0, N0, Z, PP, N0IN, DMIN, DMIN1, DMIN2, DN, DN1,
     $             DN2, TAU, TTYPE, G )
*
*     Call dqds until DMIN > 0.
*
   70 CONTINUE
*
      CALL SLASQ5( I0, N0, Z, PP, TAU, SIGMA, DMIN, DMIN1, DMIN2, DN,
     $             DN1, DN2, IEEE, EPS )
*
      NDIV = NDIV + ( N0-I0+2 )
      ITER = ITER + 1
*
*     Check status.
*
      IF( DMIN.GE.ZERO .AND. DMIN1.GE.ZERO ) THEN
*
*        Success.
*
         GO TO 90
*
      ELSE IF( DMIN.LT.ZERO .AND. DMIN1.GT.ZERO .AND.
     $         Z( 4*( N0-1 )-PP ).LT.TOL*( SIGMA+DN1 ) .AND.
     $         ABS( DN ).LT.TOL*SIGMA ) THEN
*
*        Convergence hidden by negative DN.
*
         Z( 4*( N0-1 )-PP+2 ) = ZERO
         DMIN = ZERO
         GO TO 90
      ELSE IF( DMIN.LT.ZERO ) THEN
*
*        TAU too big. Select new TAU and try again.
*
         NFAIL = NFAIL + 1
         IF( TTYPE.LT.-22 ) THEN
*
*           Failed twice. Play it safe.
*
            TAU = ZERO
         ELSE IF( DMIN1.GT.ZERO ) THEN
*
*           Late failure. Gives excellent shift.
*
            TAU = ( TAU+DMIN )*( ONE-TWO*EPS )
            TTYPE = TTYPE - 11
         ELSE
*
*           Early failure. Divide by 4.
*
            TAU = QURTR*TAU
            TTYPE = TTYPE - 12
         END IF
         GO TO 70
      ELSE IF( SISNAN( DMIN ) ) THEN
*
*        NaN.
*
         IF( TAU.EQ.ZERO ) THEN
            GO TO 80
         ELSE
            TAU = ZERO
            GO TO 70
         END IF
      ELSE
*
*        Possible underflow. Play it safe.
*
         GO TO 80
      END IF
*
*     Risk of underflow.
*
   80 CONTINUE
      CALL SLASQ6( I0, N0, Z, PP, DMIN, DMIN1, DMIN2, DN, DN1, DN2 )
      NDIV = NDIV + ( N0-I0+2 )
      ITER = ITER + 1
      TAU = ZERO
*
   90 CONTINUE
      IF( TAU.LT.SIGMA ) THEN
         DESIG = DESIG + TAU
         T = SIGMA + DESIG
         DESIG = DESIG - ( T-SIGMA )
      ELSE
         T = SIGMA + TAU
         DESIG = SIGMA - ( T-TAU ) + DESIG
      END IF
      SIGMA = T
*
      RETURN
*
*     End of SLASQ3
*
      END
