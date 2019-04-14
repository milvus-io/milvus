*> \brief \b SGET39
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGET39( RMAX, LMAX, NINFO, KNT )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, LMAX, NINFO
*       REAL               RMAX
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGET39 tests SLAQTR, a routine for solving the real or
*> special complex quasi upper triangular system
*>
*>      op(T)*p = scale*c,
*> or
*>      op(T + iB)*(p+iq) = scale*(c+id),
*>
*> in real arithmetic. T is upper quasi-triangular.
*> If it is complex, then the first diagonal block of T must be
*> 1 by 1, B has the special structure
*>
*>                B = [ b(1) b(2) ... b(n) ]
*>                    [       w            ]
*>                    [           w        ]
*>                    [              .     ]
*>                    [                 w  ]
*>
*> op(A) = A or A', where A' denotes the conjugate transpose of
*> the matrix A.
*>
*> On input, X = [ c ].  On output, X = [ p ].
*>               [ d ]                  [ q ]
*>
*> Scale is an output less than or equal to 1, chosen to avoid
*> overflow in X.
*> This subroutine is specially designed for the condition number
*> estimation in the eigenproblem routine STRSNA.
*>
*> The test code verifies that the following residual is order 1:
*>
*>      ||(T+i*B)*(x1+i*x2) - scale*(d1+i*d2)||
*>    -----------------------------------------
*>        max(ulp*(||T||+||B||)*(||x1||+||x2||),
*>            (||T||+||B||)*smlnum/ulp,
*>            smlnum)
*>
*> (The (||T||+||B||)*smlnum/ulp term accounts for possible
*>  (gradual or nongradual) underflow in x1 and x2.)
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[out] RMAX
*> \verbatim
*>          RMAX is REAL
*>          Value of the largest test ratio.
*> \endverbatim
*>
*> \param[out] LMAX
*> \verbatim
*>          LMAX is INTEGER
*>          Example number where largest test ratio achieved.
*> \endverbatim
*>
*> \param[out] NINFO
*> \verbatim
*>          NINFO is INTEGER
*>          Number of examples where INFO is nonzero.
*> \endverbatim
*>
*> \param[out] KNT
*> \verbatim
*>          KNT is INTEGER
*>          Total number of examples tested.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SGET39( RMAX, LMAX, NINFO, KNT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KNT, LMAX, NINFO
      REAL               RMAX
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            LDT, LDT2
      PARAMETER          ( LDT = 10, LDT2 = 2*LDT )
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0, ONE = 1.0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, IVM1, IVM2, IVM3, IVM4, IVM5, J, K, N,
     $                   NDIM
      REAL               BIGNUM, DOMIN, DUMM, EPS, NORM, NORMTB, RESID,
     $                   SCALE, SMLNUM, W, XNORM
*     ..
*     .. External Functions ..
      INTEGER            ISAMAX
      REAL               SASUM, SDOT, SLAMCH, SLANGE
      EXTERNAL           ISAMAX, SASUM, SDOT, SLAMCH, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEMV, SLABAD, SLAQTR
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, COS, MAX, REAL, SIN, SQRT
*     ..
*     .. Local Arrays ..
      INTEGER            IDIM( 6 ), IVAL( 5, 5, 6 )
      REAL               B( LDT ), D( LDT2 ), DUM( 1 ), T( LDT, LDT ),
     $                   VM1( 5 ), VM2( 5 ), VM3( 5 ), VM4( 5 ),
     $                   VM5( 3 ), WORK( LDT ), X( LDT2 ), Y( LDT2 )
*     ..
*     .. Data statements ..
      DATA               IDIM / 4, 5*5 /
      DATA               IVAL / 3, 4*0, 1, 1, -1, 0, 0, 3, 2, 1, 0, 0,
     $                   4, 3, 2, 2, 0, 5*0, 1, 4*0, 2, 2, 3*0, 3, 3, 4,
     $                   0, 0, 4, 2, 2, 3, 0, 4*1, 5, 1, 4*0, 2, 4, -2,
     $                   0, 0, 3, 3, 4, 0, 0, 4, 2, 2, 3, 0, 5*1, 1,
     $                   4*0, 2, 1, -1, 0, 0, 9, 8, 1, 0, 0, 4, 9, 1, 2,
     $                   -1, 5*2, 9, 4*0, 6, 4, 0, 0, 0, 3, 2, 1, 1, 0,
     $                   5, 1, -1, 1, 0, 5*2, 4, 4*0, 2, 2, 0, 0, 0, 1,
     $                   4, 4, 0, 0, 2, 4, 2, 2, -1, 5*2 /
*     ..
*     .. Executable Statements ..
*
*     Get machine parameters
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
*
*     Set up test case parameters
*
      VM1( 1 ) = ONE
      VM1( 2 ) = SQRT( SMLNUM )
      VM1( 3 ) = SQRT( VM1( 2 ) )
      VM1( 4 ) = SQRT( BIGNUM )
      VM1( 5 ) = SQRT( VM1( 4 ) )
*
      VM2( 1 ) = ONE
      VM2( 2 ) = SQRT( SMLNUM )
      VM2( 3 ) = SQRT( VM2( 2 ) )
      VM2( 4 ) = SQRT( BIGNUM )
      VM2( 5 ) = SQRT( VM2( 4 ) )
*
      VM3( 1 ) = ONE
      VM3( 2 ) = SQRT( SMLNUM )
      VM3( 3 ) = SQRT( VM3( 2 ) )
      VM3( 4 ) = SQRT( BIGNUM )
      VM3( 5 ) = SQRT( VM3( 4 ) )
*
      VM4( 1 ) = ONE
      VM4( 2 ) = SQRT( SMLNUM )
      VM4( 3 ) = SQRT( VM4( 2 ) )
      VM4( 4 ) = SQRT( BIGNUM )
      VM4( 5 ) = SQRT( VM4( 4 ) )
*
      VM5( 1 ) = ONE
      VM5( 2 ) = EPS
      VM5( 3 ) = SQRT( SMLNUM )
*
*     Initalization
*
      KNT = 0
      RMAX = ZERO
      NINFO = 0
      SMLNUM = SMLNUM / EPS
*
*     Begin test loop
*
      DO 140 IVM5 = 1, 3
         DO 130 IVM4 = 1, 5
            DO 120 IVM3 = 1, 5
               DO 110 IVM2 = 1, 5
                  DO 100 IVM1 = 1, 5
                     DO 90 NDIM = 1, 6
*
                        N = IDIM( NDIM )
                        DO 20 I = 1, N
                           DO 10 J = 1, N
                              T( I, J ) = REAL( IVAL( I, J, NDIM ) )*
     $                                    VM1( IVM1 )
                              IF( I.GE.J )
     $                           T( I, J ) = T( I, J )*VM5( IVM5 )
   10                      CONTINUE
   20                   CONTINUE
*
                        W = ONE*VM2( IVM2 )
*
                        DO 30 I = 1, N
                           B( I ) = COS( REAL( I ) )*VM3( IVM3 )
   30                   CONTINUE
*
                        DO 40 I = 1, 2*N
                           D( I ) = SIN( REAL( I ) )*VM4( IVM4 )
   40                   CONTINUE
*
                        NORM = SLANGE( '1', N, N, T, LDT, WORK )
                        K = ISAMAX( N, B, 1 )
                        NORMTB = NORM + ABS( B( K ) ) + ABS( W )
*
                        CALL SCOPY( N, D, 1, X, 1 )
                        KNT = KNT + 1
                        CALL SLAQTR( .FALSE., .TRUE., N, T, LDT, DUM,
     $                               DUMM, SCALE, X, WORK, INFO )
                        IF( INFO.NE.0 )
     $                     NINFO = NINFO + 1
*
*                       || T*x - scale*d || /
*                         max(ulp*||T||*||x||,smlnum/ulp*||T||,smlnum)
*
                        CALL SCOPY( N, D, 1, Y, 1 )
                        CALL SGEMV( 'No transpose', N, N, ONE, T, LDT,
     $                              X, 1, -SCALE, Y, 1 )
                        XNORM = SASUM( N, X, 1 )
                        RESID = SASUM( N, Y, 1 )
                        DOMIN = MAX( SMLNUM, ( SMLNUM / EPS )*NORM,
     $                          ( NORM*EPS )*XNORM )
                        RESID = RESID / DOMIN
                        IF( RESID.GT.RMAX ) THEN
                           RMAX = RESID
                           LMAX = KNT
                        END IF
*
                        CALL SCOPY( N, D, 1, X, 1 )
                        KNT = KNT + 1
                        CALL SLAQTR( .TRUE., .TRUE., N, T, LDT, DUM,
     $                               DUMM, SCALE, X, WORK, INFO )
                        IF( INFO.NE.0 )
     $                     NINFO = NINFO + 1
*
*                       || T*x - scale*d || /
*                         max(ulp*||T||*||x||,smlnum/ulp*||T||,smlnum)
*
                        CALL SCOPY( N, D, 1, Y, 1 )
                        CALL SGEMV( 'Transpose', N, N, ONE, T, LDT, X,
     $                              1, -SCALE, Y, 1 )
                        XNORM = SASUM( N, X, 1 )
                        RESID = SASUM( N, Y, 1 )
                        DOMIN = MAX( SMLNUM, ( SMLNUM / EPS )*NORM,
     $                          ( NORM*EPS )*XNORM )
                        RESID = RESID / DOMIN
                        IF( RESID.GT.RMAX ) THEN
                           RMAX = RESID
                           LMAX = KNT
                        END IF
*
                        CALL SCOPY( 2*N, D, 1, X, 1 )
                        KNT = KNT + 1
                        CALL SLAQTR( .FALSE., .FALSE., N, T, LDT, B, W,
     $                               SCALE, X, WORK, INFO )
                        IF( INFO.NE.0 )
     $                     NINFO = NINFO + 1
*
*                       ||(T+i*B)*(x1+i*x2) - scale*(d1+i*d2)|| /
*                          max(ulp*(||T||+||B||)*(||x1||+||x2||),
*                                  smlnum/ulp * (||T||+||B||), smlnum )
*
*
                        CALL SCOPY( 2*N, D, 1, Y, 1 )
                        Y( 1 ) = SDOT( N, B, 1, X( 1+N ), 1 ) +
     $                           SCALE*Y( 1 )
                        DO 50 I = 2, N
                           Y( I ) = W*X( I+N ) + SCALE*Y( I )
   50                   CONTINUE
                        CALL SGEMV( 'No transpose', N, N, ONE, T, LDT,
     $                              X, 1, -ONE, Y, 1 )
*
                        Y( 1+N ) = SDOT( N, B, 1, X, 1 ) -
     $                             SCALE*Y( 1+N )
                        DO 60 I = 2, N
                           Y( I+N ) = W*X( I ) - SCALE*Y( I+N )
   60                   CONTINUE
                        CALL SGEMV( 'No transpose', N, N, ONE, T, LDT,
     $                              X( 1+N ), 1, ONE, Y( 1+N ), 1 )
*
                        RESID = SASUM( 2*N, Y, 1 )
                        DOMIN = MAX( SMLNUM, ( SMLNUM / EPS )*NORMTB,
     $                          EPS*( NORMTB*SASUM( 2*N, X, 1 ) ) )
                        RESID = RESID / DOMIN
                        IF( RESID.GT.RMAX ) THEN
                           RMAX = RESID
                           LMAX = KNT
                        END IF
*
                        CALL SCOPY( 2*N, D, 1, X, 1 )
                        KNT = KNT + 1
                        CALL SLAQTR( .TRUE., .FALSE., N, T, LDT, B, W,
     $                               SCALE, X, WORK, INFO )
                        IF( INFO.NE.0 )
     $                     NINFO = NINFO + 1
*
*                       ||(T+i*B)*(x1+i*x2) - scale*(d1+i*d2)|| /
*                          max(ulp*(||T||+||B||)*(||x1||+||x2||),
*                                  smlnum/ulp * (||T||+||B||), smlnum )
*
                        CALL SCOPY( 2*N, D, 1, Y, 1 )
                        Y( 1 ) = B( 1 )*X( 1+N ) - SCALE*Y( 1 )
                        DO 70 I = 2, N
                           Y( I ) = B( I )*X( 1+N ) + W*X( I+N ) -
     $                              SCALE*Y( I )
   70                   CONTINUE
                        CALL SGEMV( 'Transpose', N, N, ONE, T, LDT, X,
     $                              1, ONE, Y, 1 )
*
                        Y( 1+N ) = B( 1 )*X( 1 ) + SCALE*Y( 1+N )
                        DO 80 I = 2, N
                           Y( I+N ) = B( I )*X( 1 ) + W*X( I ) +
     $                                SCALE*Y( I+N )
   80                   CONTINUE
                        CALL SGEMV( 'Transpose', N, N, ONE, T, LDT,
     $                              X( 1+N ), 1, -ONE, Y( 1+N ), 1 )
*
                        RESID = SASUM( 2*N, Y, 1 )
                        DOMIN = MAX( SMLNUM, ( SMLNUM / EPS )*NORMTB,
     $                          EPS*( NORMTB*SASUM( 2*N, X, 1 ) ) )
                        RESID = RESID / DOMIN
                        IF( RESID.GT.RMAX ) THEN
                           RMAX = RESID
                           LMAX = KNT
                        END IF
*
   90                CONTINUE
  100             CONTINUE
  110          CONTINUE
  120       CONTINUE
  130    CONTINUE
  140 CONTINUE
*
      RETURN
*
*     End of SGET39
*
      END
