*> \brief \b DGET32
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET32( RMAX, LMAX, NINFO, KNT )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, LMAX, NINFO
*       DOUBLE PRECISION   RMAX
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGET32 tests DLASY2, a routine for solving
*>
*>         op(TL)*X + ISGN*X*op(TR) = SCALE*B
*>
*> where TL is N1 by N1, TR is N2 by N2, and N1,N2 =1 or 2 only.
*> X and B are N1 by N2, op() is an optional transpose, an
*> ISGN = 1 or -1. SCALE is chosen less than or equal to 1 to
*> avoid overflow in X.
*>
*> The test condition is that the scaled residual
*>
*> norm( op(TL)*X + ISGN*X*op(TR) = SCALE*B )
*>      / ( max( ulp*norm(TL), ulp*norm(TR)) * norm(X), SMLNUM )
*>
*> should be on the order of 1. Here, ulp is the machine precision.
*> Also, it is verified that SCALE is less than or equal to 1, and
*> that XNORM = infinity-norm(X).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[out] RMAX
*> \verbatim
*>          RMAX is DOUBLE PRECISION
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
*>          Number of examples returned with INFO.NE.0.
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DGET32( RMAX, LMAX, NINFO, KNT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KNT, LMAX, NINFO
      DOUBLE PRECISION   RMAX
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
      DOUBLE PRECISION   TWO, FOUR, EIGHT
      PARAMETER          ( TWO = 2.0D0, FOUR = 4.0D0, EIGHT = 8.0D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LTRANL, LTRANR
      INTEGER            IB, IB1, IB2, IB3, INFO, ISGN, ITL, ITLSCL,
     $                   ITR, ITRANL, ITRANR, ITRSCL, N1, N2
      DOUBLE PRECISION   BIGNUM, DEN, EPS, RES, SCALE, SGN, SMLNUM, TMP,
     $                   TNRM, XNORM, XNRM
*     ..
*     .. Local Arrays ..
      INTEGER            ITVAL( 2, 2, 8 )
      DOUBLE PRECISION   B( 2, 2 ), TL( 2, 2 ), TR( 2, 2 ), VAL( 3 ),
     $                   X( 2, 2 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLASY2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SQRT
*     ..
*     .. Data statements ..
      DATA               ITVAL / 8, 4, 2, 1, 4, 8, 1, 2, 2, 1, 8, 4, 1,
     $                   2, 4, 8, 9, 4, 2, 1, 4, 9, 1, 2, 2, 1, 9, 4, 1,
     $                   2, 4, 9 /
*     ..
*     .. Executable Statements ..
*
*     Get machine parameters
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
*
*     Set up test case parameters
*
      VAL( 1 ) = SQRT( SMLNUM )
      VAL( 2 ) = ONE
      VAL( 3 ) = SQRT( BIGNUM )
*
      KNT = 0
      NINFO = 0
      LMAX = 0
      RMAX = ZERO
*
*     Begin test loop
*
      DO 230 ITRANL = 0, 1
         DO 220 ITRANR = 0, 1
            DO 210 ISGN = -1, 1, 2
               SGN = ISGN
               LTRANL = ITRANL.EQ.1
               LTRANR = ITRANR.EQ.1
*
               N1 = 1
               N2 = 1
               DO 30 ITL = 1, 3
                  DO 20 ITR = 1, 3
                     DO 10 IB = 1, 3
                        TL( 1, 1 ) = VAL( ITL )
                        TR( 1, 1 ) = VAL( ITR )
                        B( 1, 1 ) = VAL( IB )
                        KNT = KNT + 1
                        CALL DLASY2( LTRANL, LTRANR, ISGN, N1, N2, TL,
     $                               2, TR, 2, B, 2, SCALE, X, 2, XNORM,
     $                               INFO )
                        IF( INFO.NE.0 )
     $                     NINFO = NINFO + 1
                        RES = ABS( ( TL( 1, 1 )+SGN*TR( 1, 1 ) )*
     $                        X( 1, 1 )-SCALE*B( 1, 1 ) )
                        IF( INFO.EQ.0 ) THEN
                           DEN = MAX( EPS*( ( ABS( TR( 1,
     $                           1 ) )+ABS( TL( 1, 1 ) ) )*ABS( X( 1,
     $                           1 ) ) ), SMLNUM )
                        ELSE
                           DEN = SMLNUM*MAX( ABS( X( 1, 1 ) ), ONE )
                        END IF
                        RES = RES / DEN
                        IF( SCALE.GT.ONE )
     $                     RES = RES + ONE / EPS
                        RES = RES + ABS( XNORM-ABS( X( 1, 1 ) ) ) /
     $                        MAX( SMLNUM, XNORM ) / EPS
                        IF( INFO.NE.0 .AND. INFO.NE.1 )
     $                     RES = RES + ONE / EPS
                        IF( RES.GT.RMAX ) THEN
                           LMAX = KNT
                           RMAX = RES
                        END IF
   10                CONTINUE
   20             CONTINUE
   30          CONTINUE
*
               N1 = 2
               N2 = 1
               DO 80 ITL = 1, 8
                  DO 70 ITLSCL = 1, 3
                     DO 60 ITR = 1, 3
                        DO 50 IB1 = 1, 3
                           DO 40 IB2 = 1, 3
                              B( 1, 1 ) = VAL( IB1 )
                              B( 2, 1 ) = -FOUR*VAL( IB2 )
                              TL( 1, 1 ) = ITVAL( 1, 1, ITL )*
     $                                     VAL( ITLSCL )
                              TL( 2, 1 ) = ITVAL( 2, 1, ITL )*
     $                                     VAL( ITLSCL )
                              TL( 1, 2 ) = ITVAL( 1, 2, ITL )*
     $                                     VAL( ITLSCL )
                              TL( 2, 2 ) = ITVAL( 2, 2, ITL )*
     $                                     VAL( ITLSCL )
                              TR( 1, 1 ) = VAL( ITR )
                              KNT = KNT + 1
                              CALL DLASY2( LTRANL, LTRANR, ISGN, N1, N2,
     $                                     TL, 2, TR, 2, B, 2, SCALE, X,
     $                                     2, XNORM, INFO )
                              IF( INFO.NE.0 )
     $                           NINFO = NINFO + 1
                              IF( LTRANL ) THEN
                                 TMP = TL( 1, 2 )
                                 TL( 1, 2 ) = TL( 2, 1 )
                                 TL( 2, 1 ) = TMP
                              END IF
                              RES = ABS( ( TL( 1, 1 )+SGN*TR( 1, 1 ) )*
     $                              X( 1, 1 )+TL( 1, 2 )*X( 2, 1 )-
     $                              SCALE*B( 1, 1 ) )
                              RES = RES + ABS( ( TL( 2, 2 )+SGN*TR( 1,
     $                              1 ) )*X( 2, 1 )+TL( 2, 1 )*
     $                              X( 1, 1 )-SCALE*B( 2, 1 ) )
                              TNRM = ABS( TR( 1, 1 ) ) +
     $                               ABS( TL( 1, 1 ) ) +
     $                               ABS( TL( 1, 2 ) ) +
     $                               ABS( TL( 2, 1 ) ) +
     $                               ABS( TL( 2, 2 ) )
                              XNRM = MAX( ABS( X( 1, 1 ) ),
     $                               ABS( X( 2, 1 ) ) )
                              DEN = MAX( SMLNUM, SMLNUM*XNRM,
     $                              ( TNRM*EPS )*XNRM )
                              RES = RES / DEN
                              IF( SCALE.GT.ONE )
     $                           RES = RES + ONE / EPS
                              RES = RES + ABS( XNORM-XNRM ) /
     $                              MAX( SMLNUM, XNORM ) / EPS
                              IF( RES.GT.RMAX ) THEN
                                 LMAX = KNT
                                 RMAX = RES
                              END IF
   40                      CONTINUE
   50                   CONTINUE
   60                CONTINUE
   70             CONTINUE
   80          CONTINUE
*
               N1 = 1
               N2 = 2
               DO 130 ITR = 1, 8
                  DO 120 ITRSCL = 1, 3
                     DO 110 ITL = 1, 3
                        DO 100 IB1 = 1, 3
                           DO 90 IB2 = 1, 3
                              B( 1, 1 ) = VAL( IB1 )
                              B( 1, 2 ) = -TWO*VAL( IB2 )
                              TR( 1, 1 ) = ITVAL( 1, 1, ITR )*
     $                                     VAL( ITRSCL )
                              TR( 2, 1 ) = ITVAL( 2, 1, ITR )*
     $                                     VAL( ITRSCL )
                              TR( 1, 2 ) = ITVAL( 1, 2, ITR )*
     $                                     VAL( ITRSCL )
                              TR( 2, 2 ) = ITVAL( 2, 2, ITR )*
     $                                     VAL( ITRSCL )
                              TL( 1, 1 ) = VAL( ITL )
                              KNT = KNT + 1
                              CALL DLASY2( LTRANL, LTRANR, ISGN, N1, N2,
     $                                     TL, 2, TR, 2, B, 2, SCALE, X,
     $                                     2, XNORM, INFO )
                              IF( INFO.NE.0 )
     $                           NINFO = NINFO + 1
                              IF( LTRANR ) THEN
                                 TMP = TR( 1, 2 )
                                 TR( 1, 2 ) = TR( 2, 1 )
                                 TR( 2, 1 ) = TMP
                              END IF
                              TNRM = ABS( TL( 1, 1 ) ) +
     $                               ABS( TR( 1, 1 ) ) +
     $                               ABS( TR( 1, 2 ) ) +
     $                               ABS( TR( 2, 2 ) ) +
     $                               ABS( TR( 2, 1 ) )
                              XNRM = ABS( X( 1, 1 ) ) + ABS( X( 1, 2 ) )
                              RES = ABS( ( ( TL( 1, 1 )+SGN*TR( 1,
     $                              1 ) ) )*( X( 1, 1 ) )+
     $                              ( SGN*TR( 2, 1 ) )*( X( 1, 2 ) )-
     $                              ( SCALE*B( 1, 1 ) ) )
                              RES = RES + ABS( ( ( TL( 1, 1 )+SGN*TR( 2,
     $                              2 ) ) )*( X( 1, 2 ) )+
     $                              ( SGN*TR( 1, 2 ) )*( X( 1, 1 ) )-
     $                              ( SCALE*B( 1, 2 ) ) )
                              DEN = MAX( SMLNUM, SMLNUM*XNRM,
     $                              ( TNRM*EPS )*XNRM )
                              RES = RES / DEN
                              IF( SCALE.GT.ONE )
     $                           RES = RES + ONE / EPS
                              RES = RES + ABS( XNORM-XNRM ) /
     $                              MAX( SMLNUM, XNORM ) / EPS
                              IF( RES.GT.RMAX ) THEN
                                 LMAX = KNT
                                 RMAX = RES
                              END IF
   90                      CONTINUE
  100                   CONTINUE
  110                CONTINUE
  120             CONTINUE
  130          CONTINUE
*
               N1 = 2
               N2 = 2
               DO 200 ITR = 1, 8
                  DO 190 ITRSCL = 1, 3
                     DO 180 ITL = 1, 8
                        DO 170 ITLSCL = 1, 3
                           DO 160 IB1 = 1, 3
                              DO 150 IB2 = 1, 3
                                 DO 140 IB3 = 1, 3
                                    B( 1, 1 ) = VAL( IB1 )
                                    B( 2, 1 ) = -FOUR*VAL( IB2 )
                                    B( 1, 2 ) = -TWO*VAL( IB3 )
                                    B( 2, 2 ) = EIGHT*
     $                                          MIN( VAL( IB1 ), VAL
     $                                          ( IB2 ), VAL( IB3 ) )
                                    TR( 1, 1 ) = ITVAL( 1, 1, ITR )*
     $                                           VAL( ITRSCL )
                                    TR( 2, 1 ) = ITVAL( 2, 1, ITR )*
     $                                           VAL( ITRSCL )
                                    TR( 1, 2 ) = ITVAL( 1, 2, ITR )*
     $                                           VAL( ITRSCL )
                                    TR( 2, 2 ) = ITVAL( 2, 2, ITR )*
     $                                           VAL( ITRSCL )
                                    TL( 1, 1 ) = ITVAL( 1, 1, ITL )*
     $                                           VAL( ITLSCL )
                                    TL( 2, 1 ) = ITVAL( 2, 1, ITL )*
     $                                           VAL( ITLSCL )
                                    TL( 1, 2 ) = ITVAL( 1, 2, ITL )*
     $                                           VAL( ITLSCL )
                                    TL( 2, 2 ) = ITVAL( 2, 2, ITL )*
     $                                           VAL( ITLSCL )
                                    KNT = KNT + 1
                                    CALL DLASY2( LTRANL, LTRANR, ISGN,
     $                                           N1, N2, TL, 2, TR, 2,
     $                                           B, 2, SCALE, X, 2,
     $                                           XNORM, INFO )
                                    IF( INFO.NE.0 )
     $                                 NINFO = NINFO + 1
                                    IF( LTRANR ) THEN
                                       TMP = TR( 1, 2 )
                                       TR( 1, 2 ) = TR( 2, 1 )
                                       TR( 2, 1 ) = TMP
                                    END IF
                                    IF( LTRANL ) THEN
                                       TMP = TL( 1, 2 )
                                       TL( 1, 2 ) = TL( 2, 1 )
                                       TL( 2, 1 ) = TMP
                                    END IF
                                    TNRM = ABS( TR( 1, 1 ) ) +
     $                                     ABS( TR( 2, 1 ) ) +
     $                                     ABS( TR( 1, 2 ) ) +
     $                                     ABS( TR( 2, 2 ) ) +
     $                                     ABS( TL( 1, 1 ) ) +
     $                                     ABS( TL( 2, 1 ) ) +
     $                                     ABS( TL( 1, 2 ) ) +
     $                                     ABS( TL( 2, 2 ) )
                                    XNRM = MAX( ABS( X( 1, 1 ) )+
     $                                     ABS( X( 1, 2 ) ),
     $                                     ABS( X( 2, 1 ) )+
     $                                     ABS( X( 2, 2 ) ) )
                                    RES = ABS( ( ( TL( 1, 1 )+SGN*TR( 1,
     $                                    1 ) ) )*( X( 1, 1 ) )+
     $                                    ( SGN*TR( 2, 1 ) )*
     $                                    ( X( 1, 2 ) )+( TL( 1, 2 ) )*
     $                                    ( X( 2, 1 ) )-
     $                                    ( SCALE*B( 1, 1 ) ) )
                                    RES = RES + ABS( ( TL( 1, 1 ) )*
     $                                    ( X( 1, 2 ) )+
     $                                    ( SGN*TR( 1, 2 ) )*
     $                                    ( X( 1, 1 ) )+
     $                                    ( SGN*TR( 2, 2 ) )*
     $                                    ( X( 1, 2 ) )+( TL( 1, 2 ) )*
     $                                    ( X( 2, 2 ) )-
     $                                    ( SCALE*B( 1, 2 ) ) )
                                    RES = RES + ABS( ( TL( 2, 1 ) )*
     $                                    ( X( 1, 1 ) )+
     $                                    ( SGN*TR( 1, 1 ) )*
     $                                    ( X( 2, 1 ) )+
     $                                    ( SGN*TR( 2, 1 ) )*
     $                                    ( X( 2, 2 ) )+( TL( 2, 2 ) )*
     $                                    ( X( 2, 1 ) )-
     $                                    ( SCALE*B( 2, 1 ) ) )
                                    RES = RES + ABS( ( ( TL( 2,
     $                                    2 )+SGN*TR( 2, 2 ) ) )*
     $                                    ( X( 2, 2 ) )+
     $                                    ( SGN*TR( 1, 2 ) )*
     $                                    ( X( 2, 1 ) )+( TL( 2, 1 ) )*
     $                                    ( X( 1, 2 ) )-
     $                                    ( SCALE*B( 2, 2 ) ) )
                                    DEN = MAX( SMLNUM, SMLNUM*XNRM,
     $                                    ( TNRM*EPS )*XNRM )
                                    RES = RES / DEN
                                    IF( SCALE.GT.ONE )
     $                                 RES = RES + ONE / EPS
                                    RES = RES + ABS( XNORM-XNRM ) /
     $                                    MAX( SMLNUM, XNORM ) / EPS
                                    IF( RES.GT.RMAX ) THEN
                                       LMAX = KNT
                                       RMAX = RES
                                    END IF
  140                            CONTINUE
  150                         CONTINUE
  160                      CONTINUE
  170                   CONTINUE
  180                CONTINUE
  190             CONTINUE
  200          CONTINUE
  210       CONTINUE
  220    CONTINUE
  230 CONTINUE
*
      RETURN
*
*     End of DGET32
*
      END
