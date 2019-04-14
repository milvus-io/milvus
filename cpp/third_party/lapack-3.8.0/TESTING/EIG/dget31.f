*> \brief \b DGET31
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET31( RMAX, LMAX, NINFO, KNT )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, LMAX
*       DOUBLE PRECISION   RMAX
*       ..
*       .. Array Arguments ..
*       INTEGER            NINFO( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGET31 tests DLALN2, a routine for solving
*>
*>    (ca A - w D)X = sB
*>
*> where A is an NA by NA matrix (NA=1 or 2 only), w is a real (NW=1) or
*> complex (NW=2) constant, ca is a real constant, D is an NA by NA real
*> diagonal matrix, and B is an NA by NW matrix (when NW=2 the second
*> column of B contains the imaginary part of the solution).  The code
*> returns X and s, where s is a scale factor, less than or equal to 1,
*> which is chosen to avoid overflow in X.
*>
*> If any singular values of ca A-w D are less than another input
*> parameter SMIN, they are perturbed up to SMIN.
*>
*> The test condition is that the scaled residual
*>
*>     norm( (ca A-w D)*X - s*B ) /
*>           ( max( ulp*norm(ca A-w D), SMIN )*norm(X) )
*>
*> should be on the order of 1.  Here, ulp is the machine precision.
*> Also, it is verified that SCALE is less than or equal to 1, and that
*> XNORM = infinity-norm(X).
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
*>          NINFO is INTEGER array, dimension (3)
*>          NINFO(1) = number of examples with INFO less than 0
*>          NINFO(2) = number of examples with INFO greater than 0
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
      SUBROUTINE DGET31( RMAX, LMAX, NINFO, KNT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KNT, LMAX
      DOUBLE PRECISION   RMAX
*     ..
*     .. Array Arguments ..
      INTEGER            NINFO( 2 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0D0, HALF = 0.5D0, ONE = 1.0D0 )
      DOUBLE PRECISION   TWO, THREE, FOUR
      PARAMETER          ( TWO = 2.0D0, THREE = 3.0D0, FOUR = 4.0D0 )
      DOUBLE PRECISION   SEVEN, TEN
      PARAMETER          ( SEVEN = 7.0D0, TEN = 10.0D0 )
      DOUBLE PRECISION   TWNONE
      PARAMETER          ( TWNONE = 21.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            IA, IB, ICA, ID1, ID2, INFO, ISMIN, ITRANS,
     $                   IWI, IWR, NA, NW
      DOUBLE PRECISION   BIGNUM, CA, D1, D2, DEN, EPS, RES, SCALE, SMIN,
     $                   SMLNUM, TMP, UNFL, WI, WR, XNORM
*     ..
*     .. Local Arrays ..
      LOGICAL            LTRANS( 0: 1 )
      DOUBLE PRECISION   A( 2, 2 ), B( 2, 2 ), VAB( 3 ), VCA( 5 ),
     $                   VDD( 4 ), VSMIN( 4 ), VWI( 4 ), VWR( 4 ),
     $                   X( 2, 2 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLALN2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Data statements ..
      DATA               LTRANS / .FALSE., .TRUE. /
*     ..
*     .. Executable Statements ..
*
*     Get machine parameters
*
      EPS = DLAMCH( 'P' )
      UNFL = DLAMCH( 'U' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
*
*     Set up test case parameters
*
      VSMIN( 1 ) = SMLNUM
      VSMIN( 2 ) = EPS
      VSMIN( 3 ) = ONE / ( TEN*TEN )
      VSMIN( 4 ) = ONE / EPS
      VAB( 1 ) = SQRT( SMLNUM )
      VAB( 2 ) = ONE
      VAB( 3 ) = SQRT( BIGNUM )
      VWR( 1 ) = ZERO
      VWR( 2 ) = HALF
      VWR( 3 ) = TWO
      VWR( 4 ) = ONE
      VWI( 1 ) = SMLNUM
      VWI( 2 ) = EPS
      VWI( 3 ) = ONE
      VWI( 4 ) = TWO
      VDD( 1 ) = SQRT( SMLNUM )
      VDD( 2 ) = ONE
      VDD( 3 ) = TWO
      VDD( 4 ) = SQRT( BIGNUM )
      VCA( 1 ) = ZERO
      VCA( 2 ) = SQRT( SMLNUM )
      VCA( 3 ) = EPS
      VCA( 4 ) = HALF
      VCA( 5 ) = ONE
*
      KNT = 0
      NINFO( 1 ) = 0
      NINFO( 2 ) = 0
      LMAX = 0
      RMAX = ZERO
*
*     Begin test loop
*
      DO 190 ID1 = 1, 4
         D1 = VDD( ID1 )
         DO 180 ID2 = 1, 4
            D2 = VDD( ID2 )
            DO 170 ICA = 1, 5
               CA = VCA( ICA )
               DO 160 ITRANS = 0, 1
                  DO 150 ISMIN = 1, 4
                     SMIN = VSMIN( ISMIN )
*
                     NA = 1
                     NW = 1
                     DO 30 IA = 1, 3
                        A( 1, 1 ) = VAB( IA )
                        DO 20 IB = 1, 3
                           B( 1, 1 ) = VAB( IB )
                           DO 10 IWR = 1, 4
                              IF( D1.EQ.ONE .AND. D2.EQ.ONE .AND. CA.EQ.
     $                            ONE ) THEN
                                 WR = VWR( IWR )*A( 1, 1 )
                              ELSE
                                 WR = VWR( IWR )
                              END IF
                              WI = ZERO
                              CALL DLALN2( LTRANS( ITRANS ), NA, NW,
     $                                     SMIN, CA, A, 2, D1, D2, B, 2,
     $                                     WR, WI, X, 2, SCALE, XNORM,
     $                                     INFO )
                              IF( INFO.LT.0 )
     $                           NINFO( 1 ) = NINFO( 1 ) + 1
                              IF( INFO.GT.0 )
     $                           NINFO( 2 ) = NINFO( 2 ) + 1
                              RES = ABS( ( CA*A( 1, 1 )-WR*D1 )*
     $                              X( 1, 1 )-SCALE*B( 1, 1 ) )
                              IF( INFO.EQ.0 ) THEN
                                 DEN = MAX( EPS*( ABS( ( CA*A( 1,
     $                                 1 )-WR*D1 )*X( 1, 1 ) ) ),
     $                                 SMLNUM )
                              ELSE
                                 DEN = MAX( SMIN*ABS( X( 1, 1 ) ),
     $                                 SMLNUM )
                              END IF
                              RES = RES / DEN
                              IF( ABS( X( 1, 1 ) ).LT.UNFL .AND.
     $                            ABS( B( 1, 1 ) ).LE.SMLNUM*
     $                            ABS( CA*A( 1, 1 )-WR*D1 ) )RES = ZERO
                              IF( SCALE.GT.ONE )
     $                           RES = RES + ONE / EPS
                              RES = RES + ABS( XNORM-ABS( X( 1, 1 ) ) )
     $                               / MAX( SMLNUM, XNORM ) / EPS
                              IF( INFO.NE.0 .AND. INFO.NE.1 )
     $                           RES = RES + ONE / EPS
                              KNT = KNT + 1
                              IF( RES.GT.RMAX ) THEN
                                 LMAX = KNT
                                 RMAX = RES
                              END IF
   10                      CONTINUE
   20                   CONTINUE
   30                CONTINUE
*
                     NA = 1
                     NW = 2
                     DO 70 IA = 1, 3
                        A( 1, 1 ) = VAB( IA )
                        DO 60 IB = 1, 3
                           B( 1, 1 ) = VAB( IB )
                           B( 1, 2 ) = -HALF*VAB( IB )
                           DO 50 IWR = 1, 4
                              IF( D1.EQ.ONE .AND. D2.EQ.ONE .AND. CA.EQ.
     $                            ONE ) THEN
                                 WR = VWR( IWR )*A( 1, 1 )
                              ELSE
                                 WR = VWR( IWR )
                              END IF
                              DO 40 IWI = 1, 4
                                 IF( D1.EQ.ONE .AND. D2.EQ.ONE .AND.
     $                               CA.EQ.ONE ) THEN
                                    WI = VWI( IWI )*A( 1, 1 )
                                 ELSE
                                    WI = VWI( IWI )
                                 END IF
                                 CALL DLALN2( LTRANS( ITRANS ), NA, NW,
     $                                        SMIN, CA, A, 2, D1, D2, B,
     $                                        2, WR, WI, X, 2, SCALE,
     $                                        XNORM, INFO )
                                 IF( INFO.LT.0 )
     $                              NINFO( 1 ) = NINFO( 1 ) + 1
                                 IF( INFO.GT.0 )
     $                              NINFO( 2 ) = NINFO( 2 ) + 1
                                 RES = ABS( ( CA*A( 1, 1 )-WR*D1 )*
     $                                 X( 1, 1 )+( WI*D1 )*X( 1, 2 )-
     $                                 SCALE*B( 1, 1 ) )
                                 RES = RES + ABS( ( -WI*D1 )*X( 1, 1 )+
     $                                 ( CA*A( 1, 1 )-WR*D1 )*X( 1, 2 )-
     $                                 SCALE*B( 1, 2 ) )
                                 IF( INFO.EQ.0 ) THEN
                                    DEN = MAX( EPS*( MAX( ABS( CA*A( 1,
     $                                    1 )-WR*D1 ), ABS( D1*WI ) )*
     $                                    ( ABS( X( 1, 1 ) )+ABS( X( 1,
     $                                    2 ) ) ) ), SMLNUM )
                                 ELSE
                                    DEN = MAX( SMIN*( ABS( X( 1,
     $                                    1 ) )+ABS( X( 1, 2 ) ) ),
     $                                    SMLNUM )
                                 END IF
                                 RES = RES / DEN
                                 IF( ABS( X( 1, 1 ) ).LT.UNFL .AND.
     $                               ABS( X( 1, 2 ) ).LT.UNFL .AND.
     $                               ABS( B( 1, 1 ) ).LE.SMLNUM*
     $                               ABS( CA*A( 1, 1 )-WR*D1 ) )
     $                               RES = ZERO
                                 IF( SCALE.GT.ONE )
     $                              RES = RES + ONE / EPS
                                 RES = RES + ABS( XNORM-
     $                                 ABS( X( 1, 1 ) )-
     $                                 ABS( X( 1, 2 ) ) ) /
     $                                 MAX( SMLNUM, XNORM ) / EPS
                                 IF( INFO.NE.0 .AND. INFO.NE.1 )
     $                              RES = RES + ONE / EPS
                                 KNT = KNT + 1
                                 IF( RES.GT.RMAX ) THEN
                                    LMAX = KNT
                                    RMAX = RES
                                 END IF
   40                         CONTINUE
   50                      CONTINUE
   60                   CONTINUE
   70                CONTINUE
*
                     NA = 2
                     NW = 1
                     DO 100 IA = 1, 3
                        A( 1, 1 ) = VAB( IA )
                        A( 1, 2 ) = -THREE*VAB( IA )
                        A( 2, 1 ) = -SEVEN*VAB( IA )
                        A( 2, 2 ) = TWNONE*VAB( IA )
                        DO 90 IB = 1, 3
                           B( 1, 1 ) = VAB( IB )
                           B( 2, 1 ) = -TWO*VAB( IB )
                           DO 80 IWR = 1, 4
                              IF( D1.EQ.ONE .AND. D2.EQ.ONE .AND. CA.EQ.
     $                            ONE ) THEN
                                 WR = VWR( IWR )*A( 1, 1 )
                              ELSE
                                 WR = VWR( IWR )
                              END IF
                              WI = ZERO
                              CALL DLALN2( LTRANS( ITRANS ), NA, NW,
     $                                     SMIN, CA, A, 2, D1, D2, B, 2,
     $                                     WR, WI, X, 2, SCALE, XNORM,
     $                                     INFO )
                              IF( INFO.LT.0 )
     $                           NINFO( 1 ) = NINFO( 1 ) + 1
                              IF( INFO.GT.0 )
     $                           NINFO( 2 ) = NINFO( 2 ) + 1
                              IF( ITRANS.EQ.1 ) THEN
                                 TMP = A( 1, 2 )
                                 A( 1, 2 ) = A( 2, 1 )
                                 A( 2, 1 ) = TMP
                              END IF
                              RES = ABS( ( CA*A( 1, 1 )-WR*D1 )*
     $                              X( 1, 1 )+( CA*A( 1, 2 ) )*
     $                              X( 2, 1 )-SCALE*B( 1, 1 ) )
                              RES = RES + ABS( ( CA*A( 2, 1 ) )*
     $                              X( 1, 1 )+( CA*A( 2, 2 )-WR*D2 )*
     $                              X( 2, 1 )-SCALE*B( 2, 1 ) )
                              IF( INFO.EQ.0 ) THEN
                                 DEN = MAX( EPS*( MAX( ABS( CA*A( 1,
     $                                 1 )-WR*D1 )+ABS( CA*A( 1, 2 ) ),
     $                                 ABS( CA*A( 2, 1 ) )+ABS( CA*A( 2,
     $                                 2 )-WR*D2 ) )*MAX( ABS( X( 1,
     $                                 1 ) ), ABS( X( 2, 1 ) ) ) ),
     $                                 SMLNUM )
                              ELSE
                                 DEN = MAX( EPS*( MAX( SMIN / EPS,
     $                                 MAX( ABS( CA*A( 1,
     $                                 1 )-WR*D1 )+ABS( CA*A( 1, 2 ) ),
     $                                 ABS( CA*A( 2, 1 ) )+ABS( CA*A( 2,
     $                                 2 )-WR*D2 ) ) )*MAX( ABS( X( 1,
     $                                 1 ) ), ABS( X( 2, 1 ) ) ) ),
     $                                 SMLNUM )
                              END IF
                              RES = RES / DEN
                              IF( ABS( X( 1, 1 ) ).LT.UNFL .AND.
     $                            ABS( X( 2, 1 ) ).LT.UNFL .AND.
     $                            ABS( B( 1, 1 ) )+ABS( B( 2, 1 ) ).LE.
     $                            SMLNUM*( ABS( CA*A( 1,
     $                            1 )-WR*D1 )+ABS( CA*A( 1,
     $                            2 ) )+ABS( CA*A( 2,
     $                            1 ) )+ABS( CA*A( 2, 2 )-WR*D2 ) ) )
     $                            RES = ZERO
                              IF( SCALE.GT.ONE )
     $                           RES = RES + ONE / EPS
                              RES = RES + ABS( XNORM-
     $                              MAX( ABS( X( 1, 1 ) ), ABS( X( 2,
     $                              1 ) ) ) ) / MAX( SMLNUM, XNORM ) /
     $                              EPS
                              IF( INFO.NE.0 .AND. INFO.NE.1 )
     $                           RES = RES + ONE / EPS
                              KNT = KNT + 1
                              IF( RES.GT.RMAX ) THEN
                                 LMAX = KNT
                                 RMAX = RES
                              END IF
   80                      CONTINUE
   90                   CONTINUE
  100                CONTINUE
*
                     NA = 2
                     NW = 2
                     DO 140 IA = 1, 3
                        A( 1, 1 ) = VAB( IA )*TWO
                        A( 1, 2 ) = -THREE*VAB( IA )
                        A( 2, 1 ) = -SEVEN*VAB( IA )
                        A( 2, 2 ) = TWNONE*VAB( IA )
                        DO 130 IB = 1, 3
                           B( 1, 1 ) = VAB( IB )
                           B( 2, 1 ) = -TWO*VAB( IB )
                           B( 1, 2 ) = FOUR*VAB( IB )
                           B( 2, 2 ) = -SEVEN*VAB( IB )
                           DO 120 IWR = 1, 4
                              IF( D1.EQ.ONE .AND. D2.EQ.ONE .AND. CA.EQ.
     $                            ONE ) THEN
                                 WR = VWR( IWR )*A( 1, 1 )
                              ELSE
                                 WR = VWR( IWR )
                              END IF
                              DO 110 IWI = 1, 4
                                 IF( D1.EQ.ONE .AND. D2.EQ.ONE .AND.
     $                               CA.EQ.ONE ) THEN
                                    WI = VWI( IWI )*A( 1, 1 )
                                 ELSE
                                    WI = VWI( IWI )
                                 END IF
                                 CALL DLALN2( LTRANS( ITRANS ), NA, NW,
     $                                        SMIN, CA, A, 2, D1, D2, B,
     $                                        2, WR, WI, X, 2, SCALE,
     $                                        XNORM, INFO )
                                 IF( INFO.LT.0 )
     $                              NINFO( 1 ) = NINFO( 1 ) + 1
                                 IF( INFO.GT.0 )
     $                              NINFO( 2 ) = NINFO( 2 ) + 1
                                 IF( ITRANS.EQ.1 ) THEN
                                    TMP = A( 1, 2 )
                                    A( 1, 2 ) = A( 2, 1 )
                                    A( 2, 1 ) = TMP
                                 END IF
                                 RES = ABS( ( CA*A( 1, 1 )-WR*D1 )*
     $                                 X( 1, 1 )+( CA*A( 1, 2 ) )*
     $                                 X( 2, 1 )+( WI*D1 )*X( 1, 2 )-
     $                                 SCALE*B( 1, 1 ) )
                                 RES = RES + ABS( ( CA*A( 1,
     $                                 1 )-WR*D1 )*X( 1, 2 )+
     $                                 ( CA*A( 1, 2 ) )*X( 2, 2 )-
     $                                 ( WI*D1 )*X( 1, 1 )-SCALE*
     $                                 B( 1, 2 ) )
                                 RES = RES + ABS( ( CA*A( 2, 1 ) )*
     $                                 X( 1, 1 )+( CA*A( 2, 2 )-WR*D2 )*
     $                                 X( 2, 1 )+( WI*D2 )*X( 2, 2 )-
     $                                 SCALE*B( 2, 1 ) )
                                 RES = RES + ABS( ( CA*A( 2, 1 ) )*
     $                                 X( 1, 2 )+( CA*A( 2, 2 )-WR*D2 )*
     $                                 X( 2, 2 )-( WI*D2 )*X( 2, 1 )-
     $                                 SCALE*B( 2, 2 ) )
                                 IF( INFO.EQ.0 ) THEN
                                    DEN = MAX( EPS*( MAX( ABS( CA*A( 1,
     $                                    1 )-WR*D1 )+ABS( CA*A( 1,
     $                                    2 ) )+ABS( WI*D1 ),
     $                                    ABS( CA*A( 2,
     $                                    1 ) )+ABS( CA*A( 2,
     $                                    2 )-WR*D2 )+ABS( WI*D2 ) )*
     $                                    MAX( ABS( X( 1,
     $                                    1 ) )+ABS( X( 2, 1 ) ),
     $                                    ABS( X( 1, 2 ) )+ABS( X( 2,
     $                                    2 ) ) ) ), SMLNUM )
                                 ELSE
                                    DEN = MAX( EPS*( MAX( SMIN / EPS,
     $                                    MAX( ABS( CA*A( 1,
     $                                    1 )-WR*D1 )+ABS( CA*A( 1,
     $                                    2 ) )+ABS( WI*D1 ),
     $                                    ABS( CA*A( 2,
     $                                    1 ) )+ABS( CA*A( 2,
     $                                    2 )-WR*D2 )+ABS( WI*D2 ) ) )*
     $                                    MAX( ABS( X( 1,
     $                                    1 ) )+ABS( X( 2, 1 ) ),
     $                                    ABS( X( 1, 2 ) )+ABS( X( 2,
     $                                    2 ) ) ) ), SMLNUM )
                                 END IF
                                 RES = RES / DEN
                                 IF( ABS( X( 1, 1 ) ).LT.UNFL .AND.
     $                               ABS( X( 2, 1 ) ).LT.UNFL .AND.
     $                               ABS( X( 1, 2 ) ).LT.UNFL .AND.
     $                               ABS( X( 2, 2 ) ).LT.UNFL .AND.
     $                               ABS( B( 1, 1 ) )+
     $                               ABS( B( 2, 1 ) ).LE.SMLNUM*
     $                               ( ABS( CA*A( 1, 1 )-WR*D1 )+
     $                               ABS( CA*A( 1, 2 ) )+ABS( CA*A( 2,
     $                               1 ) )+ABS( CA*A( 2,
     $                               2 )-WR*D2 )+ABS( WI*D2 )+ABS( WI*
     $                               D1 ) ) )RES = ZERO
                                 IF( SCALE.GT.ONE )
     $                              RES = RES + ONE / EPS
                                 RES = RES + ABS( XNORM-
     $                                 MAX( ABS( X( 1, 1 ) )+ABS( X( 1,
     $                                 2 ) ), ABS( X( 2,
     $                                 1 ) )+ABS( X( 2, 2 ) ) ) ) /
     $                                 MAX( SMLNUM, XNORM ) / EPS
                                 IF( INFO.NE.0 .AND. INFO.NE.1 )
     $                              RES = RES + ONE / EPS
                                 KNT = KNT + 1
                                 IF( RES.GT.RMAX ) THEN
                                    LMAX = KNT
                                    RMAX = RES
                                 END IF
  110                         CONTINUE
  120                      CONTINUE
  130                   CONTINUE
  140                CONTINUE
  150             CONTINUE
  160          CONTINUE
  170       CONTINUE
  180    CONTINUE
  190 CONTINUE
*
      RETURN
*
*     End of DGET31
*
      END
