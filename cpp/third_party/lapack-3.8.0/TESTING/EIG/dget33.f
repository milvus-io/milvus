*> \brief \b DGET33
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET33( RMAX, LMAX, NINFO, KNT )
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
*> DGET33 tests DLANV2, a routine for putting 2 by 2 blocks into
*> standard form.  In other words, it computes a two by two rotation
*> [[C,S];[-S,C]] where in
*>
*>    [ C S ][T(1,1) T(1,2)][ C -S ] = [ T11 T12 ]
*>    [-S C ][T(2,1) T(2,2)][ S  C ]   [ T21 T22 ]
*>
*> either
*>    1) T21=0 (real eigenvalues), or
*>    2) T11=T22 and T21*T12<0 (complex conjugate eigenvalues).
*> We also  verify that the residual is small.
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
*>          Number of examples returned with INFO .NE. 0.
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
      SUBROUTINE DGET33( RMAX, LMAX, NINFO, KNT )
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
      DOUBLE PRECISION   TWO, FOUR
      PARAMETER          ( TWO = 2.0D0, FOUR = 4.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I1, I2, I3, I4, IM1, IM2, IM3, IM4, J1, J2, J3
      DOUBLE PRECISION   BIGNUM, CS, EPS, RES, SMLNUM, SN, SUM, TNRM,
     $                   WI1, WI2, WR1, WR2
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   Q( 2, 2 ), T( 2, 2 ), T1( 2, 2 ), T2( 2, 2 ),
     $                   VAL( 4 ), VM( 3 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLANV2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SIGN
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
      VAL( 1 ) = ONE
      VAL( 2 ) = ONE + TWO*EPS
      VAL( 3 ) = TWO
      VAL( 4 ) = TWO - FOUR*EPS
      VM( 1 ) = SMLNUM
      VM( 2 ) = ONE
      VM( 3 ) = BIGNUM
*
      KNT = 0
      NINFO = 0
      LMAX = 0
      RMAX = ZERO
*
*     Begin test loop
*
      DO 150 I1 = 1, 4
         DO 140 I2 = 1, 4
            DO 130 I3 = 1, 4
               DO 120 I4 = 1, 4
                  DO 110 IM1 = 1, 3
                     DO 100 IM2 = 1, 3
                        DO 90 IM3 = 1, 3
                           DO 80 IM4 = 1, 3
                              T( 1, 1 ) = VAL( I1 )*VM( IM1 )
                              T( 1, 2 ) = VAL( I2 )*VM( IM2 )
                              T( 2, 1 ) = -VAL( I3 )*VM( IM3 )
                              T( 2, 2 ) = VAL( I4 )*VM( IM4 )
                              TNRM = MAX( ABS( T( 1, 1 ) ),
     $                               ABS( T( 1, 2 ) ), ABS( T( 2, 1 ) ),
     $                               ABS( T( 2, 2 ) ) )
                              T1( 1, 1 ) = T( 1, 1 )
                              T1( 1, 2 ) = T( 1, 2 )
                              T1( 2, 1 ) = T( 2, 1 )
                              T1( 2, 2 ) = T( 2, 2 )
                              Q( 1, 1 ) = ONE
                              Q( 1, 2 ) = ZERO
                              Q( 2, 1 ) = ZERO
                              Q( 2, 2 ) = ONE
*
                              CALL DLANV2( T( 1, 1 ), T( 1, 2 ),
     $                                     T( 2, 1 ), T( 2, 2 ), WR1,
     $                                     WI1, WR2, WI2, CS, SN )
                              DO 10 J1 = 1, 2
                                 RES = Q( J1, 1 )*CS + Q( J1, 2 )*SN
                                 Q( J1, 2 ) = -Q( J1, 1 )*SN +
     $                                        Q( J1, 2 )*CS
                                 Q( J1, 1 ) = RES
   10                         CONTINUE
*
                              RES = ZERO
                              RES = RES + ABS( Q( 1, 1 )**2+
     $                              Q( 1, 2 )**2-ONE ) / EPS
                              RES = RES + ABS( Q( 2, 2 )**2+
     $                              Q( 2, 1 )**2-ONE ) / EPS
                              RES = RES + ABS( Q( 1, 1 )*Q( 2, 1 )+
     $                              Q( 1, 2 )*Q( 2, 2 ) ) / EPS
                              DO 40 J1 = 1, 2
                                 DO 30 J2 = 1, 2
                                    T2( J1, J2 ) = ZERO
                                    DO 20 J3 = 1, 2
                                       T2( J1, J2 ) = T2( J1, J2 ) +
     $                                                T1( J1, J3 )*
     $                                                Q( J3, J2 )
   20                               CONTINUE
   30                            CONTINUE
   40                         CONTINUE
                              DO 70 J1 = 1, 2
                                 DO 60 J2 = 1, 2
                                    SUM = T( J1, J2 )
                                    DO 50 J3 = 1, 2
                                       SUM = SUM - Q( J3, J1 )*
     $                                       T2( J3, J2 )
   50                               CONTINUE
                                    RES = RES + ABS( SUM ) / EPS / TNRM
   60                            CONTINUE
   70                         CONTINUE
                              IF( T( 2, 1 ).NE.ZERO .AND.
     $                            ( T( 1, 1 ).NE.T( 2,
     $                            2 ) .OR. SIGN( ONE, T( 1,
     $                            2 ) )*SIGN( ONE, T( 2,
     $                            1 ) ).GT.ZERO ) )RES = RES + ONE / EPS
                              KNT = KNT + 1
                              IF( RES.GT.RMAX ) THEN
                                 LMAX = KNT
                                 RMAX = RES
                              END IF
   80                      CONTINUE
   90                   CONTINUE
  100                CONTINUE
  110             CONTINUE
  120          CONTINUE
  130       CONTINUE
  140    CONTINUE
  150 CONTINUE
*
      RETURN
*
*     End of DGET33
*
      END
