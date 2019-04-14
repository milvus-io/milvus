*> \brief \b ZGET35
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGET35( RMAX, LMAX, NINFO, KNT, NIN )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, LMAX, NIN, NINFO
*       DOUBLE PRECISION   RMAX
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGET35 tests ZTRSYL, a routine for solving the Sylvester matrix
*> equation
*>
*>    op(A)*X + ISGN*X*op(B) = scale*C,
*>
*> A and B are assumed to be in Schur canonical form, op() represents an
*> optional transpose, and ISGN can be -1 or +1.  Scale is an output
*> less than or equal to 1, chosen to avoid overflow in X.
*>
*> The test code verifies that the following residual is order 1:
*>
*>    norm(op(A)*X + ISGN*X*op(B) - scale*C) /
*>        (EPS*max(norm(A),norm(B))*norm(X))
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
*>          Number of examples where INFO is nonzero.
*> \endverbatim
*>
*> \param[out] KNT
*> \verbatim
*>          KNT is INTEGER
*>          Total number of examples tested.
*> \endverbatim
*>
*> \param[in] NIN
*> \verbatim
*>          NIN is INTEGER
*>          Input logical unit number.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZGET35( RMAX, LMAX, NINFO, KNT, NIN )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KNT, LMAX, NIN, NINFO
      DOUBLE PRECISION   RMAX
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            LDT
      PARAMETER          ( LDT = 10 )
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TWO = 2.0D0 )
      DOUBLE PRECISION   LARGE
      PARAMETER          ( LARGE = 1.0D6 )
      COMPLEX*16         CONE
      PARAMETER          ( CONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      CHARACTER          TRANA, TRANB
      INTEGER            I, IMLA, IMLAD, IMLB, IMLC, INFO, ISGN, ITRANA,
     $                   ITRANB, J, M, N
      DOUBLE PRECISION   BIGNUM, EPS, RES, RES1, SCALE, SMLNUM, TNRM,
     $                   XNRM
      COMPLEX*16         RMUL
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   DUM( 1 ), VM1( 3 ), VM2( 3 )
      COMPLEX*16         A( LDT, LDT ), ATMP( LDT, LDT ), B( LDT, LDT ),
     $                   BTMP( LDT, LDT ), C( LDT, LDT ),
     $                   CSAV( LDT, LDT ), CTMP( LDT, LDT )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, ZGEMM, ZTRSYL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, SQRT
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
      VM1( 1 ) = SQRT( SMLNUM )
      VM1( 2 ) = ONE
      VM1( 3 ) = LARGE
      VM2( 1 ) = ONE
      VM2( 2 ) = ONE + TWO*EPS
      VM2( 3 ) = TWO
*
      KNT = 0
      NINFO = 0
      LMAX = 0
      RMAX = ZERO
*
*     Begin test loop
*
   10 CONTINUE
      READ( NIN, FMT = * )M, N
      IF( N.EQ.0 )
     $   RETURN
      DO 20 I = 1, M
         READ( NIN, FMT = * )( ATMP( I, J ), J = 1, M )
   20 CONTINUE
      DO 30 I = 1, N
         READ( NIN, FMT = * )( BTMP( I, J ), J = 1, N )
   30 CONTINUE
      DO 40 I = 1, M
         READ( NIN, FMT = * )( CTMP( I, J ), J = 1, N )
   40 CONTINUE
      DO 170 IMLA = 1, 3
         DO 160 IMLAD = 1, 3
            DO 150 IMLB = 1, 3
               DO 140 IMLC = 1, 3
                  DO 130 ITRANA = 1, 2
                     DO 120 ITRANB = 1, 2
                        DO 110 ISGN = -1, 1, 2
                           IF( ITRANA.EQ.1 )
     $                        TRANA = 'N'
                           IF( ITRANA.EQ.2 )
     $                        TRANA = 'C'
                           IF( ITRANB.EQ.1 )
     $                        TRANB = 'N'
                           IF( ITRANB.EQ.2 )
     $                        TRANB = 'C'
                           TNRM = ZERO
                           DO 60 I = 1, M
                              DO 50 J = 1, M
                                 A( I, J ) = ATMP( I, J )*VM1( IMLA )
                                 TNRM = MAX( TNRM, ABS( A( I, J ) ) )
   50                         CONTINUE
                              A( I, I ) = A( I, I )*VM2( IMLAD )
                              TNRM = MAX( TNRM, ABS( A( I, I ) ) )
   60                      CONTINUE
                           DO 80 I = 1, N
                              DO 70 J = 1, N
                                 B( I, J ) = BTMP( I, J )*VM1( IMLB )
                                 TNRM = MAX( TNRM, ABS( B( I, J ) ) )
   70                         CONTINUE
   80                      CONTINUE
                           IF( TNRM.EQ.ZERO )
     $                        TNRM = ONE
                           DO 100 I = 1, M
                              DO 90 J = 1, N
                                 C( I, J ) = CTMP( I, J )*VM1( IMLC )
                                 CSAV( I, J ) = C( I, J )
   90                         CONTINUE
  100                      CONTINUE
                           KNT = KNT + 1
                           CALL ZTRSYL( TRANA, TRANB, ISGN, M, N, A,
     $                                  LDT, B, LDT, C, LDT, SCALE,
     $                                  INFO )
                           IF( INFO.NE.0 )
     $                        NINFO = NINFO + 1
                           XNRM = ZLANGE( 'M', M, N, C, LDT, DUM )
                           RMUL = CONE
                           IF( XNRM.GT.ONE .AND. TNRM.GT.ONE ) THEN
                              IF( XNRM.GT.BIGNUM / TNRM ) THEN
                                 RMUL = MAX( XNRM, TNRM )
                                 RMUL = CONE / RMUL
                              END IF
                           END IF
                           CALL ZGEMM( TRANA, 'N', M, N, M, RMUL, A,
     $                                 LDT, C, LDT, -SCALE*RMUL, CSAV,
     $                                 LDT )
                           CALL ZGEMM( 'N', TRANB, M, N, N,
     $                                 DBLE( ISGN )*RMUL, C, LDT, B,
     $                                 LDT, CONE, CSAV, LDT )
                           RES1 = ZLANGE( 'M', M, N, CSAV, LDT, DUM )
                           RES = RES1 / MAX( SMLNUM, SMLNUM*XNRM,
     $                           ( ( ABS( RMUL )*TNRM )*EPS )*XNRM )
                           IF( RES.GT.RMAX ) THEN
                              LMAX = KNT
                              RMAX = RES
                           END IF
  110                   CONTINUE
  120                CONTINUE
  130             CONTINUE
  140          CONTINUE
  150       CONTINUE
  160    CONTINUE
  170 CONTINUE
      GO TO 10
*
*     End of ZGET35
*
      END
