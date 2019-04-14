*> \brief \b DGET35
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET35( RMAX, LMAX, NINFO, KNT )
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
*> DGET35 tests DTRSYL, a routine for solving the Sylvester matrix
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
      SUBROUTINE DGET35( RMAX, LMAX, NINFO, KNT )
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
      CHARACTER          TRANA, TRANB
      INTEGER            I, IMA, IMB, IMLDA1, IMLDA2, IMLDB1, IMLOFF,
     $                   INFO, ISGN, ITRANA, ITRANB, J, M, N
      DOUBLE PRECISION   BIGNUM, CNRM, EPS, RES, RES1, RMUL, SCALE,
     $                   SMLNUM, TNRM, XNRM
*     ..
*     .. Local Arrays ..
      INTEGER            IDIM( 8 ), IVAL( 6, 6, 8 )
      DOUBLE PRECISION   A( 6, 6 ), B( 6, 6 ), C( 6, 6 ), CC( 6, 6 ),
     $                   DUM( 1 ), VM1( 3 ), VM2( 3 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLABAD, DTRSYL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, SIN, SQRT
*     ..
*     .. Data statements ..
      DATA               IDIM / 1, 2, 3, 4, 3, 3, 6, 4 /
      DATA               IVAL / 1, 35*0, 1, 2, 4*0, -2, 0, 28*0, 1, 5*0,
     $                   5, 1, 2, 3*0, -8, -2, 1, 21*0, 3, 4, 4*0, -5,
     $                   3, 4*0, 1, 2, 1, 4, 2*0, -3, -9, -1, 1, 14*0,
     $                   1, 5*0, 2, 3, 4*0, 5, 6, 7, 21*0, 1, 5*0, 1, 3,
     $                   -4, 3*0, 2, 5, 2, 21*0, 1, 2, 4*0, -2, 0, 4*0,
     $                   5, 6, 3, 4, 2*0, -1, -9, -5, 2, 2*0, 4*8, 5, 6,
     $                   4*9, -7, 5, 1, 5*0, 1, 5, 2, 3*0, 2, -21, 5,
     $                   3*0, 1, 2, 3, 4, 14*0 /
*     ..
*     .. Executable Statements ..
*
*     Get machine parameters
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' )*FOUR / EPS
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
*
*     Set up test case parameters
*
      VM1( 1 ) = SQRT( SMLNUM )
      VM1( 2 ) = ONE
      VM1( 3 ) = SQRT( BIGNUM )
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
      DO 150 ITRANA = 1, 2
         DO 140 ITRANB = 1, 2
            DO 130 ISGN = -1, 1, 2
               DO 120 IMA = 1, 8
                  DO 110 IMLDA1 = 1, 3
                     DO 100 IMLDA2 = 1, 3
                        DO 90 IMLOFF = 1, 2
                           DO 80 IMB = 1, 8
                              DO 70 IMLDB1 = 1, 3
                                 IF( ITRANA.EQ.1 )
     $                              TRANA = 'N'
                                 IF( ITRANA.EQ.2 )
     $                              TRANA = 'T'
                                 IF( ITRANB.EQ.1 )
     $                              TRANB = 'N'
                                 IF( ITRANB.EQ.2 )
     $                              TRANB = 'T'
                                 M = IDIM( IMA )
                                 N = IDIM( IMB )
                                 TNRM = ZERO
                                 DO 20 I = 1, M
                                    DO 10 J = 1, M
                                       A( I, J ) = IVAL( I, J, IMA )
                                       IF( ABS( I-J ).LE.1 ) THEN
                                          A( I, J ) = A( I, J )*
     $                                                VM1( IMLDA1 )
                                          A( I, J ) = A( I, J )*
     $                                                VM2( IMLDA2 )
                                       ELSE
                                          A( I, J ) = A( I, J )*
     $                                                VM1( IMLOFF )
                                       END IF
                                       TNRM = MAX( TNRM,
     $                                        ABS( A( I, J ) ) )
   10                               CONTINUE
   20                            CONTINUE
                                 DO 40 I = 1, N
                                    DO 30 J = 1, N
                                       B( I, J ) = IVAL( I, J, IMB )
                                       IF( ABS( I-J ).LE.1 ) THEN
                                          B( I, J ) = B( I, J )*
     $                                                VM1( IMLDB1 )
                                       ELSE
                                          B( I, J ) = B( I, J )*
     $                                                VM1( IMLOFF )
                                       END IF
                                       TNRM = MAX( TNRM,
     $                                        ABS( B( I, J ) ) )
   30                               CONTINUE
   40                            CONTINUE
                                 CNRM = ZERO
                                 DO 60 I = 1, M
                                    DO 50 J = 1, N
                                       C( I, J ) = SIN( DBLE( I*J ) )
                                       CNRM = MAX( CNRM, C( I, J ) )
                                       CC( I, J ) = C( I, J )
   50                               CONTINUE
   60                            CONTINUE
                                 KNT = KNT + 1
                                 CALL DTRSYL( TRANA, TRANB, ISGN, M, N,
     $                                        A, 6, B, 6, C, 6, SCALE,
     $                                        INFO )
                                 IF( INFO.NE.0 )
     $                              NINFO = NINFO + 1
                                 XNRM = DLANGE( 'M', M, N, C, 6, DUM )
                                 RMUL = ONE
                                 IF( XNRM.GT.ONE .AND. TNRM.GT.ONE )
     $                                THEN
                                    IF( XNRM.GT.BIGNUM / TNRM ) THEN
                                       RMUL = ONE / MAX( XNRM, TNRM )
                                    END IF
                                 END IF
                                 CALL DGEMM( TRANA, 'N', M, N, M, RMUL,
     $                                       A, 6, C, 6, -SCALE*RMUL,
     $                                       CC, 6 )
                                 CALL DGEMM( 'N', TRANB, M, N, N,
     $                                       DBLE( ISGN )*RMUL, C, 6, B,
     $                                       6, ONE, CC, 6 )
                                 RES1 = DLANGE( 'M', M, N, CC, 6, DUM )
                                 RES = RES1 / MAX( SMLNUM, SMLNUM*XNRM,
     $                                 ( ( RMUL*TNRM )*EPS )*XNRM )
                                 IF( RES.GT.RMAX ) THEN
                                    LMAX = KNT
                                    RMAX = RES
                                 END IF
   70                         CONTINUE
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
*     End of DGET35
*
      END
