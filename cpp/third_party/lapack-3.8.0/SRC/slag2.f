*> \brief \b SLAG2 computes the eigenvalues of a 2-by-2 generalized eigenvalue problem, with scaling as necessary to avoid over-/underflow.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAG2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slag2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slag2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slag2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAG2( A, LDA, B, LDB, SAFMIN, SCALE1, SCALE2, WR1,
*                         WR2, WI )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB
*       REAL               SAFMIN, SCALE1, SCALE2, WI, WR1, WR2
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAG2 computes the eigenvalues of a 2 x 2 generalized eigenvalue
*> problem  A - w B, with scaling as necessary to avoid over-/underflow.
*>
*> The scaling factor "s" results in a modified eigenvalue equation
*>
*>     s A - w B
*>
*> where  s  is a non-negative scaling factor chosen so that  w,  w B,
*> and  s A  do not overflow and, if possible, do not underflow, either.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA, 2)
*>          On entry, the 2 x 2 matrix A.  It is assumed that its 1-norm
*>          is less than 1/SAFMIN.  Entries less than
*>          sqrt(SAFMIN)*norm(A) are subject to being treated as zero.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= 2.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB, 2)
*>          On entry, the 2 x 2 upper triangular matrix B.  It is
*>          assumed that the one-norm of B is less than 1/SAFMIN.  The
*>          diagonals should be at least sqrt(SAFMIN) times the largest
*>          element of B (in absolute value); if a diagonal is smaller
*>          than that, then  +/- sqrt(SAFMIN) will be used instead of
*>          that diagonal.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= 2.
*> \endverbatim
*>
*> \param[in] SAFMIN
*> \verbatim
*>          SAFMIN is REAL
*>          The smallest positive number s.t. 1/SAFMIN does not
*>          overflow.  (This should always be SLAMCH('S') -- it is an
*>          argument in order to avoid having to call SLAMCH frequently.)
*> \endverbatim
*>
*> \param[out] SCALE1
*> \verbatim
*>          SCALE1 is REAL
*>          A scaling factor used to avoid over-/underflow in the
*>          eigenvalue equation which defines the first eigenvalue.  If
*>          the eigenvalues are complex, then the eigenvalues are
*>          ( WR1  +/-  WI i ) / SCALE1  (which may lie outside the
*>          exponent range of the machine), SCALE1=SCALE2, and SCALE1
*>          will always be positive.  If the eigenvalues are real, then
*>          the first (real) eigenvalue is  WR1 / SCALE1 , but this may
*>          overflow or underflow, and in fact, SCALE1 may be zero or
*>          less than the underflow threshold if the exact eigenvalue
*>          is sufficiently large.
*> \endverbatim
*>
*> \param[out] SCALE2
*> \verbatim
*>          SCALE2 is REAL
*>          A scaling factor used to avoid over-/underflow in the
*>          eigenvalue equation which defines the second eigenvalue.  If
*>          the eigenvalues are complex, then SCALE2=SCALE1.  If the
*>          eigenvalues are real, then the second (real) eigenvalue is
*>          WR2 / SCALE2 , but this may overflow or underflow, and in
*>          fact, SCALE2 may be zero or less than the underflow
*>          threshold if the exact eigenvalue is sufficiently large.
*> \endverbatim
*>
*> \param[out] WR1
*> \verbatim
*>          WR1 is REAL
*>          If the eigenvalue is real, then WR1 is SCALE1 times the
*>          eigenvalue closest to the (2,2) element of A B**(-1).  If the
*>          eigenvalue is complex, then WR1=WR2 is SCALE1 times the real
*>          part of the eigenvalues.
*> \endverbatim
*>
*> \param[out] WR2
*> \verbatim
*>          WR2 is REAL
*>          If the eigenvalue is real, then WR2 is SCALE2 times the
*>          other eigenvalue.  If the eigenvalue is complex, then
*>          WR1=WR2 is SCALE1 times the real part of the eigenvalues.
*> \endverbatim
*>
*> \param[out] WI
*> \verbatim
*>          WI is REAL
*>          If the eigenvalue is real, then WI is zero.  If the
*>          eigenvalue is complex, then WI is SCALE1 times the imaginary
*>          part of the eigenvalues.  WI will always be non-negative.
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
*> \ingroup realOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE SLAG2( A, LDA, B, LDB, SAFMIN, SCALE1, SCALE2, WR1,
     $                  WR2, WI )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB
      REAL               SAFMIN, SCALE1, SCALE2, WI, WR1, WR2
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0 )
      REAL               HALF
      PARAMETER          ( HALF = ONE / TWO )
      REAL               FUZZY1
      PARAMETER          ( FUZZY1 = ONE+1.0E-5 )
*     ..
*     .. Local Scalars ..
      REAL               A11, A12, A21, A22, ABI22, ANORM, AS11, AS12,
     $                   AS22, ASCALE, B11, B12, B22, BINV11, BINV22,
     $                   BMIN, BNORM, BSCALE, BSIZE, C1, C2, C3, C4, C5,
     $                   DIFF, DISCR, PP, QQ, R, RTMAX, RTMIN, S1, S2,
     $                   SAFMAX, SHIFT, SS, SUM, WABS, WBIG, WDET,
     $                   WSCALE, WSIZE, WSMALL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
      RTMIN = SQRT( SAFMIN )
      RTMAX = ONE / RTMIN
      SAFMAX = ONE / SAFMIN
*
*     Scale A
*
      ANORM = MAX( ABS( A( 1, 1 ) )+ABS( A( 2, 1 ) ),
     $        ABS( A( 1, 2 ) )+ABS( A( 2, 2 ) ), SAFMIN )
      ASCALE = ONE / ANORM
      A11 = ASCALE*A( 1, 1 )
      A21 = ASCALE*A( 2, 1 )
      A12 = ASCALE*A( 1, 2 )
      A22 = ASCALE*A( 2, 2 )
*
*     Perturb B if necessary to insure non-singularity
*
      B11 = B( 1, 1 )
      B12 = B( 1, 2 )
      B22 = B( 2, 2 )
      BMIN = RTMIN*MAX( ABS( B11 ), ABS( B12 ), ABS( B22 ), RTMIN )
      IF( ABS( B11 ).LT.BMIN )
     $   B11 = SIGN( BMIN, B11 )
      IF( ABS( B22 ).LT.BMIN )
     $   B22 = SIGN( BMIN, B22 )
*
*     Scale B
*
      BNORM = MAX( ABS( B11 ), ABS( B12 )+ABS( B22 ), SAFMIN )
      BSIZE = MAX( ABS( B11 ), ABS( B22 ) )
      BSCALE = ONE / BSIZE
      B11 = B11*BSCALE
      B12 = B12*BSCALE
      B22 = B22*BSCALE
*
*     Compute larger eigenvalue by method described by C. van Loan
*
*     ( AS is A shifted by -SHIFT*B )
*
      BINV11 = ONE / B11
      BINV22 = ONE / B22
      S1 = A11*BINV11
      S2 = A22*BINV22
      IF( ABS( S1 ).LE.ABS( S2 ) ) THEN
         AS12 = A12 - S1*B12
         AS22 = A22 - S1*B22
         SS = A21*( BINV11*BINV22 )
         ABI22 = AS22*BINV22 - SS*B12
         PP = HALF*ABI22
         SHIFT = S1
      ELSE
         AS12 = A12 - S2*B12
         AS11 = A11 - S2*B11
         SS = A21*( BINV11*BINV22 )
         ABI22 = -SS*B12
         PP = HALF*( AS11*BINV11+ABI22 )
         SHIFT = S2
      END IF
      QQ = SS*AS12
      IF( ABS( PP*RTMIN ).GE.ONE ) THEN
         DISCR = ( RTMIN*PP )**2 + QQ*SAFMIN
         R = SQRT( ABS( DISCR ) )*RTMAX
      ELSE
         IF( PP**2+ABS( QQ ).LE.SAFMIN ) THEN
            DISCR = ( RTMAX*PP )**2 + QQ*SAFMAX
            R = SQRT( ABS( DISCR ) )*RTMIN
         ELSE
            DISCR = PP**2 + QQ
            R = SQRT( ABS( DISCR ) )
         END IF
      END IF
*
*     Note: the test of R in the following IF is to cover the case when
*           DISCR is small and negative and is flushed to zero during
*           the calculation of R.  On machines which have a consistent
*           flush-to-zero threshold and handle numbers above that
*           threshold correctly, it would not be necessary.
*
      IF( DISCR.GE.ZERO .OR. R.EQ.ZERO ) THEN
         SUM = PP + SIGN( R, PP )
         DIFF = PP - SIGN( R, PP )
         WBIG = SHIFT + SUM
*
*        Compute smaller eigenvalue
*
         WSMALL = SHIFT + DIFF
         IF( HALF*ABS( WBIG ).GT.MAX( ABS( WSMALL ), SAFMIN ) ) THEN
            WDET = ( A11*A22-A12*A21 )*( BINV11*BINV22 )
            WSMALL = WDET / WBIG
         END IF
*
*        Choose (real) eigenvalue closest to 2,2 element of A*B**(-1)
*        for WR1.
*
         IF( PP.GT.ABI22 ) THEN
            WR1 = MIN( WBIG, WSMALL )
            WR2 = MAX( WBIG, WSMALL )
         ELSE
            WR1 = MAX( WBIG, WSMALL )
            WR2 = MIN( WBIG, WSMALL )
         END IF
         WI = ZERO
      ELSE
*
*        Complex eigenvalues
*
         WR1 = SHIFT + PP
         WR2 = WR1
         WI = R
      END IF
*
*     Further scaling to avoid underflow and overflow in computing
*     SCALE1 and overflow in computing w*B.
*
*     This scale factor (WSCALE) is bounded from above using C1 and C2,
*     and from below using C3 and C4.
*        C1 implements the condition  s A  must never overflow.
*        C2 implements the condition  w B  must never overflow.
*        C3, with C2,
*           implement the condition that s A - w B must never overflow.
*        C4 implements the condition  s    should not underflow.
*        C5 implements the condition  max(s,|w|) should be at least 2.
*
      C1 = BSIZE*( SAFMIN*MAX( ONE, ASCALE ) )
      C2 = SAFMIN*MAX( ONE, BNORM )
      C3 = BSIZE*SAFMIN
      IF( ASCALE.LE.ONE .AND. BSIZE.LE.ONE ) THEN
         C4 = MIN( ONE, ( ASCALE / SAFMIN )*BSIZE )
      ELSE
         C4 = ONE
      END IF
      IF( ASCALE.LE.ONE .OR. BSIZE.LE.ONE ) THEN
         C5 = MIN( ONE, ASCALE*BSIZE )
      ELSE
         C5 = ONE
      END IF
*
*     Scale first eigenvalue
*
      WABS = ABS( WR1 ) + ABS( WI )
      WSIZE = MAX( SAFMIN, C1, FUZZY1*( WABS*C2+C3 ),
     $        MIN( C4, HALF*MAX( WABS, C5 ) ) )
      IF( WSIZE.NE.ONE ) THEN
         WSCALE = ONE / WSIZE
         IF( WSIZE.GT.ONE ) THEN
            SCALE1 = ( MAX( ASCALE, BSIZE )*WSCALE )*
     $               MIN( ASCALE, BSIZE )
         ELSE
            SCALE1 = ( MIN( ASCALE, BSIZE )*WSCALE )*
     $               MAX( ASCALE, BSIZE )
         END IF
         WR1 = WR1*WSCALE
         IF( WI.NE.ZERO ) THEN
            WI = WI*WSCALE
            WR2 = WR1
            SCALE2 = SCALE1
         END IF
      ELSE
         SCALE1 = ASCALE*BSIZE
         SCALE2 = SCALE1
      END IF
*
*     Scale second eigenvalue (if real)
*
      IF( WI.EQ.ZERO ) THEN
         WSIZE = MAX( SAFMIN, C1, FUZZY1*( ABS( WR2 )*C2+C3 ),
     $           MIN( C4, HALF*MAX( ABS( WR2 ), C5 ) ) )
         IF( WSIZE.NE.ONE ) THEN
            WSCALE = ONE / WSIZE
            IF( WSIZE.GT.ONE ) THEN
               SCALE2 = ( MAX( ASCALE, BSIZE )*WSCALE )*
     $                  MIN( ASCALE, BSIZE )
            ELSE
               SCALE2 = ( MIN( ASCALE, BSIZE )*WSCALE )*
     $                  MAX( ASCALE, BSIZE )
            END IF
            WR2 = WR2*WSCALE
         ELSE
            SCALE2 = ASCALE*BSIZE
         END IF
      END IF
*
*     End of SLAG2
*
      RETURN
      END
