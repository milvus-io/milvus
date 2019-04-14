*> \brief \b CLAIC1 applies one step of incremental condition estimation.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLAIC1 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/claic1.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/claic1.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/claic1.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAIC1( JOB, J, X, SEST, W, GAMMA, SESTPR, S, C )
*
*       .. Scalar Arguments ..
*       INTEGER            J, JOB
*       REAL               SEST, SESTPR
*       COMPLEX            C, GAMMA, S
*       ..
*       .. Array Arguments ..
*       COMPLEX            W( J ), X( J )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLAIC1 applies one step of incremental condition estimation in
*> its simplest version:
*>
*> Let x, twonorm(x) = 1, be an approximate singular vector of an j-by-j
*> lower triangular matrix L, such that
*>          twonorm(L*x) = sest
*> Then CLAIC1 computes sestpr, s, c such that
*> the vector
*>                 [ s*x ]
*>          xhat = [  c  ]
*> is an approximate singular vector of
*>                 [ L      0  ]
*>          Lhat = [ w**H gamma ]
*> in the sense that
*>          twonorm(Lhat*xhat) = sestpr.
*>
*> Depending on JOB, an estimate for the largest or smallest singular
*> value is computed.
*>
*> Note that [s c]**H and sestpr**2 is an eigenpair of the system
*>
*>     diag(sest*sest, 0) + [alpha  gamma] * [ conjg(alpha) ]
*>                                           [ conjg(gamma) ]
*>
*> where  alpha =  x**H*w.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is INTEGER
*>          = 1: an estimate for the largest singular value is computed.
*>          = 2: an estimate for the smallest singular value is computed.
*> \endverbatim
*>
*> \param[in] J
*> \verbatim
*>          J is INTEGER
*>          Length of X and W
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is COMPLEX array, dimension (J)
*>          The j-vector x.
*> \endverbatim
*>
*> \param[in] SEST
*> \verbatim
*>          SEST is REAL
*>          Estimated singular value of j by j matrix L
*> \endverbatim
*>
*> \param[in] W
*> \verbatim
*>          W is COMPLEX array, dimension (J)
*>          The j-vector w.
*> \endverbatim
*>
*> \param[in] GAMMA
*> \verbatim
*>          GAMMA is COMPLEX
*>          The diagonal element gamma.
*> \endverbatim
*>
*> \param[out] SESTPR
*> \verbatim
*>          SESTPR is REAL
*>          Estimated singular value of (j+1) by (j+1) matrix Lhat.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is COMPLEX
*>          Sine needed in forming xhat.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is COMPLEX
*>          Cosine needed in forming xhat.
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
*> \ingroup complexOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE CLAIC1( JOB, J, X, SEST, W, GAMMA, SESTPR, S, C )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            J, JOB
      REAL               SEST, SESTPR
      COMPLEX            C, GAMMA, S
*     ..
*     .. Array Arguments ..
      COMPLEX            W( J ), X( J )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0, TWO = 2.0E0 )
      REAL               HALF, FOUR
      PARAMETER          ( HALF = 0.5E0, FOUR = 4.0E0 )
*     ..
*     .. Local Scalars ..
      REAL               ABSALP, ABSEST, ABSGAM, B, EPS, NORMA, S1, S2,
     $                   SCL, T, TEST, TMP, ZETA1, ZETA2
      COMPLEX            ALPHA, COSINE, SINE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CONJG, MAX, SQRT
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      COMPLEX            CDOTC
      EXTERNAL           SLAMCH, CDOTC
*     ..
*     .. Executable Statements ..
*
      EPS = SLAMCH( 'Epsilon' )
      ALPHA = CDOTC( J, X, 1, W, 1 )
*
      ABSALP = ABS( ALPHA )
      ABSGAM = ABS( GAMMA )
      ABSEST = ABS( SEST )
*
      IF( JOB.EQ.1 ) THEN
*
*        Estimating largest singular value
*
*        special cases
*
         IF( SEST.EQ.ZERO ) THEN
            S1 = MAX( ABSGAM, ABSALP )
            IF( S1.EQ.ZERO ) THEN
               S = ZERO
               C = ONE
               SESTPR = ZERO
            ELSE
               S = ALPHA / S1
               C = GAMMA / S1
               TMP = SQRT( S*CONJG( S )+C*CONJG( C ) )
               S = S / TMP
               C = C / TMP
               SESTPR = S1*TMP
            END IF
            RETURN
         ELSE IF( ABSGAM.LE.EPS*ABSEST ) THEN
            S = ONE
            C = ZERO
            TMP = MAX( ABSEST, ABSALP )
            S1 = ABSEST / TMP
            S2 = ABSALP / TMP
            SESTPR = TMP*SQRT( S1*S1+S2*S2 )
            RETURN
         ELSE IF( ABSALP.LE.EPS*ABSEST ) THEN
            S1 = ABSGAM
            S2 = ABSEST
            IF( S1.LE.S2 ) THEN
               S = ONE
               C = ZERO
               SESTPR = S2
            ELSE
               S = ZERO
               C = ONE
               SESTPR = S1
            END IF
            RETURN
         ELSE IF( ABSEST.LE.EPS*ABSALP .OR. ABSEST.LE.EPS*ABSGAM ) THEN
            S1 = ABSGAM
            S2 = ABSALP
            IF( S1.LE.S2 ) THEN
               TMP = S1 / S2
               SCL = SQRT( ONE+TMP*TMP )
               SESTPR = S2*SCL
               S = ( ALPHA / S2 ) / SCL
               C = ( GAMMA / S2 ) / SCL
            ELSE
               TMP = S2 / S1
               SCL = SQRT( ONE+TMP*TMP )
               SESTPR = S1*SCL
               S = ( ALPHA / S1 ) / SCL
               C = ( GAMMA / S1 ) / SCL
            END IF
            RETURN
         ELSE
*
*           normal case
*
            ZETA1 = ABSALP / ABSEST
            ZETA2 = ABSGAM / ABSEST
*
            B = ( ONE-ZETA1*ZETA1-ZETA2*ZETA2 )*HALF
            C = ZETA1*ZETA1
            IF( B.GT.ZERO ) THEN
               T = C / ( B+SQRT( B*B+C ) )
            ELSE
               T = SQRT( B*B+C ) - B
            END IF
*
            SINE = -( ALPHA / ABSEST ) / T
            COSINE = -( GAMMA / ABSEST ) / ( ONE+T )
            TMP = SQRT( SINE*CONJG( SINE )+COSINE*CONJG( COSINE ) )
            S = SINE / TMP
            C = COSINE / TMP
            SESTPR = SQRT( T+ONE )*ABSEST
            RETURN
         END IF
*
      ELSE IF( JOB.EQ.2 ) THEN
*
*        Estimating smallest singular value
*
*        special cases
*
         IF( SEST.EQ.ZERO ) THEN
            SESTPR = ZERO
            IF( MAX( ABSGAM, ABSALP ).EQ.ZERO ) THEN
               SINE = ONE
               COSINE = ZERO
            ELSE
               SINE = -CONJG( GAMMA )
               COSINE = CONJG( ALPHA )
            END IF
            S1 = MAX( ABS( SINE ), ABS( COSINE ) )
            S = SINE / S1
            C = COSINE / S1
            TMP = SQRT( S*CONJG( S )+C*CONJG( C ) )
            S = S / TMP
            C = C / TMP
            RETURN
         ELSE IF( ABSGAM.LE.EPS*ABSEST ) THEN
            S = ZERO
            C = ONE
            SESTPR = ABSGAM
            RETURN
         ELSE IF( ABSALP.LE.EPS*ABSEST ) THEN
            S1 = ABSGAM
            S2 = ABSEST
            IF( S1.LE.S2 ) THEN
               S = ZERO
               C = ONE
               SESTPR = S1
            ELSE
               S = ONE
               C = ZERO
               SESTPR = S2
            END IF
            RETURN
         ELSE IF( ABSEST.LE.EPS*ABSALP .OR. ABSEST.LE.EPS*ABSGAM ) THEN
            S1 = ABSGAM
            S2 = ABSALP
            IF( S1.LE.S2 ) THEN
               TMP = S1 / S2
               SCL = SQRT( ONE+TMP*TMP )
               SESTPR = ABSEST*( TMP / SCL )
               S = -( CONJG( GAMMA ) / S2 ) / SCL
               C = ( CONJG( ALPHA ) / S2 ) / SCL
            ELSE
               TMP = S2 / S1
               SCL = SQRT( ONE+TMP*TMP )
               SESTPR = ABSEST / SCL
               S = -( CONJG( GAMMA ) / S1 ) / SCL
               C = ( CONJG( ALPHA ) / S1 ) / SCL
            END IF
            RETURN
         ELSE
*
*           normal case
*
            ZETA1 = ABSALP / ABSEST
            ZETA2 = ABSGAM / ABSEST
*
            NORMA = MAX( ONE+ZETA1*ZETA1+ZETA1*ZETA2,
     $              ZETA1*ZETA2+ZETA2*ZETA2 )
*
*           See if root is closer to zero or to ONE
*
            TEST = ONE + TWO*( ZETA1-ZETA2 )*( ZETA1+ZETA2 )
            IF( TEST.GE.ZERO ) THEN
*
*              root is close to zero, compute directly
*
               B = ( ZETA1*ZETA1+ZETA2*ZETA2+ONE )*HALF
               C = ZETA2*ZETA2
               T = C / ( B+SQRT( ABS( B*B-C ) ) )
               SINE = ( ALPHA / ABSEST ) / ( ONE-T )
               COSINE = -( GAMMA / ABSEST ) / T
               SESTPR = SQRT( T+FOUR*EPS*EPS*NORMA )*ABSEST
            ELSE
*
*              root is closer to ONE, shift by that amount
*
               B = ( ZETA2*ZETA2+ZETA1*ZETA1-ONE )*HALF
               C = ZETA1*ZETA1
               IF( B.GE.ZERO ) THEN
                  T = -C / ( B+SQRT( B*B+C ) )
               ELSE
                  T = B - SQRT( B*B+C )
               END IF
               SINE = -( ALPHA / ABSEST ) / T
               COSINE = -( GAMMA / ABSEST ) / ( ONE+T )
               SESTPR = SQRT( ONE+T+FOUR*EPS*EPS*NORMA )*ABSEST
            END IF
            TMP = SQRT( SINE*CONJG( SINE )+COSINE*CONJG( COSINE ) )
            S = SINE / TMP
            C = COSINE / TMP
            RETURN
*
         END IF
      END IF
      RETURN
*
*     End of CLAIC1
*
      END
