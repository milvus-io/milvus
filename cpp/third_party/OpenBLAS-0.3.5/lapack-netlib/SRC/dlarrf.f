*> \brief \b DLARRF finds a new relatively robust representation such that at least one of the eigenvalues is relatively isolated.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLARRF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlarrf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlarrf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlarrf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLARRF( N, D, L, LD, CLSTRT, CLEND,
*                          W, WGAP, WERR,
*                          SPDIAM, CLGAPL, CLGAPR, PIVMIN, SIGMA,
*                          DPLUS, LPLUS, WORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            CLSTRT, CLEND, INFO, N
*       DOUBLE PRECISION   CLGAPL, CLGAPR, PIVMIN, SIGMA, SPDIAM
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * ), DPLUS( * ), L( * ), LD( * ),
*      $          LPLUS( * ), W( * ), WGAP( * ), WERR( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Given the initial representation L D L^T and its cluster of close
*> eigenvalues (in a relative measure), W( CLSTRT ), W( CLSTRT+1 ), ...
*> W( CLEND ), DLARRF finds a new relatively robust representation
*> L D L^T - SIGMA I = L(+) D(+) L(+)^T such that at least one of the
*> eigenvalues of L(+) D(+) L(+)^T is relatively isolated.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix (subblock, if the matrix split).
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The N diagonal elements of the diagonal matrix D.
*> \endverbatim
*>
*> \param[in] L
*> \verbatim
*>          L is DOUBLE PRECISION array, dimension (N-1)
*>          The (N-1) subdiagonal elements of the unit bidiagonal
*>          matrix L.
*> \endverbatim
*>
*> \param[in] LD
*> \verbatim
*>          LD is DOUBLE PRECISION array, dimension (N-1)
*>          The (N-1) elements L(i)*D(i).
*> \endverbatim
*>
*> \param[in] CLSTRT
*> \verbatim
*>          CLSTRT is INTEGER
*>          The index of the first eigenvalue in the cluster.
*> \endverbatim
*>
*> \param[in] CLEND
*> \verbatim
*>          CLEND is INTEGER
*>          The index of the last eigenvalue in the cluster.
*> \endverbatim
*>
*> \param[in] W
*> \verbatim
*>          W is DOUBLE PRECISION array, dimension
*>          dimension is >=  (CLEND-CLSTRT+1)
*>          The eigenvalue APPROXIMATIONS of L D L^T in ascending order.
*>          W( CLSTRT ) through W( CLEND ) form the cluster of relatively
*>          close eigenalues.
*> \endverbatim
*>
*> \param[in,out] WGAP
*> \verbatim
*>          WGAP is DOUBLE PRECISION array, dimension
*>          dimension is >=  (CLEND-CLSTRT+1)
*>          The separation from the right neighbor eigenvalue in W.
*> \endverbatim
*>
*> \param[in] WERR
*> \verbatim
*>          WERR is DOUBLE PRECISION array, dimension
*>          dimension is  >=  (CLEND-CLSTRT+1)
*>          WERR contain the semiwidth of the uncertainty
*>          interval of the corresponding eigenvalue APPROXIMATION in W
*> \endverbatim
*>
*> \param[in] SPDIAM
*> \verbatim
*>          SPDIAM is DOUBLE PRECISION
*>          estimate of the spectral diameter obtained from the
*>          Gerschgorin intervals
*> \endverbatim
*>
*> \param[in] CLGAPL
*> \verbatim
*>          CLGAPL is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[in] CLGAPR
*> \verbatim
*>          CLGAPR is DOUBLE PRECISION
*>          absolute gap on each end of the cluster.
*>          Set by the calling routine to protect against shifts too close
*>          to eigenvalues outside the cluster.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is DOUBLE PRECISION
*>          The minimum pivot allowed in the Sturm sequence.
*> \endverbatim
*>
*> \param[out] SIGMA
*> \verbatim
*>          SIGMA is DOUBLE PRECISION
*>          The shift used to form L(+) D(+) L(+)^T.
*> \endverbatim
*>
*> \param[out] DPLUS
*> \verbatim
*>          DPLUS is DOUBLE PRECISION array, dimension (N)
*>          The N diagonal elements of the diagonal matrix D(+).
*> \endverbatim
*>
*> \param[out] LPLUS
*> \verbatim
*>          LPLUS is DOUBLE PRECISION array, dimension (N-1)
*>          The first (N-1) elements of LPLUS contain the subdiagonal
*>          elements of the unit bidiagonal matrix L(+).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (2*N)
*>          Workspace.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          Signals processing OK (=0) or failure (=1)
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
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*> Beresford Parlett, University of California, Berkeley, USA \n
*> Jim Demmel, University of California, Berkeley, USA \n
*> Inderjit Dhillon, University of Texas, Austin, USA \n
*> Osni Marques, LBNL/NERSC, USA \n
*> Christof Voemel, University of California, Berkeley, USA
*
*  =====================================================================
      SUBROUTINE DLARRF( N, D, L, LD, CLSTRT, CLEND,
     $                   W, WGAP, WERR,
     $                   SPDIAM, CLGAPL, CLGAPR, PIVMIN, SIGMA,
     $                   DPLUS, LPLUS, WORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            CLSTRT, CLEND, INFO, N
      DOUBLE PRECISION   CLGAPL, CLGAPR, PIVMIN, SIGMA, SPDIAM
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * ), DPLUS( * ), L( * ), LD( * ),
     $          LPLUS( * ), W( * ), WGAP( * ), WERR( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   FOUR, MAXGROWTH1, MAXGROWTH2, ONE, QUART, TWO
      PARAMETER          ( ONE = 1.0D0, TWO = 2.0D0, FOUR = 4.0D0,
     $                     QUART = 0.25D0,
     $                     MAXGROWTH1 = 8.D0,
     $                     MAXGROWTH2 = 8.D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL   DORRR1, FORCER, NOFAIL, SAWNAN1, SAWNAN2, TRYRRR1
      INTEGER            I, INDX, KTRY, KTRYMAX, SLEFT, SRIGHT, SHIFT
      PARAMETER          ( KTRYMAX = 1, SLEFT = 1, SRIGHT = 2 )
      DOUBLE PRECISION   AVGAP, BESTSHIFT, CLWDTH, EPS, FACT, FAIL,
     $                   FAIL2, GROWTHBOUND, LDELTA, LDMAX, LSIGMA,
     $                   MAX1, MAX2, MINGAP, OLDP, PROD, RDELTA, RDMAX,
     $                   RRR1, RRR2, RSIGMA, S, SMLGROWTH, TMP, ZNM2
*     ..
*     .. External Functions ..
      LOGICAL DISNAN
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DISNAN, DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
      INFO = 0
*
*     Quick return if possible
*
      IF( N.LE.0 ) THEN
         RETURN
      END IF
*
      FACT = DBLE(2**KTRYMAX)
      EPS = DLAMCH( 'Precision' )
      SHIFT = 0
      FORCER = .FALSE.


*     Note that we cannot guarantee that for any of the shifts tried,
*     the factorization has a small or even moderate element growth.
*     There could be Ritz values at both ends of the cluster and despite
*     backing off, there are examples where all factorizations tried
*     (in IEEE mode, allowing zero pivots & infinities) have INFINITE
*     element growth.
*     For this reason, we should use PIVMIN in this subroutine so that at
*     least the L D L^T factorization exists. It can be checked afterwards
*     whether the element growth caused bad residuals/orthogonality.

*     Decide whether the code should accept the best among all
*     representations despite large element growth or signal INFO=1
*     Setting NOFAIL to .FALSE. for quick fix for bug 113
      NOFAIL = .FALSE.
*

*     Compute the average gap length of the cluster
      CLWDTH = ABS(W(CLEND)-W(CLSTRT)) + WERR(CLEND) + WERR(CLSTRT)
      AVGAP = CLWDTH / DBLE(CLEND-CLSTRT)
      MINGAP = MIN(CLGAPL, CLGAPR)
*     Initial values for shifts to both ends of cluster
      LSIGMA = MIN(W( CLSTRT ),W( CLEND )) - WERR( CLSTRT )
      RSIGMA = MAX(W( CLSTRT ),W( CLEND )) + WERR( CLEND )

*     Use a small fudge to make sure that we really shift to the outside
      LSIGMA = LSIGMA - ABS(LSIGMA)* FOUR * EPS
      RSIGMA = RSIGMA + ABS(RSIGMA)* FOUR * EPS

*     Compute upper bounds for how much to back off the initial shifts
      LDMAX = QUART * MINGAP + TWO * PIVMIN
      RDMAX = QUART * MINGAP + TWO * PIVMIN

      LDELTA = MAX(AVGAP,WGAP( CLSTRT ))/FACT
      RDELTA = MAX(AVGAP,WGAP( CLEND-1 ))/FACT
*
*     Initialize the record of the best representation found
*
      S = DLAMCH( 'S' )
      SMLGROWTH = ONE / S
      FAIL = DBLE(N-1)*MINGAP/(SPDIAM*EPS)
      FAIL2 = DBLE(N-1)*MINGAP/(SPDIAM*SQRT(EPS))
      BESTSHIFT = LSIGMA
*
*     while (KTRY <= KTRYMAX)
      KTRY = 0
      GROWTHBOUND = MAXGROWTH1*SPDIAM

 5    CONTINUE
      SAWNAN1 = .FALSE.
      SAWNAN2 = .FALSE.
*     Ensure that we do not back off too much of the initial shifts
      LDELTA = MIN(LDMAX,LDELTA)
      RDELTA = MIN(RDMAX,RDELTA)

*     Compute the element growth when shifting to both ends of the cluster
*     accept the shift if there is no element growth at one of the two ends

*     Left end
      S = -LSIGMA
      DPLUS( 1 ) = D( 1 ) + S
      IF(ABS(DPLUS(1)).LT.PIVMIN) THEN
         DPLUS(1) = -PIVMIN
*        Need to set SAWNAN1 because refined RRR test should not be used
*        in this case
         SAWNAN1 = .TRUE.
      ENDIF
      MAX1 = ABS( DPLUS( 1 ) )
      DO 6 I = 1, N - 1
         LPLUS( I ) = LD( I ) / DPLUS( I )
         S = S*LPLUS( I )*L( I ) - LSIGMA
         DPLUS( I+1 ) = D( I+1 ) + S
         IF(ABS(DPLUS(I+1)).LT.PIVMIN) THEN
            DPLUS(I+1) = -PIVMIN
*           Need to set SAWNAN1 because refined RRR test should not be used
*           in this case
            SAWNAN1 = .TRUE.
         ENDIF
         MAX1 = MAX( MAX1,ABS(DPLUS(I+1)) )
 6    CONTINUE
      SAWNAN1 = SAWNAN1 .OR.  DISNAN( MAX1 )

      IF( FORCER .OR.
     $   (MAX1.LE.GROWTHBOUND .AND. .NOT.SAWNAN1 ) ) THEN
         SIGMA = LSIGMA
         SHIFT = SLEFT
         GOTO 100
      ENDIF

*     Right end
      S = -RSIGMA
      WORK( 1 ) = D( 1 ) + S
      IF(ABS(WORK(1)).LT.PIVMIN) THEN
         WORK(1) = -PIVMIN
*        Need to set SAWNAN2 because refined RRR test should not be used
*        in this case
         SAWNAN2 = .TRUE.
      ENDIF
      MAX2 = ABS( WORK( 1 ) )
      DO 7 I = 1, N - 1
         WORK( N+I ) = LD( I ) / WORK( I )
         S = S*WORK( N+I )*L( I ) - RSIGMA
         WORK( I+1 ) = D( I+1 ) + S
         IF(ABS(WORK(I+1)).LT.PIVMIN) THEN
            WORK(I+1) = -PIVMIN
*           Need to set SAWNAN2 because refined RRR test should not be used
*           in this case
            SAWNAN2 = .TRUE.
         ENDIF
         MAX2 = MAX( MAX2,ABS(WORK(I+1)) )
 7    CONTINUE
      SAWNAN2 = SAWNAN2 .OR.  DISNAN( MAX2 )

      IF( FORCER .OR.
     $   (MAX2.LE.GROWTHBOUND .AND. .NOT.SAWNAN2 ) ) THEN
         SIGMA = RSIGMA
         SHIFT = SRIGHT
         GOTO 100
      ENDIF
*     If we are at this point, both shifts led to too much element growth

*     Record the better of the two shifts (provided it didn't lead to NaN)
      IF(SAWNAN1.AND.SAWNAN2) THEN
*        both MAX1 and MAX2 are NaN
         GOTO 50
      ELSE
         IF( .NOT.SAWNAN1 ) THEN
            INDX = 1
            IF(MAX1.LE.SMLGROWTH) THEN
               SMLGROWTH = MAX1
               BESTSHIFT = LSIGMA
            ENDIF
         ENDIF
         IF( .NOT.SAWNAN2 ) THEN
            IF(SAWNAN1 .OR. MAX2.LE.MAX1) INDX = 2
            IF(MAX2.LE.SMLGROWTH) THEN
               SMLGROWTH = MAX2
               BESTSHIFT = RSIGMA
            ENDIF
         ENDIF
      ENDIF

*     If we are here, both the left and the right shift led to
*     element growth. If the element growth is moderate, then
*     we may still accept the representation, if it passes a
*     refined test for RRR. This test supposes that no NaN occurred.
*     Moreover, we use the refined RRR test only for isolated clusters.
      IF((CLWDTH.LT.MINGAP/DBLE(128)) .AND.
     $   (MIN(MAX1,MAX2).LT.FAIL2)
     $  .AND.(.NOT.SAWNAN1).AND.(.NOT.SAWNAN2)) THEN
         DORRR1 = .TRUE.
      ELSE
         DORRR1 = .FALSE.
      ENDIF
      TRYRRR1 = .TRUE.
      IF( TRYRRR1 .AND. DORRR1 ) THEN
      IF(INDX.EQ.1) THEN
         TMP = ABS( DPLUS( N ) )
         ZNM2 = ONE
         PROD = ONE
         OLDP = ONE
         DO 15 I = N-1, 1, -1
            IF( PROD .LE. EPS ) THEN
               PROD =
     $         ((DPLUS(I+1)*WORK(N+I+1))/(DPLUS(I)*WORK(N+I)))*OLDP
            ELSE
               PROD = PROD*ABS(WORK(N+I))
            END IF
            OLDP = PROD
            ZNM2 = ZNM2 + PROD**2
            TMP = MAX( TMP, ABS( DPLUS( I ) * PROD ))
 15      CONTINUE
         RRR1 = TMP/( SPDIAM * SQRT( ZNM2 ) )
         IF (RRR1.LE.MAXGROWTH2) THEN
            SIGMA = LSIGMA
            SHIFT = SLEFT
            GOTO 100
         ENDIF
      ELSE IF(INDX.EQ.2) THEN
         TMP = ABS( WORK( N ) )
         ZNM2 = ONE
         PROD = ONE
         OLDP = ONE
         DO 16 I = N-1, 1, -1
            IF( PROD .LE. EPS ) THEN
               PROD = ((WORK(I+1)*LPLUS(I+1))/(WORK(I)*LPLUS(I)))*OLDP
            ELSE
               PROD = PROD*ABS(LPLUS(I))
            END IF
            OLDP = PROD
            ZNM2 = ZNM2 + PROD**2
            TMP = MAX( TMP, ABS( WORK( I ) * PROD ))
 16      CONTINUE
         RRR2 = TMP/( SPDIAM * SQRT( ZNM2 ) )
         IF (RRR2.LE.MAXGROWTH2) THEN
            SIGMA = RSIGMA
            SHIFT = SRIGHT
            GOTO 100
         ENDIF
      END IF
      ENDIF

 50   CONTINUE

      IF (KTRY.LT.KTRYMAX) THEN
*        If we are here, both shifts failed also the RRR test.
*        Back off to the outside
         LSIGMA = MAX( LSIGMA - LDELTA,
     $     LSIGMA - LDMAX)
         RSIGMA = MIN( RSIGMA + RDELTA,
     $     RSIGMA + RDMAX )
         LDELTA = TWO * LDELTA
         RDELTA = TWO * RDELTA
         KTRY = KTRY + 1
         GOTO 5
      ELSE
*        None of the representations investigated satisfied our
*        criteria. Take the best one we found.
         IF((SMLGROWTH.LT.FAIL).OR.NOFAIL) THEN
            LSIGMA = BESTSHIFT
            RSIGMA = BESTSHIFT
            FORCER = .TRUE.
            GOTO 5
         ELSE
            INFO = 1
            RETURN
         ENDIF
      END IF

 100  CONTINUE
      IF (SHIFT.EQ.SLEFT) THEN
      ELSEIF (SHIFT.EQ.SRIGHT) THEN
*        store new L and D back into DPLUS, LPLUS
         CALL DCOPY( N, WORK, 1, DPLUS, 1 )
         CALL DCOPY( N-1, WORK(N+1), 1, LPLUS, 1 )
      ENDIF

      RETURN
*
*     End of DLARRF
*
      END
