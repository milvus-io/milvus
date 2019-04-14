*> \brief \b SLARRB provides limited bisection to locate eigenvalues for more accuracy.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLARRB + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarrb.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarrb.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarrb.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLARRB( N, D, LLD, IFIRST, ILAST, RTOL1,
*                          RTOL2, OFFSET, W, WGAP, WERR, WORK, IWORK,
*                          PIVMIN, SPDIAM, TWIST, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            IFIRST, ILAST, INFO, N, OFFSET, TWIST
*       REAL               PIVMIN, RTOL1, RTOL2, SPDIAM
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       REAL               D( * ), LLD( * ), W( * ),
*      $                   WERR( * ), WGAP( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Given the relatively robust representation(RRR) L D L^T, SLARRB
*> does "limited" bisection to refine the eigenvalues of L D L^T,
*> W( IFIRST-OFFSET ) through W( ILAST-OFFSET ), to more accuracy. Initial
*> guesses for these eigenvalues are input in W, the corresponding estimate
*> of the error in these guesses and their gaps are input in WERR
*> and WGAP, respectively. During bisection, intervals
*> [left, right] are maintained by storing their mid-points and
*> semi-widths in the arrays W and WERR respectively.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          The N diagonal elements of the diagonal matrix D.
*> \endverbatim
*>
*> \param[in] LLD
*> \verbatim
*>          LLD is REAL array, dimension (N-1)
*>          The (N-1) elements L(i)*L(i)*D(i).
*> \endverbatim
*>
*> \param[in] IFIRST
*> \verbatim
*>          IFIRST is INTEGER
*>          The index of the first eigenvalue to be computed.
*> \endverbatim
*>
*> \param[in] ILAST
*> \verbatim
*>          ILAST is INTEGER
*>          The index of the last eigenvalue to be computed.
*> \endverbatim
*>
*> \param[in] RTOL1
*> \verbatim
*>          RTOL1 is REAL
*> \endverbatim
*>
*> \param[in] RTOL2
*> \verbatim
*>          RTOL2 is REAL
*>          Tolerance for the convergence of the bisection intervals.
*>          An interval [LEFT,RIGHT] has converged if
*>          RIGHT-LEFT.LT.MAX( RTOL1*GAP, RTOL2*MAX(|LEFT|,|RIGHT|) )
*>          where GAP is the (estimated) distance to the nearest
*>          eigenvalue.
*> \endverbatim
*>
*> \param[in] OFFSET
*> \verbatim
*>          OFFSET is INTEGER
*>          Offset for the arrays W, WGAP and WERR, i.e., the IFIRST-OFFSET
*>          through ILAST-OFFSET elements of these arrays are to be used.
*> \endverbatim
*>
*> \param[in,out] W
*> \verbatim
*>          W is REAL array, dimension (N)
*>          On input, W( IFIRST-OFFSET ) through W( ILAST-OFFSET ) are
*>          estimates of the eigenvalues of L D L^T indexed IFIRST through
*>          ILAST.
*>          On output, these estimates are refined.
*> \endverbatim
*>
*> \param[in,out] WGAP
*> \verbatim
*>          WGAP is REAL array, dimension (N-1)
*>          On input, the (estimated) gaps between consecutive
*>          eigenvalues of L D L^T, i.e., WGAP(I-OFFSET) is the gap between
*>          eigenvalues I and I+1. Note that if IFIRST.EQ.ILAST
*>          then WGAP(IFIRST-OFFSET) must be set to ZERO.
*>          On output, these gaps are refined.
*> \endverbatim
*>
*> \param[in,out] WERR
*> \verbatim
*>          WERR is REAL array, dimension (N)
*>          On input, WERR( IFIRST-OFFSET ) through WERR( ILAST-OFFSET ) are
*>          the errors in the estimates of the corresponding elements in W.
*>          On output, these errors are refined.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (2*N)
*>          Workspace.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (2*N)
*>          Workspace.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is REAL
*>          The minimum pivot in the Sturm sequence.
*> \endverbatim
*>
*> \param[in] SPDIAM
*> \verbatim
*>          SPDIAM is REAL
*>          The spectral diameter of the matrix.
*> \endverbatim
*>
*> \param[in] TWIST
*> \verbatim
*>          TWIST is INTEGER
*>          The twist index for the twisted factorization that is used
*>          for the negcount.
*>          TWIST = N: Compute negcount from L D L^T - LAMBDA I = L+ D+ L+^T
*>          TWIST = 1: Compute negcount from L D L^T - LAMBDA I = U- D- U-^T
*>          TWIST = R: Compute negcount from L D L^T - LAMBDA I = N(r) D(r) N(r)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          Error flag.
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
*> \date June 2017
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
      SUBROUTINE SLARRB( N, D, LLD, IFIRST, ILAST, RTOL1,
     $                   RTOL2, OFFSET, W, WGAP, WERR, WORK, IWORK,
     $                   PIVMIN, SPDIAM, TWIST, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            IFIRST, ILAST, INFO, N, OFFSET, TWIST
      REAL               PIVMIN, RTOL1, RTOL2, SPDIAM
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               D( * ), LLD( * ), W( * ),
     $                   WERR( * ), WGAP( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, TWO, HALF
      PARAMETER        ( ZERO = 0.0E0, TWO = 2.0E0,
     $                   HALF = 0.5E0 )
      INTEGER   MAXITR
*     ..
*     .. Local Scalars ..
      INTEGER            I, I1, II, IP, ITER, K, NEGCNT, NEXT, NINT,
     $                   OLNINT, PREV, R
      REAL               BACK, CVRGD, GAP, LEFT, LGAP, MID, MNWDTH,
     $                   RGAP, RIGHT, TMP, WIDTH
*     ..
*     .. External Functions ..
      INTEGER            SLANEG
      EXTERNAL           SLANEG
*
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
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
      MAXITR = INT( ( LOG( SPDIAM+PIVMIN )-LOG( PIVMIN ) ) /
     $           LOG( TWO ) ) + 2
      MNWDTH = TWO * PIVMIN
*
      R = TWIST
      IF((R.LT.1).OR.(R.GT.N)) R = N
*
*     Initialize unconverged intervals in [ WORK(2*I-1), WORK(2*I) ].
*     The Sturm Count, Count( WORK(2*I-1) ) is arranged to be I-1, while
*     Count( WORK(2*I) ) is stored in IWORK( 2*I ). The integer IWORK( 2*I-1 )
*     for an unconverged interval is set to the index of the next unconverged
*     interval, and is -1 or 0 for a converged interval. Thus a linked
*     list of unconverged intervals is set up.
*
      I1 = IFIRST
*     The number of unconverged intervals
      NINT = 0
*     The last unconverged interval found
      PREV = 0

      RGAP = WGAP( I1-OFFSET )
      DO 75 I = I1, ILAST
         K = 2*I
         II = I - OFFSET
         LEFT = W( II ) - WERR( II )
         RIGHT = W( II ) + WERR( II )
         LGAP = RGAP
         RGAP = WGAP( II )
         GAP = MIN( LGAP, RGAP )

*        Make sure that [LEFT,RIGHT] contains the desired eigenvalue
*        Compute negcount from dstqds facto L+D+L+^T = L D L^T - LEFT
*
*        Do while( NEGCNT(LEFT).GT.I-1 )
*
         BACK = WERR( II )
 20      CONTINUE
         NEGCNT = SLANEG( N, D, LLD, LEFT, PIVMIN, R )
         IF( NEGCNT.GT.I-1 ) THEN
            LEFT = LEFT - BACK
            BACK = TWO*BACK
            GO TO 20
         END IF
*
*        Do while( NEGCNT(RIGHT).LT.I )
*        Compute negcount from dstqds facto L+D+L+^T = L D L^T - RIGHT
*
         BACK = WERR( II )
 50      CONTINUE

         NEGCNT = SLANEG( N, D, LLD, RIGHT, PIVMIN, R )
          IF( NEGCNT.LT.I ) THEN
             RIGHT = RIGHT + BACK
             BACK = TWO*BACK
             GO TO 50
          END IF
         WIDTH = HALF*ABS( LEFT - RIGHT )
         TMP = MAX( ABS( LEFT ), ABS( RIGHT ) )
         CVRGD = MAX(RTOL1*GAP,RTOL2*TMP)
         IF( WIDTH.LE.CVRGD .OR. WIDTH.LE.MNWDTH ) THEN
*           This interval has already converged and does not need refinement.
*           (Note that the gaps might change through refining the
*            eigenvalues, however, they can only get bigger.)
*           Remove it from the list.
            IWORK( K-1 ) = -1
*           Make sure that I1 always points to the first unconverged interval
            IF((I.EQ.I1).AND.(I.LT.ILAST)) I1 = I + 1
            IF((PREV.GE.I1).AND.(I.LE.ILAST)) IWORK( 2*PREV-1 ) = I + 1
         ELSE
*           unconverged interval found
            PREV = I
            NINT = NINT + 1
            IWORK( K-1 ) = I + 1
            IWORK( K ) = NEGCNT
         END IF
         WORK( K-1 ) = LEFT
         WORK( K ) = RIGHT
 75   CONTINUE

*
*     Do while( NINT.GT.0 ), i.e. there are still unconverged intervals
*     and while (ITER.LT.MAXITR)
*
      ITER = 0
 80   CONTINUE
      PREV = I1 - 1
      I = I1
      OLNINT = NINT

      DO 100 IP = 1, OLNINT
         K = 2*I
         II = I - OFFSET
         RGAP = WGAP( II )
         LGAP = RGAP
         IF(II.GT.1) LGAP = WGAP( II-1 )
         GAP = MIN( LGAP, RGAP )
         NEXT = IWORK( K-1 )
         LEFT = WORK( K-1 )
         RIGHT = WORK( K )
         MID = HALF*( LEFT + RIGHT )

*        semiwidth of interval
         WIDTH = RIGHT - MID
         TMP = MAX( ABS( LEFT ), ABS( RIGHT ) )
         CVRGD = MAX(RTOL1*GAP,RTOL2*TMP)
         IF( ( WIDTH.LE.CVRGD ) .OR. ( WIDTH.LE.MNWDTH ).OR.
     $       ( ITER.EQ.MAXITR ) )THEN
*           reduce number of unconverged intervals
            NINT = NINT - 1
*           Mark interval as converged.
            IWORK( K-1 ) = 0
            IF( I1.EQ.I ) THEN
               I1 = NEXT
            ELSE
*              Prev holds the last unconverged interval previously examined
               IF(PREV.GE.I1) IWORK( 2*PREV-1 ) = NEXT
            END IF
            I = NEXT
            GO TO 100
         END IF
         PREV = I
*
*        Perform one bisection step
*
         NEGCNT = SLANEG( N, D, LLD, MID, PIVMIN, R )
         IF( NEGCNT.LE.I-1 ) THEN
            WORK( K-1 ) = MID
         ELSE
            WORK( K ) = MID
         END IF
         I = NEXT
 100  CONTINUE
      ITER = ITER + 1
*     do another loop if there are still unconverged intervals
*     However, in the last iteration, all intervals are accepted
*     since this is the best we can do.
      IF( ( NINT.GT.0 ).AND.(ITER.LE.MAXITR) ) GO TO 80
*
*
*     At this point, all the intervals have converged
      DO 110 I = IFIRST, ILAST
         K = 2*I
         II = I - OFFSET
*        All intervals marked by '0' have been refined.
         IF( IWORK( K-1 ).EQ.0 ) THEN
            W( II ) = HALF*( WORK( K-1 )+WORK( K ) )
            WERR( II ) = WORK( K ) - W( II )
         END IF
 110  CONTINUE
*
      DO 111 I = IFIRST+1, ILAST
         K = 2*I
         II = I - OFFSET
         WGAP( II-1 ) = MAX( ZERO,
     $                     W(II) - WERR (II) - W( II-1 ) - WERR( II-1 ))
 111  CONTINUE

      RETURN
*
*     End of SLARRB
*
      END
