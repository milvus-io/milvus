*> \brief \b SLARRJ performs refinement of the initial estimates of the eigenvalues of the matrix T.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLARRJ + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarrj.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarrj.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarrj.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLARRJ( N, D, E2, IFIRST, ILAST,
*                          RTOL, OFFSET, W, WERR, WORK, IWORK,
*                          PIVMIN, SPDIAM, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            IFIRST, ILAST, INFO, N, OFFSET
*       REAL               PIVMIN, RTOL, SPDIAM
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       REAL               D( * ), E2( * ), W( * ),
*      $                   WERR( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Given the initial eigenvalue approximations of T, SLARRJ
*> does  bisection to refine the eigenvalues of T,
*> W( IFIRST-OFFSET ) through W( ILAST-OFFSET ), to more accuracy. Initial
*> guesses for these eigenvalues are input in W, the corresponding estimate
*> of the error in these guesses in WERR. During bisection, intervals
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
*>          The N diagonal elements of T.
*> \endverbatim
*>
*> \param[in] E2
*> \verbatim
*>          E2 is REAL array, dimension (N-1)
*>          The Squares of the (N-1) subdiagonal elements of T.
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
*> \param[in] RTOL
*> \verbatim
*>          RTOL is REAL
*>          Tolerance for the convergence of the bisection intervals.
*>          An interval [LEFT,RIGHT] has converged if
*>          RIGHT-LEFT.LT.RTOL*MAX(|LEFT|,|RIGHT|).
*> \endverbatim
*>
*> \param[in] OFFSET
*> \verbatim
*>          OFFSET is INTEGER
*>          Offset for the arrays W and WERR, i.e., the IFIRST-OFFSET
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
*>          The minimum pivot in the Sturm sequence for T.
*> \endverbatim
*>
*> \param[in] SPDIAM
*> \verbatim
*>          SPDIAM is REAL
*>          The spectral diameter of T.
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
      SUBROUTINE SLARRJ( N, D, E2, IFIRST, ILAST,
     $                   RTOL, OFFSET, W, WERR, WORK, IWORK,
     $                   PIVMIN, SPDIAM, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            IFIRST, ILAST, INFO, N, OFFSET
      REAL               PIVMIN, RTOL, SPDIAM
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               D( * ), E2( * ), W( * ),
     $                   WERR( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO, HALF
      PARAMETER        ( ZERO = 0.0E0, ONE = 1.0E0, TWO = 2.0E0,
     $                   HALF = 0.5E0 )
      INTEGER   MAXITR
*     ..
*     .. Local Scalars ..
      INTEGER            CNT, I, I1, I2, II, ITER, J, K, NEXT, NINT,
     $                   OLNINT, P, PREV, SAVI1
      REAL               DPLUS, FAC, LEFT, MID, RIGHT, S, TMP, WIDTH
*
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
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
*
*     Initialize unconverged intervals in [ WORK(2*I-1), WORK(2*I) ].
*     The Sturm Count, Count( WORK(2*I-1) ) is arranged to be I-1, while
*     Count( WORK(2*I) ) is stored in IWORK( 2*I ). The integer IWORK( 2*I-1 )
*     for an unconverged interval is set to the index of the next unconverged
*     interval, and is -1 or 0 for a converged interval. Thus a linked
*     list of unconverged intervals is set up.
*

      I1 = IFIRST
      I2 = ILAST
*     The number of unconverged intervals
      NINT = 0
*     The last unconverged interval found
      PREV = 0
      DO 75 I = I1, I2
         K = 2*I
         II = I - OFFSET
         LEFT = W( II ) - WERR( II )
         MID = W(II)
         RIGHT = W( II ) + WERR( II )
         WIDTH = RIGHT - MID
         TMP = MAX( ABS( LEFT ), ABS( RIGHT ) )

*        The following test prevents the test of converged intervals
         IF( WIDTH.LT.RTOL*TMP ) THEN
*           This interval has already converged and does not need refinement.
*           (Note that the gaps might change through refining the
*            eigenvalues, however, they can only get bigger.)
*           Remove it from the list.
            IWORK( K-1 ) = -1
*           Make sure that I1 always points to the first unconverged interval
            IF((I.EQ.I1).AND.(I.LT.I2)) I1 = I + 1
            IF((PREV.GE.I1).AND.(I.LE.I2)) IWORK( 2*PREV-1 ) = I + 1
         ELSE
*           unconverged interval found
            PREV = I
*           Make sure that [LEFT,RIGHT] contains the desired eigenvalue
*
*           Do while( CNT(LEFT).GT.I-1 )
*
            FAC = ONE
 20         CONTINUE
            CNT = 0
            S = LEFT
            DPLUS = D( 1 ) - S
            IF( DPLUS.LT.ZERO ) CNT = CNT + 1
            DO 30 J = 2, N
               DPLUS = D( J ) - S - E2( J-1 )/DPLUS
               IF( DPLUS.LT.ZERO ) CNT = CNT + 1
 30         CONTINUE
            IF( CNT.GT.I-1 ) THEN
               LEFT = LEFT - WERR( II )*FAC
               FAC = TWO*FAC
               GO TO 20
            END IF
*
*           Do while( CNT(RIGHT).LT.I )
*
            FAC = ONE
 50         CONTINUE
            CNT = 0
            S = RIGHT
            DPLUS = D( 1 ) - S
            IF( DPLUS.LT.ZERO ) CNT = CNT + 1
            DO 60 J = 2, N
               DPLUS = D( J ) - S - E2( J-1 )/DPLUS
               IF( DPLUS.LT.ZERO ) CNT = CNT + 1
 60         CONTINUE
            IF( CNT.LT.I ) THEN
               RIGHT = RIGHT + WERR( II )*FAC
               FAC = TWO*FAC
               GO TO 50
            END IF
            NINT = NINT + 1
            IWORK( K-1 ) = I + 1
            IWORK( K ) = CNT
         END IF
         WORK( K-1 ) = LEFT
         WORK( K ) = RIGHT
 75   CONTINUE


      SAVI1 = I1
*
*     Do while( NINT.GT.0 ), i.e. there are still unconverged intervals
*     and while (ITER.LT.MAXITR)
*
      ITER = 0
 80   CONTINUE
      PREV = I1 - 1
      I = I1
      OLNINT = NINT

      DO 100 P = 1, OLNINT
         K = 2*I
         II = I - OFFSET
         NEXT = IWORK( K-1 )
         LEFT = WORK( K-1 )
         RIGHT = WORK( K )
         MID = HALF*( LEFT + RIGHT )

*        semiwidth of interval
         WIDTH = RIGHT - MID
         TMP = MAX( ABS( LEFT ), ABS( RIGHT ) )

         IF( ( WIDTH.LT.RTOL*TMP ) .OR.
     $      (ITER.EQ.MAXITR) )THEN
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
         CNT = 0
         S = MID
         DPLUS = D( 1 ) - S
         IF( DPLUS.LT.ZERO ) CNT = CNT + 1
         DO 90 J = 2, N
            DPLUS = D( J ) - S - E2( J-1 )/DPLUS
            IF( DPLUS.LT.ZERO ) CNT = CNT + 1
 90      CONTINUE
         IF( CNT.LE.I-1 ) THEN
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
      DO 110 I = SAVI1, ILAST
         K = 2*I
         II = I - OFFSET
*        All intervals marked by '0' have been refined.
         IF( IWORK( K-1 ).EQ.0 ) THEN
            W( II ) = HALF*( WORK( K-1 )+WORK( K ) )
            WERR( II ) = WORK( K ) - W( II )
         END IF
 110  CONTINUE
*

      RETURN
*
*     End of SLARRJ
*
      END
