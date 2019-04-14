*> \brief \b SLARRE given the tridiagonal matrix T, sets small off-diagonal elements to zero and for each unreduced block Ti, finds base representations and eigenvalues.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLARRE + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarre.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarre.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarre.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLARRE( RANGE, N, VL, VU, IL, IU, D, E, E2,
*                           RTOL1, RTOL2, SPLTOL, NSPLIT, ISPLIT, M,
*                           W, WERR, WGAP, IBLOCK, INDEXW, GERS, PIVMIN,
*                           WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          RANGE
*       INTEGER            IL, INFO, IU, M, N, NSPLIT
*       REAL               PIVMIN, RTOL1, RTOL2, SPLTOL, VL, VU
*       ..
*       .. Array Arguments ..
*       INTEGER            IBLOCK( * ), ISPLIT( * ), IWORK( * ),
*      $                   INDEXW( * )
*       REAL               D( * ), E( * ), E2( * ), GERS( * ),
*      $                   W( * ),WERR( * ), WGAP( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> To find the desired eigenvalues of a given real symmetric
*> tridiagonal matrix T, SLARRE sets any "small" off-diagonal
*> elements to zero, and for each unreduced block T_i, it finds
*> (a) a suitable shift at one end of the block's spectrum,
*> (b) the base representation, T_i - sigma_i I = L_i D_i L_i^T, and
*> (c) eigenvalues of each L_i D_i L_i^T.
*> The representations and eigenvalues found are then used by
*> SSTEMR to compute the eigenvectors of T.
*> The accuracy varies depending on whether bisection is used to
*> find a few eigenvalues or the dqds algorithm (subroutine SLASQ2) to
*> conpute all and then discard any unwanted one.
*> As an added benefit, SLARRE also outputs the n
*> Gerschgorin intervals for the matrices L_i D_i L_i^T.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] RANGE
*> \verbatim
*>          RANGE is CHARACTER*1
*>          = 'A': ("All")   all eigenvalues will be found.
*>          = 'V': ("Value") all eigenvalues in the half-open interval
*>                           (VL, VU] will be found.
*>          = 'I': ("Index") the IL-th through IU-th eigenvalues (of the
*>                           entire matrix) will be found.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix. N > 0.
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is REAL
*>          If RANGE='V', the lower bound for the eigenvalues.
*>          Eigenvalues less than or equal to VL, or greater than VU,
*>          will not be returned.  VL < VU.
*>          If RANGE='I' or ='A', SLARRE computes bounds on the desired
*>          part of the spectrum.
*> \endverbatim
*>
*> \param[in,out] VU
*> \verbatim
*>          VU is REAL
*>          If RANGE='V', the upper bound for the eigenvalues.
*>          Eigenvalues less than or equal to VL, or greater than VU,
*>          will not be returned.  VL < VU.
*>          If RANGE='I' or ='A', SLARRE computes bounds on the desired
*>          part of the spectrum.
*> \endverbatim
*>
*> \param[in] IL
*> \verbatim
*>          IL is INTEGER
*>          If RANGE='I', the index of the
*>          smallest eigenvalue to be returned.
*>          1 <= IL <= IU <= N.
*> \endverbatim
*>
*> \param[in] IU
*> \verbatim
*>          IU is INTEGER
*>          If RANGE='I', the index of the
*>          largest eigenvalue to be returned.
*>          1 <= IL <= IU <= N.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          On entry, the N diagonal elements of the tridiagonal
*>          matrix T.
*>          On exit, the N diagonal elements of the diagonal
*>          matrices D_i.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is REAL array, dimension (N)
*>          On entry, the first (N-1) entries contain the subdiagonal
*>          elements of the tridiagonal matrix T; E(N) need not be set.
*>          On exit, E contains the subdiagonal elements of the unit
*>          bidiagonal matrices L_i. The entries E( ISPLIT( I ) ),
*>          1 <= I <= NSPLIT, contain the base points sigma_i on output.
*> \endverbatim
*>
*> \param[in,out] E2
*> \verbatim
*>          E2 is REAL array, dimension (N)
*>          On entry, the first (N-1) entries contain the SQUARES of the
*>          subdiagonal elements of the tridiagonal matrix T;
*>          E2(N) need not be set.
*>          On exit, the entries E2( ISPLIT( I ) ),
*>          1 <= I <= NSPLIT, have been set to zero
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
*>           Parameters for bisection.
*>           An interval [LEFT,RIGHT] has converged if
*>           RIGHT-LEFT.LT.MAX( RTOL1*GAP, RTOL2*MAX(|LEFT|,|RIGHT|) )
*> \endverbatim
*>
*> \param[in] SPLTOL
*> \verbatim
*>          SPLTOL is REAL
*>          The threshold for splitting.
*> \endverbatim
*>
*> \param[out] NSPLIT
*> \verbatim
*>          NSPLIT is INTEGER
*>          The number of blocks T splits into. 1 <= NSPLIT <= N.
*> \endverbatim
*>
*> \param[out] ISPLIT
*> \verbatim
*>          ISPLIT is INTEGER array, dimension (N)
*>          The splitting points, at which T breaks up into blocks.
*>          The first block consists of rows/columns 1 to ISPLIT(1),
*>          the second of rows/columns ISPLIT(1)+1 through ISPLIT(2),
*>          etc., and the NSPLIT-th consists of rows/columns
*>          ISPLIT(NSPLIT-1)+1 through ISPLIT(NSPLIT)=N.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The total number of eigenvalues (of all L_i D_i L_i^T)
*>          found.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is REAL array, dimension (N)
*>          The first M elements contain the eigenvalues. The
*>          eigenvalues of each of the blocks, L_i D_i L_i^T, are
*>          sorted in ascending order ( SLARRE may use the
*>          remaining N-M elements as workspace).
*> \endverbatim
*>
*> \param[out] WERR
*> \verbatim
*>          WERR is REAL array, dimension (N)
*>          The error bound on the corresponding eigenvalue in W.
*> \endverbatim
*>
*> \param[out] WGAP
*> \verbatim
*>          WGAP is REAL array, dimension (N)
*>          The separation from the right neighbor eigenvalue in W.
*>          The gap is only with respect to the eigenvalues of the same block
*>          as each block has its own representation tree.
*>          Exception: at the right end of a block we store the left gap
*> \endverbatim
*>
*> \param[out] IBLOCK
*> \verbatim
*>          IBLOCK is INTEGER array, dimension (N)
*>          The indices of the blocks (submatrices) associated with the
*>          corresponding eigenvalues in W; IBLOCK(i)=1 if eigenvalue
*>          W(i) belongs to the first block from the top, =2 if W(i)
*>          belongs to the second block, etc.
*> \endverbatim
*>
*> \param[out] INDEXW
*> \verbatim
*>          INDEXW is INTEGER array, dimension (N)
*>          The indices of the eigenvalues within each block (submatrix);
*>          for example, INDEXW(i)= 10 and IBLOCK(i)=2 imply that the
*>          i-th eigenvalue W(i) is the 10-th eigenvalue in block 2
*> \endverbatim
*>
*> \param[out] GERS
*> \verbatim
*>          GERS is REAL array, dimension (2*N)
*>          The N Gerschgorin intervals (the i-th Gerschgorin interval
*>          is (GERS(2*i-1), GERS(2*i)).
*> \endverbatim
*>
*> \param[out] PIVMIN
*> \verbatim
*>          PIVMIN is REAL
*>          The minimum pivot in the Sturm sequence for T.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (6*N)
*>          Workspace.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (5*N)
*>          Workspace.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          > 0:  A problem occurred in SLARRE.
*>          < 0:  One of the called subroutines signaled an internal problem.
*>                Needs inspection of the corresponding parameter IINFO
*>                for further information.
*>
*>          =-1:  Problem in SLARRD.
*>          = 2:  No base representation could be found in MAXTRY iterations.
*>                Increasing MAXTRY and recompilation might be a remedy.
*>          =-3:  Problem in SLARRB when computing the refined root
*>                representation for SLASQ2.
*>          =-4:  Problem in SLARRB when preforming bisection on the
*>                desired part of the spectrum.
*>          =-5:  Problem in SLASQ2.
*>          =-6:  Problem in SLASQ2.
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
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The base representations are required to suffer very little
*>  element growth and consequently define all their eigenvalues to
*>  high relative accuracy.
*> \endverbatim
*
*> \par Contributors:
*  ==================
*>
*>     Beresford Parlett, University of California, Berkeley, USA \n
*>     Jim Demmel, University of California, Berkeley, USA \n
*>     Inderjit Dhillon, University of Texas, Austin, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*>     Christof Voemel, University of California, Berkeley, USA \n
*>
*  =====================================================================
      SUBROUTINE SLARRE( RANGE, N, VL, VU, IL, IU, D, E, E2,
     $                    RTOL1, RTOL2, SPLTOL, NSPLIT, ISPLIT, M,
     $                    W, WERR, WGAP, IBLOCK, INDEXW, GERS, PIVMIN,
     $                    WORK, IWORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          RANGE
      INTEGER            IL, INFO, IU, M, N, NSPLIT
      REAL               PIVMIN, RTOL1, RTOL2, SPLTOL, VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            IBLOCK( * ), ISPLIT( * ), IWORK( * ),
     $                   INDEXW( * )
      REAL               D( * ), E( * ), E2( * ), GERS( * ),
     $                   W( * ),WERR( * ), WGAP( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               FAC, FOUR, FOURTH, FUDGE, HALF, HNDRD,
     $                   MAXGROWTH, ONE, PERT, TWO, ZERO
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0,
     $                     TWO = 2.0E0, FOUR=4.0E0,
     $                     HNDRD = 100.0E0,
     $                     PERT = 4.0E0,
     $                     HALF = ONE/TWO, FOURTH = ONE/FOUR, FAC= HALF,
     $                     MAXGROWTH = 64.0E0, FUDGE = 2.0E0 )
      INTEGER            MAXTRY, ALLRNG, INDRNG, VALRNG
      PARAMETER          ( MAXTRY = 6, ALLRNG = 1, INDRNG = 2,
     $                     VALRNG = 3 )
*     ..
*     .. Local Scalars ..
      LOGICAL            FORCEB, NOREP, USEDQD
      INTEGER            CNT, CNT1, CNT2, I, IBEGIN, IDUM, IEND, IINFO,
     $                   IN, INDL, INDU, IRANGE, J, JBLK, MB, MM,
     $                   WBEGIN, WEND
      REAL               AVGAP, BSRTOL, CLWDTH, DMAX, DPIVOT, EABS,
     $                   EMAX, EOLD, EPS, GL, GU, ISLEFT, ISRGHT, RTL,
     $                   RTOL, S1, S2, SAFMIN, SGNDEF, SIGMA, SPDIAM,
     $                   TAU, TMP, TMP1


*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL                        SLAMCH
      EXTERNAL           SLAMCH, LSAME

*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SLARNV, SLARRA, SLARRB, SLARRC, SLARRD,
     $                   SLASQ2, SLARRK
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
*     Decode RANGE
*
      IF( LSAME( RANGE, 'A' ) ) THEN
         IRANGE = ALLRNG
      ELSE IF( LSAME( RANGE, 'V' ) ) THEN
         IRANGE = VALRNG
      ELSE IF( LSAME( RANGE, 'I' ) ) THEN
         IRANGE = INDRNG
      END IF

      M = 0

*     Get machine constants
      SAFMIN = SLAMCH( 'S' )
      EPS = SLAMCH( 'P' )

*     Set parameters
      RTL = HNDRD*EPS
*     If one were ever to ask for less initial precision in BSRTOL,
*     one should keep in mind that for the subset case, the extremal
*     eigenvalues must be at least as accurate as the current setting
*     (eigenvalues in the middle need not as much accuracy)
      BSRTOL = SQRT(EPS)*(0.5E-3)

*     Treat case of 1x1 matrix for quick return
      IF( N.EQ.1 ) THEN
         IF( (IRANGE.EQ.ALLRNG).OR.
     $       ((IRANGE.EQ.VALRNG).AND.(D(1).GT.VL).AND.(D(1).LE.VU)).OR.
     $       ((IRANGE.EQ.INDRNG).AND.(IL.EQ.1).AND.(IU.EQ.1)) ) THEN
            M = 1
            W(1) = D(1)
*           The computation error of the eigenvalue is zero
            WERR(1) = ZERO
            WGAP(1) = ZERO
            IBLOCK( 1 ) = 1
            INDEXW( 1 ) = 1
            GERS(1) = D( 1 )
            GERS(2) = D( 1 )
         ENDIF
*        store the shift for the initial RRR, which is zero in this case
         E(1) = ZERO
         RETURN
      END IF

*     General case: tridiagonal matrix of order > 1
*
*     Init WERR, WGAP. Compute Gerschgorin intervals and spectral diameter.
*     Compute maximum off-diagonal entry and pivmin.
      GL = D(1)
      GU = D(1)
      EOLD = ZERO
      EMAX = ZERO
      E(N) = ZERO
      DO 5 I = 1,N
         WERR(I) = ZERO
         WGAP(I) = ZERO
         EABS = ABS( E(I) )
         IF( EABS .GE. EMAX ) THEN
            EMAX = EABS
         END IF
         TMP1 = EABS + EOLD
         GERS( 2*I-1) = D(I) - TMP1
         GL =  MIN( GL, GERS( 2*I - 1))
         GERS( 2*I ) = D(I) + TMP1
         GU = MAX( GU, GERS(2*I) )
         EOLD  = EABS
 5    CONTINUE
*     The minimum pivot allowed in the Sturm sequence for T
      PIVMIN = SAFMIN * MAX( ONE, EMAX**2 )
*     Compute spectral diameter. The Gerschgorin bounds give an
*     estimate that is wrong by at most a factor of SQRT(2)
      SPDIAM = GU - GL

*     Compute splitting points
      CALL SLARRA( N, D, E, E2, SPLTOL, SPDIAM,
     $                    NSPLIT, ISPLIT, IINFO )

*     Can force use of bisection instead of faster DQDS.
*     Option left in the code for future multisection work.
      FORCEB = .FALSE.

*     Initialize USEDQD, DQDS should be used for ALLRNG unless someone
*     explicitly wants bisection.
      USEDQD = (( IRANGE.EQ.ALLRNG ) .AND. (.NOT.FORCEB))

      IF( (IRANGE.EQ.ALLRNG) .AND. (.NOT. FORCEB) ) THEN
*        Set interval [VL,VU] that contains all eigenvalues
         VL = GL
         VU = GU
      ELSE
*        We call SLARRD to find crude approximations to the eigenvalues
*        in the desired range. In case IRANGE = INDRNG, we also obtain the
*        interval (VL,VU] that contains all the wanted eigenvalues.
*        An interval [LEFT,RIGHT] has converged if
*        RIGHT-LEFT.LT.RTOL*MAX(ABS(LEFT),ABS(RIGHT))
*        SLARRD needs a WORK of size 4*N, IWORK of size 3*N
         CALL SLARRD( RANGE, 'B', N, VL, VU, IL, IU, GERS,
     $                    BSRTOL, D, E, E2, PIVMIN, NSPLIT, ISPLIT,
     $                    MM, W, WERR, VL, VU, IBLOCK, INDEXW,
     $                    WORK, IWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = -1
            RETURN
         ENDIF
*        Make sure that the entries M+1 to N in W, WERR, IBLOCK, INDEXW are 0
         DO 14 I = MM+1,N
            W( I ) = ZERO
            WERR( I ) = ZERO
            IBLOCK( I ) = 0
            INDEXW( I ) = 0
 14      CONTINUE
      END IF


***
*     Loop over unreduced blocks
      IBEGIN = 1
      WBEGIN = 1
      DO 170 JBLK = 1, NSPLIT
         IEND = ISPLIT( JBLK )
         IN = IEND - IBEGIN + 1

*        1 X 1 block
         IF( IN.EQ.1 ) THEN
            IF( (IRANGE.EQ.ALLRNG).OR.( (IRANGE.EQ.VALRNG).AND.
     $         ( D( IBEGIN ).GT.VL ).AND.( D( IBEGIN ).LE.VU ) )
     $        .OR. ( (IRANGE.EQ.INDRNG).AND.(IBLOCK(WBEGIN).EQ.JBLK))
     $        ) THEN
               M = M + 1
               W( M ) = D( IBEGIN )
               WERR(M) = ZERO
*              The gap for a single block doesn't matter for the later
*              algorithm and is assigned an arbitrary large value
               WGAP(M) = ZERO
               IBLOCK( M ) = JBLK
               INDEXW( M ) = 1
               WBEGIN = WBEGIN + 1
            ENDIF
*           E( IEND ) holds the shift for the initial RRR
            E( IEND ) = ZERO
            IBEGIN = IEND + 1
            GO TO 170
         END IF
*
*        Blocks of size larger than 1x1
*
*        E( IEND ) will hold the shift for the initial RRR, for now set it =0
         E( IEND ) = ZERO
*
*        Find local outer bounds GL,GU for the block
         GL = D(IBEGIN)
         GU = D(IBEGIN)
         DO 15 I = IBEGIN , IEND
            GL = MIN( GERS( 2*I-1 ), GL )
            GU = MAX( GERS( 2*I ), GU )
 15      CONTINUE
         SPDIAM = GU - GL

         IF(.NOT. ((IRANGE.EQ.ALLRNG).AND.(.NOT.FORCEB)) ) THEN
*           Count the number of eigenvalues in the current block.
            MB = 0
            DO 20 I = WBEGIN,MM
               IF( IBLOCK(I).EQ.JBLK ) THEN
                  MB = MB+1
               ELSE
                  GOTO 21
               ENDIF
 20         CONTINUE
 21         CONTINUE

            IF( MB.EQ.0) THEN
*              No eigenvalue in the current block lies in the desired range
*              E( IEND ) holds the shift for the initial RRR
               E( IEND ) = ZERO
               IBEGIN = IEND + 1
               GO TO 170
            ELSE

*              Decide whether dqds or bisection is more efficient
               USEDQD = ( (MB .GT. FAC*IN) .AND. (.NOT.FORCEB) )
               WEND = WBEGIN + MB - 1
*              Calculate gaps for the current block
*              In later stages, when representations for individual
*              eigenvalues are different, we use SIGMA = E( IEND ).
               SIGMA = ZERO
               DO 30 I = WBEGIN, WEND - 1
                  WGAP( I ) = MAX( ZERO,
     $                        W(I+1)-WERR(I+1) - (W(I)+WERR(I)) )
 30            CONTINUE
               WGAP( WEND ) = MAX( ZERO,
     $                     VU - SIGMA - (W( WEND )+WERR( WEND )))
*              Find local index of the first and last desired evalue.
               INDL = INDEXW(WBEGIN)
               INDU = INDEXW( WEND )
            ENDIF
         ENDIF
         IF(( (IRANGE.EQ.ALLRNG) .AND. (.NOT. FORCEB) ).OR.USEDQD) THEN
*           Case of DQDS
*           Find approximations to the extremal eigenvalues of the block
            CALL SLARRK( IN, 1, GL, GU, D(IBEGIN),
     $               E2(IBEGIN), PIVMIN, RTL, TMP, TMP1, IINFO )
            IF( IINFO.NE.0 ) THEN
               INFO = -1
               RETURN
            ENDIF
            ISLEFT = MAX(GL, TMP - TMP1
     $               - HNDRD * EPS* ABS(TMP - TMP1))

            CALL SLARRK( IN, IN, GL, GU, D(IBEGIN),
     $               E2(IBEGIN), PIVMIN, RTL, TMP, TMP1, IINFO )
            IF( IINFO.NE.0 ) THEN
               INFO = -1
               RETURN
            ENDIF
            ISRGHT = MIN(GU, TMP + TMP1
     $                 + HNDRD * EPS * ABS(TMP + TMP1))
*           Improve the estimate of the spectral diameter
            SPDIAM = ISRGHT - ISLEFT
         ELSE
*           Case of bisection
*           Find approximations to the wanted extremal eigenvalues
            ISLEFT = MAX(GL, W(WBEGIN) - WERR(WBEGIN)
     $                  - HNDRD * EPS*ABS(W(WBEGIN)- WERR(WBEGIN) ))
            ISRGHT = MIN(GU,W(WEND) + WERR(WEND)
     $                  + HNDRD * EPS * ABS(W(WEND)+ WERR(WEND)))
         ENDIF


*        Decide whether the base representation for the current block
*        L_JBLK D_JBLK L_JBLK^T = T_JBLK - sigma_JBLK I
*        should be on the left or the right end of the current block.
*        The strategy is to shift to the end which is "more populated"
*        Furthermore, decide whether to use DQDS for the computation of
*        the eigenvalue approximations at the end of SLARRE or bisection.
*        dqds is chosen if all eigenvalues are desired or the number of
*        eigenvalues to be computed is large compared to the blocksize.
         IF( ( IRANGE.EQ.ALLRNG ) .AND. (.NOT.FORCEB) ) THEN
*           If all the eigenvalues have to be computed, we use dqd
            USEDQD = .TRUE.
*           INDL is the local index of the first eigenvalue to compute
            INDL = 1
            INDU = IN
*           MB =  number of eigenvalues to compute
            MB = IN
            WEND = WBEGIN + MB - 1
*           Define 1/4 and 3/4 points of the spectrum
            S1 = ISLEFT + FOURTH * SPDIAM
            S2 = ISRGHT - FOURTH * SPDIAM
         ELSE
*           SLARRD has computed IBLOCK and INDEXW for each eigenvalue
*           approximation.
*           choose sigma
            IF( USEDQD ) THEN
               S1 = ISLEFT + FOURTH * SPDIAM
               S2 = ISRGHT - FOURTH * SPDIAM
            ELSE
               TMP = MIN(ISRGHT,VU) -  MAX(ISLEFT,VL)
               S1 =  MAX(ISLEFT,VL) + FOURTH * TMP
               S2 =  MIN(ISRGHT,VU) - FOURTH * TMP
            ENDIF
         ENDIF

*        Compute the negcount at the 1/4 and 3/4 points
         IF(MB.GT.1) THEN
            CALL SLARRC( 'T', IN, S1, S2, D(IBEGIN),
     $                    E(IBEGIN), PIVMIN, CNT, CNT1, CNT2, IINFO)
         ENDIF

         IF(MB.EQ.1) THEN
            SIGMA = GL
            SGNDEF = ONE
         ELSEIF( CNT1 - INDL .GE. INDU - CNT2 ) THEN
            IF( ( IRANGE.EQ.ALLRNG ) .AND. (.NOT.FORCEB) ) THEN
               SIGMA = MAX(ISLEFT,GL)
            ELSEIF( USEDQD ) THEN
*              use Gerschgorin bound as shift to get pos def matrix
*              for dqds
               SIGMA = ISLEFT
            ELSE
*              use approximation of the first desired eigenvalue of the
*              block as shift
               SIGMA = MAX(ISLEFT,VL)
            ENDIF
            SGNDEF = ONE
         ELSE
            IF( ( IRANGE.EQ.ALLRNG ) .AND. (.NOT.FORCEB) ) THEN
               SIGMA = MIN(ISRGHT,GU)
            ELSEIF( USEDQD ) THEN
*              use Gerschgorin bound as shift to get neg def matrix
*              for dqds
               SIGMA = ISRGHT
            ELSE
*              use approximation of the first desired eigenvalue of the
*              block as shift
               SIGMA = MIN(ISRGHT,VU)
            ENDIF
            SGNDEF = -ONE
         ENDIF


*        An initial SIGMA has been chosen that will be used for computing
*        T - SIGMA I = L D L^T
*        Define the increment TAU of the shift in case the initial shift
*        needs to be refined to obtain a factorization with not too much
*        element growth.
         IF( USEDQD ) THEN
*           The initial SIGMA was to the outer end of the spectrum
*           the matrix is definite and we need not retreat.
            TAU = SPDIAM*EPS*N + TWO*PIVMIN
            TAU = MAX( TAU,TWO*EPS*ABS(SIGMA) )
         ELSE
            IF(MB.GT.1) THEN
               CLWDTH = W(WEND) + WERR(WEND) - W(WBEGIN) - WERR(WBEGIN)
               AVGAP = ABS(CLWDTH / REAL(WEND-WBEGIN))
               IF( SGNDEF.EQ.ONE ) THEN
                  TAU = HALF*MAX(WGAP(WBEGIN),AVGAP)
                  TAU = MAX(TAU,WERR(WBEGIN))
               ELSE
                  TAU = HALF*MAX(WGAP(WEND-1),AVGAP)
                  TAU = MAX(TAU,WERR(WEND))
               ENDIF
            ELSE
               TAU = WERR(WBEGIN)
            ENDIF
         ENDIF
*
         DO 80 IDUM = 1, MAXTRY
*           Compute L D L^T factorization of tridiagonal matrix T - sigma I.
*           Store D in WORK(1:IN), L in WORK(IN+1:2*IN), and reciprocals of
*           pivots in WORK(2*IN+1:3*IN)
            DPIVOT = D( IBEGIN ) - SIGMA
            WORK( 1 ) = DPIVOT
            DMAX = ABS( WORK(1) )
            J = IBEGIN
            DO 70 I = 1, IN - 1
               WORK( 2*IN+I ) = ONE / WORK( I )
               TMP = E( J )*WORK( 2*IN+I )
               WORK( IN+I ) = TMP
               DPIVOT = ( D( J+1 )-SIGMA ) - TMP*E( J )
               WORK( I+1 ) = DPIVOT
               DMAX = MAX( DMAX, ABS(DPIVOT) )
               J = J + 1
 70         CONTINUE
*           check for element growth
            IF( DMAX .GT. MAXGROWTH*SPDIAM ) THEN
               NOREP = .TRUE.
            ELSE
               NOREP = .FALSE.
            ENDIF
            IF( USEDQD .AND. .NOT.NOREP ) THEN
*              Ensure the definiteness of the representation
*              All entries of D (of L D L^T) must have the same sign
               DO 71 I = 1, IN
                  TMP = SGNDEF*WORK( I )
                  IF( TMP.LT.ZERO ) NOREP = .TRUE.
 71            CONTINUE
            ENDIF
            IF(NOREP) THEN
*              Note that in the case of IRANGE=ALLRNG, we use the Gerschgorin
*              shift which makes the matrix definite. So we should end up
*              here really only in the case of IRANGE = VALRNG or INDRNG.
               IF( IDUM.EQ.MAXTRY-1 ) THEN
                  IF( SGNDEF.EQ.ONE ) THEN
*                    The fudged Gerschgorin shift should succeed
                     SIGMA =
     $                    GL - FUDGE*SPDIAM*EPS*N - FUDGE*TWO*PIVMIN
                  ELSE
                     SIGMA =
     $                    GU + FUDGE*SPDIAM*EPS*N + FUDGE*TWO*PIVMIN
                  END IF
               ELSE
                  SIGMA = SIGMA - SGNDEF * TAU
                  TAU = TWO * TAU
               END IF
            ELSE
*              an initial RRR is found
               GO TO 83
            END IF
 80      CONTINUE
*        if the program reaches this point, no base representation could be
*        found in MAXTRY iterations.
         INFO = 2
         RETURN

 83      CONTINUE
*        At this point, we have found an initial base representation
*        T - SIGMA I = L D L^T with not too much element growth.
*        Store the shift.
         E( IEND ) = SIGMA
*        Store D and L.
         CALL SCOPY( IN, WORK, 1, D( IBEGIN ), 1 )
         CALL SCOPY( IN-1, WORK( IN+1 ), 1, E( IBEGIN ), 1 )


         IF(MB.GT.1 ) THEN
*
*           Perturb each entry of the base representation by a small
*           (but random) relative amount to overcome difficulties with
*           glued matrices.
*
            DO 122 I = 1, 4
               ISEED( I ) = 1
 122        CONTINUE

            CALL SLARNV(2, ISEED, 2*IN-1, WORK(1))
            DO 125 I = 1,IN-1
               D(IBEGIN+I-1) = D(IBEGIN+I-1)*(ONE+EPS*PERT*WORK(I))
               E(IBEGIN+I-1) = E(IBEGIN+I-1)*(ONE+EPS*PERT*WORK(IN+I))
 125        CONTINUE
            D(IEND) = D(IEND)*(ONE+EPS*FOUR*WORK(IN))
*
         ENDIF
*
*        Don't update the Gerschgorin intervals because keeping track
*        of the updates would be too much work in SLARRV.
*        We update W instead and use it to locate the proper Gerschgorin
*        intervals.

*        Compute the required eigenvalues of L D L' by bisection or dqds
         IF ( .NOT.USEDQD ) THEN
*           If SLARRD has been used, shift the eigenvalue approximations
*           according to their representation. This is necessary for
*           a uniform SLARRV since dqds computes eigenvalues of the
*           shifted representation. In SLARRV, W will always hold the
*           UNshifted eigenvalue approximation.
            DO 134 J=WBEGIN,WEND
               W(J) = W(J) - SIGMA
               WERR(J) = WERR(J) + ABS(W(J)) * EPS
 134        CONTINUE
*           call SLARRB to reduce eigenvalue error of the approximations
*           from SLARRD
            DO 135 I = IBEGIN, IEND-1
               WORK( I ) = D( I ) * E( I )**2
 135        CONTINUE
*           use bisection to find EV from INDL to INDU
            CALL SLARRB(IN, D(IBEGIN), WORK(IBEGIN),
     $                  INDL, INDU, RTOL1, RTOL2, INDL-1,
     $                  W(WBEGIN), WGAP(WBEGIN), WERR(WBEGIN),
     $                  WORK( 2*N+1 ), IWORK, PIVMIN, SPDIAM,
     $                  IN, IINFO )
            IF( IINFO .NE. 0 ) THEN
               INFO = -4
               RETURN
            END IF
*           SLARRB computes all gaps correctly except for the last one
*           Record distance to VU/GU
            WGAP( WEND ) = MAX( ZERO,
     $           ( VU-SIGMA ) - ( W( WEND ) + WERR( WEND ) ) )
            DO 138 I = INDL, INDU
               M = M + 1
               IBLOCK(M) = JBLK
               INDEXW(M) = I
 138        CONTINUE
         ELSE
*           Call dqds to get all eigs (and then possibly delete unwanted
*           eigenvalues).
*           Note that dqds finds the eigenvalues of the L D L^T representation
*           of T to high relative accuracy. High relative accuracy
*           might be lost when the shift of the RRR is subtracted to obtain
*           the eigenvalues of T. However, T is not guaranteed to define its
*           eigenvalues to high relative accuracy anyway.
*           Set RTOL to the order of the tolerance used in SLASQ2
*           This is an ESTIMATED error, the worst case bound is 4*N*EPS
*           which is usually too large and requires unnecessary work to be
*           done by bisection when computing the eigenvectors
            RTOL = LOG(REAL(IN)) * FOUR * EPS
            J = IBEGIN
            DO 140 I = 1, IN - 1
               WORK( 2*I-1 ) = ABS( D( J ) )
               WORK( 2*I ) = E( J )*E( J )*WORK( 2*I-1 )
               J = J + 1
  140       CONTINUE
            WORK( 2*IN-1 ) = ABS( D( IEND ) )
            WORK( 2*IN ) = ZERO
            CALL SLASQ2( IN, WORK, IINFO )
            IF( IINFO .NE. 0 ) THEN
*              If IINFO = -5 then an index is part of a tight cluster
*              and should be changed. The index is in IWORK(1) and the
*              gap is in WORK(N+1)
               INFO = -5
               RETURN
            ELSE
*              Test that all eigenvalues are positive as expected
               DO 149 I = 1, IN
                  IF( WORK( I ).LT.ZERO ) THEN
                     INFO = -6
                     RETURN
                  ENDIF
 149           CONTINUE
            END IF
            IF( SGNDEF.GT.ZERO ) THEN
               DO 150 I = INDL, INDU
                  M = M + 1
                  W( M ) = WORK( IN-I+1 )
                  IBLOCK( M ) = JBLK
                  INDEXW( M ) = I
 150           CONTINUE
            ELSE
               DO 160 I = INDL, INDU
                  M = M + 1
                  W( M ) = -WORK( I )
                  IBLOCK( M ) = JBLK
                  INDEXW( M ) = I
 160           CONTINUE
            END IF

            DO 165 I = M - MB + 1, M
*              the value of RTOL below should be the tolerance in SLASQ2
               WERR( I ) = RTOL * ABS( W(I) )
 165        CONTINUE
            DO 166 I = M - MB + 1, M - 1
*              compute the right gap between the intervals
               WGAP( I ) = MAX( ZERO,
     $                          W(I+1)-WERR(I+1) - (W(I)+WERR(I)) )
 166        CONTINUE
            WGAP( M ) = MAX( ZERO,
     $           ( VU-SIGMA ) - ( W( M ) + WERR( M ) ) )
         END IF
*        proceed with next block
         IBEGIN = IEND + 1
         WBEGIN = WEND + 1
 170  CONTINUE
*

      RETURN
*
*     end of SLARRE
*
      END
