*> \brief \b DLARRD computes the eigenvalues of a symmetric tridiagonal matrix to suitable accuracy.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLARRD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlarrd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlarrd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlarrd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLARRD( RANGE, ORDER, N, VL, VU, IL, IU, GERS,
*                           RELTOL, D, E, E2, PIVMIN, NSPLIT, ISPLIT,
*                           M, W, WERR, WL, WU, IBLOCK, INDEXW,
*                           WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          ORDER, RANGE
*       INTEGER            IL, INFO, IU, M, N, NSPLIT
*       DOUBLE PRECISION    PIVMIN, RELTOL, VL, VU, WL, WU
*       ..
*       .. Array Arguments ..
*       INTEGER            IBLOCK( * ), INDEXW( * ),
*      $                   ISPLIT( * ), IWORK( * )
*       DOUBLE PRECISION   D( * ), E( * ), E2( * ),
*      $                   GERS( * ), W( * ), WERR( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLARRD computes the eigenvalues of a symmetric tridiagonal
*> matrix T to suitable accuracy. This is an auxiliary code to be
*> called from DSTEMR.
*> The user may ask for all eigenvalues, all eigenvalues
*> in the half-open interval (VL, VU], or the IL-th through IU-th
*> eigenvalues.
*>
*> To avoid overflow, the matrix must be scaled so that its
*> largest element is no greater than overflow**(1/2) * underflow**(1/4) in absolute value, and for greatest
*> accuracy, it should not be much smaller than that.
*>
*> See W. Kahan "Accurate Eigenvalues of a Symmetric Tridiagonal
*> Matrix", Report CS41, Computer Science Dept., Stanford
*> University, July 21, 1966.
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
*> \param[in] ORDER
*> \verbatim
*>          ORDER is CHARACTER*1
*>          = 'B': ("By Block") the eigenvalues will be grouped by
*>                              split-off block (see IBLOCK, ISPLIT) and
*>                              ordered from smallest to largest within
*>                              the block.
*>          = 'E': ("Entire matrix")
*>                              the eigenvalues for the entire matrix
*>                              will be ordered from smallest to
*>                              largest.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the tridiagonal matrix T.  N >= 0.
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is DOUBLE PRECISION
*>          If RANGE='V', the lower bound of the interval to
*>          be searched for eigenvalues.  Eigenvalues less than or equal
*>          to VL, or greater than VU, will not be returned.  VL < VU.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>          VU is DOUBLE PRECISION
*>          If RANGE='V', the upper bound of the interval to
*>          be searched for eigenvalues.  Eigenvalues less than or equal
*>          to VL, or greater than VU, will not be returned.  VL < VU.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] IL
*> \verbatim
*>          IL is INTEGER
*>          If RANGE='I', the index of the
*>          smallest eigenvalue to be returned.
*>          1 <= IL <= IU <= N, if N > 0; IL = 1 and IU = 0 if N = 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[in] IU
*> \verbatim
*>          IU is INTEGER
*>          If RANGE='I', the index of the
*>          largest eigenvalue to be returned.
*>          1 <= IL <= IU <= N, if N > 0; IL = 1 and IU = 0 if N = 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[in] GERS
*> \verbatim
*>          GERS is DOUBLE PRECISION array, dimension (2*N)
*>          The N Gerschgorin intervals (the i-th Gerschgorin interval
*>          is (GERS(2*i-1), GERS(2*i)).
*> \endverbatim
*>
*> \param[in] RELTOL
*> \verbatim
*>          RELTOL is DOUBLE PRECISION
*>          The minimum relative width of an interval.  When an interval
*>          is narrower than RELTOL times the larger (in
*>          magnitude) endpoint, then it is considered to be
*>          sufficiently small, i.e., converged.  Note: this should
*>          always be at least radix*machine epsilon.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The n diagonal elements of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The (n-1) off-diagonal elements of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] E2
*> \verbatim
*>          E2 is DOUBLE PRECISION array, dimension (N-1)
*>          The (n-1) squared off-diagonal elements of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is DOUBLE PRECISION
*>          The minimum pivot allowed in the Sturm sequence for T.
*> \endverbatim
*>
*> \param[in] NSPLIT
*> \verbatim
*>          NSPLIT is INTEGER
*>          The number of diagonal blocks in the matrix T.
*>          1 <= NSPLIT <= N.
*> \endverbatim
*>
*> \param[in] ISPLIT
*> \verbatim
*>          ISPLIT is INTEGER array, dimension (N)
*>          The splitting points, at which T breaks up into submatrices.
*>          The first submatrix consists of rows/columns 1 to ISPLIT(1),
*>          the second of rows/columns ISPLIT(1)+1 through ISPLIT(2),
*>          etc., and the NSPLIT-th consists of rows/columns
*>          ISPLIT(NSPLIT-1)+1 through ISPLIT(NSPLIT)=N.
*>          (Only the first NSPLIT elements will actually be used, but
*>          since the user cannot know a priori what value NSPLIT will
*>          have, N words must be reserved for ISPLIT.)
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The actual number of eigenvalues found. 0 <= M <= N.
*>          (See also the description of INFO=2,3.)
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is DOUBLE PRECISION array, dimension (N)
*>          On exit, the first M elements of W will contain the
*>          eigenvalue approximations. DLARRD computes an interval
*>          I_j = (a_j, b_j] that includes eigenvalue j. The eigenvalue
*>          approximation is given as the interval midpoint
*>          W(j)= ( a_j + b_j)/2. The corresponding error is bounded by
*>          WERR(j) = abs( a_j - b_j)/2
*> \endverbatim
*>
*> \param[out] WERR
*> \verbatim
*>          WERR is DOUBLE PRECISION array, dimension (N)
*>          The error bound on the corresponding eigenvalue approximation
*>          in W.
*> \endverbatim
*>
*> \param[out] WL
*> \verbatim
*>          WL is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[out] WU
*> \verbatim
*>          WU is DOUBLE PRECISION
*>          The interval (WL, WU] contains all the wanted eigenvalues.
*>          If RANGE='V', then WL=VL and WU=VU.
*>          If RANGE='A', then WL and WU are the global Gerschgorin bounds
*>                        on the spectrum.
*>          If RANGE='I', then WL and WU are computed by DLAEBZ from the
*>                        index range specified.
*> \endverbatim
*>
*> \param[out] IBLOCK
*> \verbatim
*>          IBLOCK is INTEGER array, dimension (N)
*>          At each row/column j where E(j) is zero or small, the
*>          matrix T is considered to split into a block diagonal
*>          matrix.  On exit, if INFO = 0, IBLOCK(i) specifies to which
*>          block (from 1 to the number of blocks) the eigenvalue W(i)
*>          belongs.  (DLARRD may use the remaining N-M elements as
*>          workspace.)
*> \endverbatim
*>
*> \param[out] INDEXW
*> \verbatim
*>          INDEXW is INTEGER array, dimension (N)
*>          The indices of the eigenvalues within each block (submatrix);
*>          for example, INDEXW(i)= j and IBLOCK(i)=k imply that the
*>          i-th eigenvalue W(i) is the j-th eigenvalue in block k.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (4*N)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (3*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  some or all of the eigenvalues failed to converge or
*>                were not computed:
*>                =1 or 3: Bisection failed to converge for some
*>                        eigenvalues; these eigenvalues are flagged by a
*>                        negative block number.  The effect is that the
*>                        eigenvalues may not be as accurate as the
*>                        absolute and relative tolerances.  This is
*>                        generally caused by unexpectedly inaccurate
*>                        arithmetic.
*>                =2 or 3: RANGE='I' only: Not all of the eigenvalues
*>                        IL:IU were found.
*>                        Effect: M < IU+1-IL
*>                        Cause:  non-monotonic arithmetic, causing the
*>                                Sturm sequence to be non-monotonic.
*>                        Cure:   recalculate, using RANGE='A', and pick
*>                                out eigenvalues IL:IU.  In some cases,
*>                                increasing the PARAMETER "FUDGE" may
*>                                make things work.
*>                = 4:    RANGE='I', and the Gershgorin interval
*>                        initially used was too small.  No eigenvalues
*>                        were computed.
*>                        Probable cause: your machine has sloppy
*>                                        floating-point arithmetic.
*>                        Cure: Increase the PARAMETER "FUDGE",
*>                              recompile, and try again.
*> \endverbatim
*
*> \par Internal Parameters:
*  =========================
*>
*> \verbatim
*>  FUDGE   DOUBLE PRECISION, default = 2
*>          A "fudge factor" to widen the Gershgorin intervals.  Ideally,
*>          a value of 1 should work, but on machines with sloppy
*>          arithmetic, this needs to be larger.  The default for
*>          publicly released versions should be large enough to handle
*>          the worst machine around.  Note that this has no effect
*>          on accuracy of the solution.
*> \endverbatim
*>
*> \par Contributors:
*  ==================
*>
*>     W. Kahan, University of California, Berkeley, USA \n
*>     Beresford Parlett, University of California, Berkeley, USA \n
*>     Jim Demmel, University of California, Berkeley, USA \n
*>     Inderjit Dhillon, University of Texas, Austin, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*>     Christof Voemel, University of California, Berkeley, USA \n
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
*  =====================================================================
      SUBROUTINE DLARRD( RANGE, ORDER, N, VL, VU, IL, IU, GERS,
     $                    RELTOL, D, E, E2, PIVMIN, NSPLIT, ISPLIT,
     $                    M, W, WERR, WL, WU, IBLOCK, INDEXW,
     $                    WORK, IWORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          ORDER, RANGE
      INTEGER            IL, INFO, IU, M, N, NSPLIT
      DOUBLE PRECISION    PIVMIN, RELTOL, VL, VU, WL, WU
*     ..
*     .. Array Arguments ..
      INTEGER            IBLOCK( * ), INDEXW( * ),
     $                   ISPLIT( * ), IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), E2( * ),
     $                   GERS( * ), W( * ), WERR( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO, HALF, FUDGE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0,
     $                     TWO = 2.0D0, HALF = ONE/TWO,
     $                     FUDGE = TWO )
      INTEGER   ALLRNG, VALRNG, INDRNG
      PARAMETER ( ALLRNG = 1, VALRNG = 2, INDRNG = 3 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NCNVRG, TOOFEW
      INTEGER            I, IB, IBEGIN, IDISCL, IDISCU, IE, IEND, IINFO,
     $                   IM, IN, IOFF, IOUT, IRANGE, ITMAX, ITMP1,
     $                   ITMP2, IW, IWOFF, J, JBLK, JDISC, JE, JEE, NB,
     $                   NWL, NWU
      DOUBLE PRECISION   ATOLI, EPS, GL, GU, RTOLI, TMP1, TMP2,
     $                   TNORM, UFLOW, WKILL, WLU, WUL

*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           LSAME, ILAENV, DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLAEBZ
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, INT, LOG, MAX, MIN
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
      ELSE
         IRANGE = 0
      END IF
*
*     Check for Errors
*
      IF( IRANGE.LE.0 ) THEN
         INFO = -1
      ELSE IF( .NOT.(LSAME(ORDER,'B').OR.LSAME(ORDER,'E')) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( IRANGE.EQ.VALRNG ) THEN
         IF( VL.GE.VU )
     $      INFO = -5
      ELSE IF( IRANGE.EQ.INDRNG .AND.
     $        ( IL.LT.1 .OR. IL.GT.MAX( 1, N ) ) ) THEN
         INFO = -6
      ELSE IF( IRANGE.EQ.INDRNG .AND.
     $        ( IU.LT.MIN( N, IL ) .OR. IU.GT.N ) ) THEN
         INFO = -7
      END IF
*
      IF( INFO.NE.0 ) THEN
         RETURN
      END IF

*     Initialize error flags
      INFO = 0
      NCNVRG = .FALSE.
      TOOFEW = .FALSE.

*     Quick return if possible
      M = 0
      IF( N.EQ.0 ) RETURN

*     Simplification:
      IF( IRANGE.EQ.INDRNG .AND. IL.EQ.1 .AND. IU.EQ.N ) IRANGE = 1

*     Get machine constants
      EPS = DLAMCH( 'P' )
      UFLOW = DLAMCH( 'U' )


*     Special Case when N=1
*     Treat case of 1x1 matrix for quick return
      IF( N.EQ.1 ) THEN
         IF( (IRANGE.EQ.ALLRNG).OR.
     $       ((IRANGE.EQ.VALRNG).AND.(D(1).GT.VL).AND.(D(1).LE.VU)).OR.
     $       ((IRANGE.EQ.INDRNG).AND.(IL.EQ.1).AND.(IU.EQ.1)) ) THEN
            M = 1
            W(1) = D(1)
*           The computation error of the eigenvalue is zero
            WERR(1) = ZERO
            IBLOCK( 1 ) = 1
            INDEXW( 1 ) = 1
         ENDIF
         RETURN
      END IF

*     NB is the minimum vector length for vector bisection, or 0
*     if only scalar is to be done.
      NB = ILAENV( 1, 'DSTEBZ', ' ', N, -1, -1, -1 )
      IF( NB.LE.1 ) NB = 0

*     Find global spectral radius
      GL = D(1)
      GU = D(1)
      DO 5 I = 1,N
         GL =  MIN( GL, GERS( 2*I - 1))
         GU = MAX( GU, GERS(2*I) )
 5    CONTINUE
*     Compute global Gerschgorin bounds and spectral diameter
      TNORM = MAX( ABS( GL ), ABS( GU ) )
      GL = GL - FUDGE*TNORM*EPS*N - FUDGE*TWO*PIVMIN
      GU = GU + FUDGE*TNORM*EPS*N + FUDGE*TWO*PIVMIN
*     [JAN/28/2009] remove the line below since SPDIAM variable not use
*     SPDIAM = GU - GL
*     Input arguments for DLAEBZ:
*     The relative tolerance.  An interval (a,b] lies within
*     "relative tolerance" if  b-a < RELTOL*max(|a|,|b|),
      RTOLI = RELTOL
*     Set the absolute tolerance for interval convergence to zero to force
*     interval convergence based on relative size of the interval.
*     This is dangerous because intervals might not converge when RELTOL is
*     small. But at least a very small number should be selected so that for
*     strongly graded matrices, the code can get relatively accurate
*     eigenvalues.
      ATOLI = FUDGE*TWO*UFLOW + FUDGE*TWO*PIVMIN

      IF( IRANGE.EQ.INDRNG ) THEN

*        RANGE='I': Compute an interval containing eigenvalues
*        IL through IU. The initial interval [GL,GU] from the global
*        Gerschgorin bounds GL and GU is refined by DLAEBZ.
         ITMAX = INT( ( LOG( TNORM+PIVMIN )-LOG( PIVMIN ) ) /
     $           LOG( TWO ) ) + 2
         WORK( N+1 ) = GL
         WORK( N+2 ) = GL
         WORK( N+3 ) = GU
         WORK( N+4 ) = GU
         WORK( N+5 ) = GL
         WORK( N+6 ) = GU
         IWORK( 1 ) = -1
         IWORK( 2 ) = -1
         IWORK( 3 ) = N + 1
         IWORK( 4 ) = N + 1
         IWORK( 5 ) = IL - 1
         IWORK( 6 ) = IU
*
         CALL DLAEBZ( 3, ITMAX, N, 2, 2, NB, ATOLI, RTOLI, PIVMIN,
     $         D, E, E2, IWORK( 5 ), WORK( N+1 ), WORK( N+5 ), IOUT,
     $                IWORK, W, IBLOCK, IINFO )
         IF( IINFO .NE. 0 ) THEN
            INFO = IINFO
            RETURN
         END IF
*        On exit, output intervals may not be ordered by ascending negcount
         IF( IWORK( 6 ).EQ.IU ) THEN
            WL = WORK( N+1 )
            WLU = WORK( N+3 )
            NWL = IWORK( 1 )
            WU = WORK( N+4 )
            WUL = WORK( N+2 )
            NWU = IWORK( 4 )
         ELSE
            WL = WORK( N+2 )
            WLU = WORK( N+4 )
            NWL = IWORK( 2 )
            WU = WORK( N+3 )
            WUL = WORK( N+1 )
            NWU = IWORK( 3 )
         END IF
*        On exit, the interval [WL, WLU] contains a value with negcount NWL,
*        and [WUL, WU] contains a value with negcount NWU.
         IF( NWL.LT.0 .OR. NWL.GE.N .OR. NWU.LT.1 .OR. NWU.GT.N ) THEN
            INFO = 4
            RETURN
         END IF

      ELSEIF( IRANGE.EQ.VALRNG ) THEN
         WL = VL
         WU = VU

      ELSEIF( IRANGE.EQ.ALLRNG ) THEN
         WL = GL
         WU = GU
      ENDIF



*     Find Eigenvalues -- Loop Over blocks and recompute NWL and NWU.
*     NWL accumulates the number of eigenvalues .le. WL,
*     NWU accumulates the number of eigenvalues .le. WU
      M = 0
      IEND = 0
      INFO = 0
      NWL = 0
      NWU = 0
*
      DO 70 JBLK = 1, NSPLIT
         IOFF = IEND
         IBEGIN = IOFF + 1
         IEND = ISPLIT( JBLK )
         IN = IEND - IOFF
*
         IF( IN.EQ.1 ) THEN
*           1x1 block
            IF( WL.GE.D( IBEGIN )-PIVMIN )
     $         NWL = NWL + 1
            IF( WU.GE.D( IBEGIN )-PIVMIN )
     $         NWU = NWU + 1
            IF( IRANGE.EQ.ALLRNG .OR.
     $           ( WL.LT.D( IBEGIN )-PIVMIN
     $             .AND. WU.GE. D( IBEGIN )-PIVMIN ) ) THEN
               M = M + 1
               W( M ) = D( IBEGIN )
               WERR(M) = ZERO
*              The gap for a single block doesn't matter for the later
*              algorithm and is assigned an arbitrary large value
               IBLOCK( M ) = JBLK
               INDEXW( M ) = 1
            END IF

*        Disabled 2x2 case because of a failure on the following matrix
*        RANGE = 'I', IL = IU = 4
*          Original Tridiagonal, d = [
*           -0.150102010615740E+00
*           -0.849897989384260E+00
*           -0.128208148052635E-15
*            0.128257718286320E-15
*          ];
*          e = [
*           -0.357171383266986E+00
*           -0.180411241501588E-15
*           -0.175152352710251E-15
*          ];
*
*         ELSE IF( IN.EQ.2 ) THEN
**           2x2 block
*            DISC = SQRT( (HALF*(D(IBEGIN)-D(IEND)))**2 + E(IBEGIN)**2 )
*            TMP1 = HALF*(D(IBEGIN)+D(IEND))
*            L1 = TMP1 - DISC
*            IF( WL.GE. L1-PIVMIN )
*     $         NWL = NWL + 1
*            IF( WU.GE. L1-PIVMIN )
*     $         NWU = NWU + 1
*            IF( IRANGE.EQ.ALLRNG .OR. ( WL.LT.L1-PIVMIN .AND. WU.GE.
*     $          L1-PIVMIN ) ) THEN
*               M = M + 1
*               W( M ) = L1
**              The uncertainty of eigenvalues of a 2x2 matrix is very small
*               WERR( M ) = EPS * ABS( W( M ) ) * TWO
*               IBLOCK( M ) = JBLK
*               INDEXW( M ) = 1
*            ENDIF
*            L2 = TMP1 + DISC
*            IF( WL.GE. L2-PIVMIN )
*     $         NWL = NWL + 1
*            IF( WU.GE. L2-PIVMIN )
*     $         NWU = NWU + 1
*            IF( IRANGE.EQ.ALLRNG .OR. ( WL.LT.L2-PIVMIN .AND. WU.GE.
*     $          L2-PIVMIN ) ) THEN
*               M = M + 1
*               W( M ) = L2
**              The uncertainty of eigenvalues of a 2x2 matrix is very small
*               WERR( M ) = EPS * ABS( W( M ) ) * TWO
*               IBLOCK( M ) = JBLK
*               INDEXW( M ) = 2
*            ENDIF
         ELSE
*           General Case - block of size IN >= 2
*           Compute local Gerschgorin interval and use it as the initial
*           interval for DLAEBZ
            GU = D( IBEGIN )
            GL = D( IBEGIN )
            TMP1 = ZERO

            DO 40 J = IBEGIN, IEND
               GL =  MIN( GL, GERS( 2*J - 1))
               GU = MAX( GU, GERS(2*J) )
   40       CONTINUE
*           [JAN/28/2009]
*           change SPDIAM by TNORM in lines 2 and 3 thereafter
*           line 1: remove computation of SPDIAM (not useful anymore)
*           SPDIAM = GU - GL
*           GL = GL - FUDGE*SPDIAM*EPS*IN - FUDGE*PIVMIN
*           GU = GU + FUDGE*SPDIAM*EPS*IN + FUDGE*PIVMIN
            GL = GL - FUDGE*TNORM*EPS*IN - FUDGE*PIVMIN
            GU = GU + FUDGE*TNORM*EPS*IN + FUDGE*PIVMIN
*
            IF( IRANGE.GT.1 ) THEN
               IF( GU.LT.WL ) THEN
*                 the local block contains none of the wanted eigenvalues
                  NWL = NWL + IN
                  NWU = NWU + IN
                  GO TO 70
               END IF
*              refine search interval if possible, only range (WL,WU] matters
               GL = MAX( GL, WL )
               GU = MIN( GU, WU )
               IF( GL.GE.GU )
     $            GO TO 70
            END IF

*           Find negcount of initial interval boundaries GL and GU
            WORK( N+1 ) = GL
            WORK( N+IN+1 ) = GU
            CALL DLAEBZ( 1, 0, IN, IN, 1, NB, ATOLI, RTOLI, PIVMIN,
     $                   D( IBEGIN ), E( IBEGIN ), E2( IBEGIN ),
     $                   IDUMMA, WORK( N+1 ), WORK( N+2*IN+1 ), IM,
     $                   IWORK, W( M+1 ), IBLOCK( M+1 ), IINFO )
            IF( IINFO .NE. 0 ) THEN
               INFO = IINFO
               RETURN
            END IF
*
            NWL = NWL + IWORK( 1 )
            NWU = NWU + IWORK( IN+1 )
            IWOFF = M - IWORK( 1 )

*           Compute Eigenvalues
            ITMAX = INT( ( LOG( GU-GL+PIVMIN )-LOG( PIVMIN ) ) /
     $              LOG( TWO ) ) + 2
            CALL DLAEBZ( 2, ITMAX, IN, IN, 1, NB, ATOLI, RTOLI, PIVMIN,
     $                   D( IBEGIN ), E( IBEGIN ), E2( IBEGIN ),
     $                   IDUMMA, WORK( N+1 ), WORK( N+2*IN+1 ), IOUT,
     $                   IWORK, W( M+1 ), IBLOCK( M+1 ), IINFO )
            IF( IINFO .NE. 0 ) THEN
               INFO = IINFO
               RETURN
            END IF
*
*           Copy eigenvalues into W and IBLOCK
*           Use -JBLK for block number for unconverged eigenvalues.
*           Loop over the number of output intervals from DLAEBZ
            DO 60 J = 1, IOUT
*              eigenvalue approximation is middle point of interval
               TMP1 = HALF*( WORK( J+N )+WORK( J+IN+N ) )
*              semi length of error interval
               TMP2 = HALF*ABS( WORK( J+N )-WORK( J+IN+N ) )
               IF( J.GT.IOUT-IINFO ) THEN
*                 Flag non-convergence.
                  NCNVRG = .TRUE.
                  IB = -JBLK
               ELSE
                  IB = JBLK
               END IF
               DO 50 JE = IWORK( J ) + 1 + IWOFF,
     $                 IWORK( J+IN ) + IWOFF
                  W( JE ) = TMP1
                  WERR( JE ) = TMP2
                  INDEXW( JE ) = JE - IWOFF
                  IBLOCK( JE ) = IB
   50          CONTINUE
   60       CONTINUE
*
            M = M + IM
         END IF
   70 CONTINUE

*     If RANGE='I', then (WL,WU) contains eigenvalues NWL+1,...,NWU
*     If NWL+1 < IL or NWU > IU, discard extra eigenvalues.
      IF( IRANGE.EQ.INDRNG ) THEN
         IDISCL = IL - 1 - NWL
         IDISCU = NWU - IU
*
         IF( IDISCL.GT.0 ) THEN
            IM = 0
            DO 80 JE = 1, M
*              Remove some of the smallest eigenvalues from the left so that
*              at the end IDISCL =0. Move all eigenvalues up to the left.
               IF( W( JE ).LE.WLU .AND. IDISCL.GT.0 ) THEN
                  IDISCL = IDISCL - 1
               ELSE
                  IM = IM + 1
                  W( IM ) = W( JE )
                  WERR( IM ) = WERR( JE )
                  INDEXW( IM ) = INDEXW( JE )
                  IBLOCK( IM ) = IBLOCK( JE )
               END IF
 80         CONTINUE
            M = IM
         END IF
         IF( IDISCU.GT.0 ) THEN
*           Remove some of the largest eigenvalues from the right so that
*           at the end IDISCU =0. Move all eigenvalues up to the left.
            IM=M+1
            DO 81 JE = M, 1, -1
               IF( W( JE ).GE.WUL .AND. IDISCU.GT.0 ) THEN
                  IDISCU = IDISCU - 1
               ELSE
                  IM = IM - 1
                  W( IM ) = W( JE )
                  WERR( IM ) = WERR( JE )
                  INDEXW( IM ) = INDEXW( JE )
                  IBLOCK( IM ) = IBLOCK( JE )
               END IF
 81         CONTINUE
            JEE = 0
            DO 82 JE = IM, M
               JEE = JEE + 1
               W( JEE ) = W( JE )
               WERR( JEE ) = WERR( JE )
               INDEXW( JEE ) = INDEXW( JE )
               IBLOCK( JEE ) = IBLOCK( JE )
 82         CONTINUE
            M = M-IM+1
         END IF

         IF( IDISCL.GT.0 .OR. IDISCU.GT.0 ) THEN
*           Code to deal with effects of bad arithmetic. (If N(w) is
*           monotone non-decreasing, this should never happen.)
*           Some low eigenvalues to be discarded are not in (WL,WLU],
*           or high eigenvalues to be discarded are not in (WUL,WU]
*           so just kill off the smallest IDISCL/largest IDISCU
*           eigenvalues, by marking the corresponding IBLOCK = 0
            IF( IDISCL.GT.0 ) THEN
               WKILL = WU
               DO 100 JDISC = 1, IDISCL
                  IW = 0
                  DO 90 JE = 1, M
                     IF( IBLOCK( JE ).NE.0 .AND.
     $                    ( W( JE ).LT.WKILL .OR. IW.EQ.0 ) ) THEN
                        IW = JE
                        WKILL = W( JE )
                     END IF
 90               CONTINUE
                  IBLOCK( IW ) = 0
 100           CONTINUE
            END IF
            IF( IDISCU.GT.0 ) THEN
               WKILL = WL
               DO 120 JDISC = 1, IDISCU
                  IW = 0
                  DO 110 JE = 1, M
                     IF( IBLOCK( JE ).NE.0 .AND.
     $                    ( W( JE ).GE.WKILL .OR. IW.EQ.0 ) ) THEN
                        IW = JE
                        WKILL = W( JE )
                     END IF
 110              CONTINUE
                  IBLOCK( IW ) = 0
 120           CONTINUE
            END IF
*           Now erase all eigenvalues with IBLOCK set to zero
            IM = 0
            DO 130 JE = 1, M
               IF( IBLOCK( JE ).NE.0 ) THEN
                  IM = IM + 1
                  W( IM ) = W( JE )
                  WERR( IM ) = WERR( JE )
                  INDEXW( IM ) = INDEXW( JE )
                  IBLOCK( IM ) = IBLOCK( JE )
               END IF
 130        CONTINUE
            M = IM
         END IF
         IF( IDISCL.LT.0 .OR. IDISCU.LT.0 ) THEN
            TOOFEW = .TRUE.
         END IF
      END IF
*
      IF(( IRANGE.EQ.ALLRNG .AND. M.NE.N ).OR.
     $   ( IRANGE.EQ.INDRNG .AND. M.NE.IU-IL+1 ) ) THEN
         TOOFEW = .TRUE.
      END IF

*     If ORDER='B', do nothing the eigenvalues are already sorted by
*        block.
*     If ORDER='E', sort the eigenvalues from smallest to largest

      IF( LSAME(ORDER,'E') .AND. NSPLIT.GT.1 ) THEN
         DO 150 JE = 1, M - 1
            IE = 0
            TMP1 = W( JE )
            DO 140 J = JE + 1, M
               IF( W( J ).LT.TMP1 ) THEN
                  IE = J
                  TMP1 = W( J )
               END IF
  140       CONTINUE
            IF( IE.NE.0 ) THEN
               TMP2 = WERR( IE )
               ITMP1 = IBLOCK( IE )
               ITMP2 = INDEXW( IE )
               W( IE ) = W( JE )
               WERR( IE ) = WERR( JE )
               IBLOCK( IE ) = IBLOCK( JE )
               INDEXW( IE ) = INDEXW( JE )
               W( JE ) = TMP1
               WERR( JE ) = TMP2
               IBLOCK( JE ) = ITMP1
               INDEXW( JE ) = ITMP2
            END IF
  150    CONTINUE
      END IF
*
      INFO = 0
      IF( NCNVRG )
     $   INFO = INFO + 1
      IF( TOOFEW )
     $   INFO = INFO + 2
      RETURN
*
*     End of DLARRD
*
      END
