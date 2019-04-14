*> \brief \b DLARRV computes the eigenvectors of the tridiagonal matrix T = L D LT given L, D and the eigenvalues of L D LT.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLARRV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlarrv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlarrv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlarrv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLARRV( N, VL, VU, D, L, PIVMIN,
*                          ISPLIT, M, DOL, DOU, MINRGP,
*                          RTOL1, RTOL2, W, WERR, WGAP,
*                          IBLOCK, INDEXW, GERS, Z, LDZ, ISUPPZ,
*                          WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            DOL, DOU, INFO, LDZ, M, N
*       DOUBLE PRECISION   MINRGP, PIVMIN, RTOL1, RTOL2, VL, VU
*       ..
*       .. Array Arguments ..
*       INTEGER            IBLOCK( * ), INDEXW( * ), ISPLIT( * ),
*      $                   ISUPPZ( * ), IWORK( * )
*       DOUBLE PRECISION   D( * ), GERS( * ), L( * ), W( * ), WERR( * ),
*      $                   WGAP( * ), WORK( * )
*       DOUBLE PRECISION  Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLARRV computes the eigenvectors of the tridiagonal matrix
*> T = L D L**T given L, D and APPROXIMATIONS to the eigenvalues of L D L**T.
*> The input eigenvalues should have been computed by DLARRE.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix.  N >= 0.
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is DOUBLE PRECISION
*>          Lower bound of the interval that contains the desired
*>          eigenvalues. VL < VU. Needed to compute gaps on the left or right
*>          end of the extremal eigenvalues in the desired RANGE.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>          VU is DOUBLE PRECISION
*>          Upper bound of the interval that contains the desired
*>          eigenvalues. VL < VU. 
*>          Note: VU is currently not used by this implementation of DLARRV, VU is
*>          passed to DLARRV because it could be used compute gaps on the right end
*>          of the extremal eigenvalues. However, with not much initial accuracy in
*>          LAMBDA and VU, the formula can lead to an overestimation of the right gap
*>          and thus to inadequately early RQI 'convergence'. This is currently
*>          prevented this by forcing a small right gap. And so it turns out that VU
*>          is currently not used by this implementation of DLARRV.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          On entry, the N diagonal elements of the diagonal matrix D.
*>          On exit, D may be overwritten.
*> \endverbatim
*>
*> \param[in,out] L
*> \verbatim
*>          L is DOUBLE PRECISION array, dimension (N)
*>          On entry, the (N-1) subdiagonal elements of the unit
*>          bidiagonal matrix L are in elements 1 to N-1 of L
*>          (if the matrix is not split.) At the end of each block
*>          is stored the corresponding shift as given by DLARRE.
*>          On exit, L is overwritten.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is DOUBLE PRECISION
*>          The minimum pivot allowed in the Sturm sequence.
*> \endverbatim
*>
*> \param[in] ISPLIT
*> \verbatim
*>          ISPLIT is INTEGER array, dimension (N)
*>          The splitting points, at which T breaks up into blocks.
*>          The first block consists of rows/columns 1 to
*>          ISPLIT( 1 ), the second of rows/columns ISPLIT( 1 )+1
*>          through ISPLIT( 2 ), etc.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The total number of input eigenvalues.  0 <= M <= N.
*> \endverbatim
*>
*> \param[in] DOL
*> \verbatim
*>          DOL is INTEGER
*> \endverbatim
*>
*> \param[in] DOU
*> \verbatim
*>          DOU is INTEGER
*>          If the user wants to compute only selected eigenvectors from all
*>          the eigenvalues supplied, he can specify an index range DOL:DOU.
*>          Or else the setting DOL=1, DOU=M should be applied.
*>          Note that DOL and DOU refer to the order in which the eigenvalues
*>          are stored in W.
*>          If the user wants to compute only selected eigenpairs, then
*>          the columns DOL-1 to DOU+1 of the eigenvector space Z contain the
*>          computed eigenvectors. All other columns of Z are set to zero.
*> \endverbatim
*>
*> \param[in] MINRGP
*> \verbatim
*>          MINRGP is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[in] RTOL1
*> \verbatim
*>          RTOL1 is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[in] RTOL2
*> \verbatim
*>          RTOL2 is DOUBLE PRECISION
*>           Parameters for bisection.
*>           An interval [LEFT,RIGHT] has converged if
*>           RIGHT-LEFT.LT.MAX( RTOL1*GAP, RTOL2*MAX(|LEFT|,|RIGHT|) )
*> \endverbatim
*>
*> \param[in,out] W
*> \verbatim
*>          W is DOUBLE PRECISION array, dimension (N)
*>          The first M elements of W contain the APPROXIMATE eigenvalues for
*>          which eigenvectors are to be computed.  The eigenvalues
*>          should be grouped by split-off block and ordered from
*>          smallest to largest within the block ( The output array
*>          W from DLARRE is expected here ). Furthermore, they are with
*>          respect to the shift of the corresponding root representation
*>          for their block. On exit, W holds the eigenvalues of the
*>          UNshifted matrix.
*> \endverbatim
*>
*> \param[in,out] WERR
*> \verbatim
*>          WERR is DOUBLE PRECISION array, dimension (N)
*>          The first M elements contain the semiwidth of the uncertainty
*>          interval of the corresponding eigenvalue in W
*> \endverbatim
*>
*> \param[in,out] WGAP
*> \verbatim
*>          WGAP is DOUBLE PRECISION array, dimension (N)
*>          The separation from the right neighbor eigenvalue in W.
*> \endverbatim
*>
*> \param[in] IBLOCK
*> \verbatim
*>          IBLOCK is INTEGER array, dimension (N)
*>          The indices of the blocks (submatrices) associated with the
*>          corresponding eigenvalues in W; IBLOCK(i)=1 if eigenvalue
*>          W(i) belongs to the first block from the top, =2 if W(i)
*>          belongs to the second block, etc.
*> \endverbatim
*>
*> \param[in] INDEXW
*> \verbatim
*>          INDEXW is INTEGER array, dimension (N)
*>          The indices of the eigenvalues within each block (submatrix);
*>          for example, INDEXW(i)= 10 and IBLOCK(i)=2 imply that the
*>          i-th eigenvalue W(i) is the 10-th eigenvalue in the second block.
*> \endverbatim
*>
*> \param[in] GERS
*> \verbatim
*>          GERS is DOUBLE PRECISION array, dimension (2*N)
*>          The N Gerschgorin intervals (the i-th Gerschgorin interval
*>          is (GERS(2*i-1), GERS(2*i)). The Gerschgorin intervals should
*>          be computed from the original UNshifted matrix.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (LDZ, max(1,M) )
*>          If INFO = 0, the first M columns of Z contain the
*>          orthonormal eigenvectors of the matrix T
*>          corresponding to the input eigenvalues, with the i-th
*>          column of Z holding the eigenvector associated with W(i).
*>          Note: the user must ensure that at least max(1,M) columns are
*>          supplied in the array Z.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.  LDZ >= 1, and if
*>          JOBZ = 'V', LDZ >= max(1,N).
*> \endverbatim
*>
*> \param[out] ISUPPZ
*> \verbatim
*>          ISUPPZ is INTEGER array, dimension ( 2*max(1,M) )
*>          The support of the eigenvectors in Z, i.e., the indices
*>          indicating the nonzero elements in Z. The I-th eigenvector
*>          is nonzero only in elements ISUPPZ( 2*I-1 ) through
*>          ISUPPZ( 2*I ).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (12*N)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (7*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>
*>          > 0:  A problem occurred in DLARRV.
*>          < 0:  One of the called subroutines signaled an internal problem.
*>                Needs inspection of the corresponding parameter IINFO
*>                for further information.
*>
*>          =-1:  Problem in DLARRB when refining a child's eigenvalues.
*>          =-2:  Problem in DLARRF when computing the RRR of a child.
*>                When a child is inside a tight cluster, it can be difficult
*>                to find an RRR. A partial remedy from the user's point of
*>                view is to make the parameter MINRGP smaller and recompile.
*>                However, as the orthogonality of the computed vectors is
*>                proportional to 1/MINRGP, the user should be aware that
*>                he might be trading in precision when he decreases MINRGP.
*>          =-3:  Problem in DLARRB when refining a single eigenvalue
*>                after the Rayleigh correction was rejected.
*>          = 5:  The Rayleigh Quotient Iteration failed to converge to
*>                full accuracy in MAXITR steps.
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
*> \ingroup doubleOTHERauxiliary
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
      SUBROUTINE DLARRV( N, VL, VU, D, L, PIVMIN,
     $                   ISPLIT, M, DOL, DOU, MINRGP,
     $                   RTOL1, RTOL2, W, WERR, WGAP,
     $                   IBLOCK, INDEXW, GERS, Z, LDZ, ISUPPZ,
     $                   WORK, IWORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            DOL, DOU, INFO, LDZ, M, N
      DOUBLE PRECISION   MINRGP, PIVMIN, RTOL1, RTOL2, VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            IBLOCK( * ), INDEXW( * ), ISPLIT( * ),
     $                   ISUPPZ( * ), IWORK( * )
      DOUBLE PRECISION   D( * ), GERS( * ), L( * ), W( * ), WERR( * ),
     $                   WGAP( * ), WORK( * )
      DOUBLE PRECISION  Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            MAXITR
      PARAMETER          ( MAXITR = 10 )
      DOUBLE PRECISION   ZERO, ONE, TWO, THREE, FOUR, HALF
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0,
     $                     TWO = 2.0D0, THREE = 3.0D0,
     $                     FOUR = 4.0D0, HALF = 0.5D0)
*     ..
*     .. Local Scalars ..
      LOGICAL            ESKIP, NEEDBS, STP2II, TRYRQC, USEDBS, USEDRQ
      INTEGER            DONE, I, IBEGIN, IDONE, IEND, II, IINDC1,
     $                   IINDC2, IINDR, IINDWK, IINFO, IM, IN, INDEIG,
     $                   INDLD, INDLLD, INDWRK, ISUPMN, ISUPMX, ITER,
     $                   ITMP1, J, JBLK, K, MINIWSIZE, MINWSIZE, NCLUS,
     $                   NDEPTH, NEGCNT, NEWCLS, NEWFST, NEWFTT, NEWLST,
     $                   NEWSIZ, OFFSET, OLDCLS, OLDFST, OLDIEN, OLDLST,
     $                   OLDNCL, P, PARITY, Q, WBEGIN, WEND, WINDEX,
     $                   WINDMN, WINDPL, ZFROM, ZTO, ZUSEDL, ZUSEDU,
     $                   ZUSEDW
      DOUBLE PRECISION   BSTRES, BSTW, EPS, FUDGE, GAP, GAPTOL, GL, GU,
     $                   LAMBDA, LEFT, LGAP, MINGMA, NRMINV, RESID,
     $                   RGAP, RIGHT, RQCORR, RQTOL, SAVGAP, SGNDEF,
     $                   SIGMA, SPDIAM, SSIGMA, TAU, TMP, TOL, ZTZ
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLAR1V, DLARRB, DLARRF, DLASET,
     $                   DSCAL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC ABS, DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*     ..

      INFO = 0
*
*     Quick return if possible
*
      IF( N.LE.0 ) THEN
         RETURN
      END IF
*
*     The first N entries of WORK are reserved for the eigenvalues
      INDLD = N+1
      INDLLD= 2*N+1
      INDWRK= 3*N+1
      MINWSIZE = 12 * N

      DO 5 I= 1,MINWSIZE
         WORK( I ) = ZERO
 5    CONTINUE

*     IWORK(IINDR+1:IINDR+N) hold the twist indices R for the
*     factorization used to compute the FP vector
      IINDR = 0
*     IWORK(IINDC1+1:IINC2+N) are used to store the clusters of the current
*     layer and the one above.
      IINDC1 = N
      IINDC2 = 2*N
      IINDWK = 3*N + 1

      MINIWSIZE = 7 * N
      DO 10 I= 1,MINIWSIZE
         IWORK( I ) = 0
 10   CONTINUE

      ZUSEDL = 1
      IF(DOL.GT.1) THEN
*        Set lower bound for use of Z
         ZUSEDL = DOL-1
      ENDIF
      ZUSEDU = M
      IF(DOU.LT.M) THEN
*        Set lower bound for use of Z
         ZUSEDU = DOU+1
      ENDIF
*     The width of the part of Z that is used
      ZUSEDW = ZUSEDU - ZUSEDL + 1


      CALL DLASET( 'Full', N, ZUSEDW, ZERO, ZERO,
     $                    Z(1,ZUSEDL), LDZ )

      EPS = DLAMCH( 'Precision' )
      RQTOL = TWO * EPS
*
*     Set expert flags for standard code.
      TRYRQC = .TRUE.

      IF((DOL.EQ.1).AND.(DOU.EQ.M)) THEN
      ELSE
*        Only selected eigenpairs are computed. Since the other evalues
*        are not refined by RQ iteration, bisection has to compute to full
*        accuracy.
         RTOL1 = FOUR * EPS
         RTOL2 = FOUR * EPS
      ENDIF

*     The entries WBEGIN:WEND in W, WERR, WGAP correspond to the
*     desired eigenvalues. The support of the nonzero eigenvector
*     entries is contained in the interval IBEGIN:IEND.
*     Remark that if k eigenpairs are desired, then the eigenvectors
*     are stored in k contiguous columns of Z.

*     DONE is the number of eigenvectors already computed
      DONE = 0
      IBEGIN = 1
      WBEGIN = 1
      DO 170 JBLK = 1, IBLOCK( M )
         IEND = ISPLIT( JBLK )
         SIGMA = L( IEND )
*        Find the eigenvectors of the submatrix indexed IBEGIN
*        through IEND.
         WEND = WBEGIN - 1
 15      CONTINUE
         IF( WEND.LT.M ) THEN
            IF( IBLOCK( WEND+1 ).EQ.JBLK ) THEN
               WEND = WEND + 1
               GO TO 15
            END IF
         END IF
         IF( WEND.LT.WBEGIN ) THEN
            IBEGIN = IEND + 1
            GO TO 170
         ELSEIF( (WEND.LT.DOL).OR.(WBEGIN.GT.DOU) ) THEN
            IBEGIN = IEND + 1
            WBEGIN = WEND + 1
            GO TO 170
         END IF

*        Find local spectral diameter of the block
         GL = GERS( 2*IBEGIN-1 )
         GU = GERS( 2*IBEGIN )
         DO 20 I = IBEGIN+1 , IEND
            GL = MIN( GERS( 2*I-1 ), GL )
            GU = MAX( GERS( 2*I ), GU )
 20      CONTINUE
         SPDIAM = GU - GL

*        OLDIEN is the last index of the previous block
         OLDIEN = IBEGIN - 1
*        Calculate the size of the current block
         IN = IEND - IBEGIN + 1
*        The number of eigenvalues in the current block
         IM = WEND - WBEGIN + 1

*        This is for a 1x1 block
         IF( IBEGIN.EQ.IEND ) THEN
            DONE = DONE+1
            Z( IBEGIN, WBEGIN ) = ONE
            ISUPPZ( 2*WBEGIN-1 ) = IBEGIN
            ISUPPZ( 2*WBEGIN ) = IBEGIN
            W( WBEGIN ) = W( WBEGIN ) + SIGMA
            WORK( WBEGIN ) = W( WBEGIN )
            IBEGIN = IEND + 1
            WBEGIN = WBEGIN + 1
            GO TO 170
         END IF

*        The desired (shifted) eigenvalues are stored in W(WBEGIN:WEND)
*        Note that these can be approximations, in this case, the corresp.
*        entries of WERR give the size of the uncertainty interval.
*        The eigenvalue approximations will be refined when necessary as
*        high relative accuracy is required for the computation of the
*        corresponding eigenvectors.
         CALL DCOPY( IM, W( WBEGIN ), 1,
     $                   WORK( WBEGIN ), 1 )

*        We store in W the eigenvalue approximations w.r.t. the original
*        matrix T.
         DO 30 I=1,IM
            W(WBEGIN+I-1) = W(WBEGIN+I-1)+SIGMA
 30      CONTINUE


*        NDEPTH is the current depth of the representation tree
         NDEPTH = 0
*        PARITY is either 1 or 0
         PARITY = 1
*        NCLUS is the number of clusters for the next level of the
*        representation tree, we start with NCLUS = 1 for the root
         NCLUS = 1
         IWORK( IINDC1+1 ) = 1
         IWORK( IINDC1+2 ) = IM

*        IDONE is the number of eigenvectors already computed in the current
*        block
         IDONE = 0
*        loop while( IDONE.LT.IM )
*        generate the representation tree for the current block and
*        compute the eigenvectors
   40    CONTINUE
         IF( IDONE.LT.IM ) THEN
*           This is a crude protection against infinitely deep trees
            IF( NDEPTH.GT.M ) THEN
               INFO = -2
               RETURN
            ENDIF
*           breadth first processing of the current level of the representation
*           tree: OLDNCL = number of clusters on current level
            OLDNCL = NCLUS
*           reset NCLUS to count the number of child clusters
            NCLUS = 0
*
            PARITY = 1 - PARITY
            IF( PARITY.EQ.0 ) THEN
               OLDCLS = IINDC1
               NEWCLS = IINDC2
            ELSE
               OLDCLS = IINDC2
               NEWCLS = IINDC1
            END IF
*           Process the clusters on the current level
            DO 150 I = 1, OLDNCL
               J = OLDCLS + 2*I
*              OLDFST, OLDLST = first, last index of current cluster.
*                               cluster indices start with 1 and are relative
*                               to WBEGIN when accessing W, WGAP, WERR, Z
               OLDFST = IWORK( J-1 )
               OLDLST = IWORK( J )
               IF( NDEPTH.GT.0 ) THEN
*                 Retrieve relatively robust representation (RRR) of cluster
*                 that has been computed at the previous level
*                 The RRR is stored in Z and overwritten once the eigenvectors
*                 have been computed or when the cluster is refined

                  IF((DOL.EQ.1).AND.(DOU.EQ.M)) THEN
*                    Get representation from location of the leftmost evalue
*                    of the cluster
                     J = WBEGIN + OLDFST - 1
                  ELSE
                     IF(WBEGIN+OLDFST-1.LT.DOL) THEN
*                       Get representation from the left end of Z array
                        J = DOL - 1
                     ELSEIF(WBEGIN+OLDFST-1.GT.DOU) THEN
*                       Get representation from the right end of Z array
                        J = DOU
                     ELSE
                        J = WBEGIN + OLDFST - 1
                     ENDIF
                  ENDIF
                  CALL DCOPY( IN, Z( IBEGIN, J ), 1, D( IBEGIN ), 1 )
                  CALL DCOPY( IN-1, Z( IBEGIN, J+1 ), 1, L( IBEGIN ),
     $               1 )
                  SIGMA = Z( IEND, J+1 )

*                 Set the corresponding entries in Z to zero
                  CALL DLASET( 'Full', IN, 2, ZERO, ZERO,
     $                         Z( IBEGIN, J), LDZ )
               END IF

*              Compute DL and DLL of current RRR
               DO 50 J = IBEGIN, IEND-1
                  TMP = D( J )*L( J )
                  WORK( INDLD-1+J ) = TMP
                  WORK( INDLLD-1+J ) = TMP*L( J )
   50          CONTINUE

               IF( NDEPTH.GT.0 ) THEN
*                 P and Q are index of the first and last eigenvalue to compute
*                 within the current block
                  P = INDEXW( WBEGIN-1+OLDFST )
                  Q = INDEXW( WBEGIN-1+OLDLST )
*                 Offset for the arrays WORK, WGAP and WERR, i.e., the P-OFFSET
*                 through the Q-OFFSET elements of these arrays are to be used.
*                  OFFSET = P-OLDFST
                  OFFSET = INDEXW( WBEGIN ) - 1
*                 perform limited bisection (if necessary) to get approximate
*                 eigenvalues to the precision needed.
                  CALL DLARRB( IN, D( IBEGIN ),
     $                         WORK(INDLLD+IBEGIN-1),
     $                         P, Q, RTOL1, RTOL2, OFFSET,
     $                         WORK(WBEGIN),WGAP(WBEGIN),WERR(WBEGIN),
     $                         WORK( INDWRK ), IWORK( IINDWK ),
     $                         PIVMIN, SPDIAM, IN, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     INFO = -1
                     RETURN
                  ENDIF
*                 We also recompute the extremal gaps. W holds all eigenvalues
*                 of the unshifted matrix and must be used for computation
*                 of WGAP, the entries of WORK might stem from RRRs with
*                 different shifts. The gaps from WBEGIN-1+OLDFST to
*                 WBEGIN-1+OLDLST are correctly computed in DLARRB.
*                 However, we only allow the gaps to become greater since
*                 this is what should happen when we decrease WERR
                  IF( OLDFST.GT.1) THEN
                     WGAP( WBEGIN+OLDFST-2 ) =
     $             MAX(WGAP(WBEGIN+OLDFST-2),
     $                 W(WBEGIN+OLDFST-1)-WERR(WBEGIN+OLDFST-1)
     $                 - W(WBEGIN+OLDFST-2)-WERR(WBEGIN+OLDFST-2) )
                  ENDIF
                  IF( WBEGIN + OLDLST -1 .LT. WEND ) THEN
                     WGAP( WBEGIN+OLDLST-1 ) =
     $               MAX(WGAP(WBEGIN+OLDLST-1),
     $                   W(WBEGIN+OLDLST)-WERR(WBEGIN+OLDLST)
     $                   - W(WBEGIN+OLDLST-1)-WERR(WBEGIN+OLDLST-1) )
                  ENDIF
*                 Each time the eigenvalues in WORK get refined, we store
*                 the newly found approximation with all shifts applied in W
                  DO 53 J=OLDFST,OLDLST
                     W(WBEGIN+J-1) = WORK(WBEGIN+J-1)+SIGMA
 53               CONTINUE
               END IF

*              Process the current node.
               NEWFST = OLDFST
               DO 140 J = OLDFST, OLDLST
                  IF( J.EQ.OLDLST ) THEN
*                    we are at the right end of the cluster, this is also the
*                    boundary of the child cluster
                     NEWLST = J
                  ELSE IF ( WGAP( WBEGIN + J -1).GE.
     $                    MINRGP* ABS( WORK(WBEGIN + J -1) ) ) THEN
*                    the right relative gap is big enough, the child cluster
*                    (NEWFST,..,NEWLST) is well separated from the following
                     NEWLST = J
                   ELSE
*                    inside a child cluster, the relative gap is not
*                    big enough.
                     GOTO 140
                  END IF

*                 Compute size of child cluster found
                  NEWSIZ = NEWLST - NEWFST + 1

*                 NEWFTT is the place in Z where the new RRR or the computed
*                 eigenvector is to be stored
                  IF((DOL.EQ.1).AND.(DOU.EQ.M)) THEN
*                    Store representation at location of the leftmost evalue
*                    of the cluster
                     NEWFTT = WBEGIN + NEWFST - 1
                  ELSE
                     IF(WBEGIN+NEWFST-1.LT.DOL) THEN
*                       Store representation at the left end of Z array
                        NEWFTT = DOL - 1
                     ELSEIF(WBEGIN+NEWFST-1.GT.DOU) THEN
*                       Store representation at the right end of Z array
                        NEWFTT = DOU
                     ELSE
                        NEWFTT = WBEGIN + NEWFST - 1
                     ENDIF
                  ENDIF

                  IF( NEWSIZ.GT.1) THEN
*
*                    Current child is not a singleton but a cluster.
*                    Compute and store new representation of child.
*
*
*                    Compute left and right cluster gap.
*
*                    LGAP and RGAP are not computed from WORK because
*                    the eigenvalue approximations may stem from RRRs
*                    different shifts. However, W hold all eigenvalues
*                    of the unshifted matrix. Still, the entries in WGAP
*                    have to be computed from WORK since the entries
*                    in W might be of the same order so that gaps are not
*                    exhibited correctly for very close eigenvalues.
                     IF( NEWFST.EQ.1 ) THEN
                        LGAP = MAX( ZERO,
     $                       W(WBEGIN)-WERR(WBEGIN) - VL )
                    ELSE
                        LGAP = WGAP( WBEGIN+NEWFST-2 )
                     ENDIF
                     RGAP = WGAP( WBEGIN+NEWLST-1 )
*
*                    Compute left- and rightmost eigenvalue of child
*                    to high precision in order to shift as close
*                    as possible and obtain as large relative gaps
*                    as possible
*
                     DO 55 K =1,2
                        IF(K.EQ.1) THEN
                           P = INDEXW( WBEGIN-1+NEWFST )
                        ELSE
                           P = INDEXW( WBEGIN-1+NEWLST )
                        ENDIF
                        OFFSET = INDEXW( WBEGIN ) - 1
                        CALL DLARRB( IN, D(IBEGIN),
     $                       WORK( INDLLD+IBEGIN-1 ),P,P,
     $                       RQTOL, RQTOL, OFFSET,
     $                       WORK(WBEGIN),WGAP(WBEGIN),
     $                       WERR(WBEGIN),WORK( INDWRK ),
     $                       IWORK( IINDWK ), PIVMIN, SPDIAM,
     $                       IN, IINFO )
 55                  CONTINUE
*
                     IF((WBEGIN+NEWLST-1.LT.DOL).OR.
     $                  (WBEGIN+NEWFST-1.GT.DOU)) THEN
*                       if the cluster contains no desired eigenvalues
*                       skip the computation of that branch of the rep. tree
*
*                       We could skip before the refinement of the extremal
*                       eigenvalues of the child, but then the representation
*                       tree could be different from the one when nothing is
*                       skipped. For this reason we skip at this place.
                        IDONE = IDONE + NEWLST - NEWFST + 1
                        GOTO 139
                     ENDIF
*
*                    Compute RRR of child cluster.
*                    Note that the new RRR is stored in Z
*
*                    DLARRF needs LWORK = 2*N
                     CALL DLARRF( IN, D( IBEGIN ), L( IBEGIN ),
     $                         WORK(INDLD+IBEGIN-1),
     $                         NEWFST, NEWLST, WORK(WBEGIN),
     $                         WGAP(WBEGIN), WERR(WBEGIN),
     $                         SPDIAM, LGAP, RGAP, PIVMIN, TAU,
     $                         Z(IBEGIN, NEWFTT),Z(IBEGIN, NEWFTT+1),
     $                         WORK( INDWRK ), IINFO )
                     IF( IINFO.EQ.0 ) THEN
*                       a new RRR for the cluster was found by DLARRF
*                       update shift and store it
                        SSIGMA = SIGMA + TAU
                        Z( IEND, NEWFTT+1 ) = SSIGMA
*                       WORK() are the midpoints and WERR() the semi-width
*                       Note that the entries in W are unchanged.
                        DO 116 K = NEWFST, NEWLST
                           FUDGE =
     $                          THREE*EPS*ABS(WORK(WBEGIN+K-1))
                           WORK( WBEGIN + K - 1 ) =
     $                          WORK( WBEGIN + K - 1) - TAU
                           FUDGE = FUDGE +
     $                          FOUR*EPS*ABS(WORK(WBEGIN+K-1))
*                          Fudge errors
                           WERR( WBEGIN + K - 1 ) =
     $                          WERR( WBEGIN + K - 1 ) + FUDGE
*                          Gaps are not fudged. Provided that WERR is small
*                          when eigenvalues are close, a zero gap indicates
*                          that a new representation is needed for resolving
*                          the cluster. A fudge could lead to a wrong decision
*                          of judging eigenvalues 'separated' which in
*                          reality are not. This could have a negative impact
*                          on the orthogonality of the computed eigenvectors.
 116                    CONTINUE

                        NCLUS = NCLUS + 1
                        K = NEWCLS + 2*NCLUS
                        IWORK( K-1 ) = NEWFST
                        IWORK( K ) = NEWLST
                     ELSE
                        INFO = -2
                        RETURN
                     ENDIF
                  ELSE
*
*                    Compute eigenvector of singleton
*
                     ITER = 0
*
                     TOL = FOUR * LOG(DBLE(IN)) * EPS
*
                     K = NEWFST
                     WINDEX = WBEGIN + K - 1
                     WINDMN = MAX(WINDEX - 1,1)
                     WINDPL = MIN(WINDEX + 1,M)
                     LAMBDA = WORK( WINDEX )
                     DONE = DONE + 1
*                    Check if eigenvector computation is to be skipped
                     IF((WINDEX.LT.DOL).OR.
     $                  (WINDEX.GT.DOU)) THEN
                        ESKIP = .TRUE.
                        GOTO 125
                     ELSE
                        ESKIP = .FALSE.
                     ENDIF
                     LEFT = WORK( WINDEX ) - WERR( WINDEX )
                     RIGHT = WORK( WINDEX ) + WERR( WINDEX )
                     INDEIG = INDEXW( WINDEX )
*                    Note that since we compute the eigenpairs for a child,
*                    all eigenvalue approximations are w.r.t the same shift.
*                    In this case, the entries in WORK should be used for
*                    computing the gaps since they exhibit even very small
*                    differences in the eigenvalues, as opposed to the
*                    entries in W which might "look" the same.

                     IF( K .EQ. 1) THEN
*                       In the case RANGE='I' and with not much initial
*                       accuracy in LAMBDA and VL, the formula
*                       LGAP = MAX( ZERO, (SIGMA - VL) + LAMBDA )
*                       can lead to an overestimation of the left gap and
*                       thus to inadequately early RQI 'convergence'.
*                       Prevent this by forcing a small left gap.
                        LGAP = EPS*MAX(ABS(LEFT),ABS(RIGHT))
                     ELSE
                        LGAP = WGAP(WINDMN)
                     ENDIF
                     IF( K .EQ. IM) THEN
*                       In the case RANGE='I' and with not much initial
*                       accuracy in LAMBDA and VU, the formula
*                       can lead to an overestimation of the right gap and
*                       thus to inadequately early RQI 'convergence'.
*                       Prevent this by forcing a small right gap.
                        RGAP = EPS*MAX(ABS(LEFT),ABS(RIGHT))
                     ELSE
                        RGAP = WGAP(WINDEX)
                     ENDIF
                     GAP = MIN( LGAP, RGAP )
                     IF(( K .EQ. 1).OR.(K .EQ. IM)) THEN
*                       The eigenvector support can become wrong
*                       because significant entries could be cut off due to a
*                       large GAPTOL parameter in LAR1V. Prevent this.
                        GAPTOL = ZERO
                     ELSE
                        GAPTOL = GAP * EPS
                     ENDIF
                     ISUPMN = IN
                     ISUPMX = 1
*                    Update WGAP so that it holds the minimum gap
*                    to the left or the right. This is crucial in the
*                    case where bisection is used to ensure that the
*                    eigenvalue is refined up to the required precision.
*                    The correct value is restored afterwards.
                     SAVGAP = WGAP(WINDEX)
                     WGAP(WINDEX) = GAP
*                    We want to use the Rayleigh Quotient Correction
*                    as often as possible since it converges quadratically
*                    when we are close enough to the desired eigenvalue.
*                    However, the Rayleigh Quotient can have the wrong sign
*                    and lead us away from the desired eigenvalue. In this
*                    case, the best we can do is to use bisection.
                     USEDBS = .FALSE.
                     USEDRQ = .FALSE.
*                    Bisection is initially turned off unless it is forced
                     NEEDBS =  .NOT.TRYRQC
 120                 CONTINUE
*                    Check if bisection should be used to refine eigenvalue
                     IF(NEEDBS) THEN
*                       Take the bisection as new iterate
                        USEDBS = .TRUE.
                        ITMP1 = IWORK( IINDR+WINDEX )
                        OFFSET = INDEXW( WBEGIN ) - 1
                        CALL DLARRB( IN, D(IBEGIN),
     $                       WORK(INDLLD+IBEGIN-1),INDEIG,INDEIG,
     $                       ZERO, TWO*EPS, OFFSET,
     $                       WORK(WBEGIN),WGAP(WBEGIN),
     $                       WERR(WBEGIN),WORK( INDWRK ),
     $                       IWORK( IINDWK ), PIVMIN, SPDIAM,
     $                       ITMP1, IINFO )
                        IF( IINFO.NE.0 ) THEN
                           INFO = -3
                           RETURN
                        ENDIF
                        LAMBDA = WORK( WINDEX )
*                       Reset twist index from inaccurate LAMBDA to
*                       force computation of true MINGMA
                        IWORK( IINDR+WINDEX ) = 0
                     ENDIF
*                    Given LAMBDA, compute the eigenvector.
                     CALL DLAR1V( IN, 1, IN, LAMBDA, D( IBEGIN ),
     $                    L( IBEGIN ), WORK(INDLD+IBEGIN-1),
     $                    WORK(INDLLD+IBEGIN-1),
     $                    PIVMIN, GAPTOL, Z( IBEGIN, WINDEX ),
     $                    .NOT.USEDBS, NEGCNT, ZTZ, MINGMA,
     $                    IWORK( IINDR+WINDEX ), ISUPPZ( 2*WINDEX-1 ),
     $                    NRMINV, RESID, RQCORR, WORK( INDWRK ) )
                     IF(ITER .EQ. 0) THEN
                        BSTRES = RESID
                        BSTW = LAMBDA
                     ELSEIF(RESID.LT.BSTRES) THEN
                        BSTRES = RESID
                        BSTW = LAMBDA
                     ENDIF
                     ISUPMN = MIN(ISUPMN,ISUPPZ( 2*WINDEX-1 ))
                     ISUPMX = MAX(ISUPMX,ISUPPZ( 2*WINDEX ))
                     ITER = ITER + 1

*                    sin alpha <= |resid|/gap
*                    Note that both the residual and the gap are
*                    proportional to the matrix, so ||T|| doesn't play
*                    a role in the quotient

*
*                    Convergence test for Rayleigh-Quotient iteration
*                    (omitted when Bisection has been used)
*
                     IF( RESID.GT.TOL*GAP .AND. ABS( RQCORR ).GT.
     $                    RQTOL*ABS( LAMBDA ) .AND. .NOT. USEDBS)
     $                    THEN
*                       We need to check that the RQCORR update doesn't
*                       move the eigenvalue away from the desired one and
*                       towards a neighbor. -> protection with bisection
                        IF(INDEIG.LE.NEGCNT) THEN
*                          The wanted eigenvalue lies to the left
                           SGNDEF = -ONE
                        ELSE
*                          The wanted eigenvalue lies to the right
                           SGNDEF = ONE
                        ENDIF
*                       We only use the RQCORR if it improves the
*                       the iterate reasonably.
                        IF( ( RQCORR*SGNDEF.GE.ZERO )
     $                       .AND.( LAMBDA + RQCORR.LE. RIGHT)
     $                       .AND.( LAMBDA + RQCORR.GE. LEFT)
     $                       ) THEN
                           USEDRQ = .TRUE.
*                          Store new midpoint of bisection interval in WORK
                           IF(SGNDEF.EQ.ONE) THEN
*                             The current LAMBDA is on the left of the true
*                             eigenvalue
                              LEFT = LAMBDA
*                             We prefer to assume that the error estimate
*                             is correct. We could make the interval not
*                             as a bracket but to be modified if the RQCORR
*                             chooses to. In this case, the RIGHT side should
*                             be modified as follows:
*                              RIGHT = MAX(RIGHT, LAMBDA + RQCORR)
                           ELSE
*                             The current LAMBDA is on the right of the true
*                             eigenvalue
                              RIGHT = LAMBDA
*                             See comment about assuming the error estimate is
*                             correct above.
*                              LEFT = MIN(LEFT, LAMBDA + RQCORR)
                           ENDIF
                           WORK( WINDEX ) =
     $                       HALF * (RIGHT + LEFT)
*                          Take RQCORR since it has the correct sign and
*                          improves the iterate reasonably
                           LAMBDA = LAMBDA + RQCORR
*                          Update width of error interval
                           WERR( WINDEX ) =
     $                             HALF * (RIGHT-LEFT)
                        ELSE
                           NEEDBS = .TRUE.
                        ENDIF
                        IF(RIGHT-LEFT.LT.RQTOL*ABS(LAMBDA)) THEN
*                             The eigenvalue is computed to bisection accuracy
*                             compute eigenvector and stop
                           USEDBS = .TRUE.
                           GOTO 120
                        ELSEIF( ITER.LT.MAXITR ) THEN
                           GOTO 120
                        ELSEIF( ITER.EQ.MAXITR ) THEN
                           NEEDBS = .TRUE.
                           GOTO 120
                        ELSE
                           INFO = 5
                           RETURN
                        END IF
                     ELSE
                        STP2II = .FALSE.
        IF(USEDRQ .AND. USEDBS .AND.
     $                     BSTRES.LE.RESID) THEN
                           LAMBDA = BSTW
                           STP2II = .TRUE.
                        ENDIF
                        IF (STP2II) THEN
*                          improve error angle by second step
                           CALL DLAR1V( IN, 1, IN, LAMBDA,
     $                          D( IBEGIN ), L( IBEGIN ),
     $                          WORK(INDLD+IBEGIN-1),
     $                          WORK(INDLLD+IBEGIN-1),
     $                          PIVMIN, GAPTOL, Z( IBEGIN, WINDEX ),
     $                          .NOT.USEDBS, NEGCNT, ZTZ, MINGMA,
     $                          IWORK( IINDR+WINDEX ),
     $                          ISUPPZ( 2*WINDEX-1 ),
     $                          NRMINV, RESID, RQCORR, WORK( INDWRK ) )
                        ENDIF
                        WORK( WINDEX ) = LAMBDA
                     END IF
*
*                    Compute FP-vector support w.r.t. whole matrix
*
                     ISUPPZ( 2*WINDEX-1 ) = ISUPPZ( 2*WINDEX-1 )+OLDIEN
                     ISUPPZ( 2*WINDEX ) = ISUPPZ( 2*WINDEX )+OLDIEN
                     ZFROM = ISUPPZ( 2*WINDEX-1 )
                     ZTO = ISUPPZ( 2*WINDEX )
                     ISUPMN = ISUPMN + OLDIEN
                     ISUPMX = ISUPMX + OLDIEN
*                    Ensure vector is ok if support in the RQI has changed
                     IF(ISUPMN.LT.ZFROM) THEN
                        DO 122 II = ISUPMN,ZFROM-1
                           Z( II, WINDEX ) = ZERO
 122                    CONTINUE
                     ENDIF
                     IF(ISUPMX.GT.ZTO) THEN
                        DO 123 II = ZTO+1,ISUPMX
                           Z( II, WINDEX ) = ZERO
 123                    CONTINUE
                     ENDIF
                     CALL DSCAL( ZTO-ZFROM+1, NRMINV,
     $                       Z( ZFROM, WINDEX ), 1 )
 125                 CONTINUE
*                    Update W
                     W( WINDEX ) = LAMBDA+SIGMA
*                    Recompute the gaps on the left and right
*                    But only allow them to become larger and not
*                    smaller (which can only happen through "bad"
*                    cancellation and doesn't reflect the theory
*                    where the initial gaps are underestimated due
*                    to WERR being too crude.)
                     IF(.NOT.ESKIP) THEN
                        IF( K.GT.1) THEN
                           WGAP( WINDMN ) = MAX( WGAP(WINDMN),
     $                          W(WINDEX)-WERR(WINDEX)
     $                          - W(WINDMN)-WERR(WINDMN) )
                        ENDIF
                        IF( WINDEX.LT.WEND ) THEN
                           WGAP( WINDEX ) = MAX( SAVGAP,
     $                          W( WINDPL )-WERR( WINDPL )
     $                          - W( WINDEX )-WERR( WINDEX) )
                        ENDIF
                     ENDIF
                     IDONE = IDONE + 1
                  ENDIF
*                 here ends the code for the current child
*
 139              CONTINUE
*                 Proceed to any remaining child nodes
                  NEWFST = J + 1
 140           CONTINUE
 150        CONTINUE
            NDEPTH = NDEPTH + 1
            GO TO 40
         END IF
         IBEGIN = IEND + 1
         WBEGIN = WEND + 1
 170  CONTINUE
*

      RETURN
*
*     End of DLARRV
*
      END
