*> \brief \b ZSTEMR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZSTEMR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zstemr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zstemr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zstemr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZSTEMR( JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
*                          M, W, Z, LDZ, NZC, ISUPPZ, TRYRAC, WORK, LWORK,
*                          IWORK, LIWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBZ, RANGE
*       LOGICAL            TRYRAC
*       INTEGER            IL, INFO, IU, LDZ, NZC, LIWORK, LWORK, M, N
*       DOUBLE PRECISION VL, VU
*       ..
*       .. Array Arguments ..
*       INTEGER            ISUPPZ( * ), IWORK( * )
*       DOUBLE PRECISION   D( * ), E( * ), W( * ), WORK( * )
*       COMPLEX*16         Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZSTEMR computes selected eigenvalues and, optionally, eigenvectors
*> of a real symmetric tridiagonal matrix T. Any such unreduced matrix has
*> a well defined set of pairwise different real eigenvalues, the corresponding
*> real eigenvectors are pairwise orthogonal.
*>
*> The spectrum may be computed either completely or partially by specifying
*> either an interval (VL,VU] or a range of indices IL:IU for the desired
*> eigenvalues.
*>
*> Depending on the number of desired eigenvalues, these are computed either
*> by bisection or the dqds algorithm. Numerically orthogonal eigenvectors are
*> computed by the use of various suitable L D L^T factorizations near clusters
*> of close eigenvalues (referred to as RRRs, Relatively Robust
*> Representations). An informal sketch of the algorithm follows.
*>
*> For each unreduced block (submatrix) of T,
*>    (a) Compute T - sigma I  = L D L^T, so that L and D
*>        define all the wanted eigenvalues to high relative accuracy.
*>        This means that small relative changes in the entries of D and L
*>        cause only small relative changes in the eigenvalues and
*>        eigenvectors. The standard (unfactored) representation of the
*>        tridiagonal matrix T does not have this property in general.
*>    (b) Compute the eigenvalues to suitable accuracy.
*>        If the eigenvectors are desired, the algorithm attains full
*>        accuracy of the computed eigenvalues only right before
*>        the corresponding vectors have to be computed, see steps c) and d).
*>    (c) For each cluster of close eigenvalues, select a new
*>        shift close to the cluster, find a new factorization, and refine
*>        the shifted eigenvalues to suitable accuracy.
*>    (d) For each eigenvalue with a large enough relative separation compute
*>        the corresponding eigenvector by forming a rank revealing twisted
*>        factorization. Go back to (c) for any clusters that remain.
*>
*> For more details, see:
*> - Inderjit S. Dhillon and Beresford N. Parlett: "Multiple representations
*>   to compute orthogonal eigenvectors of symmetric tridiagonal matrices,"
*>   Linear Algebra and its Applications, 387(1), pp. 1-28, August 2004.
*> - Inderjit Dhillon and Beresford Parlett: "Orthogonal Eigenvectors and
*>   Relative Gaps," SIAM Journal on Matrix Analysis and Applications, Vol. 25,
*>   2004.  Also LAPACK Working Note 154.
*> - Inderjit Dhillon: "A new O(n^2) algorithm for the symmetric
*>   tridiagonal eigenvalue/eigenvector problem",
*>   Computer Science Division Technical Report No. UCB/CSD-97-971,
*>   UC Berkeley, May 1997.
*>
*> Further Details
*> 1.ZSTEMR works only on machines which follow IEEE-754
*> floating-point standard in their handling of infinities and NaNs.
*> This permits the use of efficient inner loops avoiding a check for
*> zero divisors.
*>
*> 2. LAPACK routines can be used to reduce a complex Hermitean matrix to
*> real symmetric tridiagonal form.
*>
*> (Any complex Hermitean tridiagonal matrix has real values on its diagonal
*> and potentially complex numbers on its off-diagonals. By applying a
*> similarity transform with an appropriate diagonal matrix
*> diag(1,e^{i \phy_1}, ... , e^{i \phy_{n-1}}), the complex Hermitean
*> matrix can be transformed into a real symmetric matrix and complex
*> arithmetic can be entirely avoided.)
*>
*> While the eigenvectors of the real symmetric tridiagonal matrix are real,
*> the eigenvectors of original complex Hermitean matrix have complex entries
*> in general.
*> Since LAPACK drivers overwrite the matrix data with the eigenvectors,
*> ZSTEMR accepts complex workspace to facilitate interoperability
*> with ZUNMTR or ZUPMTR.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBZ
*> \verbatim
*>          JOBZ is CHARACTER*1
*>          = 'N':  Compute eigenvalues only;
*>          = 'V':  Compute eigenvalues and eigenvectors.
*> \endverbatim
*>
*> \param[in] RANGE
*> \verbatim
*>          RANGE is CHARACTER*1
*>          = 'A': all eigenvalues will be found.
*>          = 'V': all eigenvalues in the half-open interval (VL,VU]
*>                 will be found.
*>          = 'I': the IL-th through IU-th eigenvalues will be found.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          On entry, the N diagonal elements of the tridiagonal matrix
*>          T. On exit, D is overwritten.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N)
*>          On entry, the (N-1) subdiagonal elements of the tridiagonal
*>          matrix T in elements 1 to N-1 of E. E(N) need not be set on
*>          input, but is used internally as workspace.
*>          On exit, E is overwritten.
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is DOUBLE PRECISION
*>
*>          If RANGE='V', the lower bound of the interval to
*>          be searched for eigenvalues. VL < VU.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>          VU is DOUBLE PRECISION
*>
*>          If RANGE='V', the upper bound of the interval to
*>          be searched for eigenvalues. VL < VU.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] IL
*> \verbatim
*>          IL is INTEGER
*>
*>          If RANGE='I', the index of the
*>          smallest eigenvalue to be returned.
*>          1 <= IL <= IU <= N, if N > 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[in] IU
*> \verbatim
*>          IU is INTEGER
*>
*>          If RANGE='I', the index of the
*>          largest eigenvalue to be returned.
*>          1 <= IL <= IU <= N, if N > 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The total number of eigenvalues found.  0 <= M <= N.
*>          If RANGE = 'A', M = N, and if RANGE = 'I', M = IU-IL+1.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is DOUBLE PRECISION array, dimension (N)
*>          The first M elements contain the selected eigenvalues in
*>          ascending order.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension (LDZ, max(1,M) )
*>          If JOBZ = 'V', and if INFO = 0, then the first M columns of Z
*>          contain the orthonormal eigenvectors of the matrix T
*>          corresponding to the selected eigenvalues, with the i-th
*>          column of Z holding the eigenvector associated with W(i).
*>          If JOBZ = 'N', then Z is not referenced.
*>          Note: the user must ensure that at least max(1,M) columns are
*>          supplied in the array Z; if RANGE = 'V', the exact value of M
*>          is not known in advance and can be computed with a workspace
*>          query by setting NZC = -1, see below.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.  LDZ >= 1, and if
*>          JOBZ = 'V', then LDZ >= max(1,N).
*> \endverbatim
*>
*> \param[in] NZC
*> \verbatim
*>          NZC is INTEGER
*>          The number of eigenvectors to be held in the array Z.
*>          If RANGE = 'A', then NZC >= max(1,N).
*>          If RANGE = 'V', then NZC >= the number of eigenvalues in (VL,VU].
*>          If RANGE = 'I', then NZC >= IU-IL+1.
*>          If NZC = -1, then a workspace query is assumed; the
*>          routine calculates the number of columns of the array Z that
*>          are needed to hold the eigenvectors.
*>          This value is returned as the first entry of the Z array, and
*>          no error message related to NZC is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] ISUPPZ
*> \verbatim
*>          ISUPPZ is INTEGER array, dimension ( 2*max(1,M) )
*>          The support of the eigenvectors in Z, i.e., the indices
*>          indicating the nonzero elements in Z. The i-th computed eigenvector
*>          is nonzero only in elements ISUPPZ( 2*i-1 ) through
*>          ISUPPZ( 2*i ). This is relevant in the case when the matrix
*>          is split. ISUPPZ is only accessed when JOBZ is 'V' and N > 0.
*> \endverbatim
*>
*> \param[in,out] TRYRAC
*> \verbatim
*>          TRYRAC is LOGICAL
*>          If TRYRAC.EQ..TRUE., indicates that the code should check whether
*>          the tridiagonal matrix defines its eigenvalues to high relative
*>          accuracy.  If so, the code uses relative-accuracy preserving
*>          algorithms that might be (a bit) slower depending on the matrix.
*>          If the matrix does not define its eigenvalues to high relative
*>          accuracy, the code can uses possibly faster algorithms.
*>          If TRYRAC.EQ..FALSE., the code is not required to guarantee
*>          relatively accurate eigenvalues and can use the fastest possible
*>          techniques.
*>          On exit, a .TRUE. TRYRAC will be set to .FALSE. if the matrix
*>          does not define its eigenvalues to high relative accuracy.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*>          On exit, if INFO = 0, WORK(1) returns the optimal
*>          (and minimal) LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= max(1,18*N)
*>          if JOBZ = 'V', and LWORK >= max(1,12*N) if JOBZ = 'N'.
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (LIWORK)
*>          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK.  LIWORK >= max(1,10*N)
*>          if the eigenvectors are desired, and LIWORK >= max(1,8*N)
*>          if only the eigenvalues are to be computed.
*>          If LIWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal size of the IWORK array,
*>          returns this value as the first entry of the IWORK array, and
*>          no error message related to LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          On exit, INFO
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = 1X, internal error in DLARRE,
*>                if INFO = 2X, internal error in ZLARRV.
*>                Here, the digit X = ABS( IINFO ) < 10, where IINFO is
*>                the nonzero error code returned by DLARRE or
*>                ZLARRV, respectively.
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
*> \ingroup complex16OTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*> Beresford Parlett, University of California, Berkeley, USA \n
*> Jim Demmel, University of California, Berkeley, USA \n
*> Inderjit Dhillon, University of Texas, Austin, USA \n
*> Osni Marques, LBNL/NERSC, USA \n
*> Christof Voemel, University of California, Berkeley, USA \n
*
*  =====================================================================
      SUBROUTINE ZSTEMR( JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
     $                   M, W, Z, LDZ, NZC, ISUPPZ, TRYRAC, WORK, LWORK,
     $                   IWORK, LIWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBZ, RANGE
      LOGICAL            TRYRAC
      INTEGER            IL, INFO, IU, LDZ, NZC, LIWORK, LWORK, M, N
      DOUBLE PRECISION VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            ISUPPZ( * ), IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), W( * ), WORK( * )
      COMPLEX*16         Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, FOUR, MINRGP
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0,
     $                     FOUR = 4.0D0,
     $                     MINRGP = 1.0D-3 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ALLEIG, INDEIG, LQUERY, VALEIG, WANTZ, ZQUERY
      INTEGER            I, IBEGIN, IEND, IFIRST, IIL, IINDBL, IINDW,
     $                   IINDWK, IINFO, IINSPL, IIU, ILAST, IN, INDD,
     $                   INDE2, INDERR, INDGP, INDGRS, INDWRK, ITMP,
     $                   ITMP2, J, JBLK, JJ, LIWMIN, LWMIN, NSPLIT,
     $                   NZCMIN, OFFSET, WBEGIN, WEND
      DOUBLE PRECISION   BIGNUM, CS, EPS, PIVMIN, R1, R2, RMAX, RMIN,
     $                   RTOL1, RTOL2, SAFMIN, SCALE, SMLNUM, SN,
     $                   THRESH, TMP, TNRM, WL, WU
*     ..
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLANST
      EXTERNAL           LSAME, DLAMCH, DLANST
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLAE2, DLAEV2, DLARRC, DLARRE, DLARRJ,
     $                   DLARRR, DLASRT, DSCAL, XERBLA, ZLARRV, ZSWAP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, SQRT


*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      WANTZ = LSAME( JOBZ, 'V' )
      ALLEIG = LSAME( RANGE, 'A' )
      VALEIG = LSAME( RANGE, 'V' )
      INDEIG = LSAME( RANGE, 'I' )
*
      LQUERY = ( ( LWORK.EQ.-1 ).OR.( LIWORK.EQ.-1 ) )
      ZQUERY = ( NZC.EQ.-1 )

*     DSTEMR needs WORK of size 6*N, IWORK of size 3*N.
*     In addition, DLARRE needs WORK of size 6*N, IWORK of size 5*N.
*     Furthermore, ZLARRV needs WORK of size 12*N, IWORK of size 7*N.
      IF( WANTZ ) THEN
         LWMIN = 18*N
         LIWMIN = 10*N
      ELSE
*        need less workspace if only the eigenvalues are wanted
         LWMIN = 12*N
         LIWMIN = 8*N
      ENDIF

      WL = ZERO
      WU = ZERO
      IIL = 0
      IIU = 0
      NSPLIT = 0

      IF( VALEIG ) THEN
*        We do not reference VL, VU in the cases RANGE = 'I','A'
*        The interval (WL, WU] contains all the wanted eigenvalues.
*        It is either given by the user or computed in DLARRE.
         WL = VL
         WU = VU
      ELSEIF( INDEIG ) THEN
*        We do not reference IL, IU in the cases RANGE = 'V','A'
         IIL = IL
         IIU = IU
      ENDIF
*
      INFO = 0
      IF( .NOT.( WANTZ .OR. LSAME( JOBZ, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( ALLEIG .OR. VALEIG .OR. INDEIG ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( VALEIG .AND. N.GT.0 .AND. WU.LE.WL ) THEN
         INFO = -7
      ELSE IF( INDEIG .AND. ( IIL.LT.1 .OR. IIL.GT.N ) ) THEN
         INFO = -8
      ELSE IF( INDEIG .AND. ( IIU.LT.IIL .OR. IIU.GT.N ) ) THEN
         INFO = -9
      ELSE IF( LDZ.LT.1 .OR. ( WANTZ .AND. LDZ.LT.N ) ) THEN
         INFO = -13
      ELSE IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
         INFO = -17
      ELSE IF( LIWORK.LT.LIWMIN .AND. .NOT.LQUERY ) THEN
         INFO = -19
      END IF
*
*     Get machine constants.
*
      SAFMIN = DLAMCH( 'Safe minimum' )
      EPS = DLAMCH( 'Precision' )
      SMLNUM = SAFMIN / EPS
      BIGNUM = ONE / SMLNUM
      RMIN = SQRT( SMLNUM )
      RMAX = MIN( SQRT( BIGNUM ), ONE / SQRT( SQRT( SAFMIN ) ) )
*
      IF( INFO.EQ.0 ) THEN
         WORK( 1 ) = LWMIN
         IWORK( 1 ) = LIWMIN
*
         IF( WANTZ .AND. ALLEIG ) THEN
            NZCMIN = N
         ELSE IF( WANTZ .AND. VALEIG ) THEN
            CALL DLARRC( 'T', N, VL, VU, D, E, SAFMIN,
     $                            NZCMIN, ITMP, ITMP2, INFO )
         ELSE IF( WANTZ .AND. INDEIG ) THEN
            NZCMIN = IIU-IIL+1
         ELSE
*           WANTZ .EQ. FALSE.
            NZCMIN = 0
         ENDIF
         IF( ZQUERY .AND. INFO.EQ.0 ) THEN
            Z( 1,1 ) = NZCMIN
         ELSE IF( NZC.LT.NZCMIN .AND. .NOT.ZQUERY ) THEN
            INFO = -14
         END IF
      END IF

      IF( INFO.NE.0 ) THEN
*
         CALL XERBLA( 'ZSTEMR', -INFO )
*
         RETURN
      ELSE IF( LQUERY .OR. ZQUERY ) THEN
         RETURN
      END IF
*
*     Handle N = 0, 1, and 2 cases immediately
*
      M = 0
      IF( N.EQ.0 )
     $   RETURN
*
      IF( N.EQ.1 ) THEN
         IF( ALLEIG .OR. INDEIG ) THEN
            M = 1
            W( 1 ) = D( 1 )
         ELSE
            IF( WL.LT.D( 1 ) .AND. WU.GE.D( 1 ) ) THEN
               M = 1
               W( 1 ) = D( 1 )
            END IF
         END IF
         IF( WANTZ.AND.(.NOT.ZQUERY) ) THEN
            Z( 1, 1 ) = ONE
            ISUPPZ(1) = 1
            ISUPPZ(2) = 1
         END IF
         RETURN
      END IF
*
      IF( N.EQ.2 ) THEN
         IF( .NOT.WANTZ ) THEN
            CALL DLAE2( D(1), E(1), D(2), R1, R2 )
         ELSE IF( WANTZ.AND.(.NOT.ZQUERY) ) THEN
            CALL DLAEV2( D(1), E(1), D(2), R1, R2, CS, SN )
         END IF
         IF( ALLEIG.OR.
     $      (VALEIG.AND.(R2.GT.WL).AND.
     $                  (R2.LE.WU)).OR.
     $      (INDEIG.AND.(IIL.EQ.1)) ) THEN
            M = M+1
            W( M ) = R2
            IF( WANTZ.AND.(.NOT.ZQUERY) ) THEN
               Z( 1, M ) = -SN
               Z( 2, M ) = CS
*              Note: At most one of SN and CS can be zero.
               IF (SN.NE.ZERO) THEN
                  IF (CS.NE.ZERO) THEN
                     ISUPPZ(2*M-1) = 1
                     ISUPPZ(2*M) = 2
                  ELSE
                     ISUPPZ(2*M-1) = 1
                     ISUPPZ(2*M) = 1
                  END IF
               ELSE
                  ISUPPZ(2*M-1) = 2
                  ISUPPZ(2*M) = 2
               END IF
            ENDIF
         ENDIF
         IF( ALLEIG.OR.
     $      (VALEIG.AND.(R1.GT.WL).AND.
     $                  (R1.LE.WU)).OR.
     $      (INDEIG.AND.(IIU.EQ.2)) ) THEN
            M = M+1
            W( M ) = R1
            IF( WANTZ.AND.(.NOT.ZQUERY) ) THEN
               Z( 1, M ) = CS
               Z( 2, M ) = SN
*              Note: At most one of SN and CS can be zero.
               IF (SN.NE.ZERO) THEN
                  IF (CS.NE.ZERO) THEN
                     ISUPPZ(2*M-1) = 1
                     ISUPPZ(2*M) = 2
                  ELSE
                     ISUPPZ(2*M-1) = 1
                     ISUPPZ(2*M) = 1
                  END IF
               ELSE
                  ISUPPZ(2*M-1) = 2
                  ISUPPZ(2*M) = 2
               END IF
            ENDIF
         ENDIF
      ELSE

*        Continue with general N

         INDGRS = 1
         INDERR = 2*N + 1
         INDGP = 3*N + 1
         INDD = 4*N + 1
         INDE2 = 5*N + 1
         INDWRK = 6*N + 1
*
         IINSPL = 1
         IINDBL = N + 1
         IINDW = 2*N + 1
         IINDWK = 3*N + 1
*
*        Scale matrix to allowable range, if necessary.
*        The allowable range is related to the PIVMIN parameter; see the
*        comments in DLARRD.  The preference for scaling small values
*        up is heuristic; we expect users' matrices not to be close to the
*        RMAX threshold.
*
         SCALE = ONE
         TNRM = DLANST( 'M', N, D, E )
         IF( TNRM.GT.ZERO .AND. TNRM.LT.RMIN ) THEN
            SCALE = RMIN / TNRM
         ELSE IF( TNRM.GT.RMAX ) THEN
            SCALE = RMAX / TNRM
         END IF
         IF( SCALE.NE.ONE ) THEN
            CALL DSCAL( N, SCALE, D, 1 )
            CALL DSCAL( N-1, SCALE, E, 1 )
            TNRM = TNRM*SCALE
            IF( VALEIG ) THEN
*              If eigenvalues in interval have to be found,
*              scale (WL, WU] accordingly
               WL = WL*SCALE
               WU = WU*SCALE
            ENDIF
         END IF
*
*        Compute the desired eigenvalues of the tridiagonal after splitting
*        into smaller subblocks if the corresponding off-diagonal elements
*        are small
*        THRESH is the splitting parameter for DLARRE
*        A negative THRESH forces the old splitting criterion based on the
*        size of the off-diagonal. A positive THRESH switches to splitting
*        which preserves relative accuracy.
*
         IF( TRYRAC ) THEN
*           Test whether the matrix warrants the more expensive relative approach.
            CALL DLARRR( N, D, E, IINFO )
         ELSE
*           The user does not care about relative accurately eigenvalues
            IINFO = -1
         ENDIF
*        Set the splitting criterion
         IF (IINFO.EQ.0) THEN
            THRESH = EPS
         ELSE
            THRESH = -EPS
*           relative accuracy is desired but T does not guarantee it
            TRYRAC = .FALSE.
         ENDIF
*
         IF( TRYRAC ) THEN
*           Copy original diagonal, needed to guarantee relative accuracy
            CALL DCOPY(N,D,1,WORK(INDD),1)
         ENDIF
*        Store the squares of the offdiagonal values of T
         DO 5 J = 1, N-1
            WORK( INDE2+J-1 ) = E(J)**2
 5    CONTINUE

*        Set the tolerance parameters for bisection
         IF( .NOT.WANTZ ) THEN
*           DLARRE computes the eigenvalues to full precision.
            RTOL1 = FOUR * EPS
            RTOL2 = FOUR * EPS
         ELSE
*           DLARRE computes the eigenvalues to less than full precision.
*           ZLARRV will refine the eigenvalue approximations, and we only
*           need less accurate initial bisection in DLARRE.
*           Note: these settings do only affect the subset case and DLARRE
            RTOL1 = SQRT(EPS)
            RTOL2 = MAX( SQRT(EPS)*5.0D-3, FOUR * EPS )
         ENDIF
         CALL DLARRE( RANGE, N, WL, WU, IIL, IIU, D, E,
     $             WORK(INDE2), RTOL1, RTOL2, THRESH, NSPLIT,
     $             IWORK( IINSPL ), M, W, WORK( INDERR ),
     $             WORK( INDGP ), IWORK( IINDBL ),
     $             IWORK( IINDW ), WORK( INDGRS ), PIVMIN,
     $             WORK( INDWRK ), IWORK( IINDWK ), IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = 10 + ABS( IINFO )
            RETURN
         END IF
*        Note that if RANGE .NE. 'V', DLARRE computes bounds on the desired
*        part of the spectrum. All desired eigenvalues are contained in
*        (WL,WU]


         IF( WANTZ ) THEN
*
*           Compute the desired eigenvectors corresponding to the computed
*           eigenvalues
*
            CALL ZLARRV( N, WL, WU, D, E,
     $                PIVMIN, IWORK( IINSPL ), M,
     $                1, M, MINRGP, RTOL1, RTOL2,
     $                W, WORK( INDERR ), WORK( INDGP ), IWORK( IINDBL ),
     $                IWORK( IINDW ), WORK( INDGRS ), Z, LDZ,
     $                ISUPPZ, WORK( INDWRK ), IWORK( IINDWK ), IINFO )
            IF( IINFO.NE.0 ) THEN
               INFO = 20 + ABS( IINFO )
               RETURN
            END IF
         ELSE
*           DLARRE computes eigenvalues of the (shifted) root representation
*           ZLARRV returns the eigenvalues of the unshifted matrix.
*           However, if the eigenvectors are not desired by the user, we need
*           to apply the corresponding shifts from DLARRE to obtain the
*           eigenvalues of the original matrix.
            DO 20 J = 1, M
               ITMP = IWORK( IINDBL+J-1 )
               W( J ) = W( J ) + E( IWORK( IINSPL+ITMP-1 ) )
 20      CONTINUE
         END IF
*

         IF ( TRYRAC ) THEN
*           Refine computed eigenvalues so that they are relatively accurate
*           with respect to the original matrix T.
            IBEGIN = 1
            WBEGIN = 1
            DO 39  JBLK = 1, IWORK( IINDBL+M-1 )
               IEND = IWORK( IINSPL+JBLK-1 )
               IN = IEND - IBEGIN + 1
               WEND = WBEGIN - 1
*              check if any eigenvalues have to be refined in this block
 36         CONTINUE
               IF( WEND.LT.M ) THEN
                  IF( IWORK( IINDBL+WEND ).EQ.JBLK ) THEN
                     WEND = WEND + 1
                     GO TO 36
                  END IF
               END IF
               IF( WEND.LT.WBEGIN ) THEN
                  IBEGIN = IEND + 1
                  GO TO 39
               END IF

               OFFSET = IWORK(IINDW+WBEGIN-1)-1
               IFIRST = IWORK(IINDW+WBEGIN-1)
               ILAST = IWORK(IINDW+WEND-1)
               RTOL2 = FOUR * EPS
               CALL DLARRJ( IN,
     $                   WORK(INDD+IBEGIN-1), WORK(INDE2+IBEGIN-1),
     $                   IFIRST, ILAST, RTOL2, OFFSET, W(WBEGIN),
     $                   WORK( INDERR+WBEGIN-1 ),
     $                   WORK( INDWRK ), IWORK( IINDWK ), PIVMIN,
     $                   TNRM, IINFO )
               IBEGIN = IEND + 1
               WBEGIN = WEND + 1
 39      CONTINUE
         ENDIF
*
*        If matrix was scaled, then rescale eigenvalues appropriately.
*
         IF( SCALE.NE.ONE ) THEN
            CALL DSCAL( M, ONE / SCALE, W, 1 )
         END IF
      END IF
*
*     If eigenvalues are not in increasing order, then sort them,
*     possibly along with eigenvectors.
*
      IF( NSPLIT.GT.1 .OR. N.EQ.2 ) THEN
         IF( .NOT. WANTZ ) THEN
            CALL DLASRT( 'I', M, W, IINFO )
            IF( IINFO.NE.0 ) THEN
               INFO = 3
               RETURN
            END IF
         ELSE
            DO 60 J = 1, M - 1
               I = 0
               TMP = W( J )
               DO 50 JJ = J + 1, M
                  IF( W( JJ ).LT.TMP ) THEN
                     I = JJ
                     TMP = W( JJ )
                  END IF
 50            CONTINUE
               IF( I.NE.0 ) THEN
                  W( I ) = W( J )
                  W( J ) = TMP
                  IF( WANTZ ) THEN
                     CALL ZSWAP( N, Z( 1, I ), 1, Z( 1, J ), 1 )
                     ITMP = ISUPPZ( 2*I-1 )
                     ISUPPZ( 2*I-1 ) = ISUPPZ( 2*J-1 )
                     ISUPPZ( 2*J-1 ) = ITMP
                     ITMP = ISUPPZ( 2*I )
                     ISUPPZ( 2*I ) = ISUPPZ( 2*J )
                     ISUPPZ( 2*J ) = ITMP
                  END IF
               END IF
 60         CONTINUE
         END IF
      ENDIF
*
*
      WORK( 1 ) = LWMIN
      IWORK( 1 ) = LIWMIN
      RETURN
*
*     End of ZSTEMR
*
      END
