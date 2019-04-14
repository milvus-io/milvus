*> \brief <b> ZHEEVR computes the eigenvalues and, optionally, the left and/or right eigenvectors for HE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHEEVR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zheevr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zheevr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zheevr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHEEVR( JOBZ, RANGE, UPLO, N, A, LDA, VL, VU, IL, IU,
*                          ABSTOL, M, W, Z, LDZ, ISUPPZ, WORK, LWORK,
*                          RWORK, LRWORK, IWORK, LIWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBZ, RANGE, UPLO
*       INTEGER            IL, INFO, IU, LDA, LDZ, LIWORK, LRWORK, LWORK,
*      $                   M, N
*       DOUBLE PRECISION   ABSTOL, VL, VU
*       ..
*       .. Array Arguments ..
*       INTEGER            ISUPPZ( * ), IWORK( * )
*       DOUBLE PRECISION   RWORK( * ), W( * )
*       COMPLEX*16         A( LDA, * ), WORK( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHEEVR computes selected eigenvalues and, optionally, eigenvectors
*> of a complex Hermitian matrix A.  Eigenvalues and eigenvectors can
*> be selected by specifying either a range of values or a range of
*> indices for the desired eigenvalues.
*>
*> ZHEEVR first reduces the matrix A to tridiagonal form T with a call
*> to ZHETRD.  Then, whenever possible, ZHEEVR calls ZSTEMR to compute
*> eigenspectrum using Relatively Robust Representations.  ZSTEMR
*> computes eigenvalues by the dqds algorithm, while orthogonal
*> eigenvectors are computed from various "good" L D L^T representations
*> (also known as Relatively Robust Representations). Gram-Schmidt
*> orthogonalization is avoided as far as possible. More specifically,
*> the various steps of the algorithm are as follows.
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
*> The desired accuracy of the output can be specified by the input
*> parameter ABSTOL.
*>
*> For more details, see DSTEMR's documentation and:
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
*>
*> Note 1 : ZHEEVR calls ZSTEMR when the full spectrum is requested
*> on machines which conform to the ieee-754 floating point standard.
*> ZHEEVR calls DSTEBZ and ZSTEIN on non-ieee machines and
*> when partial spectrum requests are made.
*>
*> Normal execution of ZSTEMR may create NaNs and infinities and
*> hence may abort due to a floating point exception in environments
*> which do not handle NaNs and infinities in the ieee standard default
*> manner.
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
*>          For RANGE = 'V' or 'I' and IU - IL < N - 1, DSTEBZ and
*>          ZSTEIN are called
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          On entry, the Hermitian matrix A.  If UPLO = 'U', the
*>          leading N-by-N upper triangular part of A contains the
*>          upper triangular part of the matrix A.  If UPLO = 'L',
*>          the leading N-by-N lower triangular part of A contains
*>          the lower triangular part of the matrix A.
*>          On exit, the lower triangle (if UPLO='L') or the upper
*>          triangle (if UPLO='U') of A, including the diagonal, is
*>          destroyed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is DOUBLE PRECISION
*>          If RANGE='V', the lower bound of the interval to
*>          be searched for eigenvalues. VL < VU.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>          VU is DOUBLE PRECISION
*>          If RANGE='V', the upper bound of the interval to
*>          be searched for eigenvalues. VL < VU.
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
*> \param[in] ABSTOL
*> \verbatim
*>          ABSTOL is DOUBLE PRECISION
*>          The absolute error tolerance for the eigenvalues.
*>          An approximate eigenvalue is accepted as converged
*>          when it is determined to lie in an interval [a,b]
*>          of width less than or equal to
*>
*>                  ABSTOL + EPS *   max( |a|,|b| ) ,
*>
*>          where EPS is the machine precision.  If ABSTOL is less than
*>          or equal to zero, then  EPS*|T|  will be used in its place,
*>          where |T| is the 1-norm of the tridiagonal matrix obtained
*>          by reducing A to tridiagonal form.
*>
*>          See "Computing Small Singular Values of Bidiagonal Matrices
*>          with Guaranteed High Relative Accuracy," by Demmel and
*>          Kahan, LAPACK Working Note #3.
*>
*>          If high relative accuracy is important, set ABSTOL to
*>          DLAMCH( 'Safe minimum' ).  Doing so will guarantee that
*>          eigenvalues are computed to high relative accuracy when
*>          possible in future releases.  The current code does not
*>          make any guarantees about high relative accuracy, but
*>          furutre releases will. See J. Barlow and J. Demmel,
*>          "Computing Accurate Eigensystems of Scaled Diagonally
*>          Dominant Matrices", LAPACK Working Note #7, for a discussion
*>          of which matrices define their eigenvalues to high relative
*>          accuracy.
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
*>          Z is COMPLEX*16 array, dimension (LDZ, max(1,M))
*>          If JOBZ = 'V', then if INFO = 0, the first M columns of Z
*>          contain the orthonormal eigenvectors of the matrix A
*>          corresponding to the selected eigenvalues, with the i-th
*>          column of Z holding the eigenvector associated with W(i).
*>          If JOBZ = 'N', then Z is not referenced.
*>          Note: the user must ensure that at least max(1,M) columns are
*>          supplied in the array Z; if RANGE = 'V', the exact value of M
*>          is not known in advance and an upper bound must be used.
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
*>          indicating the nonzero elements in Z. The i-th eigenvector
*>          is nonzero only in elements ISUPPZ( 2*i-1 ) through
*>          ISUPPZ( 2*i ). This is an output of ZSTEMR (tridiagonal
*>          matrix). The support of the eigenvectors of A is typically
*>          1:N because of the unitary transformations applied by ZUNMTR.
*>          Implemented only for RANGE = 'A' or 'I' and IU - IL = N - 1
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of the array WORK.  LWORK >= max(1,2*N).
*>          For optimal efficiency, LWORK >= (NB+1)*N,
*>          where NB is the max of the blocksize for ZHETRD and for
*>          ZUNMTR as returned by ILAENV.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal sizes of the WORK, RWORK and
*>          IWORK arrays, returns these values as the first entries of
*>          the WORK, RWORK and IWORK arrays, and no error message
*>          related to LWORK or LRWORK or LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (MAX(1,LRWORK))
*>          On exit, if INFO = 0, RWORK(1) returns the optimal
*>          (and minimal) LRWORK.
*> \endverbatim
*>
*> \param[in] LRWORK
*> \verbatim
*>          LRWORK is INTEGER
*>          The length of the array RWORK.  LRWORK >= max(1,24*N).
*>
*>          If LRWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal sizes of the WORK, RWORK
*>          and IWORK arrays, returns these values as the first entries
*>          of the WORK, RWORK and IWORK arrays, and no error message
*>          related to LWORK or LRWORK or LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MAX(1,LIWORK))
*>          On exit, if INFO = 0, IWORK(1) returns the optimal
*>          (and minimal) LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK.  LIWORK >= max(1,10*N).
*>
*>          If LIWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal sizes of the WORK, RWORK
*>          and IWORK arrays, returns these values as the first entries
*>          of the WORK, RWORK and IWORK arrays, and no error message
*>          related to LWORK or LRWORK or LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  Internal error
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
*> \ingroup complex16HEeigen
*
*> \par Contributors:
*  ==================
*>
*>     Inderjit Dhillon, IBM Almaden, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*>     Ken Stanley, Computer Science Division, University of
*>       California at Berkeley, USA \n
*>     Jason Riedy, Computer Science Division, University of
*>       California at Berkeley, USA \n
*>
*  =====================================================================
      SUBROUTINE ZHEEVR( JOBZ, RANGE, UPLO, N, A, LDA, VL, VU, IL, IU,
     $                   ABSTOL, M, W, Z, LDZ, ISUPPZ, WORK, LWORK,
     $                   RWORK, LRWORK, IWORK, LIWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBZ, RANGE, UPLO
      INTEGER            IL, INFO, IU, LDA, LDZ, LIWORK, LRWORK, LWORK,
     $                   M, N
      DOUBLE PRECISION   ABSTOL, VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            ISUPPZ( * ), IWORK( * )
      DOUBLE PRECISION   RWORK( * ), W( * )
      COMPLEX*16         A( LDA, * ), WORK( * ), Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TWO = 2.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ALLEIG, INDEIG, LOWER, LQUERY, TEST, VALEIG,
     $                   WANTZ, TRYRAC
      CHARACTER          ORDER
      INTEGER            I, IEEEOK, IINFO, IMAX, INDIBL, INDIFL, INDISP,
     $                   INDIWO, INDRD, INDRDD, INDRE, INDREE, INDRWK,
     $                   INDTAU, INDWK, INDWKN, ISCALE, ITMP1, J, JJ,
     $                   LIWMIN, LLWORK, LLRWORK, LLWRKN, LRWMIN,
     $                   LWKOPT, LWMIN, NB, NSPLIT
      DOUBLE PRECISION   ABSTLL, ANRM, BIGNUM, EPS, RMAX, RMIN, SAFMIN,
     $                   SIGMA, SMLNUM, TMP1, VLL, VUU
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANSY
      EXTERNAL           LSAME, ILAENV, DLAMCH, ZLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DSCAL, DSTEBZ, DSTERF, XERBLA, ZDSCAL,
     $                   ZHETRD, ZSTEMR, ZSTEIN, ZSWAP, ZUNMTR
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      IEEEOK = ILAENV( 10, 'ZHEEVR', 'N', 1, 2, 3, 4 )
*
      LOWER = LSAME( UPLO, 'L' )
      WANTZ = LSAME( JOBZ, 'V' )
      ALLEIG = LSAME( RANGE, 'A' )
      VALEIG = LSAME( RANGE, 'V' )
      INDEIG = LSAME( RANGE, 'I' )
*
      LQUERY = ( ( LWORK.EQ.-1 ) .OR. ( LRWORK.EQ.-1 ) .OR.
     $         ( LIWORK.EQ.-1 ) )
*
      LRWMIN = MAX( 1, 24*N )
      LIWMIN = MAX( 1, 10*N )
      LWMIN = MAX( 1, 2*N )
*
      INFO = 0
      IF( .NOT.( WANTZ .OR. LSAME( JOBZ, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( ALLEIG .OR. VALEIG .OR. INDEIG ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( LOWER .OR. LSAME( UPLO, 'U' ) ) ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE
         IF( VALEIG ) THEN
            IF( N.GT.0 .AND. VU.LE.VL )
     $         INFO = -8
         ELSE IF( INDEIG ) THEN
            IF( IL.LT.1 .OR. IL.GT.MAX( 1, N ) ) THEN
               INFO = -9
            ELSE IF( IU.LT.MIN( N, IL ) .OR. IU.GT.N ) THEN
               INFO = -10
            END IF
         END IF
      END IF
      IF( INFO.EQ.0 ) THEN
         IF( LDZ.LT.1 .OR. ( WANTZ .AND. LDZ.LT.N ) ) THEN
            INFO = -15
         END IF
      END IF
*
      IF( INFO.EQ.0 ) THEN
         NB = ILAENV( 1, 'ZHETRD', UPLO, N, -1, -1, -1 )
         NB = MAX( NB, ILAENV( 1, 'ZUNMTR', UPLO, N, -1, -1, -1 ) )
         LWKOPT = MAX( ( NB+1 )*N, LWMIN )
         WORK( 1 ) = LWKOPT
         RWORK( 1 ) = LRWMIN
         IWORK( 1 ) = LIWMIN
*
         IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -18
         ELSE IF( LRWORK.LT.LRWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -20
         ELSE IF( LIWORK.LT.LIWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -22
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHEEVR', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      M = 0
      IF( N.EQ.0 ) THEN
         WORK( 1 ) = 1
         RETURN
      END IF
*
      IF( N.EQ.1 ) THEN
         WORK( 1 ) = 2
         IF( ALLEIG .OR. INDEIG ) THEN
            M = 1
            W( 1 ) = DBLE( A( 1, 1 ) )
         ELSE
            IF( VL.LT.DBLE( A( 1, 1 ) ) .AND. VU.GE.DBLE( A( 1, 1 ) ) )
     $           THEN
               M = 1
               W( 1 ) = DBLE( A( 1, 1 ) )
            END IF
         END IF
         IF( WANTZ ) THEN
            Z( 1, 1 ) = ONE
            ISUPPZ( 1 ) = 1
            ISUPPZ( 2 ) = 1
         END IF
         RETURN
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
*     Scale matrix to allowable range, if necessary.
*
      ISCALE = 0
      ABSTLL = ABSTOL
      IF (VALEIG) THEN
         VLL = VL
         VUU = VU
      END IF
      ANRM = ZLANSY( 'M', UPLO, N, A, LDA, RWORK )
      IF( ANRM.GT.ZERO .AND. ANRM.LT.RMIN ) THEN
         ISCALE = 1
         SIGMA = RMIN / ANRM
      ELSE IF( ANRM.GT.RMAX ) THEN
         ISCALE = 1
         SIGMA = RMAX / ANRM
      END IF
      IF( ISCALE.EQ.1 ) THEN
         IF( LOWER ) THEN
            DO 10 J = 1, N
               CALL ZDSCAL( N-J+1, SIGMA, A( J, J ), 1 )
   10       CONTINUE
         ELSE
            DO 20 J = 1, N
               CALL ZDSCAL( J, SIGMA, A( 1, J ), 1 )
   20       CONTINUE
         END IF
         IF( ABSTOL.GT.0 )
     $      ABSTLL = ABSTOL*SIGMA
         IF( VALEIG ) THEN
            VLL = VL*SIGMA
            VUU = VU*SIGMA
         END IF
      END IF

*     Initialize indices into workspaces.  Note: The IWORK indices are
*     used only if DSTERF or ZSTEMR fail.

*     WORK(INDTAU:INDTAU+N-1) stores the complex scalar factors of the
*     elementary reflectors used in ZHETRD.
      INDTAU = 1
*     INDWK is the starting offset of the remaining complex workspace,
*     and LLWORK is the remaining complex workspace size.
      INDWK = INDTAU + N
      LLWORK = LWORK - INDWK + 1

*     RWORK(INDRD:INDRD+N-1) stores the real tridiagonal's diagonal
*     entries.
      INDRD = 1
*     RWORK(INDRE:INDRE+N-1) stores the off-diagonal entries of the
*     tridiagonal matrix from ZHETRD.
      INDRE = INDRD + N
*     RWORK(INDRDD:INDRDD+N-1) is a copy of the diagonal entries over
*     -written by ZSTEMR (the DSTERF path copies the diagonal to W).
      INDRDD = INDRE + N
*     RWORK(INDREE:INDREE+N-1) is a copy of the off-diagonal entries over
*     -written while computing the eigenvalues in DSTERF and ZSTEMR.
      INDREE = INDRDD + N
*     INDRWK is the starting offset of the left-over real workspace, and
*     LLRWORK is the remaining workspace size.
      INDRWK = INDREE + N
      LLRWORK = LRWORK - INDRWK + 1

*     IWORK(INDIBL:INDIBL+M-1) corresponds to IBLOCK in DSTEBZ and
*     stores the block indices of each of the M<=N eigenvalues.
      INDIBL = 1
*     IWORK(INDISP:INDISP+NSPLIT-1) corresponds to ISPLIT in DSTEBZ and
*     stores the starting and finishing indices of each block.
      INDISP = INDIBL + N
*     IWORK(INDIFL:INDIFL+N-1) stores the indices of eigenvectors
*     that corresponding to eigenvectors that fail to converge in
*     DSTEIN.  This information is discarded; if any fail, the driver
*     returns INFO > 0.
      INDIFL = INDISP + N
*     INDIWO is the offset of the remaining integer workspace.
      INDIWO = INDIFL + N

*
*     Call ZHETRD to reduce Hermitian matrix to tridiagonal form.
*
      CALL ZHETRD( UPLO, N, A, LDA, RWORK( INDRD ), RWORK( INDRE ),
     $             WORK( INDTAU ), WORK( INDWK ), LLWORK, IINFO )
*
*     If all eigenvalues are desired
*     then call DSTERF or ZSTEMR and ZUNMTR.
*
      TEST = .FALSE.
      IF( INDEIG ) THEN
         IF( IL.EQ.1 .AND. IU.EQ.N ) THEN
            TEST = .TRUE.
         END IF
      END IF
      IF( ( ALLEIG.OR.TEST ) .AND. ( IEEEOK.EQ.1 ) ) THEN
         IF( .NOT.WANTZ ) THEN
            CALL DCOPY( N, RWORK( INDRD ), 1, W, 1 )
            CALL DCOPY( N-1, RWORK( INDRE ), 1, RWORK( INDREE ), 1 )
            CALL DSTERF( N, W, RWORK( INDREE ), INFO )
         ELSE
            CALL DCOPY( N-1, RWORK( INDRE ), 1, RWORK( INDREE ), 1 )
            CALL DCOPY( N, RWORK( INDRD ), 1, RWORK( INDRDD ), 1 )
*
            IF (ABSTOL .LE. TWO*N*EPS) THEN
               TRYRAC = .TRUE.
            ELSE
               TRYRAC = .FALSE.
            END IF
            CALL ZSTEMR( JOBZ, 'A', N, RWORK( INDRDD ),
     $                   RWORK( INDREE ), VL, VU, IL, IU, M, W,
     $                   Z, LDZ, N, ISUPPZ, TRYRAC,
     $                   RWORK( INDRWK ), LLRWORK,
     $                   IWORK, LIWORK, INFO )
*
*           Apply unitary matrix used in reduction to tridiagonal
*           form to eigenvectors returned by ZSTEMR.
*
            IF( WANTZ .AND. INFO.EQ.0 ) THEN
               INDWKN = INDWK
               LLWRKN = LWORK - INDWKN + 1
               CALL ZUNMTR( 'L', UPLO, 'N', N, M, A, LDA,
     $                      WORK( INDTAU ), Z, LDZ, WORK( INDWKN ),
     $                      LLWRKN, IINFO )
            END IF
         END IF
*
*
         IF( INFO.EQ.0 ) THEN
            M = N
            GO TO 30
         END IF
         INFO = 0
      END IF
*
*     Otherwise, call DSTEBZ and, if eigenvectors are desired, ZSTEIN.
*     Also call DSTEBZ and ZSTEIN if ZSTEMR fails.
*
      IF( WANTZ ) THEN
         ORDER = 'B'
      ELSE
         ORDER = 'E'
      END IF

      CALL DSTEBZ( RANGE, ORDER, N, VLL, VUU, IL, IU, ABSTLL,
     $             RWORK( INDRD ), RWORK( INDRE ), M, NSPLIT, W,
     $             IWORK( INDIBL ), IWORK( INDISP ), RWORK( INDRWK ),
     $             IWORK( INDIWO ), INFO )
*
      IF( WANTZ ) THEN
         CALL ZSTEIN( N, RWORK( INDRD ), RWORK( INDRE ), M, W,
     $                IWORK( INDIBL ), IWORK( INDISP ), Z, LDZ,
     $                RWORK( INDRWK ), IWORK( INDIWO ), IWORK( INDIFL ),
     $                INFO )
*
*        Apply unitary matrix used in reduction to tridiagonal
*        form to eigenvectors returned by ZSTEIN.
*
         INDWKN = INDWK
         LLWRKN = LWORK - INDWKN + 1
         CALL ZUNMTR( 'L', UPLO, 'N', N, M, A, LDA, WORK( INDTAU ), Z,
     $                LDZ, WORK( INDWKN ), LLWRKN, IINFO )
      END IF
*
*     If matrix was scaled, then rescale eigenvalues appropriately.
*
   30 CONTINUE
      IF( ISCALE.EQ.1 ) THEN
         IF( INFO.EQ.0 ) THEN
            IMAX = M
         ELSE
            IMAX = INFO - 1
         END IF
         CALL DSCAL( IMAX, ONE / SIGMA, W, 1 )
      END IF
*
*     If eigenvalues are not in order, then sort them, along with
*     eigenvectors.
*
      IF( WANTZ ) THEN
         DO 50 J = 1, M - 1
            I = 0
            TMP1 = W( J )
            DO 40 JJ = J + 1, M
               IF( W( JJ ).LT.TMP1 ) THEN
                  I = JJ
                  TMP1 = W( JJ )
               END IF
   40       CONTINUE
*
            IF( I.NE.0 ) THEN
               ITMP1 = IWORK( INDIBL+I-1 )
               W( I ) = W( J )
               IWORK( INDIBL+I-1 ) = IWORK( INDIBL+J-1 )
               W( J ) = TMP1
               IWORK( INDIBL+J-1 ) = ITMP1
               CALL ZSWAP( N, Z( 1, I ), 1, Z( 1, J ), 1 )
            END IF
   50    CONTINUE
      END IF
*
*     Set WORK(1) to optimal workspace size.
*
      WORK( 1 ) = LWKOPT
      RWORK( 1 ) = LRWMIN
      IWORK( 1 ) = LIWMIN
*
      RETURN
*
*     End of ZHEEVR
*
      END
