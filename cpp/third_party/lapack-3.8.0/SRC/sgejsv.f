*> \brief \b SGEJSV
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGEJSV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgejsv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgejsv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgejsv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGEJSV( JOBA, JOBU, JOBV, JOBR, JOBT, JOBP,
*                          M, N, A, LDA, SVA, U, LDU, V, LDV,
*                          WORK, LWORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       IMPLICIT    NONE
*       INTEGER     INFO, LDA, LDU, LDV, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       REAL        A( LDA, * ), SVA( N ), U( LDU, * ), V( LDV, * ),
*      $            WORK( LWORK )
*       INTEGER     IWORK( * )
*       CHARACTER*1 JOBA, JOBP, JOBR, JOBT, JOBU, JOBV
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGEJSV computes the singular value decomposition (SVD) of a real M-by-N
*> matrix [A], where M >= N. The SVD of [A] is written as
*>
*>              [A] = [U] * [SIGMA] * [V]^t,
*>
*> where [SIGMA] is an N-by-N (M-by-N) matrix which is zero except for its N
*> diagonal elements, [U] is an M-by-N (or M-by-M) orthonormal matrix, and
*> [V] is an N-by-N orthogonal matrix. The diagonal elements of [SIGMA] are
*> the singular values of [A]. The columns of [U] and [V] are the left and
*> the right singular vectors of [A], respectively. The matrices [U] and [V]
*> are computed and stored in the arrays U and V, respectively. The diagonal
*> of [SIGMA] is computed and stored in the array SVA.
*> SGEJSV can sometimes compute tiny singular values and their singular vectors much
*> more accurately than other SVD routines, see below under Further Details.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBA
*> \verbatim
*>          JOBA is CHARACTER*1
*>         Specifies the level of accuracy:
*>       = 'C': This option works well (high relative accuracy) if A = B * D,
*>              with well-conditioned B and arbitrary diagonal matrix D.
*>              The accuracy cannot be spoiled by COLUMN scaling. The
*>              accuracy of the computed output depends on the condition of
*>              B, and the procedure aims at the best theoretical accuracy.
*>              The relative error max_{i=1:N}|d sigma_i| / sigma_i is
*>              bounded by f(M,N)*epsilon* cond(B), independent of D.
*>              The input matrix is preprocessed with the QRF with column
*>              pivoting. This initial preprocessing and preconditioning by
*>              a rank revealing QR factorization is common for all values of
*>              JOBA. Additional actions are specified as follows:
*>       = 'E': Computation as with 'C' with an additional estimate of the
*>              condition number of B. It provides a realistic error bound.
*>       = 'F': If A = D1 * C * D2 with ill-conditioned diagonal scalings
*>              D1, D2, and well-conditioned matrix C, this option gives
*>              higher accuracy than the 'C' option. If the structure of the
*>              input matrix is not known, and relative accuracy is
*>              desirable, then this option is advisable. The input matrix A
*>              is preprocessed with QR factorization with FULL (row and
*>              column) pivoting.
*>       = 'G'  Computation as with 'F' with an additional estimate of the
*>              condition number of B, where A=D*B. If A has heavily weighted
*>              rows, then using this condition number gives too pessimistic
*>              error bound.
*>       = 'A': Small singular values are the noise and the matrix is treated
*>              as numerically rank deficient. The error in the computed
*>              singular values is bounded by f(m,n)*epsilon*||A||.
*>              The computed SVD A = U * S * V^t restores A up to
*>              f(m,n)*epsilon*||A||.
*>              This gives the procedure the licence to discard (set to zero)
*>              all singular values below N*epsilon*||A||.
*>       = 'R': Similar as in 'A'. Rank revealing property of the initial
*>              QR factorization is used do reveal (using triangular factor)
*>              a gap sigma_{r+1} < epsilon * sigma_r in which case the
*>              numerical RANK is declared to be r. The SVD is computed with
*>              absolute error bounds, but more accurately than with 'A'.
*> \endverbatim
*>
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>         Specifies whether to compute the columns of U:
*>       = 'U': N columns of U are returned in the array U.
*>       = 'F': full set of M left sing. vectors is returned in the array U.
*>       = 'W': U may be used as workspace of length M*N. See the description
*>              of U.
*>       = 'N': U is not computed.
*> \endverbatim
*>
*> \param[in] JOBV
*> \verbatim
*>          JOBV is CHARACTER*1
*>         Specifies whether to compute the matrix V:
*>       = 'V': N columns of V are returned in the array V; Jacobi rotations
*>              are not explicitly accumulated.
*>       = 'J': N columns of V are returned in the array V, but they are
*>              computed as the product of Jacobi rotations. This option is
*>              allowed only if JOBU .NE. 'N', i.e. in computing the full SVD.
*>       = 'W': V may be used as workspace of length N*N. See the description
*>              of V.
*>       = 'N': V is not computed.
*> \endverbatim
*>
*> \param[in] JOBR
*> \verbatim
*>          JOBR is CHARACTER*1
*>         Specifies the RANGE for the singular values. Issues the licence to
*>         set to zero small positive singular values if they are outside
*>         specified range. If A .NE. 0 is scaled so that the largest singular
*>         value of c*A is around SQRT(BIG), BIG=SLAMCH('O'), then JOBR issues
*>         the licence to kill columns of A whose norm in c*A is less than
*>         SQRT(SFMIN) (for JOBR.EQ.'R'), or less than SMALL=SFMIN/EPSLN,
*>         where SFMIN=SLAMCH('S'), EPSLN=SLAMCH('E').
*>       = 'N': Do not kill small columns of c*A. This option assumes that
*>              BLAS and QR factorizations and triangular solvers are
*>              implemented to work in that range. If the condition of A
*>              is greater than BIG, use SGESVJ.
*>       = 'R': RESTRICTED range for sigma(c*A) is [SQRT(SFMIN), SQRT(BIG)]
*>              (roughly, as described above). This option is recommended.
*>                                             ===========================
*>         For computing the singular values in the FULL range [SFMIN,BIG]
*>         use SGESVJ.
*> \endverbatim
*>
*> \param[in] JOBT
*> \verbatim
*>          JOBT is CHARACTER*1
*>         If the matrix is square then the procedure may determine to use
*>         transposed A if A^t seems to be better with respect to convergence.
*>         If the matrix is not square, JOBT is ignored. This is subject to
*>         changes in the future.
*>         The decision is based on two values of entropy over the adjoint
*>         orbit of A^t * A. See the descriptions of WORK(6) and WORK(7).
*>       = 'T': transpose if entropy test indicates possibly faster
*>         convergence of Jacobi process if A^t is taken as input. If A is
*>         replaced with A^t, then the row pivoting is included automatically.
*>       = 'N': do not speculate.
*>         This option can be used to compute only the singular values, or the
*>         full SVD (U, SIGMA and V). For only one set of singular vectors
*>         (U or V), the caller should provide both U and V, as one of the
*>         matrices is used as workspace if the matrix A is transposed.
*>         The implementer can easily remove this constraint and make the
*>         code more complicated. See the descriptions of U and V.
*> \endverbatim
*>
*> \param[in] JOBP
*> \verbatim
*>          JOBP is CHARACTER*1
*>         Issues the licence to introduce structured perturbations to drown
*>         denormalized numbers. This licence should be active if the
*>         denormals are poorly implemented, causing slow computation,
*>         especially in cases of fast convergence (!). For details see [1,2].
*>         For the sake of simplicity, this perturbations are included only
*>         when the full SVD or only the singular values are requested. The
*>         implementer/user can easily add the perturbation for the cases of
*>         computing one set of singular vectors.
*>       = 'P': introduce perturbation
*>       = 'N': do not perturb
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>         The number of rows of the input matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         The number of columns of the input matrix A. M >= N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] SVA
*> \verbatim
*>          SVA is REAL array, dimension (N)
*>          On exit,
*>          - For WORK(1)/WORK(2) = ONE: The singular values of A. During the
*>            computation SVA contains Euclidean column norms of the
*>            iterated matrices in the array A.
*>          - For WORK(1) .NE. WORK(2): The singular values of A are
*>            (WORK(1)/WORK(2)) * SVA(1:N). This factored form is used if
*>            sigma_max(A) overflows or if small singular values have been
*>            saved from underflow by scaling the input matrix A.
*>          - If JOBR='R' then some of the singular values may be returned
*>            as exact zeros obtained by "set to zero" because they are
*>            below the numerical rank threshold or are denormalized numbers.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is REAL array, dimension ( LDU, N )
*>          If JOBU = 'U', then U contains on exit the M-by-N matrix of
*>                         the left singular vectors.
*>          If JOBU = 'F', then U contains on exit the M-by-M matrix of
*>                         the left singular vectors, including an ONB
*>                         of the orthogonal complement of the Range(A).
*>          If JOBU = 'W'  .AND. (JOBV.EQ.'V' .AND. JOBT.EQ.'T' .AND. M.EQ.N),
*>                         then U is used as workspace if the procedure
*>                         replaces A with A^t. In that case, [V] is computed
*>                         in U as left singular vectors of A^t and then
*>                         copied back to the V array. This 'W' option is just
*>                         a reminder to the caller that in this case U is
*>                         reserved as workspace of length N*N.
*>          If JOBU = 'N'  U is not referenced, unless JOBT='T'.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U,  LDU >= 1.
*>          IF  JOBU = 'U' or 'F' or 'W',  then LDU >= M.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is REAL array, dimension ( LDV, N )
*>          If JOBV = 'V', 'J' then V contains on exit the N-by-N matrix of
*>                         the right singular vectors;
*>          If JOBV = 'W', AND (JOBU.EQ.'U' AND JOBT.EQ.'T' AND M.EQ.N),
*>                         then V is used as workspace if the pprocedure
*>                         replaces A with A^t. In that case, [U] is computed
*>                         in V as right singular vectors of A^t and then
*>                         copied back to the U array. This 'W' option is just
*>                         a reminder to the caller that in this case V is
*>                         reserved as workspace of length N*N.
*>          If JOBV = 'N'  V is not referenced, unless JOBT='T'.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V,  LDV >= 1.
*>          If JOBV = 'V' or 'J' or 'W', then LDV >= N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK)
*>          On exit,
*>          WORK(1) = SCALE = WORK(2) / WORK(1) is the scaling factor such
*>                    that SCALE*SVA(1:N) are the computed singular values
*>                    of A. (See the description of SVA().)
*>          WORK(2) = See the description of WORK(1).
*>          WORK(3) = SCONDA is an estimate for the condition number of
*>                    column equilibrated A. (If JOBA .EQ. 'E' or 'G')
*>                    SCONDA is an estimate of SQRT(||(R^t * R)^(-1)||_1).
*>                    It is computed using SPOCON. It holds
*>                    N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA
*>                    where R is the triangular factor from the QRF of A.
*>                    However, if R is truncated and the numerical rank is
*>                    determined to be strictly smaller than N, SCONDA is
*>                    returned as -1, thus indicating that the smallest
*>                    singular values might be lost.
*>
*>          If full SVD is needed, the following two condition numbers are
*>          useful for the analysis of the algorithm. They are provied for
*>          a developer/implementer who is familiar with the details of
*>          the method.
*>
*>          WORK(4) = an estimate of the scaled condition number of the
*>                    triangular factor in the first QR factorization.
*>          WORK(5) = an estimate of the scaled condition number of the
*>                    triangular factor in the second QR factorization.
*>          The following two parameters are computed if JOBT .EQ. 'T'.
*>          They are provided for a developer/implementer who is familiar
*>          with the details of the method.
*>
*>          WORK(6) = the entropy of A^t*A :: this is the Shannon entropy
*>                    of diag(A^t*A) / Trace(A^t*A) taken as point in the
*>                    probability simplex.
*>          WORK(7) = the entropy of A*A^t.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          Length of WORK to confirm proper allocation of work space.
*>          LWORK depends on the job:
*>
*>          If only SIGMA is needed ( JOBU.EQ.'N', JOBV.EQ.'N' ) and
*>            -> .. no scaled condition estimate required (JOBE.EQ.'N'):
*>               LWORK >= max(2*M+N,4*N+1,7). This is the minimal requirement.
*>               ->> For optimal performance (blocked code) the optimal value
*>               is LWORK >= max(2*M+N,3*N+(N+1)*NB,7). Here NB is the optimal
*>               block size for DGEQP3 and DGEQRF.
*>               In general, optimal LWORK is computed as
*>               LWORK >= max(2*M+N,N+LWORK(DGEQP3),N+LWORK(DGEQRF), 7).
*>            -> .. an estimate of the scaled condition number of A is
*>               required (JOBA='E', 'G'). In this case, LWORK is the maximum
*>               of the above and N*N+4*N, i.e. LWORK >= max(2*M+N,N*N+4*N,7).
*>               ->> For optimal performance (blocked code) the optimal value
*>               is LWORK >= max(2*M+N,3*N+(N+1)*NB, N*N+4*N, 7).
*>               In general, the optimal length LWORK is computed as
*>               LWORK >= max(2*M+N,N+LWORK(DGEQP3),N+LWORK(DGEQRF),
*>                                                     N+N*N+LWORK(DPOCON),7).
*>
*>          If SIGMA and the right singular vectors are needed (JOBV.EQ.'V'),
*>            -> the minimal requirement is LWORK >= max(2*M+N,4*N+1,7).
*>            -> For optimal performance, LWORK >= max(2*M+N,3*N+(N+1)*NB,7),
*>               where NB is the optimal block size for DGEQP3, DGEQRF, DGELQ,
*>               DORMLQ. In general, the optimal length LWORK is computed as
*>               LWORK >= max(2*M+N,N+LWORK(DGEQP3), N+LWORK(DPOCON),
*>                       N+LWORK(DGELQ), 2*N+LWORK(DGEQRF), N+LWORK(DORMLQ)).
*>
*>          If SIGMA and the left singular vectors are needed
*>            -> the minimal requirement is LWORK >= max(2*M+N,4*N+1,7).
*>            -> For optimal performance:
*>               if JOBU.EQ.'U' :: LWORK >= max(2*M+N,3*N+(N+1)*NB,7),
*>               if JOBU.EQ.'F' :: LWORK >= max(2*M+N,3*N+(N+1)*NB,N+M*NB,7),
*>               where NB is the optimal block size for DGEQP3, DGEQRF, DORMQR.
*>               In general, the optimal length LWORK is computed as
*>               LWORK >= max(2*M+N,N+LWORK(DGEQP3),N+LWORK(DPOCON),
*>                        2*N+LWORK(DGEQRF), N+LWORK(DORMQR)).
*>               Here LWORK(DORMQR) equals N*NB (for JOBU.EQ.'U') or
*>               M*NB (for JOBU.EQ.'F').
*>
*>          If the full SVD is needed: (JOBU.EQ.'U' or JOBU.EQ.'F') and
*>            -> if JOBV.EQ.'V'
*>               the minimal requirement is LWORK >= max(2*M+N,6*N+2*N*N).
*>            -> if JOBV.EQ.'J' the minimal requirement is
*>               LWORK >= max(2*M+N, 4*N+N*N,2*N+N*N+6).
*>            -> For optimal performance, LWORK should be additionally
*>               larger than N+M*NB, where NB is the optimal block size
*>               for DORMQR.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (M+3*N).
*>          On exit,
*>          IWORK(1) = the numerical rank determined after the initial
*>                     QR factorization with pivoting. See the descriptions
*>                     of JOBA and JOBR.
*>          IWORK(2) = the number of the computed nonzero singular values
*>          IWORK(3) = if nonzero, a warning message:
*>                     If IWORK(3).EQ.1 then some of the column norms of A
*>                     were denormalized floats. The requested high accuracy
*>                     is not warranted by the data.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           < 0  : if INFO = -i, then the i-th argument had an illegal value.
*>           = 0 :  successful exit;
*>           > 0 :  SGEJSV  did not converge in the maximal allowed number
*>                  of sweeps. The computed values may be inaccurate.
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
*> \ingroup realGEsing
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  SGEJSV implements a preconditioned Jacobi SVD algorithm. It uses SGEQP3,
*>  SGEQRF, and SGELQF as preprocessors and preconditioners. Optionally, an
*>  additional row pivoting can be used as a preprocessor, which in some
*>  cases results in much higher accuracy. An example is matrix A with the
*>  structure A = D1 * C * D2, where D1, D2 are arbitrarily ill-conditioned
*>  diagonal matrices and C is well-conditioned matrix. In that case, complete
*>  pivoting in the first QR factorizations provides accuracy dependent on the
*>  condition number of C, and independent of D1, D2. Such higher accuracy is
*>  not completely understood theoretically, but it works well in practice.
*>  Further, if A can be written as A = B*D, with well-conditioned B and some
*>  diagonal D, then the high accuracy is guaranteed, both theoretically and
*>  in software, independent of D. For more details see [1], [2].
*>     The computational range for the singular values can be the full range
*>  ( UNDERFLOW,OVERFLOW ), provided that the machine arithmetic and the BLAS
*>  & LAPACK routines called by SGEJSV are implemented to work in that range.
*>  If that is not the case, then the restriction for safe computation with
*>  the singular values in the range of normalized IEEE numbers is that the
*>  spectral condition number kappa(A)=sigma_max(A)/sigma_min(A) does not
*>  overflow. This code (SGEJSV) is best used in this restricted range,
*>  meaning that singular values of magnitude below ||A||_2 / SLAMCH('O') are
*>  returned as zeros. See JOBR for details on this.
*>     Further, this implementation is somewhat slower than the one described
*>  in [1,2] due to replacement of some non-LAPACK components, and because
*>  the choice of some tuning parameters in the iterative part (SGESVJ) is
*>  left to the implementer on a particular machine.
*>     The rank revealing QR factorization (in this code: SGEQP3) should be
*>  implemented as in [3]. We have a new version of SGEQP3 under development
*>  that is more robust than the current one in LAPACK, with a cleaner cut in
*>  rank deficient cases. It will be available in the SIGMA library [4].
*>  If M is much larger than N, it is obvious that the initial QRF with
*>  column pivoting can be preprocessed by the QRF without pivoting. That
*>  well known trick is not used in SGEJSV because in some cases heavy row
*>  weighting can be treated with complete pivoting. The overhead in cases
*>  M much larger than N is then only due to pivoting, but the benefits in
*>  terms of accuracy have prevailed. The implementer/user can incorporate
*>  this extra QRF step easily. The implementer can also improve data movement
*>  (matrix transpose, matrix copy, matrix transposed copy) - this
*>  implementation of SGEJSV uses only the simplest, naive data movement.
*> \endverbatim
*
*> \par Contributors:
*  ==================
*>
*>  Zlatko Drmac (Zagreb, Croatia) and Kresimir Veselic (Hagen, Germany)
*
*> \par References:
*  ================
*>
*> \verbatim
*>
*> [1] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm I.
*>     SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1322-1342.
*>     LAPACK Working note 169.
*> [2] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm II.
*>     SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1343-1362.
*>     LAPACK Working note 170.
*> [3] Z. Drmac and Z. Bujanovic: On the failure of rank-revealing QR
*>     factorization software - a case study.
*>     ACM Trans. Math. Softw. Vol. 35, No 2 (2008), pp. 1-28.
*>     LAPACK Working note 176.
*> [4] Z. Drmac: SIGMA - mathematical software library for accurate SVD, PSV,
*>     QSVD, (H,K)-SVD computations.
*>     Department of Mathematics, University of Zagreb, 2008.
*> \endverbatim
*
*>  \par Bugs, examples and comments:
*   =================================
*>
*>  Please report all bugs and send interesting examples and/or comments to
*>  drmac@math.hr. Thank you.
*>
*  =====================================================================
      SUBROUTINE SGEJSV( JOBA, JOBU, JOBV, JOBR, JOBT, JOBP,
     $                   M, N, A, LDA, SVA, U, LDU, V, LDV,
     $                   WORK, LWORK, IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      IMPLICIT    NONE
      INTEGER     INFO, LDA, LDU, LDV, LWORK, M, N
*     ..
*     .. Array Arguments ..
      REAL        A( LDA, * ), SVA( N ), U( LDU, * ), V( LDV, * ),
     $            WORK( LWORK )
      INTEGER     IWORK( * )
      CHARACTER*1 JOBA, JOBP, JOBR, JOBT, JOBU, JOBV
*     ..
*
*  ===========================================================================
*
*     .. Local Parameters ..
      REAL        ZERO,         ONE
      PARAMETER ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      REAL    AAPP,   AAQQ,   AATMAX, AATMIN, BIG,    BIG1,   COND_OK,
     $        CONDR1, CONDR2, ENTRA,  ENTRAT, EPSLN,  MAXPRJ, SCALEM,
     $        SCONDA, SFMIN,  SMALL,  TEMP1,  USCAL1, USCAL2, XSC
      INTEGER IERR,   N1,     NR,     NUMRANK,        p, q,   WARNING
      LOGICAL ALMORT, DEFR,   ERREST, GOSCAL, JRACC,  KILL,   LSVEC,
     $        L2ABER, L2KILL, L2PERT, L2RANK, L2TRAN,
     $        NOSCAL, ROWPIV, RSVEC,  TRANSP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC ABS, ALOG, MAX, MIN, FLOAT, NINT, SIGN, SQRT
*     ..
*     .. External Functions ..
      REAL      SLAMCH, SNRM2
      INTEGER   ISAMAX
      LOGICAL   LSAME
      EXTERNAL  ISAMAX, LSAME, SLAMCH, SNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL  SCOPY,  SGELQF, SGEQP3, SGEQRF, SLACPY, SLASCL,
     $          SLASET, SLASSQ, SLASWP, SORGQR, SORMLQ,
     $          SORMQR, SPOCON, SSCAL,  SSWAP,  STRSM,  XERBLA
*
      EXTERNAL  SGESVJ
*     ..
*
*     Test the input arguments
*
      LSVEC  = LSAME( JOBU, 'U' ) .OR. LSAME( JOBU, 'F' )
      JRACC  = LSAME( JOBV, 'J' )
      RSVEC  = LSAME( JOBV, 'V' ) .OR. JRACC
      ROWPIV = LSAME( JOBA, 'F' ) .OR. LSAME( JOBA, 'G' )
      L2RANK = LSAME( JOBA, 'R' )
      L2ABER = LSAME( JOBA, 'A' )
      ERREST = LSAME( JOBA, 'E' ) .OR. LSAME( JOBA, 'G' )
      L2TRAN = LSAME( JOBT, 'T' )
      L2KILL = LSAME( JOBR, 'R' )
      DEFR   = LSAME( JOBR, 'N' )
      L2PERT = LSAME( JOBP, 'P' )
*
      IF ( .NOT.(ROWPIV .OR. L2RANK .OR. L2ABER .OR.
     $     ERREST .OR. LSAME( JOBA, 'C' ) )) THEN
         INFO = - 1
      ELSE IF ( .NOT.( LSVEC  .OR. LSAME( JOBU, 'N' ) .OR.
     $                             LSAME( JOBU, 'W' )) ) THEN
         INFO = - 2
      ELSE IF ( .NOT.( RSVEC .OR. LSAME( JOBV, 'N' ) .OR.
     $   LSAME( JOBV, 'W' )) .OR. ( JRACC .AND. (.NOT.LSVEC) ) ) THEN
         INFO = - 3
      ELSE IF ( .NOT. ( L2KILL .OR. DEFR ) )    THEN
         INFO = - 4
      ELSE IF ( .NOT. ( L2TRAN .OR. LSAME( JOBT, 'N' ) ) ) THEN
         INFO = - 5
      ELSE IF ( .NOT. ( L2PERT .OR. LSAME( JOBP, 'N' ) ) ) THEN
         INFO = - 6
      ELSE IF ( M .LT. 0 ) THEN
         INFO = - 7
      ELSE IF ( ( N .LT. 0 ) .OR. ( N .GT. M ) ) THEN
         INFO = - 8
      ELSE IF ( LDA .LT. M ) THEN
         INFO = - 10
      ELSE IF ( LSVEC .AND. ( LDU .LT. M ) ) THEN
         INFO = - 13
      ELSE IF ( RSVEC .AND. ( LDV .LT. N ) ) THEN
         INFO = - 15
      ELSE IF ( (.NOT.(LSVEC .OR. RSVEC .OR. ERREST).AND.
     $                           (LWORK .LT. MAX(7,4*N+1,2*M+N))) .OR.
     $ (.NOT.(LSVEC .OR. RSVEC) .AND. ERREST .AND.
     $                         (LWORK .LT. MAX(7,4*N+N*N,2*M+N))) .OR.
     $ (LSVEC .AND. (.NOT.RSVEC) .AND. (LWORK .LT. MAX(7,2*M+N,4*N+1)))
     $ .OR.
     $ (RSVEC .AND. (.NOT.LSVEC) .AND. (LWORK .LT. MAX(7,2*M+N,4*N+1)))
     $ .OR.
     $ (LSVEC .AND. RSVEC .AND. (.NOT.JRACC) .AND.
     $                          (LWORK.LT.MAX(2*M+N,6*N+2*N*N)))
     $ .OR. (LSVEC .AND. RSVEC .AND. JRACC .AND.
     $                          LWORK.LT.MAX(2*M+N,4*N+N*N,2*N+N*N+6)))
     $   THEN
         INFO = - 17
      ELSE
*        #:)
         INFO = 0
      END IF
*
      IF ( INFO .NE. 0 ) THEN
*       #:(
         CALL XERBLA( 'SGEJSV', - INFO )
         RETURN
      END IF
*
*     Quick return for void matrix (Y3K safe)
* #:)
      IF ( ( M .EQ. 0 ) .OR. ( N .EQ. 0 ) ) THEN
         IWORK(1:3) = 0
         WORK(1:7) = 0
         RETURN
      ENDIF
*
*     Determine whether the matrix U should be M x N or M x M
*
      IF ( LSVEC ) THEN
         N1 = N
         IF ( LSAME( JOBU, 'F' ) ) N1 = M
      END IF
*
*     Set numerical parameters
*
*!    NOTE: Make sure SLAMCH() does not fail on the target architecture.
*
      EPSLN = SLAMCH('Epsilon')
      SFMIN = SLAMCH('SafeMinimum')
      SMALL = SFMIN / EPSLN
      BIG   = SLAMCH('O')
*     BIG   = ONE / SFMIN
*
*     Initialize SVA(1:N) = diag( ||A e_i||_2 )_1^N
*
*(!)  If necessary, scale SVA() to protect the largest norm from
*     overflow. It is possible that this scaling pushes the smallest
*     column norm left from the underflow threshold (extreme case).
*
      SCALEM  = ONE / SQRT(FLOAT(M)*FLOAT(N))
      NOSCAL  = .TRUE.
      GOSCAL  = .TRUE.
      DO 1874 p = 1, N
         AAPP = ZERO
         AAQQ = ONE
         CALL SLASSQ( M, A(1,p), 1, AAPP, AAQQ )
         IF ( AAPP .GT. BIG ) THEN
            INFO = - 9
            CALL XERBLA( 'SGEJSV', -INFO )
            RETURN
         END IF
         AAQQ = SQRT(AAQQ)
         IF ( ( AAPP .LT. (BIG / AAQQ) ) .AND. NOSCAL  ) THEN
            SVA(p)  = AAPP * AAQQ
         ELSE
            NOSCAL  = .FALSE.
            SVA(p)  = AAPP * ( AAQQ * SCALEM )
            IF ( GOSCAL ) THEN
               GOSCAL = .FALSE.
               CALL SSCAL( p-1, SCALEM, SVA, 1 )
            END IF
         END IF
 1874 CONTINUE
*
      IF ( NOSCAL ) SCALEM = ONE
*
      AAPP = ZERO
      AAQQ = BIG
      DO 4781 p = 1, N
         AAPP = MAX( AAPP, SVA(p) )
         IF ( SVA(p) .NE. ZERO ) AAQQ = MIN( AAQQ, SVA(p) )
 4781 CONTINUE
*
*     Quick return for zero M x N matrix
* #:)
      IF ( AAPP .EQ. ZERO ) THEN
         IF ( LSVEC ) CALL SLASET( 'G', M, N1, ZERO, ONE, U, LDU )
         IF ( RSVEC ) CALL SLASET( 'G', N, N,  ZERO, ONE, V, LDV )
         WORK(1) = ONE
         WORK(2) = ONE
         IF ( ERREST ) WORK(3) = ONE
         IF ( LSVEC .AND. RSVEC ) THEN
            WORK(4) = ONE
            WORK(5) = ONE
         END IF
         IF ( L2TRAN ) THEN
            WORK(6) = ZERO
            WORK(7) = ZERO
         END IF
         IWORK(1) = 0
         IWORK(2) = 0
         IWORK(3) = 0
         RETURN
      END IF
*
*     Issue warning if denormalized column norms detected. Override the
*     high relative accuracy request. Issue licence to kill columns
*     (set them to zero) whose norm is less than sigma_max / BIG (roughly).
* #:(
      WARNING = 0
      IF ( AAQQ .LE. SFMIN ) THEN
         L2RANK = .TRUE.
         L2KILL = .TRUE.
         WARNING = 1
      END IF
*
*     Quick return for one-column matrix
* #:)
      IF ( N .EQ. 1 ) THEN
*
         IF ( LSVEC ) THEN
            CALL SLASCL( 'G',0,0,SVA(1),SCALEM, M,1,A(1,1),LDA,IERR )
            CALL SLACPY( 'A', M, 1, A, LDA, U, LDU )
*           computing all M left singular vectors of the M x 1 matrix
            IF ( N1 .NE. N  ) THEN
               CALL SGEQRF( M, N, U,LDU, WORK, WORK(N+1),LWORK-N,IERR )
               CALL SORGQR( M,N1,1, U,LDU,WORK,WORK(N+1),LWORK-N,IERR )
               CALL SCOPY( M, A(1,1), 1, U(1,1), 1 )
            END IF
         END IF
         IF ( RSVEC ) THEN
             V(1,1) = ONE
         END IF
         IF ( SVA(1) .LT. (BIG*SCALEM) ) THEN
            SVA(1)  = SVA(1) / SCALEM
            SCALEM  = ONE
         END IF
         WORK(1) = ONE / SCALEM
         WORK(2) = ONE
         IF ( SVA(1) .NE. ZERO ) THEN
            IWORK(1) = 1
            IF ( ( SVA(1) / SCALEM) .GE. SFMIN ) THEN
               IWORK(2) = 1
            ELSE
               IWORK(2) = 0
            END IF
         ELSE
            IWORK(1) = 0
            IWORK(2) = 0
         END IF
         IWORK(3) = 0
         IF ( ERREST ) WORK(3) = ONE
         IF ( LSVEC .AND. RSVEC ) THEN
            WORK(4) = ONE
            WORK(5) = ONE
         END IF
         IF ( L2TRAN ) THEN
            WORK(6) = ZERO
            WORK(7) = ZERO
         END IF
         RETURN
*
      END IF
*
      TRANSP = .FALSE.
      L2TRAN = L2TRAN .AND. ( M .EQ. N )
*
      AATMAX = -ONE
      AATMIN =  BIG
      IF ( ROWPIV .OR. L2TRAN ) THEN
*
*     Compute the row norms, needed to determine row pivoting sequence
*     (in the case of heavily row weighted A, row pivoting is strongly
*     advised) and to collect information needed to compare the
*     structures of A * A^t and A^t * A (in the case L2TRAN.EQ..TRUE.).
*
         IF ( L2TRAN ) THEN
            DO 1950 p = 1, M
               XSC   = ZERO
               TEMP1 = ONE
               CALL SLASSQ( N, A(p,1), LDA, XSC, TEMP1 )
*              SLASSQ gets both the ell_2 and the ell_infinity norm
*              in one pass through the vector
               WORK(M+N+p)  = XSC * SCALEM
               WORK(N+p)    = XSC * (SCALEM*SQRT(TEMP1))
               AATMAX = MAX( AATMAX, WORK(N+p) )
               IF (WORK(N+p) .NE. ZERO) AATMIN = MIN(AATMIN,WORK(N+p))
 1950       CONTINUE
         ELSE
            DO 1904 p = 1, M
               WORK(M+N+p) = SCALEM*ABS( A(p,ISAMAX(N,A(p,1),LDA)) )
               AATMAX = MAX( AATMAX, WORK(M+N+p) )
               AATMIN = MIN( AATMIN, WORK(M+N+p) )
 1904       CONTINUE
         END IF
*
      END IF
*
*     For square matrix A try to determine whether A^t  would be  better
*     input for the preconditioned Jacobi SVD, with faster convergence.
*     The decision is based on an O(N) function of the vector of column
*     and row norms of A, based on the Shannon entropy. This should give
*     the right choice in most cases when the difference actually matters.
*     It may fail and pick the slower converging side.
*
      ENTRA  = ZERO
      ENTRAT = ZERO
      IF ( L2TRAN ) THEN
*
         XSC   = ZERO
         TEMP1 = ONE
         CALL SLASSQ( N, SVA, 1, XSC, TEMP1 )
         TEMP1 = ONE / TEMP1
*
         ENTRA = ZERO
         DO 1113 p = 1, N
            BIG1  = ( ( SVA(p) / XSC )**2 ) * TEMP1
            IF ( BIG1 .NE. ZERO ) ENTRA = ENTRA + BIG1 * ALOG(BIG1)
 1113    CONTINUE
         ENTRA = - ENTRA / ALOG(FLOAT(N))
*
*        Now, SVA().^2/Trace(A^t * A) is a point in the probability simplex.
*        It is derived from the diagonal of  A^t * A.  Do the same with the
*        diagonal of A * A^t, compute the entropy of the corresponding
*        probability distribution. Note that A * A^t and A^t * A have the
*        same trace.
*
         ENTRAT = ZERO
         DO 1114 p = N+1, N+M
            BIG1 = ( ( WORK(p) / XSC )**2 ) * TEMP1
            IF ( BIG1 .NE. ZERO ) ENTRAT = ENTRAT + BIG1 * ALOG(BIG1)
 1114    CONTINUE
         ENTRAT = - ENTRAT / ALOG(FLOAT(M))
*
*        Analyze the entropies and decide A or A^t. Smaller entropy
*        usually means better input for the algorithm.
*
         TRANSP = ( ENTRAT .LT. ENTRA )
*
*        If A^t is better than A, transpose A.
*
         IF ( TRANSP ) THEN
*           In an optimal implementation, this trivial transpose
*           should be replaced with faster transpose.
            DO 1115 p = 1, N - 1
               DO 1116 q = p + 1, N
                   TEMP1 = A(q,p)
                  A(q,p) = A(p,q)
                  A(p,q) = TEMP1
 1116          CONTINUE
 1115       CONTINUE
            DO 1117 p = 1, N
               WORK(M+N+p) = SVA(p)
               SVA(p)      = WORK(N+p)
 1117       CONTINUE
            TEMP1  = AAPP
            AAPP   = AATMAX
            AATMAX = TEMP1
            TEMP1  = AAQQ
            AAQQ   = AATMIN
            AATMIN = TEMP1
            KILL   = LSVEC
            LSVEC  = RSVEC
            RSVEC  = KILL
            IF ( LSVEC ) N1 = N
*
            ROWPIV = .TRUE.
         END IF
*
      END IF
*     END IF L2TRAN
*
*     Scale the matrix so that its maximal singular value remains less
*     than SQRT(BIG) -- the matrix is scaled so that its maximal column
*     has Euclidean norm equal to SQRT(BIG/N). The only reason to keep
*     SQRT(BIG) instead of BIG is the fact that SGEJSV uses LAPACK and
*     BLAS routines that, in some implementations, are not capable of
*     working in the full interval [SFMIN,BIG] and that they may provoke
*     overflows in the intermediate results. If the singular values spread
*     from SFMIN to BIG, then SGESVJ will compute them. So, in that case,
*     one should use SGESVJ instead of SGEJSV.
*
      BIG1   = SQRT( BIG )
      TEMP1  = SQRT( BIG / FLOAT(N) )
*
      CALL SLASCL( 'G', 0, 0, AAPP, TEMP1, N, 1, SVA, N, IERR )
      IF ( AAQQ .GT. (AAPP * SFMIN) ) THEN
          AAQQ = ( AAQQ / AAPP ) * TEMP1
      ELSE
          AAQQ = ( AAQQ * TEMP1 ) / AAPP
      END IF
      TEMP1 = TEMP1 * SCALEM
      CALL SLASCL( 'G', 0, 0, AAPP, TEMP1, M, N, A, LDA, IERR )
*
*     To undo scaling at the end of this procedure, multiply the
*     computed singular values with USCAL2 / USCAL1.
*
      USCAL1 = TEMP1
      USCAL2 = AAPP
*
      IF ( L2KILL ) THEN
*        L2KILL enforces computation of nonzero singular values in
*        the restricted range of condition number of the initial A,
*        sigma_max(A) / sigma_min(A) approx. SQRT(BIG)/SQRT(SFMIN).
         XSC = SQRT( SFMIN )
      ELSE
         XSC = SMALL
*
*        Now, if the condition number of A is too big,
*        sigma_max(A) / sigma_min(A) .GT. SQRT(BIG/N) * EPSLN / SFMIN,
*        as a precaution measure, the full SVD is computed using SGESVJ
*        with accumulated Jacobi rotations. This provides numerically
*        more robust computation, at the cost of slightly increased run
*        time. Depending on the concrete implementation of BLAS and LAPACK
*        (i.e. how they behave in presence of extreme ill-conditioning) the
*        implementor may decide to remove this switch.
         IF ( ( AAQQ.LT.SQRT(SFMIN) ) .AND. LSVEC .AND. RSVEC ) THEN
            JRACC = .TRUE.
         END IF
*
      END IF
      IF ( AAQQ .LT. XSC ) THEN
         DO 700 p = 1, N
            IF ( SVA(p) .LT. XSC ) THEN
               CALL SLASET( 'A', M, 1, ZERO, ZERO, A(1,p), LDA )
               SVA(p) = ZERO
            END IF
 700     CONTINUE
      END IF
*
*     Preconditioning using QR factorization with pivoting
*
      IF ( ROWPIV ) THEN
*        Optional row permutation (Bjoerck row pivoting):
*        A result by Cox and Higham shows that the Bjoerck's
*        row pivoting combined with standard column pivoting
*        has similar effect as Powell-Reid complete pivoting.
*        The ell-infinity norms of A are made nonincreasing.
         DO 1952 p = 1, M - 1
            q = ISAMAX( M-p+1, WORK(M+N+p), 1 ) + p - 1
            IWORK(2*N+p) = q
            IF ( p .NE. q ) THEN
               TEMP1       = WORK(M+N+p)
               WORK(M+N+p) = WORK(M+N+q)
               WORK(M+N+q) = TEMP1
            END IF
 1952    CONTINUE
         CALL SLASWP( N, A, LDA, 1, M-1, IWORK(2*N+1), 1 )
      END IF
*
*     End of the preparation phase (scaling, optional sorting and
*     transposing, optional flushing of small columns).
*
*     Preconditioning
*
*     If the full SVD is needed, the right singular vectors are computed
*     from a matrix equation, and for that we need theoretical analysis
*     of the Businger-Golub pivoting. So we use SGEQP3 as the first RR QRF.
*     In all other cases the first RR QRF can be chosen by other criteria
*     (eg speed by replacing global with restricted window pivoting, such
*     as in SGEQPX from TOMS # 782). Good results will be obtained using
*     SGEQPX with properly (!) chosen numerical parameters.
*     Any improvement of SGEQP3 improves overal performance of SGEJSV.
*
*     A * P1 = Q1 * [ R1^t 0]^t:
      DO 1963 p = 1, N
*        .. all columns are free columns
         IWORK(p) = 0
 1963 CONTINUE
      CALL SGEQP3( M,N,A,LDA, IWORK,WORK, WORK(N+1),LWORK-N, IERR )
*
*     The upper triangular matrix R1 from the first QRF is inspected for
*     rank deficiency and possibilities for deflation, or possible
*     ill-conditioning. Depending on the user specified flag L2RANK,
*     the procedure explores possibilities to reduce the numerical
*     rank by inspecting the computed upper triangular factor. If
*     L2RANK or L2ABER are up, then SGEJSV will compute the SVD of
*     A + dA, where ||dA|| <= f(M,N)*EPSLN.
*
      NR = 1
      IF ( L2ABER ) THEN
*        Standard absolute error bound suffices. All sigma_i with
*        sigma_i < N*EPSLN*||A|| are flushed to zero. This is an
*        agressive enforcement of lower numerical rank by introducing a
*        backward error of the order of N*EPSLN*||A||.
         TEMP1 = SQRT(FLOAT(N))*EPSLN
         DO 3001 p = 2, N
            IF ( ABS(A(p,p)) .GE. (TEMP1*ABS(A(1,1))) ) THEN
               NR = NR + 1
            ELSE
               GO TO 3002
            END IF
 3001    CONTINUE
 3002    CONTINUE
      ELSE IF ( L2RANK ) THEN
*        .. similarly as above, only slightly more gentle (less agressive).
*        Sudden drop on the diagonal of R1 is used as the criterion for
*        close-to-rank-deficient.
         TEMP1 = SQRT(SFMIN)
         DO 3401 p = 2, N
            IF ( ( ABS(A(p,p)) .LT. (EPSLN*ABS(A(p-1,p-1))) ) .OR.
     $           ( ABS(A(p,p)) .LT. SMALL ) .OR.
     $           ( L2KILL .AND. (ABS(A(p,p)) .LT. TEMP1) ) ) GO TO 3402
            NR = NR + 1
 3401    CONTINUE
 3402    CONTINUE
*
      ELSE
*        The goal is high relative accuracy. However, if the matrix
*        has high scaled condition number the relative accuracy is in
*        general not feasible. Later on, a condition number estimator
*        will be deployed to estimate the scaled condition number.
*        Here we just remove the underflowed part of the triangular
*        factor. This prevents the situation in which the code is
*        working hard to get the accuracy not warranted by the data.
         TEMP1  = SQRT(SFMIN)
         DO 3301 p = 2, N
            IF ( ( ABS(A(p,p)) .LT. SMALL ) .OR.
     $           ( L2KILL .AND. (ABS(A(p,p)) .LT. TEMP1) ) ) GO TO 3302
            NR = NR + 1
 3301    CONTINUE
 3302    CONTINUE
*
      END IF
*
      ALMORT = .FALSE.
      IF ( NR .EQ. N ) THEN
         MAXPRJ = ONE
         DO 3051 p = 2, N
            TEMP1  = ABS(A(p,p)) / SVA(IWORK(p))
            MAXPRJ = MIN( MAXPRJ, TEMP1 )
 3051    CONTINUE
         IF ( MAXPRJ**2 .GE. ONE - FLOAT(N)*EPSLN ) ALMORT = .TRUE.
      END IF
*
*
      SCONDA = - ONE
      CONDR1 = - ONE
      CONDR2 = - ONE
*
      IF ( ERREST ) THEN
         IF ( N .EQ. NR ) THEN
            IF ( RSVEC ) THEN
*              .. V is available as workspace
               CALL SLACPY( 'U', N, N, A, LDA, V, LDV )
               DO 3053 p = 1, N
                  TEMP1 = SVA(IWORK(p))
                  CALL SSCAL( p, ONE/TEMP1, V(1,p), 1 )
 3053          CONTINUE
               CALL SPOCON( 'U', N, V, LDV, ONE, TEMP1,
     $              WORK(N+1), IWORK(2*N+M+1), IERR )
            ELSE IF ( LSVEC ) THEN
*              .. U is available as workspace
               CALL SLACPY( 'U', N, N, A, LDA, U, LDU )
               DO 3054 p = 1, N
                  TEMP1 = SVA(IWORK(p))
                  CALL SSCAL( p, ONE/TEMP1, U(1,p), 1 )
 3054          CONTINUE
               CALL SPOCON( 'U', N, U, LDU, ONE, TEMP1,
     $              WORK(N+1), IWORK(2*N+M+1), IERR )
            ELSE
               CALL SLACPY( 'U', N, N, A, LDA, WORK(N+1), N )
               DO 3052 p = 1, N
                  TEMP1 = SVA(IWORK(p))
                  CALL SSCAL( p, ONE/TEMP1, WORK(N+(p-1)*N+1), 1 )
 3052          CONTINUE
*           .. the columns of R are scaled to have unit Euclidean lengths.
               CALL SPOCON( 'U', N, WORK(N+1), N, ONE, TEMP1,
     $              WORK(N+N*N+1), IWORK(2*N+M+1), IERR )
            END IF
            SCONDA = ONE / SQRT(TEMP1)
*           SCONDA is an estimate of SQRT(||(R^t * R)^(-1)||_1).
*           N^(-1/4) * SCONDA <= ||R^(-1)||_2 <= N^(1/4) * SCONDA
         ELSE
            SCONDA = - ONE
         END IF
      END IF
*
      L2PERT = L2PERT .AND. ( ABS( A(1,1)/A(NR,NR) ) .GT. SQRT(BIG1) )
*     If there is no violent scaling, artificial perturbation is not needed.
*
*     Phase 3:
*
      IF ( .NOT. ( RSVEC .OR. LSVEC ) ) THEN
*
*         Singular Values only
*
*         .. transpose A(1:NR,1:N)
         DO 1946 p = 1, MIN( N-1, NR )
            CALL SCOPY( N-p, A(p,p+1), LDA, A(p+1,p), 1 )
 1946    CONTINUE
*
*        The following two DO-loops introduce small relative perturbation
*        into the strict upper triangle of the lower triangular matrix.
*        Small entries below the main diagonal are also changed.
*        This modification is useful if the computing environment does not
*        provide/allow FLUSH TO ZERO underflow, for it prevents many
*        annoying denormalized numbers in case of strongly scaled matrices.
*        The perturbation is structured so that it does not introduce any
*        new perturbation of the singular values, and it does not destroy
*        the job done by the preconditioner.
*        The licence for this perturbation is in the variable L2PERT, which
*        should be .FALSE. if FLUSH TO ZERO underflow is active.
*
         IF ( .NOT. ALMORT ) THEN
*
            IF ( L2PERT ) THEN
*              XSC = SQRT(SMALL)
               XSC = EPSLN / FLOAT(N)
               DO 4947 q = 1, NR
                  TEMP1 = XSC*ABS(A(q,q))
                  DO 4949 p = 1, N
                     IF ( ( (p.GT.q) .AND. (ABS(A(p,q)).LE.TEMP1) )
     $                    .OR. ( p .LT. q ) )
     $                     A(p,q) = SIGN( TEMP1, A(p,q) )
 4949             CONTINUE
 4947          CONTINUE
            ELSE
               CALL SLASET( 'U', NR-1,NR-1, ZERO,ZERO, A(1,2),LDA )
            END IF
*
*            .. second preconditioning using the QR factorization
*
            CALL SGEQRF( N,NR, A,LDA, WORK, WORK(N+1),LWORK-N, IERR )
*
*           .. and transpose upper to lower triangular
            DO 1948 p = 1, NR - 1
               CALL SCOPY( NR-p, A(p,p+1), LDA, A(p+1,p), 1 )
 1948       CONTINUE
*
         END IF
*
*           Row-cyclic Jacobi SVD algorithm with column pivoting
*
*           .. again some perturbation (a "background noise") is added
*           to drown denormals
            IF ( L2PERT ) THEN
*              XSC = SQRT(SMALL)
               XSC = EPSLN / FLOAT(N)
               DO 1947 q = 1, NR
                  TEMP1 = XSC*ABS(A(q,q))
                  DO 1949 p = 1, NR
                     IF ( ( (p.GT.q) .AND. (ABS(A(p,q)).LE.TEMP1) )
     $                       .OR. ( p .LT. q ) )
     $                   A(p,q) = SIGN( TEMP1, A(p,q) )
 1949             CONTINUE
 1947          CONTINUE
            ELSE
               CALL SLASET( 'U', NR-1, NR-1, ZERO, ZERO, A(1,2), LDA )
            END IF
*
*           .. and one-sided Jacobi rotations are started on a lower
*           triangular matrix (plus perturbation which is ignored in
*           the part which destroys triangular form (confusing?!))
*
            CALL SGESVJ( 'L', 'NoU', 'NoV', NR, NR, A, LDA, SVA,
     $                      N, V, LDV, WORK, LWORK, INFO )
*
            SCALEM  = WORK(1)
            NUMRANK = NINT(WORK(2))
*
*
      ELSE IF ( RSVEC .AND. ( .NOT. LSVEC ) ) THEN
*
*        -> Singular Values and Right Singular Vectors <-
*
         IF ( ALMORT ) THEN
*
*           .. in this case NR equals N
            DO 1998 p = 1, NR
               CALL SCOPY( N-p+1, A(p,p), LDA, V(p,p), 1 )
 1998       CONTINUE
            CALL SLASET( 'Upper', NR-1, NR-1, ZERO, ZERO, V(1,2), LDV )
*
            CALL SGESVJ( 'L','U','N', N, NR, V,LDV, SVA, NR, A,LDA,
     $                  WORK, LWORK, INFO )
            SCALEM  = WORK(1)
            NUMRANK = NINT(WORK(2))

         ELSE
*
*        .. two more QR factorizations ( one QRF is not enough, two require
*        accumulated product of Jacobi rotations, three are perfect )
*
            CALL SLASET( 'Lower', NR-1, NR-1, ZERO, ZERO, A(2,1), LDA )
            CALL SGELQF( NR, N, A, LDA, WORK, WORK(N+1), LWORK-N, IERR)
            CALL SLACPY( 'Lower', NR, NR, A, LDA, V, LDV )
            CALL SLASET( 'Upper', NR-1, NR-1, ZERO, ZERO, V(1,2), LDV )
            CALL SGEQRF( NR, NR, V, LDV, WORK(N+1), WORK(2*N+1),
     $                   LWORK-2*N, IERR )
            DO 8998 p = 1, NR
               CALL SCOPY( NR-p+1, V(p,p), LDV, V(p,p), 1 )
 8998       CONTINUE
            CALL SLASET( 'Upper', NR-1, NR-1, ZERO, ZERO, V(1,2), LDV )
*
            CALL SGESVJ( 'Lower', 'U','N', NR, NR, V,LDV, SVA, NR, U,
     $                  LDU, WORK(N+1), LWORK-N, INFO )
            SCALEM  = WORK(N+1)
            NUMRANK = NINT(WORK(N+2))
            IF ( NR .LT. N ) THEN
               CALL SLASET( 'A',N-NR, NR, ZERO,ZERO, V(NR+1,1),   LDV )
               CALL SLASET( 'A',NR, N-NR, ZERO,ZERO, V(1,NR+1),   LDV )
               CALL SLASET( 'A',N-NR,N-NR,ZERO,ONE, V(NR+1,NR+1), LDV )
            END IF
*
         CALL SORMLQ( 'Left', 'Transpose', N, N, NR, A, LDA, WORK,
     $               V, LDV, WORK(N+1), LWORK-N, IERR )
*
         END IF
*
         DO 8991 p = 1, N
            CALL SCOPY( N, V(p,1), LDV, A(IWORK(p),1), LDA )
 8991    CONTINUE
         CALL SLACPY( 'All', N, N, A, LDA, V, LDV )
*
         IF ( TRANSP ) THEN
            CALL SLACPY( 'All', N, N, V, LDV, U, LDU )
         END IF
*
      ELSE IF ( LSVEC .AND. ( .NOT. RSVEC ) ) THEN
*
*        .. Singular Values and Left Singular Vectors                 ..
*
*        .. second preconditioning step to avoid need to accumulate
*        Jacobi rotations in the Jacobi iterations.
         DO 1965 p = 1, NR
            CALL SCOPY( N-p+1, A(p,p), LDA, U(p,p), 1 )
 1965    CONTINUE
         CALL SLASET( 'Upper', NR-1, NR-1, ZERO, ZERO, U(1,2), LDU )
*
         CALL SGEQRF( N, NR, U, LDU, WORK(N+1), WORK(2*N+1),
     $              LWORK-2*N, IERR )
*
         DO 1967 p = 1, NR - 1
            CALL SCOPY( NR-p, U(p,p+1), LDU, U(p+1,p), 1 )
 1967    CONTINUE
         CALL SLASET( 'Upper', NR-1, NR-1, ZERO, ZERO, U(1,2), LDU )
*
         CALL SGESVJ( 'Lower', 'U', 'N', NR,NR, U, LDU, SVA, NR, A,
     $        LDA, WORK(N+1), LWORK-N, INFO )
         SCALEM  = WORK(N+1)
         NUMRANK = NINT(WORK(N+2))
*
         IF ( NR .LT. M ) THEN
            CALL SLASET( 'A',  M-NR, NR,ZERO, ZERO, U(NR+1,1), LDU )
            IF ( NR .LT. N1 ) THEN
               CALL SLASET( 'A',NR, N1-NR, ZERO, ZERO, U(1,NR+1), LDU )
               CALL SLASET( 'A',M-NR,N1-NR,ZERO,ONE,U(NR+1,NR+1), LDU )
            END IF
         END IF
*
         CALL SORMQR( 'Left', 'No Tr', M, N1, N, A, LDA, WORK, U,
     $               LDU, WORK(N+1), LWORK-N, IERR )
*
         IF ( ROWPIV )
     $       CALL SLASWP( N1, U, LDU, 1, M-1, IWORK(2*N+1), -1 )
*
         DO 1974 p = 1, N1
            XSC = ONE / SNRM2( M, U(1,p), 1 )
            CALL SSCAL( M, XSC, U(1,p), 1 )
 1974    CONTINUE
*
         IF ( TRANSP ) THEN
            CALL SLACPY( 'All', N, N, U, LDU, V, LDV )
         END IF
*
      ELSE
*
*        .. Full SVD ..
*
         IF ( .NOT. JRACC ) THEN
*
         IF ( .NOT. ALMORT ) THEN
*
*           Second Preconditioning Step (QRF [with pivoting])
*           Note that the composition of TRANSPOSE, QRF and TRANSPOSE is
*           equivalent to an LQF CALL. Since in many libraries the QRF
*           seems to be better optimized than the LQF, we do explicit
*           transpose and use the QRF. This is subject to changes in an
*           optimized implementation of SGEJSV.
*
            DO 1968 p = 1, NR
               CALL SCOPY( N-p+1, A(p,p), LDA, V(p,p), 1 )
 1968       CONTINUE
*
*           .. the following two loops perturb small entries to avoid
*           denormals in the second QR factorization, where they are
*           as good as zeros. This is done to avoid painfully slow
*           computation with denormals. The relative size of the perturbation
*           is a parameter that can be changed by the implementer.
*           This perturbation device will be obsolete on machines with
*           properly implemented arithmetic.
*           To switch it off, set L2PERT=.FALSE. To remove it from  the
*           code, remove the action under L2PERT=.TRUE., leave the ELSE part.
*           The following two loops should be blocked and fused with the
*           transposed copy above.
*
            IF ( L2PERT ) THEN
               XSC = SQRT(SMALL)
               DO 2969 q = 1, NR
                  TEMP1 = XSC*ABS( V(q,q) )
                  DO 2968 p = 1, N
                     IF ( ( p .GT. q ) .AND. ( ABS(V(p,q)) .LE. TEMP1 )
     $                   .OR. ( p .LT. q ) )
     $                   V(p,q) = SIGN( TEMP1, V(p,q) )
                     IF ( p .LT. q ) V(p,q) = - V(p,q)
 2968             CONTINUE
 2969          CONTINUE
            ELSE
               CALL SLASET( 'U', NR-1, NR-1, ZERO, ZERO, V(1,2), LDV )
            END IF
*
*           Estimate the row scaled condition number of R1
*           (If R1 is rectangular, N > NR, then the condition number
*           of the leading NR x NR submatrix is estimated.)
*
            CALL SLACPY( 'L', NR, NR, V, LDV, WORK(2*N+1), NR )
            DO 3950 p = 1, NR
               TEMP1 = SNRM2(NR-p+1,WORK(2*N+(p-1)*NR+p),1)
               CALL SSCAL(NR-p+1,ONE/TEMP1,WORK(2*N+(p-1)*NR+p),1)
 3950       CONTINUE
            CALL SPOCON('Lower',NR,WORK(2*N+1),NR,ONE,TEMP1,
     $                   WORK(2*N+NR*NR+1),IWORK(M+2*N+1),IERR)
            CONDR1 = ONE / SQRT(TEMP1)
*           .. here need a second oppinion on the condition number
*           .. then assume worst case scenario
*           R1 is OK for inverse <=> CONDR1 .LT. FLOAT(N)
*           more conservative    <=> CONDR1 .LT. SQRT(FLOAT(N))
*
            COND_OK = SQRT(FLOAT(NR))
*[TP]       COND_OK is a tuning parameter.

            IF ( CONDR1 .LT. COND_OK ) THEN
*              .. the second QRF without pivoting. Note: in an optimized
*              implementation, this QRF should be implemented as the QRF
*              of a lower triangular matrix.
*              R1^t = Q2 * R2
               CALL SGEQRF( N, NR, V, LDV, WORK(N+1), WORK(2*N+1),
     $              LWORK-2*N, IERR )
*
               IF ( L2PERT ) THEN
                  XSC = SQRT(SMALL)/EPSLN
                  DO 3959 p = 2, NR
                     DO 3958 q = 1, p - 1
                        TEMP1 = XSC * MIN(ABS(V(p,p)),ABS(V(q,q)))
                        IF ( ABS(V(q,p)) .LE. TEMP1 )
     $                     V(q,p) = SIGN( TEMP1, V(q,p) )
 3958                CONTINUE
 3959             CONTINUE
               END IF
*
               IF ( NR .NE. N )
     $         CALL SLACPY( 'A', N, NR, V, LDV, WORK(2*N+1), N )
*              .. save ...
*
*           .. this transposed copy should be better than naive
               DO 1969 p = 1, NR - 1
                  CALL SCOPY( NR-p, V(p,p+1), LDV, V(p+1,p), 1 )
 1969          CONTINUE
*
               CONDR2 = CONDR1
*
            ELSE
*
*              .. ill-conditioned case: second QRF with pivoting
*              Note that windowed pivoting would be equaly good
*              numerically, and more run-time efficient. So, in
*              an optimal implementation, the next call to SGEQP3
*              should be replaced with eg. CALL SGEQPX (ACM TOMS #782)
*              with properly (carefully) chosen parameters.
*
*              R1^t * P2 = Q2 * R2
               DO 3003 p = 1, NR
                  IWORK(N+p) = 0
 3003          CONTINUE
               CALL SGEQP3( N, NR, V, LDV, IWORK(N+1), WORK(N+1),
     $                  WORK(2*N+1), LWORK-2*N, IERR )
**               CALL SGEQRF( N, NR, V, LDV, WORK(N+1), WORK(2*N+1),
**     $              LWORK-2*N, IERR )
               IF ( L2PERT ) THEN
                  XSC = SQRT(SMALL)
                  DO 3969 p = 2, NR
                     DO 3968 q = 1, p - 1
                        TEMP1 = XSC * MIN(ABS(V(p,p)),ABS(V(q,q)))
                        IF ( ABS(V(q,p)) .LE. TEMP1 )
     $                     V(q,p) = SIGN( TEMP1, V(q,p) )
 3968                CONTINUE
 3969             CONTINUE
               END IF
*
               CALL SLACPY( 'A', N, NR, V, LDV, WORK(2*N+1), N )
*
               IF ( L2PERT ) THEN
                  XSC = SQRT(SMALL)
                  DO 8970 p = 2, NR
                     DO 8971 q = 1, p - 1
                        TEMP1 = XSC * MIN(ABS(V(p,p)),ABS(V(q,q)))
                        V(p,q) = - SIGN( TEMP1, V(q,p) )
 8971                CONTINUE
 8970             CONTINUE
               ELSE
                  CALL SLASET( 'L',NR-1,NR-1,ZERO,ZERO,V(2,1),LDV )
               END IF
*              Now, compute R2 = L3 * Q3, the LQ factorization.
               CALL SGELQF( NR, NR, V, LDV, WORK(2*N+N*NR+1),
     $               WORK(2*N+N*NR+NR+1), LWORK-2*N-N*NR-NR, IERR )
*              .. and estimate the condition number
               CALL SLACPY( 'L',NR,NR,V,LDV,WORK(2*N+N*NR+NR+1),NR )
               DO 4950 p = 1, NR
                  TEMP1 = SNRM2( p, WORK(2*N+N*NR+NR+p), NR )
                  CALL SSCAL( p, ONE/TEMP1, WORK(2*N+N*NR+NR+p), NR )
 4950          CONTINUE
               CALL SPOCON( 'L',NR,WORK(2*N+N*NR+NR+1),NR,ONE,TEMP1,
     $              WORK(2*N+N*NR+NR+NR*NR+1),IWORK(M+2*N+1),IERR )
               CONDR2 = ONE / SQRT(TEMP1)
*
               IF ( CONDR2 .GE. COND_OK ) THEN
*                 .. save the Householder vectors used for Q3
*                 (this overwrittes the copy of R2, as it will not be
*                 needed in this branch, but it does not overwritte the
*                 Huseholder vectors of Q2.).
                  CALL SLACPY( 'U', NR, NR, V, LDV, WORK(2*N+1), N )
*                 .. and the rest of the information on Q3 is in
*                 WORK(2*N+N*NR+1:2*N+N*NR+N)
               END IF
*
            END IF
*
            IF ( L2PERT ) THEN
               XSC = SQRT(SMALL)
               DO 4968 q = 2, NR
                  TEMP1 = XSC * V(q,q)
                  DO 4969 p = 1, q - 1
*                    V(p,q) = - SIGN( TEMP1, V(q,p) )
                     V(p,q) = - SIGN( TEMP1, V(p,q) )
 4969             CONTINUE
 4968          CONTINUE
            ELSE
               CALL SLASET( 'U', NR-1,NR-1, ZERO,ZERO, V(1,2), LDV )
            END IF
*
*        Second preconditioning finished; continue with Jacobi SVD
*        The input matrix is lower trinagular.
*
*        Recover the right singular vectors as solution of a well
*        conditioned triangular matrix equation.
*
            IF ( CONDR1 .LT. COND_OK ) THEN
*
               CALL SGESVJ( 'L','U','N',NR,NR,V,LDV,SVA,NR,U,
     $              LDU,WORK(2*N+N*NR+NR+1),LWORK-2*N-N*NR-NR,INFO )
               SCALEM  = WORK(2*N+N*NR+NR+1)
               NUMRANK = NINT(WORK(2*N+N*NR+NR+2))
               DO 3970 p = 1, NR
                  CALL SCOPY( NR, V(1,p), 1, U(1,p), 1 )
                  CALL SSCAL( NR, SVA(p),    V(1,p), 1 )
 3970          CONTINUE

*        .. pick the right matrix equation and solve it
*
               IF ( NR .EQ. N ) THEN
* :))             .. best case, R1 is inverted. The solution of this matrix
*                 equation is Q2*V2 = the product of the Jacobi rotations
*                 used in SGESVJ, premultiplied with the orthogonal matrix
*                 from the second QR factorization.
                  CALL STRSM( 'L','U','N','N', NR,NR,ONE, A,LDA, V,LDV )
               ELSE
*                 .. R1 is well conditioned, but non-square. Transpose(R2)
*                 is inverted to get the product of the Jacobi rotations
*                 used in SGESVJ. The Q-factor from the second QR
*                 factorization is then built in explicitly.
                  CALL STRSM('L','U','T','N',NR,NR,ONE,WORK(2*N+1),
     $                 N,V,LDV)
                  IF ( NR .LT. N ) THEN
                    CALL SLASET('A',N-NR,NR,ZERO,ZERO,V(NR+1,1),LDV)
                    CALL SLASET('A',NR,N-NR,ZERO,ZERO,V(1,NR+1),LDV)
                    CALL SLASET('A',N-NR,N-NR,ZERO,ONE,V(NR+1,NR+1),LDV)
                  END IF
                  CALL SORMQR('L','N',N,N,NR,WORK(2*N+1),N,WORK(N+1),
     $                 V,LDV,WORK(2*N+N*NR+NR+1),LWORK-2*N-N*NR-NR,IERR)
               END IF
*
            ELSE IF ( CONDR2 .LT. COND_OK ) THEN
*
* :)           .. the input matrix A is very likely a relative of
*              the Kahan matrix :)
*              The matrix R2 is inverted. The solution of the matrix equation
*              is Q3^T*V3 = the product of the Jacobi rotations (appplied to
*              the lower triangular L3 from the LQ factorization of
*              R2=L3*Q3), pre-multiplied with the transposed Q3.
               CALL SGESVJ( 'L', 'U', 'N', NR, NR, V, LDV, SVA, NR, U,
     $              LDU, WORK(2*N+N*NR+NR+1), LWORK-2*N-N*NR-NR, INFO )
               SCALEM  = WORK(2*N+N*NR+NR+1)
               NUMRANK = NINT(WORK(2*N+N*NR+NR+2))
               DO 3870 p = 1, NR
                  CALL SCOPY( NR, V(1,p), 1, U(1,p), 1 )
                  CALL SSCAL( NR, SVA(p),    U(1,p), 1 )
 3870          CONTINUE
               CALL STRSM('L','U','N','N',NR,NR,ONE,WORK(2*N+1),N,U,LDU)
*              .. apply the permutation from the second QR factorization
               DO 873 q = 1, NR
                  DO 872 p = 1, NR
                     WORK(2*N+N*NR+NR+IWORK(N+p)) = U(p,q)
 872              CONTINUE
                  DO 874 p = 1, NR
                     U(p,q) = WORK(2*N+N*NR+NR+p)
 874              CONTINUE
 873           CONTINUE
               IF ( NR .LT. N ) THEN
                  CALL SLASET( 'A',N-NR,NR,ZERO,ZERO,V(NR+1,1),LDV )
                  CALL SLASET( 'A',NR,N-NR,ZERO,ZERO,V(1,NR+1),LDV )
                  CALL SLASET( 'A',N-NR,N-NR,ZERO,ONE,V(NR+1,NR+1),LDV )
               END IF
               CALL SORMQR( 'L','N',N,N,NR,WORK(2*N+1),N,WORK(N+1),
     $              V,LDV,WORK(2*N+N*NR+NR+1),LWORK-2*N-N*NR-NR,IERR )
            ELSE
*              Last line of defense.
* #:(          This is a rather pathological case: no scaled condition
*              improvement after two pivoted QR factorizations. Other
*              possibility is that the rank revealing QR factorization
*              or the condition estimator has failed, or the COND_OK
*              is set very close to ONE (which is unnecessary). Normally,
*              this branch should never be executed, but in rare cases of
*              failure of the RRQR or condition estimator, the last line of
*              defense ensures that SGEJSV completes the task.
*              Compute the full SVD of L3 using SGESVJ with explicit
*              accumulation of Jacobi rotations.
               CALL SGESVJ( 'L', 'U', 'V', NR, NR, V, LDV, SVA, NR, U,
     $              LDU, WORK(2*N+N*NR+NR+1), LWORK-2*N-N*NR-NR, INFO )
               SCALEM  = WORK(2*N+N*NR+NR+1)
               NUMRANK = NINT(WORK(2*N+N*NR+NR+2))
               IF ( NR .LT. N ) THEN
                  CALL SLASET( 'A',N-NR,NR,ZERO,ZERO,V(NR+1,1),LDV )
                  CALL SLASET( 'A',NR,N-NR,ZERO,ZERO,V(1,NR+1),LDV )
                  CALL SLASET( 'A',N-NR,N-NR,ZERO,ONE,V(NR+1,NR+1),LDV )
               END IF
               CALL SORMQR( 'L','N',N,N,NR,WORK(2*N+1),N,WORK(N+1),
     $              V,LDV,WORK(2*N+N*NR+NR+1),LWORK-2*N-N*NR-NR,IERR )
*
               CALL SORMLQ( 'L', 'T', NR, NR, NR, WORK(2*N+1), N,
     $              WORK(2*N+N*NR+1), U, LDU, WORK(2*N+N*NR+NR+1),
     $              LWORK-2*N-N*NR-NR, IERR )
               DO 773 q = 1, NR
                  DO 772 p = 1, NR
                     WORK(2*N+N*NR+NR+IWORK(N+p)) = U(p,q)
 772              CONTINUE
                  DO 774 p = 1, NR
                     U(p,q) = WORK(2*N+N*NR+NR+p)
 774              CONTINUE
 773           CONTINUE
*
            END IF
*
*           Permute the rows of V using the (column) permutation from the
*           first QRF. Also, scale the columns to make them unit in
*           Euclidean norm. This applies to all cases.
*
            TEMP1 = SQRT(FLOAT(N)) * EPSLN
            DO 1972 q = 1, N
               DO 972 p = 1, N
                  WORK(2*N+N*NR+NR+IWORK(p)) = V(p,q)
  972          CONTINUE
               DO 973 p = 1, N
                  V(p,q) = WORK(2*N+N*NR+NR+p)
  973          CONTINUE
               XSC = ONE / SNRM2( N, V(1,q), 1 )
               IF ( (XSC .LT. (ONE-TEMP1)) .OR. (XSC .GT. (ONE+TEMP1)) )
     $           CALL SSCAL( N, XSC, V(1,q), 1 )
 1972       CONTINUE
*           At this moment, V contains the right singular vectors of A.
*           Next, assemble the left singular vector matrix U (M x N).
            IF ( NR .LT. M ) THEN
               CALL SLASET( 'A', M-NR, NR, ZERO, ZERO, U(NR+1,1), LDU )
               IF ( NR .LT. N1 ) THEN
                  CALL SLASET('A',NR,N1-NR,ZERO,ZERO,U(1,NR+1),LDU)
                  CALL SLASET('A',M-NR,N1-NR,ZERO,ONE,U(NR+1,NR+1),LDU)
               END IF
            END IF
*
*           The Q matrix from the first QRF is built into the left singular
*           matrix U. This applies to all cases.
*
            CALL SORMQR( 'Left', 'No_Tr', M, N1, N, A, LDA, WORK, U,
     $           LDU, WORK(N+1), LWORK-N, IERR )

*           The columns of U are normalized. The cost is O(M*N) flops.
            TEMP1 = SQRT(FLOAT(M)) * EPSLN
            DO 1973 p = 1, NR
               XSC = ONE / SNRM2( M, U(1,p), 1 )
               IF ( (XSC .LT. (ONE-TEMP1)) .OR. (XSC .GT. (ONE+TEMP1)) )
     $          CALL SSCAL( M, XSC, U(1,p), 1 )
 1973       CONTINUE
*
*           If the initial QRF is computed with row pivoting, the left
*           singular vectors must be adjusted.
*
            IF ( ROWPIV )
     $          CALL SLASWP( N1, U, LDU, 1, M-1, IWORK(2*N+1), -1 )
*
         ELSE
*
*        .. the initial matrix A has almost orthogonal columns and
*        the second QRF is not needed
*
            CALL SLACPY( 'Upper', N, N, A, LDA, WORK(N+1), N )
            IF ( L2PERT ) THEN
               XSC = SQRT(SMALL)
               DO 5970 p = 2, N
                  TEMP1 = XSC * WORK( N + (p-1)*N + p )
                  DO 5971 q = 1, p - 1
                     WORK(N+(q-1)*N+p)=-SIGN(TEMP1,WORK(N+(p-1)*N+q))
 5971             CONTINUE
 5970          CONTINUE
            ELSE
               CALL SLASET( 'Lower',N-1,N-1,ZERO,ZERO,WORK(N+2),N )
            END IF
*
            CALL SGESVJ( 'Upper', 'U', 'N', N, N, WORK(N+1), N, SVA,
     $           N, U, LDU, WORK(N+N*N+1), LWORK-N-N*N, INFO )
*
            SCALEM  = WORK(N+N*N+1)
            NUMRANK = NINT(WORK(N+N*N+2))
            DO 6970 p = 1, N
               CALL SCOPY( N, WORK(N+(p-1)*N+1), 1, U(1,p), 1 )
               CALL SSCAL( N, SVA(p), WORK(N+(p-1)*N+1), 1 )
 6970       CONTINUE
*
            CALL STRSM( 'Left', 'Upper', 'NoTrans', 'No UD', N, N,
     $           ONE, A, LDA, WORK(N+1), N )
            DO 6972 p = 1, N
               CALL SCOPY( N, WORK(N+p), N, V(IWORK(p),1), LDV )
 6972       CONTINUE
            TEMP1 = SQRT(FLOAT(N))*EPSLN
            DO 6971 p = 1, N
               XSC = ONE / SNRM2( N, V(1,p), 1 )
               IF ( (XSC .LT. (ONE-TEMP1)) .OR. (XSC .GT. (ONE+TEMP1)) )
     $            CALL SSCAL( N, XSC, V(1,p), 1 )
 6971       CONTINUE
*
*           Assemble the left singular vector matrix U (M x N).
*
            IF ( N .LT. M ) THEN
               CALL SLASET( 'A',  M-N, N, ZERO, ZERO, U(N+1,1), LDU )
               IF ( N .LT. N1 ) THEN
                  CALL SLASET( 'A',N,  N1-N, ZERO, ZERO,  U(1,N+1),LDU )
                  CALL SLASET( 'A',M-N,N1-N, ZERO, ONE,U(N+1,N+1),LDU )
               END IF
            END IF
            CALL SORMQR( 'Left', 'No Tr', M, N1, N, A, LDA, WORK, U,
     $           LDU, WORK(N+1), LWORK-N, IERR )
            TEMP1 = SQRT(FLOAT(M))*EPSLN
            DO 6973 p = 1, N1
               XSC = ONE / SNRM2( M, U(1,p), 1 )
               IF ( (XSC .LT. (ONE-TEMP1)) .OR. (XSC .GT. (ONE+TEMP1)) )
     $            CALL SSCAL( M, XSC, U(1,p), 1 )
 6973       CONTINUE
*
            IF ( ROWPIV )
     $         CALL SLASWP( N1, U, LDU, 1, M-1, IWORK(2*N+1), -1 )
*
         END IF
*
*        end of the  >> almost orthogonal case <<  in the full SVD
*
         ELSE
*
*        This branch deploys a preconditioned Jacobi SVD with explicitly
*        accumulated rotations. It is included as optional, mainly for
*        experimental purposes. It does perfom well, and can also be used.
*        In this implementation, this branch will be automatically activated
*        if the  condition number sigma_max(A) / sigma_min(A) is predicted
*        to be greater than the overflow threshold. This is because the
*        a posteriori computation of the singular vectors assumes robust
*        implementation of BLAS and some LAPACK procedures, capable of working
*        in presence of extreme values. Since that is not always the case, ...
*
         DO 7968 p = 1, NR
            CALL SCOPY( N-p+1, A(p,p), LDA, V(p,p), 1 )
 7968    CONTINUE
*
         IF ( L2PERT ) THEN
            XSC = SQRT(SMALL/EPSLN)
            DO 5969 q = 1, NR
               TEMP1 = XSC*ABS( V(q,q) )
               DO 5968 p = 1, N
                  IF ( ( p .GT. q ) .AND. ( ABS(V(p,q)) .LE. TEMP1 )
     $                .OR. ( p .LT. q ) )
     $                V(p,q) = SIGN( TEMP1, V(p,q) )
                  IF ( p .LT. q ) V(p,q) = - V(p,q)
 5968          CONTINUE
 5969       CONTINUE
         ELSE
            CALL SLASET( 'U', NR-1, NR-1, ZERO, ZERO, V(1,2), LDV )
         END IF

         CALL SGEQRF( N, NR, V, LDV, WORK(N+1), WORK(2*N+1),
     $        LWORK-2*N, IERR )
         CALL SLACPY( 'L', N, NR, V, LDV, WORK(2*N+1), N )
*
         DO 7969 p = 1, NR
            CALL SCOPY( NR-p+1, V(p,p), LDV, U(p,p), 1 )
 7969    CONTINUE

         IF ( L2PERT ) THEN
            XSC = SQRT(SMALL/EPSLN)
            DO 9970 q = 2, NR
               DO 9971 p = 1, q - 1
                  TEMP1 = XSC * MIN(ABS(U(p,p)),ABS(U(q,q)))
                  U(p,q) = - SIGN( TEMP1, U(q,p) )
 9971          CONTINUE
 9970       CONTINUE
         ELSE
            CALL SLASET('U', NR-1, NR-1, ZERO, ZERO, U(1,2), LDU )
         END IF

         CALL SGESVJ( 'L', 'U', 'V', NR, NR, U, LDU, SVA,
     $        N, V, LDV, WORK(2*N+N*NR+1), LWORK-2*N-N*NR, INFO )
         SCALEM  = WORK(2*N+N*NR+1)
         NUMRANK = NINT(WORK(2*N+N*NR+2))

         IF ( NR .LT. N ) THEN
            CALL SLASET( 'A',N-NR,NR,ZERO,ZERO,V(NR+1,1),LDV )
            CALL SLASET( 'A',NR,N-NR,ZERO,ZERO,V(1,NR+1),LDV )
            CALL SLASET( 'A',N-NR,N-NR,ZERO,ONE,V(NR+1,NR+1),LDV )
         END IF

         CALL SORMQR( 'L','N',N,N,NR,WORK(2*N+1),N,WORK(N+1),
     $        V,LDV,WORK(2*N+N*NR+NR+1),LWORK-2*N-N*NR-NR,IERR )
*
*           Permute the rows of V using the (column) permutation from the
*           first QRF. Also, scale the columns to make them unit in
*           Euclidean norm. This applies to all cases.
*
            TEMP1 = SQRT(FLOAT(N)) * EPSLN
            DO 7972 q = 1, N
               DO 8972 p = 1, N
                  WORK(2*N+N*NR+NR+IWORK(p)) = V(p,q)
 8972          CONTINUE
               DO 8973 p = 1, N
                  V(p,q) = WORK(2*N+N*NR+NR+p)
 8973          CONTINUE
               XSC = ONE / SNRM2( N, V(1,q), 1 )
               IF ( (XSC .LT. (ONE-TEMP1)) .OR. (XSC .GT. (ONE+TEMP1)) )
     $           CALL SSCAL( N, XSC, V(1,q), 1 )
 7972       CONTINUE
*
*           At this moment, V contains the right singular vectors of A.
*           Next, assemble the left singular vector matrix U (M x N).
*
         IF ( NR .LT. M ) THEN
            CALL SLASET( 'A',  M-NR, NR, ZERO, ZERO, U(NR+1,1), LDU )
            IF ( NR .LT. N1 ) THEN
               CALL SLASET( 'A',NR,  N1-NR, ZERO, ZERO,  U(1,NR+1),LDU )
               CALL SLASET( 'A',M-NR,N1-NR, ZERO, ONE,U(NR+1,NR+1),LDU )
            END IF
         END IF
*
         CALL SORMQR( 'Left', 'No Tr', M, N1, N, A, LDA, WORK, U,
     $        LDU, WORK(N+1), LWORK-N, IERR )
*
            IF ( ROWPIV )
     $         CALL SLASWP( N1, U, LDU, 1, M-1, IWORK(2*N+1), -1 )
*
*
         END IF
         IF ( TRANSP ) THEN
*           .. swap U and V because the procedure worked on A^t
            DO 6974 p = 1, N
               CALL SSWAP( N, U(1,p), 1, V(1,p), 1 )
 6974       CONTINUE
         END IF
*
      END IF
*     end of the full SVD
*
*     Undo scaling, if necessary (and possible)
*
      IF ( USCAL2 .LE. (BIG/SVA(1))*USCAL1 ) THEN
         CALL SLASCL( 'G', 0, 0, USCAL1, USCAL2, NR, 1, SVA, N, IERR )
         USCAL1 = ONE
         USCAL2 = ONE
      END IF
*
      IF ( NR .LT. N ) THEN
         DO 3004 p = NR+1, N
            SVA(p) = ZERO
 3004    CONTINUE
      END IF
*
      WORK(1) = USCAL2 * SCALEM
      WORK(2) = USCAL1
      IF ( ERREST ) WORK(3) = SCONDA
      IF ( LSVEC .AND. RSVEC ) THEN
         WORK(4) = CONDR1
         WORK(5) = CONDR2
      END IF
      IF ( L2TRAN ) THEN
         WORK(6) = ENTRA
         WORK(7) = ENTRAT
      END IF
*
      IWORK(1) = NR
      IWORK(2) = NUMRANK
      IWORK(3) = WARNING
*
      RETURN
*     ..
*     .. END OF SGEJSV
*     ..
      END
*
