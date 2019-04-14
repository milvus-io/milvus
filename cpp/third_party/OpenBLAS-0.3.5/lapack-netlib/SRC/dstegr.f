*> \brief \b DSTEGR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DSTEGR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dstegr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dstegr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dstegr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSTEGR( JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
*                  ABSTOL, M, W, Z, LDZ, ISUPPZ, WORK, LWORK, IWORK,
*                  LIWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBZ, RANGE
*       INTEGER            IL, INFO, IU, LDZ, LIWORK, LWORK, M, N
*       DOUBLE PRECISION ABSTOL, VL, VU
*       ..
*       .. Array Arguments ..
*       INTEGER            ISUPPZ( * ), IWORK( * )
*       DOUBLE PRECISION   D( * ), E( * ), W( * ), WORK( * )
*       DOUBLE PRECISION   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSTEGR computes selected eigenvalues and, optionally, eigenvectors
*> of a real symmetric tridiagonal matrix T. Any such unreduced matrix has
*> a well defined set of pairwise different real eigenvalues, the corresponding
*> real eigenvectors are pairwise orthogonal.
*>
*> The spectrum may be computed either completely or partially by specifying
*> either an interval (VL,VU] or a range of indices IL:IU for the desired
*> eigenvalues.
*>
*> DSTEGR is a compatibility wrapper around the improved DSTEMR routine.
*> See DSTEMR for further details.
*>
*> One important change is that the ABSTOL parameter no longer provides any
*> benefit and hence is no longer used.
*>
*> Note : DSTEGR and DSTEMR work only on machines which follow
*> IEEE-754 floating-point standard in their handling of infinities and
*> NaNs.  Normal execution may create these exceptiona values and hence
*> may abort due to a floating point exception in environments which
*> do not conform to the IEEE-754 standard.
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
*> \param[in] ABSTOL
*> \verbatim
*>          ABSTOL is DOUBLE PRECISION
*>          Unused.  Was the absolute error tolerance for the
*>          eigenvalues/eigenvectors in previous versions.
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
*>          Z is DOUBLE PRECISION array, dimension (LDZ, max(1,M) )
*>          If JOBZ = 'V', and if INFO = 0, then the first M columns of Z
*>          contain the orthonormal eigenvectors of the matrix T
*>          corresponding to the selected eigenvalues, with the i-th
*>          column of Z holding the eigenvector associated with W(i).
*>          If JOBZ = 'N', then Z is not referenced.
*>          Note: the user must ensure that at least max(1,M) columns are
*>          supplied in the array Z; if RANGE = 'V', the exact value of M
*>          is not known in advance and an upper bound must be used.
*>          Supplying N columns is always safe.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.  LDZ >= 1, and if
*>          JOBZ = 'V', then LDZ >= max(1,N).
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
*>                if INFO = 2X, internal error in DLARRV.
*>                Here, the digit X = ABS( IINFO ) < 10, where IINFO is
*>                the nonzero error code returned by DLARRE or
*>                DLARRV, respectively.
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
*> \ingroup doubleOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*> Inderjit Dhillon, IBM Almaden, USA \n
*> Osni Marques, LBNL/NERSC, USA \n
*> Christof Voemel, LBNL/NERSC, USA \n
*
*  =====================================================================
      SUBROUTINE DSTEGR( JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
     $           ABSTOL, M, W, Z, LDZ, ISUPPZ, WORK, LWORK, IWORK,
     $           LIWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBZ, RANGE
      INTEGER            IL, INFO, IU, LDZ, LIWORK, LWORK, M, N
      DOUBLE PRECISION ABSTOL, VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            ISUPPZ( * ), IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), W( * ), WORK( * )
      DOUBLE PRECISION   Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL TRYRAC
*     ..
*     .. External Subroutines ..
      EXTERNAL DSTEMR
*     ..
*     .. Executable Statements ..
      INFO = 0
      TRYRAC = .FALSE.

      CALL DSTEMR( JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
     $                   M, W, Z, LDZ, N, ISUPPZ, TRYRAC, WORK, LWORK,
     $                   IWORK, LIWORK, INFO )
*
*     End of DSTEGR
*
      END
