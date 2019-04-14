*> \brief \b SLANEG computes the Sturm count.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLANEG + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaneg.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaneg.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaneg.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       INTEGER FUNCTION SLANEG( N, D, LLD, SIGMA, PIVMIN, R )
*
*       .. Scalar Arguments ..
*       INTEGER            N, R
*       REAL               PIVMIN, SIGMA
*       ..
*       .. Array Arguments ..
*       REAL               D( * ), LLD( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLANEG computes the Sturm count, the number of negative pivots
*> encountered while factoring tridiagonal T - sigma I = L D L^T.
*> This implementation works directly on the factors without forming
*> the tridiagonal matrix T.  The Sturm count is also the number of
*> eigenvalues of T less than sigma.
*>
*> This routine is called from SLARRB.
*>
*> The current routine does not use the PIVMIN parameter but rather
*> requires IEEE-754 propagation of Infinities and NaNs.  This
*> routine also has no input range restrictions but does require
*> default exception handling such that x/0 produces Inf when x is
*> non-zero, and Inf/Inf produces NaN.  For more information, see:
*>
*>   Marques, Riedy, and Voemel, "Benefits of IEEE-754 Features in
*>   Modern Symmetric Tridiagonal Eigensolvers," SIAM Journal on
*>   Scientific Computing, v28, n5, 2006.  DOI 10.1137/050641624
*>   (Tech report version in LAWN 172 with the same title.)
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
*> \param[in] SIGMA
*> \verbatim
*>          SIGMA is REAL
*>          Shift amount in T - sigma I = L D L^T.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is REAL
*>          The minimum pivot in the Sturm sequence.  May be used
*>          when zero pivots are encountered on non-IEEE-754
*>          architectures.
*> \endverbatim
*>
*> \param[in] R
*> \verbatim
*>          R is INTEGER
*>          The twist index for the twisted factorization that is used
*>          for the negcount.
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
*> \date December 2016
*
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Osni Marques, LBNL/NERSC, USA \n
*>     Christof Voemel, University of California, Berkeley, USA \n
*>     Jason Riedy, University of California, Berkeley, USA \n
*>
*  =====================================================================
      INTEGER FUNCTION SLANEG( N, D, LLD, SIGMA, PIVMIN, R )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            N, R
      REAL               PIVMIN, SIGMA
*     ..
*     .. Array Arguments ..
      REAL               D( * ), LLD( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER        ( ZERO = 0.0E0, ONE = 1.0E0 )
*     Some architectures propagate Infinities and NaNs very slowly, so
*     the code computes counts in BLKLEN chunks.  Then a NaN can
*     propagate at most BLKLEN columns before being detected.  This is
*     not a general tuning parameter; it needs only to be just large
*     enough that the overhead is tiny in common cases.
      INTEGER BLKLEN
      PARAMETER ( BLKLEN = 128 )
*     ..
*     .. Local Scalars ..
      INTEGER            BJ, J, NEG1, NEG2, NEGCNT
      REAL               BSAV, DMINUS, DPLUS, GAMMA, P, T, TMP
      LOGICAL SAWNAN
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC MIN, MAX
*     ..
*     .. External Functions ..
      LOGICAL SISNAN
      EXTERNAL SISNAN
*     ..
*     .. Executable Statements ..

      NEGCNT = 0

*     I) upper part: L D L^T - SIGMA I = L+ D+ L+^T
      T = -SIGMA
      DO 210 BJ = 1, R-1, BLKLEN
         NEG1 = 0
         BSAV = T
         DO 21 J = BJ, MIN(BJ+BLKLEN-1, R-1)
            DPLUS = D( J ) + T
            IF( DPLUS.LT.ZERO ) NEG1 = NEG1 + 1
            TMP = T / DPLUS
            T = TMP * LLD( J ) - SIGMA
 21      CONTINUE
         SAWNAN = SISNAN( T )
*     Run a slower version of the above loop if a NaN is detected.
*     A NaN should occur only with a zero pivot after an infinite
*     pivot.  In that case, substituting 1 for T/DPLUS is the
*     correct limit.
         IF( SAWNAN ) THEN
            NEG1 = 0
            T = BSAV
            DO 22 J = BJ, MIN(BJ+BLKLEN-1, R-1)
               DPLUS = D( J ) + T
               IF( DPLUS.LT.ZERO ) NEG1 = NEG1 + 1
               TMP = T / DPLUS
               IF (SISNAN(TMP)) TMP = ONE
               T = TMP * LLD(J) - SIGMA
 22         CONTINUE
         END IF
         NEGCNT = NEGCNT + NEG1
 210  CONTINUE
*
*     II) lower part: L D L^T - SIGMA I = U- D- U-^T
      P = D( N ) - SIGMA
      DO 230 BJ = N-1, R, -BLKLEN
         NEG2 = 0
         BSAV = P
         DO 23 J = BJ, MAX(BJ-BLKLEN+1, R), -1
            DMINUS = LLD( J ) + P
            IF( DMINUS.LT.ZERO ) NEG2 = NEG2 + 1
            TMP = P / DMINUS
            P = TMP * D( J ) - SIGMA
 23      CONTINUE
         SAWNAN = SISNAN( P )
*     As above, run a slower version that substitutes 1 for Inf/Inf.
*
         IF( SAWNAN ) THEN
            NEG2 = 0
            P = BSAV
            DO 24 J = BJ, MAX(BJ-BLKLEN+1, R), -1
               DMINUS = LLD( J ) + P
               IF( DMINUS.LT.ZERO ) NEG2 = NEG2 + 1
               TMP = P / DMINUS
               IF (SISNAN(TMP)) TMP = ONE
               P = TMP * D(J) - SIGMA
 24         CONTINUE
         END IF
         NEGCNT = NEGCNT + NEG2
 230  CONTINUE
*
*     III) Twist index
*       T was shifted by SIGMA initially.
      GAMMA = (T + SIGMA) + P
      IF( GAMMA.LT.ZERO ) NEGCNT = NEGCNT+1

      SLANEG = NEGCNT
      END
