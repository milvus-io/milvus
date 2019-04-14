*> \brief \b SLAEBZ computes the number of eigenvalues of a real symmetric tridiagonal matrix which are less than or equal to a given value, and performs other tasks required by the routine sstebz.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAEBZ + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaebz.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaebz.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaebz.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAEBZ( IJOB, NITMAX, N, MMAX, MINP, NBMIN, ABSTOL,
*                          RELTOL, PIVMIN, D, E, E2, NVAL, AB, C, MOUT,
*                          NAB, WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            IJOB, INFO, MINP, MMAX, MOUT, N, NBMIN, NITMAX
*       REAL               ABSTOL, PIVMIN, RELTOL
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * ), NAB( MMAX, * ), NVAL( * )
*       REAL               AB( MMAX, * ), C( * ), D( * ), E( * ), E2( * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAEBZ contains the iteration loops which compute and use the
*> function N(w), which is the count of eigenvalues of a symmetric
*> tridiagonal matrix T less than or equal to its argument  w.  It
*> performs a choice of two types of loops:
*>
*> IJOB=1, followed by
*> IJOB=2: It takes as input a list of intervals and returns a list of
*>         sufficiently small intervals whose union contains the same
*>         eigenvalues as the union of the original intervals.
*>         The input intervals are (AB(j,1),AB(j,2)], j=1,...,MINP.
*>         The output interval (AB(j,1),AB(j,2)] will contain
*>         eigenvalues NAB(j,1)+1,...,NAB(j,2), where 1 <= j <= MOUT.
*>
*> IJOB=3: It performs a binary search in each input interval
*>         (AB(j,1),AB(j,2)] for a point  w(j)  such that
*>         N(w(j))=NVAL(j), and uses  C(j)  as the starting point of
*>         the search.  If such a w(j) is found, then on output
*>         AB(j,1)=AB(j,2)=w.  If no such w(j) is found, then on output
*>         (AB(j,1),AB(j,2)] will be a small interval containing the
*>         point where N(w) jumps through NVAL(j), unless that point
*>         lies outside the initial interval.
*>
*> Note that the intervals are in all cases half-open intervals,
*> i.e., of the form  (a,b] , which includes  b  but not  a .
*>
*> To avoid underflow, the matrix should be scaled so that its largest
*> element is no greater than  overflow**(1/2) * underflow**(1/4)
*> in absolute value.  To assure the most accurate computation
*> of small eigenvalues, the matrix should be scaled to be
*> not much smaller than that, either.
*>
*> See W. Kahan "Accurate Eigenvalues of a Symmetric Tridiagonal
*> Matrix", Report CS41, Computer Science Dept., Stanford
*> University, July 21, 1966
*>
*> Note: the arguments are, in general, *not* checked for unreasonable
*> values.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IJOB
*> \verbatim
*>          IJOB is INTEGER
*>          Specifies what is to be done:
*>          = 1:  Compute NAB for the initial intervals.
*>          = 2:  Perform bisection iteration to find eigenvalues of T.
*>          = 3:  Perform bisection iteration to invert N(w), i.e.,
*>                to find a point which has a specified number of
*>                eigenvalues of T to its left.
*>          Other values will cause SLAEBZ to return with INFO=-1.
*> \endverbatim
*>
*> \param[in] NITMAX
*> \verbatim
*>          NITMAX is INTEGER
*>          The maximum number of "levels" of bisection to be
*>          performed, i.e., an interval of width W will not be made
*>          smaller than 2^(-NITMAX) * W.  If not all intervals
*>          have converged after NITMAX iterations, then INFO is set
*>          to the number of non-converged intervals.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The dimension n of the tridiagonal matrix T.  It must be at
*>          least 1.
*> \endverbatim
*>
*> \param[in] MMAX
*> \verbatim
*>          MMAX is INTEGER
*>          The maximum number of intervals.  If more than MMAX intervals
*>          are generated, then SLAEBZ will quit with INFO=MMAX+1.
*> \endverbatim
*>
*> \param[in] MINP
*> \verbatim
*>          MINP is INTEGER
*>          The initial number of intervals.  It may not be greater than
*>          MMAX.
*> \endverbatim
*>
*> \param[in] NBMIN
*> \verbatim
*>          NBMIN is INTEGER
*>          The smallest number of intervals that should be processed
*>          using a vector loop.  If zero, then only the scalar loop
*>          will be used.
*> \endverbatim
*>
*> \param[in] ABSTOL
*> \verbatim
*>          ABSTOL is REAL
*>          The minimum (absolute) width of an interval.  When an
*>          interval is narrower than ABSTOL, or than RELTOL times the
*>          larger (in magnitude) endpoint, then it is considered to be
*>          sufficiently small, i.e., converged.  This must be at least
*>          zero.
*> \endverbatim
*>
*> \param[in] RELTOL
*> \verbatim
*>          RELTOL is REAL
*>          The minimum relative width of an interval.  When an interval
*>          is narrower than ABSTOL, or than RELTOL times the larger (in
*>          magnitude) endpoint, then it is considered to be
*>          sufficiently small, i.e., converged.  Note: this should
*>          always be at least radix*machine epsilon.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is REAL
*>          The minimum absolute value of a "pivot" in the Sturm
*>          sequence loop.
*>          This must be at least  max |e(j)**2|*safe_min  and at
*>          least safe_min, where safe_min is at least
*>          the smallest number that can divide one without overflow.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          The diagonal elements of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is REAL array, dimension (N)
*>          The offdiagonal elements of the tridiagonal matrix T in
*>          positions 1 through N-1.  E(N) is arbitrary.
*> \endverbatim
*>
*> \param[in] E2
*> \verbatim
*>          E2 is REAL array, dimension (N)
*>          The squares of the offdiagonal elements of the tridiagonal
*>          matrix T.  E2(N) is ignored.
*> \endverbatim
*>
*> \param[in,out] NVAL
*> \verbatim
*>          NVAL is INTEGER array, dimension (MINP)
*>          If IJOB=1 or 2, not referenced.
*>          If IJOB=3, the desired values of N(w).  The elements of NVAL
*>          will be reordered to correspond with the intervals in AB.
*>          Thus, NVAL(j) on output will not, in general be the same as
*>          NVAL(j) on input, but it will correspond with the interval
*>          (AB(j,1),AB(j,2)] on output.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is REAL array, dimension (MMAX,2)
*>          The endpoints of the intervals.  AB(j,1) is  a(j), the left
*>          endpoint of the j-th interval, and AB(j,2) is b(j), the
*>          right endpoint of the j-th interval.  The input intervals
*>          will, in general, be modified, split, and reordered by the
*>          calculation.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is REAL array, dimension (MMAX)
*>          If IJOB=1, ignored.
*>          If IJOB=2, workspace.
*>          If IJOB=3, then on input C(j) should be initialized to the
*>          first search point in the binary search.
*> \endverbatim
*>
*> \param[out] MOUT
*> \verbatim
*>          MOUT is INTEGER
*>          If IJOB=1, the number of eigenvalues in the intervals.
*>          If IJOB=2 or 3, the number of intervals output.
*>          If IJOB=3, MOUT will equal MINP.
*> \endverbatim
*>
*> \param[in,out] NAB
*> \verbatim
*>          NAB is INTEGER array, dimension (MMAX,2)
*>          If IJOB=1, then on output NAB(i,j) will be set to N(AB(i,j)).
*>          If IJOB=2, then on input, NAB(i,j) should be set.  It must
*>             satisfy the condition:
*>             N(AB(i,1)) <= NAB(i,1) <= NAB(i,2) <= N(AB(i,2)),
*>             which means that in interval i only eigenvalues
*>             NAB(i,1)+1,...,NAB(i,2) will be considered.  Usually,
*>             NAB(i,j)=N(AB(i,j)), from a previous call to SLAEBZ with
*>             IJOB=1.
*>             On output, NAB(i,j) will contain
*>             max(na(k),min(nb(k),N(AB(i,j)))), where k is the index of
*>             the input interval that the output interval
*>             (AB(j,1),AB(j,2)] came from, and na(k) and nb(k) are the
*>             the input values of NAB(k,1) and NAB(k,2).
*>          If IJOB=3, then on output, NAB(i,j) contains N(AB(i,j)),
*>             unless N(w) > NVAL(i) for all search points  w , in which
*>             case NAB(i,1) will not be modified, i.e., the output
*>             value will be the same as the input value (modulo
*>             reorderings -- see NVAL and AB), or unless N(w) < NVAL(i)
*>             for all search points  w , in which case NAB(i,2) will
*>             not be modified.  Normally, NAB should be set to some
*>             distinctive value(s) before SLAEBZ is called.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MMAX)
*>          Workspace.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MMAX)
*>          Workspace.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:       All intervals converged.
*>          = 1--MMAX: The last INFO intervals did not converge.
*>          = MMAX+1:  More than MMAX intervals were generated.
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
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>      This routine is intended to be called only by other LAPACK
*>  routines, thus the interface is less user-friendly.  It is intended
*>  for two purposes:
*>
*>  (a) finding eigenvalues.  In this case, SLAEBZ should have one or
*>      more initial intervals set up in AB, and SLAEBZ should be called
*>      with IJOB=1.  This sets up NAB, and also counts the eigenvalues.
*>      Intervals with no eigenvalues would usually be thrown out at
*>      this point.  Also, if not all the eigenvalues in an interval i
*>      are desired, NAB(i,1) can be increased or NAB(i,2) decreased.
*>      For example, set NAB(i,1)=NAB(i,2)-1 to get the largest
*>      eigenvalue.  SLAEBZ is then called with IJOB=2 and MMAX
*>      no smaller than the value of MOUT returned by the call with
*>      IJOB=1.  After this (IJOB=2) call, eigenvalues NAB(i,1)+1
*>      through NAB(i,2) are approximately AB(i,1) (or AB(i,2)) to the
*>      tolerance specified by ABSTOL and RELTOL.
*>
*>  (b) finding an interval (a',b'] containing eigenvalues w(f),...,w(l).
*>      In this case, start with a Gershgorin interval  (a,b).  Set up
*>      AB to contain 2 search intervals, both initially (a,b).  One
*>      NVAL element should contain  f-1  and the other should contain  l
*>      , while C should contain a and b, resp.  NAB(i,1) should be -1
*>      and NAB(i,2) should be N+1, to flag an error if the desired
*>      interval does not lie in (a,b).  SLAEBZ is then called with
*>      IJOB=3.  On exit, if w(f-1) < w(f), then one of the intervals --
*>      j -- will have AB(j,1)=AB(j,2) and NAB(j,1)=NAB(j,2)=f-1, while
*>      if, to the specified tolerance, w(f-k)=...=w(f+r), k > 0 and r
*>      >= 0, then the interval will have  N(AB(j,1))=NAB(j,1)=f-k and
*>      N(AB(j,2))=NAB(j,2)=f+r.  The cases w(l) < w(l+1) and
*>      w(l-r)=...=w(l+k) are handled similarly.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SLAEBZ( IJOB, NITMAX, N, MMAX, MINP, NBMIN, ABSTOL,
     $                   RELTOL, PIVMIN, D, E, E2, NVAL, AB, C, MOUT,
     $                   NAB, WORK, IWORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            IJOB, INFO, MINP, MMAX, MOUT, N, NBMIN, NITMAX
      REAL               ABSTOL, PIVMIN, RELTOL
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * ), NAB( MMAX, * ), NVAL( * )
      REAL               AB( MMAX, * ), C( * ), D( * ), E( * ), E2( * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, TWO, HALF
      PARAMETER          ( ZERO = 0.0E0, TWO = 2.0E0,
     $                   HALF = 1.0E0 / TWO )
*     ..
*     .. Local Scalars ..
      INTEGER            ITMP1, ITMP2, J, JI, JIT, JP, KF, KFNEW, KL,
     $                   KLNEW
      REAL               TMP1, TMP2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Check for Errors
*
      INFO = 0
      IF( IJOB.LT.1 .OR. IJOB.GT.3 ) THEN
         INFO = -1
         RETURN
      END IF
*
*     Initialize NAB
*
      IF( IJOB.EQ.1 ) THEN
*
*        Compute the number of eigenvalues in the initial intervals.
*
         MOUT = 0
         DO 30 JI = 1, MINP
            DO 20 JP = 1, 2
               TMP1 = D( 1 ) - AB( JI, JP )
               IF( ABS( TMP1 ).LT.PIVMIN )
     $            TMP1 = -PIVMIN
               NAB( JI, JP ) = 0
               IF( TMP1.LE.ZERO )
     $            NAB( JI, JP ) = 1
*
               DO 10 J = 2, N
                  TMP1 = D( J ) - E2( J-1 ) / TMP1 - AB( JI, JP )
                  IF( ABS( TMP1 ).LT.PIVMIN )
     $               TMP1 = -PIVMIN
                  IF( TMP1.LE.ZERO )
     $               NAB( JI, JP ) = NAB( JI, JP ) + 1
   10          CONTINUE
   20       CONTINUE
            MOUT = MOUT + NAB( JI, 2 ) - NAB( JI, 1 )
   30    CONTINUE
         RETURN
      END IF
*
*     Initialize for loop
*
*     KF and KL have the following meaning:
*        Intervals 1,...,KF-1 have converged.
*        Intervals KF,...,KL  still need to be refined.
*
      KF = 1
      KL = MINP
*
*     If IJOB=2, initialize C.
*     If IJOB=3, use the user-supplied starting point.
*
      IF( IJOB.EQ.2 ) THEN
         DO 40 JI = 1, MINP
            C( JI ) = HALF*( AB( JI, 1 )+AB( JI, 2 ) )
   40    CONTINUE
      END IF
*
*     Iteration loop
*
      DO 130 JIT = 1, NITMAX
*
*        Loop over intervals
*
         IF( KL-KF+1.GE.NBMIN .AND. NBMIN.GT.0 ) THEN
*
*           Begin of Parallel Version of the loop
*
            DO 60 JI = KF, KL
*
*              Compute N(c), the number of eigenvalues less than c
*
               WORK( JI ) = D( 1 ) - C( JI )
               IWORK( JI ) = 0
               IF( WORK( JI ).LE.PIVMIN ) THEN
                  IWORK( JI ) = 1
                  WORK( JI ) = MIN( WORK( JI ), -PIVMIN )
               END IF
*
               DO 50 J = 2, N
                  WORK( JI ) = D( J ) - E2( J-1 ) / WORK( JI ) - C( JI )
                  IF( WORK( JI ).LE.PIVMIN ) THEN
                     IWORK( JI ) = IWORK( JI ) + 1
                     WORK( JI ) = MIN( WORK( JI ), -PIVMIN )
                  END IF
   50          CONTINUE
   60       CONTINUE
*
            IF( IJOB.LE.2 ) THEN
*
*              IJOB=2: Choose all intervals containing eigenvalues.
*
               KLNEW = KL
               DO 70 JI = KF, KL
*
*                 Insure that N(w) is monotone
*
                  IWORK( JI ) = MIN( NAB( JI, 2 ),
     $                          MAX( NAB( JI, 1 ), IWORK( JI ) ) )
*
*                 Update the Queue -- add intervals if both halves
*                 contain eigenvalues.
*
                  IF( IWORK( JI ).EQ.NAB( JI, 2 ) ) THEN
*
*                    No eigenvalue in the upper interval:
*                    just use the lower interval.
*
                     AB( JI, 2 ) = C( JI )
*
                  ELSE IF( IWORK( JI ).EQ.NAB( JI, 1 ) ) THEN
*
*                    No eigenvalue in the lower interval:
*                    just use the upper interval.
*
                     AB( JI, 1 ) = C( JI )
                  ELSE
                     KLNEW = KLNEW + 1
                     IF( KLNEW.LE.MMAX ) THEN
*
*                       Eigenvalue in both intervals -- add upper to
*                       queue.
*
                        AB( KLNEW, 2 ) = AB( JI, 2 )
                        NAB( KLNEW, 2 ) = NAB( JI, 2 )
                        AB( KLNEW, 1 ) = C( JI )
                        NAB( KLNEW, 1 ) = IWORK( JI )
                        AB( JI, 2 ) = C( JI )
                        NAB( JI, 2 ) = IWORK( JI )
                     ELSE
                        INFO = MMAX + 1
                     END IF
                  END IF
   70          CONTINUE
               IF( INFO.NE.0 )
     $            RETURN
               KL = KLNEW
            ELSE
*
*              IJOB=3: Binary search.  Keep only the interval containing
*                      w   s.t. N(w) = NVAL
*
               DO 80 JI = KF, KL
                  IF( IWORK( JI ).LE.NVAL( JI ) ) THEN
                     AB( JI, 1 ) = C( JI )
                     NAB( JI, 1 ) = IWORK( JI )
                  END IF
                  IF( IWORK( JI ).GE.NVAL( JI ) ) THEN
                     AB( JI, 2 ) = C( JI )
                     NAB( JI, 2 ) = IWORK( JI )
                  END IF
   80          CONTINUE
            END IF
*
         ELSE
*
*           End of Parallel Version of the loop
*
*           Begin of Serial Version of the loop
*
            KLNEW = KL
            DO 100 JI = KF, KL
*
*              Compute N(w), the number of eigenvalues less than w
*
               TMP1 = C( JI )
               TMP2 = D( 1 ) - TMP1
               ITMP1 = 0
               IF( TMP2.LE.PIVMIN ) THEN
                  ITMP1 = 1
                  TMP2 = MIN( TMP2, -PIVMIN )
               END IF
*
               DO 90 J = 2, N
                  TMP2 = D( J ) - E2( J-1 ) / TMP2 - TMP1
                  IF( TMP2.LE.PIVMIN ) THEN
                     ITMP1 = ITMP1 + 1
                     TMP2 = MIN( TMP2, -PIVMIN )
                  END IF
   90          CONTINUE
*
               IF( IJOB.LE.2 ) THEN
*
*                 IJOB=2: Choose all intervals containing eigenvalues.
*
*                 Insure that N(w) is monotone
*
                  ITMP1 = MIN( NAB( JI, 2 ),
     $                    MAX( NAB( JI, 1 ), ITMP1 ) )
*
*                 Update the Queue -- add intervals if both halves
*                 contain eigenvalues.
*
                  IF( ITMP1.EQ.NAB( JI, 2 ) ) THEN
*
*                    No eigenvalue in the upper interval:
*                    just use the lower interval.
*
                     AB( JI, 2 ) = TMP1
*
                  ELSE IF( ITMP1.EQ.NAB( JI, 1 ) ) THEN
*
*                    No eigenvalue in the lower interval:
*                    just use the upper interval.
*
                     AB( JI, 1 ) = TMP1
                  ELSE IF( KLNEW.LT.MMAX ) THEN
*
*                    Eigenvalue in both intervals -- add upper to queue.
*
                     KLNEW = KLNEW + 1
                     AB( KLNEW, 2 ) = AB( JI, 2 )
                     NAB( KLNEW, 2 ) = NAB( JI, 2 )
                     AB( KLNEW, 1 ) = TMP1
                     NAB( KLNEW, 1 ) = ITMP1
                     AB( JI, 2 ) = TMP1
                     NAB( JI, 2 ) = ITMP1
                  ELSE
                     INFO = MMAX + 1
                     RETURN
                  END IF
               ELSE
*
*                 IJOB=3: Binary search.  Keep only the interval
*                         containing  w  s.t. N(w) = NVAL
*
                  IF( ITMP1.LE.NVAL( JI ) ) THEN
                     AB( JI, 1 ) = TMP1
                     NAB( JI, 1 ) = ITMP1
                  END IF
                  IF( ITMP1.GE.NVAL( JI ) ) THEN
                     AB( JI, 2 ) = TMP1
                     NAB( JI, 2 ) = ITMP1
                  END IF
               END IF
  100       CONTINUE
            KL = KLNEW
*
         END IF
*
*        Check for convergence
*
         KFNEW = KF
         DO 110 JI = KF, KL
            TMP1 = ABS( AB( JI, 2 )-AB( JI, 1 ) )
            TMP2 = MAX( ABS( AB( JI, 2 ) ), ABS( AB( JI, 1 ) ) )
            IF( TMP1.LT.MAX( ABSTOL, PIVMIN, RELTOL*TMP2 ) .OR.
     $          NAB( JI, 1 ).GE.NAB( JI, 2 ) ) THEN
*
*              Converged -- Swap with position KFNEW,
*                           then increment KFNEW
*
               IF( JI.GT.KFNEW ) THEN
                  TMP1 = AB( JI, 1 )
                  TMP2 = AB( JI, 2 )
                  ITMP1 = NAB( JI, 1 )
                  ITMP2 = NAB( JI, 2 )
                  AB( JI, 1 ) = AB( KFNEW, 1 )
                  AB( JI, 2 ) = AB( KFNEW, 2 )
                  NAB( JI, 1 ) = NAB( KFNEW, 1 )
                  NAB( JI, 2 ) = NAB( KFNEW, 2 )
                  AB( KFNEW, 1 ) = TMP1
                  AB( KFNEW, 2 ) = TMP2
                  NAB( KFNEW, 1 ) = ITMP1
                  NAB( KFNEW, 2 ) = ITMP2
                  IF( IJOB.EQ.3 ) THEN
                     ITMP1 = NVAL( JI )
                     NVAL( JI ) = NVAL( KFNEW )
                     NVAL( KFNEW ) = ITMP1
                  END IF
               END IF
               KFNEW = KFNEW + 1
            END IF
  110    CONTINUE
         KF = KFNEW
*
*        Choose Midpoints
*
         DO 120 JI = KF, KL
            C( JI ) = HALF*( AB( JI, 1 )+AB( JI, 2 ) )
  120    CONTINUE
*
*        If no more intervals to refine, quit.
*
         IF( KF.GT.KL )
     $      GO TO 140
  130 CONTINUE
*
*     Converged
*
  140 CONTINUE
      INFO = MAX( KL+1-KF, 0 )
      MOUT = KL
*
      RETURN
*
*     End of SLAEBZ
*
      END
