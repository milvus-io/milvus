*> \brief \b CUNBDB4
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CUNBDB4 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cunbdb4.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cunbdb4.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cunbdb4.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CUNBDB4( M, P, Q, X11, LDX11, X21, LDX21, THETA, PHI,
*                           TAUP1, TAUP2, TAUQ1, PHANTOM, WORK, LWORK,
*                           INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LWORK, M, P, Q, LDX11, LDX21
*       ..
*       .. Array Arguments ..
*       REAL               PHI(*), THETA(*)
*       COMPLEX            PHANTOM(*), TAUP1(*), TAUP2(*), TAUQ1(*),
*      $                   WORK(*), X11(LDX11,*), X21(LDX21,*)
*       ..
*
*
*> \par Purpose:
*  =============
*>
*>\verbatim
*>
*> CUNBDB4 simultaneously bidiagonalizes the blocks of a tall and skinny
*> matrix X with orthonomal columns:
*>
*>                            [ B11 ]
*>      [ X11 ]   [ P1 |    ] [  0  ]
*>      [-----] = [---------] [-----] Q1**T .
*>      [ X21 ]   [    | P2 ] [ B21 ]
*>                            [  0  ]
*>
*> X11 is P-by-Q, and X21 is (M-P)-by-Q. M-Q must be no larger than P,
*> M-P, or Q. Routines CUNBDB1, CUNBDB2, and CUNBDB3 handle cases in
*> which M-Q is not the minimum dimension.
*>
*> The unitary matrices P1, P2, and Q1 are P-by-P, (M-P)-by-(M-P),
*> and (M-Q)-by-(M-Q), respectively. They are represented implicitly by
*> Householder vectors.
*>
*> B11 and B12 are (M-Q)-by-(M-Q) bidiagonal matrices represented
*> implicitly by angles THETA, PHI.
*>
*>\endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>           The number of rows X11 plus the number of rows in X21.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>           The number of rows in X11. 0 <= P <= M.
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is INTEGER
*>           The number of columns in X11 and X21. 0 <= Q <= M and
*>           M-Q <= min(P,M-P,Q).
*> \endverbatim
*>
*> \param[in,out] X11
*> \verbatim
*>          X11 is COMPLEX array, dimension (LDX11,Q)
*>           On entry, the top block of the matrix X to be reduced. On
*>           exit, the columns of tril(X11) specify reflectors for P1 and
*>           the rows of triu(X11,1) specify reflectors for Q1.
*> \endverbatim
*>
*> \param[in] LDX11
*> \verbatim
*>          LDX11 is INTEGER
*>           The leading dimension of X11. LDX11 >= P.
*> \endverbatim
*>
*> \param[in,out] X21
*> \verbatim
*>          X21 is COMPLEX array, dimension (LDX21,Q)
*>           On entry, the bottom block of the matrix X to be reduced. On
*>           exit, the columns of tril(X21) specify reflectors for P2.
*> \endverbatim
*>
*> \param[in] LDX21
*> \verbatim
*>          LDX21 is INTEGER
*>           The leading dimension of X21. LDX21 >= M-P.
*> \endverbatim
*>
*> \param[out] THETA
*> \verbatim
*>          THETA is REAL array, dimension (Q)
*>           The entries of the bidiagonal blocks B11, B21 are defined by
*>           THETA and PHI. See Further Details.
*> \endverbatim
*>
*> \param[out] PHI
*> \verbatim
*>          PHI is REAL array, dimension (Q-1)
*>           The entries of the bidiagonal blocks B11, B21 are defined by
*>           THETA and PHI. See Further Details.
*> \endverbatim
*>
*> \param[out] TAUP1
*> \verbatim
*>          TAUP1 is COMPLEX array, dimension (P)
*>           The scalar factors of the elementary reflectors that define
*>           P1.
*> \endverbatim
*>
*> \param[out] TAUP2
*> \verbatim
*>          TAUP2 is COMPLEX array, dimension (M-P)
*>           The scalar factors of the elementary reflectors that define
*>           P2.
*> \endverbatim
*>
*> \param[out] TAUQ1
*> \verbatim
*>          TAUQ1 is COMPLEX array, dimension (Q)
*>           The scalar factors of the elementary reflectors that define
*>           Q1.
*> \endverbatim
*>
*> \param[out] PHANTOM
*> \verbatim
*>          PHANTOM is COMPLEX array, dimension (M)
*>           The routine computes an M-by-1 column vector Y that is
*>           orthogonal to the columns of [ X11; X21 ]. PHANTOM(1:P) and
*>           PHANTOM(P+1:M) contain Householder vectors for Y(1:P) and
*>           Y(P+1:M), respectively.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>           The dimension of the array WORK. LWORK >= M-Q.
*>
*>           If LWORK = -1, then a workspace query is assumed; the routine
*>           only calculates the optimal size of the WORK array, returns
*>           this value as the first entry of the WORK array, and no error
*>           message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           = 0:  successful exit.
*>           < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \date July 2012
*
*> \ingroup complexOTHERcomputational
*
*> \par Further Details:
*  =====================

*> \verbatim
*>
*>  The upper-bidiagonal blocks B11, B21 are represented implicitly by
*>  angles THETA(1), ..., THETA(Q) and PHI(1), ..., PHI(Q-1). Every entry
*>  in each bidiagonal band is a product of a sine or cosine of a THETA
*>  with a sine or cosine of a PHI. See [1] or CUNCSD for details.
*>
*>  P1, P2, and Q1 are represented as products of elementary reflectors.
*>  See CUNCSD2BY1 for details on generating P1, P2, and Q1 using CUNGQR
*>  and CUNGLQ.
*> \endverbatim
*
*> \par References:
*  ================
*>
*>  [1] Brian D. Sutton. Computing the complete CS decomposition. Numer.
*>      Algorithms, 50(1):33-65, 2009.
*>
*  =====================================================================
      SUBROUTINE CUNBDB4( M, P, Q, X11, LDX11, X21, LDX21, THETA, PHI,
     $                    TAUP1, TAUP2, TAUQ1, PHANTOM, WORK, LWORK,
     $                    INFO )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     July 2012
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LWORK, M, P, Q, LDX11, LDX21
*     ..
*     .. Array Arguments ..
      REAL               PHI(*), THETA(*)
      COMPLEX            PHANTOM(*), TAUP1(*), TAUP2(*), TAUQ1(*),
     $                   WORK(*), X11(LDX11,*), X21(LDX21,*)
*     ..
*
*  ====================================================================
*
*     .. Parameters ..
      COMPLEX            NEGONE, ONE, ZERO
      PARAMETER          ( NEGONE = (-1.0E0,0.0E0), ONE = (1.0E0,0.0E0),
     $                     ZERO = (0.0E0,0.0E0) )
*     ..
*     .. Local Scalars ..
      REAL               C, S
      INTEGER            CHILDINFO, I, ILARF, IORBDB5, J, LLARF,
     $                   LORBDB5, LWORKMIN, LWORKOPT
      LOGICAL            LQUERY
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLARF, CLARFGP, CUNBDB5, CSROT, CSCAL, CLACGV,
     $                   XERBLA
*     ..
*     .. External Functions ..
      REAL               SCNRM2
      EXTERNAL           SCNRM2
*     ..
*     .. Intrinsic Function ..
      INTRINSIC          ATAN2, COS, MAX, SIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test input arguments
*
      INFO = 0
      LQUERY = LWORK .EQ. -1
*
      IF( M .LT. 0 ) THEN
         INFO = -1
      ELSE IF( P .LT. M-Q .OR. M-P .LT. M-Q ) THEN
         INFO = -2
      ELSE IF( Q .LT. M-Q .OR. Q .GT. M ) THEN
         INFO = -3
      ELSE IF( LDX11 .LT. MAX( 1, P ) ) THEN
         INFO = -5
      ELSE IF( LDX21 .LT. MAX( 1, M-P ) ) THEN
         INFO = -7
      END IF
*
*     Compute workspace
*
      IF( INFO .EQ. 0 ) THEN
         ILARF = 2
         LLARF = MAX( Q-1, P-1, M-P-1 )
         IORBDB5 = 2
         LORBDB5 = Q
         LWORKOPT = ILARF + LLARF - 1
         LWORKOPT = MAX( LWORKOPT, IORBDB5 + LORBDB5 - 1 )
         LWORKMIN = LWORKOPT
         WORK(1) = LWORKOPT
         IF( LWORK .LT. LWORKMIN .AND. .NOT.LQUERY ) THEN
           INFO = -14
         END IF
      END IF
      IF( INFO .NE. 0 ) THEN
         CALL XERBLA( 'CUNBDB4', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Reduce columns 1, ..., M-Q of X11 and X21
*
      DO I = 1, M-Q
*
         IF( I .EQ. 1 ) THEN
            DO J = 1, M
               PHANTOM(J) = ZERO
            END DO
            CALL CUNBDB5( P, M-P, Q, PHANTOM(1), 1, PHANTOM(P+1), 1,
     $                    X11, LDX11, X21, LDX21, WORK(IORBDB5),
     $                    LORBDB5, CHILDINFO )
            CALL CSCAL( P, NEGONE, PHANTOM(1), 1 )
            CALL CLARFGP( P, PHANTOM(1), PHANTOM(2), 1, TAUP1(1) )
            CALL CLARFGP( M-P, PHANTOM(P+1), PHANTOM(P+2), 1, TAUP2(1) )
            THETA(I) = ATAN2( REAL( PHANTOM(1) ), REAL( PHANTOM(P+1) ) )
            C = COS( THETA(I) )
            S = SIN( THETA(I) )
            PHANTOM(1) = ONE
            PHANTOM(P+1) = ONE
            CALL CLARF( 'L', P, Q, PHANTOM(1), 1, CONJG(TAUP1(1)), X11,
     $                  LDX11, WORK(ILARF) )
            CALL CLARF( 'L', M-P, Q, PHANTOM(P+1), 1, CONJG(TAUP2(1)),
     $                  X21, LDX21, WORK(ILARF) )
         ELSE
            CALL CUNBDB5( P-I+1, M-P-I+1, Q-I+1, X11(I,I-1), 1,
     $                    X21(I,I-1), 1, X11(I,I), LDX11, X21(I,I),
     $                    LDX21, WORK(IORBDB5), LORBDB5, CHILDINFO )
            CALL CSCAL( P-I+1, NEGONE, X11(I,I-1), 1 )
            CALL CLARFGP( P-I+1, X11(I,I-1), X11(I+1,I-1), 1, TAUP1(I) )
            CALL CLARFGP( M-P-I+1, X21(I,I-1), X21(I+1,I-1), 1,
     $                    TAUP2(I) )
            THETA(I) = ATAN2( REAL( X11(I,I-1) ), REAL( X21(I,I-1) ) )
            C = COS( THETA(I) )
            S = SIN( THETA(I) )
            X11(I,I-1) = ONE
            X21(I,I-1) = ONE
            CALL CLARF( 'L', P-I+1, Q-I+1, X11(I,I-1), 1,
     $                  CONJG(TAUP1(I)), X11(I,I), LDX11, WORK(ILARF) )
            CALL CLARF( 'L', M-P-I+1, Q-I+1, X21(I,I-1), 1,
     $                  CONJG(TAUP2(I)), X21(I,I), LDX21, WORK(ILARF) )
         END IF
*
         CALL CSROT( Q-I+1, X11(I,I), LDX11, X21(I,I), LDX21, S, -C )
         CALL CLACGV( Q-I+1, X21(I,I), LDX21 )
         CALL CLARFGP( Q-I+1, X21(I,I), X21(I,I+1), LDX21, TAUQ1(I) )
         C = REAL( X21(I,I) )
         X21(I,I) = ONE
         CALL CLARF( 'R', P-I, Q-I+1, X21(I,I), LDX21, TAUQ1(I),
     $               X11(I+1,I), LDX11, WORK(ILARF) )
         CALL CLARF( 'R', M-P-I, Q-I+1, X21(I,I), LDX21, TAUQ1(I),
     $               X21(I+1,I), LDX21, WORK(ILARF) )
         CALL CLACGV( Q-I+1, X21(I,I), LDX21 )
         IF( I .LT. M-Q ) THEN
            S = SQRT( SCNRM2( P-I, X11(I+1,I), 1 )**2
     $              + SCNRM2( M-P-I, X21(I+1,I), 1 )**2 )
            PHI(I) = ATAN2( S, C )
         END IF
*
      END DO
*
*     Reduce the bottom-right portion of X11 to [ I 0 ]
*
      DO I = M - Q + 1, P
         CALL CLACGV( Q-I+1, X11(I,I), LDX11 )
         CALL CLARFGP( Q-I+1, X11(I,I), X11(I,I+1), LDX11, TAUQ1(I) )
         X11(I,I) = ONE
         CALL CLARF( 'R', P-I, Q-I+1, X11(I,I), LDX11, TAUQ1(I),
     $               X11(I+1,I), LDX11, WORK(ILARF) )
         CALL CLARF( 'R', Q-P, Q-I+1, X11(I,I), LDX11, TAUQ1(I),
     $               X21(M-Q+1,I), LDX21, WORK(ILARF) )
         CALL CLACGV( Q-I+1, X11(I,I), LDX11 )
      END DO
*
*     Reduce the bottom-right portion of X21 to [ 0 I ]
*
      DO I = P + 1, Q
         CALL CLACGV( Q-I+1, X21(M-Q+I-P,I), LDX21 )
         CALL CLARFGP( Q-I+1, X21(M-Q+I-P,I), X21(M-Q+I-P,I+1), LDX21,
     $                 TAUQ1(I) )
         X21(M-Q+I-P,I) = ONE
         CALL CLARF( 'R', Q-I, Q-I+1, X21(M-Q+I-P,I), LDX21, TAUQ1(I),
     $               X21(M-Q+I-P+1,I), LDX21, WORK(ILARF) )
         CALL CLACGV( Q-I+1, X21(M-Q+I-P,I), LDX21 )
      END DO
*
      RETURN
*
*     End of CUNBDB4
*
      END

