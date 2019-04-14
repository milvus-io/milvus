*> \brief \b DORCSD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DORCSD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dorcsd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dorcsd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dorcsd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       RECURSIVE SUBROUTINE DORCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS,
*                                    SIGNS, M, P, Q, X11, LDX11, X12,
*                                    LDX12, X21, LDX21, X22, LDX22, THETA,
*                                    U1, LDU1, U2, LDU2, V1T, LDV1T, V2T,
*                                    LDV2T, WORK, LWORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBU1, JOBU2, JOBV1T, JOBV2T, SIGNS, TRANS
*       INTEGER            INFO, LDU1, LDU2, LDV1T, LDV2T, LDX11, LDX12,
*      $                   LDX21, LDX22, LWORK, M, P, Q
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   THETA( * )
*       DOUBLE PRECISION   U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ),
*      $                   V2T( LDV2T, * ), WORK( * ), X11( LDX11, * ),
*      $                   X12( LDX12, * ), X21( LDX21, * ), X22( LDX22,
*      $                   * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DORCSD computes the CS decomposition of an M-by-M partitioned
*> orthogonal matrix X:
*>
*>                                 [  I  0  0 |  0  0  0 ]
*>                                 [  0  C  0 |  0 -S  0 ]
*>     [ X11 | X12 ]   [ U1 |    ] [  0  0  0 |  0  0 -I ] [ V1 |    ]**T
*> X = [-----------] = [---------] [---------------------] [---------]   .
*>     [ X21 | X22 ]   [    | U2 ] [  0  0  0 |  I  0  0 ] [    | V2 ]
*>                                 [  0  S  0 |  0  C  0 ]
*>                                 [  0  0  I |  0  0  0 ]
*>
*> X11 is P-by-Q. The orthogonal matrices U1, U2, V1, and V2 are P-by-P,
*> (M-P)-by-(M-P), Q-by-Q, and (M-Q)-by-(M-Q), respectively. C and S are
*> R-by-R nonnegative diagonal matrices satisfying C^2 + S^2 = I, in
*> which R = MIN(P,M-P,Q,M-Q).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBU1
*> \verbatim
*>          JOBU1 is CHARACTER
*>          = 'Y':      U1 is computed;
*>          otherwise:  U1 is not computed.
*> \endverbatim
*>
*> \param[in] JOBU2
*> \verbatim
*>          JOBU2 is CHARACTER
*>          = 'Y':      U2 is computed;
*>          otherwise:  U2 is not computed.
*> \endverbatim
*>
*> \param[in] JOBV1T
*> \verbatim
*>          JOBV1T is CHARACTER
*>          = 'Y':      V1T is computed;
*>          otherwise:  V1T is not computed.
*> \endverbatim
*>
*> \param[in] JOBV2T
*> \verbatim
*>          JOBV2T is CHARACTER
*>          = 'Y':      V2T is computed;
*>          otherwise:  V2T is not computed.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER
*>          = 'T':      X, U1, U2, V1T, and V2T are stored in row-major
*>                      order;
*>          otherwise:  X, U1, U2, V1T, and V2T are stored in column-
*>                      major order.
*> \endverbatim
*>
*> \param[in] SIGNS
*> \verbatim
*>          SIGNS is CHARACTER
*>          = 'O':      The lower-left block is made nonpositive (the
*>                      "other" convention);
*>          otherwise:  The upper-right block is made nonpositive (the
*>                      "default" convention).
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows and columns in X.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of rows in X11 and X12. 0 <= P <= M.
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is INTEGER
*>          The number of columns in X11 and X21. 0 <= Q <= M.
*> \endverbatim
*>
*> \param[in,out] X11
*> \verbatim
*>          X11 is DOUBLE PRECISION array, dimension (LDX11,Q)
*>          On entry, part of the orthogonal matrix whose CSD is desired.
*> \endverbatim
*>
*> \param[in] LDX11
*> \verbatim
*>          LDX11 is INTEGER
*>          The leading dimension of X11. LDX11 >= MAX(1,P).
*> \endverbatim
*>
*> \param[in,out] X12
*> \verbatim
*>          X12 is DOUBLE PRECISION array, dimension (LDX12,M-Q)
*>          On entry, part of the orthogonal matrix whose CSD is desired.
*> \endverbatim
*>
*> \param[in] LDX12
*> \verbatim
*>          LDX12 is INTEGER
*>          The leading dimension of X12. LDX12 >= MAX(1,P).
*> \endverbatim
*>
*> \param[in,out] X21
*> \verbatim
*>          X21 is DOUBLE PRECISION array, dimension (LDX21,Q)
*>          On entry, part of the orthogonal matrix whose CSD is desired.
*> \endverbatim
*>
*> \param[in] LDX21
*> \verbatim
*>          LDX21 is INTEGER
*>          The leading dimension of X11. LDX21 >= MAX(1,M-P).
*> \endverbatim
*>
*> \param[in,out] X22
*> \verbatim
*>          X22 is DOUBLE PRECISION array, dimension (LDX22,M-Q)
*>          On entry, part of the orthogonal matrix whose CSD is desired.
*> \endverbatim
*>
*> \param[in] LDX22
*> \verbatim
*>          LDX22 is INTEGER
*>          The leading dimension of X11. LDX22 >= MAX(1,M-P).
*> \endverbatim
*>
*> \param[out] THETA
*> \verbatim
*>          THETA is DOUBLE PRECISION array, dimension (R), in which R =
*>          MIN(P,M-P,Q,M-Q).
*>          C = DIAG( COS(THETA(1)), ... , COS(THETA(R)) ) and
*>          S = DIAG( SIN(THETA(1)), ... , SIN(THETA(R)) ).
*> \endverbatim
*>
*> \param[out] U1
*> \verbatim
*>          U1 is DOUBLE PRECISION array, dimension (LDU1,P)
*>          If JOBU1 = 'Y', U1 contains the P-by-P orthogonal matrix U1.
*> \endverbatim
*>
*> \param[in] LDU1
*> \verbatim
*>          LDU1 is INTEGER
*>          The leading dimension of U1. If JOBU1 = 'Y', LDU1 >=
*>          MAX(1,P).
*> \endverbatim
*>
*> \param[out] U2
*> \verbatim
*>          U2 is DOUBLE PRECISION array, dimension (LDU2,M-P)
*>          If JOBU2 = 'Y', U2 contains the (M-P)-by-(M-P) orthogonal
*>          matrix U2.
*> \endverbatim
*>
*> \param[in] LDU2
*> \verbatim
*>          LDU2 is INTEGER
*>          The leading dimension of U2. If JOBU2 = 'Y', LDU2 >=
*>          MAX(1,M-P).
*> \endverbatim
*>
*> \param[out] V1T
*> \verbatim
*>          V1T is DOUBLE PRECISION array, dimension (LDV1T,Q)
*>          If JOBV1T = 'Y', V1T contains the Q-by-Q matrix orthogonal
*>          matrix V1**T.
*> \endverbatim
*>
*> \param[in] LDV1T
*> \verbatim
*>          LDV1T is INTEGER
*>          The leading dimension of V1T. If JOBV1T = 'Y', LDV1T >=
*>          MAX(1,Q).
*> \endverbatim
*>
*> \param[out] V2T
*> \verbatim
*>          V2T is DOUBLE PRECISION array, dimension (LDV2T,M-Q)
*>          If JOBV2T = 'Y', V2T contains the (M-Q)-by-(M-Q) orthogonal
*>          matrix V2**T.
*> \endverbatim
*>
*> \param[in] LDV2T
*> \verbatim
*>          LDV2T is INTEGER
*>          The leading dimension of V2T. If JOBV2T = 'Y', LDV2T >=
*>          MAX(1,M-Q).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*>          If INFO > 0 on exit, WORK(2:R) contains the values PHI(1),
*>          ..., PHI(R-1) that, together with THETA(1), ..., THETA(R),
*>          define the matrix in intermediate bidiagonal-block form
*>          remaining after nonconvergence. INFO specifies the number
*>          of nonzero PHI's.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the work array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (M-MIN(P, M-P, Q, M-Q))
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  DBBCSD did not converge. See the description of WORK
*>                above for details.
*> \endverbatim
*
*> \par References:
*  ================
*>
*>  [1] Brian D. Sutton. Computing the complete CS decomposition. Numer.
*>      Algorithms, 50(1):33-65, 2009.
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date June 2017
*
*> \ingroup doubleOTHERcomputational
*
*  =====================================================================
      RECURSIVE SUBROUTINE DORCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS,
     $                             SIGNS, M, P, Q, X11, LDX11, X12,
     $                             LDX12, X21, LDX21, X22, LDX22, THETA,
     $                             U1, LDU1, U2, LDU2, V1T, LDV1T, V2T,
     $                             LDV2T, WORK, LWORK, IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          JOBU1, JOBU2, JOBV1T, JOBV2T, SIGNS, TRANS
      INTEGER            INFO, LDU1, LDU2, LDV1T, LDV2T, LDX11, LDX12,
     $                   LDX21, LDX22, LWORK, M, P, Q
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   THETA( * )
      DOUBLE PRECISION   U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ),
     $                   V2T( LDV2T, * ), WORK( * ), X11( LDX11, * ),
     $                   X12( LDX12, * ), X21( LDX21, * ), X22( LDX22,
     $                   * )
*     ..
*
*  ===================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D0,
     $                     ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      CHARACTER          TRANST, SIGNST
      INTEGER            CHILDINFO, I, IB11D, IB11E, IB12D, IB12E,
     $                   IB21D, IB21E, IB22D, IB22E, IBBCSD, IORBDB,
     $                   IORGLQ, IORGQR, IPHI, ITAUP1, ITAUP2, ITAUQ1,
     $                   ITAUQ2, J, LBBCSDWORK, LBBCSDWORKMIN,
     $                   LBBCSDWORKOPT, LORBDBWORK, LORBDBWORKMIN,
     $                   LORBDBWORKOPT, LORGLQWORK, LORGLQWORKMIN,
     $                   LORGLQWORKOPT, LORGQRWORK, LORGQRWORKMIN,
     $                   LORGQRWORKOPT, LWORKMIN, LWORKOPT
      LOGICAL            COLMAJOR, DEFAULTSIGNS, LQUERY, WANTU1, WANTU2,
     $                   WANTV1T, WANTV2T
*     ..
*     .. External Subroutines ..
      EXTERNAL           DBBCSD, DLACPY, DLAPMR, DLAPMT,
     $                   DORBDB, DORGLQ, DORGQR, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Intrinsic Functions
      INTRINSIC          INT, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test input arguments
*
      INFO = 0
      WANTU1 = LSAME( JOBU1, 'Y' )
      WANTU2 = LSAME( JOBU2, 'Y' )
      WANTV1T = LSAME( JOBV1T, 'Y' )
      WANTV2T = LSAME( JOBV2T, 'Y' )
      COLMAJOR = .NOT. LSAME( TRANS, 'T' )
      DEFAULTSIGNS = .NOT. LSAME( SIGNS, 'O' )
      LQUERY = LWORK .EQ. -1
      IF( M .LT. 0 ) THEN
         INFO = -7
      ELSE IF( P .LT. 0 .OR. P .GT. M ) THEN
         INFO = -8
      ELSE IF( Q .LT. 0 .OR. Q .GT. M ) THEN
         INFO = -9
      ELSE IF ( COLMAJOR .AND.  LDX11 .LT. MAX( 1, P ) ) THEN
        INFO = -11
      ELSE IF (.NOT. COLMAJOR .AND. LDX11 .LT. MAX( 1, Q ) ) THEN
        INFO = -11
      ELSE IF (COLMAJOR .AND. LDX12 .LT. MAX( 1, P ) ) THEN
        INFO = -13
      ELSE IF (.NOT. COLMAJOR .AND. LDX12 .LT. MAX( 1, M-Q ) ) THEN
        INFO = -13
      ELSE IF (COLMAJOR .AND. LDX21 .LT. MAX( 1, M-P ) ) THEN
        INFO = -15
      ELSE IF (.NOT. COLMAJOR .AND. LDX21 .LT. MAX( 1, Q ) ) THEN
        INFO = -15
      ELSE IF (COLMAJOR .AND. LDX22 .LT. MAX( 1, M-P ) ) THEN
        INFO = -17
      ELSE IF (.NOT. COLMAJOR .AND. LDX22 .LT. MAX( 1, M-Q ) ) THEN
        INFO = -17
      ELSE IF( WANTU1 .AND. LDU1 .LT. P ) THEN
         INFO = -20
      ELSE IF( WANTU2 .AND. LDU2 .LT. M-P ) THEN
         INFO = -22
      ELSE IF( WANTV1T .AND. LDV1T .LT. Q ) THEN
         INFO = -24
      ELSE IF( WANTV2T .AND. LDV2T .LT. M-Q ) THEN
         INFO = -26
      END IF
*
*     Work with transpose if convenient
*
      IF( INFO .EQ. 0 .AND. MIN( P, M-P ) .LT. MIN( Q, M-Q ) ) THEN
         IF( COLMAJOR ) THEN
            TRANST = 'T'
         ELSE
            TRANST = 'N'
         END IF
         IF( DEFAULTSIGNS ) THEN
            SIGNST = 'O'
         ELSE
            SIGNST = 'D'
         END IF
         CALL DORCSD( JOBV1T, JOBV2T, JOBU1, JOBU2, TRANST, SIGNST, M,
     $                Q, P, X11, LDX11, X21, LDX21, X12, LDX12, X22,
     $                LDX22, THETA, V1T, LDV1T, V2T, LDV2T, U1, LDU1,
     $                U2, LDU2, WORK, LWORK, IWORK, INFO )
         RETURN
      END IF
*
*     Work with permutation [ 0 I; I 0 ] * X * [ 0 I; I 0 ] if
*     convenient
*
      IF( INFO .EQ. 0 .AND. M-Q .LT. Q ) THEN
         IF( DEFAULTSIGNS ) THEN
            SIGNST = 'O'
         ELSE
            SIGNST = 'D'
         END IF
         CALL DORCSD( JOBU2, JOBU1, JOBV2T, JOBV1T, TRANS, SIGNST, M,
     $                M-P, M-Q, X22, LDX22, X21, LDX21, X12, LDX12, X11,
     $                LDX11, THETA, U2, LDU2, U1, LDU1, V2T, LDV2T, V1T,
     $                LDV1T, WORK, LWORK, IWORK, INFO )
         RETURN
      END IF
*
*     Compute workspace
*
      IF( INFO .EQ. 0 ) THEN
*
         IPHI = 2
         ITAUP1 = IPHI + MAX( 1, Q - 1 )
         ITAUP2 = ITAUP1 + MAX( 1, P )
         ITAUQ1 = ITAUP2 + MAX( 1, M - P )
         ITAUQ2 = ITAUQ1 + MAX( 1, Q )
         IORGQR = ITAUQ2 + MAX( 1, M - Q )
         CALL DORGQR( M-Q, M-Q, M-Q, U1, MAX(1,M-Q), U1, WORK, -1,
     $                CHILDINFO )
         LORGQRWORKOPT = INT( WORK(1) )
         LORGQRWORKMIN = MAX( 1, M - Q )
         IORGLQ = ITAUQ2 + MAX( 1, M - Q )
         CALL DORGLQ( M-Q, M-Q, M-Q, U1, MAX(1,M-Q), U1, WORK, -1,
     $                CHILDINFO )
         LORGLQWORKOPT = INT( WORK(1) )
         LORGLQWORKMIN = MAX( 1, M - Q )
         IORBDB = ITAUQ2 + MAX( 1, M - Q )
         CALL DORBDB( TRANS, SIGNS, M, P, Q, X11, LDX11, X12, LDX12,
     $                X21, LDX21, X22, LDX22, THETA, V1T, U1, U2, V1T,
     $                V2T, WORK, -1, CHILDINFO )
         LORBDBWORKOPT = INT( WORK(1) )
         LORBDBWORKMIN = LORBDBWORKOPT
         IB11D = ITAUQ2 + MAX( 1, M - Q )
         IB11E = IB11D + MAX( 1, Q )
         IB12D = IB11E + MAX( 1, Q - 1 )
         IB12E = IB12D + MAX( 1, Q )
         IB21D = IB12E + MAX( 1, Q - 1 )
         IB21E = IB21D + MAX( 1, Q )
         IB22D = IB21E + MAX( 1, Q - 1 )
         IB22E = IB22D + MAX( 1, Q )
         IBBCSD = IB22E + MAX( 1, Q - 1 )
         CALL DBBCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS, M, P, Q,
     $                THETA, THETA, U1, LDU1, U2, LDU2, V1T, LDV1T, V2T,
     $                LDV2T, U1, U1, U1, U1, U1, U1, U1, U1, WORK, -1,
     $                CHILDINFO )
         LBBCSDWORKOPT = INT( WORK(1) )
         LBBCSDWORKMIN = LBBCSDWORKOPT
         LWORKOPT = MAX( IORGQR + LORGQRWORKOPT, IORGLQ + LORGLQWORKOPT,
     $              IORBDB + LORBDBWORKOPT, IBBCSD + LBBCSDWORKOPT ) - 1
         LWORKMIN = MAX( IORGQR + LORGQRWORKMIN, IORGLQ + LORGLQWORKMIN,
     $              IORBDB + LORBDBWORKOPT, IBBCSD + LBBCSDWORKMIN ) - 1
         WORK(1) = MAX(LWORKOPT,LWORKMIN)
*
         IF( LWORK .LT. LWORKMIN .AND. .NOT. LQUERY ) THEN
            INFO = -22
         ELSE
            LORGQRWORK = LWORK - IORGQR + 1
            LORGLQWORK = LWORK - IORGLQ + 1
            LORBDBWORK = LWORK - IORBDB + 1
            LBBCSDWORK = LWORK - IBBCSD + 1
         END IF
      END IF
*
*     Abort if any illegal arguments
*
      IF( INFO .NE. 0 ) THEN
         CALL XERBLA( 'DORCSD', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Transform to bidiagonal block form
*
      CALL DORBDB( TRANS, SIGNS, M, P, Q, X11, LDX11, X12, LDX12, X21,
     $             LDX21, X22, LDX22, THETA, WORK(IPHI), WORK(ITAUP1),
     $             WORK(ITAUP2), WORK(ITAUQ1), WORK(ITAUQ2),
     $             WORK(IORBDB), LORBDBWORK, CHILDINFO )
*
*     Accumulate Householder reflectors
*
      IF( COLMAJOR ) THEN
         IF( WANTU1 .AND. P .GT. 0 ) THEN
            CALL DLACPY( 'L', P, Q, X11, LDX11, U1, LDU1 )
            CALL DORGQR( P, P, Q, U1, LDU1, WORK(ITAUP1), WORK(IORGQR),
     $                   LORGQRWORK, INFO)
         END IF
         IF( WANTU2 .AND. M-P .GT. 0 ) THEN
            CALL DLACPY( 'L', M-P, Q, X21, LDX21, U2, LDU2 )
            CALL DORGQR( M-P, M-P, Q, U2, LDU2, WORK(ITAUP2),
     $                   WORK(IORGQR), LORGQRWORK, INFO )
         END IF
         IF( WANTV1T .AND. Q .GT. 0 ) THEN
            CALL DLACPY( 'U', Q-1, Q-1, X11(1,2), LDX11, V1T(2,2),
     $                   LDV1T )
            V1T(1, 1) = ONE
            DO J = 2, Q
               V1T(1,J) = ZERO
               V1T(J,1) = ZERO
            END DO
            CALL DORGLQ( Q-1, Q-1, Q-1, V1T(2,2), LDV1T, WORK(ITAUQ1),
     $                   WORK(IORGLQ), LORGLQWORK, INFO )
         END IF
         IF( WANTV2T .AND. M-Q .GT. 0 ) THEN
            CALL DLACPY( 'U', P, M-Q, X12, LDX12, V2T, LDV2T )
            IF (M-P .GT. Q) Then
               CALL DLACPY( 'U', M-P-Q, M-P-Q, X22(Q+1,P+1), LDX22,
     $                      V2T(P+1,P+1), LDV2T )
            END IF
            IF (M .GT. Q) THEN
               CALL DORGLQ( M-Q, M-Q, M-Q, V2T, LDV2T, WORK(ITAUQ2),
     $                      WORK(IORGLQ), LORGLQWORK, INFO )
            END IF
         END IF
      ELSE
         IF( WANTU1 .AND. P .GT. 0 ) THEN
            CALL DLACPY( 'U', Q, P, X11, LDX11, U1, LDU1 )
            CALL DORGLQ( P, P, Q, U1, LDU1, WORK(ITAUP1), WORK(IORGLQ),
     $                   LORGLQWORK, INFO)
         END IF
         IF( WANTU2 .AND. M-P .GT. 0 ) THEN
            CALL DLACPY( 'U', Q, M-P, X21, LDX21, U2, LDU2 )
            CALL DORGLQ( M-P, M-P, Q, U2, LDU2, WORK(ITAUP2),
     $                   WORK(IORGLQ), LORGLQWORK, INFO )
         END IF
         IF( WANTV1T .AND. Q .GT. 0 ) THEN
            CALL DLACPY( 'L', Q-1, Q-1, X11(2,1), LDX11, V1T(2,2),
     $                   LDV1T )
            V1T(1, 1) = ONE
            DO J = 2, Q
               V1T(1,J) = ZERO
               V1T(J,1) = ZERO
            END DO
            CALL DORGQR( Q-1, Q-1, Q-1, V1T(2,2), LDV1T, WORK(ITAUQ1),
     $                   WORK(IORGQR), LORGQRWORK, INFO )
         END IF
         IF( WANTV2T .AND. M-Q .GT. 0 ) THEN
            CALL DLACPY( 'L', M-Q, P, X12, LDX12, V2T, LDV2T )
            CALL DLACPY( 'L', M-P-Q, M-P-Q, X22(P+1,Q+1), LDX22,
     $                   V2T(P+1,P+1), LDV2T )
            CALL DORGQR( M-Q, M-Q, M-Q, V2T, LDV2T, WORK(ITAUQ2),
     $                   WORK(IORGQR), LORGQRWORK, INFO )
         END IF
      END IF
*
*     Compute the CSD of the matrix in bidiagonal-block form
*
      CALL DBBCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS, M, P, Q, THETA,
     $             WORK(IPHI), U1, LDU1, U2, LDU2, V1T, LDV1T, V2T,
     $             LDV2T, WORK(IB11D), WORK(IB11E), WORK(IB12D),
     $             WORK(IB12E), WORK(IB21D), WORK(IB21E), WORK(IB22D),
     $             WORK(IB22E), WORK(IBBCSD), LBBCSDWORK, INFO )
*
*     Permute rows and columns to place identity submatrices in top-
*     left corner of (1,1)-block and/or bottom-right corner of (1,2)-
*     block and/or bottom-right corner of (2,1)-block and/or top-left
*     corner of (2,2)-block
*
      IF( Q .GT. 0 .AND. WANTU2 ) THEN
         DO I = 1, Q
            IWORK(I) = M - P - Q + I
         END DO
         DO I = Q + 1, M - P
            IWORK(I) = I - Q
         END DO
         IF( COLMAJOR ) THEN
            CALL DLAPMT( .FALSE., M-P, M-P, U2, LDU2, IWORK )
         ELSE
            CALL DLAPMR( .FALSE., M-P, M-P, U2, LDU2, IWORK )
         END IF
      END IF
      IF( M .GT. 0 .AND. WANTV2T ) THEN
         DO I = 1, P
            IWORK(I) = M - P - Q + I
         END DO
         DO I = P + 1, M - Q
            IWORK(I) = I - P
         END DO
         IF( .NOT. COLMAJOR ) THEN
            CALL DLAPMT( .FALSE., M-Q, M-Q, V2T, LDV2T, IWORK )
         ELSE
            CALL DLAPMR( .FALSE., M-Q, M-Q, V2T, LDV2T, IWORK )
         END IF
      END IF
*
      RETURN
*
*     End DORCSD
*
      END

