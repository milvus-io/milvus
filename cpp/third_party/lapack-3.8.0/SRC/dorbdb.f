*> \brief \b DORBDB
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DORBDB + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dorbdb.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dorbdb.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dorbdb.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DORBDB( TRANS, SIGNS, M, P, Q, X11, LDX11, X12, LDX12,
*                          X21, LDX21, X22, LDX22, THETA, PHI, TAUP1,
*                          TAUP2, TAUQ1, TAUQ2, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          SIGNS, TRANS
*       INTEGER            INFO, LDX11, LDX12, LDX21, LDX22, LWORK, M, P,
*      $                   Q
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   PHI( * ), THETA( * )
*       DOUBLE PRECISION   TAUP1( * ), TAUP2( * ), TAUQ1( * ), TAUQ2( * ),
*      $                   WORK( * ), X11( LDX11, * ), X12( LDX12, * ),
*      $                   X21( LDX21, * ), X22( LDX22, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DORBDB simultaneously bidiagonalizes the blocks of an M-by-M
*> partitioned orthogonal matrix X:
*>
*>                                 [ B11 | B12 0  0 ]
*>     [ X11 | X12 ]   [ P1 |    ] [  0  |  0 -I  0 ] [ Q1 |    ]**T
*> X = [-----------] = [---------] [----------------] [---------]   .
*>     [ X21 | X22 ]   [    | P2 ] [ B21 | B22 0  0 ] [    | Q2 ]
*>                                 [  0  |  0  0  I ]
*>
*> X11 is P-by-Q. Q must be no larger than P, M-P, or M-Q. (If this is
*> not the case, then X must be transposed and/or permuted. This can be
*> done in constant time using the TRANS and SIGNS options. See DORCSD
*> for details.)
*>
*> The orthogonal matrices P1, P2, Q1, and Q2 are P-by-P, (M-P)-by-
*> (M-P), Q-by-Q, and (M-Q)-by-(M-Q), respectively. They are
*> represented implicitly by Householder vectors.
*>
*> B11, B12, B21, and B22 are Q-by-Q bidiagonal matrices represented
*> implicitly by angles THETA, PHI.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
*>          The number of columns in X11 and X21. 0 <= Q <=
*>          MIN(P,M-P,M-Q).
*> \endverbatim
*>
*> \param[in,out] X11
*> \verbatim
*>          X11 is DOUBLE PRECISION array, dimension (LDX11,Q)
*>          On entry, the top-left block of the orthogonal matrix to be
*>          reduced. On exit, the form depends on TRANS:
*>          If TRANS = 'N', then
*>             the columns of tril(X11) specify reflectors for P1,
*>             the rows of triu(X11,1) specify reflectors for Q1;
*>          else TRANS = 'T', and
*>             the rows of triu(X11) specify reflectors for P1,
*>             the columns of tril(X11,-1) specify reflectors for Q1.
*> \endverbatim
*>
*> \param[in] LDX11
*> \verbatim
*>          LDX11 is INTEGER
*>          The leading dimension of X11. If TRANS = 'N', then LDX11 >=
*>          P; else LDX11 >= Q.
*> \endverbatim
*>
*> \param[in,out] X12
*> \verbatim
*>          X12 is DOUBLE PRECISION array, dimension (LDX12,M-Q)
*>          On entry, the top-right block of the orthogonal matrix to
*>          be reduced. On exit, the form depends on TRANS:
*>          If TRANS = 'N', then
*>             the rows of triu(X12) specify the first P reflectors for
*>             Q2;
*>          else TRANS = 'T', and
*>             the columns of tril(X12) specify the first P reflectors
*>             for Q2.
*> \endverbatim
*>
*> \param[in] LDX12
*> \verbatim
*>          LDX12 is INTEGER
*>          The leading dimension of X12. If TRANS = 'N', then LDX12 >=
*>          P; else LDX11 >= M-Q.
*> \endverbatim
*>
*> \param[in,out] X21
*> \verbatim
*>          X21 is DOUBLE PRECISION array, dimension (LDX21,Q)
*>          On entry, the bottom-left block of the orthogonal matrix to
*>          be reduced. On exit, the form depends on TRANS:
*>          If TRANS = 'N', then
*>             the columns of tril(X21) specify reflectors for P2;
*>          else TRANS = 'T', and
*>             the rows of triu(X21) specify reflectors for P2.
*> \endverbatim
*>
*> \param[in] LDX21
*> \verbatim
*>          LDX21 is INTEGER
*>          The leading dimension of X21. If TRANS = 'N', then LDX21 >=
*>          M-P; else LDX21 >= Q.
*> \endverbatim
*>
*> \param[in,out] X22
*> \verbatim
*>          X22 is DOUBLE PRECISION array, dimension (LDX22,M-Q)
*>          On entry, the bottom-right block of the orthogonal matrix to
*>          be reduced. On exit, the form depends on TRANS:
*>          If TRANS = 'N', then
*>             the rows of triu(X22(Q+1:M-P,P+1:M-Q)) specify the last
*>             M-P-Q reflectors for Q2,
*>          else TRANS = 'T', and
*>             the columns of tril(X22(P+1:M-Q,Q+1:M-P)) specify the last
*>             M-P-Q reflectors for P2.
*> \endverbatim
*>
*> \param[in] LDX22
*> \verbatim
*>          LDX22 is INTEGER
*>          The leading dimension of X22. If TRANS = 'N', then LDX22 >=
*>          M-P; else LDX22 >= M-Q.
*> \endverbatim
*>
*> \param[out] THETA
*> \verbatim
*>          THETA is DOUBLE PRECISION array, dimension (Q)
*>          The entries of the bidiagonal blocks B11, B12, B21, B22 can
*>          be computed from the angles THETA and PHI. See Further
*>          Details.
*> \endverbatim
*>
*> \param[out] PHI
*> \verbatim
*>          PHI is DOUBLE PRECISION array, dimension (Q-1)
*>          The entries of the bidiagonal blocks B11, B12, B21, B22 can
*>          be computed from the angles THETA and PHI. See Further
*>          Details.
*> \endverbatim
*>
*> \param[out] TAUP1
*> \verbatim
*>          TAUP1 is DOUBLE PRECISION array, dimension (P)
*>          The scalar factors of the elementary reflectors that define
*>          P1.
*> \endverbatim
*>
*> \param[out] TAUP2
*> \verbatim
*>          TAUP2 is DOUBLE PRECISION array, dimension (M-P)
*>          The scalar factors of the elementary reflectors that define
*>          P2.
*> \endverbatim
*>
*> \param[out] TAUQ1
*> \verbatim
*>          TAUQ1 is DOUBLE PRECISION array, dimension (Q)
*>          The scalar factors of the elementary reflectors that define
*>          Q1.
*> \endverbatim
*>
*> \param[out] TAUQ2
*> \verbatim
*>          TAUQ2 is DOUBLE PRECISION array, dimension (M-Q)
*>          The scalar factors of the elementary reflectors that define
*>          Q2.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= M-Q.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \ingroup doubleOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The bidiagonal blocks B11, B12, B21, and B22 are represented
*>  implicitly by angles THETA(1), ..., THETA(Q) and PHI(1), ...,
*>  PHI(Q-1). B11 and B21 are upper bidiagonal, while B21 and B22 are
*>  lower bidiagonal. Every entry in each bidiagonal band is a product
*>  of a sine or cosine of a THETA with a sine or cosine of a PHI. See
*>  [1] or DORCSD for details.
*>
*>  P1, P2, Q1, and Q2 are represented as products of elementary
*>  reflectors. See DORCSD for details on generating P1, P2, Q1, and Q2
*>  using DORGQR and DORGLQ.
*> \endverbatim
*
*> \par References:
*  ================
*>
*>  [1] Brian D. Sutton. Computing the complete CS decomposition. Numer.
*>      Algorithms, 50(1):33-65, 2009.
*>
*  =====================================================================
      SUBROUTINE DORBDB( TRANS, SIGNS, M, P, Q, X11, LDX11, X12, LDX12,
     $                   X21, LDX21, X22, LDX22, THETA, PHI, TAUP1,
     $                   TAUP2, TAUQ1, TAUQ2, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          SIGNS, TRANS
      INTEGER            INFO, LDX11, LDX12, LDX21, LDX22, LWORK, M, P,
     $                   Q
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   PHI( * ), THETA( * )
      DOUBLE PRECISION   TAUP1( * ), TAUP2( * ), TAUQ1( * ), TAUQ2( * ),
     $                   WORK( * ), X11( LDX11, * ), X12( LDX12, * ),
     $                   X21( LDX21, * ), X22( LDX22, * )
*     ..
*
*  ====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   REALONE
      PARAMETER          ( REALONE = 1.0D0 )
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            COLMAJOR, LQUERY
      INTEGER            I, LWORKMIN, LWORKOPT
      DOUBLE PRECISION   Z1, Z2, Z3, Z4
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DLARF, DLARFGP, DSCAL, XERBLA
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DNRM2
      LOGICAL            LSAME
      EXTERNAL           DNRM2, LSAME
*     ..
*     .. Intrinsic Functions
      INTRINSIC          ATAN2, COS, MAX, SIN
*     ..
*     .. Executable Statements ..
*
*     Test input arguments
*
      INFO = 0
      COLMAJOR = .NOT. LSAME( TRANS, 'T' )
      IF( .NOT. LSAME( SIGNS, 'O' ) ) THEN
         Z1 = REALONE
         Z2 = REALONE
         Z3 = REALONE
         Z4 = REALONE
      ELSE
         Z1 = REALONE
         Z2 = -REALONE
         Z3 = REALONE
         Z4 = -REALONE
      END IF
      LQUERY = LWORK .EQ. -1
*
      IF( M .LT. 0 ) THEN
         INFO = -3
      ELSE IF( P .LT. 0 .OR. P .GT. M ) THEN
         INFO = -4
      ELSE IF( Q .LT. 0 .OR. Q .GT. P .OR. Q .GT. M-P .OR.
     $         Q .GT. M-Q ) THEN
         INFO = -5
      ELSE IF( COLMAJOR .AND. LDX11 .LT. MAX( 1, P ) ) THEN
         INFO = -7
      ELSE IF( .NOT.COLMAJOR .AND. LDX11 .LT. MAX( 1, Q ) ) THEN
         INFO = -7
      ELSE IF( COLMAJOR .AND. LDX12 .LT. MAX( 1, P ) ) THEN
         INFO = -9
      ELSE IF( .NOT.COLMAJOR .AND. LDX12 .LT. MAX( 1, M-Q ) ) THEN
         INFO = -9
      ELSE IF( COLMAJOR .AND. LDX21 .LT. MAX( 1, M-P ) ) THEN
         INFO = -11
      ELSE IF( .NOT.COLMAJOR .AND. LDX21 .LT. MAX( 1, Q ) ) THEN
         INFO = -11
      ELSE IF( COLMAJOR .AND. LDX22 .LT. MAX( 1, M-P ) ) THEN
         INFO = -13
      ELSE IF( .NOT.COLMAJOR .AND. LDX22 .LT. MAX( 1, M-Q ) ) THEN
         INFO = -13
      END IF
*
*     Compute workspace
*
      IF( INFO .EQ. 0 ) THEN
         LWORKOPT = M - Q
         LWORKMIN = M - Q
         WORK(1) = LWORKOPT
         IF( LWORK .LT. LWORKMIN .AND. .NOT. LQUERY ) THEN
            INFO = -21
         END IF
      END IF
      IF( INFO .NE. 0 ) THEN
         CALL XERBLA( 'xORBDB', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Handle column-major and row-major separately
*
      IF( COLMAJOR ) THEN
*
*        Reduce columns 1, ..., Q of X11, X12, X21, and X22
*
         DO I = 1, Q
*
            IF( I .EQ. 1 ) THEN
               CALL DSCAL( P-I+1, Z1, X11(I,I), 1 )
            ELSE
               CALL DSCAL( P-I+1, Z1*COS(PHI(I-1)), X11(I,I), 1 )
               CALL DAXPY( P-I+1, -Z1*Z3*Z4*SIN(PHI(I-1)), X12(I,I-1),
     $                     1, X11(I,I), 1 )
            END IF
            IF( I .EQ. 1 ) THEN
               CALL DSCAL( M-P-I+1, Z2, X21(I,I), 1 )
            ELSE
               CALL DSCAL( M-P-I+1, Z2*COS(PHI(I-1)), X21(I,I), 1 )
               CALL DAXPY( M-P-I+1, -Z2*Z3*Z4*SIN(PHI(I-1)), X22(I,I-1),
     $                     1, X21(I,I), 1 )
            END IF
*
            THETA(I) = ATAN2( DNRM2( M-P-I+1, X21(I,I), 1 ),
     $                 DNRM2( P-I+1, X11(I,I), 1 ) )
*
            IF( P .GT. I ) THEN
               CALL DLARFGP( P-I+1, X11(I,I), X11(I+1,I), 1, TAUP1(I) )
            ELSE IF( P .EQ. I ) THEN
               CALL DLARFGP( P-I+1, X11(I,I), X11(I,I), 1, TAUP1(I) )
            END IF
            X11(I,I) = ONE
            IF ( M-P .GT. I ) THEN
               CALL DLARFGP( M-P-I+1, X21(I,I), X21(I+1,I), 1,
     $                       TAUP2(I) )
            ELSE IF ( M-P .EQ. I ) THEN
               CALL DLARFGP( M-P-I+1, X21(I,I), X21(I,I), 1, TAUP2(I) )
            END IF
            X21(I,I) = ONE
*
            IF ( Q .GT. I ) THEN
               CALL DLARF( 'L', P-I+1, Q-I, X11(I,I), 1, TAUP1(I),
     $                     X11(I,I+1), LDX11, WORK )
            END IF
            IF ( M-Q+1 .GT. I ) THEN
               CALL DLARF( 'L', P-I+1, M-Q-I+1, X11(I,I), 1, TAUP1(I),
     $                     X12(I,I), LDX12, WORK )
            END IF
            IF ( Q .GT. I ) THEN
               CALL DLARF( 'L', M-P-I+1, Q-I, X21(I,I), 1, TAUP2(I),
     $                     X21(I,I+1), LDX21, WORK )
            END IF
            IF ( M-Q+1 .GT. I ) THEN
               CALL DLARF( 'L', M-P-I+1, M-Q-I+1, X21(I,I), 1, TAUP2(I),
     $                     X22(I,I), LDX22, WORK )
            END IF
*
            IF( I .LT. Q ) THEN
               CALL DSCAL( Q-I, -Z1*Z3*SIN(THETA(I)), X11(I,I+1),
     $                     LDX11 )
               CALL DAXPY( Q-I, Z2*Z3*COS(THETA(I)), X21(I,I+1), LDX21,
     $                     X11(I,I+1), LDX11 )
            END IF
            CALL DSCAL( M-Q-I+1, -Z1*Z4*SIN(THETA(I)), X12(I,I), LDX12 )
            CALL DAXPY( M-Q-I+1, Z2*Z4*COS(THETA(I)), X22(I,I), LDX22,
     $                  X12(I,I), LDX12 )
*
            IF( I .LT. Q )
     $         PHI(I) = ATAN2( DNRM2( Q-I, X11(I,I+1), LDX11 ),
     $                  DNRM2( M-Q-I+1, X12(I,I), LDX12 ) )
*
            IF( I .LT. Q ) THEN
               IF ( Q-I .EQ. 1 ) THEN
                  CALL DLARFGP( Q-I, X11(I,I+1), X11(I,I+1), LDX11,
     $                          TAUQ1(I) )
               ELSE
                  CALL DLARFGP( Q-I, X11(I,I+1), X11(I,I+2), LDX11,
     $                          TAUQ1(I) )
               END IF
               X11(I,I+1) = ONE
            END IF
            IF ( Q+I-1 .LT. M ) THEN
               IF ( M-Q .EQ. I ) THEN
                  CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I,I), LDX12,
     $                          TAUQ2(I) )
               ELSE
                  CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I,I+1), LDX12,
     $                          TAUQ2(I) )
               END IF
            END IF
            X12(I,I) = ONE
*
            IF( I .LT. Q ) THEN
               CALL DLARF( 'R', P-I, Q-I, X11(I,I+1), LDX11, TAUQ1(I),
     $                     X11(I+1,I+1), LDX11, WORK )
               CALL DLARF( 'R', M-P-I, Q-I, X11(I,I+1), LDX11, TAUQ1(I),
     $                     X21(I+1,I+1), LDX21, WORK )
            END IF
            IF ( P .GT. I ) THEN
               CALL DLARF( 'R', P-I, M-Q-I+1, X12(I,I), LDX12, TAUQ2(I),
     $                     X12(I+1,I), LDX12, WORK )
            END IF
            IF ( M-P .GT. I ) THEN
               CALL DLARF( 'R', M-P-I, M-Q-I+1, X12(I,I), LDX12,
     $                     TAUQ2(I), X22(I+1,I), LDX22, WORK )
            END IF
*
         END DO
*
*        Reduce columns Q + 1, ..., P of X12, X22
*
         DO I = Q + 1, P
*
            CALL DSCAL( M-Q-I+1, -Z1*Z4, X12(I,I), LDX12 )
            IF ( I .GE. M-Q ) THEN
               CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I,I), LDX12,
     $                       TAUQ2(I) )
            ELSE
               CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I,I+1), LDX12,
     $                       TAUQ2(I) )
            END IF
            X12(I,I) = ONE
*
            IF ( P .GT. I ) THEN
               CALL DLARF( 'R', P-I, M-Q-I+1, X12(I,I), LDX12, TAUQ2(I),
     $                     X12(I+1,I), LDX12, WORK )
            END IF
            IF( M-P-Q .GE. 1 )
     $         CALL DLARF( 'R', M-P-Q, M-Q-I+1, X12(I,I), LDX12,
     $                     TAUQ2(I), X22(Q+1,I), LDX22, WORK )
*
         END DO
*
*        Reduce columns P + 1, ..., M - Q of X12, X22
*
         DO I = 1, M - P - Q
*
            CALL DSCAL( M-P-Q-I+1, Z2*Z4, X22(Q+I,P+I), LDX22 )
            IF ( I .EQ. M-P-Q ) THEN
               CALL DLARFGP( M-P-Q-I+1, X22(Q+I,P+I), X22(Q+I,P+I),
     $                       LDX22, TAUQ2(P+I) )
            ELSE
               CALL DLARFGP( M-P-Q-I+1, X22(Q+I,P+I), X22(Q+I,P+I+1),
     $                       LDX22, TAUQ2(P+I) )
            END IF
            X22(Q+I,P+I) = ONE
            IF ( I .LT. M-P-Q ) THEN
               CALL DLARF( 'R', M-P-Q-I, M-P-Q-I+1, X22(Q+I,P+I), LDX22,
     $                     TAUQ2(P+I), X22(Q+I+1,P+I), LDX22, WORK )
            END IF
*
         END DO
*
      ELSE
*
*        Reduce columns 1, ..., Q of X11, X12, X21, X22
*
         DO I = 1, Q
*
            IF( I .EQ. 1 ) THEN
               CALL DSCAL( P-I+1, Z1, X11(I,I), LDX11 )
            ELSE
               CALL DSCAL( P-I+1, Z1*COS(PHI(I-1)), X11(I,I), LDX11 )
               CALL DAXPY( P-I+1, -Z1*Z3*Z4*SIN(PHI(I-1)), X12(I-1,I),
     $                     LDX12, X11(I,I), LDX11 )
            END IF
            IF( I .EQ. 1 ) THEN
               CALL DSCAL( M-P-I+1, Z2, X21(I,I), LDX21 )
            ELSE
               CALL DSCAL( M-P-I+1, Z2*COS(PHI(I-1)), X21(I,I), LDX21 )
               CALL DAXPY( M-P-I+1, -Z2*Z3*Z4*SIN(PHI(I-1)), X22(I-1,I),
     $                     LDX22, X21(I,I), LDX21 )
            END IF
*
            THETA(I) = ATAN2( DNRM2( M-P-I+1, X21(I,I), LDX21 ),
     $                 DNRM2( P-I+1, X11(I,I), LDX11 ) )
*
            CALL DLARFGP( P-I+1, X11(I,I), X11(I,I+1), LDX11, TAUP1(I) )
            X11(I,I) = ONE
            IF ( I .EQ. M-P ) THEN
               CALL DLARFGP( M-P-I+1, X21(I,I), X21(I,I), LDX21,
     $                    TAUP2(I) )
            ELSE
               CALL DLARFGP( M-P-I+1, X21(I,I), X21(I,I+1), LDX21,
     $                    TAUP2(I) )
            END IF
            X21(I,I) = ONE
*
            IF ( Q .GT. I ) THEN
               CALL DLARF( 'R', Q-I, P-I+1, X11(I,I), LDX11, TAUP1(I),
     $                     X11(I+1,I), LDX11, WORK )
            END IF
            IF ( M-Q+1 .GT. I ) THEN
               CALL DLARF( 'R', M-Q-I+1, P-I+1, X11(I,I), LDX11,
     $                     TAUP1(I), X12(I,I), LDX12, WORK )
            END IF
            IF ( Q .GT. I ) THEN
               CALL DLARF( 'R', Q-I, M-P-I+1, X21(I,I), LDX21, TAUP2(I),
     $                     X21(I+1,I), LDX21, WORK )
            END IF
            IF ( M-Q+1 .GT. I ) THEN
               CALL DLARF( 'R', M-Q-I+1, M-P-I+1, X21(I,I), LDX21,
     $                     TAUP2(I), X22(I,I), LDX22, WORK )
            END IF
*
            IF( I .LT. Q ) THEN
               CALL DSCAL( Q-I, -Z1*Z3*SIN(THETA(I)), X11(I+1,I), 1 )
               CALL DAXPY( Q-I, Z2*Z3*COS(THETA(I)), X21(I+1,I), 1,
     $                     X11(I+1,I), 1 )
            END IF
            CALL DSCAL( M-Q-I+1, -Z1*Z4*SIN(THETA(I)), X12(I,I), 1 )
            CALL DAXPY( M-Q-I+1, Z2*Z4*COS(THETA(I)), X22(I,I), 1,
     $                  X12(I,I), 1 )
*
            IF( I .LT. Q )
     $         PHI(I) = ATAN2( DNRM2( Q-I, X11(I+1,I), 1 ),
     $                  DNRM2( M-Q-I+1, X12(I,I), 1 ) )
*
            IF( I .LT. Q ) THEN
               IF ( Q-I .EQ. 1) THEN
                  CALL DLARFGP( Q-I, X11(I+1,I), X11(I+1,I), 1,
     $                          TAUQ1(I) )
               ELSE
                  CALL DLARFGP( Q-I, X11(I+1,I), X11(I+2,I), 1,
     $                          TAUQ1(I) )
               END IF
               X11(I+1,I) = ONE
            END IF
            IF ( M-Q .GT. I ) THEN
               CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I+1,I), 1,
     $                       TAUQ2(I) )
            ELSE
               CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I,I), 1,
     $                       TAUQ2(I) )
            END IF
            X12(I,I) = ONE
*
            IF( I .LT. Q ) THEN
               CALL DLARF( 'L', Q-I, P-I, X11(I+1,I), 1, TAUQ1(I),
     $                     X11(I+1,I+1), LDX11, WORK )
               CALL DLARF( 'L', Q-I, M-P-I, X11(I+1,I), 1, TAUQ1(I),
     $                     X21(I+1,I+1), LDX21, WORK )
            END IF
            CALL DLARF( 'L', M-Q-I+1, P-I, X12(I,I), 1, TAUQ2(I),
     $                  X12(I,I+1), LDX12, WORK )
            IF ( M-P-I .GT. 0 ) THEN
               CALL DLARF( 'L', M-Q-I+1, M-P-I, X12(I,I), 1, TAUQ2(I),
     $                     X22(I,I+1), LDX22, WORK )
            END IF
*
         END DO
*
*        Reduce columns Q + 1, ..., P of X12, X22
*
         DO I = Q + 1, P
*
            CALL DSCAL( M-Q-I+1, -Z1*Z4, X12(I,I), 1 )
            CALL DLARFGP( M-Q-I+1, X12(I,I), X12(I+1,I), 1, TAUQ2(I) )
            X12(I,I) = ONE
*
            IF ( P .GT. I ) THEN
               CALL DLARF( 'L', M-Q-I+1, P-I, X12(I,I), 1, TAUQ2(I),
     $                  X12(I,I+1), LDX12, WORK )
            END IF
            IF( M-P-Q .GE. 1 )
     $         CALL DLARF( 'L', M-Q-I+1, M-P-Q, X12(I,I), 1, TAUQ2(I),
     $                     X22(I,Q+1), LDX22, WORK )
*
         END DO
*
*        Reduce columns P + 1, ..., M - Q of X12, X22
*
         DO I = 1, M - P - Q
*
            CALL DSCAL( M-P-Q-I+1, Z2*Z4, X22(P+I,Q+I), 1 )
            IF ( M-P-Q .EQ. I ) THEN
               CALL DLARFGP( M-P-Q-I+1, X22(P+I,Q+I), X22(P+I,Q+I), 1,
     $                       TAUQ2(P+I) )
            ELSE
               CALL DLARFGP( M-P-Q-I+1, X22(P+I,Q+I), X22(P+I+1,Q+I), 1,
     $                       TAUQ2(P+I) )
               CALL DLARF( 'L', M-P-Q-I+1, M-P-Q-I, X22(P+I,Q+I), 1,
     $                  TAUQ2(P+I), X22(P+I,Q+I+1), LDX22, WORK )
            END IF
            X22(P+I,Q+I) = ONE
*
         END DO
*
      END IF
*
      RETURN
*
*     End of DORBDB
*
      END

