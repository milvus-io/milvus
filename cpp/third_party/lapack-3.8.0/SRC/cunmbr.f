*> \brief \b CUNMBR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CUNMBR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cunmbr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cunmbr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cunmbr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CUNMBR( VECT, SIDE, TRANS, M, N, K, A, LDA, TAU, C,
*                          LDC, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          SIDE, TRANS, VECT
*       INTEGER            INFO, K, LDA, LDC, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), C( LDC, * ), TAU( * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> If VECT = 'Q', CUNMBR overwrites the general complex M-by-N matrix C
*> with
*>                 SIDE = 'L'     SIDE = 'R'
*> TRANS = 'N':      Q * C          C * Q
*> TRANS = 'C':      Q**H * C       C * Q**H
*>
*> If VECT = 'P', CUNMBR overwrites the general complex M-by-N matrix C
*> with
*>                 SIDE = 'L'     SIDE = 'R'
*> TRANS = 'N':      P * C          C * P
*> TRANS = 'C':      P**H * C       C * P**H
*>
*> Here Q and P**H are the unitary matrices determined by CGEBRD when
*> reducing a complex matrix A to bidiagonal form: A = Q * B * P**H. Q
*> and P**H are defined as products of elementary reflectors H(i) and
*> G(i) respectively.
*>
*> Let nq = m if SIDE = 'L' and nq = n if SIDE = 'R'. Thus nq is the
*> order of the unitary matrix Q or P**H that is applied.
*>
*> If VECT = 'Q', A is assumed to have been an NQ-by-K matrix:
*> if nq >= k, Q = H(1) H(2) . . . H(k);
*> if nq < k, Q = H(1) H(2) . . . H(nq-1).
*>
*> If VECT = 'P', A is assumed to have been a K-by-NQ matrix:
*> if k < nq, P = G(1) G(2) . . . G(k);
*> if k >= nq, P = G(1) G(2) . . . G(nq-1).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] VECT
*> \verbatim
*>          VECT is CHARACTER*1
*>          = 'Q': apply Q or Q**H;
*>          = 'P': apply P or P**H.
*> \endverbatim
*>
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'L': apply Q, Q**H, P or P**H from the Left;
*>          = 'R': apply Q, Q**H, P or P**H from the Right.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N':  No transpose, apply Q or P;
*>          = 'C':  Conjugate transpose, apply Q**H or P**H.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix C. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix C. N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          If VECT = 'Q', the number of columns in the original
*>          matrix reduced by CGEBRD.
*>          If VECT = 'P', the number of rows in the original
*>          matrix reduced by CGEBRD.
*>          K >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension
*>                                (LDA,min(nq,K)) if VECT = 'Q'
*>                                (LDA,nq)        if VECT = 'P'
*>          The vectors which define the elementary reflectors H(i) and
*>          G(i), whose products determine the matrices Q and P, as
*>          returned by CGEBRD.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.
*>          If VECT = 'Q', LDA >= max(1,nq);
*>          if VECT = 'P', LDA >= max(1,min(nq,K)).
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is COMPLEX array, dimension (min(nq,K))
*>          TAU(i) must contain the scalar factor of the elementary
*>          reflector H(i) or G(i) which determines Q or P, as returned
*>          by CGEBRD in the array argument TAUQ or TAUP.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is COMPLEX array, dimension (LDC,N)
*>          On entry, the M-by-N matrix C.
*>          On exit, C is overwritten by Q*C or Q**H*C or C*Q**H or C*Q
*>          or P*C or P**H*C or C*P or C*P**H.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of the array C. LDC >= max(1,M).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If SIDE = 'L', LWORK >= max(1,N);
*>          if SIDE = 'R', LWORK >= max(1,M);
*>          if N = 0 or M = 0, LWORK >= 1.
*>          For optimum performance LWORK >= max(1,N*NB) if SIDE = 'L',
*>          and LWORK >= max(1,M*NB) if SIDE = 'R', where NB is the
*>          optimal blocksize. (NB = 0 if M = 0 or N = 0.)
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
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \ingroup complexOTHERcomputational
*
*  =====================================================================
      SUBROUTINE CUNMBR( VECT, SIDE, TRANS, M, N, K, A, LDA, TAU, C,
     $                   LDC, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          SIDE, TRANS, VECT
      INTEGER            INFO, K, LDA, LDC, LWORK, M, N
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), C( LDC, * ), TAU( * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            APPLYQ, LEFT, LQUERY, NOTRAN
      CHARACTER          TRANST
      INTEGER            I1, I2, IINFO, LWKOPT, MI, NB, NI, NQ, NW
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      EXTERNAL           ILAENV, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CUNMLQ, CUNMQR, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      APPLYQ = LSAME( VECT, 'Q' )
      LEFT = LSAME( SIDE, 'L' )
      NOTRAN = LSAME( TRANS, 'N' )
      LQUERY = ( LWORK.EQ.-1 )
*
*     NQ is the order of Q or P and NW is the minimum dimension of WORK
*
      IF( LEFT ) THEN
         NQ = M
         NW = N
      ELSE
         NQ = N
         NW = M
      END IF
      IF( M.EQ.0 .OR. N.EQ.0 ) THEN
         NW = 0
      END IF
      IF( .NOT.APPLYQ .AND. .NOT.LSAME( VECT, 'P' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LEFT .AND. .NOT.LSAME( SIDE, 'R' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'C' ) ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( K.LT.0 ) THEN
         INFO = -6
      ELSE IF( ( APPLYQ .AND. LDA.LT.MAX( 1, NQ ) ) .OR.
     $         ( .NOT.APPLYQ .AND. LDA.LT.MAX( 1, MIN( NQ, K ) ) ) )
     $          THEN
         INFO = -8
      ELSE IF( LDC.LT.MAX( 1, M ) ) THEN
         INFO = -11
      ELSE IF( LWORK.LT.MAX( 1, NW ) .AND. .NOT.LQUERY ) THEN
         INFO = -13
      END IF
*
      IF( INFO.EQ.0 ) THEN
         IF( NW.GT.0 ) THEN
            IF( APPLYQ ) THEN
               IF( LEFT ) THEN
                  NB = ILAENV( 1, 'CUNMQR', SIDE // TRANS, M-1, N, M-1,
     $                         -1 )
               ELSE
                  NB = ILAENV( 1, 'CUNMQR', SIDE // TRANS, M, N-1, N-1,
     $                         -1 )
               END IF
            ELSE
               IF( LEFT ) THEN
                  NB = ILAENV( 1, 'CUNMLQ', SIDE // TRANS, M-1, N, M-1,
     $                         -1 )
               ELSE
                  NB = ILAENV( 1, 'CUNMLQ', SIDE // TRANS, M, N-1, N-1,
     $                         -1 )
               END IF
            END IF
            LWKOPT = MAX( 1, NW*NB )
         ELSE
            LWKOPT = 1
         END IF
         WORK( 1 ) = LWKOPT
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CUNMBR', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 )
     $   RETURN
*
      IF( APPLYQ ) THEN
*
*        Apply Q
*
         IF( NQ.GE.K ) THEN
*
*           Q was determined by a call to CGEBRD with nq >= k
*
            CALL CUNMQR( SIDE, TRANS, M, N, K, A, LDA, TAU, C, LDC,
     $                   WORK, LWORK, IINFO )
         ELSE IF( NQ.GT.1 ) THEN
*
*           Q was determined by a call to CGEBRD with nq < k
*
            IF( LEFT ) THEN
               MI = M - 1
               NI = N
               I1 = 2
               I2 = 1
            ELSE
               MI = M
               NI = N - 1
               I1 = 1
               I2 = 2
            END IF
            CALL CUNMQR( SIDE, TRANS, MI, NI, NQ-1, A( 2, 1 ), LDA, TAU,
     $                   C( I1, I2 ), LDC, WORK, LWORK, IINFO )
         END IF
      ELSE
*
*        Apply P
*
         IF( NOTRAN ) THEN
            TRANST = 'C'
         ELSE
            TRANST = 'N'
         END IF
         IF( NQ.GT.K ) THEN
*
*           P was determined by a call to CGEBRD with nq > k
*
            CALL CUNMLQ( SIDE, TRANST, M, N, K, A, LDA, TAU, C, LDC,
     $                   WORK, LWORK, IINFO )
         ELSE IF( NQ.GT.1 ) THEN
*
*           P was determined by a call to CGEBRD with nq <= k
*
            IF( LEFT ) THEN
               MI = M - 1
               NI = N
               I1 = 2
               I2 = 1
            ELSE
               MI = M
               NI = N - 1
               I1 = 1
               I2 = 2
            END IF
            CALL CUNMLQ( SIDE, TRANST, MI, NI, NQ-1, A( 1, 2 ), LDA,
     $                   TAU, C( I1, I2 ), LDC, WORK, LWORK, IINFO )
         END IF
      END IF
      WORK( 1 ) = LWKOPT
      RETURN
*
*     End of CUNMBR
*
      END
