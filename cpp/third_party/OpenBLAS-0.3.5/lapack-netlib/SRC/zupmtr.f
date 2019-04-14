*> \brief \b ZUPMTR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZUPMTR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zupmtr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zupmtr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zupmtr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZUPMTR( SIDE, UPLO, TRANS, M, N, AP, TAU, C, LDC, WORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          SIDE, TRANS, UPLO
*       INTEGER            INFO, LDC, M, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         AP( * ), C( LDC, * ), TAU( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZUPMTR overwrites the general complex M-by-N matrix C with
*>
*>                 SIDE = 'L'     SIDE = 'R'
*> TRANS = 'N':      Q * C          C * Q
*> TRANS = 'C':      Q**H * C       C * Q**H
*>
*> where Q is a complex unitary matrix of order nq, with nq = m if
*> SIDE = 'L' and nq = n if SIDE = 'R'. Q is defined as the product of
*> nq-1 elementary reflectors, as returned by ZHPTRD using packed
*> storage:
*>
*> if UPLO = 'U', Q = H(nq-1) . . . H(2) H(1);
*>
*> if UPLO = 'L', Q = H(1) H(2) . . . H(nq-1).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'L': apply Q or Q**H from the Left;
*>          = 'R': apply Q or Q**H from the Right.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U': Upper triangular packed storage used in previous
*>                 call to ZHPTRD;
*>          = 'L': Lower triangular packed storage used in previous
*>                 call to ZHPTRD.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N':  No transpose, apply Q;
*>          = 'C':  Conjugate transpose, apply Q**H.
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
*> \param[in] AP
*> \verbatim
*>          AP is COMPLEX*16 array, dimension
*>                               (M*(M+1)/2) if SIDE = 'L'
*>                               (N*(N+1)/2) if SIDE = 'R'
*>          The vectors which define the elementary reflectors, as
*>          returned by ZHPTRD.  AP is modified by the routine but
*>          restored on exit.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array, dimension (M-1) if SIDE = 'L'
*>                                     or (N-1) if SIDE = 'R'
*>          TAU(i) must contain the scalar factor of the elementary
*>          reflector H(i), as returned by ZHPTRD.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is COMPLEX*16 array, dimension (LDC,N)
*>          On entry, the M-by-N matrix C.
*>          On exit, C is overwritten by Q*C or Q**H*C or C*Q**H or C*Q.
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
*>          WORK is COMPLEX*16 array, dimension
*>                                   (N) if SIDE = 'L'
*>                                   (M) if SIDE = 'R'
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
*> \ingroup complex16OTHERcomputational
*
*  =====================================================================
      SUBROUTINE ZUPMTR( SIDE, UPLO, TRANS, M, N, AP, TAU, C, LDC, WORK,
     $                   INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          SIDE, TRANS, UPLO
      INTEGER            INFO, LDC, M, N
*     ..
*     .. Array Arguments ..
      COMPLEX*16         AP( * ), C( LDC, * ), TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         ONE
      PARAMETER          ( ONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            FORWRD, LEFT, NOTRAN, UPPER
      INTEGER            I, I1, I2, I3, IC, II, JC, MI, NI, NQ
      COMPLEX*16         AII, TAUI
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZLARF
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCONJG, MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      LEFT = LSAME( SIDE, 'L' )
      NOTRAN = LSAME( TRANS, 'N' )
      UPPER = LSAME( UPLO, 'U' )
*
*     NQ is the order of Q
*
      IF( LEFT ) THEN
         NQ = M
      ELSE
         NQ = N
      END IF
      IF( .NOT.LEFT .AND. .NOT.LSAME( SIDE, 'R' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'C' ) ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDC.LT.MAX( 1, M ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZUPMTR', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 )
     $   RETURN
*
      IF( UPPER ) THEN
*
*        Q was determined by a call to ZHPTRD with UPLO = 'U'
*
         FORWRD = ( LEFT .AND. NOTRAN ) .OR.
     $            ( .NOT.LEFT .AND. .NOT.NOTRAN )
*
         IF( FORWRD ) THEN
            I1 = 1
            I2 = NQ - 1
            I3 = 1
            II = 2
         ELSE
            I1 = NQ - 1
            I2 = 1
            I3 = -1
            II = NQ*( NQ+1 ) / 2 - 1
         END IF
*
         IF( LEFT ) THEN
            NI = N
         ELSE
            MI = M
         END IF
*
         DO 10 I = I1, I2, I3
            IF( LEFT ) THEN
*
*              H(i) or H(i)**H is applied to C(1:i,1:n)
*
               MI = I
            ELSE
*
*              H(i) or H(i)**H is applied to C(1:m,1:i)
*
               NI = I
            END IF
*
*           Apply H(i) or H(i)**H
*
            IF( NOTRAN ) THEN
               TAUI = TAU( I )
            ELSE
               TAUI = DCONJG( TAU( I ) )
            END IF
            AII = AP( II )
            AP( II ) = ONE
            CALL ZLARF( SIDE, MI, NI, AP( II-I+1 ), 1, TAUI, C, LDC,
     $                  WORK )
            AP( II ) = AII
*
            IF( FORWRD ) THEN
               II = II + I + 2
            ELSE
               II = II - I - 1
            END IF
   10    CONTINUE
      ELSE
*
*        Q was determined by a call to ZHPTRD with UPLO = 'L'.
*
         FORWRD = ( LEFT .AND. .NOT.NOTRAN ) .OR.
     $            ( .NOT.LEFT .AND. NOTRAN )
*
         IF( FORWRD ) THEN
            I1 = 1
            I2 = NQ - 1
            I3 = 1
            II = 2
         ELSE
            I1 = NQ - 1
            I2 = 1
            I3 = -1
            II = NQ*( NQ+1 ) / 2 - 1
         END IF
*
         IF( LEFT ) THEN
            NI = N
            JC = 1
         ELSE
            MI = M
            IC = 1
         END IF
*
         DO 20 I = I1, I2, I3
            AII = AP( II )
            AP( II ) = ONE
            IF( LEFT ) THEN
*
*              H(i) or H(i)**H is applied to C(i+1:m,1:n)
*
               MI = M - I
               IC = I + 1
            ELSE
*
*              H(i) or H(i)**H is applied to C(1:m,i+1:n)
*
               NI = N - I
               JC = I + 1
            END IF
*
*           Apply H(i) or H(i)**H
*
            IF( NOTRAN ) THEN
               TAUI = TAU( I )
            ELSE
               TAUI = DCONJG( TAU( I ) )
            END IF
            CALL ZLARF( SIDE, MI, NI, AP( II ), 1, TAUI, C( IC, JC ),
     $                  LDC, WORK )
            AP( II ) = AII
*
            IF( FORWRD ) THEN
               II = II + NQ - I + 1
            ELSE
               II = II - NQ + I - 2
            END IF
   20    CONTINUE
      END IF
      RETURN
*
*     End of ZUPMTR
*
      END
