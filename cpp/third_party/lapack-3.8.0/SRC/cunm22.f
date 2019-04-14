*> \brief \b CUNM22 multiplies a general matrix by a banded unitary matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CUNM22 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cunm22.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cunm22.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cunm22.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*     SUBROUTINE CUNM22( SIDE, TRANS, M, N, N1, N2, Q, LDQ, C, LDC,
*    $                   WORK, LWORK, INFO )
*
*     .. Scalar Arguments ..
*     CHARACTER          SIDE, TRANS
*     INTEGER            M, N, N1, N2, LDQ, LDC, LWORK, INFO
*     ..
*     .. Array Arguments ..
*     COMPLEX            Q( LDQ, * ), C( LDC, * ), WORK( * )
*     ..
*
*> \par Purpose
*  ============
*>
*> \verbatim
*>
*>  CUNM22 overwrites the general complex M-by-N matrix C with
*>
*>                  SIDE = 'L'     SIDE = 'R'
*>  TRANS = 'N':      Q * C          C * Q
*>  TRANS = 'C':      Q**H * C       C * Q**H
*>
*>  where Q is a complex unitary matrix of order NQ, with NQ = M if
*>  SIDE = 'L' and NQ = N if SIDE = 'R'.
*>  The unitary matrix Q processes a 2-by-2 block structure
*>
*>         [  Q11  Q12  ]
*>     Q = [            ]
*>         [  Q21  Q22  ],
*>
*>  where Q12 is an N1-by-N1 lower triangular matrix and Q21 is an
*>  N2-by-N2 upper triangular matrix.
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
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N':  apply Q (No transpose);
*>          = 'C':  apply Q**H (Conjugate transpose).
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
*> \param[in] N1
*> \param[in] N2
*> \verbatim
*>          N1 is INTEGER
*>          N2 is INTEGER
*>          The dimension of Q12 and Q21, respectively. N1, N2 >= 0.
*>          The following requirement must be satisfied:
*>          N1 + N2 = M if SIDE = 'L' and N1 + N2 = N if SIDE = 'R'.
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is COMPLEX array, dimension
*>                              (LDQ,M) if SIDE = 'L'
*>                              (LDQ,N) if SIDE = 'R'
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.
*>          LDQ >= max(1,M) if SIDE = 'L'; LDQ >= max(1,N) if SIDE = 'R'.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is COMPLEX array, dimension (LDC,N)
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
*>          WORK is COMPLEX array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If SIDE = 'L', LWORK >= max(1,N);
*>          if SIDE = 'R', LWORK >= max(1,M).
*>          For optimum performance LWORK >= M*N.
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
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date January 2015
*
*> \ingroup complexOTHERcomputational
*
*  =====================================================================
      SUBROUTINE CUNM22( SIDE, TRANS, M, N, N1, N2, Q, LDQ, C, LDC,
     $                   WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     January 2015
*
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          SIDE, TRANS
      INTEGER            M, N, N1, N2, LDQ, LDC, LWORK, INFO
*     ..
*     .. Array Arguments ..
      COMPLEX            Q( LDQ, * ), C( LDC, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ONE
      PARAMETER          ( ONE = ( 1.0E+0, 0.0E+0 ) )
*
*     .. Local Scalars ..
      LOGICAL            LEFT, LQUERY, NOTRAN
      INTEGER            I, LDWORK, LEN, LWKOPT, NB, NQ, NW
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMM, CLACPY, CTRMM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CMPLX, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      LEFT = LSAME( SIDE, 'L' )
      NOTRAN = LSAME( TRANS, 'N' )
      LQUERY = ( LWORK.EQ.-1 )
*
*     NQ is the order of Q;
*     NW is the minimum dimension of WORK.
*
      IF( LEFT ) THEN
         NQ = M
      ELSE
         NQ = N
      END IF
      NW = NQ
      IF( N1.EQ.0 .OR. N2.EQ.0 ) NW = 1
      IF( .NOT.LEFT .AND. .NOT.LSAME( SIDE, 'R' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( TRANS, 'N' ) .AND. .NOT.LSAME( TRANS, 'C' ) )
     $          THEN
         INFO = -2
      ELSE IF( M.LT.0 ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( N1.LT.0 .OR. N1+N2.NE.NQ ) THEN
         INFO = -5
      ELSE IF( N2.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDQ.LT.MAX( 1, NQ ) ) THEN
         INFO = -8
      ELSE IF( LDC.LT.MAX( 1, M ) ) THEN
         INFO = -10
      ELSE IF( LWORK.LT.NW .AND. .NOT.LQUERY ) THEN
         INFO = -12
      END IF
*
      IF( INFO.EQ.0 ) THEN
         LWKOPT = M*N
         WORK( 1 ) = CMPLX( LWKOPT )
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CUNM22', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 ) THEN
         WORK( 1 ) = 1
         RETURN
      END IF
*
*     Degenerate cases (N1 = 0 or N2 = 0) are handled using CTRMM.
*
      IF( N1.EQ.0 ) THEN
         CALL CTRMM( SIDE, 'Upper', TRANS, 'Non-Unit', M, N, ONE,
     $               Q, LDQ, C, LDC )
         WORK( 1 ) = ONE
         RETURN
      ELSE IF( N2.EQ.0 ) THEN
         CALL CTRMM( SIDE, 'Lower', TRANS, 'Non-Unit', M, N, ONE,
     $               Q, LDQ, C, LDC )
         WORK( 1 ) = ONE
         RETURN
      END IF
*
*     Compute the largest chunk size available from the workspace.
*
      NB = MAX( 1, MIN( LWORK, LWKOPT ) / NQ )
*
      IF( LEFT ) THEN
         IF( NOTRAN ) THEN
            DO I = 1, N, NB
               LEN = MIN( NB, N-I+1 )
               LDWORK = M
*
*              Multiply bottom part of C by Q12.
*
               CALL CLACPY( 'All', N1, LEN, C( N2+1, I ), LDC, WORK,
     $                      LDWORK )
               CALL CTRMM( 'Left', 'Lower', 'No Transpose', 'Non-Unit',
     $                     N1, LEN, ONE, Q( 1, N2+1 ), LDQ, WORK,
     $                     LDWORK )
*
*              Multiply top part of C by Q11.
*
               CALL CGEMM( 'No Transpose', 'No Transpose', N1, LEN, N2,
     $                     ONE, Q, LDQ, C( 1, I ), LDC, ONE, WORK,
     $                     LDWORK )
*
*              Multiply top part of C by Q21.
*
               CALL CLACPY( 'All', N2, LEN, C( 1, I ), LDC,
     $                      WORK( N1+1 ), LDWORK )
               CALL CTRMM( 'Left', 'Upper', 'No Transpose', 'Non-Unit',
     $                     N2, LEN, ONE, Q( N1+1, 1 ), LDQ,
     $                     WORK( N1+1 ), LDWORK )
*
*              Multiply bottom part of C by Q22.
*
               CALL CGEMM( 'No Transpose', 'No Transpose', N2, LEN, N1,
     $                     ONE, Q( N1+1, N2+1 ), LDQ, C( N2+1, I ), LDC,
     $                     ONE, WORK( N1+1 ), LDWORK )
*
*              Copy everything back.
*
               CALL CLACPY( 'All', M, LEN, WORK, LDWORK, C( 1, I ),
     $                      LDC )
            END DO
         ELSE
            DO I = 1, N, NB
               LEN = MIN( NB, N-I+1 )
               LDWORK = M
*
*              Multiply bottom part of C by Q21**H.
*
               CALL CLACPY( 'All', N2, LEN, C( N1+1, I ), LDC, WORK,
     $                      LDWORK )
               CALL CTRMM( 'Left', 'Upper', 'Conjugate', 'Non-Unit',
     $                     N2, LEN, ONE, Q( N1+1, 1 ), LDQ, WORK,
     $                     LDWORK )
*
*              Multiply top part of C by Q11**H.
*
               CALL CGEMM( 'Conjugate', 'No Transpose', N2, LEN, N1,
     $                     ONE, Q, LDQ, C( 1, I ), LDC, ONE, WORK,
     $                     LDWORK )
*
*              Multiply top part of C by Q12**H.
*
               CALL CLACPY( 'All', N1, LEN, C( 1, I ), LDC,
     $                      WORK( N2+1 ), LDWORK )
               CALL CTRMM( 'Left', 'Lower', 'Conjugate', 'Non-Unit',
     $                     N1, LEN, ONE, Q( 1, N2+1 ), LDQ,
     $                     WORK( N2+1 ), LDWORK )
*
*              Multiply bottom part of C by Q22**H.
*
               CALL CGEMM( 'Conjugate', 'No Transpose', N1, LEN, N2,
     $                     ONE, Q( N1+1, N2+1 ), LDQ, C( N1+1, I ), LDC,
     $                     ONE, WORK( N2+1 ), LDWORK )
*
*              Copy everything back.
*
               CALL CLACPY( 'All', M, LEN, WORK, LDWORK, C( 1, I ),
     $                      LDC )
            END DO
         END IF
      ELSE
         IF( NOTRAN ) THEN
            DO I = 1, M, NB
               LEN = MIN( NB, M-I+1 )
               LDWORK = LEN
*
*              Multiply right part of C by Q21.
*
               CALL CLACPY( 'All', LEN, N2, C( I, N1+1 ), LDC, WORK,
     $                      LDWORK )
               CALL CTRMM( 'Right', 'Upper', 'No Transpose', 'Non-Unit',
     $                     LEN, N2, ONE, Q( N1+1, 1 ), LDQ, WORK,
     $                     LDWORK )
*
*              Multiply left part of C by Q11.
*
               CALL CGEMM( 'No Transpose', 'No Transpose', LEN, N2, N1,
     $                     ONE, C( I, 1 ), LDC, Q, LDQ, ONE, WORK,
     $                     LDWORK )
*
*              Multiply left part of C by Q12.
*
               CALL CLACPY( 'All', LEN, N1, C( I, 1 ), LDC,
     $                      WORK( 1 + N2*LDWORK ), LDWORK )
               CALL CTRMM( 'Right', 'Lower', 'No Transpose', 'Non-Unit',
     $                     LEN, N1, ONE, Q( 1, N2+1 ), LDQ,
     $                     WORK( 1 + N2*LDWORK ), LDWORK )
*
*              Multiply right part of C by Q22.
*
               CALL CGEMM( 'No Transpose', 'No Transpose', LEN, N1, N2,
     $                     ONE, C( I, N1+1 ), LDC, Q( N1+1, N2+1 ), LDQ,
     $                     ONE, WORK( 1 + N2*LDWORK ), LDWORK )
*
*              Copy everything back.
*
               CALL CLACPY( 'All', LEN, N, WORK, LDWORK, C( I, 1 ),
     $                      LDC )
            END DO
         ELSE
            DO I = 1, M, NB
               LEN = MIN( NB, M-I+1 )
               LDWORK = LEN
*
*              Multiply right part of C by Q12**H.
*
               CALL CLACPY( 'All', LEN, N1, C( I, N2+1 ), LDC, WORK,
     $                      LDWORK )
               CALL CTRMM( 'Right', 'Lower', 'Conjugate', 'Non-Unit',
     $                     LEN, N1, ONE, Q( 1, N2+1 ), LDQ, WORK,
     $                     LDWORK )
*
*              Multiply left part of C by Q11**H.
*
               CALL CGEMM( 'No Transpose', 'Conjugate', LEN, N1, N2,
     $                     ONE, C( I, 1 ), LDC, Q, LDQ, ONE, WORK,
     $                     LDWORK )
*
*              Multiply left part of C by Q21**H.
*
               CALL CLACPY( 'All', LEN, N2, C( I, 1 ), LDC,
     $                      WORK( 1 + N1*LDWORK ), LDWORK )
               CALL CTRMM( 'Right', 'Upper', 'Conjugate', 'Non-Unit',
     $                     LEN, N2, ONE, Q( N1+1, 1 ), LDQ,
     $                     WORK( 1 + N1*LDWORK ), LDWORK )
*
*              Multiply right part of C by Q22**H.
*
               CALL CGEMM( 'No Transpose', 'Conjugate', LEN, N2, N1,
     $                     ONE, C( I, N2+1 ), LDC, Q( N1+1, N2+1 ), LDQ,
     $                     ONE, WORK( 1 + N1*LDWORK ), LDWORK )
*
*              Copy everything back.
*
               CALL CLACPY( 'All', LEN, N, WORK, LDWORK, C( I, 1 ),
     $                      LDC )
            END DO
         END IF
      END IF
*
      WORK( 1 ) = CMPLX( LWKOPT )
      RETURN
*
*     End of CUNM22
*
      END
