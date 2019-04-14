*> \brief \b DRQT03
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DRQT03( M, N, K, AF, C, CC, Q, LDA, TAU, WORK, LWORK,
*                          RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            K, LDA, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   AF( LDA, * ), C( LDA, * ), CC( LDA, * ),
*      $                   Q( LDA, * ), RESULT( * ), RWORK( * ), TAU( * ),
*      $                   WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DRQT03 tests DORMRQ, which computes Q*C, Q'*C, C*Q or C*Q'.
*>
*> DRQT03 compares the results of a call to DORMRQ with the results of
*> forming Q explicitly by a call to DORGRQ and then performing matrix
*> multiplication by a call to DGEMM.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows or columns of the matrix C; C is n-by-m if
*>          Q is applied from the left, or m-by-n if Q is applied from
*>          the right.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the orthogonal matrix Q.  N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of elementary reflectors whose product defines the
*>          orthogonal matrix Q.  N >= K >= 0.
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is DOUBLE PRECISION array, dimension (LDA,N)
*>          Details of the RQ factorization of an m-by-n matrix, as
*>          returned by DGERQF. See SGERQF for further details.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is DOUBLE PRECISION array, dimension (LDA,N)
*> \endverbatim
*>
*> \param[out] CC
*> \verbatim
*>          CC is DOUBLE PRECISION array, dimension (LDA,N)
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDA,N)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays AF, C, CC, and Q.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is DOUBLE PRECISION array, dimension (min(M,N))
*>          The scalar factors of the elementary reflectors corresponding
*>          to the RQ factorization in AF.
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
*>          The length of WORK.  LWORK must be at least M, and should be
*>          M*NB, where NB is the blocksize for this environment.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (M)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (4)
*>          The test ratios compare two techniques for multiplying a
*>          random matrix C by an n-by-n orthogonal matrix Q.
*>          RESULT(1) = norm( Q*C - Q*C )  / ( N * norm(C) * EPS )
*>          RESULT(2) = norm( C*Q - C*Q )  / ( N * norm(C) * EPS )
*>          RESULT(3) = norm( Q'*C - Q'*C )/ ( N * norm(C) * EPS )
*>          RESULT(4) = norm( C*Q' - C*Q' )/ ( N * norm(C) * EPS )
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DRQT03( M, N, K, AF, C, CC, Q, LDA, TAU, WORK, LWORK,
     $                   RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            K, LDA, LWORK, M, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   AF( LDA, * ), C( LDA, * ), CC( LDA, * ),
     $                   Q( LDA, * ), RESULT( * ), RWORK( * ), TAU( * ),
     $                   WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
      DOUBLE PRECISION   ROGUE
      PARAMETER          ( ROGUE = -1.0D+10 )
*     ..
*     .. Local Scalars ..
      CHARACTER          SIDE, TRANS
      INTEGER            INFO, ISIDE, ITRANS, J, MC, MINMN, NC
      DOUBLE PRECISION   CNORM, EPS, RESID
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           LSAME, DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLACPY, DLARNV, DLASET, DORGRQ, DORMRQ
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 )
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
*     ..
*     .. Scalars in Common ..
      CHARACTER*32       SRNAMT
*     ..
*     .. Common blocks ..
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Data statements ..
      DATA               ISEED / 1988, 1989, 1990, 1991 /
*     ..
*     .. Executable Statements ..
*
      EPS = DLAMCH( 'Epsilon' )
      MINMN = MIN( M, N )
*
*     Quick return if possible
*
      IF( MINMN.EQ.0 ) THEN
         RESULT( 1 ) = ZERO
         RESULT( 2 ) = ZERO
         RESULT( 3 ) = ZERO
         RESULT( 4 ) = ZERO
         RETURN
      END IF
*
*     Copy the last k rows of the factorization to the array Q
*
      CALL DLASET( 'Full', N, N, ROGUE, ROGUE, Q, LDA )
      IF( K.GT.0 .AND. N.GT.K )
     $   CALL DLACPY( 'Full', K, N-K, AF( M-K+1, 1 ), LDA,
     $                Q( N-K+1, 1 ), LDA )
      IF( K.GT.1 )
     $   CALL DLACPY( 'Lower', K-1, K-1, AF( M-K+2, N-K+1 ), LDA,
     $                Q( N-K+2, N-K+1 ), LDA )
*
*     Generate the n-by-n matrix Q
*
      SRNAMT = 'DORGRQ'
      CALL DORGRQ( N, N, K, Q, LDA, TAU( MINMN-K+1 ), WORK, LWORK,
     $             INFO )
*
      DO 30 ISIDE = 1, 2
         IF( ISIDE.EQ.1 ) THEN
            SIDE = 'L'
            MC = N
            NC = M
         ELSE
            SIDE = 'R'
            MC = M
            NC = N
         END IF
*
*        Generate MC by NC matrix C
*
         DO 10 J = 1, NC
            CALL DLARNV( 2, ISEED, MC, C( 1, J ) )
   10    CONTINUE
         CNORM = DLANGE( '1', MC, NC, C, LDA, RWORK )
         IF( CNORM.EQ.0.0D0 )
     $      CNORM = ONE
*
         DO 20 ITRANS = 1, 2
            IF( ITRANS.EQ.1 ) THEN
               TRANS = 'N'
            ELSE
               TRANS = 'T'
            END IF
*
*           Copy C
*
            CALL DLACPY( 'Full', MC, NC, C, LDA, CC, LDA )
*
*           Apply Q or Q' to C
*
            SRNAMT = 'DORMRQ'
            IF( K.GT.0 )
     $         CALL DORMRQ( SIDE, TRANS, MC, NC, K, AF( M-K+1, 1 ), LDA,
     $                      TAU( MINMN-K+1 ), CC, LDA, WORK, LWORK,
     $                      INFO )
*
*           Form explicit product and subtract
*
            IF( LSAME( SIDE, 'L' ) ) THEN
               CALL DGEMM( TRANS, 'No transpose', MC, NC, MC, -ONE, Q,
     $                     LDA, C, LDA, ONE, CC, LDA )
            ELSE
               CALL DGEMM( 'No transpose', TRANS, MC, NC, NC, -ONE, C,
     $                     LDA, Q, LDA, ONE, CC, LDA )
            END IF
*
*           Compute error in the difference
*
            RESID = DLANGE( '1', MC, NC, CC, LDA, RWORK )
            RESULT( ( ISIDE-1 )*2+ITRANS ) = RESID /
     $         ( DBLE( MAX( 1, N ) )*CNORM*EPS )
*
   20    CONTINUE
   30 CONTINUE
*
      RETURN
*
*     End of DRQT03
*
      END
