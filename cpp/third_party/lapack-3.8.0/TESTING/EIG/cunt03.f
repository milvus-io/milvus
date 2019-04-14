*> \brief \b CUNT03
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CUNT03( RC, MU, MV, N, K, U, LDU, V, LDV, WORK, LWORK,
*                          RWORK, RESULT, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER*( * )    RC
*       INTEGER            INFO, K, LDU, LDV, LWORK, MU, MV, N
*       REAL               RESULT
*       ..
*       .. Array Arguments ..
*       REAL               RWORK( * )
*       COMPLEX            U( LDU, * ), V( LDV, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CUNT03 compares two unitary matrices U and V to see if their
*> corresponding rows or columns span the same spaces.  The rows are
*> checked if RC = 'R', and the columns are checked if RC = 'C'.
*>
*> RESULT is the maximum of
*>
*>    | V*V' - I | / ( MV ulp ), if RC = 'R', or
*>
*>    | V'*V - I | / ( MV ulp ), if RC = 'C',
*>
*> and the maximum over rows (or columns) 1 to K of
*>
*>    | U(i) - S*V(i) |/ ( N ulp )
*>
*> where abs(S) = 1 (chosen to minimize the expression), U(i) is the
*> i-th row (column) of U, and V(i) is the i-th row (column) of V.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] RC
*> \verbatim
*>          RC is CHARACTER*1
*>          If RC = 'R' the rows of U and V are to be compared.
*>          If RC = 'C' the columns of U and V are to be compared.
*> \endverbatim
*>
*> \param[in] MU
*> \verbatim
*>          MU is INTEGER
*>          The number of rows of U if RC = 'R', and the number of
*>          columns if RC = 'C'.  If MU = 0 CUNT03 does nothing.
*>          MU must be at least zero.
*> \endverbatim
*>
*> \param[in] MV
*> \verbatim
*>          MV is INTEGER
*>          The number of rows of V if RC = 'R', and the number of
*>          columns if RC = 'C'.  If MV = 0 CUNT03 does nothing.
*>          MV must be at least zero.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          If RC = 'R', the number of columns in the matrices U and V,
*>          and if RC = 'C', the number of rows in U and V.  If N = 0
*>          CUNT03 does nothing.  N must be at least zero.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of rows or columns of U and V to compare.
*>          0 <= K <= max(MU,MV).
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is COMPLEX array, dimension (LDU,N)
*>          The first matrix to compare.  If RC = 'R', U is MU by N, and
*>          if RC = 'C', U is N by MU.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  If RC = 'R', LDU >= max(1,MU),
*>          and if RC = 'C', LDU >= max(1,N).
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is COMPLEX array, dimension (LDV,N)
*>          The second matrix to compare.  If RC = 'R', V is MV by N, and
*>          if RC = 'C', V is N by MV.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of V.  If RC = 'R', LDV >= max(1,MV),
*>          and if RC = 'C', LDV >= max(1,N).
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
*>          The length of the array WORK.  For best performance, LWORK
*>          should be at least N*N if RC = 'C' or M*M if RC = 'R', but
*>          the tests will be done even if LWORK is 0.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (max(MV,N))
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL
*>          The value computed by the test described above.  RESULT is
*>          limited to 1/ulp to avoid overflow.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          0  indicates a successful exit
*>          -k indicates the k-th parameter had an illegal value
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CUNT03( RC, MU, MV, N, K, U, LDU, V, LDV, WORK, LWORK,
     $                   RWORK, RESULT, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*( * )    RC
      INTEGER            INFO, K, LDU, LDV, LWORK, MU, MV, N
      REAL               RESULT
*     ..
*     .. Array Arguments ..
      REAL               RWORK( * )
      COMPLEX            U( LDU, * ), V( LDV, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IRC, J, LMX
      REAL               RES1, RES2, ULP
      COMPLEX            S, SU, SV
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ICAMAX
      REAL               SLAMCH
      EXTERNAL           LSAME, ICAMAX, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CMPLX, MAX, MIN, REAL
*     ..
*     .. External Subroutines ..
      EXTERNAL           CUNT01, XERBLA
*     ..
*     .. Executable Statements ..
*
*     Check inputs
*
      INFO = 0
      IF( LSAME( RC, 'R' ) ) THEN
         IRC = 0
      ELSE IF( LSAME( RC, 'C' ) ) THEN
         IRC = 1
      ELSE
         IRC = -1
      END IF
      IF( IRC.EQ.-1 ) THEN
         INFO = -1
      ELSE IF( MU.LT.0 ) THEN
         INFO = -2
      ELSE IF( MV.LT.0 ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( K.LT.0 .OR. K.GT.MAX( MU, MV ) ) THEN
         INFO = -5
      ELSE IF( ( IRC.EQ.0 .AND. LDU.LT.MAX( 1, MU ) ) .OR.
     $         ( IRC.EQ.1 .AND. LDU.LT.MAX( 1, N ) ) ) THEN
         INFO = -7
      ELSE IF( ( IRC.EQ.0 .AND. LDV.LT.MAX( 1, MV ) ) .OR.
     $         ( IRC.EQ.1 .AND. LDV.LT.MAX( 1, N ) ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CUNT03', -INFO )
         RETURN
      END IF
*
*     Initialize result
*
      RESULT = ZERO
      IF( MU.EQ.0 .OR. MV.EQ.0 .OR. N.EQ.0 )
     $   RETURN
*
*     Machine constants
*
      ULP = SLAMCH( 'Precision' )
*
      IF( IRC.EQ.0 ) THEN
*
*        Compare rows
*
         RES1 = ZERO
         DO 20 I = 1, K
            LMX = ICAMAX( N, U( I, 1 ), LDU )
            IF( V( I, LMX ).EQ.CMPLX( ZERO ) ) THEN
               SV = ONE
            ELSE
               SV = ABS( V( I, LMX ) ) / V( I, LMX )
            END IF
            IF( U( I, LMX ).EQ.CMPLX( ZERO ) ) THEN
               SU = ONE
            ELSE
               SU = ABS( U( I, LMX ) ) / U( I, LMX )
            END IF
            S = SV / SU
            DO 10 J = 1, N
               RES1 = MAX( RES1, ABS( U( I, J )-S*V( I, J ) ) )
   10       CONTINUE
   20    CONTINUE
         RES1 = RES1 / ( REAL( N )*ULP )
*
*        Compute orthogonality of rows of V.
*
         CALL CUNT01( 'Rows', MV, N, V, LDV, WORK, LWORK, RWORK, RES2 )
*
      ELSE
*
*        Compare columns
*
         RES1 = ZERO
         DO 40 I = 1, K
            LMX = ICAMAX( N, U( 1, I ), 1 )
            IF( V( LMX, I ).EQ.CMPLX( ZERO ) ) THEN
               SV = ONE
            ELSE
               SV = ABS( V( LMX, I ) ) / V( LMX, I )
            END IF
            IF( U( LMX, I ).EQ.CMPLX( ZERO ) ) THEN
               SU = ONE
            ELSE
               SU = ABS( U( LMX, I ) ) / U( LMX, I )
            END IF
            S = SV / SU
            DO 30 J = 1, N
               RES1 = MAX( RES1, ABS( U( J, I )-S*V( J, I ) ) )
   30       CONTINUE
   40    CONTINUE
         RES1 = RES1 / ( REAL( N )*ULP )
*
*        Compute orthogonality of columns of V.
*
         CALL CUNT01( 'Columns', N, MV, V, LDV, WORK, LWORK, RWORK,
     $                RES2 )
      END IF
*
      RESULT = MIN( MAX( RES1, RES2 ), ONE / ULP )
      RETURN
*
*     End of CUNT03
*
      END
