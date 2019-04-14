*> \brief \b SLARZT forms the triangular factor T of a block reflector H = I - vtvH.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLARZT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarzt.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarzt.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarzt.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLARZT( DIRECT, STOREV, N, K, V, LDV, TAU, T, LDT )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIRECT, STOREV
*       INTEGER            K, LDT, LDV, N
*       ..
*       .. Array Arguments ..
*       REAL               T( LDT, * ), TAU( * ), V( LDV, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLARZT forms the triangular factor T of a real block reflector
*> H of order > n, which is defined as a product of k elementary
*> reflectors.
*>
*> If DIRECT = 'F', H = H(1) H(2) . . . H(k) and T is upper triangular;
*>
*> If DIRECT = 'B', H = H(k) . . . H(2) H(1) and T is lower triangular.
*>
*> If STOREV = 'C', the vector which defines the elementary reflector
*> H(i) is stored in the i-th column of the array V, and
*>
*>    H  =  I - V * T * V**T
*>
*> If STOREV = 'R', the vector which defines the elementary reflector
*> H(i) is stored in the i-th row of the array V, and
*>
*>    H  =  I - V**T * T * V
*>
*> Currently, only STOREV = 'R' and DIRECT = 'B' are supported.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] DIRECT
*> \verbatim
*>          DIRECT is CHARACTER*1
*>          Specifies the order in which the elementary reflectors are
*>          multiplied to form the block reflector:
*>          = 'F': H = H(1) H(2) . . . H(k) (Forward, not supported yet)
*>          = 'B': H = H(k) . . . H(2) H(1) (Backward)
*> \endverbatim
*>
*> \param[in] STOREV
*> \verbatim
*>          STOREV is CHARACTER*1
*>          Specifies how the vectors which define the elementary
*>          reflectors are stored (see also Further Details):
*>          = 'C': columnwise                        (not supported yet)
*>          = 'R': rowwise
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the block reflector H. N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The order of the triangular factor T (= the number of
*>          elementary reflectors). K >= 1.
*> \endverbatim
*>
*> \param[in,out] V
*> \verbatim
*>          V is REAL array, dimension
*>                               (LDV,K) if STOREV = 'C'
*>                               (LDV,N) if STOREV = 'R'
*>          The matrix V. See further details.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V.
*>          If STOREV = 'C', LDV >= max(1,N); if STOREV = 'R', LDV >= K.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is REAL array, dimension (K)
*>          TAU(i) must contain the scalar factor of the elementary
*>          reflector H(i).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is REAL array, dimension (LDT,K)
*>          The k by k triangular factor T of the block reflector.
*>          If DIRECT = 'F', T is upper triangular; if DIRECT = 'B', T is
*>          lower triangular. The rest of the array is not used.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= K.
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
*> \ingroup realOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*>    A. Petitet, Computer Science Dept., Univ. of Tenn., Knoxville, USA
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The shape of the matrix V and the storage of the vectors which define
*>  the H(i) is best illustrated by the following example with n = 5 and
*>  k = 3. The elements equal to 1 are not stored; the corresponding
*>  array elements are modified but restored on exit. The rest of the
*>  array is not used.
*>
*>  DIRECT = 'F' and STOREV = 'C':         DIRECT = 'F' and STOREV = 'R':
*>
*>                                              ______V_____
*>         ( v1 v2 v3 )                        /            \
*>         ( v1 v2 v3 )                      ( v1 v1 v1 v1 v1 . . . . 1 )
*>     V = ( v1 v2 v3 )                      ( v2 v2 v2 v2 v2 . . . 1   )
*>         ( v1 v2 v3 )                      ( v3 v3 v3 v3 v3 . . 1     )
*>         ( v1 v2 v3 )
*>            .  .  .
*>            .  .  .
*>            1  .  .
*>               1  .
*>                  1
*>
*>  DIRECT = 'B' and STOREV = 'C':         DIRECT = 'B' and STOREV = 'R':
*>
*>                                                        ______V_____
*>            1                                          /            \
*>            .  1                           ( 1 . . . . v1 v1 v1 v1 v1 )
*>            .  .  1                        ( . 1 . . . v2 v2 v2 v2 v2 )
*>            .  .  .                        ( . . 1 . . v3 v3 v3 v3 v3 )
*>            .  .  .
*>         ( v1 v2 v3 )
*>         ( v1 v2 v3 )
*>     V = ( v1 v2 v3 )
*>         ( v1 v2 v3 )
*>         ( v1 v2 v3 )
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SLARZT( DIRECT, STOREV, N, K, V, LDV, TAU, T, LDT )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIRECT, STOREV
      INTEGER            K, LDT, LDV, N
*     ..
*     .. Array Arguments ..
      REAL               T( LDT, * ), TAU( * ), V( LDV, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMV, STRMV, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Executable Statements ..
*
*     Check for currently supported options
*
      INFO = 0
      IF( .NOT.LSAME( DIRECT, 'B' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( STOREV, 'R' ) ) THEN
         INFO = -2
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLARZT', -INFO )
         RETURN
      END IF
*
      DO 20 I = K, 1, -1
         IF( TAU( I ).EQ.ZERO ) THEN
*
*           H(i)  =  I
*
            DO 10 J = I, K
               T( J, I ) = ZERO
   10       CONTINUE
         ELSE
*
*           general case
*
            IF( I.LT.K ) THEN
*
*              T(i+1:k,i) = - tau(i) * V(i+1:k,1:n) * V(i,1:n)**T
*
               CALL SGEMV( 'No transpose', K-I, N, -TAU( I ),
     $                     V( I+1, 1 ), LDV, V( I, 1 ), LDV, ZERO,
     $                     T( I+1, I ), 1 )
*
*              T(i+1:k,i) = T(i+1:k,i+1:k) * T(i+1:k,i)
*
               CALL STRMV( 'Lower', 'No transpose', 'Non-unit', K-I,
     $                     T( I+1, I+1 ), LDT, T( I+1, I ), 1 )
            END IF
            T( I, I ) = TAU( I )
         END IF
   20 CONTINUE
      RETURN
*
*     End of SLARZT
*
      END
