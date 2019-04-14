*> \brief \b ZLARFX applies an elementary reflector to a general rectangular matrix, with loop unrolling when the reflector has order ≤ 10.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLARFX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlarfx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlarfx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlarfx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLARFX( SIDE, M, N, V, TAU, C, LDC, WORK )
*
*       .. Scalar Arguments ..
*       CHARACTER          SIDE
*       INTEGER            LDC, M, N
*       COMPLEX*16         TAU
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         C( LDC, * ), V( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLARFX applies a complex elementary reflector H to a complex m by n
*> matrix C, from either the left or the right. H is represented in the
*> form
*>
*>       H = I - tau * v * v**H
*>
*> where tau is a complex scalar and v is a complex vector.
*>
*> If tau = 0, then H is taken to be the unit matrix
*>
*> This version uses inline code if H has order < 11.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'L': form  H * C
*>          = 'R': form  C * H
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix C.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix C.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is COMPLEX*16 array, dimension (M) if SIDE = 'L'
*>                                        or (N) if SIDE = 'R'
*>          The vector v in the representation of H.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is COMPLEX*16
*>          The value tau in the representation of H.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is COMPLEX*16 array, dimension (LDC,N)
*>          On entry, the m by n matrix C.
*>          On exit, C is overwritten by the matrix H * C if SIDE = 'L',
*>          or C * H if SIDE = 'R'.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of the array C. LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N) if SIDE = 'L'
*>                                            or (M) if SIDE = 'R'
*>          WORK is not referenced if H has order < 11.
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
*> \ingroup complex16OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE ZLARFX( SIDE, M, N, V, TAU, C, LDC, WORK )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          SIDE
      INTEGER            LDC, M, N
      COMPLEX*16         TAU
*     ..
*     .. Array Arguments ..
      COMPLEX*16         C( LDC, * ), V( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0D+0, 0.0D+0 ),
     $                   ONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            J
      COMPLEX*16         SUM, T1, T10, T2, T3, T4, T5, T6, T7, T8, T9,
     $                   V1, V10, V2, V3, V4, V5, V6, V7, V8, V9
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZLARF
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DCONJG
*     ..
*     .. Executable Statements ..
*
      IF( TAU.EQ.ZERO )
     $   RETURN
      IF( LSAME( SIDE, 'L' ) ) THEN
*
*        Form  H * C, where H has order m.
*
         GO TO ( 10, 30, 50, 70, 90, 110, 130, 150,
     $           170, 190 )M
*
*        Code for general M
*
         CALL ZLARF( SIDE, M, N, V, 1, TAU, C, LDC, WORK )
         GO TO 410
   10    CONTINUE
*
*        Special code for 1 x 1 Householder
*
         T1 = ONE - TAU*V( 1 )*DCONJG( V( 1 ) )
         DO 20 J = 1, N
            C( 1, J ) = T1*C( 1, J )
   20    CONTINUE
         GO TO 410
   30    CONTINUE
*
*        Special code for 2 x 2 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         DO 40 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
   40    CONTINUE
         GO TO 410
   50    CONTINUE
*
*        Special code for 3 x 3 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         DO 60 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
   60    CONTINUE
         GO TO 410
   70    CONTINUE
*
*        Special code for 4 x 4 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         DO 80 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
   80    CONTINUE
         GO TO 410
   90    CONTINUE
*
*        Special code for 5 x 5 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         V5 = DCONJG( V( 5 ) )
         T5 = TAU*DCONJG( V5 )
         DO 100 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J ) + V5*C( 5, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
            C( 5, J ) = C( 5, J ) - SUM*T5
  100    CONTINUE
         GO TO 410
  110    CONTINUE
*
*        Special code for 6 x 6 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         V5 = DCONJG( V( 5 ) )
         T5 = TAU*DCONJG( V5 )
         V6 = DCONJG( V( 6 ) )
         T6 = TAU*DCONJG( V6 )
         DO 120 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J ) + V5*C( 5, J ) + V6*C( 6, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
            C( 5, J ) = C( 5, J ) - SUM*T5
            C( 6, J ) = C( 6, J ) - SUM*T6
  120    CONTINUE
         GO TO 410
  130    CONTINUE
*
*        Special code for 7 x 7 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         V5 = DCONJG( V( 5 ) )
         T5 = TAU*DCONJG( V5 )
         V6 = DCONJG( V( 6 ) )
         T6 = TAU*DCONJG( V6 )
         V7 = DCONJG( V( 7 ) )
         T7 = TAU*DCONJG( V7 )
         DO 140 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J ) + V5*C( 5, J ) + V6*C( 6, J ) +
     $            V7*C( 7, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
            C( 5, J ) = C( 5, J ) - SUM*T5
            C( 6, J ) = C( 6, J ) - SUM*T6
            C( 7, J ) = C( 7, J ) - SUM*T7
  140    CONTINUE
         GO TO 410
  150    CONTINUE
*
*        Special code for 8 x 8 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         V5 = DCONJG( V( 5 ) )
         T5 = TAU*DCONJG( V5 )
         V6 = DCONJG( V( 6 ) )
         T6 = TAU*DCONJG( V6 )
         V7 = DCONJG( V( 7 ) )
         T7 = TAU*DCONJG( V7 )
         V8 = DCONJG( V( 8 ) )
         T8 = TAU*DCONJG( V8 )
         DO 160 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J ) + V5*C( 5, J ) + V6*C( 6, J ) +
     $            V7*C( 7, J ) + V8*C( 8, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
            C( 5, J ) = C( 5, J ) - SUM*T5
            C( 6, J ) = C( 6, J ) - SUM*T6
            C( 7, J ) = C( 7, J ) - SUM*T7
            C( 8, J ) = C( 8, J ) - SUM*T8
  160    CONTINUE
         GO TO 410
  170    CONTINUE
*
*        Special code for 9 x 9 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         V5 = DCONJG( V( 5 ) )
         T5 = TAU*DCONJG( V5 )
         V6 = DCONJG( V( 6 ) )
         T6 = TAU*DCONJG( V6 )
         V7 = DCONJG( V( 7 ) )
         T7 = TAU*DCONJG( V7 )
         V8 = DCONJG( V( 8 ) )
         T8 = TAU*DCONJG( V8 )
         V9 = DCONJG( V( 9 ) )
         T9 = TAU*DCONJG( V9 )
         DO 180 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J ) + V5*C( 5, J ) + V6*C( 6, J ) +
     $            V7*C( 7, J ) + V8*C( 8, J ) + V9*C( 9, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
            C( 5, J ) = C( 5, J ) - SUM*T5
            C( 6, J ) = C( 6, J ) - SUM*T6
            C( 7, J ) = C( 7, J ) - SUM*T7
            C( 8, J ) = C( 8, J ) - SUM*T8
            C( 9, J ) = C( 9, J ) - SUM*T9
  180    CONTINUE
         GO TO 410
  190    CONTINUE
*
*        Special code for 10 x 10 Householder
*
         V1 = DCONJG( V( 1 ) )
         T1 = TAU*DCONJG( V1 )
         V2 = DCONJG( V( 2 ) )
         T2 = TAU*DCONJG( V2 )
         V3 = DCONJG( V( 3 ) )
         T3 = TAU*DCONJG( V3 )
         V4 = DCONJG( V( 4 ) )
         T4 = TAU*DCONJG( V4 )
         V5 = DCONJG( V( 5 ) )
         T5 = TAU*DCONJG( V5 )
         V6 = DCONJG( V( 6 ) )
         T6 = TAU*DCONJG( V6 )
         V7 = DCONJG( V( 7 ) )
         T7 = TAU*DCONJG( V7 )
         V8 = DCONJG( V( 8 ) )
         T8 = TAU*DCONJG( V8 )
         V9 = DCONJG( V( 9 ) )
         T9 = TAU*DCONJG( V9 )
         V10 = DCONJG( V( 10 ) )
         T10 = TAU*DCONJG( V10 )
         DO 200 J = 1, N
            SUM = V1*C( 1, J ) + V2*C( 2, J ) + V3*C( 3, J ) +
     $            V4*C( 4, J ) + V5*C( 5, J ) + V6*C( 6, J ) +
     $            V7*C( 7, J ) + V8*C( 8, J ) + V9*C( 9, J ) +
     $            V10*C( 10, J )
            C( 1, J ) = C( 1, J ) - SUM*T1
            C( 2, J ) = C( 2, J ) - SUM*T2
            C( 3, J ) = C( 3, J ) - SUM*T3
            C( 4, J ) = C( 4, J ) - SUM*T4
            C( 5, J ) = C( 5, J ) - SUM*T5
            C( 6, J ) = C( 6, J ) - SUM*T6
            C( 7, J ) = C( 7, J ) - SUM*T7
            C( 8, J ) = C( 8, J ) - SUM*T8
            C( 9, J ) = C( 9, J ) - SUM*T9
            C( 10, J ) = C( 10, J ) - SUM*T10
  200    CONTINUE
         GO TO 410
      ELSE
*
*        Form  C * H, where H has order n.
*
         GO TO ( 210, 230, 250, 270, 290, 310, 330, 350,
     $           370, 390 )N
*
*        Code for general N
*
         CALL ZLARF( SIDE, M, N, V, 1, TAU, C, LDC, WORK )
         GO TO 410
  210    CONTINUE
*
*        Special code for 1 x 1 Householder
*
         T1 = ONE - TAU*V( 1 )*DCONJG( V( 1 ) )
         DO 220 J = 1, M
            C( J, 1 ) = T1*C( J, 1 )
  220    CONTINUE
         GO TO 410
  230    CONTINUE
*
*        Special code for 2 x 2 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         DO 240 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
  240    CONTINUE
         GO TO 410
  250    CONTINUE
*
*        Special code for 3 x 3 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         DO 260 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
  260    CONTINUE
         GO TO 410
  270    CONTINUE
*
*        Special code for 4 x 4 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         DO 280 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
  280    CONTINUE
         GO TO 410
  290    CONTINUE
*
*        Special code for 5 x 5 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         V5 = V( 5 )
         T5 = TAU*DCONJG( V5 )
         DO 300 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 ) + V5*C( J, 5 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
            C( J, 5 ) = C( J, 5 ) - SUM*T5
  300    CONTINUE
         GO TO 410
  310    CONTINUE
*
*        Special code for 6 x 6 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         V5 = V( 5 )
         T5 = TAU*DCONJG( V5 )
         V6 = V( 6 )
         T6 = TAU*DCONJG( V6 )
         DO 320 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 ) + V5*C( J, 5 ) + V6*C( J, 6 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
            C( J, 5 ) = C( J, 5 ) - SUM*T5
            C( J, 6 ) = C( J, 6 ) - SUM*T6
  320    CONTINUE
         GO TO 410
  330    CONTINUE
*
*        Special code for 7 x 7 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         V5 = V( 5 )
         T5 = TAU*DCONJG( V5 )
         V6 = V( 6 )
         T6 = TAU*DCONJG( V6 )
         V7 = V( 7 )
         T7 = TAU*DCONJG( V7 )
         DO 340 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 ) + V5*C( J, 5 ) + V6*C( J, 6 ) +
     $            V7*C( J, 7 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
            C( J, 5 ) = C( J, 5 ) - SUM*T5
            C( J, 6 ) = C( J, 6 ) - SUM*T6
            C( J, 7 ) = C( J, 7 ) - SUM*T7
  340    CONTINUE
         GO TO 410
  350    CONTINUE
*
*        Special code for 8 x 8 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         V5 = V( 5 )
         T5 = TAU*DCONJG( V5 )
         V6 = V( 6 )
         T6 = TAU*DCONJG( V6 )
         V7 = V( 7 )
         T7 = TAU*DCONJG( V7 )
         V8 = V( 8 )
         T8 = TAU*DCONJG( V8 )
         DO 360 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 ) + V5*C( J, 5 ) + V6*C( J, 6 ) +
     $            V7*C( J, 7 ) + V8*C( J, 8 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
            C( J, 5 ) = C( J, 5 ) - SUM*T5
            C( J, 6 ) = C( J, 6 ) - SUM*T6
            C( J, 7 ) = C( J, 7 ) - SUM*T7
            C( J, 8 ) = C( J, 8 ) - SUM*T8
  360    CONTINUE
         GO TO 410
  370    CONTINUE
*
*        Special code for 9 x 9 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         V5 = V( 5 )
         T5 = TAU*DCONJG( V5 )
         V6 = V( 6 )
         T6 = TAU*DCONJG( V6 )
         V7 = V( 7 )
         T7 = TAU*DCONJG( V7 )
         V8 = V( 8 )
         T8 = TAU*DCONJG( V8 )
         V9 = V( 9 )
         T9 = TAU*DCONJG( V9 )
         DO 380 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 ) + V5*C( J, 5 ) + V6*C( J, 6 ) +
     $            V7*C( J, 7 ) + V8*C( J, 8 ) + V9*C( J, 9 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
            C( J, 5 ) = C( J, 5 ) - SUM*T5
            C( J, 6 ) = C( J, 6 ) - SUM*T6
            C( J, 7 ) = C( J, 7 ) - SUM*T7
            C( J, 8 ) = C( J, 8 ) - SUM*T8
            C( J, 9 ) = C( J, 9 ) - SUM*T9
  380    CONTINUE
         GO TO 410
  390    CONTINUE
*
*        Special code for 10 x 10 Householder
*
         V1 = V( 1 )
         T1 = TAU*DCONJG( V1 )
         V2 = V( 2 )
         T2 = TAU*DCONJG( V2 )
         V3 = V( 3 )
         T3 = TAU*DCONJG( V3 )
         V4 = V( 4 )
         T4 = TAU*DCONJG( V4 )
         V5 = V( 5 )
         T5 = TAU*DCONJG( V5 )
         V6 = V( 6 )
         T6 = TAU*DCONJG( V6 )
         V7 = V( 7 )
         T7 = TAU*DCONJG( V7 )
         V8 = V( 8 )
         T8 = TAU*DCONJG( V8 )
         V9 = V( 9 )
         T9 = TAU*DCONJG( V9 )
         V10 = V( 10 )
         T10 = TAU*DCONJG( V10 )
         DO 400 J = 1, M
            SUM = V1*C( J, 1 ) + V2*C( J, 2 ) + V3*C( J, 3 ) +
     $            V4*C( J, 4 ) + V5*C( J, 5 ) + V6*C( J, 6 ) +
     $            V7*C( J, 7 ) + V8*C( J, 8 ) + V9*C( J, 9 ) +
     $            V10*C( J, 10 )
            C( J, 1 ) = C( J, 1 ) - SUM*T1
            C( J, 2 ) = C( J, 2 ) - SUM*T2
            C( J, 3 ) = C( J, 3 ) - SUM*T3
            C( J, 4 ) = C( J, 4 ) - SUM*T4
            C( J, 5 ) = C( J, 5 ) - SUM*T5
            C( J, 6 ) = C( J, 6 ) - SUM*T6
            C( J, 7 ) = C( J, 7 ) - SUM*T7
            C( J, 8 ) = C( J, 8 ) - SUM*T8
            C( J, 9 ) = C( J, 9 ) - SUM*T9
            C( J, 10 ) = C( J, 10 ) - SUM*T10
  400    CONTINUE
         GO TO 410
      END IF
  410 CONTINUE
      RETURN
*
*     End of ZLARFX
*
      END
