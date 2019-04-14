*> \brief \b ZGBBRD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGBBRD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgbbrd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgbbrd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgbbrd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGBBRD( VECT, M, N, NCC, KL, KU, AB, LDAB, D, E, Q,
*                          LDQ, PT, LDPT, C, LDC, WORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          VECT
*       INTEGER            INFO, KL, KU, LDAB, LDC, LDPT, LDQ, M, N, NCC
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * ), E( * ), RWORK( * )
*       COMPLEX*16         AB( LDAB, * ), C( LDC, * ), PT( LDPT, * ),
*      $                   Q( LDQ, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGBBRD reduces a complex general m-by-n band matrix A to real upper
*> bidiagonal form B by a unitary transformation: Q**H * A * P = B.
*>
*> The routine computes B, and optionally forms Q or P**H, or computes
*> Q**H*C for a given matrix C.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] VECT
*> \verbatim
*>          VECT is CHARACTER*1
*>          Specifies whether or not the matrices Q and P**H are to be
*>          formed.
*>          = 'N': do not form Q or P**H;
*>          = 'Q': form Q only;
*>          = 'P': form P**H only;
*>          = 'B': form both.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NCC
*> \verbatim
*>          NCC is INTEGER
*>          The number of columns of the matrix C.  NCC >= 0.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>          The number of subdiagonals of the matrix A. KL >= 0.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>          The number of superdiagonals of the matrix A. KU >= 0.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is COMPLEX*16 array, dimension (LDAB,N)
*>          On entry, the m-by-n band matrix A, stored in rows 1 to
*>          KL+KU+1. The j-th column of A is stored in the j-th column of
*>          the array AB as follows:
*>          AB(ku+1+i-j,j) = A(i,j) for max(1,j-ku)<=i<=min(m,j+kl).
*>          On exit, A is overwritten by values generated during the
*>          reduction.
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array A. LDAB >= KL+KU+1.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (min(M,N))
*>          The diagonal elements of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (min(M,N)-1)
*>          The superdiagonal elements of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDQ,M)
*>          If VECT = 'Q' or 'B', the m-by-m unitary matrix Q.
*>          If VECT = 'N' or 'P', the array Q is not referenced.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.
*>          LDQ >= max(1,M) if VECT = 'Q' or 'B'; LDQ >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] PT
*> \verbatim
*>          PT is COMPLEX*16 array, dimension (LDPT,N)
*>          If VECT = 'P' or 'B', the n-by-n unitary matrix P'.
*>          If VECT = 'N' or 'Q', the array PT is not referenced.
*> \endverbatim
*>
*> \param[in] LDPT
*> \verbatim
*>          LDPT is INTEGER
*>          The leading dimension of the array PT.
*>          LDPT >= max(1,N) if VECT = 'P' or 'B'; LDPT >= 1 otherwise.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is COMPLEX*16 array, dimension (LDC,NCC)
*>          On entry, an m-by-ncc matrix C.
*>          On exit, C is overwritten by Q**H*C.
*>          C is not referenced if NCC = 0.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of the array C.
*>          LDC >= max(1,M) if NCC > 0; LDC >= 1 if NCC = 0.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (max(M,N))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (max(M,N))
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
*> \ingroup complex16GBcomputational
*
*  =====================================================================
      SUBROUTINE ZGBBRD( VECT, M, N, NCC, KL, KU, AB, LDAB, D, E, Q,
     $                   LDQ, PT, LDPT, C, LDC, WORK, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          VECT
      INTEGER            INFO, KL, KU, LDAB, LDC, LDPT, LDQ, M, N, NCC
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * ), E( * ), RWORK( * )
      COMPLEX*16         AB( LDAB, * ), C( LDC, * ), PT( LDPT, * ),
     $                   Q( LDQ, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            WANTB, WANTC, WANTPT, WANTQ
      INTEGER            I, INCA, J, J1, J2, KB, KB1, KK, KLM, KLU1,
     $                   KUN, L, MINMN, ML, ML0, MU, MU0, NR, NRT
      DOUBLE PRECISION   ABST, RC
      COMPLEX*16         RA, RB, RS, T
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZLARGV, ZLARTG, ZLARTV, ZLASET, ZROT,
     $                   ZSCAL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DCONJG, MAX, MIN
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      WANTB = LSAME( VECT, 'B' )
      WANTQ = LSAME( VECT, 'Q' ) .OR. WANTB
      WANTPT = LSAME( VECT, 'P' ) .OR. WANTB
      WANTC = NCC.GT.0
      KLU1 = KL + KU + 1
      INFO = 0
      IF( .NOT.WANTQ .AND. .NOT.WANTPT .AND. .NOT.LSAME( VECT, 'N' ) )
     $     THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NCC.LT.0 ) THEN
         INFO = -4
      ELSE IF( KL.LT.0 ) THEN
         INFO = -5
      ELSE IF( KU.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDAB.LT.KLU1 ) THEN
         INFO = -8
      ELSE IF( LDQ.LT.1 .OR. WANTQ .AND. LDQ.LT.MAX( 1, M ) ) THEN
         INFO = -12
      ELSE IF( LDPT.LT.1 .OR. WANTPT .AND. LDPT.LT.MAX( 1, N ) ) THEN
         INFO = -14
      ELSE IF( LDC.LT.1 .OR. WANTC .AND. LDC.LT.MAX( 1, M ) ) THEN
         INFO = -16
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGBBRD', -INFO )
         RETURN
      END IF
*
*     Initialize Q and P**H to the unit matrix, if needed
*
      IF( WANTQ )
     $   CALL ZLASET( 'Full', M, M, CZERO, CONE, Q, LDQ )
      IF( WANTPT )
     $   CALL ZLASET( 'Full', N, N, CZERO, CONE, PT, LDPT )
*
*     Quick return if possible.
*
      IF( M.EQ.0 .OR. N.EQ.0 )
     $   RETURN
*
      MINMN = MIN( M, N )
*
      IF( KL+KU.GT.1 ) THEN
*
*        Reduce to upper bidiagonal form if KU > 0; if KU = 0, reduce
*        first to lower bidiagonal form and then transform to upper
*        bidiagonal
*
         IF( KU.GT.0 ) THEN
            ML0 = 1
            MU0 = 2
         ELSE
            ML0 = 2
            MU0 = 1
         END IF
*
*        Wherever possible, plane rotations are generated and applied in
*        vector operations of length NR over the index set J1:J2:KLU1.
*
*        The complex sines of the plane rotations are stored in WORK,
*        and the real cosines in RWORK.
*
         KLM = MIN( M-1, KL )
         KUN = MIN( N-1, KU )
         KB = KLM + KUN
         KB1 = KB + 1
         INCA = KB1*LDAB
         NR = 0
         J1 = KLM + 2
         J2 = 1 - KUN
*
         DO 90 I = 1, MINMN
*
*           Reduce i-th column and i-th row of matrix to bidiagonal form
*
            ML = KLM + 1
            MU = KUN + 1
            DO 80 KK = 1, KB
               J1 = J1 + KB
               J2 = J2 + KB
*
*              generate plane rotations to annihilate nonzero elements
*              which have been created below the band
*
               IF( NR.GT.0 )
     $            CALL ZLARGV( NR, AB( KLU1, J1-KLM-1 ), INCA,
     $                         WORK( J1 ), KB1, RWORK( J1 ), KB1 )
*
*              apply plane rotations from the left
*
               DO 10 L = 1, KB
                  IF( J2-KLM+L-1.GT.N ) THEN
                     NRT = NR - 1
                  ELSE
                     NRT = NR
                  END IF
                  IF( NRT.GT.0 )
     $               CALL ZLARTV( NRT, AB( KLU1-L, J1-KLM+L-1 ), INCA,
     $                            AB( KLU1-L+1, J1-KLM+L-1 ), INCA,
     $                            RWORK( J1 ), WORK( J1 ), KB1 )
   10          CONTINUE
*
               IF( ML.GT.ML0 ) THEN
                  IF( ML.LE.M-I+1 ) THEN
*
*                    generate plane rotation to annihilate a(i+ml-1,i)
*                    within the band, and apply rotation from the left
*
                     CALL ZLARTG( AB( KU+ML-1, I ), AB( KU+ML, I ),
     $                            RWORK( I+ML-1 ), WORK( I+ML-1 ), RA )
                     AB( KU+ML-1, I ) = RA
                     IF( I.LT.N )
     $                  CALL ZROT( MIN( KU+ML-2, N-I ),
     $                             AB( KU+ML-2, I+1 ), LDAB-1,
     $                             AB( KU+ML-1, I+1 ), LDAB-1,
     $                             RWORK( I+ML-1 ), WORK( I+ML-1 ) )
                  END IF
                  NR = NR + 1
                  J1 = J1 - KB1
               END IF
*
               IF( WANTQ ) THEN
*
*                 accumulate product of plane rotations in Q
*
                  DO 20 J = J1, J2, KB1
                     CALL ZROT( M, Q( 1, J-1 ), 1, Q( 1, J ), 1,
     $                          RWORK( J ), DCONJG( WORK( J ) ) )
   20             CONTINUE
               END IF
*
               IF( WANTC ) THEN
*
*                 apply plane rotations to C
*
                  DO 30 J = J1, J2, KB1
                     CALL ZROT( NCC, C( J-1, 1 ), LDC, C( J, 1 ), LDC,
     $                          RWORK( J ), WORK( J ) )
   30             CONTINUE
               END IF
*
               IF( J2+KUN.GT.N ) THEN
*
*                 adjust J2 to keep within the bounds of the matrix
*
                  NR = NR - 1
                  J2 = J2 - KB1
               END IF
*
               DO 40 J = J1, J2, KB1
*
*                 create nonzero element a(j-1,j+ku) above the band
*                 and store it in WORK(n+1:2*n)
*
                  WORK( J+KUN ) = WORK( J )*AB( 1, J+KUN )
                  AB( 1, J+KUN ) = RWORK( J )*AB( 1, J+KUN )
   40          CONTINUE
*
*              generate plane rotations to annihilate nonzero elements
*              which have been generated above the band
*
               IF( NR.GT.0 )
     $            CALL ZLARGV( NR, AB( 1, J1+KUN-1 ), INCA,
     $                         WORK( J1+KUN ), KB1, RWORK( J1+KUN ),
     $                         KB1 )
*
*              apply plane rotations from the right
*
               DO 50 L = 1, KB
                  IF( J2+L-1.GT.M ) THEN
                     NRT = NR - 1
                  ELSE
                     NRT = NR
                  END IF
                  IF( NRT.GT.0 )
     $               CALL ZLARTV( NRT, AB( L+1, J1+KUN-1 ), INCA,
     $                            AB( L, J1+KUN ), INCA,
     $                            RWORK( J1+KUN ), WORK( J1+KUN ), KB1 )
   50          CONTINUE
*
               IF( ML.EQ.ML0 .AND. MU.GT.MU0 ) THEN
                  IF( MU.LE.N-I+1 ) THEN
*
*                    generate plane rotation to annihilate a(i,i+mu-1)
*                    within the band, and apply rotation from the right
*
                     CALL ZLARTG( AB( KU-MU+3, I+MU-2 ),
     $                            AB( KU-MU+2, I+MU-1 ),
     $                            RWORK( I+MU-1 ), WORK( I+MU-1 ), RA )
                     AB( KU-MU+3, I+MU-2 ) = RA
                     CALL ZROT( MIN( KL+MU-2, M-I ),
     $                          AB( KU-MU+4, I+MU-2 ), 1,
     $                          AB( KU-MU+3, I+MU-1 ), 1,
     $                          RWORK( I+MU-1 ), WORK( I+MU-1 ) )
                  END IF
                  NR = NR + 1
                  J1 = J1 - KB1
               END IF
*
               IF( WANTPT ) THEN
*
*                 accumulate product of plane rotations in P**H
*
                  DO 60 J = J1, J2, KB1
                     CALL ZROT( N, PT( J+KUN-1, 1 ), LDPT,
     $                          PT( J+KUN, 1 ), LDPT, RWORK( J+KUN ),
     $                          DCONJG( WORK( J+KUN ) ) )
   60             CONTINUE
               END IF
*
               IF( J2+KB.GT.M ) THEN
*
*                 adjust J2 to keep within the bounds of the matrix
*
                  NR = NR - 1
                  J2 = J2 - KB1
               END IF
*
               DO 70 J = J1, J2, KB1
*
*                 create nonzero element a(j+kl+ku,j+ku-1) below the
*                 band and store it in WORK(1:n)
*
                  WORK( J+KB ) = WORK( J+KUN )*AB( KLU1, J+KUN )
                  AB( KLU1, J+KUN ) = RWORK( J+KUN )*AB( KLU1, J+KUN )
   70          CONTINUE
*
               IF( ML.GT.ML0 ) THEN
                  ML = ML - 1
               ELSE
                  MU = MU - 1
               END IF
   80       CONTINUE
   90    CONTINUE
      END IF
*
      IF( KU.EQ.0 .AND. KL.GT.0 ) THEN
*
*        A has been reduced to complex lower bidiagonal form
*
*        Transform lower bidiagonal form to upper bidiagonal by applying
*        plane rotations from the left, overwriting superdiagonal
*        elements on subdiagonal elements
*
         DO 100 I = 1, MIN( M-1, N )
            CALL ZLARTG( AB( 1, I ), AB( 2, I ), RC, RS, RA )
            AB( 1, I ) = RA
            IF( I.LT.N ) THEN
               AB( 2, I ) = RS*AB( 1, I+1 )
               AB( 1, I+1 ) = RC*AB( 1, I+1 )
            END IF
            IF( WANTQ )
     $         CALL ZROT( M, Q( 1, I ), 1, Q( 1, I+1 ), 1, RC,
     $                    DCONJG( RS ) )
            IF( WANTC )
     $         CALL ZROT( NCC, C( I, 1 ), LDC, C( I+1, 1 ), LDC, RC,
     $                    RS )
  100    CONTINUE
      ELSE
*
*        A has been reduced to complex upper bidiagonal form or is
*        diagonal
*
         IF( KU.GT.0 .AND. M.LT.N ) THEN
*
*           Annihilate a(m,m+1) by applying plane rotations from the
*           right
*
            RB = AB( KU, M+1 )
            DO 110 I = M, 1, -1
               CALL ZLARTG( AB( KU+1, I ), RB, RC, RS, RA )
               AB( KU+1, I ) = RA
               IF( I.GT.1 ) THEN
                  RB = -DCONJG( RS )*AB( KU, I )
                  AB( KU, I ) = RC*AB( KU, I )
               END IF
               IF( WANTPT )
     $            CALL ZROT( N, PT( I, 1 ), LDPT, PT( M+1, 1 ), LDPT,
     $                       RC, DCONJG( RS ) )
  110       CONTINUE
         END IF
      END IF
*
*     Make diagonal and superdiagonal elements real, storing them in D
*     and E
*
      T = AB( KU+1, 1 )
      DO 120 I = 1, MINMN
         ABST = ABS( T )
         D( I ) = ABST
         IF( ABST.NE.ZERO ) THEN
            T = T / ABST
         ELSE
            T = CONE
         END IF
         IF( WANTQ )
     $      CALL ZSCAL( M, T, Q( 1, I ), 1 )
         IF( WANTC )
     $      CALL ZSCAL( NCC, DCONJG( T ), C( I, 1 ), LDC )
         IF( I.LT.MINMN ) THEN
            IF( KU.EQ.0 .AND. KL.EQ.0 ) THEN
               E( I ) = ZERO
               T = AB( 1, I+1 )
            ELSE
               IF( KU.EQ.0 ) THEN
                  T = AB( 2, I )*DCONJG( T )
               ELSE
                  T = AB( KU, I+1 )*DCONJG( T )
               END IF
               ABST = ABS( T )
               E( I ) = ABST
               IF( ABST.NE.ZERO ) THEN
                  T = T / ABST
               ELSE
                  T = CONE
               END IF
               IF( WANTPT )
     $            CALL ZSCAL( N, T, PT( I+1, 1 ), LDPT )
               T = AB( KU+1, I+1 )*DCONJG( T )
            END IF
         END IF
  120 CONTINUE
      RETURN
*
*     End of ZGBBRD
*
      END
