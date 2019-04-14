*> \brief \b SGBBRD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGBBRD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgbbrd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgbbrd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgbbrd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGBBRD( VECT, M, N, NCC, KL, KU, AB, LDAB, D, E, Q,
*                          LDQ, PT, LDPT, C, LDC, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          VECT
*       INTEGER            INFO, KL, KU, LDAB, LDC, LDPT, LDQ, M, N, NCC
*       ..
*       .. Array Arguments ..
*       REAL               AB( LDAB, * ), C( LDC, * ), D( * ), E( * ),
*      $                   PT( LDPT, * ), Q( LDQ, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGBBRD reduces a real general m-by-n band matrix A to upper
*> bidiagonal form B by an orthogonal transformation: Q**T * A * P = B.
*>
*> The routine computes B, and optionally forms Q or P**T, or computes
*> Q**T*C for a given matrix C.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] VECT
*> \verbatim
*>          VECT is CHARACTER*1
*>          Specifies whether or not the matrices Q and P**T are to be
*>          formed.
*>          = 'N': do not form Q or P**T;
*>          = 'Q': form Q only;
*>          = 'P': form P**T only;
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
*>          AB is REAL array, dimension (LDAB,N)
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
*>          D is REAL array, dimension (min(M,N))
*>          The diagonal elements of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is REAL array, dimension (min(M,N)-1)
*>          The superdiagonal elements of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is REAL array, dimension (LDQ,M)
*>          If VECT = 'Q' or 'B', the m-by-m orthogonal matrix Q.
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
*>          PT is REAL array, dimension (LDPT,N)
*>          If VECT = 'P' or 'B', the n-by-n orthogonal matrix P'.
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
*>          C is REAL array, dimension (LDC,NCC)
*>          On entry, an m-by-ncc matrix C.
*>          On exit, C is overwritten by Q**T*C.
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
*>          WORK is REAL array, dimension (2*max(M,N))
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
*> \ingroup realGBcomputational
*
*  =====================================================================
      SUBROUTINE SGBBRD( VECT, M, N, NCC, KL, KU, AB, LDAB, D, E, Q,
     $                   LDQ, PT, LDPT, C, LDC, WORK, INFO )
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
      REAL               AB( LDAB, * ), C( LDC, * ), D( * ), E( * ),
     $                   PT( LDPT, * ), Q( LDQ, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            WANTB, WANTC, WANTPT, WANTQ
      INTEGER            I, INCA, J, J1, J2, KB, KB1, KK, KLM, KLU1,
     $                   KUN, L, MINMN, ML, ML0, MN, MU, MU0, NR, NRT
      REAL               RA, RB, RC, RS
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLARGV, SLARTG, SLARTV, SLASET, SROT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
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
         CALL XERBLA( 'SGBBRD', -INFO )
         RETURN
      END IF
*
*     Initialize Q and P**T to the unit matrix, if needed
*
      IF( WANTQ )
     $   CALL SLASET( 'Full', M, M, ZERO, ONE, Q, LDQ )
      IF( WANTPT )
     $   CALL SLASET( 'Full', N, N, ZERO, ONE, PT, LDPT )
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
*        The sines of the plane rotations are stored in WORK(1:max(m,n))
*        and the cosines in WORK(max(m,n)+1:2*max(m,n)).
*
         MN = MAX( M, N )
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
     $            CALL SLARGV( NR, AB( KLU1, J1-KLM-1 ), INCA,
     $                         WORK( J1 ), KB1, WORK( MN+J1 ), KB1 )
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
     $               CALL SLARTV( NRT, AB( KLU1-L, J1-KLM+L-1 ), INCA,
     $                            AB( KLU1-L+1, J1-KLM+L-1 ), INCA,
     $                            WORK( MN+J1 ), WORK( J1 ), KB1 )
   10          CONTINUE
*
               IF( ML.GT.ML0 ) THEN
                  IF( ML.LE.M-I+1 ) THEN
*
*                    generate plane rotation to annihilate a(i+ml-1,i)
*                    within the band, and apply rotation from the left
*
                     CALL SLARTG( AB( KU+ML-1, I ), AB( KU+ML, I ),
     $                            WORK( MN+I+ML-1 ), WORK( I+ML-1 ),
     $                            RA )
                     AB( KU+ML-1, I ) = RA
                     IF( I.LT.N )
     $                  CALL SROT( MIN( KU+ML-2, N-I ),
     $                             AB( KU+ML-2, I+1 ), LDAB-1,
     $                             AB( KU+ML-1, I+1 ), LDAB-1,
     $                             WORK( MN+I+ML-1 ), WORK( I+ML-1 ) )
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
                     CALL SROT( M, Q( 1, J-1 ), 1, Q( 1, J ), 1,
     $                          WORK( MN+J ), WORK( J ) )
   20             CONTINUE
               END IF
*
               IF( WANTC ) THEN
*
*                 apply plane rotations to C
*
                  DO 30 J = J1, J2, KB1
                     CALL SROT( NCC, C( J-1, 1 ), LDC, C( J, 1 ), LDC,
     $                          WORK( MN+J ), WORK( J ) )
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
                  AB( 1, J+KUN ) = WORK( MN+J )*AB( 1, J+KUN )
   40          CONTINUE
*
*              generate plane rotations to annihilate nonzero elements
*              which have been generated above the band
*
               IF( NR.GT.0 )
     $            CALL SLARGV( NR, AB( 1, J1+KUN-1 ), INCA,
     $                         WORK( J1+KUN ), KB1, WORK( MN+J1+KUN ),
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
     $               CALL SLARTV( NRT, AB( L+1, J1+KUN-1 ), INCA,
     $                            AB( L, J1+KUN ), INCA,
     $                            WORK( MN+J1+KUN ), WORK( J1+KUN ),
     $                            KB1 )
   50          CONTINUE
*
               IF( ML.EQ.ML0 .AND. MU.GT.MU0 ) THEN
                  IF( MU.LE.N-I+1 ) THEN
*
*                    generate plane rotation to annihilate a(i,i+mu-1)
*                    within the band, and apply rotation from the right
*
                     CALL SLARTG( AB( KU-MU+3, I+MU-2 ),
     $                            AB( KU-MU+2, I+MU-1 ),
     $                            WORK( MN+I+MU-1 ), WORK( I+MU-1 ),
     $                            RA )
                     AB( KU-MU+3, I+MU-2 ) = RA
                     CALL SROT( MIN( KL+MU-2, M-I ),
     $                          AB( KU-MU+4, I+MU-2 ), 1,
     $                          AB( KU-MU+3, I+MU-1 ), 1,
     $                          WORK( MN+I+MU-1 ), WORK( I+MU-1 ) )
                  END IF
                  NR = NR + 1
                  J1 = J1 - KB1
               END IF
*
               IF( WANTPT ) THEN
*
*                 accumulate product of plane rotations in P**T
*
                  DO 60 J = J1, J2, KB1
                     CALL SROT( N, PT( J+KUN-1, 1 ), LDPT,
     $                          PT( J+KUN, 1 ), LDPT, WORK( MN+J+KUN ),
     $                          WORK( J+KUN ) )
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
                  AB( KLU1, J+KUN ) = WORK( MN+J+KUN )*AB( KLU1, J+KUN )
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
*        A has been reduced to lower bidiagonal form
*
*        Transform lower bidiagonal form to upper bidiagonal by applying
*        plane rotations from the left, storing diagonal elements in D
*        and off-diagonal elements in E
*
         DO 100 I = 1, MIN( M-1, N )
            CALL SLARTG( AB( 1, I ), AB( 2, I ), RC, RS, RA )
            D( I ) = RA
            IF( I.LT.N ) THEN
               E( I ) = RS*AB( 1, I+1 )
               AB( 1, I+1 ) = RC*AB( 1, I+1 )
            END IF
            IF( WANTQ )
     $         CALL SROT( M, Q( 1, I ), 1, Q( 1, I+1 ), 1, RC, RS )
            IF( WANTC )
     $         CALL SROT( NCC, C( I, 1 ), LDC, C( I+1, 1 ), LDC, RC,
     $                    RS )
  100    CONTINUE
         IF( M.LE.N )
     $      D( M ) = AB( 1, M )
      ELSE IF( KU.GT.0 ) THEN
*
*        A has been reduced to upper bidiagonal form
*
         IF( M.LT.N ) THEN
*
*           Annihilate a(m,m+1) by applying plane rotations from the
*           right, storing diagonal elements in D and off-diagonal
*           elements in E
*
            RB = AB( KU, M+1 )
            DO 110 I = M, 1, -1
               CALL SLARTG( AB( KU+1, I ), RB, RC, RS, RA )
               D( I ) = RA
               IF( I.GT.1 ) THEN
                  RB = -RS*AB( KU, I )
                  E( I-1 ) = RC*AB( KU, I )
               END IF
               IF( WANTPT )
     $            CALL SROT( N, PT( I, 1 ), LDPT, PT( M+1, 1 ), LDPT,
     $                       RC, RS )
  110       CONTINUE
         ELSE
*
*           Copy off-diagonal elements to E and diagonal elements to D
*
            DO 120 I = 1, MINMN - 1
               E( I ) = AB( KU, I+1 )
  120       CONTINUE
            DO 130 I = 1, MINMN
               D( I ) = AB( KU+1, I )
  130       CONTINUE
         END IF
      ELSE
*
*        A is diagonal. Set elements of E to zero and copy diagonal
*        elements to D.
*
         DO 140 I = 1, MINMN - 1
            E( I ) = ZERO
  140    CONTINUE
         DO 150 I = 1, MINMN
            D( I ) = AB( 1, I )
  150    CONTINUE
      END IF
      RETURN
*
*     End of SGBBRD
*
      END
