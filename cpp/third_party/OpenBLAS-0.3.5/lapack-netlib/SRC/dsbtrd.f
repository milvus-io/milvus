*> \brief \b DSBTRD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DSBTRD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dsbtrd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dsbtrd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dsbtrd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSBTRD( VECT, UPLO, N, KD, AB, LDAB, D, E, Q, LDQ,
*                          WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO, VECT
*       INTEGER            INFO, KD, LDAB, LDQ, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   AB( LDAB, * ), D( * ), E( * ), Q( LDQ, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSBTRD reduces a real symmetric band matrix A to symmetric
*> tridiagonal form T by an orthogonal similarity transformation:
*> Q**T * A * Q = T.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] VECT
*> \verbatim
*>          VECT is CHARACTER*1
*>          = 'N':  do not form Q;
*>          = 'V':  form Q;
*>          = 'U':  update a matrix X, by forming X*Q.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KD
*> \verbatim
*>          KD is INTEGER
*>          The number of superdiagonals of the matrix A if UPLO = 'U',
*>          or the number of subdiagonals if UPLO = 'L'.  KD >= 0.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is DOUBLE PRECISION array, dimension (LDAB,N)
*>          On entry, the upper or lower triangle of the symmetric band
*>          matrix A, stored in the first KD+1 rows of the array.  The
*>          j-th column of A is stored in the j-th column of the array AB
*>          as follows:
*>          if UPLO = 'U', AB(kd+1+i-j,j) = A(i,j) for max(1,j-kd)<=i<=j;
*>          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=min(n,j+kd).
*>          On exit, the diagonal elements of AB are overwritten by the
*>          diagonal elements of the tridiagonal matrix T; if KD > 0, the
*>          elements on the first superdiagonal (if UPLO = 'U') or the
*>          first subdiagonal (if UPLO = 'L') are overwritten by the
*>          off-diagonal elements of T; the rest of AB is overwritten by
*>          values generated during the reduction.
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array AB.  LDAB >= KD+1.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The diagonal elements of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The off-diagonal elements of the tridiagonal matrix T:
*>          E(i) = T(i,i+1) if UPLO = 'U'; E(i) = T(i+1,i) if UPLO = 'L'.
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ,N)
*>          On entry, if VECT = 'U', then Q must contain an N-by-N
*>          matrix X; if VECT = 'N' or 'V', then Q need not be set.
*>
*>          On exit:
*>          if VECT = 'V', Q contains the N-by-N orthogonal matrix Q;
*>          if VECT = 'U', Q contains the product X*Q;
*>          if VECT = 'N', the array Q is not referenced.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.
*>          LDQ >= 1, and LDQ >= N if VECT = 'V' or 'U'.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (N)
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
*> \ingroup doubleOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Modified by Linda Kaufman, Bell Labs.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DSBTRD( VECT, UPLO, N, KD, AB, LDAB, D, E, Q, LDQ,
     $                   WORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO, VECT
      INTEGER            INFO, KD, LDAB, LDQ, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   AB( LDAB, * ), D( * ), E( * ), Q( LDQ, * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            INITQ, UPPER, WANTQ
      INTEGER            I, I2, IBL, INCA, INCX, IQAEND, IQB, IQEND, J,
     $                   J1, J1END, J1INC, J2, JEND, JIN, JINC, K, KD1,
     $                   KDM1, KDN, L, LAST, LEND, NQ, NR, NRT
      DOUBLE PRECISION   TEMP
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLAR2V, DLARGV, DLARTG, DLARTV, DLASET, DROT,
     $                   XERBLA
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
      INITQ = LSAME( VECT, 'V' )
      WANTQ = INITQ .OR. LSAME( VECT, 'U' )
      UPPER = LSAME( UPLO, 'U' )
      KD1 = KD + 1
      KDM1 = KD - 1
      INCX = LDAB - 1
      IQEND = 1
*
      INFO = 0
      IF( .NOT.WANTQ .AND. .NOT.LSAME( VECT, 'N' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( KD.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDAB.LT.KD1 ) THEN
         INFO = -6
      ELSE IF( LDQ.LT.MAX( 1, N ) .AND. WANTQ ) THEN
         INFO = -10
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DSBTRD', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Initialize Q to the unit matrix, if needed
*
      IF( INITQ )
     $   CALL DLASET( 'Full', N, N, ZERO, ONE, Q, LDQ )
*
*     Wherever possible, plane rotations are generated and applied in
*     vector operations of length NR over the index set J1:J2:KD1.
*
*     The cosines and sines of the plane rotations are stored in the
*     arrays D and WORK.
*
      INCA = KD1*LDAB
      KDN = MIN( N-1, KD )
      IF( UPPER ) THEN
*
         IF( KD.GT.1 ) THEN
*
*           Reduce to tridiagonal form, working with upper triangle
*
            NR = 0
            J1 = KDN + 2
            J2 = 1
*
            DO 90 I = 1, N - 2
*
*              Reduce i-th row of matrix to tridiagonal form
*
               DO 80 K = KDN + 1, 2, -1
                  J1 = J1 + KDN
                  J2 = J2 + KDN
*
                  IF( NR.GT.0 ) THEN
*
*                    generate plane rotations to annihilate nonzero
*                    elements which have been created outside the band
*
                     CALL DLARGV( NR, AB( 1, J1-1 ), INCA, WORK( J1 ),
     $                            KD1, D( J1 ), KD1 )
*
*                    apply rotations from the right
*
*
*                    Dependent on the the number of diagonals either
*                    DLARTV or DROT is used
*
                     IF( NR.GE.2*KD-1 ) THEN
                        DO 10 L = 1, KD - 1
                           CALL DLARTV( NR, AB( L+1, J1-1 ), INCA,
     $                                  AB( L, J1 ), INCA, D( J1 ),
     $                                  WORK( J1 ), KD1 )
   10                   CONTINUE
*
                     ELSE
                        JEND = J1 + ( NR-1 )*KD1
                        DO 20 JINC = J1, JEND, KD1
                           CALL DROT( KDM1, AB( 2, JINC-1 ), 1,
     $                                AB( 1, JINC ), 1, D( JINC ),
     $                                WORK( JINC ) )
   20                   CONTINUE
                     END IF
                  END IF
*
*
                  IF( K.GT.2 ) THEN
                     IF( K.LE.N-I+1 ) THEN
*
*                       generate plane rotation to annihilate a(i,i+k-1)
*                       within the band
*
                        CALL DLARTG( AB( KD-K+3, I+K-2 ),
     $                               AB( KD-K+2, I+K-1 ), D( I+K-1 ),
     $                               WORK( I+K-1 ), TEMP )
                        AB( KD-K+3, I+K-2 ) = TEMP
*
*                       apply rotation from the right
*
                        CALL DROT( K-3, AB( KD-K+4, I+K-2 ), 1,
     $                             AB( KD-K+3, I+K-1 ), 1, D( I+K-1 ),
     $                             WORK( I+K-1 ) )
                     END IF
                     NR = NR + 1
                     J1 = J1 - KDN - 1
                  END IF
*
*                 apply plane rotations from both sides to diagonal
*                 blocks
*
                  IF( NR.GT.0 )
     $               CALL DLAR2V( NR, AB( KD1, J1-1 ), AB( KD1, J1 ),
     $                            AB( KD, J1 ), INCA, D( J1 ),
     $                            WORK( J1 ), KD1 )
*
*                 apply plane rotations from the left
*
                  IF( NR.GT.0 ) THEN
                     IF( 2*KD-1.LT.NR ) THEN
*
*                    Dependent on the the number of diagonals either
*                    DLARTV or DROT is used
*
                        DO 30 L = 1, KD - 1
                           IF( J2+L.GT.N ) THEN
                              NRT = NR - 1
                           ELSE
                              NRT = NR
                           END IF
                           IF( NRT.GT.0 )
     $                        CALL DLARTV( NRT, AB( KD-L, J1+L ), INCA,
     $                                     AB( KD-L+1, J1+L ), INCA,
     $                                     D( J1 ), WORK( J1 ), KD1 )
   30                   CONTINUE
                     ELSE
                        J1END = J1 + KD1*( NR-2 )
                        IF( J1END.GE.J1 ) THEN
                           DO 40 JIN = J1, J1END, KD1
                              CALL DROT( KD-1, AB( KD-1, JIN+1 ), INCX,
     $                                   AB( KD, JIN+1 ), INCX,
     $                                   D( JIN ), WORK( JIN ) )
   40                      CONTINUE
                        END IF
                        LEND = MIN( KDM1, N-J2 )
                        LAST = J1END + KD1
                        IF( LEND.GT.0 )
     $                     CALL DROT( LEND, AB( KD-1, LAST+1 ), INCX,
     $                                AB( KD, LAST+1 ), INCX, D( LAST ),
     $                                WORK( LAST ) )
                     END IF
                  END IF
*
                  IF( WANTQ ) THEN
*
*                    accumulate product of plane rotations in Q
*
                     IF( INITQ ) THEN
*
*                 take advantage of the fact that Q was
*                 initially the Identity matrix
*
                        IQEND = MAX( IQEND, J2 )
                        I2 = MAX( 0, K-3 )
                        IQAEND = 1 + I*KD
                        IF( K.EQ.2 )
     $                     IQAEND = IQAEND + KD
                        IQAEND = MIN( IQAEND, IQEND )
                        DO 50 J = J1, J2, KD1
                           IBL = I - I2 / KDM1
                           I2 = I2 + 1
                           IQB = MAX( 1, J-IBL )
                           NQ = 1 + IQAEND - IQB
                           IQAEND = MIN( IQAEND+KD, IQEND )
                           CALL DROT( NQ, Q( IQB, J-1 ), 1, Q( IQB, J ),
     $                                1, D( J ), WORK( J ) )
   50                   CONTINUE
                     ELSE
*
                        DO 60 J = J1, J2, KD1
                           CALL DROT( N, Q( 1, J-1 ), 1, Q( 1, J ), 1,
     $                                D( J ), WORK( J ) )
   60                   CONTINUE
                     END IF
*
                  END IF
*
                  IF( J2+KDN.GT.N ) THEN
*
*                    adjust J2 to keep within the bounds of the matrix
*
                     NR = NR - 1
                     J2 = J2 - KDN - 1
                  END IF
*
                  DO 70 J = J1, J2, KD1
*
*                    create nonzero element a(j-1,j+kd) outside the band
*                    and store it in WORK
*
                     WORK( J+KD ) = WORK( J )*AB( 1, J+KD )
                     AB( 1, J+KD ) = D( J )*AB( 1, J+KD )
   70             CONTINUE
   80          CONTINUE
   90       CONTINUE
         END IF
*
         IF( KD.GT.0 ) THEN
*
*           copy off-diagonal elements to E
*
            DO 100 I = 1, N - 1
               E( I ) = AB( KD, I+1 )
  100       CONTINUE
         ELSE
*
*           set E to zero if original matrix was diagonal
*
            DO 110 I = 1, N - 1
               E( I ) = ZERO
  110       CONTINUE
         END IF
*
*        copy diagonal elements to D
*
         DO 120 I = 1, N
            D( I ) = AB( KD1, I )
  120    CONTINUE
*
      ELSE
*
         IF( KD.GT.1 ) THEN
*
*           Reduce to tridiagonal form, working with lower triangle
*
            NR = 0
            J1 = KDN + 2
            J2 = 1
*
            DO 210 I = 1, N - 2
*
*              Reduce i-th column of matrix to tridiagonal form
*
               DO 200 K = KDN + 1, 2, -1
                  J1 = J1 + KDN
                  J2 = J2 + KDN
*
                  IF( NR.GT.0 ) THEN
*
*                    generate plane rotations to annihilate nonzero
*                    elements which have been created outside the band
*
                     CALL DLARGV( NR, AB( KD1, J1-KD1 ), INCA,
     $                            WORK( J1 ), KD1, D( J1 ), KD1 )
*
*                    apply plane rotations from one side
*
*
*                    Dependent on the the number of diagonals either
*                    DLARTV or DROT is used
*
                     IF( NR.GT.2*KD-1 ) THEN
                        DO 130 L = 1, KD - 1
                           CALL DLARTV( NR, AB( KD1-L, J1-KD1+L ), INCA,
     $                                  AB( KD1-L+1, J1-KD1+L ), INCA,
     $                                  D( J1 ), WORK( J1 ), KD1 )
  130                   CONTINUE
                     ELSE
                        JEND = J1 + KD1*( NR-1 )
                        DO 140 JINC = J1, JEND, KD1
                           CALL DROT( KDM1, AB( KD, JINC-KD ), INCX,
     $                                AB( KD1, JINC-KD ), INCX,
     $                                D( JINC ), WORK( JINC ) )
  140                   CONTINUE
                     END IF
*
                  END IF
*
                  IF( K.GT.2 ) THEN
                     IF( K.LE.N-I+1 ) THEN
*
*                       generate plane rotation to annihilate a(i+k-1,i)
*                       within the band
*
                        CALL DLARTG( AB( K-1, I ), AB( K, I ),
     $                               D( I+K-1 ), WORK( I+K-1 ), TEMP )
                        AB( K-1, I ) = TEMP
*
*                       apply rotation from the left
*
                        CALL DROT( K-3, AB( K-2, I+1 ), LDAB-1,
     $                             AB( K-1, I+1 ), LDAB-1, D( I+K-1 ),
     $                             WORK( I+K-1 ) )
                     END IF
                     NR = NR + 1
                     J1 = J1 - KDN - 1
                  END IF
*
*                 apply plane rotations from both sides to diagonal
*                 blocks
*
                  IF( NR.GT.0 )
     $               CALL DLAR2V( NR, AB( 1, J1-1 ), AB( 1, J1 ),
     $                            AB( 2, J1-1 ), INCA, D( J1 ),
     $                            WORK( J1 ), KD1 )
*
*                 apply plane rotations from the right
*
*
*                    Dependent on the the number of diagonals either
*                    DLARTV or DROT is used
*
                  IF( NR.GT.0 ) THEN
                     IF( NR.GT.2*KD-1 ) THEN
                        DO 150 L = 1, KD - 1
                           IF( J2+L.GT.N ) THEN
                              NRT = NR - 1
                           ELSE
                              NRT = NR
                           END IF
                           IF( NRT.GT.0 )
     $                        CALL DLARTV( NRT, AB( L+2, J1-1 ), INCA,
     $                                     AB( L+1, J1 ), INCA, D( J1 ),
     $                                     WORK( J1 ), KD1 )
  150                   CONTINUE
                     ELSE
                        J1END = J1 + KD1*( NR-2 )
                        IF( J1END.GE.J1 ) THEN
                           DO 160 J1INC = J1, J1END, KD1
                              CALL DROT( KDM1, AB( 3, J1INC-1 ), 1,
     $                                   AB( 2, J1INC ), 1, D( J1INC ),
     $                                   WORK( J1INC ) )
  160                      CONTINUE
                        END IF
                        LEND = MIN( KDM1, N-J2 )
                        LAST = J1END + KD1
                        IF( LEND.GT.0 )
     $                     CALL DROT( LEND, AB( 3, LAST-1 ), 1,
     $                                AB( 2, LAST ), 1, D( LAST ),
     $                                WORK( LAST ) )
                     END IF
                  END IF
*
*
*
                  IF( WANTQ ) THEN
*
*                    accumulate product of plane rotations in Q
*
                     IF( INITQ ) THEN
*
*                 take advantage of the fact that Q was
*                 initially the Identity matrix
*
                        IQEND = MAX( IQEND, J2 )
                        I2 = MAX( 0, K-3 )
                        IQAEND = 1 + I*KD
                        IF( K.EQ.2 )
     $                     IQAEND = IQAEND + KD
                        IQAEND = MIN( IQAEND, IQEND )
                        DO 170 J = J1, J2, KD1
                           IBL = I - I2 / KDM1
                           I2 = I2 + 1
                           IQB = MAX( 1, J-IBL )
                           NQ = 1 + IQAEND - IQB
                           IQAEND = MIN( IQAEND+KD, IQEND )
                           CALL DROT( NQ, Q( IQB, J-1 ), 1, Q( IQB, J ),
     $                                1, D( J ), WORK( J ) )
  170                   CONTINUE
                     ELSE
*
                        DO 180 J = J1, J2, KD1
                           CALL DROT( N, Q( 1, J-1 ), 1, Q( 1, J ), 1,
     $                                D( J ), WORK( J ) )
  180                   CONTINUE
                     END IF
                  END IF
*
                  IF( J2+KDN.GT.N ) THEN
*
*                    adjust J2 to keep within the bounds of the matrix
*
                     NR = NR - 1
                     J2 = J2 - KDN - 1
                  END IF
*
                  DO 190 J = J1, J2, KD1
*
*                    create nonzero element a(j+kd,j-1) outside the
*                    band and store it in WORK
*
                     WORK( J+KD ) = WORK( J )*AB( KD1, J )
                     AB( KD1, J ) = D( J )*AB( KD1, J )
  190             CONTINUE
  200          CONTINUE
  210       CONTINUE
         END IF
*
         IF( KD.GT.0 ) THEN
*
*           copy off-diagonal elements to E
*
            DO 220 I = 1, N - 1
               E( I ) = AB( 2, I )
  220       CONTINUE
         ELSE
*
*           set E to zero if original matrix was diagonal
*
            DO 230 I = 1, N - 1
               E( I ) = ZERO
  230       CONTINUE
         END IF
*
*        copy diagonal elements to D
*
         DO 240 I = 1, N
            D( I ) = AB( 1, I )
  240    CONTINUE
      END IF
*
      RETURN
*
*     End of DSBTRD
*
      END
