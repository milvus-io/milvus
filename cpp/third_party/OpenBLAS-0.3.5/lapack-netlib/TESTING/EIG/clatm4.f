*> \brief \b CLATM4
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLATM4( ITYPE, N, NZ1, NZ2, RSIGN, AMAGN, RCOND,
*                          TRIANG, IDIST, ISEED, A, LDA )
*
*       .. Scalar Arguments ..
*       LOGICAL            RSIGN
*       INTEGER            IDIST, ITYPE, LDA, N, NZ1, NZ2
*       REAL               AMAGN, RCOND, TRIANG
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 )
*       COMPLEX            A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLATM4 generates basic square matrices, which may later be
*> multiplied by others in order to produce test matrices.  It is
*> intended mainly to be used to test the generalized eigenvalue
*> routines.
*>
*> It first generates the diagonal and (possibly) subdiagonal,
*> according to the value of ITYPE, NZ1, NZ2, RSIGN, AMAGN, and RCOND.
*> It then fills in the upper triangle with random numbers, if TRIANG is
*> non-zero.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ITYPE
*> \verbatim
*>          ITYPE is INTEGER
*>          The "type" of matrix on the diagonal and sub-diagonal.
*>          If ITYPE < 0, then type abs(ITYPE) is generated and then
*>             swapped end for end (A(I,J) := A'(N-J,N-I).)  See also
*>             the description of AMAGN and RSIGN.
*>
*>          Special types:
*>          = 0:  the zero matrix.
*>          = 1:  the identity.
*>          = 2:  a transposed Jordan block.
*>          = 3:  If N is odd, then a k+1 x k+1 transposed Jordan block
*>                followed by a k x k identity block, where k=(N-1)/2.
*>                If N is even, then k=(N-2)/2, and a zero diagonal entry
*>                is tacked onto the end.
*>
*>          Diagonal types.  The diagonal consists of NZ1 zeros, then
*>             k=N-NZ1-NZ2 nonzeros.  The subdiagonal is zero.  ITYPE
*>             specifies the nonzero diagonal entries as follows:
*>          = 4:  1, ..., k
*>          = 5:  1, RCOND, ..., RCOND
*>          = 6:  1, ..., 1, RCOND
*>          = 7:  1, a, a^2, ..., a^(k-1)=RCOND
*>          = 8:  1, 1-d, 1-2*d, ..., 1-(k-1)*d=RCOND
*>          = 9:  random numbers chosen from (RCOND,1)
*>          = 10: random numbers with distribution IDIST (see CLARND.)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix.
*> \endverbatim
*>
*> \param[in] NZ1
*> \verbatim
*>          NZ1 is INTEGER
*>          If abs(ITYPE) > 3, then the first NZ1 diagonal entries will
*>          be zero.
*> \endverbatim
*>
*> \param[in] NZ2
*> \verbatim
*>          NZ2 is INTEGER
*>          If abs(ITYPE) > 3, then the last NZ2 diagonal entries will
*>          be zero.
*> \endverbatim
*>
*> \param[in] RSIGN
*> \verbatim
*>          RSIGN is LOGICAL
*>          = .TRUE.:  The diagonal and subdiagonal entries will be
*>                     multiplied by random numbers of magnitude 1.
*>          = .FALSE.: The diagonal and subdiagonal entries will be
*>                     left as they are (usually non-negative real.)
*> \endverbatim
*>
*> \param[in] AMAGN
*> \verbatim
*>          AMAGN is REAL
*>          The diagonal and subdiagonal entries will be multiplied by
*>          AMAGN.
*> \endverbatim
*>
*> \param[in] RCOND
*> \verbatim
*>          RCOND is REAL
*>          If abs(ITYPE) > 4, then the smallest diagonal entry will be
*>          RCOND.  RCOND must be between 0 and 1.
*> \endverbatim
*>
*> \param[in] TRIANG
*> \verbatim
*>          TRIANG is REAL
*>          The entries above the diagonal will be random numbers with
*>          magnitude bounded by TRIANG (i.e., random numbers multiplied
*>          by TRIANG.)
*> \endverbatim
*>
*> \param[in] IDIST
*> \verbatim
*>          IDIST is INTEGER
*>          On entry, DIST specifies the type of distribution to be used
*>          to generate a random matrix .
*>          = 1: real and imaginary parts each UNIFORM( 0, 1 )
*>          = 2: real and imaginary parts each UNIFORM( -1, 1 )
*>          = 3: real and imaginary parts each NORMAL( 0, 1 )
*>          = 4: complex number uniform in DISK( 0, 1 )
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          On entry ISEED specifies the seed of the random number
*>          generator.  The values of ISEED are changed on exit, and can
*>          be used in the next call to CLATM4 to continue the same
*>          random number sequence.
*>          Note: ISEED(4) should be odd, for the random number generator
*>          used at present.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA, N)
*>          Array to be computed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          Leading dimension of A.  Must be at least 1 and at least N.
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
      SUBROUTINE CLATM4( ITYPE, N, NZ1, NZ2, RSIGN, AMAGN, RCOND,
     $                   TRIANG, IDIST, ISEED, A, LDA )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            RSIGN
      INTEGER            IDIST, ITYPE, LDA, N, NZ1, NZ2
      REAL               AMAGN, RCOND, TRIANG
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
      COMPLEX            A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, ISDB, ISDE, JC, JD, JR, K, KBEG, KEND, KLEN
      REAL               ALPHA
      COMPLEX            CTEMP
*     ..
*     .. External Functions ..
      REAL               SLARAN
      COMPLEX            CLARND
      EXTERNAL           SLARAN, CLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLASET
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CMPLX, EXP, LOG, MAX, MIN, MOD, REAL
*     ..
*     .. Executable Statements ..
*
      IF( N.LE.0 )
     $   RETURN
      CALL CLASET( 'Full', N, N, CZERO, CZERO, A, LDA )
*
*     Insure a correct ISEED
*
      IF( MOD( ISEED( 4 ), 2 ).NE.1 )
     $   ISEED( 4 ) = ISEED( 4 ) + 1
*
*     Compute diagonal and subdiagonal according to ITYPE, NZ1, NZ2,
*     and RCOND
*
      IF( ITYPE.NE.0 ) THEN
         IF( ABS( ITYPE ).GE.4 ) THEN
            KBEG = MAX( 1, MIN( N, NZ1+1 ) )
            KEND = MAX( KBEG, MIN( N, N-NZ2 ) )
            KLEN = KEND + 1 - KBEG
         ELSE
            KBEG = 1
            KEND = N
            KLEN = N
         END IF
         ISDB = 1
         ISDE = 0
         GO TO ( 10, 30, 50, 80, 100, 120, 140, 160,
     $           180, 200 )ABS( ITYPE )
*
*        abs(ITYPE) = 1: Identity
*
   10    CONTINUE
         DO 20 JD = 1, N
            A( JD, JD ) = CONE
   20    CONTINUE
         GO TO 220
*
*        abs(ITYPE) = 2: Transposed Jordan block
*
   30    CONTINUE
         DO 40 JD = 1, N - 1
            A( JD+1, JD ) = CONE
   40    CONTINUE
         ISDB = 1
         ISDE = N - 1
         GO TO 220
*
*        abs(ITYPE) = 3: Transposed Jordan block, followed by the
*                        identity.
*
   50    CONTINUE
         K = ( N-1 ) / 2
         DO 60 JD = 1, K
            A( JD+1, JD ) = CONE
   60    CONTINUE
         ISDB = 1
         ISDE = K
         DO 70 JD = K + 2, 2*K + 1
            A( JD, JD ) = CONE
   70    CONTINUE
         GO TO 220
*
*        abs(ITYPE) = 4: 1,...,k
*
   80    CONTINUE
         DO 90 JD = KBEG, KEND
            A( JD, JD ) = CMPLX( JD-NZ1 )
   90    CONTINUE
         GO TO 220
*
*        abs(ITYPE) = 5: One large D value:
*
  100    CONTINUE
         DO 110 JD = KBEG + 1, KEND
            A( JD, JD ) = CMPLX( RCOND )
  110    CONTINUE
         A( KBEG, KBEG ) = CONE
         GO TO 220
*
*        abs(ITYPE) = 6: One small D value:
*
  120    CONTINUE
         DO 130 JD = KBEG, KEND - 1
            A( JD, JD ) = CONE
  130    CONTINUE
         A( KEND, KEND ) = CMPLX( RCOND )
         GO TO 220
*
*        abs(ITYPE) = 7: Exponentially distributed D values:
*
  140    CONTINUE
         A( KBEG, KBEG ) = CONE
         IF( KLEN.GT.1 ) THEN
            ALPHA = RCOND**( ONE / REAL( KLEN-1 ) )
            DO 150 I = 2, KLEN
               A( NZ1+I, NZ1+I ) = CMPLX( ALPHA**REAL( I-1 ) )
  150       CONTINUE
         END IF
         GO TO 220
*
*        abs(ITYPE) = 8: Arithmetically distributed D values:
*
  160    CONTINUE
         A( KBEG, KBEG ) = CONE
         IF( KLEN.GT.1 ) THEN
            ALPHA = ( ONE-RCOND ) / REAL( KLEN-1 )
            DO 170 I = 2, KLEN
               A( NZ1+I, NZ1+I ) = CMPLX( REAL( KLEN-I )*ALPHA+RCOND )
  170       CONTINUE
         END IF
         GO TO 220
*
*        abs(ITYPE) = 9: Randomly distributed D values on ( RCOND, 1):
*
  180    CONTINUE
         ALPHA = LOG( RCOND )
         DO 190 JD = KBEG, KEND
            A( JD, JD ) = EXP( ALPHA*SLARAN( ISEED ) )
  190    CONTINUE
         GO TO 220
*
*        abs(ITYPE) = 10: Randomly distributed D values from DIST
*
  200    CONTINUE
         DO 210 JD = KBEG, KEND
            A( JD, JD ) = CLARND( IDIST, ISEED )
  210    CONTINUE
*
  220    CONTINUE
*
*        Scale by AMAGN
*
         DO 230 JD = KBEG, KEND
            A( JD, JD ) = AMAGN*REAL( A( JD, JD ) )
  230    CONTINUE
         DO 240 JD = ISDB, ISDE
            A( JD+1, JD ) = AMAGN*REAL( A( JD+1, JD ) )
  240    CONTINUE
*
*        If RSIGN = .TRUE., assign random signs to diagonal and
*        subdiagonal
*
         IF( RSIGN ) THEN
            DO 250 JD = KBEG, KEND
               IF( REAL( A( JD, JD ) ).NE.ZERO ) THEN
                  CTEMP = CLARND( 3, ISEED )
                  CTEMP = CTEMP / ABS( CTEMP )
                  A( JD, JD ) = CTEMP*REAL( A( JD, JD ) )
               END IF
  250       CONTINUE
            DO 260 JD = ISDB, ISDE
               IF( REAL( A( JD+1, JD ) ).NE.ZERO ) THEN
                  CTEMP = CLARND( 3, ISEED )
                  CTEMP = CTEMP / ABS( CTEMP )
                  A( JD+1, JD ) = CTEMP*REAL( A( JD+1, JD ) )
               END IF
  260       CONTINUE
         END IF
*
*        Reverse if ITYPE < 0
*
         IF( ITYPE.LT.0 ) THEN
            DO 270 JD = KBEG, ( KBEG+KEND-1 ) / 2
               CTEMP = A( JD, JD )
               A( JD, JD ) = A( KBEG+KEND-JD, KBEG+KEND-JD )
               A( KBEG+KEND-JD, KBEG+KEND-JD ) = CTEMP
  270       CONTINUE
            DO 280 JD = 1, ( N-1 ) / 2
               CTEMP = A( JD+1, JD )
               A( JD+1, JD ) = A( N+1-JD, N-JD )
               A( N+1-JD, N-JD ) = CTEMP
  280       CONTINUE
         END IF
*
      END IF
*
*     Fill in upper triangle
*
      IF( TRIANG.NE.ZERO ) THEN
         DO 300 JC = 2, N
            DO 290 JR = 1, JC - 1
               A( JR, JC ) = TRIANG*CLARND( IDIST, ISEED )
  290       CONTINUE
  300    CONTINUE
      END IF
*
      RETURN
*
*     End of CLATM4
*
      END
