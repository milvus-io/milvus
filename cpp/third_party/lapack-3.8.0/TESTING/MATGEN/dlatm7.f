*> \brief \b DLATM7
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLATM7( MODE, COND, IRSIGN, IDIST, ISEED, D, N,
*                          RANK, INFO )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   COND
*       INTEGER            IDIST, INFO, IRSIGN, MODE, N, RANK
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * )
*       INTEGER            ISEED( 4 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DLATM7 computes the entries of D as specified by MODE
*>    COND and IRSIGN. IDIST and ISEED determine the generation
*>    of random numbers. DLATM7 is called by DLATMT to generate
*>    random test matrices.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  MODE   - INTEGER
*>           On entry describes how D is to be computed:
*>           MODE = 0 means do not change D.
*>
*>           MODE = 1 sets D(1)=1 and D(2:RANK)=1.0/COND
*>           MODE = 2 sets D(1:RANK-1)=1 and D(RANK)=1.0/COND
*>           MODE = 3 sets D(I)=COND**(-(I-1)/(RANK-1)) I=1:RANK
*>
*>           MODE = 4 sets D(i)=1 - (i-1)/(N-1)*(1 - 1/COND)
*>           MODE = 5 sets D to random numbers in the range
*>                    ( 1/COND , 1 ) such that their logarithms
*>                    are uniformly distributed.
*>           MODE = 6 set D to random numbers from same distribution
*>                    as the rest of the matrix.
*>           MODE < 0 has the same meaning as ABS(MODE), except that
*>              the order of the elements of D is reversed.
*>           Thus if MODE is positive, D has entries ranging from
*>              1 to 1/COND, if negative, from 1/COND to 1,
*>           Not modified.
*>
*>  COND   - DOUBLE PRECISION
*>           On entry, used as described under MODE above.
*>           If used, it must be >= 1. Not modified.
*>
*>  IRSIGN - INTEGER
*>           On entry, if MODE neither -6, 0 nor 6, determines sign of
*>           entries of D
*>           0 => leave entries of D unchanged
*>           1 => multiply each entry of D by 1 or -1 with probability .5
*>
*>  IDIST  - CHARACTER*1
*>           On entry, IDIST specifies the type of distribution to be
*>           used to generate a random matrix .
*>           1 => UNIFORM( 0, 1 )
*>           2 => UNIFORM( -1, 1 )
*>           3 => NORMAL( 0, 1 )
*>           Not modified.
*>
*>  ISEED  - INTEGER array, dimension ( 4 )
*>           On entry ISEED specifies the seed of the random number
*>           generator. The random number generator uses a
*>           linear congruential sequence limited to small
*>           integers, and so should produce machine independent
*>           random numbers. The values of ISEED are changed on
*>           exit, and can be used in the next call to DLATM7
*>           to continue the same random number sequence.
*>           Changed on exit.
*>
*>  D      - DOUBLE PRECISION array, dimension ( MIN( M , N ) )
*>           Array to be computed according to MODE, COND and IRSIGN.
*>           May be changed on exit if MODE is nonzero.
*>
*>  N      - INTEGER
*>           Number of entries of D. Not modified.
*>
*>  RANK   - INTEGER
*>           The rank of matrix to be generated for modes 1,2,3 only.
*>           D( RANK+1:N ) = 0.
*>           Not modified.
*>
*>  INFO   - INTEGER
*>            0  => normal termination
*>           -1  => if MODE not in range -6 to 6
*>           -2  => if MODE neither -6, 0 nor 6, and
*>                  IRSIGN neither 0 nor 1
*>           -3  => if MODE neither -6, 0 nor 6 and COND less than 1
*>           -4  => if MODE equals 6 or -6 and IDIST not in range 1 to 3
*>           -7  => if N negative
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
*> \ingroup double_matgen
*
*  =====================================================================
      SUBROUTINE DLATM7( MODE, COND, IRSIGN, IDIST, ISEED, D, N,
     $                   RANK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   COND
      INTEGER            IDIST, INFO, IRSIGN, MODE, N, RANK
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * )
      INTEGER            ISEED( 4 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D0 )
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
      DOUBLE PRECISION   HALF
      PARAMETER          ( HALF = 0.5D0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   ALPHA, TEMP
      INTEGER            I
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLARAN
      EXTERNAL           DLARAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLARNV, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, EXP, LOG
*     ..
*     .. Executable Statements ..
*
*     Decode and Test the input parameters. Initialize flags & seed.
*
      INFO = 0
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Set INFO if an error
*
      IF( MODE.LT.-6 .OR. MODE.GT.6 ) THEN
         INFO = -1
      ELSE IF( ( MODE.NE.-6 .AND. MODE.NE.0 .AND. MODE.NE.6 ) .AND.
     $         ( IRSIGN.NE.0 .AND. IRSIGN.NE.1 ) ) THEN
         INFO = -2
      ELSE IF( ( MODE.NE.-6 .AND. MODE.NE.0 .AND. MODE.NE.6 ) .AND.
     $         COND.LT.ONE ) THEN
         INFO = -3
      ELSE IF( ( MODE.EQ.6 .OR. MODE.EQ.-6 ) .AND.
     $         ( IDIST.LT.1 .OR. IDIST.GT.3 ) ) THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -7
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLATM7', -INFO )
         RETURN
      END IF
*
*     Compute D according to COND and MODE
*
      IF( MODE.NE.0 ) THEN
         GO TO ( 100, 130, 160, 190, 210, 230 )ABS( MODE )
*
*        One large D value:
*
  100    CONTINUE
         DO 110 I = 2, RANK
            D( I ) = ONE / COND
  110    CONTINUE
         DO 120 I = RANK + 1, N
            D( I ) = ZERO
  120    CONTINUE
         D( 1 ) = ONE
         GO TO 240
*
*        One small D value:
*
  130    CONTINUE
         DO 140 I = 1, RANK - 1
            D( I ) = ONE
  140    CONTINUE
         DO 150 I = RANK + 1, N
            D( I ) = ZERO
  150    CONTINUE
         D( RANK ) = ONE / COND
         GO TO 240
*
*        Exponentially distributed D values:
*
  160    CONTINUE
         D( 1 ) = ONE
         IF( N.GT.1 .AND. RANK.GT.1 ) THEN
            ALPHA = COND**( -ONE / DBLE( RANK-1 ) )
            DO 170 I = 2, RANK
               D( I ) = ALPHA**( I-1 )
  170       CONTINUE
            DO 180 I = RANK + 1, N
               D( I ) = ZERO
  180       CONTINUE
         END IF
         GO TO 240
*
*        Arithmetically distributed D values:
*
  190    CONTINUE
         D( 1 ) = ONE
         IF( N.GT.1 ) THEN
            TEMP = ONE / COND
            ALPHA = ( ONE-TEMP ) / DBLE( N-1 )
            DO 200 I = 2, N
               D( I ) = DBLE( N-I )*ALPHA + TEMP
  200       CONTINUE
         END IF
         GO TO 240
*
*        Randomly distributed D values on ( 1/COND , 1):
*
  210    CONTINUE
         ALPHA = LOG( ONE / COND )
         DO 220 I = 1, N
            D( I ) = EXP( ALPHA*DLARAN( ISEED ) )
  220    CONTINUE
         GO TO 240
*
*        Randomly distributed D values from IDIST
*
  230    CONTINUE
         CALL DLARNV( IDIST, ISEED, N, D )
*
  240    CONTINUE
*
*        If MODE neither -6 nor 0 nor 6, and IRSIGN = 1, assign
*        random signs to D
*
         IF( ( MODE.NE.-6 .AND. MODE.NE.0 .AND. MODE.NE.6 ) .AND.
     $       IRSIGN.EQ.1 ) THEN
            DO 250 I = 1, N
               TEMP = DLARAN( ISEED )
               IF( TEMP.GT.HALF )
     $            D( I ) = -D( I )
  250       CONTINUE
         END IF
*
*        Reverse if MODE < 0
*
         IF( MODE.LT.0 ) THEN
            DO 260 I = 1, N / 2
               TEMP = D( I )
               D( I ) = D( N+1-I )
               D( N+1-I ) = TEMP
  260       CONTINUE
         END IF
*
      END IF
*
      RETURN
*
*     End of DLATM7
*
      END
