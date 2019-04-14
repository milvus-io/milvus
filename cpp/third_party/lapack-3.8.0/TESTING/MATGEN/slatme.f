*> \brief \b SLATME
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLATME( N, DIST, ISEED, D, MODE, COND, DMAX, EI,
*         RSIGN,
*                          UPPER, SIM, DS, MODES, CONDS, KL, KU, ANORM,
*         A,
*                          LDA, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIST, RSIGN, SIM, UPPER
*       INTEGER            INFO, KL, KU, LDA, MODE, MODES, N
*       REAL               ANORM, COND, CONDS, DMAX
*       ..
*       .. Array Arguments ..
*       CHARACTER          EI( * )
*       INTEGER            ISEED( 4 )
*       REAL               A( LDA, * ), D( * ), DS( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    SLATME generates random non-symmetric square matrices with
*>    specified eigenvalues for testing LAPACK programs.
*>
*>    SLATME operates by applying the following sequence of
*>    operations:
*>
*>    1. Set the diagonal to D, where D may be input or
*>         computed according to MODE, COND, DMAX, and RSIGN
*>         as described below.
*>
*>    2. If complex conjugate pairs are desired (MODE=0 and EI(1)='R',
*>         or MODE=5), certain pairs of adjacent elements of D are
*>         interpreted as the real and complex parts of a complex
*>         conjugate pair; A thus becomes block diagonal, with 1x1
*>         and 2x2 blocks.
*>
*>    3. If UPPER='T', the upper triangle of A is set to random values
*>         out of distribution DIST.
*>
*>    4. If SIM='T', A is multiplied on the left by a random matrix
*>         X, whose singular values are specified by DS, MODES, and
*>         CONDS, and on the right by X inverse.
*>
*>    5. If KL < N-1, the lower bandwidth is reduced to KL using
*>         Householder transformations.  If KU < N-1, the upper
*>         bandwidth is reduced to KU.
*>
*>    6. If ANORM is not negative, the matrix is scaled to have
*>         maximum-element-norm ANORM.
*>
*>    (Note: since the matrix cannot be reduced beyond Hessenberg form,
*>     no packing options are available.)
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           The number of columns (or rows) of A. Not modified.
*> \endverbatim
*>
*> \param[in] DIST
*> \verbatim
*>          DIST is CHARACTER*1
*>           On entry, DIST specifies the type of distribution to be used
*>           to generate the random eigen-/singular values, and for the
*>           upper triangle (see UPPER).
*>           'U' => UNIFORM( 0, 1 )  ( 'U' for uniform )
*>           'S' => UNIFORM( -1, 1 ) ( 'S' for symmetric )
*>           'N' => NORMAL( 0, 1 )   ( 'N' for normal )
*>           Not modified.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension ( 4 )
*>           On entry ISEED specifies the seed of the random number
*>           generator. They should lie between 0 and 4095 inclusive,
*>           and ISEED(4) should be odd. The random number generator
*>           uses a linear congruential sequence limited to small
*>           integers, and so should produce machine independent
*>           random numbers. The values of ISEED are changed on
*>           exit, and can be used in the next call to SLATME
*>           to continue the same random number sequence.
*>           Changed on exit.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL array, dimension ( N )
*>           This array is used to specify the eigenvalues of A.  If
*>           MODE=0, then D is assumed to contain the eigenvalues (but
*>           see the description of EI), otherwise they will be
*>           computed according to MODE, COND, DMAX, and RSIGN and
*>           placed in D.
*>           Modified if MODE is nonzero.
*> \endverbatim
*>
*> \param[in] MODE
*> \verbatim
*>          MODE is INTEGER
*>           On entry this describes how the eigenvalues are to
*>           be specified:
*>           MODE = 0 means use D (with EI) as input
*>           MODE = 1 sets D(1)=1 and D(2:N)=1.0/COND
*>           MODE = 2 sets D(1:N-1)=1 and D(N)=1.0/COND
*>           MODE = 3 sets D(I)=COND**(-(I-1)/(N-1))
*>           MODE = 4 sets D(i)=1 - (i-1)/(N-1)*(1 - 1/COND)
*>           MODE = 5 sets D to random numbers in the range
*>                    ( 1/COND , 1 ) such that their logarithms
*>                    are uniformly distributed.  Each odd-even pair
*>                    of elements will be either used as two real
*>                    eigenvalues or as the real and imaginary part
*>                    of a complex conjugate pair of eigenvalues;
*>                    the choice of which is done is random, with
*>                    50-50 probability, for each pair.
*>           MODE = 6 set D to random numbers from same distribution
*>                    as the rest of the matrix.
*>           MODE < 0 has the same meaning as ABS(MODE), except that
*>              the order of the elements of D is reversed.
*>           Thus if MODE is between 1 and 4, D has entries ranging
*>              from 1 to 1/COND, if between -1 and -4, D has entries
*>              ranging from 1/COND to 1,
*>           Not modified.
*> \endverbatim
*>
*> \param[in] COND
*> \verbatim
*>          COND is REAL
*>           On entry, this is used as described under MODE above.
*>           If used, it must be >= 1. Not modified.
*> \endverbatim
*>
*> \param[in] DMAX
*> \verbatim
*>          DMAX is REAL
*>           If MODE is neither -6, 0 nor 6, the contents of D, as
*>           computed according to MODE and COND, will be scaled by
*>           DMAX / max(abs(D(i))).  Note that DMAX need not be
*>           positive: if DMAX is negative (or zero), D will be
*>           scaled by a negative number (or zero).
*>           Not modified.
*> \endverbatim
*>
*> \param[in] EI
*> \verbatim
*>          EI is CHARACTER*1 array, dimension ( N )
*>           If MODE is 0, and EI(1) is not ' ' (space character),
*>           this array specifies which elements of D (on input) are
*>           real eigenvalues and which are the real and imaginary parts
*>           of a complex conjugate pair of eigenvalues.  The elements
*>           of EI may then only have the values 'R' and 'I'.  If
*>           EI(j)='R' and EI(j+1)='I', then the j-th eigenvalue is
*>           CMPLX( D(j) , D(j+1) ), and the (j+1)-th is the complex
*>           conjugate thereof.  If EI(j)=EI(j+1)='R', then the j-th
*>           eigenvalue is D(j) (i.e., real).  EI(1) may not be 'I',
*>           nor may two adjacent elements of EI both have the value 'I'.
*>           If MODE is not 0, then EI is ignored.  If MODE is 0 and
*>           EI(1)=' ', then the eigenvalues will all be real.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] RSIGN
*> \verbatim
*>          RSIGN is CHARACTER*1
*>           If MODE is not 0, 6, or -6, and RSIGN='T', then the
*>           elements of D, as computed according to MODE and COND, will
*>           be multiplied by a random sign (+1 or -1).  If RSIGN='F',
*>           they will not be.  RSIGN may only have the values 'T' or
*>           'F'.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] UPPER
*> \verbatim
*>          UPPER is CHARACTER*1
*>           If UPPER='T', then the elements of A above the diagonal
*>           (and above the 2x2 diagonal blocks, if A has complex
*>           eigenvalues) will be set to random numbers out of DIST.
*>           If UPPER='F', they will not.  UPPER may only have the
*>           values 'T' or 'F'.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] SIM
*> \verbatim
*>          SIM is CHARACTER*1
*>           If SIM='T', then A will be operated on by a "similarity
*>           transform", i.e., multiplied on the left by a matrix X and
*>           on the right by X inverse.  X = U S V, where U and V are
*>           random unitary matrices and S is a (diagonal) matrix of
*>           singular values specified by DS, MODES, and CONDS.  If
*>           SIM='F', then A will not be transformed.
*>           Not modified.
*> \endverbatim
*>
*> \param[in,out] DS
*> \verbatim
*>          DS is REAL array, dimension ( N )
*>           This array is used to specify the singular values of X,
*>           in the same way that D specifies the eigenvalues of A.
*>           If MODE=0, the DS contains the singular values, which
*>           may not be zero.
*>           Modified if MODE is nonzero.
*> \endverbatim
*>
*> \param[in] MODES
*> \verbatim
*>          MODES is INTEGER
*> \endverbatim
*>
*> \param[in] CONDS
*> \verbatim
*>          CONDS is REAL
*>           Same as MODE and COND, but for specifying the diagonal
*>           of S.  MODES=-6 and +6 are not allowed (since they would
*>           result in randomly ill-conditioned eigenvalues.)
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>           This specifies the lower bandwidth of the  matrix.  KL=1
*>           specifies upper Hessenberg form.  If KL is at least N-1,
*>           then A will have full lower bandwidth.  KL must be at
*>           least 1.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>           This specifies the upper bandwidth of the  matrix.  KU=1
*>           specifies lower Hessenberg form.  If KU is at least N-1,
*>           then A will have full upper bandwidth; if KU and KL
*>           are both at least N-1, then A will be dense.  Only one of
*>           KU and KL may be less than N-1.  KU must be at least 1.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] ANORM
*> \verbatim
*>          ANORM is REAL
*>           If ANORM is not negative, then A will be scaled by a non-
*>           negative real number to make the maximum-element-norm of A
*>           to be ANORM.
*>           Not modified.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is REAL array, dimension ( LDA, N )
*>           On exit A is the desired test matrix.
*>           Modified.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>           LDA specifies the first dimension of A as declared in the
*>           calling program.  LDA must be at least N.
*>           Not modified.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension ( 3*N )
*>           Workspace.
*>           Modified.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           Error code.  On exit, INFO will be set to one of the
*>           following values:
*>             0 => normal return
*>            -1 => N negative
*>            -2 => DIST illegal string
*>            -5 => MODE not in range -6 to 6
*>            -6 => COND less than 1.0, and MODE neither -6, 0 nor 6
*>            -8 => EI(1) is not ' ' or 'R', EI(j) is not 'R' or 'I', or
*>                  two adjacent elements of EI are 'I'.
*>            -9 => RSIGN is not 'T' or 'F'
*>           -10 => UPPER is not 'T' or 'F'
*>           -11 => SIM   is not 'T' or 'F'
*>           -12 => MODES=0 and DS has a zero singular value.
*>           -13 => MODES is not in the range -5 to 5.
*>           -14 => MODES is nonzero and CONDS is less than 1.
*>           -15 => KL is less than 1.
*>           -16 => KU is less than 1, or KL and KU are both less than
*>                  N-1.
*>           -19 => LDA is less than N.
*>            1  => Error return from SLATM1 (computing D)
*>            2  => Cannot scale to DMAX (max. eigenvalue is 0)
*>            3  => Error return from SLATM1 (computing DS)
*>            4  => Error return from SLARGE
*>            5  => Zero singular value from SLATM1.
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
*> \ingroup real_matgen
*
*  =====================================================================
      SUBROUTINE SLATME( N, DIST, ISEED, D, MODE, COND, DMAX, EI,
     $  RSIGN,
     $                   UPPER, SIM, DS, MODES, CONDS, KL, KU, ANORM,
     $  A,
     $                   LDA, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIST, RSIGN, SIM, UPPER
      INTEGER            INFO, KL, KU, LDA, MODE, MODES, N
      REAL               ANORM, COND, CONDS, DMAX
*     ..
*     .. Array Arguments ..
      CHARACTER          EI( * )
      INTEGER            ISEED( 4 )
      REAL               A( LDA, * ), D( * ), DS( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E0 )
      REAL               ONE
      PARAMETER          ( ONE = 1.0E0 )
      REAL               HALF
      PARAMETER          ( HALF = 1.0E0 / 2.0E0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADEI, BADS, USEEI
      INTEGER            I, IC, ICOLS, IDIST, IINFO, IR, IROWS, IRSIGN,
     $                   ISIM, IUPPER, J, JC, JCR, JR
      REAL               ALPHA, TAU, TEMP, XNORMS
*     ..
*     .. Local Arrays ..
      REAL               TEMPA( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLANGE, SLARAN
      EXTERNAL           LSAME, SLANGE, SLARAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEMV, SGER, SLARFG, SLARGE, SLARNV,
     $                   SLATM1, SLASET, SSCAL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MOD
*     ..
*     .. Executable Statements ..
*
*     1)      Decode and Test the input parameters.
*             Initialize flags & seed.
*
      INFO = 0
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Decode DIST
*
      IF( LSAME( DIST, 'U' ) ) THEN
         IDIST = 1
      ELSE IF( LSAME( DIST, 'S' ) ) THEN
         IDIST = 2
      ELSE IF( LSAME( DIST, 'N' ) ) THEN
         IDIST = 3
      ELSE
         IDIST = -1
      END IF
*
*     Check EI
*
      USEEI = .TRUE.
      BADEI = .FALSE.
      IF( LSAME( EI( 1 ), ' ' ) .OR. MODE.NE.0 ) THEN
         USEEI = .FALSE.
      ELSE
         IF( LSAME( EI( 1 ), 'R' ) ) THEN
            DO 10 J = 2, N
               IF( LSAME( EI( J ), 'I' ) ) THEN
                  IF( LSAME( EI( J-1 ), 'I' ) )
     $               BADEI = .TRUE.
               ELSE
                  IF( .NOT.LSAME( EI( J ), 'R' ) )
     $               BADEI = .TRUE.
               END IF
   10       CONTINUE
         ELSE
            BADEI = .TRUE.
         END IF
      END IF
*
*     Decode RSIGN
*
      IF( LSAME( RSIGN, 'T' ) ) THEN
         IRSIGN = 1
      ELSE IF( LSAME( RSIGN, 'F' ) ) THEN
         IRSIGN = 0
      ELSE
         IRSIGN = -1
      END IF
*
*     Decode UPPER
*
      IF( LSAME( UPPER, 'T' ) ) THEN
         IUPPER = 1
      ELSE IF( LSAME( UPPER, 'F' ) ) THEN
         IUPPER = 0
      ELSE
         IUPPER = -1
      END IF
*
*     Decode SIM
*
      IF( LSAME( SIM, 'T' ) ) THEN
         ISIM = 1
      ELSE IF( LSAME( SIM, 'F' ) ) THEN
         ISIM = 0
      ELSE
         ISIM = -1
      END IF
*
*     Check DS, if MODES=0 and ISIM=1
*
      BADS = .FALSE.
      IF( MODES.EQ.0 .AND. ISIM.EQ.1 ) THEN
         DO 20 J = 1, N
            IF( DS( J ).EQ.ZERO )
     $         BADS = .TRUE.
   20    CONTINUE
      END IF
*
*     Set INFO if an error
*
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( IDIST.EQ.-1 ) THEN
         INFO = -2
      ELSE IF( ABS( MODE ).GT.6 ) THEN
         INFO = -5
      ELSE IF( ( MODE.NE.0 .AND. ABS( MODE ).NE.6 ) .AND. COND.LT.ONE )
     $          THEN
         INFO = -6
      ELSE IF( BADEI ) THEN
         INFO = -8
      ELSE IF( IRSIGN.EQ.-1 ) THEN
         INFO = -9
      ELSE IF( IUPPER.EQ.-1 ) THEN
         INFO = -10
      ELSE IF( ISIM.EQ.-1 ) THEN
         INFO = -11
      ELSE IF( BADS ) THEN
         INFO = -12
      ELSE IF( ISIM.EQ.1 .AND. ABS( MODES ).GT.5 ) THEN
         INFO = -13
      ELSE IF( ISIM.EQ.1 .AND. MODES.NE.0 .AND. CONDS.LT.ONE ) THEN
         INFO = -14
      ELSE IF( KL.LT.1 ) THEN
         INFO = -15
      ELSE IF( KU.LT.1 .OR. ( KU.LT.N-1 .AND. KL.LT.N-1 ) ) THEN
         INFO = -16
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -19
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLATME', -INFO )
         RETURN
      END IF
*
*     Initialize random number generator
*
      DO 30 I = 1, 4
         ISEED( I ) = MOD( ABS( ISEED( I ) ), 4096 )
   30 CONTINUE
*
      IF( MOD( ISEED( 4 ), 2 ).NE.1 )
     $   ISEED( 4 ) = ISEED( 4 ) + 1
*
*     2)      Set up diagonal of A
*
*             Compute D according to COND and MODE
*
      CALL SLATM1( MODE, COND, IRSIGN, IDIST, ISEED, D, N, IINFO )
      IF( IINFO.NE.0 ) THEN
         INFO = 1
         RETURN
      END IF
      IF( MODE.NE.0 .AND. ABS( MODE ).NE.6 ) THEN
*
*        Scale by DMAX
*
         TEMP = ABS( D( 1 ) )
         DO 40 I = 2, N
            TEMP = MAX( TEMP, ABS( D( I ) ) )
   40    CONTINUE
*
         IF( TEMP.GT.ZERO ) THEN
            ALPHA = DMAX / TEMP
         ELSE IF( DMAX.NE.ZERO ) THEN
            INFO = 2
            RETURN
         ELSE
            ALPHA = ZERO
         END IF
*
         CALL SSCAL( N, ALPHA, D, 1 )
*
      END IF
*
      CALL SLASET( 'Full', N, N, ZERO, ZERO, A, LDA )
      CALL SCOPY( N, D, 1, A, LDA+1 )
*
*     Set up complex conjugate pairs
*
      IF( MODE.EQ.0 ) THEN
         IF( USEEI ) THEN
            DO 50 J = 2, N
               IF( LSAME( EI( J ), 'I' ) ) THEN
                  A( J-1, J ) = A( J, J )
                  A( J, J-1 ) = -A( J, J )
                  A( J, J ) = A( J-1, J-1 )
               END IF
   50       CONTINUE
         END IF
*
      ELSE IF( ABS( MODE ).EQ.5 ) THEN
*
         DO 60 J = 2, N, 2
            IF( SLARAN( ISEED ).GT.HALF ) THEN
               A( J-1, J ) = A( J, J )
               A( J, J-1 ) = -A( J, J )
               A( J, J ) = A( J-1, J-1 )
            END IF
   60    CONTINUE
      END IF
*
*     3)      If UPPER='T', set upper triangle of A to random numbers.
*             (but don't modify the corners of 2x2 blocks.)
*
      IF( IUPPER.NE.0 ) THEN
         DO 70 JC = 2, N
            IF( A( JC-1, JC ).NE.ZERO ) THEN
               JR = JC - 2
            ELSE
               JR = JC - 1
            END IF
            CALL SLARNV( IDIST, ISEED, JR, A( 1, JC ) )
   70    CONTINUE
      END IF
*
*     4)      If SIM='T', apply similarity transformation.
*
*                                -1
*             Transform is  X A X  , where X = U S V, thus
*
*             it is  U S V A V' (1/S) U'
*
      IF( ISIM.NE.0 ) THEN
*
*        Compute S (singular values of the eigenvector matrix)
*        according to CONDS and MODES
*
         CALL SLATM1( MODES, CONDS, 0, 0, ISEED, DS, N, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = 3
            RETURN
         END IF
*
*        Multiply by V and V'
*
         CALL SLARGE( N, A, LDA, ISEED, WORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = 4
            RETURN
         END IF
*
*        Multiply by S and (1/S)
*
         DO 80 J = 1, N
            CALL SSCAL( N, DS( J ), A( J, 1 ), LDA )
            IF( DS( J ).NE.ZERO ) THEN
               CALL SSCAL( N, ONE / DS( J ), A( 1, J ), 1 )
            ELSE
               INFO = 5
               RETURN
            END IF
   80    CONTINUE
*
*        Multiply by U and U'
*
         CALL SLARGE( N, A, LDA, ISEED, WORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = 4
            RETURN
         END IF
      END IF
*
*     5)      Reduce the bandwidth.
*
      IF( KL.LT.N-1 ) THEN
*
*        Reduce bandwidth -- kill column
*
         DO 90 JCR = KL + 1, N - 1
            IC = JCR - KL
            IROWS = N + 1 - JCR
            ICOLS = N + KL - JCR
*
            CALL SCOPY( IROWS, A( JCR, IC ), 1, WORK, 1 )
            XNORMS = WORK( 1 )
            CALL SLARFG( IROWS, XNORMS, WORK( 2 ), 1, TAU )
            WORK( 1 ) = ONE
*
            CALL SGEMV( 'T', IROWS, ICOLS, ONE, A( JCR, IC+1 ), LDA,
     $                  WORK, 1, ZERO, WORK( IROWS+1 ), 1 )
            CALL SGER( IROWS, ICOLS, -TAU, WORK, 1, WORK( IROWS+1 ), 1,
     $                 A( JCR, IC+1 ), LDA )
*
            CALL SGEMV( 'N', N, IROWS, ONE, A( 1, JCR ), LDA, WORK, 1,
     $                  ZERO, WORK( IROWS+1 ), 1 )
            CALL SGER( N, IROWS, -TAU, WORK( IROWS+1 ), 1, WORK, 1,
     $                 A( 1, JCR ), LDA )
*
            A( JCR, IC ) = XNORMS
            CALL SLASET( 'Full', IROWS-1, 1, ZERO, ZERO, A( JCR+1, IC ),
     $                   LDA )
   90    CONTINUE
      ELSE IF( KU.LT.N-1 ) THEN
*
*        Reduce upper bandwidth -- kill a row at a time.
*
         DO 100 JCR = KU + 1, N - 1
            IR = JCR - KU
            IROWS = N + KU - JCR
            ICOLS = N + 1 - JCR
*
            CALL SCOPY( ICOLS, A( IR, JCR ), LDA, WORK, 1 )
            XNORMS = WORK( 1 )
            CALL SLARFG( ICOLS, XNORMS, WORK( 2 ), 1, TAU )
            WORK( 1 ) = ONE
*
            CALL SGEMV( 'N', IROWS, ICOLS, ONE, A( IR+1, JCR ), LDA,
     $                  WORK, 1, ZERO, WORK( ICOLS+1 ), 1 )
            CALL SGER( IROWS, ICOLS, -TAU, WORK( ICOLS+1 ), 1, WORK, 1,
     $                 A( IR+1, JCR ), LDA )
*
            CALL SGEMV( 'C', ICOLS, N, ONE, A( JCR, 1 ), LDA, WORK, 1,
     $                  ZERO, WORK( ICOLS+1 ), 1 )
            CALL SGER( ICOLS, N, -TAU, WORK, 1, WORK( ICOLS+1 ), 1,
     $                 A( JCR, 1 ), LDA )
*
            A( IR, JCR ) = XNORMS
            CALL SLASET( 'Full', 1, ICOLS-1, ZERO, ZERO, A( IR, JCR+1 ),
     $                   LDA )
  100    CONTINUE
      END IF
*
*     Scale the matrix to have norm ANORM
*
      IF( ANORM.GE.ZERO ) THEN
         TEMP = SLANGE( 'M', N, N, A, LDA, TEMPA )
         IF( TEMP.GT.ZERO ) THEN
            ALPHA = ANORM / TEMP
            DO 110 J = 1, N
               CALL SSCAL( N, ALPHA, A( 1, J ), 1 )
  110       CONTINUE
         END IF
      END IF
*
      RETURN
*
*     End of SLATME
*
      END
