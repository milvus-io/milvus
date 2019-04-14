*> \brief \b CLATME
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLATME( N, DIST, ISEED, D, MODE, COND, DMAX,
*         RSIGN,
*                          UPPER, SIM, DS, MODES, CONDS, KL, KU, ANORM,
*         A,
*                          LDA, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIST, RSIGN, SIM, UPPER
*       INTEGER            INFO, KL, KU, LDA, MODE, MODES, N
*       REAL               ANORM, COND, CONDS
*       COMPLEX            DMAX
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 )
*       REAL               DS( * )
*       COMPLEX            A( LDA, * ), D( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CLATME generates random non-symmetric square matrices with
*>    specified eigenvalues for testing LAPACK programs.
*>
*>    CLATME operates by applying the following sequence of
*>    operations:
*>
*>    1. Set the diagonal to D, where D may be input or
*>         computed according to MODE, COND, DMAX, and RSIGN
*>         as described below.
*>
*>    2. If UPPER='T', the upper triangle of A is set to random values
*>         out of distribution DIST.
*>
*>    3. If SIM='T', A is multiplied on the left by a random matrix
*>         X, whose singular values are specified by DS, MODES, and
*>         CONDS, and on the right by X inverse.
*>
*>    4. If KL < N-1, the lower bandwidth is reduced to KL using
*>         Householder transformations.  If KU < N-1, the upper
*>         bandwidth is reduced to KU.
*>
*>    5. If ANORM is not negative, the matrix is scaled to have
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
*>           to generate the random eigen-/singular values, and on the
*>           upper triangle (see UPPER).
*>           'U' => UNIFORM( 0, 1 )  ( 'U' for uniform )
*>           'S' => UNIFORM( -1, 1 ) ( 'S' for symmetric )
*>           'N' => NORMAL( 0, 1 )   ( 'N' for normal )
*>           'D' => uniform on the complex disc |z| < 1.
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
*>           exit, and can be used in the next call to CLATME
*>           to continue the same random number sequence.
*>           Changed on exit.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is COMPLEX array, dimension ( N )
*>           This array is used to specify the eigenvalues of A.  If
*>           MODE=0, then D is assumed to contain the eigenvalues
*>           otherwise they will be computed according to MODE, COND,
*>           DMAX, and RSIGN and placed in D.
*>           Modified if MODE is nonzero.
*> \endverbatim
*>
*> \param[in] MODE
*> \verbatim
*>          MODE is INTEGER
*>           On entry this describes how the eigenvalues are to
*>           be specified:
*>           MODE = 0 means use D as input
*>           MODE = 1 sets D(1)=1 and D(2:N)=1.0/COND
*>           MODE = 2 sets D(1:N-1)=1 and D(N)=1.0/COND
*>           MODE = 3 sets D(I)=COND**(-(I-1)/(N-1))
*>           MODE = 4 sets D(i)=1 - (i-1)/(N-1)*(1 - 1/COND)
*>           MODE = 5 sets D to random numbers in the range
*>                    ( 1/COND , 1 ) such that their logarithms
*>                    are uniformly distributed.
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
*>          DMAX is COMPLEX
*>           If MODE is neither -6, 0 nor 6, the contents of D, as
*>           computed according to MODE and COND, will be scaled by
*>           DMAX / max(abs(D(i))).  Note that DMAX need not be
*>           positive or real: if DMAX is negative or complex (or zero),
*>           D will be scaled by a negative or complex number (or zero).
*>           If RSIGN='F' then the largest (absolute) eigenvalue will be
*>           equal to DMAX.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] RSIGN
*> \verbatim
*>          RSIGN is CHARACTER*1
*>           If MODE is not 0, 6, or -6, and RSIGN='T', then the
*>           elements of D, as computed according to MODE and COND, will
*>           be multiplied by a random complex number from the unit
*>           circle |z| = 1.  If RSIGN='F', they will not be.  RSIGN may
*>           only have the values 'T' or 'F'.
*>           Not modified.
*> \endverbatim
*>
*> \param[in] UPPER
*> \verbatim
*>          UPPER is CHARACTER*1
*>           If UPPER='T', then the elements of A above the diagonal
*>           will be set to random numbers out of DIST.  If UPPER='F',
*>           they will not.  UPPER may only have the values 'T' or 'F'.
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
*>           Similar to MODE and COND, but for specifying the diagonal
*>           of S.  MODES=-6 and +6 are not allowed (since they would
*>           result in randomly ill-conditioned eigenvalues.)
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>           This specifies the lower bandwidth of the  matrix.  KL=1
*>           specifies upper Hessenberg form.  If KL is at least N-1,
*>           then A will have full lower bandwidth.
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
*>           KU and KL may be less than N-1.
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
*>          A is COMPLEX array, dimension ( LDA, N )
*>           On exit A is the desired test matrix.
*>           Modified.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>           LDA specifies the first dimension of A as declared in the
*>           calling program.  LDA must be at least M.
*>           Not modified.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension ( 3*N )
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
*>            -9 => RSIGN is not 'T' or 'F'
*>           -10 => UPPER is not 'T' or 'F'
*>           -11 => SIM   is not 'T' or 'F'
*>           -12 => MODES=0 and DS has a zero singular value.
*>           -13 => MODES is not in the range -5 to 5.
*>           -14 => MODES is nonzero and CONDS is less than 1.
*>           -15 => KL is less than 1.
*>           -16 => KU is less than 1, or KL and KU are both less than
*>                  N-1.
*>           -19 => LDA is less than M.
*>            1  => Error return from CLATM1 (computing D)
*>            2  => Cannot scale to DMAX (max. eigenvalue is 0)
*>            3  => Error return from SLATM1 (computing DS)
*>            4  => Error return from CLARGE
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
*> \ingroup complex_matgen
*
*  =====================================================================
      SUBROUTINE CLATME( N, DIST, ISEED, D, MODE, COND, DMAX,
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
      REAL               ANORM, COND, CONDS
      COMPLEX            DMAX
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
      REAL               DS( * )
      COMPLEX            A( LDA, * ), D( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
      REAL               ONE
      PARAMETER          ( ONE = 1.0E+0 )
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ) )
      COMPLEX            CONE
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADS
      INTEGER            I, IC, ICOLS, IDIST, IINFO, IR, IROWS, IRSIGN,
     $                   ISIM, IUPPER, J, JC, JCR
      REAL               RALPHA, TEMP
      COMPLEX            ALPHA, TAU, XNORMS
*     ..
*     .. Local Arrays ..
      REAL               TEMPA( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANGE
      COMPLEX            CLARND
      EXTERNAL           LSAME, CLANGE, CLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           CCOPY, CGEMV, CGERC, CLACGV, CLARFG, CLARGE,
     $                   CLARNV, CLATM1, CLASET, CSCAL, CSSCAL, SLATM1,
     $                   XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CONJG, MAX, MOD
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
      ELSE IF( LSAME( DIST, 'D' ) ) THEN
         IDIST = 4
      ELSE
         IDIST = -1
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
         DO 10 J = 1, N
            IF( DS( J ).EQ.ZERO )
     $         BADS = .TRUE.
   10    CONTINUE
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
         CALL XERBLA( 'CLATME', -INFO )
         RETURN
      END IF
*
*     Initialize random number generator
*
      DO 20 I = 1, 4
         ISEED( I ) = MOD( ABS( ISEED( I ) ), 4096 )
   20 CONTINUE
*
      IF( MOD( ISEED( 4 ), 2 ).NE.1 )
     $   ISEED( 4 ) = ISEED( 4 ) + 1
*
*     2)      Set up diagonal of A
*
*             Compute D according to COND and MODE
*
      CALL CLATM1( MODE, COND, IRSIGN, IDIST, ISEED, D, N, IINFO )
      IF( IINFO.NE.0 ) THEN
         INFO = 1
         RETURN
      END IF
      IF( MODE.NE.0 .AND. ABS( MODE ).NE.6 ) THEN
*
*        Scale by DMAX
*
         TEMP = ABS( D( 1 ) )
         DO 30 I = 2, N
            TEMP = MAX( TEMP, ABS( D( I ) ) )
   30    CONTINUE
*
         IF( TEMP.GT.ZERO ) THEN
            ALPHA = DMAX / TEMP
         ELSE
            INFO = 2
            RETURN
         END IF
*
         CALL CSCAL( N, ALPHA, D, 1 )
*
      END IF
*
      CALL CLASET( 'Full', N, N, CZERO, CZERO, A, LDA )
      CALL CCOPY( N, D, 1, A, LDA+1 )
*
*     3)      If UPPER='T', set upper triangle of A to random numbers.
*
      IF( IUPPER.NE.0 ) THEN
         DO 40 JC = 2, N
            CALL CLARNV( IDIST, ISEED, JC-1, A( 1, JC ) )
   40    CONTINUE
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
         CALL CLARGE( N, A, LDA, ISEED, WORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = 4
            RETURN
         END IF
*
*        Multiply by S and (1/S)
*
         DO 50 J = 1, N
            CALL CSSCAL( N, DS( J ), A( J, 1 ), LDA )
            IF( DS( J ).NE.ZERO ) THEN
               CALL CSSCAL( N, ONE / DS( J ), A( 1, J ), 1 )
            ELSE
               INFO = 5
               RETURN
            END IF
   50    CONTINUE
*
*        Multiply by U and U'
*
         CALL CLARGE( N, A, LDA, ISEED, WORK, IINFO )
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
         DO 60 JCR = KL + 1, N - 1
            IC = JCR - KL
            IROWS = N + 1 - JCR
            ICOLS = N + KL - JCR
*
            CALL CCOPY( IROWS, A( JCR, IC ), 1, WORK, 1 )
            XNORMS = WORK( 1 )
            CALL CLARFG( IROWS, XNORMS, WORK( 2 ), 1, TAU )
            TAU = CONJG( TAU )
            WORK( 1 ) = CONE
            ALPHA = CLARND( 5, ISEED )
*
            CALL CGEMV( 'C', IROWS, ICOLS, CONE, A( JCR, IC+1 ), LDA,
     $                  WORK, 1, CZERO, WORK( IROWS+1 ), 1 )
            CALL CGERC( IROWS, ICOLS, -TAU, WORK, 1, WORK( IROWS+1 ), 1,
     $                  A( JCR, IC+1 ), LDA )
*
            CALL CGEMV( 'N', N, IROWS, CONE, A( 1, JCR ), LDA, WORK, 1,
     $                  CZERO, WORK( IROWS+1 ), 1 )
            CALL CGERC( N, IROWS, -CONJG( TAU ), WORK( IROWS+1 ), 1,
     $                  WORK, 1, A( 1, JCR ), LDA )
*
            A( JCR, IC ) = XNORMS
            CALL CLASET( 'Full', IROWS-1, 1, CZERO, CZERO,
     $                   A( JCR+1, IC ), LDA )
*
            CALL CSCAL( ICOLS+1, ALPHA, A( JCR, IC ), LDA )
            CALL CSCAL( N, CONJG( ALPHA ), A( 1, JCR ), 1 )
   60    CONTINUE
      ELSE IF( KU.LT.N-1 ) THEN
*
*        Reduce upper bandwidth -- kill a row at a time.
*
         DO 70 JCR = KU + 1, N - 1
            IR = JCR - KU
            IROWS = N + KU - JCR
            ICOLS = N + 1 - JCR
*
            CALL CCOPY( ICOLS, A( IR, JCR ), LDA, WORK, 1 )
            XNORMS = WORK( 1 )
            CALL CLARFG( ICOLS, XNORMS, WORK( 2 ), 1, TAU )
            TAU = CONJG( TAU )
            WORK( 1 ) = CONE
            CALL CLACGV( ICOLS-1, WORK( 2 ), 1 )
            ALPHA = CLARND( 5, ISEED )
*
            CALL CGEMV( 'N', IROWS, ICOLS, CONE, A( IR+1, JCR ), LDA,
     $                  WORK, 1, CZERO, WORK( ICOLS+1 ), 1 )
            CALL CGERC( IROWS, ICOLS, -TAU, WORK( ICOLS+1 ), 1, WORK, 1,
     $                  A( IR+1, JCR ), LDA )
*
            CALL CGEMV( 'C', ICOLS, N, CONE, A( JCR, 1 ), LDA, WORK, 1,
     $                  CZERO, WORK( ICOLS+1 ), 1 )
            CALL CGERC( ICOLS, N, -CONJG( TAU ), WORK, 1,
     $                  WORK( ICOLS+1 ), 1, A( JCR, 1 ), LDA )
*
            A( IR, JCR ) = XNORMS
            CALL CLASET( 'Full', 1, ICOLS-1, CZERO, CZERO,
     $                   A( IR, JCR+1 ), LDA )
*
            CALL CSCAL( IROWS+1, ALPHA, A( IR, JCR ), 1 )
            CALL CSCAL( N, CONJG( ALPHA ), A( JCR, 1 ), LDA )
   70    CONTINUE
      END IF
*
*     Scale the matrix to have norm ANORM
*
      IF( ANORM.GE.ZERO ) THEN
         TEMP = CLANGE( 'M', N, N, A, LDA, TEMPA )
         IF( TEMP.GT.ZERO ) THEN
            RALPHA = ANORM / TEMP
            DO 80 J = 1, N
               CALL CSSCAL( N, RALPHA, A( 1, J ), 1 )
   80       CONTINUE
         END IF
      END IF
*
      RETURN
*
*     End of CLATME
*
      END
