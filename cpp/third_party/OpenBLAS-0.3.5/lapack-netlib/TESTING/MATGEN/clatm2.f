*> \brief \b CLATM2
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       COMPLEX FUNCTION CLATM2( M, N, I, J, KL, KU, IDIST, ISEED, D,
*                                IGRADE, DL, DR, IPVTNG, IWORK, SPARSE )
*
*       .. Scalar Arguments ..
*
*       INTEGER            I, IDIST, IGRADE, IPVTNG, J, KL, KU, M, N
*       REAL               SPARSE
*       ..
*
*       .. Array Arguments ..
*
*       INTEGER            ISEED( 4 ), IWORK( * )
*       COMPLEX            D( * ), DL( * ), DR( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CLATM2 returns the (I,J) entry of a random matrix of dimension
*>    (M, N) described by the other parameters. It is called by the
*>    CLATMR routine in order to build random test matrices. No error
*>    checking on parameters is done, because this routine is called in
*>    a tight loop by CLATMR which has already checked the parameters.
*>
*>    Use of CLATM2 differs from CLATM3 in the order in which the random
*>    number generator is called to fill in random matrix entries.
*>    With CLATM2, the generator is called to fill in the pivoted matrix
*>    columnwise. With CLATM3, the generator is called to fill in the
*>    matrix columnwise, after which it is pivoted. Thus, CLATM3 can
*>    be used to construct random matrices which differ only in their
*>    order of rows and/or columns. CLATM2 is used to construct band
*>    matrices while avoiding calling the random number generator for
*>    entries outside the band (and therefore generating random numbers
*>
*>    The matrix whose (I,J) entry is returned is constructed as
*>    follows (this routine only computes one entry):
*>
*>      If I is outside (1..M) or J is outside (1..N), return zero
*>         (this is convenient for generating matrices in band format).
*>
*>      Generate a matrix A with random entries of distribution IDIST.
*>
*>      Set the diagonal to D.
*>
*>      Grade the matrix, if desired, from the left (by DL) and/or
*>         from the right (by DR or DL) as specified by IGRADE.
*>
*>      Permute, if desired, the rows and/or columns as specified by
*>         IPVTNG and IWORK.
*>
*>      Band the matrix to have lower bandwidth KL and upper
*>         bandwidth KU.
*>
*>      Set random entries to zero as specified by SPARSE.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>           Number of rows of matrix. Not modified.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           Number of columns of matrix. Not modified.
*> \endverbatim
*>
*> \param[in] I
*> \verbatim
*>          I is INTEGER
*>           Row of entry to be returned. Not modified.
*> \endverbatim
*>
*> \param[in] J
*> \verbatim
*>          J is INTEGER
*>           Column of entry to be returned. Not modified.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>           Lower bandwidth. Not modified.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>           Upper bandwidth. Not modified.
*> \endverbatim
*>
*> \param[in] IDIST
*> \verbatim
*>          IDIST is INTEGER
*>           On entry, IDIST specifies the type of distribution to be
*>           used to generate a random matrix .
*>           1 => real and imaginary parts each UNIFORM( 0, 1 )
*>           2 => real and imaginary parts each UNIFORM( -1, 1 )
*>           3 => real and imaginary parts each NORMAL( 0, 1 )
*>           4 => complex number uniform in DISK( 0 , 1 )
*>           Not modified.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array of dimension ( 4 )
*>           Seed for random number generator.
*>           Changed on exit.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is COMPLEX array of dimension ( MIN( I , J ) )
*>           Diagonal entries of matrix. Not modified.
*> \endverbatim
*>
*> \param[in] IGRADE
*> \verbatim
*>          IGRADE is INTEGER
*>           Specifies grading of matrix as follows:
*>           0  => no grading
*>           1  => matrix premultiplied by diag( DL )
*>           2  => matrix postmultiplied by diag( DR )
*>           3  => matrix premultiplied by diag( DL ) and
*>                         postmultiplied by diag( DR )
*>           4  => matrix premultiplied by diag( DL ) and
*>                         postmultiplied by inv( diag( DL ) )
*>           5  => matrix premultiplied by diag( DL ) and
*>                         postmultiplied by diag( CONJG(DL) )
*>           6  => matrix premultiplied by diag( DL ) and
*>                         postmultiplied by diag( DL )
*>           Not modified.
*> \endverbatim
*>
*> \param[in] DL
*> \verbatim
*>          DL is COMPLEX array ( I or J, as appropriate )
*>           Left scale factors for grading matrix.  Not modified.
*> \endverbatim
*>
*> \param[in] DR
*> \verbatim
*>          DR is COMPLEX array ( I or J, as appropriate )
*>           Right scale factors for grading matrix.  Not modified.
*> \endverbatim
*>
*> \param[in] IPVTNG
*> \verbatim
*>          IPVTNG is INTEGER
*>           On entry specifies pivoting permutations as follows:
*>           0 => none.
*>           1 => row pivoting.
*>           2 => column pivoting.
*>           3 => full pivoting, i.e., on both sides.
*>           Not modified.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array ( I or J, as appropriate )
*>           This array specifies the permutation used. The
*>           row (or column) in position K was originally in
*>           position IWORK( K ).
*>           This differs from IWORK for CLATM3. Not modified.
*> \endverbatim
*>
*> \param[in] SPARSE
*> \verbatim
*>          SPARSE is REAL
*>           Value between 0. and 1.
*>           On entry specifies the sparsity of the matrix
*>           if sparse matix is to be generated.
*>           SPARSE should lie between 0 and 1.
*>           A uniform ( 0, 1 ) random number x is generated and
*>           compared to SPARSE; if x is larger the matrix entry
*>           is unchanged and if x is smaller the entry is set
*>           to zero. Thus on the average a fraction SPARSE of the
*>           entries will be set to zero.
*>           Not modified.
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
*> \date June 2016
*
*> \ingroup complex_matgen
*
*  =====================================================================
      COMPLEX FUNCTION CLATM2( M, N, I, J, KL, KU, IDIST, ISEED, D,
     $                         IGRADE, DL, DR, IPVTNG, IWORK, SPARSE )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
*
      INTEGER            I, IDIST, IGRADE, IPVTNG, J, KL, KU, M, N
      REAL               SPARSE
*     ..
*
*     .. Array Arguments ..
*
      INTEGER            ISEED( 4 ), IWORK( * )
      COMPLEX            D( * ), DL( * ), DR( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
*
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E0, 0.0E0 ) )
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E0 )
*     ..
*
*     .. Local Scalars ..
*
      INTEGER            ISUB, JSUB
      COMPLEX            CTEMP
*     ..
*
*     .. External Functions ..
*
      REAL               SLARAN
      COMPLEX            CLARND
      EXTERNAL           SLARAN, CLARND
*     ..
*
*     .. Intrinsic Functions ..
*
      INTRINSIC          CONJG
*     ..
*
*-----------------------------------------------------------------------
*
*     .. Executable Statements ..
*
*
*     Check for I and J in range
*
      IF( I.LT.1 .OR. I.GT.M .OR. J.LT.1 .OR. J.GT.N ) THEN
         CLATM2 = CZERO
         RETURN
      END IF
*
*     Check for banding
*
      IF( J.GT.I+KU .OR. J.LT.I-KL ) THEN
         CLATM2 = CZERO
         RETURN
      END IF
*
*     Check for sparsity
*
      IF( SPARSE.GT.ZERO ) THEN
         IF( SLARAN( ISEED ).LT.SPARSE ) THEN
            CLATM2 = CZERO
            RETURN
         END IF
      END IF
*
*     Compute subscripts depending on IPVTNG
*
      IF( IPVTNG.EQ.0 ) THEN
         ISUB = I
         JSUB = J
      ELSE IF( IPVTNG.EQ.1 ) THEN
         ISUB = IWORK( I )
         JSUB = J
      ELSE IF( IPVTNG.EQ.2 ) THEN
         ISUB = I
         JSUB = IWORK( J )
      ELSE IF( IPVTNG.EQ.3 ) THEN
         ISUB = IWORK( I )
         JSUB = IWORK( J )
      END IF
*
*     Compute entry and grade it according to IGRADE
*
      IF( ISUB.EQ.JSUB ) THEN
         CTEMP = D( ISUB )
      ELSE
         CTEMP = CLARND( IDIST, ISEED )
      END IF
      IF( IGRADE.EQ.1 ) THEN
         CTEMP = CTEMP*DL( ISUB )
      ELSE IF( IGRADE.EQ.2 ) THEN
         CTEMP = CTEMP*DR( JSUB )
      ELSE IF( IGRADE.EQ.3 ) THEN
         CTEMP = CTEMP*DL( ISUB )*DR( JSUB )
      ELSE IF( IGRADE.EQ.4 .AND. ISUB.NE.JSUB ) THEN
         CTEMP = CTEMP*DL( ISUB ) / DL( JSUB )
      ELSE IF( IGRADE.EQ.5 ) THEN
         CTEMP = CTEMP*DL( ISUB )*CONJG( DL( JSUB ) )
      ELSE IF( IGRADE.EQ.6 ) THEN
         CTEMP = CTEMP*DL( ISUB )*DL( JSUB )
      END IF
      CLATM2 = CTEMP
      RETURN
*
*     End of CLATM2
*
      END
