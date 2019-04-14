*> \brief \b ZLATM3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       COMPLEX*16   FUNCTION ZLATM3( M, N, I, J, ISUB, JSUB, KL, KU,
*                        IDIST, ISEED, D, IGRADE, DL, DR, IPVTNG, IWORK,
*                        SPARSE )
*
*       .. Scalar Arguments ..
*
*       INTEGER            I, IDIST, IGRADE, IPVTNG, ISUB, J, JSUB, KL,
*      $                   KU, M, N
*       DOUBLE PRECISION   SPARSE
*       ..
*
*       .. Array Arguments ..
*
*       INTEGER            ISEED( 4 ), IWORK( * )
*       COMPLEX*16         D( * ), DL( * ), DR( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZLATM3 returns the (ISUB,JSUB) entry of a random matrix of
*>    dimension (M, N) described by the other parameters. (ISUB,JSUB)
*>    is the final position of the (I,J) entry after pivoting
*>    according to IPVTNG and IWORK. ZLATM3 is called by the
*>    ZLATMR routine in order to build random test matrices. No error
*>    checking on parameters is done, because this routine is called in
*>    a tight loop by ZLATMR which has already checked the parameters.
*>
*>    Use of ZLATM3 differs from CLATM2 in the order in which the random
*>    number generator is called to fill in random matrix entries.
*>    With ZLATM2, the generator is called to fill in the pivoted matrix
*>    columnwise. With ZLATM3, the generator is called to fill in the
*>    matrix columnwise, after which it is pivoted. Thus, ZLATM3 can
*>    be used to construct random matrices which differ only in their
*>    order of rows and/or columns. ZLATM2 is used to construct band
*>    matrices while avoiding calling the random number generator for
*>    entries outside the band (and therefore generating random numbers
*>    in different orders for different pivot orders).
*>
*>    The matrix whose (ISUB,JSUB) entry is returned is constructed as
*>    follows (this routine only computes one entry):
*>
*>      If ISUB is outside (1..M) or JSUB is outside (1..N), return zero
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
*>           Row of unpivoted entry to be returned. Not modified.
*> \endverbatim
*>
*> \param[in] J
*> \verbatim
*>          J is INTEGER
*>           Column of unpivoted entry to be returned. Not modified.
*> \endverbatim
*>
*> \param[in,out] ISUB
*> \verbatim
*>          ISUB is INTEGER
*>           Row of pivoted entry to be returned. Changed on exit.
*> \endverbatim
*>
*> \param[in,out] JSUB
*> \verbatim
*>          JSUB is INTEGER
*>           Column of pivoted entry to be returned. Changed on exit.
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
*>          D is COMPLEX*16 array of dimension ( MIN( I , J ) )
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
*>          DL is COMPLEX*16 array ( I or J, as appropriate )
*>           Left scale factors for grading matrix.  Not modified.
*> \endverbatim
*>
*> \param[in] DR
*> \verbatim
*>          DR is COMPLEX*16 array ( I or J, as appropriate )
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
*> \param[in] IWORK
*> \verbatim
*>          IWORK is INTEGER array ( I or J, as appropriate )
*>           This array specifies the permutation used. The
*>           row (or column) originally in position K is in
*>           position IWORK( K ) after pivoting.
*>           This differs from IWORK for ZLATM2. Not modified.
*> \endverbatim
*>
*> \param[in] SPARSE
*> \verbatim
*>          SPARSE is DOUBLE PRECISION between 0. and 1.
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
*> \ingroup complex16_matgen
*
*  =====================================================================
      COMPLEX*16   FUNCTION ZLATM3( M, N, I, J, ISUB, JSUB, KL, KU,
     $                 IDIST, ISEED, D, IGRADE, DL, DR, IPVTNG, IWORK,
     $                 SPARSE )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
*
      INTEGER            I, IDIST, IGRADE, IPVTNG, ISUB, J, JSUB, KL,
     $                   KU, M, N
      DOUBLE PRECISION   SPARSE
*     ..
*
*     .. Array Arguments ..
*
      INTEGER            ISEED( 4 ), IWORK( * )
      COMPLEX*16         D( * ), DL( * ), DR( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
*
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D0, 0.0D0 ) )
*     ..
*
*     .. Local Scalars ..
*
      COMPLEX*16         CTEMP
*     ..
*
*     .. External Functions ..
*
      DOUBLE PRECISION   DLARAN
      COMPLEX*16         ZLARND
      EXTERNAL           DLARAN, ZLARND
*     ..
*
*     .. Intrinsic Functions ..
*
      INTRINSIC          DCONJG
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
         ISUB = I
         JSUB = J
         ZLATM3 = CZERO
         RETURN
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
*     Check for banding
*
      IF( JSUB.GT.ISUB+KU .OR. JSUB.LT.ISUB-KL ) THEN
         ZLATM3 = CZERO
         RETURN
      END IF
*
*     Check for sparsity
*
      IF( SPARSE.GT.ZERO ) THEN
         IF( DLARAN( ISEED ).LT.SPARSE ) THEN
            ZLATM3 = CZERO
            RETURN
         END IF
      END IF
*
*     Compute entry and grade it according to IGRADE
*
      IF( I.EQ.J ) THEN
         CTEMP = D( I )
      ELSE
         CTEMP = ZLARND( IDIST, ISEED )
      END IF
      IF( IGRADE.EQ.1 ) THEN
         CTEMP = CTEMP*DL( I )
      ELSE IF( IGRADE.EQ.2 ) THEN
         CTEMP = CTEMP*DR( J )
      ELSE IF( IGRADE.EQ.3 ) THEN
         CTEMP = CTEMP*DL( I )*DR( J )
      ELSE IF( IGRADE.EQ.4 .AND. I.NE.J ) THEN
         CTEMP = CTEMP*DL( I ) / DL( J )
      ELSE IF( IGRADE.EQ.5 ) THEN
         CTEMP = CTEMP*DL( I )*DCONJG( DL( J ) )
      ELSE IF( IGRADE.EQ.6 ) THEN
         CTEMP = CTEMP*DL( I )*DL( J )
      END IF
      ZLATM3 = CTEMP
      RETURN
*
*     End of ZLATM3
*
      END
