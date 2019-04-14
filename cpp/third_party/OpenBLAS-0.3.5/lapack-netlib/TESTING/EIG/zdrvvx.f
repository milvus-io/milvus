*> \brief \b ZDRVVX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZDRVVX( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NIUNIT, NOUNIT, A, LDA, H, W, W1, VL, LDVL, VR,
*                          LDVR, LRE, LDLRE, RCONDV, RCNDV1, RCDVIN,
*                          RCONDE, RCNDE1, RCDEIN, SCALE, SCALE1, RESULT,
*                          WORK, NWORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDLRE, LDVL, LDVR, NIUNIT, NOUNIT,
*      $                   NSIZES, NTYPES, NWORK
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), NN( * )
*       DOUBLE PRECISION   RCDEIN( * ), RCDVIN( * ), RCNDE1( * ),
*      $                   RCNDV1( * ), RCONDE( * ), RCONDV( * ),
*      $                   RESULT( 11 ), RWORK( * ), SCALE( * ),
*      $                   SCALE1( * )
*       COMPLEX*16         A( LDA, * ), H( LDA, * ), LRE( LDLRE, * ),
*      $                   VL( LDVL, * ), VR( LDVR, * ), W( * ), W1( * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZDRVVX  checks the nonsymmetric eigenvalue problem expert driver
*>    ZGEEVX.
*>
*>    ZDRVVX uses both test matrices generated randomly depending on
*>    data supplied in the calling sequence, as well as on data
*>    read from an input file and including precomputed condition
*>    numbers to which it compares the ones it computes.
*>
*>    When ZDRVVX is called, a number of matrix "sizes" ("n's") and a
*>    number of matrix "types" are specified in the calling sequence.
*>    For each size ("n") and each type of matrix, one matrix will be
*>    generated and used to test the nonsymmetric eigenroutines.  For
*>    each matrix, 9 tests will be performed:
*>
*>    (1)     | A * VR - VR * W | / ( n |A| ulp )
*>
*>      Here VR is the matrix of unit right eigenvectors.
*>      W is a diagonal matrix with diagonal entries W(j).
*>
*>    (2)     | A**H  * VL - VL * W**H | / ( n |A| ulp )
*>
*>      Here VL is the matrix of unit left eigenvectors, A**H is the
*>      conjugate transpose of A, and W is as above.
*>
*>    (3)     | |VR(i)| - 1 | / ulp and largest component real
*>
*>      VR(i) denotes the i-th column of VR.
*>
*>    (4)     | |VL(i)| - 1 | / ulp and largest component real
*>
*>      VL(i) denotes the i-th column of VL.
*>
*>    (5)     W(full) = W(partial)
*>
*>      W(full) denotes the eigenvalues computed when VR, VL, RCONDV
*>      and RCONDE are also computed, and W(partial) denotes the
*>      eigenvalues computed when only some of VR, VL, RCONDV, and
*>      RCONDE are computed.
*>
*>    (6)     VR(full) = VR(partial)
*>
*>      VR(full) denotes the right eigenvectors computed when VL, RCONDV
*>      and RCONDE are computed, and VR(partial) denotes the result
*>      when only some of VL and RCONDV are computed.
*>
*>    (7)     VL(full) = VL(partial)
*>
*>      VL(full) denotes the left eigenvectors computed when VR, RCONDV
*>      and RCONDE are computed, and VL(partial) denotes the result
*>      when only some of VR and RCONDV are computed.
*>
*>    (8)     0 if SCALE, ILO, IHI, ABNRM (full) =
*>                 SCALE, ILO, IHI, ABNRM (partial)
*>            1/ulp otherwise
*>
*>      SCALE, ILO, IHI and ABNRM describe how the matrix is balanced.
*>      (full) is when VR, VL, RCONDE and RCONDV are also computed, and
*>      (partial) is when some are not computed.
*>
*>    (9)     RCONDV(full) = RCONDV(partial)
*>
*>      RCONDV(full) denotes the reciprocal condition numbers of the
*>      right eigenvectors computed when VR, VL and RCONDE are also
*>      computed. RCONDV(partial) denotes the reciprocal condition
*>      numbers when only some of VR, VL and RCONDE are computed.
*>
*>    The "sizes" are specified by an array NN(1:NSIZES); the value of
*>    each element NN(j) specifies one size.
*>    The "types" are specified by a logical array DOTYPE( 1:NTYPES );
*>    if DOTYPE(j) is .TRUE., then matrix type "j" will be generated.
*>    Currently, the list of possible types is:
*>
*>    (1)  The zero matrix.
*>    (2)  The identity matrix.
*>    (3)  A (transposed) Jordan block, with 1's on the diagonal.
*>
*>    (4)  A diagonal matrix with evenly spaced entries
*>         1, ..., ULP  and random complex angles.
*>         (ULP = (first number larger than 1) - 1 )
*>    (5)  A diagonal matrix with geometrically spaced entries
*>         1, ..., ULP  and random complex angles.
*>    (6)  A diagonal matrix with "clustered" entries 1, ULP, ..., ULP
*>         and random complex angles.
*>
*>    (7)  Same as (4), but multiplied by a constant near
*>         the overflow threshold
*>    (8)  Same as (4), but multiplied by a constant near
*>         the underflow threshold
*>
*>    (9)  A matrix of the form  U' T U, where U is unitary and
*>         T has evenly spaced entries 1, ..., ULP with random complex
*>         angles on the diagonal and random O(1) entries in the upper
*>         triangle.
*>
*>    (10) A matrix of the form  U' T U, where U is unitary and
*>         T has geometrically spaced entries 1, ..., ULP with random
*>         complex angles on the diagonal and random O(1) entries in
*>         the upper triangle.
*>
*>    (11) A matrix of the form  U' T U, where U is unitary and
*>         T has "clustered" entries 1, ULP,..., ULP with random
*>         complex angles on the diagonal and random O(1) entries in
*>         the upper triangle.
*>
*>    (12) A matrix of the form  U' T U, where U is unitary and
*>         T has complex eigenvalues randomly chosen from
*>         ULP < |z| < 1   and random O(1) entries in the upper
*>         triangle.
*>
*>    (13) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has evenly spaced entries 1, ..., ULP
*>         with random complex angles on the diagonal and random O(1)
*>         entries in the upper triangle.
*>
*>    (14) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has geometrically spaced entries
*>         1, ..., ULP with random complex angles on the diagonal
*>         and random O(1) entries in the upper triangle.
*>
*>    (15) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has "clustered" entries 1, ULP,..., ULP
*>         with random complex angles on the diagonal and random O(1)
*>         entries in the upper triangle.
*>
*>    (16) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has complex eigenvalues randomly chosen
*>         from ULP < |z| < 1 and random O(1) entries in the upper
*>         triangle.
*>
*>    (17) Same as (16), but multiplied by a constant
*>         near the overflow threshold
*>    (18) Same as (16), but multiplied by a constant
*>         near the underflow threshold
*>
*>    (19) Nonsymmetric matrix with random entries chosen from |z| < 1
*>         If N is at least 4, all entries in first two rows and last
*>         row, and first column and last two columns are zero.
*>    (20) Same as (19), but multiplied by a constant
*>         near the overflow threshold
*>    (21) Same as (19), but multiplied by a constant
*>         near the underflow threshold
*>
*>    In addition, an input file will be read from logical unit number
*>    NIUNIT. The file contains matrices along with precomputed
*>    eigenvalues and reciprocal condition numbers for the eigenvalues
*>    and right eigenvectors. For these matrices, in addition to tests
*>    (1) to (9) we will compute the following two tests:
*>
*>   (10)  |RCONDV - RCDVIN| / cond(RCONDV)
*>
*>      RCONDV is the reciprocal right eigenvector condition number
*>      computed by ZGEEVX and RCDVIN (the precomputed true value)
*>      is supplied as input. cond(RCONDV) is the condition number of
*>      RCONDV, and takes errors in computing RCONDV into account, so
*>      that the resulting quantity should be O(ULP). cond(RCONDV) is
*>      essentially given by norm(A)/RCONDE.
*>
*>   (11)  |RCONDE - RCDEIN| / cond(RCONDE)
*>
*>      RCONDE is the reciprocal eigenvalue condition number
*>      computed by ZGEEVX and RCDEIN (the precomputed true value)
*>      is supplied as input.  cond(RCONDE) is the condition number
*>      of RCONDE, and takes errors in computing RCONDE into account,
*>      so that the resulting quantity should be O(ULP). cond(RCONDE)
*>      is essentially given by norm(A)/RCONDV.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  NSIZES must be at
*>          least zero. If it is zero, no randomly generated matrices
*>          are tested, but any test matrices read from NIUNIT will be
*>          tested.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER array, dimension (NSIZES)
*>          An array containing the sizes to be used for the matrices.
*>          Zero values will be skipped.  The values must be at least
*>          zero.
*> \endverbatim
*>
*> \param[in] NTYPES
*> \verbatim
*>          NTYPES is INTEGER
*>          The number of elements in DOTYPE. NTYPES must be at least
*>          zero. If it is zero, no randomly generated test matrices
*>          are tested, but and test matrices read from NIUNIT will be
*>          tested. If it is MAXTYP+1 and NSIZES is 1, then an
*>          additional type, MAXTYP+1 is defined, which is to use
*>          whatever matrix is in A.  This is only useful if
*>          DOTYPE(1:MAXTYP) is .FALSE. and DOTYPE(MAXTYP+1) is .TRUE. .
*> \endverbatim
*>
*> \param[in] DOTYPE
*> \verbatim
*>          DOTYPE is LOGICAL array, dimension (NTYPES)
*>          If DOTYPE(j) is .TRUE., then for each size in NN a
*>          matrix of that size and of type j will be generated.
*>          If NTYPES is smaller than the maximum number of types
*>          defined (PARAMETER MAXTYP), then types NTYPES+1 through
*>          MAXTYP will not be generated.  If NTYPES is larger
*>          than MAXTYP, DOTYPE(MAXTYP+1) through DOTYPE(NTYPES)
*>          will be ignored.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          On entry ISEED specifies the seed of the random number
*>          generator. The array elements should be between 0 and 4095;
*>          if not they will be reduced mod 4096.  Also, ISEED(4) must
*>          be odd.  The random number generator uses a linear
*>          congruential sequence limited to small integers, and so
*>          should produce machine independent random numbers. The
*>          values of ISEED are changed on exit, and can be used in the
*>          next call to ZDRVVX to continue the same random number
*>          sequence.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is DOUBLE PRECISION
*>          A test will count as "failed" if the "error", computed as
*>          described above, exceeds THRESH.  Note that the error
*>          is scaled to be O(1), so THRESH should be a reasonably
*>          small multiple of 1, e.g., 10 or 100.  In particular,
*>          it should not depend on the precision (single vs. double)
*>          or the size of the matrix.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] NIUNIT
*> \verbatim
*>          NIUNIT is INTEGER
*>          The FORTRAN unit number for reading in the data file of
*>          problems to solve.
*> \endverbatim
*>
*> \param[in] NOUNIT
*> \verbatim
*>          NOUNIT is INTEGER
*>          The FORTRAN unit number for printing out error messages
*>          (e.g., if a routine returns INFO not equal to 0.)
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, max(NN,12))
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually used.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, and H. LDA must be at
*>          least 1 and at least max( NN, 12 ). (12 is the
*>          dimension of the largest matrix on the precomputed
*>          input file.)
*> \endverbatim
*>
*> \param[out] H
*> \verbatim
*>          H is COMPLEX*16 array, dimension (LDA, max(NN,12))
*>          Another copy of the test matrix A, modified by ZGEEVX.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX*16 array, dimension (max(NN,12))
*>          Contains the eigenvalues of A.
*> \endverbatim
*>
*> \param[out] W1
*> \verbatim
*>          W1 is COMPLEX*16 array, dimension (max(NN,12))
*>          Like W, this array contains the eigenvalues of A,
*>          but those computed when ZGEEVX only computes a partial
*>          eigendecomposition, i.e. not the eigenvalues and left
*>          and right eigenvectors.
*> \endverbatim
*>
*> \param[out] VL
*> \verbatim
*>          VL is COMPLEX*16 array, dimension (LDVL, max(NN,12))
*>          VL holds the computed left eigenvectors.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          Leading dimension of VL. Must be at least max(1,max(NN,12)).
*> \endverbatim
*>
*> \param[out] VR
*> \verbatim
*>          VR is COMPLEX*16 array, dimension (LDVR, max(NN,12))
*>          VR holds the computed right eigenvectors.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          Leading dimension of VR. Must be at least max(1,max(NN,12)).
*> \endverbatim
*>
*> \param[out] LRE
*> \verbatim
*>          LRE is COMPLEX*16 array, dimension (LDLRE, max(NN,12))
*>          LRE holds the computed right or left eigenvectors.
*> \endverbatim
*>
*> \param[in] LDLRE
*> \verbatim
*>          LDLRE is INTEGER
*>          Leading dimension of LRE. Must be at least max(1,max(NN,12))
*> \endverbatim
*>
*> \param[out] RCONDV
*> \verbatim
*>          RCONDV is DOUBLE PRECISION array, dimension (N)
*>          RCONDV holds the computed reciprocal condition numbers
*>          for eigenvectors.
*> \endverbatim
*>
*> \param[out] RCNDV1
*> \verbatim
*>          RCNDV1 is DOUBLE PRECISION array, dimension (N)
*>          RCNDV1 holds more computed reciprocal condition numbers
*>          for eigenvectors.
*> \endverbatim
*>
*> \param[in] RCDVIN
*> \verbatim
*>          RCDVIN is DOUBLE PRECISION array, dimension (N)
*>          When COMP = .TRUE. RCDVIN holds the precomputed reciprocal
*>          condition numbers for eigenvectors to be compared with
*>          RCONDV.
*> \endverbatim
*>
*> \param[out] RCONDE
*> \verbatim
*>          RCONDE is DOUBLE PRECISION array, dimension (N)
*>          RCONDE holds the computed reciprocal condition numbers
*>          for eigenvalues.
*> \endverbatim
*>
*> \param[out] RCNDE1
*> \verbatim
*>          RCNDE1 is DOUBLE PRECISION array, dimension (N)
*>          RCNDE1 holds more computed reciprocal condition numbers
*>          for eigenvalues.
*> \endverbatim
*>
*> \param[in] RCDEIN
*> \verbatim
*>          RCDEIN is DOUBLE PRECISION array, dimension (N)
*>          When COMP = .TRUE. RCDEIN holds the precomputed reciprocal
*>          condition numbers for eigenvalues to be compared with
*>          RCONDE.
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is DOUBLE PRECISION array, dimension (N)
*>          Holds information describing balancing of matrix.
*> \endverbatim
*>
*> \param[out] SCALE1
*> \verbatim
*>          SCALE1 is DOUBLE PRECISION array, dimension (N)
*>          Holds information describing balancing of matrix.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (NWORK)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (11)
*>          The values computed by the seven tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*> \endverbatim
*>
*> \param[in] NWORK
*> \verbatim
*>          NWORK is INTEGER
*>          The number of entries in WORK.  This must be at least
*>          max(6*12+2*12**2,6*NN(j)+2*NN(j)**2) =
*>          max(    360     ,6*NN(j)+2*NN(j)**2)    for all j.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (2*max(NN,12))
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          If 0,  then successful exit.
*>          If <0, then input parameter -INFO is incorrect.
*>          If >0, ZLATMR, CLATMS, CLATME or ZGET23 returned an error
*>                 code, and INFO is its absolute value.
*>
*>-----------------------------------------------------------------------
*>
*>     Some Local Variables and Parameters:
*>     ---- ----- --------- --- ----------
*>
*>     ZERO, ONE       Real 0 and 1.
*>     MAXTYP          The number of types defined.
*>     NMAX            Largest value in NN or 12.
*>     NERRS           The number of tests which have exceeded THRESH
*>     COND, CONDS,
*>     IMODE           Values to be passed to the matrix generators.
*>     ANORM           Norm of A; passed to matrix generators.
*>
*>     OVFL, UNFL      Overflow and underflow thresholds.
*>     ULP, ULPINV     Finest relative precision and its inverse.
*>     RTULP, RTULPI   Square roots of the previous 4 values.
*>
*>             The following four arrays decode JTYPE:
*>     KTYPE(j)        The general type (1-10) for type "j".
*>     KMODE(j)        The MODE value to be passed to the matrix
*>                     generator for type "j".
*>     KMAGN(j)        The order of magnitude ( O(1),
*>                     O(overflow^(1/2) ), O(underflow^(1/2) )
*>     KCONDS(j)       Selectw whether CONDS is to be 1 or
*>                     1/sqrt(ulp).  (0 means irrelevant.)
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZDRVVX( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NIUNIT, NOUNIT, A, LDA, H, W, W1, VL, LDVL, VR,
     $                   LDVR, LRE, LDLRE, RCONDV, RCNDV1, RCDVIN,
     $                   RCONDE, RCNDE1, RCDEIN, SCALE, SCALE1, RESULT,
     $                   WORK, NWORK, RWORK, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDLRE, LDVL, LDVR, NIUNIT, NOUNIT,
     $                   NSIZES, NTYPES, NWORK
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), NN( * )
      DOUBLE PRECISION   RCDEIN( * ), RCDVIN( * ), RCNDE1( * ),
     $                   RCNDV1( * ), RCONDE( * ), RCONDV( * ),
     $                   RESULT( 11 ), RWORK( * ), SCALE( * ),
     $                   SCALE1( * )
      COMPLEX*16         A( LDA, * ), H( LDA, * ), LRE( LDLRE, * ),
     $                   VL( LDVL, * ), VR( LDVR, * ), W( * ), W1( * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ) )
      COMPLEX*16         CONE
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ) )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 21 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN
      CHARACTER          BALANC
      CHARACTER*3        PATH
      INTEGER            I, IBAL, IINFO, IMODE, ISRT, ITYPE, IWK, J,
     $                   JCOL, JSIZE, JTYPE, MTYPES, N, NERRS, NFAIL,
     $                   NMAX, NNWORK, NTEST, NTESTF, NTESTT
      DOUBLE PRECISION   ANORM, COND, CONDS, OVFL, RTULP, RTULPI, ULP,
     $                   ULPINV, UNFL, WI, WR
*     ..
*     .. Local Arrays ..
      CHARACTER          BAL( 4 )
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), KCONDS( MAXTYP ),
     $                   KMAGN( MAXTYP ), KMODE( MAXTYP ),
     $                   KTYPE( MAXTYP )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLASUM, XERBLA, ZGET23, ZLASET, ZLATME,
     $                   ZLATMR, ZLATMS
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DCMPLX, MAX, MIN, SQRT
*     ..
*     .. Data statements ..
      DATA               KTYPE / 1, 2, 3, 5*4, 4*6, 6*6, 3*9 /
      DATA               KMAGN / 3*1, 1, 1, 1, 2, 3, 4*1, 1, 1, 1, 1, 2,
     $                   3, 1, 2, 3 /
      DATA               KMODE / 3*0, 4, 3, 1, 4, 4, 4, 3, 1, 5, 4, 3,
     $                   1, 5, 5, 5, 4, 3, 1 /
      DATA               KCONDS / 3*0, 5*0, 4*1, 6*2, 3*0 /
      DATA               BAL / 'N', 'P', 'S', 'B' /
*     ..
*     .. Executable Statements ..
*
      PATH( 1: 1 ) = 'Zomplex precision'
      PATH( 2: 3 ) = 'VX'
*
*     Check for errors
*
      NTESTT = 0
      NTESTF = 0
      INFO = 0
*
*     Important constants
*
      BADNN = .FALSE.
*
*     7 is the largest dimension in the input file of precomputed
*     problems
*
      NMAX = 7
      DO 10 J = 1, NSIZES
         NMAX = MAX( NMAX, NN( J ) )
         IF( NN( J ).LT.0 )
     $      BADNN = .TRUE.
   10 CONTINUE
*
*     Check for errors
*
      IF( NSIZES.LT.0 ) THEN
         INFO = -1
      ELSE IF( BADNN ) THEN
         INFO = -2
      ELSE IF( NTYPES.LT.0 ) THEN
         INFO = -3
      ELSE IF( THRESH.LT.ZERO ) THEN
         INFO = -6
      ELSE IF( LDA.LT.1 .OR. LDA.LT.NMAX ) THEN
         INFO = -10
      ELSE IF( LDVL.LT.1 .OR. LDVL.LT.NMAX ) THEN
         INFO = -15
      ELSE IF( LDVR.LT.1 .OR. LDVR.LT.NMAX ) THEN
         INFO = -17
      ELSE IF( LDLRE.LT.1 .OR. LDLRE.LT.NMAX ) THEN
         INFO = -19
      ELSE IF( 6*NMAX+2*NMAX**2.GT.NWORK ) THEN
         INFO = -30
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZDRVVX', -INFO )
         RETURN
      END IF
*
*     If nothing to do check on NIUNIT
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   GO TO 160
*
*     More Important constants
*
      UNFL = DLAMCH( 'Safe minimum' )
      OVFL = ONE / UNFL
      CALL DLABAD( UNFL, OVFL )
      ULP = DLAMCH( 'Precision' )
      ULPINV = ONE / ULP
      RTULP = SQRT( ULP )
      RTULPI = ONE / RTULP
*
*     Loop over sizes, types
*
      NERRS = 0
*
      DO 150 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 140 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 140
*
*           Save ISEED in case of an error.
*
            DO 20 J = 1, 4
               IOLDSD( J ) = ISEED( J )
   20       CONTINUE
*
*           Compute "A"
*
*           Control parameters:
*
*           KMAGN  KCONDS  KMODE        KTYPE
*       =1  O(1)   1       clustered 1  zero
*       =2  large  large   clustered 2  identity
*       =3  small          exponential  Jordan
*       =4                 arithmetic   diagonal, (w/ eigenvalues)
*       =5                 random log   symmetric, w/ eigenvalues
*       =6                 random       general, w/ eigenvalues
*       =7                              random diagonal
*       =8                              random symmetric
*       =9                              random general
*       =10                             random triangular
*
            IF( MTYPES.GT.MAXTYP )
     $         GO TO 90
*
            ITYPE = KTYPE( JTYPE )
            IMODE = KMODE( JTYPE )
*
*           Compute norm
*
            GO TO ( 30, 40, 50 )KMAGN( JTYPE )
*
   30       CONTINUE
            ANORM = ONE
            GO TO 60
*
   40       CONTINUE
            ANORM = OVFL*ULP
            GO TO 60
*
   50       CONTINUE
            ANORM = UNFL*ULPINV
            GO TO 60
*
   60       CONTINUE
*
            CALL ZLASET( 'Full', LDA, N, CZERO, CZERO, A, LDA )
            IINFO = 0
            COND = ULPINV
*
*           Special Matrices -- Identity & Jordan block
*
*              Zero
*
            IF( ITYPE.EQ.1 ) THEN
               IINFO = 0
*
            ELSE IF( ITYPE.EQ.2 ) THEN
*
*              Identity
*
               DO 70 JCOL = 1, N
                  A( JCOL, JCOL ) = ANORM
   70          CONTINUE
*
            ELSE IF( ITYPE.EQ.3 ) THEN
*
*              Jordan Block
*
               DO 80 JCOL = 1, N
                  A( JCOL, JCOL ) = ANORM
                  IF( JCOL.GT.1 )
     $               A( JCOL, JCOL-1 ) = ONE
   80          CONTINUE
*
            ELSE IF( ITYPE.EQ.4 ) THEN
*
*              Diagonal Matrix, [Eigen]values Specified
*
               CALL ZLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, 0, 0, 'N', A, LDA, WORK( N+1 ),
     $                      IINFO )
*
            ELSE IF( ITYPE.EQ.5 ) THEN
*
*              Symmetric, eigenvalues specified
*
               CALL ZLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, N, N, 'N', A, LDA, WORK( N+1 ),
     $                      IINFO )
*
            ELSE IF( ITYPE.EQ.6 ) THEN
*
*              General, eigenvalues specified
*
               IF( KCONDS( JTYPE ).EQ.1 ) THEN
                  CONDS = ONE
               ELSE IF( KCONDS( JTYPE ).EQ.2 ) THEN
                  CONDS = RTULPI
               ELSE
                  CONDS = ZERO
               END IF
*
               CALL ZLATME( N, 'D', ISEED, WORK, IMODE, COND, CONE,
     $                      'T', 'T', 'T', RWORK, 4, CONDS, N, N, ANORM,
     $                      A, LDA, WORK( 2*N+1 ), IINFO )
*
            ELSE IF( ITYPE.EQ.7 ) THEN
*
*              Diagonal, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'S', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IDUMMA, IINFO )
*
            ELSE IF( ITYPE.EQ.8 ) THEN
*
*              Symmetric, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'H', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IDUMMA, IINFO )
*
            ELSE IF( ITYPE.EQ.9 ) THEN
*
*              General, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IDUMMA, IINFO )
               IF( N.GE.4 ) THEN
                  CALL ZLASET( 'Full', 2, N, CZERO, CZERO, A, LDA )
                  CALL ZLASET( 'Full', N-3, 1, CZERO, CZERO, A( 3, 1 ),
     $                         LDA )
                  CALL ZLASET( 'Full', N-3, 2, CZERO, CZERO,
     $                         A( 3, N-1 ), LDA )
                  CALL ZLASET( 'Full', 1, N, CZERO, CZERO, A( N, 1 ),
     $                         LDA )
               END IF
*
            ELSE IF( ITYPE.EQ.10 ) THEN
*
*              Triangular, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IDUMMA, IINFO )
*
            ELSE
*
               IINFO = 1
            END IF
*
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9992 )'Generator', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               RETURN
            END IF
*
   90       CONTINUE
*
*           Test for minimal and generous workspace
*
            DO 130 IWK = 1, 3
               IF( IWK.EQ.1 ) THEN
                  NNWORK = 2*N
               ELSE IF( IWK.EQ.2 ) THEN
                  NNWORK = 2*N + N**2
               ELSE
                  NNWORK = 6*N + 2*N**2
               END IF
               NNWORK = MAX( NNWORK, 1 )
*
*              Test for all balancing options
*
               DO 120 IBAL = 1, 4
                  BALANC = BAL( IBAL )
*
*                 Perform tests
*
                  CALL ZGET23( .FALSE., 0, BALANC, JTYPE, THRESH,
     $                         IOLDSD, NOUNIT, N, A, LDA, H, W, W1, VL,
     $                         LDVL, VR, LDVR, LRE, LDLRE, RCONDV,
     $                         RCNDV1, RCDVIN, RCONDE, RCNDE1, RCDEIN,
     $                         SCALE, SCALE1, RESULT, WORK, NNWORK,
     $                         RWORK, INFO )
*
*                 Check for RESULT(j) > THRESH
*
                  NTEST = 0
                  NFAIL = 0
                  DO 100 J = 1, 9
                     IF( RESULT( J ).GE.ZERO )
     $                  NTEST = NTEST + 1
                     IF( RESULT( J ).GE.THRESH )
     $                  NFAIL = NFAIL + 1
  100             CONTINUE
*
                  IF( NFAIL.GT.0 )
     $               NTESTF = NTESTF + 1
                  IF( NTESTF.EQ.1 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )PATH
                     WRITE( NOUNIT, FMT = 9998 )
                     WRITE( NOUNIT, FMT = 9997 )
                     WRITE( NOUNIT, FMT = 9996 )
                     WRITE( NOUNIT, FMT = 9995 )THRESH
                     NTESTF = 2
                  END IF
*
                  DO 110 J = 1, 9
                     IF( RESULT( J ).GE.THRESH ) THEN
                        WRITE( NOUNIT, FMT = 9994 )BALANC, N, IWK,
     $                     IOLDSD, JTYPE, J, RESULT( J )
                     END IF
  110             CONTINUE
*
                  NERRS = NERRS + NFAIL
                  NTESTT = NTESTT + NTEST
*
  120          CONTINUE
  130       CONTINUE
  140    CONTINUE
  150 CONTINUE
*
  160 CONTINUE
*
*     Read in data from file to check accuracy of condition estimation.
*     Assume input eigenvalues are sorted lexicographically (increasing
*     by real part, then decreasing by imaginary part)
*
      JTYPE = 0
  170 CONTINUE
      READ( NIUNIT, FMT = *, END = 220 )N, ISRT
*
*     Read input data until N=0
*
      IF( N.EQ.0 )
     $   GO TO 220
      JTYPE = JTYPE + 1
      ISEED( 1 ) = JTYPE
      DO 180 I = 1, N
         READ( NIUNIT, FMT = * )( A( I, J ), J = 1, N )
  180 CONTINUE
      DO 190 I = 1, N
         READ( NIUNIT, FMT = * )WR, WI, RCDEIN( I ), RCDVIN( I )
         W1( I ) = DCMPLX( WR, WI )
  190 CONTINUE
      CALL ZGET23( .TRUE., ISRT, 'N', 22, THRESH, ISEED, NOUNIT, N, A,
     $             LDA, H, W, W1, VL, LDVL, VR, LDVR, LRE, LDLRE,
     $             RCONDV, RCNDV1, RCDVIN, RCONDE, RCNDE1, RCDEIN,
     $             SCALE, SCALE1, RESULT, WORK, 6*N+2*N**2, RWORK,
     $             INFO )
*
*     Check for RESULT(j) > THRESH
*
      NTEST = 0
      NFAIL = 0
      DO 200 J = 1, 11
         IF( RESULT( J ).GE.ZERO )
     $      NTEST = NTEST + 1
         IF( RESULT( J ).GE.THRESH )
     $      NFAIL = NFAIL + 1
  200 CONTINUE
*
      IF( NFAIL.GT.0 )
     $   NTESTF = NTESTF + 1
      IF( NTESTF.EQ.1 ) THEN
         WRITE( NOUNIT, FMT = 9999 )PATH
         WRITE( NOUNIT, FMT = 9998 )
         WRITE( NOUNIT, FMT = 9997 )
         WRITE( NOUNIT, FMT = 9996 )
         WRITE( NOUNIT, FMT = 9995 )THRESH
         NTESTF = 2
      END IF
*
      DO 210 J = 1, 11
         IF( RESULT( J ).GE.THRESH ) THEN
            WRITE( NOUNIT, FMT = 9993 )N, JTYPE, J, RESULT( J )
         END IF
  210 CONTINUE
*
      NERRS = NERRS + NFAIL
      NTESTT = NTESTT + NTEST
      GO TO 170
  220 CONTINUE
*
*     Summary
*
      CALL DLASUM( PATH, NOUNIT, NERRS, NTESTT )
*
 9999 FORMAT( / 1X, A3, ' -- Complex Eigenvalue-Eigenvector ',
     $      'Decomposition Expert Driver',
     $      / ' Matrix types (see ZDRVVX for details): ' )
*
 9998 FORMAT( / ' Special Matrices:', / '  1=Zero matrix.             ',
     $      '           ', '  5=Diagonal: geometr. spaced entries.',
     $      / '  2=Identity matrix.                    ', '  6=Diagona',
     $      'l: clustered entries.', / '  3=Transposed Jordan block.  ',
     $      '          ', '  7=Diagonal: large, evenly spaced.', / '  ',
     $      '4=Diagonal: evenly spaced entries.    ', '  8=Diagonal: s',
     $      'mall, evenly spaced.' )
 9997 FORMAT( ' Dense, Non-Symmetric Matrices:', / '  9=Well-cond., ev',
     $      'enly spaced eigenvals.', ' 14=Ill-cond., geomet. spaced e',
     $      'igenals.', / ' 10=Well-cond., geom. spaced eigenvals. ',
     $      ' 15=Ill-conditioned, clustered e.vals.', / ' 11=Well-cond',
     $      'itioned, clustered e.vals. ', ' 16=Ill-cond., random comp',
     $      'lex ', / ' 12=Well-cond., random complex ', '         ',
     $      ' 17=Ill-cond., large rand. complx ', / ' 13=Ill-condi',
     $      'tioned, evenly spaced.     ', ' 18=Ill-cond., small rand.',
     $      ' complx ' )
 9996 FORMAT( ' 19=Matrix with random O(1) entries.    ', ' 21=Matrix ',
     $      'with small random entries.', / ' 20=Matrix with large ran',
     $      'dom entries.   ', ' 22=Matrix read from input file', / )
 9995 FORMAT( ' Tests performed with test threshold =', F8.2,
     $      / / ' 1 = | A VR - VR W | / ( n |A| ulp ) ',
     $      / ' 2 = | transpose(A) VL - VL W | / ( n |A| ulp ) ',
     $      / ' 3 = | |VR(i)| - 1 | / ulp ',
     $      / ' 4 = | |VL(i)| - 1 | / ulp ',
     $      / ' 5 = 0 if W same no matter if VR or VL computed,',
     $      ' 1/ulp otherwise', /
     $      ' 6 = 0 if VR same no matter what else computed,',
     $      '  1/ulp otherwise', /
     $      ' 7 = 0 if VL same no matter what else computed,',
     $      '  1/ulp otherwise', /
     $      ' 8 = 0 if RCONDV same no matter what else computed,',
     $      '  1/ulp otherwise', /
     $      ' 9 = 0 if SCALE, ILO, IHI, ABNRM same no matter what else',
     $      ' computed,  1/ulp otherwise',
     $      / ' 10 = | RCONDV - RCONDV(precomputed) | / cond(RCONDV),',
     $      / ' 11 = | RCONDE - RCONDE(precomputed) | / cond(RCONDE),' )
 9994 FORMAT( ' BALANC=''', A1, ''',N=', I4, ',IWK=', I1, ', seed=',
     $      4( I4, ',' ), ' type ', I2, ', test(', I2, ')=', G10.3 )
 9993 FORMAT( ' N=', I5, ', input example =', I3, ',  test(', I2, ')=',
     $      G10.3 )
 9992 FORMAT( ' ZDRVVX: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
      RETURN
*
*     End of ZDRVVX
*
      END
