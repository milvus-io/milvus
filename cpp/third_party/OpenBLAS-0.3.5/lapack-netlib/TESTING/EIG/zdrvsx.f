*> \brief \b ZDRVSX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZDRVSX( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NIUNIT, NOUNIT, A, LDA, H, HT, W, WT, WTMP, VS,
*                          LDVS, VS1, RESULT, WORK, LWORK, RWORK, BWORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDVS, LWORK, NIUNIT, NOUNIT, NSIZES,
*      $                   NTYPES
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * ), DOTYPE( * )
*       INTEGER            ISEED( 4 ), NN( * )
*       DOUBLE PRECISION   RESULT( 17 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), H( LDA, * ), HT( LDA, * ),
*      $                   VS( LDVS, * ), VS1( LDVS, * ), W( * ),
*      $                   WORK( * ), WT( * ), WTMP( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZDRVSX checks the nonsymmetric eigenvalue (Schur form) problem
*>    expert driver ZGEESX.
*>
*>    ZDRVSX uses both test matrices generated randomly depending on
*>    data supplied in the calling sequence, as well as on data
*>    read from an input file and including precomputed condition
*>    numbers to which it compares the ones it computes.
*>
*>    When ZDRVSX is called, a number of matrix "sizes" ("n's") and a
*>    number of matrix "types" are specified.  For each size ("n")
*>    and each type of matrix, one matrix will be generated and used
*>    to test the nonsymmetric eigenroutines.  For each matrix, 15
*>    tests will be performed:
*>
*>    (1)     0 if T is in Schur form, 1/ulp otherwise
*>           (no sorting of eigenvalues)
*>
*>    (2)     | A - VS T VS' | / ( n |A| ulp )
*>
*>      Here VS is the matrix of Schur eigenvectors, and T is in Schur
*>      form  (no sorting of eigenvalues).
*>
*>    (3)     | I - VS VS' | / ( n ulp ) (no sorting of eigenvalues).
*>
*>    (4)     0     if W are eigenvalues of T
*>            1/ulp otherwise
*>            (no sorting of eigenvalues)
*>
*>    (5)     0     if T(with VS) = T(without VS),
*>            1/ulp otherwise
*>            (no sorting of eigenvalues)
*>
*>    (6)     0     if eigenvalues(with VS) = eigenvalues(without VS),
*>            1/ulp otherwise
*>            (no sorting of eigenvalues)
*>
*>    (7)     0 if T is in Schur form, 1/ulp otherwise
*>            (with sorting of eigenvalues)
*>
*>    (8)     | A - VS T VS' | / ( n |A| ulp )
*>
*>      Here VS is the matrix of Schur eigenvectors, and T is in Schur
*>      form  (with sorting of eigenvalues).
*>
*>    (9)     | I - VS VS' | / ( n ulp ) (with sorting of eigenvalues).
*>
*>    (10)    0     if W are eigenvalues of T
*>            1/ulp otherwise
*>            If workspace sufficient, also compare W with and
*>            without reciprocal condition numbers
*>            (with sorting of eigenvalues)
*>
*>    (11)    0     if T(with VS) = T(without VS),
*>            1/ulp otherwise
*>            If workspace sufficient, also compare T with and without
*>            reciprocal condition numbers
*>            (with sorting of eigenvalues)
*>
*>    (12)    0     if eigenvalues(with VS) = eigenvalues(without VS),
*>            1/ulp otherwise
*>            If workspace sufficient, also compare VS with and without
*>            reciprocal condition numbers
*>            (with sorting of eigenvalues)
*>
*>    (13)    if sorting worked and SDIM is the number of
*>            eigenvalues which were SELECTed
*>            If workspace sufficient, also compare SDIM with and
*>            without reciprocal condition numbers
*>
*>    (14)    if RCONDE the same no matter if VS and/or RCONDV computed
*>
*>    (15)    if RCONDV the same no matter if VS and/or RCONDE computed
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
*>         T has evenly spaced entries 1, ..., ULP with random
*>         complex angles on the diagonal and random O(1) entries in
*>         the upper triangle.
*>
*>    (10) A matrix of the form  U' T U, where U is unitary and
*>         T has geometrically spaced entries 1, ..., ULP with random
*>         complex angles on the diagonal and random O(1) entries in
*>         the upper triangle.
*>
*>    (11) A matrix of the form  U' T U, where U is orthogonal and
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
*>    (19) Nonsymmetric matrix with random entries chosen from (-1,1).
*>         If N is at least 4, all entries in first two rows and last
*>         row, and first column and last two columns are zero.
*>    (20) Same as (19), but multiplied by a constant
*>         near the overflow threshold
*>    (21) Same as (19), but multiplied by a constant
*>         near the underflow threshold
*>
*>    In addition, an input file will be read from logical unit number
*>    NIUNIT. The file contains matrices along with precomputed
*>    eigenvalues and reciprocal condition numbers for the eigenvalue
*>    average and right invariant subspace. For these matrices, in
*>    addition to tests (1) to (15) we will compute the following two
*>    tests:
*>
*>   (16)  |RCONDE - RCDEIN| / cond(RCONDE)
*>
*>      RCONDE is the reciprocal average eigenvalue condition number
*>      computed by ZGEESX and RCDEIN (the precomputed true value)
*>      is supplied as input.  cond(RCONDE) is the condition number
*>      of RCONDE, and takes errors in computing RCONDE into account,
*>      so that the resulting quantity should be O(ULP). cond(RCONDE)
*>      is essentially given by norm(A)/RCONDV.
*>
*>   (17)  |RCONDV - RCDVIN| / cond(RCONDV)
*>
*>      RCONDV is the reciprocal right invariant subspace condition
*>      number computed by ZGEESX and RCDVIN (the precomputed true
*>      value) is supplied as input. cond(RCONDV) is the condition
*>      number of RCONDV, and takes errors in computing RCONDV into
*>      account, so that the resulting quantity should be O(ULP).
*>      cond(RCONDV) is essentially given by norm(A)/RCONDE.
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
*>          next call to ZDRVSX to continue the same random number
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
*>          A is COMPLEX*16 array, dimension (LDA, max(NN))
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually used.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, and H. LDA must be at
*>          least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[out] H
*> \verbatim
*>          H is COMPLEX*16 array, dimension (LDA, max(NN))
*>          Another copy of the test matrix A, modified by ZGEESX.
*> \endverbatim
*>
*> \param[out] HT
*> \verbatim
*>          HT is COMPLEX*16 array, dimension (LDA, max(NN))
*>          Yet another copy of the test matrix A, modified by ZGEESX.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX*16 array, dimension (max(NN))
*>          The computed eigenvalues of A.
*> \endverbatim
*>
*> \param[out] WT
*> \verbatim
*>          WT is COMPLEX*16 array, dimension (max(NN))
*>          Like W, this array contains the eigenvalues of A,
*>          but those computed when ZGEESX only computes a partial
*>          eigendecomposition, i.e. not Schur vectors
*> \endverbatim
*>
*> \param[out] WTMP
*> \verbatim
*>          WTMP is COMPLEX*16 array, dimension (max(NN))
*>          More temporary storage for eigenvalues.
*> \endverbatim
*>
*> \param[out] VS
*> \verbatim
*>          VS is COMPLEX*16 array, dimension (LDVS, max(NN))
*>          VS holds the computed Schur vectors.
*> \endverbatim
*>
*> \param[in] LDVS
*> \verbatim
*>          LDVS is INTEGER
*>          Leading dimension of VS. Must be at least max(1,max(NN)).
*> \endverbatim
*>
*> \param[out] VS1
*> \verbatim
*>          VS1 is COMPLEX*16 array, dimension (LDVS, max(NN))
*>          VS1 holds another copy of the computed Schur vectors.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (17)
*>          The values computed by the 17 tests described above.
*>          The values are currently limited to 1/ulp, to avoid overflow.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The number of entries in WORK.  This must be at least
*>          max(1,2*NN(j)**2) for all j.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] BWORK
*> \verbatim
*>          BWORK is LOGICAL array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          If 0,  successful exit.
*>            <0,  input parameter -INFO is incorrect
*>            >0,  ZLATMR, CLATMS, CLATME or ZGET24 returned an error
*>                 code and INFO is its absolute value
*>
*>-----------------------------------------------------------------------
*>
*>     Some Local Variables and Parameters:
*>     ---- ----- --------- --- ----------
*>     ZERO, ONE       Real 0 and 1.
*>     MAXTYP          The number of types defined.
*>     NMAX            Largest value in NN.
*>     NERRS           The number of tests which have exceeded THRESH
*>     COND, CONDS,
*>     IMODE           Values to be passed to the matrix generators.
*>     ANORM           Norm of A; passed to matrix generators.
*>
*>     OVFL, UNFL      Overflow and underflow thresholds.
*>     ULP, ULPINV     Finest relative precision and its inverse.
*>     RTULP, RTULPI   Square roots of the previous 4 values.
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
      SUBROUTINE ZDRVSX( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NIUNIT, NOUNIT, A, LDA, H, HT, W, WT, WTMP, VS,
     $                   LDVS, VS1, RESULT, WORK, LWORK, RWORK, BWORK,
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDVS, LWORK, NIUNIT, NOUNIT, NSIZES,
     $                   NTYPES
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * ), DOTYPE( * )
      INTEGER            ISEED( 4 ), NN( * )
      DOUBLE PRECISION   RESULT( 17 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), H( LDA, * ), HT( LDA, * ),
     $                   VS( LDVS, * ), VS1( LDVS, * ), W( * ),
     $                   WORK( * ), WT( * ), WTMP( * )
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
      CHARACTER*3        PATH
      INTEGER            I, IINFO, IMODE, ISRT, ITYPE, IWK, J, JCOL,
     $                   JSIZE, JTYPE, MTYPES, N, NERRS, NFAIL, NMAX,
     $                   NNWORK, NSLCT, NTEST, NTESTF, NTESTT
      DOUBLE PRECISION   ANORM, COND, CONDS, OVFL, RCDEIN, RCDVIN,
     $                   RTULP, RTULPI, ULP, ULPINV, UNFL
*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), ISLCT( 20 ),
     $                   KCONDS( MAXTYP ), KMAGN( MAXTYP ),
     $                   KMODE( MAXTYP ), KTYPE( MAXTYP )
*     ..
*     .. Arrays in Common ..
      LOGICAL            SELVAL( 20 )
      DOUBLE PRECISION   SELWI( 20 ), SELWR( 20 )
*     ..
*     .. Scalars in Common ..
      INTEGER            SELDIM, SELOPT
*     ..
*     .. Common blocks ..
      COMMON             / SSLCT / SELOPT, SELDIM, SELVAL, SELWR, SELWI
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLASUM, XERBLA, ZGET24, ZLASET, ZLATME,
     $                   ZLATMR, ZLATMS
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SQRT
*     ..
*     .. Data statements ..
      DATA               KTYPE / 1, 2, 3, 5*4, 4*6, 6*6, 3*9 /
      DATA               KMAGN / 3*1, 1, 1, 1, 2, 3, 4*1, 1, 1, 1, 1, 2,
     $                   3, 1, 2, 3 /
      DATA               KMODE / 3*0, 4, 3, 1, 4, 4, 4, 3, 1, 5, 4, 3,
     $                   1, 5, 5, 5, 4, 3, 1 /
      DATA               KCONDS / 3*0, 5*0, 4*1, 6*2, 3*0 /
*     ..
*     .. Executable Statements ..
*
      PATH( 1: 1 ) = 'Zomplex precision'
      PATH( 2: 3 ) = 'SX'
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
*     8 is the largest dimension in the input file of precomputed
*     problems
*
      NMAX = 8
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
      ELSE IF( NIUNIT.LE.0 ) THEN
         INFO = -7
      ELSE IF( NOUNIT.LE.0 ) THEN
         INFO = -8
      ELSE IF( LDA.LT.1 .OR. LDA.LT.NMAX ) THEN
         INFO = -10
      ELSE IF( LDVS.LT.1 .OR. LDVS.LT.NMAX ) THEN
         INFO = -20
      ELSE IF( MAX( 3*NMAX, 2*NMAX**2 ).GT.LWORK ) THEN
         INFO = -24
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZDRVSX', -INFO )
         RETURN
      END IF
*
*     If nothing to do check on NIUNIT
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   GO TO 150
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
      DO 140 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 130 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 130
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
            IF( ITYPE.EQ.1 ) THEN
*
*              Zero
*
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
     $               A( JCOL, JCOL-1 ) = CONE
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
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, 6, ONE, CONE,
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
               WRITE( NOUNIT, FMT = 9991 )'Generator', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               RETURN
            END IF
*
   90       CONTINUE
*
*           Test for minimal and generous workspace
*
            DO 120 IWK = 1, 2
               IF( IWK.EQ.1 ) THEN
                  NNWORK = 2*N
               ELSE
                  NNWORK = MAX( 2*N, N*( N+1 ) / 2 )
               END IF
               NNWORK = MAX( NNWORK, 1 )
*
               CALL ZGET24( .FALSE., JTYPE, THRESH, IOLDSD, NOUNIT, N,
     $                      A, LDA, H, HT, W, WT, WTMP, VS, LDVS, VS1,
     $                      RCDEIN, RCDVIN, NSLCT, ISLCT, 0, RESULT,
     $                      WORK, NNWORK, RWORK, BWORK, INFO )
*
*              Check for RESULT(j) > THRESH
*
               NTEST = 0
               NFAIL = 0
               DO 100 J = 1, 15
                  IF( RESULT( J ).GE.ZERO )
     $               NTEST = NTEST + 1
                  IF( RESULT( J ).GE.THRESH )
     $               NFAIL = NFAIL + 1
  100          CONTINUE
*
               IF( NFAIL.GT.0 )
     $            NTESTF = NTESTF + 1
               IF( NTESTF.EQ.1 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )PATH
                  WRITE( NOUNIT, FMT = 9998 )
                  WRITE( NOUNIT, FMT = 9997 )
                  WRITE( NOUNIT, FMT = 9996 )
                  WRITE( NOUNIT, FMT = 9995 )THRESH
                  WRITE( NOUNIT, FMT = 9994 )
                  NTESTF = 2
               END IF
*
               DO 110 J = 1, 15
                  IF( RESULT( J ).GE.THRESH ) THEN
                     WRITE( NOUNIT, FMT = 9993 )N, IWK, IOLDSD, JTYPE,
     $                  J, RESULT( J )
                  END IF
  110          CONTINUE
*
               NERRS = NERRS + NFAIL
               NTESTT = NTESTT + NTEST
*
  120       CONTINUE
  130    CONTINUE
  140 CONTINUE
*
  150 CONTINUE
*
*     Read in data from file to check accuracy of condition estimation
*     Read input data until N=0
*
      JTYPE = 0
  160 CONTINUE
      READ( NIUNIT, FMT = *, END = 200 )N, NSLCT, ISRT
      IF( N.EQ.0 )
     $   GO TO 200
      JTYPE = JTYPE + 1
      ISEED( 1 ) = JTYPE
      READ( NIUNIT, FMT = * )( ISLCT( I ), I = 1, NSLCT )
      DO 170 I = 1, N
         READ( NIUNIT, FMT = * )( A( I, J ), J = 1, N )
  170 CONTINUE
      READ( NIUNIT, FMT = * )RCDEIN, RCDVIN
*
      CALL ZGET24( .TRUE., 22, THRESH, ISEED, NOUNIT, N, A, LDA, H, HT,
     $             W, WT, WTMP, VS, LDVS, VS1, RCDEIN, RCDVIN, NSLCT,
     $             ISLCT, ISRT, RESULT, WORK, LWORK, RWORK, BWORK,
     $             INFO )
*
*     Check for RESULT(j) > THRESH
*
      NTEST = 0
      NFAIL = 0
      DO 180 J = 1, 17
         IF( RESULT( J ).GE.ZERO )
     $      NTEST = NTEST + 1
         IF( RESULT( J ).GE.THRESH )
     $      NFAIL = NFAIL + 1
  180 CONTINUE
*
      IF( NFAIL.GT.0 )
     $   NTESTF = NTESTF + 1
      IF( NTESTF.EQ.1 ) THEN
         WRITE( NOUNIT, FMT = 9999 )PATH
         WRITE( NOUNIT, FMT = 9998 )
         WRITE( NOUNIT, FMT = 9997 )
         WRITE( NOUNIT, FMT = 9996 )
         WRITE( NOUNIT, FMT = 9995 )THRESH
         WRITE( NOUNIT, FMT = 9994 )
         NTESTF = 2
      END IF
      DO 190 J = 1, 17
         IF( RESULT( J ).GE.THRESH ) THEN
            WRITE( NOUNIT, FMT = 9992 )N, JTYPE, J, RESULT( J )
         END IF
  190 CONTINUE
*
      NERRS = NERRS + NFAIL
      NTESTT = NTESTT + NTEST
      GO TO 160
  200 CONTINUE
*
*     Summary
*
      CALL DLASUM( PATH, NOUNIT, NERRS, NTESTT )
*
 9999 FORMAT( / 1X, A3, ' -- Complex Schur Form Decomposition Expert ',
     $      'Driver', / ' Matrix types (see ZDRVSX for details): ' )
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
     $      'dom entries.   ', / )
 9995 FORMAT( ' Tests performed with test threshold =', F8.2,
     $      / ' ( A denotes A on input and T denotes A on output)',
     $      / / ' 1 = 0 if T in Schur form (no sort), ',
     $      '  1/ulp otherwise', /
     $      ' 2 = | A - VS T transpose(VS) | / ( n |A| ulp ) (no sort)',
     $      / ' 3 = | I - VS transpose(VS) | / ( n ulp ) (no sort) ',
     $      / ' 4 = 0 if W are eigenvalues of T (no sort),',
     $      '  1/ulp otherwise', /
     $      ' 5 = 0 if T same no matter if VS computed (no sort),',
     $      '  1/ulp otherwise', /
     $      ' 6 = 0 if W same no matter if VS computed (no sort)',
     $      ',  1/ulp otherwise' )
 9994 FORMAT( ' 7 = 0 if T in Schur form (sort), ', '  1/ulp otherwise',
     $      / ' 8 = | A - VS T transpose(VS) | / ( n |A| ulp ) (sort)',
     $      / ' 9 = | I - VS transpose(VS) | / ( n ulp ) (sort) ',
     $      / ' 10 = 0 if W are eigenvalues of T (sort),',
     $      '  1/ulp otherwise', /
     $      ' 11 = 0 if T same no matter what else computed (sort),',
     $      '  1/ulp otherwise', /
     $      ' 12 = 0 if W same no matter what else computed ',
     $      '(sort), 1/ulp otherwise', /
     $      ' 13 = 0 if sorting successful, 1/ulp otherwise',
     $      / ' 14 = 0 if RCONDE same no matter what else computed,',
     $      ' 1/ulp otherwise', /
     $      ' 15 = 0 if RCONDv same no matter what else computed,',
     $      ' 1/ulp otherwise', /
     $      ' 16 = | RCONDE - RCONDE(precomputed) | / cond(RCONDE),',
     $      / ' 17 = | RCONDV - RCONDV(precomputed) | / cond(RCONDV),' )
 9993 FORMAT( ' N=', I5, ', IWK=', I2, ', seed=', 4( I4, ',' ),
     $      ' type ', I2, ', test(', I2, ')=', G10.3 )
 9992 FORMAT( ' N=', I5, ', input example =', I3, ',  test(', I2, ')=',
     $      G10.3 )
 9991 FORMAT( ' ZDRVSX: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
      RETURN
*
*     End of ZDRVSX
*
      END
