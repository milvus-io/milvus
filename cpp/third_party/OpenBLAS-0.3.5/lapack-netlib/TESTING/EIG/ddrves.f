*> \brief \b DDRVES
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DDRVES( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, H, HT, WR, WI, WRT, WIT, VS,
*                          LDVS, RESULT, WORK, NWORK, IWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDVS, NOUNIT, NSIZES, NTYPES, NWORK
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * ), DOTYPE( * )
*       INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
*       DOUBLE PRECISION   A( LDA, * ), H( LDA, * ), HT( LDA, * ),
*      $                   RESULT( 13 ), VS( LDVS, * ), WI( * ), WIT( * ),
*      $                   WORK( * ), WR( * ), WRT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DDRVES checks the nonsymmetric eigenvalue (Schur form) problem
*>    driver DGEES.
*>
*>    When DDRVES is called, a number of matrix "sizes" ("n's") and a
*>    number of matrix "types" are specified.  For each size ("n")
*>    and each type of matrix, one matrix will be generated and used
*>    to test the nonsymmetric eigenroutines.  For each matrix, 13
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
*>    (4)     0     if WR+sqrt(-1)*WI are eigenvalues of T
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
*>    (10)    0     if WR+sqrt(-1)*WI are eigenvalues of T
*>            1/ulp otherwise
*>            (with sorting of eigenvalues)
*>
*>    (11)    0     if T(with VS) = T(without VS),
*>            1/ulp otherwise
*>            (with sorting of eigenvalues)
*>
*>    (12)    0     if eigenvalues(with VS) = eigenvalues(without VS),
*>            1/ulp otherwise
*>            (with sorting of eigenvalues)
*>
*>    (13)    if sorting worked and SDIM is the number of
*>            eigenvalues which were SELECTed
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
*>         1, ..., ULP  and random signs.
*>         (ULP = (first number larger than 1) - 1 )
*>    (5)  A diagonal matrix with geometrically spaced entries
*>         1, ..., ULP  and random signs.
*>    (6)  A diagonal matrix with "clustered" entries 1, ULP, ..., ULP
*>         and random signs.
*>
*>    (7)  Same as (4), but multiplied by a constant near
*>         the overflow threshold
*>    (8)  Same as (4), but multiplied by a constant near
*>         the underflow threshold
*>
*>    (9)  A matrix of the form  U' T U, where U is orthogonal and
*>         T has evenly spaced entries 1, ..., ULP with random signs
*>         on the diagonal and random O(1) entries in the upper
*>         triangle.
*>
*>    (10) A matrix of the form  U' T U, where U is orthogonal and
*>         T has geometrically spaced entries 1, ..., ULP with random
*>         signs on the diagonal and random O(1) entries in the upper
*>         triangle.
*>
*>    (11) A matrix of the form  U' T U, where U is orthogonal and
*>         T has "clustered" entries 1, ULP,..., ULP with random
*>         signs on the diagonal and random O(1) entries in the upper
*>         triangle.
*>
*>    (12) A matrix of the form  U' T U, where U is orthogonal and
*>         T has real or complex conjugate paired eigenvalues randomly
*>         chosen from ( ULP, 1 ) and random O(1) entries in the upper
*>         triangle.
*>
*>    (13) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has evenly spaced entries 1, ..., ULP
*>         with random signs on the diagonal and random O(1) entries
*>         in the upper triangle.
*>
*>    (14) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has geometrically spaced entries
*>         1, ..., ULP with random signs on the diagonal and random
*>         O(1) entries in the upper triangle.
*>
*>    (15) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has "clustered" entries 1, ULP,..., ULP
*>         with random signs on the diagonal and random O(1) entries
*>         in the upper triangle.
*>
*>    (16) A matrix of the form  X' T X, where X has condition
*>         SQRT( ULP ) and T has real or complex conjugate paired
*>         eigenvalues randomly chosen from ( ULP, 1 ) and random
*>         O(1) entries in the upper triangle.
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
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          DDRVES does nothing.  It must be at least zero.
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
*>          The number of elements in DOTYPE.   If it is zero, DDRVES
*>          does nothing.  It must be at least zero.  If it is MAXTYP+1
*>          and NSIZES is 1, then an additional type, MAXTYP+1 is
*>          defined, which is to use whatever matrix is in A.  This
*>          is only useful if DOTYPE(1:MAXTYP) is .FALSE. and
*>          DOTYPE(MAXTYP+1) is .TRUE. .
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
*>          next call to DDRVES to continue the same random number
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
*> \param[in] NOUNIT
*> \verbatim
*>          NOUNIT is INTEGER
*>          The FORTRAN unit number for printing out error messages
*>          (e.g., if a routine returns INFO not equal to 0.)
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA, max(NN))
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually used.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, and H. LDA must be at
*>          least 1 and at least max(NN).
*> \endverbatim
*>
*> \param[out] H
*> \verbatim
*>          H is DOUBLE PRECISION array, dimension (LDA, max(NN))
*>          Another copy of the test matrix A, modified by DGEES.
*> \endverbatim
*>
*> \param[out] HT
*> \verbatim
*>          HT is DOUBLE PRECISION array, dimension (LDA, max(NN))
*>          Yet another copy of the test matrix A, modified by DGEES.
*> \endverbatim
*>
*> \param[out] WR
*> \verbatim
*>          WR is DOUBLE PRECISION array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] WI
*> \verbatim
*>          WI is DOUBLE PRECISION array, dimension (max(NN))
*>
*>          The real and imaginary parts of the eigenvalues of A.
*>          On exit, WR + WI*i are the eigenvalues of the matrix in A.
*> \endverbatim
*>
*> \param[out] WRT
*> \verbatim
*>          WRT is DOUBLE PRECISION array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] WIT
*> \verbatim
*>          WIT is DOUBLE PRECISION array, dimension (max(NN))
*>
*>          Like WR, WI, these arrays contain the eigenvalues of A,
*>          but those computed when DGEES only computes a partial
*>          eigendecomposition, i.e. not Schur vectors
*> \endverbatim
*>
*> \param[out] VS
*> \verbatim
*>          VS is DOUBLE PRECISION array, dimension (LDVS, max(NN))
*>          VS holds the computed Schur vectors.
*> \endverbatim
*>
*> \param[in] LDVS
*> \verbatim
*>          LDVS is INTEGER
*>          Leading dimension of VS. Must be at least max(1,max(NN)).
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (13)
*>          The values computed by the 13 tests described above.
*>          The values are currently limited to 1/ulp, to avoid overflow.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (NWORK)
*> \endverbatim
*>
*> \param[in] NWORK
*> \verbatim
*>          NWORK is INTEGER
*>          The number of entries in WORK.  This must be at least
*>          5*NN(j)+2*NN(j)**2 for all j.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (max(NN))
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
*>          If 0, then everything ran OK.
*>           -1: NSIZES < 0
*>           -2: Some NN(j) < 0
*>           -3: NTYPES < 0
*>           -6: THRESH < 0
*>           -9: LDA < 1 or LDA < NMAX, where NMAX is max( NN(j) ).
*>          -17: LDVS < 1 or LDVS < NMAX, where NMAX is max( NN(j) ).
*>          -20: NWORK too small.
*>          If  DLATMR, SLATMS, SLATME or DGEES returns an error code,
*>              the absolute value of it is returned.
*>
*>-----------------------------------------------------------------------
*>
*>     Some Local Variables and Parameters:
*>     ---- ----- --------- --- ----------
*>
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DDRVES( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NOUNIT, A, LDA, H, HT, WR, WI, WRT, WIT, VS,
     $                   LDVS, RESULT, WORK, NWORK, IWORK, BWORK, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDVS, NOUNIT, NSIZES, NTYPES, NWORK
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * ), DOTYPE( * )
      INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
      DOUBLE PRECISION   A( LDA, * ), H( LDA, * ), HT( LDA, * ),
     $                   RESULT( 13 ), VS( LDVS, * ), WI( * ), WIT( * ),
     $                   WORK( * ), WR( * ), WRT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 21 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN
      CHARACTER          SORT
      CHARACTER*3        PATH
      INTEGER            I, IINFO, IMODE, ISORT, ITYPE, IWK, J, JCOL,
     $                   JSIZE, JTYPE, KNTEIG, LWORK, MTYPES, N, NERRS,
     $                   NFAIL, NMAX, NNWORK, NTEST, NTESTF, NTESTT,
     $                   RSUB, SDIM
      DOUBLE PRECISION   ANORM, COND, CONDS, OVFL, RTULP, RTULPI, TMP,
     $                   ULP, ULPINV, UNFL
*     ..
*     .. Local Arrays ..
      CHARACTER          ADUMMA( 1 )
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), KCONDS( MAXTYP ),
     $                   KMAGN( MAXTYP ), KMODE( MAXTYP ),
     $                   KTYPE( MAXTYP )
      DOUBLE PRECISION   RES( 2 )
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
      LOGICAL            DSLECT
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DSLECT, DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEES, DHST01, DLABAD, DLACPY, DLASET, DLASUM,
     $                   DLATME, DLATMR, DLATMS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SIGN, SQRT
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
      PATH( 1: 1 ) = 'Double precision'
      PATH( 2: 3 ) = 'ES'
*
*     Check for errors
*
      NTESTT = 0
      NTESTF = 0
      INFO = 0
      SELOPT = 0
*
*     Important constants
*
      BADNN = .FALSE.
      NMAX = 0
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
      ELSE IF( NOUNIT.LE.0 ) THEN
         INFO = -7
      ELSE IF( LDA.LT.1 .OR. LDA.LT.NMAX ) THEN
         INFO = -9
      ELSE IF( LDVS.LT.1 .OR. LDVS.LT.NMAX ) THEN
         INFO = -17
      ELSE IF( 5*NMAX+2*NMAX**2.GT.NWORK ) THEN
         INFO = -20
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DDRVES', -INFO )
         RETURN
      END IF
*
*     Quick return if nothing to do
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   RETURN
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
      DO 270 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         MTYPES = MAXTYP
         IF( NSIZES.EQ.1 .AND. NTYPES.EQ.MAXTYP+1 )
     $      MTYPES = MTYPES + 1
*
         DO 260 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 260
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
            CALL DLASET( 'Full', LDA, N, ZERO, ZERO, A, LDA )
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
               CALL DLATMS( N, N, 'S', ISEED, 'S', WORK, IMODE, COND,
     $                      ANORM, 0, 0, 'N', A, LDA, WORK( N+1 ),
     $                      IINFO )
*
            ELSE IF( ITYPE.EQ.5 ) THEN
*
*              Symmetric, eigenvalues specified
*
               CALL DLATMS( N, N, 'S', ISEED, 'S', WORK, IMODE, COND,
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
               ADUMMA( 1 ) = ' '
               CALL DLATME( N, 'S', ISEED, WORK, IMODE, COND, ONE,
     $                      ADUMMA, 'T', 'T', 'T', WORK( N+1 ), 4,
     $                      CONDS, N, N, ANORM, A, LDA, WORK( 2*N+1 ),
     $                      IINFO )
*
            ELSE IF( ITYPE.EQ.7 ) THEN
*
*              Diagonal, random eigenvalues
*
               CALL DLATMR( N, N, 'S', ISEED, 'S', WORK, 6, ONE, ONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.8 ) THEN
*
*              Symmetric, random eigenvalues
*
               CALL DLATMR( N, N, 'S', ISEED, 'S', WORK, 6, ONE, ONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.9 ) THEN
*
*              General, random eigenvalues
*
               CALL DLATMR( N, N, 'S', ISEED, 'N', WORK, 6, ONE, ONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
               IF( N.GE.4 ) THEN
                  CALL DLASET( 'Full', 2, N, ZERO, ZERO, A, LDA )
                  CALL DLASET( 'Full', N-3, 1, ZERO, ZERO, A( 3, 1 ),
     $                         LDA )
                  CALL DLASET( 'Full', N-3, 2, ZERO, ZERO, A( 3, N-1 ),
     $                         LDA )
                  CALL DLASET( 'Full', 1, N, ZERO, ZERO, A( N, 1 ),
     $                         LDA )
               END IF
*
            ELSE IF( ITYPE.EQ.10 ) THEN
*
*              Triangular, random eigenvalues
*
               CALL DLATMR( N, N, 'S', ISEED, 'N', WORK, 6, ONE, ONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
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
            DO 250 IWK = 1, 2
               IF( IWK.EQ.1 ) THEN
                  NNWORK = 3*N
               ELSE
                  NNWORK = 5*N + 2*N**2
               END IF
               NNWORK = MAX( NNWORK, 1 )
*
*              Initialize RESULT
*
               DO 100 J = 1, 13
                  RESULT( J ) = -ONE
  100          CONTINUE
*
*              Test with and without sorting of eigenvalues
*
               DO 210 ISORT = 0, 1
                  IF( ISORT.EQ.0 ) THEN
                     SORT = 'N'
                     RSUB = 0
                  ELSE
                     SORT = 'S'
                     RSUB = 6
                  END IF
*
*                 Compute Schur form and Schur vectors, and test them
*
                  CALL DLACPY( 'F', N, N, A, LDA, H, LDA )
                  CALL DGEES( 'V', SORT, DSLECT, N, H, LDA, SDIM, WR,
     $                        WI, VS, LDVS, WORK, NNWORK, BWORK, IINFO )
                  IF( IINFO.NE.0 .AND. IINFO.NE.N+2 ) THEN
                     RESULT( 1+RSUB ) = ULPINV
                     WRITE( NOUNIT, FMT = 9992 )'DGEES1', IINFO, N,
     $                  JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     GO TO 220
                  END IF
*
*                 Do Test (1) or Test (7)
*
                  RESULT( 1+RSUB ) = ZERO
                  DO 120 J = 1, N - 2
                     DO 110 I = J + 2, N
                        IF( H( I, J ).NE.ZERO )
     $                     RESULT( 1+RSUB ) = ULPINV
  110                CONTINUE
  120             CONTINUE
                  DO 130 I = 1, N - 2
                     IF( H( I+1, I ).NE.ZERO .AND. H( I+2, I+1 ).NE.
     $                   ZERO )RESULT( 1+RSUB ) = ULPINV
  130             CONTINUE
                  DO 140 I = 1, N - 1
                     IF( H( I+1, I ).NE.ZERO ) THEN
                        IF( H( I, I ).NE.H( I+1, I+1 ) .OR.
     $                      H( I, I+1 ).EQ.ZERO .OR.
     $                      SIGN( ONE, H( I+1, I ) ).EQ.
     $                      SIGN( ONE, H( I, I+1 ) ) )RESULT( 1+RSUB )
     $                      = ULPINV
                     END IF
  140             CONTINUE
*
*                 Do Tests (2) and (3) or Tests (8) and (9)
*
                  LWORK = MAX( 1, 2*N*N )
                  CALL DHST01( N, 1, N, A, LDA, H, LDA, VS, LDVS, WORK,
     $                         LWORK, RES )
                  RESULT( 2+RSUB ) = RES( 1 )
                  RESULT( 3+RSUB ) = RES( 2 )
*
*                 Do Test (4) or Test (10)
*
                  RESULT( 4+RSUB ) = ZERO
                  DO 150 I = 1, N
                     IF( H( I, I ).NE.WR( I ) )
     $                  RESULT( 4+RSUB ) = ULPINV
  150             CONTINUE
                  IF( N.GT.1 ) THEN
                     IF( H( 2, 1 ).EQ.ZERO .AND. WI( 1 ).NE.ZERO )
     $                  RESULT( 4+RSUB ) = ULPINV
                     IF( H( N, N-1 ).EQ.ZERO .AND. WI( N ).NE.ZERO )
     $                  RESULT( 4+RSUB ) = ULPINV
                  END IF
                  DO 160 I = 1, N - 1
                     IF( H( I+1, I ).NE.ZERO ) THEN
                        TMP = SQRT( ABS( H( I+1, I ) ) )*
     $                        SQRT( ABS( H( I, I+1 ) ) )
                        RESULT( 4+RSUB ) = MAX( RESULT( 4+RSUB ),
     $                                     ABS( WI( I )-TMP ) /
     $                                     MAX( ULP*TMP, UNFL ) )
                        RESULT( 4+RSUB ) = MAX( RESULT( 4+RSUB ),
     $                                     ABS( WI( I+1 )+TMP ) /
     $                                     MAX( ULP*TMP, UNFL ) )
                     ELSE IF( I.GT.1 ) THEN
                        IF( H( I+1, I ).EQ.ZERO .AND. H( I, I-1 ).EQ.
     $                      ZERO .AND. WI( I ).NE.ZERO )RESULT( 4+RSUB )
     $                       = ULPINV
                     END IF
  160             CONTINUE
*
*                 Do Test (5) or Test (11)
*
                  CALL DLACPY( 'F', N, N, A, LDA, HT, LDA )
                  CALL DGEES( 'N', SORT, DSLECT, N, HT, LDA, SDIM, WRT,
     $                        WIT, VS, LDVS, WORK, NNWORK, BWORK,
     $                        IINFO )
                  IF( IINFO.NE.0 .AND. IINFO.NE.N+2 ) THEN
                     RESULT( 5+RSUB ) = ULPINV
                     WRITE( NOUNIT, FMT = 9992 )'DGEES2', IINFO, N,
     $                  JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     GO TO 220
                  END IF
*
                  RESULT( 5+RSUB ) = ZERO
                  DO 180 J = 1, N
                     DO 170 I = 1, N
                        IF( H( I, J ).NE.HT( I, J ) )
     $                     RESULT( 5+RSUB ) = ULPINV
  170                CONTINUE
  180             CONTINUE
*
*                 Do Test (6) or Test (12)
*
                  RESULT( 6+RSUB ) = ZERO
                  DO 190 I = 1, N
                     IF( WR( I ).NE.WRT( I ) .OR. WI( I ).NE.WIT( I ) )
     $                  RESULT( 6+RSUB ) = ULPINV
  190             CONTINUE
*
*                 Do Test (13)
*
                  IF( ISORT.EQ.1 ) THEN
                     RESULT( 13 ) = ZERO
                     KNTEIG = 0
                     DO 200 I = 1, N
                        IF( DSLECT( WR( I ), WI( I ) ) .OR.
     $                      DSLECT( WR( I ), -WI( I ) ) )
     $                      KNTEIG = KNTEIG + 1
                        IF( I.LT.N ) THEN
                           IF( ( DSLECT( WR( I+1 ),
     $                         WI( I+1 ) ) .OR. DSLECT( WR( I+1 ),
     $                         -WI( I+1 ) ) ) .AND.
     $                         ( .NOT.( DSLECT( WR( I ),
     $                         WI( I ) ) .OR. DSLECT( WR( I ),
     $                         -WI( I ) ) ) ) .AND. IINFO.NE.N+2 )
     $                         RESULT( 13 ) = ULPINV
                        END IF
  200                CONTINUE
                     IF( SDIM.NE.KNTEIG ) THEN
                        RESULT( 13 ) = ULPINV
                     END IF
                  END IF
*
  210          CONTINUE
*
*              End of Loop -- Check for RESULT(j) > THRESH
*
  220          CONTINUE
*
               NTEST = 0
               NFAIL = 0
               DO 230 J = 1, 13
                  IF( RESULT( J ).GE.ZERO )
     $               NTEST = NTEST + 1
                  IF( RESULT( J ).GE.THRESH )
     $               NFAIL = NFAIL + 1
  230          CONTINUE
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
               DO 240 J = 1, 13
                  IF( RESULT( J ).GE.THRESH ) THEN
                     WRITE( NOUNIT, FMT = 9993 )N, IWK, IOLDSD, JTYPE,
     $                  J, RESULT( J )
                  END IF
  240          CONTINUE
*
               NERRS = NERRS + NFAIL
               NTESTT = NTESTT + NTEST
*
  250       CONTINUE
  260    CONTINUE
  270 CONTINUE
*
*     Summary
*
      CALL DLASUM( PATH, NOUNIT, NERRS, NTESTT )
*
 9999 FORMAT( / 1X, A3, ' -- Real Schur Form Decomposition Driver',
     $      / ' Matrix types (see DDRVES for details): ' )
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
     $      'lex ', / ' 12=Well-cond., random complex ', 6X, '   ',
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
     $      / ' 3 = | I - VS transpose(VS) | / ( n ulp ) (no sort) ', /
     $      ' 4 = 0 if WR+sqrt(-1)*WI are eigenvalues of T (no sort),',
     $      '  1/ulp otherwise', /
     $      ' 5 = 0 if T same no matter if VS computed (no sort),',
     $      '  1/ulp otherwise', /
     $      ' 6 = 0 if WR, WI same no matter if VS computed (no sort)',
     $      ',  1/ulp otherwise' )
 9994 FORMAT( ' 7 = 0 if T in Schur form (sort), ', '  1/ulp otherwise',
     $      / ' 8 = | A - VS T transpose(VS) | / ( n |A| ulp ) (sort)',
     $      / ' 9 = | I - VS transpose(VS) | / ( n ulp ) (sort) ',
     $      / ' 10 = 0 if WR+sqrt(-1)*WI are eigenvalues of T (sort),',
     $      '  1/ulp otherwise', /
     $      ' 11 = 0 if T same no matter if VS computed (sort),',
     $      '  1/ulp otherwise', /
     $      ' 12 = 0 if WR, WI same no matter if VS computed (sort),',
     $      '  1/ulp otherwise', /
     $      ' 13 = 0 if sorting successful, 1/ulp otherwise', / )
 9993 FORMAT( ' N=', I5, ', IWK=', I2, ', seed=', 4( I4, ',' ),
     $      ' type ', I2, ', test(', I2, ')=', G10.3 )
 9992 FORMAT( ' DDRVES: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
      RETURN
*
*     End of DDRVES
*
      END
