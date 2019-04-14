*> \brief \b ZCHKHS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZCHKHS( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, H, T1, T2, U, LDU, Z, UZ, W1,
*                          W3, EVECTL, EVECTR, EVECTY, EVECTX, UU, TAU,
*                          WORK, NWORK, RWORK, IWORK, SELECT, RESULT,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDU, NOUNIT, NSIZES, NTYPES, NWORK
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * ), SELECT( * )
*       INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
*       DOUBLE PRECISION   RESULT( 14 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), EVECTL( LDU, * ),
*      $                   EVECTR( LDU, * ), EVECTX( LDU, * ),
*      $                   EVECTY( LDU, * ), H( LDA, * ), T1( LDA, * ),
*      $                   T2( LDA, * ), TAU( * ), U( LDU, * ),
*      $                   UU( LDU, * ), UZ( LDU, * ), W1( * ), W3( * ),
*      $                   WORK( * ), Z( LDU, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZCHKHS  checks the nonsymmetric eigenvalue problem routines.
*>
*>            ZGEHRD factors A as  U H U' , where ' means conjugate
*>            transpose, H is hessenberg, and U is unitary.
*>
*>            ZUNGHR generates the unitary matrix U.
*>
*>            ZUNMHR multiplies a matrix by the unitary matrix U.
*>
*>            ZHSEQR factors H as  Z T Z' , where Z is unitary and T
*>            is upper triangular.  It also computes the eigenvalues,
*>            w(1), ..., w(n); we define a diagonal matrix W whose
*>            (diagonal) entries are the eigenvalues.
*>
*>            ZTREVC computes the left eigenvector matrix L and the
*>            right eigenvector matrix R for the matrix T.  The
*>            columns of L are the complex conjugates of the left
*>            eigenvectors of T.  The columns of R are the right
*>            eigenvectors of T.  L is lower triangular, and R is
*>            upper triangular.
*>
*>            ZHSEIN computes the left eigenvector matrix Y and the
*>            right eigenvector matrix X for the matrix H.  The
*>            columns of Y are the complex conjugates of the left
*>            eigenvectors of H.  The columns of X are the right
*>            eigenvectors of H.  Y is lower triangular, and X is
*>            upper triangular.
*>
*>    When ZCHKHS is called, a number of matrix "sizes" ("n's") and a
*>    number of matrix "types" are specified.  For each size ("n")
*>    and each type of matrix, one matrix will be generated and used
*>    to test the nonsymmetric eigenroutines.  For each matrix, 14
*>    tests will be performed:
*>
*>    (1)     | A - U H U**H | / ( |A| n ulp )
*>
*>    (2)     | I - UU**H | / ( n ulp )
*>
*>    (3)     | H - Z T Z**H | / ( |H| n ulp )
*>
*>    (4)     | I - ZZ**H | / ( n ulp )
*>
*>    (5)     | A - UZ H (UZ)**H | / ( |A| n ulp )
*>
*>    (6)     | I - UZ (UZ)**H | / ( n ulp )
*>
*>    (7)     | T(Z computed) - T(Z not computed) | / ( |T| ulp )
*>
*>    (8)     | W(Z computed) - W(Z not computed) | / ( |W| ulp )
*>
*>    (9)     | TR - RW | / ( |T| |R| ulp )
*>
*>    (10)    | L**H T - W**H L | / ( |T| |L| ulp )
*>
*>    (11)    | HX - XW | / ( |H| |X| ulp )
*>
*>    (12)    | Y**H H - W**H Y | / ( |H| |Y| ulp )
*>
*>    (13)    | AX - XW | / ( |A| |X| ulp )
*>
*>    (14)    | Y**H A - W**H Y | / ( |A| |Y| ulp )
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
*>    (7)  Same as (4), but multiplied by SQRT( overflow threshold )
*>    (8)  Same as (4), but multiplied by SQRT( underflow threshold )
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
*>         from   ULP < |z| < 1   and random O(1) entries in the upper
*>         triangle.
*>
*>    (17) Same as (16), but multiplied by SQRT( overflow threshold )
*>    (18) Same as (16), but multiplied by SQRT( underflow threshold )
*>
*>    (19) Nonsymmetric matrix with random entries chosen from |z| < 1
*>    (20) Same as (19), but multiplied by SQRT( overflow threshold )
*>    (21) Same as (19), but multiplied by SQRT( underflow threshold )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  NSIZES - INTEGER
*>           The number of sizes of matrices to use.  If it is zero,
*>           ZCHKHS does nothing.  It must be at least zero.
*>           Not modified.
*>
*>  NN     - INTEGER array, dimension (NSIZES)
*>           An array containing the sizes to be used for the matrices.
*>           Zero values will be skipped.  The values must be at least
*>           zero.
*>           Not modified.
*>
*>  NTYPES - INTEGER
*>           The number of elements in DOTYPE.   If it is zero, ZCHKHS
*>           does nothing.  It must be at least zero.  If it is MAXTYP+1
*>           and NSIZES is 1, then an additional type, MAXTYP+1 is
*>           defined, which is to use whatever matrix is in A.  This
*>           is only useful if DOTYPE(1:MAXTYP) is .FALSE. and
*>           DOTYPE(MAXTYP+1) is .TRUE. .
*>           Not modified.
*>
*>  DOTYPE - LOGICAL array, dimension (NTYPES)
*>           If DOTYPE(j) is .TRUE., then for each size in NN a
*>           matrix of that size and of type j will be generated.
*>           If NTYPES is smaller than the maximum number of types
*>           defined (PARAMETER MAXTYP), then types NTYPES+1 through
*>           MAXTYP will not be generated.  If NTYPES is larger
*>           than MAXTYP, DOTYPE(MAXTYP+1) through DOTYPE(NTYPES)
*>           will be ignored.
*>           Not modified.
*>
*>  ISEED  - INTEGER array, dimension (4)
*>           On entry ISEED specifies the seed of the random number
*>           generator. The array elements should be between 0 and 4095;
*>           if not they will be reduced mod 4096.  Also, ISEED(4) must
*>           be odd.  The random number generator uses a linear
*>           congruential sequence limited to small integers, and so
*>           should produce machine independent random numbers. The
*>           values of ISEED are changed on exit, and can be used in the
*>           next call to ZCHKHS to continue the same random number
*>           sequence.
*>           Modified.
*>
*>  THRESH - DOUBLE PRECISION
*>           A test will count as "failed" if the "error", computed as
*>           described above, exceeds THRESH.  Note that the error
*>           is scaled to be O(1), so THRESH should be a reasonably
*>           small multiple of 1, e.g., 10 or 100.  In particular,
*>           it should not depend on the precision (single vs. double)
*>           or the size of the matrix.  It must be at least zero.
*>           Not modified.
*>
*>  NOUNIT - INTEGER
*>           The FORTRAN unit number for printing out error messages
*>           (e.g., if a routine returns IINFO not equal to 0.)
*>           Not modified.
*>
*>  A      - COMPLEX*16 array, dimension (LDA,max(NN))
*>           Used to hold the matrix whose eigenvalues are to be
*>           computed.  On exit, A contains the last matrix actually
*>           used.
*>           Modified.
*>
*>  LDA    - INTEGER
*>           The leading dimension of A, H, T1 and T2.  It must be at
*>           least 1 and at least max( NN ).
*>           Not modified.
*>
*>  H      - COMPLEX*16 array, dimension (LDA,max(NN))
*>           The upper hessenberg matrix computed by ZGEHRD.  On exit,
*>           H contains the Hessenberg form of the matrix in A.
*>           Modified.
*>
*>  T1     - COMPLEX*16 array, dimension (LDA,max(NN))
*>           The Schur (="quasi-triangular") matrix computed by ZHSEQR
*>           if Z is computed.  On exit, T1 contains the Schur form of
*>           the matrix in A.
*>           Modified.
*>
*>  T2     - COMPLEX*16 array, dimension (LDA,max(NN))
*>           The Schur matrix computed by ZHSEQR when Z is not computed.
*>           This should be identical to T1.
*>           Modified.
*>
*>  LDU    - INTEGER
*>           The leading dimension of U, Z, UZ and UU.  It must be at
*>           least 1 and at least max( NN ).
*>           Not modified.
*>
*>  U      - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The unitary matrix computed by ZGEHRD.
*>           Modified.
*>
*>  Z      - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The unitary matrix computed by ZHSEQR.
*>           Modified.
*>
*>  UZ     - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The product of U times Z.
*>           Modified.
*>
*>  W1     - COMPLEX*16 array, dimension (max(NN))
*>           The eigenvalues of A, as computed by a full Schur
*>           decomposition H = Z T Z'.  On exit, W1 contains the
*>           eigenvalues of the matrix in A.
*>           Modified.
*>
*>  W3     - COMPLEX*16 array, dimension (max(NN))
*>           The eigenvalues of A, as computed by a partial Schur
*>           decomposition (Z not computed, T only computed as much
*>           as is necessary for determining eigenvalues).  On exit,
*>           W3 contains the eigenvalues of the matrix in A, possibly
*>           perturbed by ZHSEIN.
*>           Modified.
*>
*>  EVECTL - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The conjugate transpose of the (upper triangular) left
*>           eigenvector matrix for the matrix in T1.
*>           Modified.
*>
*>  EVEZTR - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The (upper triangular) right eigenvector matrix for the
*>           matrix in T1.
*>           Modified.
*>
*>  EVECTY - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The conjugate transpose of the left eigenvector matrix
*>           for the matrix in H.
*>           Modified.
*>
*>  EVECTX - COMPLEX*16 array, dimension (LDU,max(NN))
*>           The right eigenvector matrix for the matrix in H.
*>           Modified.
*>
*>  UU     - COMPLEX*16 array, dimension (LDU,max(NN))
*>           Details of the unitary matrix computed by ZGEHRD.
*>           Modified.
*>
*>  TAU    - COMPLEX*16 array, dimension (max(NN))
*>           Further details of the unitary matrix computed by ZGEHRD.
*>           Modified.
*>
*>  WORK   - COMPLEX*16 array, dimension (NWORK)
*>           Workspace.
*>           Modified.
*>
*>  NWORK  - INTEGER
*>           The number of entries in WORK.  NWORK >= 4*NN(j)*NN(j) + 2.
*>
*>  RWORK  - DOUBLE PRECISION array, dimension (max(NN))
*>           Workspace.  Could be equivalenced to IWORK, but not SELECT.
*>           Modified.
*>
*>  IWORK  - INTEGER array, dimension (max(NN))
*>           Workspace.
*>           Modified.
*>
*>  SELECT - LOGICAL array, dimension (max(NN))
*>           Workspace.  Could be equivalenced to IWORK, but not RWORK.
*>           Modified.
*>
*>  RESULT - DOUBLE PRECISION array, dimension (14)
*>           The values computed by the fourteen tests described above.
*>           The values are currently limited to 1/ulp, to avoid
*>           overflow.
*>           Modified.
*>
*>  INFO   - INTEGER
*>           If 0, then everything ran OK.
*>            -1: NSIZES < 0
*>            -2: Some NN(j) < 0
*>            -3: NTYPES < 0
*>            -6: THRESH < 0
*>            -9: LDA < 1 or LDA < NMAX, where NMAX is max( NN(j) ).
*>           -14: LDU < 1 or LDU < NMAX.
*>           -26: NWORK too small.
*>           If  ZLATMR, CLATMS, or CLATME returns an error code, the
*>               absolute value of it is returned.
*>           If 1, then ZHSEQR could not find all the shifts.
*>           If 2, then the EISPACK code (for small blocks) failed.
*>           If >2, then 30*N iterations were not enough to find an
*>               eigenvalue or to decompose the problem.
*>           Modified.
*>
*>-----------------------------------------------------------------------
*>
*>     Some Local Variables and Parameters:
*>     ---- ----- --------- --- ----------
*>
*>     ZERO, ONE       Real 0 and 1.
*>     MAXTYP          The number of types defined.
*>     MTEST           The number of tests defined: care must be taken
*>                     that (1) the size of RESULT, (2) the number of
*>                     tests actually performed, and (3) MTEST agree.
*>     NTEST           The number of tests performed on this matrix
*>                     so far.  This should be less than MTEST, and
*>                     equal to it by the last test.  It will be less
*>                     if any of the routines being tested indicates
*>                     that it could not compute the matrices that
*>                     would be tested.
*>     NMAX            Largest value in NN.
*>     NMATS           The number of matrices generated so far.
*>     NERRS           The number of tests which have exceeded THRESH
*>                     so far (computed by DLAFTS).
*>     COND, CONDS,
*>     IMODE           Values to be passed to the matrix generators.
*>     ANORM           Norm of A; passed to matrix generators.
*>
*>     OVFL, UNFL      Overflow and underflow thresholds.
*>     ULP, ULPINV     Finest relative precision and its inverse.
*>     RTOVFL, RTUNFL,
*>     RTULP, RTULPI   Square roots of the previous 4 values.
*>
*>             The following four arrays decode JTYPE:
*>     KTYPE(j)        The general type (1-10) for type "j".
*>     KMODE(j)        The MODE value to be passed to the matrix
*>                     generator for type "j".
*>     KMAGN(j)        The order of magnitude ( O(1),
*>                     O(overflow^(1/2) ), O(underflow^(1/2) )
*>     KCONDS(j)       Selects whether CONDS is to be 1 or
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
*> \date December 2016
*
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZCHKHS( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NOUNIT, A, LDA, H, T1, T2, U, LDU, Z, UZ, W1,
     $                   W3, EVECTL, EVECTR, EVECTY, EVECTX, UU, TAU,
     $                   WORK, NWORK, RWORK, IWORK, SELECT, RESULT,
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDU, NOUNIT, NSIZES, NTYPES, NWORK
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * ), SELECT( * )
      INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
      DOUBLE PRECISION   RESULT( 14 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), EVECTL( LDU, * ),
     $                   EVECTR( LDU, * ), EVECTX( LDU, * ),
     $                   EVECTY( LDU, * ), H( LDA, * ), T1( LDA, * ),
     $                   T2( LDA, * ), TAU( * ), U( LDU, * ),
     $                   UU( LDU, * ), UZ( LDU, * ), W1( * ), W3( * ),
     $                   WORK( * ), Z( LDU, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 21 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN, MATCH
      INTEGER            I, IHI, IINFO, ILO, IMODE, IN, ITYPE, J, JCOL,
     $                   JJ, JSIZE, JTYPE, K, MTYPES, N, N1, NERRS,
     $                   NMATS, NMAX, NTEST, NTESTT
      DOUBLE PRECISION   ANINV, ANORM, COND, CONDS, OVFL, RTOVFL, RTULP,
     $                   RTULPI, RTUNFL, TEMP1, TEMP2, ULP, ULPINV, UNFL
*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), KCONDS( MAXTYP ),
     $                   KMAGN( MAXTYP ), KMODE( MAXTYP ),
     $                   KTYPE( MAXTYP )
      DOUBLE PRECISION   DUMMA( 4 )
      COMPLEX*16         CDUMMA( 4 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLAFTS, DLASUM, XERBLA, ZCOPY, ZGEHRD,
     $                   ZGEMM, ZGET10, ZGET22, ZHSEIN, ZHSEQR, ZHST01,
     $                   ZLACPY, ZLASET, ZLATME, ZLATMR, ZLATMS, ZTREVC,
     $                   ZUNGHR, ZUNMHR
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, MIN, SQRT
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
*     Check for errors
*
      NTESTT = 0
      INFO = 0
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
      ELSE IF( LDA.LE.1 .OR. LDA.LT.NMAX ) THEN
         INFO = -9
      ELSE IF( LDU.LE.1 .OR. LDU.LT.NMAX ) THEN
         INFO = -14
      ELSE IF( 4*NMAX*NMAX+2.GT.NWORK ) THEN
         INFO = -26
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZCHKHS', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   RETURN
*
*     More important constants
*
      UNFL = DLAMCH( 'Safe minimum' )
      OVFL = DLAMCH( 'Overflow' )
      CALL DLABAD( UNFL, OVFL )
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
      ULPINV = ONE / ULP
      RTUNFL = SQRT( UNFL )
      RTOVFL = SQRT( OVFL )
      RTULP = SQRT( ULP )
      RTULPI = ONE / RTULP
*
*     Loop over sizes, types
*
      NERRS = 0
      NMATS = 0
*
      DO 260 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         IF( N.EQ.0 )
     $      GO TO 260
         N1 = MAX( 1, N )
         ANINV = ONE / DBLE( N1 )
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 250 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 250
            NMATS = NMATS + 1
            NTEST = 0
*
*           Save ISEED in case of an error.
*
            DO 20 J = 1, 4
               IOLDSD( J ) = ISEED( J )
   20       CONTINUE
*
*           Initialize RESULT
*
            DO 30 J = 1, 14
               RESULT( J ) = ZERO
   30       CONTINUE
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
*       =5                 random log   hermitian, w/ eigenvalues
*       =6                 random       general, w/ eigenvalues
*       =7                              random diagonal
*       =8                              random hermitian
*       =9                              random general
*       =10                             random triangular
*
            IF( MTYPES.GT.MAXTYP )
     $         GO TO 100
*
            ITYPE = KTYPE( JTYPE )
            IMODE = KMODE( JTYPE )
*
*           Compute norm
*
            GO TO ( 40, 50, 60 )KMAGN( JTYPE )
*
   40       CONTINUE
            ANORM = ONE
            GO TO 70
*
   50       CONTINUE
            ANORM = ( RTOVFL*ULP )*ANINV
            GO TO 70
*
   60       CONTINUE
            ANORM = RTUNFL*N*ULPINV
            GO TO 70
*
   70       CONTINUE
*
            CALL ZLASET( 'Full', LDA, N, CZERO, CZERO, A, LDA )
            IINFO = 0
            COND = ULPINV
*
*           Special Matrices
*
            IF( ITYPE.EQ.1 ) THEN
*
*              Zero
*
               IINFO = 0
            ELSE IF( ITYPE.EQ.2 ) THEN
*
*              Identity
*
               DO 80 JCOL = 1, N
                  A( JCOL, JCOL ) = ANORM
   80          CONTINUE
*
            ELSE IF( ITYPE.EQ.3 ) THEN
*
*              Jordan Block
*
               DO 90 JCOL = 1, N
                  A( JCOL, JCOL ) = ANORM
                  IF( JCOL.GT.1 )
     $               A( JCOL, JCOL-1 ) = ONE
   90          CONTINUE
*
            ELSE IF( ITYPE.EQ.4 ) THEN
*
*              Diagonal Matrix, [Eigen]values Specified
*
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, IMODE, COND,
     $                      CONE, 'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.5 ) THEN
*
*              Hermitian, eigenvalues specified
*
               CALL ZLATMS( N, N, 'D', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, N, N, 'N', A, LDA, WORK, IINFO )
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
     $                      A, LDA, WORK( N+1 ), IINFO )
*
            ELSE IF( ITYPE.EQ.7 ) THEN
*
*              Diagonal, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.8 ) THEN
*
*              Hermitian, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'H', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.9 ) THEN
*
*              General, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.10 ) THEN
*
*              Triangular, random eigenvalues
*
               CALL ZLATMR( N, N, 'D', ISEED, 'N', WORK, 6, ONE, CONE,
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
               WRITE( NOUNIT, FMT = 9999 )'Generator', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               RETURN
            END IF
*
  100       CONTINUE
*
*           Call ZGEHRD to compute H and U, do tests.
*
            CALL ZLACPY( ' ', N, N, A, LDA, H, LDA )
            NTEST = 1
*
            ILO = 1
            IHI = N
*
            CALL ZGEHRD( N, ILO, IHI, H, LDA, WORK, WORK( N+1 ),
     $                   NWORK-N, IINFO )
*
            IF( IINFO.NE.0 ) THEN
               RESULT( 1 ) = ULPINV
               WRITE( NOUNIT, FMT = 9999 )'ZGEHRD', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
            DO 120 J = 1, N - 1
               UU( J+1, J ) = CZERO
               DO 110 I = J + 2, N
                  U( I, J ) = H( I, J )
                  UU( I, J ) = H( I, J )
                  H( I, J ) = CZERO
  110          CONTINUE
  120       CONTINUE
            CALL ZCOPY( N-1, WORK, 1, TAU, 1 )
            CALL ZUNGHR( N, ILO, IHI, U, LDU, WORK, WORK( N+1 ),
     $                   NWORK-N, IINFO )
            NTEST = 2
*
            CALL ZHST01( N, ILO, IHI, A, LDA, H, LDA, U, LDU, WORK,
     $                   NWORK, RWORK, RESULT( 1 ) )
*
*           Call ZHSEQR to compute T1, T2 and Z, do tests.
*
*           Eigenvalues only (W3)
*
            CALL ZLACPY( ' ', N, N, H, LDA, T2, LDA )
            NTEST = 3
            RESULT( 3 ) = ULPINV
*
            CALL ZHSEQR( 'E', 'N', N, ILO, IHI, T2, LDA, W3, UZ, LDU,
     $                   WORK, NWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHSEQR(E)', IINFO, N, JTYPE,
     $            IOLDSD
               IF( IINFO.LE.N+2 ) THEN
                  INFO = ABS( IINFO )
                  GO TO 240
               END IF
            END IF
*
*           Eigenvalues (W1) and Full Schur Form (T2)
*
            CALL ZLACPY( ' ', N, N, H, LDA, T2, LDA )
*
            CALL ZHSEQR( 'S', 'N', N, ILO, IHI, T2, LDA, W1, UZ, LDU,
     $                   WORK, NWORK, IINFO )
            IF( IINFO.NE.0 .AND. IINFO.LE.N+2 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHSEQR(S)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
*           Eigenvalues (W1), Schur Form (T1), and Schur Vectors (UZ)
*
            CALL ZLACPY( ' ', N, N, H, LDA, T1, LDA )
            CALL ZLACPY( ' ', N, N, U, LDU, UZ, LDU )
*
            CALL ZHSEQR( 'S', 'V', N, ILO, IHI, T1, LDA, W1, UZ, LDU,
     $                   WORK, NWORK, IINFO )
            IF( IINFO.NE.0 .AND. IINFO.LE.N+2 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHSEQR(V)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
*           Compute Z = U' UZ
*
            CALL ZGEMM( 'C', 'N', N, N, N, CONE, U, LDU, UZ, LDU, CZERO,
     $                  Z, LDU )
            NTEST = 8
*
*           Do Tests 3: | H - Z T Z' | / ( |H| n ulp )
*                and 4: | I - Z Z' | / ( n ulp )
*
            CALL ZHST01( N, ILO, IHI, H, LDA, T1, LDA, Z, LDU, WORK,
     $                   NWORK, RWORK, RESULT( 3 ) )
*
*           Do Tests 5: | A - UZ T (UZ)' | / ( |A| n ulp )
*                and 6: | I - UZ (UZ)' | / ( n ulp )
*
            CALL ZHST01( N, ILO, IHI, A, LDA, T1, LDA, UZ, LDU, WORK,
     $                   NWORK, RWORK, RESULT( 5 ) )
*
*           Do Test 7: | T2 - T1 | / ( |T| n ulp )
*
            CALL ZGET10( N, N, T2, LDA, T1, LDA, WORK, RWORK,
     $                   RESULT( 7 ) )
*
*           Do Test 8: | W3 - W1 | / ( max(|W1|,|W3|) ulp )
*
            TEMP1 = ZERO
            TEMP2 = ZERO
            DO 130 J = 1, N
               TEMP1 = MAX( TEMP1, ABS( W1( J ) ), ABS( W3( J ) ) )
               TEMP2 = MAX( TEMP2, ABS( W1( J )-W3( J ) ) )
  130       CONTINUE
*
            RESULT( 8 ) = TEMP2 / MAX( UNFL, ULP*MAX( TEMP1, TEMP2 ) )
*
*           Compute the Left and Right Eigenvectors of T
*
*           Compute the Right eigenvector Matrix:
*
            NTEST = 9
            RESULT( 9 ) = ULPINV
*
*           Select every other eigenvector
*
            DO 140 J = 1, N
               SELECT( J ) = .FALSE.
  140       CONTINUE
            DO 150 J = 1, N, 2
               SELECT( J ) = .TRUE.
  150       CONTINUE
            CALL ZTREVC( 'Right', 'All', SELECT, N, T1, LDA, CDUMMA,
     $                   LDU, EVECTR, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTREVC(R,A)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
*           Test 9:  | TR - RW | / ( |T| |R| ulp )
*
            CALL ZGET22( 'N', 'N', 'N', N, T1, LDA, EVECTR, LDU, W1,
     $                   WORK, RWORK, DUMMA( 1 ) )
            RESULT( 9 ) = DUMMA( 1 )
            IF( DUMMA( 2 ).GT.THRESH ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Right', 'ZTREVC',
     $            DUMMA( 2 ), N, JTYPE, IOLDSD
            END IF
*
*           Compute selected right eigenvectors and confirm that
*           they agree with previous right eigenvectors
*
            CALL ZTREVC( 'Right', 'Some', SELECT, N, T1, LDA, CDUMMA,
     $                   LDU, EVECTL, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTREVC(R,S)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
            K = 1
            MATCH = .TRUE.
            DO 170 J = 1, N
               IF( SELECT( J ) ) THEN
                  DO 160 JJ = 1, N
                     IF( EVECTR( JJ, J ).NE.EVECTL( JJ, K ) ) THEN
                        MATCH = .FALSE.
                        GO TO 180
                     END IF
  160             CONTINUE
                  K = K + 1
               END IF
  170       CONTINUE
  180       CONTINUE
            IF( .NOT.MATCH )
     $         WRITE( NOUNIT, FMT = 9997 )'Right', 'ZTREVC', N, JTYPE,
     $         IOLDSD
*
*           Compute the Left eigenvector Matrix:
*
            NTEST = 10
            RESULT( 10 ) = ULPINV
            CALL ZTREVC( 'Left', 'All', SELECT, N, T1, LDA, EVECTL, LDU,
     $                   CDUMMA, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTREVC(L,A)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
*           Test 10:  | LT - WL | / ( |T| |L| ulp )
*
            CALL ZGET22( 'C', 'N', 'C', N, T1, LDA, EVECTL, LDU, W1,
     $                   WORK, RWORK, DUMMA( 3 ) )
            RESULT( 10 ) = DUMMA( 3 )
            IF( DUMMA( 4 ).GT.THRESH ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Left', 'ZTREVC', DUMMA( 4 ),
     $            N, JTYPE, IOLDSD
            END IF
*
*           Compute selected left eigenvectors and confirm that
*           they agree with previous left eigenvectors
*
            CALL ZTREVC( 'Left', 'Some', SELECT, N, T1, LDA, EVECTR,
     $                   LDU, CDUMMA, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTREVC(L,S)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 240
            END IF
*
            K = 1
            MATCH = .TRUE.
            DO 200 J = 1, N
               IF( SELECT( J ) ) THEN
                  DO 190 JJ = 1, N
                     IF( EVECTL( JJ, J ).NE.EVECTR( JJ, K ) ) THEN
                        MATCH = .FALSE.
                        GO TO 210
                     END IF
  190             CONTINUE
                  K = K + 1
               END IF
  200       CONTINUE
  210       CONTINUE
            IF( .NOT.MATCH )
     $         WRITE( NOUNIT, FMT = 9997 )'Left', 'ZTREVC', N, JTYPE,
     $         IOLDSD
*
*           Call ZHSEIN for Right eigenvectors of H, do test 11
*
            NTEST = 11
            RESULT( 11 ) = ULPINV
            DO 220 J = 1, N
               SELECT( J ) = .TRUE.
  220       CONTINUE
*
            CALL ZHSEIN( 'Right', 'Qr', 'Ninitv', SELECT, N, H, LDA, W3,
     $                   CDUMMA, LDU, EVECTX, LDU, N1, IN, WORK, RWORK,
     $                   IWORK, IWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHSEIN(R)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 )
     $            GO TO 240
            ELSE
*
*              Test 11:  | HX - XW | / ( |H| |X| ulp )
*
*                        (from inverse iteration)
*
               CALL ZGET22( 'N', 'N', 'N', N, H, LDA, EVECTX, LDU, W3,
     $                      WORK, RWORK, DUMMA( 1 ) )
               IF( DUMMA( 1 ).LT.ULPINV )
     $            RESULT( 11 ) = DUMMA( 1 )*ANINV
               IF( DUMMA( 2 ).GT.THRESH ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'Right', 'ZHSEIN',
     $               DUMMA( 2 ), N, JTYPE, IOLDSD
               END IF
            END IF
*
*           Call ZHSEIN for Left eigenvectors of H, do test 12
*
            NTEST = 12
            RESULT( 12 ) = ULPINV
            DO 230 J = 1, N
               SELECT( J ) = .TRUE.
  230       CONTINUE
*
            CALL ZHSEIN( 'Left', 'Qr', 'Ninitv', SELECT, N, H, LDA, W3,
     $                   EVECTY, LDU, CDUMMA, LDU, N1, IN, WORK, RWORK,
     $                   IWORK, IWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHSEIN(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 )
     $            GO TO 240
            ELSE
*
*              Test 12:  | YH - WY | / ( |H| |Y| ulp )
*
*                        (from inverse iteration)
*
               CALL ZGET22( 'C', 'N', 'C', N, H, LDA, EVECTY, LDU, W3,
     $                      WORK, RWORK, DUMMA( 3 ) )
               IF( DUMMA( 3 ).LT.ULPINV )
     $            RESULT( 12 ) = DUMMA( 3 )*ANINV
               IF( DUMMA( 4 ).GT.THRESH ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'Left', 'ZHSEIN',
     $               DUMMA( 4 ), N, JTYPE, IOLDSD
               END IF
            END IF
*
*           Call ZUNMHR for Right eigenvectors of A, do test 13
*
            NTEST = 13
            RESULT( 13 ) = ULPINV
*
            CALL ZUNMHR( 'Left', 'No transpose', N, N, ILO, IHI, UU,
     $                   LDU, TAU, EVECTX, LDU, WORK, NWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUNMHR(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 )
     $            GO TO 240
            ELSE
*
*              Test 13:  | AX - XW | / ( |A| |X| ulp )
*
*                        (from inverse iteration)
*
               CALL ZGET22( 'N', 'N', 'N', N, A, LDA, EVECTX, LDU, W3,
     $                      WORK, RWORK, DUMMA( 1 ) )
               IF( DUMMA( 1 ).LT.ULPINV )
     $            RESULT( 13 ) = DUMMA( 1 )*ANINV
            END IF
*
*           Call ZUNMHR for Left eigenvectors of A, do test 14
*
            NTEST = 14
            RESULT( 14 ) = ULPINV
*
            CALL ZUNMHR( 'Left', 'No transpose', N, N, ILO, IHI, UU,
     $                   LDU, TAU, EVECTY, LDU, WORK, NWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUNMHR(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 )
     $            GO TO 240
            ELSE
*
*              Test 14:  | YA - WY | / ( |A| |Y| ulp )
*
*                        (from inverse iteration)
*
               CALL ZGET22( 'C', 'N', 'C', N, A, LDA, EVECTY, LDU, W3,
     $                      WORK, RWORK, DUMMA( 3 ) )
               IF( DUMMA( 3 ).LT.ULPINV )
     $            RESULT( 14 ) = DUMMA( 3 )*ANINV
            END IF
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
  240       CONTINUE
*
            NTESTT = NTESTT + NTEST
            CALL DLAFTS( 'ZHS', N, N, JTYPE, NTEST, RESULT, IOLDSD,
     $                   THRESH, NOUNIT, NERRS )
*
  250    CONTINUE
  260 CONTINUE
*
*     Summary
*
      CALL DLASUM( 'ZHS', NOUNIT, NERRS, NTESTT )
*
      RETURN
*
 9999 FORMAT( ' ZCHKHS: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
 9998 FORMAT( ' ZCHKHS: ', A, ' Eigenvectors from ', A, ' incorrectly ',
     $      'normalized.', / ' Bits of error=', 0P, G10.3, ',', 9X,
     $      'N=', I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5,
     $      ')' )
 9997 FORMAT( ' ZCHKHS: Selected ', A, ' Eigenvectors from ', A,
     $      ' do not match other eigenvectors ', 9X, 'N=', I6,
     $      ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
*     End of ZCHKHS
*
      END
