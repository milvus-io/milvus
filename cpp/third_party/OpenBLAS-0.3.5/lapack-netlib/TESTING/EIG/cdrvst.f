*> \brief \b CDRVST
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CDRVST( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, D1, D2, D3, WA1, WA2, WA3, U,
*                          LDU, V, TAU, Z, WORK, LWORK, RWORK, LRWORK,
*                          IWORK, LIWORK, RESULT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDU, LIWORK, LRWORK, LWORK, NOUNIT,
*      $                   NSIZES, NTYPES
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
*       REAL               D1( * ), D2( * ), D3( * ), RESULT( * ),
*      $                   RWORK( * ), WA1( * ), WA2( * ), WA3( * )
*       COMPLEX            A( LDA, * ), TAU( * ), U( LDU, * ),
*      $                   V( LDU, * ), WORK( * ), Z( LDU, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      CDRVST  checks the Hermitian eigenvalue problem drivers.
*>
*>              CHEEVD computes all eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix,
*>              using a divide-and-conquer algorithm.
*>
*>              CHEEVX computes selected eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix.
*>
*>              CHEEVR computes selected eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix
*>              using the Relatively Robust Representation where it can.
*>
*>              CHPEVD computes all eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix in packed
*>              storage, using a divide-and-conquer algorithm.
*>
*>              CHPEVX computes selected eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix in packed
*>              storage.
*>
*>              CHBEVD computes all eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian band matrix,
*>              using a divide-and-conquer algorithm.
*>
*>              CHBEVX computes selected eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian band matrix.
*>
*>              CHEEV computes all eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix.
*>
*>              CHPEV computes all eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian matrix in packed
*>              storage.
*>
*>              CHBEV computes all eigenvalues and, optionally,
*>              eigenvectors of a complex Hermitian band matrix.
*>
*>      When CDRVST is called, a number of matrix "sizes" ("n's") and a
*>      number of matrix "types" are specified.  For each size ("n")
*>      and each type of matrix, one matrix will be generated and used
*>      to test the appropriate drivers.  For each matrix and each
*>      driver routine called, the following tests will be performed:
*>
*>      (1)     | A - Z D Z' | / ( |A| n ulp )
*>
*>      (2)     | I - Z Z' | / ( n ulp )
*>
*>      (3)     | D1 - D2 | / ( |D1| ulp )
*>
*>      where Z is the matrix of eigenvectors returned when the
*>      eigenvector option is given and D1 and D2 are the eigenvalues
*>      returned with and without the eigenvector option.
*>
*>      The "sizes" are specified by an array NN(1:NSIZES); the value of
*>      each element NN(j) specifies one size.
*>      The "types" are specified by a logical array DOTYPE( 1:NTYPES );
*>      if DOTYPE(j) is .TRUE., then matrix type "j" will be generated.
*>      Currently, the list of possible types is:
*>
*>      (1)  The zero matrix.
*>      (2)  The identity matrix.
*>
*>      (3)  A diagonal matrix with evenly spaced entries
*>           1, ..., ULP  and random signs.
*>           (ULP = (first number larger than 1) - 1 )
*>      (4)  A diagonal matrix with geometrically spaced entries
*>           1, ..., ULP  and random signs.
*>      (5)  A diagonal matrix with "clustered" entries 1, ULP, ..., ULP
*>           and random signs.
*>
*>      (6)  Same as (4), but multiplied by SQRT( overflow threshold )
*>      (7)  Same as (4), but multiplied by SQRT( underflow threshold )
*>
*>      (8)  A matrix of the form  U* D U, where U is unitary and
*>           D has evenly spaced entries 1, ..., ULP with random signs
*>           on the diagonal.
*>
*>      (9)  A matrix of the form  U* D U, where U is unitary and
*>           D has geometrically spaced entries 1, ..., ULP with random
*>           signs on the diagonal.
*>
*>      (10) A matrix of the form  U* D U, where U is unitary and
*>           D has "clustered" entries 1, ULP,..., ULP with random
*>           signs on the diagonal.
*>
*>      (11) Same as (8), but multiplied by SQRT( overflow threshold )
*>      (12) Same as (8), but multiplied by SQRT( underflow threshold )
*>
*>      (13) Symmetric matrix with random entries chosen from (-1,1).
*>      (14) Same as (13), but multiplied by SQRT( overflow threshold )
*>      (15) Same as (13), but multiplied by SQRT( underflow threshold )
*>      (16) A band matrix with half bandwidth randomly chosen between
*>           0 and N-1, with evenly spaced eigenvalues 1, ..., ULP
*>           with random signs.
*>      (17) Same as (16), but multiplied by SQRT( overflow threshold )
*>      (18) Same as (16), but multiplied by SQRT( underflow threshold )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  NSIZES  INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          CDRVST does nothing.  It must be at least zero.
*>          Not modified.
*>
*>  NN      INTEGER array, dimension (NSIZES)
*>          An array containing the sizes to be used for the matrices.
*>          Zero values will be skipped.  The values must be at least
*>          zero.
*>          Not modified.
*>
*>  NTYPES  INTEGER
*>          The number of elements in DOTYPE.   If it is zero, CDRVST
*>          does nothing.  It must be at least zero.  If it is MAXTYP+1
*>          and NSIZES is 1, then an additional type, MAXTYP+1 is
*>          defined, which is to use whatever matrix is in A.  This
*>          is only useful if DOTYPE(1:MAXTYP) is .FALSE. and
*>          DOTYPE(MAXTYP+1) is .TRUE. .
*>          Not modified.
*>
*>  DOTYPE  LOGICAL array, dimension (NTYPES)
*>          If DOTYPE(j) is .TRUE., then for each size in NN a
*>          matrix of that size and of type j will be generated.
*>          If NTYPES is smaller than the maximum number of types
*>          defined (PARAMETER MAXTYP), then types NTYPES+1 through
*>          MAXTYP will not be generated.  If NTYPES is larger
*>          than MAXTYP, DOTYPE(MAXTYP+1) through DOTYPE(NTYPES)
*>          will be ignored.
*>          Not modified.
*>
*>  ISEED   INTEGER array, dimension (4)
*>          On entry ISEED specifies the seed of the random number
*>          generator. The array elements should be between 0 and 4095;
*>          if not they will be reduced mod 4096.  Also, ISEED(4) must
*>          be odd.  The random number generator uses a linear
*>          congruential sequence limited to small integers, and so
*>          should produce machine independent random numbers. The
*>          values of ISEED are changed on exit, and can be used in the
*>          next call to CDRVST to continue the same random number
*>          sequence.
*>          Modified.
*>
*>  THRESH  REAL
*>          A test will count as "failed" if the "error", computed as
*>          described above, exceeds THRESH.  Note that the error
*>          is scaled to be O(1), so THRESH should be a reasonably
*>          small multiple of 1, e.g., 10 or 100.  In particular,
*>          it should not depend on the precision (single vs. double)
*>          or the size of the matrix.  It must be at least zero.
*>          Not modified.
*>
*>  NOUNIT  INTEGER
*>          The FORTRAN unit number for printing out error messages
*>          (e.g., if a routine returns IINFO not equal to 0.)
*>          Not modified.
*>
*>  A       COMPLEX array, dimension (LDA , max(NN))
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually
*>          used.
*>          Modified.
*>
*>  LDA     INTEGER
*>          The leading dimension of A.  It must be at
*>          least 1 and at least max( NN ).
*>          Not modified.
*>
*>  D1      REAL array, dimension (max(NN))
*>          The eigenvalues of A, as computed by CSTEQR simlutaneously
*>          with Z.  On exit, the eigenvalues in D1 correspond with the
*>          matrix in A.
*>          Modified.
*>
*>  D2      REAL array, dimension (max(NN))
*>          The eigenvalues of A, as computed by CSTEQR if Z is not
*>          computed.  On exit, the eigenvalues in D2 correspond with
*>          the matrix in A.
*>          Modified.
*>
*>  D3      REAL array, dimension (max(NN))
*>          The eigenvalues of A, as computed by SSTERF.  On exit, the
*>          eigenvalues in D3 correspond with the matrix in A.
*>          Modified.
*>
*>  WA1     REAL array, dimension
*>
*>  WA2     REAL array, dimension
*>
*>  WA3     REAL array, dimension
*>
*>  U       COMPLEX array, dimension (LDU, max(NN))
*>          The unitary matrix computed by CHETRD + CUNGC3.
*>          Modified.
*>
*>  LDU     INTEGER
*>          The leading dimension of U, Z, and V.  It must be at
*>          least 1 and at least max( NN ).
*>          Not modified.
*>
*>  V       COMPLEX array, dimension (LDU, max(NN))
*>          The Housholder vectors computed by CHETRD in reducing A to
*>          tridiagonal form.
*>          Modified.
*>
*>  TAU     COMPLEX array, dimension (max(NN))
*>          The Householder factors computed by CHETRD in reducing A
*>          to tridiagonal form.
*>          Modified.
*>
*>  Z       COMPLEX array, dimension (LDU, max(NN))
*>          The unitary matrix of eigenvectors computed by CHEEVD,
*>          CHEEVX, CHPEVD, CHPEVX, CHBEVD, and CHBEVX.
*>          Modified.
*>
*>  WORK  - COMPLEX array of dimension ( LWORK )
*>           Workspace.
*>           Modified.
*>
*>  LWORK - INTEGER
*>           The number of entries in WORK.  This must be at least
*>           2*max( NN(j), 2 )**2.
*>           Not modified.
*>
*>  RWORK   REAL array, dimension (3*max(NN))
*>           Workspace.
*>           Modified.
*>
*>  LRWORK - INTEGER
*>           The number of entries in RWORK.
*>
*>  IWORK   INTEGER array, dimension (6*max(NN))
*>          Workspace.
*>          Modified.
*>
*>  LIWORK - INTEGER
*>           The number of entries in IWORK.
*>
*>  RESULT  REAL array, dimension (??)
*>          The values computed by the tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*>          Modified.
*>
*>  INFO    INTEGER
*>          If 0, then everything ran OK.
*>           -1: NSIZES < 0
*>           -2: Some NN(j) < 0
*>           -3: NTYPES < 0
*>           -5: THRESH < 0
*>           -9: LDA < 1 or LDA < NMAX, where NMAX is max( NN(j) ).
*>          -16: LDU < 1 or LDU < NMAX.
*>          -21: LWORK too small.
*>          If  SLATMR, SLATMS, CHETRD, SORGC3, CSTEQR, SSTERF,
*>              or SORMC2 returns an error code, the
*>              absolute value of it is returned.
*>          Modified.
*>
*>-----------------------------------------------------------------------
*>
*>       Some Local Variables and Parameters:
*>       ---- ----- --------- --- ----------
*>       ZERO, ONE       Real 0 and 1.
*>       MAXTYP          The number of types defined.
*>       NTEST           The number of tests performed, or which can
*>                       be performed so far, for the current matrix.
*>       NTESTT          The total number of tests performed so far.
*>       NMAX            Largest value in NN.
*>       NMATS           The number of matrices generated so far.
*>       NERRS           The number of tests which have exceeded THRESH
*>                       so far (computed by SLAFTS).
*>       COND, IMODE     Values to be passed to the matrix generators.
*>       ANORM           Norm of A; passed to matrix generators.
*>
*>       OVFL, UNFL      Overflow and underflow thresholds.
*>       ULP, ULPINV     Finest relative precision and its inverse.
*>       RTOVFL, RTUNFL  Square roots of the previous 2 values.
*>               The following four arrays decode JTYPE:
*>       KTYPE(j)        The general type (1-10) for type "j".
*>       KMODE(j)        The MODE value to be passed to the matrix
*>                       generator for type "j".
*>       KMAGN(j)        The order of magnitude ( O(1),
*>                       O(overflow^(1/2) ), O(underflow^(1/2) )
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
      SUBROUTINE CDRVST( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NOUNIT, A, LDA, D1, D2, D3, WA1, WA2, WA3, U,
     $                   LDU, V, TAU, Z, WORK, LWORK, RWORK, LRWORK,
     $                   IWORK, LIWORK, RESULT, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDU, LIWORK, LRWORK, LWORK, NOUNIT,
     $                   NSIZES, NTYPES
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
      REAL               D1( * ), D2( * ), D3( * ), RESULT( * ),
     $                   RWORK( * ), WA1( * ), WA2( * ), WA3( * )
      COMPLEX            A( LDA, * ), TAU( * ), U( LDU, * ),
     $                   V( LDU, * ), WORK( * ), Z( LDU, * )
*     ..
*
*  =====================================================================
*
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO, TEN
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0,
     $                   TEN = 10.0E+0 )
      REAL               HALF
      PARAMETER          ( HALF = ONE / TWO )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 18 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN
      CHARACTER          UPLO
      INTEGER            I, IDIAG, IHBW, IINFO, IL, IMODE, INDWRK, INDX,
     $                   IROW, ITEMP, ITYPE, IU, IUPLO, J, J1, J2, JCOL,
     $                   JSIZE, JTYPE, KD, LGN, LIWEDC, LRWEDC, LWEDC,
     $                   M, M2, M3, MTYPES, N, NERRS, NMATS, NMAX,
     $                   NTEST, NTESTT
      REAL               ABSTOL, ANINV, ANORM, COND, OVFL, RTOVFL,
     $                   RTUNFL, TEMP1, TEMP2, TEMP3, ULP, ULPINV, UNFL,
     $                   VL, VU
*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), ISEED2( 4 ),
     $                   ISEED3( 4 ), KMAGN( MAXTYP ), KMODE( MAXTYP ),
     $                   KTYPE( MAXTYP )
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLARND, SSXT1
      EXTERNAL           SLAMCH, SLARND, SSXT1
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALASVM, CHBEV, CHBEVD, CHBEVX, CHEEV, CHEEVD,
     $                   CHEEVR, CHEEVX, CHET21, CHET22, CHPEV, CHPEVD,
     $                   CHPEVX, CLACPY, CLASET, CLATMR, CLATMS, SLABAD,
     $                   SLAFTS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, INT, LOG, MAX, MIN, REAL, SQRT
*     ..
*     .. Data statements ..
      DATA               KTYPE / 1, 2, 5*4, 5*5, 3*8, 3*9 /
      DATA               KMAGN / 2*1, 1, 1, 1, 2, 3, 1, 1, 1, 2, 3, 1,
     $                   2, 3, 1, 2, 3 /
      DATA               KMODE / 2*0, 4, 3, 1, 4, 4, 4, 3, 1, 4, 4, 0,
     $                   0, 0, 4, 4, 4 /
*     ..
*     .. Executable Statements ..
*
*     1)      Check for errors
*
      NTESTT = 0
      INFO = 0
*
      BADNN = .FALSE.
      NMAX = 1
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
      ELSE IF( LDA.LT.NMAX ) THEN
         INFO = -9
      ELSE IF( LDU.LT.NMAX ) THEN
         INFO = -16
      ELSE IF( 2*MAX( 2, NMAX )**2.GT.LWORK ) THEN
         INFO = -22
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CDRVST', -INFO )
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
      UNFL = SLAMCH( 'Safe minimum' )
      OVFL = SLAMCH( 'Overflow' )
      CALL SLABAD( UNFL, OVFL )
      ULP = SLAMCH( 'Epsilon' )*SLAMCH( 'Base' )
      ULPINV = ONE / ULP
      RTUNFL = SQRT( UNFL )
      RTOVFL = SQRT( OVFL )
*
*     Loop over sizes, types
*
      DO 20 I = 1, 4
         ISEED2( I ) = ISEED( I )
         ISEED3( I ) = ISEED( I )
   20 CONTINUE
*
      NERRS = 0
      NMATS = 0
*
      DO 1220 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         IF( N.GT.0 ) THEN
            LGN = INT( LOG( REAL( N ) ) / LOG( TWO ) )
            IF( 2**LGN.LT.N )
     $         LGN = LGN + 1
            IF( 2**LGN.LT.N )
     $         LGN = LGN + 1
            LWEDC = MAX( 2*N+N*N, 2*N*N )
            LRWEDC = 1 + 4*N + 2*N*LGN + 3*N**2
            LIWEDC = 3 + 5*N
         ELSE
            LWEDC = 2
            LRWEDC = 8
            LIWEDC = 8
         END IF
         ANINV = ONE / REAL( MAX( 1, N ) )
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 1210 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 1210
            NMATS = NMATS + 1
            NTEST = 0
*
            DO 30 J = 1, 4
               IOLDSD( J ) = ISEED( J )
   30       CONTINUE
*
*           2)      Compute "A"
*
*                   Control parameters:
*
*               KMAGN  KMODE        KTYPE
*           =1  O(1)   clustered 1  zero
*           =2  large  clustered 2  identity
*           =3  small  exponential  (none)
*           =4         arithmetic   diagonal, (w/ eigenvalues)
*           =5         random log   Hermitian, w/ eigenvalues
*           =6         random       (none)
*           =7                      random diagonal
*           =8                      random Hermitian
*           =9                      band Hermitian, w/ eigenvalues
*
            IF( MTYPES.GT.MAXTYP )
     $         GO TO 110
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
            CALL CLASET( 'Full', LDA, N, CZERO, CZERO, A, LDA )
            IINFO = 0
            COND = ULPINV
*
*           Special Matrices -- Identity & Jordan block
*
*                   Zero
*
            IF( ITYPE.EQ.1 ) THEN
               IINFO = 0
*
            ELSE IF( ITYPE.EQ.2 ) THEN
*
*              Identity
*
               DO 80 JCOL = 1, N
                  A( JCOL, JCOL ) = ANORM
   80          CONTINUE
*
            ELSE IF( ITYPE.EQ.4 ) THEN
*
*              Diagonal Matrix, [Eigen]values Specified
*
               CALL CLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, 0, 0, 'N', A, LDA, WORK, IINFO )
*
            ELSE IF( ITYPE.EQ.5 ) THEN
*
*              Hermitian, eigenvalues specified
*
               CALL CLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, N, N, 'N', A, LDA, WORK, IINFO )
*
            ELSE IF( ITYPE.EQ.7 ) THEN
*
*              Diagonal, random eigenvalues
*
               CALL CLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.8 ) THEN
*
*              Hermitian, random eigenvalues
*
               CALL CLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.9 ) THEN
*
*              Hermitian banded, eigenvalues specified
*
               IHBW = INT( ( N-1 )*SLARND( 1, ISEED3 ) )
               CALL CLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, IHBW, IHBW, 'Z', U, LDU, WORK,
     $                      IINFO )
*
*              Store as dense matrix for most routines.
*
               CALL CLASET( 'Full', LDA, N, CZERO, CZERO, A, LDA )
               DO 100 IDIAG = -IHBW, IHBW
                  IROW = IHBW - IDIAG + 1
                  J1 = MAX( 1, IDIAG+1 )
                  J2 = MIN( N, N+IDIAG )
                  DO 90 J = J1, J2
                     I = J - IDIAG
                     A( I, J ) = U( IROW, J )
   90             CONTINUE
  100          CONTINUE
            ELSE
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
  110       CONTINUE
*
            ABSTOL = UNFL + UNFL
            IF( N.LE.1 ) THEN
               IL = 1
               IU = N
            ELSE
               IL = 1 + INT( ( N-1 )*SLARND( 1, ISEED2 ) )
               IU = 1 + INT( ( N-1 )*SLARND( 1, ISEED2 ) )
               IF( IL.GT.IU ) THEN
                  ITEMP = IL
                  IL = IU
                  IU = ITEMP
               END IF
            END IF
*
*           Perform tests storing upper or lower triangular
*           part of matrix.
*
            DO 1200 IUPLO = 0, 1
               IF( IUPLO.EQ.0 ) THEN
                  UPLO = 'L'
               ELSE
                  UPLO = 'U'
               END IF
*
*              Call CHEEVD and CHEEVX.
*
               CALL CLACPY( ' ', N, N, A, LDA, V, LDU )
*
               NTEST = NTEST + 1
               CALL CHEEVD( 'V', UPLO, N, A, LDU, D1, WORK, LWEDC,
     $                      RWORK, LRWEDC, IWORK, LIWEDC, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVD(V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 130
                  END IF
               END IF
*
*              Do tests 1 and 2.
*
               CALL CHET21( 1, UPLO, N, 0, V, LDU, D1, D2, A, LDU, Z,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               NTEST = NTEST + 2
               CALL CHEEVD( 'N', UPLO, N, A, LDU, D3, WORK, LWEDC,
     $                      RWORK, LRWEDC, IWORK, LIWEDC, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVD(N,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 130
                  END IF
               END IF
*
*              Do test 3.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 120 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D3( J ) ) )
  120          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
  130          CONTINUE
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               NTEST = NTEST + 1
*
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( D1( 1 ) ), ABS( D1( N ) ) )
                  IF( IL.NE.1 ) THEN
                     VL = D1( IL ) - MAX( HALF*( D1( IL )-D1( IL-1 ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  ELSE IF( N.GT.0 ) THEN
                     VL = D1( 1 ) - MAX( HALF*( D1( N )-D1( 1 ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  END IF
                  IF( IU.NE.N ) THEN
                     VU = D1( IU ) + MAX( HALF*( D1( IU+1 )-D1( IU ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  ELSE IF( N.GT.0 ) THEN
                     VU = D1( N ) + MAX( HALF*( D1( N )-D1( 1 ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  END IF
               ELSE
                  TEMP3 = ZERO
                  VL = ZERO
                  VU = ONE
               END IF
*
               CALL CHEEVX( 'V', 'A', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M, WA1, Z, LDU, WORK, LWORK, RWORK,
     $                      IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVX(V,A,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 150
                  END IF
               END IF
*
*              Do tests 4 and 5.
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               CALL CHET21( 1, UPLO, N, 0, A, LDU, WA1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
               CALL CHEEVX( 'N', 'A', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, WORK, LWORK, RWORK,
     $                      IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVX(N,A,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 150
                  END IF
               END IF
*
*              Do test 6.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 140 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( WA1( J ) ), ABS( WA2( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( WA1( J )-WA2( J ) ) )
  140          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
  150          CONTINUE
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               NTEST = NTEST + 1
*
               CALL CHEEVX( 'V', 'I', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, WORK, LWORK, RWORK,
     $                      IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVX(V,I,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 160
                  END IF
               END IF
*
*              Do tests 7 and 8.
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               CALL CHEEVX( 'N', 'I', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M3, WA3, Z, LDU, WORK, LWORK, RWORK,
     $                      IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVX(N,I,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 160
                  END IF
               END IF
*
*              Do test 9.
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
  160          CONTINUE
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               NTEST = NTEST + 1
*
               CALL CHEEVX( 'V', 'V', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, WORK, LWORK, RWORK,
     $                      IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVX(V,V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 170
                  END IF
               END IF
*
*              Do tests 10 and 11.
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               CALL CHEEVX( 'N', 'V', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M3, WA3, Z, LDU, WORK, LWORK, RWORK,
     $                      IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVX(N,V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 170
                  END IF
               END IF
*
               IF( M3.EQ.0 .AND. N.GT.0 ) THEN
                  RESULT( NTEST ) = ULPINV
                  GO TO 170
               END IF
*
*              Do test 12.
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
  170          CONTINUE
*
*              Call CHPEVD and CHPEVX.
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
*              Load array WORK with the upper or lower triangular
*              part of the matrix in packed form.
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 190 J = 1, N
                     DO 180 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  180                CONTINUE
  190             CONTINUE
               ELSE
                  INDX = 1
                  DO 210 J = 1, N
                     DO 200 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  200                CONTINUE
  210             CONTINUE
               END IF
*
               NTEST = NTEST + 1
               INDWRK = N*( N+1 ) / 2 + 1
               CALL CHPEVD( 'V', UPLO, N, WORK, D1, Z, LDU,
     $                      WORK( INDWRK ), LWEDC, RWORK, LRWEDC, IWORK,
     $                      LIWEDC, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVD(V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 270
                  END IF
               END IF
*
*              Do tests 13 and 14.
*
               CALL CHET21( 1, UPLO, N, 0, A, LDA, D1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 230 J = 1, N
                     DO 220 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  220                CONTINUE
  230             CONTINUE
               ELSE
                  INDX = 1
                  DO 250 J = 1, N
                     DO 240 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  240                CONTINUE
  250             CONTINUE
               END IF
*
               NTEST = NTEST + 2
               INDWRK = N*( N+1 ) / 2 + 1
               CALL CHPEVD( 'N', UPLO, N, WORK, D3, Z, LDU,
     $                      WORK( INDWRK ), LWEDC, RWORK, LRWEDC, IWORK,
     $                      LIWEDC, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVD(N,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 270
                  END IF
               END IF
*
*              Do test 15.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 260 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D3( J ) ) )
  260          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
*              Load array WORK with the upper or lower triangular part
*              of the matrix in packed form.
*
  270          CONTINUE
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 290 J = 1, N
                     DO 280 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  280                CONTINUE
  290             CONTINUE
               ELSE
                  INDX = 1
                  DO 310 J = 1, N
                     DO 300 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  300                CONTINUE
  310             CONTINUE
               END IF
*
               NTEST = NTEST + 1
*
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( D1( 1 ) ), ABS( D1( N ) ) )
                  IF( IL.NE.1 ) THEN
                     VL = D1( IL ) - MAX( HALF*( D1( IL )-D1( IL-1 ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  ELSE IF( N.GT.0 ) THEN
                     VL = D1( 1 ) - MAX( HALF*( D1( N )-D1( 1 ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  END IF
                  IF( IU.NE.N ) THEN
                     VU = D1( IU ) + MAX( HALF*( D1( IU+1 )-D1( IU ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  ELSE IF( N.GT.0 ) THEN
                     VU = D1( N ) + MAX( HALF*( D1( N )-D1( 1 ) ),
     $                    TEN*ULP*TEMP3, TEN*RTUNFL )
                  END IF
               ELSE
                  TEMP3 = ZERO
                  VL = ZERO
                  VU = ONE
               END IF
*
               CALL CHPEVX( 'V', 'A', UPLO, N, WORK, VL, VU, IL, IU,
     $                      ABSTOL, M, WA1, Z, LDU, V, RWORK, IWORK,
     $                      IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVX(V,A,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 370
                  END IF
               END IF
*
*              Do tests 16 and 17.
*
               CALL CHET21( 1, UPLO, N, 0, A, LDU, WA1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 330 J = 1, N
                     DO 320 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  320                CONTINUE
  330             CONTINUE
               ELSE
                  INDX = 1
                  DO 350 J = 1, N
                     DO 340 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  340                CONTINUE
  350             CONTINUE
               END IF
*
               CALL CHPEVX( 'N', 'A', UPLO, N, WORK, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, V, RWORK, IWORK,
     $                      IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVX(N,A,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 370
                  END IF
               END IF
*
*              Do test 18.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 360 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( WA1( J ) ), ABS( WA2( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( WA1( J )-WA2( J ) ) )
  360          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
  370          CONTINUE
               NTEST = NTEST + 1
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 390 J = 1, N
                     DO 380 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  380                CONTINUE
  390             CONTINUE
               ELSE
                  INDX = 1
                  DO 410 J = 1, N
                     DO 400 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  400                CONTINUE
  410             CONTINUE
               END IF
*
               CALL CHPEVX( 'V', 'I', UPLO, N, WORK, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, V, RWORK, IWORK,
     $                      IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVX(V,I,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 460
                  END IF
               END IF
*
*              Do tests 19 and 20.
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 430 J = 1, N
                     DO 420 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  420                CONTINUE
  430             CONTINUE
               ELSE
                  INDX = 1
                  DO 450 J = 1, N
                     DO 440 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  440                CONTINUE
  450             CONTINUE
               END IF
*
               CALL CHPEVX( 'N', 'I', UPLO, N, WORK, VL, VU, IL, IU,
     $                      ABSTOL, M3, WA3, Z, LDU, V, RWORK, IWORK,
     $                      IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVX(N,I,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 460
                  END IF
               END IF
*
*              Do test 21.
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
  460          CONTINUE
               NTEST = NTEST + 1
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 480 J = 1, N
                     DO 470 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  470                CONTINUE
  480             CONTINUE
               ELSE
                  INDX = 1
                  DO 500 J = 1, N
                     DO 490 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  490                CONTINUE
  500             CONTINUE
               END IF
*
               CALL CHPEVX( 'V', 'V', UPLO, N, WORK, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, V, RWORK, IWORK,
     $                      IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVX(V,V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 550
                  END IF
               END IF
*
*              Do tests 22 and 23.
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 520 J = 1, N
                     DO 510 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  510                CONTINUE
  520             CONTINUE
               ELSE
                  INDX = 1
                  DO 540 J = 1, N
                     DO 530 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  530                CONTINUE
  540             CONTINUE
               END IF
*
               CALL CHPEVX( 'N', 'V', UPLO, N, WORK, VL, VU, IL, IU,
     $                      ABSTOL, M3, WA3, Z, LDU, V, RWORK, IWORK,
     $                      IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEVX(N,V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 550
                  END IF
               END IF
*
               IF( M3.EQ.0 .AND. N.GT.0 ) THEN
                  RESULT( NTEST ) = ULPINV
                  GO TO 550
               END IF
*
*              Do test 24.
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
  550          CONTINUE
*
*              Call CHBEVD and CHBEVX.
*
               IF( JTYPE.LE.7 ) THEN
                  KD = 0
               ELSE IF( JTYPE.GE.8 .AND. JTYPE.LE.15 ) THEN
                  KD = MAX( N-1, 0 )
               ELSE
                  KD = IHBW
               END IF
*
*              Load array V with the upper or lower triangular part
*              of the matrix in band form.
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 570 J = 1, N
                     DO 560 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  560                CONTINUE
  570             CONTINUE
               ELSE
                  DO 590 J = 1, N
                     DO 580 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  580                CONTINUE
  590             CONTINUE
               END IF
*
               NTEST = NTEST + 1
               CALL CHBEVD( 'V', UPLO, N, KD, V, LDU, D1, Z, LDU, WORK,
     $                      LWEDC, RWORK, LRWEDC, IWORK, LIWEDC, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVD(V,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 650
                  END IF
               END IF
*
*              Do tests 25 and 26.
*
               CALL CHET21( 1, UPLO, N, 0, A, LDA, D1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 610 J = 1, N
                     DO 600 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  600                CONTINUE
  610             CONTINUE
               ELSE
                  DO 630 J = 1, N
                     DO 620 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  620                CONTINUE
  630             CONTINUE
               END IF
*
               NTEST = NTEST + 2
               CALL CHBEVD( 'N', UPLO, N, KD, V, LDU, D3, Z, LDU, WORK,
     $                      LWEDC, RWORK, LRWEDC, IWORK, LIWEDC, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVD(N,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 650
                  END IF
               END IF
*
*              Do test 27.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 640 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D3( J ) ) )
  640          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
*              Load array V with the upper or lower triangular part
*              of the matrix in band form.
*
  650          CONTINUE
               IF( IUPLO.EQ.1 ) THEN
                  DO 670 J = 1, N
                     DO 660 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  660                CONTINUE
  670             CONTINUE
               ELSE
                  DO 690 J = 1, N
                     DO 680 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  680                CONTINUE
  690             CONTINUE
               END IF
*
               NTEST = NTEST + 1
               CALL CHBEVX( 'V', 'A', UPLO, N, KD, V, LDU, U, LDU, VL,
     $                      VU, IL, IU, ABSTOL, M, WA1, Z, LDU, WORK,
     $                      RWORK, IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHBEVX(V,A,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 750
                  END IF
               END IF
*
*              Do tests 28 and 29.
*
               CALL CHET21( 1, UPLO, N, 0, A, LDU, WA1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 710 J = 1, N
                     DO 700 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  700                CONTINUE
  710             CONTINUE
               ELSE
                  DO 730 J = 1, N
                     DO 720 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  720                CONTINUE
  730             CONTINUE
               END IF
*
               CALL CHBEVX( 'N', 'A', UPLO, N, KD, V, LDU, U, LDU, VL,
     $                      VU, IL, IU, ABSTOL, M2, WA2, Z, LDU, WORK,
     $                      RWORK, IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVX(N,A,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 750
                  END IF
               END IF
*
*              Do test 30.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 740 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( WA1( J ) ), ABS( WA2( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( WA1( J )-WA2( J ) ) )
  740          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
*              Load array V with the upper or lower triangular part
*              of the matrix in band form.
*
  750          CONTINUE
               NTEST = NTEST + 1
               IF( IUPLO.EQ.1 ) THEN
                  DO 770 J = 1, N
                     DO 760 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  760                CONTINUE
  770             CONTINUE
               ELSE
                  DO 790 J = 1, N
                     DO 780 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  780                CONTINUE
  790             CONTINUE
               END IF
*
               CALL CHBEVX( 'V', 'I', UPLO, N, KD, V, LDU, U, LDU, VL,
     $                      VU, IL, IU, ABSTOL, M2, WA2, Z, LDU, WORK,
     $                      RWORK, IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVX(V,I,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 840
                  END IF
               END IF
*
*              Do tests 31 and 32.
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 810 J = 1, N
                     DO 800 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  800                CONTINUE
  810             CONTINUE
               ELSE
                  DO 830 J = 1, N
                     DO 820 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  820                CONTINUE
  830             CONTINUE
               END IF
               CALL CHBEVX( 'N', 'I', UPLO, N, KD, V, LDU, U, LDU, VL,
     $                      VU, IL, IU, ABSTOL, M3, WA3, Z, LDU, WORK,
     $                      RWORK, IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVX(N,I,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 840
                  END IF
               END IF
*
*              Do test 33.
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
*              Load array V with the upper or lower triangular part
*              of the matrix in band form.
*
  840          CONTINUE
               NTEST = NTEST + 1
               IF( IUPLO.EQ.1 ) THEN
                  DO 860 J = 1, N
                     DO 850 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  850                CONTINUE
  860             CONTINUE
               ELSE
                  DO 880 J = 1, N
                     DO 870 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  870                CONTINUE
  880             CONTINUE
               END IF
               CALL CHBEVX( 'V', 'V', UPLO, N, KD, V, LDU, U, LDU, VL,
     $                      VU, IL, IU, ABSTOL, M2, WA2, Z, LDU, WORK,
     $                      RWORK, IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVX(V,V,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 930
                  END IF
               END IF
*
*              Do tests 34 and 35.
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 900 J = 1, N
                     DO 890 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
  890                CONTINUE
  900             CONTINUE
               ELSE
                  DO 920 J = 1, N
                     DO 910 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
  910                CONTINUE
  920             CONTINUE
               END IF
               CALL CHBEVX( 'N', 'V', UPLO, N, KD, V, LDU, U, LDU, VL,
     $                      VU, IL, IU, ABSTOL, M3, WA3, Z, LDU, WORK,
     $                      RWORK, IWORK, IWORK( 5*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEVX(N,V,' // UPLO //
     $               ')', IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 930
                  END IF
               END IF
*
               IF( M3.EQ.0 .AND. N.GT.0 ) THEN
                  RESULT( NTEST ) = ULPINV
                  GO TO 930
               END IF
*
*              Do test 36.
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
  930          CONTINUE
*
*              Call CHEEV
*
               CALL CLACPY( ' ', N, N, A, LDA, V, LDU )
*
               NTEST = NTEST + 1
               CALL CHEEV( 'V', UPLO, N, A, LDU, D1, WORK, LWORK, RWORK,
     $                     IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEV(V,' // UPLO // ')',
     $               IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 950
                  END IF
               END IF
*
*              Do tests 37 and 38
*
               CALL CHET21( 1, UPLO, N, 0, V, LDU, D1, D2, A, LDU, Z,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               NTEST = NTEST + 2
               CALL CHEEV( 'N', UPLO, N, A, LDU, D3, WORK, LWORK, RWORK,
     $                     IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEV(N,' // UPLO // ')',
     $               IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 950
                  END IF
               END IF
*
*              Do test 39
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 940 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D3( J ) ) )
  940          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
  950          CONTINUE
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
*              Call CHPEV
*
*              Load array WORK with the upper or lower triangular
*              part of the matrix in packed form.
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 970 J = 1, N
                     DO 960 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  960                CONTINUE
  970             CONTINUE
               ELSE
                  INDX = 1
                  DO 990 J = 1, N
                     DO 980 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
  980                CONTINUE
  990             CONTINUE
               END IF
*
               NTEST = NTEST + 1
               INDWRK = N*( N+1 ) / 2 + 1
               CALL CHPEV( 'V', UPLO, N, WORK, D1, Z, LDU,
     $                     WORK( INDWRK ), RWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEV(V,' // UPLO // ')',
     $               IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 1050
                  END IF
               END IF
*
*              Do tests 40 and 41.
*
               CALL CHET21( 1, UPLO, N, 0, A, LDA, D1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               IF( IUPLO.EQ.1 ) THEN
                  INDX = 1
                  DO 1010 J = 1, N
                     DO 1000 I = 1, J
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
 1000                CONTINUE
 1010             CONTINUE
               ELSE
                  INDX = 1
                  DO 1030 J = 1, N
                     DO 1020 I = J, N
                        WORK( INDX ) = A( I, J )
                        INDX = INDX + 1
 1020                CONTINUE
 1030             CONTINUE
               END IF
*
               NTEST = NTEST + 2
               INDWRK = N*( N+1 ) / 2 + 1
               CALL CHPEV( 'N', UPLO, N, WORK, D3, Z, LDU,
     $                     WORK( INDWRK ), RWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHPEV(N,' // UPLO // ')',
     $               IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 1050
                  END IF
               END IF
*
*              Do test 42
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 1040 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D3( J ) ) )
 1040          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
 1050          CONTINUE
*
*              Call CHBEV
*
               IF( JTYPE.LE.7 ) THEN
                  KD = 0
               ELSE IF( JTYPE.GE.8 .AND. JTYPE.LE.15 ) THEN
                  KD = MAX( N-1, 0 )
               ELSE
                  KD = IHBW
               END IF
*
*              Load array V with the upper or lower triangular part
*              of the matrix in band form.
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 1070 J = 1, N
                     DO 1060 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
 1060                CONTINUE
 1070             CONTINUE
               ELSE
                  DO 1090 J = 1, N
                     DO 1080 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
 1080                CONTINUE
 1090             CONTINUE
               END IF
*
               NTEST = NTEST + 1
               CALL CHBEV( 'V', UPLO, N, KD, V, LDU, D1, Z, LDU, WORK,
     $                     RWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEV(V,' // UPLO // ')',
     $               IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 1140
                  END IF
               END IF
*
*              Do tests 43 and 44.
*
               CALL CHET21( 1, UPLO, N, 0, A, LDA, D1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               IF( IUPLO.EQ.1 ) THEN
                  DO 1110 J = 1, N
                     DO 1100 I = MAX( 1, J-KD ), J
                        V( KD+1+I-J, J ) = A( I, J )
 1100                CONTINUE
 1110             CONTINUE
               ELSE
                  DO 1130 J = 1, N
                     DO 1120 I = J, MIN( N, J+KD )
                        V( 1+I-J, J ) = A( I, J )
 1120                CONTINUE
 1130             CONTINUE
               END IF
*
               NTEST = NTEST + 2
               CALL CHBEV( 'N', UPLO, N, KD, V, LDU, D3, Z, LDU, WORK,
     $                     RWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9998 )'CHBEV(N,' // UPLO // ')',
     $               IINFO, N, KD, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 1140
                  END IF
               END IF
*
 1140          CONTINUE
*
*              Do test 45.
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 1150 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D3( J ) ) )
 1150          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
               CALL CLACPY( ' ', N, N, A, LDA, V, LDU )
               NTEST = NTEST + 1
               CALL CHEEVR( 'V', 'A', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M, WA1, Z, LDU, IWORK, WORK, LWORK,
     $                      RWORK, LRWORK, IWORK( 2*N+1 ), LIWORK-2*N,
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVR(V,A,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 1170
                  END IF
               END IF
*
*              Do tests 45 and 46 (or ... )
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               CALL CHET21( 1, UPLO, N, 0, A, LDU, WA1, D2, Z, LDU, V,
     $                      LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
               CALL CHEEVR( 'N', 'A', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, IWORK, WORK, LWORK,
     $                      RWORK, LRWORK, IWORK( 2*N+1 ), LIWORK-2*N,
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVR(N,A,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 1170
                  END IF
               END IF
*
*              Do test 47 (or ... )
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 1160 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( WA1( J ) ), ABS( WA2( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( WA1( J )-WA2( J ) ) )
 1160          CONTINUE
               RESULT( NTEST ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
 1170          CONTINUE
*
               NTEST = NTEST + 1
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
               CALL CHEEVR( 'V', 'I', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, IWORK, WORK, LWORK,
     $                      RWORK, LRWORK, IWORK( 2*N+1 ), LIWORK-2*N,
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVR(V,I,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 1180
                  END IF
               END IF
*
*              Do tests 48 and 49 (or +??)
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
               CALL CHEEVR( 'N', 'I', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M3, WA3, Z, LDU, IWORK, WORK, LWORK,
     $                      RWORK, LRWORK, IWORK( 2*N+1 ), LIWORK-2*N,
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVR(N,I,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 1180
                  END IF
               END IF
*
*              Do test 50 (or +??)
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, ULP*TEMP3 )
 1180          CONTINUE
*
               NTEST = NTEST + 1
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
               CALL CHEEVR( 'V', 'V', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M2, WA2, Z, LDU, IWORK, WORK, LWORK,
     $                      RWORK, LRWORK, IWORK( 2*N+1 ), LIWORK-2*N,
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVR(V,V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     RESULT( NTEST+1 ) = ULPINV
                     RESULT( NTEST+2 ) = ULPINV
                     GO TO 1190
                  END IF
               END IF
*
*              Do tests 51 and 52 (or +??)
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
               CALL CHET22( 1, UPLO, N, M2, 0, A, LDU, WA2, D2, Z, LDU,
     $                      V, LDU, TAU, WORK, RWORK, RESULT( NTEST ) )
*
               NTEST = NTEST + 2
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
               CALL CHEEVR( 'N', 'V', UPLO, N, A, LDU, VL, VU, IL, IU,
     $                      ABSTOL, M3, WA3, Z, LDU, IWORK, WORK, LWORK,
     $                      RWORK, LRWORK, IWORK( 2*N+1 ), LIWORK-2*N,
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHEEVR(N,V,' // UPLO //
     $               ')', IINFO, N, JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( NTEST ) = ULPINV
                     GO TO 1190
                  END IF
               END IF
*
               IF( M3.EQ.0 .AND. N.GT.0 ) THEN
                  RESULT( NTEST ) = ULPINV
                  GO TO 1190
               END IF
*
*              Do test 52 (or +??)
*
               TEMP1 = SSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
               TEMP2 = SSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
               IF( N.GT.0 ) THEN
                  TEMP3 = MAX( ABS( WA1( 1 ) ), ABS( WA1( N ) ) )
               ELSE
                  TEMP3 = ZERO
               END IF
               RESULT( NTEST ) = ( TEMP1+TEMP2 ) /
     $                           MAX( UNFL, TEMP3*ULP )
*
               CALL CLACPY( ' ', N, N, V, LDU, A, LDA )
*
*
*
*
*              Load array V with the upper or lower triangular part
*              of the matrix in band form.
*
 1190          CONTINUE
*
 1200       CONTINUE
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
            NTESTT = NTESTT + NTEST
            CALL SLAFTS( 'CST', N, N, JTYPE, NTEST, RESULT, IOLDSD,
     $                   THRESH, NOUNIT, NERRS )
*
 1210    CONTINUE
 1220 CONTINUE
*
*     Summary
*
      CALL ALASVM( 'CST', NOUNIT, NERRS, NTESTT, 0 )
*
 9999 FORMAT( ' CDRVST: ', A, ' returned INFO=', I6, / 9X, 'N=', I6,
     $      ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
 9998 FORMAT( ' CDRVST: ', A, ' returned INFO=', I6, / 9X, 'N=', I6,
     $      ', KD=', I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5,
     $      ')' )
*
      RETURN
*
*     End of CDRVST
*
      END
