*> \brief \b SDRVSG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SDRVSG( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, B, LDB, D, Z, LDZ, AB, BB, AP,
*                          BP, WORK, NWORK, IWORK, LIWORK, RESULT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDB, LDZ, LIWORK, NOUNIT, NSIZES,
*      $                   NTYPES, NWORK
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
*       REAL               A( LDA, * ), AB( LDA, * ), AP( * ),
*      $                   B( LDB, * ), BB( LDB, * ), BP( * ), D( * ),
*      $                   RESULT( * ), WORK( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      SDRVSG checks the real symmetric generalized eigenproblem
*>      drivers.
*>
*>              SSYGV computes all eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite generalized
*>              eigenproblem.
*>
*>              SSYGVD computes all eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite generalized
*>              eigenproblem using a divide and conquer algorithm.
*>
*>              SSYGVX computes selected eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite generalized
*>              eigenproblem.
*>
*>              SSPGV computes all eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite generalized
*>              eigenproblem in packed storage.
*>
*>              SSPGVD computes all eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite generalized
*>              eigenproblem in packed storage using a divide and
*>              conquer algorithm.
*>
*>              SSPGVX computes selected eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite generalized
*>              eigenproblem in packed storage.
*>
*>              SSBGV computes all eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite banded
*>              generalized eigenproblem.
*>
*>              SSBGVD computes all eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite banded
*>              generalized eigenproblem using a divide and conquer
*>              algorithm.
*>
*>              SSBGVX computes selected eigenvalues and, optionally,
*>              eigenvectors of a real symmetric-definite banded
*>              generalized eigenproblem.
*>
*>      When SDRVSG is called, a number of matrix "sizes" ("n's") and a
*>      number of matrix "types" are specified.  For each size ("n")
*>      and each type of matrix, one matrix A of the given type will be
*>      generated; a random well-conditioned matrix B is also generated
*>      and the pair (A,B) is used to test the drivers.
*>
*>      For each pair (A,B), the following tests are performed:
*>
*>      (1) SSYGV with ITYPE = 1 and UPLO ='U':
*>
*>              | A Z - B Z D | / ( |A| |Z| n ulp )
*>
*>      (2) as (1) but calling SSPGV
*>      (3) as (1) but calling SSBGV
*>      (4) as (1) but with UPLO = 'L'
*>      (5) as (4) but calling SSPGV
*>      (6) as (4) but calling SSBGV
*>
*>      (7) SSYGV with ITYPE = 2 and UPLO ='U':
*>
*>              | A B Z - Z D | / ( |A| |Z| n ulp )
*>
*>      (8) as (7) but calling SSPGV
*>      (9) as (7) but with UPLO = 'L'
*>      (10) as (9) but calling SSPGV
*>
*>      (11) SSYGV with ITYPE = 3 and UPLO ='U':
*>
*>              | B A Z - Z D | / ( |A| |Z| n ulp )
*>
*>      (12) as (11) but calling SSPGV
*>      (13) as (11) but with UPLO = 'L'
*>      (14) as (13) but calling SSPGV
*>
*>      SSYGVD, SSPGVD and SSBGVD performed the same 14 tests.
*>
*>      SSYGVX, SSPGVX and SSBGVX performed the above 14 tests with
*>      the parameter RANGE = 'A', 'N' and 'I', respectively.
*>
*>      The "sizes" are specified by an array NN(1:NSIZES); the value
*>      of each element NN(j) specifies one size.
*>      The "types" are specified by a logical array DOTYPE( 1:NTYPES );
*>      if DOTYPE(j) is .TRUE., then matrix type "j" will be generated.
*>      This type is used for the matrix A which has half-bandwidth KA.
*>      B is generated as a well-conditioned positive definite matrix
*>      with half-bandwidth KB (<= KA).
*>      Currently, the list of possible types for A is:
*>
*>      (1)  The zero matrix.
*>      (2)  The identity matrix.
*>
*>      (3)  A diagonal matrix with evenly spaced entries
*>           1, ..., ULP  and random signs.
*>           (ULP = (first number larger than 1) - 1 )
*>      (4)  A diagonal matrix with geometrically spaced entries
*>           1, ..., ULP  and random signs.
*>      (5)  A diagonal matrix with "clustered" entries
*>           1, ULP, ..., ULP and random signs.
*>
*>      (6)  Same as (4), but multiplied by SQRT( overflow threshold )
*>      (7)  Same as (4), but multiplied by SQRT( underflow threshold )
*>
*>      (8)  A matrix of the form  U* D U, where U is orthogonal and
*>           D has evenly spaced entries 1, ..., ULP with random signs
*>           on the diagonal.
*>
*>      (9)  A matrix of the form  U* D U, where U is orthogonal and
*>           D has geometrically spaced entries 1, ..., ULP with random
*>           signs on the diagonal.
*>
*>      (10) A matrix of the form  U* D U, where U is orthogonal and
*>           D has "clustered" entries 1, ULP,..., ULP with random
*>           signs on the diagonal.
*>
*>      (11) Same as (8), but multiplied by SQRT( overflow threshold )
*>      (12) Same as (8), but multiplied by SQRT( underflow threshold )
*>
*>      (13) symmetric matrix with random entries chosen from (-1,1).
*>      (14) Same as (13), but multiplied by SQRT( overflow threshold )
*>      (15) Same as (13), but multiplied by SQRT( underflow threshold)
*>
*>      (16) Same as (8), but with KA = 1 and KB = 1
*>      (17) Same as (8), but with KA = 2 and KB = 1
*>      (18) Same as (8), but with KA = 2 and KB = 2
*>      (19) Same as (8), but with KA = 3 and KB = 1
*>      (20) Same as (8), but with KA = 3 and KB = 2
*>      (21) Same as (8), but with KA = 3 and KB = 3
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  NSIZES  INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          SDRVSG does nothing.  It must be at least zero.
*>          Not modified.
*>
*>  NN      INTEGER array, dimension (NSIZES)
*>          An array containing the sizes to be used for the matrices.
*>          Zero values will be skipped.  The values must be at least
*>          zero.
*>          Not modified.
*>
*>  NTYPES  INTEGER
*>          The number of elements in DOTYPE.   If it is zero, SDRVSG
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
*>          next call to SDRVSG to continue the same random number
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
*>  A       REAL array, dimension (LDA , max(NN))
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually
*>          used.
*>          Modified.
*>
*>  LDA     INTEGER
*>          The leading dimension of A and AB.  It must be at
*>          least 1 and at least max( NN ).
*>          Not modified.
*>
*>  B       REAL array, dimension (LDB , max(NN))
*>          Used to hold the symmetric positive definite matrix for
*>          the generailzed problem.
*>          On exit, B contains the last matrix actually
*>          used.
*>          Modified.
*>
*>  LDB     INTEGER
*>          The leading dimension of B and BB.  It must be at
*>          least 1 and at least max( NN ).
*>          Not modified.
*>
*>  D       REAL array, dimension (max(NN))
*>          The eigenvalues of A. On exit, the eigenvalues in D
*>          correspond with the matrix in A.
*>          Modified.
*>
*>  Z       REAL array, dimension (LDZ, max(NN))
*>          The matrix of eigenvectors.
*>          Modified.
*>
*>  LDZ     INTEGER
*>          The leading dimension of Z.  It must be at least 1 and
*>          at least max( NN ).
*>          Not modified.
*>
*>  AB      REAL array, dimension (LDA, max(NN))
*>          Workspace.
*>          Modified.
*>
*>  BB      REAL array, dimension (LDB, max(NN))
*>          Workspace.
*>          Modified.
*>
*>  AP      REAL array, dimension (max(NN)**2)
*>          Workspace.
*>          Modified.
*>
*>  BP      REAL array, dimension (max(NN)**2)
*>          Workspace.
*>          Modified.
*>
*>  WORK    REAL array, dimension (NWORK)
*>          Workspace.
*>          Modified.
*>
*>  NWORK   INTEGER
*>          The number of entries in WORK.  This must be at least
*>          1+5*N+2*N*lg(N)+3*N**2 where N = max( NN(j) ) and
*>          lg( N ) = smallest integer k such that 2**k >= N.
*>          Not modified.
*>
*>  IWORK   INTEGER array, dimension (LIWORK)
*>          Workspace.
*>          Modified.
*>
*>  LIWORK  INTEGER
*>          The number of entries in WORK.  This must be at least 6*N.
*>          Not modified.
*>
*>  RESULT  REAL array, dimension (70)
*>          The values computed by the 70 tests described above.
*>          Modified.
*>
*>  INFO    INTEGER
*>          If 0, then everything ran OK.
*>           -1: NSIZES < 0
*>           -2: Some NN(j) < 0
*>           -3: NTYPES < 0
*>           -5: THRESH < 0
*>           -9: LDA < 1 or LDA < NMAX, where NMAX is max( NN(j) ).
*>          -16: LDZ < 1 or LDZ < NMAX.
*>          -21: NWORK too small.
*>          -23: LIWORK too small.
*>          If  SLATMR, SLATMS, SSYGV, SSPGV, SSBGV, SSYGVD, SSPGVD,
*>              SSBGVD, SSYGVX, SSPGVX or SSBGVX returns an error code,
*>              the absolute value of it is returned.
*>          Modified.
*>
*> ----------------------------------------------------------------------
*>
*>       Some Local Variables and Parameters:
*>       ---- ----- --------- --- ----------
*>       ZERO, ONE       Real 0 and 1.
*>       MAXTYP          The number of types defined.
*>       NTEST           The number of tests that have been run
*>                       on this matrix.
*>       NTESTT          The total number of tests for this call.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SDRVSG( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NOUNIT, A, LDA, B, LDB, D, Z, LDZ, AB, BB, AP,
     $                   BP, WORK, NWORK, IWORK, LIWORK, RESULT, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDB, LDZ, LIWORK, NOUNIT, NSIZES,
     $                   NTYPES, NWORK
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
      REAL               A( LDA, * ), AB( LDA, * ), AP( * ),
     $                   B( LDB, * ), BB( LDB, * ), BP( * ), D( * ),
     $                   RESULT( * ), WORK( * ), Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0, TEN = 10.0E0 )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 21 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN
      CHARACTER          UPLO
      INTEGER            I, IBTYPE, IBUPLO, IINFO, IJ, IL, IMODE, ITEMP,
     $                   ITYPE, IU, J, JCOL, JSIZE, JTYPE, KA, KA9, KB,
     $                   KB9, M, MTYPES, N, NERRS, NMATS, NMAX, NTEST,
     $                   NTESTT
      REAL               ABSTOL, ANINV, ANORM, COND, OVFL, RTOVFL,
     $                   RTUNFL, ULP, ULPINV, UNFL, VL, VU
*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), ISEED2( 4 ),
     $                   KMAGN( MAXTYP ), KMODE( MAXTYP ),
     $                   KTYPE( MAXTYP )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH, SLARND
      EXTERNAL           LSAME, SLAMCH, SLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLABAD, SLACPY, SLAFTS, SLASET, SLASUM, SLATMR,
     $                   SLATMS, SSBGV, SSBGVD, SSBGVX, SSGT01, SSPGV,
     $                   SSPGVD, SSPGVX, SSYGV, SSYGVD, SSYGVX, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, REAL, SQRT
*     ..
*     .. Data statements ..
      DATA               KTYPE / 1, 2, 5*4, 5*5, 3*8, 6*9 /
      DATA               KMAGN / 2*1, 1, 1, 1, 2, 3, 1, 1, 1, 2, 3, 1,
     $                   2, 3, 6*1 /
      DATA               KMODE / 2*0, 4, 3, 1, 4, 4, 4, 3, 1, 4, 4, 0,
     $                   0, 0, 6*4 /
*     ..
*     .. Executable Statements ..
*
*     1)      Check for errors
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
      ELSE IF( LDA.LE.1 .OR. LDA.LT.NMAX ) THEN
         INFO = -9
      ELSE IF( LDZ.LE.1 .OR. LDZ.LT.NMAX ) THEN
         INFO = -16
      ELSE IF( 2*MAX( NMAX, 3 )**2.GT.NWORK ) THEN
         INFO = -21
      ELSE IF( 2*MAX( NMAX, 3 )**2.GT.LIWORK ) THEN
         INFO = -23
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SDRVSG', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
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
      DO 20 I = 1, 4
         ISEED2( I ) = ISEED( I )
   20 CONTINUE
*
*     Loop over sizes, types
*
      NERRS = 0
      NMATS = 0
*
      DO 650 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         ANINV = ONE / REAL( MAX( 1, N ) )
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         KA9 = 0
         KB9 = 0
         DO 640 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 640
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
*           =4         arithmetic   diagonal, w/ eigenvalues
*           =5         random log   hermitian, w/ eigenvalues
*           =6         random       (none)
*           =7                      random diagonal
*           =8                      random hermitian
*           =9                      banded, w/ eigenvalues
*
            IF( MTYPES.GT.MAXTYP )
     $         GO TO 90
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
            IINFO = 0
            COND = ULPINV
*
*           Special Matrices -- Identity & Jordan block
*
            IF( ITYPE.EQ.1 ) THEN
*
*              Zero
*
               KA = 0
               KB = 0
               CALL SLASET( 'Full', LDA, N, ZERO, ZERO, A, LDA )
*
            ELSE IF( ITYPE.EQ.2 ) THEN
*
*              Identity
*
               KA = 0
               KB = 0
               CALL SLASET( 'Full', LDA, N, ZERO, ZERO, A, LDA )
               DO 80 JCOL = 1, N
                  A( JCOL, JCOL ) = ANORM
   80          CONTINUE
*
            ELSE IF( ITYPE.EQ.4 ) THEN
*
*              Diagonal Matrix, [Eigen]values Specified
*
               KA = 0
               KB = 0
               CALL SLATMS( N, N, 'S', ISEED, 'S', WORK, IMODE, COND,
     $                      ANORM, 0, 0, 'N', A, LDA, WORK( N+1 ),
     $                      IINFO )
*
            ELSE IF( ITYPE.EQ.5 ) THEN
*
*              symmetric, eigenvalues specified
*
               KA = MAX( 0, N-1 )
               KB = KA
               CALL SLATMS( N, N, 'S', ISEED, 'S', WORK, IMODE, COND,
     $                      ANORM, N, N, 'N', A, LDA, WORK( N+1 ),
     $                      IINFO )
*
            ELSE IF( ITYPE.EQ.7 ) THEN
*
*              Diagonal, random eigenvalues
*
               KA = 0
               KB = 0
               CALL SLATMR( N, N, 'S', ISEED, 'S', WORK, 6, ONE, ONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.8 ) THEN
*
*              symmetric, random eigenvalues
*
               KA = MAX( 0, N-1 )
               KB = KA
               CALL SLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE, ONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.9 ) THEN
*
*              symmetric banded, eigenvalues specified
*
*              The following values are used for the half-bandwidths:
*
*                ka = 1   kb = 1
*                ka = 2   kb = 1
*                ka = 2   kb = 2
*                ka = 3   kb = 1
*                ka = 3   kb = 2
*                ka = 3   kb = 3
*
               KB9 = KB9 + 1
               IF( KB9.GT.KA9 ) THEN
                  KA9 = KA9 + 1
                  KB9 = 1
               END IF
               KA = MAX( 0, MIN( N-1, KA9 ) )
               KB = MAX( 0, MIN( N-1, KB9 ) )
               CALL SLATMS( N, N, 'S', ISEED, 'S', WORK, IMODE, COND,
     $                      ANORM, KA, KA, 'N', A, LDA, WORK( N+1 ),
     $                      IINFO )
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
   90       CONTINUE
*
            ABSTOL = UNFL + UNFL
            IF( N.LE.1 ) THEN
               IL = 1
               IU = N
            ELSE
               IL = 1 + ( N-1 )*SLARND( 1, ISEED2 )
               IU = 1 + ( N-1 )*SLARND( 1, ISEED2 )
               IF( IL.GT.IU ) THEN
                  ITEMP = IL
                  IL = IU
                  IU = ITEMP
               END IF
            END IF
*
*           3) Call SSYGV, SSPGV, SSBGV, SSYGVD, SSPGVD, SSBGVD,
*              SSYGVX, SSPGVX, and SSBGVX, do tests.
*
*           loop over the three generalized problems
*                 IBTYPE = 1: A*x = (lambda)*B*x
*                 IBTYPE = 2: A*B*x = (lambda)*x
*                 IBTYPE = 3: B*A*x = (lambda)*x
*
            DO 630 IBTYPE = 1, 3
*
*              loop over the setting UPLO
*
               DO 620 IBUPLO = 1, 2
                  IF( IBUPLO.EQ.1 )
     $               UPLO = 'U'
                  IF( IBUPLO.EQ.2 )
     $               UPLO = 'L'
*
*                 Generate random well-conditioned positive definite
*                 matrix B, of bandwidth not greater than that of A.
*
                  CALL SLATMS( N, N, 'U', ISEED, 'P', WORK, 5, TEN, ONE,
     $                         KB, KB, UPLO, B, LDB, WORK( N+1 ),
     $                         IINFO )
*
*                 Test SSYGV
*
                  NTEST = NTEST + 1
*
                  CALL SLACPY( ' ', N, N, A, LDA, Z, LDZ )
                  CALL SLACPY( UPLO, N, N, B, LDB, BB, LDB )
*
                  CALL SSYGV( IBTYPE, 'V', UPLO, N, Z, LDZ, BB, LDB, D,
     $                        WORK, NWORK, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSYGV(V,' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 100
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
*                 Test SSYGVD
*
                  NTEST = NTEST + 1
*
                  CALL SLACPY( ' ', N, N, A, LDA, Z, LDZ )
                  CALL SLACPY( UPLO, N, N, B, LDB, BB, LDB )
*
                  CALL SSYGVD( IBTYPE, 'V', UPLO, N, Z, LDZ, BB, LDB, D,
     $                         WORK, NWORK, IWORK, LIWORK, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSYGVD(V,' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 100
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
*                 Test SSYGVX
*
                  NTEST = NTEST + 1
*
                  CALL SLACPY( ' ', N, N, A, LDA, AB, LDA )
                  CALL SLACPY( UPLO, N, N, B, LDB, BB, LDB )
*
                  CALL SSYGVX( IBTYPE, 'V', 'A', UPLO, N, AB, LDA, BB,
     $                         LDB, VL, VU, IL, IU, ABSTOL, M, D, Z,
     $                         LDZ, WORK, NWORK, IWORK( N+1 ), IWORK,
     $                         IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSYGVX(V,A' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 100
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
                  NTEST = NTEST + 1
*
                  CALL SLACPY( ' ', N, N, A, LDA, AB, LDA )
                  CALL SLACPY( UPLO, N, N, B, LDB, BB, LDB )
*
*                 since we do not know the exact eigenvalues of this
*                 eigenpair, we just set VL and VU as constants.
*                 It is quite possible that there are no eigenvalues
*                 in this interval.
*
                  VL = ZERO
                  VU = ANORM
                  CALL SSYGVX( IBTYPE, 'V', 'V', UPLO, N, AB, LDA, BB,
     $                         LDB, VL, VU, IL, IU, ABSTOL, M, D, Z,
     $                         LDZ, WORK, NWORK, IWORK( N+1 ), IWORK,
     $                         IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSYGVX(V,V,' //
     $                  UPLO // ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 100
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
                  NTEST = NTEST + 1
*
                  CALL SLACPY( ' ', N, N, A, LDA, AB, LDA )
                  CALL SLACPY( UPLO, N, N, B, LDB, BB, LDB )
*
                  CALL SSYGVX( IBTYPE, 'V', 'I', UPLO, N, AB, LDA, BB,
     $                         LDB, VL, VU, IL, IU, ABSTOL, M, D, Z,
     $                         LDZ, WORK, NWORK, IWORK( N+1 ), IWORK,
     $                         IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSYGVX(V,I,' //
     $                  UPLO // ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 100
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
  100             CONTINUE
*
*                 Test SSPGV
*
                  NTEST = NTEST + 1
*
*                 Copy the matrices into packed storage.
*
                  IF( LSAME( UPLO, 'U' ) ) THEN
                     IJ = 1
                     DO 120 J = 1, N
                        DO 110 I = 1, J
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  110                   CONTINUE
  120                CONTINUE
                  ELSE
                     IJ = 1
                     DO 140 J = 1, N
                        DO 130 I = J, N
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  130                   CONTINUE
  140                CONTINUE
                  END IF
*
                  CALL SSPGV( IBTYPE, 'V', UPLO, N, AP, BP, D, Z, LDZ,
     $                        WORK, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSPGV(V,' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 310
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
*                 Test SSPGVD
*
                  NTEST = NTEST + 1
*
*                 Copy the matrices into packed storage.
*
                  IF( LSAME( UPLO, 'U' ) ) THEN
                     IJ = 1
                     DO 160 J = 1, N
                        DO 150 I = 1, J
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  150                   CONTINUE
  160                CONTINUE
                  ELSE
                     IJ = 1
                     DO 180 J = 1, N
                        DO 170 I = J, N
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  170                   CONTINUE
  180                CONTINUE
                  END IF
*
                  CALL SSPGVD( IBTYPE, 'V', UPLO, N, AP, BP, D, Z, LDZ,
     $                         WORK, NWORK, IWORK, LIWORK, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSPGVD(V,' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 310
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
*                 Test SSPGVX
*
                  NTEST = NTEST + 1
*
*                 Copy the matrices into packed storage.
*
                  IF( LSAME( UPLO, 'U' ) ) THEN
                     IJ = 1
                     DO 200 J = 1, N
                        DO 190 I = 1, J
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  190                   CONTINUE
  200                CONTINUE
                  ELSE
                     IJ = 1
                     DO 220 J = 1, N
                        DO 210 I = J, N
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  210                   CONTINUE
  220                CONTINUE
                  END IF
*
                  CALL SSPGVX( IBTYPE, 'V', 'A', UPLO, N, AP, BP, VL,
     $                         VU, IL, IU, ABSTOL, M, D, Z, LDZ, WORK,
     $                         IWORK( N+1 ), IWORK, INFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSPGVX(V,A' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 310
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
                  NTEST = NTEST + 1
*
*                 Copy the matrices into packed storage.
*
                  IF( LSAME( UPLO, 'U' ) ) THEN
                     IJ = 1
                     DO 240 J = 1, N
                        DO 230 I = 1, J
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  230                   CONTINUE
  240                CONTINUE
                  ELSE
                     IJ = 1
                     DO 260 J = 1, N
                        DO 250 I = J, N
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  250                   CONTINUE
  260                CONTINUE
                  END IF
*
                  VL = ZERO
                  VU = ANORM
                  CALL SSPGVX( IBTYPE, 'V', 'V', UPLO, N, AP, BP, VL,
     $                         VU, IL, IU, ABSTOL, M, D, Z, LDZ, WORK,
     $                         IWORK( N+1 ), IWORK, INFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSPGVX(V,V' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 310
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
                  NTEST = NTEST + 1
*
*                 Copy the matrices into packed storage.
*
                  IF( LSAME( UPLO, 'U' ) ) THEN
                     IJ = 1
                     DO 280 J = 1, N
                        DO 270 I = 1, J
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  270                   CONTINUE
  280                CONTINUE
                  ELSE
                     IJ = 1
                     DO 300 J = 1, N
                        DO 290 I = J, N
                           AP( IJ ) = A( I, J )
                           BP( IJ ) = B( I, J )
                           IJ = IJ + 1
  290                   CONTINUE
  300                CONTINUE
                  END IF
*
                  CALL SSPGVX( IBTYPE, 'V', 'I', UPLO, N, AP, BP, VL,
     $                         VU, IL, IU, ABSTOL, M, D, Z, LDZ, WORK,
     $                         IWORK( N+1 ), IWORK, INFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'SSPGVX(V,I' // UPLO //
     $                  ')', IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( NTEST ) = ULPINV
                        GO TO 310
                     END IF
                  END IF
*
*                 Do Test
*
                  CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                         LDZ, D, WORK, RESULT( NTEST ) )
*
  310             CONTINUE
*
                  IF( IBTYPE.EQ.1 ) THEN
*
*                    TEST SSBGV
*
                     NTEST = NTEST + 1
*
*                    Copy the matrices into band storage.
*
                     IF( LSAME( UPLO, 'U' ) ) THEN
                        DO 340 J = 1, N
                           DO 320 I = MAX( 1, J-KA ), J
                              AB( KA+1+I-J, J ) = A( I, J )
  320                      CONTINUE
                           DO 330 I = MAX( 1, J-KB ), J
                              BB( KB+1+I-J, J ) = B( I, J )
  330                      CONTINUE
  340                   CONTINUE
                     ELSE
                        DO 370 J = 1, N
                           DO 350 I = J, MIN( N, J+KA )
                              AB( 1+I-J, J ) = A( I, J )
  350                      CONTINUE
                           DO 360 I = J, MIN( N, J+KB )
                              BB( 1+I-J, J ) = B( I, J )
  360                      CONTINUE
  370                   CONTINUE
                     END IF
*
                     CALL SSBGV( 'V', UPLO, N, KA, KB, AB, LDA, BB, LDB,
     $                           D, Z, LDZ, WORK, IINFO )
                     IF( IINFO.NE.0 ) THEN
                        WRITE( NOUNIT, FMT = 9999 )'SSBGV(V,' //
     $                     UPLO // ')', IINFO, N, JTYPE, IOLDSD
                        INFO = ABS( IINFO )
                        IF( IINFO.LT.0 ) THEN
                           RETURN
                        ELSE
                           RESULT( NTEST ) = ULPINV
                           GO TO 620
                        END IF
                     END IF
*
*                    Do Test
*
                     CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                            LDZ, D, WORK, RESULT( NTEST ) )
*
*                    TEST SSBGVD
*
                     NTEST = NTEST + 1
*
*                    Copy the matrices into band storage.
*
                     IF( LSAME( UPLO, 'U' ) ) THEN
                        DO 400 J = 1, N
                           DO 380 I = MAX( 1, J-KA ), J
                              AB( KA+1+I-J, J ) = A( I, J )
  380                      CONTINUE
                           DO 390 I = MAX( 1, J-KB ), J
                              BB( KB+1+I-J, J ) = B( I, J )
  390                      CONTINUE
  400                   CONTINUE
                     ELSE
                        DO 430 J = 1, N
                           DO 410 I = J, MIN( N, J+KA )
                              AB( 1+I-J, J ) = A( I, J )
  410                      CONTINUE
                           DO 420 I = J, MIN( N, J+KB )
                              BB( 1+I-J, J ) = B( I, J )
  420                      CONTINUE
  430                   CONTINUE
                     END IF
*
                     CALL SSBGVD( 'V', UPLO, N, KA, KB, AB, LDA, BB,
     $                            LDB, D, Z, LDZ, WORK, NWORK, IWORK,
     $                            LIWORK, IINFO )
                     IF( IINFO.NE.0 ) THEN
                        WRITE( NOUNIT, FMT = 9999 )'SSBGVD(V,' //
     $                     UPLO // ')', IINFO, N, JTYPE, IOLDSD
                        INFO = ABS( IINFO )
                        IF( IINFO.LT.0 ) THEN
                           RETURN
                        ELSE
                           RESULT( NTEST ) = ULPINV
                           GO TO 620
                        END IF
                     END IF
*
*                    Do Test
*
                     CALL SSGT01( IBTYPE, UPLO, N, N, A, LDA, B, LDB, Z,
     $                            LDZ, D, WORK, RESULT( NTEST ) )
*
*                    Test SSBGVX
*
                     NTEST = NTEST + 1
*
*                    Copy the matrices into band storage.
*
                     IF( LSAME( UPLO, 'U' ) ) THEN
                        DO 460 J = 1, N
                           DO 440 I = MAX( 1, J-KA ), J
                              AB( KA+1+I-J, J ) = A( I, J )
  440                      CONTINUE
                           DO 450 I = MAX( 1, J-KB ), J
                              BB( KB+1+I-J, J ) = B( I, J )
  450                      CONTINUE
  460                   CONTINUE
                     ELSE
                        DO 490 J = 1, N
                           DO 470 I = J, MIN( N, J+KA )
                              AB( 1+I-J, J ) = A( I, J )
  470                      CONTINUE
                           DO 480 I = J, MIN( N, J+KB )
                              BB( 1+I-J, J ) = B( I, J )
  480                      CONTINUE
  490                   CONTINUE
                     END IF
*
                     CALL SSBGVX( 'V', 'A', UPLO, N, KA, KB, AB, LDA,
     $                            BB, LDB, BP, MAX( 1, N ), VL, VU, IL,
     $                            IU, ABSTOL, M, D, Z, LDZ, WORK,
     $                            IWORK( N+1 ), IWORK, IINFO )
                     IF( IINFO.NE.0 ) THEN
                        WRITE( NOUNIT, FMT = 9999 )'SSBGVX(V,A' //
     $                     UPLO // ')', IINFO, N, JTYPE, IOLDSD
                        INFO = ABS( IINFO )
                        IF( IINFO.LT.0 ) THEN
                           RETURN
                        ELSE
                           RESULT( NTEST ) = ULPINV
                           GO TO 620
                        END IF
                     END IF
*
*                    Do Test
*
                     CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                            LDZ, D, WORK, RESULT( NTEST ) )
*
*
                     NTEST = NTEST + 1
*
*                    Copy the matrices into band storage.
*
                     IF( LSAME( UPLO, 'U' ) ) THEN
                        DO 520 J = 1, N
                           DO 500 I = MAX( 1, J-KA ), J
                              AB( KA+1+I-J, J ) = A( I, J )
  500                      CONTINUE
                           DO 510 I = MAX( 1, J-KB ), J
                              BB( KB+1+I-J, J ) = B( I, J )
  510                      CONTINUE
  520                   CONTINUE
                     ELSE
                        DO 550 J = 1, N
                           DO 530 I = J, MIN( N, J+KA )
                              AB( 1+I-J, J ) = A( I, J )
  530                      CONTINUE
                           DO 540 I = J, MIN( N, J+KB )
                              BB( 1+I-J, J ) = B( I, J )
  540                      CONTINUE
  550                   CONTINUE
                     END IF
*
                     VL = ZERO
                     VU = ANORM
                     CALL SSBGVX( 'V', 'V', UPLO, N, KA, KB, AB, LDA,
     $                            BB, LDB, BP, MAX( 1, N ), VL, VU, IL,
     $                            IU, ABSTOL, M, D, Z, LDZ, WORK,
     $                            IWORK( N+1 ), IWORK, IINFO )
                     IF( IINFO.NE.0 ) THEN
                        WRITE( NOUNIT, FMT = 9999 )'SSBGVX(V,V' //
     $                     UPLO // ')', IINFO, N, JTYPE, IOLDSD
                        INFO = ABS( IINFO )
                        IF( IINFO.LT.0 ) THEN
                           RETURN
                        ELSE
                           RESULT( NTEST ) = ULPINV
                           GO TO 620
                        END IF
                     END IF
*
*                    Do Test
*
                     CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                            LDZ, D, WORK, RESULT( NTEST ) )
*
                     NTEST = NTEST + 1
*
*                    Copy the matrices into band storage.
*
                     IF( LSAME( UPLO, 'U' ) ) THEN
                        DO 580 J = 1, N
                           DO 560 I = MAX( 1, J-KA ), J
                              AB( KA+1+I-J, J ) = A( I, J )
  560                      CONTINUE
                           DO 570 I = MAX( 1, J-KB ), J
                              BB( KB+1+I-J, J ) = B( I, J )
  570                      CONTINUE
  580                   CONTINUE
                     ELSE
                        DO 610 J = 1, N
                           DO 590 I = J, MIN( N, J+KA )
                              AB( 1+I-J, J ) = A( I, J )
  590                      CONTINUE
                           DO 600 I = J, MIN( N, J+KB )
                              BB( 1+I-J, J ) = B( I, J )
  600                      CONTINUE
  610                   CONTINUE
                     END IF
*
                     CALL SSBGVX( 'V', 'I', UPLO, N, KA, KB, AB, LDA,
     $                            BB, LDB, BP, MAX( 1, N ), VL, VU, IL,
     $                            IU, ABSTOL, M, D, Z, LDZ, WORK,
     $                            IWORK( N+1 ), IWORK, IINFO )
                     IF( IINFO.NE.0 ) THEN
                        WRITE( NOUNIT, FMT = 9999 )'SSBGVX(V,I' //
     $                     UPLO // ')', IINFO, N, JTYPE, IOLDSD
                        INFO = ABS( IINFO )
                        IF( IINFO.LT.0 ) THEN
                           RETURN
                        ELSE
                           RESULT( NTEST ) = ULPINV
                           GO TO 620
                        END IF
                     END IF
*
*                    Do Test
*
                     CALL SSGT01( IBTYPE, UPLO, N, M, A, LDA, B, LDB, Z,
     $                            LDZ, D, WORK, RESULT( NTEST ) )
*
                  END IF
*
  620          CONTINUE
  630       CONTINUE
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
            NTESTT = NTESTT + NTEST
            CALL SLAFTS( 'SSG', N, N, JTYPE, NTEST, RESULT, IOLDSD,
     $                   THRESH, NOUNIT, NERRS )
  640    CONTINUE
  650 CONTINUE
*
*     Summary
*
      CALL SLASUM( 'SSG', NOUNIT, NERRS, NTESTT )
*
      RETURN
*
*     End of SDRVSG
*
 9999 FORMAT( ' SDRVSG: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
      END
