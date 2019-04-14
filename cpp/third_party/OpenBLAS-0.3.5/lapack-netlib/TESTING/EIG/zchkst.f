*> \brief \b ZCHKST
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZCHKST( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, AP, SD, SE, D1, D2, D3, D4, D5,
*                          WA1, WA2, WA3, WR, U, LDU, V, VP, TAU, Z, WORK,
*                          LWORK, RWORK, LRWORK, IWORK, LIWORK, RESULT,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDU, LIWORK, LRWORK, LWORK, NOUNIT,
*      $                   NSIZES, NTYPES
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
*       DOUBLE PRECISION   D1( * ), D2( * ), D3( * ), D4( * ), D5( * ),
*      $                   RESULT( * ), RWORK( * ), SD( * ), SE( * ),
*      $                   WA1( * ), WA2( * ), WA3( * ), WR( * )
*       COMPLEX*16         A( LDA, * ), AP( * ), TAU( * ), U( LDU, * ),
*      $                   V( LDU, * ), VP( * ), WORK( * ), Z( LDU, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZCHKST  checks the Hermitian eigenvalue problem routines.
*>
*>    ZHETRD factors A as  U S U* , where * means conjugate transpose,
*>    S is real symmetric tridiagonal, and U is unitary.
*>    ZHETRD can use either just the lower or just the upper triangle
*>    of A; ZCHKST checks both cases.
*>    U is represented as a product of Householder
*>    transformations, whose vectors are stored in the first
*>    n-1 columns of V, and whose scale factors are in TAU.
*>
*>    ZHPTRD does the same as ZHETRD, except that A and V are stored
*>    in "packed" format.
*>
*>    ZUNGTR constructs the matrix U from the contents of V and TAU.
*>
*>    ZUPGTR constructs the matrix U from the contents of VP and TAU.
*>
*>    ZSTEQR factors S as  Z D1 Z* , where Z is the unitary
*>    matrix of eigenvectors and D1 is a diagonal matrix with
*>    the eigenvalues on the diagonal.  D2 is the matrix of
*>    eigenvalues computed when Z is not computed.
*>
*>    DSTERF computes D3, the matrix of eigenvalues, by the
*>    PWK method, which does not yield eigenvectors.
*>
*>    ZPTEQR factors S as  Z4 D4 Z4* , for a
*>    Hermitian positive definite tridiagonal matrix.
*>    D5 is the matrix of eigenvalues computed when Z is not
*>    computed.
*>
*>    DSTEBZ computes selected eigenvalues.  WA1, WA2, and
*>    WA3 will denote eigenvalues computed to high
*>    absolute accuracy, with different range options.
*>    WR will denote eigenvalues computed to high relative
*>    accuracy.
*>
*>    ZSTEIN computes Y, the eigenvectors of S, given the
*>    eigenvalues.
*>
*>    ZSTEDC factors S as Z D1 Z* , where Z is the unitary
*>    matrix of eigenvectors and D1 is a diagonal matrix with
*>    the eigenvalues on the diagonal ('I' option). It may also
*>    update an input unitary matrix, usually the output
*>    from ZHETRD/ZUNGTR or ZHPTRD/ZUPGTR ('V' option). It may
*>    also just compute eigenvalues ('N' option).
*>
*>    ZSTEMR factors S as Z D1 Z* , where Z is the unitary
*>    matrix of eigenvectors and D1 is a diagonal matrix with
*>    the eigenvalues on the diagonal ('I' option).  ZSTEMR
*>    uses the Relatively Robust Representation whenever possible.
*>
*> When ZCHKST is called, a number of matrix "sizes" ("n's") and a
*> number of matrix "types" are specified.  For each size ("n")
*> and each type of matrix, one matrix will be generated and used
*> to test the Hermitian eigenroutines.  For each matrix, a number
*> of tests will be performed:
*>
*> (1)     | A - V S V* | / ( |A| n ulp ) ZHETRD( UPLO='U', ... )
*>
*> (2)     | I - UV* | / ( n ulp )        ZUNGTR( UPLO='U', ... )
*>
*> (3)     | A - V S V* | / ( |A| n ulp ) ZHETRD( UPLO='L', ... )
*>
*> (4)     | I - UV* | / ( n ulp )        ZUNGTR( UPLO='L', ... )
*>
*> (5-8)   Same as 1-4, but for ZHPTRD and ZUPGTR.
*>
*> (9)     | S - Z D Z* | / ( |S| n ulp ) ZSTEQR('V',...)
*>
*> (10)    | I - ZZ* | / ( n ulp )        ZSTEQR('V',...)
*>
*> (11)    | D1 - D2 | / ( |D1| ulp )        ZSTEQR('N',...)
*>
*> (12)    | D1 - D3 | / ( |D1| ulp )        DSTERF
*>
*> (13)    0 if the true eigenvalues (computed by sturm count)
*>         of S are within THRESH of
*>         those in D1.  2*THRESH if they are not.  (Tested using
*>         DSTECH)
*>
*> For S positive definite,
*>
*> (14)    | S - Z4 D4 Z4* | / ( |S| n ulp ) ZPTEQR('V',...)
*>
*> (15)    | I - Z4 Z4* | / ( n ulp )        ZPTEQR('V',...)
*>
*> (16)    | D4 - D5 | / ( 100 |D4| ulp )       ZPTEQR('N',...)
*>
*> When S is also diagonally dominant by the factor gamma < 1,
*>
*> (17)    max | D4(i) - WR(i) | / ( |D4(i)| omega ) ,
*>          i
*>         omega = 2 (2n-1) ULP (1 + 8 gamma**2) / (1 - gamma)**4
*>                                              DSTEBZ( 'A', 'E', ...)
*>
*> (18)    | WA1 - D3 | / ( |D3| ulp )          DSTEBZ( 'A', 'E', ...)
*>
*> (19)    ( max { min | WA2(i)-WA3(j) | } +
*>            i     j
*>           max { min | WA3(i)-WA2(j) | } ) / ( |D3| ulp )
*>            i     j
*>                                              DSTEBZ( 'I', 'E', ...)
*>
*> (20)    | S - Y WA1 Y* | / ( |S| n ulp )  DSTEBZ, ZSTEIN
*>
*> (21)    | I - Y Y* | / ( n ulp )          DSTEBZ, ZSTEIN
*>
*> (22)    | S - Z D Z* | / ( |S| n ulp )    ZSTEDC('I')
*>
*> (23)    | I - ZZ* | / ( n ulp )           ZSTEDC('I')
*>
*> (24)    | S - Z D Z* | / ( |S| n ulp )    ZSTEDC('V')
*>
*> (25)    | I - ZZ* | / ( n ulp )           ZSTEDC('V')
*>
*> (26)    | D1 - D2 | / ( |D1| ulp )           ZSTEDC('V') and
*>                                              ZSTEDC('N')
*>
*> Test 27 is disabled at the moment because ZSTEMR does not
*> guarantee high relatvie accuracy.
*>
*> (27)    max | D6(i) - WR(i) | / ( |D6(i)| omega ) ,
*>          i
*>         omega = 2 (2n-1) ULP (1 + 8 gamma**2) / (1 - gamma)**4
*>                                              ZSTEMR('V', 'A')
*>
*> (28)    max | D6(i) - WR(i) | / ( |D6(i)| omega ) ,
*>          i
*>         omega = 2 (2n-1) ULP (1 + 8 gamma**2) / (1 - gamma)**4
*>                                              ZSTEMR('V', 'I')
*>
*> Tests 29 through 34 are disable at present because ZSTEMR
*> does not handle partial specturm requests.
*>
*> (29)    | S - Z D Z* | / ( |S| n ulp )    ZSTEMR('V', 'I')
*>
*> (30)    | I - ZZ* | / ( n ulp )           ZSTEMR('V', 'I')
*>
*> (31)    ( max { min | WA2(i)-WA3(j) | } +
*>            i     j
*>           max { min | WA3(i)-WA2(j) | } ) / ( |D3| ulp )
*>            i     j
*>         ZSTEMR('N', 'I') vs. CSTEMR('V', 'I')
*>
*> (32)    | S - Z D Z* | / ( |S| n ulp )    ZSTEMR('V', 'V')
*>
*> (33)    | I - ZZ* | / ( n ulp )           ZSTEMR('V', 'V')
*>
*> (34)    ( max { min | WA2(i)-WA3(j) | } +
*>            i     j
*>           max { min | WA3(i)-WA2(j) | } ) / ( |D3| ulp )
*>            i     j
*>         ZSTEMR('N', 'V') vs. CSTEMR('V', 'V')
*>
*> (35)    | S - Z D Z* | / ( |S| n ulp )    ZSTEMR('V', 'A')
*>
*> (36)    | I - ZZ* | / ( n ulp )           ZSTEMR('V', 'A')
*>
*> (37)    ( max { min | WA2(i)-WA3(j) | } +
*>            i     j
*>           max { min | WA3(i)-WA2(j) | } ) / ( |D3| ulp )
*>            i     j
*>         ZSTEMR('N', 'A') vs. CSTEMR('V', 'A')
*>
*> The "sizes" are specified by an array NN(1:NSIZES); the value of
*> each element NN(j) specifies one size.
*> The "types" are specified by a logical array DOTYPE( 1:NTYPES );
*> if DOTYPE(j) is .TRUE., then matrix type "j" will be generated.
*> Currently, the list of possible types is:
*>
*> (1)  The zero matrix.
*> (2)  The identity matrix.
*>
*> (3)  A diagonal matrix with evenly spaced entries
*>      1, ..., ULP  and random signs.
*>      (ULP = (first number larger than 1) - 1 )
*> (4)  A diagonal matrix with geometrically spaced entries
*>      1, ..., ULP  and random signs.
*> (5)  A diagonal matrix with "clustered" entries 1, ULP, ..., ULP
*>      and random signs.
*>
*> (6)  Same as (4), but multiplied by SQRT( overflow threshold )
*> (7)  Same as (4), but multiplied by SQRT( underflow threshold )
*>
*> (8)  A matrix of the form  U* D U, where U is unitary and
*>      D has evenly spaced entries 1, ..., ULP with random signs
*>      on the diagonal.
*>
*> (9)  A matrix of the form  U* D U, where U is unitary and
*>      D has geometrically spaced entries 1, ..., ULP with random
*>      signs on the diagonal.
*>
*> (10) A matrix of the form  U* D U, where U is unitary and
*>      D has "clustered" entries 1, ULP,..., ULP with random
*>      signs on the diagonal.
*>
*> (11) Same as (8), but multiplied by SQRT( overflow threshold )
*> (12) Same as (8), but multiplied by SQRT( underflow threshold )
*>
*> (13) Hermitian matrix with random entries chosen from (-1,1).
*> (14) Same as (13), but multiplied by SQRT( overflow threshold )
*> (15) Same as (13), but multiplied by SQRT( underflow threshold )
*> (16) Same as (8), but diagonal elements are all positive.
*> (17) Same as (9), but diagonal elements are all positive.
*> (18) Same as (10), but diagonal elements are all positive.
*> (19) Same as (16), but multiplied by SQRT( overflow threshold )
*> (20) Same as (16), but multiplied by SQRT( underflow threshold )
*> (21) A diagonally dominant tridiagonal matrix with geometrically
*>      spaced diagonal entries 1, ..., ULP.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          ZCHKST does nothing.  It must be at least zero.
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
*>          The number of elements in DOTYPE.   If it is zero, ZCHKST
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
*>          next call to ZCHKST to continue the same random number
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
*>          (e.g., if a routine returns IINFO not equal to 0.)
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array of
*>                                  dimension ( LDA , max(NN) )
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually
*>          used.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at
*>          least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[out] AP
*> \verbatim
*>          AP is COMPLEX*16 array of
*>                      dimension( max(NN)*max(NN+1)/2 )
*>          The matrix A stored in packed format.
*> \endverbatim
*>
*> \param[out] SD
*> \verbatim
*>          SD is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The diagonal of the tridiagonal matrix computed by ZHETRD.
*>          On exit, SD and SE contain the tridiagonal form of the
*>          matrix in A.
*> \endverbatim
*>
*> \param[out] SE
*> \verbatim
*>          SE is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The off-diagonal of the tridiagonal matrix computed by
*>          ZHETRD.  On exit, SD and SE contain the tridiagonal form of
*>          the matrix in A.
*> \endverbatim
*>
*> \param[out] D1
*> \verbatim
*>          D1 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The eigenvalues of A, as computed by ZSTEQR simlutaneously
*>          with Z.  On exit, the eigenvalues in D1 correspond with the
*>          matrix in A.
*> \endverbatim
*>
*> \param[out] D2
*> \verbatim
*>          D2 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The eigenvalues of A, as computed by ZSTEQR if Z is not
*>          computed.  On exit, the eigenvalues in D2 correspond with
*>          the matrix in A.
*> \endverbatim
*>
*> \param[out] D3
*> \verbatim
*>          D3 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The eigenvalues of A, as computed by DSTERF.  On exit, the
*>          eigenvalues in D3 correspond with the matrix in A.
*> \endverbatim
*>
*> \param[out] D4
*> \verbatim
*>          D4 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The eigenvalues of A, as computed by ZPTEQR(V).
*>          ZPTEQR factors S as  Z4 D4 Z4*
*>          On exit, the eigenvalues in D4 correspond with the matrix in A.
*> \endverbatim
*>
*> \param[out] D5
*> \verbatim
*>          D5 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          The eigenvalues of A, as computed by ZPTEQR(N)
*>          when Z is not computed. On exit, the
*>          eigenvalues in D4 correspond with the matrix in A.
*> \endverbatim
*>
*> \param[out] WA1
*> \verbatim
*>          WA1 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          All eigenvalues of A, computed to high
*>          absolute accuracy, with different range options.
*>          as computed by DSTEBZ.
*> \endverbatim
*>
*> \param[out] WA2
*> \verbatim
*>          WA2 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          Selected eigenvalues of A, computed to high
*>          absolute accuracy, with different range options.
*>          as computed by DSTEBZ.
*>          Choose random values for IL and IU, and ask for the
*>          IL-th through IU-th eigenvalues.
*> \endverbatim
*>
*> \param[out] WA3
*> \verbatim
*>          WA3 is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          Selected eigenvalues of A, computed to high
*>          absolute accuracy, with different range options.
*>          as computed by DSTEBZ.
*>          Determine the values VL and VU of the IL-th and IU-th
*>          eigenvalues and ask for all eigenvalues in this range.
*> \endverbatim
*>
*> \param[out] WR
*> \verbatim
*>          WR is DOUBLE PRECISION array of
*>                             dimension( max(NN) )
*>          All eigenvalues of A, computed to high
*>          absolute accuracy, with different options.
*>          as computed by DSTEBZ.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX*16 array of
*>                             dimension( LDU, max(NN) ).
*>          The unitary matrix computed by ZHETRD + ZUNGTR.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U, Z, and V.  It must be at least 1
*>          and at least max( NN ).
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is COMPLEX*16 array of
*>                             dimension( LDU, max(NN) ).
*>          The Housholder vectors computed by ZHETRD in reducing A to
*>          tridiagonal form.  The vectors computed with UPLO='U' are
*>          in the upper triangle, and the vectors computed with UPLO='L'
*>          are in the lower triangle.  (As described in ZHETRD, the
*>          sub- and superdiagonal are not set to 1, although the
*>          true Householder vector has a 1 in that position.  The
*>          routines that use V, such as ZUNGTR, set those entries to
*>          1 before using them, and then restore them later.)
*> \endverbatim
*>
*> \param[out] VP
*> \verbatim
*>          VP is COMPLEX*16 array of
*>                      dimension( max(NN)*max(NN+1)/2 )
*>          The matrix V stored in packed format.
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array of
*>                             dimension( max(NN) )
*>          The Householder factors computed by ZHETRD in reducing A
*>          to tridiagonal form.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is COMPLEX*16 array of
*>                             dimension( LDU, max(NN) ).
*>          The unitary matrix of eigenvectors computed by ZSTEQR,
*>          ZPTEQR, and ZSTEIN.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array of
*>                      dimension( LWORK )
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The number of entries in WORK.  This must be at least
*>          1 + 4 * Nmax + 2 * Nmax * lg Nmax + 3 * Nmax**2
*>          where Nmax = max( NN(j), 2 ) and lg = log base 2.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array,
*>          Workspace.
*> \endverbatim
*>
*> \param[out] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The number of entries in IWORK.  This must be at least
*>                  6 + 6*Nmax + 5 * Nmax * lg Nmax
*>          where Nmax = max( NN(j), 2 ) and lg = log base 2.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array
*> \endverbatim
*>
*> \param[in] LRWORK
*> \verbatim
*>          LRWORK is INTEGER
*>          The number of entries in LRWORK (dimension( ??? )
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (26)
*>          The values computed by the tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          If 0, then everything ran OK.
*>           -1: NSIZES < 0
*>           -2: Some NN(j) < 0
*>           -3: NTYPES < 0
*>           -5: THRESH < 0
*>           -9: LDA < 1 or LDA < NMAX, where NMAX is max( NN(j) ).
*>          -23: LDU < 1 or LDU < NMAX.
*>          -29: LWORK too small.
*>          If  ZLATMR, CLATMS, ZHETRD, ZUNGTR, ZSTEQR, DSTERF,
*>              or ZUNMC2 returns an error code, the
*>              absolute value of it is returned.
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
*>       NBLOCK          Blocksize as returned by ENVIR.
*>       NMAX            Largest value in NN.
*>       NMATS           The number of matrices generated so far.
*>       NERRS           The number of tests which have exceeded THRESH
*>                       so far.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZCHKST( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NOUNIT, A, LDA, AP, SD, SE, D1, D2, D3, D4, D5,
     $                   WA1, WA2, WA3, WR, U, LDU, V, VP, TAU, Z, WORK,
     $                   LWORK, RWORK, LRWORK, IWORK, LIWORK, RESULT,
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDU, LIWORK, LRWORK, LWORK, NOUNIT,
     $                   NSIZES, NTYPES
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), IWORK( * ), NN( * )
      DOUBLE PRECISION   D1( * ), D2( * ), D3( * ), D4( * ), D5( * ),
     $                   RESULT( * ), RWORK( * ), SD( * ), SE( * ),
     $                   WA1( * ), WA2( * ), WA3( * ), WR( * )
      COMPLEX*16         A( LDA, * ), AP( * ), TAU( * ), U( LDU, * ),
     $                   V( LDU, * ), VP( * ), WORK( * ), Z( LDU, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO, EIGHT, TEN, HUN
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TWO = 2.0D0,
     $                   EIGHT = 8.0D0, TEN = 10.0D0, HUN = 100.0D0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
      DOUBLE PRECISION   HALF
      PARAMETER          ( HALF = ONE / TWO )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 21 )
      LOGICAL            CRANGE
      PARAMETER          ( CRANGE = .FALSE. )
      LOGICAL            CREL
      PARAMETER          ( CREL = .FALSE. )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN, TRYRAC
      INTEGER            I, IINFO, IL, IMODE, INDE, INDRWK, ITEMP,
     $                   ITYPE, IU, J, JC, JR, JSIZE, JTYPE, LGN,
     $                   LIWEDC, LOG2UI, LRWEDC, LWEDC, M, M2, M3,
     $                   MTYPES, N, NAP, NBLOCK, NERRS, NMATS, NMAX,
     $                   NSPLIT, NTEST, NTESTT
      DOUBLE PRECISION   ABSTOL, ANINV, ANORM, COND, OVFL, RTOVFL,
     $                   RTUNFL, TEMP1, TEMP2, TEMP3, TEMP4, ULP,
     $                   ULPINV, UNFL, VL, VU
*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), ISEED2( 4 ),
     $                   KMAGN( MAXTYP ), KMODE( MAXTYP ),
     $                   KTYPE( MAXTYP )
      DOUBLE PRECISION   DUMMA( 1 )
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, DLARND, DSXT1
      EXTERNAL           ILAENV, DLAMCH, DLARND, DSXT1
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLABAD, DLASUM, DSTEBZ, DSTECH, DSTERF,
     $                   XERBLA, ZCOPY, ZHET21, ZHETRD, ZHPT21, ZHPTRD,
     $                   ZLACPY, ZLASET, ZLATMR, ZLATMS, ZPTEQR, ZSTEDC,
     $                   ZSTEMR, ZSTEIN, ZSTEQR, ZSTT21, ZSTT22, ZUNGTR,
     $                   ZUPGTR
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, INT, LOG, MAX, MIN, SQRT
*     ..
*     .. Data statements ..
      DATA               KTYPE / 1, 2, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 8,
     $                   8, 8, 9, 9, 9, 9, 9, 10 /
      DATA               KMAGN / 1, 1, 1, 1, 1, 2, 3, 1, 1, 1, 2, 3, 1,
     $                   2, 3, 1, 1, 1, 2, 3, 1 /
      DATA               KMODE / 0, 0, 4, 3, 1, 4, 4, 4, 3, 1, 4, 4, 0,
     $                   0, 0, 4, 3, 1, 4, 4, 3 /
*     ..
*     .. Executable Statements ..
*
*     Keep ftnchek happy
      IDUMMA( 1 ) = 1
*
*     Check for errors
*
      NTESTT = 0
      INFO = 0
*
*     Important constants
*
      BADNN = .FALSE.
      TRYRAC = .TRUE.
      NMAX = 1
      DO 10 J = 1, NSIZES
         NMAX = MAX( NMAX, NN( J ) )
         IF( NN( J ).LT.0 )
     $      BADNN = .TRUE.
   10 CONTINUE
*
      NBLOCK = ILAENV( 1, 'ZHETRD', 'L', NMAX, -1, -1, -1 )
      NBLOCK = MIN( NMAX, MAX( 1, NBLOCK ) )
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
         INFO = -23
      ELSE IF( 2*MAX( 2, NMAX )**2.GT.LWORK ) THEN
         INFO = -29
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZCHKST', -INFO )
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
      UNFL = DLAMCH( 'Safe minimum' )
      OVFL = ONE / UNFL
      CALL DLABAD( UNFL, OVFL )
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
      ULPINV = ONE / ULP
      LOG2UI = INT( LOG( ULPINV ) / LOG( TWO ) )
      RTUNFL = SQRT( UNFL )
      RTOVFL = SQRT( OVFL )
*
*     Loop over sizes, types
*
      DO 20 I = 1, 4
         ISEED2( I ) = ISEED( I )
   20 CONTINUE
      NERRS = 0
      NMATS = 0
*
      DO 310 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         IF( N.GT.0 ) THEN
            LGN = INT( LOG( DBLE( N ) ) / LOG( TWO ) )
            IF( 2**LGN.LT.N )
     $         LGN = LGN + 1
            IF( 2**LGN.LT.N )
     $         LGN = LGN + 1
            LWEDC = 1 + 4*N + 2*N*LGN + 4*N**2
            LRWEDC = 1 + 3*N + 2*N*LGN + 4*N**2
            LIWEDC = 6 + 6*N + 5*N*LGN
         ELSE
            LWEDC = 8
            LRWEDC = 7
            LIWEDC = 12
         END IF
         NAP = ( N*( N+1 ) ) / 2
         ANINV = ONE / DBLE( MAX( 1, N ) )
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 300 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 300
            NMATS = NMATS + 1
            NTEST = 0
*
            DO 30 J = 1, 4
               IOLDSD( J ) = ISEED( J )
   30       CONTINUE
*
*           Compute "A"
*
*           Control parameters:
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
*           =9                      positive definite
*           =10                     diagonally dominant tridiagonal
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
            IF( JTYPE.LE.15 ) THEN
               COND = ULPINV
            ELSE
               COND = ULPINV*ANINV / TEN
            END IF
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
               DO 80 JC = 1, N
                  A( JC, JC ) = ANORM
   80          CONTINUE
*
            ELSE IF( ITYPE.EQ.4 ) THEN
*
*              Diagonal Matrix, [Eigen]values Specified
*
               CALL ZLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, 0, 0, 'N', A, LDA, WORK, IINFO )
*
*
            ELSE IF( ITYPE.EQ.5 ) THEN
*
*              Hermitian, eigenvalues specified
*
               CALL ZLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE, COND,
     $                      ANORM, N, N, 'N', A, LDA, WORK, IINFO )
*
            ELSE IF( ITYPE.EQ.7 ) THEN
*
*              Diagonal, random eigenvalues
*
               CALL ZLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.8 ) THEN
*
*              Hermitian, random eigenvalues
*
               CALL ZLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE, CONE,
     $                      'T', 'N', WORK( N+1 ), 1, ONE,
     $                      WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, N, N,
     $                      ZERO, ANORM, 'NO', A, LDA, IWORK, IINFO )
*
            ELSE IF( ITYPE.EQ.9 ) THEN
*
*              Positive definite, eigenvalues specified.
*
               CALL ZLATMS( N, N, 'S', ISEED, 'P', RWORK, IMODE, COND,
     $                      ANORM, N, N, 'N', A, LDA, WORK, IINFO )
*
            ELSE IF( ITYPE.EQ.10 ) THEN
*
*              Positive definite tridiagonal, eigenvalues specified.
*
               CALL ZLATMS( N, N, 'S', ISEED, 'P', RWORK, IMODE, COND,
     $                      ANORM, 1, 1, 'N', A, LDA, WORK, IINFO )
               DO 90 I = 2, N
                  TEMP1 = ABS( A( I-1, I ) )
                  TEMP2 = SQRT( ABS( A( I-1, I-1 )*A( I, I ) ) )
                  IF( TEMP1.GT.HALF*TEMP2 ) THEN
                     A( I-1, I ) = A( I-1, I )*
     $                             ( HALF*TEMP2 / ( UNFL+TEMP1 ) )
                     A( I, I-1 ) = DCONJG( A( I-1, I ) )
                  END IF
   90          CONTINUE
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
*           Call ZHETRD and ZUNGTR to compute S and U from
*           upper triangle.
*
            CALL ZLACPY( 'U', N, N, A, LDA, V, LDU )
*
            NTEST = 1
            CALL ZHETRD( 'U', N, V, LDU, SD, SE, TAU, WORK, LWORK,
     $                   IINFO )
*
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHETRD(U)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 1 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            CALL ZLACPY( 'U', N, N, V, LDU, U, LDU )
*
            NTEST = 2
            CALL ZUNGTR( 'U', N, U, LDU, TAU, WORK, LWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUNGTR(U)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 2 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do tests 1 and 2
*
            CALL ZHET21( 2, 'Upper', N, 1, A, LDA, SD, SE, U, LDU, V,
     $                   LDU, TAU, WORK, RWORK, RESULT( 1 ) )
            CALL ZHET21( 3, 'Upper', N, 1, A, LDA, SD, SE, U, LDU, V,
     $                   LDU, TAU, WORK, RWORK, RESULT( 2 ) )
*
*           Call ZHETRD and ZUNGTR to compute S and U from
*           lower triangle, do tests.
*
            CALL ZLACPY( 'L', N, N, A, LDA, V, LDU )
*
            NTEST = 3
            CALL ZHETRD( 'L', N, V, LDU, SD, SE, TAU, WORK, LWORK,
     $                   IINFO )
*
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHETRD(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 3 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            CALL ZLACPY( 'L', N, N, V, LDU, U, LDU )
*
            NTEST = 4
            CALL ZUNGTR( 'L', N, U, LDU, TAU, WORK, LWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUNGTR(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 4 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            CALL ZHET21( 2, 'Lower', N, 1, A, LDA, SD, SE, U, LDU, V,
     $                   LDU, TAU, WORK, RWORK, RESULT( 3 ) )
            CALL ZHET21( 3, 'Lower', N, 1, A, LDA, SD, SE, U, LDU, V,
     $                   LDU, TAU, WORK, RWORK, RESULT( 4 ) )
*
*           Store the upper triangle of A in AP
*
            I = 0
            DO 120 JC = 1, N
               DO 110 JR = 1, JC
                  I = I + 1
                  AP( I ) = A( JR, JC )
  110          CONTINUE
  120       CONTINUE
*
*           Call ZHPTRD and ZUPGTR to compute S and U from AP
*
            CALL ZCOPY( NAP, AP, 1, VP, 1 )
*
            NTEST = 5
            CALL ZHPTRD( 'U', N, VP, SD, SE, TAU, IINFO )
*
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHPTRD(U)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 5 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            NTEST = 6
            CALL ZUPGTR( 'U', N, VP, TAU, U, LDU, WORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUPGTR(U)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 6 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do tests 5 and 6
*
            CALL ZHPT21( 2, 'Upper', N, 1, AP, SD, SE, U, LDU, VP, TAU,
     $                   WORK, RWORK, RESULT( 5 ) )
            CALL ZHPT21( 3, 'Upper', N, 1, AP, SD, SE, U, LDU, VP, TAU,
     $                   WORK, RWORK, RESULT( 6 ) )
*
*           Store the lower triangle of A in AP
*
            I = 0
            DO 140 JC = 1, N
               DO 130 JR = JC, N
                  I = I + 1
                  AP( I ) = A( JR, JC )
  130          CONTINUE
  140       CONTINUE
*
*           Call ZHPTRD and ZUPGTR to compute S and U from AP
*
            CALL ZCOPY( NAP, AP, 1, VP, 1 )
*
            NTEST = 7
            CALL ZHPTRD( 'L', N, VP, SD, SE, TAU, IINFO )
*
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHPTRD(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 7 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            NTEST = 8
            CALL ZUPGTR( 'L', N, VP, TAU, U, LDU, WORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUPGTR(L)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 8 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            CALL ZHPT21( 2, 'Lower', N, 1, AP, SD, SE, U, LDU, VP, TAU,
     $                   WORK, RWORK, RESULT( 7 ) )
            CALL ZHPT21( 3, 'Lower', N, 1, AP, SD, SE, U, LDU, VP, TAU,
     $                   WORK, RWORK, RESULT( 8 ) )
*
*           Call ZSTEQR to compute D1, D2, and Z, do tests.
*
*           Compute D1 and Z
*
            CALL DCOPY( N, SD, 1, D1, 1 )
            IF( N.GT.0 )
     $         CALL DCOPY( N-1, SE, 1, RWORK, 1 )
            CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
            NTEST = 9
            CALL ZSTEQR( 'V', N, D1, RWORK, Z, LDU, RWORK( N+1 ),
     $                   IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZSTEQR(V)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 9 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Compute D2
*
            CALL DCOPY( N, SD, 1, D2, 1 )
            IF( N.GT.0 )
     $         CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
            NTEST = 11
            CALL ZSTEQR( 'N', N, D2, RWORK, WORK, LDU, RWORK( N+1 ),
     $                   IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZSTEQR(N)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 11 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Compute D3 (using PWK method)
*
            CALL DCOPY( N, SD, 1, D3, 1 )
            IF( N.GT.0 )
     $         CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
            NTEST = 12
            CALL DSTERF( N, D3, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'DSTERF', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 12 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do Tests 9 and 10
*
            CALL ZSTT21( N, 0, SD, SE, D1, DUMMA, Z, LDU, WORK, RWORK,
     $                   RESULT( 9 ) )
*
*           Do Tests 11 and 12
*
            TEMP1 = ZERO
            TEMP2 = ZERO
            TEMP3 = ZERO
            TEMP4 = ZERO
*
            DO 150 J = 1, N
               TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D2( J ) ) )
               TEMP2 = MAX( TEMP2, ABS( D1( J )-D2( J ) ) )
               TEMP3 = MAX( TEMP3, ABS( D1( J ) ), ABS( D3( J ) ) )
               TEMP4 = MAX( TEMP4, ABS( D1( J )-D3( J ) ) )
  150       CONTINUE
*
            RESULT( 11 ) = TEMP2 / MAX( UNFL, ULP*MAX( TEMP1, TEMP2 ) )
            RESULT( 12 ) = TEMP4 / MAX( UNFL, ULP*MAX( TEMP3, TEMP4 ) )
*
*           Do Test 13 -- Sturm Sequence Test of Eigenvalues
*                         Go up by factors of two until it succeeds
*
            NTEST = 13
            TEMP1 = THRESH*( HALF-ULP )
*
            DO 160 J = 0, LOG2UI
               CALL DSTECH( N, SD, SE, D1, TEMP1, RWORK, IINFO )
               IF( IINFO.EQ.0 )
     $            GO TO 170
               TEMP1 = TEMP1*TWO
  160       CONTINUE
*
  170       CONTINUE
            RESULT( 13 ) = TEMP1
*
*           For positive definite matrices ( JTYPE.GT.15 ) call ZPTEQR
*           and do tests 14, 15, and 16 .
*
            IF( JTYPE.GT.15 ) THEN
*
*              Compute D4 and Z4
*
               CALL DCOPY( N, SD, 1, D4, 1 )
               IF( N.GT.0 )
     $            CALL DCOPY( N-1, SE, 1, RWORK, 1 )
               CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
               NTEST = 14
               CALL ZPTEQR( 'V', N, D4, RWORK, Z, LDU, RWORK( N+1 ),
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'ZPTEQR(V)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 14 ) = ULPINV
                     GO TO 280
                  END IF
               END IF
*
*              Do Tests 14 and 15
*
               CALL ZSTT21( N, 0, SD, SE, D4, DUMMA, Z, LDU, WORK,
     $                      RWORK, RESULT( 14 ) )
*
*              Compute D5
*
               CALL DCOPY( N, SD, 1, D5, 1 )
               IF( N.GT.0 )
     $            CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
               NTEST = 16
               CALL ZPTEQR( 'N', N, D5, RWORK, Z, LDU, RWORK( N+1 ),
     $                      IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'ZPTEQR(N)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 16 ) = ULPINV
                     GO TO 280
                  END IF
               END IF
*
*              Do Test 16
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 180 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D4( J ) ), ABS( D5( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D4( J )-D5( J ) ) )
  180          CONTINUE
*
               RESULT( 16 ) = TEMP2 / MAX( UNFL,
     $                        HUN*ULP*MAX( TEMP1, TEMP2 ) )
            ELSE
               RESULT( 14 ) = ZERO
               RESULT( 15 ) = ZERO
               RESULT( 16 ) = ZERO
            END IF
*
*           Call DSTEBZ with different options and do tests 17-18.
*
*              If S is positive definite and diagonally dominant,
*              ask for all eigenvalues with high relative accuracy.
*
            VL = ZERO
            VU = ZERO
            IL = 0
            IU = 0
            IF( JTYPE.EQ.21 ) THEN
               NTEST = 17
               ABSTOL = UNFL + UNFL
               CALL DSTEBZ( 'A', 'E', N, VL, VU, IL, IU, ABSTOL, SD, SE,
     $                      M, NSPLIT, WR, IWORK( 1 ), IWORK( N+1 ),
     $                      RWORK, IWORK( 2*N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'DSTEBZ(A,rel)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 17 ) = ULPINV
                     GO TO 280
                  END IF
               END IF
*
*              Do test 17
*
               TEMP2 = TWO*( TWO*N-ONE )*ULP*( ONE+EIGHT*HALF**2 ) /
     $                 ( ONE-HALF )**4
*
               TEMP1 = ZERO
               DO 190 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D4( J )-WR( N-J+1 ) ) /
     $                    ( ABSTOL+ABS( D4( J ) ) ) )
  190          CONTINUE
*
               RESULT( 17 ) = TEMP1 / TEMP2
            ELSE
               RESULT( 17 ) = ZERO
            END IF
*
*           Now ask for all eigenvalues with high absolute accuracy.
*
            NTEST = 18
            ABSTOL = UNFL + UNFL
            CALL DSTEBZ( 'A', 'E', N, VL, VU, IL, IU, ABSTOL, SD, SE, M,
     $                   NSPLIT, WA1, IWORK( 1 ), IWORK( N+1 ), RWORK,
     $                   IWORK( 2*N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'DSTEBZ(A)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 18 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do test 18
*
            TEMP1 = ZERO
            TEMP2 = ZERO
            DO 200 J = 1, N
               TEMP1 = MAX( TEMP1, ABS( D3( J ) ), ABS( WA1( J ) ) )
               TEMP2 = MAX( TEMP2, ABS( D3( J )-WA1( J ) ) )
  200       CONTINUE
*
            RESULT( 18 ) = TEMP2 / MAX( UNFL, ULP*MAX( TEMP1, TEMP2 ) )
*
*           Choose random values for IL and IU, and ask for the
*           IL-th through IU-th eigenvalues.
*
            NTEST = 19
            IF( N.LE.1 ) THEN
               IL = 1
               IU = N
            ELSE
               IL = 1 + ( N-1 )*INT( DLARND( 1, ISEED2 ) )
               IU = 1 + ( N-1 )*INT( DLARND( 1, ISEED2 ) )
               IF( IU.LT.IL ) THEN
                  ITEMP = IU
                  IU = IL
                  IL = ITEMP
               END IF
            END IF
*
            CALL DSTEBZ( 'I', 'E', N, VL, VU, IL, IU, ABSTOL, SD, SE,
     $                   M2, NSPLIT, WA2, IWORK( 1 ), IWORK( N+1 ),
     $                   RWORK, IWORK( 2*N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'DSTEBZ(I)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 19 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Determine the values VL and VU of the IL-th and IU-th
*           eigenvalues and ask for all eigenvalues in this range.
*
            IF( N.GT.0 ) THEN
               IF( IL.NE.1 ) THEN
                  VL = WA1( IL ) - MAX( HALF*( WA1( IL )-WA1( IL-1 ) ),
     $                 ULP*ANORM, TWO*RTUNFL )
               ELSE
                  VL = WA1( 1 ) - MAX( HALF*( WA1( N )-WA1( 1 ) ),
     $                 ULP*ANORM, TWO*RTUNFL )
               END IF
               IF( IU.NE.N ) THEN
                  VU = WA1( IU ) + MAX( HALF*( WA1( IU+1 )-WA1( IU ) ),
     $                 ULP*ANORM, TWO*RTUNFL )
               ELSE
                  VU = WA1( N ) + MAX( HALF*( WA1( N )-WA1( 1 ) ),
     $                 ULP*ANORM, TWO*RTUNFL )
               END IF
            ELSE
               VL = ZERO
               VU = ONE
            END IF
*
            CALL DSTEBZ( 'V', 'E', N, VL, VU, IL, IU, ABSTOL, SD, SE,
     $                   M3, NSPLIT, WA3, IWORK( 1 ), IWORK( N+1 ),
     $                   RWORK, IWORK( 2*N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'DSTEBZ(V)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 19 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            IF( M3.EQ.0 .AND. N.NE.0 ) THEN
               RESULT( 19 ) = ULPINV
               GO TO 280
            END IF
*
*           Do test 19
*
            TEMP1 = DSXT1( 1, WA2, M2, WA3, M3, ABSTOL, ULP, UNFL )
            TEMP2 = DSXT1( 1, WA3, M3, WA2, M2, ABSTOL, ULP, UNFL )
            IF( N.GT.0 ) THEN
               TEMP3 = MAX( ABS( WA1( N ) ), ABS( WA1( 1 ) ) )
            ELSE
               TEMP3 = ZERO
            END IF
*
            RESULT( 19 ) = ( TEMP1+TEMP2 ) / MAX( UNFL, TEMP3*ULP )
*
*           Call ZSTEIN to compute eigenvectors corresponding to
*           eigenvalues in WA1.  (First call DSTEBZ again, to make sure
*           it returns these eigenvalues in the correct order.)
*
            NTEST = 21
            CALL DSTEBZ( 'A', 'B', N, VL, VU, IL, IU, ABSTOL, SD, SE, M,
     $                   NSPLIT, WA1, IWORK( 1 ), IWORK( N+1 ), RWORK,
     $                   IWORK( 2*N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'DSTEBZ(A,B)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 20 ) = ULPINV
                  RESULT( 21 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
            CALL ZSTEIN( N, SD, SE, M, WA1, IWORK( 1 ), IWORK( N+1 ), Z,
     $                   LDU, RWORK, IWORK( 2*N+1 ), IWORK( 3*N+1 ),
     $                   IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZSTEIN', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 20 ) = ULPINV
                  RESULT( 21 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do tests 20 and 21
*
            CALL ZSTT21( N, 0, SD, SE, WA1, DUMMA, Z, LDU, WORK, RWORK,
     $                   RESULT( 20 ) )
*
*           Call ZSTEDC(I) to compute D1 and Z, do tests.
*
*           Compute D1 and Z
*
            INDE = 1
            INDRWK = INDE + N
            CALL DCOPY( N, SD, 1, D1, 1 )
            IF( N.GT.0 )
     $         CALL DCOPY( N-1, SE, 1, RWORK( INDE ), 1 )
            CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
            NTEST = 22
            CALL ZSTEDC( 'I', N, D1, RWORK( INDE ), Z, LDU, WORK, LWEDC,
     $                   RWORK( INDRWK ), LRWEDC, IWORK, LIWEDC, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZSTEDC(I)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 22 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do Tests 22 and 23
*
            CALL ZSTT21( N, 0, SD, SE, D1, DUMMA, Z, LDU, WORK, RWORK,
     $                   RESULT( 22 ) )
*
*           Call ZSTEDC(V) to compute D1 and Z, do tests.
*
*           Compute D1 and Z
*
            CALL DCOPY( N, SD, 1, D1, 1 )
            IF( N.GT.0 )
     $         CALL DCOPY( N-1, SE, 1, RWORK( INDE ), 1 )
            CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
            NTEST = 24
            CALL ZSTEDC( 'V', N, D1, RWORK( INDE ), Z, LDU, WORK, LWEDC,
     $                   RWORK( INDRWK ), LRWEDC, IWORK, LIWEDC, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZSTEDC(V)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 24 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do Tests 24 and 25
*
            CALL ZSTT21( N, 0, SD, SE, D1, DUMMA, Z, LDU, WORK, RWORK,
     $                   RESULT( 24 ) )
*
*           Call ZSTEDC(N) to compute D2, do tests.
*
*           Compute D2
*
            CALL DCOPY( N, SD, 1, D2, 1 )
            IF( N.GT.0 )
     $         CALL DCOPY( N-1, SE, 1, RWORK( INDE ), 1 )
            CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
            NTEST = 26
            CALL ZSTEDC( 'N', N, D2, RWORK( INDE ), Z, LDU, WORK, LWEDC,
     $                   RWORK( INDRWK ), LRWEDC, IWORK, LIWEDC, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZSTEDC(N)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               IF( IINFO.LT.0 ) THEN
                  RETURN
               ELSE
                  RESULT( 26 ) = ULPINV
                  GO TO 280
               END IF
            END IF
*
*           Do Test 26
*
            TEMP1 = ZERO
            TEMP2 = ZERO
*
            DO 210 J = 1, N
               TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D2( J ) ) )
               TEMP2 = MAX( TEMP2, ABS( D1( J )-D2( J ) ) )
  210       CONTINUE
*
            RESULT( 26 ) = TEMP2 / MAX( UNFL, ULP*MAX( TEMP1, TEMP2 ) )
*
*           Only test ZSTEMR if IEEE compliant
*
            IF( ILAENV( 10, 'ZSTEMR', 'VA', 1, 0, 0, 0 ).EQ.1 .AND.
     $          ILAENV( 11, 'ZSTEMR', 'VA', 1, 0, 0, 0 ).EQ.1 ) THEN
*
*           Call ZSTEMR, do test 27 (relative eigenvalue accuracy)
*
*              If S is positive definite and diagonally dominant,
*              ask for all eigenvalues with high relative accuracy.
*
               VL = ZERO
               VU = ZERO
               IL = 0
               IU = 0
               IF( JTYPE.EQ.21 .AND. CREL ) THEN
                  NTEST = 27
                  ABSTOL = UNFL + UNFL
                  CALL ZSTEMR( 'V', 'A', N, SD, SE, VL, VU, IL, IU,
     $                         M, WR, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                         RWORK, LRWORK, IWORK( 2*N+1 ), LWORK-2*N,
     $                         IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(V,A,rel)',
     $                  IINFO, N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( 27 ) = ULPINV
                        GO TO 270
                     END IF
                  END IF
*
*              Do test 27
*
                  TEMP2 = TWO*( TWO*N-ONE )*ULP*( ONE+EIGHT*HALF**2 ) /
     $                    ( ONE-HALF )**4
*
                  TEMP1 = ZERO
                  DO 220 J = 1, N
                     TEMP1 = MAX( TEMP1, ABS( D4( J )-WR( N-J+1 ) ) /
     $                       ( ABSTOL+ABS( D4( J ) ) ) )
  220             CONTINUE
*
                  RESULT( 27 ) = TEMP1 / TEMP2
*
                  IL = 1 + ( N-1 )*INT( DLARND( 1, ISEED2 ) )
                  IU = 1 + ( N-1 )*INT( DLARND( 1, ISEED2 ) )
                  IF( IU.LT.IL ) THEN
                     ITEMP = IU
                     IU = IL
                     IL = ITEMP
                  END IF
*
                  IF( CRANGE ) THEN
                     NTEST = 28
                     ABSTOL = UNFL + UNFL
                     CALL ZSTEMR( 'V', 'I', N, SD, SE, VL, VU, IL, IU,
     $                            M, WR, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                            RWORK, LRWORK, IWORK( 2*N+1 ),
     $                            LWORK-2*N, IINFO )
*
                     IF( IINFO.NE.0 ) THEN
                        WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(V,I,rel)',
     $                     IINFO, N, JTYPE, IOLDSD
                        INFO = ABS( IINFO )
                        IF( IINFO.LT.0 ) THEN
                           RETURN
                        ELSE
                           RESULT( 28 ) = ULPINV
                           GO TO 270
                        END IF
                     END IF
*
*
*                 Do test 28
*
                     TEMP2 = TWO*( TWO*N-ONE )*ULP*
     $                       ( ONE+EIGHT*HALF**2 ) / ( ONE-HALF )**4
*
                     TEMP1 = ZERO
                     DO 230 J = IL, IU
                        TEMP1 = MAX( TEMP1, ABS( WR( J-IL+1 )-D4( N-J+
     $                          1 ) ) / ( ABSTOL+ABS( WR( J-IL+1 ) ) ) )
  230                CONTINUE
*
                     RESULT( 28 ) = TEMP1 / TEMP2
                  ELSE
                     RESULT( 28 ) = ZERO
                  END IF
               ELSE
                  RESULT( 27 ) = ZERO
                  RESULT( 28 ) = ZERO
               END IF
*
*           Call ZSTEMR(V,I) to compute D1 and Z, do tests.
*
*           Compute D1 and Z
*
               CALL DCOPY( N, SD, 1, D5, 1 )
               IF( N.GT.0 )
     $            CALL DCOPY( N-1, SE, 1, RWORK, 1 )
               CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
               IF( CRANGE ) THEN
                  NTEST = 29
                  IL = 1 + ( N-1 )*INT( DLARND( 1, ISEED2 ) )
                  IU = 1 + ( N-1 )*INT( DLARND( 1, ISEED2 ) )
                  IF( IU.LT.IL ) THEN
                     ITEMP = IU
                     IU = IL
                     IL = ITEMP
                  END IF
                  CALL ZSTEMR( 'V', 'I', N, D5, RWORK, VL, VU, IL, IU,
     $                         M, D1, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                         RWORK( N+1 ), LRWORK-N, IWORK( 2*N+1 ),
     $                         LIWORK-2*N, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(V,I)', IINFO,
     $                  N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( 29 ) = ULPINV
                        GO TO 280
                     END IF
                  END IF
*
*           Do Tests 29 and 30
*
*
*           Call ZSTEMR to compute D2, do tests.
*
*           Compute D2
*
                  CALL DCOPY( N, SD, 1, D5, 1 )
                  IF( N.GT.0 )
     $               CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
                  NTEST = 31
                  CALL ZSTEMR( 'N', 'I', N, D5, RWORK, VL, VU, IL, IU,
     $                         M, D2, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                         RWORK( N+1 ), LRWORK-N, IWORK( 2*N+1 ),
     $                         LIWORK-2*N, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(N,I)', IINFO,
     $                  N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( 31 ) = ULPINV
                        GO TO 280
                     END IF
                  END IF
*
*           Do Test 31
*
                  TEMP1 = ZERO
                  TEMP2 = ZERO
*
                  DO 240 J = 1, IU - IL + 1
                     TEMP1 = MAX( TEMP1, ABS( D1( J ) ),
     $                       ABS( D2( J ) ) )
                     TEMP2 = MAX( TEMP2, ABS( D1( J )-D2( J ) ) )
  240             CONTINUE
*
                  RESULT( 31 ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
*
*
*           Call ZSTEMR(V,V) to compute D1 and Z, do tests.
*
*           Compute D1 and Z
*
                  CALL DCOPY( N, SD, 1, D5, 1 )
                  IF( N.GT.0 )
     $               CALL DCOPY( N-1, SE, 1, RWORK, 1 )
                  CALL ZLASET( 'Full', N, N, CZERO, CONE, Z, LDU )
*
                  NTEST = 32
*
                  IF( N.GT.0 ) THEN
                     IF( IL.NE.1 ) THEN
                        VL = D2( IL ) - MAX( HALF*
     $                       ( D2( IL )-D2( IL-1 ) ), ULP*ANORM,
     $                       TWO*RTUNFL )
                     ELSE
                        VL = D2( 1 ) - MAX( HALF*( D2( N )-D2( 1 ) ),
     $                       ULP*ANORM, TWO*RTUNFL )
                     END IF
                     IF( IU.NE.N ) THEN
                        VU = D2( IU ) + MAX( HALF*
     $                       ( D2( IU+1 )-D2( IU ) ), ULP*ANORM,
     $                       TWO*RTUNFL )
                     ELSE
                        VU = D2( N ) + MAX( HALF*( D2( N )-D2( 1 ) ),
     $                       ULP*ANORM, TWO*RTUNFL )
                     END IF
                  ELSE
                     VL = ZERO
                     VU = ONE
                  END IF
*
                  CALL ZSTEMR( 'V', 'V', N, D5, RWORK, VL, VU, IL, IU,
     $                         M, D1, Z, LDU, M, IWORK( 1 ), TRYRAC,
     $                         RWORK( N+1 ), LRWORK-N, IWORK( 2*N+1 ),
     $                         LIWORK-2*N, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(V,V)', IINFO,
     $                  N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( 32 ) = ULPINV
                        GO TO 280
                     END IF
                  END IF
*
*           Do Tests 32 and 33
*
                  CALL ZSTT22( N, M, 0, SD, SE, D1, DUMMA, Z, LDU, WORK,
     $                         M, RWORK, RESULT( 32 ) )
*
*           Call ZSTEMR to compute D2, do tests.
*
*           Compute D2
*
                  CALL DCOPY( N, SD, 1, D5, 1 )
                  IF( N.GT.0 )
     $               CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
                  NTEST = 34
                  CALL ZSTEMR( 'N', 'V', N, D5, RWORK, VL, VU, IL, IU,
     $                         M, D2, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                         RWORK( N+1 ), LRWORK-N, IWORK( 2*N+1 ),
     $                         LIWORK-2*N, IINFO )
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(N,V)', IINFO,
     $                  N, JTYPE, IOLDSD
                     INFO = ABS( IINFO )
                     IF( IINFO.LT.0 ) THEN
                        RETURN
                     ELSE
                        RESULT( 34 ) = ULPINV
                        GO TO 280
                     END IF
                  END IF
*
*           Do Test 34
*
                  TEMP1 = ZERO
                  TEMP2 = ZERO
*
                  DO 250 J = 1, IU - IL + 1
                     TEMP1 = MAX( TEMP1, ABS( D1( J ) ),
     $                       ABS( D2( J ) ) )
                     TEMP2 = MAX( TEMP2, ABS( D1( J )-D2( J ) ) )
  250             CONTINUE
*
                  RESULT( 34 ) = TEMP2 / MAX( UNFL,
     $                           ULP*MAX( TEMP1, TEMP2 ) )
               ELSE
                  RESULT( 29 ) = ZERO
                  RESULT( 30 ) = ZERO
                  RESULT( 31 ) = ZERO
                  RESULT( 32 ) = ZERO
                  RESULT( 33 ) = ZERO
                  RESULT( 34 ) = ZERO
               END IF
*
*
*           Call ZSTEMR(V,A) to compute D1 and Z, do tests.
*
*           Compute D1 and Z
*
               CALL DCOPY( N, SD, 1, D5, 1 )
               IF( N.GT.0 )
     $            CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
               NTEST = 35
*
               CALL ZSTEMR( 'V', 'A', N, D5, RWORK, VL, VU, IL, IU,
     $                      M, D1, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                      RWORK( N+1 ), LRWORK-N, IWORK( 2*N+1 ),
     $                      LIWORK-2*N, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(V,A)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 35 ) = ULPINV
                     GO TO 280
                  END IF
               END IF
*
*           Do Tests 35 and 36
*
               CALL ZSTT22( N, M, 0, SD, SE, D1, DUMMA, Z, LDU, WORK, M,
     $                      RWORK, RESULT( 35 ) )
*
*           Call ZSTEMR to compute D2, do tests.
*
*           Compute D2
*
               CALL DCOPY( N, SD, 1, D5, 1 )
               IF( N.GT.0 )
     $            CALL DCOPY( N-1, SE, 1, RWORK, 1 )
*
               NTEST = 37
               CALL ZSTEMR( 'N', 'A', N, D5, RWORK, VL, VU, IL, IU,
     $                      M, D2, Z, LDU, N, IWORK( 1 ), TRYRAC,
     $                      RWORK( N+1 ), LRWORK-N, IWORK( 2*N+1 ),
     $                      LIWORK-2*N, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'ZSTEMR(N,A)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 37 ) = ULPINV
                     GO TO 280
                  END IF
               END IF
*
*           Do Test 34
*
               TEMP1 = ZERO
               TEMP2 = ZERO
*
               DO 260 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D2( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D2( J ) ) )
  260          CONTINUE
*
               RESULT( 37 ) = TEMP2 / MAX( UNFL,
     $                        ULP*MAX( TEMP1, TEMP2 ) )
            END IF
  270       CONTINUE
  280       CONTINUE
            NTESTT = NTESTT + NTEST
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
*
*           Print out tests which fail.
*
            DO 290 JR = 1, NTEST
               IF( RESULT( JR ).GE.THRESH ) THEN
*
*                 If this is the first test to fail,
*                 print a header to the data file.
*
                  IF( NERRS.EQ.0 ) THEN
                     WRITE( NOUNIT, FMT = 9998 )'ZST'
                     WRITE( NOUNIT, FMT = 9997 )
                     WRITE( NOUNIT, FMT = 9996 )
                     WRITE( NOUNIT, FMT = 9995 )'Hermitian'
                     WRITE( NOUNIT, FMT = 9994 )
*
*                    Tests performed
*
                     WRITE( NOUNIT, FMT = 9987 )
                  END IF
                  NERRS = NERRS + 1
                  IF( RESULT( JR ).LT.10000.0D0 ) THEN
                     WRITE( NOUNIT, FMT = 9989 )N, JTYPE, IOLDSD, JR,
     $                  RESULT( JR )
                  ELSE
                     WRITE( NOUNIT, FMT = 9988 )N, JTYPE, IOLDSD, JR,
     $                  RESULT( JR )
                  END IF
               END IF
  290       CONTINUE
  300    CONTINUE
  310 CONTINUE
*
*     Summary
*
      CALL DLASUM( 'ZST', NOUNIT, NERRS, NTESTT )
      RETURN
*
 9999 FORMAT( ' ZCHKST: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
 9998 FORMAT( / 1X, A3, ' -- Complex Hermitian eigenvalue problem' )
 9997 FORMAT( ' Matrix types (see ZCHKST for details): ' )
*
 9996 FORMAT( / ' Special Matrices:',
     $      / '  1=Zero matrix.                        ',
     $      '  5=Diagonal: clustered entries.',
     $      / '  2=Identity matrix.                    ',
     $      '  6=Diagonal: large, evenly spaced.',
     $      / '  3=Diagonal: evenly spaced entries.    ',
     $      '  7=Diagonal: small, evenly spaced.',
     $      / '  4=Diagonal: geometr. spaced entries.' )
 9995 FORMAT( ' Dense ', A, ' Matrices:',
     $      / '  8=Evenly spaced eigenvals.            ',
     $      ' 12=Small, evenly spaced eigenvals.',
     $      / '  9=Geometrically spaced eigenvals.     ',
     $      ' 13=Matrix with random O(1) entries.',
     $      / ' 10=Clustered eigenvalues.              ',
     $      ' 14=Matrix with large random entries.',
     $      / ' 11=Large, evenly spaced eigenvals.     ',
     $      ' 15=Matrix with small random entries.' )
 9994 FORMAT( ' 16=Positive definite, evenly spaced eigenvalues',
     $      / ' 17=Positive definite, geometrically spaced eigenvlaues',
     $      / ' 18=Positive definite, clustered eigenvalues',
     $      / ' 19=Positive definite, small evenly spaced eigenvalues',
     $      / ' 20=Positive definite, large evenly spaced eigenvalues',
     $      / ' 21=Diagonally dominant tridiagonal, geometrically',
     $      ' spaced eigenvalues' )
*
 9989 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I3, ' is', 0P, F8.2 )
 9988 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I3, ' is', 1P, D10.3 )
*
 9987 FORMAT( / 'Test performed:  see ZCHKST for details.', / )
*     End of ZCHKST
*
      END
