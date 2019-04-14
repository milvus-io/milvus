*> \brief \b CCHKHBSTG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CCHKHBSTG( NSIZES, NN, NWDTHS, KK, NTYPES, DOTYPE,
*                          ISEED, THRESH, NOUNIT, A, LDA, SD, SE, D1,
*                          D2, D3, U, LDU, WORK, LWORK, RWORK RESULT, 
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDU, LWORK, NOUNIT, NSIZES, NTYPES,
*      $                   NWDTHS
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), KK( * ), NN( * )
*       REAL               RESULT( * ), RWORK( * ), SD( * ), SE( * )
*       COMPLEX            A( LDA, * ), U( LDU, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CCHKHBSTG tests the reduction of a Hermitian band matrix to tridiagonal
*> from, used with the Hermitian eigenvalue problem.
*>
*> CHBTRD factors a Hermitian band matrix A as  U S U* , where * means
*> conjugate transpose, S is symmetric tridiagonal, and U is unitary.
*> CHBTRD can use either just the lower or just the upper triangle
*> of A; CCHKHBSTG checks both cases.
*>
*> CHETRD_HB2ST factors a Hermitian band matrix A as  U S U* , 
*> where * means conjugate transpose, S is symmetric tridiagonal, and U is
*> unitary. CHETRD_HB2ST can use either just the lower or just
*> the upper triangle of A; CCHKHBSTG checks both cases.
*>
*> DSTEQR factors S as  Z D1 Z'.  
*> D1 is the matrix of eigenvalues computed when Z is not computed
*> and from the S resulting of DSBTRD "U" (used as reference for DSYTRD_SB2ST)
*> D2 is the matrix of eigenvalues computed when Z is not computed
*> and from the S resulting of DSYTRD_SB2ST "U".
*> D3 is the matrix of eigenvalues computed when Z is not computed
*> and from the S resulting of DSYTRD_SB2ST "L".
*>
*> When CCHKHBSTG is called, a number of matrix "sizes" ("n's"), a number
*> of bandwidths ("k's"), and a number of matrix "types" are
*> specified.  For each size ("n"), each bandwidth ("k") less than or
*> equal to "n", and each type of matrix, one matrix will be generated
*> and used to test the hermitian banded reduction routine.  For each
*> matrix, a number of tests will be performed:
*>
*> (1)     | A - V S V* | / ( |A| n ulp )  computed by CHBTRD with
*>                                         UPLO='U'
*>
*> (2)     | I - UU* | / ( n ulp )
*>
*> (3)     | A - V S V* | / ( |A| n ulp )  computed by CHBTRD with
*>                                         UPLO='L'
*>
*> (4)     | I - UU* | / ( n ulp )
*>
*> (5)     | D1 - D2 | / ( |D1| ulp )      where D1 is computed by
*>                                         DSBTRD with UPLO='U' and
*>                                         D2 is computed by
*>                                         CHETRD_HB2ST with UPLO='U'
*>
*> (6)     | D1 - D3 | / ( |D1| ulp )      where D1 is computed by
*>                                         DSBTRD with UPLO='U' and
*>                                         D3 is computed by
*>                                         CHETRD_HB2ST with UPLO='L'
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
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          CCHKHBSTG does nothing.  It must be at least zero.
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
*> \param[in] NWDTHS
*> \verbatim
*>          NWDTHS is INTEGER
*>          The number of bandwidths to use.  If it is zero,
*>          CCHKHBSTG does nothing.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] KK
*> \verbatim
*>          KK is INTEGER array, dimension (NWDTHS)
*>          An array containing the bandwidths to be used for the band
*>          matrices.  The values must be at least zero.
*> \endverbatim
*>
*> \param[in] NTYPES
*> \verbatim
*>          NTYPES is INTEGER
*>          The number of elements in DOTYPE.   If it is zero, CCHKHBSTG
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
*>          next call to CCHKHBSTG to continue the same random number
*>          sequence.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
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
*>          A is COMPLEX array, dimension
*>                            (LDA, max(NN))
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 2 (not 1!)
*>          and at least max( KK )+1.
*> \endverbatim
*>
*> \param[out] SD
*> \verbatim
*>          SD is REAL array, dimension (max(NN))
*>          Used to hold the diagonal of the tridiagonal matrix computed
*>          by CHBTRD.
*> \endverbatim
*>
*> \param[out] SE
*> \verbatim
*>          SE is REAL array, dimension (max(NN))
*>          Used to hold the off-diagonal of the tridiagonal matrix
*>          computed by CHBTRD.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX array, dimension (LDU, max(NN))
*>          Used to hold the unitary matrix computed by CHBTRD.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  It must be at least 1
*>          and at least max( NN ).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The number of entries in WORK.  This must be at least
*>          max( LDA+1, max(NN)+1 )*max(NN).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (4)
*>          The values computed by the tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          If 0, then everything ran OK.
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
*> \date June 2017
*
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CCHKHB2STG( NSIZES, NN, NWDTHS, KK, NTYPES, DOTYPE,
     $                   ISEED, THRESH, NOUNIT, A, LDA, SD, SE, D1,
     $                   D2, D3, U, LDU, WORK, LWORK, RWORK, RESULT, 
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDU, LWORK, NOUNIT, NSIZES, NTYPES,
     $                   NWDTHS
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), KK( * ), NN( * )
      REAL               RESULT( * ), RWORK( * ), SD( * ), SE( * ),
     $                   D1( * ), D2( * ), D3( * )
      COMPLEX            A( LDA, * ), U( LDU, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
      REAL               ZERO, ONE, TWO, TEN
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0,
     $                   TEN = 10.0E+0 )
      REAL               HALF
      PARAMETER          ( HALF = ONE / TWO )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 15 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN, BADNNB
      INTEGER            I, IINFO, IMODE, ITYPE, J, JC, JCOL, JR, JSIZE,
     $                   JTYPE, JWIDTH, K, KMAX, LH, LW, MTYPES, N,
     $                   NERRS, NMATS, NMAX, NTEST, NTESTT
      REAL               ANINV, ANORM, COND, OVFL, RTOVFL, RTUNFL,
     $                   TEMP1, TEMP2, TEMP3, TEMP4, ULP, ULPINV, UNFL
*     ..
*     .. Local Arrays ..
      INTEGER            IDUMMA( 1 ), IOLDSD( 4 ), KMAGN( MAXTYP ),
     $                   KMODE( MAXTYP ), KTYPE( MAXTYP )
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLASUM, XERBLA, CHBT21, CHBTRD, CLACPY, CLASET,
     $                   CLATMR, CLATMS, CHETRD_HB2ST, CSTEQR
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, CONJG, MAX, MIN, SQRT
*     ..
*     .. Data statements ..
      DATA               KTYPE / 1, 2, 5*4, 5*5, 3*8 /
      DATA               KMAGN / 2*1, 1, 1, 1, 2, 3, 1, 1, 1, 2, 3, 1,
     $                   2, 3 /
      DATA               KMODE / 2*0, 4, 3, 1, 4, 4, 4, 3, 1, 4, 4, 0,
     $                   0, 0 /
*     ..
*     .. Executable Statements ..
*
*     Check for errors
*
      NTESTT = 0
      INFO = 0
*
*     Important constants
*
      BADNN = .FALSE.
      NMAX = 1
      DO 10 J = 1, NSIZES
         NMAX = MAX( NMAX, NN( J ) )
         IF( NN( J ).LT.0 )
     $      BADNN = .TRUE.
   10 CONTINUE
*
      BADNNB = .FALSE.
      KMAX = 0
      DO 20 J = 1, NSIZES
         KMAX = MAX( KMAX, KK( J ) )
         IF( KK( J ).LT.0 )
     $      BADNNB = .TRUE.
   20 CONTINUE
      KMAX = MIN( NMAX-1, KMAX )
*
*     Check for errors
*
      IF( NSIZES.LT.0 ) THEN
         INFO = -1
      ELSE IF( BADNN ) THEN
         INFO = -2
      ELSE IF( NWDTHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( BADNNB ) THEN
         INFO = -4
      ELSE IF( NTYPES.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.KMAX+1 ) THEN
         INFO = -11
      ELSE IF( LDU.LT.NMAX ) THEN
         INFO = -15
      ELSE IF( ( MAX( LDA, NMAX )+1 )*NMAX.GT.LWORK ) THEN
         INFO = -17
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CCHKHBSTG', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 .OR. NWDTHS.EQ.0 )
     $   RETURN
*
*     More Important constants
*
      UNFL = SLAMCH( 'Safe minimum' )
      OVFL = ONE / UNFL
      ULP = SLAMCH( 'Epsilon' )*SLAMCH( 'Base' )
      ULPINV = ONE / ULP
      RTUNFL = SQRT( UNFL )
      RTOVFL = SQRT( OVFL )
*
*     Loop over sizes, types
*
      NERRS = 0
      NMATS = 0
*
      DO 190 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         ANINV = ONE / REAL( MAX( 1, N ) )
*
         DO 180 JWIDTH = 1, NWDTHS
            K = KK( JWIDTH )
            IF( K.GT.N )
     $         GO TO 180
            K = MAX( 0, MIN( N-1, K ) )
*
            IF( NSIZES.NE.1 ) THEN
               MTYPES = MIN( MAXTYP, NTYPES )
            ELSE
               MTYPES = MIN( MAXTYP+1, NTYPES )
            END IF
*
            DO 170 JTYPE = 1, MTYPES
               IF( .NOT.DOTYPE( JTYPE ) )
     $            GO TO 170
               NMATS = NMATS + 1
               NTEST = 0
*
               DO 30 J = 1, 4
                  IOLDSD( J ) = ISEED( J )
   30          CONTINUE
*
*              Compute "A".
*              Store as "Upper"; later, we will copy to other format.
*
*              Control parameters:
*
*                  KMAGN  KMODE        KTYPE
*              =1  O(1)   clustered 1  zero
*              =2  large  clustered 2  identity
*              =3  small  exponential  (none)
*              =4         arithmetic   diagonal, (w/ eigenvalues)
*              =5         random log   hermitian, w/ eigenvalues
*              =6         random       (none)
*              =7                      random diagonal
*              =8                      random hermitian
*              =9                      positive definite
*              =10                     diagonally dominant tridiagonal
*
               IF( MTYPES.GT.MAXTYP )
     $            GO TO 100
*
               ITYPE = KTYPE( JTYPE )
               IMODE = KMODE( JTYPE )
*
*              Compute norm
*
               GO TO ( 40, 50, 60 )KMAGN( JTYPE )
*
   40          CONTINUE
               ANORM = ONE
               GO TO 70
*
   50          CONTINUE
               ANORM = ( RTOVFL*ULP )*ANINV
               GO TO 70
*
   60          CONTINUE
               ANORM = RTUNFL*N*ULPINV
               GO TO 70
*
   70          CONTINUE
*
               CALL CLASET( 'Full', LDA, N, CZERO, CZERO, A, LDA )
               IINFO = 0
               IF( JTYPE.LE.15 ) THEN
                  COND = ULPINV
               ELSE
                  COND = ULPINV*ANINV / TEN
               END IF
*
*              Special Matrices -- Identity & Jordan block
*
*                 Zero
*
               IF( ITYPE.EQ.1 ) THEN
                  IINFO = 0
*
               ELSE IF( ITYPE.EQ.2 ) THEN
*
*                 Identity
*
                  DO 80 JCOL = 1, N
                     A( K+1, JCOL ) = ANORM
   80             CONTINUE
*
               ELSE IF( ITYPE.EQ.4 ) THEN
*
*                 Diagonal Matrix, [Eigen]values Specified
*
                  CALL CLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE,
     $                         COND, ANORM, 0, 0, 'Q', A( K+1, 1 ), LDA,
     $                         WORK, IINFO )
*
               ELSE IF( ITYPE.EQ.5 ) THEN
*
*                 Hermitian, eigenvalues specified
*
                  CALL CLATMS( N, N, 'S', ISEED, 'H', RWORK, IMODE,
     $                         COND, ANORM, K, K, 'Q', A, LDA, WORK,
     $                         IINFO )
*
               ELSE IF( ITYPE.EQ.7 ) THEN
*
*                 Diagonal, random eigenvalues
*
                  CALL CLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE,
     $                         CONE, 'T', 'N', WORK( N+1 ), 1, ONE,
     $                         WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, 0, 0,
     $                         ZERO, ANORM, 'Q', A( K+1, 1 ), LDA,
     $                         IDUMMA, IINFO )
*
               ELSE IF( ITYPE.EQ.8 ) THEN
*
*                 Hermitian, random eigenvalues
*
                  CALL CLATMR( N, N, 'S', ISEED, 'H', WORK, 6, ONE,
     $                         CONE, 'T', 'N', WORK( N+1 ), 1, ONE,
     $                         WORK( 2*N+1 ), 1, ONE, 'N', IDUMMA, K, K,
     $                         ZERO, ANORM, 'Q', A, LDA, IDUMMA, IINFO )
*
               ELSE IF( ITYPE.EQ.9 ) THEN
*
*                 Positive definite, eigenvalues specified.
*
                  CALL CLATMS( N, N, 'S', ISEED, 'P', RWORK, IMODE,
     $                         COND, ANORM, K, K, 'Q', A, LDA,
     $                         WORK( N+1 ), IINFO )
*
               ELSE IF( ITYPE.EQ.10 ) THEN
*
*                 Positive definite tridiagonal, eigenvalues specified.
*
                  IF( N.GT.1 )
     $               K = MAX( 1, K )
                  CALL CLATMS( N, N, 'S', ISEED, 'P', RWORK, IMODE,
     $                         COND, ANORM, 1, 1, 'Q', A( K, 1 ), LDA,
     $                         WORK, IINFO )
                  DO 90 I = 2, N
                     TEMP1 = ABS( A( K, I ) ) /
     $                       SQRT( ABS( A( K+1, I-1 )*A( K+1, I ) ) )
                     IF( TEMP1.GT.HALF ) THEN
                        A( K, I ) = HALF*SQRT( ABS( A( K+1,
     $                              I-1 )*A( K+1, I ) ) )
                     END IF
   90             CONTINUE
*
               ELSE
*
                  IINFO = 1
               END IF
*
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'Generator', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
*
  100          CONTINUE
*
*              Call CHBTRD to compute S and U from upper triangle.
*
               CALL CLACPY( ' ', K+1, N, A, LDA, WORK, LDA )
*
               NTEST = 1
               CALL CHBTRD( 'V', 'U', N, K, WORK, LDA, SD, SE, U, LDU,
     $                      WORK( LDA*N+1 ), IINFO )
*
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHBTRD(U)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 1 ) = ULPINV
                     GO TO 150
                  END IF
               END IF
*
*              Do tests 1 and 2
*
               CALL CHBT21( 'Upper', N, K, 1, A, LDA, SD, SE, U, LDU,
     $                      WORK, RWORK, RESULT( 1 ) )
*
*              Before converting A into lower for DSBTRD, run DSYTRD_SB2ST 
*              otherwise matrix A will be converted to lower and then need
*              to be converted back to upper in order to run the upper case 
*              ofDSYTRD_SB2ST
*            
*              Compute D1 the eigenvalues resulting from the tridiagonal
*              form using the DSBTRD and used as reference to compare
*              with the DSYTRD_SB2ST routine
*            
*              Compute D1 from the DSBTRD and used as reference for the
*              DSYTRD_SB2ST
*            
               CALL SCOPY( N, SD, 1, D1, 1 )
               IF( N.GT.0 )
     $            CALL SCOPY( N-1, SE, 1, RWORK, 1 )
*
               CALL CSTEQR( 'N', N, D1, RWORK, WORK, LDU,
     $                      RWORK( N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CSTEQR(N)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 5 ) = ULPINV
                     GO TO 150
                  END IF
               END IF
*            
*              DSYTRD_SB2ST Upper case is used to compute D2.
*              Note to set SD and SE to zero to be sure not reusing 
*              the one from above. Compare it with D1 computed 
*              using the DSBTRD.
*            
               CALL DLASET( 'Full', N, 1, ZERO, ZERO, SD, 1 )
               CALL DLASET( 'Full', N, 1, ZERO, ZERO, SE, 1 )
               CALL CLACPY( ' ', K+1, N, A, LDA, U, LDU )
               LH = MAX(1, 4*N)
               LW = LWORK - LH
               CALL CHETRD_HB2ST( 'N', 'N', "U", N, K, U, LDU, SD, SE, 
     $                      WORK, LH, WORK( LH+1 ), LW, IINFO )
*            
*              Compute D2 from the DSYTRD_SB2ST Upper case
*            
               CALL SCOPY( N, SD, 1, D2, 1 )
               IF( N.GT.0 )
     $            CALL SCOPY( N-1, SE, 1, RWORK, 1 )
*            
               CALL CSTEQR( 'N', N, D2, RWORK, WORK, LDU,
     $                      RWORK( N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CSTEQR(N)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 5 ) = ULPINV
                     GO TO 150
                  END IF
               END IF
*
*              Convert A from Upper-Triangle-Only storage to
*              Lower-Triangle-Only storage.
*
               DO 120 JC = 1, N
                  DO 110 JR = 0, MIN( K, N-JC )
                     A( JR+1, JC ) = CONJG( A( K+1-JR, JC+JR ) )
  110             CONTINUE
  120          CONTINUE
               DO 140 JC = N + 1 - K, N
                  DO 130 JR = MIN( K, N-JC ) + 1, K
                     A( JR+1, JC ) = ZERO
  130             CONTINUE
  140          CONTINUE
*
*              Call CHBTRD to compute S and U from lower triangle
*
               CALL CLACPY( ' ', K+1, N, A, LDA, WORK, LDA )
*
               NTEST = 3
               CALL CHBTRD( 'V', 'L', N, K, WORK, LDA, SD, SE, U, LDU,
     $                      WORK( LDA*N+1 ), IINFO )
*
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CHBTRD(L)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 3 ) = ULPINV
                     GO TO 150
                  END IF
               END IF
               NTEST = 4
*
*              Do tests 3 and 4
*
               CALL CHBT21( 'Lower', N, K, 1, A, LDA, SD, SE, U, LDU,
     $                      WORK, RWORK, RESULT( 3 ) )
*
*              DSYTRD_SB2ST Lower case is used to compute D3.
*              Note to set SD and SE to zero to be sure not reusing 
*              the one from above. Compare it with D1 computed 
*              using the DSBTRD. 
*           
               CALL DLASET( 'Full', N, 1, ZERO, ZERO, SD, 1 )
               CALL DLASET( 'Full', N, 1, ZERO, ZERO, SE, 1 )
               CALL CLACPY( ' ', K+1, N, A, LDA, U, LDU )
               LH = MAX(1, 4*N)
               LW = LWORK - LH
               CALL CHETRD_HB2ST( 'N', 'N', "L", N, K, U, LDU, SD, SE, 
     $                      WORK, LH, WORK( LH+1 ), LW, IINFO )
*           
*              Compute D3 from the 2-stage Upper case
*           
               CALL SCOPY( N, SD, 1, D3, 1 )
               IF( N.GT.0 )
     $            CALL SCOPY( N-1, SE, 1, RWORK, 1 )
*           
               CALL CSTEQR( 'N', N, D3, RWORK, WORK, LDU,
     $                      RWORK( N+1 ), IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )'CSTEQR(N)', IINFO, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  IF( IINFO.LT.0 ) THEN
                     RETURN
                  ELSE
                     RESULT( 6 ) = ULPINV
                     GO TO 150
                  END IF
               END IF
*           
*           
*              Do Tests 3 and 4 which are similar to 11 and 12 but with the
*              D1 computed using the standard 1-stage reduction as reference
*           
               NTEST = 6
               TEMP1 = ZERO
               TEMP2 = ZERO
               TEMP3 = ZERO
               TEMP4 = ZERO
*           
               DO 151 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( D1( J ) ), ABS( D2( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( D1( J )-D2( J ) ) )
                  TEMP3 = MAX( TEMP3, ABS( D1( J ) ), ABS( D3( J ) ) )
                  TEMP4 = MAX( TEMP4, ABS( D1( J )-D3( J ) ) )
  151          CONTINUE
*           
               RESULT(5) = TEMP2 / MAX( UNFL, ULP*MAX( TEMP1, TEMP2 ) )
               RESULT(6) = TEMP4 / MAX( UNFL, ULP*MAX( TEMP3, TEMP4 ) )
*
*              End of Loop -- Check for RESULT(j) > THRESH
*
  150          CONTINUE
               NTESTT = NTESTT + NTEST
*
*              Print out tests which fail.
*
               DO 160 JR = 1, NTEST
                  IF( RESULT( JR ).GE.THRESH ) THEN
*
*                    If this is the first test to fail,
*                    print a header to the data file.
*
                     IF( NERRS.EQ.0 ) THEN
                        WRITE( NOUNIT, FMT = 9998 )'CHB'
                        WRITE( NOUNIT, FMT = 9997 )
                        WRITE( NOUNIT, FMT = 9996 )
                        WRITE( NOUNIT, FMT = 9995 )'Hermitian'
                        WRITE( NOUNIT, FMT = 9994 )'unitary', '*',
     $                     'conjugate transpose', ( '*', J = 1, 6 )
                     END IF
                     NERRS = NERRS + 1
                     WRITE( NOUNIT, FMT = 9993 )N, K, IOLDSD, JTYPE,
     $                  JR, RESULT( JR )
                  END IF
  160          CONTINUE
*
  170       CONTINUE
  180    CONTINUE
  190 CONTINUE
*
*     Summary
*
      CALL SLASUM( 'CHB', NOUNIT, NERRS, NTESTT )
      RETURN
*
 9999 FORMAT( ' CCHKHBSTG: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
 9998 FORMAT( / 1X, A3,
     $     ' -- Complex Hermitian Banded Tridiagonal Reduction Routines'
     $       )
 9997 FORMAT( ' Matrix types (see SCHK23 for details): ' )
*
 9996 FORMAT( / ' Special Matrices:',
     $      / '  1=Zero matrix.                        ',
     $      '  5=Diagonal: clustered entries.',
     $      / '  2=Identity matrix.                    ',
     $      '  6=Diagonal: large, evenly spaced.',
     $      / '  3=Diagonal: evenly spaced entries.    ',
     $      '  7=Diagonal: small, evenly spaced.',
     $      / '  4=Diagonal: geometr. spaced entries.' )
 9995 FORMAT( ' Dense ', A, ' Banded Matrices:',
     $      / '  8=Evenly spaced eigenvals.            ',
     $      ' 12=Small, evenly spaced eigenvals.',
     $      / '  9=Geometrically spaced eigenvals.     ',
     $      ' 13=Matrix with random O(1) entries.',
     $      / ' 10=Clustered eigenvalues.              ',
     $      ' 14=Matrix with large random entries.',
     $      / ' 11=Large, evenly spaced eigenvals.     ',
     $      ' 15=Matrix with small random entries.' )
*
 9994 FORMAT( / ' Tests performed:   (S is Tridiag,  U is ', A, ',',
     $      / 20X, A, ' means ', A, '.', / ' UPLO=''U'':',
     $      / '  1= | A - U S U', A1, ' | / ( |A| n ulp )     ',
     $      '  2= | I - U U', A1, ' | / ( n ulp )', / ' UPLO=''L'':',
     $      / '  3= | A - U S U', A1, ' | / ( |A| n ulp )     ',
     $      '  4= | I - U U', A1, ' | / ( n ulp )' / ' Eig check:',
     $      /'  5= | D1 - D2', '', ' | / ( |D1| ulp )         ',
     $      '  6= | D1 - D3', '', ' | / ( |D1| ulp )          ' )
 9993 FORMAT( ' N=', I5, ', K=', I4, ', seed=', 4( I4, ',' ), ' type ',
     $      I2, ', test(', I2, ')=', G10.3 )
*
*     End of CCHKHBSTG
*
      END
