*> \brief \b ZDRGES3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZDRGES3( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, B, S, T, Q, LDQ, Z, ALPHA,
*                          BETA, WORK, LWORK, RWORK, RESULT, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDQ, LWORK, NOUNIT, NSIZES, NTYPES
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * ), DOTYPE( * )
*       INTEGER            ISEED( 4 ), NN( * )
*       DOUBLE PRECISION   RESULT( 13 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDA, * ),
*      $                   BETA( * ), Q( LDQ, * ), S( LDA, * ),
*      $                   T( LDA, * ), WORK( * ), Z( LDQ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZDRGES3 checks the nonsymmetric generalized eigenvalue (Schur form)
*> problem driver ZGGES3.
*>
*> ZGGES3 factors A and B as Q*S*Z'  and Q*T*Z' , where ' means conjugate
*> transpose, S and T are  upper triangular (i.e., in generalized Schur
*> form), and Q and Z are unitary. It also computes the generalized
*> eigenvalues (alpha(j),beta(j)), j=1,...,n.  Thus,
*> w(j) = alpha(j)/beta(j) is a root of the characteristic equation
*>
*>                 det( A - w(j) B ) = 0
*>
*> Optionally it also reorder the eigenvalues so that a selected
*> cluster of eigenvalues appears in the leading diagonal block of the
*> Schur forms.
*>
*> When ZDRGES3 is called, a number of matrix "sizes" ("N's") and a
*> number of matrix "TYPES" are specified.  For each size ("N")
*> and each TYPE of matrix, a pair of matrices (A, B) will be generated
*> and used for testing. For each matrix pair, the following 13 tests
*> will be performed and compared with the threshold THRESH except
*> the tests (5), (11) and (13).
*>
*>
*> (1)   | A - Q S Z' | / ( |A| n ulp ) (no sorting of eigenvalues)
*>
*>
*> (2)   | B - Q T Z' | / ( |B| n ulp ) (no sorting of eigenvalues)
*>
*>
*> (3)   | I - QQ' | / ( n ulp ) (no sorting of eigenvalues)
*>
*>
*> (4)   | I - ZZ' | / ( n ulp ) (no sorting of eigenvalues)
*>
*> (5)   if A is in Schur form (i.e. triangular form) (no sorting of
*>       eigenvalues)
*>
*> (6)   if eigenvalues = diagonal elements of the Schur form (S, T),
*>       i.e., test the maximum over j of D(j)  where:
*>
*>                     |alpha(j) - S(j,j)|        |beta(j) - T(j,j)|
*>           D(j) = ------------------------ + -----------------------
*>                  max(|alpha(j)|,|S(j,j)|)   max(|beta(j)|,|T(j,j)|)
*>
*>       (no sorting of eigenvalues)
*>
*> (7)   | (A,B) - Q (S,T) Z' | / ( |(A,B)| n ulp )
*>       (with sorting of eigenvalues).
*>
*> (8)   | I - QQ' | / ( n ulp ) (with sorting of eigenvalues).
*>
*> (9)   | I - ZZ' | / ( n ulp ) (with sorting of eigenvalues).
*>
*> (10)  if A is in Schur form (i.e. quasi-triangular form)
*>       (with sorting of eigenvalues).
*>
*> (11)  if eigenvalues = diagonal elements of the Schur form (S, T),
*>       i.e. test the maximum over j of D(j)  where:
*>
*>                     |alpha(j) - S(j,j)|        |beta(j) - T(j,j)|
*>           D(j) = ------------------------ + -----------------------
*>                  max(|alpha(j)|,|S(j,j)|)   max(|beta(j)|,|T(j,j)|)
*>
*>       (with sorting of eigenvalues).
*>
*> (12)  if sorting worked and SDIM is the number of eigenvalues
*>       which were CELECTed.
*>
*> Test Matrices
*> =============
*>
*> The sizes of the test matrices are specified by an array
*> NN(1:NSIZES); the value of each element NN(j) specifies one size.
*> The "types" are specified by a logical array DOTYPE( 1:NTYPES ); if
*> DOTYPE(j) is .TRUE., then matrix type "j" will be generated.
*> Currently, the list of possible types is:
*>
*> (1)  ( 0, 0 )         (a pair of zero matrices)
*>
*> (2)  ( I, 0 )         (an identity and a zero matrix)
*>
*> (3)  ( 0, I )         (an identity and a zero matrix)
*>
*> (4)  ( I, I )         (a pair of identity matrices)
*>
*>         t   t
*> (5)  ( J , J  )       (a pair of transposed Jordan blocks)
*>
*>                                     t                ( I   0  )
*> (6)  ( X, Y )         where  X = ( J   0  )  and Y = (      t )
*>                                  ( 0   I  )          ( 0   J  )
*>                       and I is a k x k identity and J a (k+1)x(k+1)
*>                       Jordan block; k=(N-1)/2
*>
*> (7)  ( D, I )         where D is diag( 0, 1,..., N-1 ) (a diagonal
*>                       matrix with those diagonal entries.)
*> (8)  ( I, D )
*>
*> (9)  ( big*D, small*I ) where "big" is near overflow and small=1/big
*>
*> (10) ( small*D, big*I )
*>
*> (11) ( big*I, small*D )
*>
*> (12) ( small*I, big*D )
*>
*> (13) ( big*D, big*I )
*>
*> (14) ( small*D, small*I )
*>
*> (15) ( D1, D2 )        where D1 is diag( 0, 0, 1, ..., N-3, 0 ) and
*>                        D2 is diag( 0, N-3, N-4,..., 1, 0, 0 )
*>           t   t
*> (16) Q ( J , J ) Z     where Q and Z are random orthogonal matrices.
*>
*> (17) Q ( T1, T2 ) Z    where T1 and T2 are upper triangular matrices
*>                        with random O(1) entries above the diagonal
*>                        and diagonal entries diag(T1) =
*>                        ( 0, 0, 1, ..., N-3, 0 ) and diag(T2) =
*>                        ( 0, N-3, N-4,..., 1, 0, 0 )
*>
*> (18) Q ( T1, T2 ) Z    diag(T1) = ( 0, 0, 1, 1, s, ..., s, 0 )
*>                        diag(T2) = ( 0, 1, 0, 1,..., 1, 0 )
*>                        s = machine precision.
*>
*> (19) Q ( T1, T2 ) Z    diag(T1)=( 0,0,1,1, 1-d, ..., 1-(N-5)*d=s, 0 )
*>                        diag(T2) = ( 0, 1, 0, 1, ..., 1, 0 )
*>
*>                                                        N-5
*> (20) Q ( T1, T2 ) Z    diag(T1)=( 0, 0, 1, 1, a, ..., a   =s, 0 )
*>                        diag(T2) = ( 0, 1, 0, 1, ..., 1, 0, 0 )
*>
*> (21) Q ( T1, T2 ) Z    diag(T1)=( 0, 0, 1, r1, r2, ..., r(N-4), 0 )
*>                        diag(T2) = ( 0, 1, 0, 1, ..., 1, 0, 0 )
*>                        where r1,..., r(N-4) are random.
*>
*> (22) Q ( big*T1, small*T2 ) Z    diag(T1) = ( 0, 0, 1, ..., N-3, 0 )
*>                                  diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (23) Q ( small*T1, big*T2 ) Z    diag(T1) = ( 0, 0, 1, ..., N-3, 0 )
*>                                  diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (24) Q ( small*T1, small*T2 ) Z  diag(T1) = ( 0, 0, 1, ..., N-3, 0 )
*>                                  diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (25) Q ( big*T1, big*T2 ) Z      diag(T1) = ( 0, 0, 1, ..., N-3, 0 )
*>                                  diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (26) Q ( T1, T2 ) Z     where T1 and T2 are random upper-triangular
*>                         matrices.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          DDRGES3 does nothing.  NSIZES >= 0.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER array, dimension (NSIZES)
*>          An array containing the sizes to be used for the matrices.
*>          Zero values will be skipped.  NN >= 0.
*> \endverbatim
*>
*> \param[in] NTYPES
*> \verbatim
*>          NTYPES is INTEGER
*>          The number of elements in DOTYPE.   If it is zero, DDRGES3
*>          does nothing.  It must be at least zero.  If it is MAXTYP+1
*>          and NSIZES is 1, then an additional type, MAXTYP+1 is
*>          defined, which is to use whatever matrix is in A on input.
*>          This is only useful if DOTYPE(1:MAXTYP) is .FALSE. and
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
*>          MAXTYP will not be generated. If NTYPES is larger
*>          than MAXTYP, DOTYPE(MAXTYP+1) through DOTYPE(NTYPES)
*>          will be ignored.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          On entry ISEED specifies the seed of the random number
*>          generator. The array elements should be between 0 and 4095;
*>          if not they will be reduced mod 4096. Also, ISEED(4) must
*>          be odd.  The random number generator uses a linear
*>          congruential sequence limited to small integers, and so
*>          should produce machine independent random numbers. The
*>          values of ISEED are changed on exit, and can be used in the
*>          next call to DDRGES3 to continue the same random number
*>          sequence.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is DOUBLE PRECISION
*>          A test will count as "failed" if the "error", computed as
*>          described above, exceeds THRESH.  Note that the error is
*>          scaled to be O(1), so THRESH should be a reasonably small
*>          multiple of 1, e.g., 10 or 100.  In particular, it should
*>          not depend on the precision (single vs. double) or the size
*>          of the matrix.  THRESH >= 0.
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
*>          A is COMPLEX*16 array, dimension(LDA, max(NN))
*>          Used to hold the original A matrix.  Used as input only
*>          if NTYPES=MAXTYP+1, DOTYPE(1:MAXTYP)=.FALSE., and
*>          DOTYPE(MAXTYP+1)=.TRUE.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, B, S, and T.
*>          It must be at least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension(LDA, max(NN))
*>          Used to hold the original B matrix.  Used as input only
*>          if NTYPES=MAXTYP+1, DOTYPE(1:MAXTYP)=.FALSE., and
*>          DOTYPE(MAXTYP+1)=.TRUE.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The Schur form matrix computed from A by ZGGES3.  On exit, S
*>          contains the Schur form matrix corresponding to the matrix
*>          in A.
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The upper triangular matrix computed from B by ZGGES3.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDQ, max(NN))
*>          The (left) orthogonal matrix computed by ZGGES3.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of Q and Z. It must
*>          be at least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension( LDQ, max(NN) )
*>          The (right) orthogonal matrix computed by ZGGES3.
*> \endverbatim
*>
*> \param[out] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX*16 array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is COMPLEX*16 array, dimension (max(NN))
*>
*>          The generalized eigenvalues of (A,B) computed by ZGGES3.
*>          ALPHA(k) / BETA(k) is the k-th generalized eigenvalue of A
*>          and B.
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
*>          The dimension of the array WORK.  LWORK >= 3*N*N.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension ( 8*N )
*>          Real workspace.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (15)
*>          The values computed by the tests described above.
*>          The values are currently limited to 1/ulp, to avoid overflow.
*> \endverbatim
*>
*> \param[out] BWORK
*> \verbatim
*>          BWORK is LOGICAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  A routine returned an error code.  INFO is the
*>                absolute value of the INFO value returned.
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
*> \date February 2015
*
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZDRGES3( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                    NOUNIT, A, LDA, B, S, T, Q, LDQ, Z, ALPHA,
     $                    BETA, WORK, LWORK, RWORK, RESULT, BWORK,
     $                    INFO )
*
*  -- LAPACK test routine (version 3.6.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     February 2015
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDQ, LWORK, NOUNIT, NSIZES, NTYPES
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * ), DOTYPE( * )
      INTEGER            ISEED( 4 ), NN( * )
      DOUBLE PRECISION   RESULT( 13 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDA, * ),
     $                   BETA( * ), Q( LDQ, * ), S( LDA, * ),
     $                   T( LDA, * ), WORK( * ), Z( LDQ, * )
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
      PARAMETER          ( MAXTYP = 26 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN, ILABAD
      CHARACTER          SORT
      INTEGER            I, IADD, IINFO, IN, ISORT, J, JC, JR, JSIZE,
     $                   JTYPE, KNTEIG, MAXWRK, MINWRK, MTYPES, N, N1,
     $                   NB, NERRS, NMATS, NMAX, NTEST, NTESTT, RSUB,
     $                   SDIM
      DOUBLE PRECISION   SAFMAX, SAFMIN, TEMP1, TEMP2, ULP, ULPINV
      COMPLEX*16         CTEMP, X
*     ..
*     .. Local Arrays ..
      LOGICAL            LASIGN( MAXTYP ), LBSIGN( MAXTYP )
      INTEGER            IOLDSD( 4 ), KADD( 6 ), KAMAGN( MAXTYP ),
     $                   KATYPE( MAXTYP ), KAZERO( MAXTYP ),
     $                   KBMAGN( MAXTYP ), KBTYPE( MAXTYP ),
     $                   KBZERO( MAXTYP ), KCLASS( MAXTYP ),
     $                   KTRIAN( MAXTYP ), KZ1( 6 ), KZ2( 6 )
      DOUBLE PRECISION   RMAGN( 0: 3 )
*     ..
*     .. External Functions ..
      LOGICAL            ZLCTES
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH
      COMPLEX*16         ZLARND
      EXTERNAL           ZLCTES, ILAENV, DLAMCH, ZLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALASVM, DLABAD, XERBLA, ZGET51, ZGET54, ZGGES3,
     $                   ZLACPY, ZLARFG, ZLASET, ZLATM4, ZUNM2R
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, DIMAG, MAX, MIN, SIGN
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   ABS1
*     ..
*     .. Statement Function definitions ..
      ABS1( X ) = ABS( DBLE( X ) ) + ABS( DIMAG( X ) )
*     ..
*     .. Data statements ..
      DATA               KCLASS / 15*1, 10*2, 1*3 /
      DATA               KZ1 / 0, 1, 2, 1, 3, 3 /
      DATA               KZ2 / 0, 0, 1, 2, 1, 1 /
      DATA               KADD / 0, 0, 0, 0, 3, 2 /
      DATA               KATYPE / 0, 1, 0, 1, 2, 3, 4, 1, 4, 4, 1, 1, 4,
     $                   4, 4, 2, 4, 5, 8, 7, 9, 4*4, 0 /
      DATA               KBTYPE / 0, 0, 1, 1, 2, -3, 1, 4, 1, 1, 4, 4,
     $                   1, 1, -4, 2, -4, 8*8, 0 /
      DATA               KAZERO / 6*1, 2, 1, 2*2, 2*1, 2*2, 3, 1, 3,
     $                   4*5, 4*3, 1 /
      DATA               KBZERO / 6*1, 1, 2, 2*1, 2*2, 2*1, 4, 1, 4,
     $                   4*6, 4*4, 1 /
      DATA               KAMAGN / 8*1, 2, 3, 2, 3, 2, 3, 7*1, 2, 3, 3,
     $                   2, 1 /
      DATA               KBMAGN / 8*1, 3, 2, 3, 2, 2, 3, 7*1, 3, 2, 3,
     $                   2, 1 /
      DATA               KTRIAN / 16*0, 10*1 /
      DATA               LASIGN / 6*.FALSE., .TRUE., .FALSE., 2*.TRUE.,
     $                   2*.FALSE., 3*.TRUE., .FALSE., .TRUE.,
     $                   3*.FALSE., 5*.TRUE., .FALSE. /
      DATA               LBSIGN / 7*.FALSE., .TRUE., 2*.FALSE.,
     $                   2*.TRUE., 2*.FALSE., .TRUE., .FALSE., .TRUE.,
     $                   9*.FALSE. /
*     ..
*     .. Executable Statements ..
*
*     Check for errors
*
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
      ELSE IF( LDQ.LE.1 .OR. LDQ.LT.NMAX ) THEN
         INFO = -14
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       NB refers to the optimal block size for the immediately
*       following subroutine, as returned by ILAENV.
*
      MINWRK = 1
      IF( INFO.EQ.0 .AND. LWORK.GE.1 ) THEN
         MINWRK = 3*NMAX*NMAX
         NB = MAX( 1, ILAENV( 1, 'ZGEQRF', ' ', NMAX, NMAX, -1, -1 ),
     $        ILAENV( 1, 'ZUNMQR', 'LC', NMAX, NMAX, NMAX, -1 ),
     $        ILAENV( 1, 'ZUNGQR', ' ', NMAX, NMAX, NMAX, -1 ) )
         MAXWRK = MAX( NMAX+NMAX*NB, 3*NMAX*NMAX )
         WORK( 1 ) = MAXWRK
      END IF
*
      IF( LWORK.LT.MINWRK )
     $   INFO = -19
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZDRGES3', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   RETURN
*
      ULP = DLAMCH( 'Precision' )
      SAFMIN = DLAMCH( 'Safe minimum' )
      SAFMIN = SAFMIN / ULP
      SAFMAX = ONE / SAFMIN
      CALL DLABAD( SAFMIN, SAFMAX )
      ULPINV = ONE / ULP
*
*     The values RMAGN(2:3) depend on N, see below.
*
      RMAGN( 0 ) = ZERO
      RMAGN( 1 ) = ONE
*
*     Loop over matrix sizes
*
      NTESTT = 0
      NERRS = 0
      NMATS = 0
*
      DO 190 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         N1 = MAX( 1, N )
         RMAGN( 2 ) = SAFMAX*ULP / DBLE( N1 )
         RMAGN( 3 ) = SAFMIN*ULPINV*DBLE( N1 )
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
*        Loop over matrix types
*
         DO 180 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 180
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
            DO 30 J = 1, 13
               RESULT( J ) = ZERO
   30       CONTINUE
*
*           Generate test matrices A and B
*
*           Description of control parameters:
*
*           KZLASS: =1 means w/o rotation, =2 means w/ rotation,
*                   =3 means random.
*           KATYPE: the "type" to be passed to ZLATM4 for computing A.
*           KAZERO: the pattern of zeros on the diagonal for A:
*                   =1: ( xxx ), =2: (0, xxx ) =3: ( 0, 0, xxx, 0 ),
*                   =4: ( 0, xxx, 0, 0 ), =5: ( 0, 0, 1, xxx, 0 ),
*                   =6: ( 0, 1, 0, xxx, 0 ).  (xxx means a string of
*                   non-zero entries.)
*           KAMAGN: the magnitude of the matrix: =0: zero, =1: O(1),
*                   =2: large, =3: small.
*           LASIGN: .TRUE. if the diagonal elements of A are to be
*                   multiplied by a random magnitude 1 number.
*           KBTYPE, KBZERO, KBMAGN, LBSIGN: the same, but for B.
*           KTRIAN: =0: don't fill in the upper triangle, =1: do.
*           KZ1, KZ2, KADD: used to implement KAZERO and KBZERO.
*           RMAGN: used to implement KAMAGN and KBMAGN.
*
            IF( MTYPES.GT.MAXTYP )
     $         GO TO 110
            IINFO = 0
            IF( KCLASS( JTYPE ).LT.3 ) THEN
*
*              Generate A (w/o rotation)
*
               IF( ABS( KATYPE( JTYPE ) ).EQ.3 ) THEN
                  IN = 2*( ( N-1 ) / 2 ) + 1
                  IF( IN.NE.N )
     $               CALL ZLASET( 'Full', N, N, CZERO, CZERO, A, LDA )
               ELSE
                  IN = N
               END IF
               CALL ZLATM4( KATYPE( JTYPE ), IN, KZ1( KAZERO( JTYPE ) ),
     $                      KZ2( KAZERO( JTYPE ) ), LASIGN( JTYPE ),
     $                      RMAGN( KAMAGN( JTYPE ) ), ULP,
     $                      RMAGN( KTRIAN( JTYPE )*KAMAGN( JTYPE ) ), 2,
     $                      ISEED, A, LDA )
               IADD = KADD( KAZERO( JTYPE ) )
               IF( IADD.GT.0 .AND. IADD.LE.N )
     $            A( IADD, IADD ) = RMAGN( KAMAGN( JTYPE ) )
*
*              Generate B (w/o rotation)
*
               IF( ABS( KBTYPE( JTYPE ) ).EQ.3 ) THEN
                  IN = 2*( ( N-1 ) / 2 ) + 1
                  IF( IN.NE.N )
     $               CALL ZLASET( 'Full', N, N, CZERO, CZERO, B, LDA )
               ELSE
                  IN = N
               END IF
               CALL ZLATM4( KBTYPE( JTYPE ), IN, KZ1( KBZERO( JTYPE ) ),
     $                      KZ2( KBZERO( JTYPE ) ), LBSIGN( JTYPE ),
     $                      RMAGN( KBMAGN( JTYPE ) ), ONE,
     $                      RMAGN( KTRIAN( JTYPE )*KBMAGN( JTYPE ) ), 2,
     $                      ISEED, B, LDA )
               IADD = KADD( KBZERO( JTYPE ) )
               IF( IADD.NE.0 .AND. IADD.LE.N )
     $            B( IADD, IADD ) = RMAGN( KBMAGN( JTYPE ) )
*
               IF( KCLASS( JTYPE ).EQ.2 .AND. N.GT.0 ) THEN
*
*                 Include rotations
*
*                 Generate Q, Z as Householder transformations times
*                 a diagonal matrix.
*
                  DO 50 JC = 1, N - 1
                     DO 40 JR = JC, N
                        Q( JR, JC ) = ZLARND( 3, ISEED )
                        Z( JR, JC ) = ZLARND( 3, ISEED )
   40                CONTINUE
                     CALL ZLARFG( N+1-JC, Q( JC, JC ), Q( JC+1, JC ), 1,
     $                            WORK( JC ) )
                     WORK( 2*N+JC ) = SIGN( ONE, DBLE( Q( JC, JC ) ) )
                     Q( JC, JC ) = CONE
                     CALL ZLARFG( N+1-JC, Z( JC, JC ), Z( JC+1, JC ), 1,
     $                            WORK( N+JC ) )
                     WORK( 3*N+JC ) = SIGN( ONE, DBLE( Z( JC, JC ) ) )
                     Z( JC, JC ) = CONE
   50             CONTINUE
                  CTEMP = ZLARND( 3, ISEED )
                  Q( N, N ) = CONE
                  WORK( N ) = CZERO
                  WORK( 3*N ) = CTEMP / ABS( CTEMP )
                  CTEMP = ZLARND( 3, ISEED )
                  Z( N, N ) = CONE
                  WORK( 2*N ) = CZERO
                  WORK( 4*N ) = CTEMP / ABS( CTEMP )
*
*                 Apply the diagonal matrices
*
                  DO 70 JC = 1, N
                     DO 60 JR = 1, N
                        A( JR, JC ) = WORK( 2*N+JR )*
     $                                DCONJG( WORK( 3*N+JC ) )*
     $                                A( JR, JC )
                        B( JR, JC ) = WORK( 2*N+JR )*
     $                                DCONJG( WORK( 3*N+JC ) )*
     $                                B( JR, JC )
   60                CONTINUE
   70             CONTINUE
                  CALL ZUNM2R( 'L', 'N', N, N, N-1, Q, LDQ, WORK, A,
     $                         LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
                  CALL ZUNM2R( 'R', 'C', N, N, N-1, Z, LDQ, WORK( N+1 ),
     $                         A, LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
                  CALL ZUNM2R( 'L', 'N', N, N, N-1, Q, LDQ, WORK, B,
     $                         LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
                  CALL ZUNM2R( 'R', 'C', N, N, N-1, Z, LDQ, WORK( N+1 ),
     $                         B, LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
               END IF
            ELSE
*
*              Random matrices
*
               DO 90 JC = 1, N
                  DO 80 JR = 1, N
                     A( JR, JC ) = RMAGN( KAMAGN( JTYPE ) )*
     $                             ZLARND( 4, ISEED )
                     B( JR, JC ) = RMAGN( KBMAGN( JTYPE ) )*
     $                             ZLARND( 4, ISEED )
   80             CONTINUE
   90          CONTINUE
            END IF
*
  100       CONTINUE
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
            DO 120 I = 1, 13
               RESULT( I ) = -ONE
  120       CONTINUE
*
*           Test with and without sorting of eigenvalues
*
            DO 150 ISORT = 0, 1
               IF( ISORT.EQ.0 ) THEN
                  SORT = 'N'
                  RSUB = 0
               ELSE
                  SORT = 'S'
                  RSUB = 5
               END IF
*
*              Call ZGGES3 to compute H, T, Q, Z, alpha, and beta.
*
               CALL ZLACPY( 'Full', N, N, A, LDA, S, LDA )
               CALL ZLACPY( 'Full', N, N, B, LDA, T, LDA )
               NTEST = 1 + RSUB + ISORT
               RESULT( 1+RSUB+ISORT ) = ULPINV
               CALL ZGGES3( 'V', 'V', SORT, ZLCTES, N, S, LDA, T, LDA,
     $                     SDIM, ALPHA, BETA, Q, LDQ, Z, LDQ, WORK,
     $                     LWORK, RWORK, BWORK, IINFO )
               IF( IINFO.NE.0 .AND. IINFO.NE.N+2 ) THEN
                  RESULT( 1+RSUB+ISORT ) = ULPINV
                  WRITE( NOUNIT, FMT = 9999 )'ZGGES3', IINFO, N, JTYPE,
     $               IOLDSD
                  INFO = ABS( IINFO )
                  GO TO 160
               END IF
*
               NTEST = 4 + RSUB
*
*              Do tests 1--4 (or tests 7--9 when reordering )
*
               IF( ISORT.EQ.0 ) THEN
                  CALL ZGET51( 1, N, A, LDA, S, LDA, Q, LDQ, Z, LDQ,
     $                         WORK, RWORK, RESULT( 1 ) )
                  CALL ZGET51( 1, N, B, LDA, T, LDA, Q, LDQ, Z, LDQ,
     $                         WORK, RWORK, RESULT( 2 ) )
               ELSE
                  CALL ZGET54( N, A, LDA, B, LDA, S, LDA, T, LDA, Q,
     $                         LDQ, Z, LDQ, WORK, RESULT( 2+RSUB ) )
               END IF
*
               CALL ZGET51( 3, N, B, LDA, T, LDA, Q, LDQ, Q, LDQ, WORK,
     $                      RWORK, RESULT( 3+RSUB ) )
               CALL ZGET51( 3, N, B, LDA, T, LDA, Z, LDQ, Z, LDQ, WORK,
     $                      RWORK, RESULT( 4+RSUB ) )
*
*              Do test 5 and 6 (or Tests 10 and 11 when reordering):
*              check Schur form of A and compare eigenvalues with
*              diagonals.
*
               NTEST = 6 + RSUB
               TEMP1 = ZERO
*
               DO 130 J = 1, N
                  ILABAD = .FALSE.
                  TEMP2 = ( ABS1( ALPHA( J )-S( J, J ) ) /
     $                    MAX( SAFMIN, ABS1( ALPHA( J ) ), ABS1( S( J,
     $                    J ) ) )+ABS1( BETA( J )-T( J, J ) ) /
     $                    MAX( SAFMIN, ABS1( BETA( J ) ), ABS1( T( J,
     $                    J ) ) ) ) / ULP
*
                  IF( J.LT.N ) THEN
                     IF( S( J+1, J ).NE.ZERO ) THEN
                        ILABAD = .TRUE.
                        RESULT( 5+RSUB ) = ULPINV
                     END IF
                  END IF
                  IF( J.GT.1 ) THEN
                     IF( S( J, J-1 ).NE.ZERO ) THEN
                        ILABAD = .TRUE.
                        RESULT( 5+RSUB ) = ULPINV
                     END IF
                  END IF
                  TEMP1 = MAX( TEMP1, TEMP2 )
                  IF( ILABAD ) THEN
                     WRITE( NOUNIT, FMT = 9998 )J, N, JTYPE, IOLDSD
                  END IF
  130          CONTINUE
               RESULT( 6+RSUB ) = TEMP1
*
               IF( ISORT.GE.1 ) THEN
*
*                 Do test 12
*
                  NTEST = 12
                  RESULT( 12 ) = ZERO
                  KNTEIG = 0
                  DO 140 I = 1, N
                     IF( ZLCTES( ALPHA( I ), BETA( I ) ) )
     $                  KNTEIG = KNTEIG + 1
  140             CONTINUE
                  IF( SDIM.NE.KNTEIG )
     $               RESULT( 13 ) = ULPINV
               END IF
*
  150       CONTINUE
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
  160       CONTINUE
*
            NTESTT = NTESTT + NTEST
*
*           Print out tests which fail.
*
            DO 170 JR = 1, NTEST
               IF( RESULT( JR ).GE.THRESH ) THEN
*
*                 If this is the first test to fail,
*                 print a header to the data file.
*
                  IF( NERRS.EQ.0 ) THEN
                     WRITE( NOUNIT, FMT = 9997 )'ZGS'
*
*                    Matrix types
*
                     WRITE( NOUNIT, FMT = 9996 )
                     WRITE( NOUNIT, FMT = 9995 )
                     WRITE( NOUNIT, FMT = 9994 )'Unitary'
*
*                    Tests performed
*
                     WRITE( NOUNIT, FMT = 9993 )'unitary', '''',
     $                  'transpose', ( '''', J = 1, 8 )
*
                  END IF
                  NERRS = NERRS + 1
                  IF( RESULT( JR ).LT.10000.0D0 ) THEN
                     WRITE( NOUNIT, FMT = 9992 )N, JTYPE, IOLDSD, JR,
     $                  RESULT( JR )
                  ELSE
                     WRITE( NOUNIT, FMT = 9991 )N, JTYPE, IOLDSD, JR,
     $                  RESULT( JR )
                  END IF
               END IF
  170       CONTINUE
*
  180    CONTINUE
  190 CONTINUE
*
*     Summary
*
      CALL ALASVM( 'ZGS', NOUNIT, NERRS, NTESTT, 0 )
*
      WORK( 1 ) = MAXWRK
*
      RETURN
*
 9999 FORMAT( ' ZDRGES3: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 4( I4, ',' ), I5, ')' )
*
 9998 FORMAT( ' ZDRGES3: S not in Schur form at eigenvalue ', I6, '.',
     $      / 9X, 'N=', I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ),
     $      I5, ')' )
*
 9997 FORMAT( / 1X, A3, ' -- Complex Generalized Schur from problem ',
     $      'driver' )
*
 9996 FORMAT( ' Matrix types (see ZDRGES3 for details): ' )
*
 9995 FORMAT( ' Special Matrices:', 23X,
     $      '(J''=transposed Jordan block)',
     $      / '   1=(0,0)  2=(I,0)  3=(0,I)  4=(I,I)  5=(J'',J'')  ',
     $      '6=(diag(J'',I), diag(I,J''))', / ' Diagonal Matrices:  ( ',
     $      'D=diag(0,1,2,...) )', / '   7=(D,I)   9=(large*D, small*I',
     $      ')  11=(large*I, small*D)  13=(large*D, large*I)', /
     $      '   8=(I,D)  10=(small*D, large*I)  12=(small*I, large*D) ',
     $      ' 14=(small*D, small*I)', / '  15=(D, reversed D)' )
 9994 FORMAT( ' Matrices Rotated by Random ', A, ' Matrices U, V:',
     $      / '  16=Transposed Jordan Blocks             19=geometric ',
     $      'alpha, beta=0,1', / '  17=arithm. alpha&beta             ',
     $      '      20=arithmetic alpha, beta=0,1', / '  18=clustered ',
     $      'alpha, beta=0,1            21=random alpha, beta=0,1',
     $      / ' Large & Small Matrices:', / '  22=(large, small)   ',
     $      '23=(small,large)    24=(small,small)    25=(large,large)',
     $      / '  26=random O(1) matrices.' )
*
 9993 FORMAT( / ' Tests performed:  (S is Schur, T is triangular, ',
     $      'Q and Z are ', A, ',', / 19X,
     $      'l and r are the appropriate left and right', / 19X,
     $      'eigenvectors, resp., a is alpha, b is beta, and', / 19X, A,
     $      ' means ', A, '.)', / ' Without ordering: ',
     $      / '  1 = | A - Q S Z', A,
     $      ' | / ( |A| n ulp )      2 = | B - Q T Z', A,
     $      ' | / ( |B| n ulp )', / '  3 = | I - QQ', A,
     $      ' | / ( n ulp )             4 = | I - ZZ', A,
     $      ' | / ( n ulp )', / '  5 = A is in Schur form S',
     $      / '  6 = difference between (alpha,beta)',
     $      ' and diagonals of (S,T)', / ' With ordering: ',
     $      / '  7 = | (A,B) - Q (S,T) Z', A, ' | / ( |(A,B)| n ulp )',
     $      / '  8 = | I - QQ', A,
     $      ' | / ( n ulp )             9 = | I - ZZ', A,
     $      ' | / ( n ulp )', / ' 10 = A is in Schur form S',
     $      / ' 11 = difference between (alpha,beta) and diagonals',
     $      ' of (S,T)', / ' 12 = SDIM is the correct number of ',
     $      'selected eigenvalues', / )
 9992 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I2, ' is', 0P, F8.2 )
 9991 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I2, ' is', 1P, D10.3 )
*
*     End of ZDRGES3
*
      END
