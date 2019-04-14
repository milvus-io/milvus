*> \brief \b CDRGEV
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CDRGEV( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          NOUNIT, A, LDA, B, S, T, Q, LDQ, Z, QE, LDQE,
*                          ALPHA, BETA, ALPHA1, BETA1, WORK, LWORK, RWORK,
*                          RESULT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDQ, LDQE, LWORK, NOUNIT, NSIZES,
*      $                   NTYPES
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), NN( * )
*       REAL               RESULT( * ), RWORK( * )
*       COMPLEX            A( LDA, * ), ALPHA( * ), ALPHA1( * ),
*      $                   B( LDA, * ), BETA( * ), BETA1( * ),
*      $                   Q( LDQ, * ), QE( LDQE, * ), S( LDA, * ),
*      $                   T( LDA, * ), WORK( * ), Z( LDQ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CDRGEV checks the nonsymmetric generalized eigenvalue problem driver
*> routine CGGEV.
*>
*> CGGEV computes for a pair of n-by-n nonsymmetric matrices (A,B) the
*> generalized eigenvalues and, optionally, the left and right
*> eigenvectors.
*>
*> A generalized eigenvalue for a pair of matrices (A,B) is a scalar w
*> or a ratio  alpha/beta = w, such that A - w*B is singular.  It is
*> usually represented as the pair (alpha,beta), as there is reasonable
*> interpretation for beta=0, and even for both being zero.
*>
*> A right generalized eigenvector corresponding to a generalized
*> eigenvalue  w  for a pair of matrices (A,B) is a vector r  such that
*> (A - wB) * r = 0.  A left generalized eigenvector is a vector l such
*> that l**H * (A - wB) = 0, where l**H is the conjugate-transpose of l.
*>
*> When CDRGEV is called, a number of matrix "sizes" ("n's") and a
*> number of matrix "types" are specified.  For each size ("n")
*> and each type of matrix, a pair of matrices (A, B) will be generated
*> and used for testing.  For each matrix pair, the following tests
*> will be performed and compared with the threshold THRESH.
*>
*> Results from CGGEV:
*>
*> (1)  max over all left eigenvalue/-vector pairs (alpha/beta,l) of
*>
*>      | VL**H * (beta A - alpha B) |/( ulp max(|beta A|, |alpha B|) )
*>
*>      where VL**H is the conjugate-transpose of VL.
*>
*> (2)  | |VL(i)| - 1 | / ulp and whether largest component real
*>
*>      VL(i) denotes the i-th column of VL.
*>
*> (3)  max over all left eigenvalue/-vector pairs (alpha/beta,r) of
*>
*>      | (beta A - alpha B) * VR | / ( ulp max(|beta A|, |alpha B|) )
*>
*> (4)  | |VR(i)| - 1 | / ulp and whether largest component real
*>
*>      VR(i) denotes the i-th column of VR.
*>
*> (5)  W(full) = W(partial)
*>      W(full) denotes the eigenvalues computed when both l and r
*>      are also computed, and W(partial) denotes the eigenvalues
*>      computed when only W, only W and r, or only W and l are
*>      computed.
*>
*> (6)  VL(full) = VL(partial)
*>      VL(full) denotes the left eigenvectors computed when both l
*>      and r are computed, and VL(partial) denotes the result
*>      when only l is computed.
*>
*> (7)  VR(full) = VR(partial)
*>      VR(full) denotes the right eigenvectors computed when both l
*>      and r are also computed, and VR(partial) denotes the result
*>      when only l is computed.
*>
*>
*> Test Matrices
*> ---- --------
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
*>          CDRGES does nothing.  NSIZES >= 0.
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
*>          The number of elements in DOTYPE.   If it is zero, CDRGEV
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
*>          next call to CDRGES to continue the same random number
*>          sequence.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>          A test will count as "failed" if the "error", computed as
*>          described above, exceeds THRESH.  Note that the error is
*>          scaled to be O(1), so THRESH should be a reasonably small
*>          multiple of 1, e.g., 10 or 100.  In particular, it should
*>          not depend on the precision (single vs. double) or the size
*>          of the matrix.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] NOUNIT
*> \verbatim
*>          NOUNIT is INTEGER
*>          The FORTRAN unit number for printing out error messages
*>          (e.g., if a routine returns IERR not equal to 0.)
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension(LDA, max(NN))
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
*>          B is COMPLEX array, dimension(LDA, max(NN))
*>          Used to hold the original B matrix.  Used as input only
*>          if NTYPES=MAXTYP+1, DOTYPE(1:MAXTYP)=.FALSE., and
*>          DOTYPE(MAXTYP+1)=.TRUE.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is COMPLEX array, dimension (LDA, max(NN))
*>          The Schur form matrix computed from A by CGGEV.  On exit, S
*>          contains the Schur form matrix corresponding to the matrix
*>          in A.
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is COMPLEX array, dimension (LDA, max(NN))
*>          The upper triangular matrix computed from B by CGGEV.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is COMPLEX array, dimension (LDQ, max(NN))
*>          The (left) eigenvectors matrix computed by CGGEV.
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
*>          Z is COMPLEX array, dimension( LDQ, max(NN) )
*>          The (right) orthogonal matrix computed by CGGEV.
*> \endverbatim
*>
*> \param[out] QE
*> \verbatim
*>          QE is COMPLEX array, dimension( LDQ, max(NN) )
*>          QE holds the computed right or left eigenvectors.
*> \endverbatim
*>
*> \param[in] LDQE
*> \verbatim
*>          LDQE is INTEGER
*>          The leading dimension of QE. LDQE >= max(1,max(NN)).
*> \endverbatim
*>
*> \param[out] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is COMPLEX array, dimension (max(NN))
*>
*>          The generalized eigenvalues of (A,B) computed by CGGEV.
*>          ( ALPHAR(k)+ALPHAI(k)*i ) / BETA(k) is the k-th
*>          generalized eigenvalue of A and B.
*> \endverbatim
*>
*> \param[out] ALPHA1
*> \verbatim
*>          ALPHA1 is COMPLEX array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] BETA1
*> \verbatim
*>          BETA1 is COMPLEX array, dimension (max(NN))
*>
*>          Like ALPHAR, ALPHAI, BETA, these arrays contain the
*>          eigenvalues of A and B, but those computed when CGGEV only
*>          computes a partial eigendecomposition, i.e. not the
*>          eigenvalues and left and right eigenvectors.
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
*>          The number of entries in WORK.  LWORK >= N*(N+1)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (8*N)
*>          Real workspace.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (2)
*>          The values computed by the tests described above.
*>          The values are currently limited to 1/ulp, to avoid overflow.
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
*> \date June 2016
*
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CDRGEV( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   NOUNIT, A, LDA, B, S, T, Q, LDQ, Z, QE, LDQE,
     $                   ALPHA, BETA, ALPHA1, BETA1, WORK, LWORK, RWORK,
     $                   RESULT, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDQ, LDQE, LWORK, NOUNIT, NSIZES,
     $                   NTYPES
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), NN( * )
      REAL               RESULT( * ), RWORK( * )
      COMPLEX            A( LDA, * ), ALPHA( * ), ALPHA1( * ),
     $                   B( LDA, * ), BETA( * ), BETA1( * ),
     $                   Q( LDQ, * ), QE( LDQE, * ), S( LDA, * ),
     $                   T( LDA, * ), WORK( * ), Z( LDQ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 26 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN
      INTEGER            I, IADD, IERR, IN, J, JC, JR, JSIZE, JTYPE,
     $                   MAXWRK, MINWRK, MTYPES, N, N1, NB, NERRS,
     $                   NMATS, NMAX, NTESTT
      REAL               SAFMAX, SAFMIN, ULP, ULPINV
      COMPLEX            CTEMP
*     ..
*     .. Local Arrays ..
      LOGICAL            LASIGN( MAXTYP ), LBSIGN( MAXTYP )
      INTEGER            IOLDSD( 4 ), KADD( 6 ), KAMAGN( MAXTYP ),
     $                   KATYPE( MAXTYP ), KAZERO( MAXTYP ),
     $                   KBMAGN( MAXTYP ), KBTYPE( MAXTYP ),
     $                   KBZERO( MAXTYP ), KCLASS( MAXTYP ),
     $                   KTRIAN( MAXTYP ), KZ1( 6 ), KZ2( 6 )
      REAL               RMAGN( 0: 3 )
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      REAL               SLAMCH
      COMPLEX            CLARND
      EXTERNAL           ILAENV, SLAMCH, CLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALASVM, CGET52, CGGEV, CLACPY, CLARFG, CLASET,
     $                   CLATM4, CUNM2R, SLABAD, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CONJG, MAX, MIN, REAL, SIGN
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
      ELSE IF( LDQE.LE.1 .OR. LDQE.LT.NMAX ) THEN
         INFO = -17
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
         MINWRK = NMAX*( NMAX+1 )
         NB = MAX( 1, ILAENV( 1, 'CGEQRF', ' ', NMAX, NMAX, -1, -1 ),
     $        ILAENV( 1, 'CUNMQR', 'LC', NMAX, NMAX, NMAX, -1 ),
     $        ILAENV( 1, 'CUNGQR', ' ', NMAX, NMAX, NMAX, -1 ) )
         MAXWRK = MAX( 2*NMAX, NMAX*( NB+1 ), NMAX*( NMAX+1 ) )
         WORK( 1 ) = MAXWRK
      END IF
*
      IF( LWORK.LT.MINWRK )
     $   INFO = -23
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CDRGEV', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   RETURN
*
      ULP = SLAMCH( 'Precision' )
      SAFMIN = SLAMCH( 'Safe minimum' )
      SAFMIN = SAFMIN / ULP
      SAFMAX = ONE / SAFMIN
      CALL SLABAD( SAFMIN, SAFMAX )
      ULPINV = ONE / ULP
*
*     The values RMAGN(2:3) depend on N, see below.
*
      RMAGN( 0 ) = ZERO
      RMAGN( 1 ) = ONE
*
*     Loop over sizes, types
*
      NTESTT = 0
      NERRS = 0
      NMATS = 0
*
      DO 220 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         N1 = MAX( 1, N )
         RMAGN( 2 ) = SAFMAX*ULP / REAL( N1 )
         RMAGN( 3 ) = SAFMIN*ULPINV*N1
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 210 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 210
            NMATS = NMATS + 1
*
*           Save ISEED in case of an error.
*
            DO 20 J = 1, 4
               IOLDSD( J ) = ISEED( J )
   20       CONTINUE
*
*           Generate test matrices A and B
*
*           Description of control parameters:
*
*           KCLASS: =1 means w/o rotation, =2 means w/ rotation,
*                   =3 means random.
*           KATYPE: the "type" to be passed to CLATM4 for computing A.
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
     $         GO TO 100
            IERR = 0
            IF( KCLASS( JTYPE ).LT.3 ) THEN
*
*              Generate A (w/o rotation)
*
               IF( ABS( KATYPE( JTYPE ) ).EQ.3 ) THEN
                  IN = 2*( ( N-1 ) / 2 ) + 1
                  IF( IN.NE.N )
     $               CALL CLASET( 'Full', N, N, CZERO, CZERO, A, LDA )
               ELSE
                  IN = N
               END IF
               CALL CLATM4( KATYPE( JTYPE ), IN, KZ1( KAZERO( JTYPE ) ),
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
     $               CALL CLASET( 'Full', N, N, CZERO, CZERO, B, LDA )
               ELSE
                  IN = N
               END IF
               CALL CLATM4( KBTYPE( JTYPE ), IN, KZ1( KBZERO( JTYPE ) ),
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
                  DO 40 JC = 1, N - 1
                     DO 30 JR = JC, N
                        Q( JR, JC ) = CLARND( 3, ISEED )
                        Z( JR, JC ) = CLARND( 3, ISEED )
   30                CONTINUE
                     CALL CLARFG( N+1-JC, Q( JC, JC ), Q( JC+1, JC ), 1,
     $                            WORK( JC ) )
                     WORK( 2*N+JC ) = SIGN( ONE, REAL( Q( JC, JC ) ) )
                     Q( JC, JC ) = CONE
                     CALL CLARFG( N+1-JC, Z( JC, JC ), Z( JC+1, JC ), 1,
     $                            WORK( N+JC ) )
                     WORK( 3*N+JC ) = SIGN( ONE, REAL( Z( JC, JC ) ) )
                     Z( JC, JC ) = CONE
   40             CONTINUE
                  CTEMP = CLARND( 3, ISEED )
                  Q( N, N ) = CONE
                  WORK( N ) = CZERO
                  WORK( 3*N ) = CTEMP / ABS( CTEMP )
                  CTEMP = CLARND( 3, ISEED )
                  Z( N, N ) = CONE
                  WORK( 2*N ) = CZERO
                  WORK( 4*N ) = CTEMP / ABS( CTEMP )
*
*                 Apply the diagonal matrices
*
                  DO 60 JC = 1, N
                     DO 50 JR = 1, N
                        A( JR, JC ) = WORK( 2*N+JR )*
     $                                CONJG( WORK( 3*N+JC ) )*
     $                                A( JR, JC )
                        B( JR, JC ) = WORK( 2*N+JR )*
     $                                CONJG( WORK( 3*N+JC ) )*
     $                                B( JR, JC )
   50                CONTINUE
   60             CONTINUE
                  CALL CUNM2R( 'L', 'N', N, N, N-1, Q, LDQ, WORK, A,
     $                         LDA, WORK( 2*N+1 ), IERR )
                  IF( IERR.NE.0 )
     $               GO TO 90
                  CALL CUNM2R( 'R', 'C', N, N, N-1, Z, LDQ, WORK( N+1 ),
     $                         A, LDA, WORK( 2*N+1 ), IERR )
                  IF( IERR.NE.0 )
     $               GO TO 90
                  CALL CUNM2R( 'L', 'N', N, N, N-1, Q, LDQ, WORK, B,
     $                         LDA, WORK( 2*N+1 ), IERR )
                  IF( IERR.NE.0 )
     $               GO TO 90
                  CALL CUNM2R( 'R', 'C', N, N, N-1, Z, LDQ, WORK( N+1 ),
     $                         B, LDA, WORK( 2*N+1 ), IERR )
                  IF( IERR.NE.0 )
     $               GO TO 90
               END IF
            ELSE
*
*              Random matrices
*
               DO 80 JC = 1, N
                  DO 70 JR = 1, N
                     A( JR, JC ) = RMAGN( KAMAGN( JTYPE ) )*
     $                             CLARND( 4, ISEED )
                     B( JR, JC ) = RMAGN( KBMAGN( JTYPE ) )*
     $                             CLARND( 4, ISEED )
   70             CONTINUE
   80          CONTINUE
            END IF
*
   90       CONTINUE
*
            IF( IERR.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'Generator', IERR, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IERR )
               RETURN
            END IF
*
  100       CONTINUE
*
            DO 110 I = 1, 7
               RESULT( I ) = -ONE
  110       CONTINUE
*
*           Call CGGEV to compute eigenvalues and eigenvectors.
*
            CALL CLACPY( ' ', N, N, A, LDA, S, LDA )
            CALL CLACPY( ' ', N, N, B, LDA, T, LDA )
            CALL CGGEV( 'V', 'V', N, S, LDA, T, LDA, ALPHA, BETA, Q,
     $                  LDQ, Z, LDQ, WORK, LWORK, RWORK, IERR )
            IF( IERR.NE.0 .AND. IERR.NE.N+1 ) THEN
               RESULT( 1 ) = ULPINV
               WRITE( NOUNIT, FMT = 9999 )'CGGEV1', IERR, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IERR )
               GO TO 190
            END IF
*
*           Do the tests (1) and (2)
*
            CALL CGET52( .TRUE., N, A, LDA, B, LDA, Q, LDQ, ALPHA, BETA,
     $                   WORK, RWORK, RESULT( 1 ) )
            IF( RESULT( 2 ).GT.THRESH ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Left', 'CGGEV1',
     $            RESULT( 2 ), N, JTYPE, IOLDSD
            END IF
*
*           Do the tests (3) and (4)
*
            CALL CGET52( .FALSE., N, A, LDA, B, LDA, Z, LDQ, ALPHA,
     $                   BETA, WORK, RWORK, RESULT( 3 ) )
            IF( RESULT( 4 ).GT.THRESH ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Right', 'CGGEV1',
     $            RESULT( 4 ), N, JTYPE, IOLDSD
            END IF
*
*           Do test (5)
*
            CALL CLACPY( ' ', N, N, A, LDA, S, LDA )
            CALL CLACPY( ' ', N, N, B, LDA, T, LDA )
            CALL CGGEV( 'N', 'N', N, S, LDA, T, LDA, ALPHA1, BETA1, Q,
     $                  LDQ, Z, LDQ, WORK, LWORK, RWORK, IERR )
            IF( IERR.NE.0 .AND. IERR.NE.N+1 ) THEN
               RESULT( 1 ) = ULPINV
               WRITE( NOUNIT, FMT = 9999 )'CGGEV2', IERR, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IERR )
               GO TO 190
            END IF
*
            DO 120 J = 1, N
               IF( ALPHA( J ).NE.ALPHA1( J ) .OR. BETA( J ).NE.
     $             BETA1( J ) )RESULT( 5 ) = ULPINV
  120       CONTINUE
*
*           Do test (6): Compute eigenvalues and left eigenvectors,
*           and test them
*
            CALL CLACPY( ' ', N, N, A, LDA, S, LDA )
            CALL CLACPY( ' ', N, N, B, LDA, T, LDA )
            CALL CGGEV( 'V', 'N', N, S, LDA, T, LDA, ALPHA1, BETA1, QE,
     $                  LDQE, Z, LDQ, WORK, LWORK, RWORK, IERR )
            IF( IERR.NE.0 .AND. IERR.NE.N+1 ) THEN
               RESULT( 1 ) = ULPINV
               WRITE( NOUNIT, FMT = 9999 )'CGGEV3', IERR, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IERR )
               GO TO 190
            END IF
*
            DO 130 J = 1, N
               IF( ALPHA( J ).NE.ALPHA1( J ) .OR. BETA( J ).NE.
     $             BETA1( J ) )RESULT( 6 ) = ULPINV
  130       CONTINUE
*
            DO 150 J = 1, N
               DO 140 JC = 1, N
                  IF( Q( J, JC ).NE.QE( J, JC ) )
     $               RESULT( 6 ) = ULPINV
  140          CONTINUE
  150       CONTINUE
*
*           Do test (7): Compute eigenvalues and right eigenvectors,
*           and test them
*
            CALL CLACPY( ' ', N, N, A, LDA, S, LDA )
            CALL CLACPY( ' ', N, N, B, LDA, T, LDA )
            CALL CGGEV( 'N', 'V', N, S, LDA, T, LDA, ALPHA1, BETA1, Q,
     $                  LDQ, QE, LDQE, WORK, LWORK, RWORK, IERR )
            IF( IERR.NE.0 .AND. IERR.NE.N+1 ) THEN
               RESULT( 1 ) = ULPINV
               WRITE( NOUNIT, FMT = 9999 )'CGGEV4', IERR, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IERR )
               GO TO 190
            END IF
*
            DO 160 J = 1, N
               IF( ALPHA( J ).NE.ALPHA1( J ) .OR. BETA( J ).NE.
     $             BETA1( J ) )RESULT( 7 ) = ULPINV
  160       CONTINUE
*
            DO 180 J = 1, N
               DO 170 JC = 1, N
                  IF( Z( J, JC ).NE.QE( J, JC ) )
     $               RESULT( 7 ) = ULPINV
  170          CONTINUE
  180       CONTINUE
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
  190       CONTINUE
*
            NTESTT = NTESTT + 7
*
*           Print out tests which fail.
*
            DO 200 JR = 1, 7
               IF( RESULT( JR ).GE.THRESH ) THEN
*
*                 If this is the first test to fail,
*                 print a header to the data file.
*
                  IF( NERRS.EQ.0 ) THEN
                     WRITE( NOUNIT, FMT = 9997 )'CGV'
*
*                    Matrix types
*
                     WRITE( NOUNIT, FMT = 9996 )
                     WRITE( NOUNIT, FMT = 9995 )
                     WRITE( NOUNIT, FMT = 9994 )'Orthogonal'
*
*                    Tests performed
*
                     WRITE( NOUNIT, FMT = 9993 )
*
                  END IF
                  NERRS = NERRS + 1
                  IF( RESULT( JR ).LT.10000.0 ) THEN
                     WRITE( NOUNIT, FMT = 9992 )N, JTYPE, IOLDSD, JR,
     $                  RESULT( JR )
                  ELSE
                     WRITE( NOUNIT, FMT = 9991 )N, JTYPE, IOLDSD, JR,
     $                  RESULT( JR )
                  END IF
               END IF
  200       CONTINUE
*
  210    CONTINUE
  220 CONTINUE
*
*     Summary
*
      CALL ALASVM( 'CGV', NOUNIT, NERRS, NTESTT, 0 )
*
      WORK( 1 ) = MAXWRK
*
      RETURN
*
 9999 FORMAT( ' CDRGEV: ', A, ' returned INFO=', I6, '.', / 3X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
 9998 FORMAT( ' CDRGEV: ', A, ' Eigenvectors from ', A, ' incorrectly ',
     $      'normalized.', / ' Bits of error=', 0P, G10.3, ',', 3X,
     $      'N=', I4, ', JTYPE=', I3, ', ISEED=(', 3( I4, ',' ), I5,
     $      ')' )
*
 9997 FORMAT( / 1X, A3, ' -- Complex Generalized eigenvalue problem ',
     $      'driver' )
*
 9996 FORMAT( ' Matrix types (see CDRGEV for details): ' )
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
 9993 FORMAT( / ' Tests performed:    ',
     $      / ' 1 = max | ( b A - a B )''*l | / const.,',
     $      / ' 2 = | |VR(i)| - 1 | / ulp,',
     $      / ' 3 = max | ( b A - a B )*r | / const.',
     $      / ' 4 = | |VL(i)| - 1 | / ulp,',
     $      / ' 5 = 0 if W same no matter if r or l computed,',
     $      / ' 6 = 0 if l same no matter if l computed,',
     $      / ' 7 = 0 if r same no matter if r computed,', / 1X )
 9992 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I2, ' is', 0P, F8.2 )
 9991 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I2, ' is', 1P, E10.3 )
*
*     End of CDRGEV
*
      END
