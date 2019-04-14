*> \brief \b ZCHKGG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZCHKGG( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          TSTDIF, THRSHN, NOUNIT, A, LDA, B, H, T, S1,
*                          S2, P1, P2, U, LDU, V, Q, Z, ALPHA1, BETA1,
*                          ALPHA3, BETA3, EVECTL, EVECTR, WORK, LWORK,
*                          RWORK, LLWORK, RESULT, INFO )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTDIF
*       INTEGER            INFO, LDA, LDU, LWORK, NOUNIT, NSIZES, NTYPES
*       DOUBLE PRECISION   THRESH, THRSHN
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * ), LLWORK( * )
*       INTEGER            ISEED( 4 ), NN( * )
*       DOUBLE PRECISION   RESULT( 15 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), ALPHA1( * ), ALPHA3( * ),
*      $                   B( LDA, * ), BETA1( * ), BETA3( * ),
*      $                   EVECTL( LDU, * ), EVECTR( LDU, * ),
*      $                   H( LDA, * ), P1( LDA, * ), P2( LDA, * ),
*      $                   Q( LDU, * ), S1( LDA, * ), S2( LDA, * ),
*      $                   T( LDA, * ), U( LDU, * ), V( LDU, * ),
*      $                   WORK( * ), Z( LDU, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZCHKGG  checks the nonsymmetric generalized eigenvalue problem
*> routines.
*>                                H          H        H
*> ZGGHRD factors A and B as U H V  and U T V , where   means conjugate
*> transpose, H is hessenberg, T is triangular and U and V are unitary.
*>
*>                                 H          H
*> ZHGEQZ factors H and T as  Q S Z  and Q P Z , where P and S are upper
*> triangular and Q and Z are unitary.  It also computes the generalized
*> eigenvalues (alpha(1),beta(1)),...,(alpha(n),beta(n)), where
*> alpha(j)=S(j,j) and beta(j)=P(j,j) -- thus, w(j) = alpha(j)/beta(j)
*> is a root of the generalized eigenvalue problem
*>
*>     det( A - w(j) B ) = 0
*>
*> and m(j) = beta(j)/alpha(j) is a root of the essentially equivalent
*> problem
*>
*>     det( m(j) A - B ) = 0
*>
*> ZTGEVC computes the matrix L of left eigenvectors and the matrix R
*> of right eigenvectors for the matrix pair ( S, P ).  In the
*> description below,  l and r are left and right eigenvectors
*> corresponding to the generalized eigenvalues (alpha,beta).
*>
*> When ZCHKGG is called, a number of matrix "sizes" ("n's") and a
*> number of matrix "types" are specified.  For each size ("n")
*> and each type of matrix, one matrix will be generated and used
*> to test the nonsymmetric eigenroutines.  For each matrix, 13
*> tests will be performed.  The first twelve "test ratios" should be
*> small -- O(1).  They will be compared with the threshold THRESH:
*>
*>                  H
*> (1)   | A - U H V  | / ( |A| n ulp )
*>
*>                  H
*> (2)   | B - U T V  | / ( |B| n ulp )
*>
*>               H
*> (3)   | I - UU  | / ( n ulp )
*>
*>               H
*> (4)   | I - VV  | / ( n ulp )
*>
*>                  H
*> (5)   | H - Q S Z  | / ( |H| n ulp )
*>
*>                  H
*> (6)   | T - Q P Z  | / ( |T| n ulp )
*>
*>               H
*> (7)   | I - QQ  | / ( n ulp )
*>
*>               H
*> (8)   | I - ZZ  | / ( n ulp )
*>
*> (9)   max over all left eigenvalue/-vector pairs (beta/alpha,l) of
*>                           H
*>       | (beta A - alpha B) l | / ( ulp max( |beta A|, |alpha B| ) )
*>
*> (10)  max over all left eigenvalue/-vector pairs (beta/alpha,l') of
*>                           H
*>       | (beta H - alpha T) l' | / ( ulp max( |beta H|, |alpha T| ) )
*>
*>       where the eigenvectors l' are the result of passing Q to
*>       DTGEVC and back transforming (JOB='B').
*>
*> (11)  max over all right eigenvalue/-vector pairs (beta/alpha,r) of
*>
*>       | (beta A - alpha B) r | / ( ulp max( |beta A|, |alpha B| ) )
*>
*> (12)  max over all right eigenvalue/-vector pairs (beta/alpha,r') of
*>
*>       | (beta H - alpha T) r' | / ( ulp max( |beta H|, |alpha T| ) )
*>
*>       where the eigenvectors r' are the result of passing Z to
*>       DTGEVC and back transforming (JOB='B').
*>
*> The last three test ratios will usually be small, but there is no
*> mathematical requirement that they be so.  They are therefore
*> compared with THRESH only if TSTDIF is .TRUE.
*>
*> (13)  | S(Q,Z computed) - S(Q,Z not computed) | / ( |S| ulp )
*>
*> (14)  | P(Q,Z computed) - P(Q,Z not computed) | / ( |P| ulp )
*>
*> (15)  max( |alpha(Q,Z computed) - alpha(Q,Z not computed)|/|S| ,
*>            |beta(Q,Z computed) - beta(Q,Z not computed)|/|P| ) / ulp
*>
*> In addition, the normalization of L and R are checked, and compared
*> with the threshold THRSHN.
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
*> (7)  ( D, I )         where D is P*D1, P is a random unitary diagonal
*>                       matrix (i.e., with random magnitude 1 entries
*>                       on the diagonal), and D1=diag( 0, 1,..., N-1 )
*>                       (i.e., a diagonal matrix with D1(1,1)=0,
*>                       D1(2,2)=1, ..., D1(N,N)=N-1.)
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
*> (15) ( D1, D2 )        where D1=P*diag( 0, 0, 1, ..., N-3, 0 ) and
*>                        D2=Q*diag( 0, N-3, N-4,..., 1, 0, 0 ), and
*>                        P and Q are random unitary diagonal matrices.
*>           t   t
*> (16) U ( J , J ) V     where U and V are random unitary matrices.
*>
*> (17) U ( T1, T2 ) V    where T1 and T2 are upper triangular matrices
*>                        with random O(1) entries above the diagonal
*>                        and diagonal entries diag(T1) =
*>                        P*( 0, 0, 1, ..., N-3, 0 ) and diag(T2) =
*>                        Q*( 0, N-3, N-4,..., 1, 0, 0 )
*>
*> (18) U ( T1, T2 ) V    diag(T1) = ( 0, 0, 1, 1, s, ..., s, 0 )
*>                        diag(T2) = ( 0, 1, 0, 1,..., 1, 0 )
*>                        s = machine precision.
*>
*> (19) U ( T1, T2 ) V    diag(T1)=( 0,0,1,1, 1-d, ..., 1-(N-5)*d=s, 0 )
*>                        diag(T2) = ( 0, 1, 0, 1, ..., 1, 0 )
*>
*>                                                        N-5
*> (20) U ( T1, T2 ) V    diag(T1)=( 0, 0, 1, 1, a, ..., a   =s, 0 )
*>                        diag(T2) = ( 0, 1, 0, 1, ..., 1, 0, 0 )
*>
*> (21) U ( T1, T2 ) V    diag(T1)=( 0, 0, 1, r1, r2, ..., r(N-4), 0 )
*>                        diag(T2) = ( 0, 1, 0, 1, ..., 1, 0, 0 )
*>                        where r1,..., r(N-4) are random.
*>
*> (22) U ( big*T1, small*T2 ) V   diag(T1) = P*( 0, 0, 1, ..., N-3, 0 )
*>                                 diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (23) U ( small*T1, big*T2 ) V   diag(T1) = P*( 0, 0, 1, ..., N-3, 0 )
*>                                 diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (24) U ( small*T1, small*T2 ) V diag(T1) = P*( 0, 0, 1, ..., N-3, 0 )
*>                                 diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (25) U ( big*T1, big*T2 ) V     diag(T1) = P*( 0, 0, 1, ..., N-3, 0 )
*>                                 diag(T2) = ( 0, 1, ..., 1, 0, 0 )
*>
*> (26) U ( T1, T2 ) V     where T1 and T2 are random upper-triangular
*>                         matrices.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          ZCHKGG does nothing.  It must be at least zero.
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
*>          The number of elements in DOTYPE.   If it is zero, ZCHKGG
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
*>          next call to ZCHKGG to continue the same random number
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
*> \param[in] TSTDIF
*> \verbatim
*>          TSTDIF is LOGICAL
*>          Specifies whether test ratios 13-15 will be computed and
*>          compared with THRESH.
*>          = .FALSE.: Only test ratios 1-12 will be computed and tested.
*>                     Ratios 13-15 will be set to zero.
*>          = .TRUE.:  All the test ratios 1-15 will be computed and
*>                     tested.
*> \endverbatim
*>
*> \param[in] THRSHN
*> \verbatim
*>          THRSHN is DOUBLE PRECISION
*>          Threshold for reporting eigenvector normalization error.
*>          If the normalization of any eigenvector differs from 1 by
*>          more than THRSHN*ulp, then a special error message will be
*>          printed.  (This is handled separately from the other tests,
*>          since only a compiler or programming error should cause an
*>          error message, at least if THRSHN is at least 5--10.)
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
*>          A is COMPLEX*16 array, dimension (LDA, max(NN))
*>          Used to hold the original A matrix.  Used as input only
*>          if NTYPES=MAXTYP+1, DOTYPE(1:MAXTYP)=.FALSE., and
*>          DOTYPE(MAXTYP+1)=.TRUE.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, B, H, T, S1, P1, S2, and P2.
*>          It must be at least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDA, max(NN))
*>          Used to hold the original B matrix.  Used as input only
*>          if NTYPES=MAXTYP+1, DOTYPE(1:MAXTYP)=.FALSE., and
*>          DOTYPE(MAXTYP+1)=.TRUE.
*> \endverbatim
*>
*> \param[out] H
*> \verbatim
*>          H is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The upper Hessenberg matrix computed from A by ZGGHRD.
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The upper triangular matrix computed from B by ZGGHRD.
*> \endverbatim
*>
*> \param[out] S1
*> \verbatim
*>          S1 is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The Schur (upper triangular) matrix computed from H by ZHGEQZ
*>          when Q and Z are also computed.
*> \endverbatim
*>
*> \param[out] S2
*> \verbatim
*>          S2 is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The Schur (upper triangular) matrix computed from H by ZHGEQZ
*>          when Q and Z are not computed.
*> \endverbatim
*>
*> \param[out] P1
*> \verbatim
*>          P1 is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The upper triangular matrix computed from T by ZHGEQZ
*>          when Q and Z are also computed.
*> \endverbatim
*>
*> \param[out] P2
*> \verbatim
*>          P2 is COMPLEX*16 array, dimension (LDA, max(NN))
*>          The upper triangular matrix computed from T by ZHGEQZ
*>          when Q and Z are not computed.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX*16 array, dimension (LDU, max(NN))
*>          The (left) unitary matrix computed by ZGGHRD.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U, V, Q, Z, EVECTL, and EVEZTR.  It
*>          must be at least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is COMPLEX*16 array, dimension (LDU, max(NN))
*>          The (right) unitary matrix computed by ZGGHRD.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDU, max(NN))
*>          The (left) unitary matrix computed by ZHGEQZ.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension (LDU, max(NN))
*>          The (left) unitary matrix computed by ZHGEQZ.
*> \endverbatim
*>
*> \param[out] ALPHA1
*> \verbatim
*>          ALPHA1 is COMPLEX*16 array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] BETA1
*> \verbatim
*>          BETA1 is COMPLEX*16 array, dimension (max(NN))
*>          The generalized eigenvalues of (A,B) computed by ZHGEQZ
*>          when Q, Z, and the full Schur matrices are computed.
*> \endverbatim
*>
*> \param[out] ALPHA3
*> \verbatim
*>          ALPHA3 is COMPLEX*16 array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] BETA3
*> \verbatim
*>          BETA3 is COMPLEX*16 array, dimension (max(NN))
*>          The generalized eigenvalues of (A,B) computed by ZHGEQZ
*>          when neither Q, Z, nor the Schur matrices are computed.
*> \endverbatim
*>
*> \param[out] EVECTL
*> \verbatim
*>          EVECTL is COMPLEX*16 array, dimension (LDU, max(NN))
*>          The (lower triangular) left eigenvector matrix for the
*>          matrices in S1 and P1.
*> \endverbatim
*>
*> \param[out] EVECTR
*> \verbatim
*>          EVECTR is COMPLEX*16 array, dimension (LDU, max(NN))
*>          The (upper triangular) right eigenvector matrix for the
*>          matrices in S1 and P1.
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
*>          max( 4*N, 2 * N**2, 1 ), for all N=NN(j).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (2*max(NN))
*> \endverbatim
*>
*> \param[out] LLWORK
*> \verbatim
*>          LLWORK is LOGICAL array, dimension (max(NN))
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (15)
*>          The values computed by the tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZCHKGG( NSIZES, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   TSTDIF, THRSHN, NOUNIT, A, LDA, B, H, T, S1,
     $                   S2, P1, P2, U, LDU, V, Q, Z, ALPHA1, BETA1,
     $                   ALPHA3, BETA3, EVECTL, EVECTR, WORK, LWORK,
     $                   RWORK, LLWORK, RESULT, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTDIF
      INTEGER            INFO, LDA, LDU, LWORK, NOUNIT, NSIZES, NTYPES
      DOUBLE PRECISION   THRESH, THRSHN
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * ), LLWORK( * )
      INTEGER            ISEED( 4 ), NN( * )
      DOUBLE PRECISION   RESULT( 15 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), ALPHA1( * ), ALPHA3( * ),
     $                   B( LDA, * ), BETA1( * ), BETA3( * ),
     $                   EVECTL( LDU, * ), EVECTR( LDU, * ),
     $                   H( LDA, * ), P1( LDA, * ), P2( LDA, * ),
     $                   Q( LDU, * ), S1( LDA, * ), S2( LDA, * ),
     $                   T( LDA, * ), U( LDU, * ), V( LDU, * ),
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
      PARAMETER          ( MAXTYP = 26 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADNN
      INTEGER            I1, IADD, IINFO, IN, J, JC, JR, JSIZE, JTYPE,
     $                   LWKOPT, MTYPES, N, N1, NERRS, NMATS, NMAX,
     $                   NTEST, NTESTT
      DOUBLE PRECISION   ANORM, BNORM, SAFMAX, SAFMIN, TEMP1, TEMP2,
     $                   ULP, ULPINV
      COMPLEX*16         CTEMP
*     ..
*     .. Local Arrays ..
      LOGICAL            LASIGN( MAXTYP ), LBSIGN( MAXTYP )
      INTEGER            IOLDSD( 4 ), KADD( 6 ), KAMAGN( MAXTYP ),
     $                   KATYPE( MAXTYP ), KAZERO( MAXTYP ),
     $                   KBMAGN( MAXTYP ), KBTYPE( MAXTYP ),
     $                   KBZERO( MAXTYP ), KCLASS( MAXTYP ),
     $                   KTRIAN( MAXTYP ), KZ1( 6 ), KZ2( 6 )
      DOUBLE PRECISION   DUMMA( 4 ), RMAGN( 0: 3 )
      COMPLEX*16         CDUMMA( 4 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE
      COMPLEX*16         ZLARND
      EXTERNAL           DLAMCH, ZLANGE, ZLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLASUM, XERBLA, ZGEQR2, ZGET51, ZGET52,
     $                   ZGGHRD, ZHGEQZ, ZLACPY, ZLARFG, ZLASET, ZLATM4,
     $                   ZTGEVC, ZUNM2R
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, MAX, MIN, SIGN
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
      LWKOPT = MAX( 2*NMAX*NMAX, 4*NMAX, 1 )
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
         INFO = -10
      ELSE IF( LDU.LE.1 .OR. LDU.LT.NMAX ) THEN
         INFO = -19
      ELSE IF( LWKOPT.GT.LWORK ) THEN
         INFO = -30
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZCHKGG', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( NSIZES.EQ.0 .OR. NTYPES.EQ.0 )
     $   RETURN
*
      SAFMIN = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
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
*     Loop over sizes, types
*
      NTESTT = 0
      NERRS = 0
      NMATS = 0
*
      DO 240 JSIZE = 1, NSIZES
         N = NN( JSIZE )
         N1 = MAX( 1, N )
         RMAGN( 2 ) = SAFMAX*ULP / DBLE( N1 )
         RMAGN( 3 ) = SAFMIN*ULPINV*N1
*
         IF( NSIZES.NE.1 ) THEN
            MTYPES = MIN( MAXTYP, NTYPES )
         ELSE
            MTYPES = MIN( MAXTYP+1, NTYPES )
         END IF
*
         DO 230 JTYPE = 1, MTYPES
            IF( .NOT.DOTYPE( JTYPE ) )
     $         GO TO 230
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
            DO 30 J = 1, 15
               RESULT( J ) = ZERO
   30       CONTINUE
*
*           Compute A and B
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
*           RMAGN:  used to implement KAMAGN and KBMAGN.
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
     $                      RMAGN( KTRIAN( JTYPE )*KAMAGN( JTYPE ) ), 4,
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
     $                      RMAGN( KTRIAN( JTYPE )*KBMAGN( JTYPE ) ), 4,
     $                      ISEED, B, LDA )
               IADD = KADD( KBZERO( JTYPE ) )
               IF( IADD.NE.0 )
     $            B( IADD, IADD ) = RMAGN( KBMAGN( JTYPE ) )
*
               IF( KCLASS( JTYPE ).EQ.2 .AND. N.GT.0 ) THEN
*
*                 Include rotations
*
*                 Generate U, V as Householder transformations times a
*                 diagonal matrix.  (Note that ZLARFG makes U(j,j) and
*                 V(j,j) real.)
*
                  DO 50 JC = 1, N - 1
                     DO 40 JR = JC, N
                        U( JR, JC ) = ZLARND( 3, ISEED )
                        V( JR, JC ) = ZLARND( 3, ISEED )
   40                CONTINUE
                     CALL ZLARFG( N+1-JC, U( JC, JC ), U( JC+1, JC ), 1,
     $                            WORK( JC ) )
                     WORK( 2*N+JC ) = SIGN( ONE, DBLE( U( JC, JC ) ) )
                     U( JC, JC ) = CONE
                     CALL ZLARFG( N+1-JC, V( JC, JC ), V( JC+1, JC ), 1,
     $                            WORK( N+JC ) )
                     WORK( 3*N+JC ) = SIGN( ONE, DBLE( V( JC, JC ) ) )
                     V( JC, JC ) = CONE
   50             CONTINUE
                  CTEMP = ZLARND( 3, ISEED )
                  U( N, N ) = CONE
                  WORK( N ) = CZERO
                  WORK( 3*N ) = CTEMP / ABS( CTEMP )
                  CTEMP = ZLARND( 3, ISEED )
                  V( N, N ) = CONE
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
                  CALL ZUNM2R( 'L', 'N', N, N, N-1, U, LDU, WORK, A,
     $                         LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
                  CALL ZUNM2R( 'R', 'C', N, N, N-1, V, LDU, WORK( N+1 ),
     $                         A, LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
                  CALL ZUNM2R( 'L', 'N', N, N, N-1, U, LDU, WORK, B,
     $                         LDA, WORK( 2*N+1 ), IINFO )
                  IF( IINFO.NE.0 )
     $               GO TO 100
                  CALL ZUNM2R( 'R', 'C', N, N, N-1, V, LDU, WORK( N+1 ),
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
            ANORM = ZLANGE( '1', N, N, A, LDA, RWORK )
            BNORM = ZLANGE( '1', N, N, B, LDA, RWORK )
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
*           Call ZGEQR2, ZUNM2R, and ZGGHRD to compute H, T, U, and V
*
            CALL ZLACPY( ' ', N, N, A, LDA, H, LDA )
            CALL ZLACPY( ' ', N, N, B, LDA, T, LDA )
            NTEST = 1
            RESULT( 1 ) = ULPINV
*
            CALL ZGEQR2( N, N, T, LDA, WORK, WORK( N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZGEQR2', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZUNM2R( 'L', 'C', N, N, N, T, LDA, WORK, H, LDA,
     $                   WORK( N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUNM2R', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZLASET( 'Full', N, N, CZERO, CONE, U, LDU )
            CALL ZUNM2R( 'R', 'N', N, N, N, T, LDA, WORK, U, LDU,
     $                   WORK( N+1 ), IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZUNM2R', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZGGHRD( 'V', 'I', N, 1, N, H, LDA, T, LDA, U, LDU, V,
     $                   LDU, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZGGHRD', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
            NTEST = 4
*
*           Do tests 1--4
*
            CALL ZGET51( 1, N, A, LDA, H, LDA, U, LDU, V, LDU, WORK,
     $                   RWORK, RESULT( 1 ) )
            CALL ZGET51( 1, N, B, LDA, T, LDA, U, LDU, V, LDU, WORK,
     $                   RWORK, RESULT( 2 ) )
            CALL ZGET51( 3, N, B, LDA, T, LDA, U, LDU, U, LDU, WORK,
     $                   RWORK, RESULT( 3 ) )
            CALL ZGET51( 3, N, B, LDA, T, LDA, V, LDU, V, LDU, WORK,
     $                   RWORK, RESULT( 4 ) )
*
*           Call ZHGEQZ to compute S1, P1, S2, P2, Q, and Z, do tests.
*
*           Compute T1 and UZ
*
*           Eigenvalues only
*
            CALL ZLACPY( ' ', N, N, H, LDA, S2, LDA )
            CALL ZLACPY( ' ', N, N, T, LDA, P2, LDA )
            NTEST = 5
            RESULT( 5 ) = ULPINV
*
            CALL ZHGEQZ( 'E', 'N', 'N', N, 1, N, S2, LDA, P2, LDA,
     $                   ALPHA3, BETA3, Q, LDU, Z, LDU, WORK, LWORK,
     $                   RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHGEQZ(E)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
*           Eigenvalues and Full Schur Form
*
            CALL ZLACPY( ' ', N, N, H, LDA, S2, LDA )
            CALL ZLACPY( ' ', N, N, T, LDA, P2, LDA )
*
            CALL ZHGEQZ( 'S', 'N', 'N', N, 1, N, S2, LDA, P2, LDA,
     $                   ALPHA1, BETA1, Q, LDU, Z, LDU, WORK, LWORK,
     $                   RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHGEQZ(S)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
*           Eigenvalues, Schur Form, and Schur Vectors
*
            CALL ZLACPY( ' ', N, N, H, LDA, S1, LDA )
            CALL ZLACPY( ' ', N, N, T, LDA, P1, LDA )
*
            CALL ZHGEQZ( 'S', 'I', 'I', N, 1, N, S1, LDA, P1, LDA,
     $                   ALPHA1, BETA1, Q, LDU, Z, LDU, WORK, LWORK,
     $                   RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZHGEQZ(V)', IINFO, N, JTYPE,
     $            IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            NTEST = 8
*
*           Do Tests 5--8
*
            CALL ZGET51( 1, N, H, LDA, S1, LDA, Q, LDU, Z, LDU, WORK,
     $                   RWORK, RESULT( 5 ) )
            CALL ZGET51( 1, N, T, LDA, P1, LDA, Q, LDU, Z, LDU, WORK,
     $                   RWORK, RESULT( 6 ) )
            CALL ZGET51( 3, N, T, LDA, P1, LDA, Q, LDU, Q, LDU, WORK,
     $                   RWORK, RESULT( 7 ) )
            CALL ZGET51( 3, N, T, LDA, P1, LDA, Z, LDU, Z, LDU, WORK,
     $                   RWORK, RESULT( 8 ) )
*
*           Compute the Left and Right Eigenvectors of (S1,P1)
*
*           9: Compute the left eigenvector Matrix without
*              back transforming:
*
            NTEST = 9
            RESULT( 9 ) = ULPINV
*
*           To test "SELECT" option, compute half of the eigenvectors
*           in one call, and half in another
*
            I1 = N / 2
            DO 120 J = 1, I1
               LLWORK( J ) = .TRUE.
  120       CONTINUE
            DO 130 J = I1 + 1, N
               LLWORK( J ) = .FALSE.
  130       CONTINUE
*
            CALL ZTGEVC( 'L', 'S', LLWORK, N, S1, LDA, P1, LDA, EVECTL,
     $                   LDU, CDUMMA, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTGEVC(L,S1)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            I1 = IN
            DO 140 J = 1, I1
               LLWORK( J ) = .FALSE.
  140       CONTINUE
            DO 150 J = I1 + 1, N
               LLWORK( J ) = .TRUE.
  150       CONTINUE
*
            CALL ZTGEVC( 'L', 'S', LLWORK, N, S1, LDA, P1, LDA,
     $                   EVECTL( 1, I1+1 ), LDU, CDUMMA, LDU, N, IN,
     $                   WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTGEVC(L,S2)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZGET52( .TRUE., N, S1, LDA, P1, LDA, EVECTL, LDU,
     $                   ALPHA1, BETA1, WORK, RWORK, DUMMA( 1 ) )
            RESULT( 9 ) = DUMMA( 1 )
            IF( DUMMA( 2 ).GT.THRSHN ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Left', 'ZTGEVC(HOWMNY=S)',
     $            DUMMA( 2 ), N, JTYPE, IOLDSD
            END IF
*
*           10: Compute the left eigenvector Matrix with
*               back transforming:
*
            NTEST = 10
            RESULT( 10 ) = ULPINV
            CALL ZLACPY( 'F', N, N, Q, LDU, EVECTL, LDU )
            CALL ZTGEVC( 'L', 'B', LLWORK, N, S1, LDA, P1, LDA, EVECTL,
     $                   LDU, CDUMMA, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTGEVC(L,B)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZGET52( .TRUE., N, H, LDA, T, LDA, EVECTL, LDU, ALPHA1,
     $                   BETA1, WORK, RWORK, DUMMA( 1 ) )
            RESULT( 10 ) = DUMMA( 1 )
            IF( DUMMA( 2 ).GT.THRSHN ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Left', 'ZTGEVC(HOWMNY=B)',
     $            DUMMA( 2 ), N, JTYPE, IOLDSD
            END IF
*
*           11: Compute the right eigenvector Matrix without
*               back transforming:
*
            NTEST = 11
            RESULT( 11 ) = ULPINV
*
*           To test "SELECT" option, compute half of the eigenvectors
*           in one call, and half in another
*
            I1 = N / 2
            DO 160 J = 1, I1
               LLWORK( J ) = .TRUE.
  160       CONTINUE
            DO 170 J = I1 + 1, N
               LLWORK( J ) = .FALSE.
  170       CONTINUE
*
            CALL ZTGEVC( 'R', 'S', LLWORK, N, S1, LDA, P1, LDA, CDUMMA,
     $                   LDU, EVECTR, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTGEVC(R,S1)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            I1 = IN
            DO 180 J = 1, I1
               LLWORK( J ) = .FALSE.
  180       CONTINUE
            DO 190 J = I1 + 1, N
               LLWORK( J ) = .TRUE.
  190       CONTINUE
*
            CALL ZTGEVC( 'R', 'S', LLWORK, N, S1, LDA, P1, LDA, CDUMMA,
     $                   LDU, EVECTR( 1, I1+1 ), LDU, N, IN, WORK,
     $                   RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTGEVC(R,S2)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZGET52( .FALSE., N, S1, LDA, P1, LDA, EVECTR, LDU,
     $                   ALPHA1, BETA1, WORK, RWORK, DUMMA( 1 ) )
            RESULT( 11 ) = DUMMA( 1 )
            IF( DUMMA( 2 ).GT.THRESH ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Right', 'ZTGEVC(HOWMNY=S)',
     $            DUMMA( 2 ), N, JTYPE, IOLDSD
            END IF
*
*           12: Compute the right eigenvector Matrix with
*               back transforming:
*
            NTEST = 12
            RESULT( 12 ) = ULPINV
            CALL ZLACPY( 'F', N, N, Z, LDU, EVECTR, LDU )
            CALL ZTGEVC( 'R', 'B', LLWORK, N, S1, LDA, P1, LDA, CDUMMA,
     $                   LDU, EVECTR, LDU, N, IN, WORK, RWORK, IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUNIT, FMT = 9999 )'ZTGEVC(R,B)', IINFO, N,
     $            JTYPE, IOLDSD
               INFO = ABS( IINFO )
               GO TO 210
            END IF
*
            CALL ZGET52( .FALSE., N, H, LDA, T, LDA, EVECTR, LDU,
     $                   ALPHA1, BETA1, WORK, RWORK, DUMMA( 1 ) )
            RESULT( 12 ) = DUMMA( 1 )
            IF( DUMMA( 2 ).GT.THRESH ) THEN
               WRITE( NOUNIT, FMT = 9998 )'Right', 'ZTGEVC(HOWMNY=B)',
     $            DUMMA( 2 ), N, JTYPE, IOLDSD
            END IF
*
*           Tests 13--15 are done only on request
*
            IF( TSTDIF ) THEN
*
*              Do Tests 13--14
*
               CALL ZGET51( 2, N, S1, LDA, S2, LDA, Q, LDU, Z, LDU,
     $                      WORK, RWORK, RESULT( 13 ) )
               CALL ZGET51( 2, N, P1, LDA, P2, LDA, Q, LDU, Z, LDU,
     $                      WORK, RWORK, RESULT( 14 ) )
*
*              Do Test 15
*
               TEMP1 = ZERO
               TEMP2 = ZERO
               DO 200 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( ALPHA1( J )-ALPHA3( J ) ) )
                  TEMP2 = MAX( TEMP2, ABS( BETA1( J )-BETA3( J ) ) )
  200          CONTINUE
*
               TEMP1 = TEMP1 / MAX( SAFMIN, ULP*MAX( TEMP1, ANORM ) )
               TEMP2 = TEMP2 / MAX( SAFMIN, ULP*MAX( TEMP2, BNORM ) )
               RESULT( 15 ) = MAX( TEMP1, TEMP2 )
               NTEST = 15
            ELSE
               RESULT( 13 ) = ZERO
               RESULT( 14 ) = ZERO
               RESULT( 15 ) = ZERO
               NTEST = 12
            END IF
*
*           End of Loop -- Check for RESULT(j) > THRESH
*
  210       CONTINUE
*
            NTESTT = NTESTT + NTEST
*
*           Print out tests which fail.
*
            DO 220 JR = 1, NTEST
               IF( RESULT( JR ).GE.THRESH ) THEN
*
*                 If this is the first test to fail,
*                 print a header to the data file.
*
                  IF( NERRS.EQ.0 ) THEN
                     WRITE( NOUNIT, FMT = 9997 )'ZGG'
*
*                    Matrix types
*
                     WRITE( NOUNIT, FMT = 9996 )
                     WRITE( NOUNIT, FMT = 9995 )
                     WRITE( NOUNIT, FMT = 9994 )'Unitary'
*
*                    Tests performed
*
                     WRITE( NOUNIT, FMT = 9993 )'unitary', '*',
     $                  'conjugate transpose', ( '*', J = 1, 10 )
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
  220       CONTINUE
*
  230    CONTINUE
  240 CONTINUE
*
*     Summary
*
      CALL DLASUM( 'ZGG', NOUNIT, NERRS, NTESTT )
      RETURN
*
 9999 FORMAT( ' ZCHKGG: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
 9998 FORMAT( ' ZCHKGG: ', A, ' Eigenvectors from ', A, ' incorrectly ',
     $      'normalized.', / ' Bits of error=', 0P, G10.3, ',', 9X,
     $      'N=', I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5,
     $      ')' )
*
 9997 FORMAT( 1X, A3, ' -- Complex Generalized eigenvalue problem' )
*
 9996 FORMAT( ' Matrix types (see ZCHKGG for details): ' )
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
 9993 FORMAT( / ' Tests performed:   (H is Hessenberg, S is Schur, B, ',
     $      'T, P are triangular,', / 20X, 'U, V, Q, and Z are ', A,
     $      ', l and r are the', / 20X,
     $      'appropriate left and right eigenvectors, resp., a is',
     $      / 20X, 'alpha, b is beta, and ', A, ' means ', A, '.)',
     $      / ' 1 = | A - U H V', A,
     $      ' | / ( |A| n ulp )      2 = | B - U T V', A,
     $      ' | / ( |B| n ulp )', / ' 3 = | I - UU', A,
     $      ' | / ( n ulp )             4 = | I - VV', A,
     $      ' | / ( n ulp )', / ' 5 = | H - Q S Z', A,
     $      ' | / ( |H| n ulp )', 6X, '6 = | T - Q P Z', A,
     $      ' | / ( |T| n ulp )', / ' 7 = | I - QQ', A,
     $      ' | / ( n ulp )             8 = | I - ZZ', A,
     $      ' | / ( n ulp )', / ' 9 = max | ( b S - a P )', A,
     $      ' l | / const.  10 = max | ( b H - a T )', A,
     $      ' l | / const.', /
     $      ' 11= max | ( b S - a P ) r | / const.   12 = max | ( b H',
     $      ' - a T ) r | / const.', / 1X )
*
 9992 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I2, ' is', 0P, F8.2 )
 9991 FORMAT( ' Matrix order=', I5, ', type=', I2, ', seed=',
     $      4( I4, ',' ), ' result ', I2, ' is', 1P, D10.3 )
*
*     End of ZCHKGG
*
      END
