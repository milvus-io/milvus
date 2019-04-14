*> \brief \b SDRGSX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SDRGSX( NSIZE, NCMAX, THRESH, NIN, NOUT, A, LDA, B,
*                          AI, BI, Z, Q, ALPHAR, ALPHAI, BETA, C, LDC, S,
*                          WORK, LWORK, IWORK, LIWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDC, LIWORK, LWORK, NCMAX, NIN,
*      $                   NOUT, NSIZE
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       INTEGER            IWORK( * )
*       REAL               A( LDA, * ), AI( LDA, * ), ALPHAI( * ),
*      $                   ALPHAR( * ), B( LDA, * ), BETA( * ),
*      $                   BI( LDA, * ), C( LDC, * ), Q( LDA, * ), S( * ),
*      $                   WORK( * ), Z( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SDRGSX checks the nonsymmetric generalized eigenvalue (Schur form)
*> problem expert driver SGGESX.
*>
*> SGGESX factors A and B as Q S Z' and Q T Z', where ' means
*> transpose, T is upper triangular, S is in generalized Schur form
*> (block upper triangular, with 1x1 and 2x2 blocks on the diagonal,
*> the 2x2 blocks corresponding to complex conjugate pairs of
*> generalized eigenvalues), and Q and Z are orthogonal.  It also
*> computes the generalized eigenvalues (alpha(1),beta(1)), ...,
*> (alpha(n),beta(n)). Thus, w(j) = alpha(j)/beta(j) is a root of the
*> characteristic equation
*>
*>     det( A - w(j) B ) = 0
*>
*> Optionally it also reorders the eigenvalues so that a selected
*> cluster of eigenvalues appears in the leading diagonal block of the
*> Schur forms; computes a reciprocal condition number for the average
*> of the selected eigenvalues; and computes a reciprocal condition
*> number for the right and left deflating subspaces corresponding to
*> the selected eigenvalues.
*>
*> When SDRGSX is called with NSIZE > 0, five (5) types of built-in
*> matrix pairs are used to test the routine SGGESX.
*>
*> When SDRGSX is called with NSIZE = 0, it reads in test matrix data
*> to test SGGESX.
*>
*> For each matrix pair, the following tests will be performed and
*> compared with the threshold THRESH except for the tests (7) and (9):
*>
*> (1)   | A - Q S Z' | / ( |A| n ulp )
*>
*> (2)   | B - Q T Z' | / ( |B| n ulp )
*>
*> (3)   | I - QQ' | / ( n ulp )
*>
*> (4)   | I - ZZ' | / ( n ulp )
*>
*> (5)   if A is in Schur form (i.e. quasi-triangular form)
*>
*> (6)   maximum over j of D(j)  where:
*>
*>       if alpha(j) is real:
*>                     |alpha(j) - S(j,j)|        |beta(j) - T(j,j)|
*>           D(j) = ------------------------ + -----------------------
*>                  max(|alpha(j)|,|S(j,j)|)   max(|beta(j)|,|T(j,j)|)
*>
*>       if alpha(j) is complex:
*>                                 | det( s S - w T ) |
*>           D(j) = ---------------------------------------------------
*>                  ulp max( s norm(S), |w| norm(T) )*norm( s S - w T )
*>
*>           and S and T are here the 2 x 2 diagonal blocks of S and T
*>           corresponding to the j-th and j+1-th eigenvalues.
*>
*> (7)   if sorting worked and SDIM is the number of eigenvalues
*>       which were selected.
*>
*> (8)   the estimated value DIF does not differ from the true values of
*>       Difu and Difl more than a factor 10*THRESH. If the estimate DIF
*>       equals zero the corresponding true values of Difu and Difl
*>       should be less than EPS*norm(A, B). If the true value of Difu
*>       and Difl equal zero, the estimate DIF should be less than
*>       EPS*norm(A, B).
*>
*> (9)   If INFO = N+3 is returned by SGGESX, the reordering "failed"
*>       and we check that DIF = PL = PR = 0 and that the true value of
*>       Difu and Difl is < EPS*norm(A, B). We count the events when
*>       INFO=N+3.
*>
*> For read-in test matrices, the above tests are run except that the
*> exact value for DIF (and PL) is input data.  Additionally, there is
*> one more test run for read-in test matrices:
*>
*> (10)  the estimated value PL does not differ from the true value of
*>       PLTRU more than a factor THRESH. If the estimate PL equals
*>       zero the corresponding true value of PLTRU should be less than
*>       EPS*norm(A, B). If the true value of PLTRU equal zero, the
*>       estimate PL should be less than EPS*norm(A, B).
*>
*> Note that for the built-in tests, a total of 10*NSIZE*(NSIZE-1)
*> matrix pairs are generated and tested. NSIZE should be kept small.
*>
*> SVD (routine SGESVD) is used for computing the true value of DIF_u
*> and DIF_l when testing the built-in test problems.
*>
*> Built-in Test Matrices
*> ======================
*>
*> All built-in test matrices are the 2 by 2 block of triangular
*> matrices
*>
*>          A = [ A11 A12 ]    and      B = [ B11 B12 ]
*>              [     A22 ]                 [     B22 ]
*>
*> where for different type of A11 and A22 are given as the following.
*> A12 and B12 are chosen so that the generalized Sylvester equation
*>
*>          A11*R - L*A22 = -A12
*>          B11*R - L*B22 = -B12
*>
*> have prescribed solution R and L.
*>
*> Type 1:  A11 = J_m(1,-1) and A_22 = J_k(1-a,1).
*>          B11 = I_m, B22 = I_k
*>          where J_k(a,b) is the k-by-k Jordan block with ``a'' on
*>          diagonal and ``b'' on superdiagonal.
*>
*> Type 2:  A11 = (a_ij) = ( 2(.5-sin(i)) ) and
*>          B11 = (b_ij) = ( 2(.5-sin(ij)) ) for i=1,...,m, j=i,...,m
*>          A22 = (a_ij) = ( 2(.5-sin(i+j)) ) and
*>          B22 = (b_ij) = ( 2(.5-sin(ij)) ) for i=m+1,...,k, j=i,...,k
*>
*> Type 3:  A11, A22 and B11, B22 are chosen as for Type 2, but each
*>          second diagonal block in A_11 and each third diagonal block
*>          in A_22 are made as 2 by 2 blocks.
*>
*> Type 4:  A11 = ( 20(.5 - sin(ij)) ) and B22 = ( 2(.5 - sin(i+j)) )
*>             for i=1,...,m,  j=1,...,m and
*>          A22 = ( 20(.5 - sin(i+j)) ) and B22 = ( 2(.5 - sin(ij)) )
*>             for i=m+1,...,k,  j=m+1,...,k
*>
*> Type 5:  (A,B) and have potentially close or common eigenvalues and
*>          very large departure from block diagonality A_11 is chosen
*>          as the m x m leading submatrix of A_1:
*>                  |  1  b                            |
*>                  | -b  1                            |
*>                  |        1+d  b                    |
*>                  |         -b 1+d                   |
*>           A_1 =  |                  d  1            |
*>                  |                 -1  d            |
*>                  |                        -d  1     |
*>                  |                        -1 -d     |
*>                  |                               1  |
*>          and A_22 is chosen as the k x k leading submatrix of A_2:
*>                  | -1  b                            |
*>                  | -b -1                            |
*>                  |       1-d  b                     |
*>                  |       -b  1-d                    |
*>           A_2 =  |                 d 1+b            |
*>                  |               -1-b d             |
*>                  |                       -d  1+b    |
*>                  |                      -1+b  -d    |
*>                  |                              1-d |
*>          and matrix B are chosen as identity matrices (see SLATM5).
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZE
*> \verbatim
*>          NSIZE is INTEGER
*>          The maximum size of the matrices to use. NSIZE >= 0.
*>          If NSIZE = 0, no built-in tests matrices are used, but
*>          read-in test matrices are used to test SGGESX.
*> \endverbatim
*>
*> \param[in] NCMAX
*> \verbatim
*>          NCMAX is INTEGER
*>          Maximum allowable NMAX for generating Kroneker matrix
*>          in call to SLAKF2
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
*>          or the size of the matrix.  THRESH >= 0.
*> \endverbatim
*>
*> \param[in] NIN
*> \verbatim
*>          NIN is INTEGER
*>          The FORTRAN unit number for reading in the data file of
*>          problems to solve.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The FORTRAN unit number for printing out error messages
*>          (e.g., if a routine returns IINFO not equal to 0.)
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is REAL array, dimension (LDA, NSIZE)
*>          Used to store the matrix whose eigenvalues are to be
*>          computed.  On exit, A contains the last matrix actually used.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, B, AI, BI, Z and Q,
*>          LDA >= max( 1, NSIZE ). For the read-in test,
*>          LDA >= max( 1, N ), N is the size of the test matrices.
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is REAL array, dimension (LDA, NSIZE)
*>          Used to store the matrix whose eigenvalues are to be
*>          computed.  On exit, B contains the last matrix actually used.
*> \endverbatim
*>
*> \param[out] AI
*> \verbatim
*>          AI is REAL array, dimension (LDA, NSIZE)
*>          Copy of A, modified by SGGESX.
*> \endverbatim
*>
*> \param[out] BI
*> \verbatim
*>          BI is REAL array, dimension (LDA, NSIZE)
*>          Copy of B, modified by SGGESX.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is REAL array, dimension (LDA, NSIZE)
*>          Z holds the left Schur vectors computed by SGGESX.
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is REAL array, dimension (LDA, NSIZE)
*>          Q holds the right Schur vectors computed by SGGESX.
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is REAL array, dimension (NSIZE)
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is REAL array, dimension (NSIZE)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (NSIZE)
*> \verbatim
*>          On exit, (ALPHAR + ALPHAI*i)/BETA are the eigenvalues.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is REAL array, dimension (LDC, LDC)
*>          Store the matrix generated by subroutine SLAKF2, this is the
*>          matrix formed by Kronecker products used for estimating
*>          DIF.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of C. LDC >= max(1, LDA*LDA/2 ).
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (LDC)
*>          Singular values of C
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          LWORK >= MAX( 5*NSIZE*NSIZE/2 - 2, 10*(NSIZE+1) )
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (LIWORK)
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK. LIWORK >= NSIZE + 6.
*> \endverbatim
*>
*> \param[out] BWORK
*> \verbatim
*>          BWORK is LOGICAL array, dimension (LDA)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  A routine returned an error code.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SDRGSX( NSIZE, NCMAX, THRESH, NIN, NOUT, A, LDA, B,
     $                   AI, BI, Z, Q, ALPHAR, ALPHAI, BETA, C, LDC, S,
     $                   WORK, LWORK, IWORK, LIWORK, BWORK, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDC, LIWORK, LWORK, NCMAX, NIN,
     $                   NOUT, NSIZE
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      INTEGER            IWORK( * )
      REAL               A( LDA, * ), AI( LDA, * ), ALPHAI( * ),
     $                   ALPHAR( * ), B( LDA, * ), BETA( * ),
     $                   BI( LDA, * ), C( LDC, * ), Q( LDA, * ), S( * ),
     $                   WORK( * ), Z( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TEN = 1.0E+1 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ILABAD
      CHARACTER          SENSE
      INTEGER            BDSPAC, I, I1, IFUNC, IINFO, J, LINFO, MAXWRK,
     $                   MINWRK, MM, MN2, NERRS, NPTKNT, NTEST, NTESTT,
     $                   PRTYPE, QBA, QBB
      REAL               ABNRM, BIGNUM, DIFTRU, PLTRU, SMLNUM, TEMP1,
     $                   TEMP2, THRSH2, ULP, ULPINV, WEIGHT
*     ..
*     .. Local Arrays ..
      REAL               DIFEST( 2 ), PL( 2 ), RESULT( 10 )
*     ..
*     .. External Functions ..
      LOGICAL            SLCTSX
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           SLCTSX, ILAENV, SLAMCH, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALASVM, SGESVD, SGET51, SGET53, SGGESX, SLABAD,
     $                   SLACPY, SLAKF2, SLASET, SLATM5, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Scalars in Common ..
      LOGICAL            FS
      INTEGER            K, M, MPLUSN, N
*     ..
*     .. Common blocks ..
      COMMON             / MN / M, N, MPLUSN, K, FS
*     ..
*     .. Executable Statements ..
*
*     Check for errors
*
      IF( NSIZE.LT.0 ) THEN
         INFO = -1
      ELSE IF( THRESH.LT.ZERO ) THEN
         INFO = -2
      ELSE IF( NIN.LE.0 ) THEN
         INFO = -3
      ELSE IF( NOUT.LE.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.1 .OR. LDA.LT.NSIZE ) THEN
         INFO = -6
      ELSE IF( LDC.LT.1 .OR. LDC.LT.NSIZE*NSIZE / 2 ) THEN
         INFO = -17
      ELSE IF( LIWORK.LT.NSIZE+6 ) THEN
         INFO = -21
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       NB refers to the optimal block size for the immediately
*       following subroutine, as returned by ILAENV.)
*
      MINWRK = 1
      IF( INFO.EQ.0 .AND. LWORK.GE.1 ) THEN
c        MINWRK = MAX( 10*( NSIZE+1 ), 5*NSIZE*NSIZE / 2-2 )
         MINWRK = MAX( 10*( NSIZE+1 ), 5*NSIZE*NSIZE / 2 )
*
*        workspace for sggesx
*
         MAXWRK = 9*( NSIZE+1 ) + NSIZE*
     $            ILAENV( 1, 'SGEQRF', ' ', NSIZE, 1, NSIZE, 0 )
         MAXWRK = MAX( MAXWRK, 9*( NSIZE+1 )+NSIZE*
     $            ILAENV( 1, 'SORGQR', ' ', NSIZE, 1, NSIZE, -1 ) )
*
*        workspace for sgesvd
*
         BDSPAC = 5*NSIZE*NSIZE / 2
         MAXWRK = MAX( MAXWRK, 3*NSIZE*NSIZE / 2+NSIZE*NSIZE*
     $            ILAENV( 1, 'SGEBRD', ' ', NSIZE*NSIZE / 2,
     $            NSIZE*NSIZE / 2, -1, -1 ) )
         MAXWRK = MAX( MAXWRK, BDSPAC )
*
         MAXWRK = MAX( MAXWRK, MINWRK )
*
         WORK( 1 ) = MAXWRK
      END IF
*
      IF( LWORK.LT.MINWRK )
     $   INFO = -19
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SDRGSX', -INFO )
         RETURN
      END IF
*
*     Important constants
*
      ULP = SLAMCH( 'P' )
      ULPINV = ONE / ULP
      SMLNUM = SLAMCH( 'S' ) / ULP
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
      THRSH2 = TEN*THRESH
      NTESTT = 0
      NERRS = 0
*
*     Go to the tests for read-in matrix pairs
*
      IFUNC = 0
      IF( NSIZE.EQ.0 )
     $   GO TO 70
*
*     Test the built-in matrix pairs.
*     Loop over different functions (IFUNC) of SGGESX, types (PRTYPE)
*     of test matrices, different size (M+N)
*
      PRTYPE = 0
      QBA = 3
      QBB = 4
      WEIGHT = SQRT( ULP )
*
      DO 60 IFUNC = 0, 3
         DO 50 PRTYPE = 1, 5
            DO 40 M = 1, NSIZE - 1
               DO 30 N = 1, NSIZE - M
*
                  WEIGHT = ONE / WEIGHT
                  MPLUSN = M + N
*
*                 Generate test matrices
*
                  FS = .TRUE.
                  K = 0
*
                  CALL SLASET( 'Full', MPLUSN, MPLUSN, ZERO, ZERO, AI,
     $                         LDA )
                  CALL SLASET( 'Full', MPLUSN, MPLUSN, ZERO, ZERO, BI,
     $                         LDA )
*
                  CALL SLATM5( PRTYPE, M, N, AI, LDA, AI( M+1, M+1 ),
     $                         LDA, AI( 1, M+1 ), LDA, BI, LDA,
     $                         BI( M+1, M+1 ), LDA, BI( 1, M+1 ), LDA,
     $                         Q, LDA, Z, LDA, WEIGHT, QBA, QBB )
*
*                 Compute the Schur factorization and swapping the
*                 m-by-m (1,1)-blocks with n-by-n (2,2)-blocks.
*                 Swapping is accomplished via the function SLCTSX
*                 which is supplied below.
*
                  IF( IFUNC.EQ.0 ) THEN
                     SENSE = 'N'
                  ELSE IF( IFUNC.EQ.1 ) THEN
                     SENSE = 'E'
                  ELSE IF( IFUNC.EQ.2 ) THEN
                     SENSE = 'V'
                  ELSE IF( IFUNC.EQ.3 ) THEN
                     SENSE = 'B'
                  END IF
*
                  CALL SLACPY( 'Full', MPLUSN, MPLUSN, AI, LDA, A, LDA )
                  CALL SLACPY( 'Full', MPLUSN, MPLUSN, BI, LDA, B, LDA )
*
                  CALL SGGESX( 'V', 'V', 'S', SLCTSX, SENSE, MPLUSN, AI,
     $                         LDA, BI, LDA, MM, ALPHAR, ALPHAI, BETA,
     $                         Q, LDA, Z, LDA, PL, DIFEST, WORK, LWORK,
     $                         IWORK, LIWORK, BWORK, LINFO )
*
                  IF( LINFO.NE.0 .AND. LINFO.NE.MPLUSN+2 ) THEN
                     RESULT( 1 ) = ULPINV
                     WRITE( NOUT, FMT = 9999 )'SGGESX', LINFO, MPLUSN,
     $                  PRTYPE
                     INFO = LINFO
                     GO TO 30
                  END IF
*
*                 Compute the norm(A, B)
*
                  CALL SLACPY( 'Full', MPLUSN, MPLUSN, AI, LDA, WORK,
     $                         MPLUSN )
                  CALL SLACPY( 'Full', MPLUSN, MPLUSN, BI, LDA,
     $                         WORK( MPLUSN*MPLUSN+1 ), MPLUSN )
                  ABNRM = SLANGE( 'Fro', MPLUSN, 2*MPLUSN, WORK, MPLUSN,
     $                    WORK )
*
*                 Do tests (1) to (4)
*
                  CALL SGET51( 1, MPLUSN, A, LDA, AI, LDA, Q, LDA, Z,
     $                         LDA, WORK, RESULT( 1 ) )
                  CALL SGET51( 1, MPLUSN, B, LDA, BI, LDA, Q, LDA, Z,
     $                         LDA, WORK, RESULT( 2 ) )
                  CALL SGET51( 3, MPLUSN, B, LDA, BI, LDA, Q, LDA, Q,
     $                         LDA, WORK, RESULT( 3 ) )
                  CALL SGET51( 3, MPLUSN, B, LDA, BI, LDA, Z, LDA, Z,
     $                         LDA, WORK, RESULT( 4 ) )
                  NTEST = 4
*
*                 Do tests (5) and (6): check Schur form of A and
*                 compare eigenvalues with diagonals.
*
                  TEMP1 = ZERO
                  RESULT( 5 ) = ZERO
                  RESULT( 6 ) = ZERO
*
                  DO 10 J = 1, MPLUSN
                     ILABAD = .FALSE.
                     IF( ALPHAI( J ).EQ.ZERO ) THEN
                        TEMP2 = ( ABS( ALPHAR( J )-AI( J, J ) ) /
     $                          MAX( SMLNUM, ABS( ALPHAR( J ) ),
     $                          ABS( AI( J, J ) ) )+
     $                          ABS( BETA( J )-BI( J, J ) ) /
     $                          MAX( SMLNUM, ABS( BETA( J ) ),
     $                          ABS( BI( J, J ) ) ) ) / ULP
                        IF( J.LT.MPLUSN ) THEN
                           IF( AI( J+1, J ).NE.ZERO ) THEN
                              ILABAD = .TRUE.
                              RESULT( 5 ) = ULPINV
                           END IF
                        END IF
                        IF( J.GT.1 ) THEN
                           IF( AI( J, J-1 ).NE.ZERO ) THEN
                              ILABAD = .TRUE.
                              RESULT( 5 ) = ULPINV
                           END IF
                        END IF
                     ELSE
                        IF( ALPHAI( J ).GT.ZERO ) THEN
                           I1 = J
                        ELSE
                           I1 = J - 1
                        END IF
                        IF( I1.LE.0 .OR. I1.GE.MPLUSN ) THEN
                           ILABAD = .TRUE.
                        ELSE IF( I1.LT.MPLUSN-1 ) THEN
                           IF( AI( I1+2, I1+1 ).NE.ZERO ) THEN
                              ILABAD = .TRUE.
                              RESULT( 5 ) = ULPINV
                           END IF
                        ELSE IF( I1.GT.1 ) THEN
                           IF( AI( I1, I1-1 ).NE.ZERO ) THEN
                              ILABAD = .TRUE.
                              RESULT( 5 ) = ULPINV
                           END IF
                        END IF
                        IF( .NOT.ILABAD ) THEN
                           CALL SGET53( AI( I1, I1 ), LDA, BI( I1, I1 ),
     $                                  LDA, BETA( J ), ALPHAR( J ),
     $                                  ALPHAI( J ), TEMP2, IINFO )
                           IF( IINFO.GE.3 ) THEN
                              WRITE( NOUT, FMT = 9997 )IINFO, J,
     $                           MPLUSN, PRTYPE
                              INFO = ABS( IINFO )
                           END IF
                        ELSE
                           TEMP2 = ULPINV
                        END IF
                     END IF
                     TEMP1 = MAX( TEMP1, TEMP2 )
                     IF( ILABAD ) THEN
                        WRITE( NOUT, FMT = 9996 )J, MPLUSN, PRTYPE
                     END IF
   10             CONTINUE
                  RESULT( 6 ) = TEMP1
                  NTEST = NTEST + 2
*
*                 Test (7) (if sorting worked)
*
                  RESULT( 7 ) = ZERO
                  IF( LINFO.EQ.MPLUSN+3 ) THEN
                     RESULT( 7 ) = ULPINV
                  ELSE IF( MM.NE.N ) THEN
                     RESULT( 7 ) = ULPINV
                  END IF
                  NTEST = NTEST + 1
*
*                 Test (8): compare the estimated value DIF and its
*                 value. first, compute the exact DIF.
*
                  RESULT( 8 ) = ZERO
                  MN2 = MM*( MPLUSN-MM )*2
                  IF( IFUNC.GE.2 .AND. MN2.LE.NCMAX*NCMAX ) THEN
*
*                    Note: for either following two causes, there are
*                    almost same number of test cases fail the test.
*
                     CALL SLAKF2( MM, MPLUSN-MM, AI, LDA,
     $                            AI( MM+1, MM+1 ), BI,
     $                            BI( MM+1, MM+1 ), C, LDC )
*
                     CALL SGESVD( 'N', 'N', MN2, MN2, C, LDC, S, WORK,
     $                            1, WORK( 2 ), 1, WORK( 3 ), LWORK-2,
     $                            INFO )
                     DIFTRU = S( MN2 )
*
                     IF( DIFEST( 2 ).EQ.ZERO ) THEN
                        IF( DIFTRU.GT.ABNRM*ULP )
     $                     RESULT( 8 ) = ULPINV
                     ELSE IF( DIFTRU.EQ.ZERO ) THEN
                        IF( DIFEST( 2 ).GT.ABNRM*ULP )
     $                     RESULT( 8 ) = ULPINV
                     ELSE IF( ( DIFTRU.GT.THRSH2*DIFEST( 2 ) ) .OR.
     $                        ( DIFTRU*THRSH2.LT.DIFEST( 2 ) ) ) THEN
                        RESULT( 8 ) = MAX( DIFTRU / DIFEST( 2 ),
     $                                DIFEST( 2 ) / DIFTRU )
                     END IF
                     NTEST = NTEST + 1
                  END IF
*
*                 Test (9)
*
                  RESULT( 9 ) = ZERO
                  IF( LINFO.EQ.( MPLUSN+2 ) ) THEN
                     IF( DIFTRU.GT.ABNRM*ULP )
     $                  RESULT( 9 ) = ULPINV
                     IF( ( IFUNC.GT.1 ) .AND. ( DIFEST( 2 ).NE.ZERO ) )
     $                  RESULT( 9 ) = ULPINV
                     IF( ( IFUNC.EQ.1 ) .AND. ( PL( 1 ).NE.ZERO ) )
     $                  RESULT( 9 ) = ULPINV
                     NTEST = NTEST + 1
                  END IF
*
                  NTESTT = NTESTT + NTEST
*
*                 Print out tests which fail.
*
                  DO 20 J = 1, 9
                     IF( RESULT( J ).GE.THRESH ) THEN
*
*                       If this is the first test to fail,
*                       print a header to the data file.
*
                        IF( NERRS.EQ.0 ) THEN
                           WRITE( NOUT, FMT = 9995 )'SGX'
*
*                          Matrix types
*
                           WRITE( NOUT, FMT = 9993 )
*
*                          Tests performed
*
                           WRITE( NOUT, FMT = 9992 )'orthogonal', '''',
     $                        'transpose', ( '''', I = 1, 4 )
*
                        END IF
                        NERRS = NERRS + 1
                        IF( RESULT( J ).LT.10000.0 ) THEN
                           WRITE( NOUT, FMT = 9991 )MPLUSN, PRTYPE,
     $                        WEIGHT, M, J, RESULT( J )
                        ELSE
                           WRITE( NOUT, FMT = 9990 )MPLUSN, PRTYPE,
     $                        WEIGHT, M, J, RESULT( J )
                        END IF
                     END IF
   20             CONTINUE
*
   30          CONTINUE
   40       CONTINUE
   50    CONTINUE
   60 CONTINUE
*
      GO TO 150
*
   70 CONTINUE
*
*     Read in data from file to check accuracy of condition estimation
*     Read input data until N=0
*
      NPTKNT = 0
*
   80 CONTINUE
      READ( NIN, FMT = *, END = 140 )MPLUSN
      IF( MPLUSN.EQ.0 )
     $   GO TO 140
      READ( NIN, FMT = *, END = 140 )N
      DO 90 I = 1, MPLUSN
         READ( NIN, FMT = * )( AI( I, J ), J = 1, MPLUSN )
   90 CONTINUE
      DO 100 I = 1, MPLUSN
         READ( NIN, FMT = * )( BI( I, J ), J = 1, MPLUSN )
  100 CONTINUE
      READ( NIN, FMT = * )PLTRU, DIFTRU
*
      NPTKNT = NPTKNT + 1
      FS = .TRUE.
      K = 0
      M = MPLUSN - N
*
      CALL SLACPY( 'Full', MPLUSN, MPLUSN, AI, LDA, A, LDA )
      CALL SLACPY( 'Full', MPLUSN, MPLUSN, BI, LDA, B, LDA )
*
*     Compute the Schur factorization while swaping the
*     m-by-m (1,1)-blocks with n-by-n (2,2)-blocks.
*
      CALL SGGESX( 'V', 'V', 'S', SLCTSX, 'B', MPLUSN, AI, LDA, BI, LDA,
     $             MM, ALPHAR, ALPHAI, BETA, Q, LDA, Z, LDA, PL, DIFEST,
     $             WORK, LWORK, IWORK, LIWORK, BWORK, LINFO )
*
      IF( LINFO.NE.0 .AND. LINFO.NE.MPLUSN+2 ) THEN
         RESULT( 1 ) = ULPINV
         WRITE( NOUT, FMT = 9998 )'SGGESX', LINFO, MPLUSN, NPTKNT
         GO TO 130
      END IF
*
*     Compute the norm(A, B)
*        (should this be norm of (A,B) or (AI,BI)?)
*
      CALL SLACPY( 'Full', MPLUSN, MPLUSN, AI, LDA, WORK, MPLUSN )
      CALL SLACPY( 'Full', MPLUSN, MPLUSN, BI, LDA,
     $             WORK( MPLUSN*MPLUSN+1 ), MPLUSN )
      ABNRM = SLANGE( 'Fro', MPLUSN, 2*MPLUSN, WORK, MPLUSN, WORK )
*
*     Do tests (1) to (4)
*
      CALL SGET51( 1, MPLUSN, A, LDA, AI, LDA, Q, LDA, Z, LDA, WORK,
     $             RESULT( 1 ) )
      CALL SGET51( 1, MPLUSN, B, LDA, BI, LDA, Q, LDA, Z, LDA, WORK,
     $             RESULT( 2 ) )
      CALL SGET51( 3, MPLUSN, B, LDA, BI, LDA, Q, LDA, Q, LDA, WORK,
     $             RESULT( 3 ) )
      CALL SGET51( 3, MPLUSN, B, LDA, BI, LDA, Z, LDA, Z, LDA, WORK,
     $             RESULT( 4 ) )
*
*     Do tests (5) and (6): check Schur form of A and compare
*     eigenvalues with diagonals.
*
      NTEST = 6
      TEMP1 = ZERO
      RESULT( 5 ) = ZERO
      RESULT( 6 ) = ZERO
*
      DO 110 J = 1, MPLUSN
         ILABAD = .FALSE.
         IF( ALPHAI( J ).EQ.ZERO ) THEN
            TEMP2 = ( ABS( ALPHAR( J )-AI( J, J ) ) /
     $              MAX( SMLNUM, ABS( ALPHAR( J ) ), ABS( AI( J,
     $              J ) ) )+ABS( BETA( J )-BI( J, J ) ) /
     $              MAX( SMLNUM, ABS( BETA( J ) ), ABS( BI( J, J ) ) ) )
     $               / ULP
            IF( J.LT.MPLUSN ) THEN
               IF( AI( J+1, J ).NE.ZERO ) THEN
                  ILABAD = .TRUE.
                  RESULT( 5 ) = ULPINV
               END IF
            END IF
            IF( J.GT.1 ) THEN
               IF( AI( J, J-1 ).NE.ZERO ) THEN
                  ILABAD = .TRUE.
                  RESULT( 5 ) = ULPINV
               END IF
            END IF
         ELSE
            IF( ALPHAI( J ).GT.ZERO ) THEN
               I1 = J
            ELSE
               I1 = J - 1
            END IF
            IF( I1.LE.0 .OR. I1.GE.MPLUSN ) THEN
               ILABAD = .TRUE.
            ELSE IF( I1.LT.MPLUSN-1 ) THEN
               IF( AI( I1+2, I1+1 ).NE.ZERO ) THEN
                  ILABAD = .TRUE.
                  RESULT( 5 ) = ULPINV
               END IF
            ELSE IF( I1.GT.1 ) THEN
               IF( AI( I1, I1-1 ).NE.ZERO ) THEN
                  ILABAD = .TRUE.
                  RESULT( 5 ) = ULPINV
               END IF
            END IF
            IF( .NOT.ILABAD ) THEN
               CALL SGET53( AI( I1, I1 ), LDA, BI( I1, I1 ), LDA,
     $                      BETA( J ), ALPHAR( J ), ALPHAI( J ), TEMP2,
     $                      IINFO )
               IF( IINFO.GE.3 ) THEN
                  WRITE( NOUT, FMT = 9997 )IINFO, J, MPLUSN, NPTKNT
                  INFO = ABS( IINFO )
               END IF
            ELSE
               TEMP2 = ULPINV
            END IF
         END IF
         TEMP1 = MAX( TEMP1, TEMP2 )
         IF( ILABAD ) THEN
            WRITE( NOUT, FMT = 9996 )J, MPLUSN, NPTKNT
         END IF
  110 CONTINUE
      RESULT( 6 ) = TEMP1
*
*     Test (7) (if sorting worked)  <--------- need to be checked.
*
      NTEST = 7
      RESULT( 7 ) = ZERO
      IF( LINFO.EQ.MPLUSN+3 )
     $   RESULT( 7 ) = ULPINV
*
*     Test (8): compare the estimated value of DIF and its true value.
*
      NTEST = 8
      RESULT( 8 ) = ZERO
      IF( DIFEST( 2 ).EQ.ZERO ) THEN
         IF( DIFTRU.GT.ABNRM*ULP )
     $      RESULT( 8 ) = ULPINV
      ELSE IF( DIFTRU.EQ.ZERO ) THEN
         IF( DIFEST( 2 ).GT.ABNRM*ULP )
     $      RESULT( 8 ) = ULPINV
      ELSE IF( ( DIFTRU.GT.THRSH2*DIFEST( 2 ) ) .OR.
     $         ( DIFTRU*THRSH2.LT.DIFEST( 2 ) ) ) THEN
         RESULT( 8 ) = MAX( DIFTRU / DIFEST( 2 ), DIFEST( 2 ) / DIFTRU )
      END IF
*
*     Test (9)
*
      NTEST = 9
      RESULT( 9 ) = ZERO
      IF( LINFO.EQ.( MPLUSN+2 ) ) THEN
         IF( DIFTRU.GT.ABNRM*ULP )
     $      RESULT( 9 ) = ULPINV
         IF( ( IFUNC.GT.1 ) .AND. ( DIFEST( 2 ).NE.ZERO ) )
     $      RESULT( 9 ) = ULPINV
         IF( ( IFUNC.EQ.1 ) .AND. ( PL( 1 ).NE.ZERO ) )
     $      RESULT( 9 ) = ULPINV
      END IF
*
*     Test (10): compare the estimated value of PL and it true value.
*
      NTEST = 10
      RESULT( 10 ) = ZERO
      IF( PL( 1 ).EQ.ZERO ) THEN
         IF( PLTRU.GT.ABNRM*ULP )
     $      RESULT( 10 ) = ULPINV
      ELSE IF( PLTRU.EQ.ZERO ) THEN
         IF( PL( 1 ).GT.ABNRM*ULP )
     $      RESULT( 10 ) = ULPINV
      ELSE IF( ( PLTRU.GT.THRESH*PL( 1 ) ) .OR.
     $         ( PLTRU*THRESH.LT.PL( 1 ) ) ) THEN
         RESULT( 10 ) = ULPINV
      END IF
*
      NTESTT = NTESTT + NTEST
*
*     Print out tests which fail.
*
      DO 120 J = 1, NTEST
         IF( RESULT( J ).GE.THRESH ) THEN
*
*           If this is the first test to fail,
*           print a header to the data file.
*
            IF( NERRS.EQ.0 ) THEN
               WRITE( NOUT, FMT = 9995 )'SGX'
*
*              Matrix types
*
               WRITE( NOUT, FMT = 9994 )
*
*              Tests performed
*
               WRITE( NOUT, FMT = 9992 )'orthogonal', '''',
     $            'transpose', ( '''', I = 1, 4 )
*
            END IF
            NERRS = NERRS + 1
            IF( RESULT( J ).LT.10000.0 ) THEN
               WRITE( NOUT, FMT = 9989 )NPTKNT, MPLUSN, J, RESULT( J )
            ELSE
               WRITE( NOUT, FMT = 9988 )NPTKNT, MPLUSN, J, RESULT( J )
            END IF
         END IF
*
  120 CONTINUE
*
  130 CONTINUE
      GO TO 80
  140 CONTINUE
*
  150 CONTINUE
*
*     Summary
*
      CALL ALASVM( 'SGX', NOUT, NERRS, NTESTT, 0 )
*
      WORK( 1 ) = MAXWRK
*
      RETURN
*
 9999 FORMAT( ' SDRGSX: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ')' )
*
 9998 FORMAT( ' SDRGSX: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', Input Example #', I2, ')' )
*
 9997 FORMAT( ' SDRGSX: SGET53 returned INFO=', I1, ' for eigenvalue ',
     $      I6, '.', / 9X, 'N=', I6, ', JTYPE=', I6, ')' )
*
 9996 FORMAT( ' SDRGSX: S not in Schur form at eigenvalue ', I6, '.',
     $      / 9X, 'N=', I6, ', JTYPE=', I6, ')' )
*
 9995 FORMAT( / 1X, A3, ' -- Real Expert Generalized Schur form',
     $      ' problem driver' )
*
 9994 FORMAT( 'Input Example' )
*
 9993 FORMAT( ' Matrix types: ', /
     $      '  1:  A is a block diagonal matrix of Jordan blocks ',
     $      'and B is the identity ', / '      matrix, ',
     $      / '  2:  A and B are upper triangular matrices, ',
     $      / '  3:  A and B are as type 2, but each second diagonal ',
     $      'block in A_11 and ', /
     $      '      each third diaongal block in A_22 are 2x2 blocks,',
     $      / '  4:  A and B are block diagonal matrices, ',
     $      / '  5:  (A,B) has potentially close or common ',
     $      'eigenvalues.', / )
*
 9992 FORMAT( / ' Tests performed:  (S is Schur, T is triangular, ',
     $      'Q and Z are ', A, ',', / 19X,
     $      ' a is alpha, b is beta, and ', A, ' means ', A, '.)',
     $      / '  1 = | A - Q S Z', A,
     $      ' | / ( |A| n ulp )      2 = | B - Q T Z', A,
     $      ' | / ( |B| n ulp )', / '  3 = | I - QQ', A,
     $      ' | / ( n ulp )             4 = | I - ZZ', A,
     $      ' | / ( n ulp )', / '  5 = 1/ULP  if A is not in ',
     $      'Schur form S', / '  6 = difference between (alpha,beta)',
     $      ' and diagonals of (S,T)', /
     $      '  7 = 1/ULP  if SDIM is not the correct number of ',
     $      'selected eigenvalues', /
     $      '  8 = 1/ULP  if DIFEST/DIFTRU > 10*THRESH or ',
     $      'DIFTRU/DIFEST > 10*THRESH',
     $      / '  9 = 1/ULP  if DIFEST <> 0 or DIFTRU > ULP*norm(A,B) ',
     $      'when reordering fails', /
     $      ' 10 = 1/ULP  if PLEST/PLTRU > THRESH or ',
     $      'PLTRU/PLEST > THRESH', /
     $      '    ( Test 10 is only for input examples )', / )
 9991 FORMAT( ' Matrix order=', I2, ', type=', I2, ', a=', E10.3,
     $      ', order(A_11)=', I2, ', result ', I2, ' is ', 0P, F8.2 )
 9990 FORMAT( ' Matrix order=', I2, ', type=', I2, ', a=', E10.3,
     $      ', order(A_11)=', I2, ', result ', I2, ' is ', 0P, E10.3 )
 9989 FORMAT( ' Input example #', I2, ', matrix order=', I4, ',',
     $      ' result ', I2, ' is', 0P, F8.2 )
 9988 FORMAT( ' Input example #', I2, ', matrix order=', I4, ',',
     $      ' result ', I2, ' is', 1P, E10.3 )
*
*     End of SDRGSX
*
      END
