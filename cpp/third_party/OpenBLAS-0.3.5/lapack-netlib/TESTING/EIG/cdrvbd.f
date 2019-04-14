*> \brief \b CDRVBD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CDRVBD( NSIZES, MM, NN, NTYPES, DOTYPE, ISEED, THRESH,
*                          A, LDA, U, LDU, VT, LDVT, ASAV, USAV, VTSAV, S,
*                          SSAV, E, WORK, LWORK, RWORK, IWORK, NOUNIT,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDU, LDVT, LWORK, NOUNIT, NSIZES,
*      $                   NTYPES
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            ISEED( 4 ), IWORK( * ), MM( * ), NN( * )
*       REAL               E( * ), RWORK( * ), S( * ), SSAV( * )
*       COMPLEX            A( LDA, * ), ASAV( LDA, * ), U( LDU, * ),
*      $                   USAV( LDU, * ), VT( LDVT, * ),
*      $                   VTSAV( LDVT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CDRVBD checks the singular value decomposition (SVD) driver CGESVD
*> and CGESDD.
*> CGESVD and CGESDD factors A = U diag(S) VT, where U and VT are
*> unitary and diag(S) is diagonal with the entries of the array S on
*> its diagonal. The entries of S are the singular values, nonnegative
*> and stored in decreasing order.  U and VT can be optionally not
*> computed, overwritten on A, or computed partially.
*>
*> A is M by N. Let MNMIN = min( M, N ). S has dimension MNMIN.
*> U can be M by M or M by MNMIN. VT can be N by N or MNMIN by N.
*>
*> When CDRVBD is called, a number of matrix "sizes" (M's and N's)
*> and a number of matrix "types" are specified.  For each size (M,N)
*> and each type of matrix, and for the minimal workspace as well as
*> workspace adequate to permit blocking, an  M x N  matrix "A" will be
*> generated and used to test the SVD routines.  For each matrix, A will
*> be factored as A = U diag(S) VT and the following 12 tests computed:
*>
*> Test for CGESVD:
*>
*> (1)   | A - U diag(S) VT | / ( |A| max(M,N) ulp )
*>
*> (2)   | I - U'U | / ( M ulp )
*>
*> (3)   | I - VT VT' | / ( N ulp )
*>
*> (4)   S contains MNMIN nonnegative values in decreasing order.
*>       (Return 0 if true, 1/ULP if false.)
*>
*> (5)   | U - Upartial | / ( M ulp ) where Upartial is a partially
*>       computed U.
*>
*> (6)   | VT - VTpartial | / ( N ulp ) where VTpartial is a partially
*>       computed VT.
*>
*> (7)   | S - Spartial | / ( MNMIN ulp |S| ) where Spartial is the
*>       vector of singular values from the partial SVD
*>
*> Test for CGESDD:
*>
*> (1)   | A - U diag(S) VT | / ( |A| max(M,N) ulp )
*>
*> (2)   | I - U'U | / ( M ulp )
*>
*> (3)   | I - VT VT' | / ( N ulp )
*>
*> (4)   S contains MNMIN nonnegative values in decreasing order.
*>       (Return 0 if true, 1/ULP if false.)
*>
*> (5)   | U - Upartial | / ( M ulp ) where Upartial is a partially
*>       computed U.
*>
*> (6)   | VT - VTpartial | / ( N ulp ) where VTpartial is a partially
*>       computed VT.
*>
*> (7)   | S - Spartial | / ( MNMIN ulp |S| ) where Spartial is the
*>       vector of singular values from the partial SVD
*>
*> Test for CGESVJ:
*>
*> (1)   | A - U diag(S) VT | / ( |A| max(M,N) ulp )
*>
*> (2)   | I - U'U | / ( M ulp )
*>
*> (3)   | I - VT VT' | / ( N ulp )
*>
*> (4)   S contains MNMIN nonnegative values in decreasing order.
*>       (Return 0 if true, 1/ULP if false.)
*>
*> Test for CGEJSV:
*>
*> (1)   | A - U diag(S) VT | / ( |A| max(M,N) ulp )
*>
*> (2)   | I - U'U | / ( M ulp )
*>
*> (3)   | I - VT VT' | / ( N ulp )
*>
*> (4)   S contains MNMIN nonnegative values in decreasing order.
*>        (Return 0 if true, 1/ULP if false.)
*>
*> Test for CGESVDX( 'V', 'V', 'A' )/CGESVDX( 'N', 'N', 'A' )
*>
*> (1)   | A - U diag(S) VT | / ( |A| max(M,N) ulp )
*>
*> (2)   | I - U'U | / ( M ulp )
*>
*> (3)   | I - VT VT' | / ( N ulp )
*>
*> (4)   S contains MNMIN nonnegative values in decreasing order.
*>       (Return 0 if true, 1/ULP if false.)
*>
*> (5)   | U - Upartial | / ( M ulp ) where Upartial is a partially
*>       computed U.
*>
*> (6)   | VT - VTpartial | / ( N ulp ) where VTpartial is a partially
*>       computed VT.
*>
*> (7)   | S - Spartial | / ( MNMIN ulp |S| ) where Spartial is the
*>       vector of singular values from the partial SVD
*>
*> Test for CGESVDX( 'V', 'V', 'I' )
*>
*> (8)   | U' A VT''' - diag(S) | / ( |A| max(M,N) ulp )
*>
*> (9)   | I - U'U | / ( M ulp )
*>
*> (10)  | I - VT VT' | / ( N ulp )
*>
*> Test for CGESVDX( 'V', 'V', 'V' )
*>
*> (11)   | U' A VT''' - diag(S) | / ( |A| max(M,N) ulp )
*>
*> (12)   | I - U'U | / ( M ulp )
*>
*> (13)   | I - VT VT' | / ( N ulp )
*>
*> The "sizes" are specified by the arrays MM(1:NSIZES) and
*> NN(1:NSIZES); the value of each element pair (MM(j),NN(j))
*> specifies one size.  The "types" are specified by a logical array
*> DOTYPE( 1:NTYPES ); if DOTYPE(j) is .TRUE., then matrix type "j"
*> will be generated.
*> Currently, the list of possible types is:
*>
*> (1)  The zero matrix.
*> (2)  The identity matrix.
*> (3)  A matrix of the form  U D V, where U and V are unitary and
*>      D has evenly spaced entries 1, ..., ULP with random signs
*>      on the diagonal.
*> (4)  Same as (3), but multiplied by the underflow-threshold / ULP.
*> (5)  Same as (3), but multiplied by the overflow-threshold * ULP.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NSIZES
*> \verbatim
*>          NSIZES is INTEGER
*>          The number of sizes of matrices to use.  If it is zero,
*>          CDRVBD does nothing.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] MM
*> \verbatim
*>          MM is INTEGER array, dimension (NSIZES)
*>          An array containing the matrix "heights" to be used.  For
*>          each j=1,...,NSIZES, if MM(j) is zero, then MM(j) and NN(j)
*>          will be ignored.  The MM(j) values must be at least zero.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER array, dimension (NSIZES)
*>          An array containing the matrix "widths" to be used.  For
*>          each j=1,...,NSIZES, if NN(j) is zero, then MM(j) and NN(j)
*>          will be ignored.  The NN(j) values must be at least zero.
*> \endverbatim
*>
*> \param[in] NTYPES
*> \verbatim
*>          NTYPES is INTEGER
*>          The number of elements in DOTYPE.   If it is zero, CDRVBD
*>          does nothing.  It must be at least zero.  If it is MAXTYP+1
*>          and NSIZES is 1, then an additional type, MAXTYP+1 is
*>          defined, which is to use whatever matrices are in A and B.
*>          This is only useful if DOTYPE(1:MAXTYP) is .FALSE. and
*>          DOTYPE(MAXTYP+1) is .TRUE. .
*> \endverbatim
*>
*> \param[in] DOTYPE
*> \verbatim
*>          DOTYPE is LOGICAL array, dimension (NTYPES)
*>          If DOTYPE(j) is .TRUE., then for each size (m,n), a matrix
*>          of type j will be generated.  If NTYPES is smaller than the
*>          maximum number of types defined (PARAMETER MAXTYP), then
*>          types NTYPES+1 through MAXTYP will not be generated.  If
*>          NTYPES is larger than MAXTYP, DOTYPE(MAXTYP+1) through
*>          DOTYPE(NTYPES) will be ignored.
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
*>          next call to CDRVBD to continue the same random number
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
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,max(NN))
*>          Used to hold the matrix whose singular values are to be
*>          computed.  On exit, A contains the last matrix actually
*>          used.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at
*>          least 1 and at least max( MM ).
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX array, dimension (LDU,max(MM))
*>          Used to hold the computed matrix of right singular vectors.
*>          On exit, U contains the last such vectors actually computed.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  It must be at
*>          least 1 and at least max( MM ).
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is COMPLEX array, dimension (LDVT,max(NN))
*>          Used to hold the computed matrix of left singular vectors.
*>          On exit, VT contains the last such vectors actually computed.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>          The leading dimension of VT.  It must be at
*>          least 1 and at least max( NN ).
*> \endverbatim
*>
*> \param[out] ASAV
*> \verbatim
*>          ASAV is COMPLEX array, dimension (LDA,max(NN))
*>          Used to hold a different copy of the matrix whose singular
*>          values are to be computed.  On exit, A contains the last
*>          matrix actually used.
*> \endverbatim
*>
*> \param[out] USAV
*> \verbatim
*>          USAV is COMPLEX array, dimension (LDU,max(MM))
*>          Used to hold a different copy of the computed matrix of
*>          right singular vectors. On exit, USAV contains the last such
*>          vectors actually computed.
*> \endverbatim
*>
*> \param[out] VTSAV
*> \verbatim
*>          VTSAV is COMPLEX array, dimension (LDVT,max(NN))
*>          Used to hold a different copy of the computed matrix of
*>          left singular vectors. On exit, VTSAV contains the last such
*>          vectors actually computed.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (max(min(MM,NN)))
*>          Contains the computed singular values.
*> \endverbatim
*>
*> \param[out] SSAV
*> \verbatim
*>          SSAV is REAL array, dimension (max(min(MM,NN)))
*>          Contains another copy of the computed singular values.
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is REAL array, dimension (max(min(MM,NN)))
*>          Workspace for CGESVD.
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
*>          MAX(3*MIN(M,N)+MAX(M,N)**2,5*MIN(M,N),3*MAX(M,N)) for all
*>          pairs  (M,N)=(MM(j),NN(j))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array,
*>                      dimension ( 5*max(max(MM,NN)) )
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension at least 8*min(M,N)
*> \endverbatim
*>
*> \param[in] NOUNIT
*> \verbatim
*>          NOUNIT is INTEGER
*>          The FORTRAN unit number for printing out error messages
*>          (e.g., if a routine returns IINFO not equal to 0.)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          If 0, then everything ran OK.
*>           -1: NSIZES < 0
*>           -2: Some MM(j) < 0
*>           -3: Some NN(j) < 0
*>           -4: NTYPES < 0
*>           -7: THRESH < 0
*>          -10: LDA < 1 or LDA < MMAX, where MMAX is max( MM(j) ).
*>          -12: LDU < 1 or LDU < MMAX.
*>          -14: LDVT < 1 or LDVT < NMAX, where NMAX is max( NN(j) ).
*>          -29: LWORK too small.
*>          If  CLATMS, or CGESVD returns an error code, the
*>              absolute value of it is returned.
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
      SUBROUTINE CDRVBD( NSIZES, MM, NN, NTYPES, DOTYPE, ISEED, THRESH,
     $                   A, LDA, U, LDU, VT, LDVT, ASAV, USAV, VTSAV, S,
     $                   SSAV, E, WORK, LWORK, RWORK, IWORK, NOUNIT,
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDU, LDVT, LWORK, NOUNIT, NSIZES,
     $                   NTYPES
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            ISEED( 4 ), IWORK( * ), MM( * ), NN( * )
      REAL               E( * ), RWORK( * ), S( * ), SSAV( * )
      COMPLEX            A( LDA, * ), ASAV( LDA, * ), U( LDU, * ),
     $                   USAV( LDU, * ), VT( LDVT, * ),
     $                   VTSAV( LDVT, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL              ZERO, ONE, TWO, HALF
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0, TWO = 2.0E0,
     $                   HALF = 0.5E0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
      INTEGER            MAXTYP
      PARAMETER          ( MAXTYP = 5 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BADMM, BADNN
      CHARACTER          JOBQ, JOBU, JOBVT, RANGE
      INTEGER            I, IINFO, IJQ, IJU, IJVT, IL, IU, ITEMP,
     $                   IWSPC, IWTMP, J, JSIZE, JTYPE, LSWORK, M,
     $                   MINWRK, MMAX, MNMAX, MNMIN, MTYPES, N,
     $                   NERRS, NFAIL, NMAX, NS, NSI, NSV, NTEST,
     $                   NTESTF, NTESTT, LRWORK
      REAL               ANORM, DIF, DIV, OVFL, RTUNFL, ULP, ULPINV,
     $                   UNFL, VL, VU
*     ..
*     .. Local Arrays ..
      CHARACTER          CJOB( 4 ), CJOBR( 3 ), CJOBV( 2 )
      INTEGER            IOLDSD( 4 ), ISEED2( 4 )
      REAL               RESULT( 35 )
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLARND
      EXTERNAL           SLAMCH, SLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALASVM, XERBLA, CBDT01, CBDT05, CGESDD,
     $                   CGESVD, CGESVJ, CGEJSV, CGESVDX, CLACPY,
     $                   CLASET, CLATMS, CUNT01, CUNT03
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, MAX, MIN
*     ..
*     .. Scalars in Common ..
      CHARACTER*32       SRNAMT
*     ..
*     .. Common blocks ..
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Data statements ..
      DATA               CJOB / 'N', 'O', 'S', 'A' /
      DATA               CJOBR / 'A', 'V', 'I' /
      DATA               CJOBV / 'N', 'V' /
*     ..
*     .. Executable Statements ..
*
*     Check for errors
*
      INFO = 0
*
*     Important constants
*
      NERRS = 0
      NTESTT = 0
      NTESTF = 0
      BADMM = .FALSE.
      BADNN = .FALSE.
      MMAX = 1
      NMAX = 1
      MNMAX = 1
      MINWRK = 1
      DO 10 J = 1, NSIZES
         MMAX = MAX( MMAX, MM( J ) )
         IF( MM( J ).LT.0 )
     $      BADMM = .TRUE.
         NMAX = MAX( NMAX, NN( J ) )
         IF( NN( J ).LT.0 )
     $      BADNN = .TRUE.
         MNMAX = MAX( MNMAX, MIN( MM( J ), NN( J ) ) )
         MINWRK = MAX( MINWRK, MAX( 3*MIN( MM( J ),
     $            NN( J ) )+MAX( MM( J ), NN( J ) )**2, 5*MIN( MM( J ),
     $            NN( J ) ), 3*MAX( MM( J ), NN( J ) ) ) )
   10 CONTINUE
*
*     Check for errors
*
      IF( NSIZES.LT.0 ) THEN
         INFO = -1
      ELSE IF( BADMM ) THEN
         INFO = -2
      ELSE IF( BADNN ) THEN
         INFO = -3
      ELSE IF( NTYPES.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, MMAX ) ) THEN
         INFO = -10
      ELSE IF( LDU.LT.MAX( 1, MMAX ) ) THEN
         INFO = -12
      ELSE IF( LDVT.LT.MAX( 1, NMAX ) ) THEN
         INFO = -14
      ELSE IF( MINWRK.GT.LWORK ) THEN
         INFO = -21
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CDRVBD', -INFO )
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
      UNFL = SLAMCH( 'S' )
      OVFL = ONE / UNFL
      ULP = SLAMCH( 'E' )
      ULPINV = ONE / ULP
      RTUNFL = SQRT( UNFL )
*
*     Loop over sizes, types
*
      NERRS = 0
*
      DO 310 JSIZE = 1, NSIZES
         M = MM( JSIZE )
         N = NN( JSIZE )
         MNMIN = MIN( M, N )
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
            NTEST = 0
*
            DO 20 J = 1, 4
               IOLDSD( J ) = ISEED( J )
   20       CONTINUE
*
*           Compute "A"
*
            IF( MTYPES.GT.MAXTYP )
     $         GO TO 50
*
            IF( JTYPE.EQ.1 ) THEN
*
*              Zero matrix
*
               CALL CLASET( 'Full', M, N, CZERO, CZERO, A, LDA )
               DO 30 I = 1, MIN( M, N )
                  S( I ) = ZERO
   30          CONTINUE
*
            ELSE IF( JTYPE.EQ.2 ) THEN
*
*              Identity matrix
*
               CALL CLASET( 'Full', M, N, CZERO, CONE, A, LDA )
               DO 40 I = 1, MIN( M, N )
                  S( I ) = ONE
   40          CONTINUE
*
            ELSE
*
*              (Scaled) random matrix
*
               IF( JTYPE.EQ.3 )
     $            ANORM = ONE
               IF( JTYPE.EQ.4 )
     $            ANORM = UNFL / ULP
               IF( JTYPE.EQ.5 )
     $            ANORM = OVFL*ULP
               CALL CLATMS( M, N, 'U', ISEED, 'N', S, 4, REAL( MNMIN ),
     $                      ANORM, M-1, N-1, 'N', A, LDA, WORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9996 )'Generator', IINFO, M, N,
     $               JTYPE, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
            END IF
*
   50       CONTINUE
            CALL CLACPY( 'F', M, N, A, LDA, ASAV, LDA )
*
*           Do for minimal and adequate (for blocking) workspace
*
            DO 290 IWSPC = 1, 4
*
*              Test for CGESVD
*
               IWTMP = 2*MIN( M, N )+MAX( M, N )
               LSWORK = IWTMP + ( IWSPC-1 )*( LWORK-IWTMP ) / 3
               LSWORK = MIN( LSWORK, LWORK )
               LSWORK = MAX( LSWORK, 1 )
               IF( IWSPC.EQ.4 )
     $            LSWORK = LWORK
*
               DO 60 J = 1, 35
                  RESULT( J ) = -ONE
   60          CONTINUE
*
*              Factorize A
*
               IF( IWSPC.GT.1 )
     $            CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
               SRNAMT = 'CGESVD'
               CALL CGESVD( 'A', 'A', M, N, A, LDA, SSAV, USAV, LDU,
     $                      VTSAV, LDVT, WORK, LSWORK, RWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9995 )'GESVD', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
*
*              Do tests 1--4
*
               CALL CBDT01( M, N, 0, ASAV, LDA, USAV, LDU, SSAV, E,
     $                      VTSAV, LDVT, WORK, RWORK, RESULT( 1 ) )
               IF( M.NE.0 .AND. N.NE.0 ) THEN
                  CALL CUNT01( 'Columns', MNMIN, M, USAV, LDU, WORK,
     $                         LWORK, RWORK, RESULT( 2 ) )
                  CALL CUNT01( 'Rows', MNMIN, N, VTSAV, LDVT, WORK,
     $                         LWORK, RWORK, RESULT( 3 ) )
               END IF
               RESULT( 4 ) = 0
               DO 70 I = 1, MNMIN - 1
                  IF( SSAV( I ).LT.SSAV( I+1 ) )
     $               RESULT( 4 ) = ULPINV
                  IF( SSAV( I ).LT.ZERO )
     $               RESULT( 4 ) = ULPINV
   70          CONTINUE
               IF( MNMIN.GE.1 ) THEN
                  IF( SSAV( MNMIN ).LT.ZERO )
     $               RESULT( 4 ) = ULPINV
               END IF
*
*              Do partial SVDs, comparing to SSAV, USAV, and VTSAV
*
               RESULT( 5 ) = ZERO
               RESULT( 6 ) = ZERO
               RESULT( 7 ) = ZERO
               DO 100 IJU = 0, 3
                  DO 90 IJVT = 0, 3
                     IF( ( IJU.EQ.3 .AND. IJVT.EQ.3 ) .OR.
     $                   ( IJU.EQ.1 .AND. IJVT.EQ.1 ) )GO TO 90
                     JOBU = CJOB( IJU+1 )
                     JOBVT = CJOB( IJVT+1 )
                     CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
                     SRNAMT = 'CGESVD'
                     CALL CGESVD( JOBU, JOBVT, M, N, A, LDA, S, U, LDU,
     $                            VT, LDVT, WORK, LSWORK, RWORK, IINFO )
*
*                    Compare U
*
                     DIF = ZERO
                     IF( M.GT.0 .AND. N.GT.0 ) THEN
                        IF( IJU.EQ.1 ) THEN
                           CALL CUNT03( 'C', M, MNMIN, M, MNMIN, USAV,
     $                                  LDU, A, LDA, WORK, LWORK, RWORK,
     $                                  DIF, IINFO )
                        ELSE IF( IJU.EQ.2 ) THEN
                           CALL CUNT03( 'C', M, MNMIN, M, MNMIN, USAV,
     $                                  LDU, U, LDU, WORK, LWORK, RWORK,
     $                                  DIF, IINFO )
                        ELSE IF( IJU.EQ.3 ) THEN
                           CALL CUNT03( 'C', M, M, M, MNMIN, USAV, LDU,
     $                                  U, LDU, WORK, LWORK, RWORK, DIF,
     $                                  IINFO )
                        END IF
                     END IF
                     RESULT( 5 ) = MAX( RESULT( 5 ), DIF )
*
*                    Compare VT
*
                     DIF = ZERO
                     IF( M.GT.0 .AND. N.GT.0 ) THEN
                        IF( IJVT.EQ.1 ) THEN
                           CALL CUNT03( 'R', N, MNMIN, N, MNMIN, VTSAV,
     $                                  LDVT, A, LDA, WORK, LWORK,
     $                                  RWORK, DIF, IINFO )
                        ELSE IF( IJVT.EQ.2 ) THEN
                           CALL CUNT03( 'R', N, MNMIN, N, MNMIN, VTSAV,
     $                                  LDVT, VT, LDVT, WORK, LWORK,
     $                                  RWORK, DIF, IINFO )
                        ELSE IF( IJVT.EQ.3 ) THEN
                           CALL CUNT03( 'R', N, N, N, MNMIN, VTSAV,
     $                                  LDVT, VT, LDVT, WORK, LWORK,
     $                                  RWORK, DIF, IINFO )
                        END IF
                     END IF
                     RESULT( 6 ) = MAX( RESULT( 6 ), DIF )
*
*                    Compare S
*
                     DIF = ZERO
                     DIV = MAX( REAL( MNMIN )*ULP*S( 1 ),
     $                     SLAMCH( 'Safe minimum' ) )
                     DO 80 I = 1, MNMIN - 1
                        IF( SSAV( I ).LT.SSAV( I+1 ) )
     $                     DIF = ULPINV
                        IF( SSAV( I ).LT.ZERO )
     $                     DIF = ULPINV
                        DIF = MAX( DIF, ABS( SSAV( I )-S( I ) ) / DIV )
   80                CONTINUE
                     RESULT( 7 ) = MAX( RESULT( 7 ), DIF )
   90             CONTINUE
  100          CONTINUE
*
*              Test for CGESDD
*
               IWTMP = 2*MNMIN*MNMIN + 2*MNMIN + MAX( M, N )
               LSWORK = IWTMP + ( IWSPC-1 )*( LWORK-IWTMP ) / 3
               LSWORK = MIN( LSWORK, LWORK )
               LSWORK = MAX( LSWORK, 1 )
               IF( IWSPC.EQ.4 )
     $            LSWORK = LWORK
*
*              Factorize A
*
               CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
               SRNAMT = 'CGESDD'
               CALL CGESDD( 'A', M, N, A, LDA, SSAV, USAV, LDU, VTSAV,
     $                      LDVT, WORK, LSWORK, RWORK, IWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9995 )'GESDD', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
*
*              Do tests 1--4
*
               CALL CBDT01( M, N, 0, ASAV, LDA, USAV, LDU, SSAV, E,
     $                      VTSAV, LDVT, WORK, RWORK, RESULT( 8 ) )
               IF( M.NE.0 .AND. N.NE.0 ) THEN
                  CALL CUNT01( 'Columns', MNMIN, M, USAV, LDU, WORK,
     $                         LWORK, RWORK, RESULT( 9 ) )
                  CALL CUNT01( 'Rows', MNMIN, N, VTSAV, LDVT, WORK,
     $                         LWORK, RWORK, RESULT( 10 ) )
               END IF
               RESULT( 11 ) = 0
               DO 110 I = 1, MNMIN - 1
                  IF( SSAV( I ).LT.SSAV( I+1 ) )
     $               RESULT( 11 ) = ULPINV
                  IF( SSAV( I ).LT.ZERO )
     $               RESULT( 11 ) = ULPINV
  110          CONTINUE
               IF( MNMIN.GE.1 ) THEN
                  IF( SSAV( MNMIN ).LT.ZERO )
     $               RESULT( 11 ) = ULPINV
               END IF
*
*              Do partial SVDs, comparing to SSAV, USAV, and VTSAV
*
               RESULT( 12 ) = ZERO
               RESULT( 13 ) = ZERO
               RESULT( 14 ) = ZERO
               DO 130 IJQ = 0, 2
                  JOBQ = CJOB( IJQ+1 )
                  CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
                  SRNAMT = 'CGESDD'
                  CALL CGESDD( JOBQ, M, N, A, LDA, S, U, LDU, VT, LDVT,
     $                         WORK, LSWORK, RWORK, IWORK, IINFO )
*
*                 Compare U
*
                  DIF = ZERO
                  IF( M.GT.0 .AND. N.GT.0 ) THEN
                     IF( IJQ.EQ.1 ) THEN
                        IF( M.GE.N ) THEN
                           CALL CUNT03( 'C', M, MNMIN, M, MNMIN, USAV,
     $                                  LDU, A, LDA, WORK, LWORK, RWORK,
     $                                  DIF, IINFO )
                        ELSE
                           CALL CUNT03( 'C', M, MNMIN, M, MNMIN, USAV,
     $                                  LDU, U, LDU, WORK, LWORK, RWORK,
     $                                  DIF, IINFO )
                        END IF
                     ELSE IF( IJQ.EQ.2 ) THEN
                        CALL CUNT03( 'C', M, MNMIN, M, MNMIN, USAV, LDU,
     $                               U, LDU, WORK, LWORK, RWORK, DIF,
     $                               IINFO )
                     END IF
                  END IF
                  RESULT( 12 ) = MAX( RESULT( 12 ), DIF )
*
*                 Compare VT
*
                  DIF = ZERO
                  IF( M.GT.0 .AND. N.GT.0 ) THEN
                     IF( IJQ.EQ.1 ) THEN
                        IF( M.GE.N ) THEN
                           CALL CUNT03( 'R', N, MNMIN, N, MNMIN, VTSAV,
     $                                  LDVT, VT, LDVT, WORK, LWORK,
     $                                  RWORK, DIF, IINFO )
                        ELSE
                           CALL CUNT03( 'R', N, MNMIN, N, MNMIN, VTSAV,
     $                                  LDVT, A, LDA, WORK, LWORK,
     $                                  RWORK, DIF, IINFO )
                        END IF
                     ELSE IF( IJQ.EQ.2 ) THEN
                        CALL CUNT03( 'R', N, MNMIN, N, MNMIN, VTSAV,
     $                               LDVT, VT, LDVT, WORK, LWORK, RWORK,
     $                               DIF, IINFO )
                     END IF
                  END IF
                  RESULT( 13 ) = MAX( RESULT( 13 ), DIF )
*
*                 Compare S
*
                  DIF = ZERO
                  DIV = MAX( REAL( MNMIN )*ULP*S( 1 ),
     $                  SLAMCH( 'Safe minimum' ) )
                  DO 120 I = 1, MNMIN - 1
                     IF( SSAV( I ).LT.SSAV( I+1 ) )
     $                  DIF = ULPINV
                     IF( SSAV( I ).LT.ZERO )
     $                  DIF = ULPINV
                     DIF = MAX( DIF, ABS( SSAV( I )-S( I ) ) / DIV )
  120             CONTINUE
                  RESULT( 14 ) = MAX( RESULT( 14 ), DIF )
  130          CONTINUE

*
*              Test CGESVJ: Factorize A
*              Note: CGESVJ does not work for M < N
*
               RESULT( 15 ) = ZERO
               RESULT( 16 ) = ZERO
               RESULT( 17 ) = ZERO
               RESULT( 18 ) = ZERO
*
               IF( M.GE.N ) THEN
               IWTMP = 2*MNMIN*MNMIN + 2*MNMIN + MAX( M, N )
               LSWORK = IWTMP + ( IWSPC-1 )*( LWORK-IWTMP ) / 3
               LSWORK = MIN( LSWORK, LWORK )
               LSWORK = MAX( LSWORK, 1 )
               LRWORK = MAX(6,N)
               IF( IWSPC.EQ.4 )
     $            LSWORK = LWORK
*
                  CALL CLACPY( 'F', M, N, ASAV, LDA, USAV, LDA )
                  SRNAMT = 'CGESVJ'
                  CALL CGESVJ( 'G', 'U', 'V', M, N, USAV, LDA, SSAV,
     &                        0, A, LDVT, WORK, LWORK, RWORK,
     &                        LRWORK, IINFO )
*
*                 CGESVJ retuns V not VT, so we transpose to use the same
*                 test suite.
*
                  DO J=1,N
                     DO I=1,N
                        VTSAV(J,I) = CONJG (A(I,J))
                     END DO
                  END DO
*
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9995 )'GESVJ', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                     INFO = ABS( IINFO )
                     RETURN
                  END IF
*
*                 Do tests 15--18
*
                  CALL CBDT01( M, N, 0, ASAV, LDA, USAV, LDU, SSAV, E,
     $                         VTSAV, LDVT, WORK, RWORK, RESULT( 15 ) )
                  IF( M.NE.0 .AND. N.NE.0 ) THEN
                     CALL CUNT01( 'Columns', M, M, USAV, LDU, WORK,
     $                            LWORK, RWORK, RESULT( 16 ) )
                     CALL CUNT01( 'Rows', N, N, VTSAV, LDVT, WORK,
     $                            LWORK, RWORK, RESULT( 17 ) )
                  END IF
                  RESULT( 18 ) = ZERO
                  DO 131 I = 1, MNMIN - 1
                     IF( SSAV( I ).LT.SSAV( I+1 ) )
     $                  RESULT( 18 ) = ULPINV
                     IF( SSAV( I ).LT.ZERO )
     $                  RESULT( 18 ) = ULPINV
  131             CONTINUE
                  IF( MNMIN.GE.1 ) THEN
                     IF( SSAV( MNMIN ).LT.ZERO )
     $                  RESULT( 18 ) = ULPINV
                  END IF
               END IF
*
*              Test CGEJSV: Factorize A
*              Note: CGEJSV does not work for M < N
*
               RESULT( 19 ) = ZERO
               RESULT( 20 ) = ZERO
               RESULT( 21 ) = ZERO
               RESULT( 22 ) = ZERO
               IF( M.GE.N ) THEN
               IWTMP = 2*MNMIN*MNMIN + 2*MNMIN + MAX( M, N )
               LSWORK = IWTMP + ( IWSPC-1 )*( LWORK-IWTMP ) / 3
               LSWORK = MIN( LSWORK, LWORK )
               LSWORK = MAX( LSWORK, 1 )
               IF( IWSPC.EQ.4 )
     $            LSWORK = LWORK
               LRWORK = MAX( 7, N + 2*M)
*
                 CALL CLACPY( 'F', M, N, ASAV, LDA, VTSAV, LDA )
                  SRNAMT = 'CGEJSV'
                  CALL CGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     &                   M, N, VTSAV, LDA, SSAV, USAV, LDU, A, LDVT,
     &                   WORK, LWORK, RWORK,
     &                   LRWORK, IWORK, IINFO )
*
*                 CGEJSV retuns V not VT, so we transpose to use the same
*                 test suite.
*
                  DO 133 J=1,N
                     DO 132 I=1,N
                        VTSAV(J,I) = CONJG (A(I,J))
  132                END DO
  133             END DO
*
                  IF( IINFO.NE.0 ) THEN
                     WRITE( NOUNIT, FMT = 9995 )'GESVJ', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                     INFO = ABS( IINFO )
                     RETURN
                  END IF
*
*                 Do tests 19--22
*
                  CALL CBDT01( M, N, 0, ASAV, LDA, USAV, LDU, SSAV, E,
     $                         VTSAV, LDVT, WORK, RWORK, RESULT( 19 ) )
                  IF( M.NE.0 .AND. N.NE.0 ) THEN
                     CALL CUNT01( 'Columns', M, M, USAV, LDU, WORK,
     $                            LWORK, RWORK, RESULT( 20 ) )
                     CALL CUNT01( 'Rows', N, N, VTSAV, LDVT, WORK,
     $                            LWORK, RWORK, RESULT( 21 ) )
                  END IF
                  RESULT( 22 ) = ZERO
                  DO 134 I = 1, MNMIN - 1
                     IF( SSAV( I ).LT.SSAV( I+1 ) )
     $                  RESULT( 22 ) = ULPINV
                     IF( SSAV( I ).LT.ZERO )
     $                  RESULT( 22 ) = ULPINV
  134             CONTINUE
                  IF( MNMIN.GE.1 ) THEN
                     IF( SSAV( MNMIN ).LT.ZERO )
     $                  RESULT( 22 ) = ULPINV
                  END IF
               END IF
*
*              Test CGESVDX
*
*              Factorize A
*
               CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
               SRNAMT = 'CGESVDX'
               CALL CGESVDX( 'V', 'V', 'A', M, N, A, LDA,
     $                       VL, VU, IL, IU, NS, SSAV, USAV, LDU,
     $                       VTSAV, LDVT, WORK, LWORK, RWORK,
     $                       IWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9995 )'GESVDX', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
*
*              Do tests 1--4
*
               RESULT( 23 ) = ZERO
               RESULT( 24 ) = ZERO
               RESULT( 25 ) = ZERO
               CALL CBDT01( M, N, 0, ASAV, LDA, USAV, LDU, SSAV, E,
     $                      VTSAV, LDVT, WORK, RWORK, RESULT( 23 ) )
               IF( M.NE.0 .AND. N.NE.0 ) THEN
                  CALL CUNT01( 'Columns', MNMIN, M, USAV, LDU, WORK,
     $                         LWORK, RWORK, RESULT( 24 ) )
                  CALL CUNT01( 'Rows', MNMIN, N, VTSAV, LDVT, WORK,
     $                         LWORK, RWORK, RESULT( 25 ) )
               END IF
               RESULT( 26 ) = ZERO
               DO 140 I = 1, MNMIN - 1
                  IF( SSAV( I ).LT.SSAV( I+1 ) )
     $               RESULT( 26 ) = ULPINV
                  IF( SSAV( I ).LT.ZERO )
     $               RESULT( 26 ) = ULPINV
  140          CONTINUE
               IF( MNMIN.GE.1 ) THEN
                  IF( SSAV( MNMIN ).LT.ZERO )
     $               RESULT( 26 ) = ULPINV
               END IF
*
*              Do partial SVDs, comparing to SSAV, USAV, and VTSAV
*
               RESULT( 27 ) = ZERO
               RESULT( 28 ) = ZERO
               RESULT( 29 ) = ZERO
               DO 170 IJU = 0, 1
                  DO 160 IJVT = 0, 1
                     IF( ( IJU.EQ.0 .AND. IJVT.EQ.0 ) .OR.
     $                   ( IJU.EQ.1 .AND. IJVT.EQ.1 ) ) GO TO 160
                     JOBU = CJOBV( IJU+1 )
                     JOBVT = CJOBV( IJVT+1 )
                     RANGE = CJOBR( 1 )
                     CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
                     SRNAMT = 'CGESVDX'
                     CALL CGESVDX( JOBU, JOBVT, 'A', M, N, A, LDA,
     $                            VL, VU, IL, IU, NS, SSAV, U, LDU,
     $                            VT, LDVT, WORK, LWORK, RWORK,
     $                            IWORK, IINFO )
*
*                    Compare U
*
                     DIF = ZERO
                     IF( M.GT.0 .AND. N.GT.0 ) THEN
                        IF( IJU.EQ.1 ) THEN
                           CALL CUNT03( 'C', M, MNMIN, M, MNMIN, USAV,
     $                                  LDU, U, LDU, WORK, LWORK, RWORK,
     $                                  DIF, IINFO )
                        END IF
                     END IF
                     RESULT( 27 ) = MAX( RESULT( 27 ), DIF )
*
*                    Compare VT
*
                     DIF = ZERO
                     IF( M.GT.0 .AND. N.GT.0 ) THEN
                        IF( IJVT.EQ.1 ) THEN
                           CALL CUNT03( 'R', N, MNMIN, N, MNMIN, VTSAV,
     $                                  LDVT, VT, LDVT, WORK, LWORK,
     $                                  RWORK, DIF, IINFO )
                        END IF
                     END IF
                     RESULT( 28 ) = MAX( RESULT( 28 ), DIF )
*
*                    Compare S
*
                     DIF = ZERO
                     DIV = MAX( REAL( MNMIN )*ULP*S( 1 ),
     $                     SLAMCH( 'Safe minimum' ) )
                     DO 150 I = 1, MNMIN - 1
                        IF( SSAV( I ).LT.SSAV( I+1 ) )
     $                     DIF = ULPINV
                        IF( SSAV( I ).LT.ZERO )
     $                     DIF = ULPINV
                        DIF = MAX( DIF, ABS( SSAV( I )-S( I ) ) / DIV )
  150                CONTINUE
                     RESULT( 29) = MAX( RESULT( 29 ), DIF )
  160             CONTINUE
  170          CONTINUE
*
*              Do tests 8--10
*
               DO 180 I = 1, 4
                  ISEED2( I ) = ISEED( I )
  180          CONTINUE
               IF( MNMIN.LE.1 ) THEN
                  IL = 1
                  IU = MAX( 1, MNMIN )
               ELSE
                  IL = 1 + INT( ( MNMIN-1 )*SLARND( 1, ISEED2 ) )
                  IU = 1 + INT( ( MNMIN-1 )*SLARND( 1, ISEED2 ) )
                  IF( IU.LT.IL ) THEN
                     ITEMP = IU
                     IU = IL
                     IL = ITEMP
                  END IF
               END IF
               CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
               SRNAMT = 'CGESVDX'
               CALL CGESVDX( 'V', 'V', 'I', M, N, A, LDA,
     $                       VL, VU, IL, IU, NSI, S, U, LDU,
     $                       VT, LDVT, WORK, LWORK, RWORK,
     $                       IWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9995 )'GESVDX', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
*
               RESULT( 30 ) = ZERO
               RESULT( 31 ) = ZERO
               RESULT( 32 ) = ZERO
               CALL CBDT05( M, N, ASAV, LDA, S, NSI, U, LDU,
     $                      VT, LDVT, WORK, RESULT( 30 ) )
               IF( M.NE.0 .AND. N.NE.0 ) THEN
                  CALL CUNT01( 'Columns', M, NSI, U, LDU, WORK,
     $                         LWORK, RWORK, RESULT( 31 ) )
                  CALL CUNT01( 'Rows', NSI, N, VT, LDVT, WORK,
     $                         LWORK, RWORK, RESULT( 32 ) )
               END IF
*
*              Do tests 11--13
*
               IF( MNMIN.GT.0 .AND. NSI.GT.1 ) THEN
                  IF( IL.NE.1 ) THEN
                     VU = SSAV( IL ) +
     $                    MAX( HALF*ABS( SSAV( IL )-SSAV( IL-1 ) ),
     $                    ULP*ANORM, TWO*RTUNFL )
                  ELSE
                     VU = SSAV( 1 ) +
     $                    MAX( HALF*ABS( SSAV( NS )-SSAV( 1 ) ),
     $                    ULP*ANORM, TWO*RTUNFL )
                  END IF
                  IF( IU.NE.NS ) THEN
                     VL = SSAV( IU ) - MAX( ULP*ANORM, TWO*RTUNFL,
     $                    HALF*ABS( SSAV( IU+1 )-SSAV( IU ) ) )
                  ELSE
                     VL = SSAV( NS ) - MAX( ULP*ANORM, TWO*RTUNFL,
     $                    HALF*ABS( SSAV( NS )-SSAV( 1 ) ) )
                  END IF
                  VL = MAX( VL,ZERO )
                  VU = MAX( VU,ZERO )
                  IF( VL.GE.VU ) VU = MAX( VU*2, VU+VL+HALF )
               ELSE
                  VL = ZERO
                  VU = ONE
               END IF
               CALL CLACPY( 'F', M, N, ASAV, LDA, A, LDA )
               SRNAMT = 'CGESVDX'
               CALL CGESVDX( 'V', 'V', 'V', M, N, A, LDA,
     $                       VL, VU, IL, IU, NSV, S, U, LDU,
     $                       VT, LDVT, WORK, LWORK, RWORK,
     $                       IWORK, IINFO )
               IF( IINFO.NE.0 ) THEN
                  WRITE( NOUNIT, FMT = 9995 )'GESVDX', IINFO, M, N,
     $               JTYPE, LSWORK, IOLDSD
                  INFO = ABS( IINFO )
                  RETURN
               END IF
*
               RESULT( 33 ) = ZERO
               RESULT( 34 ) = ZERO
               RESULT( 35 ) = ZERO
               CALL CBDT05( M, N, ASAV, LDA, S, NSV, U, LDU,
     $                      VT, LDVT, WORK, RESULT( 33 ) )
               IF( M.NE.0 .AND. N.NE.0 ) THEN
                  CALL CUNT01( 'Columns', M, NSV, U, LDU, WORK,
     $                         LWORK, RWORK, RESULT( 34 ) )
                  CALL CUNT01( 'Rows', NSV, N, VT, LDVT, WORK,
     $                         LWORK, RWORK, RESULT( 35 ) )
               END IF
*
*              End of Loop -- Check for RESULT(j) > THRESH
*
               NTEST = 0
               NFAIL = 0
               DO 190 J = 1, 35
                  IF( RESULT( J ).GE.ZERO )
     $               NTEST = NTEST + 1
                  IF( RESULT( J ).GE.THRESH )
     $               NFAIL = NFAIL + 1
  190          CONTINUE
*
               IF( NFAIL.GT.0 )
     $            NTESTF = NTESTF + 1
               IF( NTESTF.EQ.1 ) THEN
                  WRITE( NOUNIT, FMT = 9999 )
                  WRITE( NOUNIT, FMT = 9998 )THRESH
                  NTESTF = 2
               END IF
*
               DO 200 J = 1, 35
                  IF( RESULT( J ).GE.THRESH ) THEN
                     WRITE( NOUNIT, FMT = 9997 )M, N, JTYPE, IWSPC,
     $                  IOLDSD, J, RESULT( J )
                  END IF
  200          CONTINUE
*
               NERRS = NERRS + NFAIL
               NTESTT = NTESTT + NTEST
*
  290       CONTINUE
*
  300    CONTINUE
  310 CONTINUE
*
*     Summary
*
      CALL ALASVM( 'CBD', NOUNIT, NERRS, NTESTT, 0 )
*
 9999 FORMAT( ' SVD -- Complex Singular Value Decomposition Driver ',
     $      / ' Matrix types (see CDRVBD for details):',
     $      / / ' 1 = Zero matrix', / ' 2 = Identity matrix',
     $      / ' 3 = Evenly spaced singular values near 1',
     $      / ' 4 = Evenly spaced singular values near underflow',
     $      / ' 5 = Evenly spaced singular values near overflow',
     $      / / ' Tests performed: ( A is dense, U and V are unitary,',
     $      / 19X, ' S is an array, and Upartial, VTpartial, and',
     $      / 19X, ' Spartial are partially computed U, VT and S),', / )
 9998 FORMAT( ' Tests performed with Test Threshold = ', F8.2,
     $      / ' CGESVD: ', /
     $      ' 1 = | A - U diag(S) VT | / ( |A| max(M,N) ulp ) ',
     $      / ' 2 = | I - U**T U | / ( M ulp ) ',
     $      / ' 3 = | I - VT VT**T | / ( N ulp ) ',
     $      / ' 4 = 0 if S contains min(M,N) nonnegative values in',
     $      ' decreasing order, else 1/ulp',
     $      / ' 5 = | U - Upartial | / ( M ulp )',
     $      / ' 6 = | VT - VTpartial | / ( N ulp )',
     $      / ' 7 = | S - Spartial | / ( min(M,N) ulp |S| )',
     $      / ' CGESDD: ', /
     $      ' 8 = | A - U diag(S) VT | / ( |A| max(M,N) ulp ) ',
     $      / ' 9 = | I - U**T U | / ( M ulp ) ',
     $      / '10 = | I - VT VT**T | / ( N ulp ) ',
     $      / '11 = 0 if S contains min(M,N) nonnegative values in',
     $      ' decreasing order, else 1/ulp',
     $      / '12 = | U - Upartial | / ( M ulp )',
     $      / '13 = | VT - VTpartial | / ( N ulp )',
     $      / '14 = | S - Spartial | / ( min(M,N) ulp |S| )',
     $      / ' CGESVJ: ', /
     $      / '15 = | A - U diag(S) VT | / ( |A| max(M,N) ulp ) ',
     $      / '16 = | I - U**T U | / ( M ulp ) ',
     $      / '17 = | I - VT VT**T | / ( N ulp ) ',
     $      / '18 = 0 if S contains min(M,N) nonnegative values in',
     $      ' decreasing order, else 1/ulp',
     $      / ' CGESJV: ', /
     $      / '19 = | A - U diag(S) VT | / ( |A| max(M,N) ulp )',
     $      / '20 = | I - U**T U | / ( M ulp ) ',
     $      / '21 = | I - VT VT**T | / ( N ulp ) ',
     $      / '22 = 0 if S contains min(M,N) nonnegative values in',
     $      ' decreasing order, else 1/ulp',
     $      / ' CGESVDX(V,V,A): ', /
     $        '23 = | A - U diag(S) VT | / ( |A| max(M,N) ulp ) ',
     $      / '24 = | I - U**T U | / ( M ulp ) ',
     $      / '25 = | I - VT VT**T | / ( N ulp ) ',
     $      / '26 = 0 if S contains min(M,N) nonnegative values in',
     $      ' decreasing order, else 1/ulp',
     $      / '27 = | U - Upartial | / ( M ulp )',
     $      / '28 = | VT - VTpartial | / ( N ulp )',
     $      / '29 = | S - Spartial | / ( min(M,N) ulp |S| )',
     $      / ' CGESVDX(V,V,I): ',
     $      / '30 = | U**T A VT**T - diag(S) | / ( |A| max(M,N) ulp )',
     $      / '31 = | I - U**T U | / ( M ulp ) ',
     $      / '32 = | I - VT VT**T | / ( N ulp ) ',
     $      / ' CGESVDX(V,V,V) ',
     $      / '33 = | U**T A VT**T - diag(S) | / ( |A| max(M,N) ulp )',
     $      / '34 = | I - U**T U | / ( M ulp ) ',
     $      / '35 = | I - VT VT**T | / ( N ulp ) ',
     $      / / )
 9997 FORMAT( ' M=', I5, ', N=', I5, ', type ', I1, ', IWS=', I1,
     $      ', seed=', 4( I4, ',' ), ' test(', I2, ')=', G11.4 )
 9996 FORMAT( ' CDRVBD: ', A, ' returned INFO=', I6, '.', / 9X, 'M=',
     $      I6, ', N=', I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ),
     $      I5, ')' )
 9995 FORMAT( ' CDRVBD: ', A, ' returned INFO=', I6, '.', / 9X, 'M=',
     $      I6, ', N=', I6, ', JTYPE=', I6, ', LSWORK=', I6, / 9X,
     $      'ISEED=(', 3( I5, ',' ), I5, ')' )
*
      RETURN
*
*     End of CDRVBD
*
      END
