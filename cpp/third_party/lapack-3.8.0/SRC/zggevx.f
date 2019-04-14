*> \brief <b> ZGGEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGGEVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zggevx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zggevx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zggevx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGGEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, B, LDB,
*                          ALPHA, BETA, VL, LDVL, VR, LDVR, ILO, IHI,
*                          LSCALE, RSCALE, ABNRM, BBNRM, RCONDE, RCONDV,
*                          WORK, LWORK, RWORK, IWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          BALANC, JOBVL, JOBVR, SENSE
*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDVL, LDVR, LWORK, N
*       DOUBLE PRECISION   ABNRM, BBNRM
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   LSCALE( * ), RCONDE( * ), RCONDV( * ),
*      $                   RSCALE( * ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ),
*      $                   BETA( * ), VL( LDVL, * ), VR( LDVR, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGGEVX computes for a pair of N-by-N complex nonsymmetric matrices
*> (A,B) the generalized eigenvalues, and optionally, the left and/or
*> right generalized eigenvectors.
*>
*> Optionally, it also computes a balancing transformation to improve
*> the conditioning of the eigenvalues and eigenvectors (ILO, IHI,
*> LSCALE, RSCALE, ABNRM, and BBNRM), reciprocal condition numbers for
*> the eigenvalues (RCONDE), and reciprocal condition numbers for the
*> right eigenvectors (RCONDV).
*>
*> A generalized eigenvalue for a pair of matrices (A,B) is a scalar
*> lambda or a ratio alpha/beta = lambda, such that A - lambda*B is
*> singular. It is usually represented as the pair (alpha,beta), as
*> there is a reasonable interpretation for beta=0, and even for both
*> being zero.
*>
*> The right eigenvector v(j) corresponding to the eigenvalue lambda(j)
*> of (A,B) satisfies
*>                  A * v(j) = lambda(j) * B * v(j) .
*> The left eigenvector u(j) corresponding to the eigenvalue lambda(j)
*> of (A,B) satisfies
*>                  u(j)**H * A  = lambda(j) * u(j)**H * B.
*> where u(j)**H is the conjugate-transpose of u(j).
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] BALANC
*> \verbatim
*>          BALANC is CHARACTER*1
*>          Specifies the balance option to be performed:
*>          = 'N':  do not diagonally scale or permute;
*>          = 'P':  permute only;
*>          = 'S':  scale only;
*>          = 'B':  both permute and scale.
*>          Computed reciprocal condition numbers will be for the
*>          matrices after permuting and/or balancing. Permuting does
*>          not change condition numbers (in exact arithmetic), but
*>          balancing does.
*> \endverbatim
*>
*> \param[in] JOBVL
*> \verbatim
*>          JOBVL is CHARACTER*1
*>          = 'N':  do not compute the left generalized eigenvectors;
*>          = 'V':  compute the left generalized eigenvectors.
*> \endverbatim
*>
*> \param[in] JOBVR
*> \verbatim
*>          JOBVR is CHARACTER*1
*>          = 'N':  do not compute the right generalized eigenvectors;
*>          = 'V':  compute the right generalized eigenvectors.
*> \endverbatim
*>
*> \param[in] SENSE
*> \verbatim
*>          SENSE is CHARACTER*1
*>          Determines which reciprocal condition numbers are computed.
*>          = 'N': none are computed;
*>          = 'E': computed for eigenvalues only;
*>          = 'V': computed for eigenvectors only;
*>          = 'B': computed for eigenvalues and eigenvectors.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A, B, VL, and VR.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          On entry, the matrix A in the pair (A,B).
*>          On exit, A has been overwritten. If JOBVL='V' or JOBVR='V'
*>          or both, then A contains the first part of the complex Schur
*>          form of the "balanced" versions of the input A and B.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N)
*>          On entry, the matrix B in the pair (A,B).
*>          On exit, B has been overwritten. If JOBVL='V' or JOBVR='V'
*>          or both, then B contains the second part of the complex
*>          Schur form of the "balanced" versions of the input A and B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX*16 array, dimension (N)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is COMPLEX*16 array, dimension (N)
*>          On exit, ALPHA(j)/BETA(j), j=1,...,N, will be the generalized
*>          eigenvalues.
*>
*>          Note: the quotient ALPHA(j)/BETA(j) ) may easily over- or
*>          underflow, and BETA(j) may even be zero.  Thus, the user
*>          should avoid naively computing the ratio ALPHA/BETA.
*>          However, ALPHA will be always less than and usually
*>          comparable with norm(A) in magnitude, and BETA always less
*>          than and usually comparable with norm(B).
*> \endverbatim
*>
*> \param[out] VL
*> \verbatim
*>          VL is COMPLEX*16 array, dimension (LDVL,N)
*>          If JOBVL = 'V', the left generalized eigenvectors u(j) are
*>          stored one after another in the columns of VL, in the same
*>          order as their eigenvalues.
*>          Each eigenvector will be scaled so the largest component
*>          will have abs(real part) + abs(imag. part) = 1.
*>          Not referenced if JOBVL = 'N'.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the matrix VL. LDVL >= 1, and
*>          if JOBVL = 'V', LDVL >= N.
*> \endverbatim
*>
*> \param[out] VR
*> \verbatim
*>          VR is COMPLEX*16 array, dimension (LDVR,N)
*>          If JOBVR = 'V', the right generalized eigenvectors v(j) are
*>          stored one after another in the columns of VR, in the same
*>          order as their eigenvalues.
*>          Each eigenvector will be scaled so the largest component
*>          will have abs(real part) + abs(imag. part) = 1.
*>          Not referenced if JOBVR = 'N'.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the matrix VR. LDVR >= 1, and
*>          if JOBVR = 'V', LDVR >= N.
*> \endverbatim
*>
*> \param[out] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[out] IHI
*> \verbatim
*>          IHI is INTEGER
*>          ILO and IHI are integer values such that on exit
*>          A(i,j) = 0 and B(i,j) = 0 if i > j and
*>          j = 1,...,ILO-1 or i = IHI+1,...,N.
*>          If BALANC = 'N' or 'S', ILO = 1 and IHI = N.
*> \endverbatim
*>
*> \param[out] LSCALE
*> \verbatim
*>          LSCALE is DOUBLE PRECISION array, dimension (N)
*>          Details of the permutations and scaling factors applied
*>          to the left side of A and B.  If PL(j) is the index of the
*>          row interchanged with row j, and DL(j) is the scaling
*>          factor applied to row j, then
*>            LSCALE(j) = PL(j)  for j = 1,...,ILO-1
*>                      = DL(j)  for j = ILO,...,IHI
*>                      = PL(j)  for j = IHI+1,...,N.
*>          The order in which the interchanges are made is N to IHI+1,
*>          then 1 to ILO-1.
*> \endverbatim
*>
*> \param[out] RSCALE
*> \verbatim
*>          RSCALE is DOUBLE PRECISION array, dimension (N)
*>          Details of the permutations and scaling factors applied
*>          to the right side of A and B.  If PR(j) is the index of the
*>          column interchanged with column j, and DR(j) is the scaling
*>          factor applied to column j, then
*>            RSCALE(j) = PR(j)  for j = 1,...,ILO-1
*>                      = DR(j)  for j = ILO,...,IHI
*>                      = PR(j)  for j = IHI+1,...,N
*>          The order in which the interchanges are made is N to IHI+1,
*>          then 1 to ILO-1.
*> \endverbatim
*>
*> \param[out] ABNRM
*> \verbatim
*>          ABNRM is DOUBLE PRECISION
*>          The one-norm of the balanced matrix A.
*> \endverbatim
*>
*> \param[out] BBNRM
*> \verbatim
*>          BBNRM is DOUBLE PRECISION
*>          The one-norm of the balanced matrix B.
*> \endverbatim
*>
*> \param[out] RCONDE
*> \verbatim
*>          RCONDE is DOUBLE PRECISION array, dimension (N)
*>          If SENSE = 'E' or 'B', the reciprocal condition numbers of
*>          the eigenvalues, stored in consecutive elements of the array.
*>          If SENSE = 'N' or 'V', RCONDE is not referenced.
*> \endverbatim
*>
*> \param[out] RCONDV
*> \verbatim
*>          RCONDV is DOUBLE PRECISION array, dimension (N)
*>          If JOB = 'V' or 'B', the estimated reciprocal condition
*>          numbers of the eigenvectors, stored in consecutive elements
*>          of the array. If the eigenvalues cannot be reordered to
*>          compute RCONDV(j), RCONDV(j) is set to 0; this can only occur
*>          when the true value would be very small anyway.
*>          If SENSE = 'N' or 'E', RCONDV is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= max(1,2*N).
*>          If SENSE = 'E', LWORK >= max(1,4*N).
*>          If SENSE = 'V' or 'B', LWORK >= max(1,2*N*N+2*N).
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (lrwork)
*>          lrwork must be at least max(1,6*N) if BALANC = 'S' or 'B',
*>          and at least max(1,2*N) otherwise.
*>          Real workspace.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N+2)
*>          If SENSE = 'E', IWORK is not referenced.
*> \endverbatim
*>
*> \param[out] BWORK
*> \verbatim
*>          BWORK is LOGICAL array, dimension (N)
*>          If SENSE = 'N', BWORK is not referenced.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          = 1,...,N:
*>                The QZ iteration failed.  No eigenvectors have been
*>                calculated, but ALPHA(j) and BETA(j) should be correct
*>                for j=INFO+1,...,N.
*>          > N:  =N+1: other than QZ iteration failed in ZHGEQZ.
*>                =N+2: error return from ZTGEVC.
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
*> \date April 2012
*
*> \ingroup complex16GEeigen
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Balancing a matrix pair (A,B) includes, first, permuting rows and
*>  columns to isolate eigenvalues, second, applying diagonal similarity
*>  transformation to the rows and columns to make the rows and columns
*>  as close in norm as possible. The computed reciprocal condition
*>  numbers correspond to the balanced matrix. Permuting rows and columns
*>  will not change the condition numbers (in exact arithmetic) but
*>  diagonal scaling will.  For further explanation of balancing, see
*>  section 4.11.1.2 of LAPACK Users' Guide.
*>
*>  An approximate error bound on the chordal distance between the i-th
*>  computed generalized eigenvalue w and the corresponding exact
*>  eigenvalue lambda is
*>
*>       chord(w, lambda) <= EPS * norm(ABNRM, BBNRM) / RCONDE(I)
*>
*>  An approximate error bound for the angle between the i-th computed
*>  eigenvector VL(i) or VR(i) is given by
*>
*>       EPS * norm(ABNRM, BBNRM) / DIF(i).
*>
*>  For further explanation of the reciprocal condition numbers RCONDE
*>  and RCONDV, see section 4.11 of LAPACK User's Guide.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZGGEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, B, LDB,
     $                   ALPHA, BETA, VL, LDVL, VR, LDVR, ILO, IHI,
     $                   LSCALE, RSCALE, ABNRM, BBNRM, RCONDE, RCONDV,
     $                   WORK, LWORK, RWORK, IWORK, BWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          BALANC, JOBVL, JOBVR, SENSE
      INTEGER            IHI, ILO, INFO, LDA, LDB, LDVL, LDVR, LWORK, N
      DOUBLE PRECISION   ABNRM, BBNRM
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      INTEGER            IWORK( * )
      DOUBLE PRECISION   LSCALE( * ), RCONDE( * ), RCONDV( * ),
     $                   RSCALE( * ), RWORK( * )
      COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ),
     $                   BETA( * ), VL( LDVL, * ), VR( LDVR, * ),
     $                   WORK( * )
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
*     ..
*     .. Local Scalars ..
      LOGICAL            ILASCL, ILBSCL, ILV, ILVL, ILVR, LQUERY, NOSCL,
     $                   WANTSB, WANTSE, WANTSN, WANTSV
      CHARACTER          CHTEMP
      INTEGER            I, ICOLS, IERR, IJOBVL, IJOBVR, IN, IROWS,
     $                   ITAU, IWRK, IWRK1, J, JC, JR, M, MAXWRK, MINWRK
      DOUBLE PRECISION   ANRM, ANRMTO, BIGNUM, BNRM, BNRMTO, EPS,
     $                   SMLNUM, TEMP
      COMPLEX*16         X
*     ..
*     .. Local Arrays ..
      LOGICAL            LDUMMA( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, DLASCL, XERBLA, ZGEQRF, ZGGBAK, ZGGBAL,
     $                   ZGGHRD, ZHGEQZ, ZLACPY, ZLASCL, ZLASET, ZTGEVC,
     $                   ZTGSNA, ZUNGQR, ZUNMQR
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           LSAME, ILAENV, DLAMCH, ZLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DIMAG, MAX, SQRT
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   ABS1
*     ..
*     .. Statement Function definitions ..
      ABS1( X ) = ABS( DBLE( X ) ) + ABS( DIMAG( X ) )
*     ..
*     .. Executable Statements ..
*
*     Decode the input arguments
*
      IF( LSAME( JOBVL, 'N' ) ) THEN
         IJOBVL = 1
         ILVL = .FALSE.
      ELSE IF( LSAME( JOBVL, 'V' ) ) THEN
         IJOBVL = 2
         ILVL = .TRUE.
      ELSE
         IJOBVL = -1
         ILVL = .FALSE.
      END IF
*
      IF( LSAME( JOBVR, 'N' ) ) THEN
         IJOBVR = 1
         ILVR = .FALSE.
      ELSE IF( LSAME( JOBVR, 'V' ) ) THEN
         IJOBVR = 2
         ILVR = .TRUE.
      ELSE
         IJOBVR = -1
         ILVR = .FALSE.
      END IF
      ILV = ILVL .OR. ILVR
*
      NOSCL  = LSAME( BALANC, 'N' ) .OR. LSAME( BALANC, 'P' )
      WANTSN = LSAME( SENSE, 'N' )
      WANTSE = LSAME( SENSE, 'E' )
      WANTSV = LSAME( SENSE, 'V' )
      WANTSB = LSAME( SENSE, 'B' )
*
*     Test the input arguments
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
      IF( .NOT.( NOSCL .OR. LSAME( BALANC,'S' ) .OR.
     $    LSAME( BALANC, 'B' ) ) ) THEN
         INFO = -1
      ELSE IF( IJOBVL.LE.0 ) THEN
         INFO = -2
      ELSE IF( IJOBVR.LE.0 ) THEN
         INFO = -3
      ELSE IF( .NOT.( WANTSN .OR. WANTSE .OR. WANTSB .OR. WANTSV ) )
     $          THEN
         INFO = -4
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LDVL.LT.1 .OR. ( ILVL .AND. LDVL.LT.N ) ) THEN
         INFO = -13
      ELSE IF( LDVR.LT.1 .OR. ( ILVR .AND. LDVR.LT.N ) ) THEN
         INFO = -15
      END IF
*
*     Compute workspace
*      (Note: Comments in the code beginning "Workspace:" describe the
*       minimal amount of workspace needed at that point in the code,
*       as well as the preferred amount for good performance.
*       NB refers to the optimal block size for the immediately
*       following subroutine, as returned by ILAENV. The workspace is
*       computed assuming ILO = 1 and IHI = N, the worst case.)
*
      IF( INFO.EQ.0 ) THEN
         IF( N.EQ.0 ) THEN
            MINWRK = 1
            MAXWRK = 1
         ELSE
            MINWRK = 2*N
            IF( WANTSE ) THEN
               MINWRK = 4*N
            ELSE IF( WANTSV .OR. WANTSB ) THEN
               MINWRK = 2*N*( N + 1)
            END IF
            MAXWRK = MINWRK
            MAXWRK = MAX( MAXWRK,
     $                    N + N*ILAENV( 1, 'ZGEQRF', ' ', N, 1, N, 0 ) )
            MAXWRK = MAX( MAXWRK,
     $                    N + N*ILAENV( 1, 'ZUNMQR', ' ', N, 1, N, 0 ) )
            IF( ILVL ) THEN
               MAXWRK = MAX( MAXWRK, N +
     $                       N*ILAENV( 1, 'ZUNGQR', ' ', N, 1, N, 0 ) )
            END IF
         END IF
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -25
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGGEVX', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Get machine constants
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
      SMLNUM = SQRT( SMLNUM ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = ZLANGE( 'M', N, N, A, LDA, RWORK )
      ILASCL = .FALSE.
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         ANRMTO = SMLNUM
         ILASCL = .TRUE.
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         ANRMTO = BIGNUM
         ILASCL = .TRUE.
      END IF
      IF( ILASCL )
     $   CALL ZLASCL( 'G', 0, 0, ANRM, ANRMTO, N, N, A, LDA, IERR )
*
*     Scale B if max element outside range [SMLNUM,BIGNUM]
*
      BNRM = ZLANGE( 'M', N, N, B, LDB, RWORK )
      ILBSCL = .FALSE.
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
         BNRMTO = SMLNUM
         ILBSCL = .TRUE.
      ELSE IF( BNRM.GT.BIGNUM ) THEN
         BNRMTO = BIGNUM
         ILBSCL = .TRUE.
      END IF
      IF( ILBSCL )
     $   CALL ZLASCL( 'G', 0, 0, BNRM, BNRMTO, N, N, B, LDB, IERR )
*
*     Permute and/or balance the matrix pair (A,B)
*     (Real Workspace: need 6*N if BALANC = 'S' or 'B', 1 otherwise)
*
      CALL ZGGBAL( BALANC, N, A, LDA, B, LDB, ILO, IHI, LSCALE, RSCALE,
     $             RWORK, IERR )
*
*     Compute ABNRM and BBNRM
*
      ABNRM = ZLANGE( '1', N, N, A, LDA, RWORK( 1 ) )
      IF( ILASCL ) THEN
         RWORK( 1 ) = ABNRM
         CALL DLASCL( 'G', 0, 0, ANRMTO, ANRM, 1, 1, RWORK( 1 ), 1,
     $                IERR )
         ABNRM = RWORK( 1 )
      END IF
*
      BBNRM = ZLANGE( '1', N, N, B, LDB, RWORK( 1 ) )
      IF( ILBSCL ) THEN
         RWORK( 1 ) = BBNRM
         CALL DLASCL( 'G', 0, 0, BNRMTO, BNRM, 1, 1, RWORK( 1 ), 1,
     $                IERR )
         BBNRM = RWORK( 1 )
      END IF
*
*     Reduce B to triangular form (QR decomposition of B)
*     (Complex Workspace: need N, prefer N*NB )
*
      IROWS = IHI + 1 - ILO
      IF( ILV .OR. .NOT.WANTSN ) THEN
         ICOLS = N + 1 - ILO
      ELSE
         ICOLS = IROWS
      END IF
      ITAU = 1
      IWRK = ITAU + IROWS
      CALL ZGEQRF( IROWS, ICOLS, B( ILO, ILO ), LDB, WORK( ITAU ),
     $             WORK( IWRK ), LWORK+1-IWRK, IERR )
*
*     Apply the unitary transformation to A
*     (Complex Workspace: need N, prefer N*NB)
*
      CALL ZUNMQR( 'L', 'C', IROWS, ICOLS, IROWS, B( ILO, ILO ), LDB,
     $             WORK( ITAU ), A( ILO, ILO ), LDA, WORK( IWRK ),
     $             LWORK+1-IWRK, IERR )
*
*     Initialize VL and/or VR
*     (Workspace: need N, prefer N*NB)
*
      IF( ILVL ) THEN
         CALL ZLASET( 'Full', N, N, CZERO, CONE, VL, LDVL )
         IF( IROWS.GT.1 ) THEN
            CALL ZLACPY( 'L', IROWS-1, IROWS-1, B( ILO+1, ILO ), LDB,
     $                   VL( ILO+1, ILO ), LDVL )
         END IF
         CALL ZUNGQR( IROWS, IROWS, IROWS, VL( ILO, ILO ), LDVL,
     $                WORK( ITAU ), WORK( IWRK ), LWORK+1-IWRK, IERR )
      END IF
*
      IF( ILVR )
     $   CALL ZLASET( 'Full', N, N, CZERO, CONE, VR, LDVR )
*
*     Reduce to generalized Hessenberg form
*     (Workspace: none needed)
*
      IF( ILV .OR. .NOT.WANTSN ) THEN
*
*        Eigenvectors requested -- work on whole matrix.
*
         CALL ZGGHRD( JOBVL, JOBVR, N, ILO, IHI, A, LDA, B, LDB, VL,
     $                LDVL, VR, LDVR, IERR )
      ELSE
         CALL ZGGHRD( 'N', 'N', IROWS, 1, IROWS, A( ILO, ILO ), LDA,
     $                B( ILO, ILO ), LDB, VL, LDVL, VR, LDVR, IERR )
      END IF
*
*     Perform QZ algorithm (Compute eigenvalues, and optionally, the
*     Schur forms and Schur vectors)
*     (Complex Workspace: need N)
*     (Real Workspace: need N)
*
      IWRK = ITAU
      IF( ILV .OR. .NOT.WANTSN ) THEN
         CHTEMP = 'S'
      ELSE
         CHTEMP = 'E'
      END IF
*
      CALL ZHGEQZ( CHTEMP, JOBVL, JOBVR, N, ILO, IHI, A, LDA, B, LDB,
     $             ALPHA, BETA, VL, LDVL, VR, LDVR, WORK( IWRK ),
     $             LWORK+1-IWRK, RWORK, IERR )
      IF( IERR.NE.0 ) THEN
         IF( IERR.GT.0 .AND. IERR.LE.N ) THEN
            INFO = IERR
         ELSE IF( IERR.GT.N .AND. IERR.LE.2*N ) THEN
            INFO = IERR - N
         ELSE
            INFO = N + 1
         END IF
         GO TO 90
      END IF
*
*     Compute Eigenvectors and estimate condition numbers if desired
*     ZTGEVC: (Complex Workspace: need 2*N )
*             (Real Workspace:    need 2*N )
*     ZTGSNA: (Complex Workspace: need 2*N*N if SENSE='V' or 'B')
*             (Integer Workspace: need N+2 )
*
      IF( ILV .OR. .NOT.WANTSN ) THEN
         IF( ILV ) THEN
            IF( ILVL ) THEN
               IF( ILVR ) THEN
                  CHTEMP = 'B'
               ELSE
                  CHTEMP = 'L'
               END IF
            ELSE
               CHTEMP = 'R'
            END IF
*
            CALL ZTGEVC( CHTEMP, 'B', LDUMMA, N, A, LDA, B, LDB, VL,
     $                   LDVL, VR, LDVR, N, IN, WORK( IWRK ), RWORK,
     $                   IERR )
            IF( IERR.NE.0 ) THEN
               INFO = N + 2
               GO TO 90
            END IF
         END IF
*
         IF( .NOT.WANTSN ) THEN
*
*           compute eigenvectors (DTGEVC) and estimate condition
*           numbers (DTGSNA). Note that the definition of the condition
*           number is not invariant under transformation (u,v) to
*           (Q*u, Z*v), where (u,v) are eigenvectors of the generalized
*           Schur form (S,T), Q and Z are orthogonal matrices. In order
*           to avoid using extra 2*N*N workspace, we have to
*           re-calculate eigenvectors and estimate the condition numbers
*           one at a time.
*
            DO 20 I = 1, N
*
               DO 10 J = 1, N
                  BWORK( J ) = .FALSE.
   10          CONTINUE
               BWORK( I ) = .TRUE.
*
               IWRK = N + 1
               IWRK1 = IWRK + N
*
               IF( WANTSE .OR. WANTSB ) THEN
                  CALL ZTGEVC( 'B', 'S', BWORK, N, A, LDA, B, LDB,
     $                         WORK( 1 ), N, WORK( IWRK ), N, 1, M,
     $                         WORK( IWRK1 ), RWORK, IERR )
                  IF( IERR.NE.0 ) THEN
                     INFO = N + 2
                     GO TO 90
                  END IF
               END IF
*
               CALL ZTGSNA( SENSE, 'S', BWORK, N, A, LDA, B, LDB,
     $                      WORK( 1 ), N, WORK( IWRK ), N, RCONDE( I ),
     $                      RCONDV( I ), 1, M, WORK( IWRK1 ),
     $                      LWORK-IWRK1+1, IWORK, IERR )
*
   20       CONTINUE
         END IF
      END IF
*
*     Undo balancing on VL and VR and normalization
*     (Workspace: none needed)
*
      IF( ILVL ) THEN
         CALL ZGGBAK( BALANC, 'L', N, ILO, IHI, LSCALE, RSCALE, N, VL,
     $                LDVL, IERR )
*
         DO 50 JC = 1, N
            TEMP = ZERO
            DO 30 JR = 1, N
               TEMP = MAX( TEMP, ABS1( VL( JR, JC ) ) )
   30       CONTINUE
            IF( TEMP.LT.SMLNUM )
     $         GO TO 50
            TEMP = ONE / TEMP
            DO 40 JR = 1, N
               VL( JR, JC ) = VL( JR, JC )*TEMP
   40       CONTINUE
   50    CONTINUE
      END IF
*
      IF( ILVR ) THEN
         CALL ZGGBAK( BALANC, 'R', N, ILO, IHI, LSCALE, RSCALE, N, VR,
     $                LDVR, IERR )
         DO 80 JC = 1, N
            TEMP = ZERO
            DO 60 JR = 1, N
               TEMP = MAX( TEMP, ABS1( VR( JR, JC ) ) )
   60       CONTINUE
            IF( TEMP.LT.SMLNUM )
     $         GO TO 80
            TEMP = ONE / TEMP
            DO 70 JR = 1, N
               VR( JR, JC ) = VR( JR, JC )*TEMP
   70       CONTINUE
   80    CONTINUE
      END IF
*
*     Undo scaling if necessary
*
   90 CONTINUE
*
      IF( ILASCL )
     $   CALL ZLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHA, N, IERR )
*
      IF( ILBSCL )
     $   CALL ZLASCL( 'G', 0, 0, BNRMTO, BNRM, N, 1, BETA, N, IERR )
*
      WORK( 1 ) = MAXWRK
      RETURN
*
*     End of ZGGEVX
*
      END
