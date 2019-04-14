*> \brief <b> SGGEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGGEVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sggevx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sggevx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sggevx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGGEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, B, LDB,
*                          ALPHAR, ALPHAI, BETA, VL, LDVL, VR, LDVR, ILO,
*                          IHI, LSCALE, RSCALE, ABNRM, BBNRM, RCONDE,
*                          RCONDV, WORK, LWORK, IWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          BALANC, JOBVL, JOBVR, SENSE
*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDVL, LDVR, LWORK, N
*       REAL               ABNRM, BBNRM
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       INTEGER            IWORK( * )
*       REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
*      $                   B( LDB, * ), BETA( * ), LSCALE( * ),
*      $                   RCONDE( * ), RCONDV( * ), RSCALE( * ),
*      $                   VL( LDVL, * ), VR( LDVR, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGGEVX computes for a pair of N-by-N real nonsymmetric matrices (A,B)
*> the generalized eigenvalues, and optionally, the left and/or right
*> generalized eigenvectors.
*>
*> Optionally also, it computes a balancing transformation to improve
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
*>
*>                  A * v(j) = lambda(j) * B * v(j) .
*>
*> The left eigenvector u(j) corresponding to the eigenvalue lambda(j)
*> of (A,B) satisfies
*>
*>                  u(j)**H * A  = lambda(j) * u(j)**H * B.
*>
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
*>          Specifies the balance option to be performed.
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
*>          A is REAL array, dimension (LDA, N)
*>          On entry, the matrix A in the pair (A,B).
*>          On exit, A has been overwritten. If JOBVL='V' or JOBVR='V'
*>          or both, then A contains the first part of the real Schur
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
*>          B is REAL array, dimension (LDB, N)
*>          On entry, the matrix B in the pair (A,B).
*>          On exit, B has been overwritten. If JOBVL='V' or JOBVR='V'
*>          or both, then B contains the second part of the real Schur
*>          form of the "balanced" versions of the input A and B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (N)
*>          On exit, (ALPHAR(j) + ALPHAI(j)*i)/BETA(j), j=1,...,N, will
*>          be the generalized eigenvalues.  If ALPHAI(j) is zero, then
*>          the j-th eigenvalue is real; if positive, then the j-th and
*>          (j+1)-st eigenvalues are a complex conjugate pair, with
*>          ALPHAI(j+1) negative.
*>
*>          Note: the quotients ALPHAR(j)/BETA(j) and ALPHAI(j)/BETA(j)
*>          may easily over- or underflow, and BETA(j) may even be zero.
*>          Thus, the user should avoid naively computing the ratio
*>          ALPHA/BETA. However, ALPHAR and ALPHAI will be always less
*>          than and usually comparable with norm(A) in magnitude, and
*>          BETA always less than and usually comparable with norm(B).
*> \endverbatim
*>
*> \param[out] VL
*> \verbatim
*>          VL is REAL array, dimension (LDVL,N)
*>          If JOBVL = 'V', the left eigenvectors u(j) are stored one
*>          after another in the columns of VL, in the same order as
*>          their eigenvalues. If the j-th eigenvalue is real, then
*>          u(j) = VL(:,j), the j-th column of VL. If the j-th and
*>          (j+1)-th eigenvalues form a complex conjugate pair, then
*>          u(j) = VL(:,j)+i*VL(:,j+1) and u(j+1) = VL(:,j)-i*VL(:,j+1).
*>          Each eigenvector will be scaled so the largest component have
*>          abs(real part) + abs(imag. part) = 1.
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
*>          VR is REAL array, dimension (LDVR,N)
*>          If JOBVR = 'V', the right eigenvectors v(j) are stored one
*>          after another in the columns of VR, in the same order as
*>          their eigenvalues. If the j-th eigenvalue is real, then
*>          v(j) = VR(:,j), the j-th column of VR. If the j-th and
*>          (j+1)-th eigenvalues form a complex conjugate pair, then
*>          v(j) = VR(:,j)+i*VR(:,j+1) and v(j+1) = VR(:,j)-i*VR(:,j+1).
*>          Each eigenvector will be scaled so the largest component have
*>          abs(real part) + abs(imag. part) = 1.
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
*>          LSCALE is REAL array, dimension (N)
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
*>          RSCALE is REAL array, dimension (N)
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
*>          ABNRM is REAL
*>          The one-norm of the balanced matrix A.
*> \endverbatim
*>
*> \param[out] BBNRM
*> \verbatim
*>          BBNRM is REAL
*>          The one-norm of the balanced matrix B.
*> \endverbatim
*>
*> \param[out] RCONDE
*> \verbatim
*>          RCONDE is REAL array, dimension (N)
*>          If SENSE = 'E' or 'B', the reciprocal condition numbers of
*>          the eigenvalues, stored in consecutive elements of the array.
*>          For a complex conjugate pair of eigenvalues two consecutive
*>          elements of RCONDE are set to the same value. Thus RCONDE(j),
*>          RCONDV(j), and the j-th columns of VL and VR all correspond
*>          to the j-th eigenpair.
*>          If SENSE = 'N' or 'V', RCONDE is not referenced.
*> \endverbatim
*>
*> \param[out] RCONDV
*> \verbatim
*>          RCONDV is REAL array, dimension (N)
*>          If SENSE = 'V' or 'B', the estimated reciprocal condition
*>          numbers of the eigenvectors, stored in consecutive elements
*>          of the array. For a complex eigenvector two consecutive
*>          elements of RCONDV are set to the same value. If the
*>          eigenvalues cannot be reordered to compute RCONDV(j),
*>          RCONDV(j) is set to 0; this can only occur when the true
*>          value would be very small anyway.
*>          If SENSE = 'N' or 'E', RCONDV is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK. LWORK >= max(1,2*N).
*>          If BALANC = 'S' or 'B', or JOBVL = 'V', or JOBVR = 'V',
*>          LWORK >= max(1,6*N).
*>          If SENSE = 'E', LWORK >= max(1,10*N).
*>          If SENSE = 'V' or 'B', LWORK >= 2*N*N+8*N+16.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N+6)
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
*>                calculated, but ALPHAR(j), ALPHAI(j), and BETA(j)
*>                should be correct for j=INFO+1,...,N.
*>          > N:  =N+1: other than QZ iteration failed in SHGEQZ.
*>                =N+2: error return from STGEVC.
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
*> \ingroup realGEeigen
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
      SUBROUTINE SGGEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, B, LDB,
     $                   ALPHAR, ALPHAI, BETA, VL, LDVL, VR, LDVR, ILO,
     $                   IHI, LSCALE, RSCALE, ABNRM, BBNRM, RCONDE,
     $                   RCONDV, WORK, LWORK, IWORK, BWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          BALANC, JOBVL, JOBVR, SENSE
      INTEGER            IHI, ILO, INFO, LDA, LDB, LDVL, LDVR, LWORK, N
      REAL               ABNRM, BBNRM
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      INTEGER            IWORK( * )
      REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
     $                   B( LDB, * ), BETA( * ), LSCALE( * ),
     $                   RCONDE( * ), RCONDV( * ), RSCALE( * ),
     $                   VL( LDVL, * ), VR( LDVR, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ILASCL, ILBSCL, ILV, ILVL, ILVR, LQUERY, NOSCL,
     $                   PAIR, WANTSB, WANTSE, WANTSN, WANTSV
      CHARACTER          CHTEMP
      INTEGER            I, ICOLS, IERR, IJOBVL, IJOBVR, IN, IROWS,
     $                   ITAU, IWRK, IWRK1, J, JC, JR, M, MAXWRK,
     $                   MINWRK, MM
      REAL               ANRM, ANRMTO, BIGNUM, BNRM, BNRMTO, EPS,
     $                   SMLNUM, TEMP
*     ..
*     .. Local Arrays ..
      LOGICAL            LDUMMA( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEQRF, SGGBAK, SGGBAL, SGGHRD, SHGEQZ, SLABAD,
     $                   SLACPY, SLASCL, SLASET, SORGQR, SORMQR, STGEVC,
     $                   STGSNA, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           LSAME, ILAENV, SLAMCH, SLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
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
      IF( .NOT.( NOSCL .OR. LSAME( BALANC, 'S' ) .OR.
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
         INFO = -14
      ELSE IF( LDVR.LT.1 .OR. ( ILVR .AND. LDVR.LT.N ) ) THEN
         INFO = -16
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
            IF( NOSCL .AND. .NOT.ILV ) THEN
               MINWRK = 2*N
            ELSE
               MINWRK = 6*N
            END IF
            IF( WANTSE ) THEN
               MINWRK = 10*N
            ELSE IF( WANTSV .OR. WANTSB ) THEN
               MINWRK = 2*N*( N + 4 ) + 16
            END IF
            MAXWRK = MINWRK
            MAXWRK = MAX( MAXWRK,
     $                    N + N*ILAENV( 1, 'SGEQRF', ' ', N, 1, N, 0 ) )
            MAXWRK = MAX( MAXWRK,
     $                    N + N*ILAENV( 1, 'SORMQR', ' ', N, 1, N, 0 ) )
            IF( ILVL ) THEN
               MAXWRK = MAX( MAXWRK, N +
     $                       N*ILAENV( 1, 'SORGQR', ' ', N, 1, N, 0 ) )
            END IF
         END IF
         WORK( 1 ) = MAXWRK
*
         IF( LWORK.LT.MINWRK .AND. .NOT.LQUERY ) THEN
            INFO = -26
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGGEVX', -INFO )
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
*
*     Get machine constants
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' )
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
      SMLNUM = SQRT( SMLNUM ) / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = SLANGE( 'M', N, N, A, LDA, WORK )
      ILASCL = .FALSE.
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         ANRMTO = SMLNUM
         ILASCL = .TRUE.
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         ANRMTO = BIGNUM
         ILASCL = .TRUE.
      END IF
      IF( ILASCL )
     $   CALL SLASCL( 'G', 0, 0, ANRM, ANRMTO, N, N, A, LDA, IERR )
*
*     Scale B if max element outside range [SMLNUM,BIGNUM]
*
      BNRM = SLANGE( 'M', N, N, B, LDB, WORK )
      ILBSCL = .FALSE.
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
         BNRMTO = SMLNUM
         ILBSCL = .TRUE.
      ELSE IF( BNRM.GT.BIGNUM ) THEN
         BNRMTO = BIGNUM
         ILBSCL = .TRUE.
      END IF
      IF( ILBSCL )
     $   CALL SLASCL( 'G', 0, 0, BNRM, BNRMTO, N, N, B, LDB, IERR )
*
*     Permute and/or balance the matrix pair (A,B)
*     (Workspace: need 6*N if BALANC = 'S' or 'B', 1 otherwise)
*
      CALL SGGBAL( BALANC, N, A, LDA, B, LDB, ILO, IHI, LSCALE, RSCALE,
     $             WORK, IERR )
*
*     Compute ABNRM and BBNRM
*
      ABNRM = SLANGE( '1', N, N, A, LDA, WORK( 1 ) )
      IF( ILASCL ) THEN
         WORK( 1 ) = ABNRM
         CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, 1, 1, WORK( 1 ), 1,
     $                IERR )
         ABNRM = WORK( 1 )
      END IF
*
      BBNRM = SLANGE( '1', N, N, B, LDB, WORK( 1 ) )
      IF( ILBSCL ) THEN
         WORK( 1 ) = BBNRM
         CALL SLASCL( 'G', 0, 0, BNRMTO, BNRM, 1, 1, WORK( 1 ), 1,
     $                IERR )
         BBNRM = WORK( 1 )
      END IF
*
*     Reduce B to triangular form (QR decomposition of B)
*     (Workspace: need N, prefer N*NB )
*
      IROWS = IHI + 1 - ILO
      IF( ILV .OR. .NOT.WANTSN ) THEN
         ICOLS = N + 1 - ILO
      ELSE
         ICOLS = IROWS
      END IF
      ITAU = 1
      IWRK = ITAU + IROWS
      CALL SGEQRF( IROWS, ICOLS, B( ILO, ILO ), LDB, WORK( ITAU ),
     $             WORK( IWRK ), LWORK+1-IWRK, IERR )
*
*     Apply the orthogonal transformation to A
*     (Workspace: need N, prefer N*NB)
*
      CALL SORMQR( 'L', 'T', IROWS, ICOLS, IROWS, B( ILO, ILO ), LDB,
     $             WORK( ITAU ), A( ILO, ILO ), LDA, WORK( IWRK ),
     $             LWORK+1-IWRK, IERR )
*
*     Initialize VL and/or VR
*     (Workspace: need N, prefer N*NB)
*
      IF( ILVL ) THEN
         CALL SLASET( 'Full', N, N, ZERO, ONE, VL, LDVL )
         IF( IROWS.GT.1 ) THEN
            CALL SLACPY( 'L', IROWS-1, IROWS-1, B( ILO+1, ILO ), LDB,
     $                   VL( ILO+1, ILO ), LDVL )
         END IF
         CALL SORGQR( IROWS, IROWS, IROWS, VL( ILO, ILO ), LDVL,
     $                WORK( ITAU ), WORK( IWRK ), LWORK+1-IWRK, IERR )
      END IF
*
      IF( ILVR )
     $   CALL SLASET( 'Full', N, N, ZERO, ONE, VR, LDVR )
*
*     Reduce to generalized Hessenberg form
*     (Workspace: none needed)
*
      IF( ILV .OR. .NOT.WANTSN ) THEN
*
*        Eigenvectors requested -- work on whole matrix.
*
         CALL SGGHRD( JOBVL, JOBVR, N, ILO, IHI, A, LDA, B, LDB, VL,
     $                LDVL, VR, LDVR, IERR )
      ELSE
         CALL SGGHRD( 'N', 'N', IROWS, 1, IROWS, A( ILO, ILO ), LDA,
     $                B( ILO, ILO ), LDB, VL, LDVL, VR, LDVR, IERR )
      END IF
*
*     Perform QZ algorithm (Compute eigenvalues, and optionally, the
*     Schur forms and Schur vectors)
*     (Workspace: need N)
*
      IF( ILV .OR. .NOT.WANTSN ) THEN
         CHTEMP = 'S'
      ELSE
         CHTEMP = 'E'
      END IF
*
      CALL SHGEQZ( CHTEMP, JOBVL, JOBVR, N, ILO, IHI, A, LDA, B, LDB,
     $             ALPHAR, ALPHAI, BETA, VL, LDVL, VR, LDVR, WORK,
     $             LWORK, IERR )
      IF( IERR.NE.0 ) THEN
         IF( IERR.GT.0 .AND. IERR.LE.N ) THEN
            INFO = IERR
         ELSE IF( IERR.GT.N .AND. IERR.LE.2*N ) THEN
            INFO = IERR - N
         ELSE
            INFO = N + 1
         END IF
         GO TO 130
      END IF
*
*     Compute Eigenvectors and estimate condition numbers if desired
*     (Workspace: STGEVC: need 6*N
*                 STGSNA: need 2*N*(N+2)+16 if SENSE = 'V' or 'B',
*                         need N otherwise )
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
            CALL STGEVC( CHTEMP, 'B', LDUMMA, N, A, LDA, B, LDB, VL,
     $                   LDVL, VR, LDVR, N, IN, WORK, IERR )
            IF( IERR.NE.0 ) THEN
               INFO = N + 2
               GO TO 130
            END IF
         END IF
*
         IF( .NOT.WANTSN ) THEN
*
*           compute eigenvectors (STGEVC) and estimate condition
*           numbers (STGSNA). Note that the definition of the condition
*           number is not invariant under transformation (u,v) to
*           (Q*u, Z*v), where (u,v) are eigenvectors of the generalized
*           Schur form (S,T), Q and Z are orthogonal matrices. In order
*           to avoid using extra 2*N*N workspace, we have to recalculate
*           eigenvectors and estimate one condition numbers at a time.
*
            PAIR = .FALSE.
            DO 20 I = 1, N
*
               IF( PAIR ) THEN
                  PAIR = .FALSE.
                  GO TO 20
               END IF
               MM = 1
               IF( I.LT.N ) THEN
                  IF( A( I+1, I ).NE.ZERO ) THEN
                     PAIR = .TRUE.
                     MM = 2
                  END IF
               END IF
*
               DO 10 J = 1, N
                  BWORK( J ) = .FALSE.
   10          CONTINUE
               IF( MM.EQ.1 ) THEN
                  BWORK( I ) = .TRUE.
               ELSE IF( MM.EQ.2 ) THEN
                  BWORK( I ) = .TRUE.
                  BWORK( I+1 ) = .TRUE.
               END IF
*
               IWRK = MM*N + 1
               IWRK1 = IWRK + MM*N
*
*              Compute a pair of left and right eigenvectors.
*              (compute workspace: need up to 4*N + 6*N)
*
               IF( WANTSE .OR. WANTSB ) THEN
                  CALL STGEVC( 'B', 'S', BWORK, N, A, LDA, B, LDB,
     $                         WORK( 1 ), N, WORK( IWRK ), N, MM, M,
     $                         WORK( IWRK1 ), IERR )
                  IF( IERR.NE.0 ) THEN
                     INFO = N + 2
                     GO TO 130
                  END IF
               END IF
*
               CALL STGSNA( SENSE, 'S', BWORK, N, A, LDA, B, LDB,
     $                      WORK( 1 ), N, WORK( IWRK ), N, RCONDE( I ),
     $                      RCONDV( I ), MM, M, WORK( IWRK1 ),
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
         CALL SGGBAK( BALANC, 'L', N, ILO, IHI, LSCALE, RSCALE, N, VL,
     $                LDVL, IERR )
*
         DO 70 JC = 1, N
            IF( ALPHAI( JC ).LT.ZERO )
     $         GO TO 70
            TEMP = ZERO
            IF( ALPHAI( JC ).EQ.ZERO ) THEN
               DO 30 JR = 1, N
                  TEMP = MAX( TEMP, ABS( VL( JR, JC ) ) )
   30          CONTINUE
            ELSE
               DO 40 JR = 1, N
                  TEMP = MAX( TEMP, ABS( VL( JR, JC ) )+
     $                   ABS( VL( JR, JC+1 ) ) )
   40          CONTINUE
            END IF
            IF( TEMP.LT.SMLNUM )
     $         GO TO 70
            TEMP = ONE / TEMP
            IF( ALPHAI( JC ).EQ.ZERO ) THEN
               DO 50 JR = 1, N
                  VL( JR, JC ) = VL( JR, JC )*TEMP
   50          CONTINUE
            ELSE
               DO 60 JR = 1, N
                  VL( JR, JC ) = VL( JR, JC )*TEMP
                  VL( JR, JC+1 ) = VL( JR, JC+1 )*TEMP
   60          CONTINUE
            END IF
   70    CONTINUE
      END IF
      IF( ILVR ) THEN
         CALL SGGBAK( BALANC, 'R', N, ILO, IHI, LSCALE, RSCALE, N, VR,
     $                LDVR, IERR )
         DO 120 JC = 1, N
            IF( ALPHAI( JC ).LT.ZERO )
     $         GO TO 120
            TEMP = ZERO
            IF( ALPHAI( JC ).EQ.ZERO ) THEN
               DO 80 JR = 1, N
                  TEMP = MAX( TEMP, ABS( VR( JR, JC ) ) )
   80          CONTINUE
            ELSE
               DO 90 JR = 1, N
                  TEMP = MAX( TEMP, ABS( VR( JR, JC ) )+
     $                   ABS( VR( JR, JC+1 ) ) )
   90          CONTINUE
            END IF
            IF( TEMP.LT.SMLNUM )
     $         GO TO 120
            TEMP = ONE / TEMP
            IF( ALPHAI( JC ).EQ.ZERO ) THEN
               DO 100 JR = 1, N
                  VR( JR, JC ) = VR( JR, JC )*TEMP
  100          CONTINUE
            ELSE
               DO 110 JR = 1, N
                  VR( JR, JC ) = VR( JR, JC )*TEMP
                  VR( JR, JC+1 ) = VR( JR, JC+1 )*TEMP
  110          CONTINUE
            END IF
  120    CONTINUE
      END IF
*
*     Undo scaling if necessary
*
  130 CONTINUE
*
      IF( ILASCL ) THEN
         CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHAR, N, IERR )
         CALL SLASCL( 'G', 0, 0, ANRMTO, ANRM, N, 1, ALPHAI, N, IERR )
      END IF
*
      IF( ILBSCL ) THEN
         CALL SLASCL( 'G', 0, 0, BNRMTO, BNRM, N, 1, BETA, N, IERR )
      END IF
*
      WORK( 1 ) = MAXWRK
      RETURN
*
*     End of SGGEVX
*
      END
