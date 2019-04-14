*> \brief \b DBDSVDX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DBDSVDX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dbdsvdx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dbdsvdx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dbdsvdx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*     SUBROUTINE DBDSVDX( UPLO, JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
*    $                    NS, S, Z, LDZ, WORK, IWORK, INFO )
*
*     .. Scalar Arguments ..
*      CHARACTER          JOBZ, RANGE, UPLO
*      INTEGER            IL, INFO, IU, LDZ, N, NS
*      DOUBLE PRECISION   VL, VU
*     ..
*     .. Array Arguments ..
*      INTEGER            IWORK( * )
*      DOUBLE PRECISION   D( * ), E( * ), S( * ), WORK( * ),
*                         Z( LDZ, * )
*       ..
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>  DBDSVDX computes the singular value decomposition (SVD) of a real
*>  N-by-N (upper or lower) bidiagonal matrix B, B = U * S * VT,
*>  where S is a diagonal matrix with non-negative diagonal elements
*>  (the singular values of B), and U and VT are orthogonal matrices
*>  of left and right singular vectors, respectively.
*>
*>  Given an upper bidiagonal B with diagonal D = [ d_1 d_2 ... d_N ]
*>  and superdiagonal E = [ e_1 e_2 ... e_N-1 ], DBDSVDX computes the
*>  singular value decompositon of B through the eigenvalues and
*>  eigenvectors of the N*2-by-N*2 tridiagonal matrix
*>
*>        |  0  d_1                |
*>        | d_1  0  e_1            |
*>  TGK = |     e_1  0  d_2        |
*>        |         d_2  .   .     |
*>        |              .   .   . |
*>
*>  If (s,u,v) is a singular triplet of B with ||u|| = ||v|| = 1, then
*>  (+/-s,q), ||q|| = 1, are eigenpairs of TGK, with q = P * ( u' +/-v' ) /
*>  sqrt(2) = ( v_1 u_1 v_2 u_2 ... v_n u_n ) / sqrt(2), and
*>  P = [ e_{n+1} e_{1} e_{n+2} e_{2} ... ].
*>
*>  Given a TGK matrix, one can either a) compute -s,-v and change signs
*>  so that the singular values (and corresponding vectors) are already in
*>  descending order (as in DGESVD/DGESDD) or b) compute s,v and reorder
*>  the values (and corresponding vectors). DBDSVDX implements a) by
*>  calling DSTEVX (bisection plus inverse iteration, to be replaced
*>  with a version of the Multiple Relative Robust Representation
*>  algorithm. (See P. Willems and B. Lang, A framework for the MR^3
*>  algorithm: theory and implementation, SIAM J. Sci. Comput.,
*>  35:740-766, 2013.)
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  B is upper bidiagonal;
*>          = 'L':  B is lower bidiagonal.
*> \endverbatim
*>
*> \param[in] JOBZ
*> \verbatim
*>          JOBZ is CHARACTER*1
*>          = 'N':  Compute singular values only;
*>          = 'V':  Compute singular values and singular vectors.
*> \endverbatim
*>
*> \param[in] RANGE
*> \verbatim
*>          RANGE is CHARACTER*1
*>          = 'A': all singular values will be found.
*>          = 'V': all singular values in the half-open interval [VL,VU)
*>                 will be found.
*>          = 'I': the IL-th through IU-th singular values will be found.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the bidiagonal matrix.  N >= 0.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The n diagonal elements of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (max(1,N-1))
*>          The (n-1) superdiagonal elements of the bidiagonal matrix
*>          B in elements 1 to N-1.
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>         VL is DOUBLE PRECISION
*>          If RANGE='V', the lower bound of the interval to
*>          be searched for singular values. VU > VL.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>         VU is DOUBLE PRECISION
*>          If RANGE='V', the upper bound of the interval to
*>          be searched for singular values. VU > VL.
*>          Not referenced if RANGE = 'A' or 'I'.
*> \endverbatim
*>
*> \param[in] IL
*> \verbatim
*>          IL is INTEGER
*>          If RANGE='I', the index of the
*>          smallest singular value to be returned.
*>          1 <= IL <= IU <= min(M,N), if min(M,N) > 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[in] IU
*> \verbatim
*>          IU is INTEGER
*>          If RANGE='I', the index of the
*>          largest singular value to be returned.
*>          1 <= IL <= IU <= min(M,N), if min(M,N) > 0.
*>          Not referenced if RANGE = 'A' or 'V'.
*> \endverbatim
*>
*> \param[out] NS
*> \verbatim
*>          NS is INTEGER
*>          The total number of singular values found.  0 <= NS <= N.
*>          If RANGE = 'A', NS = N, and if RANGE = 'I', NS = IU-IL+1.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (N)
*>          The first NS elements contain the selected singular values in
*>          ascending order.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension (2*N,K) )
*>          If JOBZ = 'V', then if INFO = 0 the first NS columns of Z
*>          contain the singular vectors of the matrix B corresponding to
*>          the selected singular values, with U in rows 1 to N and V
*>          in rows N+1 to N*2, i.e.
*>          Z = [ U ]
*>              [ V ]
*>          If JOBZ = 'N', then Z is not referenced.
*>          Note: The user must ensure that at least K = NS+1 columns are
*>          supplied in the array Z; if RANGE = 'V', the exact value of
*>          NS is not known in advance and an upper bound must be used.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z. LDZ >= 1, and if
*>          JOBZ = 'V', LDZ >= max(2,N*2).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (14*N)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (12*N)
*>          If JOBZ = 'V', then if INFO = 0, the first NS elements of
*>          IWORK are zero. If INFO > 0, then IWORK contains the indices
*>          of the eigenvectors that failed to converge in DSTEVX.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, then i eigenvectors failed to converge
*>                   in DSTEVX. The indices of the eigenvectors
*>                   (as returned by DSTEVX) are stored in the
*>                   array IWORK.
*>                if INFO = N*2 + 1, an internal error occurred.
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
*> \ingroup doubleOTHEReigen
*
*  =====================================================================
      SUBROUTINE DBDSVDX( UPLO, JOBZ, RANGE, N, D, E, VL, VU, IL, IU,
     $                    NS, S, Z, LDZ, WORK, IWORK, INFO)
*
*  -- LAPACK driver routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      CHARACTER          JOBZ, RANGE, UPLO
      INTEGER            IL, INFO, IU, LDZ, N, NS
      DOUBLE PRECISION   VL, VU
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), S( * ), WORK( * ),
     $                   Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TEN, HNDRD, MEIGTH
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TEN = 10.0D0,
     $                     HNDRD = 100.0D0, MEIGTH = -0.1250D0 )
      DOUBLE PRECISION   FUDGE
      PARAMETER          ( FUDGE = 2.0D0 )
*     ..
*     .. Local Scalars ..
      CHARACTER          RNGVX
      LOGICAL            ALLSV, INDSV, LOWER, SPLIT, SVEQ0, VALSV, WANTZ
      INTEGER            I, ICOLZ, IDBEG, IDEND, IDTGK, IDPTR, IEPTR,
     $                   IETGK, IIFAIL, IIWORK, ILTGK, IROWU, IROWV,
     $                   IROWZ, ISBEG, ISPLT, ITEMP, IUTGK, J, K,
     $                   NTGK, NRU, NRV, NSL
      DOUBLE PRECISION   ABSTOL, EPS, EMIN, MU, NRMU, NRMV, ORTOL, SMAX,
     $                   SMIN, SQRT2, THRESH, TOL, ULP,
     $                   VLTGK, VUTGK, ZJTJI
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            IDAMAX
      DOUBLE PRECISION   DDOT, DLAMCH, DNRM2
      EXTERNAL           IDAMAX, LSAME, DAXPY, DDOT, DLAMCH, DNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           DSTEVX, DCOPY, DLASET, DSCAL, DSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      ALLSV = LSAME( RANGE, 'A' )
      VALSV = LSAME( RANGE, 'V' )
      INDSV = LSAME( RANGE, 'I' )
      WANTZ = LSAME( JOBZ, 'V' )
      LOWER = LSAME( UPLO, 'L' )
*
      INFO = 0
      IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LOWER ) THEN
         INFO = -1
      ELSE IF( .NOT.( WANTZ .OR. LSAME( JOBZ, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( ALLSV .OR. VALSV .OR. INDSV ) ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( N.GT.0 ) THEN
         IF( VALSV ) THEN
            IF( VL.LT.ZERO ) THEN
               INFO = -7
            ELSE IF( VU.LE.VL ) THEN
               INFO = -8
            END IF
         ELSE IF( INDSV ) THEN
            IF( IL.LT.1 .OR. IL.GT.MAX( 1, N ) ) THEN
               INFO = -9
            ELSE IF( IU.LT.MIN( N, IL ) .OR. IU.GT.N ) THEN
               INFO = -10
            END IF
         END IF
      END IF
      IF( INFO.EQ.0 ) THEN
         IF( LDZ.LT.1 .OR. ( WANTZ .AND. LDZ.LT.N*2 ) ) INFO = -14
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DBDSVDX', -INFO )
         RETURN
      END IF
*
*     Quick return if possible (N.LE.1)
*
      NS = 0
      IF( N.EQ.0 ) RETURN
*
      IF( N.EQ.1 ) THEN
         IF( ALLSV .OR. INDSV ) THEN
            NS = 1
            S( 1 ) = ABS( D( 1 ) )
         ELSE
            IF( VL.LT.ABS( D( 1 ) ) .AND. VU.GE.ABS( D( 1 ) ) ) THEN
               NS = 1
               S( 1 ) = ABS( D( 1 ) )
            END IF
         END IF
         IF( WANTZ ) THEN
            Z( 1, 1 ) = SIGN( ONE, D( 1 ) )
            Z( 2, 1 ) = ONE
         END IF
         RETURN
      END IF
*
      ABSTOL = 2*DLAMCH( 'Safe Minimum' )
      ULP = DLAMCH( 'Precision' )
      EPS = DLAMCH( 'Epsilon' )
      SQRT2 = SQRT( 2.0D0 )
      ORTOL = SQRT( ULP )
*
*     Criterion for splitting is taken from DBDSQR when singular
*     values are computed to relative accuracy TOL. (See J. Demmel and
*     W. Kahan, Accurate singular values of bidiagonal matrices, SIAM
*     J. Sci. and Stat. Comput., 11:873–912, 1990.)
*
      TOL = MAX( TEN, MIN( HNDRD, EPS**MEIGTH ) )*EPS
*
*     Compute approximate maximum, minimum singular values.
*
      I = IDAMAX( N, D, 1 )
      SMAX = ABS( D( I ) )
      I = IDAMAX( N-1, E, 1 )
      SMAX = MAX( SMAX, ABS( E( I ) ) )
*
*     Compute threshold for neglecting D's and E's.
*
      SMIN = ABS( D( 1 ) )
      IF( SMIN.NE.ZERO ) THEN
         MU = SMIN
         DO I = 2, N
            MU = ABS( D( I ) )*( MU / ( MU+ABS( E( I-1 ) ) ) )
            SMIN = MIN( SMIN, MU )
            IF( SMIN.EQ.ZERO ) EXIT
         END DO
      END IF
      SMIN = SMIN / SQRT( DBLE( N ) )
      THRESH = TOL*SMIN
*
*     Check for zeros in D and E (splits), i.e. submatrices.
*
      DO I = 1, N-1
         IF( ABS( D( I ) ).LE.THRESH ) D( I ) = ZERO
         IF( ABS( E( I ) ).LE.THRESH ) E( I ) = ZERO
      END DO
      IF( ABS( D( N ) ).LE.THRESH ) D( N ) = ZERO
*
*     Pointers for arrays used by DSTEVX.
*
      IDTGK = 1
      IETGK = IDTGK + N*2
      ITEMP = IETGK + N*2
      IIFAIL = 1
      IIWORK = IIFAIL + N*2
*
*     Set RNGVX, which corresponds to RANGE for DSTEVX in TGK mode.
*     VL,VU or IL,IU are redefined to conform to implementation a)
*     described in the leading comments.
*
      ILTGK = 0
      IUTGK = 0
      VLTGK = ZERO
      VUTGK = ZERO
*
      IF( ALLSV ) THEN
*
*        All singular values will be found. We aim at -s (see
*        leading comments) with RNGVX = 'I'. IL and IU are set
*        later (as ILTGK and IUTGK) according to the dimension
*        of the active submatrix.
*
         RNGVX = 'I'
         IF( WANTZ ) CALL DLASET( 'F', N*2, N+1, ZERO, ZERO, Z, LDZ )
      ELSE IF( VALSV ) THEN
*
*        Find singular values in a half-open interval. We aim
*        at -s (see leading comments) and we swap VL and VU
*        (as VUTGK and VLTGK), changing their signs.
*
         RNGVX = 'V'
         VLTGK = -VU
         VUTGK = -VL
         WORK( IDTGK:IDTGK+2*N-1 ) = ZERO
         CALL DCOPY( N, D, 1, WORK( IETGK ), 2 )
         CALL DCOPY( N-1, E, 1, WORK( IETGK+1 ), 2 )
         CALL DSTEVX( 'N', 'V', N*2, WORK( IDTGK ), WORK( IETGK ),
     $                VLTGK, VUTGK, ILTGK, ILTGK, ABSTOL, NS, S,
     $                Z, LDZ, WORK( ITEMP ), IWORK( IIWORK ),
     $                IWORK( IIFAIL ), INFO )
         IF( NS.EQ.0 ) THEN
            RETURN
         ELSE
            IF( WANTZ ) CALL DLASET( 'F', N*2, NS, ZERO, ZERO, Z, LDZ )
         END IF
      ELSE IF( INDSV ) THEN
*
*        Find the IL-th through the IU-th singular values. We aim
*        at -s (see leading comments) and indices are mapped into
*        values, therefore mimicking DSTEBZ, where
*
*        GL = GL - FUDGE*TNORM*ULP*N - FUDGE*TWO*PIVMIN
*        GU = GU + FUDGE*TNORM*ULP*N + FUDGE*PIVMIN
*
         ILTGK = IL
         IUTGK = IU
         RNGVX = 'V'
         WORK( IDTGK:IDTGK+2*N-1 ) = ZERO
         CALL DCOPY( N, D, 1, WORK( IETGK ), 2 )
         CALL DCOPY( N-1, E, 1, WORK( IETGK+1 ), 2 )
         CALL DSTEVX( 'N', 'I', N*2, WORK( IDTGK ), WORK( IETGK ),
     $                VLTGK, VLTGK, ILTGK, ILTGK, ABSTOL, NS, S,
     $                Z, LDZ, WORK( ITEMP ), IWORK( IIWORK ),
     $                IWORK( IIFAIL ), INFO )
         VLTGK = S( 1 ) - FUDGE*SMAX*ULP*N
         WORK( IDTGK:IDTGK+2*N-1 ) = ZERO
         CALL DCOPY( N, D, 1, WORK( IETGK ), 2 )
         CALL DCOPY( N-1, E, 1, WORK( IETGK+1 ), 2 )
         CALL DSTEVX( 'N', 'I', N*2, WORK( IDTGK ), WORK( IETGK ),
     $                VUTGK, VUTGK, IUTGK, IUTGK, ABSTOL, NS, S,
     $                Z, LDZ, WORK( ITEMP ), IWORK( IIWORK ),
     $                IWORK( IIFAIL ), INFO )
         VUTGK = S( 1 ) + FUDGE*SMAX*ULP*N
         VUTGK = MIN( VUTGK, ZERO )
*
*        If VLTGK=VUTGK, DSTEVX returns an error message,
*        so if needed we change VUTGK slightly.
*
         IF( VLTGK.EQ.VUTGK ) VLTGK = VLTGK - TOL
*
         IF( WANTZ ) CALL DLASET( 'F', N*2, IU-IL+1, ZERO, ZERO, Z, LDZ)
      END IF
*
*     Initialize variables and pointers for S, Z, and WORK.
*
*     NRU, NRV: number of rows in U and V for the active submatrix
*     IDBEG, ISBEG: offsets for the entries of D and S
*     IROWZ, ICOLZ: offsets for the rows and columns of Z
*     IROWU, IROWV: offsets for the rows of U and V
*
      NS = 0
      NRU = 0
      NRV = 0
      IDBEG = 1
      ISBEG = 1
      IROWZ = 1
      ICOLZ = 1
      IROWU = 2
      IROWV = 1
      SPLIT = .FALSE.
      SVEQ0 = .FALSE.
*
*     Form the tridiagonal TGK matrix.
*
      S( 1:N ) = ZERO
      WORK( IETGK+2*N-1 ) = ZERO
      WORK( IDTGK:IDTGK+2*N-1 ) = ZERO
      CALL DCOPY( N, D, 1, WORK( IETGK ), 2 )
      CALL DCOPY( N-1, E, 1, WORK( IETGK+1 ), 2 )
*
*
*     Check for splits in two levels, outer level
*     in E and inner level in D.
*
      DO IEPTR = 2, N*2, 2
         IF( WORK( IETGK+IEPTR-1 ).EQ.ZERO ) THEN
*
*           Split in E (this piece of B is square) or bottom
*           of the (input bidiagonal) matrix.
*
            ISPLT = IDBEG
            IDEND = IEPTR - 1
            DO IDPTR = IDBEG, IDEND, 2
               IF( WORK( IETGK+IDPTR-1 ).EQ.ZERO ) THEN
*
*                 Split in D (rectangular submatrix). Set the number
*                 of rows in U and V (NRU and NRV) accordingly.
*
                  IF( IDPTR.EQ.IDBEG ) THEN
*
*                    D=0 at the top.
*
                     SVEQ0 = .TRUE.
                     IF( IDBEG.EQ.IDEND) THEN
                        NRU = 1
                        NRV = 1
                     END IF
                  ELSE IF( IDPTR.EQ.IDEND ) THEN
*
*                    D=0 at the bottom.
*
                     SVEQ0 = .TRUE.
                     NRU = (IDEND-ISPLT)/2 + 1
                     NRV = NRU
                     IF( ISPLT.NE.IDBEG ) THEN
                        NRU = NRU + 1
                     END IF
                  ELSE
                     IF( ISPLT.EQ.IDBEG ) THEN
*
*                       Split: top rectangular submatrix.
*
                        NRU = (IDPTR-IDBEG)/2
                        NRV = NRU + 1
                     ELSE
*
*                       Split: middle square submatrix.
*
                        NRU = (IDPTR-ISPLT)/2 + 1
                        NRV = NRU
                     END IF
                  END IF
               ELSE IF( IDPTR.EQ.IDEND ) THEN
*
*                 Last entry of D in the active submatrix.
*
                  IF( ISPLT.EQ.IDBEG ) THEN
*
*                    No split (trivial case).
*
                     NRU = (IDEND-IDBEG)/2 + 1
                     NRV = NRU
                  ELSE
*
*                    Split: bottom rectangular submatrix.
*
                     NRV = (IDEND-ISPLT)/2 + 1
                     NRU = NRV + 1
                  END IF
               END IF
*
               NTGK = NRU + NRV
*
               IF( NTGK.GT.0 ) THEN
*
*                 Compute eigenvalues/vectors of the active
*                 submatrix according to RANGE:
*                 if RANGE='A' (ALLSV) then RNGVX = 'I'
*                 if RANGE='V' (VALSV) then RNGVX = 'V'
*                 if RANGE='I' (INDSV) then RNGVX = 'V'
*
                  ILTGK = 1
                  IUTGK = NTGK / 2
                  IF( ALLSV .OR. VUTGK.EQ.ZERO ) THEN
                     IF( SVEQ0 .OR.
     $                   SMIN.LT.EPS .OR.
     $                   MOD(NTGK,2).GT.0 ) THEN
*                        Special case: eigenvalue equal to zero or very
*                        small, additional eigenvector is needed.
                         IUTGK = IUTGK + 1
                     END IF
                  END IF
*
*                 Workspace needed by DSTEVX:
*                 WORK( ITEMP: ): 2*5*NTGK
*                 IWORK( 1: ): 2*6*NTGK
*
                  CALL DSTEVX( JOBZ, RNGVX, NTGK, WORK( IDTGK+ISPLT-1 ),
     $                         WORK( IETGK+ISPLT-1 ), VLTGK, VUTGK,
     $                         ILTGK, IUTGK, ABSTOL, NSL, S( ISBEG ),
     $                         Z( IROWZ,ICOLZ ), LDZ, WORK( ITEMP ),
     $                         IWORK( IIWORK ), IWORK( IIFAIL ),
     $                         INFO )
                  IF( INFO.NE.0 ) THEN
*                    Exit with the error code from DSTEVX.
                     RETURN
                  END IF
                  EMIN = ABS( MAXVAL( S( ISBEG:ISBEG+NSL-1 ) ) )
*
                  IF( NSL.GT.0 .AND. WANTZ ) THEN
*
*                    Normalize u=Z([2,4,...],:) and v=Z([1,3,...],:),
*                    changing the sign of v as discussed in the leading
*                    comments. The norms of u and v may be (slightly)
*                    different from 1/sqrt(2) if the corresponding
*                    eigenvalues are very small or too close. We check
*                    those norms and, if needed, reorthogonalize the
*                    vectors.
*
                     IF( NSL.GT.1 .AND.
     $                   VUTGK.EQ.ZERO .AND.
     $                   MOD(NTGK,2).EQ.0 .AND.
     $                   EMIN.EQ.0 .AND. .NOT.SPLIT ) THEN
*
*                       D=0 at the top or bottom of the active submatrix:
*                       one eigenvalue is equal to zero; concatenate the
*                       eigenvectors corresponding to the two smallest
*                       eigenvalues.
*
                        Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-2 ) =
     $                  Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-2 ) +
     $                  Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-1 )
                        Z( IROWZ:IROWZ+NTGK-1,ICOLZ+NSL-1 ) =
     $                  ZERO
*                       IF( IUTGK*2.GT.NTGK ) THEN
*                          Eigenvalue equal to zero or very small.
*                          NSL = NSL - 1
*                       END IF
                     END IF
*
                     DO I = 0, MIN( NSL-1, NRU-1 )
                        NRMU = DNRM2( NRU, Z( IROWU, ICOLZ+I ), 2 )
                        IF( NRMU.EQ.ZERO ) THEN
                           INFO = N*2 + 1
                           RETURN
                        END IF
                        CALL DSCAL( NRU, ONE/NRMU,
     $                              Z( IROWU,ICOLZ+I ), 2 )
                        IF( NRMU.NE.ONE .AND.
     $                      ABS( NRMU-ORTOL )*SQRT2.GT.ONE )
     $                      THEN
                           DO J = 0, I-1
                              ZJTJI = -DDOT( NRU, Z( IROWU, ICOLZ+J ),
     $                                       2, Z( IROWU, ICOLZ+I ), 2 )
                              CALL DAXPY( NRU, ZJTJI,
     $                                    Z( IROWU, ICOLZ+J ), 2,
     $                                    Z( IROWU, ICOLZ+I ), 2 )
                           END DO
                           NRMU = DNRM2( NRU, Z( IROWU, ICOLZ+I ), 2 )
                           CALL DSCAL( NRU, ONE/NRMU,
     $                                 Z( IROWU,ICOLZ+I ), 2 )
                        END IF
                     END DO
                     DO I = 0, MIN( NSL-1, NRV-1 )
                        NRMV = DNRM2( NRV, Z( IROWV, ICOLZ+I ), 2 )
                        IF( NRMV.EQ.ZERO ) THEN
                           INFO = N*2 + 1
                           RETURN
                        END IF
                        CALL DSCAL( NRV, -ONE/NRMV,
     $                              Z( IROWV,ICOLZ+I ), 2 )
                        IF( NRMV.NE.ONE .AND.
     $                      ABS( NRMV-ORTOL )*SQRT2.GT.ONE )
     $                      THEN
                           DO J = 0, I-1
                              ZJTJI = -DDOT( NRV, Z( IROWV, ICOLZ+J ),
     $                                       2, Z( IROWV, ICOLZ+I ), 2 )
                              CALL DAXPY( NRU, ZJTJI,
     $                                    Z( IROWV, ICOLZ+J ), 2,
     $                                    Z( IROWV, ICOLZ+I ), 2 )
                           END DO
                           NRMV = DNRM2( NRV, Z( IROWV, ICOLZ+I ), 2 )
                           CALL DSCAL( NRV, ONE/NRMV,
     $                                 Z( IROWV,ICOLZ+I ), 2 )
                        END IF
                     END DO
                     IF( VUTGK.EQ.ZERO .AND.
     $                   IDPTR.LT.IDEND .AND.
     $                   MOD(NTGK,2).GT.0 ) THEN
*
*                       D=0 in the middle of the active submatrix (one
*                       eigenvalue is equal to zero): save the corresponding
*                       eigenvector for later use (when bottom of the
*                       active submatrix is reached).
*
                        SPLIT = .TRUE.
                        Z( IROWZ:IROWZ+NTGK-1,N+1 ) =
     $                     Z( IROWZ:IROWZ+NTGK-1,NS+NSL )
                        Z( IROWZ:IROWZ+NTGK-1,NS+NSL ) =
     $                     ZERO
                     END IF
                  END IF !** WANTZ **!
*
                  NSL = MIN( NSL, NRU )
                  SVEQ0 = .FALSE.
*
*                 Absolute values of the eigenvalues of TGK.
*
                  DO I = 0, NSL-1
                     S( ISBEG+I ) = ABS( S( ISBEG+I ) )
                  END DO
*
*                 Update pointers for TGK, S and Z.
*
                  ISBEG = ISBEG + NSL
                  IROWZ = IROWZ + NTGK
                  ICOLZ = ICOLZ + NSL
                  IROWU = IROWZ
                  IROWV = IROWZ + 1
                  ISPLT = IDPTR + 1
                  NS = NS + NSL
                  NRU = 0
                  NRV = 0
               END IF !** NTGK.GT.0 **!
               IF( IROWZ.LT.N*2 .AND. WANTZ ) THEN
                  Z( 1:IROWZ-1, ICOLZ ) = ZERO
               END IF
            END DO !** IDPTR loop **!
            IF( SPLIT .AND. WANTZ ) THEN
*
*              Bring back eigenvector corresponding
*              to eigenvalue equal to zero.
*
               Z( IDBEG:IDEND-NTGK+1,ISBEG-1 ) =
     $         Z( IDBEG:IDEND-NTGK+1,ISBEG-1 ) +
     $         Z( IDBEG:IDEND-NTGK+1,N+1 )
               Z( IDBEG:IDEND-NTGK+1,N+1 ) = 0
            END IF
            IROWV = IROWV - 1
            IROWU = IROWU + 1
            IDBEG = IEPTR + 1
            SVEQ0 = .FALSE.
            SPLIT = .FALSE.
         END IF !** Check for split in E **!
      END DO !** IEPTR loop **!
*
*     Sort the singular values into decreasing order (insertion sort on
*     singular values, but only one transposition per singular vector)
*
      DO I = 1, NS-1
         K = 1
         SMIN = S( 1 )
         DO J = 2, NS + 1 - I
            IF( S( J ).LE.SMIN ) THEN
               K = J
               SMIN = S( J )
            END IF
         END DO
         IF( K.NE.NS+1-I ) THEN
            S( K ) = S( NS+1-I )
            S( NS+1-I ) = SMIN
            IF( WANTZ ) CALL DSWAP( N*2, Z( 1,K ), 1, Z( 1,NS+1-I ), 1 )
         END IF
      END DO
*
*     If RANGE=I, check for singular values/vectors to be discarded.
*
      IF( INDSV ) THEN
         K = IU - IL + 1
         IF( K.LT.NS ) THEN
            S( K+1:NS ) = ZERO
            IF( WANTZ ) Z( 1:N*2,K+1:NS ) = ZERO
            NS = K
         END IF
      END IF
*
*     Reorder Z: U = Z( 1:N,1:NS ), V = Z( N+1:N*2,1:NS ).
*     If B is a lower diagonal, swap U and V.
*
      IF( WANTZ ) THEN
      DO I = 1, NS
         CALL DCOPY( N*2, Z( 1,I ), 1, WORK, 1 )
         IF( LOWER ) THEN
            CALL DCOPY( N, WORK( 2 ), 2, Z( N+1,I ), 1 )
            CALL DCOPY( N, WORK( 1 ), 2, Z( 1  ,I ), 1 )
         ELSE
            CALL DCOPY( N, WORK( 2 ), 2, Z( 1  ,I ), 1 )
            CALL DCOPY( N, WORK( 1 ), 2, Z( N+1,I ), 1 )
         END IF
      END DO
      END IF
*
      RETURN
*
*     End of DBDSVDX
*
      END
