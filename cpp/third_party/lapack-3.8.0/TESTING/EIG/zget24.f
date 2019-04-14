*> \brief \b ZGET24
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGET24( COMP, JTYPE, THRESH, ISEED, NOUNIT, N, A, LDA,
*                          H, HT, W, WT, WTMP, VS, LDVS, VS1, RCDEIN,
*                          RCDVIN, NSLCT, ISLCT, ISRT, RESULT, WORK,
*                          LWORK, RWORK, BWORK, INFO )
*
*       .. Scalar Arguments ..
*       LOGICAL            COMP
*       INTEGER            INFO, ISRT, JTYPE, LDA, LDVS, LWORK, N, NOUNIT,
*      $                   NSLCT
*       DOUBLE PRECISION   RCDEIN, RCDVIN, THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            BWORK( * )
*       INTEGER            ISEED( 4 ), ISLCT( * )
*       DOUBLE PRECISION   RESULT( 17 ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), H( LDA, * ), HT( LDA, * ),
*      $                   VS( LDVS, * ), VS1( LDVS, * ), W( * ),
*      $                   WORK( * ), WT( * ), WTMP( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZGET24 checks the nonsymmetric eigenvalue (Schur form) problem
*>    expert driver ZGEESX.
*>
*>    If COMP = .FALSE., the first 13 of the following tests will be
*>    be performed on the input matrix A, and also tests 14 and 15
*>    if LWORK is sufficiently large.
*>    If COMP = .TRUE., all 17 test will be performed.
*>
*>    (1)     0 if T is in Schur form, 1/ulp otherwise
*>           (no sorting of eigenvalues)
*>
*>    (2)     | A - VS T VS' | / ( n |A| ulp )
*>
*>      Here VS is the matrix of Schur eigenvectors, and T is in Schur
*>      form  (no sorting of eigenvalues).
*>
*>    (3)     | I - VS VS' | / ( n ulp ) (no sorting of eigenvalues).
*>
*>    (4)     0     if W are eigenvalues of T
*>            1/ulp otherwise
*>            (no sorting of eigenvalues)
*>
*>    (5)     0     if T(with VS) = T(without VS),
*>            1/ulp otherwise
*>            (no sorting of eigenvalues)
*>
*>    (6)     0     if eigenvalues(with VS) = eigenvalues(without VS),
*>            1/ulp otherwise
*>            (no sorting of eigenvalues)
*>
*>    (7)     0 if T is in Schur form, 1/ulp otherwise
*>            (with sorting of eigenvalues)
*>
*>    (8)     | A - VS T VS' | / ( n |A| ulp )
*>
*>      Here VS is the matrix of Schur eigenvectors, and T is in Schur
*>      form  (with sorting of eigenvalues).
*>
*>    (9)     | I - VS VS' | / ( n ulp ) (with sorting of eigenvalues).
*>
*>    (10)    0     if W are eigenvalues of T
*>            1/ulp otherwise
*>            If workspace sufficient, also compare W with and
*>            without reciprocal condition numbers
*>            (with sorting of eigenvalues)
*>
*>    (11)    0     if T(with VS) = T(without VS),
*>            1/ulp otherwise
*>            If workspace sufficient, also compare T with and without
*>            reciprocal condition numbers
*>            (with sorting of eigenvalues)
*>
*>    (12)    0     if eigenvalues(with VS) = eigenvalues(without VS),
*>            1/ulp otherwise
*>            If workspace sufficient, also compare VS with and without
*>            reciprocal condition numbers
*>            (with sorting of eigenvalues)
*>
*>    (13)    if sorting worked and SDIM is the number of
*>            eigenvalues which were SELECTed
*>            If workspace sufficient, also compare SDIM with and
*>            without reciprocal condition numbers
*>
*>    (14)    if RCONDE the same no matter if VS and/or RCONDV computed
*>
*>    (15)    if RCONDV the same no matter if VS and/or RCONDE computed
*>
*>    (16)  |RCONDE - RCDEIN| / cond(RCONDE)
*>
*>       RCONDE is the reciprocal average eigenvalue condition number
*>       computed by ZGEESX and RCDEIN (the precomputed true value)
*>       is supplied as input.  cond(RCONDE) is the condition number
*>       of RCONDE, and takes errors in computing RCONDE into account,
*>       so that the resulting quantity should be O(ULP). cond(RCONDE)
*>       is essentially given by norm(A)/RCONDV.
*>
*>    (17)  |RCONDV - RCDVIN| / cond(RCONDV)
*>
*>       RCONDV is the reciprocal right invariant subspace condition
*>       number computed by ZGEESX and RCDVIN (the precomputed true
*>       value) is supplied as input. cond(RCONDV) is the condition
*>       number of RCONDV, and takes errors in computing RCONDV into
*>       account, so that the resulting quantity should be O(ULP).
*>       cond(RCONDV) is essentially given by norm(A)/RCONDE.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] COMP
*> \verbatim
*>          COMP is LOGICAL
*>          COMP describes which input tests to perform:
*>            = .FALSE. if the computed condition numbers are not to
*>                      be tested against RCDVIN and RCDEIN
*>            = .TRUE.  if they are to be compared
*> \endverbatim
*>
*> \param[in] JTYPE
*> \verbatim
*>          JTYPE is INTEGER
*>          Type of input matrix. Used to label output if error occurs.
*> \endverbatim
*>
*> \param[in] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          If COMP = .FALSE., the random number generator seed
*>          used to produce matrix.
*>          If COMP = .TRUE., ISEED(1) = the number of the example.
*>          Used to label output if error occurs.
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
*>          (e.g., if a routine returns INFO not equal to 0.)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The dimension of A. N must be at least 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          Used to hold the matrix whose eigenvalues are to be
*>          computed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A, and H. LDA must be at
*>          least 1 and at least N.
*> \endverbatim
*>
*> \param[out] H
*> \verbatim
*>          H is COMPLEX*16 array, dimension (LDA, N)
*>          Another copy of the test matrix A, modified by ZGEESX.
*> \endverbatim
*>
*> \param[out] HT
*> \verbatim
*>          HT is COMPLEX*16 array, dimension (LDA, N)
*>          Yet another copy of the test matrix A, modified by ZGEESX.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX*16 array, dimension (N)
*>          The computed eigenvalues of A.
*> \endverbatim
*>
*> \param[out] WT
*> \verbatim
*>          WT is COMPLEX*16 array, dimension (N)
*>          Like W, this array contains the eigenvalues of A,
*>          but those computed when ZGEESX only computes a partial
*>          eigendecomposition, i.e. not Schur vectors
*> \endverbatim
*>
*> \param[out] WTMP
*> \verbatim
*>          WTMP is COMPLEX*16 array, dimension (N)
*>          Like W, this array contains the eigenvalues of A,
*>          but sorted by increasing real or imaginary part.
*> \endverbatim
*>
*> \param[out] VS
*> \verbatim
*>          VS is COMPLEX*16 array, dimension (LDVS, N)
*>          VS holds the computed Schur vectors.
*> \endverbatim
*>
*> \param[in] LDVS
*> \verbatim
*>          LDVS is INTEGER
*>          Leading dimension of VS. Must be at least max(1, N).
*> \endverbatim
*>
*> \param[out] VS1
*> \verbatim
*>          VS1 is COMPLEX*16 array, dimension (LDVS, N)
*>          VS1 holds another copy of the computed Schur vectors.
*> \endverbatim
*>
*> \param[in] RCDEIN
*> \verbatim
*>          RCDEIN is DOUBLE PRECISION
*>          When COMP = .TRUE. RCDEIN holds the precomputed reciprocal
*>          condition number for the average of selected eigenvalues.
*> \endverbatim
*>
*> \param[in] RCDVIN
*> \verbatim
*>          RCDVIN is DOUBLE PRECISION
*>          When COMP = .TRUE. RCDVIN holds the precomputed reciprocal
*>          condition number for the selected right invariant subspace.
*> \endverbatim
*>
*> \param[in] NSLCT
*> \verbatim
*>          NSLCT is INTEGER
*>          When COMP = .TRUE. the number of selected eigenvalues
*>          corresponding to the precomputed values RCDEIN and RCDVIN.
*> \endverbatim
*>
*> \param[in] ISLCT
*> \verbatim
*>          ISLCT is INTEGER array, dimension (NSLCT)
*>          When COMP = .TRUE. ISLCT selects the eigenvalues of the
*>          input matrix corresponding to the precomputed values RCDEIN
*>          and RCDVIN. For I=1, ... ,NSLCT, if ISLCT(I) = J, then the
*>          eigenvalue with the J-th largest real or imaginary part is
*>          selected. The real part is used if ISRT = 0, and the
*>          imaginary part if ISRT = 1.
*>          Not referenced if COMP = .FALSE.
*> \endverbatim
*>
*> \param[in] ISRT
*> \verbatim
*>          ISRT is INTEGER
*>          When COMP = .TRUE., ISRT describes how ISLCT is used to
*>          choose a subset of the spectrum.
*>          Not referenced if COMP = .FALSE.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (17)
*>          The values computed by the 17 tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (2*N*N)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The number of entries in WORK to be passed to ZGEESX. This
*>          must be at least 2*N, and N*(N+1)/2 if tests 14--16 are to
*>          be performed.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N)
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
*>          If 0,  successful exit.
*>          If <0, input parameter -INFO had an incorrect value.
*>          If >0, ZGEESX returned an error code, the absolute
*>                 value of which is returned.
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
      SUBROUTINE ZGET24( COMP, JTYPE, THRESH, ISEED, NOUNIT, N, A, LDA,
     $                   H, HT, W, WT, WTMP, VS, LDVS, VS1, RCDEIN,
     $                   RCDVIN, NSLCT, ISLCT, ISRT, RESULT, WORK,
     $                   LWORK, RWORK, BWORK, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            COMP
      INTEGER            INFO, ISRT, JTYPE, LDA, LDVS, LWORK, N, NOUNIT,
     $                   NSLCT
      DOUBLE PRECISION   RCDEIN, RCDVIN, THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            BWORK( * )
      INTEGER            ISEED( 4 ), ISLCT( * )
      DOUBLE PRECISION   RESULT( 17 ), RWORK( * )
      COMPLEX*16         A( LDA, * ), H( LDA, * ), HT( LDA, * ),
     $                   VS( LDVS, * ), VS1( LDVS, * ), W( * ),
     $                   WORK( * ), WT( * ), WTMP( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      DOUBLE PRECISION   EPSIN
      PARAMETER          ( EPSIN = 5.9605D-8 )
*     ..
*     .. Local Scalars ..
      CHARACTER          SORT
      INTEGER            I, IINFO, ISORT, ITMP, J, KMIN, KNTEIG, RSUB,
     $                   SDIM, SDIM1
      DOUBLE PRECISION   ANORM, EPS, RCNDE1, RCNDV1, RCONDE, RCONDV,
     $                   SMLNUM, TOL, TOLIN, ULP, ULPINV, V, VRICMP,
     $                   VRIMIN, WNORM
      COMPLEX*16         CTMP
*     ..
*     .. Local Arrays ..
      INTEGER            IPNT( 20 )
*     ..
*     .. External Functions ..
      LOGICAL            ZSLECT
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           ZSLECT, DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZCOPY, ZGEESX, ZGEMM, ZLACPY, ZUNT01
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DIMAG, MAX, MIN
*     ..
*     .. Arrays in Common ..
      LOGICAL            SELVAL( 20 )
      DOUBLE PRECISION   SELWI( 20 ), SELWR( 20 )
*     ..
*     .. Scalars in Common ..
      INTEGER            SELDIM, SELOPT
*     ..
*     .. Common blocks ..
      COMMON             / SSLCT / SELOPT, SELDIM, SELVAL, SELWR, SELWI
*     ..
*     .. Executable Statements ..
*
*     Check for errors
*
      INFO = 0
      IF( THRESH.LT.ZERO ) THEN
         INFO = -3
      ELSE IF( NOUNIT.LE.0 ) THEN
         INFO = -5
      ELSE IF( N.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDA.LT.1 .OR. LDA.LT.N ) THEN
         INFO = -8
      ELSE IF( LDVS.LT.1 .OR. LDVS.LT.N ) THEN
         INFO = -15
      ELSE IF( LWORK.LT.2*N ) THEN
         INFO = -24
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGET24', -INFO )
         RETURN
      END IF
*
*     Quick return if nothing to do
*
      DO 10 I = 1, 17
         RESULT( I ) = -ONE
   10 CONTINUE
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Important constants
*
      SMLNUM = DLAMCH( 'Safe minimum' )
      ULP = DLAMCH( 'Precision' )
      ULPINV = ONE / ULP
*
*     Perform tests (1)-(13)
*
      SELOPT = 0
      DO 90 ISORT = 0, 1
         IF( ISORT.EQ.0 ) THEN
            SORT = 'N'
            RSUB = 0
         ELSE
            SORT = 'S'
            RSUB = 6
         END IF
*
*        Compute Schur form and Schur vectors, and test them
*
         CALL ZLACPY( 'F', N, N, A, LDA, H, LDA )
         CALL ZGEESX( 'V', SORT, ZSLECT, 'N', N, H, LDA, SDIM, W, VS,
     $                LDVS, RCONDE, RCONDV, WORK, LWORK, RWORK, BWORK,
     $                IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 1+RSUB ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX1', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX1', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            RETURN
         END IF
         IF( ISORT.EQ.0 ) THEN
            CALL ZCOPY( N, W, 1, WTMP, 1 )
         END IF
*
*        Do Test (1) or Test (7)
*
         RESULT( 1+RSUB ) = ZERO
         DO 30 J = 1, N - 1
            DO 20 I = J + 1, N
               IF( H( I, J ).NE.CZERO )
     $            RESULT( 1+RSUB ) = ULPINV
   20       CONTINUE
   30    CONTINUE
*
*        Test (2) or (8): Compute norm(A - Q*H*Q') / (norm(A) * N * ULP)
*
*        Copy A to VS1, used as workspace
*
         CALL ZLACPY( ' ', N, N, A, LDA, VS1, LDVS )
*
*        Compute Q*H and store in HT.
*
         CALL ZGEMM( 'No transpose', 'No transpose', N, N, N, CONE, VS,
     $               LDVS, H, LDA, CZERO, HT, LDA )
*
*        Compute A - Q*H*Q'
*
         CALL ZGEMM( 'No transpose', 'Conjugate transpose', N, N, N,
     $               -CONE, HT, LDA, VS, LDVS, CONE, VS1, LDVS )
*
         ANORM = MAX( ZLANGE( '1', N, N, A, LDA, RWORK ), SMLNUM )
         WNORM = ZLANGE( '1', N, N, VS1, LDVS, RWORK )
*
         IF( ANORM.GT.WNORM ) THEN
            RESULT( 2+RSUB ) = ( WNORM / ANORM ) / ( N*ULP )
         ELSE
            IF( ANORM.LT.ONE ) THEN
               RESULT( 2+RSUB ) = ( MIN( WNORM, N*ANORM ) / ANORM ) /
     $                            ( N*ULP )
            ELSE
               RESULT( 2+RSUB ) = MIN( WNORM / ANORM, DBLE( N ) ) /
     $                            ( N*ULP )
            END IF
         END IF
*
*        Test (3) or (9):  Compute norm( I - Q'*Q ) / ( N * ULP )
*
         CALL ZUNT01( 'Columns', N, N, VS, LDVS, WORK, LWORK, RWORK,
     $                RESULT( 3+RSUB ) )
*
*        Do Test (4) or Test (10)
*
         RESULT( 4+RSUB ) = ZERO
         DO 40 I = 1, N
            IF( H( I, I ).NE.W( I ) )
     $         RESULT( 4+RSUB ) = ULPINV
   40    CONTINUE
*
*        Do Test (5) or Test (11)
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'N', SORT, ZSLECT, 'N', N, HT, LDA, SDIM, WT, VS,
     $                LDVS, RCONDE, RCONDV, WORK, LWORK, RWORK, BWORK,
     $                IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 5+RSUB ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX2', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX2', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
         RESULT( 5+RSUB ) = ZERO
         DO 60 J = 1, N
            DO 50 I = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 5+RSUB ) = ULPINV
   50       CONTINUE
   60    CONTINUE
*
*        Do Test (6) or Test (12)
*
         RESULT( 6+RSUB ) = ZERO
         DO 70 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 6+RSUB ) = ULPINV
   70    CONTINUE
*
*        Do Test (13)
*
         IF( ISORT.EQ.1 ) THEN
            RESULT( 13 ) = ZERO
            KNTEIG = 0
            DO 80 I = 1, N
               IF( ZSLECT( W( I ) ) )
     $            KNTEIG = KNTEIG + 1
               IF( I.LT.N ) THEN
                  IF( ZSLECT( W( I+1 ) ) .AND.
     $                ( .NOT.ZSLECT( W( I ) ) ) )RESULT( 13 ) = ULPINV
               END IF
   80       CONTINUE
            IF( SDIM.NE.KNTEIG )
     $         RESULT( 13 ) = ULPINV
         END IF
*
   90 CONTINUE
*
*     If there is enough workspace, perform tests (14) and (15)
*     as well as (10) through (13)
*
      IF( LWORK.GE.( N*( N+1 ) ) / 2 ) THEN
*
*        Compute both RCONDE and RCONDV with VS
*
         SORT = 'S'
         RESULT( 14 ) = ZERO
         RESULT( 15 ) = ZERO
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'V', SORT, ZSLECT, 'B', N, HT, LDA, SDIM1, WT,
     $                VS1, LDVS, RCONDE, RCONDV, WORK, LWORK, RWORK,
     $                BWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 14 ) = ULPINV
            RESULT( 15 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX3', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX3', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
*        Perform tests (10), (11), (12), and (13)
*
         DO 110 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 10 ) = ULPINV
            DO 100 J = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 11 ) = ULPINV
               IF( VS( I, J ).NE.VS1( I, J ) )
     $            RESULT( 12 ) = ULPINV
  100       CONTINUE
  110    CONTINUE
         IF( SDIM.NE.SDIM1 )
     $      RESULT( 13 ) = ULPINV
*
*        Compute both RCONDE and RCONDV without VS, and compare
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'N', SORT, ZSLECT, 'B', N, HT, LDA, SDIM1, WT,
     $                VS1, LDVS, RCNDE1, RCNDV1, WORK, LWORK, RWORK,
     $                BWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 14 ) = ULPINV
            RESULT( 15 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX4', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX4', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
*        Perform tests (14) and (15)
*
         IF( RCNDE1.NE.RCONDE )
     $      RESULT( 14 ) = ULPINV
         IF( RCNDV1.NE.RCONDV )
     $      RESULT( 15 ) = ULPINV
*
*        Perform tests (10), (11), (12), and (13)
*
         DO 130 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 10 ) = ULPINV
            DO 120 J = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 11 ) = ULPINV
               IF( VS( I, J ).NE.VS1( I, J ) )
     $            RESULT( 12 ) = ULPINV
  120       CONTINUE
  130    CONTINUE
         IF( SDIM.NE.SDIM1 )
     $      RESULT( 13 ) = ULPINV
*
*        Compute RCONDE with VS, and compare
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'V', SORT, ZSLECT, 'E', N, HT, LDA, SDIM1, WT,
     $                VS1, LDVS, RCNDE1, RCNDV1, WORK, LWORK, RWORK,
     $                BWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 14 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX5', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX5', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
*        Perform test (14)
*
         IF( RCNDE1.NE.RCONDE )
     $      RESULT( 14 ) = ULPINV
*
*        Perform tests (10), (11), (12), and (13)
*
         DO 150 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 10 ) = ULPINV
            DO 140 J = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 11 ) = ULPINV
               IF( VS( I, J ).NE.VS1( I, J ) )
     $            RESULT( 12 ) = ULPINV
  140       CONTINUE
  150    CONTINUE
         IF( SDIM.NE.SDIM1 )
     $      RESULT( 13 ) = ULPINV
*
*        Compute RCONDE without VS, and compare
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'N', SORT, ZSLECT, 'E', N, HT, LDA, SDIM1, WT,
     $                VS1, LDVS, RCNDE1, RCNDV1, WORK, LWORK, RWORK,
     $                BWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 14 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX6', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX6', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
*        Perform test (14)
*
         IF( RCNDE1.NE.RCONDE )
     $      RESULT( 14 ) = ULPINV
*
*        Perform tests (10), (11), (12), and (13)
*
         DO 170 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 10 ) = ULPINV
            DO 160 J = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 11 ) = ULPINV
               IF( VS( I, J ).NE.VS1( I, J ) )
     $            RESULT( 12 ) = ULPINV
  160       CONTINUE
  170    CONTINUE
         IF( SDIM.NE.SDIM1 )
     $      RESULT( 13 ) = ULPINV
*
*        Compute RCONDV with VS, and compare
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'V', SORT, ZSLECT, 'V', N, HT, LDA, SDIM1, WT,
     $                VS1, LDVS, RCNDE1, RCNDV1, WORK, LWORK, RWORK,
     $                BWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 15 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX7', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX7', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
*        Perform test (15)
*
         IF( RCNDV1.NE.RCONDV )
     $      RESULT( 15 ) = ULPINV
*
*        Perform tests (10), (11), (12), and (13)
*
         DO 190 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 10 ) = ULPINV
            DO 180 J = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 11 ) = ULPINV
               IF( VS( I, J ).NE.VS1( I, J ) )
     $            RESULT( 12 ) = ULPINV
  180       CONTINUE
  190    CONTINUE
         IF( SDIM.NE.SDIM1 )
     $      RESULT( 13 ) = ULPINV
*
*        Compute RCONDV without VS, and compare
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'N', SORT, ZSLECT, 'V', N, HT, LDA, SDIM1, WT,
     $                VS1, LDVS, RCNDE1, RCNDV1, WORK, LWORK, RWORK,
     $                BWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 15 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'ZGEESX8', IINFO, N, JTYPE,
     $            ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'ZGEESX8', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 220
         END IF
*
*        Perform test (15)
*
         IF( RCNDV1.NE.RCONDV )
     $      RESULT( 15 ) = ULPINV
*
*        Perform tests (10), (11), (12), and (13)
*
         DO 210 I = 1, N
            IF( W( I ).NE.WT( I ) )
     $         RESULT( 10 ) = ULPINV
            DO 200 J = 1, N
               IF( H( I, J ).NE.HT( I, J ) )
     $            RESULT( 11 ) = ULPINV
               IF( VS( I, J ).NE.VS1( I, J ) )
     $            RESULT( 12 ) = ULPINV
  200       CONTINUE
  210    CONTINUE
         IF( SDIM.NE.SDIM1 )
     $      RESULT( 13 ) = ULPINV
*
      END IF
*
  220 CONTINUE
*
*     If there are precomputed reciprocal condition numbers, compare
*     computed values with them.
*
      IF( COMP ) THEN
*
*        First set up SELOPT, SELDIM, SELVAL, SELWR and SELWI so that
*        the logical function ZSLECT selects the eigenvalues specified
*        by NSLCT, ISLCT and ISRT.
*
         SELDIM = N
         SELOPT = 1
         EPS = MAX( ULP, EPSIN )
         DO 230 I = 1, N
            IPNT( I ) = I
            SELVAL( I ) = .FALSE.
            SELWR( I ) = DBLE( WTMP( I ) )
            SELWI( I ) = DIMAG( WTMP( I ) )
  230    CONTINUE
         DO 250 I = 1, N - 1
            KMIN = I
            IF( ISRT.EQ.0 ) THEN
               VRIMIN = DBLE( WTMP( I ) )
            ELSE
               VRIMIN = DIMAG( WTMP( I ) )
            END IF
            DO 240 J = I + 1, N
               IF( ISRT.EQ.0 ) THEN
                  VRICMP = DBLE( WTMP( J ) )
               ELSE
                  VRICMP = DIMAG( WTMP( J ) )
               END IF
               IF( VRICMP.LT.VRIMIN ) THEN
                  KMIN = J
                  VRIMIN = VRICMP
               END IF
  240       CONTINUE
            CTMP = WTMP( KMIN )
            WTMP( KMIN ) = WTMP( I )
            WTMP( I ) = CTMP
            ITMP = IPNT( I )
            IPNT( I ) = IPNT( KMIN )
            IPNT( KMIN ) = ITMP
  250    CONTINUE
         DO 260 I = 1, NSLCT
            SELVAL( IPNT( ISLCT( I ) ) ) = .TRUE.
  260    CONTINUE
*
*        Compute condition numbers
*
         CALL ZLACPY( 'F', N, N, A, LDA, HT, LDA )
         CALL ZGEESX( 'N', 'S', ZSLECT, 'B', N, HT, LDA, SDIM1, WT, VS1,
     $                LDVS, RCONDE, RCONDV, WORK, LWORK, RWORK, BWORK,
     $                IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 16 ) = ULPINV
            RESULT( 17 ) = ULPINV
            WRITE( NOUNIT, FMT = 9999 )'ZGEESX9', IINFO, N, ISEED( 1 )
            INFO = ABS( IINFO )
            GO TO 270
         END IF
*
*        Compare condition number for average of selected eigenvalues
*        taking its condition number into account
*
         ANORM = ZLANGE( '1', N, N, A, LDA, RWORK )
         V = MAX( DBLE( N )*EPS*ANORM, SMLNUM )
         IF( ANORM.EQ.ZERO )
     $      V = ONE
         IF( V.GT.RCONDV ) THEN
            TOL = ONE
         ELSE
            TOL = V / RCONDV
         END IF
         IF( V.GT.RCDVIN ) THEN
            TOLIN = ONE
         ELSE
            TOLIN = V / RCDVIN
         END IF
         TOL = MAX( TOL, SMLNUM / EPS )
         TOLIN = MAX( TOLIN, SMLNUM / EPS )
         IF( EPS*( RCDEIN-TOLIN ).GT.RCONDE+TOL ) THEN
            RESULT( 16 ) = ULPINV
         ELSE IF( RCDEIN-TOLIN.GT.RCONDE+TOL ) THEN
            RESULT( 16 ) = ( RCDEIN-TOLIN ) / ( RCONDE+TOL )
         ELSE IF( RCDEIN+TOLIN.LT.EPS*( RCONDE-TOL ) ) THEN
            RESULT( 16 ) = ULPINV
         ELSE IF( RCDEIN+TOLIN.LT.RCONDE-TOL ) THEN
            RESULT( 16 ) = ( RCONDE-TOL ) / ( RCDEIN+TOLIN )
         ELSE
            RESULT( 16 ) = ONE
         END IF
*
*        Compare condition numbers for right invariant subspace
*        taking its condition number into account
*
         IF( V.GT.RCONDV*RCONDE ) THEN
            TOL = RCONDV
         ELSE
            TOL = V / RCONDE
         END IF
         IF( V.GT.RCDVIN*RCDEIN ) THEN
            TOLIN = RCDVIN
         ELSE
            TOLIN = V / RCDEIN
         END IF
         TOL = MAX( TOL, SMLNUM / EPS )
         TOLIN = MAX( TOLIN, SMLNUM / EPS )
         IF( EPS*( RCDVIN-TOLIN ).GT.RCONDV+TOL ) THEN
            RESULT( 17 ) = ULPINV
         ELSE IF( RCDVIN-TOLIN.GT.RCONDV+TOL ) THEN
            RESULT( 17 ) = ( RCDVIN-TOLIN ) / ( RCONDV+TOL )
         ELSE IF( RCDVIN+TOLIN.LT.EPS*( RCONDV-TOL ) ) THEN
            RESULT( 17 ) = ULPINV
         ELSE IF( RCDVIN+TOLIN.LT.RCONDV-TOL ) THEN
            RESULT( 17 ) = ( RCONDV-TOL ) / ( RCDVIN+TOLIN )
         ELSE
            RESULT( 17 ) = ONE
         END IF
*
  270    CONTINUE
*
      END IF
*
 9999 FORMAT( ' ZGET24: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', INPUT EXAMPLE NUMBER = ', I4 )
 9998 FORMAT( ' ZGET24: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', ISEED=(', 3( I5, ',' ), I5, ')' )
*
      RETURN
*
*     End of ZGET24
*
      END
