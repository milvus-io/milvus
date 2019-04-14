*> \brief \b DGET23
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET23( COMP, BALANC, JTYPE, THRESH, ISEED, NOUNIT, N,
*                          A, LDA, H, WR, WI, WR1, WI1, VL, LDVL, VR,
*                          LDVR, LRE, LDLRE, RCONDV, RCNDV1, RCDVIN,
*                          RCONDE, RCNDE1, RCDEIN, SCALE, SCALE1, RESULT,
*                          WORK, LWORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       LOGICAL            COMP
*       CHARACTER          BALANC
*       INTEGER            INFO, JTYPE, LDA, LDLRE, LDVL, LDVR, LWORK, N,
*      $                   NOUNIT
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 ), IWORK( * )
*       DOUBLE PRECISION   A( LDA, * ), H( LDA, * ), LRE( LDLRE, * ),
*      $                   RCDEIN( * ), RCDVIN( * ), RCNDE1( * ),
*      $                   RCNDV1( * ), RCONDE( * ), RCONDV( * ),
*      $                   RESULT( 11 ), SCALE( * ), SCALE1( * ),
*      $                   VL( LDVL, * ), VR( LDVR, * ), WI( * ),
*      $                   WI1( * ), WORK( * ), WR( * ), WR1( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DGET23  checks the nonsymmetric eigenvalue problem driver SGEEVX.
*>    If COMP = .FALSE., the first 8 of the following tests will be
*>    performed on the input matrix A, and also test 9 if LWORK is
*>    sufficiently large.
*>    if COMP is .TRUE. all 11 tests will be performed.
*>
*>    (1)     | A * VR - VR * W | / ( n |A| ulp )
*>
*>      Here VR is the matrix of unit right eigenvectors.
*>      W is a block diagonal matrix, with a 1x1 block for each
*>      real eigenvalue and a 2x2 block for each complex conjugate
*>      pair.  If eigenvalues j and j+1 are a complex conjugate pair,
*>      so WR(j) = WR(j+1) = wr and WI(j) = - WI(j+1) = wi, then the
*>      2 x 2 block corresponding to the pair will be:
*>
*>              (  wr  wi  )
*>              ( -wi  wr  )
*>
*>      Such a block multiplying an n x 2 matrix  ( ur ui ) on the
*>      right will be the same as multiplying  ur + i*ui  by  wr + i*wi.
*>
*>    (2)     | A**H * VL - VL * W**H | / ( n |A| ulp )
*>
*>      Here VL is the matrix of unit left eigenvectors, A**H is the
*>      conjugate transpose of A, and W is as above.
*>
*>    (3)     | |VR(i)| - 1 | / ulp and largest component real
*>
*>      VR(i) denotes the i-th column of VR.
*>
*>    (4)     | |VL(i)| - 1 | / ulp and largest component real
*>
*>      VL(i) denotes the i-th column of VL.
*>
*>    (5)     0 if W(full) = W(partial), 1/ulp otherwise
*>
*>      W(full) denotes the eigenvalues computed when VR, VL, RCONDV
*>      and RCONDE are also computed, and W(partial) denotes the
*>      eigenvalues computed when only some of VR, VL, RCONDV, and
*>      RCONDE are computed.
*>
*>    (6)     0 if VR(full) = VR(partial), 1/ulp otherwise
*>
*>      VR(full) denotes the right eigenvectors computed when VL, RCONDV
*>      and RCONDE are computed, and VR(partial) denotes the result
*>      when only some of VL and RCONDV are computed.
*>
*>    (7)     0 if VL(full) = VL(partial), 1/ulp otherwise
*>
*>      VL(full) denotes the left eigenvectors computed when VR, RCONDV
*>      and RCONDE are computed, and VL(partial) denotes the result
*>      when only some of VR and RCONDV are computed.
*>
*>    (8)     0 if SCALE, ILO, IHI, ABNRM (full) =
*>                 SCALE, ILO, IHI, ABNRM (partial)
*>            1/ulp otherwise
*>
*>      SCALE, ILO, IHI and ABNRM describe how the matrix is balanced.
*>      (full) is when VR, VL, RCONDE and RCONDV are also computed, and
*>      (partial) is when some are not computed.
*>
*>    (9)     0 if RCONDV(full) = RCONDV(partial), 1/ulp otherwise
*>
*>      RCONDV(full) denotes the reciprocal condition numbers of the
*>      right eigenvectors computed when VR, VL and RCONDE are also
*>      computed. RCONDV(partial) denotes the reciprocal condition
*>      numbers when only some of VR, VL and RCONDE are computed.
*>
*>   (10)     |RCONDV - RCDVIN| / cond(RCONDV)
*>
*>      RCONDV is the reciprocal right eigenvector condition number
*>      computed by DGEEVX and RCDVIN (the precomputed true value)
*>      is supplied as input. cond(RCONDV) is the condition number of
*>      RCONDV, and takes errors in computing RCONDV into account, so
*>      that the resulting quantity should be O(ULP). cond(RCONDV) is
*>      essentially given by norm(A)/RCONDE.
*>
*>   (11)     |RCONDE - RCDEIN| / cond(RCONDE)
*>
*>      RCONDE is the reciprocal eigenvalue condition number
*>      computed by DGEEVX and RCDEIN (the precomputed true value)
*>      is supplied as input.  cond(RCONDE) is the condition number
*>      of RCONDE, and takes errors in computing RCONDE into account,
*>      so that the resulting quantity should be O(ULP). cond(RCONDE)
*>      is essentially given by norm(A)/RCONDV.
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
*> \param[in] BALANC
*> \verbatim
*>          BALANC is CHARACTER
*>          Describes the balancing option to be tested.
*>            = 'N' for no permuting or diagonal scaling
*>            = 'P' for permuting but no diagonal scaling
*>            = 'S' for no permuting but diagonal scaling
*>            = 'B' for permuting and diagonal scaling
*> \endverbatim
*>
*> \param[in] JTYPE
*> \verbatim
*>          JTYPE is INTEGER
*>          Type of input matrix. Used to label output if error occurs.
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
*> \param[in] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          If COMP = .FALSE., the random number generator seed
*>          used to produce matrix.
*>          If COMP = .TRUE., ISEED(1) = the number of the example.
*>          Used to label output if error occurs.
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
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
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
*>          H is DOUBLE PRECISION array, dimension (LDA,N)
*>          Another copy of the test matrix A, modified by DGEEVX.
*> \endverbatim
*>
*> \param[out] WR
*> \verbatim
*>          WR is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] WI
*> \verbatim
*>          WI is DOUBLE PRECISION array, dimension (N)
*>
*>          The real and imaginary parts of the eigenvalues of A.
*>          On exit, WR + WI*i are the eigenvalues of the matrix in A.
*> \endverbatim
*>
*> \param[out] WR1
*> \verbatim
*>          WR1 is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] WI1
*> \verbatim
*>          WI1 is DOUBLE PRECISION array, dimension (N)
*>
*>          Like WR, WI, these arrays contain the eigenvalues of A,
*>          but those computed when DGEEVX only computes a partial
*>          eigendecomposition, i.e. not the eigenvalues and left
*>          and right eigenvectors.
*> \endverbatim
*>
*> \param[out] VL
*> \verbatim
*>          VL is DOUBLE PRECISION array, dimension (LDVL,N)
*>          VL holds the computed left eigenvectors.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          Leading dimension of VL. Must be at least max(1,N).
*> \endverbatim
*>
*> \param[out] VR
*> \verbatim
*>          VR is DOUBLE PRECISION array, dimension (LDVR,N)
*>          VR holds the computed right eigenvectors.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          Leading dimension of VR. Must be at least max(1,N).
*> \endverbatim
*>
*> \param[out] LRE
*> \verbatim
*>          LRE is DOUBLE PRECISION array, dimension (LDLRE,N)
*>          LRE holds the computed right or left eigenvectors.
*> \endverbatim
*>
*> \param[in] LDLRE
*> \verbatim
*>          LDLRE is INTEGER
*>          Leading dimension of LRE. Must be at least max(1,N).
*> \endverbatim
*>
*> \param[out] RCONDV
*> \verbatim
*>          RCONDV is DOUBLE PRECISION array, dimension (N)
*>          RCONDV holds the computed reciprocal condition numbers
*>          for eigenvectors.
*> \endverbatim
*>
*> \param[out] RCNDV1
*> \verbatim
*>          RCNDV1 is DOUBLE PRECISION array, dimension (N)
*>          RCNDV1 holds more computed reciprocal condition numbers
*>          for eigenvectors.
*> \endverbatim
*>
*> \param[in] RCDVIN
*> \verbatim
*>          RCDVIN is DOUBLE PRECISION array, dimension (N)
*>          When COMP = .TRUE. RCDVIN holds the precomputed reciprocal
*>          condition numbers for eigenvectors to be compared with
*>          RCONDV.
*> \endverbatim
*>
*> \param[out] RCONDE
*> \verbatim
*>          RCONDE is DOUBLE PRECISION array, dimension (N)
*>          RCONDE holds the computed reciprocal condition numbers
*>          for eigenvalues.
*> \endverbatim
*>
*> \param[out] RCNDE1
*> \verbatim
*>          RCNDE1 is DOUBLE PRECISION array, dimension (N)
*>          RCNDE1 holds more computed reciprocal condition numbers
*>          for eigenvalues.
*> \endverbatim
*>
*> \param[in] RCDEIN
*> \verbatim
*>          RCDEIN is DOUBLE PRECISION array, dimension (N)
*>          When COMP = .TRUE. RCDEIN holds the precomputed reciprocal
*>          condition numbers for eigenvalues to be compared with
*>          RCONDE.
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is DOUBLE PRECISION array, dimension (N)
*>          Holds information describing balancing of matrix.
*> \endverbatim
*>
*> \param[out] SCALE1
*> \verbatim
*>          SCALE1 is DOUBLE PRECISION array, dimension (N)
*>          Holds information describing balancing of matrix.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (11)
*>          The values computed by the 11 tests described above.
*>          The values are currently limited to 1/ulp, to avoid
*>          overflow.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The number of entries in WORK.  This must be at least
*>          3*N, and 6*N+N**2 if tests 9, 10 or 11 are to be performed.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          If 0,  successful exit.
*>          If <0, input parameter -INFO had an incorrect value.
*>          If >0, DGEEVX returned an error code, the absolute
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DGET23( COMP, BALANC, JTYPE, THRESH, ISEED, NOUNIT, N,
     $                   A, LDA, H, WR, WI, WR1, WI1, VL, LDVL, VR,
     $                   LDVR, LRE, LDLRE, RCONDV, RCNDV1, RCDVIN,
     $                   RCONDE, RCNDE1, RCDEIN, SCALE, SCALE1, RESULT,
     $                   WORK, LWORK, IWORK, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            COMP
      CHARACTER          BALANC
      INTEGER            INFO, JTYPE, LDA, LDLRE, LDVL, LDVR, LWORK, N,
     $                   NOUNIT
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 ), IWORK( * )
      DOUBLE PRECISION   A( LDA, * ), H( LDA, * ), LRE( LDLRE, * ),
     $                   RCDEIN( * ), RCDVIN( * ), RCNDE1( * ),
     $                   RCNDV1( * ), RCONDE( * ), RCONDV( * ),
     $                   RESULT( 11 ), SCALE( * ), SCALE1( * ),
     $                   VL( LDVL, * ), VR( LDVR, * ), WI( * ),
     $                   WI1( * ), WORK( * ), WR( * ), WR1( * )
*     ..
*
*  =====================================================================
*
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TWO = 2.0D0 )
      DOUBLE PRECISION   EPSIN
      PARAMETER          ( EPSIN = 5.9605D-8 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BALOK, NOBAL
      CHARACTER          SENSE
      INTEGER            I, IHI, IHI1, IINFO, ILO, ILO1, ISENS, ISENSM,
     $                   J, JJ, KMIN
      DOUBLE PRECISION   ABNRM, ABNRM1, EPS, SMLNUM, TNRM, TOL, TOLIN,
     $                   ULP, ULPINV, V, VIMIN, VMAX, VMX, VRMIN, VRMX,
     $                   VTST
*     ..
*     .. Local Arrays ..
      CHARACTER          SENS( 2 )
      DOUBLE PRECISION   DUM( 1 ), RES( 2 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLAPY2, DNRM2
      EXTERNAL           LSAME, DLAMCH, DLAPY2, DNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEEVX, DGET22, DLACPY, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, MIN
*     ..
*     .. Data statements ..
      DATA               SENS / 'N', 'V' /
*     ..
*     .. Executable Statements ..
*
*     Check for errors
*
      NOBAL = LSAME( BALANC, 'N' )
      BALOK = NOBAL .OR. LSAME( BALANC, 'P' ) .OR.
     $        LSAME( BALANC, 'S' ) .OR. LSAME( BALANC, 'B' )
      INFO = 0
      IF( .NOT.BALOK ) THEN
         INFO = -2
      ELSE IF( THRESH.LT.ZERO ) THEN
         INFO = -4
      ELSE IF( NOUNIT.LE.0 ) THEN
         INFO = -6
      ELSE IF( N.LT.0 ) THEN
         INFO = -7
      ELSE IF( LDA.LT.1 .OR. LDA.LT.N ) THEN
         INFO = -9
      ELSE IF( LDVL.LT.1 .OR. LDVL.LT.N ) THEN
         INFO = -16
      ELSE IF( LDVR.LT.1 .OR. LDVR.LT.N ) THEN
         INFO = -18
      ELSE IF( LDLRE.LT.1 .OR. LDLRE.LT.N ) THEN
         INFO = -20
      ELSE IF( LWORK.LT.3*N .OR. ( COMP .AND. LWORK.LT.6*N+N*N ) ) THEN
         INFO = -31
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DGET23', -INFO )
         RETURN
      END IF
*
*     Quick return if nothing to do
*
      DO 10 I = 1, 11
         RESULT( I ) = -ONE
   10 CONTINUE
*
      IF( N.EQ.0 )
     $   RETURN
*
*     More Important constants
*
      ULP = DLAMCH( 'Precision' )
      SMLNUM = DLAMCH( 'S' )
      ULPINV = ONE / ULP
*
*     Compute eigenvalues and eigenvectors, and test them
*
      IF( LWORK.GE.6*N+N*N ) THEN
         SENSE = 'B'
         ISENSM = 2
      ELSE
         SENSE = 'E'
         ISENSM = 1
      END IF
      CALL DLACPY( 'F', N, N, A, LDA, H, LDA )
      CALL DGEEVX( BALANC, 'V', 'V', SENSE, N, H, LDA, WR, WI, VL, LDVL,
     $             VR, LDVR, ILO, IHI, SCALE, ABNRM, RCONDE, RCONDV,
     $             WORK, LWORK, IWORK, IINFO )
      IF( IINFO.NE.0 ) THEN
         RESULT( 1 ) = ULPINV
         IF( JTYPE.NE.22 ) THEN
            WRITE( NOUNIT, FMT = 9998 )'DGEEVX1', IINFO, N, JTYPE,
     $         BALANC, ISEED
         ELSE
            WRITE( NOUNIT, FMT = 9999 )'DGEEVX1', IINFO, N, ISEED( 1 )
         END IF
         INFO = ABS( IINFO )
         RETURN
      END IF
*
*     Do Test (1)
*
      CALL DGET22( 'N', 'N', 'N', N, A, LDA, VR, LDVR, WR, WI, WORK,
     $             RES )
      RESULT( 1 ) = RES( 1 )
*
*     Do Test (2)
*
      CALL DGET22( 'T', 'N', 'T', N, A, LDA, VL, LDVL, WR, WI, WORK,
     $             RES )
      RESULT( 2 ) = RES( 1 )
*
*     Do Test (3)
*
      DO 30 J = 1, N
         TNRM = ONE
         IF( WI( J ).EQ.ZERO ) THEN
            TNRM = DNRM2( N, VR( 1, J ), 1 )
         ELSE IF( WI( J ).GT.ZERO ) THEN
            TNRM = DLAPY2( DNRM2( N, VR( 1, J ), 1 ),
     $             DNRM2( N, VR( 1, J+1 ), 1 ) )
         END IF
         RESULT( 3 ) = MAX( RESULT( 3 ),
     $                 MIN( ULPINV, ABS( TNRM-ONE ) / ULP ) )
         IF( WI( J ).GT.ZERO ) THEN
            VMX = ZERO
            VRMX = ZERO
            DO 20 JJ = 1, N
               VTST = DLAPY2( VR( JJ, J ), VR( JJ, J+1 ) )
               IF( VTST.GT.VMX )
     $            VMX = VTST
               IF( VR( JJ, J+1 ).EQ.ZERO .AND. ABS( VR( JJ, J ) ).GT.
     $             VRMX )VRMX = ABS( VR( JJ, J ) )
   20       CONTINUE
            IF( VRMX / VMX.LT.ONE-TWO*ULP )
     $         RESULT( 3 ) = ULPINV
         END IF
   30 CONTINUE
*
*     Do Test (4)
*
      DO 50 J = 1, N
         TNRM = ONE
         IF( WI( J ).EQ.ZERO ) THEN
            TNRM = DNRM2( N, VL( 1, J ), 1 )
         ELSE IF( WI( J ).GT.ZERO ) THEN
            TNRM = DLAPY2( DNRM2( N, VL( 1, J ), 1 ),
     $             DNRM2( N, VL( 1, J+1 ), 1 ) )
         END IF
         RESULT( 4 ) = MAX( RESULT( 4 ),
     $                 MIN( ULPINV, ABS( TNRM-ONE ) / ULP ) )
         IF( WI( J ).GT.ZERO ) THEN
            VMX = ZERO
            VRMX = ZERO
            DO 40 JJ = 1, N
               VTST = DLAPY2( VL( JJ, J ), VL( JJ, J+1 ) )
               IF( VTST.GT.VMX )
     $            VMX = VTST
               IF( VL( JJ, J+1 ).EQ.ZERO .AND. ABS( VL( JJ, J ) ).GT.
     $             VRMX )VRMX = ABS( VL( JJ, J ) )
   40       CONTINUE
            IF( VRMX / VMX.LT.ONE-TWO*ULP )
     $         RESULT( 4 ) = ULPINV
         END IF
   50 CONTINUE
*
*     Test for all options of computing condition numbers
*
      DO 200 ISENS = 1, ISENSM
*
         SENSE = SENS( ISENS )
*
*        Compute eigenvalues only, and test them
*
         CALL DLACPY( 'F', N, N, A, LDA, H, LDA )
         CALL DGEEVX( BALANC, 'N', 'N', SENSE, N, H, LDA, WR1, WI1, DUM,
     $                1, DUM, 1, ILO1, IHI1, SCALE1, ABNRM1, RCNDE1,
     $                RCNDV1, WORK, LWORK, IWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 1 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'DGEEVX2', IINFO, N, JTYPE,
     $            BALANC, ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'DGEEVX2', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 190
         END IF
*
*        Do Test (5)
*
         DO 60 J = 1, N
            IF( WR( J ).NE.WR1( J ) .OR. WI( J ).NE.WI1( J ) )
     $         RESULT( 5 ) = ULPINV
   60    CONTINUE
*
*        Do Test (8)
*
         IF( .NOT.NOBAL ) THEN
            DO 70 J = 1, N
               IF( SCALE( J ).NE.SCALE1( J ) )
     $            RESULT( 8 ) = ULPINV
   70       CONTINUE
            IF( ILO.NE.ILO1 )
     $         RESULT( 8 ) = ULPINV
            IF( IHI.NE.IHI1 )
     $         RESULT( 8 ) = ULPINV
            IF( ABNRM.NE.ABNRM1 )
     $         RESULT( 8 ) = ULPINV
         END IF
*
*        Do Test (9)
*
         IF( ISENS.EQ.2 .AND. N.GT.1 ) THEN
            DO 80 J = 1, N
               IF( RCONDV( J ).NE.RCNDV1( J ) )
     $            RESULT( 9 ) = ULPINV
   80       CONTINUE
         END IF
*
*        Compute eigenvalues and right eigenvectors, and test them
*
         CALL DLACPY( 'F', N, N, A, LDA, H, LDA )
         CALL DGEEVX( BALANC, 'N', 'V', SENSE, N, H, LDA, WR1, WI1, DUM,
     $                1, LRE, LDLRE, ILO1, IHI1, SCALE1, ABNRM1, RCNDE1,
     $                RCNDV1, WORK, LWORK, IWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 1 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'DGEEVX3', IINFO, N, JTYPE,
     $            BALANC, ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'DGEEVX3', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 190
         END IF
*
*        Do Test (5) again
*
         DO 90 J = 1, N
            IF( WR( J ).NE.WR1( J ) .OR. WI( J ).NE.WI1( J ) )
     $         RESULT( 5 ) = ULPINV
   90    CONTINUE
*
*        Do Test (6)
*
         DO 110 J = 1, N
            DO 100 JJ = 1, N
               IF( VR( J, JJ ).NE.LRE( J, JJ ) )
     $            RESULT( 6 ) = ULPINV
  100       CONTINUE
  110    CONTINUE
*
*        Do Test (8) again
*
         IF( .NOT.NOBAL ) THEN
            DO 120 J = 1, N
               IF( SCALE( J ).NE.SCALE1( J ) )
     $            RESULT( 8 ) = ULPINV
  120       CONTINUE
            IF( ILO.NE.ILO1 )
     $         RESULT( 8 ) = ULPINV
            IF( IHI.NE.IHI1 )
     $         RESULT( 8 ) = ULPINV
            IF( ABNRM.NE.ABNRM1 )
     $         RESULT( 8 ) = ULPINV
         END IF
*
*        Do Test (9) again
*
         IF( ISENS.EQ.2 .AND. N.GT.1 ) THEN
            DO 130 J = 1, N
               IF( RCONDV( J ).NE.RCNDV1( J ) )
     $            RESULT( 9 ) = ULPINV
  130       CONTINUE
         END IF
*
*        Compute eigenvalues and left eigenvectors, and test them
*
         CALL DLACPY( 'F', N, N, A, LDA, H, LDA )
         CALL DGEEVX( BALANC, 'V', 'N', SENSE, N, H, LDA, WR1, WI1, LRE,
     $                LDLRE, DUM, 1, ILO1, IHI1, SCALE1, ABNRM1, RCNDE1,
     $                RCNDV1, WORK, LWORK, IWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 1 ) = ULPINV
            IF( JTYPE.NE.22 ) THEN
               WRITE( NOUNIT, FMT = 9998 )'DGEEVX4', IINFO, N, JTYPE,
     $            BALANC, ISEED
            ELSE
               WRITE( NOUNIT, FMT = 9999 )'DGEEVX4', IINFO, N,
     $            ISEED( 1 )
            END IF
            INFO = ABS( IINFO )
            GO TO 190
         END IF
*
*        Do Test (5) again
*
         DO 140 J = 1, N
            IF( WR( J ).NE.WR1( J ) .OR. WI( J ).NE.WI1( J ) )
     $         RESULT( 5 ) = ULPINV
  140    CONTINUE
*
*        Do Test (7)
*
         DO 160 J = 1, N
            DO 150 JJ = 1, N
               IF( VL( J, JJ ).NE.LRE( J, JJ ) )
     $            RESULT( 7 ) = ULPINV
  150       CONTINUE
  160    CONTINUE
*
*        Do Test (8) again
*
         IF( .NOT.NOBAL ) THEN
            DO 170 J = 1, N
               IF( SCALE( J ).NE.SCALE1( J ) )
     $            RESULT( 8 ) = ULPINV
  170       CONTINUE
            IF( ILO.NE.ILO1 )
     $         RESULT( 8 ) = ULPINV
            IF( IHI.NE.IHI1 )
     $         RESULT( 8 ) = ULPINV
            IF( ABNRM.NE.ABNRM1 )
     $         RESULT( 8 ) = ULPINV
         END IF
*
*        Do Test (9) again
*
         IF( ISENS.EQ.2 .AND. N.GT.1 ) THEN
            DO 180 J = 1, N
               IF( RCONDV( J ).NE.RCNDV1( J ) )
     $            RESULT( 9 ) = ULPINV
  180       CONTINUE
         END IF
*
  190    CONTINUE
*
  200 CONTINUE
*
*     If COMP, compare condition numbers to precomputed ones
*
      IF( COMP ) THEN
         CALL DLACPY( 'F', N, N, A, LDA, H, LDA )
         CALL DGEEVX( 'N', 'V', 'V', 'B', N, H, LDA, WR, WI, VL, LDVL,
     $                VR, LDVR, ILO, IHI, SCALE, ABNRM, RCONDE, RCONDV,
     $                WORK, LWORK, IWORK, IINFO )
         IF( IINFO.NE.0 ) THEN
            RESULT( 1 ) = ULPINV
            WRITE( NOUNIT, FMT = 9999 )'DGEEVX5', IINFO, N, ISEED( 1 )
            INFO = ABS( IINFO )
            GO TO 250
         END IF
*
*        Sort eigenvalues and condition numbers lexicographically
*        to compare with inputs
*
         DO 220 I = 1, N - 1
            KMIN = I
            VRMIN = WR( I )
            VIMIN = WI( I )
            DO 210 J = I + 1, N
               IF( WR( J ).LT.VRMIN ) THEN
                  KMIN = J
                  VRMIN = WR( J )
                  VIMIN = WI( J )
               END IF
  210       CONTINUE
            WR( KMIN ) = WR( I )
            WI( KMIN ) = WI( I )
            WR( I ) = VRMIN
            WI( I ) = VIMIN
            VRMIN = RCONDE( KMIN )
            RCONDE( KMIN ) = RCONDE( I )
            RCONDE( I ) = VRMIN
            VRMIN = RCONDV( KMIN )
            RCONDV( KMIN ) = RCONDV( I )
            RCONDV( I ) = VRMIN
  220    CONTINUE
*
*        Compare condition numbers for eigenvectors
*        taking their condition numbers into account
*
         RESULT( 10 ) = ZERO
         EPS = MAX( EPSIN, ULP )
         V = MAX( DBLE( N )*EPS*ABNRM, SMLNUM )
         IF( ABNRM.EQ.ZERO )
     $      V = ONE
         DO 230 I = 1, N
            IF( V.GT.RCONDV( I )*RCONDE( I ) ) THEN
               TOL = RCONDV( I )
            ELSE
               TOL = V / RCONDE( I )
            END IF
            IF( V.GT.RCDVIN( I )*RCDEIN( I ) ) THEN
               TOLIN = RCDVIN( I )
            ELSE
               TOLIN = V / RCDEIN( I )
            END IF
            TOL = MAX( TOL, SMLNUM / EPS )
            TOLIN = MAX( TOLIN, SMLNUM / EPS )
            IF( EPS*( RCDVIN( I )-TOLIN ).GT.RCONDV( I )+TOL ) THEN
               VMAX = ONE / EPS
            ELSE IF( RCDVIN( I )-TOLIN.GT.RCONDV( I )+TOL ) THEN
               VMAX = ( RCDVIN( I )-TOLIN ) / ( RCONDV( I )+TOL )
            ELSE IF( RCDVIN( I )+TOLIN.LT.EPS*( RCONDV( I )-TOL ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( RCDVIN( I )+TOLIN.LT.RCONDV( I )-TOL ) THEN
               VMAX = ( RCONDV( I )-TOL ) / ( RCDVIN( I )+TOLIN )
            ELSE
               VMAX = ONE
            END IF
            RESULT( 10 ) = MAX( RESULT( 10 ), VMAX )
  230    CONTINUE
*
*        Compare condition numbers for eigenvalues
*        taking their condition numbers into account
*
         RESULT( 11 ) = ZERO
         DO 240 I = 1, N
            IF( V.GT.RCONDV( I ) ) THEN
               TOL = ONE
            ELSE
               TOL = V / RCONDV( I )
            END IF
            IF( V.GT.RCDVIN( I ) ) THEN
               TOLIN = ONE
            ELSE
               TOLIN = V / RCDVIN( I )
            END IF
            TOL = MAX( TOL, SMLNUM / EPS )
            TOLIN = MAX( TOLIN, SMLNUM / EPS )
            IF( EPS*( RCDEIN( I )-TOLIN ).GT.RCONDE( I )+TOL ) THEN
               VMAX = ONE / EPS
            ELSE IF( RCDEIN( I )-TOLIN.GT.RCONDE( I )+TOL ) THEN
               VMAX = ( RCDEIN( I )-TOLIN ) / ( RCONDE( I )+TOL )
            ELSE IF( RCDEIN( I )+TOLIN.LT.EPS*( RCONDE( I )-TOL ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( RCDEIN( I )+TOLIN.LT.RCONDE( I )-TOL ) THEN
               VMAX = ( RCONDE( I )-TOL ) / ( RCDEIN( I )+TOLIN )
            ELSE
               VMAX = ONE
            END IF
            RESULT( 11 ) = MAX( RESULT( 11 ), VMAX )
  240    CONTINUE
  250    CONTINUE
*
      END IF
*
 9999 FORMAT( ' DGET23: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', INPUT EXAMPLE NUMBER = ', I4 )
 9998 FORMAT( ' DGET23: ', A, ' returned INFO=', I6, '.', / 9X, 'N=',
     $      I6, ', JTYPE=', I6, ', BALANC = ', A, ', ISEED=(',
     $      3( I5, ',' ), I5, ')' )
*
      RETURN
*
*     End of DGET23
*
      END
