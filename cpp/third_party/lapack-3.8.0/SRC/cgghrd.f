*> \brief \b CGGHRD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CGGHRD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgghrd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgghrd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgghrd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGGHRD( COMPQ, COMPZ, N, ILO, IHI, A, LDA, B, LDB, Q,
*                          LDQ, Z, LDZ, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          COMPQ, COMPZ
*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDQ, LDZ, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
*      $                   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGGHRD reduces a pair of complex matrices (A,B) to generalized upper
*> Hessenberg form using unitary transformations, where A is a
*> general matrix and B is upper triangular.  The form of the generalized
*> eigenvalue problem is
*>    A*x = lambda*B*x,
*> and B is typically made upper triangular by computing its QR
*> factorization and moving the unitary matrix Q to the left side
*> of the equation.
*>
*> This subroutine simultaneously reduces A to a Hessenberg matrix H:
*>    Q**H*A*Z = H
*> and transforms B to another upper triangular matrix T:
*>    Q**H*B*Z = T
*> in order to reduce the problem to its standard form
*>    H*y = lambda*T*y
*> where y = Z**H*x.
*>
*> The unitary matrices Q and Z are determined as products of Givens
*> rotations.  They may either be formed explicitly, or they may be
*> postmultiplied into input matrices Q1 and Z1, so that
*>      Q1 * A * Z1**H = (Q1*Q) * H * (Z1*Z)**H
*>      Q1 * B * Z1**H = (Q1*Q) * T * (Z1*Z)**H
*> If Q1 is the unitary matrix from the QR factorization of B in the
*> original equation A*x = lambda*B*x, then CGGHRD reduces the original
*> problem to generalized Hessenberg form.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] COMPQ
*> \verbatim
*>          COMPQ is CHARACTER*1
*>          = 'N': do not compute Q;
*>          = 'I': Q is initialized to the unit matrix, and the
*>                 unitary matrix Q is returned;
*>          = 'V': Q must contain a unitary matrix Q1 on entry,
*>                 and the product Q1*Q is returned.
*> \endverbatim
*>
*> \param[in] COMPZ
*> \verbatim
*>          COMPZ is CHARACTER*1
*>          = 'N': do not compute Z;
*>          = 'I': Z is initialized to the unit matrix, and the
*>                 unitary matrix Z is returned;
*>          = 'V': Z must contain a unitary matrix Z1 on entry,
*>                 and the product Z1*Z is returned.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[in] IHI
*> \verbatim
*>          IHI is INTEGER
*>
*>          ILO and IHI mark the rows and columns of A which are to be
*>          reduced.  It is assumed that A is already upper triangular
*>          in rows and columns 1:ILO-1 and IHI+1:N.  ILO and IHI are
*>          normally set by a previous call to CGGBAL; otherwise they
*>          should be set to 1 and N respectively.
*>          1 <= ILO <= IHI <= N, if N > 0; ILO=1 and IHI=0, if N=0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA, N)
*>          On entry, the N-by-N general matrix to be reduced.
*>          On exit, the upper triangle and the first subdiagonal of A
*>          are overwritten with the upper Hessenberg matrix H, and the
*>          rest is set to zero.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB, N)
*>          On entry, the N-by-N upper triangular matrix B.
*>          On exit, the upper triangular matrix T = Q**H B Z.  The
*>          elements below the diagonal are set to zero.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX array, dimension (LDQ, N)
*>          On entry, if COMPQ = 'V', the unitary matrix Q1, typically
*>          from the QR factorization of B.
*>          On exit, if COMPQ='I', the unitary matrix Q, and if
*>          COMPQ = 'V', the product Q1*Q.
*>          Not referenced if COMPQ='N'.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.
*>          LDQ >= N if COMPQ='V' or 'I'; LDQ >= 1 otherwise.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is COMPLEX array, dimension (LDZ, N)
*>          On entry, if COMPZ = 'V', the unitary matrix Z1.
*>          On exit, if COMPZ='I', the unitary matrix Z, and if
*>          COMPZ = 'V', the product Z1*Z.
*>          Not referenced if COMPZ='N'.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.
*>          LDZ >= N if COMPZ='V' or 'I'; LDZ >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \ingroup complexOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  This routine reduces A to Hessenberg and B to triangular form by
*>  an unblocked reduction, as described in _Matrix_Computations_,
*>  by Golub and van Loan (Johns Hopkins Press).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE CGGHRD( COMPQ, COMPZ, N, ILO, IHI, A, LDA, B, LDB, Q,
     $                   LDQ, Z, LDZ, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          COMPQ, COMPZ
      INTEGER            IHI, ILO, INFO, LDA, LDB, LDQ, LDZ, N
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
     $                   Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            CONE, CZERO
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ),
     $                   CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            ILQ, ILZ
      INTEGER            ICOMPQ, ICOMPZ, JCOL, JROW
      REAL               C
      COMPLEX            CTEMP, S
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLARTG, CLASET, CROT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CONJG, MAX
*     ..
*     .. Executable Statements ..
*
*     Decode COMPQ
*
      IF( LSAME( COMPQ, 'N' ) ) THEN
         ILQ = .FALSE.
         ICOMPQ = 1
      ELSE IF( LSAME( COMPQ, 'V' ) ) THEN
         ILQ = .TRUE.
         ICOMPQ = 2
      ELSE IF( LSAME( COMPQ, 'I' ) ) THEN
         ILQ = .TRUE.
         ICOMPQ = 3
      ELSE
         ICOMPQ = 0
      END IF
*
*     Decode COMPZ
*
      IF( LSAME( COMPZ, 'N' ) ) THEN
         ILZ = .FALSE.
         ICOMPZ = 1
      ELSE IF( LSAME( COMPZ, 'V' ) ) THEN
         ILZ = .TRUE.
         ICOMPZ = 2
      ELSE IF( LSAME( COMPZ, 'I' ) ) THEN
         ILZ = .TRUE.
         ICOMPZ = 3
      ELSE
         ICOMPZ = 0
      END IF
*
*     Test the input parameters.
*
      INFO = 0
      IF( ICOMPQ.LE.0 ) THEN
         INFO = -1
      ELSE IF( ICOMPZ.LE.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( ILO.LT.1 ) THEN
         INFO = -4
      ELSE IF( IHI.GT.N .OR. IHI.LT.ILO-1 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( ( ILQ .AND. LDQ.LT.N ) .OR. LDQ.LT.1 ) THEN
         INFO = -11
      ELSE IF( ( ILZ .AND. LDZ.LT.N ) .OR. LDZ.LT.1 ) THEN
         INFO = -13
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CGGHRD', -INFO )
         RETURN
      END IF
*
*     Initialize Q and Z if desired.
*
      IF( ICOMPQ.EQ.3 )
     $   CALL CLASET( 'Full', N, N, CZERO, CONE, Q, LDQ )
      IF( ICOMPZ.EQ.3 )
     $   CALL CLASET( 'Full', N, N, CZERO, CONE, Z, LDZ )
*
*     Quick return if possible
*
      IF( N.LE.1 )
     $   RETURN
*
*     Zero out lower triangle of B
*
      DO 20 JCOL = 1, N - 1
         DO 10 JROW = JCOL + 1, N
            B( JROW, JCOL ) = CZERO
   10    CONTINUE
   20 CONTINUE
*
*     Reduce A and B
*
      DO 40 JCOL = ILO, IHI - 2
*
         DO 30 JROW = IHI, JCOL + 2, -1
*
*           Step 1: rotate rows JROW-1, JROW to kill A(JROW,JCOL)
*
            CTEMP = A( JROW-1, JCOL )
            CALL CLARTG( CTEMP, A( JROW, JCOL ), C, S,
     $                   A( JROW-1, JCOL ) )
            A( JROW, JCOL ) = CZERO
            CALL CROT( N-JCOL, A( JROW-1, JCOL+1 ), LDA,
     $                 A( JROW, JCOL+1 ), LDA, C, S )
            CALL CROT( N+2-JROW, B( JROW-1, JROW-1 ), LDB,
     $                 B( JROW, JROW-1 ), LDB, C, S )
            IF( ILQ )
     $         CALL CROT( N, Q( 1, JROW-1 ), 1, Q( 1, JROW ), 1, C,
     $                    CONJG( S ) )
*
*           Step 2: rotate columns JROW, JROW-1 to kill B(JROW,JROW-1)
*
            CTEMP = B( JROW, JROW )
            CALL CLARTG( CTEMP, B( JROW, JROW-1 ), C, S,
     $                   B( JROW, JROW ) )
            B( JROW, JROW-1 ) = CZERO
            CALL CROT( IHI, A( 1, JROW ), 1, A( 1, JROW-1 ), 1, C, S )
            CALL CROT( JROW-1, B( 1, JROW ), 1, B( 1, JROW-1 ), 1, C,
     $                 S )
            IF( ILZ )
     $         CALL CROT( N, Z( 1, JROW ), 1, Z( 1, JROW-1 ), 1, C, S )
   30    CONTINUE
   40 CONTINUE
*
      RETURN
*
*     End of CGGHRD
*
      END
