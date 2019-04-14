*> \brief \b DBDSDC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DBDSDC + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dbdsdc.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dbdsdc.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dbdsdc.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DBDSDC( UPLO, COMPQ, N, D, E, U, LDU, VT, LDVT, Q, IQ,
*                          WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          COMPQ, UPLO
*       INTEGER            INFO, LDU, LDVT, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IQ( * ), IWORK( * )
*       DOUBLE PRECISION   D( * ), E( * ), Q( * ), U( LDU, * ),
*      $                   VT( LDVT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DBDSDC computes the singular value decomposition (SVD) of a real
*> N-by-N (upper or lower) bidiagonal matrix B:  B = U * S * VT,
*> using a divide and conquer method, where S is a diagonal matrix
*> with non-negative diagonal elements (the singular values of B), and
*> U and VT are orthogonal matrices of left and right singular vectors,
*> respectively. DBDSDC can be used to compute all singular values,
*> and optionally, singular vectors or singular vectors in compact form.
*>
*> This code makes very mild assumptions about floating point
*> arithmetic. It will work on machines with a guard digit in
*> add/subtract, or on those binary machines without guard digits
*> which subtract like the Cray X-MP, Cray Y-MP, Cray C-90, or Cray-2.
*> It could conceivably fail on hexadecimal or decimal machines
*> without guard digits, but we know of none.  See DLASD3 for details.
*>
*> The code currently calls DLASDQ if singular values only are desired.
*> However, it can be slightly modified to compute singular values
*> using the divide and conquer method.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  B is upper bidiagonal.
*>          = 'L':  B is lower bidiagonal.
*> \endverbatim
*>
*> \param[in] COMPQ
*> \verbatim
*>          COMPQ is CHARACTER*1
*>          Specifies whether singular vectors are to be computed
*>          as follows:
*>          = 'N':  Compute singular values only;
*>          = 'P':  Compute singular values and compute singular
*>                  vectors in compact form;
*>          = 'I':  Compute singular values and singular vectors.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix B.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          On entry, the n diagonal elements of the bidiagonal matrix B.
*>          On exit, if INFO=0, the singular values of B.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          On entry, the elements of E contain the offdiagonal
*>          elements of the bidiagonal matrix whose SVD is desired.
*>          On exit, E has been destroyed.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU,N)
*>          If  COMPQ = 'I', then:
*>             On exit, if INFO = 0, U contains the left singular vectors
*>             of the bidiagonal matrix.
*>          For other values of COMPQ, U is not referenced.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U.  LDU >= 1.
*>          If singular vectors are desired, then LDU >= max( 1, N ).
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension (LDVT,N)
*>          If  COMPQ = 'I', then:
*>             On exit, if INFO = 0, VT**T contains the right singular
*>             vectors of the bidiagonal matrix.
*>          For other values of COMPQ, VT is not referenced.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>          The leading dimension of the array VT.  LDVT >= 1.
*>          If singular vectors are desired, then LDVT >= max( 1, N ).
*> \endverbatim
*>
*> \param[out] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ)
*>          If  COMPQ = 'P', then:
*>             On exit, if INFO = 0, Q and IQ contain the left
*>             and right singular vectors in a compact form,
*>             requiring O(N log N) space instead of 2*N**2.
*>             In particular, Q contains all the DOUBLE PRECISION data in
*>             LDQ >= N*(11 + 2*SMLSIZ + 8*INT(LOG_2(N/(SMLSIZ+1))))
*>             words of memory, where SMLSIZ is returned by ILAENV and
*>             is equal to the maximum size of the subproblems at the
*>             bottom of the computation tree (usually about 25).
*>          For other values of COMPQ, Q is not referenced.
*> \endverbatim
*>
*> \param[out] IQ
*> \verbatim
*>          IQ is INTEGER array, dimension (LDIQ)
*>          If  COMPQ = 'P', then:
*>             On exit, if INFO = 0, Q and IQ contain the left
*>             and right singular vectors in a compact form,
*>             requiring O(N log N) space instead of 2*N**2.
*>             In particular, IQ contains all INTEGER data in
*>             LDIQ >= N*(3 + 3*INT(LOG_2(N/(SMLSIZ+1))))
*>             words of memory, where SMLSIZ is returned by ILAENV and
*>             is equal to the maximum size of the subproblems at the
*>             bottom of the computation tree (usually about 25).
*>          For other values of COMPQ, IQ is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (MAX(1,LWORK))
*>          If COMPQ = 'N' then LWORK >= (4 * N).
*>          If COMPQ = 'P' then LWORK >= (6 * N).
*>          If COMPQ = 'I' then LWORK >= (3 * N**2 + 4 * N).
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (8*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  The algorithm failed to compute a singular value.
*>                The update process of divide and conquer failed.
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
*> \ingroup auxOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Huan Ren, Computer Science Division, University of
*>     California at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE DBDSDC( UPLO, COMPQ, N, D, E, U, LDU, VT, LDVT, Q, IQ,
     $                   WORK, IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          COMPQ, UPLO
      INTEGER            INFO, LDU, LDVT, N
*     ..
*     .. Array Arguments ..
      INTEGER            IQ( * ), IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), Q( * ), U( LDU, * ),
     $                   VT( LDVT, * ), WORK( * )
*     ..
*
*  =====================================================================
*  Changed dimension statement in comment describing E from (N) to
*  (N-1).  Sven, 17 Feb 05.
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0, TWO = 2.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            DIFL, DIFR, GIVCOL, GIVNUM, GIVPTR, I, IC,
     $                   ICOMPQ, IERR, II, IS, IU, IUPLO, IVT, J, K, KK,
     $                   MLVL, NM1, NSIZE, PERM, POLES, QSTART, SMLSIZ,
     $                   SMLSZP, SQRE, START, WSTART, Z
      DOUBLE PRECISION   CS, EPS, ORGNRM, P, R, SN
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, DLANST
      EXTERNAL           LSAME, ILAENV, DLAMCH, DLANST
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLARTG, DLASCL, DLASD0, DLASDA, DLASDQ,
     $                   DLASET, DLASR, DSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, INT, LOG, SIGN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IUPLO = 0
      IF( LSAME( UPLO, 'U' ) )
     $   IUPLO = 1
      IF( LSAME( UPLO, 'L' ) )
     $   IUPLO = 2
      IF( LSAME( COMPQ, 'N' ) ) THEN
         ICOMPQ = 0
      ELSE IF( LSAME( COMPQ, 'P' ) ) THEN
         ICOMPQ = 1
      ELSE IF( LSAME( COMPQ, 'I' ) ) THEN
         ICOMPQ = 2
      ELSE
         ICOMPQ = -1
      END IF
      IF( IUPLO.EQ.0 ) THEN
         INFO = -1
      ELSE IF( ICOMPQ.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( ( LDU.LT.1 ) .OR. ( ( ICOMPQ.EQ.2 ) .AND. ( LDU.LT.
     $         N ) ) ) THEN
         INFO = -7
      ELSE IF( ( LDVT.LT.1 ) .OR. ( ( ICOMPQ.EQ.2 ) .AND. ( LDVT.LT.
     $         N ) ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DBDSDC', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
      SMLSIZ = ILAENV( 9, 'DBDSDC', ' ', 0, 0, 0, 0 )
      IF( N.EQ.1 ) THEN
         IF( ICOMPQ.EQ.1 ) THEN
            Q( 1 ) = SIGN( ONE, D( 1 ) )
            Q( 1+SMLSIZ*N ) = ONE
         ELSE IF( ICOMPQ.EQ.2 ) THEN
            U( 1, 1 ) = SIGN( ONE, D( 1 ) )
            VT( 1, 1 ) = ONE
         END IF
         D( 1 ) = ABS( D( 1 ) )
         RETURN
      END IF
      NM1 = N - 1
*
*     If matrix lower bidiagonal, rotate to be upper bidiagonal
*     by applying Givens rotations on the left
*
      WSTART = 1
      QSTART = 3
      IF( ICOMPQ.EQ.1 ) THEN
         CALL DCOPY( N, D, 1, Q( 1 ), 1 )
         CALL DCOPY( N-1, E, 1, Q( N+1 ), 1 )
      END IF
      IF( IUPLO.EQ.2 ) THEN
         QSTART = 5
         IF( ICOMPQ .EQ. 2 ) WSTART = 2*N - 1
         DO 10 I = 1, N - 1
            CALL DLARTG( D( I ), E( I ), CS, SN, R )
            D( I ) = R
            E( I ) = SN*D( I+1 )
            D( I+1 ) = CS*D( I+1 )
            IF( ICOMPQ.EQ.1 ) THEN
               Q( I+2*N ) = CS
               Q( I+3*N ) = SN
            ELSE IF( ICOMPQ.EQ.2 ) THEN
               WORK( I ) = CS
               WORK( NM1+I ) = -SN
            END IF
   10    CONTINUE
      END IF
*
*     If ICOMPQ = 0, use DLASDQ to compute the singular values.
*
      IF( ICOMPQ.EQ.0 ) THEN
*        Ignore WSTART, instead using WORK( 1 ), since the two vectors
*        for CS and -SN above are added only if ICOMPQ == 2,
*        and adding them exceeds documented WORK size of 4*n.
         CALL DLASDQ( 'U', 0, N, 0, 0, 0, D, E, VT, LDVT, U, LDU, U,
     $                LDU, WORK( 1 ), INFO )
         GO TO 40
      END IF
*
*     If N is smaller than the minimum divide size SMLSIZ, then solve
*     the problem with another solver.
*
      IF( N.LE.SMLSIZ ) THEN
         IF( ICOMPQ.EQ.2 ) THEN
            CALL DLASET( 'A', N, N, ZERO, ONE, U, LDU )
            CALL DLASET( 'A', N, N, ZERO, ONE, VT, LDVT )
            CALL DLASDQ( 'U', 0, N, N, N, 0, D, E, VT, LDVT, U, LDU, U,
     $                   LDU, WORK( WSTART ), INFO )
         ELSE IF( ICOMPQ.EQ.1 ) THEN
            IU = 1
            IVT = IU + N
            CALL DLASET( 'A', N, N, ZERO, ONE, Q( IU+( QSTART-1 )*N ),
     $                   N )
            CALL DLASET( 'A', N, N, ZERO, ONE, Q( IVT+( QSTART-1 )*N ),
     $                   N )
            CALL DLASDQ( 'U', 0, N, N, N, 0, D, E,
     $                   Q( IVT+( QSTART-1 )*N ), N,
     $                   Q( IU+( QSTART-1 )*N ), N,
     $                   Q( IU+( QSTART-1 )*N ), N, WORK( WSTART ),
     $                   INFO )
         END IF
         GO TO 40
      END IF
*
      IF( ICOMPQ.EQ.2 ) THEN
         CALL DLASET( 'A', N, N, ZERO, ONE, U, LDU )
         CALL DLASET( 'A', N, N, ZERO, ONE, VT, LDVT )
      END IF
*
*     Scale.
*
      ORGNRM = DLANST( 'M', N, D, E )
      IF( ORGNRM.EQ.ZERO )
     $   RETURN
      CALL DLASCL( 'G', 0, 0, ORGNRM, ONE, N, 1, D, N, IERR )
      CALL DLASCL( 'G', 0, 0, ORGNRM, ONE, NM1, 1, E, NM1, IERR )
*
      EPS = (0.9D+0)*DLAMCH( 'Epsilon' )
*
      MLVL = INT( LOG( DBLE( N ) / DBLE( SMLSIZ+1 ) ) / LOG( TWO ) ) + 1
      SMLSZP = SMLSIZ + 1
*
      IF( ICOMPQ.EQ.1 ) THEN
         IU = 1
         IVT = 1 + SMLSIZ
         DIFL = IVT + SMLSZP
         DIFR = DIFL + MLVL
         Z = DIFR + MLVL*2
         IC = Z + MLVL
         IS = IC + 1
         POLES = IS + 1
         GIVNUM = POLES + 2*MLVL
*
         K = 1
         GIVPTR = 2
         PERM = 3
         GIVCOL = PERM + MLVL
      END IF
*
      DO 20 I = 1, N
         IF( ABS( D( I ) ).LT.EPS ) THEN
            D( I ) = SIGN( EPS, D( I ) )
         END IF
   20 CONTINUE
*
      START = 1
      SQRE = 0
*
      DO 30 I = 1, NM1
         IF( ( ABS( E( I ) ).LT.EPS ) .OR. ( I.EQ.NM1 ) ) THEN
*
*           Subproblem found. First determine its size and then
*           apply divide and conquer on it.
*
            IF( I.LT.NM1 ) THEN
*
*              A subproblem with E(I) small for I < NM1.
*
               NSIZE = I - START + 1
            ELSE IF( ABS( E( I ) ).GE.EPS ) THEN
*
*              A subproblem with E(NM1) not too small but I = NM1.
*
               NSIZE = N - START + 1
            ELSE
*
*              A subproblem with E(NM1) small. This implies an
*              1-by-1 subproblem at D(N). Solve this 1-by-1 problem
*              first.
*
               NSIZE = I - START + 1
               IF( ICOMPQ.EQ.2 ) THEN
                  U( N, N ) = SIGN( ONE, D( N ) )
                  VT( N, N ) = ONE
               ELSE IF( ICOMPQ.EQ.1 ) THEN
                  Q( N+( QSTART-1 )*N ) = SIGN( ONE, D( N ) )
                  Q( N+( SMLSIZ+QSTART-1 )*N ) = ONE
               END IF
               D( N ) = ABS( D( N ) )
            END IF
            IF( ICOMPQ.EQ.2 ) THEN
               CALL DLASD0( NSIZE, SQRE, D( START ), E( START ),
     $                      U( START, START ), LDU, VT( START, START ),
     $                      LDVT, SMLSIZ, IWORK, WORK( WSTART ), INFO )
            ELSE
               CALL DLASDA( ICOMPQ, SMLSIZ, NSIZE, SQRE, D( START ),
     $                      E( START ), Q( START+( IU+QSTART-2 )*N ), N,
     $                      Q( START+( IVT+QSTART-2 )*N ),
     $                      IQ( START+K*N ), Q( START+( DIFL+QSTART-2 )*
     $                      N ), Q( START+( DIFR+QSTART-2 )*N ),
     $                      Q( START+( Z+QSTART-2 )*N ),
     $                      Q( START+( POLES+QSTART-2 )*N ),
     $                      IQ( START+GIVPTR*N ), IQ( START+GIVCOL*N ),
     $                      N, IQ( START+PERM*N ),
     $                      Q( START+( GIVNUM+QSTART-2 )*N ),
     $                      Q( START+( IC+QSTART-2 )*N ),
     $                      Q( START+( IS+QSTART-2 )*N ),
     $                      WORK( WSTART ), IWORK, INFO )
            END IF
            IF( INFO.NE.0 ) THEN
               RETURN
            END IF
            START = I + 1
         END IF
   30 CONTINUE
*
*     Unscale
*
      CALL DLASCL( 'G', 0, 0, ONE, ORGNRM, N, 1, D, N, IERR )
   40 CONTINUE
*
*     Use Selection Sort to minimize swaps of singular vectors
*
      DO 60 II = 2, N
         I = II - 1
         KK = I
         P = D( I )
         DO 50 J = II, N
            IF( D( J ).GT.P ) THEN
               KK = J
               P = D( J )
            END IF
   50    CONTINUE
         IF( KK.NE.I ) THEN
            D( KK ) = D( I )
            D( I ) = P
            IF( ICOMPQ.EQ.1 ) THEN
               IQ( I ) = KK
            ELSE IF( ICOMPQ.EQ.2 ) THEN
               CALL DSWAP( N, U( 1, I ), 1, U( 1, KK ), 1 )
               CALL DSWAP( N, VT( I, 1 ), LDVT, VT( KK, 1 ), LDVT )
            END IF
         ELSE IF( ICOMPQ.EQ.1 ) THEN
            IQ( I ) = I
         END IF
   60 CONTINUE
*
*     If ICOMPQ = 1, use IQ(N,1) as the indicator for UPLO
*
      IF( ICOMPQ.EQ.1 ) THEN
         IF( IUPLO.EQ.1 ) THEN
            IQ( N ) = 1
         ELSE
            IQ( N ) = 0
         END IF
      END IF
*
*     If B is lower bidiagonal, update U by those Givens rotations
*     which rotated B to be upper bidiagonal
*
      IF( ( IUPLO.EQ.2 ) .AND. ( ICOMPQ.EQ.2 ) )
     $   CALL DLASR( 'L', 'V', 'B', N, N, WORK( 1 ), WORK( N ), U, LDU )
*
      RETURN
*
*     End of DBDSDC
*
      END
