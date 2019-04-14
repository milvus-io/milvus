*> \brief \b ZLALSD uses the singular value decomposition of A to solve the least squares problem.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLALSD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlalsd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlalsd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlalsd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLALSD( UPLO, SMLSIZ, N, NRHS, D, E, B, LDB, RCOND,
*                          RANK, WORK, RWORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDB, N, NRHS, RANK, SMLSIZ
*       DOUBLE PRECISION   RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   D( * ), E( * ), RWORK( * )
*       COMPLEX*16         B( LDB, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLALSD uses the singular value decomposition of A to solve the least
*> squares problem of finding X to minimize the Euclidean norm of each
*> column of A*X-B, where A is N-by-N upper bidiagonal, and X and B
*> are N-by-NRHS. The solution X overwrites B.
*>
*> The singular values of A smaller than RCOND times the largest
*> singular value are treated as zero in solving the least squares
*> problem; in this case a minimum norm solution is returned.
*> The actual singular values are returned in D in ascending order.
*>
*> This code makes very mild assumptions about floating point
*> arithmetic. It will work on machines with a guard digit in
*> add/subtract, or on those binary machines without guard digits
*> which subtract like the Cray XMP, Cray YMP, Cray C 90, or Cray 2.
*> It could conceivably fail on hexadecimal or decimal machines
*> without guard digits, but we know of none.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>         = 'U': D and E define an upper bidiagonal matrix.
*>         = 'L': D and E define a  lower bidiagonal matrix.
*> \endverbatim
*>
*> \param[in] SMLSIZ
*> \verbatim
*>          SMLSIZ is INTEGER
*>         The maximum size of the subproblems at the bottom of the
*>         computation tree.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         The dimension of the  bidiagonal matrix.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>         The number of columns of B. NRHS must be at least 1.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>         On entry D contains the main diagonal of the bidiagonal
*>         matrix. On exit, if INFO = 0, D contains its singular values.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>         Contains the super-diagonal entries of the bidiagonal matrix.
*>         On exit, E has been destroyed.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,NRHS)
*>         On input, B contains the right hand sides of the least
*>         squares problem. On output, B contains the solution X.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>         The leading dimension of B in the calling subprogram.
*>         LDB must be at least max(1,N).
*> \endverbatim
*>
*> \param[in] RCOND
*> \verbatim
*>          RCOND is DOUBLE PRECISION
*>         The singular values of A less than or equal to RCOND times
*>         the largest singular value are treated as zero in solving
*>         the least squares problem. If RCOND is negative,
*>         machine precision is used instead.
*>         For example, if diag(S)*X=B were the least squares problem,
*>         where diag(S) is a diagonal matrix of singular values, the
*>         solution would be X(i) = B(i) / S(i) if S(i) is greater than
*>         RCOND*max(S), and X(i) = 0 if S(i) is less than or equal to
*>         RCOND*max(S).
*> \endverbatim
*>
*> \param[out] RANK
*> \verbatim
*>          RANK is INTEGER
*>         The number of singular values of A greater than RCOND times
*>         the largest singular value.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N * NRHS)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension at least
*>         (9*N + 2*N*SMLSIZ + 8*N*NLVL + 3*SMLSIZ*NRHS +
*>         MAX( (SMLSIZ+1)**2, N*(1+NRHS) + 2*NRHS ),
*>         where
*>         NLVL = MAX( 0, INT( LOG_2( MIN( M,N )/(SMLSIZ+1) ) ) + 1 )
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension at least
*>         (3*N*NLVL + 11*N).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>         = 0:  successful exit.
*>         < 0:  if INFO = -i, the i-th argument had an illegal value.
*>         > 0:  The algorithm failed to compute a singular value while
*>               working on the submatrix lying in rows and columns
*>               INFO/(N+1) through MOD(INFO,N+1).
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
*> \date June 2017
*
*> \ingroup complex16OTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Ren-Cang Li, Computer Science Division, University of
*>       California at Berkeley, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*
*  =====================================================================
      SUBROUTINE ZLALSD( UPLO, SMLSIZ, N, NRHS, D, E, B, LDB, RCOND,
     $                   RANK, WORK, RWORK, IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDB, N, NRHS, RANK, SMLSIZ
      DOUBLE PRECISION   RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), RWORK( * )
      COMPLEX*16         B( LDB, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TWO = 2.0D0 )
      COMPLEX*16         CZERO
      PARAMETER          ( CZERO = ( 0.0D0, 0.0D0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            BX, BXST, C, DIFL, DIFR, GIVCOL, GIVNUM,
     $                   GIVPTR, I, ICMPQ1, ICMPQ2, IRWB, IRWIB, IRWRB,
     $                   IRWU, IRWVT, IRWWRK, IWK, J, JCOL, JIMAG,
     $                   JREAL, JROW, K, NLVL, NM1, NRWORK, NSIZE, NSUB,
     $                   PERM, POLES, S, SIZEI, SMLSZP, SQRE, ST, ST1,
     $                   U, VT, Z
      DOUBLE PRECISION   CS, EPS, ORGNRM, RCND, R, SN, TOL
*     ..
*     .. External Functions ..
      INTEGER            IDAMAX
      DOUBLE PRECISION   DLAMCH, DLANST
      EXTERNAL           IDAMAX, DLAMCH, DLANST
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLARTG, DLASCL, DLASDA, DLASDQ, DLASET,
     $                   DLASRT, XERBLA, ZCOPY, ZDROT, ZLACPY, ZLALSA,
     $                   ZLASCL, ZLASET
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCMPLX, DIMAG, INT, LOG, SIGN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.1 ) THEN
         INFO = -4
      ELSE IF( ( LDB.LT.1 ) .OR. ( LDB.LT.N ) ) THEN
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZLALSD', -INFO )
         RETURN
      END IF
*
      EPS = DLAMCH( 'Epsilon' )
*
*     Set up the tolerance.
*
      IF( ( RCOND.LE.ZERO ) .OR. ( RCOND.GE.ONE ) ) THEN
         RCND = EPS
      ELSE
         RCND = RCOND
      END IF
*
      RANK = 0
*
*     Quick return if possible.
*
      IF( N.EQ.0 ) THEN
         RETURN
      ELSE IF( N.EQ.1 ) THEN
         IF( D( 1 ).EQ.ZERO ) THEN
            CALL ZLASET( 'A', 1, NRHS, CZERO, CZERO, B, LDB )
         ELSE
            RANK = 1
            CALL ZLASCL( 'G', 0, 0, D( 1 ), ONE, 1, NRHS, B, LDB, INFO )
            D( 1 ) = ABS( D( 1 ) )
         END IF
         RETURN
      END IF
*
*     Rotate the matrix if it is lower bidiagonal.
*
      IF( UPLO.EQ.'L' ) THEN
         DO 10 I = 1, N - 1
            CALL DLARTG( D( I ), E( I ), CS, SN, R )
            D( I ) = R
            E( I ) = SN*D( I+1 )
            D( I+1 ) = CS*D( I+1 )
            IF( NRHS.EQ.1 ) THEN
               CALL ZDROT( 1, B( I, 1 ), 1, B( I+1, 1 ), 1, CS, SN )
            ELSE
               RWORK( I*2-1 ) = CS
               RWORK( I*2 ) = SN
            END IF
   10    CONTINUE
         IF( NRHS.GT.1 ) THEN
            DO 30 I = 1, NRHS
               DO 20 J = 1, N - 1
                  CS = RWORK( J*2-1 )
                  SN = RWORK( J*2 )
                  CALL ZDROT( 1, B( J, I ), 1, B( J+1, I ), 1, CS, SN )
   20          CONTINUE
   30       CONTINUE
         END IF
      END IF
*
*     Scale.
*
      NM1 = N - 1
      ORGNRM = DLANST( 'M', N, D, E )
      IF( ORGNRM.EQ.ZERO ) THEN
         CALL ZLASET( 'A', N, NRHS, CZERO, CZERO, B, LDB )
         RETURN
      END IF
*
      CALL DLASCL( 'G', 0, 0, ORGNRM, ONE, N, 1, D, N, INFO )
      CALL DLASCL( 'G', 0, 0, ORGNRM, ONE, NM1, 1, E, NM1, INFO )
*
*     If N is smaller than the minimum divide size SMLSIZ, then solve
*     the problem with another solver.
*
      IF( N.LE.SMLSIZ ) THEN
         IRWU = 1
         IRWVT = IRWU + N*N
         IRWWRK = IRWVT + N*N
         IRWRB = IRWWRK
         IRWIB = IRWRB + N*NRHS
         IRWB = IRWIB + N*NRHS
         CALL DLASET( 'A', N, N, ZERO, ONE, RWORK( IRWU ), N )
         CALL DLASET( 'A', N, N, ZERO, ONE, RWORK( IRWVT ), N )
         CALL DLASDQ( 'U', 0, N, N, N, 0, D, E, RWORK( IRWVT ), N,
     $                RWORK( IRWU ), N, RWORK( IRWWRK ), 1,
     $                RWORK( IRWWRK ), INFO )
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
*
*        In the real version, B is passed to DLASDQ and multiplied
*        internally by Q**H. Here B is complex and that product is
*        computed below in two steps (real and imaginary parts).
*
         J = IRWB - 1
         DO 50 JCOL = 1, NRHS
            DO 40 JROW = 1, N
               J = J + 1
               RWORK( J ) = DBLE( B( JROW, JCOL ) )
   40       CONTINUE
   50    CONTINUE
         CALL DGEMM( 'T', 'N', N, NRHS, N, ONE, RWORK( IRWU ), N,
     $               RWORK( IRWB ), N, ZERO, RWORK( IRWRB ), N )
         J = IRWB - 1
         DO 70 JCOL = 1, NRHS
            DO 60 JROW = 1, N
               J = J + 1
               RWORK( J ) = DIMAG( B( JROW, JCOL ) )
   60       CONTINUE
   70    CONTINUE
         CALL DGEMM( 'T', 'N', N, NRHS, N, ONE, RWORK( IRWU ), N,
     $               RWORK( IRWB ), N, ZERO, RWORK( IRWIB ), N )
         JREAL = IRWRB - 1
         JIMAG = IRWIB - 1
         DO 90 JCOL = 1, NRHS
            DO 80 JROW = 1, N
               JREAL = JREAL + 1
               JIMAG = JIMAG + 1
               B( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                           RWORK( JIMAG ) )
   80       CONTINUE
   90    CONTINUE
*
         TOL = RCND*ABS( D( IDAMAX( N, D, 1 ) ) )
         DO 100 I = 1, N
            IF( D( I ).LE.TOL ) THEN
               CALL ZLASET( 'A', 1, NRHS, CZERO, CZERO, B( I, 1 ), LDB )
            ELSE
               CALL ZLASCL( 'G', 0, 0, D( I ), ONE, 1, NRHS, B( I, 1 ),
     $                      LDB, INFO )
               RANK = RANK + 1
            END IF
  100    CONTINUE
*
*        Since B is complex, the following call to DGEMM is performed
*        in two steps (real and imaginary parts). That is for V * B
*        (in the real version of the code V**H is stored in WORK).
*
*        CALL DGEMM( 'T', 'N', N, NRHS, N, ONE, WORK, N, B, LDB, ZERO,
*    $               WORK( NWORK ), N )
*
         J = IRWB - 1
         DO 120 JCOL = 1, NRHS
            DO 110 JROW = 1, N
               J = J + 1
               RWORK( J ) = DBLE( B( JROW, JCOL ) )
  110       CONTINUE
  120    CONTINUE
         CALL DGEMM( 'T', 'N', N, NRHS, N, ONE, RWORK( IRWVT ), N,
     $               RWORK( IRWB ), N, ZERO, RWORK( IRWRB ), N )
         J = IRWB - 1
         DO 140 JCOL = 1, NRHS
            DO 130 JROW = 1, N
               J = J + 1
               RWORK( J ) = DIMAG( B( JROW, JCOL ) )
  130       CONTINUE
  140    CONTINUE
         CALL DGEMM( 'T', 'N', N, NRHS, N, ONE, RWORK( IRWVT ), N,
     $               RWORK( IRWB ), N, ZERO, RWORK( IRWIB ), N )
         JREAL = IRWRB - 1
         JIMAG = IRWIB - 1
         DO 160 JCOL = 1, NRHS
            DO 150 JROW = 1, N
               JREAL = JREAL + 1
               JIMAG = JIMAG + 1
               B( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                           RWORK( JIMAG ) )
  150       CONTINUE
  160    CONTINUE
*
*        Unscale.
*
         CALL DLASCL( 'G', 0, 0, ONE, ORGNRM, N, 1, D, N, INFO )
         CALL DLASRT( 'D', N, D, INFO )
         CALL ZLASCL( 'G', 0, 0, ORGNRM, ONE, N, NRHS, B, LDB, INFO )
*
         RETURN
      END IF
*
*     Book-keeping and setting up some constants.
*
      NLVL = INT( LOG( DBLE( N ) / DBLE( SMLSIZ+1 ) ) / LOG( TWO ) ) + 1
*
      SMLSZP = SMLSIZ + 1
*
      U = 1
      VT = 1 + SMLSIZ*N
      DIFL = VT + SMLSZP*N
      DIFR = DIFL + NLVL*N
      Z = DIFR + NLVL*N*2
      C = Z + NLVL*N
      S = C + N
      POLES = S + N
      GIVNUM = POLES + 2*NLVL*N
      NRWORK = GIVNUM + 2*NLVL*N
      BX = 1
*
      IRWRB = NRWORK
      IRWIB = IRWRB + SMLSIZ*NRHS
      IRWB = IRWIB + SMLSIZ*NRHS
*
      SIZEI = 1 + N
      K = SIZEI + N
      GIVPTR = K + N
      PERM = GIVPTR + N
      GIVCOL = PERM + NLVL*N
      IWK = GIVCOL + NLVL*N*2
*
      ST = 1
      SQRE = 0
      ICMPQ1 = 1
      ICMPQ2 = 0
      NSUB = 0
*
      DO 170 I = 1, N
         IF( ABS( D( I ) ).LT.EPS ) THEN
            D( I ) = SIGN( EPS, D( I ) )
         END IF
  170 CONTINUE
*
      DO 240 I = 1, NM1
         IF( ( ABS( E( I ) ).LT.EPS ) .OR. ( I.EQ.NM1 ) ) THEN
            NSUB = NSUB + 1
            IWORK( NSUB ) = ST
*
*           Subproblem found. First determine its size and then
*           apply divide and conquer on it.
*
            IF( I.LT.NM1 ) THEN
*
*              A subproblem with E(I) small for I < NM1.
*
               NSIZE = I - ST + 1
               IWORK( SIZEI+NSUB-1 ) = NSIZE
            ELSE IF( ABS( E( I ) ).GE.EPS ) THEN
*
*              A subproblem with E(NM1) not too small but I = NM1.
*
               NSIZE = N - ST + 1
               IWORK( SIZEI+NSUB-1 ) = NSIZE
            ELSE
*
*              A subproblem with E(NM1) small. This implies an
*              1-by-1 subproblem at D(N), which is not solved
*              explicitly.
*
               NSIZE = I - ST + 1
               IWORK( SIZEI+NSUB-1 ) = NSIZE
               NSUB = NSUB + 1
               IWORK( NSUB ) = N
               IWORK( SIZEI+NSUB-1 ) = 1
               CALL ZCOPY( NRHS, B( N, 1 ), LDB, WORK( BX+NM1 ), N )
            END IF
            ST1 = ST - 1
            IF( NSIZE.EQ.1 ) THEN
*
*              This is a 1-by-1 subproblem and is not solved
*              explicitly.
*
               CALL ZCOPY( NRHS, B( ST, 1 ), LDB, WORK( BX+ST1 ), N )
            ELSE IF( NSIZE.LE.SMLSIZ ) THEN
*
*              This is a small subproblem and is solved by DLASDQ.
*
               CALL DLASET( 'A', NSIZE, NSIZE, ZERO, ONE,
     $                      RWORK( VT+ST1 ), N )
               CALL DLASET( 'A', NSIZE, NSIZE, ZERO, ONE,
     $                      RWORK( U+ST1 ), N )
               CALL DLASDQ( 'U', 0, NSIZE, NSIZE, NSIZE, 0, D( ST ),
     $                      E( ST ), RWORK( VT+ST1 ), N, RWORK( U+ST1 ),
     $                      N, RWORK( NRWORK ), 1, RWORK( NRWORK ),
     $                      INFO )
               IF( INFO.NE.0 ) THEN
                  RETURN
               END IF
*
*              In the real version, B is passed to DLASDQ and multiplied
*              internally by Q**H. Here B is complex and that product is
*              computed below in two steps (real and imaginary parts).
*
               J = IRWB - 1
               DO 190 JCOL = 1, NRHS
                  DO 180 JROW = ST, ST + NSIZE - 1
                     J = J + 1
                     RWORK( J ) = DBLE( B( JROW, JCOL ) )
  180             CONTINUE
  190          CONTINUE
               CALL DGEMM( 'T', 'N', NSIZE, NRHS, NSIZE, ONE,
     $                     RWORK( U+ST1 ), N, RWORK( IRWB ), NSIZE,
     $                     ZERO, RWORK( IRWRB ), NSIZE )
               J = IRWB - 1
               DO 210 JCOL = 1, NRHS
                  DO 200 JROW = ST, ST + NSIZE - 1
                     J = J + 1
                     RWORK( J ) = DIMAG( B( JROW, JCOL ) )
  200             CONTINUE
  210          CONTINUE
               CALL DGEMM( 'T', 'N', NSIZE, NRHS, NSIZE, ONE,
     $                     RWORK( U+ST1 ), N, RWORK( IRWB ), NSIZE,
     $                     ZERO, RWORK( IRWIB ), NSIZE )
               JREAL = IRWRB - 1
               JIMAG = IRWIB - 1
               DO 230 JCOL = 1, NRHS
                  DO 220 JROW = ST, ST + NSIZE - 1
                     JREAL = JREAL + 1
                     JIMAG = JIMAG + 1
                     B( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                                 RWORK( JIMAG ) )
  220             CONTINUE
  230          CONTINUE
*
               CALL ZLACPY( 'A', NSIZE, NRHS, B( ST, 1 ), LDB,
     $                      WORK( BX+ST1 ), N )
            ELSE
*
*              A large problem. Solve it using divide and conquer.
*
               CALL DLASDA( ICMPQ1, SMLSIZ, NSIZE, SQRE, D( ST ),
     $                      E( ST ), RWORK( U+ST1 ), N, RWORK( VT+ST1 ),
     $                      IWORK( K+ST1 ), RWORK( DIFL+ST1 ),
     $                      RWORK( DIFR+ST1 ), RWORK( Z+ST1 ),
     $                      RWORK( POLES+ST1 ), IWORK( GIVPTR+ST1 ),
     $                      IWORK( GIVCOL+ST1 ), N, IWORK( PERM+ST1 ),
     $                      RWORK( GIVNUM+ST1 ), RWORK( C+ST1 ),
     $                      RWORK( S+ST1 ), RWORK( NRWORK ),
     $                      IWORK( IWK ), INFO )
               IF( INFO.NE.0 ) THEN
                  RETURN
               END IF
               BXST = BX + ST1
               CALL ZLALSA( ICMPQ2, SMLSIZ, NSIZE, NRHS, B( ST, 1 ),
     $                      LDB, WORK( BXST ), N, RWORK( U+ST1 ), N,
     $                      RWORK( VT+ST1 ), IWORK( K+ST1 ),
     $                      RWORK( DIFL+ST1 ), RWORK( DIFR+ST1 ),
     $                      RWORK( Z+ST1 ), RWORK( POLES+ST1 ),
     $                      IWORK( GIVPTR+ST1 ), IWORK( GIVCOL+ST1 ), N,
     $                      IWORK( PERM+ST1 ), RWORK( GIVNUM+ST1 ),
     $                      RWORK( C+ST1 ), RWORK( S+ST1 ),
     $                      RWORK( NRWORK ), IWORK( IWK ), INFO )
               IF( INFO.NE.0 ) THEN
                  RETURN
               END IF
            END IF
            ST = I + 1
         END IF
  240 CONTINUE
*
*     Apply the singular values and treat the tiny ones as zero.
*
      TOL = RCND*ABS( D( IDAMAX( N, D, 1 ) ) )
*
      DO 250 I = 1, N
*
*        Some of the elements in D can be negative because 1-by-1
*        subproblems were not solved explicitly.
*
         IF( ABS( D( I ) ).LE.TOL ) THEN
            CALL ZLASET( 'A', 1, NRHS, CZERO, CZERO, WORK( BX+I-1 ), N )
         ELSE
            RANK = RANK + 1
            CALL ZLASCL( 'G', 0, 0, D( I ), ONE, 1, NRHS,
     $                   WORK( BX+I-1 ), N, INFO )
         END IF
         D( I ) = ABS( D( I ) )
  250 CONTINUE
*
*     Now apply back the right singular vectors.
*
      ICMPQ2 = 1
      DO 320 I = 1, NSUB
         ST = IWORK( I )
         ST1 = ST - 1
         NSIZE = IWORK( SIZEI+I-1 )
         BXST = BX + ST1
         IF( NSIZE.EQ.1 ) THEN
            CALL ZCOPY( NRHS, WORK( BXST ), N, B( ST, 1 ), LDB )
         ELSE IF( NSIZE.LE.SMLSIZ ) THEN
*
*           Since B and BX are complex, the following call to DGEMM
*           is performed in two steps (real and imaginary parts).
*
*           CALL DGEMM( 'T', 'N', NSIZE, NRHS, NSIZE, ONE,
*    $                  RWORK( VT+ST1 ), N, RWORK( BXST ), N, ZERO,
*    $                  B( ST, 1 ), LDB )
*
            J = BXST - N - 1
            JREAL = IRWB - 1
            DO 270 JCOL = 1, NRHS
               J = J + N
               DO 260 JROW = 1, NSIZE
                  JREAL = JREAL + 1
                  RWORK( JREAL ) = DBLE( WORK( J+JROW ) )
  260          CONTINUE
  270       CONTINUE
            CALL DGEMM( 'T', 'N', NSIZE, NRHS, NSIZE, ONE,
     $                  RWORK( VT+ST1 ), N, RWORK( IRWB ), NSIZE, ZERO,
     $                  RWORK( IRWRB ), NSIZE )
            J = BXST - N - 1
            JIMAG = IRWB - 1
            DO 290 JCOL = 1, NRHS
               J = J + N
               DO 280 JROW = 1, NSIZE
                  JIMAG = JIMAG + 1
                  RWORK( JIMAG ) = DIMAG( WORK( J+JROW ) )
  280          CONTINUE
  290       CONTINUE
            CALL DGEMM( 'T', 'N', NSIZE, NRHS, NSIZE, ONE,
     $                  RWORK( VT+ST1 ), N, RWORK( IRWB ), NSIZE, ZERO,
     $                  RWORK( IRWIB ), NSIZE )
            JREAL = IRWRB - 1
            JIMAG = IRWIB - 1
            DO 310 JCOL = 1, NRHS
               DO 300 JROW = ST, ST + NSIZE - 1
                  JREAL = JREAL + 1
                  JIMAG = JIMAG + 1
                  B( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                              RWORK( JIMAG ) )
  300          CONTINUE
  310       CONTINUE
         ELSE
            CALL ZLALSA( ICMPQ2, SMLSIZ, NSIZE, NRHS, WORK( BXST ), N,
     $                   B( ST, 1 ), LDB, RWORK( U+ST1 ), N,
     $                   RWORK( VT+ST1 ), IWORK( K+ST1 ),
     $                   RWORK( DIFL+ST1 ), RWORK( DIFR+ST1 ),
     $                   RWORK( Z+ST1 ), RWORK( POLES+ST1 ),
     $                   IWORK( GIVPTR+ST1 ), IWORK( GIVCOL+ST1 ), N,
     $                   IWORK( PERM+ST1 ), RWORK( GIVNUM+ST1 ),
     $                   RWORK( C+ST1 ), RWORK( S+ST1 ),
     $                   RWORK( NRWORK ), IWORK( IWK ), INFO )
            IF( INFO.NE.0 ) THEN
               RETURN
            END IF
         END IF
  320 CONTINUE
*
*     Unscale and sort the singular values.
*
      CALL DLASCL( 'G', 0, 0, ONE, ORGNRM, N, 1, D, N, INFO )
      CALL DLASRT( 'D', N, D, INFO )
      CALL ZLASCL( 'G', 0, 0, ORGNRM, ONE, N, NRHS, B, LDB, INFO )
*
      RETURN
*
*     End of ZLALSD
*
      END
