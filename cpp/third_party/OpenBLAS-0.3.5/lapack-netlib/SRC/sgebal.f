*> \brief \b SGEBAL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGEBAL + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgebal.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgebal.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgebal.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGEBAL( JOB, N, A, LDA, ILO, IHI, SCALE, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOB
*       INTEGER            IHI, ILO, INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), SCALE( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGEBAL balances a general real matrix A.  This involves, first,
*> permuting A by a similarity transformation to isolate eigenvalues
*> in the first 1 to ILO-1 and last IHI+1 to N elements on the
*> diagonal; and second, applying a diagonal similarity transformation
*> to rows and columns ILO to IHI to make the rows and columns as
*> close in norm as possible.  Both steps are optional.
*>
*> Balancing may reduce the 1-norm of the matrix, and improve the
*> accuracy of the computed eigenvalues and/or eigenvectors.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER*1
*>          Specifies the operations to be performed on A:
*>          = 'N':  none:  simply set ILO = 1, IHI = N, SCALE(I) = 1.0
*>                  for i = 1,...,N;
*>          = 'P':  permute only;
*>          = 'S':  scale only;
*>          = 'B':  both permute and scale.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the input matrix A.
*>          On exit,  A is overwritten by the balanced matrix.
*>          If JOB = 'N', A is not referenced.
*>          See Further Details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*> \param[out] IHI
*> \verbatim
*>          IHI is INTEGER
*>          ILO and IHI are set to integers such that on exit
*>          A(i,j) = 0 if i > j and j = 1,...,ILO-1 or I = IHI+1,...,N.
*>          If JOB = 'N' or 'S', ILO = 1 and IHI = N.
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is REAL array, dimension (N)
*>          Details of the permutations and scaling factors applied to
*>          A.  If P(j) is the index of the row and column interchanged
*>          with row and column j and D(j) is the scaling factor
*>          applied to row and column j, then
*>          SCALE(j) = P(j)    for j = 1,...,ILO-1
*>                   = D(j)    for j = ILO,...,IHI
*>                   = P(j)    for j = IHI+1,...,N.
*>          The order in which the interchanges are made is N to IHI+1,
*>          then 1 to ILO-1.
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
*> \ingroup realGEcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The permutations consist of row and column interchanges which put
*>  the matrix in the form
*>
*>             ( T1   X   Y  )
*>     P A P = (  0   B   Z  )
*>             (  0   0   T2 )
*>
*>  where T1 and T2 are upper triangular matrices whose eigenvalues lie
*>  along the diagonal.  The column indices ILO and IHI mark the starting
*>  and ending columns of the submatrix B. Balancing consists of applying
*>  a diagonal similarity transformation inv(D) * B * D to make the
*>  1-norms of each row of B and its corresponding column nearly equal.
*>  The output matrix is
*>
*>     ( T1     X*D          Y    )
*>     (  0  inv(D)*B*D  inv(D)*Z ).
*>     (  0      0           T2   )
*>
*>  Information about the permutations P and the diagonal matrix D is
*>  returned in the vector SCALE.
*>
*>  This subroutine is based on the EISPACK routine BALANC.
*>
*>  Modified by Tzu-Yi Chen, Computer Science Division, University of
*>    California at Berkeley, USA
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SGEBAL( JOB, N, A, LDA, ILO, IHI, SCALE, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOB
      INTEGER            IHI, ILO, INFO, LDA, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), SCALE( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      REAL               SCLFAC
      PARAMETER          ( SCLFAC = 2.0E+0 )
      REAL               FACTOR
      PARAMETER          ( FACTOR = 0.95E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOCONV
      INTEGER            I, ICA, IEXC, IRA, J, K, L, M
      REAL               C, CA, F, G, R, RA, S, SFMAX1, SFMAX2, SFMIN1,
     $                   SFMIN2
*     ..
*     .. External Functions ..
      LOGICAL            SISNAN, LSAME
      INTEGER            ISAMAX
      REAL               SLAMCH, SNRM2
      EXTERNAL           SISNAN, LSAME, ISAMAX, SLAMCH, SNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           SSCAL, SSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*
*     Test the input parameters
*
      INFO = 0
      IF( .NOT.LSAME( JOB, 'N' ) .AND. .NOT.LSAME( JOB, 'P' ) .AND.
     $    .NOT.LSAME( JOB, 'S' ) .AND. .NOT.LSAME( JOB, 'B' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGEBAL', -INFO )
         RETURN
      END IF
*
      K = 1
      L = N
*
      IF( N.EQ.0 )
     $   GO TO 210
*
      IF( LSAME( JOB, 'N' ) ) THEN
         DO 10 I = 1, N
            SCALE( I ) = ONE
   10    CONTINUE
         GO TO 210
      END IF
*
      IF( LSAME( JOB, 'S' ) )
     $   GO TO 120
*
*     Permutation to isolate eigenvalues if possible
*
      GO TO 50
*
*     Row and column exchange.
*
   20 CONTINUE
      SCALE( M ) = J
      IF( J.EQ.M )
     $   GO TO 30
*
      CALL SSWAP( L, A( 1, J ), 1, A( 1, M ), 1 )
      CALL SSWAP( N-K+1, A( J, K ), LDA, A( M, K ), LDA )
*
   30 CONTINUE
      GO TO ( 40, 80 )IEXC
*
*     Search for rows isolating an eigenvalue and push them down.
*
   40 CONTINUE
      IF( L.EQ.1 )
     $   GO TO 210
      L = L - 1
*
   50 CONTINUE
      DO 70 J = L, 1, -1
*
         DO 60 I = 1, L
            IF( I.EQ.J )
     $         GO TO 60
            IF( A( J, I ).NE.ZERO )
     $         GO TO 70
   60    CONTINUE
*
         M = L
         IEXC = 1
         GO TO 20
   70 CONTINUE
*
      GO TO 90
*
*     Search for columns isolating an eigenvalue and push them left.
*
   80 CONTINUE
      K = K + 1
*
   90 CONTINUE
      DO 110 J = K, L
*
         DO 100 I = K, L
            IF( I.EQ.J )
     $         GO TO 100
            IF( A( I, J ).NE.ZERO )
     $         GO TO 110
  100    CONTINUE
*
         M = K
         IEXC = 2
         GO TO 20
  110 CONTINUE
*
  120 CONTINUE
      DO 130 I = K, L
         SCALE( I ) = ONE
  130 CONTINUE
*
      IF( LSAME( JOB, 'P' ) )
     $   GO TO 210
*
*     Balance the submatrix in rows K to L.
*
*     Iterative loop for norm reduction
*
      SFMIN1 = SLAMCH( 'S' ) / SLAMCH( 'P' )
      SFMAX1 = ONE / SFMIN1
      SFMIN2 = SFMIN1*SCLFAC
      SFMAX2 = ONE / SFMIN2
  140 CONTINUE
      NOCONV = .FALSE.
*
      DO 200 I = K, L
*
         C = SNRM2( L-K+1, A( K, I ), 1 )
         R = SNRM2( L-K+1, A( I, K ), LDA )
         ICA = ISAMAX( L, A( 1, I ), 1 )
         CA = ABS( A( ICA, I ) )
         IRA = ISAMAX( N-K+1, A( I, K ), LDA )
         RA = ABS( A( I, IRA+K-1 ) )
*
*        Guard against zero C or R due to underflow.
*
         IF( C.EQ.ZERO .OR. R.EQ.ZERO )
     $      GO TO 200
         G = R / SCLFAC
         F = ONE
         S = C + R
  160    CONTINUE
         IF( C.GE.G .OR. MAX( F, C, CA ).GE.SFMAX2 .OR.
     $       MIN( R, G, RA ).LE.SFMIN2 )GO TO 170
         F = F*SCLFAC
         C = C*SCLFAC
         CA = CA*SCLFAC
         R = R / SCLFAC
         G = G / SCLFAC
         RA = RA / SCLFAC
         GO TO 160
*
  170    CONTINUE
         G = C / SCLFAC
  180    CONTINUE
         IF( G.LT.R .OR. MAX( R, RA ).GE.SFMAX2 .OR.
     $       MIN( F, C, G, CA ).LE.SFMIN2 )GO TO 190
            IF( SISNAN( C+F+CA+R+G+RA ) ) THEN
*
*           Exit if NaN to avoid infinite loop
*
            INFO = -3
            CALL XERBLA( 'SGEBAL', -INFO )
            RETURN
         END IF
         F = F / SCLFAC
         C = C / SCLFAC
         G = G / SCLFAC
         CA = CA / SCLFAC
         R = R*SCLFAC
         RA = RA*SCLFAC
         GO TO 180
*
*        Now balance.
*
  190    CONTINUE
         IF( ( C+R ).GE.FACTOR*S )
     $      GO TO 200
         IF( F.LT.ONE .AND. SCALE( I ).LT.ONE ) THEN
            IF( F*SCALE( I ).LE.SFMIN1 )
     $         GO TO 200
         END IF
         IF( F.GT.ONE .AND. SCALE( I ).GT.ONE ) THEN
            IF( SCALE( I ).GE.SFMAX1 / F )
     $         GO TO 200
         END IF
         G = ONE / F
         SCALE( I ) = SCALE( I )*F
         NOCONV = .TRUE.
*
         CALL SSCAL( N-K+1, G, A( I, K ), LDA )
         CALL SSCAL( L, F, A( 1, I ), 1 )
*
  200 CONTINUE
*
      IF( NOCONV )
     $   GO TO 140
*
  210 CONTINUE
      ILO = K
      IHI = L
*
      RETURN
*
*     End of SGEBAL
*
      END
