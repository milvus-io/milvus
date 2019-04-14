*> \brief \b DSYTRD_SY2SB
*
*  @generated from zhetrd_he2hb.f, fortran z -> d, Wed Dec  7 08:22:39 2016
*      
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at 
*            http://www.netlib.org/lapack/explore-html/ 
*
*> \htmlonly
*> Download DSYTRD_SY2SB + dependencies 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dsytrd.f"> 
*> [TGZ]</a> 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dsytrd.f"> 
*> [ZIP]</a> 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dsytrd.f"> 
*> [TXT]</a>
*> \endhtmlonly 
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSYTRD_SY2SB( UPLO, N, KD, A, LDA, AB, LDAB, TAU, 
*                              WORK, LWORK, INFO )
*
*       IMPLICIT NONE
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, LDAB, LWORK, N, KD
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), AB( LDAB, * ), 
*                          TAU( * ), WORK( * )
*       ..
*  
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSYTRD_SY2SB reduces a real symmetric matrix A to real symmetric
*> band-diagonal form AB by a orthogonal similarity transformation:
*> Q**T * A * Q = AB.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KD
*> \verbatim
*>          KD is INTEGER
*>          The number of superdiagonals of the reduced matrix if UPLO = 'U',
*>          or the number of subdiagonals if UPLO = 'L'.  KD >= 0.
*>          The reduced matrix is stored in the array AB.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          On entry, the symmetric matrix A.  If UPLO = 'U', the leading
*>          N-by-N upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading N-by-N lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.
*>          On exit, if UPLO = 'U', the diagonal and first superdiagonal
*>          of A are overwritten by the corresponding elements of the
*>          tridiagonal matrix T, and the elements above the first
*>          superdiagonal, with the array TAU, represent the orthogonal
*>          matrix Q as a product of elementary reflectors; if UPLO
*>          = 'L', the diagonal and first subdiagonal of A are over-
*>          written by the corresponding elements of the tridiagonal
*>          matrix T, and the elements below the first subdiagonal, with
*>          the array TAU, represent the orthogonal matrix Q as a product
*>          of elementary reflectors. See Further Details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] AB
*> \verbatim
*>          AB is DOUBLE PRECISION array, dimension (LDAB,N)
*>          On exit, the upper or lower triangle of the symmetric band
*>          matrix A, stored in the first KD+1 rows of the array.  The
*>          j-th column of A is stored in the j-th column of the array AB
*>          as follows:
*>          if UPLO = 'U', AB(kd+1+i-j,j) = A(i,j) for max(1,j-kd)<=i<=j;
*>          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=min(n,j+kd).
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array AB.  LDAB >= KD+1.
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is DOUBLE PRECISION array, dimension (N-KD)
*>          The scalar factors of the elementary reflectors (see Further
*>          Details).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*>          On exit, if INFO = 0, or if LWORK=-1, 
*>          WORK(1) returns the size of LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK which should be calculated
*>          by a workspace query. LWORK = MAX(1, LWORK_QUERY)
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*>          LWORK_QUERY = N*KD + N*max(KD,FACTOPTNB) + 2*KD*KD
*>          where FACTOPTNB is the blocking used by the QR or LQ
*>          algorithm, usually FACTOPTNB=128 is a good choice otherwise
*>          putting LWORK=-1 will provide the size of WORK.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \date November 2017
*
*> \ingroup doubleSYcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Implemented by Azzam Haidar.
*>
*>  All details are available on technical report, SC11, SC13 papers.
*>
*>  Azzam Haidar, Hatem Ltaief, and Jack Dongarra.
*>  Parallel reduction to condensed forms for symmetric eigenvalue problems
*>  using aggregated fine-grained and memory-aware kernels. In Proceedings
*>  of 2011 International Conference for High Performance Computing,
*>  Networking, Storage and Analysis (SC '11), New York, NY, USA,
*>  Article 8 , 11 pages.
*>  http://doi.acm.org/10.1145/2063384.2063394
*>
*>  A. Haidar, J. Kurzak, P. Luszczek, 2013.
*>  An improved parallel singular value algorithm and its implementation 
*>  for multicore hardware, In Proceedings of 2013 International Conference
*>  for High Performance Computing, Networking, Storage and Analysis (SC '13).
*>  Denver, Colorado, USA, 2013.
*>  Article 90, 12 pages.
*>  http://doi.acm.org/10.1145/2503210.2503292
*>
*>  A. Haidar, R. Solca, S. Tomov, T. Schulthess and J. Dongarra.
*>  A novel hybrid CPU-GPU generalized eigensolver for electronic structure 
*>  calculations based on fine-grained memory aware tasks.
*>  International Journal of High Performance Computing Applications.
*>  Volume 28 Issue 2, Pages 196-209, May 2014.
*>  http://hpc.sagepub.com/content/28/2/196 
*>
*> \endverbatim
*>
*> \verbatim
*>
*>  If UPLO = 'U', the matrix Q is represented as a product of elementary
*>  reflectors
*>
*>     Q = H(k)**T . . . H(2)**T H(1)**T, where k = n-kd.
*>
*>  Each H(i) has the form
*>
*>     H(i) = I - tau * v * v**T
*>
*>  where tau is a real scalar, and v is a real vector with
*>  v(1:i+kd-1) = 0 and v(i+kd) = 1; conjg(v(i+kd+1:n)) is stored on exit in
*>  A(i,i+kd+1:n), and tau in TAU(i).
*>
*>  If UPLO = 'L', the matrix Q is represented as a product of elementary
*>  reflectors
*>
*>     Q = H(1) H(2) . . . H(k), where k = n-kd.
*>
*>  Each H(i) has the form
*>
*>     H(i) = I - tau * v * v**T
*>
*>  where tau is a real scalar, and v is a real vector with
*>  v(kd+1:i) = 0 and v(i+kd+1) = 1; v(i+kd+2:n) is stored on exit in
*>  A(i+kd+2:n,i), and tau in TAU(i).
*>
*>  The contents of A on exit are illustrated by the following examples
*>  with n = 5:
*>
*>  if UPLO = 'U':                       if UPLO = 'L':
*>
*>    (  ab  ab/v1  v1      v1     v1    )              (  ab                            )
*>    (      ab     ab/v2   v2     v2    )              (  ab/v1  ab                     )
*>    (             ab      ab/v3  v3    )              (  v1     ab/v2  ab              )
*>    (                     ab     ab/v4 )              (  v1     v2     ab/v3  ab       )
*>    (                            ab    )              (  v1     v2     v3     ab/v4 ab )
*>
*>  where d and e denote diagonal and off-diagonal elements of T, and vi
*>  denotes an element of the vector defining H(i).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DSYTRD_SY2SB( UPLO, N, KD, A, LDA, AB, LDAB, TAU, 
     $                         WORK, LWORK, INFO )
*
      IMPLICIT NONE
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, LDAB, LWORK, N, KD
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), AB( LDAB, * ), 
     $                   TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   RONE
      DOUBLE PRECISION   ZERO, ONE, HALF
      PARAMETER          ( RONE = 1.0D+0,
     $                   ZERO = 0.0D+0,
     $                   ONE = 1.0D+0,
     $                   HALF = 0.5D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, UPPER
      INTEGER            I, J, IINFO, LWMIN, PN, PK, LK,
     $                   LDT, LDW, LDS2, LDS1, 
     $                   LS2, LS1, LW, LT,
     $                   TPOS, WPOS, S2POS, S1POS
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, DSYR2K, DSYMM, DGEMM, DCOPY,
     $                   DLARFT, DGELQF, DGEQRF, DLASET
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MIN, MAX
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV 
      EXTERNAL           LSAME, ILAENV
*     ..
*     .. Executable Statements ..
*
*     Determine the minimal workspace size required 
*     and test the input parameters
*
      INFO   = 0
      UPPER  = LSAME( UPLO, 'U' )
      LQUERY = ( LWORK.EQ.-1 )
      LWMIN  = ILAENV( 20, 'DSYTRD_SY2SB', '', N, KD, -1, -1 )
      
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( KD.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDAB.LT.MAX( 1, KD+1 ) ) THEN
         INFO = -7
      ELSE IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
         INFO = -10
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DSYTRD_SY2SB', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         WORK( 1 ) = LWMIN
         RETURN
      END IF
*
*     Quick return if possible        
*     Copy the upper/lower portion of A into AB 
*
      IF( N.LE.KD+1 ) THEN
          IF( UPPER ) THEN
              DO 100 I = 1, N
                  LK = MIN( KD+1, I )
                  CALL DCOPY( LK, A( I-LK+1, I ), 1, 
     $                            AB( KD+1-LK+1, I ), 1 )
  100         CONTINUE
          ELSE
              DO 110 I = 1, N
                  LK = MIN( KD+1, N-I+1 )
                  CALL DCOPY( LK, A( I, I ), 1, AB( 1, I ), 1 )
  110         CONTINUE
          ENDIF
          WORK( 1 ) = 1
          RETURN
      END IF
*
*     Determine the pointer position for the workspace
*      
      LDT    = KD
      LDS1   = KD
      LT     = LDT*KD
      LW     = N*KD
      LS1    = LDS1*KD
      LS2    = LWMIN - LT - LW - LS1
*      LS2 = N*MAX(KD,FACTOPTNB) 
      TPOS   = 1
      WPOS   = TPOS  + LT
      S1POS  = WPOS  + LW
      S2POS  = S1POS + LS1 
      IF( UPPER ) THEN
          LDW    = KD
          LDS2   = KD
      ELSE
          LDW    = N
          LDS2   = N
      ENDIF
*
*
*     Set the workspace of the triangular matrix T to zero once such a
*     way everytime T is generated the upper/lower portion will be always zero  
*   
      CALL DLASET( "A", LDT, KD, ZERO, ZERO, WORK( TPOS ), LDT )
*
      IF( UPPER ) THEN
          DO 10 I = 1, N - KD, KD
             PN = N-I-KD+1
             PK = MIN( N-I-KD+1, KD )
*        
*            Compute the LQ factorization of the current block
*        
             CALL DGELQF( KD, PN, A( I, I+KD ), LDA,
     $                    TAU( I ), WORK( S2POS ), LS2, IINFO )
*        
*            Copy the upper portion of A into AB
*        
             DO 20 J = I, I+PK-1
                LK = MIN( KD, N-J ) + 1
                CALL DCOPY( LK, A( J, J ), LDA, AB( KD+1, J ), LDAB-1 )
   20        CONTINUE
*                
             CALL DLASET( 'Lower', PK, PK, ZERO, ONE, 
     $                    A( I, I+KD ), LDA )
*        
*            Form the matrix T
*        
             CALL DLARFT( 'Forward', 'Rowwise', PN, PK,
     $                    A( I, I+KD ), LDA, TAU( I ), 
     $                    WORK( TPOS ), LDT )
*        
*            Compute W:
*             
             CALL DGEMM( 'Conjugate', 'No transpose', PK, PN, PK,
     $                   ONE,  WORK( TPOS ), LDT,
     $                         A( I, I+KD ), LDA,
     $                   ZERO, WORK( S2POS ), LDS2 )
*        
             CALL DSYMM( 'Right', UPLO, PK, PN,
     $                   ONE,  A( I+KD, I+KD ), LDA,
     $                         WORK( S2POS ), LDS2,
     $                   ZERO, WORK( WPOS ), LDW )
*        
             CALL DGEMM( 'No transpose', 'Conjugate', PK, PK, PN,
     $                   ONE,  WORK( WPOS ), LDW,
     $                         WORK( S2POS ), LDS2,
     $                   ZERO, WORK( S1POS ), LDS1 )
*        
             CALL DGEMM( 'No transpose', 'No transpose', PK, PN, PK,
     $                   -HALF, WORK( S1POS ), LDS1, 
     $                          A( I, I+KD ), LDA,
     $                   ONE,   WORK( WPOS ), LDW )
*             
*        
*            Update the unreduced submatrix A(i+kd:n,i+kd:n), using
*            an update of the form:  A := A - V'*W - W'*V
*        
             CALL DSYR2K( UPLO, 'Conjugate', PN, PK,
     $                    -ONE, A( I, I+KD ), LDA,
     $                          WORK( WPOS ), LDW,
     $                    RONE, A( I+KD, I+KD ), LDA )
   10     CONTINUE
*
*        Copy the upper band to AB which is the band storage matrix
*
         DO 30 J = N-KD+1, N
            LK = MIN(KD, N-J) + 1
            CALL DCOPY( LK, A( J, J ), LDA, AB( KD+1, J ), LDAB-1 )
   30    CONTINUE
*
      ELSE
*
*         Reduce the lower triangle of A to lower band matrix
*        
          DO 40 I = 1, N - KD, KD
             PN = N-I-KD+1
             PK = MIN( N-I-KD+1, KD )
*        
*            Compute the QR factorization of the current block
*        
             CALL DGEQRF( PN, KD, A( I+KD, I ), LDA,
     $                    TAU( I ), WORK( S2POS ), LS2, IINFO )
*        
*            Copy the upper portion of A into AB 
*        
             DO 50 J = I, I+PK-1
                LK = MIN( KD, N-J ) + 1
                CALL DCOPY( LK, A( J, J ), 1, AB( 1, J ), 1 )
   50        CONTINUE
*                
             CALL DLASET( 'Upper', PK, PK, ZERO, ONE, 
     $                    A( I+KD, I ), LDA )
*        
*            Form the matrix T
*        
             CALL DLARFT( 'Forward', 'Columnwise', PN, PK,
     $                    A( I+KD, I ), LDA, TAU( I ), 
     $                    WORK( TPOS ), LDT )
*        
*            Compute W:
*             
             CALL DGEMM( 'No transpose', 'No transpose', PN, PK, PK,
     $                   ONE, A( I+KD, I ), LDA,
     $                         WORK( TPOS ), LDT,
     $                   ZERO, WORK( S2POS ), LDS2 )
*        
             CALL DSYMM( 'Left', UPLO, PN, PK,
     $                   ONE, A( I+KD, I+KD ), LDA,
     $                         WORK( S2POS ), LDS2,
     $                   ZERO, WORK( WPOS ), LDW )
*        
             CALL DGEMM( 'Conjugate', 'No transpose', PK, PK, PN,
     $                   ONE, WORK( S2POS ), LDS2,
     $                         WORK( WPOS ), LDW,
     $                   ZERO, WORK( S1POS ), LDS1 )
*        
             CALL DGEMM( 'No transpose', 'No transpose', PN, PK, PK,
     $                   -HALF, A( I+KD, I ), LDA,
     $                         WORK( S1POS ), LDS1,
     $                   ONE, WORK( WPOS ), LDW )
*             
*        
*            Update the unreduced submatrix A(i+kd:n,i+kd:n), using
*            an update of the form:  A := A - V*W' - W*V'
*        
             CALL DSYR2K( UPLO, 'No transpose', PN, PK,
     $                    -ONE, A( I+KD, I ), LDA,
     $                           WORK( WPOS ), LDW,
     $                    RONE, A( I+KD, I+KD ), LDA )
*            ==================================================================
*            RESTORE A FOR COMPARISON AND CHECKING TO BE REMOVED
*             DO 45 J = I, I+PK-1
*                LK = MIN( KD, N-J ) + 1
*                CALL DCOPY( LK, AB( 1, J ), 1, A( J, J ), 1 )
*   45        CONTINUE
*            ==================================================================
   40     CONTINUE
*
*        Copy the lower band to AB which is the band storage matrix
*
         DO 60 J = N-KD+1, N
            LK = MIN(KD, N-J) + 1
            CALL DCOPY( LK, A( J, J ), 1, AB( 1, J ), 1 )
   60    CONTINUE

      END IF
*
      WORK( 1 ) = LWMIN
      RETURN
*
*     End of DSYTRD_SY2SB
*
      END
