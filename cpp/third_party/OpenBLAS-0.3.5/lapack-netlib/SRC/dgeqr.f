*
*  Definition:
*  ===========
*
*       SUBROUTINE DGEQR( M, N, A, LDA, T, TSIZE, WORK, LWORK,
*                         INFO )
*
*       .. Scalar Arguments ..
*       INTEGER           INFO, LDA, M, N, TSIZE, LWORK
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION  A( LDA, * ), T( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> DGEQR computes a QR factorization of an M-by-N matrix A.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, the elements on and above the diagonal of the array
*>          contain the min(M,N)-by-N upper trapezoidal matrix R
*>          (R is upper triangular if M >= N);
*>          the elements below the diagonal are used to store part of the 
*>          data structure to represent Q.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is DOUBLE PRECISION array, dimension (MAX(5,TSIZE))
*>          On exit, if INFO = 0, T(1) returns optimal (or either minimal 
*>          or optimal, if query is assumed) TSIZE. See TSIZE for details.
*>          Remaining T contains part of the data structure used to represent Q.
*>          If one wants to apply or construct Q, then one needs to keep T 
*>          (in addition to A) and pass it to further subroutines.
*> \endverbatim
*>
*> \param[in] TSIZE
*> \verbatim
*>          TSIZE is INTEGER
*>          If TSIZE >= 5, the dimension of the array T.
*>          If TSIZE = -1 or -2, then a workspace query is assumed. The routine
*>          only calculates the sizes of the T and WORK arrays, returns these
*>          values as the first entries of the T and WORK arrays, and no error
*>          message related to T or WORK is issued by XERBLA.
*>          If TSIZE = -1, the routine calculates optimal size of T for the 
*>          optimum performance and returns this value in T(1).
*>          If TSIZE = -2, the routine calculates minimal size of T and 
*>          returns this value in T(1).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          (workspace) DOUBLE PRECISION array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) contains optimal (or either minimal
*>          or optimal, if query was assumed) LWORK.
*>          See LWORK for details.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If LWORK = -1 or -2, then a workspace query is assumed. The routine
*>          only calculates the sizes of the T and WORK arrays, returns these
*>          values as the first entries of the T and WORK arrays, and no error
*>          message related to T or WORK is issued by XERBLA.
*>          If LWORK = -1, the routine calculates optimal size of WORK for the
*>          optimal performance and returns this value in WORK(1).
*>          If LWORK = -2, the routine calculates minimal size of WORK and 
*>          returns this value in WORK(1).
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
*> \par Further Details
*  ====================
*>
*> \verbatim
*>
*> The goal of the interface is to give maximum freedom to the developers for
*> creating any QR factorization algorithm they wish. The triangular 
*> (trapezoidal) R has to be stored in the upper part of A. The lower part of A
*> and the array T can be used to store any relevant information for applying or
*> constructing the Q factor. The WORK array can safely be discarded after exit.
*>
*> Caution: One should not expect the sizes of T and WORK to be the same from one 
*> LAPACK implementation to the other, or even from one execution to the other.
*> A workspace query (for T and WORK) is needed at each execution. However, 
*> for a given execution, the size of T and WORK are fixed and will not change 
*> from one query to the next.
*>
*> \endverbatim
*>
*> \par Further Details particular to this LAPACK implementation:
*  ==============================================================
*>
*> \verbatim
*>
*> These details are particular for this LAPACK implementation. Users should not 
*> take them for granted. These details may change in the future, and are unlikely not
*> true for another LAPACK implementation. These details are relevant if one wants
*> to try to understand the code. They are not part of the interface.
*>
*> In this version,
*>
*>          T(2): row block size (MB)
*>          T(3): column block size (NB)
*>          T(6:TSIZE): data structure needed for Q, computed by
*>                           DLATSQR or DGEQRT
*>
*>  Depending on the matrix dimensions M and N, and row and column
*>  block sizes MB and NB returned by ILAENV, DGEQR will use either
*>  DLATSQR (if the matrix is tall-and-skinny) or DGEQRT to compute
*>  the QR factorization.
*>
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DGEQR( M, N, A, LDA, T, TSIZE, WORK, LWORK,
     $                  INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd. --
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, M, N, TSIZE, LWORK
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), T( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, LMINWS, MINT, MINW
      INTEGER            MB, NB, MINTSZ, NBLCKS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLATSQR, DGEQRT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, MOD
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      EXTERNAL           ILAENV
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
*
      LQUERY = ( TSIZE.EQ.-1 .OR. TSIZE.EQ.-2 .OR.
     $           LWORK.EQ.-1 .OR. LWORK.EQ.-2 )
*
      MINT = .FALSE.
      MINW = .FALSE.
      IF( TSIZE.EQ.-2 .OR. LWORK.EQ.-2 ) THEN
        IF( TSIZE.NE.-1 ) MINT = .TRUE.
        IF( LWORK.NE.-1 ) MINW = .TRUE.
      END IF
*
*     Determine the block size
*
      IF( MIN( M, N ).GT.0 ) THEN
        MB = ILAENV( 1, 'DGEQR ', ' ', M, N, 1, -1 )
        NB = ILAENV( 1, 'DGEQR ', ' ', M, N, 2, -1 )
      ELSE
        MB = M
        NB = 1
      END IF
      IF( MB.GT.M .OR. MB.LE.N ) MB = M
      IF( NB.GT.MIN( M, N ) .OR. NB.LT.1 ) NB = 1
      MINTSZ = N + 5
      IF( MB.GT.N .AND. M.GT.N ) THEN
        IF( MOD( M - N, MB - N ).EQ.0 ) THEN
          NBLCKS = ( M - N ) / ( MB - N )
        ELSE
          NBLCKS = ( M - N ) / ( MB - N ) + 1
        END IF
      ELSE
        NBLCKS = 1
      END IF
*
*     Determine if the workspace size satisfies minimal size
*
      LMINWS = .FALSE.
      IF( ( TSIZE.LT.MAX( 1, NB*N*NBLCKS + 5 ) .OR. LWORK.LT.NB*N )
     $    .AND. ( LWORK.GE.N ) .AND. ( TSIZE.GE.MINTSZ )
     $    .AND. ( .NOT.LQUERY ) ) THEN
        IF( TSIZE.LT.MAX( 1, NB*N*NBLCKS + 5 ) ) THEN
          LMINWS = .TRUE.
          NB = 1
          MB = M
        END IF
        IF( LWORK.LT.NB*N ) THEN
          LMINWS = .TRUE.
          NB = 1
        END IF
      END IF
*
      IF( M.LT.0 ) THEN
        INFO = -1
      ELSE IF( N.LT.0 ) THEN
        INFO = -2
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
        INFO = -4
      ELSE IF( TSIZE.LT.MAX( 1, NB*N*NBLCKS + 5 )
     $   .AND. ( .NOT.LQUERY ) .AND. ( .NOT.LMINWS ) ) THEN
        INFO = -6
      ELSE IF( ( LWORK.LT.MAX( 1, N*NB ) ) .AND. ( .NOT.LQUERY )
     $   .AND. ( .NOT.LMINWS ) ) THEN
        INFO = -8
      END IF
*
      IF( INFO.EQ.0 ) THEN
        IF( MINT ) THEN
          T( 1 ) = MINTSZ
        ELSE
          T( 1 ) = NB*N*NBLCKS + 5
        END IF
        T( 2 ) = MB
        T( 3 ) = NB
        IF( MINW ) THEN
          WORK( 1 ) = MAX( 1, N )
        ELSE
          WORK( 1 ) = MAX( 1, NB*N )
        END IF
      END IF
      IF( INFO.NE.0 ) THEN
        CALL XERBLA( 'DGEQR', -INFO )
        RETURN
      ELSE IF( LQUERY ) THEN
        RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN( M, N ).EQ.0 ) THEN
        RETURN
      END IF
*
*     The QR Decomposition
*
      IF( ( M.LE.N ) .OR. ( MB.LE.N ) .OR. ( MB.GE.M ) ) THEN
        CALL DGEQRT( M, N, NB, A, LDA, T( 6 ), NB, WORK, INFO )
      ELSE
        CALL DLATSQR( M, N, MB, NB, A, LDA, T( 6 ), NB, WORK,
     $                LWORK, INFO )
      END IF
*
      WORK( 1 ) = MAX( 1, NB*N )
*
      RETURN
*
*     End of DGEQR
*
      END
