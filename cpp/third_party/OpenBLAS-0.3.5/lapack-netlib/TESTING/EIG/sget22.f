*> \brief \b SGET22
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGET22( TRANSA, TRANSE, TRANSW, N, A, LDA, E, LDE, WR,
*                          WI, WORK, RESULT )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANSA, TRANSE, TRANSW
*       INTEGER            LDA, LDE, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), E( LDE, * ), RESULT( 2 ), WI( * ),
*      $                   WORK( * ), WR( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGET22 does an eigenvector check.
*>
*> The basic test is:
*>
*>    RESULT(1) = | A E  -  E W | / ( |A| |E| ulp )
*>
*> using the 1-norm.  It also tests the normalization of E:
*>
*>    RESULT(2) = max | m-norm(E(j)) - 1 | / ( n ulp )
*>                 j
*>
*> where E(j) is the j-th eigenvector, and m-norm is the max-norm of a
*> vector.  If an eigenvector is complex, as determined from WI(j)
*> nonzero, then the max-norm of the vector ( er + i*ei ) is the maximum
*> of
*>    |er(1)| + |ei(1)|, ... , |er(n)| + |ei(n)|
*>
*> W is a block diagonal matrix, with a 1 by 1 block for each real
*> eigenvalue and a 2 by 2 block for each complex conjugate pair.
*> If eigenvalues j and j+1 are a complex conjugate pair, so that
*> WR(j) = WR(j+1) = wr and WI(j) = - WI(j+1) = wi, then the 2 by 2
*> block corresponding to the pair will be:
*>
*>    (  wr  wi  )
*>    ( -wi  wr  )
*>
*> Such a block multiplying an n by 2 matrix ( ur ui ) on the right
*> will be the same as multiplying  ur + i*ui  by  wr + i*wi.
*>
*> To handle various schemes for storage of left eigenvectors, there are
*> options to use A-transpose instead of A, E-transpose instead of E,
*> and/or W-transpose instead of W.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANSA
*> \verbatim
*>          TRANSA is CHARACTER*1
*>          Specifies whether or not A is transposed.
*>          = 'N':  No transpose
*>          = 'T':  Transpose
*>          = 'C':  Conjugate transpose (= Transpose)
*> \endverbatim
*>
*> \param[in] TRANSE
*> \verbatim
*>          TRANSE is CHARACTER*1
*>          Specifies whether or not E is transposed.
*>          = 'N':  No transpose, eigenvectors are in columns of E
*>          = 'T':  Transpose, eigenvectors are in rows of E
*>          = 'C':  Conjugate transpose (= Transpose)
*> \endverbatim
*>
*> \param[in] TRANSW
*> \verbatim
*>          TRANSW is CHARACTER*1
*>          Specifies whether or not W is transposed.
*>          = 'N':  No transpose
*>          = 'T':  Transpose, use -WI(j) instead of WI(j)
*>          = 'C':  Conjugate transpose, use -WI(j) instead of WI(j)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          The matrix whose eigenvectors are in E.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is REAL array, dimension (LDE,N)
*>          The matrix of eigenvectors. If TRANSE = 'N', the eigenvectors
*>          are stored in the columns of E, if TRANSE = 'T' or 'C', the
*>          eigenvectors are stored in the rows of E.
*> \endverbatim
*>
*> \param[in] LDE
*> \verbatim
*>          LDE is INTEGER
*>          The leading dimension of the array E.  LDE >= max(1,N).
*> \endverbatim
*>
*> \param[in] WR
*> \verbatim
*>          WR is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[in] WI
*> \verbatim
*>          WI is REAL array, dimension (N)
*>
*>          The real and imaginary parts of the eigenvalues of A.
*>          Purely real eigenvalues are indicated by WI(j) = 0.
*>          Complex conjugate pairs are indicated by WR(j)=WR(j+1) and
*>          WI(j) = - WI(j+1) non-zero; the real part is assumed to be
*>          stored in the j-th row/column and the imaginary part in
*>          the (j+1)-th row/column.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (N*(N+1))
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL array, dimension (2)
*>          RESULT(1) = | A E  -  E W | / ( |A| |E| ulp )
*>          RESULT(2) = max | m-norm(E(j)) - 1 | / ( n ulp )
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SGET22( TRANSA, TRANSE, TRANSW, N, A, LDA, E, LDE, WR,
     $                   WI, WORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANSA, TRANSE, TRANSW
      INTEGER            LDA, LDE, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), E( LDE, * ), RESULT( 2 ), WI( * ),
     $                   WORK( * ), WR( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0, ONE = 1.0 )
*     ..
*     .. Local Scalars ..
      CHARACTER          NORMA, NORME
      INTEGER            IECOL, IEROW, INCE, IPAIR, ITRNSE, J, JCOL,
     $                   JVEC
      REAL               ANORM, ENORM, ENRMAX, ENRMIN, ERRNRM, TEMP1,
     $                   ULP, UNFL
*     ..
*     .. Local Arrays ..
      REAL               WMAT( 2, 2 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH, SLANGE
      EXTERNAL           LSAME, SLAMCH, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SAXPY, SGEMM, SLASET
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
*     Initialize RESULT (in case N=0)
*
      RESULT( 1 ) = ZERO
      RESULT( 2 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      UNFL = SLAMCH( 'Safe minimum' )
      ULP = SLAMCH( 'Precision' )
*
      ITRNSE = 0
      INCE = 1
      NORMA = 'O'
      NORME = 'O'
*
      IF( LSAME( TRANSA, 'T' ) .OR. LSAME( TRANSA, 'C' ) ) THEN
         NORMA = 'I'
      END IF
      IF( LSAME( TRANSE, 'T' ) .OR. LSAME( TRANSE, 'C' ) ) THEN
         NORME = 'I'
         ITRNSE = 1
         INCE = LDE
      END IF
*
*     Check normalization of E
*
      ENRMIN = ONE / ULP
      ENRMAX = ZERO
      IF( ITRNSE.EQ.0 ) THEN
*
*        Eigenvectors are column vectors.
*
         IPAIR = 0
         DO 30 JVEC = 1, N
            TEMP1 = ZERO
            IF( IPAIR.EQ.0 .AND. JVEC.LT.N .AND. WI( JVEC ).NE.ZERO )
     $         IPAIR = 1
            IF( IPAIR.EQ.1 ) THEN
*
*              Complex eigenvector
*
               DO 10 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( E( J, JVEC ) )+
     $                    ABS( E( J, JVEC+1 ) ) )
   10          CONTINUE
               ENRMIN = MIN( ENRMIN, TEMP1 )
               ENRMAX = MAX( ENRMAX, TEMP1 )
               IPAIR = 2
            ELSE IF( IPAIR.EQ.2 ) THEN
               IPAIR = 0
            ELSE
*
*              Real eigenvector
*
               DO 20 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( E( J, JVEC ) ) )
   20          CONTINUE
               ENRMIN = MIN( ENRMIN, TEMP1 )
               ENRMAX = MAX( ENRMAX, TEMP1 )
               IPAIR = 0
            END IF
   30    CONTINUE
*
      ELSE
*
*        Eigenvectors are row vectors.
*
         DO 40 JVEC = 1, N
            WORK( JVEC ) = ZERO
   40    CONTINUE
*
         DO 60 J = 1, N
            IPAIR = 0
            DO 50 JVEC = 1, N
               IF( IPAIR.EQ.0 .AND. JVEC.LT.N .AND. WI( JVEC ).NE.ZERO )
     $            IPAIR = 1
               IF( IPAIR.EQ.1 ) THEN
                  WORK( JVEC ) = MAX( WORK( JVEC ),
     $                           ABS( E( J, JVEC ) )+ABS( E( J,
     $                           JVEC+1 ) ) )
                  WORK( JVEC+1 ) = WORK( JVEC )
               ELSE IF( IPAIR.EQ.2 ) THEN
                  IPAIR = 0
               ELSE
                  WORK( JVEC ) = MAX( WORK( JVEC ),
     $                           ABS( E( J, JVEC ) ) )
                  IPAIR = 0
               END IF
   50       CONTINUE
   60    CONTINUE
*
         DO 70 JVEC = 1, N
            ENRMIN = MIN( ENRMIN, WORK( JVEC ) )
            ENRMAX = MAX( ENRMAX, WORK( JVEC ) )
   70    CONTINUE
      END IF
*
*     Norm of A:
*
      ANORM = MAX( SLANGE( NORMA, N, N, A, LDA, WORK ), UNFL )
*
*     Norm of E:
*
      ENORM = MAX( SLANGE( NORME, N, N, E, LDE, WORK ), ULP )
*
*     Norm of error:
*
*     Error =  AE - EW
*
      CALL SLASET( 'Full', N, N, ZERO, ZERO, WORK, N )
*
      IPAIR = 0
      IEROW = 1
      IECOL = 1
*
      DO 80 JCOL = 1, N
         IF( ITRNSE.EQ.1 ) THEN
            IEROW = JCOL
         ELSE
            IECOL = JCOL
         END IF
*
         IF( IPAIR.EQ.0 .AND. WI( JCOL ).NE.ZERO )
     $      IPAIR = 1
*
         IF( IPAIR.EQ.1 ) THEN
            WMAT( 1, 1 ) = WR( JCOL )
            WMAT( 2, 1 ) = -WI( JCOL )
            WMAT( 1, 2 ) = WI( JCOL )
            WMAT( 2, 2 ) = WR( JCOL )
            CALL SGEMM( TRANSE, TRANSW, N, 2, 2, ONE, E( IEROW, IECOL ),
     $                  LDE, WMAT, 2, ZERO, WORK( N*( JCOL-1 )+1 ), N )
            IPAIR = 2
         ELSE IF( IPAIR.EQ.2 ) THEN
            IPAIR = 0
*
         ELSE
*
            CALL SAXPY( N, WR( JCOL ), E( IEROW, IECOL ), INCE,
     $                  WORK( N*( JCOL-1 )+1 ), 1 )
            IPAIR = 0
         END IF
*
   80 CONTINUE
*
      CALL SGEMM( TRANSA, TRANSE, N, N, N, ONE, A, LDA, E, LDE, -ONE,
     $            WORK, N )
*
      ERRNRM = SLANGE( 'One', N, N, WORK, N, WORK( N*N+1 ) ) / ENORM
*
*     Compute RESULT(1) (avoiding under/overflow)
*
      IF( ANORM.GT.ERRNRM ) THEN
         RESULT( 1 ) = ( ERRNRM / ANORM ) / ULP
      ELSE
         IF( ANORM.LT.ONE ) THEN
            RESULT( 1 ) = ( MIN( ERRNRM, ANORM ) / ANORM ) / ULP
         ELSE
            RESULT( 1 ) = MIN( ERRNRM / ANORM, ONE ) / ULP
         END IF
      END IF
*
*     Compute RESULT(2) : the normalization error in E.
*
      RESULT( 2 ) = MAX( ABS( ENRMAX-ONE ), ABS( ENRMIN-ONE ) ) /
     $              ( REAL( N )*ULP )
*
      RETURN
*
*     End of SGET22
*
      END
