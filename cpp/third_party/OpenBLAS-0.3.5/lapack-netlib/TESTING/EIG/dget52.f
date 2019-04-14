*> \brief \b DGET52
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGET52( LEFT, N, A, LDA, B, LDB, E, LDE, ALPHAR,
*                          ALPHAI, BETA, WORK, RESULT )
*
*       .. Scalar Arguments ..
*       LOGICAL            LEFT
*       INTEGER            LDA, LDB, LDE, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
*      $                   B( LDB, * ), BETA( * ), E( LDE, * ),
*      $                   RESULT( 2 ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGET52  does an eigenvector check for the generalized eigenvalue
*> problem.
*>
*> The basic test for right eigenvectors is:
*>
*>                           | b(j) A E(j) -  a(j) B E(j) |
*>         RESULT(1) = max   -------------------------------
*>                      j    n ulp max( |b(j) A|, |a(j) B| )
*>
*> using the 1-norm.  Here, a(j)/b(j) = w is the j-th generalized
*> eigenvalue of A - w B, or, equivalently, b(j)/a(j) = m is the j-th
*> generalized eigenvalue of m A - B.
*>
*> For real eigenvalues, the test is straightforward.  For complex
*> eigenvalues, E(j) and a(j) are complex, represented by
*> Er(j) + i*Ei(j) and ar(j) + i*ai(j), resp., so the test for that
*> eigenvector becomes
*>
*>                 max( |Wr|, |Wi| )
*>     --------------------------------------------
*>     n ulp max( |b(j) A|, (|ar(j)|+|ai(j)|) |B| )
*>
*> where
*>
*>     Wr = b(j) A Er(j) - ar(j) B Er(j) + ai(j) B Ei(j)
*>
*>     Wi = b(j) A Ei(j) - ai(j) B Er(j) - ar(j) B Ei(j)
*>
*>                         T   T  _
*> For left eigenvectors, A , B , a, and b  are used.
*>
*> DGET52 also tests the normalization of E.  Each eigenvector is
*> supposed to be normalized so that the maximum "absolute value"
*> of its elements is 1, where in this case, "absolute value"
*> of a complex value x is  |Re(x)| + |Im(x)| ; let us call this
*> maximum "absolute value" norm of a vector v  M(v).
*> if a(j)=b(j)=0, then the eigenvector is set to be the jth coordinate
*> vector.  The normalization test is:
*>
*>         RESULT(2) =      max       | M(v(j)) - 1 | / ( n ulp )
*>                    eigenvectors v(j)
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] LEFT
*> \verbatim
*>          LEFT is LOGICAL
*>          =.TRUE.:  The eigenvectors in the columns of E are assumed
*>                    to be *left* eigenvectors.
*>          =.FALSE.: The eigenvectors in the columns of E are assumed
*>                    to be *right* eigenvectors.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrices.  If it is zero, DGET52 does
*>          nothing.  It must be at least zero.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA, N)
*>          The matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB, N)
*>          The matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (LDE, N)
*>          The matrix of eigenvectors.  It must be O( 1 ).  Complex
*>          eigenvalues and eigenvectors always come in pairs, the
*>          eigenvalue and its conjugate being stored in adjacent
*>          elements of ALPHAR, ALPHAI, and BETA.  Thus, if a(j)/b(j)
*>          and a(j+1)/b(j+1) are a complex conjugate pair of
*>          generalized eigenvalues, then E(,j) contains the real part
*>          of the eigenvector and E(,j+1) contains the imaginary part.
*>          Note that whether E(,j) is a real eigenvector or part of a
*>          complex one is specified by whether ALPHAI(j) is zero or not.
*> \endverbatim
*>
*> \param[in] LDE
*> \verbatim
*>          LDE is INTEGER
*>          The leading dimension of E.  It must be at least 1 and at
*>          least N.
*> \endverbatim
*>
*> \param[in] ALPHAR
*> \verbatim
*>          ALPHAR is DOUBLE PRECISION array, dimension (N)
*>          The real parts of the values a(j) as described above, which,
*>          along with b(j), define the generalized eigenvalues.
*>          Complex eigenvalues always come in complex conjugate pairs
*>          a(j)/b(j) and a(j+1)/b(j+1), which are stored in adjacent
*>          elements in ALPHAR, ALPHAI, and BETA.  Thus, if the j-th
*>          and (j+1)-st eigenvalues form a pair, ALPHAR(j+1)/BETA(j+1)
*>          is assumed to be equal to ALPHAR(j)/BETA(j).
*> \endverbatim
*>
*> \param[in] ALPHAI
*> \verbatim
*>          ALPHAI is DOUBLE PRECISION array, dimension (N)
*>          The imaginary parts of the values a(j) as described above,
*>          which, along with b(j), define the generalized eigenvalues.
*>          If ALPHAI(j)=0, then the eigenvalue is real, otherwise it
*>          is part of a complex conjugate pair.  Complex eigenvalues
*>          always come in complex conjugate pairs a(j)/b(j) and
*>          a(j+1)/b(j+1), which are stored in adjacent elements in
*>          ALPHAR, ALPHAI, and BETA.  Thus, if the j-th and (j+1)-st
*>          eigenvalues form a pair, ALPHAI(j+1)/BETA(j+1) is assumed to
*>          be equal to  -ALPHAI(j)/BETA(j).  Also, nonzero values in
*>          ALPHAI are assumed to always come in adjacent pairs.
*> \endverbatim
*>
*> \param[in] BETA
*> \verbatim
*>          BETA is DOUBLE PRECISION array, dimension (N)
*>          The values b(j) as described above, which, along with a(j),
*>          define the generalized eigenvalues.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (N**2+N)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (2)
*>          The values computed by the test described above.  If A E or
*>          B E is likely to overflow, then RESULT(1:2) is set to
*>          10 / ulp.
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
      SUBROUTINE DGET52( LEFT, N, A, LDA, B, LDB, E, LDE, ALPHAR,
     $                   ALPHAI, BETA, WORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            LEFT
      INTEGER            LDA, LDB, LDE, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
     $                   B( LDB, * ), BETA( * ), E( LDE, * ),
     $                   RESULT( 2 ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TEN = 10.0D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ILCPLX
      CHARACTER          NORMAB, TRANS
      INTEGER            J, JVEC
      DOUBLE PRECISION   ABMAX, ACOEF, ALFMAX, ANORM, BCOEFI, BCOEFR,
     $                   BETMAX, BNORM, ENORM, ENRMER, ERRNRM, SAFMAX,
     $                   SAFMIN, SALFI, SALFR, SBETA, SCALE, TEMP1, ULP
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX
*     ..
*     .. Executable Statements ..
*
      RESULT( 1 ) = ZERO
      RESULT( 2 ) = ZERO
      IF( N.LE.0 )
     $   RETURN
*
      SAFMIN = DLAMCH( 'Safe minimum' )
      SAFMAX = ONE / SAFMIN
      ULP = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
*
      IF( LEFT ) THEN
         TRANS = 'T'
         NORMAB = 'I'
      ELSE
         TRANS = 'N'
         NORMAB = 'O'
      END IF
*
*     Norm of A, B, and E:
*
      ANORM = MAX( DLANGE( NORMAB, N, N, A, LDA, WORK ), SAFMIN )
      BNORM = MAX( DLANGE( NORMAB, N, N, B, LDB, WORK ), SAFMIN )
      ENORM = MAX( DLANGE( 'O', N, N, E, LDE, WORK ), ULP )
      ALFMAX = SAFMAX / MAX( ONE, BNORM )
      BETMAX = SAFMAX / MAX( ONE, ANORM )
*
*     Compute error matrix.
*     Column i = ( b(i) A - a(i) B ) E(i) / max( |a(i) B| |b(i) A| )
*
      ILCPLX = .FALSE.
      DO 10 JVEC = 1, N
         IF( ILCPLX ) THEN
*
*           2nd Eigenvalue/-vector of pair -- do nothing
*
            ILCPLX = .FALSE.
         ELSE
            SALFR = ALPHAR( JVEC )
            SALFI = ALPHAI( JVEC )
            SBETA = BETA( JVEC )
            IF( SALFI.EQ.ZERO ) THEN
*
*              Real eigenvalue and -vector
*
               ABMAX = MAX( ABS( SALFR ), ABS( SBETA ) )
               IF( ABS( SALFR ).GT.ALFMAX .OR. ABS( SBETA ).GT.
     $             BETMAX .OR. ABMAX.LT.ONE ) THEN
                  SCALE = ONE / MAX( ABMAX, SAFMIN )
                  SALFR = SCALE*SALFR
                  SBETA = SCALE*SBETA
               END IF
               SCALE = ONE / MAX( ABS( SALFR )*BNORM,
     $                 ABS( SBETA )*ANORM, SAFMIN )
               ACOEF = SCALE*SBETA
               BCOEFR = SCALE*SALFR
               CALL DGEMV( TRANS, N, N, ACOEF, A, LDA, E( 1, JVEC ), 1,
     $                     ZERO, WORK( N*( JVEC-1 )+1 ), 1 )
               CALL DGEMV( TRANS, N, N, -BCOEFR, B, LDA, E( 1, JVEC ),
     $                     1, ONE, WORK( N*( JVEC-1 )+1 ), 1 )
            ELSE
*
*              Complex conjugate pair
*
               ILCPLX = .TRUE.
               IF( JVEC.EQ.N ) THEN
                  RESULT( 1 ) = TEN / ULP
                  RETURN
               END IF
               ABMAX = MAX( ABS( SALFR )+ABS( SALFI ), ABS( SBETA ) )
               IF( ABS( SALFR )+ABS( SALFI ).GT.ALFMAX .OR.
     $             ABS( SBETA ).GT.BETMAX .OR. ABMAX.LT.ONE ) THEN
                  SCALE = ONE / MAX( ABMAX, SAFMIN )
                  SALFR = SCALE*SALFR
                  SALFI = SCALE*SALFI
                  SBETA = SCALE*SBETA
               END IF
               SCALE = ONE / MAX( ( ABS( SALFR )+ABS( SALFI ) )*BNORM,
     $                 ABS( SBETA )*ANORM, SAFMIN )
               ACOEF = SCALE*SBETA
               BCOEFR = SCALE*SALFR
               BCOEFI = SCALE*SALFI
               IF( LEFT ) THEN
                  BCOEFI = -BCOEFI
               END IF
*
               CALL DGEMV( TRANS, N, N, ACOEF, A, LDA, E( 1, JVEC ), 1,
     $                     ZERO, WORK( N*( JVEC-1 )+1 ), 1 )
               CALL DGEMV( TRANS, N, N, -BCOEFR, B, LDA, E( 1, JVEC ),
     $                     1, ONE, WORK( N*( JVEC-1 )+1 ), 1 )
               CALL DGEMV( TRANS, N, N, BCOEFI, B, LDA, E( 1, JVEC+1 ),
     $                     1, ONE, WORK( N*( JVEC-1 )+1 ), 1 )
*
               CALL DGEMV( TRANS, N, N, ACOEF, A, LDA, E( 1, JVEC+1 ),
     $                     1, ZERO, WORK( N*JVEC+1 ), 1 )
               CALL DGEMV( TRANS, N, N, -BCOEFI, B, LDA, E( 1, JVEC ),
     $                     1, ONE, WORK( N*JVEC+1 ), 1 )
               CALL DGEMV( TRANS, N, N, -BCOEFR, B, LDA, E( 1, JVEC+1 ),
     $                     1, ONE, WORK( N*JVEC+1 ), 1 )
            END IF
         END IF
   10 CONTINUE
*
      ERRNRM = DLANGE( 'One', N, N, WORK, N, WORK( N**2+1 ) ) / ENORM
*
*     Compute RESULT(1)
*
      RESULT( 1 ) = ERRNRM / ULP
*
*     Normalization of E:
*
      ENRMER = ZERO
      ILCPLX = .FALSE.
      DO 40 JVEC = 1, N
         IF( ILCPLX ) THEN
            ILCPLX = .FALSE.
         ELSE
            TEMP1 = ZERO
            IF( ALPHAI( JVEC ).EQ.ZERO ) THEN
               DO 20 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( E( J, JVEC ) ) )
   20          CONTINUE
               ENRMER = MAX( ENRMER, TEMP1-ONE )
            ELSE
               ILCPLX = .TRUE.
               DO 30 J = 1, N
                  TEMP1 = MAX( TEMP1, ABS( E( J, JVEC ) )+
     $                    ABS( E( J, JVEC+1 ) ) )
   30          CONTINUE
               ENRMER = MAX( ENRMER, TEMP1-ONE )
            END IF
         END IF
   40 CONTINUE
*
*     Compute RESULT(2) : the normalization error in E.
*
      RESULT( 2 ) = ENRMER / ( DBLE( N )*ULP )
*
      RETURN
*
*     End of DGET52
*
      END
