*> \brief \b DLAROT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAROT( LROWS, LLEFT, LRIGHT, NL, C, S, A, LDA, XLEFT,
*                          XRIGHT )
*
*       .. Scalar Arguments ..
*       LOGICAL            LLEFT, LRIGHT, LROWS
*       INTEGER            LDA, NL
*       DOUBLE PRECISION   C, S, XLEFT, XRIGHT
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DLAROT applies a (Givens) rotation to two adjacent rows or
*>    columns, where one element of the first and/or last column/row
*>    for use on matrices stored in some format other than GE, so
*>    that elements of the matrix may be used or modified for which
*>    no array element is provided.
*>
*>    One example is a symmetric matrix in SB format (bandwidth=4), for
*>    which UPLO='L':  Two adjacent rows will have the format:
*>
*>    row j:     C> C> C> C> C> .  .  .  .
*>    row j+1:      C> C> C> C> C> .  .  .  .
*>
*>    '*' indicates elements for which storage is provided,
*>    '.' indicates elements for which no storage is provided, but
*>    are not necessarily zero; their values are determined by
*>    symmetry.  ' ' indicates elements which are necessarily zero,
*>     and have no storage provided.
*>
*>    Those columns which have two '*'s can be handled by DROT.
*>    Those columns which have no '*'s can be ignored, since as long
*>    as the Givens rotations are carefully applied to preserve
*>    symmetry, their values are determined.
*>    Those columns which have one '*' have to be handled separately,
*>    by using separate variables "p" and "q":
*>
*>    row j:     C> C> C> C> C> p  .  .  .
*>    row j+1:   q  C> C> C> C> C> .  .  .  .
*>
*>    The element p would have to be set correctly, then that column
*>    is rotated, setting p to its new value.  The next call to
*>    DLAROT would rotate columns j and j+1, using p, and restore
*>    symmetry.  The element q would start out being zero, and be
*>    made non-zero by the rotation.  Later, rotations would presumably
*>    be chosen to zero q out.
*>
*>    Typical Calling Sequences: rotating the i-th and (i+1)-st rows.
*>    ------- ------- ---------
*>
*>      General dense matrix:
*>
*>              CALL DLAROT(.TRUE.,.FALSE.,.FALSE., N, C,S,
*>                      A(i,1),LDA, DUMMY, DUMMY)
*>
*>      General banded matrix in GB format:
*>
*>              j = MAX(1, i-KL )
*>              NL = MIN( N, i+KU+1 ) + 1-j
*>              CALL DLAROT( .TRUE., i-KL.GE.1, i+KU.LT.N, NL, C,S,
*>                      A(KU+i+1-j,j),LDA-1, XLEFT, XRIGHT )
*>
*>              [ note that i+1-j is just MIN(i,KL+1) ]
*>
*>      Symmetric banded matrix in SY format, bandwidth K,
*>      lower triangle only:
*>
*>              j = MAX(1, i-K )
*>              NL = MIN( K+1, i ) + 1
*>              CALL DLAROT( .TRUE., i-K.GE.1, .TRUE., NL, C,S,
*>                      A(i,j), LDA, XLEFT, XRIGHT )
*>
*>      Same, but upper triangle only:
*>
*>              NL = MIN( K+1, N-i ) + 1
*>              CALL DLAROT( .TRUE., .TRUE., i+K.LT.N, NL, C,S,
*>                      A(i,i), LDA, XLEFT, XRIGHT )
*>
*>      Symmetric banded matrix in SB format, bandwidth K,
*>      lower triangle only:
*>
*>              [ same as for SY, except:]
*>                  . . . .
*>                      A(i+1-j,j), LDA-1, XLEFT, XRIGHT )
*>
*>              [ note that i+1-j is just MIN(i,K+1) ]
*>
*>      Same, but upper triangle only:
*>                   . . .
*>                      A(K+1,i), LDA-1, XLEFT, XRIGHT )
*>
*>      Rotating columns is just the transpose of rotating rows, except
*>      for GB and SB: (rotating columns i and i+1)
*>
*>      GB:
*>              j = MAX(1, i-KU )
*>              NL = MIN( N, i+KL+1 ) + 1-j
*>              CALL DLAROT( .TRUE., i-KU.GE.1, i+KL.LT.N, NL, C,S,
*>                      A(KU+j+1-i,i),LDA-1, XTOP, XBOTTM )
*>
*>              [note that KU+j+1-i is just MAX(1,KU+2-i)]
*>
*>      SB: (upper triangle)
*>
*>                   . . . . . .
*>                      A(K+j+1-i,i),LDA-1, XTOP, XBOTTM )
*>
*>      SB: (lower triangle)
*>
*>                   . . . . . .
*>                      A(1,i),LDA-1, XTOP, XBOTTM )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  LROWS  - LOGICAL
*>           If .TRUE., then DLAROT will rotate two rows.  If .FALSE.,
*>           then it will rotate two columns.
*>           Not modified.
*>
*>  LLEFT  - LOGICAL
*>           If .TRUE., then XLEFT will be used instead of the
*>           corresponding element of A for the first element in the
*>           second row (if LROWS=.FALSE.) or column (if LROWS=.TRUE.)
*>           If .FALSE., then the corresponding element of A will be
*>           used.
*>           Not modified.
*>
*>  LRIGHT - LOGICAL
*>           If .TRUE., then XRIGHT will be used instead of the
*>           corresponding element of A for the last element in the
*>           first row (if LROWS=.FALSE.) or column (if LROWS=.TRUE.) If
*>           .FALSE., then the corresponding element of A will be used.
*>           Not modified.
*>
*>  NL     - INTEGER
*>           The length of the rows (if LROWS=.TRUE.) or columns (if
*>           LROWS=.FALSE.) to be rotated.  If XLEFT and/or XRIGHT are
*>           used, the columns/rows they are in should be included in
*>           NL, e.g., if LLEFT = LRIGHT = .TRUE., then NL must be at
*>           least 2.  The number of rows/columns to be rotated
*>           exclusive of those involving XLEFT and/or XRIGHT may
*>           not be negative, i.e., NL minus how many of LLEFT and
*>           LRIGHT are .TRUE. must be at least zero; if not, XERBLA
*>           will be called.
*>           Not modified.
*>
*>  C, S   - DOUBLE PRECISION
*>           Specify the Givens rotation to be applied.  If LROWS is
*>           true, then the matrix ( c  s )
*>                                 (-s  c )  is applied from the left;
*>           if false, then the transpose thereof is applied from the
*>           right.  For a Givens rotation, C**2 + S**2 should be 1,
*>           but this is not checked.
*>           Not modified.
*>
*>  A      - DOUBLE PRECISION array.
*>           The array containing the rows/columns to be rotated.  The
*>           first element of A should be the upper left element to
*>           be rotated.
*>           Read and modified.
*>
*>  LDA    - INTEGER
*>           The "effective" leading dimension of A.  If A contains
*>           a matrix stored in GE or SY format, then this is just
*>           the leading dimension of A as dimensioned in the calling
*>           routine.  If A contains a matrix stored in band (GB or SB)
*>           format, then this should be *one less* than the leading
*>           dimension used in the calling routine.  Thus, if
*>           A were dimensioned A(LDA,*) in DLAROT, then A(1,j) would
*>           be the j-th element in the first of the two rows
*>           to be rotated, and A(2,j) would be the j-th in the second,
*>           regardless of how the array may be stored in the calling
*>           routine.  [A cannot, however, actually be dimensioned thus,
*>           since for band format, the row number may exceed LDA, which
*>           is not legal FORTRAN.]
*>           If LROWS=.TRUE., then LDA must be at least 1, otherwise
*>           it must be at least NL minus the number of .TRUE. values
*>           in XLEFT and XRIGHT.
*>           Not modified.
*>
*>  XLEFT  - DOUBLE PRECISION
*>           If LLEFT is .TRUE., then XLEFT will be used and modified
*>           instead of A(2,1) (if LROWS=.TRUE.) or A(1,2)
*>           (if LROWS=.FALSE.).
*>           Read and modified.
*>
*>  XRIGHT - DOUBLE PRECISION
*>           If LRIGHT is .TRUE., then XRIGHT will be used and modified
*>           instead of A(1,NL) (if LROWS=.TRUE.) or A(NL,1)
*>           (if LROWS=.FALSE.).
*>           Read and modified.
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
*> \ingroup double_matgen
*
*  =====================================================================
      SUBROUTINE DLAROT( LROWS, LLEFT, LRIGHT, NL, C, S, A, LDA, XLEFT,
     $                   XRIGHT )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            LLEFT, LRIGHT, LROWS
      INTEGER            LDA, NL
      DOUBLE PRECISION   C, S, XLEFT, XRIGHT
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            IINC, INEXT, IX, IY, IYT, NT
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   XT( 2 ), YT( 2 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DROT, XERBLA
*     ..
*     .. Executable Statements ..
*
*     Set up indices, arrays for ends
*
      IF( LROWS ) THEN
         IINC = LDA
         INEXT = 1
      ELSE
         IINC = 1
         INEXT = LDA
      END IF
*
      IF( LLEFT ) THEN
         NT = 1
         IX = 1 + IINC
         IY = 2 + LDA
         XT( 1 ) = A( 1 )
         YT( 1 ) = XLEFT
      ELSE
         NT = 0
         IX = 1
         IY = 1 + INEXT
      END IF
*
      IF( LRIGHT ) THEN
         IYT = 1 + INEXT + ( NL-1 )*IINC
         NT = NT + 1
         XT( NT ) = XRIGHT
         YT( NT ) = A( IYT )
      END IF
*
*     Check for errors
*
      IF( NL.LT.NT ) THEN
         CALL XERBLA( 'DLAROT', 4 )
         RETURN
      END IF
      IF( LDA.LE.0 .OR. ( .NOT.LROWS .AND. LDA.LT.NL-NT ) ) THEN
         CALL XERBLA( 'DLAROT', 8 )
         RETURN
      END IF
*
*     Rotate
*
      CALL DROT( NL-NT, A( IX ), IINC, A( IY ), IINC, C, S )
      CALL DROT( NT, XT, 1, YT, 1, C, S )
*
*     Stuff values back into XLEFT, XRIGHT, etc.
*
      IF( LLEFT ) THEN
         A( 1 ) = XT( 1 )
         XLEFT = YT( 1 )
      END IF
*
      IF( LRIGHT ) THEN
         XRIGHT = XT( NT )
         A( IYT ) = YT( NT )
      END IF
*
      RETURN
*
*     End of DLAROT
*
      END
