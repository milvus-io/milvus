*> \brief \b ALAHD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ALAHD( IOUNIT, PATH )
*
*       .. Scalar Arguments ..
*       CHARACTER*3        PATH
*       INTEGER            IOUNIT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ALAHD prints header information for the different test paths.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IOUNIT
*> \verbatim
*>          IOUNIT is INTEGER
*>          The unit number to which the header information should be
*>          printed.
*> \endverbatim
*>
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The name of the path for which the header information is to
*>          be printed.  Current paths are
*>             _GE:  General matrices
*>             _GB:  General band
*>             _GT:  General Tridiagonal
*>             _PO:  Symmetric or Hermitian positive definite
*>             _PS:  Symmetric or Hermitian positive semi-definite
*>             _PP:  Symmetric or Hermitian positive definite packed
*>             _PB:  Symmetric or Hermitian positive definite band
*>             _PT:  Symmetric or Hermitian positive definite tridiagonal
*>             _SY:  Symmetric indefinite,
*>                     with partial (Bunch-Kaufman) pivoting
*>             _SR:  Symmetric indefinite,
*>                     with rook (bounded Bunch-Kaufman) pivoting
*>             _SK:  Symmetric indefinite,
*>                     with rook (bounded Bunch-Kaufman) pivoting
*>                     ( new storage format for factors:
*>                       L and diagonal of D is stored in A,
*>                       subdiagonal of D is stored in E )
*>             _SP:  Symmetric indefinite packed,
*>                     with partial (Bunch-Kaufman) pivoting
*>             _HA:  (complex) Hermitian ,
*>                     with Aasen Algorithm
*>             _HE:  (complex) Hermitian indefinite,
*>                     with partial (Bunch-Kaufman) pivoting
*>             _HR:  (complex) Hermitian indefinite,
*>                     with rook (bounded Bunch-Kaufman) pivoting
*>             _HK:  (complex) Hermitian indefinite,
*>                     with rook (bounded Bunch-Kaufman) pivoting
*>                     ( new storage format for factors:
*>                       L and diagonal of D is stored in A,
*>                       subdiagonal of D is stored in E )
*>             _HP:  (complex) Hermitian indefinite packed,
*>                     with partial (Bunch-Kaufman) pivoting
*>             _TR:  Triangular
*>             _TP:  Triangular packed
*>             _TB:  Triangular band
*>             _QR:  QR (general matrices)
*>             _LQ:  LQ (general matrices)
*>             _QL:  QL (general matrices)
*>             _RQ:  RQ (general matrices)
*>             _QP:  QR with column pivoting
*>             _TZ:  Trapezoidal
*>             _LS:  Least Squares driver routines
*>             _LU:  LU variants
*>             _CH:  Cholesky variants
*>             _QS:  QR variants
*>             _QT:  QRT (general matrices)
*>             _QX:  QRT (triangular-pentagonal matrices)
*>          The first character must be one of S, D, C, or Z (C or Z only
*>          if complex).
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
*> \ingroup aux_lin
*
*  =====================================================================
      SUBROUTINE ALAHD( IOUNIT, PATH )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      INTEGER            IOUNIT
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            CORZ, SORD
      CHARACTER          C1, C3
      CHARACTER*2        P2
      CHARACTER*4        EIGCNM
      CHARACTER*32       SUBNAM
      CHARACTER*9        SYM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, LSAMEN
      EXTERNAL           LSAME, LSAMEN
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          LEN_TRIM
*     ..
*     .. Executable Statements ..
*
      IF( IOUNIT.LE.0 )
     $   RETURN
      C1 = PATH( 1: 1 )
      C3 = PATH( 3: 3 )
      P2 = PATH( 2: 3 )
      SORD = LSAME( C1, 'S' ) .OR. LSAME( C1, 'D' )
      CORZ = LSAME( C1, 'C' ) .OR. LSAME( C1, 'Z' )
      IF( .NOT.( SORD .OR. CORZ ) )
     $   RETURN
*
      IF( LSAMEN( 2, P2, 'GE' ) ) THEN
*
*        GE: General dense
*
         WRITE( IOUNIT, FMT = 9999 )PATH
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9979 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9962 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9959 )4
         WRITE( IOUNIT, FMT = 9958 )5
         WRITE( IOUNIT, FMT = 9957 )6
         WRITE( IOUNIT, FMT = 9956 )7
         WRITE( IOUNIT, FMT = 9955 )8
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'GB' ) ) THEN
*
*        GB: General band
*
         WRITE( IOUNIT, FMT = 9998 )PATH
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9978 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9962 )1
         WRITE( IOUNIT, FMT = 9960 )2
         WRITE( IOUNIT, FMT = 9959 )3
         WRITE( IOUNIT, FMT = 9958 )4
         WRITE( IOUNIT, FMT = 9957 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'GT' ) ) THEN
*
*        GT: General tridiagonal
*
         WRITE( IOUNIT, FMT = 9997 )PATH
         WRITE( IOUNIT, FMT = 9977 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9962 )1
         WRITE( IOUNIT, FMT = 9960 )2
         WRITE( IOUNIT, FMT = 9959 )3
         WRITE( IOUNIT, FMT = 9958 )4
         WRITE( IOUNIT, FMT = 9957 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'PO' ) .OR. LSAMEN( 2, P2, 'PP' ) ) THEN
*
*        PO: Positive definite full
*        PP: Positive definite packed
*
         IF( SORD ) THEN
            SYM = 'Symmetric'
         ELSE
            SYM = 'Hermitian'
         END IF
         IF( LSAME( C3, 'O' ) ) THEN
            WRITE( IOUNIT, FMT = 9996 )PATH, SYM
         ELSE
            WRITE( IOUNIT, FMT = 9995 )PATH, SYM
         END IF
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9975 )PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9954 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9959 )4
         WRITE( IOUNIT, FMT = 9958 )5
         WRITE( IOUNIT, FMT = 9957 )6
         WRITE( IOUNIT, FMT = 9956 )7
         WRITE( IOUNIT, FMT = 9955 )8
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'PS' ) ) THEN
*
*        PS: Positive semi-definite full
*
         IF( SORD ) THEN
            SYM = 'Symmetric'
         ELSE
            SYM = 'Hermitian'
         END IF
         IF( LSAME( C1, 'S' ) .OR. LSAME( C1, 'C' ) ) THEN
            EIGCNM = '1E04'
         ELSE
            EIGCNM = '1D12'
         END IF
         WRITE( IOUNIT, FMT = 9995 )PATH, SYM
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 8973 )EIGCNM, EIGCNM, EIGCNM
         WRITE( IOUNIT, FMT = '( '' Difference:'' )' )
         WRITE( IOUNIT, FMT = 8972 )C1
         WRITE( IOUNIT, FMT = '( '' Test ratio:'' )' )
         WRITE( IOUNIT, FMT = 8950 )
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
      ELSE IF( LSAMEN( 2, P2, 'PB' ) ) THEN
*
*        PB: Positive definite band
*
         IF( SORD ) THEN
            WRITE( IOUNIT, FMT = 9994 )PATH, 'Symmetric'
         ELSE
            WRITE( IOUNIT, FMT = 9994 )PATH, 'Hermitian'
         END IF
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9973 )PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9954 )1
         WRITE( IOUNIT, FMT = 9960 )2
         WRITE( IOUNIT, FMT = 9959 )3
         WRITE( IOUNIT, FMT = 9958 )4
         WRITE( IOUNIT, FMT = 9957 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'PT' ) ) THEN
*
*        PT: Positive definite tridiagonal
*
         IF( SORD ) THEN
            WRITE( IOUNIT, FMT = 9993 )PATH, 'Symmetric'
         ELSE
            WRITE( IOUNIT, FMT = 9993 )PATH, 'Hermitian'
         END IF
         WRITE( IOUNIT, FMT = 9976 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9952 )1
         WRITE( IOUNIT, FMT = 9960 )2
         WRITE( IOUNIT, FMT = 9959 )3
         WRITE( IOUNIT, FMT = 9958 )4
         WRITE( IOUNIT, FMT = 9957 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'SY' )  ) THEN
*
*        SY: Symmetric indefinite full,
*            with partial (Bunch-Kaufman) pivoting algorithm
*
         IF( LSAME( C3, 'Y' ) ) THEN
            WRITE( IOUNIT, FMT = 9992 )PATH, 'Symmetric'
         ELSE
            WRITE( IOUNIT, FMT = 9991 )PATH, 'Symmetric'
         END IF
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         IF( SORD ) THEN
            WRITE( IOUNIT, FMT = 9972 )
         ELSE
            WRITE( IOUNIT, FMT = 9971 )
         END IF
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9960 )4
         WRITE( IOUNIT, FMT = 9959 )5
         WRITE( IOUNIT, FMT = 9958 )6
         WRITE( IOUNIT, FMT = 9956 )7
         WRITE( IOUNIT, FMT = 9957 )8
         WRITE( IOUNIT, FMT = 9955 )9
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'SR' ) .OR. LSAMEN( 2, P2, 'SK') ) THEN
*
*        SR: Symmetric indefinite full,
*            with rook (bounded Bunch-Kaufman) pivoting algorithm
*
*        SK: Symmetric indefinite full,
*            with rook (bounded Bunch-Kaufman) pivoting algorithm,
*            ( new storage format for factors:
*              L and diagonal of D is stored in A,
*              subdiagonal of D is stored in E )
*
         WRITE( IOUNIT, FMT = 9892 )PATH, 'Symmetric'
*
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         IF( SORD ) THEN
            WRITE( IOUNIT, FMT = 9972 )
         ELSE
            WRITE( IOUNIT, FMT = 9971 )
         END IF
*
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9927 )3
         WRITE( IOUNIT, FMT = 9928 )
         WRITE( IOUNIT, FMT = 9926 )4
         WRITE( IOUNIT, FMT = 9928 )
         WRITE( IOUNIT, FMT = 9960 )5
         WRITE( IOUNIT, FMT = 9959 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'SP' ) ) THEN
*
*        SP: Symmetric indefinite packed,
*            with partial (Bunch-Kaufman) pivoting algorithm
*
         IF( LSAME( C3, 'Y' ) ) THEN
            WRITE( IOUNIT, FMT = 9992 )PATH, 'Symmetric'
         ELSE
            WRITE( IOUNIT, FMT = 9991 )PATH, 'Symmetric'
         END IF
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         IF( SORD ) THEN
            WRITE( IOUNIT, FMT = 9972 )
         ELSE
            WRITE( IOUNIT, FMT = 9971 )
         END IF
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9959 )4
         WRITE( IOUNIT, FMT = 9958 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9957 )7
         WRITE( IOUNIT, FMT = 9955 )8
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'HA' )  ) THEN
*
*        HA: Hermitian,
*            with Assen Algorithm
*
         WRITE( IOUNIT, FMT = 9992 )PATH, 'Hermitian'
*
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9972 )
*
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9960 )4
         WRITE( IOUNIT, FMT = 9959 )5
         WRITE( IOUNIT, FMT = 9958 )6
         WRITE( IOUNIT, FMT = 9956 )7
         WRITE( IOUNIT, FMT = 9957 )8
         WRITE( IOUNIT, FMT = 9955 )9
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'HE' )  ) THEN
*
*        HE: Hermitian indefinite full,
*            with partial (Bunch-Kaufman) pivoting algorithm
*
         WRITE( IOUNIT, FMT = 9992 )PATH, 'Hermitian'
*
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9972 )
*
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9960 )4
         WRITE( IOUNIT, FMT = 9959 )5
         WRITE( IOUNIT, FMT = 9958 )6
         WRITE( IOUNIT, FMT = 9956 )7
         WRITE( IOUNIT, FMT = 9957 )8
         WRITE( IOUNIT, FMT = 9955 )9
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'HR' ) .OR. LSAMEN( 2, P2, 'HR' ) ) THEN
*
*        HR: Hermitian indefinite full,
*            with rook (bounded Bunch-Kaufman) pivoting algorithm
*
*        HK: Hermitian indefinite full,
*            with rook (bounded Bunch-Kaufman) pivoting algorithm,
*            ( new storage format for factors:
*              L and diagonal of D is stored in A,
*              subdiagonal of D is stored in E )
*
         WRITE( IOUNIT, FMT = 9892 )PATH, 'Hermitian'
*
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9972 )
*
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9927 )3
         WRITE( IOUNIT, FMT = 9928 )
         WRITE( IOUNIT, FMT = 9926 )4
         WRITE( IOUNIT, FMT = 9928 )
         WRITE( IOUNIT, FMT = 9960 )5
         WRITE( IOUNIT, FMT = 9959 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'HP' ) ) THEN
*
*        HP: Hermitian indefinite packed,
*            with partial (Bunch-Kaufman) pivoting algorithm
*
         IF( LSAME( C3, 'E' ) ) THEN
            WRITE( IOUNIT, FMT = 9992 )PATH, 'Hermitian'
         ELSE
            WRITE( IOUNIT, FMT = 9991 )PATH, 'Hermitian'
         END IF
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9972 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9953 )1
         WRITE( IOUNIT, FMT = 9961 )2
         WRITE( IOUNIT, FMT = 9960 )3
         WRITE( IOUNIT, FMT = 9959 )4
         WRITE( IOUNIT, FMT = 9958 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9957 )7
         WRITE( IOUNIT, FMT = 9955 )8
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'TR' ) .OR. LSAMEN( 2, P2, 'TP' ) ) THEN
*
*        TR: Triangular full
*        TP: Triangular packed
*
         IF( LSAME( C3, 'R' ) ) THEN
            WRITE( IOUNIT, FMT = 9990 )PATH
            SUBNAM = PATH( 1: 1 ) // 'LATRS'
         ELSE
            WRITE( IOUNIT, FMT = 9989 )PATH
            SUBNAM = PATH( 1: 1 ) // 'LATPS'
         END IF
         WRITE( IOUNIT, FMT = 9966 )PATH
         WRITE( IOUNIT, FMT = 9965 )SUBNAM(1:LEN_TRIM( SUBNAM ))
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9961 )1
         WRITE( IOUNIT, FMT = 9960 )2
         WRITE( IOUNIT, FMT = 9959 )3
         WRITE( IOUNIT, FMT = 9958 )4
         WRITE( IOUNIT, FMT = 9957 )5
         WRITE( IOUNIT, FMT = 9956 )6
         WRITE( IOUNIT, FMT = 9955 )7
         WRITE( IOUNIT, FMT = 9951 )SUBNAM(1:LEN_TRIM( SUBNAM )), 8
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'TB' ) ) THEN
*
*        TB: Triangular band
*
         WRITE( IOUNIT, FMT = 9988 )PATH
         SUBNAM = PATH( 1: 1 ) // 'LATBS'
         WRITE( IOUNIT, FMT = 9964 )PATH
         WRITE( IOUNIT, FMT = 9963 )SUBNAM(1:LEN_TRIM( SUBNAM ))
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9960 )1
         WRITE( IOUNIT, FMT = 9959 )2
         WRITE( IOUNIT, FMT = 9958 )3
         WRITE( IOUNIT, FMT = 9957 )4
         WRITE( IOUNIT, FMT = 9956 )5
         WRITE( IOUNIT, FMT = 9955 )6
         WRITE( IOUNIT, FMT = 9951 )SUBNAM(1:LEN_TRIM( SUBNAM )), 7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'QR' ) ) THEN
*
*        QR decomposition of rectangular matrices
*
         WRITE( IOUNIT, FMT = 9987 )PATH, 'QR'
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9970 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9950 )1
         WRITE( IOUNIT, FMT = 6950 )8
         WRITE( IOUNIT, FMT = 9946 )2
         WRITE( IOUNIT, FMT = 9944 )3, 'M'
         WRITE( IOUNIT, FMT = 9943 )4, 'M'
         WRITE( IOUNIT, FMT = 9942 )5, 'M'
         WRITE( IOUNIT, FMT = 9941 )6, 'M'
         WRITE( IOUNIT, FMT = 9960 )7
         WRITE( IOUNIT, FMT = 6660 )9
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'LQ' ) ) THEN
*
*        LQ decomposition of rectangular matrices
*
         WRITE( IOUNIT, FMT = 9987 )PATH, 'LQ'
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9970 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9949 )1
         WRITE( IOUNIT, FMT = 9945 )2
         WRITE( IOUNIT, FMT = 9944 )3, 'N'
         WRITE( IOUNIT, FMT = 9943 )4, 'N'
         WRITE( IOUNIT, FMT = 9942 )5, 'N'
         WRITE( IOUNIT, FMT = 9941 )6, 'N'
         WRITE( IOUNIT, FMT = 9960 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'QL' ) ) THEN
*
*        QL decomposition of rectangular matrices
*
         WRITE( IOUNIT, FMT = 9987 )PATH, 'QL'
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9970 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9948 )1
         WRITE( IOUNIT, FMT = 9946 )2
         WRITE( IOUNIT, FMT = 9944 )3, 'M'
         WRITE( IOUNIT, FMT = 9943 )4, 'M'
         WRITE( IOUNIT, FMT = 9942 )5, 'M'
         WRITE( IOUNIT, FMT = 9941 )6, 'M'
         WRITE( IOUNIT, FMT = 9960 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'RQ' ) ) THEN
*
*        RQ decomposition of rectangular matrices
*
         WRITE( IOUNIT, FMT = 9987 )PATH, 'RQ'
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9970 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9947 )1
         WRITE( IOUNIT, FMT = 9945 )2
         WRITE( IOUNIT, FMT = 9944 )3, 'N'
         WRITE( IOUNIT, FMT = 9943 )4, 'N'
         WRITE( IOUNIT, FMT = 9942 )5, 'N'
         WRITE( IOUNIT, FMT = 9941 )6, 'N'
         WRITE( IOUNIT, FMT = 9960 )7
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'QP' ) ) THEN
*
*        QR decomposition with column pivoting
*
         WRITE( IOUNIT, FMT = 9986 )PATH
         WRITE( IOUNIT, FMT = 9969 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9940 )1
         WRITE( IOUNIT, FMT = 9939 )2
         WRITE( IOUNIT, FMT = 9938 )3
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'TZ' ) ) THEN
*
*        TZ:  Trapezoidal
*
         WRITE( IOUNIT, FMT = 9985 )PATH
         WRITE( IOUNIT, FMT = 9968 )
         WRITE( IOUNIT, FMT = 9929 )C1
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 9940 )1
         WRITE( IOUNIT, FMT = 9937 )2
         WRITE( IOUNIT, FMT = 9938 )3
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'LS' ) ) THEN
*
*        LS:  Least Squares driver routines for
*             LS, LSD, LSS, LSX and LSY.
*
         WRITE( IOUNIT, FMT = 9984 )PATH
         WRITE( IOUNIT, FMT = 9967 )
         WRITE( IOUNIT, FMT = 9921 )C1, C1, C1, C1
         WRITE( IOUNIT, FMT = 9935 )1
         WRITE( IOUNIT, FMT = 9931 )2
         WRITE( IOUNIT, FMT = 9933 )3
         WRITE( IOUNIT, FMT = 9935 )4
         WRITE( IOUNIT, FMT = 9934 )5
         WRITE( IOUNIT, FMT = 9932 )6
         WRITE( IOUNIT, FMT = 9920 )
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'LU' ) ) THEN
*
*        LU factorization variants
*
         WRITE( IOUNIT, FMT = 9983 )PATH
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9979 )
         WRITE( IOUNIT, FMT = '( '' Test ratio:'' )' )
         WRITE( IOUNIT, FMT = 9962 )1
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'CH' ) ) THEN
*
*        Cholesky factorization variants
*
         WRITE( IOUNIT, FMT = 9982 )PATH
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9974 )
         WRITE( IOUNIT, FMT = '( '' Test ratio:'' )' )
         WRITE( IOUNIT, FMT = 9954 )1
         WRITE( IOUNIT, FMT = '( '' Messages:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'QS' ) ) THEN
*
*        QR factorization variants
*
         WRITE( IOUNIT, FMT = 9981 )PATH
         WRITE( IOUNIT, FMT = '( '' Matrix types:'' )' )
         WRITE( IOUNIT, FMT = 9970 )
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
*
      ELSE IF( LSAMEN( 2, P2, 'QT' ) ) THEN
*
*        QRT (general matrices)
*
         WRITE( IOUNIT, FMT = 8000 ) PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 8011 ) 1
         WRITE( IOUNIT, FMT = 8012 ) 2
         WRITE( IOUNIT, FMT = 8013 ) 3
         WRITE( IOUNIT, FMT = 8014 ) 4
         WRITE( IOUNIT, FMT = 8015 ) 5
         WRITE( IOUNIT, FMT = 8016 ) 6
*
      ELSE IF( LSAMEN( 2, P2, 'QX' ) ) THEN
*
*        QRT (triangular-pentagonal)
*
         WRITE( IOUNIT, FMT = 8001 ) PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 8017 ) 1
         WRITE( IOUNIT, FMT = 8018 ) 2
         WRITE( IOUNIT, FMT = 8019 ) 3
         WRITE( IOUNIT, FMT = 8020 ) 4
         WRITE( IOUNIT, FMT = 8021 ) 5
         WRITE( IOUNIT, FMT = 8022 ) 6
*
      ELSE IF( LSAMEN( 2, P2, 'TQ' ) ) THEN
*
*        QRT (triangular-pentagonal)
*
         WRITE( IOUNIT, FMT = 8002 ) PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 8023 ) 1
         WRITE( IOUNIT, FMT = 8024 ) 2
         WRITE( IOUNIT, FMT = 8025 ) 3
         WRITE( IOUNIT, FMT = 8026 ) 4
         WRITE( IOUNIT, FMT = 8027 ) 5
         WRITE( IOUNIT, FMT = 8028 ) 6
*
      ELSE IF( LSAMEN( 2, P2, 'XQ' ) ) THEN
*
*        QRT (triangular-pentagonal)
*
         WRITE( IOUNIT, FMT = 8003 ) PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 8029 ) 1
         WRITE( IOUNIT, FMT = 8030 ) 2
         WRITE( IOUNIT, FMT = 8031 ) 3
         WRITE( IOUNIT, FMT = 8032 ) 4
         WRITE( IOUNIT, FMT = 8033 ) 5
         WRITE( IOUNIT, FMT = 8034 ) 6
*
      ELSE IF( LSAMEN( 2, P2, 'TS' ) ) THEN
*
*        QRT (triangular-pentagonal)
*
         WRITE( IOUNIT, FMT = 8004 ) PATH
         WRITE( IOUNIT, FMT = '( '' Test ratios:'' )' )
         WRITE( IOUNIT, FMT = 8035 ) 1
         WRITE( IOUNIT, FMT = 8036 ) 2
         WRITE( IOUNIT, FMT = 8037 ) 3
         WRITE( IOUNIT, FMT = 8038 ) 4
         WRITE( IOUNIT, FMT = 8039 ) 5
         WRITE( IOUNIT, FMT = 8040 ) 6
*
      ELSE
*
*        Print error message if no header is available.
*
         WRITE( IOUNIT, FMT = 9980 )PATH
      END IF
*
*     First line of header
*
 9999 FORMAT( / 1X, A3, ':  General dense matrices' )
 9998 FORMAT( / 1X, A3, ':  General band matrices' )
 9997 FORMAT( / 1X, A3, ':  General tridiagonal' )
 9996 FORMAT( / 1X, A3, ':  ', A9, ' positive definite matrices' )
 9995 FORMAT( / 1X, A3, ':  ', A9, ' positive definite packed matrices'
     $       )
 9994 FORMAT( / 1X, A3, ':  ', A9, ' positive definite band matrices' )
 9993 FORMAT( / 1X, A3, ':  ', A9, ' positive definite tridiagonal' )
 9992 FORMAT( / 1X, A3, ':  ', A9, ' indefinite matrices',
     $      ', partial (Bunch-Kaufman) pivoting' )
 9991 FORMAT( / 1X, A3, ':  ', A9, ' indefinite packed matrices',
     $      ', partial (Bunch-Kaufman) pivoting' )
 9892 FORMAT( / 1X, A3, ':  ', A9, ' indefinite matrices',
     $      ', "rook" (bounded Bunch-Kaufman) pivoting' )
 9891 FORMAT( / 1X, A3, ':  ', A9, ' indefinite packed matrices',
     $      ', "rook" (bounded Bunch-Kaufman) pivoting' )
 9990 FORMAT( / 1X, A3, ':  Triangular matrices' )
 9989 FORMAT( / 1X, A3, ':  Triangular packed matrices' )
 9988 FORMAT( / 1X, A3, ':  Triangular band matrices' )
 9987 FORMAT( / 1X, A3, ':  ', A2, ' factorization of general matrices'
     $       )
 9986 FORMAT( / 1X, A3, ':  QR factorization with column pivoting' )
 9985 FORMAT( / 1X, A3, ':  RQ factorization of trapezoidal matrix' )
 9984 FORMAT( / 1X, A3, ':  Least squares driver routines' )
 9983 FORMAT( / 1X, A3, ':  LU factorization variants' )
 9982 FORMAT( / 1X, A3, ':  Cholesky factorization variants' )
 9981 FORMAT( / 1X, A3, ':  QR factorization variants' )
 9980 FORMAT( / 1X, A3, ':  No header available' )
 8000 FORMAT( / 1X, A3, ':  QRT factorization for general matrices' )
 8001 FORMAT( / 1X, A3, ':  QRT factorization for ',
     $       'triangular-pentagonal matrices' )
 8002 FORMAT( / 1X, A3, ':  LQT factorization for general matrices' )
 8003 FORMAT( / 1X, A3, ':  LQT factorization for ',
     $       'triangular-pentagonal matrices' )
 8004 FORMAT( / 1X, A3, ':  TS factorization for ',
     $       'tall-skiny or short-wide matrices' )
*
*     GE matrix types
*
 9979 FORMAT( 4X, '1. Diagonal', 24X, '7. Last n/2 columns zero', / 4X,
     $      '2. Upper triangular', 16X,
     $      '8. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '3. Lower triangular', 16X, '9. Random, CNDNUM = 0.1/EPS',
     $      / 4X, '4. Random, CNDNUM = 2', 13X,
     $      '10. Scaled near underflow', / 4X, '5. First column zero',
     $      14X, '11. Scaled near overflow', / 4X,
     $      '6. Last column zero' )
*
*     GB matrix types
*
 9978 FORMAT( 4X, '1. Random, CNDNUM = 2', 14X,
     $      '5. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '2. First column zero', 15X, '6. Random, CNDNUM = .01/EPS',
     $      / 4X, '3. Last column zero', 16X,
     $      '7. Scaled near underflow', / 4X,
     $      '4. Last n/2 columns zero', 11X, '8. Scaled near overflow' )
*
*     GT matrix types
*
 9977 FORMAT( ' Matrix types (1-6 have specified condition numbers):',
     $      / 4X, '1. Diagonal', 24X, '7. Random, unspecified CNDNUM',
     $      / 4X, '2. Random, CNDNUM = 2', 14X, '8. First column zero',
     $      / 4X, '3. Random, CNDNUM = sqrt(0.1/EPS)', 2X,
     $      '9. Last column zero', / 4X, '4. Random, CNDNUM = 0.1/EPS',
     $      7X, '10. Last n/2 columns zero', / 4X,
     $      '5. Scaled near underflow', 10X,
     $      '11. Scaled near underflow', / 4X,
     $      '6. Scaled near overflow', 11X, '12. Scaled near overflow' )
*
*     PT matrix types
*
 9976 FORMAT( ' Matrix types (1-6 have specified condition numbers):',
     $      / 4X, '1. Diagonal', 24X, '7. Random, unspecified CNDNUM',
     $      / 4X, '2. Random, CNDNUM = 2', 14X,
     $      '8. First row and column zero', / 4X,
     $      '3. Random, CNDNUM = sqrt(0.1/EPS)', 2X,
     $      '9. Last row and column zero', / 4X,
     $      '4. Random, CNDNUM = 0.1/EPS', 7X,
     $      '10. Middle row and column zero', / 4X,
     $      '5. Scaled near underflow', 10X,
     $      '11. Scaled near underflow', / 4X,
     $      '6. Scaled near overflow', 11X, '12. Scaled near overflow' )
*
*     PO, PP matrix types
*
 9975 FORMAT( 4X, '1. Diagonal', 24X,
     $      '6. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '2. Random, CNDNUM = 2', 14X, '7. Random, CNDNUM = 0.1/EPS',
     $      / 3X, '*3. First row and column zero', 7X,
     $      '8. Scaled near underflow', / 3X,
     $      '*4. Last row and column zero', 8X,
     $      '9. Scaled near overflow', / 3X,
     $      '*5. Middle row and column zero', / 3X,
     $      '(* - tests error exits from ', A3,
     $      'TRF, no test ratios are computed)' )
*
*     CH matrix types
*
 9974 FORMAT( 4X, '1. Diagonal', 24X,
     $      '6. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '2. Random, CNDNUM = 2', 14X, '7. Random, CNDNUM = 0.1/EPS',
     $      / 3X, '*3. First row and column zero', 7X,
     $      '8. Scaled near underflow', / 3X,
     $      '*4. Last row and column zero', 8X,
     $      '9. Scaled near overflow', / 3X,
     $      '*5. Middle row and column zero', / 3X,
     $      '(* - tests error exits, no test ratios are computed)' )
*
*     PS matrix types
*
 8973 FORMAT( 4X, '1. Diagonal', / 4X, '2. Random, CNDNUM = 2', 14X,
     $      / 3X, '*3. Nonzero eigenvalues of: D(1:RANK-1)=1 and ',
     $      'D(RANK) = 1.0/', A4, / 3X,
     $      '*4. Nonzero eigenvalues of: D(1)=1 and ',
     $      ' D(2:RANK) = 1.0/', A4, / 3X,
     $      '*5. Nonzero eigenvalues of: D(I) = ', A4,
     $      '**(-(I-1)/(RANK-1)) ', ' I=1:RANK', / 4X,
     $      '6. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '7. Random, CNDNUM = 0.1/EPS', / 4X,
     $      '8. Scaled near underflow', / 4X, '9. Scaled near overflow',
     $      / 3X, '(* - Semi-definite tests )' )
 8972 FORMAT( 3X, 'RANK minus computed rank, returned by ', A, 'PSTRF' )
*
*     PB matrix types
*
 9973 FORMAT( 4X, '1. Random, CNDNUM = 2', 14X,
     $      '5. Random, CNDNUM = sqrt(0.1/EPS)', / 3X,
     $      '*2. First row and column zero', 7X,
     $      '6. Random, CNDNUM = 0.1/EPS', / 3X,
     $      '*3. Last row and column zero', 8X,
     $      '7. Scaled near underflow', / 3X,
     $      '*4. Middle row and column zero', 6X,
     $      '8. Scaled near overflow', / 3X,
     $      '(* - tests error exits from ', A3,
     $      'TRF, no test ratios are computed)' )
*
*     SSY, SSR, SSP, CHE, CHR, CHP matrix types
*
 9972 FORMAT( 4X, '1. Diagonal', 24X,
     $      '6. Last n/2 rows and columns zero', / 4X,
     $      '2. Random, CNDNUM = 2', 14X,
     $      '7. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '3. First row and column zero', 7X,
     $      '8. Random, CNDNUM = 0.1/EPS', / 4X,
     $      '4. Last row and column zero', 8X,
     $      '9. Scaled near underflow', / 4X,
     $      '5. Middle row and column zero', 5X,
     $      '10. Scaled near overflow' )
*
*     CSY, CSR, CSP matrix types
*
 9971 FORMAT( 4X, '1. Diagonal', 24X,
     $      '7. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '2. Random, CNDNUM = 2', 14X, '8. Random, CNDNUM = 0.1/EPS',
     $      / 4X, '3. First row and column zero', 7X,
     $      '9. Scaled near underflow', / 4X,
     $      '4. Last row and column zero', 7X,
     $      '10. Scaled near overflow', / 4X,
     $      '5. Middle row and column zero', 5X,
     $      '11. Block diagonal matrix', / 4X,
     $      '6. Last n/2 rows and columns zero' )
*
*     QR matrix types
*
 9970 FORMAT( 4X, '1. Diagonal', 24X,
     $      '5. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '2. Upper triangular', 16X, '6. Random, CNDNUM = 0.1/EPS',
     $      / 4X, '3. Lower triangular', 16X,
     $      '7. Scaled near underflow', / 4X, '4. Random, CNDNUM = 2',
     $      14X, '8. Scaled near overflow' )
*
*     QP matrix types
*
 9969 FORMAT( ' Matrix types (2-6 have condition 1/EPS):', / 4X,
     $      '1. Zero matrix', 21X, '4. First n/2 columns fixed', / 4X,
     $      '2. One small eigenvalue', 12X, '5. Last n/2 columns fixed',
     $      / 4X, '3. Geometric distribution', 10X,
     $      '6. Every second column fixed' )
*
*     TZ matrix types
*
 9968 FORMAT( ' Matrix types (2-3 have condition 1/EPS):', / 4X,
     $      '1. Zero matrix', / 4X, '2. One small eigenvalue', / 4X,
     $      '3. Geometric distribution' )
*
*     LS matrix types
*
 9967 FORMAT( ' Matrix types (1-3: full rank, 4-6: rank deficient):',
     $      / 4X, '1 and 4. Normal scaling', / 4X,
     $      '2 and 5. Scaled near overflow', / 4X,
     $      '3 and 6. Scaled near underflow' )
*
*     TR, TP matrix types
*
 9966 FORMAT( ' Matrix types for ', A3, ' routines:', / 4X,
     $      '1. Diagonal', 24X, '6. Scaled near overflow', / 4X,
     $      '2. Random, CNDNUM = 2', 14X, '7. Identity', / 4X,
     $      '3. Random, CNDNUM = sqrt(0.1/EPS)  ',
     $      '8. Unit triangular, CNDNUM = 2', / 4X,
     $      '4. Random, CNDNUM = 0.1/EPS', 8X,
     $      '9. Unit, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '5. Scaled near underflow', 10X,
     $      '10. Unit, CNDNUM = 0.1/EPS' )
 9965 FORMAT( ' Special types for testing ', A, ':', / 3X,
     $      '11. Matrix elements are O(1), large right hand side', / 3X,
     $      '12. First diagonal causes overflow,',
     $      ' offdiagonal column norms < 1', / 3X,
     $      '13. First diagonal causes overflow,',
     $      ' offdiagonal column norms > 1', / 3X,
     $      '14. Growth factor underflows, solution does not overflow',
     $      / 3X, '15. Small diagonal causes gradual overflow', / 3X,
     $      '16. One zero diagonal element', / 3X,
     $      '17. Large offdiagonals cause overflow when adding a column'
     $      , / 3X, '18. Unit triangular with large right hand side' )
*
*     TB matrix types
*
 9964 FORMAT( ' Matrix types for ', A3, ' routines:', / 4X,
     $      '1. Random, CNDNUM = 2', 14X, '6. Identity', / 4X,
     $      '2. Random, CNDNUM = sqrt(0.1/EPS)  ',
     $      '7. Unit triangular, CNDNUM = 2', / 4X,
     $      '3. Random, CNDNUM = 0.1/EPS', 8X,
     $      '8. Unit, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '4. Scaled near underflow', 11X,
     $      '9. Unit, CNDNUM = 0.1/EPS', / 4X,
     $      '5. Scaled near overflow' )
 9963 FORMAT( ' Special types for testing ', A, ':', / 3X,
     $      '10. Matrix elements are O(1), large right hand side', / 3X,
     $      '11. First diagonal causes overflow,',
     $      ' offdiagonal column norms < 1', / 3X,
     $      '12. First diagonal causes overflow,',
     $      ' offdiagonal column norms > 1', / 3X,
     $      '13. Growth factor underflows, solution does not overflow',
     $      / 3X, '14. Small diagonal causes gradual overflow', / 3X,
     $      '15. One zero diagonal element', / 3X,
     $      '16. Large offdiagonals cause overflow when adding a column'
     $      , / 3X, '17. Unit triangular with large right hand side' )
*
*     Test ratios
*
 9962 FORMAT( 3X, I2, ': norm( L * U - A )  / ( N * norm(A) * EPS )' )
 9961 FORMAT( 3X, I2, ': norm( I - A*AINV ) / ',
     $      '( N * norm(A) * norm(AINV) * EPS )' )
 9960 FORMAT( 3X, I2, ': norm( B - A * X )  / ',
     $      '( norm(A) * norm(X) * EPS )' )
 6660 FORMAT( 3X, I2, ': diagonal is not non-negative')
 9959 FORMAT( 3X, I2, ': norm( X - XACT )   / ',
     $      '( norm(XACT) * CNDNUM * EPS )' )
 9958 FORMAT( 3X, I2, ': norm( X - XACT )   / ',
     $      '( norm(XACT) * CNDNUM * EPS ), refined' )
 9957 FORMAT( 3X, I2, ': norm( X - XACT )   / ',
     $      '( norm(XACT) * (error bound) )' )
 9956 FORMAT( 3X, I2, ': (backward error)   / EPS' )
 9955 FORMAT( 3X, I2, ': RCOND * CNDNUM - 1.0' )
 9954 FORMAT( 3X, I2, ': norm( U'' * U - A ) / ( N * norm(A) * EPS )',
     $      ', or', / 7X, 'norm( L * L'' - A ) / ( N * norm(A) * EPS )'
     $       )
 8950 FORMAT( 3X,
     $      'norm( P * U'' * U * P'' - A ) / ( N * norm(A) * EPS )',
     $      ', or', / 3X,
     $      'norm( P * L * L'' * P'' - A ) / ( N * norm(A) * EPS )' )
 9953 FORMAT( 3X, I2, ': norm( U*D*U'' - A ) / ( N * norm(A) * EPS )',
     $      ', or', / 7X, 'norm( L*D*L'' - A ) / ( N * norm(A) * EPS )'
     $       )
 9952 FORMAT( 3X, I2, ': norm( U''*D*U - A ) / ( N * norm(A) * EPS )',
     $      ', or', / 7X, 'norm( L*D*L'' - A ) / ( N * norm(A) * EPS )'
     $       )
 9951 FORMAT( ' Test ratio for ', A, ':', / 3X, I2,
     $      ': norm( s*b - A*x )  / ( norm(A) * norm(x) * EPS )' )
 9950 FORMAT( 3X, I2, ': norm( R - Q'' * A ) / ( M * norm(A) * EPS )' )
 6950 FORMAT( 3X, I2, ': norm( R - Q'' * A ) / ( M * norm(A) * EPS )
     $       [RFPG]' )
 9949 FORMAT( 3X, I2, ': norm( L - A * Q'' ) / ( N * norm(A) * EPS )' )
 9948 FORMAT( 3X, I2, ': norm( L - Q'' * A ) / ( M * norm(A) * EPS )' )
 9947 FORMAT( 3X, I2, ': norm( R - A * Q'' ) / ( N * norm(A) * EPS )' )
 9946 FORMAT( 3X, I2, ': norm( I - Q''*Q )   / ( M * EPS )' )
 9945 FORMAT( 3X, I2, ': norm( I - Q*Q'' )   / ( N * EPS )' )
 9944 FORMAT( 3X, I2, ': norm( Q*C - Q*C )  / ', '( ', A1,
     $      ' * norm(C) * EPS )' )
 9943 FORMAT( 3X, I2, ': norm( C*Q - C*Q )  / ', '( ', A1,
     $      ' * norm(C) * EPS )' )
 9942 FORMAT( 3X, I2, ': norm( Q''*C - Q''*C )/ ', '( ', A1,
     $      ' * norm(C) * EPS )' )
 9941 FORMAT( 3X, I2, ': norm( C*Q'' - C*Q'' )/ ', '( ', A1,
     $      ' * norm(C) * EPS )' )
 9940 FORMAT( 3X, I2, ': norm(svd(A) - svd(R)) / ',
     $      '( M * norm(svd(R)) * EPS )' )
 9939 FORMAT( 3X, I2, ': norm( A*P - Q*R )     / ( M * norm(A) * EPS )'
     $       )
 9938 FORMAT( 3X, I2, ': norm( I - Q''*Q )      / ( M * EPS )' )
 9937 FORMAT( 3X, I2, ': norm( A - R*Q )       / ( M * norm(A) * EPS )'
     $       )
 9935 FORMAT( 3X, I2, ': norm( B - A * X )   / ',
     $      '( max(M,N) * norm(A) * norm(X) * EPS )' )
 9934 FORMAT( 3X, I2, ': norm( (A*X-B)'' *A ) / ',
     $      '( max(M,N,NRHS) * norm(A) * norm(B) * EPS )' )
 9933 FORMAT( 3X, I2, ': norm(svd(A)-svd(R)) / ',
     $      '( min(M,N) * norm(svd(R)) * EPS )' )
 9932 FORMAT( 3X, I2, ': Check if X is in the row space of A or A''' )
 9931 FORMAT( 3X, I2, ': norm( (A*X-B)'' *A ) / ',
     $      '( max(M,N,NRHS) * norm(A) * norm(B) * EPS )', / 7X,
     $      'if TRANS=''N'' and M.GE.N or TRANS=''T'' and M.LT.N, ',
     $      'otherwise', / 7X,
     $      'check if X is in the row space of A or A'' ',
     $      '(overdetermined case)' )
 9929 FORMAT( ' Test ratios (1-3: ', A1, 'TZRZF):' )
 9920 FORMAT( 3X, ' 7-10: same as 3-6', 3X, ' 11-14: same as 3-6' )
 9921 FORMAT( ' Test ratios:', / '    (1-2: ', A1, 'GELS, 3-6: ', A1,
     $      'GELSY, 7-10: ', A1, 'GELSS, 11-14: ', A1, 'GELSD, 15-16: '
     $        A1, 'GETSLS)')
 9928 FORMAT( 7X, 'where ALPHA = ( 1 + SQRT( 17 ) ) / 8' )
 9927 FORMAT( 3X, I2, ': ABS( Largest element in L )', / 12X,
     $      ' - ( 1 / ( 1 - ALPHA ) ) + THRESH' )
 9926 FORMAT( 3X, I2, ': Largest 2-Norm of 2-by-2 pivots', / 12X,
     $      ' - ( ( 1 + ALPHA ) / ( 1 - ALPHA ) ) + THRESH' )
 8011 FORMAT(3X,I2,': norm( R - Q''*A ) / ( M * norm(A) * EPS )' )
 8012 FORMAT(3X,I2,': norm( I - Q''*Q ) / ( M * EPS )' )
 8013 FORMAT(3X,I2,': norm( Q*C - Q*C ) / ( M * norm(C) * EPS )' )
 8014 FORMAT(3X,I2,': norm( Q''*C - Q''*C ) / ( M * norm(C) * EPS )')
 8015 FORMAT(3X,I2,': norm( C*Q - C*Q ) / ( M * norm(C) * EPS )' )
 8016 FORMAT(3X,I2,': norm( C*Q'' - C*Q'' ) / ( M * norm(C) * EPS )')
 8017 FORMAT(3X,I2,': norm( R - Q''*A ) / ( (M+N) * norm(A) * EPS )' )
 8018 FORMAT(3X,I2,': norm( I - Q''*Q ) / ( (M+N) * EPS )' )
 8019 FORMAT(3X,I2,': norm( Q*C - Q*C ) / ( (M+N) * norm(C) * EPS )' )
 8020 FORMAT(3X,I2,
     $ ': norm( Q''*C - Q''*C ) / ( (M+N) * norm(C) * EPS )')
 8021 FORMAT(3X,I2,': norm( C*Q - C*Q ) / ( (M+N) * norm(C) * EPS )' )
 8022 FORMAT(3X,I2,
     $ ': norm( C*Q'' - C*Q'' ) / ( (M+N) * norm(C) * EPS )')
 8023 FORMAT(3X,I2,': norm( L - A*Q'' ) / ( (M+N) * norm(A) * EPS )' )
 8024 FORMAT(3X,I2,': norm( I - Q*Q'' ) / ( (M+N) * EPS )' )
 8025 FORMAT(3X,I2,': norm( Q*C - Q*C ) / ( (M+N) * norm(C) * EPS )' )
 8026 FORMAT(3X,I2,
     $ ': norm( Q''*C - Q''*C ) / ( (M+N) * norm(C) * EPS )')
 8027 FORMAT(3X,I2,': norm( C*Q - C*Q ) / ( (M+N) * norm(C) * EPS )' )
 8028 FORMAT(3X,I2,
     $ ': norm( C*Q'' - C*Q'' ) / ( (M+N) * norm(C) * EPS )')
 8029 FORMAT(3X,I2,': norm( L - A*Q'' ) / ( (M+N) * norm(A) * EPS )' )
 8030 FORMAT(3X,I2,': norm( I - Q*Q'' ) / ( (M+N) * EPS )' )
 8031 FORMAT(3X,I2,': norm( Q*C - Q*C ) / ( (M+N) * norm(C) * EPS )' )
 8032 FORMAT(3X,I2,
     $ ': norm( Q''*C - Q''*C ) / ( (M+N) * norm(C) * EPS )')
 8033 FORMAT(3X,I2,': norm( C*Q - C*Q ) / ( (M+N) * norm(C) * EPS )' )
 8034 FORMAT(3X,I2,
     $ ': norm( C*Q'' - C*Q'' ) / ( (M+N) * norm(C) * EPS )')
 8035 FORMAT(3X,I2,': norm( R - Q''*A ) / ( (M+N) * norm(A) * EPS )' )
 8036 FORMAT(3X,I2,': norm( I - Q''*Q ) / ( (M+N) * EPS )' )
 8037 FORMAT(3X,I2,': norm( Q*C - Q*C ) / ( (M+N) * norm(C) * EPS )' )
 8038 FORMAT(3X,I2,
     $ ': norm( Q''*C - Q''*C ) / ( (M+N) * norm(C) * EPS )')
 8039 FORMAT(3X,I2,': norm( C*Q - C*Q ) / ( (M+N) * norm(C) * EPS )' )
 8040 FORMAT(3X,I2,
     $ ': norm( C*Q'' - C*Q'' ) / ( (M+N) * norm(C) * EPS )')
*
      RETURN
*
*     End of ALAHD
*
      END
