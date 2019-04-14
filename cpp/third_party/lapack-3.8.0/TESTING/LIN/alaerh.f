*> \brief \b ALAERH
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ALAERH( PATH, SUBNAM, INFO, INFOE, OPTS, M, N, KL, KU,
*                          N5, IMAT, NFAIL, NERRS, NOUT )
*
*       .. Scalar Arguments ..
*       CHARACTER*3        PATH
*       CHARACTER*( * )    SUBNAM
*       CHARACTER*( * )    OPTS
*       INTEGER            IMAT, INFO, INFOE, KL, KU, M, N, N5, NERRS,
*      $                   NFAIL, NOUT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ALAERH is an error handler for the LAPACK routines.  It prints the
*> header if this is the first error message and prints the error code
*> and form of recovery, if any.  The character evaluations in this
*> routine may make it slow, but it should not be called once the LAPACK
*> routines are fully debugged.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name of subroutine SUBNAM.
*> \endverbatim
*>
*> \param[in] SUBNAM
*> \verbatim
*>          SUBNAM is CHARACTER*(*)
*>          The name of the subroutine that returned an error code.
*> \endverbatim
*>
*> \param[in] INFO
*> \verbatim
*>          INFO is INTEGER
*>          The error code returned from routine SUBNAM.
*> \endverbatim
*>
*> \param[in] INFOE
*> \verbatim
*>          INFOE is INTEGER
*>          The expected error code from routine SUBNAM, if SUBNAM were
*>          error-free.  If INFOE = 0, an error message is printed, but
*>          if INFOE.NE.0, we assume only the return code INFO is wrong.
*> \endverbatim
*>
*> \param[in] OPTS
*> \verbatim
*>          OPTS is CHARACTER*(*)
*>          The character options to the subroutine SUBNAM, concatenated
*>          into a single character string.  For example, UPLO = 'U',
*>          TRANS = 'T', and DIAG = 'N' for a triangular routine would
*>          be specified as OPTS = 'UTN'.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The matrix row dimension.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The matrix column dimension.  Accessed only if PATH = xGE or
*>          xGB.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>          The number of sub-diagonals of the matrix.  Accessed only if
*>          PATH = xGB, xPB, or xTB.  Also used for NRHS for PATH = xLS.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>          The number of super-diagonals of the matrix.  Accessed only
*>          if PATH = xGB.
*> \endverbatim
*>
*> \param[in] N5
*> \verbatim
*>          N5 is INTEGER
*>          A fifth integer parameter, may be the blocksize NB or the
*>          number of right hand sides NRHS.
*> \endverbatim
*>
*> \param[in] IMAT
*> \verbatim
*>          IMAT is INTEGER
*>          The matrix type.
*> \endverbatim
*>
*> \param[in] NFAIL
*> \verbatim
*>          NFAIL is INTEGER
*>          The number of prior tests that did not pass the threshold;
*>          used to determine if the header should be printed.
*> \endverbatim
*>
*> \param[in,out] NERRS
*> \verbatim
*>          NERRS is INTEGER
*>          On entry, the number of errors already detected; used to
*>          determine if the header should be printed.
*>          On exit, NERRS is increased by 1.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number on which results are to be printed.
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
      SUBROUTINE ALAERH( PATH, SUBNAM, INFO, INFOE, OPTS, M, N, KL, KU,
     $                   N5, IMAT, NFAIL, NERRS, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      CHARACTER*( * )    SUBNAM
      CHARACTER*( * )    OPTS
      INTEGER            IMAT, INFO, INFOE, KL, KU, M, N, N5, NERRS,
     $                   NFAIL, NOUT
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      CHARACTER          UPLO
      CHARACTER*2        P2
      CHARACTER*3        C3
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, LSAMEN
      EXTERNAL           LSAME, LSAMEN
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          LEN_TRIM
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALADHD, ALAHD
*     ..
*     .. Executable Statements ..
*
      IF( INFO.EQ.0 )
     $   RETURN
      P2 = PATH( 2: 3 )
      C3 = SUBNAM( 4: 6 )
*
*     Print the header if this is the first error message.
*
      IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 ) THEN
         IF( LSAMEN( 3, C3, 'SV ' ) .OR. LSAMEN( 3, C3, 'SVX' ) ) THEN
            CALL ALADHD( NOUT, PATH )
         ELSE
            CALL ALAHD( NOUT, PATH )
         END IF
      END IF
      NERRS = NERRS + 1
*
*     Print the message detailing the error and form of recovery,
*     if any.
*
      IF( LSAMEN( 2, P2, 'GE' ) ) THEN
*
*        xGE:  General matrices
*
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9988 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, M, N, N5,
     $            IMAT
            ELSE
               WRITE( NOUT, FMT = 9975 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, N5, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9984 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, N, N5,
     $            IMAT
            ELSE
               WRITE( NOUT, FMT = 9970 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9992 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9997 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'TRI' ) ) THEN
*
            WRITE( NOUT, FMT = 9971 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, N5, IMAT
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) ) THEN
*
            WRITE( NOUT, FMT = 9978 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, IMAT
*
         ELSE IF( LSAMEN( 3, C3, 'CON' ) ) THEN
*
            WRITE( NOUT, FMT = 9969 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M,
     $         IMAT
*
         ELSE IF( LSAMEN( 3, C3, 'LS ' ) ) THEN
*
            WRITE( NOUT, FMT = 9965 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M, N,
     $         KL, N5, IMAT
*
         ELSE IF( LSAMEN( 3, C3, 'LSX' ) .OR. LSAMEN( 3, C3, 'LSS' ) )
     $             THEN
*
            WRITE( NOUT, FMT = 9974 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, N5, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9963 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M, N5,
     $         IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'GB' ) ) THEN
*
*        xGB:  General band matrices
*
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9989 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, M, N, KL,
     $            KU, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9976 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, KU, N5,
     $            IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9986 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, N, KL, KU,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9972 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, KL, KU, N5,
     $            IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9993 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, KL, KU, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9998 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, KL, KU, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) ) THEN
*
            WRITE( NOUT, FMT = 9977 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, KU, IMAT
*
         ELSE IF( LSAMEN( 3, C3, 'CON' ) ) THEN
*
            WRITE( NOUT, FMT = 9968 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M, KL,
     $         KU, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9964 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M, KL,
     $         KU, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'GT' ) ) THEN
*
*        xGT:  General tridiagonal matrices
*
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9987 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, N, IMAT
            ELSE
               WRITE( NOUT, FMT = 9973 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9984 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, N, N5,
     $            IMAT
            ELSE
               WRITE( NOUT, FMT = 9970 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9992 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9997 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'CON' ) ) THEN
*
            WRITE( NOUT, FMT = 9969 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M,
     $         IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9963 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M, N5,
     $         IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'PO' ) ) THEN
*
*        xPO:  Symmetric or Hermitian positive definite matrices
*
         UPLO = OPTS( 1: 1 )
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9980 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, M,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9956 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, N5, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9979 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, N,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9955 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9990 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9995 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'TRI' ) ) THEN
*
            WRITE( NOUT, FMT = 9956 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, N5, IMAT
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) .OR.
     $            LSAMEN( 3, C3, 'CON' ) ) THEN
*
            WRITE( NOUT, FMT = 9960 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9955 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'PS' ) ) THEN
*
*        xPS:  Symmetric or Hermitian positive semi-definite matrices
*
         UPLO = OPTS( 1: 1 )
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9980 )SUBNAM, INFO, INFOE, UPLO, M,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9956 )SUBNAM, INFO, UPLO, M, N5, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9979 )SUBNAM, INFO, INFOE, UPLO, N,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9955 )SUBNAM, INFO, UPLO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9990 )SUBNAM, INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9995 )SUBNAM, INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'TRI' ) ) THEN
*
            WRITE( NOUT, FMT = 9956 )SUBNAM, INFO, UPLO, M, N5, IMAT
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMT' ) .OR.
     $            LSAMEN( 3, C3, 'CON' ) ) THEN
*
            WRITE( NOUT, FMT = 9960 )SUBNAM, INFO, UPLO, M, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9955 )SUBNAM, INFO, UPLO, M, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'SY' )
     $         .OR. LSAMEN( 2, P2, 'SR' )
     $         .OR. LSAMEN( 2, P2, 'SK' )
     $         .OR. LSAMEN( 2, P2, 'HE' )
     $         .OR. LSAMEN( 2, P2, 'HR' )
     $         .OR. LSAMEN( 2, P2, 'HK' )
     $         .OR. LSAMEN( 2, P2, 'HA' ) ) THEN
*
*        xSY: symmetric indefinite matrices
*             with partial (Bunch-Kaufman) pivoting;
*        xSR: symmetric indefinite matrices
*             with rook (bounded Bunch-Kaufman) pivoting;
*        xSK: symmetric indefinite matrices
*             with rook (bounded Bunch-Kaufman) pivoting,
*             new storage format;
*        xHE: Hermitian indefinite matrices
*             with partial (Bunch-Kaufman) pivoting.
*        xHR: Hermitian indefinite matrices
*             with rook (bounded Bunch-Kaufman) pivoting;
*        xHK: Hermitian indefinite matrices
*             with rook (bounded Bunch-Kaufman) pivoting,
*             new storage format;
*        xHA: Hermitian matrices
*             Aasen Algorithm
*
         UPLO = OPTS( 1: 1 )
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9980 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, M,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9956 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, N5, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 2, C3, 'SV' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9979 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, N,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9955 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9990 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9995 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) .OR.
     $            LSAMEN( 3, C3, 'TRI' ) .OR. LSAMEN( 3, C3, 'CON' ) )
     $             THEN
*
            WRITE( NOUT, FMT = 9960 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9955 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'PP' ) .OR. LSAMEN( 2, P2, 'SP' ) .OR.
     $         LSAMEN( 2, P2, 'HP' ) ) THEN
*
*        xPP, xHP, or xSP:  Symmetric or Hermitian packed matrices
*
         UPLO = OPTS( 1: 1 )
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9983 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, M,
     $            IMAT
            ELSE
               WRITE( NOUT, FMT = 9960 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9979 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, N,
     $            N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9955 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9990 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9995 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) .OR.
     $            LSAMEN( 3, C3, 'TRI' ) .OR. LSAMEN( 3, C3, 'CON' ) )
     $             THEN
*
            WRITE( NOUT, FMT = 9960 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9955 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'PB' ) ) THEN
*
*        xPB:  Symmetric (Hermitian) positive definite band matrix
*
         UPLO = OPTS( 1: 1 )
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9982 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, M,
     $            KL, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9958 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, KL, N5,
     $            IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9981 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, UPLO, N,
     $            KL, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9957 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, N, KL, N5,
     $            IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9991 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), OPTS( 2: 2 ), N, KL, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9996 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $            OPTS( 2: 2 ), N, KL, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) .OR.
     $            LSAMEN( 3, C3, 'CON' ) ) THEN
*
            WRITE( NOUT, FMT = 9959 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, KL, IMAT
*
         ELSE
*
            WRITE( NOUT, FMT = 9957 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, UPLO, M, KL, N5,
     $         IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'PT' ) ) THEN
*
*        xPT:  Positive definite tridiagonal matrices
*
         IF( LSAMEN( 3, C3, 'TRF' ) ) THEN
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9987 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, N, IMAT
            ELSE
               WRITE( NOUT, FMT = 9973 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, IMAT
            END IF
            IF( INFO.NE.0 )
     $         WRITE( NOUT, FMT = 9949 )
*
         ELSE IF( LSAMEN( 3, C3, 'SV ' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9984 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, N, N5,
     $            IMAT
            ELSE
               WRITE( NOUT, FMT = 9970 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, N, N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'SVX' ) ) THEN
*
            IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
               WRITE( NOUT, FMT = 9994 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE,
     $            OPTS( 1: 1 ), N, N5, IMAT
            ELSE
               WRITE( NOUT, FMT = 9999 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), N,
     $            N5, IMAT
            END IF
*
         ELSE IF( LSAMEN( 3, C3, 'CON' ) ) THEN
*
            IF( LSAME( SUBNAM( 1: 1 ), 'S' ) .OR.
     $          LSAME( SUBNAM( 1: 1 ), 'D' ) ) THEN
               WRITE( NOUT, FMT = 9973 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, IMAT
            ELSE
               WRITE( NOUT, FMT = 9969 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M,
     $            IMAT
            END IF
*
         ELSE
*
            WRITE( NOUT, FMT = 9963 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ), M, N5,
     $         IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'TR' ) ) THEN
*
*        xTR:  Triangular matrix
*
         IF( LSAMEN( 3, C3, 'TRI' ) ) THEN
            WRITE( NOUT, FMT = 9961 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), M, N5, IMAT
         ELSE IF( LSAMEN( 3, C3, 'CON' ) ) THEN
            WRITE( NOUT, FMT = 9967 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), M, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATRS' ) ) THEN
            WRITE( NOUT, FMT = 9952 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), OPTS( 4: 4 ), M, IMAT
         ELSE
            WRITE( NOUT, FMT = 9953 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), M, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'TP' ) ) THEN
*
*        xTP:  Triangular packed matrix
*
         IF( LSAMEN( 3, C3, 'TRI' ) ) THEN
            WRITE( NOUT, FMT = 9962 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), M, IMAT
         ELSE IF( LSAMEN( 3, C3, 'CON' ) ) THEN
            WRITE( NOUT, FMT = 9967 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), M, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATPS' ) ) THEN
            WRITE( NOUT, FMT = 9952 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), OPTS( 4: 4 ), M, IMAT
         ELSE
            WRITE( NOUT, FMT = 9953 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), M, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'TB' ) ) THEN
*
*        xTB:  Triangular band matrix
*
         IF( LSAMEN( 3, C3, 'CON' ) ) THEN
            WRITE( NOUT, FMT = 9966 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), M, KL, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATBS' ) ) THEN
            WRITE( NOUT, FMT = 9951 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), OPTS( 4: 4 ), M, KL, IMAT
         ELSE
            WRITE( NOUT, FMT = 9954 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, OPTS( 1: 1 ),
     $         OPTS( 2: 2 ), OPTS( 3: 3 ), M, KL, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'QR' ) ) THEN
*
*        xQR:  QR factorization
*
         IF( LSAMEN( 3, C3, 'QRS' ) ) THEN
            WRITE( NOUT, FMT = 9974 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, N5, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) ) THEN
            WRITE( NOUT, FMT = 9978 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'LQ' ) ) THEN
*
*        xLQ:  LQ factorization
*
         IF( LSAMEN( 3, C3, 'LQS' ) ) THEN
            WRITE( NOUT, FMT = 9974 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, N5, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) ) THEN
            WRITE( NOUT, FMT = 9978 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'QL' ) ) THEN
*
*        xQL:  QL factorization
*
         IF( LSAMEN( 3, C3, 'QLS' ) ) THEN
            WRITE( NOUT, FMT = 9974 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, N5, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) ) THEN
            WRITE( NOUT, FMT = 9978 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'RQ' ) ) THEN
*
*        xRQ:  RQ factorization
*
         IF( LSAMEN( 3, C3, 'RQS' ) ) THEN
            WRITE( NOUT, FMT = 9974 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, KL, N5, IMAT
         ELSE IF( LSAMEN( 5, SUBNAM( 2: 6 ), 'LATMS' ) ) THEN
            WRITE( NOUT, FMT = 9978 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'LU' ) ) THEN
*
         IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
            WRITE( NOUT, FMT = 9988 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, M, N, N5,
     $         IMAT
         ELSE
            WRITE( NOUT, FMT = 9975 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N, N5, IMAT
         END IF
*
      ELSE IF( LSAMEN( 2, P2, 'CH' ) ) THEN
*
         IF( INFO.NE.INFOE .AND. INFOE.NE.0 ) THEN
            WRITE( NOUT, FMT = 9985 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, INFOE, M, N5, IMAT
         ELSE
            WRITE( NOUT, FMT = 9971 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO, M, N5, IMAT
         END IF
*
      ELSE
*
*        Print a generic message if the path is unknown.
*
         WRITE( NOUT, FMT = 9950 )
     $     SUBNAM(1:LEN_TRIM( SUBNAM )), INFO
      END IF
*
*     Description of error message (alphabetical, left to right)
*
*     SUBNAM, INFO, FACT, N, NRHS, IMAT
*
 9999 FORMAT( ' *** Error code from ', A, '=', I5, ', FACT=''', A1,
     $      ''', N=', I5, ', NRHS=', I4, ', type ', I2 )
*
*     SUBNAM, INFO, FACT, TRANS, N, KL, KU, NRHS, IMAT
*
 9998 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> FACT=''',
     $      A1, ''', TRANS=''', A1, ''', N=', I5, ', KL=', I5, ', KU=',
     $      I5, ', NRHS=', I4, ', type ', I1 )
*
*     SUBNAM, INFO, FACT, TRANS, N, NRHS, IMAT
*
 9997 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> FACT=''',
     $      A1, ''', TRANS=''', A1, ''', N =', I5, ', NRHS =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, FACT, UPLO, N, KD, NRHS, IMAT
*
 9996 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> FACT=''',
     $      A1, ''', UPLO=''', A1, ''', N=', I5, ', KD=', I5, ', NRHS=',
     $      I4, ', type ', I2 )
*
*     SUBNAM, INFO, FACT, UPLO, N, NRHS, IMAT
*
 9995 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> FACT=''',
     $      A1, ''', UPLO=''', A1, ''', N =', I5, ', NRHS =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, FACT, N, NRHS, IMAT
*
 9994 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> FACT=''', A1, ''', N =', I5, ', NRHS =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, FACT, TRANS, N, KL, KU, NRHS, IMAT
*
 9993 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> FACT=''', A1, ''', TRANS=''', A1, ''', N=', I5,
     $      ', KL=', I5, ', KU=', I5, ', NRHS=', I4, ', type ', I1 )
*
*     SUBNAM, INFO, INFOE, FACT, TRANS, N, NRHS, IMAT
*
 9992 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> FACT=''', A1, ''', TRANS=''', A1, ''', N =', I5,
     $      ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, FACT, UPLO, N, KD, NRHS, IMAT
*
 9991 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> FACT=''', A1, ''', UPLO=''', A1, ''', N=', I5,
     $      ', KD=', I5, ', NRHS=', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, FACT, UPLO, N, NRHS, IMAT
*
 9990 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> FACT=''', A1, ''', UPLO=''', A1, ''', N =', I5,
     $      ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, M, N, KL, KU, NB, IMAT
*
 9989 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> M = ', I5, ', N =', I5, ', KL =', I5, ', KU =',
     $      I5, ', NB =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, M, N, NB, IMAT
*
 9988 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> M =', I5, ', N =', I5, ', NB =', I4, ', type ',
     $      I2 )
*
*     SUBNAM, INFO, INFOE, N, IMAT
*
 9987 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, ' for N=', I5, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, N, KL, KU, NRHS, IMAT
*
 9986 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> N =', I5, ', KL =', I5, ', KU =', I5,
     $      ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, N, NB, IMAT
*
 9985 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> N =', I5, ', NB =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, N, NRHS, IMAT
*
 9984 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> N =', I5, ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, UPLO, N, IMAT
*
 9983 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> UPLO = ''', A1, ''', N =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, UPLO, N, KD, NB, IMAT
*
 9982 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> UPLO = ''', A1, ''', N =', I5, ', KD =', I5,
     $      ', NB =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, UPLO, N, KD, NRHS, IMAT
*
 9981 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> UPLO=''', A1, ''', N =', I5, ', KD =', I5,
     $      ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, UPLO, N, NB, IMAT
*
 9980 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> UPLO = ''', A1, ''', N =', I5, ', NB =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, INFOE, UPLO, N, NRHS, IMAT
*
 9979 FORMAT( ' *** ', A, ' returned with INFO =', I5, ' instead of ',
     $      I2, / ' ==> UPLO = ''', A1, ''', N =', I5, ', NRHS =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, M, N, IMAT
*
 9978 FORMAT( ' *** Error code from ', A, ' =', I5, ' for M =', I5,
     $      ', N =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, M, N, KL, KU, IMAT
*
 9977 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> M = ', I5,
     $      ', N =', I5, ', KL =', I5, ', KU =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, M, N, KL, KU, NB, IMAT
*
 9976 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> M = ', I5,
     $      ', N =', I5, ', KL =', I5, ', KU =', I5, ', NB =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, M, N, NB, IMAT
*
 9975 FORMAT( ' *** Error code from ', A, '=', I5, ' for M=', I5,
     $      ', N=', I5, ', NB=', I4, ', type ', I2 )
*
*     SUBNAM, INFO, M, N, NRHS, NB, IMAT
*
 9974 FORMAT( ' *** Error code from ', A, '=', I5, / ' ==> M =', I5,
     $      ', N =', I5, ', NRHS =', I4, ', NB =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, N, IMAT
*
 9973 FORMAT( ' *** Error code from ', A, ' =', I5, ' for N =', I5,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, N, KL, KU, NRHS, IMAT
*
 9972 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> N =', I5,
     $      ', KL =', I5, ', KU =', I5, ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, N, NB, IMAT
*
 9971 FORMAT( ' *** Error code from ', A, '=', I5, ' for N=', I5,
     $      ', NB=', I4, ', type ', I2 )
*
*     SUBNAM, INFO, N, NRHS, IMAT
*
 9970 FORMAT( ' *** Error code from ', A, ' =', I5, ' for N =', I5,
     $      ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, NORM, N, IMAT
*
 9969 FORMAT( ' *** Error code from ', A, ' =', I5, ' for NORM = ''',
     $      A1, ''', N =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, NORM, N, KL, KU, IMAT
*
 9968 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> NORM =''',
     $      A1, ''', N =', I5, ', KL =', I5, ', KU =', I5, ', type ',
     $      I2 )
*
*     SUBNAM, INFO, NORM, UPLO, DIAG, N, IMAT
*
 9967 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> NORM=''',
     $      A1, ''', UPLO =''', A1, ''', DIAG=''', A1, ''', N =', I5,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, NORM, UPLO, DIAG, N, KD, IMAT
*
 9966 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> NORM=''',
     $      A1, ''', UPLO =''', A1, ''', DIAG=''', A1, ''', N=', I5,
     $      ', KD=', I5, ', type ', I2 )
*
*     SUBNAM, INFO, TRANS, M, N, NRHS, NB, IMAT
*
 9965 FORMAT( ' *** Error code from ', A, ' =', I5,
     $      / ' ==> TRANS = ''', A1, ''', M =', I5, ', N =', I5,
     $      ', NRHS =', I4, ', NB =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, TRANS, N, KL, KU, NRHS, IMAT
*
 9964 FORMAT( ' *** Error code from ', A, '=', I5, / ' ==> TRANS=''',
     $      A1, ''', N =', I5, ', KL =', I5, ', KU =', I5, ', NRHS =',
     $      I4, ', type ', I2 )
*
*     SUBNAM, INFO, TRANS, N, NRHS, IMAT
*
 9963 FORMAT( ' *** Error code from ', A, ' =', I5,
     $      / ' ==> TRANS = ''', A1, ''', N =', I5, ', NRHS =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, DIAG, N, IMAT
*
 9962 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO=''',
     $      A1, ''', DIAG =''', A1, ''', N =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, DIAG, N, NB, IMAT
*
 9961 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO=''',
     $      A1, ''', DIAG =''', A1, ''', N =', I5, ', NB =', I4,
     $      ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, N, IMAT
*
 9960 FORMAT( ' *** Error code from ', A, ' =', I5, ' for UPLO = ''',
     $      A1, ''', N =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, N, KD, IMAT
*
 9959 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO = ''',
     $      A1, ''', N =', I5, ', KD =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, N, KD, NB, IMAT
*
 9958 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO = ''',
     $      A1, ''', N =', I5, ', KD =', I5, ', NB =', I4, ', type ',
     $      I2 )
*
*     SUBNAM, INFO, UPLO, N, KD, NRHS, IMAT
*
 9957 FORMAT( ' *** Error code from ', A, '=', I5, / ' ==> UPLO = ''',
     $      A1, ''', N =', I5, ', KD =', I5, ', NRHS =', I4, ', type ',
     $      I2 )
*
*     SUBNAM, INFO, UPLO, N, NB, IMAT
*
 9956 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO = ''',
     $      A1, ''', N =', I5, ', NB =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, N, NRHS, IMAT
*
 9955 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO = ''',
     $      A1, ''', N =', I5, ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, TRANS, DIAG, N, KD, NRHS, IMAT
*
 9954 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO=''',
     $      A1, ''', TRANS=''', A1, ''', DIAG=''', A1, ''', N=', I5,
     $      ', KD=', I5, ', NRHS=', I4, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, TRANS, DIAG, N, NRHS, IMAT
*
 9953 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO=''',
     $      A1, ''', TRANS=''', A1, ''', DIAG=''', A1, ''', N =', I5,
     $      ', NRHS =', I4, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, TRANS, DIAG, NORMIN, N, IMAT
*
 9952 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO=''',
     $      A1, ''', TRANS=''', A1, ''', DIAG=''', A1, ''', NORMIN=''',
     $      A1, ''', N =', I5, ', type ', I2 )
*
*     SUBNAM, INFO, UPLO, TRANS, DIAG, NORMIN, N, KD, IMAT
*
 9951 FORMAT( ' *** Error code from ', A, ' =', I5, / ' ==> UPLO=''',
     $      A1, ''', TRANS=''', A1, ''', DIAG=''', A1, ''', NORMIN=''',
     $      A1, ''', N=', I5, ', KD=', I5, ', type ', I2 )
*
*     Unknown type
*
 9950 FORMAT( ' *** Error code from ', A, ' =', I5 )
*
*     What we do next
*
 9949 FORMAT( ' ==> Doing only the condition estimate for this case' )
*
      RETURN
*
*     End of ALAERH
*
      END
