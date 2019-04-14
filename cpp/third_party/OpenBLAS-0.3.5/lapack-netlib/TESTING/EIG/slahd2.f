*> \brief \b SLAHD2
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAHD2( IOUNIT, PATH )
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
*> SLAHD2 prints header information for the different test paths.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IOUNIT
*> \verbatim
*>          IOUNIT is INTEGER.
*>          On entry, IOUNIT specifies the unit number to which the
*>          header information should be printed.
*> \endverbatim
*>
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3.
*>          On entry, PATH contains the name of the path for which the
*>          header information is to be printed.  Current paths are
*>
*>             SHS, CHS:  Non-symmetric eigenproblem.
*>             SST, CST:  Symmetric eigenproblem.
*>             SSG, CSG:  Symmetric Generalized eigenproblem.
*>             SBD, CBD:  Singular Value Decomposition (SVD)
*>             SBB, CBB:  General Banded reduction to bidiagonal form
*>
*>          These paths also are supplied in double precision (replace
*>          leading S by D and leading C by Z in path names).
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
      SUBROUTINE SLAHD2( IOUNIT, PATH )
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
      CHARACTER*2        C2
      INTEGER            J
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, LSAMEN
      EXTERNAL           LSAME, LSAMEN
*     ..
*     .. Executable Statements ..
*
      IF( IOUNIT.LE.0 )
     $   RETURN
      SORD = LSAME( PATH, 'S' ) .OR. LSAME( PATH, 'D' )
      CORZ = LSAME( PATH, 'C' ) .OR. LSAME( PATH, 'Z' )
      IF( .NOT.SORD .AND. .NOT.CORZ ) THEN
         WRITE( IOUNIT, FMT = 9999 )PATH
      END IF
      C2 = PATH( 2: 3 )
*
      IF( LSAMEN( 2, C2, 'HS' ) ) THEN
         IF( SORD ) THEN
*
*           Real Non-symmetric Eigenvalue Problem:
*
            WRITE( IOUNIT, FMT = 9998 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9988 )
            WRITE( IOUNIT, FMT = 9987 )
            WRITE( IOUNIT, FMT = 9986 )'pairs ', 'pairs ', 'prs.',
     $         'prs.'
            WRITE( IOUNIT, FMT = 9985 )
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9984 )'orthogonal', '''=transpose',
     $         ( '''', J = 1, 6 )
*
         ELSE
*
*           Complex Non-symmetric Eigenvalue Problem:
*
            WRITE( IOUNIT, FMT = 9997 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9988 )
            WRITE( IOUNIT, FMT = 9987 )
            WRITE( IOUNIT, FMT = 9986 )'e.vals', 'e.vals', 'e.vs',
     $         'e.vs'
            WRITE( IOUNIT, FMT = 9985 )
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9984 )'unitary', '*=conj.transp.',
     $         ( '*', J = 1, 6 )
         END IF
*
      ELSE IF( LSAMEN( 2, C2, 'ST' ) ) THEN
*
         IF( SORD ) THEN
*
*           Real Symmetric Eigenvalue Problem:
*
            WRITE( IOUNIT, FMT = 9996 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9983 )
            WRITE( IOUNIT, FMT = 9982 )
            WRITE( IOUNIT, FMT = 9981 )'Symmetric'
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9968 )
*
         ELSE
*
*           Complex Hermitian Eigenvalue Problem:
*
            WRITE( IOUNIT, FMT = 9995 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9983 )
            WRITE( IOUNIT, FMT = 9982 )
            WRITE( IOUNIT, FMT = 9981 )'Hermitian'
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9967 )
         END IF
*
      ELSE IF( LSAMEN( 2, C2, 'SG' ) ) THEN
*
         IF( SORD ) THEN
*
*           Real Symmetric Generalized Eigenvalue Problem:
*
            WRITE( IOUNIT, FMT = 9992 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9980 )
            WRITE( IOUNIT, FMT = 9979 )
            WRITE( IOUNIT, FMT = 9978 )'Symmetric'
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9977 )
            WRITE( IOUNIT, FMT = 9976 )
*
         ELSE
*
*           Complex Hermitian Generalized Eigenvalue Problem:
*
            WRITE( IOUNIT, FMT = 9991 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9980 )
            WRITE( IOUNIT, FMT = 9979 )
            WRITE( IOUNIT, FMT = 9978 )'Hermitian'
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9975 )
            WRITE( IOUNIT, FMT = 9974 )
*
         END IF
*
      ELSE IF( LSAMEN( 2, C2, 'BD' ) ) THEN
*
         IF( SORD ) THEN
*
*           Real Singular Value Decomposition:
*
            WRITE( IOUNIT, FMT = 9994 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9973 )
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9972 )'orthogonal'
            WRITE( IOUNIT, FMT = 9971 )
         ELSE
*
*           Complex Singular Value Decomposition:
*
            WRITE( IOUNIT, FMT = 9993 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9973 )
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9972 )'unitary   '
            WRITE( IOUNIT, FMT = 9971 )
         END IF
*
      ELSE IF( LSAMEN( 2, C2, 'BB' ) ) THEN
*
         IF( SORD ) THEN
*
*           Real General Band reduction to bidiagonal form:
*
            WRITE( IOUNIT, FMT = 9990 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9970 )
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9969 )'orthogonal'
         ELSE
*
*           Complex Band reduction to bidiagonal form:
*
            WRITE( IOUNIT, FMT = 9989 )PATH
*
*           Matrix types
*
            WRITE( IOUNIT, FMT = 9970 )
*
*           Tests performed
*
            WRITE( IOUNIT, FMT = 9969 )'unitary   '
         END IF
*
      ELSE
*
         WRITE( IOUNIT, FMT = 9999 )PATH
         RETURN
      END IF
*
      RETURN
*
 9999 FORMAT( 1X, A3, ':  no header available' )
 9998 FORMAT( / 1X, A3, ' -- Real Non-symmetric eigenvalue problem' )
 9997 FORMAT( / 1X, A3, ' -- Complex Non-symmetric eigenvalue problem' )
 9996 FORMAT( / 1X, A3, ' -- Real Symmetric eigenvalue problem' )
 9995 FORMAT( / 1X, A3, ' -- Complex Hermitian eigenvalue problem' )
 9994 FORMAT( / 1X, A3, ' -- Real Singular Value Decomposition' )
 9993 FORMAT( / 1X, A3, ' -- Complex Singular Value Decomposition' )
 9992 FORMAT( / 1X, A3, ' -- Real Symmetric Generalized eigenvalue ',
     $      'problem' )
 9991 FORMAT( / 1X, A3, ' -- Complex Hermitian Generalized eigenvalue ',
     $      'problem' )
 9990 FORMAT( / 1X, A3, ' -- Real Band reduc. to bidiagonal form' )
 9989 FORMAT( / 1X, A3, ' -- Complex Band reduc. to bidiagonal form' )
*
 9988 FORMAT( ' Matrix types (see xCHKHS for details): ' )
*
 9987 FORMAT( / ' Special Matrices:', / '  1=Zero matrix.             ',
     $      '           ', '  5=Diagonal: geometr. spaced entries.',
     $      / '  2=Identity matrix.                    ', '  6=Diagona',
     $      'l: clustered entries.', / '  3=Transposed Jordan block.  ',
     $      '          ', '  7=Diagonal: large, evenly spaced.', / '  ',
     $      '4=Diagonal: evenly spaced entries.    ', '  8=Diagonal: s',
     $      'mall, evenly spaced.' )
 9986 FORMAT( ' Dense, Non-Symmetric Matrices:', / '  9=Well-cond., ev',
     $      'enly spaced eigenvals.', ' 14=Ill-cond., geomet. spaced e',
     $      'igenals.', / ' 10=Well-cond., geom. spaced eigenvals. ',
     $      ' 15=Ill-conditioned, clustered e.vals.', / ' 11=Well-cond',
     $      'itioned, clustered e.vals. ', ' 16=Ill-cond., random comp',
     $      'lex ', A6, / ' 12=Well-cond., random complex ', A6, '   ',
     $      ' 17=Ill-cond., large rand. complx ', A4, / ' 13=Ill-condi',
     $      'tioned, evenly spaced.     ', ' 18=Ill-cond., small rand.',
     $      ' complx ', A4 )
 9985 FORMAT( ' 19=Matrix with random O(1) entries.    ', ' 21=Matrix ',
     $      'with small random entries.', / ' 20=Matrix with large ran',
     $      'dom entries.   ' )
 9984 FORMAT( / ' Tests performed:   ', '(H is Hessenberg, T is Schur,',
     $      ' U and Z are ', A, ',', / 20X, A, ', W is a diagonal matr',
     $      'ix of eigenvalues,', / 20X, 'L and R are the left and rig',
     $      'ht eigenvector matrices)', / '  1 = | A - U H U', A1, ' |',
     $      ' / ( |A| n ulp )         ', '  2 = | I - U U', A1, ' | / ',
     $      '( n ulp )', / '  3 = | H - Z T Z', A1, ' | / ( |H| n ulp ',
     $      ')         ', '  4 = | I - Z Z', A1, ' | / ( n ulp )',
     $      / '  5 = | A - UZ T (UZ)', A1, ' | / ( |A| n ulp )     ',
     $      '  6 = | I - UZ (UZ)', A1, ' | / ( n ulp )', / '  7 = | T(',
     $      'e.vects.) - T(no e.vects.) | / ( |T| ulp )', / '  8 = | W',
     $      '(e.vects.) - W(no e.vects.) | / ( |W| ulp )', / '  9 = | ',
     $      'TR - RW | / ( |T| |R| ulp )     ', ' 10 = | LT - WL | / (',
     $      ' |T| |L| ulp )', / ' 11= |HX - XW| / (|H| |X| ulp)  (inv.',
     $      'it)', ' 12= |YH - WY| / (|H| |Y| ulp)  (inv.it)' )
*
*     Symmetric/Hermitian eigenproblem
*
 9983 FORMAT( ' Matrix types (see xDRVST for details): ' )
*
 9982 FORMAT( / ' Special Matrices:', / '  1=Zero matrix.             ',
     $      '           ', '  5=Diagonal: clustered entries.', / '  2=',
     $      'Identity matrix.                    ', '  6=Diagonal: lar',
     $      'ge, evenly spaced.', / '  3=Diagonal: evenly spaced entri',
     $      'es.    ', '  7=Diagonal: small, evenly spaced.', / '  4=D',
     $      'iagonal: geometr. spaced entries.' )
 9981 FORMAT( ' Dense ', A, ' Matrices:', / '  8=Evenly spaced eigen',
     $      'vals.            ', ' 12=Small, evenly spaced eigenvals.',
     $      / '  9=Geometrically spaced eigenvals.     ', ' 13=Matrix ',
     $      'with random O(1) entries.', / ' 10=Clustered eigenvalues.',
     $      '              ', ' 14=Matrix with large random entries.',
     $      / ' 11=Large, evenly spaced eigenvals.     ', ' 15=Matrix ',
     $      'with small random entries.' )
*
*     Symmetric/Hermitian Generalized eigenproblem
*
 9980 FORMAT( ' Matrix types (see xDRVSG for details): ' )
*
 9979 FORMAT( / ' Special Matrices:', / '  1=Zero matrix.             ',
     $      '           ', '  5=Diagonal: clustered entries.', / '  2=',
     $      'Identity matrix.                    ', '  6=Diagonal: lar',
     $      'ge, evenly spaced.', / '  3=Diagonal: evenly spaced entri',
     $      'es.    ', '  7=Diagonal: small, evenly spaced.', / '  4=D',
     $      'iagonal: geometr. spaced entries.' )
 9978 FORMAT( ' Dense or Banded ', A, ' Matrices: ',
     $      / '  8=Evenly spaced eigenvals.         ',
     $      ' 15=Matrix with small random entries.',
     $      / '  9=Geometrically spaced eigenvals.  ',
     $      ' 16=Evenly spaced eigenvals, KA=1, KB=1.',
     $      / ' 10=Clustered eigenvalues.           ',
     $      ' 17=Evenly spaced eigenvals, KA=2, KB=1.',
     $      / ' 11=Large, evenly spaced eigenvals.  ',
     $      ' 18=Evenly spaced eigenvals, KA=2, KB=2.',
     $      / ' 12=Small, evenly spaced eigenvals.  ',
     $      ' 19=Evenly spaced eigenvals, KA=3, KB=1.',
     $      / ' 13=Matrix with random O(1) entries. ',
     $      ' 20=Evenly spaced eigenvals, KA=3, KB=2.',
     $      / ' 14=Matrix with large random entries.',
     $      ' 21=Evenly spaced eigenvals, KA=3, KB=3.' )
 9977 FORMAT( / ' Tests performed:   ',
     $      / '( For each pair (A,B), where A is of the given type ',
     $      / ' and B is a random well-conditioned matrix. D is ',
     $      / ' diagonal, and Z is orthogonal. )',
     $      / ' 1 = SSYGV, with ITYPE=1 and UPLO=''U'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 2 = SSPGV, with ITYPE=1 and UPLO=''U'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 3 = SSBGV, with ITYPE=1 and UPLO=''U'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 4 = SSYGV, with ITYPE=1 and UPLO=''L'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 5 = SSPGV, with ITYPE=1 and UPLO=''L'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 6 = SSBGV, with ITYPE=1 and UPLO=''L'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ' )
 9976 FORMAT( ' 7 = SSYGV, with ITYPE=2 and UPLO=''U'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 8 = SSPGV, with ITYPE=2 and UPLO=''U'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 9 = SSPGV, with ITYPE=2 and UPLO=''L'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '10 = SSPGV, with ITYPE=2 and UPLO=''L'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '11 = SSYGV, with ITYPE=3 and UPLO=''U'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '12 = SSPGV, with ITYPE=3 and UPLO=''U'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '13 = SSYGV, with ITYPE=3 and UPLO=''L'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '14 = SSPGV, with ITYPE=3 and UPLO=''L'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ' )
 9975 FORMAT( / ' Tests performed:   ',
     $      / '( For each pair (A,B), where A is of the given type ',
     $      / ' and B is a random well-conditioned matrix. D is ',
     $      / ' diagonal, and Z is unitary. )',
     $      / ' 1 = CHEGV, with ITYPE=1 and UPLO=''U'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 2 = CHPGV, with ITYPE=1 and UPLO=''U'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 3 = CHBGV, with ITYPE=1 and UPLO=''U'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 4 = CHEGV, with ITYPE=1 and UPLO=''L'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 5 = CHPGV, with ITYPE=1 and UPLO=''L'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 6 = CHBGV, with ITYPE=1 and UPLO=''L'':',
     $      '  | A Z - B Z D | / ( |A| |Z| n ulp )     ' )
 9974 FORMAT( ' 7 = CHEGV, with ITYPE=2 and UPLO=''U'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 8 = CHPGV, with ITYPE=2 and UPLO=''U'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / ' 9 = CHPGV, with ITYPE=2 and UPLO=''L'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '10 = CHPGV, with ITYPE=2 and UPLO=''L'':',
     $      '  | A B Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '11 = CHEGV, with ITYPE=3 and UPLO=''U'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '12 = CHPGV, with ITYPE=3 and UPLO=''U'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '13 = CHEGV, with ITYPE=3 and UPLO=''L'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ',
     $      / '14 = CHPGV, with ITYPE=3 and UPLO=''L'':',
     $      '  | B A Z - Z D | / ( |A| |Z| n ulp )     ' )
*
*     Singular Value Decomposition
*
 9973 FORMAT( ' Matrix types (see xCHKBD for details):',
     $      / ' Diagonal matrices:', / '   1: Zero', 28X,
     $      ' 5: Clustered entries', / '   2: Identity', 24X,
     $      ' 6: Large, evenly spaced entries',
     $      / '   3: Evenly spaced entries', 11X,
     $      ' 7: Small, evenly spaced entries',
     $      / '   4: Geometrically spaced entries',
     $      / ' General matrices:', / '   8: Evenly spaced sing. vals.',
     $      7X, '12: Small, evenly spaced sing vals',
     $      / '   9: Geometrically spaced sing vals  ',
     $      '13: Random, O(1) entries', / '  10: Clustered sing. vals.',
     $      11X, '14: Random, scaled near overflow',
     $      / '  11: Large, evenly spaced sing vals  ',
     $      '15: Random, scaled near underflow' )
*
 9972 FORMAT( / ' Test ratios:  ',
     $      '(B: bidiagonal, S: diagonal, Q, P, U, and V: ', A10, / 16X,
     $      'X: m x nrhs, Y = Q'' X, and Z = U'' Y)' )
 9971 FORMAT( '   1: norm( A - Q B P'' ) / ( norm(A) max(m,n) ulp )',
     $      / '   2: norm( I - Q'' Q )   / ( m ulp )',
     $      / '   3: norm( I - P'' P )   / ( n ulp )',
     $      / '   4: norm( B - U S V'' ) / ( norm(B) min(m,n) ulp )',
     $      / '   5: norm( Y - U Z )    / ',
     $        '( norm(Z) max(min(m,n),k) ulp )',
     $      / '   6: norm( I - U'' U )   / ( min(m,n) ulp )',
     $      / '   7: norm( I - V'' V )   / ( min(m,n) ulp )',
     $      / '   8: Test ordering of S  (0 if nondecreasing, 1/ulp ',
     $        ' otherwise)',
     $      / '   9: norm( S - S1 )     / ( norm(S) ulp ),',
     $        ' where S1 is computed', / 43X,
     $        ' without computing U and V''',
     $      / '  10: Sturm sequence test ',
     $        '(0 if sing. vals of B within THRESH of S)',
     $      / '  11: norm( A - (QU) S (V'' P'') ) / ',
     $        '( norm(A) max(m,n) ulp )',
     $      / '  12: norm( X - (QU) Z )         / ( |X| max(M,k) ulp )',
     $      / '  13: norm( I - (QU)''(QU) )      / ( M ulp )',
     $      / '  14: norm( I - (V'' P'') (P V) )  / ( N ulp )',
     $      / '  15: norm( B - U S V'' ) / ( norm(B) min(m,n) ulp )',
     $      / '  16: norm( I - U'' U )   / ( min(m,n) ulp )',
     $      / '  17: norm( I - V'' V )   / ( min(m,n) ulp )',
     $      / '  18: Test ordering of S  (0 if nondecreasing, 1/ulp ',
     $        ' otherwise)',
     $      / '  19: norm( S - S1 )     / ( norm(S) ulp ),',
     $        ' where S1 is computed', / 43X,
     $        ' without computing U and V''',
     $      / '  20: norm( B - U S V'' )  / ( norm(B) min(m,n) ulp )',
     $        '  SBDSVX(V,A)',
     $      / '  21: norm( I - U'' U )    / ( min(m,n) ulp )',
     $      / '  22: norm( I - V'' V )    / ( min(m,n) ulp )',
     $      / '  23: Test ordering of S  (0 if nondecreasing, 1/ulp ',
     $        ' otherwise)',
     $      / '  24: norm( S - S1 )      / ( norm(S) ulp ),',
     $        ' where S1 is computed', / 44X,
     $        ' without computing U and V''',
     $      / '  25: norm( S - U'' B V ) / ( norm(B) n ulp )',
     $        '  SBDSVX(V,I)',
     $      / '  26: norm( I - U'' U )    / ( min(m,n) ulp )',
     $      / '  27: norm( I - V'' V )    / ( min(m,n) ulp )',
     $      / '  28: Test ordering of S  (0 if nondecreasing, 1/ulp ',
     $        ' otherwise)',
     $      / '  29: norm( S - S1 )      / ( norm(S) ulp ),',
     $        ' where S1 is computed', / 44X,
     $        ' without computing U and V''',
     $      / '  30: norm( S - U'' B V ) / ( norm(B) n ulp )',
     $        '  SBDSVX(V,V)',
     $      / '  31: norm( I - U'' U )    / ( min(m,n) ulp )',
     $      / '  32: norm( I - V'' V )    / ( min(m,n) ulp )',
     $      / '  33: Test ordering of S  (0 if nondecreasing, 1/ulp ',
     $        ' otherwise)',
     $      / '  34: norm( S - S1 )      / ( norm(S) ulp ),',
     $        ' where S1 is computed', / 44X,
     $        ' without computing U and V''' )
*
*     Band reduction to bidiagonal form
*
 9970 FORMAT( ' Matrix types (see xCHKBB for details):',
     $      / ' Diagonal matrices:', / '   1: Zero', 28X,
     $      ' 5: Clustered entries', / '   2: Identity', 24X,
     $      ' 6: Large, evenly spaced entries',
     $      / '   3: Evenly spaced entries', 11X,
     $      ' 7: Small, evenly spaced entries',
     $      / '   4: Geometrically spaced entries',
     $      / ' General matrices:', / '   8: Evenly spaced sing. vals.',
     $      7X, '12: Small, evenly spaced sing vals',
     $      / '   9: Geometrically spaced sing vals  ',
     $      '13: Random, O(1) entries', / '  10: Clustered sing. vals.',
     $      11X, '14: Random, scaled near overflow',
     $      / '  11: Large, evenly spaced sing vals  ',
     $      '15: Random, scaled near underflow' )
*
 9969 FORMAT( / ' Test ratios:  ', '(B: upper bidiagonal, Q and P: ',
     $      A10, / 16X, 'C: m x nrhs, PT = P'', Y = Q'' C)',
     $      / ' 1: norm( A - Q B PT ) / ( norm(A) max(m,n) ulp )',
     $      / ' 2: norm( I - Q'' Q )   / ( m ulp )',
     $      / ' 3: norm( I - PT PT'' )   / ( n ulp )',
     $      / ' 4: norm( Y - Q'' C )   / ( norm(Y) max(m,nrhs) ulp )' )
 9968 FORMAT( / ' Tests performed:  See sdrvst.f' )
 9967 FORMAT( / ' Tests performed:  See cdrvst.f' )
*
*     End of SLAHD2
*
      END
