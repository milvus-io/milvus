*> \brief \b SLAMRG creates a permutation list to merge the entries of two independently sorted sets into a single set sorted in ascending order.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAMRG + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slamrg.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slamrg.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slamrg.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAMRG( N1, N2, A, STRD1, STRD2, INDEX )
*
*       .. Scalar Arguments ..
*       INTEGER            N1, N2, STRD1, STRD2
*       ..
*       .. Array Arguments ..
*       INTEGER            INDEX( * )
*       REAL               A( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAMRG will create a permutation list which will merge the elements
*> of A (which is composed of two independently sorted sets) into a
*> single set which is sorted in ascending order.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N1
*> \verbatim
*>          N1 is INTEGER
*> \endverbatim
*>
*> \param[in] N2
*> \verbatim
*>          N2 is INTEGER
*>         These arguments contain the respective lengths of the two
*>         sorted lists to be merged.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (N1+N2)
*>         The first N1 elements of A contain a list of numbers which
*>         are sorted in either ascending or descending order.  Likewise
*>         for the final N2 elements.
*> \endverbatim
*>
*> \param[in] STRD1
*> \verbatim
*>          STRD1 is INTEGER
*> \endverbatim
*>
*> \param[in] STRD2
*> \verbatim
*>          STRD2 is INTEGER
*>         These are the strides to be taken through the array A.
*>         Allowable strides are 1 and -1.  They indicate whether a
*>         subset of A is sorted in ascending (STRDx = 1) or descending
*>         (STRDx = -1) order.
*> \endverbatim
*>
*> \param[out] INDEX
*> \verbatim
*>          INDEX is INTEGER array, dimension (N1+N2)
*>         On exit this array will contain a permutation such that
*>         if B( I ) = A( INDEX( I ) ) for I=1,N1+N2, then B will be
*>         sorted in ascending order.
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
*> \date June 2016
*
*> \ingroup auxOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SLAMRG( N1, N2, A, STRD1, STRD2, INDEX )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            N1, N2, STRD1, STRD2
*     ..
*     .. Array Arguments ..
      INTEGER            INDEX( * )
      REAL               A( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, IND1, IND2, N1SV, N2SV
*     ..
*     .. Executable Statements ..
*
      N1SV = N1
      N2SV = N2
      IF( STRD1.GT.0 ) THEN
         IND1 = 1
      ELSE
         IND1 = N1
      END IF
      IF( STRD2.GT.0 ) THEN
         IND2 = 1 + N1
      ELSE
         IND2 = N1 + N2
      END IF
      I = 1
*     while ( (N1SV > 0) & (N2SV > 0) )
   10 CONTINUE
      IF( N1SV.GT.0 .AND. N2SV.GT.0 ) THEN
         IF( A( IND1 ).LE.A( IND2 ) ) THEN
            INDEX( I ) = IND1
            I = I + 1
            IND1 = IND1 + STRD1
            N1SV = N1SV - 1
         ELSE
            INDEX( I ) = IND2
            I = I + 1
            IND2 = IND2 + STRD2
            N2SV = N2SV - 1
         END IF
         GO TO 10
      END IF
*     end while
      IF( N1SV.EQ.0 ) THEN
         DO 20 N1SV = 1, N2SV
            INDEX( I ) = IND2
            I = I + 1
            IND2 = IND2 + STRD2
   20    CONTINUE
      ELSE
*     N2SV .EQ. 0
         DO 30 N2SV = 1, N1SV
            INDEX( I ) = IND1
            I = I + 1
            IND1 = IND1 + STRD1
   30    CONTINUE
      END IF
*
      RETURN
*
*     End of SLAMRG
*
      END
