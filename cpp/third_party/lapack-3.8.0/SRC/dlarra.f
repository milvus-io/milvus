*> \brief \b DLARRA computes the splitting points with the specified threshold.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLARRA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlarra.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlarra.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlarra.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLARRA( N, D, E, E2, SPLTOL, TNRM,
*                           NSPLIT, ISPLIT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, N, NSPLIT
*       DOUBLE PRECISION    SPLTOL, TNRM
*       ..
*       .. Array Arguments ..
*       INTEGER            ISPLIT( * )
*       DOUBLE PRECISION   D( * ), E( * ), E2( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Compute the splitting points with threshold SPLTOL.
*> DLARRA sets any "small" off-diagonal elements to zero.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix. N > 0.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          On entry, the N diagonal elements of the tridiagonal
*>          matrix T.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N)
*>          On entry, the first (N-1) entries contain the subdiagonal
*>          elements of the tridiagonal matrix T; E(N) need not be set.
*>          On exit, the entries E( ISPLIT( I ) ), 1 <= I <= NSPLIT,
*>          are set to zero, the other entries of E are untouched.
*> \endverbatim
*>
*> \param[in,out] E2
*> \verbatim
*>          E2 is DOUBLE PRECISION array, dimension (N)
*>          On entry, the first (N-1) entries contain the SQUARES of the
*>          subdiagonal elements of the tridiagonal matrix T;
*>          E2(N) need not be set.
*>          On exit, the entries E2( ISPLIT( I ) ),
*>          1 <= I <= NSPLIT, have been set to zero
*> \endverbatim
*>
*> \param[in] SPLTOL
*> \verbatim
*>          SPLTOL is DOUBLE PRECISION
*>          The threshold for splitting. Two criteria can be used:
*>          SPLTOL<0 : criterion based on absolute off-diagonal value
*>          SPLTOL>0 : criterion that preserves relative accuracy
*> \endverbatim
*>
*> \param[in] TNRM
*> \verbatim
*>          TNRM is DOUBLE PRECISION
*>          The norm of the matrix.
*> \endverbatim
*>
*> \param[out] NSPLIT
*> \verbatim
*>          NSPLIT is INTEGER
*>          The number of blocks T splits into. 1 <= NSPLIT <= N.
*> \endverbatim
*>
*> \param[out] ISPLIT
*> \verbatim
*>          ISPLIT is INTEGER array, dimension (N)
*>          The splitting points, at which T breaks up into blocks.
*>          The first block consists of rows/columns 1 to ISPLIT(1),
*>          the second of rows/columns ISPLIT(1)+1 through ISPLIT(2),
*>          etc., and the NSPLIT-th consists of rows/columns
*>          ISPLIT(NSPLIT-1)+1 through ISPLIT(NSPLIT)=N.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
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
*> \date June 2017
*
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*> Beresford Parlett, University of California, Berkeley, USA \n
*> Jim Demmel, University of California, Berkeley, USA \n
*> Inderjit Dhillon, University of Texas, Austin, USA \n
*> Osni Marques, LBNL/NERSC, USA \n
*> Christof Voemel, University of California, Berkeley, USA
*
*  =====================================================================
      SUBROUTINE DLARRA( N, D, E, E2, SPLTOL, TNRM,
     $                    NSPLIT, ISPLIT, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, N, NSPLIT
      DOUBLE PRECISION    SPLTOL, TNRM
*     ..
*     .. Array Arguments ..
      INTEGER            ISPLIT( * )
      DOUBLE PRECISION   D( * ), E( * ), E2( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      DOUBLE PRECISION   EABS, TMP1

*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
      INFO = 0
*
*     Quick return if possible
*
      IF( N.LE.0 ) THEN
         RETURN
      END IF
*
*     Compute splitting points
      NSPLIT = 1
      IF(SPLTOL.LT.ZERO) THEN
*        Criterion based on absolute off-diagonal value
         TMP1 = ABS(SPLTOL)* TNRM
         DO 9 I = 1, N-1
            EABS = ABS( E(I) )
            IF( EABS .LE. TMP1) THEN
               E(I) = ZERO
               E2(I) = ZERO
               ISPLIT( NSPLIT ) = I
               NSPLIT = NSPLIT + 1
            END IF
 9       CONTINUE
      ELSE
*        Criterion that guarantees relative accuracy
         DO 10 I = 1, N-1
            EABS = ABS( E(I) )
            IF( EABS .LE. SPLTOL * SQRT(ABS(D(I)))*SQRT(ABS(D(I+1))) )
     $      THEN
               E(I) = ZERO
               E2(I) = ZERO
               ISPLIT( NSPLIT ) = I
               NSPLIT = NSPLIT + 1
            END IF
 10      CONTINUE
      ENDIF
      ISPLIT( NSPLIT ) = N

      RETURN
*
*     End of DLARRA
*
      END
