*> \brief \b DLARRC computes the number of eigenvalues of the symmetric tridiagonal matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLARRC + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlarrc.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlarrc.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlarrc.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLARRC( JOBT, N, VL, VU, D, E, PIVMIN,
*                                   EIGCNT, LCNT, RCNT, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBT
*       INTEGER            EIGCNT, INFO, LCNT, N, RCNT
*       DOUBLE PRECISION   PIVMIN, VL, VU
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * ), E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Find the number of eigenvalues of the symmetric tridiagonal matrix T
*> that are in the interval (VL,VU] if JOBT = 'T', and of L D L^T
*> if JOBT = 'L'.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBT
*> \verbatim
*>          JOBT is CHARACTER*1
*>          = 'T':  Compute Sturm count for matrix T.
*>          = 'L':  Compute Sturm count for matrix L D L^T.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix. N > 0.
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is DOUBLE PRECISION
*>          The lower bound for the eigenvalues.
*> \endverbatim
*>
*> \param[in] VU
*> \verbatim
*>          VU is DOUBLE PRECISION
*>          The upper bound for the eigenvalues.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          JOBT = 'T': The N diagonal elements of the tridiagonal matrix T.
*>          JOBT = 'L': The N diagonal elements of the diagonal matrix D.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N)
*>          JOBT = 'T': The N-1 offdiagonal elements of the matrix T.
*>          JOBT = 'L': The N-1 offdiagonal elements of the matrix L.
*> \endverbatim
*>
*> \param[in] PIVMIN
*> \verbatim
*>          PIVMIN is DOUBLE PRECISION
*>          The minimum pivot in the Sturm sequence for T.
*> \endverbatim
*>
*> \param[out] EIGCNT
*> \verbatim
*>          EIGCNT is INTEGER
*>          The number of eigenvalues of the symmetric tridiagonal matrix T
*>          that are in the interval (VL,VU]
*> \endverbatim
*>
*> \param[out] LCNT
*> \verbatim
*>          LCNT is INTEGER
*> \endverbatim
*>
*> \param[out] RCNT
*> \verbatim
*>          RCNT is INTEGER
*>          The left and right negcounts of the interval.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
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
      SUBROUTINE DLARRC( JOBT, N, VL, VU, D, E, PIVMIN,
     $                            EIGCNT, LCNT, RCNT, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBT
      INTEGER            EIGCNT, INFO, LCNT, N, RCNT
      DOUBLE PRECISION   PIVMIN, VL, VU
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * ), E( * )
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
      LOGICAL            MATT
      DOUBLE PRECISION   LPIVOT, RPIVOT, SL, SU, TMP, TMP2

*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
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
      LCNT = 0
      RCNT = 0
      EIGCNT = 0
      MATT = LSAME( JOBT, 'T' )


      IF (MATT) THEN
*        Sturm sequence count on T
         LPIVOT = D( 1 ) - VL
         RPIVOT = D( 1 ) - VU
         IF( LPIVOT.LE.ZERO ) THEN
            LCNT = LCNT + 1
         ENDIF
         IF( RPIVOT.LE.ZERO ) THEN
            RCNT = RCNT + 1
         ENDIF
         DO 10 I = 1, N-1
            TMP = E(I)**2
            LPIVOT = ( D( I+1 )-VL ) - TMP/LPIVOT
            RPIVOT = ( D( I+1 )-VU ) - TMP/RPIVOT
            IF( LPIVOT.LE.ZERO ) THEN
               LCNT = LCNT + 1
            ENDIF
            IF( RPIVOT.LE.ZERO ) THEN
               RCNT = RCNT + 1
            ENDIF
 10      CONTINUE
      ELSE
*        Sturm sequence count on L D L^T
         SL = -VL
         SU = -VU
         DO 20 I = 1, N - 1
            LPIVOT = D( I ) + SL
            RPIVOT = D( I ) + SU
            IF( LPIVOT.LE.ZERO ) THEN
               LCNT = LCNT + 1
            ENDIF
            IF( RPIVOT.LE.ZERO ) THEN
               RCNT = RCNT + 1
            ENDIF
            TMP = E(I) * D(I) * E(I)
*
            TMP2 = TMP / LPIVOT
            IF( TMP2.EQ.ZERO ) THEN
               SL =  TMP - VL
            ELSE
               SL = SL*TMP2 - VL
            END IF
*
            TMP2 = TMP / RPIVOT
            IF( TMP2.EQ.ZERO ) THEN
               SU =  TMP - VU
            ELSE
               SU = SU*TMP2 - VU
            END IF
 20      CONTINUE
         LPIVOT = D( N ) + SL
         RPIVOT = D( N ) + SU
         IF( LPIVOT.LE.ZERO ) THEN
            LCNT = LCNT + 1
         ENDIF
         IF( RPIVOT.LE.ZERO ) THEN
            RCNT = RCNT + 1
         ENDIF
      ENDIF
      EIGCNT = RCNT - LCNT

      RETURN
*
*     end of DLARRC
*
      END
