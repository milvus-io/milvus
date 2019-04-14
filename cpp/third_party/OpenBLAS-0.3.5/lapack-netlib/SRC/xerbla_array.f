*> \brief \b XERBLA_ARRAY
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download XERBLA_ARRAY + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/xerbla_array.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/xerbla_array.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/xerbla_array.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE XERBLA_ARRAY( SRNAME_ARRAY, SRNAME_LEN, INFO)
*
*       .. Scalar Arguments ..
*       INTEGER SRNAME_LEN, INFO
*       ..
*       .. Array Arguments ..
*       CHARACTER(1) SRNAME_ARRAY(SRNAME_LEN)
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> XERBLA_ARRAY assists other languages in calling XERBLA, the LAPACK
*> and BLAS error handler.  Rather than taking a Fortran string argument
*> as the function's name, XERBLA_ARRAY takes an array of single
*> characters along with the array's length.  XERBLA_ARRAY then copies
*> up to 32 characters of that array into a Fortran string and passes
*> that to XERBLA.  If called with a non-positive SRNAME_LEN,
*> XERBLA_ARRAY will call XERBLA with a string of all blank characters.
*>
*> Say some macro or other device makes XERBLA_ARRAY available to C99
*> by a name lapack_xerbla and with a common Fortran calling convention.
*> Then a C99 program could invoke XERBLA via:
*>    {
*>      int flen = strlen(__func__);
*>      lapack_xerbla(__func__, &flen, &info);
*>    }
*>
*> Providing XERBLA_ARRAY is not necessary for intercepting LAPACK
*> errors.  XERBLA_ARRAY calls XERBLA.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SRNAME_ARRAY
*> \verbatim
*>          SRNAME_ARRAY is CHARACTER(1) array, dimension (SRNAME_LEN)
*>          The name of the routine which called XERBLA_ARRAY.
*> \endverbatim
*>
*> \param[in] SRNAME_LEN
*> \verbatim
*>          SRNAME_LEN is INTEGER
*>          The length of the name in SRNAME_ARRAY.
*> \endverbatim
*>
*> \param[in] INFO
*> \verbatim
*>          INFO is INTEGER
*>          The position of the invalid parameter in the parameter list
*>          of the calling routine.
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
*> \ingroup OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE XERBLA_ARRAY( SRNAME_ARRAY, SRNAME_LEN, INFO)
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER SRNAME_LEN, INFO
*     ..
*     .. Array Arguments ..
      CHARACTER(1) SRNAME_ARRAY(SRNAME_LEN)
*     ..
*
* =====================================================================
*
*     ..
*     .. Local Scalars ..
      INTEGER I
*     ..
*     .. Local Arrays ..
      CHARACTER*32 SRNAME
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC MIN, LEN
*     ..
*     .. External Functions ..
      EXTERNAL XERBLA
*     ..
*     .. Executable Statements ..
      SRNAME = ''
      DO I = 1, MIN( SRNAME_LEN, LEN( SRNAME ) )
         SRNAME( I:I ) = SRNAME_ARRAY( I )
      END DO

      CALL XERBLA( SRNAME, INFO )

      RETURN
      END
