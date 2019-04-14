*> \brief \b IPARAM2STAGE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at 
*            http://www.netlib.org/lapack/explore-html/ 
*
*> \htmlonly
*> Download IPARAM2STAGE + dependencies 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/iparam2stage.F"> 
*> [TGZ]</a> 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/iparam2stage.F"> 
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/iparam2stage.F"> 
*> [TXT]</a>
*> \endhtmlonly 
*
*  Definition:
*  ===========
*
*       INTEGER FUNCTION IPARAM2STAGE( ISPEC, NAME, OPTS, 
*                                    NI, NBI, IBI, NXI )
*       #if defined(_OPENMP)
*           use omp_lib
*       #endif
*       IMPLICIT NONE
*
*       .. Scalar Arguments ..
*       CHARACTER*( * )    NAME, OPTS
*       INTEGER            ISPEC, NI, NBI, IBI, NXI
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      This program sets problem and machine dependent parameters
*>      useful for xHETRD_2STAGE, xHETRD_H@2HB, xHETRD_HB2ST,
*>      xGEBRD_2STAGE, xGEBRD_GE2GB, xGEBRD_GB2BD 
*>      and related subroutines for eigenvalue problems. 
*>      It is called whenever ILAENV is called with 17 <= ISPEC <= 21.
*>      It is called whenever ILAENV2STAGE is called with 1 <= ISPEC <= 5
*>      with a direct conversion ISPEC + 16.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ISPEC
*> \verbatim
*>          ISPEC is integer scalar
*>              ISPEC specifies which tunable parameter IPARAM2STAGE should
*>              return.
*>
*>              ISPEC=17: the optimal blocksize nb for the reduction to
*                         BAND
*>
*>              ISPEC=18: the optimal blocksize ib for the eigenvectors
*>                        singular vectors update routine
*>
*>              ISPEC=19: The length of the array that store the Housholder 
*>                        representation for the second stage 
*>                        Band to Tridiagonal or Bidiagonal
*>
*>              ISPEC=20: The workspace needed for the routine in input.
*>
*>              ISPEC=21: For future release.
*> \endverbatim
*>
*> \param[in] NAME
*> \verbatim
*>          NAME is character string
*>               Name of the calling subroutine
*> \endverbatim
*>
*> \param[in] OPTS
*> \verbatim
*>          OPTS is CHARACTER*(*)
*>          The character options to the subroutine NAME, concatenated
*>          into a single character string.  For example, UPLO = 'U',
*>          TRANS = 'T', and DIAG = 'N' for a triangular routine would
*>          be specified as OPTS = 'UTN'.
*> \endverbatim
*>
*> \param[in] NI
*> \verbatim
*>          NI is INTEGER which is the size of the matrix
*> \endverbatim
*>
*> \param[in] NBI
*> \verbatim
*>          NBI is INTEGER which is the used in the reduciton, 
*           (e.g., the size of the band), needed to compute workspace
*           and LHOUS2.
*> \endverbatim
*>
*> \param[in] IBI
*> \verbatim
*>          IBI is INTEGER which represent the IB of the reduciton,
*           needed to compute workspace and LHOUS2.
*> \endverbatim
*>
*> \param[in] NXI
*> \verbatim
*>          NXI is INTEGER needed in the future release.
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
*> \ingroup auxOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Implemented by Azzam Haidar.
*>
*>  All detail are available on technical report, SC11, SC13 papers.
*>
*>  Azzam Haidar, Hatem Ltaief, and Jack Dongarra.
*>  Parallel reduction to condensed forms for symmetric eigenvalue problems
*>  using aggregated fine-grained and memory-aware kernels. In Proceedings
*>  of 2011 International Conference for High Performance Computing,
*>  Networking, Storage and Analysis (SC '11), New York, NY, USA,
*>  Article 8 , 11 pages.
*>  http://doi.acm.org/10.1145/2063384.2063394
*>
*>  A. Haidar, J. Kurzak, P. Luszczek, 2013.
*>  An improved parallel singular value algorithm and its implementation 
*>  for multicore hardware, In Proceedings of 2013 International Conference
*>  for High Performance Computing, Networking, Storage and Analysis (SC '13).
*>  Denver, Colorado, USA, 2013.
*>  Article 90, 12 pages.
*>  http://doi.acm.org/10.1145/2503210.2503292
*>
*>  A. Haidar, R. Solca, S. Tomov, T. Schulthess and J. Dongarra.
*>  A novel hybrid CPU-GPU generalized eigensolver for electronic structure 
*>  calculations based on fine-grained memory aware tasks.
*>  International Journal of High Performance Computing Applications.
*>  Volume 28 Issue 2, Pages 196-209, May 2014.
*>  http://hpc.sagepub.com/content/28/2/196 
*>
*> \endverbatim
*>
*  =====================================================================
      INTEGER FUNCTION IPARAM2STAGE( ISPEC, NAME, OPTS, 
     $                              NI, NBI, IBI, NXI )
#if defined(_OPENMP)
      use omp_lib
#endif
      IMPLICIT NONE
*
*  -- LAPACK auxiliary routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER*( * )    NAME, OPTS
      INTEGER            ISPEC, NI, NBI, IBI, NXI
*
*  ================================================================
*     ..
*     .. Local Scalars ..
      INTEGER            I, IC, IZ, KD, IB, LHOUS, LWORK, NTHREADS,
     $                   FACTOPTNB, QROPTNB, LQOPTNB
      LOGICAL            RPREC, CPREC
      CHARACTER          PREC*1, ALGO*3, STAG*5, SUBNAM*12, VECT*1
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CHAR, ICHAR, MAX
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      EXTERNAL           ILAENV
*     ..
*     .. Executable Statements ..
*
*     Invalid value for ISPEC
*
      IF( (ISPEC.LT.17).OR.(ISPEC.GT.21) ) THEN
          IPARAM2STAGE = -1
          RETURN
      ENDIF
*
*     Get the number of threads
*      
      NTHREADS = 1
#if defined(_OPENMP)
!$OMP PARALLEL 
      NTHREADS = OMP_GET_NUM_THREADS()
!$OMP END PARALLEL
#endif
*      WRITE(*,*) 'IPARAM VOICI NTHREADS ISPEC ',NTHREADS, ISPEC
*
      IF( ISPEC .NE. 19 ) THEN
*
*        Convert NAME to upper case if the first character is lower case.
*
         IPARAM2STAGE = -1
         SUBNAM = NAME
         IC = ICHAR( SUBNAM( 1: 1 ) )
         IZ = ICHAR( 'Z' )
         IF( IZ.EQ.90 .OR. IZ.EQ.122 ) THEN
*
*           ASCII character set
*
            IF( IC.GE.97 .AND. IC.LE.122 ) THEN
               SUBNAM( 1: 1 ) = CHAR( IC-32 )
               DO 100 I = 2, 12
                  IC = ICHAR( SUBNAM( I: I ) )
                  IF( IC.GE.97 .AND. IC.LE.122 )
     $               SUBNAM( I: I ) = CHAR( IC-32 )
  100          CONTINUE
            END IF
*
         ELSE IF( IZ.EQ.233 .OR. IZ.EQ.169 ) THEN
*
*           EBCDIC character set
*
            IF( ( IC.GE.129 .AND. IC.LE.137 ) .OR.
     $          ( IC.GE.145 .AND. IC.LE.153 ) .OR.
     $          ( IC.GE.162 .AND. IC.LE.169 ) ) THEN
               SUBNAM( 1: 1 ) = CHAR( IC+64 )
               DO 110 I = 2, 12
                  IC = ICHAR( SUBNAM( I: I ) )
                  IF( ( IC.GE.129 .AND. IC.LE.137 ) .OR.
     $                ( IC.GE.145 .AND. IC.LE.153 ) .OR.
     $                ( IC.GE.162 .AND. IC.LE.169 ) )SUBNAM( I:
     $                I ) = CHAR( IC+64 )
  110          CONTINUE
            END IF
*
         ELSE IF( IZ.EQ.218 .OR. IZ.EQ.250 ) THEN
*
*           Prime machines:  ASCII+128
*
            IF( IC.GE.225 .AND. IC.LE.250 ) THEN
               SUBNAM( 1: 1 ) = CHAR( IC-32 )
               DO 120 I = 2, 12
                 IC = ICHAR( SUBNAM( I: I ) )
                 IF( IC.GE.225 .AND. IC.LE.250 )
     $             SUBNAM( I: I ) = CHAR( IC-32 )
  120          CONTINUE
            END IF
         END IF
*
         PREC  = SUBNAM( 1: 1 )
         ALGO  = SUBNAM( 4: 6 )
         STAG  = SUBNAM( 8:12 )
         RPREC = PREC.EQ.'S' .OR. PREC.EQ.'D'
         CPREC = PREC.EQ.'C' .OR. PREC.EQ.'Z'
*
*        Invalid value for PRECISION
*      
         IF( .NOT.( RPREC .OR. CPREC ) ) THEN
             IPARAM2STAGE = -1
             RETURN
         ENDIF
      ENDIF
*      WRITE(*,*),'RPREC,CPREC ',RPREC,CPREC,
*     $           '   ALGO ',ALGO,'    STAGE ',STAG
*      
*
      IF (( ISPEC .EQ. 17 ) .OR. ( ISPEC .EQ. 18 )) THEN 
*
*     ISPEC = 17, 18:  block size KD, IB
*     Could be also dependent from N but for now it
*     depend only on sequential or parallel
*
         IF( NTHREADS.GT.4 ) THEN
            IF( CPREC ) THEN
               KD = 128
               IB = 32
            ELSE
               KD = 160
               IB = 40
            ENDIF
         ELSE IF( NTHREADS.GT.1 ) THEN
            IF( CPREC ) THEN
               KD = 64
               IB = 32
            ELSE
               KD = 64
               IB = 32
            ENDIF
         ELSE
            IF( CPREC ) THEN
               KD = 16
               IB = 16
            ELSE
               KD = 32
               IB = 16
            ENDIF
         ENDIF
         IF( ISPEC.EQ.17 ) IPARAM2STAGE = KD
         IF( ISPEC.EQ.18 ) IPARAM2STAGE = IB
*
      ELSE IF ( ISPEC .EQ. 19 ) THEN
*
*     ISPEC = 19:  
*     LHOUS length of the Houselholder representation
*     matrix (V,T) of the second stage. should be >= 1.
*
*     Will add the VECT OPTION HERE next release
         VECT  = OPTS(1:1)
         IF( VECT.EQ.'N' ) THEN
            LHOUS = MAX( 1, 4*NI )
         ELSE
*           This is not correct, it need to call the ALGO and the stage2
            LHOUS = MAX( 1, 4*NI ) + IBI
         ENDIF
         IF( LHOUS.GE.0 ) THEN
            IPARAM2STAGE = LHOUS
         ELSE
            IPARAM2STAGE = -1
         ENDIF
*
      ELSE IF ( ISPEC .EQ. 20 ) THEN
*
*     ISPEC = 20: (21 for future use)  
*     LWORK length of the workspace for 
*     either or both stages for TRD and BRD. should be >= 1.
*     TRD:
*     TRD_stage 1: = LT + LW + LS1 + LS2
*                  = LDT*KD + N*KD + N*MAX(KD,FACTOPTNB) + LDS2*KD 
*                    where LDT=LDS2=KD
*                  = N*KD + N*max(KD,FACTOPTNB) + 2*KD*KD
*     TRD_stage 2: = (2NB+1)*N + KD*NTHREADS
*     TRD_both   : = max(stage1,stage2) + AB ( AB=(KD+1)*N )
*                  = N*KD + N*max(KD+1,FACTOPTNB) 
*                    + max(2*KD*KD, KD*NTHREADS) 
*                    + (KD+1)*N
         LWORK        = -1
         SUBNAM(1:1)  = PREC
         SUBNAM(2:6)  = 'GEQRF'
         QROPTNB      = ILAENV( 1, SUBNAM, ' ', NI, NBI, -1, -1 )
         SUBNAM(2:6)  = 'GELQF'
         LQOPTNB      = ILAENV( 1, SUBNAM, ' ', NBI, NI, -1, -1 )
*        Could be QR or LQ for TRD and the max for BRD
         FACTOPTNB    = MAX(QROPTNB, LQOPTNB)
         IF( ALGO.EQ.'TRD' ) THEN
            IF( STAG.EQ.'2STAG' ) THEN
               LWORK = NI*NBI + NI*MAX(NBI+1,FACTOPTNB) 
     $              + MAX(2*NBI*NBI, NBI*NTHREADS) 
     $              + (NBI+1)*NI
            ELSE IF( (STAG.EQ.'HE2HB').OR.(STAG.EQ.'SY2SB') ) THEN
               LWORK = NI*NBI + NI*MAX(NBI,FACTOPTNB) + 2*NBI*NBI
            ELSE IF( (STAG.EQ.'HB2ST').OR.(STAG.EQ.'SB2ST') ) THEN
               LWORK = (2*NBI+1)*NI + NBI*NTHREADS
            ENDIF
         ELSE IF( ALGO.EQ.'BRD' ) THEN
            IF( STAG.EQ.'2STAG' ) THEN
               LWORK = 2*NI*NBI + NI*MAX(NBI+1,FACTOPTNB) 
     $              + MAX(2*NBI*NBI, NBI*NTHREADS) 
     $              + (NBI+1)*NI
            ELSE IF( STAG.EQ.'GE2GB' ) THEN
               LWORK = NI*NBI + NI*MAX(NBI,FACTOPTNB) + 2*NBI*NBI
            ELSE IF( STAG.EQ.'GB2BD' ) THEN
               LWORK = (3*NBI+1)*NI + NBI*NTHREADS
            ENDIF
         ENDIF
         LWORK = MAX ( 1, LWORK )

         IF( LWORK.GT.0 ) THEN
            IPARAM2STAGE = LWORK
         ELSE
            IPARAM2STAGE = -1
         ENDIF
*
      ELSE IF ( ISPEC .EQ. 21 ) THEN
*
*     ISPEC = 21 for future use 
         IPARAM2STAGE = NXI
      ENDIF
*
*     ==== End of IPARAM2STAGE ====
*
      END
