include ../make.inc

#######################################################################
#  This is the makefile to create a library for LAPACK.
#  The files are organized as follows:
#
#       ALLAUX  -- Auxiliary routines called from all precisions
#       SCLAUX  -- Auxiliary routines called from single precision
#       DZLAUX  -- Auxiliary routines called from double precision
#
#       DSLASRC -- Double-single mixed precision real routines called from
#                  single, single-extra and double precision real LAPACK
#                  routines (i.e. from SLASRC, SXLASRC, DLASRC).
#       ZCLASRC -- Double-single mixed precision complex routines called from
#                  single, single-extra and double precision complex LAPACK
#                  routines (i.e. from CLASRC, CXLASRC, ZLASRC).
#
#       SLASRC  -- Single precision real LAPACK routines
#       SXLASRC -- Single precision real LAPACK routines using extra
#                  precision.
#       CLASRC  -- Single precision complex LAPACK routines
#       CXLASRC -- Single precision complex LAPACK routines using extra
#                  precision.
#       DLASRC  -- Double precision real LAPACK routines
#       DXLASRC -- Double precision real LAPACK routines using extra
#                  precision.
#       ZLASRC  -- Double precision complex LAPACK routines
#       ZXLASRC -- Double precision complex LAPACK routines using extra
#                  precision.
#
#    DEPRECATED -- Deprecated routines in all precisions
#
#  The library can be set up to include routines for any combination
#  of the four precisions.  To create or add to the library, enter make
#  followed by one or more of the precisions desired.  Some examples:
#       make single
#       make single complex
#       make single double complex complex16
#  Alternatively, the command
#       make
#  without any arguments creates a library of all four precisions.
#  The library is called
#       lapack.a
#  and is created at the next higher directory level.
#
#  To remove the object files after the library is created, enter
#       make cleanobj
#  On some systems, you can force the source files to be recompiled by
#  entering (for example)
#       make single FRC=FRC
#
#  ***Note***
#  The functions lsame, second, dsecnd, slamch, and dlamch may have
#  to be installed before compiling the library.  Refer to the
#  installation guide, LAPACK Working Note 41, for instructions.
#
#######################################################################

ALLAUX = ilaenv.o ilaenv2stage.o ieeeck.o lsamen.o xerbla.o xerbla_array.o \
   iparmq.o iparam2stage.o \
   ilaprec.o ilatrans.o ilauplo.o iladiag.o chla_transtype.o \
   ../INSTALL/ilaver.o ../INSTALL/lsame.o ../INSTALL/slamch.o

SCLAUX = \
   sbdsdc.o \
   sbdsqr.o sdisna.o slabad.o slacpy.o sladiv.o slae2.o  slaebz.o \
   slaed0.o slaed1.o slaed2.o slaed3.o slaed4.o slaed5.o slaed6.o \
   slaed7.o slaed8.o slaed9.o slaeda.o slaev2.o slagtf.o \
   slagts.o slamrg.o slanst.o \
   slapy2.o slapy3.o slarnv.o \
   slarra.o slarrb.o slarrc.o slarrd.o slarre.o slarrf.o slarrj.o \
   slarrk.o slarrr.o slaneg.o \
   slartg.o slaruv.o slas2.o  slascl.o \
   slasd0.o slasd1.o slasd2.o slasd3.o slasd4.o slasd5.o slasd6.o \
   slasd7.o slasd8.o slasda.o slasdq.o slasdt.o \
   slaset.o slasq1.o slasq2.o slasq3.o slasq4.o slasq5.o slasq6.o \
   slasr.o  slasrt.o slassq.o slasv2.o spttrf.o sstebz.o sstedc.o \
   ssteqr.o ssterf.o slaisnan.o sisnan.o \
   slartgp.o slartgs.o \
   ../INSTALL/second_$(TIMER).o

DZLAUX = \
   dbdsdc.o \
   dbdsqr.o ddisna.o dlabad.o dlacpy.o dladiv.o dlae2.o  dlaebz.o \
   dlaed0.o dlaed1.o dlaed2.o dlaed3.o dlaed4.o dlaed5.o dlaed6.o \
   dlaed7.o dlaed8.o dlaed9.o dlaeda.o dlaev2.o dlagtf.o \
   dlagts.o dlamrg.o dlanst.o \
   dlapy2.o dlapy3.o dlarnv.o \
   dlarra.o dlarrb.o dlarrc.o dlarrd.o dlarre.o dlarrf.o dlarrj.o \
   dlarrk.o dlarrr.o dlaneg.o \
   dlartg.o dlaruv.o dlas2.o  dlascl.o \
   dlasd0.o dlasd1.o dlasd2.o dlasd3.o dlasd4.o dlasd5.o dlasd6.o \
   dlasd7.o dlasd8.o dlasda.o dlasdq.o dlasdt.o \
   dlaset.o dlasq1.o dlasq2.o dlasq3.o dlasq4.o dlasq5.o dlasq6.o \
   dlasr.o  dlasrt.o dlassq.o dlasv2.o dpttrf.o dstebz.o dstedc.o \
   dsteqr.o dsterf.o dlaisnan.o disnan.o \
   dlartgp.o dlartgs.o \
   ../INSTALL/dlamch.o ../INSTALL/dsecnd_$(TIMER).o

SLASRC = \
   sbdsvdx.o spotrf2.o sgetrf2.o \
   sgbbrd.o sgbcon.o sgbequ.o sgbrfs.o sgbsv.o \
   sgbsvx.o sgbtf2.o sgbtrf.o sgbtrs.o sgebak.o sgebal.o sgebd2.o \
   sgebrd.o sgecon.o sgeequ.o sgees.o  sgeesx.o sgeev.o  sgeevx.o \
   sgehd2.o sgehrd.o sgelq2.o sgelqf.o \
   sgels.o  sgelsd.o sgelss.o sgelsy.o sgeql2.o sgeqlf.o \
   sgeqp3.o sgeqr2.o sgeqr2p.o sgeqrf.o sgeqrfp.o sgerfs.o \
   sgerq2.o sgerqf.o sgesc2.o sgesdd.o sgesv.o  sgesvd.o sgesvdx.o sgesvx.o \
   sgetc2.o sgetf2.o sgetri.o \
   sggbak.o sggbal.o sgges.o  sgges3.o sggesx.o \
   sggev.o  sggev3.o sggevx.o \
   sggglm.o sgghrd.o sgghd3.o sgglse.o sggqrf.o \
   sggrqf.o sggsvd3.o sggsvp3.o sgtcon.o sgtrfs.o sgtsv.o \
   sgtsvx.o sgttrf.o sgttrs.o sgtts2.o shgeqz.o \
   shsein.o shseqr.o slabrd.o slacon.o slacn2.o \
   slaein.o slaexc.o slag2.o  slags2.o slagtm.o slagv2.o slahqr.o \
   slahr2.o slaic1.o slaln2.o slals0.o slalsa.o slalsd.o \
   slangb.o slange.o slangt.o slanhs.o slansb.o slansp.o \
   slansy.o slantb.o slantp.o slantr.o slanv2.o \
   slapll.o slapmt.o \
   slaqgb.o slaqge.o slaqp2.o slaqps.o slaqsb.o slaqsp.o slaqsy.o \
   slaqr0.o slaqr1.o slaqr2.o slaqr3.o slaqr4.o slaqr5.o \
   slaqtr.o slar1v.o slar2v.o ilaslr.o ilaslc.o \
   slarf.o  slarfb.o slarfg.o slarfgp.o slarft.o slarfx.o slarfy.o slargv.o \
   slarrv.o slartv.o \
   slarz.o  slarzb.o slarzt.o slaswp.o slasy2.o slasyf.o slasyf_rook.o \
   slasyf_rk.o \
   slatbs.o slatdf.o slatps.o slatrd.o slatrs.o slatrz.o \
   slauu2.o slauum.o sopgtr.o sopmtr.o sorg2l.o sorg2r.o \
   sorgbr.o sorghr.o sorgl2.o sorglq.o sorgql.o sorgqr.o sorgr2.o \
   sorgrq.o sorgtr.o sorm2l.o sorm2r.o sorm22.o \
   sormbr.o sormhr.o sorml2.o sormlq.o sormql.o sormqr.o sormr2.o \
   sormr3.o sormrq.o sormrz.o sormtr.o spbcon.o spbequ.o spbrfs.o \
   spbstf.o spbsv.o  spbsvx.o \
   spbtf2.o spbtrf.o spbtrs.o spocon.o spoequ.o sporfs.o sposv.o \
   sposvx.o spotf2.o spotri.o spstrf.o spstf2.o \
   sppcon.o sppequ.o \
   spprfs.o sppsv.o  sppsvx.o spptrf.o spptri.o spptrs.o sptcon.o \
   spteqr.o sptrfs.o sptsv.o  sptsvx.o spttrs.o sptts2.o srscl.o \
   ssbev.o  ssbevd.o ssbevx.o ssbgst.o ssbgv.o  ssbgvd.o ssbgvx.o \
   ssbtrd.o sspcon.o sspev.o  sspevd.o sspevx.o sspgst.o \
   sspgv.o  sspgvd.o sspgvx.o ssprfs.o sspsv.o  sspsvx.o ssptrd.o \
   ssptrf.o ssptri.o ssptrs.o sstegr.o sstein.o sstev.o  sstevd.o sstevr.o \
   sstevx.o \
   ssycon.o ssyev.o  ssyevd.o ssyevr.o ssyevx.o ssygs2.o \
   ssygst.o ssygv.o  ssygvd.o ssygvx.o ssyrfs.o ssysv.o  ssysvx.o \
   ssytd2.o ssytf2.o ssytrd.o ssytrf.o ssytri.o ssytri2.o ssytri2x.o \
   ssyswapr.o ssytrs.o ssytrs2.o \
   ssyconv.o ssyconvf.o ssyconvf_rook.o \
   ssytf2_rook.o ssytrf_rook.o ssytrs_rook.o \
   ssytri_rook.o ssycon_rook.o ssysv_rook.o \
   ssytf2_rk.o ssytrf_rk.o ssytrs_3.o \
   ssytri_3.o ssytri_3x.o ssycon_3.o ssysv_rk.o \
   slasyf_aa.o ssysv_aa.o ssytrf_aa.o ssytrs_aa.o \
   ssysv_aa_2stage.o ssytrf_aa_2stage.o ssytrs_aa_2stage.o \
   stbcon.o \
   stbrfs.o stbtrs.o stgevc.o stgex2.o stgexc.o stgsen.o \
   stgsja.o stgsna.o stgsy2.o stgsyl.o stpcon.o stprfs.o stptri.o \
   stptrs.o \
   strcon.o strevc.o strevc3.o strexc.o strrfs.o strsen.o strsna.o strsyl.o \
   strti2.o strtri.o strtrs.o stzrzf.o sstemr.o \
   slansf.o spftrf.o spftri.o spftrs.o ssfrk.o stfsm.o stftri.o stfttp.o \
   stfttr.o stpttf.o stpttr.o strttf.o strttp.o \
   sgejsv.o sgesvj.o sgsvj0.o sgsvj1.o \
   sgeequb.o ssyequb.o spoequb.o sgbequb.o \
   sbbcsd.o slapmr.o sorbdb.o sorbdb1.o sorbdb2.o sorbdb3.o sorbdb4.o \
   sorbdb5.o sorbdb6.o sorcsd.o sorcsd2by1.o \
   sgeqrt.o sgeqrt2.o sgeqrt3.o sgemqrt.o \
   stpqrt.o stpqrt2.o stpmqrt.o stprfb.o \
   sgelqt.o sgelqt3.o sgemlqt.o \
   sgetsls.o sgeqr.o slatsqr.o slamtsqr.o sgemqr.o \
   sgelq.o slaswlq.o slamswlq.o sgemlq.o \
   stplqt.o stplqt2.o stpmlqt.o \
   ssytrd_2stage.o ssytrd_sy2sb.o ssytrd_sb2st.o ssb2st_kernels.o \
   ssyevd_2stage.o ssyev_2stage.o ssyevx_2stage.o ssyevr_2stage.o \
   ssbev_2stage.o ssbevx_2stage.o ssbevd_2stage.o ssygv_2stage.o

DSLASRC = spotrs.o sgetrs.o spotrf.o sgetrf.o

ifdef USEXBLAS
SXLASRC = sgesvxx.o sgerfsx.o sla_gerfsx_extended.o sla_geamv.o \
   sla_gercond.o sla_gerpvgrw.o ssysvxx.o ssyrfsx.o \
   sla_syrfsx_extended.o sla_syamv.o sla_syrcond.o sla_syrpvgrw.o \
   sposvxx.o sporfsx.o sla_porfsx_extended.o sla_porcond.o \
   sla_porpvgrw.o sgbsvxx.o sgbrfsx.o sla_gbrfsx_extended.o \
   sla_gbamv.o sla_gbrcond.o sla_gbrpvgrw.o sla_lin_berr.o slarscl2.o \
   slascl2.o sla_wwaddw.o
endif

CLASRC = \
   cpotrf2.o cgetrf2.o \
   cbdsqr.o cgbbrd.o cgbcon.o cgbequ.o cgbrfs.o cgbsv.o  cgbsvx.o \
   cgbtf2.o cgbtrf.o cgbtrs.o cgebak.o cgebal.o cgebd2.o cgebrd.o \
   cgecon.o cgeequ.o cgees.o  cgeesx.o cgeev.o  cgeevx.o \
   cgehd2.o cgehrd.o cgelq2.o cgelqf.o \
   cgels.o  cgelsd.o cgelss.o cgelsy.o cgeql2.o cgeqlf.o cgeqp3.o \
   cgeqr2.o cgeqr2p.o cgeqrf.o cgeqrfp.o cgerfs.o \
   cgerq2.o cgerqf.o cgesc2.o cgesdd.o cgesv.o  cgesvd.o  cgesvdx.o \
   cgesvj.o cgejsv.o cgsvj0.o cgsvj1.o \
   cgesvx.o cgetc2.o cgetf2.o cgetri.o \
   cggbak.o cggbal.o cgges.o  cgges3.o cggesx.o \
   cggev.o  cggev3.o cggevx.o cggglm.o \
   cgghrd.o cgghd3.o cgglse.o cggqrf.o cggrqf.o \
   cggsvd3.o cggsvp3.o \
   cgtcon.o cgtrfs.o cgtsv.o  cgtsvx.o cgttrf.o cgttrs.o cgtts2.o chbev.o \
   chbevd.o chbevx.o chbgst.o chbgv.o  chbgvd.o chbgvx.o chbtrd.o \
   checon.o cheev.o  cheevd.o cheevr.o cheevx.o chegs2.o chegst.o \
   chegv.o  chegvd.o chegvx.o cherfs.o chesv.o  chesvx.o chetd2.o \
   chetf2.o chetrd.o \
   chetrf.o chetri.o chetri2.o chetri2x.o cheswapr.o \
   chetrs.o chetrs2.o \
   chetf2_rook.o chetrf_rook.o chetri_rook.o \
   chetrs_rook.o checon_rook.o chesv_rook.o \
   chetf2_rk.o chetrf_rk.o chetri_3.o chetri_3x.o \
   chetrs_3.o checon_3.o chesv_rk.o \
   chesv_aa.o chetrf_aa.o chetrs_aa.o clahef_aa.o \
   chesv_aa_2stage.o chetrf_aa_2stage.o chetrs_aa_2stage.o \
   chgeqz.o chpcon.o chpev.o  chpevd.o \
   chpevx.o chpgst.o chpgv.o  chpgvd.o chpgvx.o chprfs.o chpsv.o \
   chpsvx.o \
   chptrd.o chptrf.o chptri.o chptrs.o chsein.o chseqr.o clabrd.o \
   clacgv.o clacon.o clacn2.o clacp2.o clacpy.o clacrm.o clacrt.o cladiv.o \
   claed0.o claed7.o claed8.o \
   claein.o claesy.o claev2.o clags2.o clagtm.o \
   clahef.o clahef_rook.o clahef_rk.o clahqr.o \
   clahr2.o claic1.o clals0.o clalsa.o clalsd.o clangb.o clange.o clangt.o \
   clanhb.o clanhe.o \
   clanhp.o clanhs.o clanht.o clansb.o clansp.o clansy.o clantb.o \
   clantp.o clantr.o clapll.o clapmt.o clarcm.o claqgb.o claqge.o \
   claqhb.o claqhe.o claqhp.o claqp2.o claqps.o claqsb.o \
   claqr0.o claqr1.o claqr2.o claqr3.o claqr4.o claqr5.o \
   claqsp.o claqsy.o clar1v.o clar2v.o ilaclr.o ilaclc.o \
   clarf.o  clarfb.o clarfg.o clarft.o clarfgp.o \
   clarfx.o clarfy.o clargv.o clarnv.o clarrv.o clartg.o clartv.o \
   clarz.o  clarzb.o clarzt.o clascl.o claset.o clasr.o  classq.o \
   claswp.o clasyf.o clasyf_rook.o clasyf_rk.o clasyf_aa.o \
   clatbs.o clatdf.o clatps.o clatrd.o clatrs.o clatrz.o \
   clauu2.o clauum.o cpbcon.o cpbequ.o cpbrfs.o cpbstf.o cpbsv.o \
   cpbsvx.o cpbtf2.o cpbtrf.o cpbtrs.o cpocon.o cpoequ.o cporfs.o \
   cposv.o  cposvx.o cpotf2.o cpotri.o cpstrf.o cpstf2.o \
   cppcon.o cppequ.o cpprfs.o cppsv.o  cppsvx.o cpptrf.o cpptri.o cpptrs.o \
   cptcon.o cpteqr.o cptrfs.o cptsv.o  cptsvx.o cpttrf.o cpttrs.o cptts2.o \
   crot.o   cspcon.o cspmv.o  cspr.o   csprfs.o cspsv.o \
   cspsvx.o csptrf.o csptri.o csptrs.o csrscl.o cstedc.o \
   cstegr.o cstein.o csteqr.o \
   csycon.o csymv.o \
   csyr.o csyrfs.o csysv.o csysvx.o csytf2.o csytrf.o csytri.o csytri2.o csytri2x.o \
   csyswapr.o csytrs.o csytrs2.o \
   csyconv.o csyconvf.o csyconvf_rook.o \
   csytf2_rook.o csytrf_rook.o csytrs_rook.o \
   csytri_rook.o csycon_rook.o csysv_rook.o \
   csytf2_rk.o csytrf_rk.o csytrf_aa.o csytrs_3.o csytrs_aa.o \
   csytri_3.o csytri_3x.o csycon_3.o csysv_rk.o csysv_aa.o \
   csysv_aa_2stage.o csytrf_aa_2stage.o csytrs_aa_2stage.o \
   ctbcon.o ctbrfs.o ctbtrs.o ctgevc.o ctgex2.o \
   ctgexc.o ctgsen.o ctgsja.o ctgsna.o ctgsy2.o ctgsyl.o ctpcon.o \
   ctprfs.o ctptri.o \
   ctptrs.o ctrcon.o ctrevc.o ctrevc3.o ctrexc.o ctrrfs.o ctrsen.o ctrsna.o \
   ctrsyl.o ctrti2.o ctrtri.o ctrtrs.o ctzrzf.o cung2l.o cung2r.o \
   cungbr.o cunghr.o cungl2.o cunglq.o cungql.o cungqr.o cungr2.o \
   cungrq.o cungtr.o cunm2l.o cunm2r.o cunmbr.o cunmhr.o cunml2.o cunm22.o \
   cunmlq.o cunmql.o cunmqr.o cunmr2.o cunmr3.o cunmrq.o cunmrz.o \
   cunmtr.o cupgtr.o cupmtr.o icmax1.o scsum1.o cstemr.o \
   chfrk.o ctfttp.o clanhf.o cpftrf.o cpftri.o cpftrs.o ctfsm.o ctftri.o \
   ctfttr.o ctpttf.o ctpttr.o ctrttf.o ctrttp.o \
   cgeequb.o cgbequb.o csyequb.o cpoequb.o cheequb.o \
   cbbcsd.o clapmr.o cunbdb.o cunbdb1.o cunbdb2.o cunbdb3.o cunbdb4.o \
   cunbdb5.o cunbdb6.o cuncsd.o cuncsd2by1.o \
   cgeqrt.o cgeqrt2.o cgeqrt3.o cgemqrt.o \
   ctpqrt.o ctpqrt2.o ctpmqrt.o ctprfb.o \
   cgelqt.o cgelqt3.o cgemlqt.o \
   cgetsls.o cgeqr.o clatsqr.o clamtsqr.o cgemqr.o \
   cgelq.o claswlq.o clamswlq.o cgemlq.o \
   ctplqt.o ctplqt2.o ctpmlqt.o \
   chetrd_2stage.o chetrd_he2hb.o chetrd_hb2st.o chb2st_kernels.o \
   cheevd_2stage.o cheev_2stage.o cheevx_2stage.o cheevr_2stage.o \
   chbev_2stage.o chbevx_2stage.o chbevd_2stage.o chegv_2stage.o

ifdef USEXBLAS
CXLASRC = cgesvxx.o cgerfsx.o cla_gerfsx_extended.o cla_geamv.o \
   cla_gercond_c.o cla_gercond_x.o cla_gerpvgrw.o \
   csysvxx.o csyrfsx.o cla_syrfsx_extended.o cla_syamv.o \
   cla_syrcond_c.o cla_syrcond_x.o cla_syrpvgrw.o \
   cposvxx.o cporfsx.o cla_porfsx_extended.o \
   cla_porcond_c.o cla_porcond_x.o cla_porpvgrw.o \
   cgbsvxx.o cgbrfsx.o cla_gbrfsx_extended.o cla_gbamv.o \
   cla_gbrcond_c.o cla_gbrcond_x.o cla_gbrpvgrw.o \
   chesvxx.o cherfsx.o cla_herfsx_extended.o cla_heamv.o \
   cla_hercond_c.o cla_hercond_x.o cla_herpvgrw.o \
   cla_lin_berr.o clarscl2.o clascl2.o cla_wwaddw.o
endif

ZCLASRC = cpotrs.o cgetrs.o cpotrf.o cgetrf.o

DLASRC = \
   dpotrf2.o dgetrf2.o \
   dbdsvdx.o \
   dgbbrd.o dgbcon.o dgbequ.o dgbrfs.o dgbsv.o \
   dgbsvx.o dgbtf2.o dgbtrf.o dgbtrs.o dgebak.o dgebal.o dgebd2.o \
   dgebrd.o dgecon.o dgeequ.o dgees.o  dgeesx.o dgeev.o  dgeevx.o \
   dgehd2.o dgehrd.o dgelq2.o dgelqf.o \
   dgels.o  dgelsd.o dgelss.o dgelsy.o dgeql2.o dgeqlf.o \
   dgeqp3.o dgeqr2.o dgeqr2p.o dgeqrf.o dgeqrfp.o dgerfs.o \
   dgerq2.o dgerqf.o dgesc2.o dgesdd.o dgesv.o  dgesvd.o dgesvdx.o dgesvx.o \
   dgetc2.o dgetf2.o dgetrf.o dgetri.o \
   dgetrs.o dggbak.o dggbal.o dgges.o  dgges3.o dggesx.o \
   dggev.o  dggev3.o dggevx.o \
   dggglm.o dgghrd.o dgghd3.o dgglse.o dggqrf.o \
   dggrqf.o dggsvd3.o dggsvp3.o dgtcon.o dgtrfs.o dgtsv.o \
   dgtsvx.o dgttrf.o dgttrs.o dgtts2.o dhgeqz.o \
   dhsein.o dhseqr.o dlabrd.o dlacon.o dlacn2.o \
   dlaein.o dlaexc.o dlag2.o  dlags2.o dlagtm.o dlagv2.o dlahqr.o \
   dlahr2.o dlaic1.o dlaln2.o dlals0.o dlalsa.o dlalsd.o \
   dlangb.o dlange.o dlangt.o dlanhs.o dlansb.o dlansp.o \
   dlansy.o dlantb.o dlantp.o dlantr.o dlanv2.o \
   dlapll.o dlapmt.o \
   dlaqgb.o dlaqge.o dlaqp2.o dlaqps.o dlaqsb.o dlaqsp.o dlaqsy.o \
   dlaqr0.o dlaqr1.o dlaqr2.o dlaqr3.o dlaqr4.o dlaqr5.o \
   dlaqtr.o dlar1v.o dlar2v.o iladlr.o iladlc.o \
   dlarf.o  dlarfb.o dlarfg.o dlarfgp.o dlarft.o dlarfx.o dlarfy.o \
   dlargv.o dlarrv.o dlartv.o \
   dlarz.o  dlarzb.o dlarzt.o dlaswp.o dlasy2.o \
   dlasyf.o dlasyf_rook.o dlasyf_rk.o \
   dlatbs.o dlatdf.o dlatps.o dlatrd.o dlatrs.o dlatrz.o dlauu2.o \
   dlauum.o dopgtr.o dopmtr.o dorg2l.o dorg2r.o \
   dorgbr.o dorghr.o dorgl2.o dorglq.o dorgql.o dorgqr.o dorgr2.o \
   dorgrq.o dorgtr.o dorm2l.o dorm2r.o dorm22.o \
   dormbr.o dormhr.o dorml2.o dormlq.o dormql.o dormqr.o dormr2.o \
   dormr3.o dormrq.o dormrz.o dormtr.o dpbcon.o dpbequ.o dpbrfs.o \
   dpbstf.o dpbsv.o  dpbsvx.o \
   dpbtf2.o dpbtrf.o dpbtrs.o dpocon.o dpoequ.o dporfs.o dposv.o \
   dposvx.o dpotf2.o dpotrf.o dpotri.o dpotrs.o dpstrf.o dpstf2.o \
   dppcon.o dppequ.o \
   dpprfs.o dppsv.o  dppsvx.o dpptrf.o dpptri.o dpptrs.o dptcon.o \
   dpteqr.o dptrfs.o dptsv.o  dptsvx.o dpttrs.o dptts2.o drscl.o \
   dsbev.o  dsbevd.o dsbevx.o dsbgst.o dsbgv.o  dsbgvd.o dsbgvx.o \
   dsbtrd.o dspcon.o dspev.o  dspevd.o dspevx.o dspgst.o \
   dspgv.o  dspgvd.o dspgvx.o dsprfs.o dspsv.o  dspsvx.o dsptrd.o \
   dsptrf.o dsptri.o dsptrs.o dstegr.o dstein.o dstev.o  dstevd.o dstevr.o \
   dstevx.o \
   dsycon.o dsyev.o  dsyevd.o dsyevr.o \
   dsyevx.o dsygs2.o dsygst.o dsygv.o  dsygvd.o dsygvx.o dsyrfs.o \
   dsysv.o  dsysvx.o \
   dsytd2.o dsytf2.o dsytrd.o dsytrf.o dsytri.o dsytri2.o dsytri2x.o \
   dsyswapr.o dsytrs.o dsytrs2.o \
   dsyconv.o dsyconvf.o dsyconvf_rook.o \
   dsytf2_rook.o dsytrf_rook.o dsytrs_rook.o \
   dsytri_rook.o dsycon_rook.o dsysv_rook.o \
   dsytf2_rk.o dsytrf_rk.o dsytrs_3.o \
   dsytri_3.o dsytri_3x.o dsycon_3.o dsysv_rk.o \
   dlasyf_aa.o dsysv_aa.o dsytrf_aa.o dsytrs_aa.o \
   dsysv_aa_2stage.o dsytrf_aa_2stage.o dsytrs_aa_2stage.o \
   dtbcon.o dtbrfs.o dtbtrs.o dtgevc.o dtgex2.o dtgexc.o dtgsen.o \
   dtgsja.o dtgsna.o dtgsy2.o dtgsyl.o dtpcon.o dtprfs.o dtptri.o \
   dtptrs.o \
   dtrcon.o dtrevc.o dtrevc3.o dtrexc.o dtrrfs.o dtrsen.o dtrsna.o dtrsyl.o \
   dtrti2.o dtrtri.o dtrtrs.o dtzrzf.o dstemr.o \
   dsgesv.o dsposv.o dlag2s.o slag2d.o dlat2s.o \
   dlansf.o dpftrf.o dpftri.o dpftrs.o dsfrk.o dtfsm.o dtftri.o dtfttp.o \
   dtfttr.o dtpttf.o dtpttr.o dtrttf.o dtrttp.o \
   dgejsv.o dgesvj.o dgsvj0.o dgsvj1.o \
   dgeequb.o dsyequb.o dpoequb.o dgbequb.o \
   dbbcsd.o dlapmr.o dorbdb.o dorbdb1.o dorbdb2.o dorbdb3.o dorbdb4.o \
   dorbdb5.o dorbdb6.o dorcsd.o dorcsd2by1.o \
   dgeqrt.o dgeqrt2.o dgeqrt3.o dgemqrt.o \
   dtpqrt.o dtpqrt2.o dtpmqrt.o dtprfb.o \
   dgelqt.o dgelqt3.o dgemlqt.o \
   dgetsls.o dgeqr.o dlatsqr.o dlamtsqr.o dgemqr.o \
   dgelq.o dlaswlq.o dlamswlq.o dgemlq.o \
   dtplqt.o dtplqt2.o dtpmlqt.o \
   dsytrd_2stage.o dsytrd_sy2sb.o dsytrd_sb2st.o dsb2st_kernels.o \
   dsyevd_2stage.o dsyev_2stage.o dsyevx_2stage.o dsyevr_2stage.o \
   dsbev_2stage.o dsbevx_2stage.o dsbevd_2stage.o dsygv_2stage.o

ifdef USEXBLAS
DXLASRC = dgesvxx.o dgerfsx.o dla_gerfsx_extended.o dla_geamv.o \
   dla_gercond.o dla_gerpvgrw.o dsysvxx.o dsyrfsx.o \
   dla_syrfsx_extended.o dla_syamv.o dla_syrcond.o dla_syrpvgrw.o \
   dposvxx.o dporfsx.o dla_porfsx_extended.o dla_porcond.o \
   dla_porpvgrw.o dgbsvxx.o dgbrfsx.o dla_gbrfsx_extended.o \
   dla_gbamv.o dla_gbrcond.o dla_gbrpvgrw.o dla_lin_berr.o dlarscl2.o \
   dlascl2.o dla_wwaddw.o
endif

ZLASRC = \
   zpotrf2.o zgetrf2.o \
   zbdsqr.o zgbbrd.o zgbcon.o zgbequ.o zgbrfs.o zgbsv.o  zgbsvx.o \
   zgbtf2.o zgbtrf.o zgbtrs.o zgebak.o zgebal.o zgebd2.o zgebrd.o \
   zgecon.o zgeequ.o zgees.o  zgeesx.o zgeev.o  zgeevx.o \
   zgehd2.o zgehrd.o zgelq2.o zgelqf.o \
   zgels.o  zgelsd.o zgelss.o zgelsy.o zgeql2.o zgeqlf.o zgeqp3.o \
   zgeqr2.o zgeqr2p.o zgeqrf.o zgeqrfp.o zgerfs.o zgerq2.o zgerqf.o \
   zgesc2.o zgesdd.o zgesv.o  zgesvd.o zgesvdx.o \
   zgesvj.o zgejsv.o zgsvj0.o zgsvj1.o \
   zgesvx.o zgetc2.o zgetf2.o zgetrf.o \
   zgetri.o zgetrs.o \
   zggbak.o zggbal.o zgges.o  zgges3.o zggesx.o \
   zggev.o  zggev3.o zggevx.o zggglm.o \
   zgghrd.o zgghd3.o zgglse.o zggqrf.o zggrqf.o \
   zggsvd3.o zggsvp3.o \
   zgtcon.o zgtrfs.o zgtsv.o  zgtsvx.o zgttrf.o zgttrs.o zgtts2.o zhbev.o \
   zhbevd.o zhbevx.o zhbgst.o zhbgv.o  zhbgvd.o zhbgvx.o zhbtrd.o \
   zhecon.o zheev.o  zheevd.o zheevr.o zheevx.o zhegs2.o zhegst.o \
   zhegv.o  zhegvd.o zhegvx.o zherfs.o zhesv.o  zhesvx.o zhetd2.o \
   zhetf2.o zhetrd.o \
   zhetrf.o zhetri.o zhetri2.o zhetri2x.o zheswapr.o \
   zhetrs.o zhetrs2.o \
   zhetf2_rook.o zhetrf_rook.o zhetri_rook.o \
   zhetrs_rook.o zhecon_rook.o zhesv_rook.o \
   zhetf2_rk.o zhetrf_rk.o zhetri_3.o zhetri_3x.o \
   zhetrs_3.o zhecon_3.o zhesv_rk.o \
   zhesv_aa.o zhetrf_aa.o zhetrs_aa.o zlahef_aa.o \
   zhesv_aa_2stage.o zhetrf_aa_2stage.o zhetrs_aa_2stage.o \
   zhgeqz.o zhpcon.o zhpev.o  zhpevd.o \
   zhpevx.o zhpgst.o zhpgv.o  zhpgvd.o zhpgvx.o zhprfs.o zhpsv.o \
   zhpsvx.o \
   zhptrd.o zhptrf.o zhptri.o zhptrs.o zhsein.o zhseqr.o zlabrd.o \
   zlacgv.o zlacon.o zlacn2.o zlacp2.o zlacpy.o zlacrm.o zlacrt.o zladiv.o \
   zlaed0.o zlaed7.o zlaed8.o \
   zlaein.o zlaesy.o zlaev2.o zlags2.o zlagtm.o \
   zlahef.o zlahef_rook.o zlahef_rk.o zlahqr.o \
   zlahr2.o zlaic1.o zlals0.o zlalsa.o zlalsd.o zlangb.o zlange.o \
   zlangt.o zlanhb.o \
   zlanhe.o \
   zlanhp.o zlanhs.o zlanht.o zlansb.o zlansp.o zlansy.o zlantb.o \
   zlantp.o zlantr.o zlapll.o zlapmt.o zlaqgb.o zlaqge.o \
   zlaqhb.o zlaqhe.o zlaqhp.o zlaqp2.o zlaqps.o zlaqsb.o \
   zlaqr0.o zlaqr1.o zlaqr2.o zlaqr3.o zlaqr4.o zlaqr5.o \
   zlaqsp.o zlaqsy.o zlar1v.o zlar2v.o ilazlr.o ilazlc.o \
   zlarcm.o zlarf.o  zlarfb.o \
   zlarfg.o zlarft.o zlarfgp.o \
   zlarfx.o zlarfy.o zlargv.o zlarnv.o zlarrv.o zlartg.o zlartv.o \
   zlarz.o  zlarzb.o zlarzt.o zlascl.o zlaset.o zlasr.o \
   zlassq.o zlaswp.o zlasyf.o zlasyf_rook.o zlasyf_rk.o zlasyf_aa.o \
   zlatbs.o zlatdf.o zlatps.o zlatrd.o zlatrs.o zlatrz.o zlauu2.o \
   zlauum.o zpbcon.o zpbequ.o zpbrfs.o zpbstf.o zpbsv.o \
   zpbsvx.o zpbtf2.o zpbtrf.o zpbtrs.o zpocon.o zpoequ.o zporfs.o \
   zposv.o  zposvx.o zpotf2.o zpotrf.o zpotri.o zpotrs.o zpstrf.o zpstf2.o \
   zppcon.o zppequ.o zpprfs.o zppsv.o  zppsvx.o zpptrf.o zpptri.o zpptrs.o \
   zptcon.o zpteqr.o zptrfs.o zptsv.o  zptsvx.o zpttrf.o zpttrs.o zptts2.o \
   zrot.o   zspcon.o zspmv.o  zspr.o   zsprfs.o zspsv.o \
   zspsvx.o zsptrf.o zsptri.o zsptrs.o zdrscl.o zstedc.o \
   zstegr.o zstein.o zsteqr.o \
   zsycon.o zsymv.o \
   zsyr.o zsyrfs.o zsysv.o zsysvx.o zsytf2.o zsytrf.o zsytri.o zsytri2.o zsytri2x.o \
   zsyswapr.o zsytrs.o zsytrs2.o \
   zsyconv.o zsyconvf.o zsyconvf_rook.o \
   zsytf2_rook.o zsytrf_rook.o zsytrs_rook.o zsytrs_aa.o \
   zsytri_rook.o zsycon_rook.o zsysv_rook.o \
   zsysv_aa_2stage.o zsytrf_aa_2stage.o zsytrs_aa_2stage.o \
   zsytf2_rk.o zsytrf_rk.o zsytrf_aa.o zsytrs_3.o \
   zsytri_3.o zsytri_3x.o zsycon_3.o zsysv_rk.o zsysv_aa.o \
   ztbcon.o ztbrfs.o ztbtrs.o ztgevc.o ztgex2.o \
   ztgexc.o ztgsen.o ztgsja.o ztgsna.o ztgsy2.o ztgsyl.o ztpcon.o \
   ztprfs.o ztptri.o \
   ztptrs.o ztrcon.o ztrevc.o ztrevc3.o ztrexc.o ztrrfs.o ztrsen.o ztrsna.o \
   ztrsyl.o ztrti2.o ztrtri.o ztrtrs.o ztzrzf.o zung2l.o \
   zung2r.o zungbr.o zunghr.o zungl2.o zunglq.o zungql.o zungqr.o zungr2.o \
   zungrq.o zungtr.o zunm2l.o zunm2r.o zunmbr.o zunmhr.o zunml2.o zunm22.o \
   zunmlq.o zunmql.o zunmqr.o zunmr2.o zunmr3.o zunmrq.o zunmrz.o \
   zunmtr.o zupgtr.o \
   zupmtr.o izmax1.o dzsum1.o zstemr.o \
   zcgesv.o zcposv.o zlag2c.o clag2z.o zlat2c.o \
   zhfrk.o ztfttp.o zlanhf.o zpftrf.o zpftri.o zpftrs.o ztfsm.o ztftri.o \
   ztfttr.o ztpttf.o ztpttr.o ztrttf.o ztrttp.o \
   zgeequb.o zgbequb.o zsyequb.o zpoequb.o zheequb.o \
   zbbcsd.o zlapmr.o zunbdb.o zunbdb1.o zunbdb2.o zunbdb3.o zunbdb4.o \
   zunbdb5.o zunbdb6.o zuncsd.o zuncsd2by1.o \
   zgeqrt.o zgeqrt2.o zgeqrt3.o zgemqrt.o \
   ztpqrt.o ztpqrt2.o ztpmqrt.o ztprfb.o \
   ztplqt.o ztplqt2.o ztpmlqt.o \
   zgelqt.o zgelqt3.o zgemlqt.o \
   zgetsls.o zgeqr.o zlatsqr.o zlamtsqr.o zgemqr.o \
   zgelq.o zlaswlq.o zlamswlq.o zgemlq.o \
   zhetrd_2stage.o zhetrd_he2hb.o zhetrd_hb2st.o zhb2st_kernels.o \
   zheevd_2stage.o zheev_2stage.o zheevx_2stage.o zheevr_2stage.o \
   zhbev_2stage.o zhbevx_2stage.o zhbevd_2stage.o zhegv_2stage.o

ifdef USEXBLAS
ZXLASRC = zgesvxx.o zgerfsx.o zla_gerfsx_extended.o zla_geamv.o \
   zla_gercond_c.o zla_gercond_x.o zla_gerpvgrw.o zsysvxx.o zsyrfsx.o \
   zla_syrfsx_extended.o zla_syamv.o zla_syrcond_c.o zla_syrcond_x.o \
   zla_syrpvgrw.o zposvxx.o zporfsx.o zla_porfsx_extended.o \
   zla_porcond_c.o zla_porcond_x.o zla_porpvgrw.o zgbsvxx.o zgbrfsx.o \
   zla_gbrfsx_extended.o zla_gbamv.o zla_gbrcond_c.o zla_gbrcond_x.o \
   zla_gbrpvgrw.o zhesvxx.o zherfsx.o zla_herfsx_extended.o \
   zla_heamv.o zla_hercond_c.o zla_hercond_x.o zla_herpvgrw.o \
   zla_lin_berr.o zlarscl2.o zlascl2.o zla_wwaddw.o
endif

DEPRECSRC = DEPRECATED/cgegs.o DEPRECATED/cgegv.o DEPRECATED/cgelsx.o \
   DEPRECATED/cgeqpf.o DEPRECATED/cggsvd.o DEPRECATED/cggsvp.o \
   DEPRECATED/clahrd.o DEPRECATED/clatzm.o DEPRECATED/ctzrqf.o \
   DEPRECATED/dgegs.o  DEPRECATED/dgegv.o  DEPRECATED/dgelsx.o \
   DEPRECATED/dgeqpf.o DEPRECATED/dggsvd.o DEPRECATED/dggsvp.o \
   DEPRECATED/dlahrd.o DEPRECATED/dlatzm.o DEPRECATED/dtzrqf.o \
   DEPRECATED/sgegs.o  DEPRECATED/sgegv.o  DEPRECATED/sgelsx.o \
   DEPRECATED/sgeqpf.o DEPRECATED/sggsvd.o DEPRECATED/sggsvp.o \
   DEPRECATED/slahrd.o DEPRECATED/slatzm.o DEPRECATED/stzrqf.o \
   DEPRECATED/zgegs.o  DEPRECATED/zgegv.o  DEPRECATED/zgelsx.o \
   DEPRECATED/zgeqpf.o DEPRECATED/zggsvd.o DEPRECATED/zggsvp.o \
   DEPRECATED/zlahrd.o DEPRECATED/zlatzm.o DEPRECATED/ztzrqf.o

ALLOBJ = $(SLASRC) $(DLASRC) $(DSLASRC) $(CLASRC) $(ZLASRC) $(ZCLASRC) \
   $(SCLAUX) $(DZLAUX) $(ALLAUX)

ifdef USEXBLAS
ALLXOBJ = $(SXLASRC) $(DXLASRC) $(CXLASRC) $(ZXLASRC)
endif

ifdef BUILD_DEPRECATED
DEPRECATED = $(DEPRECSRC)
endif

all: ../$(LAPACKLIB)

../$(LAPACKLIB): $(ALLOBJ) $(ALLXOBJ) $(DEPRECATED)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

single: $(SLASRC) $(DSLASRC) $(SXLASRC) $(SCLAUX) $(ALLAUX)
	$(ARCH) $(ARCHFLAGS) ../$(LAPACKLIB) $^
	$(RANLIB) ../$(LAPACKLIB)

complex: $(CLASRC) $(ZCLASRC) $(CXLASRC) $(SCLAUX) $(ALLAUX)
	$(ARCH) $(ARCHFLAGS) ../$(LAPACKLIB) $^
	$(RANLIB) ../$(LAPACKLIB)

double: $(DLASRC) $(DSLASRC) $(DXLASRC) $(DZLAUX) $(ALLAUX)
	$(ARCH) $(ARCHFLAGS) ../$(LAPACKLIB) $^
	$(RANLIB) ../$(LAPACKLIB)

complex16: $(ZLASRC) $(ZCLASRC) $(ZXLASRC) $(DZLAUX) $(ALLAUX)
	$(ARCH) $(ARCHFLAGS) ../$(LAPACKLIB) $^
	$(RANLIB) ../$(LAPACKLIB)

$(ALLAUX): $(FRC)
$(SCLAUX): $(FRC)
$(DZLAUX): $(FRC)
$(SLASRC): $(FRC)
$(CLASRC): $(FRC)
$(DLASRC): $(FRC)
$(ZLASRC): $(FRC)
$(ZCLASRC): $(FRC)
$(DSLASRC): $(FRC)
ifdef USEXBLAS
$(SXLASRC): $(FRC)
$(CXLASRC): $(FRC)
$(DXLASRC): $(FRC)
$(ZXLASRC): $(FRC)
endif

FRC:
	@FRC=$(FRC)

clean: cleanobj cleanlib
cleanobj:
	rm -f *.o DEPRECATED/*.o
cleanlib:
	rm -f ../$(LAPACKLIB)

.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<
.F.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<

slaruv.o: slaruv.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
dlaruv.o: dlaruv.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
sla_wwaddw.o: sla_wwaddw.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
dla_wwaddw.o: dla_wwaddw.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
cla_wwaddw.o: cla_wwaddw.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
zla_wwaddw.o: zla_wwaddw.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
