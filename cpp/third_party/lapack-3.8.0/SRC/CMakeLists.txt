#######################################################################
#  This is the makefile to create a library for LAPACK.
#  The files are organized as follows:

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
#  ***Note***
#  The functions lsame, second, dsecnd, slamch, and dlamch may have
#  to be installed before compiling the library.  Refer to the
#  installation guide, LAPACK Working Note 41, for instructions.
#
#######################################################################

set(ALLAUX ilaenv.f ilaenv2stage.f ieeeck.f lsamen.f iparmq.f iparam2stage.F
   ilaprec.f ilatrans.f ilauplo.f iladiag.f chla_transtype.f
   ../INSTALL/ilaver.f ../INSTALL/lsame.f xerbla.f xerbla_array.f
   ../INSTALL/slamch.f)

set(SCLAUX
   sbdsdc.f
   sbdsqr.f sdisna.f slabad.f slacpy.f sladiv.f slae2.f  slaebz.f
   slaed0.f slaed1.f slaed2.f slaed3.f slaed4.f slaed5.f slaed6.f
   slaed7.f slaed8.f slaed9.f slaeda.f slaev2.f slagtf.f
   slagts.f slamrg.f slanst.f
   slapy2.f slapy3.f slarnv.f
   slarra.f slarrb.f slarrc.f slarrd.f slarre.f slarrf.f slarrj.f
   slarrk.f slarrr.f slaneg.f
   slartg.f slaruv.f slas2.f  slascl.f
   slasd0.f slasd1.f slasd2.f slasd3.f slasd4.f slasd5.f slasd6.f
   slasd7.f slasd8.f slasda.f slasdq.f slasdt.f
   slaset.f slasq1.f slasq2.f slasq3.f slasq4.f slasq5.f slasq6.f
   slasr.f  slasrt.f slassq.f slasv2.f spttrf.f sstebz.f sstedc.f
   ssteqr.f ssterf.f slaisnan.f sisnan.f
   slartgp.f slartgs.f
   ${SECOND_SRC})

set(DZLAUX
   dbdsdc.f
   dbdsqr.f ddisna.f dlabad.f dlacpy.f dladiv.f dlae2.f  dlaebz.f
   dlaed0.f dlaed1.f dlaed2.f dlaed3.f dlaed4.f dlaed5.f dlaed6.f
   dlaed7.f dlaed8.f dlaed9.f dlaeda.f dlaev2.f dlagtf.f
   dlagts.f dlamrg.f dlanst.f
   dlapy2.f dlapy3.f dlarnv.f
   dlarra.f dlarrb.f dlarrc.f dlarrd.f dlarre.f dlarrf.f dlarrj.f
   dlarrk.f dlarrr.f dlaneg.f
   dlartg.f dlaruv.f dlas2.f  dlascl.f
   dlasd0.f dlasd1.f dlasd2.f dlasd3.f dlasd4.f dlasd5.f dlasd6.f
   dlasd7.f dlasd8.f dlasda.f dlasdq.f dlasdt.f
   dlaset.f dlasq1.f dlasq2.f dlasq3.f dlasq4.f dlasq5.f dlasq6.f
   dlasr.f  dlasrt.f dlassq.f dlasv2.f dpttrf.f dstebz.f dstedc.f
   dsteqr.f dsterf.f dlaisnan.f disnan.f
   dlartgp.f dlartgs.f
   ../INSTALL/dlamch.f ${DSECOND_SRC})

set(SLASRC
   sbdsvdx.f sgbbrd.f sgbcon.f sgbequ.f sgbrfs.f sgbsv.f
   sgbsvx.f sgbtf2.f sgbtrf.f sgbtrs.f sgebak.f sgebal.f sgebd2.f
   sgebrd.f sgecon.f sgeequ.f sgees.f  sgeesx.f sgeev.f  sgeevx.f
   sgehd2.f sgehrd.f sgelq2.f sgelqf.f
   sgels.f  sgelsd.f sgelss.f sgelsy.f sgeql2.f sgeqlf.f
   sgeqp3.f sgeqr2.f sgeqr2p.f sgeqrf.f sgeqrfp.f sgerfs.f sgerq2.f sgerqf.f
   sgesc2.f sgesdd.f sgesv.f  sgesvd.f sgesvdx.f sgesvx.f sgetc2.f sgetf2.f
   sgetrf2.f sgetri.f
   sggbak.f sggbal.f
   sgges.f  sgges3.f sggesx.f sggev.f  sggev3.f sggevx.f
   sggglm.f sgghrd.f sgghd3.f sgglse.f sggqrf.f
   sggrqf.f sggsvd3.f sggsvp3.f sgtcon.f sgtrfs.f sgtsv.f
   sgtsvx.f sgttrf.f sgttrs.f sgtts2.f shgeqz.f
   shsein.f shseqr.f slabrd.f slacon.f slacn2.f
   slaein.f slaexc.f slag2.f  slags2.f slagtm.f slagv2.f slahqr.f
   slahr2.f slaic1.f slaln2.f slals0.f slalsa.f slalsd.f
   slangb.f slange.f slangt.f slanhs.f slansb.f slansp.f
   slansy.f slantb.f slantp.f slantr.f slanv2.f
   slapll.f slapmt.f
   slaqgb.f slaqge.f slaqp2.f slaqps.f slaqsb.f slaqsp.f slaqsy.f
   slaqr0.f slaqr1.f slaqr2.f slaqr3.f slaqr4.f slaqr5.f
   slaqtr.f slar1v.f slar2v.f ilaslr.f ilaslc.f
   slarf.f  slarfb.f slarfg.f slarfgp.f slarft.f slarfx.f slarfy.f slargv.f
   slarrv.f slartv.f
   slarz.f  slarzb.f slarzt.f slaswp.f slasy2.f 
   slasyf.f slasyf_rook.f slasyf_rk.f slasyf_aa.f
   slatbs.f slatdf.f slatps.f slatrd.f slatrs.f slatrz.f
   slauu2.f slauum.f sopgtr.f sopmtr.f sorg2l.f sorg2r.f
   sorgbr.f sorghr.f sorgl2.f sorglq.f sorgql.f sorgqr.f sorgr2.f
   sorgrq.f sorgtr.f sorm2l.f sorm2r.f sorm22.f
   sormbr.f sormhr.f sorml2.f sormlq.f sormql.f sormqr.f sormr2.f
   sormr3.f sormrq.f sormrz.f sormtr.f spbcon.f spbequ.f spbrfs.f
   spbstf.f spbsv.f  spbsvx.f
   spbtf2.f spbtrf.f spbtrs.f spocon.f spoequ.f sporfs.f sposv.f
   sposvx.f spotf2.f spotrf2.f spotri.f spstrf.f spstf2.f
   sppcon.f sppequ.f
   spprfs.f sppsv.f  sppsvx.f spptrf.f spptri.f spptrs.f sptcon.f
   spteqr.f sptrfs.f sptsv.f  sptsvx.f spttrs.f sptts2.f srscl.f
   ssbev.f  ssbevd.f ssbevx.f ssbgst.f ssbgv.f  ssbgvd.f ssbgvx.f
   ssbtrd.f sspcon.f sspev.f  sspevd.f sspevx.f sspgst.f
   sspgv.f  sspgvd.f sspgvx.f ssprfs.f sspsv.f  sspsvx.f ssptrd.f
   ssptrf.f ssptri.f ssptrs.f sstegr.f sstein.f sstev.f  sstevd.f sstevr.f
   sstevx.f ssycon.f ssyev.f  ssyevd.f ssyevr.f ssyevx.f ssygs2.f
   ssygst.f ssygv.f  ssygvd.f ssygvx.f ssyrfs.f ssysv.f  ssysvx.f
   ssytd2.f ssytf2.f ssytrd.f ssytrf.f ssytri.f ssytri2.f ssytri2x.f
   ssyswapr.f ssytrs.f ssytrs2.f
   ssyconv.f ssyconvf.f ssyconvf_rook.f
   ssytf2_rook.f ssytrf_rook.f ssytrs_rook.f
   ssytri_rook.f ssycon_rook.f ssysv_rook.f
   ssytf2_rk.f ssytrf_rk.f ssytrs_3.f
   ssytri_3.f ssytri_3x.f ssycon_3.f ssysv_rk.f
   ssysv_aa.f ssytrf_aa.f ssytrs_aa.f
   ssysv_aa_2stage.f ssytrf_aa_2stage.f ssytrs_aa_2stage.f
   stbcon.f
   stbrfs.f stbtrs.f stgevc.f stgex2.f stgexc.f stgsen.f
   stgsja.f stgsna.f stgsy2.f stgsyl.f stpcon.f stprfs.f stptri.f
   stptrs.f
   strcon.f strevc.f strevc3.f strexc.f strrfs.f strsen.f strsna.f strsyl.f
   strti2.f strtri.f strtrs.f stzrzf.f sstemr.f
   slansf.f spftrf.f spftri.f spftrs.f ssfrk.f stfsm.f stftri.f stfttp.f
   stfttr.f stpttf.f stpttr.f strttf.f strttp.f
   sgejsv.f sgesvj.f sgsvj0.f sgsvj1.f
   sgeequb.f ssyequb.f spoequb.f sgbequb.f
   sbbcsd.f slapmr.f sorbdb.f sorbdb1.f sorbdb2.f sorbdb3.f sorbdb4.f
   sorbdb5.f sorbdb6.f sorcsd.f sorcsd2by1.f
   sgeqrt.f sgeqrt2.f sgeqrt3.f sgemqrt.f
   stpqrt.f stpqrt2.f stpmqrt.f stprfb.f
   sgelqt.f sgelqt3.f sgemlqt.f
   sgetsls.f sgeqr.f slatsqr.f slamtsqr.f sgemqr.f
   sgelq.f slaswlq.f slamswlq.f sgemlq.f
   stplqt.f stplqt2.f stpmlqt.f
   ssytrd_2stage.f ssytrd_sy2sb.f ssytrd_sb2st.F ssb2st_kernels.f
   ssyevd_2stage.f ssyev_2stage.f ssyevx_2stage.f ssyevr_2stage.f
   ssbev_2stage.f ssbevx_2stage.f ssbevd_2stage.f ssygv_2stage.f)

set(DSLASRC spotrs.f sgetrs.f spotrf.f sgetrf.f)

set(SXLASRC sgesvxx.f sgerfsx.f sla_gerfsx_extended.f sla_geamv.f
   sla_gercond.f sla_gerpvgrw.f ssysvxx.f ssyrfsx.f
   sla_syrfsx_extended.f sla_syamv.f sla_syrcond.f sla_syrpvgrw.f
   sposvxx.f sporfsx.f sla_porfsx_extended.f sla_porcond.f
   sla_porpvgrw.f sgbsvxx.f sgbrfsx.f sla_gbrfsx_extended.f
   sla_gbamv.f sla_gbrcond.f sla_gbrpvgrw.f sla_lin_berr.f slarscl2.f
   slascl2.f sla_wwaddw.f)

set(CLASRC
   cbdsqr.f cgbbrd.f cgbcon.f cgbequ.f cgbrfs.f cgbsv.f  cgbsvx.f
   cgbtf2.f cgbtrf.f cgbtrs.f cgebak.f cgebal.f cgebd2.f cgebrd.f
   cgecon.f cgeequ.f cgees.f  cgeesx.f cgeev.f  cgeevx.f
   cgehd2.f cgehrd.f cgelq2.f cgelqf.f
   cgels.f  cgelsd.f cgelss.f cgelsy.f cgeql2.f cgeqlf.f cgeqp3.f
   cgeqr2.f cgeqr2p.f cgeqrf.f cgeqrfp.f cgerfs.f cgerq2.f cgerqf.f
   cgesc2.f cgesdd.f cgesv.f  cgesvd.f cgesvdx.f
   cgesvj.f cgejsv.f cgsvj0.f cgsvj1.f
   cgesvx.f cgetc2.f cgetf2.f cgetrf2.f
   cgetri.f
   cggbak.f cggbal.f
   cgges.f  cgges3.f cggesx.f cggev.f  cggev3.f cggevx.f
   cggglm.f cgghrd.f cgghd3.f cgglse.f cggqrf.f cggrqf.f
   cggsvd3.f cggsvp3.f
   cgtcon.f cgtrfs.f cgtsv.f  cgtsvx.f cgttrf.f cgttrs.f cgtts2.f chbev.f
   chbevd.f chbevx.f chbgst.f chbgv.f  chbgvd.f chbgvx.f chbtrd.f
   checon.f cheev.f  cheevd.f cheevr.f cheevx.f chegs2.f chegst.f
   chegv.f  chegvd.f chegvx.f cherfs.f chesv.f  chesvx.f chetd2.f
   chetf2.f chetrd.f
   chetrf.f chetri.f chetri2.f chetri2x.f cheswapr.f
   chetrs.f chetrs2.f
   chetf2_rook.f chetrf_rook.f chetri_rook.f
   chetrs_rook.f checon_rook.f chesv_rook.f
   chetf2_rk.f chetrf_rk.f chetri_3.f chetri_3x.f
   chetrs_3.f checon_3.f chesv_rk.f
   chesv_aa.f chetrf_aa.f chetrs_aa.f
   chesv_aa_2stage.f chetrf_aa_2stage.f chetrs_aa_2stage.f
   chgeqz.f chpcon.f chpev.f  chpevd.f
   chpevx.f chpgst.f chpgv.f  chpgvd.f chpgvx.f chprfs.f chpsv.f
   chpsvx.f
   chptrd.f chptrf.f chptri.f chptrs.f chsein.f chseqr.f clabrd.f
   clacgv.f clacon.f clacn2.f clacp2.f clacpy.f clacrm.f clacrt.f cladiv.f
   claed0.f claed7.f claed8.f
   claein.f claesy.f claev2.f clags2.f clagtm.f
   clahef.f clahef_rook.f clahef_rk.f clahef_aa.f clahqr.f
   clahr2.f claic1.f clals0.f clalsa.f clalsd.f clangb.f clange.f clangt.f
   clanhb.f clanhe.f
   clanhp.f clanhs.f clanht.f clansb.f clansp.f clansy.f clantb.f
   clantp.f clantr.f clapll.f clapmt.f clarcm.f claqgb.f claqge.f
   claqhb.f claqhe.f claqhp.f claqp2.f claqps.f claqsb.f
   claqr0.f claqr1.f claqr2.f claqr3.f claqr4.f claqr5.f
   claqsp.f claqsy.f clar1v.f clar2v.f ilaclr.f ilaclc.f
   clarf.f  clarfb.f clarfg.f clarfgp.f clarft.f
   clarfx.f clarfy.f clargv.f clarnv.f clarrv.f clartg.f clartv.f
   clarz.f  clarzb.f clarzt.f clascl.f claset.f clasr.f  classq.f
   claswp.f clasyf.f clasyf_rook.f clasyf_rk.f clasyf_aa.f
   clatbs.f clatdf.f clatps.f clatrd.f clatrs.f clatrz.f
   clauu2.f clauum.f cpbcon.f cpbequ.f cpbrfs.f cpbstf.f cpbsv.f
   cpbsvx.f cpbtf2.f cpbtrf.f cpbtrs.f cpocon.f cpoequ.f cporfs.f
   cposv.f  cposvx.f cpotf2.f cpotrf2.f cpotri.f cpstrf.f cpstf2.f
   cppcon.f cppequ.f cpprfs.f cppsv.f  cppsvx.f cpptrf.f cpptri.f cpptrs.f
   cptcon.f cpteqr.f cptrfs.f cptsv.f  cptsvx.f cpttrf.f cpttrs.f cptts2.f
   crot.f   cspcon.f cspmv.f  cspr.f   csprfs.f cspsv.f
   cspsvx.f csptrf.f csptri.f csptrs.f csrscl.f cstedc.f
   cstegr.f cstein.f csteqr.f csycon.f csymv.f
   csyr.f   csyrfs.f csysv.f  csysvx.f csytf2.f csytrf.f csytri.f
   csytri2.f csytri2x.f csyswapr.f
   csytrs.f csytrs2.f
   csyconv.f csyconvf.f csyconvf_rook.f
   csytf2_rook.f csytrf_rook.f csytrs_rook.f
   csytri_rook.f csycon_rook.f csysv_rook.f
   csytf2_rk.f csytrf_rk.f csytrf_aa.f csytrs_3.f csytrs_aa.f
   csytri_3.f csytri_3x.f csycon_3.f csysv_rk.f csysv_aa.f
   csysv_aa_2stage.f csytrf_aa_2stage.f csytrs_aa_2stage.f
   ctbcon.f ctbrfs.f ctbtrs.f ctgevc.f ctgex2.f
   ctgexc.f ctgsen.f ctgsja.f ctgsna.f ctgsy2.f ctgsyl.f ctpcon.f
   ctprfs.f ctptri.f
   ctptrs.f ctrcon.f ctrevc.f ctrevc3.f ctrexc.f ctrrfs.f ctrsen.f ctrsna.f
   ctrsyl.f ctrti2.f ctrtri.f ctrtrs.f ctzrzf.f cung2l.f cung2r.f
   cungbr.f cunghr.f cungl2.f cunglq.f cungql.f cungqr.f cungr2.f
   cungrq.f cungtr.f cunm2l.f cunm2r.f cunmbr.f cunmhr.f cunml2.f cunm22.f
   cunmlq.f cunmql.f cunmqr.f cunmr2.f cunmr3.f cunmrq.f cunmrz.f
   cunmtr.f cupgtr.f cupmtr.f icmax1.f scsum1.f cstemr.f
   chfrk.f ctfttp.f clanhf.f cpftrf.f cpftri.f cpftrs.f ctfsm.f ctftri.f
   ctfttr.f ctpttf.f ctpttr.f ctrttf.f ctrttp.f
   cgeequb.f cgbequb.f csyequb.f cpoequb.f cheequb.f
   cbbcsd.f clapmr.f cunbdb.f cunbdb1.f cunbdb2.f cunbdb3.f cunbdb4.f
   cunbdb5.f cunbdb6.f cuncsd.f cuncsd2by1.f
   cgeqrt.f cgeqrt2.f cgeqrt3.f cgemqrt.f
   ctpqrt.f ctpqrt2.f ctpmqrt.f ctprfb.f
   cgelqt.f cgelqt3.f cgemlqt.f
   cgetsls.f cgeqr.f clatsqr.f clamtsqr.f cgemqr.f
   cgelq.f claswlq.f clamswlq.f cgemlq.f
   ctplqt.f ctplqt2.f ctpmlqt.f
   chetrd_2stage.f chetrd_he2hb.f chetrd_hb2st.F chb2st_kernels.f
   cheevd_2stage.f cheev_2stage.f cheevx_2stage.f cheevr_2stage.f
   chbev_2stage.f chbevx_2stage.f chbevd_2stage.f chegv_2stage.f)

set(CXLASRC cgesvxx.f cgerfsx.f cla_gerfsx_extended.f cla_geamv.f
   cla_gercond_c.f cla_gercond_x.f cla_gerpvgrw.f
   csysvxx.f csyrfsx.f cla_syrfsx_extended.f cla_syamv.f
   cla_syrcond_c.f cla_syrcond_x.f cla_syrpvgrw.f
   cposvxx.f cporfsx.f cla_porfsx_extended.f
   cla_porcond_c.f cla_porcond_x.f cla_porpvgrw.f
   cgbsvxx.f cgbrfsx.f cla_gbrfsx_extended.f cla_gbamv.f
   cla_gbrcond_c.f cla_gbrcond_x.f cla_gbrpvgrw.f
   chesvxx.f cherfsx.f cla_herfsx_extended.f cla_heamv.f
   cla_hercond_c.f cla_hercond_x.f cla_herpvgrw.f
   cla_lin_berr.f clarscl2.f clascl2.f cla_wwaddw.f)

set(ZCLASRC cpotrs.f cgetrs.f cpotrf.f cgetrf.f)

set(DLASRC
   dbdsvdx.f dgbbrd.f dgbcon.f dgbequ.f dgbrfs.f dgbsv.f
   dgbsvx.f dgbtf2.f dgbtrf.f dgbtrs.f dgebak.f dgebal.f dgebd2.f
   dgebrd.f dgecon.f dgeequ.f dgees.f  dgeesx.f dgeev.f  dgeevx.f
   dgehd2.f dgehrd.f dgelq2.f dgelqf.f
   dgels.f  dgelsd.f dgelss.f dgelsy.f dgeql2.f dgeqlf.f
   dgeqp3.f dgeqr2.f dgeqr2p.f dgeqrf.f dgeqrfp.f dgerfs.f dgerq2.f dgerqf.f
   dgesc2.f dgesdd.f dgesv.f  dgesvd.f dgesvdx.f dgesvx.f dgetc2.f dgetf2.f
   dgetrf.f dgetrf2.f dgetri.f
   dgetrs.f dggbak.f dggbal.f
   dgges.f  dgges3.f dggesx.f dggev.f  dggev3.f dggevx.f
   dggglm.f dgghrd.f dgghd3.f dgglse.f dggqrf.f
   dggrqf.f dggsvd3.f dggsvp3.f dgtcon.f dgtrfs.f dgtsv.f
   dgtsvx.f dgttrf.f dgttrs.f dgtts2.f dhgeqz.f
   dhsein.f dhseqr.f dlabrd.f dlacon.f dlacn2.f
   dlaein.f dlaexc.f dlag2.f  dlags2.f dlagtm.f dlagv2.f dlahqr.f
   dlahr2.f dlaic1.f dlaln2.f dlals0.f dlalsa.f dlalsd.f
   dlangb.f dlange.f dlangt.f dlanhs.f dlansb.f dlansp.f
   dlansy.f dlantb.f dlantp.f dlantr.f dlanv2.f
   dlapll.f dlapmt.f
   dlaqgb.f dlaqge.f dlaqp2.f dlaqps.f dlaqsb.f dlaqsp.f dlaqsy.f
   dlaqr0.f dlaqr1.f dlaqr2.f dlaqr3.f dlaqr4.f dlaqr5.f
   dlaqtr.f dlar1v.f dlar2v.f iladlr.f iladlc.f
   dlarf.f  dlarfb.f dlarfg.f dlarfgp.f dlarft.f dlarfx.f dlarfy.f
   dlargv.f dlarrv.f dlartv.f
   dlarz.f  dlarzb.f dlarzt.f dlaswp.f dlasy2.f 
   dlasyf.f dlasyf_rook.f dlasyf_rk.f dlasyf_aa.f
   dlatbs.f dlatdf.f dlatps.f dlatrd.f dlatrs.f dlatrz.f dlauu2.f
   dlauum.f dopgtr.f dopmtr.f dorg2l.f dorg2r.f
   dorgbr.f dorghr.f dorgl2.f dorglq.f dorgql.f dorgqr.f dorgr2.f
   dorgrq.f dorgtr.f dorm2l.f dorm2r.f dorm22.f
   dormbr.f dormhr.f dorml2.f dormlq.f dormql.f dormqr.f dormr2.f
   dormr3.f dormrq.f dormrz.f dormtr.f dpbcon.f dpbequ.f dpbrfs.f
   dpbstf.f dpbsv.f  dpbsvx.f
   dpbtf2.f dpbtrf.f dpbtrs.f dpocon.f dpoequ.f dporfs.f dposv.f
   dposvx.f dpotf2.f dpotrf.f dpotrf2.f dpotri.f dpotrs.f dpstrf.f dpstf2.f
   dppcon.f dppequ.f
   dpprfs.f dppsv.f  dppsvx.f dpptrf.f dpptri.f dpptrs.f dptcon.f
   dpteqr.f dptrfs.f dptsv.f  dptsvx.f dpttrs.f dptts2.f drscl.f
   dsbev.f  dsbevd.f dsbevx.f dsbgst.f dsbgv.f  dsbgvd.f dsbgvx.f
   dsbtrd.f dspcon.f dspev.f  dspevd.f dspevx.f dspgst.f
   dspgv.f  dspgvd.f dspgvx.f dsprfs.f dspsv.f  dspsvx.f dsptrd.f
   dsptrf.f dsptri.f dsptrs.f dstegr.f dstein.f dstev.f  dstevd.f dstevr.f
   dstevx.f dsycon.f dsyev.f  dsyevd.f dsyevr.f
   dsyevx.f dsygs2.f dsygst.f dsygv.f  dsygvd.f dsygvx.f dsyrfs.f
   dsysv.f  dsysvx.f
   dsytd2.f dsytf2.f dsytrd.f dsytrf.f dsytri.f dsytrs.f dsytrs2.f
   dsytri2.f dsytri2x.f dsyswapr.f
   dsyconv.f dsyconvf.f dsyconvf_rook.f
   dsytf2_rook.f dsytrf_rook.f dsytrs_rook.f
   dsytri_rook.f dsycon_rook.f dsysv_rook.f
   dsytf2_rk.f dsytrf_rk.f dsytrs_3.f
   dsytri_3.f dsytri_3x.f dsycon_3.f dsysv_rk.f
   dsysv_aa.f dsytrf_aa.f dsytrs_aa.f
   dsysv_aa_2stage.f dsytrf_aa_2stage.f dsytrs_aa_2stage.f
   dtbcon.f
   dtbrfs.f dtbtrs.f dtgevc.f dtgex2.f dtgexc.f dtgsen.f
   dtgsja.f dtgsna.f dtgsy2.f dtgsyl.f dtpcon.f dtprfs.f dtptri.f
   dtptrs.f
   dtrcon.f dtrevc.f dtrevc3.f dtrexc.f dtrrfs.f dtrsen.f dtrsna.f dtrsyl.f
   dtrti2.f dtrtri.f dtrtrs.f dtzrzf.f dstemr.f
   dsgesv.f dsposv.f dlag2s.f slag2d.f dlat2s.f
   dlansf.f dpftrf.f dpftri.f dpftrs.f dsfrk.f dtfsm.f dtftri.f dtfttp.f
   dtfttr.f dtpttf.f dtpttr.f dtrttf.f dtrttp.f
   dgejsv.f dgesvj.f dgsvj0.f dgsvj1.f
   dgeequb.f dsyequb.f dpoequb.f dgbequb.f
   dbbcsd.f dlapmr.f dorbdb.f dorbdb1.f dorbdb2.f dorbdb3.f dorbdb4.f
   dorbdb5.f dorbdb6.f dorcsd.f dorcsd2by1.f
   dgeqrt.f dgeqrt2.f dgeqrt3.f dgemqrt.f
   dtpqrt.f dtpqrt2.f dtpmqrt.f dtprfb.f
   dgelqt.f dgelqt3.f dgemlqt.f
   dgetsls.f dgeqr.f dlatsqr.f dlamtsqr.f dgemqr.f
   dgelq.f dlaswlq.f dlamswlq.f dgemlq.f
   dtplqt.f dtplqt2.f dtpmlqt.f
   dsytrd_2stage.f dsytrd_sy2sb.f dsytrd_sb2st.F dsb2st_kernels.f
   dsyevd_2stage.f dsyev_2stage.f dsyevx_2stage.f dsyevr_2stage.f
   dsbev_2stage.f dsbevx_2stage.f dsbevd_2stage.f dsygv_2stage.f)

set(DXLASRC dgesvxx.f dgerfsx.f dla_gerfsx_extended.f dla_geamv.f
   dla_gercond.f dla_gerpvgrw.f dsysvxx.f dsyrfsx.f
   dla_syrfsx_extended.f dla_syamv.f dla_syrcond.f dla_syrpvgrw.f
   dposvxx.f dporfsx.f dla_porfsx_extended.f dla_porcond.f
   dla_porpvgrw.f dgbsvxx.f dgbrfsx.f dla_gbrfsx_extended.f
   dla_gbamv.f dla_gbrcond.f dla_gbrpvgrw.f dla_lin_berr.f dlarscl2.f
   dlascl2.f dla_wwaddw.f)

set(ZLASRC
   zbdsqr.f zgbbrd.f zgbcon.f zgbequ.f zgbrfs.f zgbsv.f  zgbsvx.f
   zgbtf2.f zgbtrf.f zgbtrs.f zgebak.f zgebal.f zgebd2.f zgebrd.f
   zgecon.f zgeequ.f zgees.f  zgeesx.f zgeev.f  zgeevx.f
   zgehd2.f zgehrd.f zgelq2.f zgelqf.f
   zgels.f  zgelsd.f zgelss.f zgelsy.f zgeql2.f zgeqlf.f zgeqp3.f
   zgeqr2.f zgeqr2p.f zgeqrf.f zgeqrfp.f zgerfs.f zgerq2.f zgerqf.f
   zgesc2.f zgesdd.f zgesv.f  zgesvd.f zgesvdx.f zgesvx.f
   zgesvj.f zgejsv.f zgsvj0.f zgsvj1.f
   zgetc2.f zgetf2.f zgetrf.f zgetrf2.f
   zgetri.f zgetrs.f
   zggbak.f zggbal.f
   zgges.f  zgges3.f zggesx.f zggev.f  zggev3.f zggevx.f
   zggglm.f zgghrd.f zgghd3.f zgglse.f zggqrf.f zggrqf.f
   zggsvd3.f zggsvp3.f
   zgtcon.f zgtrfs.f zgtsv.f  zgtsvx.f zgttrf.f zgttrs.f zgtts2.f zhbev.f
   zhbevd.f zhbevx.f zhbgst.f zhbgv.f  zhbgvd.f zhbgvx.f zhbtrd.f
   zhecon.f zheev.f  zheevd.f zheevr.f zheevx.f zhegs2.f zhegst.f
   zhegv.f  zhegvd.f zhegvx.f zherfs.f zhesv.f  zhesvx.f zhetd2.f
   zhetf2.f zhetrd.f
   zhetrf.f zhetri.f zhetri2.f zhetri2x.f zheswapr.f
   zhetrs.f zhetrs2.f
   zhetf2_rook.f zhetrf_rook.f zhetri_rook.f
   zhetrs_rook.f zhecon_rook.f zhesv_rook.f
   zhetf2_rk.f zhetrf_rk.f zhetri_3.f zhetri_3x.f
   zhetrs_3.f zhecon_3.f zhesv_rk.f
   zhesv_aa.f zhetrf_aa.f zhetrs_aa.f
   zhesv_aa_2stage.f zhetrf_aa_2stage.f zhetrs_aa_2stage.f
   zhgeqz.f zhpcon.f zhpev.f  zhpevd.f
   zhpevx.f zhpgst.f zhpgv.f  zhpgvd.f zhpgvx.f zhprfs.f zhpsv.f
   zhpsvx.f
   zhptrd.f zhptrf.f zhptri.f zhptrs.f zhsein.f zhseqr.f zlabrd.f
   zlacgv.f zlacon.f zlacn2.f zlacp2.f zlacpy.f zlacrm.f zlacrt.f zladiv.f
   zlaed0.f zlaed7.f zlaed8.f
   zlaein.f zlaesy.f zlaev2.f zlags2.f zlagtm.f
   zlahef.f zlahef_rook.f zlahef_rk.f zlahef_aa.f zlahqr.f
   zlahr2.f zlaic1.f zlals0.f zlalsa.f zlalsd.f zlangb.f zlange.f
   zlangt.f zlanhb.f
   zlanhe.f
   zlanhp.f zlanhs.f zlanht.f zlansb.f zlansp.f zlansy.f zlantb.f
   zlantp.f zlantr.f zlapll.f zlapmt.f zlaqgb.f zlaqge.f
   zlaqhb.f zlaqhe.f zlaqhp.f zlaqp2.f zlaqps.f zlaqsb.f
   zlaqr0.f zlaqr1.f zlaqr2.f zlaqr3.f zlaqr4.f zlaqr5.f
   zlaqsp.f zlaqsy.f zlar1v.f zlar2v.f ilazlr.f ilazlc.f
   zlarcm.f zlarf.f  zlarfb.f
   zlarfg.f zlarfgp.f zlarft.f
   zlarfx.f zlarfy.f zlargv.f zlarnv.f zlarrv.f zlartg.f zlartv.f
   zlarz.f  zlarzb.f zlarzt.f zlascl.f zlaset.f zlasr.f
   zlassq.f zlaswp.f zlasyf.f zlasyf_rook.f zlasyf_rk.f zlasyf_aa.f
   zlatbs.f zlatdf.f zlatps.f zlatrd.f zlatrs.f zlatrz.f zlauu2.f
   zlauum.f zpbcon.f zpbequ.f zpbrfs.f zpbstf.f zpbsv.f
   zpbsvx.f zpbtf2.f zpbtrf.f zpbtrs.f zpocon.f zpoequ.f zporfs.f
   zposv.f  zposvx.f zpotf2.f zpotrf.f zpotrf2.f zpotri.f zpotrs.f zpstrf.f zpstf2.f
   zppcon.f zppequ.f zpprfs.f zppsv.f  zppsvx.f zpptrf.f zpptri.f zpptrs.f
   zptcon.f zpteqr.f zptrfs.f zptsv.f  zptsvx.f zpttrf.f zpttrs.f zptts2.f
   zrot.f   zspcon.f zspmv.f  zspr.f   zsprfs.f zspsv.f
   zspsvx.f zsptrf.f zsptri.f zsptrs.f zdrscl.f zstedc.f
   zstegr.f zstein.f zsteqr.f zsycon.f zsymv.f
   zsyr.f   zsyrfs.f zsysv.f  zsysvx.f zsytf2.f zsytrf.f zsytri.f
   zsytri2.f zsytri2x.f zsyswapr.f
   zsytrs.f zsytrs2.f
   zsyconv.f zsyconvf.f zsyconvf_rook.f
   zsytf2_rook.f zsytrf_rook.f zsytrs_rook.f zsytrs_aa.f
   zsytri_rook.f zsycon_rook.f zsysv_rook.f
   zsytf2_rk.f zsytrf_rk.f zsytrf_aa.f zsytrs_3.f
   zsysv_aa_2stage.f zsytrf_aa_2stage.f zsytrs_aa_2stage.f
   zsytri_3.f zsytri_3x.f zsycon_3.f zsysv_rk.f zsysv_aa.f
   ztbcon.f ztbrfs.f ztbtrs.f ztgevc.f ztgex2.f
   ztgexc.f ztgsen.f ztgsja.f ztgsna.f ztgsy2.f ztgsyl.f ztpcon.f
   ztprfs.f ztptri.f
   ztptrs.f ztrcon.f ztrevc.f ztrevc3.f ztrexc.f ztrrfs.f ztrsen.f ztrsna.f
   ztrsyl.f ztrti2.f ztrtri.f ztrtrs.f ztzrzf.f zung2l.f
   zung2r.f zungbr.f zunghr.f zungl2.f zunglq.f zungql.f zungqr.f zungr2.f
   zungrq.f zungtr.f zunm2l.f zunm2r.f zunmbr.f zunmhr.f zunml2.f zunm22.f
   zunmlq.f zunmql.f zunmqr.f zunmr2.f zunmr3.f zunmrq.f zunmrz.f
   zunmtr.f zupgtr.f
   zupmtr.f izmax1.f dzsum1.f zstemr.f
   zcgesv.f zcposv.f zlag2c.f clag2z.f zlat2c.f
   zhfrk.f ztfttp.f zlanhf.f zpftrf.f zpftri.f zpftrs.f ztfsm.f ztftri.f
   ztfttr.f ztpttf.f ztpttr.f ztrttf.f ztrttp.f
   zgeequb.f zgbequb.f zsyequb.f zpoequb.f zheequb.f
   zbbcsd.f zlapmr.f zunbdb.f zunbdb1.f zunbdb2.f zunbdb3.f zunbdb4.f
   zunbdb5.f zunbdb6.f zuncsd.f zuncsd2by1.f
   zgeqrt.f zgeqrt2.f zgeqrt3.f zgemqrt.f
   ztpqrt.f ztpqrt2.f ztpmqrt.f ztprfb.f
   ztplqt.f ztplqt2.f ztpmlqt.f
   zgelqt.f zgelqt3.f zgemlqt.f
   zgetsls.f zgeqr.f zlatsqr.f zlamtsqr.f zgemqr.f
   zgelq.f zlaswlq.f zlamswlq.f zgemlq.f
   zhetrd_2stage.f zhetrd_he2hb.f zhetrd_hb2st.F zhb2st_kernels.f
   zheevd_2stage.f zheev_2stage.f zheevx_2stage.f zheevr_2stage.f
   zhbev_2stage.f zhbevx_2stage.f zhbevd_2stage.f zhegv_2stage.f)

set(ZXLASRC zgesvxx.f zgerfsx.f zla_gerfsx_extended.f zla_geamv.f
   zla_gercond_c.f zla_gercond_x.f zla_gerpvgrw.f zsysvxx.f zsyrfsx.f
   zla_syrfsx_extended.f zla_syamv.f zla_syrcond_c.f zla_syrcond_x.f
   zla_syrpvgrw.f zposvxx.f zporfsx.f zla_porfsx_extended.f
   zla_porcond_c.f zla_porcond_x.f zla_porpvgrw.f zgbsvxx.f zgbrfsx.f
   zla_gbrfsx_extended.f zla_gbamv.f zla_gbrcond_c.f zla_gbrcond_x.f
   zla_gbrpvgrw.f zhesvxx.f zherfsx.f zla_herfsx_extended.f
   zla_heamv.f zla_hercond_c.f zla_hercond_x.f zla_herpvgrw.f
   zla_lin_berr.f zlarscl2.f zlascl2.f zla_wwaddw.f)

if(BUILD_DEPRECATED)
  list(APPEND SLASRC DEPRECATED/sgegs.f DEPRECATED/sgegv.f
    DEPRECATED/sgeqpf.f DEPRECATED/sgelsx.f DEPRECATED/sggsvd.f
    DEPRECATED/sggsvp.f DEPRECATED/slahrd.f DEPRECATED/slatzm.f DEPRECATED/stzrqf.f)
  list(APPEND DLASRC DEPRECATED/dgegs.f DEPRECATED/dgegv.f
    DEPRECATED/dgeqpf.f DEPRECATED/dgelsx.f DEPRECATED/dggsvd.f
    DEPRECATED/dggsvp.f DEPRECATED/dlahrd.f DEPRECATED/dlatzm.f DEPRECATED/dtzrqf.f)
  list(APPEND CLASRC DEPRECATED/cgegs.f DEPRECATED/cgegv.f
    DEPRECATED/cgeqpf.f DEPRECATED/cgelsx.f DEPRECATED/cggsvd.f
    DEPRECATED/cggsvp.f DEPRECATED/clahrd.f DEPRECATED/clatzm.f DEPRECATED/ctzrqf.f)
  list(APPEND ZLASRC DEPRECATED/zgegs.f DEPRECATED/zgegv.f
    DEPRECATED/zgeqpf.f DEPRECATED/zgelsx.f DEPRECATED/zggsvd.f
    DEPRECATED/zggsvp.f DEPRECATED/zlahrd.f DEPRECATED/zlatzm.f DEPRECATED/ztzrqf.f)
endif()

if(USE_XBLAS)
  list(APPEND SLASRC ${SXLASRC})
  list(APPEND DLASRC ${DXLASRC})
  list(APPEND CLASRC ${CXLASRC})
  list(APPEND ZLASRC ${ZXLASRC})
endif()


set(SOURCES)
if(BUILD_SINGLE)
  list(APPEND SOURCES ${SLASRC} ${DSLASRC} ${SCLAUX} ${ALLAUX})
endif()
if(BUILD_DOUBLE)
  list(APPEND SOURCES ${DLASRC} ${DSLASRC} ${DZLAUX} ${ALLAUX})
endif()
if(BUILD_COMPLEX)
  list(APPEND SOURCES ${CLASRC} ${ZCLASRC} ${SCLAUX} ${ALLAUX})
endif()
if(BUILD_COMPLEX16)
  list(APPEND SOURCES ${ZLASRC} ${ZCLASRC} ${DZLAUX} ${ALLAUX})
endif()
list(REMOVE_DUPLICATES SOURCES)

add_library(lapack ${SOURCES})
set_target_properties(
  lapack PROPERTIES
  VERSION ${LAPACK_VERSION}
  SOVERSION ${LAPACK_MAJOR_VERSION}
  )

if(USE_XBLAS)
  target_link_libraries(lapack PRIVATE ${XBLAS_LIBRARY})
endif()
target_link_libraries(lapack PRIVATE ${BLAS_LIBRARIES})

if (${CMAKE_BUILD_TYPE_UPPER} STREQUAL "COVERAGE")
  target_link_libraries(lapack PRIVATE gcov)
  add_coverage(lapack)
endif()

lapack_install_library(lapack)
