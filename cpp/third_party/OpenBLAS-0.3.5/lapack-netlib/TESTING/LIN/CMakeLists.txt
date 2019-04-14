set(ALINTST
   aladhd.f alaerh.f alaesm.f alahd.f alareq.f
   alasum.f alasvm.f chkxer.f icopy.f ilaenv.f xlaenv.f xerbla.f)

set(SCLNTST slaord.f)

set(DZLNTST dlaord.f)

set(SLINTST schkaa.f
   schkeq.f schkgb.f schkge.f schkgt.f
   schklq.f schkpb.f schkpo.f schkps.f schkpp.f
   schkpt.f schkq3.f schkql.f schkqr.f schkrq.f
   schksp.f schksy.f schksy_rook.f schksy_rk.f 
   schksy_aa.f schksy_aa_2stage.f 
   schktb.f schktp.f schktr.f
   schktz.f
   sdrvgt.f sdrvls.f sdrvpb.f
   sdrvpp.f sdrvpt.f sdrvsp.f sdrvsy_rook.f sdrvsy_rk.f 
   sdrvsy_aa.f sdrvsy_aa_2stage.f
   serrgt.f serrlq.f serrls.f
   serrps.f serrql.f serrqp.f serrqr.f
   serrrq.f serrtr.f serrtz.f
   sgbt01.f sgbt02.f sgbt05.f sgelqs.f sgeqls.f sgeqrs.f
   sgerqs.f sget01.f sget02.f
   sget03.f sget04.f sget06.f sget07.f sgtt01.f sgtt02.f
   sgtt05.f slaptm.f slarhs.f slatb4.f slatb5.f slattb.f slattp.f
   slattr.f slavsp.f slavsy.f slavsy_rook.f slqt01.f slqt02.f
   slqt03.f spbt01.f spbt02.f spbt05.f spot01.f
   spot02.f spot03.f spot05.f spst01.f sppt01.f
   sppt02.f sppt03.f sppt05.f sptt01.f sptt02.f
   sptt05.f sqlt01.f sqlt02.f sqlt03.f sqpt01.f
   sqrt01.f sqrt01p.f sqrt02.f sqrt03.f sqrt11.f sqrt12.f
   sqrt13.f sqrt14.f sqrt15.f sqrt16.f sqrt17.f
   srqt01.f srqt02.f srqt03.f srzt01.f srzt02.f
   sspt01.f ssyt01.f ssyt01_rook.f ssyt01_3.f 
   ssyt01_aa.f
   stbt02.f stbt03.f stbt05.f stbt06.f stpt01.f
   stpt02.f stpt03.f stpt05.f stpt06.f strt01.f
   strt02.f strt03.f strt05.f strt06.f
   sgennd.f sqrt04.f sqrt05.f schkqrt.f serrqrt.f schkqrtp.f serrqrtp.f
   schklqt.f schklqtp.f schktsqr.f
   serrlqt.f serrlqtp.f serrtsqr.f stsqr01.f slqt04.f slqt05.f)

if(USE_XBLAS)
  list(APPEND SLINTST sdrvgbx.f sdrvgex.f sdrvsyx.f sdrvpox.f
                      serrvxx.f serrgex.f serrsyx.f serrpox.f
                      sebchvxx.f)
else()
  list(APPEND SLINTST sdrvgb.f sdrvge.f sdrvsy.f sdrvpo.f
                      serrvx.f serrge.f serrsy.f serrpo.f)
endif()

set(CLINTST cchkaa.f
   cchkeq.f cchkgb.f cchkge.f cchkgt.f
   cchkhe.f cchkhe_rook.f cchkhe_rk.f 
   cchkhe_aa.f cchkhe_aa_2stage.f
   cchkhp.f cchklq.f cchkpb.f
   cchkpo.f cchkps.f cchkpp.f cchkpt.f cchkq3.f cchkql.f
   cchkqr.f cchkrq.f cchksp.f cchksy.f cchksy_rook.f cchksy_rk.f
   cchksy_aa.f cchksy_aa_2stage.f
   cchktb.f
   cchktp.f cchktr.f cchktz.f
   cdrvgt.f cdrvhe_rook.f cdrvhe_rk.f 
   cdrvhe_aa.f cdrvhe_aa_2stage.f cdrvsy_aa_2stage.f
   cdrvhp.f
   cdrvls.f cdrvpb.f cdrvpp.f cdrvpt.f
   cdrvsp.f cdrvsy_rook.f cdrvsy_rk.f 
   cdrvsy_aa.f 
   cerrgt.f cerrlq.f
   cerrls.f cerrps.f cerrql.f cerrqp.f
   cerrqr.f cerrrq.f cerrtr.f cerrtz.f
   cgbt01.f cgbt02.f cgbt05.f cgelqs.f cgeqls.f cgeqrs.f
   cgerqs.f cget01.f cget02.f
   cget03.f cget04.f cget07.f cgtt01.f cgtt02.f
   cgtt05.f chet01.f chet01_rook.f chet01_3.f
   chet01_aa.f
   chpt01.f claipd.f claptm.f clarhs.f clatb4.f clatb5.f
   clatsp.f clatsy.f clattb.f clattp.f clattr.f
   clavhe.f clavhe_rook.f clavhp.f clavsp.f clavsy.f clavsy_rook.f clqt01.f
   clqt02.f clqt03.f cpbt01.f cpbt02.f cpbt05.f
   cpot01.f cpot02.f cpot03.f cpot05.f cpst01.f
   cppt01.f cppt02.f cppt03.f cppt05.f cptt01.f
   cptt02.f cptt05.f cqlt01.f cqlt02.f cqlt03.f
   cqpt01.f cqrt01.f cqrt01p.f cqrt02.f cqrt03.f cqrt11.f
   cqrt12.f cqrt13.f cqrt14.f cqrt15.f cqrt16.f
   cqrt17.f crqt01.f crqt02.f crqt03.f crzt01.f crzt02.f
   csbmv.f  cspt01.f
   cspt02.f cspt03.f csyt01.f csyt01_rook.f csyt01_3.f
   csyt01_aa.f 
   csyt02.f csyt03.f
   ctbt02.f ctbt03.f ctbt05.f ctbt06.f ctpt01.f
   ctpt02.f ctpt03.f ctpt05.f ctpt06.f ctrt01.f
   ctrt02.f ctrt03.f ctrt05.f ctrt06.f
   sget06.f cgennd.f
   cqrt04.f cqrt05.f cchkqrt.f cerrqrt.f cchkqrtp.f cerrqrtp.f
   cchklqt.f cchklqtp.f cchktsqr.f
   cerrlqt.f cerrlqtp.f cerrtsqr.f ctsqr01.f clqt04.f clqt05.f)

if(USE_XBLAS)
  list(APPEND CLINTST cdrvgbx.f cdrvgex.f cdrvhex.f cdrvsyx.f cdrvpox.f
                      cerrvxx.f cerrgex.f cerrhex.f cerrsyx.f cerrpox.f
                      cebchvxx.f)
else()
  list(APPEND CLINTST cdrvgb.f cdrvge.f cdrvhe.f cdrvsy.f cdrvpo.f
                      cerrvx.f cerrge.f cerrhe.f cerrsy.f cerrpo.f)
endif()

set(DLINTST dchkaa.f
   dchkeq.f dchkgb.f dchkge.f dchkgt.f
   dchklq.f dchkpb.f dchkpo.f dchkps.f dchkpp.f
   dchkpt.f dchkq3.f dchkql.f dchkqr.f dchkrq.f
   dchksp.f dchksy.f dchksy_rook.f dchksy_rk.f 
   dchksy_aa.f dchksy_aa_2stage.f
   dchktb.f dchktp.f dchktr.f
   dchktz.f
   ddrvgt.f ddrvls.f ddrvpb.f
   ddrvpp.f ddrvpt.f ddrvsp.f ddrvsy_rook.f ddrvsy_rk.f 
   ddrvsy_aa.f ddrvsy_aa_2stage.f
   derrgt.f derrlq.f derrls.f
   derrps.f derrql.f derrqp.f derrqr.f
   derrrq.f derrtr.f derrtz.f
   dgbt01.f dgbt02.f dgbt05.f dgelqs.f dgeqls.f dgeqrs.f
   dgerqs.f dget01.f dget02.f
   dget03.f dget04.f dget06.f dget07.f dgtt01.f dgtt02.f
   dgtt05.f dlaptm.f dlarhs.f dlatb4.f dlatb5.f dlattb.f dlattp.f
   dlattr.f dlavsp.f dlavsy.f dlavsy_rook.f dlqt01.f dlqt02.f
   dlqt03.f dpbt01.f dpbt02.f dpbt05.f dpot01.f
   dpot02.f dpot03.f dpot05.f dpst01.f dppt01.f
   dppt02.f dppt03.f dppt05.f dptt01.f dptt02.f
   dptt05.f dqlt01.f dqlt02.f dqlt03.f dqpt01.f
   dqrt01.f dqrt01p.f dqrt02.f dqrt03.f dqrt11.f dqrt12.f
   dqrt13.f dqrt14.f dqrt15.f dqrt16.f dqrt17.f
   drqt01.f drqt02.f drqt03.f drzt01.f drzt02.f
   dspt01.f dsyt01.f dsyt01_rook.f dsyt01_3.f
   dsyt01_aa.f
   dtbt02.f dtbt03.f dtbt05.f dtbt06.f dtpt01.f
   dtpt02.f dtpt03.f dtpt05.f dtpt06.f dtrt01.f
   dtrt02.f dtrt03.f dtrt05.f dtrt06.f
   dgennd.f
   dqrt04.f dqrt05.f dchkqrt.f derrqrt.f dchkqrtp.f derrqrtp.f
   dchklq.f dchklqt.f dchklqtp.f dchktsqr.f
   derrlqt.f derrlqtp.f derrtsqr.f dtsqr01.f dlqt04.f dlqt05.f)

if(USE_XBLAS)
  list(APPEND DLINTST ddrvgbx.f ddrvgex.f ddrvsyx.f ddrvpox.f
                      derrvxx.f derrgex.f derrsyx.f derrpox.f
                      debchvxx.f)
else()
  list(APPEND DLINTST ddrvgb.f ddrvge.f ddrvsy.f ddrvpo.f
                      derrvx.f derrge.f derrsy.f derrpo.f)
endif()

set(ZLINTST zchkaa.f
   zchkeq.f zchkgb.f zchkge.f zchkgt.f
   zchkhe.f zchkhe_rook.f zchkhe_rk.f 
   zchkhe_aa.f zchkhe_aa_2stage.f
   zchkhp.f zchklq.f zchkpb.f
   zchkpo.f zchkps.f zchkpp.f zchkpt.f zchkq3.f zchkql.f
   zchkqr.f zchkrq.f zchksp.f zchksy.f zchksy_rook.f zchksy_rk.f
   zchksy_aa.f  zchksy_aa_2stage.f
   zchktb.f
   zchktp.f zchktr.f zchktz.f
   zdrvgt.f zdrvhe_rook.f zdrvhe_rk.f 
   zdrvhe_aa.f  zdrvhe_aa_2stage.f
   zdrvhp.f
   zdrvls.f zdrvpb.f zdrvpp.f zdrvpt.f
   zdrvsp.f zdrvsy_rook.f zdrvsy_rk.f 
   zdrvsy_aa.f zdrvsy_aa_2stage.f 
   zerrgt.f zerrlq.f
   zerrls.f zerrps.f zerrql.f zerrqp.f
   zerrqr.f zerrrq.f zerrtr.f zerrtz.f
   zgbt01.f zgbt02.f zgbt05.f zgelqs.f zgeqls.f zgeqrs.f
   zgerqs.f zget01.f zget02.f
   zget03.f zget04.f zget07.f zgtt01.f zgtt02.f
   zgtt05.f zhet01.f zhet01_rook.f zhet01_3.f
   zhet01_aa.f
   zhpt01.f zlaipd.f zlaptm.f zlarhs.f zlatb4.f zlatb5.f
   zlatsp.f zlatsy.f zlattb.f zlattp.f zlattr.f
   zlavhe.f zlavhe_rook.f zlavhp.f zlavsp.f zlavsy.f zlavsy_rook.f zlqt01.f
   zlqt02.f zlqt03.f zpbt01.f zpbt02.f zpbt05.f
   zpot01.f zpot02.f zpot03.f zpot05.f zpst01.f
   zppt01.f zppt02.f zppt03.f zppt05.f zptt01.f
   zptt02.f zptt05.f zqlt01.f zqlt02.f zqlt03.f
   zqpt01.f zqrt01.f zqrt01p.f zqrt02.f zqrt03.f zqrt11.f
   zqrt12.f zqrt13.f zqrt14.f zqrt15.f zqrt16.f
   zqrt17.f zrqt01.f zrqt02.f zrqt03.f zrzt01.f zrzt02.f
   zsbmv.f  zspt01.f
   zspt02.f zspt03.f zsyt01.f zsyt01_rook.f zsyt01_3.f
   zsyt01_aa.f
   zsyt02.f zsyt03.f
   ztbt02.f ztbt03.f ztbt05.f ztbt06.f ztpt01.f
   ztpt02.f ztpt03.f ztpt05.f ztpt06.f ztrt01.f
   ztrt02.f ztrt03.f ztrt05.f ztrt06.f
   dget06.f zgennd.f
   zqrt04.f zqrt05.f zchkqrt.f zerrqrt.f zchkqrtp.f zerrqrtp.f
   zchklqt.f zchklqtp.f zchktsqr.f
   zerrlqt.f zerrlqtp.f zerrtsqr.f ztsqr01.f zlqt04.f zlqt05.f)

if(USE_XBLAS)
  list(APPEND ZLINTST zdrvgbx.f zdrvgex.f zdrvhex.f zdrvsyx.f zdrvpox.f
                      zerrvxx.f zerrgex.f zerrhex.f zerrsyx.f zerrpox.f
                      zebchvxx.f)
else()
  list(APPEND ZLINTST zdrvgb.f zdrvge.f zdrvhe.f zdrvsy.f zdrvpo.f
                      zerrvx.f zerrge.f zerrhe.f zerrsy.f zerrpo.f)
endif()

set(DSLINTST dchkab.f
	ddrvab.f ddrvac.f derrab.f derrac.f dget08.f
	alaerh.f alahd.f  aladhd.f alareq.f
	chkxer.f dlarhs.f dlatb4.f xerbla.f
	dget02.f dpot06.f)

set(ZCLINTST zchkab.f
	zdrvab.f zdrvac.f zerrab.f zerrac.f zget08.f
	alaerh.f alahd.f  aladhd.f alareq.f
	chkxer.f zget02.f zlarhs.f zlatb4.f
	zsbmv.f  xerbla.f zpot06.f zlaipd.f)

set(SLINTSTRFP schkrfp.f sdrvrfp.f sdrvrf1.f sdrvrf2.f sdrvrf3.f sdrvrf4.f serrrfp.f
	slatb4.f slarhs.f sget04.f spot01.f spot03.f spot02.f
	chkxer.f xerbla.f alaerh.f aladhd.f alahd.f alasvm.f)

set(DLINTSTRFP dchkrfp.f ddrvrfp.f ddrvrf1.f ddrvrf2.f ddrvrf3.f ddrvrf4.f derrrfp.f
	dlatb4.f dlarhs.f dget04.f dpot01.f dpot03.f dpot02.f
	chkxer.f xerbla.f alaerh.f aladhd.f alahd.f alasvm.f)

set(CLINTSTRFP cchkrfp.f cdrvrfp.f cdrvrf1.f cdrvrf2.f cdrvrf3.f cdrvrf4.f cerrrfp.f
  claipd.f clatb4.f clarhs.f csbmv.f cget04.f cpot01.f cpot03.f cpot02.f
  chkxer.f xerbla.f alaerh.f aladhd.f alahd.f alasvm.f)

set(ZLINTSTRFP zchkrfp.f zdrvrfp.f zdrvrf1.f zdrvrf2.f zdrvrf3.f zdrvrf4.f zerrrfp.f
  zlatb4.f zlaipd.f zlarhs.f zsbmv.f zget04.f zpot01.f zpot03.f zpot02.f
  chkxer.f xerbla.f alaerh.f aladhd.f alahd.f alasvm.f)

macro(add_lin_executable name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} tmglib ${LAPACK_LIBRARIES} ${BLAS_LIBRARIES})
endmacro()

if(BUILD_SINGLE)
  add_lin_executable(xlintsts ${ALINTST} ${SLINTST} ${SCLNTST})
  add_lin_executable(xlintstrfs ${SLINTSTRFP})
endif()

if(BUILD_DOUBLE)
  add_lin_executable(xlintstd ${ALINTST} ${DLINTST} ${DZLNTST})
  add_lin_executable(xlintstrfd ${DLINTSTRFP})
endif()

if(BUILD_SINGLE AND BUILD_DOUBLE)
  add_lin_executable(xlintstds ${DSLINTST})
endif()

if(BUILD_COMPLEX)
  add_lin_executable(xlintstc ${ALINTST} ${CLINTST} ${SCLNTST})
  add_lin_executable(xlintstrfc ${CLINTSTRFP})
endif()

if(BUILD_COMPLEX16)
  add_lin_executable(xlintstz ${ALINTST} ${ZLINTST} ${DZLNTST})
  add_lin_executable(xlintstrfz ${ZLINTSTRFP})
endif()

if(BUILD_COMPLEX AND BUILD_COMPLEX16)
  add_lin_executable(xlintstzc ${ZCLINTST})
endif()
