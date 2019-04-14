########################################################################
#  This is the makefile for the eigenvalue test program from LAPACK.
#  The test files are organized as follows:
#
#     AEIGTST -- Auxiliary test routines used in all precisions
#     SCIGTST -- Auxiliary test routines used in single precision
#     DZIGTST -- Auxiliary test routines used in double precision
#     SEIGTST -- Single precision real test routines
#     CEIGTST -- Single precision complex test routines
#     DEIGTST -- Double precision real test routines
#     ZEIGTST -- Double precision complex test routines
#
########################################################################

set(AEIGTST
   alahdg.f
   alasum.f
   alasvm.f
   alareq.f
   ilaenv.f
   xerbla.f
   xlaenv.f
   chkxer.f)

set(SCIGTST slafts.f slahd2.f slasum.f slatb9.f sstech.f sstect.f
   ssvdch.f ssvdct.f ssxt1.f)

set(SEIGTST schkee.f
   sbdt01.f sbdt02.f sbdt03.f sbdt04.f sbdt05.f
   schkbb.f schkbd.f schkbk.f schkbl.f schkec.f
   schkgg.f schkgk.f schkgl.f schkhs.f schksb.f schkst.f schkst2stg.f schksb2stg.f
   sckcsd.f sckglm.f sckgqr.f sckgsv.f scklse.f scsdts.f
   sdrges.f sdrgev.f sdrges3.f sdrgev3.f sdrgsx.f sdrgvx.f
   sdrvbd.f sdrves.f sdrvev.f sdrvsg.f sdrvsg2stg.f
   sdrvst.f sdrvst2stg.f sdrvsx.f sdrvvx.f
   serrbd.f serrec.f serred.f serrgg.f serrhs.f serrst.f
   sget02.f sget10.f sget22.f sget23.f sget24.f sget31.f
   sget32.f sget33.f sget34.f sget35.f sget36.f
   sget37.f sget38.f sget39.f sget51.f sget52.f sget53.f
   sget54.f sglmts.f sgqrts.f sgrqts.f sgsvts3.f
   shst01.f slarfy.f slarhs.f slatm4.f slctes.f slctsx.f slsets.f sort01.f
   sort03.f ssbt21.f ssgt01.f sslect.f sspt21.f sstt21.f
   sstt22.f ssyt21.f ssyt22.f)

set(CEIGTST cchkee.f
   cbdt01.f cbdt02.f cbdt03.f cbdt05.f
   cchkbb.f cchkbd.f cchkbk.f cchkbl.f cchkec.f
   cchkgg.f cchkgk.f cchkgl.f cchkhb.f cchkhs.f cchkst.f cchkst2stg.f cchkhb2stg.f
   cckcsd.f cckglm.f cckgqr.f cckgsv.f ccklse.f ccsdts.f
   cdrges.f cdrgev.f cdrges3.f cdrgev3.f cdrgsx.f cdrgvx.f
   cdrvbd.f cdrves.f cdrvev.f cdrvsg.f cdrvsg2stg.f
   cdrvst.f cdrvst2stg.f cdrvsx.f cdrvvx.f
   cerrbd.f cerrec.f cerred.f cerrgg.f cerrhs.f cerrst.f
   cget02.f cget10.f cget22.f cget23.f cget24.f
   cget35.f cget36.f cget37.f cget38.f cget51.f cget52.f
   cget54.f cglmts.f cgqrts.f cgrqts.f cgsvts3.f
   chbt21.f chet21.f chet22.f chpt21.f chst01.f
   clarfy.f clarhs.f clatm4.f clctes.f clctsx.f clsets.f csbmv.f
   csgt01.f cslect.f
   cstt21.f cstt22.f cunt01.f cunt03.f)

set(DZIGTST dlafts.f dlahd2.f dlasum.f dlatb9.f dstech.f dstect.f
   dsvdch.f dsvdct.f dsxt1.f)

set(DEIGTST dchkee.f
   dbdt01.f dbdt02.f dbdt03.f dbdt04.f dbdt05.f
   dchkbb.f dchkbd.f dchkbk.f dchkbl.f dchkec.f
   dchkgg.f dchkgk.f dchkgl.f dchkhs.f dchksb.f dchkst.f dchkst2stg.f dchksb2stg.f
   dckcsd.f dckglm.f dckgqr.f dckgsv.f dcklse.f dcsdts.f
   ddrges.f ddrgev.f ddrges3.f ddrgev3.f ddrgsx.f ddrgvx.f
   ddrvbd.f ddrves.f ddrvev.f ddrvsg.f ddrvsg2stg.f
   ddrvst.f ddrvst2stg.f ddrvsx.f ddrvvx.f
   derrbd.f derrec.f derred.f derrgg.f derrhs.f derrst.f
   dget02.f dget10.f dget22.f dget23.f dget24.f dget31.f
   dget32.f dget33.f dget34.f dget35.f dget36.f
   dget37.f dget38.f dget39.f dget51.f dget52.f dget53.f
   dget54.f dglmts.f dgqrts.f dgrqts.f dgsvts3.f
   dhst01.f dlarfy.f dlarhs.f dlatm4.f dlctes.f dlctsx.f dlsets.f dort01.f
   dort03.f dsbt21.f dsgt01.f dslect.f dspt21.f dstt21.f
   dstt22.f dsyt21.f dsyt22.f)

set(ZEIGTST zchkee.f
   zbdt01.f zbdt02.f zbdt03.f zbdt05.f
   zchkbb.f zchkbd.f zchkbk.f zchkbl.f zchkec.f
   zchkgg.f zchkgk.f zchkgl.f zchkhb.f zchkhs.f zchkst.f zchkst2stg.f zchkhb2stg.f
   zckcsd.f zckglm.f zckgqr.f zckgsv.f zcklse.f zcsdts.f
   zdrges.f zdrgev.f zdrges3.f zdrgev3.f zdrgsx.f zdrgvx.f
   zdrvbd.f zdrves.f zdrvev.f zdrvsg.f zdrvsg2stg.f
   zdrvst.f zdrvst2stg.f zdrvsx.f zdrvvx.f
   zerrbd.f zerrec.f zerred.f zerrgg.f zerrhs.f zerrst.f
   zget02.f zget10.f zget22.f zget23.f zget24.f
   zget35.f zget36.f zget37.f zget38.f zget51.f zget52.f
   zget54.f zglmts.f zgqrts.f zgrqts.f zgsvts3.f
   zhbt21.f zhet21.f zhet22.f zhpt21.f zhst01.f
   zlarfy.f zlarhs.f zlatm4.f zlctes.f zlctsx.f zlsets.f zsbmv.f
   zsgt01.f zslect.f
   zstt21.f zstt22.f zunt01.f zunt03.f)

macro(add_eig_executable name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} tmglib ${LAPACK_LIBRARIES} ${BLAS_LIBRARIES})
endmacro()

if(BUILD_SINGLE)
add_eig_executable(xeigtsts ${SEIGTST} ${SCIGTST} ${AEIGTST})
endif()

if(BUILD_COMPLEX)
add_eig_executable(xeigtstc ${CEIGTST} ${SCIGTST} ${AEIGTST})
endif()

if(BUILD_DOUBLE)
add_eig_executable(xeigtstd ${DEIGTST} ${DZIGTST} ${AEIGTST})
endif()

if(BUILD_COMPLEX16)
add_eig_executable(xeigtstz ${ZEIGTST} ${DZIGTST} ${AEIGTST})
endif()
