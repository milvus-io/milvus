include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

set(RELAFILES
clauum.c
ctrsyl_rec2.c
dsytrf.c
spbtrf.c
strsyl_rec2.c
zhetrf_rook_rec2.c
ztrsyl.c
cgbtrf.c
cpbtrf.c
ctrtri.c
dsytrf_rec2.c
spotrf.c
strtri.c
zlauum.c
ztrsyl_rec2.c
cgemmt.c
cpotrf.c
dgbtrf.c
dsytrf_rook.c
lapack_wrappers.c
ssygst.c
zgbtrf.c
zpbtrf.c
ztrtri.c
cgetrf.c
csytrf.c
dgemmt.c
dsytrf_rook_rec2.c
ssytrf.c
zgemmt.c
zpotrf.c
chegst.c
csytrf_rec2.c
dgetrf.c
dtgsyl.c
ssytrf_rec2.c
zgetrf.c
zsytrf.c
chetrf.c
csytrf_rook.c
dlauum.c
dtrsyl.c
sgbtrf.c
ssytrf_rook.c
zhegst.c
zsytrf_rec2.c
chetrf_rec2.c
csytrf_rook_rec2.c
dpbtrf.c
dtrsyl_rec2.c
sgemmt.c
ssytrf_rook_rec2.c
zhetrf.c
zsytrf_rook.c
chetrf_rook.c
ctgsyl.c
dpotrf.c
dtrtri.c
sgetrf.c
stgsyl.c
zhetrf_rec2.c
zsytrf_rook_rec2.c
chetrf_rook_rec2.c
ctrsyl.c
dsygst.c
f2c.c
slauum.c
strsyl.c
zhetrf_rook.c
ztgsyl.c
)



# add relapack folder to the sources
set(RELA_SOURCES "")
foreach (RELA_FILE ${RELAFILES})
  list(APPEND RELA_SOURCES "${PROJECT_SOURCE_DIR}/relapack/src/${RELA_FILE}")
endforeach ()
add_library(relapack_src OBJECT ${RELA_SOURCES})
set_source_files_properties(${RELA_SOURCES} PROPERTIES COMPILE_FLAGS "${LAPACK_CFLAGS}")
