#ifndef RELAPACK_H
#define RELAPACK_H

void RELAPACK_slauum(const char *, const int *, float *, const int *, int *);
void RELAPACK_dlauum(const char *, const int *, double *, const int *, int *);
void RELAPACK_clauum(const char *, const int *, float *, const int *, int *);
void RELAPACK_zlauum(const char *, const int *, double *, const int *, int *);

void RELAPACK_strtri(const char *, const char *, const int *, float *, const int *, int *);
void RELAPACK_dtrtri(const char *, const char *, const int *, double *, const int *, int *);
void RELAPACK_ctrtri(const char *, const char *, const int *, float *, const int *, int *);
void RELAPACK_ztrtri(const char *, const char *, const int *, double *, const int *, int *);

void RELAPACK_spotrf(const char *, const int *, float *, const int *, int *);
void RELAPACK_dpotrf(const char *, const int *, double *, const int *, int *);
void RELAPACK_cpotrf(const char *, const int *, float *, const int *, int *);
void RELAPACK_zpotrf(const char *, const int *, double *, const int *, int *);

void RELAPACK_spbtrf(const char *, const int *, const int *, float *, const int *, int *);
void RELAPACK_dpbtrf(const char *, const int *, const int *, double *, const int *, int *);
void RELAPACK_cpbtrf(const char *, const int *, const int *, float *, const int *, int *);
void RELAPACK_zpbtrf(const char *, const int *, const int *, double *, const int *, int *);

void RELAPACK_ssytrf(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void RELAPACK_dsytrf(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void RELAPACK_csytrf(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void RELAPACK_chetrf(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void RELAPACK_zsytrf(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void RELAPACK_zhetrf(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void RELAPACK_ssytrf_rook(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void RELAPACK_dsytrf_rook(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void RELAPACK_csytrf_rook(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void RELAPACK_chetrf_rook(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void RELAPACK_zsytrf_rook(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void RELAPACK_zhetrf_rook(const char *, const int *, double *, const int *, int *, double *, const int *, int *);

void RELAPACK_sgetrf(const int *, const int *, float *, const int *, int *, int *);
void RELAPACK_dgetrf(const int *, const int *, double *, const int *, int *, int *);
void RELAPACK_cgetrf(const int *, const int *, float *, const int *, int *, int *);
void RELAPACK_zgetrf(const int *, const int *, double *, const int *, int *, int *);

void RELAPACK_sgbtrf(const int *, const int *, const int *, const int *, float *, const int *, int *, int *);
void RELAPACK_dgbtrf(const int *, const int *, const int *, const int *, double *, const int *, int *, int *);
void RELAPACK_cgbtrf(const int *, const int *, const int *, const int *, float *, const int *, int *, int *);
void RELAPACK_zgbtrf(const int *, const int *, const int *, const int *, double *, const int *, int *, int *);

void RELAPACK_ssygst(const int *, const char *, const int *, float *, const int *, const float *, const int *, int *);
void RELAPACK_dsygst(const int *, const char *, const int *, double *, const int *, const double *, const int *, int *);
void RELAPACK_chegst(const int *, const char *, const int *, float *, const int *, const float *, const int *, int *);
void RELAPACK_zhegst(const int *, const char *, const int *, double *, const int *, const double *, const int *, int *);

void RELAPACK_strsyl(const char *, const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, int *);
void RELAPACK_dtrsyl(const char *, const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, int *);
void RELAPACK_ctrsyl(const char *, const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, int *);
void RELAPACK_ztrsyl(const char *, const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, int *);

void RELAPACK_stgsyl(const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, float *, float *, const int *, int *, int *);
void RELAPACK_dtgsyl(const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, double *, double *, const int *, int *, int *);
void RELAPACK_ctgsyl(const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, float *, float *, const int *, int *, int *);
void RELAPACK_ztgsyl(const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, double *, double *, const int *, int *, int *);

void RELAPACK_sgemmt(const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, float *, const int *);
void RELAPACK_dgemmt(const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, double *, const int *);
void RELAPACK_cgemmt(const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, float *, const int *);
void RELAPACK_zgemmt(const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, double *, const int *);

#endif /*  RELAPACK_H */
