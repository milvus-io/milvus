#ifndef LAPACK_H2
#define LAPACK_H2

#include "../config.h"

void LAPACK(slauum)(const char *, const int *, float *, const int *, int *);
void LAPACK(dlauum)(const char *, const int *, double *, const int *, int *);
void LAPACK(clauum)(const char *, const int *, float *, const int *, int *);
void LAPACK(zlauum)(const char *, const int *, double *, const int *, int *);

void LAPACK(strtri)(const char *, const char *, const int *, float *, const int *, int *);
void LAPACK(dtrtri)(const char *, const char *, const int *, double *, const int *, int *);
void LAPACK(ctrtri)(const char *, const char *, const int *, float *, const int *, int *);
void LAPACK(ztrtri)(const char *, const char *, const int *, double *, const int *, int *);

void LAPACK(spotrf)(const char *, const int *, float *, const int *, int *);
void LAPACK(dpotrf)(const char *, const int *, double *, const int *, int *);
void LAPACK(cpotrf)(const char *, const int *, float *, const int *, int *);
void LAPACK(zpotrf)(const char *, const int *, double *, const int *, int *);

void LAPACK(spbtrf)(const char *, const int *, const int *, float *, const int *, int *);
void LAPACK(dpbtrf)(const char *, const int *, const int *, double *, const int *, int *);
void LAPACK(cpbtrf)(const char *, const int *, const int *, float *, const int *, int *);
void LAPACK(zpbtrf)(const char *, const int *, const int *, double *, const int *, int *);

void LAPACK(ssytrf)(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void LAPACK(dsytrf)(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void LAPACK(csytrf)(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void LAPACK(chetrf)(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void LAPACK(zsytrf)(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void LAPACK(zhetrf)(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void LAPACK(ssytrf_rook)(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void LAPACK(dsytrf_rook)(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void LAPACK(csytrf_rook)(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void LAPACK(chetrf_rook)(const char *, const int *, float *, const int *, int *, float *, const int *, int *);
void LAPACK(zsytrf_rook)(const char *, const int *, double *, const int *, int *, double *, const int *, int *);
void LAPACK(zhetrf_rook)(const char *, const int *, double *, const int *, int *, double *, const int *, int *);

void LAPACK(sgetrf)(const int *, const int *, float *, const int *, int *, int *);
void LAPACK(dgetrf)(const int *, const int *, double *, const int *, int *, int *);
void LAPACK(cgetrf)(const int *, const int *, float *, const int *, int *, int *);
void LAPACK(zgetrf)(const int *, const int *, double *, const int *, int *, int *);

void LAPACK(sgbtrf)(const int *, const int *, const int *, const int *, float *, const int *, int *, int *);
void LAPACK(dgbtrf)(const int *, const int *, const int *, const int *, double *, const int *, int *, int *);
void LAPACK(cgbtrf)(const int *, const int *, const int *, const int *, float *, const int *, int *, int *);
void LAPACK(zgbtrf)(const int *, const int *, const int *, const int *, double *, const int *, int *, int *);

void LAPACK(ssygst)(const int *, const char *, const int *, float *, const int *, const float *, const int *, int *);
void LAPACK(dsygst)(const int *, const char *, const int *, double *, const int *, const double *, const int *, int *);
void LAPACK(chegst)(const int *, const char *, const int *, float *, const int *, const float *, const int *, int *);
void LAPACK(zhegst)(const int *, const char *, const int *, double *, const int *, const double *, const int *, int *);

void LAPACK(strsyl)(const char *, const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, int *);
void LAPACK(dtrsyl)(const char *, const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, int *);
void LAPACK(ctrsyl)(const char *, const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, int *);
void LAPACK(ztrsyl)(const char *, const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, int *);

void LAPACK(stgsyl)(const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, float *, float *, const int *, int *, int *);
void LAPACK(dtgsyl)(const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, double *, double *, const int *, int *, int *);
void LAPACK(ctgsyl)(const char *, const int *, const int *, const int *, const float *, const int *, const float *, const int *, float *, const int *, const float *, const int *, const float *, const int *, float *, const int *, float *, float *, float *, const int *, int *, int *);
void LAPACK(ztgsyl)(const char *, const int *, const int *, const int *, const double *, const int *, const double *, const int *, double *, const int *, const double *, const int *, const double *, const int *, double *, const int *, double *, double *, double *, const int *, int *, int *);

#endif /*  LAPACK_H2 */
