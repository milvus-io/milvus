#ifndef BLAS_H
#define BLAS_H

extern void BLAS(sswap)(const int *, float *, const int *, float *, const int *);
extern void BLAS(dswap)(const int *, double *, const int *, double *, const int *);
extern void BLAS(cswap)(const int *, float *, const int *, float *, const int *);
extern void BLAS(zswap)(const int *, double *, const int *, double *, const int *);

extern void BLAS(sscal)(const int *, const float *, float *, const int *);
extern void BLAS(dscal)(const int *, const double *, double *, const int *);
extern void BLAS(cscal)(const int *, const float *, float *, const int *);
extern void BLAS(zscal)(const int *, const double *, double *, const int *);

extern void BLAS(saxpy)(const int *, const float *, const float *, const int *, float *, const int *);
extern void BLAS(daxpy)(const int *, const double *, const double *, const int *, double *, const int *);
extern void BLAS(caxpy)(const int *, const float *, const float *, const int *, float *, const int *);
extern void BLAS(zaxpy)(const int *, const double *, const double *, const int *, double *, const int *);

extern void BLAS(sgemv)(const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, const float *, const int*);
extern void BLAS(dgemv)(const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, const double *, const int*);
extern void BLAS(cgemv)(const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, const float *, const int*);
extern void BLAS(zgemv)(const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, const double *, const int*);

extern void BLAS(sgemm)(const char *, const char *, const int *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, const float *, const int*);
extern void BLAS(dgemm)(const char *, const char *, const int *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, const double *, const int*);
extern void BLAS(cgemm)(const char *, const char *, const int *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, const float *, const int*);
extern void BLAS(zgemm)(const char *, const char *, const int *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, const double *, const int*);

extern void BLAS(strsm)(const char *, const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, float *, const int *);
extern void BLAS(dtrsm)(const char *, const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, double *, const int *);
extern void BLAS(ctrsm)(const char *, const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, float *, const int *);
extern void BLAS(ztrsm)(const char *, const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, double *, const int *);

extern void BLAS(strmm)(const char *, const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, float *, const int *);
extern void BLAS(dtrmm)(const char *, const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, double *, const int *);
extern void BLAS(ctrmm)(const char *, const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, float *, const int *);
extern void BLAS(ztrmm)(const char *, const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, double *, const int *);

extern void BLAS(ssyrk)(const char *, const char *, const int *, const int *, const float *, float *, const int *, const float *, float *, const int *);
extern void BLAS(dsyrk)(const char *, const char *, const int *, const int *, const double *, double *, const int *, const double *, double *, const int *);
extern void BLAS(cherk)(const char *, const char *, const int *, const int *, const float *, float *, const int *, const float *, float *, const int *);
extern void BLAS(zherk)(const char *, const char *, const int *, const int *, const double *, double *, const int *, const double *, double *, const int *);

extern void BLAS(ssymm)(const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, float *, const int *);
extern void BLAS(dsymm)(const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, double *, const int *);
extern void BLAS(chemm)(const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, float *, const int *);
extern void BLAS(zhemm)(const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, double *, const int *);

extern void BLAS(ssyr2k)(const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, float *, const int *);
extern void BLAS(dsyr2k)(const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, double *, const int *);
extern void BLAS(cher2k)(const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, float *, const int *);
extern void BLAS(zher2k)(const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, double *, const int *);

#if HAVE_XGEMMT
extern void BLAS(sgemmt)(const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, const float *, const int*);
extern void BLAS(dgemmt)(const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, const double *, const int*);
extern void BLAS(cgemmt)(const char *, const char *, const char *, const int *, const int *, const float *, const float *, const int *, const float *, const int *, const float *, const float *, const int*);
extern void BLAS(zgemmt)(const char *, const char *, const char *, const int *, const int *, const double *, const double *, const int *, const double *, const int *, const double *, const double *, const int*);
#endif

#endif /* BLAS_H */
