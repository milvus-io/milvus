#ifndef TEST_H
#define TEST_H

#include "../config.h"
#include "config.h"

#if BLAS_UNDERSCORE
#define BLAS(routine) routine ## _
#else
#define BLAS(routine) routine
#endif

#if LAPACK_UNDERSCORE
#define LAPACK(routine) routine ## _
#else
#define LAPACK(routine) routine
#endif

#include "../inc/relapack.h"
#include "lapack.h"
#include "util.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

// some name mangling macros
#define CAT(A, B) A ## B
#define XCAT(A, B) CAT(A, B)
#define XLAPACK(X) LAPACK(X)
#define XRELAPACK(X) XCAT(RELAPACK_, X)
#define STR(X) #X
#define XSTR(X) STR(X)

// default setup and error computation names: pre() and post()
#define PRE pre
#define POST post

// TEST macro:
// run setup (pre()), ReLAPACK routine (i = 0), LAPACK routine (i = 1), compute
// error (post()), check error bound, and  print setup and error
#define TEST(...) \
    PRE(); \
    i = 0; \
    XRELAPACK(ROUTINE)(__VA_ARGS__); \
    i = 1; \
    XLAPACK(ROUTINE)(__VA_ARGS__); \
    POST(); \
    fail |= error > ERR_BOUND; \
    printf("%s(%s)\t%g\n", XSTR(ROUTINE), #__VA_ARGS__, error);

// generalized datatype treatment: DT_PREFIX determines the type s, d, c, or z
#define XPREF(A) XCAT(DT_PREFIX, A)

// matrix generation and error computation routines
#define x2matgen XPREF(2matgen)
#define x2vecerr XPREF(2vecerr)

// error bounds
#define ERR_BOUND XPREF(ERR_BOUND_)
#define sERR_BOUND_ SINGLE_ERR_BOUND
#define dERR_BOUND_ DOUBLE_ERR_BOUND
#define cERR_BOUND_ SINGLE_ERR_BOUND
#define zERR_BOUND_ DOUBLE_ERR_BOUND

// C datatypes
#define datatype XPREF(datatype_)
#define sdatatype_ float
#define ddatatype_ double
#define cdatatype_ float
#define zdatatype_ double

// number of C datatype elements per element
#define x1 XPREF(DT_MULT)
#define sDT_MULT 1
#define dDT_MULT 1
#define cDT_MULT 2
#define zDT_MULT 2

// typed allocations
#define xmalloc XPREF(malloc)
#define imalloc(S) malloc((S) * sizeof(int))
#define smalloc(S) malloc((S) * sizeof(float))
#define dmalloc(S) malloc((S) * sizeof(double))
#define cmalloc(S) malloc((S) * 2 * sizeof(float))
#define zmalloc(S) malloc((S) * 2 * sizeof(double))

// transpositions
#define xCTRANS XPREF(CTRANS)
#define sCTRANS "T"
#define dCTRANS "T"
#define cCTRANS "C"
#define zCTRANS "C"

// some constants
#define MONE XPREF(MONE)
const float  sMONE[] = { -1. };
const double dMONE[] = { -1. };
const float  cMONE[] = { -1., 0. };
const double zMONE[] = { -1., 0. };

#define ZERO XPREF(ZERO)
const float  sZERO[] = { 0. };
const double dZERO[] = { 0. };
const float  cZERO[] = { 0., 0. };
const double zZERO[] = { 0., 0. };

#define ONE  XPREF(ONE)
const float  sONE[]  = { 1. };
const double dONE[]  = { 1. };
const float  cONE[]  = { 1., 0. };
const double zONE[]  = { 1., 0. };

const int iMONE[]  = { -1 };
const int iZERO[]  = { 0 };
const int iONE[]   = { 1 };
const int iTWO[]   = { 2 };
const int iTHREE[] = { 3 };
const int iFOUR[]  = { 4 };

void tests();

// global variables (used in tests(), pre(), and post())
int i, n, n2, fail;
double error;

int main(int argc, char* argv[]) {
    n = TEST_SIZE;
    n2 = (3 * n) / 4;
    fail = 0;

    tests();

    return fail;
}

#endif /* TEST_H */
