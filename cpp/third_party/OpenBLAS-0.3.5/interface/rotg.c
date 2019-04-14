#include <math.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#ifndef CBLAS

void NAME(FLOAT *DA, FLOAT *DB, FLOAT *C, FLOAT *S){

#else

void CNAME(FLOAT *DA, FLOAT *DB, FLOAT *C, FLOAT *S){

#endif

#if defined(__i386__) || defined(__x86_64__) || defined(__ia64__) || defined(_M_X64) || defined(_M_IX86)

  long double da = *DA;
  long double db = *DB;
  long double c;
  long double s;
  long double r, roe, z;

  long double ada = fabsl(da);
  long double adb = fabsl(db);
  long double scale = ada + adb;

#ifndef CBLAS
  PRINT_DEBUG_NAME;
#else
  PRINT_DEBUG_CNAME;
#endif

  roe = db;
  if (ada > adb) roe = da;

  if (scale == ZERO) {
    *C = ONE;
    *S = ZERO;
    *DA = ZERO;
    *DB = ZERO;
  } else {
    r = sqrt(da * da + db * db);
    if (roe < 0) r = -r;
    c = da / r;
    s = db / r;
    z = ONE;
    if (da != ZERO) {
      if (ada > adb){
	z = s;
      } else {
	z = ONE / c;
      }
    }

    *C = c;
    *S = s;
    *DA = r;
    *DB = z;
  }

#else
  FLOAT da = *DA;
  FLOAT db = *DB;
  FLOAT c  = *C;
  FLOAT s  = *S;
  FLOAT r, roe, z;

  FLOAT ada = fabs(da);
  FLOAT adb = fabs(db);
  FLOAT scale = ada + adb;

#ifndef CBLAS
  PRINT_DEBUG_NAME;
#else
  PRINT_DEBUG_CNAME;
#endif

  roe = db;
  if (ada > adb) roe = da;

  if (scale == ZERO) {
    *C = ONE;
    *S = ZERO;
    *DA = ZERO;
    *DB = ZERO;
  } else {
    FLOAT aa = da / scale;
    FLOAT bb = db / scale;

    r = scale * sqrt(aa * aa + bb * bb);
    if (roe < 0) r = -r;
    c = da / r;
    s = db / r;
    z = ONE;
    if (ada > adb) z = s;
    if ((ada <= adb) && (c != ZERO)) z = ONE / c;

    *C = c;
    *S = s;
    *DA = r;
    *DB = z;
  }
#endif

  return;
}
