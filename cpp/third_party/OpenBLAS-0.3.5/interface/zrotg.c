#include <math.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

void NAME(FLOAT *DA, FLOAT *DB, FLOAT *C, FLOAT *S){

#if defined(__i386__) || defined(__x86_64__) || defined(__ia64__) || defined(_M_X64) || defined(_M_IX86)

  long double da_r = *(DA + 0);
  long double da_i = *(DA + 1);
  long double db_r = *(DB + 0);
  long double db_i = *(DB + 1);
  long double r;

  long double ada = fabsl(da_r) + fabsl(da_i);

  PRINT_DEBUG_NAME;

  IDEBUG_START;

  FUNCTION_PROFILE_START();

  if (ada == ZERO) {
    *C        = ZERO;
    *(S  + 0) = ONE;
    *(S  + 1) = ZERO;
    *(DA + 0) = db_r;
    *(DA + 1) = db_i;
  } else {
    long double alpha_r, alpha_i;

    ada = sqrt(da_r * da_r + da_i * da_i);

    r = sqrt(da_r * da_r + da_i * da_i + db_r * db_r + db_i * db_i);

    alpha_r = da_r / ada;
    alpha_i = da_i / ada;

    *(C + 0)  = ada / r;
    *(S + 0)  = (alpha_r * db_r + alpha_i *db_i) / r;
    *(S + 1)  = (alpha_i * db_r - alpha_r *db_i) / r;
    *(DA + 0) = alpha_r * r;
    *(DA + 1) = alpha_i * r;
  }
#else
  FLOAT da_r = *(DA + 0);
  FLOAT da_i = *(DA + 1);
  FLOAT db_r = *(DB + 0);
  FLOAT db_i = *(DB + 1);
  FLOAT r;

  FLOAT ada = fabs(da_r) + fabs(da_i);
  FLOAT adb;

  PRINT_DEBUG_NAME;

  IDEBUG_START;

  FUNCTION_PROFILE_START();

  if (ada == ZERO) {
    *C        = ZERO;
    *(S  + 0) = ONE;
    *(S  + 1) = ZERO;
    *(DA + 0) = db_r;
    *(DA + 1) = db_i;
  } else {
    FLOAT scale;
    FLOAT aa_r, aa_i, bb_r, bb_i;
    FLOAT alpha_r, alpha_i;

    aa_r = fabs(da_r);
    aa_i = fabs(da_i);

    if (aa_i > aa_r) {
      aa_r = fabs(da_i);
      aa_i = fabs(da_r);
    }

    scale = (aa_i / aa_r);
    ada = aa_r * sqrt(ONE + scale * scale);

    bb_r = fabs(db_r);
    bb_i = fabs(db_i);

    if (bb_i > bb_r) {
      bb_r = fabs(bb_i);
      bb_i = fabs(bb_r);
    }

    scale = (bb_i / bb_r);
    adb = bb_r * sqrt(ONE + scale * scale);

    scale = ada + adb;

    aa_r    = da_r / scale;
    aa_i    = da_i / scale;
    bb_r    = db_r / scale;
    bb_i    = db_i / scale;

    r = scale * sqrt(aa_r * aa_r + aa_i * aa_i + bb_r * bb_r + bb_i * bb_i);

    alpha_r = da_r / ada;
    alpha_i = da_i / ada;

    *(C + 0)  = ada / r;
    *(S + 0)  = (alpha_r * db_r + alpha_i *db_i) / r;
    *(S + 1)  = (alpha_i * db_r - alpha_r *db_i) / r;
    *(DA + 0) = alpha_r * r;
    *(DA + 1) = alpha_i * r;
  }
#endif

  FUNCTION_PROFILE_END(4, 4, 4);

  IDEBUG_END;

  return;
}
