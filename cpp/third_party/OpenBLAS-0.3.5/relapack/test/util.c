#include "util.h"
#include <stdlib.h>
#include <time.h>
#include <math.h>

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))

///////////////////////
// matrix generation //
///////////////////////
// Each routine x2matgen is passed the size (m, n) of the desired matrix and
// geneartes two copies of such a matrix in in its output arguments A and B.
// The generated matrices is filled with random entries in [0, 1[ (+i*[0, 1[ in
// the complex case).  Then m is added to the diagonal; this is numerically
// favorable for routines working with triangular and symmetric matrices.  For
// the same reason the imaginary part of the diagonal is set to 0.

void s2matgen(const int m, const int n, float *A, float *B) {
    srand(time(NULL) + (size_t) A);
    int i, j;
    for (i = 0; i < m; i++)
        for (j = 0; j < n; j++)
            A[i + m * j] = B[i + m * j] = (float) rand() / RAND_MAX + m * (i == j);
}

void d2matgen(const int m, const int n, double *A, double *B) {
    srand(time(NULL) + (size_t) A);
    int i, j;
    for (i = 0; i < m; i++)
        for (j = 0; j < n; j++)
            A[i + m * j] = B[i + m * j] = (double) rand() / RAND_MAX + m * (i == j);
}

void c2matgen(const int m, const int n, float *A, float *B) {
    srand(time(NULL) + (size_t) A);
    int i, j;
    for (i = 0; i < m; i++)
        for (j = 0; j < n; j++) {
            A[2* (i + m * j)]     = B[2 * (i + m * j)]     = (float) rand() / RAND_MAX + m * (i == j);
            A[2* (i + m * j) + 1] = B[2 * (i + m * j) + 1] = ((float) rand() / RAND_MAX) * (i != j);
        }
}

void z2matgen(const int m, const int n, double *A, double *B) {
    srand(time(NULL) + (size_t) A);
    int i, j;
    for (i = 0; i < m; i++)
        for (j = 0; j < n; j++) {
            A[2* (i + m * j)]     = B[2 * (i + m * j)]     = (double) rand() / RAND_MAX + m * (i == j);
            A[2* (i + m * j) + 1] = B[2 * (i + m * j) + 1] = ((double) rand() / RAND_MAX) * (i != j);
        }
}

////////////////////////
// error computations //
////////////////////////
// Each routine x2vecerrr is passed a vector lengh n and two vectors x and y.
// It returns the maximum of the element-wise error between these two vectors.
// This error is the minimum of the absolute difference and the relative
// differene with respect to y.

double i2vecerr(const int n, const int *x, const int *y) {
    double error = 0;
    int i;
    for (i = 0; i < n; i++) {
        double nom = abs(x[i] - y[i]);
        double den = abs(y[i]);
        error = MAX(error, (den > 0) ? MIN(nom, nom / den) : nom);
    }
    return error;
}

double s2vecerr(const int n, const float *x, const float *y) {
    float error = 0;
    int i;
    for (i = 0; i < n; i++) {
        double nom = fabs((double) x[i] - y[i]);
        double den = fabs(y[i]);
        error = MAX(error, (den > 0) ? MIN(nom, nom / den) : nom);
    }
    return error;
}

double d2vecerr(const int n, const double *x, const double *y) {
    double error = 0;
    int i;
    for (i = 0; i < n; i++) {
        double nom = fabs(x[i] - y[i]);
        double den = fabs(y[i]);
        error = MAX(error, (den > 0) ? MIN(nom, nom / den) : nom);
    }
    return error;
}

double c2vecerr(const int n, const float *x, const float *y) {
    double error = 0;
    int i;
    for (i = 0; i < n; i++) {
        double nom = sqrt(((double) x[2 * i] - y[2 * i]) * ((double) x[2 * i] - y[2 * i]) + ((double) x[2 * i + 1] - y[2 * i + 1]) * ((double) x[2 * i + 1] - y[2 * i + 1]));
        double den = sqrt((double) y[2 * i] * y[2 * i] + (double) y[2 * i + 1] * y[2 * i + 1]);
        error = MAX(error, (den > 0) ? MIN(nom, nom / den) : nom);
    }
    return error;
}

double z2vecerr(const int n, const double *x, const double *y) {
    double error = 0;
    int i;
    for (i = 0; i < n; i++) {
        double nom = sqrt((x[2 * i] - y[2 * i]) * (x[2 * i] - y[2 * i]) + (x[2 * i + 1] - y[2 * i + 1]) * (x[2 * i + 1] - y[2 * i + 1]));
        double den = sqrt(y[2 * i] * y[2 * i] + y[2 * i + 1] * y[2 * i + 1]);
        error = MAX(error, (den > 0) ? MIN(nom, nom / den) : nom);
    }
    return error;
}
