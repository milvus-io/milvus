// run with OPENBLAS_NUM_THREADS=1 and OMP_NUM_THREADS=n
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <cblas.h>
#include <omp.h>
#include <pthread.h>
#define MIN_SIZE 5
#define MAX_SIZE 60
#define NB_SIZE 10

// number of loop for a 1x1 matrix. Lower it if the test is
// too slow on you computer.
#define NLOOP 2e7

typedef struct {
    int matrix_size;
    int n_loop;
    void (* bench_func)();
    void (* blas_func)();
    void * (* create_matrix)(int size);
} BenchParam;

void * s_create_matrix(int size) {
    float * r = malloc(size * sizeof(double));
    int i;
    for(i = 0; i < size; i++)
        r[i] = 1e3 * i / size;
    return r;
}

void * c_create_matrix(int size) {
    float * r = malloc(size * 2 * sizeof(double));
    int i;
    for(i = 0; i < 2 * size; i++)
        r[i] = 1e3 * i / size;
    return r;
}

void * z_create_matrix(int size) {
    double * r = malloc(size * 2 * sizeof(double));
    int i;
    for(i = 0; i < 2 * size; i++)
        r[i] = 1e3 * i / size;
    return r;
}

void * d_create_matrix(int size) {
    double * r = malloc(size * sizeof(double));
    int i;
    for(i = 0; i < size; i++)
        r[i] = 1e3 * i / size;
    return r;
}

void trmv_bench(BenchParam * param)
{
    int i, n;
    int size = param->matrix_size;
    n = param->n_loop / size;
    int one = 1;
    void * A = param->create_matrix(size * size);
    void * y = param->create_matrix(size);
    for(i = 0; i < n; i++) {
        param->blas_func("U", "N", "N", &size, A, &size, y, &one);
    }
    free(A);
    free(y);
}

void gemv_bench(BenchParam * param)
{
    int i, n;
    int size = param->matrix_size;
    n = param->n_loop / size;
    double v = 1.01;
    int one = 1;
    void * A = param->create_matrix(size * size);
    void * y = param->create_matrix(size);
    for(i = 0; i < n; i++) {
        param->blas_func("N", &size, &size, &v, A, &size, y, &one, &v, y, &one);
    }
    free(A);
    free(y);
}

void ger_bench(BenchParam * param) {
    int i, n;
    int size = param->matrix_size;
    n = param->n_loop / size;
    double v = 1.01;
    int one = 1;
    void * A = param->create_matrix(size * size);
    void * y = param->create_matrix(size);
    for(i = 0; i < n; i++) {
        param->blas_func(&size, &size, &v, y, &one, y, &one, A, &size);
    }
    free(A);
    free(y);
}

#ifndef _WIN32
void * pthread_func_wrapper(void * param) {
    ((BenchParam *)param)->bench_func(param);
    pthread_exit(NULL);
}
#endif

#define NB_TESTS 5
void * TESTS[4 * NB_TESTS] = {
    trmv_bench, ztrmv_, z_create_matrix, "ztrmv",
    gemv_bench, dgemv_, d_create_matrix, "dgemv",
    gemv_bench, zgemv_, z_create_matrix, "zgemv",
    ger_bench, dger_, d_create_matrix, "dger",
    ger_bench, zgerc_, z_create_matrix, "zgerc",
};

inline static double delta_time(struct timespec tick) {
    struct timespec tock;
	clock_gettime(CLOCK_MONOTONIC, &tock);
	return (tock.tv_sec - tick.tv_sec) + (tock.tv_nsec - tick.tv_nsec) / 1e9;
}

double pthread_bench(BenchParam * param, int nb_threads)
{
#ifdef _WIN32
    return 0;
#else
    BenchParam threaded_param = *param;
    pthread_t threads[nb_threads];
    int t, rc;
    struct timespec tick;
    threaded_param.n_loop /= nb_threads;
    clock_gettime(CLOCK_MONOTONIC, &tick);
    for(t=0; t<nb_threads; t++){
        rc = pthread_create(&threads[t], NULL, pthread_func_wrapper, &threaded_param);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
    }
    for(t=0; t<nb_threads; t++){
        pthread_join(threads[t], NULL);
    }
	return delta_time(tick);
#endif
}

double seq_bench(BenchParam * param) {
    struct timespec tick;
    clock_gettime(CLOCK_MONOTONIC, &tick);
    param->bench_func(param);
    return delta_time(tick);
}

double omp_bench(BenchParam * param) {
    BenchParam threaded_param = *param;
    struct timespec tick;
    int t;
    int nb_threads = omp_get_max_threads();
    threaded_param.n_loop /= nb_threads;
    clock_gettime(CLOCK_MONOTONIC, &tick);
    #pragma omp parallel for
    for(t = 0; t < nb_threads; t ++){
        param->bench_func(&threaded_param);
    }
    return delta_time(tick);
}

int main(int argc, char * argv[]) {
    double inc_factor = exp(log((double)MAX_SIZE / MIN_SIZE) / NB_SIZE);
    BenchParam param;
    int test_id;
    printf ("Running on %d threads\n", omp_get_max_threads());
    for(test_id = 0; test_id < NB_TESTS; test_id ++) {
        double size = MIN_SIZE;
        param.bench_func = TESTS[test_id * 4];
        param.blas_func = TESTS[test_id * 4 + 1];
        param.create_matrix = TESTS[test_id * 4 + 2];
        printf("\nBenchmark of %s\n", (char*)TESTS[test_id * 4 + 3]);
        param.n_loop = NLOOP;
        while(size <= MAX_SIZE) {
            param.matrix_size = (int)(size + 0.5);
            double seq_time = seq_bench(&param);
            double omp_time = omp_bench(&param);
            double pthread_time = pthread_bench(&param, omp_get_max_threads());
            printf("matrix size %d, sequential %gs, openmp %gs, speedup %g, "
                   "pthread %gs, speedup %g\n",
                   param.matrix_size, seq_time,
                   omp_time, seq_time / omp_time,
                   pthread_time, seq_time / pthread_time);
            size *= inc_factor;
        }
    }
    return(0);
}
