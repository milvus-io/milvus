// Licensed under the Apache License, Version 2.0.
#include <omp.h>
#include <cstdio>
#include <cstring>

static volatile int counter = 0;

extern "C" __attribute__((noinline)) void
tsan_openmp_increment() {
    counter = counter + 1;
}

int
main(int argc, char** argv) {
    if (argc != 2)
        return 2;
    omp_set_dynamic(0);
    omp_set_num_threads(4);
    if (std::strcmp(argv[1], "omp-clean-lock") == 0) {
        omp_lock_t lock;
        omp_init_lock(&lock);
#pragma omp parallel for
        for (int i = 0; i < 10000; ++i) {
            omp_set_lock(&lock);
            tsan_openmp_increment();
            omp_unset_lock(&lock);
        }
        omp_destroy_lock(&lock);
        if (counter != 10000)
            return 3;
    } else if (std::strcmp(argv[1], "omp-clean-barrier") == 0) {
        int values[4] = {};
        int sums[4] = {};
#pragma omp parallel shared(values, sums)
        {
            int id = omp_get_thread_num();
            values[id] = id + 1;
#pragma omp barrier
            for (int value : values) sums[id] += value;
        }
        for (int sum : sums)
            if (sum != 10)
                return 3;
    } else if (std::strcmp(argv[1], "omp-clean-task") == 0) {
        int value = 0, result = 0;
#pragma omp parallel shared(value, result)
#pragma omp single
        {
#pragma omp task depend(out : value) shared(value)
            value = 42;
#pragma omp task depend(in : value) shared(value, result)
            result = value;
#pragma omp taskwait
        }
        if (result != 42)
            return 3;
    } else if (std::strcmp(argv[1], "omp-race") == 0) {
#pragma omp parallel for
        for (int i = 0; i < 10000; ++i) tsan_openmp_increment();
    } else {
        return 2;
    }
    std::printf("OpenMP probe completed: %s\n", argv[1]);
}
