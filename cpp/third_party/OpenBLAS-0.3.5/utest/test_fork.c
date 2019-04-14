/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of
      its contributors may be used to endorse or promote products
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

#include <sys/types.h>
#include <sys/wait.h>
#include <cblas.h>
#include "openblas_utest.h"

void* xmalloc(size_t n)
{
    void* tmp;
    tmp = malloc(n);
    if (tmp == NULL) {
        fprintf(stderr, "You are about to die\n");
        exit(1);
    } else {
        return tmp;
    }
}

void check_dgemm(double *a, double *b, double *result, double *expected, blasint n)
{
    char trans1 = 'T';
    char trans2 = 'N';
    double zerod = 0, oned = 1;
    int i;
    BLASFUNC(dgemm)(&trans1, &trans2, &n, &n, &n, &oned, a, &n, b, &n, &zerod, result, &n);
    for(i = 0; i < n * n; ++i) {
        ASSERT_DBL_NEAR_TOL(expected[i], result[i], DOUBLE_EPS);
    }
}

CTEST(fork, safety)
{
    blasint n = 1000;
    int i;

    double *a, *b, *c, *d;
    size_t n_bytes;

    pid_t fork_pid;
    pid_t fork_pid_nested;

    n_bytes = sizeof(*a) * n * n;

    a = xmalloc(n_bytes);
    b = xmalloc(n_bytes);
    c = xmalloc(n_bytes);
    d = xmalloc(n_bytes);

    // Put ones in a and b
    for(i = 0; i < n * n; ++i) {
        a[i] = 1;
        b[i] = 1;
    }

    // Compute a DGEMM product in the parent process prior to forking to
    // ensure that the OpenBLAS thread pool is initialized.
    char trans1 = 'T';
    char trans2 = 'N';
    double zerod = 0, oned = 1;
    BLASFUNC(dgemm)(&trans1, &trans2, &n, &n, &n, &oned, a, &n, b, &n, &zerod, c, &n);

    fork_pid = fork();
    if (fork_pid == -1) {
        CTEST_ERR("Failed to fork process.");
    } else if (fork_pid == 0) {
        // Compute a DGEMM product in the child process to check that the
        // thread pool as been properly been reinitialized after the fork.
        check_dgemm(a, b, d, c, n);

        // Nested fork to check that the pthread_atfork protection can work
        // recursively
        fork_pid_nested = fork();
        if (fork_pid_nested == -1) {
            CTEST_ERR("Failed to fork process.");
            exit(1);
        } else if (fork_pid_nested == 0) {
            check_dgemm(a, b, d, c, n);
            exit(0);
        } else {
            check_dgemm(a, b, d, c, n);
            int child_status = 0;
            pid_t wait_pid = wait(&child_status);
            ASSERT_EQUAL(wait_pid, fork_pid_nested);
            ASSERT_EQUAL(0, WEXITSTATUS (child_status));
            exit(0);
        }
    } else {
        check_dgemm(a, b, d, c, n);
        // Wait for the child to finish and check the exit code.
        int child_status = 0;
        pid_t wait_pid = wait(&child_status);
        ASSERT_EQUAL(wait_pid, fork_pid);
        ASSERT_EQUAL(0, WEXITSTATUS (child_status));
    }
}
