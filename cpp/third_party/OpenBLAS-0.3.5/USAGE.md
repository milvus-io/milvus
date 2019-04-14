# Notes on OpenBLAS usage
## Usage

#### Program is Terminated. Because you tried to allocate too many memory regions

In OpenBLAS, we mange a pool of memory buffers and allocate the number of
buffers as the following.
```
#define NUM_BUFFERS (MAX_CPU_NUMBER * 2)
```
This error indicates that the program exceeded the number of buffers.

Please build OpenBLAS with larger `NUM_THREADS`. For example, `make
NUM_THREADS=32` or `make NUM_THREADS=64`.  In `Makefile.system`, we will set
`MAX_CPU_NUMBER=NUM_THREADS`.

Despite its name, and due to the use of memory buffers in functions like SGEMM,
the setting of NUM_THREADS can be relevant even for a single-threaded build 
of OpenBLAS, if such functions get called by multiple threads of a program
that uses OpenBLAS. In some cases, the affected code may simply crash or throw 
a segmentation fault without displaying the above warning first.

Note that the number of threads used at runtime can be altered to differ from the
value NUM_THREADS was set to at build time. At runtime, the actual number of
threads can be set anywhere from 1 to the build's NUM_THREADS (note however,
that this does not change the number of memory buffers that will be allocated,
which is set at build time). The number of threads for a process can be set by
using the mechanisms described below.


#### How can I use OpenBLAS in multi-threaded applications?

If your application is already multi-threaded, it will conflict with OpenBLAS
multi-threading. Thus, you must set OpenBLAS to use single thread in any of the
following ways:

* `export OPENBLAS_NUM_THREADS=1` in the environment variables.
* Call `openblas_set_num_threads(1)` in the application on runtime.
* Build OpenBLAS single thread version, e.g. `make USE_THREAD=0`

If the application is parallelized by OpenMP, please use OpenBLAS built with
`USE_OPENMP=1`

#### How to choose TARGET manually at runtime when compiled with DYNAMIC_ARCH

The environment variable which control the kernel selection is
`OPENBLAS_CORETYPE` (see `driver/others/dynamic.c`) e.g. `export
OPENBLAS_CORETYPE=Haswell` and the function `char* openblas_get_corename()`
returns the used target.

#### How could I disable OpenBLAS threading affinity on runtime?

You can define the `OPENBLAS_MAIN_FREE` or `GOTOBLAS_MAIN_FREE` environment
variable to disable threading affinity on runtime. For example, before the
running,
```
export OPENBLAS_MAIN_FREE=1
```

Alternatively, you can disable affinity feature with enabling `NO_AFFINITY=1`
in `Makefile.rule`.

## Linking with the library

* Link with shared library

`gcc -o test test.c -I /your_path/OpenBLAS/include/ -L/your_path/OpenBLAS/lib -lopenblas`

If the library is multithreaded, please add `-lpthread`. If the library
contains LAPACK functions, please add `-lgfortran` or other Fortran libs.

* Link with static library

`gcc -o test test.c /your/path/libopenblas.a`

You can download `test.c` from https://gist.github.com/xianyi/5780018

On Linux, if OpenBLAS was compiled with threading support (`USE_THREAD=1` by
default), custom programs statically linked against `libopenblas.a` should also
link with the pthread library e.g.:

```
gcc -static -I/opt/OpenBLAS/include -L/opt/OpenBLAS/lib -o my_program my_program.c -lopenblas -lpthread
```

Failing to add the `-lpthread` flag will cause errors such as:

```
/opt/OpenBLAS/libopenblas.a(memory.o): In function `_touch_memory':
memory.c:(.text+0x15): undefined reference to `pthread_mutex_lock'
memory.c:(.text+0x41): undefined reference to `pthread_mutex_unlock'
...
```

## Code examples

#### Call CBLAS interface
This example shows calling cblas_dgemm in C. https://gist.github.com/xianyi/6930656
```
#include <cblas.h>
#include <stdio.h>

void main()
{
  int i=0;
  double A[6] = {1.0,2.0,1.0,-3.0,4.0,-1.0};
  double B[6] = {1.0,2.0,1.0,-3.0,4.0,-1.0};
  double C[9] = {.5,.5,.5,.5,.5,.5,.5,.5,.5};
  cblas_dgemm(CblasColMajor, CblasNoTrans, CblasTrans,3,3,2,1,A, 3, B, 3,2,C,3);

  for(i=0; i<9; i++)
    printf("%lf ", C[i]);
  printf("\n");
}
```
`gcc -o test_cblas_open test_cblas_dgemm.c -I /your_path/OpenBLAS/include/ -L/your_path/OpenBLAS/lib -lopenblas -lpthread -lgfortran`

#### Call BLAS Fortran interface

This example shows calling dgemm Fortran interface in C. https://gist.github.com/xianyi/5780018

```
#include "stdio.h"
#include "stdlib.h"
#include "sys/time.h"
#include "time.h"

extern void dgemm_(char*, char*, int*, int*,int*, double*, double*, int*, double*, int*, double*, double*, int*);

int main(int argc, char* argv[])
{
  int i;
  printf("test!\n");
  if(argc<4){
    printf("Input Error\n");
    return 1;
  }

  int m = atoi(argv[1]);
  int n = atoi(argv[2]);
  int k = atoi(argv[3]);
  int sizeofa = m * k;
  int sizeofb = k * n;
  int sizeofc = m * n;
  char ta = 'N';
  char tb = 'N';
  double alpha = 1.2;
  double beta = 0.001;

  struct timeval start,finish;
  double duration;

  double* A = (double*)malloc(sizeof(double) * sizeofa);
  double* B = (double*)malloc(sizeof(double) * sizeofb);
  double* C = (double*)malloc(sizeof(double) * sizeofc);

  srand((unsigned)time(NULL));

  for (i=0; i<sizeofa; i++)
    A[i] = i%3+1;//(rand()%100)/10.0;

  for (i=0; i<sizeofb; i++)
    B[i] = i%3+1;//(rand()%100)/10.0;

  for (i=0; i<sizeofc; i++)
    C[i] = i%3+1;//(rand()%100)/10.0;
  //#if 0
  printf("m=%d,n=%d,k=%d,alpha=%lf,beta=%lf,sizeofc=%d\n",m,n,k,alpha,beta,sizeofc);
  gettimeofday(&start, NULL);
  dgemm_(&ta, &tb, &m, &n, &k, &alpha, A, &m, B, &k, &beta, C, &m);
  gettimeofday(&finish, NULL);

  duration = ((double)(finish.tv_sec-start.tv_sec)*1000000 + (double)(finish.tv_usec-start.tv_usec)) / 1000000;
  double gflops = 2.0 * m *n*k;
  gflops = gflops/duration*1.0e-6;

  FILE *fp;
  fp = fopen("timeDGEMM.txt", "a");
  fprintf(fp, "%dx%dx%d\t%lf s\t%lf MFLOPS\n", m, n, k, duration, gflops);
  fclose(fp);

  free(A);
  free(B);
  free(C);
  return 0;
}
```

` gcc -o time_dgemm time_dgemm.c /your/path/libopenblas.a`

` ./time_dgemm <m> <n> <k> `

## Troubleshooting
* Please read [Faq](https://github.com/xianyi/OpenBLAS/wiki/Faq) at first.
* Please use gcc version 4.6 and above to compile Sandy Bridge AVX kernels on Linux/MingW/BSD.
* Please use Clang version 3.1 and above to compile the library on Sandy Bridge microarchitecture. The Clang 3.0 will generate the wrong AVX binary code.
* The number of CPUs/Cores should less than or equal to 256. On Linux x86_64(amd64), there is experimental support for up to 1024 CPUs/Cores and 128 numa nodes if you build the library with BIGNUMA=1.
* OpenBLAS does not set processor affinity by default. On Linux, you can enable processor affinity by commenting the line NO_AFFINITY=1 in Makefile.rule. But this may cause [the conflict with R parallel](https://stat.ethz.ch/pipermail/r-sig-hpc/2012-April/001348.html).
* On Loongson 3A. make test would be failed because of pthread_create error. The error code is EAGAIN. However, it will be OK when you run the same testcase on shell.

## BLAS reference manual
If you want to understand every BLAS function and definition, please read
[Intel MKL reference manual](https://software.intel.com/sites/products/documentation/doclib/iss/2013/mkl/mklman/GUID-F7ED9FB8-6663-4F44-A62B-61B63C4F0491.htm)
or [netlib.org](http://netlib.org/blas/)

Here are [OpenBLAS extension functions](https://github.com/xianyi/OpenBLAS/wiki/OpenBLAS-Extensions)

## How to reference OpenBLAS.

You can reference our [papers](https://github.com/xianyi/OpenBLAS/wiki/publications).

Alternatively, you can cite the OpenBLAS homepage http://www.openblas.net directly.

