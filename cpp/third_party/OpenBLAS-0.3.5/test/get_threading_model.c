#include "../cblas.h"

int main() {
	int th_model = openblas_get_parallel();
	switch(th_model) {
		case OPENBLAS_SEQUENTIAL:
			printf("OpenBLAS is compiled sequentially.\n");
			break;
		case OPENBLAS_THREAD:
			printf("OpenBLAS is compiled using the normal threading model\n");
			break;
		case OPENBLAS_OPENMP:
			printf("OpenBLAS is compiled using OpenMP\n");
			break;
	}
	return 0;
}

