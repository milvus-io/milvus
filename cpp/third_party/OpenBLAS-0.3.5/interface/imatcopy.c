/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
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
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/***********************************************************
 * 2014-06-10 Saar
 * 2015-09-07 grisuthedragon 
***********************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#if defined(DOUBLE)
#define ERROR_NAME "DIMATCOPY"
#else
#define ERROR_NAME "SIMATCOPY"
#endif

#define BlasRowMajor 0
#define BlasColMajor 1
#define BlasNoTrans  0
#define BlasTrans    1

#undef malloc
#undef free

/* Enables the New IMATCOPY code with inplace operation if lda == ldb   */
#define NEW_IMATCOPY

#ifndef CBLAS
void NAME( char* ORDER, char* TRANS, blasint *rows, blasint *cols, FLOAT *alpha, FLOAT *a, blasint *lda, blasint *ldb)
{

	char Order, Trans;
	int order=-1,trans=-1;
	blasint info = -1;
	FLOAT *b;
	size_t msize;

	Order = *ORDER;
	Trans = *TRANS;

	TOUPPER(Order);
	TOUPPER(Trans);

	if ( Order == 'C' ) order = BlasColMajor;
	if ( Order == 'R' ) order = BlasRowMajor;
	if ( Trans == 'N' ) trans = BlasNoTrans;
	if ( Trans == 'R' ) trans = BlasNoTrans;
	if ( Trans == 'T' ) trans = BlasTrans;
	if ( Trans == 'C' ) trans = BlasTrans;
#else 
void CNAME( enum CBLAS_ORDER CORDER, enum CBLAS_TRANSPOSE CTRANS, blasint crows, blasint ccols, FLOAT calpha, FLOAT *a, blasint clda, blasint cldb)
{
	int order=-1,trans=-1;
	blasint info = -1;
	FLOAT *b;
	size_t msize;
	blasint *lda, *ldb, *rows, *cols; 
	FLOAT *alpha; 

	if ( CORDER == CblasColMajor) order = BlasColMajor; 
	if ( CORDER == CblasRowMajor) order = BlasRowMajor; 
	if ( CTRANS == CblasNoTrans || CTRANS == CblasConjNoTrans) trans = BlasNoTrans; 
	if ( CTRANS == CblasTrans   || CTRANS == CblasConjTrans  ) trans = BlasTrans; 

	rows = &crows; 
	cols = &ccols; 
	alpha = &calpha; 
	lda = &clda; 
	ldb = &cldb; 	
#endif 

	if ( order == BlasColMajor)
	{
        	if ( trans == BlasNoTrans  &&  *ldb < *rows ) info = 9;
        	if ( trans == BlasTrans    &&  *ldb < *cols ) info = 9;
	}
	if ( order == BlasRowMajor)
	{
        	if ( trans == BlasNoTrans  &&  *ldb < *cols ) info = 9;
        	if ( trans == BlasTrans    &&  *ldb < *rows ) info = 9;
	}

	if ( order == BlasColMajor &&  *lda < *rows ) info = 7;
	if ( order == BlasRowMajor &&  *lda < *cols ) info = 7;
	if ( *cols <= 0 ) info = 4;
	if ( *rows <= 0 ) info = 3;
	if ( trans < 0  ) info = 2;
	if ( order < 0  ) info = 1;

	if (info >= 0) {
    		BLASFUNC(xerbla)(ERROR_NAME, &info, sizeof(ERROR_NAME));
    		return;
  	}
#ifdef NEW_IMATCOPY
    if ( *lda == *ldb && *rows == *cols) {
        if ( order == BlasColMajor )
        {
            if ( trans == BlasNoTrans )
            {
                IMATCOPY_K_CN(*rows, *cols, *alpha, a, *lda );
            }
            else
            {
                IMATCOPY_K_CT(*rows, *cols, *alpha, a, *lda );
            }
        }
        else
        {
            if ( trans == BlasNoTrans )
            {
                IMATCOPY_K_RN(*rows, *cols, *alpha, a, *lda );
            }
            else
            {
                IMATCOPY_K_RT(*rows, *cols, *alpha, a, *lda );
            }
        }
        return; 
    }

#endif

	if ( *lda >  *ldb )
		msize = (*lda) * (*ldb)  * sizeof(FLOAT);
	else
		msize = (*ldb) * (*ldb)  * sizeof(FLOAT);

	b = malloc(msize);
	if ( b == NULL )
	{
		printf("Memory alloc failed\n");
		exit(1);
	}

	if ( order == BlasColMajor )
	{
		if ( trans == BlasNoTrans )
		{
			OMATCOPY_K_CN(*rows, *cols, *alpha, a, *lda, b, *ldb );
			OMATCOPY_K_CN(*rows, *cols, (FLOAT) 1.0 , b, *ldb, a, *ldb );
		}
		else
		{
			OMATCOPY_K_CT(*rows, *cols, *alpha, a, *lda, b, *ldb );
			OMATCOPY_K_CN(*cols, *rows, (FLOAT) 1.0, b, *ldb, a, *ldb );
		}
	}
	else
	{
		if ( trans == BlasNoTrans )
		{
			OMATCOPY_K_RN(*rows, *cols, *alpha, a, *lda, b, *ldb );
			OMATCOPY_K_RN(*rows, *cols, (FLOAT) 1.0, b, *ldb, a, *ldb );
		}
		else
		{
			OMATCOPY_K_RT(*rows, *cols, *alpha, a, *lda, b, *ldb );
			OMATCOPY_K_RN(*cols, *rows, (FLOAT) 1.0, b, *ldb, a, *ldb );
		}
	}

	free(b);
	return;

}


