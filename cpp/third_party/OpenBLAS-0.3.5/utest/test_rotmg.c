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

#include "openblas_utest.h"

CTEST (drotmg,rotmg)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i=0;
	// original test case for libGoto bug fixed by feb2014 rewrite
	te_d1= 0.21149573940783739;
	te_d2= 0.046892057172954082;
	te_x1= -0.42272687517106533;
	te_y1= 0.42211309121921659;


	for(i=0; i<5; i++){
	  te_param[i]=tr_param[i]=0.0;
	}

	//reference values as calculated by netlib blas

        tr_d1= 0.1732048;
        tr_d2= 0.03840234;
        tr_x1= -0.516180;
        tr_y1= 0.422113;
        tr_d1= 0.17320483687975;
        tr_d2= 0.03840233915037;
        tr_x1= -0.51618034832329;
        tr_y1= 0.42211309121922;

	tr_param[0]= 0.0;
	tr_param[1]= 0.0;
	tr_param[2]= 0.99854803659786; 
	tr_param[3]= -0.22139439665872;
	tr_param[4]= 0.0;

	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);
	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

CTEST (drotmg,rotmg_issue1452)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i=0;

	// from issue #1452
	te_d1 = 5.9e-8;
	te_d2 = 5.960464e-8;
	te_x1 = 1.0;
	te_y1 = 150.0;

	for(i=0; i<5; i++){
	  te_param[i]=tr_param[i]=0.0;
	}
	te_param[3]=1./4096.;
	//reference values as calculated by gonum blas with rotmg rewritten to Hopkins' algorithm
	tr_d1= 0.99995592822897;
	tr_d2= 0.98981219860583;
	tr_x1= 0.03662270484346;
	tr_y1= 150.000000000000;

	tr_param[0]= -1.0;
	tr_param[1]= 0.00000161109346;
	tr_param[2]= -0.00024414062500;
	tr_param[3]= 0.00024414062500;
	tr_param[4]= 0.00000162760417;

	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}

}

CTEST(drotmg, rotmg_D1eqD2_X1eqX2)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i=0;
	te_d1= tr_d1=2.;
	te_d2= tr_d2=2.;
	te_x1= tr_x1=8.;
	te_y1= tr_y1=8.;

	for(i=0; i<5; i++){
	  te_param[i]=tr_param[i]=0.0;
	}
	
	//reference values as calculated by netlib blas
        tr_d1= 1.0;
        tr_d2= 1.0;
        tr_x1= 16.0;
        tr_y1= 8.0;

	tr_param[0]=1.0;
	tr_param[1]=1.0;
	tr_param[2]=0.0;
	tr_param[3]=0.0;
	tr_param[4]=1.0;

	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

CTEST(drotmg, drotmg_D1_big_D2_big_flag_zero)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5]={1.,4096.,-4096.,1.,4096.};
	double tr_param[5]={-1.,4096.,-3584.,1792.,4096.};
	int i=0;
	te_d1= tr_d1=1600000000.;
	te_d2= tr_d2=800000000.;
	te_x1= tr_x1=8.;
	te_y1= tr_y1=7.;

	
	//reference values as calculated by gonum 
        tr_d1= 68.96627824858757;
        tr_d2= 34.483139124293785;
        tr_x1= 45312.;
        tr_y1= 7.0;


	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}
