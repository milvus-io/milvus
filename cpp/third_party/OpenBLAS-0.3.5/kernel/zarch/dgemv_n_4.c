/***************************************************************************
Copyright (c) 2017, The OpenBLAS Project
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


#include "common.h"

#define NBMAX 2048

#define HAVE_KERNEL_4x4_VEC 1
#define HAVE_KERNEL_4x2_VEC 1
#define HAVE_KERNEL_4x1_VEC 1

#if defined(HAVE_KERNEL_4x4_VEC) || defined(HAVE_KERNEL_4x2_VEC) || defined(HAVE_KERNEL_4x1_VEC)
 #include <vecintrin.h>
#endif

#ifdef HAVE_KERNEL_4x4

#elif HAVE_KERNEL_4x4_VEC

static void dgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    BLASLONG i;
    FLOAT x0,x1,x2,x3;
    x0 = xo[0] * *alpha;
    x1 = xo[1] * *alpha;
    x2 = xo[2] * *alpha;
    x3 = xo[3] * *alpha;
    __vector double   v_x0 = {x0,x0};
    __vector double   v_x1 = {x1,x1};
    __vector double   v_x2 = {x2,x2};
    __vector double   v_x3 = {x3,x3};
    __vector double* v_y =(__vector double*)y;      
    __vector double* va0 = (__vector double*)ap[0];
    __vector double* va1 = (__vector double*)ap[1];
    __vector double* va2 = (__vector double*)ap[2];
    __vector double* va3 = (__vector double*)ap[3]; 

    for ( i=0; i< n/2; i+=2 )
    {
        v_y[i]   += v_x0 * va0[i]   +  v_x1 * va1[i]   + v_x2 * va2[i]   + v_x3 * va3[i] ;
        v_y[i+1] += v_x0 * va0[i+1] +  v_x1 * va1[i+1] + v_x2 * va2[i+1] + v_x3 * va3[i+1] ;        
    }
}

#else

static void dgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    BLASLONG i;
    FLOAT *a0,*a1,*a2,*a3;
    FLOAT x[4]  __attribute__ ((aligned (16)));
    a0 = ap[0];
    a1 = ap[1];
    a2 = ap[2];
    a3 = ap[3];

    for ( i=0; i<4; i++)
        x[i] = xo[i] * *alpha;

    for ( i=0; i< n; i+=4 )
    {
        y[i] += a0[i]*x[0] + a1[i]*x[1] + a2[i]*x[2] + a3[i]*x[3];        
        y[i+1] += a0[i+1]*x[0] + a1[i+1]*x[1] + a2[i+1]*x[2] + a3[i+1]*x[3];        
        y[i+2] += a0[i+2]*x[0] + a1[i+2]*x[1] + a2[i+2]*x[2] + a3[i+2]*x[3];        
        y[i+3] += a0[i+3]*x[0] + a1[i+3]*x[1] + a2[i+3]*x[2] + a3[i+3]*x[3];        
    }
}


#endif

#ifdef HAVE_KERNEL_4x2

#elif HAVE_KERNEL_4x2_VEC

static void dgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    BLASLONG i;
    FLOAT x0,x1;
    x0 = xo[0] * *alpha;
    x1 = xo[1] * *alpha; 
    __vector double   v_x0 = {x0,x0};
    __vector double   v_x1 = {x1,x1}; 
    __vector double* v_y =(__vector double*)y;      
    __vector double* va0 = (__vector double*)ap[0];
    __vector double* va1 = (__vector double*)ap[1]; 

    for ( i=0; i< n/2; i+=2 )
    {
        v_y[i]   += v_x0 * va0[i] +  v_x1 * va1[i]   ;
        v_y[i+1] += v_x0 * va0[i+1] +  v_x1 * va1[i+1]  ;        
    } 
}
#else

static void dgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    BLASLONG i;
    FLOAT *a0,*a1;
    FLOAT x[4]  __attribute__ ((aligned (16)));
    a0 = ap[0];
    a1 = ap[1];

    for ( i=0; i<2; i++)
        x[i] = xo[i] * *alpha;

    for ( i=0; i< n; i+=4 )
    {
        y[i] += a0[i]*x[0] + a1[i]*x[1];        
        y[i+1] += a0[i+1]*x[0] + a1[i+1]*x[1];        
        y[i+2] += a0[i+2]*x[0] + a1[i+2]*x[1];        
        y[i+3] += a0[i+3]*x[0] + a1[i+3]*x[1];        
    }
}


#endif

#ifdef HAVE_KERNEL_4x1

#elif HAVE_KERNEL_4x1_VEC
static void dgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    
    BLASLONG i;
    FLOAT x0;
    x0 = xo[0] * *alpha;
    __vector double   v_x0 = {x0,x0};
    __vector double* v_y =(__vector double*)y;      
    __vector double* va0 = (__vector double*)ap;

    for ( i=0; i< n/2; i+=2 )
    {
        v_y[i] += v_x0 * va0[i]    ;
        v_y[i+1] += v_x0 * va0[i+1]  ;        
    }
        
 
}

#else
static void dgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    BLASLONG i;
    FLOAT *a0;
    FLOAT x[4]  __attribute__ ((aligned (16)));
    a0 = ap;

    for ( i=0; i<1; i++)
        x[i] = xo[i] * *alpha;

    for ( i=0; i< n; i+=4 )
    {
        y[i] += a0[i]*x[0];        
        y[i+1] += a0[i+1]*x[0];        
        y[i+2] += a0[i+2]*x[0];        
        y[i+3] += a0[i+3]*x[0];        
    }
}


#endif

 

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest)
{
    BLASLONG i;
        
    for ( i=0; i<n; i++ ){
            *dest += *src;
            src++;
            dest += inc_dest;
    }
    return;
     
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG i;
    BLASLONG j;
    FLOAT *a_ptr;
    FLOAT *x_ptr;
    FLOAT *y_ptr;
    FLOAT *ap[4];
    BLASLONG n1;
    BLASLONG m1;
    BLASLONG m2;
    BLASLONG m3;
    BLASLONG n2;
    BLASLONG lda4 =  lda << 2;
    FLOAT xbuffer[8],*ybuffer;

    if ( m < 1 ) return(0);
    if ( n < 1 ) return(0);

    ybuffer = buffer;
    
    n1 = n >> 2 ;
    n2 = n &  3 ;

    m3 = m & 3  ;
    m1 = m & -4 ;
    m2 = (m & (NBMAX-1)) - m3 ;

    y_ptr = y;

    BLASLONG NB = NBMAX;

    while ( NB == NBMAX )
    {
        
        m1 -= NB;
        if ( m1 < 0)
        {
            if ( m2 == 0 ) break;    
            NB = m2;
        }
        
        a_ptr = a;
        x_ptr = x;
        
        ap[0] = a_ptr;
        ap[1] = a_ptr + lda;
        ap[2] = ap[1] + lda;
        ap[3] = ap[2] + lda;

        if ( inc_y != 1 )
            memset(ybuffer,0,NB*8);
        else
            ybuffer = y_ptr;

        if ( inc_x == 1 )
        {


            for( i = 0; i < n1 ; i++)
            {
                dgemv_kernel_4x4(NB,ap,x_ptr,ybuffer,&alpha);
                ap[0] += lda4; 
                ap[1] += lda4; 
                ap[2] += lda4; 
                ap[3] += lda4; 
                a_ptr += lda4;
                x_ptr += 4;    
            }

            if ( n2 & 2 )
            {
                dgemv_kernel_4x2(NB,ap,x_ptr,ybuffer,&alpha);
                a_ptr += lda*2;
                x_ptr += 2;    
            }


            if ( n2 & 1 )
            {
                dgemv_kernel_4x1(NB,a_ptr,x_ptr,ybuffer,&alpha);
                a_ptr += lda;
                x_ptr += 1;    

            }


        }
        else
        {

            for( i = 0; i < n1 ; i++)
            {
                xbuffer[0] = x_ptr[0];
                x_ptr += inc_x;    
                xbuffer[1] =  x_ptr[0];
                x_ptr += inc_x;    
                xbuffer[2] =  x_ptr[0];
                x_ptr += inc_x;    
                xbuffer[3] = x_ptr[0];
                x_ptr += inc_x;    
                dgemv_kernel_4x4(NB,ap,xbuffer,ybuffer,&alpha);
                ap[0] += lda4; 
                ap[1] += lda4; 
                ap[2] += lda4; 
                ap[3] += lda4; 
                a_ptr += lda4;
            }

            for( i = 0; i < n2 ; i++)
            {
                xbuffer[0] = x_ptr[0];
                x_ptr += inc_x;    
                dgemv_kernel_4x1(NB,a_ptr,xbuffer,ybuffer,&alpha);
                a_ptr += lda;

            }

        }

        a     += NB;
        if ( inc_y != 1 )
        {
            add_y(NB,ybuffer,y_ptr,inc_y);
            y_ptr += NB * inc_y;
        }
        else
            y_ptr += NB ;

    }

    if ( m3 == 0 ) return(0);

    if ( m3 == 3 )
    {
        a_ptr = a;
        x_ptr = x;
        FLOAT temp0 = 0.0;
        FLOAT temp1 = 0.0;
        FLOAT temp2 = 0.0;
        if ( lda == 3 && inc_x ==1 )
        {

            for( i = 0; i < ( n & -4 ); i+=4 )
            {

                temp0 += a_ptr[0] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                temp1 += a_ptr[1] * x_ptr[0] + a_ptr[4] * x_ptr[1];
                temp2 += a_ptr[2] * x_ptr[0] + a_ptr[5] * x_ptr[1];

                temp0 += a_ptr[6] * x_ptr[2] + a_ptr[9]  * x_ptr[3];
                temp1 += a_ptr[7] * x_ptr[2] + a_ptr[10] * x_ptr[3];
                temp2 += a_ptr[8] * x_ptr[2] + a_ptr[11] * x_ptr[3];

                a_ptr += 12;
                x_ptr += 4;
            }

            for( ; i < n; i++ )
            {
                temp0 += a_ptr[0] * x_ptr[0];
                temp1 += a_ptr[1] * x_ptr[0];
                temp2 += a_ptr[2] * x_ptr[0];
                a_ptr += 3;
                x_ptr ++;
            }

        }
        else
        {

            for( i = 0; i < n; i++ )
            {
                temp0 += a_ptr[0] * x_ptr[0];
                temp1 += a_ptr[1] * x_ptr[0];
                temp2 += a_ptr[2] * x_ptr[0];
                a_ptr += lda;
                x_ptr += inc_x;


            }

        }
        y_ptr[0] += alpha * temp0;
        y_ptr += inc_y;
        y_ptr[0] += alpha * temp1;
        y_ptr += inc_y;
        y_ptr[0] += alpha * temp2;
        return(0);
    }


    if ( m3 == 2 )
    {
        a_ptr = a;
        x_ptr = x;
        FLOAT temp0 = 0.0;
        FLOAT temp1 = 0.0;
        if ( lda == 2 && inc_x ==1 )
        {

            for( i = 0; i < (n & -4) ; i+=4 )
            {
                temp0 += a_ptr[0] * x_ptr[0] + a_ptr[2] * x_ptr[1];
                temp1 += a_ptr[1] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                temp0 += a_ptr[4] * x_ptr[2] + a_ptr[6] * x_ptr[3];
                temp1 += a_ptr[5] * x_ptr[2] + a_ptr[7] * x_ptr[3];
                a_ptr += 8;
                x_ptr += 4;

            }


            for( ; i < n; i++ )
            {
                temp0 += a_ptr[0]   * x_ptr[0];
                temp1 += a_ptr[1]   * x_ptr[0];
                a_ptr += 2;
                x_ptr ++;
            }

        }
        else
        {

            for( i = 0; i < n; i++ )
            {
                temp0 += a_ptr[0] * x_ptr[0];
                temp1 += a_ptr[1] * x_ptr[0];
                a_ptr += lda;
                x_ptr += inc_x;


            }

        }
        y_ptr[0] += alpha * temp0;
        y_ptr += inc_y;
        y_ptr[0] += alpha * temp1;
        return(0);
    }

    if ( m3 == 1 )
    {
        a_ptr = a;
        x_ptr = x;
        FLOAT temp = 0.0;
        if ( lda == 1 && inc_x ==1 )
        {

            for( i = 0; i < (n & -4); i+=4 )
            {
                temp += a_ptr[i] * x_ptr[i] + a_ptr[i+1] * x_ptr[i+1] + a_ptr[i+2] * x_ptr[i+2] + a_ptr[i+3] * x_ptr[i+3];
    
            }

            for( ; i < n; i++ )
            {
                temp += a_ptr[i] * x_ptr[i];
            }

        }
        else
        {

            for( i = 0; i < n; i++ )
            {
                temp += a_ptr[0] * x_ptr[0];
                a_ptr += lda;
                x_ptr += inc_x;
            }

        }
        y_ptr[0] += alpha * temp;
        return(0);
    }


    return(0);
}


