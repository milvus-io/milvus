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

#define HAVE_KERNEL_4x4_VEC 1
#define HAVE_KERNEL_4x2_VEC 1
#define HAVE_KERNEL_4x1_VEC 1

#if defined(HAVE_KERNEL_4x4_VEC) || defined(HAVE_KERNEL_4x2_VEC) || defined(HAVE_KERNEL_4x1_VEC)
 #include <vecintrin.h>
#endif
#define NBMAX 2048

#ifdef HAVE_KERNEL_4x4

#elif HAVE_KERNEL_4x4_VEC

static void dgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{
    BLASLONG i; 
    __vector double* va0 = (__vector double*)ap[0];
    __vector double* va1 = (__vector double*)ap[1];
    __vector double* va2 = (__vector double*)ap[2];
    __vector double* va3 = (__vector double*)ap[3];     
    __vector double* v_x =(__vector double*)x;           
    __vector double temp0 = {0,0};      
    __vector double temp1 = {0,0};       
    __vector double temp2 = {0,0};       
    __vector double temp3 = {0,0};       

    for ( i=0; i< n/2; i+=2 )
    {
        temp0 += v_x[i] * va0[i]  + v_x[i+1] * va0[i+1] ;        
        temp1 += v_x[i] * va1[i]  + v_x[i+1] * va1[i+1] ;    
        temp2 += v_x[i] * va2[i]  + v_x[i+1] * va2[i+1] ;    
        temp3 += v_x[i] * va3[i]  + v_x[i+1] * va3[i+1] ;        
    }
        
    y[0] = temp0[0] + temp0[1];
    y[1] = temp1[0] + temp1[1];
    y[2] = temp2[0] + temp2[1];
    y[3] = temp3[0] + temp3[1];; 
}
#else
static void dgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{
    BLASLONG i;
    FLOAT *a0,*a1,*a2,*a3;
    a0 = ap[0];
    a1 = ap[1];
    a2 = ap[2];
    a3 = ap[3];
    FLOAT temp0 = 0.0;
    FLOAT temp1 = 0.0;
    FLOAT temp2 = 0.0;
    FLOAT temp3 = 0.0;

    for ( i=0; i< n; i+=4 )
    {
        temp0 += a0[i]*x[i] + a0[i+1]*x[i+1] + a0[i+2]*x[i+2] + a0[i+3]*x[i+3];        
        temp1 += a1[i]*x[i] + a1[i+1]*x[i+1] + a1[i+2]*x[i+2] + a1[i+3]*x[i+3];        
        temp2 += a2[i]*x[i] + a2[i+1]*x[i+1] + a2[i+2]*x[i+2] + a2[i+3]*x[i+3];        
        temp3 += a3[i]*x[i] + a3[i+1]*x[i+1] + a3[i+2]*x[i+2] + a3[i+3]*x[i+3];        
    }
    y[0] = temp0;
    y[1] = temp1;
    y[2] = temp2;
    y[3] = temp3;
}
    
#endif
 
#ifdef HAVE_KERNEL_4x2

#elif HAVE_KERNEL_4x2_VEC

static void dgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{
    BLASLONG i; 
    __vector double* va0 = (__vector double*)ap[0];
    __vector double* va1 = (__vector double*)ap[1];     
    __vector double* v_x =(__vector double*)x;           
    __vector double temp0 = {0,0};      
    __vector double temp1 = {0,0};         

    for ( i=0; i< n/2; i+=2 )
    {
        temp0 += v_x[i] * va0[i]  + v_x[i+1] * va0[i+1] ;        
        temp1 += v_x[i] * va1[i]  + v_x[i+1] * va1[i+1] ;        
    }
        
    y[0] = temp0[0] + temp0[1];
    y[1] = temp1[0] + temp1[1]; 
}
#else
static void dgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{

    BLASLONG i;
    FLOAT *a0,*a1;
    a0 = ap[0];
    a1 = ap[1]; 
    FLOAT temp0 = 0.0;
    FLOAT temp1 = 0.0; 

    for ( i=0; i< n; i+=4 )
    {
        temp0 += a0[i]*x[i] + a0[i+1]*x[i+1] + a0[i+2]*x[i+2] + a0[i+3]*x[i+3];        
        temp1 += a1[i]*x[i] + a1[i+1]*x[i+1] + a1[i+2]*x[i+2] + a1[i+3]*x[i+3];         
    }
    y[0] = temp0;
    y[1] = temp1; 

}
#endif    

#ifdef HAVE_KERNEL_4x1

#elif HAVE_KERNEL_4x1_VEC

static void dgemv_kernel_4x1(BLASLONG n, FLOAT *a0, FLOAT *x, FLOAT *y)
{
    BLASLONG i; 
    __vector double* va0 = (__vector double*)a0;      
    __vector double* v_x =(__vector double*)x;           
    __vector double temp0 = {0,0};          

    for ( i=0; i< n/2; i+=2 )
    {
        temp0 += v_x[i] * va0[i]  + v_x[i+1] * va0[i+1] ;         
    }
        
    y[0] = temp0[0] + temp0[1]; 
}
#else
static void dgemv_kernel_4x1(BLASLONG n, FLOAT *a0, FLOAT *x, FLOAT *y)
{
    BLASLONG i;
    
     
    FLOAT temp0 = 0.0; 

    for ( i=0; i< n; i+=4 )
    {
        temp0 += a0[i]*x[i] + a0[i+1]*x[i+1] + a0[i+2]*x[i+2] + a0[i+3]*x[i+3];         
    }
    y[0] = temp0; 
}
#endif
    
static void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src)
{
        BLASLONG i;
        for ( i=0; i<n; i++ )
        {
                *dest = *src;
                dest++;
                src += inc_src;
        }
}
 
static void add_y(BLASLONG n, FLOAT da , FLOAT *src, FLOAT *dest, BLASLONG inc_dest)
{

        BLASLONG i;
 
        for ( i=0; i<n; i++ )
        {
                *dest += src[i]  * da;
                dest  += inc_dest;
        }
        return; 
         
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG register i;
    BLASLONG register j;
    FLOAT *a_ptr;
    FLOAT *x_ptr;
    FLOAT *y_ptr;
    BLASLONG n0;
    BLASLONG n1;
    BLASLONG m1;
    BLASLONG m2;
    BLASLONG m3;
    BLASLONG n2;
    FLOAT ybuffer[4],*xbuffer;
    FLOAT *ytemp;

    if ( m < 1 ) return(0);
    if ( n < 1 ) return(0);

    xbuffer = buffer;
    ytemp   = buffer + (m < NBMAX ? m : NBMAX);
    
    n0 = n / NBMAX;
    n1 = (n % NBMAX)  >> 2 ;
    n2 = n & 3  ;

    m3 = m & 3  ;
    m1 = m & -4 ;
    m2 = (m & (NBMAX-1)) - m3 ;


    BLASLONG NB = NBMAX;

    while ( NB == NBMAX )
    {
        
        m1 -= NB;
        if ( m1 < 0)
        {
            if ( m2 == 0 ) break;    
            NB = m2;
        }
        
        y_ptr = y;
        a_ptr = a;
        x_ptr = x;

        if ( inc_x == 1 )
            xbuffer = x_ptr;
        else
            copy_x(NB,x_ptr,xbuffer,inc_x);


        FLOAT *ap[4];
        FLOAT *yp;
        BLASLONG register lda4 = 4 * lda;
        ap[0] = a_ptr;
        ap[1] = a_ptr + lda;
        ap[2] = ap[1] + lda;
        ap[3] = ap[2] + lda;

        if ( n0 > 0 )
        {
            BLASLONG nb1 = NBMAX / 4;
            for( j=0; j<n0; j++)
            {

                yp = ytemp;
                for( i = 0; i < nb1  ; i++)
                {
                    dgemv_kernel_4x4(NB,ap,xbuffer,yp);
                    ap[0] += lda4 ;
                    ap[1] += lda4 ;
                    ap[2] += lda4 ;
                    ap[3] += lda4 ;
                    yp += 4;
                }
                add_y(nb1*4, alpha, ytemp, y_ptr, inc_y );
                y_ptr += nb1 * inc_y * 4;
                a_ptr += nb1 * lda4 ;

            }

        }


        yp = ytemp;

        for( i = 0; i < n1 ; i++)
        {
            dgemv_kernel_4x4(NB,ap,xbuffer,yp);
            ap[0] += lda4 ;
            ap[1] += lda4 ;
            ap[2] += lda4 ;
            ap[3] += lda4 ;
            yp += 4;
        }
        if ( n1 > 0 )
        {
            add_y(n1*4, alpha, ytemp, y_ptr, inc_y );
            y_ptr += n1 * inc_y * 4;
            a_ptr += n1 * lda4 ;
        }

        if ( n2 & 2 )
        {

            dgemv_kernel_4x2(NB,ap,xbuffer,ybuffer);
            a_ptr  += lda * 2;
            *y_ptr += ybuffer[0] * alpha;
            y_ptr  += inc_y;
            *y_ptr += ybuffer[1] * alpha;
            y_ptr  += inc_y;

        }

        if ( n2 & 1 )
        {

            dgemv_kernel_4x1(NB,a_ptr,xbuffer,ybuffer);
            a_ptr  += lda;
            *y_ptr += ybuffer[0] * alpha;
            y_ptr  += inc_y;

        }
        a += NB;
        x += NB * inc_x;    
    }

    if ( m3 == 0 ) return(0);

    x_ptr = x;
    a_ptr = a;
    if ( m3 == 3 )
    {
        FLOAT xtemp0 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp1 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp2 = *x_ptr * alpha;

        FLOAT *aj = a_ptr;
        y_ptr = y;

        if ( lda == 3 && inc_y == 1 )
        {

            for ( j=0; j< ( n & -4) ; j+=4 )
            {

                y_ptr[j]   += aj[0] * xtemp0 + aj[1]  * xtemp1 + aj[2]  * xtemp2;
                y_ptr[j+1] += aj[3] * xtemp0 + aj[4]  * xtemp1 + aj[5]  * xtemp2;
                y_ptr[j+2] += aj[6] * xtemp0 + aj[7]  * xtemp1 + aj[8]  * xtemp2;
                y_ptr[j+3] += aj[9] * xtemp0 + aj[10] * xtemp1 + aj[11] * xtemp2;
                 aj        += 12;
            }

            for ( ; j<n; j++ )
            {
                y_ptr[j]  += aj[0] * xtemp0 + aj[1] * xtemp1 + aj[2] * xtemp2;
                 aj        += 3;
            }

        }
        else
        {

            if ( inc_y == 1 )
            {

                BLASLONG register lda2 = lda << 1;
                BLASLONG register lda4 = lda << 2;
                BLASLONG register lda3 = lda2 + lda;

                for ( j=0; j< ( n & -4 ); j+=4 )
                {

                    y_ptr[j]    += *aj        * xtemp0 + *(aj+1)      * xtemp1 + *(aj+2)      * xtemp2;
                    y_ptr[j+1]  += *(aj+lda)  * xtemp0 + *(aj+lda+1)  * xtemp1 + *(aj+lda+2)  * xtemp2;
                    y_ptr[j+2]  += *(aj+lda2) * xtemp0 + *(aj+lda2+1) * xtemp1 + *(aj+lda2+2) * xtemp2;
                    y_ptr[j+3]  += *(aj+lda3) * xtemp0 + *(aj+lda3+1) * xtemp1 + *(aj+lda3+2) * xtemp2;
                     aj          += lda4;
                }

                for ( ; j< n ; j++ )
                {

                    y_ptr[j]    += *aj * xtemp0 + *(aj+1) * xtemp1 + *(aj+2) * xtemp2 ;
                     aj          += lda;
                }

            }
            else
            {

                for ( j=0; j<n; j++ )
                {
                    *y_ptr += *aj * xtemp0 + *(aj+1) * xtemp1 + *(aj+2) * xtemp2;
                     y_ptr += inc_y;
                     aj    += lda;
                }


            }

        }
        return(0);
    }

    if ( m3 == 2 )
    {
        FLOAT xtemp0 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp1 = *x_ptr * alpha;

        FLOAT *aj = a_ptr;
        y_ptr = y;

        if ( lda == 2 && inc_y == 1 )
        {

            for ( j=0; j< ( n & -4) ; j+=4 )
            {
                y_ptr[j]   += aj[0] * xtemp0 + aj[1] * xtemp1 ;
                y_ptr[j+1] += aj[2] * xtemp0 + aj[3] * xtemp1 ;
                y_ptr[j+2] += aj[4] * xtemp0 + aj[5] * xtemp1 ;
                y_ptr[j+3] += aj[6] * xtemp0 + aj[7] * xtemp1 ;
                 aj         += 8;

            }

            for ( ; j<n; j++ )
            {
                y_ptr[j] += aj[0] * xtemp0 + aj[1] * xtemp1 ;
                 aj       += 2;
            }

        }
        else
        {
            if ( inc_y == 1 )
            {

                BLASLONG register lda2 = lda << 1;
                BLASLONG register lda4 = lda << 2;
                BLASLONG register lda3 = lda2 + lda;

                for ( j=0; j< ( n & -4 ); j+=4 )
                {

                    y_ptr[j]    += *aj        * xtemp0 + *(aj+1)      * xtemp1 ;
                    y_ptr[j+1]  += *(aj+lda)  * xtemp0 + *(aj+lda+1)  * xtemp1 ;
                    y_ptr[j+2]  += *(aj+lda2) * xtemp0 + *(aj+lda2+1) * xtemp1 ;
                    y_ptr[j+3]  += *(aj+lda3) * xtemp0 + *(aj+lda3+1) * xtemp1 ;
                     aj          += lda4;
                }

                for ( ; j< n ; j++ )
                {

                    y_ptr[j]    += *aj * xtemp0 + *(aj+1) * xtemp1 ;
                     aj          += lda;
                }

            }
            else
            {
                for ( j=0; j<n; j++ )
                {
                    *y_ptr += *aj * xtemp0 + *(aj+1) * xtemp1 ;
                     y_ptr += inc_y;
                     aj    += lda;
                }
            }

        }
        return(0);

    }

    FLOAT xtemp = *x_ptr * alpha;
    FLOAT *aj = a_ptr;
    y_ptr = y;
    if ( lda == 1 && inc_y == 1 )
    {
        for ( j=0; j< ( n & -4) ; j+=4 )
        {
            y_ptr[j]   += aj[j]   * xtemp;
            y_ptr[j+1] += aj[j+1] * xtemp;
            y_ptr[j+2] += aj[j+2] * xtemp;
            y_ptr[j+3] += aj[j+3] * xtemp;
        }
        for ( ; j<n   ; j++ )
        {
            y_ptr[j] += aj[j] * xtemp;
        }



    }
    else
    {
        if ( inc_y == 1 )
        {

            BLASLONG register lda2 = lda << 1;
            BLASLONG register lda4 = lda << 2;
            BLASLONG register lda3 = lda2 + lda;
            for ( j=0; j< ( n & -4 ); j+=4 )
            {
                y_ptr[j]    += *aj        * xtemp;
                y_ptr[j+1]  += *(aj+lda)  * xtemp;
                y_ptr[j+2]  += *(aj+lda2) * xtemp;
                y_ptr[j+3]  += *(aj+lda3) * xtemp;
                 aj          += lda4  ;
            }

            for ( ; j<n; j++ )
            {
                y_ptr[j]  += *aj * xtemp;
                 aj        += lda;
            }

        }
        else
        {
            for ( j=0; j<n; j++ )
            {
                *y_ptr += *aj * xtemp;
                 y_ptr += inc_y;
                 aj    += lda;
            }

        }
    }

    return(0);
}


