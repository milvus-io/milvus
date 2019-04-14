#include "stdlib.h"
#include "stdio.h"
#include "signal.h"
#include "f2c.h"

#ifndef SIGIOT
#ifdef SIGABRT
#define SIGIOT SIGABRT
#endif
#endif

void sig_die(const char *s, int kill) {
	/* print error message, then clear buffers */
	fprintf(stderr, "%s\n", s);

	if(kill) {
		fflush(stderr);
		/* now get a core */
		signal(SIGIOT, SIG_DFL);
		abort();
	} else
		exit(1);
}

void c_div(complex *c, complex *a, complex *b) {
	double ratio, den;
	double abr, abi, cr;

	if( (abr = b->r) < 0.)
		abr = - abr;
	if( (abi = b->i) < 0.)
		abi = - abi;
	if( abr <= abi ) {
		if(abi == 0) {
#ifdef IEEE_COMPLEX_DIVIDE
			float af, bf;
			af = bf = abr;
			if (a->i != 0 || a->r != 0)
				af = 1.;
			c->i = c->r = af / bf;
			return;
#else
			sig_die("complex division by zero", 1);
#endif
		}
		ratio = (double)b->r / b->i ;
		den = b->i * (1 + ratio*ratio);
		cr = (a->r*ratio + a->i) / den;
		c->i = (a->i*ratio - a->r) / den;
	} else {
		ratio = (double)b->i / b->r ;
		den = b->r * (1 + ratio*ratio);
		cr = (a->r + a->i*ratio) / den;
		c->i = (a->i - a->r*ratio) / den;
    }
	c->r = cr;
}

void z_div(doublecomplex *c, doublecomplex *a, doublecomplex *b) {
	double ratio, den;
	double abr, abi, cr;

	if( (abr = b->r) < 0.)
		abr = - abr;
	if( (abi = b->i) < 0.)
		abi = - abi;
	if( abr <= abi ) {
		if(abi == 0) {
#ifdef IEEE_COMPLEX_DIVIDE
			if (a->i != 0 || a->r != 0)
				abi = 1.;
			c->i = c->r = abi / abr;
			return;
#else
			sig_die("complex division by zero", 1);
#endif
        }
		ratio = b->r / b->i ;
		den = b->i * (1 + ratio*ratio);
		cr = (a->r*ratio + a->i) / den;
		c->i = (a->i*ratio - a->r) / den;
	} else {
		ratio = b->i / b->r ;
		den = b->r * (1 + ratio*ratio);
		cr = (a->r + a->i*ratio) / den;
		c->i = (a->i - a->r*ratio) / den;
    }
	c->r = cr;
}

float r_imag(complex *z) {
    return z->i;
}

void r_cnjg(complex *r, complex *z) {
	float zi = z->i;
	r->r = z->r;
	r->i = -zi;
}

double d_imag(doublecomplex *z) {
    return z->i;
}

void d_cnjg(doublecomplex *r, doublecomplex *z) {
	double zi = z->i;
	r->r = z->r;
	r->i = -zi;
}
