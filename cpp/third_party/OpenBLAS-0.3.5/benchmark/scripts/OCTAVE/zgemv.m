#!/usr/bin/octave --silent 

nfrom = 128 ;
nto   = 2048;
nstep = 128;
loops = 1;


arg_list = argv();
for i = 1:nargin

	switch(i)
		case 1
			nfrom = str2num(arg_list{i});
		case 2
			nto   = str2num(arg_list{i});
		case 3
			nstep = str2num(arg_list{i});
		case 4
			loops = str2num(arg_list{i});

	endswitch

endfor

p = getenv("OPENBLAS_LOOPS");
if p 
	loops = str2num(p);
endif

printf("From %d To %d Step=%d Loops=%d\n",nfrom, nto, nstep, loops);
printf("        SIZE             FLOPS             TIME\n");

n = nfrom;
while n <= nto

	A = double(rand(n,n)) + double(rand(n,n)) * 1i;
	B = double(rand(n,1)) + double(rand(n,1)) * 1i;
	start = clock();

	l=0;
	while l < loops

		C = A * B;
		l = l + 1;

	endwhile

	timeg = etime(clock(), start);
	mflops = ( 4.0 * 2.0*n*n *loops ) / ( timeg * 1.0e6 );

	st1 = sprintf("%dx%d : ", n,n);
	printf("%20s %10.2f MFlops %10.6f sec\n", st1, mflops, timeg);
	n = n + nstep;

endwhile
