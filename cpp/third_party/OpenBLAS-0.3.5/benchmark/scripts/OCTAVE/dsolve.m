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

	A = double(rand(n,n));
	B = double(rand(n,n));
	start = clock();

	l=0;
	while l < loops

		x = linsolve(A,B);
		#x = A / B;
		l = l + 1;

	endwhile


	timeg = etime(clock(), start);
	#r = norm(A*x - B)/norm(B)
	mflops = ( 2.0/3.0 *n*n*n + 2.0*n*n*n ) *loops / ( timeg * 1.0e6 );

	st1 = sprintf("%dx%d : ", n,n);
	printf("%20s %10.2f MFlops %10.6f sec\n", st1, mflops, timeg );
	n = n + nstep;

endwhile
