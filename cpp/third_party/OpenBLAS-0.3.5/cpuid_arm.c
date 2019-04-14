/**************************************************************************
  Copyright (c) 2013, The OpenBLAS Project
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

#include <string.h>

#define CPU_UNKNOWN     	0
#define CPU_ARMV6       	1
#define CPU_ARMV7       	2
#define CPU_CORTEXA9       	3
#define CPU_CORTEXA15       	4

static char *cpuname[] = {
  "UNKNOWN",
  "ARMV6",
  "ARMV7",
  "CORTEXA9",
  "CORTEXA15"
};


static char *cpuname_lower[] = {
  "unknown",
  "armv6",
  "armv7",
  "cortexa9",
  "cortexa15"
};


int get_feature(char *search)
{

#ifdef linux
	FILE *infile;
  	char buffer[2048], *p,*t;
  	p = (char *) NULL ;

  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if (!strncmp("Features", buffer, 8))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}

  	fclose(infile);


	if( p == NULL ) return 0;

	t = strtok(p," ");
	while( t = strtok(NULL," "))
	{
		if (!strcmp(t, search))   { return(1); }
	}

#endif
	return(0);
}


int detect(void)
{

#ifdef linux

	FILE *infile;
  	char buffer[512], *p;
  	p = (char *) NULL ;

  	infile = fopen("/proc/cpuinfo", "r");
	while (fgets(buffer, sizeof(buffer), infile))
	{

		if (!strncmp("CPU part", buffer, 8))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}

  	fclose(infile);
  	if(p != NULL) {
	  if (strstr(p, "0xc09")) {
	    return CPU_CORTEXA9;
	  }
	  if (strstr(p, "0xc0f")) {
	    return CPU_CORTEXA15;
	  }
	  if (strstr(p, "0xd07")) {
	    return CPU_ARMV7;  //ARMV8 on 32-bit
	  }

	}

  	p = (char *) NULL ;
  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if ((!strncmp("model name", buffer, 10)) || (!strncmp("Processor", buffer, 9)))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}

  	fclose(infile);

  	if(p != NULL)
	{

		if (strstr(p, "ARMv7"))
		{
			 if ( get_feature("vfpv4"))
			 	return CPU_ARMV7;

			 if ( get_feature("vfpv3"))
			 	return CPU_ARMV7;

			 if ( get_feature("vfp"))
			 	return CPU_ARMV6;


		}

		if (strstr(p, "ARMv6"))
		{
			 if ( get_feature("vfp"))
			 	return CPU_ARMV6;
		}


	}

  	p = (char *) NULL ;
  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if ((!strncmp("CPU architecture", buffer, 16)))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}
  	fclose(infile);
  	if(p != NULL) {
	  if (strstr(p, "8")) {
	    return CPU_ARMV7;  //ARMV8 on 32-bit
	  }

	}

#endif

	return CPU_UNKNOWN;
}

char *get_corename(void)
{
	return cpuname[detect()];
}

void get_architecture(void)
{
	printf("ARM");
}

void get_subarchitecture(void)
{
	int d = detect();
	printf("%s", cpuname[d]);
}

void get_subdirname(void)
{
	printf("arm");
}

void get_cpuconfig(void)
{

	int d = detect();
	switch (d)
	{
	       case CPU_CORTEXA9:
    			printf("#define CORTEXA9\n");
    			printf("#define ARMV7\n");
    			printf("#define HAVE_VFP\n");
    			printf("#define HAVE_VFPV3\n");
			if ( get_feature("neon"))	printf("#define HAVE_NEON\n");
			if ( get_feature("vfpv4"))	printf("#define HAVE_VFPV4\n");
    			printf("#define L1_DATA_SIZE 32768\n");
    			printf("#define L1_DATA_LINESIZE 32\n");
    			printf("#define L2_SIZE 1048576\n");
    			printf("#define L2_LINESIZE 32\n");
    			printf("#define DTB_DEFAULT_ENTRIES 128\n");
    			printf("#define DTB_SIZE 4096\n");
    			printf("#define L2_ASSOCIATIVE 4\n");
			break;

	       case CPU_CORTEXA15:
    			printf("#define CORTEXA15\n");
    			printf("#define ARMV7\n");
    			printf("#define HAVE_VFP\n");
    			printf("#define HAVE_VFPV3\n");
			if ( get_feature("neon"))	printf("#define HAVE_NEON\n");
			if ( get_feature("vfpv4"))	printf("#define HAVE_VFPV4\n");
    			printf("#define L1_DATA_SIZE 32768\n");
    			printf("#define L1_DATA_LINESIZE 32\n");
    			printf("#define L2_SIZE 1048576\n");
    			printf("#define L2_LINESIZE 32\n");
    			printf("#define DTB_DEFAULT_ENTRIES 128\n");
    			printf("#define DTB_SIZE 4096\n");
    			printf("#define L2_ASSOCIATIVE 4\n");
			break;


		case CPU_ARMV7:
    			printf("#define ARMV7\n");
    			printf("#define HAVE_VFP\n");
    			printf("#define HAVE_VFPV3\n");
			if ( get_feature("neon"))	printf("#define HAVE_NEON\n");
			if ( get_feature("vfpv4"))	printf("#define HAVE_VFPV4\n");
    			printf("#define L1_DATA_SIZE 65536\n");
    			printf("#define L1_DATA_LINESIZE 32\n");
    			printf("#define L2_SIZE 512488\n");
    			printf("#define L2_LINESIZE 32\n");
    			printf("#define DTB_DEFAULT_ENTRIES 64\n");
    			printf("#define DTB_SIZE 4096\n");
    			printf("#define L2_ASSOCIATIVE 4\n");
			break;

		case CPU_ARMV6:
    			printf("#define ARMV6\n");
    			printf("#define HAVE_VFP\n");
    			printf("#define L1_DATA_SIZE 65536\n");
    			printf("#define L1_DATA_LINESIZE 32\n");
    			printf("#define L2_SIZE 512488\n");
    			printf("#define L2_LINESIZE 32\n");
    			printf("#define DTB_DEFAULT_ENTRIES 64\n");
    			printf("#define DTB_SIZE 4096\n");
    			printf("#define L2_ASSOCIATIVE 4\n");
			break;

	}
}


void get_libname(void)
{

	int d = detect();
	printf("%s", cpuname_lower[d]);
}


void get_features(void)
{

#ifdef linux
	FILE *infile;
  	char buffer[2048], *p,*t;
  	p = (char *) NULL ;

  	infile = fopen("/proc/cpuinfo", "r");

	while (fgets(buffer, sizeof(buffer), infile))
	{

		if (!strncmp("Features", buffer, 8))
		{
			p = strchr(buffer, ':') + 2;
			break;
      		}
  	}

  	fclose(infile);


	if( p == NULL ) return;

	t = strtok(p," ");
	while( t = strtok(NULL," "))
	{
		if (!strcmp(t, "vfp"))   { printf("HAVE_VFP=1\n"); continue; }
		if (!strcmp(t, "vfpv3")) { printf("HAVE_VFPV3=1\n"); continue; }
		if (!strcmp(t, "vfpv4")) { printf("HAVE_VFPV4=1\n"); continue; }
		if (!strcmp(t, "neon"))  { printf("HAVE_NEON=1\n"); continue; }
	}

#endif
	return;
}


