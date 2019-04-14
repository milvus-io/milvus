#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(int argc, char**argv) {
FILE *fp;
char line[100];
char line2[80];
char *s;
int i;

fprintf(stdout,"#ifndef OPENBLAS_CONFIG_H\n");
fprintf(stdout,"#define OPENBLAS_CONFIG_H\n");
fp=fopen(argv[1],"r");
do{
s=fgets(line,80,fp);
if (s== NULL) break;
memset(line2,0,80);
i=sscanf(line,"#define %70c",line2);
if (i!=0) {
	fprintf(stdout,"#define OPENBLAS_%s",line2);
} else {
	fprintf(stdout,"\n");
}
} while (1);
fclose(fp);
fprintf(stdout,"#define OPENBLAS_VERSION \"OpenBLAS %s\"\n", VERSION);
fp=fopen(argv[2],"r");
do{
s=fgets(line,100,fp);
if (s== NULL) break;
fprintf(stdout,"%s",line);
} while(1);
fclose(fp);
fprintf(stdout,"#endif /* OPENBLAS_CONFIG_H */\n");
exit(0);
}
