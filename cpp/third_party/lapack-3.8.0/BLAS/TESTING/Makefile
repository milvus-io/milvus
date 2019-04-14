include ../../make.inc

all: single double complex complex16
single:    xblat1s xblat2s xblat3s
double:    xblat1d xblat2d xblat3d
complex:   xblat1c xblat2c xblat3c
complex16: xblat1z xblat2z xblat3z

xblat1s: sblat1.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat1d: dblat1.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat1c: cblat1.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat1z: zblat1.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^

xblat2s: sblat2.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat2d: dblat2.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat2c: cblat2.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat2z: zblat2.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^

xblat3s: sblat3.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat3d: dblat3.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat3c: cblat3.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xblat3z: zblat3.o $(BLASLIB)
	$(LOADER) $(LOADOPTS) -o $@ $^

run: all
	./xblat1s > sblat1.out
	./xblat1d > dblat1.out
	./xblat1c > cblat1.out
	./xblat1z > zblat1.out
	./xblat2s < sblat2.in
	./xblat2d < dblat2.in
	./xblat2c < cblat2.in
	./xblat2z < zblat2.in
	./xblat3s < sblat3.in
	./xblat3d < dblat3.in
	./xblat3c < cblat3.in
	./xblat3z < zblat3.in

clean: cleanobj cleanexe cleantest
cleanobj:
	rm -f *.o
cleanexe:
	rm -f xblat*
cleantest:
	rm -f *.out core

.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<
