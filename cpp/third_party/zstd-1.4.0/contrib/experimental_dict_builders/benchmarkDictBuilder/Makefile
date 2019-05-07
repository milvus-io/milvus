ARG :=

CC ?= gcc
CFLAGS ?= -O3
INCLUDES := -I ../randomDictBuilder -I ../../../programs -I ../../../lib/common -I ../../../lib -I ../../../lib/dictBuilder

RANDOM_FILE := ../randomDictBuilder/random.c
IO_FILE := ../randomDictBuilder/io.c

all: run clean

.PHONY: run
run: benchmark
	echo "Benchmarking with $(ARG)"
	./benchmark $(ARG)

.PHONY: test
test: benchmarkTest clean

.PHONY: benchmarkTest
benchmarkTest: benchmark test.sh
	sh test.sh

benchmark: benchmark.o io.o random.o libzstd.a
	$(CC) $(CFLAGS) benchmark.o io.o random.o libzstd.a -o benchmark

benchmark.o: benchmark.c
	$(CC) $(CFLAGS) $(INCLUDES) -c benchmark.c

random.o: $(RANDOM_FILE)
	$(CC) $(CFLAGS) $(INCLUDES) -c $(RANDOM_FILE)

io.o: $(IO_FILE)
	$(CC) $(CFLAGS) $(INCLUDES) -c $(IO_FILE)

libzstd.a:
	$(MAKE) -C ../../../lib libzstd.a
	mv ../../../lib/libzstd.a .

.PHONY: clean
clean:
	rm -f *.o benchmark libzstd.a
	$(MAKE) -C ../../../lib clean
	echo "Cleaning is completed"
